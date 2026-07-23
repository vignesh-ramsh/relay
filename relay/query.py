"""
relay.query
-------------------
The bounded Query Engine (Architecture §3.4) — filters, ordering, pagination,
single-hop relation resolution, and aggregates. Lives in Relay, not psqldb
(psqldb only ever executes SQL — arc.psqldb.fetch/fetch_one/fetch_val/execute
— and holds the schema registry every table's shape is read from; Relay is
where the vocabulary of "what a filter/field/order_by means" is decided and
turned into one parameterized SQL string).

Everything below is a pure function: schema in, (sql, params) out, no I/O,
no `psqldb` import beyond the type hints on Field/TableSchema (the same,
already-established minimal exception RelayProvider.register_hooks() takes
on psqldb.model.slugify_table_name — see relay/__init__.py). That purity is
deliberate: this is the first genuinely unit-testable surface in the project
(no live Postgres needed to assert what SQL a given filter dict produces),
which matters more here than anywhere else — this is "the single riskiest
piece of the whole system" per §3.4.

Scope, stated plainly (not an oversight list — a boundary):
  * filters: flat dict, implicit AND, one of a fixed operator set below.
    No OR/NOT grouping — that's the complexity cliff every ORM falls off.
  * order_by / fields / group_by: plain columns on the table itself only.
    No dotted paths, no joins — joins happen ONLY inside an explicit
    arc.relay.resolve(field, subfields), never implicitly.
  * resolve() is ONE hop deep, through REFERENCE fields only. A REFERENCE
    field's bare column value is already the target's real business key by
    default now (psqldb's target_field, docs/arc.MD) — resolve() is for
    pulling ADDITIONAL fields off the related row, not for reading the
    relation itself.
  * aggregates: group_by + count/sum/avg/min/max, no HAVING, no resolve().
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# Another deliberate, minimal dependency Relay takes on psqldb's internals —
# same exception §3.2 already carves out for register_hooks()'s
# slugify_table_name import: type hints only, not a second query-building
# implementation duplicating psqldb's own field/schema knowledge.
from psqldb.fields import Field
from psqldb.model import TableSchema

from .resolvers import FieldResolver

MAX_ORDER_FIELDS = 5
MAX_AGGREGATES = 10

_COMPARISON_OPS: dict[str, str] = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_LIKE_OPS = frozenset({"like", "ilike"})
_LIKE_WRAP_OPS: dict[str, str] = {"contains": "%{}%", "startswith": "{}%", "endswith": "%{}"}
_SET_OPS = frozenset({"in", "not_in"})
_ALL_OPS = frozenset(_COMPARISON_OPS) | _LIKE_OPS | frozenset(_LIKE_WRAP_OPS) | _SET_OPS | {"range", "is_null"}
_AGG_FNS = frozenset({"count", "sum", "avg", "min", "max"})

# psqldb.migrate.ddl.RefColumn's shape, duck-typed here (table/column/sql_type
# attributes) rather than imported — RelayProvider passes arc.psqldb.ref_columns()
# straight through; this module never constructs one itself.
RefColumns = dict[tuple[str, str], Any]
SchemaLookup = Callable[[str], TableSchema]


class QueryError(ValueError):
    """A caller's filters/fields/order_by/aggregates don't make sense
    against the schema — an unknown table/column/operator, a malformed
    operator value, a bound exceeded. Always the caller's mistake to fix —
    a plain ValueError subclass, not a RelayError (relay/__init__.py's
    whitelist() error handling only converts RelayError into an HTTP
    response)."""


@dataclass(frozen=True)
class Resolve:
    """Built by arc.relay.resolve(field, subfields) — a marker placed inside
    a `fields=[...]` list telling the engine to LEFT JOIN through a
    REFERENCE field and pull specific columns off the related row, nested
    under `field` in the result: row["department"] = {"dept_name": ..., ...}.
    One hop only — `field` must itself be a plain column name on the base
    table, never another Resolve or a dotted path."""
    field: str
    subfields: tuple[str, ...]


def _escape_like(value: str) -> str:
    """Escapes literal %, _, and \\ in a caller-supplied value before it's
    wrapped with % for contains/startswith/endswith — otherwise a value that
    happens to contain '%' or '_' would silently turn into an unintended
    wildcard (not an injection risk, still parameterized, but a real
    correctness bug: a search for "100%" matching more than the caller
    meant)."""
    return str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _field_sql_type(table: str, field: Field, ref_columns: RefColumns) -> str:
    """The field's REAL physical SQL type — field.sql_type() alone is wrong
    for a REFERENCE field with a non-default target_field (it always
    returns "UUID"; see Field.target_field's docstring in psqldb.fields).
    Same helper shape as the fix already applied to psqldb's update_many."""
    if field.type == "REFERENCE":
        return ref_columns[(table, field.name)].sql_type
    return field.sql_type()


def parse_filters(schema: TableSchema, filters: dict[str, Any] | None) -> list[tuple[Field, str, Any]]:
    """Validates a raw filter dict into (field, operator, value) triples.
    `{"status": "Active"}` is sugar for `{"status": {"eq": "Active"}}`."""
    if not filters:
        return []
    columns = {f.name: f for f in schema.column_fields()}
    unknown = [k for k in filters if k not in columns]
    if unknown:
        raise QueryError(f"unknown column(s) {unknown} on table '{schema.table}' (known: {sorted(columns)}).")

    parsed: list[tuple[Field, str, Any]] = []
    for name, raw in filters.items():
        field = columns[name]
        if isinstance(raw, dict):
            if len(raw) != 1:
                raise QueryError(
                    f"filter for '{name}' must have exactly one operator, got {list(raw)} — "
                    f"the engine doesn't support combining operators on one field (or OR/NOT "
                    f"grouping across fields) in this bounded first cut."
                )
            (op, value), = raw.items()
        else:
            op, value = "eq", raw
        if op not in _ALL_OPS:
            raise QueryError(f"filter '{name}': unknown operator '{op}' — expected one of {sorted(_ALL_OPS)}.")
        if op == "range" and (not isinstance(value, (list, tuple)) or len(value) != 2):
            raise QueryError(f"filter '{name}': 'range' requires a 2-element [min, max] list, got {value!r}.")
        if op in _SET_OPS and not isinstance(value, (list, tuple)):
            raise QueryError(f"filter '{name}': '{op}' requires a list of values, got {value!r}.")
        parsed.append((field, op, value))
    return parsed


def render_where(
    table: str, parsed: list[tuple[Field, str, Any]], ref_columns: RefColumns, *, start: int = 1
) -> tuple[str, list[Any]]:
    """Turns parsed (field, op, value) triples into one parameterized WHERE
    fragment — implicit AND across all of them. Returns ("", []) for no
    filters, never "WHERE" with nothing after it."""
    if not parsed:
        return "", []
    clauses: list[str] = []
    params: list[Any] = []
    i = start
    for field, op, value in parsed:
        col = f'"{field.name}"'
        if op in _COMPARISON_OPS:
            clauses.append(f"{col} {_COMPARISON_OPS[op]} ${i}")
            params.append(value)
            i += 1
        elif op in _LIKE_OPS:
            clauses.append(f"{col} {op.upper()} ${i} ESCAPE '\\'")
            params.append(value)
            i += 1
        elif op in _LIKE_WRAP_OPS:
            clauses.append(f"{col} ILIKE ${i} ESCAPE '\\'")
            params.append(_LIKE_WRAP_OPS[op].format(_escape_like(value)))
            i += 1
        elif op in _SET_OPS:
            cast = _field_sql_type(table, field, ref_columns)
            fn = "= ANY" if op == "in" else "<> ALL"
            clauses.append(f"{col} {fn}(${i}::{cast}[])")
            params.append(list(value))
            i += 1
        elif op == "range":
            lo, hi = value
            clauses.append(f"{col} BETWEEN ${i} AND ${i + 1}")
            params.extend([lo, hi])
            i += 2
        elif op == "is_null":
            clauses.append(f"{col} IS NULL" if value else f"{col} IS NOT NULL")
        else:  # pragma: no cover - _ALL_OPS is exhaustive against the branches above
            raise AssertionError(f"unhandled operator {op!r}")
    return " AND ".join(clauses), params


def render_order_by(table: str, schema: TableSchema, order_by: list[str]) -> str:
    if len(order_by) > MAX_ORDER_FIELDS:
        raise QueryError(f"order_by supports at most {MAX_ORDER_FIELDS} fields, got {len(order_by)}.")
    columns = {f.name: f for f in schema.column_fields()}
    parts = []
    for raw in order_by:
        desc = raw.startswith("-")
        name = raw[1:] if desc else raw
        if name not in columns:
            raise QueryError(f"unknown column '{name}' in order_by on table '{table}' (known: {sorted(columns)}).")
        parts.append(f'"{name}" {"DESC" if desc else "ASC"}')
    return ", ".join(parts)


def _validate_limit_offset(limit: int | None, offset: int) -> None:
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 0):
        raise QueryError(f"limit must be a non-negative integer or None (fetch all), got {limit!r}.")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise QueryError(f"offset must be a non-negative integer, got {offset!r}.")


def build_select(
    table: str,
    schema: TableSchema,
    *,
    filters: dict[str, Any] | None,
    fields: list[str | Resolve | FieldResolver] | None,
    order_by: list[str] | None,
    limit: int | None,
    offset: int,
    distinct: bool,
    ref_columns: RefColumns,
    schema_lookup: SchemaLookup,
) -> tuple[str, list[Any]]:
    """Builds one SELECT — the base primitive get()/list() both funnel
    through (get() is just this with filters={"id": ...} or a caller-given
    filter dict, limit=1)."""
    _validate_limit_offset(limit, offset)
    own_columns = {f.name: f for f in schema.column_fields()}

    select_parts: list[str] = []
    join_parts: list[str] = []
    if fields is None:
        select_parts.append(f'"{table}".*')
    else:
        seen_aliases: set[str] = set()
        for item in fields:
            if isinstance(item, Resolve):
                ref_field = own_columns.get(item.field)
                if ref_field is None or ref_field.type != "REFERENCE":
                    raise QueryError(
                        f"arc.relay.resolve('{item.field}', ...): '{item.field}' is not a "
                        f"REFERENCE field on table '{table}'."
                    )
                ref = ref_columns.get((table, item.field))
                if ref is None:  # defensive — every REFERENCE field always has one, see resolve_ref_columns
                    raise QueryError(f"'{item.field}' on table '{table}' has no resolvable target.")
                target_schema = schema_lookup(ref.table)  # raises psqldb.SchemaError if somehow missing
                target_columns = {f.name: f for f in target_schema.column_fields()}
                unknown = [sf for sf in item.subfields if sf not in target_columns]
                if unknown:
                    raise QueryError(
                        f"arc.relay.resolve('{item.field}', ...): unknown column(s) {unknown} "
                        f"on table '{ref.table}' (known: {sorted(target_columns)})."
                    )
                alias = item.field
                if alias not in seen_aliases:
                    join_parts.append(
                        f'LEFT JOIN "{ref.table}" AS "{alias}" ON "{alias}"."{ref.column}" = "{table}"."{item.field}"'
                    )
                    seen_aliases.add(alias)
                for sf in item.subfields:
                    select_parts.append(f'"{alias}"."{sf}" AS "{alias}.{sf}"')
            elif isinstance(item, FieldResolver):
                # Selected exactly like a plain column — the resolver only
                # changes what happens to the value AFTER the fetch (see
                # relay.resolvers / RelayProvider._resolve_fields), not how
                # it's read out of SQL.
                if item.field not in own_columns:
                    raise QueryError(
                        f"unknown column '{item.field}' on table '{table}' (known: {sorted(own_columns)})."
                    )
                select_parts.append(f'"{table}"."{item.field}" AS "{item.field}"')
            else:
                if item not in own_columns:
                    raise QueryError(f"unknown column '{item}' on table '{table}' (known: {sorted(own_columns)}).")
                select_parts.append(f'"{table}"."{item}" AS "{item}"')

    parsed_filters = parse_filters(schema, filters)
    where_sql, params = render_where(table, parsed_filters, ref_columns, start=1)

    sql = f'SELECT {"DISTINCT " if distinct else ""}{", ".join(select_parts)} FROM "{table}"'
    sql += "".join(f" {j}" for j in join_parts)
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_by:
        sql += " ORDER BY " + render_order_by(table, schema, order_by)
    if limit is not None:
        sql += f" LIMIT {limit}"
    if offset:
        sql += f" OFFSET {offset}"
    return sql, params


def build_count(
    table: str, schema: TableSchema, *, filters: dict[str, Any] | None, ref_columns: RefColumns
) -> tuple[str, list[Any]]:
    parsed = parse_filters(schema, filters)
    where_sql, params = render_where(table, parsed, ref_columns, start=1)
    sql = f'SELECT count(*) FROM "{table}"'
    if where_sql:
        sql += f" WHERE {where_sql}"
    return sql, params


def build_aggregate(
    table: str,
    schema: TableSchema,
    *,
    group_by: list[str] | None,
    aggregates: dict[str, tuple[str, str]],
    filters: dict[str, Any] | None,
    ref_columns: RefColumns,
) -> tuple[str, list[Any]]:
    if not aggregates:
        raise QueryError("aggregate() requires at least one entry in `aggregates`.")
    if len(aggregates) > MAX_AGGREGATES:
        raise QueryError(f"aggregate() supports at most {MAX_AGGREGATES} aggregates, got {len(aggregates)}.")
    columns = {f.name: f for f in schema.column_fields()}
    group_by = group_by or []
    unknown_group = [g for g in group_by if g not in columns]
    if unknown_group:
        raise QueryError(f"unknown column(s) {unknown_group} in group_by on table '{table}' (known: {sorted(columns)}).")

    select_parts = [f'"{g}"' for g in group_by]
    for alias, spec in aggregates.items():
        # The alias is the ONE caller-supplied identifier in the engine that
        # can't be validated against the schema (it's an output name, not a
        # column) — so it gets the same whitelist treatment everything else
        # here gets, or a '"' inside it would break out of the quoted
        # identifier position it's interpolated into below.
        if not isinstance(alias, str) or not alias.isidentifier():
            raise QueryError(
                f"aggregate alias {alias!r} must be a plain identifier "
                f"(letters, digits, underscores; not starting with a digit)."
            )
        if not isinstance(spec, (list, tuple)) or len(spec) != 2:
            raise QueryError(f"aggregate '{alias}': expected a (function, field) pair, got {spec!r}.")
        fn, field_name = spec
        if fn not in _AGG_FNS:
            raise QueryError(f"aggregate '{alias}': unknown function '{fn}' — expected one of {sorted(_AGG_FNS)}.")
        if field_name == "*":
            if fn != "count":
                raise QueryError(f"aggregate '{alias}': '*' is only valid with 'count'.")
            select_parts.append(f'{fn}(*) AS "{alias}"')
            continue
        if field_name not in columns:
            raise QueryError(f"aggregate '{alias}': unknown column '{field_name}' on table '{table}'.")
        select_parts.append(f'{fn}("{field_name}") AS "{alias}"')

    parsed = parse_filters(schema, filters)
    where_sql, params = render_where(table, parsed, ref_columns, start=1)
    sql = f'SELECT {", ".join(select_parts)} FROM "{table}"'
    if where_sql:
        sql += f" WHERE {where_sql}"
    if group_by:
        sql += " GROUP BY " + ", ".join(f'"{g}"' for g in group_by)
    return sql, params
