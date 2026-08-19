"""Query Engine (docs/arc.MD §3.4) — get/list/count/aggregate all funnel
through query.build_select/build_count/build_aggregate; none of them run
hooks (that taxonomy is entirely about writes, relay/hooks.py). All of
them honor the ambient connection (relay/transactions.py's _connection())
by default — a read inside a hook sees that write's own uncommitted
changes, unless new_transaction=True asks for a genuinely independent
read.

Split out of relay/__init__.py for maintainability — ReadsMixin is one of
several mixins RelayProvider (relay/__init__.py) inherits from; `self` is
the single shared RelayProvider instance everywhere, including
`self._connection` (defined on TransactionsMixin, relay/transactions.py).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from . import query
from .resolvers import FieldResolver

# list()/list_page()'s row cap (§9/§3 — the doc's own "the day a 2M-row
# table meets a UI that forgot `limit`" scenario), configurable per project
# via arc.settings (declared in register(), relay/__init__.py, plain — not
# secret, there's nothing sensitive about a row-count ceiling).
#
#   * RELAY_LIST_DEFAULT_LIMIT_KEY — used when a caller doesn't pass
#     `limit` at all. Callers that genuinely need everything pass
#     limit=None explicitly — that escape hatch is untouched by either
#     setting, and always means "no cap," never something a client can
#     reach through a query-string value (a whitelisted function author
#     has to choose it in code).
#   * RELAY_LIST_MAX_LIMIT_KEY — a hard ceiling on any EXPLICIT `limit`,
#     scoped to list()/list_page() only (see _resolve_and_cap_list_limit).
#     Deliberately NOT enforced on `get()`'s own internal limit=1, or on
#     save()/save_many()'s match_on resolution lookups — those are
#     already-bounded internal correctness mechanisms, not the "someone
#     forgot a limit on a browse endpoint" case this setting exists for.
#
# The constants below are only the DECLARED DEFAULTS for those two
# settings (used the first time a project boots, before anyone has run
# `arc settings set` for either key) — the numbers actually in effect
# always come from self._list_default_limit/_list_max_limit, resolved once
# in register() from arc.settings, same pattern as psqldb's own
# POOL_MIN_SIZE_KEY/POOL_MAX_SIZE_KEY.
RELAY_LIST_DEFAULT_LIMIT_KEY = "relay_list_default_limit"
RELAY_LIST_MAX_LIMIT_KEY = "relay_list_max_limit"
DEFAULT_LIST_LIMIT = 20
MAX_LIST_LIMIT = 1000

# The Python-level sentinel for "the caller passed no `limit` argument at
# all" — distinct from an EXPLICIT `limit=None` (which already has its own
# meaning: no cap, ever, unconditionally). A plain `= DEFAULT_LIST_LIMIT`
# default can't be used instead: the actual number to fall back to is a
# per-project SETTING, resolved once at boot onto the instance
# (self._list_default_limit), not a fixed value known at class-definition
# time.
_UNSET_LIMIT = object()


class ReadsMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _psqldb: Any
    _list_default_limit: int
    _list_max_limit: int

    def all_columns(self, table: str) -> list[str]:
        """Every real column's name on `table` — for the internal, never-
        returned-to-a-client callers that genuinely need the full row
        (most single-row lookups a hook or an API function does purely to
        read a few of its OWN fields back out in Python, e.g. authn
        reading `password_hash` off `_users` to verify a login). `fields`
        is required on every read now (see query.build_select) — this is
        the explicit, greppable way to say "give me everything," visibly
        different from a caller that names a short, deliberate list
        because it actually only needs a few columns and is about to hand
        the result to an untrusted response body. Prefer naming exact
        fields wherever the caller can — this exists so a call site that
        genuinely needs the whole row isn't forced to hand-copy and
        maintain its own column list that silently goes stale the next
        time that schema changes."""
        return [f.name for f in self._psqldb.schema(table).column_fields()]

    def _resolve_and_cap_list_limit(self, limit: Any) -> int | None:
        """The one place list()/list_page()'s `limit` argument is turned
        into a concrete value — see the module-level RELAY_LIST_*_KEY
        settings and _UNSET_LIMIT for the full reasoning.

          * not given at all (_UNSET_LIMIT) -> the configured default.
          * explicit `None` -> unchanged: unconditionally unbounded, a
            developer's own deliberate choice, never client-reachable.
          * any other explicit value -> checked against the configured
            max, raising a clear QueryError rather than a silent
            truncation if it's over.

        Deliberately a plain method on RelayProvider, not a check inside
        query.py's build_select — query.py stays a pure function of
        (schema, arguments) with no settings/config dependency at all
        (its own module docstring), and this check only ever applies to
        list()/list_page() specifically, never to get()'s internal
        limit=1 or to save()/save_many()'s own match_on resolution
        lookups, which are a different, already-bounded concern."""
        if limit is _UNSET_LIMIT:
            return self._list_default_limit
        if limit is not None and limit > self._list_max_limit:
            raise query.QueryError(
                f"limit {limit} exceeds the maximum of {self._list_max_limit} allowed "
                f"(see the '{RELAY_LIST_MAX_LIMIT_KEY}' setting) — pass limit=None if "
                f"you genuinely need every matching row."
            )
        return limit

    async def get(
        self,
        table: str,
        key: UUID | str | dict[str, Any],
        fields: list[str | query.Resolve | FieldResolver] | None = None,
        *,
        new_transaction: bool = False,
    ) -> dict | None:
        """Single row, by id (`key` a UUID/str) or by any other field(s)
        (`key` a dict of equality filters, first match) — replaces the old
        separate get()/get_by(). `fields` is the same query-engine-native
        projection list() takes, including arc.relay.resolve(...) entries
        — REQUIRED, no wildcard (see query.build_select): name the exact
        columns you want, or pass arc.relay.all_columns(table) for the
        whole row.

            await arc.relay.get("employee", employee_id, ["full_name"])       # by id
            await arc.relay.get("employee", {"employee_code": "E001"}, ["full_name"])
            await arc.relay.get("employee", {"employee_code": "E001"},
                                 ["full_name", arc.relay.resolve("department", ["dept_name", "code"])])
        """
        self._psqldb.schema(table)  # SchemaError early if unknown, before building anything
        filters = key if isinstance(key, dict) else {"id": key}
        rows = await self._select(
            table,
            filters=filters,
            fields=fields,
            order_by=None,
            limit=1,
            offset=0,
            distinct=False,
            new_transaction=new_transaction,
        )
        return rows[0] if rows else None

    async def list(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None = None,
        fields: list[str | query.Resolve | FieldResolver] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = _UNSET_LIMIT,
        offset: int = 0,
        distinct: bool = False,
        new_transaction: bool = False,
    ) -> list[dict]:
        """Multiple rows, capped at the configured default (20 out of the
        box, `relay_list_default_limit` setting) unless told otherwise —
        protects the caller's future self from the day a table that used
        to be small enough to fetch whole isn't anymore. Pass an explicit
        `limit` to change the cap for this call (rejected with a clear
        QueryError past the configured max — `relay_list_max_limit`,
        1000 out of the box), or `limit=None` to mean it literally: fetch
        everything that matches `filters`, no cap at all — still
        available, just no longer the default."""
        self._psqldb.schema(table)
        return await self._select(
            table,
            filters=filters,
            fields=fields,
            order_by=order_by,
            limit=self._resolve_and_cap_list_limit(limit),
            offset=offset,
            distinct=distinct,
            new_transaction=new_transaction,
        )

    async def _select(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None,
        fields: list[str | query.Resolve | FieldResolver] | None,
        order_by: list[str] | None,
        limit: int | None,
        offset: int,
        distinct: bool,
        new_transaction: bool = False,
    ) -> list[dict]:
        schema = self._psqldb.schema(table)
        sql, params = query.build_select(
            table,
            schema,
            filters=filters,
            fields=fields,
            order_by=order_by,
            limit=limit,
            offset=offset,
            distinct=distinct,
            ref_columns=self._psqldb.ref_columns(),
            schema_lookup=self._psqldb.schema,
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            rows = await conn.fetch(sql, *params)
        shaped = [self._shape_row(dict(r), fields) for r in rows]
        await self._resolve_fields(shaped, fields)
        return shaped

    @staticmethod
    def _shape_row(flat: dict, fields: list[str | query.Resolve | FieldResolver]) -> dict:
        """Re-nests each arc.relay.resolve(...) entry's flat
        "field.subfield" column aliases back into row[field] = {subfield:
        value, ...}; a FieldResolver entry (e.g. arc.filer.url(...)) passes
        its raw column value through unchanged here — _resolve_fields
        overwrites it with the resolved value afterward. Everything else
        (plain column names) passes through as a top-level key. `fields`
        is always a real, non-empty list by the time this runs —
        query.build_select already rejects None/[] before either caller
        (_select/list_page) reaches this call."""
        out: dict[str, Any] = {}
        for item in fields:
            if isinstance(item, query.Resolve):
                out[item.field] = {sf: flat.get(f"{item.field}.{sf}") for sf in item.subfields}
            elif isinstance(item, FieldResolver):
                out[item.field] = flat.get(item.field)
            else:
                out[item] = flat.get(item)
        return out

    @staticmethod
    async def _resolve_fields(
        rows: list[dict], fields: list[str | query.Resolve | FieldResolver]
    ) -> None:
        """Applies every FieldResolver marker in `fields`, in place. Each
        resolver's prepare() runs exactly ONCE per query — regardless of
        row count — with every row's raw value for its field already
        collected; resolve() then runs per row against that shared
        context. This is what keeps arc.filer.url() (and any future
        FieldResolver) free of N+1 queries — see relay.resolvers."""
        for item in fields:
            if not isinstance(item, FieldResolver):
                continue
            raw_values = [row[item.field] for row in rows]
            context = await item.prepare(raw_values)
            for row in rows:
                row[item.field] = item.resolve(row[item.field], context)

    async def count(
        self, table: str, *, filters: dict[str, Any] | None = None, new_transaction: bool = False
    ) -> int:
        schema = self._psqldb.schema(table)
        sql, params = query.build_count(
            table, schema, filters=filters, ref_columns=self._psqldb.ref_columns()
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            return await conn.fetchval(sql, *params)

    async def exists(
        self,
        table: str,
        key: UUID | str | dict[str, Any],
        *,
        new_transaction: bool = False,
    ) -> bool:
        """Same lookup shape as get() — `key` a UUID/str means "by id", a
        dict means the same equality/operator filter dict list()/count()
        take (get()'s own `filters={"id": key}` sugar, reused here
        identically) — but answers a yes/no question via SQL
        EXISTS(... LIMIT 1) instead of fetching and materializing a row:
        cheaper on a filter that matches many rows, since Postgres can
        stop at the very first match rather than get() reading (and
        deserializing) a whole row's worth of columns just to check it's
        not None."""
        schema = self._psqldb.schema(table)
        filters = key if isinstance(key, dict) else {"id": key}
        sql, params = query.build_exists(
            table, schema, filters=filters, ref_columns=self._psqldb.ref_columns()
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            return await conn.fetchval(sql, *params)

    async def list_page(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None = None,
        fields: list[str | query.Resolve | FieldResolver] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = _UNSET_LIMIT,
        offset: int = 0,
        distinct: bool = False,
        new_transaction: bool = False,
    ) -> tuple[list[dict], int]:
        """(rows, total) — the same rows list() would return for these
        exact arguments, plus a total count of every row matching
        `filters` regardless of limit/offset, so a caller can tell "is
        there more" without a second, separately-written count() call
        (and without the two ever silently drifting out of sync on
        filters, since both are built from the same `filters` here).

        Deliberately NOT a dict with fixed key names like {"rows": ...,
        "total": ...} — that would be dictating an HTTP response envelope
        from inside a plain data-access method, which isn't this layer's
        job; list()/count()/aggregate() return plain data with no opinion
        about wire shape either, and this stays consistent with them. The
        caller (a whitelisted function, usually) decides what its own
        response actually looks like.

        Also deliberately not "the" pagination mechanism — this is the
        OFFSET+total convenience specifically. A caller that wants
        cursor-based paging (no COUNT(*) at all — cheaper and more
        consistent under concurrent writes at real scale) already has
        everything it needs in plain list() + filters + order_by (e.g.
        filters={"id": {"gt": last_seen_id}}); it never needs this method.

        Runs both queries on ONE shared connection rather than two
        separate pool checkouts (still two queries, not one round-trip —
        no SELECT ... count(*) OVER() window-function trick here; revisit
        only if profiling ever shows the extra round-trip actually
        matters)."""
        schema = self._psqldb.schema(table)
        limit = self._resolve_and_cap_list_limit(limit)
        select_sql, select_params = query.build_select(
            table,
            schema,
            filters=filters,
            fields=fields,
            order_by=order_by,
            limit=limit,
            offset=offset,
            distinct=distinct,
            ref_columns=self._psqldb.ref_columns(),
            schema_lookup=self._psqldb.schema,
        )
        count_sql, count_params = query.build_count(
            table, schema, filters=filters, ref_columns=self._psqldb.ref_columns()
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            rows = await conn.fetch(select_sql, *select_params)
            total = await conn.fetchval(count_sql, *count_params)
        shaped = [self._shape_row(dict(r), fields) for r in rows]
        await self._resolve_fields(shaped, fields)
        return shaped, total

    async def aggregate(
        self,
        table: str,
        *,
        group_by: list[str] | None = None,
        aggregates: dict[str, tuple[str, str]],
        filters: dict[str, Any] | None = None,
        new_transaction: bool = False,
    ) -> dict | list[dict]:
        """`aggregates` maps output name -> (function, field), field="*" only
        valid with "count". No `group_by` -> a single dict (the whole
        table's aggregate). `group_by` given -> a list of dicts, one per
        group. No HAVING, no resolve() — see query.py's module docstring."""
        schema = self._psqldb.schema(table)
        sql, params = query.build_aggregate(
            table,
            schema,
            group_by=group_by,
            aggregates=aggregates,
            filters=filters,
            ref_columns=self._psqldb.ref_columns(),
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            rows = [dict(r) for r in await conn.fetch(sql, *params)]
        if not group_by:
            return rows[0] if rows else {}
        return rows

    def resolve(self, field: str, subfields: list[str]) -> query.Resolve:
        """Use inside a `fields=[...]` list to pull named columns off a
        REFERENCE field's related row, one hop, e.g.
        `fields=["full_name", arc.relay.resolve("department", ["dept_name", "code"])]`.
        Not a join through filters/order_by — those stay local-column-only
        on purpose (see query.py's module docstring)."""
        return query.Resolve(field=field, subfields=tuple(subfields))

    async def sql(self, statement: str, *params: Any, new_transaction: bool = False) -> list[dict]:
        """The raw-SQL escape hatch (docs/arc.MD §3.4) for the ~20% of
        queries the bounded engine above deliberately doesn't try to cover —
        parameters always bound, exactly like every other primitive in this
        system; there is no string-formatting path anywhere in ARC's SQL."""
        async with self._connection(new_transaction=new_transaction) as conn:
            rows = await conn.fetch(statement, *params)
        return [dict(r) for r in rows]

    async def sql_one(
        self, statement: str, *params: Any, new_transaction: bool = False
    ) -> dict | None:
        async with self._connection(new_transaction=new_transaction) as conn:
            row = await conn.fetchrow(statement, *params)
        return dict(row) if row else None

    async def sql_val(self, statement: str, *params: Any, new_transaction: bool = False) -> Any:
        async with self._connection(new_transaction=new_transaction) as conn:
            return await conn.fetchval(statement, *params)
