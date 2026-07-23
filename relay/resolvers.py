"""
relay.resolvers
-------------------
Generic post-fetch field-resolver hook for arc.relay.list()/get()'s
`fields=[...]` list — lets another capability (filer's arc.filer.url(),
and any future one) contribute a computed value for a column without
relay importing that capability. Mirrors query.Resolve's role as a marker
placed inside `fields`, but for values that need capability-specific
logic instead of a SQL join.

The split into prepare()/resolve() is what keeps this N+1-safe: relay
calls prepare() exactly ONCE per resolver instance per list()/get() call,
with every row's raw value for that field already collected — a resolver
needing a DB lookup (e.g. filer batching a `filerfile` query for every
file_id across every row) does it there, in one round trip, regardless of
row count. resolve() then runs per row against that shared context and
must be pure — no I/O, no further queries.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class FieldResolver(ABC):
    """Base class for a `fields=[...]` marker that resolves `field`'s raw
    column value into something else after the row fetch. Subclass this,
    set `field` to the underlying column name (selected from SQL exactly
    like a plain field name — see query.build_select), and implement
    prepare()/resolve() below."""

    field: str

    @abstractmethod
    async def prepare(self, raw_values: list[Any]) -> Any:
        """Called once per query with every row's raw value for `field`
        (not deduplicated — do that yourself if it matters). Return
        whatever context resolve() needs, e.g. a dict keyed by id."""
        ...

    @abstractmethod
    def resolve(self, raw_value: Any, context: Any) -> Any:
        """Pure, per-row — no I/O. Anything requiring a lookup belongs in
        prepare()'s single batched call, not here."""
        ...
