"""CRUD — single-row and batch writes. Every write runs inside ONE
transaction spanning precommit hooks + the actual pgdb write; postcommit
hooks run strictly after that transaction has resolved AND its connection
has been released back to the pool (ctx.conn is None by then on purpose —
see HookContext — and postcommit hooks never share the ambient connection
either, since by definition there's no live transaction left to share).

create()/update() from earlier cuts are now private (_insert/_update) —
save() below is the one public entry point (docs/arc-relay.MD §2.1). Not
kept as deprecated aliases: this project carries no version number and no
back-compat promise yet (docs/arc.MD §1).

Batch writes run one shared transaction per BATCH (all-or-nothing, same
atomic mental model as a single save/delete) — not one transaction per
row. Hooks still fire ONCE PER ROW, not once for the whole batch: a hook
written for save() works unmodified for save_many() with no special
"batch mode" to author against. Precommit hooks (validate/before_save)
run for every row BEFORE the batched SQL statement(s) execute, so any
row's hook can still abort the entire batch by raising; after_save/
after_commit/on_rollback then run per row using the real per-row result.

create_many()/update_many() are now private (_insert_many/_update_many) —
save_many() below is the one public entry point. get_many() is gone
entirely: it was a special case of list(table, filters={"id": {"in": ids}}),
not a distinct capability.

Split out of relay/__init__.py for maintainability — CrudMixin is one of
several mixins RelayProvider (relay/__init__.py) inherits from; `self` is
the single shared RelayProvider instance everywhere, including
`self._has_hooks`/`self._run_hooks` (HooksMixin, relay/hooks.py),
`self._postcommit_scope`/`self._write_transaction`/
`self._transaction_or_dry_run` (TransactionsMixin, relay/transactions.py),
`self.list`/`self._select` (ReadsMixin, relay/reads.py), and `self.lock`
(CacheLockMixin, relay/cache_lock.py).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from . import query
from .ambient import _dry_run, _skip_hook_events
from .shapes import (
    POSTCOMMIT_EVENTS,
    PRECOMMIT_EVENTS,
    HookContext,
    _delete_doc,
    _postwrite_doc,
    _precommit_doc,
)


def _resolve_skip_events(skip_hooks: bool, **flags: bool) -> frozenset[str]:
    """Turns save()/save_many()/delete()/delete_many()'s own skip_hooks +
    per-event skip_<event> kwargs into the frozenset _run_hooks/
    _run_hooks_resolved/_has_hooks actually check. `skip_hooks=True` short-
    circuits to every hook event that exists at all — harmless for, say,
    save() to include before_delete/after_delete in that set, since those
    can never fire from a save() call regardless. Otherwise, only the
    individually-named flags that were actually passed True. One small
    function rather than four copies of the same "which ones are True"
    logic, one per write method."""
    if skip_hooks:
        return PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS
    return frozenset(event for event, skip in flags.items() if skip)


class CrudMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind this.
    _psqldb: Any

    async def _insert(
        self, table: str, data: dict, *, by: str | None = None, new_transaction: bool = False
    ) -> dict:
        self._psqldb.schema(table)  # SchemaError early if unknown, before opening anything
        ctx = HookContext(table=table, old=None, payload=dict(data))
        ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
        dry_run = _dry_run.get()
        async with self._postcommit_scope(
            new_transaction=new_transaction, dry_run=dry_run
        ) as deferred:
            async with self._write_transaction(new_transaction=new_transaction) as conn:
                try:
                    async with self._transaction_or_dry_run(conn, dry_run):
                        ctx.conn = conn
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                        row = await self._psqldb.insert(table, ctx.payload, created_by=by, conn=conn)
                        ctx.new = row  # pgdb.insert() already returns a plain dict
                        ctx.doc = _postwrite_doc(ctx.new, None, is_new=True)
                        await self._run_hooks(table, "after_save", ctx)
                except Exception as exc:
                    ctx.conn = None
                    ctx.error = exc
                    deferred.append((table, ctx, False))
                    raise
                ctx.conn = None
            deferred.append((table, ctx, True))
        return ctx.new

    async def _update(
        self,
        table: str,
        id: UUID,
        data: dict,
        *,
        by: str | None = None,
        new_transaction: bool = False,
    ) -> dict | None:
        self._psqldb.schema(table)
        ctx = HookContext(table=table, payload=dict(data))
        dry_run = _dry_run.get()
        async with self._postcommit_scope(
            new_transaction=new_transaction, dry_run=dry_run
        ) as deferred:
            async with self._write_transaction(new_transaction=new_transaction) as conn:
                try:
                    async with self._transaction_or_dry_run(conn, dry_run):
                        ctx.conn = conn
                        if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                            # pgdb.get() already returns a plain dict (or None)
                            ctx.old = await self._psqldb.get(table, id, conn=conn)
                        ctx.doc = _precommit_doc(ctx.old, ctx.payload, is_new=False)
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                        # pgdb.update() already returns a plain dict (or None)
                        ctx.new = await self._psqldb.update(
                            table, id, ctx.payload, updated_by=by, conn=conn
                        )
                        ctx.doc = _postwrite_doc(ctx.new, ctx.doc.old, is_new=False)
                        await self._run_hooks(table, "after_save", ctx)
                except Exception as exc:
                    ctx.conn = None
                    ctx.error = exc
                    deferred.append((table, ctx, False))
                    raise
                ctx.conn = None
            deferred.append((table, ctx, True))
        return ctx.new

    def _match_on_is_constraint_backed(self, table: str, match_on: list[str]) -> bool:
        """Whether `match_on` is backed by a REAL Postgres constraint —
        either a single field declared `"unique": true`, or an exact
        `unique_together` group (field set match, order-independent) — as
        opposed to an arbitrary set of columns with no database-level
        guarantee behind them at all. See save()'s own docstring for what
        this changes."""
        schema = self._psqldb.schema(table)
        if len(match_on) == 1:
            only = match_on[0]
            if any(f.name == only and f.unique for f in schema.fields):
                return True
        match_set = set(match_on)
        return any(set(ut["fields"]) == match_set for ut in schema.unique_together)

    async def _resolve_match_on(
        self,
        table: str,
        data: dict,
        match_on: list[str],
        filters: dict,
        *,
        allow_insert: bool,
        allow_update: bool,
        by: str | None,
        new_transaction: bool,
    ) -> dict:
        """The shared lookup-then-branch body save() runs either with or
        without its own lock wrapped around it — see save() for which.
        allow_insert/allow_update are checked HERE, not by the caller,
        because which one applies depends on what this lookup finds —
        save() itself doesn't know insert-vs-update for a match_on call
        until this runs."""
        matches = await self.list(
            table, filters=filters, fields=["id"], limit=2, new_transaction=new_transaction
        )
        if len(matches) > 1:
            raise query.QueryError(
                f"save(): match_on {filters} matched more than one row on '{table}' — "
                f"save() only ever affects exactly one row; use save_many() for a bulk match."
            )
        if matches:
            if not allow_update:
                raise query.QueryError(
                    f"save(): match_on {filters} matched an existing row on '{table}', but "
                    f"allow_update=False — refusing to update it."
                )
            payload = {k: v for k, v in data.items() if k not in match_on}
            return await self._update(
                table, matches[0]["id"], payload, by=by, new_transaction=new_transaction
            )
        if not allow_insert:
            raise query.QueryError(
                f"save(): match_on {filters} matched no existing row on '{table}', but "
                f"allow_insert=False — refusing to insert a new one."
            )
        # The insert MUST happen while any lock the caller took is still
        # held — it used to sit after this block, so the lock was released
        # between "saw no match" and "inserted", and two concurrent saves
        # could both reach here and both insert: the exact race the lock
        # exists to prevent for a match_on with no real constraint behind it.
        payload = {k: v for k, v in data.items() if k != "id"}
        return await self._insert(table, payload, by=by, new_transaction=new_transaction)

    async def save(
        self,
        table: str,
        data: dict,
        *,
        match_on: list[str] | None = None,
        allow_insert: bool = True,
        allow_update: bool = True,
        by: str | None = None,
        new_transaction: bool = False,
        skip_hooks: bool = False,
        skip_validate: bool = False,
        skip_before_save: bool = False,
        skip_after_save: bool = False,
        skip_after_commit: bool = False,
        skip_on_rollback: bool = False,
    ) -> dict:
        """Insert-or-update, one method (docs/arc-relay.MD §2.1).

        allow_insert/allow_update (both default True — today's original
        behavior, unchanged) let a caller block one side of that decision:
        allow_insert=False for an "edit this record" endpoint that must
        never silently create a new row if id/match_on didn't resolve to
        one; allow_update=False for an import that should never overwrite
        something that already exists. Checked AFTER the match_on lookup
        (which branch would fire isn't known before that runs) but BEFORE
        any hook or write — a blocked attempt never partially happens.
        Passing both False is rejected immediately (QueryError): no call
        could ever succeed with neither allowed. An explicit `id` still
        always means "update, or a hard error if that id doesn't exist"
        regardless of allow_insert — allow_insert never turns a not-found
        id into an insert, it only gates the two branches that were
        already capable of inserting (match_on-no-match, and
        no-id-no-match_on).

        skip_hooks=True skips every hook this call would otherwise fire,
        for a bulk/import-style caller that wants raw pgdb-speed writes
        without losing save()'s own match_on/lock/upsert conveniences.
        The individual skip_validate/skip_before_save/skip_after_save/
        skip_after_commit/skip_on_rollback flags are for the narrower
        case — e.g. "run validate (still reject bad data) but don't fire
        after_save's side effects (an email, an event) for this one
        import." Default is False/unset everywhere: nothing skipped,
        today's original behavior, byte-for-byte. Scoped to exactly this
        call — a hook this call itself triggers that makes its OWN nested
        save()/delete() call does not inherit these flags; skipping is
        explicit per call, never silently inherited.

        `data["id"]` present -> update that row directly, no match_on
        needed — a hard QueryError if no row has that id (a stale id
        silently doing nothing, and returning None, is the one behavior
        nobody would choose from an "insert-or-update" contract).
        Otherwise, `match_on` given -> look up by those field(s):
        0 matches -> insert, 1 match -> update, MORE than 1 -> a hard
        error — save() is a strictly single-row contract; use save_many()
        for an intentional fan-out. Neither id nor match_on -> plain
        insert.

        match_on doesn't have to name a DB-unique field or a declared
        `unique_together` group — if you want that enforced even for
        direct create()/save() calls bypassing this lookup, that's a
        `validate` hook's job, not this method's. When it ISN'T backed by
        a real constraint, this method guarantees the ENTIRE lookup-then-
        branch sequence — including the no-match insert — runs inside one
        arc.relay.lock(), so two concurrent save() calls for the SAME
        match_on values can't both see "no match" and both insert — the
        classic upsert race. (Real guarantee with redix installed; same
        weaker in-process-only fallback as every other arc.relay.lock()
        use otherwise — see lock()'s own docstring.)

        When match_on DOES exactly match a declared unique field or
        `unique_together` group, the lock is skipped entirely — Postgres's
        own constraint already guarantees no duplicate can exist, in every
        process, lock or no lock, which is a STRONGER guarantee than the
        in-process-only fallback lock ever gave without redix installed
        (two callers in two different Gateway worker processes could
        previously race past it and create a genuine duplicate row). The
        rare loser of that race now gets a friendly `ValidationError`
        (pgdb.validation.friendly_unique_error) instead of a lock wait —
        not a single atomic statement (this is still select-then-branch,
        just without the lock), a deliberate, smaller-scope fix: a true
        one-statement `INSERT ... ON CONFLICT` upsert would need to decide
        insert-vs-update before precommit hooks run, which breaks their
        existing `ctx.old`/`doc.is_new` contract — left as a documented,
        separate future increment, not silently attempted here.
        """
        if not allow_insert and not allow_update:
            raise query.QueryError(
                "save(): allow_insert and allow_update cannot both be False — "
                "at least one must stay True for this call to ever succeed."
            )
        skip = _resolve_skip_events(
            skip_hooks,
            validate=skip_validate,
            before_save=skip_before_save,
            after_save=skip_after_save,
            after_commit=skip_after_commit,
            on_rollback=skip_on_rollback,
        )
        token = _skip_hook_events.set(skip)
        try:
            if data.get("id") is not None:
                if not allow_update:
                    raise query.QueryError(
                        f"save(): '{table}' has allow_update=False, but an explicit id "
                        f"always means update — omit the id (or use match_on) to insert instead."
                    )
                payload = {k: v for k, v in data.items() if k != "id"}
                updated = await self._update(
                    table, data["id"], payload, by=by, new_transaction=new_transaction
                )
                if updated is None:
                    raise query.QueryError(
                        f"save(): no row on '{table}' with id {data['id']!r} — an explicit id "
                        f"always means update; to insert, omit the id (or use match_on)."
                    )
                return updated

            if match_on:
                filters = {f: data[f] for f in match_on}
                if self._match_on_is_constraint_backed(table, match_on):
                    return await self._resolve_match_on(
                        table,
                        data,
                        match_on,
                        filters,
                        allow_insert=allow_insert,
                        allow_update=allow_update,
                        by=by,
                        new_transaction=new_transaction,
                    )
                # str() every value so the same logical key always locks the
                # same name regardless of how the caller spelled it (UUID vs
                # str, ...).
                lock_key = (
                    f"relay:save:{table}:{sorted((k, str(v)) for k, v in filters.items())}"
                )
                async with self.lock(lock_key):
                    return await self._resolve_match_on(
                        table,
                        data,
                        match_on,
                        filters,
                        allow_insert=allow_insert,
                        allow_update=allow_update,
                        by=by,
                        new_transaction=new_transaction,
                    )

            if not allow_insert:
                raise query.QueryError(
                    f"save(): '{table}' has allow_insert=False, and no id/match_on was given "
                    f"to update by — nothing to update, and inserting is not allowed."
                )
            payload = {k: v for k, v in data.items() if k != "id"}
            return await self._insert(table, payload, by=by, new_transaction=new_transaction)
        finally:
            _skip_hook_events.reset(token)

    async def delete(
        self,
        table: str,
        id: UUID,
        *,
        by: str | None = None,
        new_transaction: bool = False,
        skip_hooks: bool = False,
        skip_before_delete: bool = False,
        skip_after_delete: bool = False,
        skip_after_commit: bool = False,
        skip_on_rollback: bool = False,
    ) -> None:
        """skip_hooks/skip_<event> — see save()'s own docstring for the
        full reasoning; same scoped-to-this-call, opt-in-only semantics,
        just against this method's own before_delete/after_delete/
        after_commit/on_rollback events (delete() never fires validate/
        before_save/after_save at all, so there's no skip flag for them
        here)."""
        skip = _resolve_skip_events(
            skip_hooks,
            before_delete=skip_before_delete,
            after_delete=skip_after_delete,
            after_commit=skip_after_commit,
            on_rollback=skip_on_rollback,
        )
        token = _skip_hook_events.set(skip)
        try:
            self._psqldb.schema(table)
            ctx = HookContext(table=table)
            dry_run = _dry_run.get()
            async with self._postcommit_scope(
                new_transaction=new_transaction, dry_run=dry_run
            ) as deferred:
                async with self._write_transaction(new_transaction=new_transaction) as conn:
                    try:
                        async with self._transaction_or_dry_run(conn, dry_run):
                            ctx.conn = conn
                            if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                                # pgdb.get() already returns a plain dict (or None)
                                ctx.old = await self._psqldb.get(table, id, conn=conn)
                            ctx.doc = _delete_doc(ctx.old)
                            await self._run_hooks(table, "before_delete", ctx)
                            await self._psqldb.soft_delete(table, id, deleted_by=by, conn=conn)
                            await self._run_hooks(table, "after_delete", ctx)
                    except Exception as exc:
                        ctx.conn = None
                        ctx.error = exc
                        deferred.append((table, ctx, False))
                        raise
                    ctx.conn = None
                deferred.append((table, ctx, True))
        finally:
            _skip_hook_events.reset(token)

    async def _insert_many(
        self, table: str, rows: list[dict], *, by: str | None = None, new_transaction: bool = False
    ) -> list[dict]:
        self._psqldb.schema(table)
        if not rows:
            return []
        ctxs = [HookContext(table=table, old=None, payload=dict(r)) for r in rows]
        for ctx in ctxs:
            ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
        dry_run = _dry_run.get()
        async with self._postcommit_scope(
            new_transaction=new_transaction, dry_run=dry_run
        ) as deferred:
            async with self._write_transaction(new_transaction=new_transaction) as conn:
                try:
                    async with self._transaction_or_dry_run(conn, dry_run):
                        for ctx in ctxs:
                            ctx.conn = conn
                            await self._run_hooks(table, "validate", ctx)
                            await self._run_hooks(table, "before_save", ctx)
                        results = await self._psqldb.insert_many(
                            table, [ctx.payload for ctx in ctxs], created_by=by, conn=conn
                        )
                        for ctx, row in zip(ctxs, results):
                            ctx.new = row  # pgdb.insert_many() already returns plain dicts
                            ctx.doc = _postwrite_doc(ctx.new, None, is_new=True)
                        for ctx in ctxs:
                            await self._run_hooks(table, "after_save", ctx)
                except Exception as exc:
                    for ctx in ctxs:
                        ctx.conn = None
                        ctx.error = exc
                        deferred.append((table, ctx, False))
                    raise
                for ctx in ctxs:
                    ctx.conn = None
            for ctx in ctxs:
                deferred.append((table, ctx, True))
        return [ctx.new for ctx in ctxs]

    async def _update_many(
        self,
        table: str,
        updates: list[dict],
        *,
        by: str | None = None,
        new_transaction: bool = False,
    ) -> list[dict]:
        """`updates` is `[{"id": ..., "data": {...}}, ...]`."""
        self._psqldb.schema(table)
        if not updates:
            return []
        need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
        ctxs = [HookContext(table=table, payload=dict(u["data"])) for u in updates]
        ids = [u["id"] for u in updates]
        dry_run = _dry_run.get()
        async with self._postcommit_scope(
            new_transaction=new_transaction, dry_run=dry_run
        ) as deferred:
            async with self._write_transaction(new_transaction=new_transaction) as conn:
                try:
                    async with self._transaction_or_dry_run(conn, dry_run):
                        if need_old:
                            old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                            # pgdb.get_many() already returns plain dicts
                            old_by_id = {r["id"]: r for r in old_rows}
                            for ctx, row_id in zip(ctxs, ids):
                                ctx.old = old_by_id.get(row_id)
                        for ctx in ctxs:
                            ctx.doc = _precommit_doc(ctx.old, ctx.payload, is_new=False)
                        for ctx in ctxs:
                            ctx.conn = conn
                            await self._run_hooks(table, "validate", ctx)
                            await self._run_hooks(table, "before_save", ctx)
                        results = await self._psqldb.update_many(
                            table,
                            [{"id": row_id, "data": ctx.payload} for row_id, ctx in zip(ids, ctxs)],
                            updated_by=by,
                            conn=conn,
                        )
                        # pgdb.update_many() already returns plain dicts
                        results_by_id = {r["id"]: r for r in results}
                        for ctx, row_id in zip(ctxs, ids):
                            ctx.new = results_by_id.get(row_id)
                            ctx.doc = _postwrite_doc(ctx.new, ctx.doc.old, is_new=False)
                        for ctx in ctxs:
                            await self._run_hooks(table, "after_save", ctx)
                except Exception as exc:
                    for ctx in ctxs:
                        ctx.conn = None
                        ctx.error = exc
                        deferred.append((table, ctx, False))
                    raise
                for ctx in ctxs:
                    ctx.conn = None
            for ctx in ctxs:
                deferred.append((table, ctx, True))
        return [ctx.new for ctx in ctxs]

    async def save_many(
        self,
        table: str,
        rows: list[dict],
        *,
        match_on: list[str] | None = None,
        allow_insert: bool = True,
        allow_update: bool = True,
        by: str | None = None,
        limit: int | None = None,
        new_transaction: bool = False,
        skip_hooks: bool = False,
        skip_validate: bool = False,
        skip_before_save: bool = False,
        skip_after_save: bool = False,
        skip_after_commit: bool = False,
        skip_on_rollback: bool = False,
    ) -> list[dict]:
        """Bulk insert-or-update, one shared transaction for the whole
        batch. Per row: `id` present -> update that row directly. No id,
        `match_on` given -> matched against existing rows on those
        field(s) — unlike save(), ALL matches get updated (an intentional
        fan-out, not an error). `limit`, if given, caps how many existing
        rows one row's match_on is allowed to affect at once — omit it and
        a broader-than-expected match_on updates everything it matches, no
        implicit cap (same "no cap unless you ask for one" philosophy as
        list()'s own `limit`). Neither id nor match_on -> insert.

        allow_insert/allow_update (both default True) — same meaning as
        save()'s own, but checked ONCE for the WHOLE batch after every
        row's insert-or-update kind is resolved (id, then match_on
        fan-out), before any hook or write runs: if allow_insert=False and
        even one row in the batch would have been inserted, or
        allow_update=False and even one row would have been updated, the
        ENTIRE batch is rejected — consistent with save_many()'s existing
        all-or-nothing contract (one row's problem already aborts
        everything else here; this is just one more way a row can have a
        problem). Passing both False is rejected immediately, same as
        save(). An explicit `id` on a row still always means update for
        that row regardless of allow_insert, same as save().

        Known simplification, not an oversight: unlike save(), this does
        NOT take a per-key arc.relay.lock() around match_on resolution —
        it's meant for bulk/import-style work, already inside one shared
        transaction, not a guarantee against a CONCURRENT save()/save_many()
        call racing the exact same match_on values. Add that locking here
        too if it ever becomes a real scenario, not before.

        skip_hooks/skip_validate/skip_before_save/skip_after_save/
        skip_after_commit/skip_on_rollback — see save()'s own docstring
        for the full reasoning; applies uniformly across the WHOLE batch
        (one skip decision per call, not per row) — exactly what a bulk
        import wants.
        """
        if not allow_insert and not allow_update:
            raise query.QueryError(
                "save_many(): allow_insert and allow_update cannot both be False — "
                "at least one must stay True for this call to ever succeed."
            )
        skip = _resolve_skip_events(
            skip_hooks,
            validate=skip_validate,
            before_save=skip_before_save,
            after_save=skip_after_save,
            after_commit=skip_after_commit,
            on_rollback=skip_on_rollback,
        )
        token = _skip_hook_events.set(skip)
        try:
            self._psqldb.schema(table)
            if not rows:
                return []
            insert_ctxs: list[HookContext] = []
            update_ctxs: list[HookContext] = []
            dry_run = _dry_run.get()
            async with self._postcommit_scope(
                new_transaction=new_transaction, dry_run=dry_run
            ) as deferred:
                async with self._write_transaction(new_transaction=new_transaction) as conn:
                    try:
                        async with self._transaction_or_dry_run(conn, dry_run):
                            resolved: list[tuple[str, UUID | None, dict]] = []
                            pending_match: list[dict] = []
                            for row in rows:
                                row_id = row.get("id")
                                if row_id is not None:
                                    resolved.append(
                                        (
                                            "update",
                                            row_id,
                                            {k: v for k, v in row.items() if k != "id"},
                                        )
                                    )
                                elif match_on:
                                    pending_match.append(row)
                                else:
                                    resolved.append(
                                        ("insert", None, {k: v for k, v in row.items() if k != "id"})
                                    )

                            if pending_match:
                                # Resolves every match_on-based row's lookup
                                # in as few round trips as possible instead
                                # of one _select() per ROW — the exact N+1
                                # pattern that made a 100-row save_many()
                                # ~24x slower than an equivalent insert-only
                                # batch, and ~1s for 500 rows, benchmarked
                                # against a 20k-row table. Distinct VALUE
                                # combos first (a real bulk batch commonly
                                # repeats the same combo across many rows,
                                # e.g. the same `status`), so even the
                                # per-combo fallback below is already fewer
                                # queries than one per row whenever they do.
                                distinct_filters: dict[tuple, dict] = {}
                                for row in pending_match:
                                    key = tuple(row[f] for f in match_on)
                                    distinct_filters.setdefault(key, {f: row[f] for f in match_on})
                                read_fields = list(dict.fromkeys([*match_on, "id"]))
                                matches_by_key: dict[tuple, list[dict]] = {}

                                if limit is None:
                                    # No per-row cap to enforce -> every
                                    # distinct combo can be OR'd together
                                    # (query.py's own any_of) and resolved
                                    # query.MAX_ANY_OF_BRANCHES combos at a
                                    # time, unbounded — matches the existing
                                    # "no limit given -> no implicit cap"
                                    # contract exactly, just batched.
                                    items = list(distinct_filters.items())
                                    for i in range(0, len(items), query.MAX_ANY_OF_BRANCHES):
                                        group = items[i : i + query.MAX_ANY_OF_BRANCHES]
                                        found = await self._select(
                                            table,
                                            filters={query.ANY_OF_KEY: [f for _key, f in group]},
                                            fields=read_fields,
                                            order_by=None,
                                            limit=None,
                                            offset=0,
                                            distinct=False,
                                        )
                                        for r in found:
                                            matches_by_key.setdefault(
                                                tuple(r[f] for f in match_on), []
                                            ).append(r)
                                else:
                                    # A real per-row bound WAS requested. A
                                    # single SQL LIMIT can't be split fairly
                                    # across several OR'd combos in one
                                    # query — it caps the COMBINED result
                                    # set, silently starving whichever
                                    # combo's rows Postgres happens to
                                    # return last, not each combo's own
                                    # count — so this stays one query per
                                    # DISTINCT combo (not per row) instead:
                                    # correct, and still a real win over the
                                    # old per-row loop whenever combos repeat.
                                    cap = limit + 1
                                    for key, filters in distinct_filters.items():
                                        matches_by_key[key] = await self._select(
                                            table,
                                            filters=filters,
                                            fields=read_fields,
                                            order_by=None,
                                            limit=cap,
                                            offset=0,
                                            distinct=False,
                                        )

                                for row in pending_match:
                                    key = tuple(row[f] for f in match_on)
                                    filters = {f: row[f] for f in match_on}
                                    matches = matches_by_key.get(key, [])
                                    if limit is not None and len(matches) > limit:
                                        raise query.QueryError(
                                            f"save_many(): match_on {filters} matched more than "
                                            f"limit={limit} row(s) on '{table}'."
                                        )
                                    if matches:
                                        data = {k: v for k, v in row.items() if k not in match_on}
                                        for m in matches:
                                            resolved.append(("update", m["id"], data))
                                    else:
                                        resolved.append(
                                            (
                                                "insert",
                                                None,
                                                {k: v for k, v in row.items() if k != "id"},
                                            )
                                        )

                            # Checked here — every row's insert-or-update
                            # kind is now known, and nothing below this
                            # point has touched a hook or written anything
                            # yet — so a violation aborts the WHOLE batch
                            # before any of it partially happens.
                            if not allow_insert and any(kind == "insert" for kind, _rid, _data in resolved):
                                raise query.QueryError(
                                    f"save_many(): allow_insert=False, but at least one row on "
                                    f"'{table}' would have been inserted (no id, and no match_on "
                                    f"hit) — refusing the whole batch."
                                )
                            if not allow_update and any(kind == "update" for kind, _rid, _data in resolved):
                                raise query.QueryError(
                                    f"save_many(): allow_update=False, but at least one row on "
                                    f"'{table}' would have been updated (id given, or match_on "
                                    f"matched an existing row) — refusing the whole batch."
                                )

                            for kind, _rid, data in resolved:
                                if kind == "insert":
                                    ctx = HookContext(table=table, old=None, payload=dict(data))
                                    ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
                                    insert_ctxs.append(ctx)
                            update_targets = [
                                (rid, data) for kind, rid, data in resolved if kind == "update"
                            ]
                            for _rid, data in update_targets:
                                update_ctxs.append(HookContext(table=table, payload=dict(data)))

                            need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
                            if need_old and update_targets:
                                old_rows = await self._psqldb.get_many(
                                    table, [rid for rid, _ in update_targets], conn=conn
                                )
                                # pgdb.get_many() already returns plain dicts
                                old_by_id = {r["id"]: r for r in old_rows}
                                for ctx, (rid, _data) in zip(update_ctxs, update_targets):
                                    ctx.old = old_by_id.get(rid)
                            for ctx in update_ctxs:
                                ctx.doc = _precommit_doc(ctx.old, ctx.payload, is_new=False)

                            all_ctxs = [*insert_ctxs, *update_ctxs]
                            for ctx in all_ctxs:
                                ctx.conn = conn
                                await self._run_hooks(table, "validate", ctx)
                                await self._run_hooks(table, "before_save", ctx)

                            if insert_ctxs:
                                results = await self._psqldb.insert_many(
                                    table,
                                    [c.payload for c in insert_ctxs],
                                    created_by=by,
                                    conn=conn,
                                )
                                # pgdb.insert_many() already returns plain dicts
                                for c, r in zip(insert_ctxs, results):
                                    c.new = r
                                    c.doc = _postwrite_doc(c.new, None, is_new=True)
                            if update_ctxs:
                                results = await self._psqldb.update_many(
                                    table,
                                    [
                                        {"id": rid, "data": c.payload}
                                        for (rid, _d), c in zip(update_targets, update_ctxs)
                                    ],
                                    updated_by=by,
                                    conn=conn,
                                )
                                # pgdb.update_many() already returns plain dicts
                                results_by_id = {r["id"]: r for r in results}
                                for (rid, _d), c in zip(update_targets, update_ctxs):
                                    c.new = results_by_id.get(rid)
                                    c.doc = _postwrite_doc(c.new, c.doc.old, is_new=False)

                            for ctx in all_ctxs:
                                await self._run_hooks(table, "after_save", ctx)
                    except Exception as exc:
                        for ctx in [*insert_ctxs, *update_ctxs]:
                            ctx.conn = None
                            ctx.error = exc
                            deferred.append((table, ctx, False))
                        raise
                    for ctx in [*insert_ctxs, *update_ctxs]:
                        ctx.conn = None
                for ctx in [*insert_ctxs, *update_ctxs]:
                    deferred.append((table, ctx, True))
            return [ctx.new for ctx in [*insert_ctxs, *update_ctxs]]
        finally:
            _skip_hook_events.reset(token)

    async def delete_many(
        self,
        table: str,
        ids: list[UUID],
        *,
        by: str | None = None,
        new_transaction: bool = False,
        skip_hooks: bool = False,
        skip_before_delete: bool = False,
        skip_after_delete: bool = False,
        skip_after_commit: bool = False,
        skip_on_rollback: bool = False,
    ) -> None:
        """skip_hooks/skip_<event> — see save()'s own docstring; applies
        uniformly across the whole batch, same as save_many()."""
        skip = _resolve_skip_events(
            skip_hooks,
            before_delete=skip_before_delete,
            after_delete=skip_after_delete,
            after_commit=skip_after_commit,
            on_rollback=skip_on_rollback,
        )
        token = _skip_hook_events.set(skip)
        try:
            self._psqldb.schema(table)
            if not ids:
                return
            need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
            ctxs = [HookContext(table=table) for _ in ids]
            dry_run = _dry_run.get()
            async with self._postcommit_scope(
                new_transaction=new_transaction, dry_run=dry_run
            ) as deferred:
                async with self._write_transaction(new_transaction=new_transaction) as conn:
                    try:
                        async with self._transaction_or_dry_run(conn, dry_run):
                            if need_old:
                                old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                                # pgdb.get_many() already returns plain dicts
                                old_by_id = {r["id"]: r for r in old_rows}
                                for ctx, row_id in zip(ctxs, ids):
                                    ctx.old = old_by_id.get(row_id)
                            for ctx in ctxs:
                                ctx.doc = _delete_doc(ctx.old)
                            for ctx in ctxs:
                                ctx.conn = conn
                                await self._run_hooks(table, "before_delete", ctx)
                            await self._psqldb.soft_delete_many(
                                table, ids, deleted_by=by, conn=conn
                            )
                            for ctx in ctxs:
                                await self._run_hooks(table, "after_delete", ctx)
                    except Exception as exc:
                        for ctx in ctxs:
                            ctx.conn = None
                            ctx.error = exc
                            deferred.append((table, ctx, False))
                        raise
                    for ctx in ctxs:
                        ctx.conn = None
                for ctx in ctxs:
                    deferred.append((table, ctx, True))
        finally:
            _skip_hook_events.reset(token)
