"""Session-bound connections (relay/__init__.py's own module docstring;
docs/arc.MD §8's arc.di gap, minimal/targeted version). _connection() is
what every read AND write method goes through to get a connection;
_write_transaction() additionally PUBLISHES it as the ambient one for the
duration, so a hook's own nested arc.relay.* calls pick it up
automatically — correct nested-transaction semantics come for free from
asyncpg, which turns a conn.transaction() called while already inside one
on the same connection into a real SAVEPOINT: if the outer write later
fails, a nested write sharing its connection rolls back with it, because
it was never a separate transaction to begin with.

Split out of relay/__init__.py for maintainability — TransactionsMixin is
one of several mixins RelayProvider (relay/__init__.py) inherits from;
`self` is the single shared RelayProvider instance everywhere, including
`self._run_hooks_resolved` (defined on HooksMixin, relay/hooks.py).
"""

from __future__ import annotations

import contextlib
from typing import Any

from .ambient import (
    _MAX_WRITE_DEPTH,
    _active_conn,
    _postcommit_queue,
    _postcommit_waiters,
    _write_depth,
)
from .shapes import HookEvent, RelayError, _DryRunRollback


class TransactionsMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind this.
    _psqldb: Any

    @contextlib.asynccontextmanager
    async def _connection(self, *, new_transaction: bool = False):
        ambient = None if new_transaction else _active_conn.get()
        if ambient is not None:
            yield ambient
            return
        async with self._psqldb.acquire() as conn:
            yield conn

    @contextlib.asynccontextmanager
    async def _write_transaction(self, *, new_transaction: bool = False):
        depth = _write_depth.get()
        if depth >= _MAX_WRITE_DEPTH:
            raise RelayError(
                f"write recursion depth exceeded ({_MAX_WRITE_DEPTH}) — a hook is "
                f"likely calling arc.relay.save()/save_many()/delete()/delete_many() "
                f"on itself, directly or indirectly, with no base case that stops it.",
                status=500,
                code="write_recursion_limit",
            )
        depth_token = _write_depth.set(depth + 1)
        try:
            async with self._connection(new_transaction=new_transaction) as conn:
                token = _active_conn.set(conn)
                try:
                    yield conn
                finally:
                    _active_conn.reset(token)
        finally:
            _write_depth.reset(depth_token)

    @contextlib.asynccontextmanager
    async def _postcommit_scope(self, *, new_transaction: bool, dry_run: bool):
        """Defers after_commit/on_rollback until the OUTERMOST write
        transaction has genuinely resolved, and yields the list this write
        records its own outcome into.

        The bug this exists to fix: every write used to run its own
        post-commit hooks as soon as its `async with self._write_transaction(...)`
        block ended. For an outermost write that is correct — the block
        ending means the transaction committed. For a NESTED write (a hook
        calling arc.relay.save()) it means nothing at all: the nested call
        shares the outer connection, so asyncpg turned its transaction()
        into a mere SAVEPOINT, and the real transaction is still open and
        can still roll back. after_commit — the one hook whose entire
        contract is "this data is durable now, it is safe to tell the
        outside world" — therefore fired for rows that then vanished.
        Confirmed against real Postgres: an outer save whose before_save
        hook did a nested save and then raised left the nested row's
        after_commit hook fired and the table empty. Sending a confirmation
        email for an order that no longer exists is exactly the failure
        this shape produces. The mirror was just as wrong: the nested
        write's on_rollback never fired, so nothing ever corrected it.

        The rule now, stated once: a write records WHETHER IT ITSELF
        succeeded; the outermost transaction decides WHICH event that
        becomes.
          * outer committed + this write succeeded -> after_commit
          * outer committed + this write failed    -> on_rollback (its own
            SAVEPOINT rolled back, but the outer legitimately carried on)
          * outer rolled back                      -> on_rollback for every
            entry, whatever each one's own outcome was — nothing persisted
        A dry run flushes nothing at all: after_commit must not fire
        (nothing committed) and on_rollback must not either (nothing
        failed) — unchanged from the previous behavior, just centralized.

        `outermost` is computed BEFORE _write_transaction() runs, since
        that is what sets _active_conn in the first place; new_transaction
        =True always means outermost, because it acquires its own separate
        connection and is therefore its own real transaction."""
        outermost = new_transaction or _active_conn.get() is None
        token = _postcommit_queue.set([]) if outermost else None
        waiters_token = _postcommit_waiters.set([]) if outermost else None
        deferred = _postcommit_queue.get()
        if deferred is None:  # defensive — only reachable if a caller nulls it mid-write
            deferred = []
        try:
            yield deferred
        except BaseException as exc:
            if outermost:
                # Resolve durable-enqueue waiters (background_jobs.py's
                # enqueue()) BEFORE flushing hooks — a dry run never fails
                # this way (_DryRunRollback is caught inside
                # _transaction_or_dry_run, never reaches here), so
                # `committed=False` here always means a genuine failure, and
                # a job enqueued from inside it must never be dispatched.
                self._resolve_postcommit_waiters(committed=False)
                if not dry_run:
                    await self._flush_postcommit(deferred, committed=False, error=exc)
            raise
        else:
            if outermost:
                # Deliberately NOT gated on dry_run, unlike the hook flush
                # just below: enqueue() has never taken dry_run into account
                # (see ambient.py's _dry_run docstring — it protects a
                # background job's OWN writes, not whether the job gets
                # dispatched at all) and this durable path must not start
                # now, silently, only during a real write.
                self._resolve_postcommit_waiters(committed=True)
                if not dry_run:
                    await self._flush_postcommit(deferred, committed=True, error=None)
        finally:
            if token is not None:
                _postcommit_queue.reset(token)
            if waiters_token is not None:
                _postcommit_waiters.reset(waiters_token)

    @staticmethod
    def _resolve_postcommit_waiters(*, committed: bool) -> None:
        """Wakes every enqueue() call made from inside this write (see
        ambient.py's _postcommit_waiters docstring) with the outermost
        write's real fate. A future can already be done here if the task
        awaiting it was itself cancelled before this ran — set_result would
        raise InvalidStateError in that case, harmless to skip."""
        waiters = _postcommit_waiters.get()
        if not waiters:
            return
        for fut in waiters:
            if not fut.done():
                fut.set_result(committed)

    async def _flush_postcommit(
        self, deferred: list, *, committed: bool, error: BaseException | None
    ) -> None:
        """Runs every deferred entry's real post-commit event, in the order
        the writes happened. `error` is the outermost failure, attached to
        any entry that succeeded on its own but is being rolled back by it
        — otherwise a hook asking "why am I being told this rolled back?"
        would find ctx.error empty for the one case where the answer is
        most useful."""
        for table, ctx, succeeded in deferred:
            if committed and succeeded:
                event: HookEvent = "after_commit"
            else:
                event = "on_rollback"
                if ctx.error is None:
                    ctx.error = error
            await self._run_hooks_resolved(table, event, ctx)

    @contextlib.asynccontextmanager
    async def _transaction_or_dry_run(self, conn: Any, dry_run: bool):
        """Drop-in replacement for `async with conn.transaction():` in every
        write primitive below. Runs the caller's body (hooks + the real
        INSERT/UPDATE/DELETE, via `RETURNING *` so ctx.new reflects real
        server-computed defaults/triggers) exactly as normal; if `dry_run`,
        raises a sentinel right after the body completes — still INSIDE
        conn.transaction()'s own `async with`, so asyncpg rolls back for
        real — then swallows it here so the caller sees a normal return,
        never an exception. The caller is responsible for skipping
        after_commit when dry_run is true (on_rollback is never fired here
        either way, since _DryRunRollback isn't a real failure)."""
        try:
            async with conn.transaction():
                yield
                if dry_run:
                    raise _DryRunRollback()
        except _DryRunRollback:
            pass
