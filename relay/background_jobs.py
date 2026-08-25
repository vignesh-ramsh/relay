"""Background/scheduled jobs — plugins/<plugin>/tasks/*.py, loaded via
register_tasks(), same directory-loading pattern as hooks/api.

This is the ONLY surface a business plugin should ever call for this —
never `arc.lineup.task(...)`/`arc.lineup.register_tasks(...)` directly
(docs/arc.MD §3.15). Same posture as cache_get/cache_set/lock() (relay/
cache_lock.py): relay is the facade every plugin writes against, `lineup`
(like `redix`) is an optional power source relay reaches for when
installed, never a dependency a business plugin declares or thinks about
itself. A plugin that only ever calls arc.relay.task/register_tasks/
enqueue needs no `optional_requires=["lineup"]` of its own at all — the
boot-order guarantee it'd otherwise need comes for free, transitively,
through relay's OWN optional_requires on lineup (§3.1's resolver treats
optional_requires as a real topological edge when both plugins are
present; since every business plugin already hard-requires relay, and
relay->lineup is such an edge whenever lineup is installed, lineup is
guaranteed to boot before every plugin downstream of relay too — verified
against a real `arc doctor` run after removing example_hr's own direct
optional_requires=["lineup"]).

Split out of relay/__init__.py for maintainability — BackgroundJobsMixin
is one of several mixins RelayProvider (relay/__init__.py) inherits from;
`self` is the single shared RelayProvider instance everywhere.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

import arc

from .ambient import _active_conn, _active_stream, _dry_run, _postcommit_queue, _postcommit_waiters

# How long a claim on a _job_log row is honored before another poller (this
# process's own reaper, or another process's) treats it as abandoned and
# re-claims it. A job that's still genuinely running past this now HAS a
# heartbeat renewing it (_run_claimed_job_log_row's own background task,
# below) — this only ever actually lapses when that heartbeat itself
# stops (a real crash, a hung event loop), not merely "the job ran long,"
# which used to be indistinguishable from a dead process and meant EVERY
# job over 60s was guaranteed to be reaped and double-executed by another
# poller. The lease is the crash-recovery signal now; the heartbeat is
# what keeps a merely-slow job from ever tripping it.
_JOB_LEASE_SECONDS = 60
# How often the heartbeat renews a still-running claim — a third of the
# lease, the same margin redix's own _RenewingLock uses (its docstring:
# "enough headroom that one slow/dropped renewal doesn't cost the lock
# outright, without renewing so often it meaningfully adds load"), applied
# here to the identical shape of problem.
_LEASE_RENEW_INTERVAL_SECONDS = _JOB_LEASE_SECONDS / 3
# How often RelayProvider's own poller (started in open(), see relay/
# __init__.py) checks for abandoned rows. Independent of lineup entirely —
# this exists so `arc.relay.enqueue()`'s in-process fallback survives a
# crash even in a project that never installs lineup at all (docs/"Missing
# Failure-Mode Audits", items 15/19 — see this method's own docstring below
# for the reasoning).
_REAP_INTERVAL_SECONDS = 15


def _durable_job_reference(fn: Any) -> tuple[str, str] | None:
    """(module_path, qualname) if `fn` can be reconstructed and re-called
    by a DIFFERENT process/task later — the same resolvability contract
    `lineup.check_resolvable()` enforces, duplicated here in small,
    deliberately, rather than imported: relay optionally depends on
    lineup, never the reverse (module docstring above), so relay cannot
    reach into lineup's implementation even for a helper this similar.

    Returns None instead of raising on anything unresolvable (a lambda, a
    closure, a reassigned name) — unlike lineup's version, this is not a
    hard requirement a caller opted into by installing lineup; it is an
    optional upgrade enqueue() below tries to give every caller for free.
    A caller whose fn doesn't qualify simply keeps today's exact behavior,
    unchanged and with no error — see enqueue()'s own docstring."""
    name = getattr(fn, "__name__", None)
    qualname = getattr(fn, "__qualname__", None)
    module_path = getattr(fn, "__module__", None)
    if name == "<lambda>" or not qualname or not module_path or "<locals>" in qualname:
        return None
    module = sys.modules.get(module_path)
    if module is None:
        try:
            module = importlib.import_module(module_path)
        except ImportError:
            return None
    resolved: Any = module
    try:
        for part in qualname.split("."):
            resolved = getattr(resolved, part)
    except AttributeError:
        return None
    if resolved is not fn:
        return None
    return module_path, qualname


def _json_safe(value: Any) -> bool:
    """Whether `value` survives a JSONB round trip unchanged in kind — the
    other half of durable eligibility alongside _durable_job_reference().
    A durable job's args/kwargs have to be reconstructable from a Postgres
    row by a process that never saw the original Python objects (including
    this SAME process, after a crash and restart) — a live object, a
    closure-captured connection, or anything else json.dumps can't handle
    can only ever have worked by being passed directly in memory, which is
    exactly what the non-durable fallback still does, unchanged."""
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False
    return True


class BackgroundJobsMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _kernel: Any
    _background_tasks: set[asyncio.Task]
    _loading_plugin: str | None
    _worker_id: str
    _reap_task: asyncio.Task | None

    def _track_task(self, task: asyncio.Task) -> asyncio.Task:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _dispatch_module_allowed(self, module_path: str | None) -> bool:
        """Whether `module_path` names code this project is willing to
        import-and-call from a `_job_log` row's own payload.

        _run_claimed_job_log_row below reconstructs a job purely from that
        row — a JSONB column, not trusted source — so without this check
        anyone who can write a `_job_log` row gets arbitrary code execution
        in whichever process next reaps it (`("asyncio",
        "create_subprocess_shell", ["..."], {})` is awaitable, so even the
        `await target(...)` shape is no obstacle). That's the same hole
        lineup's own generic dispatch task closed for its Redis-delivered
        equivalent; this is deliberately the SAME rule, duplicated in small
        rather than imported, for the same reason _durable_job_reference
        above duplicates check_resolvable(): relay optionally depends on
        lineup, never the reverse (module docstring), so it can't reach
        into lineup's implementation even for a helper this identical.

        Bounds dispatch to code belonging to this project: a module whose
        root is an installed plugin's own package (flat layout: package
        name == plugin name, §3.7), or one of the synthetic module names
        relay/lineup's directory loaders register for api/hooks/tasks
        files. Checked at RUN time, where it matters — enqueue()'s own
        _durable_job_reference() is a convenience check, not a security
        boundary (nothing forces a row to have come from enqueue() at
        all)."""
        if not module_path:
            return False
        root = module_path.split(".")[0]
        if root.startswith("_arc_relay_") or root.startswith("_arc_lineup_"):
            return True
        caps = self._kernel.capabilities()
        return root in {cap.plugin for cap in caps.values()} or root in caps

    # ------------------------------------------------------------------ #
    # Durable-queue lifecycle — RelayProvider.open()/close() (relay/
    # __init__.py) start/stop this poller the same way pgdb/redix/lineup
    # start their own connections, via Gateway's ASGI lifespan calling both
    # automatically for any capability that has them. A CLI-only process
    # (not behind Gateway) never calls open(), so it never runs this loop —
    # fine, since reaping is a "something alive has to be polling" concern,
    # only meaningful for a long-running process in the first place.
    # ------------------------------------------------------------------ #
    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(_REAP_INTERVAL_SECONDS)
            try:
                await self._reap_stale_jobs()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad pass must not kill the loop
                self.log(f"durable job reaper pass failed: {exc}", level="error")

    @staticmethod
    def _detach_request_scoped_state() -> None:
        """Call FIRST inside any coroutine handed to asyncio.create_task()
        from here — it severs the request-scoped ambient state the new task
        would otherwise silently inherit, while deliberately KEEPING the
        call context.

        asyncio.create_task() copies the current contextvar context into
        the new task. That is exactly what relay wants for hooks (a nested
        save correctly sees the enclosing write's connection), and exactly
        what it must NOT have for a background job, which outlives the
        request that spawned it:

          * `_active_conn` — the enclosing write's pooled connection. By
            the time the job runs, that connection has been RELEASED back
            to the pool and very likely handed to a different request. The
            job would either fail with a confusing "cannot use
            Connection.transaction() in a manually started transaction"
            (its work silently lost — only a log line, nothing raised to
            anyone) or, worse, issue queries on a connection another
            request is actively using.
          * `_dry_run` — a job enqueued during a GET inherited dry_run=True
            and silently rolled back every write it made, forever, with no
            error anywhere.
          * `_postcommit_queue` — the enclosing write's deferred hook list.
            A background job's own writes are their own outermost
            transaction and must flush their own hooks, not append to a
            queue nobody will ever flush.
          * `_postcommit_waiters` — the enclosing write's list of durable-
            enqueue futures (this file's own dispatch-deferral mechanism,
            see enqueue()). Same reasoning as `_postcommit_queue`: a job's
            own nested enqueue() call is its own outermost write and must
            dispatch immediately, not register a future against an outer
            scope that already resolved (or, worse, one that hasn't yet
            and never will from in here).
          * `_active_stream` — publish() from a detached job would push
            into a response stream that has already been sent and closed.

        `_call_context` (request_id / user / roles) is deliberately NOT
        cleared: it is plain immutable data holding no connection and no
        lifetime, and carrying it is the whole point — it is what makes a
        job traceable back to the request and user that queued it.
        """
        _active_conn.set(None)
        _dry_run.set(False)
        _postcommit_queue.set(None)
        _postcommit_waiters.set(None)
        _active_stream.set(None)

    def register_tasks(self, tasks_dir: str | Path) -> None:
        tasks_dir = Path(tasks_dir)
        if not tasks_dir.exists():
            return
        plugin = self._kernel.current_plugin() or "<direct>"
        for path in sorted(tasks_dir.glob("*.py")):
            self._loading_plugin = plugin
            try:
                module_name = f"_arc_relay_tasks_{plugin}_{path.stem}"
                spec = importlib.util.spec_from_file_location(module_name, path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
            finally:
                self._loading_plugin = None

    def task(
        self, *, queue: str = "default", cron: str | None = None
    ) -> Callable[[Callable[..., Awaitable[Any]]], Any]:
        """`@arc.relay.task(queue="default")` — a durable, on-demand job
        when `lineup` is installed (real Redis-backed dispatch, docs/
        arc.MD §3.15); `@arc.relay.task(queue="default", cron="0 23 * * *")`
        additionally schedules it, dispatched by `arc lineup scheduler`
        at its real next occurrence, never at registration time.

        With no `lineup` installed, this degrades to returning `fn`
        completely unchanged — `arc.relay.enqueue(fn, ...)` already knows
        how to run a plain function in-process (exactly as it always has,
        docs/arc.MD §3.11), so the decorator itself never needs to wrap
        anything in that case. A `cron=` that can't actually be honored
        without a scheduler process running is surfaced once via
        `kernel.advise()` (the same "advise, don't fail" posture as every
        other optional-capability degradation in this project, §3.5)
        rather than silently doing nothing with no signal at all."""

        def decorator(fn: Callable[..., Awaitable[Any]]) -> Any:
            if self._kernel.has("lineup"):
                return self._kernel.get("lineup").task(queue=queue, cron=cron)(fn)
            if cron is not None:
                plugin = self._loading_plugin or self._kernel.current_plugin() or "<direct>"
                self._kernel.advise(
                    f"relay: task '{plugin}.{fn.__name__}' declared cron={cron!r} but no "
                    f"'lineup' plugin is installed — it will never run automatically, only "
                    f"via a manual arc.relay.enqueue({fn.__name__}, ...) call."
                )
            return fn

        return decorator

    def enqueue(
        self, fn: Callable[..., Awaitable[Any]], *args: Any, queue: str = "default", **kwargs: Any
    ) -> asyncio.Task:
        """The one entry point for background work — declared ahead of
        time (`@arc.relay.task(...)`, a plugins/<plugin>/tasks/*.py file)
        or completely ad hoc (any plain function, called from literally
        anywhere: a whitelisted function, a hook, wherever). Docs/arc.MD
        §3.15 — this is deliberately the ONLY surface a business plugin
        should ever call for this; never `arc.lineup.*` directly.

        Four paths, in order:
        1. `fn` is already a `@arc.relay.task(...)`-declared task
           (`arc.lineup.is_task(fn)`) — dispatched via its own `.kiq()`,
           on whatever queue it was declared with (`queue=` here is
           ignored in this path; the task's own declared queue wins).
        2. `lineup` is installed and `fn` is a plain, resolvable function
           (checked synchronously, immediately, via
           `arc.lineup.check_resolvable` — raises TypeError right here at
           the call site if `fn` is a lambda, a closure, or otherwise
           can't be re-imported by a worker process later) — dispatched
           via `arc.lineup.enqueue_by_path(fn, queue=queue, ...)`, no
           decorator or tasks/ file required at all.
        3. No `lineup`, but `fn` is "durable-eligible" — a genuine,
           resolvable module-level function (same resolvability contract
           as path 2's `check_resolvable`, checked here via
           `_durable_job_reference()` instead since relay cannot import
           lineup's own implementation) AND every arg/kwarg survives a
           JSON round trip (`_json_safe()`). Durable via a Postgres outbox,
           no Redis needed: `_dispatch_durable_fallback()` writes a
           `status="Queued"` `_job_log` row FIRST (module path, qualname,
           args, kwargs — everything needed to reconstruct and re-run the
           call from the row alone), then immediately self-claims and runs
           it in this process, same near-zero latency path 4 always had.
           If this process dies before finishing, the row sits claimed
           with a short lease; `_reap_loop` (started by `open()`, polling
           every `_REAP_INTERVAL_SECONDS`, independent of `lineup`
           entirely) reclaims and re-runs it later — possibly in a
           different process. This ONLY runs in a process that calls
           `open()` (behind Gateway, or anything else that opens relay
           explicitly) — a one-off CLI invocation never polls.
        4. Not durable-eligible (a lambda, a closure, or an argument that
           can't survive a JSON round trip — a live object, a connection,
           ...) — the original in-process `asyncio.create_task()`
           fallback, genuinely unchanged: lost on crash/restart, no retry,
           no persistence (`queue=` has no meaning here either).

        Paths 1-3 are all durable now, not just 1-2 — path 3 is the newer
        addition (a Postgres outbox, not Redis) and is what makes "no
        `lineup` installed" no longer synonymous with "not durable" the
        way it used to be; only path 4 (an ineligible `fn`) still is. The
        semantic difference that matters: for path 4, the returned Task
        completing means `fn` actually finished running, in THIS process.
        For paths 1-3, it means the job was successfully HANDED OFF
        (to Redis, or to the outbox row) — actual execution may happen
        later, possibly in a different process, and this Task's own
        success/failure says nothing about whether that later execution
        succeeds (there's no result backend wired up to report that back
        yet, docs/arc.MD §8). Either way this stays fire-and-forget from
        the caller's point of view.

        AMBIENT STATE (all four paths): the spawned task deliberately does
        NOT inherit the enclosing request's database connection, dry-run
        flag, deferred post-commit queue, or active stream — see
        _detach_request_scoped_state() for why each one is actively harmful
        to carry, and for the confirmed bug that used to result. It DOES
        inherit the CallContext (request_id / user / roles), which is plain
        immutable data and is what makes a queued job traceable back to the
        request and user that queued it.

        Path 3 and path 4 both capture it explicitly, once, at the call
        site (`call_ctx = self.context()`, before either branch runs) —
        not by relying on the contextvar copy `asyncio.create_task()` gives
        the new task for free — so the `_job_log` row stays correct even
        if the closure's own contextvar snapshot were ever changed
        independently. The durable `lineup` paths (1, 2) hand the job to
        another PROCESS entirely, where a contextvar cannot reach at all —
        so it travels as data instead: lineup stamps `context_labels()`
        onto the TaskIQ message it kicks, and the worker rebinds them via
        `use_context_labels()` around the job before running it. Every
        path's `_job_log` row records `request_id`/`triggered_by`, so the
        trail survives even after the process that queued the job is gone.

        DISPATCH TIMING when called from inside an active write (a hook):
        none of the four paths above actually dispatch until the OUTERMOST
        write's real fate is known — committed or rolled back. Building the
        Redis message / durable row eagerly, before that, used to mean a
        rolled-back write could still leave a job queued for data that was
        never persisted (docs/"Missing Failure-Mode Audits", item 19). The
        returned Task always exists immediately (so `_track_task` still
        holds a strong reference from the moment this call returns), but
        its body waits on a future the enclosing write resolves the instant
        its transaction block exits — see ambient.py's _postcommit_waiters
        and transactions.py's _postcommit_scope. Called with no active
        write in progress (the overwhelmingly common case — an API handler,
        a CLI, a background job itself), there is nothing to wait for and
        dispatch starts immediately, exactly as before."""
        waiters = _postcommit_waiters.get()

        def _await_gate() -> "asyncio.Future | None":
            if waiters is None:
                return None
            fut = asyncio.get_running_loop().create_future()
            waiters.append(fut)
            return fut

        if self._kernel.has("lineup"):
            lineup = self._kernel.get("lineup")
            if lineup.is_task(fn):
                coro = lineup.enqueue(fn, *args, **kwargs)
            else:
                lineup.check_resolvable(
                    fn
                )  # raises TypeError here, synchronously, before any Task exists
                coro = lineup.enqueue_by_path(fn, *args, queue=queue, **kwargs)

            gate = _await_gate()

            async def _handoff() -> Any:
                if gate is not None:
                    committed = await gate
                    if not committed:
                        # `coro` (lineup.enqueue()/enqueue_by_path(), built
                        # above but never run — a coroutine's body doesn't
                        # execute until awaited) must be closed explicitly
                        # here, or Python logs a "coroutine was never
                        # awaited" warning once it's garbage-collected.
                        coro.close()
                        return None
                # Only pushes a message to Redis — but it still must not
                # run holding the enclosing write's DB connection, and a
                # handoff during a GET must not be silently dry-run'd into
                # doing nothing. Same detachment as the fallback below.
                self._detach_request_scoped_state()
                return await coro

            # _track_task: most callers drop the returned Task — without a
            # strong reference here it could be GC-cancelled mid-handoff.
            task = self._track_task(asyncio.create_task(_handoff()))

            def _on_enqueue_done(t: asyncio.Task) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    self.log(f"lineup enqueue for task {fn.__name__} failed: {exc}", level="error")

            task.add_done_callback(_on_enqueue_done)
            return task

        # Captured HERE, in the caller's own context, not inside the task —
        # _detach_request_scoped_state() deliberately leaves _call_context
        # alone, so reading it inside would work too, but capturing it at
        # the call site keeps the log row correct even if that ever changes.
        call_ctx = self.context()
        durable_ref = _durable_job_reference(fn)
        durable_eligible = (
            durable_ref is not None and _json_safe(list(args)) and _json_safe(kwargs)
        )

        if durable_eligible:
            module_path, qualname = durable_ref
            payload = {
                "module": module_path,
                "qualname": qualname,
                "args": list(args),
                "kwargs": kwargs,
            }
            gate = _await_gate()

            async def _run_durable() -> Any:
                if gate is not None:
                    committed = await gate
                    if not committed:
                        return None
                self._detach_request_scoped_state()
                return await self._dispatch_durable_fallback(
                    fn, args, kwargs, payload=payload, queue=queue, call_ctx=call_ctx
                )

            task = self._track_task(asyncio.create_task(_run_durable()))

            def _on_durable_done(t: asyncio.Task) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    self.log(f"enqueued task {fn.__name__} failed: {exc}", level="error")

            task.add_done_callback(_on_durable_done)
            return task

        # Not durable-eligible (a lambda, a closure, or an argument that
        # can't survive a JSONB round trip) — today's original in-process
        # fallback, completely unchanged: lost on crash/restart, no retry,
        # no persistence. `queue=` has no meaning here, there's no queue
        # concept without a durable record or lineup.
        started_at = arc.tz.utcnow()

        async def _run_and_log() -> Any:
            # FIRST statement in the task: drop the enclosing request's
            # connection/dry-run/post-commit/stream state, keep the call
            # context. See _detach_request_scoped_state()'s docstring.
            self._detach_request_scoped_state()
            status, error = "success", None
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                status, error = "failed", f"{type(exc).__name__}: {exc}"
                raise
            finally:
                # Best-effort — a DB hiccup writing the log row must never
                # mask or replace the real task's own outcome above.
                finished_at = arc.tz.utcnow()
                try:
                    await arc.pgdb.insert(
                        "_job_log",
                        {
                            "task_name": getattr(fn, "__qualname__", None)
                            or getattr(fn, "__name__", None)
                            or repr(fn),
                            "queue": queue,
                            "executor": "relay",
                            "job_type": "Task",  # the in-process fallback has no scheduling concept at all
                            "queued_by": (getattr(fn, "__module__", "") or "").split(".")[0]
                            or None,
                            "status": status,
                            "error": error,
                            "started_at": started_at,
                            "finished_at": finished_at,
                            "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
                            # Which request queued this, and as whom — the
                            # whole point of carrying the CallContext across
                            # the task boundary. Both NULL for a job with no
                            # originating request (a CLI run, a test).
                            "request_id": call_ctx.request_id,
                            "triggered_by": call_ctx.user,
                        },
                    )
                except Exception as log_exc:
                    self.log(
                        f"failed to write _job_log row for {fn.__name__}: {log_exc}", level="error"
                    )

        task = self._track_task(asyncio.create_task(_run_and_log()))

        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                self.log(f"enqueued task {fn.__name__} failed: {exc}", level="error")

        task.add_done_callback(_on_done)
        return task

    # ------------------------------------------------------------------ #
    # Durable dispatch for the no-lineup fallback — see enqueue()'s own
    # docstring for the eligibility rule and the DISPATCH TIMING section.
    # ------------------------------------------------------------------ #
    async def _dispatch_durable_fallback(
        self,
        fn: Callable[..., Awaitable[Any]],
        args: tuple,
        kwargs: dict,
        *,
        payload: dict,
        queue: str,
        call_ctx: Any,
    ) -> Any:
        """Writes the Queued+payload row first — durable, reconstructable
        by ANY process from the row alone, not just this one — then
        immediately self-claims and runs it in THIS process, for the same
        near-zero latency the plain fallback always had. If this process
        dies before finishing, the row is left claimed with a lease that
        expires; _reap_stale_jobs() (this process's own poller, or any
        other process's, since claiming uses SKIP LOCKED) picks it back up
        later — the crash-recovery half of what used to be a pure
        fire-and-forget `asyncio.create_task()` with nothing behind it.

        If the DB itself is unreachable right now, durability just isn't
        available this instant — runs `fn` directly instead of losing the
        job outright over a failed logging-table write, the same
        "best-effort, must never mask the real outcome" posture the
        original _run_and_log() finally-block already used for logging."""
        queued_at = arc.tz.utcnow()
        try:
            # pgdb.insert() strips "id" as a system column and lets
            # Postgres default-generate it — so the row's real id comes
            # back from the INSERT's own RETURNING *, not from anything
            # chosen up front.
            inserted = await arc.pgdb.insert(
                "_job_log",
                {
                    "task_name": f"{payload['module']}.{payload['qualname']}",
                    "queue": queue,
                    "executor": "relay",
                    "job_type": "Task",
                    "queued_by": payload["module"].split(".")[0] or None,
                    "status": "Queued",
                    "payload": payload,
                    "queued_at": queued_at,
                    "request_id": call_ctx.request_id,
                    "triggered_by": call_ctx.user,
                },
            )
        except Exception as exc:  # noqa: BLE001 - a logging-table outage must not lose the job
            self.log(
                f"failed to write durable _job_log row for {fn.__name__}: {exc} — "
                f"running in-process without a durable record instead",
                level="error",
            )
            return await fn(*args, **kwargs)

        claimed = await self._claim_job_log_row(inserted["id"])
        if claimed is None:
            # A reaper pass (this process's own, or another's) already
            # claimed it in the narrow window since our own INSERT
            # committed — nothing left to do, whoever claimed it runs it.
            return None
        return await self._run_claimed_job_log_row(claimed)

    async def _claim_job_log_row(self, row_id: str) -> Any | None:
        """Conditionally claims one specific row by id — used right after
        this process's own INSERT, where it's virtually always uncontested.
        `WHERE status='Queued'` makes a lost race a no-op, not a stolen
        claim: if a reaper pass already got there first, this simply
        updates zero rows and returns None.

        Stamps a fresh `claim_token` (a random uuid, not a business
        value) alongside the lease — see _run_claimed_job_log_row's own
        docstring for what it fences against."""
        started_at = arc.tz.utcnow()
        lease_until = arc.tz.add(seconds=_JOB_LEASE_SECONDS, base=started_at)
        claim_token = uuid.uuid4().hex
        async with arc.pgdb.acquire() as conn:
            return await conn.fetchrow(
                'UPDATE "_job_log" SET status=$1, claimed_by=$2, lease_expires_at=$3, '
                'started_at=$4, claim_token=$5 WHERE id=$6 AND status=$7 RETURNING *',
                "Running",
                self._worker_id,
                lease_until,
                started_at,
                claim_token,
                row_id,
                "Queued",
            )

    async def _heartbeat_job_lease(self, row_id: Any, claim_token: str) -> None:
        """Runs for as long as _run_claimed_job_log_row is actually
        executing the job's own target(...) coroutine — renews
        lease_expires_at every _LEASE_RENEW_INTERVAL_SECONDS so a job that
        merely runs long never looks abandoned to _reap_stale_jobs. Same
        shape as redix._RenewingLock._renew_loop (sleep, try renew, stop
        quietly if it's no longer ours), duplicated here in small rather
        than shared — relay optionally depends on redix, never requires
        it, and this heartbeat has to work identically with or without
        redix installed at all.

        Every UPDATE (this one, and the final status write in
        _run_claimed_job_log_row's own finally block) is conditioned on
        `claim_token` matching, not just `id` — if a heartbeat write ever
        affects zero rows, another poller has already reclaimed this row
        (this process was gone long enough for its lease to lapse before
        this renewal reached the database), and continuing to "renew" a
        claim that's no longer ours would just fight the new claimant for
        the same row. Stops, rather than trying to reacquire — the row
        belongs to whoever holds the current token now."""
        try:
            while True:
                await asyncio.sleep(_LEASE_RENEW_INTERVAL_SECONDS)
                lease_until = arc.tz.add(seconds=_JOB_LEASE_SECONDS)
                try:
                    async with arc.pgdb.acquire() as conn:
                        renewed = await conn.fetchval(
                            'UPDATE "_job_log" SET lease_expires_at=$1 '
                            "WHERE id=$2 AND claim_token=$3 RETURNING id",
                            lease_until,
                            row_id,
                            claim_token,
                        )
                except Exception as exc:  # noqa: BLE001 - a missed renewal isn't fatal on its own
                    self.log(
                        f"lease heartbeat for job {row_id} failed to write ({exc}) — "
                        f"will retry next interval",
                        level="warning",
                    )
                    continue
                if renewed is None:
                    self.log(
                        f"lease heartbeat for job {row_id}: claim token no longer matches — "
                        f"another poller has already reclaimed it; stopping renewal",
                        level="warning",
                    )
                    return
        except asyncio.CancelledError:
            pass

    async def _run_claimed_job_log_row(self, row: Any) -> Any:
        """Reconstructs and runs a job purely from an already-claimed
        `_job_log` row's own payload — shared by the immediate self-claim
        path above and _reap_stale_jobs() below, which is the entire point:
        a row claimed by a poller on a DIFFERENT process, hours after the
        process that enqueued it is gone, runs through the exact same code
        path as the common case, with no special-casing for "this is a
        recovery, not the first attempt." Re-raises on failure (after
        recording it on the row) so a direct caller's own done-callback
        still logs it — _reap_stale_jobs() catches around each call for
        exactly that reason, so one bad job doesn't stop the reap pass.

        Runs a background lease heartbeat (_heartbeat_job_lease) for the
        duration of the job — see its own docstring — and writes the
        FINAL status only if `claim_token` still matches this claim.
        Without that guard, a genuinely-abandoned execution (the rare case
        the heartbeat itself couldn't save — a real crash, a hung event
        loop) that somehow still limped to completion afterward could
        silently overwrite whatever the NEW claimant already recorded —
        marking a job "success" here after someone else already reran and
        recorded "failed" (or vice versa) would be a duplicated, out-of-
        order side effect on the bookkeeping itself, exactly the class of
        bug idempotency is meant to close. This can't stop target(...)
        itself from having run twice — relay has no visibility into what
        a caller's own job actually does — but it does guarantee this
        row's own status/error/duration reflect whichever execution
        legitimately holds the claim, never a stale one clobbering a
        fresher one."""
        payload = row["payload"] or {}
        module_path, qualname = payload.get("module"), payload.get("qualname")
        args, kwargs = payload.get("args") or [], payload.get("kwargs") or {}
        row_id = row["id"]
        claim_token = row["claim_token"]
        started_at = row["started_at"] or arc.tz.utcnow()
        heartbeat = self._track_task(
            asyncio.create_task(self._heartbeat_job_lease(row_id, claim_token))
        )
        status, error = "success", None
        try:
            if not self._dispatch_module_allowed(module_path):
                raise PermissionError(
                    f"refusing to run '{module_path}.{qualname}' from _job_log row "
                    f"{row_id} — its root module is not an installed ARC plugin "
                    f"package (or a relay/lineup-loaded module)."
                )
            module = importlib.import_module(module_path)
            target: Any = module
            for part in qualname.split("."):
                target = getattr(target, part)
            return await target(*args, **kwargs)
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            raise
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            finished_at = arc.tz.utcnow()
            try:
                async with arc.pgdb.acquire() as conn:
                    updated = await conn.fetchval(
                        'UPDATE "_job_log" SET status=$1, error=$2, finished_at=$3, '
                        "duration_ms=$4 WHERE id=$5 AND claim_token=$6 RETURNING id",
                        status,
                        error,
                        finished_at,
                        int((finished_at - started_at).total_seconds() * 1000),
                        row_id,
                        claim_token,
                    )
                if updated is None:
                    self.log(
                        f"job {row_id} finished ({status}) but its claim was already "
                        f"reclaimed by another poller — not overwriting that poller's "
                        f"own result",
                        level="warning",
                    )
            except Exception as log_exc:  # noqa: BLE001 - must not mask the job's own outcome
                self.log(f"failed to write finished _job_log row {row_id}: {log_exc}", level="error")

    async def _reap_stale_jobs(self, limit: int = 10) -> int:
        """Claims relay-fallback jobs (`executor='relay'`) abandoned by a
        dead process — either stuck 'Queued' because the enqueuing process
        crashed before its own self-claim ever ran, or stuck 'Running'
        because the process running it crashed mid-job (a heartbeat means
        "still Running" no longer ALSO means "merely slow" — see
        _heartbeat_job_lease's own docstring; a lapsed lease past this
        point really does mean nobody is renewing it). `FOR UPDATE SKIP
        LOCKED` means any number of processes can run this concurrently
        with zero coordination: each claims a disjoint set of rows, the
        same pattern psqldb_migrate.migration_lock's own polling already
        establishes elsewhere in this codebase, applied here to individual
        rows instead of one global lock.

        Only ever touches this relay's own durable jobs: `executor='relay'`
        excludes lineup-owned rows (lineup runs its own equivalent reaper —
        see lineup/__init__.py's _reap_and_run_stale), and `payload IS NOT
        NULL` excludes every row this table already had before durability
        existed (a Scheduler-fired job, or a call enqueue() judged
        ineligible) — there is nothing to reconstruct or run for either.

        One fresh `claim_token` for the WHOLE batch this pass claims, not
        one per row — uuid4() is already globally unique per call, and
        fencing only needs "is this specific claim-instance still active
        for this row," which a shared per-pass token still guarantees
        against any FUTURE reclaim (which always gets its own fresh
        token); generating N tokens for N rows would guard nothing extra.

        Dispatches every claimed row concurrently (asyncio.gather), not
        serially — a serial await here previously meant row N's own lease
        clock (already ticking since ITS claim UPDATE above) burned down
        while rows 1..N-1 ran first, shrinking its real runway before its
        own heartbeat could even start."""
        started_at = arc.tz.utcnow()
        lease_until = arc.tz.add(seconds=_JOB_LEASE_SECONDS, base=started_at)
        claim_token = uuid.uuid4().hex
        async with arc.pgdb.acquire() as conn:
            claimed = await conn.fetch(
                'UPDATE "_job_log" SET status=$1, claimed_by=$2, lease_expires_at=$3, '
                "started_at=COALESCE(started_at, $4), claim_token=$5 "
                "WHERE id IN ("
                '  SELECT id FROM "_job_log" '
                "  WHERE executor=$6 AND payload IS NOT NULL "
                "    AND status IN ('Queued', 'Running') "
                "    AND (lease_expires_at IS NULL OR lease_expires_at < now()) "
                "  ORDER BY queued_at NULLS FIRST "
                "  LIMIT $7 "
                "  FOR UPDATE SKIP LOCKED"
                ") RETURNING *",
                "Running",
                self._worker_id,
                lease_until,
                started_at,
                claim_token,
                "relay",
                limit,
            )

        async def _run_and_log(row: Any) -> None:
            try:
                await self._run_claimed_job_log_row(row)
            except Exception as exc:  # noqa: BLE001 - already recorded on the row; keep reaping
                self.log(f"reaped job {row['id']} failed: {exc}", level="error")

        await asyncio.gather(*(_run_and_log(row) for row in claimed))
        return len(claimed)
