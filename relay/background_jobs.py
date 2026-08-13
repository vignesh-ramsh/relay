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
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import arc

from .ambient import _active_conn, _active_stream, _dry_run, _postcommit_queue


class BackgroundJobsMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _kernel: Any
    _background_tasks: set[asyncio.Task]
    _loading_plugin: str | None

    def _track_task(self, task: asyncio.Task) -> asyncio.Task:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

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

        Three paths, in order:
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
        3. No `lineup` installed — the original in-process
           `asyncio.create_task()` fallback: lost on crash/restart, no
           retry, no persistence, same as always (`queue=` has no meaning
           here, there's no queue concept without lineup).

        The two durable paths (1, 2) and the fallback (3) have a real
        semantic difference, not just an implementation swap: in the
        fallback, the returned Task completing means `fn` actually
        finished running, in THIS process. In a durable path, it means
        the job was successfully handed off to Redis — actual execution
        happens later, in a separate `arc lineup worker` process, and this
        Task's own success/failure says nothing about whether that later
        execution succeeds (there's no result backend wired up to report
        that back yet, docs/arc.MD §8). Either way this stays fire-and-
        forget from the caller's point of view — durable-fire-and-forget
        instead of volatile-fire-and-forget.

        AMBIENT STATE (all three paths): the spawned task deliberately does
        NOT inherit the enclosing request's database connection, dry-run
        flag, deferred post-commit queue, or active stream — see
        _detach_request_scoped_state() for why each one is actively harmful
        to carry, and for the confirmed bug that used to result. It DOES
        inherit the CallContext (request_id / user / roles), which is plain
        immutable data and is what makes a queued job traceable back to the
        request and user that queued it.

        In the in-process fallback that context is carried by the
        contextvar itself. The durable `lineup` paths hand the job to
        another PROCESS entirely, where a contextvar cannot reach — so it
        travels as data instead: lineup stamps `context_labels()` onto the
        TaskIQ message it kicks, and the worker rebinds them via
        `use_context_labels()` around the job before running it. Either
        way the `_job_log` row records `request_id`/`triggered_by`, so the
        trail survives even after the process that queued the job is gone."""
        if self._kernel.has("lineup"):
            lineup = self._kernel.get("lineup")
            if lineup.is_task(fn):
                coro = lineup.enqueue(fn, *args, **kwargs)
            else:
                lineup.check_resolvable(
                    fn
                )  # raises TypeError here, synchronously, before any Task exists
                coro = lineup.enqueue_by_path(fn, *args, queue=queue, **kwargs)

            async def _handoff() -> Any:
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

        started_at = datetime.now(timezone.utc)
        # Captured HERE, in the caller's own context, not inside the task —
        # _detach_request_scoped_state() deliberately leaves _call_context
        # alone, so reading it inside would work too, but capturing it at
        # the call site keeps the log row correct even if that ever changes.
        call_ctx = self.context()

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
                finished_at = datetime.now(timezone.utc)
                try:
                    await arc.psqldb.insert(
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
