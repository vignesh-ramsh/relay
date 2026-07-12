"""
relay — ARC application runtime (Architecture §2/§3.4).

Exports `arc.relay`: hook-wrapped CRUD over whatever tables `psqldb` has
already declared (schemas/patches, §3.9) — Relay never redeclares the data
model, it only attaches behavior to one psqldb already owns. This first cut
covers single-row CRUD + the full hook taxonomy; batch CRUD, cache/lock/
queue, RBAC, whitelisting, and streaming are separate, later increments —
see docs/arc.MD §7 for the build order this follows.

requires=["psqldb"]: the only hard dependency — a Resource with nothing to
store isn't a Resource. optional_requires=["authn", "redix", "gateway"]:
all three are used by LATER increments (RBAC, cache/lock/queue, whitelisted
routes) and are harmless to declare now even though this cut doesn't touch
any of them yet.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

from rich.console import Console

import arc  # safe at module level — same pattern gateway/request.py already uses;
            # arc.codec is stateless and needs no active kernel to be imported

CAPABILITY = "relay"

HookEvent = Literal[
    "validate", "before_save", "after_save",
    "before_delete", "after_delete",
    "after_commit", "on_rollback",
]
PRECOMMIT_EVENTS = frozenset({"validate", "before_save", "after_save", "before_delete", "after_delete"})
POSTCOMMIT_EVENTS = frozenset({"after_commit", "on_rollback"})

_console = Console()
_LOG_STYLES = {"info": "cyan", "success": "bold green", "warning": "yellow", "error": "bold red"}


class RelayError(Exception):
    """Raised via arc.relay.throw() — a business/user-facing error, not an
    internal failure. Carries enough to become an HTTP response later
    (whitelisting increment) but works identically with no Gateway involved:
    from a hook, the CLI, or a queued task it's just a normal exception to
    whatever called in."""

    def __init__(self, message: str, *, status: int = 400, code: str | None = None) -> None:
        self.message = message
        self.status = status
        self.code = code
        super().__init__(message)


@dataclass
class HookContext:
    """What a hook receives — shape depends on which event fired it:
      * validate / before_save:            old, payload
      * after_save / before_delete/after_delete: old, payload, new
      * after_commit / on_rollback:         old, new (conn is None — the
        transaction is already resolved, one way or the other, by the time
        these run; nothing here should try to write more "inside" it)
    payload is the exact dict the caller passed for THIS operation, mutable
    — a before_save hook that edits ctx.payload changes what actually gets
    written, since it's the same dict insert()/update() is called with
    afterward."""
    table: str
    old: dict | None = None
    payload: dict = field(default_factory=dict)
    new: dict | None = None
    conn: Any = None
    error: BaseException | None = None  # set for on_rollback only


HookFn = Callable[[HookContext], Awaitable[None]]


@dataclass(frozen=True)
class WhitelistedFunction:
    """One @arc.relay.whitelist()-decorated function — always callable
    directly via arc.relay.call(name, **kwargs); additionally routed
    through Gateway (if installed) at `path`. Minimal RBAC for this cut
    (see whitelist() below): with no authn installed, every caller
    resolves to role "Guest" and nothing else — a real role is a hard
    boot-time error, matching §3.3's existing rule for the same situation
    everywhere else in ARC. Full RBAC (real role resolution once authn
    exists) is still a separate, later increment."""
    name: str            # "<plugin>.<function_name>" — also arc.relay.call()'s key
    plugin: str
    fn: Callable[..., Awaitable[Any]]
    methods: list[str]
    roles: list[str]
    path: str


class RelayProvider:
    # Exposed on the instance (not just the module) so callers can do
    # `except arc.relay.RelayError:` — catching it via `from relay import
    # RelayError` would mean a business plugin importing Relay's own
    # implementation module directly, exactly what "single import" (§3.2)
    # rules out. Same reasoning for HookContext, for type-hinting a hook's
    # `ctx` parameter without that import either.
    RelayError = RelayError
    HookContext = HookContext
    WhitelistedFunction = WhitelistedFunction

    def __init__(self, kernel: Any) -> None:
        self._kernel = kernel
        self._psqldb = kernel.get("psqldb")
        self._redix = kernel.get("redix") if kernel.has("redix") else None
        self._hooks: dict[tuple[str, str], list[HookFn]] = {}
        self._whitelisted: dict[str, WhitelistedFunction] = {}
        # only set while register_hooks()/register_api() is importing one
        # file — lets the decorators below know which table/plugin they're
        # attaching to without the file itself having to say so.
        self._loading_table: str | None = None
        self._loading_plugin: str | None = None
        # Fallback cache/lock state, used only when redix isn't installed —
        # see cache_get/set/lock below for what's actually weaker about it.
        self._local_cache: dict[str, Any] = {}
        self._local_locks: dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------ #
    # Hook registration — plugins/<plugin>/hooks/<Table Name>.py, same
    # filename-to-table convention as schemas/patches (psqldb.model).
    # ------------------------------------------------------------------ #
    def register_hooks(self, hooks_dir: str | Path) -> None:
        from psqldb.model import slugify_table_name  # only dependency Relay takes on psqldb's internals

        hooks_dir = Path(hooks_dir)
        if not hooks_dir.exists():
            return
        plugin = self._kernel.current_plugin() or "<direct>"
        for path in sorted(hooks_dir.glob("*.py")):
            table = slugify_table_name(path.stem)
            self._loading_table = table
            self._loading_plugin = plugin
            try:
                module_name = f"_arc_relay_hooks_{plugin}_{table}"
                spec = importlib.util.spec_from_file_location(module_name, path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module  # standard module_from_spec usage — also lets
                spec.loader.exec_module(module)    # a hook file's own internals (dataclasses, etc.) resolve correctly
            finally:
                self._loading_table = None
                self._loading_plugin = None

    def add_hook(self, table: str, event: HookEvent, fn: HookFn) -> HookFn:
        """Callable directly, and what the decorators below delegate to."""
        self._hooks.setdefault((table, event), []).append(fn)
        return fn

    def _decorator_for(self, event: HookEvent) -> Callable[[HookFn], HookFn]:
        def decorator(fn: HookFn) -> HookFn:
            if self._loading_table is None:
                raise RuntimeError(
                    f"@arc.relay.{event} used outside register_hooks() — hook decorators only "
                    f"work on functions defined in a plugins/<plugin>/hooks/<Table>.py file "
                    f"loaded via arc.relay.register_hooks()."
                )
            return self.add_hook(self._loading_table, event, fn)
        return decorator

    @property
    def validate(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("validate")

    @property
    def before_save(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("before_save")

    @property
    def after_save(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("after_save")

    @property
    def before_delete(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("before_delete")

    @property
    def after_delete(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("after_delete")

    @property
    def after_commit(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("after_commit")

    @property
    def on_rollback(self) -> Callable[[HookFn], HookFn]:
        return self._decorator_for("on_rollback")

    def _has_hooks(self, table: str, events: frozenset[str]) -> bool:
        return any((table, event) in self._hooks for event in events)

    async def _run_hooks(self, table: str, event: HookEvent, ctx: HookContext) -> None:
        for fn in self._hooks.get((table, event), ()):
            await fn(ctx)

    # ------------------------------------------------------------------ #
    # CRUD — single-row. Every write runs inside ONE transaction spanning
    # precommit hooks + the actual psqldb write; postcommit hooks run
    # strictly after that transaction has resolved (ctx.conn is None by
    # then on purpose — see HookContext).
    # ------------------------------------------------------------------ #
    async def create(self, table: str, data: dict, *, created_by: UUID | None = None) -> dict:
        self._psqldb.schema(table)  # SchemaError early if unknown, before opening anything
        ctx = HookContext(table=table, old=None, payload=dict(data))
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    await self._run_hooks(table, "validate", ctx)
                    await self._run_hooks(table, "before_save", ctx)
                    row = await self._psqldb.insert(table, ctx.payload, created_by=created_by, conn=conn)
                    ctx.new = dict(row)
                    await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks(table, "on_rollback", ctx)
                raise
            ctx.conn = None
            await self._run_hooks(table, "after_commit", ctx)
        return ctx.new

    async def update(self, table: str, id: UUID, data: dict, *, updated_by: UUID | None = None) -> dict:
        self._psqldb.schema(table)
        ctx = HookContext(table=table, payload=dict(data))
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                        old_row = await self._psqldb.get(table, id, conn=conn)
                        ctx.old = dict(old_row) if old_row else None
                    await self._run_hooks(table, "validate", ctx)
                    await self._run_hooks(table, "before_save", ctx)
                    row = await self._psqldb.update(table, id, ctx.payload, updated_by=updated_by, conn=conn)
                    ctx.new = dict(row) if row else None
                    await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks(table, "on_rollback", ctx)
                raise
            ctx.conn = None
            await self._run_hooks(table, "after_commit", ctx)
        return ctx.new

    async def delete(self, table: str, id: UUID, *, deleted_by: UUID | None = None) -> None:
        self._psqldb.schema(table)
        ctx = HookContext(table=table)
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                        old_row = await self._psqldb.get(table, id, conn=conn)
                        ctx.old = dict(old_row) if old_row else None
                    await self._run_hooks(table, "before_delete", ctx)
                    await self._psqldb.soft_delete(table, id, deleted_by=deleted_by, conn=conn)
                    await self._run_hooks(table, "after_delete", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks(table, "on_rollback", ctx)
                raise
            ctx.conn = None
            await self._run_hooks(table, "after_commit", ctx)

    async def get(self, table: str, id: UUID) -> dict | None:
        """A plain read — no hooks involved (the taxonomy above is entirely
        about writes; see docs/arc.MD §3's hook design)."""
        row = await self._psqldb.get(table, id)
        return dict(row) if row else None

    async def get_by(self, table: str, **filters: Any) -> dict | None:
        """Look a row up by any field(s), not just id — e.g.
        `arc.relay.get_by("employee", employee_code="E001")`. Equality
        filters, first match — see psqldb.get_by() for the "why this exists
        and why it's deliberately not the full Query Engine" reasoning."""
        row = await self._psqldb.get_by(table, filters)
        return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # Batch CRUD. One shared transaction per BATCH (all-or-nothing, same
    # atomic mental model as a single create/update/delete) — not one
    # transaction per row. Hooks still fire ONCE PER ROW, not once for the
    # whole batch: a hook written for create() works unmodified for
    # create_many() with no special "batch mode" to author against.
    # Precommit hooks (validate/before_save) run for every row BEFORE the
    # one batched SQL statement executes, so any row's hook can still abort
    # the entire batch by raising; after_save/after_commit/on_rollback then
    # run per row using the real per-row result.
    # ------------------------------------------------------------------ #
    async def create_many(self, table: str, rows: list[dict], *, created_by: UUID | None = None) -> list[dict]:
        self._psqldb.schema(table)
        if not rows:
            return []
        ctxs = [HookContext(table=table, old=None, payload=dict(r)) for r in rows]
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    for ctx in ctxs:
                        ctx.conn = conn
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                    results = await self._psqldb.insert_many(
                        table, [ctx.payload for ctx in ctxs], created_by=created_by, conn=conn
                    )
                    for ctx, row in zip(ctxs, results):
                        ctx.new = dict(row)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
            for ctx in ctxs:
                await self._run_hooks(table, "after_commit", ctx)
        return [ctx.new for ctx in ctxs]

    async def update_many(self, table: str, updates: list[dict], *, updated_by: UUID | None = None) -> list[dict]:
        """`updates` is `[{"id": ..., "data": {...}}, ...]`."""
        self._psqldb.schema(table)
        if not updates:
            return []
        need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
        ctxs = [HookContext(table=table, payload=dict(u["data"])) for u in updates]
        ids = [u["id"] for u in updates]
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    if need_old:
                        old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                        old_by_id = {r["id"]: dict(r) for r in old_rows}
                        for ctx, row_id in zip(ctxs, ids):
                            ctx.old = old_by_id.get(row_id)
                    for ctx in ctxs:
                        ctx.conn = conn
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                    results = await self._psqldb.update_many(
                        table,
                        [{"id": row_id, "data": ctx.payload} for row_id, ctx in zip(ids, ctxs)],
                        updated_by=updated_by, conn=conn,
                    )
                    results_by_id = {r["id"]: dict(r) for r in results}
                    for ctx, row_id in zip(ctxs, ids):
                        ctx.new = results_by_id.get(row_id)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
            for ctx in ctxs:
                await self._run_hooks(table, "after_commit", ctx)
        return [ctx.new for ctx in ctxs]

    async def delete_many(self, table: str, ids: list[UUID], *, deleted_by: UUID | None = None) -> None:
        self._psqldb.schema(table)
        if not ids:
            return
        need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
        ctxs = [HookContext(table=table) for _ in ids]
        async with self._psqldb.acquire() as conn:
            try:
                async with conn.transaction():
                    if need_old:
                        old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                        old_by_id = {r["id"]: dict(r) for r in old_rows}
                        for ctx, row_id in zip(ctxs, ids):
                            ctx.old = old_by_id.get(row_id)
                    for ctx in ctxs:
                        ctx.conn = conn
                        await self._run_hooks(table, "before_delete", ctx)
                    await self._psqldb.soft_delete_many(table, ids, deleted_by=deleted_by, conn=conn)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_delete", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
            for ctx in ctxs:
                await self._run_hooks(table, "after_commit", ctx)

    async def get_many(self, table: str, ids: list[UUID]) -> list[dict]:
        rows = await self._psqldb.get_many(table, ids)
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Whitelisting — plugins/<plugin>/api/*.py, same controlled-loading
    # pattern as register_hooks() (files aren't table-named here — a
    # whitelisted function isn't tied to one table). Every whitelisted
    # function is ALWAYS callable directly via arc.relay.call(name, **kw)
    # regardless of whether Gateway is installed; it's ADDITIONALLY wired
    # into arc.gateway as a real route when Gateway is present. No implicit
    # REST-per-table routes anywhere — this is the only way anything in
    # Relay ever becomes web-reachable.
    # ------------------------------------------------------------------ #
    def register_api(self, api_dir: str | Path) -> None:
        api_dir = Path(api_dir)
        if not api_dir.exists():
            return
        plugin = self._kernel.current_plugin() or "<direct>"
        for path in sorted(api_dir.glob("*.py")):
            self._loading_plugin = plugin
            try:
                module_name = f"_arc_relay_api_{plugin}_{path.stem}"
                spec = importlib.util.spec_from_file_location(module_name, path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
            finally:
                self._loading_plugin = None

    def whitelist(
        self, *, methods: list[str] | None = None, roles: list[str] | None = None, path: str | None = None
    ) -> Callable[[Callable], Callable]:
        methods = methods or ["POST"]
        roles = roles or ["Guest"]

        def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            plugin = self._loading_plugin or self._kernel.current_plugin() or "<direct>"
            if not self._kernel.has("authn") and set(roles) - {"Guest"}:
                # Same rule §3.3 already applies everywhere else: declaring a
                # real role with no way to ever resolve it is a boot-time
                # config error, not something that quietly does nothing at
                # request time.
                raise RuntimeError(
                    f"whitelisted function '{fn.__name__}' (plugin '{plugin}') declares "
                    f"roles={roles}, but no authn plugin is installed — only 'Guest' can "
                    f"ever be granted without one. Install authn, or restrict this to "
                    f"roles=['Guest']."
                )
            name = f"{plugin}.{fn.__name__}"
            derived_path = path or f"/api/method/{name}"
            wf = WhitelistedFunction(name=name, plugin=plugin, fn=fn, methods=methods, roles=roles, path=derived_path)
            if wf.name in self._whitelisted:
                raise RuntimeError(f"whitelisted function '{wf.name}' is already registered.")
            self._whitelisted[wf.name] = wf
            if self._kernel.has("gateway"):
                self._wire_gateway_route(wf)
            return fn

        return decorator

    def whitelisted(self) -> list[WhitelistedFunction]:
        return list(self._whitelisted.values())

    async def call(self, name: str, **kwargs: Any) -> Any:
        """Direct invocation — no RBAC check here. This is a trusted,
        server-side call (from the CLI, a queued task, another hook), not a
        public HTTP request; the role gate only applies at the Gateway
        boundary a request actually crosses (see _wire_gateway_route)."""
        wf = self._whitelisted.get(name)
        if wf is None:
            raise RelayError(f"no whitelisted function named '{name}'", status=404, code="not_found")
        return await wf.fn(**kwargs)

    def _wire_gateway_route(self, wf: WhitelistedFunction) -> None:
        gateway = self._kernel.get("gateway")
        from gateway.request import HTTPError  # only imported when gateway is actually present

        async def handler(request: Any) -> Any:
            # Minimal RBAC for this cut (see WhitelistedFunction docstring):
            # with no authn installed, every request resolves to role
            # "Guest" and nothing else — unresolved identity defaults to
            # Guest by design, and a real role can't exist without authn
            # (already refused at registration time above).
            if "Guest" not in wf.roles:
                raise HTTPError(403, {"error": "forbidden", "detail": f"requires role(s) {wf.roles}"})
            try:
                body = request.json() if request.body else {}
            except Exception as exc:
                raise HTTPError(422, {"error": "invalid JSON body", "detail": str(exc)}) from exc
            try:
                return await wf.fn(**body)
            except RelayError as exc:
                raise HTTPError(exc.status, {"error": exc.message, "code": exc.code}) from exc

        for method in wf.methods:
            gateway.add_route(method, wf.path, handler, summary=f"whitelisted: {wf.name}")

    # ------------------------------------------------------------------ #
    # Cache / lock — genuinely redix-backed when redix is installed, and
    # genuinely weaker (not just "the same but local") when it isn't; see
    # each method for exactly what's lost. Values always round-trip through
    # arc.codec (not a second, separate serialization scheme) when redix is
    # backing the cache — the local fallback stores Python objects as-is,
    # same process, no serialization boundary to cross.
    # ------------------------------------------------------------------ #
    async def cache_get(self, key: str) -> Any:
        if self._redix is not None:
            raw = await self._redix.get(key)
            return arc.codec.decode(raw) if raw is not None else None
        return self._local_cache.get(key)

    async def cache_set(self, key: str, value: Any, *, ex: int | None = None) -> None:
        if self._redix is not None:
            await self._redix.set(key, arc.codec.encode(value), ex=ex)
            return
        self._local_cache[key] = value
        if ex is not None:
            async def _expire() -> None:
                await asyncio.sleep(ex)
                self._local_cache.pop(key, None)
            asyncio.create_task(_expire())

    async def cache_delete(self, key: str) -> None:
        if self._redix is not None:
            await self._redix.delete(key)
            return
        self._local_cache.pop(key, None)

    def lock(self, name: str, *, timeout: float = 10.0):
        """`async with arc.relay.lock("job:123"):` either way. redix-backed:
        a real distributed lock, `timeout` is its auto-expiry lease (so a
        crashed holder eventually releases it). Local fallback: a plain
        `asyncio.Lock` — protects concurrent tasks in THIS process only
        (two Gateway workers can both "hold" a same-named lock at once),
        and has no auto-expiry at all — a crashed holder deadlocks that
        lock name for the rest of the process's life. `timeout` is accepted
        for API symmetry but not enforced here."""
        if self._redix is not None:
            return self._redix.lock(name, timeout=timeout)
        return self._local_lock_cm(name)

    @contextlib.asynccontextmanager
    async def _local_lock_cm(self, name: str):
        lock = self._local_locks.setdefault(name, asyncio.Lock())
        async with lock:
            yield

    def enqueue(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> asyncio.Task:
        """NOT durable regardless of whether redix is installed — unlike
        cache/lock, this doesn't actually upgrade when redix is present,
        because TaskIQ (the real queue backend named in the tech stack)
        isn't integrated yet. This is always in-process fire-and-forget for
        now: lost on crash/restart, no retry, no persistence. Said plainly
        here rather than dressed up to look more capable than it is."""
        task = asyncio.create_task(fn(*args, **kwargs))

        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                self.log(f"enqueued task {fn.__name__} failed: {exc}", level="error")

        task.add_done_callback(_on_done)
        return task

    # ------------------------------------------------------------------ #
    # Error / logging — arc.relay.throw() / .log(). log() is console-only
    # for now (deliberate, see docs/arc.MD §8) — structured/persisted
    # logging is a separate, later decision.
    # ------------------------------------------------------------------ #
    def throw(self, message: str, *, status: int = 400, code: str | None = None) -> None:
        raise RelayError(message, status=status, code=code)

    def log(self, message: str, *, level: str = "info", **context: Any) -> None:
        style = _LOG_STYLES.get(level, "white")
        suffix = f" {context}" if context else ""
        _console.print(f"[{style}][{level.upper()}][/{style}] {message}{suffix}")

    async def health(self) -> dict:
        hook_count = sum(len(fns) for fns in self._hooks.values())
        return {
            "ok": True,
            "hooks_registered": hook_count,
            "whitelisted_functions": len(self._whitelisted),
            "cache_lock_backend": "redix" if self._redix is not None else "in-process fallback",
            "queue_backend": "in-process (TaskIQ not integrated yet)",
        }


def register(kernel: Any) -> None:
    provider = RelayProvider(kernel)
    if not kernel.has("redix"):
        kernel.advise(
            "redix not installed — arc.relay.cache_get/set/delete and arc.relay.lock() "
            "are running in an in-process fallback: cache is lost on restart and never "
            "shared across worker processes, and a crashed lock holder deadlocks that "
            "lock name for the rest of the process's life. Install redix for real "
            "guarantees. (arc.relay.enqueue() is unaffected by this — it's in-process "
            "fire-and-forget either way; see its own docstring for why.)"
        )
    kernel.export(CAPABILITY, provider, requires=["psqldb"], optional_requires=["authn", "redix", "gateway"])
