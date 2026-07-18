"""
relay — ARC application runtime (Architecture §2/§3.4).

Exports `arc.relay`: hook-wrapped CRUD over whatever tables `psqldb` has
already declared (schemas/patches, §3.9) — Relay never redeclares the data
model, it only attaches behavior to one psqldb already owns. Single-row +
batch writes (save/save_many/delete/delete_many), the full hook taxonomy,
cache/lock/queue, whitelisting with full role-intersection RBAC (§3.11/
§3.13), and the bounded Query Engine (get/list/count/aggregate/sql — see
relay.query) are all built; streaming remains a separate, later increment
— see docs/arc.MD §7 for the build order this follows.

Two things worth knowing before reading the CRUD section below:

  * Session-bound hook transactions. A hook's own nested arc.relay.* calls
    (read or write) share the enclosing operation's connection/transaction
    by default — a targeted, minimal build-out of the "scoped lifecycle"
    idea docs/arc.MD §8 names as arc.di's deferred purpose. `new_transaction=True`
    on any read/write method opts a call OUT of that sharing, for work that
    should proceed (or fail) independently of the operation it was called
    from. See _active_conn / _connection() / _write_transaction() below.
  * doc / doc.old. Every hook's HookContext carries `ctx.doc` — the current
    row as a Doc (attribute AND dict-style access: doc.salary or
    doc["salary"]), and `ctx.doc.old` — a nested, read-only Doc of the
    pre-write row (None on insert). Any field this write doesn't touch
    reads identically from doc.X and doc.old.X (falls through to the same
    old-row value either way). ctx.old/ctx.payload/ctx.new stay exactly as
    they were — doc is an additional, friendlier layer over the same data,
    not a replacement.

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
import inspect
import sys
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

from rich.console import Console

import arc  # safe at module level — same pattern gateway/request.py already uses;
            # arc.codec is stateless and needs no active kernel to be imported

from . import query

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


class Doc:
    """Attribute-style view over one row — `doc.fieldname` for the current
    value, `doc.old` for a nested, read-only Doc of the pre-write row (None
    on insert). Dict-style access (`doc["fieldname"]`, `.get(...)`,
    iteration, `.items()`) works too — a hook doing something generic like
    "for each field that changed" isn't stuck with attribute-only syntax,
    which can't take a runtime variable as a field name at all.

    During validate/before_save (the write hasn't actually happened yet),
    `doc` is a LIVE merged view: whatever's currently in the pending
    payload, overlaid on the old row — so `doc.salary` reflects "what this
    row will look like after this write," and it's live because `_overlay`
    IS `ctx.payload` (same dict object, not a copy) — a hook mutating
    ctx.payload is immediately visible through doc too. Any field this
    write doesn't touch falls through to the same old-row value for both
    `doc.X` and `doc.old.X` — they're the same data, not two separately
    tracked copies. From after_save (or after_delete/after_commit) onward,
    `doc` is replaced with the REAL persisted row and becomes read-only,
    exactly like `doc.old` always is — writing to a read-only Doc raises,
    rather than silently doing nothing.
    """

    def __init__(self, *, base: dict, overlay: dict | None, old: "Doc | None", is_new: bool) -> None:
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_overlay", overlay)  # ctx.payload while writable, else None (read-only)
        object.__setattr__(self, "old", old)
        object.__setattr__(self, "_is_new", is_new)

    def _merged(self) -> dict:
        return {**self._base, **self._overlay} if self._overlay is not None else self._base

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        overlay = self._overlay
        if overlay is not None and name in overlay:
            return overlay[name]
        try:
            return self._base[name]
        except KeyError:
            raise AttributeError(f"'{name}' is not a field on this doc.") from None

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "old" or name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if self._overlay is None:
            raise AttributeError(
                f"'{name}' is read-only on this doc — the write already happened "
                f"(or this is doc.old, which is always read-only)."
            )
        self._overlay[name] = value

    def __getitem__(self, name: str) -> Any:
        return self._merged()[name]

    def __setitem__(self, name: str, value: Any) -> None:
        setattr(self, name, value)

    def __contains__(self, name: str) -> bool:
        return name in self._merged()

    def __iter__(self):
        return iter(self._merged())

    def get(self, name: str, default: Any = None) -> Any:
        return self._merged().get(name, default)

    def keys(self):
        return self._merged().keys()

    def values(self):
        return self._merged().values()

    def items(self):
        return self._merged().items()

    def __repr__(self) -> str:
        return f"Doc({self._merged()!r}, is_new={self._is_new})"


def _old_doc(old: dict | None) -> Doc | None:
    return Doc(base=dict(old), overlay=None, old=None, is_new=False) if old is not None else None


def _precommit_doc(old: dict | None, payload: dict, *, is_new: bool) -> Doc:
    """validate/before_save's live, writable, old-overlaid-with-payload view."""
    return Doc(base=dict(old) if old is not None else {}, overlay=payload, old=_old_doc(old), is_new=is_new)


def _postwrite_doc(new: dict | None, old_doc: Doc | None, *, is_new: bool) -> Doc:
    """after_save onward — the real persisted row, read-only."""
    return Doc(base=dict(new) if new is not None else {}, overlay=None, old=old_doc, is_new=is_new)


def _delete_doc(old: dict | None) -> Doc:
    """before_delete/after_delete — there's no "new" (nothing is being
    written, the row is being removed), so doc and doc.old both show the
    same, only, snapshot: the row as it is/was."""
    old_doc = _old_doc(old)
    return Doc(base=dict(old) if old is not None else {}, overlay=None, old=old_doc, is_new=False)


@dataclass
class HookContext:
    """What a hook receives — shape depends on which event fired it:
      * validate / before_save:            old, payload, doc (live, writable)
      * after_save / before_delete/after_delete: old, payload, new, doc (read-only)
      * after_commit / on_rollback:         old, new, doc (conn is None — the
        transaction is already resolved, one way or the other, by the time
        these run; nothing here should try to write more "inside" it)
    payload is the exact dict the caller passed for THIS operation, mutable
    — a before_save hook that edits ctx.payload changes what actually gets
    written, since it's the same dict insert()/update() is called with
    afterward (and doc.fieldname = value is sugar for exactly that same
    mutation — see Doc's docstring)."""
    table: str
    old: dict | None = None
    payload: dict = field(default_factory=dict)
    new: dict | None = None
    doc: Doc | None = None
    conn: Any = None
    error: BaseException | None = None  # set for on_rollback only


HookFn = Callable[[HookContext], Awaitable[None]]


@dataclass(frozen=True)
class WhitelistedFunction:
    """One @arc.relay.whitelist()-decorated function — always callable
    directly via arc.relay.call(name, **kwargs); additionally routed
    through Gateway (if installed) at `path`. RBAC (_wire_gateway_route
    below): with no authn installed, every caller resolves to role "Guest"
    and nothing else — a real role, or "*" (any resolved identity,
    role-agnostic), is a hard boot-time error without authn installed,
    matching §3.3's rule for the same situation everywhere else in ARC.
    With authn installed, a caller's real roles (from request.identity) are
    checked against `roles`.
    wants_identity: computed once at decoration time (whitelist() below) by
    inspecting fn's own signature — true if it declares an `identity`
    parameter. Lets a function that needs to know its caller (e.g.
    authn.create_access_key) opt in just by naming the parameter, without
    every other whitelisted function (arc.relay.call() from the CLI, a
    hook, example_hr's Guest-only functions) needing to know or care.
    wants_client_ip: the same mechanism, mirrored, for a function that
    declares a `client_ip` parameter — e.g. authn.login()'s per-(ip,email)
    rate limiting, which has no other way to see the caller's resolved,
    proxy-aware IP (gateway.middleware.client_ip_middleware) short of this."""
    name: str            # "<plugin>.<function_name>" — also arc.relay.call()'s key
    plugin: str
    fn: Callable[..., Awaitable[Any]]
    methods: list[str]
    roles: list[str]
    path: str
    wants_identity: bool = False
    wants_client_ip: bool = False
    signature: Any = None  # inspect.Signature, computed once at decoration time —
                            # _wire_gateway_route uses signature.bind() to reject a
                            # malformed body as a 400 before ever calling fn, rather
                            # than recomputing inspect.signature(fn) per request


# The ambient "current write's connection" — set only for the duration of a
# write's precommit-hooks-through-write phase (see RelayProvider._write_transaction),
# read by every read/write method before deciding whether to acquire a fresh
# connection. contextvars are task-scoped and inherited across `await`s within
# the same task, so this correctly threads through arbitrarily deep hook
# nesting (a hook calling arc.relay.create(), whose OWN hooks call something
# else again, ...) with no extra plumbing per level.
_active_conn: ContextVar[Any | None] = ContextVar("arc_relay_active_conn", default=None)


class RelayProvider:
    # Exposed on the instance (not just the module) so callers can do
    # `except arc.relay.RelayError:` — catching it via `from relay import
    # RelayError` would mean a business plugin importing Relay's own
    # implementation module directly, exactly what "single import" (§3.2)
    # rules out. Same reasoning for HookContext/Doc, for type-hinting a
    # hook's `ctx`/`ctx.doc` without that import either.
    RelayError = RelayError
    HookContext = HookContext
    WhitelistedFunction = WhitelistedFunction
    Doc = Doc
    QueryError = query.QueryError  # get/list/count/aggregate/save's "bad filter/field/operator" error — see relay.query

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
        # _local_cache maps key -> (value, monotonic-expiry-or-None): expiry
        # is checked on read rather than via a detached asyncio timer task
        # (which asyncio only weak-references — it could be GC-cancelled and
        # silently never expire the key). _local_locks holds a refcount per
        # name so an entry is dropped the moment the last holder/waiter
        # leaves — the earlier grow-forever dict leaked one permanent Lock
        # per distinct save() match_on value.
        self._local_cache: dict[str, tuple[Any, float | None]] = {}
        self._local_locks: dict[str, tuple[asyncio.Lock, int]] = {}
        # Strong references to fire-and-forget tasks (enqueue's handoff/
        # fallback runners) — asyncio holds only a WEAK reference to a task,
        # so one nothing else references can be garbage-collected mid-flight
        # and silently cancelled. Same fix authn already carries (§3.13).
        self._background_tasks: set[asyncio.Task] = set()

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
            # system=True only preserves a leading "_" when the filename itself
            # has one (psqldb.model.slugify_table_name) — safe unconditionally,
            # a non-underscore filename like "Employee.py" is unaffected. Without
            # this, a hook file for a "system": true table (e.g. "_users.py")
            # would resolve to table key "users", never matching the real table
            # "_users" — the hook would silently never fire.
            table = slugify_table_name(path.stem, system=True)
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

    async def _run_hooks_resolved(self, table: str, event: HookEvent, ctx: HookContext) -> None:
        """For on_rollback/after_commit ONLY — events that fire after the
        transaction's outcome is already decided. A hook raising here must
        never mask the original failure (on_rollback runs inside the except
        block, so an uncaught hook error would REPLACE the exception the
        caller needs to see) nor make a committed write look failed
        (after_commit runs after a successful commit). Logged and
        swallowed; precommit hooks keep their raise-aborts-the-write
        semantics untouched via _run_hooks above."""
        for fn in self._hooks.get((table, event), ()):
            try:
                await fn(ctx)
            except Exception as exc:
                self.log(
                    f"{event} hook '{getattr(fn, '__name__', fn)!s}' on '{table}' raised "
                    f"{type(exc).__name__}: {exc} — ignored (transaction already resolved)",
                    level="error",
                )

    def _track_task(self, task: asyncio.Task) -> asyncio.Task:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    # ------------------------------------------------------------------ #
    # Session-bound connections (module docstring; docs/arc.MD §8's arc.di
    # gap, minimal/targeted version). _connection() is what every read AND
    # write method goes through to get a connection; _write_transaction()
    # additionally PUBLISHES it as the ambient one for the duration, so a
    # hook's own nested arc.relay.* calls pick it up automatically —
    # correct nested-transaction semantics come for free from asyncpg,
    # which turns a conn.transaction() called while already inside one on
    # the same connection into a real SAVEPOINT: if the outer write later
    # fails, a nested write sharing its connection rolls back with it,
    # because it was never a separate transaction to begin with.
    # ------------------------------------------------------------------ #
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
        async with self._connection(new_transaction=new_transaction) as conn:
            token = _active_conn.set(conn)
            try:
                yield conn
            finally:
                _active_conn.reset(token)

    # ------------------------------------------------------------------ #
    # CRUD — single-row. Every write runs inside ONE transaction spanning
    # precommit hooks + the actual psqldb write; postcommit hooks run
    # strictly after that transaction has resolved AND its connection has
    # been released back to the pool (ctx.conn is None by then on purpose
    # — see HookContext — and postcommit hooks never share the ambient
    # connection either, since by definition there's no live transaction
    # left to share).
    #
    # create()/update() from earlier cuts are now private (_insert/_update)
    # — save() below is the one public entry point (docs/relay/Sample.MD).
    # Not kept as deprecated aliases: this project carries no version
    # number and no back-compat promise yet (docs/arc.MD §1).
    # ------------------------------------------------------------------ #
    async def _insert(self, table: str, data: dict, *, by: str | None = None, new_transaction: bool = False) -> dict:
        self._psqldb.schema(table)  # SchemaError early if unknown, before opening anything
        ctx = HookContext(table=table, old=None, payload=dict(data))
        ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    await self._run_hooks(table, "validate", ctx)
                    await self._run_hooks(table, "before_save", ctx)
                    row = await self._psqldb.insert(table, ctx.payload, created_by=by, conn=conn)
                    ctx.new = dict(row)
                    ctx.doc = _postwrite_doc(ctx.new, None, is_new=True)
                    await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            ctx.conn = None
        await self._run_hooks_resolved(table, "after_commit", ctx)
        return ctx.new

    async def _update(
        self, table: str, id: UUID, data: dict, *, by: str | None = None, new_transaction: bool = False
    ) -> dict | None:
        self._psqldb.schema(table)
        ctx = HookContext(table=table, payload=dict(data))
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                        old_row = await self._psqldb.get(table, id, conn=conn)
                        ctx.old = dict(old_row) if old_row else None
                    ctx.doc = _precommit_doc(ctx.old, ctx.payload, is_new=False)
                    await self._run_hooks(table, "validate", ctx)
                    await self._run_hooks(table, "before_save", ctx)
                    row = await self._psqldb.update(table, id, ctx.payload, updated_by=by, conn=conn)
                    ctx.new = dict(row) if row else None
                    ctx.doc = _postwrite_doc(ctx.new, ctx.doc.old, is_new=False)
                    await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            ctx.conn = None
        await self._run_hooks_resolved(table, "after_commit", ctx)
        return ctx.new

    async def save(
        self,
        table: str,
        data: dict,
        *,
        match_on: list[str] | None = None,
        by: str | None = None,
        new_transaction: bool = False,
    ) -> dict:
        """Insert-or-update, one method (docs/relay/Sample.MD).

        `data["id"]` present -> update that row directly, no match_on
        needed — a hard QueryError if no row has that id (a stale id
        silently doing nothing, and returning None, is the one behavior
        nobody would choose from an "insert-or-update" contract).
        Otherwise, `match_on` given -> look up by those field(s):
        0 matches -> insert, 1 match -> update, MORE than 1 -> a hard
        error — save() is a strictly single-row contract; use save_many()
        for an intentional fan-out. Neither id nor match_on -> plain
        insert.

        match_on doesn't have to name a DB-unique field — if you want that
        enforced even for direct create()/save() calls bypassing this
        lookup, that's a `validate` hook's job, not this method's. What
        THIS method guarantees on its own: the ENTIRE lookup-then-branch
        sequence — including the no-match insert — runs inside one
        arc.relay.lock(), so two concurrent save() calls for the SAME
        match_on values can't both see "no match" and both insert — the
        classic upsert race. (Real guarantee with redix installed; same
        weaker in-process-only fallback as every other arc.relay.lock()
        use otherwise — see lock()'s own docstring.)
        """
        if data.get("id") is not None:
            payload = {k: v for k, v in data.items() if k != "id"}
            updated = await self._update(table, data["id"], payload, by=by, new_transaction=new_transaction)
            if updated is None:
                raise query.QueryError(
                    f"save(): no row on '{table}' with id {data['id']!r} — an explicit id "
                    f"always means update; to insert, omit the id (or use match_on)."
                )
            return updated

        if match_on:
            filters = {f: data[f] for f in match_on}
            # str() every value so the same logical key always locks the same
            # name regardless of how the caller spelled it (UUID vs str, ...).
            lock_key = f"relay:save:{table}:{sorted((k, str(v)) for k, v in filters.items())}"
            async with self.lock(lock_key):
                matches = await self.list(
                    table, filters=filters, fields=["id"], limit=2, new_transaction=new_transaction
                )
                if len(matches) > 1:
                    raise query.QueryError(
                        f"save(): match_on {filters} matched more than one row on '{table}' — "
                        f"save() only ever affects exactly one row; use save_many() for a bulk match."
                    )
                if matches:
                    payload = {k: v for k, v in data.items() if k not in match_on}
                    return await self._update(
                        table, matches[0]["id"], payload, by=by, new_transaction=new_transaction
                    )
                # The insert MUST happen while the lock is still held — it used
                # to sit after this block, so the lock was released between
                # "saw no match" and "inserted", and two concurrent saves could
                # both reach here and both insert: the exact race the lock
                # exists to prevent.
                payload = {k: v for k, v in data.items() if k != "id"}
                return await self._insert(table, payload, by=by, new_transaction=new_transaction)

        payload = {k: v for k, v in data.items() if k != "id"}
        return await self._insert(table, payload, by=by, new_transaction=new_transaction)

    async def delete(self, table: str, id: UUID, *, by: str | None = None, new_transaction: bool = False) -> None:
        self._psqldb.schema(table)
        ctx = HookContext(table=table)
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    ctx.conn = conn
                    if self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS):
                        old_row = await self._psqldb.get(table, id, conn=conn)
                        ctx.old = dict(old_row) if old_row else None
                    ctx.doc = _delete_doc(ctx.old)
                    await self._run_hooks(table, "before_delete", ctx)
                    await self._psqldb.soft_delete(table, id, deleted_by=by, conn=conn)
                    await self._run_hooks(table, "after_delete", ctx)
            except Exception as exc:
                ctx.conn = None
                ctx.error = exc
                await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            ctx.conn = None
        await self._run_hooks_resolved(table, "after_commit", ctx)

    # ------------------------------------------------------------------ #
    # Query Engine (docs/arc.MD §3.4) — get/list/count/aggregate all funnel
    # through query.build_select/build_count/build_aggregate; none of them
    # run hooks (the taxonomy above is entirely about writes). All of them
    # honor the ambient connection (see _connection() above) by default —
    # a read inside a hook sees that write's own uncommitted changes,
    # unless new_transaction=True asks for a genuinely independent read.
    # ------------------------------------------------------------------ #
    async def get(
        self,
        table: str,
        key: UUID | str | dict[str, Any],
        fields: list[str | query.Resolve] | None = None,
        *,
        new_transaction: bool = False,
    ) -> dict | None:
        """Single row, by id (`key` a UUID/str) or by any other field(s)
        (`key` a dict of equality filters, first match) — replaces the old
        separate get()/get_by(). `fields` is the same query-engine-native
        projection list() takes, including arc.relay.resolve(...) entries:

            await arc.relay.get("employee", employee_id)                     # by id
            await arc.relay.get("employee", {"employee_code": "E001"})       # by any field
            await arc.relay.get("employee", {"employee_code": "E001"},
                                 ["full_name", arc.relay.resolve("department", ["dept_name", "code"])])
        """
        self._psqldb.schema(table)  # SchemaError early if unknown, before building anything
        filters = key if isinstance(key, dict) else {"id": key}
        rows = await self._select(
            table, filters=filters, fields=fields, order_by=None, limit=1, offset=0, distinct=False,
            new_transaction=new_transaction,
        )
        return rows[0] if rows else None

    async def list(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None = None,
        fields: list[str | query.Resolve] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
        distinct: bool = False,
        new_transaction: bool = False,
    ) -> list[dict]:
        """Multiple rows. `limit=None` (the default) fetches everything that
        matches `filters` — no implicit cap. That's a deliberate choice, not
        an oversight: pass an explicit `limit` for anything user-facing
        where an unbounded result would be a real problem; the engine won't
        make that call for you."""
        self._psqldb.schema(table)
        return await self._select(
            table, filters=filters, fields=fields, order_by=order_by, limit=limit, offset=offset,
            distinct=distinct, new_transaction=new_transaction,
        )

    async def _select(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None,
        fields: list[str | query.Resolve] | None,
        order_by: list[str] | None,
        limit: int | None,
        offset: int,
        distinct: bool,
        new_transaction: bool = False,
    ) -> list[dict]:
        schema = self._psqldb.schema(table)
        sql, params = query.build_select(
            table, schema, filters=filters, fields=fields, order_by=order_by, limit=limit, offset=offset,
            distinct=distinct, ref_columns=self._psqldb.ref_columns(), schema_lookup=self._psqldb.schema,
        )
        async with self._connection(new_transaction=new_transaction) as conn:
            rows = await conn.fetch(sql, *params)
        return [self._shape_row(dict(r), fields) for r in rows]

    @staticmethod
    def _shape_row(flat: dict, fields: list[str | query.Resolve] | None) -> dict:
        """SELECT * (fields=None) returns the flat row unchanged, same as
        every other CRUD method. An explicit `fields` list re-nests each
        arc.relay.resolve(...) entry's flat "field.subfield" column aliases
        back into row[field] = {subfield: value, ...} — everything else
        (plain column names) passes through as a top-level key."""
        if fields is None:
            return flat
        out: dict[str, Any] = {}
        for item in fields:
            if isinstance(item, query.Resolve):
                out[item.field] = {sf: flat.get(f"{item.field}.{sf}") for sf in item.subfields}
            else:
                out[item] = flat.get(item)
        return out

    async def count(self, table: str, *, filters: dict[str, Any] | None = None, new_transaction: bool = False) -> int:
        schema = self._psqldb.schema(table)
        sql, params = query.build_count(table, schema, filters=filters, ref_columns=self._psqldb.ref_columns())
        async with self._connection(new_transaction=new_transaction) as conn:
            return await conn.fetchval(sql, *params)

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
            table, schema, group_by=group_by, aggregates=aggregates, filters=filters,
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

    async def sql_one(self, statement: str, *params: Any, new_transaction: bool = False) -> dict | None:
        async with self._connection(new_transaction=new_transaction) as conn:
            row = await conn.fetchrow(statement, *params)
        return dict(row) if row else None

    async def sql_val(self, statement: str, *params: Any, new_transaction: bool = False) -> Any:
        async with self._connection(new_transaction=new_transaction) as conn:
            return await conn.fetchval(statement, *params)

    # ------------------------------------------------------------------ #
    # Batch writes. One shared transaction per BATCH (all-or-nothing, same
    # atomic mental model as a single save/delete) — not one transaction
    # per row. Hooks still fire ONCE PER ROW, not once for the whole batch:
    # a hook written for save() works unmodified for save_many() with no
    # special "batch mode" to author against. Precommit hooks (validate/
    # before_save) run for every row BEFORE the batched SQL statement(s)
    # execute, so any row's hook can still abort the entire batch by
    # raising; after_save/after_commit/on_rollback then run per row using
    # the real per-row result.
    #
    # create_many()/update_many() are now private (_insert_many/_update_many)
    # — save_many() below is the one public entry point. get_many() is gone
    # entirely: it was a special case of list(table, filters={"id": {"in": ids}}),
    # not a distinct capability.
    # ------------------------------------------------------------------ #
    async def _insert_many(
        self, table: str, rows: list[dict], *, by: str | None = None, new_transaction: bool = False
    ) -> list[dict]:
        self._psqldb.schema(table)
        if not rows:
            return []
        ctxs = [HookContext(table=table, old=None, payload=dict(r)) for r in rows]
        for ctx in ctxs:
            ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    for ctx in ctxs:
                        ctx.conn = conn
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                    results = await self._psqldb.insert_many(
                        table, [ctx.payload for ctx in ctxs], created_by=by, conn=conn
                    )
                    for ctx, row in zip(ctxs, results):
                        ctx.new = dict(row)
                        ctx.doc = _postwrite_doc(ctx.new, None, is_new=True)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
        for ctx in ctxs:
            await self._run_hooks_resolved(table, "after_commit", ctx)
        return [ctx.new for ctx in ctxs]

    async def _update_many(
        self, table: str, updates: list[dict], *, by: str | None = None, new_transaction: bool = False
    ) -> list[dict]:
        """`updates` is `[{"id": ..., "data": {...}}, ...]`."""
        self._psqldb.schema(table)
        if not updates:
            return []
        need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
        ctxs = [HookContext(table=table, payload=dict(u["data"])) for u in updates]
        ids = [u["id"] for u in updates]
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    if need_old:
                        old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                        old_by_id = {r["id"]: dict(r) for r in old_rows}
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
                        updated_by=by, conn=conn,
                    )
                    results_by_id = {r["id"]: dict(r) for r in results}
                    for ctx, row_id in zip(ctxs, ids):
                        ctx.new = results_by_id.get(row_id)
                        ctx.doc = _postwrite_doc(ctx.new, ctx.doc.old, is_new=False)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
        for ctx in ctxs:
            await self._run_hooks_resolved(table, "after_commit", ctx)
        return [ctx.new for ctx in ctxs]

    async def save_many(
        self,
        table: str,
        rows: list[dict],
        *,
        match_on: list[str] | None = None,
        by: str | None = None,
        limit: int | None = None,
        new_transaction: bool = False,
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

        Known simplification, not an oversight: unlike save(), this does
        NOT take a per-key arc.relay.lock() around match_on resolution —
        it's meant for bulk/import-style work, already inside one shared
        transaction, not a guarantee against a CONCURRENT save()/save_many()
        call racing the exact same match_on values. Add that locking here
        too if it ever becomes a real scenario, not before.
        """
        self._psqldb.schema(table)
        if not rows:
            return []
        insert_ctxs: list[HookContext] = []
        update_ctxs: list[HookContext] = []
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    resolved: list[tuple[str, UUID | None, dict]] = []
                    for row in rows:
                        row_id = row.get("id")
                        if row_id is not None:
                            resolved.append(("update", row_id, {k: v for k, v in row.items() if k != "id"}))
                            continue
                        if match_on:
                            filters = {f: row[f] for f in match_on}
                            cap = (limit + 1) if limit is not None else None
                            matches = await self._select(
                                table, filters=filters, fields=["id"], order_by=None,
                                limit=cap, offset=0, distinct=False,
                            )
                            if limit is not None and len(matches) > limit:
                                raise query.QueryError(
                                    f"save_many(): match_on {filters} matched more than "
                                    f"limit={limit} row(s) on '{table}'."
                                )
                            if matches:
                                data = {k: v for k, v in row.items() if k not in match_on}
                                for m in matches:
                                    resolved.append(("update", m["id"], data))
                                continue
                        resolved.append(("insert", None, {k: v for k, v in row.items() if k != "id"}))

                    for kind, _rid, data in resolved:
                        if kind == "insert":
                            ctx = HookContext(table=table, old=None, payload=dict(data))
                            ctx.doc = _precommit_doc(None, ctx.payload, is_new=True)
                            insert_ctxs.append(ctx)
                    update_targets = [(rid, data) for kind, rid, data in resolved if kind == "update"]
                    for _rid, data in update_targets:
                        update_ctxs.append(HookContext(table=table, payload=dict(data)))

                    need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
                    if need_old and update_targets:
                        old_rows = await self._psqldb.get_many(table, [rid for rid, _ in update_targets], conn=conn)
                        old_by_id = {r["id"]: dict(r) for r in old_rows}
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
                            table, [c.payload for c in insert_ctxs], created_by=by, conn=conn
                        )
                        for c, r in zip(insert_ctxs, results):
                            c.new = dict(r)
                            c.doc = _postwrite_doc(c.new, None, is_new=True)
                    if update_ctxs:
                        results = await self._psqldb.update_many(
                            table,
                            [{"id": rid, "data": c.payload} for (rid, _d), c in zip(update_targets, update_ctxs)],
                            updated_by=by, conn=conn,
                        )
                        results_by_id = {r["id"]: dict(r) for r in results}
                        for (rid, _d), c in zip(update_targets, update_ctxs):
                            c.new = results_by_id.get(rid)
                            c.doc = _postwrite_doc(c.new, c.doc.old, is_new=False)

                    for ctx in all_ctxs:
                        await self._run_hooks(table, "after_save", ctx)
            except Exception as exc:
                for ctx in [*insert_ctxs, *update_ctxs]:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            for ctx in [*insert_ctxs, *update_ctxs]:
                ctx.conn = None
        for ctx in [*insert_ctxs, *update_ctxs]:
            await self._run_hooks_resolved(table, "after_commit", ctx)
        return [ctx.new for ctx in [*insert_ctxs, *update_ctxs]]

    async def delete_many(
        self, table: str, ids: list[UUID], *, by: str | None = None, new_transaction: bool = False
    ) -> None:
        self._psqldb.schema(table)
        if not ids:
            return
        need_old = self._has_hooks(table, PRECOMMIT_EVENTS | POSTCOMMIT_EVENTS)
        ctxs = [HookContext(table=table) for _ in ids]
        async with self._write_transaction(new_transaction=new_transaction) as conn:
            try:
                async with conn.transaction():
                    if need_old:
                        old_rows = await self._psqldb.get_many(table, ids, conn=conn)
                        old_by_id = {r["id"]: dict(r) for r in old_rows}
                        for ctx, row_id in zip(ctxs, ids):
                            ctx.old = old_by_id.get(row_id)
                    for ctx in ctxs:
                        ctx.doc = _delete_doc(ctx.old)
                    for ctx in ctxs:
                        ctx.conn = conn
                        await self._run_hooks(table, "before_delete", ctx)
                    await self._psqldb.soft_delete_many(table, ids, deleted_by=by, conn=conn)
                    for ctx in ctxs:
                        await self._run_hooks(table, "after_delete", ctx)
            except Exception as exc:
                for ctx in ctxs:
                    ctx.conn = None
                    ctx.error = exc
                    await self._run_hooks_resolved(table, "on_rollback", ctx)
                raise
            for ctx in ctxs:
                ctx.conn = None
        for ctx in ctxs:
            await self._run_hooks_resolved(table, "after_commit", ctx)

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

    # ------------------------------------------------------------------ #
    # Background/scheduled jobs — plugins/<plugin>/tasks/*.py, loaded via
    # register_tasks(), same directory-loading pattern as hooks/api.
    #
    # This is the ONLY surface a business plugin should ever call for
    # this — never `arc.lineup.task(...)`/`arc.lineup.register_tasks(...)`
    # directly (docs/arc.MD §3.15). Same posture as cache_get/cache_set/
    # lock() above: relay is the facade every plugin writes against,
    # `lineup` (like `redix`) is an optional power source relay reaches
    # for when installed, never a dependency a business plugin declares
    # or thinks about itself. A plugin that only ever calls arc.relay.task/
    # register_tasks/enqueue needs no `optional_requires=["lineup"]` of
    # its own at all — the boot-order guarantee it'd otherwise need comes
    # for free, transitively, through relay's OWN optional_requires on
    # lineup (§3.1's resolver treats optional_requires as a real
    # topological edge when both plugins are present; since every business
    # plugin already hard-requires relay, and relay->lineup is such an
    # edge whenever lineup is installed, lineup is guaranteed to boot
    # before every plugin downstream of relay too — verified against a
    # real `arc doctor` run after removing example_hr's own direct
    # optional_requires=["lineup"]).
    # ------------------------------------------------------------------ #
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
            sig = inspect.signature(fn)
            wf = WhitelistedFunction(
                name=name, plugin=plugin, fn=fn, methods=methods, roles=roles, path=derived_path,
                wants_identity="identity" in sig.parameters, wants_client_ip="client_ip" in sig.parameters,
                signature=sig,
            )
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
            # request.identity is None whenever authn isn't installed, or a
            # request carries no valid credentials — both collapse to
            # caller_roles = {"Guest"}, identical to this cut's old
            # Guest-only behavior, so roles=["Guest"] endpoints (e.g.
            # example_hr's) are unaffected either way. "*" means "any
            # resolved identity, role-agnostic" — declaring it with no authn
            # installed is already refused at registration time above (only
            # "Guest" can ever be granted without one), so by the time this
            # runs, "*" here always means authn IS installed.
            identity = getattr(request, "identity", None)
            caller_roles = set(getattr(identity, "roles", None) or [])
            if "*" in wf.roles:
                authorized = identity is not None
            elif "*" in caller_roles:
                # The same "*" sentinel, meaning the same thing symmetrically
                # on the CALLER's side instead of the endpoint's: a caller
                # whose own roles include the wildcard satisfies any specific
                # role requirement, not just an endpoint that opted into
                # roles=["*"]. Relay itself has no idea what grants this —
                # it's authn's own convention (a "Superuser" role causing
                # resolve_identity to inject "*") to decide who qualifies;
                # relay only needs to know the one sentinel already has this
                # meaning on the other side of the same check.
                authorized = True
            else:
                authorized = bool((caller_roles | {"Guest"}) & set(wf.roles))
            if not authorized:
                raise HTTPError(403, {"error": "forbidden", "detail": f"requires role(s) {wf.roles}"})
            try:
                body = request.json() if request.body else {}
            except Exception as exc:
                raise HTTPError(422, {"error": "invalid JSON body", "detail": str(exc)}) from exc
            # A JSON body that decodes to a list/string/number/etc. isn't
            # spreadable as **kwargs at all — reject it as a client mistake
            # (422) here, rather than letting it become a bare TypeError
            # below that would otherwise escape as an unstructured 500.
            if not isinstance(body, dict):
                raise HTTPError(422, {"error": "JSON body must be an object", "detail": type(body).__name__})
            kwargs = dict(body)
            if wf.wants_identity:
                # Always the server-resolved identity, never a client-supplied
                # "identity" key in the body — this overwrites it deliberately.
                kwargs["identity"] = identity
            if wf.wants_client_ip:
                kwargs["client_ip"] = getattr(request, "client_ip", None)
            try:
                # Bind (never call) against the real signature first — this
                # is where a body with missing/unexpected/mistyped keys
                # surfaces as TypeError, entirely separate from actually
                # invoking wf.fn. Doing it this way means a TypeError raised
                # from INSIDE wf.fn's own body (a genuine bug) is never
                # mistaken for a client mistake here — only this bind() call
                # can produce this specific 400, the real call below cannot.
                wf.signature.bind(**kwargs)
            except TypeError as exc:
                raise HTTPError(400, {"error": "invalid arguments", "detail": str(exc)}) from exc
            try:
                return await wf.fn(**kwargs)
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
    # Every key this cache ever touches gets a stable "cache:" prefix,
    # transparently — callers still just pass their own logical key name.
    # This exists purely so `arc clear-cache` (the kernel's own CLI) can
    # find and delete exactly these entries without also catching
    # authn's session/access-key cache (its own "session:"/"access_key:"
    # prefixes), lineup's job queues ("lineup:<queue>"), or redix's rate
    # limit counters ("ratelimit:<key>") — all sharing the same Redis
    # instance. Verified safe to add: nothing in this codebase calls
    # cache_get/cache_set/cache_delete today, so there's no existing raw
    # key name anywhere that this would silently stop matching.
    def _cache_key(self, key: str) -> str:
        return f"cache:{key}"

    async def cache_get(self, key: str) -> Any:
        if self._redix is not None:
            raw = await self._redix.get(self._cache_key(key))
            return arc.codec.decode(raw) if raw is not None else None
        entry = self._local_cache.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._local_cache.pop(key, None)
            return None
        return value

    async def cache_set(self, key: str, value: Any, *, ex: int | None = None) -> None:
        if self._redix is not None:
            await self._redix.set(self._cache_key(key), arc.codec.encode(value), ex=ex)
            return
        # Expiry is a stored deadline checked on read — NOT a detached
        # asyncio.sleep() task (asyncio only weak-references tasks; an
        # unreferenced one can be GC-cancelled and the key then never
        # expires). Setting also sweeps any already-expired entries so the
        # fallback dict can't grow unboundedly on write-once keys.
        self._local_cache[key] = (value, time.monotonic() + ex if ex is not None else None)
        if len(self._local_cache) % 256 == 0:
            now = time.monotonic()
            for k in [k for k, (_v, exp) in self._local_cache.items() if exp is not None and now >= exp]:
                self._local_cache.pop(k, None)

    async def cache_delete(self, key: str) -> None:
        if self._redix is not None:
            await self._redix.delete(self._cache_key(key))
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
        # Refcounted: the entry lives while any task holds OR waits on the
        # lock, and is dropped when the last one leaves — a plain
        # setdefault() dict grew one permanent Lock per distinct name for
        # the life of the process (save()'s match_on locks are keyed by
        # DATA VALUES, so that was an unbounded leak). The refcount, not
        # lock.locked(), decides removal: popping a lock another task still
        # waits on would let a newcomer mint a second Lock under the same
        # name and break mutual exclusion.
        entry = self._local_locks.get(name)
        if entry is None:
            lock, count = asyncio.Lock(), 0
        else:
            lock, count = entry
        self._local_locks[name] = (lock, count + 1)
        try:
            async with lock:
                yield
        finally:
            lock2, count2 = self._local_locks[name]
            if count2 <= 1:
                del self._local_locks[name]
            else:
                self._local_locks[name] = (lock2, count2 - 1)

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
        instead of volatile-fire-and-forget."""
        if self._kernel.has("lineup"):
            lineup = self._kernel.get("lineup")
            if lineup.is_task(fn):
                coro = lineup.enqueue(fn, *args, **kwargs)
            else:
                lineup.check_resolvable(fn)  # raises TypeError here, synchronously, before any Task exists
                coro = lineup.enqueue_by_path(fn, *args, queue=queue, **kwargs)

            # _track_task: most callers drop the returned Task — without a
            # strong reference here it could be GC-cancelled mid-handoff.
            task = self._track_task(asyncio.create_task(coro))

            def _on_enqueue_done(t: asyncio.Task) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    self.log(f"lineup enqueue for task {fn.__name__} failed: {exc}", level="error")

            task.add_done_callback(_on_enqueue_done)
            return task

        started_at = datetime.now(timezone.utc)

        async def _run_and_log() -> Any:
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
                            "queued_by": (getattr(fn, "__module__", "") or "").split(".")[0] or None,
                            "status": status,
                            "error": error,
                            "started_at": started_at,
                            "finished_at": finished_at,
                            "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
                        },
                    )
                except Exception as log_exc:
                    self.log(f"failed to write _job_log row for {fn.__name__}: {log_exc}", level="error")

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
            "queue_backend": "lineup (TaskIQ + Redis)" if self._kernel.has("lineup") else "in-process fallback",
        }


def register(kernel: Any) -> None:
    # The one knob governing _job_log retention (_maintenance.prune_job_log)
    # — declared here so it's discoverable via arc.settings.list_all() and
    # admin's Settings page (§3.5: declare once, by the owning plugin).
    kernel.settings.declare("job_log_retention_days")

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

    # relay's own operational table (§3.15) — logs every job it directly
    # ran itself (the in-process enqueue() fallback, see enqueue() above);
    # lineup writes into this same table too, from its own worker/scheduler
    # processes, for everything IT runs. relay owns the schema since it's
    # the common denominator: every project using lineup also has relay,
    # not the reverse.
    kernel.get("psqldb").register_model(Path(__file__).parent / "schemas")

    kernel.export(
        CAPABILITY, provider, requires=["psqldb"], optional_requires=["authn", "redix", "gateway", "lineup"]
    )

    # relay's own maintenance job, registered on itself via the exact
    # facade every other plugin uses (docs/arc.MD §3.11) — not loaded via
    # register_tasks()'s directory convention (see _maintenance.py's own
    # docstring for why that's unnecessary here).
    from relay._maintenance import prune_job_log

    provider.task(queue="default", cron="0 2 * * *")(prune_job_log)
