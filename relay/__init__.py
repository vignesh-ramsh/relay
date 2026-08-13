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
import logging
from pathlib import Path
from typing import Any

import arc  # safe at module level — same pattern gateway/request.py already uses;

# arc.codec is stateless and needs no active kernel to be imported
from . import query
from .ambient import (
    _CTX_LABEL_REQUEST_ID,
    _CTX_LABEL_ROLES,
    _CTX_LABEL_USER,
    _call_context,
)
from .ambient import _MAX_WRITE_DEPTH as _MAX_WRITE_DEPTH
from .ambient import _active_conn as _active_conn
from .ambient import _active_stream as _active_stream
from .ambient import _dry_run as _dry_run
from .ambient import _postcommit_queue as _postcommit_queue
from .ambient import _skip_hook_events as _skip_hook_events
from .ambient import _write_depth as _write_depth
from .background_jobs import BackgroundJobsMixin
from .cache_lock import _LOCAL_CACHE_MAX_ENTRIES as _LOCAL_CACHE_MAX_ENTRIES
from .cache_lock import CacheLockMixin
from .crud import CrudMixin
from .crud import _resolve_skip_events as _resolve_skip_events
from .hooks import HooksMixin
from .reads import _UNSET_LIMIT as _UNSET_LIMIT
from .reads import (
    DEFAULT_LIST_LIMIT,
    MAX_LIST_LIMIT,
    RELAY_LIST_DEFAULT_LIMIT_KEY,
    RELAY_LIST_MAX_LIMIT_KEY,
    ReadsMixin,
)
from .resolvers import FieldResolver as FieldResolver
from .shapes import _EMPTY_CALL_CONTEXT as _EMPTY_CALL_CONTEXT
from .shapes import POSTCOMMIT_EVENTS as POSTCOMMIT_EVENTS
from .shapes import PRECOMMIT_EVENTS as PRECOMMIT_EVENTS
from .shapes import (
    CallContext,
    Doc,
    HookContext,
    HookFn,
    RelayError,
    WhitelistedFunction,
)
from .shapes import HookEvent as HookEvent
from .shapes import RelayStream as RelayStream
from .shapes import _delete_doc as _delete_doc
from .shapes import _DryRunRollback as _DryRunRollback
from .shapes import _old_doc as _old_doc
from .shapes import _postwrite_doc as _postwrite_doc
from .shapes import _precommit_doc as _precommit_doc
from .transactions import TransactionsMixin
from .whitelisting import ALL_METHODS as ALL_METHODS
from .whitelisting import WhitelistingMixin

CAPABILITY = "relay"

# Everything that used to live at module level here — RelayError, Doc,
# HookContext/HookFn, WhitelistedFunction, RelayStream, CallContext,
# _DryRunRollback (relay/shapes.py); every ContextVar plus _MAX_WRITE_DEPTH
# and the _CTX_LABEL_* wire-format constants (relay/ambient.py);
# RELAY_LIST_*_KEY/DEFAULT_LIST_LIMIT/MAX_LIST_LIMIT/_UNSET_LIMIT
# (relay/reads.py); _LOCAL_CACHE_MAX_ENTRIES (relay/cache_lock.py);
# _resolve_skip_events (relay/crud.py); ALL_METHODS plus the whitelisted-
# function coercion helpers (relay/whitelisting.py) — now live in their
# own per-concern files (imported above) and are re-exported here
# unchanged, so `arc.relay.X` and direct `relay_module.X` access both keep
# working exactly as before the split.

_logger = logging.getLogger("relay")
# "success" isn't a real stdlib logging level — mapped to INFO for dispatch,
# but preserved as-is in the JSON output (arc.log.JsonFormatter reads
# record.relay_level when present) so a caller's own info/success/warning/
# error vocabulary survives into structured logs, not just console text.
_LOG_LEVELS = {
    "info": logging.INFO,
    "success": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class RelayProvider(
    CacheLockMixin,
    BackgroundJobsMixin,
    TransactionsMixin,
    HooksMixin,
    ReadsMixin,
    CrudMixin,
    WhitelistingMixin,
):
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
    CallContext = CallContext
    QueryError = (
        query.QueryError
    )  # get/list/count/aggregate/save's "bad filter/field/operator" error — see relay.query

    def __init__(
        self,
        kernel: Any,
        *,
        list_default_limit: int = DEFAULT_LIST_LIMIT,
        list_max_limit: int = MAX_LIST_LIMIT,
    ) -> None:
        self._kernel = kernel
        self._psqldb = kernel.get("psqldb")
        self._redix = kernel.get("redix") if kernel.has("redix") else None
        self._list_default_limit = list_default_limit
        self._list_max_limit = list_max_limit
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

    # Hook registration/dispatch (register_hooks, add_hook, _decorator_for,
    # the 7 event properties, _has_hooks, _run_hooks, _run_hooks_resolved)
    # now live in relay/hooks.py's HooksMixin — RelayProvider inherits from
    # it, see the class declaration below.

    def context(self) -> CallContext:
        """Who is asking, and as part of which request — readable from
        anywhere downstream of a Gateway call (a hook, a nested save, a
        background job enqueued by that request) with no plumbing.

        Always returns a real CallContext, never None: outside any request
        (a CLI run, a test, a `lineup` scheduled task) every field is
        simply empty, so a caller never needs a None check. See
        CallContext's own docstring for why it carries plain data rather
        than the live identity object."""
        return _call_context.get()

    def context_labels(self) -> dict[str, str]:
        """Encode the CURRENT CallContext as flat string labels, for a
        durable queue that has to carry it to another PROCESS (see
        lineup's enqueue paths, which stamp these onto the TaskIQ message).

        Relay owns both halves of this wire format — this and
        use_context_labels() below — precisely so `lineup` never has to
        know what a CallContext is. It moves an opaque dict of strings and
        hands it back; if relay's own context ever grows a field, only
        these two methods change, and lineup needs no edit at all. That
        also keeps the dependency direction correct: relay optionally
        depends on lineup, never the reverse.

        Strings only, and only for fields that are actually set: TaskIQ's
        own `AsyncKicker.with_labels(**labels: str | float)` types labels
        as scalars, so nothing structured may go on the wire directly.
        `roles` is JSON-encoded (via arc.codec, §3.10's one shared codec)
        rather than comma-joined — a role name containing the separator
        would silently split into two otherwise.

        Returns `{}` for an empty context, which is the common case
        outside a request — a caller can then skip labelling entirely."""
        ctx = self.context()
        labels: dict[str, str] = {}
        if ctx.request_id is not None:
            labels[_CTX_LABEL_REQUEST_ID] = ctx.request_id
        if ctx.user is not None:
            labels[_CTX_LABEL_USER] = ctx.user
        if ctx.roles:
            labels[_CTX_LABEL_ROLES] = arc.codec.encode(list(ctx.roles)).decode()
        return labels

    @contextlib.contextmanager
    def use_context_labels(self, labels: Any):
        """Decode labels produced by context_labels() and bind them as the
        ambient CallContext for the duration of this block — the receiving
        half of the wire format, called by a `lineup` worker around the
        job it is about to run.

        `labels` is whatever the queue handed back, which contains TaskIQ's
        OWN labels too (`schedule`, `schedule_id`, ...) — only the
        `arc_ctx_*` keys are read, everything else is ignored rather than
        rejected, so this stays forward-compatible with any label another
        layer adds later. A message with no arc_ctx_* keys at all (a job
        enqueued before this existed, or from a CLI with no request) binds
        a normal empty context, never an error.

        Deliberately tolerant of a malformed `roles` value: a job should
        not fail because its provenance metadata was garbled in transit —
        the roles are dropped, the request_id/user are kept, and the work
        still runs."""
        labels = labels or {}
        roles: tuple[str, ...] = ()
        raw_roles = labels.get(_CTX_LABEL_ROLES)
        if raw_roles:
            try:
                decoded = arc.codec.decode(
                    raw_roles.encode() if isinstance(raw_roles, str) else raw_roles
                )
                if isinstance(decoded, list):
                    roles = tuple(str(r) for r in decoded)
            except Exception:  # noqa: BLE001 - provenance metadata must never break the job
                _logger.warning("could not decode %s from job labels", _CTX_LABEL_ROLES)
        ctx = CallContext(
            request_id=labels.get(_CTX_LABEL_REQUEST_ID),
            user=labels.get(_CTX_LABEL_USER),
            roles=roles,
        )
        token = _call_context.set(ctx)
        try:
            yield ctx
        finally:
            _call_context.reset(token)

    # _track_task and _detach_request_scoped_state now live in relay/
    # background_jobs.py's BackgroundJobsMixin — RelayProvider inherits
    # from it, see the class declaration below.

    # Session-bound connections (_connection, _write_transaction,
    # _postcommit_scope, _flush_postcommit, _transaction_or_dry_run) now
    # live in relay/transactions.py's TransactionsMixin — RelayProvider
    # inherits from it, see the class declaration below.

    # CRUD — single-row and batch writes (_insert, _update,
    # _match_on_is_constraint_backed, _resolve_match_on, save, delete,
    # _insert_many, _update_many, save_many, delete_many) now live in
    # relay/crud.py's CrudMixin — RelayProvider inherits from it, see the
    # class declaration below.

    # all_columns/_resolve_and_cap_list_limit and the full Query Engine
    # (get, list, _select, _shape_row, _resolve_fields, count, list_page,
    # aggregate, resolve, sql, sql_one, sql_val — docs/arc.MD §3.4) now
    # live in relay/reads.py's ReadsMixin — RelayProvider inherits from it,
    # see the class declaration below.

    # Whitelisting (register_api, whitelist, whitelisted, call, stream,
    # _drive_coroutine, publish, _wire_gateway_route) now lives in relay/
    # whitelisting.py's WhitelistingMixin — RelayProvider inherits from it,
    # see the class declaration below.

    # Cache/lock (_cache_key, cache_get, cache_set, cache_delete, lock,
    # _local_lock_cm) now live in relay/cache_lock.py's CacheLockMixin —
    # RelayProvider inherits from it, see the class declaration below.

    # enqueue now lives in relay/background_jobs.py's BackgroundJobsMixin —
    # RelayProvider inherits from it, see the class declaration below.

    # ------------------------------------------------------------------ #
    # Error / logging — arc.relay.throw() / .log(). log() used to be
    # console-only, by deliberate choice, pending a later decision on real
    # structured/persisted logging (docs/arc.MD §8) — arc.log (§3.10) is
    # that decision. log() now goes through logging.getLogger("relay"),
    # same as every other plugin's own logger, so it gets arc.log's console
    # + rotating JSON file handlers for free instead of a second, separate
    # rich.Console output nothing else in the system shared.
    # ------------------------------------------------------------------ #
    def throw(
        self,
        message: str,
        *,
        status: int = 400,
        code: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        raise RelayError(message, status=status, code=code, extra=extra)

    def log(self, message: str, *, level: str = "info", **context: Any) -> None:
        _logger.log(
            _LOG_LEVELS.get(level, logging.INFO),
            message,
            extra={"relay_level": level, **context},
        )

    async def health(self) -> dict:
        hook_count = sum(len(fns) for fns in self._hooks.values())
        return {
            "ok": True,
            "hooks_registered": hook_count,
            "whitelisted_functions": len(self._whitelisted),
            "cache_lock_backend": "redix" if self._redix is not None else "in-process fallback",
            "queue_backend": "lineup (TaskIQ + Redis)"
            if self._kernel.has("lineup")
            else "in-process fallback",
        }


def register(kernel: Any) -> None:
    # The one knob governing _job_log retention (_maintenance.prune_job_log)
    # — declared here so it's discoverable via arc.settings.list_all() and
    # admin's Settings page (§3.5: declare once, by the owning plugin).
    kernel.settings.declare("job_log_retention_days")

    # list()/list_page()'s row cap — see the module-level constants' own
    # docstring for the full split between "default" and "max". Plain
    # settings, not secret: there's nothing sensitive about a row-count
    # ceiling.
    kernel.settings.declare(
        RELAY_LIST_DEFAULT_LIMIT_KEY,
        type=int,
        default=DEFAULT_LIST_LIMIT,
        doc="Row cap for arc.relay.list()/list_page() when the caller doesn't pass "
        "`limit` at all. Pass limit=None on a specific call for a genuinely "
        "unbounded read.",
    )
    kernel.settings.declare(
        RELAY_LIST_MAX_LIMIT_KEY,
        type=int,
        default=MAX_LIST_LIMIT,
        doc="Hard ceiling on any explicit `limit` passed to arc.relay.list()/"
        "list_page() — a caller asking for more gets a clear QueryError, never a "
        "silent truncation. Pass limit=None (never a large number) for an "
        "intentionally unbounded read.",
    )
    list_default_limit = kernel.settings.get(RELAY_LIST_DEFAULT_LIMIT_KEY)
    list_max_limit = kernel.settings.get(RELAY_LIST_MAX_LIMIT_KEY)
    if list_default_limit > list_max_limit:
        raise RuntimeError(
            f"'{RELAY_LIST_DEFAULT_LIMIT_KEY}' ({list_default_limit}) is greater than "
            f"'{RELAY_LIST_MAX_LIMIT_KEY}' ({list_max_limit}) — every ordinary "
            f"list()/list_page() call (no explicit limit) would immediately exceed "
            f"the configured maximum. Lower the default or raise the max."
        )

    provider = RelayProvider(
        kernel, list_default_limit=list_default_limit, list_max_limit=list_max_limit
    )
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
    kernel.get("psqldb").register_model(Path(__file__).parent.parent / "schemas")

    kernel.export(
        CAPABILITY,
        provider,
        requires=["psqldb"],
        optional_requires=["authn", "redix", "gateway", "lineup"],
    )

    # relay's own maintenance job, registered on itself via the exact
    # facade every other plugin uses (docs/arc.MD §3.11) — not loaded via
    # register_tasks()'s directory convention (see _maintenance.py's own
    # docstring for why that's unnecessary here).
    from relay._maintenance import prune_job_log

    provider.task(queue="default", cron="0 2 * * *")(prune_job_log)
