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
import logging
import sys
import time as time_module
import types
import typing
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

import arc  # safe at module level — same pattern gateway/request.py already uses;

# arc.codec is stateless and needs no active kernel to be imported
from . import query
from .resolvers import FieldResolver

CAPABILITY = "relay"

# list()/list_page()'s row cap (§9/§3 — the doc's own "the day a 2M-row
# table meets a UI that forgot `limit`" scenario), configurable per project
# via arc.settings (declared in register() below, plain — not secret,
# there's nothing sensitive about a row-count ceiling).
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

# Cap on the in-process cache_get/cache_set fallback (no redix installed) —
# see cache_set's own comment for why the existing expiry sweep alone isn't
# enough. A plain constant, not a setting: this is a "does this deployment
# have redix or not" concern, not something a project would ever want to
# tune per environment.
_LOCAL_CACHE_MAX_ENTRIES = 10_000

# Cap on how deep a write can nest inside another write's own hooks (a
# hook calling arc.relay.save()/delete(), whose own hook calls another,
# ...) before this is almost certainly a bug (an infinite loop, or a hook
# that recurses without a base case) rather than a legitimate call chain
# — see _write_transaction's own use of this.
_MAX_WRITE_DEPTH = 10

# The concrete verb set a whitelisted function registers when `methods` is
# left unspecified (docs: methods is a RESTRICTION when given, not a
# required allowlist — the function body, plus dry_run below, decides the
# actual behavior, RPC-style). OPTIONS is deliberately excluded — CORS
# preflight is already handled entirely by cors_middleware upstream of
# routing, so a whitelisted route never needs to own that verb itself.
# QUERY (IETF draft, safe + idempotent like GET but WITH a body) is
# included here for the same "RPC-style, verb doesn't restrict" reasoning
# — gateway.router matches methods as plain strings (no fixed enum) and
# Granian passes arbitrary method tokens straight through to ASGI
# (verified directly: `curl -X QUERY` reaches this app's own routing, not
# a transport-level rejection), so there's nothing QUERY-specific either
# of them needs to know about. A client that can't send QUERY (older
# browser fetch()/XHR implementations) always has GET or POST as a
# fallback on any endpoint that hasn't deliberately restricted `methods`.
ALL_METHODS = ("GET", "QUERY", "POST", "PUT", "PATCH", "DELETE")

HookEvent = Literal[
    "validate",
    "before_save",
    "after_save",
    "before_delete",
    "after_delete",
    "after_commit",
    "on_rollback",
]
PRECOMMIT_EVENTS = frozenset(
    {"validate", "before_save", "after_save", "before_delete", "after_delete"}
)
POSTCOMMIT_EVENTS = frozenset({"after_commit", "on_rollback"})

# Injected by _wire_gateway_route itself (identity/client_ip/cookies/request/
# dry_run/request_id) — never sourced from the caller's own query/body/path,
# so these names are never candidates for the coercion/payload inspection
# below, whatever a function happens to annotate them as. request_id in
# particular MUST be here rather than left to ordinary kwarg handling: an
# inbound `?request_id=...` would otherwise let a caller forge its own
# correlation id straight into the logs.
_INJECTED_PARAM_NAMES = frozenset(
    {"identity", "client_ip", "cookies", "request", "dry_run", "request_id"}
)

# Every scalar type arc.codec (msgspec) can coerce a raw string into —
# exactly what a query-string value always arrives as, since a URL has no
# native number/boolean/date type at all (only text). A parameter typed as
# one of these, or one of these wrapped in `| None`, gets its incoming
# value coerced before `fn` is ever called (§1 P0 / "typed relay APIs") —
# an untyped parameter, or one typed as something else entirely (dict,
# list, Any, an arc.codec.Struct — see payload handling below), is left
# completely untouched, exactly as before.
_COERCIBLE_SCALAR_TYPES = (int, float, bool, str, UUID, date, datetime, time)


def _coercible_type(annotation: Any) -> Any | None:
    """The type to coerce this parameter's incoming value against, or None
    if `annotation` isn't one this mechanism recognizes at all."""
    if annotation is inspect.Parameter.empty:
        return None
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:  # `X | None` / `Optional[X]`
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1 and args[0] in _COERCIBLE_SCALAR_TYPES:
            return annotation  # keep the full `X | None` shape — msgspec handles the union (incl. None) itself
        return None
    if annotation in _COERCIBLE_SCALAR_TYPES:
        return annotation
    return None


def _is_struct_type(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, arc.codec.Struct)


def _inspect_whitelisted_signature(
    sig: inspect.Signature,
) -> tuple[dict[str, Any], Any, str | None]:
    """Splits a whitelisted function's real (non-injected) parameters into
    either: several coercible scalar params (param_types), OR exactly one
    arc.codec.Struct-typed "payload" param (payload_type/payload_param) —
    never both; see whitelist()'s own docstring for why mixing the two
    styles isn't supported. A parameter that's neither (untyped, or an
    annotation this mechanism doesn't recognize) is simply absent from
    param_types and isn't a payload either — untouched, exactly as before
    this feature existed."""
    param_types: dict[str, Any] = {}
    payload_type: Any = None
    payload_param: str | None = None
    for pname, param in sig.parameters.items():
        if pname in _INJECTED_PARAM_NAMES:
            continue
        if _is_struct_type(param.annotation):
            if payload_type is not None:
                raise RuntimeError(
                    f"whitelisted function has more than one arc.codec.Struct-typed "
                    f"parameter ('{payload_param}' and '{pname}') — only one typed "
                    f"payload parameter is supported per function."
                )
            payload_type = param.annotation
            payload_param = pname
            continue
        coercible = _coercible_type(param.annotation)
        if coercible is not None:
            param_types[pname] = coercible
    return param_types, payload_type, payload_param


def _attach_server_timezone_if_naive(value: Any) -> Any:
    """A `datetime` decoded with no explicit UTC offset in its source text
    (e.g. "2026-10-10T14:30:00", vs. "...+05:30" or "...Z") comes back from
    msgspec as naive — tzinfo=None, not "assume UTC". Left alone, asyncpg's
    own encoder WOULD then assume UTC when writing it to a TIMESTAMPTZ
    column (its default for a naive datetime), which is exactly the silent
    hardcoded-UTC behavior arc.tz's whole point is to avoid. Attaching the
    configured arc_server_timezone here — once, right after coercion — is
    what makes a caller's naive "wall clock" input mean what the deployment
    actually configured it to mean, consistently, everywhere downstream."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=arc.tz.server_timezone())
    return value


def _coerce_kwargs(wf: "WhitelistedFunction", kwargs: dict[str, Any]) -> None:
    """Coerces kwargs in place against wf.param_types — every plain scalar
    parameter whitelist() recognized at decoration time. A key not present
    in kwargs (the caller simply didn't pass it — the function's own
    default applies) is left alone; a key present but not coercible to its
    annotated type raises arc.codec.CodecError naming the parameter, which
    _wire_gateway_route turns into a 400. Fully opt-in and additive: a
    function with no annotated parameters has an empty param_types and
    this is a no-op, byte-for-byte the same request handling as before
    this feature existed."""
    for pname, ptype in wf.param_types.items():
        if pname not in kwargs:
            continue
        try:
            coerced = arc.codec.validate(kwargs[pname], type=ptype, strict=False)
        except arc.codec.CodecError as exc:
            raise arc.codec.CodecError(f"parameter '{pname}': {exc}") from exc
        kwargs[pname] = _attach_server_timezone_if_naive(coerced)


def _build_payload(wf: "WhitelistedFunction", kwargs: dict[str, Any]) -> Any:
    """The opt-in "typed payload" style (§1 P1): a whitelisted function with
    ONE arc.codec.Struct-typed parameter gets the WHOLE request (every
    query/body/path key, minus the injected identity/client_ip/cookies/
    request/dry_run ones — those stay their own separate kwargs, never
    swept into the payload) decoded and validated against that Struct in
    one shot, instead of coercing each field name individually. Raises
    arc.codec.CodecError (caller turns it into a 400) naming exactly which
    field is wrong, the same way msgspec already reports a bad Struct.

    Known, deliberate limitation: only TOP-LEVEL datetime fields on the
    Struct get the same naive-datetime-means-arc_server_timezone treatment
    _coerce_kwargs's scalar path gets (via msgspec.structs.replace below) —
    a naive datetime nested inside a Struct-typed FIELD of this Struct
    would not. No whitelisted function in this codebase nests Structs like
    that yet; revisit if one needs to."""
    payload_source = {k: v for k, v in kwargs.items() if k not in _INJECTED_PARAM_NAMES}
    payload = arc.codec.validate(payload_source, type=wf.payload_type, strict=False)

    import msgspec

    updates = {
        f.name: _attach_server_timezone_if_naive(getattr(payload, f.name))
        for f in msgspec.structs.fields(wf.payload_type)
        if isinstance(getattr(payload, f.name), datetime)
        and getattr(payload, f.name).tzinfo is None
    }
    return msgspec.structs.replace(payload, **updates) if updates else payload


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

    def __init__(
        self, *, base: dict, overlay: dict | None, old: "Doc | None", is_new: bool
    ) -> None:
        object.__setattr__(self, "_base", base)
        object.__setattr__(
            self, "_overlay", overlay
        )  # ctx.payload while writable, else None (read-only)
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
    return Doc(
        base=dict(old) if old is not None else {}, overlay=payload, old=_old_doc(old), is_new=is_new
    )


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
    proxy-aware IP (gateway.middleware.client_ip_middleware) short of this.
    wants_cookies: the same mechanism again, for a function that declares a
    `cookies` parameter — e.g. authn.logout(), which has to read the
    arc_session cookie itself to know which specific session to revoke
    (identity alone only says WHO, not which of that user's sessions this
    particular request's cookie belongs to).
    wants_request: the same mechanism again, for a function that declares a
    `request` parameter — the raw gateway.request.Request, for the rare
    handler that genuinely needs something no JSON-kwarg shape can carry
    (e.g. filer's file_upload(), which calls request.form() for the raw
    multipart body — arc.relay.call()'s **kwargs contract has no way to
    represent an uploaded file's bytes at all). Every other whitelisted
    function keeps working exactly as before without knowing this exists,
    same as every wants_* flag above."""

    name: str  # "<plugin>.<function_name>" — also arc.relay.call()'s key
    plugin: str
    fn: Callable[..., Awaitable[Any]]
    methods: list[str]
    roles: list[str]
    path: str
    wants_identity: bool = False
    wants_client_ip: bool = False
    wants_cookies: bool = False
    wants_request: bool = False
    wants_request_id: bool = False  # same mechanism again, for a function that
    # declares a `request_id` parameter — Gateway's own
    # per-request correlation id (gateway.middleware.
    # request_id_middleware). Always the server-assigned
    # value, never a caller-supplied `?request_id=`;
    # it's in _INJECTED_PARAM_NAMES precisely so a
    # client can't forge one into the logs. Also
    # available without declaring the parameter at all,
    # via arc.relay.context().request_id.
    wants_dry_run: bool = False  # same mechanism, mirrored, for a function that
    # declares a `dry_run` parameter — True whenever
    # the inbound request used a safe/idempotent verb
    # (today: GET) against a write-capable endpoint.
    # Purely informational for the function's OWN
    # non-relay side effects (mail, enqueue, ...) —
    # the REAL enforcement for arc.relay.save/delete
    # is the _dry_run contextvar below, active
    # regardless of whether the function asked for
    # this kwarg at all.
    signature: Any = None  # inspect.Signature, computed once at decoration time —
    # _wire_gateway_route uses signature.bind() to reject a
    # malformed body as a 400 before ever calling fn, rather
    # than recomputing inspect.signature(fn) per request
    max_body_bytes: int | None = None  # None -> gateway's own shared ceiling;
    # passed straight through to gateway.add_route()
    param_types: dict[str, Any] = field(default_factory=dict)
    # name -> annotation, for every plain parameter whitelist() recognized as
    # coercible (int/float/bool/str/UUID/date/datetime/time, or one of those
    # wrapped in `| None`) — computed once here, at decoration time, the same
    # way signature/wants_* already are. A parameter with no annotation, or
    # one this mechanism doesn't recognize (dict/list/Any, or an
    # arc.codec.Struct — see payload_type below), is simply absent from this
    # dict and stays completely untouched at request time, exactly as
    # before: fully opt-in, nothing breaks for an existing function that
    # never asked for this.
    payload_type: Any = None
    # The type of the ONE Struct-typed parameter, if this function chose the
    # "typed payload" style instead of plain kwargs (see whitelist()'s own
    # docstring) — None for every ordinary (plain-kwargs or untyped)
    # function, which is unaffected by any of this.
    payload_param: str | None = None


# The ambient "current write's connection" — set only for the duration of a
# write's precommit-hooks-through-write phase (see RelayProvider._write_transaction),
# read by every read/write method before deciding whether to acquire a fresh
# connection. contextvars are task-scoped and inherited across `await`s within
# the same task, so this correctly threads through arbitrarily deep hook
# nesting (a hook calling arc.relay.create(), whose OWN hooks call something
# else again, ...) with no extra plumbing per level.
_active_conn: ContextVar[Any | None] = ContextVar("arc_relay_active_conn", default=None)

# How many writes deep the current call chain is nested (a hook calling
# arc.relay.save()/delete(), whose own hook calls another, ...) — same
# task-scoped-and-inherited contextvar shape as _active_conn, incremented
# once per _write_transaction() entry regardless of new_transaction (the
# guard this backs is about runaway recursion/stack depth, not about
# transaction independence, so an intentionally-independent nested write
# still counts). See _write_transaction's own use of _MAX_WRITE_DEPTH.
_write_depth: ContextVar[int] = ContextVar("arc_relay_write_depth", default=0)

# Set for the duration of a GET-triggered whitelisted call (_wire_gateway_route,
# below) — read by every write primitive to decide whether its own
# conn.transaction() should actually commit. Same task-scoped-and-inherited
# contextvar property as _active_conn: a hook's own nested arc.relay.save()
# call correctly inherits the outer request's dry-run status with no extra
# plumbing. Scoped to arc.relay.save/save_many/delete/delete_many ONLY — it
# cannot and does not make safe anything else a whitelisted function does
# (arc.mail.send(), arc.relay.enqueue(), a raw arc.relay.sql() write, an
# external HTTP call, admin's own DDL-applying endpoints) — those remain the
# function author's own responsibility to guard, via the `dry_run` kwarg
# injection (WhitelistedFunction.wants_dry_run) if they choose to.
_dry_run: ContextVar[bool] = ContextVar("arc_relay_dry_run", default=False)

# Set only for the duration of arc.relay.stream()'s own internal coroutine-
# driving loop (below) — read by arc.relay.publish() to find "the queue for
# whatever stream is currently running in this task," if any. task-scoped
# and inherited across awaits, same as _active_conn/_dry_run above, which is
# what lets publish() work when called from deep inside the streamed
# coroutine's own call chain (e.g. a hook it triggers) with no extra
# plumbing. None (the default) means "no stream is currently active" —
# publish() treats that as "nobody's listening" and is a silent no-op,
# never an error.
_active_stream: ContextVar["asyncio.Queue | None"] = ContextVar(
    "arc_relay_active_stream", default=None
)

# Post-commit hooks (after_commit/on_rollback) waiting on the OUTERMOST
# write transaction to actually resolve — see _postcommit_scope below for
# the full reasoning. Holds a list of (table, ctx, this_write_succeeded)
# entries; only the outermost write ever creates one, flushes it, or
# resets it. None (the default) means no write is in progress at all.
_postcommit_queue: ContextVar[list | None] = ContextVar(
    "arc_relay_postcommit_queue", default=None
)


@dataclass(frozen=True)
class CallContext:
    """Ambient "who is asking, and as part of what request" — populated at
    the Gateway boundary (_wire_gateway_route) and readable anywhere
    downstream via `arc.relay.context()`.

    Every field is a plain, immutable, JSON-encodable value on purpose.
    The live `identity` object is deliberately NOT carried here: it can
    hold references to a session, a cache entry, or a connection, none of
    which are safe to hand to a background task that outlives the request
    (that is exactly the class of bug enqueue() used to have with the
    ambient DB connection). A whitelisted function that genuinely needs
    the full identity object still declares an `identity` parameter and
    gets it injected directly, unchanged.

    `request_id` is Gateway's own per-request correlation id (see
    gateway.middleware.request_id_middleware) — the same value returned to
    the caller in the `X-Request-ID` response header and written into
    every `_job_log` row a job enqueued by this request produces, which is
    what makes "this slow job came from that request" answerable at all.
    """

    request_id: str | None = None
    user: str | None = None  # the acting user's EMAIL — same value psqldb's
    # created_by/updated_by audit columns store (never a UUID)
    roles: tuple[str, ...] = ()

    def is_empty(self) -> bool:
        return self.request_id is None and self.user is None and not self.roles


_EMPTY_CALL_CONTEXT = CallContext()

# The on-the-wire label names a CallContext travels under when a job has to
# cross a PROCESS boundary (see context_labels()/use_context_labels()). The
# `arc_ctx_` prefix keeps them unambiguously ours: a TaskIQ message also
# carries TaskIQ's own labels (`schedule`, `schedule_id`), and a future
# layer may add more — the decoder reads only these three keys and ignores
# everything else rather than assuming it owns the whole label dict.
_CTX_LABEL_REQUEST_ID = "arc_ctx_request_id"
_CTX_LABEL_USER = "arc_ctx_user"
_CTX_LABEL_ROLES = "arc_ctx_roles"

# WHO and WHICH REQUEST the current call belongs to — deliberately plain,
# immutable, serializable data (never the live authn identity object or
# anything holding a DB connection), because this is the one piece of
# ambient state that SHOULD survive into a background job: see enqueue(),
# which clears every other contextvar it inherits and keeps only this one.
# Empty (the default) whenever nothing set it: a CLI run, a test, a
# scheduled task with no originating request. Never an error to read.
_call_context: ContextVar[CallContext] = ContextVar(
    "arc_relay_call_context", default=_EMPTY_CALL_CONTEXT
)


class RelayStream:
    """Returned by arc.relay.stream() — a thin async-iterator wrapper, not a
    new response-format concept of its own. Two ways this gets consumed:
      * Gateway installed, reached over HTTP (_wire_gateway_route below):
        recognized by this type and turned into a live, chunked HTTP
        response (gateway.request.StreamResponse) — each item this
        iterates is one chunk sent to the client as soon as it's produced,
        instead of buffering the whole thing first.
      * Called any other way (arc.relay.call(), a hook, the CLI, no Gateway
        installed at all): there's no open connection to push chunks down,
        so the caller gets no special treatment automatically — see
        RelayProvider.call()'s own draining fallback, which runs this
        iterator to completion and hands back only the LAST item, exactly
        like calling any other function that just returns its result.
    """

    def __init__(self, source: Any) -> None:
        self._source = source.__aiter__() if hasattr(source, "__aiter__") else source

    def __aiter__(self) -> "RelayStream":
        return self

    async def __anext__(self) -> Any:
        return await self._source.__anext__()


class _DryRunRollback(Exception):
    """Internal sentinel only — never escapes _transaction_or_dry_run, never
    seen by a caller or a hook. Forces asyncpg's conn.transaction() to roll
    back (any exception raised inside its `async with` block does) without
    being treated as a genuine failure: on_rollback must never fire for a
    dry run (nothing failed, it was never meant to persist), and neither
    should after_commit (nothing was actually committed either)."""


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

    # ------------------------------------------------------------------ #
    # Hook registration — plugins/<plugin>/hooks/<Table Name>.py, same
    # filename-to-table convention as schemas/patches (psqldb.model).
    # ------------------------------------------------------------------ #
    def register_hooks(self, hooks_dir: str | Path) -> None:
        from psqldb.model import (
            slugify_table_name,
        )  # only dependency Relay takes on psqldb's internals

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
                spec.loader.exec_module(
                    module
                )  # a hook file's own internals (dataclasses, etc.) resolve correctly
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
        deferred = _postcommit_queue.get()
        if deferred is None:  # defensive — only reachable if a caller nulls it mid-write
            deferred = []
        try:
            yield deferred
        except BaseException as exc:
            if outermost and not dry_run:
                await self._flush_postcommit(deferred, committed=False, error=exc)
            raise
        else:
            if outermost and not dry_run:
                await self._flush_postcommit(deferred, committed=True, error=None)
        finally:
            if token is not None:
                _postcommit_queue.reset(token)

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
                        ctx.new = row  # psqldb.insert() already returns a plain dict
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
                            # psqldb.get() already returns a plain dict (or None)
                            ctx.old = await self._psqldb.get(table, id, conn=conn)
                        ctx.doc = _precommit_doc(ctx.old, ctx.payload, is_new=False)
                        await self._run_hooks(table, "validate", ctx)
                        await self._run_hooks(table, "before_save", ctx)
                        # psqldb.update() already returns a plain dict (or None)
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
        by: str | None,
        new_transaction: bool,
    ) -> dict:
        """The shared lookup-then-branch body save() runs either with or
        without its own lock wrapped around it — see save() for which."""
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
        (psqldb.validation.friendly_unique_error) instead of a lock wait —
        not a single atomic statement (this is still select-then-branch,
        just without the lock), a deliberate, smaller-scope fix: a true
        one-statement `INSERT ... ON CONFLICT` upsert would need to decide
        insert-vs-update before precommit hooks run, which breaks their
        existing `ctx.old`/`doc.is_new` contract — left as a documented,
        separate future increment, not silently attempted here.
        """
        if data.get("id") is not None:
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
                    table, data, match_on, filters, by=by, new_transaction=new_transaction
                )
            # str() every value so the same logical key always locks the same
            # name regardless of how the caller spelled it (UUID vs str, ...).
            lock_key = f"relay:save:{table}:{sorted((k, str(v)) for k, v in filters.items())}"
            async with self.lock(lock_key):
                return await self._resolve_match_on(
                    table, data, match_on, filters, by=by, new_transaction=new_transaction
                )

        payload = {k: v for k, v in data.items() if k != "id"}
        return await self._insert(table, payload, by=by, new_transaction=new_transaction)

    async def delete(
        self, table: str, id: UUID, *, by: str | None = None, new_transaction: bool = False
    ) -> None:
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
                            # psqldb.get() already returns a plain dict (or None)
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
                            ctx.new = row  # psqldb.insert_many() already returns plain dicts
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
                            # psqldb.get_many() already returns plain dicts
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
                        # psqldb.update_many() already returns plain dicts
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
        dry_run = _dry_run.get()
        async with self._postcommit_scope(
            new_transaction=new_transaction, dry_run=dry_run
        ) as deferred:
            async with self._write_transaction(new_transaction=new_transaction) as conn:
                try:
                    async with self._transaction_or_dry_run(conn, dry_run):
                        resolved: list[tuple[str, UUID | None, dict]] = []
                        for row in rows:
                            row_id = row.get("id")
                            if row_id is not None:
                                resolved.append(
                                    ("update", row_id, {k: v for k, v in row.items() if k != "id"})
                                )
                                continue
                            if match_on:
                                filters = {f: row[f] for f in match_on}
                                cap = (limit + 1) if limit is not None else None
                                matches = await self._select(
                                    table,
                                    filters=filters,
                                    fields=["id"],
                                    order_by=None,
                                    limit=cap,
                                    offset=0,
                                    distinct=False,
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
                            resolved.append(
                                ("insert", None, {k: v for k, v in row.items() if k != "id"})
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
                            # psqldb.get_many() already returns plain dicts
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
                                table, [c.payload for c in insert_ctxs], created_by=by, conn=conn
                            )
                            # psqldb.insert_many() already returns plain dicts
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
                            # psqldb.update_many() already returns plain dicts
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

    async def delete_many(
        self, table: str, ids: list[UUID], *, by: str | None = None, new_transaction: bool = False
    ) -> None:
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
                            # psqldb.get_many() already returns plain dicts
                            old_by_id = {r["id"]: r for r in old_rows}
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
                        deferred.append((table, ctx, False))
                    raise
                for ctx in ctxs:
                    ctx.conn = None
            for ctx in ctxs:
                deferred.append((table, ctx, True))

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
        self,
        *,
        methods: list[str] | None = None,
        roles: list[str] | None = None,
        path: str | None = None,
        max_body_bytes: int | None = None,
    ) -> Callable[[Callable], Callable]:
        """`methods`/`roles` are RESTRICTIONS, applied only when given —
        never a required allowlist a caller must fill in just to get a
        sane default:

          * `methods=None` -> every verb in ALL_METHODS is registered; the
            function body (and the injected `dry_run` kwarg, honored
            automatically for arc.relay.save/save_many/delete/delete_many
            via a rollback contextvar — see _transaction_or_dry_run) decides
            what actually happens, RPC-style. Pass an explicit `methods=[...]`
            to restrict — e.g. `methods=["POST"]` for something that should
            never be reachable via a safe/idempotent verb at all.
          * `roles=None` -> the "*" sentinel (any resolved identity,
            role-agnostic) — authenticated-by-default, NOT public. Public/
            Guest access always needs an explicit `roles=["Guest"]` — never
            a fallback, whether from an omission or from authn happening to
            be absent.

        `max_body_bytes=None` (default) -> this route shares the gateway-
        wide `gateway_max_body_bytes` ceiling like every other endpoint.
        Pass a value to give just this one route its own outer ASGI-level
        body limit — e.g. filer's file_upload, which needs to accept much
        larger requests than an ordinary JSON API call without raising the
        shared ceiling everything else is bounded by (gateway/router.py's
        own RouteEntry.max_body_bytes docstring has the full reasoning).
        Fixed at decoration time, same as gateway_max_body_bytes itself is
        fixed at boot — not a live-editable setting.
        """
        roles = roles if roles is not None else ["*"]
        methods = methods if methods is not None else list(ALL_METHODS)

        def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            plugin = self._loading_plugin or self._kernel.current_plugin() or "<direct>"
            if not self._kernel.has("authn") and set(roles) - {"Guest"}:
                # Advisory only, NOT a boot-time error (correction, was a
                # hard RuntimeError): the system boots cleanly regardless of
                # authn's presence. Without authn, identity_middleware
                # always resolves identity=None for every caller, so
                # _wire_gateway_route's own (unchanged) authorization check
                # already correctly 403s everyone for ANY non-Guest
                # requirement — no dummy bypass, no special-casing, the
                # exact same "wrong role" denial path a real caller with
                # insufficient roles would hit. This advisory exists purely
                # so an operator deploying without authn sees, at boot,
                # which endpoints will be permanently unreachable — the
                # same "degrade, don't fail, but say so" posture §3.5/§3.11
                # already use for redix-absent cache/lock.
                self._kernel.advise(
                    f"whitelisted function '{fn.__name__}' (plugin '{plugin}') declares "
                    f"roles={roles}, but no authn plugin is installed — every request to "
                    f"it will be rejected with 403 until authn is installed (or this is "
                    f"changed to roles=['Guest'] for genuinely public access)."
                )
            name = f"{plugin}.{fn.__name__}"
            derived_path = path or f"/api/method/{name}"
            # eval_str=True: many plugins write `from __future__ import
            # annotations`, which makes every annotation a plain STRING
            # (e.g. "int", not the type int) unless something evaluates it
            # — needed here (unlike before this feature existed) because
            # param_types/payload_type below actually inspect what the
            # annotation IS, not just whether one exists. A string
            # annotation is evaluated against fn's own MODULE globals
            # (__globals__), never the local scope of whatever function
            # happened to define it — a real trap for a whitelisted
            # function nested inside another function with its own local
            # `from datetime import datetime`-style import (verified
            # directly: raises NameError at boot). Every real business
            # plugin already avoids this naturally — register_api() loads
            # each api/*.py file as its own module, and a normal top-level
            # `import` there already lands in that module's globals.
            sig = inspect.signature(fn, eval_str=True)
            param_types, payload_type, payload_param = _inspect_whitelisted_signature(sig)
            wf = WhitelistedFunction(
                name=name,
                plugin=plugin,
                fn=fn,
                methods=methods,
                roles=roles,
                path=derived_path,
                wants_identity="identity" in sig.parameters,
                wants_client_ip="client_ip" in sig.parameters,
                wants_cookies="cookies" in sig.parameters,
                wants_request="request" in sig.parameters,
                wants_request_id="request_id" in sig.parameters,
                wants_dry_run="dry_run" in sig.parameters,
                signature=sig,
                max_body_bytes=max_body_bytes,
                param_types=param_types,
                payload_type=payload_type,
                payload_param=payload_param,
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
            raise RelayError(
                f"no whitelisted function named '{name}'", status=404, code="not_found"
            )
        result = await wf.fn(**kwargs)
        if isinstance(result, RelayStream):
            # No open HTTP connection exists here to push chunks down (this
            # is a direct, in-process call) — drain the whole thing and
            # hand back only the last item, so a caller gets the exact
            # same "one normal return value" shape whether or not the
            # function it called happens to use arc.relay.stream()
            # internally. See RelayStream's own docstring for why the last
            # item is the meaningful one (arc.relay.publish()'s progress
            # pings are interim, the final yield/return is the real result).
            last = None
            async for last in result:
                pass
            return last
        return result

    def stream(self, source: Any) -> RelayStream:
        """Wraps `source` so it can be returned from a whitelisted function
        as a live, chunked response instead of one buffered blob — for two
        distinct reasons to reach for this, not one:

          * A large payload (a big export, a big query result, a file) —
            pass an async generator that yields pieces of it. Nothing here
            is redix-backed or cross-process; it's plain chunked transfer
            on the one connection that's already open, purely so the
            server never has to hold the whole thing in memory at once.
          * A long-running action that wants to keep its OWN caller
            informed while it works — pass a plain coroutine (not a
            generator) that calls arc.relay.publish(event, **payload) as it
            goes and returns its real result at the end; stream() drives
            that coroutine itself and turns each publish() call plus the
            final return value into the sequence of chunks sent out.

        Both cases return the identical RelayStream wrapper — Gateway (and
        arc.relay.call()'s own fallback) don't need to know or care which
        kind of source produced it.
        """
        if inspect.iscoroutine(source):
            return RelayStream(self._drive_coroutine(source))
        return RelayStream(source)

    async def _drive_coroutine(self, coro: Any):
        """Runs `coro` as a background task while concurrently yielding
        whatever arc.relay.publish() sends into its queue, in the order
        it's sent, live — then yields the coroutine's own return value
        last, once it finishes. _active_stream is scoped to this task via
        asyncio.create_task's own context-copying (same task-scoped
        propagation _active_conn/_dry_run already rely on elsewhere), so a
        publish() call made from arbitrarily deep inside coro's own call
        chain (a hook it triggers, a function it calls) finds this queue
        with no extra plumbing."""
        queue: asyncio.Queue = asyncio.Queue()
        token = _active_stream.set(queue)
        try:
            task = asyncio.ensure_future(coro)
            get_next = asyncio.ensure_future(queue.get())
            try:
                while True:
                    done, _pending = await asyncio.wait(
                        {get_next, task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if get_next in done:
                        yield get_next.result()
                        get_next = asyncio.ensure_future(queue.get())
                    if task in done:
                        get_next.cancel()
                        while not queue.empty():
                            yield queue.get_nowait()
                        yield task.result()
                        return
            finally:
                get_next.cancel()
        finally:
            _active_stream.reset(token)

    async def publish(self, event: str, **payload: Any) -> None:
        """Pushes one small update into whichever arc.relay.stream()-driven
        coroutine is currently running in this task, if any — read the
        design note on _active_stream above for exactly what "currently
        running in this task" means. No active stream (this wasn't called
        from inside a stream()-driven coroutine at all, or the function
        was invoked directly rather than through Gateway's live path) is
        NOT an error: it just means nobody is in a position to receive
        this update right now, so it's silently dropped — the same
        "degrade, don't fail" posture already used for cache/lock/enqueue
        when redix/lineup are absent (§3.11), except here there's no
        install-a-plugin fix for it; it's simply a property of how the
        enclosing function was called."""
        queue = _active_stream.get()
        if queue is None:
            return
        await queue.put({"event": event, **payload})

    def _wire_gateway_route(self, wf: WhitelistedFunction) -> None:
        gateway = self._kernel.get("gateway")
        from gateway.request import (
            HTTPError,
            Response,
            StreamResponse,
        )  # only imported when gateway is actually present

        async def handler(request: Any) -> Any:
            # request.identity is None whenever authn isn't installed, or a
            # request carries no valid credentials — both collapse to
            # caller_roles = {"Guest"}, identical to this cut's old
            # Guest-only behavior, so roles=["Guest"] endpoints (e.g.
            # example_hr's) are unaffected either way. "*" now means EITHER
            # an explicit roles=["*"] OR the (new) implicit default —
            # authn absent makes identity always None either way, so this
            # branch already resolves to `authorized = False` for both
            # cases with zero special-casing (see whitelist()'s own
            # docstring — this is what makes the boot-time error safe to
            # remove: the request-time 403 below was ALREADY correct).
            identity = getattr(request, "identity", None)
            caller_roles = set(getattr(identity, "roles", None) or [])
            request_id = getattr(request, "request_id", None)
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
                if identity is None and not self._kernel.has("authn") and set(wf.roles) - {"Guest"}:
                    # Distinguishable from an ordinary wrong-role denial —
                    # the caller didn't lack a role, there's structurally no
                    # way for ANY caller to ever satisfy this endpoint right
                    # now, because nothing can authenticate anyone.
                    raise HTTPError(
                        403,
                        {
                            "error": "forbidden",
                            "detail": "authentication is required for this endpoint, but no authn plugin is installed",
                        },
                    )
                raise HTTPError(
                    403, {"error": "forbidden", "detail": f"requires role(s) {wf.roles}"}
                )
            try:
                # wants_request functions get the raw Request instead (see
                # WhitelistedFunction's own docstring) and parse their own
                # body however they need to (e.g. request.form() for
                # multipart) — attempting a JSON decode here first would
                # 422 every such call before it ever reached its handler,
                # since a multipart/form-data body is never valid JSON.
                body = request.json() if request.body and not wf.wants_request else {}
            except Exception as exc:
                raise HTTPError(422, {"error": "invalid JSON body", "detail": str(exc)}) from exc
            # A JSON body that decodes to a list/string/number/etc. isn't
            # spreadable as **kwargs at all — reject it as a client mistake
            # (422) here, rather than letting it become a bare TypeError
            # below that would otherwise escape as an unstructured 500.
            if not isinstance(body, dict):
                raise HTTPError(
                    422, {"error": "JSON body must be an object", "detail": type(body).__name__}
                )
            # Query-string args first (the natural way to call a GET),
            # then the body on top (only ever present for methods that
            # conventionally carry one) — so a GET with no body works from
            # query params alone, and an existing POST-only caller sees no
            # change at all (it never had query params to begin with).
            # First value per key only (request.query() already does the
            # same). Coercion against each param's own annotation happens
            # below, once every source (query/body/path/injected) has been
            # merged — see wf.param_types / _coerce_kwargs.
            kwargs: dict[str, Any] = {k: v[0] for k, v in request.query_params.items()}
            kwargs.update(body)
            # Path params (a `path="/files/{file_id}"`-style route) win
            # over both — they're the most structural, least-spoofable
            # part of the URL a caller controls, so a query string or body
            # key of the same name must never be able to override what the
            # route itself already matched. Every existing whitelisted
            # function predates this (none used a `{param}` path before
            # filer's file_download/serve_file), so this is purely
            # additive — nothing that only ever used query_params/body
            # sees any change here.
            kwargs.update(request.path_params)
            # GET and QUERY are the two verbs this system treats as
            # safe/idempotent by convention — everything else (POST/PUT/
            # PATCH/DELETE) is a real call, full stop. This signal is what
            # makes an unrestricted-by-default write endpoint safe: it
            # drives BOTH the injected `dry_run` kwarg below AND the
            # _dry_run contextvar every arc.relay.save/save_many/delete/
            # delete_many call already honors, regardless of whether wf.fn
            # itself asked for the kwarg. QUERY exists specifically so a
            # safe read can carry a real body (a GET physically cannot —
            # every browser's fetch()/XHR throws synchronously if you try)
            # without losing this same safety guarantee GET already has.
            dry_run_signal = request.method in ("GET", "QUERY")
            if wf.wants_identity:
                # Always the server-resolved identity, never a client-supplied
                # "identity" key in the body — this overwrites it deliberately.
                kwargs["identity"] = identity
            if wf.wants_client_ip:
                kwargs["client_ip"] = getattr(request, "client_ip", None)
            if wf.wants_cookies:
                kwargs["cookies"] = getattr(request, "cookies", {})
            if wf.wants_request:
                kwargs["request"] = request
            if wf.wants_request_id:
                kwargs["request_id"] = request_id
            if wf.wants_dry_run:
                kwargs["dry_run"] = dry_run_signal
            if wf.payload_type is not None:
                # Opt-in "typed payload" style (§1 P1) — the whole request
                # becomes ONE validated Struct instead of several
                # individually-coerced kwargs; reshape kwargs down to just
                # {payload_param: <Struct>, **whatever injected kwargs this
                # function also asked for} so the bind()/call below see
                # exactly wf.fn's real parameter list either way.
                try:
                    payload = _build_payload(wf, kwargs)
                except arc.codec.CodecError as exc:
                    raise HTTPError(
                        400, {"error": "invalid arguments", "detail": str(exc)}
                    ) from exc
                kwargs = {
                    wf.payload_param: payload,
                    **{k: v for k, v in kwargs.items() if k in _INJECTED_PARAM_NAMES},
                }
            else:
                try:
                    # Coerce each param_types entry (§1 P0) BEFORE bind()
                    # below — a bad value is a 400 naming the parameter,
                    # from here, not a TypeError from bind() (wrong
                    # arity/name only, blind to types) or a 500 from deep
                    # inside wf.fn.
                    _coerce_kwargs(wf, kwargs)
                except arc.codec.CodecError as exc:
                    raise HTTPError(
                        400, {"error": "invalid arguments", "detail": str(exc)}
                    ) from exc
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
            # Ambient "who/which request" for everything downstream of this
            # call — hooks, nested writes, and (deliberately, unlike every
            # other contextvar here) any background job enqueued during it.
            # Built from server-resolved values only, never from kwargs.
            call_ctx = CallContext(
                request_id=request_id,
                user=getattr(identity, "email", None),
                roles=tuple(sorted(caller_roles)),
            )
            token = _dry_run.set(dry_run_signal)
            ctx_token = _call_context.set(call_ctx)
            try:
                result = await wf.fn(**kwargs)
            except RelayError as exc:
                raise HTTPError(exc.status, {"error": exc.message, "code": exc.code}) from exc
            finally:
                _dry_run.reset(token)
                _call_context.reset(ctx_token)
            if isinstance(result, RelayStream):
                # A stream is a live read, not a write whose outcome needs
                # hiding behind X-Dry-Run — GET already means "safe" for
                # every OTHER whitelisted function via dry_run, and a
                # stream-returning function is no different; hand it
                # straight to Gateway's chunked-response path instead of
                # going through the dry-run wrapping below, which assumes
                # a single, already-complete value.
                #
                # Real correctness subtlety this handles: wf.fn() above
                # only CONSTRUCTS the RelayStream (an unstarted async
                # generator) — the streamed coroutine, and any
                # arc.relay.save/delete it makes, doesn't actually run
                # until Gateway's send_stream iterates it, which happens
                # AFTER this handler returns, i.e. after the _dry_run.reset()
                # a few lines up already fired. Without re-establishing
                # dry_run around that later iteration, a GET against a
                # stream()-returning write would silently persist for real
                # — re-scoping it here, around the actual iteration, is
                # what keeps the same guarantee dry_run gives every other
                # whitelisted write.
                # The CallContext is re-established here for exactly the
                # same reason dry_run is: the streamed coroutine's real
                # work happens during THIS iteration, long after the
                # handler's own `finally` reset both. Without it, a job
                # enqueued from inside a streamed function would lose the
                # request_id/user that every other path carries.
                async def _dry_run_scoped(stream: RelayStream):
                    inner_token = _dry_run.set(dry_run_signal)
                    inner_ctx_token = _call_context.set(call_ctx)
                    try:
                        async for chunk in stream:
                            yield chunk
                    finally:
                        _dry_run.reset(inner_token)
                        _call_context.reset(inner_ctx_token)

                return StreamResponse(source=_dry_run_scoped(result))
            if isinstance(result, StreamResponse):
                # A whitelisted function that constructs its OWN
                # gateway.request.StreamResponse directly (not via
                # arc.relay.stream()) — e.g. filer's file_download,
                # streaming bytes it already has an async iterator for,
                # with no arc.relay.save/delete anywhere inside it to
                # guard. Same "this is a live read, not a write" reasoning
                # as the RelayStream branch above: pass it straight to
                # Gateway's chunked-response path, never through the
                # dry-run JSON-wrapping below, which assumes a single,
                # already-complete, JSON-encodable value — wrapping a
                # StreamResponse (an async generator) as `content` there
                # would try to JSON-encode it and fail outright.
                return result
            if dry_run_signal:
                # Never let a dry-run response look indistinguishable from
                # a real write's — a caller must be able to tell nothing
                # actually persisted, without having to already know this
                # endpoint's own semantics.
                if isinstance(result, Response):
                    result.headers = {**result.headers, "X-Dry-Run": "true"}
                    return result
                return Response(content=result, headers={"X-Dry-Run": "true"})
            return result

        for method in wf.methods:
            gateway.add_route(
                method,
                wf.path,
                handler,
                summary=f"whitelisted: {wf.name}",
                max_body_bytes=wf.max_body_bytes,
            )

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
        if expires_at is not None and time_module.monotonic() >= expires_at:
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
        self._local_cache[key] = (value, time_module.monotonic() + ex if ex is not None else None)
        if len(self._local_cache) % 256 == 0:
            now = time_module.monotonic()
            for k in [
                k for k, (_v, exp) in self._local_cache.items() if exp is not None and now >= exp
            ]:
                self._local_cache.pop(k, None)
        # The sweep above only ever removes entries that have actually
        # EXPIRED — a set of keys with no `ex` (or one that just hasn't
        # come due yet) sails straight through it and grows forever, since
        # this fallback is a plain process-lifetime dict with nothing else
        # bounding it (unlike redix, which is Redis's own problem to size).
        # Evict oldest-first (plain dict insertion order) once the cap is
        # hit — not a real LRU (a cache_get doesn't move a key to the
        # back), but enough to make "forever" actually mean something
        # finite for the one deployment shape (no redix installed) this
        # fallback exists for at all.
        while len(self._local_cache) > _LOCAL_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(self._local_cache))
            self._local_cache.pop(oldest_key, None)

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

    # ------------------------------------------------------------------ #
    # Error / logging — arc.relay.throw() / .log(). log() used to be
    # console-only, by deliberate choice, pending a later decision on real
    # structured/persisted logging (docs/arc.MD §8) — arc.log (§3.10) is
    # that decision. log() now goes through logging.getLogger("relay"),
    # same as every other plugin's own logger, so it gets arc.log's console
    # + rotating JSON file handlers for free instead of a second, separate
    # rich.Console output nothing else in the system shared.
    # ------------------------------------------------------------------ #
    def throw(self, message: str, *, status: int = 400, code: str | None = None) -> None:
        raise RelayError(message, status=status, code=code)

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
    kernel.get("psqldb").register_model(Path(__file__).parent / "schemas")

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
