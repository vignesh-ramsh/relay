"""Pure data shapes and exceptions used across relay's other modules — no
RelayProvider dependency, no I/O, nothing that reads or writes ambient
state (see relay/ambient.py for the ContextVars these shapes travel
through). Split out of relay/__init__.py for maintainability; every name
here is re-exported from relay/__init__.py so `arc.relay.RelayError`,
`arc.relay.HookContext`, etc. and `relay_module.CallContext`-style direct
module access both keep working unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal


class RelayError(Exception):
    """Raised via arc.relay.throw() — a business/user-facing error, not an
    internal failure. Carries enough to become an HTTP response later
    (whitelisting increment) but works identically with no Gateway involved:
    from a hook, the CLI, or a queued task it's just a normal exception to
    whatever called in.

    `extra` — additive, JSON-serializable fields merged into the HTTP error
    body alongside error/code (never overwriting either, even if a caller's
    dict happens to use those keys). For an error that's really a guided
    NEXT STEP rather than a dead end — e.g. login's max_sessions_reached
    handing back the caller's own active session list so a client can offer
    "log out an existing one and retry" — without it every such case would
    need its own bespoke non-2xx response shape, undiscoverable by the
    generic {error, code} handling every other whitelisted-function caller
    already relies on."""

    def __init__(
        self,
        message: str,
        *,
        status: int = 400,
        code: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.status = status
        self.code = code
        self.extra = extra
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
    wants_headers: the same mechanism again, for a function that declares a
    `headers` parameter (lowercase names, gateway.request.Request.headers'
    own shape) — e.g. authn.login() capturing User-Agent for its own
    _sessions row. Unlike wants_request, this does NOT skip normal JSON
    body parsing (see wants_request's own docstring below for why that one
    does) — a function can want BOTH the parsed body kwargs it always had
    AND the raw headers, which is exactly login()'s case.
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
    wants_headers: bool = False
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


class _DryRunRollback(Exception):
    """Internal sentinel only — never escapes _transaction_or_dry_run, never
    seen by a caller or a hook. Forces asyncpg's conn.transaction() to roll
    back (any exception raised inside its `async with` block does) without
    being treated as a genuine failure: on_rollback must never fire for a
    dry run (nothing failed, it was never meant to persist), and neither
    should after_commit (nothing was actually committed either)."""
