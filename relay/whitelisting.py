"""Whitelisting — plugins/<plugin>/api/*.py, same controlled-loading
pattern as register_hooks() (files aren't table-named here — a
whitelisted function isn't tied to one table). Every whitelisted function
is ALWAYS callable directly via arc.relay.call(name, **kw) regardless of
whether Gateway is installed; it's ADDITIONALLY wired into arc.gateway as
a real route when Gateway is present. No implicit REST-per-table routes
anywhere — this is the only way anything in Relay ever becomes
web-reachable.

Split out of relay/__init__.py for maintainability — WhitelistingMixin is
one of several mixins RelayProvider (relay/__init__.py) inherits from;
`self` is the single shared RelayProvider instance everywhere.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import inspect
import sys
import types
import typing
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import UUID

import arc

from .ambient import _active_stream, _call_context, _dry_run
from .shapes import CallContext, RelayError, RelayStream, WhitelistedFunction

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

#: Which verbs conditional-GET/ETag semantics apply to — same set
#: dry_run_signal (below, computed per-request from request.method) uses
#: for the identical "safe and idempotent" reasoning, kept as its own
#: constant here since it's evaluated once per (method, path)
#: REGISTRATION rather than once per request.
_ETAG_ELIGIBLE_METHODS = frozenset({"GET", "QUERY"})


def _resolve_etag(etag: bool | None, method: str) -> bool:
    """whitelist()'s own `etag` kwarg, resolved for one specific method
    registration — see that kwarg's own docstring. Always False outside
    _ETAG_ELIGIBLE_METHODS, regardless of what was asked for: conditional-
    GET semantics don't apply to a mutating verb, so there is no opt-IN
    for one, only the opt-OUT this function already gives GET/QUERY."""
    if method not in _ETAG_ELIGIBLE_METHODS:
        return False
    return True if etag is None else etag

# Injected by _wire_gateway_route itself (identity/client_ip/cookies/
# headers/request/dry_run/request_id) — never sourced from the caller's
# own query/body/path, so these names are never candidates for the
# coercion/payload inspection below, whatever a function happens to
# annotate them as. request_id in particular MUST be here rather than
# left to ordinary kwarg handling: an inbound `?request_id=...` would
# otherwise let a caller forge its own correlation id straight into the
# logs. `headers` belongs here for the identical reason `cookies`/
# `client_ip` already are — it's server-derived (wf.wants_headers,
# shapes.py's own docstring), not a caller-supplied kwarg — and its
# ABSENCE here was a real bug, not just an inconsistency: a
# Struct-payload function that also declares `headers` (wf.wants_headers
# AND wf.payload_type both set) had its injected headers dict swept into
# _build_payload's own payload_source below (validated against the
# Struct as if the caller had sent it, which it never did) AND dropped
# entirely from the final kwargs `_wire_gateway_route` calls `fn` with
# (its own `{wf.payload_param: payload, **{k: v for k, v in kwargs.items()
# if k in _INJECTED_PARAM_NAMES}}` reconstruction only keeps names in
# THIS set) — such a function received no `headers` argument at all,
# despite having explicitly asked for one.
_INJECTED_PARAM_NAMES = frozenset(
    {"identity", "client_ip", "cookies", "headers", "request", "dry_run", "request_id"}
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


def _type_label(py_type: Any) -> str:
    """A short, human-readable type name for the /openapi UI's request-
    schema listing — "str", "int | None" stays just "str"/"int" (the
    Optional-unwrap whitelist() itself already does for coercion purposes,
    see _coercible_type), never a raw typing internals repr."""
    origin = typing.get_origin(py_type)
    if origin is typing.Union:
        args = [a for a in typing.get_args(py_type) if a is not type(None)]
        if args:
            return _type_label(args[0])
    return getattr(py_type, "__name__", str(py_type))


def _request_schema_for(wf: "WhitelistedFunction") -> Any:
    """A best-effort, JSON-serializable description of this function's
    request payload, for the /openapi UI's per-method "Request" tab —
    NOT used for validation (param_types/_coerce_kwargs already do that,
    completely unaffected by this). A typed-payload function
    (wf.payload_type) hands back the real arc.codec.Struct type itself —
    gateway.openapi._schema_for() derives its actual field schema the
    same way it already does for any other Struct type. A plain-kwargs
    function has no single type to point at, so this hand-builds an
    object schema from param_types (every parameter whitelist() actually
    recognizes and coerces) instead — an untyped/unrecognized parameter
    (dict/list/Any) is invisible to param_types and so absent here too,
    same as it always was for coercion."""
    if wf.payload_type is not None:
        return wf.payload_type
    if not wf.param_types:
        return None
    properties: dict[str, Any] = {}
    required: list[str] = []
    for name, ptype in wf.param_types.items():
        properties[name] = {"type": _type_label(ptype)}
        param = wf.signature.parameters.get(name) if wf.signature is not None else None
        if param is not None and param.default is inspect.Parameter.empty:
            required.append(name)
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _response_schema_for(wf: "WhitelistedFunction") -> Any:
    """The function's own return-type annotation, if it has one — handed
    to gateway.openapi._schema_for() the same way payload_type is above.
    Harmlessly becomes None there for anything that isn't a real
    arc.codec.Struct (a plain str/dict/list/int return type, or no
    annotation at all) — this never claims more than the function's own
    signature actually states."""
    if wf.signature is None:
        return None
    ret = wf.signature.return_annotation
    if ret is inspect.Signature.empty or ret is None:
        return None
    return ret


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


class WhitelistingMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _kernel: Any
    _loading_plugin: str | None
    _whitelisted: dict[str, WhitelistedFunction]

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
        self,
        *,
        methods: list[str] | None = None,
        roles: list[str] | None = None,
        path: str | None = None,
        max_body_bytes: int | None = None,
        etag: bool | None = None,
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

        `etag` (2026-08-27): conditional-GET support (strong ETag, hashed
        via arc.hash() over the exact response body; a matching
        `If-None-Match` gets a bodyless 304 back instead of the full 200)
        for this function's GET/QUERY registration(s) — a pure bandwidth
        optimization, no caching policy of its own (see
        gateway.GatewayProvider.add_route's own docstring for the full
        reasoning). `None` (the default) means True for GET/QUERY, and is
        simply never eligible for any other method — pass `etag=False` to
        opt a specific GET/QUERY-serving function out entirely (e.g. one
        whose response legitimately differs on every call in a way that
        would make every request pay the hashing cost for zero 304s in
        return, however cheap that cost normally is). `etag=True` is a
        no-op for a function with no GET/QUERY in `methods` at all.
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
            derived_path = path or f"/api/v1/{name}"
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
                wants_headers="headers" in sig.parameters,
                wants_request="request" in sig.parameters,
                wants_request_id="request_id" in sig.parameters,
                wants_dry_run="dry_run" in sig.parameters,
                signature=sig,
                max_body_bytes=max_body_bytes,
                etag=etag,
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
            if wf.wants_headers:
                kwargs["headers"] = getattr(request, "headers", {})
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
            # Span covers exactly this HTTP-triggered invocation of wf.fn —
            # the direct/CLI/queued-task path (call(), below) deliberately
            # does NOT get one this pass, per arc.tracing's own "CLI
            # processes never call start_exporter()" scoping; get_tracer()
            # is None there anyway, so this would no-op regardless.
            _tracer = arc.tracing.get_tracer()
            _span_cm = (
                _tracer.start_as_current_span(
                    "arc.relay.call", attributes={"arc.plugin": wf.plugin, "arc.relay.function": wf.name}
                )
                if _tracer is not None
                else contextlib.nullcontext()
            )
            token = _dry_run.set(dry_run_signal)
            ctx_token = _call_context.set(call_ctx)
            try:
                with _span_cm:
                    result = await wf.fn(**kwargs)
            except RelayError as exc:
                body = {**(exc.extra or {}), "error": exc.message, "code": exc.code}
                raise HTTPError(exc.status, body) from exc
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
                etag=_resolve_etag(wf.etag, method),
                plugin=wf.plugin,
                # Not enforcement — an ordinary HTTP route's real auth check
                # still happens inside `handler` above via caller_roles, same
                # as always. This is documentation-only: RouteEntry.roles
                # was previously WS-only (gateway's WS handshake DOES read
                # it for real enforcement there); nothing in the HTTP
                # dispatch path reads it, so populating it here is safe and
                # is what lets the /openapi UI show "allowed roles" per
                # route without relay needing a second, parallel channel.
                roles=frozenset(wf.roles) if wf.roles else None,
                request_schema=_request_schema_for(wf),
                response_schema=_response_schema_for(wf),
            )
