"""Every piece of ambient, task-scoped state relay threads through a call
chain without explicit plumbing — all plain `contextvars.ContextVar`s,
task-scoped and inherited across `await`s within the same task. Split out
of relay/__init__.py for maintainability; every name here is re-exported
from relay/__init__.py, including for direct `relay_module._active_conn`
-style access some tests and internals rely on.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from .shapes import _EMPTY_CALL_CONTEXT, CallContext

# Cap on how deep a write can nest inside another write's own hooks (a
# hook calling arc.relay.save()/delete(), whose own hook calls another,
# ...) before this is almost certainly a bug (an infinite loop, or a hook
# that recurses without a base case) rather than a legitimate call chain
# — see _write_transaction's own use of this.
_MAX_WRITE_DEPTH = 10

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

# Which hook events (if any) the CURRENTLY-EXECUTING save/save_many/delete/
# delete_many call has asked to skip — set fresh (never unioned with
# whatever was there before) at the top of each of those four methods and
# reset in a finally, so it's scoped to exactly that one call, not "leaked"
# to a nested arc.relay.* call one of its own (non-skipped) hooks happens to
# make. A hook that itself calls save()/delete() again always starts from
# an empty skip-set unless IT was also given its own skip_* flags — skipping
# is an explicit, per-call opt-in, never silently inherited. Empty (the
# default) means "skip nothing," today's original behavior, unchanged.
_skip_hook_events: ContextVar[frozenset[str]] = ContextVar(
    "arc_relay_skip_hook_events", default=frozenset()
)

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

# A durable enqueue() call made from INSIDE an active write (a hook calling
# arc.relay.enqueue()) must not actually record or dispatch the job until
# the OUTERMOST write's real fate — committed or rolled back — is known;
# writing a Queued _job_log row for work whose data never persisted would
# be exactly the "job ran, but the thing it was about doesn't exist" bug
# this exists to prevent (docs/"Missing Failure-Mode Audits", item 19).
#
# Same shape as _postcommit_queue: only the outermost write creates the
# list (in _postcommit_scope), a nested enqueue() appends its own
# asyncio.Future to whatever list it finds (contextvars propagate the
# SAME outermost list down through nested awaits), and _postcommit_scope
# resolves every future with the final committed/not-committed bool the
# instant the outermost write's transaction block exits — before hook
# flushing, so a durable job's dispatch and a table's after_commit hooks
# become eligible to run at essentially the same moment, not one blocking
# the other. None (the default) means "no active write" — enqueue() reads
# that as "dispatch immediately," its original, unchanged behavior.
_postcommit_waiters: ContextVar[list | None] = ContextVar(
    "arc_relay_postcommit_waiters", default=None
)

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
