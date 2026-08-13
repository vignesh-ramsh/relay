"""Cache / lock — genuinely redix-backed when redix is installed, and
genuinely weaker (not just "the same but local") when it isn't; see each
method for exactly what's lost. Values always round-trip through
arc.codec (not a second, separate serialization scheme) when redix is
backing the cache — the local fallback stores Python objects as-is, same
process, no serialization boundary to cross.

Split out of relay/__init__.py for maintainability — CacheLockMixin is one
of several mixins RelayProvider (relay/__init__.py) inherits from; `self`
is the single shared RelayProvider instance everywhere.
"""

from __future__ import annotations

import asyncio
import contextlib
import time as time_module
from typing import Any

import arc

# Cap on the in-process cache_get/cache_set fallback (no redix installed) —
# see cache_set's own comment for why the existing expiry sweep alone isn't
# enough. A plain constant, not a setting: this is a "does this deployment
# have redix or not" concern, not something a project would ever want to
# tune per environment.
_LOCAL_CACHE_MAX_ENTRIES = 10_000


class CacheLockMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _redix: Any
    _local_cache: dict[str, tuple[Any, float | None]]
    _local_locks: dict[str, tuple[asyncio.Lock, int]]

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
