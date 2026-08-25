"""Hook registration — plugins/<plugin>/hooks/<Table Name>.py, same
filename-to-table convention as schemas/patches (psqldb.model).

Split out of relay/__init__.py for maintainability — HooksMixin is one of
several mixins RelayProvider (relay/__init__.py) inherits from; `self` is
the single shared RelayProvider instance everywhere, including `self.log`
(defined directly on RelayProvider, relay/__init__.py).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Callable

from .ambient import _skip_hook_events
from .shapes import HookContext, HookEvent, HookFn


class HooksMixin:
    # Set by RelayProvider.__init__ — see relay/__init__.py for the full
    # reasoning behind each of these.
    _kernel: Any
    _hooks: dict[tuple[str, str], list[HookFn]]
    _loading_table: str | None
    _loading_plugin: str | None

    def register_hooks(self, hooks_dir: str | Path) -> None:
        from pgdb.model import (
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
        active = events - _skip_hook_events.get()
        return any((table, event) in self._hooks for event in active)

    async def _run_hooks(self, table: str, event: HookEvent, ctx: HookContext) -> None:
        if event in _skip_hook_events.get():
            return
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
        if event in _skip_hook_events.get():
            return
        for fn in self._hooks.get((table, event), ()):
            try:
                await fn(ctx)
            except Exception as exc:
                self.log(
                    f"{event} hook '{getattr(fn, '__name__', fn)!s}' on '{table}' raised "
                    f"{type(exc).__name__}: {exc} — ignored (transaction already resolved)",
                    level="error",
                )
