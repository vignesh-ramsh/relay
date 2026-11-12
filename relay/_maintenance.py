"""relay's own maintenance jobs — prune tables that only ever grow:

  * `_job_log` rows older than `job_log_retention_days` (default 60 days);
  * `_trash` rows (snapshots of deleted rows, tables and columns) older than `trash_retention_days` (default 90);
  * `_audit_*` rows (the change history of each plugin's tables) older than `audit_retention_days`. The default
    is 0, which KEEPS THEM ALL: an audit trail is a record, and how long to keep it is a policy decision for
    whoever runs the system, not a default to ship.

For each, a setting of 0 (or less) keeps everything. Deletes run in batches of `BATCH_SIZE`, one short
statement each, so no long lock is held.

Registered as relay's own `@arc.relay.task(...)` (dogfooding the exact
facade every other plugin uses, docs/arc.MD §3.11/§3.15) directly inside
`register(kernel)` — not loaded via the `plugins/<plugin>/tasks/*.py`
directory convention, since that convention exists purely to ATTRIBUTE a
dynamically-loaded file to whichever plugin is currently registering.
relay doesn't need that here; it already knows this is its own code, so
`current_plugin()` correctly resolves to "relay" without it.

When `lineup` is installed this becomes a real, scheduled, durable job
like any other. When it isn't, `@arc.relay.task(cron=...)` degrades to
returning the function unchanged (with a boot advisory that the schedule
won't fire on its own) — `arc relay prune-job-log` (relay/cli.py) exists
for exactly that case, mirroring `authn prune-sessions`'s own precedent.

`_job_log` is a hard DELETE, never a soft one — it's a `"system": true`
table (§3.9: system tables always get a real hard delete; they have no
`_state` column or trash trigger at all), and "recovering" a log entry
from `_trash` wouldn't mean anything useful anyway."""

from __future__ import annotations

import re

import arc

DEFAULT_RETENTION_DAYS = 60
DEFAULT_TRASH_RETENTION_DAYS = 90
DEFAULT_AUDIT_RETENTION_DAYS = 0  # keep: see the module docstring

TRASH_RETENTION_KEY = "trash_retention_days"
AUDIT_RETENTION_KEY = "audit_retention_days"

#: rows deleted per statement
BATCH_SIZE = 5_000

# `_audit_<plugin>` as the catalog lists it; checked before the name is put (quoted) into SQL
_AUDIT_TABLE = re.compile(r"_audit_[A-Za-z0-9_]+")


async def prune_job_log(older_than_days: int | None = None) -> int:
    if older_than_days is None:
        raw = arc.settings.get("job_log_retention_days")
        older_than_days = int(raw) if raw else DEFAULT_RETENTION_DAYS
    cutoff = arc.tz.ago(days=older_than_days)
    rows = await arc.relay.sql('DELETE FROM "_job_log" WHERE finished_at < $1 RETURNING id', cutoff)
    return len(rows)


def _retention_days(key: str) -> int:
    try:
        return int(arc.settings.get(key) or 0)
    except (TypeError, ValueError):
        return 0  # an unusable value must never turn into "delete everything"


async def _delete_in_batches(table: str, finished: str, cutoff) -> int:
    """Deletes the rows of `table` matching `finished` ($1 is the cutoff), `BATCH_SIZE` per statement. `table` is
    never user input: a literal, or a name read from the catalog that matched _AUDIT_TABLE."""
    deleted = 0
    while True:
        rows = await arc.relay.sql(
            f'DELETE FROM "{table}" WHERE id IN (SELECT id FROM "{table}" WHERE {finished} LIMIT $2) RETURNING id',
            cutoff,
            BATCH_SIZE,
        )
        deleted += len(rows)
        if len(rows) < BATCH_SIZE:
            return deleted


async def prune_trash() -> int:
    """Deletes `_trash` rows deleted more than `trash_retention_days` ago (restored or not); 0 keeps them all."""
    days = _retention_days(TRASH_RETENTION_KEY)
    if days <= 0:
        return 0
    return await _delete_in_batches("_trash", "deleted_at < $1", arc.tz.ago(days=days))


async def prune_audit() -> dict[str, int]:
    """Deletes `_audit_*` rows changed more than `audit_retention_days` ago, in every plugin's audit table; the
    default of 0 keeps them all. Returns how many were deleted per table."""
    days = _retention_days(AUDIT_RETENTION_KEY)
    if days <= 0:
        return {}
    cutoff = arc.tz.ago(days=days)
    tables = await arc.relay.sql(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name LIKE '\\_audit\\_%' ORDER BY table_name"
    )
    result: dict[str, int] = {}
    for row in tables:
        name = row["table_name"]
        if _AUDIT_TABLE.fullmatch(name):
            result[name] = await _delete_in_batches(name, "changed_at < $1", cutoff)
    return result
