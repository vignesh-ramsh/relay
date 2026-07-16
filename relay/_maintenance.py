"""relay's own maintenance job — prunes `_job_log` rows older than a
configurable retention window (`arc.settings.get("job_log_retention_days")`,
default 60 days).

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

from datetime import datetime, timedelta, timezone

import arc

DEFAULT_RETENTION_DAYS = 60


async def prune_job_log(older_than_days: int | None = None) -> int:
    if older_than_days is None:
        raw = arc.settings.get("job_log_retention_days")
        older_than_days = int(raw) if raw else DEFAULT_RETENTION_DAYS
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=older_than_days)
    rows = await arc.relay.sql('DELETE FROM "_job_log" WHERE finished_at < $1 RETURNING id', cutoff)
    return len(rows)
