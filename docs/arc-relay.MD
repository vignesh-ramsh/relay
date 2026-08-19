# ARC Relay — A Complete, Plain-English Guide

This document explains **everything** about `arc.relay` — what it is, every
method on it, every concept it introduces (hooks, whitelisting, the query
engine, background jobs, caching/locking, streaming), when to use each
piece, and how, with pseudo-code for all of it. Written so someone who has
never touched ARC before can read it top to bottom and understand the whole
system, and also use it as a reference to jump straight to one method later.

If you only read one section, read **"The Big Picture"**.

---

## 1. The Big Picture

`psqldb` (a separate plugin) owns the **shape** of your data — table
schemas, columns, types. `arc.relay` is everything you actually **do** with
that data:

- **Read and write rows** — `save`, `delete`, `get`, `list`, and their bulk
  cousins — all through one consistent API, regardless of which table.
- **React to changes** — "hooks" that run automatically before/after a row
  is saved or deleted (validation, side effects, cache invalidation, emails
  — anything).
- **Expose functions to the outside world** — turn a plain Python function
  into something callable over HTTP, with role-based access control, with
  one decorator.
- **Run background work** — fire off something that shouldn't block the
  request that triggered it, optionally durable (survives a restart) if
  the `lineup` plugin is installed.
- **Cache and lock** — simple helpers that use `redix` (Redis or Valkey)
  when it's installed, and quietly fall back to something weaker but still
  useful when it isn't.

**Mental model:** psqldb is the filing cabinet (labeled drawers, labeled
folders). Relay is the person at the desk who actually opens drawers, files
new paperwork, double-checks it before filing (hooks), answers the phone
when someone calls in asking for something (whitelisted functions), and
keeps a notepad of "call so-and-so back later" (background jobs).

### Where you get `arc.relay` from

Same as every other capability — after `arc.boot()`:

```python
import arc
arc.boot()

employee = await arc.relay.get("employee", employee_id, ["full_name", "email"])
```

You never construct a `RelayProvider` yourself. It's built once, during
boot, and everything after that is just calling methods on `arc.relay`.

### The three files a plugin can add, and what each is for

A plugin that wants to hang behavior off relay drops files into three
specific folders, and calls three matching `register_*` methods once, in
its own `register(kernel)`:

| Folder                              | Registered via              | Contains                                  |
|--------------------------------------|-------------------------------|---------------------------------------------|
| `plugins/<plugin>/hooks/<Table>.py`  | `kernel.get("relay").register_hooks(...)` | Hook functions for ONE table (filename = table name) |
| `plugins/<plugin>/api/*.py`          | `.register_api(...)`         | Whitelisted functions (callable + optionally HTTP-reachable) |
| `plugins/<plugin>/tasks/*.py`        | `.register_tasks(...)`       | Background/scheduled job functions          |

```python
def register(kernel):
    relay = kernel.get("relay")
    relay.register_hooks(Path(__file__).parent / "hooks")
    relay.register_api(Path(__file__).parent / "api")
    relay.register_tasks(Path(__file__).parent / "tasks")
```

Every file in each folder is auto-loaded (in filename order) — you never
manually list or import them.

---

## 2. CRUD — Reading and Writing Rows

Every method below works on ANY table psqldb knows about — you never
declare a table to relay itself, only which one you're operating on, by
name.

### 2.1 `await arc.relay.save(table, data, ...)`

**What it does:** Insert-or-update, one method. This is THE way to write a
row — you'll use this far more than raw insert/update.

**When to use it:** Any single-row create or update.

**How to decide what happens:**

| What `data` contains          | What `save()` does                                              |
|---------------------------------|---------------------------------------------------------------------|
| `data["id"]` is set              | **Update** that exact row. If no row has that id, this is a hard error (a stale id never silently does nothing). |
| No `id`, but `match_on=[...]` given | **Look up** by those field(s) first: 0 matches → insert, 1 match → update, more than 1 → hard error (save() is always exactly one row) |
| Neither                         | Plain **insert**                                                     |

```python
# Insert (no id given)
employee = await arc.relay.save("employee", {"full_name": "Vignesh", "email": "v@x.com"})

# Update (id given)
await arc.relay.save("employee", {"id": employee["id"], "full_name": "V. Ramesh"})

# Upsert by a business key, not id
await arc.relay.save(
    "employee",
    {"employee_code": "E001", "full_name": "Vignesh"},
    match_on=["employee_code"],
)
```

**Concurrency note (worth knowing, rarely worth worrying about):** when
`match_on` doesn't correspond to a real database-level unique constraint,
`save()` wraps the whole "look up, then insert-or-update" sequence in a
lock, so two simultaneous calls with the same `match_on` values can't both
insert a duplicate. When `match_on` DOES match a real unique
column/constraint, the lock is skipped — Postgres's own constraint already
guarantees no duplicate, which is an even stronger guarantee.

**Blocking insert or update on purpose — `allow_insert` / `allow_update`
(both default `True` — nothing changes unless you set one `False`):**

| Flag                  | Set to `False` to…                                                        |
|------------------------|----------------------------------------------------------------------------|
| `allow_insert=False`   | Refuse to create a new row — for an "edit this record" call that must fail loudly if there's nothing to update, instead of silently creating one. |
| `allow_update=False`   | Refuse to touch an existing row — for an import that should only ever add new rows, never overwrite one that's already there. |

```python
# "Edit" endpoint — must never accidentally create a new employee
await arc.relay.save(
    "employee", {"employee_code": "E001", "full_name": "Renamed"},
    match_on=["employee_code"], allow_insert=False,
)

# Import that must never overwrite an existing row
await arc.relay.save(
    "employee", {"employee_code": "E099", "full_name": "New Hire"},
    match_on=["employee_code"], allow_update=False,
)
```

Both raise `QueryError` (§11.2) the moment the BLOCKED branch turns out to
be the one that would have fired — checked after `match_on`'s lookup runs
(so which branch actually applies is known) but before any hook or write
happens, so a blocked attempt never partially happens. Passing both
`False` at once is rejected immediately — no call could ever succeed with
neither allowed. An explicit `id` always still means "update, or a hard
error if it doesn't exist," regardless of `allow_insert` — these flags
never turn a not-found id into an insert; they only gate the two branches
that were already capable of inserting (`match_on` with no match, and
plain no-id-no-`match_on`).

**Extra options:**
- `by="someone@x.com"` — recorded as the row's `created_by`/`updated_by`.
- `new_transaction=True` — run this write independently of whatever
  transaction might currently be open around the calling code (see §4.7).
- `skip_hooks=True` (or the individual `skip_validate` / `skip_before_save`
  / `skip_after_save` / `skip_after_commit` / `skip_on_rollback` flags) —
  see §4.6.

### 2.2 `await arc.relay.delete(table, id, ...)`

**What it does:** Soft-deletes one row by id (moves it to trash — psqldb's
own concern, not something relay decides).

**When to use it:** Deleting a single row.

```python
await arc.relay.delete("employee", employee_id)
await arc.relay.delete("employee", employee_id, by="admin@x.com")
```

Same `skip_hooks`/`skip_before_delete`/`skip_after_delete`/
`skip_after_commit`/`skip_on_rollback`/`new_transaction` options as `save()`
— see §4.6.

### 2.3 `await arc.relay.save_many(table, rows, ...)`

**What it does:** The bulk version of `save()` — one shared transaction for
the whole batch (all rows succeed, or none do), but hooks still fire ONCE
PER ROW — a hook you wrote for `save()` works completely unchanged with
`save_many()`.

**When to use it:** Importing/updating many rows at once, where you want
one atomic all-or-nothing operation instead of looping `save()` calls
(looping would also open one transaction per row, and one hook failure
wouldn't roll back rows already committed).

```python
await arc.relay.save_many(
    "employee",
    [
        {"employee_code": "E001", "full_name": "A"},
        {"employee_code": "E002", "full_name": "B"},
    ],
    match_on=["employee_code"],
)
```

**One real difference from `save()`:** with `match_on`, if MORE than one
existing row matches for a given input row, `save_many()` updates **all**
of them (an intentional fan-out) — `save()` would instead raise an error.
Pass `limit=N` to cap how many rows one input row's `match_on` is allowed
to touch, if you want a safety rail.

**`allow_insert`/`allow_update` here too** (§2.1) — same meaning as
`save()`'s own, but checked ONCE for the whole batch, after every row's
insert-or-update kind is resolved: if even a single row in the batch would
have been an insert (with `allow_insert=False`) or an update (with
`allow_update=False`), the ENTIRE batch is rejected before anything is
written — consistent with `save_many()`'s existing all-or-nothing
contract (one row's problem already aborts everything else here; this is
just one more way a row can have a problem).

```python
# Import that must add ONLY new employees — if even one row's
# employee_code already exists, reject the whole file, nothing partial.
await arc.relay.save_many(
    "employee", rows, match_on=["employee_code"], allow_update=False,
)
```

### 2.4 `await arc.relay.delete_many(table, ids, ...)`

**What it does:** The bulk version of `delete()` — same one-shared-
transaction, hooks-fire-per-row shape as `save_many()`.

```python
await arc.relay.delete_many("employee", [id1, id2, id3])
```

### 2.5 `await arc.relay.get(table, key, fields)`

**What it does:** Fetch exactly one row.

**When to use it:** Whenever you want a single, specific row.

```python
# by id
employee = await arc.relay.get("employee", employee_id, ["full_name", "email"])

# by any other field(s) — first match
employee = await arc.relay.get("employee", {"employee_code": "E001"}, ["full_name"])
```

**Important, easy to trip on:** `fields` is **required** — there's no
"give me everything" wildcard. You always name exactly the columns you
want back. (Reason: handing back a whole row by default is exactly how a
sensitive column like a password hash accidentally leaks to a client that
forgot to think about it.) Need every column? Use
`arc.relay.all_columns(table)`:

```python
all_fields = arc.relay.all_columns("employee")
employee = await arc.relay.get("employee", employee_id, all_fields)
```

### 2.6 `await arc.relay.list(table, ...)`

**What it does:** Fetch multiple rows, with filters/ordering/paging.

**When to use it:** Any "give me several rows matching X" read.

```python
active = await arc.relay.list(
    "employee",
    filters={"status": "Active"},
    fields=["full_name", "email"],
    order_by=["-created_at"],
    limit=50,
    offset=0,
)
```

**The row cap you must know about:** if you don't pass `limit` at all,
you get a configured default (20 out of the box — the
`relay_list_default_limit` setting) — this protects you from "the day this
table quietly grows to 2 million rows and something forgot to page it."
Pass an explicit `limit=N` to change it for one call (rejected if it's over
`relay_list_max_limit`, 1000 by default), or `limit=None` to mean it
literally: fetch everything matching the filter, no cap, ever. See §7 for
the full filter syntax.

### 2.7 `await arc.relay.list_page(table, ...)`

**What it does:** Exactly what `list()` returns, PLUS the total count of
every row matching the filter (ignoring limit/offset) — for building a
"page 3 of 12" UI without a second, separately-written `count()` call that
could drift out of sync with your filters.

```python
rows, total = await arc.relay.list_page(
    "employee", filters={"status": "Active"}, fields=["full_name"], limit=20, offset=40,
)
```

Returns a plain `(rows, total)` tuple — not a fixed-shape dict — because
deciding the wire/response shape is the whitelisted function's job, not
this method's.

### 2.8 `await arc.relay.count(table, filters=...)`

**What it does:** Just the count, no rows.

```python
n = await arc.relay.count("employee", filters={"status": "Active"})
```

### 2.9 `await arc.relay.aggregate(table, ...)`

**What it does:** `GROUP BY` + `count`/`sum`/`avg`/`min`/`max`.

```python
# No group_by -> one dict for the whole table
totals = await arc.relay.aggregate(
    "employee", aggregates={"headcount": ("count", "*"), "total_salary": ("sum", "salary")},
)
# -> {"headcount": 42, "total_salary": 3150000}

# group_by -> a list of dicts, one per group
by_dept = await arc.relay.aggregate(
    "employee",
    group_by=["department_id"],
    aggregates={"headcount": ("count", "*")},
    filters={"status": "Active"},
)
# -> [{"department_id": "...", "headcount": 5}, {"department_id": "...", "headcount": 9}, ...]
```

No `HAVING`, no `resolve()` inside an aggregate — deliberately bounded, see
§7.

### 2.10 `arc.relay.resolve(field, subfields)`

**What it does:** Used INSIDE a `fields=[...]` list to pull extra columns
off a related row through a `REFERENCE` field, in one query (a LEFT JOIN
under the hood) — not a second round trip per row.

```python
rows = await arc.relay.list(
    "employee",
    fields=["full_name", arc.relay.resolve("department", ["dept_name", "code"])],
)
# -> [{"full_name": "...", "department": {"dept_name": "...", "code": "..."}}, ...]
```

One hop only — `field` must be a plain `REFERENCE` column on the table
you're querying, not a dotted path, and not another `resolve(...)`.

### 2.11 `await arc.relay.sql(statement, *params)` / `sql_one` / `sql_val`

**What it does:** The raw-SQL escape hatch, for the minority of queries the
bounded engine above deliberately doesn't try to cover (complex joins,
window functions, whatever). Parameters are always bound (`$1`, `$2`, ...)
— there is no string-formatting path anywhere in this system, so this is
still fully safe against SQL injection as long as you use `$N` placeholders
and pass values as `*params`, never by interpolating them into the string
yourself.

```python
rows = await arc.relay.sql('SELECT * FROM "employee" WHERE salary > $1', 50000)
row = await arc.relay.sql_one('SELECT * FROM "employee" WHERE id = $1', employee_id)
count = await arc.relay.sql_val('SELECT count(*) FROM "employee"')
```

### 2.12 `arc.relay.all_columns(table)`

**What it does:** Returns every real column name on a table, as a plain
list — for the (should-be-rare) case where you genuinely need the whole
row, without hand-maintaining your own column list that goes stale the
next time the schema changes.

```python
fields = arc.relay.all_columns("employee")
```

### 2.13 `await arc.relay.exists(table, key, ...)`

**What it does:** Answers a plain yes/no — does at least one row match?
Same `key` shape as `get()` (§2.5): a UUID/str means "by id," a dict means
the same filter operators `list()`/`count()` take (§7). Built on SQL
`EXISTS(... LIMIT 1)`, not a fetch-and-check-for-`None` or a
`count() > 0` — Postgres can stop at the very first match, so this stays
cheap even when the filter matches a huge number of rows.

**When to use it:** Whenever you only need to know IF something exists,
not what it looks like. `get(...) is not None` technically works too, but
fetches (and discards) a whole row's worth of columns just to answer the
same question.

```python
if await arc.relay.exists("employee", {"employee_code": "E001"}):
    arc.relay.throw("employee code already in use", code="duplicate_code")

if not await arc.relay.exists("employee", employee_id):
    arc.relay.throw("employee not found", status=404)
```

---

## 3. What Every Read/Write Method Shares

A few behaviors apply uniformly across every method in §2 — worth
understanding once, rather than re-explaining per method:

- **`new_transaction=True`** (available on every read/write method): by
  default, a call made from INSIDE a hook (or inside another relay call)
  shares the connection/transaction of whatever's already running — a
  `before_save` hook that reads another table sees that write's own
  not-yet-committed changes, for instance. Pass `new_transaction=True` to
  opt OUT of that sharing — for work that should succeed or fail
  independently of whatever it was called from.
- **Filters always validate against the real schema.** An unknown column
  name, or an unsupported operator, always raises `QueryError` — never
  silently ignored, never a query that quietly returns nothing.
- **Every read/write always exists — there is no separate "ORM model"
  object to instantiate.** You always operate directly on `arc.relay` with
  a table name string; there's no `Employee.objects.filter(...)`-style
  class per table.

---

## 4. Hooks — Reacting to Writes

A **hook** is a plain async function that runs automatically at a specific
point in a row's save/delete lifecycle. This is how you attach validation,
side effects, and business logic to a table — without touching `save()`'s
own code, and without every caller of `save()` having to remember to also
call your validation function.

### 4.1 The full hook taxonomy

| Event           | Fires…                                                       | Can it stop the write? |
|-----------------|-----------------------------------------------------------------|--------------------------|
| `validate`      | Before the write, first                                         | Yes — raise/`arc.relay.throw()` to reject |
| `before_save`   | Before the write, after `validate`                               | Yes |
| `after_save`    | Right after the write (still inside the same transaction)        | Yes (raising here still rolls the whole write back) |
| `before_delete` | Before a soft-delete                                              | Yes |
| `after_delete`  | Right after a soft-delete (still inside the transaction)          | Yes |
| `after_commit`  | AFTER the transaction has genuinely, durably committed             | No — see §4.5 |
| `on_rollback`   | AFTER the transaction has genuinely rolled back (or this row's own nested write failed even though the outer one committed) | No — see §4.5 |

`validate`/`before_save`/`after_save`/`before_delete`/`after_delete` are
called **precommit** hooks — they run inside the write's own transaction,
so raising from any of them aborts the whole write (nothing is saved).
`after_commit`/`on_rollback` are **postcommit** hooks — the transaction's
fate is already sealed by the time they run, so nothing they do can change
the outcome (and an exception inside one of them is logged and swallowed,
never allowed to look like the write itself failed).

### 4.2 Writing a hook

```python
# plugins/hrms/hooks/Employee.py
# Filename matches the TABLE name exactly.

import arc

@arc.relay.validate
async def reject_negative_salary(ctx):
    if ctx.doc.salary is not None and ctx.doc.salary < 0:
        arc.relay.throw("salary cannot be negative", code="invalid_salary")

@arc.relay.after_save
async def notify_on_status_change(ctx):
    if ctx.doc.old and ctx.doc.old.status != ctx.doc.status:
        arc.relay.log(f"{ctx.doc.full_name} status changed to {ctx.doc.status}")

@arc.relay.after_commit
async def send_welcome_email(ctx):
    if ctx.doc.is_new:
        await arc.mail.send(to=ctx.doc.email, subject="Welcome!", body="...")
```

Every hook decorator (`@arc.relay.validate`, `.before_save`, `.after_save`,
`.before_delete`, `.after_delete`, `.after_commit`, `.on_rollback`) only
works inside a file loaded by `register_hooks()` — the decorator has no
effect (raises) used anywhere else, because it needs to know which TABLE
it's attaching to, and that's inferred from the filename at load time.

**Multiple hooks on the same event, same table** are all called, in the
order they were registered (normally, the order they appear in the file).

### 4.3 `ctx` — what a hook actually receives

Every hook receives one argument, `ctx` (a `HookContext`), whose exact
shape depends on which event fired it:

| Field       | Meaning                                                                    |
|-------------|-------------------------------------------------------------------------------|
| `ctx.table` | Table name, always                                                             |
| `ctx.old`   | The row as it was BEFORE this write (plain dict, or `None` on insert)          |
| `ctx.payload` | The exact dict the caller passed for this write — **mutable**; editing it changes what actually gets saved |
| `ctx.new`   | The row as it now is, AFTER the write (only set from `after_save` onward)      |
| `ctx.doc`   | A friendlier view over the same data — see §4.4                               |
| `ctx.error` | Only set for `on_rollback` — the exception that caused the rollback           |

### 4.4 `ctx.doc` — the friendly way to read/write fields

`ctx.doc` is an attribute-AND-dict-style view over the row:

```python
ctx.doc.salary          # attribute access
ctx.doc["salary"]        # same thing, dict-style
ctx.doc.get("salary")    # same, with a default
ctx.doc.old.salary       # the value BEFORE this write (None on insert)
ctx.doc.is_new           # True on insert, False on update — actually a
                          # constructor flag, read it via ctx.doc._is_new
                          # is discouraged; check `ctx.old is None` instead
```

**During `validate`/`before_save`** (the write hasn't happened yet), `doc`
is a LIVE view: whatever's in the pending payload, overlaid on the old row
— so `doc.salary` already reflects "what this row will look like after this
write." Setting `doc.salary = 50000` is a shortcut for editing
`ctx.payload["salary"]` directly — same effect, friendlier syntax.

**From `after_save` onward**, `doc` is replaced with the REAL, persisted
row and becomes **read-only** — attempting to write to it raises, same as
`doc.old` always being read-only.

A field this write doesn't touch reads the same value from `doc.X` and
`doc.old.X` — they're not two separately-tracked copies, they fall through
to the same underlying old-row value.

### 4.5 Why `after_commit`/`on_rollback` are special (and why it matters)

This is the one subtlety worth genuinely understanding, not just knowing
the name of: **a nested write's "commit" isn't necessarily real yet.**

If a `before_save` hook on table A calls `arc.relay.save()` on table B, B's
write shares A's own connection/transaction (see §3) — Postgres turns B's
"transaction" into a mere SAVEPOINT. If A's write later fails for any
reason, B's SAVEPOINT rolls back too, even though B's own write, by itself,
looked completely successful.

Because of that, relay doesn't fire `after_commit`/`on_rollback` the
instant each individual write's own code block finishes — it **defers**
them until the OUTERMOST write in the whole chain has genuinely resolved,
then decides, per nested write, which event it actually gets:

- Outer write committed, and this row's own write succeeded → `after_commit`
- Outer write committed, but THIS row's own nested write failed on its own → `on_rollback` (its own SAVEPOINT rolled back, even though the outer one carried on)
- Outer write rolled back entirely → `on_rollback` for every row involved, regardless of each one's own individual outcome

**What this means practically:** never do anything in `after_save` that
must only happen if the data is truly, durably saved (sending a
confirmation email, calling an external API) — use `after_commit` for
that. Use `after_save` for things that should be undone automatically if
the write rolls back (because it's still inside the same transaction).

### 4.6 Skipping hooks on purpose

Every write method accepts `skip_hooks=True` (skip everything) or
individual `skip_validate`/`skip_before_save`/`skip_after_save`/
`skip_before_delete`/`skip_after_delete`/`skip_after_commit`/
`skip_on_rollback` flags:

```python
# A bulk import that wants raw speed and doesn't want a hundred emails sent
await arc.relay.save_many("employee", rows, skip_hooks=True)

# Still validate, but skip the side-effecting after_save (e.g. "don't
# re-notify anyone" during a data migration)
await arc.relay.save("employee", data, skip_after_save=True)
```

These are **scoped to exactly that one call** — never silently inherited.
If a hook itself makes its OWN nested `save()`/`delete()` call, that nested
call starts from "skip nothing" unless you explicitly pass skip flags to
IT too.

### 4.7 Nested writes and independent writes

By default, a write made from inside a hook shares the enclosing write's
connection and transaction — so if the OUTER write later fails, the
nested one rolls back with it (real nested-transaction correctness, for
free, via Postgres SAVEPOINTs). Pass `new_transaction=True` on the nested
call if you specifically want it to succeed or fail on its own, completely
independent of whatever triggered it.

There's also a hard safety limit: writes can only nest 10 levels deep
(`a hook's save() triggers another hook's save()...`) before relay raises
a clear error — almost always a sign of an accidental infinite loop, not a
legitimate call chain.

---

## 5. Whitelisting — Making a Function Callable (and Reachable Over HTTP)

**A "whitelisted function" is the ONLY way anything becomes reachable from
outside the process in ARC.** There is no automatic REST-per-table
routing anywhere — you always explicitly write and decorate the function
that should be callable.

### 5.1 `@arc.relay.whitelist(...)`

**What it does:** Registers a plain async function two ways at once:
1. Always callable directly, in-process, via `arc.relay.call(name, **kwargs)`.
2. ADDITIONALLY wired up as a real HTTP route, if the `gateway` plugin is
   installed.

**When to use it:** Any function you want to expose — to the CLI, to a
scheduled job, to a frontend, to another plugin calling in.

```python
# plugins/hrms/api/employee_api.py

import arc

@arc.relay.whitelist(methods=["GET"], roles=["HR", "Admin"])
async def get_employee(employee_id: str) -> dict:
    row = await arc.relay.get("employee", employee_id, ["full_name", "email"])
    if row is None:
        arc.relay.throw("employee not found", status=404)
    return row

@arc.relay.whitelist(methods=["POST"], roles=["Guest"], path="/signup")
async def public_signup(email: str, full_name: str) -> dict:
    return await arc.relay.save("employee", {"email": email, "full_name": full_name})
```

**Options:**

| Option           | Meaning                                                                                          |
|-------------------|-------------------------------------------------------------------------------------------------------|
| `methods`         | Which HTTP verbs reach this function. Omit it entirely and EVERY verb works — the function body (and `dry_run`, see below) decides what actually happens, RPC-style. Pass an explicit list (e.g. `["POST"]`) to restrict. |
| `roles`           | Which caller roles may call this over HTTP. Omit it and the default is "any authenticated caller" (**not public** — that's a deliberate, safe default). Public access always needs an explicit `roles=["Guest"]`, never a fallback. |
| `path`            | The URL. Defaults to `/api/v1/<plugin>.<function_name>` if you don't set one.                     |
| `max_body_bytes`  | A per-route body-size ceiling, if this one route needs a bigger (or smaller) limit than the project-wide default. |

The function's own name becomes part of its identity:
`arc.relay.call("hrms.get_employee", employee_id=...)` — `<plugin>.<function name>`.

### 5.2 Parameters: typed and automatically coerced

A URL query string only ever carries text — there's no such thing as a
"real" integer or boolean in a URL. If you annotate a parameter with a
plain type relay recognizes (`int`, `float`, `bool`, `str`, `UUID`, `date`,
`datetime`, `time` — or one of those with `| None`), relay automatically
converts the incoming text into that real type before your function ever
runs, and rejects a bad value with a clear 400 error naming the parameter
— you never hand-parse `request.query_params` yourself.

```python
@arc.relay.whitelist(methods=["GET"])
async def list_active(limit: int = 20, since: datetime | None = None) -> list[dict]:
    filters = {"status": "Active"}
    if since:
        filters["created_at"] = {"gte": since}
    return await arc.relay.list("employee", filters=filters, fields=["full_name"], limit=limit)
```

**Alternative style — one typed "payload" parameter:** instead of several
individually-coerced scalar parameters, a function may declare ONE
parameter typed as an `arc.codec.Struct` — the WHOLE request body/query
gets decoded and validated against that shape in one shot:

```python
class CreateEmployeePayload(arc.codec.Struct):
    full_name: str
    email: str
    salary: float | None = None

@arc.relay.whitelist(methods=["POST"])
async def create_employee(payload: CreateEmployeePayload) -> dict:
    return await arc.relay.save("employee", {"full_name": payload.full_name, "email": payload.email})
```

You can't mix the two styles — a function is either "several typed scalar
params" or "one Struct payload param," never both.

### 5.3 Parameters that get injected automatically

Declare a parameter with one of these EXACT names, and relay fills it in
for you — the caller can never forge these via the query string or body:

| Parameter name | What you get |
|------------------|----------------|
| `identity`       | The resolved caller identity (or `None`) — who's actually calling |
| `client_ip`      | The caller's real IP (proxy-aware)                                  |
| `cookies`        | The request's cookies, as a dict                                    |
| `request`        | The raw request object, for the rare case a JSON-kwargs shape can't represent what you need (e.g. reading an uploaded file's raw bytes) |
| `request_id`     | This request's own correlation id (also visible in logs)            |
| `dry_run`        | `True` when this call came in over a "safe" verb (GET/QUERY) against a write-capable endpoint — see §5.4 |

```python
@arc.relay.whitelist(methods=["POST"])
async def change_my_password(new_password: str, identity=None) -> dict:
    await arc.relay.save("_users", {"id": identity.user_id, "password_hash": hash_it(new_password)})
    return {"ok": True}
```

### 5.4 `dry_run` — a safety net for accidental writes over GET

If someone calls a write-capable, unrestricted (`methods=None`) endpoint
with GET, relay automatically makes every `arc.relay.save`/`save_many`/
`delete`/`delete_many` call made during that request roll back at the very
end — **whether or not the function itself asked for the `dry_run`
kwarg.** This is why leaving `methods` unrestricted is safe by default: a
curious `curl` GET against a write endpoint can never actually persist
anything. The response also gets an `X-Dry-Run: true` header so nothing
pretends a real write happened when it didn't.

### 5.5 Role checks — how "roles" is actually evaluated

- `roles=["*"]` (or the implicit default when you don't pass `roles` at
  all) — any resolved, authenticated identity may call it (not
  role-specific, but NOT public either).
- `roles=["Guest"]` — the one explicit way to make an endpoint truly
  public.
- `roles=["HR", "Admin"]` — the caller's own roles must intersect this
  set (or the caller must have relay's own `"*"` superuser sentinel,
  which is `authn`'s convention for "this identity satisfies any role
  check").
- No `authn` plugin installed at all → every caller resolves to role
  `"Guest"` — so any endpoint requiring a real role is unreachable by
  anyone until `authn` is installed (relay warns you about this with
  `kernel.advise()` at boot, it doesn't fail boot over it).

### 5.6 `await arc.relay.call(name, **kwargs)`

**What it does:** Invokes a whitelisted function directly, in-process — no
HTTP, no RBAC check (this is a trusted, server-side call, e.g. from a CLI
command, another hook, a background job).

```python
result = await arc.relay.call("hrms.get_employee", employee_id=some_id)
```

### 5.7 `arc.relay.whitelisted()`

**What it does:** Returns the list of every registered whitelisted
function — for introspection/tooling (e.g. an admin page listing every API
endpoint).

### 5.8 `register_api(api_dir)`

**What it does:** Loads every `.py` file in a folder, so their
`@arc.relay.whitelist(...)`-decorated functions register themselves purely
by being imported.

```python
def register(kernel):
    relay = kernel.get("relay")
    relay.register_api(Path(__file__).parent / "api")
```

---

## 6. Streaming — Chunked Responses and Live Progress

### 6.1 `arc.relay.stream(source)`

**What it does:** Lets a whitelisted function return something a client
receives as a live, chunked response instead of one buffered blob at the
end. Two different reasons to use this:

**A. Streaming a large payload** — pass an async generator:

```python
@arc.relay.whitelist(methods=["GET"])
async def export_employees():
    async def rows():
        for chunk in big_query_in_batches():
            yield chunk
    return arc.relay.stream(rows())
```

**B. Reporting progress on a long-running action** — pass a plain
coroutine that calls `arc.relay.publish(...)` as it works, and returns its
real result at the end:

```python
@arc.relay.whitelist(methods=["POST"])
async def long_import():
    async def do_it():
        for i, row in enumerate(rows_to_import):
            await arc.relay.save("employee", row)
            await arc.relay.publish("progress", done=i, total=len(rows_to_import))
        return {"ok": True, "imported": len(rows_to_import)}
    return arc.relay.stream(do_it())
```

Called any OTHER way (`arc.relay.call()`, no Gateway installed) — there's
no live connection to push chunks down, so the caller just gets the final
result, as if the function returned normally.

### 6.2 `await arc.relay.publish(event, **payload)`

**What it does:** Sends one progress update to whichever `stream()`-driven
coroutine is currently running in this same call. If nothing is listening
(this wasn't called from inside a streaming function, or the function was
invoked directly rather than through Gateway) it's silently a no-op — never
an error.

---

## 7. The Query Engine — Filters, in Full

Every `filters={...}` dict you pass to `get`/`list`/`list_page`/`count`/
`aggregate`/`save`/`save_many` follows the same rules.

### 7.1 The basics

A plain value means equality:

```python
filters = {"status": "Active"}
```

Is shorthand for:

```python
filters = {"status": {"eq": "Active"}}
```

Multiple keys are always AND'd together:

```python
filters = {"status": "Active", "department_id": some_id}   # status = X AND department_id = Y
```

### 7.2 Every operator

| Operator                       | Meaning                                      | Value shape           |
|-----------------------------------|--------------------------------------------------|--------------------------|
| `eq`                              | equals (the default when you pass a bare value)   | any                       |
| `ne`                              | not equals                                        | any                       |
| `gt` / `gte` / `lt` / `lte`       | greater/less than (or equal)                      | any comparable            |
| `like` / `ilike`                   | raw SQL `LIKE`/`ILIKE` (case-insensitive)          | a pattern string with your own `%`/`_` |
| `contains` / `startswith` / `endswith` | wraps your value in `%...%` for you, and escapes any literal `%`/`_` in it | plain string |
| `in` / `not_in`                    | value is/isn't one of a list                       | a list                   |
| `range`                            | between two values, inclusive                      | `[min, max]`             |
| `is_null`                          | `IS NULL` / `IS NOT NULL`                          | `True` / `False`         |

```python
filters = {
    "salary": {"gte": 50000},
    "full_name": {"contains": "vign"},
    "status": {"in": ["Active", "OnLeave"]},
    "created_at": {"range": [start, end]},
    "terminated_at": {"is_null": True},
}
```

### 7.3 `any_of` — the one escape hatch for OR

Filters are implicit-AND by default, with exactly one bounded way to
express OR: the reserved `any_of` key, a list of filter dicts, OR'd
together (each branch is itself a normal AND-dict; branches can't be
nested):

```python
filters = {
    "department_id": some_dept,
    "any_of": [
        {"status": "Active"},
        {"status": "OnLeave", "return_date": {"lte": today}},
    ],
}
# department_id = X AND (status = 'Active' OR (status = 'OnLeave' AND return_date <= today))
```

### 7.4 What's deliberately NOT supported (a boundary, not an oversight)

- No arbitrary nested boolean trees (AND-of-ORs-of-ANDs...) — `any_of` is
  the one bounded exception.
- `order_by`/`fields`/`group_by` only ever name plain columns on the table
  itself — no dotted paths, no implicit joins. The only way to pull data
  from a related row is `arc.relay.resolve(...)` (§2.10), explicitly, one
  hop.
- `aggregate()` has no `HAVING` and no `resolve()`.

If you need something outside these bounds, that's what `arc.relay.sql(...)`
(§2.11) is for.

### 7.5 Custom computed fields — `FieldResolver`

Beyond `arc.relay.resolve()` (a SQL join), a capability can contribute its
own **computed** field value that isn't a plain column and isn't a join —
`arc.filer.url(field_name)` (turning a stored file id into a real signed
download URL) is the built-in example. You'd only build one of these if
you're writing a capability that wants to offer this same "computed value
inside a normal `fields=[...]` list" trick to its own callers:

```python
from relay.resolvers import FieldResolver

class MyResolver(FieldResolver):
    field = "avatar_id"

    async def prepare(self, raw_values: list) -> dict:
        # called ONCE per query, with every row's raw value already collected
        # — do your one batched lookup here, never per-row
        return {v: f"https://cdn.example.com/{v}" for v in raw_values if v}

    def resolve(self, raw_value, context) -> str | None:
        # pure, no I/O — runs once per row against the shared context above
        return context.get(raw_value)
```

Used exactly like any other `fields=[...]` entry:
`fields=["full_name", MyResolver()]`.

---

## 8. Background Jobs

### 8.1 `await arc.relay.enqueue(fn, *args, queue="default", **kwargs)`

**What it does:** Fire off `fn` to run separately from the current
request/call — the ONE entry point for background work (never call
`arc.lineup.*` directly).

**When to use it:** Anything that shouldn't block the caller — sending an
email, generating a report, calling a slow external API.

```python
async def send_report(employee_id):
    ...

@arc.relay.whitelist(methods=["POST"])
async def request_report(employee_id: str) -> dict:
    arc.relay.enqueue(send_report, employee_id)
    return {"ok": True, "queued": True}
```

**Two very different behaviors depending on what's installed, same call:**

- **`lineup` installed:** durable — handed off to Redis (or Valkey,
  whichever `redix_url` points at); a separate `arc lineup worker` process
  actually runs it later, so it survives this process crashing/restarting.
- **No `lineup`:** an in-process `asyncio.create_task()` fallback — runs
  in this same process, right now, but is lost if the process dies before
  it finishes. No retry, no persistence.

Either way, it's still fire-and-forget from the caller's point of view —
you get a `Task` back you can generally ignore.

**What does NOT carry over into the background job:** the enclosing
request's database connection, its dry-run flag, and its deferred
postcommit-hook queue — carrying any of those into code that outlives the
request would be actively dangerous (using a connection another request
now owns, or silently no-oping every write forever because it inherited
`dry_run=True`). **What DOES carry over:** the request's `request_id`/
`user`/`roles` (§9) — so a slow job is still traceable back to who/what
triggered it, in `_job_log`.

### 8.2 `@arc.relay.task(queue="default", cron=None)`

**What it does:** Declares a function as a proper, named background task
ahead of time — optionally on a recurring schedule.

**When to use it:** A job you want to be able to schedule (`cron=...`), or
one you want to be resolvable by name across processes (required if
`lineup` workers run in a different process than the one enqueuing).

```python
# plugins/hrms/tasks/reports.py

@arc.relay.task(queue="reports", cron="0 2 * * *")   # every day at 2am
async def nightly_headcount_report():
    ...
```

With `lineup` installed, `cron` is real: `arc lineup scheduler` fires it at
its actual next occurrence. Without `lineup`, the decorator just returns
the function unchanged, and relay warns you at boot that the schedule
won't fire on its own — you'd call it manually via `arc.relay.enqueue(fn)`
or an external cron instead.

### 8.3 `register_tasks(tasks_dir)`

Same auto-loading pattern as hooks/api — loads every file in
`plugins/<plugin>/tasks/`, so their `@arc.relay.task(...)`-decorated
functions register by being imported.

### 8.4 `_job_log` — where job history goes

Every job run through `enqueue()`'s in-process fallback (and everything
`lineup` itself runs) is recorded in the `_job_log` table: task name,
queue, status (success/failed), timings, and — thanks to the call-context
carrying over — which request and which user triggered it. This is a
system table (hard-deleted, never soft-deleted) with its own retention
job; see §8.5.

### 8.5 Job log retention

Relay registers its own maintenance job that prunes old `_job_log` rows
(default: 60 days, the `job_log_retention_days` setting). With `lineup`
installed this runs on its own schedule; without it, run
`arc relay prune-job-log` by hand (or from an external cron).

---

## 9. Call Context — "Who Is Asking, and As Part of What"

### 9.1 `arc.relay.context()`

**What it does:** Returns a `CallContext` (`request_id`, `user` — the
caller's email — `roles`) describing the CURRENT request, readable from
anywhere downstream: a hook, a nested save, a background job the request
enqueued.

**When to use it:** Whenever you need to know who's actually behind the
current call, without threading an explicit parameter through everything.

```python
async def some_hook(ctx):
    who = arc.relay.context().user   # the email of whoever triggered this write
```

Outside of any request (a CLI run, a test, a scheduled task with no
originating request) every field is simply empty — never `None`-checking
required, always a real object.

### 9.2 `context_labels()` / `use_context_labels()`

**What they're for:** Carrying the call context across a PROCESS boundary
— when a job is hand-off to a separate `lineup` worker process, a plain
Python contextvar can't reach across that boundary, so it travels as
plain string data instead. You will rarely call these yourself — `relay`
and `lineup` use them internally so a job enqueued via `arc.relay.enqueue`
is still traceable back to its originating request even once it's running
in a totally different process.

---

## 10. Cache and Locking

### 10.1 `cache_get` / `cache_set` / `cache_delete`

```python
await arc.relay.cache_set("dashboard:totals", data, ex=300)   # expires in 300s
cached = await arc.relay.cache_get("dashboard:totals")
await arc.relay.cache_delete("dashboard:totals")
```

**With `redix` installed:** a real, shared cache (Redis or Valkey) —
visible across every worker process, survives a single process restarting.

**Without `redix`:** a weaker in-process fallback — lost on restart, never
shared between worker processes, capped at 10,000 entries. Still useful
for a single-process dev setup, just don't rely on it for correctness
across multiple workers.

### 10.2 `arc.relay.lock(name, timeout=10.0)`

**What it does:** A named mutual-exclusion lock.

```python
async with arc.relay.lock(f"import:{file_id}"):
    ... # only one caller at a time can be inside here, for this file_id
```

**With `redix`:** a real distributed lock — `timeout` is its auto-expiry
lease, so a crashed holder eventually releases it, across every process.

**Without `redix`:** a plain `asyncio.Lock` — only protects concurrent
tasks in the SAME process (two separate worker processes can both "hold" a
same-named lock at once), and never auto-expires — a crash while holding
it deadlocks that name for the rest of the process's life.

If your deployment runs more than one worker process (it almost certainly
does), install `redix` for this to mean what it sounds like it means.

---

## 11. Errors

### 11.1 `arc.relay.throw(message, status=400, code=None)`

**What it does:** Raises a `RelayError` — the standard way to reject
something for a business reason (not a bug) from a hook, a whitelisted
function, anywhere.

```python
if ctx.doc.salary < 0:
    arc.relay.throw("salary cannot be negative", code="invalid_salary")
```

Works identically whether or not Gateway is involved:
- Reached via HTTP → automatically becomes a proper HTTP error response
  with that `status` code and a JSON body naming `message`/`code`.
- Called directly (CLI, hook, background job) → it's just a normal Python
  exception the caller can catch: `except arc.relay.RelayError:`.

### 11.2 `QueryError`

Raised whenever a `filters`/`fields`/`order_by`/`aggregates` argument
doesn't make sense against the real schema — an unknown column, a bad
operator, too many `order_by` fields, a `match_on` that matched more than
one row for `save()`, `save()`/`save_many()`'s `allow_insert=False` or
`allow_update=False` blocking a branch that would otherwise have fired,
and so on. Always the CALLER's mistake to fix — a
plain `ValueError` subclass, not a `RelayError` (so it's never
auto-converted into an HTTP response the way `RelayError` is — an
unhandled `QueryError` reaching Gateway becomes a generic 500, on purpose,
because "your filter dict is malformed" is a bug in the calling code, not
a business rule to report nicely to an end user).

### 11.3 `arc.relay.log(message, level="info", **context)`

Writes a line through relay's own logger (`logging.getLogger("relay")`) —
same console + `logs/relay.jsonl` routing every other plugin's logging
gets automatically (see `docs/arc-kernel.MD` §4.2). `level` can be
`"info"`, `"success"`, `"warning"`, or `"error"` — `"success"` is relay's
own vocabulary, preserved as-is in the JSON output even though it maps to
plain `INFO` for dispatch.

---

## 12. Settings Relay Declares

| Key                          | Default | What it controls |
|-------------------------------|---------|---------------------|
| `relay_list_default_limit`    | 20      | Row cap for `list()`/`list_page()` when you don't pass `limit` |
| `relay_list_max_limit`        | 1000    | Hard ceiling on any explicit `limit` you DO pass |
| `job_log_retention_days`      | 60      | How long `_job_log` rows are kept before pruning |

Change any of these with `arc settings set <key> <value>`.

---

## 13. A Complete Worked Example

A small "leave request" feature, end to end — hooks, an API, a background
task, all in one place.

```python
# plugins/hrms/hooks/LeaveRequest.py

import arc

@arc.relay.validate
async def check_dates(ctx):
    if ctx.doc.end_date < ctx.doc.start_date:
        arc.relay.throw("end_date cannot be before start_date", code="invalid_dates")

@arc.relay.after_commit
async def notify_manager(ctx):
    if ctx.doc.is_new:
        arc.relay.enqueue(send_leave_notification, ctx.doc.id)
```

```python
# plugins/hrms/tasks/notifications.py

import arc

@arc.relay.task(queue="notifications")
async def send_leave_notification(leave_request_id: str):
    row = await arc.relay.get("leave_request", leave_request_id, ["employee_id", "start_date"])
    manager_email = await _lookup_manager_email(row["employee_id"])
    await arc.mail.send(to=manager_email, subject="New leave request", body="...")
```

```python
# plugins/hrms/api/leave_api.py

import arc

@arc.relay.whitelist(methods=["POST"], roles=["Employee"])
async def request_leave(start_date, end_date, identity=None) -> dict:
    return await arc.relay.save(
        "leave_request",
        {"employee_id": identity.user_id, "start_date": start_date, "end_date": end_date},
    )

@arc.relay.whitelist(methods=["GET"], roles=["Manager", "HR"])
async def list_pending_leave(department_id: str | None = None) -> list[dict]:
    filters = {"status": "Pending"}
    if department_id:
        filters["department_id"] = department_id
    return await arc.relay.list(
        "leave_request",
        filters=filters,
        fields=["start_date", "end_date", arc.relay.resolve("employee", ["full_name"])],
        order_by=["start_date"],
    )
```

```python
def register(kernel):
    relay = kernel.get("relay")
    relay.register_hooks(Path(__file__).parent / "hooks")
    relay.register_tasks(Path(__file__).parent / "tasks")
    relay.register_api(Path(__file__).parent / "api")
```

Calling it, from anywhere:

```python
result = await arc.relay.call("hrms.request_leave", start_date=d1, end_date=d2, identity=me)
# ...or, if gateway is installed, a real POST to /api/v1/hrms.request_leave
```

---

## 14. Common Patterns (Recipes)

**"I need to validate data before it's saved."** → a `@arc.relay.validate`
hook, `arc.relay.throw(...)` to reject.

**"I need to react after something is saved, but only if it REALLY,
durably happened."** → `@arc.relay.after_commit`, never `after_save`, for
anything externally visible (email, webhook, another system).

**"I need one field computed from a related table."** →
`arc.relay.resolve(field, subfields)` in your `fields=[...]` list.

**"I need to expose something to the frontend/CLI."** →
`@arc.relay.whitelist(...)`.

**"I need something to run in the background."** →
`arc.relay.enqueue(fn, ...)` for ad hoc, `@arc.relay.task(...)` if you also
want it schedulable or resolvable by name across processes.

**"I need to make sure two requests don't race each other."** →
`async with arc.relay.lock(name):`.

**"I need a bulk import that skips side effects."** →
`arc.relay.save_many(table, rows, skip_hooks=True)`.

**"I want to know who's calling, deep inside a hook."** →
`arc.relay.context().user`.

**"I only need to know if something exists, not fetch it."** →
`arc.relay.exists(table, key)`.

**"I need an import that never overwrites existing data."** →
`arc.relay.save_many(table, rows, match_on=[...], allow_update=False)`.

---

## 15. Common Mistakes to Avoid

- **Doing something externally visible in `after_save` instead of
  `after_commit`.** `after_save` can still be rolled back by something
  later in the same transaction — see §4.5.
- **Forgetting `fields=[...]` is required.** There's no `SELECT *` — name
  the columns you want, or `arc.relay.all_columns(table)` if you truly
  need every one.
- **Assuming `list()` returns everything.** It's capped by default
  (`relay_list_default_limit`) — pass `limit=None` if you deliberately
  want no cap.
- **Using `save()` for a bulk operation in a loop.** Use `save_many()` —
  one shared transaction, not one per row, and hooks still fire per row
  the same way.
- **Calling `arc.lineup.*` directly.** Always go through
  `arc.relay.enqueue`/`.task`/`.register_tasks` — that's the one facade
  every plugin should depend on; it degrades gracefully when `lineup`
  isn't installed, direct `lineup` calls don't.
- **Relying on the in-process cache/lock fallback in a multi-worker
  deployment.** It only works correctly within a single process — install
  `redix` if you run more than one worker.
- **Mixing typed scalar parameters and a Struct payload parameter on the
  same whitelisted function.** Pick one style per function.
- **Forgetting `roles=["Guest"]` for a genuinely public endpoint.** The
  default (`roles` omitted, or `["*"]`) means "any authenticated caller,"
  never public.

---

## 16. Quick Reference

| I want to…                                          | Use this                                          |
|---------------------------------------------------------|------------------------------------------------------|
| Insert or update one row                                  | `await arc.relay.save(table, data, ...)`             |
| Soft-delete one row                                        | `await arc.relay.delete(table, id, ...)`             |
| Insert/update many rows, one transaction                   | `await arc.relay.save_many(table, rows, ...)`        |
| Soft-delete many rows, one transaction                     | `await arc.relay.delete_many(table, ids, ...)`       |
| Fetch exactly one row                                       | `await arc.relay.get(table, key, fields)`            |
| Fetch multiple rows                                         | `await arc.relay.list(table, filters=..., fields=...)` |
| Fetch rows AND a total count                                | `await arc.relay.list_page(table, ...)`              |
| Just a count                                                 | `await arc.relay.count(table, filters=...)`          |
| Just a yes/no — does at least one row match                  | `await arc.relay.exists(table, key)`                 |
| GROUP BY + count/sum/avg/min/max                             | `await arc.relay.aggregate(table, ...)`              |
| Pull fields off a related row (one hop)                      | `arc.relay.resolve(field, subfields)`                |
| Raw SQL escape hatch                                          | `await arc.relay.sql(...)` / `.sql_one(...)` / `.sql_val(...)` |
| Every column name on a table                                   | `arc.relay.all_columns(table)`                       |
| Load hook files for a table                                    | `relay.register_hooks(dir)`                          |
| Attach behavior to a save/delete                                | `@arc.relay.validate` / `.before_save` / `.after_save` / `.before_delete` / `.after_delete` / `.after_commit` / `.on_rollback` |
| Reject a write with a clear reason                              | `arc.relay.throw(message, status=400, code=...)`     |
| Skip hooks for one call                                          | `skip_hooks=True` (or `skip_<event>=True`)           |
| Block insert or update in `save()`/`save_many()`                  | `allow_insert=False` / `allow_update=False`          |
| Make a function callable / HTTP-reachable                        | `@arc.relay.whitelist(methods=..., roles=..., path=...)` |
| Call a whitelisted function directly, in-process                   | `await arc.relay.call(name, **kwargs)`               |
| List every whitelisted function                                    | `arc.relay.whitelisted()`                             |
| Stream a large or long-running response                             | `arc.relay.stream(source)` / `await arc.relay.publish(event, **payload)` |
| Run something in the background                                     | `arc.relay.enqueue(fn, *args, queue=...)`            |
| Declare a schedulable/durable background task                       | `@arc.relay.task(queue=..., cron=...)`               |
| Cache a value                                                          | `await arc.relay.cache_set/get/delete(key, ...)`     |
| Prevent two callers from racing                                        | `async with arc.relay.lock(name):`                   |
| Know who's calling, right now                                          | `arc.relay.context()`                                |
| Log a message                                                            | `arc.relay.log(message, level=...)`                  |

---

**In one sentence:** Relay is the one facade every business plugin writes
against for reading/writing rows (with hooks reacting to every write),
exposing functions to the outside world (with role-based access control
built in), running background work, and caching/locking — psqldb owns the
data's shape, relay owns everything you actually do with it.
