# Case study: what a "consistent" report does not cover

Managed migration services move data well. Most of them also run a consistency
check and report that the source and the target match.

That report is usually true. It is also much narrower than it sounds, and the
gap between "the check passed" and "the database works" is where migrations
fail.

This is a write-up of that gap, from migrations we verified with migkit after a
managed service had already reported success. Names, accounts and identifiers
are removed; the findings and the counts are real.

---

## 1. What row-level validators actually compare

Read the documentation of any managed validator and you find a list of
exclusions. The wording differs by vendor, the shape does not. Typically
excluded:

- stored procedures and functions
- views
- user accounts
- anything not selected in the migration task
- data written to the target outside the task

And typically limited:

- tables with **no primary key** are checked only below a row threshold
  (we have seen 10,000 and 50,000 depending on the engine), and skipped above it
- **DDL during the sync is not detected**, so the check result may be wrong and
  has to be re-run
- a single query that runs longer than a fixed timeout fails the check task —
  and long queries are exactly what large tables produce
- for large volumes, **sampling** is offered instead of full extraction,
  because full extraction loads the source

One more, which matters more than it looks:

- some validators **write a checksum table into the source database**. If the
  source is read-only, the check is skipped.

So "100% consistent" means, accurately: *the rows of the tables you selected
matched at the moment we compared them, if nothing on that list applied.*

That is a useful guarantee. It is not the guarantee the phrase suggests.

---

## 2. The failures we found, after the check passed

Every item below was found by migkit on a migration that a managed service had
already reported as consistent. None of them is a row, which is exactly why a
row comparison passed.

### Sequences had collided — the app breaks on the first INSERT

Five sequences where the target's next value was **at or below** the maximum id
already present in the table:

| Sequence | Next value | Max id in table |
|---|---|---|
| A | 2,574 | 2,595 |
| B | 3,916 | 3,928 |
| C | 203,277 | 205,193 |
| D | 1 | 3 |
| E | 1 | (populated) |

Each one is a guaranteed duplicate-key error on the first insert after cutover.
Every row matched. A sequence is not a row.

This is not a misconfiguration: at least one vendor's own migration notes
instruct the operator to update sequence values **by hand**. The problem is not
that the step exists. The problem is that the "consistent" report is issued
**before** anyone performs it.

Separately, sequence *parity* had drifted on several more where no collision
had happened yet.

### Permissions were missing at a level the service does not migrate

One vendor documents it plainly: privileges are migrated at the database and
global level only, not at the table, column or procedure level. Table-level
grants are how most applications are actually permissioned.

| Migration leg | Missing table grants |
|---|---|
| 1 | 511 |
| 2 | 208 (plus 53 the target had and the source did not) |
| 3 | 200 (plus 97 extra) |
| 4 | 120 |

**Sequence grants** were missing separately: 21, 18 and 3 across three legs.
This one is hard to debug from the symptom. The application can `SELECT`. The
table grants look complete. Every `INSERT` fails with
`permission denied for sequence`.

Neither a row comparison nor a table-grant comparison finds it.

### 26 application accounts did not exist on the target

Across 20 databases, 26 accounts present on the source were absent on the
target. Each one is an application that cannot log in at cutover.

Accounts are documented as excluded from the consistency check, so nothing in
the managed service would have said so.

### The tables most likely to be wrong were the ones never checked

Tables with no primary key stack two problems:

1. CDC **drops their updates and deletes** — there is no key to match a change
   against
2. the validator **skips them** above its row threshold

So the tables at highest risk of being wrong are precisely the tables nobody
verifies. We found 12 of them across 4 databases.

migkit's checksum is a commutative aggregate over the whole table and needs no
key at all, which is why it can answer for these.

### The current month's partitions arrived empty

| Table | Source | Target |
|---|---|---|
| `orders_<current_month>` | 73 | **0** |
| `order_items_<current_month>` | 83 | **0** |
| `order_status_history_<current_month>` | 210 | **0** |
| `packages_<current_month>` | 68 | **0** |

The live partition — the one holding this month's orders — was empty. Row
counts at the parent level can hide this completely.

### The target had rows the source never had

| Table | Source | Target |
|---|---|---|
| A | 293 | 322 |
| B | 158,892 | 158,895 |
| C | 66,482 | 66,483 |

This direction matters. A validator built around *"did everything from the
source arrive?"* answers yes. It never asks *"is there anything here that
should not be?"*

### NULL became empty string

Counts identical. Checksums of counts identical. Application behaviour
different, because `IS NULL` and `= ''` are different predicates.

12 text columns on one leg, 5 on another.

### Server settings changed the meaning of identical rows

- `default_text_search_config` went from `simple` to `english` on **every**
  PostgreSQL database. Full-text search returns different results on rows that
  are byte-for-byte identical.
- `system_time_zone` UTC → CST and `time_zone` SYSTEM → +00:00 across 21 MySQL
  databases.

The data is equal. The system is not.

---

## 3. The checklist: what has to match before a database is usable

"Did the rows arrive" is one line of this.

### Data

| Factor | Typical managed validator | migkit |
|---|---|---|
| Row counts | yes | yes |
| Row content | yes | yes |
| Content of tables with **no key** | skipped above a threshold | yes — needs no key |
| Which column differs | no | yes |
| Shape: missing / extra / replaced / changed | no | yes, in the same scan |
| Target rows the source never had | no | yes |
| Rows landed in the right partition | no | yes |
| NULL vs empty-string split | no | yes |
| Float and decimal drift | no | yes |
| Whole-column timestamp shift | no | yes |

### Identity

| Factor | Validator | migkit |
|---|---|---|
| Sequence is **usable** (next value above max id) | no | yes |
| Sequence parity with source | no | yes |

### Access

| Factor | Validator | migkit |
|---|---|---|
| Users exist | no (excluded) | yes |
| Password hashes match | no | yes |
| Table grants | no | yes |
| Sequence grants | no | yes |
| Row-level security policies | no | yes |
| Object owner (who may ALTER or DROP it) | no | yes |
| DEFINER and SQL SECURITY mode | no | yes |

Zero RLS policies on a table that had them is either deny-all or expose-all.
Neither is what you migrated.

Ownership is the one a schema differ structurally cannot catch: it compares
definitions, and an owner is not part of a definition. Measured - a table
owned by the application role arrived owned by the migration account, and the
generated fix contained no `OWNER TO` statement at all.

The MySQL half is worse than a name change. A view or routine that was
`SQL SECURITY DEFINER` and came across as `INVOKER` now runs with the
**caller's** privileges instead of the definer's. It either stops working, or
starts working for callers who should not have been able to run it. Schema
comparison misses this on purpose: movers rewrite `DEFINER=` on every object,
so a text diff that kept it would bury every real finding under noise. The
answer is to report it separately, not to ignore it.

### Schema objects

| Factor | Validator | migkit |
|---|---|---|
| Views | no (excluded) | yes |
| Functions, procedures, triggers | no (excluded) | yes |
| Indexes and keys | no | yes |
| Foreign keys and orphan rows | no | yes |
| CHECK constraints, including `NOT VALID` | no | yes |
| Deferrable constraint settings | no | yes |
| Generated columns | no | yes |
| Column type narrowing | no | yes |
| Materialized view **content** | no | yes |
| Extensions present on the target | no | yes |

A materialized view is the quiet one: the definition travels, the contents do
not. Reads succeed and return stale data, with no error anywhere.

Type narrowing is the dangerous one: a shorter `varchar` or a smaller int on
the target truncates on write, silently, forever.

### Engine behaviour

| Factor | Validator | migkit |
|---|---|---|
| Database encoding | no | yes |
| Collation and charset | no | yes |
| Server parameters | no | yes |
| Time zone settings | no | yes |

Collation changes sort order **and uniqueness**. Two rows that were distinct
under one collation can collide under another.

### MongoDB

| Factor | Validator | migkit |
|---|---|---|
| Document content by `_id` | often **not offered for the link at all** | yes |
| Index definitions | no | yes |
| BSON type drift (int32 vs double) | no | yes |
| Capped collection settings | no | yes |
| Shard key | no | yes |

A time-series or capped collection created by plain inserts becomes an ordinary
collection holding the same documents. No error is raised. The shard key cannot
be changed after data lands.

### Objects no mover carries

| Factor | Validator | migkit |
|---|---|---|
| Tables with no key | no | yes |
| Scheduled events / jobs | no | yes |
| Objects whose DEFINER is missing on the target | no | yes |
| Capped / time-series collections created as the wrong kind | no | yes |
| Large objects (outside every table) | no | yes |

### The conditions of the proof itself

| Factor | Validator | migkit |
|---|---|---|
| Verify with the source **frozen / read-only** | check is skipped if it must write to the source | yes — reads only |
| One consistent snapshot across all tables | no | yes — one transaction per side, LSN fence |
| Verification survives a restart | task fails, start again | yes — resumable |
| Verification does not overload the source | answer is sampling | yes — backs off on the source's own load |
| Verify a migration **someone else** performed | no | yes |

That last row is the structural one. **You cannot audit a vendor with the
vendor's own tool.** migkit points at two databases; it does not care who moved
the data or whether a task exists.

---

## 4. The work that still has to happen after the report

These steps come **after** the "consistent" report. Until they are done, the
database does not work.

| Step | What breaks if skipped |
|---|---|
| Set sequence / AUTO_INCREMENT values | First INSERT fails: duplicate key |
| Create application users | Apps cannot log in at all |
| Apply table-level grants | App connects, then permission denied |
| Apply sequence grants | Every INSERT fails |
| Recreate views, functions, procedures | Queries the app calls do not exist |
| REFRESH materialized views | Reads return stale data, no error |
| Recreate scheduled events | Batch work never runs; found at month end |
| Align server parameters | Same rows, different results |
| Create capped / time-series collections as the right kind | Wrong behaviour, no error |
| Move large objects | Blobs missing |
| Disable TTL indexes and events on the target during sync | **The target deletes its own data while syncing** |
| Validate `NOT VALID` constraints, fix FK orphans | Invalid data accepted from day one |
| Fix object ownership | The app no longer owns its own tables and cannot alter them |
| Prove all of the above | — |

None of these is a row.

---

## 5. Doing it with migkit

```bash
# before the move: readiness, plus an inventory of what no mover will carry
migkit assess <hop>

# after the managed service reports success
migkit check <hop> --deep

# schema objects the validator excluded
migkit sync <hop> --kind schema            # dry run
#   reads structural-fix.locks.txt  - what each statement blocks
#   reads structural-fix.revert.sql - the undo, and what it cannot restore
migkit sync <hop> --kind schema --apply

# accounts, keeping the same password hash
migkit users <hop> create --apply

# sequences: setval(GREATEST(source, target_max)) so it can never sit below a
# live row, with undo
migkit sync <hop> --kind sequences --apply

# rows
migkit sync <hop> --kind rows --apply

# cutover: freeze writes, then prove it, in one transaction per side
migkit check <hop> --consistent

# evidence and a way back
migkit sync <hop> --go --tag pre-cutover
migkit report <hop> --open
migkit history <hop>
migkit rollback <hop> --state pre-cutover --apply
```

`migkit check --consistent` writes nothing to the source. That is what makes
the safest cutover order — freeze, prove, switch — possible at all.

Grants are applied by the same `--kind schema` run, because a GRANT is DDL -
there is no separate word to learn. Each one gets a REVOKE saved as its undo.

Extra grants on the target are reported but **not** revoked. Removing a
privilege somebody added on purpose is a different decision from restoring one
the migration dropped, and doing both under one word would hide the second
inside the first.

---

## 6. The honest part

**This is not an argument that managed migration services are bad.** They move
data across networks, accounts and regions at a scale and reliability that a
script on one host does not match, and they support engine pairs migkit cannot
move at all. On the migrations described here we used them to move, and migkit
to prove.

It is also not an argument that any one vendor is worse than another. We ran
the return legs on a different vendor's managed service and verified those the
same way. It TRUNCATEd a partitioned parent table — deleting every child
partition — while reporting zero errors and a healthy table count. 258 million
rows. On another table it dropped a LOB column, wrote NULL, reported the
correct row count, and raised no error: 8,637 values.

Neither of those would be found by the vendor's own validator, because the
validator and the fault come from the same place.

**The claim is narrower and more useful than "vendor X is unreliable":**

> A mover's consistency report answers whether the rows it selected arrived.
> Whether the database is usable is a different question with about forty
> parts, and the report does not touch most of them.

Verify with something that has no stake in the answer.
