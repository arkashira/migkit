# What a migration actually costs, and which of it migkit removes

This is the working map for where migkit goes next. It is written from what
people who have run migrations say goes wrong, from how the fast tools get
their speed, and from what this repository can already be measured doing. It
is deliberately blunt about the gaps: a list that only records wins is not a
plan.

The goal it serves: an operator configures a hop and two endpoints, and
migkit removes as many of the factors below as a tool possibly can.

## 1. The factors somebody has to handle

Ordered by how often practitioners name them, not by how hard they are.
"Covered" means migkit does it today and there is a live test proving it.

| Factor | What goes wrong | migkit today |
|---|---|---|
| Everything attached to the tables | Scope is written as "the tables", then permissions, sequences, triggers, views and downstream jobs surface in cutover week | **Covered in part** — checks for grants, ownership, sequences, constraints, indexes, extensions, triggers, matviews, RLS; `handwork` names what cannot be carried. Not covered: downstream jobs, application-side config |
| Privileges that do not exist on the target | The source has superuser, the managed target does not; `ALTER ROLE ... SUPERUSER` in a dump fails, `GRANT ALL ON DATABASE` grants less than people think, extensions are refused | **Partly** — grants and ownership are compared and repaired. The *pre-flight* "your target cannot accept this" report does not exist yet |
| Sequences and identity counters | Logical replication does not carry them; the target's counter sits below the source and the first insert after cutover collides | **Covered** — parity check plus a collision check, and a repair |
| Stored logic | Procedures, packages and triggers rarely run unmodified on another engine; the port/rewrite/retire decision is per object | **Not covered** — migkit reports their presence, converts nothing |
| Constraints and indexes around a bulk load | Dropped for speed, then rebuilt wrong or not at all | **Partly** — constraint and index repair exist; migkit does not own the drop/rebuild cycle of somebody else's load |
| LOBs | The single biggest full-load slowdown; tools truncate silently past a configured maximum | **Not covered** — no LOB strategy, and this is a correctness problem as much as a speed one: a truncated LOB is data loss that a row count will not find |
| Validation beyond row counts | Counts match while contents differ; the check that certifies equality is the one nobody audits | **Covered, and this is migkit's strongest ground** — injective row encoding, in-server digests, per-row drilldown, repair with undo |
| A difference nobody can act on | The validator says "3 rows differ" and stops | **Covered** — every engine names the rows and can repair them |
| Cutover | Not one switch: freeze, delta sync, rollback rehearsal, cache warming, dual writes | **Partly** — delta verify, revert/restore, leftovers. No cutover runbook the tool drives |
| Resume after a crash | A copy that dies at 80% starts over, or resumes inconsistently | **Partly** — chunked resume exists per engine, on one machine, with a proof file |
| Scale | Practices that work at 10 GB fail at 1 TB; nobody finds out until the rehearsal | **Measured to 10M rows** — [scale.md](scale.md): flat throughput, constant client memory, and one real bug found by running it |
| Cloud without tools | No OS access, no `pg_dump` on the box, no ports outbound, no place to put a dump file | **Partly** — everything runs from the operator's machine over normal client connections; no reliance on being on the host |
| Two engines that disagree about a value | Collation, timezone, NULL vs empty, float rendering, charset | **Covered, with the size of the refusal measured** — canonical rendering per engine, and a refusal when a type has no agreed rendering. On a 37-column PostgreSQL table the cross-engine path compares 20 and names the 17 it declines (`enum`, `interval`, `hstore`, `tsvector` among them); the same-engine path hashes the whole row and misses none. Catalogue D15 |

## 2. How the fast tools get fast

Worth copying, and worth knowing the cost of.

**pgcopydb** exports one snapshot with `pg_export_snapshot()` and has every
worker `SET TRANSACTION SNAPSHOT` to it, so parallel workers see one
consistent database. It orders tables largest-first across a pool of table
jobs, splits a table larger than a threshold into non-overlapping `WHERE`
ranges (by unique integer column, else by `ctid`), builds indexes
concurrently *after* the data, then creates the primary key `USING` the
already-built index to avoid an exclusive lock, and `setval()`s sequences at
the end. Its honesty is worth copying too: `--resume` requires
`--not-consistent`, because the exported snapshot is gone after a crash.

**AWS DMS** gets full-load speed from `MaxFullLoadSubTasks` (default 8, max
49), parallel load by partition or by hand-tuned ranges, deferring the
primary key to after the load, and commit-rate tuning. Its documented
advice is that `partitions-auto` is often *not* fastest, because skewed
partitions leave one long-tail subtask - the same lesson as pgcopydb's
range splitting. LOBs are where it loses: full LOB mode moves them one at a
time, limited LOB mode pre-allocates and truncates past `LobMaxSize`.

**What this means for migkit.** The speed is not in a clever transfer
protocol. It is in four things, none of which need a new wire format:

1. one consistent snapshot shared by every worker,
2. work ordered largest-first, with big tables split into ranges that are
   *balanced*, not merely equal in count,
3. indexes and constraints after the data, built in parallel,
4. not paying for LOBs on tables that have none.

migkit already shells out to the native movers for the pairs that have one.
The open question is the pairs that do not - cross-engine, and the engines
with no `pgcopydb` of their own. That is where a migkit-owned mover would
have to earn its place, and it should be measured against the native tools
on the same hardware before it is written, not after.

## 3. Where the competition stops

DMS validation needs a primary or unique key, refuses CLOB/BLOB keys and
VARCHAR keys over 1024, cannot handle NULL in a key, stops the whole task
after 10,000 failures, cannot validate a row that keeps changing, skips
views, will not span databases, skips a whole table when one column is
masked, and its enhanced validation covers four engine pairs and requires
Secrets Manager. migkit has none of those limits, and that is the part of
the product that is genuinely ahead rather than merely different.

## 4. The plan

In order. Each item says what has to be measured before it is written.

1. **Prove the scale claim, or retract it.** *Done to ten million rows -
   [scale.md](scale.md).* `bench/seed.py` builds the table and the numbers
   are published with the hardware beside them: throughput flat between 1M
   and 10M (~173k rows/s), and migkit's own memory did not follow the data
   (30 MB moving, 333 MB verifying, at both sizes). It also found a bug no
   small table could - a chunked table reported the differing chunk's row
   count as the table's. **Still open:** a LOB column in the bench table, a
   comparison against pgcopydb on this hardware, and anything above ten
   million rows (the 20 GiB sandbox disk is the ceiling).
2. **Oracle**, then the rest of the DMS list. Measured this tick:
   `gvenzl/oracle-free:slim` has an arm64 build, boots to Oracle 26ai Free,
   and `python-oracledb` 26 connects in thin mode with no Instant Client -
   so it is testable here. `icr.io/db2_community/db2` is amd64/ppc64le/s390x
   only, so Db2 would be emulation: it goes behind S3 (minio) and Redshift.
3. ~~**The index nobody can see is broken.**~~ **Done** - `check --deep`
   reads `indisvalid` on both sides, tells apart the invalid index that costs
   every write from the one that only holds its name, and warns rather than
   fails on a partitioned parent. **Corrected since:** migkit never builds an
   index at all, and the rebuild settings did not reproduce on this
   hardware (1.65 s against 1.54 s). What the printed setup plan *did*
   need was the ordering - see the entry on A7.
4. **The sort order that moved.** **Done** - `check --deep` compares the
   recorded collation version against the one the OS provides now, on both
   sides, for every collation something is actually sorted by. The duplicate
   hunt that follows it is done too: once something says an index cannot be
   trusted, `check --deep` groups by every unique text key with the index
   paths shut off. Measured on 200,001 rows, the planner left to itself
   reported 0 duplicates through an index-only scan where the hunt reports
   1.
5. **The text that was broken before the move.** **Done** - `check --deep`
   samples every text column on the source and reports which ones hold
   *both* double-encoded and correct rows, because that pair is what makes
   a blanket conversion destructive. The repair is done too: `sync` plans
   one `UPDATE` per confirmed row against the target and leaves the
   genuinely accented rows beside them untouched, with undo and a match on
   the old value. It is the one repair here withheld by default
   (`MIGKIT_REPAIR_TEXT`), because it is the one that moves the target
   *away* from its source.
6. **The temporal types.** **Done for the meaning** - `canon.time_meaning`
   answers what a temporal column is *for*, separately from how to render
   it, and `check --deep` compares it column by column on both engines. It
   exists because `timestamp` means opposite things in the two: PostgreSQL's
   is the wall clock, MySQL's is the instant, both measured. The zone rules
   behind them are done too: `check --deep` fingerprints what every named
   zone does at seven probe instants and reports a zone the target cannot
   resolve, a zone whose rules differ, or a server that can resolve none -
   all three proven live. Values the target has no room for are
   counted before the move too, as rows rather than as a schema opinion.
   **Still open from this item:** values of the wrong *shape* rather than
   the wrong size (`0000-00-00` stored by MySQL and refused outright by
   PostgreSQL), and which zones the data actually uses.
7. **Two things the target does to the rows as they land**, both
   measured this tick. **Done:** a `BEFORE INSERT` trigger on the target
   rewrote `2001-01-01` to today's date through a real `migkit move --go`,
   which reported `bulk copy complete`; the loading connection now carries
   `session_replication_role = replica` and the values survive. The
   `pg_dump` path was already covered by `--disable-triggers`, which is why
   the change is one line. The deep check now also names the enabled
   triggers on tables a load writes, so the operator knows what is being
   quieted on their behalf. The identity half is **done**: a
   `GENERATED ALWAYS AS IDENTITY` key refused the exact `insert ... on
   conflict` statement migkit's own repair builds, and the repair now emits
   `OVERRIDING SYSTEM VALUE` where the catalog says it is needed. `move`
   was measured to be unaffected, because `COPY` is not subject to the
   restriction.
8. **Two blind spots measured in migkit itself.** The first is **done**:
   `counts` and `data` reported OK on a target missing half its rows,
   because row-level security filtered both sides, and now say WARN naming
   what they could not see - while a role that sees everything is not
   nagged, the rule having been measured across all four role shapes.
   The second is **done** too: `_apply_upsert` could not write a
   table with a `GENERATED ALWAYS AS ... STORED` column at all, and now
   leaves those columns to the server on both engines. Unlike the identity
   case, `OVERRIDING SYSTEM VALUE` did not help - a test pins that.
9. ~~**Counts computed over nothing.**~~ **Done** - the merged counts
   line reported `OK 0 tables, rows 0==0` when the checksum pass errored on
   every table, and now names the tables it could not read. An empty
   database still reports ok, which is the distinction that made this more
   than a one-line change.
10. **The documents the table only points at.** A large object lives in
    `pg_largeobject`, not in your table, and there is no referential
    integrity between them. Measured on a pair whose table contents are
    byte-identical: the source resolves the oid to a document, the target
    answers `large object 16391 does not exist`, and `migkit check` says
    **`verdict: same`**. **Done** - the verification now compares
    `pg_largeobject_metadata` on both sides and checks that every oid
    column resolving on the source resolves on the target, while leaving
    alone the oid columns that hold something else entirely.
11. ~~**The pre-flight the practitioners keep asking for**~~ **Done for
    the data half** - `assess` now runs the deep checks that predict what
    the move will do (capacity, temporal meaning, time zone rules,
    collation versions, mojibake), with the fix hint attached, and leaves
    out the ones that compare what is on the target because that is empty
    before a move. **Still open:** what the target will refuse for reasons
    other than data - unsupported
    extensions, privileges the account does not have, types with no home
    on the other side. `assess` reports those today, but scattered rather
    than in the same section as the rest.
12. **LOBs**, as correctness first and speed second: find them, size them,
   and refuse to truncate silently.
13. **A cutover runbook migkit drives**: freeze, delta, verify, sequence
   reset, rollback rehearsal - the steps exist as separate commands today.
14. **Then** consider a migkit-owned mover, with the benchmark from item 1
   as the bar it has to clear.
15. **A difference an operator can see.** **Done** - `check --drill`
   used to lose a carriage return to the reader's own text decoding and
   report a differing table as clean, contradicting the digest in the same
   run; it now reads bytes and counts every invisible difference (NFC/NFD,
   trailing space, zero-width and non-breaking spaces, CRLF) - and names
   each one beside the escaped values, so the reader gets a finding rather
   than a number. Classified by Unicode category on the base contract, so
   both engines read one implementation.
16. **A verdict that states its own coverage.** **Done** -
   `verdict.json` from a run narrowed with `--table` and `--only` used to
   be byte-identical to one from a full run over a healthy database, while
   a table sat 60% empty; a CI gate on `has_differences` could not tell
   them apart. The envelope now carries a `coverage` block and reports
   `incomplete` rather than `same` when the run was narrowed, with
   `has_differences` left alone.
17. **Mapping and transformation, verified rather than declared.** *(core landed: `mapping.tables` / `mapping.where` on the hop, with the same right-anchored naming `exclude` uses, ambiguous renames refused and rules that match nothing reported - `test_mapping_rules.py`. `assess` says what the mapping would do before anything is copied - a collision fails it, a rule matching nothing warns, and a hop without a mapping prints nothing (`test_mapping_preflight.py`). The check reads the same mapping: `match_tables` pairs a renamed table with what it was renamed to, and a rename pointing at a table the target does not have stays missing and says the rename is why (`test_mapping_drives_the_check.py`). The MySQL mover pushes the row filter down: a generated defaults-file gives mydumper one section per table, verified live - 2 of 3 rows dumped where a rule applied, untouched where none did (`test_mapping_reaches_the_mover.py`). Still to wire: pgcopydb `--filters`, `pg_dump -t/-T`, Debezium `table.include.list`.)* DMS and
   DTS rename schemas, tables and columns and filter rows during a move,
   and migkit has none of it - only a database-name map (`_d(side, db)`).
   The shallow version is a rename list. **Deeper, and the part DMS cannot
   do:** the same mapping drives the *verification* leg, so a renamed table
   is compared against its renamed counterpart and a filtered load is
   compared against the same filter - DMS's own validation is documented as
   unable to validate a transformed target. Three more things it should do
   that a rule list does not: translate one `mapping:` block into each
   wrapped tool's own dialect (pgcopydb `--filters`, mydumper `--regex`,
   `pg_dump -t/-T`, Debezium `table.include.list` + `RegexRouter`) so the
   filtering happens at the mover and fewer bytes move; **refuse an
   ambiguous mapping instead of guessing**, the way `hetero.match_tables`
   already refuses two tables with the same unqualified name; and report a
   rule that matched nothing, because a filter that silently matches
   nothing is how a table goes missing.

18. **Continuous verification that costs what changed, not what exists.**
   GoldenGate Veridata compares on a schedule and reports; the comparison
   is a pass over the tables. **migkit already has the stronger mechanism**
   and it is worth saying so: `watch --verify --delta` re-verifies only the
   rows touched since the last verified point, driven by the WAL slot,
   binlog or change stream - implemented on postgres, mysql, mongodb, mssql
   and kafka - and the LSN fence tells a real difference from in-flight
   replication deterministically instead of waiting and hoping. What is
   missing is not the mechanism but the **operation around it**: a per-table
   ledger of last-verified position, verdict and cost; a sweep ordered by
   risk (changed most, failed before, never verified) rather than
   round-robin; and repair that stays bounded on a table too big to hold.
   That is the Veridata capability, reached from a better starting point.

19. **Bisection diffing across engines, on a rendering both sides agree
   on.** `reladiff` is wired for PostgreSQL only. data-diff's bisection is
   the right shape for a table too large to hash whole, and the reason it
   cannot be trusted across engines is that it leans on each server's own
   checksum semantics. **migkit has the missing piece already**: the canon
   layer defines a canonical rendering per engine, so the bisection can run
   over text both sides agree on. Two things it must do that data-diff does
   not: a column with no canonical rendering is named per column rather
   than quietly left out of the hash (catalogue D15), and a mismatch found
   mid-bisection is put through the same in-flight fence before it is
   called a difference.

20. **PL/SQL conversion with behavioural proof.** Section H says stored
   logic is reported and never converted. Ora2Pg converts it, and wrapping
   Ora2Pg is the obvious move. **The deeper half is what nobody ships:** a
   conversion is a guess until the two versions are run against the same
   inputs and their outputs compared - which is the one thing a tool that
   already owns a verification engine can do. Convert with Ora2Pg, run both
   sides, compare with the machinery that already exists, and put what
   could not be converted or could not be proven into `handwork` by name.


## Sources

- [A Practical Guide to Database Migration Without Operational Chaos](https://dev.to/kate_steeleeee/a-practical-guide-to-database-migration-without-operational-chaos-563m)
- [How We Migrated 1 Billion Records Without Downtime](https://medium.com/@himanshusingour7/how-we-migrated-db-1-to-db-2-1-billion-records-without-downtime-c034ce85d889)
- [Database Migration Best Practices: A Field-Tested Checklist](https://www.disqr.com/services/data-migration-services/database-migration-best-practices/)
- [AWS DMS: Setting LOB support](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.LOBSupport.html)
- [AWS DMS: table and collection settings, parallel load](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Tablesettings.html)
- [Perform parallel load for partitioned data using AWS DMS](https://aws.amazon.com/blogs/database/perform-parallel-load-for-partitioned-data-into-amazon-s3-using-aws-dms/)
- [pgcopydb: resuming operations and snapshots](https://pgcopydb.readthedocs.io/en/latest/resume.html)
- [pgcopydb clone reference](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html)
- [pgcopydb concurrency design](https://www.rockdata.net/external/pgcopydb-concurrency/)
- [Understanding the rds_superuser role](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Roles.rds_superuser.html)
- [RDS delegated extension support](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/RDS_delegated_ext.html)
- [Logical replication for Amazon RDS for PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html)
