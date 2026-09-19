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
| Scale | Practices that work at 10 GB fail at 1 TB; nobody finds out until the rehearsal | **Unproven** — largest measured run here is 200k rows on a laptop |
| Cloud without tools | No OS access, no `pg_dump` on the box, no ports outbound, no place to put a dump file | **Partly** — everything runs from the operator's machine over normal client connections; no reliance on being on the host |
| Two engines that disagree about a value | Collation, timezone, NULL vs empty, float rendering, charset | **Covered** — canonical rendering per engine, and a refusal when a type has no agreed rendering |

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

1. **Prove the scale claim, or retract it.** Generate rows with `faker`
   into the docker sandbox - 1M, 10M, and a table wide enough to matter -
   and record: full-load time per mover, verify time, memory, and where it
   falls over. Publish the numbers with the hardware beside them. Until
   this exists, "works at scale" is not a claim migkit is allowed to make.
2. **Oracle**, then the rest of the DMS list. Measured this tick:
   `gvenzl/oracle-free:slim` has an arm64 build, boots to Oracle 26ai Free,
   and `python-oracledb` 26 connects in thin mode with no Instant Client -
   so it is testable here. `icr.io/db2_community/db2` is amd64/ppc64le/s390x
   only, so Db2 would be emulation: it goes behind S3 (minio) and Redshift.
3. **The pre-flight the practitioners keep asking for**: before anything
   moves, report what the target will refuse - unsupported extensions,
   privileges the account does not have, types with no home on the other
   side, tables with no key, LOB columns. Everything needed for this is
   already in `assess`; what is missing is saying it in one place, early.
4. **LOBs**, as correctness first and speed second: find them, size them,
   and refuse to truncate silently.
5. **A cutover runbook migkit drives**: freeze, delta, verify, sequence
   reset, rollback rehearsal - the steps exist as separate commands today.
6. **Then** consider a migkit-owned mover, with the benchmark from item 1
   as the bar it has to clear.

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
