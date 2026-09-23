# Backlog: what is left before migkit finishes the job

One list, rebuilt on 2026-09-23 from:

* **Inside the project:**
  * the problems `problems-and-what-ends-them.md` still marks *Partly* or *Not yet* (18)
  * the plan items in `what-a-migration-actually-costs.md` not yet done (10)
  * the queue carried by the work loop
* **Outside:** how the managed services and the specialist tools actually
  work. The mechanism matters, not the feature name:
  * the managed services: AWS DMS, Azure's PostgreSQL migration service,
    Google Cloud DMS, Tencent DTS
  * the verifiers and movers that do one thing well: Oracle GoldenGate
    Veridata, Crunchy pgCompare, Google's Data Validation Tool, Vitess
    VDiff, Debezium's DBLog snapshots, MongoDB's migration-verifier

**How to read an item.** Each item says four things:
* what the best tool does, down to the mechanism
* what migkit has today, found by grepping the code rather than
  remembering it
* what the deeper version would be
* whether something already written can be wrapped to get there

"Wrap" means what was done with reladiff, migra/results, pgcopydb and
mydumper. The tool goes underneath, migkit decides when and how, and the
operator never sees the tool's name. Every wrap is measured before it is
trusted. Three tools have already surprised: `atlas migrate lint` needed a
paid login, `liquibase rollback` wrote tables into the target, and half the
mydumper flags in use no longer existed.

Rules that apply to every item:
* no new CLI modes or flags; hop options and environment variables are fine
* migkit never writes to a source
* a hop that does not use a feature behaves exactly as before
* nothing lands without a test against a real database

---

## What the others do that shapes this list

**Verification that runs while data is still moving.** Every serious
verifier has an answer for rows that differ only because a change has not
arrived yet:

| Tool | Mechanism |
|---|---|
| Veridata | hashes each row's non-key columns and puts mismatches in a "maybe out of sync" queue. After the configured replication latency has passed it re-reads only those keys. Each is then *in-sync*, *in-flight* (changed since, cannot be judged), or *persistently out of sync*. Only the last is reported, with repair SQL. |
| pgCompare | re-fetches only the flagged rows in its `check` pass. Its 0.7.0 fix is worth copying: a row deleted from both sides between rounds must count as converged. It used to fail the table forever. |
| migration-verifier (MongoDB) | queues every changed or mismatched document into a numbered *generation* and rechecks one generation per round until writes are switched off. It fails permanently on DDL. |
| DTS | re-checks N times at an interval. |

**Scale without holding a snapshot open:**

| Tool | Mechanism |
|---|---|
| Vitess VDiff v2 | resumable, can be stopped and resumed, re-runnable as a rolling diff. `--max-diff-duration` restarts a table's diff so a long repeatable-read snapshot does not hold back purge. |
| Debezium incremental snapshots (Netflix's DBLog) | read a table in chunks between low and high watermarks in the change log. Streamed events win over snapshot rows for the same key inside a window, so no global snapshot is ever held. The read-only variants use GTID sets (MySQL) or transaction IDs (PostgreSQL) instead of writing a signal table. |

**Repair after verification:**

| Tool | Mechanism |
|---|---|
| AWS DMS data resync (2025) | reads validation failures from a control table on the *target*, fetches each key's current row from the source, and upserts or deletes. Under CDC it runs on a cron schedule with a maximum duration, **pausing replication and validation while it works**. Needs a primary key. |
| Veridata | generates repair DML. |
| DTS independent check | generates repair SQL. |

**Before the move:**

| Tool | Mechanism |
|---|---|
| AWS DMS premigration assessments | one named check per risk. Examples: long transactions, `max_slot_wal_keep_size`, `logical_decoding_work_mem`, binlog compression, binlog retention > 0, `binlog_row_image=FULL`, `server_id>=1`, `max_allowed_packet` against the largest LOB, tables differing only by case, ARRAY columns without a key, PostGIS, target is a read replica, more than 10,000 tables. |
| Google DMS "test job" | **suggests the parameter values** instead of only failing on them. |
| Azure's service | runs a validate-only pass until it is clean. It also advises: target storage 1.25-2x the source for WAL, and HA and read replicas off on the target during the load. |
| DTS | 12 checks for PostgreSQL and 17 for MySQL, including multi-task conflict detection and DDL-loop detection. |

**What none of the managed services do.** Azure's own documentation tells
the operator to validate the data by hand: counts, object counts, min/max
ids. Google's PostgreSQL path says the same and points at DVT for the rest.
This is the ground migkit stands on.

---

## P0: the decision layer underneath everything

**0. Choose per table, from measured facts, and combine.**

* **Today:** one function, `movers.pick()`, chooses the mover per *engine*.
  It takes the first tool found installed (pgcopydb, pg_dump, mydumper,
  pgloader, mongodump) and falls back to the builtin copier. It never looks
  at:
  * how big the table is
  * whether it holds LOBs or has a key
  * the versions on either side
  * which network path works
  * whether a change stream has to follow

  Verification works the same way: the builtin checksum always, a second
  reader only when an environment variable asks for one.
* **Deeper:** a planner that decides for each table:
  1. reads the facts once: size, key, LOB columns, types, versions,
     reachability, the flags each installed tool actually has (`tool_flag`
     already asks the binary)
  2. picks a path per table rather than per database. A 40 GB keyed table
     can go through the parallel copier, a LOB table through a path that
     carries LOBs whole, and a key-less table through a consistent snapshot.
  3. combines them in one run
  4. picks the verification the same way: an independent second reader
     where the pair is cross-engine, or where the first reader has a known
     blind spot
* **What the operator sees:** only what was done and why, in migkit's own
  words ("copied in parallel chunks: 40 GB, keyed"). Never which program
  did it.
* **Wraps it combines:** everything already wrapped, plus item 8's DVT.
* **Tests:** assert the decision per table against real tables built to
  each shape.

**Rule from the owner (2026-09-24), binding on every item from here on:**
* choosing the tool is migkit's decision, made per task, per engine, and
  per table: the smartest choice, the best quality, the fastest finish
* progress and logs are normalised into migkit's own events and words.
  Nothing a wrapped program prints reaches the operator as that program's
  output.

**0e. One command, one result, every engine.**

*Owner's rule (2026-09-24):* the same hop configuration and the same
commands give the same result, with the full capability, on every engine
and every provider - the way DMS and DTS present one task whatever sits
underneath. How each engine gets there is migkit's business, and it is
normalised the same way everywhere. Where an engine has no native way, find
the library or tool that produces the result, wrap it, and measure it.

*Measured from the code on 2026-09-24* (which engine class implements each
capability itself, plus the movers and `users.py`):

| capability | pg | mysql | mongo | redis | kafka | mssql | sqlite | generic | hetero |
|---|---|---|---|---|---|---|---|---|---|
| schema / counts / data / deep | yes | yes | yes | no schema | yes | yes | yes | yes | no deep |
| bulk move | yes | yes | yes | - | - | - | - | - | yes |
| table-by-table copy | yes | yes | - | - | - | - | - | - | yes |
| change stream (CDC) | yes | yes | tail only | - | - | - | - | - | yes |
| fence before cutover | yes | - | - | - | - | - | - | - | - |
| delta verification | yes | yes | yes | yes | yes | yes | - | - | - |
| sequences / auto-increment | yes | yes | - | n/a | n/a | yes | yes | - | - |
| server parameters | yes | yes | yes | - | - | yes | - | - | - |
| users and grants | yes | yes | yes | - | - | - | - | - | - |
| moved-nothing guard, post-load statistics | yes | yes | - | - | - | - | - | - | - |
| snapshot and rollback | yes | yes | yes | - | - | - | - | - | - |

**The deeper version:**
* **A declared matrix, not a hidden one.** Every engine states, for every
  capability, one of: *implemented*, *not applicable* (with the reason),
  or *not yet* (with the backlog item).
* **A test holds the declarations against the code**, both ways: a method
  present must be declared, and a declared one must exist. A gap can
  neither appear nor disappear unnoticed.
* **The operator is told in migkit's words when a command is not yet
  available for an engine**, and what to do instead - never a traceback.

**Filling the gaps, by engine.** Each wrap candidate is measured before it
is trusted:
* **Redis:**
  * move and sync: redis-shake (scan, sync and restore modes; clusters) or
    RIOT, with native `DUMP`/`RESTORE` as the fallback
  * fence: replication offset
  * users: ACL users and their rules
  * schema: keyspace types, TTL policy and config parity
* **Kafka:**
  * move and sync: MirrorMaker 2, whose checkpoints translate consumer
    offsets (with item 13)
  * fence: end offsets per partition
  * users: ACLs and SCRAM credentials
  * schemas: topic configuration and the schema registry
* **MongoDB:**
  * a collection-by-collection copier (the builtin path is missing)
  * `move --mode cdc` over change streams (`tail_apply` exists; wire it in)
  * fence: resume token
  * the guard and post-load index builds
  * `mongosync` for mongo-to-mongo (item 12)
* **SQL Server:**
  * bulk move: `bcp`, or a builtin copier
  * stream: CDC or Change Tracking (delta already reads Change Tracking)
  * fence: LSN
  * users: logins and users
  * post-load: statistics
  * snapshot and rollback
  * depth still needs an x86 runner (item 27)
* **SQLite:** builtin copier, pragma parity, guard, snapshot.
* **Generic** (warehouses and the engines reached through the second
  readers): moves via item 33. Checks exist already.
* **Hetero** (cross-engine):
  * deep battery (6-ก)
  * sequence and auto-increment carried across engines
  * parameters that mean the same thing on both engines, compared as such
  * guard, post-load statistics, snapshot

**Transform, in scope here, means migration-time transformation:** rename,
filter, column subset and rename, type conversion and column expressions.
It must work identically through every engine's copy, check and repair.
General-purpose ETL stays out, as the owner set earlier. Whether that
still holds is an open question to the owner.

**0c. Progress and logs in migkit's own words.**
* **Today:** `_sh` logs every command line it runs (`$ pg_dump ...`,
  `$ mydumper ...`), and some paths log a program's own message
  (`pg_restore ignored version-mismatch SET statements`).
* **Deeper:**
  * one progress vocabulary for every path: phase, table, rows, bytes,
    rate, ETA
  * each wrapped program's own progress output parsed into it
  * the command line kept in the run's local log for whoever debugs
    migkit, never in what the operator reads
* **Guard:** a test that runs each path against a live pair and scans
  everything the operator sees for program names. The static scan cannot
  see names that arrive through a variable.

**0d. The row filter is honoured by the move and ignored by the check.**

*In progress (2026-09-24): check side written and proved live, commits together with the routing below: before, PostgreSQL said `public.orders src=4 dst=2` and MySQL `missing=2 ...` `kind=rows-missing` for a correctly filtered move. Now every check read goes through one `_scope()` per engine: counts, checksums, chunk ranges, drilldown, and the MySQL second reader's `--where`. Target rows *outside* the filter are counted and reported separately, so narrowing the comparison hides nothing. `test_the_check_reads_the_row_filter.py`: 10 tests, 6 of them fail on the old code. Still open: the routing and the refusal wording below.*

`Hop.row_filter()` has no caller anywhere in `migkit/`. Its docstring says
the predicate is "pushed into the mover's own flag and into the checksum's
`WHERE`". The MySQL mover does apply it, through `mydumper_defaults`. No
engine's check reads it.

So a filtered move is expected to be reported as missing rows for good,
which is the exact failure `refuse_unpushable_filters` says it prevents.
That refusal also names programs, through a variable the static guard
cannot see (`"pg_dump 18.6 filters by table..."`, `f"the {via} mover..."`).

It is also item 0's first real decision: route the filtered tables through
a path that can apply the predicate (the builtin copier's `_copy_select`
already takes one), send the rest down the fast path, and apply the same
predicate in every check - instead of refusing the whole database.

**0a. Wrap the Data Validation Tool as a second reader.**

DVT (`google-pso-data-validator`, built on Ibis) does:
* column aggregates, row hash, and schema validation
* custom-query validation
* partitioned runs for large tables

It reaches engines migkit does not read natively: Oracle, Teradata, Db2,
Snowflake, BigQuery, Spanner.

**Measured install (8.9.3, into a scratch venv on Python 3.12):** 34 s,
547 MB, 107 packages. It imports and its CLI starts. It **cannot share
migkit's environment**; it pins an older stack than the one migkit runs on:

| Package | migkit | DVT |
|---|---|---|
| numpy | 2.5.3 | 1.26.4 |
| pandas | 3.0.5 | 2.3.3 |
| pyarrow | 25.0.1 | 14.0.2 |
| sqlglot | 30.18.0 | 19.9.0 (through ibis-framework 7.1.0) |

So the wrap is **out of process**, the way pgcopydb runs from its own
container image:
* `doctor --install` builds it a venv of its own
* migkit drives it through a small runner inside that venv, which calls its
  Python API and hands back JSON
* its findings are translated into migkit's verdict envelope
* its name is never printed

**Read in its source (8.9.3) before measuring:**
* **Results:** with no result handler configured, DVT prints a table to
  stdout. It also ships a `postgres` result handler that **writes results
  into a database table**. Pointed at the target, that is a write to the
  target. `DataValidation(config, result_handler=...)` takes a handler
  object, so the runner passes one whose `execute(df)` just returns the
  frame: nothing printed, nothing written, and the frame goes back to
  migkit as JSON.
* **Saved state:** named connections live under the directory in
  `PSO_DV_CONFIG_HOME`, defaulting to one in the user's home. The runner
  points it at a directory of migkit's own for the run and passes
  connections inline.

Still to measure:
* that, run that way, it writes nothing to either side: tables and
  schemas listed before and after, on both
* its speed against migkit's own checksum on the same pair
* which of its type mappings disagree with `canon`, where it would call
  equal what migkit calls different

The planner (item 0) uses it where it sees something migkit's own reader
does not, not everywhere.

**0b. Re-snapshot through the stream without pausing it.**

*Answered already, and measured: the path does not write to the source.*

Debezium is wrapped (`movers.py`, Kafka signal channel), and `sync` asks it
to re-read the tables a check found wrong (`resnapshot_message`). Debezium
refused the standard incremental snapshot on that pipeline. It brackets
each chunk with watermark rows written to a signalling table **in the
source**, and without that table:

    Incremental snapshot is not properly configured, either sinalling
    data collection is not provided or connector-specific snapshotting
    not set

So migkit sends a *blocking* snapshot instead. Measured: 50 rows re-emitted,
`snapshot=BLOCKING snapshot_completed=true`, nothing written to the source.
The price is that streaming stops while the table is re-read.

**Deeper:** the read-only incremental variants. They take the watermarks
from the server instead of writing them: the executed GTID set on MySQL
(needs `gtid_mode=ON`), the in-progress transaction ID on PostgreSQL.
Chunks interleave with the stream, and a conflicting key keeps the
streamed event (the DBLog rule).

The planner picks per table:
* read-only incremental where the source allows it
* blocking where it does not, saying that streaming will pause and for how
  long, from the table's size and the measured rate

Measure on this pipeline first:
* that `read.only=true` on the connector version shipped here runs without
  a signalling table
* that a key updated during a chunk ends up with the streamed value

## P0: correctness at cutover

**1. Confirm before calling it different.**
* **Today:** the PostgreSQL LSN fence re-compares only after the consumers
  have flushed past a captured position, and `watch` names hot tables.
  Other engines settle on elapsed time. The verdict does not distinguish
  "still in flight" from "wrong".
* **Deeper:** every engine gets Veridata's three states, per key:
  * the first pass queues suspects
  * the confirm pass re-reads only those keys once the lag threshold has
    passed. The threshold is measured from the pipeline where migkit can
    see it (slot flush, binlog position, change-stream resume token), and
    is a hop option otherwise.
  * *in-flight* is reported as such, never as a pass or a difference
  * a key missing on both sides at confirm time counts as converged
    (pgCompare's bug)
* **Wrap:** none needed; the queue is migkit's own drilldown key files.

**2. Hold the source snapshot for a bounded time.**
* **Today:** `check --consistent` opens one repeatable-read transaction per
  side for the whole pass. On a large table that holds back vacuum
  (PostgreSQL) or purge (InnoDB history list) for as long as the pass takes.
* **Deeper:** a per-table time limit, after which the table's pass restarts
  from its last completed chunk. The chunk checkpoints already exist in
  `checkpoint.py`. `assess`/`doctor` report the source's `backend_xmin` age
  and `innodb_history_list_length` while it runs.

**3. Stop the application writing to the target before cutover.**
* **Today:** nothing sets it. DTS has `IsDstReadOnly`.
* **Deeper:** read-only for the application's roles, not for the load:
  * PostgreSQL: `ALTER ROLE ... SET default_transaction_read_only`, applied
    only to the roles the hop names, with the undo written first
  * MySQL: `super_read_only` blocks the loader too, so this needs
    measuring before choosing
* Reported in `doctor`, reverted by `rollback`.

**4. A repair that knows the stream is running.**
* **Today:** the rule "no data repair while CDC runs" lives in the runbook,
  not in the code.
* **Deeper:** DMS's shape, where migkit owns the stream:
  1. pause the subscription or follow process
  2. repair the confirmed keys
  3. resume
* It runs inside a window with a maximum duration, as a hop option. Where
  migkit does not own the stream, the repair refuses and names the stream it
  can see.

**5. DDL during the move.**
* **Today:** a DDL on the source mid-move is invisible until the next
  schema check.
* **Deeper:**
  * detect it in the window, from the replication stream where migkit
    reads one, and from catalogue snapshots otherwise
  * mark every data verdict taken across it as stale; migration-verifier
    fails permanently for the same reason
  * for MySQL native replication, carry DTS's allow-list of DDL kinds
  * recognise online schema-change temp tables (`_gho/_ghc/_del`,
    `_new/_old`) and the final `RENAME`, instead of reporting them as
    strangers

## P1: before the move - one assessment that answers every managed service's list

**6. Pre-checks with the value to set, not just the verdict.**

Merge the AWS, Google, Azure and DTS lists into `assess`. Each check prints
the value to set, computed from what the server reports. For example,
`max_replication_slots >= databases in scope + slots already in use`.

Items not already present (each confirmed by grep before it is written):

| Area | Checks to add |
|---|---|
| PostgreSQL | `logical_decoding_work_mem`, `max_slot_wal_keep_size`, `wal_sender_timeout` |
| MySQL | binlog transaction compression, binlog retention, `binlog_row_image`, `server_id`, `max_allowed_packet` against the largest LOB actually stored, `gtid_mode` / `enforce_gtid_consistency` mismatch |
| Any engine | tables differing only by case; ARRAY/JSON/XML/point columns on key-less tables (DTS fails those outright); other replication already reading the same objects (multi-task conflict); more than 10,000 tables in scope |
| Target | storage needed, WAL included; HA or read replicas on during the load |

**7. What the move will cost (A6, *Not yet*).**

Bytes per table are already known. Add the transfer cost for the network
path the hop uses, the target storage, and the time from the measured
throughput of the chosen path.

## P1: verification the operator can extend

**8. Business-rule and aggregate checks (D7, *Partly*).**

A hop option holding named SQL pairs (`sum(amount) by day`, `count by
status`). Both sides run them and the results are compared through `canon`,
so a cross-engine pair compares values rather than text. DVT's custom-query
validation is the model.

* **Wrap candidate:** DVT (`google-pso-data-validator`). It is built on Ibis
  and reaches Oracle, Teradata, Db2, Snowflake and BigQuery. Its dependency
  set is heavy (the Google Cloud clients and a pinned Ibis). Measure the
  install and its write behaviour first, and use it as an independent
  cross-check for engines migkit does not read natively, the way
  `MIGKIT_CROSSCHECK` uses the second PostgreSQL reader today.

**9. Column subset and column rename in the mapping.**

`mapping` reads only `where` and `tables` today. Add `columns` - keep or
drop, and rename - read by the mover, the check and the repair alike. The
DTS and DMS transformation rules are the model.

**10. Newer-row-wins conflict policy.**

`--on-conflict` has `source-wins` and `keep-target`. Add a hop option for
DTS's `ConditionCover` and pglogical's `last_update_wins`: compare a named
column and keep the newer row. It needs a column both sides maintain; the
refusal says so when there is none.

## P2: reach

**11. Oracle (plan 2).**
* Reader: `python-oracledb` in thin mode (no Instant Client).
* Schema: Ora2Pg's export and its `TEST` / `TEST_DATA` comparisons, wrapped
  and measured.
* DVT as the cross-check.
* Plan 20, PL/SQL conversion with behavioural proof, stays last.

**12. MongoDB to MongoDB with `mongosync` underneath.**

A mover for the only pair where the vendor ships one. MongoDB's
migration-verifier is a cross-check candidate. Measure where it writes its
metadata before trusting it: it must be neither the source nor the target
of the migration.

**13. Kafka offsets across a cutover (E2).**

Compare against MirrorMaker 2's checkpoint translation rather than raw
offsets, which differ by design.

## P2: the plan items still open

* **7:** what the target does to rows as they land
* **10:** documents the table only points at
* **12:** LOBs, correctness then speed
* **13:** a cutover runbook migkit drives - freeze, delta, verify,
  sequences, flip. Items 1-5 above are its parts.
* **14:** a migkit-owned mover, only with the benchmark of item 1
* **18:** continuous verification that costs what changed (delta exists for
  PostgreSQL, MySQL and MongoDB; generalise the cursor contract)
* **19:** bisection diffing across engines on the canonical rendering
* **20:** PL/SQL with behavioural proof

## P2: *Partly* in the problems file, still to close

* **A1** scope
* **A2** privileges down to columns (with **D10**)
* **A4** fork identity
* **B1** type mappings that change values
* **B4** default collation changes
* **B6** values the target refuses
* **C1** speed
* **C3** resume after a crash
* **C4** load on the source
* **D11** documents outside the table
* **E4** change streams vs oplog
* **F1** dual writes
* **F4** poolers (*Not yet*)
* **G2** masking what the drilldown shows

## P3: carried over from the work loop

| Item | Note |
|---|---|
| pgcopydb receives passwords in URIs on its command line | the log is redacted; `ps` is not |
| PostgreSQL index window names indexes without their schema | an index outside `public` cannot be dropped; equal names in two schemas collide |
| `create publication` | not idempotent |
| `follow` | ends at the current position; there is no long-running mode |
| generic engine | does not compare string length or nullability |
| MySQL `_health` | ignores replication lag; `_q_named` exists now |
| hetero | has no deep battery |
| sqlglot | declared and never imported: open it for DDL translation or drop it |
| to measure before wrapping | boto3 Secrets Manager, `mongodump --query/--oplog`, datacompy `all_mismatch()`, mydumper `--regex/--rows` |
| resumable-dump flag | tell the operator when a restart loses the dump's progress (DTS `DumperResumeCtrl`) |
| timed start and auto-retry window | low value next to cron |
| two-way and many-to-one topologies with loop detection | moved up to item 36 |
| waiting on the owner | should dry-run plans hide the command lines they print? |

---

## From the project's memory and the old work queue (added 2026-09-24)

Swept from the notes kept across sessions, then checked against the code.

**Already done, so not listed:**
* float tolerance (`options.float_tolerance`)
* MongoDB skipping `system.buckets` / `system.views`
* the local/S3 state backend wired into the CLI
* parameter comparison (`check_params`)
* MySQL type narrowing and time-shift

### P1

**14. A network path migkit can open itself.**
* **What happened on real legs:**
  * a VPN path black-holed bulk traffic while TCP still opened (an MTU
    problem)
  * a VPN-routed path stalled mid-copy, and the fix that worked was a
    cloud port-forward over the provider's API (SSM)
  * migkit only prints advice about it (`advisors.py`)
* **Deeper:**
  * SSH tunnels and cloud port-forwards as hop options, opened and closed
    by migkit
  * `doctor` telling "TCP opens but bulk stalls" apart from "cannot
    connect"
* This is the operator-side half of what DTS offers as access types.

**15. Rows the target has and the source does not: who wrote them, and
when.**

An open question from a real leg. The deep boundary check flags a target
*ahead* of its source, but cannot say why. The candidates are:
* a double apply of full load plus CDC
* a target that was not emptied
* snapshot rows replayed by CDC
* deletes that never propagated
* writes made on the target

Attribute them where the engine can say:
* PostgreSQL: commit timestamps where `track_commit_timestamp` is on,
  transaction age otherwise
* MongoDB: ObjectId time
* MySQL: the binlog, where it still covers the window

Report each row's age against when the move started.

**16. The side scripts in `tools/` become migkit, or go.**

Twelve standalone scripts sit beside the package. The owner's rule is one
tool, so each either becomes part of a migkit verb or is deleted where
migkit already does its job:
* `check_grants` / `apply_grants`
* `check_users` / `user_sync`
* `check_nopk`
* `check_routines`
* `full_compare` / `full_compare_mongo`
* `spot_check` (index-seek sampling)
* `validate_constraints` (validates NOT VALID constraints: a repair migkit
  lacks)
* `param_diff`
* `gen_changelog`

**17. MySQL events: repaired, not only detected.**

The schema check names a missing event. The schema-fix tool migkit wraps
does not model MySQL events and calls such a pair clean, so the fix DDL
never includes them. migkit writes that DDL itself.

**18. Rehearse the undo (F2).**

The undo written beside every schema fix has only ever run in tests.
Prove it on a scratch copy of the target before anyone relies on it on the
day.

*Also folded into existing items:*
* item 6 gains the `gtid_mode` / `enforce_gtid_consistency` mismatch. A
  target that enforces GTID consistency rejects `CREATE TABLE ... SELECT`
  and temporary tables inside a transaction.
* item 7 gains per-phase, per-engine timings recorded on every run and
  scaled to the production size, which is what a rehearsal is for.

### P2

**19. Canonical rendering for enum, interval, hstore and tsvector.**

Confirm which are already canonical, then add the rest.

**20. The PostgreSQL-only helpers, ported where the idea exists elsewhere.**

`_filtered_tables`, `_extension_data`, `_large_objects`, `_mojibake_repair`.

**21. `setup_target_plan` for MySQL.**

**22. Coverage of `unchanged_since`.** Which checks honour it, and which
still re-read everything.

**23. The time zone the data actually uses.** Compare it with the time zone
each server declares.

**24. G1: a target that is correct and slow.** Close what
`problems-and-what-ends-them.md` G1 leaves open.

**25. Long reads over unstable links.** Sustained MongoDB cursors stalled
over a tunnel; single-command reads did not. Read in bounded chunks that
resume from the last key, on every engine.

### P3

**26. Research not yet done**, for the decision layer's list of what can be
wrapped:
* Bytebase, Airbyte, sqlpipe, schemachange, Trino
* Striim, Qlik Replicate, Fivetran HVR

The question is mechanism, not features.

**27. SQL Server depth.** Closed as untestable on this arm64 machine. Either
find an x86 runner, or list it plainly in section H of
`problems-and-what-ends-them.md` as a limit.

---

## Where the paid tools and the clouds still lead (added 2026-09-24)

From the comparison against GoldenGate + Veridata, Qlik Replicate, Striim,
Fivetran HVR, AWS DMS, Azure, Google DMS and Tencent/Alibaba DTS.

**The owner's rule for this section:** everything they lead on, migkit
does too - researched down to the mechanism, and built smarter and
faster than what it replaces. ETL is out of scope. AI assistance is
*soon*, and never tied to one vendor.

### P1: without these the claim "as good as the paid tools" is not honest

**28. A benchmark anyone can re-run.**

No speed claim holds until this exists. Build a harness on one machine:
* a fixed data generator covering the shapes that matter: keyed and
  key-less tables, wide rows, LOB tables, partitioned tables, a skewed key
* measure three things:
  * bulk copy time
  * change-stream lag at fixed write rates (1k, 10k, 50k tx/s)
  * verification time
* run it against migkit and the open engines it could have used instead

The managed services cannot run on a laptop. For them, publish a recipe
the owner runs on the same instance class, with the cost of the run
stated. Results are published with the hardware and every setting that
produced them.

**29. Change apply that keeps up at very high write rates.**

The paid tools lead here because they apply changes in parallel while
keeping each key's order, and collapse many changes to one row before
writing. Build it in two steps:

1. **The planner sets the native parallel-apply knobs where the target
   has them:**
   * PostgreSQL 16+ `streaming = parallel` on subscriptions
   * MySQL `replica_parallel_workers` with `WRITESET` dependency tracking

   Measure each against item 28's rates.
2. **Where migkit owns the apply (the stream path), a migkit applier:**
   * changes partitioned by table and key hash, so each key stays in order
   * batches that collapse a row's changes before writing (DMS
     `BatchApply` semantics)
   * commit order kept where a table's foreign keys require it

   Lag, throughput and apply errors are reported in migkit's own words.

**30. A control plane.**

HA, resume on another machine, a scheduler, a UI and roles:
* **State, shared:** the local/S3 state backend already exists. Move every
  piece of run state onto it - checkpoints, change-stream cursors, locks.
  Locks become leases with a heartbeat, so a second machine can take over
  a run whose machine died, from the last committed chunk or cursor.
* **Scheduling:** run specifications on a schedule, with phases that are
  safe to repeat.
* **UI:** the read-only dashboard (`report --serve`) grows into an
  operations view.
* **Roles:** viewer, operator, approver. Anything that writes (`--go`,
  `apply`) needs an approval when the hop says so.
* **Audit:** an audit trail of who approved what.
* **Tension to resolve:** the rule against new CLI modes. Keep all of
  this inside existing verbs and hop options, and say so where it is not
  possible.

**31. Scale-out across machines.**

A coordinator hands out tables and chunks from a queue in the shared
store (item 30), and workers lease them. This is DMS Serverless and a
Striim cluster done the open way. The throttle already scales work
*down* when the source strains; add scaling *up* when the source has
headroom.

**32. Alerts, not only metrics.**

Prometheus metrics exist. Add shipped alert rules and webhook
notifications (Slack, Teams, PagerDuty, generic HTTP) for:
* change-stream lag
* WAL or binlog retention filling up
* a verification finding a difference
* a run stalling

This is the part of CloudWatch alarms that migkit can own.

**44. Failure caused on purpose.**

The paid tools earned trust from years of production failures. The open
way to earn it is to cause those failures deliberately, in a harness
anyone can re-run, and publish what happens. Each case, on every engine
0e declares a move or stream for:
* migkit killed mid-dump, mid-load, mid-copy, mid-verify
* the network between source and target cut, then restored
* the source restarted, and the source failing over to a replica during
  a change stream
* the target's disk filling
* DDL on the source mid-stream (with item 5)
* the replication slot dropped, the binlog purged, the change-stream
  resume point expired

Every case ends one of two ways: the run resumes from the last committed
point, or it stops and says in migkit's words what happened and what to
do. Either way, `check` afterwards proves the target. An outcome that is
silently wrong fails the harness.

**45. Proof at size.**

Item 28 measures speed. This measures correctness and resource use at a
size where the small-sandbox answers can change:
* terabyte-class runs on an instance the owner rents, with the time and
  cost stated
* deep verification at zero differences afterwards
* memory staying flat as tables grow (every read streamed, never a whole
  table in memory)
* a real migration, run end to end with its numbers, as the reference
  case; its data stays private

**46. Wrapped programs stay wrapped when they change.**

Every wrapped program has already changed underneath migkit once: flags
missing from the installed build, and a newer build emitting settings an
older server rejects. So:
* CI runs the move and check paths against a matrix of each wrapped
  program's versions
* each program declares the version range migkit supports, and `assess`
  says when the installed one is outside it (`_client_tool_versions`
  exists; extend it to every wrapped program)
* a release that changes behaviour is caught in CI, not by an operator

### P2: reach the paid tools have and migkit does not

**33. Targets that are not databases.**
* **Warehouses:** Snowflake, BigQuery, Redshift, ClickHouse. Load through
  staged Parquet files and each warehouse's own bulk load. Verification
  already reaches these through the second readers.
* **Streams:** Kinesis, Pub/Sub, Event Hubs, alongside Kafka.

Measure a wrap candidate before writing a loader, the rule as always.

**34. More engines to move, not only to verify.**
* **Sources DMS takes that migkit cannot:** Db2, SAP ASE, SQL Server at
  full depth.
* **Targets:** S3 (Parquet), DynamoDB, OpenSearch/Elasticsearch,
  Cassandra.

Order by what a real migration asks for. Every one arrives with its
verification, or it does not arrive.

**35. Change-stream delivery in the formats consumers expect.**

DTS offers five. migkit should offer:
* Avro, JSON, Debezium and Canal-compatible formats, with a schema
  registry (Redpanda ships one)
* topic and partition rules by table, key or column
* skipping oversize messages, with a count of what was skipped

**36. Sync in both directions, and other topologies.**

Promoted from the P3 line below. Covers two-way, many-to-one and
one-to-many:
* loops are prevented by tagging each change with its origin:
  PostgreSQL replication origins, MySQL `server_id` and GTID, the
  stream's own source metadata
* conflicts are detected per key and resolved by policy: newer-row-wins
  (item 10), source-wins, or custom
* every conflict is reported, never silently resolved

SymmetricDS and GoldenGate are the references for the mechanism.

**37. Rollback that loses nothing: live reverse replication.**

At cutover, start a stream from the new target back to the old source,
positioned at the same fence. If the cutover is abandoned, the old side
has every write made since. Verify both directions while it runs. This is
the GoldenGate and Striim failback, with migkit's checks on it.

**38. Accounts and clouds without long-lived passwords.**
* **Passwordless sign-in to managed databases:** assume-role and workload
  identity, RDS IAM authentication tokens, Cloud SQL IAM
* **Secrets:** AWS Secrets Manager, GCP Secret Manager and Azure Key
  Vault next to the existing Vault
* **Cross-account access:** the cross-account role pattern DTS uses,
  done with each cloud's own mechanism

**39. Converting schema and code between engines, wider.**

Ora2Pg covers Oracle. Add:
* SQL Server T-SQL to PostgreSQL
* MySQL routines to PostgreSQL

Use sqlglot where SQL is enough. Every converted object stays behind
migkit's behavioural proof (plan 20): same inputs, same outputs, on both
sides.

**40. Estimates that rest on measurement.**

SCT estimates effort. migkit refused to guess, because it had nothing to
calibrate against. Item 7's per-phase timings from real rehearsals are
that calibration. Estimate time and effort as ranges, and state the runs
each range came from. Without enough runs, say so instead of guessing.

**41. Trust, the open-source way.**

A vendor sells certifications. What an open tool can offer instead:
* signed releases, an SBOM and build provenance (SLSA)
* a written threat model
* an audit log of every write migkit makes, on either side
* reports encrypted at rest
* redaction of values in what is shown and stored (with G2)

**42. Support, without an SLA.**

A troubleshooting guide keyed by every error migkit can raise, each with
its fix. A diagnostics bundle collected by `doctor` with secrets and
values redacted, so a problem can be reported without handing anything
over (an environment variable, not a new flag).

### Soon

**43. AI assistance, any provider.**

One provider interface: any OpenAI-compatible endpoint, Anthropic,
Google, local models. For:
* suggesting conversions (items 39 and 20)
* explaining a finding in plain language
* drafting a fix

What it produces is treated as a proposal: it goes through the same
verification and behavioural proof as anything else, and is never applied
on trust. Off unless a provider is configured.

### Not pursued

* **ETL and transformation**, beyond the mapping migkit already has.

---

## Order of work

0. **0e runs underneath everything that follows:** the declared matrix
   and its test first, then each engine's gaps filled as the items that
   touch them come up, so no item lands for one engine only.
1. **0d, then 0c:** 0d is a verdict that is wrong today on every hop with a
   row filter; 0c is the owner's rule applied to what already exists.
2. **0 and 0a together:** the planner is what makes each later wrap
   worth having.
3. **0b** once the pipeline can be stood up; the source-write question it
   started from is already answered (no).
4. **Then 1-5:** each one closes a way a cutover goes wrong without anyone
   seeing it.
5. **Then 6-10, and 28** (the benchmark) as soon as there is something
   worth measuring.
6. **Then 29-32 and 44-46**, the control plane, scale, and the proof
   that it holds under failure, at size and across versions, before the
   reach items.
7. **Then 33-42** by what the next real migration needs; 43 when the
   rest can check what it proposes.

## Sources

* AWS DMS: [data resync](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.DataResync.html), [resync settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.DataResyncSettings.html), [data validation](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html), [premigration assessments](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.Assessments.html), [PostgreSQL](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.PG.html), [MySQL](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.MySQL.html)
* Azure: [migration service overview](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/overview-migration-service-postgresql), [premigration validations](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-premigration-migration-service), [best practices](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql)
* Google: [DMS source configuration](https://cloud.google.com/database-migration/docs/postgresql-to-alloydb/configure-source-database), [known limitations](https://docs.cloud.google.com/database-migration/docs/postgres/known-limitations), [verify a migration](https://docs.cloud.google.com/database-migration/docs/postgres/verify-migration), [Data Validation Tool](https://github.com/GoogleCloudPlatform/professional-services-data-validator)
* Tencent DTS: API `2021-12-06` models (SDK 3.1.157), [check items](https://cloud.tencent.com/document/product/571/61639), [consistency check](https://www.tencentcloud.com/document/product/571/42724)
* [Veridata: how it works](https://blogs.oracle.com/dataintegration/oracle-goldengate-veridata-how-it-works), [Veridata jobs](https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDUG/working_with_jobs.htm)
* [pgCompare](https://github.com/CrunchyData/pgCompare), [issue #103](https://github.com/CrunchyData/pgCompare/issues/103)
* [Vitess VDiff v2](https://vitess.io/blog/2022-11-22-vdiff-v2/)
* [Debezium incremental snapshots](https://debezium.io/blog/2021/10/07/incremental-snapshots/), [read-only variant](https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/)
* [MongoDB migration-verifier](https://github.com/mongodb-labs/migration-verifier)
