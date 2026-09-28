# Backlog: what is left before migkit finishes the job

**What migkit is to become (the owner, restated 2026-09-28).** A data
migration platform that finishes the job - taken up, run, and the
migration is done - and that does more, deeper and better than the paid
tools, not as much as them. It gets there by wrapping every open-source
tool and library that adds a capability, the way Debezium and DVT are
wrapped already, and using each of them whole: every capability and
feature it has, not one flag. migkit's own logic decides, per task and
per what the task needs, which tools to use, which of their capabilities
to use, and how to combine them - and adds its own logic on top of each
to get more out of it than the tool gives alone. The decision is always
for the best result and the fastest, never at the cost of correctness or
idempotence; the complexity lives in migkit, the operator sees one
platform and migkit's own words. Every tool, paid and open, is researched
to its mechanism first; the open ones are wrapped, the paid ones are
matched and passed. The rules that carry this out: "Paused 2026-09-27"
(how migkit is implemented, and every wrapped tool used whole), 0f (the
used / unused / topped-up table per tool), P0 "Decided 2026-09-27" (the
decision engine that composes a strategy per unit of work) and "the bar"
beneath it.

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

**Decided 2026-09-27 (the owner): one way of deciding for every choice
migkit makes, not one per feature - and not a ladder if a ladder is not
the best way.** The owner's words: find the best way, the smartest and
fastest, one that supports every shape ("a check or a move that cannot
take a table without a key is not acceptable - we make it work"), one
standard that is migkit's own logic, not flat, built for the features
still to come: the fastest move, the most exact and fastest validation,
idempotent, the highest throughput. So the design below is a **decision
engine that composes a strategy from parts**, and a ladder is only what
it looks like when one part has a single axis. migkit already decides in
four places, each in its own way: `movers.pick` (a fixed chain by what is installed),
`planner.plan` (per table, by the hop's rules, no measurement),
PostgreSQL's `_verify_way` (each way tried once, then the cheaper - the
only one that measures), and `loops_prevented` (MySQL only). The two-way
rungs of R3 would have been a fifth. They become one thing, `ladder`,
that every choice goes through, so a rule learned once holds everywhere
and a new choice costs one list of rungs:

* **A rung** names what it gives (the capabilities: `apart`, `exact`,
  `row filter`, `column mapping`, `resume by key`, `consistent as of`,
  ...), what it needs (version, grant, program installed, a server
  setting already on, a key on the table), how it is proved (a probe
  run end to end before it is trusted: the loop mark read back through
  migkit's own reader, a program's `--help` holding the option, a
  100-row copy verified, the digest way timed) and how it is costed (a
  measurement, kept: rows a second, seconds a row, the cost a
  transaction).
* **The climb**, per unit of work - a table, a side, a leg, a batch -
  from the facts (`table_facts`, versions, grants, the network's RTT and
  bandwidth, the checkpoint): drop every rung that lacks something the
  work needs (a counter hop drops `apart`-only rungs; a filtered table
  drops rungs without `row filter`), rank the rest by measured cost,
  prove the top one, fall a rung on a failed proof and say so. The
  footprint (a table made, a slot, a file) is the tie-breaker, never the
  ranking: a rung with a footprint that measures faster and steadier is
  the top rung (the owner's rule, R3).
* **Idempotent and exact by construction:** the choice is written where
  the work's position is written (the checkpoint, the tail's token), so
  a restart climbs the same rung, and a rung that cannot say what it
  applied twice (`exact`) is never given work that must not be applied
  twice. The costs learned live with the hop's rates
  (`planner.record_rate`), so the second run chooses from the first
  run's numbers and the benchmark's, never from a guess.
* **Said in migkit's words:** every choice carries its reason
  (`planner.Decision`), the dry run reads them out per table, `doctor`
  names the rung each side and leg stands on and the footprint it leaves,
  and no program is named.
* **Composed, not picked from a list:** a strategy for a unit of work
  is a combination - how it is read (a program's dump, COPY, a server
  cursor, a relay beside the source), how it is chunked (key ranges by
  quantile, physical ranges by ctid or page, hash buckets of the whole
  row, partitions, a file at a time), how it is written (COPY stage,
  multi-row statements, LOAD DATA, a program's load, native bulk), how
  it is verified as it lands (a digest a range, rows read back, a count
  and a fold), how it resumes (by key, by bucket, by position), how its
  changes are followed and marked. Each part has its candidates with
  what they give, need, prove and cost; the engine keeps only the
  combinations whose parts fit each other and the work's shape, and
  ranks them by measured throughput under the constraint that
  correctness and exactness are never traded. A new part, or a new
  candidate for a part, is one entry - nothing else changes.
* **Every shape has a way, and that is tested:** a table without a key
  or a unique index, a key of many columns or of text, a table too
  large for one range, one with large values, one partitioned, a source
  that is read-only or standby, a user without the grant a program
  needs, a target that must stay read-only, a link too slow for one
  stream. For each shape and each engine the engine must produce at
  least one strategy, and the matrix of shape by engine is a test that
  fails where none exists (the way `capabilities.matrix` fails on a cell
  never built). **Keyless tables in particular:** chunked by hash
  buckets of the whole row computed on the server (`mod(hash(row), n)`,
  `n` sized from the row count), each bucket copied, verified by its
  count and digest, and resumed as a unit; duplicate identical rows
  carried by their counts; on PostgreSQL the physical range (ctid) as
  the faster way where the table is not being rewritten; changes
  applied by the whole row as the key (`REPLICA IDENTITY FULL`,
  `binlog_row_image=FULL`), and where the source cannot give that, said
  before the move, not after.
* **Reusable on purpose:** the same module decides the mover per table,
  the verify way per table, the two-way mark per side, the tunnel legs
  per link, the apply path per batch (COPY stage or statements), the
  read path (relay or direct), and whatever comes next (the stored-code
  converter per routine in R11, the engine cell in R13). One list of
  rungs each; the climb, the proof, the costing, the record and the
  wording are shared.

Measured before it replaces anything: the four existing choices climb
the ladder and reach the same answers on the current suite, then the
speed rules (item 0) become rungs ranked by the benchmark's numbers.

*Built (2026-09-28), `migkit/decide.py`:*
* **A rung** gives capabilities, needs predicates over facts, is proved
  by a probe and is costed in seconds a unit, with its footprint beside
  it. Each fact is read once, and only for a rung still in the running
  (`Facts`).
* **The climb** drops the rungs that lack something, ranks the rest,
  proves the top one and falls a rung saying why. A rung never timed
  goes first, the rest go by measured cost, and a tie within 5% goes to
  the smaller footprint. The reason names only the ways the list
  prefers over the chosen one.
* **A strategy** is composed of parts. The combinations whose parts fit
  each other are ranked by the sum of their parts' measured costs; F1's
  passes A-D are composed that way in a test.
* **Kept beside the position:** the choice goes in a `-ways.json` file
  next to the checkpoint or the tail's token (`choose_kept`), and goes
  with it. Costs are kept for the run (`Costs`) or for the hop beside
  its rates (`HopCosts`).

The four choices go through it with their answers unchanged:
* `movers.pick` and `fitted` climb one ladder (`_ladder`). The rungs
  below the pick are `fitted`'s fallback, so the two cannot disagree.
* `planner.plan` climbs `ways(via)`. The planner's reasons come out word
  for word.
* PostgreSQL's verify way climbs `VERIFY_WAYS` on the run's own costs.
* MySQL's `loops_prevented` is the needs of one rung (`BOTH_WAYS`). The
  answer is the first need missing, and the auto-increment columns are
  counted only once the settings allow two ways.

`decide.coverage()` is the matrix of shape by engine, declared as
`capabilities.GAPS` is and resolved against the code: a way removed turns
its cell stale. Ten shapes by 23 engines: 115 cells with a way, 57 not
applicable, 58 not yet. A `yes` means migkit carries the shape correctly
today. A table without a key, or with a text key, still goes in one pass
or by where its rows are stored, until F1's hash buckets resume it by
bucket. Tests: `tests/test_the_decision_engine.py`.

Still open:
* ranking the mover by the hop's measured rates. It waits on the
  benchmark (28), because a whole run's rate at one size does not carry
  to another, and a database is not moved twice to learn which way is
  faster.
* `doctor` naming each side's rung.
* the two-way marks and the change reader (fast-python 13) as ladders
  through `choose_kept`, by their owners.

**The bar (the owner, 2026-09-27): not "as good as the best of them" but
above every one of them on every axis, on every engine and across
engines - any source, any target, one platform.** What that means, axis
by axis, and the mechanism that gets there; nothing below is claimed
until it is measured against the tool that leads that axis:

* **Telling migkit's own writes apart, any pair of engines.** Every
  other tool does it inside one family (GoldenGate's tags on Oracle,
  origins PostgreSQL to PostgreSQL, server ids MySQL to MySQL). migkit's
  mark is neutral: *the first write in whatever atomic unit the engine
  has* - a transaction (PostgreSQL, MySQL, MongoDB 4.0+, SQL Server,
  Oracle), a logged batch (Cassandra), MULTI/EXEC (Redis, seen whole
  through PSYNC), a transact-write (DynamoDB), a header on every message
  (Kafka, as the Redpanda migrator marks offsets) - with the native tag
  (origin, tagged GTID) in its place wherever one exists and proves.
  So MySQL to MongoDB to PostgreSQL both ways, and a mesh of any
  engines, carries provenance and never loops. Nobody does this across
  engines.
* **Exactly once, every engine, not at least once.** DMS's batch
  apply, Debezium, MirrorMaker 2 (without EOS), mongosync's
  re-application after a restart are at-least-once, safe only where
  every row has a key and every change is its final state. migkit's
  mark carries the batch's number where the engine has no native
  progress (origins, `gtid_executed`), so the target always answers
  "which batch did you last commit" and no batch lands twice - which is
  what counters, keyless rows and append-only targets need. A rung
  without it is never given such work.
* **Verified as it lands, to the row, on both sides of any pair.**
  Veridata and DVT verify after; DMS validates by partitions of rows
  with a lag re-check; pgcopydb compares after. migkit verifies each
  range as it is written (done for the table copier and bulk paths),
  re-checks what was in flight behind an LSN fence (done), and bisects
  to the row across engines by the canonical rendering (`canon`), with
  keyless tables by whole-row hash buckets (above). The target must be
  read-only or its writes named (done for PostgreSQL and MySQL; every
  engine, R13).
* **Faster: the parts, each measured, combined.** A relay beside the
  source reading compressed (done), key-quantile or physical ranges,
  COPY staging or LOAD DATA or native bulk, indexes deferred only where
  measured faster (PostgreSQL yes, MySQL no), parallel index builds,
  workers sized from the host, the servers and the link, throttled by
  the source's own load (done), legs in parallel on a long link (done).
  The bar in numbers, from the research: PeerDB's ~150 MB/s PostgreSQL
  to PostgreSQL on a 1 TB table (source-network-bound), MySQL Shell's
  >200 MB/s load, Alibaba DTS's 180-200k rows/s full load and 11k
  rows/s incremental at the large class, DMS batch apply ~7k changes/s.
  Measured in docker against pgcopydb, MySQL Shell and the builtin
  paths before any speed is claimed (item 28, the benchmark).
* **One platform.** The neutral row, change, type and DDL (`canon`), the
  neutral read and write every engine implements, the pair (`hetero`),
  the capability matrix that fails on a cell not built (R13), and the
  decision engine above them: one hop file, one command, the same
  answer on every engine and pair.

**0f. Every wrapped program and library used whole, smarter, and topped
up (the owner, 2026-09-27).** "We already hold DVT and Debezium inside
migkit. Use every tool and library to the fullest - all of its features,
not one feature and done - but smarter than it is used on its own; where
our own function pulls more out of it, write that; where research finds
another tool or library that adds a capability, take it." Measured on
the code the same day, what migkit asks of what it wraps against what
each offers:

* **DVT** (`data-validation`): used - column validation with `count`,
  `sum`, `min`, `max` and `--filters`, and `connections`. Not used -
  row validation by hash or by concatenated comparison fields
  (`validate row --hash --primary-keys --comparison-fields`), custom
  query validation on both sides, `--threshold` (a tolerance for
  aggregates that drift), `--grouped-columns` (aggregates per group, the
  cheap way to find *where* a sum differs), `bit_xor`, `std`, `avg`,
  random-row sampling (`--use-random-row --random-row-batch-size`),
  `--filter-status fail` (only the failures back), `--exclude-columns`,
  `--trim-whitespace`, `--case-insensitive-match`, `--cast-to-bigint`,
  YAML config files run as a batch, labels, the result handlers. The
  top-up: its grouped aggregates to bisect a differing table to the
  group, then migkit's own row compare on that group only.
* **pgcopydb**: used - `clone`/`copy` with `--dir`, `--no-owner`,
  `--filters`, `--table-jobs`, `--resume`, `--not-consistent`,
  `--snapshot`, `--drop-if-exists`, `compare schema`, `compare data`,
  `list tables`, `follow` with slot, origin, plugin and endpos, the
  sentinel. Not used - `--split-tables-larger-than` (same-table
  concurrency, the thing its author credits its speed to),
  `--index-jobs` and `--restore-jobs` (index and constraint builds in
  parallel), `--large-objects-jobs` / `--skip-large-objects`,
  `--use-copy-binary`, `--skip-vacuum`, `--skip-analyze`,
  `--estimate-table-sizes`, `--skip-split-by-ctid`, `--skip-extensions`
  / `--skip-collations` / `--skip-db-properties`, `--no-role-passwords`,
  `--fail-fast`, `--restart`, `--requirements`, `list progress` and
  `--summary` (its own progress and timing per table for the planner's
  rates), `stream sentinel get` as a lag reading. The top-up: migkit's
  facts (`table_facts`, the largest tables, the link's bandwidth) set
  its jobs and split threshold per run, not defaults.
* **Debezium** (embedded server): used - incremental and blocking
  snapshots through the signal channel, heartbeats, read-only mode,
  `topic.prefix`, tombstones, schema history, `table.include`,
  `snapshot.mode`, `decimal.handling`. Not used -
  `skipped.operations`, `column.mask.hash.*` / `column.truncate.*` /
  `column.include`, `provide.transaction.metadata` (transaction
  boundaries, which migkit's exact batches could ride on),
  `snapshot.select.statement.overrides` (row filters at snapshot),
  `snapshot.locking.mode`, `max.batch.size` / `max.queue.size` /
  `poll.interval.ms` (sized by migkit's sizing, not left default),
  `time.precision.mode` / `binary.handling.mode` (aligned to `canon`),
  `event.processing.failure.handling.mode`, the SMTs (`ExtractNewRecordState`,
  routing, `MaskField`), `pause-snapshot` / `resume-snapshot` /
  `stop-snapshot` signals, `log-signal`, the notification channel
  (snapshot progress as events).
* The same audit is owed for each of the rest - mydumper/myloader
  (`--rows`, `--chunk-filesize`, `--innodb-optimize-keys`, `--trx-tables`,
  `--omit-from-file`, masking functions, `--checksum-all`, `--where`),
  MySQL Shell, pg_dump/pg_restore (`-j`, `--section`), pgloader,
  reladiff (bisection depth, `--threads`, `--bisection-factor`,
  `--materialize`), mongosync (the whole `/start` body: `buildIndexes`,
  `reversible`, `preExistingDestinationData`, `detectRandomId`,
  `verification`, `/reverse`, `/progress` lag fields), mongodump
  (`--oplog`, `--numInsertionWorkersPerCollection`, `--archive`,
  `--nsFrom/--nsTo`), redis-shake (readers, Lua function, filters,
  `status_port`), MirrorMaker 2 (offset syncs, checkpoints, ACL and
  config sync, exactly-once), DSBulk (`schema.splits`, checkpoint and
  replay, `count` modes, `preserveTimestamp`/`preserveTtl`), CDM
  (`trackRun`, `autocorrect`, guardrail), clickhouse-backup (diff, RBAC,
  embedded mode, API callbacks), elasticdump, atlas/liquibase/migra
  (lint, diff, dry run), sqlglot (transpile, optimize, lineage), the
  drivers (psycopg3 pipeline and binary COPY, asyncpg
  `copy_records_to_table`, pymongo raw batches and client bulk_write,
  confluent-kafka transactions, valkey-glide batches). The research
  reports under `docs/research/` list each tool's full surface; the
  audit turns each into a table of used / unused / topped-up, kept like
  `capabilities.matrix`, and a feature not used has a reason written
  (measured slower, unsafe, or not yet).

**0. Choose per table, from measured facts, and combine.**

*Started (2026-09-24):* `migkit/planner.py` makes one decision per table,
each with its reason. The first rules are the ones correctness forces:
* a table the hop excludes is left alone
* a table whose row filter the bulk path cannot apply goes table by table

These decisions used to be made in three places and were never said per
table. The facts come from the source's catalogue in one query per
database (`table_facts`: a row estimate, `null` rather than 0 for a table
never analysed, and whether there is a key; PostgreSQL and MySQL). The
dry run of `move` now reads the plan out, every table off the usual path
first (`tests/test_the_plan_by_table.py`). The second reading is the
planner's first verification rule (0a). Rules that choose on speed are
still to come. They wait for the benchmark (28) to give them measurements
to choose from.

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

*Status (2026-09-24): the declared matrix and its test are done.*
`migkit/capabilities.py` reads what each engine does off the code (its own
methods, the movers, `users.py`'s dispatch) and declares only the gaps;
`tests/test_every_engine_declares_its_gaps.py` fails on an undeclared gap
and on a declared gap the code has closed. Still open: telling the operator
in migkit's words when a command is not yet available, and filling the
gaps below.

The matrix as `capabilities.matrix()` computes it (`-` = not yet, `n/a` =
declared not applicable with the reason):

| capability | pg | mysql | mongo | mssql | redis | kafka | sqlite | parquet | clickhouse | dynamodb | oracle | db2 | ase | opensearch | cassandra | redshift | snowflake | bigquery | kinesis | pubsub | hetero | generic |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| comparing the schema | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| comparing row counts | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| comparing the data itself | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| the deep checks | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | - | yes | yes | - | - | - | n/a | n/a | yes | yes |
| carrying sequences and auto-increment values | yes | yes | n/a | yes | n/a | n/a | yes | n/a | n/a | n/a | - | - | - | n/a | n/a | - | - | - | n/a | n/a | yes | - |
| comparing server settings | yes | yes | yes | yes | yes | yes | yes | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| moving a whole database in bulk | yes | yes | yes | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| copying table by table, resumably | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| keeping the target following the source | yes | yes | yes | yes | yes | yes | n/a | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| proving the target has caught up before cutover | yes | yes | yes | yes | yes | yes | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| telling a difference still arriving from one that is wrong | yes | yes | yes | - | yes | - | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| verifying only what changed | yes | yes | yes | yes | - | yes | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| carrying users and their grants | yes | yes | yes | - | yes | - | n/a | n/a | - | n/a | - | - | - | - | - | - | - | - | n/a | n/a | - | - |
| noticing a move that moved nothing | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| refreshing the target's statistics after a load | yes | yes | n/a | yes | n/a | n/a | yes | n/a | n/a | n/a | yes | - | - | n/a | n/a | - | - | - | n/a | n/a | yes | - |
| snapshotting the target so a cutover can be rolled back | yes | yes | yes | yes | yes | - | yes | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |

The first hand-made version of this table said MongoDB's change stream was
"tail only". It is wired into `move --mode cdc`; the probe read it right.
The probe then said Redis verified deltas: its `delta_verify` did nothing
but return an error. That method is gone, and a method whose whole body
refuses no longer counts as a capability.

Closed since: SQLite copies table by table through the cross-engine copier
(`NeutralCopier`, the one copier for every engine that reads and writes
neutrally), and MongoDB notices a move that moved nothing. Then: MySQL
fences and confirms; Redis carries users (by password hash) and compares
its settings; Kafka and SQLite compare their settings; the cross-engine
bulk path notices an empty move; and a table-by-table move refreshes
statistics on every engine, SQLite included. Redis compares its schema: the
modules loaded on each side, and which kinds of value a sample of keys
holds. The table above is
regenerated from `capabilities.matrix()`.

Closed 2026-09-25:
* MongoDB copies collection by collection, document for document as
  stored, resumed by `_id` through a comparison that orders across types.
  A plain `$gt` would have lost every string `_id` after a batch ending
  on a number.
* Redis copies a keyspace key for key, in the server's own serialised
  form with each key's remaining time to live, resumed from the scan's
  cursor.
* SQL Server copies table by table through the shared copier, and
  refreshes the target's statistics.
* The empty-move guard asks through each side's own reads, so it covers
  every pair and every engine on the shared copier.
* A pair compares the two settings two engines share: what the zone names
  mean, and what the text can hold.
* Restore points: a pair keeps the target engine's own; SQLite keeps the
  whole target file through its online backup; Redis keeps the target's
  key kinds, beside the repair's own undo of every key it replaces.
* Redis replies are decoded so that every byte comes back as sent. Strict
  decoding stopped `check` on the first key that was not UTF-8.
* Kafka copies a topic message for message: each partition's messages go
  into the same partition on the target, with their keys, values, headers
  and times. The times are what a group's position is translated by
  afterwards. The topic is made with the source's partition count and the
  configs that decide what it means, and only committed messages are
  read. The copy was stopped after a batch reached the target and before
  its checkpoint was saved. On resume, that batch was counted off the
  target's own end and not sent twice: 300 messages arrived as 300, in
  order, identical
  (`tests/test_a_topic_copied_message_for_message.py`).
* A pair verifies only what changed (`delta`), from a source whose log
  reads without moving (`tests/test_a_pair_verifies_only_what_changed.py`).
* Redis follows, fences and confirms through its own replication
  (REPLICAOF). The replica signs in to the source as an account that may
  only replicate (`+psync +replconf +ping`), with a password drawn for
  the run; the source's own password never reaches the target. Measured
  on 7.4 first:
  * a replica's first sync empties every database of the target before
    it loads the source's snapshot, and it carries every database and
    key the source has, with no filter. So a hop that excludes keys, or
    whose databases leave out keys on either side, is refused, naming
    the databases. A Redis Cluster is refused too.
  * a 6.2 target of a 7.4 source cannot read the snapshot (`Can't handle
    RDB format version 12`): the link stays down, and the target had
    been emptied already. An older target is refused before anything is
    set up.
  * REPLICAOF answers OK when the replica cannot sign in, and says why
    only in the target's log. The status line waits for the link and,
    when it stays down, reads the source's record of refused sign-ins
    (ACL LOG): "the source refused the replica's sign-in".
  * the fence waits on the replica's offset against the source's, under
    the source's replication id. The check's confirm pass uses it: with
    the link held down while the source changed, the check said the
    difference was still arriving, and a key only the target had was
    still named once the replica caught up.
  * `move --mode full+cdc` on a Redis hop is the replica, as a
    subscription with its copy is PostgreSQL's. A repair beside a
    replica is refused, since a replica takes no writes but its own.
  (`tests/test_a_redis_target_that_replicates_the_source.py`)
* SQL Server keeps a restore point: each identity's current value, which
  a rollback reseeds with `dbcc checkident`, and the definitions of its
  views, functions, procedures and triggers. Found on the way: reading
  rows through the driver had given the engine a second `_q`, which hid
  the one that runs statements through the client, and every native
  check of a SQL Server hop stopped on a `TypeError` before asking the
  server anything. The target was also asked under the source's database
  name, whatever `db_map` said. Both fixed
  (`tests/test_sql_server_native_checks_run.py`).
* SQL Server follows and fences through Change Tracking - the key and
  last operation of each row changed since a version, read as the row is
  now. Measured against Azure SQL Edge (SQL Server's engine, in the build
  that runs on arm64): an insert, an update of it and a delete came back
  as the inserted row with its new value and a delete, and the tail kept
  a mapped database level with the fence passing. A table the database
  does not track, or changes its retention has cleaned up, stop the tail
  by name (`tests/test_sql_server_follows_through_change_tracking.py`).
  Every engine read through a DB-API driver - SQL Server, Oracle, Db2,
  SAP ASE, the warehouses - now applies a change stream: a row updated
  by its key, and inserted where none was, so a change carrying only
  some columns keeps the others.
* The native SQL Server checks, run against a server for the first time,
  had three faults, all fixed (`tests/test_sql_server_checks_against_a_server.py`):
  * the client refused the server's own certificate (`x509: negative
    serial number`, since Go 1.23), so no check ran at all
  * a statement that failed exited 0, with its message read as rows -
    and both sides of a comparison can print the same message
  * a constraint the server named was compared by that name: the same
    primary key was `PK__o__3213E83F720CC3A2` in one database and
    `PK__o__3213E83FA19E373F` in the other, one missing and one extra. It
    is named by its kind, table and columns now
* Kafka follows and fences: the tail is the topic copier, round after
  round, going on from the positions the copy saved, so nothing is sent
  twice; the fence waits on each partition's saved position against the
  source's end. A transaction leaves a marker at a partition's end that
  no reader of committed messages is given, and without taking the
  consumer's own position there the fence waited for an offset that never
  arrived (measured: 82 against 80 messages). A topic copied to nothing
  is named after a move
  (`tests/test_a_kafka_target_that_follows_the_source.py`).

Tests:
* `tests/test_a_collection_copied_document_for_document.py`
* `tests/test_a_keyspace_copied_key_for_key.py`
* `tests/test_a_move_that_moved_nothing_on_any_engine.py`
* `tests/test_the_settings_two_engines_share.py`

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
  * users: ACL users and their rules - *done (2026-09-24)*: `users test`
    compares them from `ACL LIST` (passwords by their SHA-256, never
    read), `create` makes the missing ones with the same password
    through the hash, and `rollback` removes only what it made
  * schema: keyspace types, TTL policy and config parity
* **Kafka:**
  * move and sync: MirrorMaker 2, whose checkpoints translate consumer
    offsets (with item 13)
  * fence: end offsets per partition
  * users: ACLs and SCRAM credentials
  * schemas: topic configuration and the schema registry
* **MongoDB:**
  * a collection-by-collection copier (the builtin path is missing)
  * fence: resume token (the change stream itself is already wired into
    `move --mode cdc`)
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
  * **the cross-engine copier only ever upserts.** Measured SQLite to
    SQLite through it: a row the target held that the source does not
    was still there after the move, and a key-less table went from 2 rows
    to 4 when the move ran again. The SQL copiers empty what they are
    about to replace; this one needs the same, through a neutral
    "empty this table" every engine implements, with foreign keys handled
    the way each engine's own copier already handles them.

**`exclude` on every engine (found 2026-09-24).** Only PostgreSQL, MySQL
and MongoDB read the hop's exclude list. Measured on SQLite: with
`exclude: [audit]`, `check` still reported `audit`, and `repair` deleted a
target-owned row from it. SQLite, Redis (key patterns), Kafka (topics) and
the generic engine now read it; SQL Server still does not, and cannot be
tested on this machine (item 27).

**Transform, in scope here, means migration-time transformation:** rename,
filter, column subset and rename, type conversion and column expressions.
It must work identically through every engine's copy, check and repair.
General-purpose ETL stays out, as the owner set earlier. Whether that
still holds is an open question to the owner.

**0c. Progress and logs in migkit's own words.**

*In progress (2026-09-24):* `migkit/wording.py` holds the vocabulary.
Every bulk path now builds its plan from `Step`s, each carrying the
operator's line and the command that runs, built once; `_sh` writes
command lines to the run's `commands.log` with secrets removed, and a
failing program's message loses the program's name and keeps the
database's words. The static guard now reads `log`, `chat` and `say`
calls too, and knows ten more program names. A failed copy in `move` is a
sentence, not a traceback. The PostgreSQL dump path reports each table as
it is read and loaded (`public.orders: loaded (3 tables)`), taken from
what the programs print as they go. `test_a_real_run_names_no_program.py`
runs `move` for real down every PostgreSQL path, dry run and `--go`, and
reads everything it printed; `--help` of every command is scanned too
(it said "anything reladiff speaks"). The MySQL and MongoDB paths now
report per table too, each read from what the programs print, as measured
on the installed builds:
* **MySQL:** both programs log one JSON object per event. The reader takes
  the dump's `dump_table_progress` and the load's `restore_data_progress`
  by field, never by the wording of a message. A failure is the program's
  own error messages, not the raw tail of a machine log. The load's closing
  error count is checked even when it exits 0
  (`test_the_mysql_bulk_path_speaks.py`).
* **MongoDB:** the load's "finished restoring" line gives what landed and
  what failed for each collection. Measured: a document that failed was
  counted there, and the program still exited 0
  (`test_the_mongo_load_says_what_it_did.py`).

*One vocabulary (2026-09-25):* every bulk path's progress line now counts
against the source's own catalogue, the tables it carries and their
bytes, with the rate and the time left:

    public.orders: read (3 of 12 tables, 1.2 GB of 4.8 GB (25%), 40.0 MB/s, about 1m30s left)

This covers the PostgreSQL dump and restore, the MySQL dump and load,
and the MongoDB load. The table copier says rows the same way, with its
rate taken over the current run's own rows, not rows an earlier run
carried before a restart. Where the catalogue gives no sizes, the line
counts tables and guesses nothing
(`tests/test_one_progress_vocabulary.py`).
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

*Done (2026-09-24), except the logging, which is 0c.*
* **The check:** before, PostgreSQL said `public.orders src=4 dst=2` and
  MySQL `missing=2 ...` `kind=rows-missing` for a correctly filtered move.
  Now every check read goes through one `_scope()` per engine: counts,
  checksums, chunk ranges, drilldown, the moved-nothing guard, and the
  MySQL second reader's `--where`. Target rows *outside* the filter are
  counted and reported separately, so narrowing the comparison hides
  nothing.
* **The copiers:** the PostgreSQL and MySQL table copiers read and replace
  only what the filter selects (MySQL doubles `%` where the statement
  carries parameters).
* **The routing:** instead of refusing the whole database, the PostgreSQL
  bulk paths leave the filtered tables out and `move` hands them to the
  table copier; every other table still goes the fast way. A path that can
  apply no filter at all still refuses, without naming a program.
* `test_the_check_reads_the_row_filter.py`: 12 tests, 8 of which fail on
  the old code. Full suite: 1391 passed.
* **Done for the pair too (2026-09-25).** A hop between engines, and
  SQLite, which copies through the pair, used to refuse a row filter.
  They now apply it:
  * The copier reads through the filter on the source. On the target it
    replaces only what the filter selects there, so the target's own rows
    outside it stay.
  * The comparison reads both sides through the filter and counts the
    target rows outside it separately.
  * The tail asks the source which of the changed rows the filter selects
    now, one read per table per batch. An insert or update outside it
    becomes a delete: a row updated out of the filter leaves the target,
    and one moved in and out within a batch ends up gone. Asking the
    source's current state rather than the change's converges the same
    way a replay does.
  * The filter is SQL for the source. For the target, sqlglot translates
    it into that engine's SQL, with each column under the name the hop's
    mapping gives it. A filter it cannot translate stops the move, the
    tail and that table's check before anything is read. A side that
    speaks no SQL (MongoDB) refuses a filter.
  * The copier written for MySQL to PostgreSQL applies the filter too. It
    used to write a column-mapped table under the source's column names;
    such a table now goes through the copier that reads the mapping.
  (`tests/test_the_pair_honours_the_row_filter.py`: `like 'ap%'` through
  a renamed column, both copiers, the tail.)

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
  `PSO_DV_CONN_HOME` (measured; an earlier note here said `_CONFIG_`), defaulting to one in the user's home. The runner
  points it at a directory of migkit's own for the run and passes
  connections inline.

*Measured (2026-09-24), not yet wired into `check`:*
`tests/test_second_reader_live.py`, against a live PostgreSQL pair: the
catalogue on both sides is identical before and after a count run and a
row-hash run (nothing written); equal data reads `ok`; one value changed on
the target reads `diff`. The first run found the state-directory variable
named wrong in the runner (the reader then made its default directory in
the user's home and looked for connections there); the runner now reads
the name from the reader itself, and a test pins that the home directory
is left alone.

*Runner:*
`migkit/runners/second_reader.py` runs inside the reader's own
environment, takes a job as JSON on stdin and answers JSON on stdout. It
passes a collecting result handler, so nothing is printed and nothing is
written to a database, and keeps connections in a directory made for the
run, owner-only, removed at the end.

*Wired into `check` (2026-09-24), by the planner's first rule:* a
cross-engine hop gets a second reading whenever the reader is installed
and speaks both engines. Every value is converted on the way, and
migkit's own reading was the only one looking. No flag and no option is
involved. `planned_checks()` is how an engine adds a check from what it
can see (`tests/test_second_reading_in_check.py`).

*Measured, MySQL 8 against PostgreSQL 16, the same values on both sides*
(bigint past 2^53, `decimal(12,4)`, empty string, `datetime`, `date`,
`tinyint(1)`/`boolean`, `double`):
* count, sum, min and max of every column: all equal, 3.8 s
* field-by-field comparison by key: all equal, 2.0 s
* **row hash: one false difference.** The row holding the double `-1e-07`
  hashed differently, because the two servers spell it differently before
  hashing. Hashed column by column, only the `double` column disagreed.

So the second reading runs the aggregates and never the row hash across
engines. A second opinion that reports differences which are not there is
worse than none.

Still to measure:
* its speed against migkit's own checksum at size
* where else its type handling disagrees with `canon`: other engines and
  other types (`json`, `time`, `bit`, spatial)

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

*Measured (2026-09-24), MySQL 8 with `gtid_mode=ON`, the connector
image shipped here, the Kafka signal channel, no signalling table
anywhere:* with `read.only=true` on the source connector, an
`execute-snapshot` signal of type `INCREMENTAL` ran. The connector logged
`Requested 'INCREMENTAL' snapshot of data collections '[app.t]'`, then
`will end at position [50]`, then `incremental snapshotting of table
'app.t' finished`. That is the same request the standard incremental
snapshot refused on this pipeline for want of a table in the source. So the
read-only variant needs no write to the source.

*Measured again at 50 rows, and done for MySQL (2026-09-24):* the initial
snapshot put 50 messages on the topic. The incremental re-read added 50
more, next to the one streamed update, for 101. A key changed before its
chunk was read came out as the new value both in the stream and in the
re-read (`49 -> -1 -> -1`). The connector stayed `RUNNING` throughout.
`stream_codegen` now sets `read.only=true` when the source answers
`gtid_mode = ON`, with a single attempt so that a source that cannot be
asked keeps the pause rather than guessing. The repair asks for the
incremental kind wherever the pipeline has it, and says whether the stream
will pause (`tests/test_reading_again_without_pausing.py`).

*Done for PostgreSQL (2026-09-25).* This was measured with the connector
shipped here (3.0.8), PostgreSQL 16, 50 rows, and no signalling table
anywhere:
* Without `read.only`, the incremental re-read was refused, the same way
  as on MySQL.
* With it, the connector read the table again beside the stream (`will
  end at position [50]`, then `finished`), taking its watermarks from the
  server's own snapshot of transactions in flight.
* A row updated before its chunk came out as the new value in the stream
  and in the re-read (`49 -> -1 -> -1`). The connector stayed `RUNNING`,
  and nothing was written to the source.
* The option is missing from the connector's advertised configuration,
  so it was measured, not looked up.

The pipeline now sets it wherever the source runs PostgreSQL 13 or later,
which is where `pg_current_snapshot()` exists. It asks once, and a source
it cannot ask keeps the pause
(`tests/test_reading_again_without_pausing.py`).

Still open: a key changed *inside* a chunk's window. At this size the
chunks finish too fast to land a write in one, and the first try at 3,000
one-row chunks took the sandbox down with it.

*Blocked (2026-09-25), with the reason:* landing a write inside a chunk
needs a table large enough that one chunk takes seconds, beside the full
Kafka Connect stack, and on this machine (a 4 GiB VM shared with other
work) that stack is the one it may not run. The measurement waits for a
machine with room for it; the code path it would exercise is the one
measured above.

## P0: correctness at cutover

**1. Confirm before calling it different.**

*Progress (2026-09-24):* the confirm pass (`fenced_recheck`,
`_resolve_inflight`) lives in the base. Each engine drives it with its own
position, fence and key compare:
* PostgreSQL: the LSN.
* MySQL: the executed GTID set (`tests/test_mysql_fence.py`).
* MongoDB: the source's cluster time, against the position migkit's own
  tail has applied up to. That position is read out of the tail's saved
  resume token, which carries its cluster time after its first byte. Both
  tails now save their position when idle too, so it says how far the
  target is even when nothing is changing. A change the tail had not yet
  applied reads `ok ... still arriving`. A change made on the target alone
  still reads `diff` (`tests/test_mongo_confirms_before_diff.py`).

*Cross-engine hops (2026-09-24), from a MySQL source:* where migkit's own
tail is running, the check waits until the tail has read as far as the
source's log is at that moment. It then walks the differing tables again,
and what converged reads `ok ... still arriving`. A difference made on the
target alone still reads `diff`. With the confirm pass switched off, the
same test reports the in-flight row as a difference.

Found on the way: the MySQL tail's position did not move past what it left
out (other databases, excluded tables), so on a server busy elsewhere it
never reached the log's end, and a fence waiting for that could never pass.
It now moves to the end of what it has read
(`tests/test_cross_engine_confirms_before_diff.py`).

*And from PostgreSQL and MongoDB sources (2026-09-24):* the PostgreSQL
tail reads its slot up to where the log is now, and once it has read that
much whole, its position moves there even with nothing in it for the hop.
It stayed at the last change, so on a quiet database a fence never passed.
The MongoDB stream's own token already moves on with the cluster, and its
cluster time is the fence. Both are measured the same way as MySQL: a
change read and not yet applied reads `ok ... still arriving`, and reads
`diff` with the confirm pass switched off
(`tests/test_cross_engine_confirms_before_diff.py`,
`tests/test_cross_engine_confirms_from_mongo.py`).

Still open:
* SQL Server, which has no change stream in migkit yet (0e).

*Replication migkit does not drive (2026-09-25):* the fence already covers
it wherever the replication shows a position:
* PostgreSQL waits for every active slot on the database, whoever owns it.
  That includes a native subscription, a connector, or a managed service's
  task.
* MySQL waits on a target that is a native replica, through the server's
  own `WAIT_FOR_EXECUTED_GTID_SET` (or `MASTER_GTID_WAIT` on MariaDB).
* A consumer that shows no position on either server falls back to the
  hop's settle time, and the verdict says it settled on time rather than
  on a fence.

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

*Progress (2026-09-24): measured, not yet bounded.*
* `check --consistent` says how long it held the source's snapshot, in its
  verdict and in its progress lines.
* On PostgreSQL, `assess` names the oldest snapshot on the source: its age
  in transactions, how long it has been held, and which session holds it.
  Measured with a report job holding a repeatable-read transaction open:
  the report named `report-job`, and nothing was reported once it closed.
* On MySQL, `assess` reports InnoDB's history list length, the undo the
  source has not yet purged (`tests/test_what_holds_the_source_back.py`).

*Found on the way (2026-09-24):* the consistent pass made its own list of
tables and read each one whole. Every other pass reads through the hop's
exclude list and row filters. So on a target that was exactly what the hop
asked for, a table the target owns and a table under a row filter both
came out different. It reads the same tables and rows as the others now,
in the single script and in the lanes sharing a snapshot
(`tests/test_a_consistent_check_reads_what_the_hop_asks.py`).

*The bound (2026-09-24):* the hop option `snapshot_limit` (seconds) ends a
consistent pass once a side has held its snapshot that long. The snapshot
is released, and the verdict is an error saying so rather than a verdict.
Two things found while bounding it, both measured:
* The lanes that share a snapshot so they can read in parallel were fed
  their scripts one after another, so they read in turn: 8 s where 4 s was
  due. They now run together.
* The source's exported snapshot stayed open until the target's side had
  finished too. It is now released as soon as the source's lanes are done.

(`tests/test_the_snapshot_is_held_for_a_bounded_time.py`)

*Closed by reasoning (2026-09-25):* restarting a table's pass from its
last chunk under a fresh snapshot is not a consistent pass any more. Its
chunks would come from different instants, which is the table-by-table
pass that already exists. That pass reads each table on its own, is
resumable, and is confirmed against the stream's position where there is
one (item 1). The consistent pass keeps its one instant, bounded by
`snapshot_limit`. When the bound is reached it ends with an error rather
than a verdict built from two instants.
* **Today:** `check --consistent` opens one repeatable-read transaction per
  side for the whole pass. On a large table that holds back vacuum
  (PostgreSQL) or purge (InnoDB history list) for as long as the pass takes.
* **Deeper:** a per-table time limit, after which the table's pass restarts
  from its last completed chunk. The chunk checkpoints already exist in
  `checkpoint.py`. `assess`/`doctor` report the source's `backend_xmin` age
  and `innodb_history_list_length` while it runs.

**3. Stop the application writing to the target before cutover.**

*Measured (2026-09-24), PostgreSQL 16, before choosing a mechanism:*
* `ALTER ROLE app SET default_transaction_read_only = on` stops a **new**
  session of `app`: `cannot execute INSERT in a read-only transaction`.
* An `app` session **already connected** when it lands keeps writing. Its
  setting still read `off`, and its insert went in. A pooled application
  would not notice the freeze until its connections were recycled.
* The setting is not a boundary. The application can turn it off for its
  own later transactions.

So the role setting alone is not a freeze. The candidates are:
* revoke the write privileges the hop's roles hold on the target, which
  takes effect on the next statement of every session, with the grants
  recorded first so `rollback` restores exactly those. This does not stop
  a role that owns its tables.
* the role setting plus ending the sessions already connected, which is
  intrusive
* both

*Done for PostgreSQL (2026-09-24), the owner's call: the best, smartest
way that people actually use, decided per role (`migkit/freeze.py`).*
* **On:** the hop option `protect_target: true`. `move --go` freezes before
  it copies anything. Tearing the stream down at cutover
  (`move --mode cdc --drop --go`) gives the writes back, and so does
  `rollback --apply`. `doctor` says which databases are frozen and for
  which roles, from the record, so it can say it even when the target is
  unreachable.
* **Which roles:** `app_roles`, or else the login roles present on both
  sides, which are the application's accounts carried across by
  `migkit users`. Cloud system accounts and migkit's own accounts are
  never included.
* **Per role, from the catalogue:**
  * The write grants the role holds directly are revoked, and recorded
    before anything is changed.
  * If the role can still write afterwards (it owns the table, inherits
    the privilege, or the privilege is granted to everyone), it is also
    made read-only by default in that database, and its open sessions are
    ended.
  * It says which of the two it did, for each role.
* **Thaw** grants back exactly what was revoked, and restores the role's
  own earlier setting if it had one.
* An engine without the freeze refuses before anything is copied
  (`tests/test_the_target_is_kept_from_the_app.py`: a direct grant, an
  inherited one, an owner with a session already open; all stopped, all
  given back).

*MySQL, measured on 8.4 and then done the same way:*
* A table-level revoke stopped a connected session on its next statement.
* A database-level revoke did not. A session that had already chosen the
  database kept inserting, and only new sessions were refused.
* `read_only` stopped the application and left an administrator writing.
  But it is one switch for the whole server, so a per-database hop may
  not reach for it.

So each account's write grants on the database, at both database and
table level, are revoked and recorded. Its open sessions are then ended,
so that the database-level revokes reach them. Privileges an account holds
server-wide, or through a role, cannot be taken back for one database
without taking them everywhere. Those are named and left as they are.
Live test: an account with database-level grants and a session open, one
with table-level grants, one with server-wide privileges, one writing
through a role.
* **Today:** nothing sets it. DTS has `IsDstReadOnly`.
* **Deeper:** read-only for the application's roles, not for the load:
  * PostgreSQL: `ALTER ROLE ... SET default_transaction_read_only`, applied
    only to the roles the hop names, with the undo written first
  * MySQL: `super_read_only` blocks the loader too, so this needs
    measuring before choosing
* Reported in `doctor`, reverted by `rollback`.

**4. A repair that knows the stream is running.**

*Done (2026-09-24) for what migkit drives, and refused beside what it
does not:*
* **migkit's own change tail** (the cross-engine tail and MongoDB's)
  marks itself running (`tail.pid`). It can be asked to pause
  (`migkit/tailctl.py`), and acknowledges only between batches, with
  everything it has read applied and its position saved. A row repair
  pauses it, repairs, and tells it to resume. It then replays from its
  saved position on top of the repair, by key, which is the order that
  converges. The hop option `repair_window` (default 300 s) bounds the
  wait for the pause.
* **Replication migkit does not drive**, meaning a subscription on a
  PostgreSQL target or a running replica applier on MySQL, is named, and
  the repair refuses before writing anything.
* **The generated streaming pipeline** already re-reads the table through
  the stream instead of writing beside it (0b).

`tests/test_repair_beside_a_stream.py`: a tail paused, the repair landed,
and the tail carried a new change afterwards; a real subscription stopped
the repair with nothing applied.

*The subscription migkit set up (2026-09-24):* on PostgreSQL the
subscription `migkit move --mode cdc` creates for this hop is now paused
for a repair, not refused beside it.
* It is first let catch up with the source as it is at that moment,
  through the same fence the confirm pass uses. So nothing the repair is
  about to write is still on its way, and the key conflict (an insert
  queued in the slot for a key the repair has just written) cannot come
  from the stretch being repaired.
* It is then disabled until its apply worker has gone, and enabled again
  after the repair.
* What the source wrote meanwhile arrives by key on top of the repair.

Any other subscription is still refused.
(`tests/test_repair_beside_a_stream.py`: a row lost on the target alone,
repaired while the source inserted one row and updated the repaired one;
both arrived afterwards.)

*The MySQL replica migkit sets up (2026-09-25).* It had no name to tell it
from anyone else's, so every repair beside it stopped. It signs in as
migkit's own replication account, and that account is the name. It is
paused the way the subscription is:
* First it catches up with the source as it is now, by GTID where the
  source runs it and by binary log position otherwise. A change still in
  the relay log, applied after the repair, would put back an older value.
* Then its applier stops while the receiver keeps fetching.
* After the repair it starts again, and what the source wrote meanwhile
  lands on top.

A replica signed in under any other account still stops the repair
(`tests/test_a_repair_pauses_migkits_own_mysql_replica.py`, two MySQL 8.4
servers).
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

*Progress (2026-09-24):* every move path reads the source's column
catalogue before and after itself (`migkit/drift.py`; one query on
PostgreSQL and MySQL, table by table elsewhere) and, when it changed,
stops short of "complete" and says what changed. Tables the hop excludes
are not watched. `tests/test_ddl_during_the_move.py`.

*The tail too (2026-09-24):* a binlog or a logical slot carries rows, not
a DDL the tail could rely on. The tail therefore keeps the source's column
shape beside its saved position, reads it again (one query) before each
batch it would apply, and compares.
* A change stops the tail before that batch. Its position is not moved,
  and it names what changed.
* A DDL made while the tail was stopped is seen the same way.
* A changed shape is accepted once the target has every column the source
  now has on the tables it touched. The tail then carries on from where it
  stopped.
* The working tables of an online schema change (`_x_gho`, `_x_ghc`,
  `_x_del`, `_x_new`, `_x_old`) are not carried, and their appearing is not
  a change. What the swap does to the real table is.

Measured before: a column added mid-tail arrived as an insert naming a
column the target lacked (`UndefinedColumn`), and a ghost table's rows
went to a table the target does not have
(`tests/test_the_tail_stops_at_a_ddl.py`).

*Verdicts across a DDL (2026-09-24):* `check` reads the source's column
catalogue before and after each database's checks, where that is one
query (PostgreSQL, MySQL, SQLite, and the cross-engine hops through their
source). When it changed:
* every schema answer for the database, and every count and data answer
  about the database as a whole or about a table the DDL touched, is taken
  back. Its status becomes `skip`, the record says what changed and what
  it had read, and `verdict.json` is `incomplete` with the scopes under
  `stale`. Answers about the tables it did not touch keep their verdicts.
* the run exits non-zero and says to check again once the change has
  settled; `has_differences` stays false, since no difference was found.
* `check --resume` keeps the shape it ended on, and reuses nothing from a
  run the source's schema has changed under since.

Measured before: a column added between the count pass and the data pass
left `schema main: OK ... identical` in the verdict. On PostgreSQL, where
a clean database is one data record, a column added after the data pass
ended in `all green`. A `--resume` after a column was added reused the
schema check's `OK` (`tests/test_verdicts_a_ddl_overtook.py`).

*MySQL native replication kept to the hop (2026-09-24):* the replica
migkit set up had no filters. Measured on 8.4:
* a write into a database the hop does not name arrived on the target
* a row for a table the hop excludes was applied, and stopped the replica
  on a key the target already had

The plan now scopes the replica with filters in the target's names: the
hop's databases (`REPLICATE_WILD_DO_TABLE`), the renames its `db_map`
makes (`REPLICATE_REWRITE_DB`), and the tables it excludes
(`REPLICATE_IGNORE_TABLE`). With them, neither write arrived, and neither
did a DROP of the excluded table.
* **Restarts:** the filters do not survive one. Measured, the replica came
  back by itself with none, set for the channel or not. So the plan names
  the configuration lines, and the replica's status says `NOT limited to
  this hop` when they are missing.
* **Managed and MariaDB targets:** a managed target is told the parameter
  group settings, and MariaDB is given its own statements.
* **One replica per run:** a hop of several databases now sets up one
  replica, with one password. It used to set one up per database: the
  second failed on a running replica, and gave the replication user a
  password the first no longer had.

(`tests/test_the_native_replica_stays_in_the_hop.py`)

*Which schema changes a native replica applies (measured 2026-09-25, 8.4,
under the plan's own filters):*
* **Same database name on both sides:** every table, view, routine,
  trigger and index statement in the hop's database arrived, including
  one naming its table with the database while another database was in
  use. A table in another database did not arrive, and neither did an
  account.
* **A renamed database:** the replica's rewrite applies to the database
  in use, not to one a statement names. `alter table cx.t add column d`
  and `create table cx.t3`, run with another database in use or none,
  never reached the renamed target. The replica kept running. The next
  row arrived as `7 1 2`, without the value of the column that never
  arrived, and nothing reported an error.

So a hop that renames a database is not given a native replica. `move`
says why and points at migkit's own change tail, which stops on a
schema change rather than carrying half of it. This is the same rule a
hop that maps columns already had
(`tests/test_which_schema_changes_a_native_replica_carries.py`).
DTS-style allow-lists of DDL kinds are left to the tail, since a native
replica filters by database and table, never by the kind of statement.

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

**5b. MySQL to MySQL full+cdc: the copy and the replica start from
different points.**

*Done (2026-09-24) for the tail; native replication still open.*
* **Measured, before:** `move --mode full+cdc --go` copied and printed a
  replica plan, with the binlog position taken before the copy. Nothing
  started the replica. When it was started by hand on 8.4 from that
  position, it stopped on the first row the copy had already carried:
  `Replica_SQL_Running: No`, `Last_SQL_Errno: 1062`. A row written after
  that never arrived. The copy is not a snapshot at any position, so no
  position taken beside it is right for a strict replica.
* **Now:** the same hop runs the change tail that the cross-engine hops
  use, as a pair of MySQL with itself. It starts from a position taken
  before the copy and applies by key, so the stretch the copy and the log
  share converges instead of stopping. Any engine with its own change log
  and an applier gets this. `tests/test_mysql_full_cdc.py` (fails on the
  old code).
* **`--mode cdc` after a separate `--mode full` (2026-09-24):** every full
  copy now records when it ran, and where asking costs the source nothing
  (MySQL, MongoDB) where the log was before it (`copy-position.json`). The
  MySQL bulk copy also records the position its dump is a snapshot at,
  from the dump's own metadata. `--mode cdc` then:
  * starts a native replica exactly at the dump's position, by file and
    position even with GTID on, since the target's GTID set does not hold
    the source's. Measured on 8.4 with GTID on: an insert and an update
    made after the dump arrived, and the replica kept running.
  * starts a change tail from the position taken before the copy, and
    replays by key. A row written between the copy and the tail used to be
    carried by nothing; now it arrives.
  * says plainly, on PostgreSQL, that a subscription made now carries only
    what changes from now on, and points at `--mode full+cdc`.
  * uses a recorded position once, since a second stream from it would
    replay what the first applied.

  `tests/test_cdc_after_a_separate_copy.py` and
  `tests/test_full_cdc_misses_nothing.py`.
* *Closed by reasoning (2026-09-25):* a native replica after a copy that
  was not a snapshot at one position (the table copier's) cannot be
  right. A strict replica stops on the first row the copy already
  carried (measured above: 1062), and no position makes it right. The
  tail replays by key from before the copy and converges, and that is
  what `--mode cdc` gives such a copy.

## P1: before the move - one assessment that answers every managed service's list

**6. Pre-checks with the value to set, not just the verdict.**

*Progress (2026-09-24):*
* **Every engine:** `assess` gives the number of tables in scope, warning
  above 10,000 with the advice to split the hop. It fails on table names
  that differ only by case, since a target that folds case keeps one of
  each pair.
* **PostgreSQL:** `max_replication_slots` and `max_wal_senders` are checked
  against what is in use plus one per database in scope, and the
  number to set is given.
* **MySQL:** each binlog requirement comes with the statement to run. This
  now includes `binlog_row_metadata=FULL`, which the change tail needs, and
  the RDS form of the retention setting. `server_id` is checked, and so is
  `gtid_mode` without `enforce_gtid_consistency` (not on MariaDB, which
  has no `gtid_mode`) (`tests/test_assess_says_what_to_set.py`).

* **Binlog compression (MySQL and MariaDB):** measured, MySQL 8.4 with
  `binlog_transaction_compression = ON` and MariaDB 11 with
  `log_bin_compress = ON`. An insert, an update and a delete went into the
  binlog compressed, and the change reader skipped all three: no change,
  the position not moved, nothing said. A tail on such a source called
  itself caught up for as long as it ran. Now:
  * the tail stops on the first compressed event and says which setting
    wrote it. The position stays where it was.
  * `assess` fails either setting with the statement to run.
  * An application session that turns compression on for itself, which
    `assess` cannot see, stops the tail too.
  * *Read, not stopped on (2026-09-25):* MySQL's compressed transaction
    is now opened by migkit itself (`binlog_payload.py`). The header's
    fields are read up to the end mark, the payload is decompressed with
    zstd, and each event inside is handed to the reader's own parser as
    if it had arrived on its own, with table maps updating its table
    map. Measured on 8.4: an insert, an update and a delete in a session
    that turned compression on, and an insert with the server's own
    setting on, all reached the tail in order with their values, and the
    delta verified the rows. `assess` passes the setting. MariaDB's
    compressed row events are another format, and still stop the tail
    and fail `assess`
    (`tests/test_a_compressed_binlog_is_read_or_stops_the_tail.py`).
* **The largest row against the target's `max_allowed_packet` (MySQL):**
  measured on 8.4. An 8 MiB value loaded into a target with a 4 MiB packet
  stopped the copy with `Lost connection` and nothing else. An 8 MiB value
  of zero bytes stopped a load that a 9 MiB packet carried when the value
  was letters: the loader escapes binary, and a zero byte comes out twice
  as long. Now:
  * `assess` reads the largest row's large columns per table. It skips any
    table whose file on disk is too small to hold a row over the limit,
    and it spends at most `lob_scan_seconds` (default 60) on the whole
    source. Anything it did not reach is `unknown, not clean`. A
    compressed table's file is no bound on its values (measured: 73,728
    bytes on disk for an 8 MiB value in `ROW_FORMAT=COMPRESSED`), so a
    table with row, page or column compression is always read.
  * It fails a row the packet cannot carry and warns on one that fits only
    unescaped. Both give the value to set: twice the row plus 2 MiB, in
    whole MiB. That value carried the zero-byte row through a real move.
  * A move that stops with a lost connection says the likely cause.
  * `tests/test_the_largest_row_fits_the_packet.py`.
* **PostgreSQL key-less tables that replication cannot match:** done
  earlier (`tests/test_rows_a_replica_cannot_match.py`).

* **Other replication writing into the target:** `assess` names a
  subscription on a PostgreSQL target, or a running replica applier on a
  MySQL one, and fails the item: two writers on one table race, and
  whichever lands last survives. DTS fails such a task. On an engine
  whose target cannot be asked, the item is left out rather than passed
  (`tests/test_assess_names_other_writers.py`).

*`logical_decoding_work_mem` (2026-09-25):* a transaction too large for
it is written to the source's disk while it is decoded, and read back.
Measured on 16, for a 3.3 MB transaction:
* at 64kB it spilled 3,460,000 bytes
* at 4MB it spilled nothing
* at 2MB it spilled again

`assess` now reads the server's own count (PostgreSQL 14 and later) and
names the slots that spilled. The value it gives is the next power of two
at least the average spilled transaction, and never less than twice the
current setting (`tests/test_decoding_that_spills_is_named.py`).

*Target storage with the log included (2026-09-25):* the plan's room now
counts the log the target holds at once while it loads, from the target's
own answer. Measured on PostgreSQL 16 with `max_wal_size = 64MB`, loading
a 355 MiB table in one statement:
* it wrote 411 MiB of WAL, and `pg_wal` peaked at 80 MiB, since
  checkpoints recycle the rest
* with one inactive slot on the target, `pg_wal` peaked at 416 MiB: all
  of it

So a PostgreSQL target is counted at 1.25 × `max_wal_size` plus
`wal_keep_size`, or at all of the WAL once it has a slot. A MySQL target
that keeps a binlog holds all of it until `binlog_expire_logs_seconds`
(30 days by default). The "NOT ENOUGH" warning compares against the
tables, the indexes and that log together
(`tests/test_the_room_includes_the_log_the_target_keeps.py`).

Still open: `wal_sender_timeout`, only with a measured reason for a
value.

*Left open on purpose (2026-09-25):* no measurement here has shown a
value that is wrong for a migration. The default (60 s) ends a sender
whose receiver has said nothing for that long, and migkit's own tail
reads through SQL rather than a sender, so it is not one of those
receivers. A recommendation waits for a failure that shows what the
value should be, rather than a number copied from a guide.

*Found on the way, and done (2026-09-24):* a MySQL `move --mode full` onto
a target without the tables stopped at the load on `ERROR 1146 ... doesn't
exist`. The bulk path loads data only, and nothing created what the target
lacked; the cross-engine copier did. The move now
creates exactly the tables the target does not have, from the source's
`SHOW CREATE TABLE`:
* in one session with foreign key checks off, so a child can be created
  before the table it references
* under the sql_mode a schema dump uses, so a definition the source
  accepted is accepted (measured: a zero-date default a strict target
  refuses when typed in plainly)
* the database too when it is missing, in the source's character set and
  collation

A table the target already has is left as it is, and so is one the hop
excludes. The plan says how many it will create
(`tests/test_the_mysql_move_creates_what_the_target_lacks.py`). Views,
routines, triggers and events are still `migkit schema`'s plan.

*Found on the way, and done (2026-09-24):* on PostgreSQL the dump path
called a move onto a target without the tables `bulk copy complete`, with
nothing loaded. Two causes:
* The load's exit 1 was tolerated whenever it ended in `errors ignored on
  restore`. Only a setting the server lacks is tolerated now; any other
  refusal stops the move with the server's words.
* The guard after the move asked only about tables both sides have. It
  now names a source table with rows that the target does not have, on
  PostgreSQL and MySQL (`tests/test_the_restore_says_what_it_refused.py`,
  `tests/test_move_moved_something.py`).

*And then done (2026-09-24), with two more found on the way:*
* **Tables the target lacks.** Both PostgreSQL bulk paths create them
  from the source's definition before the load, and add their keys,
  indexes and constraints after it, which is also the faster order.
  * A target with no tables at all gets every object in the source's
    pre-data (types, functions, sequences, tables, views) except the
    tables the hop excludes.
  * Otherwise only the missing tables are created.
  * The objects are created without owners or grants.
  * What was created is recorded first, so a load that fails still gets
    its keys added on the next run.
* **The streaming copy and foreign keys.** Measured: it empties each
  table it loads by itself, and a target with a foreign key refused with
  `cannot truncate a table referenced in a foreign key constraint`. So
  every schema with a foreign key failed on the default path. Such a
  target now goes through the local copy, and says why.
* **The streaming copy and sequences.** Measured, 52 rows copied: the
  target's sequence stayed at 1, and the first insert failed on
  `duplicate key ... (id)=(1)`. The copy program's own sequence command,
  run alone, read 0 sequences and reset the target's. The engine's
  sequence repair now sets them after the copy.

`tests/test_the_postgres_move_creates_what_the_target_lacks.py`. Still
open: a foreign-key window, dropping and restoring the keys around the
streaming copy the way the index window does. That keeps its speed on such
targets instead of falling back.

*Found on the way, and done (2026-09-24):* the PostgreSQL dump path's load
could only be run by a superuser. As a plain owner, the foreign keys'
system triggers refused the restore's switch, and the child rows were
refused on the key. That was 100 of 100 rows missing, reported as complete.
* A superuser keeps the switch.
* A user allowed `session_replication_role` loads as a replica.
* A user with neither is stopped before anything is emptied, with the
  grant named.

(`tests/test_the_load_keeps_triggers_quiet_without_superuser.py`)

*Found on the way, and done (2026-09-24):* the target's triggers fired for
rows loaded by the PostgreSQL table copier and by both MySQL paths. A
`BEFORE INSERT` stamping `now()` rewrote every row, and the move said
complete.
* PostgreSQL's copier now loads as a replica, as the bulk paths do.
* MySQL, which has no such mode, takes the triggers off for the load with
  their definitions saved first, and puts them back after. It refuses to
  start where it could not put one back as its definer (C6 in the problems
  file).
* The same held for everything else that writes rows onto a target: the
  change tail, the pair copier, and row repairs, in both directions. That
  is now done the same way:
  * PostgreSQL writes as a replica.
  * MySQL takes the triggers off for the tail's lifetime.
  * A per-process record of what was taken off lets `check` name what a
    killed process left off, and the next load puts it back.
  * SIGTERM stops the tail cleanly.
* ~~Left open: the index windows' `dropped-indexes.json` has the same
  crash hole.~~ Done (2026-09-25). The trigger and index windows now share
  one record (`migkit/setaside.py`), with one file per load. A later load
  builds what a dead one dropped and the target still lacks, including on
  a table where it has nothing of its own to drop
  (`test_index_window_pg.py`).

*Found on the way, and done (2026-09-24):* the MySQL bulk load left the
target's binlog without its rows. The loader turns the binlog off for its
own sessions by default. Measured with a replica following the target: it
got the tables and none of their 300,000 rows. A point-in-time restore of
the target would have missed them too. The load now writes the binlog
wherever the target keeps one
(`tests/test_the_targets_replicas_get_the_load.py`).

*Found on the way, and done (2026-09-24):* the one-pass cross-engine
load was chosen for every cross-engine hop once installed. The load file
migkit writes for it reads MySQL into PostgreSQL, the whole database, and
nothing else, so:
* any other pair was read from the wrong server as the wrong kind
* a hop with an exclude list had the target's own tables loaded over
* a table, column or row mapping was ignored

`movers.fitted` now keeps it to the hops it can carry and sends the rest
through the table copier, saying why
(`tests/test_the_one_pass_load_is_fitted_to_the_hop.py`; the load itself
is not installed here, so its own behaviour is not measured).

*Found on the way, and done (2026-09-24):* the MySQL load changed values
to fit instead of stopping on them. Measured:
* `'z'` became `0`, `'12345678901'` became `'12345'`, and `'2026-13-45'`
  became `0000-00-00`
* a source date `2026-00-15` became `0000-00-00` as well
* the table copier cut text to fit on a target whose own mode was lax

Every write to a MySQL target now runs strict, through the dump's session
and every target connection (`MySQLEngine.WRITE_SQL_MODE`). What a lax
source legitimately holds still lands as it is
(`tests/test_the_mysql_load_is_strict.py`).

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

*The time part, from measurement (2026-09-24):* every finished copy
records the rows it carried and the time it took, per path and per hop
(`throughput.json`). The dry run of the next move divides the rows it is
about to carry, from the source's own estimate, by the latest rate
measured on the same path. Before any copy has been measured, it says so
and gives no number (`tests/test_time_from_measured_rates.py`). Transfer
and storage cost are still open.

*Size and room (2026-09-24):* the plan also says how much the move
carries, from the source's catalogue. It gives the tables' and indexes'
size on disk for the tables it carries (not the ones the hop leaves
alone), the room the target needs for them, and what the target's log
grows by while it loads.
* The log figure is the measured rate: PostgreSQL wrote 79 MB of WAL
  loading 78 MB of table and index, and MySQL a 42.8 MB binlog loading
  52 MB of table data.
* Measuring the MySQL figure found the load was not writing the target's
  binlog at all (item 6, and C16c in the problems file).

(`tests/test_the_plan_says_the_size.py`)

*Transfer cost (2026-09-25):* migkit cannot read a price. The hop option
`transfer_price_per_gb` gives one, and the plan then says what carrying
the tables' rows across costs at it. It prices only the rows, because the
indexes are built on the target. Without the option, the plan names no
cost. Still open: the target's free space, which neither engine reports
over SQL.

*Free space (2026-09-25):* the plan now says what the target has free,
where the server reports it. MongoDB does (`dbStats`), and the plan says
`NOT ENOUGH` when the move needs more; measured against `df`, the two
agreed within a few MB. PostgreSQL and MySQL do not report free space,
and the plan does not guess it. A MongoDB move's plan had no size line
at all before; the collections' own statistics now give it one
(`tests/test_the_plan_says_the_targets_free_space.py`).

Bytes per table are already known. Add the transfer cost for the network
path the hop uses, the target storage, and the time from the measured
throughput of the chosen path.

## P1: verification the operator can extend

**8. Business-rule and aggregate checks (D7, *Partly*).**

*Done for the SQL engines (2026-09-24), `migkit/rules.py`:* the hop option
`rules` holds named SQL, either one statement for both sides or a
`{source, target}` pair. `check` runs them whenever the hop has any, with
no flag.
* Each runs on both sides in a transaction that cannot write. A rule that
  tries to write fails as an error, and the source is untouched
  (PostgreSQL's session is read-only, MySQL's transaction is
  `read only`, SQLite's file is opened read-only).
* Answers are compared by value, not text: `1.5000` meets `1.5`, `3` meets
  `3.0`.
* Rows are compared as a set, and a difference names the rows found on
  one side only.
* Cross-engine hops run each side on its own engine
  (`tests/test_business_rules.py`).

*MongoDB (2026-09-25):* its questions are aggregation pipelines, so a rule
there is `{collection, pipeline}`. Each document is a row, with its values
in the order the pipeline writes them and a grouped `_id` spread into its
fields. That is the same shape a SQL `group by` gives on the other side of
a pair, and `1.50` and `1.5000` as Decimal128 still meet.

MongoDB has no transaction that cannot write, so a pipeline holding
`$out` or `$merge` anywhere is refused before it runs. SQL handed to it
says what a MongoDB rule is (`tests/test_business_rules.py`).

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

*Done where columns are paired by name (2026-09-24):* `mapping.columns`
takes, per table, `keep` (only these), `drop` (all but these) and
`rename` (source name to target name), matched by suffix like every other
name in the hop.
* One function (`_mapped_types`) applies it for the cross-engine copier,
  for the table it builds on the target, for the comparison and for the
  schema check. SQLite, whose copies go through the same copier, compares
  its mapped tables through it too.
* A renamed column is read under its source name, and written and compared
  under its target name. A dropped column is neither carried nor reported
  as missing (`tests/test_column_mapping.py`).
* PostgreSQL and MySQL hops copy and hash whole rows on their own paths,
  so there the mapping is refused before anything is copied or compared,
  rather than ignored.

*Done on PostgreSQL's and MySQL's own paths (2026-09-25):* a table whose
columns the hop maps goes through the pair machinery. That machinery
already reads the mapping in one place, so none of it is written a second
time for each engine. Every other table keeps the fast path.
* The plan routes it out of every bulk copy (the dump, the streaming copy
  and the MySQL dump), the same way it routes filtered tables. The pair's
  copier builds it under the mapped names if the target lacks it, and
  fills it.
* `check` compares it through the pair: data, with drilldown, and schema,
  column by column. The whole-database schema comparers leave it out:
  * the dump diffs, with `--exclude-table` and `--ignore-table`
  * the structural differ, whose inspected objects are filtered
  * the object inventory
  * the external schema comparers, through their exclude options, or
    item by item from their report
  The schema-aware comparison's authority no longer demotes the pair's
  findings, because it never read those tables. The test caught this: a
  column dropped from a mapped table was waved through as "cosmetic".
* `sync --kind rows` plans and applies the pair's repairs for it. Writers
  are still paused around the repair, as for any row repair.
* `--mode cdc` follows the hop with the pair's change tail, which now
  applies the mapping to every change. The server's own replication
  carries whole rows under the source's names, so it is refused for such
  a hop, with the reason.
* The tail used to stop on the first change to a mapped table, on
  `Unknown column 'name'`, cross-engine hops included. If the target had
  both columns, it would have written the wrong one.
  (`tests/test_column_mapping_on_own_paths.py`)

Left open:
* ~~The pair's copier builds a missing mapped table from neutral
  classes.~~ Done (2026-09-25). Between two servers of one engine, the
  table is built with every column's type exactly as the source wrote it.
  Through the classes, a `timestamptz` had been built as `timestamp(6)`,
  with its offset gone, and an `int` as `bigint`. It keeps NOT NULL,
  defaults, identity and the source's indexes (see below).
* ~~`watch` and the delta loop still hash whole rows on the own paths.~~
  Done: the delta loop sends a mapped table's comparison to the pair. The
  pair walks the whole table rather than only the keys that changed,
  which is slower and still right. `watch` reads only row counts, so the
  mapping does not change anything there.
* *Found on the way, and done:*
  * The MySQL delta loop read the binlog with the same reader as the tail.
    It said `0 changes since last verified position` over a compressed
    transaction, then moved its position past it. It now stops where it
    is, names the setting, and does not advance.
  * Both delta loops compared the tables the hop excludes. Nothing else
    compares those tables, so they left a difference nothing would clear.
* ~~The own table copier (`MIGKIT_MOVER=builtin`) fills tables and
  creates none.~~ Done: a whole-database copy creates what the target
  lacks, the same way the bulk paths do. On PostgreSQL the keys and
  constraints are added after the rows.
* ~~The whole-database schema comparers do not leave out tables the hop
  excludes.~~ Done: an excluded table is the target's own. Every schema
  comparer now leaves it out, and so does the fix DDL. On a target that
  kept its own differently-shaped `audit`, the check used to report a
  difference nothing could clear, and the fix would have rebuilt the
  table in the source's shape
  (`tests/test_the_schema_check_leaves_what_the_hop_excludes.py`).

*Found on the way, and done (2026-09-25):* passwords on command lines. The
guard written for the MySQL bulk path covered one file. The same mistake
was in five other places:
* the MySQL schema dump took `-p<password>`
* the table sync took `p=<password>` in both connection strings
* the object comparison took both passwords as flags
* the schema comparison took both URLs, passwords inside
* the SQL Server client took `-P`

Each was measured before it was changed, and each now hands the password
over another way:
* the environment: `MYSQL_PWD`, the comparison's own password variables,
  `SQLCMDPASSWORD`
* a private defaults file named by `F=`
* a config file that reads the URLs from the environment

The streaming copy's container took both connection URIs as `-e
NAME=value` on the docker command line. It is now given `-e NAME` only,
and docker takes the value from migkit's environment (measured). The
guard now covers every module and every way a program is started
(`tests/test_no_program_is_handed_a_password.py`). The table sync's plan
also no longer names the program.

*Found on the way, and done (2026-09-25):* sequences behind the rows.
The table copier writes each row with the source's key, and a PostgreSQL
sequence does not move for a key it did not hand out. Measured, through
PostgreSQL's own copier and through the copier between engines: the first
insert after the copy failed on `duplicate key value violates unique
constraint`. The change tail between engines does the same with every row
it inserts, and a cross-engine hop had no sequence check at all. Now:
* Every copy ends by settling the target. Where the source is also
  PostgreSQL, the source's sequence values are carried. From any engine,
  each sequence that owns a column is raised to that column's largest
  value; a sequence is only ever raised, never lowered. The statistics
  are then analysed, as before.
* A cross-engine hop into PostgreSQL has the `autoinc` check. It is asked
  of the target alone and names each sequence that would hand out a key a
  row already holds. `sync --kind sequences` raises them. A target whose
  counter moves with every insert (MySQL) has no such check to run.
(`tests/test_the_target_is_usable_after_the_table_copier.py`)

*Found on the way, and done (2026-09-25):* tables built on another engine
lost their columns' rules. Measured, a MySQL table built on PostgreSQL:
`status varchar(10) not null default 'new'` arrived as `status
varchar(10)`, and `id int auto_increment` arrived as `id integer`. After
cutover, an insert that left out `status` stored NULL, and one that left
out `id` was refused. The cross-engine schema check compared only names
and kinds, so it called the two tables the same. The copier from MySQL to
PostgreSQL built no tables at all, and onto an empty target it stopped.
Now:
* A built table keeps NOT NULL.
* It keeps its defaults. Each is translated into the target's SQL and
  asked of the target first; one the target refuses is named.
* It keeps the engine numbering its key: an identity on PostgreSQL,
  `auto_increment` on MySQL.
* It gets the source's indexes, unique ones included, once its rows are
  in. Before this, a built table had none: every query read it whole, and
  a unique index the source kept was not there to refuse a duplicate. An
  index over an expression, a predicate or a column prefix is named, not
  carried.
* The check reports a lost identity, default, NOT NULL or unique index,
  both ways.
* Every copy between engines builds the missing tables first.
(`tests/test_tables_built_across_engines_keep_their_rules.py`)

*Found on the way, and done (2026-09-25):* orphans behind a validated
foreign key. PostgreSQL's orphan scan read only NOT VALID keys, assuming a
validated key cannot have orphans under it. But every migkit load writes
as a replica or with the triggers off, and a foreign key is a trigger. Row
filters and excluded parents make orphans possible. Measured: under a
filter that kept one parent and both children, the orphaned child sat
behind a validated key, and the check read "all fk constraints validated,
no orphans possible". Every key is now scanned, each within
`fk_scan_seconds`. One that runs out of time is reported as unknown, not
clean. A cross-engine hop scans its target the same way
(`tests/test_orphans_behind_a_validated_key.py`).

`mapping` reads only `where` and `tables` today. Add `columns` - keep or
drop, and rename - read by the mover, the check and the repair alike. The
DTS and DMS transformation rules are the model.

**10. Newer-row-wins conflict policy.**

*Done for PostgreSQL and MySQL (2026-09-24):* the hop option
`newer_wins: <column>` splits the changed rows. Where the target's value
in that column is later than the source's, the target row is kept, and it
is listed in `data-<table>.kept-newer`. Every other changed row is
overwritten from the source, including a row whose column either side has
empty, because the source is the truth when the rows cannot say otherwise.
A column missing from either side refuses the table before anything is
repaired. `--on-conflict keep-target` still keeps everything it was asked
to keep. The source side is read in a read-only session
(`tests/test_the_newer_row_wins.py`).

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

*Written, not yet run against a server (2026-09-25).* Oracle is a side
of a pair through `python-oracledb` in thin mode, with no Instant Client.
It sits on the base every engine read through a DB-API driver shares
(`engines/dbapi.py`, the one SQL Server now uses too). That base covers:
* reads resumed by key, with the row comparison spelled out
* writes that delete by key and insert in one transaction
* digests folded in-process through the shared renderer
* created tables, and read-only rules

What is Oracle's own:
* **Names.** A name in capitals is said in small letters, and one in
  small letters is written in capitals, so `orders` meets `ORDERS`.
* **Values.** A `CHAR` is compared without its padding. A `NUMBER(p, s)`
  is read back at its declared scale, since Oracle keeps no trailing
  zeros (`12.50` is `12.5`), and numbers are fetched as decimals, never
  as floats. A `DATE` is a timestamp, because it holds a time of day. A
  time with a zone is left out rather than rendered without its zone.
* **Carried code.** A PL/SQL function whose body is one `RETURN` is
  carried across the pair through the translator. Anything longer is
  named.
* **Deep check.** Objects a load left invalid on the target, and
  constraints left disabled or not validated.

Held without a server (`tests/test_oracle_as_one_side_of_a_pair.py`):
the values the driver hands back digest as PostgreSQL digests the same
rows in the server, including a padded `CHAR`, a `DATE` with a time and
a `NUMBER(10,2)` that kept no trailing zero. The name folding, the
driver's `:1` parameters and the PL/SQL carrying are covered too.

Oracle Free could not be run here. Measured, it filled the container
VM's disk and took 2.5 GB of its 3.8 GB of memory, and was stopped and
removed. Ora2Pg and DVT are still to wrap, and the change stream
(LogMiner) is still to come.

**12. MongoDB to MongoDB with `mongosync` underneath.**

A mover for the only pair where the vendor ships one. MongoDB's
migration-verifier is a cross-check candidate. Measure where it writes its
metadata before trusting it: it must be neither the source nor the target
of the migration.

*Done (2026-09-25):* measured with mongosync 1.21 between two MongoDB 7.0
replica sets. The build is `mongosync-macos-arm-arm64-1.21.0.zip`, signed
by MongoDB.


*Found on the way (2026-09-25):* the sync program sends telemetry to its
vendor unless told not to, and it wrote its metrics into whatever
directory it was started from. Measured: a `metrics/` directory appeared
in the directory the test ran in. It now runs with telemetry off, its
logs and metrics in the hop's reports, and the reports directory as its
working directory. The test starts it from an empty directory, and that
directory stays empty.
**What it needs, and what it writes.** Started plainly, it refused: it
wanted a user on the source so that it could turn on write blocking
there. Started with `preExistingDestinationData` and the hop's database
as its only namespace, it:
* asked for no user on the source
* wrote nothing to the source: no database or collection appeared there
* kept its own bookkeeping in `__mdb_internal_mongosync` on the target
* carried 3,000 documents, and an insert and an update made while it ran
* kept NumberLong and Decimal128 as they were
* committed

**How migkit runs it.** It is the MongoDB bulk path once installed,
because it copies online and is consistent as of its commit, which a
dump of a live source is not. migkit:
* drops the target's collections in scope first, because it refuses a
  collection that exists, even an empty one
* gives it both addresses in a private configuration file
* lists it while it runs, so a killed move leaves nothing unaccounted for
* stops it after the commit

It gives way to the dump where the hop or the servers cannot take it:
* a database mapped to another name
* a side that is not a replica set
* a server older than 6.0
* a server that cannot be asked

`doctor --install` fetches the build from the vendor for macOS and the
Linux systems the vendor publishes for, and checks its `--version`.

The option reader missed the program's own help shape (`--config value`,
one space), and found 5 of its 15 options. It now reads that shape, and
every other program's count stayed the same.

The tests are in `tests/test_mongodb_moves_online_through_the_sync.py`.

*Found on the way:* the MongoDB dump and load were handed `--uri` with
the password in it. So were the MySQL and generic comparison runs
(addresses), and the PostgreSQL cross-check (URIs). The guard missed all
of these, because it looked at the call and not at the variable the
command line was built in. It now follows variables and helper
functions. The passwords now go in:
* a private `--config` file, for the MongoDB programs
* the comparison run's own file, for the comparisons
* a password file, for the cross-check

(`tests/test_no_program_is_handed_a_password.py`,
`tests/test_the_mongodb_path_keeps_its_password_off_the_command_line.py`)

**13. Kafka offsets across a cutover (E2).**

Compare against MirrorMaker 2's checkpoint translation rather than raw
offsets, which differ by design.

*Done (2026-09-25): offsets translated by the message they point at.* A
committed offset is the next message a group reads. Where the two logs
do not line up, the same number is a different message. Such a partition
used to be named and left alone. Now the message itself is found on the
target:
* first by the time it was written (`offsets_for_times`)
* then by its key and value, scanning at most 1,000 messages forward

The group is compared with, and repaired to, the offset where that
message is. A group at the end of the source's log goes after the
source's last message. Where the message is one of several the same, the
first is taken, so a consumer reads one message again rather than
skipping one. Where the message is not on the target at all, the
partition is still named and left alone.

MirrorMaker 2 translates through its own offset-sync records and only at
the intervals it writes them. This asks the two logs themselves, so it
works for any copy that kept the messages' times: MirrorMaker, a
connector, or migkit's own.

Measured with two clusters:
* the target's partition began with seven messages the source never had,
  then the source's twenty
* a group at 12 on the source was found at 19 on the target, set there,
  and read `message 12` first
* a group whose next message only the source had was refused

(`tests/test_kafka_offsets.py`)

## P2: the plan items still open

* **7:** what the target does to rows as they land
* **10:** documents the table only points at
* **12:** LOBs, correctness then speed
* **13:** a cutover runbook migkit drives - freeze, delta, verify,
  sequences, flip. Items 1-5 above are its parts. *Written (2026-09-25):*
  `docs/cutover.md`, the order of the commands that exist, each with the
  check that has to pass before the next and what to do when it does not.
  Driving it as one command waits on the rule against new CLI modes.
* **14:** a migkit-owned mover, only with the benchmark of item 1
* ~~**18:** continuous verification that costs what changed~~ done for
  pairs (2026-09-25): a pair whose source log reads without moving
  (MySQL, MongoDB) verifies only the rows the log names, asking both
  sides for those keys and comparing them in the one rendering. The
  position moves only on a clean pass. A source read through the tail's
  slot says so (`tests/test_a_pair_verifies_only_what_changed.py`)
* ~~**19:** bisection diffing across engines on the canonical rendering~~
  done (2026-09-25): a table past the walk's 20,000-row cap, keyed by one
  integer, is digested in halves of its key range on both engines, and
  only the halves that disagree are followed down to ranges small enough
  to walk. A text range would depend on each engine's collation, so it is
  left to the walk. Measured, MySQL against PostgreSQL at 60,000 rows: a
  changed, a missing and an extra row past the walk's reach were all
  named, where the walk alone had stopped at 20,000 and said there might
  be more (`tests/test_a_large_table_is_bisected_across_engines.py`)
* **20:** PL/SQL with behavioural proof

## P2: *Partly* in the problems file, still to close

* **A1** scope
* **A2** privileges down to columns (with **D10**)
* ~~**A4** fork identity~~ done (2026-09-25): MariaDB objects MySQL has no home for
* **B1** type mappings that change values
* **B4** default collation changes (the collapse is asked; mixed collations inside stored code are not)
* ~~**B6** values the target refuses~~ done (2026-09-25): zero dates named before a move between engines
* **C1** speed (2026-09-26: tables side by side, integer keys split into
  equal-row ranges - in processes of their own for a pair, whose work per
  row is Python's - the shared copier pipelined and writing PostgreSQL
  through COPY; still to come: a comparison against a managed service)
* **C3** resume after a crash (2026-09-26: each range checked and
  checkpointed on its own, a finished table asked again before it is
  skipped; 2026-09-27: a table with no key resumes by the stored position
  of its rows where the source has one - PostgreSQL - and the streaming
  bulk copy goes on from the tables it finished; still to come: a key-less
  table on a source with no stored position, and a statement-level record
  of a repair)

*The move, faster and checked as it goes (2026-09-26).* The user asked
for a move that is faster, smarter, deeper, as correct as it can be and
safe to run again. What was measured and done, on a sandbox of 300,000
and 1,000,000 rows across four pairs:
* **Found on the way:** the MySQL to PostgreSQL copier wrote a CSV
  itself - bytes landed as their hex digits read as text and every empty
  string as NULL, 200,000 of 200,000 rows different after a move that
  reported success. That copier is gone; the pair goes through the copier
  every pair shares, whose PostgreSQL writer is COPY, and it is faster
  (5.8 s to 4.3 s before read-back). And the PostgreSQL deep checks that
  sample rows as JSON stopped on a table with a column named `s`.
* **Read back as it is written:** see C3 in the problems file. Turned off
  with the hop option `verify_batches: false`.
* **Idempotent:** the one-pass MySQL to PostgreSQL load empties the
  target first as the other bulk paths do (run again, it appended);
  Kafka goes on from the positions it saved; a keyspace is copied again.
* **Done after all (2026-09-27), the four first left out:** the streaming
  PostgreSQL bulk path goes on from the tables it finished (the rest from
  a new snapshot, and the result compared with the source); a table with
  no key resumes by stored position; MySQL is written through a pinned
  `LOAD DATA LOCAL`; and the table copiers set secondary indexes aside -
  see C1 and C3 in the problems file for what each measured.
* **C4** load on the source
* **D11** documents outside the table
* **E4** change streams vs oplog
* **F1** dual writes
* ~~**F4** poolers~~ done (2026-09-25): named in `assess`, and a refused connection setting said as the pooler's
* ~~**G2** masking what the drilldown shows~~ done (2026-09-25): hop option `mask`

## P3: carried over from the work loop

| Item | Note |
|---|---|
| pgcopydb receives passwords in URIs on its command line | done (2026-09-24/25): no password in the local copy's URIs (a password file), and the container's URIs come from the environment through `-e NAME` |
| PostgreSQL index window names indexes without their schema | done (2026-09-24): schema-qualified and quoted |
| `create publication` | done (2026-09-24): made only where it is missing, and a subscription a first run made is left to run - a second `--mode cdc` stopped on `already exists` (`tests/test_bounding_the_subscription.py`) |
| `follow` | ends at the current position; there is no long-running mode |
| generic engine | done (2026-09-25): string length and nullability are read from the same catalogue the comparison library asks, in the standard spelling and then Oracle's; where neither answers, the verdict still says they were not compared (`tests/test_generic_schema.py`) |
| MySQL `_health` | done (2026-09-24): a side that is itself a replica reports how far behind it is, by column name on MySQL and MariaDB, and the throttle backs off on it (`tests/test_mysql_fence.py`) |
| hetero | has no deep battery |
| sqlglot | decided (2026-09-25): kept, for what a type map cannot do. It translates each column default into the target's SQL: `uuid()` becomes `gen_random_uuid()`, `now()` becomes `CURRENT_TIMESTAMP()`. The target is then asked whether it takes the result, and a default it refuses is named rather than guessed at |
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

*Done for PostgreSQL and MongoDB (2026-09-24):* a move now records, as it
begins, what the target says about that moment (`move-began.json`). On
PostgreSQL that is the oldest transaction still running there. The data
check then says, of the rows only the target has:
* on PostgreSQL, how many were written before the move began (the target
  was not emptied of them) and how many after it (the load, a stream
  applying twice, or something writing to the target). Where the target
  keeps commit timestamps, it also gives when they were committed.
* on MongoDB, the same split by the second each ObjectId was made, and how
  many carry ids with no time in them
* that it cannot tell, where no move of migkit's marked the target and it
  keeps no timestamps

(`tests/test_who_wrote_the_rows_only_the_target_has.py`).

*MySQL (2026-09-25):* a move records where the target's binlog was as it
began. For the rows only the target has, the data check reads the
target's own binlog from that point:
* a row written since is there, with the server that wrote it: this
  server's own sessions, or another server arriving through a replica
* a row that is not there was held before the move began, so the target
  was not emptied of it
* where the log from that point is gone, it says it cannot tell

Measured: one row left from before and two written after, each named as
such. The test is `tests/test_who_wrote_the_rows_only_the_mysql_target_has.py`.

**16. The side scripts in `tools/` become migkit, or go.**

*Done (2026-09-24):* every migration script is gone, each because migkit
already does its job.
* **Grants:** the grant checks and repairs cover `check_grants` and
  `apply_grants`.
* **Users:** `migkit users` and `assess` cover `check_users` and
  `user_sync`.
* **Objects:** the object checks cover `check_routines`.
* **Tables with no key:** every table's whole-table checksum covers
  `check_nopk`, key or not.
* **Comparisons:** `check` covers `full_compare`, `full_compare_mongo`,
  `param_diff` and `spot_check`.
* **Constraints:** the constraint repair now validates foreign keys as
  well as checks, and only where the source's own is validated. That
  covers `validate_constraints` (`tests/test_constraint_repair_pg.py`).

`check_no_secrets` and `gen_changelog` stay: they are the repository's
release tooling, not migration scripts.

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

*Done (2026-09-24):* the schema repair (`sync --kind schema`) now creates
the events the target lacks from the source's own `SHOW CREATE EVENT`. It
redefines the ones defined differently; who defined an event and whether
it is on are not counted as differences.
* Each is made in the time zone and sql_mode the source defined it in,
  since both change what it does.
* Each is made **switched off**. An event running on the target while rows
  are still being carried rewrites them under the move, so switching them
  on is left to the cutover, and the inventory says so.
* Statements run one at a time in one session, since an event's body is
  full of semicolons.
* The undo drops what was made and puts back the target's own definition.
* Found on the way: the schema repair handed the target's password to the
  MySQL client on its command line, where any process listing could read
  it. It goes through the environment now.

(`tests/test_mysql_events_are_repaired.py`)

**18. Rehearse the undo (F2).**

The undo written beside every schema fix has only ever run in tests.
Prove it on a scratch copy of the target before anyone relies on it on the
day.

*Done for PostgreSQL (2026-09-24):* `sync --kind schema --apply` first runs
the fix and then its undo on a scratch database on the target's own
server, holding a copy of the target's schema. It compares the schema
with what it was, then drops the scratch database whatever happened.
* A fix that fails there is not applied to the target.
* An undo that does not return the schema exactly is said, with the lines
  in `rehearsal.diff`. Example: a dropped column put back without its
  default and NOT NULL.
* A target where no scratch database can be made says the undo was not
  rehearsed, and carries on as before.

(`tests/test_the_undo_is_rehearsed.py`)

*Done for MySQL (2026-09-25):* `sync --kind schema --apply` already
applies MySQL's fix DDL. It is now rehearsed the same way, on a scratch
database in the target's character set and collation. This matters more
on MySQL than on PostgreSQL, because MySQL's DDL runs outside any
transaction. Before this, a fix whose second statement failed left its
first statement applied on the target.

*Also folded into existing items:*
* item 6 gains the `gtid_mode` / `enforce_gtid_consistency` mismatch. A
  target that enforces GTID consistency rejects `CREATE TABLE ... SELECT`
  and temporary tables inside a transaction.
* item 7 gains per-phase, per-engine timings recorded on every run and
  scaled to the production size, which is what a rehearsal is for.

### P2

**19. Canonical rendering for enum, interval, hstore and tsvector.**

Confirm which are already canonical, then add the rest.

*Enums and domains (2026-09-24):* PostgreSQL's enums and domains are types
of the database's own, and the cross-engine comparison classified columns
by type name. Measured PostgreSQL to MySQL before this: both columns were
left out as types with no rendering. A row whose enum was `sad` on one side
and `ok` on the other came out `ok`, with a footnote. Now an enum compares
as its label, and a domain as the type it is built on, following domains
built on domains (`Engine.canonical_type`,
`tests/test_enums_and_domains_across_engines.py`). MySQL's `enum` and
`set` were already text.

*The same engine on both sides (2026-09-25):* the pair machinery now also
compares same-engine tables: a column mapping on a PostgreSQL or MySQL
hop, and SQLite's own. There, a type with no shared rendering was still
left out, so a changed interval read "every compared column equal". When
both sides are one engine and declare one type, the column is now compared
by that engine's own text of the value. All 37 columns of the wide test
table are compared this way, and a table the pair builds keeps those types
as the source wrote them
(`tests/test_same_engine_types_with_no_shared_rendering.py`).

*Across engines (2026-09-25):* `hstore` and `tsvector` are carried and
compared now:
* **`tsvector` and `tsquery` are compared as text.** A text search vector
  prints its lexemes sorted and once each, so its text is the value
  itself.
* **`hstore` is compared as JSON.** A key/value set is a JSON object of
  strings: the extension casts it to jsonb, and MySQL keeps it as JSON.
  It is read as a mapping now; it came back as its text (`"a"=>"1"`),
  which is not JSON at all on the other side.

Measured PostgreSQL to MySQL: a table carrying both was built (hstore as
JSON, tsvector as text), moved and checked equal. A changed key/value set
and a changed text search vector each read as a difference
(`tests/test_hstore_and_tsvector_across_engines.py`).

`interval` still has no counterpart. MySQL has no type that holds months
and seconds apart, and the driver turns a month into thirty days on the
way. It stays out of the comparison, and the footnote says so.

**20. The PostgreSQL-only helpers, ported where the idea exists elsewhere.**

`_filtered_tables`, `_extension_data`, `_large_objects`, `_mojibake_repair`.

*Done where the idea exists (2026-09-25):*
* `_mojibake_repair` now runs on MySQL too. MySQL's mojibake check had
  always asked for a row-by-row repair, and only PostgreSQL could do it.
  Which values to touch is decided in one shared place
  (`_mojibake_updates`). MySQL reads the rows on the target and applies
  the repair in one transaction. Only the values that re-encode to valid
  UTF-8 change (`cafÃ©` becomes `café`, while a real `café` stays), and
  the undo restores the old values exactly (`tests/test_mysql_text_repair.py`).
* The other three have no counterpart on the engines migkit can run here:
  * MySQL and MongoDB have no row-level security
  * neither keeps data inside extensions
  * MySQL stores large values inline rather than as large objects

  SQL Server has row-level security (security policies). It waits on the
  same hardware as item 27.

**21. `setup_target_plan` for MySQL.**

*Done (2026-09-25):* the old plan loaded the whole schema before the data,
so every secondary index was maintained row by row through the load. It
also created the database as `utf8mb4`, whatever the source used. The new
plan creates the database in the source's own character set and
collation, read from the source; a source it cannot read is said, not
guessed. The rest is said as migkit's commands:
* the move creates the tables the target lacks, sets the secondary indexes
  aside and builds them once after the load (1.41x, measured), and keeps
  the target's triggers off
* routines, views, triggers and events go through `schema --migration`,
  with events carried disabled
* accounts go through `users`

(`tests/test_setup_target_plan_mysql.py`)

**22. Coverage of `unchanged_since`.** Which checks honour it, and which
still re-read everything.

*Answered (2026-09-24):* only PostgreSQL's data pass honours it (version 15
and later, from its shared-memory statistics and `relfilenode`), and its
verdict names the tables it did not read. Every other engine, and
PostgreSQL's consistent pass, re-reads everything. That is the safe side:
no other engine has a marker that has been measured to move on every
change, TRUNCATE included, and a marker that does not is a false negative.

**23. The time zone the data actually uses.** Compare it with the time zone
each server declares.

*Done (2026-09-25):* a new deep check, `time zone in use`, on PostgreSQL
and MySQL (`migkit/zones.py`).

The problem: a column with no zone of its own holds whatever wall-clock
time the application wrote. An application writing Bangkok time into a
server set to UTC is harmless until the move. Then a target column that
has a zone, or a target server in another zone, shifts every value by
seven hours. A count and a checksum over the same text do not see it.

The evidence used is the one kind that does not guess. A column whose
latest value is in the future of the server's UTC clock is written in a
zone at least that far ahead, and the check names the column and the
zone. A latest value in the past proves nothing, and is not read as a
zone. Only columns an index leads are read, so each costs one probe and
never a scan.

Measured with values seven hours ahead on a UTC server: both engines
said "at least UTC+6:30", and a column with no index was not read
(`tests/test_the_time_zone_the_data_is_written_in.py`).

**24. G1: a target that is correct and slow.** Close what
`problems-and-what-ends-them.md` G1 leaves open.

*Done (2026-09-25), decided from what the others do.* DMS and DTS do not
look at this at all. The tools that do, Oracle's SQL Performance Analyzer
and Microsoft's Database Experimentation Assistant, take the source's
real statements, run them on both sides, and compare plans and times.
migkit now does the same for reads (`migkit/workload.py`, in the deep
checks):
* **Which statements.** The source's busiest reads by total time, from
  its own statement statistics: `pg_stat_statements` on PostgreSQL, the
  `performance_schema` digests on MySQL. migkit's own statements are left
  out: on PostgreSQL by user, on MySQL by what its checks contain.
* **Plans.** Each read is planned on both sides. PostgreSQL uses a
  generic plan where parameters remain. A table the target reads whole
  and the source does not is a finding, whatever the timing says.

  Measured on MySQL: `max(k)` over an indexed column is answered before
  execution, and its plan names no table. That counts too.
* **Times.** A read that can run as it is, is run on both sides:
  read-only, under a time limit, once to warm each side, then alternated.
  It is slower only when its median is more than twice the source's and
  more than 20 ms beyond it.
* **Where it stands.** A finding is `warn` and never `diff`, because the
  data is the same. It does not stop `check` unless the hop says
  `performance: gate`.
* **What it shows.** Only the normalised statement, never a sample's
  literal values.

With an index dropped on the target, both engines named it: "reads t
whole on the target where the source uses index t_k". With the index in
place, both said the target answers as well. The tests are in
`tests/test_the_target_answers_the_sources_reads.py` and
`tests/test_the_target_answers_the_sources_reads_mysql.py`.

**25. Long reads over unstable links.** Sustained MongoDB cursors stalled
over a tunnel; single-command reads did not. Read in bounded chunks that
resume from the last key, on every engine.

*MongoDB, and a false negative found on the way (2026-09-24):*
* **The digest** read one cursor over the whole collection. It now reads
  in chunks of 5,000, each its own short query resumed from the last key,
  and retries a chunk that fails on the way. It gives the same answer at
  any chunk size.
* **The chunked read** resumed with `$gt` on `_id`, and a query's `$gt`
  compares within one type only. Measured: seven documents keyed 1, 2,
  2.5, "a", "b" and two ObjectIds, read two at a time, came back as the
  three numbers. So the cross-engine copier and the row walk built on it
  moved and compared three of seven, without a word. It now resumes with
  an expression, which compares in the order the sort uses across types
  and still walks the `_id` index.
* A document keyed null no longer reads as the end of the collection.

(`tests/test_mongo_reads_every_key_type.py`) The SQL engines already read
in key-ordered chunks through `neutral_read`.

### P3

**26. Research not yet done**, for the decision layer's list of what can be
wrapped:
* Bytebase, Airbyte, sqlpipe, schemachange, Trino
* Striim, Qlik Replicate, Fivetran HVR

The question is mechanism, not features.

*Read (2026-09-25):* `docs/research-other-tools.md` covers HVR, Qlik
Replicate, Striim, Airbyte, Bytebase and Trino, from each vendor's own
documentation, and says what migkit took from each:
* Qlik's batch optimized apply is the model backlog 29 followed
* Striim's interval validation, and Bytebase's schema snapshot at each
  change, are ideas to take next

HVR, Qlik, Striim and Bytebase are learned from and not wrapped. They
are commercial or bring a platform of their own. Trino is a candidate
second reader, to measure first. sqlpipe and schemachange are not read
yet.

**27. SQL Server depth.** Closed as untestable on this arm64 machine. Either
find an x86 runner, or list it plainly in section H of
`problems-and-what-ends-them.md` as a limit.

*2026-09-25:* SQL Server is now a side of a pair (item 39): tables,
keys, reads, writes, digests, views and functions. It sits on the
driver base it shares with Oracle and Db2. Every line of it is held
without a server. It still waits for an x86 runner to be run for real.

---

### Audited again 2026-09-27 against every memory note

Every open item in the project's memory was grepped against this file
and the code. Most were already here or already built (the MySQL bulk
path's password off argv, options read from the program's `--help`,
`--omit-from-file`, the truncate with key checks off, `_all_tables`, the
target's database name on the load; users compared and created on every
engine; the publication made idempotently; `system.*` collections left
out; NULL against empty string; consumer-group lag parity; the state
store). What was still nowhere:

* **Partition coverage.** Measured on a DTS leg: the current month's
  partitions (`order_items`, `orders`, `order_status_history`,
  `packages_2026_08`) arrived with 0 rows while the parents' totals
  looked plausible. `check` compares each partition (child table) of a
  partitioned table by its own count and digest, names a partition the
  target has but empty where the source's is not, and a partition the
  target lacks; the same for MySQL partitions (`information_schema
  .partitions`). Folds into A5. *Done (2026-09-28):* measured before on
  both engines, a partition empty on the target, one only the target has
  and one whose rows changed all left the deep check OK, and on MySQL four
  hash partitions against three too (there the counts and data were also
  OK where the rows sat in MAXVALUE, or in fewer hash partitions). Now every leaf partition
  (`pg_inherits`, recursively) and every MySQL partition (`PARTITION (p)`)
  is held to its own count and digest, and named empty, missing, extra,
  stranded in the catch-all, with a different bound, or different in
  content - one wording for both engines (`verdict.partition_differences`),
  every partition listed in `deep-partitions.diff`.
  Test: `test_a_partition_that_arrived_empty_is_named.py`.
* **The slot before the snapshot, proved.** The rule from the RDS legs:
  the change slot is made first and the copy's snapshot exported from
  the slot's own transaction (`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`),
  never a snapshot then a slot - the gap between them is silent and
  unrecoverable, counts do not catch it, only content hashes do. Held by
  a property test: writes interleaved at every point between slot,
  snapshot, copy and tail, and the target still ends equal. Every engine
  with a position (MySQL's binlog position under the same lock as the
  consistent read, Mongo's resume token before the first read) under the
  same test. *Done (2026-09-28):* the property is
  `test_the_position_is_taken_before_the_copy_reads.py`, on a model where a
  change is visible in an order of its own (per row in commit order,
  across rows not - research L2); taken away, the order or the wait loses
  a row. Every full+cdc path: a PostgreSQL hop hands it to the server's
  subscription, whose table sync is the server's own slot and snapshot;
  every tail path (a pair, mapped columns, MySQL to MySQL) starts from
  `copy_point`. PostgreSQL's is now the slot made over the replication
  protocol with its exported snapshot, then a wait until everything that
  snapshot counts as done is visible to a read (the SQL function and a
  wait on the commit log where replication connections are refused).
  Measured on 16: a commit held by a synchronous standby that never
  answers is in the log and out of sight, and the slot's creation waited
  for it both ways; rows written between the slot and the first read,
  between tables and before the tail arrive
  (`test_rows_written_around_the_slot_arrive.py`). MySQL's binlog end was
  *not* such a position - measured on 8.4 with the commit held 1.5 s
  (`binlog_group_commit_sync_delay`): read mid-commit it was past an
  insert no read could see. `copy_point` now waits out the commits under
  way (`waiting for handler commit`), or takes `Binlog_snapshot_*` where
  the server gives it; replaying from the start of the binlog file was
  tried and dropped - measured, it brought back rows from before a `DROP
  DATABASE`. Still open: the wait needs `PROCESS` (without it nothing is
  waited for, as before); MongoDB's resume token and MariaDB's/Percona's
  snapshot position are not measured here.
* **Uniform time shift and type narrowing on MySQL.** #22 of the smart
  check work: the PostgreSQL deep checks for a constant per-row epoch
  delta (a systematic tz bug) and for a target column narrower than the
  source's are to be confirmed as ported to MySQL (the code carries the
  words; the tests must show the MySQL findings by name). *Done
  (2026-09-28):* both engines had both checks, each with its own copy of
  the reasoning; by their code neither named `numeric(12,2)` into
  `numeric(12,4)` (two integer digits fewer), nor PostgreSQL an unbounded
  `numeric` into a bounded one. The reasoning is now one place
  (`verdict.narrowing` over `canon.capacity`, `verdict.uniform_shift`),
  called by both; a shift of two deltas an hour apart is named as a zone
  with daylight saving, and rows changed each by their own amount are
  left to the row comparison. Test:
  `test_a_shifted_or_narrowed_column_is_named_on_every_engine.py`
  (mysql:8.4 and the PostgreSQL pair, the same seeds, the same lines).
* **An online rewrite's working tables.** `drift.transient` knows
  gh-ost's and pt-osc's names; pg_repack's (`repack` schema, `log_*`
  tables and its triggers) and MySQL Shell's are added, so a table being
  rewritten under the tail is neither copied nor reported. *Done
  (2026-09-28):* pg_repack's, Vitess's, Spirit's, Facebook's
  OnlineSchemaChange's, LHM's and the server's own `#sql-` copies, and the
  triggers they put on the table; MySQL Shell's load leaves no working
  table (its view placeholders carry the view's name; the `#sql-` copies
  of the ALTERs that add its deferred indexes are the server's). A name
  an application could choose too (`_archive_old`) counts only beside the
  table it would copy. Left out of the move's table list, the counts, the
  data, the object inventory and the shape watch. Still open: the change
  tail reads gh-ost's and pt-osc's names by the name alone.
  Test: `test_an_online_rewrites_working_tables_are_left_alone.py`.
* **`schema_authority: atlas` needs an alias shape** (4-จ from the old
  queue): how the hop names the authority's own alias for a table the
  hop renames; open, small.
* **reladiff as a rung, not a default:** it has no jsonb rendering and
  breaks on Python 3.14; the decision engine keeps it for the pairs it
  proves on and says why it was not used elsewhere (0f).
* **Waiting on the owner (unchanged):** the PyPI upload and whether the
  name `migkit` is free; a brew tap.

## Decided by the owner 2026-09-28 - all of it is to be built

* **Everything below and in "Research round 2026-09-28" and "Where
  migkit would still lose" is to be done**, in the order F0 first, then
  the rest, better, smarter and deeper than the tools it is measured
  against - never flat.
* **Cutover may write to the source** - only for a hop that turns
  `cutover:` on, only during the cutover, every step reversible, behind
  the approvals that exist. migkit says in `assess`, `doctor` and the
  dry run exactly what it will do on each side and why; a hop without
  `cutover:` is told what the operator must do by hand instead
  (freeze, drain, sequences, jobs, reverse leg) and gets the same
  proofs around it. With it, migkit guarantees the whole path.
* **Licence, decided 2026-09-28 (the owner: "take everything in as a
  package, change migkit's licence to whatever it takes"):** migkit is
  now AGPL-3.0-or-later, so GPL, LGPL, AGPL, Apache and MIT libraries
  are imported and combined freely. Tools that are not open source
  (RIOT-X under BSL, mongosync under MongoDB's customer terms, Liquibase
  5 under FSL, Atlas's default build) are wrapped whole and installed
  from the start, with the operator accepting each one's terms once at
  install time (`doctor --install`, or `MIGKIT_ACCEPT_TERMS` for an
  unattended install) - never accepted silently on their behalf - and
  used always, for every task their terms allow; for a task their terms
  forbid (RIOT-X into anything but Redis's own products, mongosync
  without an Atlas or Enterprise entitlement) the open path does it and
  migkit says why.
* **Every tool worth wrapping is a dependency**, forced: pip packages in
  `pyproject.toml` (mssql-python, python-oracledb, opensearch-py,
  confluent-kafka, and the rest the reports name), programs through
  `doctor --install` without asking again (mydumper, MySQL Shell,
  RedisShake, DSBulk, Ora2Pg, SQLines, plpgsql_check...). (Superseded the same
  day by the licence decision above: copyleft is imported, closed tools
  are installed with the operator's acceptance.)
* **Everything a wrapped tool can do is used**, chosen by migkit's logic:
  mydumper `--rows`/`--checksum-all`/masking, MySQL Shell
  dump/load/copy, MySQL CLONE, `pg_basebackup`, psycopg3 pipeline mode,
  pgcopydb's split and index jobs, SQL Server bulk through mssql-python
  (staged, then promoted in migkit's transaction so a batch stays
  exact), Oracle direct path load (the same), pt-table-checksum only on
  native replicas and always paired with migkit's sum digest.
* **Close the structural four as far as they go:**
  * *Oracle at high rates:* migkit's own LogMiner reader (RDS-capable,
    no licence), mining on a standby where the edition allows it, a
    compiled reader as a rung, OpenLogReplicator driven as a separate
    program where the operator runs it next to the redo; measured
    against the source's redo rate before a hop is accepted, and Oracle
    as a target through direct path loads staged and promoted.
  * *With and without the engine's CDC feature:* where Change Tracking,
    CDC or supplemental logging is on, migkit uses it; where it is off,
    migkit reads what the server lets a reader see without it - SQL
    Server's log through `fn_dblog`/`fn_dump_dblog` and log backups,
    Oracle through LogMiner on archived logs, Db2 through `db2ReadLog`
    - to the depth each proves in the sandbox, and says exactly what is
    missing and what enabling it would buy.
  * *Closed-licence capabilities, rebuilt from open parts:* mongosync's
    job from change streams + raw BSON + the verifier's generations;
    offset-preserving Kafka mirroring approximated by a source-offset
    header and exact consumer-group translation (and Cluster Linking
    driven where licensed); Redis two-way by migkit's marks and
    conflict policies (CRDT-like counters by `delta`); XStream's job by
    LogMiner. Each rebuilt capability is measured against what it
    replaces and better where it can be (verified as it lands).
  * *What only a cloud's control plane can do:* driven through the
    provider's API where it exists (snapshot copy, clone, restore to a
    point, zero-ETL where offered, Azure MI link), and the process
    imitated where it does not (a snapshot restored then fast-forwarded
    by the slot, a mover that scales its own workers).
* **W1-W9 are items to build**, not notes.
* **The machine:** a second colima profile `migkit` (aarch64, 6 GB,
  Rosetta on) for Oracle Free and the x86 engines, started only when a
  test needs it and memory allows, stopped after
  (`tools/with_docker_lock.py --vm migkit`); the default profile and
  the other session's containers are not touched. And free x86 CI on
  GitHub (`ubuntu-latest`) for SQL Server, Db2 and ASE.
* **Leapfrog research, before building these:** stored-code and
  application-SQL conversion; engine reach (mainframe, SAP, Teradata,
  Netezza, warehouses, SaaS with open licences); changing a running
  tail's table set and DDL during a tail. Not to match DTS, DMS,
  Informatica and the converters - to pass them.

## Research round 2026-09-28: what it found, first things first

Sixteen reports in `docs/research/*-2026-09-28.md` (mechanisms of the
clouds, the replication products, the CDC specialists, the DTS and
distributed-SQL toolkits; the paid and open tools with a scorecard each;
security and throughput; diff algorithms; cutover; type fidelity; stored
code; Oracle, SQL Server and Db2; Python's hot paths). Each ends in a gap
table with effort and a docker recipe; this section is the order.

**F0. Wrong answers and lost rows, found by reading the code - before
any feature (each measured in docker first, then fixed with a test that
fails on the old code):**
* MongoDB: `$toHashedIndexKey` makes every number an int64, so 2.3 ->
  2.9 or NumberLong -> Double hash alike; a collection whose `dbHash`
  differs and whose drilldown finds nothing is reported ok. Re-walk with
  the raw-BSON hash. (diff-algorithms)
* `prove_converted` (`engines/hetero.py`) says ok when every routine is
  a procedure it never ran; triggers and events are not checked; one
  failing call fails its whole side without naming the input.
  (stored-code-conversion)
* SQL Server Change Tracking read outside a SNAPSHOT transaction - the
  cleanup can drop changes silently; `rowversion` in the row hash makes
  every copied row differ; CLR types (`geography`) fail the hash query
  so those tables are never compared. (oracle-mssql-db2)
* Types (type-fidelity G1-G4, G8): PostgreSQL `infinity` rendered as
  NULL; BC years render as AD; sub-microsecond digits cut on both sides
  at read so a lossy move digests equal; `extra_float_digits` not pinned
  so a rounded float compares equal; keys distinct on the source that a
  case/accent/pad-insensitive target collation merges, overwritten by
  the MySQL upsert - count the collisions before the move.
* Cutover: the PostgreSQL fence takes `pg_current_wal_lsn()` (a commit
  acknowledged under `synchronous_commit=off` can sit past it:
  `pg_current_wal_insert_lsn()`); `fence_wait` counts only active slots
  (another consumer's caught-up slot can pass the fence while the hop's
  reconnects); `rollback` raises only the target's sequences, so going
  back collides on the old source. (cutover)
* MySQL digest: `sum(crc32)` is linear (a value swapped between rows is
  missed ~2^-16) and the md5 part is 32 bits - widen to 64, salt each
  run. (diff-algorithms)
* mongosync committed on the deprecated `lagTimeSeconds` (absent on
  1.22 -> early commit); Redis restores a relative TTL, drops
  IDLETIME/FREQ, and reads one cluster node only. (oss-nonrelational)
* The PostgreSQL tail through `psql`: a text value holding a newline
  may split the output and stall the tail - measure. (fast-python)
* Small and outward: `pt-table-sync` runs without `--no-version-check`
  (calls home); `doctor` installs Atlas's proprietary build; Liquibase
  5 is FSL; `leftovers.py` misses gh-ost's `_ghk` and the schemas of
  pgcopydb, pglogical, Spock, Bucardo, pg_repack, pgstream; datacompy's
  report names itself to the operator. (oss-relational, WIP of 0f)
* Security defaults: TLS `prefer` / `CERT_NONE` by default; seven
  `hashlib.md5` sites without `usedforsecurity=False`; a publication
  `FOR ALL TABLES`; the view's token in the URL. (security scorecard)

**F1. Verification faster and exact for every key shape
(diff-algorithms):** one scan returns every leaf of the digest tree
(`GROUP BY bucket`, sums add up the tree) instead of `_bisect`'s
log2(d)+2 sequential reads; buckets by a hash of the key so composite,
text and cross-engine keys localize; an in-SQL IBLT (sum cells) sends
~130 KB for 100 differences in 10^8 rows and works without a key; the
self-check that the differences found add up to the change in count and
sum; generations under writes. Passes A-D composed by the decision
engine.

**F2. Cutover as one timed path with automatic undo (cutover):** the
existing `move --mode cdc --drop --go` driven by a `cutover:` hop
option: preflight, reverse stream armed and verified, freeze chosen per
engine and proved, drain to a position taken after the freeze, final
verify of what changed, counters, jobs to one side, flip; any failure or
a blown budget undoes it. Budget: PostgreSQL 2-10 s, MySQL < 3 s behind
a pooler. **Waits on the owner:** freezing the source, a marker row,
toggling jobs and the reverse stream write to the source - allowed only
for a hop that turns `cutover:` on, reversible, behind approvals?

**F3. Stored code (stored-code-conversion, paid-cloud-products):** per
routine the decision engine picks among the operator's own file,
sqlglot, migkit's rules, Ora2Pg and SQLines (GPL/Apache, driven as
programs), then a model loop that gets the failing diff back - ranked by
measured proof pass rate. Proof only by execution in the sandbox on the
same fixture both sides, inputs from constants, real values and
coverage-guided search (Hypothesis), `plpgsql_check` as a gate; ok only
with execution evidence and full coverage; residue in three classes.
sqlglot's limits are listed (no procedural bodies, `CONNECT BY` passed
through, `TRY_CAST` -> `CAST`).

**F4. Types (type-fidelity):** eight new canon classes (uuid, array,
interval, inet, vector, xml, geometry, 9-place timestamp), value-normal
decimals, float4 widened before rendering, infinity marked uncomparable,
JSON folded in-process; the refusals before the move (unsigned 64-bit,
NUL bytes, 4-byte characters, DynamoDB > 38 digits); the type x
engine-pair table in its section 11.

**F5. Engines (oracle-mssql-db2):** Oracle Free runs natively
(`gvenzl/oracle-free:slim-faststart`); LogMiner reader of migkit's own;
SHA-256 summed, never Oracle's `CHECKSUM` (cancels duplicates); direct
path load. SQL Server bulk through `mssql-python` bulk copy (keeps
identity and NULLs; `pymssql`'s cannot keep identity), staged then
promoted inside migkit's transaction. Db2, ASE, Informix need x86.
**Waits on the owner:** a 6 GB colima VM and/or a Rosetta profile;
free x86 CI on GitHub for the public repo.

**F6. Speed of migkit's own paths (fast-python):** the MySQL tail is
probably held by the GIL, not by decoding (decode 8.6 us, whole tail
15.3 us a change, `_ReadAhead` a thread) - the decoder in a child
process may take it from ~65k to ~150k changes/s (measure R-B first);
one persistent connection for the PostgreSQL tail instead of three
`psql` processes a batch; rendering is 80% of the fold - specialise it
per column before any compiled renderer. Not now: free-threaded 3.14t,
subinterpreters, PyPy for all of migkit, orjson or XXH3 in the digest.

*Done (2026-09-28), measured on a laptop shared with other runs, 320,000
changes queued (200,000 inserts, an update of every other row, a delete
of every tenth, a thousand rows a transaction; `bench/tail_rates.py`):*
* **The MySQL tail was held by the interpreter's lock (R-B).** MySQL 8.4
  to PostgreSQL 16, before: the binlog decoded alone 4.6s (3.9s of CPU),
  the batches applied alone 5.5s (4.0s of CPU), the whole tail 7.7s on
  7.7s of CPU - one core, and the CPU the sum of the two parts.
* **The apply, trimmed.** A key's identity without a sort where the key
  is one column, a row's shape worked out once for each order its names
  come in (`base._ident`, `_net_rows`, `_apply_net`): applied alone 5.1s
  to 4.3s. The garbage collector's passes held to what the batches leave
  while a tail runs (`hetero._batch_gc`: what was there before frozen, a
  young pass every 50,000 new objects): 4.3s to 2.7s.
* **The decoder in a process of its own** (`hetero._ReadProcess`, on the
  range workers' process protocol): the same library and code, the
  records pickled across (1.1us a change there, 0.8us back). The whole
  tail 5.6s with the reader in a thread and 4.9s in a process (41,600,
  57,400 and 66,000 changes a second, before, thread and process; a
  second run 6.2s and 4.9s, the measured choice again the process). The
  reader is now the limit: decoded alone 4.1s against 2.7s applied. Which
  way a tail reads is measured, not set (`hetero._Reader`): `python-thread`
  and `python-process` each timed on the tail's own full batches, the
  cheaper kept (a tie to the thread), the ranking by seconds a change
  written beside the position (`tail-read.json`) for the decision engine
  to climb; a process needs two processors and a hop that can be handed
  over, and one that stops is started again from the position asked, then
  left for the thread. The process decodes the window the thread decodes,
  batch for batch and position for position, over every MySQL column kind
  and a transaction larger than a batch; a two-way counter's batch
  committed before its position was saved is not applied again
  (`test_the_binlog_is_read_in_a_process_as_in_a_thread.py`,
  `test_the_tail_reads_on_the_way_measured_faster.py`).
* **The PostgreSQL tail on one connection (R-C).** Before: four client
  program starts a batch (43 ms each), the server's decoding of all
  320,000 0.47s, parsing them here 1.7s, the whole PostgreSQL to
  PostgreSQL tail 16.1s. On one kept connection, the slot moved only when
  the position comes back, a connection lost between batches opened again
  and the statement asked once more: 7.3s. The stall the report suspected
  was real: a text value holding a line break stopped the tail on
  `unterminated value` at every try (and a carriage return came back a
  newline); read as the two columns they are, every line break arrives
  whole (`test_the_postgres_tail_reads_on_one_connection.py`, failing
  before).
* **The fold's renderer chosen once a column** (`render.renderer`, which
  sends every value it does not write straight to `canon.render_value`):
  200,000 rows of eight columns 0.58s to 0.39s, timestamps 0.58s to
  0.31s; held to `render_value` byte for byte over generated values of
  every class, and every straight path broken on purpose was caught
  (`test_a_column_renders_as_canon_renders.py`).
* Next where this leaves it: the decoder is the ceiling of a MySQL tail
  now, so a compiled decoder (R19 lever 9) is the lever that pays; the
  PostgreSQL tail's next is streaming `pgoutput`.

**F7. The rest, by report:** each report's gap table is the item list
for its area - pgcopydb split and index jobs, mydumper `--rows` /
`--checksum-all`, MySQL Shell, CLONE and `pg_basebackup` rungs
(oss-relational); RedisShake, MM2 with a source-offset header, DSBulk,
ClickHouse `remote()` with dedup tokens, OpenSearch RFS, DynamoDB
export/import (oss-nonrelational); changing a running tail's table set,
pausing only the table a DDL touched, the Kafka consumer-offset clamp,
a scheduled tail that stops when caught up (paid-cloud-products); the
source-commit timestamp in the beat, batches ended at the last COMMIT,
changed-columns merge (goldengate-qlik-hvr); statistics-based chunk
edges, exact batches by default on one-way tails, events decoded under
the schema as of their position (dts-tidb-vitess); warehouse loads with
the Storage Write API and Snowpipe channels as exact paths
(aws-azure-google-snowflake); the 16 security fixes (security scorecard).
Correction carried: AWS ended DMS Fleet Advisor on 2026-05-20.

### Leapfrog: conversion (docs/research/leapfrog-conversion-2026-09-28.md)

The paid converters cannot close the gap with more rules: none has the
source engine and the data as an oracle, and migkit does. So conversion
is a search - several candidates per object, the one kept that passes a
two-sided execution proof. Pieces P1-P21 in the report; the order:
P2 a counterexample bank (every counterexample re-run on every
candidate: model repair loops level off after 2-5 rounds and bring old
failures back), P18 the harness's own mutation score (the proof must be
shown able to fail), P10 a cross-engine `pt-upgrade` on `workload.py`
(the real workload - MySQL `QUERY_SAMPLE_TEXT`, Query Store, Oracle bind
capture - translated, replayed on both engines, agreement weighted by
frequency; ast-grep + sqlglot for code paths the log missed); then
emulation as one more candidate proved like any other (orafce,
IvorySQL, Babelfish, MariaDB Oracle mode; openHalo allowed now that
migkit is AGPL; the AWS extension packs have no public licence and stay
out), inputs from the real call history, constants, coverage-guided
search and z3 for branches never taken, failures shrunk on both sides
to a minimal repro, constraints proved to accept and reject the same
rows on both engines, a collation agreement matrix from real values,
index advice on the translated workload (HypoPG/Dexter), and rules
learned from accepted fixes adopted only with zero regressions.
Measures no vendor publishes: share of objects proved, compatibility
weighted by workload, the harness's mutation score, repro size, objects
fixed per manual fix, dependence on emulation. In progress with the
stored-code agent (P2, P18, emulation, shrinking, z3, learned rules);
P10 queued as its own item.

### Leapfrog: engine reach (docs/research/leapfrog-engine-reach-2026-09-28.md)

Today: Db2 is LUW only; warehouse sides write rows with no staged bulk
load, no exactly-once write and no server-side digest; nothing for
Teradata, Netezza, Vertica, Exasol, HANA, Databricks/Iceberg,
mainframe, IBM i, SAP application data, Cosmos NoSQL or SaaS. In order:
1. **Exact warehouse loads + one SHA-256 row digest in each engine's
   SQL** (item 33): BigQuery committed streams with row offsets (tested
   on goccy/bigquery-emulator - the free sandbox blocks DML and
   streaming), Snowpipe Streaming channels with offset tokens
   (`snowpipe-streaming`, Apache-2.0), Redshift COPY with its load
   commits, Delta `txn` through delta-rs, Iceberg snapshot properties;
   the batch number carried so a replay never lands twice.
2. **The copybook engine + IBM i:** COBOL copybooks and EBCDIC/packed
   and zoned decimals (Cobrix Apache-2.0 through a JVM, Stingray MIT,
   `ebcdic` incl. Thai cp838) for VSAM/flat-file extracts, testable on
   arm64 with no mainframe; IBM i through IBM's ODBC driver (arm64
   native) / Mapepire / jt400, changes followed through
   `QSYS2.DISPLAY_JOURNAL`, `HASH_ROW` as the digest. Db2 for z/OS
   through `ibm_db` needs the operator's Db2 Connect licence (installed
   with acceptance); no open log reader exists for Db2 z/OS, IMS or
   CICS/VSAM changes - said.
3. **Teradata** (teradatasql, FastExport/FastLoad through TPT where
   installed, HASHROW for digests; ClearScape or the Vantage Express VM
   for tests).
4. **One change-topic reader** for engines that publish changes as a
   documented stream (TiDB, Couchbase, Aurora DSQL, CockroachDB) - one
   reader in the cross-engine tail covers all four.
5. **Cosmos DB NoSQL** (vNext emulator runs on arm64; deletes need
   "all versions and deletes" mode, which needs continuous backup).
6. **SAP application data** only through ODP over OData (`pyodata`,
   Apache-2.0) with migkit's own delta tokens - ODP over RFC is
   forbidden to non-SAP tools (Note 3255746) and direct database reads
   break SAP's runtime licences; PyRFC is archived.
7. **SaaS:** dlt and its verified sources (Apache-2.0) as dependencies;
   Stitch's Singer taps (AGPL - importable now); Airbyte's certified
   connectors (ELv2) installed with acceptance and run as programs;
   Salesforce change capture through its Pub/Sub API (72 h retention).
Licence changes noted: CockroachDB and ScyllaDB are no longer open
source, Couchbase server is BSL, Greenplum closed (forks Cloudberry,
WarehousePG), MariaDB Xpand discontinued.

### Leapfrog: a live tail's table set, raw logs, closed capabilities rebuilt (docs/research/leapfrog-live-tail-and-raw-logs-2026-09-28.md)

26 items with an order in the report (start: L7, L2, L8, R1, R7).
* **L1-L8, the table set and DDL of a running tail.** None of the eight
  tools does it cleanly (DMS, Qlik, TiDB DM and Debezium stop the task;
  Vitess cannot add tables; Informatica restarts every subtask; DTS
  refuses past 10 minutes of lag). migkit: one stream, a state per
  table; a table added gets its own snapshot and joins at an exact seam
  while the others keep applying; only the table a DDL touched is
  parked (its changes spooled to disk so the source's log is not held)
  and additive DDL is applied at its log position. **L2, correctness:**
  on PostgreSQL snapshot visibility does not follow commit-LSN order,
  so "apply every commit after an LSN" can lose a transaction - the
  seam is decided by xid visibility against the range's
  `pg_current_snapshot()` (sent to the lever-8 and slot-before-snapshot
  work). **L7:** every batch, per table, changes read = applied +
  dropped as superseded + spooled - the check that would have caught
  TiDB DM dropping rows of added tables (tiflow #12859) and PostgreSQL
  before 17.5 losing changes on `ALTER PUBLICATION ADD TABLE`.
* **R1-R9, with and without the engine's change feature:** depths
  D0-D4 printed by `assess` per table, with the statement that would
  raise each. R7: lift MySQL's refusal of `MINIMAL`/`NOBLOB`/
  `PARTIAL_JSON` row images - exact by re-reading the row at the change
  (small). R9: PostgreSQL at `wal_level=replica` reaches convergent
  change reading through `pg_walinspect`/`pg_waldump` on the WAL archive
  (walminer 4 is paid). R2: SQL Server without CT/CDC - restore the
  source's log-backup chain onto a SQL Server migkit owns and read the
  log there: no privilege and no setting on the source (`sp_replcmds`
  needs a published database - not a way round). R4/R5: Oracle without
  supplemental logging - flashback reads (`AS OF SCN` by ROWID) turn
  partial updates into full images; NOLOGGING operations detected.
  Db2 without `DATA CAPTURE CHANGES` reaches only D1 (D2 on
  undocumented layouts) - said.
* **C1-C6, closed capabilities rebuilt from open parts:** C1 mongosync's
  job from 4.0+ sources to any destination, consistent at any moment,
  the unique-index conversion off the cutover path, and an audit of the
  users holding `bypassWriteBlockingMode` (the `restore` role) whose
  writes pass the block; C2 Kafka offsets kept identical from a client
  for gap-free partitions (pad the empty target partition, then
  `DeleteRecords`), C3 otherwise an offset map written in the same
  transaction as the data - exact, against MM2's error of up to
  `offset.lag.max` plus re-delivery; C4 Redis/Valkey two-site
  active-active rules (lists and streams refused); C5 XStream's
  downstream capture as LogMiner on an Oracle Free mining instance
  migkit owns; C6 RIOT-X's live mode made lossless by a resuming PSYNC
  client or an `OBJECT IDLETIME` sweep under `CLIENT NO-TOUCH`.
* **P1-P3, cloud processes without an API:** a mover that scales on the
  source's own stress and scales down at a range boundary (DMS
  Serverless waits 60 minutes); seeding from ZFS/LVM/EBS snapshots
  moving only changed blocks, then fast-forwarded; a zero-ETL-like
  continuous hop for any source and warehouse, keyless tables and
  additive DDL without a resync.

## Where migkit would still lose with every item above done (2026-09-28)

Asked by the owner: once the whole backlog is built, where do the
world's paid tools still win, on every factor (not "no users yet").
Structural first - limits that doing the backlog does not remove - then
the gaps nothing above covers yet, now items of their own.

**Structural (mitigated, not closed):**
* **Oracle at high change rates, and Oracle as a target.** GoldenGate
  captures and applies inside the kernel (integrated Extract and
  Replicat, XStream, RAC threads, ASM, TDE-encrypted redo, downstream
  capture on a standby so the source does no mining). migkit's paths are
  LogMiner (slower, type limits, no continuous mine after 19c) or
  OpenLogReplicator run as a separate program (GPL: driven, never
  bundled). Mitigation: mine on a standby where LogMiner allows it, the
  compiled reader as a rung, measured against the redo rate before a
  hop is accepted.
* **Reading the raw transaction log with no CDC feature on the source.**
  Qlik, SharePlex and GoldenGate parse SQL Server, Oracle and Db2 log
  formats themselves; migkit uses the engine's own change features
  (Change Tracking / CDC, LogMiner, SQL replication), which a DBA must
  turn on - migkit never changes a setting. Mitigation: `assess` names
  exactly what to enable and its cost; a raw-log reader is not planned.
* **Capabilities behind a closed licence:** mongosync (free only with
  Atlas or Enterprise Advanced), RIOT-X (BSL), Kafka byte-level mirroring
  with offsets kept (Cluster Linking, Shadowing; impossible from a
  client), Redis Enterprise Active-Active CRDTs, XStream. Used where the
  operator holds the licence; migkit's own path is the rung otherwise,
  and says what it cannot give (e.g. offsets translated, not preserved).
* **What only a cloud's control plane can do:** storage-level clones and
  zero-ETL seeding, serverless capacity for the mover, Azure's MI link.
  migkit drives the provider's API where one exists (R19 lever 1) and
  cannot where none does.

**Gaps not yet in any item (added now):**
* **W1. The mover itself highly available and scaled out.** A CDC leg
  that runs for months must not stop with one machine: a standby that
  takes over within seconds from the shared position (the lease exists,
  `test_another_machine_takes_the_run_over.py`), and one table's ranges
  spread across machines with a conditional-write checkpoint (R5, looked
  at and left). Paid peers: GoldenGate HA, Striim clusters, DMS Multi-AZ.
* **W2. The estate, not the hop.** Discovery of every database on a
  network or account, target sizing from the source's performance
  history (Azure's SKU recommendations, Fleet Advisor), cost of the
  target, and hundreds of migrations run and watched as one fleet; a
  REST API, a Terraform provider and a Kubernetes operator over it; SSO
  (OIDC/SAML) and SCIM on the view, not only a proxy's header.
* **W3. Code conversion at enterprise breadth.** Beyond R11: PL/SQL
  packages with the emulation libraries they need (orafce and the
  like), T-SQL at depth, and the SQL embedded in application code
  (AWS SCT scans Java, C# and .NET) found and converted, with the same
  proof by execution.
* **W4. Transformation in flight and masking for compliance.** Joins,
  enrichment and windows on the stream (Striim, Informatica, Matillion),
  and format-preserving encryption / tokenization (Informatica, Delphix)
  beside the keyed HMAC of R5 - as hop rules under the same verify, which
  then compares the transformed source with the target.
* **W5. Sources the paid tools read and migkit does not:** mainframe
  and legacy (Db2 for z/OS and IBM i, IMS, VSAM with COBOL copybooks,
  EBCDIC and packed decimals), Teradata, Netezza, SAP (HANA, and SAP
  application tables through its own extractors), Informix, Progress;
  and SaaS applications, where the open connectors are ELv2 or AGPL and
  cannot be wrapped - to be decided by what the next migration needs.
* **W6. Types only commercial engines have:** Oracle SDO_GEOMETRY,
  XMLType, object types and nested tables; SQL Server hierarchyid,
  sql_variant, FILESTREAM; Db2 DECFLOAT and GRAPHIC - mapped, carried,
  rendered for the digest, or refused before the move (the type-fidelity
  research, 2026-09-28).
* **W7. Resharding.** One database split across N targets by key, N
  merged into one with DDL coordinated (TiDB DM's shard merge), and a
  load that follows the target's own distribution (Citus, Vitess,
  CockroachDB, Spanner).
* **W8. Installed where nothing can be downloaded.** An offline bundle
  (every wheel and every wrapped program, checksummed and signed), a
  container image, and Windows; GoldenGate runs on every platform
  including z/OS and AIX.
* **W9. Compiled end to end at the very top of the rate.** R19 lever 9
  compiles the decoder; above ~100k changes a second sustained the apply
  side (collapse, render, write) needs the same, measured first.

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

*The harness (2026-09-25):* `bench/run.py` does the following on one
machine:
* starts a disposable PostgreSQL or MySQL pair
* builds the six shapes (keyed, key-less, wide, LOB, skewed, partitioned)
  inside the source
* times each move path and the check
* times the same copy through the open programs directly
* with `--cdc-rate`, measures the change tail's lag at a fixed rate

It writes the numbers with the hardware and versions to
`reports/bench/`, and `docs/scale.md` has the first run. Still open: the
10k and 50k tx/s rates, which this laptop VM cannot produce, and the
recipe for the managed services.

*Blocked (2026-09-25), with the reason:* 10k and 50k transactions a
second need a source and a target on hardware that can sustain them, and
this machine's 2-CPU VM tops out far below. `migkit bench --cdc-rate`
takes the rate as it is; the run waits for that hardware, and its
numbers go into `docs/scale.md` with the machine they came from.

**29. Change apply that keeps up at very high write rates.**

*First step (2026-09-25), found by the harness:* every change the tail
applied opened a connection of its own and committed on it. MySQL into
MySQL at 190 rows a second was still 15 seconds behind when the writer
stopped after ten. A batch is now one transaction on one connection, and
the same run ends 2.9 seconds behind. A batch that fails is rolled back
whole and replayed (`tests/test_changes_apply_as_one_batch.py`).

*Rows collapsed in a batch (2026-09-25):* a row changed several times in
one batch is written once, with what the batch left it as:
* the later change's values override the earlier one's
* a change that carries only some columns (an update written without an
  unchanged large value) is merged rather than blanking the others
* a row made and removed within the batch is not written at all
* a moved key leaves its old address and arrives at the new one, in the
  same transaction

The rows keep the order in which they were first touched, so a parent
made before its child is still written before it.

*A run of rows in one statement (2026-09-25):* rows next to each other in
a batch, in the same table and with the same columns, are written by one
statement of up to 1,000 rows on PostgreSQL and MySQL. Other engines
still write one statement per row. Measured on PostgreSQL 16 with 20,000
upserts: 3.9 seconds one row at a time, 0.06 seconds in runs.

Only neighbouring rows are joined, so the order is kept. A key seen again
starts a new statement, because PostgreSQL refuses one statement that
writes the same row twice.

On MySQL, a row made of nothing but its key used to be a plain insert.
Replaying it was error 1062, and a tail that replays a failed batch
stopped on that error every time. It is now left as it is.

The benchmark then found the tail's reader was the limit. It looked up
the table's key on a new connection for every event, and 472 one-row
transactions a second left it 38 seconds behind. It now ends 0.73
seconds behind, and 0.13 seconds behind at 1,030 a second, which is the
fastest the writer reached (`docs/scale.md`). The tests are in
`tests/test_runs_of_rows_apply_as_one_statement.py`.

Parallel apply partitioned by key (step 2 below) is not built. Measured
here, the tail keeps up with the fastest writer this machine can run, so
there is no rate available to show that it is needed.

*Large transactions streamed to the subscription (2026-09-25):* measured
on PostgreSQL 16, with the source's `logical_decoding_work_mem` at 64kB
and 20,000 rows written in one transaction. The subscription migkit made
spilled 3,460,000 bytes to the source's disk and sent them at commit.
With `streaming = parallel`, nothing spilled, the same bytes were
streamed, and all the rows arrived.

The subscription now carries the option, based on the server versions
read at planning time:
* `parallel` from 16 on the target
* `on` from 14 on the target
* nothing where either side is older than 14, or where either side
  cannot be asked

A source older than 14 cannot send a transaction before it commits.
The test is `tests/test_large_transactions_stream_to_the_subscription.py`.

*More than one applier on the MySQL-family replica (2026-09-25):*
measured with 100,000 one-row transactions queued on the replica before
its applier started.

| Target | Setting | One applier | Four appliers |
| --- | --- | --- | --- |
| MySQL 8.4 | `replica_parallel_workers` | 28.8s, 55.3s, 51.0s | 14.2s, 16.4s, 27.4s |
| MariaDB 11.8 | `slave_parallel_threads` | 7.6s to 9.1s | 4.7s to 5.2s |

The same rows arrived in every run. On MariaDB, "one applier" means
`slave_parallel_threads` 0, which is the default.

The plan now sets four appliers on a target that has only one. It leaves
the target alone in three cases:
* a MySQL target that does not keep the source's commit order
  (`replica_preserve_commit_order`)
* a MariaDB target with a replica running, where the setting cannot be
  changed
* a target that cannot be asked

The note names the configuration line that keeps the setting after a
restart. A managed target is told to set the parameter instead.

*MariaDB replica on a target with its own log (2026-09-25):* measured on
11.8. The plan started the replica at `MASTER_USE_GTID = current_pos`. On
a target that keeps a binary log, that position includes the target's own
writes, such as the tables made for the copy (`0-52-3`). The replica asked
the source for that position and stopped with error 1236. It now starts
at `slave_pos`, which holds only what was replicated. On a target with no
log, `slave_pos` is the same position as before. The test for both
changes is `tests/test_the_replica_applies_in_parallel.py`, with a binary
log on both sides.

The paid tools lead here because they apply changes in parallel while
keeping each key's order, and collapse many changes to one row before
writing. Build it in two steps:

1. **The planner sets the native parallel-apply knobs where the target
   has them:**
   * PostgreSQL 16+ `streaming = parallel` on subscriptions (done, above)
   * MySQL `replica_parallel_workers` with `WRITESET` dependency tracking
     (done, above; 8.4 always tracks by write set and has no setting for
     it; an 8.0 source's `binlog_transaction_dependency_tracking` is the
     source's setting, which migkit does not change)

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

*First step (2026-09-25): the write lock is a lease.* It was a file
holding a process number. Only a process on the same machine could ask
whether that number still ran, and a holder that died left the file
saying it did. Now:
* the holder renews a lease while it runs, every third of its term
  (`MIGKIT_LEASE_SECONDS`, 60 by default)
* a lease not renewed for its whole term is taken over, with a line
  naming the holder that let it lapse
* a holder on the same machine whose process is gone is taken over at
  once
* a holder that was taken over stops renewing, and does not remove the
  new holder's lease when it finishes
* reads and writes of the lease are made under an exclusive lock, so two
  processes deciding at once cannot both win

The tests are in `tests/test_a_hops_write_lease.py`.

*Second step (2026-09-25): the run's state in the bucket.* With the s3
state backend (`options.state.backend: s3`), a run keeps its state under
`<prefix><hop>/run/`, beside the restore points:
* **The lease.** It is written only over the version that was read, using
  the bucket's own conditional write (`If-Match`, or `If-None-Match: *`
  for the first). A write that loses reads again and decides again.
  Measured against an S3-compatible server in a container: of eight
  processes asking at once, one held the lease. A holder that stopped
  renewing was taken over after its term, and could not remove the new
  holder's lease when it came back.
* **Checkpoints.** A move's checkpoint is saved there after every chunk,
  and read from there by a machine that has none of its own. A second
  machine's dry run of a move the first had finished said it was already
  done.
* **The change position.** The tail's position, and the column shape it
  applies under, are pushed as they change, every two seconds at most. A
  position a few seconds old is safe to resume from, because the tail
  applies by key and converges on what it replays.
* **The copy record.** A later `--mode cdc` starts from it, on whichever
  machine runs it.

Run state is sealed like a restore point where `MIGKIT_STATE_KEY` is set,
since a checkpoint names the last key copied. The key is derived once per
process, not once per chunk. The lease is not sealed, because it holds
no data.

*Found on the way:* the dry run of a move looked its checkpoint up as
`schema.table`. Every copier but PostgreSQL's writes `database.table`,
and Redis writes `db<n>`. So on every other engine a finished or
half-done copy read as `todo`. Each engine now names the entry in one
place (`move_key`), and the copier and the plan both use it. A copy
resumed by a text key is said as `resume after [...]`, where formatting
it as a number would have stopped the plan.

The tests are in `tests/test_another_machine_takes_the_run_over.py` and
`tests/test_the_plan_reads_the_checkpoint_the_copier_writes.py`.

**31. Scale-out across machines.**

A coordinator hands out tables and chunks from a queue in the shared
store (item 30), and workers lease them. This is DMS Serverless and a
Striim cluster done the open way. The throttle already scales work
*down* when the source strains; add scaling *up* when the source has
headroom.

*Done for tables (2026-09-25):* a hop with `share_tables: true` and the s3
state backend can run the same `move --mode full --go` on several
machines at once. There is no coordinator: the bucket is the queue.
* The machine that arrives first sets up under the hop's lease: it
  creates what the target lacks and records the change position from
  before any copying. Every later machine uses that position.
* Then each table is taken by whichever machine gets its lease, a lease
  per table in the bucket. It is copied under its own load window, and
  the machine moves on.
* The checkpoint is merged into the bucket with conditional writes, so
  machines saving at once keep each other's tables.
* A table whose lease is held is asked for again. So a table whose
  machine died is taken over once its lease lapses, from that machine's
  last saved chunk.
* When every table is done, one machine completes the move: indexes,
  statistics, the copy record. The others say that another machine is
  completing it.

Measured with two processes, each with its own report directory, against
an S3-compatible server in a container:
* six tables were each copied once, split between the two machines, and
  every row arrived
* one machine killed partway through a 20,000-row table was taken over:
  the next machine carried on from the saved chunk, emptied nothing, and
  finished with every row once

The tests are in `tests/test_another_machine_takes_the_run_over.py`.
Still open: chunks of one table across machines, and scaling up within a
machine when the source has headroom. Meanwhile, adding machines is the
way to scale up.

**32. Alerts, not only metrics.**

Prometheus metrics exist. Add shipped alert rules and webhook
notifications (Slack, Teams, PagerDuty, generic HTTP) for:
* change-stream lag
* WAL or binlog retention filling up
* a verification finding a difference
* a run stalling

This is the part of CloudWatch alarms that migkit can own.

*Done (2026-09-25):*
* **Tail heartbeat.** A change tail now writes `tail.beat` each time
  round its loop. It records when the tail last read to the end of the
  source's log and how many changes it has applied.
* **Stop record.** A tail that stops on anything other than a person's
  ctrl-c or a service stop writes `tail.stopped`, saying what stopped it.
* **Tail metrics.** `/metrics` reads those files and exports:
  * `migkit_tail_behind_seconds`
  * `migkit_tail_heartbeat_age_seconds`
  * `migkit_tail_running`, which is 0 when the process is gone without
    stopping
  * `migkit_tail_paused`
  * `migkit_tail_changes_applied`
  * `migkit_tail_stopped`
* **Alert rules.** `deploy/prometheus-alerts.yml` ships rules over these
  metrics and the check metrics. A test holds every metric a rule names
  against what the exporter emits.
* **Notifications.** The hop option `notify`, or `MIGKIT_NOTIFY`, sends
  a message when:
  * the verdict moves between `same`, `different` and `error`
  * a tail stops on an error
  * a stopped tail runs again

  Slack, Discord, Teams workflows, PagerDuty (one incident per hop and
  one per tail, resolved when it clears) and any JSON receiver each get
  the shape they take. What is sent carries each finding's check, scope
  and status, never its detail, and a webhook's address is never
  printed.

The tests are in `tests/test_alerts_and_notifications.py`. The receivers
are local; nothing is sent to the real services.

*Retention.* Each engine is now asked how much longer the source keeps
what the tail has not read (`stream_room`). The tail asks once a minute
and `/metrics` exports the answer:
* **PostgreSQL:** the slot's `safe_wal_size` (0 once the slot has lost
  its WAL), and the bytes it holds when nothing caps them. Measured:
  32MB capped, 45,143,112 bytes of room, then `lost` after a load and a
  checkpoint.
* **MySQL:** seconds until the tail's binlog file may be purged. That
  is counted from when the file stopped being written, which is the
  time on the format event opening the next file. It is 0 once the file
  is gone, and nothing when auto-purge is off.
* **MongoDB:** seconds of oplog older than the resume point. The resume
  token opens with 0x82 and the cluster time.

There are alert rules for each (`MigkitRetentionShort`,
`MigkitRetentionBytesShort`). A managed MySQL's retention is its own
setting and is not read yet, so no number is given for it rather than a
wrong one. The tests are in `tests/test_how_long_the_source_keeps_the_log.py`.

**44. Failure caused on purpose.**

This is for day one, not instead of real use: real migrations keep
teaching after launch, and each failure they show becomes a case here.
The harness makes sure the failures already known are handled before
anyone meets them, in a form anyone can re-run. Each case, on every
engine 0e declares a move or stream for:
* migkit killed mid-dump, mid-load, mid-copy, mid-verify
* the network between source and target cut, then restored
* the source restarted, and the source failing over to a replica during
  a change stream
* the target's disk filling
* DDL on the source mid-stream (with item 5)
* the replication slot dropped, the binlog purged, the change-stream
  resume point expired

Done so far (problems file E7, `test_full_cdc_misses_nothing.py`):
* writes on the source while a `full+cdc` copy runs
* a tail stopped on a quiet source and started again
* a count-only tail followed by one that applies
* a collection dropped under a running tail
* the replication slot dropped and the binlog purged under a saved
  position (`test_a_lost_position_stops_the_tail.py`)
* **A position from another source, and MongoDB's resume point expiring
  (2026-09-25).** A saved position belongs to one server's log. After a
  failover to a server with a log of its own, or with the source rebuilt
  under the same address, a MySQL file and offset point into a different
  binlog. A MongoDB token from a rebuilt set had been accepted without a
  word. Now:
  * the source's identity is saved beside the position when it is first
    saved: MySQL's server id, PostgreSQL's system identifier (which a
    physical replica shares with its primary), and MongoDB's replica set
    name and id
  * every resume compares it. A different source stops the tail before
    it applies anything, and says which source the position belonged to
  * MongoDB is also asked, before the stream opens, whether its oplog
    still reaches back to the position, and whether the position is later
    than the set's own clock, which means it was taken on another set

  Measured by rebuilding each source under the same address: MySQL,
  PostgreSQL, and MongoDB with the same set name. A token moved behind
  the oplog's start was named as gone, and one moved past the set's clock
  as another set's. The tail stopped before opening the stream
  (`tests/test_a_position_from_another_source_stops_the_tail.py`).
* **migkit killed mid-load (2026-09-25).** This was measured with real
  moves, and the programs migkit started outlived it.
  * *MySQL, 1,500,000 rows.* The load went on without migkit: 375,000
    rows when migkit died, all of them soon after. A move started again
    at once emptied the tables under the running load, then stopped on
    `Duplicate entry '425000'`. It named neither the cause nor the
    program.
  * *PostgreSQL, 3,000,000 rows.* The copy program finished the table
    after migkit was gone. The target was left with every row, no
    primary key and no index, because the step after the copy never ran.

  The fix has two parts. First, `kill` (SIGTERM) now stops the programs
  the move started, the way ctrl-c does. Measured: the load stopped with
  migkit, the dropped index came back, and the next move was `same`.

  Second, `kill -9` and the out-of-memory killer cannot be answered by
  anything, so the programs a move runs are listed while they run
  (`running-programs.json`). The next move finds one still running,
  names the process, and stops before it writes anything. Measured: the
  second move refused while the orphaned load ran; the move after it
  ended went through, and the verdict was `same`. A process number
  reused by another program is not taken for the move's.

  The tests are in `tests/test_a_stopped_move_leaves_nothing_writing.py`.

  Seen on the way, and fixed: the deep checks said `every table has a pk
  or unique index` and `3 indexes, all valid` beside a schema check
  saying the target had lost all three. The first reads only the source
  and now says so. The second now counts each side.

  The second move after the PostgreSQL case put the keys back, and in
  the right order: it emptied the tables, loaded them, and then added
  the keys of the tables the killed run had created
  (`created-tables.json`).
* **The connection lost under a running tail (2026-09-25).** Measured
  with the target paused for 30 seconds: the tail stopped on `timeout
  expired` and did not come back. A tail is meant to run for days, and a
  brief network drop ended it.

  Now the tail re-reads from its last saved position once the server
  answers. It waits 2 seconds, then doubles the wait up to a minute, and
  logs each retry. Three cases were measured:
  * the target paused for 30 seconds
  * the target's network cut for 30 seconds (connection refused, retried
    four times)
  * the source paused for 30 seconds

  Each time the tail kept running and every row arrived once the server
  was back. An error that is not about the connection still stops the
  tail (a table dropped on the target, in
  `test_alerts_and_notifications.py`). The test is
  `tests/test_the_tail_rides_out_a_lost_connection.py`.
* **The target's disk filling (2026-09-25).** Measured by moving about
  150 MB of PostgreSQL into a target with a 150 MB disk. The move stopped,
  as it should. But its message was the last 500 characters of the copy
  program's log, mostly lines like `Sub-process exited with code 12`. The
  database's own lines were cut off above them: `could not extend file
  ...: No space left on device`, with its SQLSTATE and its hint.

  The target then stopped altogether, on `PANIC: could not write to file
  "pg_wal/..."`. The next move's log again ended in the program's own
  lines, above a refused connection.

  A failed program is now explained in the database's words. The side
  and the SQLSTATE come first, and the common ones are spelled out: `the
  target ran out of disk space: the target said: could not extend file
  ... (SQLSTATE 53100); hint: Check free disk space.; context: COPY big,
  line 175845`. A server that cannot be reached is named by its address.
  The tests use the logs as the program wrote them
  (`tests/test_the_database_words_come_through.py`).

Every case ends one of two ways: the run resumes from the last committed
point, or it stops and says in migkit's words what happened and what to
do. Either way, `check` afterwards proves the target. An outcome that is
silently wrong fails the harness.

**45. Proof at size.**

*Already done before launch:* migkit verified a real migration of about
800 GB - MySQL, PostgreSQL, and MongoDB to DocumentDB - that is now in
production use. Its data stays private.

Item 28 measures speed. What is left here:
* migkit's own move, not only its verification, at that size and past
  it, with the time and cost stated
* terabyte-class runs on an instance the owner rents, re-runnable by
  anyone with the same recipe
* memory staying flat as tables grow (every read streamed, never a whole
  table in memory)

*Memory, measured (2026-09-25):* migkit's own peak memory (`/usr/bin/time
-l`) on moves and checks of the same table at 200,000 and 1,600,000 rows.

| path | 200,000 rows | 1,600,000 rows |
|---|---|---|
| PostgreSQL to PostgreSQL, move (table copier) | 36 MB | 35 MB |
| PostgreSQL to PostgreSQL, check | 327 MB | 338 MB |
| MySQL to PostgreSQL, table keyed by text, before | 220 MB | **1,334 MB** |
| the same, now | 99 MB | 99 MB |
| MySQL to PostgreSQL, table with no key, now | 89 MB | 88 MB |
| MySQL to PostgreSQL, check | 46 MB | 44 MB |

The cross-engine copier held the table in three ways:
* a table without a single integer key went down a path that read it
  into one list
* a table with no key was read by `neutral_read` in one piece
* a keyed read took `--chunk` rows at a time (500,000), whatever the rows
  weighed

Now a keyless table is read through a cursor the server keeps
(`neutral_batches`: PostgreSQL, MySQL, SQLite). Each read is capped at
50,000 rows, and at 64 MB going by the table's own bytes per row. Each
read is still a resumable step. The copy took 22.8 seconds against 23.0
before. The tests are in `tests/test_a_copy_holds_a_read_not_a_table.py`.

*The terabyte recipe (2026-09-25):* `docs/scale.md` now carries the
recipe for the run on rented machines: the size in rows (about 3.6
billion `bench_rows` to a terabyte), the machines and volumes, the steps,
what to write down (including the cost, from each machine's hours and
price), and what counts as a pass. The pass is a `same` verdict with
peak memory flat from ten million rows. The run itself waits on the
owner renting the machines, since this laptop's container VM has a
20 GiB disk.

**46. Wrapped programs stay wrapped when they change.**

*Started (2026-09-24):* `tests/test_wrapped_flags_exist.py` reads the
command lines each bulk path builds for its dry run, and holds every long
flag against the installed program's own `--help` option column: the
MySQL, MongoDB and PostgreSQL dump and load programs. Run in a CI matrix,
it is the check that catches a renamed or removed flag before an operator
does. It found a gap in the option reader first. The MongoDB tools spell
their flags in camelCase with `=<value>`, and the reader, written for
lower-case flags followed by a column gap, saw 6 of mongodump's 37 flags.
It now reads them all, and still does not take a flag named inside
another option's description for a real one.

Every wrapped program has already changed underneath migkit once: flags
missing from the installed build, and a newer build emitting settings an
older server rejects. So:
* CI runs the move and check paths against a matrix of each wrapped
  program's versions
* each program declares the version range migkit supports, and `assess`
  says when the installed one is outside it (`_client_tool_versions`
  exists; extend it to every wrapped program)
* a release that changes behaviour is caught in CI, not by an operator

*Checked where the move runs (2026-09-25):* the same check now runs on
the machine doing the move, against the build installed there, not only
in CI. Neither of the following names the program.

`assess` gives one row for each program the chosen bulk path runs. The
row has three parts:
* the installed build's version
* whether that build is the one migkit was measured with (`MEASURED`:
  18.6, 1.0.5, 100.16.1)
* whether the build takes every option the move's own command lines
  pass it

A move with a build that lacks an option stops before anything is
written, and names the options. Tested with a stand-in older build on
the path, whose help lacked one option the move passes. `assess` failed
that row, and the move stopped without running the program.

Building this found a cache keyed on the program's name. A program
upgraded under a long-running process (`sync --serve`) was still read
as the old build. It is now keyed on the file and its modification time.

The tests are in `tests/test_the_installed_build_takes_the_options.py`.

*The matrix (2026-09-25):* `.github/workflows/wrapped-programs.yml` runs
both checks against each program's versions, weekly and on any change to
the bulk paths:
* PostgreSQL client 14 to 18, from the project's own repository
* the MySQL dump and load 0.21.4, 1.0.5 and 1.0.8
* the MongoDB tools 100.9.4, 100.12.0 and 100.16.1

Each download address was checked to exist. The workflow runs on GitHub
and has not run yet.

### P2: reach the paid tools have and migkit does not

**33. Targets that are not databases.**
* **Warehouses:** Snowflake, BigQuery, Redshift, ClickHouse. Load through
  staged Parquet files and each warehouse's own bulk load. Verification
  already reaches these through the second readers.
* **Streams:** Kinesis, Pub/Sub, Event Hubs, alongside Kafka.

Measure a wrap candidate before writing a loader, the rule as always.

*ClickHouse, done (2026-09-25):* an engine of its own, on either side of a
pair (`source_engine`/`target_engine: clickhouse`). It reads through the
HTTP driver and compares through the in-process renderer.
* **Created tables.** A table migkit creates is a MergeTree ordered by
  the source's key, with the columns outside the key nullable.
* **No doubled rows on a restart.** A MergeTree keeps a second row with
  the same key, so a batch written again after a restart deletes its keys
  first. Measured: 1,600 rows written again left 3,001 rows, 3,001 of
  them distinct, where the plain insert would have left 4,601.
* **Strings and bytes.** `String` columns are read as bytes, and decoded
  only where the column is text. Beside a column the other side declares
  bytes, a `String` is compared as bytes. Rendered as text, it had
  stopped the comparison on the first value that was not UTF-8.
* **Time zones.** A time with no zone is handed to the server as a
  wall-clock reading in the server's own zone. Measured from a machine at
  UTC+7: the driver had stored `2024-02-29 00:05` as `2024-02-28 17:05`,
  and every row read as different. Checked again against a server set to
  Tokyo, where the value round-tripped unchanged.
* **Deep check.** Mutations still being applied (the deletes of a
  restart) are named, since rows read before they finish are not settled.

A table ClickHouse made itself also moves into PostgreSQL: unsigned keys,
low-cardinality strings, a nullable decimal, millisecond times
(`tests/test_clickhouse_as_a_side_of_a_pair.py`).

*Kinesis, done (2026-09-25):* a pair with `target_engine: kinesis`
delivers its changes into Kinesis Data Streams. The messages are Kafka's,
built by the one module both use now (`streamout.py`), under the same
hop options.
* **Missing streams.** One the target does not have stops the delivery,
  unless the hop says `create_streams: true`, since a stream costs money
  for as long as it exists.
* **Order.** A key's records go to one shard. Kinesis can keep some
  records of a batch and refuse others, which would put a key's later
  change ahead of a refused earlier one. So a batch holds each key once,
  and a refused record goes again before its key's next change. Measured
  against a local stand-in: with key `b`'s record refused and the others
  kept, both keys' changes arrived in order and none twice. Resending
  from the first refused record, the obvious way, had doubled key `a`'s
  last two changes.

(`tests/test_changes_delivered_to_kinesis.py`)

*Pub/Sub, done (2026-09-25):* `target_engine: pubsub`, the same messages
from the same module. The target's `project` option names the project;
`host`/`port` an emulator. Each message carries its row's key as its
ordering key, and the publisher keeps order by it.
* **A topic nobody reads.** Measured on the emulator: a message published
  to a topic with no subscription is accepted - the publish returns its
  id - and a subscription made afterwards receives nothing. So a topic
  without a subscription stops the delivery before anything is sent, as
  a missing topic does. migkit makes neither: what reads them is not
  migkit's to decide.
* **A failed publish.** The client holds the key's later messages back;
  the delivery stops, frees the key, and the tail goes on from its last
  saved position when started again.
(`tests/test_changes_delivered_to_pubsub.py`)

*Event Hubs, done (2026-09-25) through its Kafka endpoint:* a Kafka hop
whose endpoint says `security_protocol: SASL_SSL`, `sasl_mechanism:
PLAIN`, the user `$ConnectionString` and the connection string as the
password. Every client migkit makes for a side is given the side's
sign-in (`tests/test_a_kafka_cluster_that_asks_for_a_password.py`,
against a broker that requires SCRAM; Event Hubs itself needs an Azure
account). A wrong password is said as a refused sign-in, where the
client alone said only that it could not reach the cluster. Amazon MSK's
IAM sign-in is not one of the mechanisms yet.

*Redshift, Snowflake and BigQuery, support written (2026-09-25),
untested:* each is a side of a pair (`target_engine: redshift` and so
on), reading its tables, columns and keys from its own
`information_schema` (Snowflake's keys from `show primary keys`), rows
through its DB-API driver, compared through the in-process renderer. A
table migkit creates keeps `not null` and the key, and nothing else of
the source's column rules; BigQuery's key is `not enforced`. Rows go in
as every DB-API engine writes them, except into BigQuery, which is given
a load job after the batch's keys are deleted - one insert statement per
row is a job each there. What is pinned without an account: the tables
each would create, how a page is read, how Snowflake's catalogue is read
back, and the rows BigQuery's load job is given
(`tests/test_warehouses_as_a_side_of_a_pair.py`). Still to come: loading
from staged Parquet through each warehouse's own bulk command, and a run
against real accounts.

**34. More engines to move, not only to verify.**
* **Sources DMS takes that migkit cannot:** Db2, SAP ASE, SQL Server at
  full depth.
* **Targets:** S3 (Parquet), DynamoDB, OpenSearch/Elasticsearch,
  Cassandra.

Order by what a real migration asks for. Every one arrives with its
verification, or it does not arrive.

*Parquet, done (2026-09-25):* files on a disk or under an S3 prefix, on
either side of a pair (`target_engine: parquet`, with the endpoint's
`path`, or `url: s3://bucket/prefix` plus `endpoint_url` for
S3-compatible storage).
* **Layout.** A database is a directory, and a table is its part files
  plus `_table.json`, which says what the columns are and which of them
  key the table. Files keep neither.
* **Types.** Values are Arrow types: `decimal128(p, s)`,
  `timestamp[us]`, `date32`. A decimal whose source declared no
  precision is kept as its text, since no fixed scale reproduces what an
  unconstrained numeric holds.
* **Restarts.** A keyed table names each part by the keys it holds, so a
  batch written again after a restart replaces itself.
* **As a source.** A Parquet table is read in one pass, a batch at a
  time, since files keep no order to resume by.
* **Deep check.** Every part file must open and hold the rows its footer
  says. A part cut off halfway is named.

Measured on a disk and in an S3-compatible bucket: a removed part is a
difference, and a truncated one is a deep finding. The files moved back
into PostgreSQL with every row
(`tests/test_parquet_files_as_a_side_of_a_pair.py`).

*Db2, written, not yet run against a server (2026-09-25):* on the same
driver base as Oracle and SQL Server, through `ibm_db`. A hop's database
is a schema; the endpoint's `database` option is the database to connect
to.
* It shares Oracle's name folding and its padding and scale handling.
* A character column with no code page (`FOR BIT DATA`) is bytes.
* The deep check names tables a load left in check-pending.

Held without a server, because IBM's image runs on x86 only
(`tests/test_db2_as_one_side_of_a_pair.py`).

*SAP ASE, support written (2026-09-25), untested:* a side of a pair
(`ase`, with `sybase` and `sap-ase` as other names for it), reached
through FreeTDS's ODBC driver at protocol 5.0 - the TDS driver that
installs with pip stops at SQL Server's 7.x (measured, `unrecognized tds
version: 5.0`). Tables, columns and the primary key come from ASE's own
catalogue; rows are compared through the in-process renderer. SAP's image
runs on x86 only, so what is pinned is how the connection is asked for,
how names, parameters and pages are written, how its types are classed,
and the table it would create (`tests/test_sap_ase_as_one_side_of_a_pair.py`).
Where ODBC cannot load (measured: the module installs, then misses
`libodbc.2.dylib`), it says what to install.

*Cassandra and ScyllaDB, done (2026-09-25):* on either side of a pair
(`cassandra`, with `scylladb` as another name for it). A database is a
keyspace.
* **Keyspaces.** A keyspace is created only with the replication the
  target endpoint names (`replication`), since how many copies it keeps
  is the operator's decision. The deep check warns while a keyspace
  keeps fewer than three copies.
* **Keys.** A table migkit creates takes all of the source's key columns
  together as its partition key. No clustering order is invented. A
  table with no key is refused.
* **Writes.** An insert replaces a row with the same key: 901 rows after
  a replay, not 1,802. A null is left unset rather than written, so no
  tombstone is made.
* **Reads.** Rows come back in token order, so a table is read in one
  pass, a page at a time, and looked up by key concurrently.
* **Timestamps.** A `timestamp` holds milliseconds. A source value with
  microseconds arrives without them, and the check reports a
  difference, because the target does hold less.

(`tests/test_cassandra_as_a_side_of_a_pair.py`)

*OpenSearch and Elasticsearch, done (2026-09-25):* on either side of a
pair (`opensearch`, with `elasticsearch` as another name for it). A
database is a prefix on index names, and a table is an index.
* **Ids.** A document's id is the canonical text of the row's key. A
  batch written again replaced itself: 1,201 documents after a replay,
  not 2,401.
* **Values.** What each column was is kept in the mapping's own `_meta`.
  A decimal goes into `_source` as its text, and it came back as
  `1234.5000` and `1234567890123456.7890`: a JSON number would have lost
  the scale of the first and the digits of the second.
* **Reads.** An index is read in one pass through a scroll, and looked up
  by key through `_mget`. The target is refreshed before migkit reads
  it; the source never is.
* **Indexes migkit did not make** are read by their mapping, each field
  by the class it holds, and keyed by `_id`. An object field stays
  unmapped.
* **Deep check.** A red index is named, since a check reading it is not
  reading everything.

(`tests/test_opensearch_as_a_side_of_a_pair.py`)

*DynamoDB, done (2026-09-25):* on either side of a pair
(`target_engine: dynamodb`, with `endpoint_url` and `region`; a database
is a prefix on table names, `<db>.` by default).
* **Types it does not keep.** DynamoDB keeps a number without the scale
  it was written with: `1.50` came back from the service as `1.5`. It has
  no time type either. So a table migkit creates carries each column's
  class in its tags, and values are read back as that class: a decimal
  at its scale, a time as a time. Where an endpoint takes no tags
  (DynamoDB Local), the description is kept on this machine by endpoint,
  where any hop reading that endpoint finds it.
* **Keys.** A composite key becomes the partition and sort keys. A table
  keyed by three columns, or by none, has no DynamoDB table to go to, and
  is refused by name.
* **Writes and reads.** Items are written by key, so a batch written
  again lands on itself. A table is read in one pass, since items come
  back in no order. Rows are looked up by key through batch reads.
* **Deep check.** Tables that are not active are named, and so is a
  target table without point-in-time recovery, which leaves a cutover
  nothing to go back to.

Measured against DynamoDB Local: PostgreSQL in and back out with every
value as it was. A changed item was a difference, and a replayed batch
left 701 items, not 1,402 (`tests/test_dynamodb_as_a_side_of_a_pair.py`).

**35. Change-stream delivery in the formats consumers expect.**

DTS offers five. migkit should offer:
* Avro, JSON, Debezium and Canal-compatible formats, with a schema
  registry (Redpanda ships one)
* topic and partition rules by table, key or column
* skipping oversize messages, with a count of what was skipped

*Done for Kafka (2026-09-25):* a hop with `engine: hetero` and
`target_engine: kafka` carries the change tail into topics.
* **Formats:** `format` is `json`, `debezium` (the envelope: before,
  after, source, op) or `canal` (data, type, pkNames).
* **Routing:** `topic` is a template, `{db}.{table}` by default.
  `partition_by` names a column. By default the partition comes from the
  row's key, so each key's changes stay in order.
* **Every change is sent, in the source's order.** Changes are not
  collapsed to the row's last state the way table writes are.
* **Oversize messages:** a message larger than `max_message_bytes` is
  skipped, and counted by topic in `stream-skipped.json`.

Found while building it: the tail paired each table with an existing
topic by its last name. A second stream's changes for `o` went to
`rule(a.o)`, a topic nobody read. A stream's destination is now named by
its rule alone. The tests run against Redpanda
(`tests/test_changes_delivered_to_kafka.py`). Still open: Avro and a
schema registry.

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

*Done for PostgreSQL's own replication (2026-09-25):* these streams write
to the source, so they are off unless the hop's options say so.
`topology: two_way` runs a stream each way. Each subscription is created
with `origin = none`, so it takes only what the other side's own sessions
wrote, and nothing goes round.

Measured with an insert on each side: both rows arrived on both sides,
each side held two rows, and `apply_error_count` stayed 0 on both (an
echo would have been a second insert of the same key). Tearing down with
`--drop` removes both streams. Two-way is refused before PostgreSQL 16 on
either side, because that is where a subscription can first tell the
two apart.

A conflict stops the stream that meets it, and the stream's status says
so. Resolving conflicts by policy needs migkit's own tail and is still
open, as are MySQL two-way and more than two nodes
(`tests/test_streams_back_and_both_ways.py`).

**37. Rollback that loses nothing: live reverse replication.**

At cutover, start a stream from the new target back to the old source,
positioned at the same fence. If the cutover is abandoned, the old side
has every write made since. Verify both directions while it runs. This is
the GoldenGate and Striim failback, with migkit's checks on it.

*Done for PostgreSQL's own replication (2026-09-25):* `reverse:
at_cutover` makes tearing the stream down (`move --mode cdc --drop --go`)
also start a stream back, from the new primary to the old source, from
that moment. The application's roles are given back the target only once
that stream runs.

Measured: a row written on the new primary after cutover reached the old
source, and a row written on the old source no longer went forward.
Without the option, nothing is created on the source. MySQL takes the
same path (a replica on the old source) and is still to be measured
(`tests/test_streams_back_and_both_ways.py`).

**38. Accounts and clouds without long-lived passwords.**
* **Passwordless sign-in to managed databases:** assume-role and workload
  identity, RDS IAM authentication tokens, Cloud SQL IAM
* **Secrets:** AWS Secrets Manager, GCP Secret Manager and Azure Key
  Vault next to the existing Vault
* **Cross-account access:** the cross-account role pattern DTS uses,
  done with each cloud's own mechanism

*Done, checked offline (2026-09-25):*
* **Sign in without a password.** `auth: aws_iam` on an endpoint signs
  in to RDS and Aurora with a token the cloud signs. A token opens
  connections for 15 minutes, so migkit signs a new one once the old one
  is 10 minutes old, and a long run does not start failing halfway.
  `aws_role_arn` signs as a role in another account, assumed for each
  token.
* **Secrets from each cloud's store**, read at load time:
  * `aws-sm:<id>[#field]`: AWS Secrets Manager, taking the region from
    the ARN, and a field from a JSON secret such as the ones RDS keeps
  * `gcp-sm:projects/<p>/secrets/<s>`: Google Secret Manager
  * `azure-kv:https://<vault>.vault.azure.net/secrets/<name>`: Azure Key
    Vault

  Each of these is beside Vault, `env:` and `file:`.

The tests reach no cloud. The token is signed on this machine from
stand-in credentials, and it names the host, the user, the action and
the role's key. The secret store and the role answer through the SDK's
own stub (`tests/test_cloud_sign_in_and_secrets.py`).

Still to do against the real services: a sign-in to RDS with a token,
Cloud SQL IAM, and managed identities.

**39. Converting schema and code between engines, wider.**

Ora2Pg covers Oracle. Add:
* SQL Server T-SQL to PostgreSQL
* MySQL routines to PostgreSQL

Use sqlglot where SQL is enough. Every converted object stays behind
migkit's behavioural proof (plan 20): same inputs, same outputs, on both
sides.

*Done (2026-09-25):* `schema --convert` now carries views and functions
whose body is one expression, between MySQL, PostgreSQL and SQL Server,
in any direction:
* Views are created after the views they read.
* A qualifier naming the source's own database, or a schema holding
  something the move carries, is dropped. Any other qualifier stays, and
  fails on the target rather than pointing at a table of the same name.
* A body of statements, a procedure, or a type with no counterpart is
  written as a comment naming it and saying why. `--apply` leaves any
  object the target already has alone, and lists any it could not
  create.

The proof is in the hetero deep check, `converted code`:
* A view is compared by the digest of its rows.
* A function is compared by its answers, in one read-only query per
  side, to every combination of its arguments' test values: null, the
  edges, a value that rounds, case, a trailing space, a character wider
  than a byte, and dates.
* A converted object the target lacks is a warning, not a pass.

Measured on MySQL to PostgreSQL:
* The translator turns MySQL's `length` (bytes) into PostgreSQL's
  `length` (characters). Only the wide-character input tells them apart,
  and the proof names the function.
* PostgreSQL ignores a function's declared decimal scale, for both its
  arguments and its result, where MySQL rounds. The conversion now states
  the scale as a cast in the body, and the proof caught the version
  without it.

SQL Server as a pair's side:
* Tables, keys, reads, writes, digests, views and functions, read through
  its driver.
* Rendered in-process by the renderer every in-process engine shares.
  The digest of the values the driver returns was measured equal to
  PostgreSQL's in-server digest of the same rows.
* The T-SQL paths are written, but have not yet been run against a
  server, because none runs on this machine's architecture.

Tests: `tests/test_views_and_functions_carried_across_engines.py` and
`tests/test_sql_server_as_one_side_of_a_pair.py`.

**40. Estimates that rest on measurement.**

SCT estimates effort. migkit refused to guess, because it had nothing to
calibrate against. Item 7's per-phase timings from real rehearsals are
that calibration. Estimate time and effort as ranges, and state the runs
each range came from. Without enough runs, say so instead of guessing.

*Done for time (2026-09-25):* the plan estimated from the latest run on
the path alone, however the others had gone. It now gives a range from
every run kept (the last ten), for example "between 5min and 20min going
by the 3 runs on this path (5,000 to 20,000 rows/s, first date to last
date)". A single run is called "one number, not a range".

A hop that has not run yet is given the runs of other hops with the same
engines on the same path, and told whose they were. Typically these are
a rehearsal's runs, for the migration it rehearses. Runs on other engines
are never used. With nothing measured, the plan still estimates nothing.
The tests are in `tests/test_time_from_measured_rates.py`. Effort
estimates for conversion wait on item 39.

**41. Trust, the open-source way.**

A vendor sells certifications. What an open tool can offer instead:
* signed releases, an SBOM and build provenance (SLSA)
* a written threat model
* an audit log of every write migkit makes, on either side
* reports encrypted at rest
* redaction of values in what is shown and stored (with G2)

*Done (2026-09-25):*
* **Threat model.** `docs/threat-model.md` covers what migkit is given,
  what it writes and where, its secrets, the data in reports, and the
  processes it runs. Each claim names the test that holds it.
* **A finding while writing it: the local copy.** A dump-and-load move
  keeps the source's whole data in files under the report directory.
  That copy was removed only after a load that succeeded. A load that
  failed or was stopped left it readable by anyone who could read the
  directory. Now the copy is readable by this user only (0700) before
  anything is written into it, and it is removed when the move ends,
  whichever way it ends (`tests/test_the_local_copy_is_private_and_goes.py`).
* **Redaction.** Redaction was done with G2 (`mask`). Notifications and
  the diagnostics bundle carry no values.
* **Audit log.** `changelog.jsonl` records each operation.

*Then (2026-09-25):*
* **Releases** carry a CycloneDX SBOM of the wheel as installed, and build
  provenance signed through GitHub's Sigstore attestation. The SBOM
  command was run here: 51 components. The attestation steps run on the
  next tagged release.
* **Restore points** are encrypted with `MIGKIT_STATE_KEY` before they
  leave the working directory, for the mirror and the bucket. A restore
  point holds whole rows. Measured: without the passphrase, or with
  another one, a point is refused rather than read (`tests/test_state.py`).

Still open: the report directory's own files at rest (drilldowns). Mask
the values with `mask`, and use the disk's encryption for the rest.

**42. Support, without an SLA.**

A troubleshooting guide keyed by every error migkit can raise, each with
its fix. A diagnostics bundle collected by `doctor` with secrets and
values redacted, so a problem can be reported without handing anything
over (an environment variable, not a new flag).

*Done (2026-09-25):*
* **`docs/troubleshooting.md`** is generated from the code. It lists
  every refusal written where it is raised, 129 of them, grouped by the
  part of migkit that raises it. Each message already says what to do.
  A test fails when the guide falls behind the code, and it names no
  program.
* **`MIGKIT_DIAGNOSE=<file>.zip migkit doctor`** writes a bundle to
  report a problem with. It holds versions, what each engine can do
  here, the configuration, and each hop's last verdict and change log.
  Passwords, tokens, keys, notification addresses and credentials inside
  an address are taken out before anything is written, and so is every
  finding's detail, which is where keys and values are
  (`tests/test_a_problem_can_be_reported_without_handing_anything_over.py`).

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

*First use (2026-09-25):* `MIGKIT_AI` picks the provider:
* `openai`: any OpenAI-compatible endpoint, including local models, at
  `MIGKIT_AI_URL`
* `anthropic`
* `google`

The key is `MIGKIT_AI_KEY` and the model `MIGKIT_AI_MODEL`. migkit chooses
no model for an OpenAI-style endpoint.

After a `check` that found something, the provider's plain-language
account of the findings is printed under the verdict. It is marked as a
proposal, and changes nothing.

What is sent is each finding's check, scope, status and category. The
detail, where keys and values are, is sent only with
`MIGKIT_AI_SHARE=detail`, and never for a hop that masks values. A
provider that cannot be reached is said, and the check's result stands.
The tests use a local stand-in for each API's own shape
(`tests/test_help_from_a_model_any_provider.py`).

*Conversions (2026-09-25):* a view or function the translator cannot
carry can get a proposal from the provider, when `MIGKIT_AI_SHARE`
includes `code`:
* **What is sent.** The object's own definition, from the source (`show
  create` on MySQL, `pg_get_functiondef` and `pg_get_viewdef` on
  PostgreSQL). No row is sent.
* **What comes back.** One `create` statement, taken from a fenced block
  or from the answer. It goes into the converted file marked as a
  proposal, and only `--apply` puts it on the target.
* **The proof.** The proof now covers every view and function of the
  source: the translator's, a model's, or a person's. Each is asked the
  same inputs on both sides. One the target does not have is named. A
  procedure answers nothing to compare, and is counted as such.

Measured with a local stand-in provider: of two proposals, the one that
doubled where the source doubled and added one was named as answering
differently. Once rewritten by hand, both passed. Without `code` in
`MIGKIT_AI_SHARE`, nothing was sent
(`tests/test_a_model_proposes_what_the_translator_cannot_carry.py`).

Still to use it for: drafting fixes, which go through the same proof as
anything else.

### Not pursued

* **ETL and transformation**, beyond the mapping migkit already has.

---

## Research round 2026-09-27: what to wrap, what to build, in what order

The owner's rules for this round: research every tool and technique first,
write each finding here before building it so nothing is dropped, wrap the
best existing tools and libraries under migkit's own names (as reladiff is
wrapped) rather than writing everything by hand, and never make the
operator size parallelism by hand. Seven research passes; what each found
and what migkit does with it. `todo` items are built in the order at the
end of this section; `blocked` items say what they wait for.

### R0. Found while measuring, fixed the same day

* **The MySQL change tail lost rows (fixed).** One insert of 20,000 rows,
  read 1,000 at a time: the reader kept its position after whichever rows
  event the limit fell on, and read from there the rest of the transaction
  came before the table map describing it - the reader dropped it and went
  on to the end of the log. 1,281 changes came back, 18,719 were lost, and
  nothing was said. The change-only verify stopped at its limit the same
  way. The position is now kept only where a transaction ended, with the
  count of its rows already handed back (as Debezium keeps its own), and
  the stream is held open while the caller comes back with the position it
  was given (`test_the_binlog_reader_keeps_its_place_between_transactions.py`).
  PostgreSQL's reader was checked and is safe: a slot is peeked whole
  transactions at a time.
* **`on duplicate key update` on a table with more than one unique index
  (done).** MySQL documents that it may update a different row than the one
  the key names (bug 79937, closed as expected); migkit's MySQL writer and
  applier used it for every table. Measured: a batch writing (2, 'a') and
  then (1, 'b') onto a target holding (1, 'a') gave row 1 the new row's
  values and never made row 2. Such a table is now written by its key -
  the rows already there updated, then the others inserted - and a value
  two rows really both claim is refused
  (`test_runs_of_rows_apply_as_one_statement.py`).
* **The applier's collapse broke foreign-key order (done).** A row's last
  change went where its first had been: an update of a parent, the child's
  update and delete, the parent's delete became the parent's delete before
  the child's - refused by the key, and the same on every replay. Tables
  joined by keys or with a second unique index are now written change by
  change in the source's order (`test_the_tail_applies_side_by_side.py`).

### R1. Parallelism sized by migkit, never by the operator (done 2026-09-27)

Built as below (`migkit/sizing.py`, `Engine.capacity` for PostgreSQL and
MySQL, `ranges.Slots` paced, range processes kept for every range a slot
copies). The controller, fed curves with a best point: it settled at 6.0
where 6 was best, 3.0 on a plateau from 3, 10.0 through 10% noise
(`test_how_many_at_once_is_worked_out.py`). On the sandbox (a 2-CPU MySQL
serving as source and target, a million rows) the worked-out number
matched the best fixed one within the run-to-run noise (8.7s to 11.5s
either way) with nothing set. One finding on the way: starting a process
for every range made more, smaller ranges slower (7.7s in eight ranges,
11.2s in sixteen); a process is now kept for every range its slot copies.

What others do: almost every tool has a static knob anchored to the
server's cores (pgcopydb 4/4, pg_restore -j, mydumper threads, DMS
`MaxFullLoadSubTasks`); the adaptive ones combine a bounded range with
feedback from the measured rate (MongoDB 7's throughput probing, GoldenGate
min/max apply parallelism, mydumper `--rows MIN:START:MAX`, pt-osc
`--chunk-time`, gh-ost `max-load`/`critical-load`).

migkit's design:
* **A first estimate from every factor at once:** the migkit machine
  (cgroup-aware CPUs, available memory over the measured memory a worker
  takes, load), each server's free connections (max minus reserved minus in
  use, a quarter of them on a production primary, half on a replica or
  target), each server's CPUs and running sessions where it can be asked
  (PostgreSQL `max_worker_processes` and `max_connections` defaults reveal a
  managed instance's size; MySQL `RESOURCE_GROUPS.VCPU_IDS`, `Threads_running`;
  SQL Server `dm_os_sys_info`; Oracle `v$osstat`; MongoDB `hostInfo`;
  ClickHouse `CGroupMaxCPU`), the ranges there are to run, and the link
  (RTT of a trivial query; bytes per second of the first ranges). A factor
  that cannot be read drops out of the minimum and caps at 4. Budgets are
  per server, so two hops on one server share it.
* **A controller while it runs:** a safety loop that cuts workers by 30%
  and holds for two windows on replica lag, running sessions over CPUs, a
  growing InnoDB history list or ticket queue, throttle replies, credit or
  burst balance, or connection errors (the existing `Throttle`,
  generalised); an optimiser that hill-climbs on committed bytes per second
  (slow start while the gain is 25% or more, then steps of 10% kept only
  for a 5% gain, and a probe upward one window in eight, as BBR does); and
  a separate, faster loop sizing ranges to one or two seconds of work each.
  Decrease fast, increase slow, and never change twice within two windows.
* **`workers` on a hop becomes a ceiling,** for an operator who wants to
  cap it; nothing needs it set.
* Wrapped: psutil (host), loky's cgroup-aware CPU count (or its logic).
  Written: the controller (no mature Python library exists; the ones
  there are asyncio-only and alpha).
* Tested: the controller against synthetic throughput curves (plateau,
  cliff, noisy); the estimate against the sandbox's servers with load made
  by pgbench and sysbench.

### R2. The change applier at high rates (1-3 done 2026-09-27; 4 done
for PostgreSQL; 5, 6 todo)

Measured, MySQL to PostgreSQL, 320,000 changes queued: 21.8s before; the
next batch read while one is applied and batches that grow while behind
(1,000 to 16,000), 7.7s; the column mapping skipped where there is none,
5.5s; lanes, 4.9s. With the target 10 ms away: 59.6s in one lane, 17.1s in
four; the rows no order binds written as one run of deletes and one of
upserts a table, 10.0s and 6.0s. A PostgreSQL source is not read ahead: its
slot takes the position it is handed as everything before it applied
(`READS_AHEAD`). 4 on PostgreSQL: a run of a thousand upserts or more
goes by COPY into a temporary table shaped as the target one (made once a
session) and on by one `insert ... select ... on conflict` that also
empties it - measured, 5,000 rows in 24 ms against 51 as multi-row
statements, 20,000 in 91 against 166, and two round trips a run; a value
of a kind COPY's text is not written exactly for falls back to the
statements (`test_runs_of_rows_apply_as_one_statement.py`). Still to
build: 4 on SQL Server; on MySQL measured and not adopted (2026-09-27):
5,000 upserts 36 ms as multi-row statements and 55 ms by the pinned load
into a temporary table and one `insert ... select`, 20,000 86 against 84 -
and five round trips against three. 5 done (2026-09-27,
`test_keys_off_where_every_parent_comes_too.py`): where every parent a
table in scope points at is in scope too, MySQL's apply sessions turn
foreign key checks off, and PostgreSQL's - running as a replica already -
stop holding a parent and its child together; each table's rows then go as
runs and lanes. Measured, MySQL 10 ms away, 6,000 parents and children
written alternately: 154s held together in source order (a statement a
row), 1.4s with the keys off; locally 0.41-0.49s against 0.35s. A hop that
leaves a parent out keeps them on; the deep check's orphan scan finds a
child whose parent never came. 6 measured first (2026-09-27): the binlog reader decodes 300,000
changes in 2.58s (116,000 a second) on its own, while the whole tail
applied 320,000 in 4.9s (65,000 a second) - the applier is the limit, and
the reader already runs beside it (`_ReadAhead`); decoding moves out of
process only once the apply passes the reader. It did (2026-09-28, F6):
beside it in a thread the two took turns on one core, and with the apply
trimmed and the decoder in a process of its own the reader is the limit.

What others do: MySQL write-set and GoldenGate parallel replicat order only
the changes whose keys overlap; MariaDB applies optimistically and retries
on conflict; DMS, Qlik and HVR apply net changes per table and ask for
foreign keys off. DMS never applies CDC in parallel to MySQL, PostgreSQL or
SQL Server targets.

migkit's design, in the order it pays:
1. **Read and apply at the same time,** the reader in a process of its own
   feeding a queue capped in bytes; a batch ends only where a source
   transaction ended; the position is saved after every lane has
   committed.
2. **Parallel lanes by dependency:** for every collapsed row, its table and
   old and new key, every unique index's old and new value, and for a child
   row its parent's key; rows joined by any of them go to one lane
   (union-find), lanes packed largest first. A table with no key, DDL and a
   transaction larger than the batch are barriers. Within a lane: parents
   upserted before children, children deleted before parents, rows sorted
   by key in each statement.
3. **Errors:** a deadlock or serialization failure retries that lane; a
   constraint error rolls the lane back and applies its rows one by one in
   source order; still failing, the tail stops and says so.
4. **The statement by run length:** a few rows as one multi-row statement;
   past about a thousand on PostgreSQL, COPY into a temporary table and one
   `insert ... select ... on conflict` (5,000 rows: 24 ms against 110 ms
   published); MySQL through the pinned load into a temporary table where
   the table has one unique index; SQL Server through a staging table and
   update-then-insert.
5. **Foreign keys:** enforced by default (edges join lanes and order them);
   off (`session_replication_role`, `foreign_key_checks`) only where every
   parent is in scope, with an orphan scan added to the verify.
6. **Decoding:** measured against apply first; where it is the limit, the
   reader under PyPy, then a sidecar (go-mysql for MySQL, pglogrepl for
   PostgreSQL) writing records migkit reads.

### R3. Two-way and many-node topologies, conflicts (MySQL native
two-way, migkit's own tails both ways and conflict policies done
2026-09-27; the ladder of marks - origins, messages, tagged GTIDs,
MariaDB's own flag, comments - done 2026-09-28; MariaDB's GTID domain
and many-node topologies todo)

Built and run (`test_mysql_streams_both_ways.py`, two MySQL 8.4 servers
each a replica of the other): `loops_prevented` for MySQL asks what keeps
a change from going round (each side's own server id, GTIDs on both,
changes passed on) and what keeps two sides' new rows apart (an
auto-increment increment of two and an offset each), and names whichever
is missing before anything is set up. Measured: rows written on both at
once converge, the executed GTID sets hold still, the replicas report no
error. Measured too, and now said when two-way is set up: a row both sides
change at once ends holding each other's values - neither server's
replication reconciles it; `check` finds it. Then migkit's own tails both
ways (`migkit/twoway.py`, `test_two_ways_through_migkits_own_tails.py`,
MySQL and PostgreSQL both ways at once): a hop's `two_way` makes every
transaction its tail applies begin with a row of `migkit_origin` (a row a
thread, so lanes do not wait on one), and the tail reading that side the
other way leaves any transaction that begins so out whole - the mark kept
with the position where a read stops inside one. Measured before, 20 rows
written on each side: 40 changes applied one way and 60 the other, every
change back where it began; after, 21 and 22, and nothing more while
quiet. Each change is held to the target's row as it is now: the change's
before image (a binlog's full row, REPLICA IDENTITY FULL) against it tells
`update_origin_differs` and `delete_origin_differs`, and `insert_exists`
and `update_missing` need none; `error` (the default) stops the tail
before the batch is applied, `apply_remote`, `keep_local` and
`last_update_wins` (by a named column) decide, and every conflict is
written with both versions to `conflicts.jsonl`. A slot's timestamp
arrives as the text it printed and a MySQL row's as a datetime: compared
as they rendered, a row nobody had changed was called a conflict
(measured) - both are read as their class first. Still to, as
researched:

* **Telling migkit's own writes apart on MySQL and MariaDB,** chosen per
  target by the decision layer: MariaDB `skip_replication` (flag in the event
  header, read by the reader); a GTID domain or tag of migkit's own (MariaDB
  domain per writer; MySQL 8.3+ tagged GTIDs, where the reader can parse
  them); otherwise a comment on every applied statement, read back from the
  rows-query or annotate event (`binlog_rows_query_log_events`). A marker
  table in the user's database only when the hop allows it. Decided per
  transaction.
* **Conflict policies:** `error` (default), `apply_remote`, `keep_local`,
  `last_update_wins` (commit time or a named column, node rank breaking a
  tie), `source_priority`, and per-column `delta` for counters; named as
  PostgreSQL 18 names conflicts (`insert_exists`, `update_origin_differs`,
  `update_missing`, `delete_missing`, ...); every decision logged locally
  with both versions of the row.
* **Topologies:** many-to-one (keys kept apart, deletes scoped to their
  origin, verified per origin), one-to-many (the slowest target holds the
  source's log), a full mesh rather than a forwarding ring.
* Tested in docker: pairs of mysql:8.4 and mariadb:11, both directions
  writing, echoes counted (must be 0), both sides hashed equal.

Done 2026-09-27 (`test_two_way_counters_add_on_both_sides.py`):
`source_priority` (`source_rank`/`target_rank`, which also breaks a tie
of `last_update_wins`) and `delta` counters. Measured before, a balance
of 100 moved +10 on MySQL and +5 on PostgreSQL: `error` stopped both
tails; `apply_remote` left MySQL at 105 and PostgreSQL at 110. Now a
counter's change is applied as what it added, where the target's row
stands (`canon.Added`, `n = n + by` in the batch's transaction), so both
end at 115; a row that differs only in its counters is no conflict; where
the policy keeps the target's row, the counters still add. A batch applied
twice would add twice, so a hop with counters applies each batch as one
transaction (no lanes) whose `migkit_origin` mark says which batch it was;
the tail going on after a stop, or after a connection lost at commit, asks
the target and goes on after a batch it had committed (measured with a
failpoint between the commit and the saved position: added once).
Still to: MariaDB's GTID domain, many-node topologies (the marks: done
2026-09-28, below).

**Decided 2026-09-27 (the owner: "the smartest way, a ladder of our own
if that is what it takes"): how migkit's own writes are told apart is a
ladder, climbed per side, proved end to end before it is trusted.** The
table is the bottom rung, not the design. Two things a rung can give:
*apart* (the tail reading that side leaves migkit's transactions out) and
*exact* (the target itself says which batch it last committed, so a batch
is never applied twice - what counters need, `exact`). Per side:

* PostgreSQL:
  1. **A replication origin of migkit's own**
     (`pg_replication_origin_session_setup`, the batch's position handed
     to `pg_replication_origin_xact_setup`): apart *and* exact - the
     origin's progress is the last committed batch, the very machinery a
     subscription uses. Grantable from 16, superuser before. The reader
     leaves out what carries an origin (server-side `origin = none` from
     16; the origin message in the stream before that).
  2. **A transactional logical message first in the transaction**
     (`pg_logical_emit_message(true, 'migkit', ...)`): apart, any user,
     no table, one WAL record a transaction; decoded from 14. Exact only
     with the table's batch mark beside it.
  3. **The table** (`migkit_origin`, any version, needs CREATE): apart
     and exact.
* MySQL and MariaDB:
  1. **A GTID of migkit's own**: MySQL 8.3+ tagged GTIDs
     (`gtid_next = 'UUID:migkit:n'`, migkit's own UUID, n the batch
     number, `TRANSACTION_GTID_TAG`): apart by the tag, exact because
     `gtid_executed` names the batches committed. MariaDB: a
     `gtid_domain_id` of migkit's own plus `gtid_seq_no` (SUPER): the
     same two things.
  2. **MariaDB `skip_replication`** (SUPER; the flag in the event header,
     read by the reader): apart only.
  3. **A comment on every applied statement**, read back from the
     rows-query or annotate event: apart only, and only where the server
     already logs them (`binlog_rows_query_log_events`,
     `binlog_annotate_row_events`) - migkit changes no server setting.
  4. **The table**: apart and exact.

The rule: the highest rung the side's version and the tail's grants
allow; a hop with counters (`exact`) skips a rung that is apart only. Then
the proof, before anything is applied: the tail writes one probe
transaction on that side through the chosen rung, reads that side's log
from just before it with its own reader, and must see the probe come
back marked as migkit's own - a rung that does not prove itself is said
and the next one down is tried. The rung chosen is written in the tail's
token so a restart keeps it, both directions may sit on different rungs,
and `doctor` names each side's rung and footprint (the table, where it
is the rung, and the two-way teardown drops it). Measured in docker per
version and grant before any rung is claimed: PostgreSQL 13/14/16,
MySQL 8.0/8.4, MariaDB 11 - what pymysqlreplication and migkit's own
pgoutput decoder deliver of tags, flags, origins and messages.

**The owner's rule for the order of the rungs (2026-09-27): what wins is
what is measured faster, checks better and holds up better - the
footprint is the tie-breaker, not the ranking. If the table rung proves
faster or steadier than a tag on some version, the table is the top rung
there, and that is fine.** So the ladder is not ordered by taste but by
three measurements a rung must pass, per engine version, in docker
before it is ranked: (1) the applier's rate with the rung on against
the rung off (`bench/run.py`, transactions a second and the cost a
transaction of the mark, the tag or the origin call); (2) the reader's
rate and what it must decode to leave a transaction out (server-side
filtering counts for it); (3) the failure battery of R7 run under the
rung - the connection lost at commit, the tail killed between apply and
save, the target failed over, the tail restarted from an old token - with
zero changes carried back and every batch applied exactly once. A rung
that fails (3) is not a rung, however fast. What no other tool does, and
this must: choose per side from those measurements, prove the choice
with a probe before applying, keep every batch exact on every rung a
counter hop uses, and change no setting on either server.

Done 2026-09-28 (`migkit/marks.py`, `migkit/binlog_marks.py`;
`test_two_way_marks_climb_the_ladder.py`, `test_two_way_marks_on_mariadb.py`,
`test_two_way_rungs_are_ranked_by_measurement.py`). The rungs are data -
what each gives, what it needs of the side, how it is proved, what it was
measured to cost - and `choose_rung` is the one place they are ranked,
the seam the decision layer takes over. PostgreSQL: a replication origin
of migkit's (`migkit_twoway_<hop>`, the batch's number handed over as the
origin's position), a transactional logical message (prefix `migkit`),
the table. MySQL: a tagged GTID (the hop's own UUID, tag `migkit`, the
batch's number), a comment on every applied statement read back from the
rows-query event where the server already logs those, the table. MariaDB:
`skip_replication`, the table. The reader of a side knows every rung's
mark at once, so the tail applying into it and the one reading it need
not agree on anything. Measured before anything was claimed
(`bench/marks_probe_pg.py`, `bench/marks_probe_my.py`): `test_decoding`
never prints an origin (14 and 16), and `only-local` has the server leave
those transactions out before decoding; the binary protocol sends an
Origin message on both, and `origin = none` only from 16; GRANT EXECUTE
opens the origin functions on 14 as on 16; a second session cannot take
an origin one holds. MySQL 8.4: the binlog reader cannot open the tagged
GTID event (type 42), so migkit decodes it (a format version, the size,
numbered fields of variable-length integers, the UUID as sixteen of
them); it needs TRANSACTION_GTID_TAG and one of SESSION_VARIABLES_ADMIN,
SYSTEM_VARIABLES_ADMIN, REPLICATION_APPLIER; **a transaction under a GTID
already executed is skipped with its statements answering as though they
ran** - so a number is never handed out twice, and a batch the target
committed whose end this machine did not record stops the tail instead of
being applied again. A statement's leading comment reaches the rows-query
event as sent. MariaDB 11.8: the flag (0x8000) is on every event of the
transaction, and a user with no global privilege may set it. Ranked by
cost measured through migkit's own applier (`bench/marks_cost.py`,
microseconds a one-row transaction against none in the same round, the
median of ten rounds; mark added / reader): PostgreSQL 16 origin 676/600,
message none/625, table 204/612 (none's own rounds spread 476-614) -
taking an origin costs a connection about a millisecond and the applier
opens one a batch, so **the table outranks the origin there** for a hop
that counts, and the message leads for one that does not; MySQL 8.4
gtid_tag 277/155, comment 197/136, table 325/199, all inside the spread
of none's rounds (1274-1840) on a shared machine, so the footprint
decides and the tagged GTID leads; MariaDB 11.8 skip_flag 237/152, table
575/189. Footprint breaks only ties within a brand's spread. Every rung is proved
before it is trusted - a probe through it, read back by migkit's own
reader (a temporary slot on PostgreSQL; the binlog from just before it,
under a replica id of the proof's own, on MySQL) - and the proof paid for
itself at once: the first decoding of the tagged GTID was wrong, the probe
came back as the application's, and the climb fell to the comment (the
cost run showed it too: 300 of 300 carried back). The rung is kept in the
tail's token with the batch being applied (`next`), since an origin or a
GTID says only that a batch was committed, not where it ended; a failpoint
between commit and save under each exact rung adds once. `doctor` names
each side's rung and what it leaves, and for the flag, that the target's
own replicas filtering such events miss migkit's writes and that a
transaction the application marks so itself is left out too; `move --mode
cdc --drop` on a two-way hop takes away what its rung left (the table, the
origins). Left: MariaDB's GTID domain (a domain of migkit's own changes
what the target's own replicas track; not built until measured).

### R4. Avro, schema registries, MSK sign-in (done 2026-09-27, but the
MSK handshake)

Built (`migkit/registry.py`, `migkit/avrostream.py`) and run against
Redpanda's registry (`test_changes_go_as_avro_through_a_registry.py`):
`format: avro` on a Kafka target - Debezium's envelope, the registry's
framing, a tombstone after each delete, a table's schema following what
its changes carry (a new column a new version, which the registry's rule
takes; a change its rule refuses stops the stream and says so); an Avro
topic copied between two clusters with registries of their own, each
schema registered in the target's and the id in every message changed,
compared by the schema's fingerprint; `AWS_MSK_IAM` taken over SASL_SSL.
Found on the way: a whole-cluster copy carried `_schemas`, the registry's
own store, over the target registry's - the internal topics are left out
now. The real MSK handshake stays **blocked** on an AWS account. As
planned:

* A small client for the Confluent registry API (register, get by id,
  compatibility, config) and fastavro for the bodies; the Confluent wire
  format (magic byte and schema id), topic-name subjects, a
  Debezium-compatible envelope, tombstones after deletes; schema changes
  mapped to what the registry's compatibility allows (a new nullable column
  passes, a new NOT NULL column is refused and said). JSON Schema next,
  Protobuf last. confluent-kafka only in the tests, as a byte-for-byte
  check. Tested against Redpanda's registry.
* MSK IAM: the Kafka client migkit already uses signs `AWS_MSK_IAM` itself;
  raise its floor to 2.2.15. Tested offline (the signed payload with frozen
  credentials and clock); a real handshake is **blocked** on an AWS account.

### R5. The control plane (schedule, operations view, approvals, the
record and roles for a shared view done 2026-09-27; report files at rest
and one table across machines todo)

Built and run: `migkit/schedule.py` - a hop's `schedule` fired by
anything on a timer with `MIGKIT_SCHEDULED=1`, or by `report --serve`
(`test_a_hop_runs_on_its_schedule.py`); the dashboard holding a running
tail and letting it go on, only for the page whose address was printed -
a token a start, this machine's own names only, a header only that page
sends (`test_the_dashboard_holds_a_tail_only_for_its_page.py`);
`migkit/approvals.py` - cutover, repair and rollback waiting for the
approvals the hop asks for, signed with the approvers' own SSH keys and
counted only for the request they signed, never the asker's own; and
`migkit/audit.py` - the run's record chained, so an entry changed or taken
out shows in `history` (`test_a_step_waits_for_its_approvers.py`). Roles:
the view shared behind a proxy that signs people in names each in a header
of its own (`MIGKIT_UI_USER_HEADER`, its public name in
`MIGKIT_UI_HOSTS`); a hop's `access` lists its operators and viewers
(shell patterns), a viewer sees it and cannot hold its tail, someone it
does not name does not see it or its report, and each action is recorded
with who took it (`test_the_dashboard_holds_a_tail_only_for_its_page.py`).
Approving from the view is left out on purpose: an approval stays a
signature with the approver's own key, which a proxy's header is not.
Report files at rest, measured before building: 29 places read or write a
drilldown file across the engines, plus the undo files, the snapshots'
copies and `conflicts.jsonl` - one module (`evidence`) that every one of
them goes through comes first, then age recipients (pyrage) and a keyed
HMAC for what a shared report shows. As planned:

* **Schedule:** a `schedule:` block in the hop (cron, time zone, catch-up
  window, maximum duration including retries, a retry window bounded by
  time as DTS bounds it, skip on overlap, pause after repeated failures);
  croniter parses the expression; the run fires only on the machine holding
  the lease.
* **Operations view:** the dashboard grows actions (hold, resume, drain to
  a position, abort, approve a cutover when lag and the last verify allow
  it), each written as an intent into the run's state for the lease holder
  to pick up and audit - the view never touches a database. Bound to
  127.0.0.1, a token per start, host allowlist, CSRF on every POST.
* **Roles and approvals:** viewer, operator, approver from an `access:`
  block; approvals where the hop asks for them (count, not the requester,
  expiry, bound to the plan's hash), signed with the approvers' SSH keys
  (`ssh-keygen -Y`), or clicked in the view behind the team's proxy.
  pycasbin underneath.
* **Audit:** hash-chained JSON lines, signed checkpoints, optionally
  anchored in an object-locked bucket; who, what, when, from where,
  approved by, plan hash.
* **One table across machines:** a lease per chunk in the shared bucket
  (conditional writes, a fencing token), work stealing by splitting the
  largest chunk in flight at its last checkpointed key. Looked at
  (2026-09-27) and left: the checkpoint the machines share is written
  whole, not over the version read, so two machines finishing ranges of
  one table would each overwrite the other's - the checkpoint's own
  conditional write comes first; and one machine already copies a table
  in as many processes as the sizing gives it.
* **Report files at rest:** masked (keyed HMAC, so equal values still
  match) and then encrypted to recipients the hop names (age, through
  pyrage).

### R6. The network path (done 2026-09-27)

Built: `migkit/tunnel.py` (an endpoint's `tunnel: {ssh: ...}` or
`tunnel: {command: ...}`; up once its port answers, started again when it
dies, closed at the end; a tunnel that cannot come up says why before
anything is copied - `test_a_server_behind_a_bastion_is_reached.py`, an
SSH bastion container), `Engine.link_probe` in `doctor` (measured: through
a path dropping packets over 1,200 bytes, a reply of 512 bytes came back
and one of 1,200 never did - `test_doctor_tells_a_stalling_link_apart.py`)
and `migkit/stall.py` (a copy that moves nothing for
`MIGKIT_STALL_SECONDS` is ended - `test_a_link_cut_mid_move_is_survived.py`).
The clouds' forwarders go through `command:`; their own sign-in is
blocked on accounts. As planned:

* A tunnel supervisor that ends every path in a local port: the system's
  ssh (`-L`, jump hosts and keys from the operator's own config), asyncssh
  where there is none, and the clouds' own forwarders (SSM port forwarding,
  IAP, the Cloud SQL proxy, Azure Bastion) as programs it starts; up only
  when a database-level ping answers; a pool of tunnels for many workers;
  restarted with backoff, and the copy retried a range at a time.
* `doctor` tells "connects but bulk stalls" apart: a ladder of incompressible
  payloads in both directions (512 bytes to 64 KB, five seconds each) that
  names a path-MTU black hole and its direction, a throughput probe, and an
  idle probe (a NAT gateway resets after 350 s, SSM after its idle timeout).
  While a copy runs, a stalled stream is cancelled from a side connection
  and its range copied again.
* Tested: an SSH bastion container, an MTU black hole made with iptables in
  the database's network namespace.

### R7. Failure made on purpose (item 44) (done 2026-09-27)

Built and run: failpoints at every place a move keeps what it has done,
killed at the first and a later hit, PostgreSQL to PostgreSQL and MySQL to
PostgreSQL - 15 stops, each ending equal after a run again; the same as a
property (a place, a hit, a way of stopping and writes to the source drawn
at random); the link to the target cut, reset and held; the target turned
read-only and its disk filled; migkit's own disk filled; the source failed
over to its promoted standby mid-move. Found on the way: the PostgreSQL
table copier waited for good on a target that refused the copy (the
source's side of the pipe had nowhere to write), and a read-only or full
target stopped a move saying only the server's words - both fixed. Tests:
`test_a_move_stopped_anywhere_ends_equal.py`,
`test_a_link_cut_mid_move_is_survived.py`,
`test_a_move_meets_a_target_that_changes_under_it.py`,
`test_a_source_that_fails_over_mid_move.py`. As planned:

* Failpoints named by an environment variable (`MIGKIT_FAILPOINT`) at every
  durable boundary: snapshot taken, range committed before its checkpoint,
  checkpoint written, verify after a table, tail position saved. Each is
  crashed at its first, a middle and its last hit, the move run again, and
  the target compared by content; a crash during the resume too.
* A Hypothesis state machine: writes to the source (key changes, wide rows,
  NULLs, unicode), a move to a failpoint, a crash, a resume, a fault on the
  link; at rest the target equals the source.
* The link cut mid-move through Toxiproxy (its HTTP API; the image is
  multi-arch): a cut after so many bytes, a reset, a link that holds and
  passes nothing (which the stall watchdog must catch), a refused
  reconnect. Also an L3 cut, and a hung server (`docker pause`).
* The source failed over mid-move (PostgreSQL standby promoted behind the
  proxy; MySQL replica promoted), the target turned read-only mid-load, the
  target's disk and migkit's own disk filled (bounded volumes only).
* Kept out of the default run under a `chaos` mark.

### R8. Verification that is not migkit's own (mostly already there)

Checked before building (2026-09-27): Google's validation tool is already
wrapped - it is migkit's second reader (`migkit/second_reader.py`, in an
environment of its own that `doctor --install` makes), for PostgreSQL,
MySQL and SQL Server. Still open: the engines it would matter most for
(Oracle, Db2, Snowflake, BigQuery) - **blocked** on an x86 runner and
accounts; MongoDB's migration verifier - a program downloaded from its
releases, **waiting on the owner's go-ahead** to download it; explaining a
narrowed mismatch column by column. As researched:

* Google's validation tool as the second opinion where migkit reads a
  database only through the pair (Oracle, Db2, Snowflake, BigQuery): run in
  an environment of its own, per key-range partition, results as JSON,
  never its database result handler; reported as "equal after its
  normalisation" (it trims and replaces NULLs).
* MongoDB's migration verifier with its metadata on a local server, never
  the destination.
* A mismatch narrowed to one range explained column by column (datacompy on
  Polars frames, with tolerances).
* Veridata's "in flight" answer built into migkit's own verify: rows that
  differ are queued, re-read after the replication delay, and reported as
  in sync, in flight or persistently different.

### R9. Assessments (done 2026-09-27, but free space and DocumentDB)

Built and tested (`test_assess_names_what_slows_or_changes_a_load.py`):
standbys streaming from a PostgreSQL target and replicas reading from a
MySQL one (semi-synchronous said), MySQL stored code written under another
collation than the target database's, and key-less MySQL tables with JSON,
spatial or large columns named as the ones a replica applies by reading
whole. Still open: free space, which neither server reports over SQL
(the cloud's metrics are **blocked** on an account), and the DocumentDB
checks (**blocked**, no DocumentDB here). As planned:

* Free space: the target's needed size from the source; free space where
  the server can say (`system_stats` on PostgreSQL where installed) or the
  cloud can (RDS `FreeStorageSpace`, Aurora `FreeLocalStorage`); and the
  source's own space for the log the slot will hold during the load.
* HA or read replicas on the target during the load (`pg_stat_replication`,
  synchronous standbys, MySQL replicas, group replication, Aurora
  replicas); AWS advises them off until cutover.
* Key-less tables with columns a subscriber cannot match (PostgreSQL `json`,
  `point`, `box` under `REPLICA IDENTITY FULL`; MySQL's whole-row hash scan).
* Collations mixed inside stored code: each routine's creation-time
  collations from the catalogue against the target's; COLLATE clauses found
  by parsing the routines (sqlfluff parsed T-SQL, PL/SQL and MySQL bodies
  where sqlglot lost them).
* DocumentDB: change streams enabled per collection (`$listChangeStreams`),
  the retention setting, and DDL seen by comparing catalogue snapshots.

### R10. Large values (sizing, and PostgreSQL's large objects in pieces,
done 2026-09-27; MySQL's and Oracle's large columns in pieces todo)

`assess` sizes them: PostgreSQL's large objects and out-of-line values
against the database's size, MySQL's large columns measured on a sample
and scaled, with the largest value seen and where. PostgreSQL's large
objects now follow a whole-database table copy under the same oids, read
and written 8 MB at a time (`lo_get` and `lo_put` with an offset), one
already there compared piece by piece and written again only where a piece
differs, its owner given where the target has the role - the table copier
had carried the `oid` in each row and not the object, and the deep check
found every reference dangling
(`test_large_objects_are_carried_in_pieces.py`). Still to: a MySQL or
Oracle column value larger than a read should hold, read by `SUBSTRING`
or LOB locator in steps and appended on the target. As planned:

* Sized before the move per engine (PostgreSQL large objects and TOAST,
  Oracle LOB segments, SQL Server LOB allocation units, MySQL sampled
  lengths) and turned into a time estimate (values x chunks x RTT plus bytes
  over bandwidth).
* Read in pieces rather than whole: `lo_get` with offsets, Oracle LOB
  locators read by chunk size, MySQL `SUBSTRING` in steps; the planner
  routes a table with large values through that reader.

### R11. Stored code converted and proved (todo; Oracle part waits on R12)

* Ora2Pg run as a program for Oracle, MySQL and SQL Server sources, its
  cost units turned into the effort estimate (item 40).
* Behaviour proved by a differential harness: inputs generated from each
  routine's parameters and real rows, the routine run on both sides in a
  transaction rolled back, results, errors and the digests of the tables it
  touched compared; kept as tests on the target.

### R12. Engines the sandbox can now run (todo)

* **Oracle Free runs natively on arm64 from 23.5** (the slim image). It is
  capped at 2 GB of memory and 2 cores; the VM needs 4 GB or more. This
  unblocks item 11's run against a server, LogMiner (item 11.iii), users by
  `DBMS_METADATA`, statistics, restore points - to be tried within the
  VM's limits.
* **MinIO's images left Docker Hub on 2026-09-11.** The object store tests
  use a copy cached here; VersityGW (arm64, versioning) replaces it before
  it is needed again.
* SQL Server: the 2022 image runs only under Rosetta, which colima does
  not use - but SQL Edge (`azure-sql-edge`, retired and still runnable)
  runs on arm64 and is what the tests use; found its checksum's blind
  spot and carried its logins with it (2026-09-27). What only the full
  server has (Agent jobs, CDC proper rather than Change Tracking) still
  waits on an x86 runner (item 27).

### R13. The capability matrix, engine by engine (in progress)

Done (2026-09-27):
* Kafka: SCRAM users and ACLs compared, carried and taken back by their
  full key (`test_kafka_users_and_acls_are_carried.py`); a partition that
  differs while the tail is behind confirmed by waiting on the tail and
  comparing up to the same message, what follows it on the target held to
  what the source wrote after (a stray message stays a difference); the
  snapshot records ends, group offsets and topic settings
  (`test_a_kafka_target_that_follows_the_source.py`).
* ClickHouse: each server sums a hash of its rows per partition (the
  target grouped by the source's partition key through `partitionId`, a
  NULL and the word NULL told apart) and only a table whose sums differ is
  read row by row; delta compares only partitions whose parts changed on
  either side; settings both sides; roles, users (password as its hash
  where the server shows it, else from the passwords file) and grants;
  the snapshot freezes every target table
  (`test_clickhouse_to_clickhouse_is_compared_on_the_servers.py`). Still
  to: the bulk path as the target pulling from the source (`remote()`,
  no row through migkit), a follow by changed partitions.
* SQLite and DuckDB: a bulk path of the engine's own (`native_bulk`, the
  move's `native` path, chosen where the engine has one): the source
  attached read-only and each table put in by one `INSERT ... SELECT`
  inside the database; filtered tables still go to the table copier,
  shaped the source's way first. 500,000 rows: SQLite 10.3s to 1.1s,
  DuckDB 115s to 4.5s (`test_a_file_database_moves_by_its_own_means.py`).
  DuckDB: sequences compared by their next value (read from the START the
  file keeps - `last_value` means two things), indexes, keys and views
  left behind named, a snapshot copied by DuckDB itself
  (`test_duckdb_to_duckdb_keeps_what_a_row_copy_leaves_behind.py`).
  Found on the way, all measured and fixed: the table copier ran ranges
  side by side into a SQLite file (`database is locked`); a same-engine
  DuckDB or ClickHouse table was built with every column text; DuckDB's
  table copier wrote by `executemany` (Arrow now, 115s to 37.8s); a DuckDB
  source was opened for writing, so writes in its log were checkpointed
  into it on close; SQLite's counts ignored the hop's row filter.
* Parquet: part files copied as they are (the store copies them between
  two S3 locations of its own); a snapshot keeps the target's files and
  their sizes. A follow is not applicable: files at rest keep no log.
* Redis: verify of only the keys the source wrote since the last cycle,
  told by the source itself (`CLIENT TRACKING ... BCAST`, redirected to a
  listening connection over RESP2 - over RESP3 nothing was heard): the
  first cycle is the baseline, a key that differed is asked again until it
  matches, a flush compares the keyspace whole, and a listener cut off is
  an error - the client reconnects it under another id the tracking no
  longer sends to, and the server's own tracking info still named the old
  one, so the id is asked for every second
  (`test_redis_verifies_only_the_keys_written.py`).
* DynamoDB: the table's stream is its change log (`neutral_changes`):
  shards read parent first, the item as the stream's image or read again
  where it keeps keys only, a stream turned off and on refused, one that
  is off said and never turned on. Through it: the follow, the fence, the
  confirm, the verify of only what changed - and DynamoDB as a source of
  any pair (into PostgreSQL, tested). Settings per table (key, billing,
  stream, encryption, class, indexes, time to live, recovery; what the
  endpoint does not answer said as such), a backup as the snapshot where
  the service takes one, and a whole move by a parallel scan writing the
  items back unchanged with the source's key and secondary indexes. Maps,
  lists and sets cross whole (`RawAttr`), where the copier had refused the
  table (`test_dynamodb_follows_through_its_stream.py`).
* SQL Server (on SQL Edge): logins with their password hashes and SIDs,
  server roles, database users joined to them, roles and permissions,
  carried and taken back; a table that differs while the tail is behind
  confirmed by waiting on it and asking the rows again; the check's sum of
  `BINARY_CHECKSUM` replaced by each row's `FOR JSON` hash - it had passed
  tables whose xml or text columns differed on every row
  (`test_sql_server_sums_what_each_row_is.py`,
  `test_sql_server_follows_through_change_tracking.py`).
* OpenSearch: documents copied as they are held (`_source` and `_id`) by a
  sliced scroll, a slice per worker, into an index made with the source's
  mappings and analysis, loaded with no replica and no refresh and given
  both back after - where the table copier had refused every index on its
  `_id`; the cluster's and each index's settings compared, analysis as
  behaviour; a snapshot taken by the cluster into a file repository under
  its `path.repo` (said where it has none); a verify of only the documents
  written since, by each primary shard's sequence numbers, a delete found
  by the deleted count moving and the ids compared
  (`test_opensearch_to_opensearch_keeps_documents_as_they_are.py`). Still
  to: the follow (the cross-cluster replication plugin, or the same
  sequence numbers), security roles (a secured cluster in the sandbox).
* Cassandra: roles carried with their salted hashes (`HASHED PASSWORD`,
  4.1), memberships and permissions, the keyspace renamed as the target
  calls it, and taken back; the server's settings, the keyspace's
  replication and every table's options compared (a default time to live
  or gc grace that differs is behaviour); the target's tables recorded
  before a repair, a node's own snapshot said as out of reach
  (`test_cassandra_roles_and_settings_are_carried.py`). Found: the table
  copier carries neither a row's remaining time to live nor its write
  time, so a row that expires on the source lives on the target. The
  move's own path now copies by token ranges side by side, each column
  written back `USING TTL ... AND TIMESTAMP ...` as `TTL()` and
  `WRITETIME()` read it (a collection, which has no single one, as now),
  into a table made by the source's own definition; a counter table is
  refused by name. Still to: a deep check that samples rows with a time to
  live on both sides, the follow through ScyllaDB's CDC tables.

What each still-missing cell can be built on, where a local image exists:
* ClickHouse: caught up when its replication, mutation and distribution
  queues are empty; verify by partition fingerprints; users from
  `SHOW ACCESS`; a snapshot by `BACKUP`; changed settings from its system
  tables.
* DynamoDB (Local): follow through Streams; a sentinel item as the fence;
  change-only verify by the keys in the stream; settings from
  `DescribeTable`.
* OpenSearch: follow by polling sequence numbers per shard plus an id-set
  diff for deletes; snapshots to a file repository; security roles; changed
  settings.
* Cassandra and ScyllaDB: follow Scylla's CDC log tables; a sentinel and
  `writetime()` as the fence; roles with their hashed passwords; snapshots
  through `nodetool`; settings from `system_views.settings`.
* Kafka: ACLs and quotas; SCRAM passwords cannot be exported and are said
  so; a rollback point as recorded end offsets; changed configs.
* Redis: `ACL LIST` with its hashes; `BGSAVE` as the snapshot; `CONFIG GET`;
  an RDB from Redis 7.4 or later refused by Valkey, said before a move.
* SQLite: the session extension for changes (through APSW), the backup API
  as the snapshot, pragmas as settings, `ANALYZE`.
* Parquet on object storage: manifests and footers as the fence, versioning
  as the snapshot, only changed objects verified.
* Snowflake, BigQuery, Redshift: only emulators with gaps exist; **blocked**
  on accounts for anything the emulators lack.

### R14. Measurements owed (done 2026-09-27)

mydumper 1.0.5, a million rows, four threads: it splits a table into as
many files as threads by itself; `--rows 50000:200000:0` and `--rows 20000`
(51 files) loaded in 3.3s to 4.5s against 3.4s to 3.7s without, within the
noise - so the bulk path is left as it is. Its own data checksums
(`--checksum-all` with the loader's `--checksum fail`) scan both sides
again for what `_held_to_the_source` already compares; not added.
`--machine-log-json` was already read. mongodump 100.16: `--query` needs
one collection and cannot take `--oplog`, which the MongoDB path already
refuses to push a filter it cannot keep consistent into. As researched:

* mongodump: `--query` needs one collection and cannot take `--oplog`; a
  filtered dump is consistent only with a change stream opened before it.
* mydumper 1.0.x: adaptive `--rows`, `--machine-log-json` to read instead of
  its text, `--checksum-all`; myloader's resume file is written only on a
  clean stop, so migkit keeps its own chunk ledger for the bulk dump.

* Proving a copied PostgreSQL range (2026-09-27): read back and held to
  what was sent, or digested on both servers so two numbers cross the
  link. Measured, 400,000 rows: 1.9s and 2.4s beside the target, 2.5s and
  3.2s 10 ms away with bandwidth unbounded, 4.4s and 4.2s with each
  connection held to 20 MB/s - no one way wins everywhere, so the copier
  tries each once, times it per row, and keeps the cheaper.

### R15. DuckDB as an engine of its own (done 2026-09-27)

Built (`migkit/engines/duckdb.py`) and run: a PostgreSQL table of every
common type moved into a DuckDB file and back, read back batch by batch,
checked equal, and a value changed by 1e-9 in the file found
(`test_duckdb_is_a_side_of_a_pair.py`). Found on the way, and fixed for
every engine rendered in this process: JSON had no rendering here (now
`jsonb`'s form, which MySQL's matches), and an instant with its zone was
written at its own zone where the SQL renderings write UTC. And DuckDB's
own: a cursor is a connection of its own and did not carry the session's
time zone - 17:00 UTC written through one landed as 00:00.

**R15. What the DuckDB engine does not do yet.** Its deep checks, carrying
sequences, comparing settings, a bulk path of its own and snapshots - the
cells `migkit doctor` names as not yet built for it.

Asked by the owner (2026-09-27): Parquet is supported, so is DuckDB? Only
half: the Parquet migkit writes is standard (Arrow; decimals as
decimal128/256) and DuckDB reads it as it is, and a DuckDB database can be
compared through the `generic` engine - but not moved into or out of with
a resumable, checked copy, and nothing had been run against DuckDB at all.
To build: a DuckDB engine as SQLite is one - a file, no server - with the
neutral read, write, key and digest (the `duckdb` library, MIT, arm64
wheels), so any pair can move and verify through it; and DuckDB as the
second reader of R8, for Parquet, PostgreSQL and MySQL. Tested in-process,
no container.

### R16. Vectors and graphs (a: this round; b, c: deferred)

Asked by the owner (2026-09-27): do vector and graph databases need
support - does anyone support them? Hardly: the general tools (DMS and the
other clouds' services, Debezium, Striim) take neither as a source; DMS
writes into Neptune as a target only; Airbyte loads Pinecone, Qdrant,
Weaviate and Milvus for retrieval, which is not a checked move; the moves
that happen go through each vendor's own tool (Qdrant's migration tool,
`neo4j-admin dump/load`). So b and c wait until the rest is done. Found on
the way to the answer: a pgvector column (`vector`,
`halfvec`, `sparsevec`) has no canonical rendering, so `check` leaves it
out of the comparison - a wrong vector would pass. So:
* **a (done 2026-09-27 for pgvector; MySQL 9 and MongoDB still to):**
  `test_vectors_are_compared_value_for_value.py`. Vectors inside the
  engines already supported -
  pgvector's types, MySQL 9's `VECTOR`, arrays of floats in MongoDB -
  copied and compared value for value, float32 exactly.
* **b (deferred): Qdrant as an engine** (arm64 image; its Python
  client, Apache-2.0): a collection's configuration carried (size,
  distance, named and sparse vectors, HNSW, quantization, payload
  indexes); points read by id in order, so a copy resumes; a digest of each
  point's id, vector bytes and payload; and a check no row comparison
  gives - the same nearest-neighbour queries asked of both sides and their
  top results compared (recall at k), since an index built again answers
  differently from the same data.
* **c (deferred): Neo4j as an engine** (arm64 image; its driver,
  Apache-2.0): nodes by label and relationships by type, placed by a
  business key per label (a node's own id does not survive a move), the
  schema's constraints and indexes carried, counts per label and type,
  property digests per label, the endpoints of every relationship
  compared; the bulk path its own dump and load.

### R17. The path between the databases, and what it carries (a, b and
c done 2026-09-27; d begun: the read beside the source)

Asked by the owner (2026-09-27): two-way tunnels and fewer hops, a
protocol of migkit's own for speed, and the data kept from anyone on the
way - should migkit, and how do others do it? Researched:

* **Nobody writes their own cryptography.** DMS, DTS, Qlik, HVR,
  GoldenGate, Striim, Google's and Azure's services all ride on TLS or SSH
  (Qlik: its own channel, but Diffie-Hellman and AES-256 underneath). The
  private side dials out: reverse SSH (Fivetran, Google), paths the target
  opens (GoldenGate), an agent on 443 only (Azure, DTS's gateway).
* **The fast ones put an agent on each side:** capture next to the source,
  apply next to the target, one compressed, checkpointed stream between
  (HVR: "commonly 10x or higher" compression; GoldenGate: "typically at
  least 4:1"; Qlik: files over several streams). A chatty database
  protocol pays the round trip per statement; a bulk stream does not.
  Resume is the same everywhere: the sender keeps what the receiver has not
  acknowledged, the receiver checkpoints what it applied, apply is
  idempotent per piece.
* **The physics:** one TCP flow is capped near MSS/(RTT x sqrt(loss));
  OpenSSH's fixed 2 MB channel window caps one tunnel at window / RTT
  (about 20 MB/s at 100 ms); several independent flows scale until the
  bottleneck fills; ControlMaster puts everything on one flow. WARP's
  tunnel MTU is 1280 (the link probe already finds the black hole).
* **What not to build:** a transport cipher, QUIC in Python (aioquic is
  the slowest measured), WireGuard in user space (a Go sidecar, poor single
  flow), Noise (unmaintained since 2020).
* **At rest:** `cryptography` 50 ships Cobblestone (C2SP chunked AES-GCM,
  16 KiB chunks, truncation and reorder detected, about 0.1% overhead) -
  checked installed here; age through pyrage when a file must open with a
  person's SSH key. A report shows a keyed HMAC of a value, never an
  unkeyed hash (a phone number's hash is a dictionary lookup away).

Done (a, 2026-09-27, `test_doctor_says_what_carries_each_leg.py`):
`doctor` says of each side off this machine whether its connection is
encrypted, as the database itself reports it (`pg_stat_ssl`, the session's
`Ssl_cipher`), or that a tunnel carries it; and, both round trips
measured, where this machine sits - far from both, every row crosses a
wide network twice. Found on the way: no connection migkit opened took a
TLS setting at all - PostgreSQL's were libpq's default (TLS where offered,
nothing checked) and MySQL's the client's (the same, measured on 8.4), so
a certificate from anyone was accepted. An endpoint's `sslmode`,
`sslrootcert`, `sslcert`, `sslkey` reach every PostgreSQL connection and
every program on libpq (its environment); `ssl_ca` (then the certificate
and the name on it are checked), `ssl_cert`, `ssl_key`, `ssl` reach every
MySQL connection. Still to: the MySQL bulk programs' own TLS flags,
MongoDB's and Redis's word on their connections.

Done (b, 2026-09-27, `test_what_holds_values_is_kept_encrypted.py`): a
hop's `at_rest.recipients` (SSH or age keys) makes the files that hold the
application's values - a drilldown's keys, the undo statements and rows,
a two-way tail's conflicts - age files to them, read back with the
operator's own key (`MIGKIT_IDENTITY`). Every place that reads or writes
one goes through the hop's report directory, so the path itself encrypts
(`migkit/evidence.py`): the 29 places did not change. A record added to a
line at a time is sealed a line at a time. A key that is not a recipient
reads nothing; a file written before the hop asked reads as it was. Still
to: the keyed HMAC for what a shared report shows, and what a program
migkit drives writes itself (a dump's files: an encrypted disk for now).

Done (c, 2026-09-27, `test_a_server_behind_a_bastion_is_reached.py`):
where the round trip to the bastion (a TCP connect, the least of three)
says one SSH window cannot carry 100 MB/s, as many ssh connections as it
takes - never more than the run's workers - each on a port of its own,
behind a splitter on the endpoint's port that hands each new connection to
the next; one where the bastion is near, so nothing changes there.
Measured honestly: over a 50 ms path (toxiproxy), 400,000 rows took 28.3s
through one leg and 27.2s through four - the copier's own round trips (the
rows written, then read back) are that path's limit, not the window; the
legs pay where one stream is held to the window, as a dump's is.

Begun (d, 2026-09-27): the reader beside the source, without an agent of
migkit's own. Where the source is reached through an ssh tunnel whose
machine has psql and zstd, the PostgreSQL copier's rows are read there,
next to the database, and cross the link once, zstd-compressed, inside
the ssh connection; the password goes on the read's standard input, and
the read's own exit status comes back on its error stream (a pipe's is the
compressor's). Measured, 200,000 rows over a link held to 5 MB/s and
10 ms: 7.1s as COPY's text, 4.3s read beside the source
(`test_a_server_behind_a_bastion_is_reached.py`). Still to: the writer
beside the target, and the engines whose reader is a Python driver - the
agent proper.

To build, in this order:
1. **a. Say what each leg is:** whether the connection to each database
   is encrypted, asked of the database itself (PostgreSQL `pg_stat_ssl`,
   MySQL `Ssl_cipher`, MongoDB's TLS state), in `doctor` and `assess`; and
   where migkit runs - both legs far away means every row crosses the wide
   network twice, said with the measured round trips, as AWS says to put
   the replication instance next to the target.
2. **b. Files at rest:** the files that hold row values (undo files,
   drilldown lists, snapshots, spill) written through one place that
   encrypts them to the recipients a hop names (`at_rest: [ssh-ed25519
   ...]`), readable by migkit with the operator's own key; the keys and
   values a shared report shows masked by a keyed HMAC, equal values still
   equal.
3. **c. Several tunnels, sized:** one SSH connection per few workers
   rather than one for all, connections spread over them by a local
   splitter, the count taken from the link probe's round trip and loss
   (never configured), the SSH window sized to the path.
4. **d. The relay:** a reader next to the source and a writer next to the
   target, started by migkit over SSH (the module, not a new command),
   frames of a chunk with a sequence number and a digest, zstd-compressed,
   over several TLS 1.3 connections with certificates made for the run and
   held in memory; the writer lands each frame in an encrypted spool,
   acknowledges it, applies it idempotently and acknowledges again; either
   side may dial. Tested in docker with netem delay and loss and toxiproxy
   cuts: bytes on the wire, MB/s, CPU per side, and a zero diff. Expected
   2-4x at 30-60 ms, more on lossy paths or where migkit ran off-path.

### R18. Libraries and techniques the whole of migkit can stand on
(researched 2026-09-27; one finding fixed the same day)

Asked by the owner (2026-09-27): as DuckDB came in to help, what else -
tools, libraries, databases, caches - for correctness and for reading and
writing fast? Researched:

* **How the fast ones are fast.** PeerDB: ctid ranges under one exported
  snapshot, binary COPY, a 1 TB table in 1h50m on 8 threads against 17h
  for dump and restore - and slower on 16, the network full. Airbyte's
  2025 speed-up: one table read by many queries at once first, then
  protobuf instead of JSON (JSON was "the final bottleneck"). CloudQuery:
  batches of rows rather than a message a row. MySQL Shell and mydumper:
  zstd over gzip, about twice as fast. Debezium: one task per PostgreSQL
  connector, incremental snapshots an order slower than plain ones.
* **Digests.** A range digest is (count, sum of a k-bit row hash mod 2^k):
  a difference is missed with odds about 2^-k per comparison whatever the
  row count. **XOR is not a digest:** two equal rows cancel. Sums can be
  kept current per change (subtract the old row's hash, add the new).
  Never persist an engine's internal hash (polars' changes by version).
* **Candidates** (licence, arm64 and 3.13/3.14 wheels checked on PyPI):
  pyarrow and ADBC for PostgreSQL (NUMERIC comes back as text), mssql-python
  (Arrow bulk copy), python-oracledb (Arrow fetch; a known bug drops rows
  in DATE columns), hiredis (redis-py uses it when present), confluent-kafka
  (about twice kafka-python), xxhash (XXH3 tens of GB/s against MD5's 0.6),
  pyroaring and rbloom for key sets, zstd (stdlib in 3.14, backports.zstd
  before), msgspec and pickle 5 out-of-band buffers over shared memory
  between workers, DuckDB for an out-of-core key diff (pinned; a spilled
  join on its main branch drops rows, `join_filter_pushdown` off). Not:
  diskcache (unmaintained), pglast and mysqlclient as defaults (GPL),
  PyMongoArrow for MongoDB to MongoDB (slower), ConnectorX as a general
  reader (drops time zones), free-threaded 3.14 (too few wheels).
* **A correctness spec to test against:** "Generalized DBLog" (2026) - no
  change falls through a gap between chunk copies and the log, and an
  older copied row never overwrites a newer change or brings back a
  deleted row.

Found and fixed on the way (2026-09-27): **MySQL's checksum folded rows
by BIT_XOR**, so a table with no key holding one row twice on the source
and another row twice on the target passed as equal - rows and checksums
alike - and the column fingerprint missed a column changed to the same
value on two rows. It sums now (an exact DECIMAL), and a checkpoint from
the XOR days is not mixed in (`test_the_mysql_table_copy_checks_by_range`:
both fail on the XOR fold). **Still open, blocked with SQL Server (R12):**
its check sums `binary_checksum(*)`, which skips text, ntext, image and
xml columns and collides on some string changes. Done the same day, on
SQL Edge (arm64): measured, a table whose xml or text column differed on
every row passed as equal; the check now sums each row's SHA-256 of its
`FOR JSON` - the drilldown's own hash - as a decimal
(`test_sql_server_sums_what_each_row_is.py`, which also carries logins
with their hashes and SIDs, users, roles and permissions).

To build, in the order they pay:
1. **Pass-through where both sides are the same engine:** MongoDB done
   (2026-09-27, `test_a_collection_copies_as_its_bytes.py`) - raw BSON,
   unordered inserts into the collection the copy emptied (a duplicate
   after a stop replaced instead), and the resume past the last `_id` by
   the index where every `_id` is of one type (BSON orders by type first,
   so one type at both ends is one type throughout): 200,000 documents in
   0.7-1.0s against 3.4s. Already so before: Redis (DUMP and RESTORE),
   PostgreSQL's table copy (COPY's text piped from one server to the other,
   which the read-back tallies - binary would give that up).
2. ~~**hiredis**~~ measured and not adopted (2026-09-27): installed,
   redis-py packs every command through it, which encodes text strictly -
   a key that is not UTF-8 (read with `surrogateescape`) stopped the
   keyspace copy with `UnicodeEncodeError`; `test_a_keyspace_copied_key_
   for_key` failed. The client now packs commands itself whatever is
   installed, so a machine that has hiredis for another reason copies
   such keys too. With that, 200,000 keys copied in 3.7s without it and
   3.4-3.7s with it: not worth a dependency. **zstd** for spill files and
   the relay (R17d).
3. ~~**The row hash in this process** on XXH3~~ measured and not
   adopted (2026-09-27): folding 200,000 rows took 0.45s, of which the
   rendering 0.36s and MD5 0.06s - and the fold is held to the digests the
   SQL engines compute themselves, in MD5, which no other hash would meet.
4. ~~**An out-of-core key diff**~~ looked at and not built (2026-09-27):
   every drilldown already stops at a cap (20,000 rows; 2,000,000 on SQL
   Server's) and says so, and a table that differs past it is copied again,
   a range at a time, not repaired row by row.
5. ~~**The chunk and change interleave**~~ property-tested (2026-09-27,
   `test_a_copy_and_its_changes_interleave_safely.py`): the source's
   writes, the chunks falling between them, the batch edges and a stop that
   reads again from behind the saved position all chosen by Hypothesis, 400
   interleavings through migkit's own applier - the target ends as the
   source every time; the test fails when deletes are dropped or when an
   update that moves a key no longer leaves its old address.
6. **Arrow batches** between readers and writers where a reader gives them
   natively (PostgreSQL through ADBC, SQL Server, Oracle, ClickHouse),
   with per-column guards for the known losses.
7. **confluent-kafka** in place of kafka-python, lz4 or zstd on produce.

### R19. Faster than the fastest, without giving up a row (added
2026-09-28, the owner: "can we beat them with logic, technique or
process, at no cost to correctness or idempotence?")

Yes, on both the bulk and the changes, and none of it is a new way to
write rows faster than the server can - it is work not done, bytes not
sent, round trips not made, and verification done while it is nearly
free. Each lever names why it is faster, why nothing is lost, and what
it is measured against before it is claimed.

**Bulk**

1. **Physical where the source allows it, logical everywhere else.** A
   base backup (`pg_basebackup` with server-side zstd and streamed WAL,
   MySQL's `CLONE INSTANCE FROM` 8.0.17+, XtraBackup, Mongo's file
   snapshot with `--oplog`) copies the pages, indexes included - no row
   decode, no index rebuild, no verification of rows needed for the
   bulk (block checksums prove the pages). Then the tail from a slot
   made *before* the backup, fast-forwarded to the backup's own end
   position (which the restored target knows: its control file's
   checkpoint, the clone's `gtid_executed`, the oplog's last entry), so
   nothing is applied twice and nothing is missed - exact. Where the
   cloud gives a storage snapshot (an RDS snapshot copied across
   accounts, an Aurora clone, an EBS or ZFS snapshot), the same, and a
   terabyte lands in minutes. A rung of the decision engine, chosen only
   when the same major version and the privilege are there
   (`REPLICATION`, `BACKUP_ADMIN`); refused, not guessed, on a managed
   source that blocks it. Measured against pgcopydb on the same pair.
2. **Bytes not sent.** The relay beside the source reads the rows there
   and sends them compressed (zstd, built; 3-5x fewer bytes on ordinary
   rows), and several TCP legs on a long link (built) - where the
   source's egress or the link's window is the limit, as it was for
   PeerDB's 150 MB/s, that limit moves by the compression ratio. Binary
   COPY, not text, on both ends (pgcopydb sends text unless asked).
   Measured against pgcopydb over a link with RTT and loss added.
3. **Rows not copied.** A run after a rehearsal, a retry after a stop
   or a re-sync before cutover copies only the ranges whose digest
   differs (built for the copier and the bulk paths): the others re-do
   the full load. Extended to the physical rung by page-level
   comparison where the engine exposes page checksums.
4. **Verification while it is nearly free.** A range's digest is read
   on the target right after its COPY, while its pages are still in the
   cache, and on the source from the same rows just read - so verifying
   as it lands costs a fraction of verifying after, which is what
   Veridata, DVT and `pgcopydb compare` do, cold. Measured: verify-as-
   it-lands against verify-after on the same table.
5. **Parallelism that does not collapse.** Workers grown while rows a
   second rises and cut back on the source's own stress (built, R1);
   pgcopydb and MySQL Shell run a fixed number of jobs, which on a
   shared server is either too few or the cause of its own slowdown.
   Per table, the split by key quantiles or ctid so no worker idles on
   a short table while one carries a long one (built). Indexes deferred
   only where measured faster (PostgreSQL), built in parallel while
   later tables still copy, loaded in key order where the engine
   rewards it (MySQL).
6. **LOAD DATA LOCAL made safe.** Measured 36% faster on MySQL and set
   aside because `local_infile` lets the server ask the client for any
   file. The client answers only with the chunk migkit prepared and
   refuses every other name - the same guard MySQL Shell's copy uses -
   so the 36% is taken.

**Changes**

7. **The writer beside the target, the reader beside the source** (R17d).
   The tail's apply runs on an agent next to the target and receives
   whole batches compressed; the reader runs next to the source. Each
   side talks to its database at LAN latency, and the RTT of the link
   is paid once a batch instead of once a statement - which is what
   DMS's single replication instance pays, and why its latency climbs
   with distance. Exactness unchanged: the batch's mark and number
   travel with it.
8. **Changes not applied.** During the copy, a change to a key in a
   range not yet copied is dropped - the copy will read that row's
   later state anyway - and only changes to ranges already copied are
   applied (Netflix's DBLog watermark, Debezium's incremental snapshot
   buffer; migkit's interleaving invariants are already held by
   `test_a_copy_and_its_changes_interleave_safely.py`). On a write-heavy
   source this removes most of the apply work of the catch-up phase.
   Correct because a range is copied as it is when read and every
   change after that read is applied.
   **Done 2026-09-28**, decided by what the source shows, not by a log
   position: each range keeps the source's mark taken just before its
   read (PostgreSQL's snapshot `xmin:xmax:xip` with the cluster it was
   taken on, MySQL's executed GTID set; `ranges.started`, in the copy's
   own checkpoint), each change carries its transaction (the decoded
   xid, the binlog's GTID), and the tail leaves a change out where the
   marks of every range it touches - both, for a key it moves - show it
   committed (`hetero._CopiedRanges`, `ranges.already_read`). A position
   rule loses a transaction logged before the position and shown after
   the read (PostgreSQL shows commits out of log order; MySQL writes its
   binlog before the engine commits; a PostgreSQL change carries the
   position of the change, not of its commit). A range not begun is left
   to the copy only while the process that planned it holds the move's
   lease and the source shows the change before the checkpoint is read.
   Every batch is accounted for per table - read = applied + left out
   and why - before it is applied (`hetero._Accounts`,
   `tail-accounts.json`). Held by `test_the_tail_leaves_out_what_the_
   copy_read.py` (commits shown out of log order; fails on the position
   rule, on skipping what a mark does not show, on a range taken as read
   whatever its mark, on a range not begun left to a stopped copy, and on
   one left to the copy before the source shows the change). Measured,
   PostgreSQL 16, 200,000 rows in 8 ranges with a writer running: 35-42%
   of the catch-up's changes left out, the applier busy 0.58s against
   1.11s; the catch-up's wall clock 7.5s against 9.8s in one run and even
   in two, reading the log being most of it here (lever 9). MySQL 8.4
   with GTIDs, 60,000 rows by MySQL's own copier in processes: 205 of
   661 left out, the target equal, and every change of a transaction
   larger than a read carrying its GTID whether the reader is held open
   or started again (`test_a_tail_after_a_mysql_copy_applies_only_what_
   the_copy_did_not_read.py`). With GTIDs off, or on MariaDB, nothing is
   left out.
9. **Decoding off the Python thread.** Measured before it is built
   (R2.6): where the reader's decode is the ceiling, a compiled sidecar
   (go-mysql for binlogs, pglogrepl for WAL) hands migkit neutral
   records as Arrow batches; the tail's logic, marks and exactness stay
   in migkit. Debezium decodes in Java; a Python decoder cannot match it
   at the highest rates.
10. **Round trips removed on the statement path.** psycopg3's pipeline
    mode and binary COPY for the runs too small for the staging path
    (100 statements at 300 ms RTT: 30 s to 0.3 s, per its docs); on
    MySQL, the staging path through the safe LOAD DATA of lever 6 and
    `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE`.
11. **Exact batches make idempotence cheaper, not dearer.** Because the
    target says which batch it last committed (R3), the tail after a
    stop resumes after it instead of replaying a window of changes it
    cannot tell apart - less work on every restart, and the only design
    among those compared where a counter survives a restart.

None of these lowers what is verified: every lever keeps the range
digest, the fenced re-check and the exact batch; the physical rung adds
block checksums under them. Order: 6, 8, 10 (small, measurable now),
then 4 and 2's binary COPY, then 1 (the largest gain), then 7 and 9.

### R20. The hardware as the only ceiling, the proof nearly free (added 2026-09-29, the owner: "faster still - brutal throughput, in everything, moving and verifying, with consistency and idempotence kept")

Measured where migkit stands after F6: the MySQL tail's ceiling is the
Python decoder (~78k changes/s), then apply (~6.7 us a change); the
PostgreSQL tail spends 1.7 s parsing what the server decoded in 0.47 s;
the fold spends 80% rendering rows. So the levers that remain are all
one idea - **rows never pass through Python on the data plane**; Python
decides, schedules and compares numbers - and one more - **the proof is
kept up as the data moves, so verifying at the end costs almost
nothing**. Each is measured before it is claimed; none gives up the
range digest, the fenced re-check, exact batches or the final proof.

**Moving**
1. **Pass-through for a same-engine pair:** PostgreSQL `COPY (...) TO
   STDOUT (FORMAT binary)` piped straight into `COPY ... FROM STDIN
   (FORMAT binary)` on the target, per range, in parallel - the bytes
   are never parsed; the range's digest is computed by each server in
   SQL, not by Python. MySQL: the same with the pinned `LOAD DATA
   LOCAL` fed from `SELECT ... INTO`-shaped streams (or MySQL Shell's
   copy). Where a relay beside the source exists, the pipe runs there
   compressed.
2. **Arrow for a cross-engine pair:** read into Arrow record batches
   (ADBC / ConnectorX / psycopg binary into Arrow / DuckDB scanners),
   transform with pyarrow compute (vectorised, canon's rules as Arrow
   kernels), write through binary COPY / LOAD DATA / bulk copy / Arrow
   ingest - no per-row Python.
3. **Compiled change path end to end (W9, R19 lever 9):** a compiled
   binlog/pgoutput decoder (Rust `mysql_common`/Go `go-mysql`; the
   replication protocol, not SQL peeks) handing Arrow batches to
   migkit, and the collapse + write of a batch compiled too once
   decoding is no longer the ceiling; streaming replication
   (`START_REPLICATION` with binary pgoutput) instead of polling.
4. **Session-level load settings where they are safe:** `COPY ... FREEZE`
   into a table created or emptied in the same transaction;
   `synchronous_commit = off` on the loading sessions (a crash loses at
   most the last fraction of a second, which the final proof catches -
   never "ok" without it); MySQL `unique_checks = 0` and, only where the
   hop says the target has no replicas, `sql_log_bin = 0`. Never a
   server setting.
5. **Phases overlapped:** index builds of a table while the next copies
   (PostgreSQL), verification of a range while the next loads, the
   tail started as soon as the first table is done (with lever 8).
6. **Compression and legs chosen by measurement:** zstd level picked
   from measured CPU vs link (level 1 or none when CPU-bound, higher
   on a slow link), legs added while the rate rises (built), the relay
   beside the source and the writer beside the target (R17d) so the
   link's RTT is paid once a batch.
7. **Files:** dumps, Parquet and object copies moved with many parallel
   ranged parts (s5cmd-style), server-side copies where source and
   target share a provider, checksums taken while streaming (S3's
   CRC64NVME comes free) - never a second read to verify.

**Verifying**
8. **The proof kept up as the data moves (incremental multiset
   hashing):** the digest is a sum of row hashes, so it can be
   maintained - each leaf's digest is written when its range is copied
   (already computed), and the tail adds the new row's hash and
   subtracts the old one for every change it applies (both sides: the
   source's from the change's before and after images, the target's
   from what it wrote). At cutover the proof is comparing two kept
   numbers per leaf plus a re-read of the leaves the last seconds
   touched - O(changes), not O(table). A periodic background re-read
   of cold leaves guards against anything the stream did not see
   (LtHash-style; before images required - REPLICA IDENTITY FULL /
   `binlog_row_image=FULL`, else the touched keys are re-read).
9. **One scan for the whole tree, any key (F1):** every leaf digest in
   one `GROUP BY bucket` pass, buckets by a hash of the key, an in-SQL
   IBLT when differences are few and the link is slow.
10. **The fastest hash the pair allows:** same engine and version ->
    the server's native row hash (`hash_record_extended` and the like,
    no text rendering); cross-engine -> canonical rendering + md5,
    the rendering pushed into SQL where it is 80% of the cost;
    parallel workers on the server (`max_parallel_workers_per_gather`
    as a session setting) for the digest scan.
11. **Only what changed since the last proof:** tables and leaves
    untouched since they were last proved (modification counters,
    the tail's own record of keys touched) are not read again.
12. **Parallel both sides at once**, the source's and the target's
    digest of the same range read at the same moment on separate
    connections, sized by the decision engine from both servers' load.

13. **DuckDB as the compiled cross-engine mover (the owner, 2026-09-29:
    "why does it go through Python at all?"):** DuckDB, already a
    dependency, attaches MySQL, PostgreSQL, SQLite, Parquet, Iceberg
    and Delta at once and runs `INSERT INTO target.t SELECT ... FROM
    source.t` in parallel C++ (its PostgreSQL writer uses binary COPY);
    the hop's row filter, column mapping and casts become its SQL,
    canon's rules its expressions. A rung chosen per table where it
    proves byte-equal by the range digest and measures faster.
14. **The source renders the target's load format in SQL:** a
    `SELECT` on the source that emits each row already in the target's
    bulk-load format (PostgreSQL COPY text, MySQL LOAD DATA fields),
    piped as bytes into the target's loader - both servers do all the
    work, nothing is parsed between them. Only for the types whose
    escaping is proved exhaustively (a property test over every canon
    class and every byte), the rest refused to the other rungs.

Why rows pass through Python today at all, and what the fast tools do
instead: the generic range copier reads rows as Python objects because
it is the one path that works for every pair, applies the hop's row
filters, column rules and masking, and resumes by key; same-engine
bulk paths already pipe bytes (pgcopydb, the COPY pipe through the
relay, mydumper, raw BSON, DUMP/RESTORE) and digests are computed by
the servers. The fast tools never loop over rows in an interpreted
language: they pipe bytes server to server (pgcopydb, MySQL Shell,
mydumper), parse in compiled code in columnar batches (PeerDB, DMS,
GoldenGate, DuckDB, ConnectorX), or skip the SQL layer (base backups,
CLONE, storage snapshots, TiDB Lightning's SST ingest). R20 takes all
three, chosen per table.

Every path above competes with the tools migkit already drives -
pgcopydb, mydumper, pgloader, Debezium, DVT, reladiff - as rungs of the
decision engine measured on the same data (the owner, 2026-09-29):
where theirs is better or faster for a task it is used, where migkit's
is it is used, and migkit decides per table and per task.

Order: 8 and 9 (the proof nearly free), 1 (pass-through), 13, 14, 10, 11, 4,
5, 2, 3, 6, 7 - each against the tool that leads it (pgcopydb and
PeerDB for moving, Veridata/DVT/pgCompare for verifying), numbers in
the docstrings.

### R21. Easy enough to use without thinking (added 2026-09-29, the owner: "however good the tool, nobody uses it if it is hard and full of things they do not understand - configuring it must take no thought, with helpers that do it for them")

Measured on the code the same day: 13 commands (fine); the starter
config still shows `workers`, `big_rows` and `slice` (knobs the owner's
rule says nobody sets), an `engine` and a `service` the operator has to
know, a password field that invites plaintext; 64 hop option keys read
across the code; `init` writes a static template and asks nothing. So:

1. **Two addresses are a hop.** `source: postgres://app@db1/shop`,
   `target: mysql://app@db2/shop` - the engine, the pair (same-engine or
   across), the port and the databases come from the addresses and
   from the servers themselves; the cloud and managed service
   (RDS/Aurora/Cloud SQL/Azure/Tencent/Alibaba) from the host and what
   the server reports. Everything else is decided by migkit and shown,
   not asked. The long form stays valid.
2. **`init` becomes the guide** (the same command, no new mode): asks
   for the two addresses (or reads them from the environment), connects,
   says what it found (engines, versions, sizes, tables without keys,
   types that need care, what the source allows - CDC on or off,
   grants, a replica or a primary), proposes the plan in plain words
   with an estimate of time and cost, and writes the smallest config
   that says it - only what differs from what migkit would decide.
   Passwords never written in plain text: stored in the OS keychain
   (`keyring`) or as an `env:` reference, the choice offered.
3. **What the DBA must do, written for them:** where a grant, a setting
   or an extension is missing, `init`/`doctor` write the exact
   least-privilege script for that engine and that service (with the
   cloud's own spelling - parameter groups, flags), and re-check after.
4. **Nothing to tune:** workers, batch sizes, chunking, compression,
   legs, the path per table - all decided and adapted by migkit; a hop
   key that sets one becomes a ceiling at most, and the starter config
   shows none. Options keep sane defaults; `doctor` flags any key that
   does nothing or fights migkit's own choice.
5. **One word for what you want:** a hop option `goal:` - `copy`
   (one-off move), `cutover` (move, follow, verify, switch with the
   least downtime), `verify` (prove two databases equal), `two-way`,
   `keep-in-sync` - sets the right defaults for everything under it;
   the CLI stays as it is.
6. **Config checked like code:** unknown keys with "did you mean",
   wrong types, contradictions (a filter on a table that is excluded),
   secrets in plain text, all before anything runs, in migkit's words.
7. **Coming from another tool:** read an AWS DMS task, a Tencent/Alibaba
   DTS job, a Debezium connector config or a pgloader load file and
   write the hop that does the same - switching costs nothing.
8. **The view helps too:** the dashboard gets a setup page doing what
   `init` does, for operators who prefer a form (behind the same
   roles and host checks).
9. **Every message says what to do next**, in plain words, with the
   exact command or statement.

Measured by: time from nothing to a verified move for a new operator
(scripted in docker with each engine pair), number of config lines a
typical hop needs (target: the two addresses and nothing else), and
questions the guide asks (target: none beyond the addresses and a
password).

### R22. Every resource in one model: workers, connections, hops, time and the machine to run on (added 2026-09-29)

The owner: "take everything - the machine migkit runs on, the network
and connections on both sides, every other factor - compute it all
together, so migkit knows how many connections and workers give the
best result for each task, how to remove hops and cut latency; and
help with the ETA and with choosing the right instance type."

Today (read from the code): `sizing.estimate` starts from the least of
this host's CPUs and memory, each server's free connections, load and
CPUs, and the work; `sizing.Pace` then climbs while rows a second rise
and backs off on a server's strain (BBR-like probing); `tunnel` measures
the link's round trip and adds legs; `planner.estimate` gives a time
range from past runs; `watch` shows a live rate and ETA. What is
missing is one model that knows *which* resource binds and plans to it:

1. **One resource model per task:** source read (cores, I/O rate, free
   connections, load, replica lag, what the move may take of a primary),
   target write (cores, I/O, WAL/binlog volume, index maintenance, lag
   of its own replicas), the migkit host (cores, memory, NIC, disk for
   spill), the link each way (RTT, per-stream and total bandwidth, loss
   - the bandwidth-delay product sets the window and the batch), and
   the path's cost per row for the chosen strategy (bytes a row on the
   wire, CPU a row where it is converted). Measured by short probes
   before the move and kept current during it. **Memory is one of the
   resources, driven by large values** (the owner: AWS DMS asks for a
   LOB size - too large and the replication instance is OOM-killed, too
   small and values are silently truncated): each table's largest and
   99th-percentile row measured before the move; per-worker memory =
   batch bytes in flight + the largest row times the pieces buffered +
   the driver's own overhead; concurrency never more than free memory
   allows, so wide tables get few workers and small batches and narrow
   ones many; a value larger than the budget streamed in pieces (R10),
   never truncated; a watchdog shrinks workers before the kernel kills
   anything, and says so. No operator ever sets a LOB size.
2. **The bottleneck named, and concurrency sized to it:** throughput is
   the least of the stages' rates (a roofline); workers = what saturates
   the binding stage and not one more (Little's law: concurrency = rate
   x latency of one unit), connections per side from that, split so the
   source's read and the target's write are sized separately. Said in
   words: "the target's writes bind at ~42 MB/s; more workers would not
   help; a larger target instance would".
3. **One controller per resource while it runs:** the source's read
   pace backs off on its load or replica lag, the target's write pace
   on its lag or I/O wait, the link's legs on loss and RTT growth -
   instead of one pace for all (AIMD / gradient concurrency limits as
   TCP Vegas and Netflix's concurrency-limits do), each probing up now
   and then.
4. **Hops removed:** the fewest legs the data can take - a server that
   pulls straight from the other where the hop allows its footprint
   (ClickHouse `remote()`, PostgreSQL through a foreign server or a
   subscription, MySQL CLONE; credentials removed after, said), else the
   relay beside the source and the writer beside the target so the link
   is crossed once, compressed, in few round trips; migkit told where
   it itself should run (which network, which zone) and why.
5. **ETA with its reasons:** per phase (copy by table, index builds,
   verify, catch-up, cutover), from the measured rates of this hop, of
   the rehearsal, or of the same engines elsewhere, with a range (p50 /
   p90) and the binding resource of each phase; live, from the recent
   rate (EWMA) and what is left; the catch-up's own sum - changes
   arriving against changes applied: if apply cannot pass arrival, it
   says it never catches up and what would change that.
6. **The machine to run on:** for migkit and its agents - the
   instance type in the source's or target's cloud that meets a
   deadline at the least cost, from the model (a bigger machine when
   migkit's CPU or NIC binds; the same one and a warning when a server
   binds), knowing that many types' network is a burst over a much
   lower baseline (a long move runs at the baseline), and EBS/disk
   throughput limits for spill; for the target database - the class
   its load needs, from the source's performance history (W2). A
   catalog shipped with migkit (vCPU, memory, network baseline and
   burst, disk throughput, price by region) refreshed from the
   provider's API when credentials are there.

Measured: the model's predicted rate and ETA against the measured ones
on docker pairs with throttled CPU, I/O and network (tc/toxiproxy),
the binding resource named correctly in each case, and no run slower
than today's `Pace`.

### R23. What people who migrate actually want, and what is still missing (added 2026-09-29)

Asked by the owner: what does a migration tool people really want look
like, and what does migkit lack. What they want, in their words: *did I
lose anything, and can I show it*; *how long, how much downtime, how
much money - before I start*; *it must not hurt production or my
target*; *the cutover must not go wrong, and I must be able to go
back*; *setup without a manual*; *I can see where it is*; *the odd
things (types, sequences, users, jobs, LOBs) are handled for me*; *the
application still works and is not slower*; *it runs where my
databases are*. Most of that is in the items above (verify, R19-R22,
cutover, R21, types, users, workload). Not yet anywhere:

1. **An evidence export per migration (not a certificate):** migkit is
   no authority and issues no standard - the owner's point: "who are we
   to issue one". What the paid tools hand over is reports of what they
   ran (DMS's premigration assessment and validation results, its Schema
   Conversion assessment, DTS's consistency-check report, Veridata's
   comparison reports, Datafold's diff reports) - none is a standard
   either. What makes migkit's worth referencing is that anyone can
   check it again: what moved, per table the counts and digests of both
   sides with the exact method written beside them (so the reader, or
   `migkit check` run again, recomputes the same numbers), the times,
   the downtime measured, the approvals, what was left and removed - in
   the formats audit and change-management processes already take (PDF
   or HTML for people, CSV/JSON for tools), made tamper-evident by the
   audit chain. It claims only what was measured.
2. **Rehearse, then forecast:** a goal that runs the whole path on a
   clone or a sample the operator names, and turns what it measured
   into the forecast for the real run - time per phase, downtime,
   bytes, cost - with the confidence of a sample of that size; the real
   run later compared with its forecast.
3. **An interlock on the target:** the plan shows each side's identity
   (cluster id, host, version, size, whether it holds data) and `--go`
   refuses when the target is the source itself, when it looks like a
   production system the hop does not name as the target, or when it
   holds data and the hop has not said what to do with it; the
   identities are checked again at the start of every write step.
4. **Performance after a change of engine:** `workload.py` compares the
   source's own reads on the same engine; after a change of engine the
   translated workload (P10) is timed too, writes as well as reads, and
   the target is advised (indexes, statistics, the settings its load
   needs) before cutover.
5. **Copies for testing:** a referentially complete subset (the closure
   of foreign keys from the rows chosen) with masking, into a staging
   or developer database - the same engine pairs, the same proof of
   what was copied.
6. **Right the first time, not a runbook to recover with** (the
   owner: "not a document - make it succeed once, no rollback needed,
   high correctness, high quality"): the rehearsal (2), the preflight
   and refusals before anything is written, the proofs at every step
   and the go/no-go gates are run by migkit itself, so the real run
   repeats a path already proved; the rollback stays armed as a safety,
   not as a plan. What people must still do by hand (a DBA grant, an
   application switch) is listed by the plan at the moment it is
   needed, not in a separate document.
7. **A major-version upgrade as a goal:** `goal: upgrade` - the same
   engine at a new version with the least downtime, choosing between
   the in-place upgrade and a logical move by measurement, with the
   extensions and their versions checked first.
8. **Working inside the team's tools**, as the others do: Airbyte and
   Fivetran ship Airflow operators, a Terraform provider and an API;
   Datafold runs its diff in GitHub CI on pull requests; Liquibase,
   Flyway and Atlas ship GitHub Actions; Debezium and PeerDB are watched
   through Prometheus and Grafana; DMS through CloudWatch and
   EventBridge. migkit: tasks for GitHub Actions, Argo and Airflow,
   OpenTelemetry traces beside the Prometheus metrics, Grafana
   dashboards shipped with the rules, with W2's API and Terraform
   provider (to be confirmed tool by tool before it is claimed).
10. **Notifications wherever the team is:** today Slack, Discord, Teams,
   PagerDuty and a JSON webhook. The others: DMS through SNS and
   EventBridge (so anything behind them), Airbyte Slack and webhooks,
   Fivetran email and webhooks, Estuary email, Slack and webhooks,
   GoldenGate and Qlik email/SNMP. migkit takes in the Apprise library
   (BSD-2, about a hundred services behind one URL scheme) as a
   dependency so a receiver can be any of them - Microsoft Teams,
   Google Chat, Lark/Feishu, LINE (Messaging API - LINE Notify closed in
   2025), Telegram, Discord, email, SNS, Opsgenie, Mattermost,
   Rocket.Chat, DingTalk, WeCom, ntfy, Pushover, Matrix, Signal, Firebase
   Cloud Messaging for phones, plain webhooks - with migkit's own
   message shaped per channel (cards where the channel has them), the
   same rules as now (no row values, the address never printed, a
   delivery failure said and the run going on), and a test per channel
   against a local stub.
9. **Documentation as part of the product:** a five-minute quickstart
   for each common pair, a page per engine saying what is carried,
   what is refused and why, recipes for the usual moves (a managed
   service to another, on-premises to cloud, one engine to another),
   in migkit's own words; measured by a new operator following it.

### Paused 2026-09-27 (the owner's call: out of tokens) - resume here

Pushed **without the full suite run** (the owner's call, out of tokens):
everything since `1da6415` (the R-items marked done above, the fixes after
the last full suite, the follow plan worded in migkit's own terms, and the
R3 counters). The last full suite (before the last of these) was 2244
passed / 28 failed; the deterministic failures were fixed, the rest were
load-timing ones that pass alone. **The full suite runs once, at release** (the owner's rule, for
speed); until then each change runs only the tests it touches. The research passes below were stopped
before any result came back; rerun them. One earlier pass did finish: MongoDB's tools (mongosync, migration-verifier, the dump tools, MongoShake, dbHash, pymongo raw batches) in `docs/research/mongodb-tools-2026-09-27.md`, and Redis/Valkey's (RedisShake, RIOT/RIOT-X licence, RDB parsers and the Redis/Valkey format split, DUMP/RESTORE, redis-full-check's rounds, what managed services block) in `docs/research/redis-tools-2026-09-27.md`, and Kafka's (MirrorMaker 2's offset syncs and checkpoints, KIP-1279, Replicator, the Redpanda migrator's offset translation by a header, Python clients, partitioner and transaction pitfalls) in `docs/research/kafka-tools-2026-09-27.md`, and Cassandra/Scylla's (DSBulk, CDM, ZDM proxy, scylla-migrator, sstable loading, commitlog and Scylla CDC, WRITETIME/TTL rules, counters) in `docs/research/cassandra-scylla-tools-2026-09-27.md`, and DynamoDB, files and lakes, generic pipelines and vector/graph notes (export and import to S3 and their silent failures, rclone, s5cmd, S3 checksums, DuckDB, pyarrow, pyiceberg, delta-rs, Redpanda Connect/bento, Airbyte and Singer licences, dlt) in `docs/research/dynamodb-lakes-pipelines-2026-09-27.md`, and the published throughput of DMS, both DTS products, pgcopydb, MySQL Shell, mydumper, TiDB Lightning (GoldenGate, Qlik, Vitess, mongosync and pg_dump -j publish none) in `docs/research/throughput-published-2026-09-27.md`, and ClickHouse and Elasticsearch/OpenSearch's (clickhouse-backup, remote() copies and their dedup and retry rules, BACKUP/RESTORE, part hashes only comparable after a physical copy, PeerDB now AGPLv3, elasticdump, Migration Assistant's reindex-from-snapshot and leases, remote reindex, snapshot compatibility, CCR, the Python clients) in `docs/research/clickhouse-opensearch-tools-2026-09-27.md`, and the published rates of the change and load tools (PeerDB, Artie, Estuary, Fivetran/HVR, ConnectorX, dlt, ADBC, DuckDB's scanner, ClickHouse remote()) and of each load technique (COPY vs INSERT, COPY FREEZE, parallel index builds, LOAD DATA, deferred indexes hurting MySQL, zstd, TCP window and loss) in `docs/research/throughput-cdc-techniques-2026-09-27.md`, and the security of 18 open-source tools, factor by factor (TLS, secrets, files at rest, footprint and grants, audit, masking, network, RBAC, supply chain, FIPS), with twelve practices to take from them, in `docs/research/security-oss-tools-2026-09-27.md`, and the security of the managed and commercial services (DMS, Google DMS/Datastream, Azure, Alibaba and Tencent DTS, GoldenGate, Qlik, Fivetran, Striim, Debezium, Airbyte, Estuary) with twelve practices worth copying, in `docs/research/security-managed-tools-2026-09-27.md`; none committed yet. Rule from the owner: implement
everything first, run the suite once, fix once, commit once.

**How migkit is implemented (the owner's rule, 2026-09-27, for this
project only):** implement to the end; tests are written with each
feature and run small - only the tests the feature touches, incremental,
as it lands; when every item is done, the full suite once, and then the
fixing. Never the full suite in the middle, never a commit that waits on
it.

**Standing rule for the next round and every round after (the owner,
2026-09-27):** every program and library migkit wraps is used to the
fullest - all of its capabilities, not one - and all of them together,
with migkit's own logic choosing the tool, choosing the capability, and
pulling out of the combination the best and fastest result - always,
with the complexity and the intelligence of the choice in migkit, by
task and by what the task needs, everything wrapped in migkit (0f above:
the used / unused / topped-up table per tool, a reason for every
capability left unused, top-ups where migkit's own function gets more
out of a tool, and any tool or library research finds that adds a
capability is taken in).

Asked by the owner and still to do, in order:

1. **Research, all of it, before building** (the passes on the paid products' mechanisms - GoldenGate, Qlik, HVR, IBM, SharePlex; the CDC/ELT specialists; DTS, TiDB, Vitess, MOLT, Voyager; AWS, Azure, Google, Snowflake - all died on the rate limit before any result, and the four general passes were stopped to save tokens; rerun them all):
   * paid and cloud products (DMS, DTS, Tencent DTS, Google DMS and
     Datastream, Azure DMS, GoldenGate and Veridata, Qlik, Fivetran/HVR,
     Striim, Informatica, IBM IIDR, SharePlex, Airbyte, Estuary, PeerDB,
     Artie, Datafold, MOLT, Voyager, Vitess, TiDB DM, Relational
     Migrator/mongosync, RIOT, ...);
   * open-source relational tools and libraries to wrap (pgcopydb,
     pglogical, pg_chameleon, pgloader, Bucardo, gh-ost, pt-toolkit, MySQL
     Shell dump/load/copy, mydumper, VDiff, Lightning, sync-diff-inspector,
     canal/Maxwell, bcp/sqlpackage, ora2pg, Sling, dlt, ConnectorX, ADBC,
     DuckDB scanners, data-diff, DVT, datacompy, sqlglot, schema-diff tools,
     psycopg3 binary COPY, LOAD DATA LOCAL, xxhash/blake3, pyarrow, ...);
   * open-source non-relational (mongosync, migration-verifier,
     MongoShake, redis-shake, RIOT, MirrorMaker 2, DSBulk, CDM, ZDM,
     scylla-migrator, clickhouse-backup, remote(), OpenSearch Migration
     Assistant/RFS, elasticdump, DynamoDB export/import, rclone, pyiceberg,
     delta-rs, Benthos, ...);
   * security and throughput: every tool's TLS/mTLS, secrets handling,
     encryption of staging, spill and logs, source-side footprint and least
     privilege, audit, masking, RBAC, supply chain; and every tool's
     published rates with the technique behind them.
   Each pass ends in a **scorecard per tool and factor - behind / equal /
   ahead, with the tool's source and the migkit file:line** - and a gap
   table: their mechanism, migkit's better one, chosen by migkit's own
   decision layer from measured facts (no new modes or flags), effort,
   testable in docker.
2. Write the findings here, then wrap what is worth wrapping as
   dependencies (pip first; binaries through `doctor --install` after the
   owner allows the downloads, asked once with the list), each picked per
   table and task by `movers.pick`/`fitted`/`planner` - deep, not one flat
   rule.
3. The locally doable open items found by the audits: ClickHouse
   `remote()` bulk and partition follow; OpenSearch follow and users;
   Cassandra/Scylla change follow and a deep TTL check; users across
   engines; SQL Server bulk and staging; MySQL/Oracle large values in
   pieces; R11 stored code from MySQL and SQL Server sources; R3 above;
   R5 view actions and signed audit anchoring, HMAC masking of what a
   shared report shows; R17a TLS for the MySQL bulk programs, Mongo and
   Redis legs; R17d a writer beside the target; R18 zstd spill, Arrow
   batches, the Kafka client; R16a vectors; the DTS gaps (DDL replication
   and allow-list, operation filters, added columns, a running job
   changed, start at a time, sampled check, GTID-set compare, change-stream
   filter, resumable dump status, read-only target and newer-wins on more
   engines); the problems file's *Partly* items (A2/D10, A5, B4, C3 on
   MySQL and SQL Server, C4, D15); the planner's speed rules; stale docs.
4. The owner's bar for all of it: faster, smarter, deeper and more exact
   than every tool compared, a cutover with no data problem, high
   throughput and strong security.
5. At release only: memory and swap checked, the suite once in `/tmp/migkit-suite`
   (no `conf/hops.yaml`, `MIGKIT_CONF` an empty file), everything fixed,
   `git diff --cached` scanned, the secrets check, one one-line commit,
   push.
6. Decided by the owner (2026-09-27): `migkit_origin`, a table in the
   application's database made only where a hop asks for two ways, is
   allowed - the exception to the rule that migkit creates nothing on the
   target. The same device the others use: GoldenGate's trace table
   (`ADD TRACETABLE`, a row written first in each applied transaction so
   the capture on that side leaves the transaction out), Bucardo's and
   SymmetricDS's own tables on every side, Estuary's watermarks table,
   the DTS products' helper schemas. Where a side can tag its own
   transactions instead (GoldenGate's `EXCLUDETAG`, PostgreSQL 16's
   origin filter, MariaDB `skip_replication`, MySQL 8.3+ tagged GTIDs),
   R3 still prefers the tag, chosen per target, and the table is the
   fallback. Still to: the two-way teardown drops the table, and `doctor`
   names it as migkit's footprint.

### Order for this round

1. R0 (the applier's key-update on tables with two unique indexes)
2. R1 (parallelism sized by migkit)
3. R2 (the applier: pipeline, lanes, bulk statements)
4. R7 and R6 (failure on purpose, the network path)
5. R9, R10, R14 (assessments, large values, the owed measurements)
6. R8 (other verifiers)
7. R3 and R4 (two-way, Avro and registries)
8. R5 (the control plane)
9. R13 and R12 (engine cells, Oracle Free), R15 (DuckDB), R16a (vectors)
10. R11 (stored code)
11. R17 (the path between the databases: a, b, c, then d)
12. R18 (libraries: pass-through, hiredis and zstd, the row hash, the
    out-of-core diff, the interleave spec, Arrow, the Kafka client)

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
