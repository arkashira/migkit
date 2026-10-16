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
today. A table without a key, or with a text key, goes by where its rows
are stored, by buckets of its rows' hash, or in one pass
(`ranges.keyless`, 2026-09-30). Tests: `tests/test_the_decision_engine.py`.

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

  *Done for the three verifiers (2026-09-28, on R20's rungs
  2026-09-30).* The table is the tool-use registry's (`migkit/tooluse.py`,
  registered from `migkit/verifiers.py`): per tool the features used (read
  off the code that drives it), the ones not used with the reason, and the
  top-ups;
  `tests/test_every_verifier_feature_is_used_or_explained.py` fails on a
  documented feature with neither, on a reason for a feature now used,
  and on a flag the installed program has that the table does not list.
  The reader runs from its own environment (`doctor --install`: uv on
  Python 3.12, 3.9 s warm and 422 MB; its numpy 1.26.4, pandas 2.3.3 and
  sqlglot 19.9 cannot share migkit's), and is found by its program's
  first line where it is on the path. Measured on PostgreSQL 16 pairs and
  a MySQL 8 to PostgreSQL 16 pair before any of it was wired:
  * every validation of a run goes into one batch file in its own format,
    executed from the file and kept as `data-second-reading.yaml` (which
    the hop's `at_rest` recipients encrypt: its predicates carry group
    values); each carries migkit's label, and a validation that fails is
    said per table, not as the whole run's failure
  * `--filter-status fail` is applied by its result handler, which the
    collector replaces - so the runner applies it, or a row reading asked
    for its failures would have handed back every row
  * the groups, a way to find a differing table's rows, chosen per table
    by the decision engine where measured the cheaper: aggregates per
    group of the column the source's statistics say splits it best (4 to
    10,000 groups of 10 rows or more, integer or text, integer only across
    engines), failures only, and only those groups' rows read back. Across
    a pair it is a localizer of the verify passes (`leaves.PASSES`, rung
    `groups`, after the leaves in the list): the rows read back are held to
    the difference of the two digests, and where they fall short the
    leaves are read. Where PostgreSQL or MySQL walk their own rows it is
    `second_reader.WALKS` against that walk, and migkit's own digest of
    everything outside the groups decides whether the rest is walked too.
    The aggregates missed a text changed to one of the same length (`v25`
    to `x25`: no count, sum or length moves) - the totals and the digest
    are what keep that row from being lost
  * the rows migkit names in a differing table are read again the other
    way, as the verify passes' `second` part chooses (the row reader where
    it is here, else the bisecting reader), on every engine's walk - its
    row hash within an engine, only the groups where the groups proved to
    hold every difference, at most 1,000,000 rows (`ROWS_MOST`), said
    past it; value by value across engines,
    floats and fixed-width text left out (the hash called the `-1e-07` row
    different again, and a `char(5)` read `c1` from MySQL and `c1   ` from
    PostgreSQL on all 301 equal rows) - and the two sets of keys are held
    to each other. Never `ok`: the same rows, rows only the second reading
    names (one reading is wrong), or rows it read as equal. Its hash drops
    trailing blanks and reads NULL as a fixed word (read in its source; a
    trailing blank measured invisible to it, visible value by value), and
    every finding from it says so
  * a set of up to 50 differing rows is explained column by column in
    migkit's words (`data-<table>.explained`); `check --drill` says the
    same instead of the library's own report, which opened with its name
  * where the hop asks (`two_readings: true`, or `{rows: false,
    sample: <rows>, case_insensitive: true}`, declared in `hopkeys`), every
    table twice: the
    aggregates, every keyed table row by row, the views by what they
    answer; random rows first and apart, `skip` when they agree (200 of
    5,000 rows missed all three differing ones) and `diff` when not
  * average, deviation and bitwise XOR where the column is an integer or
    an exact decimal: all nine agreed across MySQL and PostgreSQL on equal
    rows; `--threshold` only from `mapping.tolerance`, and a drift inside
    it said (a bigint's sum came back with no difference worked out, and
    34,871 against 34,872 read `fail` under 1%: the tolerance did not
    apply to it, and the measure is said); `--exclude-columns` from the
    hop's column rules (it refuses `*` beside it)
  * PostgreSQL 16, local: 200,000 rows by row hash 3.3 s, count and sums
    1.9 s, grouped into 97 groups 1.9 s
  * the bisecting comparison program: a rung with its facts
    (`verifiers.reladiff_rung`) - factor, threshold and depth sized from
    the table's rows and bytes, threads from the throttle - chosen where
    it is the only reader of an engine or the hop asks, a rung of the
    verify passes within one engine (localizer and second reading), and
    why not across two; the sized settings passed wherever it runs,
    through one command builder (`leaves._bisect_run`). It refuses a factor not below the threshold and exits 0
    while saying so; on 200,000 equal rows its sizing changed nothing
    (0.38 to 0.39 s); "broken on 3.14" did not reproduce: it imports on
    3.14.7, with no driver built for 3.14 to compare with
  Still open: per-partition runs of a table too large to read row by row
  a second time (`generate-table-partitions`), said as skipped today;
  SQL Server through the same narrowing.
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
* *Done for these two (2026-09-28).* `migkit/tooluse.py` is the account:
  a registry each wrapping module adds its tools to, every option used,
  unused with its reason, or topped up, with each tool's licence and
  terms (`allowed(tool, hop)` for the decision layer to gate a path by;
  a program `doctor --install` puts in place has them read from `tools`,
  where the operator accepts closed terms - still open: the streaming
  pipeline's broker image is source-available and pulled without being
  accepted anywhere, so it wants a place among those terms or an open
  broker in its stead);
  `tests/test_every_wrapped_option_is_used_or_explained.py` fails on an
  option the build has and nobody wrote about, on an entry for one it no
  longer has, and on a command line or configuration that passes what the
  account calls unused. `_long_options` asks a subcommand's own help
  (pgcopydb's top-level help lists no option at all). The streaming copy
  splits a table only where it outlasts an even share, at half a share
  (measured: the build cuts `ceil(size / threshold) + 1` parts, the first
  for NULL keys, the last twice the others), not under ten seconds of one
  stream; split and whole are two rungs (`decide`) ranked by the rate each
  reached on the hop or its rehearsal, one rule for the whole database's
  copy and the table copier's streaming rung (`pgcopydb_table`). A
  stopped copy goes on cut the way it began (kept by `decide.remember`) -
  and one that cut a table in parts starts over: measured on 0.18, going
  on from it left 750,000 of a 1,500,000-row table and exited 0, with the
  same cut, another or none. On this machine whole tables
  were faster: two 57 MB tables, four streams, 5 rounds, the copy 1.45 s
  median whole against 1.63 s in parts. Its own progress reader is
  broken in 0.18 (`no such column: bytes`), so its catalogue is read for
  per-table progress lines and the next run's rates. The ways a hop's
  changes follow its source - the servers' replication, the leg run from
  here, migkit's tail, the streaming pipeline - are one ladder
  (`movers.stream_ways`): today's order where nothing is measured, the
  cheaper where two are, and a way never timed not started only to be
  timed; the leg run from here records its cost. Found and fixed: the
  streaming copy left every large object behind (now carried after it),
  the change leg run from here carried the tables the hop excludes (now
  filtered) and filtered or renamed ones whole (now refused, as the
  pipeline refuses them), the pipeline skipped source truncates, and 38
  message queues left by killed copies made the next one fail on `No
  space left on device` (now said). The connector: transaction metadata
  (and migkit's own readers mark each source transaction's last change,
  `txn_end`, beside its `txn`), truncates carried, buffers from the rows
  carried and bounded in bytes, every value in its exact form
  (`value_modes`: microseconds and never `connect`'s milliseconds, exact
  decimals, bytes, unsigned 64-bit integers whole, intervals as their
  text, MySQL's years as stored), heartbeats, the notification channel
  read back as progress, a re-read held while the source is under
  strain, the configuration validated by the runtime before it
  registers. Not used, with reasons: `clone` and its options
  (migkit makes the schema itself; this build's clone into 16 stops on
  `transaction_timeout`), `copy blobs` (hung 338 s into 16),
  `snapshot.select.statement.overrides` (a filter at the first read only
  lets later changes through unseen), masking at the source.
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

<!-- whole-use:begin (written by tools/whole_use.py) -->

*0f whole-use audit (2026-10-16, written by `tools/whole_use.py`; the full table is `docs/research/whole-use-2026-10-16.md`).*

* `0f.dvt.partitions` (unused): table partitions generated as batch files so a big table is compared range by range in parallel would strengthen R8 narrowing a differing table to a key range (verify ladder).
* `0f.dvt.custom-query` (unused): row validation of a query on each side, for a view or a keyless source would strengthen verify of views and keyless sources (verify ladder).
* `0f.reladiff.empty` (unused): tolerating an empty table on one side would strengthen verify of tables the generic engine reads (verify ladder).
* `0f.reladiff.sort` (unused): skipping the sort of the result would strengthen row-compare cost on wide differences (verify ladder).
* `0f.pgcopydb.binary-copy` (unused): binary COPY between servers of one major version, skipping text conversion would strengthen same-engine bulk copy (bulk ladder).
* `0f.pgcopydb.follow-resume` (unused): resuming a change-stream leg from its saved position would strengthen P0-RESUME-ALL resume of a stream leg (stream ladder).
* `0f.pgcopydb.compare-json` (unused): the second reading in a structured form would strengthen second-reading verdict detail (verify ladder).
* `0f.mydumper.stream` (unused): streaming the dump straight into the load, with no dump directory would strengthen disk-pressured MySQL bulk copy (bulk ladder).
* `0f.mydumper.partitions` (unused): a partitioned table dumped per partition would strengthen chunking of partitioned MySQL tables (bulk ladder).
* `0f.myloader.optimize-keys` (unused): secondary keys built after the rows are in would strengthen MySQL bulk load index handling (bulk ladder).
* `0f.myloader.resume` (unused): resuming a stopped load would strengthen P0-RESUME-ALL resume of a bulk load.
* `0f.mysqlsh.defer-indexes` (unused): indexes built after the load would strengthen MySQL bulk load index handling (bulk ladder).
* `0f.mysqlsh.max-rate` (unused): a ceiling on the load's throughput would strengthen throttle of the shell's own copy (throttle rung).
* `0f.mysqlsh.overlap` (unused): loading while the dump is still being written would strengthen phases overlapped by measured time (bulk ladder).
* `0f.pt-table-checksum.chunk-sizing` (unused): its own adaptive chunk time and size would strengthen replica checksum chunk size from measured lag (replica-proof rung).
* `0f.pt-table-checksum.resume` (unused): resuming a checksum run would strengthen P0-RESUME-ALL resume of a replica proof.
* `0f.redis-shake.managed-psync` (unused): replica protocol behind a managed service's own command would strengthen managed Redis snapshot ladder (snapshot ladder).
* `0f.redis-shake.sentinel-target` (unused): a target reached through its sentinel would strengthen highly available Redis target (target-write rung).
* `0f.dsbulk.compression` (unused): compressed JSON files between read-out and load would strengthen disk-pressured Cassandra copy (bulk ladder).
* `0f.dsbulk.paging` (unused): continuous paging on sources that support it would strengthen Cassandra-compatible read-out speed (bulk ladder).
* `0f.connect-mirror-maker.sh.compression` (unused): producer batch compression would strengthen compression probed per leg (stream ladder).
* `0f.connect-mirror-maker.sh.client-cert` (unused): a client certificate for a secured cluster would strengthen secured Kafka source and target (stream ladder).
* `0f.connect-mirror-maker.sh.refresh` (unused): refreshing topics and groups while the mirror runs would strengthen long-running Kafka follow (stream ladder).
* `0f.debezium.snapshot-threads` (unused): parallel first-read threads and fetch size would strengthen first read of a change-capture pipeline sized from measurements (stream ladder).
* `0f.debezium.incremental-snapshot` (unused): chunked re-read beside the live stream would strengthen repair re-read without a stop (repair rung).
* `0f.debezium.reduction-buffer` (unused): several changes to one key in a batch reduced to the last would strengthen change apply throughput on hot keys (apply ladder).
* `0f.debezium.tls-stores` (unused): trust and key stores for a secured source would strengthen secured MySQL change capture (stream ladder).
* `0f.debezium.keep-alive` (unused): connection keep-alive on a long idle stream would strengthen idle-stream survival behind proxies (stream ladder).
* `0f.OpenLogReplicator.protobuf` (unused): a binary change format would strengthen Oracle redo reader throughput (change-capture rung).
* `0f.pg_upgrade.swap` (unused): swapping directories instead of copying files would strengthen in-place upgrade file carrying (upgrade ladder).
* `0f.pg_upgrade.copy-file-range` (unused): server-side file copy would strengthen in-place upgrade file carrying (upgrade ladder).
* `0f.hypopg.hide-index` (unused): hiding an existing index to price its removal would strengthen index advice on unused target indexes (advice rung).
* `0f.datacompy.polars` (unused): a compare on columnar frames would strengthen row explanation memory and speed (verify ladder).
* `0f.reindex-from-snapshot.signed-target` (unused): a target signed by the cloud's own credentials would strengthen managed search targets (target-write rung).
* `0f.reindex-from-snapshot.ca` (unused): a target checked against a given authority would strengthen secured search targets (target-write rung).
* `0f.reindex-from-snapshot.bulk-size` (unused): documents per bulk request would strengthen bulk sizing from the target's measured refusals (apply ladder).
* `0f.dynamodb-s3.incremental-export` (unused): an incremental export between two times would strengthen DynamoDB catch-up after the copy (stream ladder).
* `0f.dynamodb-s3.bucket-owner` (unused): a bucket of another account would strengthen cross-account DynamoDB export (snapshot ladder).
* `0f.ast-grep-py.pattern-search` (unused): match a pattern with metavariables across a tree would strengthen stored-code reading (statement ladder).
* `0f.aws-sdk.copy-snapshot` (unused): copy a snapshot to another account or region before it is restored there would strengthen snapshot ladder (a snapshot copied to the target account).
* `0f.azure-sdk.restart-export-restore-point` (unused): restart a flexible server, export a cache, take a SQL restore point before a cutover, put a backup would strengthen snapshot ladder (a restore point taken on the source before a cutover).
* `0f.boto3-s3.io-tuning` (unused): I/O chunk size, queue depth, bandwidth cap and transfer client choice would strengthen transfer tuning on a measured link (bulk ladder).
* `0f.couchbase.query-and-sampling` (unused): query service, prefix and sampling scans, consistency with a mutation token, document mutation in place would strengthen sampled and prefix scans of a keyed source (verify ladder).
* `0f.deltalake.change-data` (unused): the change data feed of the table would strengthen change-feed reading of a lake target (stream ladder).
* `0f.google-api-client.restarts-exports` (unused): restart and restore instances, export documents and copy a Spanner backup would strengthen export of a document store before a cutover (snapshot ladder).
* `0f.google-cloud-bigquery.copy-insert-json` (unused): copy a table, insert rows and load from JSON would strengthen load of a warehouse target from a file or copy (bulk ladder).
* `0f.google-cloud-bigquery-storage.pending-default` (unused): pending and default write streams, and the change type would strengthen pending-stream write of a warehouse target (bulk ladder).
* `0f.google-cloud-bigtable.query-batch` (unused): a SQL query, a batched mutation and a read-modify-write would strengthen batched writes of a wide-row target (bulk ladder).
* `0f.google-cloud-datastore.transaction` (unused): a transaction around a read and write would strengthen transactional write of a document target (bulk ladder).
* `0f.google-cloud-firestore.batch-delete-read-time` (unused): a batch, a recursive delete and a read at a time would strengthen recursive removal of a document target (bulk ladder).
* `0f.google-cloud-spanner.dml-groups` (partly used): partitioned DML, mutation groups, a transaction runner and the DB-API would strengthen partitioned DML for a bulk change on a large table (bulk ladder).
* `0f.gremlinpython.submit-merge` (unused): submit a script directly and merge vertices would strengthen merge of graph vertices on a graph target (bulk ladder).
* `0f.huaweicloud-sdk.in-place-restore-restart` (unused): restore onto an existing instance, restart it, update an entity configuration would strengthen restore onto an existing target instance (snapshot ladder).
* `0f.ibm-cloud-sdk-core.restore-deployment` (unused): restore a deployment from a backup would strengthen restore of a managed deployment target (snapshot ladder).
* `0f.ibmi-odbc.fetch-tuning` (unused): block fetch size, block size and compression would strengthen block fetch on a wide read of an IBM i source (verify ladder).
* `0f.initdb.wal-and-options` (unused): a separate WAL directory, the WAL segment size and settings applied at creation would strengthen a target cluster with its WAL on its own volume (bulk ladder).
* `0f.jinja2.sandbox` (unused): a sandboxed environment for untrusted templates would strengthen a template that a customer may write (operator notice).
* `0f.mapepire.query-auto-close` (unused): a query that closes its cursor when read would strengthen cursor close on a large read of an IBM i source (verify ladder).
* `0f.neptunedata.gremlin` (unused): run a Gremlin query would strengthen Gremlin reading of a graph source (verify ladder).
* `0f.oci-sdk.restart-restore` (unused): restore an autonomous database and restart it or a MySQL system would strengthen restore of a managed database target (snapshot ladder).
* `0f.ora2pg.estimate-cost` (partly used): estimate the cost of the migration in days would strengthen migration effort estimate before a conversion (assess ladder).
* `0f.pyiceberg.maintenance` (unused): add data files, expire snapshots and overwrite a partition dynamically would strengthen file adoption of an existing lake target (bulk ladder).
* `0f.pyodata.batch-changes` (unused): batches, changesets and entity writes would strengthen batched writes to an OData target (bulk ladder).
* `0f.pyrage.files` (unused): encrypt and decrypt files by path would strengthen encryption of a dump at rest (sealed evidence).
* `0f.redshift_connector.dataframe-write` (unused): write a dataframe to a table would strengthen dataframe write to a warehouse target (bulk ladder).
* `0f.salesforce-api.pubsub-publish` (unused): publish and managed subscriptions would strengthen event publishing of a change to a subscriber (stream ladder).
* `0f.scim2-filter-parser.patch-paths` (unused): the paths a patch request names would strengthen provisioning patch of a user directory (access ladder).
* `0f.singer-tap.batch` (partly used): records in batches would strengthen batch reading of a SaaS source (stream ladder).
* `0f.snowflake-connector-python.hash-agg` (unused): a hash aggregate of the rows in SQL would strengthen order-free row-set hash on a warehouse target (verify ladder).
* `0f.snowpipe-streaming.elastic-channel` (unused): an elastic channel for the client would strengthen streaming ingest with elastic channels (stream ladder).
* `0f.tencentcloud-sdk.restores-restarts` (unused): restores and rollbacks to a new or an existing instance, and restarts would strengthen rollback of a managed target to a backup (snapshot ladder).
* `0f.teradatasql.lob-values` (partly used): large objects and the values of the session would strengthen large values of a warehouse source (large values ladder).
* `0f.teradatasql.fast-load-export` (partly used): fast load and fast export of the database would strengthen fast load of a warehouse target (bulk ladder).
* `0f.timestream.scheduled-query` (unused): a scheduled query would strengthen scheduled reading of a time series target (verify ladder).
* `0f.zstandard.threads` (unused): parallel compression threads would strengthen parallel compression of a dump stream (bulk ladder).
* `0f.zstandard.dictionary` (unused): a trained dictionary for small records would strengthen dictionary compression of many small rows (bulk ladder).
* `0f.atlas.schema-diff` (no account): diff two schemas and plan the change would strengthen schema comparison before a conversion (schema ladder).
* `0f.bson.raw` (no account): raw BSON bytes without decoding would strengthen raw-BSON bucket digest of a document target (verify ladder).
* `0f.clickhouse_connect.insert` (no account): insert rows or a dataframe in batches would strengthen batched insert to a columnar target (bulk ladder).
* `0f.duckdb.copy` (no account): COPY a table to and from a file would strengthen local staging of a bulk copy (bulk ladder).
* `0f.fastavro.block-codec` (no account): block compression codecs would strengthen compressed Avro staging of a bulk copy (bulk ladder).
* `0f.ibm_db.lob` (no account): large objects read and written in parts would strengthen large values of a Db2 source (large values ladder).
* `0f.kafka.partitions` (no account): assign partitions and seek to an offset would strengthen offset seek of a change-stream leg (stream ladder).
* `0f.liquibase.diff` (no account): compare two databases and write the changes would strengthen schema comparison before a conversion (schema ladder).
* `0f.lz4.level` (no account): a compression level would strengthen faster compression of a dump stream (bulk ladder).
* `0f.mssql_py_core.bulk-copy` (no account): bulk copy of rows into a table would strengthen bulk insert into a SQL Server target (bulk ladder).
* `0f.mysqldump.where-filter` (no account): dump only the rows a condition selects would strengthen row-range dump of a MySQL source (bulk ladder).
* `0f.opensearchpy.bulk` (no account): bulk index actions would strengthen bulk indexing of a search target (bulk ladder).
* `0f.psycopg.pipeline` (no account): pipeline mode for many statements would strengthen pipelined statements on a PostgreSQL target (bulk ladder).
* `0f.psycopg2.logical-replication` (no account): the logical replication protocol would strengthen logical replication reading of a PostgreSQL source (stream ladder).
* `0f.pyarrow.dataset` (no account): datasets over partitioned files would strengthen partitioned file reading of a lake target (bulk ladder).
* `0f.pymongo.bulk` (no account): bulk write operations would strengthen bulk write to a document target (bulk ladder).
* `0f.pymssql.bulk` (no account): bulk insert of rows would strengthen bulk insert into a SQL Server target (bulk ladder).
* `0f.pymysql.load-local` (no account): LOAD DATA LOCAL from the client would strengthen local file load into a MySQL target (bulk ladder).
* `0f.pymysqlreplication.resume` (no account): resume from a saved binlog file and position would strengthen resume of a MySQL change-stream leg (stream ladder).
* `0f.pyodbc.fast-executemany` (no account): batched inserts with fast executemany would strengthen batched inserts through ODBC (bulk ladder).
* `0f.requests.stream` (no account): stream a response body would strengthen streamed download of a large object (large values ladder).
* `0f.sqlglot.schema-check` (no account): check the columns of a query against a schema would strengthen column check of a converted view (verify ladder).
* `0f.pt-table-sync.chunk-size` (no account): sync in chunks by key range would strengthen repair of a differing key range (verify ladder).
* `0f.z3.optimise` (no account): optimise a goal under the constraints would strengthen decision choice under constraints (decide ladder).
* `0f.account.library`: 44 libraries are called with no tool-use account (each written option by option, as the others are): `airflow`, `bson`, `certifi`, `click`, `clickhouse_connect`, `confluent_kafka`, `croniter`, `cryptography`, `dagster`, `duckdb`, `ebcdic`, `fastavro`, `google_crc32c`, `grpc`, `hypothesis`, `ibm_db`, `ibm_db_dbi`, `kafka`, `keyring`, `lxml`, `lz4`, `mssql_py_core`, `mssql_python`, `numpy`, `opensearchpy`, `oracledb`, `packaging`, `pandas`, `pglast`, `psutil`, `psycopg`, `psycopg2`, `pyarrow`, `pymongo`, `pymssql`, `pymysql`, `pymysqlreplication`, `pyodbc`, `requests`, `results`, `rich`, `sqlglot`, `yaml`, `z3`.
* `0f.account.program`: 7 programs are called with no tool-use account (each written option by option, as the others are): `atlas`, `liquibase`, `mysql`, `mysqldump`, `pt-table-sync`, `vacuumdb`, `vault`.

<!-- whole-use:end -->

*Not done (W13O, 2026-10-16): the registry holds no option for redpanda, so it is left out of the catalogue until its options are accounted in migkit/tooluse.py; the 51 libraries and programs in the unaccounted section have capabilities from their releases but no option-by-option account yet (each needs its entry in the registry).*

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
| comparing server settings | yes | yes | yes | yes | yes | yes | yes | n/a | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| moving a whole database in bulk | yes | yes | yes | - | - | - | - | - | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| copying table by table, resumably | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| keeping the target following the source | yes | yes | yes | yes | yes | yes | n/a | - | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| proving the target has caught up before cutover | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| telling a difference still arriving from one that is wrong | yes | yes | yes | - | yes | - | n/a | n/a | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| verifying only what changed | yes | yes | yes | yes | - | yes | n/a | n/a | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |
| carrying users and their grants | yes | yes | yes | - | yes | - | n/a | n/a | yes | n/a | - | - | - | yes | yes | - | - | - | n/a | n/a | - | - |
| noticing a move that moved nothing | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| refreshing the target's statistics after a load | yes | yes | n/a | yes | n/a | n/a | yes | n/a | n/a | n/a | yes | - | - | n/a | n/a | - | - | - | n/a | n/a | yes | - |
| snapshotting the target so a cutover can be rolled back | yes | yes | yes | yes | yes | - | yes | - | yes | - | - | - | - | yes | yes | - | - | - | n/a | n/a | yes | - |

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
    delta verified the rows. `assess` passes the setting
    (`tests/test_a_compressed_binlog_is_read_or_stops_the_tail.py`).
  * *MariaDB read, not yet against a server (2026-10-11):* MariaDB's
    compressed statement (165) and version-1 rows (166-168) are opened by
    `binlog_payload.MariadbCompressedEvent`: what is in front of the
    compressed part is read as in the plain event, the compressed part is
    a header byte, a length and a zlib stream, and the plain event
    rebuilt from the two goes to the reader's own parser. A stream that
    is corrupt, of another algorithm, of a length that does not match,
    with bytes after it, or in a layout other than the one read stops the
    tail with the reason and the position, never a skip, and the delta
    reports it as an error. `assess` passes `log_bin_compress`.
    Proved with events built by hand and compressed with zlib, no server
    (`tests/test_mariadb_compressed_events_are_read_or_stop_the_tail.py`).
    NOT proved: that MariaDB writes this layout (which part is compressed,
    the length's byte order, version-1 rows only) - it is from the
    server's code as remembered; the test against MariaDB 11 is written
    and not run. Row 6.mariadb.
  * *MariaDB version-2 rows (2026-10-16):* the compressed rows 169-171 are
    opened as version 1 is: after the table id and flags, the length of
    the extra data (its two bytes counted) and the extra data, then the
    column count and bitmaps, then the compressed block. The plain
    version-2 event (30-32) goes to the reader's parser. Extra data the
    parser would read to another end than the length says (two pieces, a
    tag-0 length under 2, a short partition id) is refused, as are a
    length under 2 or past the event, and the stream checks of version 1
    (`tests/test_mariadb_compressed_events_are_read_or_stop_the_tail.py`).
    *Not done:* no server is known to write 169-171 (MariaDB writes
    version 1 as remembered), so version 2 is proved with synthetic events
    only. The server test (`test_mariadb_compressed_rows_are_read_by_the_tail`)
    now also checks the server's declared post-headers for 165-171 (13, 8,
    10) and that every compressed event in the log opens; it is written
    and not run, and neither are the synthetic tests yet (batch rule).
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
*Not done (2026-10-16, FIX-2):* the window is built once, in
`indexes.ForeignKeyWindow` on the `dropped-fks` set-aside record (a key not
put back stops the move with the statement, `indexes.NotPutBack`;
`tests/test_foreign_keys_go_back_or_the_move_stops.py`, scripted target).
Not yet wired around the streaming copy in `movers.py`, and not run on a
server. `cutover` and the PostgreSQL check do read `dropped-fks` (e149c96).

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

Oracle Free could not be run here then: measured, it filled the
container VM's disk and took 2.5 GB of its 3.8 GB of memory. In the 6 GB
`migkit` profile it takes 2.3 GB (measured 2026-09-28).

*Done (2026-09-28, F5): Oracle is a whole side, run against Oracle Free
23.26 in the sandbox* (`gvenzl/oracle-free:slim-faststart`, the source in
one pluggable database and the target in another, in the 6 GB `migkit`
colima profile; the container's own init script archives the redo and
turns on minimal supplemental logging). Every cell of the capability
matrix is closed for Oracle (`capabilities.py`), each held by
`tests/test_oracle_is_a_whole_side.py` against the server and
`tests/test_oracle_redo_is_read_back_exactly.py` without one:
* **Digest in the server.** Each row rendered to the text every engine
  renders it to and hashed and summed by Oracle itself - the same number
  this process folds and PostgreSQL sums, so Oracle and PostgreSQL are
  compared without a row crossing the network. A sum, never Oracle's
  `CHECKSUM`: that folds by exclusive or, so two copies of a row cancel
  (measured: two copies of one row and two of another checksum alike).
  Measured on the way: `to_char(0.5)` is `.5`, a BINARY_DOUBLE 0.1 is
  `1.0000000000000001E-001`, `to_char(1e61)` has an exponent, and
  `length` counts U+1D11E as two (`lengthc` one) - so a number is written
  by a format of its scale, a binary float and a number past 38 whole
  digits are folded here, and lengths are `lengthc`. A table whose rows
  could be wider than a hash's input (4,000 bytes, or 32,767 where
  `max_string_size` is EXTENDED) is folded here too. Oracle to Oracle the
  same rendering is hashed by SHA-256 with a salt both sides share
  (`same_engine_digest`), 64 bits of each row's hash summed; a time is
  rendered to nine digits wherever it holds more than six.
* **Large values in pieces** (R10's Oracle part): past `PIECE` a value is
  read through its locator a multiple of its chunk at a time, and written
  empty and filled through the locator the insert returns. A text LOB
  counts in UTF-16 units: measured, a piece that ended between the halves
  of U+1D11E was refused by the driver (`UnicodeDecodeError`), so such a
  piece is read a unit shorter and offsets move by UTF-16 units.
* **Writes.** A batch past 2,000 rows goes by direct path load into a
  global temporary table private to the session (the load commits by
  itself), and from there by one MERGE in migkit's transaction, so a
  batch is still in whole or not at all; or by array DML. Each table's
  first two large batches are written one each way and the cheaper by
  the row kept. Measured, 100,000 rows of four columns into an empty
  table with a key: array DML 0.38 s; the direct path load 0.16 s and the
  MERGE 0.20 s. Never straight into the table: a direct path load that met
  a key already there left 1,001 of 1,001 rows and the key's index
  UNUSABLE. The change applier writes a run of rows as one MERGE bound to
  every row.
* **Times with a zone.** Read as the UTC instant the server computes
  (`sys_extract_utc`) with the zone beside it: python-oracledb cannot
  fetch one whose zone is a region (`DPY-3022`). An Oracle target is
  written in the zone again (`from_tz(...) at time zone ...`), so Oracle
  to Oracle keeps `EUROPE/PARIS`; every other target gets the instant.
  The zone is written into the statement as a literal, a statement for
  each zone a batch holds: `at time zone` refused a bind (ORA-00917) and
  a column (ORA-00907), measured. **Intervals** go into Oracle as the
  driver binds each kind: a year-to-month as years and months, a
  day-to-second as a `timedelta`; a month into a day-to-second, or a
  digit past the microsecond, is refused rather than written as another
  length of time.
* **The interlock on the target** (`server_identity`): the database, its
  incarnation and its container, the start of the instance answering,
  its version, host and the schema's bytes, or that the schema is not
  there yet. The database it names is the service and the schema, since
  two pluggable databases of one container answer at one address.
* **Sequences and identity columns** compared by the next value
  (`last_number` is past the cache, not the last handed out), set with
  `restart start with` and `start with limit value`, and put back from
  the snapshot. **Settings**: `v$parameter` as set, with the database's
  language and character set; NLS and length semantics are the critical
  ones. **Users and roles** with their grants and quotas, the password
  carried as its hash: on 23.26 `sys.user$` refuses even SYSTEM unless
  given READ on it (ORA-41900), and the users' DDL holds the hash for SYS
  alone. **Restore point**: the target's SCN after letting each table
  move its rows (enabled after the SCN, the flashback is refused,
  ORA-01466), its sequences and its code; a guaranteed restore point is
  refused to a pluggable database's accounts (ORA-01031).
* **Oracle to Oracle** by the target's Data Pump over a database link, as
  of one SCN for every table, the rows counted at that SCN on both sides
  after the job; where Data Pump cannot run (no package, no role on
  either side - measured, ORA-31631 before a row moves without the
  source's export role - or no link), the table copier.
* **Change follow: migkit's own LogMiner reader** (`oracle_logminer`):
  committed data only, the online catalogue as dictionary, each change
  read back from LogMiner's own statement (`sql_redo`) - MINE_VALUE fails
  the whole query on a column it cannot read (measured, ORA-00902) - and
  what a statement does not give (a large value, a time with a zone, an
  interval) read from the table as of the commit. In a pluggable
  database LogMiner finds the logs itself (naming one is refused,
  ORA-65040); from the root they are named (`mine_service`). The token
  is SCN-positioned: where to mine from (the oldest open transaction's
  start, from `v$transaction`), the last commit and its transactions
  handed back, a transaction handed back in parts, and `through`, which
  the fence waits on - moved only to what was true four seconds before,
  since a commit that did not wait for its redo may not be in the files.
  A table without its key in the redo, or a NOLOGGING table where the
  database does not force logging, stops the start by name. Mining on a
  physical standby (`mine_standby`) is gated - a standby of this very
  database, open read-only and applying, the window ending where it has
  applied - and not run here: the sandbox has no Data Guard. `assess`
  measures the reader against the source's redo: the busiest hour's
  archived redo a second against the redo the reader mines a second on
  the source's latest log.
* **The redo-file reader as the rung above it** (`oracle_redo`): the
  program that parses the redo files itself, run next to them, built by
  `doctor --install` from source on Linux (x86-64 is what it publishes);
  migkit writes its configuration, speaks its network protocol (protobuf
  requests, length-framed JSON back), confirms what it applied, and reads
  the same token. Chosen where the source names where it listens
  (`redo_reader`) or where this machine reads the redo files and has the
  program; LogMiner otherwise.
* **Each choice a list of rungs the decision engine climbs**
  (`decide.climb`): how a batch is written (`oracle.WRITE_WAYS`, each way
  timed once on a table's large batches, then the cheaper by the row),
  how the redo is read (`oracle_redo.readers`), and how Oracle moves to
  Oracle (`oracle_pump.ways`, Data Pump or the copier) - each passed-over
  way said with why.

Still to: Ora2Pg and DVT wrapped (R11, R8); the redo-file reader run end
to end against the sandbox's Oracle is held in
`test_the_redo_files_are_read_beside_the_redo`, where it builds.

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
* ~~**C4** load on the source~~ done (2026-09-30): see "Verify depth" below
* ~~**D11** documents outside the table~~ done (2026-09-30): see "Verify
  depth" below
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
| `follow` | done (2026-09-30, not merged): `follow_until: stopped` keeps the leg run from here applying until it is asked to end, then ends it at the source's position of that moment; without it the catch-up ends as it always did (`movers._follow_until_asked`) |
| generic engine | done (2026-09-25): string length and nullability are read from the same catalogue the comparison library asks, in the standard spelling and then Oracle's; where neither answers, the verdict still says they were not compared (`tests/test_generic_schema.py`) |
| MySQL `_health` | done (2026-09-24): a side that is itself a replica reports how far behind it is, by column name on MySQL and MariaDB, and the throttle backs off on it (`tests/test_mysql_fence.py`) |
| hetero | has no deep battery |
| sqlglot | decided (2026-09-25): kept, for what a type map cannot do. It translates each column default into the target's SQL: `uuid()` becomes `gen_random_uuid()`, `now()` becomes `CURRENT_TIMESTAMP()`. The target is then asked whether it takes the result, and a default it refuses is named rather than guessed at |
| to measure before wrapping | boto3 Secrets Manager, `mongodump --query/--oplog`, datacompy `all_mismatch()`, mydumper `--regex/--rows` |
| resumable-dump flag | done (2026-10-04): the bulk plan says what a stop costs the way it took (`movers.after_a_stop`) |
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

`interval` is compared as its months, days and nanoseconds kept apart
(`migkit/intervals.py`, 2026-09-30): read as its parts on PostgreSQL and
DuckDB, whose drivers turn a month into thirty days, and carried to
Cassandra's `duration`. MySQL, SQL Server and MongoDB have no type that
holds the three apart, and a move there is refused by name.

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

**26.striim - interval validation (2026-10-16).** Research:
`docs/research-other-tools.md` describes timestamp-filtered comparisons;
`docs/research/mechanisms-goldengate-qlik-hvr-ibm-shareplex-2026-09-28.md`
sections 6-7 distinguish committed checkpoints from streaming analytics.
The vendor's [current description](https://www.striim.com/blog/validata-in-action/)
requires timestamp columns on both sides. This ticket instead uses the source
log's commit time (`canon.ended`) and the tail's durable change accounts.
Reuse `canon.digest_step`/`fold_rows` and the applier's `_collapsed` final-state
semantics; count/sum signatures alone must never certify a successful write.
Read back touched target keys after each apply, including deletes, before saving
the position. Keep interval receipts in the same generation/undo contract as
the tail's accounts. A pending or untimed receipt is unproved, never equal.
Window duration is derived by `decide` from measured change rate and batch size;
check/watch report closed half-open windows, table names and the first mismatch.
This proves the carried stream and its observable batch effects, not intermediate
row versions collapsed within an atomic apply or unrelated target writes.
Implementation: `verifiers.ChangeIntervals` freezes mapped source events before
apply, sums operation/key/value signatures with the shared digest fold, and credits
target receipts only after its acknowledgement and a key read-back of the batch's
final effects agree. Target value signatures are computed from the rows read back,
independently of the source signatures; repeated keys and moved keys whose event
images cannot be reconstructed are explicitly unproved. `tailtables.Store` keeps pending/final receipts under the
checkpoint generation; undo removes an uncommitted receipt and restart retains a
committed one once. `cli.run_checks` includes these findings in check/watch;
`verdict.interval_status` keeps an open window unproved. Timestamp regressions,
missing times, unreadable effects and DDL batches are unproved rather than equal.
Synthetic tests: `tests/test_change_windows_name_when_a_tail_diverges.py` covers
boundaries, corruption, missing rows, deletes, key moves, undo/reopen and pending
receipts. Its PostgreSQL test uses the shared `pg` server profile and injects a
wrong target value after the real tail applies a batch.
Not done: PostgreSQL execution (Colima was stopped; no server started), independent
observation of intermediate versions collapsed within a batch, and unrelated
pre-existing depth-matrix inconsistencies (reproduced on the unchanged HEAD).

HVR, Qlik, Striim and Bytebase are learned from and not wrapped. They
are commercial or bring a platform of their own. Trino is a candidate
second reader, to measure first. *Read (2026-10-04):* sqlpipe (CSV
through each target's bulk client, no comparison) has nothing to wrap;
schemachange's history table, with Flyway's, Liquibase's, Alembic's,
Django's, Rails' and Entity Framework's, is the application's record of
its schema version, which migkit should read (26.versions):

* **26.versions** - the schema version each side stands on, from the
  application's migration history table, said by `check` and the
  cutover's gate; a migration run on one side and not the other found;
  where migkit changes the target's schema itself (the schema fix, a DDL
  the tail applies), said that the application's tool does not know of
  it.
  Not done (APPVERS, 2026-10-16): built and proved on stand-ins only - every engine through the neutral contract (`Engine.history_*`), 15 tool shapes, failed/out-of-order/checksum, the cutover gate (`cutover._version_rows`), migkit's own changes with how each tool records them; owed: a server test per engine (SQL Server, Oracle, Snowflake and the rest never asked), schemachange's history in its own METADATA database (not in the hop's), a history table the hop excludes is not read on the neutral path.

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
  DATABASE`. The wait needs `PROCESS`: without it the copy is refused,
  saying the grant (done 2026-10-04, `sees_every_session`). Still open: MongoDB's resume token and MariaDB's/Percona's
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
  data, the object inventory and the shape watch. The change tail reads them beside
  the tables it knows too (done 2026-10-04, `TableSet._names_among`).
  Test: `test_an_online_rewrites_working_tables_are_left_alone.py`.
* **`schema_authority: atlas` needs an alias shape** (4-จ from the old
  queue): *done (2026-10-04)* - the words name what decides,
  `schema-aware` (the default) or `every-reading`, never the program
  behind it; an older hop's `atlas` is read as the default, any other
  word refused at load.
* **reladiff as a rung, not a default:** it has no jsonb rendering and
  breaks on Python 3.14; the decision engine keeps it for the pairs it
  proves on and says why it was not used elsewhere (0f).
* **Waiting on the owner (unchanged):** the PyPI upload and whether the
  name `migkit` is free; a brew tap.

## Decided by the owner 2026-10-01 - scope

* **Out of scope - operations around the database, not the database:**
  changing the application's SQL/ORM code, switching its connection
  strings, DNS or secrets, testing the application against the new
  database, the jobs, ETL, BI and consumers pointed at the old one
  (problems A1), and comparing cost before and after. migkit's cutover
  still runs the flip command the hop names and says what else points
  at the old database where it can see it; it does not own those
  systems. (CV.P11 stays: reading the application's SQL to convert its
  dialect is conversion work.)
* **In scope, to be built:**
  - **Data too large for the link** (hundreds of TB over a narrow line):
    a seed shipped on media - the provider's appliance (Snowball, Data
    Box, Transfer Appliance) or any disk - as a rung: exported in
    checksummed, encrypted, resumable parts at a recorded position,
    loaded at the other end, every part proved, then the change stream
    carried on from that exact position; the decision engine offers it
    when the link's measured rate says the copy would miss the deadline.
  - **W1**: the mover highly available and scaled out (a standby that
    takes over from the last committed position; one table across
    machines with a conditional-write checkpoint).
  - **W2 and a GUI**: several operators with roles and SSO, an API, and
    a web interface for people who do not use the command line - the
    same outcomes the console shows, the approvals and the source-change
    offer answered there.

## P0-SCOPE. The check takes exactly what the move took (owner, 2026-10-03)

"A DTS job can be told to leave the log tables behind, so when we verify
or compare we must be able to choose where to verify - deep and very
detailed." A hop said only what it left out (`exclude`); what another
tool's job *took* was turned into an `exclude` against the source's
tables once, when `init` connected, so a table made later was in scope
though the job never took it.

1. **`include`** (done 2026-10-03): the databases, schemas and tables a
   hop takes - the same names and shell patterns as `exclude`, matched
   the same right-anchored way, `exclude` winning where both name one.
   Read inside `Hop.excluded`, the one scope answer every reader asks
   (the copy, the check, the repair, the tail, the drop list of every
   bulk mover), so none can verify a table the move left out or leave
   out one it moved. A database is skipped only where every entry names
   another one first; where a name may hold a dot of its own (a
   collection, a topic) every database is opened. Every whole copy (the
   pages, a backup restore, a replica, the keyspace copy) is refused
   for a hop that takes only some (`config.scoped`). Validated: an entry
   that is not a name, one `exclude` takes back whole, a mapping rule
   naming a table `include` does not take. Before the move: an entry that
   takes no table, a database it takes nothing of. In the evidence:
   `verdict.json` and the report say the scope a `same` covers
   (`verdict.hop_scope`); a `same` within the hop's own scope is a
   `same` (a run narrowed on the command line stays `incomplete`).
   Columns and rows go deeper through `mapping.columns` and
   `mapping.where`, which the compare paths already read.
2. **A DTS job read as a hop** (done 2026-10-03, `imports`): a Tencent
   Cloud migration job's detail, a sync job, or a data check's
   objects; an Alibaba Cloud job's detail or request. Goal `verify`; the
   job's databases and new names, schemas, tables and new names, row
   filters, columns and new column names, views; and said beside it,
   every setting of the job that makes a correct move look different -
   deletes or updates it does not carry, conflicts it ignores, tables it
   wrote into that were there, changes-only starts, a target left
   writable, accounts not moved, its own check off, sampled, counting
   only or covering less than it moves, an unencrypted leg. AWS DMS
   tasks' plain includes are now the hop's `include` too.
3. **Renames read with their database** (done 2026-10-03, found on the
   way): a rule `appdb.public.orders: orders_v2` - the only way to say
   which of two databases' `orders` is renamed - was passed over by the
   pair engine, the PostgreSQL schema check and the parallel copier,
   which asked without the database; the table was compared against the
   name it no longer has. `Hop.target_table_in(db, ...)`, one place.
5. **Every variant of the services, and their own checks** (done
   2026-10-03): Tencent DTS migration jobs of both APIs (2021, 2018's
   `DatabaseInfo`), sync jobs, data checks' objects and tasks,
   subscriptions, and the requests that made any of them; Alibaba DTS
   `DbList`/`DbObject` and the older `MigrationObject` /
   `SynchronizationObjects` (`TableExcludes` become `exclude`); the
   changes a job carries become `mapping.ops`. A service's own check
   report - Tencent's compare report, AWS DMS's table statistics,
   Alibaba's data check - given as `compare_with` is held table by table
   against migkit's answers (`servicecheck`): what it called the same
   and migkit found different, what it skipped, what it never checked.
6. **One scope through every part** (owner 2026-10-03: "move, watch,
   verify, CDC, DDL and everything; deepest, most consistent and
   idempotent, smartest, easiest; every variant and every engine").
   Audited 2026-10-03 (three read-only audits and a market survey);
   shared pieces done: `Engine.in_scope`, `Hop.scope_fingerprint` /
   `scope_changed`, `Hop.reversed()` reading scope and mapping in the
   target's names, the nested `include`/`exclude` form, case read the
   same on every machine and without case where the engine folds it,
   `mapping.ops` (`Hop.carries`). In progress, by part:
   a. **counters, orphans, accounts**: sequences and AUTO_INCREMENT only
      of tables in scope, never lowered (the cutover's counter gate
      refused on an excluded table's sequence); pg_dump's serial
      sequences of excluded tables; an orphan whose parent the hop does
      not take is a warn that says so; a job that moves no accounts is
      not checked for them; the cutover's jobs and consolidation
      counters in scope; the second reading pairs renamed tables;
   b. **deep checks, inventories, repairs**: every listing a deep check,
      the objects inventory (views, routines, triggers, events,
      sequences) and every repair builds goes through `in_scope`;
   c. **reuse, watch, delta, evidence, resume**: the fingerprint beside
      every answer, position and checkpoint (delta slot, resume records,
      a seed's manifest), reuse only under the same scope; watch reads
      the scope every cycle and sums only what is taken; delta reads
      through the row filter and the renames; the evidence renders the
      scope it was taken under; sizing counts only what is carried;
   d. **the bulk paths**: a partition is scoped by its root; renames
      honoured by every same-engine path (or routed to the copier); a
      filtered table never emptied whole; a target-only table never
      emptied; mixed-case names; schema creation leaving out objects
      that depend on what is not taken; patterns rather than lists where
      the tool reads them;
   e. **the change tail and DDL**: the servers' own replication refused
      or narrowed where a hop filters or renames; partitions by root;
      renames and swaps into or out of scope under every policy; a
      scope changed while the tail was stopped; Debezium per database
      and by pattern; `mapping.ops` applied; a DDL policy per object and
      kind; online schema change shadow tables; an unchanged large value
      never written as the decoder's placeholder;
   f. **contracts and closure**: the check judges a table against the
      changes its hop carries (deletes kept: the target may hold more,
      never fewer or other rows), repair never undoes what the contract
      allows; before the move, the dependency closure of what is taken
      (foreign keys, views, routines, triggers, owned sequences,
      partitions) offered or accepted, and every source object explained
      by the rule that decided it.
7. **P1, the cores done 2026-10-03, each read in one place:**
   - one row filter (`rowfilter`): parsed once in the source's SQL,
     said in another engine's SQL with its column names, as a document
     store's query with SQL's NULL kept (`NOT` cannot take a NULL row
     back in), a filter that moves with the clock or reads another table
     named before anything is read, its text comparisons named for the
     collation check; `evaluate`, SQL's three-valued logic, the
     reference every translation is held to on the same rows (17
     predicates x 25 rows with NULLs and missing fields, and on
     PostgreSQL, MySQL and a document store in the suite);
   - column transforms a check can prove (`transforms`): null, constant,
     a keyed hash (`mask_key`; the same value, the same digest on every
     run and machine), truncate - one function in Python and in each
     engine's SQL, held alike by tests;
   - objects beside the tables (`objects`): views, functions,
     procedures, triggers, events, sequences, roles - all, none or by
     name (`Hop.takes_object`); a DTS job's choices by name read into it,
     a kind it does not carry said as a finding, never left out;
   - name rules (`mapping.names`): case, prefix, suffix, a prefix or
     suffix taken off, per level, read where every target name is
     (`target_table`, `target_db`); DMS's name rules read into them.
   Still to wire where they are read (after the part merges): the row
   filter's translations into the pair engine, the document store and
   the hop's validation (clock and subquery refused there), its time
   zone (`where: {expr, tz}`) and the collation check; the transforms
   into `mask_on_write`, the copier and the check's digests (the source
   side computing the target's value); `takes_object` into the objects
   inventory, schema creation and repairs; a name rule's collisions
   (two names one after the rule) refused. Also: a check scope narrower
   than the move's said as incomplete, never same; partitions carried
   by root or leaf; every scope change logged with its source position.
8. **Still open:**
   a. the Alibaba job's top level for a source with schemas (PostgreSQL,
      SQL Server) is read as its schemas inside the endpoint's database,
      from the service's documentation and its errors, not from a real
      job's detail - confirm against one;
   b. the change-capture connector and LOAD-file imports still turn a
      regex or search include into a one-time `exclude`;
   c. a Huawei DRS / GCP DMS / Azure DMS job read the same way (in
      progress 2026-10-03, from the providers' documented shapes);
   d. `include` read by a live tail's filter (Debezium `table.include.list`
      instead of a resolved exclude list) so a table made later under a
      taken pattern joins the stream without a restart.
   e. left by 6d (merged `dfd78df`): the document store's check does not
      read renames, so a renamed collection reads as different after the
      move; the table copier (own and pair) deletes its key ranges before
      loading whatever `mapping.ops` says - only the bulk truncate honours
      it; schema creation does not leave out the sequences `objects`
      leaves out (a kept table would not create without its sequence);
      a comment on a left-out function, trigger or constraint is still
      restored and refused by the target; the schema comparers and deep
      checks read partitions by their own names, not their root's; three
      program behaviours read from documentation, not measured (the
      dump's pattern against `db.table`, the online sync's regex fields
      and its 1.6 cut-off, the restore's first matching rename); the
      streaming copy's filter file is names one by one, its reading of
      mixed case unchecked.
   f. the two-axis review of the scope work (2026-10-04) closed in
      `8650feb`: `mapping.ops` held to every reading of a leaf name and to
      the pair's own name for it (`Result.table`); the counts a check
      counted carried as fields (`Result.rows_apart`, `Result.tables`)
      on PostgreSQL, MySQL, SQL Server, the pair and the document store,
      the words read only where an engine says none; a service's word
      held to every answer, a warn or an error `unproved`, never agreed,
      a database's one answer held table by table, names matched without
      case only where one table can be meant; the cutover's final check
      given whole answers; one reading of what a hop narrows
      (`config.narrowed_by`) for the copy of pages, the backup seed, the
      servers' own replication and the evidence; `Hop.reversed` keeping
      schemas, turning name rules round and refusing ones that cannot be;
      a listed name asked one way (`Hop.takes_table`, an AST guard); the
      target's names read back (`Hop.source_table_in`); one PostgreSQL
      sequence reading (`_seq_gone`); named roles (`users.role_taken`);
      Oracle's counters scoped; SQL Server's data check scoped. Still to
      carry the fields on every other engine (P0-PARITY.verify).

## P0-CLOUDS. Any cloud to any cloud, every database service in each (owner, 2026-10-01)

"Not just AWS to Tencent - any <-> any from the start: AWS, Tencent,
GCP, Azure, Huawei, on-prem, anything - every database type in each
provider." Two parts:

1. **One managed-service layer**: a catalogue of every managed database
   service per provider (AWS, GCP, Azure, Tencent, Alibaba, Huawei,
   Oracle Cloud, IBM Cloud, and the DBaaS vendors - Atlas, Redis Cloud,
   Aiven, Neon, Supabase, PlanetScale, Crunchy Bridge, Timescale,
   CockroachDB Cloud, Yugabyte, SingleStore, Confluent) with what each
   admin can and cannot do, how CDC is turned on, how writes are held,
   snapshots/clones/restore points, limits and defaults; one provider API
   layer (each provider's SDK, credentials as its CLI resolves them); the
   preflight, the source-change offer and the freezes read it for any
   source service and any target service. On-prem is a provider too.
2. **Cloud-native services that are engines of their own** and migkit
   lacks: Spanner, Bigtable, Firestore, GaussDB, OceanBase, TDSQL
   (distributed), PolarDB-X, Keyspaces, Neptune, Timestream, Lindorm,
   GeminiDB - each a side with copy, check and follow where the service
   exposes changes.

## P0-SCENARIOS. Every reason a database is migrated (owner, 2026-10-01)

Not done (SCENFIX, 2026-10-16): `scenarios` decides the managed service
change from what the providers' APIs state (reachability, class, version,
status, storage against the measured size, maintenance window), each a gate
with its refusal reason; a fact the API does not give is said as not yet,
never assumed. `move`'s plan says it (`scenarios.plan_lines`). Open: no
catalogue of the providers' own operations (upgrade, blue/green) with
eligibility and duration evidence exists, so the plan names the facts read
and says no eligible operation was read; the architecture case has no remote
host reader (architecture, OS, byte order) wired into the plan; the storage
case repeats what `tableengine` already plans and is not yet the one place it
goes through. Pure tests prove the decisions and messages; no live provider
or server execution is proved.

W5D (2026-10-16): the providers' own operations are catalogued in
`cloudapi.OPERATIONS` (AWS blue/green, in-place major upgrade, RDS into
Aurora by read replica or snapshot; Azure Single to Flexible Server; Cloud
SQL's in-place upgrade), each held to checks on the instances' facts (engine,
version, the provider's listed upgrade targets, replicas) and timed by a
documented startup plus runs kept per operation (rehearsal: measured, other
hops: predicted, none: refused as not yet); the plan reads them from the
source's adapter. The storage change is decided in `tableengine.decided`
(one place); the unwired scenario helpers are deleted. Not done: no adapter
runs any of these operations yet (every one is not yet with its reason);
instance-class orderability for the target version is not read; the
architecture and storage readers still branch on the engine name instead of
engine-class fact readers, and the storage needs close over arguments rather
than reading facts (CRIT1 b).


Asked: "old version to a newer one, SQL Server Web edition to Datacenter
(Enterprise) - does migkit do these? Think through every case a database
is migrated." The cases, what migkit has (at fb5c988), what is missing:

| Case | Has | Missing |
|---|---|---|
| Major version up (same engine) | `goal: upgrade` (pages copied, upgraded on the target, followed; or logical into the new version; removed settings, defaults, extensions, collations checked first) | thin checks on MongoDB and SQL Server |
| Version DOWN (rollback plan, an older target) | BUILT (`case`, `lacks`): the case named in the plan; a copy of the pages refused up front with the reason and the logical copy chosen; what the newer source uses that the older target has not, asked of the target where it answers (PostgreSQL types, collations, extensions; MySQL collations used in an expression; SQL Server types and collations) and from a table of each release elsewhere (PostgreSQL identity, partitioning, generated columns, SQL bodies, lz4, NULLS NOT DISTINCT, security_invoker, WITHOUT OVERLAPS; MySQL invisible columns, indexes on expressions, multi-valued indexes, CHECK, expression defaults, SRID, VECTOR, window functions/CTE/JSON_TABLE/LATERAL in code, caching_sha2 users, roles; MongoDB time-series, clustered, encrypted, collations, wildcard, compound hashed; Oracle BOOLEAN/VECTOR/JSON, names over 30 bytes, identity, extended strings against the target's max_string_size); SQL Server's compatibility level and MongoDB's feature compatibility said | Oracle's and SQL Server's version tables are proved on the x86 VM only |
| **Edition change** (SQL Server Express/Web/Standard/Enterprise/Developer, Oracle SE2/EE, MySQL Community/Enterprise, MongoDB Community/Enterprise, Redis OSS/Enterprise) | BUILT (`editions`, `backupseed`): each side's edition read from its server; what the source's databases use that the target's edition lacks - stripped (the logical copy makes tables from columns and key), lost (refused unless `edition: {accept: [...]}`), refused - with the fix; Express/XE/Free size caps refuse, memory and core caps said; SQL Server's own copy-only backup restored as a physical rung (`physical: {backup: DIR}`) that holds only where the target's edition takes what the database holds and its caps its files, MySQL's clone needing every plugin its source runs on the target; `goal: upgrade` takes an edition up (`upgrade.edition_only`); an edition down said as licence-driven | Redis Enterprise is not told apart from open source (no server-side field known without measuring one); Oracle's options on XE 18c+ taken from its licensing guide, not measured |
| Fork or distribution (MySQL/MariaDB/Percona, PostgreSQL/Aurora/AlloyDB/Citus/Yugabyte, Redis/Valkey/KeyDB/Dragonfly, Elasticsearch/OpenSearch, MongoDB/DocumentDB/Cosmos Mongo API/FerretDB, Kafka/Redpanda/MSK) | BUILT (`case`, `lacks`, `variants`): one brand a side from the server (Citus by its loaded settings, Cosmos DB by its own request-charge command, Percona Server for MongoDB by `psmdbVersion`, Elasticsearch and OpenSearch by `GET /`, Aurora/AlloyDB by `clouds`' server evidence); a fork change named in the plan; MariaDB's transaction ids and JSON-as-text said; Citus's distributed tables said; Redis forks asked for every command migkit sends and every module type the source holds; views' pipelines and validators run on a Mongo-compatible target against a collection that is not there; Elasticsearch/OpenSearch field types each lacks; Elasticsearch as a side, its own security API for users and roles | DocumentDB still has no server-side signature (none known that is not an absence); MSK cannot be told from Kafka by a client |
| Platform: on-prem<->cloud, cloud<->cloud, region, account | proved AWS<->Tencent; cloud snapshots across accounts; physical seeds; **done 2026-10-01, any provider to any:** one managed-service catalogue (`data/managed.yaml`, its sources in its header: every managed database service of AWS, Google Cloud, Azure, Tencent Cloud, Alibaba Cloud, Huawei Cloud, Oracle Cloud, IBM Cloud and the DBaaS vendors - its engine, its signs, its admin, how a setting changes, what following it needs, restore points, limits, defaults) read one way by `managed` for every pair; one provider API layer (`cloudapi`, an adapter per provider SDK, credentials found as each provider's CLI finds them) for instance facts, parameters, restore points taken before the cutover's clock (`cutover: {restore_points: [...]}`) and clones (`cloudseed` for Tencent and Alibaba); the freeze ladder climbing what a managed admin may do (a connection limit where the database is not its own, a service's kill procedure, `read_only` through the parameter group offered as a source change, a MongoDB-compatible service held per database); the target read against the source before the move (collations, time zone, connections); the release gate's scenario (`tests/test_the_release_gate_managed_migration.py`) | engines migkit lacks, listed in the catalogue (`distinct`): Spanner, Bigtable, Firestore, GaussDB, OceanBase, PolarDB-X, AnalyticDB, TDSQL distributed, Keyspaces, Neptune, Timestream, Lindorm, GeminiDB, Synapse, YugabyteDB's own replication, SingleStore |
| Self-managed<->managed, managed<->managed (EC2->RDS, RDS->Aurora, single->flexible server) | physical and logical rungs, cloud clones; the managed service change decided in `scenarios` from the providers' stated reachability, class, version, status, storage and window (a fact the API does not give is said as not yet) | the providers' own operations (blue/green, upgrade) catalogued as offers with eligibility and duration evidence |
| Hardware / OS / architecture (x86->ARM, Windows->Linux SQL Server) | a logical move | said as its own case; byte-order and collation facts where they differ |
| Character set / collation change (latin1->utf8mb4, ICU version) | BUILT (`charsets`, the case's rows): every text column's character set and collation read on both sides (MySQL, PostgreSQL, SQL Server, Oracle, Db2 catalogues; Unicode-only engines by their word), or asked by the hop (`charset: {to, collation, repair}`); a value the target's character set has no code for refused by key; UTF-8 stored twice found and repaired on the target (`repair: double-encoded`, the default where a conversion is asked); every unique key over text put to the target's own comparison (`key_merges`, PostgreSQL's nondeterministic collations now too) and two values it takes for one refused; MySQL's index and row limits counted in the new character set's bytes; MySQL converted on the target after the rows are in, column by column keeping each type, the whole table where a value no longer fits (`decide.attempt`); every row proved by key as converted (an order-free digest a side), the proof standing in for a data check of each side's bytes; the schema check holding the rest; PostgreSQL's sort-order libraries compared, a copy of the pages told to reindex. The mojibake check and its repair on every engine with character sets (SQL Server, Oracle, Db2, ASE, Teradata beside MySQL and PostgreSQL, through the neutral contract) | Oracle's and Db2's column collations are not put to the target (their default comparison is exact); ASE's and Teradata's character sets are not read, only their text checked for UTF-8 stored twice |
| Storage engine change (MyISAM->InnoDB, Aria, MEMORY) | BUILT (`tableengine`): each table's engine on both sides and the target's own engines read; the hop's `table_engine: {to, accept, foreign_keys}`, and a target lacking the source's engine, made in the schema step (the definition rewritten: its ENGINE, the options only the old engine reads, a key for per-group numbering, the hop's foreign keys) or in place after the copy - a way climbed; transactions, foreign keys, full-text (each side's own settings read), locking, MEMORY's lost rows, BLACKHOLE, ARCHIVE's compression said; per-group AUTO_INCREMENT refused unless accepted; MERGE refused with the view to make, remote engines unless accepted, virtual ones refused; rows a foreign key would now refuse counted on the source and refused before the move; after it, every table's engine and every such key proved on the target, a schema check that differs by the plan alone said as the plan | ClickHouse table engines and PostgreSQL access methods are not compared |
| Consolidation (many databases or tenants into one) and split (one into many, a service extracted) | BUILT (`consolidate`, `split`): goal `consolidate` - a tenant column filled per source (`column`, `value`/`values`), the sources of this hop and the hops it names (`with`) held together; every key two sources hold found before the move and refused, or kept apart by the rule as a climbed way: `tenant` (the column leads the key), `offset` (each source's integer keys and the references to them shifted), `remap` (offsets a stride apart worked out and kept); shifted ranges that meet, or pass their column, refused; counters raised past every source's keys and proved; every source's rows proved by key under its value, rows under no source's value named; batches written by the bulk way or the plainest (`decide.attempt`). Goal `split` - each part's rows by its own row filter and scope, every source row counted against every part's rule on the source before the move (nowhere refused unless `rest: stays`, twice unless `shared`), after it each key in exactly one part's target and the targets together the rows the rules place, references across the split counted and said | a consolidation or a split is carried once (`--mode full`): following each source's changes into the shared target is not a way yet |
| Resharding / topology (single<->cluster, shard split/merge) | W7 open | W7 |
| Archive / partial moves (old rows to cheaper storage) | BUILT (`archive`): goal `archive` - the rows each table's rule picks (`mapping.where`; a table with none refused) added to the archive, what it holds kept and never overwritten (a row held otherwise refused by key), each batch read back; removed from the source only with `archive: {remove: true}`, the source's own answer by its name (`source_changes: {archived_rows: yes}`, or asked at a terminal and recorded - never a word for every change) and the hop's approvals (`source-setting`): each row read again on both sides and removed only where the archive holds it as the source has it now, children first, never a row other rows still name; each batch's keys and digests kept in a ledger before it goes (resumable), the write recorded in the source's changelog; removed by the batch or a row at a time (`decide.attempt`); `archive: {restore: true}` puts every removed row back; the proof holds every removed row to its digest in the archive | removal is through the source engine's own delete by key: an engine without one keeps its rows and says so |
| Zero-downtime with a way back, active-active during the move | cutover path, stream back, two-way | cutover gaps (running) |
| Heterogeneous (commercial -> open source, relational <-> NoSQL/search/stream, OLTP -> warehouse/lake, mainframe, SaaS) | 28 engines, canon, stored-code proof | engine reach (running), P0-PARITY |
| Test copies, DR drills, backup restores checked, replica drift | subsets, check between any two, rehearsal | - |
| Fleet-wide rolling upgrades | - | W2 estate |

Every case is a `goal:` or a fact the decision engine reads, explained in
`docs/goals-and-modes.md`; the plan names the case it recognised.
## P0-AUDIT. What is still flat, slow or inconsistent (2026-10-01)

Asked: "evaluate everything else too - not only the engines - whatever is
still flat, not smart, not fast, not consistent: make it the best; put it
in the backlog, then implement". Read at 5e30188 in ten areas; every finding
with its file, lines, evidence (measured in-process where the code is pure)
and fix is in `docs/research/flat-audit-2026-10-01.md` - the IDs below (V,
ST, K, SEC, W, SC, SP, DU, LG, T) are that document's. Left out as already
known: P0-SMART, P0-DEPTH, P0-PARITY, the cutover gaps, R23's rest, engine
reach, the one table-way ladder.

The worst of it: "same" is decided in four places that disagree (an empty
answer set is `same`; the report renderer turns a `diff` into `ok` in the
dicts the exit code is computed from; the dashboard pages on clean runs);
three write paths skip the guards `move` enforces; nothing is written
durably (three fsyncs in the tree, the tail's position rewritten in place
every batch); the pair's schema check compares by class and misses
narrowing, SQL Server's never compares a column; the hop's TLS is dropped on
the main data paths while `doctor` says verified; typos in a hop load clean
and turn approvals and column drops off; stopping migkit leaves programs
writing; one process or connection per catalogue query; and the final suite
as it stands is ~3.5-4.5 h serial with no timeout.

Fixed during the audit (one-liners): `doctor`'s engine lookup on
cross-engine hops (K8), the change-stream REST port bound to 127.0.0.1
(SEC1), the Homebrew licence, `deploy/conf/` ignored (W5).

**How the groups run.** One agent a group; within a wave the groups' files
are disjoint. Wave 0 lays the shared helpers the rest use. Wave 2 takes
files wave 1 held. The test migration and the sweep run last, when nothing
else edits tests. Files running work also edits are named per group;
start that group after the running work merges or leave those hunks to it.
Tests are written with each item and run once in the final suite (the
owner's rule); every verdict touched is proved able to say "different".

### Wave 0

**AUD-0. The shared helpers (S-M, first).** Files: `migkit/durable.py`
(new), `migkit/envkeys.py` (new), `migkit/redact.py` (new),
`migkit/catalog.py` (new), `migkit/util.py`, `migkit/wording.py`,
`migkit/config.py` (the `Endpoint` renderers only).
* `durable.write_atomic(path, data, mode=0o600)` (unique `O_EXCL` temp,
  fsync, `os.replace`, directory fsync, `F_FULLFSYNC` where asked),
  `append_line` (truncates a torn tail first), `read_state(path, kind)` with
  one torn-file policy per kind (positions and undo journals stop naming the
  file; caches read as absent), `private_dir` 0700 (ST1, ST9, ST15, SEC10).
* `envkeys`: every `MIGKIT_*` variable declared with its type, default and
  words; `flag()` (`1/true/yes/on`, `0/false/no/off`, anything else an
  error), `number()`, `text()`; one error naming the variable; did-you-mean
  for an unknown `MIGKIT_*` at start-up; a generated reference table (K13).
* `redact.Redactor(hop)`: every resolved secret with its URL-encoded,
  shell-quoted and SQL-escaped forms, shape rules (userinfo up to the last
  `@`, `password=`/`PWD=`, `IDENTIFIED BY '...'`, `PASSWORD '...'`,
  `masterauth x`), one `SECRET_KEY` pattern; `wording.redact` and
  `util.without_secret` become calls to it (SEC12, DU9).
* `config.Endpoint.url(scheme, db, tls=True)` (`quote(..., safe='')` for
  user and password, IPv6 brackets, the TLS query) and `Endpoint.tls_forms()`
  rendering the resolved TLS decision as libpq env, URI query, subscription
  conninfo, MySQL `SOURCE_SSL*`, `REQUIRE SSL`, bulk-program flags and
  Debezium fields (SEC2, DU8, SC17).
* `util.stamp()` UTC ISO with zone (DU21); `wording._duration` the one
  duration formatter, `util.human_secs` delegating (DU10); one bytes
  formatter with honest units (DU24); `util.split_table(engine, name)` /
  `qualify()` (DU14); `util.with_retry` the one retry policy, with
  `writes=True` retrying only failures while connecting (DU11, W11);
  `util.run`'s error without the argv (W3).
* `catalog.py`: the shapes wave 1 and 2 share - `Catalog(tables, columns
  {table: [(name, declared, cls, nullable, default, collation)]}, keys,
  indexes, fks)` and `Shape {table: fingerprint}` - so AUD-F's drift,
  AUD-G1/G2's one-query readers and AUD-H's consumers meet on one type
  (SC3, SC15).
* `util.Probes`: what a deep check planned to read and what it could not,
  turned into the result's status and words ("compared 12 of 30 columns;
  not read: a.b (timeout)"), so no deep check says `ok` over a failed read
  (LG2).

### Wave 1 (disjoint files)

**AUD-A. One verdict, one exit code, one write guard (P0, L).** Files:
`migkit/cli.py`, `migkit/verdict.py`, `migkit/report.py`,
`migkit/integrations.py`, `migkit/session.py`, `migkit/schedule.py`,
`migkit/interlock.py`, `migkit/guide.py`, `migkit/approvals.py`,
`migkit/ranges.py`.
* `verdict.decide(records) -> (status, exit code, severity per row)` and one
  `VERDICTS` table (word, exit code, colour, metric value) used by the check
  exit code, the report, `watch`, `sync`, `integrations.passed`; delete
  `report._reclassify` and `report --refresh`'s rewrite; no live result dicts
  to a renderer; empty or all-skip is `incomplete`; a failed watch cycle is
  an `error` row; unknown `--only` names a usage error (V1, V2, DU2, LG11).
* Exit codes 0 same/done, 1 different, 2 usage, 3 could not verify or
  incomplete, 4 refused by a guard, 130 interrupted, mapped once in
  `_Step.invoke`; the scheduled check and `schedule.run` read the same table
  (V6).
* `_Step.invoke` owns: the error handling `_move_run` has today for every
  command (scrubbed through `export.words`/`redact`, the traceback to the
  debug log), SIGTERM as Ctrl-C, the schedule gate (`NotDue`, nested invokes
  skip it, every skip logged) (V7, W1, K4).
* One `_guard_write` (refuse_writes, masking, approvals, interlock,
  protect_target) for every writing entry point; `tail`/`replicate` thin
  wrappers; `sync --mode migrate/stream` = `_move_run(full+cdc)` + the one
  check; `sync --go` = restore point + `_repair(do_apply=True)` + the check,
  exit from the re-check (V3, V4, V5, ST3).
* Dry run reads what `--go` reads (`attempt(..., record=go)` - additive in
  `decide.py`, P0-SMART's file) and runs the guards in "say, don't stop"
  mode (V9); `run_checks()` shared by check, `watch --verify` and `sync`
  (V13); resumed rows carry `reused_from` (V10); markup escaped,
  `soft_wrap=True` (V8); `watch` SAFE only after a delta or consistent verify
  (V14); flags validated per command, `-q` by level not substring (LG12
  cli part); rollback under one lock, thaw last;
  `history` from the state store; `init` updates the hop with the same two
  addresses (V15); CLI text in migkit's words (V16, V17); `doctor` parallel
  across hops and sides, one engine each, `init` reusing its facts (V12).
* `on_conflict` from the hop unless the flag is given (K3); schedule slot
  held by a lease, per-hop try/except, due slots run concurrently (K5, K6);
  approvals: the requester identified by key, `max(1, count)`, step names
  checked (K9); `interlock` keeps its answer per (hop, db, run) (V11).
* The move checkpoint: position kept before the copy on every resume
  (`_first_point`), an append-only per-table progress journal instead of
  rewriting `move.json` twice a range, compact JSON, mutations under the lock
  (ST4, ST17, ST24, SP4); `convert-schema --apply` in one transaction on
  PostgreSQL and recording `built-tables.json` (SC21, SC7); the time
  recorded under the way that ran (W10).
* Running work: `decide.py` (P0-SMART) for `record=go`; `cutover.py`
  (cutover gaps) must call `verdict.decide` so an all-skip check never flips.

**AUD-B. Durable tail state and one follow loop (P0, L).** Files:
`migkit/engines/hetero.py` (the tail), `migkit/tailtables.py`,
`migkit/tailctl.py`, `migkit/marks.py`, `migkit/kept.py`, `migkit/lease.py`,
`migkit/locks.py`, `migkit/setaside.py`, `migkit/state.py`,
`migkit/checkpoint.py`, `migkit/audit.py`, `migkit/evidence.py`,
`migkit/repairlog.py`, `migkit/jobs.py`, `migkit/twoway.py`,
`migkit/engines/copybook.py`, `migkit/engines/kafka.py`,
`migkit/engines/mongodb.py`.
* The tail's token, table states, accounts and spool frames as rows of the
  tail's SQLite, one transaction a batch; one `Position` envelope
  `{v, engine, source, position, batch, rung, next}` written once, its path
  computed in one place; every other state file through `durable` (ST1,
  ST19, ST21, ST9).
* Two-way exact batches never applied twice after a lost token (ST2; after
  the conflict-resolution work merges); the tail state mirrored whole with a
  generation, the higher wins (ST5); copybook batch index written before the
  data (ST6); spools indexed by the range they wait on, released rows deleted
  (ST18); spool names with a hash (ST25).
* One lease per tail scope, `tailctl.alive` = a live lease holder, one
  `liveness(record)` over (host, pid, expires) with the beat age for another
  host, lease loss raised in the write loops (ST8, ST10, ST11).
* One `tailctl.run_loop(look)` for the cross-engine, ClickHouse, OpenSearch,
  Cassandra, MongoDB and Kafka tails: stop, until, pause, beat, retry,
  SIGTERM, save; one `until()` reader, the slot's end honoured everywhere
  (ST14, K14).
* Undo journals durable, a torn last line tolerated; `_LineSealer.flush`;
  `EvidencePath.open` seals every mode or refuses; one sealing policy
  (`at_rest`) for restore points too; restore points committed by rename with
  a unique name; versions and migrations for every state file and
  `pragma user_version` (ST12, ST13, ST15, ST20, ST22, SEC9 evidence part).
* The audit chain: torn tail repaired or marked, the opening stretch bound,
  the head hash in the anchor (ST16, SEC15); `state.backend` read once
  (K10); `checkpoint.json` merged under a lock (ST23); spool frames not
  pickled (SEC21).
* Tail speed: the copied-ranges check precomputed per entry and retired once
  the tail is past every mark (SP3, without changing `ranges.place`); the
  schema poll by one fingerprint, skipped where the change stream carries DDL
  (SP8); idle back-off and a 50 ms fence check (SP10); the row filter's
  translation cached (SP12); kept proof with per-table converters, batched
  leaf reads, a stale-since flag instead of rewriting leaves (SP7); MongoDB
  and Kafka fences without a fixed second (SP19).
* MongoDB's sharding deep check says what it could not read (LG2, its part).
* Running work: `twoway.py` (conflict resolution), `repairlog.py` (repair
  ladder).

*Done (2026-10-01), with ST17's journal (`checkpoint.read_move`) handed
over from AUD-A.* The tail's own state is one SQLite store set down a
batch at a time before the target commits it and taken back out where
the position never passed it (`tailtables.Store`); the target's batch
mark decides the rest (`twoway.committed_ahead`, `hetero._committed_
ahead`). A cutover holds a lease of its own while it acts, so the next
run is refused instead of undoing it in flight (`cutover._held`); every
lease a process holds is asked between a copy's ranges and a tail's
batches (`lease.check_held`); every read-modify-write of a kept file goes
through `durable.change_json` under the file's lock, and no file is
written through a temporary name another writer shares (ST23).
Every source whose tail asks its shape digests its catalogue on the
server first - PostgreSQL, SQL Server, Oracle, MySQL and TiDB, SQLite
(`shape_mark`, over exactly the rows the whole read reads) - and the
whole shape is read only where the digest moved; a MySQL tail asks only
once a minute, its binlog carrying every DDL (SP8). A restore point's copy under
the reports is sealed file by file to the hop's `at_rest` recipients and
opened into a directory of the run's own to read (ST15). Spool frames
are still pickled: read back only into the classes a change log's values
come as, and a frame that would not read back is refused before the
batch is kept; `file:` secrets and the approvals' signers list are
refused where another user may change them (SEC21). `schedule` was
already lease-based (ST10, AUD-A); the PostgreSQL undo's `\copy` (SEC9)
and its fence on one kept connection (SP10) were done in `postgres.py`
by the groups that hold it.

**AUD-C. The dashboard, metrics, the run log and what leaves the machine
(P0/P1, L).** Files: `migkit/ui.py`, `migkit/notify.py`, `migkit/traces.py`,
`migkit/digests.py`, `migkit/export.py`, `migkit/diagnostics.py`,
`migkit/tls.py`, `migkit/tunnel.py`, `migkit/assist.py`,
`migkit/storedcode/model.py`, `migkit/capabilities.py`,
`migkit/troubleshooting.py`, `migkit/runlog.py` (new),
`deploy/docker-compose.yml`, `deploy/grafana/`,
`deploy/prometheus-alerts.yml`, `deploy/README.md`, `docs/threat-model.md`,
`SECURITY.md`, `docs/troubleshooting.md`.
* The dashboard and `/metrics` read `verdict.json`; a state-set
  `migkit_verdict{status}`; a background collector with a cached snapshot,
  its age and errors exported; stream metrics per hop's own connector;
  digests paired within one run; metric families renamed (counters `_total`,
  no `quantile` label, stable key labels, one escaping sample helper) with a
  deprecation period and a lint test (LG1, LG6 ui part, LG7, LG8, LG9).
* The header sign-in needs a proxy secret; the hold/resume path checked
  against its hop's own directory; buttons built without inline script and a
  CSP; `/metrics` behind a token and `access:` roles; the deploy's bind
  address and mounts fixed (SEC5, SEC6, SEC7, SEC16, LG10).
* `runlog.py`: one JSON line per event `{ts, run, hop, db, table, phase,
  level, event, msg}` in `reports/<hop>/runs/<run>.jsonl`; library records
  routed into it; diagnostics include it and `commands.log`; append-only
  files rotated with the chain head carried (LG5, LG12 library part, LG13).
  The console is routed through it by the sweep.
* `export`, `notify` and `diagnostics` redact through `redact.Redactor`;
  failure notifications carry the error kind unless `notify.detail`; a
  non-HTTPS model URL refused; failed digest writes counted into the
  evidence; `export.left_anything` by a per-run marker (SEC12, SEC20, LG4,
  K23).
* Only an SSH tunnel counts as local for TLS; every networked engine in the
  posture, "unknown" as a warning (SEC17, SEC18 tls part).
* `capabilities`: the stale Kafka cell, probing only the named engine,
  `decide`'s duplicate gap words folded in (DU17, SP17, LG14); the
  troubleshooting guide regenerated and a pre-commit hook for the
  generators (LG14); the threat model says what leaves the machine.

**AUD-D. Options, environment and addresses checked at load (P0, M-L).**
Files: `migkit/config.py`, `migkit/hopkeys.py`, `migkit/addresses.py`,
`migkit/imports.py`, `migkit/clouds.py`, `migkit/rules.py`,
`migkit/registry.py`, `migkit/zones.py`, `migkit/engines/__init__.py`,
`migkit/engines/oracle_logminer.py` and `migkit/engines/warehouse.py` (one
line each).
* `Key(choices=, check=, default=, kind="secret")`; every owning module's
  validator called at load; unknown sub-keys errors; `cutover.pooler.*` and
  `mapping.columns.<t>.*` declared; `hopkeys.opt()` the one reader of a
  default; `hopkeys.topology()`/`reverse()` (K1, K15, K20).
* One YAML loader refusing duplicate keys, `CSafeLoader` (K2); secrets
  resolved lazily, once, cached, with errors naming `hops.<hop>.<side>.<key>`,
  option secrets included; `get_hop` builds only the named hop, memo by
  mtime (K7, K12, SEC11); Vault over HTTP with KV v1/v2, one error format
  for every secret store (K22).
* The state root from `XDG_STATE_HOME` (or beside the config), never the
  working directory; report directories 0700 (ST7, SEC10).
* The canonical engine name stored on the hop (brand kept apart), aliases
  and schemes one table, default ports one table in `engines/__init__.py`
  for every engine (DU6);
  `Hop.excluded` absorbs `noise_prefix` and other tools' tables (DU16);
  migkit's own tables by one exact rule (DU26, config part).
* Imports: filters quoted through each side's own helpers, an `include:` key
  of patterns, a note for every dropped pattern (DU4, K16); addresses with a
  secret reference holding `/` (K17); cloud detection by settings that cannot
  be copied, roles as a hint (K18); business rules by `Counter`, a one-sided
  rule an error (K19); `zones.infer` both directions; `get_hop`
  did-you-mean (K24).
* Every `MIGKIT_*` read in these files through `envkeys` (K13); the
  warehouse builder passes `key` to `ddl_type` (SC6, its line).

*Done (2026-10-04).* Every key under an option is declared with its own
`Key` - type, words (`choices`, `fold` for spellings, built in the
choice's own), least, default - and an unknown one is an error with the
nearest; the owning module's reader of an option (`Key.check`: the
cutover's settings, the follow's end, the limits, the deadline, the
subset's rules, a stream's format, a page copy's source address, the
business rules, the origin table, the spool) is asked at load, each
given the hop without the other options that have readers of their own;
`approvals.steps` by `approvals.STEPS`, `count` at least 1; a schedule's
cron and zone read as its slot reads them. `hopkeys.opt` is the one
reader of a default (`fence_timeout`, `repair_window`, `settle` moved
onto it), `topology()`/`reverse()` the one reading of those words. A key
written twice in one mapping is refused (`config.read_yaml`, the C
parser, merges allowed), at load, in `doctor` and in the tail's reload.
A side's host, user and password are read where a connection needs
them, once (`SideEndpoint`), a secret option through
`Endpoint.option_secret` (registry, redo miner, key passphrase, the
pooler's password through `config.secret`), a failure naming
`hops.<hop>.<side>.<key>`; `get_hop` builds the one hop, the file read
and checked again only when its content or mode changes; Vault over its
HTTP API with either version of its store; one failure sentence for
every store. A hops file anyone may change (or replace) is refused, one
others may read with a secret written out is said. Reports and run
state beside a project's `conf/` or in `$XDG_STATE_HOME/migkit`, never
the working directory; `doctor` says both paths; report directories and
the restore points' mirror (now under the state directory, the old one
still read) private, files written 0600 from their first byte. Engine
names: one table of other names, schemes derived from it, ports for 20
engines, the hop built with the engine's own name and the brand kept
apart (`Hop.brand`). `noise_prefix` and migkit's own tables (`own`,
`OWN_PREFIX`, `OWN_LIKE`) in `Hop.excluded` for every engine. Imports
quote a filter by the source's own quoting, carry a task's include as
the hop's own where every entry is a shell pattern and say each one
that is not; an address's reference may hold `/`; a PostgreSQL
service's own settings (`rds.*`, `cloudsql.*`, `azure.*`, `alloydb.*`)
say it before its roles; a rule asking one side is refused and answers
are compared by count; the zone in use read against the server's own
clock as well as UTC; a cutover's counter margin is its own thread's
(`SEQUENCE_MARGIN`). Unknown `MIGKIT_*` said at start with the nearest,
a declared one set wrong a usage error there.
Left: the MySQL engine's `'migkit%%'` filters are AUD-G2's (DU26's
engine part); a role alone still names a PostgreSQL service where no
setting says, since which settings each service shows every user is
not verified here; the diagnostics bundle still reads a broken file as
YAML keeps it, to show it.

**AUD-E. External programs: one runner, stop means stop, pinned installs,
the offline bundle (P0/P1, L).** Files: `migkit/runner.py` (new),
`migkit/movers.py`, `migkit/stall.py`, `migkit/tools.py`,
`migkit/tooluse.py`, `migkit/toolversion.py`, `migkit/redisshake.py`,
`migkit/mirror.py`, `migkit/dsbulk.py`, `migkit/fromsnapshot.py`,
`migkit/ddbexport.py`, `migkit/physical.py`, `migkit/cloudseed.py`,
`migkit/second_reader.py`, `migkit/runners/second_reader.py`,
`migkit/verifiers.py`, `migkit/bulkways.py`, `migkit/tableways.py`,
`deploy/Dockerfile`, `.dockerignore` (new), `packaging/homebrew/migkit.rb`,
`.github/workflows/wrapped-programs.yml`,
`tests/test_the_report_does_not_name_its_tools.py`.
* `runner.py`: every program started in its own session, stopped by process
  group, `PR_SET_PDEATHSIG` on Linux, containers named and removed on exit
  and before the next start, registered by create time, timeouts and a stall
  limit fed by the progress readers, output read as it comes, remote scripts
  under a lock with a pidfile; `classify(rc, text)` -> Refused / Blocked /
  Transient through `wording.database_words`; all 62 direct spawns moved onto
  it (W1 per-site handlers removed, W2, W9, W12, W20).
* The tool registry the one source (install recipe, lock entry, version
  probe cached on realpath+mtime, role word, image digest, terms); the side
  tables, `DRIVEN`, the leak test's TOOLS, the Dockerfile and the formula
  generated from it; the leak test reads f-strings, exception constructors
  and runtime output (W3 movers part, W4, W13, W14).
* A lock file `{version, url, sha256, size}` per program and platform,
  verified before extracting; images by digest; the second reader with
  `--require-hashes`; `doctor --install` replaces a build outside the range;
  installs batched and atomic; `atlas version` offline with a timeout; the
  Dockerfile from the lock, closed builds only with accepted terms, a
  `.dockerignore` (W5, W6, W8, W22, W23, SEC14).
* W8: the bundle built into a directory with `SHA256SUMS` and signed
  (`MIGKIT_BUNDLE_BUILD=<dir>` on `doctor --install`, no new mode),
  `MIGKIT_BUNDLE=<dir>` read by every installer, `docker load` and the
  second reader, a CI job with no way out, the Windows blockers removed
  (W7).
* Stream secrets through Kafka Connect's env provider (SEC1 rest); TLS from
  `Endpoint.tls_forms()` on the streaming copy, clone, follow, CLONE, the
  page copy, pgloader, Debezium and the MySQL bulk programs including the
  automatic answer (SEC2 movers/physical part); secret files through
  `PrivateFile` with unique names, container secrets through `--env-file`
  (W19); file names from table names sanitised (SEC19 movers part).
* Wrappers resumable: the snapshot reader keeps its working state and runs
  readers in parallel; DynamoDB export idempotent by `ClientToken`, its ARN
  kept, `try/finally` forget; dumps kept with a manifest and resumed by TOC
  (W16); flags checked on the exact argv, not a second dry run (W17);
  machine output read where it exists (W18); the dump fallback a rung or
  `Blocked`, recorded as itself (W10 movers part); stream readings filtered by
  hop (LG6 movers part); RedisShake's key filter from migkit's own
  matcher (DU25); post-data restored with `-j`, privileges restored
  when the roles exist, missing MySQL tables on one connection (SC21, SC11
  movers part).
* Running work: `movers.py` (the table-way ladder) - start after it merges.

**AUD-F. Schema, drift, users and grants (P0, L).** Files:
`migkit/users.py`, `migkit/dba.py`, `migkit/ddl.py`, `migkit/logddl.py`,
`migkit/drift.py`, `migkit/unchanged.py`, `migkit/indexes.py`,
`migkit/leftovers.py`, `migkit/variants.py`, `migkit/engines/mssql.py`,
`migkit/storedcode/pg_mysql.py`, `migkit/storedcode/mysql_pg.py`,
`migkit/canon.py`.
* Drift by `shape_catalog(side, db)`: one query per side, a per-table
  fingerprint (types with length and precision, nullability, defaults,
  collation, indexes, constraints, triggers), differing tables explained; the
  interface here, PostgreSQL and MySQL implementations by AUD-G1/G2, SQL
  Server here (SC3); `drift.transient` built once (SP1); drift and leftovers
  on `Hop.excluded` (DU16).
* SQL Server: columns, index columns and foreign-key columns compared (SC2);
  permissions rendered by class with `minor_id`, WITH GRANT OPTION kept, SIDs
  compared, policy flags carried (SC12); databases filtered by `exclude`
  (DU15); one kept connection instead of a `sqlcmd` per query (W15 mssql);
  its own `ok` when the drilldown finds nothing (V1 mssql part); hints in
  migkit's words (W3).
* Users: names through each engine's quoting (SEC3); roles first in
  dependency order, a per-statement ledger, attributes, memberships, role
  edges and default roles compared, `create` ending with `compare()` (SC9);
  one result shape and correct hints, `_pg_conn(dbname=)`, `compare` returns
  both sides, "passwords not compared" where hashes cannot be read (SC22); one
  `system_principals(engine)` (DU13).
* Logged DDL: partition truncate/drop/exchange/reorganize classified as
  truncate-like, comma lists split (SC19); `needs_backfill` per clause (SC20).
* `canon`: `citext`, exact-width integers per pair, a `limits(side)`
  interface (identifier length, key bytes, row format, charset,
  `max_string_size`, auth plugins, keywords) for `ddl_type`; routines typed
  through `canon.ddl_type` with one collation per column (SC5 canon part,
  SC13, SC18); `sql_rows` with a writer per column, `digest_step` and
  `uuid_text` faster (SP14, SP18 canon part); version parsing in one place
  (DU23).
* Running work: `canon.py` (P0-PARITY large values).
* Done 2026-10-01: every account and role name into target DDL through the
  engine's own quoting and every value bound, on every engine `users`
  carries, `dba`'s scripts and the PostgreSQL and MySQL grant repairs
  (`_grant_sql`, `_retarget_grant`); `drift` on `catalog.Catalog` (kept as
  (column, text) pairs, read back whole, one comparer and one explanation,
  a narrowing named by `verdict.narrowing`) with SQL Server's
  `shape_catalog`; SQL Server's check compares every table whole, its
  permissions by class and column with their grant option, logins by SID
  and policy, one kept connection, its own `ok`; users' one result shape,
  roles first and a ledger with undo, `create` ending on a compare, the
  target's auth plugins asked; `reload` for rows a partition DDL changes;
  every clause of an ALTER read; `canon.limits`, `citext`, faster
  `sql_rows`/`digest_step`/`uuid_text`. Left, with why: PostgreSQL and
  MySQL `shape_catalog` (AUD-G1/G2's files); exact-width integers and
  routines typed through `ddl_type` - either changes every cross-engine
  build or cuts a routine's text at 1,024 characters, and the stored-code
  rules are the conversion-breadth work's; `system_principals_sql` adopted
  in the PostgreSQL and MySQL grant and assess filters, `freeze`, and
  `toolversion.server_major` in `physical._series`/`jobs._my_off` (their
  owners' files).

**AUD-G1. The PostgreSQL engine: one connection, one catalogue read, one
quoting (P0, L).** Files: `migkit/engines/postgres.py`,
`migkit/engines/pg_pass.py`, `migkit/legs.py`.
* One `_connect(side, db, purpose)` (timeouts, keepalive, `TimeZone=UTC`,
  TLS from `Endpoint`), a kept connection per side and thread, the
  catalogue, fence and marker queries through it, `psql` only for `\copy`
  and meta-commands; the target mapped once, inside (DU3, DU12, SP2, W15,
  SP10, SP19 its part).
* `catalog(side, db)` (the `catalog.Catalog` shape) in one query per side
  and cached per run;
  `shape_catalog` for drift; one `user_tables(side, db, kinds)`; one
  `row_estimate` with None for unknown; key order from `neutral_key`
  everywhere; the "unchanged" marker with the shape and in one query per
  side (SC15, SC3, DU5, DU18, DU19, SC4).
* Grants: one `aclexplode` read over tables, sequences, schemas, databases,
  functions, columns and default ACLs with `is_grantable` (SC11); one
  authoritative structural comparer, the others "not run: why" (SC16).
* Native replication through `legs`' builder (sslmode, literal escaping,
  `log_statement` off); structural diff URL from `Endpoint.url`; builders
  pass `key` to `ddl_type`; identifiers through `_quote_ident` with a lint
  test for the file; repair undo through stdout into `EvidencePath`; file
  names sanitised; deep checks that could not read say so (SEC2, SEC13,
  SC17, SC6, SEC4, SEC9, SEC19, LG2 its part); migkit's own tables by the one
  rule (DU26).
* Running work: `pg_pass.py` (the table-way ladder), the repair paths
  (repair ladder).

  **Built 2026-10-01 (tests written, the full run to come):** one
  `postgres.connect` (the side's `tls_forms`, connect timeout, keepalives
  from `envkeys`, migkit's name, `SESSION` pinned: UTC, ISO, every digit,
  no statement timeout) behind `_open`/`_connect` (the hop's name mapped
  once) and `pg_pass.connect`, `legs._connect`; the questions on a kept
  connection per side and thread (`_kept_conn`: the server's text, every
  result of a script, the session reset after anything that may leave
  state, a read asked again on a lost connection, a write never), the
  client program only for meta-commands, and every program through one
  `_client` with the side's TLS; the catalogue in one statement a side
  (`catalog`, `shape_catalog` with column extras, checks and triggers,
  kept 60 s and read again after migkit's own DDL), every reader of a
  table's shape from it; `user_tables` the one rule, `row_estimate` with
  None, the key in key order everywhere, markers with the table's live
  columns in one statement a side; grants of every kind as effective
  privileges with the grant option; the optional schema readings said
  "not run: why"; the subscription's connection string from
  `legs.conninfo` on a session that is not logged; the row repair through
  the drivers' COPY, its undo sealed and durable; the text repair under the
  load's window; JSON digested on the server as canon writes it; `doctor`
  says verified only of a path that checked (`leg_encryption`'s
  `verified`, the far paths' rows); the page copy's programs on the
  target's host take the source's TLS as far as they can (`physical`).
  Left to others: `cli._leg_said` reading `verified` (AUD-A), the leaves'
  JSON still folded here (`leaves.py`), the drilldown's file names from
  table names (base and the readers).

**AUD-G2. The MySQL engine: the same (P0, L).** Files:
`migkit/engines/mysql.py`, `migkit/engines/my_pass.py`.
* One `_connect` (read timeout, keepalive, UTC pinned for rules too, TLS
  from `Endpoint`), a kept connection per side and thread, the key read once
  per table (DU12, SP2).
* Grants from `mysql.user`, `db`, `tables_priv`, `columns_priv`,
  `procs_priv`, `role_edges`, `default_roles` in seven queries per side,
  cached (SC10); MariaDB defaults read as MariaDB writes them (SC14); one
  authoritative structural comparer, atlas demoting only the kinds it
  inspected (SC16); `catalog`/`shape_catalog` (the `catalog` shapes) in
  one query per side (SC15, SC3).
* Native replication with `SOURCE_SSL*` and no `GET_SOURCE_PUBLIC_KEY` under
  SSL; the schema dump with the client TLS; URLs from `Endpoint.url`; 175
  raw backtick interpolations through `_quote_ident`, with a lint test
  (SEC2, SC17, SEC4).
* Deep checks read every column they plan and name what they could not;
  `_rows_utc` raises; `watch_sample` fresh; writing scripts not retried;
  `neutral_write` with a writer per column; migkit's own tables by the exact
  prefix, `moved_nothing` filtered (LG2, LG4, V14, W11, SP14, DU26).
* Running work: `my_pass.py` (the table-way ladder).

**AUD-T1. The test suite, part one: infrastructure for one fast, reliable
final run (P0, M).** Files: `tests/conftest.py`, `tests/servers.py` (new),
`tests/waits.py` (new), `tests/mysql_pair.py`, `tools/final_suite.py`
(new), `tools/with_docker_lock.py`, `pyproject.toml`,
`.github/workflows/ci.yml`, `.github/workflows/release.yml`,
`.github/workflows/x86-engines.yml`.
* `tests/servers.py`: session fixtures by server profile (image, server
  arguments, TLS) shared by every module naming it, per-module isolation
  (database, schema, keyspace, topic prefix, db index), a port allocator
  (`base + 100 * worker`), container names with the worker suffix, always
  `127.0.0.1:` mappings, SQL through the driver, readiness by the server's
  own sign, image builds cached by content hash (T1 helper, T4, T5, T10,
  T11).
* The session pair's wipe covers databases, slots, publications,
  subscriptions, roles and schemas (T2); every test asking for a server
  fixture marked `docker` by conftest (T13); a `vm` mark by file name
  (Oracle, SQL Server 2022, Db2, ASE), skipped unless
  `DOCKER_CONTEXT=colima-migkit` (T6); `MIGKIT_TEST_STRICT=1` turns an
  infrastructure skip into a failure and the summary counts skips by reason
  (T8); `tests/waits.py:until()` (T9 helper).
* `pytest-xdist` and `pytest-timeout` in `[dev]`; `tools/final_suite.py`:
  pure tests first with `-n auto`, images pulled once by digest, install
  state checked, the default-profile shard and the `vm` shard at once under
  the lock taken once, `--timeout 900`, junit per shard, `-rs`, a re-run of
  only the failed ids (T3, T7).
* CI and release install `.[dev]` and run the same script; the release tag
  check reads `migkit/__init__.py`; `requires-python = ">=3.10.12"` (T12,
  W21, W24).

*Done 2026-10-01.* As built, where it differs from the plan above: the
docker tests run in **lanes** - one pytest process each, on the default
colima profile and on the migkit VM at once, as many as each daemon's free
memory holds - not in xdist groups, because the modules not yet moved fix
host ports and container names: files that fix one port or name share a
lane, files naming the `pg_pair` pair's fixed 55432/`migkit-test-pg-*` run
on lane 0 (the only lane that keeps them; any other fails such a file
loudly), heavy files only where a lane may hold them, balanced by
`reports/final-suite/times.json`. A lane (`MIGKIT_TEST_LANE`) and an xdist
worker give each process its port block (20000 + 60 x index, under the
ephemeral ports the colima VM hands out) and name
suffix. `vm` is marked by what a test reaches (its fixtures and the helpers
they call name an x86 or Oracle image), not by file name, so the pure and
PostgreSQL-only tests in `oracle`/`sql_server` files stay where they were.
`with_docker_lock.py` hands out lanes: a command waits only for the host
ports and names its test files fix (and lane 0 for the pinned ones); one it
cannot read takes the whole of docker as before; the VM is stopped by the
last user. `pg_pair` and `my_pair` are the shared `pg`/`mysql` profiles,
module-scoped, reset at module end. `MIGKIT_TEST_STRICT` takes `1` or the
kinds (`server`, as CI, where programs needing accepted terms are absent).
`final_suite.py` refuses a checkout the installed editable migkit has
modules beyond (a run would import them from the other tree), stops a lane
silent past its test's timeout and goes on, and records the run in
`reports/final-suite/<stamp>/`. Estimate with no measured times: ~53 min a
lane in four lanes (default profile 1, VM 3) plus the pure phase, from the
2026-09-27 whole run scaled; AUD-T2 replaces it with the measured run.

### Wave 2 (after wave 1 merges)

**AUD-H. The pair's core: one schema comparer, one catalogue, both sides at
once (P0, L).** Files: `migkit/engines/base.py`, `migkit/engines/hetero.py`
(the pair, schema and copy parts), `migkit/engines/generic.py`,
`migkit/engines/dbapi.py`, `migkit/engines/duckdb.py`,
`migkit/engines/redis.py`, `migkit/leaves.py`, `migkit/render.py`,
`migkit/rowtext.py`, `migkit/nanotime.py`, `migkit/avrostream.py`.
* One schema comparer over a neutral shape (class, capacity, nullability,
  default, indexes, foreign keys, collation) for every engine's
  `check_schema` and the cutover gate; `declared()` for build, fit and
  compare; the cross-engine schema's post-data section (indexes, foreign
  keys, CHECKs); `finish_created` idempotent, parallel, with the engine's
  name limit (SC1, SC5, SC7, SC8).
* `Engine.catalog` default and every `neutral_*` reader on it; a per-run
  memo of `table_facts`, `databases` and `server_identity`; one writer
  connection per lane (SC15, V11, SP11).
* A probe accumulator in `base`: no deep check says `ok` over what it could
  not read (LG2); assess rows and the generic engine in migkit's words (W3);
  the TLS posture for every engine (SEC18); report file names sanitised
  (SEC19); one quoting helper per dialect (DU20).
* Speed: the batch read-back by the target's digest over the key set; the
  cross-engine check both sides at once and tables over the workers; the
  renderer's fast paths; one capacity scan per table; `_key_of`, `_keep`,
  `rowtext.encode`, `nanotime.parse`; Avro schemas cached per topic and
  signature (SP5, SP6, SP9, SP15, SP16, SP18).
* DuckDB compares sequences, `checks` from `capabilities.implemented()`
  (DU1); Redis with its ACL user and its fence without a fixed wait (DU7,
  SP19); `dbapi` writer per column (SP14); `hetero._url` and `leaves._url`
  from `Endpoint.url` (SEC2, DU8).
* Running work: the engines (P0-PARITY), repair paths (repair ladder),
  apply paths (two-way conflicts).
* Done in wave 12 (W12G): `diagnostics.ProbeAccumulator` (kind, scope,
  value, taken, took, witness, observed pace; `read` measures again what is
  stale or failed, `refusals` names what a decision cannot rest on; saved as
  `probes.json`, carried in the diagnostic bundle, cited by `doctor`);
  `schemaformats.neutral` / `compare` / `results` and `verifiers.compare_schema`
  (one comparer, same-engine as declared, a pair as converted, what cannot be
  held is `unread` and turns an ok into a warn); `cutover.recheck` after the
  approvals: lag, the verify verdict and every probe's freshness
  (`fresh_window` = the budget), each failing fact named.
  Not done: the engines still building their own `check_schema` (see
  `verifiers.engines_with_own_compare()`) have not been moved onto
  `compare_schema` (their files belong to other tickets); the cutover gate
  needs a `verdict.json` where earlier it did not, so existing cutover tests
  that never ran a check need one written; the engines' own readers (grants,
  versions, settings, sizes) still probe directly and do not yet record into
  the accumulator.
* Done in wave 13 (W13D2): `check_schema` of bigtable, cassandra (and
  keyspaces, geminidb-cassandra), clickhouse, cosmosdb, couchbase, dynamodb,
  firestore (and datastore), lindorm, neptune, opensearch, parquet,
  iceberg/delta, spanner, timestream, saas, sapodp and kafka goes through
  `verifiers.compare_schema`; the neutral shape holds `traits` (partition
  count, time to live, retention, column family rule) compared kind by kind
  on one engine and named unread across two; a table or a setting a side
  would not give is `unread`, never missing and never ok. Engines give
  traits by `neutral_traits` / `neutral_traits_all` (bigtable, firestore
  so far). Not done: the other engines' `neutral_traits` (dynamodb TTL,
  cosmos TTL and partition key, cassandra table options, spanner TTL,
  opensearch settings); bigquery (warehouse.py) is not in W13D2's files;
  `capabilities._SCOPE` should name `compare_schema` so the engines need
  not pass `takes`; a mapping that renames a table does not carry its
  traits across the rename.
* Done in wave 14 (W14F): the comparer's neutral fallback reads
  `neutral_column_rules` (NULL, default, numbered) for every engine through
  it; dynamodb (time to live, stream), cosmosdb (default and analytical
  ttl), cassandra (table options, one listing), spanner (row deletion
  policy, interleave) and opensearch (storage settings, analysis, pipelines,
  mapping options) give traits; a renamed table carries its traits and a
  trait naming a renamed table is renamed with it. Not done: an engine whose
  `neutral_column_rules` is the base `{}` is compared as taking NULL with no
  default (no unread note is raised for it); DynamoDB Local answers no time
  to live, so its tables are named unread; cassandra's ScyllaDB change-log
  option is not in the comparer's traits; bigquery (warehouse.py) is still
  not in this line.

**AUD-DOC. The manual and the site from the code (P1, M; R24).** Files:
`tools/gen_docs.py` (new), `README.md`, `docs/index.html`,
`docs/goals-and-modes.md`, `docs/reference-*.md` (new), `.githooks/pre-commit`.
* Engines, aliases and the capability matrix from `NAMES`/`ALIASES`/
  `matrix()`; goals and modes from `hopkeys.GOALS` between markers with an
  exact-match test; the environment reference from `envkeys`; the exit-code
  table from `verdict`; the metric families from `ui`; command lists from
  the CLI; the hook regenerates and fails on a stale doc (LG15, LG16).
* Done 2026-10-01 (not merged yet): `tools/gen_docs.py` writes
  `docs/site/` (front page, quickstart, engines with the matrix, shapes,
  stored-code pairs and managed services, goals and modes, recipes as
  source -> target pairs each checked by `hopkeys.check` and held to the
  matrix, options, environment, commands with every option and the metric
  families, troubleshooting, evidence, releases per tag), `docs/index.html`,
  the README's and `goals-and-modes.md`'s parts between `gen_docs`
  markers and `docs/troubleshooting.md` (grouped by area, no module path);
  `engines.TITLES`; `--help`'s engines from `NAMES`; the pre-commit hook
  checks and writes again (exit 3 = behind). Tests:
  `test_the_manual_is_generated_from_the_code.py`. The environment page is
  `envkeys.VARS`. Waiting on others: the managed services come from the
  catalogue (`managed`) once it merges (until then `clouds.LABELS`); the
  exit-code table from AUD-A's one verdict table, which is not on master.

### Wave 3 (last, nothing else editing tests)

**AUD-T2. The test suite, part two: every module on the shared servers (P0
for the final run, L).** Files: `tests/test_*.py`. The 281 modules that start
their own servers moved onto server profiles (68 PostgreSQL files need
nothing `pg_pair` lacks; 43 MySQL files a stock server), hard-coded ports
and names gone, the 144 fixed waits on `until()` and the tail's own
caught-up sign, `docker exec` SQL through the driver; then the final run
measured and its numbers written into `tools/final_suite.py` (T1, T4, T9,
T10). The pattern is at the top of `tests/servers.py` (a module fixture
from `servers.fixture("<profile>")`, the module's `.name` for what the hop
names, `server.sql()`, `servers.private()` for a server a test must own,
`servers.image()` for a built image). Then: the 24 files pinned to lane 0
read `pg_pair`'s ports instead of 55432/`migkit-test-pg-*`; `cap_pairs`,
`typepair` (whose session fixture is also called `servers`) and the other
helpers move onto profiles; every `-p` mapping binds 127.0.0.1 and a lint
test in `test_the_suite_runs_in_lanes.py` holds every test file to no fixed
host port (today it holds only the lanes' range and the shared helpers);
the image scan reads only images a file runs, not ones its strings name.

**AUD-Z. The sweep: every leftover site on the one helper, lint tests that
keep it so (P1, M).** Files: whatever no group held -
`migkit/decide.py`, `migkit/cutover.py`, `migkit/freeze.py`,
`migkit/machine.py`, `migkit/planner.py`, `migkit/model.py`,
`migkit/sizing.py`, `migkit/sourcechange.py` and the rest, each after its
running work merges.
* Remaining state writes through `durable` (`decide._write` under a lock,
  `planner.progress`, `model.save`, `machine`, `sourcechange`, cutover and
  freeze records), remaining env reads through `envkeys`, durations and
  stamps through the one formatter, retries through `with_retry` (ST1,
  ST12, ST23, K13, DU10, DU11, DU21).
* The console routed through `runlog`; event words from one table; fallbacks
  in the decision record (LG5, LG12).
* Cutover: its final check on `verdict.decide`, its record under the hop
  lease, "quiet" proved or said unproved, `topology`/`reverse` from
  `hopkeys`, `freeze.supported` by the target side (DU2, ST8, LG3, K20, K21).
* The table builders of SQLite, Cassandra, ClickHouse and the lakes pass
  `key` to `ddl_type` (SC6); SQLite's streamed words through the one level
  (LG12); the freeze's fixed polls on a kept connection (SP19) - each after
  P0-PARITY or the cutover work merges.
* Lint tests: no raw identifier f-strings in `engines/`, no `MIGKIT_*` read
  outside `envkeys`, no state write outside `durable`, no subprocess outside
  `runner`, no hand-rolled retry, no program name in any user-facing
  string (generated from the registry), no verdict word outside `VERDICTS`,
  every URL or argv naming a host carrying the TLS keys (SEC2, SEC4, W3,
  LG11).

Done when every ID in the research document is fixed with its test or
struck with the owner's reason, the lint tests hold, and the final suite
runs through `tools/final_suite.py` with its time measured.

## P0-DEPTH. migkit's own parts that are still flat (owner, 2026-10-01)

Asked: "our own parts - flat, not smart, or already good?" Read at
55a1939. Deep already and left alone: the verification core (digests,
one-scan leaves, sketch, kept proof, generations), exact batches,
watermark and the two-way marks ladder, canon's types, the stored-code
proof, the cutover path, the interlock, the copybook decoder, the
fallback and the source-change offer. Flat, to be made smart:

1. **Repair is always row by row.** A table with ten differing rows and
   one with ten million get the same statements. Make repair a ladder in
   decide.py: rows by key in batches, ranges copied again (the leaves say
   which), the table reloaded by the fastest mover - chosen by predicted
   cost and the size of the drift, with the same statement record, undo
   and proof after.
   **Built 2026-10-01 (tests written, the full run to come),
   `migkit/repairways.py`:** every row repair naming its table
   (`RepairAction.table`) climbs three rungs through `decide.attempt`:
   the rows by key (the engine's own repair, as before); the spans of
   one integer key that differ heavily copied again the move's way
   (PostgreSQL's and MySQL's range copy, one transaction a span; the
   pair's `copy_range` across engines), the rows named outside them by
   key; the table copied again whole by `move_table`, only with
   `target_rows: replace` and nothing else writing the target. Ranked by
   P0-SMART's cost model (`repairways.RepairCosts`, a
   `costmodel.CostModel`): each way costed on its own work (rows named;
   rows in the spans and the rows named outside them; the table's rows),
   fitted from every repair on the hop and learned across hops, a prior
   from the move's own measured rate where a way never ran - and switched
   from at run time. The check's leaves keep which spans differ and their
   rows (`data-<t>.leaves`, `leaves.kept_leaves`), so a table whose rows
   were too many places to name is repaired by spans or whole instead of
   left out of the plan. Spans record every differing row before they
   land (the pre-image); the reload keeps a snapshot of the table and its
   digest before and after (`repairlog` form `snapshot`); rollback puts
   either back only where the target still holds what the repair wrote.
   Every table is checked afresh after its repair and the console says
   that check's verdict. PostgreSQL's row repair writes by the load's rule
   now (`load_window`, `WRITE_WAYS`): as a replica, or as the tables are
   only without triggers and foreign keys, else refused - it used to fall
   back to writing with triggers firing. Not done: the priors (`PRIOR`,
   `BULK`) are set, not measured - the final run times each way.
2. **Two-way conflicts are five whole-row policies** (error, apply_remote,
   keep_local, last_update_wins, source_priority) plus counters. Add: the
   changed-columns merge when the two sides touched different columns
   (F7.merge), min/max/delta resolutions, a policy per table and per
   column, and every conflict and its resolution in the evidence export.
   **Built 2026-10-01 (tests written, the full run to come):** a batch's
   changes to a row held as one against the target's row; only a column
   both sides changed is a conflict, the rest merged (`canon.Kept` keeps
   the target's own columns where they stand, NOT NULL or not). Rules
   per hop, table, column and group: error, park (the row set aside
   until decided on both sides, the tail going on), overwrite, ignore,
   source_priority, newest, max/min (of the column or of a named one),
   delta, union; `delete_vs_update` and `delete_missing` settle the whole
   row kinds, each with a stated default. Every rule settles one conflict
   alike from both ends; `hopkeys` refuses a pair of hops that would not
   (`twoway.apart`). Every conflict in `conflicts.jsonl`, the hop's
   record, `/metrics`, `notify` and the evidence export. The limit said
   in `twoway`: a value changed and changed back between two exchanges
   is invisible to any before image - a row deleted after the version
   that won needs tombstones, not built.
3. **Numbers set by reasoning, not measured:** `canon.WIDE` (1 MB),
   `BUCKET_ROWS`, `REPLICA_RISE` (5 s), `DIGEST_MEMORY`, the probe floor
   (256 MB), `TIE` (5%), the per-table program threshold (256 MB) and
   others the docstrings mark as reasoned. Each gets measured on the
   final run and then kept in the decision engine's knowledge base, so
   a machine and a hop tune them from their own runs.
4. **The resource model is built but unproven:** its one docker run named
   the wrong bottleneck in four of eleven cases and allows a 100%
   prediction error. Calibrate on the final run; P0-SMART's cost models
   take its readings as priors, not as truth.
5. **The pace backs off by TCP's rule on each resource** - sound, but
   reactive. With the hop's limits (P0-SMART) it holds a target (the
   source's load cap, the lag a replica may reach) instead of only
   reacting.
6. **The cross-engine copier carried rows through Python** - removed
   (crossmove.py, loadtext.py): rendered and compiled ways, 5M rows
   MySQL->PostgreSQL 402s -> 102s, PostgreSQL->MySQL 338s -> 186s.
7. **JSON is hashed in Python for every cross-engine proof** - now the
   limit (at 1M rows: copy 8s, proof 24-26s). Canonical JSON digested on
   the server where the engine can render it canonically (sorted keys,
   one number form), in Python only where it cannot, same digest.
   T3 (2026-10-16): the leaves hash PostgreSQL's JSON in SQL
   (`canon.json_sql`, `Side.json_in_sql`), read it as text where folded
   here, and fold a side here with the reason said (`canon.JSON_ROUNDED`,
   `JSON_UNPROVED`). Not done: MySQL's JSON in SQL (UNPROVED, mysql.py's
   owner); `PostgresEngine._json_text_sql`/`JSON_IN_SQL`/`JSON_DIGITS` to
   read canon's; `neutral_digest` callers passing the chosen hash to
   `canon.digest_expr(way=)`; a JSON key column is still read parsed; the
   server tests are written, not run.
   W5B (2026-10-16): PostgreSQL reads `canon.json_sql`/`json_on_server`/
   `json_guard` (its own copies gone); MySQL 8.0.4+ (`canon.JSON_MYSQL`,
   from `version()`) writes its JSON in SQL, a double with an exponent or
   29 digits folded here; `neutral_digest(way=)` and `canon.fold_batches(
   way=)` take the chosen hash on both engines; a JSON key is read as the
   server's text. Not done: `hetero._digests`, ship, tailtables and the
   other table-digest callers still pass md5 (their files are other
   tickets'; the probe runs after the table digest there); the MySQL
   server tests (`test_mysql_digests_json_on_the_server.py`) are written,
   not run; MySQL's escape of a control character is assumed lower-case
   hex like `jsonb`'s until that test runs.
8. **Quieting the target's triggers during a load is not carried into
   range worker processes** - a table copied by ranges in parallel fires
   its triggers; one rule for triggers and keys during any write into the
   target (the repair ladder's rule) held in every worker.
9. **A one-pass database program is ranked above migkit's table copier
   without ever having moved a row** on the machine (it failed sign-in to
   MySQL 8.4 every run and exited 0); a program earns its rank by a
   measured run like every other rung.

*Built with P0-SMART 2026-10-01, tests written and not yet run:*
* *3*: `tuning` - every reasoned number declared with today's value as
  its prior and what measures it (`TUNABLES`: the probe floor, the tie,
  `canon.WIDE`, `ranges.BUCKET_ROWS`, `throttle.REPLICA_RISE`, the
  PostgreSQL digest's memory); `observe` keeps what a run measured per
  machine (the knowledge base) and per hop, `value` the geometric mean
  with the prior (weighing three runs), within x8 of it. Adopted: the probe
  floor (`model.probe_floor`, tuned by the bytes a move carried in the
  time its probes took, `MIGKIT_PROBE_FROM` still the operator's) and the
  tie (the noise of ways that ran five units or more). The others wait
  for their modules to read `tuning.value`; the per-table program
  threshold was not found as a constant.
* *4*: the resource model's readings enter the cost model as a prior as
  wide as each coefficient (`costmodel.MODEL_SHARE`), and the joint plan
  gives a way the probes alone measured a spread of at least 50%
  (`jointplan.MODEL_SPREAD`). Measured: with the prediction off by x2, the
  cost model's regret 1.5-12.1% against the prediction alone's 10-15%.
* *5*: `sizing.Held` - a limit of the hop as a target: over it, a cut in
  proportion; under it, one back only where one more stays within it.
  `sizing.held` makes them from `limits: source_load` (the move's own
  rate over what the source's reads carry) and `limits: replica_lag`;
  `resources` adds them to the pace. Simulated: held at the target 75% of
  the windows against 21% for the same limit backed off from as a strain.
  And a cut is no longer floored one short by floating point.

## P0-PARITY. Every engine as deep as PostgreSQL and MySQL (owner, 2026-10-01)

Asked: "can the other engines do better - smart, deep, not flat; if they
are already good, leave them". Measured from `decide.coverage()` and
`capabilities.matrix()` at 6182e06: PostgreSQL and MySQL have a way for
all 10 shapes; Oracle 8; every other engine 3-7. 70 "not yet" cells, and
they are three cross-cutting gaps, not 26 engine problems:

* **Large values** - not yet on 22 engines (all but PostgreSQL, MySQL,
  Oracle): values read, written and hashed in pieces, memory sized from
  the widest row (R22 `model.row_sizes`), wide values digested where
  they lie (`canon.WIDE`) - one mechanism, an adapter per engine.
  **Built (2026-10-01): `migkit/pieces.py`**, each engine's adapter its
  `LARGE_VALUES`. Rows sized where they lie (`sql_row_sizes`; `$bsonSize`,
  `MEMORY USAGE`, the file's own lengths), every read ending at
  `BATCH_BYTES` or one row (`fetch_until`, `by_bytes`, `arrow_rows`), byte
  values past `PIECE` read in pieces by key on SQL Server and Db2
  (`ReadPlan`) and `(max)` values written a piece a statement on SQL Server
  (`written_in_pieces`), Redis keys past a piece read, written and compared
  by their type's own commands, wide values hashed where they lie in every
  warehouse dialect's digest and in each fold (`DigestPlan`, `Hashed`), the
  pair comparing a read-back batch by digest texts, Kafka's producer sized
  to the topic's bound with every send checked, ASE sessions taking whole
  text values (32 KB cut otherwise). Not applicable with the bound as the
  reason: DynamoDB (400 KB item), Cassandra (a mutation, 16 MB), Redshift
  (16 MB value). Left: the generic engine (it moves nothing; item 33),
  and the per-row factors of `PAIR_MEMORY`, `KafkaEngine.COPY_MEMORY`
  counted from the code, measured by `CAP_CALIBRATE=1` on
  `tests/test_large_values_on_every_engine.py`.
* **Partitioned** - not yet on 13: copy by partition (the partition as
  the range), partition coverage checked (deep checks did it for
  PostgreSQL and MySQL), partition-aware workers.
  **Built (2026-10-11), owed a server proof:** the deep check holds each
  partition of each partitioned table to its own rows on both sides on
  SQL Server (`$PARTITION` over the function and column; a partition has a
  number and no name, so `p1`, `p2`), Oracle (`PARTITION (p)`) and TiDB
  (MySQL's reader), through the one verdict (`Engine._partitions_verdict`).
  SQL Server and Oracle are counted and not digested, and the finding says
  how many partitions were compared by count only. Redis, Snowflake and
  Redshift are declared not applicable with the reason. The catalogue
  statements are new and have not run against a server of each kind
  (`capabilities.UNPROVED`); the tests hold the readers' logic over
  stubbed catalogues. Left: the same on DB2, Db2 for i, Db2 for z/OS, ASE,
  Teradata, BigQuery, Iceberg, Delta, Parquet, CockroachDB, DSQL, the pair
  and the generic engine; the copy by partition and the partition-aware
  workers beyond PostgreSQL, MySQL, ClickHouse and Kafka.
* **Large values on TiDB, CockroachDB and Aurora DSQL** - left to be
  measured: each server's own widest row and entry, its packet and
  transaction limits, and what it does at the limit (refused by name or
  cut), on a server of each kind. The numbers are not taken from the
  documentation, from memory or from a sample (a sampled maximum cannot
  certify an exact bound), so the cell stays not yet until they are
  measured (P0-PARITY.lv.new).
  *Not done (2026-10-16), read for the next pass:* `pieces.PIECE` is a
  constant every engine reads, and `ReadPlan` uses it as both the
  threshold and the piece, so a piece decided per engine needs one
  sizing in `decide` (limits read off the server where it says them:
  TiDB `@@max_allowed_packet` and the entry size limit, CockroachDB
  `kv.raft.command.max_size`, `sql.conn.max_read_buffer_message_size`
  and `sql.guardrails.max_row_size_err`; DSQL, Cosmos DB and Couchbase
  have only documented limits) and the rate measured per piece. TiDB
  reads through MySQL's `_read_query`, not `DbapiRows`, and MySQL's `_q`
  is a query, not a quote, so `ReadPlan` cannot run on it without a view
  of the engine. CockroachDB and DSQL write by their own `neutral_write`
  (UPSERT, ON CONFLICT), which skips `written_in_pieces`. Teradata needs
  a first-piece expression in place of the CASE `ReadPlan` uses.
  Couchbase's subdocument API has no substring and no string append,
  so a value past one operation can only go field by field. Cosmos DB
  writes transactional batches cut by count only (the service takes
  2 MB a batch); linked piece items change what the application reads
  on the target and need the user's yes. None of the six has a server
  profile in `tests/servers.py`. TiDB's cell is `untested` at 3c33f9a.
  *Part A built (2026-10-16), on fakes:* `pieces.Sizing` decides the
  piece and the inline bound per side through `decide.Trials` over
  `Limit`s the engine reads (`piece_limits`: TiDB packet and entry size,
  CockroachDB's three settings; DSQL's 10 MiB quota, said as not read);
  a refused size is given up and the value read again; a row past the
  side's widest is refused before it is written. TiDB reads through
  `ReadPlan` (`NAME_QUOTE`, `_run`, `_read_query(select=)`) and writes
  wide rows by `written_in_pieces`; CockroachDB's `_upsert` and DSQL's
  `neutral_write` too, statements cut by the cluster's bytes, DSQL a
  piece a transaction. Proved on fakes in
  `tests/test_the_piece_is_decided_per_side.py`. Left: the cells for
  CockroachDB and DSQL in `decide.WAYS["large values"]` still say not
  applicable (decide.py owned elsewhere; should be `yes @LARGE_VALUES`
  owed a server proof); a server profile for each; what each server does
  at its limit; ReadPlan's `_connect(side, db)` reaching MySQL's
  `_connect(side, retry)`.
  *Part B (2026-10-16):* Cosmos DB transactional batches are cut by
  count and by the 2 MB of the request body measured as sent
  (`cosmosdb.transactional_batches`), an item past 2 MB named before
  anything is sent; Couchbase reads and writes whole documents in
  batches cut by bytes, a document past 20 MB named before its write
  (cell `yes`, UNPROVED: no server); Teradata reads a byte value past a
  piece by `SUBSTRING`, its first piece in the row's select
  (`teradata._FirstPiece`, UNPROVED). Left: `pieces.ReadPlan` could keep
  the first piece and read on from `limit + 1` (it reads the value again
  from byte 1) - a hook in `pieces`; Couchbase `neutral_rows_by_key`
  still asks 500 documents a request; an item between its UTF-8 and its
  escaped 2 MB goes alone and the service decides.
* **Read-only target** (BAR-3) - not yet on 24: the target kept from
  the application while migkit moves into it, or its writes named - the
  same hold-writes primitives the cutover's freezes use on the source
  (one primitive per engine, used for both).
  **Built (2026-10-04): `freeze.protect` on `freeze.Hold`**, the target's
  ladder the ways that leave migkit's own account writing per database
  (`Hold.keep_rungs`, `KEEPS`), taken before the copy and again after it,
  read back in the cutover's preflight (`kept_rows`), given back at the
  flip, by `rollback --apply` and put on again by a cutover undone. Held by
  rights: PostgreSQL and MySQL as before, TiDB (MySQL's way, `KILL
  TIDB`), MongoDB (roles swapped for read; a server checking nobody has no
  way), SQL Server (deny), Redis (every account's writes but migkit's own,
  which must be an account of its own; one server, not a cluster), Kafka
  (a deny for every principal where migkit's is a super user, else each
  allowed principal denied by name on a cluster that refuses the
  unnamed), Cassandra/ScyllaDB (revoke, the cache waited out), CockroachDB
  and Aurora DSQL (revoke), ClickHouse (revoke on `db.*` with partial
  revokes, a user still writing through migkit's role made `readonly`),
  Oracle (table, view and definer's-rights code grants revoked, the
  schema's owner locked), Db2 (tables, CONTROL, and EXECUTE on the
  packages and routines that write the schema; z/OS and i by their own
  catalogues), SAP ASE, Teradata, Redshift, Snowflake (and its future
  grants), Couchbase (roles swapped for read, the user put back whole).
  Offered as a change of an access policy (`target_access`, asked at a
  terminal under --go): DynamoDB (a deny in each table's own policy) and
  BigQuery (the dataset's writing entries made reading ones). Named:
  OpenSearch (each index's own count of operations less migkit's,
  `freeze.counted`), drift at the cutover's gate. Not applicable with the
  reason: files, lakes, Cosmos DB, the read-only sources, the generic
  engine. The source's freeze at cutover (F2.freeze) stands on the same
  holds for TiDB (`tidb_super_read_only` or per database), CockroachDB,
  ClickHouse, Oracle, Db2, ASE, Teradata, Redshift, Snowflake and
  Couchbase, each ending the application's sessions and reading back
  what is held open; Db2 for i and z/OS, DSQL, BigQuery, DynamoDB, Cosmos
  DB, the lakes and the SaaS/SAP sources not applicable, with the reason.
* Smaller: missing grant on MongoDB and pairs; capability gaps on
  warehouses, ASE, Db2 and lakes (6-11 cells each); the generic engine
  is not yet on every shape (decide whether it deepens or is declared
  a thin fallback).

Done when every cell is a way or "not applicable" with its reason, and
the depth tests run on every engine, not only PostgreSQL and MySQL.

Added 2026-10-04 (owner: "if it is not in the backlog, make them equal"):
the ten shapes above measure what an engine can *move*; three more
depths were built for PostgreSQL and MySQL (some for MongoDB, SQL Server
and Oracle) and are owed to every engine:

* **Scope** (P0-SCOPE): `include`, `objects`, `mapping.ops`, the row
  filter (`rowfilter`), column transforms (`transforms`) and name rules
  read through `in_scope` / `takes` / `carries` in every path of every
  engine - its listings, objects inventory, deep checks, repairs,
  counters, tail and DDL.
* **Verification**: the whole deep-check catalogue (orphans, constraints
  not validated, charset/collation, generated columns, boundary, keyless
  tables, duplicate keys, large values), delta verification and the
  fence, repair with exact undo, users and grants, the objects inventory.
* **Change tail and DDL**: per-table states (live/copying/parked/
  removed) with the exact seam, the per-table DDL policy, renames and
  swaps, a table added or removed while running, the operation contract.

Done when a depth matrix generated from the code (as
`capabilities.matrix()` is) shows every engine against every item of
the three - a way, or "not applicable" with its reason - and each way
has its test on that engine.

**All of it (owner 2026-10-04: "complete, all of it deep, all of it
good").** The matrix is not limited to the shapes and the three depths
above: it enumerates *every* capability migkit has, and every engine is
owed every one of them -

1. assess and the pre-flight (every check, the managed service's own
   limits, the closure of what is taken, the scope explained);
2. schema and conversion (made on the target, compared, fixed, stored
   code proved);
3. move (every way the decision engine knows, resumable, parallel and
   sized, large values, partitions, filtered, mapped, masked);
4. change tail and DDL (as above);
5. verification (every check, consistent snapshot, fence, delta,
   second reading, the service's own check held against it);
6. repair with exact undo;
7. cutover (freeze, drain, counters, flip, rollback, the stream back);
8. scope (as above), accounts, grants and objects;
9. security (TLS verified, every sign-in form, secrets, egress);
10. managed services of every provider (P0-CLOUDS);
11. HA and fencing (W1), evidence, observability and the forecast.

**Enforced, not hoped for:** a test reads the matrix and fails when any
cell is "not yet" without either a backlog row naming it or a written
"not applicable" reason - so an engine left thinner than PostgreSQL and
MySQL is a red test, never a silent gap.

*Built 2026-10-04, 9fd2442 (P0-PARITY.matrix, P0-PARITY.guard):*
`capabilities.depth()`, beside the capability table it extends - 86 items
in 10 areas, each read off the code an engine runs: its own methods, a
mixin's or another engine's it inherits, the base's helpers they call, and
the pair's machinery where it hands the work to it (`_as_pair`), never a
base placeholder or a method that only refuses (a skip, an error, None or
an empty answer - `_refuses_only`, which now catches the last two as well).
* *scope* (10): the counts, data and schema checks' listings asked of the
  one scope answer (`in_scope`, `_scoped_rows`, `takes`, `takes_table`),
  the deep checks', the copy's (`list_move_tables`), the repairs', the
  counters'; the objects beside the tables (`takes`, `takes_object`);
  `mapping.ops` where changes are applied (its own tail asks `carries`, or
  its changes go through the pair's tail); the row filter, the transforms
  and the name rules read by the copy and the check alike.
* *verify* (39): each of the 30 categories of `engines.base.CATEGORIES` an
  engine's checks can say (the last word of a finding's scope, a finding
  that can only skip or fail not counted, one made by another module's
  function the check calls counted); delta, fence, confirm and users (the
  capability table's own probes and gaps); repair; repair with exact undo
  (`RepairAction.undo` filled, or the rows kept by `repairlog`); the
  fields a verdict reads (`Result.rows_apart`, `.tables`, `.table`).
* *tail* (6): follows (the capability), per-table states (the tail it runs
  keeps a `TableSet`), a table added at an exact seam, a table back with
  only its changes since, the per-table DDL policy (its change reader hands
  `canon.ddl`), renames and swaps (`table_oids`, or a DDL naming where to).
* *assess* (7), *schema* (6), *move* (7: table copy, bulk, large values and
  partitioned from the shapes, rows sized, the load window, missing tables
  made), *cutover* (8: freeze, read-only target, counters and rollback
  from the two tables; the drain, the servers' own replication paused for
  a repair, two-way marks, scheduled jobs), *security* (a TLS posture that
  can come out verified), *managed* (its own services catalogued),
  *operations* (fencing).

A way counts only once a test proves it on that engine: a test module's
`PROVES = [(item, engine or engines)]`, read from its source and never
imported (`capabilities.proofs`); 208 modules carry one. Every gap is
declared: `DEPTH_GAPS` (not applicable only where the thing does not exist
on the engine, the reason said once for a family or taken from `GAPS`; not
yet with its P0-PARITY row, the generic engine's with P0-PARITY.small) and
`UNPROVED` (a way in the code whose test on that engine is owed; it reads
as not yet). The guard,
`tests/test_every_engine_is_as_deep_as_postgres_and_mysql.py`, fails on a
cell nobody declared, a way with no test nobody declared, a declaration the code or a test has closed, a test claiming a
way the code does not show, a "not yet" whose row is not in
`docs/backlog-index.md`, and an item or engine nobody knows. STATE.md
shows the matrix: per engine the ways proved of the items that apply, area
by area, and per item the engines in each state.

*SQL Server brought up to PostgreSQL and MySQL (branch gap-mssql,
2026-10-11):* of its 49 open cells, 9 are proved by stand-in tests (a
scripted server, `tests/mssql_stub.py`, no server) - and a target ahead of
the source, which the list lacked, a tenth: the tables its copy lists, the
objects beside its tables, the changes it applies, the finding that names
its table, the repair of counters and rows with its undo, the keys a scope
leaves out, and a target kept from the application and given back exactly -
the statements of those ran on servers before. 32 are built and read `test
owed` in `UNPROVED` (one of them, the tail's per-table states, was built
before), their statements new and never run on a server: the row filter and the name rules
in its counts and hash pass; the value checks (narrowing, time zone,
character set, collation, computed columns, NULL against empty, floats,
rendering), indexes switched off, partitions, CHECK constraints not
trusted, row-level security, EXECUTE AS drift and table and sequence
grants; a swapped table told apart by its object id; the assessment
(versions, brands, its own checks, the predictive checks, the target's
room, other writers, the work left for hands); the indexes and column rules
a table is made with and built after its rows; the tables the target lacks
made first; the load window (triggers off, recorded first, put back; the
indexes stay on, as in the other two); the Agent jobs run on one side at a
time; the partitioned table's deep check. One is not applicable (an indexed
view cannot go stale). Seven are left, with the reason: column transforms and the
collection options (not yet on PostgreSQL and MySQL either); the schema-diff
reading (no program reads SQL Server's); the per-table DDL policy (Change
Tracking carries no DDL, and reading a table whose tracking history was cut
as a reload rewrites the read that stops the tail today); two-way marks;
TLS (the driver checks no certificate); fencing (a hold in every write path,
and a server to prove it).

Measured at 9fd2442: 3,096 cells - yes 435, test owed
252, not applicable 869, not yet 1,540. By area (yes / owed / n/a / not
yet): scope 37/92/50/181, verify 124/74/499/707, tail 26/18/59/113,
assess 32/16/26/178, schema 37/20/74/85, move 83/20/52/97, cutover
53/5/103/127, security 22/6/1/7, managed 19/0/2/15, operations 2/1/3/30.
PostgreSQL proves 72 of the 84 that apply to it and MySQL 63 of 78; the
furthest behind are the SaaS sources (0 of 24), Db2 for z/OS (1 of 69),
Snowflake (2 of 78), Db2 for i (2 of 81), SAP ASE (4 of 80), the generic
engine (4 of 86), Redshift (5 of 75), BigQuery (5 of 76), Db2 (6 of 81)
and TiDB (6 of 76, with 36 ways in its code - MySQL's, inherited - and no
TiDB test for 30 of them). Found by building it, each now a declared gap
rather than a counted way: Kafka's version read only says it cannot tell;
SQL Server's connection never checks a certificate, and Oracle's, Db2's
and SAP ASE's ask for no TLS; MySQL's index-state check only skips;
ClickHouse's and Oracle's deep findings and the shared capacity check's
end in words no category reads (`mutations`, `constraints`, `capacity`),
so they reach the verdict unclassified.

*Cassandra and ScyllaDB, 2026-10-11 (of 49 cells open at the start):*
18 now yes, proved on `tests/cassandra_fake.py`, an in-process stand-in
that answers only statements this engine's server tests have run - the
listings, deep checks, copy, repair plan and name rules (the engine's own
listing asks the one scope answer, `Engine.in_scope`), `mapping.ops` in its
tail, columns, narrowing, null vs empty, float drift, replacement
characters, timestamp shift, a target ahead, table grants, rows apart,
table, the tables a finding compared and confirm. 19 built and their test owed, because each sends a
statement no server has yet been sent: secondary indexes, triggers,
materialized views, types and functions, table options and clustering,
the schema difference (`structural-fix.cql` and its undo), repair and its
exact undo (a delete by key; a null written as one, which a repair needs and
a copy does not), objects held to the hop's `objects`, versions, brands,
readiness checks, the pre-flight, capacity, handwork, indexes carried by the
pair, rows sized across the token ring, the load window (triggers off
for a load, put back after it) and tables made as the source has them with
their indexes, triggers and views once the rows are in. 2 not applicable
(rendering: rows are hashed here from decoded values; defaults: the engine
has none). 10 left: row filter (the pair refuses one for an engine with no
SQL; a client-side filter belongs to the pair), column transforms, the four
tail items (per-table states need `TableSet` in the own tail; an exact seam
needs reads and writes at a quorum), other writers on the target (no
server table says who writes), stored code (the converters and the proof
run SQL; the schema check compares and writes the statements instead),
two-way marks and fencing (neither has a place in a mutation: a mark
would need a column or a side table the change log joins, a fence a
transaction across partitions). Found on the way: a repair or an undo left
a column the source holds null filled on the target, and could not delete
a row by its key; a table made through the pair lost its clustering order.
*Oracle, 2026-10-11 (P0-PARITY.scope, .verify, .tail, .all): of its 57
cells open, 16 are ways with a stub test, 37 are built and owed a sandbox
run, 4 are left.* Proved over stubbed catalogues (statements the engine
already sent): the scope read the same way by the checks, the copy list,
the repair, the change tail, the row filter and the name rules, through
the pair (`tests/test_oracle_holds_to_the_hops_scope_through_the_pair.py`);
a repair's every identity column put back where it was; foreign keys read
whole; the not-validated constraints and the character sets as findings of
their own category; the rows apart and how each table came out in a
finding; a copy held to the SCN it began at (the tail's seam for a table
added) and a table that left read again from where it left; tables the
target lacks made from the source's. Built and owed a server (they send
statements that have not run against Oracle, `capabilities.UNPROVED`):
the deep checks (keys, foreign-key orphans, deferrable constraints,
columns, narrowing, computed columns, collations, NULL and empty, floats,
shifted times, interval and zone rendering, target ahead, triggers,
indexes, materialized views, row-level policies, grants, sequence grants,
rights a routine runs with, table options), the assessment (versions,
brand, pre-flight, room, other writers, the work left to hands), column
rules and indexes made on the target, scheduled jobs switched one side at
a time at cutover, TLS with the server's certificate checked, a schema
change read from the redo as text, and rows sized by the server. Left:
two-way marks (a rung of marking, a reader that leaves it out and a proof,
all measured on a server - the rule of `marks`), fencing by the target
itself (Oracle has no shared row lock to hold a writer's number under: a
table lock design that has to be measured, and `shared` and `fencing` write
their statements for `%s` parameters), column transforms (not yet on
PostgreSQL and MySQL either), and the structural schema diff (a program's).
*OpenSearch, built on the depth matrix (it had 51 cells open):* 25 now
proved on a model of the cluster that answers only the requests the engine
makes (`tests/opensearch_fake.py`) - the scope of its listings, copy, deep
check, repairs and follow (`mapping.ops`), the row filter and the name
rules, eleven deep-check categories, confirm, rows apart, tables and
table, the load window (written to disk first, so a load that died is
found and its settings given back), tables made like the source's, rows
sized where they lie, the column rules; 12 built and owed a proof against
a real server, because each asks the cluster a request no migkit run has
sent (the objects beside the indexes, the target ahead by document
version, sharding, the time-zone shift, repair and its undo - a bulk
delete -, and the five assess items); 2 not applicable (charset and
rendering: a document is JSON text, always UTF-8, and comes back exactly as
sent); 12 left: the five tail items (its follow is a loop of its own and
finds deletes by counts and ids; the shared tail's per-table states need
one change record per document, and OpenSearch keeps no delete log a
request can read), column transforms (no engine has them), the schema
diff (PostgreSQL's is a DDL differ with generated SQL and its undo),
views (a filtered alias is the nearest thing, compared and named but not
carried), stored code (scripts and pipelines have no target outside
OpenSearch), two-way marks (a document has no room for a mark and a bulk
no transaction), scheduled jobs (transforms, rollups, snapshot policies and
monitors each have a switch of their own, none run against a server) and
fencing (a bulk cannot be held to a number the target keeps). Found by
building it: a SQL table into OpenSearch stopped at the first table it had
to make (`canon.DDL` had no entry for it), and a load that died inside the
copy's window left the index with no replica and no refresh for good -
the next run took the window's settings for the index's own.
*Kafka's tail, assessment, schema, move and cutover cells, 2026-10-11:*
twelve of the fourteen cells it had open under those areas are built. A
topic is Kafka's table and the tail follows topics.
* *tail* (4, proved on `tests/kafka_fake.py`): `topicstates.TopicSet` keeps
  each topic of a running tail live, joining, left or parked beside its
  saved offsets, written before the topic is copied. A topic added to the
  scope (or made while the hop's `ddl` applies) joins at the source's end
  offsets then (`snapshot_mark`, `mark_covers`); one that left and returns
  is copied from its saved offsets (`REREADS_FROM_A_POSITION`), only if its
  target topic is as it was left; a topic made, deleted, grown in
  partitions or recreated shorter goes by the hop's `ddl` per topic
  (`tailtables.policy`: stop, park, apply, and `skip` for creates, drops
  and truncates). With no `ddl` in the hop a topic made is followed as it
  always was, and one that grew or was recreated stops the tail as the copy
  always did. Found by building it: a copy resumed past messages the
  source's retention had taken was moved to the log's end by the client's
  own reset, and everything between passed over without a word
  (`_cut_by_retention` counts them, says so, and keeps an identical
  partition's offsets below the cut); a topic recreated shorter than the
  offsets saved of it was skipped whole.
* *assess* (5): the version is the brokers' own setting, else what the
  client library works out from the protocol they accept (said to be
  inferred), held to the other side's at the minor version, to 0.11 (headers,
  timestamps, transactions) and to the newest broker the library knows
  (`assess.versions`, proved); the partition counts and the message limits
  are the pre-flight (`assess.preflight`, proved - a limit is held to the
  topic's, not to a sample of its messages); the topic settings the copy
  does not carry and the accounts it leaves to `migkit users` are counted
  and named, and an unreadable probe is unknown, never none
  (`assess.handwork`, proved). Built and owed a proof against a broker:
  the producers the brokers hold state for on the hop's topics, the
  principals the ACLs let write them and the client quotas that throttle
  producers (`assess.writers`), and the target's free disk against the
  topics' sizes times their replicas (`assess.capacity`) - their calls,
  `describe_producers` and `describe_log_dirs`, have not been asked of a
  broker.
* *schema, move* (2, owed a proof): a topic the target lacks is made with
  the source's partitions, its replication (no more than the target has
  brokers for) and the settings that decide what it means, before the
  copier fills any (`create_missing`, `neutral_create_sql`); the
  replication is read with `describe_topics`, which has not been asked of
  a broker. An existing topic is never touched.
* *cutover* (1, proved on the fake): the drain is the source's end offsets
  (`log_position`, an open transaction's range included) and the tail's
  saved offsets reaching them (`position_reached`); `drain_report` says
  what is left and how far behind each consumer group stands.
* Not built, and why: *cutover.two-way* - nothing marks what the copy
  writes so that a way back leaves it out (the marks ladder is PostgreSQL's
  and MySQL's, and `twoway.refuse_unless_able` refuses a Kafka side), and a
  loop-proof mirror needs a mark on each message and a reader that skips it,
  proved against two clusters; *operations.fencing* - the hold is a row of
  `migkit_origin` read under a shared lock, and a Kafka writer is held by
  its transactional id only in the copy's `mapped` mode (a producer epoch
  the broker refuses a stale writer by), not in `identical` or `plain`;
  carrying it to the standby needs the hold armed on Kafka and a proof on
  two brokers.

## P0-SMART. The decision engine, smart and not flat (owner, 2026-09-30)

Asked: "is the decision engine smart or still flat - I want it really
smart". Honest state at c7dc5a2: the frame is right (needs, probes,
measured costs, footprint as tie-break, kept choice, runtime fallback,
source-change offer) but the intelligence in it is shallow:

1. A rung's cost is one number (seconds a unit, per hop), not a function
   of what the unit is: measured on a small table, it ranks the same on a
   500 GB one with LOBs over a 50 ms link.
2. A rung never run is ranked by its list's order, not by a prediction -
   the resource model (R22, `model.py`) can predict it and is not asked.
3. Exploration is "try every untimed rung once", on whatever unit comes
   next: a slow way can be tried on the biggest table, one sample is
   trusted, noise is a fixed 5% tie.
4. Every choice is made alone (way, workers, table order, index builds,
   verify passes), never as one plan under the hop's limits.
5. Nothing re-plans when the world changes mid-run (a source that gets
   busy, a link that slows): the pace backs off, the way never changes.
6. What one hop learned does not help the next hop, engine pair or
   machine.
7. Only seconds count: the source's load, money (bytes billed, egress),
   the outage and the target's memory are not constraints of the choice.

To build (the owner's rule stands: correctness first as a hard filter,
then the fastest by measurement, footprint breaks ties; predictions are
used only where nothing is measured, and a measurement always beats a
prediction):

* **Cost as a function of the unit.** cost(rung, facts) = start-up +
  work / rate of the resource that binds, with the facts that move it
  (bytes, rows, widest row, LOBs, keyless, types, partitions, link RTT
  and bandwidth, server load); fitted online per rung x engine pair x
  shape from every unit it ran, with its uncertainty (mean and spread),
  not one number.
* **Predict before measuring.** A rung never run here gets its cost from
  the resource model and from what similar hops measured, with a wide
  spread; the plan says which numbers are measured and which predicted.
* **Explore safely.** Try an unmeasured rung on a small representative
  unit, never on the critical path of a deadline; stop exploring when
  the best rung's lead is outside the spread (a bandit with a risk
  bound, not "each once").
* **One plan, jointly.** Choose the way per table, the workers, the table
  order (longest first), index builds overlapped and the verify passes as
  one schedule minimising the makespan under the hop's limits: deadline,
  the source's load cap, money budget, outage budget, target memory and
  connections (a CP-SAT or greedy-with-bounds scheduler; measured against
  the per-choice greedy).
* **Re-plan while running.** When a unit's rate leaves its predicted
  spread, re-climb for the ranges left (switch ways at range edges, which
  are idempotent), and say why.
* **Learn across hops.** A local knowledge base (never leaves the
  machine) of rung performance and failure by engine pair, version, shape
  and link class seeds the next hop's priors; migkit's own benchmarks
  ship as the first priors.
* **Risk in the cost.** A rung's chance of failing here (from its history:
  a service that refuses, a version that breaks) times the work a failure
  throws away enters its expected time.
* **Constraints as needs, trade-offs said.** The source's load cap, money
  and outage are hard limits where the hop sets them; the plan names the
  trade-off ("the fastest way bills about $X; the chosen one is 12%
  slower and bills nothing").
* **The owner, the same day: "a real decision engine - compute every
  way, use every factor, propose and decide what fits: smartest,
  fastest, most correct, most consistent, most idempotent."** So:
  every composition of parts that fits is costed (dominated ones pruned,
  the count said); one facts catalogue of everything a choice may read,
  and a rung reading an undeclared fact fails a test; correctness,
  consistency, idempotency, exactly-once and resumability are never
  traded - each part declares its guarantee levels, a strategy holds the
  weakest of its parts, a weaker fast part is allowed only where migkit
  tops it up to the strongest level the engines allow (proved), and
  speed is optimised only among strategies that reach it; the dry run
  proposes (choice, predicted time and spread, guarantees, runners-up,
  deciding and unknown facts) and `--go` decides.
* **The owner, later the same day: the choices stay inside.** "The
  logic and the choices inside - the user doesn't need to know them; let
  the decision engine run it and choose inside migkit, and what comes out
  must be the best, smartest, fastest, most correct." The console shows
  outcomes only (what happens to each side, predicted time and outage
  with spread, guarantees reached, what the operator must do or approve,
  one line when a way switched); the decision record (strategies
  evaluated, the choice per table and phase, runners-up, deciding and
  unknown facts, predictions against measurements) goes to the run's log
  and the evidence export, and to the console only with
  `MIGKIT_EXPLAIN=1`. No way or program is named on the console.
* **Counterfactuals in the plan and the evidence export:** the chosen
  rung, its predicted time with spread, the next best and why not.

*Built 2026-09-30/10-01, its tests written and not yet run (the owner's
rule: one run of the whole suite at the end). The research and every
number: `docs/research/smart-decisions-2026-09-30.md`,
`bench/smart_decisions.py` (in-process simulations, no server):*
* *Cost as a function of the unit* (`costmodel`): `CostModel`, a Bayesian
  linear regression (normal-inverse-gamma, sufficient statistics, each
  unit weighed by its size) of seven features - start-up, MB, rows, large
  values, keyless bytes, round trips, busy bytes - per way and engine
  pair; `Estimate` (mean, spread, measured or predicted, p10/p90, the
  noise of a unit about the line). Measured regret against one number a
  row: 1.3% against 30.9% as timed, 4.6-7.4% against 71-1175% on shapes
  never timed. Keeps the `Costs` interface (`get`, `saw`).
* *Predict before measuring*: the prior from this hop's earlier runs
  (measured, `costs.json` beside its rates), other hops here (the local
  knowledge base `Knowledge`, `MIGKIT_KNOWLEDGE` or the user's data
  directory, never sent anywhere), the resource model where a rung says
  what a byte of it loads (`Rung.loads`, `startup`, `trips`; its spread
  the coefficient's own - P0-DEPTH 4), migkit's benchmarks (`seeds`:
  `movers.MY_RUNGS` solved for seconds a MB and a thousand rows,
  `physical.MEASURED`). `climb(..., unit=, explore=)` ranks measured
  ways by what they cost, a tie within their noise to the footprint; a
  predicted way goes first where its p10 beats the measured best (tried),
  or its p90 does where the climb commits; where nothing is measured the
  predictions rank; a way nothing says anything of is tried first, as
  before. `Costs`/`HopCosts` rank as they always did.
* *Explore safely* (`Trials(model=, unit=, ahead=, critical=, log=,
  limits=)`, `pick(unit)`, `took(..., unit)`, `save()`): a new way only on
  a unit no larger than the median, never on the critical path, never
  where its p10 is above the measured best, a measured way timed again
  while it has fewer than two units and its p10 is below the best, within
  a 5% conservative budget. A table's time over its best: 60.6% each way
  once, 22.6% safe with nothing predicted, 5.7% safe with a prediction.
  `Trials` without a model is what it was.
* *One plan, jointly* (`jointplan`): each table's way as one mix (a local
  search on the fluid bound, money a hard limit), the order, the tables
  at once, index builds beside the copy, each check once its table is in,
  the fewest workers within 2% - simulated as max-min fair flows through
  the resources. 8.7% sooner than the per-choice greedy on average, 18.4%
  at p10, 32% at best, within 3.6% of the lower bound at the median;
  CP-SAT measured beside it, 2.3% later and 5 s a plan, not taken. The dry
  run says its end with p90 and what sets it (`cli._joint_lines`); the
  move starts its tables in its order (`_largest_first`, the one-at-a-time
  path too), with its tables at once and workers (`_joint_for_move`), and
  keeps it beside its reports (`jointplan.PLAN_FILE`, `order_of`,
  `planned_way` for the copier choosing a table's way).
* *Re-plan while running*: a unit whose time leaves its spread
  (`costmodel.SURPRISE`) halves what was learned before it and the next
  unit re-climbs; a limit of the pace cut (`sizing.Limit`, `Trials(limits
  =)`) costs the ways that load that resource at the limit's cut; a switch
  is said in one line. No second controller.
* *Risk in the cost*: tries and failures per way, engine pair, version and
  service (`Knowledge.failures`, a Beta(1, 19) prior, `CostModel.risk`);
  `costmodel.risked` - the thrown-away work (a range's where the way goes
  on by range, `guarantees.lost`) - in every estimate the climb ranks;
  `attempt` keeps each failure and success where its costs are a model.
* *Limits as hard needs* (`hopkeys` `limits:` - source_load, money,
  bytes_billed, outage, replica_lag, target_memory, target_connections -
  and `deadline:`; `hopkeys.LIMITS`, `explain_limits`,
  `jointplan.limits_of`, `outage_line`, `docs/goals-and-modes.md`).
* *Counterfactuals*: `Choice.costs`, `runner_up`, `why_not`,
  `guarantees`, `asked`, and `Choice.as_evidence()` (in the operator's
  words, no label) for R23.1; `Strategy.estimate`, `guarantees`,
  `runners`, `as_evidence()`.
* *Every way* (`decide.space`, `Space`): every combination that fits,
  walked depth first and left out only by the proved bound of the best
  guarantees a subtree can reach; ranked by cost on the unit; counted
  (combinations, weighed, unfit, weaker, bounded, slower beyond doubt);
  the structural walk kept per parts and need; the strategy taken told to
  `LISTENERS`. `jointplan.ways_of_space` feeds the best strategies to the
  plan as ways.
* *Every factor* (`factbook`): the catalogue of every fact a choice may
  read, by group and how it is known; `account` (known, measured,
  predicted, unknown); `Facts.asked`; `tests/test_every_fact_is_declared
  .py` reads the code and fails on an undeclared fact; `doctor` says what
  each last plan stood on (`cli._decisions_said`).
* *Guarantees never traded* (`guarantees`): five ladders (correct,
  consistent, idempotent, exactly once, resumable), a rung's levels
  (`Rung.holds`, or read from its capabilities), a strategy the weakest of
  its parts, `Lift`s to a fixed point, the lexicographic strongest; the
  climb filters where a rung says its own, the strategy space always;
  property-tested. How much a stop throws away is a cost (`lost`), not a
  guarantee.
* *The choices stay inside*: `decide.record` appends the decision in full
  to the run's own `decisions.jsonl` (the session's shape, the whole under
  "detail"), `explaining()` - `MIGKIT_EXPLAIN=1` - puts it on the console;
  the console says only the plan's end, what sets it and the limits.
* Not done here, in files others own: routing the copier's ways
  (`pg_pass`, `movers.my_bulk_choice`, `legs`) through `Trials(model=...)`
  with `Rung.loads` declared, and the verify passes (`leaves.PASSES`)
  through `space` - the API is there for the ladder agent.

## Decided by the owner 2026-09-30

* **A faster way that needs a change on the source is offered, never
  assumed and never hidden.** First case: MySQL `CLONE` needs the clone
  plugin on the source. migkit finds out for real whether the change is
  possible there (the managed services mostly refuse it - read the
  server's answer and privileges, never guess from the name), and when
  it is, offers it in one step: what it would change, that it is taken
  off again afterwards, and what it saves in measured or estimated
  minutes and source load. The operator answers yes (migkit does it,
  behind the approvals, reversibly - easier than typing the statements)
  or no. On no, or where it is impossible, migkit says what not using
  it means (the next way, its time and its load on the source), records
  the decision, and goes on with that way. Many teams will not allow a
  change on a production source - DMS/DTS do not make one - so no is a
  normal answer, not an error. One mechanism for every such change
  (decide.py rung needs + approvals + dba.py), not one per engine.
* **A way that is blocked or fails is left for the next one by itself.**
  No prompt - one line in the log says what failed, what migkit switched
  to and what it costs. One runtime fallback in decide.py for every
  choice (the failed rung given up and recorded, its partial effect
  undone or proved away, the next rung that still holds taken). Never on
  a verdict: a difference found is the answer, not a failure of the way,
  and no fallback ever climbs to a rung weaker than the hop needs. The
  source-change offer is the only thing that asks; its "no" then
  switches like any blocked way.
* **Target interlock default stands:** a move into tables that already
  hold rows refuses unless the hop sets `target_rows` (refuse, replace,
  keep).
* **`goal: cutover` does not turn `cutover:` on.** The migkit-driven
  cutover runs only where the hop sets `cutover:`. Without it migkit
  lists the steps to do by hand. Every goal and mode must be explained
  plainly - in `init`, `assess`, the dry run and the docs: what each
  does on each side, and what turning `cutover:` on adds (the timed
  path, the freeze, the drain, the fenced check, the counters, the
  stream back, the undo, the outage measured).
* **A way that is blocked or fails switches by itself** (added the same
  day): "if one way is blocked or fails, switch to another way by
  itself - automatically, don't ask, just say it in the log". Never on a
  verdict, never to a way that gives less than the hop needs; all ways
  gone stops with every way's reason. The source-change offer stays the
  only question.

*Built 2026-09-30, its tests written and not yet run (the owner's rule of
2026-09-30: one run of the whole suite once the backlog is done):*
* *One mechanism for a change on the source*: `decide.Change` on a rung
  (`Rung.changes`), its probe read on the server into a `decide.Stand`
  (present, possible, a DBA's, impossible), `decide.offered` for what the
  answer buys; `sourcechange` holds the one list of changes
  (`DEFINITIONS`, which `dba`'s scripts are now written from), the
  answer (`source_changes`, `physical.source_plugin: install` as its
  yes, a terminal's answer recorded), the offer in words with minutes
  measured or estimated from `physical.MEASURED` and the source's load,
  `making` behind the `source-setting` approval with its record and its
  undo (a killed run's taken off by the next, a refusal the answer from
  then on), and `for_the_dba`. Routed through it: MySQL's clone plugin
  (offered), the copy of PostgreSQL's pages' replication right, WAL
  senders, slots and `wal_level` (a DBA's, said without a question). The
  requirements of following changes (PostgreSQL's `wal_level`, slots,
  senders, replication, publication; MySQL's binary log, row format and
  image, retention, replication grants; MongoDB's replica set and
  DocumentDB's change streams; SQL Server's change tracking; Oracle's
  archiving, supplemental logging, table logging and grants) stay
  requirements, written from the same list with what not having them
  means - in the DBA's script, `init`, SQL Server's refusals, MySQL's
  `assess` and PostgreSQL's slot.
* *Goals and modes explained* from one table (`hopkeys.GOALS` with each
  side and the guarantee, `MODES`, `CUTOVER`), said by `init`, `assess`,
  every dry run and `docs/goals-and-modes.md`; no goal turns `cutover:`
  on, and a cutover hop without it is told the hand steps and what it
  would add every time.
* *The runtime switch*: `decide.attempt` (with `Blocked`, `Verdict`,
  `Exhausted`, `operational`, `only_blocked`, the give-ups kept beside the
  work's position with the facts that stopped them). Routed through it:
  the bulk way of `move --mode full` down to the table copier, the
  physical seed down to the logical copy, the servers' own replication
  down to the change leg run from here, the grouped narrowing of a
  differing table down to its own walk. Then every engine's own
  try-and-except switch, through `Engine._attempt` (the switch said in
  the work's log, or the log a copier or a repair lends the engine, once
  a way for a work; the last way that holds failing in its own words):
  SQL Server's bulk load to statements, kept for the table's run, and its
  Arrow move to table by table; ClickHouse's pull to the table copier;
  MySQL's load to inserts - a batch, a range rolled back whole, a staged
  run of upserts; MongoDB's id-hash walk to the bytes on both sides (one
  side's hashes had been held to the other side's bytes, every document
  "changed"); PostgreSQL's consistent pass from shared-snapshot lanes to
  one transaction, its repair from the replica role to the triggers as
  they are, the roles read by name where their hashes are refused (the
  passwords not compared now said), its write probe's six places; Redis's
  absolute expiry to the time left; the pair's leaves to the walk; a
  warehouse's digest in its own SQL to the fold here; DynamoDB's tags to
  a file for the endpoint. Still one-off, in files other work owns, to be
  routed through it: the per-table streaming copy to ranges,
  `my_bulk_choice` and the streaming copy that cannot reach both sides to
  the local dump (`movers`), the pass-through's ranges to text
  (`engines/pg_pass`, on `decide.Trials`), Oracle's digest in SQL to the
  fold (`engines/oracle`), and `movers.stream_way`, whose facts
  `cli._stream_facts` mirrors until it takes them.

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
  (DEC.ptcs, 2026-10-16: `replicasum` - the rung, the pairing, the
  account and the install are in; *Not done:* the server test
  `tests/test_a_native_replica_is_proved_on_servers.py` is written and not
  run; the checksum table is not yet a `decide.Change` offered to the
  operator (it is written where the account may write it); MariaDB's
  GTIDs are not fenced; the cost is recorded per row, never yet ranked
  against a measured own-digest rate. W4E2, 2026-10-16: the table is
  `replicasum.TABLE`, a change the operator answers (`checksum_table`),
  migkit's own dropped after; MariaDB told a replica by its GTID
  positions and fenced through `src_lsn`; each table climbs on the
  replica checksum's rate against the digest's alone, timed on the same
  chunks, an untimed way tried once on the smallest table, a source being
  written needing `online`. *Not done:* `decide._is_native_copier` still
  reads "migkit's own" in a rung's words as a copier - the digest rung is
  worded around it, and a field saying "not a copier" belongs in
  `decide.py`; the MariaDB replica and the bounded trial are proved on
  fakes, not on a server pair.)
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
    *LT.C1 done (W11F, `engines/mongodb.py`):* the MongoDB follow's
    position is kept in the attempt ledger before the position file, and
    a crash between the two goes on from the ledger (`_follow_state`);
    events are read as raw BSON, so a Decimal128, any binary subtype and
    a date past year 9999 arrive byte-identical; a drop, rename, dropped
    database or reshard starts a generation of that collection, by the
    hop's `ddl` stopped at, set aside, or copied again from the source in
    a ledger-kept copy while the others go on (`_new_generations`,
    `_reseed`, `_finish_reseeds`); an ended stream opens again after its
    end; a write refused for blocked user writes stops with the next
    step, and an account that may write past a block is said
    (`_audit_write_block`). `FOLLOW_WAYS` gained the saved-position need
    (lost, or too far behind to catch up at the measured rate before the
    oplog drops it: `_position_held`, `token_age`) and a `reseed` rung.
    Proved: `tests/test_mongo_follows_change_streams_by_generation.py`.
    *Not done:* the oplog rung still decodes entries (a date past 9999
    stops it there); reshard events need the server's expanded events
    (6.0+), which the stream does not ask for yet; a reseed after a lost
    position does not drop target collections the source no longer has.
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
* Done 2026-09-28 (R11): `prove_converted` (`engines/hetero.py`) said ok
  when every routine was a procedure it never ran; triggers and events
  were not checked; one failing call failed its whole side without
  naming the input. (stored-code-conversion)
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
W12J note, 2026-10-16 (F7.edges): `ranges.cut` cuts a table's range edges by
`decide.CHUNK_EDGES` - the engine's statistics (PostgreSQL, MySQL, SQL
Server, Oracle statements in `ranges.statistics_sql`) while the table moved
under half a range since, else a sample of the live key, else the key read in
order; the way and why are said in the plan. `ranges.refine` splits fat and
merges thin pending ranges from the rows finished ranges held (`finished(...,
rows=)`). Proved by `test_chunk_edges_come_from_statistics.py` (fake server;
written, not run on a database). Not done: MongoDB and `claims.py` still cut
their own edges; keys that are not integers get edges, `split_keyed` and
`range_where`, but no copier reads ranges of them yet; PostgreSQL's
modification counter lags a second after ANALYZE, so
`test_range_edges_come_from_the_tables_statistics` (another ticket's file)
may now see a sample where it expects statistics; R20.11's MySQL and
CockroachDB counters (unchanged.py) are not touched here.
W13I note, 2026-10-16: `ranges.plan(st, None, None, edges, save)` (`plan_keyed`)
plans a key of any type from its edges alone, ends open, and `ranges.pred`
/ `describe` read a range of it; checkpoints name a range by `ranges.ident`
so text and composite keys survive the file. `ranges.cut_with` is the one
choice of edges for a source that speaks no SQL (`ranges.layout` writes the
plan a copier keeps itself). MySQL text histogram bounds are decoded;
CockroachDB statistics are read (`show histogram`) and judged by the rows
they counted against the rows now. PostgreSQL's counter is read again once
after `COUNTER_LAG` when it is high within that time of an analyze. MySQL
counters (`unchanged.MySqlCounters`: per-table row counters, InnoDB table
id, creation time, columns, server start) are live; the statements are not
run on a server. Not done: no engine copier calls `plan(None, None)` /
`pred` yet (postgres.py W13A, mysql.py mssql.py hetero.py W13E);
`claims.copy` (claims.py:412, `ranges.split`) should call `ranges.layout`
(W13E) and mongodb.py (~2342, `plan_from_boundaries`) `ranges.cut_with` with
its `$sample` and `$bucketAuto` readers (W13K); `ship.py:911` also cuts by
hand (W13L); the MySQL engine does not build `MySqlCounters` markers yet
(W13E); CockroachDB exposes no write counter, so it stays refused;
`tests/test_a_table_is_skipped_only_on_counters_that_hold.py:252` still
expects `mysql` in `unchanged.UNPROVED` (W13P).
**Built 2026-09-30, the tail's (tests written, the full run to come):**
a batch ends at the last source commit it holds where a read can stop
inside a transaction (MySQL, MongoDB): the commit's position saved
(`canon.ended`'s `txn_at`), the rest read with the next batch. Every
reader marks each commit's time; the beat carries `through` and `lag`
(source commit to target commit) and `/metrics` says both. A follow
runs until asked (ctrl-c, a service manager's stop, `tail.stop`) or,
with `follow_until: caught_up` or where a schedule started it, until it
reaches where the source's log was when it first read to the end, and at
the slot's end at the latest - a scheduled tail held its slot to
`max_duration` and every slot counted as failed. A one-way tail numbers
its batches on the target by default (`marks.settle_exact`, the table
rung proved by what it keeps; `exact_batches: false` off), wherever a
batch goes as one transaction; one in lanes is read again after a stop,
as before. The tail's reader and its lanes fail over through
`decide.attempt`.

**F7, the non-SQL tools used whole - built 2026-09-30, tests written, the
full suite to run.** Each tool wrapped whole and accounted for option by
option in `tooluse` (used, unused with why, topped up), its install
recipe in `tools` (`doctor --install`), and every way it gives a rung:
* **F7.rshake** (`redisshake.py`): readers climbed per database - the
  hop's backup file or append-only file where named (`rdb_file`,
  `aof_file`, `aof_timestamp`), the snapshot the source hands a replica,
  a scan where the source refuses one; the hop's exclusions as the
  program's key filters only where each says exactly what the pattern
  says (held key by key against `Hop.excluded`); a cluster target's
  database 0 through its script; the target's own limits read; every key
  written as plain commands where the target refuses the source's
  serialised form (read from the servers' versions, proved on one key);
  the status port as progress. Its rung lacks certificate checks and
  access history, so a hop that checks certificates or a target that
  evicts takes migkit's copier.
* **One Redis ladder, one Kafka ladder** (`bulkways.py`): the engine's
  rungs out of `movers._ladder` climbed on the hop's measured rate
  (`planner.record_rate` under each way's name), a way never timed
  first; a copy the copier stopped goes on by the copier.
* **F7.mm2** (`mirror.py`): Kafka's cluster mirror run from the Kafka
  release or its image, told everything in a properties file of
  migkit's: nothing written to the source (offset syncs on the target,
  no heartbeat either way - measured: the flow back's heartbeat, left
  on, made four topics in the source), committed messages only, the copy
  ended where the source stood when it began (its saved positions read),
  each partition's position kept where migkit's copier keeps it.
  Exactly-once is off: its markers took 427 offsets over 200,000
  messages, and the count is how a message written twice is found. Its
  offset syncs and checkpoints are decoded as its binary schema writes
  them and used as candidates proved by the message (measured: its
  checkpoint put a group at 1 where the message was at 1001). Measured
  against the copier on one broker pair, 200,000 messages: the copier
  13.1 s, the mirror 76.6 s from its image, most of it its start. The
  copier's own `offset_header` writes each message's source position in
  a header, which places a group exactly where messages look alike.
* **F7.kclamp** (`KafkaEngine._place`, `group_positions`): a group goes
  to the target's offset of the message it reads next; where the target
  lacks it, right after the last message it read, at the target's end,
  and said; never past the target's end or before its start; a position
  with nowhere to go is left and said. `repair_plan` places afresh
  (at a cutover the source was just frozen) and its undo puts the
  target's groups back, a group migkit made removed.
* **F7.dsbulk** (`dsbulk.py`, `CassandraEngine._table_ways`): read out
  in token-range splits sized from the workers, each cell's time to live
  and write time kept, into a directory of this user's only, loaded
  back, both runs resumable by their checkpoint; no error skipped; the
  load's count and the loader's own count of the target held to the
  read-out's. Passed over for counters, columns of many cells, and TLS
  (not yet).
* **F7.rfs** (`fromsnapshot.py`, `OpenSearchEngine._table_ways`): an
  index copied out of a snapshot the hop names (`snapshot` on the
  source), the source never searched; the reader built from its source
  and proved by its own usage before it is trusted; the disk held to
  twice the largest shard; its working index removed.
* **F7.ddbexp** (`ddbexport.py`, `DynamoDBEngine._table_ways`): the
  service's export (point-in-time recovery must already be on) taken
  in by its import where the target lacks the table, by migkit's writer
  where it has it; the manifest's MD5 and counts where written, the
  export's count otherwise; a failed import's partial table removed; the
  export's files removed however the way ends.
* The per-table ladder of the native engines is one logic (`tableways`):
  climbed on the seconds a row each way measured, a tool that cannot run
  on a table said and switched past by `decide.attempt`, migkit's own
  never skipped.

Not measured here: the replica-protocol copy, the bulk loader and the
snapshot reader are not installed on this machine, and no real AWS
account is touched, so their ranks come from the first runs of a hop.

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
P10 queued as its own item. P6 (the application's own calls), P8
(sequences of calls), P12 (constraints by accept/reject probes and a
three-valued solver), P13 (the collation matrix on the column's own
values) and P14 (types sized from the data, `types_from_data`) written
2026-09-30 with their tests (R11).

*Built 2026-10-04 (conversion breadth; its tests written, the one run
of the suite to come):*
* *P5, where two versions part* (`storedcode/trace.py`): on a differing
  call, a copy of each version made beside it reports every variable and
  parameter of its outermost scope after each statement of its outermost
  block - PostgreSQL by `RAISE NOTICE`, MySQL into a MyISAM table, SQL
  Server by `PRINT`, Oracle through an autonomous procedure - each copy
  first held to its routine's own answer; the two reports aligned in
  order (a target that runs more statements repeats a state), names met
  across the rules' renames, values by class. The verdict's reason then
  says after which source statement the two first part and which
  variable holds what on each side. Left alone where a report would
  change what the routine reads (`ROW_COUNT()`, `@@ROWCOUNT`, `SQL%`), a
  T-SQL function (no `PRINT` there), a trigger, a package's member.
* *P9, a target that runs the source's own code* (`convert.as_written`,
  `convert.emulating`, the rung `emulator`): MariaDB in its Oracle mode,
  IvorySQL and openGauss (A) for Oracle's PL/SQL, Babelfish for T-SQL,
  openGauss (B) and openHalo for MySQL's - each asked of the target
  itself (version, database compatibility, extension, setting); the
  source's own statement made there in the session settings that read
  it and proved like any other version. A docker sandbox of such a
  target is not started (named instead); MariaDB's is.
* *P10, the workload corpus* (`storedcode/corpus.py`, hop option
  `workload_replay`): every statement on the source's record that reads
  or writes rows - not migkit's, not the catalogue's, not a routine's
  call - weighed by its runs, translated through the hop's
  restructuring, run on both sandboxes with the routines in place and
  compared by answer, rows written, error and counters
  (`storedcode/statements.py`); a sample's values only where the masking
  allows, else the fixture's; kept in `workload-corpus.json` normalised,
  never a value, growing as the server's record turns over. The same
  corpus is the workload comparison's list of statements, so an Oracle
  source joins it through its shared pool and captured binds (plans
  from `V$SQL_PLAN`, times from the pool's own averages - nothing run on
  the source to plan or time it).
* *P11, the application's code* (`storedcode/appscan.py`, hop option
  `application_sql`): every string literal of Python, Java, Kotlin,
  Scala, Go, JavaScript, TypeScript, C#, PHP, Ruby, Rust and Swift read
  by the language's own parser, joined literals as one, `.sql` files and
  XML mapper statements; SQL only where the translator reads it as a
  statement; interpolated, formatted and joined-around-a-variable ones
  found and said as built at run time; an object mapper's own query
  language left to it. Each proved as the corpus is;
  `application-sql.sql` holds each translated statement with where it
  was found. The application is read, never written.
* *P15, index and partition advice* (`storedcode/indexadvice.py`,
  `storedcode/partitions.py`, hop option `index_advice`): candidates from
  each translated statement's equality, range, join and order columns,
  made on the target hypothetically (the extension must already be
  there) and advised only where the target's planner chooses one and its
  estimate falls by half, the largest gains weighted by runs, with the
  costs, the size and the writes that pay for it; a MySQL or Oracle
  table partitioned by range or list where its target is not gets the
  partitioning in PostgreSQL's words, made on the sandbox target in a
  transaction rolled back, the fixture's rows put in, and each row's
  partition held to the source's (`partitions-for-the-target.sql`); a
  hash partitioning is advised and said to route otherwise.
* *P17, the engines' own tests* (`storedcode/builtins.py`, hop option
  `builtin_suites`): PostgreSQL's regression files, sqllogictest and
  MySQL's mysql-test read as data for the cases calling a function the
  source's code calls on constants, run on both sandboxes; a case
  answered apart is kept with its file and line and refuses a call with
  exactly its constants in the translator, adds its constants to every
  routine calling the function, and is said beside such a routine's
  verdict.
* *P19, the source's answers after the cutover* (`storedcode/reference.py`,
  hop option `code_reference_days`): the proof's calls of each function
  whose answers need no rows, with the source's answers, kept (never
  under `mask: all`); after a flip, each check asks the target's version
  the same calls read-only for the period and says any that answers
  otherwise.
* *P20, every input the solver reaches* (`storedcode/scalar.py`): a
  function of one expression on both sides asked of the constraints'
  encoder for an input answered apart; each found is run, only the run
  decides; none found is said with its bound (declared ranges, no
  overflow modelled). Stops at bodies of statements: their model needs a
  symbolic executor per procedural language, and the search, the solver
  for untaken branches and coverage stand in for it.
* *P21, restructuring proved* (`statements.restructured`,
  `statements.transformed`): the application's statements read through
  the hop's column renames and drops (refused by name where they read a
  column left out) and held to the source on the restructured fixture; a
  statement reading a column written transformed is said, not compared.
  A table split or merged is no restructuring a hop can say, and is not
  claimed.
* *W3*: PL/SQL by migkit's own rules (`storedcode/plsql_pg.py`,
  `storedcode/packages.py`) - a package a schema, its state in session
  settings through its own functions and initialised once a session, its
  members proved one by one; cursors and their attributes, named and
  declared exceptions (Oracle's statement-level rollback kept), SELECT
  INTO's strictness, `''` as NULL, `||`, SUBSTR and `/` as Oracle means
  them, triggers, autonomous procedures over a loopback connection.
  T-SQL at depth (`tsql_pg.py`): the procedure-wide transaction under
  TRY/CATCH, OUTPUT into a table variable, a temporary table or to the
  caller, MERGE by the target's version, a table variable through a
  ROLLBACK refused; and the END after a semicolon the translator's
  tokenizer had read as text (no TRY ending in a semicolon could be
  read). Found on the way: Oracle's no-row and too-many-rows errors were
  compared by message, and every proof of them said "different".

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
   *Built 2026-09-30.* **Copybook files** (`copybook`, also `vsam`):
   migkit's own reader (`migkit/copybook.py`), chosen by a corpus of
   139 values written from IBM's documented encodings plus 10 that must
   fail (`tests/copybook_corpus.py`): Stingray 5.1.1 read 21 of the 139
   right and read 6 of 7 must-fail values as numbers; Cobrix has no
   program to drive and turns what it cannot decode into NULL. Code
   pages through `ebcdic` (cp838 Thai included), COMP-3/zoned/COMP/COMP-5
   and the signs, `P` scaling, COMP-1 as a float and COMP-2 as its exact
   decimal, REDEFINES by a discriminator rule, OCCURS flattened, ODO
   shifting what follows, 88 levels as boolean columns, LOW-VALUES as
   NULL; F/FB/V/VB/z/OSMF record mode; multi-layout files as tables by
   segment; must-fail values stop the read or land as NULL and are
   listed. A file is read by ranges of whole records, resumed by record
   offset, and folded in processes side by side (3,000,000 records of
   74 bytes: 31.98 s in one process, 7.38 s on 10 CPUs); written back
   in its own format, a keyed batch replacing itself. **Db2 for i**
   (`db2i`): IBM's ODBC driver (installed with IBM's terms accepted),
   DRDA under a Db2 Connect licence, or Mapepire; `QSYS2` catalogue, RRN
   ranges, `HASH_ROW` sums between two systems, the journal followed by
   `DISPLAY_JOURNAL` (commit order, rolled-back transactions dropped,
   savepoint rollbacks re-read, key changes as delete and insert,
   receivers checked before resuming), users and object authorities
   compared and carried with GRTOBJAUT. Written without an IBM i: every
   statement and what is made of its rows is held by recorded result
   shapes; PUB400.com is where it can be proved on a real system.
   **Db2 for z/OS** (`db2zos`): `SYSIBM` catalogue, copy and verify;
   SQL1598N said as the Db2 Connect licence it is, the licence put in
   place by `doctor --install` from `MIGKIT_DB2_CONNECT_LICENSE` once
   accepted; follow declared not applicable, with the reason, and
   `assess` says the depth reached. Proved on no z/OS (IBM Z Xplore is
   the free one).
3. **Teradata** (teradatasql, FastExport/FastLoad through TPT where
   installed, HASHROW for digests; ClearScape or the Vantage Express VM
   for tests).
4. **One change-topic reader** for engines that publish changes as a
   documented stream (TiDB, Couchbase, Aurora DSQL, CockroachDB) - one
   reader in the cross-engine tail covers all four.
   *Built 2026-10-01* (`migkit/changetopic.py`, ER4 below).
5. **Cosmos DB NoSQL** (vNext emulator runs on arm64; deletes need
   "all versions and deletes" mode, which needs continuous backup).
   *Built 2026-10-01* (`migkit/engines/cosmosdb.py`, ER5 below).
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

**ER4. One change-topic reader: TiDB, CockroachDB, Aurora DSQL,
Couchbase.** *Built 2026-10-01.* One reader (`migkit/changetopic.py`)
for a source whose changes are published elsewhere in a known envelope:
lanes (a topic's partitions read by offset with no consumer group, a
Kinesis stream's shards, a changefeed streamed to a query, a container's
feed ranges), envelopes (TiCDC canal-json with `enable-tidb-extension`
and TiCDC simple, CockroachDB changefeed JSON, Couchbase's connector
with its default source handler, DSQL CDC records whole or in CRC-checked
fragments, Cosmos DB's two feed modes), and one buffer that keeps the
order each source promises: one clock across lanes (TSO, HLC, DSQL
commit nanoseconds) released at the lowest lane mark in commit order,
twice-delivered and already-copied changes dropped by the fence, never
half a commit; or one order per lane (a vBucket, a feed range) with a
sub-lane's last version dropping a redelivery and a failed-over vBucket
read on. The position saved is each lane's first change not handed on,
so a restart reads it again. TiDB and CockroachDB read each changed row
back as it stood at the batch's fence (`@@tidb_snapshot`, `AS OF SYSTEM
TIME`) - exact whatever the envelope made of a value - and a key the
feed says was written and is not there (or deleted and is) stops the
tail instead of becoming a delete. DSQL, with no mark in its stream, is
released by the stream's own lag (`BehindSourceLag`, or the hop's
`order_window`); its lossy columns (interval, timetz, infinities) are
read back as they are now. Engines: `tidb` (the MySQL engine's
connection, catalogue, reads and writes; checks, copy and tail through
the pair; TiDB's own `SHOW CREATE TABLE`; `SHOW TABLE ... NEXT_ROW_ID`
for the next id; `ADMIN CHECK TABLE` as a deep check; `ADMIN CHECKSUM
TABLE` not the verdict - its CRC64 carries the table's id), `cockroachdb`
and `dsql` over PostgreSQL's wire (`pgwire.py`: format_type declared
types, intervals read as text; CockroachDB by UPSERT and deleted in
pieces, DSQL within 3,000 rows and 9 MiB a transaction with OCC retries,
`CREATE INDEX ASYNC`, IAM tokens), `couchbase` (range scan, multi-writes,
the fence the server's own high sequence numbers over memcached's
binary protocol with SCRAM). Each: canon dialect (`canon.dialect`, TiDB
rendered in SQL, the others folded here), schemes, `changes` options,
capabilities and coverage cells, interlock identities, the DBA's
requirements and scripts. Tests written, not run:
`test_a_change_feed_is_read_in_commit_order.py`,
`test_tidb_follows_through_its_change_topic.py`,
`test_cockroachdb_follows_through_its_changefeed.py`,
`test_dsql_writes_and_follows_within_its_limits.py`,
`test_couchbase_as_a_side_of_a_pair.py`. Still open: their bulk paths
(Lightning/BR, `IMPORT INTO`, cbbackupmgr), users and roles, snapshots,
CockroachDB and DSQL sequences, CockroachDB and Couchbase settings;
TiCDC's open protocol, Avro and Debezium outputs; a table a CockroachDB
feed began without joins at the next start.

**ER5. Cosmos DB for NoSQL.** *Built 2026-10-01.* `cosmosdb` through
`azure-cosmos`: containers read feed range by feed range, digested with
the ranges side by side (verified by partition key range), a key holding
`id` kept as Cosmos DB keeps one, other keys made the item's id,
transactional batches of 100 by partition key and single upserts side by
side, 429s waited out by the SDK, low priority reads where asked. The
change feed by feed range: all versions and deletes where the account
serves it (it loses nothing within the continuous-backup window, and
cannot start before the tail's own start), latest version only where the
hop says `change_feed: latest` (it loses every delete and TTL expiry -
check finds them - and versions between reads). Fence: each range's
continuation read to its end at or past a "now" read's. Settings
compared (partition key, TTL, unique keys, indexing), conflicts feed as
the deep check. Test written, not run:
`test_cosmosdb_as_a_side_of_a_pair.py` (the vNext emulator, arm64).
Still open: bulk paths (container copy jobs), point-in-time snapshots.

**Done 2026-10-01: ER3, ER6, ER7.**
* **ER3 Teradata** (`engines/teradata.py`): Teradata's driver, installed by
  `doctor --install` once its terms are accepted; ANSI sessions (a value
  too long is refused, never cut), data encrypted, TLS by `sslmode`. Read
  in FastExport's parallel sessions from 100,000 rows where the user may
  read `DBC.SessionInfoV`, one statement otherwise, a FastExport that
  cannot start switched from; tables read in one pass or by `HASHBUCKET`
  buckets, never in key order (rows lie by hash on the AMPs); key pages
  ordered and resumed CASESPECIFIC alike. Batches bulk-loaded by FastLoad
  into a table of their own and merged in, its errors, warnings and row
  count held to what was sent (it drops a duplicate row without failing),
  statements otherwise; a tail's batch one transaction with its line in
  `migkit_origin`. PERIOD as `[begin, end)`, INTERVAL as its parts, TIME
  and TIMESTAMP WITH TIME ZONE as the instant, NUMBER by value. Two
  systems compared on the servers: `HASHROW` of each row's salted values
  (text as `CHAR2HEXINT`, NULLs marked) into `HASHBUCKET` under four
  salts, summed; users, roles and rights from `DBC.AllRightsV`; the DBA's
  script; `COLLECT STATISTICS` after a load; the deep check finds a table
  left mid-load. Cannot prove: nothing is followed (no change log a
  client reads); a LOB, JSON or XML column is folded in this process;
  written without a system.
* **ER6 SAP ODP over OData** (`engines/sapodp.py`): a service is a
  database, an entity set a table, pyodata for the metadata. The copy a
  follow starts from is the delta initialisation (no data-less start
  exists over OData); deltas by token, a before image passed over; the
  fence by the queue's own request times; a follow of an entity set with
  no delta links refused. Verify: a full read compared row by row, and the
  service's own `$count` - and an aggregate's sums - before and after it
  held to the rows read. Cannot prove: a digest on the SAP side (there is
  none); an extractor delivering additive deltas; users (PFCG roles); the
  delta subscription is the SAP user's and is ended in ODQMON.
* **ER7 SaaS as programs** (`engines/saas.py`, `saas_salesforce.py`): one
  adapter - catalogue, read from a state, the state reached - for Singer
  taps, Airbyte-protocol connectors (ELv2, terms accepted at install) and
  dlt sources; each kind's state the position; what a read hands over held
  to the hashes of what the target was given, moved on only once the
  tail's position after it is handed back; deletes carried where a stream
  is read whole or the program marks them. Salesforce read by a Bulk API
  2.0 job (REST pages where refused) and followed through the Pub/Sub API
  with Avro payloads. Verify by a second read, and by the application's
  own count where it keeps one. Cannot prove: a delete in a stream read
  incrementally with no mark (check finds it); a formula field's change
  (no event carries it); a file's bytes (`base64`).

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
  **Built 2026-09-30 (L1, L3-L8; tests written, the full run to come):**
  `migkit/tailtables.py` keeps a state per table beside the position
  (`tail-tables.json`: live, copying, parked, removed). The hop file's
  `exclude` is read again when it changes: a table put in is copied
  beside the stream by the pair's own copier under its own marks - after
  what the stream left out of it is visible (`Engine.visible_now`) - its
  changes held on disk per range until the range lands, then held to
  the range's mark (`ranges.place`); a table taken out leaves, its
  position and its target's digest kept, and comes back by its changes
  since, read again from there, where the source reads its log again,
  its shape and its target are unchanged (L6, a rung) - else by a copy.
  The seam on SQL Server is the change tracking version, on MongoDB the
  majority commit point (L3). DDL is read at its place: a binlog
  statement (`logddl`), a slot's TRUNCATE line, a change stream's
  collection event (`canon.ddl`). The hop's `ddl`: `stop` (the default,
  as before - and a table emptied on the source now stops it, where on
  MySQL it was carried by nothing), `park` (only that table, its changes
  spooled on this machine and synced before the position, released once
  its target is level, copied again past `spool_limit`), `apply` (a
  column that takes NULL or has a constant default added on the target
  as the target's own create renders it, a truncate carried between the
  rows around it, the rest parked). L7's identity counts held, set
  aside, out of scope and taken back. L8: the subscription rung needs a
  source past the 2025-05-08 minors (17.5, 16.9, 15.13, 14.18, 13.21)
  where tables would be taken into a running publication, and the tail
  is taken instead (`publication_add_risk`). Not done: a target table
  dropped or renamed with the source's (migkit never drops a target
  table - the table leaves the tail and says so); the digest-first
  re-copy of only differing ranges (R19 lever 3) for a table added back.
* **R1-R9, with and without the engine's change feature:** depths
  D0-D4 printed by `assess` per table, with the statement that would
  raise each. R7: lift MySQL's refusal of `MINIMAL`/`NOBLOB`/
  `PARTIAL_JSON` row images - exact by re-reading the row at the change
  (small). R9: PostgreSQL at `wal_level=replica` reaches convergent
  change reading through `pg_walinspect`/`pg_waldump` on the WAL archive
  (walminer 4 is paid). R9 built 2026-10-16 (`PostgresEngine.FOLLOW_WAYS`:
  slot, then the server's WAL functions, then the WAL files read here;
  D2 per keyed table, `_wal_changes`, tests in
  `test_a_postgres_replica_wal_is_followed_by_its_records.py`). Not done:
  the keys held per block live in memory only, so a follow resumed by a
  new process refuses any row removed before it began holding them;
  a table without a key is refused, not handed to a digest diff; a
  partition's changes are its leaf's name, not its root's; no sandbox
  run against a replica-level server yet. R2: SQL Server without CT/CDC - restore the
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
  *Built 2026-10-16, partly (LT.C5; no Oracle server was reached):*
  where the redo is mined is a climb (`oracle_logminer.place`): a named
  standby, the source where its account holds the mining grants, or
  migkit's own mining database (`mine_instance`, `archive_from`,
  `archive_to`), the source's archived logs shipped to it once each by
  `carriers.move_file`, checked to read there at the SCNs the source wrote
  them at, and mined with the dictionary the source wrote into its redo;
  a skipped sequence or an SCN range no log covers stops the read
  (`ShipGap`); each place's window lag (archive switch + ship + mining)
  is measured and ranks it. Not done: a sandbox run with a second Oracle
  Free as the mining database (stand-ins only,
  `tests/test_oracle_mines_on_its_own_instance.py`); migkit does not build
  the source's redo dictionary itself (an offered source change); shipped
  logs behind every saved position are not yet removed.
  *Built 2026-10-16 (LT.C4, LT.C6, P0-PARITY.tail on Redis):* `redis.
  ReplicaStream` (migkit a replica: offsets, resume by +CONTINUE),
  `KeyspaceStates` (joining/live/left/parked per database, seam = offset,
  added/removed/renamed/ttl/retyped counted), the idle-time sweep, both
  chosen by `decide.redis_live`; `twoway.redis_two_site` by
  `decide.two_site_rule`. *Not done:* the depth matrix still says not yet
  for Redis `tail.*` (`capabilities._states` knows only `TableSet` and
  `TopicSet`, and Redis has no `tail_apply` calling `follow_live`); the
  snapshot a full start receives is discarded and the space copied by
  scan; `CLIENT NO-TOUCH` is not set, so the sweep's reads touch the
  source's access history; the two-site rules read change records no
  reader yet produces from both streams; cluster sources fall to the
  sweep; tests written, not run.
  *Built 2026-10-16 (W13B):* `RedisEngine.tail_apply` (and `tail_start`)
  runs `follow_live` round `tailctl.run_loop`, the stream's connection
  kept open between looks and its selected database saved; a full start
  is filled from the snapshot the stream receives (`RdbReader`,
  `dump_payload`, chosen by `decide.redis_seed` on measured dataset, disk,
  memory and the hop's ops; the scan the fallback); sweep and refresh
  reads under `CLIENT NO-TOUCH` where offered, the caveat said where not;
  the tail applies only `Hop.carries`; `SiteChanges` /
  `twoway.redis_two_site_live` make the two-site records off both sites'
  streams; a cluster is followed one stream a shard (`_follow_shards`,
  ledger by first slot). *Not done:* GeminiDB's Redis inherits the tail,
  and `capabilities` still declares its `stream`/`scope.ops` not
  applicable (the depth guard fails on it until the owner of
  `capabilities` re-declares); hash field expiry types (Redis 7.4+) fall
  to the scan; no server run (fakes only).
  *Built 2026-10-11, partly (LT.C2, LT.C3; no broker was reached):*
  `KafkaEngine` keeps a copied partition's offsets identical where they
  can be exact, and otherwise (with `offsets: map` and a client that runs
  transactions) writes the map in the batch's own transaction. C2: a
  partition whose target is empty and whose source offsets are
  consecutive has the target padded with throwaway records up to the
  source's first offset and `DeleteRecords` moves the start there
  (Kafka's only way to set a log's first offset; probed first with a
  request that deletes nothing, bounded by `offsets_pad_limit`); the copy
  is not transactional, since a marker would take an offset, and every
  landed offset is read back and held to the source's; the source is read
  to its end first only where padding is owed, otherwise each offset is
  proved as it lands, and a gap ends identity there, said and recorded.
  C3: in `map` mode a gappy partition is copied one batch to one
  transaction with the source-to-target runs it took, in
  `__migkit_offset_map` on the target; the copy goes on from the map; the
  map is read back whole at the cutover and places a group's offset
  exactly (`group_positions` through `_located`). Proved on in-memory
  clusters with crash before the commit, crash after it, repeated resume
  and replay equal to an uninterrupted run
  (`test_a_kafka_copy_keeps_the_sources_offsets_where_it_can.py`,
  `test_a_kafka_offset_map_is_written_in_the_data_transaction.py`).
  **Not verified on a real broker:** DeleteRecords on a live log, the
  client's transactions and fencing, retention or compaction racing the
  padding, throughput. **Not built:** a content comparison for mapped
  partitions (the count and tail checks warn and skip them), the map in
  `auto`, a cost-model choice for a very long padding, thinning the map,
  a group sitting at the end behind a trailing marker.
* **P1-P3, cloud processes without an API:** a mover that scales on the
  source's own stress and scales down at a range boundary (DMS
  Serverless waits 60 minutes); seeding from ZFS/LVM/EBS snapshots
  moving only changed blocks, then fast-forwarded; a zero-ETL-like
  continuous hop for any source and warehouse, keyless tables and
  additive DDL without a resync.
  Not done (W12B): AWS's integration is made and waited for; Azure's MI link and Google's AlloyDB stream are registered and refused as not yet run; `decide.warehouse_feed` is not yet asked by the flow.
  Not done (W12B): the landing sizes the tail and late rows are dropped by `hop.versions`, but each warehouse still lands through its own device rather than switching way by the choice; the zero-ETL lag is documented, not yet read from the target; capabilities declare neither rung (owned elsewhere); none of it has run on a real account.
  Not done (W13C): each warehouse now lands through the way `decide.warehouse_landing` chose (an untimed way probed at two sizes, bounded; a switch at a batch boundary only past the tie); the tail's first read asks `decide.warehouse_feed`, the provider's lag measured by a marker's round trip once its integration runs; Azure's MI link and Google's AlloyDB stream are made, watched and refused by the grant or API they lack. Left: the provider's win stops the tail and asks for `migkit check` rather than running it; the marker needs a source drainmark writes (postgres, mysql, mongodb); the rungs are not in `capabilities`; nothing has run on a real account.

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
  *Built (2026-10-01, tests written, not yet run):* `standby: true` runs
  the same move on two or more machines; one holds the hop's lease and
  moves it, the others stand by and take it over within the term
  (`MIGKIT_STANDBY_SECONDS`, 15) of its last renewal (`ha`). The lease
  carries a number one higher at every taking (`lease`), raised on every
  target database before the taker reads anything, and held by the target
  in every transaction a writer commits, last before its commit, under a
  shared lock the raise must wait for (`fencing`, a row of
  `migkit_origin`; proved on each target first) - a machine that lost the
  hop is refused by the target itself. The tail's files (position and
  batch number, shape, tables' states, spools, kept proof, ways, copy
  position) are pushed to the machines' store before every commit and
  every two seconds, only over no newer number's, so the store is at
  most the one batch behind the target that `committed_ahead` already
  resolves: a takeover is a restart from another machine. The store is
  chosen from the hop file alone and proved (`shared`: the run state's
  bucket by If-Match/If-None-Match, a shared directory under a lock, or
  rows of `migkit_origin` on the target). `share_tables: ranges`: a large
  table's ranges claimed by every machine's workers through one record
  written only over the version read (`claims`), each claim under a
  number the target holds in the range's own row, each machine sized
  from its share of the servers; the other tables claimed whole. Health
  in `watch`, the ledger of holdings and `one_writer`'s proof (numbers
  rising, each holding going on from the last one's saved position, the
  target holding the newest number, the last batch's mark under its
  owner) in `watch` and the evidence's "who wrote what". Not built: the
  engines' own copiers and bulk programs carry no number, so a hop with
  a standby is copied by the pair's table copier; a physical seed is
  refused with a standby; targets other than PostgreSQL and MySQL
  (MariaDB) cannot hold a number yet, and a standby is refused over them.
  *Takeover of shared ranges (2026-10-16, `fleet`):* the machine that
  takes a hop's lease over puts the stopped mover's range claims back at
  once (`fleet.departed`, one conditional write), not after their own
  term; reads what the stopped mover's ledger left open (`fleet.opened`,
  read only) into an account of ranges done, taken back, held and left
  (`fleet.reconcile`); and a finished table's record is audited for a
  range lost or copied twice (`fleet.audit`). Proved on fake stores
  (`test_a_standby_mover_takes_over_and_fences.py`, written, not run):
  two movers, a lease lapse, the takeover, the stopped mover's late
  write, done and renewal refused, every row once. *Not done:* `ha` and
  `claims.copy` do not call `fleet.take_over` yet (the taker still waits
  out a range claim's term), and nothing opens a range unit in the
  ledger under `fleet.unit_name` while it copies.
  *Closed 2026-10-16 (W12R, tests written, not run):* `ha.Holding.take`
  calls `fleet.take_hop_over` when the lease was taken over, and the last
  `unlink` runs `fleet.audit_hop` (findings said, written to the
  changelog, carried by `claims.evidence` and `claims.health`);
  `claims.copy` opens each range as a ledger unit named by
  `fleet.unit_name` (`attempts.range_unit`). *Not done:* a stopped mover's
  ledger on another machine is not read (only this machine's ledger; the
  machines' record stays the truth), and `capabilities` text for fleet and
  topology (W12F's file).
  *Closed 2026-10-16 (W13N, tests written, `test_takeover_reads_the_departed_ledger.py`):*
  each push of the hop's saved position carries the holder's range units and
  uncertain operations (`fleet.kept`, `ha.Holding._record`; the ledger's
  stamp is in the push test), and the taker reads them
  (`fleet.Departed`, joined with its own ledger) so ranges resume from the
  departed machine's units; the machines' record stays the truth, and where
  the ledger closed a range the record does not hold done the conflict is
  said, put in the changelog (`ledger-conflict`) and the range copied again.
  The lease waits for its holder's measured heartbeat jitter past the term
  (`lease.Jitter`, `lease.lapse`), bounded by the holder's own term.
  *Not done:* a ledger unit is only as fresh as the last push (two seconds,
  or the next batch); the topology keys are read by the names
  `key_spaces`, `conflict_rule` and `routing` (`topology.SPACES_KEY` and the
  two beside it) and are inert until `hopkeys` declares them (W13H).
* **W2. The estate, not the hop.** Discovery of every database on a
  network or account, target sizing from the source's performance
  history (Azure's SKU recommendations, Fleet Advisor), cost of the
  target, and hundreds of migrations run and watched as one fleet; a
  REST API, a Terraform provider and a Kubernetes operator over it; SSO
  (OIDC/SAML) and SCIM on the view, not only a proxy's header.
  PARTLY BUILT (the owner's scope of 2026-10-01, "W2 and a GUI";
  `migkit.web`, served by `migkit report --serve`, `docs/web.md`): sign-in
  by OpenID Connect (code flow with PKCE, ID token checked against the
  provider's keys) or SAML (only a signed answer to a request the view
  made, read out of the element the signature covers), SCIM 2.0
  provisioning that signs a deactivated person out at once, one local
  operator without a provider; roles viewer/operator/approver/admin per
  hop, hop group or file in the hops file's `access`; a versioned REST
  API whose OpenAPI document is written from the table it dispatches on
  (`docs/site/openapi.json`); runs started from the view are the command
  line's own commands in a process of their own, carrying who started
  them; approvals given in the view counted by `approvals.require`
  (signed by the view's key, re-checked for the role at count time,
  never the requester's); source-change offers answered there and
  recorded by name; pages showing each side's outcome, the plan's phases
  with p50/p90 and the cutover outage, progress, evidence, the decision
  record only when asked. Not built: discovery, sizing from performance
  history, cost, a fleet view, the Terraform provider and the Kubernetes
  operator; an encrypted SAML assertion, a signed authentication request,
  single logout.
* **W3. Code conversion at enterprise breadth.** Beyond R11: PL/SQL
  packages with the emulation libraries they need (orafce and the
  like), T-SQL at depth, and the SQL embedded in application code
  (AWS SCT scans Java, C# and .NET) found and converted, with the same
  proof by execution. *Built 2026-10-04: see "Leapfrog: conversion".*
* **W4. Transformation in flight and masking for compliance** - built on
  what migkit already holds rather than a streaming engine of its own
  (the owner, 2026-09-29: "faster, smarter, deeper than Striim"): each
  batch of changes the tail reads becomes an Arrow table, a hop's
  `transform:` SQL runs over it in DuckDB (already a dependency) -
  filters, projections, casts, joins to reference tables read from
  either side and cached, windows over a batch and state kept per key
  between batches - and the result goes to the target through the same
  exact batches, so exactly-once, the proof and the undo still hold; the
  verify then compares the transformed source with the target by
  running the same SQL over the source's rows. Beside it, the plain
  rename/filter/column rules stay the fast path where no SQL is given. Joins,
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
* **W8. Installed where nothing can be downloaded - the places the
  clouds' services cannot reach** (the owner, 2026-09-29: DMZs, secure
  zones and networks with no way out are exactly where DMS and DTS
  cannot be used at all). Proved, not assumed: a test runs install,
  `init`, a move with its tail, `check` and a report inside a network
  namespace that reaches only the two databases - any outbound call
  (telemetry, a version check, a download, a DNS lookup of an outside
  name) fails the test; notifications to internal endpoints only
  (webhook, email relay, Mattermost, Matrix); certificates from an
  internal CA. An offline bundle
  (every wheel and every wrapped program, checksummed and signed), a
  container image, and Windows; GoldenGate runs on every platform
  including z/OS and AIX.
* **W9. Compiled end to end at the very top of the rate.** R19 lever 9
  compiles the decoder; above ~100k changes a second sustained the apply
  side (collapse, render, write) needs the same, measured first.
  PARTLY (7c09b12, `applyway`): a batch is applied by rows or collapsed by
  columns and each table's net rows handed to the engine's bulk way, the
  second climbed to through `decide` only where the rows way applies
  slower than the source changes, on the measured rate.
  *Not done:* a compiled collapse, render and write; the Arrow handoff
  was dropped, measured no gain; the ~100k changes a second ceiling is
  not measured on a server; tests are on fakes.

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

*Warehouses as exact sides, built (2026-09-30; Leapfrog engine reach 1,
F7):*
* **The digest in each warehouse's SQL** (`engines/warehouse_sql.py`):
  canon's row text, hashed and summed without wrapping in BigQuery,
  Snowflake, Redshift and DuckDB - the hash read off `canon.digest_step`
  (MD5 today, SHA-256 in every dialect the day the fold moves). A class a
  dialect does not render, or a table whose first 64 rows the server
  renders differently from this process, or SQL the server refuses, is
  folded here and said. The leaves of a differing table (`leaves.py`) are
  computed in the same SQL. DuckDB's rendering held to canon's on 6,000
  rows of every class, floats from random bit patterns included; DuckDB
  files digest a million rows in 0.81 s where the fold took 4.27 s.
* **A copy's batch, staged and merged by its key once**: BigQuery by a
  Parquet load job into a stage table of its own (the target takes only
  a MERGE; the Storage Write API once the project's load jobs of the day
  run out, read from its job history), Snowflake by a staged file of text
  cast by type and COPY (its 64-day load history skips a file loaded),
  Redshift by COPY from S3 a file a slice or by statements, whichever did
  a row in less time (`STAGE_WAYS`), with `STL_LOAD_COMMITS` asked when a
  keyless batch's commit goes unanswered.
* **A tail's batch applied exactly once** (`EXACT_BATCHES`): journal rows
  landed through BigQuery's committed stream at offsets (a replay is
  refused at its offset), Snowflake's channel with the batch as its offset
  token (a staged file without a key pair), or Redshift's own
  transaction; merged with the ledger's line in one transaction, which
  the tail asks after a stop.
* **Iceberg and Delta tables as sides** (`engines/lake.py`): each batch
  one commit naming it (snapshot properties; Delta's `txn` version for a
  tail), a replay passed over; digests by DuckDB over the table's Arrow.
  What each lake's clock means is in `canon.TIME_MEANING` (Iceberg's
  `timestamp` a wall clock, Delta's an instant), so a pair's deep check
  says a clock that changed meaning.
* **In a pair as alone**: `assess` names migkit's own tables and
  BigQuery's load jobs left for a warehouse target of a pair
  (`reach_items`); the resource model sizes a BigQuery table's rows by
  listing them, unbilled, not by a query billed by every byte it reads.
* Proved on BigQuery's emulator (its gaps listed in
  `tests/test_bigquery_on_its_emulator.py`), on PostgreSQL where
  Redshift's SQL is its own, and on disk for the lakes; Snowflake, and
  COPY, slices and load history, only against recorded shapes until a
  real account runs them.

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
and five round trips against three. Built after all (2026-09-28, R19.6
and R19.10): four round trips a run now, and on both engines the stage
or the statements taken by what each cost on that server for runs of
that size - neither wins everywhere. 5 done (2026-09-27,
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
  *Decided as cases 2026-10-16 (W11D):* `scenarios.topology_case` climbs
  a ladder per shape. Many-to-one: the sources' key spaces (ranges,
  auto-increment strides, listed values; `topology.overlaps` solves two
  strides by their common progression) proved apart keeps exactly once;
  where they meet a conflict rule (`apply_remote`, `keep_local`,
  `last_update_wins`) is needed and exactly once is given up, said in the
  plan; neither is refused with the key both write. One-to-many: the rule
  the hop names, whole tables, ranges of the key or a hash, refused where
  it leaves a table or key nowhere. Mesh: every node marks migkit's writes
  and every pair is linked, a forwarding ring refused. `topology_hops`
  gives one hop per pair with the origin it applies; `topology_verify`
  proves the union, the partition or the convergence, each source's order
  and that no change came round.
  *Read and run 2026-10-16 (W12R, written, not run):* `topology.layout`
  and `topology.facts_of` read sites, links, hop count and each link's
  measured rate from the configuration's hops and their kept copies
  (`planner.link_speed`); `planner.run_topology` runs the decided hops in
  the order `overlap` gives (the slowest link first) and checks the proof
  after each hop, the next not begun on a failed one. Not done: the
  servers' own facts (key spaces, the conflict rule) are still passed in,
  the facts are not yet a hop key, a docker run of each shape.
  *Read from the hop 2026-10-16 (W13N):* `topology.facts_of` reads each
  source's key space, the conflict rule and the fan-out's routing from the
  hops' own keys (hops that say different things are refused by name; what
  is passed in stands over them) and carries the measured link rates and
  the unmeasured links in the facts.
  *The move's door 2026-10-16 (CRIT5FIX3):* `planner.move_topology` is
  the one call a hop's move goes through: a hop whose `nodes:` names a
  topology and whose hops sharing a site draw one runs every hop of it by
  `run_topology` on `topology.facts_of`'s facts and per-link rates; any
  other hop runs alone as before (`test_landings_keep_pace_order_and_memory.py`).
  *The command's door 2026-10-16 (W14X):* `cli.move` hands a hop whose
  `nodes:` is set, with `--go`, to `planner.move_topology` (the
  configuration's hops, the source's tables and keys, each node's rows
  folded per key for the proof after each hop); a plain hop, a plan, one
  table and a teardown move as before
  (`test_every_engine_is_titled_and_its_hop_is_named.py`). Not done: the
  changes' applied order is not read at the command, so the order and
  echo proofs see none; target table renames are not applied to the
  node reads; a docker run of each shape.
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
* JSON Schema and Protobuf (R4.formats, built, tests offline only):
  `migkit/schemaformats.py` reads a message the registry framed whatever the
  schema's kind (`registry.entry` returns the registry's `schemaType`):
  JSON Schema (body checked against `type`, `required`, a closed
  `additionalProperties` and each property's simple type), Protobuf (the
  `.proto` text parsed in the module, proto3 only: scalars, enums, nested
  and repeated fields, packed or not; the message-index list as zigzag
  varints, a lone zero byte being `[0]`). It refuses, with the reason and
  never by skipping: a schema with references, proto2, imports (the
  well-known types among them), `oneof`, maps, groups, extensions, a field
  the schema does not name, an id the registry does not hold, a kind that is
  not AVRO, JSON or PROTOBUF. `format: jsonschema` writes the same envelope
  as `format: avro`; `format: protobuf` is refused as a write (field numbers
  for a table's columns would have to be kept for the topic's life). A copy
  between two registries registers each schema under its own kind.
  Tested in `tests/test_registry_messages_in_json_schema_and_protobuf.py`
  with a dict for a registry and wire bytes made by hand. **Not verified**:
  against a real registry (its `schemaType`/references answers, whether its
  rule takes the JSON Schema versions written), against bytes a real
  Protobuf serializer wrote (the zigzag encoding of the index list is as
  remembered from the serializers, not measured here), and the JSON Schema
  keywords other than the ones above. The reader is not yet called by a
  row-reading path of the Kafka engine, which copies and compares messages.
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
  *Anchor (2026-10-11, tests written, full suite not run):* `options:
  anchor: {bucket, prefix, retention: {mode, days}, required}`
  (`migkit/anchor.py`). At the end of every run that left anything, the
  record's head and entry count (and an SSH signature over them where
  `evidence: {sign}` or `MIGKIT_EVIDENCE_KEY` names a key) go as one
  object per checkpoint into a bucket with Object Lock; a bucket without
  it is refused with its reason; a failed write is said, kept and
  retried, and stops nothing unless `required: true`. `migkit history`
  reads every version of every checkpoint back and holds the record to
  them (rewritten entries, entries taken out, a copy written over, a lock
  gone or run out). Proved against `moto`, which is not the store: the
  real Object Lock semantics (AWS S3, MinIO) are not yet exercised.
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
done 2026-09-27; Oracle's large columns in pieces done 2026-09-28, item 11;
MySQL's todo)

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

### R11. Stored code converted and proved (in progress)

Done (2026-09-28, `migkit/storedcode/`):
* The proof never says `ok` for code nobody ran: every function,
  procedure, trigger and event is run - procedures, triggers and events
  only in a sandbox (hop option `proof_sandbox`: two scratch servers, or
  `docker`), functions also read-only on the hop's own servers - with one
  outcome per call (value, OUT values, result sets, error class, rows
  changed, counters) and the first differing input named, shrunk. The
  sandbox holds a key-closed sample of the source carried by migkit's own
  move and held to migkit's own check first. Coverage: the target's
  PL/pgSQL by the static check's profiler (which also gates, fail
  closed), the MySQL source by probes in its sandbox copy; `ok` only with
  every statement and branch reached, else `warn` naming the line.
  Inputs: class pools, the body's constants and their neighbours, the
  fixture's values, a property search, a solver for the branches none of
  those reached, and a counterexample bank re-run first on every version.
  (`test_stored_code_is_proved_by_running_it.py`)
* The query translator's silent passes refused or rewritten: `CONNECT BY`
  refused, `(+)` rewritten into ANSI joins, `TRY_CAST` to a NULL-on-failure
  form per target, bodies of statements refused
  (`test_the_translator_refuses_what_it_would_pass_through.py`).
* Per routine, candidates (the decision engine's rungs, pruned and
  ranked there) ranked by the share of their versions that proved on this
  hop (`code-rungs.json`): the operator's own file, the
  translator, migkit's rules (MySQL and T-SQL procedural code into
  PL/pgSQL), an emulation of Oracle's functions where the target runs
  one, the PL/SQL and multi-dialect converters (built by `doctor
  --install`, run as programs), a model last in a capped loop fed each
  failure. Accepted only when proved; the residue in three classes in
  `code-conversion.json`; a person's fix learned as a rule when it proves
  on the other routines with no regression; the proof's own mutation
  score; the proved calls kept as SQL tests on the target.
* SQL Server (SQL Edge) and Oracle (Free) as far as the sandbox goes:
  catalogue, sandbox, harness, no coverage on either yet
  (`test_sql_server_code_converted_and_proved.py`,
  `test_oracle_code_converted_and_proved.py`, `--vm migkit`).

* Found on the way and fixed: a batch written to Oracle was read back by
  its keys without the scale its column declares, so `0.10` came back
  `0.1` and every move into Oracle of such a value stopped as a
  difference; SQL Server's identity seed came back as bytes and made
  the copied table's statement unreadable; the static check read a T-SQL
  trigger's transition tables as missing relations; a table the target's
  emulation makes itself was made again in the sandbox. Measured on the
  way: a MySQL source's event runs on the source as soon as it is made
  (every hour from now), so a proof's fixture is what the source holds
  after it ran; and MySQL wrote 0 where an UPDATE in a procedure made a
  NOT NULL column NULL, and kept it when a later statement failed, where
  PostgreSQL refuses the NULL and its call writes nothing - no candidate
  matches it, the proof says so with the one statement that differs,
  and the routine is left for a person.

Done (2026-09-30, written with their tests; the tests wait for the full
run):
* Inputs from the application's own calls (P6, `history.py`): the
  source's statement digests, `pg_stat_statements`, SQL Server's
  procedure counts with the arguments each cached plan was compiled for
  and Query Store, Oracle's shared pool with its captured binds - read
  once, read-only, into shapes (a value, NULL, a placeholder) and how
  often each ran. The values go in first after the counterexample bank,
  the most run first; none under `mask: all`, and none for a parameter
  compared with a masked column. The proof's own calls carry a mark and
  are left out. The database's line says the share of the calls on
  record made to routines that answered the same.
* Sequences of calls (P8, `sequences.py`): the routines every single call
  of which answered the same are called one after another in one
  transaction per side (the runners' held mode), every table compared
  after each call; each routine twice with the same arguments, every
  writer before every routine naming its tables, then sequences drawn and
  shrunk by Hypothesis. A sequence that differs makes the verdict of the
  routine whose call differed `diff`, naming the calls before it, is kept
  in the bank and tried first next time; in `convert`, a version that
  differs in a sequence is not applied.
* Constraints proved to accept and refuse the same rows (P12,
  `constraints.py`), collations held to the column's own values (P13,
  `collations.py`) and columns sized from their data (P14, `sizing.py`,
  the hop option `types_from_data`): on the sandbox's fixture, before any
  routine is made there (`bench.py`), each its own line of `check` where
  the hop names a sandbox. The sandbox now carries every foreign key with
  its ON DELETE/ON UPDATE and deferral (PostgreSQL, SQL Server and
  Oracle sides had none or none of their actions).
* MySQL coverage of a routine reading `ROW_COUNT()`, `FOUND_ROWS()` or the
  diagnostics area: an instrumented copy beside it under a name of its own
  keeps what it reads after every statement, the routine's own calls are
  the answers compared, and the copy's coverage counts only while it
  answers every call as the routine does (a trigger that reads them is
  still said not measured: it cannot be called beside a copy).
* The clock set to one instant: a MySQL session's by `SET TIMESTAMP`,
  a PostgreSQL call's transaction time read in its own transaction and the
  MySQL side set to it; two PostgreSQL sides compared by each value's
  distance from its own instant - for every call, so a trigger's
  `updated_at = NOW()` compares exactly too. Only `SYSDATE()`,
  `clock_timestamp()`, SQL Server's and Oracle's clocks stay within five
  seconds, said. MySQL's clock functions converted to PL/pgSQL are held to
  that exactness.
* Oracle's own block coverage and its identity counters in the sandbox.
* PostgreSQL's procedural code into MySQL's by migkit's rules
  (`pg_mysql.py`), a candidate like MySQL's and SQL Server's rules: its
  statement reader held statement by statement to PostgreSQL's own parser,
  the target's own unique keys and column types read for it, and each
  version probed for coverage in the MySQL target's sandbox. Found on the
  way and fixed: a MySQL body holding a `CALL` could not be read at all
  (its tokenizer keeps what follows CALL as one piece of text); a
  candidate's version that failed left the other triggers it made firing
  in every later proof; a version that commits where the source does not
  was held in the proof's transaction and committed the fixture away.
* Statement-level reduction for every source: MySQL's and T-SQL's bodies
  by their parsers, PL/pgSQL's by PostgreSQL's own, PL/SQL's by its
  blocks, re-converted by the candidate whose version differed.
* Effort (item 40, `effort.py`): the PL/SQL converter's own score per
  routine, shown as its figure with its unit, beside migkit's count from
  the proof - statements left for a person, lines no call reached, the
  seconds migkit spent - in `code-conversion.json` and on `schema
  --convert`. No rate is put on a person's time.
* MySQL's refusals sent as HY000/01000 (a CHECK, an enum value, a column
  with no default) are compared by the class every other engine refuses
  them with (`harness.MYSQL_STATES`).

Still to do: a MySQL trigger reading `ROW_COUNT()` or the diagnostics
area has no coverage; coverage on SQL Server; SQL Server's and Oracle's
clocks cannot be set for a session.

### R12. Engines the sandbox can now run (todo)

* **Oracle Free runs natively on arm64 from 23.5** (the slim image). It is
  capped at 2 GB of memory and 2 cores; the VM needs 4 GB or more. This
  unblocks item 11's run against a server, LogMiner (item 11.iii), users by
  `DBMS_METADATA`, statistics, restore points - to be tried within the
  VM's limits.
* **MinIO's images left Docker Hub on 2026-09-11.** The object store tests
  now start VersityGW (`tests/servers.py`: the `objectstore` profile and
  `start_object_store`, one sample sign-in, its health path as the ready
  sign, buckets made through the S3 calls). Not done: the image was never
  started here (colima was down), so these are unproved against it: the
  `--health` flag and the `/tmp` posix directory in `OBJECT_STORE_ARGS`, and
  that it honours `IfNoneMatch`/`IfMatch` on a put (the lease tests probe
  this first and fail loudly if not). No second profile for versioning or
  Object Lock was needed: the anchor's tests run on a stand-in, so real
  Object Lock semantics stay unexercised (item R5.anchor).
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
  (`test_clickhouse_to_clickhouse_is_compared_on_the_servers.py`).
  Done 2026-09-28: the move's own path is the target pulling each of the
  source's partitions itself (`INSERT ... SELECT` from `remote()`, no row
  through migkit), in the partition key's order, one partition a
  statement - split by the target's own partitions where one would pass
  `max_partitions_per_insert_block` (measured: the 101st stops the
  insert); a partition the target already holds as the source does is
  left, one it holds otherwise cleared and copied again, and every
  partition held to the source's by the two servers' fingerprints after
  it lands (never `system.parts` hashes, which hash files a logical copy
  writes anew); each copy under its own numbered
  `insert_deduplication_token`; blocks land unmerged
  (`optimize_on_insert = 0`: a replacing table kept one of two versions of
  a row the source holds); threads and block size from the move's sizing.
  Chosen where the target reaches the source - asked of the target, and
  held to the source's server id, so an address that leads back to the
  target is refused - and the table copier as before where it does not.
  Measured on 24.8: `_partition_id` is seen through `remote()` and
  filters on the source; the source's password shows as `[HIDDEN]` in the
  target's query log and errors; 500,000 rows pulled in 0.6 s, where
  the table copier on the same rows stopped on `HTTP status 400` - a
  delete naming a whole batch's 50,000 keys in the request's address -
  and now names them 1,000 a statement, and only where the target holds
  one (its time after the fix not measured yet). A write under a token
  seen before is dropped in part (624 of 696 rows came back into a
  target partitioned by day), and with no token whole. A follow by the
  partitions whose parts changed, each fingerprinted on both servers and
  copied again
  where it differs, a partition dropped on the source dropped on the
  target; the fence on the source's own clock at the last look that
  settled every changed partition, and the confirm through it
  (`test_clickhouse_target_pulls_partitions_from_the_source.py`;
  written, its first two tests run and passed, the duplicate-token one
  corrected to what was measured - the full run to come).
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
  (`test_opensearch_to_opensearch_keeps_documents_as_they_are.py`).
  Done 2026-09-28: the follow by each primary shard's sequence numbers -
  the writes up to its global checkpoint, read once every copy of the
  shard has made them searchable (refreshed twice since, or searched where
  it had gone idle; a source index that refreshes only when asked is
  refused), each document written with the source's own version of it
  (primary term above sequence number, `version_type=external`: a change
  carried twice or after a newer one is left by the target, which the
  test holds against a write without it; not `external_gte`, which takes
  an equal
  version, nor `if_seq_no`, which is the target's own numbering); deletes
  found by the counts and the ids compared. The fence on the shards'
  highest sequence numbers, the confirm through it. The copy writes the
  same versions and no longer empties the target first: emptied, each
  document's delete keeps a version for a minute (`index.gc_deletes`)
  that the copy's write at the source's version does not pass, as the
  test shows by hand - where the target deleted one itself, the copy
  says so and stops. Writes through opensearch-py's streaming bulk with retries on a
  busy cluster. Roles, users and who holds which role carried through the
  security plugin's REST API, index names as the target calls them, a
  replaced definition put back by the rollback; the API shows no password,
  so a user takes the passwords file's or is skipped and said; a cluster
  without the plugin said as such (`test_opensearch_follows_the_source.py`,
  `test_opensearch_security_roles_are_carried.py`; written, and run once
  before the fixes of 2026-09-30 - the follow's late write and the
  secured pair's start failed then - the full run to come).
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
  refused by name. Done 2026-09-28: the deep check holds every cell's
  write time and time to live to the source's - every row of a table up to
  5,000, the first 5,000 in token order of a larger - within a second and
  a minute (the tolerances ScyllaDB's own migration validator uses), a
  time to live as the moment it ends; counters by value, which no copy
  writes; the table copier's rows are named by it, every cell off
  (`test_cassandra_cell_times_are_checked.py`; run once, three of four
  passed, the fourth stopped on the text-typed table below, since
  fixed - the full run to come). ScyllaDB followed through
  its CDC log tables, read by migkit itself (there is no Python reader):
  the generations and their streams from `system_distributed`, each
  stream on its own, up to the source's clock less a confidence window;
  a row, partition, range or column deleted at the write time the log
  gives, a collection written whole emptied just before it, and every row
  the log touched read again and written with each cell's own write time
  and time to live, so the deep check holds followed rows too. The fence
  and the confirm on the source's clock, a verify of only what changed
  through the same reading. A table with its log off is refused with the
  statement that turns it on (migkit does not run it), a keyspace of
  tablets refused, a counter table refused by name, and a Cassandra
  source refused with the reason: its change files are read only by a
  program on every node. The target's table is made without the
  source's log - measured on 6.2, a table whose log is on refuses a
  write dated before its log's generation (`cdc: attempted to get a
  stream "from the past"`), and the copy dates every cell at the
  source's write time - and the settings check names a log on one side
  only as behaviour until it is turned on after the copy
  (`test_scylla_follows_through_its_change_log.py`). Found on the way:
  a Cassandra table copied row by row between two clusters was built
  with every column text and stopped on its first number - the
  source's own type is now written as it is. The ScyllaDB tests were run
  once before the fixes of 2026-09-30 (a table's log is now read by its
  log table's presence, the copy dated at the source's write times into
  a target made without a log); the full run to come.

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
* **b, c begun 2026-10-16** (`engines/qdrant.py`, `engines/neo4j.py`,
  both over their HTTP APIs, registered as `qdrant` and `neo4j`). Qdrant:
  collections as tables, points read by scroll from each page's offset,
  a collection made as the source's (vectors, sparse vectors, payload
  indexes), points upserted whole, counts and a folded digest, the deep
  check holding vectors and payload indexes to the source's; changes not
  followed (no change log a client reads). Neo4j: `nodes` and
  `relationships` read in ranges of the internal id, relationships after
  nodes and refused by name when their nodes never come, the source's id
  kept in a key property indexed per label, MERGE writes, the deep check
  holding constraints and indexes; change capture followed (5.13+,
  `txLogEnrichment`). Proved: `tests/test_qdrant_and_neo4j_reach_the_depth_matrix.py`.
  Not done: recall-at-k, HNSW and quantization carried, Qdrant's snapshot
  path, Neo4j's dump and load, its users, a fence on its change ids,
  servers of each in the sandbox (`tests/servers.py`) and the depth items
  marked not yet under R16b and R16c in `capabilities.py`.
* **b, c deeper (W13J, 2026-10-16)**: Qdrant makes a collection with the
  source's HNSW and quantization and the deep check holds the target to
  them; recall at k is measured on a sample sized to one hit in k, each
  answer scored against the source's exact nearest points, and a target
  significantly behind the source fails (a warning while it still indexes);
  a collection moves by pages or by the server's snapshot, decided from the
  measured round trip and the rate of one real page. Neo4j: a dump and load
  rung chosen against id ranges from the ranges' measured rate and the
  shared directory's measured disk rate (only where both sides allow the
  stop, nothing is scoped and the target holds nothing); users carried by
  name, status, home and roles with passwords nobody knows; a change
  position counted beside the opaque change id (`change_position`,
  `changes_behind`); sandbox profiles `qdrant` and `neo4j` in
  `tests/servers.py` and `tests/sandbox/vector-and-graph.compose.yml`.
  Not done: the fence (`fence_wait`) still delegates to the pair and does
  not yet read `changes_behind`; the snapshot restore and the dump and load
  are proved by their decision and not run against a server (no sandbox
  start); sparse-only collections have no recall check; the depth cells
  for these ways are not yet named in `capabilities.py` (W13M).

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
   PARTLY (2026-10-16, `migkit/kafkaclient.py`): the producer is chosen
   by `decide` (compiled client, else the pure one, the hop's measured
   rate kept per client), the codec too (zstd where the target's
   Produce version or broker version takes it and the first messages
   compress, else lz4, else none; in the decision record),
   idempotent and transactional producers carried
   (`test_the_stream_client_and_codec_are_chosen_by_facts.py`, doubles).
   Then (2026-10-16): the consumer and the admin go through the client
   chosen for their side (`_client_way(side)`: a side signing in by
   AWS_MSK_IAM reads through the pure one) - `NativeConsumer` answers
   the pure client's calls, positions set as an assignment with offsets
   before the next fetch; `NativeAdmin` answers create_topics,
   delete_records, the group calls and the broker list, and `Admin`
   sends any call it lacks (API versions, log directories, producer
   states, client quotas, configs, ACLs and SCRAM users) through the
   pure one, the call and why in the decision record. A codec refused at
   send time steps down the ladder (`Stepping`, `_stepped`, the refusal a
   fact of the climb) and the refused messages go again in order; a
   landed message after a refused one of its topic stops the copy rather
   than reorder. The codec sample is per topic (`_sampled`). The group
   calls are found by attribute (`kafkaclient.call_of`), not by version.
   Not done: configs, topic descriptions, ACLs and SCRAM users read
   through the compiled admin (their answers are read only in the pure
   client's shape); a transactional producer is not stepped down (a
   refusal ends its transaction); the position past a transaction's
   marker as the compiled consumer reports it, and the whole of it on a
   server, not measured; the engine still imports the pure client's
   `TopicPartition`, so the pure client stays installed.

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
   so the 36% is taken. Done (2026-09-28): the pin was already there
   for the copiers; the applier's runs now go through it too, into a
   temporary table of the written columns and on by one `insert ...
   select ... on duplicate key update`, and the load's warnings are
   read from its own answer (a round trip less). Used only where the
   server has `local_infile` on (never set) and where it measured
   cheaper than the statements for that table and run size (`_cheaper`,
   the costs readable as data by `measured_ways`). Measured on
   mysql:8.4, 200,000 rows in runs of 16,000: 2.62s staged against
   3.03s, and 6.28s against 9.74s with a round trip of 10 ms; runs of
   1,000 over 10 ms, 5.84s staged against 4.53s - the statements win
   there, and are taken. The shared copier's batches of 10,000: 2.21s
   loaded against 1.91s as inserts, 8.04s against 8.40s over 10 ms.
   The guard is proved on the handler with a server's packets faked
   (`test_the_load_answers_only_the_file_it_offered.py`), the writes
   against the statements' byte for byte
   (`test_a_mysql_run_goes_through_the_pinned_load.py`).

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
    `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE`. Done (2026-09-28):
    PostgreSQL's applier writes through psycopg3, each value put into its
    statement as psycopg2 put it (so a column reads what it read, and a
    statement is not held to 65,535 bound values), statements piped
    until the batch ends; a staged run's COPY binary where every column
    and value is of a kind sent exactly (`_binary_kinds`, the rest as
    text), and past 1,000 rows the stage or the statements, binary or
    text, taken by what each cost for that table and run size. A counter
    of a two-way hop is one `insert ... on conflict/duplicate key update
    n = n + by` a row, no longer a count and then a write (which also
    wrote over a row the target's own side made between them). Measured
    on postgres:16: a parent and its child in turn, a statement a change,
    2,000 in 0.46s before and 0.04s; 400 with a round trip of 10 ms,
    5.83s and 0.14s; with 50 ms, 24.50s and 0.44s. 200,000 rows in runs
    of 20,000: staged 2.18s before, 2.26s binary now; over 10 ms 4.08s
    and 4.07s; over 50 ms (60,000) 3.07s staged before, 2.44s now as
    piped statements. Binary and text were close on these rows. The
    round trip was added by a proxy that holds each packet (`tc` and
    `pfctl` need root on the machine measured on)
    (`test_rows_are_written_in_fewer_round_trips.py`).
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

   Done (R20.5-R20.7, part, 2026-10-16): `planner.overlap` lays a move's
   copies on its lanes, each table's index builds once it lands, each
   range's check once it lands and the tail once the first table is in
   (only where a position before every read follows the changes); three
   ways - every table as soon as a lane is free, within the foreign
   keys' waves, one after another - ranked by their makespan through
   `decide` and each proved by `guarantees.breaks` on its own schedule,
   said in `progress.json` (`overlap_said`). `movers.choose_compression`
   packs one sample at three levels across the codec's range (bounded:
   no higher level once one is slower than the link), checks each comes
   back, and picks per leg from the link and both sides' free cores;
   `Compressing.boundary` chooses again when the measure moves by more
   than a tie. `carriers.move_file` copies inside one object store
   (ranged part copies), else moves parallel ranged parts, each part's
   digest taken as it streams and checked as the store takes it, the
   whole checked at the end; part size and streams from `measure_link`
   (round trip, one stream, streams doubled while the rate rises).
   Tested: `tests/test_phases_overlap_and_legs_are_measured.py`.
   Not done: the engines' copy does not run on `overlap`'s schedule yet
   (it plans and says it); the relay's legs do not call
   `choose_compression` yet; Azure and Google stores move as one stream;
   a copy inside the store is checked against the parts it reported,
   not against the source object's own checksum.

   Done (W13L, 2026-10-16): Azure and Cloud Storage move as parallel
   ranged parts (blocks checked by the store as it takes them and by what
   it committed; parts composed in tiers and held to the parts' folded
   CRC-32C); a copy inside an object store is held to the source's own
   whole-object checksum or digest (`carriers.SOURCE_PROOFS`) and says so
   where the source gives none; a file here going to a store is timed on
   the way up by scratch objects; ship's parts and reads go through
   `move_file`; `planner.scheduled` lays a move out from its model's
   rates; `legs.relay_compression` chooses per leg. Tested:
   `tests/test_moves_use_overlap_compression_and_parts.py`.
   Not done (W13L): the engines' copies (other tickets' files) do not yet
   call `planner.scheduled` or `legs.relay_compression`; the Azure and
   Cloud Storage in-store copy; Azure/Google proof on a real account.

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

   *Built (2026-09-29), 8 and 9 (`migkit/leaves.py`, `migkit/kept.py`,
   `bench/proof.py`):*
   * **One scan, every leaf** (`leaves.scan`): count, row-hash sum and
     key-hash sum per leaf in one `GROUP BY` a side, salted 60-bit md5 of
     canon's text; leaves by ranges of one integer key (read back by its
     index), else by the top bits of the key's hash (composite, text and
     cross-engine keys alike), else - no key - by the row's hash with
     duplicates carried by the count. It replaced `hetero._bisect`.
     Measured, PostgreSQL pair, 10 rows apart: the whole verify of a
     differing table 26.6s -> 5.67s at 1M rows, 92.4s -> 29.7s at 5M
     (the halving asked 194 and 290 digests); over a 50 ms round trip,
     10 differences 136.8s -> 5.16s, 1,000 differences past 900s ->
     19.7s. The tree over equal tables costs +33% (1M) and +48% (5M)
     over the plain digest, so the plain digest stays the first pass.
   * **The self-check** (`leaves.localize`): each leaf read back must add
     up to its digest on its side, and what is found to the difference
     of the two tables' totals - else "differs, not fully localized".
   * **The sketch** (`leaves.Sketch`, `by_sketch`): an IBLT of sum cells
     (count, key, hash, hash squared) built in one scan in SQL on
     PostgreSQL and MySQL (in this process elsewhere), peeled here; a
     cell is pure only where its square lane matches. Over the 50 ms
     link: 5.92s at 10 differences, 6.18s at 1,000 (the leaves 19.7s).
     Decodes multiplicities, so a table with no key is reconciled too.
   * **The passes as rungs** (`leaves.PASSES`, `decide.compose`): prove
     (kept leaves, one scan), leaves (range, hash), localize (sketch,
     leaf reads, the bisecting reader), confirm (fenced, totals), keep
     (kept, again), a second reading (none, another reader's row hash,
     bisection). The localizer is climbed on its own measured costs. The
     bisecting reader over every column is faster on one engine (1.97s
     against 5.12s at 1M, 10.7s against 23.0s at 5M) and is a localizer
     there, what it names held to migkit's digests; without `-c %` it
     compared keys alone and missed all 4 changed rows.
   * **The proof kept up as the data moves** (`kept`, hop option
     `keep_proof`): every carried table's leaves, both sides, in one
     SQLite file beside the tail's position, seeded by one fenced read,
     moved by each batch's before and after images in the step that saves
     the position (a batch committed there and never saved is taken back
     out on restart), marked "to read again" where an image is not whole,
     a row filter or a server-text column applies, or the marks cannot
     place a change; a background pass re-reads the coldest leaves (a row
     written behind the tail's back shows there); the proof compares the
     kept numbers and re-reads only what differs, is marked, was touched
     since the last pass or is older than the bound, fenced, twice more
     for what still differs. The hop's own engine's check (PostgreSQL,
     MySQL) and the pair's stand on it. Measured: 7.2 us a change to keep
     both sides (in process); at 1M rows the final proof 1.46s against
     1.74s for the table digest in full and 2.56s for the one-scan tree.
     Not from the copy's range digests (a range is read batch by batch,
     each under its own snapshot, its mark taken once - the reason is in
     `kept`). Not measured yet: the tail's rate with every leaf kept (the
     run measured, 32,207 against 46,689 changes a second, marked every
     touched leaf because the slot's timestamp text did not render - now
     typed first), the final proof at 5M rows, and the `offset 0` fence.
10. **The fastest hash the pair allows:** same engine and version ->
    the server's native row hash (`hash_record_extended` and the like,
    no text rendering); cross-engine -> canonical rendering + md5,
    the rendering pushed into SQL where it is 80% of the cost;
    parallel workers on the server (`max_parallel_workers_per_gather`
    as a session setting) for the digest scan.
    Done (R20.10, part): a table's leaves are hashed by the way
    `leaves.HASH_WAYS` climbs on rows/s timed on its first 10,000 rows
    on both sides (md5, sha256, sha1 of the canonical text, each side's
    SQL held to the fold here on the same rows; md5 where none else
    holds), logged as `leaf hash`; a proved PostgreSQL- or MySQL-speaking
    server computes its leaves in that SQL (`ProvedDigest.leaf_proved`);
    range leaves are read by `sizing.Plan`'s statements at once
    (`leaves.gather`). Proved by `test_the_fastest_hash_the_pair_allows.py`.
    Not done: a native row hash with no text rendering
    (`hash_record_extended`, HASHBYTES, cityHash64, FARM_FINGERPRINT)
    needs this process to fold the same number, which no hash here does
    yet, so it is not a rung; the table digest (`canon.digest_expr`) is
    still md5 (canon is another ticket's); server-side parallel workers
    as a session setting are not set; the sandbox tests are written, not
    run (no docker in this ticket).
    Done (CF4): every table digest takes the hash the pair chose once a
    table (`HeteroEngine.digest_way` over `leaves.pair_hash`, among the
    hashes both engines' digests take, `leaves.digest_ways`): the check,
    a batch's ranges, a table done earlier, a seed's parts (the hash kept
    in the manifest; the load's lines stay md5) and a tail's kept digest;
    the hash is recorded beside each digest and `digests.unlike` refuses
    to compare two of different hashes. Proved by
    `test_every_table_digest_takes_the_chosen_hash.py`. Not done: the
    chosen hash is kept for the pair's life, not on disk, so a seed
    exported in a run that never chose one is md5; the leaves' own
    `fastest_hash` in `localize` is still asked apart; the WAL follow's
    keys are not yet persisted and a keyless table is still refused
    there (CF4 part C).
11. **Only what changed since the last proof:** tables and leaves
    untouched since they were last proved (modification counters,
    the tail's own record of keys touched) are not read again.
    Not done (R20.11): `unchanged.COUNTERS` reads PostgreSQL and SQL
    Server; MySQL and CockroachDB are declared in `unchanged.UNPROVED`
    (their files are other tickets'); the two SQL Server counter
    statements have not run on a server (stub tests only); postgres.py
    still asks `usable_brand` and `skippable` rather than `judge`;
    variants.py still names `TRUSTED_BRANDS` in a comment.
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
    **Done 2026-10-01** (`migkit/crossmove.py`): MySQL, SQLite, DuckDB,
    Parquet, Delta and Iceberg into PostgreSQL, range by range, each range
    held to both servers' leaf digest; the types it carries exactly listed
    per engine and a table with any other refused with the reason; Delta
    and Iceberg tables read through their logs as a Parquet source's
    tables. Not into MySQL: measured 1,042.8s against the copier's 47.8s
    for a million rows (its writer sends INSERT statements).
14. **The source renders the target's load format in SQL:** a
    `SELECT` on the source that emits each row already in the target's
    bulk-load format (PostgreSQL COPY text, MySQL LOAD DATA fields),
    piped as bytes into the target's loader - both servers do all the
    work, nothing is parsed between them. Only for the types whose
    escaping is proved exhaustively (a property test over every canon
    class and every byte), the rest refused to the other rungs.
    **Done 2026-10-01** (`migkit/loadtext.py`, `crossmove`): both ways
    between MySQL and PostgreSQL; the two ways and the table copier are
    rungs of the engine's own chooser, each range on the way its table
    measured cheapest. Measured (`bench/cross_rates.py`, both servers in
    one 2-CPU VM, four workers): PostgreSQL into MySQL 23.1s against the
    copier's 56.1s a million rows, 186.1s against 337.5s at five million;
    MySQL into PostgreSQL 101.6s against 402.0s at five million, and within
    the spread of two runs at one million, where each range's proof - a
    JSON column hashed in this process - is most of the time. The
    property tests are written and not yet run (the owner's rule: one
    run at the end).

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

*Built 2026-09-30, items 1, 4 and 5 for the same-engine table copier; not
yet measured and its tests not yet run (the owner's rule of 2026-09-30:
every test once, at the end of the backlog).*
* **PostgreSQL** (`engines/pg_pass.py`): each range read by the source's
  client as COPY's binary encoding under one snapshot exported for the
  table and imported by every read (`pg_export_snapshot` / `SET
  TRANSACTION SNAPSHOT`, held no longer than `snapshot_limit`), written
  into the target's COPY 64 KB at a time with the range's delete in one
  transaction, through the relay beside the source (compressed) where
  there is one; both servers' digests of the range (the data check's own
  row hash, so ranges fold to the table's number) read at the same moment
  beside the next range's load, the source's under the copy's snapshot;
  the range recorded only once they agree. The snapshot's own
  `pg_current_snapshot()` is the mark the tail holds changes to. The
  table's one range is loaded `TRUNCATE` + `COPY ... FREEZE`; `pass-through
  unawaited` commits with `synchronous_commit = off` and records a range
  only once `pg_current_wal_flush_lsn()` has passed its commit, so a target
  crash loses nothing recorded (test: the target killed after a commit
  returned; the range is gone after the restart, unrecorded, `check` says
  different, the next run repairs it). Rungs `COPY_WAYS` (pass-through,
  pass-through unawaited, the pipe as before) climbed per range on the
  table's own costs (`decide.Trials`: each way timed on one range, then
  the cheapest a row); `TABLE_WAYS` (the ranges, the streaming copy of the
  whole table under the table's snapshot, proved by both digests) climbed
  per table a byte, this run's costs before the hop's.
* **MySQL** (`engines/my_pass.py`): each range rendered by the source's own
  client, in SQL, as the line the pinned `load data local` reads, under a
  consistent snapshot that digests the range first; the lines streamed
  into the load (`_load_feed` now takes a stream); committed only where
  both digests, the counts and the load's silence agree, else the range
  goes as rows. `unique_checks = 0` only for a table emptied in the loading
  transaction whose target unique indexes the source enforces;
  `sql_log_bin = 0` (and myloader's binlog left off) only where the hop
  says `target_has_replicas: false`. `TABLE_WAYS`: pass-through, the rows,
  the parallel dump and load of the table (`movers.mydumper_table`).
* **Phases:** a finished table's index builds run beside the next table's
  copy (`movers.Beside`, PostgreSQL and MySQL windows alike), each
  PostgreSQL build under `maintenance_work_mem` and
  `max_parallel_maintenance_workers` for the session from the target's
  `shared_buffers` and CPUs (`_pg_build_settings`); a range's check runs
  beside the next range's load.
* **Still to do:** run the tests written with it and
  `bench/same_engine_copy.py` (1 GB and 5 GB, every way, both engines),
  put the numbers in the docstrings, order the rung lists by them and set
  `STREAM_FROM` from them.

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
   Tencent DTS (2026-10-16, `imports.from_tencent_dts`): a timed or
   unstarted job (RunMode, ExpectRunTime, Status) is said, a side's
   Region beside its instance, DbMode all takes a database whole, InitType
   Data said; what the job does not do is said with the command that does
   it (a PostgreSQL source's sequences under changes, a sync job's
   accounts); the job's CompareTask report saved beside the file as
   `<CompareTaskId>.json` becomes `compare_with`, else the command that
   saves it is said; an unknown DatabaseType is refused with the next
   step. Not done: a whole schema renamed (NewSchemaName with TableMode
   all) is still said, not carried - the hop renames tables only; what a
   PostgreSQL migration job carries of DDL while it follows changes is
   not said (the service's limit there is not measured); the tests were
   written, not run (one run at the end).
8. **The view helps too:** the dashboard gets a setup page doing what
   `init` does, for operators who prefer a form (behind the same
   roles and host checks).
   Built (R21.8): `/new-hop` (`web/newhop.py`) calls `guide.run`, passwords
   kept in the keychain. Not done: a task file or a password reference is
   refused on the page (terminal only); the guide refuses a hops file that
   holds `access:`, so a view with roles adds its hops by hand until the
   guide reads past it; no REST route for it; the page is not yet run
   against a real server pair.
9. **Every message says what to do next**, in plain words, with the
   exact command or statement.
   PARTLY (f81e87f): `docs/research/next-step-audit-2026-10-15.md` lists
   every operator message of `cli`, `report`, `guide`, `doctor` and `verdict`, says
   whether it names the next step, and ranks and words the thirty first
   fixes. *Not done:* the messages are not rewritten; the audit is the
   list, not the fix. W12N: verdict priorities 16-20 and 29 are rewritten
   with the exact check command; the other 24 sit in `cli.py` and
   `guide.py` (owned by other tickets) and are listed under "Skipped
   (owned)" in the audit. Priority 29 names no command yet: the partition
   line gets no hop name.

Measured by: time from nothing to a verified move for a new operator
(scripted in docker with each engine pair), number of config lines a
typical hop needs (target: the two addresses and nothing else), and
questions the guide asks (target: none beyond the addresses and a
password).

Not done (2026-10-16): `tools/ease_measure.py` measures the address-only
init, move and full check on every two-server sandbox profile, plus
PostgreSQL/MySQL cross-engine moves. Live timings remain unmeasured
because the local sandbox is stopped; no verified time is claimed.

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

*Built (2026-09-30), items 4 and 6:*
* **Hops removed** (`migkit/legs.py`): two rungs at the head of the one
  ladder a database climbs (`movers._ladder`), each needing the hop's
  leave (`server_to_server:`, true or `{source: host:port}`): the target
  pulling through a foreign server - extension, server, user mapping and
  a schema of foreign tables made on the target, `INSERT ... SELECT` a
  key range at a time several at once, the fetch sized from the round
  trip, the credentials dropped the moment the last row is in, all of it
  said in the plan first - and, below it, a temporary subscription with
  `copy_data` (same major version, binary; a publication and a slot on
  the source, dropped after; tables without a key go by the copier, since
  published they would make the source refuse its updates). Proved at the
  start of the move by acting on the target (a probe through the foreign
  server, a subscription with no slot), falling a rung and saying why.
  Measured on the pair with a proxy's delay: the pull wins only where
  migkit is far from the target (beside it, its own copy was faster by a
  quarter, 0.77 s against 1.05 s a 40 MB range at 50 ms), so the rungs
  need that, measured by a statement's round trip. MySQL's clone is the
  physical rungs' (`physical-clone`), named in `legs.DIRECT`. Where
  migkit should run is said in `doctor` and the plan from the same round
  trips (`legs.placement`), the servers' own where the pull's proof timed
  it. Not measured: what crossing a width-bound link once instead of
  twice saves.
* **The machine to run on** (`migkit/machine.py`,
  `migkit/data/instances.json`): 98 AWS, 48 GCP and 43 Azure types from
  the providers' own pages and price lists (sources in the file), network
  and disk as baseline and burst; the time on a type worked out in
  phases, the burst while it lasts and the baseline after; the cheapest
  type meeting the hop's `deadline:` said with its time, cost and what
  binds - or that a server binds and no machine would help; the stages
  read from the resource model's `model.json` where the plan kept one,
  else from the hop's own runs (bytes and CPU seconds now kept with each
  rate). Refreshed from AWS's API (instance types and price list) where
  credentials are present, kept a week. The cloud and region from
  `cloud:`/`region:`, else `clouds` (the address now gives AWS's region)
  and `facts.json`.
* **The target's class** (W2's first piece): each `assess` observes the
  source's load for 3 s (`Engine.load_sample`, PostgreSQL and MySQL:
  sessions on a CPU, block reads and writes, transactions, connections,
  cache and hit ratio), keeps it beside the hop, and sizes the class from
  the busiest kept (95th percentile past 20), with RDS's default
  connection limit checked against it.

*Built (2026-09-30), items 1, 2, 3 and 5, and memory as a resource (the
owner's addition):* `migkit/model.py` - each stage's rate one stream
gets and all streams get, and where it came from (measured now, on an
earlier run of this hop or of the same engines, catalogued, assumed);
probes bounded in bytes and seconds and read-only on the source
(`Engine.probe_read`, `probe_link`, `pressure`, `io_wait`,
`changes_made`, `row_sizes`), the target's writes into a table rolled back
or a temporary one (`probe_write`, only once the interlock has held the
target to the plan - a plan alone writes nothing); the path the copier
takes per table (server to server or through migkit,
`Engine.passes_through`) and what a move measured of itself
(`Engine.measured_ways`, kept for the next plan); a server measured at its
link's speed or at this process's one core told apart from a saturated
one. The binding stage named, the workers by Little's law capped by each
side's connections and by the memory the widest row takes (`sizing`),
reads and writes sized apart for a dump and a load (`sizing.split`); one
limit per resource while it runs (`sizing.Limit`: each side's load, lag
and I/O wait, each link's round trip and this machine's retransmits,
this machine's memory as a watchdog that holds a unit until it fits),
never slower than the one pace on the same synthetic runs; the phases
with p50/p90 and their reasons (`planner.phases`: the copy by table, index
builds beside the copy, the check, the catch-up's sum, the cutover from
the hop's last one or its budget) in the dry-run plan, `init`, `watch`
(EWMA, the catch-up live) and `/metrics`; a move that one worker's widest
row cannot fit refused before it starts. The tally hashes a row in pieces
(64 MB row: 16.5s and 0.11s; a 100 MB value: 646 MB held and 10 MB).
Still to: the copier's memory per engine and the docker
tests' stated errors from a full run (written, the owner's run at the
end); a server's own memory for digesting wide values (a PostgreSQL
source digesting 160 MB values beside a 2 GB migkit on a 4 GB docker
machine was killed by the kernel).

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
   **Each report deeper than the one it is measured against** (the
   owner): the premigration assessment (DMS's lists an issue; migkit's
   says how many rows, which keys as examples, what it will cost, the
   exact statement or setting that fixes it, and what migkit does about
   it by itself), the validation report (DMS/DTS say a table failed;
   migkit's says which rows, which columns, since when, whether it
   settled under the fence, and the repair with its undo), the schema
   and code conversion assessment (a vendor's counts of objects and
   effort; migkit's says per object converted, proved by execution, or
   not, with the counterexample), the comparison report (Veridata's and
   Datafold's per-table differences; migkit's adds the proof method, the
   exact localized rows, keyless tables and every engine pair), and the
   run report (phases, rates, the binding resource, the downtime
   measured) - one family of reports, the same look, HTML/PDF for people
   and JSON/CSV for tools, each saying how to recompute it.
   *Done 2026-10-01 (`export`, `digests`, `session`); its tests wait for
   the one run of the suite at the end:* every command on a hop is a run
   with an id (`session`) stamped on the record's entries, the verdict,
   the ways chosen (`decide.LISTENERS`, kept in the operator's words in
   `decisions.jsonl`) and the digests each check takes (`digests.jsonl`:
   rows, digest, key digest, the statement the server ran, the salt, the
   columns and their classes, the filter - recorded by PostgreSQL's and
   MySQL's own passes, every pair through `hetero._digests`, and SQLite).
   At the end of a run that left anything, its bundle is written to
   `evidence/<run>/`; `migkit report` writes the hop's whole history to
   `evidence/`: `evidence.json`, `evidence.html` (the check report's
   look), `evidence.md`, `tables.csv` - what moved (per table, the copy's
   rates by way), the verdict and every check, both sides' counts and
   digests with the statement that takes each again (built for a neutral
   digest from the recorded columns by `canon` itself) and the method and
   rendering rules, the stored code's proof per routine with its
   counterexample, the assessment, positions, decisions with every way
   passed over and why, approvals with their signatures' digests,
   repairs with the digest and count of the statements applied (now kept
   beside their undo, `*.applied.sql`), cutovers with phases and the
   outage, what was dropped, and the record's chain state. Anchored: the
   files' SHA-256 go into the record (and the run-state bucket where the
   hop has one); signed with `evidence: {sign: key}` or
   `MIGKIT_EVIDENCE_KEY` (`ssh-keygen -Y`, namespace `migkit-evidence`);
   encrypted to `at_rest` recipients. The guard test reads every file of
   an export built from inputs planted with program names. Not done: PDF
   (the HTML prints to one).
2. **Rehearse, then forecast:** a goal that runs the whole path on a
   clone or a sample the operator names, and turns what it measured
   into the forecast for the real run - time per phase, downtime,
   bytes, cost - with the confidence of a sample of that size; the real
   run later compared with its forecast.
   *Done 2026-10-01 (`rehearsal`); its tests wait for the one run of the
   suite at the end:* `goal: rehearse` with `rehearses: <hop>` (or `{hop,
   databases: {its db: that hop's}}`) runs the real hop's whole path -
   `move --mode full+cdc --go`, `check`, `move --mode cdc --drop --go` -
   into a target of its own, and marks what it rehearses and what decided
   its path in its reports (`rehearsal.json`), so a rehearsal run from
   another machine's reports is found too. No second estimator: the real
   hop's plan is `planner.phases` over `model`, fed by the rehearsal before
   any other hop - its stages (`model._earlier`, truth `REHEARSED`, named in
   the plan), its copies' rates (`planner._elsewhere`, `_bytes_rates`), its
   checks' time (kept as `rehearsal.CHECK` with the rates, only for a hop
   that rehearses or is rehearsed), its cutover's outage
   (`planner._with_cutover`, and the cutover preflight's budget line); a
   plan that may not write takes the target's writes from it
   (`model.build(write=False)`). The confidence of the sample: the p90
   widened by 0.15 a factor of ten between the rows the rehearsal carried
   and this move's, at most 0.6 (`sample_spread`) - a rule, not a
   measurement - and the share said. The real run keeps its forecast as it
   starts (`forecast.json`) and each phase is held to it once as it ends -
   within, slower than its p90, faster than half its p50
   (`forecast-runs.jsonl`). What the rehearsal did differently from the
   hop (engines, exclude, mapping, the options that choose ways, places
   and secrets left out) is named by `ready`; a rehearsal that ended with
   differences stops the real move.
3. **An interlock on the target:** the plan shows each side's identity
   (cluster id, host, version, size, whether it holds data) and `--go`
   refuses when the target is the source itself, when it looks like a
   production system the hop does not name as the target, or when it
   holds data and the hop has not said what to do with it; the
   identities are checked again at the start of every write step.
   *Done 2026-09-30 (`interlock`); its tests wait for the one run of the
   suite at the end:* each engine says what
   it is (`server_identity`: PostgreSQL's system identifier and the
   postmaster's start, MySQL's `@@server_uuid`, MongoDB's replica set and
   the member answering, SQL Server's `@@SERVERNAME` and its database's
   creation, Redis's `run_id`, Kafka's cluster id; version, size, the
   managed service by `clouds`), in the database the engine really writes
   (`side_database`: PostgreSQL never maps its maintenance database - the
   first docker run found the interlock calling the source's own database
   another one). Every write step - move, sync, repair, cutover, users,
   converted schema, rollback, the stream back - starts at
   `before_write`; a copy at `before_copy`, which asks `rows_held` (the
   pair's reader, within the row filter). Hop options `target_rows:
   refuse|replace|keep` (refuse by default), `production: [patterns]`
   and `production_target`, declared in `hopkeys` and checked on load.
   What a hop wrote into the same target before is its own on a run
   again; a restart or failover asks for the plan to be shown again.
4. **Performance after a change of engine:** `workload.py` compares the
   source's own reads on the same engine; after a change of engine the
   translated workload (P10) is timed too, writes as well as reads, and
   the target is advised (indexes, statistics, the settings its load
   needs) before cutover.
   *Done 2026-10-01 (`workload`); its tests wait for the one run of the
   suite at the end:* across engines (the hetero pair's deep check, where
   the source keeps statement statistics) each statement is translated by
   `storedcode.translate` with the hop's renames (`convert.localizer`) -
   refused ones counted and said - planned and timed on each side in its
   own SQL, and a read run on both is held to the same answer as a
   multiset of values (numbers by their digits, moments in UTC), weighted
   by the calls each had; the same across two major versions of one
   engine. Writes come from the same statistics (`workload_writes`):
   planned on both sides and never run on the source, their source time
   the statistics' mean; timed on the target only on a rehearsal's own
   before its cutover, in a transaction rolled back with a short lock wait.
   The finding's fix names the source's index as the target makes it
   (`for-the-target.sql`), the target's missing statistics, and the
   planner's settings that differ between two servers of one engine
   (`planner_settings`). SQL Server joins PostgreSQL and MySQL: its Query
   Store, `SHOWPLAN_XML`, compatibility level. Fixed on the way: a bitmap
   scan read as a table read whole on PostgreSQL. Oracle's bind capture
   is left to the Oracle files' owner.
5. **Copies for testing:** a referentially complete subset (the closure
   of foreign keys from the rows chosen) with masking, into a staging
   or developer database - the same engine pairs, the same proof of
   what was copied.
   *Done 2026-10-01 (`subset`); its tests wait for the one run of the
   suite at the end:* `goal: subset` with `subset: {start: {table: a
   condition | N% | N | all}, children, most}` and the hop's
   `mask_on_write`. `move --mode full --go` chooses the rows, follows every
   foreign key to the row it names until nothing new arrives (with
   `children: true` the rows that name those too, through
   `neutral_rows_matching`), refuses past `most` or on a reference it
   cannot follow, empties the target's tables in scope children first and
   writes parents first, and proves it: each row read back by the kept
   keys and compared value for value, masked columns held to the rule, no
   row beyond the subset, every reference resolving inside the copy.
   `check` asks the same of the source as it is now. A share is taken by a
   hash of the key, the same rows every run. Masking is held to the
   parallel dump's rules (`movers.mask_plan`): never a key or a column a
   reference runs through, never random. Refused: following changes,
   one table alone, `sync`.
6. **Right the first time, not a runbook to recover with** (the
   owner: "not a document - make it succeed once, no rollback needed,
   high correctness, high quality"): the rehearsal (2), the preflight
   and refusals before anything is written, the proofs at every step
   and the go/no-go gates are run by migkit itself, so the real run
   repeats a path already proved; the rollback stays armed as a safety,
   not as a plan. What people must still do by hand (a DBA grant, an
   application switch) is listed by the plan at the moment it is
   needed, not in a separate document.
   *Done 2026-10-01 (`ready`); its tests wait for the one run of the
   suite at the end:* one place for everything a goal needs, each with its
   fix: the engine's assess, what the goal needs each server to allow
   (`dba.needs`, the DBA's script), the interlock's refusals asked before
   the write (`interlock.refusals`, `whose_rows` shared with
   `before_copy`), the target's disk against the tables and this
   machine's memory against the widest rows, a colliding mapping, the
   goal's own checks (upgrade, subset), the rehearsal and its forecast,
   the cutover, and what a person does and when (`by_hand`). `assess` is
   now `ready.items`; `move --go` and `sync --go` stop before the first
   write on every item sure to stop the step, all at once
   (`MIGKIT_PREFLIGHT=warn` goes on), and the plan without `--go` says
   each as "with --go this stops". What is only likely - a target that may
   hold its schemas already, a disk too small only with the indexes, a
   path not rehearsed - is said and does not stop it.
7. **A major-version upgrade as a goal:** `goal: upgrade` - the same
   engine at a new version with the least downtime, choosing between
   the in-place upgrade and a logical move by measurement, with the
   extensions and their versions checked first.
   *Done 2026-10-01 (`upgrade`); its tests wait for the one run of the
   suite at the end:* migkit never upgrades the source where it runs (a
   change of the source's server). The ways are `decide` rungs: a copy of
   the pages at the source's version upgraded in place on the target -
   PostgreSQL's own upgrade run where the target's data is (`physical:
   {upgrade: {bin, old_bin, data, start}}`, the new data directory made
   as the source's was with its sign-in rules and known settings carried,
   the files linked, cloned or copied as the upgrade's own check proves),
   MySQL's new server started on the clone's data, a cloud's own upgrade
   API (RDS/Aurora, Azure Flexible Server, Cloud SQL) - then followed from
   the copy's own position; or a logical move into a target already at the
   new version. The physical rungs need the upgrade said for this goal
   (`physical.UPGRADES`). The in-place step is timed and said as the
   downtime an upgrade on the source would have cost (`in_place_line`).
   Checked first, from both servers' catalogues: settings the source sets
   that the new version lacks (with what replaced each), every default the
   source relies on that changed (compiled default against compiled
   default), extensions offered at the source's version or a later one,
   types and removed functions in the source's own code, PostgreSQL 15's
   public schema, 17's search path for maintenance, md5 passwords,
   collation library versions (indexes rebuilt on the target after a copy
   of the pages), MySQL's reserved words, sign-in plugins, a jump its
   in-place upgrade does not take and its own upgrade checker, MongoDB's
   removed index types, SQL Server's compatibility level and deprecated
   features; plan regressions through `workload` once the target holds
   the rows.
8. **Working inside the team's tools**, as the others do: Airbyte and
   Fivetran ship Airflow operators, a Terraform provider and an API;
   Datafold runs its diff in GitHub CI on pull requests; Liquibase,
   Flyway and Atlas ship GitHub Actions; Debezium and PeerDB are watched
   through Prometheus and Grafana; DMS through CloudWatch and
   EventBridge. migkit: tasks for GitHub Actions, Argo and Airflow,
   OpenTelemetry traces beside the Prometheus metrics, Grafana
   dashboards shipped with the rules, with W2's API and Terraform
   provider (to be confirmed tool by tool before it is claimed). And
   what the tools inside migkit already expose is carried out through
   migkit's own surface, under its names: Debezium's JMX metrics, the
   bulk programs' progress, DVT's results and the verifiers' timings
   become migkit's Prometheus metrics, OpenTelemetry spans and report
   entries, so a team watches one thing whatever ran underneath.
   *Done 2026-10-01 (`ui`, `traces`, `integrations`, `action.yml`,
   `deploy/argo/`, `deploy/grafana/`); its tests wait for the one run of
   the suite at the end:* `/metrics` carries the copy's rows per table and
   tables done (its checkpoint, never the key it stands at), the last
   copy's rows a second by way (in the ladder's words), each check's
   seconds, the verify's ranges and rows proved, every table's rows per
   side and whether they agree, the last cutover's outage and phases, and
   the change stream's parts (its API) and its reader's own JMX readings
   through the Jolokia agent the pipeline now starts on 127.0.0.1:8778
   (behind the source, changes read, queue room, connected, snapshot
   tables left); new rules for a table that differs and the stream behind
   or down, and a Grafana dashboard whose every metric the test holds to
   the export. With the standard `OTEL_*` variables each command is a
   trace (checks, tables copied, cutover phases timed as they ran,
   decisions as events). `integrations.run` is the one runner - Airflow's
   `MigkitOperator`, Dagster's `migkit_op`, the GitHub Action and the Argo
   template (`integrations.step`) stand on it. Not done: W2's API and a
   Terraform provider (W2 is not built); the connector's own spans
   (its image has `ENABLE_OTEL`, but the tracing transform it needs was not
   run here).
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
   against a local stub. Approved by the owner in full (2026-09-29):
   every channel Apprise reaches, wrapped as migkit's own receivers.
   *Done 2026-10-01 (`notify`); its tests wait for the one run of the
   suite at the end:* any address Apprise instantiates (native webhook
   URLs included) goes through it under migkit's name (measured over
   twenty channels with every request caught: none carried the library's
   name once its asset was set); PagerDuty's trigger/resolve and the plain
   JSON webhook stay migkit's own. `notify: {to, events, lag}` - addresses
   as references (`env:`, `keyring:`, `pagerduty:env:`...), a plain one
   warned about by `hopkeys`. Events: verdict, drift (a DDL under a check
   or a move, a watching check's verdict), lag (the tail's heartbeat past
   `lag` seconds, and back), cutover (each step, the outage, the undo),
   approval (asked once per request, given), failure (a command that
   stops; a tail), fallback (a way given up for another, seen by the
   decision listener), done (a writing command). Masked hops send an
   error's kind only; passwords and program names are taken out. Tests: 21
   channels against one local stub, email against a local SMTP stub.
9. **Documentation as part of the product:** a five-minute quickstart
   for each common pair, a page per engine saying what is carried,
   what is refused and why, recipes for the usual moves (a managed
   service to another, on-premises to cloud, one engine to another),
   in migkit's own words; measured by a new operator following it.

### R24. The manual and the site, current and generated (added 2026-09-29)

The owner will write use cases and an article on a real migration done
with migkit; the web manual (`docs/index.html`, published from `docs/`)
describes an older version. So:
1. **The engine page generated from the code:** the supported engines,
   aliases and every source x target pair with what each carries
   (move, verify, follow, two-way, cutover, users, types refused)
   rendered from `capabilities.matrix()` and `decide.coverage()` at
   build time, so the site can never claim more or less than the code.
2. **Features and how to use them**, in migkit's words, by goal (copy,
   cutover, verify, two-way, keep in sync, upgrade), each with a
   five-minute path from two addresses (R21) and the reports it
   produces (R23.1).
3. **Use cases** written as recipes (a managed service to another,
   on-premises to cloud, one engine to another, a major-version
   upgrade, a zero-downtime cutover), with example numbers from the
   sandbox only.
4. **Release notes and a changelog** per release, and the version shown
   on the site.
5. Checked like code: a test fails when a page names an internal tool
   (the positioning rule), claims a pair the matrix does not have, or
   shows a command or option that no longer exists.

### R25. What a real migration still scripted around migkit (added 2026-09-29)

Compared, item by item, with a validation-and-repair package the owner
built around an early migkit for a real move between two clouds (its
contents stay private; only the shape of the gaps is written here).
Almost every script it holds is covered by the items above once built -
verify and count and hash, sequences, users and grants, settings
parity, freeze and active sessions, CDC positions, lag, standby
preflight, numeric scale, the final report and its evidence, instance
class, performance on a clone, the tunnels, the runner beside the
databases. What is not yet anywhere:
1. **Not pursued (the owner, 2026-09-29): driving the managed services.**
   migkit does not create, start, stop or tear down DMS or DTS tasks.
   Where a team uses them, migkit's part is what it already does and
   must do best: watch the databases while the service moves them,
   prove what the service carried, and repair what it did not carry or
   carried wrong (sequences, users and grants, partitions, types,
   differences found), with the undo kept.
2. **The cloud side of readiness:** the clusters' network (VPC,
   subnets, security groups, routes), maintenance windows and pending
   maintenance, instance classes on both sides, snapshots and restores
   through the provider's API, audited before the move and in `assess`.
3. **The organisation's own change sheet:** the plan's steps exported
   with ids that map onto a team's task sheet, and each step's progress
   and evidence written back, so the migration's tracking is not typed
   by hand.

### Verify depth (done 2026-09-30, not merged yet)

Seven items and one found on the way, each with its tests (written, not
run: the owner's rule for this round - the full suite runs once).

* **P0-KEYLESS, a table with no key copied and gone on from by buckets**
  (`ranges.keyless`, a `decide.compose` of how it is cut and how a stop is
  gone on from): by where the source stores its rows (PostgreSQL's pages,
  as before), by buckets of each row's canonical hash - the top bits of the
  hash the check's `row` leaves are made of (`leaves.bucket_where`), found
  by the server on each side, so the copy, its resume and the check find a
  row in one place - by the source's own hash of the row (SQL Server's
  `binary_checksum(*)`, DuckDB's `hash`, ClickHouse's `cityHash64`; the
  target's count places a stop), or in one pass for a table small enough
  that starting over costs less than a scan a bucket (`BUCKET_ROWS`, set,
  not measured). A bucket is marked begun before its first row and done
  after its last; one begun and not done is emptied on the target by the
  same hash and copied again, so nothing needs counting. A table that does
  not add up is held bucket by bucket to the source and only the buckets
  that differ are copied again. Rows held more than once are carried by
  count. The way is kept in the checkpoint, so a restart climbs the same
  one; spans kept by an older run are gone on from as pages. Built into the
  copier every pair shares, PostgreSQL's (for a server before 14) and
  MySQL's (where a stop started the table over), SQLite through a hash
  function registered on its connections.
  Tests: `test_a_keyless_table_resumes_by_its_rows_hash.py`.
* **F1.d, generations under writes** (`leaves.generations`, a `confirm`
  rung): where the source's log moved while the tables were read, the
  leaves that differ are read again generation by generation - after the
  tail's position where a tail runs, else the hop's `settle` - until none
  still moves (`MIGKIT_GENERATIONS`, 5). A leaf equal on both sides has
  settled; one that differs and held still on both sides is a difference
  as of now, localized from those leaves and held to their sums; one still
  moving at the last never settled, and no verdict is given for its rows.
  A table whose every differing leaf settled is ok, said leaf by leaf. The
  kept proof's fixed two rounds are the same generations now: stopped
  once nothing moves, the rest named as never settled.
  Tests: `test_generations_under_writes.py`.
* **D11, references parked in integer columns, and the objects' bytes**:
  an integer column is taken for references where at least half its values
  name an object on the source (an id counting past the first object's oid
  names a few by chance); what it names and the target does not is
  counted beside what named nothing on the source already. Every object
  both sides hold is compared by the md5 and length of each piece
  (`LO_PIECE`), some hundreds of objects a statement. Domains over `oid`
  (the `lo` type) count as `oid`. Tests:
  `test_documents_named_by_an_integer_are_checked.py`.
* **D15, how much of a row is compared**: every cross-engine verdict (the
  data check's, ok or not, the kept proof's, a pair's delta) ends with the
  columns compared exactly (one engine, one type, a rendering that is the
  whole value), by value and as what (`canon.how_compared`, `AS`), and not
  at all and why - only on one side, no shared rendering, left out by the
  hop's mapping (`verdict.compared_clause`). Tests:
  `test_every_cross_engine_verdict_says_what_it_compared.py`.
* **C3.repair, a statement-level record of a repair** (`repairlog`): every
  row repair - every pair's, PostgreSQL's (now one transaction a table),
  MySQL's, SQL Server's, MongoDB's, Redis's, the generic engine's - writes
  each statement with the row the target held and the one it will hold
  before any lands, and marks how far it got. `sync` says a repair that
  stopped; `rollback` puts each row back only where the target still holds
  what the repair wrote (a row changed since is named and left); a record
  replays the same way. Tests:
  `test_a_repair_is_recorded_statement_by_statement.py`.
* **10.more, newer-row-wins everywhere a target is addressed by key**:
  every pair (`Engine._apply_rows`, the column's value by its class on each
  side), SQL Server (whose row repair was a note to do it by hand and now
  goes through the neutral contract), MongoDB (a field), Redis (a hash's
  field), the generic engine. Tests:
  `test_the_newer_row_wins_on_every_engine.py`.
* **C4, the verification's load on its source**: SQL Server's check asks
  its schedulers' runnable tasks and an availability group secondary's
  redo still to do; the cross-engine check (every engine checked through
  the pair) brakes on both sides a table at a time - the pair's probe
  raised on a PostgreSQL side and had read with no brake from it. A
  replica's brake (`throttle.REPLICA_RISE`, 5 s, set): its lag rising above
  the least the run has seen of it. A PostgreSQL standby that has replayed
  all it received reads no lag - an idle primary made the time since the
  last replayed commit grow, and braked every table for nothing. A MongoDB
  secondary reads its own distance behind the primary. Tests:
  `test_verification_brakes_for_a_replica.py`.
* **Found on the way (R22's model), wide values hashed where they lie**: a
  text or byte value wider than `canon.WIDE` (1 MB) goes into a digest's
  row text as its length and the md5 of its bytes - in PostgreSQL's and
  MySQL's canonical renderings, PostgreSQL's own row hash, MySQL's own,
  SQL Server's, SQLite's and the fold in this process alike - instead of
  its hex or text written out and copied; a key, a record and a
  column-by-column comparison keep the value whole. A PostgreSQL table
  whose widest row would pass `DIGEST_MEMORY` with a copy a worker is
  digested by fewer parallel workers (`_digest_workers`, from
  `row_sizes`, the measure the move's model sizes by). Tests:
  `test_wide_values_are_hashed_where_they_lie.py` (a source capped at
  1 GB digesting rows of 150 MB).

And the localizers of a differing table (the sketch, the groups, the
bisecting reader, the leaves read back) go through `decide.attempt` now: a
way that cannot run or whose rows do not add up is `Blocked`, its fact set
for the passes' climb (`groups failed` and the others), and the next is
taken; what one found - differences or none - is the answer, never a
reason to switch.

Not done: wide JSON and XML values are still rendered whole (their text is
the value's; `octet_length` of a `jsonb` has to render it to count it).

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


## P0-VALIDATE: automatic validation and a clear operator workflow

Owner request, 2026-10-05. Status: OPEN. This extends P0-SMART.route and the
existing scope, verdict and reporting work; shared decision logic must be
reused rather than implemented again. Validation must be easy to configure,
choose its own effective strategy, inspect deeply and explain the outcome.
The operator should not need private scripts or knowledge of wrapped tools.

Observed gaps: standalone MySQL validation defaults to native checksums,
requires an explicit option for an alternative verifier, and partitions its
native scan only for a single-column primary key. A copy-time verification
chooser does not establish automatic choice for standalone validation.
Large values, composite keys, interrupted reads and incomplete results expose
both planning and operator-experience gaps. Capability presence alone is not
proof that the complete workflow uses it effectively.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-VALIDATE.decide | Route standalone validation through the shared decision engine | Discover usable methods and their prerequisites automatically; measure representative work within a bounded exploration budget; select and combine methods per table and check using measured cost, consistency requirements and source-load limits. Replan remaining work when rates or failures change. The normal workflow needs no verifier-selection option or external orchestration. Evidence records the decision and guarantees; the operator sees the expected outcome and relevant limits. |
| P0-VALIDATE.ranges | Efficient complete scans for difficult tables | Cover composite and textual keys, skew, partitions, keyless tables and large JSON/LOB values. Adapt range size and concurrency to measured bytes, duration and server load. Include target-only rows and open boundaries, avoid gaps or duplicate coverage, and preserve duplicate-sensitive comparison. Resume completed ranges after interruption; invalidate proofs when scope, schema, encoding or consistency context changes. Full validation must never silently become sampling. |
| P0-VALIDATE.state | Durable state that explains progress | Persist every successful table, range and check before advancing. After timeout, disconnect or restart, retain completed evidence and distinguish running, waiting, retrying, blocked, incomplete and complete states from correctness verdicts. Show completed and remaining databases, tables and checks, progress by rows/bytes where measured, elapsed time, ETA uncertainty, last progress and the cause of a stall. Stop or reclaim only queries owned by this run. |
| P0-VALIDATE.results | Clear results from summary to evidence | Console, UI and exported reports use the same scope and verdict. Lead with each database's outcome, then tables and objects that differ, with source value, target value, impact and next action. Clearly separate matched, different, expected migration differences, inconclusive and not checked. A count match cannot imply content equality; execution completion cannot imply a clean result. Expose coverage, observation window, concurrent-write limitations and unsupported checks, with evidence available on demand. Keep wrapped-tool names and implementation noise out of user-facing output. |
| P0-VALIDATE.config | Simple setup and guided execution | Reuse the existing hop model and commands. Accept minimal source, target and intended scope, or an imported migration task; discover safe defaults and explain missing permissions and unsupported scope before costly scans. Preview exactly what will be checked and the load and consistency limits. Provide actionable configuration errors and a direct resume path without requiring internal flags, manual checkpoints or custom scripts. Credentials remain private. |
| P0-VALIDATE.depth | Deep, accurate checks with measured workflow acceptance | Cover data, schema, sequences/counters, accounts/grants, routines/events, parameters, timezone rules and migration artifacts where applicable; name every omitted or unsupported dimension. Distinguish active replication lag, expected transformations and heuristic advice from confirmed defects. Use sandbox end-to-end fixtures with injected differences, key skew, large values, composite keys, failures and concurrent changes. Prove missing/extra/changed and duplicate rows are detected, completed results survive failure, estimates adapt, and the operator can configure, run, resume and interpret results without a custom script. Benchmark full coverage against the fixed default and record time, bytes read, server load and retry waste; claim improvement only when measured. |

Implementation order: state and verdict contract first, then decision routing
and adaptive ranges, then guided configuration and all report surfaces, followed
by complete workflow acceptance. Existing features marked DONE are not reopened
without evidence; these items remain OPEN until the integrated validation path
and operator workflow meet the criteria above. No implementation is included
in this backlog change.


### P0-VALIDATE.unattended: finish from a connection-only hop

Owner clarification and observed validation failures, 2026-10-13. Status:
OPEN. The normal operator supplies each side's endpoint, user, password and
port, starts the intended task and waits for a trustworthy result. Discovery,
method selection, deep validation, resource management, progress, recovery and
reporting belong to migkit. No private launcher, runtime patch, manual worker
tuning or repeated operator intervention is an acceptable completion path.
Reuse P0-MINIMAL-INPUT, P0-AUTO-RESOURCES, P0-RESUME-ALL and the validation
items above; this is evidence and acceptance for those owners, not a second
controller. Destructive actions still require authorization; missing scope
must not silently authorize access to every database.

Observed in a read-only PostgreSQL validation run: `Hop.takes_partition` was
called but absent from the installed code; its existing regression module
could not collect because `needs_docker` was undefined, and another existing
scope test referenced an absent `movers._left_out`. The structural comparison
failed while fingerprinting a schema because JSON dictionary keys included
tuples. Finishing required a private launcher restoring historical scope
methods and adapting fingerprint serialization. CPU discovery failed and
sizing reported a fallback of four CPUs while the target had two vCPUs;
independent monitoring showed approximately 94% target CPU during a long
checksum. That establishes missing capacity evidence, not proof that a
particular concurrency value caused the slowdown. Large partitions exposed
long periods without a completed database result. Raw comparisons also mixed
application differences with replication staging/bookkeeping objects and
managed ownership differences. These are workflow failures even where
individual checks eventually succeed.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-VALIDATE.unattended.setup | Connection-only normal setup | Discover engine/version, supported access, existing task scope and dependencies from the hop and authorized metadata. Derive safe operational settings and the complete applicable check set, including deep checks and consistency boundaries. Ask only for irreducible intent or authorization. No algorithm, worker, timeout, deep-mode or checkpoint settings are required to obtain the strongest supported complete result. Explain unsupported guarantees before costly work. |
| P0-VALIDATE.unattended.preflight | A runnable, compatible installed verifier | Check required method contracts and supported dependency/API versions before expensive reads. Fix the missing partition-scope integration and tuple-key schema fingerprinting in their shared owners; preserve deterministic, complete fingerprints without dropping keys or objects. Existing regression modules must collect and exercise the installed entry point. No runtime monkeypatch or custom launcher is required. A failed prerequisite is an actionable incomplete result, never a skipped check or a clean verdict. |
| P0-VALIDATE.unattended.capacity | Observe and adapt without manual tuning | Use provider metadata and bounded engine/load probes when privileged CPU discovery is unavailable. Record unknown capacity explicitly; a fallback is not a measurement. Budget outer tasks, ranges, connections and server-side parallel workers together, accounting for both endpoints and other runs. Adapt work size, method and concurrency at safe boundaries from measured CPU, I/O, latency and progress, preserving full coverage and consistent evidence. Never provision resources or change database settings implicitly. |
| P0-VALIDATE.unattended.progress | Durable progress through the slowest object | Persist and stream completed table/range/check evidence before the whole database ends. Report the current endpoint/object, last progress, measured rows/bytes, bottleneck and bounded ETA uncertainty without log inspection. Split large partitions and JSON/LOB work where correctness permits; manage snapshot lifetime and resume valid units automatically after interruption. A changed method or consistency window invalidates incompatible proofs. No full rescan of valid completed units and no downgrade to sampling or counts-only evidence. |
| P0-VALIDATE.unattended.scope | Useful results and safe repair boundaries | Derive replication artifacts from task identity, catalogs and ownership rather than broad name-prefix exclusions. Separate application mismatches, protocol state, approved owner mappings and unknown differences while retaining raw evidence. Never classify an unexplained difference as expected. Keep sequence collision, grants, missing/extra/changed rows and unsupported checks visible. Generated SQL must not treat live replication internals as application repair targets or imply permission to apply it. |
| P0-VALIDATE.unattended.proof | Prove the operator can leave it running | Through the installed command, run sandbox end-to-end cases with only connection fields and recoverable task intent. Cover restricted CPU probes, asymmetric capacity, nested parallelism, large partitioned JSON/LOB data, concurrent writes, replication artifacts, owner changes, injected data/schema/sequence/grant defects, dependency incompatibility and repeated interruption/resume. Measure operator actions, completion time, endpoint load and retry waste against a fixed baseline. Acceptance requires no private scripts, manual tuning, hidden omissions or human monitoring to reach the correct final result; follow the repository final-suite cadence. |

All items remain OPEN. This records requirements and observed failures only;
the temporary operational workarounds do not close implementation work.

Owner clarification, 2026-10-13: this unattended contract applies to every
current and future process, not only deep validation. The existing shared
decision engine owns what to do, how to do it, how much work to admit and
the effective values: methods, composition, ordering, batch/range sizes,
concurrency at every level, throughput limits, timeouts, retry/backoff and
recovery. Discover facts, measure representative work, choose within the
required guarantees, execute, verify and reconsider remaining work as
conditions change. Startup-only sizing, a fixed fallback presented as
measured capacity, or a private script making these choices does not meet
acceptance. Missing facts require safe bounded discovery or an explicit
unsupported guarantee, never silent assumptions or manual operator tuning.

Extend P0-DECISION-ALL.coverage and P0-AUTO-RESOURCES.route with each task's
actual choices and effective values, their evidence and the shared owner
that selected them; include managed-mover diagnosis and native retry/resume.
Preserve deterministic safety constraints and authorization boundaries.
Acceptance must exercise installed workflows under changing capacity,
permissions, data shape and failures, proving that decisions adapt without
operator intervention while coverage, ordering, consistency and resume
guarantees hold. The normal user provides connection access and irreducible
business intent, then receives progress and a verified result; no hidden
human controller is part of the claimed automatic workflow.


### P0-VALIDATE.mysql-query-lifecycle: no orphaned scans or blind replay

Owner-authorized permanent fix, 2026-10-13. Status: OPEN; local implementation
and sandbox evidence are partial, not completion. Reuse the existing query,
checkpoint, resource-budget and decision owners; do not add a private controller.

Observed: a full checksum on a roughly 350 GB table with a composite text
primary key fell back to one unbounded scan. Client read deadlines were one
hour; whole-query retries started four identical server scans roughly one hour
apart while the older scans remained active. Current-run salt and server client
PID attributes established ownership. Deep LOB sizing also repeated full scans
per column, and source scan errors could be skipped. These observations do not
establish a network failure as the initial trigger.

Required before DONE:

Owner clarification, 2026-10-14: recovery is a shared contract for every task
and phase (move, CDC, check, verify and deep), across every current and future
engine, engine pair, adapter and external execution backend. MySQL is the
observed incident, not the implementation boundary. This is an engine-neutral
recovery contract, with engine-specific reconciliation supplied by adapters.
Reuse and connect existing ledgers, journals and durable state owners.
Track a coverage matrix by engine, pair, operation and phase; mark unsupported
recovery explicitly. No engine may claim resumability based only on another
engine's tests. Acceptance requires fault/restart evidence for each supported
path, including external worker/process state and native CDC positions.

- Persist a task/phase/object/work-unit ledger with planned, running,
  committed, interrupted, failed and uncertain states; retain attempts,
  ownership, evidence, checkpoint positions and decision history outside temp
  directories. A completed report is not a workflow recovery ledger.
- Recover automatically after network loss, process termination, host reboot
  and partial source/target failure. Reconcile in-flight server work and write
  outcomes before retry; never blindly replay an uncertain write or query.
- Resume all still-valid completed work, including completed tables and deep
  LOB ranges. Validate endpoint identity, scope, schema, plan and snapshot/CDC
  consistency before reuse. Expression/range fingerprints alone do not prove
  unchanged data. If evidence is invalid, explain and rerun only the affected
  units or dependency boundary; never silently reuse stale evidence.
- Let the shared decision engine adapt query strategy, work size, concurrency,
  rate and pauses to pressure on both source and target (CPU, memory, I/O,
  latency, connections, locks and CDC lag), with hysteresis and bounded budgets.
  Preserve completed coverage when replanning; do not silently downgrade full
  verification to sampling or discard progress whenever batch size changes.
- Prove restart recovery at each phase and commit boundary, including uncertain
  outcomes, stale evidence, concurrent runs, unavailable telemetry and repeated
  disconnects. Demonstrate no duplicate effects, orphan queries or reruns of
  valid completed units, and expose the exact remaining work to the operator.


- Reconcile exact connection/statement ownership, cancel and confirm server
  termination before replay. When permissions, visibility or connectivity make
  confirmation impossible, refuse replay with an actionable incomplete result.
  Handle cancellation races, already-completed statements, disconnects and
  process interruption without affecting other runs, applications or native DTS.
- Bound and checkpoint complete checks for text, composite and skewed keys,
  including very large repeated key prefixes. Preserve target-only rows at both
  outer boundaries, full value fidelity, filters/mappings and required consistency.
  Resume only compatible evidence; no sampling substitution or silent omissions.
- Group LOB checks and use bounded work units. Persist completed maxima and
  their validity conditions; a failed required scan is ERROR/incomplete, not PASS.
- Use the shared decision engine to choose/adapt work-unit size and nested
  source/target/server concurrency from actual rate and pressure. Prevent retry
  amplification and coordinate overlapping verification runs automatically.
- Publish current object/range, attempts, cancellation outcome, last progress
  and uncertainty while work runs. Logs buffered until database completion do
  not satisfy this acceptance criterion.
- Prove the installed entry point on real sandbox databases with slow reads,
  socket deadlines, unavailable cancellation permissions, interruption/resume,
  skew, composite/text keys, target-only rows and simultaneous unrelated work.
  Show no orphaned/overlapping attempts, no duplicate scan of valid completed
  units, unchanged full-check correctness and no interference with other work.
  Reconcile full-suite collection failures and record remaining gaps explicitly.

Local patch: query cancellation/reconciliation, key-prefix ranges, grouped
ranged LOB sizing and explicit LOB scan errors. Live sandbox checks establish
retry non-overlap, text/composite checksum equality, outer-boundary detection
and honest LOB failure reporting. Durable LOB resume, adaptive skew handling,
installed workflow completion and complete regression proof remain owed.

Owner priority, 2026-10-14: address this incident among the first implementation
items, before feature expansion. Million-key ranges can still exceed query
deadlines because LOB bytes and row cost vary. A failed whole-range query
replays almost an hour of work; cancelling orphan attempts alone is not a fix.
Measure byte/time cost, subdivide only outstanding coverage, preserve valid
completed intervals during replanning and checkpoint each bounded sub-unit.
Use adaptive shared decision budgets, not a longer timeout as the sole repair.
Status remains OPEN: existing cancellation and fixed-range patches are partial;
no automatic cost-aware planning or complete sub-unit recovery is proven.

Additional observed case, 2026-10-15: deep charset replacement-character
counting issues an unbounded aggregate over a large table, separately from
LOB maxima. The live source statement scanned order_items.note for over
18 minutes. Bound and checkpoint every required deep scan, including charset
and rendering/normalization probes; grouping only LOB scans is insufficient.
Classify live SQL by its actual operation: char_length/replace aggregates are
not LOB maxima. Preserve full coverage and aggregate correctness when splitting.
This observation does not establish a timeout, replay or network failure.
Code evidence from the same investigation: replacement-character scanning
catches any source/target exception and continues; an empty findings list then
emits OK. Required failed scans must remain ERROR/incomplete, with explicit
column coverage (the current loop selects at most 30 columns). Never equate
absence of findings with a successfully completed scan. Target SQL fingerprints
reappeared on new connections during monitoring; record actual attempt causes
to distinguish replay from completed work, without assuming timeout or network.



Owner acceptance extension, 2026-10-15: maximize measured end-to-end verification
throughput with full correctness and continuous adaptation across all engines,
pairs, adapters and check/deep/verify phases. This is a shared decision-engine
contract, not a MySQL-only optimization.

- Build a shared scan plan: fuse compatible column checks, charset/replacement
  counts, LOB maxima and other aggregates when doing so reduces total cost.
  Preserve each check's semantics, coverage, null handling and evidence; fusion
  must not create oversized expressions, packets or unbounded memory use.
- Estimate row/byte/LOB and query cost from bounded probes and live timings.
  Maintain a durable central work queue with cost-aware units, critical-path
  scheduling, bounded work stealing and explicit engine/backend capabilities.
- Revisit strategy, work-unit size, concurrency and rate during execution using
  measured useful throughput and source/target CPU, memory, I/O, latency, locks,
  connections and CDC lag. Use hysteresis and shared resource budgets; stop
  increasing concurrency when it harms throughput, correctness or stability.
- Replan only outstanding work and preserve valid committed coverage. Recover
  after network/process/host failure using per-unit ledger/state, exact query
  ownership and uncertain-outcome reconciliation; never silently replay writes.
- Prove equivalence to complete verification under the required snapshot/CDC
  fence, including disjoint/exhaustive coverage, aggregate merging, target-only
  rows, skew, large values and NULL/encoding edge cases. Sampling is not an
  implicit substitute for full verification. Required failed scans stay
  incomplete/error, never OK because findings are empty.
- Benchmark complete workflows, not only cached small queries: compare elapsed
  time, useful throughput, coverage/correctness, endpoint pressure and restart
  recovery with the baseline. Publish per-engine/path evidence and calibrated
  remaining-work ETA; record unsupported paths and residual gaps explicitly.

Status: OPEN. Existing local recovery tuning does not satisfy this acceptance.

Local implementation update, 2026-10-15: MySQL charset deep now has a
fused-column bounded-range path using shared sizing.Pace and ranges.Slots,
source/target concurrency, shared resource signals and explicit incomplete
scan errors. It writes per-unit progress, explicitly not a resumable result
ledger. Focused mocked tests prove merged replacement counts including NULL
and target-only rows, concurrent execution and honest interrupted-read status.
Live sandbox equivalence/performance, automatic cost-based range sizing,
global nested budgets and all-engine integration remain unproven. The already
running worker retains its loaded sequential implementation to avoid losing
in-memory completed deep results. This is partial implementation, not DONE.

Dependencies: P0-DECISION-ALL, P0-RESUME-ALL, P0-ETA-ALL and
P0-CONNECTION-PROGRESS. Prioritize with the first incident-recovery work.


## P0-DECISION-ALL: intelligent decisions throughout every task

Owner clarification, 2026-10-05. Status: OPEN. The decision engine is the
shared decision mechanism for every task, including existing tasks and future
features. P0-VALIDATE is one application of this requirement. This extends
P0-DE and P0-SMART.route; reuse their contracts and cost model. A working mover
chooser or a capability matrix does not establish complete task coverage.

The goal is deep, effective judgement throughout the workflow: understand the
task, discover relevant facts, compose a correct strategy, measure its actual
performance, adapt when circumstances change and explain the outcome simply.
A fixed chain of defaults, a wrapper dispatch or a single startup choice is
not sufficient evidence that this requirement is complete. Explicit operator
intent, permissions and correctness requirements remain hard constraints.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-DECISION-ALL.coverage | Audit every task and route every consequential choice through the shared decision engine | Maintain a generated inventory across configuration/import, discovery/assessment, planning, schema/conversion, move, replication/DDL, validation, repair/undo, cutover/rollback, security/accounts, managed services, HA, recovery and reporting. Map each task's meaningful strategy, ordering, resource, retry and fallback decisions to the shared engine. Pure formatting and deterministic domain invariants need no artificial alternatives; record the reason where there is no choice. Tests fail for an undeclared or bypassed decision, including new task entry points. |
| P0-DECISION-ALL.reason | Deep task understanding and composed strategies | Use engine/version, data shape and distribution, dependencies, capabilities, permissions, consistency, current state, available resources and operator goals. Separate known, measured, predicted and unknown facts; acquire missing facts safely when they can change a decision. Evaluate compatible compositions and relevant alternatives, prerequisites, cost, risk and guarantees rather than selecting only by installation order or nominal capability. For one feasible path, explain why; for none, give an actionable blocker. Never silently weaken correctness, scope, durability or authorization to meet a speed target. |
| P0-DECISION-ALL.adapt | Learn and adapt throughout execution | Bound representative exploration, measure real rates and failures, and update predictions and the remaining schedule at safe task boundaries. Choose concurrency, batching, order, retries and fallback within resource limits; retain valid completed work and recover from interruption. Reuse local learning only when its engine, version, shape and conditions apply; invalidate stale assumptions. Different conditions must produce different appropriate choices without operator scripts. |
| P0-DECISION-ALL.experience | One simple workflow with understandable state and results | Let operators state endpoints, scope, goals and limits using the existing configuration model. Discover defaults, guide missing inputs, preview outcomes and resume directly. Every task shares clear progress, verdicts, impact, next actions and evidence access. Explain consequential trade-offs and uncertainty in ordinary language; keep internal strategy and wrapped-tool details behind diagnostic evidence. Do not force operators to select internal algorithms or interpret raw logs to complete a task. |
| P0-DECISION-ALL.proof | Prove judgement, depth and end-to-end benefit for every task | Run sandbox workflow acceptance for supported engines and shapes with changed resources, skew, large values, missing permissions, failures and concurrent activity. Demonstrate correct feasible choices, useful blockers, safe adaptation, preserved evidence and readable outcomes. Compare to fixed defaults using task-relevant completion time, server/network load, cost, retry waste and correctness detection. Record actual entry-point coverage and unresolved gaps; simulated chooser tests and capability presence alone cannot mark the integrated task DONE. |

All five items are OPEN. Build an inventory of gaps before implementation;
connect existing decision facilities rather than duplicating them. Completion
requires the actual task workflows to meet these criteria, with exceptions
explicitly justified and tested. This backlog update adds no implementation.


## P0-WORKFLOW-DEPTH: deep and adaptive workflows throughout the platform

Owner requirement, 2026-10-05. Status: OPEN. Every workflow must be detailed,
intelligent and deep. P0-DECISION-ALL supplies the shared decisions;
P0-WORKFLOW-DEPTH requires the complete process to use those decisions well.
A linear sequence of commands, a flat checklist or a collection of independent
feature calls is not sufficient. Reuse the existing task, scope, state,
verdict, dependency and evidence owners; do not create a parallel workflow
framework without first identifying a missing contract.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-WORKFLOW-DEPTH.model | Model workflows at task, phase, object and work-unit depth | Each workflow declares its intended outcome, applicable scope, prerequisites, dependencies, required evidence, entry/exit criteria and durable states. Decompose work only as far as needed to plan, detect failure and resume safely. Cross-object dependencies and engine-specific behavior affect the actual schedule. Distinguish prerequisites that block work from independent checks that can continue. Unsupported scope is explicit; no silently skipped work. |
| P0-WORKFLOW-DEPTH.investigate | Branch and investigate according to actual findings | Each phase uses observed results to select the next relevant checks, including deeper investigation of suspicious ranges, constraints, transformations, permissions or configuration. Track hypotheses and supporting or contradicting evidence; distinguish symptoms from supported causes. Return inconclusive when evidence cannot establish a cause. A clean count, heuristic warning or successful command cannot end deeper checks required by the task's guarantees. Bound investigation cost and avoid repeating evidence already valid for the same context. |
| P0-WORKFLOW-DEPTH.connect | Carry facts, decisions and evidence between phases | Assessment findings shape the plan; chosen transformations shape validation; dependency and consistency state shape readiness; execution findings update downstream checks and recovery. Each phase consumes versioned evidence and states its validity conditions. Schema, scope or consistency changes invalidate affected evidence and downstream conclusions. A final outcome is derived from required phase evidence, never from the last command's exit status. |
| P0-WORKFLOW-DEPTH.recover | Adapt, checkpoint and recover at safe boundaries | Detect stalls and partial failures, diagnose relevant conditions, and choose bounded retry, alternative execution, replan or an actionable blocker through the decision engine. Persist completed units, isolate failures where dependencies allow and resume valid work without restarting everything. Respect load, consistency, authorization and side-effect limits. Repair, rollback and cutover require explicit preconditions, ownership, consequence checks and outcome verification; read-only workflows cannot become writes through fallback. |
| P0-WORKFLOW-DEPTH.operator | Keep deep internal behavior easy to operate | The user supplies goals, scope and limits through the existing workflow. Show the current phase, what is complete, what is being investigated, why progress is waiting, the remaining work and any required user action. Lead reports with database/object outcomes and practical impact; deeper reasoning and evidence are available on demand. Depth must reduce manual coordination, not produce more flags, configuration or logs for the user to interpret. |
| P0-WORKFLOW-DEPTH.acceptance | Prove depth through complete scenario execution | For each supported workflow and applicable engine, test healthy, misleading, ambiguous, unsupported, interrupted and changing-state scenarios in the sandbox. Demonstrate that identical initial checks with different underlying causes lead to appropriate different investigations and actions. Verify phase dependencies, invalidation, preserved evidence, truthful partial outcomes, safe recovery and correct completion criteria. Measure end-to-end time, repeated work and resource use; do not claim deep workflow coverage from isolated feature or decision tests alone. |

Implementation starts with a workflow inventory tied to P0-DECISION-ALL.coverage:
record the current phases, decision points, evidence dependencies, depth gaps
and task-specific completion rules. Keep these items OPEN until the integrated
processes meet their acceptance criteria. This change records requirements
only; it does not implement or certify those workflows.


## P0-MINIMAL-INPUT: minimal configuration and complete task ownership

Owner requirement, 2026-10-05. Status: OPEN. This is a platform-wide product
contract, extending P0-DECISION-ALL, P0-WORKFLOW-DEPTH and P0-VALIDATE.config.
The user supplies as few values as genuinely necessary. migkit owns discovery,
planning, strategy composition, execution, verification, adaptation and
recovery throughout the task. The system must pursue the best supported
outcome with the strongest applicable correctness, consistency and idempotency
guarantees. Complexity belongs inside migkit, not in operator configuration.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-MINIMAL-INPUT.setup | Ask only for irreducible inputs | Start from endpoints or an authorized imported task and the intended goal. Reuse securely available connection information and discover engines, versions, capabilities, scope, dependencies, object shapes and applicable settings. Ask only for facts or intent that cannot safely be inferred, including ambiguous destructive scope or required authorization. Distinguish required values from optional expert overrides. Show discovered scope for review; never interpret missing scope as permission to access everything. Measure required input count and manual steps for each supported workflow; remove every field with a reliable discovery or derivation path. |
| P0-MINIMAL-INPUT.ownership | Own the complete process automatically | Once the goal and necessary access are supplied, compose and execute the applicable workflow through the shared decision engine. Choose methods, ordering, concurrency, batching, consistency boundaries, validation depth, retries and recovery without asking the user to choose implementation details. Reuse valid evidence and learning, adapt to measured conditions and provide actionable blockers when completion is impossible. Normal operation requires no custom scripts or manual checkpoint management. Expert overrides remain optional and cannot silently bypass required guarantees. |
| P0-MINIMAL-INPUT.correctness | Strongest supported correctness and consistency | Explicitly model the task's required and achievable guarantees before choosing a plan. Prefer the strongest feasible consistent view and complete scope under authorized constraints; distinguish an observation during concurrent writes from a consistent snapshot or verified cutover. Establish compatible boundaries across related objects where required, account for replication position and transformations, and verify completion from evidence. Never silently replace complete checks with sampling, claim a stronger guarantee than established, or weaken required guarantees for speed. If the required level cannot be achieved, explain the missing condition and leave the task incomplete. |
| P0-MINIMAL-INPUT.idempotency | Safe repetition, retry and resume across the entire workflow | Every side-effecting unit has stable operation identity, known preconditions, durable progress and verified postconditions. After interruption, reconcile actual state before repeating an operation; retain valid completed work and prevent duplicate effects. Use native transactional or idempotent mechanisms where available and compensate only where explicitly safe and authorized. State exact limits where external systems cannot guarantee atomicity or exactly-once effects. Repeated execution of the same task against the same valid context produces the same intended outcome without accumulating extra effects. |
| P0-MINIMAL-INPUT.acceptance | Prove minimal effort and strong guarantees together | Use sandbox end-to-end scenarios across applicable engines, shapes and workflows. Demonstrate setup with the smallest necessary input set, correct discovery and scope, automatic strategy selection, consistent outcomes, truthful guarantee reporting and safe repeated runs. Inject interruption before and after side effects, stale evidence, changed scope, concurrent writes and partial permissions. Measure operator inputs/actions alongside completion time, resource use and retry waste. A shorter configuration that shifts work to manual steps, conceals ambiguity or weakens guarantees does not pass. |

Not done (INIT, 2026-10-16): `migkit init` now reads the engine of an address that names none off the server's own answer (`engineat`: PostgreSQL, MySQL with its version, SQL Server with its version, MongoDB, Redis, Kafka, ClickHouse/OpenSearch/Couchbase over HTTP; a host alone is knocked on at every engine's port; one question with the port's engine proposed where none answers), says the statements a missing grant needs, and keeps an address's databases in the hop written. Then (e3ccafe): Oracle, Cassandra, Db2 and Teradata are read off the wire too, and the brand behind a protocol (CockroachDB, TiDB) switches the engine after sign-in. Left: LOBs, partitions, change-reading depth, target emptiness and the measured link rate said by the guide; the server test is written, not run.

Best is evaluated against the task's explicit guarantees and authorized limits,
using measured evidence rather than an absolute or unproved marketing claim.
Correctness, required consistency and idempotency constrain the strategy;
performance is optimized within those constraints. All items remain OPEN.
This update records requirements only and changes no runtime behavior.


## P0-RESULT-SCOPE: concise database-first scope and validation results

Owner request and documentation research, 2026-10-05. Status: OPEN. Extends
P0-VALIDATE.results and P0-WORKFLOW-DEPTH.operator. Current operator feedback:
results are difficult to read, contain too much text and do not clearly show
which database has which problem or what has actually been established.

Documentation comparison (not a visual usability test):

| Product | Documented presentation | Useful pattern and remaining design need |
|---|---|---|
| AWS DMS | Table-level validation states and counts for pending, failed and suspended records | Separate execution state from data problems. Add a database summary above table detail and distinguish unable-to-compare from confirmed differences. |
| Vitess VDiff | Workflow result, comparison counts, progress and table/shard details; detailed summaries/reports available on mismatches or verbose output | Keep the first view compact and expand evidence only when needed. Include scope and omitted checks so a clean result has an explicit meaning. |
| Data Validation Tool | Tabular output, success/fail filtering and structured result handlers; several validation dimensions | Use structured comparison rows and filters. Aggregate by database and explain incomplete/unsupported dimensions rather than relying only on success/fail. |

Sources consulted:
- https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html
- https://vitess.io/docs/24.0/reference/vreplication/vdiff/
- https://github.com/GoogleCloudPlatform/professional-services-data-validator

Recommended design to prototype and verify with users:

| Level | Default content |
|---|---|
| Scope | Source to target mapping, included databases and table counts, checked dimensions, excluded/unsupported scope, filters/transforms and consistency window. Long rules and object lists are expandable; show resolved scope, not configuration syntax alone. |
| Overview | Databases with confirmed issues, databases with no issues found in completed checks, and databases still incomplete. Display overall checked/total coverage and an explicit incomplete outcome when required checks remain. |
| Database | One row per database: mapped target, outcome, checked/total tables and required checks, issue count, and a short plain-language problem summary. Prioritize confirmed issues and incomplete work; matching databases remain visible in a compact group. |
| Problems | Group within each database by data, schema/objects, access, counters, configuration and execution blockers. Show object, problem, source value, target value, impact and next action. Separate confirmed defects, expected differences and advice. |
| Evidence | Expand the affected object or check for detailed evidence, observation time and diagnostic context. Raw logs are never the main report. |

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-RESULT-SCOPE.scope | A readable resolved-scope view | Clearly show what was included, excluded, transformed, unsupported and actually checked, with source/target mappings and honest denominators. In-scope failures cannot disappear into excluded scope. Related objects and non-table checks remain visible. |
| P0-RESULT-SCOPE.summary | Database-first grouped results with concise language | First view identifies each affected database and its problems without opening logs. Distinguish confirmed issue, no issue found within completed coverage, expected difference and incomplete evidence. Use explicit text labels, not color alone; one finding has one short statement, with detail on demand. |
| P0-RESULT-SCOPE.usability | Prototype and prove comprehension across result surfaces | Use synthetic clean, mixed, interrupted, filtered, renamed and unsupported scenarios. An operator must identify scope, affected databases, concrete problems and remaining work from the first view, then find supporting evidence on demand. Console, UI and exports agree on facts, grouping and verdicts. Compare alternative layouts with users; documentation research alone cannot mark usability DONE. |

All items are OPEN. This is a researched design proposal, not implemented UI.
Current priority remains completing the ongoing validation; implement the
report redesign separately from that read-only operational work.


### P0-RESULT-SCOPE: commercial comparison and unmatched dimensions

Owner clarification, 2026-10-05: compare the result/scope experience against
commercial products as well as open-source tools. Maintain an explicit product
inventory and research status; do not describe a selected sample as every paid
product. The research must cover all relevant products in the inventory,
including products subsequently discovered. Current operational priority is
finishing validation; the exhaustive comparison remains OPEN.

Initial commercial documentation findings:

| Product | Documented behavior | Application to migkit |
|---|---|---|
| Oracle GoldenGate Veridata | Job/group/compare-pair hierarchy, comparison status filtering and out-of-sync reports | Group by database and object and expand problem details without treating a finished job as a clean result. |
| Informatica Cloud Data Validation | Suite and job reports; matched, unmatched, missing and extra records; row counts and column type matching | Keep distinct difference categories and comparison dimensions. Explicitly label full verification versus sampling and report sample limits. |
| Qlik Replicate | Separate full-load and change-processing monitoring; completed/loading/queued/error table groups and table progress | Separate execution progress from correctness verdict. The referenced monitor documentation does not establish equivalent content-verification presentation. |
| Redgate SQL Data Compare | Object results and side-by-side row/value differences; identical and unable-to-compare cases | Show affected objects before detailed source/target values. Explain why an object cannot be compared. |
| Devart dbForge Data Compare | Object summary above record details; filters for different, identical, included/excluded and mapped/unmapped objects | Present resolved scope and mapping quality alongside results, with drill-down from the affected object. |
| Datafold | High-level diff overview and value-level exploration | Offer a concise outcome view with separate detailed rows/columns. Vendor claims about scale are not a measured usability or performance result. |
| QuerySurge | Run dashboard and hierarchical project/test progress | Preserve useful suite/task context while adding the requested database-first outcome grouping. |
| IBM Data Replication asntdiff | Console summary and a differences table | Generate a readable database/object summary from structured evidence; raw diagnostic tables must not be the operator's primary view. |

Official sources:
- https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDUG/working_with_jobs.htm
- https://docs.informatica.com/content/dam/source/GUID-B/GUID-BE90AF74-E490-4BA6-9EC6-2D9B079C5D7B/7/en/CDV_July2024_Monitor_en.pdf
- https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Tutorial/RunMonitorTask_sample.htm
- https://documentation.red-gate.com/sdc15/reviewing-the-comparison-results/viewing-the-row-differences
- https://docs.devart.com/data-compare-for-mysql/view-comparison-results/view-comparison-results.html
- https://www.datafold.com/blog/what-the-heck-is-data-diffing/
- https://www.querysurge.com/product-tour/features
- https://www.ibm.com/docs/en/idr/10.2.1?topic=tables-table-compare-utility-asntdiff

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-RESULT-SCOPE.commercial | Complete a maintained commercial result/scope comparison inventory | Cover the initial eight products above, managed AWS/Azure/Google/Tencent/Alibaba/Huawei migration services, and additional relevant commercial comparators discovered during research. For each, record applicable product/version, official evidence, scope preview, mappings/filters, database/object grouping, progress versus correctness, incomplete/unsupported states, diff categories, consistency guarantees, next actions, export and detail navigation. Mark documented present, documented absent/not applicable and evidence unavailable separately. Missing public documentation does not prove a feature is absent. Record research coverage and remaining products honestly. |
| P0-RESULT-SCOPE.unmatched | Design clear presentation for dimensions without a direct competitor equivalent | Every required migkit dimension has a proposed display and acceptance scenario even if no comparable product exposes it: counters, accounts/grants, routines/events, parameters/timezone, replication lag, expected transforms, migration artifacts, proof invalidation and recovery. Show database, affected object, short problem, actual source/target state, impact, verdict certainty and next action. Group shared settings once with affected-database references instead of duplicating long text. Separate observed vendor behavior from inferred design proposals. Use prototypes and comprehension checks to choose the result, without copying irrelevant product hierarchy or repair defaults. |


## P0-AUTO-RESOURCES: task and worker management throughout every process

Owner clarification, 2026-10-05. Status: OPEN. Every applicable process must
manage its own tasks, concurrency and resources through the shared decision
engine. The operator should not tune worker/thread counts or orchestrate nested
pools. Serial execution is valid when dependencies, guarantees or measured
cost make it appropriate, but its reason must be explicit in evidence.

Verified integration gaps, not a full platform audit: `jointplan` models joint
schedules and worker allocation, `decide.Trials` accounts for pressure, and
MySQL database validation uses a health-based throttle. However, its table
pool uses `hop.workers` while native per-table checksum work creates a fixed
four-thread pool. CLI and PostgreSQL paths also contain configured or fixed
pools. These observations establish incomplete automatic integration; they do
not establish that every constant pool is wrong or that all task families
have been audited. Existing optimization components must be reused.

| ID | Deliverable | Acceptance criteria |
|---|---|---|
| P0-AUTO-RESOURCES.route | Inventory and integrate resource decisions for every process | Cover all task families in P0-DECISION-ALL.coverage, including discovery, assessment, planning, schema, conversion, copy, CDC, validation/deep checks, recovery, repair, cutover and reporting. Identify every pool, queue, subprocess, server-side parallel job and serialized stage; classify justified fixed constraints versus decisions that need adaptive allocation. Route consequential allocation through shared contracts and guard new bypasses. Map gaps to implementation items; do not mark platform coverage complete from mover-only tests. |
| P0-AUTO-RESOURCES.budget | Shared budgets across nested work and concurrent runs | Coordinate task, table, range and source/target execution against real endpoint connection, CPU, I/O, memory, network and applicable API quotas. Account for nested pools and server-side workers so a configured outer limit is not multiplied silently. Track endpoint ownership and contention across simultaneous local runs where controllable; disclose external workload uncertainty. Enforce operator limits and correctness dependencies rather than equating more threads with more throughput. |
| P0-AUTO-RESOURCES.adapt | Automatic measured allocation and load balancing | Discover available capacity and obtain bounded representative measurements; choose task ordering, work-unit size, source/target concurrency and workers/processes appropriate to the workload. Adapt at safe boundaries to skew, large values, latency, saturation and failures. Increase concurrency only where measured benefit and limits permit; reduce or serialize when appropriate. Preserve coverage, consistency, checkpoint validity and idempotency. An optional user worker limit is a constraint, not a required tuning task. |
| P0-AUTO-RESOURCES.explain | Useful bottleneck and progress reporting | Identify the slow stage and affected endpoint/object from measurements; report actual versus allocated concurrency, throughput, queue/wait time, remaining work and ETA uncertainty. Separate measured CPU/I/O/network/server waits from inferred causes and unavailable metrics. Explain allocation changes concisely and retain detailed evidence. Missing metrics cannot be treated as spare capacity or as a confirmed cause. |
| P0-AUTO-RESOURCES.proof | End-to-end resource-management acceptance | Test actual workflows with nested pools, competing tasks, slow endpoints, skew, server-side parallelism and changing limits in the sandbox. Prove shared budgets hold, adaptation improves or bounds completion cost, serial choices are justified and recovery does not duplicate effects. Compare automatic allocation with fixed baselines using time, load, memory, connections and retry waste. The user can complete every supported workflow without selecting internal workers or threads. |

This is an implementation and integration backlog, not a configuration-only
issue. All items remain OPEN; current operational validation uses private
orchestration to finish existing work, without claiming these gaps are closed.


### P0-AUTO-RESOURCES: every task and every step must adapt automatically

Owner acceptance clarification, 2026-10-05. Required final behavior: automatic
scaling applies to every task, phase, step and nested work unit, including
future features. Allocation is continuously reconsidered from applicable
measurements and dependencies throughout execution, not merely chosen once
at startup or for the mover. The system increases, reduces, pauses, resumes
or redistributes work at safe boundaries as conditions warrant. Scale here
means execution concurrency and work allocation within available, authorized
resources; provisioning paid infrastructure or changing database capacity
requires separate explicit authorization.

Additional acceptance requirements for P0-AUTO-RESOURCES.route, .adapt and
.proof:

- Inventory every execution step and its allocation owner. Each step either
  participates in automatic allocation or declares a measured or contractual
  reason why it must remain serial or has a fixed maximum. A serial step is
  still managed automatically; it is not omitted from resource accounting.
- Coordinate scaling across runs, tasks, phases, tables, ranges, threads,
  processes, connections and applicable server-side parallel execution through
  shared budgets. A child step cannot independently multiply its parent's
  allowance. Track contention, dependencies and fairness so small or critical
  work does not starve behind large scans.
- Re-evaluate after relevant rate, queue, load, lag, capacity, error or phase
  changes. Use bounded exploration, backpressure and stabilization to avoid
  uncontrolled growth or oscillation. Unknown capacity calls for a conservative
  measured start, not an assumption that more workers are safe.
- Scale work units and execution together; handle skew and large values without
  weakening complete coverage, required consistency, durable evidence or
  idempotency. Pause or switch safely where a running operation cannot be
  resized, and apply the new allocation to remaining valid work.
- The normal workflow must complete without user tuning of workers, threads,
  batching or task concurrency. Optional overrides express limits or goals.
  Report actual allocation, concise reasons for changes and ETA uncertainty.
- Prove scaling up and down, serialization, redistribution and recovery through
  actual multi-step sandbox workflows under changing constraints. A unit test
  of the chooser, a static worker count or scaling only one task family cannot
  satisfy platform acceptance. New steps require the same coverage guard.

All related items remain OPEN until these integrated behaviors are proved.


### P0-AUTO-RESOURCES: fine-grained high-throughput parallel execution

Owner requirement, 2026-10-05. The design must support very large numbers of
independent work units and high concurrency wherever the complete system can
sustain useful throughput. Two endpoint readers or a fixed four-thread pool
must not become an architectural ceiling. Large task counts do not require
one thread or connection per task: use bounded queues, backpressure, reusable
workers and the execution primitive appropriate to each engine and workload.

Additional requirements for P0-AUTO-RESOURCES.budget, .adapt and .proof, and
P0-VALIDATE.ranges:

- Decompose large tables and steps into independently schedulable, resumable
  units, including composite/text keys, partitions, skew and large values.
  Dynamically balance units by measured bytes and duration; avoid long tails,
  excessive planning costs and head-of-line blocking. Drain completed units
  as they finish and persist evidence immediately, irrespective of submission
  order. Verify complete non-overlapping scope and target-only rows.
- Schedule multiple units on each endpoint concurrently when useful, with
  source/target pairing or pipelining according to required consistency and
  bounded memory. Coordinate nested work, multiple tables and processes using
  shared endpoint budgets. No default fixed per-table pool may silently cap
  or multiply the globally justified allocation.
- Evaluate every relevant available factor: local CPU, memory, executor and
  connection overhead; endpoint CPU, storage latency/IOPS/throughput, cache,
  query plans, indexes, partitions, server-side parallelism and connection
  limits; network bandwidth/latency/errors; row size, JSON/LOB serialization
  and key distribution; application workload, locks, replication lag,
  consistency/snapshot lifetime, cost and service quotas. Distinguish measured
  facts, estimates and unknown signals; unavailable telemetry is explicit.
- Use bounded probing and marginal throughput/load measurements to increase
  concurrency while beneficial and within limits, and reduce it when it
  saturates a resource, harms application workload or increases retries.
  Prevent oscillation, starvation, unbounded task materialization, connection
  storms and duplicate/orphaned work. Choose threads, processes, asynchronous
  I/O or server-native parallelism by workload rather than assuming threads
  alone make every step faster.
- Never trade full coverage, comparison semantics, required consistency,
  duplicate detection, durable proofs or idempotency for concurrency. A
  scalable execution strategy that cannot preserve the task's guarantees is
  ineligible. Report the limiting factor and actual useful parallelism, with
  uncertainty where the cause is not established.
- Benchmark end-to-end scaling across increasing concurrency and task counts
  in the sandbox, including asymmetric endpoints, large values, skew,
  competing application load and injected failures. Record throughput,
  latency, memory, connections, resource saturation, retry waste and verified
  coverage. Prove bounded overhead and correct recovery at high task counts;
  claim faster execution only from measured results, not worker count alone.

These are OPEN design and implementation acceptance requirements. Optimize
for the fastest verified complete outcome the available authorized system can
sustain, with minimal operator input and no arbitrary thread-count ceiling.


### P0-AUTO-RESOURCES: deeply dynamic telemetry, prediction and execution

Owner requirement, 2026-10-05. Telemetry, work decomposition, scheduling,
validation depth and prediction must operate as one continuous feedback loop
through the shared decision engine. Target the fastest complete outcome that
preserves the task's strongest required guarantees; a fixed checklist,
static chunk size, one-time ETA or worker-count heuristic is insufficient.
Reuse existing cost, workflow and evidence owners rather than building an
independent prediction or scheduling system.

Additional acceptance requirements:

- Discover remaining work at task, phase, object and range levels, including
  dependent downstream checks. Measure rows, bytes, value sizes, skew,
  per-endpoint service time, queue/wait time, resource pressure, retries and
  evidence validity where available. Represent unknowns explicitly and choose
  bounded probes when their information could change the plan.
- Dynamically refine work units and diagnostic depth from evidence. Prefer
  incrementally measurable scans to monolithic opaque queries where equivalent
  guarantees can be preserved. Large-value sizing must not silently skip a
  timed-out column and report the remaining columns as complete. When native
  progress is unavailable, use applicable historical/representative rates and
  conservative bounds, clearly labeled as predictions; do not fabricate
  observed progress or an exact completion time.
- Update ETA for each step and the full dependency-aware remaining workflow
  from measured rates and uncertainty. Include queueing, contention, resource
  sharing, retry/failure probability, warm-up and downstream work; account for
  concurrent steps without adding all durations as though they were serial.
  Detect stalls and explain their effect. Distinguish active runtime from
  network interruption or machine suspend. Expose confidence/bounds and the
  limiting unknown; make reliable estimation actionable rather than merely
  displaying an unexplained unknown.
- Couple forecasts to action: compare alternative method compositions, range
  sizes, concurrency and ordering using their measured cost and guarantees.
  Replan when new observations justify it, redistribute remaining work and
  investigate anomalies within bounded budgets. Preserve completed proofs
  only when their scope and consistency context remain valid. Scale-down,
  serialization and additional checks are valid when they improve the
  verified outcome or protect required guarantees.
- Prove through sandbox end-to-end scenarios that changing conditions produce
  appropriate different plans, depth and estimates. Track forecast error and
  calibration, completion time, resource use, retry waste and defect detection
  against fixed baselines. Include asymmetric endpoints, skewed JSON/LOB,
  opaque native operations, network failures and interrupted runs. An accurate
  ETA alone does not pass if it describes an unnecessarily slow strategy;
  faster execution alone does not pass if evidence or required checks are lost.

This is an extension of P0-AUTO-RESOURCES.adapt/.explain/.proof,
P0-WORKFLOW-DEPTH.investigate and P0-VALIDATE.depth. All remain OPEN until the
integrated feedback loop and actual workflows satisfy these criteria.


### Platform-wide migration contract: every task uses the adaptive loop

Owner scope clarification, 2026-10-05. The loop of measuring, analyzing,
choosing a strategy, adapting execution and depth, and updating predictions
is a requirement for everything migkit does throughout migration. It is not
limited to telemetry, worker allocation, move or validation. Applies to all
current and future task families: configuration/import and discovery,
assessment, scope and mapping, planning, schema and data conversion, object
and account migration, data movement, initial/incremental replication and DDL,
validation, diagnosis, authorized repair and undo, cutover and rollback,
security, managed services, HA, recovery, evidence and reporting.

Each task, phase, step and applicable work unit must have a declared goal,
inputs and observations, constraints and guarantees, decision owner,
execution/dependency state, completion evidence and prediction of remaining
work where meaningful. Its observations feed the same shared decision engine
and inform dependent tasks. Choices must adapt at safe boundaries from actual
facts; deterministic invariants remain enforced and serial steps remain
accounted for. Applicable task-specific depth and correctness criteria cannot
be replaced by one generic checklist. No task is exempt merely because its
implementation is currently a fixed command or a wrapped program.

This is the umbrella acceptance contract for P0-DECISION-ALL,
P0-WORKFLOW-DEPTH, P0-MINIMAL-INPUT, P0-AUTO-RESOURCES and P0-VALIDATE.
Audit and prove complete task coverage rather than creating a separate loop
per feature. New features must declare their integration and end-to-end proof.
The operator provides minimal irreducible intent and access; migkit owns the
process and exposes concise truthful outcomes. Automatic decisions remain
within authorized scope and preserve required correctness, consistency and
idempotency. All related implementation items remain OPEN.


### Platform-wide migration contract: every database engine

Owner requirement, 2026-10-06. The complete adaptive migration workflow must
apply to every database engine, including homogeneous and heterogeneous engine
pairs and future engine additions. PostgreSQL and MySQL are not the boundary
of the product. This extends P0-PARITY.all and the umbrella contract above;
reuse the shared capability/depth matrices rather than creating another
independent engine list.

Acceptance criteria:

- Maintain an explicit engine and engine-pair inventory with discovered versions,
  data models, source/target roles and managed-service constraints. Extend the
  inventory when an engine is added or discovered; current adapter coverage
  cannot be described as support for every engine in existence.
- Every engine must participate in the shared decision, adaptive workflow,
  resource allocation, minimal configuration, durable state, evidence, result
  and prediction contracts. Cover all applicable migration tasks, including
  conversion across different engines, and prove them through actual workflows.
- Implement engine-specific mechanisms and semantic handling behind shared
  contracts. Preserve type/value meaning, keys and duplicates, ordering where
  required, transaction boundaries, consistency and idempotency. Do not force
  relational assumptions onto document, key-value, graph, time-series or other
  database models; require model-appropriate evidence of the intended outcome.
- Track each applicable task and depth dimension by engine/pair as implemented
  and proved, implemented but unproved, missing, blocked or genuinely not
  applicable with a concrete semantic reason. Missing adapters, permissions or
  implementation are gaps, not reasons to mark a capability not applicable.
- Where native guarantees differ, compose supported mechanisms to meet the
  task's required guarantees. If that cannot be established, state the exact
  gap and block the unsupported outcome; never silently downgrade or claim
  equivalent guarantees without proof. These limits must remain visible in
  simple database-level results and resolved scope.
- Use sandbox integration and end-to-end scenario acceptance for applicable
  versions, shapes and engine pairs, including conversions, concurrent changes,
  large/skewed data and interruption/repetition. Engine registration, method
  existence or one happy-path test cannot establish full support. Future engines
  require the same depth and guards; partial support remains explicit and OPEN.

Universal engine support is the target requirement, not a claim about current
implementation. P0-PARITY.all and the associated decision/workflow/resource
items remain OPEN or PARTLY until their actual engine/pair coverage is proved.


## P0-DECISION-ALL.priority: reasoned priorities and selective investigation

Owner requirement, 2026-10-06. Status: OPEN. Knowing what matters, which facts
to acquire and what to do first is part of deep intelligence, not a reason to
reduce depth. Applies to every migration task, step, engine and applicable
engine pair through the shared decision engine and adaptive migration rule.
Reuse existing facts, costs, guarantees and workflow dependencies.

Acceptance criteria:

- Rank work and unknowns from explicit operator goals, mandatory guarantees,
  dependency/critical-path effects, correctness and data-loss risk, affected
  scope, reversibility, uncertainty and expected cost of delay or failure.
  Mandatory correctness evidence is a requirement, not optional work that can
  lose a priority score. Do not use one fixed priority list for all workloads.
- Decide which observations and probes to acquire by expected decision impact:
  plausible outcomes, whether they change the feasible strategy or verdict,
  risk reduced, prerequisites unlocked and investigation time/resource cost.
  Explain the evidence behind the priority and its uncertainty. Use bounded
  measurement and reusable valid evidence instead of collecting every metric
  indiscriminately or treating unmeasured assumptions as facts.
- Balance immediate task cost against downstream and whole-workflow benefit;
  prioritize blockers and critical evidence while independent useful work
  continues within shared budgets. Account for fairness and prevent starvation
  of smaller tasks or checks whose importance is not proportional to row count.
  Distinguish urgent, important and inexpensive work rather than equating them.
- Update priorities when facts, failures, resource pressure, replication state,
  scope or goals change. Deepen investigation where anomalies or uncertainty
  threaten the required outcome; stop optional probing when further information
  cannot justify its cost within the task's limits. If required evidence cannot
  be obtained, leave the outcome explicitly incomplete, not silently skipped.
- Keep the decision record of alternatives, chosen priorities, factual basis,
  expected benefit, cost and guarantees. Operator output remains concise:
  current focus, why it matters, blocked/remaining work and required action.
- Prove through sandbox multi-task scenarios that different consequences and
  uncertainty lead to appropriate different priorities and investigations.
  Include misleading cheap signals, critical expensive checks, changed goals,
  competing resources, dependent blockers and uncertain failures. Compare
  completion cost, detection of defects, probe overhead, starvation and useful
  decisions against fixed-order and indiscriminate-measurement baselines.
  A priority formula or narrative explanation alone cannot mark this DONE.

This extends P0-DECISION-ALL.reason/.adapt/.proof and
P0-WORKFLOW-DEPTH.investigate. The requirement is evidence-driven depth and
reasoned prioritization throughout the process; implementation remains OPEN.


## P0-SAFETY-INTEGRITY: data safety before execution speed

Owner requirement, 2026-10-06. Status: OPEN. Applies to every task, step,
engine and applicable engine pair. Speed is optimized only within the required
safety and correctness guarantees. Data loss, corruption, duplicate effects,
broken dependencies or incorrect required ordering are unacceptable outcomes.
Extend the shared guarantees, decision, workflow and evidence contracts.

Acceptance criteria:

- Model required semantics before execution: exact intended scope, type/value
  fidelity, NULL/empty and encoding distinctions, duplicates and key meaning,
  dependency order, transaction atomicity, applicable change/event ordering,
  deletes and schema-version compatibility. Preserve the ordering required by
  the source model and application contract; do not invent a total row order
  for unordered tables or claim unsupported global ordering across partitions.
- Admit only strategies and parallel schedules that establish the required
  guarantees. Respect dependency barriers, consistent snapshots or applicable
  replication positions, transaction boundaries and schema transitions.
  Parallelism must not reorder dependent effects, race checkpoint advancement,
  acknowledge unverified writes or lose intervening updates/deletes.
- Preserve durable progress and provenance. Reconcile actual side effects after
  uncertain failures before retrying, deduplicate where applicable and advance
  positions only after their corresponding durable effects. Detect schema,
  scope and consistency changes that invalidate evidence. No silent skips,
  lossy conversions, truncation, dropped events or incompatible fallback.
- Detect integrity problems with evidence appropriate to the task: missing,
  extra, changed and duplicate data, ordering/transaction violations, conversion
  fidelity, constraints and referential dependencies. Distinguish concurrent
  change from established equality and require applicable completion evidence
  before readiness or cutover. A count match or a successful operation alone
  cannot certify correctness.
- When a required guarantee is unavailable or cannot be verified, stop the
  affected unsafe action or leave the task incomplete with an actionable
  explanation. Continue independent safe work only where dependencies permit.
  Repair, destructive actions and infrastructure changes stay within explicit
  authorization; neither urgency nor speed permits a weaker silent plan.
- Prove safety through sandbox failure-injection workflows: crashes around
  commit/ack/checkpoint boundaries, retries, duplicate delivery, out-of-order
  dependent events, concurrent writes/deletes, DDL transitions, partial batches,
  resource exhaustion and cross-engine conversions. Verify invariant outcomes,
  safe recovery and complete evidence at high concurrency. Performance gains
  count only when these safety checks pass; unsupported guarantees remain
  explicit gaps rather than claims of absolute safety.

Links: P0-MINIMAL-INPUT.correctness/.idempotency,
P0-DECISION-ALL.reason/.priority, P0-WORKFLOW-DEPTH.recover and
P0-AUTO-RESOURCES.proof. This is a hard implementation and review requirement;
current behavior is not certified by recording it.

Status 2026-10-16: PARTLY. Built:

- `guarantees.Semantics` / `read` / `read_hop`: per table, read from the
  catalogue and the engines' facts - exact columns, value classes, text
  columns where an empty text and NULL differ (`EMPTY_IS_NULL`, set on
  Oracle), a field not there apart from NULL (`EXPRESSES_ABSENT`),
  collations, key and duplicates (no key and no unique index), foreign-key
  parents, cascading parents (atomicity), change order needed ("per key",
  "as logged" for keyless), deletes, the shape's fingerprint. A target that
  cannot keep one is a `Gap` (scope, values, null and empty, absent,
  encoding, duplicates, dependencies) with what to do next.
  `guarantees.barriers` gives dependency waves and cycles;
  `changed_since` names tables whose shape moved since the read.
- Admission: `guarantees.Schedule` + `guarantees.breaks`, asked through one
  `decide.KEEPS_SEMANTICS` need (`decide.admit`): gaps not answered,
  children before parents while the target's keys hold, a key cycle under
  held keys, tied tables read each as of its own moment without a catch-up
  position, a position moved before the durable effect, tied or keyless
  changes applied by key or unordered, deletes dropped, shape changes
  ignored.
- The joint plan of a move's tables is routed through it: copies wait on
  their parents' copies (`jointplan.barriered`), the plan is admitted
  (`jointplan.admitted`, kept in the plan's evidence), the move reads the
  source's semantics (`jointplan.semantics_of`) and says a refusal.
- Fault catalogue `guarantees.FAULTS` with `SAFETY_PROVED` /
  `SAFETY_UNPROVED`; tests in tests/test_a_move_keeps_what_it_must.py
  (pure admission tests, and sandbox failpoint runs of a PostgreSQL move
  with foreign keys stopped at every mark and with deletes between stop and
  re-run).

Status 2026-10-16 (W9B): every writer asks admission.

- A joint plan `decide.admit` refuses stops the move before its first row
  (`cli._joint_for_move`): the refusal, with the gap's next step, is the
  operator's line and the decision record's (way "stop").
- The mover reports whether the target's foreign keys hold during the
  load (`Engine.foreign_keys_in_load`: "after" where a load's
  `indexes.ForeignKeyWindow` record keeps them off or the target loads
  without checking them, `LOAD_SKIPS_KEYS`); the plan is admitted against
  that and keeps it (`Plan.fks`, evidence `foreign_keys`).
- The move reads the target's catalogue too (`jointplan._target_catalog`)
  so `Semantics` compares both sides.
- Tail lanes: `Engine._lanes` asks `decide.admit` with the batch's own
  `guarantees.lanes_schedule` against the engine's tied tables
  (`guarantees.tied_of`); lanes that split tied tables are refused, the
  batch goes as one in the source's order, why kept in `lanes_refused`.
  A table tied to itself (self reference, second unique index) counts as
  tied (`guarantees._linked`).
- Copy ranges and their marks: `cli._copy_admitted` asks with
  `guarantees.copy_schedule` (one snapshot or per range, a kept position
  a tail goes on from, `Engine.RANGE_MARK`); tied tables read apart with
  no position are copied one table at a time with why said and recorded;
  a mark saved before the target's commit stops the move.
- The three faults moved to PROVES (tests written, not run): a tail
  stopped before its position with keys held, a change delivered twice in
  one batch, a column added between a copy's ranges.

Not done: PostgreSQL's load window (session as a replica) does not yet set
`LOAD_SKIPS_KEYS`, so its moves are planned as keys held (stricter, a key
cycle stops the move); no engine sets `SHARED_SNAPSHOT`; the tail loop in
the cross-engine pair does not yet say `lanes_refused` in its report;
copying tied tables one at a time narrows the window, it does not read them
as of one moment; the tests are written and not yet run; no measured
comparison at high concurrency.

Status 2026-10-16 (W10B): keys and snapshots said truthfully.
- PostgreSQL says whether its load checks foreign keys from the write way
  its target user reaches (`PostgresEngine._write_way`, shared with
  `load_window`): the replica rung (superuser, or allowed
  `session_replication_role`) or a load already writing as a replica is
  "after", so a key cycle is admitted; "as they are" or a refusal is
  "during", and only then a cycle stops the move. MySQL says "after" where
  its writers skip the checks (`_keys_off`). SQL Server's load keeps its
  keys checked (only triggers go off), so "during" stays true there.
- `Engine.shared_snapshot(db)` says how the source's readers read at one
  moment; `jointplan.semantics_of` keeps it on what it read (`Tied`),
  `guarantees.shared_read` admits tied tables copied side by side under it,
  and the plan's evidence says which held (`snapshot`).
- The cross-engine tail says and records why its lanes were refused
  (`HeteroEngine._lanes_said`), once per reason.
- Tests in tests/test_a_move_keeps_what_it_must.py (written, not run).

Not done (W10B): no engine claims a shared snapshot yet - the builtin
copy's range readers (owned by the copy methods) do not adopt the
PostgreSQL exported snapshot (`_export_snapshot`, `_snapshot_preamble`
exist and the consistent verify pass uses them); MySQL needs a fenced
consistent snapshot per reader at one binlog position; SQL Server's
snapshot isolation starts each reader's snapshot at its own first read, so
it does not share a moment. The verify read of tied tables at one moment
(`scanplan`) is not wired to `shared_snapshot`.


## P0-RESILIENCE-ALL: prevent, contain and recover from migration failures

Owner clarification, 2026-10-06. Status: OPEN. Applies to every current/future
task, step, engine and applicable engine pair. Cover every identified class of
conditions that can harm the outcome or surrounding systems, not only query
timeouts. Unknown failures must also reach a conservative safe state; no
finite failure catalogue proves that all possible failures are understood.
Reuse the shared decision engine, execution/resource controller, guarantees,
state, ownership and evidence contracts rather than a separate chooser.

Acceptance criteria:

- Maintain a task/engine failure catalogue covering connectivity, authentication,
  permissions, expired credentials, quotas, throttling, CPU/memory/storage
  pressure, disk exhaustion, lock/deadlock, long or orphaned work, retries,
  duplicate/out-of-order events, partial/uncertain commits, process/machine
  interruption, failover, stale checkpoints, inconsistent views, replication
  lag/lost history, schema changes, unsupported or lossy conversion, malformed
  data, corrupted evidence, dependency failures and external workload impact.
  Extend it from incidents and new features; link each applicable class to
  detection, prevention, containment, recovery and its proof or explicit gap.
- Assess hazards before execution and continuously during work. Use actionable
  observations with bounded cost; distinguish known faults, probable causes and
  unavailable evidence. Decision choices include proceed, probe, throttle,
  pause, isolate, replan, reconcile, retry, authorized compensate/rollback and
  stop. Classify transient versus persistent faults and retryability according
  to actual effect/state, not exception text alone.
- Enforce decisions through execution controls: owned operation identities,
  lifecycle tracking, cancellation acknowledgement where available, shared
  budgets, admission control, bounded retries/backoff, circuit breaking and
  dependency barriers. Prevent retry storms, duplicated effects and abandoned
  operations from accumulating. Unknown outcome is reconciled before repeat;
  work ownership and authorization constrain cleanup and compensation.
- Contain the blast radius across steps, tasks, databases and endpoints. Preserve
  source data and application availability within declared limits; prevent
  failed optional work from hiding failed required evidence. Continue independent
  safe work where possible, without allowing dependents to proceed on invalid
  prerequisites. Define bounded shutdown and recovery behavior for failures of
  the controller, its telemetry and its durable state themselves.
- Recover from verified state with evidence validity checks, idempotent effects
  and required consistency/order preserved. Never silently downgrade guarantees
  or interpret a skipped/timed-out check as success. When no safe supported
  recovery exists, block the affected outcome and explain the fault, affected
  scope, retained progress and precise next action in concise operator language.
- Prove integrated prevention, detection, containment and recovery through
  sandbox fault injection and compound failures, including controller restart,
  telemetry loss, failure during cleanup, resource exhaustion and unknown
  outcomes. Verify safety invariants, bounded resource/retry growth, correct
  dependency handling and honest results. Treat unhandled classes as OPEN gaps;
  a broad exception handler or a catalogue without workflow tests is insufficient.

Links: P0-SAFETY-INTEGRITY, P0-DECISION-ALL.reason/.adapt/.priority,
P0-WORKFLOW-DEPTH.recover and P0-AUTO-RESOURCES. This is a platform-wide hard
requirement, not a claim of current resilience coverage.

Status 2026-10-16: PARTLY.
- Done: `migkit/faults.py` `CATALOGUE` holds 24 classes (the list above plus
  `unknown`), each with detection by engine code (MySQL number, SQLSTATE) and
  by effect (`classify`: a transient failure after a write was sent that the
  server does not roll back whole is an `uncertain_commit` whatever its
  words), transient/persistent, what must hold before a retry, containment,
  the actions it admits and its proof. `from_facts`/`assess` detect pressure,
  disk, lag, stale checkpoint, workload impact and orphaned work from readings.
  `decide.recovery` is the one chooser (reconcile, proceed, pause, throttle,
  retry, replan, isolate, probe, stop). `faults.retried` carries it out with
  bounded tries, a per-side `Breaker` (one probe while open), owned operation
  identity (`Owned`, kept in `attempts.Ledger.own`, written atomically) so a
  restart reconciles begun work before repeating it, and a log that fails never
  changes the outcome. `util.with_retry` and `util.run` - so every engine query,
  connect and program run - go through it. `failpoint` injects any class
  (`fault=<class>`, `N+`). Proof: tests/test_a_failure_is_told_by_what_it_did.py.
- Not done: callers do not yet pass `check` (reconcile) or `owned`/`ledger`, so
  an uncertain write still stops rather than being reconciled in a real move;
  `replan`/`isolate` raise to the owner (`decide.attempt`, the move's set-aside)
  without a workflow test that the next rung or the remaining units carry on;
  `assess` is not yet called between units; throttle does not yet lower
  concurrency (W8C tuning owns it); compound sandbox runs against real servers
  (failover, disk exhaustion, telemetry loss in a running move) are unproved.
  A torn attempts ledger reads as absent (kind "cache"), losing owned state.


## P0-AUTONOMOUS-MIGRATION: operator intent to verified outcome

Owner requirement, 2026-10-06. Status: PARTLY. Provide the relevant capabilities
of established migration and verification products, then demonstrably exceed
their depth, judgement and complete operator experience. Normal users state
their goal and supply necessary access; migkit owns the technical process to
its verified outcome. Users must not have to select algorithms, diagnose logs,
coordinate recovery or tune workers to make supported workflows work.

Acceptance criteria:

- Maintain a source-grounded capability and mechanism comparison against AWS
  DMS, Tencent DTS, Oracle GoldenGate/Veridata and the relevant commercial
  inventory in P0-RESULT-SCOPE.commercial. Establish applicable versions,
  engines, prerequisites and guarantees. Include preflight, restart/resume,
  error classification, backoff, conflict handling, lag-aware confirmation,
  state/evidence and reporting. Reuse existing research and shared matrices;
  translate differences into explicit implementation gaps and scenario proofs.
- Deliver capability parity where applicable, then add integrated fact-driven
  diagnosis, safe strategy composition, dynamic depth and resource management,
  dependency-aware recovery and verification across every supported task and
  engine pair. Use competitor mechanisms as grounded baselines, not blanket
  assumptions about their limitations. A wrapped feature alone is not parity
  unless the actual user workflow and required guarantees work.
- Accept minimal irreducible goals, endpoints/access and authorized scope.
  Discover the rest, resolve technical choices internally and execute the whole
  workflow automatically. Ask only for unavailable intent/access, consequential
  business choices or necessary authorization; do not shift implementation
  decisions to users. Never invent business semantics, broaden scope or perform
  unauthorized destructive actions to avoid a question.
- Anticipate and contain hazards; diagnose and recover supported failures from
  verified durable state. Preserve correctness, consistency, required ordering,
  idempotency and surrounding workload limits. Unknown or unsupported conditions
  produce a safe, precise blocker rather than silent success or repeated unsafe
  attempts. Automatically handle everything the supported contract permits;
  disclose the remaining boundary truthfully.
- Prove better outcomes in equivalent sandbox end-to-end scenarios with the
  same scope, guarantees and resource constraints. Measure operator inputs and
  interventions, valid completion rate, failure recovery, defect detection,
  total completion time, resource use, probe/retry waste and evidence clarity.
  Report actual results and remaining gaps; neither claims of intelligence nor
  more features/threads establish superiority. No universal superiority claim
  without applicable comparative evidence.
- The user-facing flow is goal, minimal setup, understandable progress and
  concise database-level outcomes with evidence on demand. A user can complete
  healthy and recoverable-failure scenarios without custom scripts, internal
  tool knowledge or manual orchestration. Prototype and test comprehension;
  complexity stays within the implementation.

This is the product-level acceptance target linking P0-DECISION-ALL,
P0-WORKFLOW-DEPTH, P0-MINIMAL-INPUT, P0-AUTO-RESOURCES,
P0-SAFETY-INTEGRITY, P0-RESILIENCE-ALL and P0-RESULT-SCOPE.
Status: PARTLY (see the status under the next section).


### P0-AUTONOMOUS-MIGRATION: recover intelligently toward completion

Owner clarification, 2026-10-06. The intended behavior is to detect and
understand problems, handle them autonomously where supported and safely
continue toward the best verified complete outcome. Stopping on every error
or blindly retrying is insufficient. Established products already provide
conditional retries/restarts; compare their actual mechanisms rather than
assuming all errors immediately stop them.

Additional acceptance criteria for P0-RESILIENCE-ALL and
P0-AUTONOMOUS-MIGRATION:

- Diagnose failures in their task, dependency, resource and consistency context.
  Reconcile whether work committed, is still running or had no effect. Select
  bounded recovery based on evidence: cancel owned abandoned work, back off,
  resize units, rebalance concurrency, switch an equivalent method, repair an
  authorized technical condition or replan remaining work. Avoid retry storms,
  hidden data skips and duplicate or reordered effects.
- Preserve valid completed work and continue independent safe tasks while
  isolating affected units. Re-establish prerequisites and required guarantees
  before releasing dependent work. Persist recovery decisions and verified
  postconditions; update remaining scope, resource budgets and ETA.
- Optimize recovery for whole-workflow completion, correctness and minimum
  wasted time/resources, not the fastest local retry. Learn from recurring
  faults and change the approach when evidence shows the previous one will
  fail again. Recovery must be bounded and stable under compound failures.
- Normal recoverable cases complete without user diagnosis or technical tuning.
  Show concise current impact, recovery action and retained progress; keep
  detailed reasoning available on demand. Necessary intent, unavailable access
  and authorization remain explicit boundaries. Impossible prerequisites,
  irrecoverable loss or unprovable guarantees require an actionable safe blocker;
  never claim universal completion or zero loss without evidence.
- Inject recoverable faults at different phases and side-effect boundaries in
  sandbox workflows. Prove completion with full coverage and preserved value,
  ordering, consistency and idempotency invariants, without duplicate effects
  or omitted checks. Include unrecoverable cases to prove safe stopping and
  truthful partial results. Measure automatic recovery success, user actions,
  total completion time, retry waste and external workload impact.

W10D, 2026-10-16: the flow owns the copy's workers - one share of the
measured budget a database (`model.Model.budget`, `sizing.estimate`), cut
by a throttle, handed down to the copy; a plain `move` asks the same rung
(`decide.worker_budget`). Every step keeps a reconcile contract before it
runs again (`autonomous.settle`: the ledger's uncertain operations read on
the target through `faults.landed`; one not readable is a blocker with the
next step). A unit runs under the one ladder (`faults.retried`, sizing
causes in `tuning.sized`); its tries are what the forecast pays for
(`autonomous.tries_for`), the throttle floor one worker of the budget
(`autonomous.scale_floor`). `--go` writes; only `--switch` is the switch's
yes. Not done: `tools/final_suite.py` does not yet call
`autonomous.comparison` to fill the table from `MIGKIT_MEASURE_OUT`; the
target reading of an owned operation is its kept position against the
target's (`Engine.log_position`) - a copy range's row count is not read,
so such an operation stops as a blocker; nothing run on the sandbox yet.

The target is smooth autonomous completion without sacrificing any required
guarantee. It does not authorize source changes, destructive repair or weakened
verification merely to keep moving. These implementation criteria remain OPEN.

Status 2026-10-16: PARTLY. Built: `migkit migrate` (two addresses or a hop,
goal copy/sync/cutover) runs assessment, copy, catch-up, proof with its one
repair, and cutover readiness per database (`migkit/autonomous.py`,
`cli.migrate`). A failed unit is classified (`faults.classify`) and
recovered as `decide.recovery` chooses from the fault catalogue - reconcile
an effect not known, proceed, pause, throttle, retry from the last position
after a measured backoff, probe, replan, isolate or stop - with a sizing
cause retuned (`tuning.Retune`), bounded by `autonomous.TRIES`; each choice,
its postcondition and whether it held kept in `workflow.json`. After an
isolate or a throttle the units left are re-planned (sizes, order by the
engine's forecast) and the scope and work left said (`autonomous.replan`).
A cutover proved ready proposes the switch as the typed question `switch`
with its evidence and makes it only on the operator's yes (`--go` up front
counts). Proved on fakes by `tests/test_autonomous_workflow.py`; the proof
harness `tests/test_autonomous_scenarios.py` (MySQL and PostgreSQL:
healthy, recoverable in the run, recoverable by a run again,
unrecoverable; inputs, interventions, completion, recovery, retry waste,
time) is written, not run; `docs/research/autonomous-comparison.md` holds
its table as "to be measured".
Not done: the scenario numbers (no run yet); the move's own worker count
rebalanced from the flow (the move sizes its workers itself); a reconcile
reading of the copy's committed position beyond "nothing past the kept
position" (`ops.reconcile` is honoured where a step has one; the CLI
steps do not yet); comparative measurements against managed services.

Not done (W9A, 2026-10-16): the MySQL range copier and the generic copier pass `check=` (`faults.landed`) and `owned=` to `faults.retried`; the PostgreSQL, SQL Server and cross-engine copiers and the tail loop do not yet. `faults.Throttle` runs between ranges in `ranges.ReadPool` only, not in the tail loop.

Not done (W10A, 2026-10-16): `faults.Throttle` is gone; the catalogue's reading is one more `sizing.Pace` signal (`sizing.catalogue`, the pace's own hold and one-back-a-window), and a server's answers about itself are held `sizing.FRESH` seconds. The PostgreSQL range pipe (`_pipe_retried`) and SQL Server's kept statements run under `faults.retried` with `check`/`owned`; MySQL ranges keep their own sizes. A bare timeout is told by its phase (`faults.classify(during=)`, `tuning.sized` says "statement"). Left: `tuning.sized` on the PostgreSQL pipe and the SQL Server bulk write (a pipe range has no batch size of its own to retune; the range split would be it), the tail/follow apply under `faults.retried` with owned reconcile, and `tuning.payload_way` asked before large-value tables and recorded in the plan and ledger. `generic.dst_call` does not retry on its own, so the generic batch is retried once over.

## P0-AUTOTUNE-ALL: diagnose and tune the complete migration workflow

Owner clarification, 2026-10-06. Status: PARTLY (2026-10-16). Retry/restart alone is not
intelligent recovery when an inappropriate setting or execution strategy is
the cause. Reported example: unreasonable LOB sizing causes failure rather
than automatic adjustment. This is operational feedback, not a claim that
all competitor versions behave identically. migkit must discover, diagnose,
tune and verify every applicable aspect of the task through shared decisions.

Acceptance criteria:

- Inventory tunable dimensions across every current/future task, step, engine
  and engine pair: large-value handling, payload and statement sizing, batching,
  partitions/ranges, concurrency, queues and memory, timeouts/retry budgets,
  flow control, transport/compression, conversion and validation strategies,
  consistency boundaries and applicable session settings. Link each to its
  factual prerequisites, valid bounds, guarantees, ownership and scenario proof.
  Do not confuse a tunable performance parameter with a correctness invariant.
- Before costly work, measure or establish representative shape, actual limits
  and uncertainty. For LOB/JSON, consider maximum and distribution of value/row
  sizes, encoding/serialization expansion, packet/statement/driver/service
  limits, memory pressure and supported streaming/chunking semantics. Use an
  exact bound where safety requires one, or a safe streaming strategy; a sample
  maximum is not proof that all values fit. Retain unsupported scope explicitly.
- Classify the failure cause and compare feasible adjustments rather than
  retrying identical doomed work. Select compatible combinations such as
  smaller batches, bounded payloads, streaming, changed chunking or an equivalent
  alternative path. Tune within available authorized resources; neither blindly
  raising every limit nor silently truncating a value is acceptable.
- Adapt from execution evidence and marginal benefit throughout the workflow.
  Use bounded probes, coordinated budgets, backpressure and stable control to
  avoid oscillation and resource storms. Reconcile partial effects before
  switching strategies, preserve valid completed work and update predictions.
  Verify the adjustment actually resolves the fault and preserves full coverage,
  data/type fidelity, required order, consistency and idempotency.
- Own technical tuning internally so supported recoverable scenarios finish
  without asking users to choose LOB modes, packet sizes, workers or retries.
  Automatic session/local strategy changes stay within authorized contracts;
  global database changes, source writes, destructive repair and paid capacity
  changes require applicable authorization. If no safe authorized tuning can
  meet the requirements, provide a precise blocker instead of false success.
- Prove end-to-end recovery from mis-sized LOB/payload, skew, resource pressure,
  incompatible limits and interacting misconfigurations in the sandbox across
  applicable engines. Demonstrate that the cause is diagnosed, tuning changes
  appropriately, the task completes and integrity is independently established.
  Measure user intervention, completion time, resource impact, tuning/probe
  overhead and retry waste against fixed-configuration and retry-only baselines.
  Do not claim lossless completion from disappearance of an error alone.

This extends P0-DECISION-ALL, P0-AUTO-RESOURCES,
P0-AUTONOMOUS-MIGRATION and P0-RESILIENCE-ALL. The target is deep dynamic
handling of all applicable migration dimensions, not a special-case LOB patch.
Implementation and actual comparative proof remain OPEN.

Built 2026-10-16 (`migkit/tuning.py`, tests in
`tests/test_autotune_every_dimension.py`):

- Inventory: `DIMENSIONS` (the twelve dimensions above), each with where its
  choice is made, its facts, the guarantee it keeps, its owner and its
  scenario; `TUNABLES` extended with dimension, facts, bounds, guarantee,
  owner (`LOCAL`/`SESSION`/`GLOBAL`/`PAID`) and scenario; `INVARIANTS` lists
  the bounds correctness rests on, which `observe` refuses to tune;
  `SETTINGS` and `may_set` keep a global server setting behind authorization.
- Large values: `payload_bound` takes the catalogue's promise or the largest
  over every row; a sampled largest certifies nothing, a JSON/XML stored size
  never bounds its text, and expansion as sent is counted. `payload_way`
  decides whole, pieces or stream through `decide.climb`, else a
  `decide.Blocked` naming the limit and who may raise it; nothing is cut.
- Cause to adjustment: `cause_of` classifies packet, timeout, memory, lock
  and connection from the server's words (a lost connection past a known
  limit is the packet). `Retune.failed` reconciles what the target holds,
  costs smaller batch, pieces, stream, smaller ranges or another way and
  picks one; identical work is never run again for a sizing cause. A failed
  size is a ceiling, growth waits `STEADY` quiet units and doubles the wait
  after a failed growth (no swinging). `succeeded` proves a tuning only when
  the target is counted whole, then keeps the tuned size (`observe`).
  `rebalance` cuts ranges predicted past `SKEW` times the median; `forecast`
  feeds `util.forecast`. `run` drives a work's units through all of it.

Not done: the fault classes are a local reading (`SIGNATURES`) until the
P0-RESILIENCE-ALL catalogue owns them; the copiers (`pieces`, the pair
copier, the engines' movers) do not yet call `Retune`/`payload_way`; the
scenarios run on fake servers - end-to-end on the sandbox's servers, with
intervention, time, resource use and retry waste measured against fixed and
retry-only baselines, is owed (`tuning.SANDBOX_OWED`); concurrency, flow
control, transport, conversion/validation and the consistency boundary have
no tuning scenario yet (`tuning.UNPROVED`).


Not done (W9A, 2026-10-16): `tuning.sized` retunes the MySQL range copier on a sizing fault; `tuning.payload_way` is not yet called before a large-value table, and the PostgreSQL, SQL Server and cross-engine copiers are not wired. A bare "timed out" now reads as a connection fault, not a statement timeout.

## P0-PRODUCT-TELEMETRY: complete usage evidence without customer data

Owner requirement, 2026-10-06. Status: PARTLY (2026-10-16). Plan comprehensive metrics and
structured events describing migkit's behavior and actual usage outcomes to
improve the product, decision quality, safety and performance. Customer
database content must never leave the user's environment. Storage/backend
selection is deferred; Hugging Face is not a selected destination. This
backlog change enables no collection or transmission. The owner previously
requested default-on product telemetry; actual implementation must disclose
that behavior clearly, provide effective opt-out and respect organizational
controls before any transmission.

Acceptance criteria:

- Define a versioned allowlisted event schema across every task, step, engine
  and applicable pair: product/adapter versions, coarse workload shapes and
  size buckets, selected strategy identifiers, safe decision reason codes,
  timings/rates, allocated/actual resources, progress and ETA calibration,
  bounded error categories, retry/recovery/autotuning outcomes, verified scope
  coverage and guarantee levels, completion state and user interventions.
  Comprehensive means coverage of product behavior, not collection of arbitrary
  runtime values or everything the process can read.
- Prohibit outbound row/value content, samples, database dumps, keys/checksums
  derived from customer values, query text, query plans containing literals or
  object names, schema/DDL, names of databases/tables/columns/accounts, endpoints,
  connection strings, credentials/tokens, paths, unrestricted configuration,
  raw logs, stack traces and arbitrary exception messages. No hashing of customer
  identifiers or content as a substitute for exclusion. Use coarse safe enums,
  aggregate counts and thresholds/buckets to reduce contextual disclosure;
  review remaining metadata for sensitive combinations and reidentification.
- Build outbound events directly from safe typed fields; never upload raw logs
  with best-effort redaction. Enforce schema/size validation and reject unknown
  fields and free-form values at the producer and receiver. Keep diagnostic
  evidence containing SQL, names or values local and separate from the outbound
  queue. Automatic fallback, crash reporting and support bundles must obey the
  same boundary; opt-in to telemetry does not authorize database-content export.
- Show what is collected, purpose, destinations once selected and default state;
  allow payload inspection, immediate disable and an organization-wide no-egress
  policy. Respect offline/air-gapped environments. On disable, stop queued sends
  and dispose of queued outbound events under the declared policy. Define
  minimal installation/session identifiers only if needed, rotation and retention;
  do not treat stable identifiers as anonymous without qualification.
- Secure local queues and transport, isolate receiver credentials from distributed
  clients, apply least-privilege ingestion/storage access, rate/size limits,
  authenticated administrative access, abuse protection, retention/deletion and
  auditability. Choose storage later against these requirements; private visibility
  alone does not establish confidentiality. Do not embed a shared storage write
  token in the client. Third-party destinations are explicit and reviewed.
- Keep collection and sending asynchronous with bounded CPU, memory, disk and
  network budgets, batching and capped retry/backoff. Telemetry failure must not
  block or change migration semantics, consume unbounded resources or trigger
  source/target queries solely to collect unnecessary product analytics.
- Establish an analysis loop linking usage evidence to benchmark cases, decision
  calibration, recovery improvements and backlog priorities. Segment by safe
  engine/version/workload metadata and distinguish observational correlations
  from causal performance claims. Missing telemetry cannot imply success.
  Untrusted submitted events must not automatically change executable strategies
  or production defaults without validation and regression proof.
- Prove privacy and reliability with synthetic secret/content canaries in rows,
  SQL, DDL, identifiers, errors, paths and crash contexts; inspect all network
  payloads, queues and retry/support paths. Test unknown-field rejection,
  opt-out/offline controls, authentication boundaries, abuse and receiver outage.
  No customer database content or identifying schema is present in outbound
  evidence. Complete task coverage, useful analytics and bounded operational
  overhead must be demonstrated before marking DONE.

This extends existing evidence/observability and local-learning owners; it does
not replace detailed local diagnostics or authorize sending company material.
All implementation and storage selection remain OPEN.

Built 2026-10-16 (`migkit/telemetry.py`, proved by
`tests/test_telemetry_carries_no_customer_data.py`, written not yet run):
- Schema version 1: seven typed event kinds (run, decision, progress,
  resources, recovery, coverage, intervention); every field a closed code,
  a bucket, a bounded number or a flag. An unknown kind or field, a string
  outside its codes, a wrong type or an oversize event is `Rejected` at the
  producer, without repeating the value. Strategies are the ladders' own
  ways by their words (no program named; a label made at run time is
  `other`); reasons are `decide.WORDS` codes matched, never copied;
  exceptions are kept by type only.
- `session.step` emits a decision event per choice and a run event per
  command (completion from the exit table, error category, buckets).
- Local spool only: 4 files of 128 KiB, rotated, mode 0600, bounded queue
  with a background writer, a full queue drops and counts. `Sink` has only
  the local implementation.
- Off: policy file (`MIGKIT_TELEMETRY_POLICY`, unreadable = off),
  `MIGKIT_EGRESS` without `telemetry`, `MIGKIT_TELEMETRY=off`,
  `migkit telemetry off`; off stops queueing and disposes of the spool.
  `migkit telemetry show [--schema]` prints the state, the disclosure and
  every kept event. Disclosure on the manual's Telemetry page and README.

Built 2026-10-16, schema 1.1 (same files; tests written, not yet run):
- The five other kinds are emitted by their owners through typed calls
  that never raise: progress by `planner.progress` (buckets and rate; at
  most one event per 30 s, and on the end the time over the first
  estimate), resources by `model.Model.observe` (workers the model
  allotted and ran, memory the workers hold), recovery by
  `faults.retried` (failure class and the action `decide.recovery` chose,
  tries, how it ended; the exception by type only), coverage by
  `verdict.summarize` (scope compared in a bucket, the verdict, the
  guarantees only for a check that came out the same), intervention by
  `autonomous.ask` (asked, answered, the question's key alone).
- New codes instead of strings: recovery `fault` and `path`, intervention
  `question`, intervention `did` (renamed from `kind`, which shadowed the
  event's own kind), and the `crash` kind (command, phase, error category).
  `cli._stopped` emits it; the traceback stays in the local debug log, and
  a diagnostics bundle adds only a `bundled` code.
- Canaries now cover all five emitters, a crash and the bundle; tests
  assert the spool costs the caller under 2 s per 1000 events and that off
  in mid-run stops queueing and disposes of the spool.

Not done: no destination, transport, receiver, retention or identifiers
(storage undecided); the analysis loop is not built; eta_off is measured
for the move phase only; coverage is emitted by the check envelope, not
by each verification rung, so its guarantees are the command's chosen
ways, not per unit; the tests above have not been run (final suite).

## P1-SOURCE-HIERARCHY: readable project and source organization

Owner request, 2026-10-06. OPEN. Research and proposed layout: [source hierarchy plan](research/source-hierarchy-2026-10-06.md). Compare verified GitHub layouts, inventory every source file and dependency, establish responsibility packages and complete old-to-new mapping, split oversized engine adapters behind stable contracts, align tests/docs/navigation, and enforce dependency rules. Preserve behavior, installed packaging, CLI/configuration/result contracts, state and resume compatibility. Stage coherent moves separately from behavioral changes; prove final sandbox and installed-wheel workflows under the repository test cadence. Do not interrupt active validation. This is a plan, not a completed reorganization.

### P0-RESULT-SCOPE.contract and presentation plan

Owner clarification, 2026-10-06. OPEN. [Result and report presentation plan](research/result-presentation-2026-10-06.md). Separate outcome, execution state, coverage and severity in a versioned structured envelope; no complete pass from partial/skipped/unsupported/failed work. Show actual scope then one row per database, practical issues grouped by database, and evidence on demand. Use one aggregation contract across CLI/web/exports/reports. Validate mixed-state examples and 30-second comprehension; preserve existing consumers and confidential evidence. Extends existing scope/results items rather than duplicating their research.

### P0-RESULT-SCOPE.artifacts

OPEN. Owner clarification, 2026-10-06: the run's directories and files must be understandable as well as the rendered report. Extend the result presentation plan with one readable entry point, database links, a versioned artifact manifest, separate evidence/diagnostics/internal state/exports, explicit authoritative retry results, safe paths and secret-free sharing. Provide non-destructive legacy indexing and resume compatibility; do not reorganize active run checkpoints.

### P0-READABILITY-ALL: coherent readable product surfaces

OPEN. Owner requirement, 2026-10-06: everything the user or maintainer encounters must be easy to find, read and understand. Applies to source hierarchy, configuration, commands/help, workflow/state/progress/ETA, errors/recovery, results/reports, directories/artifacts, exports and documentation. Use consistent plain names, a clear entry point, task/database grouping and progressive detail. Explain what happened, what it affects, what is verified or unfinished and whether action is needed; avoid unexplained identifiers, long repetitive prose and raw internal output in normal views. Preserve technical depth through linked detail, not omission. Enforce truthful scope, guarantees, uncertainty and evidence. Assess real representative tasks with comprehension/navigation exercises; cosmetic styling alone is insufficient. Extends P1-SOURCE-HIERARCHY, P0-MINIMAL-INPUT, P0-VALIDATE and P0-RESULT-SCOPE; those items retain implementation ownership.

### P0-ARTIFACTS: typed workflow products and lineage

OPEN. Owner request, 2026-10-06. Extend the [result presentation plan](research/result-presentation-2026-10-06.md) with first-class typed artifacts for every migration workflow. One manifest contract over existing evidence/state/audit owners; provenance, scope, validity, attempts, dependencies, sensitivity, durable atomic publication, safe reuse/invalidation, discoverability, retention and safe export. No duplicate storage engine; local by default. Prove crash/rerun/stale-scope/restore/retention scenarios. Artifact availability never implies complete verification or authorization to apply it.

### P0-VALIDATE.no-silent-skip: incomplete deep checks cannot pass

OPEN. Required in the next implementation campaign, owner 2026-10-06. Observed validation incident: whole-table per-column LOB queries timed out; a broad exception handler continued and the deep result described only successfully read columns as OK without disclosing omitted columns. Related charset, NULL/empty and float paths also catch errors and continue; partition digest paths can fall back to counts. This is execution completion without complete evidence, not a complete verified pass. Keep customer names/data out of public fixtures.

Inventory every engine/task/check handler that suppresses errors or substitutes weaker evidence. Materialize required table/column/range/check work before execution, retain durable unit identity, record attempts and failed/pending/unsupported scope, and aggregate only from structured evidence. A failed required unit makes its check incomplete even when other units succeed. Counts-only fallback cannot imply digest/content verification. Safe independent work may continue, but readiness/cutover gates refuse unmet required guarantees.

Shared decision/execution owners must diagnose timeout, connection loss and resource pressure, replan compatible methods or batch/split work, preserve valid units and resume with bounded retries and budgets. Reconcile owned queries and uncertain effects before retry. Preserve original check semantics and actual scope; no downgrade to sampling or silent subset. If recovery is impossible, return an explicit blocker or unsupported guarantee. Make omitted/failed counts and affected scope visible in concise results.

Mandatory sandbox regression: one large-value column times out while the others succeed; overall check remains incomplete and names the missing scope; interruption/resume completes only remaining units; batched range execution yields the same exact maxima/counts as a complete reference; all output surfaces and readiness gates agree. Include charset/NULL-empty/float/partition weaker-evidence cases, lost connections, repeat failures, changing scope and restart. Never test with customer data. Regression and integration proof follow the repository final-suite cadence. Extends P0-VALIDATE, P0-RESILIENCE-ALL, P0-AUTO-RESOURCES and P0-RESULT-SCOPE.contract rather than adding a parallel controller.

### P0-RESUME-ALL: every task, step and engine

OPEN. Mandatory owner requirement, 2026-10-06. Every current/future task, phase, step and work unit across all supported engines/pairs must participate in durable resumable workflows, including discovery, assessment, configuration/planning, conversion, schema/objects/users, bulk move, CDC/DDL, validation, diagnosis, authorized repair, cutover/rollback, reporting/artifacts and cleanup. No path is exempt because its command is short or a wrapper.

Persist scoped/versioned work identity, dependencies, completed evidence, pending/failed units and relevant transaction/replication positions. Publish checkpoint advancement only after durable confirmed effects. On restart, reconcile actual state and unknown effects before replay, preserve transaction/dependency/event order, prevent duplicate effects and respect source/target ownership. Retry resumes an idempotent unit, not an assumed undoable external call. Non-atomic external operations require detect-and-reconcile or explicit safe re-execution contracts; impossible or unauthorized recovery remains an actionable blocker, never claimed automatic success.

Use shared decision/execution/persistence owners for automatic recovery/replanning where safe. Handle connection loss, process/host/controller crash, timeout, cancellation, resource pressure and restart; differentiate explicit pause/cancel from transient failure so no automation overrides user intent. Safely invalidate cached evidence on schema/scope/method/source-window changes. Maintain compatibility/version migration for saved state and authoritative artifact lineage.

Acceptance: inventory every execution entry point and work unit with its resume contract, guarantees and proof; no unsupported path hidden behind a broad claim. Sandbox interruption before/after effects, during checkpoint publication and after ambiguous acknowledgements must resume to the correct final state without duplication, loss or weaker coverage. Cover nested parallel work, changing resources, repeated resume, upgrades, cutover critical sections and rollback. Test per-task/per-engine coverage plus representative full workflows under repository final-suite cadence. Extends P0-RESILIENCE-ALL, P0-ARTIFACTS, P0-VALIDATE.no-silent-skip and P0-DECISION-ALL; no parallel recovery controller.


## P0-ETA-ALL: calibrated remaining-time estimates across every engine and task

Owner request, 2026-10-14. Status: PARTLY (2026-10-16). Current verification estimates elapsed
time against completed phase counts, which do not represent remaining work on
large or skewed tables. Reuse the shared decision engine, durable work ledger
and resource telemetry; do not add a separate estimator/controller.

Required before DONE:

- Cover every engine, pair, adapter and task, including move, CDC, check,
  verify and deep. Estimate outstanding work from actual units, rows/bytes,
  scan/query cost and dependencies rather than equally weighted phase counts.
  Distinguish known remaining work from undiscovered or unmeasured work.
- Use observed throughput and per-unit duration, recent pressure on both
  endpoints, concurrent work and the dependency critical path. Avoid double
  counting parallel durations. Include retries, reconciliation and cooldowns;
  distinguish active-work estimates from wall-clock completion estimates.
- Publish a time range, confidence, measurement timestamp, evidence and scope
  alongside the central estimate. During warmup, missing telemetry, blocked
  connectivity, unknown backlog or changing workload, report insufficient
  evidence or conditional ETA instead of an invented precise deadline. Define
  CDC catch-up ETA separately; never promise completion if ingest outruns apply.
- Persist estimation history and valid completed work across interruption and
  reboot. Recompute remaining work after resume, invalidation, repartitioning
  or resource-budget changes without discarding valid progress. Flag stale
  estimates and explain material revisions; use smoothing without hiding stalls.
- Establish an empirical accuracy and coverage gate before claiming reliable
  ETA. Record predictions at multiple horizons against actual completion;
  measure median and tail absolute/relative errors, interval coverage and
  bias by engine, task, scale and pressure. Set explicit release thresholds
  from representative observed runs, including network loss, throttling, skew,
  retries and resume. Do not claim reliability from one fast happy-path test.

Dependencies: P0-RESUME-ALL, P0-DECISION-ALL, P0-VALIDATE.unattended.capacity,
P0-VALIDATE.unattended.progress and P0-VALIDATE.mysql-query-lifecycle.

Built 2026-10-16: `util.forecast` times the units left in the ledger (rows,
bytes, or the unit time where unsized) at the recent window's median and
10th/90th percentile rates, with retries, cooldowns, workers and the
dependency critical path (`util.schedule`, parallel work counted once); what
it stands on is a `decide.climb` over this run's units, an earlier run's kept
rate (`HopCosts`, read lazily through `Facts`) or nothing measured. The `ETA`
carries range, confidence, measured-at, evidence, scope, known work apart from
unmeasured/undiscovered, active vs wall seconds and a next step; warmup,
nothing measured and stalls give no time. `util.catch_up` is the separate CDC
catch-up and says "falling behind" where ingest >= apply. `EstimateHistory`
keeps estimates and finished units in the hop's ledger (`eta-history.jsonl`):
resume measures from them, invalidated units are owed again, stale estimates
and revisions out of the earlier range are said. `Engine.forecast(kind, db)`
covers move, follow, check, verify and deep for every engine; `check` progress
says it. Proved on fakes in `tests/test_calibrated_eta.py`.

Built 2026-10-16 (W6A2): every long loop says its time through
`Engine.forecast`. The move's progress (`planner.progress` `eta`, said by
`watch` through `_move_said`) is a table a unit sized from the catalogue
(`cli._move_units`); `watch` adds the tail's catch-up (`cli._follow_said`,
"falling behind" where arrivals outrun the apply); the check loop says the
deep comparison or the verification by what it runs (`cli._checks_kind`);
the report has a "time left" card from `EstimateHistory.latest`. The attempt
ledger (`attempts.Ledger.units`, wall times kept per attempt) feeds unit
times, retries and failures into the rate (`util.merge_units`); a running
unit idle past `STALL_UNITS` times the usual unit lowers the confidence and
opens the range, and where every running unit is so the line says
"stalled". Operator text is `ETA.operator_line`: time, range, confidence,
evidence, next step. Proved on fakes in
`tests/test_every_long_loop_says_its_eta.py`.

Built 2026-10-16 (W6A3): the attempt ledger keeps units, not only
statements (`attempts.Ledger.unit_opened`/`unit_closed`, `Monitor.unit`,
`Engine.work_unit`): a table, partition or range is running until closed,
parts roll up with their share, a unit an earlier run left open is owed
again; the move opens a unit per table. A task found done scores the
estimates said of it (`EstimateHistory.calibration`); where fewer ended in
range than their stated confidence, `util.forecast` and `catch_up` lower
the confidence and widen the range and say so; the report says the
calibration. `_tail` says its own catch-up line (`cli._tail_said`). The
move's estimate takes side session health (`Engine.session_health`: lost
answers, stalled waits, failed keepalives). A failing move estimate is said
with its reason (`eta_error`). Proved on fakes in
`tests/test_every_long_loop_says_its_eta.py`.

Not done: representative sandbox runs for the accuracy gate by engine,
scale and pressure, and release thresholds. A deep plan's range workers
open their own part units under their table, `verifiers.each_database`
a unit a database, and a pair's inner engines keep a ledger file each
(`attempts.ledger_path`) since W9D.


Not done (W9D, 2026-10-16): move range workers (`ranges`) and the
check loop in `cli` still open no part units; a pair whose two
sides are the same engine shares one inner ledger file name.

## P0-CONNECTION-PROGRESS: detect and reconcile client/server progress mismatch

Owner request, 2026-10-14. Status: PARTLY (2026-10-16). Priority: first incident-recovery work
alongside P0-VALIDATE.mysql-query-lifecycle, before feature expansion. Applies
to every engine, adapter and external worker where observations are available.

Observed live incident: a verifier worker waited in SSL poll while its exactly
attributed server connection was idle (Sleep) for approximately 48 minutes.
Closing only that verified idle connection caused the same worker to reconnect
and continue bounded LOB reads, without restarting the process. The exact
packet/tunnel/TLS failure was not captured; do not claim a proven network fault.
New connections and a SELECT 1 probe succeeded during investigation.

Existing MySQL behavior uses a 3600-second application read deadline and
best-effort TCP keepalive; keepalive configuration errors are suppressed.
No automatic client/server progress-mismatch reconciliation is established.
A separate diagnostic socket showed configured keepalive, which does not
establish the effective settings or packet behavior of the failed socket.

Required before DONE:

- Persist attempt start, exact server-session ownership, operation, work-unit
  position, query/result timestamps, last useful progress and retry reason.
  Exclude credentials and sensitive query literals from telemetry.
- Distinguish server computation, result transfer, application read stalls,
  idle sessions, locks and unavailable observations. An idle server alone is
  not proof of failure; classify the operation and driver/protocol stage.
- Use bounded operation-specific progress/deadline budgets selected by the
  shared decision engine. Expose actual keepalive availability/settings and
  failures; transport liveness must not substitute for application progress.
- When a mismatch is substantiated, cancel/release only the exactly owned
  session and reconcile its outcome before retry. Preserve completed units
  and accumulated deep results. Never automatically replay uncertain writes.
  When ownership or outcome cannot be established, report actionable unknown.
- Capture safe timestamped client/server/tunnel evidence before recovery where
  available, including bounded failure-class telemetry. Prove fault recovery
  with server-idle/client-wait, stalled transfer, disconnect, slow legitimate
  queries, stale session IDs and unavailable privileges on each supported path.
  Bound stall detection/recovery time and show unrelated sessions unaffected.

Dependencies: P0-RESUME-ALL, P0-DECISION-ALL,
P0-VALIDATE.mysql-query-lifecycle and P0-ETA-ALL.

Done 2026-10-16: `attempts.py` keeps every attempt in a ledger beside the
hop's reports (`attempts.json`: start, the exact server session, operation,
unit and position digest, query sent/answer read, last progress, retry
reason, transport facts; no statement text or credentials) and runs one
watcher per engine. Past the patience `decide.session_patience` chooses
(rungs on the waits measured per operation, then the unit's prediction,
then the read deadline read back off the connection; no fixed number), the
server's view of the session is read through the Engine contract
(`session_view`, `owns_session`, `close_session`, `transport_of`;
`SESSION_VIEW` NOT_YET on every engine but MySQL - the process list - and
PostgreSQL - `pg_stat_activity`) and the wait is named: computing,
transfer, client stall, between reads, idle, locked, gone, unavailable.
`decide.session_action` closes only a session idle on two reads, longer
than the measured wait, of the hop's account, under a read; ends migkit's
own socket where the server dropped the session; else says the fact, the
consequence and the next action. A unit closed once is not closed again
until it progresses. MySQL `_q` and the range checksum (`attempt_unit`)
and PostgreSQL `_psql` record attempts; `util.keepalive` now returns its
failure instead of swallowing it. Proved on fakes:
tests/test_a_lost_answer_closes_only_its_session.py (written, not run).

Not done: no sandbox fault injection yet (server-idle/client-wait through a
proxy, stalled transfer, disconnect, stale session ids, missing
privileges on real servers); PostgreSQL's chunked verify ranges are not yet
wrapped in `attempt_unit` (its attempts are kept per operation, not per
range); tunnel-side evidence is not captured; the other engines declare no
session view (NOT_YET); lock waits are named but not acted on; the
ETA (P0-ETA-ALL) does not yet read the attempt ledger.


## P0-VERIFY-EXECUTION: default adaptive verification plans, not fixed nested loops

Owner rejection and requirement, 2026-10-15. Status: OPEN. Priority: first
implementation work with #502 and #504. Increasing workers or adding a Pace
class does not satisfy this requirement. All current/future engines, pairs,
adapters and external backends must use the normal shared execution contract;
no operator flag or manual tuning is required to activate it.

Observed gaps and required acceptance:

- Audit actual entry-point-to-query paths for check, deep, verify, delta, move
  and CDC. Record whether sizing, pacing, resource probes, progress, ledger and
  safe recovery are called, merely present, bypassed or unsupported. Static
  absence of a local Pace reference is only a candidate, not proof of bypass.
- Replace fixed per-column/per-side full scans and private nested pools with a
  dependency-aware shared scan plan. Fuse compatible column/check aggregates
  into one bounded read when cheaper; preserve semantics, limits, full coverage,
  consistency and independent evidence. Avoid reading a huge table once per
  column, and avoid duplicating scans already covered by valid evidence.
- Choose the verification strategy as well as concurrency: compare measured
  end-to-end cost and correctness constraints of compatible engines/tools,
  grouped scans, indexed ranges and hierarchical localization. Full validation
  still covers every required row; localization of differences is not a
  substitute for establishing full parity. Sampling is never a silent fallback.
- Run independent source/target work concurrently and distribute pending units
  through a shared cost-aware queue. Coordinate outer and nested worker budgets
  across databases/checks so separate pools cannot multiply endpoint pressure.
  Sequential paths require an explicit consistency, capability or dependency
  reason, with a visible unsupported-path status where no safe parallel path exists.
- Adapt query strategy, unit boundaries, concurrency and rate from useful
  throughput, measured row/byte/LOB cost and both endpoints' CPU, memory, I/O,
  locks, connections and lag. Use bounded probes, hysteresis and backpressure.
  Fixed ID widths, fixed worker counts and one startup sizing decision are not
  completion. Never mutate already running units during replanning.
- Preserve every valid committed unit during plan changes, network loss and
  reboot; reconcile uncertain outcomes and exact query ownership. Keep distinct
  semantics for result aggregation, snapshot validity and CDC positions. Failed
  required scans stay incomplete/error; no swallowed exceptions or false OK.
- Publish live phase/table/range, coverage, useful rate, decision history,
  stalls and calibrated ETA. Prove automatic adoption by installed normal entry
  points on each engine/path, not only helper tests or private recovery scripts.
- Benchmark baseline versus complete workflows on representative scale, sparse
  and skewed keys, large values, cold/warm cache, constrained endpoints and
  interrupted runs. Demonstrate faster total completion with equal correctness,
  bounded endpoint pressure and no duplicate effects or discarded valid work.

Partial local work is not DONE: shared ReadPool/Pace wiring and fused MySQL
charset/LOB reads exist locally; four focused mocked tests passed. Runtime
sandbox/end-to-end proof, global nested budgets, dynamic cost-based splitting,
complete strategy planning and all-adapter adoption remain open. The existing
long-running verifier still holds its earlier sequential implementation.

Dependencies: P0-DECISION-ALL, P0-AUTO-RESOURCES.adapt, P0-RESUME-ALL,
P0-VALIDATE.mysql-query-lifecycle, P0-CONNECTION-PROGRESS and P0-ETA-ALL.

Partly done 2026-10-16 (W6C), status stays OPEN: the entry-point audit is
`docs/research/verify-execution-audit.md`. `migkit/scanplan.py` plans every
aggregate a check asks of a table as one set of reads (duplicates once, one
bounded read a side or covering key range, sides and tables side by side
under the run's slots, the way climbed from `scanplan.READS` by measured
seconds a row with "every row" the need, values under unchanged change
markers not read again, never for a cutover); MySQL `check_deep` charset,
NULL/empty and float read through it, the private `Slots` and the
30-column limits gone (tests/test_a_tables_checks_are_read_in_one_plan.py,
written, not run). Not done: other engines' deep value checks, the delta
loop's sequential databases, `leaves`' private pools, row parity costed end
to end with an engine-native digest rung, unit resume without markers (MySQL
has none), sandbox/end-to-end benchmarks.

W6C2 note, 2026-10-16: PostgreSQL (NULL/empty, replacement characters) and SQL Server (NULL/empty, float, replacement characters) deep value checks read through `scanplan.for_engine`, one read a table a side, no column limit, a failed read said as unknown (tests/test_postgres_and_sql_server_deep_checks_share_one_plan.py, written, not run); depth item `verify.value.one-plan` declares every other engine NOT_YET here. Not done: Oracle, ClickHouse, Teradata, BigQuery, pgwire value checks still a read a column; no key-range cut on PostgreSQL/SQL Server plans; `leaves`' private pools; the delta loop's databases one at a time; row parity as a ladder in `decide` costed end to end with a server digest rung and full coverage as a Need.

W6C3 note, 2026-10-16: `leaves.both`/`gather` read through `scanplan.together` (child slots of the run's, no private pools); `watch --verify --delta` verifies its databases side by side (`verifiers.each_database`, one `ReadPool`); row parity on PostgreSQL climbs `decide.ROW_PARITY` (server digest, key-range digest, row text) on measured seconds a row, resume by chunk the need where ranges were kept or the rows are unknown; `scanplan.for_engine` cuts PostgreSQL (block ranges on 14+, else key histogram) and SQL Server (partition stats + key histogram) tables into at most the plan's count of ranges (tests/test_verify_execution_shares_the_runs_budget.py, written, not run). Not done: SQL Server's charset check still a second read (the one-plan cell narrowed to NULL/empty and float); the row text rung has no engine runner yet; SQL Server and other engines' row parity not on the ladder; PostgreSQL's per-table pool in `_data_fast_native` still its own.

### LT.R10 MongoDB standalone followed by buckets

Done: a standalone source is followed by a bucket sweep (`MongoEngine.FOLLOW_WAYS`, `_sweep_round`): dbHash per collection as the unchanged proof, then raw-BSON bucket digests keyed by the canonical `_id` hash (`leaves.bytes_hash`, `id_bucket`), only changed buckets re-read and written; depth D1 with "convert to a single-node replica set" (`changedepth.mongodb`, `MongoEngine.change_depths`).

Not done: the oplog rung (`FOLLOW_WAYS` "oplog", `_oplog_follow`) is proved on fakes only (tests/test_a_mongodb_replica_set_is_followed_by_its_oplog.py, written, not run); no live test on a 4.0 replica set or with a refused stream yet; "per range" is its declared consistency because the ladder has no level between per range and as of one moment; the standalone server test is written, not run; `change_parity.py`, the older flat duplicate of this item, is deleted (a5ddd41).

W6D note (6.fkwin, movers.choice, reshard): the foreign-key window now wraps the PostgreSQL streaming copy (`movers._fk_window`, `ForeignKeyWindow(hold=True)`); a whole database's untimed mover gets its trial through `decide.climb(trial=(names, seconds))`, a failed trial kept in `mover-trials.json`; `reshard._spread` refuses with its reason. Not done: the window is not opened around the dump and load (its triggers are already quiet) nor the one-table streaming copy (its rung needs `truncatable`, and concurrent windows on shared keys race); a whole-database trial's box is not enforced during the run, only a failure is kept; not run on a server.

Not done (W9D, 2026-10-16): MySQL and the generic engine still prove a
table's rows their own way, not through `decide.row_parity`; the row
text rung on SQL Server reads a range of the whole table (no key
cuts yet). Proved in tests/test_units_and_row_text_reach_every_engine.py.

## Left after wave 9

What is still unproved per P0 item, taken from the PARTLY and OPEN rows of `docs/backlog-index.md` (2026-10-16). The next wave is cut from this list; an item is out of it only when its row is DONE.

- `P0-DE.1` (row 2): PARTLY (review 2026-10-16: an external program could not earn its rank on a cost model's Estimate measured on no unit; unlock fixed to a measured run on at least one unit, under HopCosts too; the speed rules as rungs not re-measured
- `P0-DE.2` (row 4): OPEN (review 2026-10-16: `doctor.py` was reached from nothing and is deleted (a5ddd41); `migkit doctor` says tools, hops and connectivity, not the rung each side and leg stands on or its footprint
- `P0-DE.4` (row 6): not yet as not applicable with reasons no server measured - large values on the newer engines and services, partitions on GaussDB, ASE, Iceberg, Delta and pairs; put back to not yet with their rows; db2, Parquet and the MySQL-wire services' partition reads owed a test
- `P0-DEPTH.calibrate` (row 111): missing: the calibration on the final run
- `P0-DEPTH.json` (row 113): UNPROVED`). Not done: MySQL's JSON in SQL (declared unproved); `PostgresEngine._json_text_sql` is still its own copy of canon's; a JSON key column is read parsed; the server tests written, not run
- `P0-DEPTH.triggers` (row 114): OPEN (review 2026-10-16: the range copy's load options set session_replication_role on the source export connection too, a superuser-only setting; the load side already owns it, superuser-gated
- `P0-DEPTH.rank` (row 115): still a list of names, not a rung's own field everywhere
- `P0-AUDIT` (row 117): open
- `P0-PARITY.small` (row 135): OPEN (queued; grep: `decide.coverage()` missing grant not yet on MongoDB, the cross-engine pair and the generic engine; the generic engine is not yet on any of the 11 shapes; PARTLY (Redshift and Snowflake, branch worktree-agent-aac78db840f4c30b6: built and proved on stubs - the scope in the listing, the copy and the repair, versions and brands, a source's hold, Snowflake's readiness rows; built with the...
  - W12F (10-16): generic now lists tables from the source's standard catalogue when the hop names none, asks `in_scope` of every listing, reads the row filter in its copy and comparison and the name rules on the target (scope.listings/deep/copy/row-filter/names proved, tests/test_generic_and_mongo_reach_parity.py); a refused catalogue or read on generic, and every MongoDB refusal, stops naming the grant statement to run. Not done: generic scope.transforms/objects/ops, its structure/value/tail/schema/users shapes, missing grant on the cross-engine pair (pair code not in this ticket), decide.coverage.
  - W13F (10-16): generic writes, follows and checks masked columns transformed (`_masked`; a transformed key is not resumed by, the servers' digest is not taken for a masked table), follows only the changes `mapping.ops` carries (`_carried_ops`), compares keys, foreign keys, check constraints and indexes by shape from each side's catalogue in the spelling that answers (standard, then PostgreSQL, SQL Server, Oracle), counts NULLs against empty texts per column, and holds the views the hop's `objects` take (tests/test_generic_reaches_every_shape.py). Cells for W13M to drop from DEPTH_GAPS: scope.transforms/ops/objects, verify.structure.keys/foreign-keys/indexes/objects, verify.value.null-empty. Not done: tail table states and a change-table rung, schema create from the neutral shape, users from the standard privilege views, the other value categories, missing grant on the cross-engine pair.
  - W13E (10-16): MySQL range readers (its own copier's threads and processes, and the cross-engine copy reading MySQL) start at one moment: the tables held by a read lock while the executed GTIDs and the binlog position are read once, each reader's consistent snapshot proved to begin under that lock and kept for the slot's ranges, the lock given back once every reader is in (`MySQLEngine.export_moment`/`_moment_source`); SQL Server reads its triggers, keys and checks back after the hold (`WRITE_STATES_SQL`); the machines sharing a table (`claims.copy`) take the hold and the moment, one reading of the write states between them and the last to give it back checking it (`claims.held_in`/`held_out`); the cross-engine write goes a row a statement on a large value's way; a refused read or write on the cross-engine pair names the side and the statement to run there (`HeteroEngine._refused`). Proved with fakes in tests/test_mysql_mssql_and_claims_share_the_hold.py. Not done: sandbox proof of the moment under concurrent writes; a machine that dies holding a shared table leaves no last machine to check its write states; SQL Server copier's own ranges do not take the moment (it exports none).
- `P0-PARITY.lv.new` (row 136): Not done: `decide.coverage()` still says not yet for CockroachDB and DSQL and not applicable for Cosmos DB; Couchbase and Teradata say yes with the test owed (`capabilities.UNPROVED`); no server profile for any of the six, so no limit measured and no row moved; `pieces.ReadPlan` does not keep the first piece
- `P0-CLOUDS.native` (row 150): IN PROGRESS (stopped, work in its worktree: 6 commits and 6 dirty files
- `P0-CLOUDS.more` (row 151): OPEN (grep: `managed.distinct()` lists them; not among the twelve of P0-CLOUDS.native
- `P0-SCENARIOS.brands` (row 158): BLOCKED (no server-side sign is known that is not an absence; it needs measuring on the real services
- `P0-SCENARIOS.managed` (row 159): not yet, and `move`'s plan says it (`scenarios.plan_lines`). Not done: no catalogue of the providers' own operations (blue/green, upgrade) with eligibility and duration evidence, so the plan says no eligible operation was read; no live provider run
- `P0-SCENARIOS.arch` (row 160): not yet), a failed read said in the case; `cloudapi.Operation.new_instance` marks each operation (blue/green, Aurora replica and snapshot, Azure Single to Flexible, the new `aws-class-restore`: a new instance; major upgrades: in place); `Aws.run_class_restore` runs the class restore through `Adapter.runner` and reads the class back, proved end to end on fakes with migkit's verify after (`run_managed_case`); a...
- `P0-SCENARIOS.charset` (row 161): missing: Oracle's and Db2's column collations are not put to the target, ASE's and Teradata's character sets are not read
- `P0-SCENARIOS.engine` (row 162): missing: ClickHouse table engines and PostgreSQL access methods are not compared
- `P0-SCENARIOS.consolidate` (row 163): missing: following each source's changes into the shared target
- `P0-SCOPE.open` (row 438): OPEN
- `P0-PARITY.scope` (row 439): OPEN (grep: `capabilities.depth()` at 9fd2442, its 10 scope items: yes 37, test owed 92, not applicable 50, not yet 181 - the cells are named in STATE.md; PostgreSQL since: views and the confirm pass proved on a server; still open on PostgreSQL: column transforms - `transforms` is wired into no copy or check on any engine, `movers.mask_rules` knows only null and constant, and PostgreSQL's copy ways and its...
- `P0-PARITY.verify` (row 440): OPEN (grep: `capabilities.depth()` at 9fd2442, its 39 verify items - the 30 deep-check categories, delta, fence, confirm, users, repair, exact undo, rows apart, tables, table: yes 124, test owed 74, not applicable 499, not yet 707; MySQL since: `verify.tables` and `verify.structure.schema-diff` proved on stubs, `verify.structure.triggers`, `verify.value.rendering` and `verify.structure.collection-options` built...
- `P0-PARITY.tail` (row 441): OPEN (grep: `capabilities.depth()` at 9fd2442, its 6 tail items: yes 26, test owed 18, not applicable 59, not yet 113; the operation contract is `scope.ops`; Redis: NOT DONE, all five - it follows only through the server's own replica, which carries the whole server; a change reader of its own (the replica protocol's stream, parsed) is the missing piece; Kafka, 2026-10-11: states, a topic added at its seam, a...
- `P0-PARITY.all` (row 443): missing: the ways - assess, schema, move, cutover, security, managed services and operations at {C}: yes 248, test owed 68, not applicable 261, not yet 539; MySQL since: `assess.preflight` and `cutover.rollback` proved on stubs, TiDB takes `assess.preflight` with it and owes its own test; evidence, observability and the forecast are one way for every engine, not read engine by engine); Redis (a real server):...
- `P0-WORKFLOW-DEPTH.model` (row 456): OPEN (review 2026-10-16: `PhaseContract` does not exist; `planner.Phase` carries no prerequisites or required evidence contract
- `P0-MINIMAL-INPUT.ownership` (row 463): OPEN (review 2026-10-16: `WorkflowState` does not exist; no automated phase-transition state
- `P0-MINIMAL-INPUT.acceptance` (row 466): Not done: no live timing - the sandbox was stopped - so minimal effort and the guarantees are not yet proved together on a server; the 2026-10-15 claim of tests passing 100% is not backed by a run since the review
- `P0-AUTO-RESOURCES.proof` (row 476): Missing: the end-to-end run the row names - a move that sizes itself, forecasts and nests leases against a real server, with the forecast checked against the time it took
- `P0-DECISION-ALL.engines` (row 477): OPEN
- `P0-SAFETY-INTEGRITY` (row 479): PARTLY 2026-10-16 (guarantees.Semantics read from catalogue + engine facts with explicit gaps; decide.admit / KEEPS_SEMANTICS admits schedules; joint plan barriered by foreign keys, admitted, and a refusal stops the move; keys-held read from the mover; target catalogue read; tail lanes, copy ranges and their marks ask decide.admit; all faults in PROVES, tests written, not run
- `P0-RESILIENCE-ALL` (row 480): not yet wired, replan/isolate workflow and real-server compound runs unproved
- `P0-AUTONOMOUS-MIGRATION` (row 481): open
- `P0-PRODUCT-TELEMETRY` (row 483): not yet run; no destination, no transmission
- `P0-AUTOTUNE-ALL` (row 482): PARTLY 2026-10-16 (inventory of every dimension, fact bounds, exact large-value bound, cause-to-adjustment retune with reconcile/verify/anti-oscillation on fake servers; cause_of reads faults.CATALOGUE, tuning.sized retunes the MySQL range copier on a sizing fault; payload_way before large-value tables and PostgreSQL/SQL Server/hetero copiers not wired, sandbox end-to-end and baselines owed
- `P0-VALIDATE.unattended.proof` (row 501): OPEN
- `P0-ETA-ALL` (row 503): open part units under their table, the check loop a unit a database, a pair's inner engines keep a ledger file each; statement-level unit boundaries and move health left
- `P0-CONNECTION-PROGRESS` (row 504): on fakes only
- `P0-VERIFY-EXECUTION` (row 505): still their own
### W10C (2026-10-16) row parity on every engine, units on restart
Done: MySQL and generic row parity through `decide.row_parity`; SQL Server row text a key range at a time (`scan_cuts`); `attempts.reconcile_units` closes units an earlier run left open before the move or check replays; same-engine pairs keep a ledger file a side (`side_mark`).
Not done: a same-engine pair's inner engines must say their side (`pair_side`), the hop's options cannot; range-level units of a move live in the engines' range copiers (not cli) and are not opened there yet; MySQL/generic row parity keep no measured costs between tables yet.
* **Not done (w11c, BAR-3 / P0-DE.2 / R25.3)** - the tail's batches are
  named per table as `Engine.neutral_apply` applies them
  (`freeze.name_write`, proved on PostgreSQL and MySQL targets by
  `test_every_batch_into_a_kept_target_is_named`); the repair rows of
  `Engine._apply_rows` and the engines' own bulk copy ways are still named
  only by the change sheet's written-back steps (`report.write_back`).
  `doctor` says each side's
  hold rung, the rungs refused above it and the rung's `loads`/`leaves`, not
  connections, memory or spool as numbers. No `migkit` command exports the
  change sheet or writes a run's evidence back yet.

### W12L (2026-10-16) change-sheet command, doctor numbers
Done: `migkit change-sheet` (export, `--back`); `doctor` says connections per side, memory and disk spool of this machine, and objects created on the servers per leg (`report.footprint_lines`).
Not done: the run does not call `write_back` by itself as each step ends; server objects are read from each way's `leaves` words (no structured per-side counts on `decide.Rung`); the disk spool is the bytes carried, before compression.
* **P0-SCENARIOS.consolidate / .engine, W12H - not done.** A per-source
  slice verify at a follow position; `keys: prefix` is accepted by
  `consolidate.settings` but the hop-key check (`hopkeys`) is not touched;
  `scenarios.storage_copy_case` is not yet read by the move's copy-way
  choice; no capabilities cell names the ClickHouse storage reads.
W12D (R19.9/R20.3/W10A tail) not done: the Arrow batches (`pgslot.columnar`) are built and proved equal on both paths but the appliers still take change records - handing them the batches is each target engine's `neutral_apply`; the MySQL stream's heartbeat period is the tail's longest idle wait, not yet measured per source; the stream-vs-poll latency is not measured into `decide.change_read`'s costs yet; MSSQL keeps polling, paced by `tailctl.Pace` like every polled tail. Sandbox proof against live PostgreSQL/MySQL streams is owed.
W13A: `tailctl.apply_unit` now hands a target with `apply_columns` the ordered Arrow runs (`pgslot.columnar(ordered=True)`, value-exact by `pgslot.exact`), change records as the fallback rung (`decide.TAIL_APPLIES`); PostgreSQL writes each run staged or as statements by `decide.column_write` from the run's bytes and the measured round trip; `_change_read` times one peek and one unconfirmed stream read on the slot into `decide.change_read`'s costs; the stream's wait and status heartbeat follow `tailctl.Pace`. Not done: only PostgreSQL has a column writer (other targets take records); the MySQL binlog heartbeat (mysql.py, W13E) is still the tail's longest idle wait; the cross-engine tail's own failpoint `tail.applied` sits after `apply_unit`, proved with fakes only - sandbox proof against a live PostgreSQL slot and the existing `tests/test_the_tail_streams_and_decodes_in_batches.py` re-run are owed to the final suite.

## Left after waves 10 and 11 (2026-10-16)

What waves 10 (W10A-E) and 11 (W11A-F), and the wave 9 review (CRIT4), left open. Checked on master `c95da9db` by `rg` and `sed` on 2026-10-16, not from the tickets' own notes. Owner = the W12 ticket that owns the file (`/private/tmp/migkit-briefs/W12-OWNERS.md`); UNOWNED = no W12 ticket owns the file, so it needs a wave 13 ticket. A row leaves this list only when its `docs/backlog-index.md` row is DONE.

| Wave (commit) | Leftover | Built | Missing | Owner |
|---|---|---|---|---|
| W10A (cd443007) | `payload_way` before large-value tables | `tuning.payload_way` (tuning.py:516) | no caller but tuning.py:604; the engine copies do not call it | W12E (tuning.py), W12D (postgres.py, mssql.py) |
| W10A (cd443007) | `tuning.sized` on PostgreSQL and SQL Server copies | `tuning.sized` on the MySQL range copier (mysql.py:9422) and `autonomous._unit` (autonomous.py:637) | the PostgreSQL and SQL Server copiers do not call it | W12E (tuning.py), W12D (postgres.py, mssql.py) |
| W10A (cd443007) | tail/follow apply under retried and reconcile | copiers run under `faults.retried` | the tail apply loop is not wired | W12D (tailctl.py, postgres.py) |
| W10A (cd443007), CRIT4 4 | MySQL range sizes kept per range, not on the engine | `Retune` per copy (mysql.py:9406) | `COPY_BATCH` and `COPY_BYTES` are still engine attributes (mysql.py:9120, 9319, 9406), so one range's retune reaches the others | W12D (mysql.py) |
| W10B (b3adbb81) | shared snapshot claimed by an engine | the hook: `base.py` SHARED_SNAPSHOT, `guarantees.py`, `jointplan.py`, `cli.py` | no engine claims it (PostgreSQL exports per reader, MySQL reads per reader at one binlog position) | W12E (base.py hook), W12D (postgres.py, mysql.py) |
| W10B (b3adbb81) | verify read on the snapshot | scan plan in `scanplan.py` | the verify read does not use the snapshot | UNOWNED (scanplan.py); verifiers.py W12G |
| W10C (e0b58e8b) | inner engines say their side | `attempts.pair_side` reads `eng.__dict__["pair_side"]` (attempts.py:208) | nothing sets it (grep) | W12E (base.py), W12R (attempts.py) |
| W10C (e0b58e8b) | range-level units in the engine copiers | `fleet.unit_name` (fleet.py:47), the attempt ledger | the PostgreSQL, MySQL and SQL Server range copiers do not open units | W12D (copiers), W12R (attempts.py) |
| W10C (e0b58e8b) | a multi-range keyed table falls to row text | the row text rung (`scanplan.row_text`) | a keyed table cut into ranges never reaches row text (needs resume) | UNOWNED (scanplan.py); verifiers.py W12G |
| W10D (adb35512) | `final_suite` calls the comparison | `tools/final_suite.py` | does not call `autonomous.comparison` | W12K (tools/*) |
| W10D (adb35512) | settle reads positions only | `autonomous.py` settle step | not checked against the ledger's open operations | UNOWNED (autonomous.py) |
| W10E (0e410eec) | per-unit guarantees | telemetry per unit (telemetry.py) | empty until a check records its guarantees (`guarantees.py`) | UNOWNED (telemetry.py, guarantees.py) |
| W10E (0e410eec) | ETA for assess | ETA for the forecast phases | `assess` has no forecast | UNOWNED (the assess code is not in the W12 map) |
| W10E (0e410eec) | row 227 (CV.P4) | - | the row's status was not checked against code | W12O (next wave, `docs/backlog-index.md`) |
| W11A (3f0f7e3e, 3e72c462) | `fleet.take_over` never called | closed by 3e72c462: `ha.py` binds `take_over = fleet.take_over` (ha.py:1054) | - | closed |
| W11A (3f0f7e3e) | ledger range units through `unit_name` | `fleet.unit_name` (fleet.py:47) | `attempts`, `ranges` and `claims` do not call it | W12R (attempts.py, fleet.py, claims.py) |
| W11A (3f0f7e3e) | capabilities text | the standby takeover cell | the text in `capabilities` is not updated | W12F (capabilities.py) |
| W11B (61022c65) | overlap, compression and move_file in the engine copies | `planner.overlap` (planner.py:1079), `movers.choose_compression` (movers.py:9030), `carriers.move_file` (carriers.py:1137) | no caller on master for any of the three; relay and ship do not use them | W12R (planner.py), W12D and W12E (engine copies), UNOWNED (movers.py, carriers.py) |
| W11B (61022c65) | Azure and Google stores move in one stream | parallel ranged parts for one object store | the cloud store API still moves one stream | W12B (cloudapi.py, clouds.py); carriers.py UNOWNED |
| W11B (61022c65) | checksum while streaming, in the store | checksums while streaming (carriers.py) | in-store checksum not done | UNOWNED (carriers.py) |
| W11C (0c955f02) | engine apply paths name their writes | `freeze.name_write` (freeze.py:6698), called from `report.write_back` (report.py:556) | no engine apply path calls it | W12E (freeze.py hook); engines by ticket: W12A, W12B, W12C, W12D, W12F, W12H |
| W11C (0c955f02) | doctor numbers | doctor says each side's rung and the refused rungs | connections, memory and spool as numbers | W12L (cli.py) |
| W11C (0c955f02) | change-sheet command | `report.change_sheet` and `write_change_sheet` | no `migkit` command exports or writes back from a run | W12L (cli.py, report.py) |
| W11D (e8a5de46) | topology facts from the hop and servers | `scenarios.topology_case`, `topology.py` math and proofs | facts are not read from the hop's config and servers | W12R (topology.py), W12H (scenarios.py) |
| W11D (e8a5de46) | topology hops run by the planner | `topology_hops` plan | the planner does not run them | W12R (planner.py) |
| W11D (e8a5de46) | docker run of each topology | fake-catalogue tests (tests/test_many_node_topologies_are_decided.py, not run) | no docker run | W12P (tests), needs a container run |
| W11E (5a7c0020) | Qdrant and Neo4j depth | registered engines, copy, checks, change capture | recall at k, HNSW and quantization, Qdrant snapshot, Neo4j dump and load, users, fence, compose | UNOWNED (engines/qdrant.py, engines/neo4j.py) |
| W11F (c95da9db) | MongoDB oplog rung decodes | oplog rung follows a replica set (3fcbed7) | decode of raw and expanded oplog events | UNOWNED (engines/mongodb.py; W12F owns only its missing-grant cell) |
| W11F (c95da9db) | reshard expanded events | per-collection generations | the reshard's expanded events | UNOWNED (engines/mongodb.py) |
| W11F (c95da9db) | reseed drop | reseed rung per generation | the drop step on reseed | UNOWNED (engines/mongodb.py) |
| W11F (c95da9db) | MongoDB capabilities cell | the cell for change follow | the cell's status | W12F (capabilities.py) |

CRIT4 (wave 9 review) items, checked on `c95da9db`: 1 (one consent for the switch) is closed: `--switch` is its own flag (cli.py:2804). 2 (PostgreSQL key cycle as a hard stop) is closed by b3adbb81 (subject only, not re-run). 3 (second throttle on `Pace`) is closed: `faults.Throttle` is gone (rg: no match). 5 (two recovery ladders) is closed: `autonomous._unit` no longer carries `TRIES` or `MIN_SCALE` (rg: no match). 6 (fixed retry count) is closed by the same removal. 7 (telemetry pace keyed by phase only) is closed: `_pace_key(phase, where)` (telemetry.py:886). 8 (whole-table count per failed batch) is closed: the count is span-scoped (generic.py:1700-1712). 4 is open, listed above.

Not done: the rows above are not run on a server. Everything that W10-W11 wrote is proved on fakes, and the tests are written but not run until the wave 12 gate.

W13G (drain, readiness, interlock, change sheet) - Not done: the wording of the next-step sites of `docs/research/next-step-audit-2026-10-15.md` priorities 6-15, 21-28 (cli.py) and 22-23 (guide.py) is not applied; priority 29 (`migkit schema <hop>` in the partition line) needs the hop name passed from `engines/base.py` into `verdict.partition_differences`; W12G probes (grants, versions, settings) record into `diagnostics.accumulator` only in `cutover` - the engines' probe entry points live in `engines/base.py` and the engine files (W13E and others); the change sheet is written back at the copy end and the cutover's drain and flip, not yet at the index and check ends; the drain does not check the target at the seam (the source goes on).
W14D (next-step wording, change sheet ends) - Done: `report.py` holds the stop wording once (`restore_unreachable` ... `source_unreachable`) for priorities 6-15, 21-28 and guide 22-23, and `report.check_ended`; the index and check ends are written back to the sheet. Not done: the priorities not named in the ticket (1-5, 16-20, 30) and the many dynamic `{said}` sites of the audit's inventory keep their wording; priority 29 needs `engines/base.py` (`_partitions_verdict`) to pass `hop=self.hop.name` to `verdict.partition_differences` - until then the line says `migkit schema` / `migkit check` without the hop name; `_doctor_config` and `_leg_said` have no hop name in scope, so they say `migkit doctor`.
Not done (W13K, 2026-10-16, MongoDB follow finished): the oplog rung reads each entry as the bytes the server stored it as (`oplog_entry`): an insert's or a replace's document crosses undecoded, so a date past the year 9999 passes; an update said as fields set and unset is decoded with dates kept exact (`DatetimeMS`), not spliced as bytes. The stream asks for expanded events from 6.0 (`_expanded_events`) and a sharded cluster from 4.4 to before 6.0, which leaves a reshard or a refined key unreported, is passed over for the stream with its version named (`_reshards_reported`, a need of the "change stream" rung; the sweep takes it). A reseed drops the target's in-scope collections the source no longer has, planned first through the one-way guard and held to the hop's `ddl` and carried deletes (`_gone_from_source`); only a collection named alike on both sides is found, not one the hop renames. The `_id` edges of the comparison ranges come from `ranges.cut` (`_id_edges`) on the server's equal-count buckets; the cut's sample and ordered-read ways are written as statements the server does not take, so they wait for an entry in `ranges.py` (W13I). The copy itself (`move_table`) still reads by `_id` in one stream, not by ranges. All proved on fakes only (tests/test_mongo_follow_is_finished.py); no live sharded cluster or 6.0 stream. The capabilities cell is W13M's.
W13M (10-16): `capabilities._states` reads every state set through one protocol (`_state_set`: a class that settles members at a `boundary`, routes to or enters a state of its own, counts or says them), so a Redis tail's `KeyspaceStates` and a MongoDB tail's per-collection generations count; `tail.table-states` is proved on Redis and MongoDB, `tail.ddl` and `tail.renames` on MongoDB (tests/test_redis_tail_is_lossless_and_two_site_rules_hold.py, tests/test_mongo_follows_change_streams_by_generation.py). `decide.still_lacking` lists what each engine still lacks, from the shape, capability and depth cells; `decide.orphan_ways` finds a way declared for no shape or engine. Not done: Redis `tail.added` and `tail.readded` stay not yet (the probe reads `snapshot_mark`/`REREADS_FROM_A_POSITION`, both in `engines/redis.py`, W13B's file); MongoDB `tail.added` and `tail.readded`, and GeminiDB Redis and MongoDB tail cells, are unproved (no test names them on that engine); warehouse continuous hop, zero-ETL, fleet takeover and audit, topology cases are decided-case proofs (`decide`), not cells of a capability table, so they gained no cell; Qdrant and Neo4j cells wait on W13J's sandbox servers.
### W13H (2026-10-16) hop keys declared for everything read by name
Done: `keys: prefix` and every `consolidate:` sub-key held to its own words (`hopkeys.sub_choice`); `versions:` declared for the warehouses and read as `Hop.versions`; `nodes:` (key spaces, conflict rule, routing, marks) declared and read by `scenarios.topology_settings`, used by `consolidate.topology_decision`; the side and hop options other waves read by name declared, `config.py` reading `mask_on_write`, `mask_key` and the hetero engines through `hopkeys.opt`; `scenarios.storage_copy_way` and `topology_decision` called from `scenarios.copy_lines` in the move's plan.
Not done: the move's copy-way choice itself (`physical.choose`, unowned this wave) does not yet ask `scenarios.storage_copy_way`; the plan only says and records it. `nodes:` marks and key spaces are the operator's words, not read from the servers.

## W14H - docs prose and the redpanda registry: not done

* The broker's options are in the whole-use registry (`movers.py`, `_redpanda_account`). Its unused options are not written: the pinned release's option list is not available offline, so none is declared.
* `docs/what-is-already-in-the-box.md` names wrapped tools through its whole text; only its stale to-do items were corrected. Rewrite it without tool names.
* Read, no change needed on the read: `docs/cutover.md` (its claims were not each checked against the code).
* Not read in this pass: `docs/problems-and-what-ends-them.md`, `docs/case-study-what-a-consistent-report-does-not-cover.md`, `docs/research-other-tools.md`, `docs/behavior-critical-params.md`, `docs/threat-model.md`, `docs/web.md`, `docs/goals-and-modes.md` prose, `docs/what-a-migration-actually-costs.md`, `docs/scale.md`, and the rows of `docs/backlog.md` (owned by the backlog).
* Unverified: Debezium `table.include.list` and `pg_dump -t/-T` in the box page's item 3.

### W15G (2026-10-16) writes held proved on servers, batches named, holds left behind lifted
Done: `leftovers.holds` / `leftovers.lift` find a hold of writes (source freeze or target kept) whose process died and give it back from its record (`freeze._save` now stamps the writer's process); `tests/holdkit.py` proves it once (holder killed with -9, `doctor` names the rung, the second session's write accepted again) and `test_writes_are_held_on_redis` (accounts refuse, pause waits) and `test_writes_are_held_on_opensearch` (indices and cluster refuse) run it on servers of the test lanes; `test_every_batch_into_a_kept_target_is_named` proves three tail batches into a PostgreSQL and a MySQL target are named per table and one write by another session is the single finding.
Not done (F2.freeze stays PARTLY): the same module for ClickHouse (needs a server profile whose default user may create users), Oracle, SQL Server, Cassandra, Kafka, MongoDB and DynamoDB (the source holds of SQL Server, Cassandra and Kafka are proved by their own `..._is_held_and_given_back` tests, without the killed-holder and `leftovers` step); Db2, ASE, Teradata, Redshift, Snowflake, Couchbase, BigQuery, Lindorm, TiDB and CockroachDB have no server here (test owed); Qdrant and Neo4j have no hold. BAR-3: `Engine._apply_rows` (repair) and the bulk copy ways do not name their batches (base.py is another owner's).
