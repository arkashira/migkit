# Backlog index

One line per distinct work item in `docs/backlog.md`. Built 2026-09-29 from
the backlog at `93aa147`, and brought up to date 2026-10-01 at `d579a34`
(`docs/backlog.md` at 7,212 lines, `docs/research/WAVE-STATE-2026-09-28.md`
as it stands there, and `git log --oneline 93aa147..d579a34`, where every
feature merge is one squashed commit). Where the same work is described in
several sections it has one line, with every source listed.

**Status.** DONE cites the commit that merged it, or "in text" where the
backlog itself says done (most of those predate `93aa147`). The commits
since 2026-09-30 follow the owner's rule - tests written with the feature,
the full suite run once at the end - so DONE on those means built and
merged with its tests written, not yet run. PARTLY cites the commits and
says what is built ("done:") and what is not ("missing:", from the
backlog's "Not done" notes); where an agent is working on the rest, an
IN PROGRESS row of its own says so. IN PROGRESS names the task of an agent
that was running when this was written, and nothing else is IN PROGRESS.
BLOCKED gives the reason. NOT PURSUED is used only where the backlog says
so. Where the backlog was unclear, the code on master was grepped and what
was found is noted as "grep:".

**Sources.** `Lnnnn` = line in `docs/backlog.md` at `d579a34` (the rows kept
from the first build were remapped from `93aa147`'s numbering through the
diff). `WAVE` = WAVE-STATE. Report names refer to `docs/research/*-2026-09-28.md`,
and `Lnnnn` after a report name is a line of that report.

**IDs.** Section-based: P0/P1/P2 item numbers as the backlog numbers them
(`1`-`46`, `0`, `0a`-`0f`), `R0`-`R25` (research round 2026-09-27 and after),
`F0`-`F7` (`F2.*`: the cutover's gaps), `W1`-`W9`, `S1`-`S4` (the structural
four), `CV.P1`-`CV.P21` (leapfrog conversion), `ER1`-`ER7` (leapfrog engine
reach), `L1`-`L8` and `LT.R1`-`LT.R10`, `LT.C1`-`LT.C6`, `LT.P1`-`LT.P3`
(leapfrog live tail; `LT.` keeps them apart from R1-R22), `DTS.*` (the DTS
gaps in "Paused"), `AUD.*` ("Audited again 2026-09-27"), `DEC.*` ("Decided
by the owner 2026-09-28"), `DEC30.*` ("Decided by the owner 2026-09-30"),
`P0-DEPTH.*` and `P0-PARITY.*` (owner, 2026-10-01), `P0-SMART.*` (owner,
2026-09-30), `P3.*` (the P3 table), plan and problem items by their own
names, `LADDER.table` and `P0-AUDIT` (WAVE queue items).

## Summary

395 items.

| Status | S | M | L | Total |
|---|---|---|---|---|
| DONE | 86 | 111 | 18 | 215 |
| PARTLY | 3 | 12 | 12 | 27 |
| IN PROGRESS | 5 | 20 | 8 | 33 |
| OPEN | 38 | 40 | 17 | 95 |
| BLOCKED | 8 | 4 | 6 | 18 |
| NOT PURSUED | 7 | 0 | 0 | 7 |
| **Total** | 147 | 187 | 61 | **395** |

| Area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED | Total |
|---|---|---|---|---|---|---|---|
| move | 24 | 5 | 3 | 11 | 0 | 2 | 45 |
| verify | 35 | 2 | 1 | 9 | 1 | 1 | 49 |
| change follow | 35 | 2 | 0 | 19 | 5 | 0 | 61 |
| two-way | 2 | 2 | 2 | 2 | 0 | 0 | 8 |
| cutover | 8 | 2 | 6 | 4 | 0 | 0 | 20 |
| types | 11 | 0 | 0 | 2 | 0 | 0 | 13 |
| engines | 20 | 1 | 6 | 11 | 5 | 1 | 44 |
| conversion | 18 | 4 | 0 | 9 | 0 | 0 | 31 |
| security | 10 | 1 | 0 | 5 | 1 | 1 | 18 |
| ease of use | 12 | 2 | 0 | 9 | 1 | 1 | 25 |
| operations/scale | 22 | 2 | 2 | 13 | 5 | 1 | 45 |
| tools used whole | 18 | 4 | 13 | 1 | 0 | 0 | 36 |
| **Total** | 215 | 27 | 33 | 95 | 18 | 7 | **395** |

## Items

| # | ID | Item | Status | Size | Area | Sources |
|---|---|---|---|---|---|---|
| 1 | P0-DE | Decision engine: rungs that say what they give, need, prove and cost; the climb; composed strategies; the choice kept beside the position; shape x engine coverage; pick/fitted/plan/_verify_way/loops_prevented routed through it | DONE 32ed544 | L | tools used whole | L103-229; WAVE a84f1b9 |
| 2 | P0-DE.1 | Mover and path chosen on speed from measured rates: the planner's speed rules as rungs ranked by the benchmark; a migkit-owned mover only against the benchmark | PARTLY 4e92945, 147f1c9, f8e1d33, fed3f89, 0c89d67, 4715ded (done: the ways of a table ranked by seconds measured on the hop - MySQL's bulk rungs, PostgreSQL's pass-through, SQL Server's bulk against statements, Oracle's writes, the write paths, split or whole streaming copy; missing: the planner's speed rules and `movers.pick` ranked by the benchmark (waits on item 28's measurements), and the MySQL and PostgreSQL choosers as one ladder - LADDER.table, IN PROGRESS) | M | move | L191-193, L231-235, L517-519, L2240 (plan 14), L7143; WAVE "left: ranking movers", queue "planner speed rules" |
| 3 | LADDER.table | One ladder for the table ways of MySQL and PostgreSQL: MySQL's bulk rungs (dump and load, straight copy, migkit's copier) and the table copier's ways (pass-through, rows, per-table dump), and PostgreSQL's, chosen by one chooser in one place | IN PROGRESS (table-way ladder agent) | M | move | WAVE 'QUEUE ITEM (orchestrator)'; 147f1c9, f8e1d33 |
| 4 | P0-DE.2 | `doctor` names the rung each side and leg stands on, and its footprint | PARTLY 395c8e5, 601f198 (done: `doctor` names each two-way side's rung and what it leaves, and says where migkit should run; missing: the rung every other side and leg stands on, and its footprint) | S | ease of use | L149-151, L236 |
| 5 | P0-DE.3 | Two-way mark ladder per side (PostgreSQL origin / logical message / table; MySQL tagged GTID; MariaDB domain and skip_replication; statement comment; table), proved by a probe, ranked by measured cost, kept in the tail's token, named by doctor, `migkit_origin` dropped at teardown | PARTLY 395c8e5 (done: the ladder per side - PostgreSQL origin / logical message / table, MySQL tagged GTID / statement comment / table, MariaDB skip flag / table - proved by a probe, ranked by measured cost, kept in the tail's token, named by `doctor`, taken down by `--drop`; missing: MariaDB's GTID-domain rung, not built until measured) | L | two-way | L237-238, L4999-5006, L5033, L5036-5104, L7151-7163; WAVE a4d532b |
| 6 | P0-DE.4 | Every shape has a way: close the "not yet" cells of `decide.coverage()` (58 of 230 at 32ed544) | OPEN (grep: `decide.coverage()` at d579a34 - 70 of 280 cells not yet, 6 undeclared (Iceberg and Delta x keyless, no unique index, wide key); was 58 of 230 at 32ed544; the gaps are three cross-cutting ones, P0-PARITY) | L | move | L166-182, L223-229; P0-PARITY L2839-2842 |
| 7 | P0-KEYLESS | Keyless tables copied, checked and resumed by whole-row hash buckets (duplicates by count), including sources with no stored row position (MySQL, SQL Server) | DONE 0a4276a: `ranges.keyless` - pages, buckets of the rows' canonical hash (the check's `row` leaves) or the source's own hash, one pass; a bucket begun and not done emptied on the target by the same hash; PostgreSQL, MySQL, the pair's copier; SQL Server, DuckDB, ClickHouse by their own hash | M | move | L174-182, L228-229, L2270-2276 (C3), L2294-2299 |
| 8 | BAR-1 | Neutral "migkit wrote this" mark across any engine pair: first write in the engine's atomic unit (MongoDB txn, Cassandra logged batch, Redis MULTI/EXEC, DynamoDB transact-write, Kafka header) | PARTLY 395c8e5 (done: PostgreSQL, MySQL, MariaDB; missing: MongoDB transaction, Cassandra logged batch, Redis MULTI/EXEC, DynamoDB transact-write, Kafka header) | L | two-way | L246-257 |
| 9 | BAR-2 | Exactly-once batches on every engine (the batch number carried in the mark where no native progress exists) | PARTLY 7de74bc, 63ad3f5, bd20c46 (done: PostgreSQL and MySQL counter hops; a one-way tail numbers its batches on the table rung wherever a batch is one transaction; warehouse and lake sides by their own offsets; missing: other one-way numbering rungs) | L | change follow | L258-266 |
| 10 | BAR-3 | Target kept read-only, or its writes named, on every engine | PARTLY 97dc747 (done: the hold on writes for PostgreSQL, MySQL and MongoDB, `freeze.FREEZES`; missing: the other 24 engines - F2.freeze IN PROGRESS, one neutral hold-writes primitive per engine, used for the cutover's source freeze and to keep a target from the application; P0-PARITY) | M | cutover | L273-275, L7141-7142 (DTS gap "read-only target on more engines"); P0-PARITY L2846-2849 |
| 11 | 0f-DVT | DVT used whole: row hash, custom queries, threshold, grouped columns to bisect then migkit's own row compare, random-row sampling, filter-status fail, YAML batches, labels | PARTLY 5713daf (done: row hash, custom-query column validation for views, threshold from the mapping's tolerance, grouped columns as a localizer rung, random-row sampling, failures only, one YAML batch a run, labels, explained in migkit's words; missing: per-partition runs of a table too large to read row by row a second time (`generate-table-partitions`), SQL Server through the same narrowing) | M | tools used whole | L303-315; WAVE a075608 |
| 12 | 0f-RELADIFF | reladiff as a proved rung, not a default (no jsonb rendering, breaks on Python 3.14), with the reason said where not used | DONE 5713daf | S | verify | L2788-2790; WAVE a075608 |
| 13 | 0f-PGCOPYDB | pgcopydb used whole: split-tables, index/restore jobs, LO jobs, binary COPY, skip vacuum/analyze, list progress and summary into the planner's rates, sentinel as lag; jobs sized from migkit's facts | DONE 4715ded (every option used, or unused with its reason, in the registry) | M | tools used whole | L397-412, L3083, L3290 (F7); WAVE ad4afda |
| 14 | 0f-DEBEZIUM | Debezium used whole: skipped.operations, column mask/truncate/include, transaction metadata, snapshot select overrides and locking mode, batch/queue sizing, time/binary modes aligned to canon, SMTs, pause/resume/stop-snapshot, notification channel | DONE 4715ded | M | tools used whole | L413-427 |
| 15 | 0f-MYSQLTOOLS | mydumper/myloader and MySQL Shell used whole (--rows, --checksum-all, masking, --where; Shell dump, load, copy), ranked per table by measured rate | DONE 147f1c9 | M | tools used whole | L481-483, L3080-3082, L3290-3291, L5739-5753 (R14); WAVE ae351bc |
| 16 | 0f-REGISTRY | Used / unused / topped-up table per wrapped tool, kept like `capabilities.matrix`, a reason for every unused feature | DONE 4715ded, afbf0a3 (`tooluse`: one registry for every wrapped program and verifier) | M | tools used whole | L499-502, L7085-7095; WAVE ad4afda |
| 17 | 0f-REST | Whole-use audit of the rest: pg_dump/pg_restore, pgloader, mongosync (/start body, /reverse, /progress), mongodump, redis-shake, MirrorMaker 2, DSBulk, CDM, clickhouse-backup, elasticdump, atlas/liquibase/migra, sqlglot, drivers (asyncpg, pymongo, valkey-glide) | PARTLY b4e4d98 (done: RedisShake, MirrorMaker 2, DSBulk; missing: pg_dump/pg_restore, pgloader, mongosync, mongodump, CDM, clickhouse-backup, elasticdump, atlas/liquibase/migra, sqlglot, the drivers) | L | tools used whole | L480-498 |
| 18 | 0 | Planner: one decision per table with its reason, read out in the dry run | DONE (in text 2026-09-24; routed through decide.py in 32ed544) | M | move | L504-556 |
| 19 | 0e | Declared capability matrix, held by a test both ways | DONE (in text 2026-09-24) | M | engines | L558-573, L719-725 |
| 20 | 0e.say | Operator told in migkit's words when a command is not available for an engine | DONE (grep: capabilities.require/unavailable in cli.py) | S | ease of use | L571-573, L726-727 |
| 21 | 0e.fill | Gaps closed 2026-09-24/25: SQLite through the pair copier; MongoDB, Redis, Kafka, SQL Server copy, follow, fence, users, settings, restore points; empty-move guard everywhere | DONE (in text) | L | engines | L603-711, L729-767 |
| 22 | 0e.empty | The cross-engine copier replaces what it copies (a neutral "empty this table") instead of only upserting | DONE (grep: neutral_empty on every engine) | S | move | L768-774 |
| 23 | 0e.exclude | `exclude` read on every engine, SQL Server included | DONE (grep: mssql.py reads hop.excluded) | S | move | L776-781 |
| 24 | 0c | Progress and logs in migkit's own words; one progress vocabulary; no program named | DONE (in text) | M | ease of use | L551-556, L789-840 |
| 25 | 0d | Row filter honoured by move, check, repair, pair and tail | DONE (in text) | M | verify | L842-899 |
| 26 | 0a | DVT wrapped out of process as the second reader, wired into `check` by the planner | DONE (in text 2026-09-24) | M | verify | L901-986, L1789-1794, L5316-5318 |
| 27 | 0a.measure | Second reader measured: speed against migkit's checksum at size; type agreement with canon for json, time, bit, spatial and other engines | OPEN (5713daf measured one PostgreSQL pair at 200,000 rows and, on MySQL to PostgreSQL, floats and fixed-width text; speed at size and json, time, bit, spatial still) | S | verify | L980-983 |
| 28 | 0b | Re-read through the stream without pausing it (read-only incremental snapshots, MySQL and PostgreSQL) | DONE (in text) | M | change follow | L988-1055 |
| 29 | 0b.window | A key changed inside a chunk's window, measured | BLOCKED (needs a machine with room for the Kafka Connect stack; the 4 GiB VM cannot) | S | change follow | L1057-1066 |
| 30 | 1 | Confirm before calling it different: fence per engine, cross-engine, replication migkit does not drive, SQL Server | DONE (in text; SQL Server by R13 2026-09-27) | M | verify | L1070-1136, L5623-5624 |
| 31 | 2 | Source snapshot held for a bounded time (`snapshot_limit`), its age reported | DONE (in text) | S | verify | L1138-1184 |
| 32 | 3 | Target frozen against the application's writes (PostgreSQL, MySQL) | DONE (in text) | M | cutover | L1186-1255 |
| 33 | 4 | Repair that knows the stream is running: own tail, subscription and replica paused; refused beside others | DONE (in text) | M | verify | L1257-1318 |
| 34 | 5 | DDL during the move: seen on every path, the tail stops, verdicts it overtook marked stale, the native replica scoped to the hop | DONE (in text) | M | change follow | L1320-1423 |
| 35 | 5b | MySQL full+cdc from one point; `--mode cdc` after a separate copy | DONE (in text) | M | change follow | L1425-1466 |
| 36 | 6 | Pre-checks give the value to set: tables over 10k, case collisions, slots/senders, binlog settings, packet vs largest row, other writers, decoding spill, target room with its log, max_slot_wal_keep_size, key-less columns a replica cannot match, HA/replicas on the target | DONE (in text; R9 2026-09-27; grep: max_slot_wal_keep_size in postgres.py) | M | ease of use | L1470-1711, L5338-5357 |
| 37 | 6.mariadb | MariaDB's compressed row events read instead of stopping the tail | OPEN (grep: still refused, binlog_payload.py:18, mysql.py:4925) | S | change follow | L1505-1508 |
| 38 | 6.wal_sender | `wal_sender_timeout` recommendation | NOT PURSUED (left open on purpose until a failure shows a value) | S | ease of use | L1565-1573 |
| 39 | 6.fkwin | Foreign-key window around the PostgreSQL streaming copy, instead of falling back to the local copy | OPEN (grep: no FK window) | M | move | L1627-1630 |
| 40 | 7 | Cost in the plan: time from measured rates (as ranges, item 40), size and room, transfer price, free space where the server reports it | DONE (in text) | M | operations/scale | L1713-1753, L4657-4673 |
| 41 | 7.free | Free space of PostgreSQL and MySQL targets (system_stats, or the cloud's metrics) | BLOCKED (not reported over SQL; the cloud's metrics need an account) | S | operations/scale | L1736-1749, L5345-5352 |
| 42 | R9.docdb | DocumentDB pre-flight: change streams per collection, retention, DDL by catalogue snapshots (problems E4) | BLOCKED (no DocumentDB here) | M | engines | L2303, L5346-5347, L5362-5363 |
| 43 | 8 | Business-rule checks (SQL, MongoDB pipelines) | DONE (in text) | M | verify | L1757-1794 |
| 44 | 9 | Column keep/drop/rename honoured on every path | DONE (in text) | M | move | L1796-1952 |
| 45 | 10 | Newer-row-wins conflict policy (PostgreSQL, MySQL) | DONE (in text) | S | verify | L1954-1970 |
| 46 | 10.more | Newer-row-wins on more engines | DONE 0a4276a: every pair (`Engine._apply_rows`), SQL Server (its row repair, no longer by hand), MongoDB, Redis hashes, the generic engine | S | verify | L7142 |
| 47 | 12 | MongoDB to MongoDB through mongosync | DONE (in text) | M | move | L2129-2194 |
| 48 | 13 | Kafka group offsets translated by the message they point at | DONE (in text) | M | cutover | L2196-2228 |
| 49 | plan-7 | What the target does to rows as they land (triggers quieted, identity) | DONE (what-a-migration-actually-costs.md item 7) | S | move | L2232 |
| 50 | plan-10 | Documents the table only points at (large objects behind oid columns) | DONE (plan item 10; carried by R10) | S | verify | L2233 |
| 51 | plan-18 | Continuous verification that costs what changed (pairs) | DONE (in text 2026-09-25) | M | verify | L2241-2246 |
| 52 | plan-19 | Bisection diffing across engines on the canonical rendering | DONE (in text 2026-09-25) | M | verify | L2247-2255 |
| 53 | A1 | Scope beyond the database (cron jobs, application config, downstream consumers) | OPEN (problems file A1 still Partly) | M | operations/scale | L2260 |
| 54 | A2/D10 | Privileges down to columns | OPEN | M | security | L2261, L7141 |
| 55 | A4 | MariaDB objects MySQL has no home for | DONE (in text 2026-09-25) | S | engines | L2262 |
| 56 | B4 | Collations mixed inside stored code (COLLATE clauses parsed from routine bodies) | OPEN (R9 names MySQL routines written under another collation) | M | types | L2264, L5358-5361, L7141 |
| 57 | B6 | Values the target refuses: zero dates named before a cross-engine move | DONE (in text 2026-09-25) | S | types | L2265 |
| 58 | C1.done | Tables side by side, equal-row ranges, the shared copier pipelined and writing PostgreSQL through COPY | DONE 1da6415 | M | move | L2266-2269, L2278-2293 |
| 59 | C1.managed | Speed compared against a managed service: recipe run on the same instance class, cost stated | OPEN | M | operations/scale | L2266-2269, L3701-3704, L3716-3718 |
| 60 | C3.done | Each range checked and checkpointed; a finished table asked again; streaming bulk resumes from finished tables | DONE 1da6415, in text 2026-09-27 | M | move | L2270-2276, L2289-2299 |
| 61 | C3.repair | A statement-level record of a repair | DONE 0a4276a: `repairlog` - every row repair writes each statement before/after first and marks how far it got; `rollback` puts back only rows still as the repair left them; replayable | S | verify | L2275-2276 |
| 62 | C4 | Verification load on the source: the two engines still missing and a lag-aware brake for replicas | DONE 0a4276a: SQL Server's check and the cross-engine check braked; a replica's lag rising while read brakes; a PostgreSQL standby with nothing left to replay reads no lag | M | verify | L2300, L7142 |
| 63 | D11 | Large-object references parked in plain integer columns (invisible to the check) | DONE 0a4276a: integer columns taken for references by their values; every object both sides hold compared by digest a piece at a time | S | verify | L2301 |
| 64 | D15 | How much of a row a cross-engine comparison compares | DONE 0a4276a: every cross-engine verdict ends with the columns compared exactly, by value and as what, and not at all and why | S | verify | L7142 |
| 65 | F1.dual | Dual writes drift (problems F1) | OPEN | M | cutover | L2304 |
| 66 | F4.pool | Poolers named in `assess` | DONE (in text 2026-09-25) | S | ease of use | L2305 |
| 67 | G2 | `mask` for values the drilldown shows | DONE (in text 2026-09-25) | S | security | L2306 |
| 68 | P3.pw | pgcopydb given no password on its command line | DONE (in text) | S | security | L2312 |
| 69 | P3.idx | Index window names indexes schema-qualified | DONE (in text) | S | move | L2313 |
| 70 | P3.pub | `create publication` made only where missing | DONE (in text) | S | change follow | L2314 |
| 71 | P3.follow | `follow` ends at the current position; no long-running mode | DONE 63ad3f5: the leg run from here runs until asked with `follow_until: stopped` and ends at the source's position of that moment; without it a catch-up with an end, as before (`movers._follow_until_asked`) | S | change follow | L2315 |
| 72 | P3.generic | Generic engine reads string length and nullability | DONE (in text) | S | engines | L2316 |
| 73 | P3.health | MySQL `_health` reports replica lag and the throttle backs off | DONE (in text) | S | operations/scale | L2317 |
| 74 | P3.hetero-deep | Deep battery for cross-engine hops | DONE (matrix: hetero deep checks yes) | M | verify | L583, L764, L2318 |
| 75 | P3.sqlglot | sqlglot kept for defaults a type map cannot translate | DONE (in text, decided 2026-09-25) | S | conversion | L2319 |
| 76 | P3.measure | Measured before wrapping: boto3 Secrets Manager, mongodump --query/--oplog, mydumper --regex/--rows | DONE (item 38; R14) | S | tools used whole | L2320, L5737-5753 |
| 77 | P3.resumedump | Tell the operator when a restart loses the dump's progress (DTS DumperResumeCtrl, "resumable dump status") | OPEN (grep: none) | S | move | L2321, L7141 |
| 78 | P3.timed | Timed start and auto-retry window (DTS "start at a time") | DONE 7de74bc (grep: schedule.py retry_window, max_duration, catch-up) | S | operations/scale | L2322, L5196-5198, L5221-5225, L7140 |
| 79 | P3.dryrun | Should dry-run plans hide the command lines they print | BLOCKED (waiting on the owner) | S | ease of use | L2324 |
| 80 | 14/R6 | Network path migkit opens itself: SSH or command tunnels, doctor tells "connects but bulk stalls" apart, stall watchdog | DONE (in text 2026-09-27) | M | operations/scale | L2341-2353, L5251-5278 |
| 81 | R6.cloud | The clouds' own forwarders signed in (SSM, IAP, Cloud SQL proxy, Azure Bastion) | BLOCKED (needs cloud accounts) | S | operations/scale | L5262-5263 |
| 82 | 15 | Who wrote the rows only the target has (PostgreSQL, MongoDB, MySQL) | DONE (in text) | M | verify | L2355-2399 |
| 83 | 16 | Side scripts in tools/ folded into migkit or removed | DONE (in text) | M | operations/scale | L2401-2433 |
| 84 | 17 | MySQL events repaired, not only detected | DONE (in text) | S | conversion | L2435-2453 |
| 85 | 18 | Schema fix's undo rehearsed on a scratch database (PostgreSQL, MySQL) | DONE (in text) | M | cutover | L2455-2486 |
| 86 | 19 | Enums, domains, hstore, tsvector rendered across engines; same-engine types by their own text | DONE (in text; interval in F4.b) | M | types | L2490-2531 |
| 87 | 20 | PostgreSQL-only helpers ported where the idea exists (mojibake repair on MySQL) | DONE (in text; SQL Server row-level security in the SQL Server line) | S | engines | L2535-2553 |
| 88 | 21 | `setup_target_plan` for MySQL | DONE (in text) | S | move | L2555-2570 |
| 89 | 22 | Coverage of `unchanged_since` answered (PostgreSQL only; the rest re-read) | DONE (in text) | S | verify | L2572-2580 |
| 90 | 23 | The time zone the data is written in (deep check) | DONE (in text) | S | types | L2582-2603 |
| 91 | 24 | A target that is correct and slow: the source's reads replayed and planned on both sides | DONE (in text) | M | verify | L2605-2638 |
| 92 | 25 | Long reads over unstable links: MongoDB read in resumable chunks across key types | DONE (in text) | S | verify | L2640-2659 |
| 93 | 26 | Research of HVR, Qlik, Striim, Airbyte, Bytebase, Trino; the 2026-09-27/28 research passes with scorecards | DONE (docs/research-other-tools.md; 0b64ddb, 6d2675d, 6d5ebe5) | M | tools used whole | L2663-2680, L7097-7124 |
| 94 | 26.rest | sqlpipe and schemachange read | OPEN | S | tools used whole | L2679-2680 |
| 95 | 26.trino | Trino measured as a candidate second reader | OPEN | S | verify | L2678-2679 |
| 96 | 26.striim | Striim-style interval validation | OPEN | S | verify | L2674-2675 |
| 97 | 26.bytebase | Bytebase-style schema snapshot at each change | OPEN | S | change follow | L2675 |
| 98 | MSSQL-x86 | SQL Server at full depth on a real server: T-SQL paths run, CDC proper, Agent jobs, row-level security (item 27) | BLOCKED (full SQL Server is x86-only; the x86 CI workflow is merged in fed3f89 and runs once pushed) | L | engines | L753-759, L2552-2553, L2682-2689, L4377-4378, L4651-4653, L5523-5528 |
| 99 | AUD.part | Every partition held to its own count and digest (PostgreSQL, MySQL); A5 | DONE 659b54f | M | verify | L2704-2721 |
| 100 | AUD.slot | Slot made with its snapshot; MySQL position taken after commits in flight; property-tested | DONE 659b54f | M | change follow | L2722-2752 |
| 101 | AUD.slot.process | MySQL `copy_point` waits nothing without PROCESS | OPEN | S | change follow | L2753-2754 |
| 102 | AUD.slot.more | MongoDB resume token and MariaDB/Percona snapshot position measured the same way | OPEN | S | change follow | L2754-2755 |
| 103 | AUD.shift | Uniform time shift and type narrowing reasoned in one place, both engines | DONE 659b54f | S | types | L2756-2770 |
| 104 | AUD.osc | Online-rewrite working tables left out everywhere | DONE 659b54f | S | move | L2771-2781 |
| 105 | AUD.osc.tail | The change tail recognises gh-ost/pt-osc tables beside their table, not by name alone | OPEN (grep: hetero.py calls drift.transient on the name) | S | change follow | L2782-2783 |
| 106 | AUD.atlas | `schema_authority: atlas` alias shape for a renamed table | OPEN | S | verify | L2785-2787 |
| 107 | AUD.release | PyPI upload (is the name free) and a brew tap | BLOCKED (waiting on the owner) | S | operations/scale | L2791-2792 |
| 108 | P0-DEPTH.repair | Repair as a ladder in `decide`: rows by key in batches, the ranges the leaves name copied again, the table reloaded by the fastest mover - chosen by predicted cost and the size of the drift, with the same statement record, undo and proof after | IN PROGRESS (repair-ladder agent) | L | verify | L2803-2808 |
| 109 | P0-DEPTH.conflicts | Two-way conflicts beyond five whole-row policies: the changed-columns merge (F7.merge), min, max and delta resolutions, a policy per table and per column, every conflict and its resolution in the evidence export | IN PROGRESS (two-way conflict-resolution agent) | L | two-way | L2809-2813 |
| 110 | P0-DEPTH.constants | The constants set by reasoning (`canon.WIDE`, `BUCKET_ROWS`, `REPLICA_RISE`, `DIGEST_MEMORY`, the probe floor, `TIE`, the per-table program threshold, the rehearsal's sample spread) measured on the final run and kept in the decision engine's knowledge base, so a machine and a hop tune them from their own runs | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2814-2819 |
| 111 | P0-DEPTH.calibrate | The resource model calibrated on the final run (its one docker run named the wrong bottleneck in four of eleven cases and allows a 100% prediction error), its readings taken by P0-SMART's cost models as priors, not as truth | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2820-2823 |
| 112 | P0-DEPTH.hold | The pace holds a target (the source's load cap, the lag a replica may reach) from the hop's limits instead of only backing off when a resource strains | IN PROGRESS (P0-SMART agent) | M | operations/scale | L2824-2827 |
| 113 | P0-AUDIT | A whole-codebase audit of what is flat, not smart, slow or inconsistent, each finding backlogged and built; includes making the final suite run fast (container reuse, parallel colima profiles) | IN PROGRESS (flat-audit agent) | L | operations/scale | WAVE 'Owner (2026-10-01): do all of P0-DEPTH, and evaluate everything else' |
| 114 | P0-PARITY.lv | Large values on every engine: values read, written and hashed in pieces, memory sized from the widest row, wide values digested where they lie - one mechanism, an adapter per engine (22 engines not yet) | IN PROGRESS (P0-PARITY large-values agent) | L | engines | L2839-2842; WAVE 'Owner asked whether other engines can be deeper' |
| 115 | P0-PARITY.part | Partitioned tables on every engine: copy by partition (the partition as the range), partition coverage checked, partition-aware workers (15 engines not yet) | OPEN (queued) | L | engines | L2843-2845 |
| 116 | P0-PARITY.small | The smaller gaps: a missing grant on MongoDB and pairs; capability gaps on warehouses, ASE, Db2 and lakes (6-11 cells each); the generic engine deepened or declared a thin fallback (see ER1.acct, R13-DB2, R13-ASE, R13-GENERIC) | OPEN (queued) | L | engines | L2850-2853 |
| 117 | P0-SMART.cost | Cost as a function of the unit: start-up plus work over the binding resource's rate, with the facts that move it, fitted online per rung, engine pair and shape with its uncertainty | IN PROGRESS (P0-SMART agent) | L | tools used whole | L2887-2892 |
| 118 | P0-SMART.predict | A rung never run here costed by prediction from the resource model and what similar hops measured, with a wide spread; the plan says which numbers are measured and which predicted, and a measurement always beats a prediction | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2893-2895 |
| 119 | P0-SMART.explore | Safe exploration: an unmeasured rung tried on a small representative unit, never on a deadline's critical path, stopped once the best rung's lead is outside the spread (a bandit with a risk bound) | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2896-2899 |
| 120 | P0-SMART.plan | One plan, jointly: the way per table, workers, table order, index builds and verify passes as one schedule minimising the makespan under the hop's limits (deadline, the source's load cap, money, outage, target memory and connections) | IN PROGRESS (P0-SMART agent) | L | tools used whole | L2900-2905 |
| 121 | P0-SMART.replan | Re-plan while running: when a unit's rate leaves its predicted spread, re-climb for the ranges left, switching ways at range edges, and say why | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2906-2908 |
| 122 | P0-SMART.learn | Learn across hops: a local knowledge base (never leaving the machine) of rung performance and failure by engine pair, version, shape and link class seeds the next hop; migkit's own benchmarks ship as the first priors | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2909-2912 |
| 123 | P0-SMART.risk | Risk in the cost: a rung's chance of failing here, times the work a failure throws away, enters its expected time | IN PROGRESS (P0-SMART agent) | S | tools used whole | L2913-2915 |
| 124 | P0-SMART.limits | Constraints as needs, trade-offs said: the source's load cap, money and outage are hard limits where the hop sets them, and the plan names the trade-off | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2916-2919 |
| 125 | P0-SMART.compose | Every composition that fits costed and the dominated ones pruned; one facts catalogue (a rung reading an undeclared fact fails a test); correctness, consistency, idempotency, exactly-once and resumability never traded - each part declares its guarantee levels, a strategy holds the weakest, a weaker fast part only where migkit tops it up; the dry run proposes and `--go` decides | IN PROGRESS (P0-SMART agent) | L | tools used whole | L2920-2932 |
| 126 | P0-SMART.inside | The choices stay inside: the console shows outcomes only (what happens to each side, predicted time and outage with spread, guarantees, what the operator must do), the decision record goes to the run's log and the evidence export and to the console only with `MIGKIT_EXPLAIN=1`; no way or program is named on the console | IN PROGRESS (P0-SMART agent) | M | tools used whole | L2933-2943 |
| 127 | P0-SMART.counter | Counterfactuals in the plan and the evidence export: the chosen rung, its predicted time with spread, the next best and why not | IN PROGRESS (P0-SMART agent) | S | tools used whole | L2944-2945 |
| 128 | DEC30.offer | A faster way that needs a change on the source is offered, never assumed: one mechanism for every such change (`decide.Change`, `sourcechange`), probed on the server, yes made behind the `source-setting` approval and taken off again, no or impossible switches with its cost said; first case MySQL CLONE | DONE c7dc5a2 | M | move | L2949-2963, L2992-3012 |
| 129 | DEC30.fallback | A way that is blocked or fails is left for the next one by itself, logged, never on a verdict and never to a way weaker than the hop needs: one runtime fallback for every choice (`decide.attempt`) | DONE c7dc5a2, 63ad3f5, 0a4276a, 67de8e0 (routed through it: the bulk way down to the table copier, the physical seed down to the logical copy, the servers' own replication down to the leg run from here, grouped narrowing down to its walk, the tail's reader and lanes, the localizers, every engine's own switch through `Engine._attempt`) | M | tools used whole | L2964-2972, L2983-2988, L3018-3045 |
| 130 | DEC30.sweep | The ad-hoc fallbacks still in files other work owned, routed through `decide.attempt`: the per-table streaming copy to ranges and `my_bulk_choice` (`movers`, with LADDER.table), SQL Server's bulk load to statements, ClickHouse's pull to the copier, `movers.stream_way` | PARTLY 67de8e0 (done: `Engine._attempt` over 16 sites - SQL Server's bulk load to statements and its Arrow move, ClickHouse's pull, MySQL's loads to inserts, MongoDB's hash walk, PostgreSQL's passes, repair, roles and write probe, Redis's expiry, the pair's leaves, a warehouse's digest, DynamoDB's tags; missing: the per-table streaming copy, `my_bulk_choice` and the local dump (`movers`, with LADDER.table, IN PROGRESS), the pass-through's ranges to text (`pg_pass`), Oracle's digest in SQL to the fold, `movers.stream_way`, the tail's "copied instead" and `hetero._run_ddl`) | M | tools used whole | L3039-3045; WAVE 67de8e0 'Still ad hoc' |
| 131 | DEC30.goal | `goal: cutover` does not turn `cutover:` on, and every goal and mode is explained plainly in `init`, `assess`, the dry run and the docs (`hopkeys.GOALS`, `MODES`, `CUTOVER`, `docs/goals-and-modes.md`) | DONE c7dc5a2 | S | ease of use | L2976-2982, L3013-3017 |
| 132 | DEC.licence | Relicense to AGPL-3.0-or-later | DONE be382a4 | S | tools used whole | L3060-3072 |
| 133 | DEC.terms | Closed tools installed with the operator's acceptance (`doctor --install`, MIGKIT_ACCEPT_TERMS), the open path where terms forbid, licence check over every dependency | DONE 254c0f9, afbf0a3 (`MIGKIT_ACCEPT_TERMS`; `doctor --install` asks once for every closed term, pulled images included; a closed product's tool runs only where its terms allow; a test holds every dependency's licence) | M | tools used whole | L3065-3072 |
| 134 | DEC.deps | Every worthwhile tool a dependency: mssql-python, opensearch-py, confluent-kafka and the rest in pyproject; mydumper, MySQL Shell, RedisShake, DSBulk, Ora2Pg, SQLines, plpgsql_check through `doctor --install` | PARTLY fed3f89, ba59531, 254c0f9, 0c89d67, b4e4d98 (done: mssql-python, opensearch-py and the rest of the drivers in pyproject; mydumper, MySQL Shell, Ora2Pg, SQLines, OpenLogReplicator, RedisShake, DSBulk through `doctor --install`; missing: confluent-kafka (R18.7) and plpgsql_check's install) | M | tools used whole | L3073-3079 |
| 135 | DEC.ptcs | pt-table-checksum only on native replicas, always paired with migkit's sum digest | OPEN (grep: not used) | S | verify | L3085-3086 |
| 136 | DEC.colima | Second colima profile for Oracle Free and x86 engines, started on demand, docker context restored | DONE df60027, e4b5275, 53bf947 | S | operations/scale | L3115-3119 |
| 137 | DEC.x86ci | Free x86 CI on GitHub for SQL Server, Db2, ASE | DONE fed3f89 (`x86-engines.yml`: SQL Server 2022, Db2, SAP ASE; runs on GitHub once pushed) | S | operations/scale | L3119-3120, L3224-3225 |
| 138 | DEC.research | Leapfrog research before building (conversion, engine reach, live tail) | DONE 343d2ce, 13ef328, c716e3a | M | tools used whole | L3121-3125 |
| 139 | S1.a | Oracle changes read by migkit's own LogMiner reader (RDS-capable, no licence) | DONE 0c89d67 (`oracle_logminer`) | L | change follow | L2012-2013, L3088-3093, L3219, L3586-3594 |
| 140 | S1.c | A compiled Oracle change reader as a rung | DONE 0c89d67 (`oracle_redo`, the rung above LogMiner; x86-64 only, run end to end only where it builds) | M | change follow | L3090, L3593 |
| 141 | S1.d | OpenLogReplicator driven as a separate program next to the redo | DONE 0c89d67 (OpenLogReplicator run as a separate program beside the redo, built by `doctor --install` from source on Linux, never bundled) | M | change follow | L3090-3091, L3591-3592 |
| 142 | S1.e | Oracle hop accepted only after its reader is measured against the source's redo rate | PARTLY 0c89d67 (done: `assess` measures the reader against the source's busiest-hour redo and fails one that would fall behind; missing: a gate that stops a move on it - `assess` reports only) | S | change follow | L3091-3092, L3593-3594 |
| 143 | S3.link | Kafka Cluster Linking / Shadowing driven where the operator holds the licence | OPEN | S | cutover | L3103-3105, L3603-3606 |
| 144 | S4 | Zero-ETL where offered and Azure's MI link driven through the provider's API | OPEN | M | operations/scale | L3109-3113, L3607-3610 |
| 145 | F0.1 | MongoDB digest by raw-BSON hash ($toHashedIndexKey made every number an int64) | DONE 2a4c652 | S | verify | L3139-3142 |
| 146 | F0.2 | `prove_converted`: procedures never run, triggers and events unchecked, failing input not named | DONE 7dbe6e8 | S | conversion | L3143-3146 |
| 147 | F0.3 | SQL Server Change Tracking read under SNAPSHOT; rowversion out of the hash; CLR types hashed | DONE 2a4c652 | S | verify | L3147-3150 |
| 148 | F0.4 | Types G1-G4, G8: infinity, BC years, sub-microsecond digits, extra_float_digits, keys a target collation merges counted before the move | DONE 3b05234 | M | types | L3151-3156 |
| 149 | F0.5 | Cutover bugs: fence on the insert LSN, fence_wait on the hop's own slot, rollback raises the old source's sequences | DONE 97dc747 | S | cutover | L3157-3162 |
| 150 | F0.6 | MySQL digest: salted 64-bit md5 sum, no CRC lane | DONE 2a4c652 | S | verify | L3163-3165 |
| 151 | F0.7 | mongosync committed on its reported lag (canCommit); Redis absolute TTL, IDLETIME/FREQ, every cluster node | DONE 254c0f9 | S | move | L3166-3168 |
| 152 | F0.8 | PostgreSQL tail: a newline inside a value stalled it | DONE f3884fa | S | change follow | L3169-3170 |
| 153 | F0.9 | Outward: pt tools without calling home, Atlas Community build, Liquibase 4.x, leftovers (gh-ost _ghk, pgcopydb, pglogical, Spock, Bucardo, pg_repack, pgstream schemas), datacompy's report naming itself | DONE 254c0f9, 5713daf (datacompy's report naming itself) | S | security | L3171-3175 |
| 154 | F0.10 | Security defaults: certificates verified by default, md5 marked not for security, a publication of the hop's tables only, the dashboard token out of the URL | DONE 254c0f9 | S | security | L3176-3178 |
| 155 | F1.a | One scan returns every leaf of the digest tree; buckets by a hash of the key so composite, text and cross-engine keys localise; passes A-D composed | DONE 97028a6 | M | verify | L3180-3188, L6256-6258; WAVE a31b892 |
| 156 | F1.b | In-SQL IBLT: few differences found over a slow link, no key needed | DONE 97028a6 | M | verify | L3183-3185, L6257-6258 |
| 157 | F1.c | Self-check: the differences found add up to the change in count and sum | DONE 97028a6 (`leaves.localize`) | S | verify | L3185-3187 |
| 158 | F1.d | Generations under writes (migration-verifier style) | DONE 0a4276a: `leaves.generations` - differing leaves read again until they hold still; settled, still and never-settled said; the kept proof's rounds the same | M | verify | L3187-3188 |
| 159 | F2 | Cutover as one timed path with automatic undo, driven by a `cutover:` hop option (preflight, reverse armed and verified, freeze per engine, drain, final verify, counters, jobs, flip, budget); writes to the source only under `cutover:` | PARTLY 97dc747 (done: fence on the insert position and the hop's own slot, freeze read back for PostgreSQL, MySQL, MongoDB and poolers, drain, fenced check, counters, stream back proven, flip, every step undone on failure; outage measured 2.35-2.50 s PostgreSQL, 0.52-0.90 s MySQL; missing: F2.freeze, F2.marker, F2.armed, F2.ponr, F2.origin, F2.legs - IN PROGRESS, cutover-gaps agent) | L | cutover | L2235-2239 (plan 13), L3053-3059, L3190-3198; WAVE a2ba2fe |
| 160 | F2.freeze | The freeze on the source for Redis (client pause on writes), SQL Server (database read-only) and every other engine, as one neutral hold-writes primitive per engine, used for the cutover and to keep a target from the application (BAR-3) | IN PROGRESS (cutover-gaps agent) | M | cutover | WAVE 97dc747 'Not done'; 55a1939 (the cutover agent told: one hold-writes primitive per engine); research/cutover-2026-09-28.md section 14.3 |
| 161 | F2.marker | Marker-row drain where the engine gives no position (SQL Server, a third party's pipeline, cross-engine through Kafka), the verdict saying which proof it used | IN PROGRESS (cutover-gaps agent) | M | cutover | WAVE 97dc747 'Not done'; research/cutover-2026-09-28.md section 4, marker row |
| 162 | F2.armed | The stream back pre-armed before the flip where it is loop-safe (PostgreSQL 16 `origin = none`, MySQL GTID auto-position), enabled in the outage instead of built there | IN PROGRESS (cutover-gaps agent) | M | cutover | WAVE 97dc747 'Not done'; research/cutover-2026-09-28.md sections 11 and 14.2, phase 0b |
| 163 | F2.ponr | The point of no return as its own approved step with its own record (the reverse leg stopped, the old source thawed or retired), never a side effect | IN PROGRESS (cutover-gaps agent) | S | cutover | research/cutover-2026-09-28.md sections 11 and 14.2, phase 9 |
| 164 | F2.origin | `migkit_origin` taken off the target at cutover and by `--drop` on a one-way hop (a one-way tail numbers its batches in it by default since 63ad3f5) | IN PROGRESS (cutover-gaps agent) | S | cutover | L7151-7163; WAVE 63ad3f5 'Not done' |
| 165 | F2.legs | The hop's streaming pipeline and follow legs run through the cutover path: waited for, stopped by name, their positions read as the drain's | IN PROGRESS (cutover-gaps agent) | M | cutover | WAVE 97dc747 'Not done' |
| 166 | F3 | Stored code converted per routine by the best proved candidate (operator's file, sqlglot, rules, Ora2Pg, SQLines, model loop), proved by execution on both sides, plpgsql_check gate, residue in three classes; MySQL and SQL Server sources | PARTLY 7dbe6e8, e0ca1ef, d4fa711 (done: every routine run on both sides in a sandbox, candidates ranked by pass rate, coverage gate, residue in three classes, MySQL, SQL Server and Oracle sources, PostgreSQL to MySQL rules; missing: coverage of SQL Server routines and of a MySQL trigger reading `ROW_COUNT()`, the clocks of SQL Server and Oracle cannot be set for a session) | L | conversion | L1974-1979 (11), L2256, L3200-3209, L4606-4614, L5390-5397, L7134 |
| 167 | F4.a | New canon classes and value rules: uuid, array, inet, xml, 9-place timestamps, value-normal decimals, float4 widened, infinity uncomparable, JSON in-process; refusals before the move (unsigned 64-bit, NUL, 4-byte characters, DynamoDB over 38 digits) | DONE 3b05234 | M | types | L3211-3216 |
| 168 | F4.b | interval, vector and geometry canon classes (interval still left out across engines) | DONE 679b400 | M | types | L2529-2531, L3211-3213; WAVE 3b05234 note |
| 169 | F4.c | G5: MongoDB types round trip | DONE 679b400 | S | types | WAVE 3b05234 note |
| 170 | F4.d | Empty Cassandra collections | DONE 679b400 | S | types | WAVE 3b05234 note |
| 171 | F6 | Speed of migkit's own paths: MySQL decoder in its own process chosen by measured rate, PostgreSQL tail on one kept connection, apply trims and GC held, renderer per column | DONE f3884fa | M | change follow | L3227-3287 |
| 172 | F6.no | Free-threaded 3.14t, subinterpreters, PyPy for all of migkit, orjson or XXH3 in the digest | NOT PURSUED ("not now", F6; XXH3 measured and not adopted, R18.3) | S | move | L3233-3234, L6003-6006 |
| 173 | F7.rshake | RedisShake as a rung (readers, Lua, filters, status port) | DONE b4e4d98: readers climbed per database (backup file, append-only file, the snapshot a replica gets, a scan), exclusions as its key filters where exact, a cluster target's database 0, the target's limits read, plain commands into an older target, status port as progress (`redisshake`); one Redis ladder on measured rate (`bulkways`) | M | tools used whole | L489-490, L732-733, L3292 |
| 174 | F7.mm2 | MirrorMaker 2 wrapped whole with a source-offset header and group translation | DONE b4e4d98: run from the release or Kafka's image, nothing written to the source (measured: its flow back's heartbeat made four topics there until turned off), ended where the source stood, positions kept for the copier; its syncs and checkpoints decoded and proved by the message; the copier's `offset_header`; measured 76.6 s against the copier's 13.1 s for 200,000 messages (`mirror`) | M | tools used whole | L490-491, L740-742, L3292-3293 |
| 175 | F7.dsbulk | DSBulk (splits, checkpoint and replay, count modes, preserved TTL and timestamps) | DONE b4e4d98: splits sized, each cell's time to live and write time, checkpoints resumed, no error skipped, counted on the target by the loader, per table against the token ranges (`dsbulk`, `tableways`) | M | tools used whole | L491-492, L3293 |
| 176 | F7.rfs | OpenSearch reindex-from-snapshot | DONE b4e4d98: an index out of a snapshot the hop names, the reader built from source and proved by its usage, the disk held to twice the largest shard (`fromsnapshot`) | M | tools used whole | L3294 |
| 177 | F7.ddbexp | DynamoDB export to and import from S3 | DONE b4e4d98: the service's export taken in by its import or by migkit's writer, manifest or export count held, a failed import's table removed, the files removed (`ddbexport`, against moto) | M | tools used whole | L3294 |
| 178 | F7.kclamp | Kafka consumer-offset clamp at cutover | DONE b4e4d98: a group at its very message, else after the last it read at the target's end and said, never past the end or before the start; placed afresh at cutover, undone as it was (`KafkaEngine._place`) | S | cutover | L3296 |
| 179 | F7.selfstop | A scheduled tail that stops when caught up | DONE 63ad3f5: a slot's run ends once caught up with where the source's log was when it first read to the end, and at the slot's end at the latest (`tailctl.until`, `schedule.slot_end`); `tail.stop` asks any tail to end; `move` gated by its schedule as `check` is | S | change follow | L3296-3297 |
| 180 | F7.beat | Source-commit timestamp in the tail's beat | DONE 63ad3f5: every reader marks each transaction's commit time (`canon.ended`), the beat carries `through` and `lag`, `/metrics` says both | S | change follow | L3297-3298 |
| 181 | F7.commit | A batch ended at the last COMMIT seen, so a transaction that fits is never split | DONE 63ad3f5: the batch cut after the last change that ended a transaction, its commit's position saved (`txn_at`), the rest read with the next (`_commit_cut`) | S | change follow | L3298; goldengate-qlik-hvr report L1113 |
| 182 | F7.merge | Changed-columns merge when two sides touched different columns (and USEMIN) | IN PROGRESS (two-way conflict-resolution agent, with P0-DEPTH.conflicts) | S | two-way | L3298-3299; goldengate-qlik-hvr report L1124; P0-DEPTH L2809-2813 |
| 183 | F7.edges | Statistics-based chunk edges | OPEN | S | move | L3299 |
| 184 | F7.exact | Exact batches by default on one-way tails (resume after the committed batch) | DONE 63ad3f5: numbered on the table rung, proved by what it keeps (`marks.settle_exact`), wherever a batch is applied as one transaction; `exact_batches: false` off | M | change follow | L3300, L6180-6184 (R19.11) |
| 185 | F7.sec | The 16 remaining security fixes from the security scorecard | PARTLY 254c0f9, f5285cb (done: S1 and S2 TLS verified per engine, S5 a publication of the hop's tables, S10 the dashboard token out of the URL, S13 digests marked not for security, S6 the DBA's least-privilege script, S15's warning on a plain password; missing: S3, S4, S7, S8, S9, S11, S12, S14, S16 and S15's prompt and stdin) | M | security | L3302; WAVE queue |
| 186 | CV.P1 | Candidate portfolio, selection by evidence | DONE 7dbe6e8 | M | conversion | L3391-3417; leapfrog-conversion L305 |
| 187 | CV.P2 | Counterexample bank, re-run on every candidate | DONE 7dbe6e8 | S | conversion | L3397-3399; report L306 |
| 188 | CV.P3 | Model as hole-filler on the best partial candidate, bounded rounds | DONE 7dbe6e8 (`storedcode/model.py`, a capped loop fed each failure) | M | conversion | report L307 |
| 189 | CV.P4 | Two-sided reduction (Hypothesis shrinking, ddmin) | DONE 7dbe6e8, d4fa711 (a statement-level reduction for every source) | M | conversion | L3409-3410; report L308 |
| 190 | CV.P5 | Aligned intermediate-state capture | OPEN | M | conversion | report L309 |
| 191 | CV.P6 | Inputs from the real call history | DONE d4fa711 (`storedcode/history.py`) | S | conversion | L3408; report L310 |
| 192 | CV.P7 | Concolic solving (z3) for untaken branches | DONE 7dbe6e8 (`storedcode/concolic.py`) | M | conversion | L3408-3409; report L311 |
| 193 | CV.P8 | Stateful call sequences | DONE d4fa711 (`storedcode/sequences.py`) | M | conversion | report L312 |
| 194 | CV.P9 | Emulation candidates (orafce, MariaDB Oracle mode, IvorySQL, openGauss, Babelfish, openHalo) | PARTLY 7dbe6e8 (done: orafce; missing: MariaDB Oracle mode, IvorySQL, openGauss, Babelfish, openHalo) | M | conversion | L3403-3407; report L313 |
| 195 | CV.P10 | Workload corpus + cross-engine replay (a cross-engine pt-upgrade on workload.py), frequency-weighted | PARTLY fb5c988 (done: as R23.4 - each statement translated, planned and timed on both sides, reads held to the same answer and weighted by the calls each had; missing: Oracle's bind capture as a source of the workload; the application scan is CV.P11) | M | conversion | L3399-3403, L3416-3417; WAVE queue |
| 196 | CV.P11 | Static application scan (ast-grep + sqlglot) | OPEN | M | conversion | L3402-3403; report L315 |
| 197 | CV.P12 | Constraints proved to accept and reject the same rows (z3 CHECK equivalence) | DONE d4fa711 (`storedcode/constraints.py`) | M | conversion | L3410-3411; report L316 |
| 198 | CV.P13 | Collation agreement matrix from real values | DONE d4fa711 (`storedcode/collations.py`) | S | conversion | L3411; report L317 |
| 199 | CV.P14 | Types sized from data with recorded domain changes | DONE d4fa711 (`storedcode/sizing.py`, hop option `types_from_data`) | S | conversion | report L318 |
| 200 | CV.P15 | Index/partition advice on the translated workload (HypoPG, Dexter) | OPEN | M | conversion | L3411-3412; report L319 |
| 201 | CV.P16 | Rules learned from accepted fixes, adopted only with zero regressions | DONE 7dbe6e8 (`storedcode/learn.py`) | L | conversion | L3412; report L320 |
| 202 | CV.P17 | Built-in semantics mined from the engines' own test suites | OPEN | M | conversion | report L321 |
| 203 | CV.P18 | The harness's own mutation score | DONE 7dbe6e8 (`storedcode/mutants.py`) | M | conversion | L3399-3400; report L322 |
| 204 | CV.P19 | Emulation as oracle after cutover | OPEN | M | conversion | report L323 |
| 205 | CV.P20 | Scalar routines: bounded verification of a model of both bodies | OPEN (research) | L | conversion | report L324 |
| 206 | CV.P21 | Schema restructuring with a proof | OPEN (research) | L | conversion | report L325 |
| 207 | W3 | Code conversion at enterprise breadth: PL/SQL packages with emulation, T-SQL at depth, application-embedded SQL found and converted | OPEN (pieces in CV.P9-P11) | L | conversion | L3625-3629 |
| 208 | 40.conv | Effort estimates for conversion (Ora2Pg cost units) | DONE d4fa711 (`storedcode/effort.py`) | S | conversion | L4674-4675, L5392-5393 |
| 209 | ER1 | Exact warehouse loads + one SHA-256 row digest in each engine's SQL (BigQuery committed streams, Snowpipe Streaming channels, Redshift COPY, Delta txn, Iceberg snapshot properties; staged Parquet) | DONE bd20c46 (the digest's hash is the fold's, MD5 today and SHA-256 in every dialect the day it moves) | L | engines | L3300-3301, L3425-3435, L4330-4332 |
| 210 | ER1.acct | Warehouses run against real accounts; their other cells (stream, fence, confirm, users, settings, snapshot, deep, sequences) | BLOCKED (needs accounts; emulators have gaps) | M | engines | L4318-4332, L5320-5321, L5734-5735 |
| 211 | ER2 | Copybook engine (EBCDIC, packed and zoned decimals) and IBM i with DISPLAY_JOURNAL follow and HASH_ROW | DONE 073c2c1 (proved on no real IBM i or z/OS) | L | engines | L3436-3444 |
| 212 | ER2.zos | Db2 for z/OS through ibm_db | DONE 073c2c1 (`db2zos`: copy and verify; follow said not applicable; SQL1598N said as the Db2 Connect licence it is; IMS/CICS/VSAM changes said as unreadable; proved on no z/OS) | M | engines | L3441-3444 |
| 213 | ER3 | Teradata | DONE 2026-10-01 (engine reach; written without a system: statement shapes held, a Vantage Express VM proves it where named) | M | engines | L3476-3478 |
| 214 | ER4 | One change-topic reader (TiDB, Couchbase, Aurora DSQL, CockroachDB) | PARTLY (engine reach merge; done: changetopic.py reader, tidb/cockroachdb/dsql/couchbase engines, copy/check/follow; missing: bulk-load paths, users/roles, snapshots, CockroachDB/DSQL sequences, TiCDC open-protocol/Avro outputs) | M | engines | L3479-3481 |
| 215 | ER5 | Cosmos DB NoSQL | PARTLY (engine reach merge; done: cosmosdb engine, both feed modes, digests by feed range, users; missing: bulk-load path, snapshots) | M | engines | L3482-3483 |
| 216 | ER6 | SAP application data through ODP over OData | DONE 2026-10-01 (engine reach; held to a local service of Gateway's shapes, no SAP system) | M | engines | L3484-3487 |
| 217 | ER7 | SaaS: dlt, Singer taps, Airbyte connectors as programs, Salesforce Pub/Sub | DONE 2026-10-01 (engine reach; programs of each protocol and local Salesforce services, no SaaS account) | L | engines | L3488-3491 |
| 218 | W5 | Other sources: Netezza, SAP HANA, Informix (x86), Progress, Vertica, Exasol, Databricks/Iceberg | PARTLY bd20c46 (done: Iceberg and Delta tables as sides; missing: Netezza, SAP HANA, Informix, Progress, Vertica, Exasol, Databricks) | L | engines | L3223, L3426-3428, L3646-3651 |
| 219 | L1 | Tables added or removed in a running tail, each with its own snapshot and seam | DONE 63ad3f5: `tailtables` - the hop file's `exclude` read again as it changes, a table added copied beside the stream under its own marks, its changes held per range until it lands | M | change follow | L3499-3506, L3295 (F7), L7140 (DTS "a running job changed"); live-tail report L1066 |
| 220 | L2 | Exact seam on PostgreSQL by xid visibility, not LSN | DONE 6f07ceb | S | change follow | L3507-3511, L6110-6149 |
| 221 | L3 | Exact seam on SQL Server (CT version in SNAPSHOT) and MongoDB (clusterTime) | DONE 63ad3f5: `snapshot_mark`/`mark_covers` by change tracking version and by the majority commit point, each change carrying its own | S | change follow | live-tail report L1068 |
| 222 | L4 | A DDL parks only its table, its changes spooled | DONE 63ad3f5: `ddl: park` (the default stays stop), the spool synced on this machine before the position, released once the target is level, copied again past `spool_limit` | M | change follow | L3505-3506, L3295-3296 (F7) |
| 223 | L5 | Positional DDL: additive changes applied at their log position, events decoded under the schema as of their position (DTS DDL replication, allow-list, added columns) | DONE 63ad3f5: `ddl: apply` - DDL read at its place (binlog statement, slot TRUNCATE, stream collection events), a column that takes NULL or has a constant default added on the target, a truncate carried between the rows around it; row events decoded with their own table map | M | change follow | L1410-1411, L1420-1421, L3506, L3300 (F7), L7139-7140 |
| 224 | L6 | Re-add a table from its removal position | DONE 63ad3f5: a rung of `tailtables._add_ways`, only where the source reads its log again from a position, the shape is the same and the target's digest unchanged; else copied | S | change follow | live-tail report L1071 |
| 225 | L7 | Per-table accounting identity every batch | DONE 6f07ceb, 63ad3f5 (the spool's terms: held, set aside, out of scope, taken back) | S | change follow | L3511-3514, L6133-6135 |
| 226 | L8 | Publication-add guard below PostgreSQL 17.5 | DONE 63ad3f5: `publication_add_risk` (the 2025-05-08 minors) a need of the subscription rung, migkit's own tail taken instead | S | change follow | L3513-3514; report L1073 |
| 227 | LT.R1 | Depth D0-D4 per table in assess/doctor, with the statement that raises it | OPEN | S | change follow | L3544-3546 |
| 228 | LT.R2 | SQL Server without CT/CDC: the log-backup chain restored onto a SQL Server migkit owns | BLOCKED (full SQL Server is x86-only) | L | change follow | L3550-3553 |
| 229 | LT.R3 | SQL Server live `fn_dblog` reader | BLOCKED (full SQL Server is x86-only) | L | change follow | report L1076 |
| 230 | LT.R4 | Oracle partial updates lifted to full images by flashback reads | DONE 0c89d67 (what a statement does not give is read from the table as of the commit) | M | change follow | L3553-3555 |
| 231 | LT.R5 | Oracle NOLOGGING operations detected | DONE 0c89d67 (a NOLOGGING table stops the start by name) | S | change follow | L3555 |
| 232 | LT.R6 | Db2 without DATA CAPTURE CHANGES (db2ReadLog) | BLOCKED (Db2 is x86-only) | M | change follow | L3556-3557 |
| 233 | LT.R7 | MySQL MINIMAL/NOBLOB/PARTIAL_JSON row images lifted | OPEN | S | change follow | L3546-3548 |
| 234 | LT.R8 | MySQL STATEMENT/MIXED binlogs as invalidations | OPEN | M | change follow | report L1081 |
| 235 | LT.R9 | PostgreSQL at wal_level=replica via pg_walinspect/pg_waldump | OPEN | M | change follow | L3548-3550 |
| 236 | LT.R10 | MongoDB standalone (dbHash + raw-BSON bucket digests) | OPEN (in the report's table; the backlog names R1-R9) | S | change follow | report L1083 |
| 237 | LT.C1 | mongosync's job rebuilt from change streams, raw BSON and generations, bypassWriteBlockingMode audited | OPEN | M | change follow | L3101-3103, L3558-3562 |
| 238 | LT.C2 | Kafka offsets kept identical for gap-free partitions (pad, DeleteRecords) | OPEN | M | cutover | L3103-3105, L3562-3564 |
| 239 | LT.C3 | Kafka offset map written in the data's transaction | OPEN | M | cutover | L3564-3566 |
| 240 | LT.C4 | Redis/Valkey two-site active-active rules | OPEN | L | two-way | L3105-3106, L3566-3567 |
| 241 | LT.C5 | XStream's job: LogMiner on a migkit Oracle Free mining instance (mining on a standby) | OPEN | M | change follow | L3089, L3107, L3567-3569, L3592 |
| 242 | LT.C6 | RIOT-X's live mode made lossless (PSYNC client or IDLETIME sweep) | OPEN | M | change follow | L3569-3570 |
| 243 | LT.P1 | A mover that scales itself on the source's stress (range queue, k8s/ECS/SSH workers) | OPEN | M | operations/scale | L3571-3573 |
| 244 | LT.P2 | Seeding from ZFS/LVM/EBS snapshots, only changed blocks, fast-forwarded | OPEN | M | move | L3573-3575, L6044-6047 |
| 245 | LT.P3 | Zero-ETL-like continuous hop for any source and warehouse | OPEN | L | change follow | L3575-3576 |
| 246 | W1 | Mover HA and scale-out: standby takeover within seconds; one table's ranges across machines with a conditional-write checkpoint | OPEN | L | operations/scale | L3613-3618, L3950-3952 (31), L5239-5246 (R5); WAVE queue |
| 247 | W2 | The estate: discovery, sizing from performance history, cost, fleet, REST API, Terraform provider, K8s operator, OIDC/SAML/SCIM | PARTLY 601f198 (done: the target's class from the source's observed load, PostgreSQL and MySQL; missing: discovery, cost, fleet, REST API, Terraform provider, K8s operator, OIDC/SAML/SCIM) | L | operations/scale | L3619-3624 |
| 248 | W4.a | Transformation in flight (joins, enrichment, windows) as hop rules under the same verify: each batch of changes an Arrow table, the hop's `transform:` SQL run over it in DuckDB and the result written through the same exact batches, the verify running the same SQL over the source's rows | OPEN (supersedes "ETL not pursued" and the 0e open question) | L | move | L783-787, L3630-3645 |
| 249 | W4.b | Format-preserving encryption and tokenization | OPEN | M | security | L3630-3645 |
| 250 | W6 | Types only commercial engines have (SDO_GEOMETRY, XMLType, object types, hierarchyid, sql_variant, FILESTREAM, DECFLOAT, GRAPHIC) | OPEN | L | types | L3652-3656 |
| 251 | W7 | Resharding: split, merge, distribution-aware load | OPEN | L | move | L3657-3660 |
| 252 | W8 | Offline signed bundle, container image, Windows; proved inside a network that reaches only the two databases (install, `init`, a move with its tail, `check` and a report; any outbound call fails the test) | OPEN | M | operations/scale | L3661-3673 |
| 253 | W9 | Compiled apply side (collapse, render, write) above ~100k changes/s | OPEN | L | change follow | L3674-3676, L6220-6221 |
| 254 | 28 | Re-runnable benchmark harness (six shapes, move paths, check, lag at a rate) | DONE (in text 2026-09-25) | M | operations/scale | L3690-3718 |
| 255 | 28.rates | Lag at 10k and 50k tx/s | BLOCKED (hardware: the 2-CPU VM cannot produce the rates) | S | operations/scale | L3716-3724 |
| 256 | 29 | Change apply that keeps up: a batch one transaction, rows collapsed, runs of rows, large transactions streamed, parallel native appliers, MariaDB slave_pos | DONE (in text 2026-09-25) | M | change follow | L3726-3841 |
| 257 | R2 | The applier: read and apply together, lanes by dependency, errors, COPY stage on PostgreSQL, foreign keys off where every parent is in scope | DONE (in text 2026-09-27; items 1-3, 4 on PostgreSQL, 5) | L | change follow | L3822-3841, L4889-4962 |
| 258 | R2.4b | Apply path by measured cost: MySQL through the pinned LOAD DATA into a stage, PostgreSQL pipeline mode and binary COPY for small runs (R19 lever 10) | DONE 4e92945 | M | change follow | L3083, L4906-4909, L6156-6160 |
| 259 | 30 | Write lock as a lease; run state (lease, checkpoints, change position, copy record) in the bucket | DONE (in text 2026-09-25) | M | operations/scale | L3843-3913 |
| 260 | R5 | Control plane: schedule, operations view (hold, resume), approvals signed with SSH keys, chained audit, roles behind a proxy | DONE 7de74bc (in text 2026-09-27) | L | operations/scale | L3843-3859, L5192-5214 |
| 261 | R5.actions | View actions: drain to a position, abort, approve a cutover when lag and the last verify allow | OPEN | M | operations/scale | L5226-5230, L7135 |
| 262 | R5.approve | Approving from the view | NOT PURSUED (left out on purpose: an approval stays a signature) | S | security | L5213-5214 |
| 263 | R5.anchor | Signed audit checkpoints anchored in an object-locked bucket | OPEN | S | security | L5236-5238, L7135-7136 |
| 264 | 31 | Scale-out by tables across machines (`share_tables`) | DONE (in text 2026-09-25) | M | operations/scale | L3915-3952 |
| 265 | 32 | Alerts: tail heartbeat, stop record, metrics, shipped rules, notify receivers, retention per engine | DONE (in text 2026-09-25) | M | operations/scale | L3954-4014 |
| 266 | 32.rds | Managed MySQL binlog retention read | OPEN (grep: stream_room gives nothing on RDS) | S | operations/scale | L4011-4013 |
| 267 | 44/R7 | Failure caused on purpose: failpoints, kills, link cuts, failover, full disks, lost positions | DONE (in text 2026-09-25/27) | L | operations/scale | L4016-4134, L5280-5312 |
| 268 | 45.mem | Memory flat as tables grow; the terabyte recipe | DONE (in text 2026-09-25) | M | operations/scale | L4147-4182 |
| 269 | 45.run | Terabyte-class run and migkit's own move at size, time and cost stated | BLOCKED (waits on the owner renting machines; the VM has a 20 GiB disk) | L | operations/scale | L4142-4146, L4179-4182 |
| 270 | 46 | Wrapped programs stay wrapped: flags held against the installed --help, per-program rows in assess, CI matrix | DONE (in text; the GitHub workflow has not run yet) | M | tools used whole | L4184-4238 |
| 271 | 33 | ClickHouse, Kinesis, Pub/Sub, Event Hubs | DONE (in text 2026-09-25) | L | engines | L4242-4316 |
| 272 | 34 | Parquet, Cassandra/Scylla, OpenSearch, DynamoDB as sides of a pair | DONE (in text 2026-09-25) | L | engines | L4376-4495 |
| 273 | 35 | Change delivery as json, debezium or canal, routing rules, oversize skipped and counted | DONE (in text 2026-09-25) | M | change follow | L4497-4522 |
| 274 | R4 | Avro through a schema registry; AWS_MSK_IAM signed | DONE (in text 2026-09-27) | M | change follow | L5163-5190 |
| 275 | R4.msk | A real MSK IAM handshake | BLOCKED (needs an AWS account) | S | change follow | L5176-5178, L5188-5190 |
| 276 | R4.formats | JSON Schema, then Protobuf, through the registry | OPEN | S | change follow | L5185-5186 |
| 277 | 36 | Two-way on PostgreSQL's own replication | DONE (in text 2026-09-25) | M | two-way | L4524-4553 |
| 278 | R3 | MySQL native two-way; migkit's own tails both ways; conflict policies (error, apply_remote, keep_local, last_update_wins, source_priority, delta counters) | DONE 7de74bc (in text 2026-09-27) | L | two-way | L4963-5033 |
| 279 | R3.topo | Many-node topologies: many-to-one, one-to-many, full mesh | OPEN | L | two-way | L4551-4552, L5013-5015, L5033; WAVE queue |
| 280 | 37 | Live reverse replication at cutover (PostgreSQL) | DONE (in text 2026-09-25) | M | cutover | L4555-4572 |
| 281 | 37.my | Reverse at cutover measured on MySQL | DONE 97dc747 (`test_the_cutover_on_mysql.py`: the stream back proven on MySQL, outage 0.52-0.90 s) | S | cutover | L4570-4572 |
| 282 | 38 | Passwordless sign-in (RDS IAM tokens, assumed roles) and the clouds' secret stores, checked offline | DONE (in text 2026-09-25) | M | security | L4574-4601 |
| 283 | 38.real | RDS token sign-in, Cloud SQL IAM, managed identities against the real services | BLOCKED (needs accounts) | S | security | L4603-4604 |
| 284 | 39 | Views and one-expression functions converted across MySQL, PostgreSQL, SQL Server, with proof | DONE (in text 2026-09-25) | M | conversion | L4606-4655 |
| 285 | 41 | Threat model, private local copy, redaction, audit log, SBOM and provenance, restore points encrypted | DONE (in text 2026-09-25) | M | security | L4677-4709 |
| 286 | R17b | Files holding values encrypted to the hop's recipients (evidence.py, age) | DONE (in text 2026-09-27) | M | security | L4711-4712, L5215-5219, L5870-5880 |
| 287 | R17b.hmac | Keyed HMAC for what a shared report shows | OPEN (grep: masking.py uses a salted hash, no HMAC) | S | security | L5218-5219, L5247-5248, L5852-5853, L5878-5879, L7136 |
| 288 | R17b.prog | Files written by driven programs (dumps) encrypted | OPEN | S | security | L5879-5880 |
| 289 | DOCS | Docs refresh: the threat model's at-rest section and other stale docs | OPEN | S | operations/scale | L7143; WAVE queue "docs refresh" |
| 290 | 42 | Troubleshooting guide generated from the code; diagnostics bundle | DONE (in text 2026-09-25) | S | ease of use | L4714-4733 |
| 291 | 43 | AI assistance, any provider: plain-language findings, conversion proposals under the same proof | DONE (in text 2026-09-25) | M | conversion | L4737-4787 |
| 292 | 43.fix | Drafting fixes through the same proof | OPEN | S | conversion | L4789-4790 |
| 293 | NP.ETL | ETL beyond the mapping migkit has | NOT PURSUED (the owner's 2026-09-28 decision reopens transformation in flight as W4.a) | S | move | L4792-4794 |
| 294 | R0.a | MySQL tail lost rows of a transaction larger than a read | DONE (in text 2026-09-27) | S | change follow | L4810-4821 |
| 295 | R0.b | `on duplicate key update` on tables with more than one unique index | DONE (in text 2026-09-27) | S | change follow | L4822-4830 |
| 296 | R0.c | The applier's collapse broke foreign-key order | DONE (in text 2026-09-27) | S | change follow | L4831-4836 |
| 297 | R1 | Parallelism sized by migkit (estimate, controller, workers a ceiling), scaling up within a machine | DONE (in text 2026-09-27) | M | operations/scale | L3950-3952, L4838-4887, L6069-6077 |
| 298 | R8.more | DVT for Oracle, Db2, Snowflake, BigQuery | BLOCKED (x86 runner and accounts) | M | verify | L5319-5321, L5325-5329 |
| 299 | R8.mv | MongoDB migration-verifier with its metadata on a local server | OPEN (waited on the owner's download go-ahead; the 2026-09-28 decision makes worthwhile tools dependencies) | M | verify | L2131-2134, L5321-5322, L5330-5331 |
| 300 | R8.explain | A mismatch narrowed to one range explained column by column (datacompy) | DONE 5713daf | S | verify | L2320, L5322-5323, L5332-5333 |
| 301 | R10 | Large values sized; PostgreSQL large objects carried in pieces | DONE (in text 2026-09-27) | M | move | L5365-5378 |
| 302 | R10.pieces | MySQL and Oracle large values read in pieces and appended (plan 12, LOBs) | PARTLY 0c89d67 (done: Oracle's LOBs read and written in pieces; missing: MySQL's large values by `SUBSTRING` in steps) | M | move | L2234, L5378-5388, L7134 |
| 303 | R12.minio | Object-store tests off MinIO's images (VersityGW) | OPEN (grep: tests still start bitnamilegacy/minio) | S | operations/scale | L5520-5522 |
| 304 | R13 | Engine cells done 2026-09-27: Kafka users/ACLs/confirm/snapshot; ClickHouse server-side sums, delta, settings, users, snapshot; SQLite and DuckDB native bulk; Parquet; Redis verify of written keys; DynamoDB stream follow, fence, confirm, delta, settings, backup, bulk; SQL Server logins, confirm, FOR JSON hash; OpenSearch copy, settings, snapshot, delta; Cassandra roles, settings, TTL-keeping copy | DONE (in text 2026-09-27) | L | engines | L5530-5678 |
| 305 | R13-MSSQL | SQL Server bulk move (its own bulk load, staged and promoted in migkit's transaction; R2.4 staging) | DONE fed3f89 | M | engines | L753, L3083-3085, L3221-3223, L4906, L4954-4955; capabilities.py GAPS mssql |
| 306 | R13-CH | ClickHouse bulk as the target pulling from the source (`remote()`, dedup tokens) | DONE ba59531 | M | engines | L3293, L5548-5549, L6519, L7130-7131 |
| 307 | R13-CH.follow | ClickHouse follow, fence and confirm by changed partitions (the fence on the source's clock at the last look that settled every changed partition) | DONE ba59531 | M | engines | L5549, L5713-5715; GAPS clickhouse |
| 308 | R13-OS | OpenSearch follow, fence and confirm by per-shard sequence numbers | DONE ba59531 | M | engines | L5639-5640, L5720-5722, L7131 |
| 309 | R13-OS.users | OpenSearch security roles and users | DONE ba59531 | S | engines | L5640, L7131 |
| 310 | R13-CASS | Cassandra/Scylla follow through Scylla's CDC tables, the fence on the source's clock (nothing written to the source), delta; a Cassandra source and a tablets keyspace refused with the reason | DONE ba59531 (ScyllaDB tablets refused with the reason; DBA grants for these engines not yet) | M | engines | L5678, L5723-5725, L7132 |
| 311 | R13-CASS.ttl | Deep check of every cell's write time and time to live, counters by value | DONE ba59531 | S | engines | L5677-5678, L7132 |
| 312 | R13-ORA | Oracle as a whole side: sequences, settings, direct-path bulk staged and promoted, follow, fence, confirm, delta, users, snapshot, SHA-256 summed digest, type map (B1), run on Oracle Free | DONE 0c89d67 | L | engines | L1974-2013, L2263, L3085, L3093, L3218-3221, L5515-5519; GAPS oracle |
| 313 | R13-DB2 | Db2 cells (10) and a run against a server | BLOCKED (IBM's image is x86-only; the x86 CI workflow is merged in fed3f89 and runs once pushed) | L | engines | L4408-4417, L3223; GAPS db2 |
| 314 | R13-ASE | SAP ASE cells (11) and a run against a server | BLOCKED (SAP's image is x86-only; the x86 CI workflow is merged in fed3f89 and runs once pushed) | L | engines | L4419-4429; GAPS ase |
| 315 | R13-HETERO | Users carried across engines | OPEN | M | engines | L7133; GAPS hetero users |
| 316 | R13-GENERIC | Generic engine cells (confirm, sequences, settings, bulk, table copy, follow, fence, delta, users, guard, statistics, snapshot) | OPEN | L | engines | L761-762; GAPS generic |
| 317 | R13.kafka | Kafka quotas and changed configs | OPEN | S | engines | L5726-5727 |
| 318 | R13.redis | A Redis 7.4+ RDB that Valkey refuses, said before a move | OPEN | S | engines | L5728-5729 |
| 319 | R13.sqlite | SQLite changes through the session extension (APSW) | OPEN (capabilities.py declares SQLite's follow not applicable) | S | engines | L5730-5731 |
| 320 | R13.parquet | Parquet on object storage: manifests and footers as the fence, versioning as the snapshot, only changed objects verified | OPEN (capabilities.py declares these not applicable) | M | engines | L5732-5733 |
| 321 | R14 | Measurements owed (mydumper, mongodump, proving a copied PostgreSQL range) | DONE (in text 2026-09-27) | S | tools used whole | L5737-5760 |
| 322 | R15 | DuckDB as an engine, with deep checks, sequences, snapshot and a bulk path of its own | DONE (in text 2026-09-27; grep: duckdb.py check_deep, _sequences, snapshot_state, native_bulk) | M | engines | L5581-5590, L5762-5787 |
| 323 | R15.2nd | DuckDB as a second reader for Parquet, PostgreSQL, MySQL | OPEN (grep: none) | S | verify | L5785-5786 |
| 324 | R16a | pgvector compared value for value | DONE (in text 2026-09-27) | S | types | L5801-5804 |
| 325 | R16a.more | MySQL 9 VECTOR and MongoDB float arrays | DONE 679b400 (`vectors`: MySQL 9 VECTOR, a MongoDB array's doubles) | S | types | L5801-5804; WAVE queue |
| 326 | R16b | Qdrant as an engine | OPEN (deferred until the rest is done) | M | engines | L5795-5797, L5806-5813 |
| 327 | R16c | Neo4j as an engine | OPEN (deferred until the rest is done) | M | engines | L5795-5797, L5814-5819 |
| 328 | R17a | doctor says whether each leg is encrypted and where migkit sits; TLS options reach every PostgreSQL and MySQL connection | DONE (in text 2026-09-27) | M | security | L5855-5867 |
| 329 | R17a.rest | TLS for the MySQL bulk programs, MongoDB's and Redis's connections | DONE 254c0f9 | S | security | L5867-5868, L7136-7137 |
| 330 | R17c | Several SSH legs sized from the round trip | DONE (in text 2026-09-27) | M | move | L5882-5891 |
| 331 | R17d | The relay: reader beside the source, writer beside the target, an agent for Python-driver engines, framed zstd over TLS 1.3, encrypted spool (R19 lever 7) | OPEN (the PostgreSQL reader beside the source done 2026-09-27) | L | move | L5893-5930, L6102-6109, L6235-6237; WAVE queue |
| 332 | R18.1 | Same-engine pass-through: MongoDB raw BSON | DONE (in text 2026-09-27) | S | move | L5985-5993 |
| 333 | R18.2 | hiredis | NOT PURSUED (measured, not adopted) | S | engines | L5994-6001 |
| 334 | R18.zstd | zstd for spill files | DONE 63ad3f5 (the one spill file migkit writes, a parked table's spool, is zstd-framed; the relay already compresses) | S | move | L6001-6002; WAVE queue |
| 335 | R18.4 | Out-of-core key diff | NOT PURSUED (looked at, not built) | S | verify | L6007-6010 |
| 336 | R18.5 | Chunk and change interleave property-tested | DONE (in text 2026-09-27) | S | change follow | L6011-6017 |
| 337 | R18.6/R20.2 | Arrow batches between readers and writers; the cross-engine path without per-row Python (ADBC, pyarrow compute) | PARTLY fed3f89 (done: SQL Server to SQL Server read as Arrow; missing: the cross-engine path without per-row Python - IN PROGRESS, R20 cross-engine agent) | L | move | L6018-6020, L6212-6216 |
| 338 | R18.7 | confluent-kafka in place of kafka-python, lz4/zstd on produce | OPEN (grep: not used) | S | engines | L3075, L6021 |
| 339 | R18.fix | MySQL checksum by sum, not BIT_XOR; SQL Server by each row's FOR JSON SHA-256 | DONE (in text 2026-09-27) | S | verify | L5969-5982 |
| 340 | R19.1 | Physical rungs: pg_basebackup + fast-forward, CLONE, XtraBackup, MongoDB file snapshot, RDS/Aurora snapshot and clone via boto3 | PARTLY 4f8334e (done: PostgreSQL pages with a manifest proof and the slot made first, MySQL CLONE with its position proved, Aurora clone, RDS and Aurora snapshots across accounts, Azure PITR, Cloud SQL clone; missing: XtraBackup, MongoDB file snapshot) | L | move | L3082, L3109-3113, L3291-3292, L3609-3610, L6035-6050 |
| 341 | R19.3 | Physical rung extended by page-level comparison | OPEN | M | move | L6058-6062 |
| 342 | R19.6 | LOAD DATA LOCAL made safe (pinned) | DONE 1da6415 | S | move | L2297-2298, L6078-6082 |
| 343 | R19.8 | Changes to ranges not yet copied left out (the watermark, lever 8) | DONE 6f07ceb | M | change follow | L6110-6149 |
| 344 | R19.9/R20.3 | Compiled change decoder handing Arrow batches; streaming replication instead of polling (R2.6) | OPEN | L | change follow | L3285-3287, L4959-4961, L6150-6155, L6217-6222 |
| 345 | R20.1 | Same-engine pass-through per range: binary COPY piped (PostgreSQL), LOAD DATA stream (MySQL), range digests in the servers (R19 lever 2's binary COPY) | DONE f8e1d33 | M | move | L6051-6057, L6204-6211 |
| 346 | R20.4 | Session-level load settings (COPY FREEZE, synchronous_commit off, unique_checks, sql_log_bin where no replicas) | DONE f8e1d33 | S | move | L6223-6229 |
| 347 | R20.5 | Phases overlapped: indexes while the next table copies, a range verified while the next loads, the tail after the first table (R19 lever 4) | PARTLY f8e1d33 (done: index builds beside the next table's copy, a range's check beside the next range's load; missing: the tail started after the first table) | M | move | L6063-6068, L6230-6232 |
| 348 | R20.6 | Compression level and legs chosen by measurement | OPEN | S | move | L6233-6237 |
| 349 | R20.7 | Files moved as parallel ranged parts, server-side copies, checksums while streaming | OPEN | M | move | L6238-6241 |
| 350 | R20.8 | The proof kept up as the data moves (incremental multiset hashing) | DONE 97028a6 (`kept`, hop option `keep_proof`) | L | verify | L6244-6255 |
| 351 | R20.10 | The fastest hash the pair allows (native row hash, rendering pushed into SQL, parallel gather) | OPEN | M | verify | L6313-6318 |
| 352 | R20.11 | Only what changed since the last proof is read again | PARTLY 97028a6 (done: leaves the tail touched since the last pass, through the kept proof; missing: whole tables skipped by modification counters where `unchanged_since` answers only PostgreSQL) | M | verify | L6319-6321 |
| 353 | R20.12 | Both sides' digests of a range read at once | DONE f8e1d33 | S | verify | L6322-6324 |
| 354 | R20.13 | DuckDB as the compiled cross-engine mover | IN PROGRESS (R20 cross-engine agent) | M | move | L6326-6333; P0-DEPTH L2828-2829 |
| 355 | R20.14 | The source renders the target's load format in SQL, piped as bytes | IN PROGRESS (R20 cross-engine agent) | M | move | L6334-6340; P0-DEPTH L2828-2829 |
| 356 | R21.1 | Two addresses are a hop | DONE f5285cb | M | ease of use | L6420-6426 |
| 357 | R21.2 | `init` as the guide; passwords in the keychain or env: | DONE f5285cb | M | ease of use | L6427-6435 |
| 358 | R21.3 | The DBA's least-privilege script written per engine and service | DONE f5285cb | M | ease of use | L6436-6439 |
| 359 | R21.4 | Nothing to tune: knobs only ceilings, the starter config shows none, doctor flags keys that do nothing | DONE f5285cb (grep: the starter config shows no knob; `hopkeys.said_by_doctor`) | S | ease of use | L6440-6444 |
| 360 | R21.5 | `goal:` sets the defaults | DONE f5285cb | S | ease of use | L6445-6449 |
| 361 | R21.6 | Config checked like code (did you mean, types, contradictions, plaintext secrets) | DONE f5285cb | S | ease of use | L6450-6452 |
| 362 | R21.7 | Import an AWS DMS task, a DTS job, a Debezium config or a pgloader file | PARTLY f5285cb (done: an AWS DMS task, a Debezium config, a pgloader file; missing: a DTS job) | M | ease of use | L6453-6455 |
| 363 | R21.8 | A setup page in the view | OPEN | M | ease of use | L6456-6458 |
| 364 | R21.9 | Every message says what to do next | OPEN | M | ease of use | L6459-6460 |
| 365 | R21.M | Ease measured: time to a verified move scripted per pair, config lines, questions asked | OPEN | S | ease of use | L6462-6466 |
| 366 | DTS.ops | Operation filters (skip inserts, updates or deletes) | OPEN | S | change follow | L7140 |
| 367 | DTS.sample | Sampled check | PARTLY 5713daf (done: random rows read first and apart through the second reading's `two_readings.sample`, where a second reader is installed; missing: a sampled check by migkit's own reader) | S | verify | L7140 |
| 368 | DTS.gtid | GTID-set compare | OPEN (grep: none) | S | verify | L7140-7141 |
| 369 | DTS.csfilter | Change-stream filter | OPEN | S | change follow | L7141 |
| 370 | REL | Release: memory and swap checked, the full suite once, fixes, secrets check, one commit, push | OPEN | S | operations/scale | L7066-7083, L7147-7150 |
| 371 | R22.1 | One resource model per task: source read, target write, the migkit host (memory included, driven by measured value sizes, never a LOB size to set), the link each way, the path's cost per row; probed before the move and kept current | DONE 1a1d1f3 (the copier's memory per engine is P0-PARITY's large values) | L | operations/scale | L6468-6492; WAVE a90aadb |
| 372 | R22.2 | The binding resource named, workers and connections per side sized to it (roofline, Little's law), said in words | DONE 1a1d1f3 | M | operations/scale | L6503-6509 |
| 373 | R22.3 | One controller per resource while it runs (source read pace, target write pace, link legs) | DONE 1a1d1f3 | M | operations/scale | L6510-6515 |
| 374 | R22.4 | Hops removed: a server pulling straight from the other (postgres_fdw, a temporary subscription, CLONE, `remote()`), credentials removed after; where migkit should run and why | DONE 601f198 | M | move | L6516-6522 |
| 375 | R22.5 | ETA per phase with its reasons: p50/p90 ranges from measured rates, binding resource per phase, live EWMA, "never catches up" said | DONE 1a1d1f3 | M | operations/scale | L6523-6529 |
| 376 | R22.6 | The machine to run on: instance type for migkit and its agents, target class from the source's history, a shipped catalog refreshed from the provider | DONE 601f198 | M | operations/scale | L6530-6545 |
| 377 | R22.W | Wide values hashed where they lie: no server-side digest writes a wide value's hex or text (`canon.WIDE`, the row's text takes its length and md5 on every engine and in this process); a table of wide rows digested by fewer parallel workers | DONE 0a4276a | M | verify | R22 model finding (2026-09-30) |
| 378 | R23.1 | An evidence export per migration that anyone can recompute: per table the counts and digests of both sides with the exact method, the times, the downtime, the approvals, what was left and removed - HTML, Markdown, CSV and JSON, tamper-evident - and each report deeper than the one it is measured against | DONE f7ac5e8 (`export`, `digests`, `session`; anchored in the audit chain, signed with `ssh-keygen -Y` where asked, encrypted to the `at_rest` recipients; PDF not built, the HTML prints to one) | L | verify | L6635-6690 |
| 379 | R23.2 | Rehearse, then forecast: a goal that runs the whole path on a clone or a sample the operator names and turns what it measured into the forecast (time per phase, downtime, bytes, cost) with the confidence of a sample of that size; the real run compared with it | DONE fb5c988 (`rehearsal`: `goal: rehearse`, the forecast from the rehearsal's measurements, each phase of the real run held to it; the sample's confidence is a reasoned rule, P0-DEPTH.constants) | L | operations/scale | L6691-6719 |
| 380 | R23.3 | An interlock on the target: both identities in every plan, and every write step refuses the source as the target, a production system the hop does not name, and a target holding rows unless `target_rows` says (refuse by default) | DONE 8c074f1 | M | security | L6720-6742, L2973-2975 |
| 381 | R23.4 | Performance after a change of engine: the translated workload timed on both sides, writes as well as reads, and the target advised (indexes, statistics, settings) before cutover | PARTLY fb5c988 (done: `workload` across engines - each statement translated, planned and timed on both sides, reads held to the same answer and weighted by the calls each had, writes planned and timed on a rehearsal's target, the target advised by `for-the-target.sql`; SQL Server joins PostgreSQL and MySQL; missing: Oracle's bind capture as a source of the workload) | L | conversion | L6743-6766 |
| 382 | R23.5 | Copies for testing: a referentially complete subset (the closure of foreign keys from the rows chosen) with masking, into a staging or developer database, under the same proof | DONE fb5c988 (`subset`: `goal: subset`, the closure of foreign keys, masked, proved row by row) | M | move | L6767-6786 |
| 383 | R23.6 | Right the first time, not a runbook to recover with: the rehearsal, preflight, refusals before anything is written, proofs at every step and go/no-go gates run by migkit, the hand steps listed by the plan at the moment they are needed | DONE fb5c988 (`ready`: every precondition asked before the first write, `MIGKIT_PREFLIGHT=warn` goes on) | M | cutover | L6787-6810 |
| 384 | R23.7 | A major-version upgrade as a goal (`goal: upgrade`): the same engine at a new version with the least downtime, in place or logical by measurement, the extensions checked first | DONE fb5c988 (`upgrade`: `goal: upgrade`, in place on the target only - never on the source - or a logical move, by measurement) | M | move | L6811-6840 |
| 385 | R23.8 | Working inside the team's tools: tasks for Airflow, Dagster, GitHub Actions and Argo, OpenTelemetry traces beside the Prometheus metrics, a shipped Grafana dashboard, and the inside tools' metrics carried out through migkit's own surface | PARTLY f7ac5e8 (done: `/metrics` for the copy, checks, cutover and the change stream, the connector's JMX readings, `OTEL_*` traces, `integrations.run` under Airflow, Dagster, the GitHub Action and Argo, a Grafana dashboard; missing: the connector's own spans, W2's API and Terraform provider) | L | operations/scale | L6841-6874 |
| 386 | R23.9 | Documentation as part of the product: a five-minute quickstart for each common pair, a page per engine saying what is carried and what is refused, recipes for the usual moves | OPEN | M | ease of use | L6906-6910 |
| 387 | R23.10 | Notifications wherever the team is: every channel Apprise reaches as migkit's own receivers, shaped per channel, no row values, a delivery failure said and the run going on | DONE f7ac5e8 (`notify`; 21 channels against one local stub) | M | operations/scale | L6875-6905 |
| 388 | R24.1 | The engine page generated from the code: the engines, aliases and every source x target pair with what each carries, rendered from `capabilities.matrix()` and `decide.coverage()` at build time | OPEN | M | ease of use | L6917-6921 |
| 389 | R24.2 | Features and how to use them by goal (copy, cutover, verify, two-way, keep in sync, upgrade), each with a five-minute path from two addresses and the reports it produces | OPEN | M | ease of use | L6922-6925 |
| 390 | R24.3 | Use cases as recipes (a managed service to another, on-premises to cloud, one engine to another, a major-version upgrade, a zero-downtime cutover), with example numbers from the sandbox only | OPEN | M | ease of use | L6926-6929 |
| 391 | R24.4 | Release notes and a changelog per release, and the version shown on the site | OPEN | S | ease of use | L6930-6931 |
| 392 | R24.5 | The site checked like code: a test fails when a page names an internal tool, claims a pair the matrix does not have, or shows a command or option that no longer exists | OPEN | S | ease of use | L6932-6934 |
| 393 | R25.1 | Driving the managed services (creating, starting, stopping DMS or DTS tasks): migkit watches the databases while the service moves them, proves what it carried and repairs what it did not | NOT PURSUED (the owner, 2026-09-29) | S | operations/scale | L6947-6953 |
| 394 | R25.2 | The cloud side of readiness: the clusters' network (VPC, subnets, security groups, routes), maintenance windows and pending maintenance, instance classes on both sides, snapshots and restores through the provider's API, audited before the move and in `assess` | OPEN | M | operations/scale | L6954-6957 |
| 395 | R25.3 | The organisation's own change sheet: the plan's steps exported with ids that map onto a team's task sheet, each step's progress and evidence written back | OPEN | M | operations/scale | L6958-6961 |
