# Backlog index

One line per distinct work item in `docs/backlog.md`, built 2026-09-29 from
the backlog as it stands at `93aa147` (5,570 lines, R22 included), `docs/research/WAVE-STATE-2026-09-28.md`
(working-tree copy, with the R20, R21 and R22 launches) and `git log --oneline -30`.
Where the same work is described in several sections it has one line, with
every source listed.

**Status.** DONE cites the commit, or "in text" where the backlog itself
says done (most of those predate the last 30 commits). IN PROGRESS names the
agent and its task from WAVE-STATE; "not merged" means the agent has
committed in its worktree and master does not have it yet; "paused" means the
agent is in batch B and has not been resumed yet. BLOCKED gives the reason.
NOT PURSUED is used only where the backlog says so. Where the backlog was
unclear, the code on master was grepped and what was found is noted as
"grep:".

**Sources.** `Lnnnn` = line in `docs/backlog.md`. `WAVE` = WAVE-STATE.
Report names refer to `docs/research/*-2026-09-28.md`.

**IDs.** Section-based: P0/P1/P2 item numbers as the backlog numbers them
(`1`-`46`, `0`, `0a`-`0f`), `R0`-`R22` (research round 2026-09-27 and after),
`F0`-`F7`, `W1`-`W9`, `S1`-`S4` (the structural four), `CV.P1`-`CV.P21`
(leapfrog conversion), `ER1`-`ER7` (leapfrog engine reach), `L1`-`L8` and
`LT.R1`-`LT.R10`, `LT.C1`-`LT.C6`, `LT.P1`-`LT.P3` (leapfrog live tail; `LT.`
keeps them apart from R1-R22), `DTS.*` (the DTS gaps in "Paused"), `AUD.*`
("Audited again 2026-09-27"), `DEC.*` ("Decided by the owner 2026-09-28"),
`P3.*` (the P3 table), plan and problem items by their own names.

## Summary

345 items.

| Status | S | M | L | Total |
|---|---|---|---|---|
| DONE | 45 | 57 | 9 | 111 |
| IN PROGRESS | 16 | 34 | 11 | 61 |
| OPEN | 64 | 62 | 22 | 148 |
| BLOCKED | 8 | 5 | 6 | 19 |
| NOT PURSUED | 6 | 0 | 0 | 6 |
| **Total** | 139 | 158 | 48 | **345** |

| Area | DONE | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED | Total |
|---|---|---|---|---|---|---|
| move | 15 | 8 | 16 | 0 | 2 | 41 |
| verify | 20 | 4 | 20 | 1 | 1 | 46 |
| change follow | 18 | 2 | 36 | 5 | 0 | 61 |
| two-way | 2 | 1 | 4 | 0 | 0 | 7 |
| cutover | 4 | 2 | 7 | 0 | 0 | 13 |
| types | 7 | 3 | 3 | 0 | 0 | 13 |
| engines | 9 | 10 | 15 | 6 | 1 | 41 |
| conversion | 4 | 10 | 16 | 0 | 0 | 30 |
| security | 6 | 3 | 6 | 1 | 1 | 17 |
| ease of use | 5 | 6 | 5 | 1 | 1 | 18 |
| operations/scale | 14 | 6 | 12 | 5 | 0 | 37 |
| tools used whole | 7 | 6 | 8 | 0 | 0 | 21 |
| **Total** | 111 | 61 | 148 | 19 | 6 | **345** |

## Items

| # | ID | Item | Status | Size | Area | Sources |
|---|---|---|---|---|---|---|
| 1 | P0-DE | Decision engine: rungs that say what they give, need, prove and cost; the climb; composed strategies; the choice kept beside the position; shape x engine coverage; pick/fitted/plan/_verify_way/loops_prevented routed through it | DONE 32ed544 | L | tools used whole | L103-229; WAVE a84f1b9 |
| 2 | P0-DE.1 | Mover and path chosen on speed from measured rates: the planner's speed rules as rungs ranked by the benchmark; a migkit-owned mover only against the benchmark | OPEN (waits on item 28's measurements) | M | move | L191-193, L231-235, L384-386, L1993 (plan 14), L5501; WAVE "left: ranking movers", queue "planner speed rules" |
| 3 | P0-DE.2 | `doctor` names the rung each side and leg stands on, and its footprint | OPEN (two-way sides only, in a4d532b's unmerged commit) | S | ease of use | L149-151, L236 |
| 4 | P0-DE.3 | Two-way mark ladder per side (PostgreSQL origin / logical message / table; MySQL tagged GTID; MariaDB domain and skip_replication; statement comment; table), proved by a probe, ranked by measured cost, kept in the tail's token, named by doctor, `migkit_origin` dropped at teardown | IN PROGRESS a4d532b (two-way mark rungs; agent commit 2ad0c63, not merged) | L | two-way | L237-238, L4280-4287, L4314, L4316-4384, L5509-5521; WAVE a4d532b |
| 5 | P0-DE.4 | Every shape has a way: close the "not yet" cells of `decide.coverage()` (58 of 230 at 32ed544) | OPEN | L | move | L166-182, L223-229 |
| 6 | P0-KEYLESS | Keyless tables copied, checked and resumed by whole-row hash buckets (duplicates by count), including sources with no stored row position (MySQL, SQL Server) | OPEN (PostgreSQL resumes by stored position, done 2026-09-27; bucket verify comes with F1) | M | move | L174-182, L228-229, L2023-2029 (C3), L2047-2052 |
| 7 | BAR-1 | Neutral "migkit wrote this" mark across any engine pair: first write in the engine's atomic unit (MongoDB txn, Cassandra logged batch, Redis MULTI/EXEC, DynamoDB transact-write, Kafka header) | OPEN (PostgreSQL/MySQL/MariaDB only, via a4d532b) | L | two-way | L246-257 |
| 8 | BAR-2 | Exactly-once batches on every engine (the batch number carried in the mark where no native progress exists) | OPEN (done for PostgreSQL/MySQL counter hops, R3 in 7de74bc) | L | change follow | L258-266 |
| 9 | BAR-3 | Target kept read-only, or its writes named, on every engine | OPEN (grep: freeze.py covers PostgreSQL and MySQL only) | M | cutover | L273-275, L5499-5500 (DTS gap "read-only target on more engines") |
| 10 | 0f-DVT | DVT used whole: row hash, custom queries, threshold, grouped columns to bisect then migkit's own row compare, random-row sampling, filter-status fail, YAML batches, labels | IN PROGRESS a075608 (0f verifiers; paused, WIP a5473b4) | M | tools used whole | L303-315; WAVE a075608 |
| 11 | 0f-RELADIFF | reladiff as a proved rung, not a default (no jsonb rendering, breaks on Python 3.14), with the reason said where not used | IN PROGRESS a075608 (reladiff rung; paused) | S | verify | L2538-2540; WAVE a075608 |
| 12 | 0f-PGCOPYDB | pgcopydb used whole: split-tables, index/restore jobs, LO jobs, binary COPY, skip vacuum/analyze, list progress and summary into the planner's rates, sentinel as lag; jobs sized from migkit's facts | IN PROGRESS ad4afda (0f pgcopydb + Debezium; paused, research notes only) | M | tools used whole | L316-331, L2580, L2787 (F7); WAVE ad4afda |
| 13 | 0f-DEBEZIUM | Debezium used whole: skipped.operations, column mask/truncate/include, transaction metadata, snapshot select overrides and locking mode, batch/queue sizing, time/binary modes aligned to canon, SMTs, pause/resume/stop-snapshot, notification channel | IN PROGRESS ad4afda (paused) | M | tools used whole | L332-346 |
| 14 | 0f-MYSQLTOOLS | mydumper/myloader and MySQL Shell used whole (--rows, --checksum-all, masking, --where; Shell dump, load, copy), ranked per table by measured rate | IN PROGRESS ae351bc (MySQL tools whole; agent commit 38e4a81, not merged) | M | tools used whole | L348-350, L2577-2579, L2787-2788, L4761-4775 (R14); WAVE ae351bc |
| 15 | 0f-REGISTRY | Used / unused / topped-up table per wrapped tool, kept like `capabilities.matrix`, a reason for every unused feature | IN PROGRESS ad4afda (migkit/tooluse.py; grep: not on master; paused) | M | tools used whole | L366-369, L5443-5453; WAVE ad4afda |
| 16 | 0f-REST | Whole-use audit of the rest: pg_dump/pg_restore, pgloader, mongosync (/start body, /reverse, /progress), mongodump, redis-shake, MirrorMaker 2, DSBulk, CDM, clickhouse-backup, elasticdump, atlas/liquibase/migra, sqlglot, drivers (asyncpg, pymongo, valkey-glide) | OPEN | L | tools used whole | L347-365 |
| 17 | 0 | Planner: one decision per table with its reason, read out in the dry run | DONE (in text 2026-09-24; routed through decide.py in 32ed544) | M | move | L371-423 |
| 18 | 0e | Declared capability matrix, held by a test both ways | DONE (in text 2026-09-24) | M | engines | L425-440, L586-592 |
| 19 | 0e.say | Operator told in migkit's words when a command is not available for an engine | DONE (grep: capabilities.require/unavailable in cli.py) | S | ease of use | L438-440, L593-594 |
| 20 | 0e.fill | Gaps closed 2026-09-24/25: SQLite through the pair copier; MongoDB, Redis, Kafka, SQL Server copy, follow, fence, users, settings, restore points; empty-move guard everywhere | DONE (in text) | L | engines | L470-578, L596-634 |
| 21 | 0e.empty | The cross-engine copier replaces what it copies (a neutral "empty this table") instead of only upserting | DONE (grep: neutral_empty on every engine) | S | move | L635-641 |
| 22 | 0e.exclude | `exclude` read on every engine, SQL Server included | DONE (grep: mssql.py reads hop.excluded) | S | move | L643-648 |
| 23 | 0c | Progress and logs in migkit's own words; one progress vocabulary; no program named | DONE (in text) | M | ease of use | L418-423, L656-707 |
| 24 | 0d | Row filter honoured by move, check, repair, pair and tail | DONE (in text) | M | verify | L709-766 |
| 25 | 0a | DVT wrapped out of process as the second reader, wired into `check` by the planner | DONE (in text 2026-09-24) | M | verify | L768-853, L1656-1661, L4539-4541 |
| 26 | 0a.measure | Second reader measured: speed against migkit's checksum at size; type agreement with canon for json, time, bit, spatial and other engines | OPEN | S | verify | L847-850 |
| 27 | 0b | Re-read through the stream without pausing it (read-only incremental snapshots, MySQL and PostgreSQL) | DONE (in text) | M | change follow | L855-922 |
| 28 | 0b.window | A key changed inside a chunk's window, measured | BLOCKED (needs a machine with room for the Kafka Connect stack; the 4 GiB VM cannot) | S | change follow | L924-933 |
| 29 | 1 | Confirm before calling it different: fence per engine, cross-engine, replication migkit does not drive, SQL Server | DONE (in text; SQL Server by R13 2026-09-27) | M | verify | L937-1003, L4700-4701 |
| 30 | 2 | Source snapshot held for a bounded time (`snapshot_limit`), its age reported | DONE (in text) | S | verify | L1005-1051 |
| 31 | 3 | Target frozen against the application's writes (PostgreSQL, MySQL) | DONE (in text) | M | cutover | L1053-1122 |
| 32 | 4 | Repair that knows the stream is running: own tail, subscription and replica paused; refused beside others | DONE (in text) | M | verify | L1124-1185 |
| 33 | 5 | DDL during the move: seen on every path, the tail stops, verdicts it overtook marked stale, the native replica scoped to the hop | DONE (in text) | M | change follow | L1187-1290 |
| 34 | 5b | MySQL full+cdc from one point; `--mode cdc` after a separate copy | DONE (in text) | M | change follow | L1292-1333 |
| 35 | 6 | Pre-checks give the value to set: tables over 10k, case collisions, slots/senders, binlog settings, packet vs largest row, other writers, decoding spill, target room with its log, max_slot_wal_keep_size, key-less columns a replica cannot match, HA/replicas on the target | DONE (in text; R9 2026-09-27; grep: max_slot_wal_keep_size in postgres.py) | M | ease of use | L1337-1578, L4561-4580 |
| 36 | 6.mariadb | MariaDB's compressed row events read instead of stopping the tail | OPEN (grep: still refused, binlog_payload.py:18, mysql.py:4925) | S | change follow | L1372-1375 |
| 37 | 6.wal_sender | `wal_sender_timeout` recommendation | NOT PURSUED (left open on purpose until a failure shows a value) | S | ease of use | L1432-1440 |
| 38 | 6.fkwin | Foreign-key window around the PostgreSQL streaming copy, instead of falling back to the local copy | OPEN (grep: no FK window) | M | move | L1494-1497 |
| 39 | 7 | Cost in the plan: time from measured rates (as ranges, item 40), size and room, transfer price, free space where the server reports it | DONE (in text) | M | operations/scale | L1580-1620, L3942-3958 |
| 40 | 7.free | Free space of PostgreSQL and MySQL targets (system_stats, or the cloud's metrics) | BLOCKED (not reported over SQL; the cloud's metrics need an account) | S | operations/scale | L1603-1616, L4568-4575 |
| 41 | R9.docdb | DocumentDB pre-flight: change streams per collection, retention, DDL by catalogue snapshots (problems E4) | BLOCKED (no DocumentDB here) | M | engines | L2055, L4569-4570, L4585-4586 |
| 42 | 8 | Business-rule checks (SQL, MongoDB pipelines) | DONE (in text) | M | verify | L1624-1661 |
| 43 | 9 | Column keep/drop/rename honoured on every path | DONE (in text) | M | move | L1663-1819 |
| 44 | 10 | Newer-row-wins conflict policy (PostgreSQL, MySQL) | DONE (in text) | S | verify | L1821-1837 |
| 45 | 10.more | Newer-row-wins on more engines | OPEN (grep: newer_wins only in postgres.py, mysql.py, base.py) | S | verify | L5500 |
| 46 | 12 | MongoDB to MongoDB through mongosync | DONE (in text) | M | move | L1882-1947 |
| 47 | 13 | Kafka group offsets translated by the message they point at | DONE (in text) | M | cutover | L1949-1981 |
| 48 | plan-7 | What the target does to rows as they land (triggers quieted, identity) | DONE (what-a-migration-actually-costs.md item 7) | S | move | L1985 |
| 49 | plan-10 | Documents the table only points at (large objects behind oid columns) | DONE (plan item 10; carried by R10) | S | verify | L1986 |
| 50 | plan-18 | Continuous verification that costs what changed (pairs) | DONE (in text 2026-09-25) | M | verify | L1994-1999 |
| 51 | plan-19 | Bisection diffing across engines on the canonical rendering | DONE (in text 2026-09-25) | M | verify | L2000-2008 |
| 52 | A1 | Scope beyond the database (cron jobs, application config, downstream consumers) | OPEN (problems file A1 still Partly) | M | operations/scale | L2013 |
| 53 | A2/D10 | Privileges down to columns | OPEN | M | security | L2014, L5499 |
| 54 | A4 | MariaDB objects MySQL has no home for | DONE (in text 2026-09-25) | S | engines | L2015 |
| 55 | B4 | Collations mixed inside stored code (COLLATE clauses parsed from routine bodies) | OPEN (R9 names MySQL routines written under another collation) | M | types | L2017, L4581-4584, L5499 |
| 56 | B6 | Values the target refuses: zero dates named before a cross-engine move | DONE (in text 2026-09-25) | S | types | L2018 |
| 57 | C1.done | Tables side by side, equal-row ranges, the shared copier pipelined and writing PostgreSQL through COPY | DONE 1da6415 | M | move | L2019-2022, L2031-2046 |
| 58 | C1.managed | Speed compared against a managed service: recipe run on the same instance class, cost stated | OPEN | M | operations/scale | L2019-2022, L3028-3031, L3043-3045 |
| 59 | C3.done | Each range checked and checkpointed; a finished table asked again; streaming bulk resumes from finished tables | DONE 1da6415, in text 2026-09-27 | M | move | L2023-2029, L2042-2052 |
| 60 | C3.repair | A statement-level record of a repair | OPEN | S | verify | L2028-2029 |
| 61 | C4 | Verification load on the source: the two engines still missing and a lag-aware brake for replicas | OPEN (problems C4 still Partly) | M | verify | L2053, L5500 |
| 62 | D11 | Large-object references parked in plain integer columns (invisible to the check) | OPEN (problems D11 still Partly) | S | verify | L2054 |
| 63 | D15 | How much of a row a cross-engine comparison compares | OPEN (problems D15: ended for same-engine legs only) | S | verify | L5500 |
| 64 | F1.dual | Dual writes drift (problems F1) | OPEN | M | cutover | L2056 |
| 65 | F4.pool | Poolers named in `assess` | DONE (in text 2026-09-25) | S | ease of use | L2057 |
| 66 | G2 | `mask` for values the drilldown shows | DONE (in text 2026-09-25) | S | security | L2058 |
| 67 | P3.pw | pgcopydb given no password on its command line | DONE (in text) | S | security | L2064 |
| 68 | P3.idx | Index window names indexes schema-qualified | DONE (in text) | S | move | L2065 |
| 69 | P3.pub | `create publication` made only where missing | DONE (in text) | S | change follow | L2066 |
| 70 | P3.follow | `follow` ends at the current position; no long-running mode | OPEN (the table's note is unchanged; migkit's own tail runs long) | S | change follow | L2067 |
| 71 | P3.generic | Generic engine reads string length and nullability | DONE (in text) | S | engines | L2068 |
| 72 | P3.health | MySQL `_health` reports replica lag and the throttle backs off | DONE (in text) | S | operations/scale | L2069 |
| 73 | P3.hetero-deep | Deep battery for cross-engine hops | DONE (matrix: hetero deep checks yes) | M | verify | L450, L631, L2070 |
| 74 | P3.sqlglot | sqlglot kept for defaults a type map cannot translate | DONE (in text, decided 2026-09-25) | S | conversion | L2071 |
| 75 | P3.measure | Measured before wrapping: boto3 Secrets Manager, mongodump --query/--oplog, mydumper --regex/--rows | DONE (item 38; R14) | S | tools used whole | L2072, L4759-4775 |
| 76 | P3.resumedump | Tell the operator when a restart loses the dump's progress (DTS DumperResumeCtrl, "resumable dump status") | OPEN (grep: none) | S | move | L2073, L5499 |
| 77 | P3.timed | Timed start and auto-retry window (DTS "start at a time") | DONE 7de74bc (grep: schedule.py retry_window, max_duration, catch-up) | S | operations/scale | L2074, L4419-4421, L4444-4448, L5498 |
| 78 | P3.dryrun | Should dry-run plans hide the command lines they print | BLOCKED (waiting on the owner) | S | ease of use | L2076 |
| 79 | 14/R6 | Network path migkit opens itself: SSH or command tunnels, doctor tells "connects but bulk stalls" apart, stall watchdog | DONE (in text 2026-09-27) | M | operations/scale | L2093-2105, L4474-4501 |
| 80 | R6.cloud | The clouds' own forwarders signed in (SSM, IAP, Cloud SQL proxy, Azure Bastion) | BLOCKED (needs cloud accounts) | S | operations/scale | L4485-4486 |
| 81 | 15 | Who wrote the rows only the target has (PostgreSQL, MongoDB, MySQL) | DONE (in text) | M | verify | L2107-2151 |
| 82 | 16 | Side scripts in tools/ folded into migkit or removed | DONE (in text) | M | operations/scale | L2153-2185 |
| 83 | 17 | MySQL events repaired, not only detected | DONE (in text) | S | conversion | L2187-2205 |
| 84 | 18 | Schema fix's undo rehearsed on a scratch database (PostgreSQL, MySQL) | DONE (in text) | M | cutover | L2207-2238 |
| 85 | 19 | Enums, domains, hstore, tsvector rendered across engines; same-engine types by their own text | DONE (in text; interval in F4.b) | M | types | L2242-2283 |
| 86 | 20 | PostgreSQL-only helpers ported where the idea exists (mojibake repair on MySQL) | DONE (in text; SQL Server row-level security in the SQL Server line) | S | engines | L2285-2303 |
| 87 | 21 | `setup_target_plan` for MySQL | DONE (in text) | S | move | L2305-2320 |
| 88 | 22 | Coverage of `unchanged_since` answered (PostgreSQL only; the rest re-read) | DONE (in text) | S | verify | L2322-2330 |
| 89 | 23 | The time zone the data is written in (deep check) | DONE (in text) | S | types | L2332-2353 |
| 90 | 24 | A target that is correct and slow: the source's reads replayed and planned on both sides | DONE (in text) | M | verify | L2355-2388 |
| 91 | 25 | Long reads over unstable links: MongoDB read in resumable chunks across key types | DONE (in text) | S | verify | L2390-2409 |
| 92 | 26 | Research of HVR, Qlik, Striim, Airbyte, Bytebase, Trino; the 2026-09-27/28 research passes with scorecards | DONE (docs/research-other-tools.md; 0b64ddb, 6d2675d, 6d5ebe5) | M | tools used whole | L2413-2430, L5455-5482 |
| 93 | 26.rest | sqlpipe and schemachange read | OPEN | S | tools used whole | L2429-2430 |
| 94 | 26.trino | Trino measured as a candidate second reader | OPEN | S | verify | L2428-2429 |
| 95 | 26.striim | Striim-style interval validation | OPEN | S | verify | L2424-2425 |
| 96 | 26.bytebase | Bytebase-style schema snapshot at each change | OPEN | S | change follow | L2425 |
| 97 | MSSQL-x86 | SQL Server at full depth on a real server: T-SQL paths run, CDC proper, Agent jobs, row-level security (item 27) | BLOCKED (full SQL Server is x86-only; the x86 CI workflow is in a8d572b, not merged or run) | L | engines | L620-626, L2302-2303, L2432-2439, L3662-3663, L3936-3938, L4631-4636 |
| 98 | AUD.part | Every partition held to its own count and digest (PostgreSQL, MySQL); A5 | DONE 659b54f | M | verify | L2454-2471 |
| 99 | AUD.slot | Slot made with its snapshot; MySQL position taken after commits in flight; property-tested | DONE 659b54f | M | change follow | L2472-2502 |
| 100 | AUD.slot.process | MySQL `copy_point` waits nothing without PROCESS | OPEN | S | change follow | L2503-2504 |
| 101 | AUD.slot.more | MongoDB resume token and MariaDB/Percona snapshot position measured the same way | OPEN | S | change follow | L2504-2505 |
| 102 | AUD.shift | Uniform time shift and type narrowing reasoned in one place, both engines | DONE 659b54f | S | types | L2506-2520 |
| 103 | AUD.osc | Online-rewrite working tables left out everywhere | DONE 659b54f | S | move | L2521-2531 |
| 104 | AUD.osc.tail | The change tail recognises gh-ost/pt-osc tables beside their table, not by name alone | OPEN (grep: hetero.py calls drift.transient on the name) | S | change follow | L2532-2533 |
| 105 | AUD.atlas | `schema_authority: atlas` alias shape for a renamed table | OPEN | S | verify | L2535-2537 |
| 106 | AUD.release | PyPI upload (is the name free) and a brew tap | BLOCKED (waiting on the owner) | S | operations/scale | L2541-2542 |
| 107 | DEC.licence | Relicense to AGPL-3.0-or-later | DONE be382a4 | S | tools used whole | L2557-2569 |
| 108 | DEC.terms | Closed tools installed with the operator's acceptance (`doctor --install`, MIGKIT_ACCEPT_TERMS), the open path where terms forbid, licence check over every dependency | IN PROGRESS aa2c527 (F0 outward; agent commit c580c69, not merged; grep: MIGKIT_ACCEPT_TERMS absent on master) | M | tools used whole | L2562-2569 |
| 109 | DEC.deps | Every worthwhile tool a dependency: mssql-python, opensearch-py, confluent-kafka and the rest in pyproject; mydumper, MySQL Shell, RedisShake, DSBulk, Ora2Pg, SQLines, plpgsql_check through `doctor --install` | OPEN (grep: pyproject lacks mssql-python, opensearch-py, confluent-kafka; parts in a8d572b, ae351bc, ae866ee) | M | tools used whole | L2570-2576 |
| 110 | DEC.ptcs | pt-table-checksum only on native replicas, always paired with migkit's sum digest | OPEN (grep: not used) | S | verify | L2582-2583 |
| 111 | DEC.colima | Second colima profile for Oracle Free and x86 engines, started on demand, docker context restored | DONE df60027, e4b5275, 53bf947 | S | operations/scale | L2612-2616 |
| 112 | DEC.x86ci | Free x86 CI on GitHub for SQL Server, Db2, ASE | IN PROGRESS a8d572b (SQL Server bulk + x86-engines.yml; agent commit a6090e7, not merged) | S | operations/scale | L2616-2617, L2721-2722 |
| 113 | DEC.research | Leapfrog research before building (conversion, engine reach, live tail) | DONE 343d2ce, 13ef328, c716e3a | M | tools used whole | L2618-2622 |
| 114 | S1.a | Oracle changes read by migkit's own LogMiner reader (RDS-capable, no licence) | IN PROGRESS a47bffb (Oracle full side, LogMiner reader) | L | change follow | L1879-1880, L2585-2590, L2716, L2933-2941 |
| 115 | S1.c | A compiled Oracle change reader as a rung | OPEN | M | change follow | L2587, L2940 |
| 116 | S1.d | OpenLogReplicator driven as a separate program next to the redo | OPEN | M | change follow | L2587-2588, L2938-2939 |
| 117 | S1.e | Oracle hop accepted only after its reader is measured against the source's redo rate | OPEN | S | change follow | L2588-2589, L2940-2941 |
| 118 | S3.link | Kafka Cluster Linking / Shadowing driven where the operator holds the licence | OPEN | S | cutover | L2600-2602, L2950-2953 |
| 119 | S4 | Zero-ETL where offered and Azure's MI link driven through the provider's API | OPEN | M | operations/scale | L2606-2610, L2954-2957 |
| 120 | F0.1 | MongoDB digest by raw-BSON hash ($toHashedIndexKey made every number an int64) | DONE 2a4c652 | S | verify | L2636-2639 |
| 121 | F0.2 | `prove_converted`: procedures never run, triggers and events unchecked, failing input not named | IN PROGRESS ae866ee (F0 prove_converted + R11) | S | conversion | L2640-2643 |
| 122 | F0.3 | SQL Server Change Tracking read under SNAPSHOT; rowversion out of the hash; CLR types hashed | DONE 2a4c652 | S | verify | L2644-2647 |
| 123 | F0.4 | Types G1-G4, G8: infinity, BC years, sub-microsecond digits, extra_float_digits, keys a target collation merges counted before the move | DONE 3b05234 | M | types | L2648-2653 |
| 124 | F0.5 | Cutover bugs: fence on the insert LSN, fence_wait on the hop's own slot, rollback raises the old source's sequences | IN PROGRESS a2ba2fe (cutover bugs + F2) | S | cutover | L2654-2659 |
| 125 | F0.6 | MySQL digest: salted 64-bit md5 sum, no CRC lane | DONE 2a4c652 | S | verify | L2660-2662 |
| 126 | F0.7 | mongosync committed on its reported lag (canCommit); Redis absolute TTL, IDLETIME/FREQ, every cluster node | IN PROGRESS aa2c527 (agent commit c580c69, not merged) | S | move | L2663-2665 |
| 127 | F0.8 | PostgreSQL tail: a newline inside a value stalled it | DONE f3884fa | S | change follow | L2666-2667 |
| 128 | F0.9 | Outward: pt tools without calling home, Atlas Community build, Liquibase 4.x, leftovers (gh-ost _ghk, pgcopydb, pglogical, Spock, Bucardo, pg_repack, pgstream schemas), datacompy's report naming itself | IN PROGRESS aa2c527 (c580c69, not merged; datacompy naming also in a075608) | S | security | L2668-2672 |
| 129 | F0.10 | Security defaults: certificates verified by default, md5 marked not for security, a publication of the hop's tables only, the dashboard token out of the URL | IN PROGRESS aa2c527 (c580c69, not merged) | S | security | L2673-2675 |
| 130 | F1.a | One scan returns every leaf of the digest tree; buckets by a hash of the key so composite, text and cross-engine keys localise; passes A-D composed | IN PROGRESS a31b892 (proof kept up + one-scan tree + IBLT; uncommitted leaves.py, kept.py) | M | verify | L2677-2685, L5243-5245; WAVE a31b892 |
| 131 | F1.b | In-SQL IBLT: few differences found over a slow link, no key needed | IN PROGRESS a31b892 | M | verify | L2680-2682, L5244-5245 |
| 132 | F1.c | Self-check: the differences found add up to the change in count and sum | OPEN | S | verify | L2682-2684 |
| 133 | F1.d | Generations under writes (migration-verifier style) | OPEN | M | verify | L2684-2685 |
| 134 | F2 | Cutover as one timed path with automatic undo, driven by a `cutover:` hop option (preflight, reverse armed and verified, freeze per engine, drain, final verify, counters, jobs, flip, budget); writes to the source only under `cutover:` | IN PROGRESS a2ba2fe (cutover path; WIP b477336) | L | cutover | L1988-1992 (plan 13), L2550-2556, L2687-2695; WAVE a2ba2fe |
| 135 | F3 | Stored code converted per routine by the best proved candidate (operator's file, sqlglot, rules, Ora2Pg, SQLines, model loop), proved by execution on both sides, plpgsql_check gate, residue in three classes; MySQL and SQL Server sources | IN PROGRESS ae866ee (R11 pipeline; storedcode/ in its worktree) | L | conversion | L1841-1846 (11), L2009, L2697-2706, L3891-3899, L4612-4619, L5492 |
| 136 | F4.a | New canon classes and value rules: uuid, array, inet, xml, 9-place timestamps, value-normal decimals, float4 widened, infinity uncomparable, JSON in-process; refusals before the move (unsigned 64-bit, NUL, 4-byte characters, DynamoDB over 38 digits) | DONE 3b05234 | M | types | L2708-2713 |
| 137 | F4.b | interval, vector and geometry canon classes (interval still left out across engines) | IN PROGRESS addeddc (types remainder) | M | types | L2281-2283, L2708-2710; WAVE 3b05234 note |
| 138 | F4.c | G5: MongoDB types round trip | IN PROGRESS addeddc (types remainder) | S | types | WAVE 3b05234 note |
| 139 | F4.d | Empty Cassandra collections | IN PROGRESS addeddc (types remainder) | S | types | WAVE 3b05234 note |
| 140 | F6 | Speed of migkit's own paths: MySQL decoder in its own process chosen by measured rate, PostgreSQL tail on one kept connection, apply trims and GC held, renderer per column | DONE f3884fa | M | change follow | L2724-2784 |
| 141 | F6.no | Free-threaded 3.14t, subinterpreters, PyPy for all of migkit, orjson or XXH3 in the digest | NOT PURSUED ("not now", F6; XXH3 measured and not adopted, R18.3) | S | move | L2730-2731, L5025-5028 |
| 142 | F7.rshake | RedisShake as a rung (readers, Lua, filters, status port) | OPEN | M | tools used whole | L356-357, L599-600, L2789 |
| 143 | F7.mm2 | MirrorMaker 2 wrapped whole with a source-offset header and group translation | OPEN | M | tools used whole | L357-358, L607-609, L2789-2790 |
| 144 | F7.dsbulk | DSBulk (splits, checkpoint and replay, count modes, preserved TTL and timestamps) | OPEN | M | tools used whole | L358-359, L2790 |
| 145 | F7.rfs | OpenSearch reindex-from-snapshot | OPEN | M | tools used whole | L2791 |
| 146 | F7.ddbexp | DynamoDB export to and import from S3 | OPEN | M | tools used whole | L2791 |
| 147 | F7.kclamp | Kafka consumer-offset clamp at cutover | OPEN | S | cutover | L2793 |
| 148 | F7.selfstop | A scheduled tail that stops when caught up | OPEN | S | change follow | L2793-2794 |
| 149 | F7.beat | Source-commit timestamp in the tail's beat | OPEN | S | change follow | L2794-2795 |
| 150 | F7.commit | A batch ended at the last COMMIT seen, so a transaction that fits is never split | OPEN | S | change follow | L2795; goldengate-qlik-hvr report L1113 |
| 151 | F7.merge | Changed-columns merge when two sides touched different columns (and USEMIN) | OPEN | S | two-way | L2795-2796; goldengate-qlik-hvr report L1124 |
| 152 | F7.edges | Statistics-based chunk edges | OPEN | S | move | L2796 |
| 153 | F7.exact | Exact batches by default on one-way tails (resume after the committed batch) | OPEN | M | change follow | L2797, L5167-5171 (R19.11) |
| 154 | F7.sec | The 16 remaining security fixes from the security scorecard | OPEN (some defaults in aa2c527's F0 work) | M | security | L2799; WAVE queue |
| 155 | CV.P1 | Candidate portfolio, selection by evidence | IN PROGRESS ae866ee (storedcode/rungs.py, keep.py in its worktree) | M | conversion | L2802-2828; leapfrog-conversion L305 |
| 156 | CV.P2 | Counterexample bank, re-run on every candidate | IN PROGRESS ae866ee | S | conversion | L2808-2810; report L306 |
| 157 | CV.P3 | Model as hole-filler on the best partial candidate, bounded rounds | IN PROGRESS ae866ee (storedcode/model.py in its worktree; the backlog does not list it) | M | conversion | report L307 |
| 158 | CV.P4 | Two-sided reduction (Hypothesis shrinking, ddmin) | IN PROGRESS ae866ee (storedcode/reduce.py) | M | conversion | L2820-2821; report L308 |
| 159 | CV.P5 | Aligned intermediate-state capture | OPEN | M | conversion | report L309 |
| 160 | CV.P6 | Inputs from the real call history | OPEN | S | conversion | L2819; report L310 |
| 161 | CV.P7 | Concolic solving (z3) for untaken branches | IN PROGRESS ae866ee (storedcode/concolic.py) | M | conversion | L2819-2820; report L311 |
| 162 | CV.P8 | Stateful call sequences | OPEN | M | conversion | report L312 |
| 163 | CV.P9 | Emulation candidates (orafce, MariaDB Oracle mode, IvorySQL, openGauss, Babelfish, openHalo) | IN PROGRESS ae866ee | M | conversion | L2814-2818; report L313 |
| 164 | CV.P10 | Workload corpus + cross-engine replay (a cross-engine pt-upgrade on workload.py), frequency-weighted | OPEN (queued as its own item) | M | conversion | L2810-2814, L2827-2828; WAVE queue |
| 165 | CV.P11 | Static application scan (ast-grep + sqlglot) | OPEN | M | conversion | L2813-2814; report L315 |
| 166 | CV.P12 | Constraints proved to accept and reject the same rows (z3 CHECK equivalence) | OPEN | M | conversion | L2821-2822; report L316 |
| 167 | CV.P13 | Collation agreement matrix from real values | OPEN | S | conversion | L2822; report L317 |
| 168 | CV.P14 | Types sized from data with recorded domain changes | OPEN | S | conversion | report L318 |
| 169 | CV.P15 | Index/partition advice on the translated workload (HypoPG, Dexter) | OPEN | M | conversion | L2822-2823; report L319 |
| 170 | CV.P16 | Rules learned from accepted fixes, adopted only with zero regressions | IN PROGRESS ae866ee (storedcode/learn.py) | L | conversion | L2823; report L320 |
| 171 | CV.P17 | Built-in semantics mined from the engines' own test suites | OPEN | M | conversion | report L321 |
| 172 | CV.P18 | The harness's own mutation score | IN PROGRESS ae866ee (storedcode/mutants.py) | M | conversion | L2810-2811; report L322 |
| 173 | CV.P19 | Emulation as oracle after cutover | OPEN | M | conversion | report L323 |
| 174 | CV.P20 | Scalar routines: bounded verification of a model of both bodies | OPEN (research) | L | conversion | report L324 |
| 175 | CV.P21 | Schema restructuring with a proof | OPEN (research) | L | conversion | report L325 |
| 176 | W3 | Code conversion at enterprise breadth: PL/SQL packages with emulation, T-SQL at depth, application-embedded SQL found and converted | OPEN (pieces in CV.P9-P11) | L | conversion | L2972-2976 |
| 177 | 40.conv | Effort estimates for conversion (Ora2Pg cost units) | OPEN | S | conversion | L3959-3960, L4614-4615 |
| 178 | ER1 | Exact warehouse loads + one SHA-256 row digest in each engine's SQL (BigQuery committed streams, Snowpipe Streaming channels, Redshift COPY, Delta txn, Iceberg snapshot properties; staged Parquet) | IN PROGRESS a7120bc (warehouse exact loads + SQL digest; paused) | L | engines | L2797-2798, L2832-2842, L3657-3659 |
| 179 | ER1.acct | Warehouses run against real accounts; their other cells (stream, fence, confirm, users, settings, snapshot, deep, sequences) | BLOCKED (needs accounts; emulators have gaps) | M | engines | L3645-3659, L4543-4544, L4756-4757 |
| 180 | ER2 | Copybook engine (EBCDIC, packed and zoned decimals) and IBM i with DISPLAY_JOURNAL follow and HASH_ROW | IN PROGRESS a8c4cd1 (copybook + IBM i; paused) | L | engines | L2843-2851 |
| 181 | ER2.zos | Db2 for z/OS through ibm_db | BLOCKED (needs the operator's Db2 Connect licence; IMS/CICS/VSAM changes said as unreadable) | M | engines | L2848-2851 |
| 182 | ER3 | Teradata | OPEN | M | engines | L2852-2854 |
| 183 | ER4 | One change-topic reader (TiDB, Couchbase, Aurora DSQL, CockroachDB) | OPEN | M | engines | L2855-2857 |
| 184 | ER5 | Cosmos DB NoSQL | OPEN | M | engines | L2858-2859 |
| 185 | ER6 | SAP application data through ODP over OData | OPEN | M | engines | L2860-2863 |
| 186 | ER7 | SaaS: dlt, Singer taps, Airbyte connectors as programs, Salesforce Pub/Sub | OPEN | L | engines | L2864-2867 |
| 187 | W5 | Other sources: Netezza, SAP HANA, Informix (x86), Progress, Vertica, Exasol, Databricks/Iceberg | OPEN | L | engines | L2720, L2833-2835, L2982-2987 |
| 188 | L1 | Tables added or removed in a running tail, each with its own snapshot and seam | OPEN | M | change follow | L2875-2882, L2792 (F7), L5498 (DTS "a running job changed"); live-tail report L1066 |
| 189 | L2 | Exact seam on PostgreSQL by xid visibility, not LSN | DONE 6f07ceb | S | change follow | L2883-2887, L5116-5155 |
| 190 | L3 | Exact seam on SQL Server (CT version in SNAPSHOT) and MongoDB (clusterTime) | OPEN (MySQL by GTID marks done in 6f07ceb) | S | change follow | live-tail report L1068 |
| 191 | L4 | A DDL parks only its table, its changes spooled | OPEN | M | change follow | L2881-2882, L2792-2793 (F7) |
| 192 | L5 | Positional DDL: additive changes applied at their log position, events decoded under the schema as of their position (DTS DDL replication, allow-list, added columns) | OPEN | M | change follow | L1277-1278, L1287-1288, L2882, L2797 (F7), L5497-5498 |
| 193 | L6 | Re-add a table from its removal position | OPEN | S | change follow | live-tail report L1071 |
| 194 | L7 | Per-table accounting identity every batch | DONE 6f07ceb (grep: hetero._Accounts; the spool term arrives with L4) | S | change follow | L2887-2890, L5139-5141 |
| 195 | L8 | Publication-add guard below PostgreSQL 17.5 | OPEN | S | change follow | L2889-2890; report L1073 |
| 196 | LT.R1 | Depth D0-D4 per table in assess/doctor, with the statement that raises it | OPEN | S | change follow | L2891-2893 |
| 197 | LT.R2 | SQL Server without CT/CDC: the log-backup chain restored onto a SQL Server migkit owns | BLOCKED (full SQL Server is x86-only) | L | change follow | L2897-2900 |
| 198 | LT.R3 | SQL Server live `fn_dblog` reader | BLOCKED (full SQL Server is x86-only) | L | change follow | report L1076 |
| 199 | LT.R4 | Oracle partial updates lifted to full images by flashback reads | OPEN (after the Oracle agent) | M | change follow | L2900-2902 |
| 200 | LT.R5 | Oracle NOLOGGING operations detected | OPEN | S | change follow | L2902 |
| 201 | LT.R6 | Db2 without DATA CAPTURE CHANGES (db2ReadLog) | BLOCKED (Db2 is x86-only) | M | change follow | L2903-2904 |
| 202 | LT.R7 | MySQL MINIMAL/NOBLOB/PARTIAL_JSON row images lifted | OPEN | S | change follow | L2893-2895 |
| 203 | LT.R8 | MySQL STATEMENT/MIXED binlogs as invalidations | OPEN | M | change follow | report L1081 |
| 204 | LT.R9 | PostgreSQL at wal_level=replica via pg_walinspect/pg_waldump | OPEN | M | change follow | L2895-2897 |
| 205 | LT.R10 | MongoDB standalone (dbHash + raw-BSON bucket digests) | OPEN (in the report's table; the backlog names R1-R9) | S | change follow | report L1083 |
| 206 | LT.C1 | mongosync's job rebuilt from change streams, raw BSON and generations, bypassWriteBlockingMode audited | OPEN | M | change follow | L2598-2600, L2905-2909 |
| 207 | LT.C2 | Kafka offsets kept identical for gap-free partitions (pad, DeleteRecords) | OPEN | M | cutover | L2600-2602, L2909-2911 |
| 208 | LT.C3 | Kafka offset map written in the data's transaction | OPEN | M | cutover | L2911-2913 |
| 209 | LT.C4 | Redis/Valkey two-site active-active rules | OPEN | L | two-way | L2602-2603, L2913-2914 |
| 210 | LT.C5 | XStream's job: LogMiner on a migkit Oracle Free mining instance (mining on a standby) | OPEN | M | change follow | L2586, L2604, L2914-2916, L2939 |
| 211 | LT.C6 | RIOT-X's live mode made lossless (PSYNC client or IDLETIME sweep) | OPEN | M | change follow | L2916-2917 |
| 212 | LT.P1 | A mover that scales itself on the source's stress (range queue, k8s/ECS/SSH workers) | OPEN | M | operations/scale | L2918-2920 |
| 213 | LT.P2 | Seeding from ZFS/LVM/EBS snapshots, only changed blocks, fast-forwarded | OPEN | M | move | L2920-2922, L5066-5069 |
| 214 | LT.P3 | Zero-ETL-like continuous hop for any source and warehouse | OPEN | L | change follow | L2922-2923 |
| 215 | W1 | Mover HA and scale-out: standby takeover within seconds; one table's ranges across machines with a conditional-write checkpoint | OPEN | L | operations/scale | L2960-2965, L3277-3279 (31), L4462-4469 (R5); WAVE queue |
| 216 | W2 | The estate: discovery, sizing from performance history, cost, fleet, REST API, Terraform provider, K8s operator, OIDC/SAML/SCIM | OPEN | L | operations/scale | L2966-2971 |
| 217 | W4.a | Transformation in flight (joins, enrichment, windows) as hop rules under the same verify | OPEN (supersedes "ETL not pursued" and the 0e open question) | L | move | L650-654, L2977-2981 |
| 218 | W4.b | Format-preserving encryption and tokenization | OPEN | M | security | L2977-2981 |
| 219 | W6 | Types only commercial engines have (SDO_GEOMETRY, XMLType, object types, hierarchyid, sql_variant, FILESTREAM, DECFLOAT, GRAPHIC) | OPEN | L | types | L2988-2992 |
| 220 | W7 | Resharding: split, merge, distribution-aware load | OPEN | L | move | L2993-2996 |
| 221 | W8 | Offline signed bundle, container image, Windows | OPEN | M | operations/scale | L2997-3000 |
| 222 | W9 | Compiled apply side (collapse, render, write) above ~100k changes/s | OPEN | L | change follow | L3001-3003, L5207-5208 |
| 223 | 28 | Re-runnable benchmark harness (six shapes, move paths, check, lag at a rate) | DONE (in text 2026-09-25) | M | operations/scale | L3017-3045 |
| 224 | 28.rates | Lag at 10k and 50k tx/s | BLOCKED (hardware: the 2-CPU VM cannot produce the rates) | S | operations/scale | L3043-3051 |
| 225 | 29 | Change apply that keeps up: a batch one transaction, rows collapsed, runs of rows, large transactions streamed, parallel native appliers, MariaDB slave_pos | DONE (in text 2026-09-25) | M | change follow | L3053-3168 |
| 226 | R2 | The applier: read and apply together, lanes by dependency, errors, COPY stage on PostgreSQL, foreign keys off where every parent is in scope | DONE (in text 2026-09-27; items 1-3, 4 on PostgreSQL, 5) | L | change follow | L3149-3168, L4174-4244 |
| 227 | R2.4b | Apply path by measured cost: MySQL through the pinned LOAD DATA into a stage, PostgreSQL pipeline mode and binary COPY for small runs (R19 lever 10) | IN PROGRESS a5bea35 (write paths; agent commit 901b89e, not merged) | M | change follow | L2580, L4191-4194, L5162-5166 |
| 228 | 30 | Write lock as a lease; run state (lease, checkpoints, change position, copy record) in the bucket | DONE (in text 2026-09-25) | M | operations/scale | L3170-3240 |
| 229 | R5 | Control plane: schedule, operations view (hold, resume), approvals signed with SSH keys, chained audit, roles behind a proxy | DONE 7de74bc (in text 2026-09-27) | L | operations/scale | L3170-3186, L4415-4437 |
| 230 | R5.actions | View actions: drain to a position, abort, approve a cutover when lag and the last verify allow | OPEN | M | operations/scale | L4449-4453, L5493 |
| 231 | R5.approve | Approving from the view | NOT PURSUED (left out on purpose: an approval stays a signature) | S | security | L4436-4437 |
| 232 | R5.anchor | Signed audit checkpoints anchored in an object-locked bucket | OPEN | S | security | L4459-4461, L5493-5494 |
| 233 | 31 | Scale-out by tables across machines (`share_tables`) | DONE (in text 2026-09-25) | M | operations/scale | L3242-3279 |
| 234 | 32 | Alerts: tail heartbeat, stop record, metrics, shipped rules, notify receivers, retention per engine | DONE (in text 2026-09-25) | M | operations/scale | L3281-3341 |
| 235 | 32.rds | Managed MySQL binlog retention read | OPEN (grep: stream_room gives nothing on RDS) | S | operations/scale | L3338-3340 |
| 236 | 44/R7 | Failure caused on purpose: failpoints, kills, link cuts, failover, full disks, lost positions | DONE (in text 2026-09-25/27) | L | operations/scale | L3343-3461, L4503-4535 |
| 237 | 45.mem | Memory flat as tables grow; the terabyte recipe | DONE (in text 2026-09-25) | M | operations/scale | L3474-3509 |
| 238 | 45.run | Terabyte-class run and migkit's own move at size, time and cost stated | BLOCKED (waits on the owner renting machines; the VM has a 20 GiB disk) | L | operations/scale | L3469-3473, L3506-3509 |
| 239 | 46 | Wrapped programs stay wrapped: flags held against the installed --help, per-program rows in assess, CI matrix | DONE (in text; the GitHub workflow has not run yet) | M | tools used whole | L3511-3565 |
| 240 | 33 | ClickHouse, Kinesis, Pub/Sub, Event Hubs | DONE (in text 2026-09-25) | L | engines | L3569-3643 |
| 241 | 34 | Parquet, Cassandra/Scylla, OpenSearch, DynamoDB as sides of a pair | DONE (in text 2026-09-25) | L | engines | L3661-3780 |
| 242 | 35 | Change delivery as json, debezium or canal, routing rules, oversize skipped and counted | DONE (in text 2026-09-25) | M | change follow | L3782-3807 |
| 243 | R4 | Avro through a schema registry; AWS_MSK_IAM signed | DONE (in text 2026-09-27) | M | change follow | L4386-4413 |
| 244 | R4.msk | A real MSK IAM handshake | BLOCKED (needs an AWS account) | S | change follow | L4399-4401, L4411-4413 |
| 245 | R4.formats | JSON Schema, then Protobuf, through the registry | OPEN | S | change follow | L4408-4409 |
| 246 | 36 | Two-way on PostgreSQL's own replication | DONE (in text 2026-09-25) | M | two-way | L3809-3838 |
| 247 | R3 | MySQL native two-way; migkit's own tails both ways; conflict policies (error, apply_remote, keep_local, last_update_wins, source_priority, delta counters) | DONE 7de74bc (in text 2026-09-27) | L | two-way | L4245-4314 |
| 248 | R3.topo | Many-node topologies: many-to-one, one-to-many, full mesh | OPEN | L | two-way | L3836-3837, L4294-4296, L4314; WAVE queue |
| 249 | 37 | Live reverse replication at cutover (PostgreSQL) | DONE (in text 2026-09-25) | M | cutover | L3840-3857 |
| 250 | 37.my | Reverse at cutover measured on MySQL | OPEN (grep: tests cover PostgreSQL only) | S | cutover | L3855-3857 |
| 251 | 38 | Passwordless sign-in (RDS IAM tokens, assumed roles) and the clouds' secret stores, checked offline | DONE (in text 2026-09-25) | M | security | L3859-3886 |
| 252 | 38.real | RDS token sign-in, Cloud SQL IAM, managed identities against the real services | BLOCKED (needs accounts) | S | security | L3888-3889 |
| 253 | 39 | Views and one-expression functions converted across MySQL, PostgreSQL, SQL Server, with proof | DONE (in text 2026-09-25) | M | conversion | L3891-3940 |
| 254 | 41 | Threat model, private local copy, redaction, audit log, SBOM and provenance, restore points encrypted | DONE (in text 2026-09-25) | M | security | L3962-3994 |
| 255 | R17b | Files holding values encrypted to the hop's recipients (evidence.py, age) | DONE (in text 2026-09-27) | M | security | L3996-3997, L4438-4442, L4892-4902 |
| 256 | R17b.hmac | Keyed HMAC for what a shared report shows | OPEN (grep: masking.py uses a salted hash, no HMAC) | S | security | L4441-4442, L4470-4471, L4874-4875, L4900-4901, L5494 |
| 257 | R17b.prog | Files written by driven programs (dumps) encrypted | OPEN | S | security | L4901-4902 |
| 258 | DOCS | Docs refresh: the threat model's at-rest section and other stale docs | OPEN | S | operations/scale | L5501; WAVE queue "docs refresh" |
| 259 | 42 | Troubleshooting guide generated from the code; diagnostics bundle | DONE (in text 2026-09-25) | S | ease of use | L3999-4018 |
| 260 | 43 | AI assistance, any provider: plain-language findings, conversion proposals under the same proof | DONE (in text 2026-09-25) | M | conversion | L4022-4072 |
| 261 | 43.fix | Drafting fixes through the same proof | OPEN | S | conversion | L4074-4075 |
| 262 | NP.ETL | ETL beyond the mapping migkit has | NOT PURSUED (the owner's 2026-09-28 decision reopens transformation in flight as W4.a) | S | move | L4077-4079 |
| 263 | R0.a | MySQL tail lost rows of a transaction larger than a read | DONE (in text 2026-09-27) | S | change follow | L4095-4106 |
| 264 | R0.b | `on duplicate key update` on tables with more than one unique index | DONE (in text 2026-09-27) | S | change follow | L4107-4115 |
| 265 | R0.c | The applier's collapse broke foreign-key order | DONE (in text 2026-09-27) | S | change follow | L4116-4121 |
| 266 | R1 | Parallelism sized by migkit (estimate, controller, workers a ceiling), scaling up within a machine | DONE (in text 2026-09-27) | M | operations/scale | L3277-3279, L4123-4172, L5091-5099 |
| 267 | R8.more | DVT for Oracle, Db2, Snowflake, BigQuery | BLOCKED (x86 runner and accounts) | M | verify | L4542-4544, L4548-4552 |
| 268 | R8.mv | MongoDB migration-verifier with its metadata on a local server | OPEN (waited on the owner's download go-ahead; the 2026-09-28 decision makes worthwhile tools dependencies) | M | verify | L1884-1887, L4544-4545, L4553-4554 |
| 269 | R8.explain | A mismatch narrowed to one range explained column by column (datacompy) | OPEN | S | verify | L2072, L4545-4546, L4555-4556 |
| 270 | R10 | Large values sized; PostgreSQL large objects carried in pieces | DONE (in text 2026-09-27) | M | move | L4588-4600 |
| 271 | R10.pieces | MySQL and Oracle large values read in pieces and appended (plan 12, LOBs) | OPEN | M | move | L1987, L4600-4610, L5492 |
| 272 | R12.minio | Object-store tests off MinIO's images (VersityGW) | OPEN (grep: tests still start bitnamilegacy/minio) | S | operations/scale | L4628-4630 |
| 273 | R13 | Engine cells done 2026-09-27: Kafka users/ACLs/confirm/snapshot; ClickHouse server-side sums, delta, settings, users, snapshot; SQLite and DuckDB native bulk; Parquet; Redis verify of written keys; DynamoDB stream follow, fence, confirm, delta, settings, backup, bulk; SQL Server logins, confirm, FOR JSON hash; OpenSearch copy, settings, snapshot, delta; Cassandra roles, settings, TTL-keeping copy | DONE (in text 2026-09-27) | L | engines | L4638-4732 |
| 274 | R13-MSSQL | SQL Server bulk move (its own bulk load, staged and promoted in migkit's transaction; R2.4 staging) | IN PROGRESS a8d572b (SQL Server bulk; agent commit a6090e7, not merged) | M | engines | L620, L2580-2582, L2718-2720, L4191, L4236-4237; capabilities.py GAPS mssql |
| 275 | R13-CH | ClickHouse bulk as the target pulling from the source (`remote()`, dedup tokens) | IN PROGRESS aa5d0ed (engine gaps; WIP) | M | engines | L2790, L4656-4657, L5396, L5488-5489 |
| 276 | R13-CH.follow | ClickHouse follow, fence and confirm by changed partitions and empty queues | IN PROGRESS aa5d0ed | M | engines | L4657, L4735-4737; GAPS clickhouse |
| 277 | R13-OS | OpenSearch follow, fence and confirm by per-shard sequence numbers | IN PROGRESS aa5d0ed | M | engines | L4716-4717, L4742-4744, L5489 |
| 278 | R13-OS.users | OpenSearch security roles and users | IN PROGRESS aa5d0ed | S | engines | L4717, L5489 |
| 279 | R13-CASS | Cassandra/Scylla follow through Scylla's CDC tables, sentinel fence, delta | IN PROGRESS aa5d0ed | M | engines | L4732, L4745-4747, L5490 |
| 280 | R13-CASS.ttl | Deep check sampling rows with a TTL on both sides | IN PROGRESS aa5d0ed | S | engines | L4731-4732, L5490 |
| 281 | R13-ORA | Oracle as a whole side: sequences, settings, direct-path bulk staged and promoted, follow, fence, confirm, delta, users, snapshot, SHA-256 summed digest, type map (B1), run on Oracle Free | IN PROGRESS a47bffb (Oracle full side; WIP 5a682fb) | L | engines | L1841-1880, L2016, L2582, L2590, L2715-2718, L4623-4627; GAPS oracle |
| 282 | R13-DB2 | Db2 cells (10) and a run against a server | BLOCKED (IBM's image is x86-only) | L | engines | L3693-3702, L2720; GAPS db2 |
| 283 | R13-ASE | SAP ASE cells (11) and a run against a server | BLOCKED (SAP's image is x86-only) | L | engines | L3704-3714; GAPS ase |
| 284 | R13-HETERO | Users carried across engines | OPEN | M | engines | L5491; GAPS hetero users |
| 285 | R13-GENERIC | Generic engine cells (confirm, sequences, settings, bulk, table copy, follow, fence, delta, users, guard, statistics, snapshot) | OPEN | L | engines | L628-629; GAPS generic |
| 286 | R13.kafka | Kafka quotas and changed configs | OPEN | S | engines | L4748-4749 |
| 287 | R13.redis | A Redis 7.4+ RDB that Valkey refuses, said before a move | OPEN | S | engines | L4750-4751 |
| 288 | R13.sqlite | SQLite changes through the session extension (APSW) | OPEN (capabilities.py declares SQLite's follow not applicable) | S | engines | L4752-4753 |
| 289 | R13.parquet | Parquet on object storage: manifests and footers as the fence, versioning as the snapshot, only changed objects verified | OPEN (capabilities.py declares these not applicable) | M | engines | L4754-4755 |
| 290 | R14 | Measurements owed (mydumper, mongodump, proving a copied PostgreSQL range) | DONE (in text 2026-09-27) | S | tools used whole | L4759-4782 |
| 291 | R15 | DuckDB as an engine, with deep checks, sequences, snapshot and a bulk path of its own | DONE (in text 2026-09-27; grep: duckdb.py check_deep, _sequences, snapshot_state, native_bulk) | M | engines | L4658-4667, L4784-4809 |
| 292 | R15.2nd | DuckDB as a second reader for Parquet, PostgreSQL, MySQL | OPEN (grep: none) | S | verify | L4807-4808 |
| 293 | R16a | pgvector compared value for value | DONE (in text 2026-09-27) | S | types | L4823-4826 |
| 294 | R16a.more | MySQL 9 VECTOR and MongoDB float arrays | OPEN (grep: none; the vector canon class is with addeddc, F4.b) | S | types | L4823-4826; WAVE queue |
| 295 | R16b | Qdrant as an engine | OPEN (deferred until the rest is done) | M | engines | L4817-4819, L4828-4835 |
| 296 | R16c | Neo4j as an engine | OPEN (deferred until the rest is done) | M | engines | L4817-4819, L4836-4841 |
| 297 | R17a | doctor says whether each leg is encrypted and where migkit sits; TLS options reach every PostgreSQL and MySQL connection | DONE (in text 2026-09-27) | M | security | L4877-4889 |
| 298 | R17a.rest | TLS for the MySQL bulk programs, MongoDB's and Redis's connections | IN PROGRESS aa2c527 (TLS in its F0 outward task) | S | security | L4889-4890, L5494-5495 |
| 299 | R17c | Several SSH legs sized from the round trip | DONE (in text 2026-09-27) | M | move | L4904-4913 |
| 300 | R17d | The relay: reader beside the source, writer beside the target, an agent for Python-driver engines, framed zstd over TLS 1.3, encrypted spool (R19 lever 7) | OPEN (the PostgreSQL reader beside the source done 2026-09-27) | L | move | L4915-4952, L5108-5115, L5222-5224; WAVE queue |
| 301 | R18.1 | Same-engine pass-through: MongoDB raw BSON | DONE (in text 2026-09-27) | S | move | L5007-5015 |
| 302 | R18.2 | hiredis | NOT PURSUED (measured, not adopted) | S | engines | L5016-5023 |
| 303 | R18.zstd | zstd for spill files | OPEN | S | move | L5023-5024; WAVE queue |
| 304 | R18.4 | Out-of-core key diff | NOT PURSUED (looked at, not built) | S | verify | L5029-5032 |
| 305 | R18.5 | Chunk and change interleave property-tested | DONE (in text 2026-09-27) | S | change follow | L5033-5039 |
| 306 | R18.6/R20.2 | Arrow batches between readers and writers; the cross-engine path without per-row Python (ADBC, pyarrow compute) | OPEN (a SQL Server Arrow move is in a8d572b) | L | move | L5040-5042, L5199-5203 |
| 307 | R18.7 | confluent-kafka in place of kafka-python, lz4/zstd on produce | OPEN (grep: not used) | S | engines | L2572, L5043 |
| 308 | R18.fix | MySQL checksum by sum, not BIT_XOR; SQL Server by each row's FOR JSON SHA-256 | DONE (in text 2026-09-27) | S | verify | L4991-5004 |
| 309 | R19.1 | Physical rungs: pg_basebackup + fast-forward, CLONE, XtraBackup, MongoDB file snapshot, RDS/Aurora snapshot and clone via boto3 | IN PROGRESS ad4909f (physical rungs; paused, WIP d3b1361) | L | move | L2579, L2606-2610, L2788-2789, L2956-2957, L5057-5072 |
| 310 | R19.3 | Physical rung extended by page-level comparison | OPEN | M | move | L5080-5084 |
| 311 | R19.6 | LOAD DATA LOCAL made safe (pinned) | DONE 1da6415 | S | move | L2050-2051, L5100-5104 |
| 312 | R19.8 | Changes to ranges not yet copied left out (the watermark, lever 8) | DONE 6f07ceb | M | change follow | L5116-5155 |
| 313 | R19.9/R20.3 | Compiled change decoder handing Arrow batches; streaming replication instead of polling (R2.6) | OPEN | L | change follow | L2782-2784, L4241-4243, L5156-5161, L5204-5209 |
| 314 | R20.1 | Same-engine pass-through per range: binary COPY piped (PostgreSQL), LOAD DATA stream (MySQL), range digests in the servers (R19 lever 2's binary COPY) | IN PROGRESS a5244da (pass-through binary COPY; uncommitted pg_pass.py, my_pass.py) | M | move | L5073-5079, L5191-5198 |
| 315 | R20.4 | Session-level load settings (COPY FREEZE, synchronous_commit off, unique_checks, sql_log_bin where no replicas) | IN PROGRESS a5244da | S | move | L5210-5216 |
| 316 | R20.5 | Phases overlapped: indexes while the next table copies, a range verified while the next loads, the tail after the first table (R19 lever 4) | IN PROGRESS a5244da | M | move | L5085-5090, L5217-5219 |
| 317 | R20.6 | Compression level and legs chosen by measurement | OPEN | S | move | L5220-5224 |
| 318 | R20.7 | Files moved as parallel ranged parts, server-side copies, checksums while streaming | OPEN | M | move | L5225-5228 |
| 319 | R20.8 | The proof kept up as the data moves (incremental multiset hashing) | IN PROGRESS a31b892 | L | verify | L5231-5242 |
| 320 | R20.10 | The fastest hash the pair allows (native row hash, rendering pushed into SQL, parallel gather) | OPEN | M | verify | L5246-5251 |
| 321 | R20.11 | Only what changed since the last proof is read again | OPEN | M | verify | L5252-5254 |
| 322 | R20.12 | Both sides' digests of a range read at once | OPEN | S | verify | L5255-5257 |
| 323 | R20.13 | DuckDB as the compiled cross-engine mover | IN PROGRESS ae13c03 (DuckDB mover; uncommitted crossmove.py) | M | move | L5259-5266 |
| 324 | R20.14 | The source renders the target's load format in SQL, piped as bytes | IN PROGRESS ae13c03 (source-rendered COPY text pipe; uncommitted loadtext.py) | M | move | L5267-5273 |
| 325 | R21.1 | Two addresses are a hop | IN PROGRESS ab097a0 (uncommitted addresses.py, clouds.py) | M | ease of use | L5307-5313 |
| 326 | R21.2 | `init` as the guide; passwords in the keychain or env: | IN PROGRESS ab097a0 | M | ease of use | L5314-5322 |
| 327 | R21.3 | The DBA's least-privilege script written per engine and service | IN PROGRESS ab097a0 | M | ease of use | L5323-5326 |
| 328 | R21.4 | Nothing to tune: knobs only ceilings, the starter config shows none, doctor flags keys that do nothing | OPEN (not in ab097a0's task list) | S | ease of use | L5327-5331 |
| 329 | R21.5 | `goal:` sets the defaults | IN PROGRESS ab097a0 | S | ease of use | L5332-5336 |
| 330 | R21.6 | Config checked like code (did you mean, types, contradictions, plaintext secrets) | IN PROGRESS ab097a0 (option registry + did-you-mean; uncommitted hopkeys.py) | S | ease of use | L5337-5339 |
| 331 | R21.7 | Import an AWS DMS task, a DTS job, a Debezium config or a pgloader file | IN PROGRESS ab097a0 (DMS, Debezium, pgloader; DTS not in its list) | M | ease of use | L5340-5342 |
| 332 | R21.8 | A setup page in the view | OPEN | M | ease of use | L5343-5345 |
| 333 | R21.9 | Every message says what to do next | OPEN | M | ease of use | L5346-5347 |
| 334 | R21.M | Ease measured: time to a verified move scripted per pair, config lines, questions asked | OPEN | S | ease of use | L5349-5353 |
| 335 | DTS.ops | Operation filters (skip inserts, updates or deletes) | OPEN | S | change follow | L5498 |
| 336 | DTS.sample | Sampled check | OPEN (grep: only Redis samples) | S | verify | L5498 |
| 337 | DTS.gtid | GTID-set compare | OPEN (grep: none) | S | verify | L5498-5499 |
| 338 | DTS.csfilter | Change-stream filter | OPEN | S | change follow | L5499 |
| 339 | REL | Release: memory and swap checked, the full suite once, fixes, secrets check, one commit, push | OPEN | S | operations/scale | L5424-5441, L5505-5508 |
| 340 | R22.1 | One resource model per task: source read, target write, the migkit host, the link each way, the path's cost per row; probed before the move and kept current | IN PROGRESS a90aadb (resource model + bottleneck + per-resource controllers + ETA) | L | operations/scale | L5355-5379; WAVE a90aadb |
| 341 | R22.2 | The binding resource named, workers and connections per side sized to it (roofline, Little's law), said in words | IN PROGRESS a90aadb | M | operations/scale | L5380-5386 |
| 342 | R22.3 | One controller per resource while it runs (source read pace, target write pace, link legs) | IN PROGRESS a90aadb | M | operations/scale | L5387-5392 |
| 343 | R22.4 | Hops removed: a server pulling straight from the other (postgres_fdw, a temporary subscription, CLONE, `remote()`), credentials removed after; where migkit should run and why | IN PROGRESS a939c23 (hops removed + placement advice) | M | move | L5393-5399 |
| 344 | R22.5 | ETA per phase with its reasons: p50/p90 ranges from measured rates, binding resource per phase, live EWMA, "never catches up" said | IN PROGRESS a90aadb | M | operations/scale | L5400-5406 |
| 345 | R22.6 | The machine to run on: instance type for migkit and its agents, target class from the source's history, a shipped catalog refreshed from the provider | IN PROGRESS a939c23 (instance catalog/advisor + target class) | M | operations/scale | L5407-5422 |
