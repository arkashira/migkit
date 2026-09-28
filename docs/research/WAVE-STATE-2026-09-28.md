# migkit waves — state (updated 2026-09-28, after the owner approved everything)
Rules for every agent: /tmp/migkit-rules.txt (also inline in prompts). Concurrency cap: 20 subagents.
Merge rule (orchestrator): agent reports done -> in main checkout `git merge --squash worktree-agent-<id>` (resolve by ownership), run ONLY that task's tests + tests/test_the_report_does_not_name_its_tools.py (docker via tools/with_docker_lock.py [--vm migkit]), commit ONE squashed commit, one-line message, no trailers; then `git worktree remove --force .claude/worktrees/agent-<id>` + `git branch -D worktree-agent-<id>`. Broken tests -> fix narrowly or send back to the agent (SendMessage to its id). Never full suite (release only). Never push unless the owner says.

## Running (resumed wave 1, worktrees merged with master df60027)
a84f1b9 decision engine core (decide.py; route pick/plan/_verify_way/loops_prevented)
a4d532b two-way mark rungs (marks.py; PG origin/message/table, MySQL tagged GTID, MariaDB skip_replication, comment)
a21bbd1 tail watermark lever 8 (position-per-range rule, no watermark table)
a5bea35 write paths (MySQL staged upsert via pinned LOAD DATA LOCAL, psycopg3 pipeline + binary COPY)
ad4afda 0f pgcopydb + Debezium (+ migkit/tooluse.py registry)
aa5d0ed engine gaps (ClickHouse remote(), OpenSearch follow/users, Cassandra TTL/WRITETIME, Scylla CDC)
aae6d11 deep checks (partition coverage, drift.transient, shared narrowing/timeshift, slot-before-snapshot proof)
a075608 0f verifiers (DVT in .venv-dvt, reladiff rung, datacompy naming)
## Running (wave 2, from df60027)
a5ff11d F0 verification false negatives (Mongo $toHashedIndexKey, MySQL digest 64-bit+salt, SQL Server CT snapshot/rowversion/CLR)
af1a0fe F0 types (G1-G4, G8 collisions, G6-G14, refusals G25-G29, then G15-G24 + new canon classes)
a2ba2fe F0 cutover bugs + F2 cutover path (fence insert LSN, own-slot fence, rollback source sequences, cutover: hop option)
ae866ee F0 prove_converted + sqlglot fixes + R11 pipeline
aa2c527 F0 outward (pt --no-version-check, Atlas Community, Liquibase 4.x, leftovers, mongosync canCommit, Redis ABSTTL/cluster, md5 FIPS, UI token, publication, TLS)
a3081c8 F6 speed (MySQL decoder child process, PG persistent tail connection + newline bug, renderer per column)
a8d572b SQL Server bulk (mssql-python staged) + .github/workflows/x86-engines.yml
a47bffb Oracle full side (python-oracledb, direct path staged, SHA-256 sum digest, LogMiner reader)
ae351bc MySQL tools whole (mydumper --rows/--checksum-all, MySQL Shell rung)
ad4909f R19 lever 1 physical rungs (pg_basebackup + fast-forward, CLONE, RDS/Aurora via boto3/moto)
Research done: leapfrog conversion, leapfrog engine reach (both folded into backlog). Research done: leapfrog live tail + raw logs (folded)
Running (wave 3): a7120bc warehouse exact loads + SQL digest (ports 16150-16159); a8c4cd1 copybook engine + IBM i journal (16160-16169)
## Queued (launch when a slot frees; research first)
- (launched) research leapfrog live tail + raw logs -> ad9432de
- Fold the 3 leapfrog reports into backlog before building their items.
- W1 HA + scale-out of the mover (standby takeover, one table across machines with conditional-write checkpoint)
- W2 estate: discovery, target sizing from perf history, cost, fleet, REST API, Terraform provider, K8s operator, OIDC/SAML/SCIM
- W4 transforms in flight + FPE/tokenization masking + R5 HMAC masking, view actions, signed audit anchoring
- W6 commercial types (after types agent + Oracle/SQL Server agents)
- W7 resharding (split/merge, distribution-aware load)
- W8 offline signed bundle, container image, Windows
- W9 compiled apply side (after F6 measurements)
- F1 verification passes A-D (one-scan leaf tree, key-hash buckets, in-SQL IBLT, self-check, generations) — after decision engine + F0 verification merge
- F7 lists per report (RedisShake rung, MM2 + source-offset header + group translation, DSBulk, OpenSearch RFS, DynamoDB export/import, changing a running tail's table set, Kafka offset clamp at cutover, self-stopping tail, types sized from data, SingleStore alias, source-commit timestamp in beat, batches end at COMMIT, changed-columns merge, statistics-based chunk edges, exact batches default one-way, schema-as-of-position decode, warehouse exact loads (Storage Write API, Snowpipe channels), 16 security fixes remaining)
- R2.5-6, R3 many-node, R10 LOB pieces MySQL, R12 Db2/ASE (x86 CI), R13 cells, R16a vectors, R17d writer beside target, R18 zstd spill/Arrow/Kafka client, DTS gaps, problems Partly, planner speed rules, docs refresh (threat-model stale at-rest)
- P10 cross-engine workload replay (pt-upgrade style on workload.py) — from leapfrog-conversion
- Engine reach (after warehouses): copybook engine + IBM i journal follow (ports 16160+), Teradata, change-topic reader (TiDB/Couchbase/DSQL/CockroachDB), Cosmos NoSQL, SAP OData+pyodata, SaaS via dlt/Singer (AGPL ok)/Airbyte ELv2 as programs
- LIVE TAIL (after watermark a21bbd1 + F6 a3081c8 + two-way a4d532b merge — they own tail_apply/_ReadAhead/token): L1-L8 table set + DDL in a running tail, L7 per-table accounting; R7 MySQL MINIMAL/NOBLOB/PARTIAL_JSON (small, mysql.py neutral_changes); R2 SQL Server log-backup chain on migkit-owned server; R9 PG wal_level=replica via pg_walinspect; R4/R5 Oracle flashback full images (after Oracle agent); C1 mongosync rebuild; C2/C3 Kafka exact offsets; C4 Redis two-site; C5 LogMiner mining instance; C6 lossless Redis live; P1-P3

## STOPPED by the owner 2026-09-28 (evening) — everything paused
- Cron 746d35c4 deleted. All 20 implementation agents stopped (10 by the session limit, 10 by the owner right after resuming).
- Work is in each worktree .claude/worktrees/agent-<id> (committed as WIP where the secret-check hook allowed; a8d572b, aa2c527, ad4909f, a4d532b, a5bea35 have staged changes blocked by check_no_secrets — fix the flagged test values before committing).
- Resume = SendMessage to the same agent id ("RESUME ... continue where you were ... commit, merge master, report"), at most ~10 at a time to avoid burning the session limit; then merge per the rule above.

## RESUMED 2026-09-28 (owner: "เอาเลย") — batch A running (10)
a5ff11d F0 verify, af1a0fe F0 types, a2ba2fe cutover, ae866ee stored code, aa2c527 outward, a21bbd1 watermark, a84f1b9 decision engine, aae6d11 deep checks, a4d532b two-way rungs, a5bea35 write paths
## Batch B — resume (SendMessage to the same id, "RESUME ... continue where you were ... fix secret-check flags, commit, merge master, report") when batch A agents finish, keeping at most 10 running:
a3081c8 F6 speed (was: reverting test additions for warm), a8d572b SQL Server bulk + x86 CI (staged, secret flag), a47bffb Oracle (was: snapshot/flashback test), ae351bc MySQL tools (was: straight copy summary line), ad4909f physical rungs (staged, 4 secret flags; was: a unit test), aa5d0ed engine gaps (was: Cassandra cell-times test), ad4afda 0f pgcopydb/Debezium (was: single docker job under one lock), a075608 0f DVT (was: PostgreSQL docker test), a7120bc warehouse (was: DuckDB digest test), a8c4cd1 copybook + IBM i (was: canon types and capabilities)
Then the queue above (live tail L/R/C/P items after tail_apply owners merge; engine reach 3-7; W1/W2/W4/W6-W9; F1; F7; R items).

## MERGED
- 2a4c652 F0 verification (a5ff11d): Mongo bytes, MySQL salted 64-bit md5, SQL Server CT/rowversion/CLR — agent-tested on identical tree; post-merge docker check running (bbw0aqqai)
- 6f07ceb watermark lever 8 (a21bbd1): per-range snapshot/GTID marks, xid-visibility skip, per-table accounting — property tests pass on merged tree; docker check running
- Batch B resumed so far: a3081c8 F6 speed, a8d572b SQL Server bulk + CI
- 32ed544 decision engine (a84f1b9): migkit/decide.py rungs/climb/strategies/choose_kept + coverage() 10 shapes x 23 engines; pick/fitted/plan/_verify_way/loops_prevented routed; left: ranking movers by measured rates (waits on benchmark item 28), doctor naming rungs. NOTE for all agents: expose new choices as decide.py rungs now.
- Docker context incident: `colima stop -p migkit` switched the machine's docker context to `default` (no daemon) - broke other sessions; fixed by `docker context use colima` and the lock now restores it (e4b5275, 53bf947).
- Batch B resumed: a3081c8 F6, a8d572b SQL Server, a47bffb Oracle
- f3884fa F6 speed (a3081c8): reader chosen by measured rate (thread vs decoder process: ~57k -> ~66k changes/s; decoder now the ceiling -> compiled decoder next), PG tail on one kept connection 16.1s -> 7.3s + newline stall bug fixed, apply trims + GC held (apply 5.1s -> 2.7-3.3s), per-column renderer (0.58 -> 0.39s). Made the watermark teeth check deterministic (was flaky ~1/6).
- Batch B resumed: + ae351bc MySQL tools

## TOP OF QUEUE (owner 2026-09-29: brutal throughput, verify near-free) - backlog R20
- R20.8+9 incremental proof kept by the copy and the tail (LtHash-style sums per leaf, maintained from before/after images) + one-scan leaf tree for any key (F1) + in-SQL IBLT  [after watermark/F6 merged: yes; touches tail apply + verify - own them]
- R20.1 pass-through binary COPY per range (PG->PG), LOAD DATA stream (MySQL->MySQL), server-side range digests
- R20.10/11/12 fastest hash per pair, only-what-changed, both sides parallel
- R20.4/5 session-level load settings (FREEZE, synchronous_commit off, unique_checks), phases overlapped
- R20.2/3 Arrow cross-engine path; compiled decoder + streaming replication
- R20.6/7 compression/legs by measurement; parallel ranged file moves
