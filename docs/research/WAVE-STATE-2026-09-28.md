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
- Running (R20, launched 2026-09-29): a31b892 proof kept up + one-scan tree + IBLT (16170-16179); a5244da pass-through binary COPY + safe session settings + overlap (16180-16189)
- Running (R20): ae13c03 cross-engine without Python rows: DuckDB mover + source-rendered COPY text pipe (16190-16199)
- Running (R21): ab097a0 two-address hops, init as a guide, keychain, DBA scripts, goal:, option registry + did-you-mean, import DMS/Debezium/pgloader configs (16200-16209)
- Pending commits in main checkout: deep-checks squash (staged, waiting docker biwfsu0oc), then docs/backlog.md (R20 compete rule + R21) and docs/index.html (no internal tool names) as separate commits
- 659b54f deep checks (aae6d11): partition coverage both engines, transient tables everywhere, narrowing/shift in one place (+numeric(12,2)->(12,4) caught), PG slot with EXPORT_SNAPSHOT + visibility wait, MySQL copy_point waits commits in flight (real hole measured on 8.4). Conflict with F6 in postgres.py resolved (both kept).
- eb70ff6 docs: compete rule, R21, index.html no internal tool names
- 3b05234 types (af1a0fe): G1-G4, G8 collation merges refused, preflight refusals canon.unfit, G15-G24 + XML/UUID/inet classes; not done: G5 Mongo types round trip, interval/vector/geometry classes, empty Cassandra collections, Oracle paths not run
- post-merge docker check of types+deep+F6 running (bd4pknqya); deep-checks own docker check (biwfsu0oc) still queued
- Batch B resumed: + aa5d0ed engine gaps
- Running (R22): a90aadb resource model + bottleneck + per-resource controllers + ETA per phase (16220-16229); a939c23 hops removed (postgres_fdw pull, temp subscription, placement advice) + instance catalog/advisor + target class (16230-16239)
- Running: addeddc types remainder (G5 Mongo, interval/vector/geometry, Cassandra empty collections, CH Array, nine digits) (16210-16219); ae854af backlog-index.md
- 395c8e5 two-way rungs (a4d532b): marks.py ladder (PG origin/message/table, MySQL tagged GTID (own decoder)/comment/table, MariaDB skip flag/table), probe proof caught a real decoding bug, rung in token + next batch, doctor + teardown; costs measured (PG origin ~1ms/conn -> table outranks origin for counter hops; MySQL rungs in noise -> tagged GTID by footprint). Not done: MariaDB GTID-domain rung, many-node. Conflict in postgres.py (SESSION pin vs _first_line) resolved keep both. Docker check bdv1qvq3k.
- Batch B resumed: + ad4afda 0f pgcopydb/Debezium (told: align Debezium modes to canon, rungs vs pass-through/tail)

## PAUSING 2026-09-29 (owner: tokens low — let this round finish, then pause; cron stopped)
- Cron a1c802b6 deleted. No new agents after a6bfe7c (R23.3 target interlock, 16240-16249).
- Agents still running this round (merge when the owner resumes): ae866ee stored code, a2ba2fe cutover, aa2c527 outward, a5bea35 write paths, a8d572b SQL Server + x86 CI, a47bffb Oracle, ae351bc MySQL tools, ad4909f physical, aa5d0ed engine gaps, ad4afda 0f pgcopydb/Debezium, a31b892 R20 proof, a5244da R20 pass-through, ae13c03 R20 cross-engine, ab097a0 R21 UX, addeddc types remainder, a90aadb R22 model+ETA+memory, a939c23 R22 hops+instances, a6bfe7c R23.3 interlock.
- Not resumed yet: a075608 0f DVT, a7120bc warehouse, a8c4cd1 copybook + IBM i.
- On resume: merge each finished branch (squash, its tests + name guard, docker via lock), resume the three above, then the queue (R20 rest, R22 rest, R23 1-2 and 4-9, live tail, engine reach, W-items, F7, 0f remaining tools).
- Merged so far on master (not pushed): 2a4c652, 6f07ceb, 32ed544, f3884fa, 659b54f, 3b05234, 395c8e5 (+ docs commits).
- DONE (not merged, paused): aa2c527 outward F0 -> branch worktree-agent-aa2c527df35cffc64 head a503c25 (c580c69 work). Found+fixed: PG table copier ignored exclude and overwrote target-owned tables. TODO on merge: its pending PG docker run (/tmp/f0_docker_batch5.log); merge drift.transient + leftovers.bookkeeping into ONE list; mongosync docker test not run; SQL Server TLS can't verify via pymssql.

## STOPPED ALL 2026-09-29 (owner: token low) — resume 8-10 at a time next round
- All 17 running agents stopped; queued docker jobs killed; test containers removed; colima `migkit` profile stopped; context = colima.
- WIP kept: committed on each branch, except a939c23, ab097a0, addeddc, ae13c03 = staged (secret-check blocked; fix the flagged test values before committing). aa2c527 DONE (merge first). a2ba2fe/ae351bc/ae866ee clean (already committed).
- Resume order (8-10 at a time): merge aa2c527 -> resume cutover a2ba2fe, stored code ae866ee, R20 proof a31b892, R20 pass-through a5244da, interlock a6bfe7c, write paths a5bea35, types rest addeddc, UX ab097a0 -> then Oracle a47bffb, SQL Server a8d572b, MySQL tools ae351bc, physical ad4909f, engine gaps aa5d0ed, 0f pgcopydb ad4afda, R20 cross-engine ae13c03, R22 a90aadb/a939c23 -> then not-yet-resumed a075608 DVT, a7120bc warehouse, a8c4cd1 copybook.
- Two-way merge 395c8e5 docker re-check (bdv1qvq3k) was killed before running: re-run on resume.
