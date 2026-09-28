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
Research: ac2ac5c leapfrog conversion; a3888957 leapfrog engine reach
## Queued (launch when a slot frees; research first)
- RESEARCH leapfrog live tail + raw logs -> docs/research/leapfrog-live-tail-and-raw-logs-2026-09-28.md (prompt: table-set change + DDL in a running tail; reading with/without CDC features: fn_dblog, LogMiner without supplemental, db2ReadLog, physical WAL; rebuilding closed capabilities: mongosync, Kafka offsets, Redis A-A, XStream, RIOT-X; imitating cloud control planes)
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
