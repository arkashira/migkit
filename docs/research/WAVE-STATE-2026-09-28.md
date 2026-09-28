# migkit implementation wave 1 — launched 2026-09-28 (agents in git worktrees, branch from 0b64ddb)
Merge rule: when an agent reports done -> `git worktree list` -> in main checkout: `git merge --squash <branch>` (or cherry-free: `git diff 0b64ddb..<branch> | git apply -3`), resolve conflicts by ownership below, run ONLY that task's tests + test_the_report_does_not_name_its_tools.py, then `git worktree remove` + `git branch -D`. One squashed commit per task, one-line message, no trailers. Push only when the owner says.
Ownership (do not let two tasks edit the same function):
1 decide.py + movers.pick/chosen/fitted + planner.plan + postgres._verify_way + mysql.loops_prevented (agent "decision engine core")
2 twoway.py + pgslot.py origin filter + postgres/mysql origin_mark/origin_seen + tagged GTID/skip_replication readers + doctor two-way lines + hetero token 'rung' field (agent "two-way mark rungs")
3 hetero.tail_apply watermark skip + checkpoint/ranges read paths (agent "tail watermark lever")
4 postgres/mysql _apply_upserts/_apply_added/_stage/_copy_text/neutral_write, LOAD DATA LOCAL safe handler, psycopg3 pipeline/binary COPY (agent "write-path levers 6 and 10")
5 movers.py pgcopydb argv/progress/follow + Debezium config gen + tools-used table (agent "0f pgcopydb and Debezium")
6 clickhouse.py remote() bulk + partition follow, opensearch.py follow/users, cassandra.py TTL deep + Scylla CDC, users.py sections, capabilities cells (agent "engine gaps")
7 partition coverage pg+mysql, drift.py transient names, MySQL tz-shift/narrowing proof, slot-before-snapshot property test (agent "deep checks from memory audit")
8 DVT/reladiff/datacompy whole-use + tools-used table for verifiers (agent "0f DVT")
Research agents (8) write to docs/research/*-2026-09-28.md themselves; fold into backlog when present.
Next waves (not started): SQL Server bulk/staging (bcp; check how existing mssql tests get a server), R5 view actions + signed audit anchor + HMAC masking, R10 MySQL/Oracle LOB pieces, R11 stored code, R12 engines, R16a vectors, R17d writer beside target, R18 zstd spill/Arrow/Kafka client, R19 levers 1,2(binary COPY),4,7,9, DTS gaps, problems "Partly", planner speed rules, 0f for the remaining ~20 tools, stale docs.

## Interrupted 2026-09-28 (owner: 5-hour token window nearly full)
- Cron 4d2c58af deleted. All 8 implementation agents told to commit WIP on their branch (+ WIP.md at worktree root) and stop; 7 research agents told to write partial reports and stop.
- NOTE: the agent worktrees were created from 7de74bc (not 0b64ddb): they lack tools/with_docker_lock.py and the docs commit. Merge = `git merge --squash worktree-agent-<id>` onto master (0b64ddb+); expect only doc-side differences.
- Worktrees: /Users/ashira/develop/devops/devops-tools/migkit/.claude/worktrees/agent-<id> (branches worktree-agent-<id>); mapping id->task: a84f1b9=decision engine, a4d532b=two-way rungs, a21bbd1=tail watermark, a5bea35=write paths, ad4afda=0f pgcopydb+Debezium, aa5d0ed=engine gaps, aae6d11=deep checks, a075608=0f DVT.
- Research done: mechanisms-aws-azure-google-snowflake, security-throughput-scorecard, mechanisms-cdc-elt-specialists (check completeness marker at top); others partial or missing.
- Resume: read each worktree's WIP.md, resume the agent by SendMessage to the same agent id (or launch a fresh agent with the same task prompt + "continue from WIP.md"), merge finished ones, then next waves.
