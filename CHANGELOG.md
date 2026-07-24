# Changelog

All notable changes to migkit. Dates are the working session, not release tags.

## [0.3.2] — 2026-07-24

Zero footprint on the destination.

### Changed
- Removed the in-database audit ledger. migkit no longer creates
  `public.migkit_changelog` (pg) / `<db>.migkit_changelog` (mysql) on the
  target — `record_ledger`/`read_ledger` are gone. The audit is now
  local-only (per-hop `changelog.jsonl` + state journals), which was already
  written alongside it. **Why:** a verification tool must not contaminate the
  target; the ledger table made the target differ from the source and tripped
  migkit's own schema check (surfaced by a real cart_uat run where the only
  schema diff was migkit's own table, flagged by migra/liquibase/pk-inventory
  because their exclusion of it was incomplete).
- `history` reads the local changelog and state journals only.
- The `*.migkit_changelog` exclusion filters stay in place so a table left by
  an older version doesn't show as a diff; drop leftovers with
  `drop table public.migkit_changelog` (pg) on the target.

## [0.3.1] — 2026-07-24

Read-replica endpoint guardrail (from a live UAT outage: an app pointed at
an Aurora reader instance got `SQLSTATE 25006 cannot execute SELECT FOR
UPDATE in a read-only transaction`).

### Added
- `_in_recovery()` detection (postgres): every endpoint's role is now known.
- `assess` flags endpoint role — source on a read replica = warn (fine for
  checks, but the LSN fence and logical slots need the primary), target on a
  read replica = **fail** (cannot migrate/repair into it, and apps get
  SQLSTATE 25006). This check would have caught the outage before cutover.
- `doctor` prints `writer` / `READER/read-only` next to each pg endpoint.

### Fixed
- `--consistent`, delta verify and the LSN fence no longer crash when the
  source is a standby (Aurora readers reject both `pg_current_wal_lsn()` and
  `pg_last_wal_replay_lsn()`): `src_lsn()` returns `None` → fence degrades to
  settle, `_fast_consistent` marks the snapshot `standby (no fence)`, and
  `delta_verify` returns a clear "point at the writer endpoint" error instead
  of an exception.

## [0.3.0] — 2026-07-24

Consistency by design, O(changes) verification, and best-tool movers.

### Added — verification innovations
- `check --consistent` (postgres): every table of a database checksummed
  inside ONE repeatable-read read-only transaction per side — zero intra-db
  skew — with the source LSN captured in-snapshot as the fence.
- **LSN-fenced convergence** replaces the sleep-settle heuristic wherever a
  replication slot is visible (our subscriptions *and* opaque managed movers
  keep their slot on the source): suspect rows are re-compared only after
  every consumer confirmed flushing past the captured LSN; two fenced rounds
  ride out hot rows; survivors are real diffs, proven, not guessed.
  Falls back to the old `settle` behaviour when no slot is visible.
- **Delta verify** — `watch --verify --delta`: a dedicated logical slot
  (pg, test_decoding), saved binlog position (mysql) or change-stream token
  (mongo) records what changed; each cycle re-verifies only those pks/ids on
  both sides. Cursor advances only after a clean verify → crashes and diffs
  replay the same window (idempotent); diffs write the same pk files
  `sync --apply` repairs. `--teardown` drops the slot/state.
- **Column fingerprint** — on any table diff, one scan with one aggregate
  per column reports exactly which columns drift (pg + mysql), evidence in
  `data-<table>.columns`, before any row-level work.
- **Render audit** (`check --deep`, pg): samples exotic-typed columns
  (enums, domains, ranges, money, tsvector, xml, interval, ...) and compares
  their actual text rendering by pk — surfacing the cross-version rendering
  lies that hide inside checksums. `options.checksum: jsonb` switches the
  fast path to canonical `to_jsonb` hashing (ISO timestamps regardless of
  DateStyle).

### Changed — movers drive the best tool (auto, no flags needed)
- `move --via auto` (default) picks the fastest installed mover per engine:
  parallel `pg_dump -Fd -j`/`pg_restore -j` (postgres), mydumper/myloader
  (mysql), pgloader data-only load file (mysql→pg hetero),
  `mongodump | mongorestore` (mongo). Builtin chunked copy remains the
  fallback and the only per-chunk-resumable mode. Version-mismatch
  pg_restore SET noise is tolerated — `migkit check` is the judge.
- `move --mode cdc --via debezium` generates ready-to-run Kafka Connect
  configs (redpanda + debezium/connect compose, source connector for
  pg/mysql, JDBC sink with upsert+delete), chmod 700, with a README of the
  exact curls. Platform CDC without reimplementing it.
- mysql row repair executes pt-table-sync-generated statements when the
  tool is present (bounded by `--where` to the verified pks so the undo
  stays complete and exactly restorable); builtin delete+copy fallback.

### Changed — engines that were shallow are no longer
- mssql: counts merged into the checksum pass; row-level drilldown via
  canonical `FOR JSON` + SHA2_256 hashing writes the same pk evidence files;
  deep checks for disabled/untrusted (`WITH NOCHECK`) FKs and triggers,
  column drift, and max-pk boundary.
- kafka: critical topic-config parity (cleanup.policy, retention, ...) in
  schema; deep check for consumer-group presence and lag parity — the
  offsets-not-translated failure that breaks every kafka cutover.
- redis: pipelined type-aware compare (two round trips per 1000 keys
  instead of one per key); deep checks for TTL drift/loss and biggest-key
  presence on target.

### Tests
- 49 tests (was 38): + test_decoding parser, hash-expr option, mover
  selection matrix, Debezium codegen, pt-table-sync guardrails, deep/delta
  availability, and two docker E2Es: the full delta-verify loop and the
  consistent-snapshot + fingerprint pass.

## [0.2.0] — 2026-07-24

### Changed — CLI consolidated to 11 commands
- `doctor` (absorbs `hops`), `advise`, `assess`, `schema` (absorbs
  `setup-target`, `convert-schema` → `--convert`, `gen-migration` →
  `--migration`), `check` (absorbs `sample-diff` → `--drill`), `move`
  (absorbs `replicate` + `tail` → `--mode full|cdc|full+cdc`), `watch`
  (absorbs `monitor` → `--verify`), `sync` (absorbs `repair`: dry-run plan →
  `--apply` → `--go`), `report` (absorbs `ui` → `--serve`), `history`
  (absorbs `state`), `rollback`.
- All 11 pre-0.2 names still work as hidden aliases with their old flags,
  verified by the CLI-surface tests — existing scripts keep running.
- Fix hints and playbooks now print the new spellings
  (`migkit sync … --kind sequences --apply`).

### Changed — performance
- Counts merged into the checksum pass: when `check` runs counts + data
  together (postgres, mysql, mongodb, hetero), row counts come from the
  same query/aggregate as the checksum, so every table is scanned once,
  not twice. Table presence is compared from the catalogs (no scan).
- MySQL data check now uses the built-in single-aggregate
  `bit_xor(crc32/md5)` checksum first (one query per side per table);
  reladiff moved behind `options.reladiff: true`.
- MySQL standalone counts run in parallel across tables and sides.
- MongoDB counts are implied by equal `dbHash`/per-id hashes; only diffed
  collections pay for an exact `count_documents`.
- Global `-q/--quiet` silences per-table chatter, progress lines and
  pass-level assess output; diffs, errors and summaries always print.

### Added — deep checks (`check --deep` or `--only deep`)
- FK integrity: orphan scan behind NOT VALID constraints (postgres) and a
  full FK orphan scan (mysql, where loads run with `foreign_key_checks=0`).
- Disabled triggers on target (postgres).
- Column-level drift: type / nullability / default / precision, plus
  charset + collation per column on mysql; honours `<hop>.schema-ignore`
  patterns; evidence written to `deep-columns.diff`.
- Materialized-view freshness (populated + row counts) and table-grant
  parity for roles present on both sides (postgres).
- Boundary freshness: `max(pk)` (sql) / newest `_id` (mongo) both sides —
  flags targets AHEAD of source (rogue writer / double-apply) and reports
  lag-behind tables. Directly aimed at the "dst has more rows than src"
  investigation.

### Changed — dashboard
- `report --serve` (ex-`ui`) redesigned: summary pills, engine/service
  badges, per-check tiles with pass/fail tinting, per-db status rows,
  cross-hop recent-writes feed, refined dark mode. Still read-only,
  localhost-bound, zero dependencies.
- HTML report gains a `deep` column and drops tiles for checks that
  did not run.

### Tests
- 34 tests (was 27): + CLI surface (exactly 11 visible commands, all legacy
  aliases invocable), quiet flag, counts-from-checksum parsing, deep-check
  default, `--drill` argument validation.

## [0.1.0] — 2026-07-23

First working version, built and verified against live RDS → TencentDB hops
(6 UAT databases, 1.06B rows) plus docker labs for every engine.

### Added — validation
- Layered `check`: schema, object inventory (per type), row counts,
  auto-increment/sequence values, full row-data checksums with per-PK drilldown.
- Uniform evidence format across all engines — every `OK` prints both sides'
  counts and hashes.
- Fast Postgres data path: commutative sum-of-md5 as a parallel aggregate
  (488M rows in ~10 min); large tables drill down by PK-index slices.
- `assess` — premigration readiness (wal_level/binlog prereqs, no-PK tables,
  unlogged/invalid objects, encoding+collation, extension and account parity).
- `monitor` — continuous re-check loop; `settle` option distinguishes in-flight
  replication from real diffs (confirm-out-of-sync).
- `sample-diff` — column-level report on a row sample via datacompy.

### Added — engines
- Native: postgres, mysql, mssql, mongodb, sqlite, redis, kafka.
- Aliases: mariadb, percona, tdsql, aurora-*, alloydb, documentdb,
  cosmosdb-mongo, azure-sql.
- `generic` engine over reladiff (snowflake, bigquery, redshift, clickhouse,
  oracle, trino, duckdb, …); schema via liquibase JDBC for db2/h2/etc.
- `hetero` engine — cross-engine MySQL → PostgreSQL, verified end to end.
- Managed-cloud handling: DocumentDB client-side BSON hashing, TencentDB
  unlogged rules, multi-host replica sets, TLS CA files.

### Added — movers (self-hosted, crash-resumable)
- `move` — chunked full load with a checkpoint file.
- `replicate` — native Postgres logical replication and MySQL binlog
  replication (incl. RDS `rds_set_external_source` variant).
- `tail` — MongoDB change streams and cross-engine MySQL binlog CDC, both with
  a persisted resume token.
- `convert-schema` — cross-engine DDL transpile via sqlglot/pgloader.

### Added — repair, state, safety
- `repair` / `sync` — align sequences to source value, delete-and-recopy
  differing rows by PK; dry-run by default, saves undo, idempotent on re-run.
- `state` / `rollback` — tagged snapshots in two locations, plan-preview
  rollback, per-hop changelog, in-database `migkit_changelog` audit ledger.
- Lock file to prevent concurrent writes; configured-guard on every command.

### Added — tooling for versioning and output
- `gen-migration` — Flyway-style `V__/U__` versioned SQL files from the diff.
- `advise` playbooks for aws-dms, tencent-dts, gcp-dms, native, incl. gh-ost /
  pt-online-schema-change guidance.
- `report` HTML report and `ui` live web dashboard.

### Added — quality
- `assess` now compares account credentials: role/user password hashes between
  source and target, catching the DTS failure mode where the account name is
  carried over but the password is not (apps then cannot log in).
- Test suite: 27 tests (unit, docker integration, Faker realistic data, and
  failure modes) plus a GitHub Actions CI running the no-docker subset on every
  push and the full suite on Ubuntu runners.

### Composed tools
migra, liquibase, atlas, reladiff, pt-table-sync, pgloader, sqlglot, datacompy,
python-mysql-replication — used directly, each with a graceful fallback.
