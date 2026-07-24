# Changelog

All notable changes to migkit. Dates are the working session, not release tags.

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
