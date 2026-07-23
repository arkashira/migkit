# Changelog

All notable changes to migkit. Dates are the working session, not release tags.

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
