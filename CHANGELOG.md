# Changelog

The notable changes, grouped by area. This project is pre-1.0; releases are
cut from `master` and versions are tagged as features stabilize.

## Unreleased

### Verification
- Layered check: schema, object inventory, row counts, sequence/identity
  values, and full row-data checksums with per-primary-key drilldown. Every
  pass reports both sides' counts and hashes.
- `check --consistent`: whole-database checksum inside one repeatable-read
  transaction per side, with the source LSN captured as a fence.
- LSN-fenced convergence: suspect rows are re-compared only after every
  replication consumer (including opaque managed movers) confirms flushing
  past the captured LSN. Falls back to a settle delay when no slot is visible.
- Delta verify (`watch --verify --delta`, `sync --mode stream`): re-verify
  only the rows changed since the last verified point, on every engine —
  logical slot (postgres), binlog position (mysql), change-stream token
  (mongo), offset baseline (kafka), Change Tracking (mssql). The cursor
  advances only on a clean cycle, so it is idempotent under crashes.
- Column fingerprint and, for postgres, a render audit of exotic-typed
  columns, so a diff is localized before any row-level work.
- Deep checks: FK orphans, disabled/untrusted constraints and triggers,
  column drift, materialized-view freshness, grant parity, and a max-PK
  boundary check that catches writers landing on the target.

### Repair
- Reconcile the target to the source with a saved undo for every change:
  sequence/identity values, differing rows (delete-and-recopy by primary
  key), and schema objects (atlas-generated DDL applied in one transaction).
- `sync --on-conflict source-wins | keep-target`.
- The mysql row path drives pt-table-sync when present, bounded to the
  verified keys; a built-in path is the fallback.

### Movers and CDC
- `move --via auto` drives the fastest installed tool: parallel
  pg_dump/pg_restore, mydumper/myloader, pgloader, or mongodump/mongorestore,
  with a resumable built-in copy as the fallback.
- `move --mode cdc --via debezium --go` generates the Connect configuration,
  launches the stack, registers the connectors, and reports their status;
  native logical replication / binlog / change streams remain available.

### Orchestration and operation
- `sync --mode verify | seed | stream | migrate` runs the whole flow with a
  verification step wrapped around each stage; `--serve` runs it as a service.
- Pluggable state backend (local or s3) behind `sync --go`, `rollback`, and
  `history`, with tagged restore points.
- Prometheus metrics (`report --metrics` and a `/metrics` endpoint) for
  alerting; a read-only web dashboard.
- Connection retry with backoff on transient failures; a read-replica
  guardrail (`assess`/`doctor` flag read-only endpoints).
- Credentials resolve from `env:`, `file:`, or `vault:` references instead of
  plaintext.

### Engines and packaging
- postgres, mysql, mongodb, mssql, sqlite, redis, kafka, a generic reladiff
  engine, and a mysql->postgres cross-engine path. The postgres engine is
  pure Python; the project shells out to no bundled scripts.
- `doctor --install` and `bootstrap.sh` install every external tool the
  toolkit drives. Those tools are always separate programs, never bundled.
- `db_map` for differently-named target databases; the audit is local-only,
  so the target keeps no bookkeeping of its own.
