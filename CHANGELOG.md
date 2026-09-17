# Changelog

The notable changes, grouped by area. This project is pre-1.0; releases are
cut from `master` and versions are tagged as features stabilize.

## Unreleased

### Verification
- Repair DDL now says what it will lock. `check` writes
  `structural-fix.locks.txt` next to `structural-fix.sql`, classifying every
  generated statement by the lock it takes, flagging the ones that block
  writes or block everything, and naming the safer form where one exists
  (`CREATE INDEX CONCURRENTLY`; `ADD CONSTRAINT ... NOT VALID` then
  `VALIDATE CONSTRAINT`; a validated `CHECK (col IS NOT NULL)` before
  `SET NOT NULL`). The count appears in the check's own verdict line.
  This closes an asymmetry: the check throttles itself while *reading* the
  target, then handed over DDL for *writing* to that same live target with
  nothing said about what it would block.
  The classification is verified against a live PostgreSQL rather than
  asserted - each statement is executed in a transaction, `pg_locks` is read
  for our own backend, and the transaction rolled back. The guarantee tested
  is one-sided: the static verdict may be heavier than reality, never
  lighter, because a false alarm costs a re-read and the opposite costs an
  outage. That measurement corrected one rule (`COMMENT ON` takes
  ShareUpdateExclusiveLock, not the trivial lock its harmlessness suggests).
- MongoDB collections are compared by id in `_id` ranges, so migkit no longer
  gives up past five million documents and sends the operator to another tool.
  The old code built a dict of every id and hash for both sides at once; a
  range at a time makes the memory cost proportional to the range and makes
  the comparison restartable.
  The partitioning has a trap worth recording: MongoDB's *sort* order spans
  BSON types but its *query* comparison operators are type-bracketed, so
  `{_id: {$lt: someObjectId}}` matches no integer and no string however the
  values sort. One ordered list of boundaries across a mixed-type `_id`
  therefore leaves whole types outside every range - the first version of this
  compared 600 of 680 documents and called them identical. The keyspace is now
  partitioned per BSON type, and a plan is only used after counting that it
  accounts for every document; anything else degrades to a single pass.
- MongoDB throttles its scans too, so load-awareness is now a property of
  migkit rather than of one engine. `connections.active` against the
  connection ceiling is the direct analogue of active sessions against
  max_connections; operations waiting in `globalLock.currentQueue` are folded
  into the same number, because work piling up *is* the server being at its
  limit. A standalone deployment reports replication lag as unknown rather
  than as zero - unknown and fine are different answers.
- Three checks MySQL was missing and PostgreSQL already had, taking MySQL from
  11 deep sub-checks to 14. They land on the same canonical categories with no
  extra mapping, so MySQL and PostgreSQL findings now aggregate together.
  - **NULL vs empty string** (`value.null-empty`) - a mover that swaps them
    leaves "something" in the column either way, so the row still looks
    present while the application's `IS NULL` and `= ''` branches diverge.
  - **FLOAT/DOUBLE drift** (`value.precision`) - a checksum is the wrong
    instrument for an approximation: equal values can hash differently and
    real drift can hash the same. Compared as an aggregate with a tolerance
    relative to the magnitude actually in the column.
  - **CHECK NOT ENFORCED** (`structure.unvalidated-constraints`) - MySQL's
    analogue of PostgreSQL's NOT VALID. The constraint is in the catalog and
    in every schema diff while enforcing nothing, so counting constraints
    finds both sides equal. On MySQL 5.7, where CHECK is parsed and discarded
    with no catalog to read, the check reports `skip` and says why rather than
    passing silently.
- MySQL verifies big tables in resumable ranges too, through the same range
  planner PostgreSQL uses. Porting it fixed a real gap in the MySQL code it
  replaced: those ranges started at `min(pk)` on the source, so any target row
  with a smaller key fell outside every range and was never compared. The
  shared planner is open at both ends, so that class of difference cannot hide
  on either engine. Proven against MySQL that the BIT_XOR-folded chunk totals
  equal the single-pass value - the same property as the PostgreSQL sum, but
  different algebra, so both are tested against a real server.
- MySQL throttles itself too, on `Threads_running` against `max_connections`
  plus its own query latency. The protection was PostgreSQL-only when it
  landed, which made it a property of one engine rather than of migkit.
- Chunk size is measured, not configured. How many rows a chunk should cover
  depends on row width, indexes and how busy the server is - none of which
  anyone can supply usefully as a number. migkit learns rows-per-second from
  the chunks it has already run and sizes the next table to a target runtime,
  clamped so a slow server is not split into millions of tiny queries and a
  fast one does not collapse back into the single giant query that made a long
  verify unrestartable. A table that already has partial progress keeps the
  size it was planned with, because re-chunking would change the range
  boundaries and discard every partial - an adaptive size must not defeat
  resume.
- A large-table verify is restartable. The data checksum is a commutative sum
  over `numeric`, so the per-primary-key-range sums add up to exactly the
  whole-table value - which makes partial progress meaningful rather than just
  a position marker. Tables past a size threshold with a single integer
  primary key are checksummed in ranges, each completed range is persisted,
  and a rerun only pays for what it still owes. Proven against Postgres: the
  chunked total equals the single-pass total. Partials carry a fingerprint of
  the checksum expression and the range boundaries, so a resumed run can never
  add work from two different table states. Side benefit: a difference now
  names the key range it is in, not just the table. No flag - the decision
  comes from the table's own row estimate.
- The verifier throttles itself. A full-table checksum is only a SELECT, so
  nothing used to stop `check` from running `workers` threads against a small
  instance that was serving traffic - which is how a two-vCPU Aurora instance
  got pegged and restarted twice during a real UAT run. It now reads the
  server's own load (active sessions against the configured maximum,
  replication lag) plus its own query latency, and both sleeps and *narrows*
  concurrency when any of them says the database is struggling. No flag: the
  signal comes from the database. A permanently busy server still gets
  verified - the wait per unit of work is bounded, after which it proceeds at
  minimum concurrency and records that it did. Whatever it did appears in
  `verdict.json` under `load`, because a run that silently took three times
  longer is its own kind of failure.
- One result shape across every engine: `check` writes `verdict.json` with an
  engine-independent `category` per finding, so the same failure carries the
  same name whether it came from PostgreSQL, MySQL or MongoDB. Includes a
  run-level `status`, per-category counts, and a `fingerprint` over the
  verdicts (not the wording) so a repeated check can say "identical to the
  previous run". `summary.json` is unchanged.
- Layered check: schema, object inventory, row counts, sequence/identity
  values, and full row-data checksums with per-primary-key drilldown. Every
  pass reports both sides' counts and hashes.
- `check --consistent`: whole-database checksum inside one repeatable-read
  transaction per side, with the source LSN captured as a fence.
- LSN-fenced convergence: suspect rows are re-compared only after every
  replication consumer (including opaque managed movers) confirms flushing
  past the captured LSN. Falls back to a settle delay when no slot is visible.
- Delta verify (`watch --verify --delta`, `sync --mode stream`): re-verify
  only the rows changed since the last verified point, on every engine -
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
- `move` has no tool to choose. It uses the fastest bulk path installed for
  the engine (parallel dump/restore, parallel MySQL load, cross-engine load,
  collection dump/restore) and falls back to a resumable chunked copy; a
  single table always takes the resumable path. `MIGKIT_MOVER` forces one for
  debugging - an environment variable, so it stays off the command surface.
- `move --mode cdc --go` follows live changes with the engine's native
  mechanism (logical replication, binlog, change streams). Where an engine has
  none, migkit writes, launches, registers and supervises its own streaming
  pipeline instead - same command, no extra flag, and nothing about the
  runtime underneath is a user-facing choice. Third-party components are
  credited in NOTICE.

### Install and packaging
- `pip install migkit` / `uv tool install migkit` / `pipx install migkit` give
  a working `migkit` on PATH with no checkout, no virtualenv to activate and
  no `PYTHONPATH`. Verified from a built wheel into an empty environment.
- `doctor` runs with no configuration at all - reporting what the machine can
  do is the step before there is a config. Asking for the hop list directly
  still fails, because there is nothing to list.
- `migkit init` writes a starter `~/.config/migkit/hops.yaml` at mode 600. The
  config is found there, or in `./conf/hops.yaml`, or via `MIGKIT_CONF`; an
  installed copy never asks anyone to write inside site-packages.
- The version lives in `migkit.__version__` only; `pyproject.toml` reads it
  from there so the two cannot drift.
- Tests invoke the CLI belonging to the interpreter running them instead of a
  hardcoded `<repo>/.venv/bin/migkit`.
- `packaging/` holds the Homebrew formula and the two credentialed steps
  (PyPI upload, tap) that are left.

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
