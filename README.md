# migkit

> Verify, repair, and move databases across engines — without trusting the mover.

[![engines](https://img.shields.io/badge/engines-postgres%20·%20mysql%20·%20mongodb%20·%20mssql%20·%20sqlite%20·%20redis%20·%20kafka-2a78d6)](#supported-engines)
[![cross-engine](https://img.shields.io/badge/cross--engine-mysql→postgres-0ca30c)](#cross-engine-hetero)
[![python](https://img.shields.io/badge/python-3.10+-3776ab)](pyproject.toml)
[![license](https://img.shields.io/badge/license-internal-52514e)](#license)
[![status](https://img.shields.io/badge/status-active-0ca30c)](CHANGELOG.md)

migkit does everything *around* a database migration: it prepares the target,
tells you exactly when to start the mover, watches the load, validates the
result down to every row and every object, and repairs what the mover could
not carry. The heavy data movement is done by a managed service (AWS DMS,
Tencent DTS, GCP DMS) or by native replication — or, when the network is
trusted, by migkit itself with full crash-resume.

The rule it is built on: **never let the mover be the judge of its own work.**

[ภาษาไทย →](README.th.md) · [Changelog →](CHANGELOG.md)

---

## Table of contents

- [Why](#why)
- [Features](#features)
- [Quickstart](#quickstart)
- [Install](#install)
- [Commands](#commands)
- [Supported engines](#supported-engines)
- [Cross-engine (hetero)](#cross-engine-hetero)
- [How it compares](#how-it-compares)
- [Safety model](#safety-model)
- [License](#license)

---

## Why

Managed migration tools (DMS/DTS) move rows well but leave gaps that break the
target silently: they skip sequences and identity counters, secondary indexes,
foreign keys, defaults, views, procedures and triggers; they only compare rows,
never structure; and they cannot repair a single differing row or roll back.

migkit closes every one of those gaps with a uniform, evidence-first workflow
across all engines. Every `OK` prints the counts and hashes of both sides, so
"equal" is something you can see, not something you trust.

## Features

- **Layered validation** — structure (tables, columns, PK, FK, indexes,
  defaults, views, procedures, triggers, sequences, extensions) → table
  presence and exact row counts → auto-increment / identity / sequence values →
  full row-data checksums with per-primary-key drilldown. Every pass shows both
  sides' numbers.
- **Fast at scale** — data checksums use a commutative sum-of-md5 that Postgres
  runs as a parallel aggregate; a 488M-row table verifies in ~10 minutes, a
  1.06B-row database in ~16, with zero sorts and no locks beyond a plain SELECT.
- **Repair with undo** — align sequences/identity to the source value (never
  max+1, so deleted-id gaps stay identical), or delete-and-recopy differing
  rows by primary key. Target rows are saved before any change; source is never
  written.
- **Best-tool movers, auto-selected** — `move` drives whichever proven mover
  is installed (`--via auto` is the default): parallel `pg_dump -j`/`pg_restore
  -j`, mydumper/myloader, pgloader, mongodump/mongorestore — and generates
  ready-to-run **Debezium Connect** configs (`--via debezium`) for
  platform-grade CDC. The builtin chunked copy stays the fallback and the only
  mode with per-chunk crash resume; native CDC (pg logical replication, mysql
  binlog incl. the RDS variant, mongo change streams) is one flag away.
  Whatever moves the data, migkit verifies it.
- **No double scans** — when counts and data run together, row counts ride
  along with the checksum query, so each table is scanned once, not twice.
  `-q/--quiet` drops the per-table chatter and keeps diffs, errors and
  summaries.
- **Consistency by design, not guesswork** — `check --consistent` checksums
  every table of a database inside one repeatable-read transaction per side;
  suspect rows are then proven in-flight or real with an **LSN fence**: wait
  until every replication consumer (your subscription *or* an opaque managed
  mover — its slot lives on the source) confirms flushing past the source
  LSN, then re-compare. What survives two fenced rounds is a real diff.
- **Delta verify, O(changes)** — `watch --verify --delta` keeps a logical
  slot (pg) / binlog position (mysql) / change-stream token (mongo) and each
  cycle re-verifies **only the rows touched since the last verified point**.
  The cursor advances only after a clean verify, so crashes and diffs replay
  the same window — idempotent by construction, cheap enough to run forever
  against billion-row databases.
- **Column fingerprint** — when a table differs, one extra scan with one
  aggregate per column names *which columns* drift before any row-level
  work ("only `updated_at` differs" is a timezone bug, not data loss).
- **Deep checks** (`check --deep`) — FK orphan scan behind NOT VALID
  constraints, disabled triggers, column-level type/null/default/charset
  drift, materialized-view freshness, table-grant parity, and a boundary
  check (max PK / newest `_id` both sides) that catches CDC stalls and
  rogue writers on the target.
- **Continuous validation** — `watch --verify` re-checks on an interval and
  tells transient replication lag apart from a real diff (the
  confirm-out-of-sync idea from enterprise tools).
- **Zero footprint on the destination** — migkit writes nothing of its own
  into the target, so the target stays a faithful copy of the source and
  schema verification never trips over migkit's own objects. The audit is
  local-only: a per-hop `changelog.jsonl` ledger plus state journals.
- **State and rollback** — tagged snapshots of target sequences and schema kept
  in two places, a `terraform-plan`-style rollback preview, and the local
  changelog ledger of every write migkit made.
- **Composes real tools** — migra, liquibase, atlas (schema); reladiff,
  pt-table-sync (data); pgloader, sqlglot (cross-engine); datacompy
  (column-level sample diff). Nothing reinvented; each degrades gracefully if
  absent.
- **Web dashboard** — every hop's status, tiles and reports on one
  auto-refreshing page.

## Quickstart

```bash
cd migkit && ./bootstrap.sh && source .venv/bin/activate
cp conf/hops.example.yaml conf/hops.yaml   # fill in endpoints
migkit doctor                              # hops + tools + connectivity
migkit assess  my-hop                      # readiness before the mover
migkit check   my-hop                      # read-only, exit 1 on any diff
migkit report --serve                      # dashboard at localhost:8899
```

## Install

```bash
./bootstrap.sh
```

Installs libpq, creates the main venv plus a Python 3.12 `.venv-tools` for
reladiff, and pulls optional drivers (mysql, mongo, redis, kafka) and helpers
(migra, datacompy). Anything that will not install is skipped with a note —
every feature has a built-in fallback. Then set up a hop in `conf/hops.yaml`
(gitignored, chmod 600):

```yaml
hops:
  my-hop:
    engine: postgres            # postgres | mysql | mssql | mongodb | sqlite | redis | kafka
    service: native             # playbook: aws-dms | tencent-dts | gcp-dms | native
    source: { host: src.example.com, port: 5432, user: app, password: "secret" }
    target: { host: 10.0.0.10,       port: 5432, user: app, password: "secret" }
    databases: [appdb, orders]  # empty = discover from source
    workers: 4
```

## Commands

Eleven commands cover the whole lifecycle:

| Command | What it does |
|---|---|
| `doctor` | configured hops, local tools, connectivity |
| `assess` | premigration readiness (CDC prereqs, no-PK tables, encoding, accounts) |
| `advise` | playbook for the hop's mover, phase by phase |
| `schema` | target schema plan; `--convert` transpiles cross-engine DDL, `--migration` writes Flyway-style `V__/U__` files |
| `check` | layered read-only validation, exit 1 on diff; `--consistent` = one repeatable-read txn per side + LSN fence; `--deep` adds FK-orphan/drift/render/boundary checks; `--drill` = column-level sample diff |
| `move` | drives the best installed mover (`pg_dump -j`, mydumper, pgloader, mongodump) or the builtin resumable copy; `--mode cdc` native streams, `--via debezium` generates Connect configs |
| `watch` | live load progress: counts, rate, ETA, replication state; `--verify` = continuous re-check loop; `--verify --delta` = O(changes) verification off the WAL/binlog/change stream |
| `sync` | make target equal source: dry-run plan, `--apply` executes with undo, `--go` checks + repairs with rollback checkpoints |
| `rollback` | restore any saved state, with a plan preview |
| `history` | saved rollback states + the local changelog ledger |
| `report` | HTML report from the last check; `--serve` runs the live dashboard |

Every check is read-only and rerunnable. Every repair is dry-run unless
`--apply`/`--go`, saves an undo first, and converges to the same end state on
re-run. Add `-q/--quiet` before any command to silence per-table chatter.

The pre-0.2 command names (`hops`, `setup-target`, `repair`, `replicate`,
`tail`, `convert-schema`, `gen-migration`, `sample-diff`, `ui`, `state`,
`monitor`) still work as hidden aliases, so existing scripts keep running.

## Supported engines

| Tier | Engines |
|---|---|
| Native | postgres, mysql, mssql, mongodb, sqlite, redis, kafka |
| Alias | mariadb, percona, tdsql, aurora-mysql/postgres, alloydb, documentdb, cosmosdb-mongo, azure-sql |
| Generic (reladiff) | snowflake, bigquery, redshift, clickhouse, oracle, trino, duckdb, vertica, databricks |
| Schema via liquibase (JDBC) | db2, h2, firebird, informix, sybase — drop the driver jar |

Managed services on any cloud (RDS/Aurora, Cloud SQL/AlloyDB, Azure Database,
TencentDB) work over the standard wire protocol; provider quirks (DocumentDB
without dbHash, TencentDB unlogged rules) are handled by built-in fallbacks.

## Cross-engine (hetero)

MySQL → PostgreSQL is verified end to end:

```bash
migkit schema my2pg --convert --apply    # sqlglot/pgloader DDL transpile
migkit move   my2pg --go                 # resumable chunked copy
migkit move   my2pg --mode cdc --db X --go   # CDC from the binlog, checkpointed
migkit check  my2pg                      # reladiff cross-dialect verify
```

The `hetero` engine is an orchestrator that reuses the per-side native engines,
so new pairs (pg→mysql, mssql→pg) follow the same shape.

## How it compares

- **vs DMS/DTS/Veridata** — migkit validates structure + sequences + data (they
  validate rows only or nothing), proves in-flight vs real diffs with an LSN
  fence instead of guessing, repairs by row with undo, and keeps
  state/rollback. Delta verify gives Veridata-style continuous validation on
  any of pg/mysql/mongo, for free.
- **vs pt-table-checksum/pt-table-sync** — pt gets consistency from running
  through the replication channel but only works master→replica on mysql.
  migkit fences on the replication position instead, which also works across
  clusters and opaque managed movers — and when pt-table-sync *is* usable,
  repair drives it for statement generation.
- **vs Debezium/pgloader/mydumper** — not competitors, employees: `move` picks
  and drives whichever is installed, generates the Debezium Connect configs
  when you want platform CDC, and wraps verification around all of them —
  which none of them do on their own.
- **vs migra/results** — migkit uses migra as one of four schema layers, and
  adds the entire data dimension migra does not cover.
- **vs Liquibase/Flyway** — different category (they version schema changes for
  CI/CD). migkit adopts their best ideas — rollback, preconditions (`assess`),
  versioned migration files (`schema --migration`) — but keeps its own audit
  local instead of in the target DB, and is a verification/move tool, not a
  changeset runner.

## Safety model

- `check` (incl. `--deep`/`--drill`), `assess`, `watch`, `report`, `history`
  are read-only and can run anytime.
- `sync`, `move` (all modes), `schema --convert` write to the target; all are
  dry-run by default and require `--apply`/`--go`.
- The source is never written by migkit, and neither is anything on the target
  beyond the migrated data itself — no bookkeeping tables.
- A lock file prevents concurrent writes; every write is recorded in the local
  changelog ledger.
- Movers are self-hosted only on a trusted network (or a cloud VM); managed
  services remain the recommended path over long-haul links.

## Testing

```bash
pip install -e . faker pytest
pytest tests/ -q                    # full suite (spins up throwaway docker DBs)
pytest tests/ -q -m "not docker"    # unit + fail-case only, no docker
```

49 tests: pure-logic units, CLI-surface tests (11 visible commands, legacy
aliases stay invocable), mover selection and Debezium codegen, test_decoding
parsing, end-to-end integration against throwaway Postgres containers
(including the full delta-verify loop: touch → flag → replay → repair →
advance, and the consistent-snapshot pass), exact repair-undo restore against
a MySQL pair, Faker-generated data covering every column type, and
failure-mode tests (bad credentials, missing state, locks, no-PK tables,
credential drift). CI runs the no-docker subset on every push and the full
suite on Ubuntu runners.

## License

Internal / personal tooling. Not for redistribution without the author's
consent.
