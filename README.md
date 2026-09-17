# migkit

> Verify, repair, and move databases across engines - without trusting the mover.

[![ci](https://github.com/arkashira/migkit/actions/workflows/ci.yml/badge.svg)](https://github.com/arkashira/migkit/actions/workflows/ci.yml)
[![engines](https://img.shields.io/badge/engines-postgres,%20mysql,%20mongodb,%20mssql,%20sqlite,%20redis,%20kafka-2a78d6)](#supported-engines)
[![cross-engine](https://img.shields.io/badge/cross--engine-mysql_to_postgres-0ca30c)](#cross-engine-hetero)
[![python](https://img.shields.io/badge/python-3.10+-3776ab)](pyproject.toml)
[![license](https://img.shields.io/badge/license-MIT-0ca30c)](LICENSE)

migkit does everything *around* a database migration: it prepares the target,
tells you exactly when to start the mover, watches the load, validates the
result down to every row and every object, and repairs what the mover could
not carry. The heavy data movement is done by a managed service (AWS DMS,
Tencent DTS, GCP DMS) or by native replication - or, when the network is
trusted, by migkit itself with full crash-resume.

The rule it is built on: **never let the mover be the judge of its own work.**

[Docs / usage reference ->](https://migkit.axentx.cloud/) | [Changelog ->](CHANGELOG.md)

---

## Table of contents

- [Why](#why)
- [Features](#features)
- [Quickstart](#quickstart)
- [Install](#install)
- [Commands](#commands)
- [One result shape](#one-result-shape)
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

- **Layered validation** - structure (tables, columns, PK, FK, indexes,
  defaults, views, procedures, triggers, sequences, extensions) -> table
  presence and exact row counts -> auto-increment / identity / sequence values ->
  full row-data checksums with per-primary-key drilldown. Every pass shows both
  sides' numbers.
- **Fast at scale** - data checksums use a commutative sum-of-md5 that Postgres
  runs as a parallel aggregate; a 488M-row table verifies in ~10 minutes, a
  1.06B-row database in ~16, with zero sorts and no locks beyond a plain SELECT.
- **Repair with undo** - align sequences/identity to the source value (never
  max+1, so deleted-id gaps stay identical), or delete-and-recopy differing
  rows by primary key. Target rows are saved before any change; source is never
  written.
- **One move command, no choices to make** - `move` uses the fastest bulk
  path installed for the engine and falls back to a chunked copy that resumes
  after a crash; a single table always takes the resumable path. `--mode cdc`
  follows the engine's native change feed, or stands up migkit's own streaming
  pipeline where the engine has none. You never tell migkit which tool to use:
  it knows, and `migkit doctor` tells you what this machine can do. Whatever
  moves the data, migkit verifies it.
- **No double scans** - when counts and data run together, row counts ride
  along with the checksum query, so each table is scanned once, not twice.
  `-q/--quiet` drops the per-table chatter and keeps diffs, errors and
  summaries.
- **Consistency by design, not guesswork** - `check --consistent` checksums
  every table of a database inside one repeatable-read transaction per side;
  suspect rows are then proven in-flight or real with an **LSN fence**: wait
  until every replication consumer (your subscription *or* an opaque managed
  mover - its slot lives on the source) confirms flushing past the source
  LSN, then re-compare. What survives two fenced rounds is a real diff.
- **Delta verify, O(changes)** - `watch --verify --delta` keeps a logical
  slot (pg) / binlog position (mysql) / change-stream token (mongo) and each
  cycle re-verifies **only the rows touched since the last verified point**.
  The cursor advances only after a clean verify, so crashes and diffs replay
  the same window - idempotent by construction, cheap enough to run forever
  against billion-row databases.
- **Column fingerprint** - when a table differs, one extra scan with one
  aggregate per column names *which columns* drift before any row-level
  work ("only `updated_at` differs" is a timezone bug, not data loss).
- **Deep checks** (`check --deep`) - FK orphan scan behind NOT VALID
  constraints, disabled triggers, column-level type/null/default/charset
  drift, materialized-view freshness, table-grant parity, and a boundary
  check (max PK / newest `_id` both sides) that catches CDC stalls and
  rogue writers on the target.
- **Continuous validation** - `watch --verify` re-checks on an interval and
  tells transient replication lag apart from a real diff (the
  confirm-out-of-sync idea from enterprise tools).
- **Zero footprint on the destination** - migkit writes nothing of its own
  into the target, so the target stays a faithful copy of the source and
  schema verification never trips over migkit's own objects. The audit is
  local-only: a per-hop `changelog.jsonl` ledger plus state journals.
- **State and rollback** - tagged snapshots of target sequences and schema kept
  in two places, a `terraform-plan`-style rollback preview, and the local
  changelog ledger of every write migkit made.
- **Nothing reinvented** - schema comparison, cross-engine row diffing, DDL
  transpiling, row repair and column-level sampling are each done by a proven
  implementation that migkit drives and normalizes. Which one is an
  implementation detail: `migkit doctor` reports capabilities, not program
  names, and every capability degrades to a built-in fallback rather than
  disappearing. Third-party components are credited in
  [NOTICE](NOTICE).
- **Web dashboard** - every hop's status, tiles and reports on one
  auto-refreshing page.

## Quickstart

```bash
uv tool install migkit   # or: pipx install migkit
migkit doctor            # what this machine can do (no config needed)
migkit init              # starter hops.yaml, then fill in the endpoints
migkit assess  my-hop    # readiness before anything moves
migkit check   my-hop    # read-only, exit 1 on any difference
migkit report --serve    # dashboard at localhost:8899
```

## Install

```bash
uv tool install migkit     # or: pipx install migkit
```

Either one puts `migkit` on your PATH in its own isolated environment. Plain
`pip install migkit` works too if you would rather manage the environment
yourself.

There are no extras to choose. Every engine driver and every comparison
library is a hard dependency, so the install either gives you all of them or
fails telling you why - a missing driver should not be something you discover
at cutover.

Some capabilities also need a command-line program from the platform (parallel
dump/restore, cross-engine load, schema DDL). Nothing is required to start:

```bash
migkit doctor            # capabilities: ready / reduced / unavailable
migkit doctor --install  # install what is missing via brew or apt
```

`doctor` runs before you have configured anything - that is what it is for.
When you are ready:

```bash
migkit init              # writes ~/.config/migkit/hops.yaml, mode 600
```

migkit finds that file on its own. `MIGKIT_CONF=/path/to/hops.yaml` overrides
it, and a `conf/hops.yaml` in the working directory takes precedence, so a
project can keep its hops beside its code. The file looks like this:

```yaml
hops:
  my-hop:
    engine: postgres            # postgres | mysql | mssql | mongodb | sqlite | redis | kafka
    service: native             # playbook: aws-dms | tencent-dts | gcp-dms | native
    source: { host: src.example.com, port: 5432, user: app, password: "secret" }
    target: { host: 10.0.0.10,       port: 5432, user: app, password: "secret" }
    databases: [appdb, orders]  # source db names, empty = discover from source
    db_map: { appdb: app_prod } # optional src->dst rename; unmapped = same name
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
| `move` | moves the data; migkit picks the fastest available path and falls back to a crash-resumable copy. `--mode cdc` streams changes, natively or through migkit's own pipeline |
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

## One result shape

Every engine words its own checks differently: PostgreSQL reports `encoding`
where MySQL reports `charset`, and MongoDB reports `null-missing` where
PostgreSQL reports `nullempty`. Those are the same two failures. `check`
writes `verdict.json`, in which they carry the same name on every engine, so a
report can be read - and aggregated across hops - without knowing which
engine produced it.

```json
{
  "format_version": 1,
  "tool": "migkit",
  "hop": "prod-cutover",
  "status": "different",
  "has_differences": true,
  "totals": {"ok": 231, "warn": 0, "diff": 3, "error": 0, "skip": 0},
  "by_category": {"access.sequence-grants": {"diff": 1}},
  "fingerprint": "9f2c...",
  "findings": [{"category": "identity.sequence-collision", "...": "..."}]
}
```

- `status` is one of `same`, `different`, `error`, `incomplete`
- `category` is engine-independent and stable: `value.charset`,
  `value.collation`, `value.null-empty`, `value.timezone`, `value.precision`,
  `identity.sequence-collision`, `access.sequence-grants`,
  `structure.partitions`, `movement.target-ahead`, `parity.row-count`, and so
  on. Categories are only renamed with a `format_version` bump.
- `findings` holds what is not `ok`, so an empty list means a clean run
- `fingerprint` covers the verdicts, not the wording, so a repeated check
  reports "identical to the previous run" instead of making you diff two
  reports by eye

`summary.json` is still written unchanged next to it.

## Supported engines

| Tier | Engines |
|---|---|
| Native | postgres, mysql, mssql, mongodb, sqlite, redis, kafka |
| Alias | mariadb, percona, tdsql, aurora-mysql/postgres, alloydb, documentdb, cosmosdb-mongo, azure-sql |
| Row comparison | snowflake, bigquery, redshift, clickhouse, oracle, trino, duckdb, vertica, databricks |
| Schema comparison (JDBC) | db2, h2, firebird, informix, sybase - drop the driver jar |

Managed services on any cloud (RDS/Aurora, Cloud SQL/AlloyDB, Azure Database,
TencentDB) work over the standard wire protocol; provider quirks (DocumentDB
without dbHash, TencentDB unlogged rules) are handled by built-in fallbacks.

## Cross-engine (hetero)

MySQL -> PostgreSQL is verified end to end:

```bash
migkit schema my2pg --convert --apply    # transpile the DDL and apply it
migkit move   my2pg --go                 # resumable chunked copy
migkit move   my2pg --mode cdc --db X --go   # CDC from the binlog, checkpointed
migkit check  my2pg                      # cross-dialect row verify
```

The `hetero` engine is an orchestrator that reuses the per-side native engines,
so new pairs (pg->mysql, mssql->pg) follow the same shape.

## How it compares

migkit does not try to out-move the movers. It drives the best of them and
adds the layer they all skip: proving the result is correct and repairing what
is not. The comparison below is feature-by-feature; migkit's column counts what
you get *through* migkit, including the tools it wraps at full capability.

| Capability | migkit | AWS DMS / Tencent DTS | Debezium | pt-table-sync | GoldenGate + Veridata |
|---|:--:|:--:|:--:|:--:|:--:|
| Bulk load (drives best mover) | yes | yes | no | partial | yes |
| Change data capture | yes | yes | yes | no | yes |
| Row-level verify with proof (LSN fence) | yes | partial | no | partial | yes |
| Consistency fence (no false diffs) | yes | no | no | partial | partial |
| Continuous delta verify, O(changes) | yes | no | no | no | partial |
| Column-level diff localization | yes | no | no | no | partial |
| Schema-object verify (views/routines/FK/index) | yes | no | no | no | partial |
| Sequence / identity carry and verify | yes | no | no | no | partial |
| Row-level repair with undo | yes | no | no | partial | yes |
| Schema repair (apply DDL, with undo) | yes | no | no | no | partial |
| Rollback / state snapshots | yes | no | no | no | partial |
| Self-hosted, no vendor lock-in | yes | no | yes | yes | partial |
| Zero footprint on the target | yes | no | yes | yes | partial |
| Engines | 9 + cross | cloud-scoped | 8 | mysql | oracle-centric |
| License | free (MIT) | paid | free | free | commercial |

<sub>"Through migkit" = the wrapped tool driven under a single command,
plus migkit's own verify/repair layer.</sub>

### Silent-corruption detection

The movers report success while the data is quietly wrong. These are the
failure classes migkit detects on its own - `check` auto-discovers what applies
to the hop and runs every relevant one, no flags to choose. Sourced from
real-world DMS/DTS/GoldenGate post-mortems, not guesses.

| Silent failure (data looks "present", is wrong) | migkit | DMS/DTS | Veridata | data-diff | pt-sync |
|---|:--:|:--:|:--:|:--:|:--:|
| Sequence / identity collision (`nextval <= max(pk)`) | yes | no | no | no | no |
| Type-narrowing / silent truncation (varchar, scale, int) | yes | no | no | no | no |
| Charset corruption (utf8mb4 cut, latin1 mojibake, U+FFFD) | yes | no | no | no | no |
| Uniform timezone offset (systematic, not row noise) | yes | no | no | no | no |
| Collation unique-collapse + glibc/ICU version drift | yes | no | no | no | no |
| Partition routing (rows stranded in default/MAXVALUE) | yes | no | no | no | no |
| Generated/computed column drift (stored != expression) | yes | no | no | no | no |
| RLS partial-dump / default-deny lockout | yes | no | no | no | no |
| NULL vs empty-string flip (Oracle `''`=NULL) | yes | no | no | no | no |
| No-PK table (CDC drops updates/deletes) | yes | no | partial | no | no |
| NOT VALID / untrusted constraints + FK orphans | yes | no | no | no | no |
| Deferrable-constraint drift | yes | no | no | no | no |
| Extensions + sequence-level grant parity | yes | no | no | no | no |
| Replication slot bloat / abandoned slot / long-txn | yes | partial | no | no | no |

<sub>Postgres and MySQL have the full set; MongoDB/MSSQL have the base layer
with the smart set landing engine by engine. Full usage reference:
<b><a href="https://migkit.axentx.cloud/">migkit.axentx.cloud</a></b>.</sub>

The managed services move data well but validate weakly and cannot repair a
single row or carry a sequence. Debezium and pt-table-sync are excellent at one
job each; migkit adds the verification neither performs.
Veridata is the closest match on verify-and-repair, and is commercial and
Oracle-centric. migkit is the one place that combines move, provable verify,
row-and-schema repair with undo, and rollback, across engines, for free.

## Safety model

- `check` (incl. `--deep`/`--drill`), `assess`, `watch`, `report`, `history`
  are read-only and can run anytime.
- `sync`, `move` (all modes), `schema --convert` write to the target; all are
  dry-run by default and require `--apply`/`--go`.
- The source is never written by migkit, and neither is anything on the target
  beyond the migrated data itself - no bookkeeping tables.
- A lock file prevents concurrent writes; every write is recorded in the local
  changelog ledger.
- Movers are self-hosted only on a trusted network (or a cloud VM); managed
  services remain the recommended path over long-haul links.

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -q                    # full suite (spins up throwaway docker DBs)
pytest tests/ -q -m "not docker"    # unit + fail-case only, no docker
```

100+ tests: pure-logic units, CLI-surface tests (11 visible commands, legacy
aliases stay invocable), mover and pipeline selection, test_decoding
parsing, end-to-end integration against throwaway Postgres containers
(including the full delta-verify loop: touch -> flag -> replay -> repair ->
advance, and the consistent-snapshot pass), exact repair-undo restore against
a MySQL pair, Faker-generated data covering every column type, and
failure-mode tests (bad credentials, missing state, locks, no-PK tables,
credential drift). CI runs the no-docker subset on every push and the full
suite on Ubuntu runners.

## License

MIT - see [LICENSE](LICENSE). (Confirm before publishing if a
freemium/proprietary model is preferred instead.)
