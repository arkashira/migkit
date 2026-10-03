# migkit

> Verify, repair, and move databases across engines - without trusting the mover.

<!-- gen_docs: badge -->
[![ci](https://github.com/arkashira/migkit/actions/workflows/ci.yml/badge.svg)](https://github.com/arkashira/migkit/actions/workflows/ci.yml)
[![engines](https://img.shields.io/badge/engines-34-2a78d6)](#supported-engines)
[![pairs](https://img.shields.io/badge/cross--engine-any_pair-0ca30c)](#cross-engine-hetero)
[![python](https://img.shields.io/badge/python-3.10.12+-3776ab)](pyproject.toml)
[![license](https://img.shields.io/badge/license-AGPL--3.0--or--later-0ca30c)](LICENSE)
<!-- /gen_docs: badge -->

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
- [Case study: what a "consistent" report does not cover](docs/case-study-what-a-consistent-report-does-not-cover.md)
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

Worked example, with real findings from migrations a managed service had
already reported as consistent:
[what a "consistent" report does not cover](docs/case-study-what-a-consistent-report-does-not-cover.md).

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
- **Repair DDL says what it locks** - the generated fix is accompanied by a
  per-statement lock classification, flagging what blocks writes and what
  blocks everything, with the safer form named where one exists. Verified
  against a live server, and deliberately one-sided: it may over-warn, never
  under-warn.
- **A long verify survives being interrupted** - big tables are checksummed
  in primary-key ranges whose sums add up to exactly the whole-table value, so
  a run that dies at 90% resumes instead of starting over, and a difference is
  reported against the key range it lives in. Partials are fingerprinted, so a
  table that changed underneath them is re-verified rather than half-trusted.
- **Never the heaviest thing on the server** - `check` reads the database's
  own load (active sessions against its limit, queued operations, replication
  lag) and its own query latency, then sleeps and narrows concurrency while
  the server is struggling. PostgreSQL, MySQL and MongoDB each report it in
  their own terms; a signal a provider hides reads as unknown, never as idle. Nothing to configure and no safe-concurrency to guess; a busy
  database is still verified, just more slowly, and `verdict.json` records
  that it was throttled and why.
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

Two addresses to a cutover; every command that writes shows its plan
first and does nothing until `--go`:

<!-- gen_docs: quickstart -->
```
uv tool install migkit
migkit doctor --install
migkit init
migkit assess shop
migkit move shop --mode full+cdc
migkit move shop --mode full+cdc --go
migkit check shop
migkit move shop --mode cdc --drop --go
```
<!-- /gen_docs: quickstart -->

What a hop can be for - its `goal`, one word
([what each does on each side](docs/goals-and-modes.md)):

<!-- gen_docs: goals -->
| Goal | What it is for |
|---|---|
| `copy` | move the data once and prove every row arrived |
| `cutover` | move, follow the source's changes, prove both equal, and switch with the least downtime - a stream back ready from the moment of the switch |
| `verify` | prove two databases hold the same; nothing is written to either |
| `two-way` | both sides written, each side's changes carried to the other |
| `keep-in-sync` | a copy that follows the source for as long as it runs, the application kept off it |
| `rehearse` | the whole path the real run will take - copy, follow, prove, cut over - on a copy or a sample of the source into a target of its own, every phase timed, so the hop it rehearses (rehearses:) is forecast from what it measured |
| `upgrade` | the same engine at a newer version, a higher edition (SQL Server Web to Enterprise), or both, with the least downtime: what the new version or edition changes checked first, then the data moved into it and followed, or a copy of its pages taken to the target where the hop allows it - upgraded there where the version moves - and switched over |
| `subset` | a copy for testing: the rows chosen and every row they refer to, masked where the hop says, into a staging or developer database, proved row by row |
| `consolidate` | many databases or tenants into one: each source's rows under a column the hop names, filled with its own value, keys kept apart by the hop's rule, proved source by source |
| `split` | one database into many - a service extracted, a database split by region: each part's rows by its rule, every row placed exactly once, proved |
| `archive` | old rows to cheaper storage: the rows each rule picks added to an archive and proved there, then removed from the source only once proved and only where the hop allows it |
<!-- /gen_docs: goals -->

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

In a terminal, `init` asks where the data is and where it goes - an
address each, or a task file from another tool (a DMS task, a
change-capture connector's configuration, a LOAD DATABASE file) - and the
passwords, which it keeps in the OS keychain or leaves in environment
variables. It connects, says what it found (software and versions, who runs
each side, sizes, tables without a key, types that need care across two
engines, whether changes can be followed, what the users may do), proposes
the plan, writes a script for the DBA where a grant or a setting is
missing, and writes the hop. Without a terminal it reads `MIGKIT_SOURCE`
and `MIGKIT_TARGET` instead, or writes a template.

migkit finds that file on its own. `MIGKIT_CONF=/path/to/hops.yaml` overrides
it, and a `conf/hops.yaml` in the working directory takes precedence, so a
project can keep its hops beside its code. The file looks like this:

```yaml
hops:
  shop:
    goal: cutover                 # copy (left out) | cutover | verify | two-way | keep-in-sync
    source: postgres://app@db1.example.com:5432/shop?sslmode=verify-full
    target: mysql://app@10.0.0.10/shop
```

The engines, the pair, the ports, the databases and TLS are read from the
addresses; the number of workers, the size of a range and the way each
table goes are decided from the servers, so the file says none of them. A
password is never in an address - `init` keeps it in the keychain, or
write a reference in its place (`postgres://app:env:SRC_PW@db1/shop`). The
long form (`source: {host: ..., port: ..., user: ...}`, `databases`,
`db_map`) stays valid, and every configuration is checked before anything
runs: an unknown key is refused with the closest known one, a wrong type or
a contradiction with where it is.

A move reads every range it writes back from the target and holds it to
what it copied, and compares a bulk program's result with the source before
it calls the move complete. `options: { verify_batches: false }` turns that
off for a hop where the time matters more.

## Commands

<!-- gen_docs: commands -->
| Command | What it does |
|---|---|
| `init` | Set up a hop: in a terminal, asks for the two addresses and the passwords, connects, says what it found and what it will do, and writes the hop. |
| `doctor` | What this machine can do, and whether the configured hops connect. |
| `advise` | Show the migration playbook for this hop's service. |
| `assess` | Premigration readiness check: CDC prerequisites, risky tables, encoding, extensions, accounts. |
| `schema` | Target schema workflows: preparation plan (default), cross-engine DDL conversion (--convert), versioned migration files (--migration). |
| `check` | Read-only validation of target vs source. |
| `sync` | Make target equal to source, or run a whole migration flow. |
| `move` | Move data for this hop. |
| `watch` | Watch a running migration load: row counts, rate, ETA, replication state. |
| `report` | Render the HTML report from the last check run and export the hop's evidence (what moved, every verdict with both sides' counts and digests and how to take them again, decisions, approvals, repairs, outages - HTML, Markdown, JSON, CSV), or --serve the web view: every hop's outcomes, progress, approvals, offers and evidence for the people each hop names, the REST API, and a Prometheus /metrics endpoint (`migkit.web`). |
| `history` | Audit trail: saved rollback states and every write migkit made, read from the local changelog and state journals. |
| `rollback` | Restore target sequences from a saved state, put back the rows the repairs since it wrote, where the target still holds what they wrote; where the hop streamed back or cut over, the old source's counters raised past the new primary's. |
| `users` | Sync logins source -> target keeping the same password. |
<!-- /gen_docs: commands -->

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

<!-- gen_docs: engines -->
34 engines, any two of them as a pair; migkit's own decision engine finds the fastest, exact, consistent way for each source -> target pair. What each can do, cell by cell, is on [the engines page](https://migkit.axentx.cloud/site/engines.html).

| Role | Engines |
|---|---|
| either side | PostgreSQL (`postgres`), MySQL (`mysql`), MongoDB (`mongodb`), SQL Server (`mssql`), Redis (`redis`), Kafka (`kafka`), SQLite (`sqlite`), Parquet files (`parquet`), ClickHouse (`clickhouse`), DynamoDB (`dynamodb`), Oracle Database (`oracle`), Db2 (Linux, UNIX, Windows) (`db2`), Db2 for i (`db2i`), Db2 for z/OS (`db2zos`), SAP ASE (`ase`), Teradata (`teradata`), OpenSearch (`opensearch`), Cassandra (`cassandra`), Amazon Redshift (`redshift`), Snowflake (`snowflake`), BigQuery (`bigquery`), DuckDB (`duckdb`), Files described by COBOL copybooks (`copybook`), Iceberg tables (`iceberg`), Delta tables (`delta`), TiDB (`tidb`), CockroachDB (`cockroachdb`), Aurora DSQL (`dsql`), Couchbase (`couchbase`), Cosmos DB for NoSQL (`cosmosdb`) |
| a stream a pair delivers its changes into | Kinesis Data Streams (`kinesis`), Google Pub/Sub (`pubsub`) |
| a source, read and never written | SAP application data (ODP) (`sapodp`), SaaS applications, read by a connector program (`saas`) |
| the pair: a source of one engine, a target of another | Any pair of two engines (`hetero`) |
| either side, compared by the tables the hop lists | Any other SQL engine (`generic`) |

Also written: `alloydb`, `as400`, `aurora-dsql`, `aurora-mysql`, `aurora-postgres`, `azure-cosmos`, `azure-sql`, `cobol`, `cockroach`, `cosmos`, `cosmosdb-mongo`, `cosmosdb-nosql`, `crdb`, `db2-for-i`, `db2-zos`, `db2z`, `delta-lake`, `documentdb`, `elasticsearch`, `ibmi`, `iseries`, `mariadb`, `odp`, `percona`, `pgsql`, `postgresql`, `program`, `sap-ase`, `sap-odp`, `scylla`, `scylladb`, `sqlserver`, `sybase`, `td`, `tdsql`, `valkey`, `vantage`, `vsam`.

Managed services recognised: Aiven for Apache Cassandra (Aiven); Aiven for Apache Kafka (Aiven); Aiven for ClickHouse (Aiven); Aiven for MySQL (Aiven); Aiven for OpenSearch (Aiven); Aiven for PostgreSQL (Aiven); Aiven for Valkey (Aiven); Alibaba Cloud Elasticsearch (Alibaba Cloud); AlloyDB for PostgreSQL (Google Cloud); Amazon Aurora DSQL (AWS); Amazon Aurora MySQL (AWS); Amazon Aurora PostgreSQL (AWS); Amazon DocumentDB (AWS); Amazon DynamoDB (AWS); Amazon ElastiCache (Valkey, Redis OSS) (AWS); Amazon Keyspaces (for Apache Cassandra) (AWS); Amazon Kinesis Data Streams (AWS); Amazon MSK (AWS); Amazon MemoryDB (AWS); Amazon Neptune (AWS); Amazon OpenSearch Service (AWS); Amazon RDS for Db2 (AWS); Amazon RDS for MariaDB (AWS); Amazon RDS for MySQL (AWS); Amazon RDS for Oracle (AWS); Amazon RDS for PostgreSQL (AWS); Amazon RDS for SQL Server (AWS); Amazon Redshift (AWS); Amazon Timestream (AWS); AnalyticDB for MySQL (Alibaba Cloud); AnalyticDB for PostgreSQL (Alibaba Cloud); ApsaraDB RDS for MariaDB (Alibaba Cloud); ApsaraDB RDS for MySQL (Alibaba Cloud); ApsaraDB RDS for PostgreSQL (Alibaba Cloud); ApsaraDB RDS for SQL Server (Alibaba Cloud); ApsaraDB for MongoDB (Alibaba Cloud); ApsaraDB for OceanBase (Alibaba Cloud); ApsaraMQ for Kafka (Alibaba Cloud); Azure Cache for Redis (Azure); Azure Cosmos DB for Apache Cassandra (Azure); Azure Cosmos DB for MongoDB (RU) (Azure); Azure Cosmos DB for NoSQL (Azure); Azure Cosmos DB for PostgreSQL (Azure); Azure Database for MySQL flexible server (Azure); Azure Database for PostgreSQL flexible server (Azure); Azure DocumentDB (formerly Azure Cosmos DB for MongoDB vCore) (Azure); Azure Event Hubs (Kafka endpoint) (Azure); Azure Managed Redis (Azure); Azure SQL Database (Azure); Azure SQL Managed Instance (Azure); Azure Synapse Analytics (dedicated SQL pool) (Azure); BigQuery (Google Cloud); Bigtable (Google Cloud); Cloud SQL for MySQL (Google Cloud); Cloud SQL for PostgreSQL (Google Cloud); Cloud SQL for SQL Server (Google Cloud); CockroachDB Cloud (CockroachDB Cloud); Confluent Cloud (Confluent Cloud); Crunchy Bridge (Crunchy Bridge); Firestore (Google Cloud); Huawei Cloud Distributed Cache Service (DCS) for Redis (Huawei Cloud); Huawei Cloud Distributed Message Service (DMS) for Kafka (Huawei Cloud); Huawei Cloud Document Database Service (DDS) (Huawei Cloud); Huawei Cloud GaussDB (Huawei Cloud); Huawei Cloud GeminiDB (Huawei Cloud); Huawei Cloud RDS for MySQL (Huawei Cloud); Huawei Cloud RDS for PostgreSQL (Huawei Cloud); Huawei Cloud RDS for SQL Server (Huawei Cloud); Huawei Cloud Search Service (CSS) (Huawei Cloud); Huawei Cloud TaurusDB (formerly GaussDB(for MySQL)) (Huawei Cloud); IBM Cloud Databases for Elasticsearch (IBM Cloud); IBM Cloud Databases for MongoDB (IBM Cloud); IBM Cloud Databases for MySQL (IBM Cloud); IBM Cloud Databases for PostgreSQL (IBM Cloud); IBM Cloud Databases for Redis (IBM Cloud); IBM Db2 Warehouse on Cloud (IBM Cloud); IBM Db2 on Cloud (Db2 SaaS) (IBM Cloud); IBM Event Streams (IBM Cloud); Lindorm (Alibaba Cloud); Memorystore for Redis (Google Cloud); Memorystore for Redis Cluster (Google Cloud); Memorystore for Valkey (Google Cloud); MongoDB Atlas (MongoDB Atlas); MySQL HeatWave (Oracle Cloud); Neon (Neon); OCI Cache (Oracle Cloud); OCI Database with PostgreSQL (Oracle Cloud); OCI Streaming (Kafka-compatible) (Oracle Cloud); Oracle Autonomous Database (Oracle Cloud); Oracle Base Database Service (Oracle Cloud); PlanetScale (MySQL, on Vitess) (PlanetScale); PlanetScale for Postgres (PlanetScale); PolarDB for MySQL (Alibaba Cloud); PolarDB for PostgreSQL (Alibaba Cloud); PolarDB-X (Alibaba Cloud); Pub/Sub (Google Cloud); Redis Cloud (Redis Cloud); SingleStore Helios (SingleStore Helios); Spanner (Google Cloud); Supabase (Supabase); TDSQL (distributed) (Tencent Cloud); TDSQL-C for MySQL (Tencent Cloud); TDSQL-C for PostgreSQL (Tencent Cloud); Tair (ApsaraDB for Redis) (Alibaba Cloud); Tencent Cloud CKafka (Tencent Cloud); Tencent Cloud Elasticsearch Service (Tencent Cloud); TencentDB for MariaDB (Tencent Cloud); TencentDB for MongoDB (Tencent Cloud); TencentDB for MySQL (Tencent Cloud); TencentDB for PostgreSQL (Tencent Cloud); TencentDB for Redis (Tencent Cloud); TencentDB for SQL Server (Tencent Cloud); Tiger Cloud (Timescale) (Tiger Cloud (Timescale)); YugabyteDB Aeon (YugabyteDB Aeon).
<!-- /gen_docs: engines -->

## Cross-engine (hetero)

MySQL -> PostgreSQL is verified end to end:

```bash
migkit schema my2pg --convert --apply    # transpile the DDL and apply it
migkit move   my2pg --go                 # resumable chunked copy
migkit move   my2pg --mode cdc --db X --go   # CDC from the binlog, checkpointed
migkit check  my2pg                      # cross-dialect row verify
```

The `hetero` engine is an orchestrator that reuses the per-side native engines,
so every pair follows the same shape: `source_engine` and `target_engine` in
the hop's options name any two of the engines above.

## How it compares

migkit's own decision engine finds the fastest, exact, consistent way for
each source -> target pair, and adds the layer movers skip: proving the
result is correct and repairing what is not. The comparison below is
feature-by-feature, by the kind of tool rather than any one of them.

| Capability | migkit | AWS DMS / Tencent DTS | a change-capture connector | a table-sync script | GoldenGate + Veridata |
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
| Engines | [every one below](#supported-engines), and any pair | cloud-scoped | several | one | oracle-centric |
| License | free (AGPL-3.0) | paid | free | free | commercial |

### Silent-corruption detection

The movers report success while the data is quietly wrong. These are the
failure classes migkit detects on its own - `check` auto-discovers what applies
to the hop and runs every relevant one, no flags to choose. Sourced from
real-world DMS/DTS/GoldenGate post-mortems, not guesses.

| Silent failure (data looks "present", is wrong) | migkit | DMS/DTS | Veridata | a row-diff tool | a table-sync script |
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

<sub>Postgres and MySQL have the full set; what every other engine does is on
the engines page of the usage reference, cell by cell. Full usage reference:
<b><a href="https://migkit.axentx.cloud/">migkit.axentx.cloud</a></b>.</sub>

The managed services move data well but validate weakly and cannot repair a
single row or carry a sequence. A change-capture connector and a table-sync
script are excellent at one job each; migkit adds the verification neither
performs.
Veridata is the closest match on verify-and-repair, and is commercial and
Oracle-centric. migkit is the one place that combines move, provable verify,
row-and-schema repair with undo, and rollback, across engines, for free.

## Safety model

- `check` (incl. `--deep`/`--drill`), `assess`, `watch`, `report`, `history`
  are read-only and can run anytime.
- `sync`, `move` (all modes), `schema --convert` write to the target; all are
  dry-run by default and require `--apply`/`--go`.
- The source is written only where the hop asks for it itself: the `two-way`
  goal, the cutover migkit drives (`cutover:`), or a change on the source a
  faster way needs and the operator agreed to (`source_changes`). What each
  goal does on each side is said before it runs
  ([goals and modes](docs/goals-and-modes.md)).
- A lock file prevents concurrent writes; every write is recorded in the local
  changelog ledger.
- Movers are self-hosted only on a trusted network (or a cloud VM); managed
  services remain the recommended path over long-haul links.

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -q -m "not docker"    # unit + fail-case only, no docker
python tools/final_suite.py         # the whole suite: pure tests side by side,
                                    # docker tests in lanes, a report
```

100+ tests: pure-logic units, CLI-surface tests (every visible command, legacy
aliases stay invocable), mover and pipeline selection, test_decoding
parsing, end-to-end integration against throwaway Postgres containers
(including the full delta-verify loop: touch -> flag -> replay -> repair ->
advance, and the consistent-snapshot pass), exact repair-undo restore against
a MySQL pair, Faker-generated data covering every column type, and
failure-mode tests (bad credentials, missing state, locks, no-PK tables,
credential drift). CI runs the no-docker subset on every push and the full
suite on Ubuntu runners.

## License

AGPL-3.0-or-later - see [LICENSE](LICENSE). migkit takes in, as its own
dependencies, every open tool and library that adds a capability,
copyleft ones included, and the Affero GPL is the licence all of them
can be combined under. The programs and libraries it drives keep their
own licences; those that are not open source are installed only after
the operator accepts their terms (`migkit doctor --install`) and used
only for what those terms allow.
