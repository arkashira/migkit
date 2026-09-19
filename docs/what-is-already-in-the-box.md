# What is already in the box, and how much of it is switched on

migkit's design is to wrap the best tool for each job and normalise it into
one config and one set of commands - `movers.py` says it plainly: *"migkit
reimplements none of it - it wires proven components together and owns the
lifecycle, so the operator drives everything through migkit."*

The risk that design carries is this: a wrapped tool can be used at ten per
cent of what it does, and nothing in the codebase says so. This file is the
audit. Every line below was produced by running the tool or grepping the
tree, not from memory.

Re-run it before planning new work. The cheapest capability is the one
already installed.

## 1. Declared and never imported

| Dependency | State |
|---|---|
| `sqlglot` | In `pyproject.toml`. **Zero imports anywhere in `migkit/`.** |

It used to be load-bearing: `hetero.py` records that DDL conversion "was a
sqlglot transpile followed by ten regular expressions" before it was
replaced. The replacement was right; leaving the dependency behind was not.

Two honest options, and they are not the same: drop it, or open it. sqlglot
parses and transpiles SQL across twenty-odd dialects, which is the same
shape as three things on the plan - rewriting a query for a renamed table
(item 17), translating DDL between engines, and the parsing half of stored
logic conversion (item 20). It is a decision to make deliberately rather
than a line to leave sitting in the manifest.

## 2. Wrapped, but only one door is open

Measured by running each tool's own help, and by grepping every invocation
in `migkit/`.

### pgcopydb - 2 of 11 commands used

    used:     copy table-data, ping
    unopened: clone, fork, follow, snapshot, compare, dump, restore,
              list, stream

What that leaves on the floor:

* **`follow`** - a complete logical-decoding CDC pipeline with resume and
  an end position. migkit has one CDC path (Debezium); this is a second,
  native to PostgreSQL, with no Kafka to stand up.
* **`compare data` / `compare schema`** - pgcopydb's own verification. Worth
  running *against* migkit's on the same pair: either it agrees, which is
  evidence, or it does not, which is a finding.
* **`snapshot`** - export one consistent snapshot for every worker to share.
  This is the mechanism behind the concurrency the plan admires in item 2 of
  *what a migration actually costs*, and it is one command away.
* **`clone`** - schema, data, indexes, constraints and sequences in one
  pass, in the order pgcopydb already knows is fastest.

`--filters` is now used: the hop's `exclude` list is resolved against the
tables the source actually has and written as an `[exclude-table]` file, so
a table the hop excludes is no longer copied and then ignored. Verified on
pgcopydb 0.18 - `list tables --filters` returned 2 of 3 tables with the
file, 3 without it. `pg_dump -T` gets the same resolved names (verified:
`-T public.audit_log --data-only` dumped `COPY public.orders` and nothing
else), so both PostgreSQL movers and the check exclude one identical set
rather than three readings of one pattern.

### atlas - 1 of ~6 command groups used

    used:     schema diff
    unopened: schema inspect, schema apply, migrate (diff, lint,
              validate, hash), tool

`migrate lint` is the interesting one: it reads a migration and reports the
destructive changes in it. migkit generates DDL for the operator to review
(`schema --migration`); linting that DDL with the tool already installed is
a review nobody has to do by hand.

### liquibase - 1 of many

    used:     diff
    unopened: diffChangeLog, update, rollback, snapshot, status,
              changelog-sync

`rollback` matters: migkit has its own rollback via state snapshots, and
liquibase has one for schema. Two mechanisms that should agree.

### mydumper - the row filter is already there

    used:     --threads, --trx-consistency-only, --no-schemas,
              --purge-mode, -B -h -P -u -o -d
    unopened: -x/--regex (db.table matching), --where (dump only
              selected records), --rows (chunked parallel per table)

`--where` is now wired: a generated defaults file gives mydumper one
section per table, verified on a live dump (2 of 3 rows where a rule
applied, untouched where none did). `--regex` is still unopened.

**The asymmetry this exposed is worth recording.** `pg_dump` 18.6 offers
`-t`, `-T`, `--exclude-table-data` and `--filter`; `pgcopydb` 0.18 offers
`--filters`. Every one selects *tables*. Neither has a row predicate, so a
PostgreSQL hop with `mapping.where` is refused before the copy starts
rather than moved in full and left failing its own check for ever.

### Debezium - the runtime is up, the signal channel is not

    used:     source connector (MySQL, PostgreSQL), JDBC sink, upsert,
              delete.enabled, schema.evolution basic
    unopened: signal.data.collection + signal.enabled.channels
              (ad-hoc and incremental snapshot), SMT (RegexRouter,
              Filter), other connectors on the same runtime

**Now open, and the measurement changed the design.** The `source`
channel reads the request from a signalling table in the source database;
migkit writes to a source nowhere, so the **Kafka** channel was used
instead - the broker is already in the generated compose file. Verified
end to end against Debezium 3.9 on the generated pipeline: the signal
topic is created, the `SignalProcessor` joins it, and the request is read.

What the run then refused is the part worth keeping:

    INCREMENTAL  DebeziumException: Incremental snapshot is not properly
                 configured, either sinalling data collection is not
                 provided ...          topic watermark unchanged at 50
    BLOCKING     Finished exporting 50 records for 'public.orders'
                 snapshot_completed=true   topic watermark 50 -> 100

An incremental snapshot brackets each chunk with watermark rows written
into that same table on the source, so it is not available without write
access there. Blocking needs no table and works. `resnapshot_message`
defaults to blocking for that reason, and says so where the operator
reads it. Tests: `test_resnapshot_signal.py`.

### mongodump / mongorestore

    used:     --archive, --drop, --quiet
    unopened: --query (row filter), --numParallelCollections,
              --oplog (point-in-time consistency)

### boto3 - one client

    used:     s3 (the state store)
    unopened: secretsmanager (credentials without a plaintext hops.yaml),
              rds (ask the managed target what it is, rather than probing)

### datacompy / pandas

    used:     PandasCompare(...).report()
    unopened: all_mismatch(), df1_unq_rows/df2_unq_rows, tolerances,
              the Spark and Polars backends

## 3. The engine matrix is wider than the engine list

`GenericEngine` wraps reladiff, and its own docstring names the reach:

> Any engine reladiff speaks: snowflake, bigquery, redshift, clickhouse,
> oracle, trino, presto, duckdb, vertica and more.

So migkit can already *verify* against Oracle, Snowflake, BigQuery,
Redshift, ClickHouse, Trino, DuckDB and Vertica today. What is missing is
not the reach - it is the depth and the ergonomics:

* `checks = ("counts", "data")` - no schema check, and **no deep battery at
  all** (`check_deep` returns `skip: no deep checks for this engine yet`).
* The operator must hand-write a connection URL in `options.url` and list
  every table in `options.tables`. Nothing is discovered.
* No mover: these are verification-only hops.

That is the difference between "reladiff is a dependency" and "migkit
supports Snowflake". Closing it is discovery plus the deep checks that
already exist on the neutral contract - not a new engine.

## 4. Deep checks, by engine

Counted from each engine's `check_deep`:

| engine | deep checks |
|---|---|
| postgres | 15 |
| mysql | 15 |
| mssql | ~4 (keys, fk, triggers, columns) |
| mongodb | ~4 (boundary, bson-types, null-missing, collection options) |
| redis | 2 (ttl, bigkeys) |
| kafka | 1 |
| sqlite, generic, hetero | 0 |

The zeros are honest - the base returns `skip`, not `ok` - but `hetero` is
the leg the project advertises, and `generic` is every engine in section 3.

## 5. Order of work this implies

Cheapest first, where "cheap" means no new dependency:

1. ~~Debezium signal channel~~ **done** - and wired into `sync`: when a
   pipeline is running, the row repair is planned as a re-snapshot through
   the connector instead of a direct write, because migkit and the
   connector writing the same keys is a race migkit can lose.
   `test_repair_through_the_stream.py`.
2. mydumper `--regex` / `--where`, pgcopydb `--filters`, `pg_dump -t/-T`,
   Debezium `table.include.list` - the mover half of plan item 17, all of it
   already installed.
3. pgcopydb `snapshot`, `follow`, `compare` - a consistent snapshot, a
   second CDC path, and an independent verifier to check ours against.
4. `GenericEngine` discovery + the deep battery - turns nine named engines
   from "reladiff speaks it" into "migkit supports it".
5. atlas `migrate lint` on the DDL migkit already generates.
6. Decide sqlglot: open it for transformation and DDL translation, or drop
   it from the manifest.
