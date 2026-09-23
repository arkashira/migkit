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

### pgcopydb - 7 of 11 commands used

    used:     copy table-data, ping, follow, stream sentinel,
              compare data, compare schema, list tables
    unopened: clone, fork, snapshot, dump, restore

What that leaves on the floor:

* **`follow`** - a complete logical-decoding CDC pipeline with resume and
  an end position, run from wherever migkit runs. That last part is the
  reason to want it: PostgreSQL's own `CREATE SUBSCRIPTION` needs the
  *target* to dial the source, which plenty of migrations cannot do, and
  it leaves the source's password in `pg_subscription` when it can (G2).
  `follow` dials both sides itself and stores nothing on the target.

  Tried against a pair on two docker networks with no route between them -
  exactly the case that defeats a subscription - and it replayed the
  changes correctly. Two things measured on the way, neither of them in
  the tool's help text, and both of which a wrapper has to handle:

  * It applies nothing until `pgcopydb stream sentinel set apply`. Before
    that the log says only *"Waiting until the pgcopydb sentinel apply is
    enabled"*.
  * With apply enabled it still sat on six decoded statements for over two
    minutes without touching the target - while reporting `replay_lsn`
    back to the source as fully caught up, so the *source's*
    `confirmed_flush_lsn` advanced past changes the target did not have.
    The target's own `pg_replication_origin_status` was the only honest
    number. A cutover decision read off the source slot would have been
    made against an empty target.

  **Done** - `MIGKIT_CDC=follow` on `move --mode cdc`, an environment
  variable rather than a flag because which path is possible is a fact
  about the network and not a preference. migkit drives the sentinel
  (`set apply`, `set endpos --current`), names the slot and the origin per
  hop and database, keeps the state under the hop's reports rather than
  `/tmp` - the source releases WAL as soon as pgcopydb has written a change
  there, so that directory is the only copy until the target has it - and
  judges the run by the process reaching its end position, not by
  `applied_lsn >= endpos`, which never comes true (D12, learned twice).
  `--drop` takes back the slot, the publication and the origin, so an
  abandoned leg cannot pin WAL (E5). Checked before and after a run: the
  source gained no schema and no table, only a publication.
  `test_cdc_driven_from_here.py`.
* ~~**`compare data`**~~ **done** - run against migkit's own verdict on the
  same pair under `MIGKIT_CROSSCHECK=1`, and reported as a deep check. It
  agreed on all four pairs it was tried on: identical rows, `numeric` 1.0
  against 1.00 (both *differ* - equal by `=`, not as stored), the same
  columns in a different order (both *same*), and a table with no primary
  key missing a row (both *differ*). Off by default because it reads both
  databases a second time. `test_crosscheck_pgcopydb.py`.
* ~~**`compare schema`**~~ **done, and used in one direction only.** It is
  not a second opinion the way `compare data` is - it is a *narrower*
  check. Measured on 0.18 by introducing one difference at a time:

      target missing a column       differ  (names the column)
      target missing an index       differ
      target missing a table        differ
      varchar(50) -> varchar(200)   **successful** - missed

  migkit reports that last one, deliberately: `neutral_columns` reads
  `format_type` so a target built wider than its source is visible, because
  a widened column loses a limit the application relied on without losing a
  row to show for it. So "pgcopydb says same, migkit says differs" is the
  expected shape, not a clash, and reporting it as one would cry wolf on
  every widened column. Only the other direction is reported as a
  difference: pgcopydb naming something migkit's schema check passed over
  means migkit missed it. `test_crosscheck_schema.py`.
* **`snapshot`** - and the interesting part is that migkit does not need
  pgcopydb for it. `pg_export_snapshot` is a PostgreSQL function, and the
  gap it closes is in migkit's *own* verifier: the fast data pass reads
  tables through a thread pool, one connection each, so two tables are read
  at two instants and a row moving between them looks like a difference in
  both. `--consistent` already avoids that by reading a side inside one
  transaction, and pays the parallelism for it. Measured with a writer
  between the export and the reads:

      workers not sharing the snapshot    a=2  b=2
      workers sharing the snapshot        a=1  b=1

  `_fast_consistent` now uses it: a side's tables are split across lanes,
  every lane adopts the same snapshot, and one lane asks for the fence LSN
  so there is a single position to prove convergence against. A side that
  cannot export falls back to the single script it always ran, because an
  inconsistent "consistent" pass is worse than a slow one.
  `test_shared_snapshot.py`.
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
    unopened: SMT (RegexRouter, Filter), other connectors on the same
              runtime

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

`table.exclude.list` is wired too: both connectors were asked what they
accept (a config validate lists it for PostgreSQL and MySQL alike), so the
hop's deny list maps across with no inversion, and the CDC leg stops
carrying a table the bulk movers already skip. The values are regexes, so
the dots are escaped - unescaped, `public.orders` would also match
`publicXorders` and quietly stop streaming something nobody excluded.
Unlike the signal keys, Debezium really does validate this one: a broken
regex comes back `The 'table.exclude.list' value is invalid`.

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
3. ~~pgcopydb `snapshot`, `follow`, `compare data`~~ **done** - a shared
   snapshot for the consistent pass, a CDC path driven from here for a
   target that cannot dial the source, and an independent verifier to check
   migkit's own against. `compare schema` is still unopened.
4. `GenericEngine` discovery + the deep battery - turns nine named engines
   from "reladiff speaks it" into "migkit supports it".
5. atlas `migrate lint` on the DDL migkit already generates.
6. Decide sqlglot: open it for transformation and DDL translation, or drop
   it from the manifest.
