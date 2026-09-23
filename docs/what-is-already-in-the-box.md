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
  difference: a second reading naming something the schema check passed
  over means the target's schema is not verified.

  **None of that reaches the operator as a tool name.** Someone using
  migkit is moving and verifying a database so their application keeps
  working; which library read which catalogue is migkit's business, and a
  verdict saying *"pgcopydb found a difference migkit missed"* hands them a
  puzzle they cannot act on - they did not install pgcopydb and cannot run
  it. The information is kept and the brand name is not: *"read two ways,
  with two answers"*. Pinned across every branch, including the `fix_hint`,
  by `test_the_report_does_not_name_its_tools.py`, which now scans every
  string reaching `Result(...)`, `SystemExit(...)` or `print(...)` across
  the whole package - it found 15 more and they are gone. Report scopes are
  named for what they tell you: `(fix DDL)` and `(object changes)`, with
  the files they write renamed to match (`schema-fix.sql`,
  `schema-fix.revert.sql`, `schema-objects.txt`).

  Two lines were drawn deliberately. The database's own vocabulary stays -
  `CREATE SUBSCRIPTION`, a replication slot, `wal_level` are PostgreSQL and
  the DBA acting on the message knows them. And a missing prerequisite used
  to name the program so it could be installed, which is the one place the
  name bought something; it buys less than `migkit doctor --install`, which
  is migkit's own command and installs whatever that machine is short of,
  so the messages point there and the name goes too. Still to do:
  `options.schema_authority: atlas` is a config value an operator writes.
  `test_crosscheck_schema.py`.
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

`migrate lint` **cannot be used and the reason is not a technical one.**
Measured on the installed atlas v1.2.4:

    atlas migrate lint --dir file:///tmp/mig --dev-url postgres://...
    Abort: Starting with v0.38, 'atlas migrate lint' is available only to
    Atlas Pro users.  ...  atlas login

A check migkit runs for every hop cannot sit behind a paid account and an
interactive login. It also wanted a migration directory in its own format
plus a scratch database it creates and drops schemas in - which migkit
would have to stand up, since pointing it at a real target is out.

**What it was wanted for is built instead, and the built version is
stronger.** A linter reads SQL and can only say a statement *might* be a
problem; migkit knows whether the target's tables have rows, so it says
*will*. See `ddl.py` and D9b: `ALTER TABLE ... ADD COLUMN ... NOT NULL`
with no default, which migkit generates whenever the source has such a
column, fails outright on PostgreSQL and is silently filled in by MySQL.

### liquibase - 1 of many

    used:     diff
    unopened: diffChangeLog, update, rollback, snapshot, status,
              changelog-sync

`rollback` **cannot be used, and the reason is structural.** It only undoes
changesets recorded in its own `DATABASECHANGELOG` table, and migkit never
puts any there - it generates DDL and the operator applies it, so there is
never anything to roll back. Measured on 4.33.0 against a database it had
never touched:

    liquibase ... rollback --tag=nope
    ERROR: Could not find tag 'nope' in the database

    select tablename from pg_tables where schemaname='public'
    databasechangelog
    databasechangeloglock
    t

**Two tracking tables, from a command that failed.** Getting to the point
where there was something to roll back would mean writing those into
somebody's target as a side effect of using migkit.

The same probe on `diff`, which the schema check runs on every hop, left
the target and the source exactly as they were - so that one stays, and a
test now pins that migkit never reaches for one of the others.

migkit's own undo needs neither: `revert.py` takes the same diff in the
opposite direction at the same instant, names every forward statement no
DDL can undo, and says `no undo could be generated` rather than offering a
file that is not one. `test_a_check_does_not_write_to_the_target.py`.

### mydumper - the row filter is already there

    used:     --threads, --no-schemas, --trx-tables (spelling asked
              of the installed build), --omit-from-file, --defaults-file,
              -B -h -P -u -o -d; password in MYSQL_PWD, never on argv
    unopened: -x/--regex (db.table matching), --rows (chunked parallel
              per table)

`--where` is now wired: a generated defaults file gives mydumper one
section per table, verified on a live dump (2 of 3 rows where a rule
applied, untouched where none did). `--regex` is still unopened.

**Three of the flags that list used to hold did not exist in the installed
build, and the MySQL bulk path could not run at all.** Measured against
mydumper/myloader v1.0.5:

- the password is passed attached, `-p<secret>`, and that build does not
  accept the attached short form. The characters after `-p` are then read
  as *short options*: `-ptest` becomes `-p -t -e -s -t`, and the run dies
  on `Error parsing option -t` - naming a flag migkit passed separately and
  an operator never typed. With a password whose letters all happen to be
  valid no-argument flags the parse succeeds and silently turns them on:
  `-pmd` enables `--no-schemas` and `--no-data`, then fails
  authentication, because the password was never sent.
- `--trx-consistency-only` is gone; this build spells it `--trx-tables`.
- `--purge-mode` is gone from myloader; the modes now live on
  `--drop-table`, which is an *optional-argument* option - so
  `--drop-table TRUNCATE` does not take its value and falls back to the
  documented default, `DROP`.

Worth recording separately: extracting option names from `--help` picks up
options mentioned in another flag's *description*. `--overwrite-tables`
appears in the help text only inside the description of
`--overwrite-unsafe`, and the binary answers `Unknown option
--overwrite-tables`.

And measured on a data-only dump, **neither `--drop-table=TRUNCATE` nor
`--drop-table=DELETE` empties the target**; the load appends. The
PostgreSQL paths never delegated this - they generate the truncate
themselves.

**Closed.** The password travels in `MYSQL_PWD`, which both programs read
(measured; a wrong one exits 1), so it reaches neither argv, the log, nor
the process list - and it had reached all three, plus the program's own
error: with a password containing `-`, the old line died on `Unknown
option -p<the password>`. Flag spellings are asked of the binary that is
about to run, from the option column of its `--help` only. migkit empties
the target itself, leaving what the hop excludes, with
`foreign_key_checks = 0` in one session - and only once the dump is
complete, so an unreachable source leaves the target as it was. The dump
skips excluded tables through `--omit-from-file`, resolved by the same
`excluded_tables()` as every other mover, and the loader's `-B` is the
target's name for the database, which `db_map` may change. The plan and
the run are one command line. `test_the_mysql_bulk_path_runs.py`, on a
live pair.

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
2. ~~**The MySQL bulk path does not run.**~~ **done** - it runs, and on the
   installed build: password off the command line, flags asked of the
   binary, the target emptied by migkit after the dump rather than by a
   loader flag that no longer exists, and `hop.exclude` honoured by the
   dump and the emptying alike. `test_the_mysql_bulk_path_runs.py`. Still
   open beside it: the index window drops indexes on excluded tables too,
   and the pg_dump path still empties the target before it has a dump.
3. mydumper `--regex` / `--where`, pgcopydb `--filters`, `pg_dump -t/-T`,
   Debezium `table.include.list` - the mover half of plan item 17, all of it
   already installed.
4. ~~pgcopydb `snapshot`, `follow`, `compare data`~~ **done** - a shared
   snapshot for the consistent pass, a CDC path driven from here for a
   target that cannot dial the source, and an independent verifier to check
   migkit's own against. `compare schema` is still unopened.
5. `GenericEngine` - **schema comparison and the key battery done**;
   discovery still open. `HeteroEngine` now has a schema check too (D9c) -
   it had none, and reported a column the target does not have as a
   footnote on a green row verdict. It declared `checks = ("counts", "data")`, so a hop
   on any of those nine engines never compared the two schemas at all: a
   target built with `int` where the source has `bigint` matched on counts,
   matched row for row, and overflowed later. The catalogue answers the
   same five columns for every one of them - name, declared type, datetime
   precision, numeric precision, numeric scale - so one comparison serves
   all nine, and it names which way the risk runs (`narrower on the target
   - values the source holds will not fit`, `the target drops the offset`).

   What it cannot see is measured and **said in every verdict, including
   the clean one**: string lengths and nullability are not in the query the
   library issues - `varchar(50)` and `varchar(200)` come back identical -
   and getting them would mean writing that query once per engine, eight of
   which cannot be tried here. A clean line that quietly means "some of the
   schema" is worse than no line. The normalised types are unusable for
   this and that is why the raw rows are read: `bigint` and `integer` both
   normalise to `Integer`. `test_generic_schema.py`.
6. atlas `migrate lint` on the DDL migkit already generates.
7. Decide sqlglot: open it for transformation and DDL translation, or drop
   it from the manifest.
