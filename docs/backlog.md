# Backlog: what is left before migkit finishes the job

One list, rebuilt on 2026-09-23 from:

* **Inside the project:**
  * the problems `problems-and-what-ends-them.md` still marks *Partly* or *Not yet* (18)
  * the plan items in `what-a-migration-actually-costs.md` not yet done (10)
  * the queue carried by the work loop
* **Outside:** how the managed services and the specialist tools actually
  work. The mechanism matters, not the feature name:
  * the managed services: AWS DMS, Azure's PostgreSQL migration service,
    Google Cloud DMS, Tencent DTS
  * the verifiers and movers that do one thing well: Oracle GoldenGate
    Veridata, Crunchy pgCompare, Google's Data Validation Tool, Vitess
    VDiff, Debezium's DBLog snapshots, MongoDB's migration-verifier

**How to read an item.** Each item says four things:
* what the best tool does, down to the mechanism
* what migkit has today, found by grepping the code rather than
  remembering it
* what the deeper version would be
* whether something already written can be wrapped to get there

"Wrap" means what was done with reladiff, migra/results, pgcopydb and
mydumper. The tool goes underneath, migkit decides when and how, and the
operator never sees the tool's name. Every wrap is measured before it is
trusted. Three tools have already surprised: `atlas migrate lint` needed a
paid login, `liquibase rollback` wrote tables into the target, and half the
mydumper flags in use no longer existed.

Rules that apply to every item:
* no new CLI modes or flags; hop options and environment variables are fine
* migkit never writes to a source
* a hop that does not use a feature behaves exactly as before
* nothing lands without a test against a real database

---

## What the others do that shapes this list

**Verification that runs while data is still moving.** Every serious
verifier has an answer for rows that differ only because a change has not
arrived yet:

| Tool | Mechanism |
|---|---|
| Veridata | hashes each row's non-key columns and puts mismatches in a "maybe out of sync" queue. After the configured replication latency has passed it re-reads only those keys. Each is then *in-sync*, *in-flight* (changed since, cannot be judged), or *persistently out of sync*. Only the last is reported, with repair SQL. |
| pgCompare | re-fetches only the flagged rows in its `check` pass. Its 0.7.0 fix is worth copying: a row deleted from both sides between rounds must count as converged. It used to fail the table forever. |
| migration-verifier (MongoDB) | queues every changed or mismatched document into a numbered *generation* and rechecks one generation per round until writes are switched off. It fails permanently on DDL. |
| DTS | re-checks N times at an interval. |

**Scale without holding a snapshot open:**

| Tool | Mechanism |
|---|---|
| Vitess VDiff v2 | resumable, can be stopped and resumed, re-runnable as a rolling diff. `--max-diff-duration` restarts a table's diff so a long repeatable-read snapshot does not hold back purge. |
| Debezium incremental snapshots (Netflix's DBLog) | read a table in chunks between low and high watermarks in the change log. Streamed events win over snapshot rows for the same key inside a window, so no global snapshot is ever held. The read-only variants use GTID sets (MySQL) or transaction IDs (PostgreSQL) instead of writing a signal table. |

**Repair after verification:**

| Tool | Mechanism |
|---|---|
| AWS DMS data resync (2025) | reads validation failures from a control table on the *target*, fetches each key's current row from the source, and upserts or deletes. Under CDC it runs on a cron schedule with a maximum duration, **pausing replication and validation while it works**. Needs a primary key. |
| Veridata | generates repair DML. |
| DTS independent check | generates repair SQL. |

**Before the move:**

| Tool | Mechanism |
|---|---|
| AWS DMS premigration assessments | one named check per risk. Examples: long transactions, `max_slot_wal_keep_size`, `logical_decoding_work_mem`, binlog compression, binlog retention > 0, `binlog_row_image=FULL`, `server_id>=1`, `max_allowed_packet` against the largest LOB, tables differing only by case, ARRAY columns without a key, PostGIS, target is a read replica, more than 10,000 tables. |
| Google DMS "test job" | **suggests the parameter values** instead of only failing on them. |
| Azure's service | runs a validate-only pass until it is clean. It also advises: target storage 1.25-2x the source for WAL, and HA and read replicas off on the target during the load. |
| DTS | 12 checks for PostgreSQL and 17 for MySQL, including multi-task conflict detection and DDL-loop detection. |

**What none of the managed services do.** Azure's own documentation tells
the operator to validate the data by hand: counts, object counts, min/max
ids. Google's PostgreSQL path says the same and points at DVT for the rest.
This is the ground migkit stands on.

---

## P0: the decision layer underneath everything

**0. Choose per table, from measured facts, and combine.**

*Started (2026-09-24):* `migkit/planner.py` makes one decision per table,
each with its reason. The first rules are the ones correctness forces:
* a table the hop excludes is left alone
* a table whose row filter the bulk path cannot apply goes table by table

These decisions used to be made in three places and were never said per
table. The facts come from the source's catalogue in one query per
database (`table_facts`: a row estimate, `null` rather than 0 for a table
never analysed, and whether there is a key; PostgreSQL and MySQL). The
dry run of `move` now reads the plan out, every table off the usual path
first (`tests/test_the_plan_by_table.py`). The second reading is the
planner's first verification rule (0a). Rules that choose on speed are
still to come. They wait for the benchmark (28) to give them measurements
to choose from.

* **Today:** one function, `movers.pick()`, chooses the mover per *engine*.
  It takes the first tool found installed (pgcopydb, pg_dump, mydumper,
  pgloader, mongodump) and falls back to the builtin copier. It never looks
  at:
  * how big the table is
  * whether it holds LOBs or has a key
  * the versions on either side
  * which network path works
  * whether a change stream has to follow

  Verification works the same way: the builtin checksum always, a second
  reader only when an environment variable asks for one.
* **Deeper:** a planner that decides for each table:
  1. reads the facts once: size, key, LOB columns, types, versions,
     reachability, the flags each installed tool actually has (`tool_flag`
     already asks the binary)
  2. picks a path per table rather than per database. A 40 GB keyed table
     can go through the parallel copier, a LOB table through a path that
     carries LOBs whole, and a key-less table through a consistent snapshot.
  3. combines them in one run
  4. picks the verification the same way: an independent second reader
     where the pair is cross-engine, or where the first reader has a known
     blind spot
* **What the operator sees:** only what was done and why, in migkit's own
  words ("copied in parallel chunks: 40 GB, keyed"). Never which program
  did it.
* **Wraps it combines:** everything already wrapped, plus item 8's DVT.
* **Tests:** assert the decision per table against real tables built to
  each shape.

**Rule from the owner (2026-09-24), binding on every item from here on:**
* choosing the tool is migkit's decision, made per task, per engine, and
  per table: the smartest choice, the best quality, the fastest finish
* progress and logs are normalised into migkit's own events and words.
  Nothing a wrapped program prints reaches the operator as that program's
  output.

**0e. One command, one result, every engine.**

*Owner's rule (2026-09-24):* the same hop configuration and the same
commands give the same result, with the full capability, on every engine
and every provider - the way DMS and DTS present one task whatever sits
underneath. How each engine gets there is migkit's business, and it is
normalised the same way everywhere. Where an engine has no native way, find
the library or tool that produces the result, wrap it, and measure it.

*Status (2026-09-24): the declared matrix and its test are done.*
`migkit/capabilities.py` reads what each engine does off the code (its own
methods, the movers, `users.py`'s dispatch) and declares only the gaps;
`tests/test_every_engine_declares_its_gaps.py` fails on an undeclared gap
and on a declared gap the code has closed. Still open: telling the operator
in migkit's words when a command is not yet available, and filling the
gaps below.

The matrix as `capabilities.matrix()` computes it (`-` = not yet, `n/a` =
declared not applicable with the reason):

| capability | pg | mysql | mongo | mssql | redis | kafka | sqlite | parquet | clickhouse | dynamodb | oracle | db2 | ase | opensearch | cassandra | redshift | snowflake | bigquery | kinesis | pubsub | hetero | generic |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| comparing the schema | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| comparing row counts | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| comparing the data itself | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | yes |
| the deep checks | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | - | yes | yes | - | - | - | n/a | n/a | yes | yes |
| carrying sequences and auto-increment values | yes | yes | n/a | yes | n/a | n/a | yes | n/a | n/a | n/a | - | - | - | n/a | n/a | - | - | - | n/a | n/a | yes | - |
| comparing server settings | yes | yes | yes | yes | yes | yes | yes | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| moving a whole database in bulk | yes | yes | yes | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| copying table by table, resumably | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| keeping the target following the source | yes | yes | yes | yes | yes | yes | n/a | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| proving the target has caught up before cutover | yes | yes | yes | yes | yes | yes | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| telling a difference still arriving from one that is wrong | yes | yes | yes | - | yes | - | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| verifying only what changed | yes | yes | yes | yes | - | yes | n/a | n/a | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |
| carrying users and their grants | yes | yes | yes | - | yes | - | n/a | n/a | - | n/a | - | - | - | - | - | - | - | - | n/a | n/a | - | - |
| noticing a move that moved nothing | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes | - |
| refreshing the target's statistics after a load | yes | yes | n/a | yes | n/a | n/a | yes | n/a | n/a | n/a | yes | - | - | n/a | n/a | - | - | - | n/a | n/a | yes | - |
| snapshotting the target so a cutover can be rolled back | yes | yes | yes | yes | yes | - | yes | - | - | - | - | - | - | - | - | - | - | - | n/a | n/a | yes | - |

The first hand-made version of this table said MongoDB's change stream was
"tail only". It is wired into `move --mode cdc`; the probe read it right.
The probe then said Redis verified deltas: its `delta_verify` did nothing
but return an error. That method is gone, and a method whose whole body
refuses no longer counts as a capability.

Closed since: SQLite copies table by table through the cross-engine copier
(`NeutralCopier`, the one copier for every engine that reads and writes
neutrally), and MongoDB notices a move that moved nothing. Then: MySQL
fences and confirms; Redis carries users (by password hash) and compares
its settings; Kafka and SQLite compare their settings; the cross-engine
bulk path notices an empty move; and a table-by-table move refreshes
statistics on every engine, SQLite included. Redis compares its schema: the
modules loaded on each side, and which kinds of value a sample of keys
holds. The table above is
regenerated from `capabilities.matrix()`.

Closed 2026-09-25:
* MongoDB copies collection by collection, document for document as
  stored, resumed by `_id` through a comparison that orders across types.
  A plain `$gt` would have lost every string `_id` after a batch ending
  on a number.
* Redis copies a keyspace key for key, in the server's own serialised
  form with each key's remaining time to live, resumed from the scan's
  cursor.
* SQL Server copies table by table through the shared copier, and
  refreshes the target's statistics.
* The empty-move guard asks through each side's own reads, so it covers
  every pair and every engine on the shared copier.
* A pair compares the two settings two engines share: what the zone names
  mean, and what the text can hold.
* Restore points: a pair keeps the target engine's own; SQLite keeps the
  whole target file through its online backup; Redis keeps the target's
  key kinds, beside the repair's own undo of every key it replaces.
* Redis replies are decoded so that every byte comes back as sent. Strict
  decoding stopped `check` on the first key that was not UTF-8.
* Kafka copies a topic message for message: each partition's messages go
  into the same partition on the target, with their keys, values, headers
  and times. The times are what a group's position is translated by
  afterwards. The topic is made with the source's partition count and the
  configs that decide what it means, and only committed messages are
  read. The copy was stopped after a batch reached the target and before
  its checkpoint was saved. On resume, that batch was counted off the
  target's own end and not sent twice: 300 messages arrived as 300, in
  order, identical
  (`tests/test_a_topic_copied_message_for_message.py`).
* A pair verifies only what changed (`delta`), from a source whose log
  reads without moving (`tests/test_a_pair_verifies_only_what_changed.py`).
* Redis follows, fences and confirms through its own replication
  (REPLICAOF). The replica signs in to the source as an account that may
  only replicate (`+psync +replconf +ping`), with a password drawn for
  the run; the source's own password never reaches the target. Measured
  on 7.4 first:
  * a replica's first sync empties every database of the target before
    it loads the source's snapshot, and it carries every database and
    key the source has, with no filter. So a hop that excludes keys, or
    whose databases leave out keys on either side, is refused, naming
    the databases. A Redis Cluster is refused too.
  * a 6.2 target of a 7.4 source cannot read the snapshot (`Can't handle
    RDB format version 12`): the link stays down, and the target had
    been emptied already. An older target is refused before anything is
    set up.
  * REPLICAOF answers OK when the replica cannot sign in, and says why
    only in the target's log. The status line waits for the link and,
    when it stays down, reads the source's record of refused sign-ins
    (ACL LOG): "the source refused the replica's sign-in".
  * the fence waits on the replica's offset against the source's, under
    the source's replication id. The check's confirm pass uses it: with
    the link held down while the source changed, the check said the
    difference was still arriving, and a key only the target had was
    still named once the replica caught up.
  * `move --mode full+cdc` on a Redis hop is the replica, as a
    subscription with its copy is PostgreSQL's. A repair beside a
    replica is refused, since a replica takes no writes but its own.
  (`tests/test_a_redis_target_that_replicates_the_source.py`)
* SQL Server keeps a restore point: each identity's current value, which
  a rollback reseeds with `dbcc checkident`, and the definitions of its
  views, functions, procedures and triggers. Found on the way: reading
  rows through the driver had given the engine a second `_q`, which hid
  the one that runs statements through the client, and every native
  check of a SQL Server hop stopped on a `TypeError` before asking the
  server anything. The target was also asked under the source's database
  name, whatever `db_map` said. Both fixed
  (`tests/test_sql_server_native_checks_run.py`).
* SQL Server follows and fences through Change Tracking - the key and
  last operation of each row changed since a version, read as the row is
  now. Measured against Azure SQL Edge (SQL Server's engine, in the build
  that runs on arm64): an insert, an update of it and a delete came back
  as the inserted row with its new value and a delete, and the tail kept
  a mapped database level with the fence passing. A table the database
  does not track, or changes its retention has cleaned up, stop the tail
  by name (`tests/test_sql_server_follows_through_change_tracking.py`).
  Every engine read through a DB-API driver - SQL Server, Oracle, Db2,
  SAP ASE, the warehouses - now applies a change stream: a row updated
  by its key, and inserted where none was, so a change carrying only
  some columns keeps the others.
* The native SQL Server checks, run against a server for the first time,
  had three faults, all fixed (`tests/test_sql_server_checks_against_a_server.py`):
  * the client refused the server's own certificate (`x509: negative
    serial number`, since Go 1.23), so no check ran at all
  * a statement that failed exited 0, with its message read as rows -
    and both sides of a comparison can print the same message
  * a constraint the server named was compared by that name: the same
    primary key was `PK__o__3213E83F720CC3A2` in one database and
    `PK__o__3213E83FA19E373F` in the other, one missing and one extra. It
    is named by its kind, table and columns now
* Kafka follows and fences: the tail is the topic copier, round after
  round, going on from the positions the copy saved, so nothing is sent
  twice; the fence waits on each partition's saved position against the
  source's end. A transaction leaves a marker at a partition's end that
  no reader of committed messages is given, and without taking the
  consumer's own position there the fence waited for an offset that never
  arrived (measured: 82 against 80 messages). A topic copied to nothing
  is named after a move
  (`tests/test_a_kafka_target_that_follows_the_source.py`).

Tests:
* `tests/test_a_collection_copied_document_for_document.py`
* `tests/test_a_keyspace_copied_key_for_key.py`
* `tests/test_a_move_that_moved_nothing_on_any_engine.py`
* `tests/test_the_settings_two_engines_share.py`

**The deeper version:**
* **A declared matrix, not a hidden one.** Every engine states, for every
  capability, one of: *implemented*, *not applicable* (with the reason),
  or *not yet* (with the backlog item).
* **A test holds the declarations against the code**, both ways: a method
  present must be declared, and a declared one must exist. A gap can
  neither appear nor disappear unnoticed.
* **The operator is told in migkit's words when a command is not yet
  available for an engine**, and what to do instead - never a traceback.

**Filling the gaps, by engine.** Each wrap candidate is measured before it
is trusted:
* **Redis:**
  * move and sync: redis-shake (scan, sync and restore modes; clusters) or
    RIOT, with native `DUMP`/`RESTORE` as the fallback
  * fence: replication offset
  * users: ACL users and their rules - *done (2026-09-24)*: `users test`
    compares them from `ACL LIST` (passwords by their SHA-256, never
    read), `create` makes the missing ones with the same password
    through the hash, and `rollback` removes only what it made
  * schema: keyspace types, TTL policy and config parity
* **Kafka:**
  * move and sync: MirrorMaker 2, whose checkpoints translate consumer
    offsets (with item 13)
  * fence: end offsets per partition
  * users: ACLs and SCRAM credentials
  * schemas: topic configuration and the schema registry
* **MongoDB:**
  * a collection-by-collection copier (the builtin path is missing)
  * fence: resume token (the change stream itself is already wired into
    `move --mode cdc`)
  * the guard and post-load index builds
  * `mongosync` for mongo-to-mongo (item 12)
* **SQL Server:**
  * bulk move: `bcp`, or a builtin copier
  * stream: CDC or Change Tracking (delta already reads Change Tracking)
  * fence: LSN
  * users: logins and users
  * post-load: statistics
  * snapshot and rollback
  * depth still needs an x86 runner (item 27)
* **SQLite:** builtin copier, pragma parity, guard, snapshot.
* **Generic** (warehouses and the engines reached through the second
  readers): moves via item 33. Checks exist already.
* **Hetero** (cross-engine):
  * deep battery (6-ก)
  * sequence and auto-increment carried across engines
  * parameters that mean the same thing on both engines, compared as such
  * guard, post-load statistics, snapshot
  * **the cross-engine copier only ever upserts.** Measured SQLite to
    SQLite through it: a row the target held that the source does not
    was still there after the move, and a key-less table went from 2 rows
    to 4 when the move ran again. The SQL copiers empty what they are
    about to replace; this one needs the same, through a neutral
    "empty this table" every engine implements, with foreign keys handled
    the way each engine's own copier already handles them.

**`exclude` on every engine (found 2026-09-24).** Only PostgreSQL, MySQL
and MongoDB read the hop's exclude list. Measured on SQLite: with
`exclude: [audit]`, `check` still reported `audit`, and `repair` deleted a
target-owned row from it. SQLite, Redis (key patterns), Kafka (topics) and
the generic engine now read it; SQL Server still does not, and cannot be
tested on this machine (item 27).

**Transform, in scope here, means migration-time transformation:** rename,
filter, column subset and rename, type conversion and column expressions.
It must work identically through every engine's copy, check and repair.
General-purpose ETL stays out, as the owner set earlier. Whether that
still holds is an open question to the owner.

**0c. Progress and logs in migkit's own words.**

*In progress (2026-09-24):* `migkit/wording.py` holds the vocabulary.
Every bulk path now builds its plan from `Step`s, each carrying the
operator's line and the command that runs, built once; `_sh` writes
command lines to the run's `commands.log` with secrets removed, and a
failing program's message loses the program's name and keeps the
database's words. The static guard now reads `log`, `chat` and `say`
calls too, and knows ten more program names. A failed copy in `move` is a
sentence, not a traceback. The PostgreSQL dump path reports each table as
it is read and loaded (`public.orders: loaded (3 tables)`), taken from
what the programs print as they go. `test_a_real_run_names_no_program.py`
runs `move` for real down every PostgreSQL path, dry run and `--go`, and
reads everything it printed; `--help` of every command is scanned too
(it said "anything reladiff speaks"). The MySQL and MongoDB paths now
report per table too, each read from what the programs print, as measured
on the installed builds:
* **MySQL:** both programs log one JSON object per event. The reader takes
  the dump's `dump_table_progress` and the load's `restore_data_progress`
  by field, never by the wording of a message. A failure is the program's
  own error messages, not the raw tail of a machine log. The load's closing
  error count is checked even when it exits 0
  (`test_the_mysql_bulk_path_speaks.py`).
* **MongoDB:** the load's "finished restoring" line gives what landed and
  what failed for each collection. Measured: a document that failed was
  counted there, and the program still exited 0
  (`test_the_mongo_load_says_what_it_did.py`).

*One vocabulary (2026-09-25):* every bulk path's progress line now counts
against the source's own catalogue, the tables it carries and their
bytes, with the rate and the time left:

    public.orders: read (3 of 12 tables, 1.2 GB of 4.8 GB (25%), 40.0 MB/s, about 1m30s left)

This covers the PostgreSQL dump and restore, the MySQL dump and load,
and the MongoDB load. The table copier says rows the same way, with its
rate taken over the current run's own rows, not rows an earlier run
carried before a restart. Where the catalogue gives no sizes, the line
counts tables and guesses nothing
(`tests/test_one_progress_vocabulary.py`).
* **Today:** `_sh` logs every command line it runs (`$ pg_dump ...`,
  `$ mydumper ...`), and some paths log a program's own message
  (`pg_restore ignored version-mismatch SET statements`).
* **Deeper:**
  * one progress vocabulary for every path: phase, table, rows, bytes,
    rate, ETA
  * each wrapped program's own progress output parsed into it
  * the command line kept in the run's local log for whoever debugs
    migkit, never in what the operator reads
* **Guard:** a test that runs each path against a live pair and scans
  everything the operator sees for program names. The static scan cannot
  see names that arrive through a variable.

**0d. The row filter is honoured by the move and ignored by the check.**

*Done (2026-09-24), except the logging, which is 0c.*
* **The check:** before, PostgreSQL said `public.orders src=4 dst=2` and
  MySQL `missing=2 ...` `kind=rows-missing` for a correctly filtered move.
  Now every check read goes through one `_scope()` per engine: counts,
  checksums, chunk ranges, drilldown, the moved-nothing guard, and the
  MySQL second reader's `--where`. Target rows *outside* the filter are
  counted and reported separately, so narrowing the comparison hides
  nothing.
* **The copiers:** the PostgreSQL and MySQL table copiers read and replace
  only what the filter selects (MySQL doubles `%` where the statement
  carries parameters).
* **The routing:** instead of refusing the whole database, the PostgreSQL
  bulk paths leave the filtered tables out and `move` hands them to the
  table copier; every other table still goes the fast way. A path that can
  apply no filter at all still refuses, without naming a program.
* `test_the_check_reads_the_row_filter.py`: 12 tests, 8 of which fail on
  the old code. Full suite: 1391 passed.
* **Done for the pair too (2026-09-25).** A hop between engines, and
  SQLite, which copies through the pair, used to refuse a row filter.
  They now apply it:
  * The copier reads through the filter on the source. On the target it
    replaces only what the filter selects there, so the target's own rows
    outside it stay.
  * The comparison reads both sides through the filter and counts the
    target rows outside it separately.
  * The tail asks the source which of the changed rows the filter selects
    now, one read per table per batch. An insert or update outside it
    becomes a delete: a row updated out of the filter leaves the target,
    and one moved in and out within a batch ends up gone. Asking the
    source's current state rather than the change's converges the same
    way a replay does.
  * The filter is SQL for the source. For the target, sqlglot translates
    it into that engine's SQL, with each column under the name the hop's
    mapping gives it. A filter it cannot translate stops the move, the
    tail and that table's check before anything is read. A side that
    speaks no SQL (MongoDB) refuses a filter.
  * The copier written for MySQL to PostgreSQL applies the filter too. It
    used to write a column-mapped table under the source's column names;
    such a table now goes through the copier that reads the mapping.
  (`tests/test_the_pair_honours_the_row_filter.py`: `like 'ap%'` through
  a renamed column, both copiers, the tail.)

`Hop.row_filter()` has no caller anywhere in `migkit/`. Its docstring says
the predicate is "pushed into the mover's own flag and into the checksum's
`WHERE`". The MySQL mover does apply it, through `mydumper_defaults`. No
engine's check reads it.

So a filtered move is expected to be reported as missing rows for good,
which is the exact failure `refuse_unpushable_filters` says it prevents.
That refusal also names programs, through a variable the static guard
cannot see (`"pg_dump 18.6 filters by table..."`, `f"the {via} mover..."`).

It is also item 0's first real decision: route the filtered tables through
a path that can apply the predicate (the builtin copier's `_copy_select`
already takes one), send the rest down the fast path, and apply the same
predicate in every check - instead of refusing the whole database.

**0a. Wrap the Data Validation Tool as a second reader.**

DVT (`google-pso-data-validator`, built on Ibis) does:
* column aggregates, row hash, and schema validation
* custom-query validation
* partitioned runs for large tables

It reaches engines migkit does not read natively: Oracle, Teradata, Db2,
Snowflake, BigQuery, Spanner.

**Measured install (8.9.3, into a scratch venv on Python 3.12):** 34 s,
547 MB, 107 packages. It imports and its CLI starts. It **cannot share
migkit's environment**; it pins an older stack than the one migkit runs on:

| Package | migkit | DVT |
|---|---|---|
| numpy | 2.5.3 | 1.26.4 |
| pandas | 3.0.5 | 2.3.3 |
| pyarrow | 25.0.1 | 14.0.2 |
| sqlglot | 30.18.0 | 19.9.0 (through ibis-framework 7.1.0) |

So the wrap is **out of process**, the way pgcopydb runs from its own
container image:
* `doctor --install` builds it a venv of its own
* migkit drives it through a small runner inside that venv, which calls its
  Python API and hands back JSON
* its findings are translated into migkit's verdict envelope
* its name is never printed

**Read in its source (8.9.3) before measuring:**
* **Results:** with no result handler configured, DVT prints a table to
  stdout. It also ships a `postgres` result handler that **writes results
  into a database table**. Pointed at the target, that is a write to the
  target. `DataValidation(config, result_handler=...)` takes a handler
  object, so the runner passes one whose `execute(df)` just returns the
  frame: nothing printed, nothing written, and the frame goes back to
  migkit as JSON.
* **Saved state:** named connections live under the directory in
  `PSO_DV_CONN_HOME` (measured; an earlier note here said `_CONFIG_`), defaulting to one in the user's home. The runner
  points it at a directory of migkit's own for the run and passes
  connections inline.

*Measured (2026-09-24), not yet wired into `check`:*
`tests/test_second_reader_live.py`, against a live PostgreSQL pair: the
catalogue on both sides is identical before and after a count run and a
row-hash run (nothing written); equal data reads `ok`; one value changed on
the target reads `diff`. The first run found the state-directory variable
named wrong in the runner (the reader then made its default directory in
the user's home and looked for connections there); the runner now reads
the name from the reader itself, and a test pins that the home directory
is left alone.

*Runner:*
`migkit/runners/second_reader.py` runs inside the reader's own
environment, takes a job as JSON on stdin and answers JSON on stdout. It
passes a collecting result handler, so nothing is printed and nothing is
written to a database, and keeps connections in a directory made for the
run, owner-only, removed at the end.

*Wired into `check` (2026-09-24), by the planner's first rule:* a
cross-engine hop gets a second reading whenever the reader is installed
and speaks both engines. Every value is converted on the way, and
migkit's own reading was the only one looking. No flag and no option is
involved. `planned_checks()` is how an engine adds a check from what it
can see (`tests/test_second_reading_in_check.py`).

*Measured, MySQL 8 against PostgreSQL 16, the same values on both sides*
(bigint past 2^53, `decimal(12,4)`, empty string, `datetime`, `date`,
`tinyint(1)`/`boolean`, `double`):
* count, sum, min and max of every column: all equal, 3.8 s
* field-by-field comparison by key: all equal, 2.0 s
* **row hash: one false difference.** The row holding the double `-1e-07`
  hashed differently, because the two servers spell it differently before
  hashing. Hashed column by column, only the `double` column disagreed.

So the second reading runs the aggregates and never the row hash across
engines. A second opinion that reports differences which are not there is
worse than none.

Still to measure:
* its speed against migkit's own checksum at size
* where else its type handling disagrees with `canon`: other engines and
  other types (`json`, `time`, `bit`, spatial)

The planner (item 0) uses it where it sees something migkit's own reader
does not, not everywhere.

**0b. Re-snapshot through the stream without pausing it.**

*Answered already, and measured: the path does not write to the source.*

Debezium is wrapped (`movers.py`, Kafka signal channel), and `sync` asks it
to re-read the tables a check found wrong (`resnapshot_message`). Debezium
refused the standard incremental snapshot on that pipeline. It brackets
each chunk with watermark rows written to a signalling table **in the
source**, and without that table:

    Incremental snapshot is not properly configured, either sinalling
    data collection is not provided or connector-specific snapshotting
    not set

So migkit sends a *blocking* snapshot instead. Measured: 50 rows re-emitted,
`snapshot=BLOCKING snapshot_completed=true`, nothing written to the source.
The price is that streaming stops while the table is re-read.

**Deeper:** the read-only incremental variants. They take the watermarks
from the server instead of writing them: the executed GTID set on MySQL
(needs `gtid_mode=ON`), the in-progress transaction ID on PostgreSQL.
Chunks interleave with the stream, and a conflicting key keeps the
streamed event (the DBLog rule).

The planner picks per table:
* read-only incremental where the source allows it
* blocking where it does not, saying that streaming will pause and for how
  long, from the table's size and the measured rate

*Measured (2026-09-24), MySQL 8 with `gtid_mode=ON`, the connector
image shipped here, the Kafka signal channel, no signalling table
anywhere:* with `read.only=true` on the source connector, an
`execute-snapshot` signal of type `INCREMENTAL` ran. The connector logged
`Requested 'INCREMENTAL' snapshot of data collections '[app.t]'`, then
`will end at position [50]`, then `incremental snapshotting of table
'app.t' finished`. That is the same request the standard incremental
snapshot refused on this pipeline for want of a table in the source. So the
read-only variant needs no write to the source.

*Measured again at 50 rows, and done for MySQL (2026-09-24):* the initial
snapshot put 50 messages on the topic. The incremental re-read added 50
more, next to the one streamed update, for 101. A key changed before its
chunk was read came out as the new value both in the stream and in the
re-read (`49 -> -1 -> -1`). The connector stayed `RUNNING` throughout.
`stream_codegen` now sets `read.only=true` when the source answers
`gtid_mode = ON`, with a single attempt so that a source that cannot be
asked keeps the pause rather than guessing. The repair asks for the
incremental kind wherever the pipeline has it, and says whether the stream
will pause (`tests/test_reading_again_without_pausing.py`).

*Done for PostgreSQL (2026-09-25).* This was measured with the connector
shipped here (3.0.8), PostgreSQL 16, 50 rows, and no signalling table
anywhere:
* Without `read.only`, the incremental re-read was refused, the same way
  as on MySQL.
* With it, the connector read the table again beside the stream (`will
  end at position [50]`, then `finished`), taking its watermarks from the
  server's own snapshot of transactions in flight.
* A row updated before its chunk came out as the new value in the stream
  and in the re-read (`49 -> -1 -> -1`). The connector stayed `RUNNING`,
  and nothing was written to the source.
* The option is missing from the connector's advertised configuration,
  so it was measured, not looked up.

The pipeline now sets it wherever the source runs PostgreSQL 13 or later,
which is where `pg_current_snapshot()` exists. It asks once, and a source
it cannot ask keeps the pause
(`tests/test_reading_again_without_pausing.py`).

Still open: a key changed *inside* a chunk's window. At this size the
chunks finish too fast to land a write in one, and the first try at 3,000
one-row chunks took the sandbox down with it.

*Blocked (2026-09-25), with the reason:* landing a write inside a chunk
needs a table large enough that one chunk takes seconds, beside the full
Kafka Connect stack, and on this machine (a 4 GiB VM shared with other
work) that stack is the one it may not run. The measurement waits for a
machine with room for it; the code path it would exercise is the one
measured above.

## P0: correctness at cutover

**1. Confirm before calling it different.**

*Progress (2026-09-24):* the confirm pass (`fenced_recheck`,
`_resolve_inflight`) lives in the base. Each engine drives it with its own
position, fence and key compare:
* PostgreSQL: the LSN.
* MySQL: the executed GTID set (`tests/test_mysql_fence.py`).
* MongoDB: the source's cluster time, against the position migkit's own
  tail has applied up to. That position is read out of the tail's saved
  resume token, which carries its cluster time after its first byte. Both
  tails now save their position when idle too, so it says how far the
  target is even when nothing is changing. A change the tail had not yet
  applied reads `ok ... still arriving`. A change made on the target alone
  still reads `diff` (`tests/test_mongo_confirms_before_diff.py`).

*Cross-engine hops (2026-09-24), from a MySQL source:* where migkit's own
tail is running, the check waits until the tail has read as far as the
source's log is at that moment. It then walks the differing tables again,
and what converged reads `ok ... still arriving`. A difference made on the
target alone still reads `diff`. With the confirm pass switched off, the
same test reports the in-flight row as a difference.

Found on the way: the MySQL tail's position did not move past what it left
out (other databases, excluded tables), so on a server busy elsewhere it
never reached the log's end, and a fence waiting for that could never pass.
It now moves to the end of what it has read
(`tests/test_cross_engine_confirms_before_diff.py`).

*And from PostgreSQL and MongoDB sources (2026-09-24):* the PostgreSQL
tail reads its slot up to where the log is now, and once it has read that
much whole, its position moves there even with nothing in it for the hop.
It stayed at the last change, so on a quiet database a fence never passed.
The MongoDB stream's own token already moves on with the cluster, and its
cluster time is the fence. Both are measured the same way as MySQL: a
change read and not yet applied reads `ok ... still arriving`, and reads
`diff` with the confirm pass switched off
(`tests/test_cross_engine_confirms_before_diff.py`,
`tests/test_cross_engine_confirms_from_mongo.py`).

Still open:
* SQL Server, which has no change stream in migkit yet (0e).

*Replication migkit does not drive (2026-09-25):* the fence already covers
it wherever the replication shows a position:
* PostgreSQL waits for every active slot on the database, whoever owns it.
  That includes a native subscription, a connector, or a managed service's
  task.
* MySQL waits on a target that is a native replica, through the server's
  own `WAIT_FOR_EXECUTED_GTID_SET` (or `MASTER_GTID_WAIT` on MariaDB).
* A consumer that shows no position on either server falls back to the
  hop's settle time, and the verdict says it settled on time rather than
  on a fence.

* **Today:** the PostgreSQL LSN fence re-compares only after the consumers
  have flushed past a captured position, and `watch` names hot tables.
  Other engines settle on elapsed time. The verdict does not distinguish
  "still in flight" from "wrong".
* **Deeper:** every engine gets Veridata's three states, per key:
  * the first pass queues suspects
  * the confirm pass re-reads only those keys once the lag threshold has
    passed. The threshold is measured from the pipeline where migkit can
    see it (slot flush, binlog position, change-stream resume token), and
    is a hop option otherwise.
  * *in-flight* is reported as such, never as a pass or a difference
  * a key missing on both sides at confirm time counts as converged
    (pgCompare's bug)
* **Wrap:** none needed; the queue is migkit's own drilldown key files.

**2. Hold the source snapshot for a bounded time.**

*Progress (2026-09-24): measured, not yet bounded.*
* `check --consistent` says how long it held the source's snapshot, in its
  verdict and in its progress lines.
* On PostgreSQL, `assess` names the oldest snapshot on the source: its age
  in transactions, how long it has been held, and which session holds it.
  Measured with a report job holding a repeatable-read transaction open:
  the report named `report-job`, and nothing was reported once it closed.
* On MySQL, `assess` reports InnoDB's history list length, the undo the
  source has not yet purged (`tests/test_what_holds_the_source_back.py`).

*Found on the way (2026-09-24):* the consistent pass made its own list of
tables and read each one whole. Every other pass reads through the hop's
exclude list and row filters. So on a target that was exactly what the hop
asked for, a table the target owns and a table under a row filter both
came out different. It reads the same tables and rows as the others now,
in the single script and in the lanes sharing a snapshot
(`tests/test_a_consistent_check_reads_what_the_hop_asks.py`).

*The bound (2026-09-24):* the hop option `snapshot_limit` (seconds) ends a
consistent pass once a side has held its snapshot that long. The snapshot
is released, and the verdict is an error saying so rather than a verdict.
Two things found while bounding it, both measured:
* The lanes that share a snapshot so they can read in parallel were fed
  their scripts one after another, so they read in turn: 8 s where 4 s was
  due. They now run together.
* The source's exported snapshot stayed open until the target's side had
  finished too. It is now released as soon as the source's lanes are done.

(`tests/test_the_snapshot_is_held_for_a_bounded_time.py`)

*Closed by reasoning (2026-09-25):* restarting a table's pass from its
last chunk under a fresh snapshot is not a consistent pass any more. Its
chunks would come from different instants, which is the table-by-table
pass that already exists. That pass reads each table on its own, is
resumable, and is confirmed against the stream's position where there is
one (item 1). The consistent pass keeps its one instant, bounded by
`snapshot_limit`. When the bound is reached it ends with an error rather
than a verdict built from two instants.
* **Today:** `check --consistent` opens one repeatable-read transaction per
  side for the whole pass. On a large table that holds back vacuum
  (PostgreSQL) or purge (InnoDB history list) for as long as the pass takes.
* **Deeper:** a per-table time limit, after which the table's pass restarts
  from its last completed chunk. The chunk checkpoints already exist in
  `checkpoint.py`. `assess`/`doctor` report the source's `backend_xmin` age
  and `innodb_history_list_length` while it runs.

**3. Stop the application writing to the target before cutover.**

*Measured (2026-09-24), PostgreSQL 16, before choosing a mechanism:*
* `ALTER ROLE app SET default_transaction_read_only = on` stops a **new**
  session of `app`: `cannot execute INSERT in a read-only transaction`.
* An `app` session **already connected** when it lands keeps writing. Its
  setting still read `off`, and its insert went in. A pooled application
  would not notice the freeze until its connections were recycled.
* The setting is not a boundary. The application can turn it off for its
  own later transactions.

So the role setting alone is not a freeze. The candidates are:
* revoke the write privileges the hop's roles hold on the target, which
  takes effect on the next statement of every session, with the grants
  recorded first so `rollback` restores exactly those. This does not stop
  a role that owns its tables.
* the role setting plus ending the sessions already connected, which is
  intrusive
* both

*Done for PostgreSQL (2026-09-24), the owner's call: the best, smartest
way that people actually use, decided per role (`migkit/freeze.py`).*
* **On:** the hop option `protect_target: true`. `move --go` freezes before
  it copies anything. Tearing the stream down at cutover
  (`move --mode cdc --drop --go`) gives the writes back, and so does
  `rollback --apply`. `doctor` says which databases are frozen and for
  which roles, from the record, so it can say it even when the target is
  unreachable.
* **Which roles:** `app_roles`, or else the login roles present on both
  sides, which are the application's accounts carried across by
  `migkit users`. Cloud system accounts and migkit's own accounts are
  never included.
* **Per role, from the catalogue:**
  * The write grants the role holds directly are revoked, and recorded
    before anything is changed.
  * If the role can still write afterwards (it owns the table, inherits
    the privilege, or the privilege is granted to everyone), it is also
    made read-only by default in that database, and its open sessions are
    ended.
  * It says which of the two it did, for each role.
* **Thaw** grants back exactly what was revoked, and restores the role's
  own earlier setting if it had one.
* An engine without the freeze refuses before anything is copied
  (`tests/test_the_target_is_kept_from_the_app.py`: a direct grant, an
  inherited one, an owner with a session already open; all stopped, all
  given back).

*MySQL, measured on 8.4 and then done the same way:*
* A table-level revoke stopped a connected session on its next statement.
* A database-level revoke did not. A session that had already chosen the
  database kept inserting, and only new sessions were refused.
* `read_only` stopped the application and left an administrator writing.
  But it is one switch for the whole server, so a per-database hop may
  not reach for it.

So each account's write grants on the database, at both database and
table level, are revoked and recorded. Its open sessions are then ended,
so that the database-level revokes reach them. Privileges an account holds
server-wide, or through a role, cannot be taken back for one database
without taking them everywhere. Those are named and left as they are.
Live test: an account with database-level grants and a session open, one
with table-level grants, one with server-wide privileges, one writing
through a role.
* **Today:** nothing sets it. DTS has `IsDstReadOnly`.
* **Deeper:** read-only for the application's roles, not for the load:
  * PostgreSQL: `ALTER ROLE ... SET default_transaction_read_only`, applied
    only to the roles the hop names, with the undo written first
  * MySQL: `super_read_only` blocks the loader too, so this needs
    measuring before choosing
* Reported in `doctor`, reverted by `rollback`.

**4. A repair that knows the stream is running.**

*Done (2026-09-24) for what migkit drives, and refused beside what it
does not:*
* **migkit's own change tail** (the cross-engine tail and MongoDB's)
  marks itself running (`tail.pid`). It can be asked to pause
  (`migkit/tailctl.py`), and acknowledges only between batches, with
  everything it has read applied and its position saved. A row repair
  pauses it, repairs, and tells it to resume. It then replays from its
  saved position on top of the repair, by key, which is the order that
  converges. The hop option `repair_window` (default 300 s) bounds the
  wait for the pause.
* **Replication migkit does not drive**, meaning a subscription on a
  PostgreSQL target or a running replica applier on MySQL, is named, and
  the repair refuses before writing anything.
* **The generated streaming pipeline** already re-reads the table through
  the stream instead of writing beside it (0b).

`tests/test_repair_beside_a_stream.py`: a tail paused, the repair landed,
and the tail carried a new change afterwards; a real subscription stopped
the repair with nothing applied.

*The subscription migkit set up (2026-09-24):* on PostgreSQL the
subscription `migkit move --mode cdc` creates for this hop is now paused
for a repair, not refused beside it.
* It is first let catch up with the source as it is at that moment,
  through the same fence the confirm pass uses. So nothing the repair is
  about to write is still on its way, and the key conflict (an insert
  queued in the slot for a key the repair has just written) cannot come
  from the stretch being repaired.
* It is then disabled until its apply worker has gone, and enabled again
  after the repair.
* What the source wrote meanwhile arrives by key on top of the repair.

Any other subscription is still refused.
(`tests/test_repair_beside_a_stream.py`: a row lost on the target alone,
repaired while the source inserted one row and updated the repaired one;
both arrived afterwards.)

*The MySQL replica migkit sets up (2026-09-25).* It had no name to tell it
from anyone else's, so every repair beside it stopped. It signs in as
migkit's own replication account, and that account is the name. It is
paused the way the subscription is:
* First it catches up with the source as it is now, by GTID where the
  source runs it and by binary log position otherwise. A change still in
  the relay log, applied after the repair, would put back an older value.
* Then its applier stops while the receiver keeps fetching.
* After the repair it starts again, and what the source wrote meanwhile
  lands on top.

A replica signed in under any other account still stops the repair
(`tests/test_a_repair_pauses_migkits_own_mysql_replica.py`, two MySQL 8.4
servers).
* **Today:** the rule "no data repair while CDC runs" lives in the runbook,
  not in the code.
* **Deeper:** DMS's shape, where migkit owns the stream:
  1. pause the subscription or follow process
  2. repair the confirmed keys
  3. resume
* It runs inside a window with a maximum duration, as a hop option. Where
  migkit does not own the stream, the repair refuses and names the stream it
  can see.

**5. DDL during the move.**

*Progress (2026-09-24):* every move path reads the source's column
catalogue before and after itself (`migkit/drift.py`; one query on
PostgreSQL and MySQL, table by table elsewhere) and, when it changed,
stops short of "complete" and says what changed. Tables the hop excludes
are not watched. `tests/test_ddl_during_the_move.py`.

*The tail too (2026-09-24):* a binlog or a logical slot carries rows, not
a DDL the tail could rely on. The tail therefore keeps the source's column
shape beside its saved position, reads it again (one query) before each
batch it would apply, and compares.
* A change stops the tail before that batch. Its position is not moved,
  and it names what changed.
* A DDL made while the tail was stopped is seen the same way.
* A changed shape is accepted once the target has every column the source
  now has on the tables it touched. The tail then carries on from where it
  stopped.
* The working tables of an online schema change (`_x_gho`, `_x_ghc`,
  `_x_del`, `_x_new`, `_x_old`) are not carried, and their appearing is not
  a change. What the swap does to the real table is.

Measured before: a column added mid-tail arrived as an insert naming a
column the target lacked (`UndefinedColumn`), and a ghost table's rows
went to a table the target does not have
(`tests/test_the_tail_stops_at_a_ddl.py`).

*Verdicts across a DDL (2026-09-24):* `check` reads the source's column
catalogue before and after each database's checks, where that is one
query (PostgreSQL, MySQL, SQLite, and the cross-engine hops through their
source). When it changed:
* every schema answer for the database, and every count and data answer
  about the database as a whole or about a table the DDL touched, is taken
  back. Its status becomes `skip`, the record says what changed and what
  it had read, and `verdict.json` is `incomplete` with the scopes under
  `stale`. Answers about the tables it did not touch keep their verdicts.
* the run exits non-zero and says to check again once the change has
  settled; `has_differences` stays false, since no difference was found.
* `check --resume` keeps the shape it ended on, and reuses nothing from a
  run the source's schema has changed under since.

Measured before: a column added between the count pass and the data pass
left `schema main: OK ... identical` in the verdict. On PostgreSQL, where
a clean database is one data record, a column added after the data pass
ended in `all green`. A `--resume` after a column was added reused the
schema check's `OK` (`tests/test_verdicts_a_ddl_overtook.py`).

*MySQL native replication kept to the hop (2026-09-24):* the replica
migkit set up had no filters. Measured on 8.4:
* a write into a database the hop does not name arrived on the target
* a row for a table the hop excludes was applied, and stopped the replica
  on a key the target already had

The plan now scopes the replica with filters in the target's names: the
hop's databases (`REPLICATE_WILD_DO_TABLE`), the renames its `db_map`
makes (`REPLICATE_REWRITE_DB`), and the tables it excludes
(`REPLICATE_IGNORE_TABLE`). With them, neither write arrived, and neither
did a DROP of the excluded table.
* **Restarts:** the filters do not survive one. Measured, the replica came
  back by itself with none, set for the channel or not. So the plan names
  the configuration lines, and the replica's status says `NOT limited to
  this hop` when they are missing.
* **Managed and MariaDB targets:** a managed target is told the parameter
  group settings, and MariaDB is given its own statements.
* **One replica per run:** a hop of several databases now sets up one
  replica, with one password. It used to set one up per database: the
  second failed on a running replica, and gave the replication user a
  password the first no longer had.

(`tests/test_the_native_replica_stays_in_the_hop.py`)

*Which schema changes a native replica applies (measured 2026-09-25, 8.4,
under the plan's own filters):*
* **Same database name on both sides:** every table, view, routine,
  trigger and index statement in the hop's database arrived, including
  one naming its table with the database while another database was in
  use. A table in another database did not arrive, and neither did an
  account.
* **A renamed database:** the replica's rewrite applies to the database
  in use, not to one a statement names. `alter table cx.t add column d`
  and `create table cx.t3`, run with another database in use or none,
  never reached the renamed target. The replica kept running. The next
  row arrived as `7 1 2`, without the value of the column that never
  arrived, and nothing reported an error.

So a hop that renames a database is not given a native replica. `move`
says why and points at migkit's own change tail, which stops on a
schema change rather than carrying half of it. This is the same rule a
hop that maps columns already had
(`tests/test_which_schema_changes_a_native_replica_carries.py`).
DTS-style allow-lists of DDL kinds are left to the tail, since a native
replica filters by database and table, never by the kind of statement.

* **Today:** a DDL on the source mid-move is invisible until the next
  schema check.
* **Deeper:**
  * detect it in the window, from the replication stream where migkit
    reads one, and from catalogue snapshots otherwise
  * mark every data verdict taken across it as stale; migration-verifier
    fails permanently for the same reason
  * for MySQL native replication, carry DTS's allow-list of DDL kinds
  * recognise online schema-change temp tables (`_gho/_ghc/_del`,
    `_new/_old`) and the final `RENAME`, instead of reporting them as
    strangers

**5b. MySQL to MySQL full+cdc: the copy and the replica start from
different points.**

*Done (2026-09-24) for the tail; native replication still open.*
* **Measured, before:** `move --mode full+cdc --go` copied and printed a
  replica plan, with the binlog position taken before the copy. Nothing
  started the replica. When it was started by hand on 8.4 from that
  position, it stopped on the first row the copy had already carried:
  `Replica_SQL_Running: No`, `Last_SQL_Errno: 1062`. A row written after
  that never arrived. The copy is not a snapshot at any position, so no
  position taken beside it is right for a strict replica.
* **Now:** the same hop runs the change tail that the cross-engine hops
  use, as a pair of MySQL with itself. It starts from a position taken
  before the copy and applies by key, so the stretch the copy and the log
  share converges instead of stopping. Any engine with its own change log
  and an applier gets this. `tests/test_mysql_full_cdc.py` (fails on the
  old code).
* **`--mode cdc` after a separate `--mode full` (2026-09-24):** every full
  copy now records when it ran, and where asking costs the source nothing
  (MySQL, MongoDB) where the log was before it (`copy-position.json`). The
  MySQL bulk copy also records the position its dump is a snapshot at,
  from the dump's own metadata. `--mode cdc` then:
  * starts a native replica exactly at the dump's position, by file and
    position even with GTID on, since the target's GTID set does not hold
    the source's. Measured on 8.4 with GTID on: an insert and an update
    made after the dump arrived, and the replica kept running.
  * starts a change tail from the position taken before the copy, and
    replays by key. A row written between the copy and the tail used to be
    carried by nothing; now it arrives.
  * says plainly, on PostgreSQL, that a subscription made now carries only
    what changes from now on, and points at `--mode full+cdc`.
  * uses a recorded position once, since a second stream from it would
    replay what the first applied.

  `tests/test_cdc_after_a_separate_copy.py` and
  `tests/test_full_cdc_misses_nothing.py`.
* *Closed by reasoning (2026-09-25):* a native replica after a copy that
  was not a snapshot at one position (the table copier's) cannot be
  right. A strict replica stops on the first row the copy already
  carried (measured above: 1062), and no position makes it right. The
  tail replays by key from before the copy and converges, and that is
  what `--mode cdc` gives such a copy.

## P1: before the move - one assessment that answers every managed service's list

**6. Pre-checks with the value to set, not just the verdict.**

*Progress (2026-09-24):*
* **Every engine:** `assess` gives the number of tables in scope, warning
  above 10,000 with the advice to split the hop. It fails on table names
  that differ only by case, since a target that folds case keeps one of
  each pair.
* **PostgreSQL:** `max_replication_slots` and `max_wal_senders` are checked
  against what is in use plus one per database in scope, and the
  number to set is given.
* **MySQL:** each binlog requirement comes with the statement to run. This
  now includes `binlog_row_metadata=FULL`, which the change tail needs, and
  the RDS form of the retention setting. `server_id` is checked, and so is
  `gtid_mode` without `enforce_gtid_consistency` (not on MariaDB, which
  has no `gtid_mode`) (`tests/test_assess_says_what_to_set.py`).

* **Binlog compression (MySQL and MariaDB):** measured, MySQL 8.4 with
  `binlog_transaction_compression = ON` and MariaDB 11 with
  `log_bin_compress = ON`. An insert, an update and a delete went into the
  binlog compressed, and the change reader skipped all three: no change,
  the position not moved, nothing said. A tail on such a source called
  itself caught up for as long as it ran. Now:
  * the tail stops on the first compressed event and says which setting
    wrote it. The position stays where it was.
  * `assess` fails either setting with the statement to run.
  * An application session that turns compression on for itself, which
    `assess` cannot see, stops the tail too.
  * *Read, not stopped on (2026-09-25):* MySQL's compressed transaction
    is now opened by migkit itself (`binlog_payload.py`). The header's
    fields are read up to the end mark, the payload is decompressed with
    zstd, and each event inside is handed to the reader's own parser as
    if it had arrived on its own, with table maps updating its table
    map. Measured on 8.4: an insert, an update and a delete in a session
    that turned compression on, and an insert with the server's own
    setting on, all reached the tail in order with their values, and the
    delta verified the rows. `assess` passes the setting. MariaDB's
    compressed row events are another format, and still stop the tail
    and fail `assess`
    (`tests/test_a_compressed_binlog_is_read_or_stops_the_tail.py`).
* **The largest row against the target's `max_allowed_packet` (MySQL):**
  measured on 8.4. An 8 MiB value loaded into a target with a 4 MiB packet
  stopped the copy with `Lost connection` and nothing else. An 8 MiB value
  of zero bytes stopped a load that a 9 MiB packet carried when the value
  was letters: the loader escapes binary, and a zero byte comes out twice
  as long. Now:
  * `assess` reads the largest row's large columns per table. It skips any
    table whose file on disk is too small to hold a row over the limit,
    and it spends at most `lob_scan_seconds` (default 60) on the whole
    source. Anything it did not reach is `unknown, not clean`. A
    compressed table's file is no bound on its values (measured: 73,728
    bytes on disk for an 8 MiB value in `ROW_FORMAT=COMPRESSED`), so a
    table with row, page or column compression is always read.
  * It fails a row the packet cannot carry and warns on one that fits only
    unescaped. Both give the value to set: twice the row plus 2 MiB, in
    whole MiB. That value carried the zero-byte row through a real move.
  * A move that stops with a lost connection says the likely cause.
  * `tests/test_the_largest_row_fits_the_packet.py`.
* **PostgreSQL key-less tables that replication cannot match:** done
  earlier (`tests/test_rows_a_replica_cannot_match.py`).

* **Other replication writing into the target:** `assess` names a
  subscription on a PostgreSQL target, or a running replica applier on a
  MySQL one, and fails the item: two writers on one table race, and
  whichever lands last survives. DTS fails such a task. On an engine
  whose target cannot be asked, the item is left out rather than passed
  (`tests/test_assess_names_other_writers.py`).

*`logical_decoding_work_mem` (2026-09-25):* a transaction too large for
it is written to the source's disk while it is decoded, and read back.
Measured on 16, for a 3.3 MB transaction:
* at 64kB it spilled 3,460,000 bytes
* at 4MB it spilled nothing
* at 2MB it spilled again

`assess` now reads the server's own count (PostgreSQL 14 and later) and
names the slots that spilled. The value it gives is the next power of two
at least the average spilled transaction, and never less than twice the
current setting (`tests/test_decoding_that_spills_is_named.py`).

*Target storage with the log included (2026-09-25):* the plan's room now
counts the log the target holds at once while it loads, from the target's
own answer. Measured on PostgreSQL 16 with `max_wal_size = 64MB`, loading
a 355 MiB table in one statement:
* it wrote 411 MiB of WAL, and `pg_wal` peaked at 80 MiB, since
  checkpoints recycle the rest
* with one inactive slot on the target, `pg_wal` peaked at 416 MiB: all
  of it

So a PostgreSQL target is counted at 1.25 × `max_wal_size` plus
`wal_keep_size`, or at all of the WAL once it has a slot. A MySQL target
that keeps a binlog holds all of it until `binlog_expire_logs_seconds`
(30 days by default). The "NOT ENOUGH" warning compares against the
tables, the indexes and that log together
(`tests/test_the_room_includes_the_log_the_target_keeps.py`).

Still open: `wal_sender_timeout`, only with a measured reason for a
value.

*Left open on purpose (2026-09-25):* no measurement here has shown a
value that is wrong for a migration. The default (60 s) ends a sender
whose receiver has said nothing for that long, and migkit's own tail
reads through SQL rather than a sender, so it is not one of those
receivers. A recommendation waits for a failure that shows what the
value should be, rather than a number copied from a guide.

*Found on the way, and done (2026-09-24):* a MySQL `move --mode full` onto
a target without the tables stopped at the load on `ERROR 1146 ... doesn't
exist`. The bulk path loads data only, and nothing created what the target
lacked; the cross-engine copier did. The move now
creates exactly the tables the target does not have, from the source's
`SHOW CREATE TABLE`:
* in one session with foreign key checks off, so a child can be created
  before the table it references
* under the sql_mode a schema dump uses, so a definition the source
  accepted is accepted (measured: a zero-date default a strict target
  refuses when typed in plainly)
* the database too when it is missing, in the source's character set and
  collation

A table the target already has is left as it is, and so is one the hop
excludes. The plan says how many it will create
(`tests/test_the_mysql_move_creates_what_the_target_lacks.py`). Views,
routines, triggers and events are still `migkit schema`'s plan.

*Found on the way, and done (2026-09-24):* on PostgreSQL the dump path
called a move onto a target without the tables `bulk copy complete`, with
nothing loaded. Two causes:
* The load's exit 1 was tolerated whenever it ended in `errors ignored on
  restore`. Only a setting the server lacks is tolerated now; any other
  refusal stops the move with the server's words.
* The guard after the move asked only about tables both sides have. It
  now names a source table with rows that the target does not have, on
  PostgreSQL and MySQL (`tests/test_the_restore_says_what_it_refused.py`,
  `tests/test_move_moved_something.py`).

*And then done (2026-09-24), with two more found on the way:*
* **Tables the target lacks.** Both PostgreSQL bulk paths create them
  from the source's definition before the load, and add their keys,
  indexes and constraints after it, which is also the faster order.
  * A target with no tables at all gets every object in the source's
    pre-data (types, functions, sequences, tables, views) except the
    tables the hop excludes.
  * Otherwise only the missing tables are created.
  * The objects are created without owners or grants.
  * What was created is recorded first, so a load that fails still gets
    its keys added on the next run.
* **The streaming copy and foreign keys.** Measured: it empties each
  table it loads by itself, and a target with a foreign key refused with
  `cannot truncate a table referenced in a foreign key constraint`. So
  every schema with a foreign key failed on the default path. Such a
  target now goes through the local copy, and says why.
* **The streaming copy and sequences.** Measured, 52 rows copied: the
  target's sequence stayed at 1, and the first insert failed on
  `duplicate key ... (id)=(1)`. The copy program's own sequence command,
  run alone, read 0 sequences and reset the target's. The engine's
  sequence repair now sets them after the copy.

`tests/test_the_postgres_move_creates_what_the_target_lacks.py`. Still
open: a foreign-key window, dropping and restoring the keys around the
streaming copy the way the index window does. That keeps its speed on such
targets instead of falling back.

*Found on the way, and done (2026-09-24):* the PostgreSQL dump path's load
could only be run by a superuser. As a plain owner, the foreign keys'
system triggers refused the restore's switch, and the child rows were
refused on the key. That was 100 of 100 rows missing, reported as complete.
* A superuser keeps the switch.
* A user allowed `session_replication_role` loads as a replica.
* A user with neither is stopped before anything is emptied, with the
  grant named.

(`tests/test_the_load_keeps_triggers_quiet_without_superuser.py`)

*Found on the way, and done (2026-09-24):* the target's triggers fired for
rows loaded by the PostgreSQL table copier and by both MySQL paths. A
`BEFORE INSERT` stamping `now()` rewrote every row, and the move said
complete.
* PostgreSQL's copier now loads as a replica, as the bulk paths do.
* MySQL, which has no such mode, takes the triggers off for the load with
  their definitions saved first, and puts them back after. It refuses to
  start where it could not put one back as its definer (C6 in the problems
  file).
* The same held for everything else that writes rows onto a target: the
  change tail, the pair copier, and row repairs, in both directions. That
  is now done the same way:
  * PostgreSQL writes as a replica.
  * MySQL takes the triggers off for the tail's lifetime.
  * A per-process record of what was taken off lets `check` name what a
    killed process left off, and the next load puts it back.
  * SIGTERM stops the tail cleanly.
* ~~Left open: the index windows' `dropped-indexes.json` has the same
  crash hole.~~ Done (2026-09-25). The trigger and index windows now share
  one record (`migkit/setaside.py`), with one file per load. A later load
  builds what a dead one dropped and the target still lacks, including on
  a table where it has nothing of its own to drop
  (`test_index_window_pg.py`).

*Found on the way, and done (2026-09-24):* the MySQL bulk load left the
target's binlog without its rows. The loader turns the binlog off for its
own sessions by default. Measured with a replica following the target: it
got the tables and none of their 300,000 rows. A point-in-time restore of
the target would have missed them too. The load now writes the binlog
wherever the target keeps one
(`tests/test_the_targets_replicas_get_the_load.py`).

*Found on the way, and done (2026-09-24):* the one-pass cross-engine
load was chosen for every cross-engine hop once installed. The load file
migkit writes for it reads MySQL into PostgreSQL, the whole database, and
nothing else, so:
* any other pair was read from the wrong server as the wrong kind
* a hop with an exclude list had the target's own tables loaded over
* a table, column or row mapping was ignored

`movers.fitted` now keeps it to the hops it can carry and sends the rest
through the table copier, saying why
(`tests/test_the_one_pass_load_is_fitted_to_the_hop.py`; the load itself
is not installed here, so its own behaviour is not measured).

*Found on the way, and done (2026-09-24):* the MySQL load changed values
to fit instead of stopping on them. Measured:
* `'z'` became `0`, `'12345678901'` became `'12345'`, and `'2026-13-45'`
  became `0000-00-00`
* a source date `2026-00-15` became `0000-00-00` as well
* the table copier cut text to fit on a target whose own mode was lax

Every write to a MySQL target now runs strict, through the dump's session
and every target connection (`MySQLEngine.WRITE_SQL_MODE`). What a lax
source legitimately holds still lands as it is
(`tests/test_the_mysql_load_is_strict.py`).

Merge the AWS, Google, Azure and DTS lists into `assess`. Each check prints
the value to set, computed from what the server reports. For example,
`max_replication_slots >= databases in scope + slots already in use`.

Items not already present (each confirmed by grep before it is written):

| Area | Checks to add |
|---|---|
| PostgreSQL | `logical_decoding_work_mem`, `max_slot_wal_keep_size`, `wal_sender_timeout` |
| MySQL | binlog transaction compression, binlog retention, `binlog_row_image`, `server_id`, `max_allowed_packet` against the largest LOB actually stored, `gtid_mode` / `enforce_gtid_consistency` mismatch |
| Any engine | tables differing only by case; ARRAY/JSON/XML/point columns on key-less tables (DTS fails those outright); other replication already reading the same objects (multi-task conflict); more than 10,000 tables in scope |
| Target | storage needed, WAL included; HA or read replicas on during the load |

**7. What the move will cost (A6, *Not yet*).**

*The time part, from measurement (2026-09-24):* every finished copy
records the rows it carried and the time it took, per path and per hop
(`throughput.json`). The dry run of the next move divides the rows it is
about to carry, from the source's own estimate, by the latest rate
measured on the same path. Before any copy has been measured, it says so
and gives no number (`tests/test_time_from_measured_rates.py`). Transfer
and storage cost are still open.

*Size and room (2026-09-24):* the plan also says how much the move
carries, from the source's catalogue. It gives the tables' and indexes'
size on disk for the tables it carries (not the ones the hop leaves
alone), the room the target needs for them, and what the target's log
grows by while it loads.
* The log figure is the measured rate: PostgreSQL wrote 79 MB of WAL
  loading 78 MB of table and index, and MySQL a 42.8 MB binlog loading
  52 MB of table data.
* Measuring the MySQL figure found the load was not writing the target's
  binlog at all (item 6, and C16c in the problems file).

(`tests/test_the_plan_says_the_size.py`)

*Transfer cost (2026-09-25):* migkit cannot read a price. The hop option
`transfer_price_per_gb` gives one, and the plan then says what carrying
the tables' rows across costs at it. It prices only the rows, because the
indexes are built on the target. Without the option, the plan names no
cost. Still open: the target's free space, which neither engine reports
over SQL.

*Free space (2026-09-25):* the plan now says what the target has free,
where the server reports it. MongoDB does (`dbStats`), and the plan says
`NOT ENOUGH` when the move needs more; measured against `df`, the two
agreed within a few MB. PostgreSQL and MySQL do not report free space,
and the plan does not guess it. A MongoDB move's plan had no size line
at all before; the collections' own statistics now give it one
(`tests/test_the_plan_says_the_targets_free_space.py`).

Bytes per table are already known. Add the transfer cost for the network
path the hop uses, the target storage, and the time from the measured
throughput of the chosen path.

## P1: verification the operator can extend

**8. Business-rule and aggregate checks (D7, *Partly*).**

*Done for the SQL engines (2026-09-24), `migkit/rules.py`:* the hop option
`rules` holds named SQL, either one statement for both sides or a
`{source, target}` pair. `check` runs them whenever the hop has any, with
no flag.
* Each runs on both sides in a transaction that cannot write. A rule that
  tries to write fails as an error, and the source is untouched
  (PostgreSQL's session is read-only, MySQL's transaction is
  `read only`, SQLite's file is opened read-only).
* Answers are compared by value, not text: `1.5000` meets `1.5`, `3` meets
  `3.0`.
* Rows are compared as a set, and a difference names the rows found on
  one side only.
* Cross-engine hops run each side on its own engine
  (`tests/test_business_rules.py`).

*MongoDB (2026-09-25):* its questions are aggregation pipelines, so a rule
there is `{collection, pipeline}`. Each document is a row, with its values
in the order the pipeline writes them and a grouped `_id` spread into its
fields. That is the same shape a SQL `group by` gives on the other side of
a pair, and `1.50` and `1.5000` as Decimal128 still meet.

MongoDB has no transaction that cannot write, so a pipeline holding
`$out` or `$merge` anywhere is refused before it runs. SQL handed to it
says what a MongoDB rule is (`tests/test_business_rules.py`).

A hop option holding named SQL pairs (`sum(amount) by day`, `count by
status`). Both sides run them and the results are compared through `canon`,
so a cross-engine pair compares values rather than text. DVT's custom-query
validation is the model.

* **Wrap candidate:** DVT (`google-pso-data-validator`). It is built on Ibis
  and reaches Oracle, Teradata, Db2, Snowflake and BigQuery. Its dependency
  set is heavy (the Google Cloud clients and a pinned Ibis). Measure the
  install and its write behaviour first, and use it as an independent
  cross-check for engines migkit does not read natively, the way
  `MIGKIT_CROSSCHECK` uses the second PostgreSQL reader today.

**9. Column subset and column rename in the mapping.**

*Done where columns are paired by name (2026-09-24):* `mapping.columns`
takes, per table, `keep` (only these), `drop` (all but these) and
`rename` (source name to target name), matched by suffix like every other
name in the hop.
* One function (`_mapped_types`) applies it for the cross-engine copier,
  for the table it builds on the target, for the comparison and for the
  schema check. SQLite, whose copies go through the same copier, compares
  its mapped tables through it too.
* A renamed column is read under its source name, and written and compared
  under its target name. A dropped column is neither carried nor reported
  as missing (`tests/test_column_mapping.py`).
* PostgreSQL and MySQL hops copy and hash whole rows on their own paths,
  so there the mapping is refused before anything is copied or compared,
  rather than ignored.

*Done on PostgreSQL's and MySQL's own paths (2026-09-25):* a table whose
columns the hop maps goes through the pair machinery. That machinery
already reads the mapping in one place, so none of it is written a second
time for each engine. Every other table keeps the fast path.
* The plan routes it out of every bulk copy (the dump, the streaming copy
  and the MySQL dump), the same way it routes filtered tables. The pair's
  copier builds it under the mapped names if the target lacks it, and
  fills it.
* `check` compares it through the pair: data, with drilldown, and schema,
  column by column. The whole-database schema comparers leave it out:
  * the dump diffs, with `--exclude-table` and `--ignore-table`
  * the structural differ, whose inspected objects are filtered
  * the object inventory
  * the external schema comparers, through their exclude options, or
    item by item from their report
  The schema-aware comparison's authority no longer demotes the pair's
  findings, because it never read those tables. The test caught this: a
  column dropped from a mapped table was waved through as "cosmetic".
* `sync --kind rows` plans and applies the pair's repairs for it. Writers
  are still paused around the repair, as for any row repair.
* `--mode cdc` follows the hop with the pair's change tail, which now
  applies the mapping to every change. The server's own replication
  carries whole rows under the source's names, so it is refused for such
  a hop, with the reason.
* The tail used to stop on the first change to a mapped table, on
  `Unknown column 'name'`, cross-engine hops included. If the target had
  both columns, it would have written the wrong one.
  (`tests/test_column_mapping_on_own_paths.py`)

Left open:
* ~~The pair's copier builds a missing mapped table from neutral
  classes.~~ Done (2026-09-25). Between two servers of one engine, the
  table is built with every column's type exactly as the source wrote it.
  Through the classes, a `timestamptz` had been built as `timestamp(6)`,
  with its offset gone, and an `int` as `bigint`. It keeps NOT NULL,
  defaults, identity and the source's indexes (see below).
* ~~`watch` and the delta loop still hash whole rows on the own paths.~~
  Done: the delta loop sends a mapped table's comparison to the pair. The
  pair walks the whole table rather than only the keys that changed,
  which is slower and still right. `watch` reads only row counts, so the
  mapping does not change anything there.
* *Found on the way, and done:*
  * The MySQL delta loop read the binlog with the same reader as the tail.
    It said `0 changes since last verified position` over a compressed
    transaction, then moved its position past it. It now stops where it
    is, names the setting, and does not advance.
  * Both delta loops compared the tables the hop excludes. Nothing else
    compares those tables, so they left a difference nothing would clear.
* ~~The own table copier (`MIGKIT_MOVER=builtin`) fills tables and
  creates none.~~ Done: a whole-database copy creates what the target
  lacks, the same way the bulk paths do. On PostgreSQL the keys and
  constraints are added after the rows.
* ~~The whole-database schema comparers do not leave out tables the hop
  excludes.~~ Done: an excluded table is the target's own. Every schema
  comparer now leaves it out, and so does the fix DDL. On a target that
  kept its own differently-shaped `audit`, the check used to report a
  difference nothing could clear, and the fix would have rebuilt the
  table in the source's shape
  (`tests/test_the_schema_check_leaves_what_the_hop_excludes.py`).

*Found on the way, and done (2026-09-25):* passwords on command lines. The
guard written for the MySQL bulk path covered one file. The same mistake
was in five other places:
* the MySQL schema dump took `-p<password>`
* the table sync took `p=<password>` in both connection strings
* the object comparison took both passwords as flags
* the schema comparison took both URLs, passwords inside
* the SQL Server client took `-P`

Each was measured before it was changed, and each now hands the password
over another way:
* the environment: `MYSQL_PWD`, the comparison's own password variables,
  `SQLCMDPASSWORD`
* a private defaults file named by `F=`
* a config file that reads the URLs from the environment

The streaming copy's container took both connection URIs as `-e
NAME=value` on the docker command line. It is now given `-e NAME` only,
and docker takes the value from migkit's environment (measured). The
guard now covers every module and every way a program is started
(`tests/test_no_program_is_handed_a_password.py`). The table sync's plan
also no longer names the program.

*Found on the way, and done (2026-09-25):* sequences behind the rows.
The table copier writes each row with the source's key, and a PostgreSQL
sequence does not move for a key it did not hand out. Measured, through
PostgreSQL's own copier and through the copier between engines: the first
insert after the copy failed on `duplicate key value violates unique
constraint`. The change tail between engines does the same with every row
it inserts, and a cross-engine hop had no sequence check at all. Now:
* Every copy ends by settling the target. Where the source is also
  PostgreSQL, the source's sequence values are carried. From any engine,
  each sequence that owns a column is raised to that column's largest
  value; a sequence is only ever raised, never lowered. The statistics
  are then analysed, as before.
* A cross-engine hop into PostgreSQL has the `autoinc` check. It is asked
  of the target alone and names each sequence that would hand out a key a
  row already holds. `sync --kind sequences` raises them. A target whose
  counter moves with every insert (MySQL) has no such check to run.
(`tests/test_the_target_is_usable_after_the_table_copier.py`)

*Found on the way, and done (2026-09-25):* tables built on another engine
lost their columns' rules. Measured, a MySQL table built on PostgreSQL:
`status varchar(10) not null default 'new'` arrived as `status
varchar(10)`, and `id int auto_increment` arrived as `id integer`. After
cutover, an insert that left out `status` stored NULL, and one that left
out `id` was refused. The cross-engine schema check compared only names
and kinds, so it called the two tables the same. The copier from MySQL to
PostgreSQL built no tables at all, and onto an empty target it stopped.
Now:
* A built table keeps NOT NULL.
* It keeps its defaults. Each is translated into the target's SQL and
  asked of the target first; one the target refuses is named.
* It keeps the engine numbering its key: an identity on PostgreSQL,
  `auto_increment` on MySQL.
* It gets the source's indexes, unique ones included, once its rows are
  in. Before this, a built table had none: every query read it whole, and
  a unique index the source kept was not there to refuse a duplicate. An
  index over an expression, a predicate or a column prefix is named, not
  carried.
* The check reports a lost identity, default, NOT NULL or unique index,
  both ways.
* Every copy between engines builds the missing tables first.
(`tests/test_tables_built_across_engines_keep_their_rules.py`)

*Found on the way, and done (2026-09-25):* orphans behind a validated
foreign key. PostgreSQL's orphan scan read only NOT VALID keys, assuming a
validated key cannot have orphans under it. But every migkit load writes
as a replica or with the triggers off, and a foreign key is a trigger. Row
filters and excluded parents make orphans possible. Measured: under a
filter that kept one parent and both children, the orphaned child sat
behind a validated key, and the check read "all fk constraints validated,
no orphans possible". Every key is now scanned, each within
`fk_scan_seconds`. One that runs out of time is reported as unknown, not
clean. A cross-engine hop scans its target the same way
(`tests/test_orphans_behind_a_validated_key.py`).

`mapping` reads only `where` and `tables` today. Add `columns` - keep or
drop, and rename - read by the mover, the check and the repair alike. The
DTS and DMS transformation rules are the model.

**10. Newer-row-wins conflict policy.**

*Done for PostgreSQL and MySQL (2026-09-24):* the hop option
`newer_wins: <column>` splits the changed rows. Where the target's value
in that column is later than the source's, the target row is kept, and it
is listed in `data-<table>.kept-newer`. Every other changed row is
overwritten from the source, including a row whose column either side has
empty, because the source is the truth when the rows cannot say otherwise.
A column missing from either side refuses the table before anything is
repaired. `--on-conflict keep-target` still keeps everything it was asked
to keep. The source side is read in a read-only session
(`tests/test_the_newer_row_wins.py`).

`--on-conflict` has `source-wins` and `keep-target`. Add a hop option for
DTS's `ConditionCover` and pglogical's `last_update_wins`: compare a named
column and keep the newer row. It needs a column both sides maintain; the
refusal says so when there is none.

## P2: reach

**11. Oracle (plan 2).**
* Reader: `python-oracledb` in thin mode (no Instant Client).
* Schema: Ora2Pg's export and its `TEST` / `TEST_DATA` comparisons, wrapped
  and measured.
* DVT as the cross-check.
* Plan 20, PL/SQL conversion with behavioural proof, stays last.

*Written, not yet run against a server (2026-09-25).* Oracle is a side
of a pair through `python-oracledb` in thin mode, with no Instant Client.
It sits on the base every engine read through a DB-API driver shares
(`engines/dbapi.py`, the one SQL Server now uses too). That base covers:
* reads resumed by key, with the row comparison spelled out
* writes that delete by key and insert in one transaction
* digests folded in-process through the shared renderer
* created tables, and read-only rules

What is Oracle's own:
* **Names.** A name in capitals is said in small letters, and one in
  small letters is written in capitals, so `orders` meets `ORDERS`.
* **Values.** A `CHAR` is compared without its padding. A `NUMBER(p, s)`
  is read back at its declared scale, since Oracle keeps no trailing
  zeros (`12.50` is `12.5`), and numbers are fetched as decimals, never
  as floats. A `DATE` is a timestamp, because it holds a time of day. A
  time with a zone is left out rather than rendered without its zone.
* **Carried code.** A PL/SQL function whose body is one `RETURN` is
  carried across the pair through the translator. Anything longer is
  named.
* **Deep check.** Objects a load left invalid on the target, and
  constraints left disabled or not validated.

Held without a server (`tests/test_oracle_as_one_side_of_a_pair.py`):
the values the driver hands back digest as PostgreSQL digests the same
rows in the server, including a padded `CHAR`, a `DATE` with a time and
a `NUMBER(10,2)` that kept no trailing zero. The name folding, the
driver's `:1` parameters and the PL/SQL carrying are covered too.

Oracle Free could not be run here. Measured, it filled the container
VM's disk and took 2.5 GB of its 3.8 GB of memory, and was stopped and
removed. Ora2Pg and DVT are still to wrap, and the change stream
(LogMiner) is still to come.

**12. MongoDB to MongoDB with `mongosync` underneath.**

A mover for the only pair where the vendor ships one. MongoDB's
migration-verifier is a cross-check candidate. Measure where it writes its
metadata before trusting it: it must be neither the source nor the target
of the migration.

*Done (2026-09-25):* measured with mongosync 1.21 between two MongoDB 7.0
replica sets. The build is `mongosync-macos-arm-arm64-1.21.0.zip`, signed
by MongoDB.


*Found on the way (2026-09-25):* the sync program sends telemetry to its
vendor unless told not to, and it wrote its metrics into whatever
directory it was started from. Measured: a `metrics/` directory appeared
in the directory the test ran in. It now runs with telemetry off, its
logs and metrics in the hop's reports, and the reports directory as its
working directory. The test starts it from an empty directory, and that
directory stays empty.
**What it needs, and what it writes.** Started plainly, it refused: it
wanted a user on the source so that it could turn on write blocking
there. Started with `preExistingDestinationData` and the hop's database
as its only namespace, it:
* asked for no user on the source
* wrote nothing to the source: no database or collection appeared there
* kept its own bookkeeping in `__mdb_internal_mongosync` on the target
* carried 3,000 documents, and an insert and an update made while it ran
* kept NumberLong and Decimal128 as they were
* committed

**How migkit runs it.** It is the MongoDB bulk path once installed,
because it copies online and is consistent as of its commit, which a
dump of a live source is not. migkit:
* drops the target's collections in scope first, because it refuses a
  collection that exists, even an empty one
* gives it both addresses in a private configuration file
* lists it while it runs, so a killed move leaves nothing unaccounted for
* stops it after the commit

It gives way to the dump where the hop or the servers cannot take it:
* a database mapped to another name
* a side that is not a replica set
* a server older than 6.0
* a server that cannot be asked

`doctor --install` fetches the build from the vendor for macOS and the
Linux systems the vendor publishes for, and checks its `--version`.

The option reader missed the program's own help shape (`--config value`,
one space), and found 5 of its 15 options. It now reads that shape, and
every other program's count stayed the same.

The tests are in `tests/test_mongodb_moves_online_through_the_sync.py`.

*Found on the way:* the MongoDB dump and load were handed `--uri` with
the password in it. So were the MySQL and generic comparison runs
(addresses), and the PostgreSQL cross-check (URIs). The guard missed all
of these, because it looked at the call and not at the variable the
command line was built in. It now follows variables and helper
functions. The passwords now go in:
* a private `--config` file, for the MongoDB programs
* the comparison run's own file, for the comparisons
* a password file, for the cross-check

(`tests/test_no_program_is_handed_a_password.py`,
`tests/test_the_mongodb_path_keeps_its_password_off_the_command_line.py`)

**13. Kafka offsets across a cutover (E2).**

Compare against MirrorMaker 2's checkpoint translation rather than raw
offsets, which differ by design.

*Done (2026-09-25): offsets translated by the message they point at.* A
committed offset is the next message a group reads. Where the two logs
do not line up, the same number is a different message. Such a partition
used to be named and left alone. Now the message itself is found on the
target:
* first by the time it was written (`offsets_for_times`)
* then by its key and value, scanning at most 1,000 messages forward

The group is compared with, and repaired to, the offset where that
message is. A group at the end of the source's log goes after the
source's last message. Where the message is one of several the same, the
first is taken, so a consumer reads one message again rather than
skipping one. Where the message is not on the target at all, the
partition is still named and left alone.

MirrorMaker 2 translates through its own offset-sync records and only at
the intervals it writes them. This asks the two logs themselves, so it
works for any copy that kept the messages' times: MirrorMaker, a
connector, or migkit's own.

Measured with two clusters:
* the target's partition began with seven messages the source never had,
  then the source's twenty
* a group at 12 on the source was found at 19 on the target, set there,
  and read `message 12` first
* a group whose next message only the source had was refused

(`tests/test_kafka_offsets.py`)

## P2: the plan items still open

* **7:** what the target does to rows as they land
* **10:** documents the table only points at
* **12:** LOBs, correctness then speed
* **13:** a cutover runbook migkit drives - freeze, delta, verify,
  sequences, flip. Items 1-5 above are its parts. *Written (2026-09-25):*
  `docs/cutover.md`, the order of the commands that exist, each with the
  check that has to pass before the next and what to do when it does not.
  Driving it as one command waits on the rule against new CLI modes.
* **14:** a migkit-owned mover, only with the benchmark of item 1
* ~~**18:** continuous verification that costs what changed~~ done for
  pairs (2026-09-25): a pair whose source log reads without moving
  (MySQL, MongoDB) verifies only the rows the log names, asking both
  sides for those keys and comparing them in the one rendering. The
  position moves only on a clean pass. A source read through the tail's
  slot says so (`tests/test_a_pair_verifies_only_what_changed.py`)
* ~~**19:** bisection diffing across engines on the canonical rendering~~
  done (2026-09-25): a table past the walk's 20,000-row cap, keyed by one
  integer, is digested in halves of its key range on both engines, and
  only the halves that disagree are followed down to ranges small enough
  to walk. A text range would depend on each engine's collation, so it is
  left to the walk. Measured, MySQL against PostgreSQL at 60,000 rows: a
  changed, a missing and an extra row past the walk's reach were all
  named, where the walk alone had stopped at 20,000 and said there might
  be more (`tests/test_a_large_table_is_bisected_across_engines.py`)
* **20:** PL/SQL with behavioural proof

## P2: *Partly* in the problems file, still to close

* **A1** scope
* **A2** privileges down to columns (with **D10**)
* ~~**A4** fork identity~~ done (2026-09-25): MariaDB objects MySQL has no home for
* **B1** type mappings that change values
* **B4** default collation changes (the collapse is asked; mixed collations inside stored code are not)
* ~~**B6** values the target refuses~~ done (2026-09-25): zero dates named before a move between engines
* **C1** speed
* **C3** resume after a crash
* **C4** load on the source
* **D11** documents outside the table
* **E4** change streams vs oplog
* **F1** dual writes
* ~~**F4** poolers~~ done (2026-09-25): named in `assess`, and a refused connection setting said as the pooler's
* ~~**G2** masking what the drilldown shows~~ done (2026-09-25): hop option `mask`

## P3: carried over from the work loop

| Item | Note |
|---|---|
| pgcopydb receives passwords in URIs on its command line | done (2026-09-24/25): no password in the local copy's URIs (a password file), and the container's URIs come from the environment through `-e NAME` |
| PostgreSQL index window names indexes without their schema | done (2026-09-24): schema-qualified and quoted |
| `create publication` | done (2026-09-24): made only where it is missing, and a subscription a first run made is left to run - a second `--mode cdc` stopped on `already exists` (`tests/test_bounding_the_subscription.py`) |
| `follow` | ends at the current position; there is no long-running mode |
| generic engine | done (2026-09-25): string length and nullability are read from the same catalogue the comparison library asks, in the standard spelling and then Oracle's; where neither answers, the verdict still says they were not compared (`tests/test_generic_schema.py`) |
| MySQL `_health` | done (2026-09-24): a side that is itself a replica reports how far behind it is, by column name on MySQL and MariaDB, and the throttle backs off on it (`tests/test_mysql_fence.py`) |
| hetero | has no deep battery |
| sqlglot | decided (2026-09-25): kept, for what a type map cannot do. It translates each column default into the target's SQL: `uuid()` becomes `gen_random_uuid()`, `now()` becomes `CURRENT_TIMESTAMP()`. The target is then asked whether it takes the result, and a default it refuses is named rather than guessed at |
| to measure before wrapping | boto3 Secrets Manager, `mongodump --query/--oplog`, datacompy `all_mismatch()`, mydumper `--regex/--rows` |
| resumable-dump flag | tell the operator when a restart loses the dump's progress (DTS `DumperResumeCtrl`) |
| timed start and auto-retry window | low value next to cron |
| two-way and many-to-one topologies with loop detection | moved up to item 36 |
| waiting on the owner | should dry-run plans hide the command lines they print? |

---

## From the project's memory and the old work queue (added 2026-09-24)

Swept from the notes kept across sessions, then checked against the code.

**Already done, so not listed:**
* float tolerance (`options.float_tolerance`)
* MongoDB skipping `system.buckets` / `system.views`
* the local/S3 state backend wired into the CLI
* parameter comparison (`check_params`)
* MySQL type narrowing and time-shift

### P1

**14. A network path migkit can open itself.**
* **What happened on real legs:**
  * a VPN path black-holed bulk traffic while TCP still opened (an MTU
    problem)
  * a VPN-routed path stalled mid-copy, and the fix that worked was a
    cloud port-forward over the provider's API (SSM)
  * migkit only prints advice about it (`advisors.py`)
* **Deeper:**
  * SSH tunnels and cloud port-forwards as hop options, opened and closed
    by migkit
  * `doctor` telling "TCP opens but bulk stalls" apart from "cannot
    connect"
* This is the operator-side half of what DTS offers as access types.

**15. Rows the target has and the source does not: who wrote them, and
when.**

An open question from a real leg. The deep boundary check flags a target
*ahead* of its source, but cannot say why. The candidates are:
* a double apply of full load plus CDC
* a target that was not emptied
* snapshot rows replayed by CDC
* deletes that never propagated
* writes made on the target

Attribute them where the engine can say:
* PostgreSQL: commit timestamps where `track_commit_timestamp` is on,
  transaction age otherwise
* MongoDB: ObjectId time
* MySQL: the binlog, where it still covers the window

Report each row's age against when the move started.

*Done for PostgreSQL and MongoDB (2026-09-24):* a move now records, as it
begins, what the target says about that moment (`move-began.json`). On
PostgreSQL that is the oldest transaction still running there. The data
check then says, of the rows only the target has:
* on PostgreSQL, how many were written before the move began (the target
  was not emptied of them) and how many after it (the load, a stream
  applying twice, or something writing to the target). Where the target
  keeps commit timestamps, it also gives when they were committed.
* on MongoDB, the same split by the second each ObjectId was made, and how
  many carry ids with no time in them
* that it cannot tell, where no move of migkit's marked the target and it
  keeps no timestamps

(`tests/test_who_wrote_the_rows_only_the_target_has.py`).

*MySQL (2026-09-25):* a move records where the target's binlog was as it
began. For the rows only the target has, the data check reads the
target's own binlog from that point:
* a row written since is there, with the server that wrote it: this
  server's own sessions, or another server arriving through a replica
* a row that is not there was held before the move began, so the target
  was not emptied of it
* where the log from that point is gone, it says it cannot tell

Measured: one row left from before and two written after, each named as
such. The test is `tests/test_who_wrote_the_rows_only_the_mysql_target_has.py`.

**16. The side scripts in `tools/` become migkit, or go.**

*Done (2026-09-24):* every migration script is gone, each because migkit
already does its job.
* **Grants:** the grant checks and repairs cover `check_grants` and
  `apply_grants`.
* **Users:** `migkit users` and `assess` cover `check_users` and
  `user_sync`.
* **Objects:** the object checks cover `check_routines`.
* **Tables with no key:** every table's whole-table checksum covers
  `check_nopk`, key or not.
* **Comparisons:** `check` covers `full_compare`, `full_compare_mongo`,
  `param_diff` and `spot_check`.
* **Constraints:** the constraint repair now validates foreign keys as
  well as checks, and only where the source's own is validated. That
  covers `validate_constraints` (`tests/test_constraint_repair_pg.py`).

`check_no_secrets` and `gen_changelog` stay: they are the repository's
release tooling, not migration scripts.

Twelve standalone scripts sit beside the package. The owner's rule is one
tool, so each either becomes part of a migkit verb or is deleted where
migkit already does its job:
* `check_grants` / `apply_grants`
* `check_users` / `user_sync`
* `check_nopk`
* `check_routines`
* `full_compare` / `full_compare_mongo`
* `spot_check` (index-seek sampling)
* `validate_constraints` (validates NOT VALID constraints: a repair migkit
  lacks)
* `param_diff`
* `gen_changelog`

**17. MySQL events: repaired, not only detected.**

*Done (2026-09-24):* the schema repair (`sync --kind schema`) now creates
the events the target lacks from the source's own `SHOW CREATE EVENT`. It
redefines the ones defined differently; who defined an event and whether
it is on are not counted as differences.
* Each is made in the time zone and sql_mode the source defined it in,
  since both change what it does.
* Each is made **switched off**. An event running on the target while rows
  are still being carried rewrites them under the move, so switching them
  on is left to the cutover, and the inventory says so.
* Statements run one at a time in one session, since an event's body is
  full of semicolons.
* The undo drops what was made and puts back the target's own definition.
* Found on the way: the schema repair handed the target's password to the
  MySQL client on its command line, where any process listing could read
  it. It goes through the environment now.

(`tests/test_mysql_events_are_repaired.py`)

**18. Rehearse the undo (F2).**

The undo written beside every schema fix has only ever run in tests.
Prove it on a scratch copy of the target before anyone relies on it on the
day.

*Done for PostgreSQL (2026-09-24):* `sync --kind schema --apply` first runs
the fix and then its undo on a scratch database on the target's own
server, holding a copy of the target's schema. It compares the schema
with what it was, then drops the scratch database whatever happened.
* A fix that fails there is not applied to the target.
* An undo that does not return the schema exactly is said, with the lines
  in `rehearsal.diff`. Example: a dropped column put back without its
  default and NOT NULL.
* A target where no scratch database can be made says the undo was not
  rehearsed, and carries on as before.

(`tests/test_the_undo_is_rehearsed.py`)

*Done for MySQL (2026-09-25):* `sync --kind schema --apply` already
applies MySQL's fix DDL. It is now rehearsed the same way, on a scratch
database in the target's character set and collation. This matters more
on MySQL than on PostgreSQL, because MySQL's DDL runs outside any
transaction. Before this, a fix whose second statement failed left its
first statement applied on the target.

*Also folded into existing items:*
* item 6 gains the `gtid_mode` / `enforce_gtid_consistency` mismatch. A
  target that enforces GTID consistency rejects `CREATE TABLE ... SELECT`
  and temporary tables inside a transaction.
* item 7 gains per-phase, per-engine timings recorded on every run and
  scaled to the production size, which is what a rehearsal is for.

### P2

**19. Canonical rendering for enum, interval, hstore and tsvector.**

Confirm which are already canonical, then add the rest.

*Enums and domains (2026-09-24):* PostgreSQL's enums and domains are types
of the database's own, and the cross-engine comparison classified columns
by type name. Measured PostgreSQL to MySQL before this: both columns were
left out as types with no rendering. A row whose enum was `sad` on one side
and `ok` on the other came out `ok`, with a footnote. Now an enum compares
as its label, and a domain as the type it is built on, following domains
built on domains (`Engine.canonical_type`,
`tests/test_enums_and_domains_across_engines.py`). MySQL's `enum` and
`set` were already text.

*The same engine on both sides (2026-09-25):* the pair machinery now also
compares same-engine tables: a column mapping on a PostgreSQL or MySQL
hop, and SQLite's own. There, a type with no shared rendering was still
left out, so a changed interval read "every compared column equal". When
both sides are one engine and declare one type, the column is now compared
by that engine's own text of the value. All 37 columns of the wide test
table are compared this way, and a table the pair builds keeps those types
as the source wrote them
(`tests/test_same_engine_types_with_no_shared_rendering.py`).

*Across engines (2026-09-25):* `hstore` and `tsvector` are carried and
compared now:
* **`tsvector` and `tsquery` are compared as text.** A text search vector
  prints its lexemes sorted and once each, so its text is the value
  itself.
* **`hstore` is compared as JSON.** A key/value set is a JSON object of
  strings: the extension casts it to jsonb, and MySQL keeps it as JSON.
  It is read as a mapping now; it came back as its text (`"a"=>"1"`),
  which is not JSON at all on the other side.

Measured PostgreSQL to MySQL: a table carrying both was built (hstore as
JSON, tsvector as text), moved and checked equal. A changed key/value set
and a changed text search vector each read as a difference
(`tests/test_hstore_and_tsvector_across_engines.py`).

`interval` still has no counterpart. MySQL has no type that holds months
and seconds apart, and the driver turns a month into thirty days on the
way. It stays out of the comparison, and the footnote says so.

**20. The PostgreSQL-only helpers, ported where the idea exists elsewhere.**

`_filtered_tables`, `_extension_data`, `_large_objects`, `_mojibake_repair`.

*Done where the idea exists (2026-09-25):*
* `_mojibake_repair` now runs on MySQL too. MySQL's mojibake check had
  always asked for a row-by-row repair, and only PostgreSQL could do it.
  Which values to touch is decided in one shared place
  (`_mojibake_updates`). MySQL reads the rows on the target and applies
  the repair in one transaction. Only the values that re-encode to valid
  UTF-8 change (`cafÃ©` becomes `café`, while a real `café` stays), and
  the undo restores the old values exactly (`tests/test_mysql_text_repair.py`).
* The other three have no counterpart on the engines migkit can run here:
  * MySQL and MongoDB have no row-level security
  * neither keeps data inside extensions
  * MySQL stores large values inline rather than as large objects

  SQL Server has row-level security (security policies). It waits on the
  same hardware as item 27.

**21. `setup_target_plan` for MySQL.**

*Done (2026-09-25):* the old plan loaded the whole schema before the data,
so every secondary index was maintained row by row through the load. It
also created the database as `utf8mb4`, whatever the source used. The new
plan creates the database in the source's own character set and
collation, read from the source; a source it cannot read is said, not
guessed. The rest is said as migkit's commands:
* the move creates the tables the target lacks, sets the secondary indexes
  aside and builds them once after the load (1.41x, measured), and keeps
  the target's triggers off
* routines, views, triggers and events go through `schema --migration`,
  with events carried disabled
* accounts go through `users`

(`tests/test_setup_target_plan_mysql.py`)

**22. Coverage of `unchanged_since`.** Which checks honour it, and which
still re-read everything.

*Answered (2026-09-24):* only PostgreSQL's data pass honours it (version 15
and later, from its shared-memory statistics and `relfilenode`), and its
verdict names the tables it did not read. Every other engine, and
PostgreSQL's consistent pass, re-reads everything. That is the safe side:
no other engine has a marker that has been measured to move on every
change, TRUNCATE included, and a marker that does not is a false negative.

**23. The time zone the data actually uses.** Compare it with the time zone
each server declares.

*Done (2026-09-25):* a new deep check, `time zone in use`, on PostgreSQL
and MySQL (`migkit/zones.py`).

The problem: a column with no zone of its own holds whatever wall-clock
time the application wrote. An application writing Bangkok time into a
server set to UTC is harmless until the move. Then a target column that
has a zone, or a target server in another zone, shifts every value by
seven hours. A count and a checksum over the same text do not see it.

The evidence used is the one kind that does not guess. A column whose
latest value is in the future of the server's UTC clock is written in a
zone at least that far ahead, and the check names the column and the
zone. A latest value in the past proves nothing, and is not read as a
zone. Only columns an index leads are read, so each costs one probe and
never a scan.

Measured with values seven hours ahead on a UTC server: both engines
said "at least UTC+6:30", and a column with no index was not read
(`tests/test_the_time_zone_the_data_is_written_in.py`).

**24. G1: a target that is correct and slow.** Close what
`problems-and-what-ends-them.md` G1 leaves open.

*Done (2026-09-25), decided from what the others do.* DMS and DTS do not
look at this at all. The tools that do, Oracle's SQL Performance Analyzer
and Microsoft's Database Experimentation Assistant, take the source's
real statements, run them on both sides, and compare plans and times.
migkit now does the same for reads (`migkit/workload.py`, in the deep
checks):
* **Which statements.** The source's busiest reads by total time, from
  its own statement statistics: `pg_stat_statements` on PostgreSQL, the
  `performance_schema` digests on MySQL. migkit's own statements are left
  out: on PostgreSQL by user, on MySQL by what its checks contain.
* **Plans.** Each read is planned on both sides. PostgreSQL uses a
  generic plan where parameters remain. A table the target reads whole
  and the source does not is a finding, whatever the timing says.

  Measured on MySQL: `max(k)` over an indexed column is answered before
  execution, and its plan names no table. That counts too.
* **Times.** A read that can run as it is, is run on both sides:
  read-only, under a time limit, once to warm each side, then alternated.
  It is slower only when its median is more than twice the source's and
  more than 20 ms beyond it.
* **Where it stands.** A finding is `warn` and never `diff`, because the
  data is the same. It does not stop `check` unless the hop says
  `performance: gate`.
* **What it shows.** Only the normalised statement, never a sample's
  literal values.

With an index dropped on the target, both engines named it: "reads t
whole on the target where the source uses index t_k". With the index in
place, both said the target answers as well. The tests are in
`tests/test_the_target_answers_the_sources_reads.py` and
`tests/test_the_target_answers_the_sources_reads_mysql.py`.

**25. Long reads over unstable links.** Sustained MongoDB cursors stalled
over a tunnel; single-command reads did not. Read in bounded chunks that
resume from the last key, on every engine.

*MongoDB, and a false negative found on the way (2026-09-24):*
* **The digest** read one cursor over the whole collection. It now reads
  in chunks of 5,000, each its own short query resumed from the last key,
  and retries a chunk that fails on the way. It gives the same answer at
  any chunk size.
* **The chunked read** resumed with `$gt` on `_id`, and a query's `$gt`
  compares within one type only. Measured: seven documents keyed 1, 2,
  2.5, "a", "b" and two ObjectIds, read two at a time, came back as the
  three numbers. So the cross-engine copier and the row walk built on it
  moved and compared three of seven, without a word. It now resumes with
  an expression, which compares in the order the sort uses across types
  and still walks the `_id` index.
* A document keyed null no longer reads as the end of the collection.

(`tests/test_mongo_reads_every_key_type.py`) The SQL engines already read
in key-ordered chunks through `neutral_read`.

### P3

**26. Research not yet done**, for the decision layer's list of what can be
wrapped:
* Bytebase, Airbyte, sqlpipe, schemachange, Trino
* Striim, Qlik Replicate, Fivetran HVR

The question is mechanism, not features.

*Read (2026-09-25):* `docs/research-other-tools.md` covers HVR, Qlik
Replicate, Striim, Airbyte, Bytebase and Trino, from each vendor's own
documentation, and says what migkit took from each:
* Qlik's batch optimized apply is the model backlog 29 followed
* Striim's interval validation, and Bytebase's schema snapshot at each
  change, are ideas to take next

HVR, Qlik, Striim and Bytebase are learned from and not wrapped. They
are commercial or bring a platform of their own. Trino is a candidate
second reader, to measure first. sqlpipe and schemachange are not read
yet.

**27. SQL Server depth.** Closed as untestable on this arm64 machine. Either
find an x86 runner, or list it plainly in section H of
`problems-and-what-ends-them.md` as a limit.

*2026-09-25:* SQL Server is now a side of a pair (item 39): tables,
keys, reads, writes, digests, views and functions. It sits on the
driver base it shares with Oracle and Db2. Every line of it is held
without a server. It still waits for an x86 runner to be run for real.

---

## Where the paid tools and the clouds still lead (added 2026-09-24)

From the comparison against GoldenGate + Veridata, Qlik Replicate, Striim,
Fivetran HVR, AWS DMS, Azure, Google DMS and Tencent/Alibaba DTS.

**The owner's rule for this section:** everything they lead on, migkit
does too - researched down to the mechanism, and built smarter and
faster than what it replaces. ETL is out of scope. AI assistance is
*soon*, and never tied to one vendor.

### P1: without these the claim "as good as the paid tools" is not honest

**28. A benchmark anyone can re-run.**

No speed claim holds until this exists. Build a harness on one machine:
* a fixed data generator covering the shapes that matter: keyed and
  key-less tables, wide rows, LOB tables, partitioned tables, a skewed key
* measure three things:
  * bulk copy time
  * change-stream lag at fixed write rates (1k, 10k, 50k tx/s)
  * verification time
* run it against migkit and the open engines it could have used instead

The managed services cannot run on a laptop. For them, publish a recipe
the owner runs on the same instance class, with the cost of the run
stated. Results are published with the hardware and every setting that
produced them.

*The harness (2026-09-25):* `bench/run.py` does the following on one
machine:
* starts a disposable PostgreSQL or MySQL pair
* builds the six shapes (keyed, key-less, wide, LOB, skewed, partitioned)
  inside the source
* times each move path and the check
* times the same copy through the open programs directly
* with `--cdc-rate`, measures the change tail's lag at a fixed rate

It writes the numbers with the hardware and versions to
`reports/bench/`, and `docs/scale.md` has the first run. Still open: the
10k and 50k tx/s rates, which this laptop VM cannot produce, and the
recipe for the managed services.

*Blocked (2026-09-25), with the reason:* 10k and 50k transactions a
second need a source and a target on hardware that can sustain them, and
this machine's 2-CPU VM tops out far below. `migkit bench --cdc-rate`
takes the rate as it is; the run waits for that hardware, and its
numbers go into `docs/scale.md` with the machine they came from.

**29. Change apply that keeps up at very high write rates.**

*First step (2026-09-25), found by the harness:* every change the tail
applied opened a connection of its own and committed on it. MySQL into
MySQL at 190 rows a second was still 15 seconds behind when the writer
stopped after ten. A batch is now one transaction on one connection, and
the same run ends 2.9 seconds behind. A batch that fails is rolled back
whole and replayed (`tests/test_changes_apply_as_one_batch.py`).

*Rows collapsed in a batch (2026-09-25):* a row changed several times in
one batch is written once, with what the batch left it as:
* the later change's values override the earlier one's
* a change that carries only some columns (an update written without an
  unchanged large value) is merged rather than blanking the others
* a row made and removed within the batch is not written at all
* a moved key leaves its old address and arrives at the new one, in the
  same transaction

The rows keep the order in which they were first touched, so a parent
made before its child is still written before it.

*A run of rows in one statement (2026-09-25):* rows next to each other in
a batch, in the same table and with the same columns, are written by one
statement of up to 1,000 rows on PostgreSQL and MySQL. Other engines
still write one statement per row. Measured on PostgreSQL 16 with 20,000
upserts: 3.9 seconds one row at a time, 0.06 seconds in runs.

Only neighbouring rows are joined, so the order is kept. A key seen again
starts a new statement, because PostgreSQL refuses one statement that
writes the same row twice.

On MySQL, a row made of nothing but its key used to be a plain insert.
Replaying it was error 1062, and a tail that replays a failed batch
stopped on that error every time. It is now left as it is.

The benchmark then found the tail's reader was the limit. It looked up
the table's key on a new connection for every event, and 472 one-row
transactions a second left it 38 seconds behind. It now ends 0.73
seconds behind, and 0.13 seconds behind at 1,030 a second, which is the
fastest the writer reached (`docs/scale.md`). The tests are in
`tests/test_runs_of_rows_apply_as_one_statement.py`.

Parallel apply partitioned by key (step 2 below) is not built. Measured
here, the tail keeps up with the fastest writer this machine can run, so
there is no rate available to show that it is needed.

*Large transactions streamed to the subscription (2026-09-25):* measured
on PostgreSQL 16, with the source's `logical_decoding_work_mem` at 64kB
and 20,000 rows written in one transaction. The subscription migkit made
spilled 3,460,000 bytes to the source's disk and sent them at commit.
With `streaming = parallel`, nothing spilled, the same bytes were
streamed, and all the rows arrived.

The subscription now carries the option, based on the server versions
read at planning time:
* `parallel` from 16 on the target
* `on` from 14 on the target
* nothing where either side is older than 14, or where either side
  cannot be asked

A source older than 14 cannot send a transaction before it commits.
The test is `tests/test_large_transactions_stream_to_the_subscription.py`.

*More than one applier on the MySQL-family replica (2026-09-25):*
measured with 100,000 one-row transactions queued on the replica before
its applier started.

| Target | Setting | One applier | Four appliers |
| --- | --- | --- | --- |
| MySQL 8.4 | `replica_parallel_workers` | 28.8s, 55.3s, 51.0s | 14.2s, 16.4s, 27.4s |
| MariaDB 11.8 | `slave_parallel_threads` | 7.6s to 9.1s | 4.7s to 5.2s |

The same rows arrived in every run. On MariaDB, "one applier" means
`slave_parallel_threads` 0, which is the default.

The plan now sets four appliers on a target that has only one. It leaves
the target alone in three cases:
* a MySQL target that does not keep the source's commit order
  (`replica_preserve_commit_order`)
* a MariaDB target with a replica running, where the setting cannot be
  changed
* a target that cannot be asked

The note names the configuration line that keeps the setting after a
restart. A managed target is told to set the parameter instead.

*MariaDB replica on a target with its own log (2026-09-25):* measured on
11.8. The plan started the replica at `MASTER_USE_GTID = current_pos`. On
a target that keeps a binary log, that position includes the target's own
writes, such as the tables made for the copy (`0-52-3`). The replica asked
the source for that position and stopped with error 1236. It now starts
at `slave_pos`, which holds only what was replicated. On a target with no
log, `slave_pos` is the same position as before. The test for both
changes is `tests/test_the_replica_applies_in_parallel.py`, with a binary
log on both sides.

The paid tools lead here because they apply changes in parallel while
keeping each key's order, and collapse many changes to one row before
writing. Build it in two steps:

1. **The planner sets the native parallel-apply knobs where the target
   has them:**
   * PostgreSQL 16+ `streaming = parallel` on subscriptions (done, above)
   * MySQL `replica_parallel_workers` with `WRITESET` dependency tracking
     (done, above; 8.4 always tracks by write set and has no setting for
     it; an 8.0 source's `binlog_transaction_dependency_tracking` is the
     source's setting, which migkit does not change)

   Measure each against item 28's rates.
2. **Where migkit owns the apply (the stream path), a migkit applier:**
   * changes partitioned by table and key hash, so each key stays in order
   * batches that collapse a row's changes before writing (DMS
     `BatchApply` semantics)
   * commit order kept where a table's foreign keys require it

   Lag, throughput and apply errors are reported in migkit's own words.

**30. A control plane.**

HA, resume on another machine, a scheduler, a UI and roles:
* **State, shared:** the local/S3 state backend already exists. Move every
  piece of run state onto it - checkpoints, change-stream cursors, locks.
  Locks become leases with a heartbeat, so a second machine can take over
  a run whose machine died, from the last committed chunk or cursor.
* **Scheduling:** run specifications on a schedule, with phases that are
  safe to repeat.
* **UI:** the read-only dashboard (`report --serve`) grows into an
  operations view.
* **Roles:** viewer, operator, approver. Anything that writes (`--go`,
  `apply`) needs an approval when the hop says so.
* **Audit:** an audit trail of who approved what.
* **Tension to resolve:** the rule against new CLI modes. Keep all of
  this inside existing verbs and hop options, and say so where it is not
  possible.

*First step (2026-09-25): the write lock is a lease.* It was a file
holding a process number. Only a process on the same machine could ask
whether that number still ran, and a holder that died left the file
saying it did. Now:
* the holder renews a lease while it runs, every third of its term
  (`MIGKIT_LEASE_SECONDS`, 60 by default)
* a lease not renewed for its whole term is taken over, with a line
  naming the holder that let it lapse
* a holder on the same machine whose process is gone is taken over at
  once
* a holder that was taken over stops renewing, and does not remove the
  new holder's lease when it finishes
* reads and writes of the lease are made under an exclusive lock, so two
  processes deciding at once cannot both win

The tests are in `tests/test_a_hops_write_lease.py`.

*Second step (2026-09-25): the run's state in the bucket.* With the s3
state backend (`options.state.backend: s3`), a run keeps its state under
`<prefix><hop>/run/`, beside the restore points:
* **The lease.** It is written only over the version that was read, using
  the bucket's own conditional write (`If-Match`, or `If-None-Match: *`
  for the first). A write that loses reads again and decides again.
  Measured against an S3-compatible server in a container: of eight
  processes asking at once, one held the lease. A holder that stopped
  renewing was taken over after its term, and could not remove the new
  holder's lease when it came back.
* **Checkpoints.** A move's checkpoint is saved there after every chunk,
  and read from there by a machine that has none of its own. A second
  machine's dry run of a move the first had finished said it was already
  done.
* **The change position.** The tail's position, and the column shape it
  applies under, are pushed as they change, every two seconds at most. A
  position a few seconds old is safe to resume from, because the tail
  applies by key and converges on what it replays.
* **The copy record.** A later `--mode cdc` starts from it, on whichever
  machine runs it.

Run state is sealed like a restore point where `MIGKIT_STATE_KEY` is set,
since a checkpoint names the last key copied. The key is derived once per
process, not once per chunk. The lease is not sealed, because it holds
no data.

*Found on the way:* the dry run of a move looked its checkpoint up as
`schema.table`. Every copier but PostgreSQL's writes `database.table`,
and Redis writes `db<n>`. So on every other engine a finished or
half-done copy read as `todo`. Each engine now names the entry in one
place (`move_key`), and the copier and the plan both use it. A copy
resumed by a text key is said as `resume after [...]`, where formatting
it as a number would have stopped the plan.

The tests are in `tests/test_another_machine_takes_the_run_over.py` and
`tests/test_the_plan_reads_the_checkpoint_the_copier_writes.py`.

**31. Scale-out across machines.**

A coordinator hands out tables and chunks from a queue in the shared
store (item 30), and workers lease them. This is DMS Serverless and a
Striim cluster done the open way. The throttle already scales work
*down* when the source strains; add scaling *up* when the source has
headroom.

*Done for tables (2026-09-25):* a hop with `share_tables: true` and the s3
state backend can run the same `move --mode full --go` on several
machines at once. There is no coordinator: the bucket is the queue.
* The machine that arrives first sets up under the hop's lease: it
  creates what the target lacks and records the change position from
  before any copying. Every later machine uses that position.
* Then each table is taken by whichever machine gets its lease, a lease
  per table in the bucket. It is copied under its own load window, and
  the machine moves on.
* The checkpoint is merged into the bucket with conditional writes, so
  machines saving at once keep each other's tables.
* A table whose lease is held is asked for again. So a table whose
  machine died is taken over once its lease lapses, from that machine's
  last saved chunk.
* When every table is done, one machine completes the move: indexes,
  statistics, the copy record. The others say that another machine is
  completing it.

Measured with two processes, each with its own report directory, against
an S3-compatible server in a container:
* six tables were each copied once, split between the two machines, and
  every row arrived
* one machine killed partway through a 20,000-row table was taken over:
  the next machine carried on from the saved chunk, emptied nothing, and
  finished with every row once

The tests are in `tests/test_another_machine_takes_the_run_over.py`.
Still open: chunks of one table across machines, and scaling up within a
machine when the source has headroom. Meanwhile, adding machines is the
way to scale up.

**32. Alerts, not only metrics.**

Prometheus metrics exist. Add shipped alert rules and webhook
notifications (Slack, Teams, PagerDuty, generic HTTP) for:
* change-stream lag
* WAL or binlog retention filling up
* a verification finding a difference
* a run stalling

This is the part of CloudWatch alarms that migkit can own.

*Done (2026-09-25):*
* **Tail heartbeat.** A change tail now writes `tail.beat` each time
  round its loop. It records when the tail last read to the end of the
  source's log and how many changes it has applied.
* **Stop record.** A tail that stops on anything other than a person's
  ctrl-c or a service stop writes `tail.stopped`, saying what stopped it.
* **Tail metrics.** `/metrics` reads those files and exports:
  * `migkit_tail_behind_seconds`
  * `migkit_tail_heartbeat_age_seconds`
  * `migkit_tail_running`, which is 0 when the process is gone without
    stopping
  * `migkit_tail_paused`
  * `migkit_tail_changes_applied`
  * `migkit_tail_stopped`
* **Alert rules.** `deploy/prometheus-alerts.yml` ships rules over these
  metrics and the check metrics. A test holds every metric a rule names
  against what the exporter emits.
* **Notifications.** The hop option `notify`, or `MIGKIT_NOTIFY`, sends
  a message when:
  * the verdict moves between `same`, `different` and `error`
  * a tail stops on an error
  * a stopped tail runs again

  Slack, Discord, Teams workflows, PagerDuty (one incident per hop and
  one per tail, resolved when it clears) and any JSON receiver each get
  the shape they take. What is sent carries each finding's check, scope
  and status, never its detail, and a webhook's address is never
  printed.

The tests are in `tests/test_alerts_and_notifications.py`. The receivers
are local; nothing is sent to the real services.

*Retention.* Each engine is now asked how much longer the source keeps
what the tail has not read (`stream_room`). The tail asks once a minute
and `/metrics` exports the answer:
* **PostgreSQL:** the slot's `safe_wal_size` (0 once the slot has lost
  its WAL), and the bytes it holds when nothing caps them. Measured:
  32MB capped, 45,143,112 bytes of room, then `lost` after a load and a
  checkpoint.
* **MySQL:** seconds until the tail's binlog file may be purged. That
  is counted from when the file stopped being written, which is the
  time on the format event opening the next file. It is 0 once the file
  is gone, and nothing when auto-purge is off.
* **MongoDB:** seconds of oplog older than the resume point. The resume
  token opens with 0x82 and the cluster time.

There are alert rules for each (`MigkitRetentionShort`,
`MigkitRetentionBytesShort`). A managed MySQL's retention is its own
setting and is not read yet, so no number is given for it rather than a
wrong one. The tests are in `tests/test_how_long_the_source_keeps_the_log.py`.

**44. Failure caused on purpose.**

This is for day one, not instead of real use: real migrations keep
teaching after launch, and each failure they show becomes a case here.
The harness makes sure the failures already known are handled before
anyone meets them, in a form anyone can re-run. Each case, on every
engine 0e declares a move or stream for:
* migkit killed mid-dump, mid-load, mid-copy, mid-verify
* the network between source and target cut, then restored
* the source restarted, and the source failing over to a replica during
  a change stream
* the target's disk filling
* DDL on the source mid-stream (with item 5)
* the replication slot dropped, the binlog purged, the change-stream
  resume point expired

Done so far (problems file E7, `test_full_cdc_misses_nothing.py`):
* writes on the source while a `full+cdc` copy runs
* a tail stopped on a quiet source and started again
* a count-only tail followed by one that applies
* a collection dropped under a running tail
* the replication slot dropped and the binlog purged under a saved
  position (`test_a_lost_position_stops_the_tail.py`)
* **A position from another source, and MongoDB's resume point expiring
  (2026-09-25).** A saved position belongs to one server's log. After a
  failover to a server with a log of its own, or with the source rebuilt
  under the same address, a MySQL file and offset point into a different
  binlog. A MongoDB token from a rebuilt set had been accepted without a
  word. Now:
  * the source's identity is saved beside the position when it is first
    saved: MySQL's server id, PostgreSQL's system identifier (which a
    physical replica shares with its primary), and MongoDB's replica set
    name and id
  * every resume compares it. A different source stops the tail before
    it applies anything, and says which source the position belonged to
  * MongoDB is also asked, before the stream opens, whether its oplog
    still reaches back to the position, and whether the position is later
    than the set's own clock, which means it was taken on another set

  Measured by rebuilding each source under the same address: MySQL,
  PostgreSQL, and MongoDB with the same set name. A token moved behind
  the oplog's start was named as gone, and one moved past the set's clock
  as another set's. The tail stopped before opening the stream
  (`tests/test_a_position_from_another_source_stops_the_tail.py`).
* **migkit killed mid-load (2026-09-25).** This was measured with real
  moves, and the programs migkit started outlived it.
  * *MySQL, 1,500,000 rows.* The load went on without migkit: 375,000
    rows when migkit died, all of them soon after. A move started again
    at once emptied the tables under the running load, then stopped on
    `Duplicate entry '425000'`. It named neither the cause nor the
    program.
  * *PostgreSQL, 3,000,000 rows.* The copy program finished the table
    after migkit was gone. The target was left with every row, no
    primary key and no index, because the step after the copy never ran.

  The fix has two parts. First, `kill` (SIGTERM) now stops the programs
  the move started, the way ctrl-c does. Measured: the load stopped with
  migkit, the dropped index came back, and the next move was `same`.

  Second, `kill -9` and the out-of-memory killer cannot be answered by
  anything, so the programs a move runs are listed while they run
  (`running-programs.json`). The next move finds one still running,
  names the process, and stops before it writes anything. Measured: the
  second move refused while the orphaned load ran; the move after it
  ended went through, and the verdict was `same`. A process number
  reused by another program is not taken for the move's.

  The tests are in `tests/test_a_stopped_move_leaves_nothing_writing.py`.

  Seen on the way, and fixed: the deep checks said `every table has a pk
  or unique index` and `3 indexes, all valid` beside a schema check
  saying the target had lost all three. The first reads only the source
  and now says so. The second now counts each side.

  The second move after the PostgreSQL case put the keys back, and in
  the right order: it emptied the tables, loaded them, and then added
  the keys of the tables the killed run had created
  (`created-tables.json`).
* **The connection lost under a running tail (2026-09-25).** Measured
  with the target paused for 30 seconds: the tail stopped on `timeout
  expired` and did not come back. A tail is meant to run for days, and a
  brief network drop ended it.

  Now the tail re-reads from its last saved position once the server
  answers. It waits 2 seconds, then doubles the wait up to a minute, and
  logs each retry. Three cases were measured:
  * the target paused for 30 seconds
  * the target's network cut for 30 seconds (connection refused, retried
    four times)
  * the source paused for 30 seconds

  Each time the tail kept running and every row arrived once the server
  was back. An error that is not about the connection still stops the
  tail (a table dropped on the target, in
  `test_alerts_and_notifications.py`). The test is
  `tests/test_the_tail_rides_out_a_lost_connection.py`.
* **The target's disk filling (2026-09-25).** Measured by moving about
  150 MB of PostgreSQL into a target with a 150 MB disk. The move stopped,
  as it should. But its message was the last 500 characters of the copy
  program's log, mostly lines like `Sub-process exited with code 12`. The
  database's own lines were cut off above them: `could not extend file
  ...: No space left on device`, with its SQLSTATE and its hint.

  The target then stopped altogether, on `PANIC: could not write to file
  "pg_wal/..."`. The next move's log again ended in the program's own
  lines, above a refused connection.

  A failed program is now explained in the database's words. The side
  and the SQLSTATE come first, and the common ones are spelled out: `the
  target ran out of disk space: the target said: could not extend file
  ... (SQLSTATE 53100); hint: Check free disk space.; context: COPY big,
  line 175845`. A server that cannot be reached is named by its address.
  The tests use the logs as the program wrote them
  (`tests/test_the_database_words_come_through.py`).

Every case ends one of two ways: the run resumes from the last committed
point, or it stops and says in migkit's words what happened and what to
do. Either way, `check` afterwards proves the target. An outcome that is
silently wrong fails the harness.

**45. Proof at size.**

*Already done before launch:* migkit verified a real migration of about
800 GB - MySQL, PostgreSQL, and MongoDB to DocumentDB - that is now in
production use. Its data stays private.

Item 28 measures speed. What is left here:
* migkit's own move, not only its verification, at that size and past
  it, with the time and cost stated
* terabyte-class runs on an instance the owner rents, re-runnable by
  anyone with the same recipe
* memory staying flat as tables grow (every read streamed, never a whole
  table in memory)

*Memory, measured (2026-09-25):* migkit's own peak memory (`/usr/bin/time
-l`) on moves and checks of the same table at 200,000 and 1,600,000 rows.

| path | 200,000 rows | 1,600,000 rows |
|---|---|---|
| PostgreSQL to PostgreSQL, move (table copier) | 36 MB | 35 MB |
| PostgreSQL to PostgreSQL, check | 327 MB | 338 MB |
| MySQL to PostgreSQL, table keyed by text, before | 220 MB | **1,334 MB** |
| the same, now | 99 MB | 99 MB |
| MySQL to PostgreSQL, table with no key, now | 89 MB | 88 MB |
| MySQL to PostgreSQL, check | 46 MB | 44 MB |

The cross-engine copier held the table in three ways:
* a table without a single integer key went down a path that read it
  into one list
* a table with no key was read by `neutral_read` in one piece
* a keyed read took `--chunk` rows at a time (500,000), whatever the rows
  weighed

Now a keyless table is read through a cursor the server keeps
(`neutral_batches`: PostgreSQL, MySQL, SQLite). Each read is capped at
50,000 rows, and at 64 MB going by the table's own bytes per row. Each
read is still a resumable step. The copy took 22.8 seconds against 23.0
before. The tests are in `tests/test_a_copy_holds_a_read_not_a_table.py`.

*The terabyte recipe (2026-09-25):* `docs/scale.md` now carries the
recipe for the run on rented machines: the size in rows (about 3.6
billion `bench_rows` to a terabyte), the machines and volumes, the steps,
what to write down (including the cost, from each machine's hours and
price), and what counts as a pass. The pass is a `same` verdict with
peak memory flat from ten million rows. The run itself waits on the
owner renting the machines, since this laptop's container VM has a
20 GiB disk.

**46. Wrapped programs stay wrapped when they change.**

*Started (2026-09-24):* `tests/test_wrapped_flags_exist.py` reads the
command lines each bulk path builds for its dry run, and holds every long
flag against the installed program's own `--help` option column: the
MySQL, MongoDB and PostgreSQL dump and load programs. Run in a CI matrix,
it is the check that catches a renamed or removed flag before an operator
does. It found a gap in the option reader first. The MongoDB tools spell
their flags in camelCase with `=<value>`, and the reader, written for
lower-case flags followed by a column gap, saw 6 of mongodump's 37 flags.
It now reads them all, and still does not take a flag named inside
another option's description for a real one.

Every wrapped program has already changed underneath migkit once: flags
missing from the installed build, and a newer build emitting settings an
older server rejects. So:
* CI runs the move and check paths against a matrix of each wrapped
  program's versions
* each program declares the version range migkit supports, and `assess`
  says when the installed one is outside it (`_client_tool_versions`
  exists; extend it to every wrapped program)
* a release that changes behaviour is caught in CI, not by an operator

*Checked where the move runs (2026-09-25):* the same check now runs on
the machine doing the move, against the build installed there, not only
in CI. Neither of the following names the program.

`assess` gives one row for each program the chosen bulk path runs. The
row has three parts:
* the installed build's version
* whether that build is the one migkit was measured with (`MEASURED`:
  18.6, 1.0.5, 100.16.1)
* whether the build takes every option the move's own command lines
  pass it

A move with a build that lacks an option stops before anything is
written, and names the options. Tested with a stand-in older build on
the path, whose help lacked one option the move passes. `assess` failed
that row, and the move stopped without running the program.

Building this found a cache keyed on the program's name. A program
upgraded under a long-running process (`sync --serve`) was still read
as the old build. It is now keyed on the file and its modification time.

The tests are in `tests/test_the_installed_build_takes_the_options.py`.

*The matrix (2026-09-25):* `.github/workflows/wrapped-programs.yml` runs
both checks against each program's versions, weekly and on any change to
the bulk paths:
* PostgreSQL client 14 to 18, from the project's own repository
* the MySQL dump and load 0.21.4, 1.0.5 and 1.0.8
* the MongoDB tools 100.9.4, 100.12.0 and 100.16.1

Each download address was checked to exist. The workflow runs on GitHub
and has not run yet.

### P2: reach the paid tools have and migkit does not

**33. Targets that are not databases.**
* **Warehouses:** Snowflake, BigQuery, Redshift, ClickHouse. Load through
  staged Parquet files and each warehouse's own bulk load. Verification
  already reaches these through the second readers.
* **Streams:** Kinesis, Pub/Sub, Event Hubs, alongside Kafka.

Measure a wrap candidate before writing a loader, the rule as always.

*ClickHouse, done (2026-09-25):* an engine of its own, on either side of a
pair (`source_engine`/`target_engine: clickhouse`). It reads through the
HTTP driver and compares through the in-process renderer.
* **Created tables.** A table migkit creates is a MergeTree ordered by
  the source's key, with the columns outside the key nullable.
* **No doubled rows on a restart.** A MergeTree keeps a second row with
  the same key, so a batch written again after a restart deletes its keys
  first. Measured: 1,600 rows written again left 3,001 rows, 3,001 of
  them distinct, where the plain insert would have left 4,601.
* **Strings and bytes.** `String` columns are read as bytes, and decoded
  only where the column is text. Beside a column the other side declares
  bytes, a `String` is compared as bytes. Rendered as text, it had
  stopped the comparison on the first value that was not UTF-8.
* **Time zones.** A time with no zone is handed to the server as a
  wall-clock reading in the server's own zone. Measured from a machine at
  UTC+7: the driver had stored `2024-02-29 00:05` as `2024-02-28 17:05`,
  and every row read as different. Checked again against a server set to
  Tokyo, where the value round-tripped unchanged.
* **Deep check.** Mutations still being applied (the deletes of a
  restart) are named, since rows read before they finish are not settled.

A table ClickHouse made itself also moves into PostgreSQL: unsigned keys,
low-cardinality strings, a nullable decimal, millisecond times
(`tests/test_clickhouse_as_a_side_of_a_pair.py`).

*Kinesis, done (2026-09-25):* a pair with `target_engine: kinesis`
delivers its changes into Kinesis Data Streams. The messages are Kafka's,
built by the one module both use now (`streamout.py`), under the same
hop options.
* **Missing streams.** One the target does not have stops the delivery,
  unless the hop says `create_streams: true`, since a stream costs money
  for as long as it exists.
* **Order.** A key's records go to one shard. Kinesis can keep some
  records of a batch and refuse others, which would put a key's later
  change ahead of a refused earlier one. So a batch holds each key once,
  and a refused record goes again before its key's next change. Measured
  against a local stand-in: with key `b`'s record refused and the others
  kept, both keys' changes arrived in order and none twice. Resending
  from the first refused record, the obvious way, had doubled key `a`'s
  last two changes.

(`tests/test_changes_delivered_to_kinesis.py`)

*Pub/Sub, done (2026-09-25):* `target_engine: pubsub`, the same messages
from the same module. The target's `project` option names the project;
`host`/`port` an emulator. Each message carries its row's key as its
ordering key, and the publisher keeps order by it.
* **A topic nobody reads.** Measured on the emulator: a message published
  to a topic with no subscription is accepted - the publish returns its
  id - and a subscription made afterwards receives nothing. So a topic
  without a subscription stops the delivery before anything is sent, as
  a missing topic does. migkit makes neither: what reads them is not
  migkit's to decide.
* **A failed publish.** The client holds the key's later messages back;
  the delivery stops, frees the key, and the tail goes on from its last
  saved position when started again.
(`tests/test_changes_delivered_to_pubsub.py`)

*Event Hubs, done (2026-09-25) through its Kafka endpoint:* a Kafka hop
whose endpoint says `security_protocol: SASL_SSL`, `sasl_mechanism:
PLAIN`, the user `$ConnectionString` and the connection string as the
password. Every client migkit makes for a side is given the side's
sign-in (`tests/test_a_kafka_cluster_that_asks_for_a_password.py`,
against a broker that requires SCRAM; Event Hubs itself needs an Azure
account). A wrong password is said as a refused sign-in, where the
client alone said only that it could not reach the cluster. Amazon MSK's
IAM sign-in is not one of the mechanisms yet.

*Redshift, Snowflake and BigQuery, support written (2026-09-25),
untested:* each is a side of a pair (`target_engine: redshift` and so
on), reading its tables, columns and keys from its own
`information_schema` (Snowflake's keys from `show primary keys`), rows
through its DB-API driver, compared through the in-process renderer. A
table migkit creates keeps `not null` and the key, and nothing else of
the source's column rules; BigQuery's key is `not enforced`. Rows go in
as every DB-API engine writes them, except into BigQuery, which is given
a load job after the batch's keys are deleted - one insert statement per
row is a job each there. What is pinned without an account: the tables
each would create, how a page is read, how Snowflake's catalogue is read
back, and the rows BigQuery's load job is given
(`tests/test_warehouses_as_a_side_of_a_pair.py`). Still to come: loading
from staged Parquet through each warehouse's own bulk command, and a run
against real accounts.

**34. More engines to move, not only to verify.**
* **Sources DMS takes that migkit cannot:** Db2, SAP ASE, SQL Server at
  full depth.
* **Targets:** S3 (Parquet), DynamoDB, OpenSearch/Elasticsearch,
  Cassandra.

Order by what a real migration asks for. Every one arrives with its
verification, or it does not arrive.

*Parquet, done (2026-09-25):* files on a disk or under an S3 prefix, on
either side of a pair (`target_engine: parquet`, with the endpoint's
`path`, or `url: s3://bucket/prefix` plus `endpoint_url` for
S3-compatible storage).
* **Layout.** A database is a directory, and a table is its part files
  plus `_table.json`, which says what the columns are and which of them
  key the table. Files keep neither.
* **Types.** Values are Arrow types: `decimal128(p, s)`,
  `timestamp[us]`, `date32`. A decimal whose source declared no
  precision is kept as its text, since no fixed scale reproduces what an
  unconstrained numeric holds.
* **Restarts.** A keyed table names each part by the keys it holds, so a
  batch written again after a restart replaces itself.
* **As a source.** A Parquet table is read in one pass, a batch at a
  time, since files keep no order to resume by.
* **Deep check.** Every part file must open and hold the rows its footer
  says. A part cut off halfway is named.

Measured on a disk and in an S3-compatible bucket: a removed part is a
difference, and a truncated one is a deep finding. The files moved back
into PostgreSQL with every row
(`tests/test_parquet_files_as_a_side_of_a_pair.py`).

*Db2, written, not yet run against a server (2026-09-25):* on the same
driver base as Oracle and SQL Server, through `ibm_db`. A hop's database
is a schema; the endpoint's `database` option is the database to connect
to.
* It shares Oracle's name folding and its padding and scale handling.
* A character column with no code page (`FOR BIT DATA`) is bytes.
* The deep check names tables a load left in check-pending.

Held without a server, because IBM's image runs on x86 only
(`tests/test_db2_as_one_side_of_a_pair.py`).

*SAP ASE, support written (2026-09-25), untested:* a side of a pair
(`ase`, with `sybase` and `sap-ase` as other names for it), reached
through FreeTDS's ODBC driver at protocol 5.0 - the TDS driver that
installs with pip stops at SQL Server's 7.x (measured, `unrecognized tds
version: 5.0`). Tables, columns and the primary key come from ASE's own
catalogue; rows are compared through the in-process renderer. SAP's image
runs on x86 only, so what is pinned is how the connection is asked for,
how names, parameters and pages are written, how its types are classed,
and the table it would create (`tests/test_sap_ase_as_one_side_of_a_pair.py`).
Where ODBC cannot load (measured: the module installs, then misses
`libodbc.2.dylib`), it says what to install.

*Cassandra and ScyllaDB, done (2026-09-25):* on either side of a pair
(`cassandra`, with `scylladb` as another name for it). A database is a
keyspace.
* **Keyspaces.** A keyspace is created only with the replication the
  target endpoint names (`replication`), since how many copies it keeps
  is the operator's decision. The deep check warns while a keyspace
  keeps fewer than three copies.
* **Keys.** A table migkit creates takes all of the source's key columns
  together as its partition key. No clustering order is invented. A
  table with no key is refused.
* **Writes.** An insert replaces a row with the same key: 901 rows after
  a replay, not 1,802. A null is left unset rather than written, so no
  tombstone is made.
* **Reads.** Rows come back in token order, so a table is read in one
  pass, a page at a time, and looked up by key concurrently.
* **Timestamps.** A `timestamp` holds milliseconds. A source value with
  microseconds arrives without them, and the check reports a
  difference, because the target does hold less.

(`tests/test_cassandra_as_a_side_of_a_pair.py`)

*OpenSearch and Elasticsearch, done (2026-09-25):* on either side of a
pair (`opensearch`, with `elasticsearch` as another name for it). A
database is a prefix on index names, and a table is an index.
* **Ids.** A document's id is the canonical text of the row's key. A
  batch written again replaced itself: 1,201 documents after a replay,
  not 2,401.
* **Values.** What each column was is kept in the mapping's own `_meta`.
  A decimal goes into `_source` as its text, and it came back as
  `1234.5000` and `1234567890123456.7890`: a JSON number would have lost
  the scale of the first and the digits of the second.
* **Reads.** An index is read in one pass through a scroll, and looked up
  by key through `_mget`. The target is refreshed before migkit reads
  it; the source never is.
* **Indexes migkit did not make** are read by their mapping, each field
  by the class it holds, and keyed by `_id`. An object field stays
  unmapped.
* **Deep check.** A red index is named, since a check reading it is not
  reading everything.

(`tests/test_opensearch_as_a_side_of_a_pair.py`)

*DynamoDB, done (2026-09-25):* on either side of a pair
(`target_engine: dynamodb`, with `endpoint_url` and `region`; a database
is a prefix on table names, `<db>.` by default).
* **Types it does not keep.** DynamoDB keeps a number without the scale
  it was written with: `1.50` came back from the service as `1.5`. It has
  no time type either. So a table migkit creates carries each column's
  class in its tags, and values are read back as that class: a decimal
  at its scale, a time as a time. Where an endpoint takes no tags
  (DynamoDB Local), the description is kept on this machine by endpoint,
  where any hop reading that endpoint finds it.
* **Keys.** A composite key becomes the partition and sort keys. A table
  keyed by three columns, or by none, has no DynamoDB table to go to, and
  is refused by name.
* **Writes and reads.** Items are written by key, so a batch written
  again lands on itself. A table is read in one pass, since items come
  back in no order. Rows are looked up by key through batch reads.
* **Deep check.** Tables that are not active are named, and so is a
  target table without point-in-time recovery, which leaves a cutover
  nothing to go back to.

Measured against DynamoDB Local: PostgreSQL in and back out with every
value as it was. A changed item was a difference, and a replayed batch
left 701 items, not 1,402 (`tests/test_dynamodb_as_a_side_of_a_pair.py`).

**35. Change-stream delivery in the formats consumers expect.**

DTS offers five. migkit should offer:
* Avro, JSON, Debezium and Canal-compatible formats, with a schema
  registry (Redpanda ships one)
* topic and partition rules by table, key or column
* skipping oversize messages, with a count of what was skipped

*Done for Kafka (2026-09-25):* a hop with `engine: hetero` and
`target_engine: kafka` carries the change tail into topics.
* **Formats:** `format` is `json`, `debezium` (the envelope: before,
  after, source, op) or `canal` (data, type, pkNames).
* **Routing:** `topic` is a template, `{db}.{table}` by default.
  `partition_by` names a column. By default the partition comes from the
  row's key, so each key's changes stay in order.
* **Every change is sent, in the source's order.** Changes are not
  collapsed to the row's last state the way table writes are.
* **Oversize messages:** a message larger than `max_message_bytes` is
  skipped, and counted by topic in `stream-skipped.json`.

Found while building it: the tail paired each table with an existing
topic by its last name. A second stream's changes for `o` went to
`rule(a.o)`, a topic nobody read. A stream's destination is now named by
its rule alone. The tests run against Redpanda
(`tests/test_changes_delivered_to_kafka.py`). Still open: Avro and a
schema registry.

**36. Sync in both directions, and other topologies.**

Promoted from the P3 line below. Covers two-way, many-to-one and
one-to-many:
* loops are prevented by tagging each change with its origin:
  PostgreSQL replication origins, MySQL `server_id` and GTID, the
  stream's own source metadata
* conflicts are detected per key and resolved by policy: newer-row-wins
  (item 10), source-wins, or custom
* every conflict is reported, never silently resolved

SymmetricDS and GoldenGate are the references for the mechanism.

*Done for PostgreSQL's own replication (2026-09-25):* these streams write
to the source, so they are off unless the hop's options say so.
`topology: two_way` runs a stream each way. Each subscription is created
with `origin = none`, so it takes only what the other side's own sessions
wrote, and nothing goes round.

Measured with an insert on each side: both rows arrived on both sides,
each side held two rows, and `apply_error_count` stayed 0 on both (an
echo would have been a second insert of the same key). Tearing down with
`--drop` removes both streams. Two-way is refused before PostgreSQL 16 on
either side, because that is where a subscription can first tell the
two apart.

A conflict stops the stream that meets it, and the stream's status says
so. Resolving conflicts by policy needs migkit's own tail and is still
open, as are MySQL two-way and more than two nodes
(`tests/test_streams_back_and_both_ways.py`).

**37. Rollback that loses nothing: live reverse replication.**

At cutover, start a stream from the new target back to the old source,
positioned at the same fence. If the cutover is abandoned, the old side
has every write made since. Verify both directions while it runs. This is
the GoldenGate and Striim failback, with migkit's checks on it.

*Done for PostgreSQL's own replication (2026-09-25):* `reverse:
at_cutover` makes tearing the stream down (`move --mode cdc --drop --go`)
also start a stream back, from the new primary to the old source, from
that moment. The application's roles are given back the target only once
that stream runs.

Measured: a row written on the new primary after cutover reached the old
source, and a row written on the old source no longer went forward.
Without the option, nothing is created on the source. MySQL takes the
same path (a replica on the old source) and is still to be measured
(`tests/test_streams_back_and_both_ways.py`).

**38. Accounts and clouds without long-lived passwords.**
* **Passwordless sign-in to managed databases:** assume-role and workload
  identity, RDS IAM authentication tokens, Cloud SQL IAM
* **Secrets:** AWS Secrets Manager, GCP Secret Manager and Azure Key
  Vault next to the existing Vault
* **Cross-account access:** the cross-account role pattern DTS uses,
  done with each cloud's own mechanism

*Done, checked offline (2026-09-25):*
* **Sign in without a password.** `auth: aws_iam` on an endpoint signs
  in to RDS and Aurora with a token the cloud signs. A token opens
  connections for 15 minutes, so migkit signs a new one once the old one
  is 10 minutes old, and a long run does not start failing halfway.
  `aws_role_arn` signs as a role in another account, assumed for each
  token.
* **Secrets from each cloud's store**, read at load time:
  * `aws-sm:<id>[#field]`: AWS Secrets Manager, taking the region from
    the ARN, and a field from a JSON secret such as the ones RDS keeps
  * `gcp-sm:projects/<p>/secrets/<s>`: Google Secret Manager
  * `azure-kv:https://<vault>.vault.azure.net/secrets/<name>`: Azure Key
    Vault

  Each of these is beside Vault, `env:` and `file:`.

The tests reach no cloud. The token is signed on this machine from
stand-in credentials, and it names the host, the user, the action and
the role's key. The secret store and the role answer through the SDK's
own stub (`tests/test_cloud_sign_in_and_secrets.py`).

Still to do against the real services: a sign-in to RDS with a token,
Cloud SQL IAM, and managed identities.

**39. Converting schema and code between engines, wider.**

Ora2Pg covers Oracle. Add:
* SQL Server T-SQL to PostgreSQL
* MySQL routines to PostgreSQL

Use sqlglot where SQL is enough. Every converted object stays behind
migkit's behavioural proof (plan 20): same inputs, same outputs, on both
sides.

*Done (2026-09-25):* `schema --convert` now carries views and functions
whose body is one expression, between MySQL, PostgreSQL and SQL Server,
in any direction:
* Views are created after the views they read.
* A qualifier naming the source's own database, or a schema holding
  something the move carries, is dropped. Any other qualifier stays, and
  fails on the target rather than pointing at a table of the same name.
* A body of statements, a procedure, or a type with no counterpart is
  written as a comment naming it and saying why. `--apply` leaves any
  object the target already has alone, and lists any it could not
  create.

The proof is in the hetero deep check, `converted code`:
* A view is compared by the digest of its rows.
* A function is compared by its answers, in one read-only query per
  side, to every combination of its arguments' test values: null, the
  edges, a value that rounds, case, a trailing space, a character wider
  than a byte, and dates.
* A converted object the target lacks is a warning, not a pass.

Measured on MySQL to PostgreSQL:
* The translator turns MySQL's `length` (bytes) into PostgreSQL's
  `length` (characters). Only the wide-character input tells them apart,
  and the proof names the function.
* PostgreSQL ignores a function's declared decimal scale, for both its
  arguments and its result, where MySQL rounds. The conversion now states
  the scale as a cast in the body, and the proof caught the version
  without it.

SQL Server as a pair's side:
* Tables, keys, reads, writes, digests, views and functions, read through
  its driver.
* Rendered in-process by the renderer every in-process engine shares.
  The digest of the values the driver returns was measured equal to
  PostgreSQL's in-server digest of the same rows.
* The T-SQL paths are written, but have not yet been run against a
  server, because none runs on this machine's architecture.

Tests: `tests/test_views_and_functions_carried_across_engines.py` and
`tests/test_sql_server_as_one_side_of_a_pair.py`.

**40. Estimates that rest on measurement.**

SCT estimates effort. migkit refused to guess, because it had nothing to
calibrate against. Item 7's per-phase timings from real rehearsals are
that calibration. Estimate time and effort as ranges, and state the runs
each range came from. Without enough runs, say so instead of guessing.

*Done for time (2026-09-25):* the plan estimated from the latest run on
the path alone, however the others had gone. It now gives a range from
every run kept (the last ten), for example "between 5min and 20min going
by the 3 runs on this path (5,000 to 20,000 rows/s, first date to last
date)". A single run is called "one number, not a range".

A hop that has not run yet is given the runs of other hops with the same
engines on the same path, and told whose they were. Typically these are
a rehearsal's runs, for the migration it rehearses. Runs on other engines
are never used. With nothing measured, the plan still estimates nothing.
The tests are in `tests/test_time_from_measured_rates.py`. Effort
estimates for conversion wait on item 39.

**41. Trust, the open-source way.**

A vendor sells certifications. What an open tool can offer instead:
* signed releases, an SBOM and build provenance (SLSA)
* a written threat model
* an audit log of every write migkit makes, on either side
* reports encrypted at rest
* redaction of values in what is shown and stored (with G2)

*Done (2026-09-25):*
* **Threat model.** `docs/threat-model.md` covers what migkit is given,
  what it writes and where, its secrets, the data in reports, and the
  processes it runs. Each claim names the test that holds it.
* **A finding while writing it: the local copy.** A dump-and-load move
  keeps the source's whole data in files under the report directory.
  That copy was removed only after a load that succeeded. A load that
  failed or was stopped left it readable by anyone who could read the
  directory. Now the copy is readable by this user only (0700) before
  anything is written into it, and it is removed when the move ends,
  whichever way it ends (`tests/test_the_local_copy_is_private_and_goes.py`).
* **Redaction.** Redaction was done with G2 (`mask`). Notifications and
  the diagnostics bundle carry no values.
* **Audit log.** `changelog.jsonl` records each operation.

*Then (2026-09-25):*
* **Releases** carry a CycloneDX SBOM of the wheel as installed, and build
  provenance signed through GitHub's Sigstore attestation. The SBOM
  command was run here: 51 components. The attestation steps run on the
  next tagged release.
* **Restore points** are encrypted with `MIGKIT_STATE_KEY` before they
  leave the working directory, for the mirror and the bucket. A restore
  point holds whole rows. Measured: without the passphrase, or with
  another one, a point is refused rather than read (`tests/test_state.py`).

Still open: the report directory's own files at rest (drilldowns). Mask
the values with `mask`, and use the disk's encryption for the rest.

**42. Support, without an SLA.**

A troubleshooting guide keyed by every error migkit can raise, each with
its fix. A diagnostics bundle collected by `doctor` with secrets and
values redacted, so a problem can be reported without handing anything
over (an environment variable, not a new flag).

*Done (2026-09-25):*
* **`docs/troubleshooting.md`** is generated from the code. It lists
  every refusal written where it is raised, 129 of them, grouped by the
  part of migkit that raises it. Each message already says what to do.
  A test fails when the guide falls behind the code, and it names no
  program.
* **`MIGKIT_DIAGNOSE=<file>.zip migkit doctor`** writes a bundle to
  report a problem with. It holds versions, what each engine can do
  here, the configuration, and each hop's last verdict and change log.
  Passwords, tokens, keys, notification addresses and credentials inside
  an address are taken out before anything is written, and so is every
  finding's detail, which is where keys and values are
  (`tests/test_a_problem_can_be_reported_without_handing_anything_over.py`).

### Soon

**43. AI assistance, any provider.**

One provider interface: any OpenAI-compatible endpoint, Anthropic,
Google, local models. For:
* suggesting conversions (items 39 and 20)
* explaining a finding in plain language
* drafting a fix

What it produces is treated as a proposal: it goes through the same
verification and behavioural proof as anything else, and is never applied
on trust. Off unless a provider is configured.

*First use (2026-09-25):* `MIGKIT_AI` picks the provider:
* `openai`: any OpenAI-compatible endpoint, including local models, at
  `MIGKIT_AI_URL`
* `anthropic`
* `google`

The key is `MIGKIT_AI_KEY` and the model `MIGKIT_AI_MODEL`. migkit chooses
no model for an OpenAI-style endpoint.

After a `check` that found something, the provider's plain-language
account of the findings is printed under the verdict. It is marked as a
proposal, and changes nothing.

What is sent is each finding's check, scope, status and category. The
detail, where keys and values are, is sent only with
`MIGKIT_AI_SHARE=detail`, and never for a hop that masks values. A
provider that cannot be reached is said, and the check's result stands.
The tests use a local stand-in for each API's own shape
(`tests/test_help_from_a_model_any_provider.py`).

*Conversions (2026-09-25):* a view or function the translator cannot
carry can get a proposal from the provider, when `MIGKIT_AI_SHARE`
includes `code`:
* **What is sent.** The object's own definition, from the source (`show
  create` on MySQL, `pg_get_functiondef` and `pg_get_viewdef` on
  PostgreSQL). No row is sent.
* **What comes back.** One `create` statement, taken from a fenced block
  or from the answer. It goes into the converted file marked as a
  proposal, and only `--apply` puts it on the target.
* **The proof.** The proof now covers every view and function of the
  source: the translator's, a model's, or a person's. Each is asked the
  same inputs on both sides. One the target does not have is named. A
  procedure answers nothing to compare, and is counted as such.

Measured with a local stand-in provider: of two proposals, the one that
doubled where the source doubled and added one was named as answering
differently. Once rewritten by hand, both passed. Without `code` in
`MIGKIT_AI_SHARE`, nothing was sent
(`tests/test_a_model_proposes_what_the_translator_cannot_carry.py`).

Still to use it for: drafting fixes, which go through the same proof as
anything else.

### Not pursued

* **ETL and transformation**, beyond the mapping migkit already has.

---

## Order of work

0. **0e runs underneath everything that follows:** the declared matrix
   and its test first, then each engine's gaps filled as the items that
   touch them come up, so no item lands for one engine only.
1. **0d, then 0c:** 0d is a verdict that is wrong today on every hop with a
   row filter; 0c is the owner's rule applied to what already exists.
2. **0 and 0a together:** the planner is what makes each later wrap
   worth having.
3. **0b** once the pipeline can be stood up; the source-write question it
   started from is already answered (no).
4. **Then 1-5:** each one closes a way a cutover goes wrong without anyone
   seeing it.
5. **Then 6-10, and 28** (the benchmark) as soon as there is something
   worth measuring.
6. **Then 29-32 and 44-46**, the control plane, scale, and the proof
   that it holds under failure, at size and across versions, before the
   reach items.
7. **Then 33-42** by what the next real migration needs; 43 when the
   rest can check what it proposes.

## Sources

* AWS DMS: [data resync](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.DataResync.html), [resync settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.DataResyncSettings.html), [data validation](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html), [premigration assessments](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.Assessments.html), [PostgreSQL](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.PG.html), [MySQL](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.MySQL.html)
* Azure: [migration service overview](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/overview-migration-service-postgresql), [premigration validations](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-premigration-migration-service), [best practices](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql)
* Google: [DMS source configuration](https://cloud.google.com/database-migration/docs/postgresql-to-alloydb/configure-source-database), [known limitations](https://docs.cloud.google.com/database-migration/docs/postgres/known-limitations), [verify a migration](https://docs.cloud.google.com/database-migration/docs/postgres/verify-migration), [Data Validation Tool](https://github.com/GoogleCloudPlatform/professional-services-data-validator)
* Tencent DTS: API `2021-12-06` models (SDK 3.1.157), [check items](https://cloud.tencent.com/document/product/571/61639), [consistency check](https://www.tencentcloud.com/document/product/571/42724)
* [Veridata: how it works](https://blogs.oracle.com/dataintegration/oracle-goldengate-veridata-how-it-works), [Veridata jobs](https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDUG/working_with_jobs.htm)
* [pgCompare](https://github.com/CrunchyData/pgCompare), [issue #103](https://github.com/CrunchyData/pgCompare/issues/103)
* [Vitess VDiff v2](https://vitess.io/blog/2022-11-22-vdiff-v2/)
* [Debezium incremental snapshots](https://debezium.io/blog/2021/10/07/incremental-snapshots/), [read-only variant](https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/)
* [MongoDB migration-verifier](https://github.com/mongodb-labs/migration-verifier)
