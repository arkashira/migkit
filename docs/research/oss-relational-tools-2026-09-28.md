# Open-source relational tools and libraries: full-feature wrap plans, decision rules, scorecard (2026-09-28)

**STATUS (2026-09-28):** complete: §0 inventory, §1 PostgreSQL movers,
§2 MySQL tools, §3 SQL Server/Oracle, §4 readers/ELT/CDC services,
§5 verifiers, §6 schema tools, §7 drivers/codecs, §8 prioritised table,
§9 scorecard, §10 sources | partial: items marked "(verify)" (pgcopydb
binary-COPY gain, sqlpackage/XtraBackup arm64, Compass rewrite coverage,
DuckDB snapshot sharing, skeema rule names, `results` licence) - each one
docker run or one page away | not yet: none.

Public research plus a read of migkit's code (read-only; nothing run,
installed or connected). Sibling of `oss-nonrelational-tools-2026-09-28.md`
and uses its conventions: `file:line` is migkit as of 2026-09-28; a verdict
is migkit's standing against the tool (`ahead`/`equal`/`behind`); `[key]`
is a source in §10; "(verify)" = a fact a docker run or one more page must
settle. Wrapping follows the owner's rules (backlog "P0: the decision
layer", *Decided 2026-09-27*; 0f): pip first, binaries via
`migkit doctor --install`, no program named to the operator, **no new CLI
mode or flag**, the decision engine picking tool and capability per table.

Not repeated here, cited instead:
* security facts per tool: `security-oss-tools-2026-09-27.md` (SEC §tool)
  and `security-throughput-scorecard-2026-09-28.md` (SCORE A2/B2);
* published rates: `throughput-published-2026-09-27.md` (TP §n) and
  `throughput-cdc-techniques-2026-09-27.md` (TC);
* watermark/DBLog family, Debezium internals, pglogical/Bucardo/SymmetricDS
  apply, PeerDB/Sequin/pgstream mechanisms:
  `mechanisms-cdc-elt-specialists-2026-09-28.md` (MECH §n);
* Alibaba/Tencent DTS, TiDB DM/Lightning/sync-diff, Vitess, MOLT, Voyager:
  `mechanisms-dts-tidb-vitess-molt-voyager-2026-09-28.md` (DTS §n);
* the measured facts of the two paused 0f agents (pgcopydb/Debezium and
  DVT/reladiff/datacompy), `.claude/worktrees/agent-ad4afda9caaa364c0/WIP.md`
  (WIP-A) and `agent-a0756089f57ed9b55/WIP.md` (WIP-B).

---

## 0. What migkit holds today (grepped before any gap is claimed)

**Decision layer.** `movers.pick` `movers.py:36-62` is still flat: for a
whole database it returns the first installed tool per engine
(pgcopydb > pg_dump > mydumper > pgloader > mongosync > mongodump > native >
builtin) and `builtin` for any single table. `fitted` `movers.py:87-126`
downgrades pgloader (non MySQL->PG pair, excludes, mappings) and mongosync.
`chosen` `movers.py:65-84` honours `MIGKIT_MOVER` (env, not a flag).
`planner.plan` `planner.py:55` decides BULK / COPIER / LEFT per table from
the hop's rules and `table_facts`; `record_rate`/`measured_rate`/`estimate`
`planner.py:119-179` keep rows/s per path. PostgreSQL's `_verify_way`
`postgres.py:7311` is the only measured climb (digest vs read-back).
`ROW_FILTER_MOVERS = ("mydumper",)` `movers.py:132` (measured: pg_dump 18.6
and pgcopydb 0.18 have no row predicate).

**Driven programs** `movers.py:271` DRIVEN, `PROGRAMS` `movers.py:3535`,
`MEASURED` versions `movers.py:3545`, `options_missing` `movers.py:3559`
(every long option held against the installed build's `--help`),
`_long_options` `movers.py:1857` (top-level `--help` only; WIP-A: pgcopydb
lists options per subcommand, so the check sees none of them).

| program | argv migkit builds today | file:line |
|---|---|---|
| pg_dump/pg_restore (data) | `-Fd -j N --data-only -v`, `pg_restore -j N` | `movers.py:958-965` |
| pg_dump/pg_restore (schema) | `-Fc --schema-only --section=pre-data/post-data`, `-t/-T`, `--no-owner --no-privileges` | `movers.py:1206-1213` |
| pgcopydb | **`copy table-data` only**: `--table-jobs N --dir --source --target [--resume --not-consistent] [--filters]`; docker image `dimitri/pgcopydb:latest` when not local | `movers.py:2816-2843`, `:2665` |
| pgcopydb follow | `follow --dir --slot-name --origin --plugin ... --not-consistent`, `stream sentinel` for apply | `movers.py:2990-3160` |
| mydumper/myloader | `--threads N --no-schemas --trx-tables` (or `--trx-consistency-only`), `--defaults-file` (per-table `where`), `--omit-from-file`, `--enable-binlog`, `--machine-log-json -v 3` | `movers.py:2128-2150` |
| pgloader | one-pass MySQL->PG load file, whole db | `movers.py:87-126` |
| pt-table-sync | `--print`, bounded to verified keys, only if installed | `mysql.py:4090-4094` |
| atlas | `schema diff --config` | `movers.py:1764` |
| DVT (second reader) | pinned `google-pso-data-validator==8.9.3` in its own venv; column validation count/sum/min/max only | `tools.py:40-45`, WIP-B |
| reladiff | CLI `-j`, `--stats` (generic), `-j -c %` (MySQL, behind hop option) | `generic.py:44-79`, `mysql.py:1744` |

**Libraries in `pyproject.toml`** (all hard deps): psycopg2-binary,
`psycopg[binary,pool]` (v3), pymysql, pymssql, oracledb, ibm_db, pyodbc,
reladiff, datacompy, pandas, pyarrow, sqlglot, duckdb, `results` (migra's
successor), zstandard, mysql-replication, fastavro, psutil, pyrage.

**Techniques already built (do not re-propose):**
* PG ctid page ranges `postgres.py:714-741`; key-quantile ranges and
  resumable spans `ranges.py:222-342`; workers from host/servers/link
  `sizing.py:95-204` (R1); throttle on source stress `throttle.py`.
* PG table copy = `psql \copy (select) to stdout | psql \copy from stdin`
  (text) `postgres.py:7086-7094`; relay beside the source through ssh with
  `zstd -3 -T0` `postgres.py:7143-7177` (R17d); psycopg3 `cursor.copy`
  `write_row` into a temp stage for keyed upserts `postgres.py:832-860`;
  server-side named cursor with `itersize` `postgres.py:780`.
* Range digest = `count || sum(first 64 bits of md5(row))` on both servers
  `postgres.py:2259-2309`, `:6828`; read-back vs digest measured per run
  `postgres.py:7271-7315`.
* MySQL: pymysql, `executemany` batches `mysql.py:316`; **LOAD DATA LOCAL
  answered only with the prepared chunk** `mysql.py:6082-6263` (R19 lever 6
  built); binlog via `mysql-replication`, compressed transaction payloads
  decoded `binlog_payload.py:79`.
* SQL Server/Oracle/Db2/ASE share `DbapiRows` (`dbapi.py:175-210`):
  delete-by-key then `executemany` insert. **No bulk path**:
  `capabilities.py` GAPS mssql `bulk-move: NOT_YET 0e`; oracle and db2
  bulk-move/stream/fence NOT_YET (items 11, 34).
* Cross-engine: `hetero.convert_ddl` from `neutral_create_sql`
  (`hetero.py:2044`, own type map, sqlglot transpile retired);
  `converted_code` views + single-expression functions via sqlglot
  (`hetero.py:2160`); sqlglot parses T-SQL routines `mssql.py:737-802`
  and PG predicates `hetero.py:1598-1625`.
* Online-DDL leftovers recognised (gh-ost `_gho/_ghc/_del`, pt-osc
  `_new/_old`) `leftovers.py:41-46`, `drift.py:15`; advisors text only
  mentions gh-ost/pt-osc/xtrabackup/pglogical `advisors.py:89-126`.

**Not referenced anywhere in `migkit/` (grep count 0):** MySQL Shell
(`mysqlsh`, `dumpInstance`, `copyInstance`), `CLONE INSTANCE`,
`pg_basebackup`, pg_chameleon, Bucardo, pgsync, Spock, pg_easy_replicate,
sqlpackage, bcp, Babelfish, Sling, ConnectorX, ADBC, Arrow Flight, Polars,
sqlfluff, Flyway, skeema, pgstream, Sequin, asyncpg, orjson, msgspec,
xxhash, blake3, lz4, pg_comparator, pg_repack. (xtrabackup, gh-ost,
pt-osc, pglogical, ora2pg appear only in advisory text or leftovers.)

---

## 1. PostgreSQL movers and replicators

Format per tool: **(1) lacks/slower** (after grep) and the algorithm,
**(2) wrap**, **(3) decision rule**, **(4) how migkit beats it on top**.

### 1.1 pgcopydb (dimitri; PostgreSQL licence)

(1) migkit runs only `copy table-data` (`movers.py:2816`); 0f lists what is
unused and WIP-A confirmed per subcommand on 0.18. The options that change
throughput, from the `clone` reference [pgcopydb-clone]:
* **Same-table concurrency**: `--split-tables-larger-than SIZE` and
  `--split-max-parts N` split one table into N COPY jobs "via CTID ranges or
  integer key columns; CTID is default when no unique integer column exists";
  `--skip-split-by-ctid` turns the ctid split off. `copy table-data` has the
  split options too (WIP-A). migkit's own copier splits by key quantiles and
  ctid (`ranges.py:222-342`, `postgres.py:714-741`), but the pgcopydb path
  runs whole tables per job, so one 1 TB table is one stream there.
* `--index-jobs` (CPU count recommended) / `--restore-jobs` (default =
  index-jobs) / `--large-objects-jobs`: `clone` builds each table's indexes
  as soon as its COPY ends, concurrently with other tables' COPY, and uses
  `ALTER TABLE ... ADD CONSTRAINT ... USING INDEX` for keys. migkit's
  `_IndexWindow` (`movers.py:819`, measured 1.9x on PG16) rebuilds after the
  load, not overlapped per table.
* `--use-copy-binary` (clone/copy db only, not `copy table-data`).
* `--estimate-table-sizes` (stats instead of `pg_table_size` scans on huge
  catalogs), `--skip-vacuum`/`--skip-analyze` (migkit analyses itself,
  `postgres.py:3420`), `--skip-extensions/--skip-collations/
  --skip-db-properties/--skip-ext-comments`, `--no-role-passwords`,
  `--roles`, `--no-acl`, `--no-comments`, `--no-tablespaces`,
  `--fail-fast`, `--restart`, `--requirements` (JSON of extension versions),
  `--all-databases`, `--follow` + `--endpos` in the same process.
* `list progress --json --summary` (per table/index/constraint timings)
  and `list table-parts` (the split plan, dry) [pgcopydb-list].

(2) Wrap: binary via `doctor --install` (brew/apt) or the version-matched
image (`movers.py:2665`); PostgreSQL licence (permissive). Surface =
subcommand-aware `_long_options(program, *sub)` (WIP-A step 2) so
`options_missing` stops being blind; `PGCOPYDB_OPTIONS` table per
subcommand, each option either valued from facts or with a written
reason; progress from `list progress --json --summary` into
`planner.record_tables`; lag from `stream sentinel get --json` into
`/metrics` (WIP-A). Errors: stderr lines passed on with the program's name
removed (existing `DRIVEN` rule `movers.py:271`).

(3) Decision rule (per database, then per table):
* `clone` instead of `copy table-data` when the target has **no** schema
  yet and no hop mapping/row filter touches the table set (else the
  existing `copy table-data` + migkit's own DDL).
* `--split-tables-larger-than = max(total_bytes / workers,
  single_stream_rate x 10 s)`, `--split-max-parts = workers`, only for
  tables whose bytes exceed it (facts: `table_facts`, `_source_sizes`,
  measured single-stream rate once `list progress` has run once). ctid split
  kept unless the table has a unique integer key (pgcopydb then chooses the
  key itself) or is being rewritten (`pg_stat_user_tables.n_tup_upd` rate
  high -> `--skip-split-by-ctid`).
* `--index-jobs = min(target vCPU, maintenance_work_mem budget /
  largest index build)`; `--restore-jobs` = same; `--large-objects-jobs =
  workers` only where `pg_largeobject_metadata` count > 0 (else
  `--skip-large-objects`).
* `--use-copy-binary` only when both majors are equal **and** no column is
  of a type whose binary I/O differs across builds (user-defined types,
  extensions' types) - and never where migkit's read-back must tally text
  (R18.1 note: binary gives up the text tally; the range digest still
  proves it). (verify: measured speed-up on the docker pair.)
* `--fail-fast` always (migkit restarts a failed table itself);
  `--restart` when `begun` exists but `--resume` is refused (WIP-A to
  measure); `--estimate-table-sizes` when the catalog holds > 10k relations.
* Fallback: pg_dump/pg_restore (1.2), then the builtin copier.

(4) On top: migkit digests every range as it lands and re-checks behind an
LSN fence (`pgcopydb compare data` is after the fact and whole-table);
compresses over the link through the relay (pgcopydb sends plain COPY);
sizes jobs from host/server/link and backs off on source stress (pgcopydb
fixed); resumes a split table by part (pgcopydb resumes by whole table
under `--not-consistent`); routes excluded/filtered tables to the copier
(`routed_to_copier`, `movers.py:2785`).

### 1.2 pg_dump / pg_restore (PostgreSQL licence, in every install)

(1) Used: `-Fd -j N --data-only` and `pg_restore -j N` (`movers.py:958-965`);
`--section=pre-data|post-data` for schema (`:1206-1213`). Not used, and
worth it [pg-dump][pg-restore]:
* `pg_dump --snapshot=<exported>` so the dump is consistent with a slot
  made first (the "slot before snapshot" rule; migkit's CDC with a
  pg_dump bulk leg would otherwise need the pgcopydb path).
* `pg_restore --section=data` + `post-data` split so indexes/constraints
  build under `-j` **after** migkit's own data path (lets migkit use
  pg_restore only for the parallel post-data build).
* `pg_restore -l / -L` (TOC list reordered: largest tables first, so the
  longest job starts first - the classic parallel-restore tail fix).
* `--compress=zstd[:level=N,long]` (PG16+) on `-Fd` - halves the dump's
  disk and I/O where the dump crosses a disk or network.
* `pg_restore --transaction-size=N` (PG17+; "a series of transactions,
  each processing up to N database objects", implies `--exit-on-error`;
  caps lock-table use where `--single-transaction` cannot be used with
  `-j`) [pg-restore-17] - for schemas with tens of thousands of objects.
* `pg_dump --statistics` (PG18; **off by default**, `--no-statistics` "is
  the default"; excludes `CREATE STATISTICS` objects and extension stats)
  [pg-dump-18] - carries planner stats so a PG18 target is plannable
  before migkit's `_analyze_in_stages` (`postgres.py:3420`) finishes.
* `--filter FILE` (PG17+) - one file of include/exclude patterns instead
  of many `-t/-T` argv entries (`movers.py:1204-1206`), which also keeps
  table names off the process list.
* `--load-via-partition-root` when the target's partitioning differs.

(2) Wrap: already wrapped; add options via the same `options_missing` gate.

(3) Rule: chosen when pgcopydb is absent or refused (foreign keys that stop
its per-table truncate, `movers.py:2852-2863`), or when the source and
target are separated by a disk hop (dump to a file, carried). `-j` =
workers; TOC ordered by `table_facts` bytes descending; zstd when the dump
leaves this host; `--snapshot` whenever a follow leg exists.

(4) On top: same as 1.1 - per-range digests after the restore instead of
none; resumable by table where pg_restore restarts from zero.

### 1.3 pg_basebackup (physical rung, R19.1)

(1) Not referenced in migkit (grep 0). Algorithm: streams the cluster's
files over the replication protocol; `--compress=server-zstd:level=N,
workers=W[,long]` compresses on the server with W threads before the wire
(server-side compression PG15+; zstd workers) [pg-basebackup];
`-X stream` carries WAL in parallel; `--manifest-checksums=SHA256` writes a
per-file manifest that `pg_verifybackup` checks; `-C -S slot` creates the
slot **before** the copy (gap-free tail); `--incremental=<manifest>` (PG17)
sends only changed blocks, merged by `pg_combinebackup` - the re-sync
before cutover; `-r/--max-rate` throttle; `-P` progress; `-R` writes
standby config.

(2) Wrap: binary from the PostgreSQL client package (already required for
pg_dump); PostgreSQL licence. Surface: a `physical` rung of the bulk
decision; progress from `-P` lines and `pg_stat_progress_basebackup` on
the source; errors named as "the page copy". Target is a new data
directory (cluster), so this applies to self-managed targets only.

(3) Rule: same major version on both sides, same architecture/page size,
target is a fresh self-managed cluster (not RDS/Cloud SQL - they refuse
replication-protocol base backups), user has `REPLICATION`, and the move
is the whole cluster (no excludes, no mappings, no row filters). Then:
`server-zstd` with `workers = source spare vCPU` when the link is slower
than the source's disk (bytes/s measured by the relay probe), client-side
otherwise; `--incremental` for the second pass before cutover when the
source is PG17+ with `summarize_wal=on`; tail from the pre-made slot.
Fallback: pgcopydb.

(4) On top: migkit turns it into a verified migration - `pg_verifybackup`
over the manifest, then migkit's logical checks on a sample of tables and
the slot's tail fenced by LSN; pg_basebackup alone neither tails nor
verifies logically.

### 1.4 pglogical, Spock, pg_easy_replicate, Bucardo (logical/trigger replicators)

Mechanisms are in MECH §4 (pglogical `last_update_wins`, Bucardo triggers,
SymmetricDS). New facts here only.

* **pglogical** (2ndQuadrant/EDB; PostgreSQL licence): extension on both
  sides; row filters, column lists, `synchronize_data`, conflict
  resolution, sequences sync. migkit follows with its own pgslot reader
  (`pgslot.py`) and pgcopydb follow; pglogical adds nothing native PG 15+
  logical replication lacks except conflict handlers and PG<10 sources.
  **Rule**: only for sources PG 9.4-9.6 (no native publication), extension
  installable; fallback none (say so). Beat it: migkit's exact batches and
  cross-engine targets.
* **Spock** (pgEdge; relicensed from the pgEdge Community Licence to the
  PostgreSQL licence on 2025-09-10 [pgedge-oss]; PG 15-19):
  multi-master logical replication with conflict-free delta columns,
  auto-DDL replication. **Rule**: only when the hop is a two-way PG<->PG
  topology (R3) and both sides can load the extension; migkit's R3 mark
  and exact batches remain the default because they work across engines.
* **pg_easy_replicate** (Shopify; MIT; Ruby gem): orchestrates native
  logical replication PG->PG, `bootstrap`, `start_sync`, `stats`,
  `switchover` (waits for lag < threshold, locks the source user with
  `REVOKE`/`ALTER USER NOLOGIN`, syncs sequences, drops the publication).
  migkit already does each step itself (sequences, fence, cutover); it is a
  reference for the **switchover choreography** (lock the app user, not
  the database), not a dependency.
* **Bucardo** (BSD; Perl, triggers + a daemon): multi-master with
  conflict strategies (`bucardo_latest`, custom Perl). Trigger capture
  costs every write on the source; never chosen by migkit where logical
  decoding exists. Rule: PG source without `wal_level=logical` and no
  restart allowed (so no slot) - then migkit's own trigger capture
  (backlog, not Bucardo) is the better rung.

### 1.5 pg_chameleon (BSD-2; Python), pgloader (PostgreSQL licence; Lisp), pgsync (MIT; Ruby)

* **pg_chameleon**: MySQL->PG replica in Python on `python-mysql-replication`
  (the same library migkit uses, `pyproject.toml`), init_replica with
  type mapping, per-table replay, `--rollbar`. migkit's hetero tail
  (`stream_supported` hetero `movers.py:261`) already covers this; take
  **only** its type edge cases as test fixtures (e.g. geometry, enum,
  zero dates). Not wrapped.
* **pgloader**: used for MySQL->PG whole-db one-pass (`fitted`,
  `movers.py:87`). Unused and useful: `WITH workers = N, concurrency = M,
  batch rows = R, prefetch rows`, `CAST` rules per column, `BEFORE/AFTER
  LOAD DO` SQL, `ALTER SCHEMA ... RENAME`, `INCLUDING/EXCLUDING TABLE
  NAMES MATCHING` (which would lift the "no exclude list" refusal at
  `movers.py:117`), and MS SQL and SQLite sources [pgloader]. Rule:
  source MySQL/SQLite/MSSQL -> PG, no row filter; generate the load file
  from the hop's excludes and mappings instead of refusing; `workers` from
  sizing. Beat it: pgloader has no verification and restarts from zero.
* **pgsync** (ankane): table-by-table PG->PG with `--defer-constraints`,
  data rules (`unique_email`, `random_int`, `null`, `untouched`) for
  anonymised copies, `--in-batches`/`--sleep`. migkit's `masking.py`
  covers display masking; pgsync's data rules are **write-side** masking.
  Not wrapped (Ruby); the rule set is the reference for the owed
  write-side masking (see mydumper masquerade §2.1).

---

## 2. MySQL movers, online-DDL tools and checkers

### 2.1 mydumper / myloader (GPL-3.0; driven binary, never linked)

(1) Used: `--threads --no-schemas --trx-tables|--trx-consistency-only
--defaults-file (per-table where) --omit-from-file --enable-binlog
--machine-log-json` (`movers.py:2128-2150`). Unused [mydumper][myloader]:
* dump: `--rows MIN:START_AT:MAX` (adaptive chunk size within one table -
  the same-table parallelism; without it a big table is one thread),
  `--chunk-filesize`, `--compress zstd`, `--checksum-all` /
  `--data-checksums` / `--schema-checksums` (checksums written into the
  metadata at dump time), `--sync-thread-lock-mode
  SAFE_NO_LOCK|FTWRL|LOCK_ALL|GTID`, `--source-data`/`--replica-data`
  (binlog/GTID position in metadata for the tail), `--split-partitions`,
  `--order-by-primary`, `--format INSERT|LOAD_DATA|CSV`, `--exec-per-thread`
  (a filter on each output stream, e.g. `zstd`), `--stream` (dump to stdout
  for myloader to read while it is written).
* **masquerade** (write-side masking, in the same defaults file migkit
  already writes): `` [`db`.`table`] `col`=random_int|random_string|
  random_format '<number 3>'-'<file names.txt>' `` [mydumper-mask];
  multi-mask per column and "any db/any table" sections recently (#2176,
  #2179); NULL handling fixed in 1.0.1 (#2215) - so migkit's measured
  1.0.5 (`movers.py:3546`) has the fix.
* load: `--optimize-keys AFTER_IMPORT_PER_TABLE|AFTER_IMPORT_ALL_TABLES|
  SKIP` (secondary keys built after the rows; the alias
  `--innodb-optimize-keys` on older builds), `--checksum skip|fail|warn`
  (default warn), `--resume` (a resume file in the dump dir),
  `--disable-redo-log` (8.0.21+), `--max-threads-per-table`,
  `--max-threads-for-index-creation` (default 4), `--queries-per-transaction`,
  `--retry-count`, `--set-gtid-purged`, `--stream` modes, `--skip-post`,
  `--skip-triggers`, `--ignore-errors`.

(2) Wrap: unchanged (brew/apt via `doctor --install`, `tools.py:28-29`);
GPL-3.0 is fine to drive as a separate process (never import or bundle).

(3) Rules:
* `--rows` = `max(50_000, rows_per_s_measured x 5 s)` as START, MIN =
  START/10, MAX = START x 10, only for tables with rows > workers x START
  (facts: `table_facts` rows); `--max-threads-per-table = workers`.
* `--optimize-keys AFTER_IMPORT_PER_TABLE` **only** where measured faster
  on that server (backlog: "indexes deferred only where measured faster
  (PostgreSQL yes, MySQL no)"); default SKIP until the benchmark says so.
* `--checksum fail` + `--checksum-all` always: a free second reading
  computed by the dumper, and migkit still runs its own range digests.
* `--compress zstd` when the dump directory is not on the target host;
  `--stream` when it is (no disk round trip).
* `--source-data` whenever a tail follows (position recorded by the dumper
  under its own lock, the gap-free start).
* `--disable-redo-log` only when the target is not a replica, not
  managed, 8.0.21+, and the operator's hop marks the target as disposable
  until cutover (said in the plan); never on a target with binlog replicas.
* masquerade only from a hop rule that asks for write-side masking; never
  from `mask` (display only - WIP-A note).
* Fallback: MySQL Shell (2.2), then the builtin copier with LOAD DATA
  (`mysql.py:6254`).

(4) On top: per-range digest as it lands vs mydumper's per-table checksum
after; resume by range (myloader resumes by file); workers sized and
throttled on source stress.

### 2.2 MySQL Shell `util.dumpInstance / loadDump / copyInstance` (GPLv2 + OpenSSL linking permission; the UFE covers connectors, not the Shell [mysqlsh-lic])

(1) Not referenced (grep 0). What it has that migkit's MySQL paths lack:
* `copyInstance/copySchemas/copyTables`: dump and load **streamed, no
  intermediate files**, N reader + N writer threads, `bytesPerChunk`
  (default 64M), `maxRate` per thread, `where`, `partitions`,
  `compatibility` (`strip_definers`, `create_invisible_pks`,
  `strip_restricted_grants` ...), `dryRun`, `handleGrantErrors` [mysqlsh-copy].
* `loadDump`: `waitDumpTimeout` (load while the dump is still being
  written), `progressFile` + automatic resume with de-duplication of
  partially loaded chunks, `deferTableIndexes off|fulltext|all`,
  `analyzeTables on|histogram`, `updateGtidSet append|replace` with
  `skipBinlog`, `loadUsers`, `maxBytesPerTransaction`, `sessionInitSql`,
  **`checksum`** (8.0.27+, errors on mismatch) [mysqlsh-load].
* Published >200 MB/s load (TP §7); target needs `local_infile=ON`.

(2) Wrap: `mysqlsh` binary via `doctor --install` (brew `mysql-shell`,
apt `mysql-shell` from the MySQL APT repo); driven with `--py -e` or
`--file` of a generated Python script calling `util.*`, credentials in a
login-path or an option file (never argv); progress from
`showProgress` lines and the load's `progressFile` JSON; the program's
name hidden like the others (`DRIVEN`).

(3) Rule: MySQL->MySQL (or ->HeatWave) whole database or schema set,
target `local_infile=ON` (or migkit may turn it on), no per-table column
mapping. `copyInstance` when source and target are both reachable from the
host and the link is not the bottleneck; `dumpInstance` to a local dir +
`loadDump waitDumpTimeout` when the link is slow (then zstd dump files cross
it once). Picked over mydumper when the benchmark says so (both are rungs of
the same "MySQL bulk" part); `threads = workers`, `bytesPerChunk` from
the measured rows/s x 5 s x avg row bytes, `maxRate` from the throttle,
`checksum: true` always; mydumper instead where binary/BLOB columns are
a large share of the bytes (the Shell base64-encodes binary columns and
lost the wikipedia dump to mydumper in Oracle's own benchmark, TP §7);
`updateGtidSet: append` + `skipBinlog` when a
GTID tail follows on a self-managed target, `deferTableIndexes all` only
where measured faster. Fallback mydumper.

(4) On top: range digests as it lands, cross-engine targets (Shell is
MySQL only), and one mover-independent verdict.

### 2.3 MySQL CLONE plugin (server feature, GPLv2 server) and XtraBackup (GPLv2)

(1) Neither is used (CLONE grep 0; xtrabackup only in advisory text
`advisors.py:124`). CLONE [mysql-clone]: page-level copy of all InnoDB
data, redo and undo over the MySQL protocol; recipient `CLONE_ADMIN`,
donor `BACKUP_ADMIN`; same series (8.4.x to 8.4.y), same OS/platform,
charset/collation, `innodb_page_size`; `clone_valid_donor_list`;
`max_allowed_packet >= 2 MB`; only InnoDB carried (other engines empty);
replaces the recipient's data unless `DATA DIRECTORY`; progress and the
binlog file/position and GTID set in `performance_schema.clone_status`;
tuning `clone_autotune_concurrency`, `clone_enable_compression`,
`clone_max_data_bandwidth`; SSL via `clone_ssl_*`.
XtraBackup [xtrabackup]: hot physical backup of InnoDB with `--parallel`,
`--compress=zstd`, `xbstream` streaming to another host, `--prepare`,
`xtrabackup_binlog_info` for the tail; version must match the server's
(8.0.x series tracks server minors).

(2) Wrap: CLONE = SQL only (pymysql, already a dependency); XtraBackup =
binary via `doctor --install` (percona repo; GPLv2 driven).

(3) Rule (R19.1 "physical where the source allows it"): CLONE when both
sides are self-managed MySQL 8.0.17+ of the same series and platform, all
tables InnoDB (`information_schema.tables.engine`), privileges present, the
target is disposable (whole instance), and the move is the whole instance
without excludes/mappings. Then tail from `clone_status` binlog
position/GTID. XtraBackup when the same holds but CLONE is refused
(plugin not installable, version mismatch within a series XtraBackup
supports, or the donor must not be contacted by the recipient directly);
compressed `xbstream` over the relay leg. Refused (and said) on RDS/Aurora/
Cloud SQL. Fallback: MySQL Shell / mydumper.

(4) On top: migkit verifies after a physical copy (range digests on a
sample weighted by size, plus full `CHECKSUM TABLE`-free digests on
tables the tail touched), and starts the tail from the exact position the
clone reports - neither tool does either.

### 2.4 gh-ost (MIT) and pt-online-schema-change (GPLv2)

(1) Neither is driven; migkit recognises their shadow tables
(`leftovers.py:41-46`, `drift.py:15`). **New gap found:** gh-ost now has
`--checkpoint`/`--resume`, writing state to a `_<table>_ghk` table
[gh-ost-flags]; `leftovers.py:45` lists only `_gho`, `_ghc`, `_del`, so a
checkpoint table left by a paused gh-ost is not recognised as a leftover
(S fix: add `("_", "_ghk")`). gh-ost's mechanism: triggerless, copies rows
by `--chunk-size` (default 1000) and applies the binlog to the ghost
table in `--dml-batch-size` groups; throttles on `--max-lag-millis`
across `--throttle-control-replicas`, `--max-load`, `--critical-load`;
`--postpone-cut-over-flag-file`, atomic cut-over with
`--cut-over-lock-timeout-seconds`, `--serve-socket-file` for live
commands, `--migrate-on-replica`/`--test-on-replica`, hooks.
pt-osc uses triggers; since Percona Toolkit 3.6.0 (2024-06-12) it resumes
from the last chunk with `--resume=<job id>` when the failed run used
`--history --no-drop-new-table --no-drop-triggers`, keeping state in
`percona.pt_osc_history`, and gained `--where` [pt-osc-resume].

(2)/(3) Role for migkit: **not a mover** but the DDL-during-migration
rung (backlog "DDL" and R17): when the hop's source runs an online schema
change during a follow, migkit must (a) recognise the ghost table and
exclude it (done), (b) apply the final `RENAME` swap on the target
atomically (MECH covers), (c) not count the ghost/checkpoint tables as
drift. When migkit itself must change a big MySQL table's schema on the
target before cutover (type widening found by `assess`), drive gh-ost
with `--max-lag-millis` from the throttle, `--postpone-cut-over-flag-file`
held until migkit's fence says the tail is caught up, `--checkpoint` on;
fallback plain `ALTER ... ALGORITHM=INSTANT|INPLACE` when the change is
instant (8.0.29+ instant add/drop column). pt-osc only where the table has
foreign keys that gh-ost refuses.

(4) On top: migkit verifies the ghost table against the source by range
digest before the cut-over (gh-ost only compares row counts with
`--exact-rowcount`).

### 2.5 pt-table-checksum / pt-table-sync (GPLv2)

(1) pt-table-sync is driven for printing repair statements, bounded to
verified keys (`mysql.py:4090-4094`). pt-table-checksum is not used; its
model [pt-tc]: chunks sized to `--chunk-time 0.5` s with a decaying
average, `REPLACE ... SELECT` of `COUNT(*)` and a fold of `CRC32` (or
`--function` MD5/SHA1/FNV1A_64/MURMUR_HASH) per chunk into
`percona.checksums`, **replicated statement-based** so each replica
computes the same chunk against its own data; `--max-lag`, `--max-load`,
`--resume`; tables with no index are checksummed only as one chunk.
The fold is `COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', cols,
CONCAT(ISNULL(..)))) AS UNSIGNED)),10,16)),0)` [pt-tc-query], a 32-bit
hash XOR-folded: the exact weakness migkit removed from its own MySQL checksum in R18 (a keyless
table with one row doubled on each side passes).

**Also found:** migkit runs `pt-table-sync --print` (`mysql.py:4094`)
without `--no-version-check`; Percona Toolkit's version check is on by
default and contacts Percona's servers (SEC §Percona 7). S fix: add
`--no-version-check` (a data-leaving call the operator never agreed to).

(2)/(3) pt-table-checksum is useful only for **MySQL->MySQL replica
topologies** (the check rides the replication stream, so it compares at
the same logical point without a fence). Rule: target is a binlog replica
of the source (native replication, not migkit's tail), statement-format
allowed for the session, not managed (RDS permits it with caveats);
otherwise migkit's own fenced digests. Keep pt-table-sync as today.

(4) On top: migkit's per-range sum-of-MD5 digest does not cancel on
duplicates (R18), works cross-engine, and is fenced by position, not by
replication.

---

## 3. SQL Server and Oracle

### 3.1 SQL Server bulk: mssql-python `bulkcopy` / `bulkcopy_arrow` (MIT), bcp and sqlpackage (Microsoft licence, free), SqlBulkCopy

(1) migkit writes SQL Server rows by delete-by-key + `executemany`
through pymssql (`dbapi.py:175-210`, `mssql.py:669`); `capabilities.py`
declares `mssql bulk-move: NOT_YET 0e`. Nothing uses the TDS bulk-load
protocol (grep: no bcp, sqlpackage, mssql-python). Facts [mssql-bulk]:
* `cursor.bulkcopy(table, rows, batch_size=0, timeout=30,
  column_mappings, keep_identity, check_constraints, table_lock,
  keep_nulls, fire_triggers, use_internal_transaction)` - public since
  mssql-python 1.4.0 (2026-02-27), Rust `mssql_py_core` speaking TDS bulk
  insert; returns `rows_copied`, `batch_count`, `elapsed_time`.
* `cursor.bulkcopy_arrow(table, pyarrow Table|RecordBatch|Reader)` (1.13.0,
  2026-08) reads Arrow buffers in Rust **with the GIL released**; no type
  family conversion (float64 -> money raises before any row is written;
  decimal128 required for money/decimal/numeric).
* **Commits on its own internal connection**: a rollback of the caller's
  connection cannot undo it; `use_internal_transaction=True` makes each
  batch atomic. Microsoft's own advice for gating: bulk into a staging
  table, then `INSERT ... SELECT` in the caller's transaction.
* 1.8.0 aarch64 wheels needed glibc 2.34 / libssl 3 (issue #619) - a
  platform fact for `doctor`.
* bcp (`-b` batch, `-a` packet size, `-h "TABLOCK, ORDER(...)"`, `-E` keep
  identity, `-k` keep nulls, native format `-n`) and sqlpackage
  (`/Action:Extract|Publish|Export|Import|DeployReport|Script`, dacpac/
  bacpac) are the vendor CLIs; sqlpackage `Export/Import` is the only
  logical whole-database path for Azure SQL.

(2) Wrap: **mssql-python from pip** (MIT) as the SQL Server bulk writer;
bcp/sqlpackage not wrapped as movers (bcp is superseded by the driver's
bulk copy; sqlpackage kept for `dacpac` schema extract/deploy report only,
binary via `doctor --install` where the platform has it (verify arm64)).

(3) Rule (closes `0e`): target is SQL Server/Azure SQL/MI and a batch has
more than ~1,000 rows (the driver's own guidance) -> stage path:
`bulkcopy_arrow` into a `#stage` or a `migkit_stage_<hash>` table with
`table_lock=True` (no concurrent readers on the stage), `keep_identity=True`
where the target column is IDENTITY, `keep_nulls=True` always (migkit
carries NULLs, not defaults), `check_constraints=False`,
`fire_triggers=False`, then `MERGE`/`DELETE+INSERT ... SELECT` from the
stage inside migkit's transaction with the batch mark - exact and
idempotent because the stage load is disposable and the promote is one
transaction. `batch_size` from sizing (rows/s x 5 s). Arrow path when the
reader already yields Arrow (ADBC/ConnectorX/oracledb, §4), else
`bulkcopy` with tuples. Fallback: pymssql `executemany` (today).

(4) On top: the driver has no verification, resume or throttle; migkit
digests each range after the promote (the SHA-256-of-`FOR JSON` sum,
R18) and resumes by range.

### 3.2 Babelfish Compass (Apache-2.0, Java)

(1) migkit converts T-SQL routines with sqlglot parsing (`mssql.py:737-802`)
and says what it cannot carry (`hetero.py:2160`). Compass reads T-SQL
scripts (e.g. from sqlpackage/SSMS "generate scripts") and reports each
construct as supported / not supported / review for a given Babelfish
version, with a cross-reference report and a `-rewrite` option for some
constructs [compass]. (verify: exact rewrite coverage by version.)

(2) Wrap: jar via `doctor --install` + a JRE; output HTML/CSV parsed.

(3) Rule: only for SQL Server -> Aurora/Babelfish PostgreSQL hops (the
target speaks T-SQL); the stored-code converter (R11) asks Compass per
routine and routes: supported -> carried as is; not supported -> sqlglot
transpile to PL/pgSQL where it can, else the named comment (today).

(4) On top: migkit proves converted code by running it against both sides
with the same inputs (R11 "converted and proved"); Compass only
classifies.

### 3.3 ora2pg (GPL-3.0, Perl) and python-oracledb (Apache-2.0/UPL)

(1) Oracle is a side through python-oracledb (`oracle.py:50-58`;
`fetch_lobs=False`, `fetch_decimals=True`), writes through
`DbapiRows.executemany`; Oracle bulk-move/stream/fence are NOT_YET (item
11). ora2pg appears only in a docstring (`handwork.py:11`).
* **python-oracledb 3.4+ `Connection.direct_path_load(schema_name,
  table_name, column_names, data, batch_size)`** (thin mode only; list of
  sequences or a DataFrame; **commits itself**) [oracledb-dpl];
  `executemany(..., batcherrors=True, arraydmlrowcounts=True)` (array DML
  with per-row errors), `executemany` of large inputs in batches (3.4),
  `fetch_df_all`/`fetch_df_batches` (Arrow data frames; R18: a known bug
  drops rows in DATE columns - pin past it), `arraysize`/`prefetchrows`.
* ora2pg [ora2pg]: schema + PL/SQL -> PL/pgSQL conversion with
  `SHOW_REPORT --estimate_cost` (migration effort units), data export with
  `-P PARALLEL_TABLES`, `-J ORACLE_COPIES` + `DEFINED_PK` (`WHERE
  ABS(MOD(pk, N)) = n` splits; issue #766 "missing rows during parallel
  extraction"), `-j JOBS`, Oracle `PARALLEL` hint; **`TEST_DATA`**
  compares the first `DATA_VALIDATION_ROWS` (10,000) rows per table
  ordered by key, needs a PK/unique non-LOB key, and stops after 10 errors.

(2) Wrap: python-oracledb already a dependency (bump floor to 3.4 for
direct path); ora2pg = binary via `doctor --install` (CPAN/apt) driven only
for **code conversion and the cost report** (GPL-3 fine as a separate
process); never as the data mover.

(3) Rules:
* Oracle target, range > ~10k rows, target table not replicated by a
  trigger that must fire -> `direct_path_load` into a stage table
  (because it commits itself), then `MERGE` into the target in migkit's
  transaction; fallback `executemany(batcherrors=True)` with
  `arraydmlrowcounts` to name rejected rows.
* Oracle source -> Arrow fetch (`fetch_df_batches(size=rows_per_5s)`)
  where no DATE column is affected by the pinned version's bug, else
  tuples with `arraysize = prefetchrows = batch`.
* Oracle -> PG stored code: ora2pg converts per routine, migkit runs the
  converted routine against both sides (R11) and keeps only what proves.

(4) On top: ora2pg's MOD split can miss rows (#766) and its `TEST_DATA`
checks the first 10k rows; migkit's range copier covers every row with a
coverage check and digests every range.

---

## 4. Readers, Arrow transports, ELT and PostgreSQL CDC services

Published rates for dlt, Sling, ConnectorX and ADBC are in TC (lines
88-122: dlt+ConnectorX 10m51s vs Sling free 19m34s on 9.74 GB TPC-H; 10M
PG rows 16.2 s ConnectorX+Arrow vs 8m13s SQLAlchemy); Sling's modes and
dlt's merge strategies are in MECH §3. Only what is new is below.

### 4.1 ADBC drivers (Apache-2.0; pip `adbc-driver-postgresql`, `-sqlite`, `-snowflake`, `-bigquery`, `-flightsql`)

(1) Not used (grep 0). PostgreSQL driver [adbc-pg]: reads with `COPY`
(binary) straight into Arrow; `adbc_ingest(mode=create|append|replace|
create_append)` for bulk writes; **NUMERIC read as string** ("cannot be
losslessly converted to the Arrow decimal types"); timestamps bound
**ignore the time zone**; no partitioned results; `adbc.postgresql.
use_copy` switch. migkit's PG reader is psycopg2 server cursor
(`postgres.py:780`) and the PG->PG copy is `psql \copy` text pipes
(`postgres.py:7086`) - already pass-through; ADBC's gain is for **PG ->
another engine** where the writer takes Arrow (SQL Server `bulkcopy_arrow`
§3.1, Oracle `direct_path_load` DataFrame §3.3, DuckDB/Parquet, Snowflake,
BigQuery).

(2) Wrap: pip, Apache-2.0; hidden behind `neutral_read` as an Arrow-batch
variant (R18.6 "Arrow batches between readers and writers").

(3) Rule: source PG, target engine has an Arrow writer, table has no
`timestamptz` bound as a parameter (reads are fine) and NUMERIC columns
are carried as strings then cast by the writer (never through float);
per-column guard list in `canon`. Fallback psycopg server cursor.

(4) On top: ADBC has no ranges, resume or verification; migkit keeps its
ranges and digests and uses ADBC only as the pipe.

### 4.2 ConnectorX (MIT; Rust, pip) and Polars `read_database` (MIT)

(1) Not used. ConnectorX `read_sql(conn, query, partition_on=col,
partition_num=N, return_type="arrow")` splits one query into N ranges of
a numeric column by min/max and reads them in parallel into Arrow
[connectorx]; each partition is its own connection, **so the partitions
are not one snapshot** on a live source; R18 recorded that it drops time
zones. Polars `read_database_uri(engine="connectorx"|"adbc")` is a thin
layer over the two.

(2)/(3) Rule: **not adopted as a general reader** (R18 stands). Only as
a read accelerator for a **static** source (standby frozen, snapshot
restore, or a source the hop marks read-only) with no `timestamptz` /
`datetimeoffset` columns and an integer key whose histogram is even
(max-min vs n_distinct) - else migkit's own quantile ranges. Polars: no
role (pandas/pyarrow already carry datacompy).

(4) On top: migkit's ranges are quantile-based (even under skew),
snapshot-consistent where the engine allows, resumable and digested.

### 4.3 DuckDB scanners / ATTACH (MIT; pip, already a dependency)

(1) migkit has DuckDB as an engine (`engines/duckdb.py`, R15) and for
out-of-core key diffs (pinned; `join_filter_pushdown` off, R18).
The `postgres` extension [duckdb-pg]: binary COPY (`pg_use_binary_copy`),
**ctid-parallel scans** (`pg_use_ctid_scan`, `pg_pages_per_task` = 1000),
filter pushdown, `pg_connection_limit` 64, `postgres_query`/
`postgres_execute`, `ATTACH ... (TYPE postgres, READ_ONLY)`; `mysql` and
`sqlite` scanners likewise. (verify: whether parallel ctid tasks share one
exported snapshot on a primary.)

(2)/(3) Rule: DuckDB ATTACH as a **cross-engine second reader** (read
PG and MySQL in one process, compute the same canonical digest per range
through `canon`) when DVT is absent, and as the Parquet/lake bridge
(already). Never as the mover into an OLTP target.

(4) On top: migkit's canonical rendering makes the two sides' digests
comparable across engines; DuckDB alone compares typed values that
different engines round differently.

### 4.4 Arrow Flight / Flight SQL (Apache-2.0; in pyarrow)

(1) Not used. Flight streams Arrow record batches over gRPC with
multiple endpoints in parallel, TLS/mTLS and auth handlers; Flight SQL
adds a SQL surface (servers: Dremio, Doris, InfluxDB 3, GizmoSQL). The
relay beside the source today pipes `psql \copy | zstd` over ssh
(`postgres.py:7143-7177`).

(2)/(3) Rule: a Flight endpoint on the migkit relay agent (R17d) when the
leg carries Arrow batches between two migkit processes (cross-engine
moves where both ends are Arrow-native) and the link wants several
parallel streams; zstd/lz4 IPC compression in the batch. Same-engine PG
pass-through stays text COPY over ssh. Priority low; measure first.

(4) On top: the relay keeps the mark and batch number in the batch
metadata (exactness), which Flight does not define.

### 4.5 Sling (GPL-3.0 CLI; `sling-python` wrapper MIT) and dlt (Apache-2.0)

(1) Neither used. Sling [sling-lic]: Go single binary; hooks/pipelines
only in official builds (a stub errors in source builds). dlt: Python
ELT with `sql_database` source backends `sqlalchemy|pyarrow|pandas|
connectorx`, incremental cursors, schema contracts, `_dlt_pipeline_state`
(MECH §3).

(2)/(3) **Not wrapped as movers**: neither verifies, both write through
their own staging and state tables into the target (footprint migkit
must not leave), and Sling's CLI is GPL-3 with closed-build-only
features. Take from dlt: **schema contracts** (freeze / evolve / discard
per table and column) as the model for the owed "target drift during
follow" rule; from Sling: nothing beyond MECH.

### 4.6 pgstream (Apache-2.0, Go) and Sequin (MIT, Elixir)

(1) Neither used; MECH §1.7 and table row 688 cover pgstream's snapshot
family. New facts: pgstream [pgstream] targets PG, Kafka, OpenSearch/ES,
webhooks; **DDL replication by event triggers and a schema log table in a
`pgstream` schema**; transformers from greenmask, neosync and go-masker
(write-side anonymisation); wal2json only; needs a PK/unique not-null;
no row filters. Sequin [sequin]: PG 14+, 16+ sinks, backfills of all or
some rows at any time, "exactly-once processing ... using idempotency
keys", claims >50k ops/s, 55 ms mean latency, Prometheus metrics.

(2)/(3) **Not wrapped** (both leave objects in the source: pgstream's
schema and triggers, Sequin's own Postgres). Take: pgstream's
**event-trigger DDL log** as the DDL-capture rung for PG sources where
migkit may create objects (owner-approved footprint) - fallback today's
schema diff at the fence; Sequin's **per-message idempotency key** =
migkit's batch mark carried into Kafka headers (already the R3 design for
Kafka, `oss-nonrelational` §2.3).

(4) On top: migkit tails into any engine, not only PG/Kafka/search, and
proves the target by digest; neither tool verifies the sink.

---

## 5. Verifiers

sync-diff-inspector, VDiff and MOLT Verify are in DTS §6, §8.2, §10; the
DVT/reladiff/datacompy use-vs-unused audit and design are in WIP-B and
backlog 0f. This section adds the missing algorithms and the rules.

### 5.1 reladiff (MIT; fork of Datafold data-diff, archived 2024) - pip, already a dependency

(1) Used: CLI with `--conf` private file, `--stats`, `-j` = throttle
allowance (`generic.py:44-79`, measured: `-j` is connections, not speed),
`-c %` and `--where` from the hop's row filter (`mysql.py:1735-1750`).
Unused [reladiff]: `--algorithm hashdiff|joindiff` (joindiff = one outer
join when both tables are in the same database, exact and fast),
`--bisection-factor` / `--bisection-threshold` (WIP-B measured:
factor must be lower than threshold or it raises), `--min-age/--max-age`
+ `-t/--update-column` (ignore rows younger than N - the in-flight rows
during a follow), `--assume-unique-key`, `--sample-exclusive-rows`,
`-m/--materialize` + `--table-write-limit` (writes the diff into a table),
`--json` (JSONL), `--limit`, `--skip-sort-results`. Hashdiff = checksum
per key segment on each server, recurse into segments that differ until
a segment is below the threshold, then download and compare locally.
Measured by WIP-B: JSON columns print "no compatibility handling ... may
result in false positives"; sqlite scheme unsupported.

(2) Wrap: unchanged (pip). Parse `--json` instead of the text stats.

(3) Rule: reladiff is the **cross-engine bisection rung** when migkit's
own range digests say a range differs and the range is larger than the
drilldown cap (`postgres`/`mysql` drilldown cap 20,000 rows, R18.4):
`bisection-threshold = drilldown cap`, `bisection-factor =
ceil(rows_in_range / threshold) ** (1/depth)` with depth <= 3, kept below
the threshold; `--min-age = fence lag` during a follow; `joindiff` when
the hop's two sides are schemas of one server; `--assume-unique-key` when
`table_facts` says a PK exists; refused (said) on JSON/jsonb columns
unless they are excluded and compared by migkit's canonical rendering;
`-j` from the throttle as today. Never `--materialize` on a target that
must stay read-only.

(4) On top: migkit digests in the engine's own SQL through `canon` so
JSON, floats and time zones compare canonically; reladiff's per-engine
checksum is not comparable across such types (its own warning).

### 5.2 Google DVT (Apache-2.0) - pinned in its own venv (`tools.py:42`)

(1) Used: `validate column` count/sum/min/max, `--filters`,
`connections` (`hetero.check_second`). Full surface [dvt]: `validate
column | row | schema | custom-query column | custom-query row`,
`generate-table-partitions` (`--partition-num`, `--parts-per-file`:
splits a big table into YAML partitions run in parallel), `find-tables`
(similarity match of table names across sides), `configs run|list|get`,
`query`, `beta deploy`; flags `--hash`, `--concat`,
`--comparison-fields`, `--primary-keys`, `--use-random-row`,
`--random-row-batch-size`, `--grouped-columns`, `--count/--sum/--min/
--max/--avg/--std/--bit_xor`, `--threshold`, `--filter-status`,
`--format`, `--result-handler`, `--exclude-columns`, `--cast-to-bigint`,
`--wildcard-include-string-len`, `--wildcard-include-timestamp`,
`--case-insensitive-match`, `--trim-string-pks`. 15 engines incl. Db2,
Sybase ASE, Teradata, Spanner, Hive, Impala.

(2)/(3) WIP-B's design stands (batch YAML, row hash paired with an
unfiltered count, grouped narrowing, sampling never a verdict). Add:
* `generate-table-partitions` for tables above `hop.slice` x workers,
  `--partition-num = workers`, partitions run by migkit's own pool (not
  DVT's) so the throttle governs them.
* `validate schema` as the second reader of migkit's schema check on
  cross-engine hops (its type-equivalence table is independent of
  `canon`).
* `find-tables` as an `assess` hint when the hop maps tables by rename.
* `--wildcard-include-string-len` on text columns where the target's
  charset differs (catches truncation that sums miss).
* `--bit_xor` **never on its own**: XOR cancels on duplicates (R18);
  only beside `--sum` of the same hash.
* engines migkit reads but cannot yet stream (Db2, ASE, Teradata): DVT is
  the only row-level second reader there.

(4) On top: DVT runs after, cold, whole-table; migkit verifies each range
as it lands, fenced by position, and uses DVT as the independent reading.

### 5.3 datacompy (Apache-2.0) - pip, already a dependency

(1) Used only by `migkit check --drill` to print `cmp.report()` raw
(`cli.py:673`), which leaks the tool's name (WIP-B). datacompy compares
two frames joined on keys with `abs_tol`/`rel_tol` per column,
`ignore_spaces`, `ignore_case`, `cast_column_names_lower`, and gives
`all_mismatch()`, `intersect_rows`, `df1_unq_rows`, `df2_unq_rows`,
per-column match rates; Pandas, Polars, Spark and Snowpark backends
[datacompy].

(2)/(3) Rule (WIP-B): `explain_rows` over the drilldown's frames only
(bounded by the cap), tolerances from `mapping.tolerance`, output in
migkit's words, masked. Polars backend only if Polars is already present
for another reason (not a new dependency).

(4) On top: datacompy needs both frames in memory; migkit narrows to the
differing range first (digests -> reladiff/DVT grouped -> rows).

### 5.4 pg_comparator (BSD; Perl; PG, MySQL, SQLite)

(1) Not used. Algorithm [pg-comparator]: per-row checksum (`ck`, `fnv`,
`md5`; includes the key so swapped rows are seen), then a tree of
summaries folding `2^f` rows per level by key-hash bits
(`--folding-factor`, default 7 = 128), `--aggregate sum|xor` (default
**sum**: works across engines; xor needs a loadable aggregate and has a
signed/unsigned issue across MySQL/SQLite vs PG), `--max-levels`
cut-off, `--checksum-size`; synchronises with `--do-it`; can use a
trigger-maintained tuple checksum (`--tuple-checksum`, `--key-checksum`)
so re-comparison skips the first scan. **Needs a key.**

(2)/(3) Not wrapped (Perl, key required, same families migkit already
covers). Take two ideas as parts of migkit's own digest ladder:
**fold by key-hash bits** (buckets that do not depend on key order, so
two engines with different collations bucket identically - the problem
ora2pg's `TEST_DATA` hits with non-C collations) and the **maintained
per-row checksum** for repeated re-syncs (R19.3 "rows not copied").

(4) On top: migkit already sums (not XORs) 64-bit MD5 slices per range
(`postgres.py:2259-2309`) and handles keyless tables by whole-row hash
buckets (decision-layer block); pg_comparator refuses keyless tables.

---

## 6. Schema, SQL and online-maintenance tools

### 6.1 sqlglot (MIT) - pip, already a dependency

(1) Used for **parsing**: T-SQL routines (`mssql.py:737-802`), PG
single-expression SQL functions (`postgres.py:3898-3935`), row-filter
predicates re-rendered per dialect (`hetero.py:1598-1625`), views and
one-expression functions carried (`hetero.py:2160`); `convert_ddl`
deliberately does **not** transpile DDL (`hetero.py:2044-2055`, own type
map). Unused [sqlglot]:
* `sqlglot.optimizer.optimize` (qualify, normalize, pushdown, simplify,
  `annotate_types`, canonicalize) - to compare two view definitions
  **semantically** after both are qualified and normalised, where migkit
  today compares `pg_get_viewdef` text (`postgres.py:3945`) and MySQL
  `view_definition` (`mysql.py:2928`).
* `sqlglot.diff(a, b)` - AST change-distilling edit script (insert, remove,
  move, update, keep) between two definitions: says *what* differs in a
  view or routine, not just "differs".
* `sqlglot.lineage(column, sql, schema)` - column-level lineage of a view:
  which source columns a target view column depends on, so a column the
  hop drops/renames can be traced to every view that breaks.
* `annotate_types` with a schema dict - the type of each view column, to
  create the target view's columns with the right types cross-engine.
* `transpile(..., unsupported_level=ErrorLevel.RAISE)` - make "cannot
  express" an exception instead of a silent approximation.

(2) pip, already present; nothing to add.

(3) Rules: view/routine compare = `optimize(qualify=True, schema=facts)`
on both sides then `diff`; equal edit script -> `ok`, only cosmetic
`Keep`/`Move` -> `ok` with a note, else `diff` with the edit script in
migkit's words. Lineage run for every view when the hop has column rules.
Transpile always at `ErrorLevel.RAISE`; on raise, the existing "named,
never guessed" comment.

(4) On top: migkit executes both definitions against the two sides and
compares results (R11 "proved"); sqlglot only reasons over text.

### 6.2 sqlfluff (MIT)

(1) Not used. A dialect-aware SQL linter/formatter (`lint`, `fix`,
`parse`, 20+ dialects, rules for ambiguous joins, implicit aliasing, etc.).
(2)/(3) Role: **only** to lint the SQL migkit *generates for a person to
run by hand* (the `handwork.py` scripts, converted routines) in the
target's dialect, so a syntax error is found before the operator runs it.
Rule: generated script for a dialect sqlfluff supports -> `parse` must
succeed, else say so; never `fix` (changes meaning). pip, MIT. Low value,
S effort.

### 6.3 Atlas (Apache-2.0 **Community** build; the default binary is proprietary)

(1) Used: `atlas schema diff --config` (`movers.py:1764`) and as the
authority option (`base.py:1778`); installed by `doctor` from
`ariga/tap/atlas` (`tools.py:32`). **Licence flag**: every Atlas release
says "The default binaries ... are distributed ... under Atlas EULA, and
the community binaries are released under the Apache 2.0 license"; the
Community Edition page names the Atlas MSA for the default build and says
views, materialized views, functions, procedures, triggers, sequences,
domains and extensions are **Pro** features, and `migrate lint` left the
free plan in v0.38 [atlas-ce]. So migkit's `doctor --install` puts a
proprietary binary on the operator's machine, and the object types it
can diff there depend on `atlas login`.
(2) Wrap: install the Community build (`arigaio/atlas:latest-community`
image or the `--community` installer), Apache-2.0; accept that its diff
covers tables/columns/indexes/FKs only; views/routines go through 6.1.
(3) Rule: tables-level structural diff second reading for PG/MySQL/SQLite/
SQL Server when the Community build is present; never the only reading.
(4) On top: migkit's own schema check plus sqlglot-normalised views and
executed routines cover what the Community build cannot.

### 6.4 Liquibase (FSL-1.1-ALv2 since 5.0), Flyway (Apache-2.0 community), skeema (Apache-2.0), migra/results

* **Liquibase 5.0 (2025-09-30) moved to the Functional Source License
  1.1 with Apache-2.0 future licence** (each version reverts to Apache-2.0
  after two years); Liquibase itself says FSL is not OSI open source;
  Keycloak (CNCF) had to plan a replacement [liquibase-fsl]. migkit lists
  `liquibase` in `tools.py:33,67` and `base.py:44`. **Flag**: pin to 4.x
  (Apache-2.0) or drop it; FSL forbids "competing" commercial use, which
  a migration product arguably is. Rule: used only to **read** a
  project's existing changelog (`liquibase status` / `diff`) when the hop
  points at one; never installed by default.
* **Flyway** (Redgate; community edition Apache-2.0): versioned
  `V<n>__*.sql` migrations and `flyway_schema_history`. Role: `assess`
  reads `flyway_schema_history` on both sides and says whether the target
  is at the same migration version (a cheap schema-equality proof the
  application team already trusts). No binary needed (a SQL read).
  Same for Liquibase's `DATABASECHANGELOG` table. S effort, high signal.
* **skeema** (Apache-2.0, MySQL/MariaDB): declarative `.sql` per object,
  `diff`/`push`/`lint` against a live server; `lint` rules (e.g.
  `lint-pk`, `lint-charset`, `lint-dupe-index`, `lint-zero-date`). Role:
  MySQL target lint before cutover (rules migkit's `assess` lacks: dupe
  index, zero dates allowed, charset mismatch per column). Binary via
  `doctor --install`; rule: MySQL target, skeema present. (verify: rule
  names on the current release.)
* **migra** (archived) / **results** (its successor, already a hard
  dependency, `postgres.py:1664`): PG structural diff producing the
  statements to converge; already used whole for PG schema diffs.

### 6.5 pg_repack (BSD-3)

(1) Not used; not in `leftovers.py` either. pg_repack rewrites a bloated
table online (a `repack` schema, a log table and a trigger per table
during the run, a brief `ACCESS EXCLUSIVE` at swap). Two roles:
* **Footprint recognition** (S): a repack in progress on the source during
  a follow creates `repack.log_<oid>` and a `repack_trigger`; the swap is a
  catalog-level change the slot sees as a new relfilenode (no row
  changes). `leftovers.SIGNATURES` (`leftovers.py:35-47`) has none of:
  pg_repack (`repack` schema), pgcopydb (`pgcopydb` schema and its
  `sentinel` table), pglogical (`pglogical` schema), Spock (`spock`
  schema), Bucardo (`bucardo` schema), pgstream (`pgstream` schema), dlt
  (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`), Sling
  (`_sling_loaded_at` column), PeerDB (`_peerdb_`), Airbyte (`_airbyte_`),
  gh-ost's checkpoint `_<t>_ghk` (§2.4). All S additions.
* **Target compaction after cutover**: never needed for a fresh load
  (no bloat); not wrapped.

---

## 7. Drivers, codecs and hashes

### 7.1 psycopg 3 (LGPL-3.0; already a hard dependency) - pipeline, binary COPY, server cursors

(1) Used: `cursor.copy()` + `write_row` into a temp stage for keyed
writes (`postgres.py:832-860`) and the R2.4 COPY stage for runs of 1,000+
upserts (backlog R2: 5,000 rows 24 ms vs 51 ms). Everything else is
psycopg2 (`execute_values` `postgres.py:1103-1183`, named cursor
`postgres.py:780`) and `psql \copy` text pipes. Unused [psycopg-copy]
[psycopg-pipeline]:
* **Pipeline mode** (client libpq 14+, any server): statements sent
  without waiting for each result; `executemany` uses it automatically
  since 3.1; on an error the server aborts the rest until the next sync
  (`PipelineAborted`), so a pipelined batch is still one transaction.
  Docs: 100 statements at 300 ms RTT, 30 s -> ~0.3 s. = R19.10, not built
  (grep: no `pipeline` in `migkit/`).
* **Binary COPY** with `copy.set_types([...])` - "PostgreSQL will apply no
  cast rules", so the declared types must be identical; block copy
  between two connections (`COPY ... TO STDOUT (FORMAT BINARY)` read,
  `write()` to the other) - the in-process equivalent of pgcopydb's
  `--use-copy-binary`.
* `cursor.stream()` (row-by-row; chunked with libpq 17) and named server
  cursors with `itersize` for bounded memory.

(2) Nothing to install. (3) Rules:
* Tail apply, PG target: runs below the R2.4 staging threshold (1,000
  rows) and any run when RTT > 5 ms -> one pipelined `executemany` per
  lane (sync at the batch's end, where the mark is written); >= 1,000 ->
  the existing COPY stage. Needs `psycopg.pq.version() >= 140000`
  (`Capabilities.has_pipeline`), else today's `execute_values`.
* Same-engine PG table copy: text `psql \copy` pipes stay the default
  (the read-back tally reads text, `tally.py:28`); **binary block copy**
  only when both majors are equal, no column is of an extension or
  user-defined type, the relay is not in use (the relay compresses text
  well), and the range's digest (not the tally) is the proof. Measure
  first (R19.2 "binary COPY, not text").

(4) On top: pipelined statements carry migkit's batch mark in the same
transaction, so pipelining costs no exactness.

### 7.2 asyncpg (Apache-2.0)

(1) Not used. `copy_records_to_table(table, records=..., columns=...,
schema_name=..., where=...)` encodes Python records straight into the
binary COPY protocol in Cython, typically the fastest Python writer into
PG; asyncio only; its own type codecs (not libpq).
(2)/(3) **Not adopted**: psycopg 3's `write_row` into binary COPY
reaches the same wire format without a second driver and a second type
system (every extra codec is a new place for a value to be written
differently; `canon` would need a third mapping). Re-open only if a
measured cross-engine -> PG load is writer-bound after 7.1.

### 7.3 PyMySQL (MIT; used) vs mysqlclient (GPL-2.0) vs mysql-connector-python (GPLv2 + Universal FOSS Exception)

(1) migkit uses PyMySQL everywhere (`mysql.py:50-56`), plus
`mysql-replication` (built on PyMySQL). R18 rejected mysqlclient as a
default (GPL). R2.6 measured the binlog decode at 116k changes/s and the
applier as the limit, not the driver.
(2)/(3) Rule: keep PyMySQL. Where a **bulk read** from MySQL is the
measured ceiling (rows/s per stream below the target's load rate), the
reader switches to `mysql-connector-python`'s C extension
(`use_pure=False`), whose UFE permits use from an MIT project (the
connector licence carries the UFE, the Shell does not; §2.2) - measure
first; mysqlclient never.

### 7.4 orjson (Apache-2.0/MIT) and msgspec (BSD-3)

(1) Not used; 96 `json.dumps` call sites in `migkit/*.py`. Hot paths
where JSON volume scales with rows: JSON bodies to Kafka, JSONL change
spill, the relay's batch framing, report JSONL.
(3) Rule: msgspec (JSON + MessagePack, typed decode; JSON ints of any
size, but MessagePack only within [-2**63, 2**64-1] [msgspec-types]) for
**internal** frames between migkit processes (relay
batches, worker hand-off, R18 "msgspec and pickle 5 out-of-band
buffers"); orjson only for outward JSON where every integer fits 64 bits
(orjson raises `JSONEncodeError` "Integer exceeds 64-bit range" and the
`default` hook is never called for an int [orjson-int]; NUMERIC values
carried as Python ints would hit it). MessagePack framing refuses
the same ints, so relay frames holding NUMERIC use JSON or strings. Never for `canon`'s rendering or any digest input (the
rendering is the contract with the SQL engines' own digests).

### 7.5 zstandard (BSD-3; used), lz4 (BSD-3), xxhash (BSD-2), blake3 (CC0/Apache-2.0), pyarrow (Apache-2.0; used)

* zstandard: used to decode MySQL compressed transaction payloads
  (`binlog_payload.py:79`) and for the relay stream (`postgres.py:7170-7177`,
  `zstd -3 -T0`). Unused: frame content checksums
  (`write_checksum=True`, XXH64 per frame) - free integrity on spill files
  and relay frames; `ZstdCompressor(level, threads)` in-process for the
  cross-engine relay; trained dictionaries for many small rows (Kafka
  bodies). Rule: checksum on always for anything written to disk or sent.
* lz4: rule-based alternative on fast links - `lz4` when the measured link
  exceeds zstd -1's per-core rate on this host (roughly 1 Gbit/s and up),
  zstd -3 on WAN, none on a 10G LAN. Measured by the relay probe, not
  assumed.
* xxhash / blake3: R18.3 measured and did not adopt XXH3 for the row hash
  (rendering dominates; SQL engines speak MD5). Remaining uses are already
  covered: zstd's frame checksum (integrity), SHA-256 for audit and
  verdicts (`audit.py:59`, `verdict.py:132`). Not added.
* pyarrow: used for Parquet and DuckDB (`engines/parquet.py`,
  `engines/duckdb.py:98`). Unused: `pyarrow.compute` hashing is not
  stable across versions (R18: never persist an engine's internal hash);
  Arrow IPC with zstd for relay batches (§4.4); `pyarrow.dataset` for
  file-sourced moves. Rule: Arrow is the in-memory batch format between an
  Arrow-native reader (ADBC, oracledb, ConnectorX) and an Arrow-native
  writer (`bulkcopy_arrow`, `direct_path_load`, DuckDB, Parquet); rows stay
  tuples everywhere else.

---

## 8. Prioritised table

Effort: S under a day, M a few days, L a week or more. Docker: testable
on this arm64 docker host without an account. Value is against the
owner's bar (above every tool on every axis). Rows 1-12 are the order of
work; the rest follow.

| # | tool | wrap as | licence | decision rule (measured facts) | value | effort | docker |
|---|---|---|---|---|---|---|---|
| 1 | pgcopydb whole: subcommand-aware option check, `--split-tables-larger-than`/`--split-max-parts`, `--index-jobs`/`--restore-jobs`, `clone` for empty targets, `list progress --json --summary` into rates, `stream sentinel get --json` into `/metrics`, `--fail-fast`, LO jobs (WIP-A) | binary/container (have) | PostgreSQL | PG->PG, pgcopydb present; split = max(total_bytes/workers, rate x 10 s) for tables above it; index jobs = min(target vCPU, maint mem / largest index); clone only for an empty target without mappings | the one-big-table ceiling and overlapped index builds; `options_missing` stops being blind | M | y |
| 2 | Leftover signatures: pgcopydb, pglogical, Spock, Bucardo, pg_repack, pgstream, dlt, Sling, PeerDB, Airbyte, gh-ost `_ghk`, `percona` schema (§2.4, §6.5) | data in `leftovers.py` | n/a | always | findings nobody else gives; drift false positives removed | S | y |
| 3 | pt-table-sync `--no-version-check`; Atlas Community build; Liquibase pinned to 4.x or dropped (§2.5, §6.3, §6.4) | install/argv fixes | GPL-2 driven / Apache-2.0 / FSL flag | always | no call home; no proprietary binary installed by `doctor`; no FSL | S | y |
| 4 | psycopg 3 pipeline for tail runs below the COPY-stage threshold (R19.10) | pip (have) | LGPL-3.0 | PG target, client libpq >= 14, run < 1,000 rows or RTT > 5 ms | 30 s -> 0.3 s per 100 statements at 300 ms (docs); exactness kept (mark in the same transaction) | S | y (tc netem) |
| 5 | mydumper/myloader whole: `--rows` adaptive, `--checksum-all` + `--checksum fail`, `--source-data`, zstd or `--stream`, `--optimize-keys` by measurement, `--max-threads-per-table`, masquerade from a write-side masking rule | binary (have) | GPL-3.0 driven | MySQL->MySQL; `--rows` for tables > workers x START; optimize-keys only where measured faster | same-table parallelism; free second checksum; gap-free tail start | S | y |
| 6 | SQL Server bulk: mssql-python `bulkcopy_arrow`/`bulkcopy` into a stage, promote in migkit's transaction (§3.1) | pip | MIT | SQL Server target, batch > ~1,000 rows, glibc >= 2.34 on aarch64 | closes `mssql bulk-move NOT_YET 0e`; bcp-class rate from Python, GIL released | M | y (SQL Edge/2022 image) |
| 7 | reladiff whole: `--json`, `joindiff` same-server, bisection factor/threshold from the drilldown cap, `--min-age` = fence lag, `--assume-unique-key` (§5.1) | pip (have) | MIT | a range's digest differs and the range > drilldown cap; refused on JSON columns | row-level narrowing on big tables across engines | S | y |
| 8 | DVT whole (WIP-B) + `generate-table-partitions`, `validate schema`, `--wildcard-include-string-len` (§5.2) | own venv (have) | Apache-2.0 | second reading asked, or engine without a stream (Db2, ASE, Teradata) | independent verifier, 15 engines | M | y |
| 9 | MySQL Shell `copyInstance` / `dumpInstance` + `loadDump(waitDumpTimeout, checksum, updateGtidSet)` (§2.2) | binary via doctor | GPLv2 driven | MySQL->MySQL/HeatWave, `local_infile` on, no column mapping, binary share low (else mydumper), benchmark says faster | >200 MB/s class load, resumable load | M | y |
| 10 | MySQL CLONE rung + tail from `clone_status` (§2.3) | SQL (pymysql, have) | server GPLv2 | self-managed 8.0.17+ same series/platform, all InnoDB, whole instance, `BACKUP_ADMIN`/`CLONE_ADMIN`, target disposable | pages not rows: the largest bulk gain for MySQL (R19.1) | M | y |
| 11 | sqlglot `optimize` + `diff` + `lineage` + `ErrorLevel.RAISE` for views/routines (§6.1) | pip (have) | MIT | every view/routine compare; lineage when column rules exist | says *what* differs in code; catches views a rename breaks | M | y |
| 12 | pg_dump/pg_restore extras: `--snapshot` with a follow, TOC largest-first, `--compress=zstd`, `--transaction-size`, `--statistics` (PG18), `--filter` (§1.2) | binary (have) | PostgreSQL | pgcopydb absent or refused (FK truncate), or dump crosses a disk hop | consistent dump+tail without pgcopydb; shorter restore tail | S | y |
| 13 | pg_basebackup physical rung: slot first, `server-zstd:workers=N`, manifest + `pg_verifybackup`, `--incremental` re-sync (§1.3) | binary (have) | PostgreSQL | same major/arch, self-managed target, `REPLICATION`, whole cluster, no mappings | terabytes as pages; verified by manifest + migkit | L | y |
| 14 | python-oracledb 3.4+ `direct_path_load` into a stage + `batcherrors` fallback; Arrow fetch where DATE bug absent (§3.3) | pip (have; floor 3.4) | Apache-2.0/UPL | Oracle target, range > ~10k rows | Oracle bulk cell (item 11) | M | y (Oracle Free image) |
| 15 | ADBC PostgreSQL reader for Arrow pipes into Arrow writers (§4.1) | pip | Apache-2.0 | PG source, Arrow-native writer, NUMERIC as string, no tz bound | cross-engine read rate (TC: 3.7-30x over SQLAlchemy) | M | y |
| 16 | pgloader load file generated from excludes/mappings, `CAST` rules, workers/concurrency (§1.5) | binary (have) | PostgreSQL | MySQL/SQLite/MSSQL -> PG, no row filter | lifts the three refusals in `fitted` | M | y |
| 17 | gh-ost as the target-side DDL rung with `--postpone-cut-over-flag-file` tied to migkit's fence (§2.4) | binary via doctor | MIT | MySQL target needs a non-instant ALTER on a big table before cutover | online type widening without a second copy | M | y |
| 18 | Flyway / Liquibase history tables read in `assess` (§6.4) | SQL only | n/a | table `flyway_schema_history` or `DATABASECHANGELOG` present | schema-version equality the app team trusts | S | y |
| 19 | ora2pg for code conversion + cost report only (§3.3) | binary via doctor | GPL-3.0 driven | Oracle->PG with routines | PL/SQL -> PL/pgSQL proposals, proved by R11 | M | y (Oracle Free) |
| 20 | Babelfish Compass per routine (§3.2) | jar via doctor + JRE | Apache-2.0 | SQL Server -> Babelfish target | T-SQL support classes by Babelfish version | S | y (scripts only) |
| 21 | XtraBackup rung (§2.3) | binary via doctor | GPLv2 driven | CLONE refused, same series supported | physical copy where CLONE cannot | M | y (verify arm64 build) |
| 22 | pt-table-checksum for native-replica topologies (§2.5) | binary (percona-toolkit, have) | GPLv2 driven | target is a binlog replica of the source; always paired with migkit's sum digest | check at the same logical point without a fence | S | y |
| 23 | skeema lint on MySQL targets (§6.4) | binary via doctor | Apache-2.0 | MySQL target, before cutover | dupe index, zero dates, charset per column | S | y |
| 24 | DuckDB ATTACH as a cross-engine second reader (§4.3) | pip (have) | MIT | DVT absent, both sides PG/MySQL/SQLite | independent digest in one process | M | y |
| 25 | datacompy `explain_rows` in migkit's words (WIP-B, §5.3) | pip (have) | Apache-2.0 | drilldown frames only | removes a tool name from output | S | y |
| 26 | zstd frame checksums on; lz4 on fast links; msgspec for internal frames (§7.4-7.5) | pip (zstd have) | BSD/BSD-3 | link rate vs per-core codec rate; ints > 64 bit -> JSON | integrity for free; CPU where links are fast | S | y |
| 27 | mysql-connector-python C extension for bulk MySQL reads (§7.3) | pip | GPLv2 + UFE | measured read-bound MySQL source | faster fetch without GPL-only mysqlclient | S | y |
| 28 | Spock for PG<->PG two-way (§1.4) | extension on both | PostgreSQL | two-way PG only, extension installable | native conflict-free deltas | M | y |
| 29 | pglogical for PG 9.4-9.6 sources (§1.4) | extension | PostgreSQL | source < 10 | reach old sources | M | y |
| 30 | sqlfluff `parse` of generated hand-run scripts (§6.2) | pip | MIT | a script for a supported dialect | syntax errors caught before a person runs it | S | y |
| 31 | Arrow Flight on the relay agent (§4.4) | pip (in pyarrow) | Apache-2.0 | cross-engine, both ends migkit, several streams wanted | typed batches, parallel streams | L | y |
| - | **not wrapped:** Sling (GPL-3 + closed-build features), dlt (footprint, no verify), ConnectorX/Polars as general readers (R18: time zones; no shared snapshot), asyncpg (second type system), mysqlclient (GPL), xxhash/blake3 (R18.3), pg_chameleon (same library as migkit), pgsync (Ruby; rules taken as reference), Bucardo (triggers), pg_easy_replicate (reference for switchover), pgstream/Sequin (source footprint; ideas taken), pg_comparator (key required; ideas taken), pg_repack (footprint only), Liquibase 5 (FSL), bcp (superseded by the driver), sqlpackage (schema extract only, verify arm64) | | | | | | |

---

## 9. Scorecard

Verdict = migkit against the tool on that axis. Evidence: the tool's
fact `[key]` (URLs in §10) and migkit's `file:line`. SEC = security
report section; TP/TC = throughput reports; n/a = the axis does not apply.
Security verdicts rest on SCORE A1 (secrets never on argv, 0600 files,
at-rest sealing with pyrage, `evidence.py:1-155`).

| tool | correctness / verification depth | bulk throughput + technique | CDC latency / apply rate | resume / idempotence | schema / code conversion | engine coverage | ops / observability | security | licence |
|---|---|---|---|---|---|---|---|---|---|
| pgcopydb | ahead: `compare data` after the fact, whole table [pgcopydb-clone]; migkit range digest as it lands `postgres.py:2259-2309`, fence | behind until row 1: same-table split + overlapped index builds [pgcopydb-clone]; migkit drives `copy table-data` without split `movers.py:2816`; measured 2.9-5.2x over dump path (SCORE B1) | equal: `follow` is driven by migkit `movers.py:2990`; apply rate unmeasured | ahead: resumes by table under `--not-consistent` [pgcopydb-clone]; migkit by range `ranges.py:222` | equal: pg_dump schema both | n/a (PG only) vs migkit many | behind: `list progress --json` unused (WIP-A) | ahead: SEC §pgcopydb URI/env, unsigned releases; migkit PGPASSFILE 0600 | PostgreSQL |
| pg_dump / pg_restore | ahead: none [pg-dump-18] | equal: migkit drives `-Fd -j` `movers.py:958`; TOC order, zstd unused | n/a | ahead: restore restarts from zero | equal: `--section` used `movers.py:1206` | n/a | equal | equal: env passwords | PostgreSQL |
| pg_basebackup | equal: block + manifest checksums [pg-basebackup]; migkit has no physical rung | behind: pages + server zstd workers vs migkit logical (R19.1 todo) | n/a (slot + WAL) | equal | n/a | n/a | behind: `-P` + `pg_stat_progress_basebackup`; not wrapped | equal | PostgreSQL |
| pglogical | ahead: none built in; migkit fenced digests | equal: COPY under the slot's snapshot (MECH §4) | equal: native apply vs migkit lanes (SCORE B1 65k/s applier) | behind: native origins; migkit origins + marks (R3) equal in kind | behind: syncs sequences and DDL via `replicate_ddl_command`; migkit schema diff at fence | behind: PG 9.4+ incl. pre-10; migkit uses 10+ decoding | equal | equal | PostgreSQL |
| Spock | ahead: none | n/a | equal | equal: origins | behind: auto-DDL [pgedge-oss] | behind: multi-master PG 15-19; migkit R3 cross-engine ahead | equal | equal | PostgreSQL (2025-09) |
| pg_easy_replicate | ahead: lag only | equal (native) | equal (native) | equal | equal | n/a | equal: `stats` vs migkit status | equal | MIT |
| Bucardo | ahead | behind n/a | behind: triggers on every write | equal | equal | behind: multi-master PG | equal | equal | BSD |
| pg_chameleon | ahead: none | equal (same library) | equal: same `mysql-replication` reader | ahead: migkit exact batches (R3) | equal: type map both | ahead: migkit any target | equal | equal | BSD-2 |
| pgloader | ahead: no verification (SEC §pgloader 5) | equal: migkit drives it `movers.py:87`; rules unused | n/a | ahead: restarts from zero | behind: CAST rules richer than migkit's refusals `movers.py:115-120` | behind: MSSQL/SQLite->PG sources unused | equal | ahead: SEC §pgloader verify-full rejected | PostgreSQL |
| pgsync | ahead | equal | n/a | equal | n/a | n/a | equal | behind: write-side data rules; migkit masks display only `masking.py:89` | MIT |
| pg_comparator | equal: sum or xor fold tree, key in checksum [pg-comparator]; migkit sum-of-MD5 ranges + keyless buckets ahead on keyless | n/a | n/a | equal | n/a | behind: PG/MySQL/SQLite only vs migkit many; ahead | equal | equal | BSD |
| pg_repack | n/a | n/a | n/a | n/a | n/a | n/a | behind: footprint unrecognised `leftovers.py:35-47` | equal | BSD-3 |
| mydumper / myloader | ahead: `--checksum-all` per table [mydumper]; migkit per range | behind until row 5: `--rows` same-table split, zstd, stream unused `movers.py:2128`; TP §8 | n/a | ahead: myloader resume by file; migkit by range | equal | n/a | equal: `--machine-log-json` used | ahead: SEC §mydumper; migkit `--defaults-file` | GPL-3.0 (driven) |
| MySQL Shell utilities | ahead: `checksum` after load [mysqlsh-load]; migkit per range | behind: >200 MB/s load, streamed copy [mysqlsh-copy], TP §7; not wrapped | n/a | equal: progressFile resume with dedup; migkit by range | equal: compatibility options vs migkit DDL | n/a | behind: progress JSON not read | equal: SEC §MySQL Shell | GPLv2 (driven) |
| MySQL CLONE | equal: page copy | behind: not wrapped | n/a | behind: restart only | n/a | n/a | equal: `clone_status` | equal: `clone_ssl_*` | GPLv2 server |
| XtraBackup | equal | behind: not wrapped (TP §8: fastest overall in Percona's test) | n/a | equal | n/a | n/a | equal | equal | GPLv2 (driven) |
| gh-ost | ahead: row count only | n/a | equal: binlog apply in `--dml-batch-size` [gh-ost-flags] | equal: `--checkpoint` `_ghk` (new) | n/a | n/a | behind: unauthenticated socket commands (SEC §gh-ost) - migkit ahead on auth | ahead | MIT |
| pt-osc | ahead | n/a | behind: triggers | equal: `--resume` since 3.6 [pt-osc-resume] | n/a | n/a | equal | ahead | GPL-2.0 |
| pt-table-checksum / sync | ahead: XOR-folded CRC32 cancels on duplicates [pt-tc-query]; migkit sums since R18 | n/a | n/a | equal: `--resume` | n/a | behind: replica topologies only | equal | ahead: version check calls home (fix row 3) | GPL-2.0 |
| mssql-python bulk / bcp / sqlpackage | ahead: none | behind: TDS bulk + Arrow in Rust [mssql-bulk]; migkit `executemany` `dbapi.py:203`, `0e` | n/a | ahead: commits on its own connection | equal: dacpac vs migkit sqlglot T-SQL `mssql.py:737` | n/a | equal | equal | MIT / Microsoft |
| Babelfish Compass | n/a | n/a | n/a | n/a | behind: per-version support classes [compass]; migkit names what it cannot carry `hetero.py:2160` | n/a | equal | equal | Apache-2.0 |
| ora2pg | ahead: `TEST_DATA` first 10k rows [ora2pg] | behind: MOD splits (with #766 row loss) vs migkit Oracle bulk NOT_YET | n/a | ahead | behind: PL/SQL conversion + cost report | behind: Oracle/MySQL->PG breadth; migkit Oracle read-only side | equal | equal | GPL-3.0 (driven) |
| python-oracledb | n/a | behind: `direct_path_load` unused [oracledb-dpl] | n/a | equal | n/a | n/a | n/a | equal | Apache-2.0/UPL |
| Sling | ahead: none (MECH §3) | behind/equal: TC 19m34s free vs dlt+CX 10m51s; migkit unmeasured on that shape | equal | ahead: state tables | equal | behind: many file/warehouse connectors | equal | ahead | GPL-3.0 |
| dlt | ahead | equal: backends incl. ConnectorX (TC) | equal: pg_replication source | ahead: exact batches vs dedup by key | behind: schema contracts | behind: breadth | equal | ahead | Apache-2.0 |
| ConnectorX / Polars | n/a | behind on raw read: Rust parallel partitions [connectorx]; not adopted (R18) | n/a | ahead: no shared snapshot | n/a | equal | n/a | equal | MIT |
| ADBC / Flight | n/a | behind: binary COPY -> Arrow [adbc-pg] unused | n/a | n/a | n/a | equal | n/a | equal | Apache-2.0 |
| DuckDB scanners | ahead | equal: ctid-parallel binary COPY [duckdb-pg] vs migkit ctid ranges `postgres.py:714` | n/a | ahead | n/a | equal | n/a | equal | MIT |
| reladiff | equal: hashdiff bisection [reladiff]; migkit digest + drilldown cap; ahead cross-engine via `canon` | n/a | n/a | equal | n/a | equal | behind: `--json` unused `generic.py:66` | ahead: migkit private conf `mysql.py:1736` | MIT |
| DVT | equal: row hash, grouped, partitions [dvt]; migkit uses count/sum/min/max only (WIP-B) | n/a | n/a | equal | n/a | behind: Teradata, Spanner, Hive | equal | equal | Apache-2.0 |
| datacompy | equal | n/a | n/a | n/a | n/a | n/a | behind: raw `report()` names the tool `cli.py:673` | equal | Apache-2.0 |
| sqlglot | n/a | n/a | n/a | n/a | behind: `optimize`, `diff`, `lineage` unused [sqlglot] | equal | n/a | n/a | MIT |
| sqlfluff | n/a | n/a | n/a | n/a | behind: generated scripts unlinted | equal | n/a | n/a | MIT |
| Atlas | equal: tables-level diff used `movers.py:1764` | n/a | n/a | n/a | equal | equal | equal | behind: `doctor` installs the proprietary build `tools.py:32` [atlas-ce] | Apache-2.0 CE / proprietary default |
| Liquibase / Flyway | n/a | n/a | n/a | equal | behind: history tables unread in `assess` | equal | equal | equal | FSL-1.1 (5.x) / Apache-2.0 |
| skeema | n/a | n/a | n/a | n/a | behind: MySQL lint rules | n/a | equal | equal | Apache-2.0 |
| results (migra) | equal: used `postgres.py:1664` | n/a | n/a | n/a | equal | n/a | equal | equal | Apache-2.0 (verify) |
| pgstream | ahead: none | equal | equal: wal2json only [pgstream] | equal | behind: event-trigger DDL log | behind: OpenSearch/webhooks sinks | equal | ahead: no source footprint in migkit | Apache-2.0 |
| Sequin | ahead | n/a | behind on claim: >50k ops/s, 55 ms [sequin]; migkit 65k/s applier offline (SCORE B1) - unmeasured live | equal: idempotency keys vs migkit marks | n/a | behind: 16+ sinks | equal: Prometheus both (`ui.py`) | equal | MIT |
| psycopg 3 | n/a | behind: binary block COPY unused [psycopg-copy] | behind: pipeline unused [psycopg-pipeline] | n/a | n/a | n/a | n/a | equal | LGPL-3.0 |
| asyncpg | n/a | equal (not adopted, §7.2) | n/a | n/a | n/a | n/a | n/a | equal | Apache-2.0 |
| PyMySQL / mysqlclient / connector | n/a | equal: applier-bound (R2.6) | equal | n/a | n/a | n/a | n/a | equal | MIT / GPL-2.0 / GPLv2+UFE |
| orjson / msgspec | n/a | equal | n/a | n/a | n/a | n/a | n/a | n/a | Apache-2.0,MIT / BSD-3 |
| zstd / lz4 | equal: frame checksum unused | equal: relay zstd `postgres.py:7170` | n/a | n/a | n/a | n/a | n/a | n/a | BSD-3 |
| xxhash / blake3 | equal (R18.3 measured) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | BSD-2 / CC0-Apache-2.0 |
| pyarrow | equal | equal: Parquet/DuckDB `engines/parquet.py` | n/a | n/a | n/a | n/a | n/a | n/a | Apache-2.0 |

---

## 10. Sources

- [pgcopydb-clone] https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html
- [pgcopydb-list] https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_list.html
- [pg-dump] / [pg-dump-18] https://www.postgresql.org/docs/18/app-pgdump.html
- [pg-restore] / [pg-restore-17] https://www.postgresql.org/docs/17/app-pgrestore.html
- [pg-basebackup] https://www.postgresql.org/docs/current/app-pgbasebackup.html
- [pgedge-oss] https://www.pgedge.com/blog/pgedge-goes-open-source ; https://github.com/pgEdge/spock
- [pgloader] https://pgloader.readthedocs.io/en/latest/ref/mysql.html
- [mydumper] https://mydumper.github.io/mydumper/docs/html/mydumper_usage.html
- [myloader] https://mydumper.github.io/mydumper/docs/html/myloader_usage.html
- [mydumper-mask] https://www.percona.com/blog/masquerade-your-backups-to-build-qa-testing-environments-with-mydumper/ ; https://github.com/mydumper/mydumper/releases
- [mysqlsh-copy] https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utils-copy.html
- [mysqlsh-load] https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utilities-load-dump.html
- [mysqlsh-lic] https://docs.oracle.com/cd/E17952_01/mysql-shell-8.0-license-gpl-en/licensing-information.html ; https://www.mysql.com/about/legal/licensing/oem/
- [mysql-clone] https://dev.mysql.com/doc/refman/8.4/en/clone-plugin-remote.html
- [xtrabackup] https://docs.percona.com/percona-xtrabackup/8.0/
- [gh-ost-flags] https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md
- [pt-osc-resume] https://www.percona.com/blog/whats-new-in-percona-toolkit-3-6-0/ ; https://www.percona.com/blog/resume-your-failed-pt-online-schema-change-job/
- [pt-tc] https://docs.percona.com/percona-toolkit/pt-table-checksum.html
- [pt-tc-query] https://bugs.launchpad.net/percona-toolkit/+bug/1427552 ; https://dzone.com/articles/how-avoid-hash-collisions-when
- [mssql-bulk] https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/bulk-copy?view=sql-server-ver17 ; https://github.com/microsoft/mssql-python/issues/619
- [compass] https://github.com/babelfish-for-postgresql/babelfish_compass
- [oracledb-dpl] https://python-oracledb.readthedocs.io/en/stable/release_notes.html ; https://github.com/oracle/python-oracledb/issues/543
- [ora2pg] https://ora2pg.darold.net/docs/configuration ; https://github.com/darold/ora2pg/issues/766 ; https://postgrespro.com/docs/ora2pgpro/24/ora2pgpro-data-validation
- [adbc-pg] https://arrow.apache.org/adbc/current/driver/postgresql.html
- [connectorx] https://github.com/sfu-db/connector-x
- [duckdb-pg] https://duckdb.org/docs/current/core_extensions/postgres/overview.html
- [sling-lic] https://github.com/slingdata-io/sling-cli/blob/main/LICENSE ; https://slingdata.io/
- [pgstream] https://github.com/xataio/pgstream
- [sequin] https://github.com/sequinstream/sequin
- [reladiff] https://reladiff.readthedocs.io/en/latest/how-to-use.html
- [dvt] https://github.com/GoogleCloudPlatform/professional-services-data-validator
- [datacompy] https://github.com/capitalone/datacompy
- [pg-comparator] https://manpages.debian.org/testing/postgresql-comparator/pg_comparator.1.en.html ; https://www.coelho.net/sw/pg_comparator/
- [sqlglot] https://sqlglot.com/sqlglot.html ; https://github.com/tobymao/sqlglot
- [atlas-ce] https://atlasgo.io/community-edition ; https://github.com/ariga/atlas/releases ; https://atlasgo.io/blog/2025/10/28/v038-analyzers-pii-and-migration-hooks
- [liquibase-fsl] https://www.liquibase.com/blog/liquibase-community-for-the-future-fsl ; https://docs.liquibase.com/community/release-notes/5-0 ; https://github.com/keycloak/keycloak/issues/43391
- [psycopg-copy] https://www.psycopg.org/psycopg3/docs/basic/copy.html
- [psycopg-pipeline] https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html
- [msgspec-types] https://jcristharif.com/msgspec/supported-types.html ; https://adamj.eu/tech/2026/08/07/introducing-django-msgspec/
- [orjson-int] https://github.com/ijl/orjson/blob/master/README.md ; https://github.com/ijl/orjson/issues/301
- Earlier reports cited: `security-oss-tools-2026-09-27.md`,
  `security-throughput-scorecard-2026-09-28.md`,
  `throughput-published-2026-09-27.md`, `throughput-cdc-techniques-2026-09-27.md`,
  `mechanisms-cdc-elt-specialists-2026-09-28.md`,
  `mechanisms-dts-tidb-vitess-molt-voyager-2026-09-28.md`,
  `oss-nonrelational-tools-2026-09-28.md`; WIP-A and WIP-B (paused agents).
