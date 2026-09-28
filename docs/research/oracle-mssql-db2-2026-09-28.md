# Oracle, SQL Server, Db2 (and SAP ASE, Informix) as sources and targets: bulk, follow, verify, and how to test them on an arm64 Mac (research as of 2026-09-28)

Status: COMPLETE for sections 0-6. Thinnest where vendors publish least:
startup seconds for Oracle faststart and SQL Server under Rosetta (no
measured source found; rough third-party estimates marked as such), whether
Oracle Free has Flashback Database / guaranteed restore points, the number of
PDBs Free allows, and whether CDC runs on SQL Edge in practice (docs say
yes). Every such point is marked "to measure" in section 6. Nothing was run against a database; the host was read only:
Apple M5, 16 GB, macOS 26.2, `uname -m` arm64; colima 0.10.3, profile
`default`: `vmType: vz`, `arch: aarch64`, `rosetta: false`, 2 CPU, 4 GiB,
60 GiB, virtiofs, docker runtime (`~/.colima/default/colima.yaml`).

## 0. What migkit has today (read from the code, 2026-09-28)

| engine | module | lines | driver | cells not yet built (`capabilities.py` GAPS) | how tests get a server |
|---|---|---|---|---|---|
| Oracle | `migkit/engines/oracle.py` | 217 | python-oracledb thin | sequences, params, bulk-move, stream, fence, confirm, delta, users, snapshot (all "11") | none: `tests/test_oracle_as_one_side_of_a_pair.py` holds renderer/name folding without a server; Oracle Free was tried 2026-09-25 and filled the VM disk / took 2.5 of 3.8 GB |
| SQL Server | `migkit/engines/mssql.py` | 954 | sqlcmd (native checks) + pymssql TDS 7.4 (neutral side) | bulk-move only ("0e") | `mcr.microsoft.com/azure-sql-edge:latest` natively on arm64 (`test_sql_server_follows_through_change_tracking.py`, `test_sql_server_sums_what_each_row_is.py`); `test_mssql_smart_checks.py` wants `mssql/server:2022-latest` `--platform linux/amd64` and skips when not pulled |
| Db2 LUW | `migkit/engines/db2.py` | 122 | ibm_db_dbi | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot ("34") | none: `tests/test_db2_as_one_side_of_a_pair.py` without a server ("IBM's Db2 image runs on x86 only") |
| SAP ASE | `migkit/engines/ase.py` | 125 | pyodbc + FreeTDS ODBC at TDS 5.0 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot ("34") | none: `tests/test_sap_ase_as_one_side_of_a_pair.py` |
| Informix | - | - | - | not an engine | - |

Shared base: `migkit/engines/dbapi.py` (`DbapiRows`): keyed reads resumed by
key, digest folded in-process through `canon.fold_rows` over 5,000-row
batches (`neutral_digest`, line 163), writes as delete-by-key then
`cursor.executemany` insert in one transaction (`neutral_write`, line 175),
change application by update-then-insert (`_apply_upserts`). No engine here
has a server-side digest except SQL Server's native check (sum of the first
7 bytes of `HASHBYTES('SHA2_256', (select t.* for json path ...))` as a
decimal, `mssql.py:266`). No engine here has a `native_bulk`.

SQL Server follow: Change Tracking (`mssql.py:819-954`): the version is read
before the rows (at-least-once), `CHANGE_TRACKING_MIN_VALID_VERSION` checked
before reading, every moved table must be tracked; the read is **not**
inside a `SNAPSHOT` transaction (already flagged in
`mechanisms-cdc-elt-specialists-2026-09-28.md` item 6).

Backlog anchors: item 11 (Oracle, `docs/backlog.md:1776`), item 27 (SQL
Server depth, `:2367`), item 34 (Db2, ASE, `:3080`), R10 (Oracle LOB in
pieces, `:4006`), R11 (stored code), R12 (engines the sandbox can now run,
`:4039`), R13 (capability matrix, `:4056`), R8 (DVT for Oracle/Db2 blocked on
x86, `:3955`).

## 1. The sandbox: what runs on this machine

### 1.1 Per engine

| engine | image | arm64 native? | size (compressed, arm64) | start | memory | what it gives |
|---|---|---|---|---|---|---|
| Oracle 23ai/26ai Free | `gvenzl/oracle-free:slim-faststart` (pin `23.26.x-slim-faststart`) | **yes**, multi-arch since 23.5 (Sept 2024) | 1.20 GB (`slim` 714 MB, `latest-faststart` 1.49 GB, `full-faststart` 2.54 GB) | "faststart" = datafiles pre-expanded in the image; no published seconds; Testcontainers' default 60 s wait is too short under emulation (tc-java #8590, #9057), fine natively | hard cap 2 GB RAM (SGA+PGA), 2 foreground CPUs, 12 GB user data (incl. SYSAUX per forum reports) | tables, PL/SQL, flashback query `AS OF SCN`, LogMiner (after init script: ARCHIVELOG + supplemental logging), `DBMS_COMPARISON`, `STANDARD_HASH`; **slim drops RMAN, Oracle Text, Spatial, MLE** - use regular `latest-faststart` where RMAN/Data Pump matter |
| Oracle (vendor) | `container-registry.oracle.com/database/free:latest-lite` / `23.26.0.0-lite-arm64` | yes (Nov 2024) | lite ~773 MB, full ~3.36 GB | pre-built DB, "very fast" | same caps | lite lacks perl (no `MAX_STRING_SIZE=EXTENDED` switch) |
| SQL Server (engine) | `mcr.microsoft.com/azure-sql-edge:latest` | yes, but **frozen**: ARM64 retired Sept 2023, product retired 30 Sept 2025, image left in MCR as-is | - | seconds (what the migkit tests use) | Developer cap 4 cores / 32 GB | the 2022-era engine: CT **and CDC** (docs: both supported, CLR-dependent CDC functions not), snapshot isolation, temporal tables, `BULK` TDS protocol; **not**: spatial, hierarchyid, CLR, database snapshots, linked servers, replication publisher, Ledger, `JSON_OBJECT`, `IS DISTINCT FROM` |
| SQL Server 2022/2025 | `mcr.microsoft.com/mssql/server:2022-CU<n>-ubuntu-22.04` `--platform linux/amd64` | **no** (x86-64 only, Microsoft does not support emulation) | ~1.5 GB (the skip reason in `tests/test_mssql_smart_checks.py`) | Rosetta: CPU ~80-95 % of native (sieve 26 s native / 32 s Rosetta / 254 s QEMU); QEMU fails outright (`Invalid mapping of address ... below 0x400000000000`) | give the VM >= 4 GB; 2 GB VMs SIGABRT (colima #1043, #1138) | full engine incl. Agent + CDC, spatial, CLR, Always Encrypted metadata; **risks**: a 2022 CU digest restarted in a loop under Rosetta (mssql-docker #929, pin a known-good CU); macOS 26 Tahoe broke 2025 RC1 under Rosetta; 2025 RTM needed AVX (fixed in 2025 CU1) |
| Db2 LUW 12.1 Community | `icr.io/db2_community/db2` `--platform linux/amd64 --privileged` | **no** (`no matching manifest for linux/arm64/v8`) | not published here (x86) | IBM's own macOS guide: first start **15-20 min** (instance + DB creation); later starts minutes | several GB (IBM's run line: `--shm-size=1g --ipc=host --privileged`); 12.1 Community caps server memory at 8 GB, non-production only | whole LUW engine; broke on an M3 after macOS 14.4.1 (`SQL1032N`), IBM: "we don't support the arm architecture" |
| SAP ASE | no image from SAP; community `datagrip/sybase:16.0` (JetBrains, from nguoianphu/docker-sybase) | **no** (amd64 only; SAP lists x86 for ASE 16 on Linux) | ~3.08 GB (`15.7` ~705 MB) | ~30 s after start; fails to start now and then (DataGrip/docker-env #8), so wait on a real `select 1` | - | ASE 16.0; charset iso_1, not utf8 (#9); users `sa`/`tester` |
| Informix | `icr.io/informix/informix-developer-database:15.0.0.0` (moved from Docker Hub `ibmcom/...`) | **no** container for arm64 (the 15.0.0.2 server itself now builds for aarch64 AlmaLinux 8.10) | - | - | - | Debezium's Informix connector tests use a custom image on this base |

### 1.2 What colima can do here

colima 0.10.3 on `vz` can run amd64 containers through **Rosetta** instead of
QEMU: `vmOpts.vz.rosetta: true` in the profile (the top-level `rosetta` key is
deprecated in 0.9+), keep `arch: aarch64`, run the container with
`--platform linux/amd64`. The backlog's R12 line "the 2022 image runs only
under Rosetta, which colima does not use" is out of date: colima uses Rosetta
when the profile says so. It is a VM setting the owner changes (a restart of
the VM), not something a test does. A **second profile** (`colima start x86
--vm-type vz --vz-rosetta --cpu 4 --memory 6`) keeps the arm64 default
untouched; the host has 16 GB.

Risk that decides the plan: Apple said Rosetta stays "through macOS 27" and
is then cut back to a subset; whether Linux-VM Rosetta survives is not
stated. So anything proved only under Rosetta is proved on borrowed time -
an x86 CI runner is the durable place for SQL Server 2022/2025 and Db2.

How other projects test these on arm Macs:
* Oracle: Testcontainers modules and the `gvenzl/setup-oracle-free` GitHub
  Action use `gvenzl/oracle-free:slim-faststart` (Linux runners only);
  Debezium-style LogMiner tests (e.g. Apache Pulsar connectors PR #62) mount
  one SYSDBA script in `/container-entrypoint-initdb.d/` that switches to
  ARCHIVELOG, adds minimal supplemental logging, creates `c##` common user
  and grants; the entrypoint runs it before printing `DATABASE IS READY TO
  USE!`, so the readiness wait covers the restart. python-oracledb's own
  suite runs against any reachable DB via `PYO_TEST_*` variables and
  `tests/create_schema.py`.
* SQL Server: Microsoft's own advice since 2023 is amd64 emulation under
  Rosetta; Edge is kept by projects that need arm64 native (as migkit does).
* Db2: IBM's Db2 Developer Extension docs run the amd64 image under colima
  `--vm-type=vz --vz-rosetta`; no arm64 image exists.

### 1.3 Memory budget on this VM (4 GiB today)

Oracle Free alone is 2 GB by licence cap plus ~0.3-0.5 GB of processes; the
2026-09-25 attempt measured 2.5 of 3.8 GB and filled the disk. Running
Oracle next to a PostgreSQL target needs the VM at 6 GB; Oracle plus SQL
Server under Rosetta (>= 2 GB each) needs 8 GB. The disk: `slim-faststart`
is 1.2 GB compressed and a few GB expanded; 60 GiB is enough if old images
are pruned first (the earlier failure was a full disk, not the image alone).

## 2. Oracle

### 2.1 Driver facts (python-oracledb, thin, no Instant Client)

* **Fetch.** `Cursor.arraysize` (default 100) and `prefetchrows` (default 2)
  set round trips; for bulk reads set `arraysize` to 5,000-50,000 and
  `prefetchrows = arraysize + 1`. Arrow: `Connection.fetch_df_all()` /
  `fetch_df_batches(size=...)` since 3.0 (March 2025), `decimal256` for
  NUMBER since 26.0.0 (Sept 2026); the DATE bugs noted in the backlog were
  fixed in 3.2.0 (localisation) and 3.4.1 (dates in 2038 or later).
* **Types.** NUMBER comes back as `int` where precision/scale say integer,
  else `float` unless `fetch_decimals=True` (migkit sets it). DATE is a
  `datetime`. **TIMESTAMP WITH (LOCAL) TIME ZONE come back naive, the zone
  dropped** - read them as `SYS_EXTRACT_UTC(col)` plus `TZ_OFFSET`/
  `EXTRACT(TIMEZONE_REGION ...)` as text, or `TO_CHAR(col,
  'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`; migkit leaves them out today.
  INTERVAL DAY TO SECOND is a `timedelta`, YEAR TO MONTH `oracledb.IntervalYM`.
  `''` is NULL (already handled as a difference).
* **LOBs.** `fetch_lobs=False` returns CLOB/BLOB as `str`/`bytes` (up to 1 GB
  each, held in memory); for R10's "large values in pieces" keep locators
  (`fetch_lobs=True`) and stream `lob.read(offset, amount)` in multiples of
  `lob.getchunksize()`; write with `lob.write(data, offset)` on a locator
  from `RETURNING ... INTO` or `EMPTY_CLOB()`.
* **Array DML.** `executemany(sql, rows, batcherrors=True,
  arraydmlrowcounts=True, batch_size=N)` (`batch_size` since 3.4);
  `setinputsizes` avoids buffer re-allocation on strings; a bind value is
  capped at 2 GB (`DPI-1015`).
* **Direct path load** - `Connection.direct_path_load(schema_name,
  table_name, column_names, data, batch_size=...)`, **thin mode only**, since
  **3.4.0 (Oct 2025)**: no INSERT statements, bypasses the buffer cache,
  minimal redo; takes row sequences or any Arrow PyCapsule data frame;
  **commits implicitly per batch** (no transaction control); CLOB/BLOB must be
  `str`/`bytes`; no object types. Oracle's blog: 2 M rows 4x faster than
  pandas and 3x faster than their previous executemany advice. Open issues:
  a leak when called in a loop with pandas frames (#561), `DPY-4009` with
  frames and `batch_size` (#551, 3.4.1), boolean fix in 26.1.0b1.
* **Pipelining** (23ai servers, 2.4+) and **sessionless transactions**
  (23.6, 3.3) exist; not needed for bulk.

### 2.2 Consistent read: SCN and flashback

`SELECT current_scn FROM v$database` (or
`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`) then every worker reads
`SELECT ... FROM t AS OF SCN :scn` - a snapshot shared across any number of
connections, as a PostgreSQL exported snapshot is. It needs `FLASHBACK` on
the table (or `FLASHBACK ANY TABLE`) and enough undo: a read older than
`UNDO_RETENTION` (and not guaranteed) fails `ORA-01555 snapshot too old`
rather than returning wrong rows - the failure is loud, which is the
property migkit wants. Debezium's snapshot does exactly this (all READ events
carry the same SCN). DDL after the SCN makes the flashback read fail
(`ORA-01466`), also loud.

### 2.3 Change follow

* **LogMiner** (`DBMS_LOGMNR`), in every edition incl. Free. `CONTINUOUS_MINE`
  was deprecated in 12.2 and **desupported in 19c**, so a reader now adds the
  logs itself: find online/archived logs covering `[start_scn, end_scn]`
  (`V$LOG`, `V$ARCHIVED_LOG`), `DBMS_LOGMNR.ADD_LOGFILE` each,
  `START_LOGMNR(STARTSCN, ENDSCN, OPTIONS => DICT_FROM_ONLINE_CATALOG
  [+ COMMITTED_DATA_ONLY])`, read `V$LOGMNR_CONTENTS` (SCN, COMMIT_SCN, XID,
  OPERATION, SEG_OWNER, TABLE_NAME, SQL_REDO, ROW_ID, CSF for continued
  rows), `END_LOGMNR`, advance the window. Needs ARCHIVELOG, minimal
  supplemental logging at DB level plus `(PRIMARY KEY)` or `(ALL) COLUMNS` per
  table, and grants `LOGMINING`, `SELECT ANY TRANSACTION`, `EXECUTE ON
  DBMS_LOGMNR`, `SELECT ON V_$LOGMNR_CONTENTS / V_$LOG / V_$ARCHIVED_LOG /
  V_$DATABASE`, `FLASHBACK ANY TABLE`; in a CDB the miner connects to
  CDB$ROOT as a `c##` user. Debezium's choices, worth copying as *decisions*
  not modes: dictionary `online_catalog` (default since Debezium 3.0: no
  extra redo, but no DDL tracking), `redo_log_catalog` (dictionary written to
  redo at each switch: DDL tracked, more redo), `hybrid` (2.7+: online
  catalog, falls back to its own schema history when LogMiner cannot name a
  column; **not with LOBs**); buffering: `logminer` (uncommitted rows
  buffered in the reader until commit) vs `logminer_unbuffered`
  (`COMMITTED_DATA_ONLY`, database does the buffering, risks
  `PGA_AGGREGATE_LIMIT` on large transactions); Debezium 3.6 (July 2026)
  replaced nine window/sleep knobs by a log-count-based window. SQL_REDO
  must be parsed (Debezium has a hand-written parser); a LOB change comes
  as several `SEL_LOB_LOCATOR` / `LOB_WRITE` rows. Ignore SYS/SYSTEM changes.
  Pitfall for tests: changes made as SYS are not captured.
* **XStream** (`DBMS_XSTREAM_ADM` outbound server, OCI only - thick mode):
  needs an Oracle **GoldenGate licence**; out of reach for an open-source
  default.
* **OpenLogReplicator** (C++, by Adam Leszczynski): parses redo and archive
  files itself, no LogMiner; output JSON or Protobuf to Kafka, a file, TCP or
  ZeroMQ; Debezium's `olr` adapter consumes it (OLR >= 1.3.0, 1.9 recommended
  for Debezium 2.x/3.x). Oracle 11.2-26ai, all editions incl. XE/FREE;
  single instance, not RAC; needs ARCHIVELOG + minimal supplemental logging,
  its own grants on `SYS.OBJ$`, `TAB$`, ...; reads the redo files **on the
  database host or over a mounted file system (SSHFS/NFS)** - impossible on
  RDS and most managed services. Platforms listed: Linux x86_64 and Solaris -
  **arm64 not listed** for either OLR or the database. Licence: the repo now
  says **AGPL-3.0** (older pages say GPL v3); migkit can call it as a separate
  program, never link it. 1.9 is stable, 2.0 in development.
* **Managed services**: RDS for Oracle supports LogMiner (with its own
  `rdsadmin` procedures for supplemental logging/retention); AWS DMS adds a
  "Binary Reader" (its own redo parser) for >10 GB/h redo; Datastream offers
  LogMiner and a binary reader (preview). Nothing there migkit can wrap.

### 2.4 Verify in the server

* `ORA_HASH(expr, 4294967295)`: 32-bit, not for LOB/LONG; sum per range is
  order-independent but the 32 bits collide.
* `STANDARD_HASH(expr, 'SHA256')` (12c+): RAW, cannot take LOB/LONG/object
  types; much faster than `DBMS_CRYPTO.HASH` (which does take LOBs, needs
  `EXECUTE ON DBMS_CRYPTO`). Server-side digest that matches migkit's
  (count, sum of a k-bit row hash): `sum(to_number(substr(rawtohex(
  standard_hash(<row text>, 'SHA256')), 1, 14), 'XXXXXXXXXXXXXX'))` - 56 bits
  per row summed in NUMBER (exact to 38 digits), the same shape as the SQL
  Server check's 7 bytes.
* **`CHECKSUM` (21c+) is an XOR fold** (duplicates cancel, three copies equal
  one, swaps within a column invisible): the same trap as MySQL's BIT_XOR
  found 2026-09-27. Never use it.
* `DBMS_COMPARISON` (11.1+, needs only the database licence per a user report
  of Oracle sales): Oracle-to-Oracle over a database link, buckets of rows
  hashed by ORA_HASH, recursive split, `RECHECK`, `CONVERGE` with local/
  remote wins and a session tag so converge changes are not replicated. Needs
  a unique index on number/date/timestamp columns (`ORA-23676` otherwise),
  same character set, skips LOBs, LONG, ROWID and user types; 32-bit hash.
  Useful as a *second reader* for Oracle-to-Oracle, not as migkit's check.
* `DBMS_SQLHASH.GETHASH(query, digest_type)`: one hash of a whole ordered
  result set; order-dependent, so only for a keyed range with ORDER BY.

### 2.5 What migkit should build (Oracle)

Decision rules (each read off the server, never an option):

* **Read (source).** Keyed ranges read `AS OF SCN :s` from one SCN taken at
  the start, `arraysize` sized by migkit from row width (target ~8 MB per
  fetch); Arrow batches (`fetch_df_batches`) where the target takes Arrow
  (DuckDB, Parquet, SQL Server `bulkcopy_arrow`, ClickHouse). No FLASHBACK
  privilege -> refuse the multi-worker read and say so (a single-connection
  read in a read-only serializable transaction, `SET TRANSACTION READ ONLY`,
  is the fallback). A table with LOBs over the fetch budget -> locator reads
  in chunk multiples (R10).
* **Write (target).** Empty target table, no triggers, and the run can take
  per-batch commits -> `direct_path_load` in batches sized by memory, then
  re-enable/validate constraints and rebuild indexes, and the deep check
  already names anything left invalid or NOT VALIDATED. Otherwise (resume
  into a non-empty table, triggers, change apply) -> `executemany` with
  `batcherrors=True` and `MERGE` for upserts. Oracle to Oracle, whole schema,
  database link allowed -> `DBMS_DATAPUMP` with `NETWORK_LINK` driven over
  SQL (no client tool, no row through migkit; the ClickHouse `remote()`
  pattern) - to be measured on the regular (not slim) image.
* **Follow.** LogMiner through python-oracledb, migkit's own reader (no
  licence problem, no host access, works on RDS): window by SCN over the logs
  that cover it, `DICT_FROM_ONLINE_CATALOG`, **COMMITTED_DATA_ONLY chosen when
  the largest open transaction (from `V$TRANSACTION.USED_UBLK`) fits the PGA
  budget, else buffered in migkit**; supplemental logging checked per table
  before the start (a table without PK/ALL logging is refused, named); the
  token = (commit SCN, XID, RS_ID/SSN) with `position_lost` = the oldest SCN
  still in `V$ARCHIVED_LOG` greater than the token; stream identity = DBID +
  `RESETLOGS_CHANGE#` (a flashback/resetlogs makes old SCNs mean something
  else). DDL: compare the catalogue before each batch (as `drift.py` does)
  rather than mine the dictionary. OpenLogReplicator wrapped as a mover only
  when migkit runs on the database host and the architecture is x86-64.
* **Fence.** `current_scn` on the source; the tail has applied a commit SCN
  >= it.
* **Verify.** Oracle-Oracle: the server-side STANDARD_HASH sum per key range,
  with LOB columns hashed through `DBMS_CRYPTO` when granted, else read and
  folded in-process; Oracle-other: the in-process renderer as today. Delta:
  keys named by the mined changes since the last cycle.
* **Users.** `DBMS_METADATA.GET_DDL('USER', ...)` returns the password
  **hash** (`IDENTIFIED BY VALUES 'S:...;T:...'`) when the reader has
  `SELECT_CATALOG_ROLE`, plus `GET_GRANTED_DDL('ROLE_GRANT'|'SYSTEM_GRANT'|
  'OBJECT_GRANT', user)`; profiles and quotas the same way.
* **Sequences.** `ALL_SEQUENCES.LAST_NUMBER` is the next value *after the
  cache* (not the last used); carry `max(col)+1` for identity columns
  (`ALL_TAB_IDENTITY_COLS`) and `ALTER SEQUENCE ... RESTART START WITH`
  (18c+).
* **Snapshot.** Guaranteed restore point (`CREATE RESTORE POINT x GUARANTEE
  FLASHBACK DATABASE`) needs ARCHIVELOG + a fast recovery area; `FLASHBACK
  TABLE ... TO SCN` needs row movement. Flashback Database is an Enterprise
  feature; whether Free has it is to be measured.
* **Params.** `V$PARAMETER` where `ISDEFAULT = 'FALSE'` plus NLS settings
  (`NLS_DATABASE_PARAMETERS`, character set - a WE8ISO8859P1 source into
  AL32UTF8 grows strings and can overflow `VARCHAR2(n BYTE)`).
* **Statistics.** Already `DBMS_STATS.GATHER_SCHEMA_STATS`.

## 3. SQL Server

### 3.1 Bulk from Python

| path | how | measured / claimed | notes for migkit |
|---|---|---|---|
| pyodbc `fast_executemany` | one `sp_prepare`, rows as a parameter array, server runs `sp_execute` per row | ~35-45 k rows/s in independent tests (3 M-row taxi file: 65 s) | needs Microsoft's ODBC driver; breaks on local temp tables; `setinputsizes` for strings > 255 |
| `bcp` (mssql-tools18) via files | native format `-n`, `-E` keep identity, `-h TABLOCK` | same benchmark: 72 s for 3 M (temp files, process start) | only pays off for very large loads; a separate client install (the go `sqlcmd` migkit uses has no bcp) |
| .NET `SqlBulkCopy` (arrowsqlbcpy) | TDS bulk load | 26 s for 3 M (~115 k rows/s), 2.5x `fast_executemany` | .NET runtime; not a default |
| **pymssql `Connection.bulk_copy`** (since 2.2.0, 2021) | FreeTDS bcp API: `bulk_copy(table, rows, column_ids, batch_size=1000, tablock, check_constraints, fire_triggers)` | no published numbers | **already migkit's driver** - no new dependency; **no keep-identity switch**: bcp without KEEPIDENTITY lets the server assign identity values and ignores the supplied ones (bcp `-E` semantics) - must be measured on Edge before trusting it for identity tables; a computed-column bug when built from source (#775) |
| **mssql-python `cursor.bulkcopy()`** (Microsoft, 1.4+) / `bulkcopy_arrow()` (1.13.0, 7 Aug 2026) | Rust `mssql_py_core` TDS bulk load; Arrow buffers streamed straight into bulk packets, GIL released | "comparable to bcp.exe and SqlBulkCopy" (Microsoft) | 1.15.0 (11 Sept 2026) ships wheels for macOS universal2, Linux glibc/musl x86-64 and **aarch64**, Python 3.10-3.14, driver bundled (no unixODBC). Options: `keep_identity`, `keep_nulls`, `table_lock`, `check_constraints` (default **False** - constraints end up untrusted), `fire_triggers`, `use_internal_transaction`, `batch_size`, `column_mappings`. Runs on **its own internal connection and commits independently** (a rollback on the main connection does not undo it); commit DDL first or it deadlocks. Arrow types must match (float64 into money/decimal raises; cast to `decimal128`). 1.13.0 fixed `executemany` silently inserting zero rows when a NULL appeared mid-batch in numeric arrays |

Server-side paths with no row through migkit: `BULK INSERT`/`OPENROWSET(BULK)`
need the file on the server; SQL Server to SQL Server over a **linked
server** (`INSERT ... SELECT FROM [src].db.dbo.t`) - not on Edge, not on
Azure SQL Database; **backup/restore chain** (`BACKUP DATABASE` + `BACKUP LOG`
restored `WITH NORECOVERY`, the last `WITH RECOVERY`) - the native bulk *and*
follow for a same-or-newer target version, what Azure's Log Replay Service and
Google DMS use; needs the full recovery model and a file path both servers
see (in the sandbox: a shared docker volume).

### 3.2 Follow: Change Tracking vs CDC

* **Change Tracking** (what migkit has): per table, the key + last operation
  + `SYS_CHANGE_COLUMNS` mask since a version; net changes only; synchronous,
  no Agent; retention via `CHANGE_RETENTION`. **Microsoft's documented
  protocol** runs the whole read in one SNAPSHOT transaction (database needs
  `ALLOW_SNAPSHOT_ISOLATION ON`): validate `CHANGE_TRACKING_MIN_VALID_VERSION`
  per table, read `CHANGE_TRACKING_CURRENT_VERSION()`, then
  `CHANGETABLE(CHANGES ...)` for every table, commit. Without it, cleanup can
  remove rows between the validation and the read and the result is wrong
  with no error; Microsoft's documented alternative is to **validate again
  after** the CHANGETABLE reads. migkit's `neutral_changes` validates before
  and reads outside a transaction (`mssql.py:895-939`), and its
  `stream_identity` keys on server name + database create date; Microsoft's
  "data restore" section shows a database restored with data loss handing
  out the same versions again for different changes, and nothing says the
  create date moves on a restore over an existing database. Add
  `recovery_fork_guid` from `sys.database_recovery_status` (a restore to a
  point in time starts a new fork) to the identity, and the last
  `msdb.dbo.restorehistory` row where readable. Also: `WITH CHANGE_TRACKING_CONTEXT (0x...)` on migkit's own
  writes marks them in `SYS_CHANGE_CONTEXT` - the SQL Server rung for
  two-way loop prevention; `CHANGETABLE(VERSION ...)` gives a row's last
  version for conflict checks. Caveats: a long snapshot transaction delays
  CT cleanup instance-wide (`sys.syscommittab` grows), so keep it short.
* **CDC**: an Agent job (`sp_MScdc_capture_job` -> `sp_cdc_scan` ->
  `sp_replcmds`, the replication log reader) copies from the log into
  `cdc.<schema>_<table>_CT`; every change, with `__$start_lsn` (commit LSN),
  `__$seqval` (order within the transaction), `__$operation` (1 delete, 2
  insert, 3/4 update before/after). Read with
  `cdc.fn_cdc_get_all_changes_<instance>(from_lsn, to_lsn, N'all update
  old')`, `from = sys.fn_cdc_increment_lsn(last)`, bounded by
  `sys.fn_cdc_get_min_lsn(instance)` and `sys.fn_cdc_get_max_lsn()`; an LSN
  outside the range raises the misleading **Msg 313 "insufficient number of
  arguments"** (Microsoft's own note). Default retention 3 days (4320
  minutes); the cleanup job moves `start_lsn` first, so treat
  `cdc.change_tables.start_lsn` as the low-water mark. Needs `sysadmin` to
  enable, a non-Express edition and the Agent (`MSSQL_AGENT_ENABLED=true` in
  a container). A column added after the capture instance is not captured
  until a second capture instance exists (two per table max). **Edge**: the
  docs say CT and CDC are both supported, CLR-dependent CDC functions not;
  the Agent runs T-SQL jobs on Edge (only CmdExec/PowerShell/SSIS... are
  unsupported), so CDC is **worth one measured try on Edge** before calling
  it x86-only (backlog R12 says it waits on x86).
* Decision rule (the planner's, not an option): CT where the hop's tables all
  have a primary key and net changes suffice (always true for a key-applied
  tail); CDC where a table has no primary key but a unique index the capture
  instance can use (`@index_name`), where every intermediate change matters
  (audit), or where CT is off and CDC is already on (never turn either on
  without the owner). Backup chain where source and target are both SQL
  Server, target version >= source, and the operator can give a shared path.

### 3.3 Verify pitfalls (what the current check does and misses)

`check_data` hashes `(select t.* for json path, include_null_values,
without_array_wrapper)` with SHA2_256 and sums 7 bytes per row
(`mssql.py:266`). Measured-in-docs traps:
* **CLR types** (`geography`, `geometry`, CLR UDTs; `hierarchyid` per the
  protocol spec) make FOR JSON fail with **Msg 13604** - the table is reported
  as an error, never compared. Fix: list columns, casting CLR columns to
  `varbinary(max)` (keeps SRID; FOR JSON renders binary as base64).
* **`rowversion`/`timestamp` columns** are regenerated on insert: every row
  differs on a copied table. Must be left out of the hash (and said).
  `mssql.py` has no mention of rowversion (grep, 2026-09-28).
* **HIDDEN columns** (a temporal table's period columns declared `HIDDEN`)
  are not in `t.*`: a difference there is invisible.
* A column name containing `.` becomes a nested object in `FOR JSON PATH`,
  and two such names can collide (error). Use `FOR JSON AUTO` or aliases.
* `CHECKSUM_AGG` is XOR: even repeats give 0 (never use); `CHECKSUM` is
  collation-aware (`McCavity` = `Mccavity` under CI) and errors on
  noncomparable types; `BINARY_CHECKSUM` silently skips text/ntext/image/xml/
  CLR and collides (`'H06858'` vs `'NP6858'`), and reads only the first 255
  characters of an nvarchar. `HASHBYTES`: only SHA2_256/SHA2_512 not
  deprecated since 2016; the 8,000-byte input cap is gone from 2016 on.

### 3.4 Identity, temporal, Always Encrypted

* **Identity**: `SET IDENTITY_INSERT` is one table per session at a time
  (migkit's `_before_insert` sets and unsets it); bulk copy needs
  `keep_identity`/`KEEPIDENTITY` instead; after the load, `DBCC CHECKIDENT
  (t, RESEED, max)` or carry `IDENT_CURRENT` (migkit's `snapshot_state`
  records identity values). Sequences: `sys.sequences.current_value`.
* **Temporal (system-versioned) tables**: `GENERATED ALWAYS AS ROW
  START/END` cannot be written even by bulk copy; carrying history means
  `SYSTEM_VERSIONING = OFF`, `DROP PERIOD FOR SYSTEM_TIME`, load current and
  history with their period values, `ADD PERIOD`, `SYSTEM_VERSIONING = ON
  (HISTORY_TABLE = ..., DATA_CONSISTENCY_CHECK = ON)`. Without that the
  target's period columns are the load time and the history is empty -
  a difference the check must name, not hide.
* **Always Encrypted**: the server holds ciphertext (`sys.columns.
  encryption_type` not null) and cannot decrypt. Read without
  `ColumnEncryption` to get the varbinary ciphertext, carry the CMK/CEK
  metadata (`sys.column_master_keys`, `sys.column_encryption_keys` +
  `..._values`) first, and insert only through bulk copy with
  `AllowEncryptedValueModifications` (SqlBulkCopy) / `BCPMODIFYENCRYPTED`
  (ODBC 17+ bcp API) or a user temporarily `WITH
  ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = ON`. A plain INSERT of the bytes fails
  (operand type clash); the modification switch accepts anything, so a wrong
  copy is silent corruption - verify by comparing ciphertext bytes (equal
  when copied verbatim, even for randomized encryption). Neither pymssql nor
  mssql-python exposes the modification switch as of 1.15.0 (not in the
  bulkcopy option list) - so: refuse such columns, named, unless the user
  option route is allowed.

### 3.5 What migkit should build (SQL Server)

* **Bulk (0e).** Target SQL Server: `mssql-python` `bulkcopy_arrow` when the
  source hands Arrow, `bulkcopy` otherwise, `keep_identity=True`,
  `keep_nulls=True` (else a NULL becomes the column default - a silent
  difference), `table_lock=True` on an empty table, `check_constraints=False`
  then `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL` (the deep check
  already names `is_not_trusted`). pymssql `bulk_copy` as the no-new-
  dependency fallback only for tables without identity, until measured.
  Source SQL Server: `mssql-python` Arrow fetch for wide reads; ranges read
  under `SNAPSHOT` isolation where allowed (one consistent point per worker
  is not shareable across connections in SQL Server - so read ranges while
  following CT from a version taken **before** the first range, the DBLog
  rule).
* **Follow.** Wrap the CT read in a SNAPSHOT transaction when
  `snapshot_isolation_state = 1`; otherwise re-validate
  `min_valid_version` after the read. Restore marker in the stream identity.
  CDC reader (LSN token = `__$start_lsn`, `__$seqval`) as the second rung,
  tried on Edge first.
* **Verify.** Exclude rowversion; cast CLR columns; include hidden columns
  explicitly; keep the 7-byte sum.

## 4. Db2 LUW

* **Driver.** `ibm_db` 3.2.5+ installs natively on Apple Silicon (wheel
  carries the v12.1 clidriver; Python 3.9-3.14). So migkit's client side runs
  here; only the server needs x86.
* **Bulk into Db2.** No client-side bulk API in `ibm_db` beyond
  `execute_many` (array insert). Server-side, over SQL, no CLP needed:
  `CALL SYSPROC.ADMIN_CMD('LOAD FROM (SELECT ...) OF CURSOR ... INSERT INTO
  t NONRECOVERABLE')`, and for Db2 to Db2 `LOAD FROM (DATABASE srcalias
  SELECT ...) OF CURSOR` - the **remote fetch**: target pulls from the source
  with no row through migkit (the source must be catalogued on the target
  server; no METHOD N; no WITH UR in the query; no progressive LOB
  streaming; types must match, `SQL1188N`). `INGEST` is a CLP command only.
  After LOAD: tables with constraints are left **Set Integrity Pending**
  (`SQL0668N` reason 1) until `SET INTEGRITY FOR t IMMEDIATE CHECKED`; a
  failed LOAD leaves **Load Pending** (reason 3) until `LOAD ... TERMINATE`;
  a recoverable database without `NONRECOVERABLE`/`COPY YES` leaves the
  table space **backup pending**; a NONRECOVERABLE load is skipped by a
  later rollforward (table marked invalid). migkit's Db2 deep check already
  names tables not in normal state (`db2.py` `check_deep`).
* **Consistent read.** Db2 has no flashback query for ordinary tables (only
  `FOR SYSTEM_TIME AS OF` on system-period temporal tables) and no exported
  snapshot: ranges are read as they are and reconciled by the follow from a
  log point taken before the first range.
* **Follow.** Licence-free options, in order: (1) **SQL Replication Capture**
  (`asncap`) - included with every Db2 LUW edition for Db2-to-Db2 per IBM's
  11.5 note (Q Replication and CDC left the editions in 11.5 and need IIDR);
  it reads the log (`db2ReadLog`) into CD tables in the source database, and
  Debezium's Db2 connector polls those - Debezium/Red Hat state that using
  the ASN libraries for it **requires an IIDR licence** (not an install), so
  migkit may use it only as the operator's licence allows, said in the plan;
  needs `LOGARCHMETH1` (archive logging) and `DATA CAPTURE CHANGES` on each
  table, and CD-table schemas do not follow DDL. (2) Where the table has a
  `ROW CHANGE TIMESTAMP` column or is system-period temporal: poll by it,
  deletes from the history table or a key-set diff. (3) Otherwise refuse a
  follow, named. `db2ReadLog` itself is a C API needing an agent on the host;
  not a Python path.
* **Fence.** `SELECT CURRENT_LSN FROM TABLE(MON_GET_TRANSACTION_LOG(-1))`
  on the source; ASN's `IBMSNAP_REGISTER`/`IBMSNAP_CAPMON` synch point >= it.
* **Verify in the server.** `HASH(expr, 2)` = SHA-256 VARBINARY (11.1+; the
  default algorithm 0 is MD5); `HASH8` (Jenkins, BIGINT) is **endian-dependent**
  - never compare it across platforms; `HASH4` Adler has poor coverage under a
  few hundred bytes. Row digest: `SUM(BIGINT(... first 7 bytes of
  HASH(row_text, 2) ...))` in DECIMAL(31) - same shape as SQL Server's.
* **Users.** Db2 authenticates outside the database (OS/LDAP): no password
  hashes to carry. Carry `SYSCAT.DBAUTH`, `TABAUTH`, `ROLES`, `ROLEAUTH`,
  `ROUTINEAUTH`, `SCHEMAAUTH`, `SEQUENCEAUTH` as GRANTs; say that logins are
  the operator's.
* **Sequences / identity.** `SYSCAT.SEQUENCES.NEXTCACHEFIRSTVALUE`;
  `ALTER TABLE t ALTER COLUMN c RESTART WITH n`, `ALTER SEQUENCE s RESTART
  WITH n`.
* **Statistics / params / snapshot.** `ADMIN_CMD('RUNSTATS ON TABLE s.t
  WITH DISTRIBUTION AND DETAILED INDEXES ALL')`; settings from
  `SYSIBMADM.DBCFG` and `DBMCFG`; snapshot = `ADMIN_CMD('BACKUP DATABASE ...
  ONLINE')` (needs archive logging) - no restore points.

## 5. SAP ASE and Informix (brief)

* **SAP ASE.** Bulk: bcp (Sybase client) or FreeTDS `freebcp`, which speaks
  TDS 5.0; pyodbc over FreeTDS for rows (migkit today). Verify in the server:
  ASE 16.0 `hashbytes()` offers only `md5`, `sha`/`sha1` (160-bit) and `ptn`;
  SHA-256 is an open enhancement (SAP KBA 3108409); `hash()` is
  **byte-order dependent**, use `hashbytes`. A 128/160-bit per-row hash
  summed over 7 bytes is still fine for difference detection (not for
  security). Follow: the log is read only by SAP Replication Server's
  RepAgent (licensed). Licence-free: a `timestamp` column (ASE's, updated on
  every insert/update, compared to `@@dbts`) for changed rows, key-set diff
  for deletes. Users: `syslogins` holds password hashes readable by `sso_role`;
  carrying them is ASE-to-ASE only.
* **Informix.** The server ships a free **CDC API** (`syscdcv1` database,
  created by `$INFORMIXDIR/etc/syscdcv1.sql`; `cdc_opensess`,
  `cdc_set_fullrowlogging`, `cdc_startcapture`, read as a smart large object);
  Debezium's Informix connector uses it through IBM's Java Change Streams
  client and describes itself as stable enough for production. No Python
  client for the stream exists; Informix is not a migkit engine and should
  stay a named gap until someone asks.

## 6. Plan

### 6.1 Images on this machine (arm64, colima vz, 16 GB host)

| engine | image to pin | runs how | published start / memory | VM needed |
|---|---|---|---|---|
| Oracle | `gvenzl/oracle-free:23.26.x-slim-faststart` for most tests; `23.26.x-faststart` (regular) where RMAN / Data Pump are tested | native arm64 | no seconds published (faststart = pre-expanded DB); 2 GB licence cap; measured here 2026-09-25: 2.5 GB used of 3.8 | raise the default profile to 6 GB (owner's change); prune images first - the last attempt failed on a full disk |
| Oracle, both sides | one container, source in `FREEPDB1`, target in a second PDB (`ORACLE_DATABASE=...` creates one) - halves memory; limit is `MAX_PDBS` (to measure) | native | as above | as above |
| SQL Server engine | `mcr.microsoft.com/azure-sql-edge` pinned by **digest** (frozen, retired) | native arm64 | "seconds"; ~450 MB footprint (docs) | 4 GB is enough |
| SQL Server 2022 full | `mcr.microsoft.com/mssql/server:2022-CU<known-good>-ubuntu-22.04`, `--platform linux/amd64` | **Rosetta** in a second colima profile (`colima start x86 --vm-type vz --vz-rosetta --cpu 4 --memory 6`); never QEMU | third-party estimate 60-90 s under emulation; >= 2 GB or it SIGABRTs | separate 6 GB profile (owner's change) |
| Db2 12.1 | `icr.io/db2_community/db2` pinned, `--platform linux/amd64 --privileged` | Rosetta profile (IBM's own macOS route) - or x86 CI | IBM: first start 15-20 min on macOS; keep a pre-created DB volume | same Rosetta profile |
| SAP ASE 16 | `datagrip/sybase:16.0` | Rosetta profile or x86 CI | ~30 s, flaky start; 3 GB image | same |
| Informix | none | - | - | - |

The durable home for everything x86 is an **x86 CI runner**: migkit is a
public repository, so GitHub's hosted `ubuntu-latest` runners (x86-64) cost
nothing and run SQL Server 2022, Db2 Community and `datagrip/sybase` natively;
disk on a hosted runner is small, so pull one engine per job. Rosetta is the
local stop-gap only (Apple: through macOS 27; macOS 26 already broke SQL
Server 2025 RC1 once; mssql-docker #929 shows a 2022 CU that restart-looped).

### 6.2 Cells migkit can close now, on this machine

| # | cell / fix | engine | proved on | effort |
|---|---|---|---|---|
| 1 | CT read inside a SNAPSHOT transaction when allowed, else re-validate `min_valid_version` after reading; `recovery_fork_guid` in the stream identity | SQL Server | Edge | S |
| 2 | row hash leaves out `rowversion`, names hidden columns explicitly | SQL Server | Edge (CLR casts: see 6.3) | S |
| 3 | **bulk-move (0e)**: `mssql-python` `bulkcopy`/`bulkcopy_arrow` with `keep_identity`, `keep_nulls`, `table_lock` on empty tables, constraints re-checked after; measure pymssql `bulk_copy` on identity tables before allowing it | SQL Server | Edge | M |
| 4 | CDC reader as the second follow rung (`__$start_lsn`/`__$seqval` token, Msg 313 read as "position lost") - try on Edge first | SQL Server | Edge, to measure | M |
| 5 | temporal tables: period columns and history carried, or the difference named | SQL Server | Edge | M |
| 6 | Oracle **sequences** (identity + `ALL_SEQUENCES`, `RESTART START WITH`) and **params** (`V$PARAMETER` non-default + NLS/character set) | Oracle | gvenzl slim | S + S |
| 7 | Oracle **users** (`DBMS_METADATA.GET_DDL` with hashed password, granted DDL) | Oracle | gvenzl slim | M |
| 8 | Oracle **bulk-move**: `AS OF SCN` keyed-range reads + `direct_path_load` (empty table, per-batch commit) / `executemany` + `MERGE` otherwise; Arrow reads to Arrow targets | Oracle | gvenzl slim | M |
| 9 | Oracle server-side digest (`STANDARD_HASH` SHA-256, 56 bits summed) for Oracle-Oracle, LOBs via `DBMS_CRYPTO` or in-process | Oracle | gvenzl slim | S |
| 10 | Oracle **stream / fence / confirm / delta** through a LogMiner reader (ARCHIVELOG init script, online catalog, committed-only vs buffered chosen from the largest open transaction, SCN+XID token, DBID+RESETLOGS identity) | Oracle | gvenzl slim + init script | L |
| 11 | Oracle **snapshot**: guaranteed restore point if Free allows it (to measure), else recorded SCN + `FLASHBACK TABLE ... TO SCN` | Oracle | gvenzl regular | M |
| 12 | Oracle LOBs in pieces (R10): locators read in `getchunksize()` multiples | Oracle | gvenzl slim | M |
| 13 | Oracle TIMESTAMP WITH TIME ZONE carried instead of left out (UTC + region as text) | Oracle | gvenzl slim | S |
| 14 | Oracle to Oracle whole schema by `DBMS_DATAPUMP` over a database link (target pulls) | Oracle | gvenzl regular, two PDBs | M |
| 15 | fix backlog R12's line "colima does not use Rosetta" (it does, per profile) | docs | - | S |

### 6.3 Cells that need an x86 runner (or the Rosetta profile)

| cell | engine | why not arm64 | effort |
|---|---|---|---|
| CLR/spatial/hierarchyid columns in the row hash (Msg 13604 fix) | SQL Server | Edge lacks spatial, hierarchyid, CLR | S (code) + CI |
| backup-chain bulk + follow (`BACKUP LOG` / `RESTORE ... NORECOVERY`) against 2022/2025 | SQL Server | Edge may do it (to measure); version rules need the real server | L |
| Always Encrypted: detect and refuse, or carry ciphertext via the user option | SQL Server | needs CMK/CEK made by client tools; neither Python driver exposes `AllowEncryptedValueModifications` | M |
| Agent-dependent behaviour, full CDC on 2022, `test_mssql_smart_checks.py` | SQL Server | x86 only | S (CI wiring) |
| Db2 sequences, params, users (grants only; logins are OS/LDAP), statistics | Db2 | no arm64 server | S each |
| Db2 bulk-move: `ADMIN_CMD LOAD FROM (DATABASE src SELECT ...) OF CURSOR ... NONRECOVERABLE` + `SET INTEGRITY` | Db2 | x86 | M |
| Db2 snapshot: `ADMIN_CMD('BACKUP DATABASE ... ONLINE')` | Db2 | x86 | M |
| Db2 stream/fence/confirm/delta: ASN Capture CD tables where the operator's licence covers it, else `ROW CHANGE TIMESTAMP` polling + key-set diff for deletes; fence on `MON_GET_TRANSACTION_LOG` `CURRENT_LSN` | Db2 | x86 + licence question | L |
| Db2 server-side digest `HASH(row, 2)` (never HASH8 across platforms) | Db2 | x86 | S |
| DVT as the second reader for Oracle and Db2 (backlog R8) | Oracle can move now; Db2 x86 | - | M |
| ASE: deep (states of tables/indexes), params (`sp_configure`), statistics (`update index statistics`), sequences (identity), users (`syslogins` hashes, ASE-to-ASE), bulk (`freebcp`), snapshot (`dump database`), follow (`timestamp` column vs `@@dbts` + key diff) | ASE | x86, no SAP image | S x5, M x3, L follow |
| OpenLogReplicator wrapped as a mover | Oracle | not listed for arm64; needs redo files on the host; AGPL (call, never link) | M, low priority |

### 6.4 Order

1. SQL Server S fixes (1, 2) - correctness, today, on Edge.
2. SQL Server bulk-move (3) - closes the only SQL Server gap in
   `capabilities.py`.
3. Oracle on gvenzl after the owner raises the VM to 6 GB: 6, 9, 13, 7, 8,
   then 10 (the largest, and the one that unblocks four cells), 11, 12, 14.
4. An x86 CI workflow (owner's decision to add): SQL Server 2022, Db2, ASE;
   then the 6.3 cells engine by engine.

## Sources

* gvenzl/oci-oracle-free README and ImageDetails: https://github.com/gvenzl/oci-oracle-free ; tags and sizes: https://hub.docker.com/r/gvenzl/oracle-free/tags
* Oracle 23.5 Free on ARM: https://www.geraldonit.com/oracle-database-free-for-arm-and-multi-platform-images-now-available/ ; Oracle Free Lite: https://blogs.oracle.com/database/announcing-oracle-database-23ai-free-container-images-for-armbased-apple-macbook-computers
* Oracle Free limits: https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html
* Testcontainers timeouts: https://github.com/testcontainers/testcontainers-java/issues/8590 , /9057
* Debezium Oracle LogMiner test on Oracle Free: https://github.com/apache/pulsar-connectors/pull/62
* Debezium Oracle connector: https://debezium.io/documentation/reference/stable/connectors/oracle.html ; 3.0 default strategy: https://debezium.io/blog/2024/10/02/debezium-3-0-final-released/ ; 3.6 mining window: https://debezium.io/blog/2026/07/06/oracle-logminer-no-more-tuning/
* OpenLogReplicator: https://github.com/bersler/OpenLogReplicator ; installation: https://github.com/bersler/OpenLogReplicator/blob/master/documentation/installation/installation.adoc
* python-oracledb release notes: https://python-oracledb.readthedocs.io/en/latest/release_notes.html ; batch/direct path: https://python-oracledb.readthedocs.io/en/latest/user_guide/batch_statement.html ; types: https://python-oracledb.readthedocs.io/en/latest/user_guide/sql_execution.html
* Oracle CHECKSUM behaviour: https://renenyffenegger.ch/notes/development/databases/Oracle/SQL/select/aggregate/checksum , https://www.dbaglobe.com/2022/03/checksum-function-in-oracle-21c-be.html ; ORA_HASH: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ORA_HASH.html ; DBMS_COMPARISON: https://oracle-base.com/articles/11g/dbms_comparison-identify-row-differences-between-objects
* Azure SQL Edge features and retirement: https://learn.microsoft.com/en-us/previous-versions/azure/azure-sql-edge/features ; track data changes on Edge: https://learn.microsoft.com/bs-latn-ba/previous-versions/azure/azure-sql-edge/track-data-changes
* SQL Server on Apple Silicon: https://www.nocentino.com/posts/2023-01-02-running-sql-server-apple-silicon/ ; Tahoe: https://bornsql.ca/blog/macos-tahoe-breaks-sql-server-on-docker-containers-on-apple-silicon/ ; 2025 AVX: https://www.nocentino.com/posts/2025-11-26-sql-server-2025-docker-desktop-avx-issue/ ; CU restart loop: https://github.com/microsoft/mssql-docker/issues/929 ; colima SIGABRT: https://github.com/abiosoft/colima/issues/1138
* mssql-python bulk copy: https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/bulk-copy ; 1.13.0 Arrow bulk copy: https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-13-0-arrow-bulk-copy-smarter-tokens-slimmer-wheels/4544858 ; wheels: https://pypi.org/project/mssql-python/
* pymssql bulk_copy: https://pymssql.readthedocs.io/en/stable/ref/pymssql.html
* Bulk insert benchmarks: https://pypi.org/project/arrowsqlbcpy/
* Change Tracking protocol: https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-tracking-sql-server
* CDC functions: https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql
* FOR JSON type conversion: https://learn.microsoft.com/en-us/sql/relational-databases/json/how-for-json-converts-sql-server-data-types-to-json-data-types-sql-server ; BINARY_CHECKSUM: https://learn.microsoft.com/en-us/sql/t-sql/functions/binary-checksum-transact-sql
* Always Encrypted bulk load: https://learn.microsoft.com/en-us/sql/relational-databases/security/encryption/migrate-sensitive-data-protected-by-always-encrypted
* Db2 on Apple Silicon / Docker: https://www.ibm.com/docs/en/db2/12.1.x?topic=deployments-db2-community-edition-docker , https://ibm.github.io/db2developerextension-about/docs/tips-and-tricks/manual-db2-installation
* ibm_db on macOS arm64: https://github.com/ibmdb/python-ibmdb/blob/master/INSTALL.md
* Db2 HASH: https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.sql.ref.doc/doc/r0061967.html ; ADMIN_CMD LOAD: https://www.ibm.com/docs/en/db2/12.1.x?topic=commands-load-using-admin-cmd
* Db2 replication entitlement (11.5): https://www.ibm.com/support/pages/ibm-db2-11-changes-data-replication ; Debezium Db2: https://debezium.io/documentation/reference/stable/connectors/db2.html
* SAP ASE hashbytes: https://infocenter.sybase.com/help/topic/com.sybase.infocenter.dc36271.1600/doc/html/san1393050455569.html ; datagrip/sybase: https://github.com/DataGrip/docker-env/blob/master/sybase/16.0/README.md
* Informix images: https://github.com/informix/informix-dockerhub-readme ; Debezium Informix: https://debezium.io/documentation/reference/stable/connectors/informix.html
