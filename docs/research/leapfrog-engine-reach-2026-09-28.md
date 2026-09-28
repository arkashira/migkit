# Engine reach: what the paid tools read and write that migkit does not, and the open path to each (research as of 2026-09-28)

Status: IN PROGRESS. Done: 0 (code), 1 (mainframe/IBM i), 2 (warehouses).
Next: 3 (SAP), 4 (other engines), 5 (SaaS licences), 6 (ranked table).
This block is rewritten as each lands.

Scope: backlog W5 (sources the paid tools read), item 33 (targets that are
not databases), F5 (Oracle/SQL Server/Db2 engine facts). Not repeated here:
Db2 LUW, SAP ASE and Informix basics (`oracle-mssql-db2-2026-09-28.md`
sections 4-5), Snowflake stages/Snowpipe/Snowpipe Streaming, BigQuery load
jobs and Storage Write API, Redshift COPY (`mechanisms-aws-azure-google-
snowflake-2026-09-28.md` sections 9, 15, 16), Airbyte/Singer/dlt platform
licences and the graph/vector short notes (`dynamodb-lakes-pipelines-
2026-09-27.md` sections 17-19 and "Graph and vector"). Nothing here was run;
no database was touched.

## 0. What migkit reaches today (read from the code, 2026-09-28)

`migkit/engines/__init__.py` `NAMES`: postgres, mysql, mongodb, mssql,
redis, kafka, sqlite, parquet, clickhouse, dynamodb, oracle, db2, ase,
opensearch, cassandra, redshift, snowflake, bigquery, kinesis, pubsub,
duckdb, hetero, generic. Aliases: mariadb/percona/aurora-mysql/tdsql ->
mysql; aurora-postgres/alloydb -> postgres; documentdb/cosmosdb-mongo ->
mongodb; azure-sql -> mssql; elasticsearch -> opensearch; scylladb ->
cassandra; sybase/sap-ase -> ase.

* **Relational commercial engines** (`oracle.py` 217 lines, `db2.py` 122,
  `ase.py` 125) sit on `dbapi.py` `DbapiRows`: keyed reads, digest folded
  in-process (`canon.fold_rows`), writes as delete-by-key + `executemany`.
  `capabilities.py` GAPS: Db2 and ASE lack sequences, params, bulk-move,
  stream, fence, confirm, delta, users, statistics, snapshot (item 34).
  Db2 is LUW only: nothing names z/OS or IBM i.
* **Warehouses** (`warehouse.py` 295 lines): Redshift, Snowflake, BigQuery
  as a side of a pair through DB-API; BigQuery via a load job after a key
  delete. Every cell but schema/counts/data/table-copy is `NOT_YET "33"`.
  No staged Parquet, no COPY INTO, no Storage Write API, no in-server
  digest. Written without an account; untested against a real warehouse.
* **generic** (`generic.py`): anything `reladiff` speaks (Snowflake,
  BigQuery, Redshift, ClickHouse, Oracle, Trino, Presto, DuckDB, Vertica
  per its docstring) for schema/counts/data only, with a URL and a table
  list; no move, no follow.
* **second reader** (`second_reader.py`): DVT in its own venv; today wired
  for postgres/mysql/mssql only (`READER_TYPES`); DVT itself reaches
  Teradata, Db2, Oracle, Snowflake, BigQuery, Spanner, Hive, Impala.
* **Not present at all:** Teradata, Netezza, Vertica (as a move side),
  Greenplum (as its own engine; would ride postgres), Exasol, SAP HANA,
  Databricks/Delta, Iceberg tables (pyiceberg noted in lakes research only),
  Synapse/Fabric, Db2 z/OS, IBM i, IMS, VSAM/copybook files, SAP
  application tables, Informix, Sybase IQ, Progress OpenEdge, Firebird,
  Couchbase, Cosmos DB NoSQL API, Firestore, Bigtable/HBase, Neo4j,
  InfluxDB, vector databases; SaaS applications.

## 1. Mainframe and IBM midrange

### 1.1 What the paid tools do (mechanism)

* **Db2 for z/OS, Qlik Replicate.** A load module plus a user-defined
  table function (UDTF) installed on z/OS (the "R4Z" product), run in a
  WLM-managed address space from an APF-authorised library. The
  replication task issues `SELECT ... FROM TABLE(<schema>.R4Z_UDTF__...)`;
  the UDTF opens an IFI session and issues **READS for IFCID 0306** (the
  documented Db2 interface for complete log records: start RBA/LRSN,
  filter to data-capture records, ask Db2 to decompress), and returns
  log records as a LOB result set; Replicate parses them off the host.
  Needs `DATA CAPTURE CHANGES` per table and `MONITOR2` for the user;
  32 sessions per LPAR by default (`MAXSESSIONS`); compressed table
  spaces cost CPU and need the dictionary kept (`KEEPDICTIONARY`);
  catalog tables are not captured because their log format is not
  documented. IBM IIDR and Precisely use the same IFCID 306 interface
  from their own started tasks.
* **VSAM, Precisely Connect CDC SQData / IBM Classic CDC.** CICS TS
  `LOGREPLICATE` writes before/after images of committed CICS VSAM
  updates to a z/OS System Logger **log stream**; batch VSAM updates
  need IBM **CICS VR** to log them; the capture agent reads the log
  stream, a publisher forwards units of work over TCP to "engines" that
  map records through the copybook and apply anywhere. With no logging,
  "Keyed File Compare" diffs the file against a backup.
* **IMS.** The DBD gets `EXIT=(*,LOG,KEY,DATA,CASCADE)`, which makes IMS
  write **type X'99' data-capture log records**; a log-reader agent
  (Precisely SQDIMSC, Informatica's ECCR on closed archive logs) extracts
  committed changes. Segments are flattened to rows by a scripted apply.
* **IBM i (Db2 for i), Qlik / Precisely MIMIX / IBM.** Read the **journal**
  (`QjoRetrieveJournalEntries` / `RCVJRNE`): journal code `R`, entry
  types `PT`/`PX` insert, `UB`/`UP` before/after update, `DL` delete, plus
  rollback images `BR`/`UR`/`DR` and commit-cycle entries (code `C`).
  Before-images need `IMAGES(*BOTH)`. A receiver must not be deleted
  before the reader has passed it.

### 1.2 Open libraries and programs

| need | open path | licence | notes |
|---|---|---|---|
| Db2 z/OS and IBM i over DRDA | `ibm_db` (already migkit's Db2 driver) | Apache-2.0 driver, but **a Db2 Connect licence is required** to reach z/OS or IBM i: a `db2consv_zs.lic` in `clidriver/license` or `db2connectactivate` on the subsystem; without it `SQL1598N` | the licence is the operator's; migkit can detect SQL1598N and say so |
| IBM i without Db2 Connect | **JTOpen / jt400** (IBM Toolbox for Java, `net.sf.jt400:jt400`) | **IPL-1.0** (OSI, weak copyleft; redistribution allowed) | host-server protocol, not DRDA, so no Db2 Connect; JVM - a sidecar or JPype (Apache-2.0) |
| IBM i from Python, no JVM on the client | **Mapepire** (`mapepire-python`, server component on IBM i) | Apache-2.0 (client, alpha, Python >= 3.10) | websocket to a server job IBM ships as open source; the operator installs the server on IBM i |
| IBM i over ODBC | IBM i Access ODBC driver | IBM IPLA, free download; **native macOS arm64** via IBM's Homebrew tap (unixODBC, not iODBC); Linux x86_64/ppc64le only (arm64 Linux request "not under consideration") | no 5770-XW1 key needed |
| IBM i change follow | `QSYS2.DISPLAY_JOURNAL` table function (plain SQL); Debezium **IBM i connector** (`io.debezium:debezium-connector-ibmi`, since 2.6, uses jt400 to decode entries) | IBM i built-in; Debezium Apache-2.0 | `DISPLAY_JOURNAL` returns `ENTRY_DATA` as a record image to be cut by column offsets (`INTERPRET`); IBM warns against sub-second polling; companion exit program (jhc-systems) blocks receiver deletion until read |
| Db2 z/OS change follow | Debezium Db2 connector with `db2.platform: ZOS` (**incubating**, since 2.7) over ASN SQL Replication change tables | Apache-2.0; **ASN use needs an IIDR licence** (per Debezium/Red Hat) | no open IFCID 306 reader exists; a UDTF on z/OS is licensed vendor code |
| IMS | IMS Universal JDBC driver (`imsudb.jar`) | IBM, no-charge FMID for IMS licensees, **not redistributable** | needs IMS Common Service Layer (ODBM/OM/SCI); no open log reader |
| copybook + EBCDIC files | **Cobrix** (`za.co.absa.cobrix`, parser independent of Spark) | Apache-2.0 | the mature one: REDEFINES by segment map, OCCURS DEPENDING ON (`variable_size_occurs`), RDW/BDW V/VB, multisegment hierarchies (IMS unloads), writer F/V/VB |
| same, pure Python | **Stingray Reader** 5.1.1 (Oct 2024) | MIT | documents ODO (offsets recomputed per row), COMP-3, V/VB with BDW/RDW; **Python >= 3.12** (migkit is >= 3.10) |
| same, pure Python, simpler | `aws-samples/mainframe-data-utilities` (Apache-2.0); `cobolio` (no ODO); `coboljsonifier` | Apache-2.0 / various | MDU: REDEFINES only for groups, ODO on its to-do list |
| copybook, Java | JRecord, cb2xml | **LGPL** (2.0/2.1+/3 per listing) | usable as a separate program; `JRecordCodeGen` is GPL |
| avoid | `python-cobol` (GPLv3), `cobol-parser` 1.8.x on PyPI (**proprietary**) | | |
| EBCDIC code pages | Python built-in `cp037`, `cp273`, `cp500`, `cp1140`...; **`ebcdic`** 2.0.1 (Mar 2026) adds `cp1141`-`cp1149`, `cp1047`, `cp290`, `cp420`, `cp838` (Thai), `cp870`, `cp1097`... | BSD-2-Clause | **no DBCS** (cp930/cp939 Japanese, cp933 Korean, cp935/cp937 Chinese): needs ICU (`PyICU`, MIT-style ICU licence) or a table built from IBM's CDRA |
| getting the files | z/OSMF REST `GET /zosmf/restfiles/ds/<dsn>` with `X-IBM-Data-Type: record` (each record prefixed by a 4-byte big-endian data length, no conversion); Zowe Python SDK | IBM (z/OSMF); **EPL-2.0** (Zowe) | `binary` mode drops V/VB record boundaries; FTP needs `SITE RDW` to keep them |

### 1.3 How to test it here or on free CI

* **Copybook/EBCDIC/packed decimal: fully testable on arm64 with no
  mainframe.** Fixtures are bytes. GnuCOBOL (compiler GPL, runtime LGPL;
  `brew install gnucobol`) can write COMP-3, zoned and ODO records as a
  test-time generator, never a dependency; Cobrix's own test data and the
  `csoai/cobol-copybook-decoding-corpus` dataset (41 copybooks, 166
  scored fields, 8 must-fail fields; dated today, unreviewed) are
  independent oracles. Run Cobrix on the JVM in CI as the second reader.
* **MVS 3.8j (public domain) on Hercules** (SDL Hyperion 4.9.1, **QPL-1.0**;
  builds on Apple Silicon; `praths/mvs-tk5` is an arm64 image of TK5):
  gives real IDCAMS and VSAM KSDS/ESDS and FTP of datasets, so a
  "download a VSAM file and read it with its copybook" test can run in
  CI. It has **no Db2, no CICS, no IMS, no z/OSMF** (the `mvsMF` project
  emulates the z/OSMF files API on MVS 3.8j, record mode read-only).
* **z/OS with Db2: no free self-hosted path.** ZD&T Personal Edition
  (~US$4.8k/year, being withdrawn for renewal per IBM community
  reports), Learners Edition gone, zPDT discontinued; Wazi as a Service
  is paid. **IBM Z Xplore** gives free shared z/OS with Db2 (port 5040)
  for learning; **IBM Z Trial** gives time-limited environments. Neither
  suits automated CI with stored credentials; use for a manual proof
  only, with nothing but synthetic data.
* **IBM i:** **PUB400.com** (IBM i 7.5, free, one profile, no commercial
  use, no backups) runs SQL, journals and `DISPLAY_JOURNAL`; manual
  proof only. Mapepire needs its server installed, which a PUB400 user
  cannot do. No IBM i emulator exists for x86/arm.

### 1.4 Design for migkit

* **`db2` gains a platform** (`platform: luw | zos | i`), read from
  `SYSIBM.SYSDUMMY1`/`GETVARIABLE('SYSIBM.PLATFORM')`-style probes at
  connect. Same `DbapiRows` read/write. **S.**
  * Connect: `ibm_db` for LUW and z/OS (SQL1598N turned into "this needs
    Db2 Connect: a licence file or server activation, the operator's");
    for IBM i, ODBC (native on macOS arm64) or Mapepire when `ibm_db`
    has no licence. **S** for the error, **M** for the IBM i driver.
  * In-server digest: z/OS `HASH(expr, 2)` (FL 506+, SHA-256; default 0
    is MD5 - always pass 2); IBM i 7.4+ `HASH_SHA256`, or **`HASH_ROW`**
    (SHA-512 of the whole row, 7.4 TR2+) - the cheapest per-row digest
    of any engine here, summed over the first 7 bytes as for LUW. Row
    text must be rendered with explicit CCSIDs (EBCDIC `CHAR` hashes the
    EBCDIC bytes: compare text rendered to Unicode, never raw bytes
    across platforms). **S.**
  * Bulk out: keyed reads (z/OS: `FETCH FIRST n ROWS` + `OPTIMIZE FOR`,
    `WITH UR` only when the follow reconciles); DSNTIAUL/UNLOAD are
    JCL, not SQL: out of scope. Bulk in: multi-row insert via
    `executemany`; z/OS `LOAD` needs JCL - not a Python path. **S.**
  * Follow, IBM i: **migkit's own journal tail** over
    `DISPLAY_JOURNAL(STARTING_SEQUENCE => last+1, JOURNAL_CODES => 'R C',
    JOURNAL_ENTRY_TYPES => 'PT PX UB UP DL BR UR DR CM RB')`, cut
    `ENTRY_DATA` by `QSYS2.SYSCOLUMNS` offsets (packed/zoned in-process,
    `ebcdic` for CCSIDs), commit-cycle grouping so a batch ends at a
    commit, position = journal sequence number (the fence), a
    check that the starting receiver still exists (`JOURNAL_RECEIVER_INFO`)
    before resuming. Needs `IMAGES(*BOTH)` for update keys - `assess`
    names tables journaled `*AFTER`. Debezium's IBM i connector as the
    wrapped rung. **M-L.**
  * Follow, z/OS: wrap Debezium's Db2 connector in ZOS mode where the
    operator holds IIDR, or poll by `ROW CHANGE TIMESTAMP` plus a key-set
    diff for deletes; say plainly that a log-level reader needs licensed
    code on z/OS. **M.**
  * Users/grants: z/OS authorities live in RACF/ACF2/Top Secret plus
    `SYSIBM.SYSTABAUTH` etc.; IBM i in object authorities
    (`QSYS2.OBJECT_PRIVILEGES`) - carry SQL grants, report the rest. **M.**
* **A `copybook` engine (files as a source; optionally a target).** A
  database is a directory or an S3/z/OSMF prefix; a table is a dataset
  plus its copybook (and a segment map for multi-layout files).
  * Decode: Stingray-style pure Python when the interpreter is >= 3.12,
    otherwise migkit's own decoder over a parsed copybook (COMP, COMP-3,
    zoned with overpunch signs, COMP-1/2 as IBM hex float not IEEE,
    `SIGN LEADING/TRAILING SEPARATE`, `BLANK WHEN ZERO`, ODO by counter
    field, REDEFINES chosen by a discriminator rule, `SYNC` alignment),
    Cobrix run out of process as the second reader. **M-L.**
  * Keys: a KSDS carries its key offset/length (from `LISTCAT` output or
    the operator); ESDS/flat files have none - read in one pass, compare
    by a digest of the whole record set plus counts, and refuse a keyed
    `repair`.
  * Verify: the source's own bytes are the truth: digest the raw record
    bytes *and* the decoded row, so a codepage or scale mistake shows as
    "decoded differently", not "data differs". Must-fail fields (invalid
    packed sign nibble, non-digit zoned) are reported per record, never
    turned into zero - the class of silent error these migrations are
    known for.
  * Write back (target): encode with the same copybook for the reverse
    leg or parallel runs; Cobrix already writes F/V/VB. **M.**
  * Follow: none from files. Where the site already runs CICS
    `LOGREPLICATE`, the log stream is reachable only through licensed
    agents; migkit's honest answer is repeated extracts and a keyed diff
    (the "Keyed File Compare" shape). **S** (diff of two extracts).
* **IMS:** out of scope as a live source; an IMS unload (HD unload or a
  user unload per segment) is a multisegment copybook file, handled by
  the engine above with the segment map. **S** once the copybook engine
  exists.

## 2. Warehouses and analytic engines, as sources and as targets

Snowflake stages/Snowpipe/Snowpipe Streaming, BigQuery load jobs and the
Storage Write API, and Redshift COPY are in
`mechanisms-aws-azure-google-snowflake-2026-09-28.md` sections 9, 15, 16;
only what that report lacks is here.

### 2.1 How the paid tools reach them

* **As sources, warehouses have no change log a tool can read.** Qlik's
  Teradata source is ODBC plus **"Context" columns** (a timestamp or
  counter per table, compared with the last value it stored; all changes
  assumed INSERT unless a key/unique index exists; before-images not kept;
  a context column in the key makes updates/deletes unsupported). Every
  other paid path to Teradata, Netezza, Vertica, Greenplum and Exasol is
  the same shape: bulk reads, then watermark polling. The one real
  change source is **Netezza's hidden columns**: `createxid`/`deletexid`
  per row version and a `rowid` kept across updates, readable until
  `GROOM` removes deleted versions (AWS SCT's extractor uses them:
  `createxid > last OR deletexid > last`, net change per `rowid`,
  `deletexid = 1` = rolled back during load).
* **As targets, they load through the engine's bulk protocol:** Qlik
  loads Teradata through **TPT** (Load operator = FastLoad protocol, empty
  table only; Stream operator for changes); Netezza through external
  tables; Vertica through `COPY`; Exasol through `IMPORT`; Snowflake
  through stage + `COPY INTO` or Snowpipe Streaming; BigQuery through the
  Storage Write API; Databricks through `COPY INTO` or Delta writes.
* **SAP HANA as a source:** SAP certifies **no** log-based reader (SAP
  Note 2971304). Qlik offers trigger-based CDC (three triggers per table
  into `attrep_cdc_changes`; now Qlik's recommended method; SAP upgrades
  drop foreign triggers, hence a May 2026 "continue CDC on dropped
  triggers" option) and log-based CDC (reads log segments/backups;
  deletes carry only key columns). Fivetran HVR reads HANA log segments
  and log backups directly with an agent on the HANA host (no
  supplemental logging exists on HANA, so it keys by row id); Fivetran's
  own SAP-ERP-on-HANA connector uses triggers and shadow tables, or a
  no-trigger snapshot-diff mode.

### 2.2 Engine by engine: open client, bulk, exactly-once, in-server digest, test bed

| engine | Python client (licence) | bulk out / in | exactly-once write primitive | in-server digest | test environment |
|---|---|---|---|---|---|
| **Teradata** | `teradatasql` - **proprietary Teradata licence** (free to use, accept-on-install, beta features "as is"; built from Go; PyPI lists macOS arm64 and Linux ARM64) | FastExport/FastLoad inside the driver: `{fn teradata_try_fastexport}` / `{fn teradata_require_fastload}` (early release; FastLoad only into an **empty** table, fields <= 64 KB, types fixed across batches; errors via `{fn teradata_get_errors}`). No TPT needed | none built in: load into an empty staging table by FastLoad, then `MERGE`/`INSERT SELECT` plus a batch-mark row in one transaction | **no built-in SHA-256** (DVT requires a UDF, `akuroda/teradata-udf-sha2`, and hit a VARBYTE/VARCHAR mismatch, DVT #1314); `HASHROW` is the 4-byte distribution hash - collisions common, for bucketing only | **ClearScape Analytics Experience** (free hosted; "education and testing only"; ~30 GB and time-limited per a 2023 post, now under "AI Studio"); **Vantage Express** VM (free; VirtualBox/VMware x86, **UTM on Apple Silicon**, 6 GB RAM, 30 GB disk; newest listed image 2022) |
| **Netezza / IBM PDA** | `nzpy` (IBM; README says Apache-2.0, setup.py says BSD/"IBM", libraries.io says IPL-1.0 - read the LICENSE file before depending); `nzpy_extended` fork (C extension, streaming load) | transient external tables `USING (REMOTESOURCE 'python')` both ways (NPS 11.1.2+) | none: stage + `INSERT SELECT` + mark in one transaction | **SQL Extensions Toolkit** (optional install): `hash(x, alg)` MD5/SHA-1/SHA-256 (binary), `hash8` Jenkins 64-bit, `hash4` Adler/CRC32 - toolkit absent means fold in-process | no container, no emulator any more; **NPS as a Service trial** (code `TRYNPS1`, USD 1,000 credit, ~1 month) |
| **Vertica (OpenText Analytics Database)** | `vertica-python` - Apache-2.0 | `cursor.copy("COPY t FROM STDIN ...", file)` streamed; `executemany` becomes COPY client-side; reads `AT EPOCH n` for a lock-free consistent snapshot (above the Ancient History Mark) | COPY is transactional: batch + mark row in one transaction | `SHA256()` built in (hex VARCHAR; matches standard SHA-256); `HASH()` is type-dependent, 63-bit, not for comparison | **CE container discontinued** (404 since Aug 2025; docs say "no longer available"); `opentext/vertica-k8s` usable without a licence per a 2026 forum post; amd64 only, and amd64 under emulation on M1 returned garbage on repeated JOINs - x86 CI only |
| **Greenplum** | psycopg (rides `postgres`) | `COPY`, `gpfdist` external tables for parallel segment load | PostgreSQL transaction + mark | PostgreSQL functions (`md5`, `sha256` in 11+ kernels) | Greenplum **closed-source since May 2024**; open forks **Apache Cloudberry** (incubating, Apache-2.0, 2.1.0 Apr 2026, `apache/incubator-cloudberry` image) and **WarehousePG** (EDB, Apache-2.0, binary-compatible with GP 6/7) |
| **Exasol** | `pyexasol` - MIT (2.3.x, Python 3.10-3.14) | HTTP transport: `export_to_*` / `import_from_iterable` / `import_from_file` run `EXPORT`/`IMPORT` in parallel CSV streams, zlib optional; server-side `IMPORT FROM JDBC/ORA` pulls from other databases with no row through the client | transaction + mark (IMPORT is transactional) | `HASH_SHA256` family (to verify on a server) | `exasol/docker-db` (x86-64 only, privileged, <= 10 GiB data; fails under Rosetta/QEMU on M1); Community Edition download (<= 200 GB) |
| **SAP HANA** | `hdbcli` - **SAP Developer Licence, no redistribution** (install from PyPI by the operator, never bundled or baked into an image) | `executemany` array insert; `IMPORT FROM` CSV server files; no client bulk API | transaction + mark | `HASH_SHA256(<varbinary>)` (to verify on a server) | `saplabs/hanaexpress` (HANA 2.0 SPS08, Jul 2025 build 2.00.088; amd64 only; 32 GB memory cap; free licence incl. production); **HANA Cloud free tier** (16 GB, 1 vCPU, 80 GB; stopped nightly, deleted after 30 days unstarted; BTP trial 90 days) |
| **Databricks / Delta** | `databricks-sql-connector` - Apache-2.0; `deltalake` (delta-rs) - Apache-2.0 | `COPY INTO` from cloud storage (idempotent: files already loaded are skipped even if changed; `force` disables); delta-rs writes Parquet + log directly | **Delta `txn` action**: `CommitProperties(app_transactions=[Transaction(app_id, version)])` in delta-rs, `txnAppId`/`txnVersion` in Spark; the log keeps the latest version per app id; delta-rs leaves the check to the caller (`DeltaTable.transaction_version(app_id)`) - check-then-write is racy with two writers on one app id | `sha2(x, 256)`, `xxhash64`, aggregate with `sum`/`bit_xor` | **Databricks Free Edition** (permanent, since Jun 2025; one 2X-Small serverless SQL warehouse; non-commercial); delta-rs on local disk needs no server at all |
| **Iceberg** (any engine) | `pyiceberg` - Apache-2.0 | `append`/`overwrite` Arrow tables | **snapshot summary properties** committed atomically with the data (`snapshot_properties={...}`, 0.7.0+): the Kafka Connect Iceberg sink keeps its offsets there; Polars tags each commit with a uuid and skips if a retained snapshot carries it; open pyiceberg bug #4022 (retry after an empty-table overwrite can delete the landed snapshot's manifests) | engine's own | local SQL catalog + file warehouse, no server |
| **Snowflake** | `snowflake-connector-python` (Apache-2.0); **`snowpipe-streaming`** 1.8.1 (Sep 2026; Rust core; Apache-2.0 classifier; wheels incl. macOS arm64 and Linux aarch64) | stage + `COPY INTO` (64-day load metadata); Snowpipe Streaming high-performance (GA Sep 2025, PIPE object auto-created `<TABLE>-STREAMING`) | **named channel + offset token**: `open_channel(name)` returns the last committed token; replay after it | `HASH_AGG(*)` (order-independent, counts duplicates, stable across scale changes) for same-engine; `SHA2(x,256)` for cross-engine | trial account only (no emulator); LocalStack's Snowflake emulator is commercial |
| **BigQuery** | `google-cloud-bigquery-storage` (Apache-2.0) | Storage Write API with **Arrow rows** (serialized schema in the first request, record batches after; request < 10 MB) | **COMMITTED stream + row offsets**: `ALREADY_EXISTS` = already written, skip; `OUT_OF_RANGE` = retry from last success; store stream name + offset with the source checkpoint | `BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))` same-engine (XOR cancels duplicate pairs: pair with `COUNT(*)`); `SHA256` cross-engine | sandbox (free, no card; 10 GB, 1 TB query/month; tables expire after 60 days; **no DML and no streaming**, so neither delete-then-load nor the Storage Write API can be tested there - a billed project with a spend cap is needed); **`goccy/bigquery-emulator`** v0.8.1 (MIT; multi-arch incl. arm64 since the pure-Go SQL backend; gRPC Storage Write API with COMMITTED/PENDING streams and Arrow; issue #342 "failed to find stream" from a Java client) for the offset logic in CI |
| **Redshift** | `redshift_connector` (Apache-2.0) / psycopg | `UNLOAD`/`COPY` via S3 | stage table + `MERGE` + mark in one transaction | `FNV_HASH(v, seed)` 64-bit, type-width-dependent (INT vs BIGINT differ), chained per column; no `BIT_XOR` aggregate - sum as `DECIMAL(38)`; `SHA2(x,256)` cross-engine | Redshift Serverless free trial credit; no emulator |
| **Synapse dedicated / Fabric Warehouse** | `mssql-python` / pyodbc | `COPY INTO` from ADLS/Blob | no load metadata: staging + mark in one transaction | `HASHBYTES('SHA2_256', ...)`; `CHECKSUM_AGG` is XOR-weak | Fabric trial capacity (time-limited) |

### 2.3 Design for migkit

* **One cross-engine digest, computed in the server where it can be.**
  migkit's canonical row text hashed with SHA-256, first 7 bytes summed
  as `DECIMAL(38)` (the shape SQL Server and the Db2 plan already use).
  Per engine only the hex-prefix-to-integer step differs: Snowflake
  `TO_NUMBER(SUBSTR(SHA2(t,256),1,14),'XXXXXXXXXXXXXX')`, BigQuery
  `CAST(CONCAT('0x',SUBSTR(TO_HEX(SHA256(t)),1,14)) AS INT64)`, Redshift
  `STRTOL(SUBSTRING(SHA2(t,256),1,14),16)`, Databricks
  `CONV(SUBSTR(SHA2(t,256),1,14),16,10)`, Vertica `SHA256`, Exasol/HANA
  `HASH_SHA256`, Db2 `HASH(t,2)`, Netezza `hash(t,2)` when the toolkit is
  present. The hard part is not the hash but rendering `t` identically
  in each dialect (decimal scale, timestamp precision, float text,
  NULL marker): an engine gets `SQL_DIALECT` (in-server) only after its
  rendering is proved against the in-process renderer on the same rows;
  until then it folds in-process as today. Engine-native aggregates
  (`HASH_AGG`, `BIT_XOR(FARM_FINGERPRINT)`, `FNV_HASH` sums) are
  same-engine only and serve `unchanged`/drift checks. **M** per dialect
  after the first; the first (Snowflake or BigQuery) **M-L**.
* **Exact loads keyed by migkit's batch number** (the R3 shape: the batch
  and its mark land together, and the target says which batch it last
  committed):
  * BigQuery: COMMITTED stream per (table, run), offset = rows before the
    batch; `ALREADY_EXISTS` counted as delivered; stream name + offset in
    the checkpoint. Replaces load-job-after-delete in `warehouse.py:250`
    and avoids the 1,500 load jobs/table/day quota. **M.**
  * Snowflake: bulk = Parquet staged (the existing `parquet` writer) +
    `COPY INTO` (64-day metadata makes a rerun a no-op); tail = one named
    channel per (table, lane), offset token = migkit's batch position.
    **M.**
  * Databricks/Delta and Iceberg: `Transaction(app_id=<run>, version=<batch>)`
    / snapshot property `migkit.batch=<n>`; read back before writing; one
    writer per app id (migkit's lease already guarantees it). This also
    gives `parquet` targets on S3 a transactional big brother. **M.**
  * Teradata/Netezza/Vertica/Exasol/HANA/Redshift/Synapse: stage, then
    `MERGE` + a row in a `migkit_batches` table in one transaction. That
    table is a footprint in the target; say so in `leftovers`. **S** each
    on the DB-API base once the stage path exists.
* **Change follow from warehouses** (rare in migrations, common in
  offloads): watermark by a declared column with Qlik's rules made
  checks - unique, monotonic, not in the key - plus a key-set diff for
  deletes; Netezza by `createxid`/`deletexid` with an `assess` warning
  that `GROOM` must not run past the reader's point; Vertica by epoch
  (`AT EPOCH` range reads give a consistent before/after). HANA:
  trigger-based only, and only with a full-use licence (section 3.2).
  **M.**
* **New engines on the DB-API base**, in value order: Teradata (FastLoad/
  FastExport through the driver's escape functions; `assess` flags
  multiset tables without a key, `PERIOD` and `NUMBER` types, and the
  empty-table rule), Vertica, Exasol, HANA, Netezza. Greenplum/Cloudberry/
  WarehousePG as aliases of `postgres` with `gpfdist` as a later rung and
  the distribution key carried (W7). **S-M** each for read/write/verify;
  test beds as the table says.
* **Users and grants:** Teradata `DBC.AllRightsV`/`RoleMembersV`; Vertica
  `v_catalog.grants`; Exasol `EXA_DBA_*_PRIVS`; HANA `SYS.GRANTED_PRIVILEGES`;
  Snowflake `SHOW GRANTS`; BigQuery IAM (project-level, outside SQL);
  Databricks Unity Catalog `SHOW GRANTS`. Carried as GRANT text between
  like engines, reported (not carried) across unlike ones. **M.**

## 3. SAP application data (ECC, S/4HANA, BW)

### 3.1 How the paid tools reach it

* **Qlik Replicate "SAP Application" / Fivetran HVR "SAP NetWeaver" /
  Informatica / Theobald.** Two layers: rows are captured at the
  **database** (log-based or trigger-based on the underlying Oracle,
  SQL Server, Db2, ASE or HANA), and the **application layer** is read
  through RFC for the Data Dictionary (DD02L/DD03L), so pool and
  cluster tables are unpacked (ECC's `BSEG` lives inside `RFBLG.VARDATA`,
  `KONV` in `KOCLU`, `CDPOS` in `CDCLS`) and so the tool knows which
  tables are logical. Fivetran reads the physical container and unpacks
  it "using SAP Data Dictionary metadata"; HVR ships an RFC-based
  dictionary reader. SAP's own paths: **SLT** (DB triggers into logging
  tables, licensed), **ODP** (extractors and CDS views through the
  operational delta queue, ODQ), **Datasphere replication flows** (CDC
  CDS views only).
* **Which of these a third party may use (the licence facts that decide
  the design):**
  * **ODP over RFC is forbidden to non-SAP applications** (SAP Note
    3255746: "not permitted" since Feb 2024; SAP's June 9, 2026 security
    patch blocks such calls technically; Note 3439624 is the
    self-assessment). **ODP over OData is the sanctioned third-party
    path** ("stable and recommended for all customer and third-party
    applications"), reported ~10x slower than RFC for bulk.
  * `RFC_READ_TABLE` is "not supported for customer use" (Note 382318).
  * **Direct database access** to an SAP system's database under a
    **runtime** licence (HANA runtime, Oracle ASFU through SAP, other
    DBs bought through SAP) is not permitted for extraction - only
    administration and monitoring (Note 581312 for Oracle); third-party
    log readers or triggers need a **full-use/Enterprise** DB licence.
    Audits can see non-SAP connections. So a DB-level CDC path is legal
    only where the customer bought the database licence directly.
  * **No certified HANA log reader exists** (Note 2971304).
* **Cluster/pool tables:** transparent on HANA (Suite on HANA and
  S/4HANA declustered them) and on NW 7.5 fresh installs; still packed
  in ECC on anydb upgraded from older releases. In S/4, `KONV` is
  replaced by `PRCD_ELEMENTS`, and much of FI is in `ACDOCA`.

### 3.2 Open libraries and what they need

| path | library | licence | what it needs from the customer |
|---|---|---|---|
| RFC | **PyRFC** | Apache-2.0 bindings, but **archived** (SAP-archive, read-only since May 28, 2026; last release 3.3.1 Jan 2024, **yanked on PyPI**; built against an SDK patch level SAP no longer supports) | the **NW RFC SDK** - proprietary, downloadable only with an S-user (SAP customer/partner), not redistributable |
| RFC without the SDK | `open-rfc` (Node.js, Aug 2026, implements the protocol from SAP's published documentation) | open (young) | nothing from SAP, but no Python port; RFC-based ODP remains forbidden whatever the client |
| OData v2 | **`pyodata`** (SAP/python-pyodata, 1.12.0) | **Apache-2.0** | a Gateway service; ODP delta headers are not built in (`Prefer: odata.track-changes` on the session, `__delta` / `!deltatoken` parsed by the caller) |
| ODP over OData | none open; `erpl-web` DuckDB extension does it (subscriptions, deltas, audit) | **BSL-1.1** (DataZoo; MPL-2.0 after the change date; production use allowed except hosted/embedded offering to third parties; telemetry on by default) | an ODP OData service generated per provider (SEGW, "ODP-based extraction via OData"); delta subscription is per SAP user |
| RFC into DuckDB | `erpl` (RFC, ODP, BICS, IDoc) | **BSL-1.1** | the NW RFC SDK; ODP-RFC is forbidden anyway |
| CDS extraction views (S/4 1909+) | via ODP OData | - | views with `@Analytics.dataExtraction.enabled` and `delta.changeDataCapture` (trigger-based CDC framework, keys of every joined table exposed); C1-released views for stability; CDS on AMDP not supported; test with `RODPS_REPL_TEST`, watch `ODQMON` |

### 3.3 Test environment

* **ABAP Cloud Developer Trial** (`sapse/abap-cloud-developer-trial`,
  2022/2023/2025 images; amd64 only, SAP reports it running on an M2 Pro
  with 32 GB under Docker Desktop; 16 GB minimum, 32 GB recommended,
  ~23 GB compressed / 53 GB unpacked, hostname must be `vhcala4hci`;
  licence 3 months, renewable free via minisap for system `A4H`; Docker
  Hub login needed). Has Gateway (SEGW, `/IWFND/GW_CLIENT`) and EPM demo
  data (`SEPM_REF_APPS_DG`); **whether ODP-over-OData works on it is
  unconfirmed** - prove with `RODPS_REPL_TEST` first. This is the only
  free SAP application server; too large for free CI runners (7 GB RAM).
* **SAP HANA express / HANA Cloud free tier** (section 2.2) for HANA as
  a database.
* **SAP ES5 public Gateway demo** gives OData v2 services for client
  code, not ODP.

### 3.4 What is feasible without SAP licences, and the migkit design

* **Feasible, legal, open:** reading **ODP via OData** with `pyodata`
  plus migkit's own delta-token handling, from a customer system that
  exposes the services. That is the path Microsoft, Matillion and
  AWS AppFlow moved to after Note 3255746.
  * An **`sap-odp` source engine**: a database is a Gateway service
    root; a table is an ODP entity set (`FactsOf...`/`AttrOf...`).
    Initial load with `Prefer: odata.track-changes`, paging by
    `__next`/`odata.maxpagesize`; the delta token from the last page's
    `__delta`, **or from `DeltaLinksOf<EntitySet>` when the last page
    omits it** (a real service shape, per erpl-web #250); the token is
    the position (fence = "the delta link issued after the cutover
    freeze"); a forced full reload drops the subscription explicitly.
    **M.**
  * Rows arrive with `ODQ_CHANGEMODE` / `ODQ_ENTITYCNTR` (C/U/D and
    counter) on delta pages; migkit applies them through the hetero
    tail. **S** on top.
  * Verify: ODP gives no server-side digest. Compare the target with a
    second full extraction (a fresh subscription) through the in-process
    digest, and record counts per page against ODQ's own counts. **S.**
  * Users/grants: SAP authorisations are application-level (PFCG
    roles); not carried; `assess` says so.
* **Feasible only with a full-use database licence the customer owns:**
  migkit's existing engine for the underlying database (Oracle, SQL
  Server, Db2, ASE, HANA) with `assess` asking for the licence type
  before any direct read, plus a **dictionary pass** that maps logical
  tables to physical ones (`DD02L.TABCLASS in ('POOL','CLUSTER')`,
  `SQLTAB`) and **refuses** a packed container instead of copying
  unreadable `VARDATA`. Decoding cluster tables is not planned (SAP's
  format, and every vendor that does it reads the dictionary through
  RFC). **M.**
* **Not feasible openly:** ODP over RFC (forbidden), PyRFC (archived,
  needs a licensed SDK), SLT (licensed), HANA log reading (uncertified).
* **The one mandatory safety feature:** `assess` on an SAP-looking
  database (schemas `SAPSR3`/`SAPHANADB`/`SAP<SID>`, table `T000`)
  prints the runtime-licence warning and stops unless the operator
  declares a full-use licence. **S.**

