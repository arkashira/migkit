# Engine reach: what the paid tools read and write that migkit does not, and the open path to each (research as of 2026-09-28)

Status: COMPLETE for sections 0-6. Public sources only; nothing run, no
database touched. Marked "to verify" where only one secondary source or
none was found: Exasol and HANA `HASH_SHA256` spelling, Firebird
`CRYPT_HASH` and its arm64 image, Netezza `hash()` algorithm codes, the
`nzpy` licence (three sources disagree), the Salesforce Pub/Sub proto
repo licence, `influxdb3-python` licence (one listing), whether
ODP-over-OData works on the ABAP trial image, whether the Cosmos vNext
emulator serves all-versions-and-deletes, ClearScape's current limits.
The ranking in section 6 is a judgement, argued in its preamble.

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

* **`db2` gains a platform** (`platform: luw | zos | i`), declared by the
  endpoint and confirmed at connect from the server's product id (the
  DRDA/CLI server-info the driver reports; exact probe to fix against a
  server). Same `DbapiRows` read/write. **S.**
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
| **Snowflake** | `snowflake-connector-python` (Apache-2.0); **`snowpipe-streaming`** 1.8.1 (Sep 2026; Rust core; Apache-2.0 classifier; wheels incl. macOS arm64 and Linux aarch64) | stage + `COPY INTO` (64-day load metadata); Snowpipe Streaming high-performance (GA Sep 2025, PIPE object auto-created `<TABLE>-STREAMING`) | **named channel + offset token**: `open_channel(name)` returns the last committed token; replay after it | `HASH_AGG(*)` (order-independent, counts duplicates, stable across scale changes) for same-engine; `SHA2(x,256)` for cross-engine | trial account only (no open emulator found) |
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

## 4. Other operational engines (short, ranked by how often a migration asks for them)

Ranking is a judgement from what the paid tools list first and what the
clouds push migrations toward; "rides" means an existing migkit engine
carries it with an alias plus a capability overlay.

| # | engine | reach path for migkit | open client (licence) | change follow | same-engine digest | test bed (arm64 / free CI) | effort |
|---|---|---|---|---|---|---|---|
| 1 | **Azure Cosmos DB for NoSQL** (Mongo and Cassandra APIs already ride `mongodb`/`cassandra`) | new engine: parallel reads per feed range; writes by transactional batch per partition key (100 ops), 429 + retry-after honoured as migkit's own backoff | `azure-cosmos` (MIT) | change feed pull model with a continuation token per feed range. **Latest-version mode has no deletes**; **all-versions-and-deletes** (GA Jun 2026, Python >= 4.9.1b1) needs continuous backup on the account and **cannot start from the beginning** - so: take the token *before* the bulk read, and refuse a follow without AVAD unless the app soft-deletes | none cheap (every query costs RUs): in-process digest, system properties (`_rid _self _etag _attachments _ts`) stripped | **vNext Linux emulator** GA Jun 2026, **x64 and ARM64**, NoSQL gateway mode, change feed supported; AVAD in the emulator unknown - test on a real account | **M** |
| 2 | **TiDB** | target rides `mysql` today; as a source, **binlog is not there** - read TiCDC | PyMySQL etc.; TiCDC (Apache-2.0) | **TiCDC** to Kafka in Debezium/Canal/Avro protocol, consumed by migkit's `kafka` reader into the hetero tail; or TiCDC's storage sink | `ADMIN CHECKSUM TABLE` (CRC64-XOR over KV, what Lightning uses) | `tiup playground` / `pingcap/*` images, arm64 | **S** (alias + TiCDC wiring) |
| 3 | **YugabyteDB** | rides `postgres` | psycopg; YB's Debezium fork | **PG replication protocol with `pgoutput`/`yboutput` slots** since 2024.1.1 (labelled EA in current docs). YB LSNs are **not byte offsets** and are not comparable across slots - migkit's LSN arithmetic and fence must not assume PG semantics; no DDL between slot creation and end of snapshot; table-rewriting DDL streamable only from 2026.1 | PG `md5`/`sha256` | `yugabytedb/yugabyte` image | **S-M** |
| 4 | **Aurora DSQL** (target, newly common) | rides `postgres` with a hard overlay: **3,000 rows and 10 MiB per write transaction**, one DDL per transaction and no DDL+DML mix, Repeatable Read only, 5-minute transaction cap, no TRUNCATE (DELETE), `CREATE INDEX ASYNC`, no triggers/PL/pgSQL/extensions/temp tables; sequences and identity since Feb 2026 (use large CACHE), foreign keys since Aug 27 2026 (CASCADE counts toward the 3,000) | psycopg + IAM token | **no logical replication**; **DSQL CDC (GA Jul 8 2026) to Kinesis**, one net record per changed row per transaction - migkit's `kinesis` engine as the reverse leg's reader | PG functions | AWS only (free-tier allowance) | **S-M** (batch clamp is the work) |
| 5 | **Google Cloud Spanner** | new engine; PG-dialect databases could ride `postgres` through PGAdapter (Java) for reads | `google-cloud-spanner` (Apache-2.0) | **change streams** (`READ_<stream>` partitions that split/merge - the hard part); commit timestamp = fence | `SHA256`, `FARM_FINGERPRINT` + `BIT_XOR` at one read timestamp | **emulator** 1.5.x, x86 **and arm64**, change streams supported (emulator crashes on `ALTER CHANGE STREAM ... SET FOR` after a tracked column is dropped, #371); in-memory, no auth | **M-L** |
| 6 | **CockroachDB** | rides `postgres` for reads/writes (`COPY`, `IMPORT INTO`) | psycopg | **changefeeds** (sinkless `EXPERIMENTAL CHANGEFEED FOR` to the SQL client, or Kafka/webhook sinks) with **resolved timestamps** as the fence; no `pgoutput` | `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ... AS OF SYSTEM TIME` (documented for PCR; hits "integer out of range" in 26.x tests) | `cockroachdb/cockroach` arm64; **not open source** (BSL since 2019, Core retired Nov 18 2024; Enterprise Free <US$10M revenue, telemetry mandatory) | **S-M** |
| 7 | **TimescaleDB** | rides `postgres`; hypertables are chunk child tables | psycopg | logical replication of hypertables goes through chunks; compressed chunks (Timescale Licence, not Apache) are not decodable by logical decoding - decompress or copy by time range | PG functions | `timescale/timescaledb` arm64 | **S-M** |
| 8 | **Couchbase** | new engine: KV get/upsert by key, SQL++ for scans | Python SDK (Apache-2.0) | **DCP** through the Kafka connector (`kafka-connect-couchbase`, Apache-2.0; works on CE) into the `kafka` reader; no maintained Python DCP client | in-process | official image arm64; server is **BSL 1.1** (source) / CE licence (<= 5 nodes, 4 cores/node, no XDCR); EE free for dev/test | **M** |
| 9 | **Firestore** | MongoDB-compatible Enterprise edition (GA Aug 2025; mongodump/mongorestore supported) rides `mongodb`; Native mode needs its own engine | `google-cloud-firestore` (Apache-2.0) | Mongo-compat: change streams (Preview, created in the console with a retention); Native: no change log (listeners only) | in-process | Firestore emulator (Java 21; `--edition=enterprise`); Mongo wire in the emulator unconfirmed | **S** (compat) / **M** (native) |
| 10 | **Informix** | new engine on ODBC/`ibm_db` (DRDA) | IBM drivers | server's free CDC API (`syscdcv1`), Java client only (Debezium Informix connector) | in-process | x86 image only (see `oracle-mssql-db2` s.5) | **M-L** |
| 11 | **Progress OpenEdge** | new engine over the SQL broker (ODBC, DataDirect driver shipped with OpenEdge) | pyodbc + licensed driver | **OpenEdge CDC** is a **licensed add-on** (or Advanced Enterprise): `_Cdc-Change-Tracking` ordered by `_Change-sequence` plus per-table change tables, read by SQL, consumer deletes by sequence range | in-process | no free image; ABL data can overflow SQL widths (`dbtool` fixes), `DATETIME-TZ` errors through SQL | **M** |
| 12 | **SAP ASE / Sybase IQ** | ASE exists; IQ needs the SQL Anywhere client (`sqlanydb` wrapper over a proprietary library) | - | none open (RepAgent is licensed) | ASE `hashbytes`; IQ in-process | none free | IQ **M**, low value |
| 13 | **Neo4j** | backlog R16c (deferred) | `neo4j` driver (Apache-2.0); server Community GPLv3 | **CDC (`db.cdc.query`) only in Enterprise / Aura Virtual Dedicated Cloud**, needs `txLogEnrichment` DIFF/FULL; bulk `neo4j-admin import/load` bypass it | in-process over canonical node/edge text | official image arm64; Enterprise eval licence for CDC | **M-L** |
| 14 | **InfluxDB** (1.x/2.x to 3) | new engine: SQL over Flight into Arrow; writes as line protocol (series + timestamp is the natural upsert key, so replays land on themselves) | `influxdb3-python` (Apache-2.0 per listing); server 3 Core **MIT/Apache-2.0** (5-database limit, no compactor) | none | in-process | `influxdb:3-core` arm64 | **M** |
| 15 | **Bigtable / HBase** | new engine: row-key scans, mutations | `google-cloud-bigtable` (Apache-2.0); `happybase` (MIT) over Thrift | Bigtable change streams; HBase replication/WAL are Java | in-process | Bigtable emulator in gcloud; HBase images | **M-L**, low value |
| 16 | **Vector DBs** (pgvector rides `postgres`; Milvus, Qdrant, Weaviate) | engines per backlog R16: id-keyed scans (`query_iterator`, `scroll`, cursor `after`), upserts by id | `pymilvus` (Apache-2.0), `qdrant-client` (Apache-2.0), `weaviate-client` (BSD-3) | Milvus `milvus-cdc` (Apache-2.0); others none | vectors compared as float32 bytes, never through text; index parameters carried and reported | all arm64 images | **M** each |
| 17 | **Firebird** | new engine | `firebird-driver` (MIT); server IPL/IDPL (open) | Firebird 4+ replication is log shipping between servers, not a client API: triggers or key-set diff | Firebird 4+ `CRYPT_HASH(x USING SHA256)` (to verify) | official `firebirdsql/firebird` image (arm64 to verify) | **S-M**, low value |
| 18 | **ScyllaDB Alternator** | rides `dynamodb` with `endpoint_url` | boto3 | **Alternator Streams GA in ScyllaDB 2026.2** (10 s default delay; a PutItem arrives as REMOVE+MODIFY; no Kinesis destination); server source-available since Dec 2024 (free <= 50 vCPU / 10 TB; last AGPL is 6.2.x) | in-process | `scylladb/scylla` image with Alternator port | **S** |
| 19 | **MariaDB Xpand** | rides `mysql` for reads; **not sold since Oct 2023** - exit migrations only | PyMySQL | - | in-process | none | **S** |

Elasticsearch/OpenSearch already exist (`opensearch.py`).

**Pattern worth building once:** four engines here (TiDB, Couchbase,
Aurora DSQL, CockroachDB with a Kafka sink) deliver their changes as a
Kafka or Kinesis stream in a documented envelope. A single "follow a
change topic in Debezium/Canal/TiCDC/DSQL envelope" reader in the hetero
tail, with the envelope's own position (TiCDC commit-ts, CRDB resolved
timestamp, DSQL sequence) as the fence, reaches all of them. **M** once.

## 5. SaaS applications: which open connectors may legally be dependencies

migkit is MIT. A dependency must be under a licence that lets anyone
redistribute and run migkit, including as a service: MIT, BSD, Apache-2.0,
MPL-2.0/EPL-2.0/LGPL used unmodified as separate files or libraries.
AGPL/GPL, ELv2, BSL and proprietary code can at most be **driven** as a
separate program the operator installs (the rule migkit already applies
to OpenLogReplicator), never imported, vendored or baked into an image.

| family | licence found | usable as a migkit dependency? |
|---|---|---|
| **dlt** core, incl. `dlt.sources.rest_api` (moved into core at 1.0) and `sql_database` | Apache-2.0 | **yes** |
| **dlt verified sources** (`dlt-hub/verified-sources`: Salesforce, HubSpot, Stripe, Zendesk, Jira, GitHub, Google Sheets, Notion, Shopify, Pipedrive...) | Apache-2.0 (code copied into the project by `dlt init`) | **yes**, pinned and vendored by commit |
| dltHub paid tiers (`dlthub` package), dltHub AI Workbench/Harness | commercial EULA / "dltHub AI Source Available License" (use outside dltHub services not permitted) | no |
| Meltano **singer-sdk** | Apache-2.0 | yes |
| SDK-built MeltanoLabs taps | per tap: `tap-github` Apache-2.0; `tap-salesforce` (MeltanoLabs fork of singer-io) **AGPL-3.0**; `tap-hubspot` Apache-2.0 on PyPI but README text reads like ELv2 | only after reading each LICENSE |
| **singer-io taps** (Stitch: Salesforce, HubSpot, Stripe, Marketo, Intercom, Klaviyo, Google Sheets, Jira, Shopify...) | **AGPL-3.0** across the org | **no** - drive only, unmodified |
| `pipelinewise-*` taps | AGPL-3.0 (PyPI badge on one says MIT; the LICENSE file says AGPL) | no |
| **Airbyte CDK**, PyAirbyte | MIT | yes, but they only matter with connectors |
| **Airbyte certified connectors** | **ELv2** in `metadata.yaml`: `source-salesforce` (2.9.2), `source-hubspot` (6.9.3), `source-github`, `source-jira` (also `source-postgres`, `source-mysql` per the 2026-09-27 report) | **no** (ELv2 forbids offering as a managed service; not OSI) - drive only; the licence FAQ does not list which connectors are MIT |
| `erpl`/`erpl-web` (SAP, Dynamics, M365 into DuckDB) | BSL-1.1 | no |
| vendor SDKs used directly (e.g. Salesforce Pub/Sub API from `forcedotcom/pub-sub-api`'s `pubsub_api.proto` + `grpcio` + Apache Avro) | gRPC/Avro Apache-2.0; the proto repo's licence not confirmed | yes for gRPC/Avro; generate stubs from the proto only after its licence is read |

**Design for migkit (only where a migration, not an analytics feed, is
the point):**
* A **`saas` source engine that runs a dlt source in-process** and
  receives Arrow tables into the hetero writer. A "database" is a
  configured dlt source; a table is a resource; the key is the
  resource's `primary_key`; the incremental cursor (dlt state) is the
  position. **M** for the bridge, **S** per source after that.
* **Verification without a server digest:** counts from the API's own
  count endpoints (Salesforce `SELECT COUNT() FROM <object>` per
  `SystemModstamp` window), then a second extraction by key windows
  through the in-process digest; values rendered from the source's
  declared field types, not dlt's inferred ones (dlt's pandas backend
  loses decimal precision, the 2026-09-27 report).
* **Salesforce, the most-asked one, has a real change log:** Change Data
  Capture over the **Pub/Sub API** (gRPC, Avro payloads, `replay_id`
  not contiguous - store it exactly, never compute; events kept **72
  hours**; `ManagedSubscribe` (beta) commits the replay id server-side).
  Bulk through Bulk API 2.0 query jobs. A tail that has been down longer
  than 72 hours must re-extract by `SystemModstamp` and reconcile - the
  guard `assess` names. **M.**
* Everything else stays "not reached" until a migration asks, as W5
  already says.

## 6. Ranked table

Value = how often a migration that would otherwise buy Qlik, Precisely,
Informatica, IBM or Fivetran needs it, times how much of the paid tool's
advantage the open path removes. Effort per section above.

| # | engine | reach today in migkit | best paid tool | open path + licence | test environment | effort | value |
|---|---|---|---|---|---|---|---|
| 1 | Snowflake / BigQuery / Databricks-Delta / Iceberg exact loads | sides exist (row DML; BigQuery load job after delete); no staged bulk, no exact-once primitive, no in-server digest | Fivetran, Qlik, Informatica | Storage Write API COMMITTED+offsets (Apache-2.0 client); Snowpipe Streaming channels (`snowpipe-streaming`, Apache-2.0); Delta `txn` via delta-rs (Apache-2.0); Iceberg snapshot properties (pyiceberg, Apache-2.0); SHA-256 digest in SQL | goccy/bigquery-emulator (MIT, arm64); delta-rs/pyiceberg on local disk; Snowflake trial; Databricks Free Edition | M-L | **very high** |
| 2 | Teradata | none (DVT reaches it for verify only) | Qlik (TPT target, context-column source), Informatica | `teradatasql` (proprietary, free) FastExport/FastLoad via escape functions; MERGE + batch mark; SHA-256 needs a UDF | ClearScape Analytics Experience (hosted, free, limited); Vantage Express VM (UTM on Apple Silicon) | M | **very high** |
| 3 | VSAM / sequential files with COBOL copybooks (and IMS unloads) | none | Precisely Connect CDC SQData, IBM Classic, Qlik | Cobrix (Apache-2.0, JVM) as second reader; own Python decoder or Stingray (MIT, py>=3.12); `ebcdic` (BSD-2); z/OSMF record mode or FTP `SITE RDW` | fixtures on arm64; GnuCOBOL-written records; MVS 3.8j TK5 on Hercules (QPL) for real VSAM + FTP | M-L | **high** |
| 4 | Db2 for i (IBM i) incl. journal CDC | none (Db2 engine is LUW only) | Precisely MIMIX/Connect CDC, Qlik, IBM | ODBC (IBM, free, native macOS arm64) or Mapepire (Apache-2.0) or jt400 (IPL-1.0); follow by `QSYS2.DISPLAY_JOURNAL`; `HASH_ROW` digest; Debezium IBM i connector (Apache-2.0) as rung | PUB400 (manual, non-commercial); no emulator | M-L | **high** |
| 5 | Db2 for z/OS | none | Qlik (R4Z, IFCID 306), IBM IIDR, Precisely | `ibm_db` + **operator's Db2 Connect licence**; `HASH(x,2)`; follow only by timestamp/key-set diff or Debezium ZOS mode (incubating, IIDR licence) | IBM Z Xplore / Z Trial (manual only) | M | high |
| 6 | Azure Cosmos DB for NoSQL | Mongo and Cassandra APIs ride existing engines; NoSQL none | Informatica, Qlik (via Kafka), Striim | `azure-cosmos` (MIT); change feed (AVAD for deletes, needs continuous backup) | vNext emulator, x64 + ARM64 | M | high |
| 7 | SAP ECC/S4 application data | none | Qlik (SAP Application), Fivetran HVR, Theobald, SAP SLT/Datasphere | **ODP over OData** via `pyodata` (Apache-2.0) + own delta-token handling; DB-level only with a full-use DB licence; ODP-RFC forbidden (Note 3255746), PyRFC archived | ABAP Cloud Developer Trial (amd64, 16-32 GB; ODP-OData unproved) | M-L | high |
| 8 | Change-topic follower (TiDB TiCDC, Couchbase DCP connector, Aurora DSQL CDC, CockroachDB changefeeds) | kafka/kinesis readers exist, envelopes not | Qlik, Striim, Fivetran | one envelope-aware reader in the hetero tail; all producers Apache-2.0 or cloud | tiup playground, Couchbase image, CRDB image (all arm64); DSQL AWS only | M once | high |
| 9 | Aurora DSQL (target) | would ride postgres and fail on the 3,000-row / 10 MiB transaction cap | AWS DMS does not target it (Apr 2026 blog) | postgres overlay: batch clamp, one DDL per txn, `CREATE INDEX ASYNC`, DELETE not TRUNCATE; reverse leg from DSQL CDC via kinesis | AWS | S-M | medium-high |
| 10 | SAP HANA (as a database) | none | Qlik (trigger/log), Fivetran HVR (log agent), Informatica | `hdbcli` (SAP developer licence, operator installs); `HASH_SHA256`; trigger-based follow only with full-use licence | `saplabs/hanaexpress` (amd64); HANA Cloud free tier | M | medium |
| 11 | Netezza / IBM PDA | none | Qlik, Informatica, AWS SCT agents | `nzpy` (IBM; licence metadata inconsistent) external tables `REMOTESOURCE 'python'`; `createxid`/`deletexid` follow before GROOM; `hash(x,2)` if toolkit installed | NPS SaaS trial credit only | M | medium (exits) |
| 12 | Greenplum / Cloudberry / WarehousePG | would ride postgres untested | Qlik, Informatica | psycopg; `gpfdist`; Apache-2.0 forks | `apache/incubator-cloudberry` image | S-M | medium (exits after closure) |
| 13 | Spanner | none (DVT verifies) | Datastream, Striim, Qlik | `google-cloud-spanner` (Apache-2.0); partitioned reads at a timestamp; change streams | emulator arm64 with change streams | M-L | medium |
| 14 | YugabyteDB / CockroachDB / TiDB | TiDB target rides mysql; others ride postgres untested | Qlik, Striim, vendor tools | aliases + native CDC (YB pgoutput slots EA; CRDB changefeeds; TiCDC) + native fingerprints (`ADMIN CHECKSUM TABLE`, `SHOW EXPERIMENTAL_FINGERPRINTS`) | all arm64 images | S-M each | medium |
| 15 | Vertica | generic (verify only) | Qlik, Informatica | `vertica-python` (Apache-2.0) COPY STDIN; `AT EPOCH` consistent reads; `SHA256()` | no CE image any more; x86 CI with `opentext/vertica-k8s` unlicensed | M | medium |
| 16 | SaaS (Salesforce first) | none | Fivetran, Informatica, Qlik | dlt + verified sources (Apache-2.0); Salesforce Pub/Sub CDC (72 h replay); Singer AGPL and Airbyte ELv2 connectors driven only | Salesforce Developer Edition org (free) | M bridge, S per source | medium |
| 17 | Couchbase | none | Qlik (via Kafka), Striim | Python SDK (Apache-2.0) + DCP Kafka connector (Apache-2.0) | official image arm64 (server BSL/CE licence) | M | medium-low |
| 18 | Informix | none | Qlik, Precisely, IBM | ODBC/`ibm_db`; server CDC API via Debezium (Java) | x86 image | M-L | medium-low |
| 19 | Progress OpenEdge | none | Qlik (via ODBC), Precisely | pyodbc + licensed DataDirect driver; OpenEdge CDC tables (licensed add-on) | none free | M | medium-low |
| 20 | Exasol | none | Informatica, Qlik (target) | `pyexasol` (MIT) HTTP transport IMPORT/EXPORT | `exasol/docker-db` x86 only | M | low-medium |
| 21 | Firestore | Mongo-compat rides mongodb | Fivetran | `google-cloud-firestore` (Apache-2.0) | Java emulator | S / M | low-medium |
| 22 | TimescaleDB, InfluxDB | Timescale rides postgres untested; Influx none | Fivetran, vendor tools | psycopg / `influxdb3-python` (Apache-2.0), line protocol upserts | arm64 images | S-M / M | low-medium |
| 23 | Vector DBs (Milvus, Qdrant, Weaviate) | pgvector rides postgres | Airbyte (load only), vendor tools | Apache-2.0 / BSD-3 clients; byte-exact vector compare | arm64 images | M each | low-medium |
| 24 | Neo4j | none (R16c deferred) | vendor tools | `neo4j` driver (Apache-2.0); CDC Enterprise-only | arm64 image | M-L | low |
| 25 | Bigtable/HBase, Firebird, Sybase IQ, MariaDB Xpand, IMS live | none | Qlik/Precisely (IMS), vendors | see sections 1.2 and 4 | mixed | S-L | low |

**Order that follows from the table** (each item arrives with its
verification or does not arrive, the backlog's rule):
1. Exact warehouse loads and the SQL-side SHA-256 digest (row 1): the
   sides already exist, the test beds are free and run on arm64.
2. The copybook engine (row 3) and the IBM i platform of `db2` with the
   journal tail (row 4): the largest paid-tool moats with a fully open
   path; copybook work needs no server at all.
3. Teradata (row 2) on the DB-API base with FastLoad/FastExport.
4. The change-topic follower (row 8), then the aliases it unlocks
   (rows 9, 12, 14).
5. Cosmos DB NoSQL (row 6); SAP ODP-over-OData (row 7) once a customer
   system or a proved trial image exists.
6. The rest when a migration asks.

## Sources

Mainframe / IBM i
* Qlik R4Z components: https://help.qlik.com/en-US/replicate/May2025/Content/Global_Common/Content/SharedReplicateHDD/R4Z_Install_Config/r4z-components-and-the-associated-environment.htm
* Db2 IFCID 0306: https://www.ibm.com/docs/en/db2-for-zos/13.0.0?topic=ifi-reading-complete-log-data-ifcid-0306
* Db2 z/OS HASH (FL 506): https://www.ibm.com/docs/en/db2-for-zos/12.0.0?topic=functions-hash
* ibm_db and Db2 Connect licence (SQL1598N): https://github.com/ibmdb/python-ibmdb/issues/888
* Precisely SQData VSAM/IMS capture: https://docs.precisely.services/docs/sftw/sqdata-webhelp/4.0/en-us/webhelp/HTML/web_vsam.html ; https://docs.precisely.services/docs/sftw/sqdata-webhelp/4.0/en-us/webhelp/HTML/ims_log_reader_capture.html
* IMS X'99' data capture: https://www.ibm.com/support/pages/ims-99-log-record-creation-when-multiple-ims-change-data-capture-exits-are-coded
* QSYS2.DISPLAY_JOURNAL: https://www.ibm.com/support/pages/qsys2displayjournal ; HASH_ROW: https://www.ibm.com/support/pages/hashrow-built-function
* Debezium IBM i connector: https://github.com/debezium/debezium-connector-ibmi ; Debezium 2.6 notes: https://debezium.io/blog/2024/03/06/debezium-2-6-beta1-released/
* Debezium Db2 ZOS mode: https://debezium.io/documentation/reference/stable/connectors/db2.html
* JTOpen licence: https://github.com/IBM/JTOpen/blob/main/LICENSE.md ; Mapepire: https://github.com/Mapepire-IBMi/mapepire-python
* IBM i ODBC: https://ibmi-oss-docs.readthedocs.io/en/latest/odbc/installation.html
* PUB400: https://www.itjungle.com/2024/03/04/pub400-your-free-ibm-i-playground/
* Cobrix: https://github.com/AbsaOSS/cobrix ; Stingray: https://pypi.org/project/stingray-reader/ ; MDU: https://github.com/aws-samples/mainframe-data-utilities ; ebcdic: https://pypi.org/project/ebcdic/ ; JRecord/cb2xml: https://github.com/bmTas/JRecord
* z/OSMF record mode: https://www.ibm.com/docs/en/zos/2.5.0?topic=services-zos-data-set-file-rest-interface ; mvsMF note: https://github.com/mvslovers/mvsmf/issues/361 ; Zowe SDK: https://github.com/zowe/zowe-client-python-sdk
* Hercules: https://github.com/SDL-Hercules-390/hyperion ; TK5 arm64 image: https://hub.docker.com/r/praths/mvs-tk5 ; ZD&T status: https://community.ibm.com/community/user/question/alternatives-to-zdt-personal-edition-for-individuals-help ; IBM Z Xplore: https://ibmzxplore.ibm.com/

Warehouses
* teradatasql: https://github.com/Teradata/python-driver ; DVT Teradata UDF: https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1314 ; Qlik Teradata context columns: https://help.qlik.com/en-US/replicate/May2022/Content/Replicate/Main/Teradata/set_up_teradata_change_processing.htm
* ClearScape: https://developers.teradata.com/quickstarts/get-access-to-vantage/clearscape-analytics-experience/getting-started-with-csae/ ; Vantage Express: https://developers.teradata.com/quickstarts/get-access-to-vantage/on-your-local/getting-started-vbox/
* nzpy: https://github.com/IBM/nzpy ; Netezza hash: https://www.ibm.com/docs/en/netezza?topic=set-hashing-functions-1 ; createxid CDC: https://aws.amazon.com/blogs/big-data/accelerate-your-data-warehouse-migration-to-amazon-redshift-part-7/
* vertica-python: https://github.com/vertica/vertica-python ; CE discontinued: https://docs.vertica.com/25.3.x/en/getting-started/community-edition-ce/ ; historical queries: https://docs.vertica.com/24.3.x/en/data-analysis/queries/historical-queries/
* Greenplum forks: https://github.com/apache/cloudberry/ ; https://github.com/warehouse-pg/warehouse-pg
* pyexasol: https://pypi.org/project/pyexasol/ ; docker-db: https://github.com/exasol/docker-db
* hdbcli: https://pypi.org/project/hdbcli/ ; HANA express: https://hub.docker.com/r/saplabs/hanaexpress ; HANA Cloud free tier: https://developers.sap.com/tutorials/hana-cloud-mission-trial-1.html ; HVR HANA capture: https://fivetran.com/docs/hvr6/requirements/source-and-target-requirements/sap-hana-requirements/sap-hana-as-source
* Databricks COPY INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into ; Free Edition: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations ; delta-rs idempotent writes: https://github.com/delta-io/delta-rs/issues/3821
* pyiceberg snapshot properties: https://py.iceberg.apache.org/api/ ; pyiceberg #4022: https://github.com/apache/iceberg-python/issues/4022
* Snowpipe Streaming SDK: https://pypi.org/project/snowpipe-streaming/ ; channels: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-channels ; HASH_AGG: https://docs.snowflake.com/en/sql-reference/functions/hash_agg
* BigQuery Storage Write API: https://docs.cloud.google.com/bigquery/docs/write-api-streaming ; sandbox: https://docs.cloud.google.com/bigquery/docs/sandbox ; emulator: https://github.com/goccy/bigquery-emulator
* Redshift FNV_HASH: https://docs.aws.amazon.com/redshift/latest/dg/r_FNV_HASH.html

SAP
* Note 3255746 summaries: https://theobald-software.com/en/blog/sap-note-3255746 ; https://docs.matillion.com/metl/docs/tech-note-sap-3255746/
* Runtime licence restrictions: https://snapanalytics.co.uk/sap-licence-constraints-explainer/
* PyRFC archive: https://github.com/SAP-archive/PyRFC/issues/372 ; NW RFC SDK: https://support.sap.com/en/product/connectors/nwrfcsdk.html
* pyodata: https://github.com/SAP/python-pyodata ; ODP OData delta: https://techcommunity.microsoft.com/blog/azuresynapseanalyticsblog/extracting-sap-data-using-odata---part-7---delta-extraction-using-sap-extractors/2865383 ; DeltaLinksOf recovery: https://github.com/DataZooDE/erpl-web/issues/250
* CDS CDC extraction: https://community.sap.com/t5/enterprise-resource-planning-blog-posts-by-sap/cds-based-data-extraction-part-ii-delta-handling/ba-p/13425761
* ABAP trial image: https://hub.docker.com/r/sapse/abap-cloud-developer-trial ; https://github.com/SAP-docs/abap-platform-trial-image
* Cluster tables: https://blogs.sap.com/2018/06/23/myth-and-truth-about-cluster-pool-tables-on-hana/
* erpl licences: https://github.com/DataZooDE/erpl-web ; https://github.com/DataZooDE/erpl

Other engines
* Cosmos change feed modes: https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed-modes ; vNext emulator: https://learn.microsoft.com/en-us/azure/cosmos-db/emulator-linux
* YugabyteDB logical replication: https://docs.yugabyte.com/stable/additional-features/change-data-capture/using-logical-replication/
* Aurora DSQL: quotas https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html ; CDC GA https://aws.amazon.com/about-aws/whats-new/2026/07/amazon-aurora-dsql-cdc-ga/ ; foreign keys https://aws.amazon.com/about-aws/whats-new/2026/08/aurora-dsql-foreign-key-constraints/
* Spanner emulator: https://github.com/GoogleCloudPlatform/cloud-spanner-emulator ; issue #371: https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/371
* CockroachDB licensing: https://www.cockroachlabs.com/docs/stable/licensing-faqs
* ScyllaDB licence: https://www.scylladb.com/source-available-faq/ ; Alternator Streams GA: https://www.scylladb.com/2026/06/29/scylladb-2026-2/
* Couchbase BSL: https://www.couchbase.com/blog/couchbase-adopts-bsl-license/ ; editions: https://docs.couchbase.com/server/current/introduction/editions.html
* Firestore MongoDB compatibility: https://firebase.blog/posts/2025/08/firestore-mongodb-general-availability/
* Neo4j CDC: https://neo4j.com/docs/cdc/current/
* InfluxDB 3 Core: https://github.com/influxdata/influxdb
* OpenEdge CDC: https://docs.progress.com/bundle/openedge-database-change-data-capture/page/Change-Tracking-Table.html
* MariaDB Xpand: https://www.theregister.com/2023/10/13/mariadb_restructure/

SaaS
* dlt verified sources: https://github.com/dlt-hub/verified-sources ; dltHub licences: https://dlthub.com/docs/hub/EULA
* singer-io org (AGPL-3.0): https://github.com/orgs/singer-io/repositories ; MeltanoLabs: https://github.com/orgs/MeltanoLabs/repositories ; singer-sdk: https://sdk.meltano.com/
* Airbyte metadata: https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-salesforce/metadata.yaml (and source-hubspot, source-github, source-jira) ; licence FAQ: https://docs.airbyte.com/platform/developer-guides/licenses/license-faq
* Salesforce Pub/Sub API: https://github.com/forcedotcom/pub-sub-api ; durability: https://developer.salesforce.com/docs/platform/pub-sub-api/guide/event-message-durability.html
