# Published throughput of change and load tools, and of load techniques (research as of 2026-09-27)

I collected 20+ published numbers for Part 1 and for Part 2. For several items there is no published number, and I say so. Almost every Part 1 figure is published by the tool's own vendor or a competitor, not by an independent tester. Figures marked "derived" are my own arithmetic from the published time and size.

**Limits of this research:**
- The session's web search limit (200 searches) ran out partway through. After that I could only open pages I already knew the address of. Airbyte, MirrorMaker 2, Cassandra Data Migrator, MySQL redo-log numbers and the fetch-size question got less coverage than the rest, so some "not found" there may mean "not searched deeply".
- The fetch tool saved one PDF to disk on its own when I opened the VLDB paper: `/Users/ashira/.claude/projects/-Users-ashira-develop-devops/652a7de3-e842-439d-830a-1f3882e77620/tool-results/webfetch-1790477695864-j68jbk.pdf`. I did not read it, and I created nothing else.

---

## Part 1 — Tools

**redis-shake**
- No published ops/s figure for sync, scan or rdb readers.
- The only number: with the source taking about 150k write QPS, source CPU went from 47% to 91% once scan_reader was running. The cause is DUMP, which is CPU-heavy. `count` defaults to 1, and raising it "significantly improves sync speed" but adds load on the source. https://tair-opensource.github.io/RedisShake/zh/reader/scan_reader.html
- The docs say "scan_reader will put significant pressure on the source database" and recommend sync_reader (PSync). https://tair-opensource.github.io/RedisShake/en/guide/mode.html

**Kafka MirrorMaker 2**
- No per-task or per-worker MB/s figure found.
- IBM's guidance: with small messages MM2 is CPU-bound, and with messages of 100 bytes or more the network saturates. https://jbcodeforce.github.io/kp-data-replication/mm2-provisioning/
- Nearby data point from Debezium on Kafka Connect: producer `batch.size=1,000,000`, `linger.ms=500` and lz4 compression cut a 1.5 TB Oracle snapshot from 8h to 6h, peaking at 90k ops/s. Setup was 3 Connect nodes with 12 CPU and 62 GB each. https://debezium.io/blog/2025/01/12/oracle-snapshot-performance-optimization/

**DSBulk**
- The only published claim: "loads data up to 4x faster than cqlsh's COPY command". No rows/s benchmark or setup was given. Source is the DataStax 2018 intro blog, which now redirects to IBM, so I read it through the Wayback Machine: https://web.archive.org/web/2020/https://www.datastax.com/blog/introducing-datastax-bulk-loader

**Cassandra Data Migrator**
- No throughput number published.
- Defaults: `numParts` 5K (the README suggests about table size / 10 MB), `ratelimit` 20000, `fetchSizeInRows` 1K, `batchSize` 5. A Spark cluster is recommended for multi-TB migrations. https://github.com/datastax/cassandra-data-migrator

**ClickHouse**
- **remote() INSERT…SELECT (user report):** 228.62 GB and 17.26M rows in 482 s, which is 474 MB/s and 35.8k rows/s, with `max_insert_threads=300`. https://github.com/ClickHouse/ClickHouse/issues/30479
- **ClickHouse's own tuned bulk load (from S3, not remote):**
  - About 4M rows/s for 65.33B rows (~14 TiB raw).
  - That was almost 3x faster than default settings.
  - Settings: 32 insert threads and 10M-row insert blocks, on 59-core / 236 GiB servers.
  - Rule given: `max_insert_threads` ≈ half the cores.
  - https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part2
- **clickhouse-backup:** on a 2 TB / 50k-part backup over 10 Gbps to MinIO, upload went from ~350 to ~1000 MB/s and download from ~400 to ~1100 MB/s after tuning. Raising `MaxIdleConnsPerHost` from 2 to 128 alone gave about 30% of the gain. https://github.com/Altinity/clickhouse-backup/issues/1376
- **Native `BACKUP … TO S3`:** no published GB/s figure found.

**Google Datastream**
- Current docs publish no per-stream MB/s figure.
- An older "~5 MBps" figure appears only in a third-party copy. I could not confirm it on any Google page, so treat it as unverified.
- Backfill: `maxConcurrentBackfillTasks` defaults to 15 (range 1–50). CDC: `maxConcurrentCdcTasks` defaults to 5 (range 1–50) and applies only to MySQL and Oracle; "CDC in PostgreSQL and SQL Server is single-threaded". https://docs.cloud.google.com/datastream/docs/stream-concurrency-controls
- Row size limit is 20 MB for BigQuery and 100 MB for Cloud Storage, with up to 10,000 tables per stream. https://docs.cloud.google.com/datastream/docs/faq

**PeerDB**
- **vs Airbyte, Postgres to Snowflake:**
  - Table: 6B rows, 1.5 TB.
  - Airbyte took 83h (derived ~5 MB/s). PeerDB took 43h with 1 thread, under 9h with 8 threads and under 5h with 32–48 threads.
  - PeerDB calls this 16x faster.
  - Techniques: CTID partitions, a `pg_export_snapshot` snapshot shared by all threads, Avro, ~750K-row batches.
  - https://blog.peerdb.io/benchmarking-postgres-replication-peerdb-vs-airbyte
- **Postgres to Postgres, 1 TB / 3.6B rows** (RDS db.r8g.2xlarge source, c5d.12xlarge mover):

  | Method | Time | Derived MB/s |
  |---|---|---|
  | pg_dump/pg_restore | 17h05m | ~16 |
  | Native logical replication | 8h40m | ~32 |
  | PeerDB, 8 threads | 1h50m | ~150 |
  | PeerDB, 16 threads | 2h10m | — |

  At 16 threads the article says the source RDS network bandwidth was the limit. https://clickhouse.com/blog/practical-postgres-migrations-at-scale-peerdb
- **1.5 TB vs pg_dump:** 1.5 days vs 7h ("5x"). https://blog.peerdb.io/how-can-we-make-pgdump-and-pgrestore-5-times-faster
- Postgres-to-Postgres transfer uses binary COPY; warehouse targets go through Avro. https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres
- **Docs claims:** "2x to 16x faster"; "10MBPS to 80MBPS" vs "most other tools cap at 5-6MBPS"; CDC lag 30s–1min to warehouses and 1–5s to queues; 5K TPS. https://docs.peerdb.io/why-peerdb
- No published head-to-head benchmark against Debezium or Fivetran found.

**Estuary**
- MongoDB capture:
  - 20 KB documents: 34 → 57 MB/s.
  - 250-byte documents: ~6 → ~17.5 MB/s.
  - Credited to prefetching (up to 4 batches, 64 MB) and switching BSON decoding from Go to Rust.
  - https://estuary.dev/blog/mongodb-capture-optimization
- "A single worker sustains on the order of 200 GB per hour" (derived ~55 MB/s). https://estuary.dev/blog/estuary-platform-overview
- Homepage headline stats only: "<100ms latency" and "3 petabytes/month" moved. The "7+ GB/s" figure could not be confirmed on the current pages.

**Artie**
- **vs AWS DMS, Postgres to Snowflake:**
  - Load: about 22,400 events/s for 600 s, on RDS db.m7i.2xlarge.
  - CDC latency: 29.7 s vs 2,029 s (68x).
  - History mode: 84.8 s vs 1,978 s (23x).
  - Retained WAL: 385 MB vs 6,271 MB.
  - 100M-row snapshot: 9m36s vs ~12 min.
  - https://www.artie.com/blogs/artie-vs-aws-dms
- Parallel backfill: 100M rows in 9m36s vs 67 min without parallelism (derived ~174k vs ~25k rows/s). Backfills 10 tables in parallel by default; CTID sharding is claimed at "10-20x". https://www.artie.com/blogs/online-database-backfill and https://www.artie.com/blogs/postgres-ctid-scanning

**Sling**
- No official benchmark.
- dltHub's (competitor's) test: 9.74 GB TPC-H, Cloud SQL Postgres to BigQuery, on e2-standard-4:

  | Tool / backend | Time |
  |---|---|
  | dlt + ConnectorX | 10m51s |
  | Sling Pro | 14m16s |
  | dlt + PyArrow | 15m07s |
  | dlt + pandas | 17m41s |
  | Sling (free) | 19m34s |
  | dlt + SQLAlchemy | 82m39s |

  https://dlthub.com/blog/dlt-and-sling-comparison

**ConnectorX**
- The headline "21x less time" is versus the slowest tool compared. Versus pandas it is 13x less time and 3x less memory.
- Test: TPC-H SF10 lineitem (8.6 GB), 4 cores, r5.4xlarge. https://github.com/sfu-db/connector-x
- Speedup vs pandas by database: Postgres 13x, MySQL 8x, SQLite 5x, Oracle 3x, MSSQL 14x (with 4 partitions on `l_orderkey`). https://github.com/sfu-db/connector-x/blob/main/Benchmark.md

**dlt**
- 10M Postgres rows: ConnectorX + Arrow took 16.2 s vs 8m13s for SQLAlchemy + JSON (~30x). Extract and normalize only, local DB. https://dlthub.com/blog/dlt-arrow-loading
- Docs: "Postgres is the only backend where we observed a 2x speedup with ConnectorX" over PyArrow. https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/troubleshooting
- Arrow + ADBC vs SQLAlchemy: 92 s vs 344 s for 5M rows (3.7x). The post names DuckDB as source and MySQL as destination. https://dlthub.com/blog/arrow-adbc-vs-sqlalchemy
- The 2024 benchmark states "2.8x to 6x" faster than Sling and Airbyte but gives no absolute times in the text. https://dlthub.com/blog/self-hosted-tools-benchmarking

**DuckDB postgres_scanner**
- Reads each CTID range with `COPY (SELECT …) TO STDOUT (FORMAT BINARY)`; defaults are 1000 pages per task, binary copy on, ctid scan on.
- TPC-H SF1 on an M1 Max: e.g. Q1 took 0.74 s via the scanner vs 1.12 s in Postgres itself. DuckDB on its own storage is about 10x faster. https://duckdb.org/2022/09/30/postgres-scanner and https://duckdb.org/docs/current/core_extensions/postgres/overview
- mysql_scanner: no published speed numbers found.

**ADBC**
- No official benchmark against psycopg.
- A user reports "5-10x throughput" over psycopg + pandas for large fetches, with lower CPU on both client and database. https://github.com/apache/arrow-adbc/issues/3201
- Columnar (commercial ADBC driver vendor): MySQL ingest of 600k rows went from 455 s to 7 s after adding multi-row INSERT batching; BigQuery read 31.0 s vs 56.4 s with sqlalchemy-bigquery. https://columnar.tech/blog/adbc-driver-optimization-deep-dive/ and https://columnar.tech/blog/zero-copy-zero-contest/

**Airbyte**
- Official: Snowflake destination "up to 10x faster syncs" and "95%+ cost reduction", with no MB/s given. https://airbyte.com/blog/snowflake-destination-enhancements
- The only measured figure is third-party: ~5 MB/s derived from the PeerDB test above.

**Fivetran / HVR**
- Target: 1 TB in 2h (~137 MB/s).
- Achieved: Oracle HVA to Snowflake 26 → 139 MB/s; Postgres to BigQuery 48 → 259 MB/s.
- Multithreaded initial import: Oracle extract time cut 5x, Postgres 3x faster, MySQL 3.3x faster; binlog client rewrite 3x faster.
- https://www.fivetran.com/blog/how-we-do-performance-engineering-at-fivetran

**Debezium**
- No general throughput figure published.
- The Oracle snapshot numbers are under MirrorMaker 2 above.
- JDBC sink with batching: 1M events went from ~570 min to ~7 min (79x), about 2,300 events/s, on a laptop running Docker. https://debezium.io/blog/2023/12/20/JDBC-sink-connector-batch-support/

---

## Part 2 — Load and transfer techniques

### PostgreSQL

| Technique | Published result | Source |
|---|---|---|
| Binary vs text COPY | No controlled number found. Docs only say binary is "somewhat faster". Indirect: asyncpg (binary protocol) is "on average 5x faster than psycopg3". | https://www.postgresql.org/docs/current/sql-copy.html, https://github.com/MagicStack/asyncpg |
| COPY vs INSERT | 1M rows in one transaction: INSERT 81 s vs COPY 2.6 s (31x). | https://www.cybertec-postgresql.com/en/postgresql-bulk-loading-huge-amounts-of-data/ |
| COPY vs executemany (psycopg2) | 32,500 rows: executemany 124.7 s, execute_values (page 1000) 1.47 s, COPY string iterator 0.46 s (~270x vs executemany, ~3x vs execute_values). | https://hakibenita.com/fast-load-data-python-postgresql |
| COPY FREEZE | 500M rows: 627 s → 304 s (~2x). This was run in the create-table transaction with `wal_level=minimal`, so it mixes the WAL skip with the freeze. | https://www.cybertec-postgresql.com/en/loading-data-in-the-most-efficient-way/ |
| Cost COPY FREEZE avoids | After a plain COPY of 500M rows: first SELECT 360 s vs 110 s once hint bits are set; VACUUM took 364 s. | https://www.cybertec-postgresql.com/en/speeding-up-things-with-hint-bits/ |
| UNLOGGED then SET LOGGED | COPY of 1M rows: 2.6 s logged vs 0.6 s unlogged. SET LOGGED is expensive: 16.6 s vs 1.75 s for the reverse at pgbench scale 10. No end-to-end net-gain number found. | cybertec link above; https://www.crunchydata.com/blog/postgresl-unlogged-tables |
| Indexes after load | Load with indexes 8.4 s vs load 4.8 s + index builds 2.3 s = 7.1 s. | cybertec bulk-loading link above |
| Parallel B-tree build | 500M rows (~21 GB): 17m12s serial → 11 min with 2 workers → 7m28s with 4 GB `maintenance_work_mem` → 6m48s with multiple tablespaces (~2.5x). Integer keys: 3m51s. | https://www.cybertec-postgresql.com/en/postgresql-parallel-create-index-for-better-performance/ |
| `synchronous_commit=off` | No bulk-load number found. Docs: "significant boost in throughput for small transactions"; loss window up to 3× `wal_writer_delay`. | https://www.postgresql.org/docs/current/wal-async-commit.html |
| `wal_level=minimal` + COPY into table created in same transaction | No number found apart from the COPY FREEZE row. Docs: "no WAL needs to be written". | https://www.postgresql.org/docs/current/populate.html |
| psycopg3 pipeline mode | Docs: 100 statements at 300 ms RTT take 30 s without pipelining vs as little as 0.3 s with it; `executemany` uses pipelining automatically from 3.1. Dalibo: 2x on localhost for 1,000 INSERTs. No psycopg3-specific COPY number found. | https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html, https://blog.dalibo.com/2022/09/19/psycopg-pipeline-mode.html |
| Parallel reads by CTID range | Numbers are the PeerDB, DuckDB and Artie figures in Part 1. pgcopydb splits by integer primary key, or CTID as a fallback (`--split-tables-larger-than`), but publishes no numbers. | https://pgcopydb.readthedocs.io/en/latest/concurrency.html |

### MySQL

| Technique | Published result | Source |
|---|---|---|
| LOAD DATA vs INSERT | Official docs: "usually 20 times faster than using INSERT statements"; multi-row INSERT is "many times faster in some cases". | https://dev.mysql.com/doc/refman/8.0/en/insert-optimization.html |
| `unique_checks=0`, `foreign_key_checks=0`, primary-key order, `innodb_autoinc_lock_mode=2` | Recommended in the docs with no numbers ("saves a lot of disk I/O"). | https://dev.mysql.com/doc/refman/8.0/en/optimizing-innodb-bulk-data-loading.html |
| `sql_log_bin=0` | No published number found. | — |
| `ALTER INSTANCE DISABLE INNODB REDO_LOG` (8.0.21+) | Docs say it skips redo and doublewrite writes but give no number. The MySQL Shell benchmark loaded at ">200MB/s" with redo disabled, but has no before/after comparison. | https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html, https://dev.mysql.com/blog-archive/mysql-shell-dump-load-part-2-benchmarks/ |
| Deferred secondary indexes | **This goes against the PostgreSQL result.** MySQL's own benchmark: adding indexes afterwards "makes the whole process slower in all these datasets". | same MySQL Shell benchmark link |
| Chunked parallel dump/load | MySQL Shell dumps at about 3 GB/s and loads at over 200 MB/s (88 threads, 44-core Xeon, 410 GB across 3 datasets). mysqldump is single-threaded. Partitioning one large table into 128 partitions raised load throughput. | same MySQL Shell benchmark link |

### General

| Technique | Published result | Source |
|---|---|---|
| Compression codecs (Silesia corpus, i7-9700K) | ratio / compression / decompression speed: zstd -1: 2.896 / 510 / 1550 MB/s; zlib -1: 2.743 / 105 / 390 MB/s; lz4: 2.101 / 675 / 3850 MB/s; snappy: 2.089 / 520 / 1500 MB/s. | https://github.com/facebook/zstd |
| MySQL zstd protocol compression | Docs only: level 1–22, default 3, and compression helps "primarily when there is low network bandwidth". No measured number found. | https://dev.mysql.com/doc/refman/8.0/en/connection-compression-control.html |
| libpq compression | Not in core PostgreSQL; no number found. | — |
| Arrow/columnar vs row-based | ConnectorX, ADBC and dlt figures above. turbodbc vs pyodbc: 1.5–7x faster fetch and up to 100x faster insert, which the author calls "not at all" scientific. | https://turbodbc.readthedocs.io/en/latest/pages/introduction.html |
| Server-side cursor / fetch size | No controlled number found. PgJDBC fetches all rows by default; `setFetchSize` needs autocommit off and a forward-only result set. The 2017 VLDB paper by Raasveldt and Mühleisen studies client-protocol cost, but the fetch tool could not read the PDF. The Arrow blog quotes it: some systems "take over ten minutes to transfer a dataset that should only take ten seconds". | https://jdbc.postgresql.org/documentation/query/, https://www.vldb.org/pvldb/vol10/p1022-muehleisen.pdf, https://arrow.apache.org/blog/2025/02/28/data-wants-to-be-free/ |
| Network latency / TCP window (BDP) | Buffer = bandwidth × RTT; 50 ms at 1 Gbps needs 6.25 MB; 10 Gbps single stream at 100 ms needs 120 MB; raising the buffer from 32 to 64 MB at 75 ms RTT gave nearly 2x. | https://fasterdata.es.net/host-tuning/background/ |
| Packet loss at high latency | 0.0046% loss at 90 ms RTT: 490 Mbps vs 8.2 Gbps (~17x slower). | https://fasterdata.es.net/network-tuning/tcp-issues-explained/packet-loss/ |
