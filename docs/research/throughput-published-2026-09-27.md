# Published throughput of bulk and change tools (research as of 2026-09-27)

I found usable published numbers for DMS, both DTS products, TiDB Lightning and the MySQL dump tools. For GoldenGate, Qlik, Vitess copy rate, mongosync and pg_dump -j I found no published rate. The web-search budget (200 calls) ran out part-way through, so those five gaps come from a partial search, not a thorough one. Oracle's blog and whitepaper URLs also returned 403. Anything marked "derived" is my own arithmetic from the published figures.

## 1. AWS DMS
| Number | Context | Technique | Source |
|---|---|---|---|
| 100M-row table: 1h14m → 36 min (~2x) | Oracle → RDS for Oracle, dms.R5.xlarge (Apr 2023) | parallel-load `ranges`, 24 segments | https://aws.amazon.com/blogs/database/achieve-a-high-performance-migration-to-amazon-rds-for-oracle-from-on-premises-oracle-with-aws-dms/ |
| 58 GB table + 4.5 TB LOBs: DMS estimate "around 5 days" → "20 hours" (derived ~230 GB/h) | same post | tuning incl. MaxFullLoadSubTasks (default 8, max 49) | same |
| 5B-row fact table: ~6.5 h, "almost one third" of the first run's time | Oracle → S3, c5 instance | `ranges` with 16 boundaries + MaxFullLoadSubTasks=49; `ranges` beat `partitions-auto` | https://docs.aws.amazon.com/dms/latest/sbs/oracle-s3-data-lake-step-6.html |
| 258M rows, 5 partitions: ~9 min → 6 min (partitions-auto) → 5 min (10 ranges) → ~4 min (3 tasks with partitions-list) | Oracle | parallel-load types | https://www.amazonaws.cn/en/blog-selection/speed-up-database-migration-by-using-aws-dms-with-parallel-load-and-filter-options/ |
| 235,722 → 515,599 rows/s (2.2x) | SQL Server → Aurora PG, 60M rows, dms.r6i.large | MaxFullLoadSubTasks raised to 16 | https://aws.amazon.com/blogs/database/understanding-resource-distribution-and-performance-analysis-using-aws-dms-enhanced-monitoring/ |
| 500 tables × 1M rows: ~30 min → 16.01 (MFLST=49) / 21.17 (ParallelLoadThreads=16) / 7.88 min combined (−76%; derived ~278k → ~1.06M rows/s) | → Redshift, dms.r5.4xlarge, sysbench | MaxFullLoadSubTasks + ParallelLoadThreads/BufferSize | https://aws.amazon.com/blogs/database/understand-and-optimize-replication-for-amazon-redshift-with-aws-dms/ |
| **CDC:** 0.5M inserts + 2.5M updates. Transactional apply 3.5 h, target latency ~198 min, max 372 KB/s. Batch apply ~7 min, latency spike 108 s, max 25,000 KB/s (derived ~240 → ~7,100 changes/s, ~30x) | dms.r5.large | BatchApplyEnabled | https://aws.amazon.com/blogs/database/aws-dms-key-troubleshooting-metrics-and-performance-enhancers/ |
| Same post, full load of 5 GB / 10M rows: full LOB mode ~33 min vs limited LOB <9 min; no parallelism ~22 min vs 10 threads 8 min | dms.r5.large | limited LOB mode, parallel load | same |
| Db2 full load, 75 GB / 9 tables: 4h32m (~4.6 MB/s) → 3h46m30s (−19.46%) | Db2 → RDS for Db2 | MFLST=49, CommitRate=50,000, CreatePkAfterFullLoad | https://aws.amazon.com/blogs/database/performance-optimization-of-full-load-and-ongoing-replication-tasks-from-self-managed-db2-to-amazon-rds-for-db2/ |
| Db2 CDC, default settings: source/target 76,642 / 34,140 rows/s, latency 807 / 1,257 s | same | batch apply on | same |
| Db2 CDC, 3 split tasks on dms.r6i.8xlarge: target 29,543 / 9,511 / 84,326 rows/s | same | task split + BatchSplitSize and batch timeouts | same |
| **Serverless:** Oracle → Redshift "two to ten times faster"; Oracle → S3 "up to two times faster" | automatic | auto-segmentation (ROWID) | https://aws.amazon.com/about-aws/whats-new/2024/05/aws-dms-serverless-oracle-redshift-full-load-throughput and https://aws.amazon.com/about-aws/whats-new/2024/11/aws-dms-serverless-oracle-s3-full-load-throughput/ |

The Serverless benchmark post (1,146 GB and 5,694 GB tables, 128 DCU vs dms.r6i.32xlarge) shows results only in charts, with no absolute times in the text: https://aws.amazon.com/blogs/database/enhanced-full-load-performance-in-aws-dms-serverless/

## 2. Alibaba DTS and Tencent DTS
**Alibaba, maximum incremental RPS per class** (1 KB rows, <2 ms latency, not SLA):
- Migration: small 2,000 / medium 5,000 / large 6,000 / xlarge 7,000 / 2xlarge 11,000.
- Measured RPS for small/medium/large falls as latency grows:
  - 0.26 ms: 2,566 / 4,726 / 6,378
  - 175 ms: 1,753 / 2,837 / 3,884
  - 198 ms: 1,104 / 1,724 / 2,256
- Test setup: rds.mys2.8xlarge, 20 tables, 1 KB rows.
- Source: https://www.alibabacloud.com/help/en/dts/product-overview/specifications-of-data-migration-instances
- Sync: micro <200 / small 2,000 / medium 5,000 / large 11,000 / xlarge 17,000 / 2xlarge 34,000 / 4xlarge 68,000 / 6xlarge 102,000 / 8xlarge 136,000. Source: https://www.alibabacloud.com/help/en/dts/product-overview/specifications-of-data-synchronization-channels

**Alibaba performance white paper** (RDS MySQL 8.0, 8 cores / 32 GB; sysbench 1.0.20): https://www.alibabacloud.com/help/en/dts/support/performance-white-paper
- **Full sync, oltp_write_only** (10 × 10M rows): 179,500–199,600 rows/s, 34.2–38.0 MB/s, ~500–557 s from micro to large. Full load barely changes with instance class.
- **Full sync, TPC-C** (1000 warehouses): 120,400–135,000 rows/s, 17.5–19.6 MB/s.
- **Incremental:** hits the class cap exactly (e.g. large 11,000 rows/s ≈ 5.5 MB/s).
- **Incremental with large rows:** does not reach the cap (large: 1,537 rows/s at 84 MB/s).
- **Hot-row updates:** stuck at 1,200 RPS unless `trans.hot.merge.enable=true`, which then reaches the cap.
- **DDL:** 68 ops/s.

**Tencent DTS:**
- Migration spec: small 2,000 / medium 5,000 / large 6,000 / xlarge 7,000 / 2xlarge 11,000 RPS. Test: 16-core source and target, 20 tables × ~2M rows, 1 KB rows, INSERT:UPDATE:DELETE 1:1:1. Tencent says every spec reached its limit, cross-region included. Source: https://www.tencentcloud.com/document/product/571/60014
- Sync spec: micro 1,000 / small 2,000 / medium 5,000 / large 11,000. Source: https://cloud.tencent.com/document/product/571/81780
- The tuning guide lists sync large as ">5000", which conflicts with 11,000 above.
- The tuning guide treats full export/import of 20–50 MB/s as normal (derived ~72–180 GB/h). It flags same-region latency above 3 ms as high, and incremental RPS below 60% of the spec as a bottleneck. Source: https://cloud.tencent.com/document/product/571/96723

## 3. Oracle GoldenGate
Nothing retrieved. Oracle blog and whitepaper URLs returned 403, doc URLs returned 404, and the search budget was gone.

## 4. Qlik Replicate
No published throughput number found. Search was partial (budget exhausted).

## 5. pgcopydb
- **PlanetScale:** "speeds as fast as 2 TB per hour using Metal" (derived ~555 MB/s). Their defaults are 16 table jobs, 12 index jobs, and splitting tables larger than 50 GB. Source: https://planetscale.com/docs/postgres/imports/postgres-migrate-pgcopydb
- **Azure PG migration service:** offline migration on D4ds_v4 (4 cores): 1 GB 00:01, 10 GB 00:08, 100 GB 01:00, 500 GB 04:00, 1,000 GB 07:00 (derived ~143 GB/h). Tables over 20 GB with an int or bigint PK are split and copied in parallel. The page says online mode uses `pgcopydb follow`; it does not say the benchmark itself used pgcopydb. Source: https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql
- **Medium "lessons from the trenches":** a search snippet showed 1d16h wall clock vs 24d15h cumulative COPY. The page returned 403, so I could not verify it or the dataset size. Source: https://medium.com/@r.soldin/migrating-large-postgresql-databases-to-rds-lessons-from-the-trenches-b4a6cf9af7a9
- **Dimitri Fontaine / tapoueh and the pgcopydb docs:** no TB/h figure and no numeric comparison with pg_dump|pg_restore. The docs credit three things:
  - streaming COPY with no intermediate files
  - same-table concurrency, which costs the TRUNCATE + COPY FREEZE in one transaction
  - building the PK index concurrently (`CREATE UNIQUE INDEX` then `ADD CONSTRAINT ... USING INDEX`)

  v0.18 (July 2026) adds interleaving of COPY and CREATE INDEX across databases, still with no numbers.
  - https://pgcopydb.readthedocs.io/en/latest/concurrency.html
  - https://tapoueh.org/blog/2026/07/pgcopydb-v0.18/

## 6. pg_dump -j / pg_restore -j
No published number found. The Postgres docs only say it "may reduce the time". The AWS best-practices blog is qualitative: set jobs ≤ vCPUs, give each restore job its own maintenance_work_mem, raise max_wal_size and checkpoint_timeout.
- https://www.postgresql.org/docs/current/app-pgdump.html
- https://aws.amazon.com/blogs/database/best-practices-for-migrating-postgresql-databases-to-amazon-rds-and-amazon-aurora/

## 7. MySQL Shell dump & load
- **Kenny Gryp, Part 2 benchmarks:** the text gives only "up to almost 3GB/s" dump and "above 200MB/s" load with the redo log disabled (derived ~10.8 TB/h and ~720 GB/h). Per-tool GB/h is in chart images only.
  - Setup: OCI BM.Standard.B1.44 (88 threads), 512 GB RAM, MySQL 8.0.21, `ALTER INSTANCE DISABLE INNODB REDO_LOG`, 88 threads, 256 MB chunks.
  - Datasets: stackoverflow 216 GB, wikipedia 130 GB, ontime 64 GB, all 410 GB.
  - Shell was fastest except wikipedia dump, where mydumper won because Shell base64-encodes binary columns.
  - https://dev.mysql.com/blog-archive/mysql-shell-dump-load-part-2-benchmarks/
- **Techniques in Part 3:** LOAD DATA LOCAL INFILE format, chunked parallel load with dynamic scheduling, `deferTableIndexes` (default `fulltext`), redo log plus doublewrite disabled, loading while the dump is still running, resumable load. https://dev.mysql.com/blog-archive/mysql-shell-dump-load-part-3-load-dump/
- **lefred:** 34.64 GB dumped in 1m49s, 317 MB/s uncompressed with zstd, on VM.Standard2.4 (8 cores) with 4 threads.
  - gzip ran at 162 MB/s (3m33s).
  - mysqldump took 15m20s with zstd and 21m12s with gzip (derived 8–12x slower).
  - https://lefred.be/content/mysql-shell-dump-load-and-compression/
- **Pythian, small 2-core / 4 GB box, 6.39 GB:** restore took 10m16s with Shell vs 3h59m with mysqldump. https://www.pythian.com/blog/technical-track/exploring-backup/restore-comparison-using-mysql-shell-utility-vs.-mysqldump-vs.-xtrabackup

## 8. mydumper / myloader
- **Percona, m5dn.8xlarge (32 vCPU), 177 GB, MySQL 8.0.26, 16/32/64 threads:** backup ranking is mydumper+zstd > MySQL Shell > XtraBackup > mysqlpump > mydumper+gzip > mysqldump. Timings are in charts only.
  - https://www.percona.com/blog/dump-performance-comparison-mysqldump-vs-mysql-shell-utilities-vs-mydumper/
  - Conclusion post (backup + restore): XtraBackup fastest overall, mydumper/myloader and Shell good in both phases. Tuning options it names: `--rows`, `--chunk-filesize`, `--innodb-optimize-keys`. https://www.percona.com/blog/backup-restore-performance-conclusion-mysqldump-vs-mysql-shell-utilities-vs-mydumper-vs-mysqlpump-vs-xtrabackup/
- **Percona, David Ducos:** "up to 40% on large tables with multiple secondary indexes" from fast index creation plus CSV / LOAD DATA. https://www.percona.com/blog/back-from-a-long-sleep-mydumper-lives/

## 9. TiDB Lightning
- **Physical mode:** "100~500 GiB/hour". **Logical mode:** "10~50 GiB/hour". https://docs.pingcap.com/tidb/stable/tidb-lightning-overview/
- **Physical-mode limits:** one instance handles up to 10 TiB; 32+ cores, 64+ GiB RAM and 10 Gbps recommended; about 2 GiB per region-concurrency. https://docs.pingcap.com/tidb/stable/tidb-lightning-physical-import-mode/
- **Parallel import:** 10 TiB with 5 instances cut from ~40 h to ~10 h (derived ~256 GiB/h per instance). https://docs.pingcap.com/tidb/stable/tidb-lightning-distributed-import/
- **IMPORT INTO:** 10 TiB per task, or 40 TiB with Global Sort; no speed figure given.

## 10. Vitess VReplication / MoveTables
- No published copy rate found. The Vitess 17–21 changelogs and blog have no numbers.
- **PlanetScale imports (built on Vitess):** "often 2-3x faster for tables with multiple indexes" with deferred secondary indexes, which is on by default. https://planetscale.com/docs/vitess/imports/database-imports
- **Tuning flags and defaults:** copy-phase-duration 1h, vstream-packet-size 250000, parallel-insert-workers 1, experimental-flags 7 (includes vplayer batching). https://vitess.io/docs/reference/vreplication/flags/

## 11. MongoDB mongosync / mongodump / mongorestore
- **mongosync:** no published GB/h in the docs, FAQ or release notes. The docs tell you to measure it yourself from `/progress` `estimatedCopiedBytes`. Other documented figures:
  - two round trips to the destination for each source operation
  - destinations with ≤4 vCPU and ≤64 GB of data get lower write concurrency
  - indexes are built after collection copy by default since 1.17, and on Atlas 12 `createIndexes` run concurrently
  - 90 chunks per destination shard
  - the embedded verifier needs 10 GB base memory plus 0.5 GB per 1M documents

  Sources:
  - https://www.mongodb.com/docs/mongosync/current/reference/mongosync-behavior/
  - https://www.mongodb.com/docs/mongosync/current/release-notes/1.17/
  - https://www.mongodb.com/docs/mongosync/current/release-notes/1.9/
- **mongorestore:** `--numParallelCollections` defaults to 4 and `--numInsertionWorkersPerCollection` to 1. No benchmark numbers are published. https://www.mongodb.com/docs/database-tools/mongorestore/

## Patterns across the numbers
- **Parallelism gives 2–4x in the AWS cases:** DMS ranges or segments, and more concurrent table loads with MaxFullLoadSubTasks.
- **Batch apply beats per-transaction CDC:** ~30x (DMS), and Alibaba's hot-row merge goes from 1,200 RPS to the class cap.
- **Deferred secondary indexes:** 2–3x (PlanetScale) and up to 40% (mydumper).
- **zstd over gzip:** ~2x (lefred, Percona).
- **Latency cuts CDC RPS:** roughly 40–65% lower at 175–198 ms (Alibaba table).
- **Instance class barely changes full load, but caps incremental exactly** (Alibaba white paper).

To go further on GoldenGate, Qlik, Vitess copy rate and pg_dump -j, raise `CLAUDE_CODE_MAX_WEB_SEARCHES_PER_SESSION` and I can rerun those searches.
