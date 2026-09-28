# How the mature replication products work inside, and what migkit has of each (research as of 2026-09-28)

Status: complete for every section (0-9) and every product named in
scope. Thinnest, because the vendor publishes least: SharePlex's post-queue
file layout and Post's dependency algorithm (3.2), Arcion's snapshot-to-
realtime handoff (4.1), Precisely's Apply Engine internals (2.3). Nothing
was run; every migkit status was read from the code at the lines given.

Scope: the internal mechanisms - algorithms, data structures, file
formats, protocols, recovery logic - of Oracle GoldenGate (with Veridata
and ZDM), Qlik Replicate, Fivetran HVR/LDP, IBM InfoSphere Data
Replication (CDC engine and Q Replication), Quest SharePlex, Precisely
Connect CDC (SQData), Informatica PowerExchange CDC and Cloud Database
Ingestion, Striim and Arcion. Read from each vendor's own documentation
and from field write-ups where the documentation is silent; nothing was
run. Every mechanism ends with migkit's status - **has**, **partly**,
**lacks** - against the code as of this date, with file and line, and
with how migkit does or could build the same thing better: from measured
facts about the tables and servers in front of it, never as a mode an
operator picks. The closing table collects it.

Where a sibling page already covers a mechanism
(`docs/research-other-tools.md`: HVR Compare, Striim Validata, Qlik
batch-optimized apply in outline; `docs/research/throughput-cdc-techniques-2026-09-27.md`),
this page adds the internals and does not repeat the outline.

## 0. The shape of each product, in one line each

* **GoldenGate**: Extract reads the source's redo (itself, or through the
  database's logmining server) and writes committed transactions to
  *trail files*; a pump ships trails; Replicat reads trails and applies,
  in classic, coordinated, integrated or parallel mode, keeping its
  position in a *checkpoint table* on the target.
* **Qlik Replicate**: one server process per task; a source endpoint
  reads the log into a *sorter* that holds transactions until commit; a
  target endpoint applies in *transactional* or *batch-optimized* mode
  through a *net changes* table; control tables `attrep_*` on the target.
* **HVR**: capture jobs at the source write compressed *router
  transaction files* on a *hub*; integrate jobs read them and apply in
  *continuous* or *burst* mode; *state tables* on the target hold the
  applied position; compare and refresh jobs use the same agents.
* **IBM Q Replication**: Q Capture reads the Db2 log and puts
  transactions on IBM MQ *send queues*; Q Apply's *browser* thread reads
  the *receive queue* and hands transactions to *agent* threads that apply
  in parallel with dependency analysis; control tables `IBMQREP_*`.
* **IBM CDC (IIDR CDC engine)**: a *scrape* (log reader) shared across
  subscriptions through a *staging store* with per-subscription
  *bookmarks*; *mirror* (continuous) and *refresh* (bulk or
  differential); *standard* or *adaptive* apply; *fast apply* modes.
* **SharePlex**: Capture -> capture queue -> Read -> export queue ->
  Import -> post queue -> Post, all file-backed queues; `compare`,
  `repair` and `reconcile` commands ride the same queues.
* **Precisely Connect CDC (SQData)**: capture agents on z/OS (Db2, IMS,
  VSAM) publish over TCP to an *Apply Engine* (scripted transforms, any
  target) or a *Replicator Engine* (Kafka, partitioned by root key).
* **Informatica PowerExchange CDC**: the *PowerExchange Logger* (`pwxccl`)
  condenses committed units of work from the source log into its own log
  files; consumers restart from a *restart token pair*; *CDC Publisher*
  streams those files to Kafka with per-message checkpoints. Cloud
  Database Ingestion keeps its position in `INFORMATICA_CDC_RECOVERY` on
  the target.
* **Striim**: a streaming platform (readers, streams, windows, continuous
  queries, writers) with periodic *recovery checkpoints*; a
  *DatabaseWriter* keeps exactly-once through a `CHKPOINT` table on the
  target.
* **Arcion (now Databricks)**: *Replicant* with per-engine *extractor*
  and *applier*, four modes (`snapshot`, `realtime`, `full`,
  `delta-snapshot`), parallel extraction by `split-key`, resume with
  `--resume`.

## 1. Capture

### 1.1 GoldenGate Extract: classic and integrated

*Problem*: read every committed change from the redo stream without
loading the database.

*Classic capture* reads the online and archived redo files itself from
disk (ASM needs `TRANLOGOPTIONS DBLOGREADER` or an ASM user), buffers each
transaction's changes in memory, writes them to the trail when it reads
the COMMIT and discards them on ROLLBACK; TDE needs key exchange;
deprecated from 18c. *Integrated capture* registers with the database
(`REGISTER EXTRACT`), which starts a *logmining server* (the Streams
infrastructure, `ora_capt`) inside the instance; the server mines redo,
handles RAC, ASM, TDE and multitenant itself, delivers *logical change
records* (LCRs) through the Streams pool, and RMAN will not delete redo
that `DBA_CAPTURE` says is still needed. `TRANLOGOPTIONS
ASYNCTRANSPROCESSING n` splits integrated Extract into two threads - one
grouping LCRs into transactions and filtering, one formatting committed
transactions and writing the trail - joined by a buffer of `n` committed
transactions (default 300). A *downstream* deployment runs the logmining
server on another database fed by redo transport.

*Limits* documented: classic cannot read a CDB; integrated needs 11.2.0.3+
and Streams pool memory; the SQL (LogMiner) read path in other tools
(HVR's `LogReadMethod=SQL`) tops out at single-digit GB of redo an hour.

*migkit*: **not applicable by design** for Oracle (Oracle is a "through
the pair" engine, `migkit/engines/oracle.py`, no log reader); for the
engines migkit tails, the server's own decoder is the logmining server:
PostgreSQL logical decoding through a slot (`migkit/engines/postgres.py:1350`),
MySQL binlog through `pymysqlreplication` (`migkit/engines/mysql.py:762`),
SQL Server CDC tables (`migkit/engines/mssql.py:895`), MongoDB change
streams (`migkit/engines/mongodb.py:558`). All four deliver committed
changes only, in commit order, which is the property integrated Extract
buys with the Streams pool.

### 1.2 Committed-only capture and the open-transaction cache

*Problem*: a reader that emits only committed transactions must hold
every open transaction's changes until its commit; a long transaction
makes that memory large and, after a crash, makes recovery re-read the
log from the oldest open transaction's start.

*GoldenGate*: `CACHEMGR` owns a virtual-memory cache (64 GB default on
64-bit) for uncommitted data; past `CACHESIZE` it pages transactions to
`.cm` files in `dirtmp` (a dedicated disk is advised, `CACHEDIRECTORY
dir size` caps it); past `CACHESIZEMAX` any transaction needing buffers is
paged. Extract keeps three read checkpoints: *startup* (where it began),
*recovery* (the oldest open transaction's start), *current* (the last
record read). **Bounded Recovery** (`BR BRINTERVAL 4h` default, a multiple
of `CHECKPOINTSECS`) writes, at each interval, a BR checkpoint plus one
*persisted-object file* per transaction open longer than one interval,
under `BR/<group>/`; recovery then starts within the last interval instead
of at the recovery checkpoint. A transaction is persisted only when it is
older than one interval *at the next BR checkpoint*, so worst case it is
just under two intervals old - which is why Oracle says keep eight hours
of archive logs for a four-hour interval. Oracle-only; on other sources
Extract re-reads from the oldest open transaction. Failure modes listed:
corruption or deletion of the BR files, which "corrupt the continuity of
the environment". `SEND EXTRACT ... FORCETRANS xid` writes an open
transaction to the trail as if committed, without committing it at the
source. Recovery of the trail itself is `RECOVERYOPTIONS APPENDMODE`: on
restart Extract reads the trail to find the last complete transaction it
wrote, reads the source until that transaction's commit, then appends.

*Qlik Replicate*: the **sorter** stores each open transaction in memory and
offloads it to a `.tswp` file per transaction under `tasks/<task>/sorter`
when *total transactions memory* passes 1024 MB or a transaction has been
open longer than 60 s (`Transaction Offload Tuning`); on commit the sorter
sends the transaction to the target through the *outgoing stream queue*
and deletes the file once the target acknowledges its commit. Logging
shows `SORTER_STORAGE ... Swap committed transaction to free memory`. A
restart re-reads the source log from the oldest open transaction; the
files are not a recovery checkpoint.

*HVR*: `Capture_Checkpoint_Frequency` (default every 5 minutes) writes the
in-memory state of transactions open longer than the interval to
`capckp/`, reusing earlier files for the older part of a still-open
transaction, retained under `capckpretain/`; on restart the latest
complete checkpoint is the start point, else the oldest open transaction.
Not supported for SQL Server. HVR warns that stopping capture for long
means the online redo is overwritten and archive logs may be gone, which
forces a refresh.

*Informatica PowerExchange*: the **Logger** (`pwxccl`) writes only
successful units of work, in commit-end order, to its own condense files
indexed by a CDCT file; a consumer restart goes to the Logger files, not
back to LogMiner. Starts are *cold* (from `RESTART_TOKEN`/`SEQUENCE_TOKEN`
in `pwxccl.cfg`, or end of log when absent, or the earliest available
point when zeros), *warm* (from the CDCT file), or *special* (tokens set
to skip a bad stretch of log). The restart token is a binary position
that "might contain the position of the oldest open UOW".

*migkit*: **server-side, measured, not built**. PostgreSQL's reorder
buffer holds open transactions and spills past
`logical_decoding_work_mem`; the slot's `restart_lsn` is Extract's
*recovery checkpoint* and the WAL between it and the head is what a long
transaction costs - `stream_room` reports it as `held_bytes` and the
`safe_wal_size` left (`migkit/engines/postgres.py:1311`), and a slot that
went `lost` stops the tail before it reads (`tailctl.same_source`,
`migkit/tailctl.py:174`, `test_a_lost_position_stops_the_tail.py`).
MySQL's binlog is written at commit, so the reader never sees an open
transaction; the position is kept only where a transaction ended, with
`skip_rows` for a batch that stops inside one (`migkit/engines/mysql.py:816`
onward). What migkit lacks is Bounded Recovery's *reason*: it never names
the oldest open transaction on the source as the thing holding the slot's
`restart_lsn` back. The fact is one query (`pg_stat_activity.xact_start`,
`pg_replication_slots.restart_lsn` against `confirmed_flush_lsn`); saying
"the slot holds 9 GB because transaction pid 4711 has been open 3 h"
beside `held_bytes` is a measured line in `assess`/`/metrics`, effort S,
docker-testable with an idle-in-transaction session.

### 1.3 Keeping the source's log until it is read

*Qlik Replicate on SQL Server*: with *Start transactions in the database*
on, Replicate creates `attrep_truncation_safeguard` on the source and
keeps two uncommitted UPDATEs on it open ("Latch Lock A and B", refreshed
every 5 minutes by *Apply TLOG truncation prevention policy every*), so
the log cannot truncate past the reader; documented cost is log bloat when
the reader falls behind, and a task stopped for long loses its LSN. The
table must be excluded from maintenance plans. *GoldenGate* integrated
Extract relies on RMAN honouring `DBA_CAPTURE`; classic has no hold and
the archive deletion policy must be set by hand. *HVR* has no hold either.

*migkit*: **partly**. It measures instead of holding: `stream_room` per
engine (`migkit/engines/postgres.py:1311`, `mysql.py:1009`,
`mongodb.py:456`) becomes `migkit_tail_retention_margin_*`; `assess` names
binlog retention below 24 h (`mysql.py:4542`) and SQL Server CDC's
`change_retention` (`mssql.py:590`). A PostgreSQL slot is the hold itself.
On SQL Server a held-open pseudo-transaction is the only hold there is and
migkit does not do it; on MySQL nothing can hold the binlog. Better as is:
the margin is a number the operator sees before it is a problem, and a
slot lost is refused rather than silently resumed.

### 1.4 Values the log does not carry

*Problem*: an UPDATE's log record may omit unchanged large values
(PostgreSQL TOAST under the default replica identity, Oracle LOBs without
supplemental logging, SQL Server off-row values).

*GoldenGate*: `FETCHCOLS` / `FETCHCOLSEXCEPT` on the `TABLE` statement fetch
missing columns from the source; on Oracle the fetch goes through
*Flashback Query* to the undo tablespace for a read-consistent image as of
the record's SCN (`FETCHOPTIONS USELATESTVERSION` falls back to the
current row, `NOUSELATESTVERSION` does not; `MISSINGROW` says what to do
when the row is gone; `USEROWID` before `USEKEY`); `NOFETCH` writes a token
saying the column is missing. `TRANLOGOPTIONS FETCHPARTIALLOB` fetches a
whole LOB when redo holds only the changed fragment. Oracle's own note:
supplemental logging of the columns is usually cheaper than fetching.

*Qlik Replicate*: three modes. *Limited LOB size* replicates inline and
truncates past the limit (structured LOBs become invalid); memory is
`limit x commit rate x LOB columns` (the docs' example: 5 MB x 10,000 x 6 =
300 GB). *Unlimited* does a *source lookup* per LOB by primary key or unique
index (so keyless tables lose LOB columns entirely), one LOB at a time,
which the tuning article names as a throughput ceiling. Inline LOBs from
SQL Server are read from the log; from MySQL, Replicate still looks up.
Batch-optimized apply allows LOBs only in limited mode.

*Debezium* (for contrast): emits `__debezium_unavailable_value` as a typed
sentinel and offers `ReselectColumnsPostProcessor` to re-query the source
by event key, with a documented latency cost.

*migkit*: **has**, without fetching. A change that carries only some
columns is merged later-over-earlier in `_collapsed`
(`migkit/engines/base.py:3917`, `canon.merged_values`), and the upsert
statement names only the columns the run carries (`postgres.py:1102`,
`names = sorted(full[0])`, runs grouped by column set in `_apply_net`
`base.py:3834`), so an unchanged TOAST value on the target is left as it
is - the same outcome as a fetch, with no query. MySQL is required to log
`binlog_row_image=FULL` (`mysql.py:790`), so every value is inline. Two
gaps: a *new* row on the target whose only source of a large value was a
change the tail skipped (a two-way `keep_local`) - not a case that arises;
and no read-consistent fetch exists for the day a source cannot log
whole rows, which is when the Debezium sentinel (marked, not NULL) is the
honest answer. migkit's `_flatten_changes` already counts values "not
there on the source" landed as NULL (`hetero.py:1686`); counting a column
omitted-because-unchanged separately from omitted-because-absent is a
log line, effort S.

### 1.5 Reading the log once, and reading it in parallel

*IBM CDC shared scrape*: one log reader per instance parses the log into a
*staging store*; each subscription holds a *bookmark* into it; a
subscription whose bookmark falls behind the store's oldest position is
"kicked out" to a private scrape until it catches up; an inactive
subscription with mirrored tables keeps accumulating in the store, so IBM
says delete it or set its tables to refresh. *Striim* "read once, stream
anywhere": one reader into a *persistent stream* (Kafka-backed), each
consumer application with its own checkpoint. *Informatica CDC staging*:
one *CDC Staging Task* reads the log once into cloud storage for many
ingestion tasks. *Arcion*: `realtime.threads` for extraction and applier
threads (`-r 2:2` in the demo kit), a native Oracle redo reader, and the
claim that no pipeline step is single-threaded. *HVR*: `LogReadMethod`
`DIRECT` (file I/O on the log, needs an agent on the host and SA/sysadmin)
against `SQL` (LogMiner or `fn_dblog`, slower, no agent).

*migkit*: **partly**. One reader, one applier, the next batch read while
the current one is applied (`_ReadAhead`, `hetero.py:3196`, MySQL only
because a PostgreSQL slot's position is an acknowledgement); measured, the
binlog reader decodes 116,000 changes a second and the applier is the
limit (backlog R2.6). One-to-many topologies and a spool many tails read
are R3/R19 items, not built. Parallel *decoding* of one log is not worth
it before the applier passes the reader; migkit's rule is to measure that
first (R2.6: PyPy, then a compiled sidecar handing Arrow batches).

### 1.6 Sources with no readable log: Arcion delta-snapshot

Arcion's fourth mode polls a table by a monotonic, non-repeating numeric or
timestamp column (`delta-snapshot-key`, later `delta-snapshot-keys`, any
of which triggers a row) every `delta-snapshot-interval` seconds, with a
separate `delta-snapshot-delete-interval`; deletes need their own
condition. *Limits*: no deletes without a soft-delete column, no
before-images, no transaction boundaries, misses a row whose key column
is not updated.

*migkit*: **lacks**, and the sibling page already lists a time-window
verify as worth adding. As a change source it would be a rung of the
decision layer for an engine with no log migkit can read (Oracle and Db2
today), chosen from catalogue facts (a `NOT NULL` timestamp with an index,
a soft-delete column), never asked for; effort M; docker-testable on
PostgreSQL by refusing the slot.

## 2. Transport and the durable spool

### 2.1 GoldenGate trail files

*Format*: a trail is a sequence of files (`<prefix>000000001`...), each
beginning with a *file header* record written in a token format for
forward and backward compatibility (new tokens ignored by old readers,
deprecated tokens defaulted; the `COMPATIBILITY` token names the version,
`FORMAT` on `EXTTRAIL`/`RMTTRAIL` pins it); `FILEHEADER DETAIL` in Logdump
shows the token tree: `TrailInfo`, `MachineInfo`, `DatabaseInfo`,
`ProducerInfo`, `ContinuityInfo`, and `FirstCSN`/`LastCSN` for the file's
span. Each change record has a header (GHDR: I/O type, I/O time = source
commit timestamp, record length, before/after flag, table name,
`AUDITRBA`/`AUDITPOS` = position in the source log, transaction
begin/middle/end indicator), a variable-length data area, an undocumented
internal token area (GGS tokens seen in dumps: `ORAROWID`, `LOGCSN`,
`TRANID`) and an optional user token area (`@TOKEN`). A record's address is
its *RBA* (byte offset) in file *SEQNO*; `POS`, `SFH` (scan for header) in
Logdump recover a position in a damaged file. Only committed transactions
are written; a pump copies records untouched unless mapping is asked.

*Encryption*: `ENCRYPTTRAIL AES128|AES192|AES256`. Wallet method: a
one-time *data encryption key* per trail file, wrapped by the *master key*
(ANS X9.102 key wrap) and stored in the file header; Replicat decrypts
with the shared wallet and no parameter (`DECRYPTTRAIL` needs no cipher
because the header names it); master key renewal creates a new version,
all systems must show the same `INFO MASTERKEY VERSION`; the wallet is
copied to every deployment. `ENCKEYS` method: a static named key in a file
secured by permissions, `DECRYPTTRAIL AES256 KEYNAME k` required on
Replicat; a pump decrypts and re-encrypts only when told.

*What it buys*: the reader and applier run at their own speeds; a target
down for a day costs trail disk, not source log retention; one trail feeds
many Replicats; a corrupt record is skippable by RBA.

*migkit*: **lacks by design, half-planned**. Changes go from
`neutral_changes` to `neutral_apply` in one process; the only durable
artefact is the position (`token_path`) and, for streaming targets,
messages in `json | debezium | canal | avro` shapes (`migkit/streamout.py`,
Kafka with `acks="all"`, `migkit/engines/kafka.py:98`). R19.7 puts the
reader beside the source and the applier beside the target with whole
batches compressed between them; that link is the trail's job without the
file. The measured trigger for a file would be `stream_room` shrinking
while the applier is behind: spill read-ahead batches to local disk
(format: migkit's own neutral records, one file per batch, position and
batch number in the name) and advance the source position from the
spill's tail rather than the applier's. That is a lever, not a mode; effort
M; docker-testable with a paused target (`docker pause`) and a short
`binlog_expire_logs_seconds`. Encryption of that spill would follow the
wallet pattern (a per-file key wrapped by a key migkit never writes to
disk) - not needed until the spill exists.

### 2.2 HVR's hub: router files, state tables, sequence numbers

Capture jobs write compressed, optionally encrypted changes into *router
transaction files* under
`$HVR_CONFIG/router/<hub>/<channel>/loc_<src>/loc_<tgt>`, with `.enroll`,
`.cap_state` and control files beside them (`hvrrouterview` reads all of
them); integrate jobs consume and delete them (`JournalRouterFiles` keeps
them). On the target, **state tables** `hvr_stbu_<chn>_<loc>` (burst),
`hvr_stin` (integrate) and `hvr_stis` hold commit time and transaction
information, "updated every time HVR applies transactions ... to ensure no
transactions are lost but none are applied more than once", and are the
recovery point for a hub failover ("Recovery Rewind to Target Databases'
Integrate Sequence"). `{hvr_integ_seq}` combines the source LSN with the
change's order inside its transaction so it sorts alphabetically;
`{hvr_tx_seq}` on Oracle is `SCN*65536`. `hvractivate -i time` rewinds
capture; `-I scn=` (emit) delays sending until a position, so a long
transaction opened before the capture start is still whole. Only
differences that persist across runs of an online compare are real. HVR
warns a *reset* of capture (as against a rewind) loses tracked open
transactions and forces a refresh.

*migkit*: **partly**. The state-table idea is `migkit_origin`'s `seen`
column written in the batch's own transaction for `exact` hops
(`postgres.py:1222`, `twoway.batch_seen`, `twoway.committed_ahead`
`twoway.py:150`, `_committed_ahead` `hetero.py:2672`); one-way hops keep
the position in a file after the batch commits and replay one batch on a
crash, which the appliers are idempotent for (`tail_apply`,
`hetero.py:2919`). Emit-after-capture-start is `tail_start` before the
copy (`hetero.py:2939`): the slot exists before the snapshot, so nothing
between them is lost (`test_full_cdc_misses_nothing.py`). The router file
is the spool of 2.1.

### 2.3 Queues: MQ, file queues, Kafka partitions, guaranteed delivery

*Q Replication*: Q Capture puts each transaction as one or more MQ
messages on a *send queue*; MQ moves them to the *receive queue*; a
*replication queue map* binds the two with parameters (`num_apply_agents`,
memory limit); MQ persistence is the durability. Messages are batched by
`max_message_size` and large transactions split; MQ full conditions make
Q Capture retry (APAR PK39425). *SharePlex*: capture, export and post
queues are file-backed on their hosts; a maximum of 20 processes may use
the post queue at once, compare/repair included; `SP_QUE_POST_SHMSIZE`
sizes the shared-memory window; `SP_OPO_READRELEASE_INTERVAL` controls
how often Post releases queue space after commits. *Precisely*: IMS/Db2
change records stream to Kafka topics partitioned by root key so Apply
Engine consumer groups keep per-record order; Avro schemas generated and
updated on DDL. *Informatica CDC Publisher*: one checkpoint file per
instance updated after each change; `kafkaProducerGuaranteeDelivery=true`
writes the checkpoint only after Kafka acknowledges each message - no loss
and no duplicates, slower; `false` may re-send after a Kafka crash;
v1.3 can keep the checkpoint in Kafka topics themselves, and a clean
`SHUTDOWN` syncs the backup file or duplicates follow.

*migkit*: **lacks a queue by design; has the ordering**. Streaming targets
partition by the row's key (`streamout.py`, `partition_by`), which is
Precisely's root-key rule; `acks=all` is the durability. A message larger
than `max_message_bytes` is skipped and counted, not sent. Exactly-once
into Kafka (idempotent producer, or a transaction per batch with the
position in it) is not claimed; the position is saved after the batch
sends, so a crash between them re-sends a batch - duplicates, as
Informatica's `false` setting. Measured lever: a Kafka transaction per
batch carrying the position as a message on a state topic, resumed from
the topic as CDC Publisher 1.3 does; effort M; docker-testable with a
Kafka container and a failpoint between send and save
(`failpoint.hit("tail.applied")`, `hetero.py:3062`).

## 3. Apply

### 3.1 Grouping statements and transactions

*GoldenGate BATCHSQL*: Replicat queues similar operations (same table,
operation type and column list) in memory and executes each batch as one
array operation, each statement prepared once and cached
(`MAXSQLSTATEMENTS`, LRU). It analyses foreign-key dependencies across
batches first, and may need more than one statement per batch to keep
them. Excluded: LOB/LONG columns, rows over 25 KB, tables with a unique key
besides the primary key (ordering not guaranteed if such values move),
SQL Server tables with triggers. Sizing: `BATCHTRANSOPS` (operations per
target transaction, default 1000 non-integrated / 50 integrated),
`OPSPERBATCH` 1200, `BYTESPERQUEUE`, `BATCHESPERQUEUE` 50, `OPSPERQUEUE`.
*Error ladder*: on an error the batch rolls back and Replicat retries in
*normal mode* (one statement at a time within `GROUPTRANSOPS` boundaries),
then *source mode* (the source's own transaction boundaries), then returns
to BATCHSQL; `BATCHERRORMODE` instead converts duplicate-insert to update
and ignores missing-delete inside the batch (with `HANDLECOLLISIONS`).
Oracle's own numbers: 400-500% on small rows, nothing past 5 KB rows.
`GROUPTRANSOPS` (default 1000) is a *minimum*: whole source transactions
are grouped until it is passed, or the trail runs dry; it cuts
checkpoint-table writes. `MAXTRANSOPS` splits a large source transaction
into target commits of n operations - documented as unsafe: after a crash
mid-transaction the committed part replays as duplicate-row or
missing-row errors.

*Qlik batch-optimized apply*: changes cached per source transaction in
memory (dropped on rollback), repeated changes to a row updated in place;
pre-processing groups transactions into batches "in the most efficient
way, which may affect transactional integrity" and merges by key
(DELETE+INSERT -> UPDATE, INSERT+DELETE -> ignored; the store-changes
tables still record all of them). On commit the batch is bulk-loaded into
the *net changes* table `attrep_changes<hash(task)>` (one for the task, or
one per table on some targets) whose columns are string representations of
every column of every table in the batch plus counters and keys; it is
truncated per batch. From it: bulk DELETE, then INSERT, then UPDATE per
table, or one `MERGE` per table with *Apply changes using SQL MERGE*
(three statements to one; `Optimize inserts` sends plain INSERTs when a
batch is inserts only). Batches are bounded by *Longer than* 1 s / *But
less than* 30 s and *Force apply when processing memory exceeds* 500 MB;
*Limit the number of changes applied per change processing statement*
10,000; *Apply batched changes to multiple tables concurrently* (2-50,
default 5, Snowflake/SQL Server/Redshift/Databricks/Synapse/Azure SQL
only; `attrep_apply_exceptions` unsupported then). Documented limits: no
foreign keys, LOBs only limited, binary types not on some targets, and
after a recoverable error several `attrep_changes*` tables are left for
the operator to drop. *Transactional apply*: each source transaction
applied in commit order with *Minimum number of changes per transaction*
1000 and *Maximum time to batch transactions* 1 s - so even this mode
groups source transactions into one target transaction.

*HVR burst*: all changes of a cycle sorted and *coalesced* to one change a
row, bulk-loaded into `tbl__b` burst tables (streamed straight into the
DBMS loader where one exists - Oracle direct path - or through a
`Staging_Directory`), then one set-wise statement per operation type; the
cycle is one transaction unless `BurstCommitFrequency` is `TABLE` or
`STATEMENT` (Databricks with transactions forces `TABLE` and adds
`last_successful_version` to the burst state table for version-based
recovery). Consequences HVR states: order across tables is not the source's;
triggers fire in the wrong order; "does not work with referential integrity
constraints that are enabled and immediately enforced"; deleting one of a
set of duplicate rows deletes the whole set. *Continuous*: row by row in
commit order, needs indexed keys and an agent near the target. HVR picks
the default by target type.

*IBM CDC fast apply*: *group by table* reorders a unit of work by table on
one connection; *parallelize by table* applies across `n` connections up
to a UOW size threshold (`3:12000`), past which it drops to one
connection; *external table mirror bulk apply* rewrites updates as
delete+insert, drops an insert-then-delete pair inside a UOW, and loads
through external tables (`apply threads:latency threshold:image builder
threads`). Optimisations are disabled with adaptive apply or CDR.

*SharePlex commit reduction*: Post skips the commits of small transactions
until about 100 messages (`SP_OPO_COMMIT_REDUCE_MSGS`) are combined; SQL
cache on by default; `SP_OPO_REDUCED_KEY=2` builds WHERE clauses from key
columns only, omitting the before/after comparison, with the documented
condition that nothing else writes the target and `compare` runs
regularly.

*migkit*: **has**, chosen per table from facts. One transaction per batch
(`_apply_session`, `base.py:3754`; measured 190 rows/s to no lag in
`bench/run.py`); each row's changes collapsed to its net state, later
columns over earlier, a key move as delete-then-upsert, order of first
touch kept (`_collapsed`, `base.py:3917`); runs of one table, kind and
column set as one statement, a key seen twice ending the run
(`_apply_net`, `base.py:3834`; measured 3.9 s to 0.06 s for 20,000
upserts); tables no other table depends on and with no second unique
index written as one run of deletes then one of upserts each - DMS's
order - while tables joined by keys or with a second unique index keep
source order first (`_net_rows`, `base.py:3653`, measured 56 s to 6 s at
10 ms RTT); past `STAGE_FROM` 1000 rows on PostgreSQL, COPY into a
per-session temporary table and one `insert ... select ... on conflict`
that empties it (`postgres.py:1102`, `:1148`; 24 ms against 51 for 5,000
rows), which is Qlik's net-changes table with no leftover to drop; on
MySQL multi-row `insert ... on duplicate key update` measured faster than
the staging path and kept (`mysql.py:502`); SQL Server staging is R2.4
todo. GoldenGate's BATCHSQL exclusions are migkit's `_ordered_tables`
(`postgres.py:1075`, `mysql.py:485`: foreign-key edges and tables with more
than one unique index), so the same rows are protected by the same facts,
without the 25 KB or LOB exclusions because migkit's statements carry the
values. The BATCHSQL error ladder is half there: a lane failure re-applies
the whole batch as one transaction in order (`neutral_apply`,
`base.py:3589-3603`); a constraint error inside that has no
row-by-row-in-source-order step (R2.3) - effort S, docker-testable with a
unique index whose values swap inside a batch. `MAXTRANSOPS`'s behaviour is
migkit's always: a source transaction larger than a batch is applied
across batches (MySQL `skip_rows`, PostgreSQL's `limit` on the peek), safe
because every replay is idempotent - which removes GoldenGate's documented
failure mode - at the cost that a target reader can see part of a source
transaction between two batches; R2.1 ("a batch ends only where a source
transaction ended") would give Qlik's transactional-apply guarantee for
transactions that fit a batch. On PostgreSQL a batch cut inside a
transaction also re-decodes that transaction's head on the next peek
(logical decoding delivers whole transactions at commit), which is paid
work; effort S to end a batch at the last `COMMIT` line seen when one is
in the read, measurable in `bench/run.py`.

### 3.2 Parallel apply and dependency computation

*Integrated Replicat* (Oracle target): Replicat maps records and passes
LCRs to a database *inbound server*; a *reader/preparer* computes
dependencies between transactions from the target's constraints (primary
key, unique indexes, foreign keys - needs supplemental logging of those
columns at the source and an index on every foreign-key column), groups
and sorts in dependency order; a *coordinator* keeps order among *apply
servers* (`PARALLELISM` 4, `MAX_PARALLELISM` autotuned). Independent
transactions commit out of order; dependent ones in source order
(`COMMIT_SERIALIZATION DEPENDENT_TRANSACTIONS`; `FULL` forces source
commit order). `EAGER_SIZE` (15,100 LCRs) starts applying a transaction
before its commit arrives, serialising the apply; `BATCHSQL_MODE` handles
pending dependencies; barrier transactions and DDL are managed
automatically. CDR is not performed in BATCHSQL mode; Replicat drops to
`GROUPTRANSOPS` mode then single-transaction mode on a conflict, and
detection works in all three.

*Parallel Replicat* (any target from 19c, classic trail with full metadata
required): *Mappers* (`MAP_PARALLELISM` 2) read and map in parallel; the
*Master*'s *Collater* restores trail order; its *Scheduler* computes
dependencies from *scheduling columns* in the trail (key, unique index and
foreign key values) and groups transactions into independent batches for
the *Appliers* (`APPLY_PARALLELISM` 4, or `MIN_`/`MAX_APPLY_PARALLELISM`
autotuned). `LOOK_AHEAD_TRANSACTIONS` 10,000 is how far the scheduler
looks; a transaction larger than `CHUNK_SIZE` is applied serially by one
applier with nothing scheduled beside it; `SPLIT_TRANS_RECS n` splits large
transactions into pieces applied in parallel where dependencies allow
(Oracle's tuning advice: raise this, not `CHUNK_SIZE`, which costs memory);
`COMMIT_SERIALIZATION` again. Stats show per-table dependency counts and,
for parallel Replicat only, a dependency graph; when parent and child are
the same table the dependency is a key, otherwise a foreign key.

*Coordinated Replicat*: one process, many threads each reading the trail;
`MAP ... THREAD n` pins a table to a thread, `THREADRANGE(1-3, col)`
partitions a table by a deterministic hash of the named columns (key by
default); no dependency computation for non-barrier transactions - the
hash is the whole rule. *Barrier transactions* (DDL, key updates, some
`EVENTACTIONS`, anything under `MAP ... COORDINATED`) wait for all earlier
transactions on all threads and hold all later ones;
`USEDEDICATEDCOORDINATIONTHREAD` gives them thread 0. Source transactions
are split across threads, so the target is not transactionally consistent
at any instant (Oracle: "aggregates ... abnormal results which could never
appear on the source"). A clean stop synchronises every thread to one
checkpoint; after an unclean stop `SYNCHRONIZE REPLICAT` runs all threads
to the *maximum* checkpoint (high watermark) and stops, after which
`THREADRANGE` may be re-partitioned; the alternative is to reposition all
threads to the *low* watermark and let `HANDLECOLLISIONS` absorb the
faster threads' replays.

*Q Apply*: a *browser* thread per receive queue reads messages and
rebuilds transactions; `num_apply_agents` (default 16) agent threads apply
and commit; dependency analysis at the row level normally, at the table
level for *streamed* transactions (applied before their source commit is
seen, columnar tables); too many agents show as `APPLY_SLEEP_TIME` above
40%; lock contention is answered by fewer agents, row-level locking, or a
lower deadlock timeout; `MAXAGENTS_CORRELID` serialises transactions of one
batch job to stop old transactions starving on RI retries (APAR PM44301).

*SharePlex PEP*: `SP_OPO_DEPENDENCY_CHECK=1` (Oracle) or `SP_OPX_THREADS
>= 2` (SQL Server, PostgreSQL) turns on *transaction concurrency* with
supplemental logging of keys; Capture's `SP_CAP_MIN_SESSIONS` subqueues
per concurrent source session feed it; tables with referential integrity
must share a Post queue in a multi-Post setup; a timekeeper thread ends a
SQL thread stuck in `OCIStmtExecute` past a limit.

*migkit*: **has at batch grain, plans row grain**. Past `LANES_FROM` 2000
changes, rows go to `workers` lanes by a hash of table and key; every
table in a foreign-key group, and every table with a second unique index,
goes whole to one lane in the batch's order (`_lanes`, `base.py:3609`;
groups by union-find over the target's own constraints, `_table_groups`
`base.py:3706`); each lane is its own connection and transaction
(`_apply_lanes`, `base.py:3735`); a failed lane re-applies the whole batch
in one lane (`base.py:3589`); measured 59.6 s to 17.1 s at 10 ms RTT for
320,000 changes, and 154 s to 1.4 s for alternating parents and children
once foreign-key checks are turned off where every parent is in scope
(`_keys_off`, R2.5). `workers` is a ceiling; the number runs is found by
`sizing.Pace` from rows a second and server strain (`migkit/sizing.py:204`).
That is coordinated Replicat's hash for free tables and parallel
Replicat's grouping at the grain of a table; R2.2 is the row grain: for
every collapsed row its table, old and new key, every unique index's old
and new value, and a child's parent key, joined by union-find, lanes packed
largest first, a table with no key, a DDL and a transaction larger than
the batch as barriers. Effort M; docker-testable with the harness of
`test_the_tail_applies_side_by_side.py`. Two facts to keep from the
products: `EAGER_SIZE`/`CHUNK_SIZE` say a large transaction serialises
everything - migkit's split across batches already avoids that;
coordinated Replicat's inconsistency across threads is migkit's across
lanes too, which is why `exact` hops (counters) run one lane
(`twoway.exact`, `twoway.py:125`), and why a target read during a tail is
per-row consistent, not per-transaction - said in the docs, not hidden.

### 3.3 Collisions and resilience

*GoldenGate HANDLECOLLISIONS*: for an overlap between an initial load and
the trail: a duplicate insert overwrites the loaded row with the trail's;
an update of a missing row becomes an insert (needs all columns logged);
a delete of a missing row is ignored; a key update whose old row is
missing becomes an insert, and whose new key exists deletes the old row
and overlays the new; `_ALLOWPKMISSINGROWCOLLISIONS` skips instead of
fetching; per-`MAP` scoping. Oracle: turn it off after instantiation -
left on it "provides an all lights green illusion" (field write-up), and on
tables without a key it diverges. *HVR resilient integrate*: insert to
update when the row exists, update to insert when it is missing, lost
deletes ignored, used for changes during an online refresh. *IBM adaptive
apply*: a mapping type that does the same regardless of source operation,
losing arraying and commit grouping; standard apply errors instead ("0 rows
have been updated for entry 3 from the batch"). *Qlik*: an *Apply
Conflicts* policy per case (duplicate key on insert, no record on
update/delete: ignore, log, suspend table, stop task), preset and fixed
when MERGE is on; every logged error goes to `attrep_apply_exceptions`,
never deleted, and cannot be disabled from the UI.

*migkit*: **has as the default, without the illusion**. Every applier is
idempotent by key: an insert that exists updates, a delete of a missing
row is not an error (`neutral_apply`, `base.py:3574`), which is what a
replay after a crash needs. The divergence GoldenGate warns about is
caught by the check, not by apply errors: the digest per table and the
fenced re-check (`fenced_recheck`, `base.py:3194`), and `watch --verify
--delta`. What migkit does not do is *count* the collisions: an
`update_missing` on a one-way hop is evidence of a target someone else
wrote, and a rising count is the first symptom. The rows are already
known in `_apply_upserts`'s statement outcome on PostgreSQL (`xmax = 0`
in a `returning`) and MySQL (affected rows 1 vs 2); a counter
`migkit_tail_rows_missing_on_update` and `..._existing_on_insert` in the
beat is effort S, docker-testable. Two-way hops already name them
(`insert_exists`, `update_missing`, `twoway.py:271`).

### 3.4 The position kept in the target's own transaction

*GoldenGate checkpoint table* (`ADD CHECKPOINTTABLE`): one row per Replicat
group (`GROUP_NAME`, `GROUP_KEY` primary key), `SEQNO` + `RBA` = trail
position, `AUDIT_TS` = source commit time, `LOG_CSN` = high watermark
(any transaction above it unprocessed), `LOG_CMPLT_CSN` = low watermark
(any below it applied), `LOG_CMPLT_XIDS` = the transactions between the
two already applied, overflowing into `<table>_LOX` rows keyed by
`LOG_CMPLT_XIDS_SEQ`, `LOG_BSN` = where to set Extract back to, `VERSION`.
The row is updated "together with every one replicated transaction", so
the database's own atomicity makes the position and the data agree; the
checkpoint *file* `dirchk/<group>.cpr` beside it is for the process. For a
serial Replicat high and low watermarks are equal. *Striim* `CHKPOINT (id,
sourceposition blob, pendingddl, ddl)` per target, updated by
DatabaseWriter with recovery on, "to ensure that there are no missing or
duplicate events after recovery"; with recovery on some targets lose
parallel threads. *Informatica* `INFORMATICA_CDC_RECOVERY (CYCLE_ENTRY,
SCHEMA_NAME, TABLE_NAME, CYCLE_NUMBER, SEQUENCE)` on database targets, a
checkpoint file on object stores; a checkpoint exists only once a change
has been applied, else the *restart point* (latest position by default);
log truncated past the checkpoint means a new initial load. *HVR* state
tables (2.2). *Qlik* `attrep_txn_state` when *Store task recovery data in
target database* is on; `attrep_status` (task status, memory, changes not
yet applied, source position) updated only after a batch is applied.

*migkit*: **has for `exact` hops, partly for the rest**. `migkit_origin`
carries `n` and `seen = {"token", "batch"}` in the batch's transaction
(`origin_mark`, `postgres.py:1197`; `mysql.py:704`); the tail resumes after
a batch the target committed but the file did not record
(`committed_ahead`, `twoway.py:150`; measured with a failpoint between
commit and save, added once). One-way hops save the position in a file
after the batch and replay one batch on a crash (`hetero.py:3068`,
`failpoint.hit("tail.saved")`), correct by idempotence. R3's ladder makes
the same mark on every hop with the rung the side allows - a PostgreSQL
replication origin (`pg_replication_origin_xact_setup`, progress in the
same commit, no table), a MySQL 8.3 tagged GTID whose `gtid_executed`
names the batches - and R19.11 is why: a resume that asks the target
where it is does less work than one that replays a window. The watermark
pair matters once lanes commit separately: `LOG_CMPLT_XIDS` is the list
migkit would need of lanes committed of a batch, unless lanes stay
all-or-nothing as today (a failed lane re-applies the batch whole). Effort
M, measured per rung in docker before any rung is ranked (R3).

### 3.5 DDL

*Qlik*: captures `ALTER TABLE` from the log without parsing its kind, reads
the new metadata from the source, diffs it with the old to derive
add/drop/modify (one change may hold several), parses later DML with the
new shape, and applies to the target by the *DDL handling policy* (alter
target; ignore, which leaves NULLs in renamed or dropped columns and
suspends the table on a rename); a default value on an added column is
not carried; history in `attrep_ddl_history`; a DDL during full load
reloads the table; store-changes has its own policy (change table only,
both, ignore). *GoldenGate* integrated capture gets DDL from the logmining
server without triggers (11.2.0.4+); coordinated Replicat treats it as a
barrier. *IBM* Q Rep/CDC: CDR not supported with DDL replication;
differential refresh not for tables with DDL replication.

*migkit*: **partly, deliberately**. The tail reads the source catalogue
before each batch against the shape saved beside the position
(`_shape_gate`, `hetero.py:2808`, `drift.shape`), stops when the target
lacks a column the source now has - nothing after the position applied -
and resumes once the operator has brought the target level
(`test_the_tail_stops_at_a_ddl.py`, `test_ddl_during_the_move.py`);
`check schema` generates the DDL and warns where an added `NOT NULL`
column without a default would fail on PostgreSQL or be silently filled on
MySQL (`migkit/ddl.py`); online schema change working tables are ignored
(`drift.transient`). Applying DDL to the target automatically is Qlik's
policy and its documented NULL-filling failures; migkit's choice is the
gate plus the generated statement, and `CREATES_ON_WRITE` targets take a
new column with its first row. Better as is; the missing piece is
positional: a DDL is applied at a log position, and rows before it in the
same batch have the old shape - the gate runs before the batch and the
catalogue is read after the DDL, so a batch straddling a DDL is applied
under the new shape, which for add-column is right and for drop-column
means the old rows' dropped value is discarded, which is also right. A
rename is the case that needs the log's own DDL event (PostgreSQL logical
decoding has none; MySQL's `QueryEvent` has the statement, already read
`mysql.py:830`); effort S to stop the batch at the `QueryEvent` and run the
gate there.

### 3.6 Telling the replicator's own writes apart (loop prevention)

*GoldenGate*: Replicat `DBOPTIONS SETTAG 0935` makes the inbound server tag
its redo (default tag `00`, up to 2000 hex digits, an Oracle Streams tag
also settable by `DBMS_XSTREAM_ADM.SET_TAG`); Extract `TRANLOGOPTIONS
EXCLUDETAG 0935` (one statement per tag; `EXCLUDETAG +` all tags,
`EXCLUDETAG NULL` untagged) drops them in the logmining server. Recommended
and the only option for CDBs; distinct tags per Replicat let a cascade
through while a loop is cut. Older: `TRACETABLE` (`GGS_TRACE`) - Replicat
writes a row at the *start* of each transaction, Extract drops any
transaction beginning with an operation on it; ignored by integrated and
parallel-integrated Replicat. `FILTERTABLE` on non-Oracle sources uses the
checkpoint table's write at the *end* of each Replicat transaction.
`EXCLUDEUSER`/`EXCLUDEUSERID` by the applying user (invalid once the user
is recreated, not for multitenant). *HVR*: the state table write inside
each integrate transaction, detected by capture, cuts loops in
bidirectional channels. *Q Replication*: signals and the `IBMQREP_DONEMSG`
table per receive queue.

*migkit*: **has the table rung, ladder planned and decided**. Every
two-way apply transaction begins with its thread's row of `migkit_origin`
(`origin_mark`, `base.py:3796`, `postgres.py:1197`, `mysql.py:704`); the
reader drops a transaction from the mark to its end (`postgres.py:1394`,
`mysql.py:880`, `marked` kept in the position when a read stops inside
one); measured: 20 rows written on each side, 21 and 22 changes carried,
none back (`test_two_ways_through_migkits_own_tails.py`). GoldenGate's
trace table is exactly this; its tag is the zero-footprint rung migkit's
ladder names (PostgreSQL replication origin, transactional logical message,
MySQL tagged GTID, MariaDB `skip_replication` or a GTID domain), each
proved by a probe transaction read back before it is trusted, ranked per
engine version by the applier's rate with the rung on, the reader's rate,
and the R7 failure battery (backlog R3, decided 2026-09-27). Effort M per
rung; docker matrix PostgreSQL 13/14/16, MySQL 8.0/8.4, MariaDB 11.
Better than GoldenGate: the choice is per side and measured, and `doctor`
names the rung and its footprint.

### 3.7 Conflict detection and resolution

*GoldenGate CDR* (`MAP ... COMPARECOLS ... RESOLVECONFLICT`): detection
compares the trail's *before image* with the target row on the columns
`COMPARECOLS (ON UPDATE|DELETE ALL|KEY|KEYINCLUDING (cols)|ALLEXCLUDING)`
names, which `GETBEFORECOLS` on Extract must log. Five conflict types:
`INSERTROWEXISTS`, `UPDATEROWEXISTS` (before image differs),
`UPDATEROWMISSING`, `DELETEROWEXISTS` (before image differs),
`DELETEROWMISSING`. Resolutions per *column group* (`DEFAULT` = all
columns, or named groups with different rules): `USEMAX(col)` /
`USEMAXEQ` (>=), `USEMIN` / `USEMINEQ`, `USEDELTA` (numeric only: apply
after-before to the target's value), `OVERWRITE` (apply the trail's row;
for `UPDATEROWMISSING` convert to insert), `DISCARD` (drop the record to
the discard file), `IGNORE`. Not for LOB, ADT or UDT columns. Not performed
in BATCHSQL mode - Replicat falls to `GROUPTRANSOPS` then single-transaction
mode, detection in all three. `STATS REPLICAT ... REPORTCDR` counts per
type and resolution.

*Q Replication* (`IBMQREP_TARGETS.CONFLICT_RULE` K / C / A,
`CONFLICT_ACTION`): *key* (only the key is checked), *key and changed
columns* (Q Capture sends before-values of the changed columns; two sides
updating *different* columns of one row is no conflict and the updates
are merged), *all columns* (most data sent); actions ignore / force /
stop the queue / disable the subscription, exceptions to
`IBMQREP_EXCEPTIONS`; LOB columns never detected; `OKSQLSTATES` lets named
errors through. *IBM CDC*: rules per table mapping - source wins, target
wins, largest value wins, ignore - audit table of every resolution; not
with LOBs, expressions, journal control fields or multiple uniqueness
constraints; no collation in string compares; detection only on rows the
source changed, hence differential refresh for the rest.

*migkit*: **has, one column group, three of the five resolutions**.
Detection from the change's before image (a binlog's full row, `REPLICA
IDENTITY FULL`) against the target's row as rendered by `canon`
(`twoway._differs`, `twoway.py:198`; values read back as their class so a
slot's timestamp text and a driver's datetime compare equal,
`twoway._typed`): `insert_exists`, `update_missing`,
`update_origin_differs`, `delete_origin_differs`, and blindness said once
where a source keeps no before image. Policies `error` (stop before the
batch is applied), `apply_remote` (= OVERWRITE), `keep_local` (= DISCARD),
`last_update_wins` by a named column with rank breaking the tie (= USEMAX /
USEMAXEQ), `source_priority` (= a fixed winner), `delta` counters (=
USEDELTA: applied as `n = n + by` where the target's row stands,
`_apply_added` `base.py:3862`, and a row differing only in counters is no
conflict at all - measured 100+10 and 100+5 ending 115 on both sides,
`test_two_way_counters_add_on_both_sides.py`). Every decision with both
rows to `conflicts.jsonl` (`_record`, `twoway.py:383`), which is REPORTCDR
per row. Lacks: `USEMIN`; per-column-group policies; `COMPARECOLS KEY`
(detect on key only) as a choice; and Q Replication's *changed-columns*
merge - the fact is already in hand (`c["before"]` vs `c["values"]` gives
the source's changed columns, `c["before"]` vs `here` the target's), so a
non-overlapping pair is applied as the source's changed columns only, a
decision no policy needs to name; effort S on the harness of
`test_two_ways_through_migkits_own_tails.py`. GoldenGate's CDR-not-in-BATCHSQL
limit is migkit's `exact` rule turned around: `twoway.resolve` runs before
`neutral_apply` on the whole batch (`hetero.py:3054`), so lanes and runs
keep working under CDR.

## 4. Instantiation: where the initial load and the changes meet

### 4.1 GoldenGate: ATCSN, AFTERCSN, precise instantiation

Add and start the primary Extract *before* the load so the trail covers the
gap; take the source's SCN (`FLASHBACK_SCN` for Data Pump, `UNTIL SCN` for
RMAN); start Replicat `AFTERCSN scn` (Data Pump: the export is consistent
*as of* the SCN, so start after it) or `ATCSN scn` (RMAN restores up to but
not including it). 12.2 Data Pump integration writes per-table
instantiation CSNs into the import so Replicat filters per table and
`HANDLECOLLISIONS` is unnecessary. Microservices *precise instantiation*:
the *registration SCN* of the Extract, then wait until the oldest open
transaction started after it - `SELECT MIN(SCN) FROM (SELECT
MIN(START_SCN) FROM gv$transaction UNION ALL SELECT CURRENT_SCN FROM
gv$database)` - that is the *instantiation SCN* for the load Extract and
the primary Replicat; PostgreSQL's precise instantiation reads a snapshot
consistent with an LSN in one initial-load Extract (so no parallel load
Extracts). Otherwise `HANDLECOLLISIONS` covers the overlap (3.3).

*Qlik*: full load per table, DML during it cached and applied after the
table lands; *Transaction consistency timeout* 600 s waits for open
transactions before the load starts and reloads are needed for those still
open; parallel load by *data ranges* (segment boundaries on the unique
index plus chosen columns, no DOUBLE/FLOAT/LOB), *partitions* or
*sub-partitions*, `Maximum number of tables to load in parallel` 5
sub-tasks; a column updated during the load may duplicate rows.

*HVR*: `hvrrefresh` bulk (truncate and load) or row-wise (diff and apply
the minimum); `hvrinit -i` capture rewind; *read/write skipping* tells
capture and integrate to skip changes from before the refresh and apply
those during it resiliently; the integrate job must be stopped first
because the control files are written at the refresh's end; `-M
scn=|time|now|hvr_tx_seq=` reads every table as of one moment.

*SharePlex reconcile*: after a hot backup or copy restored to a target
SCN, `reconcile` discards the post queue's changes already in the copy and
posts only the rest; it is part of an instantiation procedure, not a
command on its own. *Striim*: a stopped initial load resumes with the
tables not completely written; enable CDC before the load. *Arcion*:
snapshot split by `split-key` (`RANGE` between MIN and MAX or `MODULO` by
job id), `max-jobs-per-chunk`, `extraction-priority` per table,
constraints created after the snapshot (`init-constraint-post-snapshot`),
`--resume` from the data directory, `realtime.start-position` per engine.
*DBLog / Debezium incremental snapshot*: chunks selected between a low
and a high watermark written to a signal table; chunk rows whose keys
appear in the log inside the window are dropped; correctness per key,
no global snapshot, resumable per chunk, tables addable mid-stream;
read-only variant on MySQL by GTID.

*ZDM logical online*: phases `ZDM_PREPARE_GG_HUB`, `ZDM_ADD_HEARTBEAT_SRC`,
`ZDM_ADD_SCHEMA_TRANDATA_SRC`, `ZDM_CREATE_GG_EXTRACT_SRC` (Extract first),
`ZDM_DATAPUMP_EXPORT_SRC`, `ZDM_TRANSFER_DUMPS_SRC`, `ZDM_DATAPUMP_IMPORT_TGT`,
`ZDM_POST_DATAPUMP_TGT` (disables the target's purge jobs until after
switchover), `ZDM_ADD_CHECKPOINT_TGT`, `ZDM_CREATE_GG_REPLICAT_TGT`,
`ZDM_MONITOR_GG_LAG` (pause here for as long as needed),
`ZDM_PREPARE_SWITCHOVER_APP`, `ZDM_ADVANCE_SEQUENCES`, `ZDM_SWITCHOVER_APP`,
then removal of Extract, Replicat, trandata, heartbeat and checkpoint
tables and hub cleanup. No Data Guard role changes during replication;
`enable_goldengate_replication=true` on the target.

*migkit*: **has, by construction**. The slot is created before the copy
and the copy reads the slot's exported snapshot (`tail_start` before the
load, `hetero.py:2939`; `pg_export_snapshot`, `postgres.py:6483`;
`test_shared_snapshot.py`, `test_the_snapshot_is_held_for_a_bounded_time.py`),
which is precise instantiation with several loaders sharing one snapshot -
the parallelism GoldenGate's PostgreSQL mode gives up; MySQL takes the
binlog position at the copy's start (`change_point`, `mysql.py:1071`) and
the interleaving invariants are held by
`test_a_copy_and_its_changes_interleave_safely.py` and
`test_full_cdc_misses_nothing.py`. Ranges by key quantiles from the source
with an open first and last range (`checkpoint.plan_ranges`,
`migkit/checkpoint.py:200`; `ranges.plan` `migkit/ranges.py:222`), each
range's commit a failpoint (`range.committed`, `range.saved`,
`ranges.py:242`), keyless tables by spans with the target's count settling
a span committed but unrecorded (`spans_to_copy`, `ranges.py:260`), and a
stop anywhere ending equal (`test_a_move_stopped_anywhere_ends_equal.py`,
15 stops). Sequences advanced at cutover (`sync --kind sequences`,
`docs/cutover.md`, `cli.py:1145`), target writes frozen per role
(`migkit/freeze.py`), triggers held off while applying (`load_window`,
`postgres.py:533`, `mysql.py:989`), secondary indexes set aside and put back
by a later process if this one dies (`migkit/indexes.py`,
`migkit/setaside.py`) - which is Arcion's constraints-after-snapshot with
a record on disk. Lacks: DBLog's rule of *not applying* a change to a range
not yet copied (R19.8, effort M, measured on a write-heavy source); Qlik's
*cached changes per table applied after the table lands* is the same
lever. `reconcile` is not needed because no overlap is ever created.

## 5. Verification and repair

### 5.1 GoldenGate Veridata

Two steps. *Initial compare (row hash)*: agents on each side select rows
by a query, convert to a standard type format across engines, compare key
columns literally and non-key columns by a digital signature (hash;
optional full-column compare at a cost proportional to columns); rows that
differ go to a *maybe out-of-sync* (MOOS) queue in memory. *Confirm
out-of-sync (COOS)*: after a configured replication latency (60 s default:
a row found at 9:30 is confirmed at 9:31), predicated queries re-read those
rows in their original values and classify each *in-flight* (changed since,
assumed replicated), *in-sync* (now equal), *persistently out-of-sync*;
the step can be skipped when the source is quiet. Results to an OOS file
(binary for the UI and re-compare, XML for external repair); not in the
repository. *Repair*: automatic after compare, manual, or *Download Repair
SQL* (Oracle datatype targets only). *Delta processing*: with server-side
sorting, only rows in blocks modified since the last compare are read.
Repair SQL, partitioning of compares and agent memory are the tuning
surfaces.

*migkit*: **has, and the confirm step is deterministic**. A digest per
table inside each server over one canonical text both engines render
(`neutral_digest`, `postgres.py:1442`, `mysql.py:1081`; `migkit/canon.py`),
drilldown to keys, and a *fenced* re-check: the source's position is taken,
the tail is waited for to reach it, the suspect keys re-compared, twice
(`fenced_recheck`, `base.py:3194`; fences `postgres.py:6664/6768`,
`mysql.py:5862/5882`, `mssql.py:950/953`, `mongodb.py:1014/1058`;
`test_mysql_fence.py`), with a sleep-and-settle fallback where no fence
exists (`settle_recheck`, `base.py:3183`; `_resolve_inflight`,
`base.py:3223`). Veridata's latency timer guesses; the fence proves. Delta
processing is `delta_verify` by log position (`postgres.py:6907`,
`mysql.py:2074`, `mssql.py:578`, `mongodb.py:1555`). A long verify resumes
per range with a fingerprint of the expression and boundaries so partials
from another shape are never summed (`migkit/checkpoint.py:39`, `:88`);
the verify is throttled by the server's own signals (`migkit/throttle.py`)
because a check once restarted an Aurora instance twice. Repair statements
per difference are written and applied under a revert point
(`cli.py:1145` onward, `migkit/revert.py`). R8 notes the three Veridata
statuses as words worth reporting (in-flight, in-sync, persistent); the
fence already yields them - effort S to name them in the verdict.

### 5.2 HVR compare and refresh

Covered in `docs/research-other-tools.md` (bulk one checksum over the
transport bytes; row-wise typed values with tolerance for floats; online
compare processing changes during the compare). Added here: online compare
is a `Restrict CompareCondition="tkey <= {hvr_integ_seq}"` on a target
column filled with the integrate sequence, against a source read `-M` as of
the matching moment - a fence by sequence number, HVR's own equivalent of
migkit's LSN fence, needing a column on every target table. migkit's fence
needs none.

### 5.3 SharePlex compare and repair

`sp_cop` spawns `sp_desvr` on the source, which sends a message *through the
post queue* to start `sp_declt` on the target (so a backlog delays the
start and can lose the source's read consistency); the two talk directly.
Source rows are selected under a brief lock for a read-consistent view
(uncommitted transactions block it), target rows under an exclusive table
lock for the compare's duration (per Quest, "briefly"; per field notes,
for the whole table); rows are selected `SP_DEQ_BATCHSIZE` (10,000) at a
time, sorted (TEMP tablespace), hashed with `ORA_HASH` on the target side,
compared; `SP_DEQ_THREADS` clients split a `compare using` job;
`SP_DEQ_PARALLELISM` hints on PostgreSQL sources. Repair = the same, with
the generated INSERT/UPDATE/DELETE executed; `where` limits a repair to
rows, `key` compares keys only. Not in cascading replication; all
processes must be running; log files `desvr_*.log`, `declt_*.log/.sql`.

*migkit*: **has, without the locks**. No lock on either side: the digest is
a plain SELECT under the engine's snapshot, in-flight rows are settled by
the fence (5.1), and the tail is *paused* for a repair through
`tail.pause`/`tail.paused` files so the repair and the tail never race
(`migkit/tailctl.py:138`, `:154`): the tail acknowledges the pause only
after applying what it held and saving its position, and its replays after
the pause land on top of the repair by key, which converges. Batches of
suspect keys are capped at 20,000 per table (`base.py:3186`). Better as is.

### 5.4 IBM differential refresh, adaptive apply, asntdiff

Differential refresh selects both tables `ORDER BY` the primary key or a
unique index and merges them, sending *all* source rows to the target side
(IBM: combine with a subset refresh, run it in its own subscription, add
Db2 work file space on z/OS); modes *refresh only*, *refresh and log
differences* (a log table shaped as the target plus an action column,
before and after images for updates), *only log differences*; standard
replication only, no derived columns, not with DDL replication; the
subscription is not mirroring while it runs, so IBM's zero-latency
procedure is a second subscription and "persistent differences" across two
runs. `asntdiff` is the Q Replication compare utility. Adaptive apply and
standard apply are 3.3.

*migkit*: **has, cheaper**. The digest moves one number per table or range,
not every row; the drilldown moves keys; only differing rows move. The
"log differences without repairing" mode is `check` without `sync`; the
repair is `sync` with a revert point. Persistent-across-runs is the fence.

## 6. Lag and health

### 6.1 GoldenGate heartbeat table

`ADD HEARTBEATTABLE` creates `GG_HEARTBEAT`, `GG_HEARTBEAT_SEED`,
`GG_HEARTBEAT_HISTORY` and views `GG_LAG`, `GG_LAG_HISTORY`, plus a
scheduler job that updates the source row every 60 s. Extract captures it
and fills `OUTGOING_EXTRACT`, `OUTGOING_EXTRACT_TS`; each pump replaces
the `*` in `OUTGOING_ROUTING_PATH` with its name and overwrites the single
`OUTGOING_ROUTING_TS` (so several pumps show as one lag); Replicat, if
`OUTGOING_REPLICAT` matches its name, maps the columns to `INCOMING_*`,
sets `INCOMING_REPLICAT_TS` and `HEARTBEAT_RECEIVED_TS`, updates the target
row and inserts history; otherwise discards the record. Lags: total =
`HEARTBEAT_RECEIVED_TS - INCOMING_HEARTBEAT_TS`; extract = `INCOMING_EXTRACT_TS
- INCOMING_HEARTBEAT_TS`; pump = `INCOMING_ROUTING_TS - INCOMING_EXTRACT_TS`;
replicat read = `INCOMING_REPLICAT_TS - INCOMING_ROUTING_TS`; apply =
`HEARTBEAT_RECEIVED_TS - INCOMING_REPLICAT_TS`. Bidirectional setups
mirror the columns back; unidirectional sources hold an empty table.
Documented: lag can be negative under clock skew; the source needs a table
and a job.

### 6.2 Q Replication monitor tables

`IBMQREP_CAPMON` per `MONITOR_INTERVAL` (30 s LUW, 60 s z/OS): capture lag
= `MONITOR_TIME - CURRENT_LOG_TIME` (fixed to the min of end-of-log and log
time latencies). `IBMQREP_APPLYMON`: `END2END_LATENCY` (source commit to
target commit, ms, averaged over the interval), `CAPTURE_LATENCY` (commit to
put), `QLATENCY` (MQ put to MQ get), `APPLY_LATENCY` (get to target
commit), `HEARTBEAT_LATENCY`, `OLDEST_TRANS` (the target is consistent with
the source as of it - AWS's cutover signal), `ROWS_APPLIED`,
`APPLY_SLEEP_TIME`. A dead process is `MAX(MONITOR_TIME) < CURRENT
TIMESTAMP - MONITOR_INTERVAL - threshold`. `WARNTXLATENCY` names the
transactions past a threshold; `asnqacmd status show details` prints thread
states and queue depths.

*Qlik* `attrep_status`: task status, memory, changes not yet applied, the
source position being read, updated only after a batch is applied. *Striim*
`MON`, `SHOW ... CHECKPOINT HISTORY`. *HVR* job states (`ALERTING` retries).

*migkit*: **partly, and the missing number needs no table**. The beat file
carries `caught_up_at` (the last read that came back short of its limit,
i.e. the log's end reached), `changes`, `room` (`tailctl.beat`,
`migkit/tailctl.py:65`), from which `/metrics` reports `behind` and
`beat_age` (`tailctl.state`, `:82`); the position moves to the log's end
even with nothing to apply so a fence sees the tail arrive
(`postgres.py:1375`); a stopped tail is named with its reason
(`tail.stopped`) and sent to Slack, Teams, Discord, PagerDuty or a webhook
without row values (`migkit/notify.py`); a tail on another host counts as
running (`tailctl.alive`, `:108`), because guessing a writer has stopped
costs rows. Lacks `END2END_LATENCY` and `OLDEST_TRANS`: the source commit
time of each batch's last change. MySQL row events carry it
(`ev.timestamp`); PostgreSQL's `test_decoding` prints it with the option
`include-timestamp` on the peek; SQL Server CDC has `sys.fn_cdc_map_lsn_to_time`;
MongoDB's `clusterTime`. Written into the beat as `applied_through_ts`,
`behind` becomes "source commit to target commit", per batch, without a
table or a job on the source, and clock skew is the only caveat GoldenGate
also has; effort S, docker-testable. `OLDEST_TRANS` is the same number under
the name a cutover wants.

## 7. Streaming analytics (Striim windows, continuous queries)

Striim's *windows* (sliding, jumping, session) and *continuous queries* are
stream processing, checkpointed every `RECOVERY n SECOND INTERVAL` by
rewinding the source to the checkpoint's position and rebuilding window
state (windows are not snapshotted; changing a window's `KEEP` or a CQ's
`GROUP BY` after a checkpoint "can cause data to contain gaps or
duplicates"); *WActionStores* and standalone sources are recoverable only
when persisted (Kafka streams); at-least-once (A1P) unless the writer is
exactly-once (E1P: DatabaseWriter with `CHKPOINT`, Hive ORC with MERGE,
Event Hub with E1P; Cosmos DB, Redshift and Kafka-in-sync may duplicate).
Out of migkit's scope: migkit moves rows between databases and verifies
them; the one thing to take is E1P's definition - "no missing or duplicate
events after recovery" is the property `exact` hops already have and R3's
ladder generalises.

## 8. The table

Effort S = a day, M = a week, L = more. "Docker y" = testable in the
suite's containers without an account.

| mechanism | product | what it gives | migkit status (file:line) | how migkit builds it better (measured facts, no new modes) | effort | docker |
|---|---|---|---|---|---|---|
| Integrated capture / logmining server | GoldenGate | committed-only, commit-ordered changes; RAC/ASM/TDE/CDB handled by the database | has via the server's decoder (`postgres.py:1350`, `mysql.py:762`, `mssql.py:895`, `mongodb.py:558`); Oracle not tailed | nothing to build for the four; Oracle stays "through the pair" | - | y |
| Open-transaction cache and Bounded Recovery | GoldenGate CACHEMGR/BR, Qlik sorter `.tswp`, HVR capture checkpoints, Informatica Logger | recovery from the last interval, not the oldest open transaction | server-side (PG reorder buffer, slot `restart_lsn`); `stream_room` reports `held_bytes` (`postgres.py:1311`) | name the oldest open source transaction holding the slot back, beside `held_bytes`, in `assess` and the beat | S | y |
| Log retention hold | Qlik `attrep_truncation_safeguard`, GoldenGate/RMAN | the source cannot truncate past the reader | partly: measured margin (`postgres.py:1311`, `mysql.py:1009`, `mssql.py:590`), slot is the hold on PG | keep measuring; no pseudo-transaction on SQL Server (documented bloat) | - | y |
| Values not in the log | GoldenGate FETCHCOLS/Flashback, Qlik LOB lookup, Debezium sentinel | unchanged large values arrive whole | has without a fetch: partial merge + column-scoped upsert (`base.py:3917`, `postgres.py:1102`); MySQL FULL images required (`mysql.py:790`) | count omitted-unchanged apart from omitted-absent in `_flatten_changes` (`hetero.py:1686`) | S | y |
| Read once, many consumers | IBM shared scrape, Striim persistent stream, Informatica staging | one log read for many targets | lacks (R3 one-to-many, R19.7 relay) | a spool triggered by measured `stream_room` shrinkage, read by any tail; not before the applier passes the reader (R2.6) | M | y |
| Parallel log decoding | Arcion threads, HVR DIRECT | reader keeps up with the log | partly: read-ahead (`hetero.py:3196`), 116k changes/s measured | sidecar only when measured as the limit (R2.6) | L | y |
| No-log source polling | Arcion delta-snapshot | changes from a timestamp key | lacks | a decision-layer rung chosen from catalogue facts for engines with no readable log; deletes only with a soft-delete column | M | y |
| Trail file (format, RBA, append recovery) | GoldenGate | durable spool decoupling reader and applier | lacks by design; position file + idempotent replay (`hetero.py:2919`) | spill read-ahead batches to disk when `stream_room` shrinks while behind; source position advanced from the spill | M | y |
| Trail encryption (DEK wrapped by master key) | GoldenGate wallet/ENCKEYS | spool at rest unreadable | n/a until a spill exists | per-file key wrapped by a key never written; follows the spill | S after M | y |
| Hub router files + state tables + integ seq | HVR | applied position in the target's own transaction; hub failover rewind | partly: `migkit_origin.seen` for `exact` hops (`postgres.py:1222`, `twoway.py:150`) | R3 ladder: replication origin / tagged GTID carry the batch on every hop; resume after the committed batch (R19.11) | M | y |
| MQ / file queues / Kafka partition by key | Q Rep, SharePlex, Precisely, Informatica Publisher | ordered, durable transport | lacks a queue; has key partitioning and `acks=all` (`streamout.py`, `kafka.py:98`) | Kafka transaction per batch with the position on a state topic; resume from the topic | M | y |
| BATCHSQL + error ladder | GoldenGate | array statements, fallback to normal then source mode | has runs and staging (`base.py:3834`, `postgres.py:1148`); ladder half (`base.py:3589`) | add row-by-row-in-source-order after a constraint error in the single lane (R2.3) | S | y |
| GROUPTRANSOPS / MAXTRANSOPS | GoldenGate | many source tx in one target tx; huge tx split | has: batch = one tx; splits huge tx across batches safely (idempotent) | end a batch at the last COMMIT seen so a fitting tx is never split (R2.1); avoids PG re-decode of a split head | S | y |
| Net changes table + MERGE | Qlik batch-optimized | one statement per table per batch | has on PG: session temp table + `insert...select on conflict` (`postgres.py:1102`); MySQL measured and kept as multi-row | SQL Server staging (R2.4); nothing left behind on error by construction | S | y |
| Burst vs continuous | HVR | throughput vs order/RI | has both, chosen per table by FK/UK facts (`_net_rows` `base.py:3653`, `_ordered_tables` `postgres.py:1075`, `mysql.py:485`) | no mode: the table's constraints decide | - | y |
| Fast apply external-table bulk | IBM CDC | delete+insert through external tables | has (PG COPY staging) | - | - | y |
| Commit reduction / reduced key | SharePlex | fewer commits; key-only WHERE | has: batch tx; key-only upsert; before-image compare only under two-way | - | - | y |
| Dependency-scheduled parallel apply | GoldenGate integrated/parallel Replicat, Q Apply agents, SharePlex PEP | independent transactions in parallel, dependent in order | has at batch grain: lanes by key hash, FK/UK groups whole (`base.py:3609`, `:3706`); measured 59.6 s to 17.1 s | row-grain union-find over old/new keys, unique values, parent keys (R2.2); barriers for keyless tables, DDL, oversize tx; `Pace` sizes lanes (`sizing.py:204`) | M | y |
| THREADRANGE hash + barrier tx + SYNCHRONIZE | GoldenGate coordinated | static partitioning; recovery to high or low watermark | has the hash; lanes all-or-nothing (no per-lane watermark needed) | keep all-or-nothing; if lanes ever commit apart, record committed lanes as `LOG_CMPLT_XIDS` does | - | y |
| HANDLECOLLISIONS / resilient / adaptive apply | GoldenGate, HVR, IBM | overlap tolerated | has as default idempotence (`base.py:3574`); divergence caught by the check, not hidden | count `update_missing` / `insert_exists` on one-way hops in the beat | S | y |
| Checkpoint table in the apply tx (watermarks, LOX) | GoldenGate, Striim CHKPOINT, Informatica RECOVERY, Qlik txn_state | position and data agree atomically | has for `exact` hops; file-after-commit otherwise (`hetero.py:3068`) | R3 ladder on every hop; measured rate cost per rung before ranking | M | y |
| DDL detection by metadata diff + policy | Qlik | target altered automatically; history table | partly by choice: gate stops the tail, DDL generated with backfill warning (`hetero.py:2808`, `ddl.py`) | stop the batch at MySQL's `QueryEvent` for renames; PG has no DDL event | S | y |
| Loop prevention by tag / trace table | GoldenGate SETTAG/EXCLUDETAG, TRACETABLE, FILTERTABLE; HVR state table | own writes never carried back | has the table rung (`base.py:3796`, `postgres.py:1394`, `mysql.py:880`) | ladder of zero-footprint rungs proved by a probe, ranked by measurement (R3) | M | y |
| CDR: types, USEMAX/MIN/DELTA/OVERWRITE/DISCARD, column groups | GoldenGate; Q Rep K/C/A; IBM CDC | conflicts decided per rule, logged | has 4 kinds, 5 policies, counters (`twoway.py:217`, `base.py:3862`, `:383`) | changed-columns merge when the two sides touched different columns (no policy name needed); USEMIN; per-column-group later if asked | S | y |
| ATCSN/AFTERCSN, precise instantiation, per-table CSN | GoldenGate, ZDM | load and changes meet without overlap | has by construction: slot before snapshot, exported snapshot shared by loaders (`hetero.py:2939`, `postgres.py:6483`) | already parallel where GoldenGate's PG mode is serial | - | y |
| Full-load parallel segments | Qlik ranges/partitions, Arcion split-key | tables split across sub-tasks | has: key quantiles, open ends, per-range failpoints, keyless spans (`checkpoint.py:200`, `ranges.py:222/260`) | Arcion's `extraction-priority` = order tables by measured size so long tables start first (check `ranges.step`) | S | y |
| Skip changes to uncopied ranges | DBLog/Debezium watermarks, Qlik cached changes per table | less apply work during catch-up | lacks (R19.8) | drop a change to a key in a range not yet copied; invariants already tested | M | y |
| Reconcile after hot backup | SharePlex | duplicates in the post queue discarded | not needed: no overlap created | - | - | - |
| Row hash + MOOS + COOS + repair | Veridata | in-flight rows told from persistent ones | has, deterministic by fence (`base.py:3194`), settle fallback, delta by log (`postgres.py:6907`) | name the three statuses in the verdict (R8) | S | y |
| Compare under table locks, ORA_HASH batches | SharePlex | consistent compare | has without locks; tail paused for repair (`tailctl.py:138`) | - | - | y |
| Differential refresh (sorted merge, log table) | IBM CDC | repair without truncate | has, moving digests not rows | - | - | y |
| Heartbeat table, per-hop lag columns | GoldenGate | end-to-end lag by hop | partly: `behind` from read-short (`tailctl.py:65/82`) | source commit time of the batch's last change (binlog timestamp, `include-timestamp`, `fn_cdc_map_lsn_to_time`, `clusterTime`) into the beat as `applied_through_ts` = END2END/OLDEST_TRANS without a source table | S | y |
| APPLYMON latencies, dead-process rule | Q Replication | capture / queue / apply split; liveness | partly: `beat_age`, `tail.stopped`, notify | the split falls out of `applied_through_ts` and `caught_up_at` | S | y |
| Windows / CQ / E1P | Striim | stream processing with recovery | out of scope; E1P = `exact` | - | - | - |

## 9. Sources

GoldenGate

* Bounded Recovery: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/extract-bounded-recovery.html ; https://docs.oracle.com/en/middleware/goldengate/core/12.3.0.1/gwurf/br.html ; https://matthewdba.wordpress.com/2014/05/27/goldengate-bounded-recovery-and-log-retention/
* CACHEMGR: https://docs.oracle.com/en/middleware/goldengate/core/21.3/reference/cachemgr.html ; https://docs.oracle.com/goldengate/1212/gg-winux/GWURF/gg_parameters017.htm ; https://blogs.oracle.com/dataintegration/performance-bottlenecks-in-goldengate-the-hidden-cost-of-batch-transactions
* Checkpoints (Extract startup/recovery/current, append recovery): https://docs.oracle.com/en/middleware/goldengate/core/19.1/admin/checkpoints.html
* Checkpoint table columns and watermarks: https://docs.oracle.com/en/middleware/goldengate/core/21.3/coredoc/reference-oracle-goldengate-checkpoint-tables.html ; https://www.bersler.com/blog/oracle-goldengate-classic-replicat-checkpointing/
* Capture modes: https://docs.oracle.com/en/middleware/goldengate/core/19.1/oracle-db/choosing-capture-and-apply-modes.html ; https://docs.oracle.com/goldengate/c1230/gg-winux/GGODB/additional-configuration-steps-using-classic-capture.htm
* TRANLOGOPTIONS (ASYNCTRANSPROCESSING, EXCLUDETAG, EXCLUDEUSER, FILTERTABLE): https://docs.oracle.com/en/database/goldengate/core/26/reference/tranlogoptions.html ; https://docs.oracle.com/en/middleware/goldengate/core/18.1/reference/tranlogoptions.html
* Trail format and Logdump: https://docs.oracle.com/en/middleware/goldengate/core/21.3/coredoc/reference-oracle-goldengate-trails.html ; https://docs.oracle.com/goldengate/c1221/gg-winux/GWUAD/oracle-goldengate-trail.htm ; https://docs.oracle.com/en/middleware/goldengate/core/12.3.0.1/glogd/logdump-commands.html ; https://www.dbasolved.com/2014/04/logdump-and-trail-files/
* Trail encryption: https://docs.oracle.com/en/middleware/goldengate/core/19.1/securing/encrypting-data-master-key-and-wallet-method.html ; https://blogs.oracle.com/dataintegration/understanding-trail-file-encryption-in-goldengate ; https://docs.oracle.com/goldengate/c1230/gg-winux/OGGSE/encrypting-data-enckeys-method1.htm
* BATCHSQL, GROUPTRANSOPS, MAXTRANSOPS: https://docs.oracle.com/en/middleware/goldengate/core/21.3/reference/batchsql.html ; https://docs.oracle.com/goldengate/c1230/gg-winux/GWURF/grouptransops.htm ; https://docs.oracle.com/goldengate/1212/gg-winux/GWURF/gg_parameters105.htm
* Integrated Replicat: https://docs.oracle.com/en/middleware/goldengate/core/23/coredoc/replicat-integrated-replicat.html ; https://docs.oracle.com/en/middleware/goldengate/core/23/coredoc/replicat-additional-parameter-options-integrated-replicat.html ; https://gavinsoorma.com.au/knowledge-base/tuning-integrated-replicat-performance-using-eager_size-parameter/
* Parallel Replicat: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/replicat-parallel-replicat.html ; https://docs.oracle.com/en/middleware/goldengate/core/19.1/coredoc/replicat-basic-parameters-parallel-replicat.html ; https://blogs.oracle.com/dataintegration/parallel-replicat-parallelism-and-dependency-calculation ; https://blogs.oracle.com/dataintegration/parallel-replicat-performance-tuning-in-oracle-goldengate-a-stepbystep-guide-to-fix-replicat-lag
* Coordinated Replicat, barriers, SYNCHRONIZE: https://docs.oracle.com/en/middleware/goldengate/core/23/coredoc/replicat-coordinated-replicat.html ; https://docs.oracle.com/en/middleware/goldengate/core/21.3/coredoc/replicat-barrier-transactions.html ; https://docs.oracle.com/en/middleware/goldengate/core/21.3/ggcab/synchronizing-threads-unclean-stop.html ; https://www.bersler.com/blog/oracle-goldengate-coordinated-replicat-is-it-a-fully-transactional-replication/
* Loop prevention: https://docs.oracle.com/en/middleware/goldengate/core/21.3/coredoc/replicat-excluding-replicat-transactions-bidirectional-replication.html ; https://docs.oracle.com/en/middleware/goldengate/core/21.3/reference/tracetable-notracetable.html
* CDR: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/administer-configure-conflict-detection-and-resolution.html ; https://docs.oracle.com/goldengate/1212/gg-winux/GWUAD/conflict_resolution.htm
* HANDLECOLLISIONS, ATCSN/AFTERCSN, instantiation: https://docs.oracle.com/en/middleware/goldengate/core/19.1/reference/handlecollisions-nohandlecollisions.html ; https://docs.oracle.com/en/middleware/goldengate/core/18.1/admin/instantiating-oracle-goldengate-initial-load.html ; https://docs.oracle.com/en/middleware/goldengate/core/19.1/coredoc/instantiate-add-initial-load-extract-using-admin-client.html ; https://docs.oracle.com/en/middleware/goldengate/core/21.3/coredoc/instantiate-add-initial-load-extract-postgresql-ma.html ; https://mdinh.wordpress.com/2015/02/10/goldengate-start-replicat-atscn-or-afterscn/ ; https://alexlima.com/2024/10/28/what-is-handlecollisions-all-about-in-goldengate/
* FETCHCOLS/FETCHOPTIONS: https://docs.oracle.com/en/middleware/goldengate/core/23/reference/fetchoptions.html ; https://docs.oracle.com/en/middleware/goldengate/core/19.1/reference/table-map.html
* Heartbeat table: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/monitor-monitor-lag.html ; https://docs.oracle.com/en/middleware/goldengate/core/19.1/ggcab/understanding-heartbeat-table-end-end-replication-flow.html ; https://alexlima.com/2024/01/04/how-about-that-gg_heartbeat_history-table/
* Veridata: https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDAD/about_ogg_veridata.htm ; https://docs.oracle.com/en/middleware/goldengate/veridata/12.2.1.4/gvdug/intro-veridata.html ; https://blogs.oracle.com/dataintegration/oracle-goldengate-veridata-23c-how-it-works ; https://www.oracle-scn.com/oracle-goldengate-veridata-automatic-repair-feature/
* ZDM phases: https://docs.oracle.com/en/database/oracle/zero-downtime-migration/21.5/zdmug/zero-downtime-migration-process-phases.html ; https://dohdatabase.com/2021/05/27/zdm-logical-online-migration/ ; https://tziss.wordpress.com/2024/07/10/zdm-logical-online-migration-with-goldengate-hub/

Qlik Replicate

* Change processing tuning: https://help.qlik.com/en-US/replicate/May2026/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm
* Batch-optimized behaviours, net changes table: https://community.qlik.com/t5/Official-Support-Articles/Qlik-Replicate-batch-optimized-apply-mode-behaviors/ta-p/2537668 ; https://community.qlik.com/t5/Qlik-Replicate/Role-of-attrep-changes-and-its-behaviour/td-p/2536702 ; https://community.qlik.com/t5/Qlik-Replicate/Replicate-table-creation-quot-public-quot-quot-attrep/td-p/1786523
* Sorter and transaction offload: https://community.qlik.com/t5/Official-Support-Articles/Replicate-Sorter-Files/ta-p/1872860 ; https://community.qlik.com/t5/Official-Support-Articles/Transaction-Offload-Tuning-guidance-for-Replicate/ta-p/1908420 ; https://community.qlik.com/t5/Official-Support-Articles/Qlik-Replicate-Transaction-Consistency-Timeout-occurred/ta-p/1783594
* LOB handling: https://help.qlik.com/en-US/replicate/November2023/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/lob_table_level.htm ; https://community.qlik.com/t5/Official-Support-Articles/Latency-Performance-Troubleshooting-and-Tuning-for-Replicate/ta-p/1734097 ; https://community.qlik.com/t5/Official-Support-Articles/Replicate-Oracle-source-replicating-LOB-columns-via-ROWID-if-the/ta-p/1984303
* Control tables: https://help.qlik.com/en-US/replicate/May2021/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/control_tables.htm ; https://help.qlik.com/en-US/replicate/November2024/Content/Replicate/Main/Control%20Tables/apply_exceptions.htm ; https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Control%20Tables/replication_status.htm ; https://help.qlik.com/en-US/replicate/November2025/Content/Replicate/Main/Control%20Tables/ddl_history.htm
* Truncation safeguard: https://help.qlik.com/en-US/replicate/November2022/Content/Replicate/Main/SQL%20Server/SQLServerDBSource_AdvProps.htm ; https://community.qlik.com/t5/Official-Support-Articles/Why-are-there-two-open-transactions-on-source-SQL-database-when/ta-p/1946320
* Change tables and headers: https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Change%20Tables/read_change_tables.htm ; https://help.qlik.com/en-US/replicate/May2025/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/store_changes_settings.htm
* DDL: https://help.qlik.com/en-US/replicate/November2025/Content/Replicate/Main/Endpoints/DDLStatements.htm ; https://help.qlik.com/en-US/replicate/May2021/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangsetstab.htm
* Parallel load: https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/Parallel_Load.htm ; https://community.qlik.com/t5/Official-Support-Articles/Qlik-Replicate-Identify-Segment-Boundaries-for-Data-Ranges-based/ta-p/1966161

HVR

* Architecture, hub, router files: https://fivetran.com/docs/hvr6/getting-started/concepts/architecture ; https://fivetran.com/docs/hvr6/getting-started/concepts/hvr-high-availability ; https://fivetran.com/docs/hvr6/command-line-interface/command-reference/hvrrouterview
* Integrate (burst/continuous, Coalesce, BurstCommitFrequency): https://fivetran.com/docs/hvr6/action-reference/integrate ; https://fivetran.com/docs/hvr6/faq/expert-notes-best-practices/burst-vs-continuous-integration
* Capture (LogReadMethod, checkpoints): https://fivetran.com/docs/hvr6/action-reference/capture ; https://fivetran.com/docs/hvr6/advanced-operations/tuning-capture-checkpoints ; https://beta.fivetran.com/docs/hvr6/faq/expert-notes-best-practices/oracle-capture-methods
* Activation, rewind, emit, state tables: https://fivetran.com/docs/hvr6/getting-started/concepts/channel/components-for-activating-replication ; https://www.hvr-software.com/docs/5/commands/hvrinit
* Compare, refresh, online refresh: https://fivetran.com/docs/hvr6/getting-started/concepts/compare ; https://fivetran.com/docs/hvr5/faq/expert-notes/online-refresh ; https://fivetran.com/docs/hvr5/commands/hvrrefresh ; https://fivetran.com/docs/hvr6/action-reference/restrict

IBM

* Q Replication: https://www.ibm.com/docs/en/idr/11.4.0?topic=parameters-num-apply-agents-parameter ; https://www.ibm.com/docs/en/idr/11.4.0?topic=server-ibmqrep-targets-table ; https://www.ibm.com/docs/en/idr/11.4.0?topic=multidirectional-options-conflict-detection-bidirectional-replication ; https://www.redbooks.ibm.com/redbooks/pdfs/sg248154.pdf ; https://www.ibm.com/support/pages/q-replication-and-sql-replication-fixes-db2-linux-unix-and-windows-v115-fix-pack-5 ; https://www.ibm.com/support/pages/apar/PM44301
* Q Replication monitoring: https://www.ibm.com/docs/en/idr/11.4.0?topic=server-ibmqrep-applymon-table ; https://www.ibm.com/docs/en/idr/11.4.0?topic=server-ibmqrep-capmon-table ; https://www.ibm.com/docs/en/idr/11.4.0?topic=troubleshooting-actions-take-when-q-replication-latency-is-too-high ; https://aws.amazon.com/blogs/database/near-zero-downtime-migrations-from-self-managed-db2-on-aix-or-windows-to-amazon-rds-for-db2-using-ibm-q-replication/
* CDC engine: https://www.ibm.com/support/pages/ibm-data-replication-change-data-capture-cdc-best-practices ; https://medium.com/ibm-data-ai/introduction-unlocking-ibm-data-replication-cdcs-best-kept-secret-71e984daf046 ; https://www.ibm.com/docs/fi/SSTRGZ_11.4.0/com.ibm.cdcdoc.performancetuning.doc/tasks/parallelizebytable_fastapply.html ; https://www.ibm.com/docs/SSTRGZ_11.4.0/com.ibm.cdcdoc.performancetuning.doc/tasks/mirrorbulkapply_fastapply.html ; https://www.ibm.com/docs/ko/SSTRGZ_11.4.0/com.ibm.cdcdoc.mcadminguide.doc/concepts/conflictdetectionresolution.html ; https://www.ibm.com/docs/en/idr/11.3.3?topic=tables-mapping-using-adaptive-apply
* Differential refresh: https://www.ibm.com/support/pages/infosphere-change-data-capture-what-differential-refresh ; https://www.ibm.com/docs/en/idr/11.4.0?topic=refresh-validating-data-consistency-using-cdc-replication ; https://www.ibm.com/docs/en/idr/11.4.0?topic=refresh-flagging-source-table-differential

SharePlex

* Commands (compare, repair, reconcile): https://support.quest.com/technical-documents/shareplex/12.0/reference-guide/compare-compare-using-and-repairrepair-using ; https://support.quest.com/technical-documents/shareplex/11.0/shareplex-reference-guide/11 ; https://www.dbi-services.com/blog/shareplex-compare-and-repair-commands/ ; https://support.quest.com/kb/4299542/compare-runs-into-errors-warning-32-broken-pipe
* Post parameters and PEP: https://support.quest.com/technical-documents/shareplex/12.0/administration-guide/oracle-poster-parameters ; https://support.quest.com/technical-documents/shareplex/11.1/administration-guide/oracle-post-parameters ; https://support.quest.com/shareplex/kb/4211217/what-are-the-post-parameters-that-help-performance

Precisely, Informatica, Striim, Arcion, Debezium

* Precisely Connect CDC: https://help.precisely.com/r/Connect-CDC-SQData/Latest/en-US/Connect-CDC-SQData-Apply-engine/Apply-engine-overview ; https://help.precisely.com/r/Connect-CDC-SQData/Latest/en-US/Connect-CDC-SQData-Architecture/Components/Data-capture-agents ; https://docs.precisely.services/docs/sftw/sqdata-webhelp/4.0/en-us/webhelp/HTML/web_replicator_engine.html
* Informatica PowerExchange Logger and tokens: https://docs.informatica.com/data-integration/powerexchange-for-cdc-and-mainframe/10-5-3/cdc-guide-for-linux--unix--and-windows/part-2--powerexchange-cdc-components/powerexchange-logger-for-linux--unix--and-windows/powerexchange-logger-overview.html ; https://docs.informatica.com/data-integration/powerexchange-for-cdc-and-mainframe/10-5/cdc-guide-for-linux--unix--and-windows/part-2--powerexchange-cdc-components/powerexchange-logger-for-linux--unix--and-windows/starting-the-powerexchange-logger/how-the-powerexchange-logger-determines-the-start-point-for-a-co.html ; https://docs.informatica.com/data-integration/powerexchange-for-cdc-and-mainframe/10-5/cdc-guide-for-linux--unix--and-windows/part-4--change-data-extraction/introduction-to-change-data-extraction/restart-tokens-and-the-restart-token-file.html ; https://knowledge.informatica.com/s/article/108433?language=en_US
* Informatica CDC Publisher and Cloud ingestion: https://docs.informatica.com/data-integration/powerexchange-cdc-publisher/1-1/user-guide/powerexchange-cdc-publisher-key-concepts/checkpointing-and-guaranteed-delivery.html ; https://docs.informatica.com/data-integration/powerexchange-cdc-publisher/1-3/user-guide/powerexchange-cdc-publisher-key-concepts/storing-checkpoints-in-kafka.html ; https://docs.informatica.com/integration-cloud/data-ingestion-and-replication/current-version/database-ingestion-and-replication/managing-database-ingestion-and-replication-jobs/restart-and-recovery-for-incremental-change-data-processing.html ; https://knowledge.informatica.com/s/article/FAQ-What-is-the-significance-of-INFORMATICA-CDC-RECOVERY-table-in-Data-Ingestion-and-Replication-jobs?language=en_US
* Striim: https://www.striim.com/docs/en/recovering-applications.html ; https://www.striim.com/docs/en/creating-the-checkpoint-table.html ; https://www.striim.com/docs/en/pipelines.html ; https://developer.striim.com/onlinedocs/en/database-reader.html ; https://www.striim.com/blog/change-data-capture-best-practices-read-once-stream-anywhere-pattern/ ; https://www.striim.com/docs/en/alter-and-recompile.html
* Arcion: https://docs.arcion.io/docs/references/extractor-reference/ ; https://docs.arcion.io/docs/source-setup/oracle/setup-guide/ ; https://learn.microsoft.com/en-us/azure/cosmos-db/cassandra/oracle-migrate-cosmos-db-arcion ; https://venturebeat.com/data-infrastructure/arcion-now-reads-logs-from-oracle-directly-promises-10x-faster-data-replication
* DBLog and Debezium: https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b ; https://debezium.io/blog/2021/10/07/incremental-snapshots/ ; https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/ ; https://debezium.io/blog/2019/10/08/handling-unchanged-postgres-toast-values/ ; https://debezium.io/documentation/reference/stable/post-processors/reselect-columns.html
