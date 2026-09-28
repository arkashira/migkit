# The CDC/ELT specialists, mechanism by mechanism, and where migkit stands (2026-09-28)

**Completeness.** Sections 1-8 are complete against primary sources. Thinner than the
rest, from docs that did not expose internals: PeerDB's Avro staging and QRep
partitioning (stated from PeerDB docs/blogs at overview depth), Artie's exact
merge/dedup SQL (product docs only; Transfer repo not fetched), Singer's
`ACTIVATE_VERSION` (SDK docs, not the spec). One open measurement is flagged in
section 2 and item 4 of section 7 (`unchanged-toast-datum` in `pgslot.py`).

Public research only. Terms are the tools' own (from their docs or papers); migkit
status is from the tree as of this date, cited `file:line`. Effort: S = under a day,
M = one to three days, L = a week or more. "Docker" = provable in the sandbox
(postgres/mysql/mariadb/mongo/redpanda/azure-sql-edge images) without a real database.

Reading order: section 1 is the mechanism every log-based tool is built on (watermarked
chunk capture) and what migkit does instead; 2 is Debezium's internals; 3 the ELT
tools' apply side; 4 the multi-master and trigger tools; 5 the managed and streaming
engines; 6 the migkit map; 7 the gaps and how to build past them; 8 the table.

---

## 1. Capturing a full table beside a live log: the watermark family

### 1.1 Netflix DBLog (Andreakis, Papapanagiotou; arXiv 2010.12597)

**Problem.** Transaction logs have limited retention, so a full copy is needed; the
copy must not stall log processing and must not let an older chunk row overwrite a
newer log event (the "order of history").

**Mechanism, Algorithm 1 "Watermark-based Chunk Selection", verbatim structure:**

```
Input: table
(1) pause log event processing
    lw := uuid(), hw := uuid()
(2) update watermark table set value = lw
(3) chunk := select next chunk from table         -- rows with pk > last pk, LIMIT n
(4) update watermark table set value = hw
(5) resume log event processing; inwindow := false
    loop: e := next event from changelog
      if not inwindow:
          if e is not watermark: append e to outputbuffer
          else if e is watermark lw: inwindow := true
      else:
          if e is not watermark:
(6)           if chunk contains e.key: remove e.key from chunk
              append e to outputbuffer
          else if e is watermark hw:
(7)           for each row in chunk: append row to outputbuffer
```

Requirements the paper states: the database emits changed rows "from a linear history
in commit order" and supports "non-stale reads" (the chunk select sees every change
committed before it began); the table has a primary key with an efficient range scan;
the watermark table is a single-row table in a namespace DBLog owns, holding a UUID,
and each update to it is one ordinary change event. Chunks are ordered by primary key;
the last row of a completed chunk is stored in Zookeeper so dumps pause and resume;
dumps are triggered for all tables, one table, or specific primary keys; chunk size is
configurable to throttle. Because the exact log position of the select is unknown,
every chunk row whose key appears in a log event between `lw` and `hw` is dropped -
the log event is newer or equal, and wins. Log events are never buffered; only the
chunk is in memory. Output goes to an in-memory buffer drained to Kafka by another
thread. Sources: MySQL via the shyiko binlog connector, PostgreSQL via a slot with
`wal2json`. Table 1 of the paper compares Databus, Debezium (as of 2020), Maxwell and
MySQLStreamer on: triggered at any time, pausable, log processing does not stall,
preserves order of history, no locks, no vendor-specific features - DBLog claims all six.

**Limits.** Needs write access to the source (the watermark table). Needs a primary
key. Output is at-least-once (the paper delivers to Kafka; dedup is the consumer's).
The "Generalized DBLog" spec (2026) that migkit's interleave test cites restates the
two invariants any chunk-plus-log design must hold: no change falls through a gap
between chunks and the log; a chunk row never overwrites a newer change or brings back
a deleted row.

### 1.2 Debezium incremental snapshots (DBLog in Kafka Connect)

**Mechanism.** A signal (`execute-snapshot`, `data-collections`, `type: incremental`,
optional `additional-conditions`, `surrogate-key`) starts a per-table walk in chunks
of `incremental.snapshot.chunk.size` (default 1024) in primary-key order; the connector
first reads the table's maximum key (the "snapshot endpoint"), then selects "the next
chunk of records whose primary keys are larger than the last one from the previous
chunk". Around each chunk it writes `snapshot-window-open` and `snapshot-window-close`
rows to the signal table (the watermarks), buffers the chunk's `READ` events, and while
the window is open "the snapshot event is dropped from the buffer" for any key that a
streamed `UPDATE`/`DELETE` touched - "only the transaction log event is kept". State
for resume is three things: the tables still to snapshot, the maximum key of the
current table, and the key of the last event sent. Consumers must accept that a row can
arrive as `read` then `update` or the reverse, or as `read`+`delete` or just `delete`,
and that there won't be `read` events for every record. Tables without a primary key
are refused. `incremental.snapshot.watermarking.strategy` selects whether the window
markers are two inserts (`insert_insert`) or an insert then a delete (`insert_delete`)
so the signal table does not grow. Blocking snapshots (`type: blocking`) instead stop
streaming, snapshot the named tables as an initial snapshot would, and resume from the
recorded offset - "a delay might exist between the time that you send the signal ...
and the time when streaming stops", so duplicates are possible.

**Read-only variants (no signal table).** MySQL: `read.only=true` takes the executed
GTID set from `SHOW MASTER STATUS` before and after the chunk select as the low and
high watermark and "compares each event's GTID against the in-memory watermarks";
needs `gtid_mode=ON`, `enforce_gtid_consistency=ON`, and
`replica_preserve_commit_order=ON` when `replica_parallel_workers > 0`; a DDL during a
chunk re-selects the chunk. Signals arrive by the Kafka channel (`signal.kafka.topic`,
one partition). PostgreSQL 13+: `read.only=true` uses `pg_current_snapshot()` - its
`xmin`, `xmax` and in-progress `xip` list - and "compares the transaction ID of
write-ahead-log events or heartbeat events against low and high watermarks"; a
long-running transaction keeps the window open. Both are measured in migkit's Debezium
wrap (`migkit/movers.py:3398` `_gtid_on`, `:3416` `_pg_snapshot_watermarks`: on connector
3.0.8 against PostgreSQL 16 the read-only incremental re-read ran with nothing written
to the source; without `read.only` it was refused with "Incremental snapshot is not
properly configured").

**Parallel initial snapshot.** `snapshot.max.threads > 1` divides each table into
primary-key-range chunks that threads claim dynamically;
`snapshot.max.threads.multiplier` sets chunks per thread.

**Limits.** Debezium's own numbers (cited in migkit backlog R18) put incremental
snapshots an order of magnitude slower than a plain initial snapshot; the signal-table
default writes to the source; the buffer holds a whole chunk; resume replays the chunk.

### 1.3 Flink CDC / RisingWave: per-chunk "binlog backfill", then a filtered log split

**Mechanism** (Flink CDC MySQL source, "How Incremental Snapshot Reading works").
Tables split into chunks on `scan.incremental.snapshot.chunk.key-column` with
`scan.incremental.snapshot.chunk.size` (numeric keys by fixed step, others by
`SELECT MAX(col) FROM (SELECT * WHERE col > last ORDER BY col LIMIT n)`). Each snapshot
split, read by any of N parallel readers: (1) record the low watermark = current binlog
offset; (2) `SELECT ... WHERE key BETWEEN lo AND hi` into a buffer; (3) record the
high watermark; (4) read the binlog between low and high and *upsert those events into
the buffer* (the chunk is corrected in memory rather than rows dropped); (5) emit the
chunk as inserts. When every split is done, one "binlog split" starts from the minimum
of all high watermarks and forwards an event only if its key belongs to a finished
split and its offset is past that split's high watermark. No `FLUSH TABLES WITH READ
LOCK`; exactly-once by Flink checkpoints holding split state and binlog offset.
RisingWave embeds Debezium for the log and applies the same scheme
(`backfill.parallelism`, `backfill_num_rows_per_split` default 100,000,
`backfill_as_even_splits`, `snapshot='false'` to skip), holds a transaction's events
until its commit (`transactional='true'`), stores offsets in Hummock checkpoints, and
replicates `ADD COLUMN`/`DROP COLUMN` with `auto.schema.change='true'`.

**Limits.** The watermark pair is per chunk, so a chunk's correctness rests on the
reader seeing the binlog between two positions it read itself - fine on one binlog,
harder across sharded sources. RisingWave: no time travel on native CDC, decimals must
be strings, TOAST only for named types.

### 1.4 Estuary Flow: a watermark table, and replication filtered until backfilled

**Mechanism.** The PostgreSQL capture keeps a slot plus publication and backfills in
chunks of `backfill_chunk_size` (default 4096) in key order; before emitting a chunk it
writes to its watermarks table (default `public.flow_watermarks`) - "this write is
observed in the WAL and rows in the chunk are emitted after" - while "replication
events for keys within backfilled ranges are filtered from the change stream until
those ranges complete". Backfill modes: Normal, Precise (exact equality semantics, not
possible under some encodings), Only Changes (skip backfill), Without Primary Key
(walks by `ctid`, "lacks the exact correctness properties of the Normal mode");
`skip_backfills` per table. 2026 added a read-only mode with no watermark table that
needs a "heartbeat" table to keep the slot advancing, and capture from a PostgreSQL
16+ standby with `hot_standby_feedback=on`. TOAST columns omitted from an update are
filled by a `merge` reduction from the previous known value. Exceeding
`max_slot_wal_keep_size` invalidates the slot and forces a full re-backfill.

### 1.5 Vitess VReplication: copy, catch-up, fast-forward by `lastpk`

**Mechanism** ("Life of a stream"). Initialize: one row per table in `_vt.copy_state`.
Copy: one table at a time, rows with primary key greater than `lastpk` "using a
consistent snapshot", batched by packet size, `lastpk` saved per batch. Catchup:
replicate binlog from the copy's GTID to now, "only passing events for pks" at or
below `lastpk` of the table being copied - events beyond `lastpk` are ignored because
the copy will read that state. Fast-forward: after each snapshot but before sending its
rows, replicate from the previous batch's GTID to the new snapshot's GTID, again
filtered by `lastpk`. Replication: steady state once every table is copied. The
Debezium Vitess connector rides the same machinery through VStream (VGTID = a list of
`(keyspace, shard, gtid)`; `vitess.stop_on_reshard`; per-task offsets named
`taskId_numTasks_gen`; `transaction_epoch`/`transaction_rank` for cross-shard order).
VDiff v2 runs on tablets, stores progress in `_vt.vdiff_table` and resumes from the
last primary key, waits for the target stream to reach the source's GTID before
comparing a batch, reports extra rows on either side and mismatched rows with samples,
and warns that tables without a primary key need a full scan and filesort.

### 1.6 Airbyte: `ctid`-chunked initial load with resumable state, then the WAL

**Mechanism.** The Postgres source captures an LSN before the snapshot, then reads the
table in `ctid`-paged sub-queries (about 1 GB each), emitting a `ctid` state per chunk
so a failed sync resumes mid-table; the physical address is "prone to change" (VACUUM),
so `ctid` is never used past the initial load, after which sync switches to the WAL
(Debezium embedded, `pgoutput`, slot + publication), `xmin` mode (needs no cursor,
breaks on wraparound), or a user cursor. CDC needs GLOBAL state: `shared_state` holds
the single WAL position, per-stream states the tables. `initial_waiting_seconds` lets a
large snapshot finish before change reading. Recovery when the slot is lost: "Saved
offset is before Replication slot's confirmed_flush_lsn, Airbyte will trigger sync from
scratch". Three public bugs show the failure modes: #29237 (each chunk emitted a
"final" state, so a crash in the window switched to incremental with the table
unfinished), #49803 (the initial snapshot committed an LSN past the saved offset),
#80939 (a rejected `ctid` state left `maybeCtid == null`, read as "snapshot complete").

### 1.7 PeerDB, pgcopydb, Artie: exported snapshot plus CTID ranges, no watermark

**Mechanism.** PeerDB creates the slot, exports a snapshot with `pg_export_snapshot()`,
partitions each table into CTID ranges ("tid range scans ... in the order of how it is
stored on the disk"), reads them in parallel under that snapshot
(`snapshot_num_rows_per_partition` 500,000, `snapshot_max_parallel_workers`,
`snapshot_num_tables_in_parallel` 4), with `COPY ... BINARY` for Postgres-to-Postgres,
then replays the slot. Reported: 1 TB in about 2 hours, and slower on 16 workers than
8 because the network was full. pgcopydb notes the constraint: an exported snapshot "is
available for import only until the end of the transaction that exported it", so the
exporting session must stay open for the whole copy. Artie runs CDC first (changes
queue in Kafka), backfills by integer-key ranges ("Parallel Segmented Backfill",
"resilient to row movement caused by updates and vacuuming") or by CTID shards for the
largest tables ("sensitive to vacuum operations"), then "applies the queued CDC changes
in order". Xata's pgstream credits the same design. This family needs no watermark
because the copy is one consistent point and every change after it is replayed;
what it pays is the whole change backlog applied after the copy.

### 1.8 migkit today, against this family

- Position before the copy, on every source with a log: `HeteroEngine.tail_start`
  (`migkit/engines/hetero.py:2894`) writes the token before the first row is read;
  PostgreSQL's slot is created before the copy and a gone slot stops the tail rather
  than restarting from "now" (`migkit/engines/postgres.py:1260` `_slot_ready`); tested
  by `tests/test_full_cdc_misses_nothing.py` and `tests/test_cdc_after_a_separate_copy.py`.
- Copy in key ranges planned once and kept (`migkit/ranges.py:222` `plan`, `:242`
  `finished` - `ranges_done` and a `last` reach; keyless tables by span with the
  target's count reconciling an unaccounted span, `:260`), chunk size adapted from a
  measured rate (`migkit/checkpoint.py:172` `Rate`), one exported snapshot shared by
  verify lanes with a single fence LSN (`postgres.py:6483`, `:6524`;
  `tests/test_shared_snapshot.py`).
- Tail after the copy replays everything since the position, applied first and the
  token saved second (`hetero.py:2919`), idempotent by key (`migkit/engines/base.py:3574`),
  collapsed per key per batch (`:3917` `_collapsed`: partial updates merged later over
  earlier, a delete in between restarts the row, a key move leaves the old address).
- The interleaving is property-tested rather than reasoned: `tests/test_a_copy_and_its_
  changes_interleave_safely.py` drives Hypothesis over writes before/during/after, chunk
  cuts, batch edges, and a stop that re-reads `back` changes, 400 examples, asserting
  target == source. This holds the two Generalized-DBLog invariants in migkit's shape.
- **Not built:** dropping the changes the copy already reflects (backlog R19 lever 8,
  `docs/backlog.md:4531-4540`), i.e. the Vitess/DBLog filter. The correctness of
  apply-everything is proven; the wasted apply work on a write-heavy source is not
  removed. Section 7.1 gives the exact rule and why it needs no watermark table.

---

## 2. Debezium internals beyond the snapshot

**Snapshot modes.** `initial` (when no offsets), `always`, `initial_only`, `no_data`
(schema only, stream from the slot's point), `when_needed` (when offsets are missing or
the recorded position is gone from the server), `recovery` (rebuild the schema history),
`configuration_based`, `custom` (a `Snapshotter` SPI). PostgreSQL consistency: the slot
is created first and the snapshot transaction reads at the slot's exported snapshot /
`confirmed_flush_lsn`, so nothing is missed or doubled between snapshot and stream;
`snapshot.locking.mode` is `shared`/`none`/`custom` on PostgreSQL and
`minimal`/`extended`/`none` on MySQL (global read lock by default, table-level locks
where `RELOAD` is not granted). Oracle snapshots read `AS OF SCN n`; SQL Server
`snapshot.isolation.mode` is `read_uncommitted`/`read_committed`/`repeatable_read`/
`exclusive`/`snapshot`.

**Signal channels.** `signal.enabled.channels` = `source` (table with `id`, `type`,
`data`), `kafka` (`signal.kafka.topic`), `jmx`, `file` (`signal.file`), custom
`SignalChannelReader`. Types: `log`, `execute-snapshot`, `stop-snapshot`,
`pause-snapshot`, `resume-snapshot`, custom `SignalActionProvider`. migkit uses the
Kafka channel only, so the source is never written (`movers.py:3197-3206`,
`tests/test_resnapshot_signal.py::test_no_table_is_asked_for_on_the_source`); its own
tail has pause/resume through files beside the token (`migkit/tailctl.py:138` hold,
`:154` pause, `:170` resume) and stops only between batches.

**Transaction metadata.** `provide.transaction.metadata=true` emits `BEGIN`/`END` on
`<prefix>.transaction` with `id` (PostgreSQL: `xid:lsn`), `event_count` and per-table
`data_collections`; every event carries `transaction.total_order` and
`data_collection_order`. migkit's Kafka/Kinesis/Pub/Sub envelopes carry none of this
(`migkit/engines/kafka.py:110-175`; `migkit/avrostream.py`).

**Schema history.** MySQL/Oracle/SQL Server keep DDL with log positions in
`schema.history.internal.kafka.topic` and rebuild table structure on restart;
`snapshot.mode=recovery` rebuilds a corrupt history; DDL-parse failures stop the
connector. migkit keeps the catalogue shape beside the token (`tail-shape.json`,
`hetero.py:2808` `_shape_gate`, `migkit/drift.py:36`/`:60`), re-reads it before each
batch it would apply, stops with the position saved before the batch when the target
lacks a column the source now has, and ignores online-schema-change working tables
(`drift.py:22`; `tests/test_the_tail_stops_at_a_ddl.py`). It never learns the DDL text,
only the resulting shape.

**Exactly-once (KIP-618, Kafka 3.3).** Workers set
`exactly.once.source.support=enabled` (`preparing` for a rolling upgrade); the
connector `exactly.once.support=required|requested`; `transaction.boundary=poll` writes
each poll's records and their offsets in one Kafka transaction. Only the streaming
phase is exactly-once; "the initial snapshot phase always repeats entirely after
failures". migkit's equivalent is on the apply side: numbered batches whose mark the
target itself commits (`twoway.exact`, `hetero.py:2955-2960`, `_committed_ahead`), so a
lost commit answer is settled by asking the target (backlog R3, measured with a
failpoint between commit and save).

**LOB and TOAST.** PostgreSQL omits an unchanged TOASTed column from an `UPDATE`
unless `REPLICA IDENTITY FULL`; Debezium writes `unavailable.value.placeholder`
(default `__debezium_unavailable_value`), Oracle needs `lob.enabled`. Estuary fills the
gap by a `merge` reduction over the last known value; migkit's `_collapsed` merges a
partial update over the target's row without nulling what it did not carry. Open
question, to measure: `pgslot.py` has no branch for the unquoted
`unchanged-toast-datum` token `test_decoding` prints for such a column (grep:
`migkit/pgslot.py`, `postgres.py` - no match), so an `UPDATE` of a small column on a
row holding a wide `text` under `REPLICA IDENTITY DEFAULT` may land the literal
token. Section 7 item 4.

**Heartbeats.** `heartbeat.interval.ms` and `heartbeat.action.query` exist because a
slot on an idle database never advances while the server's other databases write WAL
the slot pins. migkit moves the token to `pg_current_wal_lsn()` when a read comes back
whole (`postgres.py:1373-1377`) and reads `safe_wal_size`/held bytes every minute for
`/metrics` (`postgres.py:1311` `stream_room`, `hetero.py:2967-2971`).

**Oracle.** Three adapters: LogMiner (default; `log.mining.strategy` =
`online_catalog` for immediate DDL visibility vs `redo_log_catalog` for history in the
archive vs `hybrid`; `log.mining.buffer.type` = `memory`/`infinispan_embedded`/
`infinispan_remote` for the uncommitted-transaction buffer, `log.mining.transaction.
retention.ms` to drop long in-flight ones, adaptive `log.mining.batch.size.min/default/max`,
`archive.log.only.mode`, `log.mining.scn.gap.detection.gap.size.min`; supplemental
logging required; `ORA-01555` when a snapshot outlives `UNDO_RETENTION`, `ORA-01466` on
concurrent DDL), XStream (`database.connection.adapter=xstream`, a GoldenGate licence
and an outbound server), OpenLogReplicator (`=olr`, an external process reading redo
and archive logs directly). migkit has no Oracle change reader (`migkit/engines/oracle.py`:
no `neutral_changes`); backlog R11/R12.

**SQL Server.** Debezium reads CDC change tables (`cdc.fn_cdc_get_all_changes_<instance>`
between the stored and `sys.fn_cdc_get_max_lsn()`), sorted by commit LSN then change
LSN; schema change through a second capture instance (offline or online procedure);
"No maximum LSN recorded in the database; SQL Server Agent is not running" is the one
true error; cleanup-job retention purges history; offsets commit periodically so an
outage yields duplicates. migkit uses Change Tracking instead (`migkit/engines/mssql.py:579`
checks, `:856` `log_position` = `change_tracking_current_version()`, `:895`
`neutral_changes`: `CHANGETABLE(CHANGES t, @v)` left-joined to the live row, version
read before rows so a change during the read is read again, `:902` refuses when the
version is below `change_tracking_min_valid_version`;
`tests/test_sql_server_follows_through_change_tracking.py`). Microsoft's own protocol
adds what migkit does not yet do: wrap the min-valid check, the current-version read and
every `CHANGETABLE` read in one `SNAPSHOT` isolation transaction so cleanup and
concurrent commits cannot skew the set; `WITH CHANGE_TRACKING_CONTEXT(@source_id)` to
mark a writer's own changes (a two-way origin rung); `CHANGETABLE(VERSION ...)` for a
per-row conflict check; and the restore hazard - after a restore to an earlier version
the client's saved version still validates and the sides diverge silently.

**MongoDB.** Change streams, not the oplog; `capture.mode` = `change_streams`,
`change_streams_update_full`, `change_streams_with_pre_image`,
`change_streams_update_full_with_pre_image`; pre-images need
`changeStreamPreAndPostImages` on the collection (6.0+) and a retention
`expireAfterSeconds`; `updateLookup` "may return a document newer than the change
event"; a resume token whose oplog entry is gone raises `ChangeStreamHistoryLost` and
Debezium re-snapshots; `startAfter` resumes past an `invalidate` where `resumeAfter`
cannot; sharded clusters get a task per shard; incremental snapshots walk `_id` order
with a signal collection. migkit: `migkit/engines/mongodb.py:532` `change_point` reads
the resume token of an empty first batch (no event consumed), `:558` `neutral_changes`
watches the whole database with `full_document="updateLookup"`, skips an update whose
lookup found nothing (its delete is behind it in the same stream), and turns
`removedFields` into `canon.ABSENT` (`migkit/canon.py:159`). No pre-image, so two-way
on MongoDB is "blind" (`migkit/twoway.py:301-306`); `resume_after` only.

**Cassandra.** A JVM agent on every node reads flushed commit-log segments from
`cdc_raw` with `CommitLogReader`; offset = segment file + position; only modified
columns (plus partition key) are present, there is no before-image, events "may arrive
out-of-order", replicas produce duplicates, the agent must delete processed segments.
migkit's Cassandra engine has no change reader.

**Spanner.** Change streams as a partition tree (child partitions, heartbeat records);
per-partition low watermark ("the timestamp at which the connector is guaranteed to
have streamed out all events with timestamp < T", `gcp.spanner.low-watermark.enabled`);
value capture `OLD_AND_NEW_VALUES`/`NEW_ROW`/`NEW_VALUES`; ordered by commit timestamp
within a key; no snapshot; can only start about an hour back by default.

**Debezium JDBC sink** (the reference apply side). `insert.mode` = `insert`/`update`/
`upsert`; `primary.key.mode` = `none`/`kafka`/`record_key`/`record_value`;
`delete.enabled` on tombstones; `schema.evolution` = `none`/`basic` (add columns only);
`batch.size` 500; `use.reduction.buffer` keeps the last event per key in a batch and
warns that it "may break secondary unique constraint dependencies"; upsert SQL per
dialect (`ON CONFLICT`, `ON DUPLICATE KEY`, `MERGE`). Confluent's JDBC sink is the
same shape with `pk.mode`, `auto.create`/`auto.evolve`, at-least-once, no transaction
across batches. migkit's applier is the reduction buffer done right: `_net_rows`
(`base.py:3653`) collapses only tables no foreign key or second unique index binds, and
writes bound tables change by change in source order; runs of like rows become one
statement (`:3837`); lanes by key hash or by foreign-key group (`:3609`, `LANES_FROM`
2000); a failed lane re-applies the whole batch in one transaction.

---

## 3. The ELT specialists' apply side

**PeerDB.** Two phases per sync: rows land in `_peerdb_raw_<table>` (sync), then a
normalize step merges into the final table with `_peerdb_synced_at`, `_peerdb_version`
and, with soft deletes on, `_peerdb_is_deleted` instead of a physical delete. Sync
interval default 60 s, `max_batch_size` 100k (500k-1M advised on 32 GB+),
`PEERDB_CDC_IDLE_TIMEOUT_SECONDS` 60, `wal_sender_timeout=0` advised, raise
`logical_decoding_work_mem`. Warehouse targets are fed by Avro files staged then loaded.
QRep (query replication) walks a watermark column in partitions for sources without a
log. Resync: drop the mirror, create `<table>_resync`, re-run the initial load, carry
soft-deleted rows across, swap atomically - also the fix for an invalidated slot.
Orchestration is Temporal; catalog is a Postgres. Limits: parallel snapshots load the
source each time; TOAST and keyless tables need care.

**Estuary Flow.** Collections are Gazette journals (append-only, fragments in the
customer's bucket, content-addressed names with offsets and SHA); a collection's key
has a "total order" within a logical partition. Reductions annotate the schema
(`"reduce": {"strategy": ...}`: `append`, `firstWriteWins`, `lastWriteWins`, `merge`,
`minimize`, `maximize`, `set`, `sum`); they "must be associative" because Flow "does not
guarantee that documents are reduced in sequential order". Materialization protocol:
`Open/Opened` (checkpoints exchanged), `Acknowledge/Acknowledged`, `Load/Loaded`,
`Flush/Flushed`, `Store`, `StartCommit/StartedCommit`; two exactly-once patterns -
"Remote Store is Authoritative" (the runtime checkpoint is committed inside the
destination's transaction, fenced by a nonce against zombie shards) and "Recovery Log
with Idempotent Apply" (the recovery log is authoritative, the driver stages effects in
stable storage and applies them idempotently); "updates to the checkpoint and to the
view state MUST always commit together"; delta-update bindings never get `Load`; the
next transaction's loads overlap the previous commit.

**Artie.** Transfer buffers per table in memory and flushes on an interval (10 s
default) or memory, merges through a staging table; Multi-Step Merge stages several
flushes before one merge (dedup at merge). System columns: `__artie_delete`,
`__artie_only_set_delete` (S3), `__artie_updated_at` (processed time),
`__artie_db_updated_at` (source transaction time), history mode writes every change to
`<table>__history` with `__artie_operation` = `CREATE`/`UPDATE`/`DELETE`,
`__artie_source_metadata` (LSN, transaction id). Typing: relational sources' schema is
read directly and applied even for all-NULL columns; document sources infer from the
first non-null value; once a destination type exists it is the source of truth;
Postgres unbounded `numeric` lands as a string.

**Airbyte Destinations V2.** Raw table (`_airbyte_raw_id`, `_airbyte_data`,
`_airbyte_extracted_at`, `_airbyte_loaded_at`, `_airbyte_meta` with typing errors)
then a typed final table, deduped by primary key keeping the latest by cursor then
`_airbyte_extracted_at`; CDC columns `_ab_cdc_lsn`, `_ab_cdc_updated_at`,
`_ab_cdc_deleted_at`; a type mismatch nulls the field and records it in `_airbyte_meta`
rather than failing; schema change may drop and recreate the final table. State types:
STREAM, GLOBAL (`shared_state` + per-stream), LEGACY; the destination emits a state only
after everything before it is persisted, and in socket mode checks
`sourceStats.recordCount` before committing.

**Sling.** Modes `full-refresh`, `incremental`, `truncate`, `snapshot`
(`_sling_loaded_at`), `backfill` (a `range` on the `update_key`), `definition-only`,
`change-capture`. Incremental strategies: primary key + update key = new-data upsert;
primary key only = full upsert; update key only = append after `max(update_key)`. The
watermark is computed at run time as `max(update_key)` on the target - nothing is
stored, and late rows below it are missed unless a lookback is written into custom SQL.
Chunking (`chunk_size` as `12h`/`7d`/`200`, or `chunk_count`) with `SLING_THREADS` is a
Pro feature.

**dlt.** `write_disposition="merge"` strategies: `delete-insert` (stage, dedup by
`primary_key` with a `dedup_sort` hint, delete by `merge_key`/`primary_key`, insert,
one transaction), `scd2` (`_dlt_valid_from`/`_dlt_valid_to`, row hash in `_dlt_id`,
absent rows retired unless a `merge_key` says the load is partial), `upsert`
(`MERGE` on a unique primary key, `hard_delete` hint), `insert-only`. Incremental:
`dlt.sources.incremental(cursor_path, initial_value, end_value, last_value_func,
row_order, range_start/range_end, on_cursor_value_missing, lag)`; rows at the boundary
are deduplicated by hash or primary key; state lives in `_dlt_pipeline_state` in the
destination and is restored from there.

**Singer / Meltano SDK.** `RECORD`, `SCHEMA` (with `key_properties`,
`bookmark_properties`), `STATE` (opaque `value`). Conventions: `bookmarks` per stream
with `replication_key`/`replication_key_value`, `currently_syncing`, partitions with
their own bookmarks; for unsorted streams the SDK keeps a `progress_marker` that is
"ignored (reset and wiped) for the purposes of resuming a failed sync" and finalizes it
only at 100 %; `is_sorted` streams resume from the last state; `ACTIVATE_VERSION`
retires an old full-table version; targets emit state after flushing.

**Kafka Connect JDBC sink.** See section 2, last paragraph.

---

## 4. Multi-master and trigger-based tools

**pglogical 2.** Output plugin on logical decoding; `synchronize_data` copies with
`COPY` under the slot's exported snapshot; `pglogical.conflict_resolution` = `error`,
`apply_remote` (default), `keep_local`, `last_update_wins`, `first_update_wins` (the last
two need `track_commit_timestamp=on`); `forward_origins='{}'` blocks non-local origins
(loop prevention), `'{all}'` forwards; provider-side row filters; DDL only through
`pglogical.replicate_ddl_command()`; `pglogical.batch_inserts` after five inserts in a
transaction; limits: `UPDATE`/`DELETE` need a primary key or replica identity, no large
objects, `TRUNCATE ... CASCADE` only local, superuser for management, keep a single
unique index on multi-source tables.

**Spock (pgEdge).** Same `spock.conflict_resolution` set; conflict-free delta-apply
columns: `ALTER TABLE t ALTER COLUMN c SET (log_old_value=true,
delta_apply_function=spock.delta_apply)` - "new-value = current-value + (remote-new -
remote-old)", made possible by a patch to PostgreSQL that logs old values of flagged
columns; one conflict per row resolved by last-write-wins, a second goes to
`spock.exception_log`; snowflake sequences; PR #617 documents divergence under LWW when
one side touched a TOAST column (not WAL-logged when unchanged) - the fix is
`REPLICA IDENTITY FULL` with the primary key as identity. migkit reaches the same
counter semantics without a server patch because the log already carries the before
image (`twoway.py:327` `_as_added`, `canon.py:97` `Added`, `base.py:3865`
`_apply_added`: `n = n + by` inside the batch's transaction), and refuses a counter
column that is not numeric (`twoway.py:236-240`).

**Bucardo.** Triggers write the primary key and `txntime` to `bucardo_delta`;
`bucardo_track` records which target already got it; a change `NOTIFY`s the daemon,
the controller starts or signals a "kid" that opens a transaction, disables triggers,
gathers changed keys since the last run, reads the current row from the source and
applies it; `conflict_strategy` = `bucardo_source`, `bucardo_target`, `bucardo_skip`,
`bucardo_random`, `bucardo_latest` (the database whose table changed most recently
wins, recomputed per conflict), `bucardo_latest_all_tables` (cached per run),
`bucardo_abort`, or custom code; `onetimecopy` for a full copy; `bucardo_purge_delta`
cleanup; primary key required; no DDL.

**SymmetricDS.** Triggers write `sym_data` (`row_data`, `old_data`, `pk_data` as CSV,
`event_type` I/U/D, `source_node_id`); `sym_data_event` links to batches;
`sym_outgoing_batch`/`sym_incoming_batch` with statuses; routers `default`, `column`,
`subselect`, `lookuptable`, `audit`; stages extract -> send -> load; initial load by
`sym_table_reload_request` with `initial.load.use.extract.job.enabled`; `sym_conflict`
detection `USE_PK_DATA`, `USE_OLD_DATA`, `USE_CHANGED_DATA`, `USE_TIMESTAMP`,
`USE_VERSION`, resolution `NEWER_WINS`, `FALLBACK`, `IGNORE`, `MANUAL` (into
`sym_incoming_error`); loops prevented by `source_node_id` and
`sync_on_incoming_batch=false`. migkit's conflict detection is `USE_OLD_DATA`
(`before` image against the target's row, `twoway.py:217` `resolve`), with
`insert_exists`/`update_missing` needing no before image, and every decision written
with both versions to `conflicts.jsonl` (`twoway.py:383`).

**pg_chameleon.** `init_replica`: `FLUSH TABLES WITH READ LOCK`, read master status,
copy in slices by CSV/`COPY`, unlock; binlog rows land as JSONB in
`sch_chameleon.t_log_replica_1/2` (alternating) with `t_batch`; `read_replica` and
`replay_replica` are separate daemons, replay by a PL/pgSQL function; `sql_token`
rewrites `CREATE/DROP/ALTER TABLE`, `DROP PRIMARY KEY`, `TRUNCATE`, `RENAME`; keyless
tables are loaded but not replicated; foreign keys created `NOT VALID`.

**Maxwell.** `maxwell.bootstrap` table or `maxwell-bootstrap`; events
`bootstrap-start`, `bootstrap-insert` per row, then any concurrent `insert`/`update`/
`delete`, then `bootstrap-complete`; `--bootstrapper=sync` blocks the binlog,
`async` bootstraps on a thread and queues the table's binlog events until done;
`select * from table` with no chunking; a crash re-runs the whole bootstrap; positions
in `maxwell.positions` (file/pos or `gtid_set`).

**canal.** Poses as a replica (`COM_BINLOG_DUMP`); `EventParser` -> `EventSink` ->
`EventStore` (ring buffer with put/get/ack cursors) -> `MetaManager` (file/pos, GTID,
timestamp fallback); ZooKeeper HA; a table-meta TSDB keeps DDL history; canal-adapter
does ETL full loads to RDB/ES/HBase; at-least-once; no bootstrap in the core.

---

## 5. Managed and streaming engines

**Google Datastream.** Backfill and CDC run concurrently per table; "the event order
isn't guaranteed" and "delivery occurs at least once"; each event carries `uuid`,
`read_timestamp`, `source_timestamp`, `sort_keys`, `read_method`
(`datastream-backfill`/`datastream-cdc` families: `mysql-cdc-binlog`,
`postgres-cdc-wal`, `oracle-cdc-logminer`, ...), and `source_metadata` (`log_file`,
`log_position`, `change_type`, `is_deleted`, `primary_keys`, `tx_id`; Oracle `scn`,
`rs_id`, `ssn`; PostgreSQL `lsn`); the consumer orders and dedups. Sources: Oracle
LogMiner or a binary log reader, MySQL binlog (GTID optional), PostgreSQL slot plus
publication, SQL Server change tracking or transaction logs; backfill by row-range
chunks; limits: keyless tables, 20 MB (BigQuery) / 100 MB (GCS) rows, DDL by hand.

**Materialize.** One slot per source; the initial snapshot runs in a transaction tied
to the slot's creation point, then the stream; LSN tracked for exactly-once; after an
interruption "the source stalls, then resumes from its committed LSN";
`REPLICA IDENTITY FULL` required; a schema change errors the subsource
(`ALTER SOURCE ... ADD SUBSOURCE`, `DROP`).

**RisingWave.** Section 1.3.

**Vitess VDiff v2.** Section 1.5.

---

## 6. migkit's map, file by file

| Concern | Where | What it does |
|---|---|---|
| Position before copy | `hetero.py:2894` `tail_start`; `postgres.py:1260`, `:1336`; `mysql.py:1071`; `mongodb.py:532`; `mssql.py:856` | Token written before the first row is read; a slot that vanished stops the tail |
| Change reading | `postgres.py:1350` (peek; advance on returned token; `upto=pg_current_wal_lsn()`; two-way mark; keyless table stops), `mysql.py:762` (`BinLogStreamReader` non-blocking, `binlog_row_metadata`/`row_image` checks, compressed payloads, token only outside a transaction `:933`, purged binlog stops `:942`), `mongodb.py:558`, `mssql.py:895`, `dynamodb.py` | Neutral `canon.change` records with a resumable token |
| Test_decoding parser | `pgslot.py:1-50`, `:104`, `:157` | Quoting, `''`, unquoted `null`, `old-key:`/`new-tuple:`, `REPLICA IDENTITY FULL` delete omits NULL columns |
| Tail loop | `hetero.py:2919` `tail_apply` | Batches 1,000 growing to 16,000 while behind; read-ahead only where `READS_AHEAD` (`mysql.py:73`; `hetero.py:3196` `_ReadAhead`); applied first, token second; transient loss returns to the saved token with backoff; SIGTERM as ctrl-c; triggers held off via `load_window` |
| Shape gate | `hetero.py:2808`; `drift.py:22`, `:36`, `:60` | Schema saved beside the token; stop before a batch the target cannot take |
| Applier | `base.py:3574`, `:3609` `_lanes`, `:3653` `_net_rows`, `:3837` `_apply_net`, `:3865` `_apply_added`, `:3917` `_collapsed` | Idempotent upsert/delete; collapse per key; runs; lanes by key hash or FK group; counters added in place |
| Two-way | `twoway.py:48` `migkit_origin`, `:49` policies, `:217` `resolve`, `:327`, `:383`; backlog R3 `docs/backlog.md:3663` | Own writes left out by the origin mark; `error`/`apply_remote`/`keep_local`/`last_update_wins`/`source_priority`; `delta` counters; exact batches; the rung ladder (PG replication origin, logical message, table; MySQL tagged GTID, MariaDB domain/`skip_replication`, statement comment, table) |
| Copy ranges and resume | `ranges.py:222`, `:242`, `:260`, `:322`; `checkpoint.py:51-140`, `:172` | Ranges planned once; keyless spans reconciled by count; verify partials fingerprinted and summed out of order |
| Shared snapshot / fence | `postgres.py:6483`, `:6524`, `:6768` | One exported snapshot across lanes; fence on `confirmed_flush_lsn` |
| Slot headroom | `postgres.py:1311` | `safe_wal_size`, held bytes for `/metrics` |
| Debezium wrap | `movers.py:3180-3215`, `:3349`, `:3398`, `:3416`, `:3446` | Kafka signal channel; blocking vs read-only incremental re-snapshot measured; never a table on the source |
| Skip-unchanged verify | `unchanged.py` | Tuple counters + `relfilenode` + size; any doubt re-verifies |
| Target freeze | `freeze.py` | Per-role revoke + `default_transaction_read_only` + session end |
| Tests | `tests/test_a_copy_and_its_changes_interleave_safely.py`, `test_full_cdc_misses_nothing.py`, `test_cdc_pg_source.py`, `test_shared_snapshot.py`, `test_resnapshot_signal.py`, `test_sql_server_follows_through_change_tracking.py`, `test_the_tail_stops_at_a_ddl.py`, `test_two_ways_through_migkits_own_tails.py`, `test_the_streaming_copy_goes_on_after_a_stop.py`, `test_the_tail_rides_out_a_lost_connection.py` | |

Airbyte's three public bugs map onto migkit's failpoints: a per-chunk "final" state
(#29237) cannot happen because `ranges_done` and `last` are separate
(`ranges.py:242`); an LSN committed past the saved offset (#49803) cannot because the
token is written before the copy and the slot is advanced only by the returned token;
a snapshot skipped after a rejected state (#80939) cannot because the checkpoint
fingerprint discards partials from another table state (`checkpoint.py:88`).

---

## 7. Gaps, and how migkit builds each better than the tool that has it

1. **Changes the copy already reflects (lever 8; DBLog step 6, Vitess `lastpk`).** Rule:
   for each key range `r`, record `pos(r)` = the source's log position read *before*
   the range's read snapshot opens (`pg_current_wal_lsn()` then the snapshot; MySQL
   `SHOW MASTER STATUS` then `START TRANSACTION WITH CONSISTENT SNAPSHOT`; Mongo
   `clusterTime`; SQL Server `CHANGE_TRACKING_CURRENT_VERSION()`), kept next to
   `ranges_done`. The tail applies a change iff `change.pos >= pos(range_of(key))`;
   a range not yet read has `pos = +inf` (dropped: the copy will read it); keyless
   tables never drop. Safe direction only: a commit between the position read and the
   snapshot open is both in the log after `pos` and visible to the read, so it is
   applied again, idempotently. No watermark table, no window buffer, no write to the
   source - the position is read, not written, which is what Debezium's `read.only`
   mode needs a GTID set or `pg_current_snapshot()` for; here the copy's own ranges
   are the chunks. Proof: extend the Hypothesis test with per-chunk positions and the
   rule; docker: PG and MySQL with a writer during the copy, digest equal after.
   Effort M.
2. **Re-read one table while the tail runs (Debezium incremental signal, on migkit's
   own tail).** `migkit tail resnapshot <table>`: pause between batches
   (`tailctl.pause`), plan ranges, compare range digests first so only differing
   ranges are copied (R19.3), record `pos(r)` per range as in item 1, resume; the tail's
   rule from item 1 then makes the re-read safe against changes arriving meanwhile.
   Better than Debezium: read-only on every engine, not only GTID MySQL and PG 13+,
   and only the ranges that differ are read. Effort M (after item 1).
3. **Transaction-bounded batches on PostgreSQL.** MySQL saves a token only outside a
   transaction (`mysql.py:933`); the PostgreSQL peek is bounded by a line count and
   its token is the last line's LSN, so a batch can end inside a transaction and a
   crash leaves the target holding half of it until the resume replays the rest
   (converges; not atomic). Fix: end the batch at the last `COMMIT` line seen, carry the
   remainder to the next read; when one transaction exceeds the batch, grow the batch
   (`TAIL_BATCH_MOST`) or say so. This is Debezium's `transaction.boundary=poll` and
   RisingWave's `transactional=true`. Effort S; docker: a 5,000-row transaction with a
   failpoint after the first batch.
4. **`unchanged-toast-datum`.** Measure with `postgres:16`: a row with a 4 KB `text`,
   `REPLICA IDENTITY DEFAULT`, `UPDATE` of an `int` column, read through
   `pg_logical_slot_peek_changes`. If the token appears, `pgslot.value` must return
   "not carried" (drop the column from `values`) so `_collapsed`'s merge keeps the
   target's value - the same outcome as Estuary's merge reduction without a placeholder
   string ever reaching the target. Effort S.
5. **MongoDB pre-images.** Where a collection already has
   `changeStreamPreAndPostImages` enabled (read `collMod` state from
   `listCollections`), open the stream with `fullDocumentBeforeChange="whenAvailable"`
   and pass it as `before`, so two-way on MongoDB stops being blind. Never enable it:
   that is a source write. Effort S; docker `mongo:7` replica set.
6. **Change Tracking under `SNAPSHOT` isolation and with context.** Wrap
   `mssql.neutral_changes` in `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` when
   `ALLOW_SNAPSHOT_ISOLATION` is on (cleanup race and cross-table consistency, per
   Microsoft's protocol); write with `WITH CHANGE_TRACKING_CONTEXT(<migkit id>)` as the
   SQL Server rung of the two-way ladder (apart, not exact); check
   `CHANGETABLE(VERSION ...)` for conflicts. Also guard the restore hazard: keep the
   database's `database_guid` and `create_date` in the token. Effort S; docker Azure
   SQL Edge.
7. **Transaction metadata in the streaming envelopes.** Add the source transaction id
   and position (`xid:lsn`, GTID, `clusterTime`, CT version) and an in-transaction
   order to `avrostream`'s envelope, and a `BEGIN`/`END` pair per transaction as an
   option, so a Kafka consumer can rebuild atomicity as Debezium's `transaction` topic
   allows. Effort S.
8. **Exact batches on one-way hops and for weak targets.** `exact` today is tied to
   `two_way`. Offer it on any hop whose target can hold a mark (every SQL engine; Mongo
   by a document), so restarts resume after the last committed batch instead of
   replaying a window - Estuary's "Remote Store is Authoritative". For Kafka, the
   "Recovery Log with Idempotent Apply" pattern: a transactional producer
   (`enable.idempotence`, `transactional.id` = hop) committing a batch's records and
   the token together. Effort M; docker redpanda.
9. **Resync with an atomic swap (PeerDB).** `move --resync <table>`: copy into
   `<table>__migkit_resync` under item 1's positions, verify by range, swap in one
   transaction (`ALTER TABLE ... RENAME`), then drop; soft-deleted rows do not exist in
   migkit so nothing to carry. Effort M.
10. **History mode / soft delete / SCD2** (Artie, PeerDB, dlt). A migration tool's
    target is the application's database, so not by default; as an option on
    warehouse, DuckDB and Kafka targets: `history: true` appends every change with
    `_migkit_op`, `_migkit_source_pos`, `_migkit_db_time`; `soft_delete: true` sets
    `_migkit_deleted` instead of deleting. Effort M.
11. **DDL text, not only shape.** MySQL's `QueryEvent` carries the statement; keep it
    in the stop message and in `tail-shape.json`, so the operator sees "ALTER TABLE
    orders ADD COLUMN note text" rather than only the resulting shape. Optionally apply
    additive columns automatically (Debezium JDBC `schema.evolution=basic`) behind an
    option. Effort S.
12. **`first_update_wins`** is missing from the policy set (pglogical/Spock have it);
    trivial beside `last_update_wins`. Effort S.
13. **Log-less incremental (Sling/dlt cursors).** A `cursor: updated_at` tail rung for
    sources with no log, with a lookback and boundary dedup by key (dlt's hash), state
    beside the token; refused for tables without a monotone cursor. Effort M.
14. **Oracle, Cassandra, Spanner change readers.** None today; Oracle behind R11/R12
    (LogMiner through `python-oracledb`, or wrapping OpenLogReplicator's output). L.

What none of the tools above do, and migkit already does: the slot advanced only by
evidence of apply (`postgres.py:1350`); the applier that keeps source order where a
foreign key or a second unique index binds rows and collapses everywhere else
(`base.py:3653`); a verify whose partial results add up out of order
(`checkpoint.py`); counters merged from the log's own before image with no server
patch (`twoway.py:327`); the copy-and-changes invariants held by a property test
rather than an argument.

---

## 8. Table

| mechanism | tool | what it gives | migkit status | how migkit builds it better | effort | docker |
|---|---|---|---|---|---|---|
| Watermarked chunk dedup (steps 1-7) | Netflix DBLog, Debezium incremental | lock-free full capture beside a live log | invariants property-tested; copy-then-tail applies everything; lever 8 not built (`backlog.md:4531`) | per-range `pos(r)` read, not written; apply iff `change.pos >= pos(range_of(key))`; no watermark table, any engine | M | yes |
| Read-only watermarks | Debezium `read.only` (GTID / `pg_current_snapshot`) | no write to the source | only through the Debezium wrap (`movers.py:3398`, `:3416`) | item 1 reads positions the copy already has; covers Mongo and SQL Server too | M | yes |
| Ad-hoc re-snapshot of one table while streaming | Debezium signals (`execute-snapshot`) | repair a drifted table without restart | Kafka signal to Debezium (`movers.py:3349`); own tail: none | `tail resnapshot`: pause, digest-compare ranges, copy only differing ranges under item 1's rule | M | yes |
| Blocking snapshot | Debezium | simplest re-read | used and measured (`movers.py:3349`) | keep as the fallback rung | done | yes |
| Copy / catch-up / fast-forward by `lastpk` | Vitess VReplication | per-table progressive consistency, resumable | `ranges.plan`/`finished`; tail after copy | item 1 is fast-forward per range without a per-batch GTID wait | M | yes |
| Resumable diff by last PK | VDiff v2 | restartable verify | `checkpoint.py` fingerprinted partials, commutative sums (parallel, out of order) | done; already stronger | - | yes |
| Exported snapshot + CTID/key ranges in parallel | PeerDB, pgcopydb, Airbyte, Artie, pgstream | fast initial load | shared snapshot for verify lanes (`postgres.py:6483`); parallel range copy sized by measurement (R1, R19.5) | keep; add binary COPY end to end (R19.2) | S | yes |
| Slot / position before the copy | Debezium, Airbyte, Materialize | no gap between copy and stream | `tail_start` `hetero.py:2894`, `_slot_ready` `:1260`; tested | done | - | yes |
| Peek, advance on evidence | (migkit) vs Debezium periodic offset commit | crash between read and apply loses nothing | `postgres.py:1350` | done | - | yes |
| Exactly-once apply (target-side ledger) | Estuary "Remote Store is Authoritative"; KIP-618 | a batch applied once | `exact` on two-way hops; R3 ladder planned | offer on any hop; PG replication-origin rung; transactional Kafka producer | M | yes |
| Transaction-bounded batches | Debezium `transaction.boundary=poll`; RisingWave `transactional` | target never shows half a transaction | MySQL yes (`mysql.py:933`); PostgreSQL no | end PG batches at `COMMIT`; grow the batch for one large transaction | S | yes |
| Transaction metadata (BEGIN/END, order) | Debezium `provide.transaction.metadata` | consumers rebuild atomicity | not in envelopes | add id/position/order to `avrostream` | S | yes |
| Schema history / shape gate | Debezium schema history topic | survive DDL | `_shape_gate` `hetero.py:2808`; stops, never mis-applies | add DDL text from `QueryEvent`; optional additive auto-ALTER | S | yes |
| Unavailable TOAST/LOB value | Debezium placeholder; Estuary merge reduction | partial update does not null a column | `_collapsed` merges partials; `unchanged-toast-datum` token unhandled in `pgslot.py` (to measure) | parse the token as "not carried"; nothing false ever reaches the target | S | yes |
| Pre-images | Debezium `capture.mode ..._with_pre_image`; Mongo 6 `fullDocumentBeforeChange` | conflict detection with a real before | not used; two-way blind on Mongo (`twoway.py:301`) | read pre-images where already enabled; never enable them | S | yes |
| Change Tracking under `SNAPSHOT`, context, `CHANGETABLE(VERSION)` | Microsoft CT protocol; Datastream uses CT | consistent version and rows; own-write marking | `mssql.py:895` without a snapshot transaction; `min_valid` checked | snapshot-wrapped read; CT context as the SQL Server origin rung; restore guard in the token | S | yes |
| Heartbeat / slot advance on idle | Debezium `heartbeat.action.query`; PeerDB WAL heartbeat; Estuary heartbeat table | WAL not pinned by an idle database | token moves to `pg_current_wal_lsn()` when caught up (`postgres.py:1373`); `stream_room` `:1311` | done; an alert threshold on `held_bytes` in `watch` | S | yes |
| Raw-then-normalize (staging + MERGE) | PeerDB, Airbyte V2, Artie MSM | warehouse throughput | PG COPY staging path (R2.4); SQL Server pending | staging + `MERGE` on SQL Server and warehouses; `_net_rows` order kept | M | partly |
| In-batch reduction (last state per key) | Debezium JDBC `use.reduction.buffer`; DMS batch apply | fewer statements | `_collapsed` + `_net_rows` FK/unique-aware (`base.py:3653`) | done; already keeps order where constraints bind | - | yes |
| Conflict policies + delta apply | pglogical, Spock `delta_apply`, Bucardo, SymmetricDS | multi-master | 5 policies + counters from the log's before image, no server patch (`twoway.py`) | add `first_update_wins`; per-column decision log already in `conflicts.jsonl` | S | yes |
| Loop prevention / origin | pglogical `forward_origins`; SymmetricDS `source_node_id`; CT context | no echo | `migkit_origin` table rung; ladder designed (R3) | PG replication origin (apart and exact), MySQL tagged GTID, MariaDB domain, CT context | M | yes |
| Trigger capture fallback | Bucardo, SymmetricDS | engines with no log | none | only as a last rung for log-less engines; low priority | L | yes |
| Bootstrap events / history mode / soft delete / SCD2 | Maxwell, Artie, PeerDB, dlt | audit trail, lakehouse dimensions | none (Kafka tombstones are the changelog) | option on warehouse/DuckDB/Kafka targets | M | yes |
| Cursor incremental, boundary dedup, state in destination | dlt, Sling, Singer | no log needed | `unchanged.py` marker; digest re-sync (R19.3) | a `cursor` tail rung with lookback and key dedup | M | yes |
| Unordered at-least-once with `sort_keys` | Datastream | consumer orders | not needed: migkit orders at apply | keep | - | - |
| Parallel splits with per-split watermark, then filtered log split | Flink CDC, RisingWave | parallel snapshot merged live | parallel range copy; tail applies after | item 1 gives the filtered-split semantics without buffering a chunk | M | yes |
| Load/Store/StartCommit with recovery log | Estuary materialization protocol | exactly-once into weak stores | `exact` mark for strong stores; Kafka at-least-once keyed | transactional producer keyed by batch; token committed with the records | M | yes |
| Resync with atomic swap | PeerDB | rebuild a table with no downtime | copy into a set-aside table exists; no swap command | `move --resync`: copy, verify, rename in one transaction | M | yes |
| Oracle LogMiner / XStream / OLR; Cassandra commit log; Spanner partitions | Debezium | reach | none | R11/R12; Oracle first | L | Oracle: no (licence); Cassandra: yes |

---

## Sources

- DBLog paper: https://arxiv.org/abs/2010.12597 (PDF: https://arxiv.org/pdf/2010.12597)
- Debezium incremental snapshots: https://debezium.io/blog/2021/10/07/incremental-snapshots/
- Debezium read-only incremental snapshots (MySQL): https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/
- Debezium signalling: https://debezium.io/documentation/reference/stable/configuration/signalling.html
- Debezium exactly-once: https://debezium.io/blog/2023/06/22/towards-exactly-once-delivery/
- Debezium PostgreSQL connector: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Debezium MySQL connector: https://debezium.io/documentation/reference/stable/connectors/mysql.html
- Debezium Oracle connector: https://debezium.io/documentation/reference/stable/connectors/oracle.html
- Debezium SQL Server connector: https://debezium.io/documentation/reference/stable/connectors/sqlserver.html
- Debezium MongoDB connector: https://debezium.io/documentation/reference/stable/connectors/mongodb.html
- Debezium Cassandra connector: https://debezium.io/documentation/reference/stable/connectors/cassandra.html
- Debezium Vitess connector: https://debezium.io/documentation/reference/stable/connectors/vitess.html
- Debezium Spanner connector: https://debezium.io/documentation/reference/stable/connectors/spanner.html
- Debezium JDBC sink: https://debezium.io/documentation/reference/stable/connectors/jdbc.html
- Confluent JDBC sink: https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html
- Flink CDC MySQL incremental snapshot: https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/flink-sources/mysql-cdc/
- RisingWave PostgreSQL CDC: https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc
- Estuary concepts: https://docs.estuary.dev/concepts/ ; reductions: https://docs.estuary.dev/reference/reduction-strategies/ ; journals: https://docs.estuary.dev/concepts/advanced/journals/ ; shards: https://docs.estuary.dev/concepts/advanced/shards/ ; materialization: https://docs.estuary.dev/concepts/materialization/ ; materialization protocol: https://docs.estuary.dev/reference/Connectors/materialization-protocol/ ; PostgreSQL capture: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/ ; backfill modes: https://docs.estuary.dev/reference/backfilling-data/ ; read-only mode announcement: https://www.postgresql.org/about/news/postgresql-cdc-evolved-read-only-mode-iam-auth-partition-support-now-in-estuary-3261
- Vitess life of a stream: https://vitess.io/docs/reference/vreplication/internal/life-of-a-stream/ ; VDiff: https://vitess.io/docs/reference/vreplication/vdiff/
- PeerDB architecture: https://docs.peerdb.io/architecture ; CDC mirror: https://docs.peerdb.io/mirror/cdc-pg-clickhouse ; resync: https://docs.peerdb.io/features/resync-mirror ; CDC configs: https://docs.peerdb.io/metrics/important_cdc_configs ; parallel initial load: https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres ; pg_dump 5x: https://blog.peerdb.io/how-can-we-make-pgdump-and-pgrestore-5-times-faster ; 1 TB in 2 h: https://clickhouse.com/blog/practical-postgres-migrations-at-scale-peerdb ; scale testing: https://github.com/PeerDB-io/ab-scale-testing
- pgcopydb snapshot constraint: https://github.com/dimitri/pgcopydb/blob/main/docs/resume.rst
- pgstream snapshots: https://xata.io/blog/behind-the-scenes-speeding-up-pgstream-snapshots-for-postgresql
- Artie docs index: https://artie.com/docs/llms.txt ; backfill: https://artie.com/docs/pipelines/backfill ; system columns: https://artie.com/docs/pipelines/system-columns ; typing: https://artie.com/docs/guides/artie/arties-typing-library ; online backfills: https://www.artie.com/blogs/online-database-backfill ; multi-step merge: https://www.artie.com/blogs/multi-step-merge ; Artie vs DMS: https://www.artie.com/blogs/artie-vs-aws-dms
- Airbyte CDC: https://docs.airbyte.com/platform/understanding-airbyte/cdc ; Postgres source: https://docs.airbyte.com/integrations/sources/postgres ; protocol/state: https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol ; typing and deduping: https://docs.airbyte.com/platform/using-airbyte/core-concepts/typing-deduping ; resumability: https://docs.airbyte.com/platform/understanding-airbyte/resumability ; issues: https://github.com/airbytehq/airbyte/issues/29237 , https://github.com/airbytehq/airbyte/issues/49803 , https://github.com/airbytehq/airbyte/issues/80939 , https://github.com/airbytehq/airbyte/issues/26492
- Sling modes: https://docs.slingdata.io/concepts/replication/modes ; chunking: https://docs.slingdata.io/examples/database-to-database/chunking ; backfill: https://docs.slingdata.io/examples/database-to-database/backfill
- dlt merge loading: https://dlthub.com/docs/general-usage/merge-loading ; incremental cursor: https://dlthub.com/docs/general-usage/incremental/cursor
- Singer spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md ; Meltano SDK state: https://sdk.meltano.com/en/latest/implementation/state.html
- pglogical: https://github.com/2ndQuadrant/pglogical/blob/REL2_x_STABLE/docs/README.md
- Spock: https://docs.pgedge.com/spock-v5/v5-0-5/install_spock/ ; https://docs.pgedge.com/platform/prerequisites/configuring/ ; https://github.com/pgEdge/spockbench ; https://github.com/pgEdge/spock/pull/617
- Bucardo: https://bucardo.org/Bucardo/operations/conflict_handling ; https://bucardo.org/Bucardo/schema/ ; https://github.com/bucardo/bucardo_org/blob/master/Bucardo/Overview.md
- SymmetricDS user guide: https://symmetricds.sourceforge.net/doc/3.15/html/user-guide.html
- pg_chameleon: https://github.com/the4thdoctor/pg_chameleon
- Maxwell bootstrapping: https://maxwells-daemon.io/bootstrapping/
- canal: https://github.com/alibaba/canal/wiki/Introduction
- Google Datastream behavior: https://docs.cloud.google.com/datastream/docs/behavior-overview ; events: https://docs.cloud.google.com/datastream/docs/events-and-streams
- Materialize PostgreSQL source: https://materialize.com/docs/sql/create-source/postgres/
- MongoDB change streams: https://www.mongodb.com/docs/manual/changeStreams/
- SQL Server Change Tracking: https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-tracking-sql-server
