# Leapfrog: changing a running tail, reading changes without the engine's CDC, closed capabilities rebuilt open, cloud control-plane processes imitated (2026-09-28)

## Status

* 2026-09-28 started. Read: backlog "Decided by the owner 2026-09-28",
  "Research round 2026-09-28", R2, R3, R19; the four mechanism reports and
  paid-cloud-products (not repeated here - cited by section); code:
  `migkit/engines/hetero.py` `tail_apply` (`:2919`), `_shape_gate`
  (`:2808`), `_tail_targets` (`:2707`), `migkit/drift.py`,
  `migkit/tailctl.py`, `migkit/twoway.py`.
* **Complete** (2026-09-28): 0 baseline, 1 live table set + DDL, 2
  reading changes without the CDC feature, 3 closed capabilities rebuilt,
  4 control-plane processes imitated, 5 the table (26 items, order of
  work), sources. Public sources only; nothing run, no database touched.
* Claims marked "to be measured" / "to be proven" are designs whose
  exactness must be shown in the sandbox before `assess` states them:
  the MySQL seam without replay (L3), `pg_walinspect` under
  `rds_superuser` (R9), IDLETIME on a replica (C6), the balancer left on
  under mongosync's job (C1), Oracle Free as a mining database (C5).
* Headline findings: (a) PostgreSQL's snapshot visibility is not
  commit-LSN order, so the per-range seam must be decided by xid
  visibility, not by an LSN (L2); (b) two paid/open tools lost rows
  silently on a scope change (TiDB DM #12859) or inside the server
  (PostgreSQL before 17.5, `ALTER PUBLICATION ADD TABLE`) - a per-table
  accounting identity (L7) catches that class; (c) with the CDC feature
  off, a stated depth is reachable on every engine, and on SQL Server the
  privileged log reading can move entirely onto a migkit-owned standby
  restored from the log-backup chain (R2); (d) Kafka offsets can be kept
  identical from a client for gap-free partitions (pad + `DeleteRecords`,
  C2), and translated exactly otherwise (map in the same transaction, C3);
  (e) XStream's downstream capture is reachable with LogMiner on a
  migkit-owned Oracle Free mining instance (C5).

## 0. What migkit's tail is today (the baseline every design below changes)

* One tail per database, one position (`token`) per database, saved after
  each batch is applied (`hetero.py:2919`); exact batches numbered where
  two-way is on (`twoway.exact`).
* Scope = the hop file's exclusions (`drift.shape` filters by
  `hop.excluded`); a table created on the source after the tail began is
  applied under its mapped name (`_tail_target`, `:2802`) - but nothing
  copies its existing rows, so a table *added to scope* mid-tail receives
  only its future changes.
* DDL: `_shape_gate` reads the source catalogue before each batch against
  `tail-shape.json`; if the target lacks a column the source now has, the
  **whole tail stops** (`SystemExit`) - every table, not only the one the
  DDL touched.
* Pause: `tailctl.pause` holds the **whole** tail between batches (files
  `tail.pause` / `tail.paused`); there is no per-table pause.
* Debezium's incremental snapshot is used only when Debezium is the mover
  (`movers.py:3349` `resnapshot_message`, Kafka signal channel).

## 1. Changing a running tail's table set and DDL during the tail

Already in the earlier reports and not repeated: DBLog/Debezium watermark
mechanics (cdc-elt s.1.1-1.2), DM's pessimistic/optimistic shard DDL
(dts-tidb s.4.1), Qlik DDL-by-metadata-diff and GoldenGate ATCSN
instantiation (goldengate-qlik s.3.5, s.4.1), and the two gap rows
"change the table set of a running tail" / "schema drift isolated to one
table" (paid-cloud-products s.5 rows 6-7). What follows is what each does
*to a running task*, and how each fails.

### 1.1 How each changes a live task

| Product | Add a table | Remove a table | DDL during the tail | Do the others pause? |
|---|---|---|---|---|
| Alibaba DTS (sync tasks only; migration tasks cannot) | "Modify Objects", then a precheck; a **child task** runs schema + full + incremental for the new objects only and is **merged** into the parent once it reaches the incremental stage; status "initializing added objects" can last long | move back to Available + precheck; "the task stops synchronizing incremental data of that object"; target rows left | per-task DDL options; a failed DDL delays the task - their fix is remove table, wait for lag 0, drop it on the target, add it back | No for the add (child task); but "if the latency of the task exceeds 10 minutes, do not modify the objects"; the child's throttling is **not inherited** |
| AWS DMS | mappings change only on a **stopped** task (`ModifyReplicationTask`), then resume; `ReloadTables` (<= 10 tables, running full-load(+CDC) tasks only, not CDC-only) re-copies while the task runs | stop, edit mappings, resume | DDL handling policy set per task; `ReloadTables` "keeps the table definition it read earlier" so an ADD/DROP COLUMN before a reload can fail it; `DO_NOTHING` prep means the operator truncates first | Yes for any mapping change (task stopped); No for a reload |
| Informatica CDIR | edit + **redeploy**: "stops each job subtask for a source table", deploys, restarts them and starts new subtasks | redeploy | per DDL kind x {Ignore, Replicate, Stop Job, Stop Table}; *Resume With Options* overrides; a schema change is **detected only at the table's next DML**; PK/unique key changes are never replicated and **stop the table** (resync needed); "column selection and DDL changes made at the same time, the results might be incorrect" | Yes, briefly, all subtasks restart on redeploy; Stop Table isolates only on DDL |
| GoldenGate | `ADD TRANDATA` (12.2+: `PREPARECSN` so a Data Pump export carries the instantiation CSN) + edit Extract/Pump `TABLE` (a restart unless a wildcard covers it) + Replicat `MAP` with `DBOPTIONS ENABLE_INSTANTIATION_FILTERING`, or a per-MAP `FILTER (@GETENV('TRANSACTION','CSN') > n)`, or a **temporary second Replicat** started `AFTERCSN n` and merged later | edit params, restart | integrated capture gets DDL from the mining server; coordinated Replicat treats DDL as a barrier | Extract/Pump restart pauses capture briefly; practitioners report the per-MAP filter **misses rows if Replicat keeps running** while the export's SCN is taken (a running Replicat without the MAP moves its checkpoint past those trail records) |
| Qlik Replicate | stop, add table, **resume**: only the new table is full-loaded, the rest continue CDC from the saved position; changes to the new table during its load are **cached** and applied after | stop, remove, resume; re-adding later from a timestamp is unsafe if DDL happened meanwhile | DDL policy per task; "a DDL during Full Load ... will cause Replicate to reload the table"; Oracle drop+recreate gets a new object id and the task keeps looking at the old one (reload); rename with *Ignore ALTER* keeps delivering until stop/resume | Yes: a stop/resume of the whole task |
| Debezium | edit `table.include.list`, **restart**, then an `execute-snapshot` signal (incremental); with `schema.history.internal.store.only.captured.tables.ddl=true` the connector stops with "Encountered change event for table ... whose schema isn't known" - fix: delete the history topic, `snapshot.mode=recovery` (only safe if no captured table changed shape meanwhile) | edit, restart | schema history topic (MySQL/Oracle/SQL Server); PostgreSQL gets the shape from pgoutput `Relation` messages | Yes: a connector restart |
| Vitess MoveTables | **cannot add tables to a running workflow** (`Workflow update` changes cells, tablet types, `--on-ddl`, config - not tables); a second workflow or cancel-and-recreate | no | `--on-ddl` = IGNORE (default) / STOP / EXEC / EXEC_IGNORE per workflow; Vitess advises against EXEC* | The whole stream stops on STOP; one stream per workflow |
| TiDB DM | `stop-task`, edit block-allow-list, `start-task`; the new table's existing rows are **not copied** in the Sync stage (a separate task, then rewind the global checkpoint to the smaller position with `safe-mode` until past the larger one) | same | `binlog-schema update --from-source/--from-target` only on a **paused** task; shard merge: pessimistic/optimistic | Yes (stop-task) |

### 1.2 Failure modes worth designing against (public)

1. **Silent row loss after a scope change**: TiDB DM, task updated through
   the OpenAPI stop -> update -> start path: the rows-event decoder keeps
   the old block-allow list, so rows of added tables arrive empty and are
   dropped; the DDL worker keeps old route/filter rules, so DDL goes to
   the old target and DML to the new (pingcap/tiflow #12859, closed "not
   planned"; #12864). Nothing counted the loss.
2. **Silent loss inside PostgreSQL itself**: before 17.5 (May 2025, and
   back-branches), `ALTER PUBLICATION ... ADD TABLE` concurrent with DML
   on that table could leave the walsender decoding with a stale catalog
   cache and **filter the table's changes out**; the follow-up fix
   (13.22 etc.) removed an exponential re-distribution of invalidations.
   Any design that adds a table by altering a publication must check the
   server's minor version.
3. **Snapshot vs log ordering**: on a PostgreSQL primary, visibility
   order (leaving the ProcArray) is not commit-LSN order; a snapshot can
   see a transaction with a later commit LSN and miss one with an earlier
   one ("Long Fork", Jepsen 2025; Haas 2015). "Copy under a snapshot,
   then apply every commit after an LSN read next to it" can miss a
   transaction whose commit record precedes the LSN but which the
   snapshot did not see. A 2026 commit made `SnapBuildInitialSnapshot()`
   wait for such transactions for the *slot's exported* snapshot - it
   does not help a snapshot taken later on a running slot.
4. **Schema known only from history**: Debezium's history topic
   (`isn't known` stop, recovery unsafe after DDL), DM's internal schema
   ("Column count doesn't match value count"), DMS's cached table
   definition on reload.
5. **Detection late or coarse**: Informatica sees DDL only at the next
   DML and merges several; DTS asks for lag < 10 min before a change;
   Vitess stops the whole stream on STOP; DMS and Qlik stop the task.
6. **Instantiation seam**: GoldenGate's per-table CSN filter misses
   changes if Replicat advanced past the SCN before the MAP existed; DM
   needs a manual checkpoint rewind + safe-mode window; DMS reload needs
   a manual truncate under `DO_NOTHING`.

### 1.3 migkit's design: one stream, a state per table, nothing else stops

Goal: add or remove tables and absorb DDL in a running tail with **zero
batches withheld from any other table**, each table's seam exact, and a
proof per transition. No new mode or command: the hop file stays the
interface (scope compared at every batch boundary, as paid-cloud s.5
row 6 said); `migkit tail status` shows the per-table states.

**State per table** (in `tail-tables.json` beside the token, written in
the same step as the token):

```
absent -> copying{ranges: {r: pos(r) | in-flight | todo}} -> live
live -> parked{since: position, why, spool: file, bytes} -> live | recopy
live -> removed{at: position, target digest at removal}
```

**The stream** keeps one reader and one saved position. For each change
`c` on table `t` in key range `r` at log position `p`:

* `t` removed / not in scope: dropped, counted under `dropped.scope[t]`.
* `t` copying, `r` todo: dropped (the copy will read a later state) -
  R19 lever 8, already in the wave-1 work.
* `t` copying, `r` in flight: **buffered in memory for that range**
  (spilled with zstd past a bound); applied after the range lands, only
  the entries the range's rule admits. Not durable: a crash re-copies
  the range with a fresh `pos(r)`, and every buffered change is older
  than it.
* `t` copying, `r` landed, or `t` live: applied iff **the range's rule
  admits it** (below).
* `t` parked: appended to the table's spool (zstd JSONL, fsynced before
  the stream token moves past it), so the source's log is **not held**
  for a parked table; the spool, not the slot, carries it.

**The range's rule, exact per engine** (the seam every product above
gets wrong at least once):

* PostgreSQL: take the range's snapshot with `pg_current_snapshot()`
  (xmin, xmax, xip - xid8) inside the REPEATABLE READ transaction that
  reads the range; a change is admitted iff its **top-level xid is not
  visible in that snapshot** (`xid >= xmax or xid in xip`, epoch-extended
  from the 32-bit xid the decoder prints). This is visibility, not LSN,
  so failure mode 3 cannot happen, and it needs no watermark row on the
  source (Debezium's PostgreSQL incremental snapshot writes to a signal
  table; DBLog writes watermarks). Duplicates are skipped exactly, not
  replayed.
* MySQL: `pos(r)` = `@@gtid_executed` read **before** the range's
  consistent-snapshot transaction starts; a change is admitted iff its
  GTID is not in that set. Everything the snapshot also saw is replayed
  once more, which full-row-image upserts and deletes by key make
  harmless in commit order (the tail already requires
  `binlog_row_image=FULL`, `mysql.py:791`). Where the server gives the
  snapshot's own position (`performance_schema.log_status` under
  `BACKUP_ADMIN`; Percona/MariaDB `Binlog_snapshot_*` status) it is used
  and nothing is replayed. To be measured in docker with a race test
  before it is claimed exact without replay.
* SQL Server CT/CDC: the range is read in a SNAPSHOT transaction and
  `pos(r)` = `CHANGE_TRACKING_CURRENT_VERSION()` / `sys.fn_cdc_get_max_lsn()`
  read inside it (the F0 CT-in-SNAPSHOT fix makes this consistent).
* MongoDB: the range read with `readConcern: snapshot` at a cluster time
  `T`; admitted iff the event's `clusterTime > T`.

**Adding a table** (it appears in scope at a batch boundary): the stream
does not stop. The copy runs in the existing ranged copier's workers
(split by key quantiles, digest per range) beside the tail; each range
lands, then its buffer is applied through the rule, then it is recorded
landed with its `pos(r)`; when every range has landed the table is live.
Before the copy, migkit asks whether the target already has the table
and whether a **removal record** exists: if the table was removed at
position `q` and the source's log still holds `q` (`stream_room`), only
the changes after `q` are needed - no copy (Qlik's re-add-from-timestamp,
made exact by position and refused if `q` is gone or a DDL touched the
table meanwhile); otherwise ranges are compared by digest first and only
differing ranges are copied (R19 lever 3).

PostgreSQL specifics: migkit's own tail reads `test_decoding`, which
emits every table - no publication change, so failure mode 2 does not
arise. Where the hop's path is a native subscription (`postgres.py`
native logical replication), adding a table is `ALTER PUBLICATION ...
ADD TABLE` + `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`, and migkit
refuses it on a server older than the fixing minor (the May 2025 set:
17.5 / 16.9 / 15.13 / 14.18 / 13.21, and preferably the August 2025 set
- 17.6 / 16.10 / 15.14 / 14.19 / 13.22 - which removed the fix's
exponential re-distribution of invalidations; the exact numbers to be
read from each branch's release notes when the guard is written) with
the reason, and uses its own tail for the added table instead.

**Removing a table**: at the batch boundary its changes stop being
applied, counted; the removal position and the target table's digest at
that moment are saved, so a later `check` proves nothing else wrote it,
and a re-add can resume from the position.

**DDL during the tail, per table, positional**:

* Where the log carries the shape with every row, the tail knows the
  exact shape of each change: MySQL with `binlog_row_metadata=FULL`
  (already required, `mysql.py:782`) puts column names and types in
  every `TABLE_MAP` event, and the DDL's `QueryEvent` marks its position;
  PostgreSQL's `pgoutput` sends a `Relation` message before the first
  change after a shape change (`test_decoding` does not - the tail reads
  the catalogue as `_shape_gate` does today, and a batch is cut at the
  first change whose column set differs from the saved shape);
  MongoDB's change stream has `create`, `drop`, `rename`, `modify`
  (6.0+ with `showExpandedEvents`) events; SQL Server CDC keeps a
  capture instance per shape (`cdc.change_tables`, two at most).
* At that position the table's lane is **cut**: changes before it are
  applied under the old shape, then the DDL's class decides:
  * additive and safe (add a nullable column or one with a constant
    default, widen a varchar/decimal the target widens without rewrite,
    add an index): applied on the target by migkit's generated DDL
    (`ddl.py`), then the table continues - other tables never waited;
  * destructive or ambiguous (drop column, narrow a type, rename where
    the log cannot tell rename from drop+add, change the key, change a
    collation): the table is **parked** (spool, as above) with the
    generated statement and a notification; resumed when the operator's
    target matches (the same test as `_target_lacks`, per table) or
    turned into a **re-copy of that table only**, by ranges, digest
    first, while the stream goes on;
  * `TRUNCATE`, `DROP TABLE`, `RENAME TABLE`: carried positionally for
    that table (they are in the log for both engines) or parked, per
    the hop's `ddl:` rule (`apply` / `park`, default `park`).
* The whole-tail `SystemExit` in `_shape_gate` (`hetero.py:2808`)
  becomes the per-table park; the tail stops only when the spool of a
  parked table passes its bound (disk) - and then it re-copies that
  table rather than stopping.

**Exactness kept.** The exact-batch number (`twoway.exact`) stays per
stream; a parked table's spool records the stream batch at which each
change was spooled, so on resume its changes carry their original batch
numbers and `committed_ahead` still answers per table. A crash between
"spool fsynced" and "token saved" re-reads the batch and the spool
dedups by position.

**Verified, per transition** (what none of the eight products does):

* A per-table **accounting identity every batch**: `read[t] = applied[t]
  + dropped.rule[t] + dropped.scope[t] + spooled[t]` - the exact class
  of loss in failure mode 1 (rows decoded empty and dropped) breaks the
  identity and stops that table with the numbers, instead of losing
  quietly. Written to `/metrics` and the evidence file.
* Each landed range: its digest on both sides at `pos(r)` - the fenced
  re-check already used by the copier.
* Each DDL applied: shape equality of that table on both sides at the
  DDL's position, then a digest of the columns it touched once the
  table is past it.
* Each removal: the target digest at removal, re-checked later.
* Each re-add from a removal position: a full digest of the table after
  catch-up.

### 1.4 Why this is better, measurably

| Measure | Best of the eight | migkit target |
|---|---|---|
| Batches withheld from other tables while one is added | DTS child task: 0, but "no change when lag > 10 min"; DMS/Qlik/DM/Debezium: task stop | 0, at any lag |
| Batches withheld from other tables on an incompatible DDL | Informatica Stop Table: 0 (table in Error, resync) | 0; the parked table resumes from its spool, no resync |
| Source log held for a parked table | slot/checkpoint held by the whole task | 0 (spool on the mover's disk, bounded) |
| Rows copied to re-add a removed table | full reload (DMS, Qlik, DTS, Informatica) | changes since removal only, or differing ranges only |
| Seam exactness on PostgreSQL | watermark rows written to the source (Debezium) or LSN rule (exposed to the visibility race) | xid visibility, read-only, no replay |
| Loss detection | none published | per-table accounting identity per batch |
| Proof after the change | DMS `validate-only` reload; others none | range digest at `pos(r)` + shape check at DDL position + digest of touched columns |

**Feasibility** high: every piece has a precedent in migkit (ranged
copier, lever 8's rule, `_shape_gate`, `_tail_targets`, `tailctl`,
exact batches). **Licence**: nothing new. **Effort**: L overall -
M for the per-table state + add/remove + accounting identity, M for the
per-table park + spool + positional DDL, S for the PostgreSQL xid rule
(once lever 8 is merged). **Docker**: PG 16/17 and MySQL 8.4 on arm64
colima; a writer at a steady rate, tables added/removed and DDLs issued
mid-tail, asserting other tables' lag and batch counts never stall and
the digests match; a race test for the PG rule (a transaction held
between commit record and ProcArray exit is not reproducible from SQL -
use a `synchronous_commit` mix and many concurrent committers with an
injected delay, and assert the xid rule against an LSN rule that loses).

## 2. Reading changes with and without the engine's CDC feature

The owner's decision: where the feature is on, migkit uses it; where it
is off, migkit reads what the server lets a reader see, "to the depth
each proves in the sandbox, and says exactly what is missing and what
enabling it would buy". This section defines the depth scale, then per
engine what is reachable, the correctness risk, and the line `assess`
prints.

### 2.1 A depth scale migkit reports per table

| Depth | What the reader gets | Exact when | Lost |
|---|---|---|---|
| **D4 full images** | every change, before and after image, commit order, transaction bounds, position | per change | nothing |
| **D3 partial images** | every change, key + changed columns (or a byte patch), commit order | per change, *applied onto the target's prior row* | before images (two-way conflict checks, non-key row filters, change digests), whole rows for append-only targets |
| **D2 row identity** | which rows changed (key, ROWID, RID, TID) and when, in commit order; values re-read | at a fence: the row re-read at a snapshot equals the source there | intermediate states between re-reads; per-change exactness |
| **D1 object signal** | which tables (or blocks) changed in a window | at a fence, after a ranged digest diff of those tables | row timing; costs scans |
| **D0 nothing** | periodic digest diff of everything | only with writes stopped (cutover freeze) | continuous tail |

D2 and D1 are *convergent*: the target is exact at every fence, and a
cutover fence (writes frozen) makes the last cycle exact. They are not
change streams: a Kafka or audit target that needs every intermediate
state is refused below D3, with the reason.

### 2.2 SQL Server without CDC or Change Tracking

**What the server exposes.** `sys.fn_dblog` (active log) and
`sys.fn_dump_dblog` (log backups), both undocumented and unsupported.
Log records per row: `LOP_INSERT_ROWS` / `LOP_DELETE_ROWS` carry the
whole in-row record (FixedVar format) in `RowLog Contents 0`;
`LOP_MODIFY_ROW` carries only the modified byte range before/after
(`RowLog Contents 0/1`) with an offset; `LOP_MODIFY_COLUMNS` several;
a key update is a delete + insert; `LOP_BEGIN_XACT` / `LOP_COMMIT_XACT`
bound transactions; page id + slot locate the row.

**Rights and reach.** `fn_dblog`: sysadmin, or db_owner + `SELECT` on
`sys.fn_dblog` (Msg 9010 otherwise); on RDS the master user can (DMS
uses it); **Azure SQL Database exposes no log**. `fn_dump_dblog`:
sysadmin only (Msg 9011); not usable on RDS (log backups are RDS's).
While `fn_dblog` scans, log clearing is disabled (log growth); before
SQL Server 2012 SP2 every `fn_dump_dblog` call leaked a hidden scheduler
and up to three threads until restart (fixed 2012 SP2+/2014; one set is
still created once). In SIMPLE recovery the log is reused at checkpoint:
a reader that falls behind loses records with no error - only FULL /
BULK_LOGGED recovery with log backups gives retention.

**`sp_replcmds` is not a way around the feature**: it runs only in a
database published with `sp_replicationdboption` (sysadmin; Msg 18757
otherwise), admits one reader per database (Msg 18752), holds log
truncation (`log_reuse_wait_desc = REPLICATION`) and *is* the reader
CDC uses. Enabling it is enabling replication; migkit says so and does
not offer it as a path.

**Community parsers and licences.** MSSQLLogAnalyzer (C#, **GPL-3.0**,
SQL CLR, trustworthy ON, sysadmin, FULL recovery - so it changes server
settings; migkit may read its decoding logic, never install it on a
source); LogCarver (C#, **MIT**, first public release 2026-09-28, types
int/datetime2/char/varchar/nchar/nvarchar only, refuses compressed
tables, off-row LOBs and records before a DDL - too young to depend on,
useful as a format reference); OrcaMDF (C#, **GPL-3.0**, reads the data
file record format, 2008 R2 era). All three are import-compatible with
AGPL-3.0-or-later; none is a Python library.

**Depth reachable.**
* D4 for inserts and deletes of in-row data (whole record in the log,
  decoded with the table's column layout from
  `sys.system_internals_partition_columns`).
* D3 for updates as a byte patch against the physical record - useless
  to a target that is not SQL Server, so migkit lifts updates to **D2**:
  page:slot -> key through the insert/delete images already seen, or a
  keyed re-read of `%%physloc%%` rows, then the row re-read under
  SNAPSHOT isolation where it is allowed (read committed otherwise, said).
* D1 for what the decoder does not cover (off-row LOB pages, ROW/PAGE
  compression, columnstore, sparse, ADR's persistent version store
  changing record shapes on 2019+): the table is marked changed and
  ranged-diffed.

**The leapfrog: read the log on migkit's own server, not the source.**
Where the source is in FULL recovery and its log backups are reachable
(a share, S3, Azure Blob) - which most production estates already have
- migkit restores the full + log chain `WITH STANDBY` onto a SQL Server
it owns (x86 docker in CI / the `migkit` colima profile via Rosetta),
and there (sysadmin is migkit's) reads each `.trn` with
`fn_dump_dblog` and reads rows from the standby between restores. The
source sees only backup reads it already does: no sysadmin, no CLR, no
scheduler leak, no log held, nothing enabled. Granularity is the log
backup interval; the final cycle at cutover takes the tail-log backup.
This is Azure LRS's / Google DMS's backup-chain mechanism (noted in the
aws-azure report s.8, not built) turned into a **change reader for any
target**. Not on RDS or Azure SQL Database (no log backups exposed).

**Correctness risks.** Undocumented format drift across versions and
CUs (the decoder is pinned per `@@VERSION` build and refuses unknown
builds until the sandbox proves them); log reuse in SIMPLE (detected by
a gap between the last LSN read and `fn_dblog`'s first - then a range
diff, never silence); minimally logged operations (bulk insert, `SELECT
INTO` under BULK_LOGGED/SIMPLE) log page allocations, not rows -
detected by `LOP_FORMAT_PAGE`/allocation records on a table and turned
into D1 for it.

**What `assess` says**: "Change Tracking is off. Without it migkit
reads the log to depth D2 (updates re-read) for N tables and D1 for M
(reasons); two-way and change-stream targets need D4. Enabling Change
Tracking (`ALTER DATABASE ... SET CHANGE_TRACKING = ON`, per table
`ENABLE CHANGE_TRACKING`; cost: an internal table per tracked table
and a commit-table write per transaction) buys D2 with no log parsing;
enabling CDC buys D4 (capture job, change tables, log held until
harvested)."

### 2.3 Oracle without supplemental logging

**What the redo holds by default.** Since 9.2 the default is *no*
supplemental logging, and Oracle says LogMiner is then "not usable".
What is in the redo anyway: inserts and deletes with the row; updates
with ROWID plus the old and new values of the changed columns only;
chained or migrated rows, index clusters and IOTs as row pieces
LogMiner cannot merge (`UNSUPPORTED` operations, non-zero `STATUS`,
incomplete statements - the public case of a team that found its
corruption audit incomplete for exactly this reason). Minimal
supplemental logging (database-wide, cheap) fixes merging; identification
key logging adds the primary key to every update's before image.

**OpenLogReplicator does not remove this**: it parses the redo itself
(C++; README says GPL-3, the docker and tutorial repos say AGPL -
check the LICENSE at the version pinned; either is compatible with
migkit's licence when driven as a program) and also requires minimal
supplemental logging and ARCHIVELOG for complete output; without
`FORCE LOGGING`, NOLOGGING operations skip redo. A raw redo reader
reads the same bytes LogMiner does - its gains are speed and no mining
session, not depth.

**Depth reachable without supplemental logging.**
* D4 for inserts and deletes of ordinary heap rows.
* Updates: ROWID + changed columns. **Oracle's flashback query lifts
  this to D4**: `SELECT <all columns> FROM t AS OF SCN :commit_scn WHERE
  ROWID IN (...)` returns the exact after image at that commit (and `AS
  OF SCN :commit_scn - 1` the before image), batched per commit SCN.
  Exact while undo still holds the SCN (`undo_retention`, `FLASHBACK`
  object privilege or `FLASHBACK ANY TABLE`); the key comes from the
  same row. ROWID is stable for a heap row unless the table has `ROW
  MOVEMENT` enabled (partition-key updates, `SHRINK SPACE`, flashback
  table) or is moved/reorganised - detected (`dba_tables.row_movement`,
  DDL on the object, `ORA-01466` "table definition has changed") and
  turned into D1 for that table. No MySQL or PostgreSQL reader has this
  lever; it is the reason Oracle is the best of the five without its
  feature.
* Chained/migrated rows, IOTs, clusters: D2 by the ROWID (heap) or D1.
* Online redo only (NOARCHIVELOG): the reader must keep up with log
  switches; a sequence gap (`v$log_history`) means D1 for everything in
  the window.
* NOLOGGING / direct path without FORCE LOGGING: invisible to any redo
  reader. **Detected** by `v$datafile.unrecoverable_change#` /
  `unrecoverable_time` advancing, mapped to the segments in those files,
  and those tables range-diffed - the only reader in this comparison
  that notices.

**What `assess` says**: minimal supplemental logging (`ALTER DATABASE
ADD SUPPLEMENTAL LOG DATA`, online, low overhead; on RDS through
`rdsadmin.rdsadmin_util.alter_supplemental_logging`) buys merged row
pieces; PK logging buys keys without flashback reads (and no undo
dependence); `FORCE LOGGING` closes the NOLOGGING hole.

### 2.4 Db2

**LUW**: `db2ReadLog` (C API in libdb2; SYSADM or DBADM;
`db2ReadLogNoConn` reads log files without a connection). With
`DB2READLOG_FILTER_ON` only *propagatable* records return - a
transaction is propagatable only when it writes a table with `DATA
CAPTURE CHANGES` - so a table without DCC returns **nothing** filtered.
`FILTER_OFF` returns every record, but IBM documents record layouts only
for the filtered stream; without DCC an update logs a partial row image
(on z/OS, from the first changed column to the end of the row). Depth
without DCC: D2 at best (the RID in the record; `RID_BIT()` re-read of
the current row - RIDs change on REORG, detected) on undocumented
layouts, realistically **D1** (which tables had records) + ranged diff.
Python reaches it through `ctypes` over libdb2 (no driver exposes it);
x86 only (CI, Rosetta).

**z/OS**: IFI `READS` of IFCID 306 (MONITOR2 privilege, monitor trace
class 1); full images need DCC; compressed table spaces need the
expansion dictionary (KEEPDICTIONARY on REORG, PH64099/CDDS on Db2 13;
the tape deadlock APARs). Not testable in docker; W5 territory - stated
and left to a customer's system.

**What `assess` says**: `ALTER TABLE ... DATA CAPTURE CHANGES` (online;
log volume grows with the share of columns updates don't touch) buys
D4 through `db2ReadLog` filtered, or through SQL/Q Replication's CD
tables.

### 2.5 MySQL with MINIMAL row image or statement format

migkit refuses today (`mysql.py:791`: "without it an UPDATE's before
image carries only the key, so a column that was not part of the change
arrives as missing rather than unchanged"). The refusal is broader than
the risk.

* **`binlog_row_image=MINIMAL`**: before image = key columns (all
  columns for a table without a key), after image = columns the
  statement set. That is **D3**, and D3 applied onto a target that holds
  the prior row is exact per change: `UPDATE ... SET <present columns>
  WHERE <key>` (PostgreSQL, SQL Server, Oracle), `$set` (MongoDB),
  `HSET` (Redis). The manual's warning ("only guaranteed to work if
  source and destination tables have all the same columns in the same
  order ... No warning or error is raised") is about MySQL's own replica
  matching by position; migkit maps by name through
  `binlog_row_metadata=FULL` (already required) and verifies by digest,
  so it does not apply. What D3 loses and migkit says: two-way conflict
  detection (`update_origin_differs` needs the before image -
  `twoway.resolve`), non-key row filters (`_in_scope` already re-reads
  the source, so this one survives), append-only targets
  (Kafka/stream-out, warehouses) and `CREATES_ON_WRITE` targets need the
  whole row: taken **from the target's current row inside the apply
  transaction** (read-modify-write, exact because the tail applies in
  commit order) where the target can be read, refused otherwise.
* **`NOBLOB`**: the same, for unchanged BLOB/TEXT (Debezium 3.7 fills a
  placeholder; migkit leaves the target's value, which is exact).
* **`binlog_row_value_options=PARTIAL_JSON`** (HeatWave's default):
  JSON updates arrive as diffs; Debezium ignores them (Oracle's own blog:
  "missing updates"). migkit translates the diff ops (REPLACE, INSERT,
  REMOVE at a path) into the target's JSON functions (`jsonb_set`,
  `JSON_MODIFY`, `$set` with a dotted path) - D3, exact - or refuses the
  table with the reason. Must be detected; today it is silent.
* **`binlog_row_metadata=MINIMAL`** (locked so on Azure Database for
  MySQL Flexible Server): no names in `TABLE_MAP`; migkit decodes by
  ordinal against the shape **as of the event's position** (the shape
  history the positional DDL work of section 1.3 keeps) and refuses a
  table whose DDL in the window it could not place.
* **`binlog_format=STATEMENT` or `MIXED`**: statements are not
  replayable across engines (non-deterministic functions, triggers,
  `LIMIT` without `ORDER BY`). migkit treats each statement as an
  **invalidation**, never a change to replay: sqlglot parses it; a
  `WHERE` that pins the key to constants gives the keys to re-read (D2);
  anything else marks the table changed in the window (D1) for a ranged
  diff at the next fence (F1's one-scan leaf tree makes that one pass).
  Row events inside MIXED are applied as rows. The re-read reads what
  the source actually did, so non-determinism does not matter.
* **Binary log off**: D0/D1 by digest diff cycles; the ranged copier's
  resume makes each cycle copy only differing ranges.

**What `assess` says**: `binlog_row_image=FULL` is dynamic (no restart;
`SET PERSIST`) and buys before images for two-way and change streams;
`binlog_format=ROW` is dynamic too but affects only new sessions.

### 2.6 PostgreSQL without logical wal_level

`wal_level=replica` is the default and needs a restart to change (on RDS,
`rds.logical_replication` + reboot). What a replica-level WAL holds: heap
insert/update/delete records with block references and offsets, the new
tuple (an update on the same page may log only the part that differs
from the old tuple), commit/abort records with xids and timestamps,
full-page images after each checkpoint; **not** the old key of an
update or delete (that is what `logical` adds), not catalog snapshots
for decoding.

* **walminer** (formerly XLogMiner) decodes replica-level WAL into SQL by
  keeping and replaying the full-page images it sees, with a data
  dictionary built from the source; it needs `full_page_writes=on`,
  parses fully only after the first checkpoint in the window, and loses
  pre-DDL DML after rewrites. Licence: the old HighGo XLogMiner is MIT
  (logical level, PG 9.4-9.6 era); **walminer 4.x needs a paid licence
  from its author** - not a dependency; a closed tool migkit could wrap
  only with the operator's own licence, like mongosync.
* **The open path migkit builds - D2 from block references:**
  * Source of records: `pg_walinspect` (PG 15+;
    `pg_get_wal_block_info` PG 16+: `relfilenode`, `relblocknumber`,
    `xid`, `record_type`, `description` with the item offsets, one row
    per block reference; `pg_get_wal_records_info` for COMMIT/ABORT) -
    through SQL, no restart, extension on the RDS list since 15; needs
    superuser or `pg_read_server_files` (on RDS whether `rds_superuser`
    may is to be measured - one report says the grant is refused on
    Aurora). Or `pg_waldump` (PostgreSQL licence) over WAL from
    `pg_receivewal` (REPLICATION privilege, self-managed) or **from the
    WAL archive** (pgBackRest / WAL-G bucket) - which needs no database
    privilege at all.
  * Each heap INSERT / UPDATE / HOT_UPDATE / DELETE / MULTI_INSERT names
    (relfilenode, block, offset) and xid; COMMIT/ABORT records say which
    xids count (aborted ones are skipped); PRUNE / VACUUM / FREEZE /
    LOCK records are ignored.
  * relfilenode -> table by `pg_filenode_relation()` at each fence; a new
    relfilenode (VACUUM FULL, CLUSTER, TRUNCATE, a rewriting ALTER) =
    "table rewritten" -> D1 ranged diff of that table.
  * Changed TIDs are re-read at a snapshot with a TID range scan (PG 14+,
    `ctid >= '(b,0)' and ctid < '(b+1,0)'`) - no index needed; a
    **ctid -> key side map** migkit keeps from the copy and every re-read
    turns a DELETE's TID into the key to delete; a TID reused later
    shows as a new INSERT record. TOAST relfilenodes map to their
    parent.
  * Depth: D2, commit-ordered, with commit timestamps from the COMMIT
    records (no `track_commit_timestamp` needed); exact at every fence.
* **xmin polling** (Hevo, Airbyte): `xmin` newer than the last snapshot's
  xmin - no deletes ("cannot support row deletions"), full scans, 32-bit
  wraparound (Airbyte checks and tells the user to switch to CDC). With
  `track_commit_timestamp=on`, `pg_xact_commit_timestamp(xmin)` orders
  them (already read by `postgres.py:272`). migkit uses it only as the D1
  fallback where WAL is unreachable: xmin for inserts/updates, key-set
  digest diff for deletes.

**What `assess` says**: `wal_level=logical` (restart; WAL grows with
old keys and catalog info) buys D4 through migkit's slot reader; until
then D2 from WAL block references (the reader needs `pg_walinspect` or
the WAL archive), and why intermediate states are not carried.

### 2.7 MongoDB without an oplog

A standalone `mongod` has no oplog: `$changeStream` fails (code 40573).
Converting to a one-member replica set needs a restart with
`replSetName` and `rs.initiate()` (and a keyfile once auth is on) - a
configuration change migkit names and does not make. Without it:
`dbHash` still runs on a standalone, so migkit reaches **D1 per
collection** each cycle (which collections changed), then ranged `_id`
bucket digests (the raw-BSON hash of the F0 fix) localise the change to
buckets (D1 per range) and only those are re-copied; deletes appear as
keys missing from a bucket. An `updatedAt`-style field, where the
application keeps one, is used as a cursor only to narrow the buckets,
never trusted alone. WiredTiger files and journal are not parsed (no
public format; not worth it next to `rs.initiate()`).

### 2.8 Summary of section 2

| Engine, feature off | Best open depth | How | Risk migkit guards | Enabling buys |
|---|---|---|---|---|
| SQL Server, no CT/CDC | D4 ins/del, D2 upd, D1 rest | `fn_dblog` / `fn_dump_dblog` on a migkit-owned standby of the log-backup chain | format drift pinned per build; log reuse gap detected; minimally logged ops -> D1 | CT: D2 native; CDC: D4 |
| Oracle, no supp. logging | D4 heap (flashback re-read), D2/D1 chained/IOT | LogMiner + `AS OF SCN` by ROWID | ROWID moves detected; NOLOGGING detected via `unrecoverable_change#`; undo age | min supp: merged pieces; PK: no undo dependence; FORCE LOGGING |
| Db2 LUW, no DCC | D1 (D2 on undocumented layouts) | `db2ReadLog` FILTER_OFF via ctypes | REORG moves RIDs | DCC: D4 |
| MySQL MINIMAL/NOBLOB/PARTIAL_JSON | D3 (exact onto target) | partial apply by name; JSON diff ops translated | two-way and append targets refused or read-modify-write | FULL: before images |
| MySQL STATEMENT/MIXED | D2 keyed, D1 otherwise | statements as invalidations (sqlglot) | never replays a statement | ROW |
| PostgreSQL replica | D2 | WAL block refs (`pg_walinspect` / `pg_waldump` on the archive) + TID re-read + ctid->key map | rewrites detected by relfilenode; aborted xids skipped | logical: D4 |
| MongoDB standalone | D1 per range | `dbHash` + raw-BSON bucket digests | deletes by key-set | replica set: D4 |

**Feasibility / effort / docker**: MySQL D3 (S-M, arm64 docker, the
largest immediate win - lifts a refusal); PostgreSQL D2 via
`pg_walinspect` (M, PG 16/17 arm64 docker, `wal_level=replica`);
SQL Server log reader on a migkit-owned standby (L; x86 CI or Rosetta;
decoder pinned per build); Oracle flashback lift (M; Oracle Free on the
`migkit` profile, supplemental logging off, which the Free image allows);
Db2 D1 (M; x86 CI); Mongo D1 (S; arm64 docker). **Better than the paid
tools, measurably**: every paid product in the mechanism reports
*requires* the feature (DMS, DTS, Qlik: FULL row image, supplemental
logging, CDC/MS-Replication, DCC) or parses the log in-kernel on the
source (Qlik/SharePlex with sysadmin); migkit reaches a stated depth
with it off, never changes a setting, moves the privileged SQL Server
work off the source entirely, and prints per table the depth, the
reason and the exact statement that would raise it.

## 3. Closed-licence capabilities, rebuilt from open parts, better

Mechanics already written down and not repeated: mongosync's phases,
REST API, temporary destination changes and write blocking
(mongodb-tools-2026-09-27 s.1), migration-verifier's generations (s.3);
MM2's offset syncs and checkpoints, Replicator, Redpanda's header
translation (kafka-tools-2026-09-27 s.1-7); Cluster Linking and Redis
Active-Active product facts (paid-cloud-products s.2.3, s.2.15); RedisShake
readers, RIOT-X licence and modes, keyspace notifications and managed
services' blocks (redis-tools-2026-09-27 s.1-2, s.6-7); LogMiner, XStream
and OpenLogReplicator (oracle-mssql-db2 s.2.3).

### 3.1 mongosync's job from change streams + raw BSON + generations + write blocking

**What mongosync is bound by** (public): source 5.0+ (minimum patches),
destination 7.0/8.0 only; closed licence, free only with Atlas or
Enterprise Advanced; the destination is "only eventually consistent
until commit" (writes combined and reordered); unique indexes built
non-unique and converted at commit, TTL disabled, balancer stopped on
both sides (15 minutes' wait); one process per shard; source write
blocking only for 6.0+ sources and unfiltered syncs; reverse only if
dual write blocking was chosen at start.

**migkit's rebuild** (each piece exists or is on the list):
1. **Position first**: the cluster time read before the copy
   (`startAtOperationTime`), the slot-before-snapshot rule, so the
   copy and the stream meet with nothing missed.
2. **Copy in raw BSON**: `_id`-ranged reads as `RawBSONDocument`
   (`engines/mongodb.py:108`) inserted as they are - no decode, types
   carried byte for byte (Decimal128, Int64 vs Int32, dates, binary
   subtypes), under `readConcern: snapshot` at a cluster time on 5.0+
   sharded sources.
3. **Stream applied in commit order, by transaction**: change events
   grouped by `lsid` + `txnNumber` so a multi-document transaction lands
   whole; update descriptions (`$set`, removed fields, `truncatedArrays`)
   applied as such (D3, exact onto the target's prior document, no
   `updateLookup` read); lanes by `_id` (R2); `showExpandedEvents` (6.0+)
   for index and collection DDL, applied per collection through
   section 1.3's per-table park. The destination is **consistent at every
   batch boundary** - a fence at any moment proves the target equals the
   source at a cluster time - where mongosync's is consistent only after
   commit.
4. **Unique indexes**: built non-unique during the copy (transient
   duplicates are possible when a value moves between documents in
   ranges copied at different times), converted with `collMod`
   `prepareUnique` then `unique` **as soon as the copy phase ends**, with
   the 7.1+ dry run listing violators - so the conversion is off the
   cutover's critical path, which is where mongosync puts it.
5. **Generations, by range not by document**: generation N re-checks
   only the `_id` ranges the stream touched since N-1, by raw-BSON range
   digest (the F0 fix of `$toHashedIndexKey`, F1's one-scan leaf tree),
   drilling down only where a digest differs. migration-verifier re-reads
   each changed document and was tested to about 15,000 writes/s before
   its generations grow; migkit's cost grows with changed *ranges*.
6. **Write blocking at cutover**: `setUserWriteBlockMode {global: true,
   reason}` on the source (6.0+; migkit's user needs `setUserWriteBlockMode`
   and `bypassWriteBlockingMode`), final generation under the block, then
   unblock the destination. `assess` lists every user whose roles carry
   `bypassWriteBlockingMode` (the `restore` role does - the public case
   where "writes still happen" under the block) because their writes
   would pass the fence. A 5.0 source (no command): the cutover's
   reversible freeze revokes the application roles' write privileges
   and restores them on undo - allowed by the owner's cutover decision.
7. **Balancer left alone**: change streams through `mongos` do not emit
   chunk-migration writes, and snapshot reads are consistent across a
   migration, so neither side's balancer has to stop - to be proven in
   a two-shard docker cluster before it is claimed.
8. **Reverse**: the F2 reverse stream (any versions with change streams),
   not tied to a choice made at start.

**Where mongosync still wins**: one supported binary for Atlas's live
migration service; embedded verifier maintained by MongoDB; destination
UUID/shard-key handling done by the vendor. migkit wraps it where the
operator is entitled (`movers.py:2472`, `_mongosync_unfit :2430`) and the
decision engine picks by facts (versions, licence, filters, target kind).

**Measurable**: supported pairs (sources 4.0+ with `client.watch()`,
destinations of any version and DocumentDB where the stream is readable,
Community Edition) vs 5.0+ -> 7.0/8.0; the destination's fence-provable
consistency at any instant (yes/no); cutover critical path (write block ->
last generation -> unblock, no index conversion) in seconds on a 10 GB
fixture against mongosync's commit time on the same fixture (where a
licence allows the comparison); verifier cost at 15k, 30k, 60k writes/s.
**Licence** open (pymongo Apache-2.0). **Effort** M (most pieces exist:
tail, raw copy; new: transaction grouping, unique conversion timing,
range generations, write-block user audit). **Docker**: arm64 replica set
and a two-shard cluster.

### 3.2 Kafka mirroring with offsets kept, and exact group translation

**What the open parts give**: MM2 with KIP-618 exactly-once (3.5.0+,
`read_committed` on the source consumer) protects the records only;
offset syncs are emitted every `offset.lag.max` (100) records, outside the
transaction, capped at ten in flight (KAFKA-14610), translation refuses
groups behind the replication flow until 3.5's in-memory index
(KAFKA-14666), which is lost on restart (KAFKA-15905) - "turning on
exactly-once doesn't prevent data from being re-delivered". KIP-1279
(native, byte-for-byte, offset-preserving mirroring in the broker) was
proposed in February 2026 and missed 4.3 and 4.4; not shippable yet.
migkit's copier today translates a group by finding the message on the
target by time, key and value (`kafka.py:904 _translate`).

**migkit's design, three rungs chosen per partition by facts:**
1. **Offsets identical (Cluster Linking's property, from a client)**.
   A partition whose source log has no internal gaps between its log
   start and end (a delete-policy topic, no transaction markers, no
   compaction - checked by reading it: offsets contiguous) is copied into
   an empty target partition so that every record lands at its source
   offset: if the source log start `S > 0`, the empty target partition is
   first padded with `S` empty records in large compressed batches and
   trimmed with `DeleteRecords(S)` (allowed up to the high watermark;
   refused on compact-only topics), then one idempotent producer per
   partition writes the records in order with explicit partition,
   source `CreateTime` and headers. Consumer groups are then copied
   verbatim, **without the source leader epoch** (the target's epochs
   differ; committing the source epoch would make a consumer's
   epoch validation misfire), clamped to the target's end. Proof: a
   per-partition rolling digest of (offset, key, value, headers,
   timestamp) equal on both sides.
2. **Offsets translated exactly** (gaps: transactions, compaction). One
   transactional producer per partition writes each batch *and* a
   mapping record `(topic, partition, source first offset, target first
   offset, count)` to a compacted `__migkit_offsets` topic in the same
   transaction - target offsets are known from the send callbacks before
   commit - so data and map are atomic; the target is its own checkpoint
   (restart reads the last map record, as R3's target-side batch mark
   does for databases). A committed source offset `o` translates to the
   target offset of the first mapped source offset `>= o` - exact, not
   within `offset.lag.max`, for groups at any position, surviving
   restarts. No header is added to user records (Redpanda's migrator adds
   one); the map lives beside them.
3. **Cluster Linking driven** where the operator is licensed (Confluent
   destination), and KIP-1279 once it ships - both verified by the same
   digest.

**Measurable**: translation error in records (MM2: up to `offset.lag.max`
and re-delivery after restart; migkit: 0, by test over random group
positions, restarts injected); share of partitions with identical offsets
on a realistic topic set; target throughput with the transactional
producer against MM2 EOS on the same pair. **Licence**: confluent-kafka
(Apache-2.0; librdkafka BSD-2). **Effort** M. **Docker**: two KRaft
brokers (arm64 images), topics with and without transactions and
compaction.

### 3.3 Redis Active-Active semantics on OSS Redis / Valkey

**What Active-Active does** (Redis docs): strings last-write-wins by OS
clock; string/hash/sorted-set counters sum every increment; sets, hashes
and sorted sets observed-remove with **add wins** over a concurrent
delete; sorted-set equal-time score conflicts go to instance 1; HLL
delete-wins; **longer TTL wins**, and the winning replica owns the
expiry; the metadata lives inside the server, and `MSET` across slots
is refused.

**What migkit can emulate with one mover handling both directions**
(the R3 shape, `twoway.py`):
* **Concurrency detected by stream positions, not clocks.** Each side's
  replication stream (a PSYNC reader with resume, 3.5) gives every command
  its offset on that side; migkit's applies to the other side are wrapped
  `MULTI` ... `EXEC` with a mark carrying the source offset they came
  from, so the reader skips its own echo and knows, for any local command,
  which remote offsets had already been applied when it ran. Two commands
  on the same key (or same member/field) are concurrent exactly when
  neither side had applied the other's before it ran - a two-component
  version vector, no metadata stored in Redis.
* **Per type, the Active-Active rule where the stream allows it:**
  counters (`INCRBY`, `HINCRBY`, `ZINCRBY`) forwarded as deltas - sums,
  exact, 64-bit (Active-Active's counters are 59-bit per the paid report);
  set/hash/zset member add vs concurrent remove -> add wins (the remove is
  not forwarded; the remover's side gets the member back); concurrent
  field or string writes -> the hop's policy (`keep_local`,
  `apply_remote`, by site rank, which is Active-Active's "instance 1"
  rule made explicit; last-write-wins only by a clock the operator
  names); `PEXPIREAT` (the stream carries absolute expiry) -> the later
  one wins, `PERSIST` beats both; HLL `PFADD` commutes (register max).
  Every decision written to `conflicts.jsonl` with both versions - A-A
  resolves silently.
* **Refused for two-way, said before it starts**: lists (no list CRDT
  without per-element ids), streams with auto IDs, `RENAME`, cross-key
  commands (`SUNIONSTORE`, `BITOP`), Lua and `MULTI` blocks that read
  before they write, `FLUSHDB`, and `INCRBYFLOAT` (propagated as a `SET`
  of the result, so its delta is gone - a float counter is treated as a
  string). These keys must have one writer; `check` finds divergence.
* **Proof**: a convergence check after each quiet window (DUMP digest per
  key on both sides) and a counter of concurrent pairs seen, per type.

**Where Active-Active still wins**: N sites with in-server merge and no
single mover, lists and streams as CRDTs, local reads always valid.
migkit's is two sites (N with an N-component mark, later), a mover that
must be HA (W1), and a loser visible for one mover round trip on its own
side. **Measurable**: convergence after partitions injected (both sides'
digests equal), counter drift 0 under concurrent increments, add-wins
cases by test; conflicts logged vs none. **Licence**: redis-py MIT,
Valkey BSD. **Effort** M-L. **Docker**: two Valkey 8/9 and two Redis 7.2+
containers, `tc`-style pauses between them.

### 3.4 XStream's job through LogMiner

**What XStream Out gives** over LogMiner (GoldenGate licence, OCI thick
mode): structured LCRs with typed values (no SQL_REDO parsing), a
processed low-watermark acknowledged by the client, large transactions
streamed without reader-side buffering, capture on a **downstream**
database so the source does no mining.

**migkit's rebuild:**
* **Typed values without a SQL parser in the hot path**: SQL_REDO has a
  fixed shape per operation; a small tokenizer (compiled rung later, R19
  lever 9) turns it into typed values; `DBMS_LOGMNR.MINE_VALUE` as the
  fallback for a column the tokenizer does not know. Debezium carries a
  hand-written parser for the same reason.
* **Transactions buffered by migkit, spilled to disk**: uncommitted rows
  held per XID (zstd spill past a bound), emitted at COMMIT, dropped at
  ROLLBACK - never `COMMITTED_DATA_ONLY` (it buffers in the database's PGA
  and fails on `PGA_AGGREGATE_LIMIT`).
* **Low-watermark**: restart SCN = the oldest open transaction's start
  SCN, dedup by commit SCN; saved on the target with the batch mark (R3),
  so "processed" is a fact the target holds, which XStream's own
  low-watermark on the source side is not.
* **Downstream mining without GoldenGate**: LogMiner mines redo from
  another database when the mining database is on the same hardware
  platform, the same or a later release, the same or a superset character
  set, the dictionary comes from the source (`DBMS_LOGMNR_D.BUILD` with
  `STORE_IN_REDO_LOGS` - flat-file dictionaries for a PDB are desupported
  from 19c), and every RAC thread's logs are supplied. migkit runs **Oracle
  Database Free (26ai) as its own mining instance** (x86_64; 2 CPUs, 2 GB
  RAM, 12 GB user data - mining stores no user data; the Free terms allow
  internal business use, to be read at install and accepted once like
  any closed tool), fed with archived logs copied from the source's archive
  destination or RMAN backup sets. The source does no mining; it needs
  ARCHIVELOG, the supplemental logging of section 2.3, and one dictionary
  build per DDL epoch. Not on RDS (no archived-log files exposed).

**Measurable**: source CPU during capture (0 mining sessions downstream
vs one LogMiner session); largest transaction handled (bounded by the
mover's disk, not PGA); restart replay (0 batches applied twice, by the
target mark). **Licence**: python-oracledb (Apache-2.0/UPL); Oracle Free
under Oracle's free-use terms, installed with acceptance. **Effort** M
(after the wave-2 LogMiner reader lands). **Docker**: Oracle Free x86
under Rosetta on the `migkit` profile or GitHub CI - two instances, one
source, one mining.

### 3.5 RIOT-X's live replication through RedisShake and notifications

**What RIOT-X live mode is**: an initial `SCAN` + `DUMP`/`RESTORE` (or
per-type `--struct`) in parallel with keyspace notifications
(`notify-keyspace-events KEA`); its own docs say live mode "does not
guarantee data consistency" (pub/sub is fire-and-forget), and a compare
runs afterwards. BSL: production use only with Redis's own products - so
a Valkey or KeyDB target, or a Redis target reached through anything but
Redis's products, needs the open path anyway.

**migkit's rebuild, lossless by construction:**
1. **Where PSYNC is allowed** (self-managed, Tair, ElastiCache by ticket):
   RedisShake's `sync_reader` has no reconnect and always forces a full
   sync (`PSYNC ? -1`). migkit's own replica client advertises `capa
   psync2`, saves the RDB to a file loaded by RedisShake's `rdb_reader`
   (every type, module and big-key path RedisShake already handles,
   `target_redis_proto_max_bulk_len=0` for Redis <-> Valkey RDB version
   splits), then applies the command stream itself with the offset as its
   position and **resumes with `PSYNC <replid> <offset>`** after a drop
   while the backlog holds it; a missed backlog falls back to a full sync
   with the reason. Exact, positioned, markable for two-way (3.3).
2. **Where PSYNC is blocked** (MemoryDB, Azure, Tencent, ElastiCache by
   default): notifications (`KEA` plus `n` for new keys, subscribed on
   every node) give low latency, and **a lossless sweep closes the gaps
   notifications leave**: each round `SCAN`s key names and asks `OBJECT
   IDLETIME` (non-LFU policies) for keys idle less than the time since
   migkit last read them - every key read or written by anyone since -
   and re-copies those whose digest differs; migkit's own connection runs
   `CLIENT NO-TOUCH ON` (7.2+, Valkey too) so its reads do not reset the
   clock; read from a replica where the source has one, where idle time
   moves only with writes and replica reads (to be measured). Under an
   LFU policy the sweep falls back to digest rounds (redis-full-check's
   shape). Deletes: names missing from the round's scan.
3. **Cutover**: a final sweep under the freeze is exact (nothing writes).

**Measurable**: keys lost after a forced subscriber disconnect during a
write load (notifications alone: > 0; with the sweep: 0 at the next
round); round time for 1M/10M keys; source CPU of the sweep vs full DUMP
rounds (RedisShake measured DUMP raising source CPU 47% -> 91%).
**Licence**: RedisShake MIT, redis-py MIT. **Effort** M (PSYNC2 client +
command applier S-M, sweep S). **Docker**: Redis 7.2/8 and Valkey 8/9
arm64; PSYNC blocked by `rename-command PSYNC ""` to imitate a managed
service.

## 4. Imitating the cloud control plane where no API exists

Not repeated: DMS Serverless's DCU mechanics, zero-ETL's seeding and
requirements, Aurora clone / RDS snapshot + slot continuation
(aws-azure s.2, s.5, s.6); R19 lever 1's physical rungs
(`pg_basebackup` + fast-forward, CLONE, RDS/Aurora through boto3), which
the wave-2 agent is building. This section is what migkit does where the
provider offers no such API - self-managed databases, other clouds,
on-premises.

### 4.1 A mover that scales itself (DMS Serverless's job, without a service)

**What Serverless does and where it hurts**: capacity in DCUs, scale-up
after sustained utilisation, scale-down only after 60 minutes under about
45% (tasks "stick" high), a range change needs a stop; auto-segmentation
only for Oracle -> Redshift/S3.

**migkit's design**:
* **Work is ranges, not a task**: the ranged copier's units (key
  quantiles or ctid splits, each with its digest) and the verifier's
  buckets are the queue; a worker takes a range under a **lease with a
  conditional write** - S3 `If-None-Match` / `If-Match` (conditional
  writes, 2024) or a row in the target's `migkit` schema (W1's
  checkpoint) - so any number of workers on any machines share one queue
  and a dead worker's range is taken over when its lease lapses (the
  existing `lease.py` shape).
* **Workers are processes migkit starts where it is told**: local
  processes (today), Kubernetes Jobs (an Indexed Job; `.spec.parallelism`
  is mutable, so scaling is one patch), ECS/Fargate `RunTask`, or SSH
  hosts - one image, one entry point `migkit worker <hop> <queue>`.
* **The controller scales on the signals migkit already reads**: rows/s
  per worker, the source's stress (R1 pace signal - the reason to shrink
  that DMS does not see), target write latency, link throughput; grows
  while rows/s per worker holds, shrinks the moment the source's stress
  rises or the queue drains - a worker finishes its range and exits, so
  scale-down is at a range boundary, never an interruption and never a
  60-minute wait. A tail's apply side scales by lanes (R2) and, for Kafka,
  by partitions; the log reader stays one per stream.
* **Scale to zero**: the self-stopping tail (paid-cloud s.5) plus a
  scheduled catch-up - a hop can run as a cron'd Job that reads to the
  log's end, applies, saves, and exits.

**Better, measurably**: worker-seconds to move a fixture vs a fixed pool;
the source's p99 read latency held under the hop's budget while scaling
(DMS Serverless scales on its own utilisation, not the source's); time
from load drop to scale-down (seconds vs 60 minutes); no stop to change
the bounds. **Licence**: kubernetes client (Apache-2.0), boto3
(Apache-2.0). **Effort** M (the lease exists; the queue and the two
launchers are new). **Docker**: `kind` inside colima (arm64) for Jobs;
ECS through moto (API only - the run itself tested as local processes).

### 4.2 Storage-snapshot seeding with filesystem snapshots, then fast-forward

**Rule** (PostgreSQL's docs, and the same for InnoDB and WiredTiger): a
frozen snapshot of *one* volume holding data and log is a crash-consistent
image that recovers by replaying the log; data and WAL (or tablespaces)
on several volumes need simultaneous snapshots, or `pg_backup_start` /
`pg_backup_stop` around non-atomic ones. MySQL: InnoDB-only with
`innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` recovers to the
last committed transaction and its `gtid_executed`; `LOCK INSTANCE FOR
BACKUP` holds DDL off during a non-atomic copy. MongoDB: journal on the
same volume, or `fsyncLock` for several.

**migkit's rung** (for self-managed sources where R19 lever 1's cloud
APIs do not apply):
1. Position first: the logical slot (PostgreSQL), the GTID set (MySQL),
   the cluster time (MongoDB) - before the snapshot.
2. The snapshot, by what the host has: `zfs snapshot` (atomic across a
   dataset tree with `-r`), `lvcreate --snapshot` (one LV at a time -
   several LVs need the backup API), EBS `CreateSnapshots` for all of an
   instance's volumes (crash-consistent across them), or a SAN's own -
   driven by a command the operator names, never guessed.
3. **Transfer only blocks**: `zfs send` (and `zfs send -i` for a re-seed
   after a rehearsal - only changed blocks), EBS direct APIs
   (`ListChangedBlocks` between two snapshots, `GetSnapshotBlock` 512 KiB
   blocks with a SHA-256 each) to write the blocks anywhere, including
   another cloud or account without the snapshot-copy queue (20 in flight
   per region, hours), compressed with zstd on the way (R19 lever 2).
4. Start the copy on the target host: crash recovery; read its end
   position (PostgreSQL `pg_controldata` / the restored server's
   `pg_current_wal_lsn()` after recovery; MySQL `gtid_executed`; MongoDB
   the oplog's last entry); **fast-forward** the slot / start the binlog
   tail after that GTID set / the change stream at that time.
5. **Proof**: block checksums (ZFS end to end; EBS per-block SHA-256;
   PostgreSQL data checksums `pg_checksums --check` on the stopped copy,
   `innochecksum` for InnoDB), the no-gap-no-overlap position proof (the
   slot's start <= the copy's end position <= the first change applied),
   then the ordinary range digests on a sample and a full `check` before
   cutover.
6. **Hetero targets**: the snapshot seeds a *staging* instance of the
   source's engine next to the target (R17d, the writer beside the
   target), which is then copied logically at LAN speed and tailed - the
   WAN carries compressed blocks, the conversion runs local to the target.

**Better, measurably**: bytes over the WAN (compressed blocks vs rows +
protocol), zero read load on the source for the bulk, re-seed time after
a rehearsal proportional to changed blocks (`zfs send -i`,
`ListChangedBlocks`) where the clouds re-copy or re-clone. **Licence**:
OpenZFS CDDL (a program, driven), LVM GPL (a program), boto3. **Effort** M
(after lever 1). **Docker**: ZFS needs a kernel module - colima's VM can
load it (Ubuntu-based profile) or GitHub CI; LVM on loop devices inside
the colima VM; EBS direct APIs through moto are not implemented - tested
against a recorded fixture or a real account by the owner only.

### 4.3 Zero-ETL-like continuous pipelines

**What zero-ETL is** (aws-azure s.5): nothing to operate; storage seeding;
lag p50 under 15 s published; Aurora/RDS -> Redshift only, same region,
every table needs a PK, DDL resyncs the table, read-only target, and the
source must run enhanced binlog / enhanced logical replication.

**migkit's `continuous` hop** (no new mode: a hop without a cutover whose
tail never ends, with these properties turned on together):
* Seeded by the cheapest exact rung (4.2 or R19 lever 1 same-engine; the
  ranged copier otherwise); tailed; **DDL applied per table** (section
  1.3) - additive DDL without a resync, only destructive DDL re-copies
  that one table.
* Warehouse apply paths that are exact: BigQuery Storage Write API
  committed streams with offsets, Snowflake Snowpipe Streaming channels
  with offset tokens, ClickHouse `ReplacingMergeTree` with the source
  commit position as version, Redshift staged Parquet + `MERGE` (F7 and
  the aws report's gap rows).
* Tables without a key carried by a hash-of-row identity (zero-ETL
  refuses them).
* Target read-only for everyone but migkit (grants revoked,
  `default_transaction_read_only` for other roles) - checked by `doctor`,
  as zero-ETL's destination is.
* Self-healing: the tail's restart and the W1 standby; the scheduled
  verify with skip-unchanged (`unchanged.py`) so the pipeline proves
  itself on a clock; lag and the per-table accounting identity of 1.3 on
  `/metrics`.
* Works from section 2's depths: a MySQL source with `MINIMAL` rows
  (D3) needs no enhanced binlog.

**Better, measurably**: any source and warehouse, any region or cloud;
additive DDL without resync (zero-ETL resyncs every DDL); keyless tables;
verified (zero-ETL publishes no verification); lag p50 measured against
the published 15 s on the same shape of load. **Effort** M-L (mostly
assembling items already listed). **Docker**: PostgreSQL/MySQL -> ClickHouse
arm64; BigQuery/Snowflake paths only against their emulators or the
owner's accounts.

## 5. The table

Effort: S < 1 week, M 1-3 weeks, L more. Docker: **a** = arm64 colima
default profile; **r** = x86 under Rosetta on the `migkit` profile;
**ci** = GitHub `ubuntu-latest`; **n** = not in docker (owner's account or
a customer system).

| # | Item | Mechanism | Feasible | Licence | Effort | Docker | Better than the paid tools, measured by |
|---|---|---|---|---|---|---|---|
| L1 | Tables added/removed in a running tail | per-table state in `tail-tables.json`; ranged copy beside the stream; hop file compared per batch | yes | none new | M | a | 0 batches withheld from other tables at any lag (DTS: not above 10 min lag; DMS/Qlik/DM/Debezium: task stop; Vitess: impossible) |
| L2 | Exact seam, PostgreSQL | admit a change iff its xid is not visible in the range's `pg_current_snapshot()` | yes | - | S | a | no lost commit under the visibility-vs-LSN race; no watermark writes (Debezium PG writes a signal table) |
| L3 | Exact seam, MySQL / SQL Server / MongoDB | `gtid_executed` before the snapshot (+ `log_status` where granted); CT version in SNAPSHOT; `clusterTime` | yes | - | S | a / ci | duplicates replayed harmlessly or none; race test in docker |
| L4 | DDL isolated to its table | per-table park + zstd spool; resume from spool or re-copy that table only | yes | - | M | a | other tables 0 batches withheld; source log held 0 for a parked table (vs slot/checkpoint held) |
| L5 | Positional DDL, additive applied | MySQL `TABLE_MAP` full metadata + `QueryEvent`; pgoutput `Relation`; Mongo expanded events; SQL Server capture instances | yes | - | M | a / ci | add-column/widen without resync (zero-ETL, Informatica resync or stop) |
| L6 | Re-add from the removal position | removal position + target digest saved; changes since only if the log still holds it | yes | - | S | a | rows copied on re-add: changes only vs full reload |
| L7 | Per-table accounting identity | read = applied + dropped(rule) + dropped(scope) + spooled, every batch | yes | - | S | a | catches DM #12859-class silent drops; none of the 8 counts |
| L8 | Publication-add guard | refuse `ALTER PUBLICATION ADD TABLE` below the 17.5-era minors; own tail instead | yes | - | S | a | avoids PostgreSQL's own pre-17.5 loss |
| R1 | Depth scale D0-D4 per table in `assess`/`doctor` | capability probe per engine and setting, the enabling statement and its cost | yes | - | S | a | paid tools refuse or require; migkit states depth, reason, fix |
| R2 | SQL Server log read on migkit's standby | restore full + `.trn` chain `WITH STANDBY` on a migkit SQL Server; `fn_dump_dblog` there; re-read rows there | yes (FULL recovery + reachable backups; not RDS/Azure SQL DB) | GPL/MIT references (MSSQLLogAnalyzer, LogCarver, OrcaMDF) | L | ci / r | 0 privileges and 0 settings on the source; no log held; D4 ins/del, D2 upd |
| R3 | SQL Server live `fn_dblog` reader | db_owner + SELECT on `sys.fn_dblog` (RDS master user); gap detection | partly (SIMPLE recovery loses records) | same | M-L | ci / r | D2 with CT/CDC off; loss detected, never silent |
| R4 | Oracle partial updates lifted | LogMiner ROWID + `AS OF SCN` flashback re-read; ROW MOVEMENT/moves -> D1 | yes (undo-bounded) | python-oracledb | M | r / ci | D4 on heap tables with supplemental logging off |
| R5 | Oracle NOLOGGING detection | `v$datafile.unrecoverable_change#` advanced -> range diff of those segments | yes | - | S | r / ci | only reader here that notices redo-less changes |
| R6 | Db2 without DCC | `db2ReadLog` FILTER_OFF via ctypes -> D1 (D2 by `RID_BIT`) | partly | libdb2 (IBM terms, installed with acceptance) | M | ci | a stated depth instead of a refusal |
| R7 | MySQL MINIMAL / NOBLOB / PARTIAL_JSON | partial apply by column name; JSON diff ops translated; read-modify-write for whole-row targets | yes | - | S-M | a | lifts a refusal; Debezium drops PARTIAL_JSON updates, DMS requires FULL |
| R8 | MySQL STATEMENT / MIXED | statements as invalidations (sqlglot) -> keyed re-read or ranged diff | yes | sqlglot MIT | M | a | never replays a statement; exact at the fence |
| R9 | PostgreSQL at `wal_level=replica` | `pg_walinspect` block refs (PG 16+) or `pg_waldump` on the WAL archive; TID re-read; ctid->key map | yes (D2) | PostgreSQL licence | M | a | CDC with no restart and no `wal_level` change; walminer 4 is paid |
| R10 | MongoDB standalone | `dbHash` + raw-BSON `_id` bucket digests per cycle | yes (D1) | - | S | a | a stated path where every tool needs a replica set |
| C1 | mongosync's job | position-first, raw-BSON ranges, transactions whole, unique conversion after copy, range generations, `setUserWriteBlockMode` + bypass audit | yes | pymongo Apache-2.0 | M | a | 4.0+ -> any version; fence-consistent destination at any instant; cutover path without index conversion |
| C2 | Kafka offsets identical | pad empty partition to the source log start + `DeleteRecords`; one idempotent producer per partition; groups copied without leader epoch | yes for gap-free partitions | confluent-kafka Apache-2.0 | M | a | Cluster Linking's offset identity from a client; digest proof per partition |
| C3 | Kafka exact translation | transactional producer writes data + offset map in one transaction; target is its own checkpoint | yes | same | M | a | translation error 0 vs up to `offset.lag.max` + re-delivery in MM2 EOS |
| C4 | Redis two-way with A-A-like rules | concurrency by stream offsets + marks; counters by delta, add-wins members, longer TTL wins, site rank; unsafe types refused | partly (2 sites; no lists/streams) | redis-py MIT | M-L | a | OSS/Valkey, 64-bit counters, conflicts logged with both versions (A-A resolves silently) |
| C5 | XStream's job | LogMiner with reader-side spill buffer, typed tokenizer, target-held low-watermark; **downstream mining on a migkit Oracle Free** | yes (same platform, archived logs reachable) | python-oracledb; Oracle Free terms accepted at install | M | r / ci | 0 mining load on the source without a GoldenGate licence |
| C6 | RIOT-X's live replication | PSYNC2 client with resume + RedisShake `rdb_reader`; else notifications + `OBJECT IDLETIME` sweep under `CLIENT NO-TOUCH` | yes | RedisShake MIT | M | a | 0 keys lost after a subscriber drop (RIOT-X live: "does not guarantee data consistency"); Valkey targets allowed |
| P1 | Self-scaling mover | range queue with conditional-write leases (S3 or target); k8s Indexed Job / ECS RunTask / SSH workers; scale on source stress | yes | kubernetes client, boto3 Apache-2.0 | M | a (kind), moto | scale-down at a range boundary in seconds (Serverless: 60 min under 45%); source p99 held |
| P2 | Filesystem-snapshot seeding | position first; ZFS/LVM/EBS snapshot; `zfs send -i` / EBS `ListChangedBlocks` blocks; recover; fast-forward; block checksums | yes (self-managed) | OpenZFS CDDL, LVM GPL (programs) | M | a (LVM), ci (ZFS), n (EBS) | WAN bytes = compressed changed blocks; re-seed after rehearsal proportional to change |
| P3 | Zero-ETL-like continuous hop | cheapest exact seed + tail + per-table DDL + exact warehouse paths + read-only target + scheduled verify | yes | - | M-L | a (ClickHouse) | any source/warehouse/region, keyless tables, additive DDL without resync, verified |

**Order, by what it fixes first**: L7, L2, L8, R1 (small, and each closes
a way to be silently wrong or silent about a limit); R7 (lifts a refusal
in the most common engine); L1 + L3 + L4 + L5 + L6 (the live tail, one
piece); R9, C3, C2, C6 (open paths that pass a paid one outright); C1,
C5, R4/R5; then R2/R3, P1, P2, P3, C4, R6, R8, R10.

## Sources

Alibaba DTS
- https://www.alibabacloud.com/help/en/data-transmission-service/latest/add-an-object-to-a-data-synchronization-task
- https://www.alibabacloud.com/help/en/dts/user-guide/remove-an-object-from-a-data-synchronization-task
- https://www.alibabacloud.com/help/en/dts/support/common-errors-and-troubleshooting/
- https://developer.aliyun.com/article/769994 (child task and merge, 2020 console guide)

AWS DMS
- https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.ReloadTables.html
- https://docs.aws.amazon.com/dms/latest/APIReference/API_ReloadTables.html
- https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.Modifying.html
- https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.SQLServer.html

Informatica CDIR
- https://docs.informatica.com/integration-cloud/data-ingestion-and-replication/current-version/database-ingestion-and-replication/database-ingestion-and-replication/key-concepts/schema-drift-handling.html
- https://docs.informatica.com/integration-cloud/data-ingestion-and-replication/current-version/database-ingestion-and-replication/managing-database-ingestion-and-replication-jobs/redeploying-a-database-ingestion-and-replication-job.html

GoldenGate
- https://docs.oracle.com/en/database/goldengate/core/26/coredoc/using-data-pump-automatic-table-instantiation-oracle-ma.html
- https://docs.oracle.com/en/middleware/goldengate/core/19.1/gclir/set-instantiation-csn.html
- https://easyoradba.com/2024/10/23/how-to-add-new-tables-for-oracle-goldengate-replication-in-a-running-extract-and-replicat-for-ogg-microservices-19c-21c-23ai/

Qlik Replicate
- https://help.qlik.com/en-US/replicate/November2021/Content/Replicate/Main/Introduction/FullCDCProcesses.htm
- https://help.qlik.com/en-US/replicate/November2021/Content/Global_Common/Content/SharedEMReplicate/Tasks%20at%20Runtime/monitor_detailed_run_options.htm
- https://community.qlik.com/t5/Qlik-Replicate/Adding-new-tables-in-a-ongoing-Task/td-p/2078776

Debezium
- https://debezium.io/blog/2025/10/06/add-new-table-to-capture-list/
- https://debezium.io/blog/2026/08/26/debezium-3-7-beta1-released/

Vitess
- https://vitess.io/docs/archive/22.0/reference/programs/vtctldclient/vtctldclient_workflow/vtctldclient_workflow_update/
- https://vitess.io/docs/25.0/reference/vreplication/internal/life-of-a-stream/
- https://vitess.io/docs/25.0/user-guides/migration/troubleshooting/

TiDB DM
- https://docs.pingcap.com/tidb/stable/dm-faq/
- https://docs.pingcap.com/tidb/stable/dm-manage-schema/
- https://github.com/pingcap/tiflow/issues/12859
- https://github.com/pingcap/tiflow/issues/12864

PostgreSQL
- https://www.postgresql.org/docs/release/17.5/
- https://www.postgresql.org/docs/13/release-13-22.html
- https://postgrespro.com/list/thread-id/1871641 (snapshot LSN, commit order vs visibility)
- https://aws.amazon.com/blogs/database/understanding-transaction-visibility-in-postgresql-clusters-with-read-replicas/
- https://www.postgresql.org/docs/current/pgwalinspect.html
- https://www.postgresql.org/docs/current/backup-file.html
- https://postgresql.org/about/news/xlogminer-enhancements-released-and-renamed-to-walminer-1919
- https://gitee.com/movead/XLogMiner
- https://github.com/HighgoSoftware/XLogMiner
- https://docs.airbyte.com/integrations/sources/postgres/postgres-troubleshooting

SQL Server
- https://www.sqlskills.com/blogs/paul/using-fn_dblog-fn_dump_dblog-and-restoring-with-stopbeforemark-to-an-lsn/
- https://www.sqlskills.com/blogs/paul/tracking-page-splits-using-the-transaction-log/
- https://www.sqlskills.com/blogs/paul/replication-preventing-log-reuse-but-no-replication-configured/
- https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-replcmds-transact-sql
- https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/change-data-capture-and-other-sql-server-features
- https://github.com/ap0405140/MSSQLLogAnalyzer
- https://github.com/caiderek/LogCarver
- https://github.com/improvedk/OrcaMDF

Oracle
- https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html
- https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOGMNR_D.html
- https://github.com/bersler/OpenLogReplicator
- https://www.bersler.com/openlogreplicator/faq/
- https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html
- https://www.oracle.com/database/free/faq/

Db2
- https://www.ibm.com/docs/en/db2/11.1.0?topic=apis-db2-log-records
- https://www.ibm.com/support/pages/50-db2-nuggets-1-tech-tip-demystifying-db2readlog-api
- https://www.ibm.com/docs/en/db2-for-zos/13.0.0?topic=ifi-reading-complete-log-data-ifcid-0306
- https://www.ibm.com/support/pages/apar/PH64099
- https://docs.oracle.com/en/database/goldengate/core/26/coredoc/prepare-transaction-logs-oracle-goldengate-db2zos.html

MySQL
- https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html
- https://dev.mysql.com/worklog/task/?id=5092
- https://blogs.oracle.com/mysql/heatwave-mysql-solving-missing-updates-for-debezium-cdc
- https://learn.microsoft.com/en-us/answers/questions/5652182/azure-mysql-flexible-server-enable-binlog-row-meta

MongoDB
- https://www.mongodb.com/docs/manual/reference/command/setUserWriteBlockMode/
- https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/mongosync/mongosync-behavior/
- https://www.mongodb.com/community/forums/t/setuserwriteblocking-in-mongosync/286138
- https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/

Kafka
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-618:%20Exactly-Once%20Support%20for%20Source%20Connectors
- https://issues.apache.org/jira/browse/KAFKA-14610
- https://issues.apache.org/jira/browse/KAFKA-14666
- https://issues.apache.org/jira/browse/KAFKA-15905
- https://current.confluent.io/2024-sessions/mirrormaker-2s-offset-translation-isnt-exactly-once-and-thats-okay
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-1279:+Cluster+Mirroring
- https://developers.redhat.com/articles/2026/09/22/data-liberation-apache-kafka-native-cluster-mirroring
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-107:+Add+deleteRecordsBefore()+API+in+AdminClient

Redis / Valkey
- https://redis.io/docs/latest/operate/rs/databases/active-active/develop/
- https://redis.io/docs/latest/operate/rs/databases/active-active/develop/data-types/sets/
- https://redis.io/docs/latest/operate/rs/databases/active-active/develop/data-types/hashes/
- https://redis.io/docs/latest/commands/client-no-touch/
- https://redis.io/docs/latest/commands/object-idletime/

Cloud control plane
- https://docs.aws.amazon.com/ebs/latest/APIReference/API_ListChangedBlocks.html
- https://docs.aws.amazon.com/ebs/latest/userguide/readsnapshots.html
- https://kubernetes.io/docs/concepts/workloads/controllers/job/
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
