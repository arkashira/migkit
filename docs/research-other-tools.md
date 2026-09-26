# How other tools do it: mechanism, and what migkit takes from each

Backlog 26 asks about mechanism, not features: how each tool works
underneath, and whether migkit's decision layer should wrap it or learn
from it. This page covers what was read from each vendor's own
documentation (2026-09-25), with sources. A tool not yet read is listed
as not read, rather than described from memory.

## Verification

### Fivetran HVR: Compare

Mechanism:
* **Bulk compare** is the default. It takes one checksum per table over
  the raw bytes of HVR's own transport format, and says only whether
  each table matches.
* **Row-wise compare** extracts the rows, moves them to the target side,
  and compares them as typed values. It writes an INSERT, UPDATE or
  DELETE for each difference. An *online* variant runs against changing
  data.
* HVR's own notes say the two can disagree:
  * unused bytes in the transport format change the checksum
  * floats are compared with a tolerance in row-wise mode only
  * type coercion depends on the direction; an Oracle target turns an
    empty string into a null
* On live systems, only differences that persist across runs are worth
  investigating.

What migkit does the same:
* a digest per table computed inside each server, then a row drilldown
  on what differs
* repair statements for each difference

What migkit does differently:
* The digest is over one canonical text that both engines produce
  (`canon`), not over a transport format. So the fast and the slow
  answers agree by construction, and a float that cannot be rendered is
  counted as uncomparable rather than matched with a tolerance.
* A difference still in flight is proved against the change stream's
  position (a fence) where the engine allows. It is not inferred from
  repeated runs.

HVR is a commercial product: it is learned from, not wrapped.

### Striim: Validata

Mechanism, the same comparison under six execution strategies:
* **Vector**, the default: compact signatures computed inside each
  system, compared on the Validata server
* **Fast record**: the key plus a hash of the other columns per record,
  computed in each system and moved to the server
* **Full record**: whole rows moved and compared column by column
* **Key**: presence only
* **Interval**: only rows changed within a time window
* **Custom**: the user's own queries on each side

Rows are keyed by a comparison key the user chooses. A row with a null
key, or a duplicate key, is excluded from the exact comparison and
reported. Each row is classified as in-sync, content mismatch, extra at
source or extra at target. Datatype rules normalise across engines, for
example trimming CHAR and cutting a datetime down to its date where the
other side is a date.

What migkit does the same:
* the four classifications (missing, extra, changed; in-sync as `ok`)
* signatures computed inside each system
* repair scripts

What migkit takes from it:
* **Interval validation.** migkit's delta verify covers what changed by
  log position (PostgreSQL, MySQL, MongoDB). A time-window variant for
  sources with no readable log is worth adding.
* **Duplicate keys surfaced and excluded** from the exact comparison,
  rather than failing it. migkit's `duplicate keys` deep check already
  names them.

Striim is commercial: learned from, not wrapped.

## Change apply

### Qlik Replicate: batch optimized apply

Mechanism:
1. Changes are gathered in memory per source transaction, and dropped on
   rollback.
2. Repeated changes to one row are merged into one.
3. On commit, the batch goes into a *net changes* table on the target.
4. It is applied from there as bulk DELETE, then INSERT, then UPDATE
   statements, or as one MERGE per table where the target supports it.
5. Batches are sized by time and by memory.

Qlik's own limits:
* no foreign keys
* LOBs only with a size limit
* transactional integrity may be affected
* leftover net-changes tables after errors must be dropped by hand

What migkit took (backlog 29, 2026-09-25):
* one transaction per batch
* each row's changes collapsed to its net state
* runs of rows written as one statement

What migkit does differently:
* It writes nothing of its own to the target. There is no net-changes
  table to leave behind, because the statements carry the rows.
* It keeps the order in which rows were first touched, so a parent
  written before its child still is. This is why foreign keys are not
  excluded.

## Change capture

### Airbyte: CDC

Mechanism:
* Debezium runs as an embedded library inside Airbyte's source connector,
  with no Kafka.
* Each scheduled sync reads the log from the saved position up to the
  time the sync started, then stops. It is not a continuous stream.
* The position and Debezium's schema history are saved in Airbyte's own
  state message.
* The first sync is a SELECT snapshot. Airbyte's docs warn about
  duplicates when the slot is made before the table is loaded.
* PostgreSQL goes through a slot and `pgoutput`. MySQL goes through the
  binlog by file, position and GTID. Records carry `_ab_cdc_*` columns.
* Airbyte documents the failure modes:
  * a slot that does not advance on a quiet database
  * a slot invalidated by `max_slot_wal_keep_size`
  * RDS's binlog retention defaulting to zero

What migkit does the same:
* the saved position
* the snapshot taken before the load (`full+cdc`)

What migkit already covers of those failure modes:
* the quiet slot: the position moves to the log's end
  (`test_cross_engine_confirms_before_diff.py`)
* the lost slot: named, and the tail stops with what to do
  (`test_a_lost_position_stops_the_tail.py`)
* how long the source keeps its log is now a metric
  (`migkit_tail_retention_margin_*`)

Airbyte is open source, but it is a platform with its own orchestrator.
Wrapping it would put a second scheduler under migkit's, so it is
learned from, not wrapped.

## Schema

### Bytebase: drift detection and schema sync

Mechanism:
* **Drift detection** records a schema snapshot at each migration it
  runs. A background scan then compares the live schema with the last
  snapshot.
* **Schema sync** dumps both schemas, parses them into syntax trees,
  compares object by object, and orders the generated DDL by dependency.
  It creates objects in a fixed order and deletes them in the reverse
  order.

What migkit does the same:
* object-by-object comparison
* generated DDL ordered for safety (`schema --migration`)

What migkit takes from it:
* **The snapshot at each change as the baseline for drift.** migkit's
  `unchanged_since` and the verdict fingerprint say that nothing moved
  between checks. A saved schema snapshot per move would let a later
  check say what drifted since the move.

Bytebase is a platform with its own approval workflow: learned from.

## A second reader

### Trino

Mechanism: a query engine that stores nothing. It reaches each database
through a connector, as a catalog, and runs one SQL statement across
catalogs:
* a count and a `checksum()` aggregate per side
* `EXCEPT` both ways
* a full outer join on the key

Each connector converts the database's native types to Trino's own, and
the join or `EXCEPT` pulls both tables into the Trino cluster.

What that means for migkit: Trino would be a second reader across
engines, the role `second_reader` fills. The question migkit asks of any
reader still applies here, with two conversions to check. Does each
connector's rendering of a value agree with the other's? migkit's
`canon` exists because the engines' own texts disagree: `1e+20` against
`1e20`, `true` against `1`. A connector that converts a MySQL
`tinyint(1)` and a PostgreSQL `boolean` into different Trino types would
report data that is the same as different. So it is a candidate to
measure, as the second reader was, before it is wrapped.

## Not read yet

* **sqlpipe** and **schemachange**.

## Sources

* [Fivetran HVR: Compare](https://fivetran.com/docs/hvr6/getting-started/concepts/compare)
* [Fivetran HVR: row-wise compare says identical, bulk checksum says different](https://fivetran.com/docs/hvr5/faq/expert-notes/table-data-mismatch)
* [Qlik Replicate: Change Processing Tuning](https://help.qlik.com/en-US/replicate/May2026/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm)
* [Qlik Community: role of attrep_changes](https://community.qlik.com/t5/Qlik-Replicate/Role-of-attrep-changes-and-its-behaviour/td-p/2536702)
* [Striim Validata: one comparison logic, many execution strategies](https://www.striim.com/blog/validata-in-action/)
* [Striim Validata: datatype comparison rules](https://www.striim.com/docs/validata/en/datatype-comparison-rules.html)
* [Airbyte: Change Data Capture](https://docs.airbyte.com/platform/understanding-airbyte/cdc)
* [Airbyte: Postgres source](https://docs.airbyte.com/integrations/sources/postgres)
* [Bytebase: schema drift detection](https://www.bytebase.com/docs/change-database/drift-detection/)
* [Bytebase: how schema sync works](https://www.bytebase.com/blog/how-schema-sync-work/)
* [Trino concepts](https://trino.io/docs/current/overview/concepts.html)
* [Trino aggregate functions (checksum)](https://trino.io/docs/current/functions/aggregate.html)
