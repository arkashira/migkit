# The problems a migration puts on one person, and which ones migkit takes off them

Every entry below is a failure that people report from real migrations, with
what it costs and what migkit does about it. The verdict on each is one of:

- **Ends it** - migkit handles it, and a named live test proves it
- **Partly** - migkit handles some of it; what is missing is stated
- **Not yet** - migkit does not do this, and it is in the plan

Nothing here is marked "ends it" on the strength of an intention. If it says
ends it, there is a test file you can run against real servers.

The companion document, [what a migration actually
costs](what-a-migration-actually-costs.md), holds the ordered plan. This one
holds the catalogue.

---

## A. Before anything moves

### A1. The scope was written as "the tables"

**What happens.** Teams scope the tables and discover in cutover week that
permissions, sequences, triggers, views, retention rules and downstream jobs
were never in the plan. Practitioners describe a migration as preserving
relationships, history, permissions and operational continuity - not rows.

**migkit: Partly.** `migkit check --deep` compares far more than tables:
grants and sequence grants, ownership, foreign keys and orphans, disabled
triggers, materialized views, partitions, row-level security, deferrable
constraints, NOT VALID constraints, extensions, generated columns, and
tables with no primary key. `migkit assess` reports what has to be done by
hand. Tests: `test_deep_*_pg.py` (12 files), `test_handwork_pg.py`,
`test_handwork_mysql.py`, `test_handwork_mongo.py`, `test_ownership_live_*`.

**Missing:** anything outside the database - cron jobs, application config,
downstream consumers. migkit does not pretend to see those.

### A2. The privileges on the two sides are not the same

**What happens.** The source has superuser; the managed target does not.
A dump carrying `ALTER ROLE ... WITH SUPERUSER` fails on restore. `GRANT ALL
PRIVILEGES ON DATABASE` grants connect and schema creation, not object
creation, so people believe they have granted more than they have. Logical
replication needs `rds_replication` rather than the `REPLICATION` attribute,
and the parameter change needs a reboot.

**migkit: Partly.** Grants and sequence grants are compared and repaired
(`test_grant_repair_pg.py`, `test_grant_repair_mysql.py`), ownership is
compared (`test_ownership_live_*`), `migkit users` carries logins keeping
the same password, and `assess` checks the CDC prerequisites before anybody
starts.

**Missing:** one pre-flight answer to "what will this target refuse from
this source". The pieces exist; saying them together, early, does not.

### A3. Extensions and plugins - PostGIS is the worst case

**What happens.** `pg_upgrade` restores a geography column before
`spatial_ref_sys` has rows and fails with `Cannot find SRID (4283) in
spatial_ref_sys` - reproduced across PostgreSQL 11, 13 and 16 with PostGIS
3.2 and 3.4, and only if you use a non-4326 SRID. Custom `spatial_ref_sys`
rows are not preserved by a normal dump, and restored rows do not override
existing ones. Both GCP and Azure state plainly that extensions their
managed target does not support are simply not migrated - the job succeeds
and the extension is gone.

**migkit: Partly.** The extension list is compared
(`test_deep_extensions_pg.py`).

**Missing:** extension *versions*, and extension-owned data such as custom
`spatial_ref_sys` rows. A PostGIS database can pass migkit's current
extension check and still be wrong. This is the clearest gap the research
turned up and it goes near the front of the plan.

### A4. The fork is not the thing it claims to be

**What happens.** MariaDB and MySQL have diverged: sequences and
system-versioned tables exist on one side only, JSON is a native binary type
on one and an alias for LONGTEXT on the other, the GTID formats do not
interoperate at all, and the UUID type is a first-class type on one side and
a `BIN_TO_UUID` convention on the other. Redis-compatible servers report a
`redis_version` that is not theirs.

**migkit: Partly.** `migkit/variants.py` identifies the brand behind the
protocol and records what that brand cannot do - measured, not assumed. It
already carries MariaDB's two migration-relevant divergences: there is no
`gtid_mode` variable at all (the query returns zero rows and
`@@gtid_executed` fails with `ERROR 1193`), and the `CHANGE REPLICATION
SOURCE` statement migkit generates uses MySQL 8 syntax. On the Redis side it
was measured that Valkey 8.1 answers `redis_version:7.2.4`, Dragonfly
answers 7.4.0 and KeyDB answers 6.3.4. Tests: `test_variants.py`,
`test_variants_live.py`, `test_variants_mongo_kafka.py`.

**Missing:** the object-level consequences - a MariaDB sequence or a
system-versioned table has no home in MySQL, and migkit does not yet name
them before the move.

---

## B. Semantics that differ between engines

### B1. A type mapping that quietly changes the value

**What happens.** Oracle's `DATE` carries a time; mapping it to PostgreSQL
`DATE` drops it - the rule practitioners repeat is *never* map Oracle DATE to
PostgreSQL DATE. Oracle treats the empty string as NULL and PostgreSQL does
not, which breaks `IS NULL` predicates, concatenation and unique
constraints after cutover. `NUMBER` without precision mapped blanket to
`numeric` changes join performance and introduces implicit casts.

**migkit: Partly.** `migkit/canon.py` maps each engine's declared types onto
a small set of canonical classes and **refuses to compare a type it has no
canonical rendering for**, rather than guessing - which is how a whole class
of false equality is avoided. Narrowing, float precision, NULL-vs-empty and
timezone shifts each have their own deep check (`test_deep_narrowing_pg.py`,
`test_deep_float_pg.py`, `test_deep_nullempty_pg.py`,
`test_deep_timeshift_pg.py`).

**Missing:** Oracle itself. The type map has no `oracle` entry yet; the
engine work is measured as possible and queued.

### B2. Collation changed under the index

**What happens.** PostgreSQL delegates sorting to glibc or ICU. glibc 2.28
changed many locales, so an index built before an OS upgrade is no longer
sorted by the current rules: queries miss rows that are physically present,
and **unique indexes stop detecting duplicates**. `ALTER COLLATION ...
REFRESH VERSION` silences the warning without fixing anything, and a clean
`amcheck` run is not proof of safety.

**migkit: Partly.** Collation is compared between the two sides
(`test_deep_collation_pg.py`).

**Missing:** the *version* comparison - `pg_collation.collversion` against
the library's current version, and the duplicate hunt that has to run with
`enable_indexscan = off` because the index itself is the thing lying. Cheap
to add, and it catches silent data loss.

### B3. Text that was already broken before you moved it

**What happens.** An application sending UTF-8 through a latin1 connection
stores the bytes as latin1 characters. Everything looks right until a
conversion or a client change, then `é` becomes `Ã©`. The repair path is a
`BLOB` round trip - which corrupts the rows that were *not* double-encoded,
so a column with mixed history breaks under a uniform fix.

**migkit: Partly.** Charset and encoding parity are checked
(`test_charset_encoding_mysql.py`, `test_deep_encoding_pg.py`), and every
comparison runs over canonical text, so a difference in stored bytes shows
as a difference rather than as noise.

**Missing:** a mojibake detector - a check that samples text columns for the
`Ã©`/`â€"` signatures and for genuine high-byte latin1, and reports that the
column has *both* before anybody runs a conversion.

---

## C. The move itself

### C1. It is slower than it needs to be

**What happens.** The speed of the good tools is not a clever protocol. It
is: one consistent snapshot shared by every worker (`pg_export_snapshot()`
plus `SET TRANSACTION SNAPSHOT`), tables ordered largest-first, big tables
split into non-overlapping ranges, indexes built after the data and in
parallel, the primary key created `USING` the already-built index to avoid
an exclusive lock, and not paying for LOB handling on tables with no LOBs.
DMS exposes the same levers: `MaxFullLoadSubTasks` (8 by default, 49 max),
parallel load by partition or by hand-tuned ranges, `CreatePkAfterFullLoad`,
commit rate. Both document the same trap: automatic partition splitting is
often *not* fastest, because skewed partitions leave one long-tail subtask.

**migkit: Partly.** migkit calls the native mover for the pairs that have
one (pgcopydb, mydumper, COPY binary - measured 1.53x for binary format),
so it inherits their speed rather than competing with it. Measured end to
end on this hardware ([scale.md](scale.md)): 10,000,000 rows / 2,777 MB
moved in 57.9 s and verified in 62.4 s, with migkit's own peak memory at
30 MB and 333 MB - flat against the 1M run, because the digest is computed
inside each server.

**Missing:** the pairs with no native mover, LOBs in the benchmark table,
and a comparison against pgcopydb on the same hardware. Until that
comparison exists migkit makes no claim about being faster than anything.

### C2. LOBs

**What happens.** Full LOB mode moves them one at a time and dominates the
whole task. Limited LOB mode is fast and **truncates past `LobMaxSize` with
a warning in a log nobody reads** - data loss a row count will never find.
`bytea` stops at 1 GB and gets difficult around 500 MB. `pg_largeobject` is
one table, so `pg_dump`/`pg_restore` are single-threaded for every large
object in the database - one team's 12.5 hour downtime was almost entirely
this.

**migkit: Not yet.** No LOB inventory, no size histogram, no truncation
detection. Given that the failure is silent and the detection is cheap
(`max(pg_column_size(col))` per candidate column, compared against whatever
limit the mover was configured with), this is high value per line.

### C3. It died at 80% and nobody knows what is safe to keep

**What happens.** pgcopydb is honest that `--resume` requires
`--not-consistent`, because the exported snapshot is gone after a crash.
Google's DMS splits errors into recoverable and unrecoverable and says the
unrecoverable ones mean starting the job again from the beginning. Schema
tools leave the database "neither the previous nor the next version".

**migkit: Partly.** Chunked, restartable verification with a proof file
exists (`test_resume_chunked_pg.py`, `test_resume_chunked_mysql.py`), and a
second verify of an unchanged table deliberately reads no rows and says so
rather than pretending to have re-read them.

**Missing:** resume that survives the machine, and a statement-level record
of what a repair had applied when it stopped.

### C4. The verification kills the source

**What happens.** pt-table-checksum exists because this is real: it sizes
chunks dynamically against a target execution time, pauses on replica lag,
pauses on `--max-load`, and skips chunks an `EXPLAIN` says are too big.

**migkit: Partly.** A throttle reads each engine's own saturation signal
and narrows the work in flight - 7 of 9 engines, including the cross-engine
path and reladiff's connection count. Tests: `test_throttle_*.py`.

**Missing:** two engines, and a lag-aware brake for replicas specifically.

---

## D. Proving the data actually landed

This is the part migkit is built around, and the part where the managed
services stop.

### D1. Row counts agree and the data does not

**What happens.** Counts are the check everybody runs and the check that
proves least. Sampling does not save you either: to have a 95% chance of
catching a defect affecting one row in ten thousand you need about thirty
thousand sampled rows, and rare defects concentrate in the most important
accounts.

**migkit: Ends it.** Both sides fold every row into one number using the
same canonical rendering, computed **inside each server**, so the size of
the table does not put data on the wire. Counts ride along with the checksum
query rather than being a second question asked at a different moment.
Tests: `test_every_check_runs.py` (7 live engines), `test_diff_kind_pg.py`,
`test_diff_kind_mysql.py`, `test_hash_collisions_*.py`.

### D2. The checksum itself was the lie

**What happens.** Joining column values with a separator is ambiguous the
moment a value contains the separator. Measured on MySQL 8, `('x#y','z')`
and `('x','y#z')` produce the identical CRC32 `3898531935` - a verifier
certifying two different databases as equal.

**migkit: Ends it.** `migkit/rowtext.py` writes each value's length in front
of it, so the encoding is injective by construction: distinct tuples cannot
collide, and NULL is marked by a length that is not a number so no literal
can impersonate it. Tests: `test_hash_collisions_pg.py`,
`test_hash_collisions_mysql.py`, `test_keys_with_teeth.py`.

### D3. Two readings that both failed, compared equal

**What happens.** A side that could not be read returns the same sentinel on
both sides, the comparison matches, and the report says the servers agree.
migkit had seven of these and they were found by running the code, not by
reading it: a MongoDB refusing `getParameter` without credentials reported
`ok | 1 settings, all equal both sides`; a sqlite table walked with
`WITHOUT ROWID` reported `rows -1==-1`; a Kafka partition nobody could read
reported agreement on both sides.

**migkit: Ends it.** An unreadable side is an `error`, never agreement, and
the message names which side and why. Test: `test_unreadable_agreement.py`,
plus `test_one_sided_checks.py` for the related family where a check only
looked at one side.

### D4. Keys that carry the characters the report is made of

**What happens.** Keys containing a backslash, a tab or a newline break the
files a repair reads. Measured in this repository: PostgreSQL wrote raw keys
and read them back with `\copy`, so `back\slash` was parsed as `backslash`,
the join matched no row, and **`sync --apply` reported a repair it had not
performed**. MySQL's repair read `rowtext`-encoded keys with `split("\t")`
and therefore did nothing at all, for every key, ordinary ones included.
Redis keys are binary-safe and a key with a newline was written as one line
and read as two.

**migkit: Ends it.** All three are fixed, each with the measurement in the
test file. Tests: `test_pg_exotic_keys.py`, `test_keys_with_teeth.py`.

### D5. The validator quietly refuses the table

**What happens.** DMS validation needs a primary or unique key, will not
take a CLOB/BLOB key or a VARCHAR key over 1024, cannot handle NULL in a
key, stops the entire task after 10,000 failures, cannot validate rows that
keep changing, skips views, will not span databases, and skips a whole table
when one column is masked. reladiff samples the values of a key column and
refuses the table outright if it reads them as free text - measured, one
row whose key held a tab flipped a `varchar(60)` key from usable to
`Cannot use a column of type Text() as a key`.

**migkit: Ends it, for its own engines.** None of the DMS limits apply.
Where migkit borrows a tool that has a limit, the limit is reported as an
`error` with the tool's own words, never as a table that matched. Test:
`test_keys_with_teeth.py`.

### D6. "Three rows differ" and nothing you can do with it

**What happens.** A validator that reports a count leaves the operator to
find and fix the rows by hand.

**migkit: Ends it.** Every engine names the differing rows in a drilldown
file, `migkit sync --kind rows` plans the repair from what the check showed
rather than from the current moment, and `--apply` writes the undo before it
touches anything, writes before it deletes, and refuses when a column has no
canonical rendering rather than writing a row with that column missing.
Tests: `test_sqlite_repair.py`, `test_generic_repair.py`,
`test_hetero_repair.py`, `test_redis_repair.py`, `test_restore_mysql.py`.

### D7. Referential integrity and business rules

**What happens.** Foreign keys that the source enforced and the target does
not leave orphans that are technically present and semantically broken.
Aggregates can reconcile while the detail is wrong.

**migkit: Partly.** Foreign key orphans, NOT VALID constraints and
deferrable constraints are checked (`test_deep_nopk_pg.py`,
`test_deep_notvalid_check_pg.py`, `test_deep_deferrable_pg.py`).

**Missing:** user-supplied business assertions ("invoice_total = sum(line
items)") run on both sides and compared.

---

## E. Keeping the two sides in step

### E1. Sequences do not replicate

**What happens.** Logical replication does not carry sequences, and DMS does
not migrate `NEXTVAL` state during ongoing replication. The target's counter
sits below the source and the first insert after cutover collides with a
row that is already there.

**migkit: Ends it.** Sequence parity is a check of its own, there is a
separate check for the collision itself (a counter below the maximum key in
the table), and `sync --kind sequences` repairs it with the previous values
saved first. Tests: `test_diff_kind_pg.py`, `test_revert_live_pg.py`.

### E2. Kafka offsets after a cutover

**What happens.** MirrorMaker 2's offset translation is explicitly not
exactly-once; a consumer can resume before or after where it should.
Negative translated offsets were a real data-loss bug.

**migkit: Partly.** Consumer group offsets are compared between clusters and
can be repaired through `alter_group_offsets`. Tests:
`test_kafka_offsets.py`, `test_kafka_unreadable.py`.

### E3. Redis TTLs and cross-version payloads

**What happens.** `DUMP`/`RESTORE` does not carry the TTL unless you read
`PTTL` and pass it, so every key lands immortal. The payload embeds an RDB
version, so a newer source into an older target is rejected.

**migkit: Ends it.** The repair carries the expiry with the value (measured:
600 seconds arrived as 599992 ms) and refuses the cross-version restore with
the server's own error plus a runnable alternative, rather than falling back
to re-issuing values with type-specific commands, which would quietly change
what some types hold. Test: `test_redis_repair.py`.

### E4. MongoDB change streams are not the oplog

**What happens.** DocumentDB has no oplog, change streams are off by
default, retention defaults to 3 hours (7 days maximum), DDL events are not
delivered, and on 3.6/4.0 the stream can only be opened against the primary.
A standalone MongoDB has no change stream at all.

**migkit: Partly.** A standalone server is reported with the server's own
message and a remedy instead of a traceback (`test_mongo_delta_guard.py`,
`test_cdc_mongo_source.py`).

**Missing:** the DocumentDB pre-flight - retention window, whether streams
are enabled on the collections in scope, and the missing DDL events.

### E5. A replication slot nobody reads fills the disk

**What happens.** An abandoned logical slot pins WAL until the source runs
out of storage - a source outage caused by the migration tooling.

**migkit: Ends it.** `assess` flags an inactive slot as source-side
operational health rather than counting slots. Tests:
`test_assess_slot_health_pg.py`, `test_pgslot_live.py`.

---

## F. Cutover and rollback

### F1. Dual writes drift

**What happens.** Application-level dual writes are not atomic. One team's
write to the new database was lost "for a specific millisecond, on a
specific record", and the fix was a three-day background scan. Cron jobs,
event processors and third-party integrations are write paths people forget
they have.

**migkit: Partly.** `migkit check` is the scan, and it is cheap enough to
run repeatedly during the parallel-run window; delta verify re-checks only
what changed since the last verified position, advancing only on a clean
run. Tests: `test_delta_pg.py`, `test_live_stream.py`.

### F2. The rollback nobody rehearsed

**What happens.** Rollback is assumed rather than tested. Teams that succeed
rehearse it - one practised three times before the real cutover.

**migkit: Ends it for the data it changed.** Every repair writes its undo
first; `migkit rollback` restores from any saved state; `migkit history` is
the audit trail of every write migkit has made. Tests:
`test_revert_live_pg.py`, `test_revert_live_mysql.py`,
`test_restore_mysql.py` (which proves the restore is byte-exact).

**Missing:** rollback of things migkit did not do.

### F3. The evidence an auditor will ask for

**What happens.** Somebody will eventually ask you to prove the migration
was complete, and that artefact is worth far more produced at cutover than
reconstructed a year later.

**migkit: Ends it.** Every check writes its evidence next to the report -
the objects dumped from both sides, the parameters, the differing columns,
the differing rows, and a proof file recording what was proved equal and
when. `migkit report` renders it, `migkit history` lists it.

---

## G. After the data has landed

### G1. The target is correct and slow

**What happens.** Autoanalyze is threshold-driven, so a freshly loaded table
keeps pre-load statistics until enough modifications accumulate - on a
10-million-row table that is a million modifications. `pg_upgrade` carries
no statistics at all. The planner then chooses a sequential scan over an
index that exists, and it gets reported as an engine regression.

**migkit: Ends it on PostgreSQL, states the gap on MySQL.**
`migkit check --deep` reports the tables the target's planner has no
statistics for, and warns when a table has been rewritten by more than
autovacuum's own scale factor since its statistics were taken. `migkit move
--go` analyzes what it loaded before it hands the target back - the same
thing pgcopydb does per table, and the thing that matters for a table loaded
with `autovacuum_enabled = false`, which autoanalyze will never catch up.
Test: `test_planner_statistics.py`.

On MySQL the check reports `skip` with the measurement behind it rather than
a verdict: `innodb_table_stats.n_rows` read 19 for a table holding 50,000
rows while the load settled, and `information_schema` `update_time` did not
move when the table was written - either would produce a false all-clear.
`migkit move` still runs `ANALYZE TABLE` on what it loaded there.

### G2. Who could see the data while it was moving

**What happens.** Migrations widen the attack surface: new sync accounts,
temporary grants, copies landing in dev and QA, and PII in CI logs that a
whole team can read.

**migkit: Partly by construction.** migkit runs from the operator's machine
over ordinary client connections; the row data it reads for a drilldown
stays local, and the evidence files are written locally. Grants are compared
so temporary ones show up as a difference.

**Missing:** masking. If an operator needs a drilldown that is safe to paste
into a ticket, migkit does not yet offer one.

---

## H. What migkit does not end, stated plainly

- Stored procedures, packages and triggers are reported, never converted.
- Application-side anything: connection strings, ORM behaviour, cron jobs.
- Throughput claims. No benchmark against another tool on the same hardware
  exists yet, so none is made.
- Engines outside the current nine, notably Oracle (measured as buildable
  here), Db2 (no arm64 image, so it would be emulation), S3/Redshift,
  DynamoDB, OpenSearch, Kinesis, Neptune.
- A control plane: HA, alerting, and resume across machines.

## Sources

Practitioner failure modes:
[operational chaos guide](https://dev.to/kate_steeleeee/a-practical-guide-to-database-migration-without-operational-chaos-563m),
[1 billion records without downtime](https://medium.com/@himanshusingour7/how-we-migrated-db-1-to-db-2-1-billion-records-without-downtime-c034ce85d889),
[field-tested checklist](https://www.disqr.com/services/data-migration-services/database-migration-best-practices/).
Validation and sampling:
[how to validate data after a migration](https://settledata.ai/blog/how-to-validate-data-after-migration),
[data verification methods and checklist](https://www.bladepipe.com/blog/data_insights/data_verification/),
[pt-table-checksum documentation](https://docs.percona.com/percona-toolkit/pt-table-checksum.html).
Speed:
[pgcopydb clone](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html),
[pgcopydb snapshots and resume](https://pgcopydb.readthedocs.io/en/latest/resume.html),
[DMS LOB support](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.LOBSupport.html),
[DMS parallel load](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Tablesettings.html).
Managed-service limits:
[GCP DMS known limitations](https://cloud.google.com/database-migration/docs/mysql/known-limitations),
[Azure DMS known issues](https://learn.microsoft.com/en-us/azure/dms/known-issues-troubleshooting-dms),
[rds_superuser](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Roles.rds_superuser.html),
[RDS logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html).
Engine semantics:
[Oracle to Postgres conversion](https://wiki.postgresql.org/wiki/Oracle_to_Postgres_Conversion),
[handling empty strings](https://aws.amazon.com/blogs/database/handle-empty-strings-when-migrating-from-oracle-to-postgresql),
[MariaDB/MySQL incompatibilities](https://mariadb.com/docs/server/server-management/install-and-upgrade-mariadb/migrating-to-mariadb/moving-from-mysql/mysql-to-mariadb-compatibility-matrix),
[PostGIS spatial_ref_sys restore bug](https://trac.osgeo.org/postgis/ticket/5899),
[glibc collations and data corruption](https://www.crunchydata.com/blog/glibc-collations-and-data-corruption),
[collation version mismatch on RDS](https://aws.amazon.com/blogs/database/manage-collation-changes-in-postgresql-on-amazon-aurora-and-amazon-rds/).
Streams and stores:
[MirrorMaker 2 offset translation](https://lenses.io/blog/2025/10/kafka-replication-mirrormaker2-complexity/),
[DocumentDB change streams](https://docs.aws.amazon.com/documentdb/latest/devguide/change_streams.html),
[RedisShake modes](https://tair-opensource.github.io/RedisShake/en/guide/mode.html).
Cutover, aftermath and compliance:
[zero-downtime patterns](https://launchdarkly.com/blog/3-best-practices-for-zero-downtime-database-migrations/),
[post-upgrade statistics](https://techcommunity.microsoft.com/blog/azuredbsupport/azure-postgresql-lesson-learned-8-post-upgrade-performance-surprises-the-one-ste/4471807),
[migration compliance checklist](https://syncopio.com/blog/data-migration-compliance-checklist/).
