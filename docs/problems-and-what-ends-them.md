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

### A5. The mover flattens the partitioning

**What happens.** AWS DMS states plainly that it does not migrate table
metadata related to partitioning or inheritance: it reports both parent and
child tables on the source and creates a **plain table** on the target. The
partitioned target has to be built by hand first, with the task set to "do
nothing" or "truncate" rather than "drop tables on target". Mapping rules
differ between phases too - partitions are the source tables for CDC, and
naming the parent there produces duplicate errors.

**migkit: Ends it.** The deep check compares partitioning between the two
sides. Measured on a range-partitioned source against a plain target of the
same name: `deep postgres partitions: DIFF public.events: not partitioned on
target`. Test: `test_deep_partitions_pg.py`.

**Missing:** it names the table, not the partitions that will be missing at
the next boundary - `pg_partman`'s own traps (the default partition that has
to be drained before a child can be created, `async_partitioning_in_progress`
silently stopping maintenance, a unique key that must include the partition
key) are not modelled.

### A7. The order the target is built in

**What happens.** The schema is restored in one piece, so every secondary
index is on the table before a single row arrives, and the load maintains
them one row at a time. It is the default shape of every "restore the
schema, then load the data" runbook.

Measured on 1,000,000 rows with three secondary indexes, on the sandbox's
two shared cores:

| | wall | on disk |
|---|---|---|
| indexes already present, then load | 4.48 s | **275 MB** |
| load, then build the same indexes | 2.53 s (0.91 + 1.62) | **218 MB** |

Not quite twice the time - and the part that does not go away: the table
loaded with its indexes in place is **26% larger**, because an index
maintained one row at a time does not pack the way one built in a single
pass does. Nothing later reclaims that.

**migkit: Ends it** - `test_setup_target_plan_pg.py`. `migkit schema
--setup` prints the plan an operator runs by hand, and that plan now splits
the restore at exactly the line that matters: `--section=pre-data` (tables,
no indexes), then the load, then `--section=post-data` (indexes, foreign
keys and triggers, built once over the loaded data). Measured: pre-data
leaves the table with **0** indexes and post-data brings them back, and a
test runs both halves against live servers rather than only reading the
printed words.

It also replaces the old advice to disable foreign keys and triggers by
hand before the load - with post-data held back, they are not there to
disable.

### A6. The bill for moving the bytes

**What happens.** Egress is charged when data leaves a provider and ingress
is free, which is the shape that makes leaving expensive. At $0.09/GB for
the first 10 TB out of AWS, a 10 TB database with two weeks of continuous
replication runs $1,200-$1,500 in transfer alone, and 50 TB costs
$3,500-$7,000. It surprises people because it is spread across line items
nobody reads as "database replication", and because dual-running pays both
providers at once.

**migkit: Not yet.** migkit knows exactly how many bytes each table holds -
it reports them in `check counts` - so the arithmetic an operator is doing
on a napkin is arithmetic migkit already has the inputs for. It does not do
it, and it does not warn that a `move` is about to push a measured number of
gigabytes across a boundary that charges for them.

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

**migkit: Ends it** for detection - `test_deep_collation_pg.py` (the two
sides' collations) and `test_collation_versions_pg.py` (the versions behind
them). `check --deep` now reads, on **both** sides, every collation a user
column or index actually references, plus each database's own default, and
compares the version recorded against the one the operating system provides
now:

    deep postgres collation versions: DIFF 1 collations changed under their
      indexes: target en_US.utf8 built under 2.17, now 2.41 - a text index
      sorted by the old rules can walk past the row it wanted, and a unique
      index can stop catching duplicates

Reproduced by faking the recorded version in the catalog, which produces the
genuine article: PostgreSQL then prints its own `collation version mismatch`
warning on every connection, and the test asserts the server says so too
rather than trusting migkit's opinion of its own output.

Two measurements shaped the check rather than decorating it:

* **The fix everyone copies does not fix anything.** `ALTER DATABASE ...
  REFRESH COLLATION VERSION` - which is what the server's own HINT tells you
  to run - was measured silencing the warning **instantly**, rebuilding
  nothing. So migkit's hint puts `REINDEX` first and says why, and a test
  asserts that ordering in the text.
* **After a real glibc upgrade everything drifts at once** - all 873
  collations this image ships. Reporting them would be a wall rather than a
  finding, so only collations something is actually sorted by are reported;
  dropping the table that used one was measured to remove it from the
  result, and a test pins both directions.

A locale the OS no longer provides at all gets its own, harsher verdict: the
rules are not merely different, they are gone, and no rebuild can help until
the locale is installed.

MySQL answers the same command with a **skip that explains itself**: measured
on 8.4, all **286** rows of `information_schema.collations` report
`IS_COMPILED = Yes` and the table has no version column at all, so no OS
upgrade can re-sort an index underneath it. The MySQL-shaped version of this
risk is [B4](#b4-the-server-changed-its-default-collation-under-you).

**The duplicate hunt that follows** is now part of the same report -
`test_duplicate_keys_pg.py`. Once something has said an index cannot be
trusted - a collation that moved, or an index that answers no query -
`check --deep` groups by every unique text key with the index paths shut
off and names the rows the constraint has been letting through:

    deep postgres duplicate keys: DIFF 1 unique indexes have duplicate rows
      underneath them: source public.people.people_email (email) 1
      duplicated values, e.g. {"email":"a@x.com","migkit_n":2} - the
      constraint is still there and stopped being enforced

**Why the planner is not allowed to answer.** On a 200,001-row table holding
one duplicate its unique index never recorded, the planner chose `Index Only
Scan using big_email` for `group by ... having count(*) > 1` by itself and
reported **0** duplicates. With `enable_indexscan`, `enable_bitmapscan` and
`enable_indexonlyscan` off, the same query on the same data reported **1**.
A hunt that trusts the planner here is a false negative wearing an
all-clear, and a test pins both numbers.

It runs **only** when something else has complained, because otherwise it is
a sequential scan of every table to prove a negative - and with no reason it
returns without opening a connection at all. Partial and expression indexes
are counted and left alone rather than guessed at, since grouping by their
columns asks a different question than the index answers.

**MySQL states the gap instead of hunting on a guess.** The two states that
break a PostgreSQL unique index were both measured absent. The MySQL-shaped
candidate is `unique_checks = 0`, which mysqldump writes into every dump and
which InnoDB is documented as being allowed to honour by skipping the check:
tried on 8.4, the duplicate was **still** rejected with error 1062, because
a small index is cached. Real in the documentation, unreproduced in this
sandbox - which is not the same as absent, so migkit claims nothing either
way.

### B3. Text that was already broken before you moved it

**What happens.** An application sending UTF-8 through a latin1 connection
stores the bytes as latin1 characters. Everything looks right until a
conversion or a client change, then `é` becomes `Ã©`. The repair path is a
`BLOB` round trip - which corrupts the rows that were *not* double-encoded,
so a column with mixed history breaks under a uniform fix.

**migkit: Ends it** for the part that decides whether a repair is safe -
`test_mojibake.py`, on PostgreSQL and MySQL alike. Charset and encoding
parity were already checked (`test_charset_encoding_mysql.py`,
`test_deep_encoding_pg.py`); what is new is reading the text itself:

    deep postgres mojibake: DIFF 1 columns hold both double-encoded and
      correct text: public.notes.body 2 double-encoded and 2 genuinely
      accented (e.g. 'cafÃ©' is really 'café') - converting the whole
      column repairs the first kind and destroys the second

The finding is deliberately *not* "this column has mojibake". It is **which
columns hold both kinds of row**, because that is exactly where the obvious
repair does damage. Measured: the blanket byte round trip over such a column
fails outright on PostgreSQL - `invalid byte sequence for encoding "UTF8":
0xa3` on a genuine `£100` - and the application-side version of the same
fix, the one written with `errors='replace'`, turns it into `�100` without
a word.

The detection is a round trip rather than a search for `Ã`: re-encode the
characters into the bytes they would have been and see whether those bytes
are valid UTF-8 that says something else. That is what keeps it off correct
text, and the separation was measured on a live column before the check was
written:

| caught | left alone |
|---|---|
| `cafÃ©` `naÃ¯ve rÃ©sumÃ©` `Â£100` | `café` `Ångström` `Müller` `Ação` |
| `emâ€"dash` (cp1252) `emâ<80><94>dash` (latin1) | `Ça va` `Ægir` `£100` `日本語` |

Both codecs are tried, because cp1252 maps the C1 block to typographic
characters and is what produces the `â€"` everybody recognises. `£100` and
`Â£100` sitting in one column is the whole problem in two rows, and a test
pins that pair.

One scan per table rather than per column (`octet_length <> char_length` is
the same "has a non-ASCII character" test in both engines), bounded to a
sample, and the rows come back as JSON so a value containing a tab or a
newline cannot be read as three values - a mistake this project made once
already, in the drilldown.

**Missing:** the repair. migkit reports which rows are safe to convert; it
does not convert them.

### B4. The server changed its default collation under you

**What happens.** MySQL 5.7 defaulted to `utf8mb4_general_ci`; MySQL 8
defaults to `utf8mb4_0900_ai_ci`. The upgrade changes nothing that already
exists and everything created afterwards, so a fleet ends up holding both -
and then `Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and
(utf8mb4_general_ci,IMPLICIT) for operation '='` arrives at runtime, often
from inside a stored procedure. The two are not interchangeable: `0900` is
Unicode 9.0 and NO PAD, `general_ci` is a simplified per-character
comparison with PAD SPACE, so trailing spaces and accented characters sort
and compare differently. Converting is its own project - the conversion
regenerates indexes, and rows that were distinct under the old collation can
collide under the new one.

**migkit: Partly.** Collation is compared between the two sides for tables
and columns (`test_deep_collation_pg.py`, `test_charset_encoding_mysql.py`),
and the server-level settings are compared by `check params`.

**Missing:** the *consequence* - which columns would collide under the
target's collation, and which comparisons in the schema now mix two of them.
Both are answerable with a query and neither is asked.

### B5. The timestamp that lost its offset

**What happens.** Neither `timestamp` nor `timestamptz` stores a time zone.
The naive one keeps the digits it was handed and throws away everything that
said what they meant; the other converts to UTC on write. Feed the same
literal to both and they stop agreeing. Measured on PostgreSQL 16:

    insert '2020-11-01 01:05:00+04' into
      timestamp    -> 2020-11-01 01:05:00
      timestamptz  -> 2020-10-31 21:05:00+00

Four hours, discarded in silence, from a value that named its own offset.
The ambiguous hour is the same wound: `2026-11-01 01:30:00-04` and
`2026-11-01 01:30:00-05` are two different instants - measured as
`05:30:00+00` and `06:30:00+00` - and a naive column records both as the
identical digits `01:30:00`. Nothing afterwards can tell them apart, which
is why the bug surfaces twice a year and is blamed on the application.

**migkit: Ends it** - `test_temporal_meaning.py`, on PostgreSQL and MySQL
alike. The gap was found by asking `canon.comparable` directly:

    postgres  timestamp with time zone      -> ('timestamp', '')
    postgres  timestamp without time zone   -> ('timestamp', '')
    mysql     datetime                      -> ('timestamp', '')

All three collapse to one canonical class - which is the right call for
*rendering a value* and the wrong one for *deciding the two sides mean the
same*. Rather than change that mapping and break the rendering that depends
on it, `canon.time_meaning` answers the separate question, and
`check --deep` compares it column by column:

    deep postgres temporal meaning: DIFF 1 columns change what they mean
      between the two sides: public.events.at timestamp with time zone
      (instant) -> timestamp without time zone (wall clock) - one side
      records an instant and the other records digits off a wall clock, so
      the offset is dropped on the way across and a checksum of what
      arrives cannot see it

**The reason it is a check and not a footnote:** `timestamp` means
*opposite things* in the two engines, measured on both. PostgreSQL's
`timestamp` is the wall clock and its `timestamptz` the instant; MySQL's
`datetime` is the wall clock and its **`timestamp` is the instant** -
written at `time_zone '+00:00'` and read back at `'+07:00'`, `datetime`
returned `12:00:00` unchanged while `timestamp` returned `19:00:00`. So a
MySQL `timestamp` landing in a PostgreSQL `timestamp` looks like the
identity mapping and is the silent conversion above. A test pins the table
in both directions precisely because a plausible-looking edit would swap
them.

The check is written entirely on the neutral contract - `neutral_tables`
and `neutral_columns` - so it is not postgres-only by construction, and an
engine whose temporal types migkit has *not* measured is reported as
unmeasured rather than counted as agreement.

### B6. The value the target will not accept at all

**What happens.** MySQL has let `0000-00-00` into date and datetime columns
for decades, and applications leaned on it as "not set" - WordPress is the
famous offender. PostgreSQL does not merely dislike the value, it refuses it:

    mysql     insert '0000-00-00'   -> stored, readable, 1 row
    postgres  select '0000-00-00'::date
              ERROR:  date/time field value out of range: "0000-00-00"

Measured both ways on live servers, with `sql_mode` relaxed on the MySQL
side the way a legacy server has it. The load stops partway, or the mover
substitutes something and the application starts rendering 1970.

**migkit: Partly.** The driver hands the value back as the string
`'0000-00-00'` rather than as NULL or an exception - measured through
migkit's own connection - so a target holding NULL there reads as a
difference and is reported rather than agreed with by accident.

**The general form of the problem is now checked** -
`test_target_capacity.py`, on both engines. A target column that cannot
hold what the source column can is found *before* the move, and reported
as rows rather than as a schema opinion:

    deep postgres target capacity: DIFF 1 columns hold values the target
      has no room for: public.people.note 2 rows, largest 120 against the
      target's 50 - the load stops on the first of them, with whatever
      moved before it already on the target

Narrowing on its own is deliberately **not** a finding: a `varchar(255)`
rebuilt as `varchar(50)` where every value is short is a non-event, and a
check that cried wolf on every rebuilt schema is one nobody reads. It says
so separately - "2 columns are narrower on the target and no row exceeds
any of them yet" - which is the thing an operator wants to know before it
becomes true. Characters, bytes, whole numbers and decimal precision are
all counted, each against the server's own refusal: the tests assert that
PostgreSQL really does answer `out of range` for the values being flagged.

Two traps it was written around. `text` has no limit to compare, so it
would fall out of a naive comparison as "nothing to worry about" when it is
the widest source there is - it is carried as an explicit unlimited rather
than as unknown. And MySQL's TEXT family is limited in **bytes** while
`varchar(n)` is limited in **characters**, so a utf8mb4 string of 20,000
characters can overflow a 65,535-byte TEXT: those pairs are counted and
reported as not compared rather than quietly passed.

**Missing:** the values that are the wrong *shape* rather than the wrong
size - `0000-00-00` is the example above, and it needs the hetero path
rather than a capacity number.

### B7. The time zone rules are not on both servers

**What happens.** MySQL keeps named zones in `mysql.time_zone*` tables that
somebody has to load with `mysql_tzinfo_to_sql`. Where they are missing,
`CONVERT_TZ` with a named zone returns NULL - and it does not complain.

Measured, and worse than the write-ups describe. The official `mysql:8`
image arrives with **1,795** zones loaded, so the usual advice ("check
whether it is empty") reads as already handled. Emptied and restarted, the
same server gives:

    select convert_tz('2026-07-01 12:00:00','UTC','America/New_York')  -> NULL
    show warnings                                                      -> (nothing)
    select convert_tz('2026-07-01 12:00:00','+00:00','-04:00')         -> 08:00:00

The offset form still works, so a smoke test written with `+00:00` passes
while every named zone silently answers NULL. And it is not confined to
queries: a **stored generated column** defined with `CONVERT_TZ` wrote
`NULL` to disk for a row whose source value was present. Real data, on
disk, wrong, with no error anywhere.

**migkit: Ends it** - `test_time_zone_rules.py`. `time_zone` and
`system_time_zone` were already compared as critical parameters, which is a
different question: both sides can agree on the session zone while one of
them cannot resolve a zone name at all. `check --deep` now asks every zone
what wall clock it shows at seven probe instants and compares the
fingerprints:

    deep mysql time zone rules: DIFF 1 zone names the target cannot
      resolve: Asia/Tehran - a conversion naming one of these returns NULL
      on the target and the same expression worked on the source

Three verdicts, all proven live against two MySQL servers: a zone whose
**rules differ** is a warning (`America/Sao_Paulo`), a zone the target
**cannot resolve** is a difference, and a server that can resolve
**nothing** is the worst of the three. The same test asserts the two
details that make this invisible without it - the offset form
`'+00:00'`/`'-04:00'` still answers correctly on the broken server, and
`show warnings` says nothing.

**The probes are chosen, not arbitrary.** A zone is its history, so asking
today's offset would miss what actually breaks a migration. Brazil
abolished DST in 2019 and `America/Sao_Paulo` reads `10:00` at the 2018
probe against `09:00` at the 2020 one - a test pins that pair, so a future
edit cannot quietly reduce the probes to instants where every rule set
agrees.

**The fingerprint is portable between engines**, which was measured rather
than hoped for: PostgreSQL 16 and MySQL 8 produce the *same* md5 for
`America/New_York`, `Asia/Tehran`, `Europe/Lisbon` and `UTC`, because both
are being asked the same question about the same instants. A test pins the
equality and also checks the four values differ from each other, so the
agreement cannot be the trivial kind.

**What could not be reproduced here:** two PostgreSQL images four major
versions apart (13 and 16) agreed on all **487** zones at these probes, so
tzdata drift between those two images is not demonstrable in this sandbox.
The drift path is proven on MySQL, where the rules can be made to differ
for real.

**Missing:** which zones the *data* actually uses. Every finding above is
about what the servers can do, not about how many rows depend on it.

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

Since measuring it, the chooser takes pgcopydb from a local binary rather
than insisting on the container image, which cut the same move from 11.7 s
to 5.4 s - and every mover's result is now checked by
`Engine.moved_nothing`, so a tool that exits 0 and leaves the target empty
cannot be reported as a completed move. Test:
`test_move_moved_something.py`.

**Missing:** the pairs with no native mover, and a comparison against a
managed service on the same hardware. Until that exists migkit makes no
claim about being faster than anything beyond the two movers measured.

### C2. LOBs

**What happens.** Full LOB mode moves them one at a time and dominates the
whole task. Limited LOB mode is fast and **truncates past `LobMaxSize` with
a warning in a log nobody reads** - data loss a row count will never find.
`bytea` stops at 1 GB and gets difficult around 500 MB. `pg_largeobject` is
one table, so `pg_dump`/`pg_restore` are single-threaded for every large
object in the database - one team's 12.5 hour downtime was almost entirely
this.

**migkit: Ends the silent part.** `migkit check --deep` reports the biggest
value in every column that can hold one - the number a mover's LOB limit has
to be set above - and, more usefully, compares it against the target's: a
column whose largest value is smaller on the target than on the source is
what truncation leaves behind, and it names the column and both sizes. It
also warns when a value approaches the engine's own ceiling (PostgreSQL's
1 GB field, MySQL's `max_allowed_packet`). Tests: `test_lob_sizes.py`.

Two things were measured rather than assumed. The obvious cheap filter -
only look at tables whose TOAST relation holds data - has a hole: a
1,000,000-byte value compressed to 11,452 bytes on the way in, so a check
reading stored sizes would have passed over exactly the value most likely
to be truncated. And it buys nothing: `max(octet_length(col))` over two
columns of a 2,000,000-row, 531 MB table answered in 0.25 s. The check
reports `octet_length`, which is the size a limit is compared against,
rather than `pg_column_size`, which is what was left after compression.

**Missing:** `pg_largeobject`-style out-of-table LOBs are inventoried by
`handwork` but not sized, and nothing yet measures how long they will take
to move.

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

### C5. The index that exists, is useless, and costs you anyway

**What happens.** `CREATE INDEX CONCURRENTLY` is the only way to add an
index to a live table, and when it fails - a duplicate for a unique index, a
cancelled statement, a dropped connection, a `statement_timeout` that was
set for ordinary queries - the index is **not** rolled back. It stays in the
catalog marked invalid. The planner ignores it, so it shows zero scans and
reads as an unused index, while it is still maintained on every write, still
occupies space, and still blocks creating an index of that name again.
Migration tooling hits this constantly: a big table plus a low
`statement_timeout` is the recipe.

**migkit: Ends it** - `tests/test_invalid_indexes.py`. Before the check
existed this was measured: a target left with two invalid indexes, one of
them sharing a name with a *valid* index on the source, produced a full
`migkit check --deep` report that mentioned the word "invalid" **zero
times**. The schema diff noticed an index the source did not have and said
"1 to remove", which reads as a spurious index rather than a broken one; when
the name existed on both sides it said nothing at all.

`migkit check --deep` now reads `indisvalid` on **both** sides, because on
the source an invalid index is a reason not to migrate yet and on the target
it is the post-load rebuild that died:

    deep postgres indexes: DIFF 1 indexes exist but answer no query:
      target public.orders.orders_uniq (not maintained - only the name is
      taken) - a CREATE INDEX CONCURRENTLY that failed leaves this behind
      and does not undo it

The two states are told apart because they cost differently, and both were
produced on a live PostgreSQL 16 rather than assumed. A build that failed
before it finished (`indisready=f`) stayed at **0 bytes** and never grew. A
build cancelled after it finished but before it was validated
(`indisready=t`) grew from **4.5 MB to 12.3 MB over 200,000 inserts** while
`EXPLAIN` on the indexed column still chose a sequential scan - every write
paying for an index no read can use. Either way `CREATE INDEX` with that name
fails with `already exists`.

The nuance a naive check gets wrong is handled rather than ignored: a
partitioned parent's index (`relkind='I'`) is invalid **by design** until
every partition's index is attached, so it is a warning that says so, not a
fault - and a real fault in the same report outranks it.

MySQL answers the same command with a **skip that explains itself**, which is
a measured answer and not a shrug: a failed `ADD UNIQUE INDEX` over duplicate
rows rolled back completely - `information_schema.statistics` came back empty
and `innodb_indexes` held only `GEN_CLUST_INDEX` - while a valid index
created straight afterwards *did* appear in the same query, so the empty
answer was real. The one state MySQL does have, a secondary index InnoDB has
marked corrupt, has no catalog column at all and surfaces only as error 1712.
Unknown, not zero.

**On the rebuild settings, a correction.** An earlier pass here recorded
that `migkit move` should raise `maintenance_work_mem` and
`max_parallel_maintenance_workers` for "the rebuild it triggers". Checked:
**migkit never builds an index at all** - there is no `CREATE INDEX` in any
code path, only in hint text, and `test_setup_target_plan_pg.py` pins that.
And the settings themselves did not reproduce here: 64 MB to 1 GB and two
workers to four made three index builds take **1.65 s against 1.54 s** on
two vCPUs. The 21.99 s / 12.82 s figures came from an article on other
hardware, so migkit does not recommend the setting.

**What the same look did find** is in [A7](#a7-the-order-the-target-is-built-in).

### C6. The target rewrote the rows as they landed

**What happens.** A trigger on the target fires on every row the load
inserts. `updated_at` becomes the time of the migration instead of the time
the row was last touched; an audit table gains one entry per migrated row;
a denormalised counter is incremented by the whole table. AWS says it
plainly for DMS - triggers present on the target "may change data loaded by
AWS DMS in unexpected ways" - and its premigration assessment looks for
them.

Measured here rather than taken on trust. A `BEFORE INSERT` trigger setting
`updated_at := now()`, then a plain `COPY` of two rows carrying 2001 and
2002:

    source     2001-01-01 00:00:00+00   2002-02-02 00:00:00+00
    target     2026-09-19 07:06:58+00   2026-09-19 07:06:58+00
    COPY said  COPY 2

Two rows, both stamped with today, and the load reported success. The same
`COPY` under `set session_replication_role = replica` kept `2003-03-03`
exactly as it was sent.

**migkit: Ends it for the move** - `test_triggers_during_load.py`. The
gap was confirmed with a real `migkit move --mode full --go` rather than
inferred: the two rows above landed stamped with today's date and migkit
printed `bulk copy complete`. The loading connection now carries
`session_replication_role = replica`, and the same move lands
`2001-01-01` and `2002-02-02` untouched.

A **connection option, not `ALTER TABLE ... DISABLE TRIGGER`.** Disabling
triggers is a change to the target that outlives a crash, and a target left
with disabled triggers is the exact failure this same deep check reports -
trading one silent corruption for another. A test asserts the trigger is
still `tgenabled = 'O'` after a move *and* that it still fires, because the
catalog flag only claims the first.

Measuring the scope kept the change to one line. The `pg_dump`/`pg_restore`
path was **already** covered - migkit passes `--disable-triggers` there -
so only pgcopydb was exposed. Both movers are now pinned by tests, so they
cannot drift apart on something this quiet, and the source URI deliberately
does *not* get the option: the source is only read.

The control matters as much as the fix: a separate test loads a row through
an unguarded connection and asserts the trigger **does** rewrite it, so a
green suite cannot mean the trigger was never firing.

**Missing:** the deep check still only reports triggers that are
*disabled*. Naming the enabled ones on the tables a move is about to write
would tell an operator what is being quieted on their behalf.

### C7. The target refuses the key you are carrying

**What happens.** A column defined `GENERATED ALWAYS AS IDENTITY` rejects
an explicit value outright. `serial` behaves like `GENERATED BY DEFAULT`
and accepts one, so a schema "modernised" from serial to identity during
the migration stops accepting the very rows being migrated.

    insert into ident (id, v) values (7, 'a');
    ERROR:  cannot insert a non-DEFAULT value into column "id"
    DETAIL:  Column "id" is an identity column defined as GENERATED ALWAYS.
    HINT:  Use OVERRIDING SYSTEM VALUE to override.

The same statement with `OVERRIDING SYSTEM VALUE` succeeds, and a
`GENERATED BY DEFAULT` column accepts the explicit value with no clause at
all. All three measured on PostgreSQL 16.

**migkit: Ends it** - `test_identity_target.py`. This one bit migkit
itself first. The repair path built `insert into ... ("id", "v") values
(...) on conflict ("id") do update set ...` with no `OVERRIDING` clause,
and calling migkit's own `_apply_upsert` against a generated-always target
produced the error above - with an ordinary table beside it as a control,
so the failure was the identity column and not the path. `migkit apply`
could not repair a single row on such a target.

It now looks the column up and emits `OVERRIDING SYSTEM VALUE` only where
the catalog says `attidentity = 'a'`. `GENERATED BY DEFAULT`, which is what
`serial` behaves like, takes the value as given and gets no clause - a test
pins both, because lumping them together would be the easy mistake.
Emitting the clause unconditionally was measured to be harmless, so the
lookup is for clarity rather than safety, and it is cached per table.

**`move` was never affected**, and measuring that is what kept the change
small: `COPY` into a generated-always column succeeds with no clause at all
(`COPY 1`, value stored). Believing otherwise would have produced a much
wider change for nothing.

A test also asserts the server still refuses the unclaused statement, so if
that assertion ever stops failing it means the sandbox changed rather than
that the problem went away.

### C8. The column the target computes for itself

**What happens.** A `GENERATED ALWAYS AS (...) STORED` column is the
server's to write, and it refuses anybody else's value - measured:

    copy gen (id, price, qty, total) from stdin
    ERROR:  column "total" is a generated column
    DETAIL:  Generated columns cannot be used in COPY.

**migkit: Not yet, and it is the same family as C7.** `_apply_upsert`
builds its column list from the row it is repairing, generated column
included, and against such a target it answers `cannot insert a non-DEFAULT
value into column "total"`. So `migkit apply` cannot repair a row in any
table that has one.

The fix is **not** the one C7 needed, which is worth writing down before
somebody assumes it is: `OVERRIDING SYSTEM VALUE` was tried here and
rejected with the same error. A generated column has to be left out of the
statement entirely, after which the server computes it - measured, an
insert omitting `total` stored `total=20` from `price * qty`. The catalog
signal is `pg_attribute.attgenerated <> ''`.

Comparing the column is right and stays: `neutral_columns` reports it, so a
target whose generated expression differs from the source's shows up as a
value difference. It is only the **writing** that has to change.

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

### D8. The verifier was looking through a filter

**What happens.** Row-level security is a `WHERE` clause the server adds to
every query, and it applies to whoever is connected - including the tool
doing the verifying. A hop configured with an application role, which is
the natural thing to do when nobody wants to hand a migration tool
superuser, sees only the rows that role is allowed to see. On both sides.

PostgreSQL is careful about this where it can be: `pg_dump` sets
`row_security = off` and **errors** rather than dumping a subset, by
explicit design. The trap is `--enable-row-security`, which someone adds to
make a failing backup script "work" - after which the dump succeeds and
contains part of the table.

**migkit: Partly, and the part that is missing is the loud one.** The deep
check already catches the condition and says so:

    deep postgres rls: DIFF migkit's source role is subject to RLS on 1
      tables

It also reports RLS tables with **zero** policies, which read as empty to
anyone who is not the owner.

But the passes that pronounce on the data do not know. Measured, with the
same policy on both sides and **five of the source's ten rows deleted from
the target**:

    counts   postgres: OK 1 tables, rows 5==5
    data     postgres: OK 1 tables, 5 rows, checksums equal both sides

Half the table missing on the target, and the two checks whose whole job is
to say whether the data landed both said OK. They were not wrong about what
they compared; they compared five rows to five rows. Nothing in either line
says the five was a filtered count.

**Missing:** `counts` and `data` should refuse to pronounce - or at least
say what they could not see - when the connected role is subject to RLS on
the tables being checked. The deep check already has the facts; the
verifying passes do not ask for them.

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

### F4. There is a pooler between you and the database

**What happens.** PgBouncer in transaction mode "breaks client expectations
of the server by design" - its own words - and nothing about it errors
loudly. Session-level `pg_advisory_lock` is the trap that matters for
migrations: the lock is taken on one backend and the unlock may land on
another, leaking it. Measured by one report against PgBouncer 1.25.2, a
migration runner's session lock leaked onto a pooled connection and a later
direct client blocked about 50 seconds until the pooler recycled it. Prepared
statements fail intermittently for the same reason unless
`max_prepared_statements` is on (1.21+), replication connections cannot be
routed through it at all, and a single `SET` outside a transaction pins a
client to one backend for the rest of its session.

**migkit: Not yet.** migkit connects with whatever the hop names, and a hop
pointed at a pooler would get CDC, advisory locks and session settings that
behave differently from the direct connection the checks assume - with no
warning. The detection is cheap: a PgBouncer connection answers
`SHOW LISTEN_ADDR` on its admin console, and more usefully the server
version string and `pg_backend_pid()` behaviour differ from a direct
connection across two statements. A hop that must not go through a pooler -
the CDC one especially - should say so before it fails oddly.

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
Later research passes:
[cloud egress pricing 2026](https://spendark.com/blog/cloud-egress-costs-guide/),
[AWS DMS with partitioned PostgreSQL tables](https://aws.amazon.com/blogs/database/migrate-data-from-partitioned-tables-in-postgresql-using-aws-dms/),
[pg_partman online partitioning gotchas](https://medium.com/fresha-data-engineering/divide-and-partition-pg-partman-online-partitioning-gotchas-042b1af626a5),
[CREATE INDEX documentation](https://www.postgresql.org/docs/current/sql-createindex.html),
[the hidden cost of invalid indexes](https://postgres.ai/blog/20260106-invalid-index-overhead),
[making index creation faster](https://techcommunity.microsoft.com/blog/adforpostgresql/postgresql-making-index-creation-faster/4067939),
[MySQL 8.0 collations: migrating from older collations](https://dev.mysql.com/blog-archive/mysql-8-0-collations-migrating-from-older-collations/),
[PgBouncer features and limitations](https://www.pgbouncer.org/features.html),
[prepared statements in transaction mode](https://www.crunchydata.com/blog/prepared-statements-in-transaction-mode-for-pgbouncer).

Temporal types and time zones:
[the DST bug that only breaks twice a year](https://www.codewithkarani.com/blog/postgres-timestamp-vs-timestamptz-dst-bug),
[Oracle datetime and time zone support](https://docs.oracle.com/en/database/oracle/oracle-database/18/nlspg/datetime-data-types-and-time-zone-support.html),
[incorrect date value '0000-00-00'](https://tableplus.com/blog/2019/10/incorrect-date-value-0000-00-00-date-datetime.html),
[pgloader and MySQL default dates](https://github.com/dimitri/pgloader/issues/252),
[convert_tz returns null if a named time zone is used](https://bugs.mysql.com/bug.php?id=12445),
[mysql_tzinfo_to_sql](https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/mysql-tzinfo-to-sql.html),
[foreign keys and circular dependencies](https://www.cybertec-postgresql.com/en/foreign-keys/),
[deferrable SQL constraints in depth](https://begriffs.com/posts/2017-08-27-deferrable-sql-constraints.html).

What the target does to the rows as they land:
[using PostgreSQL as a DMS target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.PostgreSQL.html),
[DMS best practices](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html),
[SQL Server premigration assessments](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.SqlServer.html),
[handling identity columns in AWS DMS](https://aws.amazon.com/blogs/database/handle-identity-columns-in-aws-dms-part-1/),
[PostgreSQL identity columns](https://www.postgresql.org/docs/current/ddl-identity-columns.html),
[INSERT and OVERRIDING](https://www.postgresql.org/docs/current/sql-insert.html).

The verifier's own blind spots:
[row security considerations](https://wiki.postgresql.org/wiki/Row_Security_Considerations),
[pg_dump and row_security](https://www.postgresql.org/docs/current/app-pgdump.html),
[partial dumps using RLS, on purpose](https://supabase.com/blog/partial-postgresql-data-dumps-with-rls),
[insert/dump/restore with generated columns](https://postgrespro.com/list/thread-id/2558357).

Cutover, aftermath and compliance:
[zero-downtime patterns](https://launchdarkly.com/blog/3-best-practices-for-zero-downtime-database-migrations/),
[post-upgrade statistics](https://techcommunity.microsoft.com/blog/azuredbsupport/azure-postgresql-lesson-learned-8-post-upgrade-performance-surprises-the-one-ste/4471807),
[migration compliance checklist](https://syncopio.com/blog/data-migration-compliance-checklist/).
