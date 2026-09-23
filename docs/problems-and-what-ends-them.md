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

**The pre-flight now says them together, early** - `test_preflight.py`.
The gap was never that the checks did not exist; it was that the ones which
predict what a move will *do* could only be reached from `check --deep`,
the command you run afterwards. Measured on a pair whose target column had
been narrowed and whose `timestamptz` was landing in a `timestamp`:

    assess     13 pass, 5 warn, 2 fail     (neither mentioned)

    check --deep
      postgres target capacity: DIFF 1 columns hold values the target has
        no room for
      postgres temporal meaning: DIFF 1 columns change what they mean

`assess` now runs the predictive checks too, and the fix travels with the
finding:

    fail  before the move  postgres target capacity  ... [fix] widen the
      target column, or decide what happens to those rows before the move
      rather than halfway through it
    16 pass, 5 warn, 4 fail

One implementation, read from two places - a test asserts the deep detail
appears verbatim in the pre-flight row, so the two cannot drift into
separate copies.

**What is left out matters as much.** The checks that compare what is *on*
the target - large objects, extension data, duplicate keys, counts - would
report a difference against the empty target that precedes every move, and
a wall of red in front of every migration is how people learn to skim the
section. A test asserts those stay out, by name.

A check that cannot run reports **warn**, not pass: a pre-flight row that
went green because the query failed would be the worst kind of
reassurance.

**Still missing:** the target-side refusals that are not about data -
privileges the account does not have, extensions the managed target will
not install. `assess` reports those separately today rather than in this
section.

### A3. Extensions and plugins - PostGIS is the worst case

**What happens.** `pg_upgrade` restores a geography column before
`spatial_ref_sys` has rows and fails with `Cannot find SRID (4283) in
spatial_ref_sys` - reproduced across PostgreSQL 11, 13 and 16 with PostGIS
3.2 and 3.4, and only if you use a non-4326 SRID. Custom `spatial_ref_sys`
rows are not preserved by a normal dump, and restored rows do not override
existing ones. Both GCP and Azure state plainly that extensions their
managed target does not support are simply not migrated - the job succeeds
and the extension is gone.

**migkit: Ends it** - `test_deep_extensions_pg.py` and
`test_extension_data.py`. An extension is three things, and all three are
now compared.

**A correction first.** An earlier pass here recorded extension *versions*
as missing. They were not: `check --deep` has always compared them, and a
live pair with `hstore 1.8` on one side and `1.6` on the other reports
`DIFF version mismatch: hstore 1.8`. A test pins it so the claim cannot go
stale again in the other direction.

**What genuinely was missing is the data extensions own.** PostGIS keeps
coordinate systems in `spatial_ref_sys`, registered through
`pg_extension_config_dump` so that a custom SRID is dumped at all - and a
restore does not overwrite rows the target already has, so a target
carrying the stock table keeps its own copy and the custom entry is quietly
absent. `check --deep` now reads `pg_extension.extconfig` - the general
mechanism, not a PostGIS special case - and compares the contents:

    deep postgres extension data: DIFF 1 tables an extension owns differ:
      hstore public.srids: src=2|... dst=1|... - the extension is installed
      and the rows it needs are not the same

The comparison is a **checksum, not a count**, and a test proves why: a
target holding one row under the right SRID number with the wrong
definition passes a count and fails this. The same fingerprint now serves
the materialized-view check, which had its own copy of that expression.

**How it is tested without PostGIS.** No contrib extension in the stock
image registers data - measured, `extconfig` is NULL for every one of them,
and a test asserts that rather than assuming it. So the tests hand a table
to `hstore` through the catalog, the way the collation tests age a
collation. What that exercises is the check's ability to find and compare a
table an extension owns, which is the part that was missing.

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

**migkit: Ends it** - `test_mojibake.py` for the part that decides
whether a repair is safe, on PostgreSQL and MySQL alike, and
`test_mojibake_repair.py` for the repair itself. Charset and encoding
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

**The repair is done too** - `test_mojibake_repair.py`. `sync` plans one
`UPDATE` per value `_double_encoded` confirms and nothing else, so the rows
that were never broken are not in the statement list at all:

    update "public"."notes" set "body" = 'éclair'
      where "id" = '1' and "body" = 'Ã©clair';

Measured on a column holding `Ã©clair`, `café`, `plain ascii`, `£100`,
`Ã¼ber` and `æ¥æ¬èª`: three rows repaired, three byte-identical
afterwards, and a second pass finds nothing left - repaired text no longer
classifies as double-encoded, so the work converges where running the
column conversion twice destroys it. The blanket one-liner on that same
column does not manage a single row: `ERROR: invalid byte sequence for
encoding "UTF8": 0xe9`, thrown by the `é` in `café`.

Three decisions the tests pin:

* **It writes to the target, never the source.** The source is somebody's
  live database and migkit writes to it nowhere; the target is the copy it
  is answerable for.
* **The statements are withheld by default.** Every other repair here moves
  the target *towards* the source. This one moves it away on purpose,
  because the source is what is broken - and the resync action standing
  next to it in the same plan would copy the broken text straight back. So
  the action is always listed, with that consequence in its note, and
  carries statements only under `MIGKIT_REPAIR_TEXT=1`. An automated
  reconcile loop rewrites no text on its own.
* **Every update matches the old value as well as the key**, so a row
  somebody edited between the plan and the apply is skipped rather than
  overwritten with a repair of text that is no longer there.

What it will not do is said rather than skipped: a table with no primary
key, and a broken column that *is* the primary key, are both named in the
note - rewriting a key moves the row every foreign key points at.

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

**The deep check now says both halves** -
`test_triggers_reported.py`. It used to look one way only, reporting `OK no
disabled triggers on target` while two user triggers sat on a table the
load was about to write:

    deep postgres triggers: OK no disabled triggers on target; 2 enabled on
      tables a load writes (public.notes.notes_audit,
      public.notes.notes_stamp) - `migkit move` runs with
      session_replication_role = replica, so these will not fire for the
      migrated rows

Silencing them is right; not saying so was not. Work that does not happen
is worth naming - an audit trigger records nothing for the migrated rows,
and a denormalised counter is not maintained.

**Two kinds of noise are left out on purpose**, because a line listing
things nobody can act on is a line people learn to skip: a foreign key's
own constraint triggers (internal, and this target really does carry them),
and tables that exist on the target alone, which a move never writes. A
control test asserts both are genuinely present before the exclusions are
checked.

A disabled trigger remains a **difference**, not a note at the end of an ok
line, and a test asserts the fault wins the line rather than sharing it.

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

**migkit: Ends it** - `test_generated_columns.py`, on both engines.
`_apply_upsert` built its column list from the row it was repairing,
generated column included, and against such a target answered `cannot
insert a non-DEFAULT value into column "total"`. `migkit apply` could not
repair a row in any table that had one.

The fix is **not** the one C7 needed, which is why the two are written down
separately: `OVERRIDING SYSTEM VALUE` was tried here and rejected with the
same error, and a test pins that it still is. A generated column is left
out of the statement entirely and the server computes it - measured with a
deliberately wrong value carried in, so the row proves who did the
arithmetic: `price=7, qty=4` stored `total=28`, not the 999 migkit was
holding.

MySQL refuses the same thing in its own words - `ERROR 3105 (HY000): The
value specified for generated column 'total' in table 't' is not allowed` -
and gets the same treatment, including **VIRTUAL** columns, which are not
stored at all and are refused just as flatly.

**The movers were never affected**, measured with real moves rather than
inferred from the COPY documentation: both landed all five rows with the
totals computed. That is what kept the change inside the repair path.

Comparing the column is untouched: `neutral_columns` still reports it, so a
target whose expression differs from the source's shows up as a value
difference. Only the writing gives way.

---

### C9. The table you told it not to touch

**What happens.** A bulk load is data-only, so the target has to be emptied
first or every row arrives twice. Emptying it is a separate step from
copying, and the two get told different things. The copy is told which
tables to skip; the truncate is generated from the catalogue and is told
nothing - so a table that was deliberately left out of the copy is emptied
by the step before it and never refilled.

The tables this costs you are the worst ones to pick: a hop excludes a
table precisely because its rows are written on the target and not carried
from the source, so there is no source copy to recover from.

Measured, a target holding two rows no source ever had:

    target audit_log before: 2
    $ migkit move pg --go
      # 1 tables the hop excludes are not dumped at all
      pg_dump ... -T public.audit_log
      pg_restore ...
      pgdump reported success and appdb is still empty on the target:
      public.audit_log
    target audit_log after: 0

Three failures in one run. The rows the setting exists to protect were
deleted; the plan announced it was protecting the table in the same breath;
and the closing line blamed the copy for a table the copy had been told to
skip, which sends the reader to the wrong end of the problem.

**And leaving the table out of the statement is not enough.** The truncate
carries `cascade`, because the tables being replaced reference each other.
PostgreSQL follows those references into tables nobody named, and mentions
it while it is doing it:

    truncate table public.orders cascade;
    NOTICE:  truncate cascades to table "audit_log"

**migkit: Ends it** -
`test_the_move_does_not_empty_what_the_hop_protects.py`, on a live pair.
The excluded set is resolved through the same `excluded_tables()` the dump
and `check` use, so all three empty, carry and verify the same tables
rather than three readings of one pattern. The cascade is asked about
first - the same catalogue that emits that notice answers the question in
time - and a reachable excluded table stops the move before anything is
emptied, rather than being emptied and reported.

It **refuses** instead of deciding: both ways out are the operator's call.
Either the table is not target-owned after all and belongs in the copy, or
the foreign key into the replaced data has to go first. Choosing either one
on their behalf destroys something.

The plan line moved with it. It used to read `# truncate all user tables on
target` in both bulk paths; it now says whether the hop's exclusions apply,
from one function, because a plan that claims to empty everything beside a
run that skips some is how a reader concludes the table was refilled.

**And the check after the copy had the same blind spot.** Once the copy
exits, migkit looks for tables the source has rows in and the target has
none of - the guard against a copy that exits 0 and moves nothing. It
looked at every table, so an excluded table that is empty on the target
(a new target-owned table, say) read as a copy that had failed. Measured,
after a move that did exactly what the hop said:

    target: orders=1 audit_log=0
    pgdump reported success and appdb is still empty on the target:
    public.audit_log. Nothing has been marked as moved

A correct move, reported as failed, blamed on the copy, and never recorded.
The guard now leaves out what the hop excludes - through `hop.excluded()`,
at its one caller, so it covers every engine - and a carried table left
empty beside an excluded one is still stopped. That line also named the
program that ran, from a variable the tool-name scan could not see; it now
says "the bulk copy". `test_move_moved_something.py`.

MySQL differs here in a way worth writing down. Its `TRUNCATE` does not
cascade; it refuses a table another table references:

    ERROR 1701 (42000): Cannot truncate a table referenced in a foreign
    key constraint (`appdb`.`audit_log`, CONSTRAINT `audit_log_ibfk_1`)

and with `foreign_key_checks = 0` in the same session it empties the
referenced table and leaves the referencing one alone - measured, the
excluded child kept both its rows. The MySQL path now empties the target
itself that way, so the PostgreSQL refusal has no counterpart to need -
nothing the hop excludes can be reached.

**The index window was the third step told nothing.** Both bulk paths drop
the target's secondary indexes for the load and rebuild them after - a
measured 1.9x on PostgreSQL - and both did it to every table, the excluded
ones included. On PostgreSQL that includes a unique index built with
`CREATE UNIQUE INDEX`, which is not a constraint. Measured, with the
application writing to the table it owns while the window was open:

    3 secondary indexes dropped for the load
    REBUILD FAILED for audit_ref_u: Key (ref)=(r7) is duplicated.
    2 of 3 indexes rebuilt; STILL MISSING: audit_ref_u

The table the hop said not to touch lost the constraint that would have
refused the duplicate, and now held it - and the advice to "recreate them
before the target is used" could no longer be followed. Nothing is loaded
into an excluded table, so dropping its indexes never bought anything. Both
windows now leave them in place, through `excluded_tables()`, and say how
many they left; the same run refuses the application's duplicate and
rebuilds only the carried table's index.
`test_the_move_does_not_empty_what_the_hop_protects.py`,
`test_index_window_mysql.py`. What it leaves is quieter: a row in the
excluded table can point at a row that existed only on the target and is
not coming back, and `check` does not look inside a table the hop excludes.
So the move says it, once the load is done - counted per foreign key, only
where the referencing table is excluded and the referenced one was
replaced, and said as "could not look" rather than nothing when the
catalogue cannot be read. `test_the_mysql_bulk_path_runs.py`.

### C10. The move that failed and emptied the target anyway

**What happens.** A data-only load has to empty the target first, and the
emptying is the one step that cannot be taken back. If it runs before the
copy has anything to load, every failure after it - an unreachable source,
a password that changed, a dump that dies half-way - leaves the target with
nothing in it. Measured on the pg_dump path, with a source nobody could
reach:

    move failed: ... Is the server running on that host and accepting
    TCP/IP connections?
    target orders: 2 rows before the move, 0 after

**migkit: Ends it** - `test_the_pgdump_path_empties_last.py`,
`test_the_mysql_bulk_path_runs.py`. Both paths that dump to disk now dump
first, empty second, load third, so the target is touched only once a
complete dump exists. The streaming path cannot do that - it has no dump -
and instead reaches both ends before it empties anything.

Two more faults came out of the same function. A hop that excludes tables,
on a source whose table list could not be read, went ahead with a note in
the plan - and since the emptying keeps excluded tables, the unfiltered
dump would have loaded the source's rows on top of the target's own. Both
dump paths now stop there with the same refusal, raised from one place.
And `pg_restore -d` named the source's database while the emptying named
the target's, so a hop with a `db_map` emptied one database and loaded
another.

### C11. The filter the move applied and the check did not

**What happens.** A hop that moves only some rows of a table (`mapping.
where`) gets a correct move and a check that calls it wrong for good: the
check compares the filtered target against the whole source. Measured on a
move that did exactly what the hop asked:

    PostgreSQL  counts  public.orders src=4 dst=2
    MySQL       data    missing=2  kind=rows-missing

The table copiers had the opposite fault: they ignored the filter and
copied every row. And the PostgreSQL bulk paths, which cannot filter rows,
refused the whole database instead of moving what they could.

**migkit: Ends it** - `test_the_check_reads_the_row_filter.py`. Every read
the check makes goes through one scope per engine that applies the hop's
filter; target rows outside the filter are counted and reported on their
own line, so the narrower comparison hides nothing. The table copiers read
and replace only the filtered rows. The bulk paths leave the filtered
tables out and hand them to the table copier, so the rest of the database
still goes the fast way. A path that can apply no filter refuses before
anything is copied.

### C12. The exclude list that only some engines read

**What happens.** `exclude` is how a hop protects a table the target owns:
migkit neither verifies nor repairs it. Only PostgreSQL, MySQL and MongoDB
read it. Measured on SQLite with `exclude: [audit]` and a row only the
target has in `audit`:

    counts  main        diff  audit src=1 dst=2
    repair  main.audit  delete 1 rows the source does not have: 7

and after `apply` the target's own row was gone. The MongoDB bulk path read
no exclude list either, and its restore drops each collection before
loading it.

**migkit: Ends it, except on SQL Server** -
`test_every_engine_honours_exclude.py`, `test_redis_honours_exclude.py`,
`test_kafka_honours_exclude.py`, `test_generic_honours_exclude.py`,
`test_the_mongo_bulk_path_honours_exclude.py`. SQLite tables, Redis key
patterns, Kafka topics and the generic engine's listed tables now go
through the same `hop.excluded()` rule; the MongoDB bulk path leaves an
excluded collection out of both the dump and the restore. SQL Server still
ignores the list and cannot be tested on this machine.

### C13. The copier that left the target's strays behind

**What happens.** The keyed table copiers replace one key range per chunk,
from the source's lowest key to its highest. A target row whose key lies
outside that range is in no chunk, so it stays: a target carrying an
earlier attempt keeps its strays, and a source table that is empty leaves
the target's rows untouched.

**migkit: Ends it** - `test_the_copier_removes_what_the_source_does_not_have.py`.
Once every chunk is done, the PostgreSQL and MySQL copiers remove the
target rows outside the source's range, within the hop's row filter only,
and say how many.

### C14. The password left on disk by a dry run

**What happens.** The one-pass MySQL-to-PostgreSQL path writes a load file
holding both connection strings, passwords included. It wrote it on a dry
run too, and never removed it. The same load named the source's database
on the target side, so a hop whose `db_map` renames the database loaded
into the wrong one. The MongoDB restore had the same `db_map` fault.

**migkit: Ends it** - `test_progress_in_migkit_words.py`,
`test_the_mongo_bulk_path_honours_exclude.py`. The load file is written
only for `--go`, owner-readable, and removed whether the load works or
not; both paths load into the target's name for the database.

### C15. The cross-engine copy that moved only what the target already had

**What happens.** The copier between two engines listed the tables to move
by pairing the source's list with the target's, and kept only the pairs.
Measured SQLite to SQLite, with three source tables and a target holding
only `log`: the list to move was `log`, and `orders` was left out without a
word - while the copier had the code to create a missing table all along.
A target file that did not exist yet stopped the move at the listing. It
also wrote without emptying: a row the target held before stayed, and a
key-less table doubled when the move ran again. A renamed table the target
lacked was created under its old name.

**migkit: Ends it** - `test_the_cross_engine_copier_empties_first.py`.
Every source table not excluded is listed; a missing one is created under
the name the hop's mapping gives it; a name that appears in two schemas
stops the move and says so. A table starting afresh is emptied on the
target first, through `neutral_empty`, which every engine that writes
across engines implements and which refuses a source. The same copier now
carries SQLite to SQLite table by table, so there is one copier to fix.

### C16. The schema that changed while the rows were moving

**What happens.** An application alters a table on the source while the
move is running. The rows copied before the change and after it belong to
two different tables, and the move said `move complete` over both.

**migkit: Partly ends it** - `test_ddl_during_the_move.py`. Every move path
reads the source's column catalogue before and after itself - one query on
PostgreSQL and MySQL - and when it changed, the move stops short of
"complete", names what changed (`people: column name added`), and records
it in the changelog. A table the hop excludes is not watched. What is left
is item 5 of the backlog: reading DDL from the change stream, marking
verdicts taken across it as stale, and online schema-change temp tables.

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

**migkit: Ends it** - `test_rls_filtered_verdict.py`. The deep check
already caught the condition:

    deep postgres rls: DIFF migkit's source role is subject to RLS on 1
      tables

It also reports RLS tables with **zero** policies, which read as empty to
anyone who is not the owner.

What was missing is that the passes which pronounce on the data did not
know. Measured, with the same policy on both sides and **five of the
source's ten rows deleted from the target**:

    counts   postgres: OK 1 tables, rows 5==5
    data     postgres: OK 1 tables, 5 rows, checksums equal both sides

Half the table missing, and the two checks whose whole job is to say
whether the data landed both said OK. They were not wrong about what they
compared - they compared five rows to five rows. Nothing in either line
said the five was a filtered count.

They now say it:

    counts   postgres: WARN 1 tables, rows 5==5 ... - but the role migkit
      is connected as cannot read 1 of these tables in full
      (public.tenant), so those numbers are what it was allowed to see and
      not what is there

**Only a clean verdict is softened.** A `diff` found inside what the role
*could* see is real whatever is hidden behind it, and a test pins that it
stays a diff.

**Who is actually filtered was measured**, across every role shape, rather
than taken from the documentation:

| | superuser | owner | BYPASSRLS | plain role |
|---|---|---|---|---|
| RLS enabled | 6 | 6 | 6 | **3** |
| RLS enabled + `FORCE` | 6 | **3** | 6 | **3** |

An owner reads its own tables in full until they are `FORCE`d. The deep
check used to test only `rolsuper or rolbypassrls`, so it called **every
owner** filtered - a warning on a healthy database, which is how people
learn to stop reading warnings. Both passes now ask the same question in
one place, and a test asserts a role that sees everything is not nagged.

**Missing:** the other engines. The base returns None - "no claim" rather
than "nothing is filtered" - so nothing else acquires a verdict it has not
earned.

### D9. The view that exists and holds nothing

**What happens.** `pg_dump` does not dump a materialized view's contents.
It emits `CREATE MATERIALIZED VIEW ... WITH NO DATA` and a separate
`REFRESH` in the post-data section - deliberately, to guard against hidden
dependencies. Anything that drops the refresh leaves the view present and
empty: operators skip it with `pg_restore -L` because it is slow and mean
to run it later; it times out; or it fails on permissions, which is a real
enough history that PostgreSQL was changed in 2017 to restore ACLs before
refreshing matviews rather than after.

PostgreSQL is loud about the result, which helps: an unrefreshed matview is
*unscannable* rather than empty. Measured -

    select n from daily;
    ERROR:  materialized view "daily" has not been populated
    HINT:  Use the REFRESH MATERIALIZED VIEW command.

and `pg_class.relispopulated` is `f`.

**migkit: Ends it.** This was measured expecting a gap and there was not
one. `check --deep` reports it:

    deep postgres matviews: DIFF public.daily: not populated on target

and when a matview *is* populated it does not stop there - it applies the
source's own row-hash expression to both sides and compares the result, so
a matview that refreshed against stale data, and therefore has the right
row count, is still caught. Test: covered by the deep suite for matviews.

Worth noting alongside: migkit's own target-setup plan now restores
`--section=post-data` as an explicit step, which is where the `REFRESH`
lives, so the step people drop is a line in the plan rather than an
implicit side effect.

### D9b. The fix script that cannot be applied

**What happens.** migkit generates DDL to bring the target's schema up to
the source's and says *review, then apply*. One shape in it does not
survive a target that already has rows, and the two engines break in
opposite directions - measured, same statement, same data:

| | |
|---|---|
| PostgreSQL 16 | `ERROR: column "note" of relation "t" contains null values` - and nothing after it in the script runs |
| MySQL 8, `STRICT_TRANS_TABLES` | `Query OK` - every existing row now holds `''`, length 0, not null |

PostgreSQL refuses out loud. MySQL invents a value for every row that was
already there, which is the quieter failure and the worse one: the column
exists, the counts match, nothing errors, and the contents are made up.

migkit generates exactly this whenever the source has a `NOT NULL` column
with no default - a column the application fills. Verified end to end: a
source with `note text not null` and `tagged text not null default 'x'`
against a target of three rows produced both statements side by side, and
only the second could be applied. Nothing in the report said which was
which.

**migkit: Ends it.** The generated DDL is read back before it is offered,
and the tables named in it are checked against the target for rows. A
linter reading the SQL alone can only say *might*; migkit asked, so it says
*will*, and says what this particular database does about it. An engine
nobody has measured says that rather than inheriting an answer. Test:
`test_ddl_that_cannot_be_applied.py`.

### D9c. The green verdict with the column missing from it

**What happens.** The cross-engine check compares the columns both sides
have, and mentions the ones it skipped in a note at the end of the line -
on a verdict whose status is **ok**. Measured, a SQLite source with a
`secret` column the PostgreSQL target does not have:

    OK  main.items
        rows 2 and every compared column equal across sqlite/postgres
        (digest 1475545921195705015)
        columns only on the source, not compared: secret

Every word is true: every *compared* column was equal. And a migration that
dropped a whole column passed verification, because nobody reading a green
line reads the tail of it. The engine whose entire job is a pair of
different engines had `checks = ("counts", "data")` - no schema check at
all.

**migkit: Ends it.** A column on one side only, a table on one side only,
and a column whose two declared types render to different classes are each
reported as a schema difference in their own right. Comparison is by name
and by rendered class, because two engines never spell a type the same way:
`INTEGER` against `bigint` is not a finding, `INTEGER` against `text` is. A
column neither side can render is a **warn** - "we did not look" is not
"they match", and it is not a difference either, because nothing was
compared to differ from.

The row check's wording is unchanged and still says `ok`, deliberately: the
sentence was never false, and the fix is a report that carries the other
line rather than a sentence that hedges. Both read one classification,
computed once - deciding it twice is how the footnote and the verdict come
to disagree. Test: `test_hetero_schema.py`.

### D10. The role could read the table but not the column

**What happens.** Column-level grants (`GRANT SELECT (id) ON sales TO
role`) are the natural way to keep a migration tool away from columns it
has no business reading. PostgreSQL's error when it hits one is
famously misleading - measured:

    select * from sales limit 1;
    ERROR:  permission denied for table sales

It says **table** when the missing privilege is on a column, which sends
people to the wrong GRANT. There is an open patch proposing "permission
denied for column subset of table" for exactly this reason. A second trap:
a lingering table-level `SELECT` - including one granted to `PUBLIC` -
silently overrides the column grants, because the table-level check runs
first.

**migkit: Partly, and it fails in the right direction.** The data pass
reports it rather than working around it:

    public.sales: ERROR ERROR:  permission denied for table sales
    data     postgres: ERROR  errors: public.sales
    verdict: error

which is correct - an unreadable table is not a verified one.

**The narrow thing that was wrong is fixed** -
`test_counts_over_nothing.py`. In that same run the counts line read `OK 0
tables, rows 0==0`: counts is merged into the checksum pass, so when that
pass errors on every table there is nothing left to count, and the result
was a clean verdict computed over nothing. Nobody was badly misled, because
`data` errored beside it and the overall verdict was `error` - but "OK, 0
tables" is the same shape as a mover reporting success over an empty
target, which `moved_nothing` already refuses.

It now says what it actually knows:

    counts   postgres: ERROR counted 0 tables; 1 could not be read by the
      pass these counts come from: public.sales - so this is not a count of
      the database

**An empty database is still `ok`**, and keeping those two apart is the
whole difficulty: zero tables counted is only alarming when there were
tables to count. They are told apart by whether any table exists on both
sides, not by the count being zero, and both directions are pinned by
tests - along with a control proving the grant really was restrictive, so
a green suite cannot mean the permission was never enforced.

Counts run **alone** was honest all along, and that is worth stating
precisely rather than sweeping into the fix: `select count(*)` needs only
one readable column, so it answered `100 rows both sides` and that number
is true. A test pins that it still does.

### D11. The documents were never in the table

**What happens.** A PostgreSQL large object does not live in your table. The
table holds an `oid`; the bytes live in `pg_largeobject`, with no
referential integrity between the two. Copy the table and you have copied
the integer, not the document.

`pg_dump` makes this easy to trip over: it skips large objects whenever
`-s`, `-n` or `-t` is used - a schema-restricted or table-restricted dump
produces an archive whose oid columns are intact and whose blobs are
absent. There is a `-b` to put them back and no inverse.

Measured, with a table whose contents are **byte-identical** on both sides:

    table on both sides   1contract16391,2invoice16391
    source                lo_get(16391) -> 'the actual contents...'
    target                ERROR:  large object 16391 does not exist

**migkit: Partly, and the gap is in the part that pronounces.** `assess`
does name them, before anything moves:

    large objects (a managed service moving table by table leaves them;
    migkit's own pg_dump path carries them) - 1 in pg_largeobject

That is the right warning in the right place, and it distinguishes the two
legs rather than blaming the objects.

**But the verification never looks.** On the pair above:

    counts   postgres: OK 1 tables, rows 2==2
    data     postgres: OK 1 tables, 2 rows, checksums equal both sides
    verdict: same

and `deep` does not mention large objects at all. Every document in the
database is gone and migkit certifies the migration as correct - because
the column really does hold the same integer on both sides. This is the
third finding of that exact shape, after the RLS filter and counts over
zero tables, and it is the one that survives a clean report.

**Fixed** - `test_large_objects.py`. `check --deep` now compares
`pg_largeobject_metadata` on both sides and, for every `oid` column,
whether what it points at is actually there:

    deep postgres large objects: DIFF 1 columns point at large objects the
      target does not have: public.docs.body 2 of 2 rows - the rows
      arrived and what they refer to did not

A source holding objects against a target holding none gets its own
wording, because that is the `-s`/`-n`/`-t` dump and worth naming as such.

**The design risk was the opposite mistake**, and it shaped the check.
Plenty of `oid` columns hold something that is not a large object - a
`regclass`, a type oid - and an anti-join would call every one of them
broken. So a column counts as a large object reference only when the
**source** resolves it: measured, a column holding `'refs'::regclass::oid`
resolved 0 rows where a real document column resolved 1. A test keeps that
column in the fixture and asserts it is never mentioned.

**What it still cannot see is stated in the line itself**, not left
implied: a reference parked in a plain integer column is invisible to this,
because the type is what makes it findable. `vacuumlo` has the same blind
spot and a worse consequence - it *deletes* any large object not referenced
from an `oid` or `lo` column, so a migration that parked its references in
a `bigint` loses them to the cleanup rather than to the move.

### D12. The row changed while it was being compared

**What happens.** A checksum of a live table races the application writing
to it. The row hashed on the source at one moment and on the target at
another differ, and nothing is wrong. AWS DMS treats this as the main
source of false positives and answers it with time: a CDC validation task
delays re-validation per changed row, defaulting
`ValidationQueryCdcDelaySeconds` to **180**, and suspends validation
entirely once a failure threshold is breached.

**migkit: Ends it, and by a better mechanism.** `_resolve_inflight` splits
DIFF tables into real differences and in-flight replication -
**deterministically when a fence is available**, with sleep-settle only as
the fallback. A fence is an answer rather than a guess: waiting 180 seconds
and hoping is what you do when you cannot ask the source where it had got
to. The re-check is narrowed to the keys the drilldown already named rather
than rehashing the table, and it declines to do it at all beyond 20,000
keys, which is the point where a "settle" is no longer a settle.

When every difference resolves this way the report says so in those words -
`all diffs proven in-flight replication` - rather than quietly passing.

**And the number the fence is built on is the consumer's, not the
target's.** `confirmed_flush_lsn` is what the replication consumer reported
back to the source; `pg_replication_origin_status.remote_lsn` on the target
is what the target committed. They are not the same, and they disagree in
both directions - measured on PostgreSQL 16, one pair, the same insert load:

| | |
|---|---|
| native subscription | origin **ahead** of the slot by up to 46 KB - the slot is a delayed echo of the apply |
| `pgcopydb follow` | slot **ahead** of the origin while the target still held 0 rows |

The obvious conclusion - fence on the origin, since it is the one that means
"applied" - was written, measured and reverted. On an idle healthy pair, five
samples three seconds apart:

    pg_current_wal_lsn   0/19EB660
    confirmed_flush_lsn  0/19EB660   (keepalives carry it to the end)
    origin remote_lsn    0/19EB540   (288 bytes back, and staying)

The origin can only ever be the LSN of the last *applied transaction*, so it
stops while the source's WAL keeps moving. `origin >= lsn` never becomes true
on a quiet database, and `origin >= slot` never does either. A fence built on
it times out on every idle pair and falls through to sleep-settle without
saying so - a fence that quietly stops fencing. Nor is the gap a fault signal
by itself: an idle pair and a consumer sitting on unapplied changes look the
same in LSN arithmetic, which is what the row comparison is for.

So `fence_wait` keeps reading the slot, on purpose and with the measurement
written next to it, and the target's position is exposed separately as
`applied_lsn` - correctly attributed, because origins are **cluster-wide**:
connected to `postgres` with the only subscription living in `other`, the
view still lists `pg_16407 | 0/0`, and a `min()` across that would answer
`0/0` for ever. Test: `test_where_the_target_actually_is.py`.

**On MySQL too (2026-09-24).** The confirm pass lived in the PostgreSQL
engine, so every other engine called a row still arriving a difference.
It is the base's now, run by any engine that can say where its source is,
wait for the target to get there, and compare rows by key. MySQL does all
three: its position is the source's executed GTID set, and the target's
server does the waiting (`WAIT_FOR_EXECUTED_GTID_SET`, or
`MASTER_GTID_WAIT` on MariaDB), asked in short turns so a read timeout
never cuts it off. A table that converged is read again whole, so its
count and its checksum both come from after the fence. Measured on a real
MySQL 8.4 source and replica: rows held back by a stopped applier and
released while the confirm pass waited read `ok ... still arriving`; a row
changed on the target alone still reads `diff`. Test:
`test_mysql_fence.py`.

### D12b. The key that is not a key

**What happens.** A row comparison that matches by key is only as good as
the key. Two ways it stops being one, both measured on the engine migkit
reaches nine databases through:

**A row whose key is NULL is invisible.** A source of four rows, one keyed
NULL, against a target holding the other three:

    3 rows in table A
    3 rows in table B
    0 rows exclusive to table A (not present in B)
    0.00% difference score

and migkit's own checks on that same pair, all three green - counts `the
same number of rows on both sides`, data `no row is on one side only`,
schema `same names and same declared types`. **Four rows against three,
reported as complete.** A migration that dropped every NULL-keyed row
would pass verification, and the first anyone would hear of it is the
application asking for one of them.

**A repeated key makes the answer stop being repeatable.** Same pair,
unchanged, four runs of the same command - a source holding `4/y` and
`4/z` against a target holding `4/q`:

    run 1   0 exclusive A, 0 exclusive B, 1 updated, 20.00%
    run 2   0 exclusive A, 0 exclusive B, 1 updated, 20.00%
    run 3   ERROR -        (with nothing after it)
    run 4   ERROR -

The truth is two rows on the source alone and one on the target alone.
Pointing both connection strings at the *same* server makes it refuse
outright instead - `ERROR - Duplicate primary keys` - which is the safer
failure and the one no real hop ever sees, because a real hop has two
servers.

**migkit: Ends it.** A deep check asks both sides, before any of it
matters, whether the key is filled and whether it is one row per value,
and says what each answer costs. Both questions go through the portable
query builder rather than hand-written SQL, so one implementation serves
all nine engines. Test: `test_generic_key_is_a_key.py`.

### D12c. A primary key that holds NULL

**What happens.** Every engine reads its comparison key from a real
constraint - a primary key, or MongoDB's `_id` - and a primary key cannot
hold NULL. That is true of PostgreSQL and MySQL. It is **not** true of
SQLite: a non-INTEGER `PRIMARY KEY` takes NULLs unless it also says NOT
NULL. Measured:

    create table a (id text primary key, v text)
    insert into a (id, v) values (NULL, 'x')   accepted, id is null
    create table b (id integer primary key, ...)
    insert ... (NULL, 'x')                     a rowid is assigned instead
    create table c (id text primary key not null, ...)
    insert ... (NULL, 'x')                     NOT NULL constraint failed

Two things followed, both in code every engine shares. **The data check
crashed** - `"/".join(key)` on a key holding `None` raises `TypeError:
sequence item 0: expected str instance, NoneType found`, and it took the
whole check down. The row counts had already found the difference
correctly (`src=3 dst=2`); the drilldown turned a finding into a
traceback. The same line existed in two places, so fixing one left the
other.

**And the repair would have done nothing, quietly.** The key comes back
out of the drilldown as text and through `canon.from_text`, which answers
`None` for `None` - measured - and that lands in the predicate as
`where id = NULL`, never true. The row would have been reported as carried
with nothing changed.

**migkit: Ends it.** One renderer for keys, with NULL spelled out, so the
check reports rather than crashes; the verdict says those rows cannot be
addressed on the other side and will not be repaired; and the repair
refuses them outright rather than reporting a success it did not have.
Tests: `test_a_key_that_holds_null.py`.

### D12d. The value the source kept and the target will not take

**What happens.** SQLite does not enforce a column's declared type on an
ordinary table. Measured:

    create table t (id integer primary key, n integer)
    insert into t values (2, 'not a number')     accepted
    select typeof(n) from t where id = 2         text

and the same insert into a `STRICT` table:

    cannot store TEXT value in INTEGER column s.n

Every engine migkit moves to behaves like the second one. So a SQLite
source can be perfectly consistent with itself and still hold the exact
rows a move will be refused on - discovered part-way through a load, with
the target half full.

**migkit: Ends it, and with no heuristic of its own.** The thing that makes
this answerable is that **SQLite already tried**: affinity is applied on
the way in, so a numeric column converts what it can. Measured, the same
text inserted into a column of each declared type:

    declared        '5' stored as     'abc' stored as
    integer / int / bigint / numeric / decimal(10,2)
                    integer           text
    real / double   real              text
    text / varchar / blob / (no type)
                    text              text

`'5'` became a number wherever the column had numeric affinity. A value
still sitting there as `text` is one this database itself could not
convert, so counting them is a reading rather than a judgement. Both sides
are checked - on the target it means a load already carried them in. Test:
`test_sqlite_declared_types.py`.

### D13. The difference you cannot see, and the one the reader ate

**What happens.** Two values print identically and are not equal: `é` as
one code point or as `e` plus a combining accent, a trailing space, a
zero-width space, a non-breaking space, `\r\n` against `\n`. The digest is
right and the operator, looking at two identical-looking strings, concludes
the tool is wrong. Worse, the tools that render these values often *fix*
them on the way past, so the report disagrees with the digest that produced
it.

**migkit: Ends the reading half** - `test_drill_sees_every_byte.py`. This
was found in migkit itself, and it was the worse shape of the two. On a
pair whose only difference was a carriage return, one run gave two answers:

    check --only data   DIFF, pk-level file data-public.t.changed -> 5
    check --drill       Number of rows with some compared columns unequal: 0

`--drill` exists precisely to explain a DIFF, so the answer an operator
would act on was the wrong one - and "the digest was a false positive" is
the conclusion it invites. The mechanism was isolated rather than assumed:
`fetch_sample_df` ran psql with `text=True`, and Python's universal-newline
decoding rewrites the payload. The same subprocess call, twice:

    capture_output=True                  b'"one\r\ntwo"\n'
    capture_output=True, text=True        '"one\ntwo"\n'

Both sides lost the CR, so both sides matched. Reading the bytes and
decoding them without translation ends it: the same pair now reports 6 of 6
differing rows where it reported 5, and the carriage-return-only pair
reports 1 where it reported 0. The MySQL engine reads its sample through a
driver and never had this - recorded with a test, so a future rewrite to
shell out cannot reintroduce it quietly.

**And the seeing half.** `--drill` now adds what datacompy cannot work
out - the values escaped, and the reason beside them:

      id=1  v
          source  'caf\xe9'
          target  'cafe\u0301'
          the same text written with different code points (NFC vs NFD)
      id=3  v
          source  'ab'
          target  'a\u200bb'
          a zero-width character (U+200B)

Five kinds are named from one definition of "renders the same": NFC vs NFD,
leading or trailing whitespace, a carriage return (and which side has it), a
zero-width character, and a space that is not U+0020 - each reported with
its code point. The classification is by Unicode category rather than a
hand-written list of characters, and the categories were checked against
`unicodedata` rather than assumed: `Cf` for the zero-width and BOM family,
`Zs` for every space that is not U+0020, `Cc` for carriage return and tab,
`Mn` for the combining marks NFC folds away.

Two exclusions carry as much weight as the inclusions, and both are pinned
by tests. A row differing `red` from `blue` never appears: a difference
anybody can see needs no explanation, and a section repeating every
differing row would bury the ones that do. A lone carriage return between
two letters is also left out - it returns the cursor rather than printing,
so `a\rb` and `ab` really do render differently. The section prints nothing
at all when there is nothing invisible to explain.

It lives on the base contract and reads two dataframes, so MySQL gets it
without a line of its own - asserted by a test rather than assumed, because
"it should inherit" is how two copies start.

### D14. The verdict does not say how much it looked at

**What happens.** A verification run is gated on by a machine, not read by
a person - a CI job asks "did it pass" and nothing else. If the artifact it
reads cannot distinguish "everything matched" from "the part I looked at
matched", then narrowing the run is indistinguishable from passing it.

**migkit: Ends it** - `test_narrowed_run_verdict.py`. Found by running two
commands that should not be able to agree, on one pair: a database with
`good` (100 rows both sides) and `bad` (100 rows on the source, 40 on the
target).

    check sc --only data                    data  DIFF  public.bad
                                            verdict: different
    check sc --table public.good --only data data  OK missing=0 extra=0
                                            verdict: same

The second was not wrong about the table it was asked about. The artifact
was. With the timestamp, fingerprint and tool version removed, the
`verdict.json` from that run was **byte-identical** to one from a genuinely
clean database checked in full:

    {"by_category": {"parity.row-content": {"ok": 1}}, "findings": [],
     "has_differences": false, "hop": "sc", "status": "same",
     "totals": {"diff": 0, "error": 0, "ok": 1, "skip": 0, "warn": 0}}

Same `status`, same `has_differences`, same totals - with 60 rows missing
from a table the run never opened. A gate reading `has_differences == false`
passed both. `summary.json` beside it *did* carry `"scope": "postgres
public.good"`, so the information existed and stopped short of the file
automation reads.

The envelope now carries what narrowed the run, and the same command says:

    verdict: incomplete
    "coverage": {"checks": ["data"], "table": "public.good"}
    "status": "incomplete", "has_differences": false

`incomplete` is not a new word - the vocabulary already had it for "nothing
found, not everything looked at". **`has_differences` is deliberately
untouched**: no difference was found, and flipping it would be a lie in the
other direction, aimed at forcing a gate rather than informing it.

What counts as narrowing was written to avoid crying wolf, because a
verdict that comes back `incomplete` every time is one people learn to
ignore, and then they gate on `has_differences` again - the hole this was
opened to close. `--db` on a single-database hop narrows nothing. No
`--only` means the full battery ran. The hop's own exclude list is absent
on purpose: that is the hop's definition rather than a narrowing of it, so
a run covering the hop fully is complete.

Two things migkit already got right are pinned so this change could not
take them away: a mistyped table name (`--table public.gooood`) reports
`ERROR` and `verdict: error` rather than a clean nothing, and `--table`
narrows the checksum pass while `counts` still sweeps the whole database -
which is why hiding `bad` above needed `--only data` as well.

### D15. How much of a row the comparison actually compares

**What happens.** "Checksums equal" is only as strong as the set of
columns that went into the checksum. A verifier that silently leaves out
the columns it finds awkward gives the same answer as one that compared
everything.

**migkit: Ends it for the same-engine leg, and states the size of the gap
on the cross-engine one** - `test_how_much_of_a_row_is_compared.py`. That
a type with no agreed rendering is refused rather than guessed was already
the design. What had never been measured is **how big the refusal is**, and
the two paths are not the same size. One PostgreSQL table, 37 columns,
carrying what a real schema carries:

    same-engine (pg -> pg)   data: DIFF, every change caught
    cross-engine (hetero)    compared 20 of 37, 17 refused

The same-engine digest is computed over the whole row inside the server, so
nothing is outside it - measured by changing *only* the columns the other
path refuses (`interval`, `enum`, `tsvector`, `hstore`, `bit`, `point`,
`int4range`) and watching `check` still report DIFF.

The cross-engine path has to render both sides into text two different
engines agree on. For seventeen of these types no such rendering exists:

    enum, interval, hstore, tsvector, tsquery, bit, varbit,
    int4range, int4multirange, point, box, circle, lseg, polygon,
    oid, pg_lsn, txid_snapshot

Refusing is right - comparing two renderings nobody checked agree is worse
than not comparing - and every refusal is named in the result line rather
than dropped, which a test pins by asserting that no column is both
uncompared and unmentioned.

**What is worth saying plainly:** the flagship leg, any-source to
any-target, verifies fewer columns than the leg between two of the same
engine, and `enum` and `interval` are in the gap. Those are not corners of
the type system. Closing them is canonical-rendering work, one type at a
time, and the number in the test moves when it is done.

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

### E6. Replication that started and is not running

**What happens.** Native replication is set up target-side, and the
statement that starts it is not the thing that proves it works. The two
engines get this wrong in opposite directions, both measured on live
pairs whose networks genuinely could not route to each other:

* **PostgreSQL fails late.** `CREATE SUBSCRIPTION` dials the source while
  it runs, so it does fail - after **134 seconds**, per database, with
  nothing on screen until it gives up. The parameter that looks like it
  bounds that wait does not: `connect_timeout=10` inside the
  subscription's own connection string made no difference (134s again),
  while the *same* conninfo handed to plain `psql` gave up in exactly 10.
  It is enforced by libpq's synchronous connect path, and the walreceiver
  does not use that path. `statement_timeout` on the target session does
  work - measured at 15s - and nothing is created when it fires, so a
  bounded attempt costs nothing.
* **MySQL fails silently.** `START REPLICA` returns OK whether or not the
  target can reach the source; the IO thread starts, fails and retries
  behind it. Measured on 8.4: statement succeeded,
  `Replica_IO_Running: Connecting`, and the timeout sitting in
  `Last_IO_Error` where nobody was looking.

And migkit's own line under `--mode cdc --go` read that status with
`eng._psql(...)`, which only the PostgreSQL engine has - so every MySQL
cdc run ended in `AttributeError: 'MySQLEngine' object has no attribute
'_psql'`, after both sides' statements had already run and before the
changelog entry recording it was written.

**migkit: Ends it.** `replication_status` is on the engine base as a
contract every engine that emits `replicate_sql` must answer, and the
shared path asks the engine instead of reaching for a PostgreSQL method.
`apply_replication_stmt` is the second half of that contract - the engine
runs its own statements, because it is the only thing that knows which of
them reach across to the other server. PostgreSQL bounds that one with
`statement_timeout` (45s by default, `MIGKIT_SUBSCRIBE_TIMEOUT` to change
it, and a value that is not a positive whole number is refused rather than
ignored), so a target with no route to the source is told so in seconds
instead of 135 of them, per database.

The failure message says the two things that are actually true and not
obvious. First, which side cannot reach which: the statement runs on the
target and dials the source, so it is the route, not migkit and not the
credentials. Second, what survived - **"nothing was created" was the first
draft and it was false.** The publication on the source is created by the
statement before this one, PostgreSQL has no `CREATE PUBLICATION ... IF
NOT EXISTS` (checked: syntax error), and a retry therefore stops on
`publication "..." already exists` before reaching the target at all. So
the message names the publication and the `--drop --go` that clears it,
and points at `MIGKIT_CDC=follow`, which needs no route from the target.
The MySQL implementation reads `SHOW REPLICA STATUS` **by column name**
(`_q_named`, since `_q` returns bare tuples and MariaDB spells every one
of these columns differently), and says `NOT replicating` with the real
error whenever either thread is not `Yes`. Three states measured live and
pinned: unreachable source, dead applier, and a healthy replica whose rows
actually arrived - the last so the check cannot pass by always saying no.
Test: `test_replication_says_whether_it_is_running.py`.

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

**Fixed after measurement - the check cried wolf, and its own docstring
was wrong about why.** `test_planner_statistics.py` had the right story
from the start (its fixture builds the unhealable case on purpose), but
the check could not tell the two apart at runtime, so it was never
exercised on the common one. The claim in the check's docstring was that
autoanalyze "will not run until a tenth of the rows have changed again -
which, on a table that was migrated and is now only read, may be never."
The bulk load *is* those modifications. Measured on PostgreSQL 16, a
5,000-row table loaded and then left alone, nothing touching it:

    at load       reltuples=-1  ever_analyzed=0  n_mod_since_analyze=5000
                  threshold = 50 + 0.1*5000 = 550
    90s later     ever_analyzed=1  last_autoanalyze=10:21:45  n_mod=0

Autoanalyze ran on its own one naptime after the load, because 5000 is
already far past the threshold. It does not wait for a second round of
changes. Any migrated table above about 55 rows is in the same position.

What that costs today: on a pair where **every parity check passes** -
schema identical by five separate differs, counts equal, checksums equal -
the run reports

    deep postgres statistics: DIFF 1 tables the planner has no statistics for
    verdict: different

for about sixty seconds after a load, and then reports `OK` with nothing
changed but time. `diff` everywhere else in this tool means *the two sides
do not match*; here it means *the target will be slow*, which sends the
reader hunting for missing rows on a migration that is byte-perfect.

**The check still has a real job**, which is why the answer is not to
delete it. The same table on a target started with `autovacuum=off`:

    75s later     ever_analyzed=0  n_mod=5000  reltuples=-1

Never analyzed, and never will be. That - along with a per-table
`autovacuum_enabled = false`, which `migkit move --go` already handles - is
the case worth a finding.

**Fixed.** The check now asks the target which tables autovacuum will
reach - the global `autovacuum` setting, and each table's own
`autovacuum_enabled` reloption, cast by the server rather than matched
against a guessed list of spellings, because a boolean reloption is stored
as whatever was written (`false`, `off`, `0` and `no` all occur, and the
first attempt here compared against `off` and misread a table written
`false`). A table it will reach is `ok` and named in the line - a wait, not
a fault. A table it will not is the finding it always was:

    DIFF 1 tables the planner has no statistics for and autovacuum will not
    reach: public.frozen ...; 1 more are not analyzed yet and autovacuum
    will reach them without being asked

An engine that cannot ask passes nothing and keeps the old behaviour, since
"I could not tell" is not "there is nothing there". Side effect worth
naming: `test_deep_float_pg.py::test_identical_floats_pass` had been red
since the check landed, and went green without being edited.

**Still open, as a question rather than a defect:** whether a
target-performance finding should be able to spell itself `diff` at all,
when every other `diff` in this tool means the two sides do not match.

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

**One thing worth knowing about the native replication path**, measured
rather than assumed. `CREATE SUBSCRIPTION` carries the source's connection
string, and the target keeps it:

    select subconninfo from pg_subscription
    host=10.0.0.5 port=5432 dbname=postgres user=postgres password=CHANGE_ME

Stated at its real size: a plain login role on the target got `permission
denied for table pg_subscription`, so this is a superuser-on-the-target
exposure, not a public one. It is still a reason to prefer a path that
runs from the operator's machine when the target belongs to someone else -
and **that path now exists**: `MIGKIT_CDC=follow` on `move --mode cdc`
drives `pgcopydb follow` from where migkit runs, connecting out to both
sides, so no credential is written to the target and the target never has
to dial the source. Verified end to end against a pair on two docker
networks with no route between them - the case a subscription cannot do at
all. `test_cdc_driven_from_here.py`.

migkit masks the password in the plan it prints, and **that masking had a
bug**: `stmt.replace(password, "****")` with an empty password inserts the
mask between every character, so a hop authenticating by `trust`, `.pgpass`
or a client certificate - the arrangements that keep a password out of the
config in the first place - got back
`****c****r****e****a****t****e****` instead of the statement it was about
to run. Fixed, with the guard in one shared helper:
`test_masking_an_empty_password.py`.

**Missing:** masking of *data*. If an operator needs a drilldown that is
safe to paste into a ticket, migkit does not yet offer one.

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

Views and column grants:
[REFRESH MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html),
[preventing the refresh during pg_restore](https://www.postgresql.org/message-id/1403794157042-5809367.post@n5.nabble.com),
[pg_dump emitting REFRESH after ACLs](https://www.postgresql.org/message-id/E1ddNne-0001jw-Vx@gemulon.postgresql.org),
[column-level security in PostgreSQL](https://www.enterprisedb.com/postgres-tutorials/how-implement-column-and-row-level-security-postgresql),
[reporting a column-level error when lacking privilege](https://www.postgresql.org/message-id/CAKFQuwaiP%2BkYLCtUh_5Hdd7XKUHHH_Y5JAvb-0x2JQevJevVeA%40mail.gmail.com).

Large objects and live rows:
[pg_largeobject](https://www.postgresql.org/docs/current/catalog-pg-largeobject.html),
[excluding large objects from pg_dump](https://postgrespro.com/list/thread-id/1557645),
[vacuumlo](https://www.postgresql.org/docs/current/vacuumlo.html),
[the lo module](https://www.postgresql.org/docs/current/lo.html),
[AWS DMS data validation](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html).

Cutover, aftermath and compliance:
[zero-downtime patterns](https://launchdarkly.com/blog/3-best-practices-for-zero-downtime-database-migrations/),
[post-upgrade statistics](https://techcommunity.microsoft.com/blog/azuredbsupport/azure-postgresql-lesson-learned-8-post-upgrade-performance-surprises-the-one-ste/4471807),
[migration compliance checklist](https://syncopio.com/blog/data-migration-compliance-checklist/).
