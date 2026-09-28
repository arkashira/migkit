# Type fidelity across engines: carrying and rendering every value exactly

> **Status (2026-09-28): DONE.** Sections 0-12: what migkit has (0), traps and
> canonical renderings per type family (1-8), engine-specific types (9), how
> DMS/Debezium/DVT/data-diff/pgloader/mongosync handle them (10), the type x
> pair table (11), and 29 gaps plus 8 new classes with file:line, effort and a
> docker recipe each (12). Nothing was run against a database; every "today"
> is read from the code and every recipe is there to measure it.
>
> **Headline:** four gaps can report wrong data as equal - G1 (PostgreSQL
> `infinity` renders as NULL), G2 (BC years render as AD), G3 (digits past
> microseconds cut on both sides at read), G4 (`extra_float_digits` not
> pinned) - and one silently merges rows on the way in (G8, keys that collide
> under a case/accent/pad-insensitive target collation).

Scope: carry every value between any two engines without silent change, and
render it identically on both sides so a cross-engine digest is exact. Public
sources only. Nothing here was run against a database; every "measure" item is
a docker recipe for later.

---

## 0. What migkit has today (read from the tree, 2026-09-28)

**Neutral classes** (`migkit/canon.py:66`): `integer, decimal, float, boolean,
text, bytes, date, timestamp, time, json, own text`. `OWN` (`canon.py:74`) is
same-engine-only (both sides one engine and one declared type).

**Type maps** (`canon.py:162-440`) for mysql, postgres, sqlite, mssql, parquet,
clickhouse, dynamodb, oracle, db2, duckdb, ase, redshift, snowflake, bigquery,
opensearch, cassandra, mongodb. `type_class` (`canon.py:449`) strips `(`, `[`
and ` unsigned`, and for MongoDB drops `null|missing` from the set of observed
BSON types; more than one real type left -> unmapped.

**SQL renderers** exist only for mysql, postgres (and sqlite via a registered
function) - `BUILDERS` at `canon.py:556`. Every other engine renders in-process
through `render_value` (`canon.py:622`), listed in `IN_PROCESS` (`canon.py:1275`).

| class | MySQL SQL (`_mysql`, `canon.py:476`) | PostgreSQL SQL (`_postgres`, `canon.py:503`) | in-process (`render_value`) |
|---|---|---|---|
| integer | `cast((c + 0) as char)` | `c::text` | `str(int(v))`, BIT bytes -> big-endian int |
| decimal | `cast(c as char)` | `c::text` | `format(Decimal, "f")`, Decimal128 via `to_decimal()` |
| float | banded: `decimal(65,20)` inside `[1e-20, 1e45)`, own shortest text outside | same band, `round((c::text)::numeric, 20)`; Inf/NaN -> `UNCOMPARABLE` | `_float_text` (`canon.py:559`), `-0.0` -> `0.0`, Inf/NaN -> `UNCOMPARABLE` |
| boolean | `cast(c as char)` (tinyint 0/1) | `c::int::text` | `"1"/"0"` |
| text | `cast(c as char)` | `c::text` | `str` |
| bytes | `hex(c)` | `upper(encode(c,'hex'))` | `.hex().upper()` |
| timestamp | `date_format(c,'%Y-%m-%d %H:%i:%s.%f')` | `to_char(c,'YYYY-MM-DD HH24:MI:SS.US')` | aware -> UTC, `strftime(...%f)` |
| date | `cast(c as char)` | `c::text` (DateStyle=ISO pinned in `_psql`, `postgres.py:159`) | `strftime("%Y-%m-%d")` |
| time | `time_format(c,'%H:%i:%s.%f')` | `to_char(c,'HH24:MI:SS.US')` | `strftime("%H:%M:%S.%f")` |
| json | `cast(c as char)` | `c::jsonb::text` | `json_text` (`canon.py:583`): keys by (byte length, bytes), last duplicate wins, Decimal numbers |

**Row text and digest**: length-prefixed injective encoding (`migkit/rowtext.py`),
`sum` of 60-bit md5 prefixes (`canon.py:1303-1328`), Python fold in
`fold_rows`/`digest_step` (`canon.py:1048-1071`).

**Writes**: `sql_value` (`canon.py:989`) serialises dict/list JSON with
`sort_keys=True, default=str` and turns memoryview into bytes; `from_text`
(`canon.py:1014`) turns decoder text back into values (`\x..` bytea).
MongoDB `_bind` maps `Decimal` -> `Decimal128` (`engines/mongodb.py`).
DynamoDB `_to_attr` writes integer/decimal as `N` via `str(Decimal(v))`, float
as `repr(float)` (`engines/dynamodb.py:205-231`), and reads decimals back
quantized to the scale kept in table tags.

**Absent vs NULL**: `_Absent`/`ABSENT` (`canon.py:135-159`); flattened and
counted by `_flatten_absent`/`_flatten_changes` (`engines/hetero.py:1666-1706`)
when the target cannot express absence.

**Already guarded before a move**
* MySQL zero dates into a target without them: counted per column, move
  refused as `diff` (`engines/hetero.py:625`).
* MySQL target writes under `STRICT_ALL_TABLES` (`engines/mysql.py:23`), so an
  overflow or over-long string errors instead of being cut.
* Capacity narrowing for chars/ints/numeric on MySQL and PostgreSQL
  (`canon.py:1144-1256`).
* Instant vs wall-clock meaning of temporal columns (`canon.py:1106-1138`).
* Enums as labels, domains as their base type (`engines/postgres.py:220`).
* Oracle `CHAR`/Db2 `CHAR` unpadded, Oracle `NUMBER(p,s)`/Db2 `DECIMAL`
  re-scaled on read (`engines/dbapi.py:373`).
* Cassandra `timestamp` is milliseconds; the check says so (`engines/cassandra.py:11`).
* Cross-engine columns with no rendering are named, never silently dropped
  (D15 in `docs/problems-and-what-ends-them.md:2336`).
* MySQL mojibake repair (`_mojibake_updates`, backlog 20).

**Documented open items**: `interval` has no cross-engine counterpart
(`docs/backlog.md:2216`); second reader still to be measured on `json`,
`time`, `bit`, spatial (`docs/backlog.md:784`).

---

## 1. Integers (unsigned 64-bit, bit, year, bool vs tinyint)

**Traps**
* **Unsigned 64-bit.** MySQL `BIGINT UNSIGNED`, ClickHouse `UInt64`, DuckDB
  `UBIGINT` reach 2^64-1; PostgreSQL, SQL Server, Oracle `NUMBER(19)`, MongoDB
  `long`, Cassandra `bigint`, ClickHouse `Int64` stop at 2^63-1. Wider still:
  ClickHouse `Int128/UInt128/Int256/UInt256`, DuckDB `HUGEINT`, Cassandra
  `varint` (unbounded).
* **Debezium** `bigint.unsigned.handling.mode=long` is the default and
  "may not offer the precision" - a Java `long` wraps values >= 2^63 to
  negative; only `precise` (BigDecimal) is safe
  ([Confluent config ref](https://docs.confluent.io/kafka-connectors/debezium-mysql-source/current/mysql_source_connector_config.html)).
* **AWS DMS** maps `UNSIGNED BIGINT` to `UINT8`; its release notes record a fix
  for unsigned bigint "did not replicate correctly" and one for validation
  false negatives on it ([DMS release notes](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html)).
* **pgloader** casts unsigned bigint to `numeric` and `tinyint(1)` to
  `boolean` "using tinyint-to-boolean" - a MySQL `tinyint(1)` holding 2 or -1
  becomes `true` ([pgloader MySQL cast rules](https://pgloader.readthedocs.io/en/latest/ref/mysql.html)).
* **BIT.** MySQL `BIT(1)` is used as a boolean; `BIT(n>1)` is a bit string
  whose width matters. DMS maps `BIT` to `BOOLEAN` and `BIT(64)` to
  `BYTES(8)` ([DMS MySQL source types](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html));
  pgloader maps `bit(1)` to boolean and other bits to a PG `bit` via a
  hex bit string. PostgreSQL `bit(8) '00000001'` and `varbit '1'` are
  different values with the same integer.
* **YEAR.** MySQL `YEAR` holds 1901-2155 and `0000`; two-digit input maps
  00-69 to 2000-2069 and 70-99 to 1970-1999 (Debezium `enable.time.adjuster`).
  DMS carries it as `INT2`.
* **Boolean.** Only PostgreSQL, SQL Server (`bit`), ClickHouse/DuckDB/
  Snowflake/BigQuery, Cassandra, MongoDB and DynamoDB have one. DMS writes a
  PostgreSQL boolean into Redshift as `varchar(5)` unless `MapBooleanAsBoolean`
  is set ([DMS PG source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html)).
  SQL Server `bit` stores any non-zero as 1.
* **Beyond 2^53.** Any hop through a JSON consumer that reads numbers as IEEE
  doubles (JavaScript, `mongoexport` relaxed mode read by a JS tool, some
  Kafka JSON converters) rounds integers above 2^53.
* **MongoDB int32 vs int64.** Drivers pick `int` or `long` by magnitude, so one
  field routinely holds both.

**Canonical rendering**: base-10, no sign for non-negative, no leading zeros,
no separators: Python `str(int(v))`. BIT as the unsigned big-endian integer of
its bytes (what migkit does, `canon.py:632`), and BIT width carried
separately as a schema fact, not in the value. Boolean as `0`/`1` (what migkit
does) so that `tinyint(1)` and `boolean` meet.

**What migkit must refuse or warn**
* Refuse creating a target integer column narrower than the source range:
  `DDL[*]["integer"]` is `bigint`/`Int64`/`NUMBER(19)` for every source
  (`canon.py:709,722,739,756,767,778,793,806,818,831,844,857,869`), so a
  MySQL `bigint unsigned`, ClickHouse `UInt64`, Cassandra `varint` or DuckDB
  `hugeint` value above 2^63-1 stops the load part-way (PostgreSQL, SQL
  Server) or raises in pymongo. Pick `numeric(20,0)` / `decimal(20,0)` /
  `NUMBER(20)` / `Decimal128` / `UInt64` from the source range instead.
* Warn when a MongoDB field holds `int` and `long` together - today it is
  unmapped (`type_class`, `canon.py:469`) and silently not compared; the two
  are one integer class.
* Warn when a `tinyint(1)` holds values outside {0,1} before it meets a real
  boolean (count `where c not in (0,1)`).

## 2. Decimals (precision/scale, trailing zeros, NaN/Infinity, NUMBER without scale)

**Traps**
* **Unconstrained PostgreSQL `numeric`** keeps a per-value display scale
  (`1.5` and `1.50` print differently, compare equal), allows up to 131072
  integer and 16383 fraction digits, and holds `NaN` and (PG 14+)
  `Infinity`/`-Infinity`.
* **AWS DMS** moves unconstrained numeric as `NUMERIC(28,6)`: its own example
  turns `0.611111104488373` into `0.611111`. `MapUnboundedNumericAsString`
  fixes it only PG-to-PG, and "restricts precision to 28 during CDC"
  ([DMS PG source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html)).
  DMS marks `NUMERIC(p,s)` as only "partially migrates".
* **Debezium** `decimal.handling.mode=precise` (default) uses
  `VariableScaleDecimal` for unconstrained numeric; `NaN` is only carried in
  `double` or `string` mode, and `double` "might result in a loss of
  precision" ([Debezium PG connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)).
  Debezium 2.7 fixed zero-scale numerics being ignored by the mode
  ([2.7 release](https://debezium.io/blog/2024/07/01/debezium-2-7-final-released/)).
* **MySQL rounds excess fraction digits even in strict mode**: "Truncation due
  to rounding of the fractional part is not an error, even in strict mode" -
  Note 1265 only ([MySQL expression handling](https://dev.mysql.com/doc/refman/8.4/en/precision-math-expressions.html),
  [Bug #87678](https://bugs.mysql.com/bug.php?id=87678)). So migkit's
  `STRICT_ALL_TABLES` (`engines/mysql.py:23`) does **not** protect scale.
  MySQL `DECIMAL` tops out at 65 digits, 30 after the point.
* **Oracle `NUMBER`** with no precision is a 38-digit floating decimal and keeps
  no trailing zeros (`12.50` reads `12.5`); `NUMBER(p,-s)` rounds to tens or
  hundreds on insert.
* **SQL Server / Snowflake / BigQuery `NUMERIC`** cap at 38 digits; BigQuery
  `NUMERIC` is fixed at scale 9, `BIGNUMERIC` at 38.
* **DynamoDB `N`**: 38 significant digits, magnitude 1E-130 to 9.9..9E+125, no
  NaN/Infinity, no scale kept.
* **MongoDB `Decimal128`**: 34 significant digits, keeps scale, holds NaN and
  +/-Infinity.
* **PostgreSQL `money`** prints locale text (`$1,234.56`) and depends on
  `lc_monetary`; migkit maps it to `decimal` (`canon.py:183`) and renders
  `c::text` (`canon.py:541`) - the currency symbol and separators go into the
  digest and the driver returns a string.
* **Validators**: DVT's row hash failed on `1.5000000000000` (PostgreSQL) vs
  `1.5` (Oracle) ([DVT #620](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/620))
  and `.5000` vs `0.5` Teradata/BigQuery ([DVT #1372](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1372));
  fixed with `trim_scale()`, which needs PostgreSQL 13+. data-diff casts
  numbers to `decimal(38, p)` per column, which overflows for integer parts
  over 38-p digits ([data-diff postgresql.py](https://github.com/datafold/data-diff/blob/master/data_diff/databases/postgresql.py)).

**Canonical rendering**
* Both sides declare a scale and it is the same: fixed point at that scale,
  `-` only when non-zero, a `0` before the point, never an exponent
  (`format(q, "f")`, what migkit does).
* Either side has no fixed scale (PG `numeric`, Oracle `NUMBER`, DynamoDB `N`,
  Decimal128 against a fixed column, Cassandra `decimal`, JSON numbers): the
  **value-normal form** - `Decimal.normalize()` then `format(..., "f")`, `-0`
  written `0`. In SQL: PostgreSQL `trim_scale(c)::text` (13+), MySQL
  `trim(trailing '.' from trim(trailing '0' from cast(c as char)))` guarded for
  integers. Today migkit renders the display scale on both, so PG `numeric`
  `1.5` against the `decimal(65,10)` migkit itself builds (`canon.py:723`)
  reads `1.5` vs `1.5000000000` - a difference in every row (to measure).
* `NaN`, `Infinity`, `-Infinity`: `UNCOMPARABLE` unless both engines hold
  them (PG numeric, Decimal128); today a PG numeric `NaN` renders as `NaN` and
  the MySQL target rejects it at write.
* `money`: render `c::numeric::text`, read `c::numeric`.

**What migkit must refuse or warn**
* Before creating MySQL `decimal(65,10)` for unconstrained numeric: scan
  `max(scale(c))` and `max(length(trunc(abs(c))::text))` on the source and
  refuse when either exceeds the target (30 / 55), because MySQL rounds the
  fraction silently.
* Before any move into a fixed-scale target: count rows where
  `c <> round(c, target_scale)` (PostgreSQL, Oracle, Mongo `$round`).
* Count `NaN`/`Infinity` in numeric columns when the target cannot hold them.
* Into DynamoDB: count values with more than 38 significant digits or outside
  its magnitude range; into Decimal128: more than 34.

## 3. Floats (float4 vs float8, shortest repr, -0.0, NaN, denormals, Mongo double)

**Traps**
* **float4 widened to float8** exposes the binary value: `0.1::real::float8` is
  `0.10000000149011612`. pgloader's default `float to float` widens MySQL
  `FLOAT` to `double precision` ([pgloader #746](https://github.com/dimitri/pgloader/issues/746)).
  MySQL prints `FLOAT` with 6 significant digits (`FLT_DIG`), hiding it.
* **Shortest round-trip text** is PostgreSQL 12+ default
  (`extra_float_digits=1`); before 12, or where a database sets it to 0,
  `float8::text` gives 15 digits and is lossy. migkit's `_psql` pins
  `TimeZone` and `DateStyle` but not `extra_float_digits` (`engines/postgres.py:159`).
* **DMS** supports `FLOAT` only in `-1.79E+308..-2.23E-308, 0, 2.23E-308..1.79E+308`
  - denormals are out of range and must be mapped to STRING
  ([DMS MySQL source types](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html)).
  DMS: "the migrated value of a FLOAT might not match exactly".
* **NaN/Infinity** are held by PostgreSQL, Oracle `BINARY_DOUBLE`,
  MongoDB and ClickHouse, and rejected by SQL Server, DynamoDB and MySQL. PostgreSQL defines `NaN = NaN` true (migkit notes this at
  `canon.py:510`).
* **-0.0**: PostgreSQL and MySQL keep the sign bit; numeric casts drop it.
* **Validators**: data-diff converts binary precision to decimal digits
  (`floor(log10(2**p))`, minus 2 on PostgreSQL) and casts to
  `decimal(38, 13)` for a double - any difference past the 13th decimal place
  is invisible and anything above ~1e25 overflows
  ([data-diff base.py](https://github.com/datafold/data-diff/blob/master/data_diff/databases/base.py)).
  DVT had to stop scientific notation for Db2 REAL/DOUBLE (#1723).
* **MongoDB double vs int64 vs Decimal128**: `mongoexport` relaxed mode drops the
  numeric subtype ([Extended JSON v2](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)).

**Canonical rendering** (migkit's banded rule, `canon.py:559`, is sound for
float8): fixed 20-place decimal of the shortest round-trip text inside
`[1e-20, 1e45)`, shortest text with `e+` -> `e` outside, `-0.0` -> `0`,
NaN/Inf -> `UNCOMPARABLE`. Two additions:
* **float4**: widen to float8 first on every side, then apply the float8 rule.
  In-process that is already what drivers hand back (a Python `float` of the
  float32). PostgreSQL's renderer uses `c::text` of the `real`, which is the
  shortest *float32* text (`0.1`) and so disagrees with MySQL's
  `cast(float as decimal(65,20))` and with the in-process value
  (`0.10000000149011612`). Fix: `c::float8` inside `_postgres` for `real`.
* **float vs decimal pairs** (a MySQL `double` moved into a PG `numeric`):
  render both through the float rule only when the target class is float;
  otherwise decimal-vs-float must be an explicit narrowing warning, not a
  rendering.

**What migkit must refuse or warn**
* Count NaN/+-Inf before a move into MySQL, SQL Server, DynamoDB, Db2.
* Count denormals (`abs(c) < 2.2250738585072014e-308 and c <> 0`) into
  targets that flush them (to measure on MySQL).
* Warn on float4 -> float8 carries (the digits the application sees change);
  refuse float8 -> float4 unless every value round-trips (`c::real::float8 = c`).
* Pin `extra_float_digits=1` (PG 12+) or refuse the PG float rendering on 11
  and older.

## 4. Strings (collation, padding, normalization, utf8mb3, NUL, mojibake)

**Traps**
* **Collation collapses keys.** MySQL 8's default `utf8mb4_0900_ai_ci` is
  case- and accent-insensitive ("treats 'a', 'å' and 'ä' as equal"); SQL
  Server's default `SQL_Latin1_General_CP1_CI_AS` is case-insensitive;
  PostgreSQL (deterministic collations), MongoDB (binary by default), Oracle
  (binary), DynamoDB are exact. Two keys distinct on the source become one on
  the target. With migkit's MySQL writer (`insert ... on duplicate key
  update`, `engines/mysql.py:284`) the second row **overwrites** the first:
  no error, one row short.
* **PAD SPACE vs NO PAD.** MySQL 8 `_0900_` collations are NO PAD; older ones
  (`utf8mb4_general_ci`, `utf8mb4_unicode_ci`, `utf8mb4_bin`) are PAD SPACE, so
  `'a'` and `'a '` are one key there
  ([MySQL 8.0.1 notes](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-1.html),
  [DDEV #8242](https://github.com/ddev/ddev/issues/8242)). SQL Server compares
  with ANSI padding (trailing spaces ignored in `=`). PostgreSQL `char(n)`
  strips trailing spaces on `::text`; SQL Server and Oracle `CHAR` come back
  padded.
* **utf8mb3.** MySQL `utf8` is `utf8mb3`: a 4-byte character errors in strict
  mode and truncates the string at that byte otherwise
  ([MySQL utf8mb3 -> utf8mb4](https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-conversion.html)).
* **Byte vs character limits.** MySQL `TEXT` family is bytes (migkit already
  models this, `canon.py:1179`); SQL Server `varchar(n)` is bytes in its code
  page, `nvarchar(n)` is UTF-16 code units (a supplementary character takes 2);
  Oracle `VARCHAR2(n BYTE)` vs `CHAR`; DMS caps PostgreSQL unbounded `varchar`
  as partial and truncates text to `varchar(8000)` on some targets
  ([re:Post](https://repost.aws/questions/QUTOZQXqgqSmO5hLryyGN6_Q/dms-postgresql-truncating-varchar-to-8000-characters)).
* **Unicode normalization.** NFC `é` and NFD `e+U+0301` are different bytes;
  UCA collations (`_0900_ai_ci`, ICU) compare them equal, so they also collide
  as keys. migkit's `--drill` already names NFC vs NFD (D13).
* **NUL (U+0000).** PostgreSQL text rejects it; MySQL, SQL Server, MongoDB,
  Oracle accept it. pgloader strips it by default "using remove-null-characters"
  - a silent change ([pgloader MySQL](https://pgloader.readthedocs.io/en/latest/ref/mysql.html)).
* **Empty string.** Oracle stores `''` as NULL (migkit reports this as a
  difference, `engines/oracle.py:12`). DynamoDB rejects empty strings in key
  attributes.
* **Single-byte server encodings.** Debezium supports "UTF-8 character
  encoding only" for PostgreSQL. migkit's PostgreSQL digest hashes
  `md5(text)` - the bytes in the *database* encoding - so a `LATIN1`/`WIN1252`
  database digests `é` as one byte where MySQL and Python digest two.
* **Mojibake** (latin1 bytes decoded as utf8 twice): migkit already detects and
  repairs it on MySQL and PostgreSQL (backlog 20).
* **Lone surrogates**: SQL Server `nvarchar` can hold unpaired UTF-16
  surrogates; Python `str.encode()` in `digest_step` (`canon.py:1070`) raises
  on them.
* **Validators**: DVT RSTRIPs every value before hashing (trailing-space
  differences are invisible to it) and needed separate fixes for Latin and
  Unicode strings and `length(bpchar)`
  ([DVT README](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/README.md)).

**Canonical rendering**: the exact code points, NFC **not** applied (a
normalization changes the value; migkit is right to hash as-is), UTF-8 bytes
into the hash on every side, fixed-length `CHAR` unpadded on every engine that
pads (PostgreSQL does, Oracle/Db2 do in migkit; SQL Server and ASE do not yet).
PostgreSQL: `md5(convert_to(row, 'UTF8'))` rather than `md5(row)`.

**What migkit must refuse or warn**
* **Key collision pre-check** (the one silent row loss here): when the target
  key column's collation is case-, accent- or pad-insensitive and the
  source's is not, count source keys that collide under the target's
  equality - `group by lower(k)` / `rtrim(k)` / `unaccent` on the source, or
  better, load the distinct keys into a temp table on the target and count
  duplicates there. Refuse the move if any.
* Count 4-byte characters (`c ~ '[\U00010000-\U0010FFFF]'`) when the target
  column is utf8mb3/latin1/UCS-2.
* Count NUL-bearing values (`instr(c, char(0)) > 0` on MySQL, `$regex: "\u0000"`
  on MongoDB) when the target is PostgreSQL; never strip them.
* Count values whose byte length exceeds a byte-limited target.

## 5. Binary and UUID (bytea, BLOB, BSON Binary subtypes, BINARY(16) uuids)

**Traps**
* **PostgreSQL `bytea` has two text forms**: `hex` (`\x00ff41`, default since
  9.0) and `escape` (`\000\377A`), chosen by `bytea_output`. Anything that
  parses the *text* of a bytea must handle both. migkit's `from_text`
  (`canon.py:1040-1044`) only knows `\x`; anything else is taken as
  `raw.encode()` - so a `test_decoding` stream read under
  `bytea_output=escape` would write the escape text's bytes, not the value
  (`pgslot.value`, `migkit/pgslot.py:145`). migkit lists `bytea_output` as a
  critical parameter (`engines/postgres.py:1926`) but does not pin it on the
  decoding session.
* **Driver objects**: psycopg2 returns `memoryview`; pymysql had no escape for
  it and wrote `<memory at 0x...>` (migkit found and fixed this,
  `canon.py:1004-1010`).
* **BSON Binary subtypes** (0 generic, 3 legacy UUID, 4 UUID, 5 MD5,
  6 encrypted, 7 compressed column, 8 sensitive, 9 vector). Subtype 3's byte
  order depends on the writing driver: `JAVA_LEGACY` reverses each 8-byte
  half, `CSHARP_LEGACY` reverses the first 4-2-2 bytes; reading with the wrong
  representation yields "an entirely different UUID"
  ([PyMongo UUID](https://pymongo.readthedocs.io/en/4.11/examples/uuid.html)).
  PyMongo 4 defaults to `UNSPECIFIED`. migkit maps `bindata` to `bytes`
  (`canon.py:437`) and renders the payload only, so subtype 4 -> 0 changes
  type while the digest stays equal.
* **UUID text vs binary**: PostgreSQL `uuid` and Cassandra `uuid/timeuuid`
  render lowercase `8-4-4-4-12`; SQL Server `uniqueidentifier` comes back
  **uppercase** as text through pyodbc (data-diff had to fix exactly this,
  [data-diff #806](https://github.com/datafold/data-diff/pull/806); migkit
  reads through pymssql, which returns `uuid.UUID`, lowercase) and is
  stored with its first three groups little-endian, so its `binary(16)` cast
  is not RFC byte order. MySQL has no uuid type: `CHAR(36)` or `BINARY(16)`,
  and `UUID_TO_BIN(u, 1)` swaps the time-low and time-high groups for index
  locality - the bytes are not the RFC bytes either.
* **DMS** maps MySQL `BINARY` to `BYTES(1)`, `BLOB` to `BYTES(65535)` and
  `LONGBLOB` to `BLOB`; with LOB support off, `MEDIUMBLOB`/`LONGBLOB` are not
  migrated at all ([DMS MySQL source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html)).
  JSON on PostgreSQL is a LOB to DMS and is truncated at the limited-LOB size.
* **ClickHouse `String`** is bytes; migkit already reads it as bytes against a
  bytes column (`TEXT_HOLDS_BYTES`, `engines/clickhouse.py:33`).

**Canonical rendering**: uppercase hex of the exact bytes (migkit's rule) for
`bytes`. UUID gets a class of its own: lowercase RFC 4122 text
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` on every side, converted from each
engine's physical form by its declared representation (SQL Server
uniqueidentifier text lowercased; MySQL `BINARY(16)` by a per-column
`uuid_swap` flag; BSON subtype 4 standard, subtype 3 only with an explicit
legacy representation in the hop). BSON subtype is a schema fact carried
beside the value (never dropped silently).

**What migkit must refuse or warn**
* Refuse BSON subtype 3 without a declared legacy representation.
* Warn when a MongoDB `bindata` field with subtype != 0 is written anywhere
  that keeps only bytes, and restore the subtype when it comes back to MongoDB.
* Pin `bytea_output=hex` on every PostgreSQL session that reads values as text
  (the decoding session in particular), or parse both forms.

## 6. Date and time

**Ranges and precision (the source of most silent change)**

| engine / type | range | fraction | zone |
|---|---|---|---|
| PostgreSQL `timestamp`/`timestamptz` | 4713 BC - 294276 AD, `+/-infinity` | 6 | tz: instant stored UTC, offset not kept |
| PostgreSQL `date` | 4713 BC - 5874897 AD, `+/-infinity` | - | - |
| PostgreSQL `time` | `00:00:00` - `24:00:00` | 6 | `timetz` keeps an offset |
| MySQL `DATETIME` | 1000-9999 (+ zero dates) | 0-6 | wall clock |
| MySQL `TIMESTAMP` | 1970-01-01 00:00:01 - 2038-01-19 03:14:07 UTC | 0-6 | converted from session tz |
| MySQL `TIME` | -838:59:59 - 838:59:59 | 0-6 | an interval really |
| SQL Server `datetime` | 1753-9999 | 1/300 s (`.000/.003/.007`) | - |
| SQL Server `datetime2` / `datetimeoffset` | 0001-9999 | 7 (100 ns) | offset kept in `datetimeoffset` |
| SQL Server `smalldatetime` | 1900-2079 | minutes | - |
| Oracle `DATE` | 4712 BC - 9999 | seconds, carries a time | - |
| Oracle `TIMESTAMP(p)` / `WITH [LOCAL] TIME ZONE` | 4712 BC - 9999 | 0-9 | region or offset kept / session tz |
| MongoDB `date` | +/-292 million years (int64 ms) | 3 | UTC instant |
| Cassandra `timestamp` / `date` / `time` / `duration` | int64 ms / 2^32 days / ns of day / months+days+ns | 3 / - / 9 | UTC |
| ClickHouse `DateTime` / `DateTime64(p)` / `Date` / `Date32` | 1970-2106 / 1900-2299 on older releases, current docs 0000-9999 for p<=7 and 1677-2262 for p=9 / 1970-2149 / 1900-2299 | 0 / 0-9 | zone is **column metadata**, not per value ([ClickHouse DateTime64](https://clickhouse.com/docs/sql-reference/data-types/datetime64)) |
| BigQuery `TIMESTAMP`/`DATETIME` | 0001-9999 | 6 | instant / wall |
| Snowflake `TIMESTAMP_NTZ/LTZ/TZ` | | 0-9 | wall / session / offset kept |
| Python `datetime` (every in-process path) | 0001-9999 | **6** | tzinfo optional |

**Traps**
* **Anything finer than microseconds is cut in Python.** Every migkit
  in-process engine reads through Python `datetime`: SQL Server
  `datetime2(7)`, Oracle `TIMESTAMP(9)`, ClickHouse `DateTime64(9)`,
  Snowflake `TIMESTAMP(9)`, Db2 `TIMESTAMP(12)`, DuckDB `TIMESTAMP_NS`
  lose their last digits at read - on *both* sides, so the digest agrees
  while the move truncated
  ([pyodbc datetime2 thread](https://groups.google.com/g/sqlalchemy/c/JrDiDE2ZCQ8/m/NDG8vPTEEAAJ)).
  `type_class` strips `(7)`/`(9)` (`canon.py:459`), so nothing notices.
* **Rounding vs truncating on narrower targets.** MySQL rounds excess
  fraction digits on insert with no warning (can roll over to the next
  day/year) unless `TIME_TRUNCATE_FRACTIONAL`; PostgreSQL rounds; SQL Server
  `datetime` rounds to 1/300 s; ClickHouse and Cassandra truncate. data-diff
  models this per engine (`ROUNDS_ON_PREC_LOSS`,
  [driver guide](https://github.com/datafold/data-diff/blob/master/docs/new-database-driver-guide.rst)).
* **SQL Server `datetime` -> `datetime2`** conversion changed at compatibility
  level 130: `.003` is now `.0033333` ([MS breaking changes 2016](https://learn.microsoft.com/en-us/sql/database-engine/breaking-changes-to-database-engine-features-in-sql-server-2016),
  [sql.kiwi](https://www.sql.kiwi/2024/08/dont-mix-with-datetime/)). A value
  read as `datetime` is `.003`; the same read through a `datetime2` cast is
  `.0033333`.
* **Infinity.** PostgreSQL `infinity`/`-infinity` timestamps and dates:
  psycopg2 maps them to `datetime.max`/`date.max` "and the mapping cannot be
  bidirectional", so they land as `9999-12-31 ...`
  ([psycopg2 usage](https://www.psycopg.org/docs/usage.html)). DMS truncates
  them to `9999-12-31 23:59:59` and `4713-01-01 00:00:00 BC`
  ([DMS PG source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html)).
  Debezium sends sentinel epochs and had overflow bugs in nanosecond mode
  ([dbz #1833](https://github.com/debezium/dbz/issues/1833),
  [dbz #2075](https://github.com/debezium/dbz/issues/2075)). And `to_char()`
  of an infinite timestamp returns **NULL** (`timestamp_to_char` checks
  `TIMESTAMP_NOT_FINITE`) - so migkit's PostgreSQL rendering
  (`canon.py:533-534`) renders `infinity` exactly like a NULL: a target that
  stored NULL for it compares **equal**.
* **`24:00:00`.** PostgreSQL `time` accepts it; psycopg2 returns `00:00:00`
  ("Retrieving a value of 24:00:00 results in a time of 00:00:00").
* **BC dates / year 0 / beyond 9999.** Python cannot hold them (psycopg2
  raises); PostgreSQL `to_char(..., 'YYYY')` of a BC year prints the year
  without an era, so `0044-03-15 BC` and `0044-03-15` render the same in
  migkit's SQL rendering. PyMongo raises on out-of-range BSON dates unless
  `datetime_conversion` is set - and `DATETIME_CLAMP` clamps silently.
* **MySQL zero dates** `0000-00-00`, and partial ones `2024-00-15`: DMS writes
  NULL; Debezium writes NULL, or the epoch for NOT NULL columns ("converted
  to empty value") ([DBZ-114](https://github.com/debezium/debezium/pull/105));
  migkit refuses by count (`engines/hetero.py:625`).
* **MySQL `TIME` outside 00:00-24:00**: Debezium could only capture
  `00:00:00`-`23:59:59.999999` until DBZ-342
  ([PR #320](https://github.com/debezium/debezium/pull/320)); pymysql returns a
  `timedelta`, which migkit's `render_value("time", ...)` cannot format
  (`canon.py:685-687` calls `.strftime`), and a PostgreSQL `time` target
  built for it (`canon.py:717`) cannot hold `-01:00:00` or `100:00:00`.
* **Time zones.** DMS: MySQL `TIMESTAMP` "converted to UTC on the target",
  `DATETIME` "not converted"; PostgreSQL `timestamptz` "normalized to UTC.
  The original offset literal is not retained". pgloader casts MySQL
  `datetime` to `timestamptz` by default, reading wall-clock values in the
  load session's zone ([pgloader #150](https://github.com/dimitri/pgloader/issues/150),
  [#331](https://github.com/dimitri/pgloader/issues/331)). DVT hashed TIME
  fields cast to GMT on one side only ([DVT #1301](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1301)).
  ClickHouse interprets a zone-less string in the *column's* zone. migkit
  pins sessions to UTC and models instant vs wall (`canon.py:1106`), and
  leaves `datetimeoffset` and Oracle TZ unmapped (`canon.py:262,318`).
* **Debezium `time.precision.mode=connect`** is millisecond regardless of
  column precision; `adaptive_time_microseconds` keeps precision; nanosecond
  modes overflow outside 1677-2262.
* **DMS MySQL `DATETIME`** "without a parenthetical value is replicated
  without milliseconds"; `DATETIME(1..5)` "is replicated with milliseconds".
* **Leap seconds**: no engine here stores `:60`; PostgreSQL accepts it on input
  and rolls to the next minute, Python `datetime` rejects it. Only a text
  source (CSV, JSON, Kafka) can carry one (to measure per target).
* **Intervals.** PostgreSQL `interval` keeps months, days and microseconds
  apart; Cassandra `duration` keeps months, days, nanoseconds; Oracle splits
  `YEAR TO MONTH` and `DAY TO SECOND`; MySQL and SQL Server have none.
  psycopg2 folds a month into 30 days (migkit notes it, `docs/backlog.md:2216`);
  Debezium `interval.handling.mode=numeric` is an approximate microsecond count,
  `string` is ISO 8601. DMS: `INTERVAL` "partially migrates".

**Canonical rendering**
* `timestamp` (wall): `YYYY-MM-DD HH:MM:SS.fffffffff` at **9** places, not 6,
  zero-padded to the right from whatever the engine holds; read through
  string or integer-nanosecond paths for engines past microseconds (pyodbc
  output converter, `oracledb` `fetch_lobs`/string fetch, ClickHouse
  `toString`, `pandas.Timestamp`). Years outside 0001-9999 or BC:
  `UNCOMPARABLE` until both sides can hold them, with ISO 8601 expanded years
  (`-0043-03-15`, astronomical numbering) as the rendering when they can.
* `timestamp` (instant): the same text of the UTC instant, which is what migkit
  does; the offset of `timestamptz`-like values that keep one
  (`datetimeoffset`, Oracle TSTZ, Snowflake TZ) as a separate `+HH:MM` suffix
  compared only when both sides keep an offset.
* `date`: `YYYY-MM-DD`; `time`: `HH:MM:SS.fffffffff`, with `24:00:00` allowed.
* `infinity`/`-infinity`: the literal words on both sides where both hold them,
  `UNCOMPARABLE` where one cannot - never NULL.
* `interval` (new class): ISO 8601 with the three parts kept apart,
  `P{months}M{days}DT{seconds}.{fraction}S`, compared only between engines
  that keep all three (PostgreSQL, Cassandra `duration`, Oracle pair).

**What migkit must refuse or warn**
* Refuse (or warn with a count) when the source's declared fractional precision
  exceeds the target's, or exceeds 6 on any in-process path: count rows where
  the sub-precision part is non-zero (SQL Server
  `datepart(nanosecond, c) % 1000 <> 0`, Oracle `extract(second ...)`,
  ClickHouse `toUnixTimestamp64Nano(c) % 1000 <> 0`).
* Count `infinity`, BC, beyond-9999 and `24:00:00` values before any move out
  of PostgreSQL; count out-of-range dates before a move into MySQL
  `TIMESTAMP` (2038), SQL Server `datetime` (1753), ClickHouse `DateTime64`
  (1900/2299), MongoDB via PyMongo.
* Count MySQL `TIME` values outside `00:00:00-23:59:59.999999` before building
  a `time` target; offer `interval`.
* Refuse carrying an offset-keeping type into one that drops the offset
  unless the hop says the offset is not data.

## 7. JSON (key order, duplicates, numbers, jsonb vs json, BSON types)

**Traps**
* **PostgreSQL `json`** keeps the text as given (whitespace, key order,
  duplicate keys); **`jsonb`** keeps none of them - "only the last value is
  kept" - but **"will preserve trailing fractional zeroes"**, rejects numbers
  outside `numeric`, rejects `\u0000`, and rejects escapes for characters the
  database encoding cannot hold
  ([PG JSON types](https://www.postgresql.org/docs/current/datatype-json.html)).
  So `json::jsonb` (migkit's PostgreSQL rendering, `canon.py:540`) **fails the
  whole digest query** on a `json` value holding `\u0000`.
* **MySQL JSON** keeps "last duplicate key wins" (8.0.3+), normalises
  whitespace to `", "` and `": "`, and sorts keys - but "the result of this
  ordering is subject to change and not guaranteed to be consistent across
  releases" ([MySQL JSON](https://dev.mysql.com/doc/refman/8.4/en/json.html)).
  migkit's MySQL JSON rendering is `cast(c as char)` (`canon.py:500`), i.e. it
  relies on that order matching jsonb's.
* **MySQL JSON numbers** parsed from text are int64, uint64 or **double**:
  integers outside [-2^63, 2^64-1] and decimals needing more than 17
  significant digits are rewritten silently
  ([Bug #112904](https://bugs.mysql.com/bug.php?id=112904),
  [#114740](https://bugs.mysql.com/bug.php?id=114740),
  [#116160](https://bugs.mysql.com/bug.php?id=116160) - `9088544342.689999`
  becomes `9088544342.69`). jsonb `1.10` comes back from MySQL as `1.1`,
  `1e20` prints as `1e20` on MySQL and `100000000000000000000` on jsonb.
* **SQL Server** has no JSON type before 2025 (`nvarchar(max)` + `ISJSON`);
  Oracle 21c+ `JSON` is OSON binary; Snowflake `VARIANT` and Redshift `SUPER`
  normalise differently again.
* **BSON types with no JSON counterpart**: ObjectId, Date, Decimal128, Int64 vs
  Int32 vs Double, Binary+subtype, Timestamp (an oplog counter, not a date),
  Regex, MinKey/MaxKey, JavaScript, DBRef. Relaxed Extended JSON (the
  `mongoexport` default) "can lose type information"; only canonical mode
  keeps Int32/Int64/Double apart
  ([Extended JSON v2](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)).
* **DMS** treats PostgreSQL JSON as a LOB and truncates it at the limited-LOB
  size; MySQL JSON maps to `CLOB`. data-diff renders PostgreSQL JSON as
  `::text`, so `json` vs `jsonb` columns differ on whitespace alone.
* **migkit's own writer** serialises dicts with `json.dumps(sort_keys=True,
  default=str)` (`canon.py:1003`): a `Decimal` inside a document becomes a
  JSON **string** (`"1.50"`), a `datetime` becomes a string, a `bytes` becomes
  `"b'..'"`. And `json_text` sorts keys by UTF-8 byte length then bytes
  (`canon.py:612`), which matches jsonb (it sorts by byte length then
  `memcmp`) - to be pinned by a test with non-ASCII keys.

**Canonical rendering**: parse the document and re-emit it **in process on
every side** (jsonb key order: shorter UTF-8 key first, then bytewise; last
duplicate wins; `", "`/`": "` separators; strings escaped as JSON with
non-ASCII raw), with every number in **value-normal form** (the decimal rule
from section 2: `1.10` and `1.1` and `1.10e0` all `1.1`; `1e20` as
`100000000000000000000`). Doing it in SQL on MySQL and PostgreSQL is only safe
if a probe per server shows the engine's own text equals `json_text` for a
fixed corpus (key order, numbers, escapes) - otherwise fold in process.
BSON-origin documents render through canonical Extended JSON for non-JSON
types (`{"$oid": ...}`, `{"$date": ...}`, `{"$numberDecimal": ...}`,
`{"$numberLong": ...}`), so a type change is a digest difference.

**What migkit must refuse or warn**
* Into MySQL JSON: scan the source for numbers that MySQL would turn into a
  double (integers outside int64/uint64; decimals with more than 15-17
  significant digits; trailing fractional zeros) and refuse or require
  `LONGTEXT` + `CHECK (JSON_VALID(c))`.
* PostgreSQL `json` sources: count values `jsonb` would change (duplicate keys,
  `\u0000`, key order the application reads) before building a `jsonb` target.
* Stop `sql_value`'s `default=str` from stringifying numbers and dates inside
  documents: serialise `Decimal` as a JSON number, and refuse the rest.

## 8. The rest of the type system

### 8.1 Enums and sets
* PostgreSQL enums are schema objects whose **sort order is declaration order**,
  not label order; MySQL `ENUM` sorts by index too and, outside strict mode,
  stores an invalid label as `''` (index 0). MySQL `SET` prints members in
  definition order joined by `,`. ClickHouse `Enum8/16` are name=number pairs.
* DMS: PostgreSQL `ENUM` "does not migrate"; MySQL `ENUM`/`SET` become
  `WSTRING(length)`. pgloader creates a PostgreSQL enum type per column.
* **Rendering**: the label text (migkit does this, `engines/postgres.py:220`).
  `SET`: members in the source's definition order - to compare a MySQL `SET`
  with a PostgreSQL `text[]`, render both as a sorted, de-duplicated list.
* **Warn**: a target that recreates the enum must keep the label order
  (`ORDER BY` results change otherwise); count labels on the source not
  defined on a pre-created target enum.

### 8.2 Arrays and composite types
* PostgreSQL arrays carry dimensions, lower bounds (`'[0:2]={1,2,3}'`), and
  NULL elements; `int[]` and `int[][]` are one type. Composite values print as
  `(a,"b c",)` with their own quoting. DMS: `ARRAY` needs a primary key or the
  table is suspended; `COMPOSITE` "does not migrate". MySQL, SQL Server,
  Oracle (outside VARRAY/nested tables) have no array.
* **migkit gap**: `type_class` cuts the declared type at `[`
  (`canon.py:459-461`), so a PostgreSQL `integer[]` is classed **`integer`**,
  `text[]` as `text`, `timestamp[]` as `timestamp`. The SQL renderer then
  falls through to `c::text` (`{1,2,3}`), the in-process one calls
  `int([1,2,3])`, a created target gets a `bigint` column, and the
  `test_decoding` path calls `int('{1,2,3}')`. Arrays must be unmapped - or a
  class of their own - rather than their element type.
* **Rendering** (array class): JSON array text through `json_text`, elements
  rendered by the element class, lower bound and dimensions as a prefix only
  when not the default (`[0:2]=`). MongoDB arrays and Cassandra `list<>` meet
  it; Cassandra `set<>` is sorted and `map<>` key-sorted by the server, so
  render them sorted.
* **Warn**: non-default lower bounds, multi-dimensional arrays and NULL elements
  when the target is a JSON column or Cassandra (which forbids null elements -
  "null is not supported inside collections").

### 8.3 Ranges and multiranges
* Discrete ranges are canonicalised by the server (`int4range '[1,4]'` prints
  `[1,5)`), continuous ones are not; `empty`; unbounded ends print as empty
  (`[1,)`). DMS: `RANGE` "does not migrate". No other engine here has a range
  type. migkit compares them only same-engine (`OWN`), which is right.
* **Rendering across engines** (when a target stores two columns or JSON): lower,
  upper, bound flags and `empty` as a JSON object after the server's own
  canonicalisation; never re-canonicalise in Python.

### 8.4 hstore and tsvector
* `hstore` -> JSON object of strings with `NULL` values as JSON `null`; migkit
  casts through `jsonb` (`canon.py:199-202`). Keys are unique, order is not data.
* `tsvector` prints lexemes sorted, deduplicated, with positions and weights
  (`'a':1A 'b':2`); migkit compares its text (`canon.py:191-193`). Carrying it
  to a text column keeps the value but loses the type; re-parsing it on a
  different `default_text_search_config` does not change a tsvector literal
  (it is already lexemes) but does change `to_tsvector(...)` - warn when the
  target regenerates it from source text instead of copying it.
* DMS: `TSVECTOR`/`TSQUERY` "does not migrate"; Debezium streams them as strings.

### 8.5 Network types
* PostgreSQL `inet` keeps host bits and prints `/32` only when the mask is not
  full (`10.0.0.1`, `10.0.0.1/24`); `cidr` rejects host bits; `macaddr` vs
  `macaddr8`; IPv6 is printed compressed. ClickHouse `IPv4/IPv6` are numbers
  printed in dotted/compressed form; MySQL stores `VARBINARY(16)` via
  `INET6_ATON` or text; Cassandra `inet` has no mask.
* DMS: `CIDR` migrates, `INET` and `MACADDR` "do not migrate".
* **Rendering**: `ipaddress.ip_interface(v)` in Python - the address in RFC 5952
  compressed lowercase form plus `/n` always, IPv4-mapped IPv6 kept as IPv6.
  migkit today renders `inet` as its PostgreSQL text (`canon.py:189`, class
  `text`), which meets a MySQL text column only if the application wrote it
  the way PostgreSQL prints it.
* **Warn**: `inet` values with a mask into targets that keep only an address
  (ClickHouse `IPv4`, Cassandra `inet`, MySQL `INET6_ATON`).

### 8.6 Geometry and geography (WKB/WKT/SRID, axis order)
* **Axis order**: MySQL 8 follows EPSG for geographic SRSs - SRID 4326 is
  **latitude first** in WKT/WKB input and output unless `axis-order=long-lat`;
  PostGIS is always X=longitude. Swapped points within +/-90 are accepted and
  land in the wrong place ([MySQL axis order](https://dev.mysql.com/blog-archive/axis-order-in-spatial-reference-systems/),
  [PostGIS lon/lat](https://postgis.net/documentation/tips/lon-lat-or-lat-lon/)).
  GeoJSON is always lon-lat on both.
* **Storage forms**: PostGIS EWKB (SRID inside), MySQL internal = 4-byte SRID +
  WKB, SQL Server's own CLR format, Oracle `SDO_GEOMETRY`, MongoDB GeoJSON
  objects (`2dsphere`). `geography` vs `geometry` changes what distance means.
* DMS moves MySQL spatial as `BLOB`, PostGIS types only PG-to-PG; pgloader turns
  MySQL `point` into PostgreSQL `point` (not PostGIS) and `linestring` into
  `path`.
* **Rendering**: `ST_AsBinary` (ISO WKB, **not** EWKB) in lon-lat order, hex
  uppercase, plus `SRID=n;` prefix; MySQL side
  `ST_AsWKB(c, 'axis-order=long-lat')`. Coordinates are doubles, so a
  WKT/GeoJSON route can round; compare WKB bytes, never WKT text.
* **Refuse** geographic SRIDs across MySQL <-> anything without an explicit axis
  order in the hop; warn on `geometry` <-> `geography` and on SRID 0 targets.

### 8.7 XML
* SQL Server `xml` re-serialises: the XML declaration and encoding attribute
  are dropped, insignificant whitespace may go, attribute quoting and empty
  elements normalise. Oracle `XMLType` binary storage does the same. PostgreSQL
  `xml` keeps the text. DMS: `XML` "partially migrates".
* **Rendering**: exact text when both sides keep text (PostgreSQL <-> text);
  otherwise Canonical XML 1.1 (C14N, `lxml.etree.tostring(method="c14n2")`)
  on both sides. migkit maps PostgreSQL and SQL Server `xml` to `text`
  (`canon.py:190,273`) - a SQL Server target will read as a difference in
  every row that had a declaration.

### 8.8 Large objects and LOB columns
* PostgreSQL large objects live in `pg_largeobject` and are referenced by
  `oid`; the table holds a number, not the data (migkit's `_large_objects`
  helper covers PostgreSQL only).
* DMS "limited LOB mode" truncates LOBs at the configured size (JSON included)
  and MySQL `MEDIUMBLOB/LONGBLOB/MEDIUMTEXT/LONGTEXT` are skipped entirely
  when LOB support is off.
* Oracle `LONG`/`LONG RAW`, SQL Server `text/ntext/image` are deprecated
  types that drivers stream; `max_allowed_packet` caps a MySQL write.
* **Warn**: `oid` columns that reference large objects (compare the objects'
  bytes, not the number); largest LOB vs the target's packet/item limit
  (DynamoDB item 400 KB, MongoDB document 16 MB, MySQL `max_allowed_packet`).

### 8.9 Vectors
* **pgvector** `vector` stores float4 and prints the shortest float4 text
  (`float_to_shortest_decimal_bufn`), no NaN/Inf; `halfvec` is float16
  (`0.1` becomes `0.0999755859375`); `sparsevec` prints
  `{index:value,...}/dim` with 1-based indexes
  ([pgvector](https://github.com/pgvector/pgvector)).
* **MySQL 9 `VECTOR`** is float32 returned to clients as binary;
  `VECTOR_TO_STRING` prints `[2.00000e+00,...]` - **six significant digits**,
  so its text is lossy and must never be the rendering
  ([MySQL vector functions](https://dev.mysql.com/doc/refman/9.4/en/vector-functions.html)).
* MongoDB stores embeddings as arrays of doubles or BSON Binary subtype 9
  (packed float32/int8/bit); Cassandra 5 `vector<float, n>`.
* migkit maps pgvector types to `text` (`canon.py:194-198`), which is right
  PG-to-PG and PG-to-text only.
* **Rendering** (vector class): the list of float32 values, each widened to
  float8 and printed by the float rule; `sparsevec` expanded to its dense form
  only when compared with a dense vector.
* **Refuse** float64 arrays into float32/float16 vectors unless every element
  round-trips; warn on `halfvec` targets always.

## 9. Engine-specific: Redis, Cassandra, ClickHouse, DynamoDB, MongoDB

### Redis
* Every value is a binary-safe string; numbers are strings (`INCR` on a
  64-bit signed decimal), sorted-set scores are doubles (`inf`/`-inf` allowed),
  TTL is key metadata. migkit copies Redis only to Redis with `DUMP`/`RESTORE`
  and a raw-bytes client (`engines/redis.py:10-31`), which keeps type, encoding
  and TTL - the right answer. **Trap**: `RESTORE` refuses payloads from a newer
  RDB version, and Redis 7.4+ and Valkey 8 diverged in RDB version numbering;
  check the target accepts the source's RDB version before the move.
* Cross-engine (Redis -> SQL) needs a declared mapping per key pattern; values
  are text or bytes, scores float.

### Cassandra / ScyllaDB
* `counter`: 64-bit, cannot be set, only incremented; lives in counter-only
  tables; no `INSERT` ("use UPDATE instead"). A counter moved into a counter
  table has to be written as `+ n` from zero (migkit's `Added`,
  `canon.py:97`, is the right shape).
* `duration`: months, days, nanoseconds, no ordering, not keyable.
* `tuple` is always frozen; UDTs are keyspace-scoped; non-frozen empty
  collections **are NULL** ("an empty set, list, or map is stored as a null
  set") - so an empty PostgreSQL array or MongoDB `[]` becomes NULL.
* `timestamp` is milliseconds (migkit says so, `engines/cassandra.py:11`);
  `varint` unbounded; `decimal` arbitrary; `-0.0` and `0.0` are distinct
  values and distinct keys, while migkit's float rendering writes both as `0`
  (`canon.py:572-574`).
* **Warn**: empty collections -> NULL; `-0.0` keys; values past int64 into
  `bigint`.

### ClickHouse
* **NULL into a non-Nullable column becomes the type's default (0, '')**
  because `input_format_null_as_default=1` by default, with several open
  bugs where even `0` does not stop it
  ([#67273](https://github.com/ClickHouse/ClickHouse/issues/67273),
  [#83553](https://github.com/ClickHouse/ClickHouse/issues/83553)). migkit
  creates non-key columns `Nullable` (`engines/clickhouse.py:256`) - safe for
  tables it builds, not for a pre-created target.
* **`LowCardinality(Nullable(String))`** is the legal nesting
  (`Nullable(LowCardinality(...))` is rejected by ClickHouse). migkit's
  `_unwrap` (`engines/clickhouse.py:16-23`) strips `Nullable(` first and
  `LowCardinality(` second, so the legal form comes out as
  `Nullable(String)` and the column is unmapped - named, not compared.
* `Decimal(P,S)`: extra fraction digits are **truncated, not rounded**; the
  docs say overflow is not checked for `Decimal128/256` in arithmetic ("incorrect
  result is returned, no exception") - to measure on insert
  ([ClickHouse Decimal](https://clickhouse.com/docs/sql-reference/data-types/decimal)).
  migkit builds `Decimal(38, 10)` by default (`canon.py:757`).
* `DateTime64(p, 'tz')`: zone in column metadata; strings without a zone are
  read in the column's zone. `FixedString(n)` pads with NUL bytes. `UInt64`
  and wider.
* **Warn**: pre-created non-Nullable targets (count source NULLs per column),
  unsigned ranges, `FixedString` padding.

### DynamoDB
* `N`: 38 significant digits, 1E-130 to 9.99..E+125, "Leading and trailing
  zeroes are trimmed", no NaN/Inf
  ([DynamoDB data types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html));
  migkit keeps each column's scale in table tags and re-quantises on read
  (`engines/dynamodb.py:244-252`) - the right design.
* Sets `SS/NS/BS`: unordered, unique, **never empty**; empty strings allowed
  since 2020 except in keys; item limit 400 KB. migkit carries maps/lists/sets
  as `RawAttr` canonical JSON (`engines/dynamodb.py:25-57`).
* `_to_attr` writes a float as `repr(float)` (`engines/dynamodb.py:224`): `nan`,
  `inf` and `5e-324` are rejected by the service mid-load.
* **Warn/refuse**: numbers over 38 digits or out of range, NaN/Inf, empty sets,
  empty key strings, items over 400 KB - all by count before the move.

### MongoDB
* Covered above: int32/int64/double/Decimal128 split, Date in milliseconds,
  ObjectId, Binary subtypes, absent vs null (migkit keeps them apart,
  `canon.py:135`), relaxed vs canonical Extended JSON.
* A value that came from MongoDB as `ObjectId`, `Decimal128`, `long` or a
  subtype-4 `Binary` and goes back to MongoDB through a SQL hop returns as a
  string, `Decimal128`, `int` or subtype 0 - the digest (text/decimal/integer/
  bytes classes) stays equal while the **type** changed.
* mongosync (MongoDB to MongoDB only) copies BSON as BSON; migkit measured it
  keeping `NumberLong` and `Decimal128` (`docs/backlog.md:1844`).

## 10. How the other tools handle it (one line each, sources above)

| tool | numbers | time | text/binary | JSON / docs | published loss |
|---|---|---|---|---|---|
| AWS DMS | unconstrained PG numeric -> `NUMERIC(28,6)`; `MapUnboundedNumericAsString` PG-to-PG only, 28 digits in CDC; FLOAT range excludes denormals; UINT8 for unsigned | PG `infinity` -> `9999-12-31 23:59:59`; tstz offset dropped; MySQL `TIMESTAMP` -> UTC, `DATETIME` as-is; `DATETIME` without precision loses ms; zero dates -> NULL | PG unbounded varchar partial; LOB off drops MEDIUM/LONG BLOB/TEXT; `ENUM`, `INET`, `MACADDR`, `TSVECTOR`, `RANGE`, `COMPOSITE` do not migrate | JSON is a LOB, truncated in limited-LOB mode; JSONB -> NCLOB | `0.611111104488373` -> `0.611111` (own docs); unsigned bigint and validation false negatives (release notes) |
| Debezium | `decimal.handling.mode` precise (VariableScaleDecimal) / double (lossy) / string; NaN only in double/string; `bigint.unsigned.handling.mode=long` wraps | `adaptive_time_microseconds` keeps precision; `connect` is ms; nanos overflow outside 1677-2262; PG infinity -> sentinel epochs; MySQL zero dates -> NULL or epoch | `binary.handling.mode` bytes/base64/hex; PG UTF-8 databases only | JSON as string; `interval.handling.mode` numeric (approximate) / string | infinity overflow [dbz #1833], nanos overflow [dbz #2075], TIME `00:00:00` null in snapshot [dbz #2631] |
| DVT | cast to string, RSTRIP, SHA256; `trim_scale()` for scale-less decimals (PG 13+); scientific notation blocked for Db2 | per-engine timestamp format; TIME cast to GMT on one side [#1301] | RSTRIP hides trailing spaces; per-engine UTF fixes | not normalised | NUMERIC PG vs Oracle [#620], TD vs BQ decimals [#1372] |
| data-diff (archived 2024) | `decimal(38, p)` per column; float precision from binary bits minus 2 on PG | per-engine round-or-truncate to the lower precision | UUID trimmed; MSSQL uppercase UUID fixed then reverted | PG JSON as `::text` | PG/Snowflake date normalisation false positives [#877] |
| pgloader | `tinyint(1)` -> boolean (lossy for other values); unsigned bigint -> numeric; `float` -> double (widening) | MySQL `datetime` -> `timestamptz` in session zone; zero dates -> NULL | `remove-null-characters` strips NUL silently; binary -> bytea | JSON as text/jsonb | tz shift [#150, #331]; float4 widening [#746] |
| mongosync | BSON as BSON (keeps NumberLong, Decimal128) | BSON dates as-is | - | no duplicate field names, no `$`-prefixed names, no `Timestamp(0,0)` on pre-6.0 | `$v` field fatal before 8.0.20 ([limitations](https://www.mongodb.com/docs/mongosync/current/reference/limitations/)) |

What none of them do, and migkit should: **count the values that would change
before the move** and refuse by default. Every tool above either converts
silently (DMS, pgloader), exposes a knob with a lossy default (Debezium), or
normalises during comparison so the loss is invisible (DVT RSTRIP, data-diff
rounding).

## 11. Type family x engine pair

`R` = refuse by default (override per column in the hop), `W` = warn with a
count, `-` = nothing to do. Renderings refer to sections 1-9.

| family | pair | safe mapping | canonical rendering | known loss | migkit before the move |
|---|---|---|---|---|---|
| integer | MySQL -> PG | signed as-is; `bigint unsigned` -> `numeric(20,0)`; `int unsigned` -> `bigint` | `str(int)` | overflow stops the load mid-way | R if unsigned bigint lands in `bigint` and any value > 2^63-1 |
| integer | MySQL/CH/DuckDB -> MongoDB | int64 if in range, else `Decimal128` | `str(int)` | pymongo `OverflowError` | R values > 2^63-1 without `Decimal128` |
| integer | MongoDB -> SQL | `int`+`long` -> `bigint` | `str(int)` | field left unmapped today | W fields holding `int|long|double` (ambiguous) |
| integer | any -> Cassandra | `bigint`, `varint` for wider | `str(int)` | - | R wider than int64 into `bigint` |
| integer | any -> DynamoDB | `N` | `str(int)` | >38 digits rejected | R >38 digits |
| boolean | MySQL `tinyint(1)` -> PG `boolean` | keep `smallint` unless values are {0,1} | `0/1` | pgloader-style non-zero -> true | W values outside {0,1} |
| bit | MySQL `bit(n)` -> PG | `bit(n)` or `bigint` | unsigned big-endian int + width fact | width lost as `bigint` | W `bit(n>1)` into integer |
| decimal | PG `numeric` (no scale) -> MySQL | `decimal(p,s)` from the source's measured max integer digits and scale | value-normal | fraction rounded with only Note 1265 | R measured scale > target scale or integer digits > p-s |
| decimal | PG `numeric` -> SQL Server/Snowflake/BigQuery | `decimal(38,s)` / `BIGNUMERIC` | value-normal | >38 digits | R >38 digits; R NaN/Inf |
| decimal | Oracle `NUMBER` -> PG | `numeric` (no typmod) or `numeric(p,s)` when declared | value-normal | `NUMBER(p,-s)` already rounded at source | - |
| decimal | any -> MongoDB | `Decimal128` | value-normal (scale kept, compare normal) | >34 digits raises | R >34 significant digits |
| decimal | any -> DynamoDB | `N` + scale in tags (migkit does) | value-normal | trailing zeros trimmed; >38 digits rejected | R >38 digits / out of 1E-130..1E126 |
| decimal | any -> ClickHouse | `Decimal(P,S)` from source | fixed scale | extra fraction **truncated**; Decimal128/256 overflow unchecked in arithmetic | R scale > S; measure insert overflow |
| money | PG `money` -> any | `numeric(19,2)` read via `::numeric` | fixed scale | locale text in digest today | - (fix rendering) |
| float | MySQL `FLOAT` -> PG | `real` (not `double precision`) | float4 widened to float8, banded | widening shows binary digits | W float4 -> float8; R float8 -> float4 unless round-trips |
| float | PG `float8` -> MySQL/SQL Server/DynamoDB | `double` / `float` / `N` | banded float8 | NaN/Inf rejected; denormals out of DMS range | R NaN/Inf; W denormals |
| float | any -> MongoDB | `double` | banded float8 | int vs double subtype lost in relaxed EJSON | - |
| text | PG/MongoDB/Oracle -> MySQL (`_0900_ai_ci`) / SQL Server (`CI_AS`) | same text; key columns in a binary or `_as_cs` collation | exact code points, UTF-8 bytes | **distinct keys merge, one row overwritten** | R keys colliding under target collation (case, accent, pad, NFC/NFD) |
| text | any -> MySQL `utf8mb3`/latin1 | `utf8mb4` | exact | 4-byte chars error or truncate | R 4-byte chars |
| text | MySQL/SQL Server/MongoDB -> PG | `text`/`varchar(n)` | exact | NUL rejected (pgloader strips it) | R values containing U+0000 |
| text | any -> Oracle | `VARCHAR2(n CHAR)` | exact | `''` becomes NULL | W empty strings |
| text | SQL Server/Oracle/Db2 `CHAR(n)` -> any | `varchar(n)` or `char(n)` | unpadded | - | - (fix SQL Server/ASE unpad) |
| bytes | PG `bytea` -> MySQL | `varbinary(n)`/`longblob` | uppercase hex | escape-format text misread (CDC) | pin `bytea_output=hex` |
| uuid | PG `uuid` -> SQL Server | `uniqueidentifier` | lowercase RFC text | uppercase text read back today | - (uuid class) |
| uuid | MySQL `BINARY(16)` -> PG `uuid` | `uuid` via declared swap flag | lowercase RFC text | `UUID_TO_BIN(u,1)` order | R without a declared swap flag |
| uuid | MongoDB `Binary` 3/4 -> any | `uuid` by declared representation | lowercase RFC text | legacy byte order | R subtype 3 without a representation |
| timestamp | SQL Server `datetime2(7)` / Oracle `TIMESTAMP(9)` / CH `DateTime64(9)` -> PG/MySQL | `timestamp(6)` | 9-place text read at full precision | 7th-9th digits cut, digest blind | R (count rows with sub-us digits) |
| timestamp | PG -> MySQL `DATETIME` | `datetime(6)`; `timestamptz` -> `datetime(6)` in UTC with W | 9-place UTC text | `infinity` -> 9999-12-31; BC/>9999 impossible | R infinity/BC/>9999 |
| timestamp | any -> MySQL `TIMESTAMP` | prefer `datetime(6)` | UTC text | 1970-2038 only | R out of range |
| timestamp | any -> SQL Server `datetime` | `datetime2(p)` | 9-place | 1/300 s rounding, 1753 floor | R pre-1753; W non-tick fractions |
| timestamp | any -> MongoDB / Cassandra | `date` / `timestamp` | 9-place (zeros past ms) | sub-ms cut | W rows with sub-ms digits |
| timestamp | any -> ClickHouse | `DateTime64(p,'UTC')` | 9-place | column-zone reinterpretation; range 1900-2299 (p=9: 1677-2262) | R out of range; pin column zone |
| timestamp | `datetimeoffset` / Oracle TSTZ -> PG `timestamptz` | instant | UTC text (+offset when both keep it) | offset literal dropped | W offset dropped |
| date | MySQL zero dates -> any | NULL or a chosen date by rule | `YYYY-MM-DD` | DMS/Debezium NULL/epoch silently | R (migkit already does) |
| time | MySQL `TIME` -> PG | `time(6)` when in 0-24h else `interval` | `HH:MM:SS.fffffffff` | wrap / error | R outside 00:00-23:59:59.999999 |
| time | PG `time '24:00:00'` -> any | keep `time` on PG targets | allow `24:00:00` | psycopg2 reads `00:00:00` | R 24:00:00 into non-PG |
| interval | PG `interval` <-> Cassandra `duration` / Oracle | same-shape type | ISO 8601 three parts | months folded to 30 days by psycopg2 | R into engines without one |
| json | PG `jsonb` -> MySQL `JSON` | `JSON` only if every number fits int64/uint64/double exactly | in-process `json_text` with value-normal numbers | big ints and >17-digit decimals become doubles; trailing zeros drop | R numbers MySQL would rewrite (else `LONGTEXT`+`JSON_VALID`) |
| json | PG `json` -> `jsonb`/MySQL | `jsonb` | in-process | duplicate keys, key order, `\u0000` | W duplicates; R `\u0000` |
| json | MongoDB doc -> PG `jsonb` | `jsonb` with canonical Extended JSON for BSON-only types | canonical EJSON | ObjectId/Date/Decimal128/long flattened | W BSON-only types |
| enum | PG enum -> MySQL `ENUM` | `ENUM(...)` same label order | label | order changes sort | W label order |
| set | MySQL `SET` -> PG | `text[]` | sorted unique list | - | - |
| array | PG array -> MySQL/SQL Server | `JSON` | JSON array by element class | lower bounds, dimensions, NULL elements | W non-default bounds; R multi-dim into JSON without prefix |
| array | PG array / Mongo array -> Cassandra | `list<>`/`set<>`/`frozen<>` | JSON array (sets sorted) | empty -> NULL; null elements refused | W empty; R null elements |
| range | PG range -> any other | two columns or JSON | server-canonical bounds as JSON | - | R without a declared layout |
| hstore | PG -> MySQL | `JSON` | JSON object (migkit does) | - | - |
| tsvector | PG -> non-PG | text | server text (migkit does) | type lost | W |
| network | PG `inet/cidr` -> MySQL/CH/Cassandra | text, or `IPv6`/`inet` when no masks | RFC 5952 + `/n` always | mask dropped | W values with masks into address-only targets |
| spatial | PostGIS <-> MySQL 8 | same geometry type and SRID | ISO WKB lon-lat hex + SRID | **axis swap** for SRID 4326 | R geographic SRIDs without explicit axis order |
| xml | PG `xml` -> SQL Server `xml` | `xml` | C14N 2.0 | declaration/whitespace rewritten | W |
| lob | PG large objects (`oid`) -> any | `bytea`/`BLOB` of the object | hex of object bytes | the number moves, not the data | R `oid` columns without `lo` handling |
| vector | pgvector -> MySQL 9 `VECTOR` / MongoDB | `VECTOR(n)` / array of doubles or Binary 9 | float32 list rendered by the float rule | `halfvec` float16; MySQL text is 6 digits | R float64 into float32/16 unless round-trips |
| counter | Cassandra counter -> Cassandra counter | counter table, `UPDATE += n` | `str(int)` | - | - (migkit `Added`) |
| collection | DynamoDB `SS/NS/BS` -> SQL | `JSON` sorted | canonical JSON (migkit `RawAttr`) | - | - |
| any | NULL -> ClickHouse non-Nullable (pre-created) | `Nullable(T)` | - | NULL becomes 0/'' | R NULLs into non-Nullable |
| any | MongoDB -> SQL -> MongoDB | declared BSON type per field kept in hop state | canonical EJSON | `long`->`int`, ObjectId->string, subtype 4->0 | W type change on return |

## 12. Gaps in migkit (file:line, effort, docker recipe)

Effort: **S** under a day, **M** a few days, **L** a new class or subsystem.
Every recipe follows one shape: seed the edge values on the source container,
run `migkit move` on a hop between the two containers, run `migkit check`,
and read the verdict **plus** a direct query of both sides. "Today" is what the
code reading predicts; each recipe exists to measure it.

Images: `postgres:16`, `mysql:8.4`, `mcr.microsoft.com/mssql/server:2022-latest`,
`clickhouse/clickhouse-server:24.8`, `mongo:7.0` (replica set),
`cassandra:5.0`, `amazon/dynamodb-local`. Oracle Free does not fit this
machine's container VM (`docs/backlog.md`, item 11); its recipes are marked.

### Silent false "equal" (the worst kind - fix first)

**G1. PostgreSQL `infinity` renders as NULL.** `to_char()` returns NULL for
non-finite timestamps, and `_postgres` renders `timestamp` through it
(`canon.py:533-534`); `date` goes through `::text` and is fine. A target row
holding NULL where the source held `infinity` compares equal. Fix: `case when
isfinite(c) then to_char(...) else '<UNCOMPARABLE>' end`, and count
`not isfinite(c)` before a move (psycopg2 turns them into `9999-12-31`). **S.**
Recipe: PG `create table t(id int primary key, ts timestamp, tz timestamptz)`
with rows `infinity`, `-infinity`, `NULL`, `2024-01-01`; MySQL target created
by hand with the same rows but NULL for the two infinities; `check` -> today
`same`; expected: 2 uncomparable, verdict not `same`. Then `move` into an
empty MySQL -> today `9999-12-31 23:59:59.999999` lands silently; expected
refusal naming 2 rows.

**G2. BC and five-digit years render like AD years.** `to_char(c,
'YYYY-...')` prints `0044` for `0044 BC` (`canon.py:533`). Fix: `BC` suffix or
astronomical year in the PostgreSQL format, `UNCOMPARABLE` for years outside
1-9999 in-process. **S.** Recipe: PG `ts` rows `0044-03-15 BC` and
`0044-03-15`; DuckDB target (holds BC) seeded with both swapped; `check` ->
today equal; expected different.

**G3. Sub-microsecond digits are cut on both sides at read.** Every
in-process engine reads through Python `datetime` (`render_value`,
`canon.py:666-680`) and `type_class` drops the declared precision
(`canon.py:459-461`), so `datetime2(7)`, `DateTime64(9)`, Oracle
`TIMESTAMP(9)`, Snowflake `TIMESTAMP_NTZ(9)` lose digits in the move and in the
digest alike. Fix: read those columns as text or integer nanoseconds (SQL
Server `convert(varchar(27), c, 121)`, ClickHouse `toString(c)` /
`toUnixTimestamp64Nano`), render 9 places, and add fractional precision to
`capacity()`/`narrower()` (`canon.py:1212-1256`) so a narrower target is
refused with a count. **M.** Recipe: SQL Server `datetime2(7)` rows
`2024-01-01 00:00:00.1234567`, `...0000001`, `...9999999`; PG target;
`move` then `check` -> today `same` with PG holding `.123457`/`.000000`/rolled
to the next second; expected: move refused, 3 rows counted. Same with
ClickHouse `DateTime64(9)` -> MySQL `datetime(6)`.

**G4. `extra_float_digits` is not pinned.** `_psql` sets `TimeZone` and
`DateStyle` only (`engines/postgres.py:159`); the float renderer goes through
`c::text` (`canon.py:509,527`). A database or role with
`extra_float_digits=0` (or PostgreSQL 11) prints 15 digits, so
`0.30000000000000004` renders as `0.3` - equal to a target that really holds
`0.3`. Fix: `-c extra_float_digits=1` in `PGOPTIONS` (PG 12+), refuse the SQL
float rendering on 11 and older (fold in process). **S.** Recipe: PG
`alter database d set extra_float_digits = 0`; source `f float8 = 0.1::float8 +
0.2::float8`; MySQL target seeded `0.3`; `check` -> today `same`; expected
different.

**G5. Types that change on a round trip through SQL keep their digest.**
MongoDB `long` -> SQL -> MongoDB comes back `int`, `ObjectId` comes back a
string, `Binary` subtype 4 comes back subtype 0, and `Decimal128` stays
`Decimal128` only because `_bind` catches `Decimal` (`engines/mongodb.py`
`_bind`); the classes (`canon.py:431-439`) render the payload only. Fix: keep
the declared BSON type per field in hop state and restore it on write; render
MongoDB-origin values through canonical Extended JSON type tags when the
target is MongoDB. **M.** Recipe: `mongo:7.0` collection with `{_id:
ObjectId(), n: NumberLong(1), u: UUID(...), d: NumberDecimal("1.50")}`; hop
Mongo -> PG, then PG -> a second Mongo; `check` second hop -> today `same`;
expected: `$type` of `_id`, `n`, `u` differs and is reported.

### Silent change during the move (digest catches it after the fact)

**G6. ClickHouse NULL-to-default on a pre-created target.** migkit builds
Nullable columns (`engines/clickhouse.py:256`) but writes into an existing
non-Nullable column as-is; with `input_format_null_as_default=1` NULL becomes
0/''. The source's NULL does not render like 0, so `check` catches it - after
the target was written. Fix: count source NULLs per column whose target is
not `Nullable`, refuse. **S.** Recipe: CH `create table t(id Int64, v Int64)
engine=MergeTree order by id`; PG source `v` NULL in 3 rows; `move` -> today
0s written, `check` DIFF; expected refused before writing.

**G7. MySQL rounds decimal fractions under strict mode.** Note 1265 only
(`engines/mysql.py:23` assumes strict mode catches it). And the default target
for unconstrained numeric is `decimal(65,10)` (`canon.py:723`). Fix: measure
the source's max scale and integer digits before building or writing; refuse
when either exceeds the target; build `decimal(p,s)` from the measurement.
**S.** Recipe: PG `n numeric` rows `0.12345678901234`, `1.5`, `1e40`; `move`
to MySQL (created) -> today row 1 rounded to 10 places silently; expected
refused with the measured scale 14.

**G8. Collation-colliding keys overwrite each other.** MySQL writer is
`insert ... on duplicate key update` (`engines/mysql.py:284-296`); SQL Server
and ClickHouse writers replace by key as well. Keys distinct on a
case/accent/pad-sensitive source merge on a `_ci`/`_ai`/PAD SPACE target and
the later row wins. Fix: before writing, load the distinct source keys into a
temporary table with the target's collation and count duplicates; refuse and
name them. **M.** Recipe: PG `pk text primary key` rows `a`, `A`, `a ` (trailing
space), `é` (U+00E9), `é` (U+0065 U+0301); MySQL 8.4 target database default
`utf8mb4_0900_ai_ci` (NO PAD, so `a ` stays distinct; `a`=`A`; NFC=NFD `é`);
`move` -> expected today 3 rows land (5 -> 3) and `check` reports 2 missing;
expected after the fix: refusal naming the 2 colliding groups. Repeat with
target collation `utf8mb4_unicode_ci` (PAD SPACE: `a`, `A`, `a ` are one key,
5 -> 2) and SQL Server `CI_AS`.

**G9. JSON numbers MySQL rewrites.** PG `jsonb` -> MySQL `JSON`: integers
outside int64/uint64 and decimals past double precision are stored as doubles;
trailing zeros drop. `sql_value` (`canon.py:1003`) sends the text; nothing
counts. Fix: pre-move scan of JSON numbers against MySQL's representable set,
refuse or build `longtext` + `json_valid`. **M.** Recipe: PG `j jsonb` rows
`{"n":123456789012345678901234567890}`, `{"n":9088544342.689999}`,
`{"n":1.10}`; `move` to MySQL -> today stored as `1.2345678901234568e29`,
`9088544342.69`, `1.1`, `check` DIFF on 3 rows; expected refusal on the first
two and `1.10`/`1.1` equal under value-normal numbers.

**G10. MySQL `TIME` outside a day, and `timedelta`.** DDL maps `time` to
`time(6)` (`canon.py:717,748`) and `render_value("time", ...)` calls
`.strftime` (`canon.py:685-687`), which a pymysql `timedelta` does not have.
Fix: render `timedelta` as `[-]HH:MM:SS.ffffff` with hours past 24, count
values outside `00:00:00-23:59:59.999999` and refuse or build `interval`. **S.**
Recipe: MySQL `tm time(6)` rows `-01:00:00`, `100:00:00`, `12:00:00.5`; PG
target; `move` then a batch read-back -> today an `AttributeError` or a wrapped
value (to measure); expected refusal naming 2 rows.

**G11. PostgreSQL `time '24:00:00'`.** psycopg2 reads it as `00:00:00`
(documented). Fix: count before moving to non-PostgreSQL targets. **S.**
Recipe: PG `tm time` row `24:00:00`; MySQL target; `move` -> today
`00:00:00`, `check` DIFF; expected refused.

**G12. NaN/Infinity in PostgreSQL `numeric`.** Rendered as text (`canon.py:541`,
in-process `format(Decimal('NaN'),'f')` -> `NaN`), written as-is; MySQL/SQL
Server/Oracle/DynamoDB reject at write. Fix: count
`c = 'NaN'::numeric or c in ('Infinity','-Infinity')` and refuse unless the
target holds them (MongoDB `Decimal128`); render as `UNCOMPARABLE` when only one
side can. **S.** Recipe: PG `n numeric` rows `NaN`, `Infinity`, `1`; MySQL
target; `move` -> today stops at the first; expected refused before the first
write, 2 rows counted.

**G13. `bytea` escape output in the decoding path.** `from_text` knows only
`\x` (`canon.py:1040-1044`), used by `pgslot.value` (`migkit/pgslot.py:145-154`).
Fix: pin `bytea_output=hex` on the decoding session and parse the escape form
too. **S.** Recipe: PG `alter database d set bytea_output='escape'`; tail
running; insert `'\x00ff41'::bytea`; MySQL `varbinary` target -> expected
`00FF41` on both sides (today: to measure).

**G14. `sql_value` stringifies what it does not know.** `json.dumps(...,
default=str)` (`canon.py:1003`) turns a `Decimal`, `datetime`, `bytes` or
`ObjectId` inside a document into a JSON string. Fix: an encoder that writes
`Decimal` as a JSON number and refuses the rest by name. **S.** Recipe: MongoDB
`{_id:1, sub:{d: NumberDecimal("1.50"), t: ISODate()}}` -> PG `jsonb` target
created by hand -> today `"1.50"` as a string; expected `1.50` as a number or a
refusal.

### False "different" (costs trust, not data)

**G15. PostgreSQL arrays classed as their element type.** `type_class` cuts at
`[` (`canon.py:459-461`): `integer[]` is `integer`, `text[]` is `text`. The
created target gets a scalar column and the write fails; the decoding path
calls `int('{1,2,3}')`. Fix: unmapped (named) until an `array` class exists;
then the JSON-array rendering of 8.2. **S** (unmap) / **M** (class). Recipe: PG
`a int[], s text[]` rows `{1,2,3}`, `{}`, `NULL`, `[0:1]={5,6}`; MySQL target;
`move` -> today a `bigint` column and a write error; expected "no neutral
class for a (integer[])" or a JSON column with 4 equal rows.

**G16. Unconstrained numeric vs fixed scale.** PG `numeric` `1.5` vs MySQL
`decimal(65,10)` `1.5000000000`, Oracle `NUMBER` vs PG `numeric(10,2)` (Oracle
side is re-scaled by `_unpad`, PG side is not), DynamoDB `N` without tags.
Fix: value-normal rendering whenever either side has no fixed scale
(`render_value` `canon.py:638-655`, PG `trim_scale(c)::text`, MySQL trimmed
`cast`). **S.** Recipe: PG `n numeric` rows `1.5`, `1.50`, `0`, `-0.0`, `100`;
`move` to MySQL (created `decimal(65,10)`) -> today 5 rows DIFF (to measure);
expected `same`.

**G17. `money` renders its locale text.** `money -> decimal` (`canon.py:183`)
but `_postgres` falls to `c::text` (`canon.py:541`), `$1,234.56`, and the driver
hands back that string. Fix: `c::numeric::text` and read `c::numeric`. **S.**
Recipe: PG `m money` (`lc_monetary=en_US.UTF-8`) row `1234.56`; MySQL
`decimal(19,2)` target -> today write error or DIFF; expected `same`.

**G18. float4 renders differently on PostgreSQL.** The float branch uses the
`real`'s own text, `0.1`, where MySQL's `cast(float as decimal(65,20))` and
every in-process reader see `0.10000000149011612`. Fix: `c::float8` first in
`_postgres` (`canon.py:505-528`). **S.** Recipe: PG `r real` and MySQL `r float`
both seeded `0.1`, `1.1`, `3.4028235e38`, `1e-40` (denormal); `check` -> today
DIFF on 3 rows (to measure); expected `same`.

**G19. MongoDB `int|long` left unmapped.** `type_class` treats two observed
types as ambiguous (`canon.py:464-472`); `int` and `long` are one integer.
Fix: merge `{int, long}` before the ambiguity rule; keep `double` mixed with
them ambiguous. **S.** Recipe: collection `{n: 1}` (int32) and
`{n: NumberLong("9000000000")}`; PG `bigint` target; `check` -> today `n` not
compared; expected compared, `same`.

**G20. ClickHouse `LowCardinality(Nullable(T))` not unwrapped.** `_unwrap`
strips `Nullable(` then `LowCardinality(` (`engines/clickhouse.py:16-23`); the
legal nesting is the other way round, so `Nullable(String)` is left and the
column is unmapped. The docstring's example `Nullable(LowCardinality(String))`
is a type ClickHouse rejects. Fix: loop until no wrapper matches. **S.**
Recipe: CH `k LowCardinality(Nullable(String))` rows `'a'`, NULL; PG target;
`check` -> today "no canonical rendering"; expected compared, `same`.

**G21. PostgreSQL digest hashes the database encoding's bytes.**
`md5({row_expr})` (`canon.py:1314`) hashes text in the server encoding; MySQL
and Python hash UTF-8. Fix: `md5(convert_to(row, 'UTF8'))`. **S.** Recipe:
`createdb -E LATIN1 -T template0 --locale=C d`; `s text` row `café`; MySQL
utf8mb4 target; `check` -> today DIFF; expected `same`.

**G22. SQL Server and ASE `CHAR(n)` come back padded.** Only Oracle and Db2
declare `PADDED` (`engines/oracle.py:46`, `engines/db2.py:37`) for
`_unpad` (`engines/dbapi.py:373`). Fix: `PADDED = ("char", "nchar")` on SQL
Server and ASE. **S.** Recipe: SQL Server `c char(10)` `'abc'`; PG `char(10)`
`'abc'`; `check` -> today DIFF; expected `same`.

**G23. PostgreSQL `json` with `\u0000` fails the whole digest.** `::jsonb`
(`canon.py:540`) raises. Fix: fold `json` (not `jsonb`) columns in process, or
`case` it to `UNCOMPARABLE` per row. **S.** Recipe: PG `j json` row
`'{"a":"\u0000"}'`; any target; `check` -> today the table errors; expected
1 uncomparable row, the rest compared.

**G24. MySQL JSON key order is not guaranteed.** `_mysql` renders JSON with
`cast(c as char)` (`canon.py:500`) and relies on it matching `json_text`
(`canon.py:583-619`). Fix: fold JSON in process on MySQL, or a per-server probe
that compares `cast(... as char)` with `json_text` for a fixed corpus
(non-ASCII keys, equal-length keys, numbers) before trusting it. **S.**
Recipe: MySQL 8.0, 8.4 and 9.x with `{"é":1,"z":2,"aa":3,"b":{"y":1,"x":2}}`;
compare against PG `jsonb` -> expected `same` on all three (measure).

### Missing refusals (loud failure mid-load, half-written target)

**G25. Wider-than-int64 integers.** `DDL[*]["integer"]` is 64-bit signed
everywhere (`canon.py:709,722,739,756,767,778,793,806,818,831,844,857,869`)
and `INT_RANGES` covers MySQL and PostgreSQL only (`canon.py:1144-1162`).
Fix: build from the source's range (`numeric(20,0)`, `decimal(20,0)`,
`NUMBER(20)`, `Decimal128`, `UInt64`); add ClickHouse, DuckDB, Cassandra, SQL
Server, Oracle ranges. **S.** Recipe: MySQL `u bigint unsigned` row
`18446744073709551615`; PG (created); `move` -> today stops on `bigint out of
range`; expected `numeric(20,0)` and `same`.

**G26. NUL, 4-byte characters, byte-limited targets.** No pre-scan exists for
U+0000 into PostgreSQL, 4-byte characters into utf8mb3/latin1, or UTF-16
code units into `nvarchar(n)` (`CHAR_TYPES` covers MySQL and PostgreSQL,
`canon.py:1164-1182`). **S.** Recipe: MySQL `s varchar(10)` rows `'a\0b'`,
`'😀'`; PG and a MySQL `utf8mb3` target; `move` -> today stops at the first
row; expected refusal with per-kind counts.

**G27. DynamoDB numbers.** `_to_attr` writes `repr(float)` and
`str(Decimal)` (`engines/dynamodb.py:220-224`): `nan`, `inf`, `5e-324`, more
than 38 digits are rejected by the service. **S.** Recipe: PG `f float8` rows
`'NaN'`, `5e-324`; `n numeric` row with 40 digits; `dynamodb-local` target;
`move` -> today `ValidationException` part-way; expected refusal before the
first write.

**G28. Instant vs wall clock beyond PostgreSQL and MySQL.** `TIME_MEANING`
(`canon.py:1108-1119`) knows two engines. Add SQL Server (`datetime2` wall,
`datetimeoffset` instant), Oracle (`DATE`/`TIMESTAMP` wall, `TSTZ` instant,
`LTZ` session), Snowflake (`NTZ`/`LTZ`/`TZ`), BigQuery (`DATETIME` wall,
`TIMESTAMP` instant), ClickHouse (instant, column zone), DuckDB, MongoDB and
Cassandra (instant). **S.** Recipe: PG `timestamptz` -> SQL Server
`datetime2` (created) -> expected the same warning PG -> MySQL `datetime` gets.

**G29. Cassandra `-0.0` keys and empty collections.** `_float_text` writes
`-0.0` as `0` (`canon.py:572-574`), so two distinct Cassandra keys render
alike; empty `list/set/map` are NULL. **S** (warn). Recipe: Cassandra `k double
primary key` rows `0.0`, `-0.0`; PG target -> expected a warning that two keys
render alike; PG `a int[]` `'{}'` -> Cassandra `list<int>` -> expected warning
"empty becomes NULL".

### New classes (the size of D15's gap)

| class | engines | rendering | effort | recipe |
|---|---|---|---|---|
| `uuid` | PG `uuid`, SQL Server `uniqueidentifier` (pymssql returns `uuid.UUID`, already lowercase), Cassandra `uuid/timeuuid`, ClickHouse `UUID`, DuckDB `UUID`, MySQL `BINARY(16)`/`CHAR(36)` by declaration, BSON Binary 3/4 | lowercase RFC text | M | PG `uuid` <-> MySQL `binary(16)` written with `UUID_TO_BIN(u,1)`, declared swap flag; <-> Mongo subtype 4 |
| `interval` | PG `interval`, Cassandra `duration`, Oracle `INTERVAL` pair, DuckDB `INTERVAL` | ISO 8601, three parts apart | M | PG `'1 mon 1 day 00:00:01.5'` <-> Cassandra `duration`; refuse into MySQL |
| `array` | PG arrays, MongoDB arrays, Cassandra collections, ClickHouse `Array`, DuckDB `LIST`, BigQuery `ARRAY`, JSON targets | JSON array by element class, sets sorted | M | PG `int[]`/`text[]`/`[0:1]` -> MySQL JSON, -> Mongo, -> Cassandra `list` |
| `geometry` | PostGIS, MySQL 8, SQL Server, Oracle SDO (not runnable here), MongoDB GeoJSON | ISO WKB lon-lat hex + SRID | L | PostGIS `POINT(100.5 13.75)` SRID 4326 -> MySQL 8 -> read back with `axis-order=long-lat`; expected no swap |
| `inet` | PG `inet/cidr`, ClickHouse `IPv4/IPv6`, Cassandra `inet`, MySQL text | RFC 5952 + `/n` | S | PG `inet '10.0.0.1'`, `'10.0.0.1/24'`, `'::ffff:1.2.3.4'` -> CH `IPv6` |
| `vector` | pgvector, MySQL 9 `VECTOR`, MongoDB arrays/Binary 9, Cassandra 5 `vector` | float32 list, float rule | M | pgvector `'[0.1,0.2]'` -> MySQL 9 `VECTOR(2)` (compare via binary, not `VECTOR_TO_STRING`) |
| `xml` | PG `xml`, SQL Server `xml`, Oracle `XMLType` | C14N 2.0 | M | PG `'<?xml version="1.0"?><a  b="1"/>'` -> SQL Server `xml` |
| `timestamp` at 9 places | all | 9-place text | M (with G3) | as G3 |

### Order of work

1. G1, G4, G2 - they can call wrong data equal (S each).
2. G7, G8, G9, G12, G25, G26, G27, G6 - pre-move counts that turn silent or
   mid-load loss into a refusal (mostly S; G8, G9 M).
3. G3 - sub-microsecond (M), with the 9-place timestamp.
4. G15-G24 - false differences (S each).
5. New classes: `uuid`, `array`, `interval`, `inet`, `vector`, `xml`,
   `geometry`.

Each fix lands with its recipe as a test in the existing style
(`tests/test_*_across_engines.py`), and the D15 count in
`test_how_much_of_a_row_is_compared.py` moves when a class lands.
