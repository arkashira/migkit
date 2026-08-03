# Behavior-critical database parameters

When you migrate between two servers, most settings are allowed to differ:
memory sizes, file paths, connection limits, buffer pools. Those are properties
of the box, not of your data.

A small set of settings is different. If they do not match between source and
target, the *same query can return a different answer*, or the *same data can be
stored or displayed differently*. Those are the ones worth comparing before a
cutover. `migkit check <hop> --only params` dumps every setting from both sides
to `params.json` and flags the ones in this list.

Grouped by the kind of breakage they cause.

## PostgreSQL

### Time and date
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `TimeZone` | `now()`, `current_timestamp`, `timestamptz` to text, `AT TIME ZONE` | A trigger or column default that calls `now()` stores a different wall-clock time; `timestamptz` values render shifted |
| `DateStyle` | Date parse and output format (ISO, MDY, DMY) | `'01/02/2024'` is read as February or January; text date output differs |
| `IntervalStyle` | `interval` to text | Interval values serialize differently in dumps and comparisons |

### Characters, sorting, indexes
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `server_encoding` / `client_encoding` | Database and session character encoding | Non-ASCII (Thai) text turns into mojibake or the copy fails on conversion |
| `lc_collate` | String sort order for `ORDER BY`, `<` / `>`, and **text indexes** | Rows sort in a different order; a text index returns wrong or missing rows (the classic collation-version bug) |
| `lc_ctype` | Character classification, `upper()` / `lower()` | Case folding and character class functions give different results |
| `lc_monetary` / `lc_numeric` / `lc_time` | `to_char()` and money formatting | Formatted output differs |

### Strings, binary, numbers
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `standard_conforming_strings` | Whether `\` in a string literal is literal or an escape | `'\n'` means a newline on one side and backslash-n on the other; data with backslashes is stored differently |
| `bytea_output` | `bytea` to text encoding (hex vs escape) | Binary values look different in dumps and checksums |
| `extra_float_digits` | Float precision when converting to text (0 vs 3) | Exports and checksums of `float`/`double` disagree; values look rounded |
| `backslash_quote` | Whether `\'` is accepted as a quote escape | Literal parsing and escaping behave differently |

### Semantics
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `default_transaction_isolation` | Read committed vs repeatable read | Transactions see different snapshots; concurrency anomalies differ |
| `search_path` | Schema resolution for unqualified names | An unqualified table or function resolves to a **different schema**, so you hit the wrong object |
| `array_nulls` | Whether `NULL` is recognized in array input | Array literals parse differently |
| `check_function_bodies` | Function body validation at create time | Restores that pass on one side fail on the other |
| `default_text_search_config` | Default full-text search config | `to_tsvector()` and FTS queries return different matches |

`wal_level` (replica vs logical) affects whether logical decoding / CDC is
possible, not the stored data. A managed target often runs `logical` for its own
replication; that difference is expected.

## MySQL

### Characters and collation (the most common data-corruption source)
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `character_set_server` / `character_set_database` | Default charset for new schemas and tables | latin1 vs utf8mb4 stores non-ASCII (Thai) wrong, truncates multibyte data, or returns mojibake |
| `character_set_connection` / `character_set_client` / `character_set_results` | Session encoding for statements and results | The same insert or select reinterprets bytes; round-trips corrupt non-ASCII |
| `collation_server` / `collation_database` / `collation_connection` | Sort order, case sensitivity, accent sensitivity | `ORDER BY` differs; `WHERE` matches differ; a unique index treats `é` and `e` as same or distinct |

### Time
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `time_zone` | Session zone for `NOW()`, `CURRENT_TIMESTAMP`, and `TIMESTAMP` (stored UTC) to local conversion | A trigger or default using `NOW()` stores a different time; `TIMESTAMP` columns convert to a different local time |
| `system_time_zone` | The OS zone the server picked up (used when `time_zone = SYSTEM`) | Shifts every `TIMESTAMP` conversion when the session zone is SYSTEM |

### Semantics
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `sql_mode` | Strictness and SQL dialect (STRICT, `NO_ZERO_DATE`, `ONLY_FULL_GROUP_BY`, `PIPES_AS_CONCAT`, `ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, `PAD_CHAR_TO_FULL_LENGTH`) | Data that inserts fine on one side is rejected or silently truncated/zeroed on the other; `\|\|` means OR vs concat; `"x"` is an identifier vs a string; `GROUP BY` queries behave differently |
| `lower_case_table_names` | Table-name case sensitivity (0/1/2) | `Orders` and `orders` are the same table or two tables; migration loses rows or hits a name collision |
| `explicit_defaults_for_timestamp` | `TIMESTAMP` default and nullability behavior | `TIMESTAMP` columns get different implicit `DEFAULT CURRENT_TIMESTAMP` / `ON UPDATE` behavior, so values diverge |
| `transaction_isolation` | Repeatable read vs read committed | Different snapshot visibility and concurrency behavior |
| `default_storage_engine` | Engine for new tables (InnoDB vs MyISAM) | New tables lose transactions and foreign keys |

### Limits that can block or truncate data
| Parameter | Controls | Failure if it differs |
|---|---|---|
| `max_allowed_packet` | Largest statement / row | A large row or BLOB that inserts on the source fails on a smaller target |
| `group_concat_max_len` | `GROUP_CONCAT` result length | Concatenated results are silently truncated |
| `version` | Server build (for example 8.0.42 vs 8.0.30-txsql) | A minor version or vendor fork explains subtle behavior differences |

## MongoDB

Server-level parameters are less meaningful to compare than in SQL engines, and
comparing a real MongoDB against AWS DocumentDB is not useful because they are
different engines that share almost no internal parameters. The settings that
actually change behavior are mostly per-collection or per-operation.

| Setting | Controls | Failure if it differs |
|---|---|---|
| Collection **default collation** (per collection, not server) | locale, strength, caseLevel for string sort, compare, and uniqueness | `ORDER BY`-style sorts differ; a unique index treats `é` and `e` as same or distinct; range queries match differently |
| `featureCompatibilityVersion` (FCV) | Which server features and index types are enabled | An index type or aggregation stage available on one side is missing on the other |
| `readConcern` / `writeConcern` defaults | Read consistency and write durability | Writes acknowledge with different durability guarantees |
| Balancer and chunk settings (sharded) | Data distribution across shards | Uneven distribution or orphaned documents |
| Oplog size / retention | CDC and resume-token window | Change streams cannot resume after a gap |

Dates in MongoDB are always stored in UTC and the time zone is applied at query
time (for example `$dateToString` with a `timezone`), so time-zone drift breaks
less than it does in SQL, as long as the tz database is present on both sides.

## What migkit flags today

`migkit check <hop> --only params` writes the full `params.json` (every setting,
both sides) and marks a diff only when a behavior-critical setting differs. The
currently flagged set is the PostgreSQL and MySQL tables above, plus
`featureCompatibilityVersion` for MongoDB. Everything else is written to disk and
counted, so the check stays signal rather than noise.
