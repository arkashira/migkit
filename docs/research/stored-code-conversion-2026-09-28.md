# Schema and stored-code conversion across engines, and proving the result behaves the same (research as of 2026-09-28)

**Status of this pass: COMPLETE** (2026-09-28) - sections 0-6 written against the sources listed at the end. Marked *(inferred)*, *(to be measured)* or *(not found)* where no primary page confirmed a point: Ora2Pg's internal rewrite mechanism, SQLines' internals, EvoSQL's licence, QED's prover licence, XEvents on SQL Edge, arm64 availability of Microsoft's ODBC driver, the PostgreSQL clock-shadowing technique and libfaketime under both servers. Nothing in the brief was left unresearched.

Scope: backlog R11 "Stored code converted and proved" (`docs/backlog.md:4030`) and item 39 (`:3313`), and the "Where the paid tools and the clouds still lead" section (`:2425`). Public documentation, papers and source only. Statements not confirmed from a primary page are marked *(inferred)* or *(not published)*. migkit line references are to the working tree on 2026-09-28.

---

## 0. What migkit has today

| Piece | Where | What it does | What it does not |
|---|---|---|---|
| Neutral DDL for tables | `migkit/canon.py:449` `type_class`, `:903` `ddl_type`, `:1259` `comparable`; `engines/hetero.py:2044` `convert_ddl`, `:2140` `converted_objects` | Tables built on the target from a class per column (no regex over generated SQL) | Nothing for sequences-as-objects, check constraints beyond the carried rules, partitions, collations per column beyond the class |
| Views | `engines/hetero.py:2160` `converted_code` | sqlglot `parse_one(read=src)` then `.sql(dialect=dst, unsupported_level=RAISE)`; qualifiers rewritten by `local()`; views ordered by dependency | A view whose text sqlglot parses but whose *meaning* differs (collation, NULL ordering, implicit casts) is only caught by the proof |
| Functions | same, `neutral_functions` per engine: `mysql.py:2933`, `postgres.py:3893`, `mssql.py:758`, `oracle.py:122` | Only a body that is **one expression** (`RETURN expr`, `BEGIN RETURN expr END`, `language sql ... select expr`); declared decimal scale re-stated as a cast | Every body of statements, every procedure, every trigger and event: written as a comment naming it (`-- function X not converted: its body is statements`) |
| Model proposals | `migkit/assist.py:112` `propose`, `hetero.py:2284` `_proposed` | Where the translator fails and `MIGKIT_AI_SHARE` includes `code`, a model's CREATE is written, marked `PROPOSED` (`hetero.py:2281`) | One shot, no repair loop, no feedback from the proof into the prompt |
| Proof | `hetero.py:2315` `prove_converted`, `:2398` `_call`, inputs `:2304` `PROOF_INPUTS`, cap `:2313` `PROOF_CALLS = 200` | View: digest of its rows both sides. Function: every combination (capped at 200) of a fixed pool per class (null, edges, a rounding decimal, case, trailing space, a wide character, a leap day), one read-only `select f(..), f(..)` per side, answers rendered by `canon.render_value` | Procedures, triggers, events: **not executed**; side effects not captured; errors per input not separated (one failing call fails the whole select on that side); inputs not drawn from the column's real values or constraints; no coverage measure |
| Inventory of work left | `migkit/handwork.py:39` kind `server-side-code` | Counts and names routines/triggers/views a mover does not carry; refuses to estimate effort (docstring, lines 11-17) | No per-object difficulty, no cost units |
| Revert | `migkit/revert.py` | The inverse of a repair script, generated from the same two snapshots; irreversible statements named | Nothing for routines (a `CREATE OR REPLACE FUNCTION` has no revert that restores the previous body) |
| Schema check | `hetero.py:179` `check_schema` | Columns by name and class; missing tables | Routines/triggers/events are not part of it; `prove_converted` is the only check on code |
| Decision layer | `migkit/movers.py:36` `pick`, `migkit/planner.py:55` `plan`, backlog P0 ladder (`docs/backlog.md:150-170` names "the stored-code converter per routine in R11" as a rung list) | Chooses mover per table | No rung list for converters yet |

**A verdict bug found while reading.** `prove_converted` returns `ok` with the sentence "N procedures are on the target, and answer nothing to compare" (`hetero.py:2388-2396`). If every source routine is a procedure, the result is `ok` with "0 views and functions answer the same" - a green verdict with no execution evidence at all. The same holds for triggers and events, which are not looked at. R11's rule "never ok without execution evidence" means this becomes `warn`/`skip` with the count, until procedures are executed (section 6).

**A proof weakness found while reading.** `_call` sends all calls in one `select`; one call that raises on the target makes the whole side `("failed", msg)` and the function is `diff` with no input named, and one that raises on the source makes it `unread` (`hetero.py:2357-2366`). Differential testing needs a per-input outcome - value *or* error class - on both sides, and a verdict that names the first differing input.

---

## 1. The converters: what each converts, how, what it claims, licence

### 1.1 AWS SCT and DMS Schema Conversion (rules + generative AI)

**Converts.** Tables, indexes, constraints, views, functions, procedures, packages, triggers, sequences, jobs, between many pairs (Oracle, SQL Server, Db2 LUW/zOS, SAP ASE, MySQL, PostgreSQL, Teradata, Netezza, Greenplum, Vertica, Snowflake...). SCT is a desktop Java tool; DMS Schema Conversion (DMS SC) is the same engine as a managed service, with fewer pairs.

**Mechanism.** A published four-stage model (AWS calls it "a practical workflow model rather than a description of private service implementation details"): **parse** to an AST (failure = action item 9998), **resolve** references across schemas (failure = 9997), **transform** by deterministic rules, then, if enabled, **generate** with a Bedrock model for constructs that no rule covers. The AI stage is scoped to a published list of action items per pair - e.g. Oracle->PG 5102 (MERGE), 5073/5043 (hierarchical queries), 5121 (FORALL), 5645 (BULK COLLECT into object tables), 5651 (pipelined table functions), 5665 (collections under `PRAGMA AUTONOMOUS_TRANSACTION`); SQL Server->PG 7628 (GOTO), 7637/7639 (global/dynamic cursors), 7672 (`EXEC` of a string), 7819/7929 (`INSERT ... EXEC`), 7829 (variable assignment in UPDATE), 7833 (`@@ROWCOUNT` in context), 7909 (`UPDATE(col)`/`COLUMNS_UPDATED`), 7916 (MERGE as `INSERT ON CONFLICT`), 7918 (table-valued functions). Not in AI scope: triggers, dynamic SQL, defaults, computed columns, column types, indexes, constraints. When any item of a statement went through the model, all its items are replaced by one INFO item (5444 Oracle, 7744 SQL Server) marking it as AI-generated. Quota: 48-80 statements per account per minute.

**Assessment.** Each action item has a complexity (Simple < 2h, Medium 2-6h, Significant > 6h in the current guide; an older guide said < 1h, 1-4h, > 4h); an object takes the highest of its items, or is raised when the sum of simple items passes the complex threshold. The CSV has "learning curve effort" (designing the approach once) and "effort to convert an occurrence" as weighted scales. The multi-server report gives three percentages (code objects, storage objects, syntax elements converted) and a 1-10 complexity score.

**How the output is checked.** "The engine validates that AI-generated output is syntactically valid PL/pgSQL. It doesn't validate semantic correctness" (AWS Database Blog, SQL Server to Aurora PostgreSQL with AI agents). "A procedure that compiles successfully is not guaranteed to produce semantically identical results." The limitations page: "may not achieve 100 percent accuracy ... can also produce different results for the same SQL statements over a period of time." For tests AWS points to Amazon Q Developer generating "comparable test cases" per object (insert/update/delete/mixed/empty-source scenarios comparing final table states on both platforms), with the caveat that "the number and scope of test cases generated can vary between different runs." No execution harness is part of DMS SC. Claim: "up to 90%" of a schema converted.

**Licence.** Proprietary; SCT free to download, DMS SC free for conversion, Bedrock usage inside the service.

**migkit: wrap or build.** Neither can be wrapped (desktop GUI / managed API, source code not public, pairs to PostgreSQL mostly). What to take: the **action-item vocabulary as a per-construct inventory** (a code, a construct, the object and line, a complexity class), and the honest split between "parsed", "resolved", "rule-converted", "model-proposed" - which migkit's `converted_code` collapses into converted-or-comment.

### 1.2 Google Database Migration Service: conversion workspaces with Gemini

**Converts.** Oracle and SQL Server to Cloud SQL for PostgreSQL / AlloyDB: schema and code objects (procedures, functions, triggers, packages). Legacy workspaces were driven by Ora2Pg configuration files - Google's first converter was Ora2Pg.

**Mechanism.** Deterministic rules for 1:1 mappings, then Gemini "contextual synthesis" for procedural blocks, with the workspace holding the whole source's metadata (tables, types, FKs, cross-procedure dependencies) as context. Features: auto-conversion on top of the deterministic result, a conversion assistant (prompts: explain, "help me fix object conversion issues", optimise), **code conversion suggestions** that learn from the user's fixes and propose the same fix for other failing objects, and **quality assessments** where Gemini reviews output and raises new conversion issues. Then "apply to a staging instance for functional execution and performance testing".

**How the output is checked.** Model review of the output (a model grading a model) plus the target accepting the DDL. Functional execution is left to the user on a staging instance. Google: output "can seem plausible but is factually incorrect. We recommend that you validate all output". Code may be processed outside the workspace's region.

**Licence.** Proprietary managed service; Gemini priced separately.

**migkit.** Take the **"fix once, propagate"** idea: a correction a person makes to one routine becomes a rule candidate for the same pattern elsewhere - in migkit that is a user rule in the converter's rule table, proved like every other rule (section 6).

### 1.3 Microsoft: SSMA and its Tester; the PostgreSQL VS Code extension

**SSMA** (Oracle, Db2, MySQL, SAP ASE, Access -> SQL Server / Azure SQL). Rules plus a run-time emulation layer: an extension pack installs `sysdb` and an `ssma_oracle` schema of functions emulating Oracle built-ins, package variables kept in `ssma_oracle.db_storage` keyed by session id and login time, each converted package procedure starting with `db_check_init_package`; Oracle functions that do DML become a `$impl` procedure called through an extended procedure from a UDF wrapper. The assessment report gives a "conversion rate" (percentage of statements converted automatically) and failures per object. Proprietary, free.

**SSMA Tester - the one vendor tool that executes both sides and compares.** It "executes objects selected for testing on Oracle and their counterparts in SQL Server" and compares: changes in table data, output parameter values, function return values, result sets. Inputs are **typed in by the user** per call (*Call Values*: "Add Call" adds a call with empty parameter values); the user also picks the "affected objects" (tables and FKs whose changes are compared) and can edit the comparing SELECTs. Per column/parameter comparison settings: exclude, custom numeric scale, date-only / time-only / ignore milliseconds, ignore case, ignore trailing spaces. Table changes are compared row by row by an added `ROWID` column (option *Generate ROWID column* must be set before conversion). It creates an `SSMATESTER_ORACLE` schema on the source and warns: "Never use SSMA Tester on production systems. During Tester execution the source schema and data are modified. The complete restoration of the original state may be impossible." One user at a time.

**PostgreSQL extension for VS Code, Oracle migration** (GA May 2026). A Foundry model deployment in the user's subscription converts DDL and PL/SQL (packages with package state, procedures, functions, triggers, object types, synonyms); each converted object is **compiled in a scratch schema** on a PostgreSQL server and then checked with **`plpgsql_check`** - a routine that compiles but reads a column that does not exist fails there and goes back to the model's fix loop; only routines that pass are written out. Unconvertible items become review tasks for Copilot agent mode. Microsoft: "AI systems can occasionally confirm their own mistakes ... independently validate all converted objects". The executable tests in Microsoft's own Swingbench write-up were written with Copilot and by hand, and found that PostgreSQL functions cannot `COMMIT` where the Oracle package did. A public lab (kloba/oracle-to-postgres-migration-lab) compiles each repair in a disposable PostgreSQL 16 container, runs `plpgsql_check`, then SQL assertions, and names "plpgsql_check being fail-open" as the most dangerous configuration mistake.

**migkit.** Take from SSMA Tester: **the four comparisons** (table changes, OUT parameters, return values, result sets) and **per-column tolerance as explicit, recorded settings** - but generate the inputs instead of asking for them, and never run on the source (section 5). Take from the VS Code tool: **compile + `plpgsql_check` as a gate before any execution**, failing closed. `plpgsql_check` is an open extension (MIT-style licence, packaged for PostgreSQL 12+); it runs in the docker PostgreSQL.

### 1.4 Ora2Pg

**Converts.** Oracle (and, with `-m` / `-M`, MySQL/MariaDB and SQL Server) to PostgreSQL: tables, views, sequences, indexes, constraints, types, functions, procedures, packages (as schemas, `PACKAGE_AS_SCHEMA`), triggers, grants, partitions, synonyms, DB links (as FDW), plus data (`INSERT`/`COPY`, or through `oracle_fdw`/`mysql_fdw`/`tds_fdw`). PL/SQL to PL/pgSQL with `-p`/`PLSQL_PGSQL`. Perl, **GPL-3.0**.

**Mechanism.** Regular-expression and token rewriting over the extracted source text (not a full PL/SQL grammar) - which is why its conversion is fast and wide but its failures are syntactic surprises rather than refusals *(inferred from the source layout; Ora2Pg's `Ora2Pg/PLSQL.pm` is a rewrite library)*. Specific devices: `PRAGMA AUTONOMOUS_TRANSACTION` (directive `AUTONOMOUS_TRANSACTION`, from v19.0) becomes a wrapper function calling the renamed original `<name>_atx` through **dblink** (default) or **pg_background** (`PG_BACKGROUND`, PostgreSQL 9.5+; faster above ~10 concurrent clients in Dalibo's benchmark); the dblink wrapper puts the connection string, password included, in the function body (`DBLINK_CONN`), and a 2021 issue (#1262) reports wrong output for such procedures inside packages. NUMBER: `PG_NUMERIC_TYPE` keeps `numeric(p,s)`, `PG_INTEGER_TYPE` maps scale-0 numbers to `smallint/integer/bigint`, `DEFAULT_NUMERIC` (bigint) for bare `NUMBER`.

**Assessment (`SHOW_REPORT --estimate_cost`).** A cost unit is "a fixed amount of work for a PostgreSQL expert", **5 minutes** by default (`COST_UNIT_VALUE`), summed per object from built-in weights per construct; migration level letter A (automatic), B (rewrite up to `HUMAN_DAYS_LIMIT`, default 5 person-days), C (above); technical level 1 trivial (no functions, no triggers) to 5 difficult (functions and/or triggers needing rewrite). The report covers database objects only - not application SQL.

**How the output is checked.** `-t TEST` counts objects per type on both sides (tables, views, sequences with their last value, types, per table: indexes, unique/PK/check/NOT NULL constraints, defaults, identity, FKs, triggers, partitions, columns) and **only a count** of packages/functions/procedures; `TEST_COUNT` rows; `TEST_VIEW` row counts of views; `TEST_DATA` compares the first `DATA_VALIDATION_ROWS` (10,000) rows per keyed table through FDW, stopping after 10 errors, ordered by the key (fails where the PostgreSQL key column's collation is not `C`). **No behavioural test of converted code.**

**migkit: wrap.** It is what backlog 11 and R11 already say: run `ora2pg` as a program (Perl + DBD::Oracle/DBD::mysql/DBD::ODBC; a container image does it with no install on the host) for the conversion and the cost units, and hold every object it writes to migkit's proof. Take the cost units into `handwork.py` only as Ora2Pg's figure, labelled as Ora2Pg's, beside migkit's measured count - the handwork docstring's refusal to invent a table stands.

### 1.5 Ispirer (SQLWays / Ispirer Toolkit)

Commercial, proprietary. "Parser and compiler-based engine rather than regex pattern matching", extensible with customer-specific rules that Ispirer engineers add ("typically within 3-5 business days"). Published automation rates vary by page: 90-95%+ on supported paths; 75-95% of SQL code; SQL Server->PostgreSQL ~70-80%, "up to 90% with AI". A free InsightWays assessment estimates cost and time. Behavioural testing is part of Ispirer's *service* ("tested against source Oracle test cases using function calls, queries, and data snapshots"; scenarios written where none exist), not a tool. Nothing to wrap. What it confirms: the conversion market's differentiator is **per-customer rules added fast**, and testing is sold as people.

### 1.6 SQLines

SQLines SQL Converter (C++) converts DDL, queries, views, procedures, functions, packages, triggers between SQL Server, Oracle, MySQL/MariaDB, PostgreSQL, Db2, Sybase, Informix, Teradata, Greenplum, Netezza; SQLines Data moves data and validates it. The GitHub repository (`dmtolpeko/sqlines`) says **Apache-2.0** (older 2011 downloads said GPL-3). Token-level rewriting with per-pair rule tables *(inferred from the source tree: `sqlines/` has per-construct `.cpp` files - `proc.cpp`, `func.cpp`, `datatypes.cpp`)*. No published accuracy figure and no behavioural test. **migkit: a candidate rung** for T-SQL and MySQL procedural bodies where sqlglot has no procedural generator (1.8) - run as a program, output held to the proof. Measured quality unknown: it earns its rung only by the proof's pass rate on the fixture corpus (section 6).

### 1.7 EDB Migration Portal, Babelfish Compass, CockroachDB MOLT Convert, YugabyteDB Voyager

* **EDB Migration Portal** (free with an EDB account; proprietary): upload Oracle DDL, "repair handlers" rewrite known incompatibilities into EDB Postgres Advanced Server (which runs much PL/SQL natively), a **compatibility percentage** = compatible DDL statements / all DDL statements after the handlers, a 1-5 complexity level, a knowledge base of workarounds, lately an "AI Copilot". Known gaps: unsupported objects are removed silently and not counted; wrapped objects are not assessed. EDB itself: "a compatibility score of 95% isn't necessarily easier to migrate than one with a score of 70%." Lesson for migkit: a percentage over statements hides what was dropped - migkit's count must include what it removed.
* **Babelfish Compass** (Apache-2.0, Java): classifies every T-SQL feature found in DDL/SQL as Supported, NotSupported (the default for anything not listed), ReviewSemantics, ReviewPerformance, ReviewManually or Ignored, from a versioned `BabelfishFeatures.cfg`. `-rewrite` rewrites a few unsupported features (MERGE, DATEADD/DATEDIFF units, default parameter values in calls, multi-column ALTER TABLE ADD) into a `rewritten/` directory, never inside dynamic SQL. It is a feature inventory for one target (Babelfish), not a converter to plain PostgreSQL. **Take the classification vocabulary**, especially *ReviewSemantics* ("will run but may mean something else"), which is exactly the class migkit's proof exists for.
* **CockroachDB MOLT Convert / Schema Conversion Tool** (Cloud console, proprietary): parses a schema dump from PostgreSQL, MySQL (connected: runs `pg_dump`/`mysqldump`), Oracle or SQL Server (uploaded, < 4 MB), reports errors, incidental errors, incompatible statements, compatibility notes and best-practice suggestions, bulk fixes, then creates the database. Schema only; routines are listed as incompatible rather than converted.
* **YugabyteDB Voyager** (Apache-2.0, Go): `assess-migration` and `analyze-schema` parse the `pg_dump` schema and the source's frequent queries (`pg_stat_statements`) with a Go binding of the PostgreSQL parser and walk the tree for unsupported nodes (advisory locks, system columns, XML functions, JSON_TABLE, MERGE, `UNIQUE NULLS NOT DISTINCT`, ...), inside PL/pgSQL bodies too, rating complexity LOW/MEDIUM/HIGH, per `--target-db-version`. Its own limit: statements embedded "in expressions, conditions, assignments, loop variables, function call arguments" of PL/pgSQL are not seen. Oracle/MySQL schema export went through Ora2Pg; that offline path is deprecated after 2026-10-13. **Take: the parse-tree walk against a versioned list of unsupported nodes per target version** - migkit has `pglast` (libpg_query, including `parse_plpgsql`) already in its virtual environment (`.venv`, pglast 8.4), so the same walk is available in-process for PostgreSQL sources and for checking converted PL/pgSQL.

### 1.8 sqlglot (migkit's current translator)

MIT. 30+ dialects (MySQL, PostgreSQL, T-SQL, Oracle, SQLite, DuckDB, Snowflake, BigQuery, ...). A parser to one AST, per-dialect generators, `unsupported_level` (IGNORE/WARN/RAISE/IMMEDIATE), an **optimizer** (qualify tables/columns against a schema, `annotate_types`, normalize, simplify, pushdown, unnest subqueries, canonicalize), **lineage** (`sqlglot.lineage`: which source columns feed an output column), a **semantic diff** of two ASTs (Insert/Remove/Move/Update/Keep), and a Python **executor** over dicts ("designed for testing rather than performance"). The README: "SQLGlot is a transpiler, not a validator".

Measured by reading the installed version (sqlglot 30.18.0 in `.venv`):
* Procedural code: the base generator's `storedprocedure_sql`, `ifblock_sql`, `whileblock_sql`, `loopblock_sql`, `casestatement_sql` all call `self.unsupported(...)` and return an empty string; only the T-SQL, Trino and Spark generators override them. So **no procedural body can be transpiled to PostgreSQL, MySQL or Oracle** - with `RAISE` migkit refuses, which is what it does today. Parsing an unparseable statement falls back to `exp.Command` (the text kept, nothing transpiled) with a warning; Oracle anonymous blocks raise `ParseError` (issue #1356, closed as not planned). A third-party comparison (a commercial parser vendor, so partial) found sqlglot's T-SQL `CREATE PROCEDURE` with TRY/CATCH became a `Command`, and Oracle `BULK COLLECT`/`FORALL` failed to parse.
* **`CONNECT BY`** is parsed (`exp.Connect`, `exp.Prior`) and generated back verbatim by the base `connect_sql` for any dialect, PostgreSQL included, **with no unsupported warning** - so `RAISE` does not stop it; PostgreSQL rejects the view at `CREATE`. Only DuckDB has a rewrite for `CONNECT_BY_ROOT`.
* Oracle `(+)` outer joins: `sqlglot.transforms.eliminate_join_marks` exists but no generator applies it; migkit must call it before generating PostgreSQL.
* `MERGE` to PostgreSQL goes through `merge_without_target_sql` (PostgreSQL 15+ has `MERGE`); `TRY_CAST` through `no_trycast_sql` (becomes a plain `CAST` - an error where T-SQL returned NULL: a semantic change the proof must catch).

**migkit: keep sqlglot as the expression-level rung** (views, one-expression functions, the SQL statements *inside* a procedural body once something else has split the body into statements), add `eliminate_join_marks` and a guard for `exp.Connect`, and never use it as the procedural converter.

### 1.9 sqlfluff, pglast/libpg_query, and other parsers

* **sqlfluff** (MIT, Python): a linter/formatter with dialect grammars for T-SQL, Oracle (PL/SQL partly), MySQL, PostgreSQL and others; parses procedure bodies into a tree migkit already considered for finding `COLLATE` clauses (backlog R9, `docs/backlog.md:3999-4001`). It does not transpile. Use: a second parser to **split a procedural body into statements and control structures** where sqlglot returns `Command`, so each embedded SQL statement can be handed to sqlglot. Slow on large bodies (pure-Python grammar) *(inferred; widely reported)*.
* **pglast** over **libpg_query** (BSD-3): the real PostgreSQL parser, `parse_sql` and `parse_plpgsql` (the PL/pgSQL function AST as JSON). Use: verify converted PL/pgSQL parses as the server would, walk it for unsupported nodes per target version (Voyager's method), and count statements/branches for coverage (section 4). Installed in `.venv` (8.4, pulled in by another package, not listed in `pyproject.toml`). **Its licence is `GPL-3.0-or-later`** (read from its `METADATA`) and migkit is MIT (`pyproject.toml:7`): import it only in a separate process the way `runners/second_reader.py` isolates the second reader, or use the PostgreSQL server itself (`plpgsql_check`, `pg_get_functiondef`) and libpg_query's BSD bindings instead.
* **ANTLR grammars** (`antlr/grammars-v4`: PL/SQL, T-SQL, MySQL, PostgreSQL, BSD-style licences) - full procedural grammars; heavy (Java or generated Python) and a converter would still have to be written on top. Not recommended unless sqlfluff's trees prove insufficient.

---

## 2. The rules per pair: what changes meaning without an error

The table in each subsection is the rule list migkit's own layer (section 6, rung "migkit rules") has to hold, and - more important - the **proof inputs** each rule implies: every row names a behaviour that compiles on both sides and answers differently, so the input generator must reach it.

### 2.1 pgloader's cast rules (MySQL, SQLite, MS SQL -> PostgreSQL)

pgloader (PostgreSQL licence, Common Lisp) converts **tables, not code**: its docs say views would need "a full SQL parser for the MySQL dialect" and that triggers' "difficulty ... is not yet assessed"; procedures are not migrated. Default MySQL casts: `tinyint(1)` -> `boolean` (`tinyint-to-boolean`), unsigned integers widened one size, `auto_increment` -> `serial`/`bigserial`, `enum` -> a named type per column, zero dates -> NULL (`zero-dates-to-null`) with the `'0000-00-00'` default dropped, NUL bytes removed from text. **`ON UPDATE CURRENT_TIMESTAMP`** becomes a generated `BEFORE UPDATE` trigger: function `on_update_current_timestamp_<column>()` setting `NEW.<column> = now()`, trigger `on_update_current_timestamp` per table (issue #735 shows the exact SQL; there is no switch to turn it off). The cast rules are a user-extensible DSL (`CAST type datetime to timestamptz drop default drop not null using zero-dates-to-null`).

**migkit already has** the table half in `canon.py` (`ddl_type`, `capacity`, `narrower`) and a zero-date check (`hetero.py:625` `_zero_date_result`). What it lacks is the **generated trigger for `ON UPDATE CURRENT_TIMESTAMP`** - a behaviour, not a type, so it belongs in the stored-code converter and is proved like one (update a row without touching the column, compare the column's change on both sides, with the clock frozen - see 4.6).

### 2.2 MySQL -> PostgreSQL

| Construct | Rule | Silent difference the proof must reach |
|---|---|---|
| `DECLARE CONTINUE HANDLER FOR SQLEXCEPTION` | per-statement `BEGIN ... EXCEPTION ... END` | a PL/pgSQL exception block is a subtransaction: **the failing block's writes are rolled back** where MySQL kept the earlier statements' writes; >64 subtransactions per transaction slows everything |
| `DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1` + cursor loop | `FOR rec IN query LOOP` or `FETCH ...; EXIT WHEN NOT FOUND` | a `SELECT ... INTO` with no row sets `done` in MySQL; in PL/pgSQL it leaves the variable NULL and `FOUND` false - the handler also fires on that |
| `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT=...` | `RAISE EXCEPTION ... USING ERRCODE, MESSAGE` | SQLSTATE class `01` is a warning in MySQL -> `RAISE WARNING`, not an error |
| `LAST_INSERT_ID()` | `INSERT ... RETURNING ... INTO` | `lastval()` sees a sequence a trigger used; MySQL returns 0 where nothing was inserted, `lastval()` errors |
| `GROUP_CONCAT(x ORDER BY y SEPARATOR s)` | `string_agg(x::text, s ORDER BY y)` | MySQL truncates at `group_concat_max_len` (1024 bytes default) **silently** |
| `IFNULL(a,b)` | `coalesce(a,b)` | `coalesce(int, '')` fails; MySQL coerced |
| string/number comparison, `'1abc' = 1` | explicit cast | MySQL coerces and warns; PostgreSQL errors |
| default `_ci` collations | `citext` or a nondeterministic ICU collation | equality and `DISTINCT`/`GROUP BY` fold case in MySQL; not in PostgreSQL by default |
| `ON UPDATE CURRENT_TIMESTAMP` | generated `BEFORE UPDATE` trigger (2.1) | MySQL only re-stamps when some column value **actually changed**; the pgloader trigger re-stamps on every UPDATE, including no-op ones *(MySQL semantics from its manual; the gap is inferred)* |
| zero dates, `0000-00-00` | NULL, or a sentinel | comparison and `IS NULL` differ afterwards |
| `unsigned` | wider type + `CHECK (x >= 0)` | overflow at 2^64-1 for `bigint unsigned` needs `numeric(20)` |
| `EVENT ... ON SCHEDULE EVERY` | `pg_cron` job calling a procedure | `pg_cron` needs `shared_preload_libraries` (restart) and runs in GMT unless `cron.timezone`; `AT <timestamp>` one-shot events have no form |
| `LENGTH` (bytes) | `octet_length` | already measured by migkit: sqlglot maps it to `length` (characters) (`docs/backlog.md:3442-3444`) |
| `DECIMAL(p,s)` function args/returns | cast in the body | already measured: PostgreSQL ignores the declared scale (`docs/backlog.md:3445-3447`) |
| procedure returning a result set (`SELECT` at the end) | function `RETURNS TABLE` / `SETOF`, or a `refcursor` | the caller's API changes - not provable in the database alone |
| triggers with `NEW.col` assignment in `BEFORE` | trigger function returning `NEW` | MySQL allows one trigger per timing/event before 5.7.2 and orders by `FOLLOWS/PRECEDES`; PostgreSQL fires alphabetically by name |

### 2.3 SQL Server (T-SQL) -> PostgreSQL

| Construct | Rule | Silent difference |
|---|---|---|
| `IDENTITY(1,1)` | `GENERATED BY DEFAULT AS IDENTITY` | seed/increment kept; `SET IDENTITY_INSERT` has no counterpart, `OVERRIDING SYSTEM VALUE` for `ALWAYS` |
| `SCOPE_IDENTITY()`, `@@IDENTITY` | `RETURNING ... INTO` | `@@IDENTITY` sees triggers' inserts, `SCOPE_IDENTITY` does not |
| `BEGIN TRY ... END TRY BEGIN CATCH ... END CATCH` | `BEGIN ... EXCEPTION WHEN OTHERS THEN ... END` | the TRY block's writes are rolled back in PostgreSQL; in T-SQL they stay unless the transaction is doomed (`XACT_STATE() = -1`) |
| `SET XACT_ABORT ON/OFF` | dropped | PostgreSQL always aborts the whole transaction on an error; OFF-dependent code (continue after an error) needs savepoints |
| `ERROR_MESSAGE()`, `ERROR_NUMBER()`, `THROW`, `RAISERROR` | `SQLERRM`, `SQLSTATE`, `GET STACKED DIAGNOSTICS`, `RAISE` | error numbers do not map to SQLSTATEs one to one - an error class comparison, not a number comparison |
| `@@ROWCOUNT` | `GET DIAGNOSTICS n = ROW_COUNT` / `FOUND` | `@@ROWCOUNT` is reset by almost any statement, including `IF`; order matters |
| `#temp` tables | `CREATE TEMP TABLE ... ON COMMIT DROP` or `PRESERVE ROWS` | T-SQL `#t` is dropped at the end of the procedure that made it; PostgreSQL's lives to the end of the session (a second call fails with "already exists") |
| `SELECT ... INTO #t` | `CREATE TEMP TABLE t AS SELECT` | `SELECT INTO` in PL/pgSQL means variable assignment |
| table variables `@t TABLE(...)` | temp table, array of a composite type, or a CTE | **table variables are not rolled back** by a transaction rollback in T-SQL; temp tables are |
| `MERGE` | `MERGE` (PostgreSQL 15+) or `INSERT ... ON CONFLICT` | `ON CONFLICT` needs a unique index and does not do `WHEN NOT MATCHED BY SOURCE` (PostgreSQL 17 added `BY SOURCE`); MERGE with duplicate source keys errors in both but for different reasons |
| `TOP (n)` in `UPDATE`/`DELETE` | `ctid IN (SELECT ctid ... LIMIT n)` | T-SQL's choice of rows is arbitrary - the proof must compare the *count* of affected rows, not which |
| procedure result sets (one or many) | `RETURNS TABLE`, or `SETOF refcursor` (Google DMS: many result sets -> `SETOF refcursor`, return value in a cursor named `return_value`) | refcursors vanish outside the transaction |
| `TRY_CAST`, `TRY_CONVERT` | a function that catches and returns NULL | sqlglot turns `TRY_CAST` into `CAST` (1.8) - an error where T-SQL gave NULL |
| `ISNULL(a,b)` | `coalesce(a,b)` | `ISNULL` takes the type of `a` and truncates `b` to it |
| `+` string concatenation | `||` or `concat` | `NULL + 'x'` is NULL under `CONCAT_NULL_YIELDS_NULL ON` (default); `concat` ignores NULL |
| `DATETIME` arithmetic, `GETDATE()` | `timestamp`, `now()` / `clock_timestamp()` | `GETDATE()` changes during a procedure; `now()` is fixed at transaction start |
| collation `SQL_Latin1_General_CP1_CI_AS` | ICU nondeterministic collation | case-insensitive equality by default |
| empty string vs NULL | same in both | - (only Oracle differs) |

### 2.4 Oracle PL/SQL -> PostgreSQL

| Construct | Rule (Ora2Pg's where it has one) | Silent difference |
|---|---|---|
| `''` | stays `''` | **Oracle treats `''` as NULL**: `'' IS NULL` true, `'a' || NULL = 'a'`, a NOT NULL column refuses `''`; PostgreSQL does none of these |
| `DATE` | `timestamp(0)` (not `date`) | `date` drops the time silently; `d1 - d2` is a number of days in Oracle and an `interval` in PostgreSQL |
| `NUMBER`, `NUMBER(p)`, `NUMBER(p,s)` | `numeric` / `bigint`/`integer` (`PG_INTEGER_TYPE`) / `numeric(p,s)` | integer division: Oracle `7/2 = 3.5` on NUMBER, PostgreSQL `7/2 = 3` on integer types |
| `VARCHAR2(n BYTE)` | `varchar(n)` (characters) | wider on multibyte text; `NLS_LENGTH_SEMANTICS` |
| packages | schema per package (`PACKAGE_AS_SCHEMA`); package variables via `set_config`/`current_setting` | settings are text (cast on every read), **roll back with the transaction** (Oracle package state does not), visible outside the package, `current_setting(..., true)` returns `''` after a rollback |
| package initialisation block | an explicit init function called first | Oracle runs it once per session on first reference |
| `PRAGMA AUTONOMOUS_TRANSACTION` | wrapper through `dblink` or `pg_background` (Ora2Pg `_atx`) | a new connection or background worker per call; the write survives the caller's rollback - the proof has to roll the caller back and look |
| `CONNECT BY ... START WITH`, `LEVEL`, `SYS_CONNECT_BY_PATH`, `ORDER SIBLINGS BY`, `NOCYCLE`, `CONNECT_BY_ISLEAF`, `CONNECT_BY_ROOT` | `WITH RECURSIVE` with a path array for sibling order; PostgreSQL 14+ `SEARCH DEPTH FIRST BY` / `CYCLE` | row order (Oracle returns depth-first by default); cycles; `tablefunc.connectby()` handles none of the pseudo-columns |
| `(+)` outer joins | ANSI joins (sqlglot `eliminate_join_marks`, 1.8) | a `(+)` on a constant predicate changes which rows survive |
| `DECODE` | `CASE` | `DECODE(NULL, NULL, ...)` matches (NULL = NULL is true in DECODE); `CASE x WHEN NULL` never matches |
| `NVL`, `NVL2` | `coalesce`, `CASE` | `NVL` converts `b` to `a`'s type |
| sequences `.NEXTVAL`, `CACHE`, `ORDER` | `nextval('s')` | Ora2Pg's `TEST` already warns a sequence differing by up to the cache size |
| `ROWNUM` | `row_number()` / `LIMIT` | `ROWNUM` is assigned before `ORDER BY` |
| `BULK COLLECT`, `FORALL` | arrays / set-based SQL | `SAVE EXCEPTIONS` semantics |
| `COMMIT` inside a procedure | `COMMIT` in a `PROCEDURE` (PostgreSQL 11+), not in a function, not inside an exception block, not when called from a function | found by Microsoft's own Swingbench run (1.3) |
| `SYSDATE`, `SYSTIMESTAMP` | `clock_timestamp()` / `now()` | `SYSDATE` is per statement; `now()` per transaction |
| `%TYPE`, `%ROWTYPE` | same | - |
| exceptions `NO_DATA_FOUND`, `TOO_MANY_ROWS` | `SELECT INTO STRICT` | without `STRICT`, PL/pgSQL takes the first row / NULL silently |
| `DBMS_OUTPUT`, `UTL_FILE`, `DBMS_SCHEDULER`, `DBMS_LOB` | `RAISE NOTICE`, extensions (orafce), `pg_cron` | orafce (PostgreSQL licence) emulates many `DBMS_*`/`UTL_*` packages and Oracle functions |

### 2.5 What the three tables say about migkit's proof

Almost every silent difference above is one of five kinds, and each kind is a **class of generated input or of observation** rather than a special case:
1. **NULL/empty/whitespace/case/wide-character text** - `PROOF_INPUTS['text']` already has most; add `NULL` vs `''` pairs for Oracle and case pairs where the source collation is case-insensitive.
2. **Numeric edges and scale** - integer division, rounding, overflow at the type's limit, unsigned upper range.
3. **Time** - the clock (per statement vs per transaction), time zones, zero dates, `DATE` with a time.
4. **Transaction effects** - what survives an error inside the routine (handlers, TRY/CATCH, exception blocks), what survives the caller's rollback (autonomous transactions, table variables, package state), what the routine committed.
5. **Ordering and choice** - `ROWNUM`, `TOP` without `ORDER BY`, hierarchical order, `GROUP_CONCAT` without `ORDER BY`: the proof must compare as a multiset where the source leaves order unspecified, and as a list where it specifies one.

Kinds 4 and 5 are not reachable by migkit's current proof (a single read-only `SELECT` per side); they need executed calls with side-effect capture (section 6).

---

## 3. LLM-based converters and their published accuracy

| Work | Scope | Published result | How "correct" was measured |
|---|---|---|---|
| **PARROT** (NeurIPS 2025 benchmark) | 598 query-level translation pairs from 38 benchmarks and real services, 22 systems; variants of 28,003 and 5,306 | state-of-the-art LLMs average **below 38.53%**, ~17-60% by dialect | execution of LLM-generated test cases on both sides |
| **CrackSQL** (SIGMOD 2025; MIT on GitHub) | query-level, PG<->MySQL, PG<->Oracle, MySQL<->Oracle (248/142/111 pairs); ANTLR parsing + dialect specs in JSON + LLM, a rule-only mode that is sqlglot; validates without executing | better than direct GPT-4o prompting on its benchmark; on SQLProcBench "performs poorly" (68% of its errors were answering "Cannot translate!", per RISE) | Acc_EX (runs on target), Acc_RES (same result, same order) |
| **RISE** (ICSE 2026) | query reduction: the LLM translates a reduced query, a rule is extracted and applied to the original | TPC-DS: **97.98%** (baselines: jOOQ 73.74, SQLines 67.68, **sqlglot 75.76**, LLM-DeepSeek 82.83, LLM-GPT 90.91, CrackSQL 80.81); SQLProcBench, 44 procedures, **PostgreSQL -> Oracle**: 100%; without reduction -27.27 points | execution equivalence on the benchmark data; the authors warn wrong edits can "compromise query equivalence without triggering compiler warnings" |
| **Horizon** (Microsoft, PVLDB 18(12) 2025) | any-to-any; SSMA's rules first, the LLM only on the parts SSMA could not migrate | on an anonymised production set (74 scripts SSMA errored on, 9 warn+error, 30 warn): semantically equivalent **5% -> 74% -> 76%** (rules / +LLM / +parser check) for the error class; 11 -> 11 -> 33 for warn+error; 33 -> 40 -> 47 for warn | parser, compiler (`NOEXEC ON`, `PREPARE`), a **spurious-edits check** (penalises LLM edits to blocks the rule engine marked correct; threshold 0 in their runs), execution on **2 random rows per table plus LLM-generated rows until each script returns non-empty**; procedures and triggers with side effects "currently unsupported" for execution; an LLM equivalence *score* used only offline, because as a loop check it caused "regression spirals" |
| arXiv 2609.14413 (2026) | 1,006 Oracle PL/SQL files, specification-first regeneration | 623 regenerated, **380 executed successfully** on PostgreSQL 16 | ran, not equivalent |
| arXiv 2605.28557 (2026) | Oracle -> PostgreSQL token strategies | names "semantic drift" and long-context degradation | AST comparison and expert review, not execution |
| AWS DMS SC gen-AI | Oracle/SQL Server/ASE/Db2 -> PostgreSQL | "up to 90%" converted; +5-10 points conversion rate, 15-20% of previously unconverted code (industry claim) | parses as PL/pgSQL; no semantic check |
| Snowflake AI migration agent (SnowConvert AI) | source procedures/UDFs -> Snowflake | not published as a rate | **both sides executed**: baselines captured on the source (result sets, OUT/INOUT params, table changes), compared on a fresh clone in Snowflake; inputs from query logs, the source, or a synthetic testbed; PASS/FAIL/ERROR/NO_BASELINE; failing cases patched and re-run in a loop |

**What this means for migkit.**
* No published number supports trusting an LLM conversion of a stored procedure without execution. The best query-level figures (RISE ~98% on TPC-DS) are *rule extraction guided by an LLM*, not free generation, and the one procedure benchmark is 44 procedures in the opposite direction to migkit's common one.
* The designs that work share three devices: **rules first, model only for the residue** (Horizon, AWS, Google); **a check the model cannot argue with** (compiler, `plpgsql_check`, execution); **the model's edits confined** to what the rules could not do (Horizon's spurious-edit penalty). migkit's `assist.propose` has the first half of the first; it has no loop, no confinement and no execution for procedures.
* **SQL-ProcBench** (Microsoft, MIT; archived read-only 2026-06-11) is a parallel corpus - scalar UDFs, table-valued UDFs, procedures and triggers written in T-SQL and translated by hand to PL/pgSQL and PL/SQL, on an augmented TPC-DS schema, with example invocations and plausible parameter values - and is the natural **fixture corpus** for measuring each converter rung's proof pass rate in the docker sandbox (T-SQL side needs SQL Server; the PL/pgSQL side runs on PostgreSQL alone).

---

## 4. The proof side: research and tools

### 4.1 Equivalence provers (Cosette, SPES, SQLSolver, QED, VeriEQL)

| Prover | Method | Published reach | Licence | Counterexamples |
|---|---|---|---|---|
| Cosette (CIDR 2017) | Coq + Rosette, U-semiring | no NULLs, no intersect/except, no arithmetic, no string operations; a spurious refutation reported by VeriEQL's authors | open (research) | yes (Rosette), not always genuine |
| SPES (ICDE 2022) | symbolic, bag semantics as a bijection between output tuples | 95/232 Calcite pairs | open (research) | no |
| SQLSolver (SIGMOD 2024) | linear integer arithmetic over unbounded summations, Z3 | 232/232 Calcite, 114/127 Spark SQL, 19/19 TPC-C, 19/22 TPC-H | **Apache-2.0** (Java 17, Calcite parser) | no (EQ / NEQ / UNKNOWN / TIMEOUT; NEQ may be wrong) |
| QED (VLDB 2024) | Q-expressions, normal forms, NULLs and constraints; Rust, cvc5 + z3 | 299/444 Calcite, 979/1287 CockroachDB; a separate *disprover* searches for distinguishing databases | parser Apache-2.0, prover licence not found; an unsoundness issue (`COUNT(x)` vs `COUNT(*)` through the Calcite parser) opened 2026-09-28 | disprover |
| VeriEQL (OOPSLA 2024, distinguished paper) | bounded SMT over symbolic tuples with integrity constraints; bag and list semantics; WITH, CASE, ORDER BY, LIMIT, set ops, three-valued NULLs | proved or disproved > 70% of 24,455 pairs | **MIT** (Python 3.10+) | **yes, small databases, confirmed on MySQL as "genuine"** |

**What they cannot do for migkit.** All of them take *one SELECT per side in a standard SQL subset* and model *standard SQL semantics*. None handles procedural code, side effects, triggers, or - decisive for a cross-engine conversion - **engine-specific meaning**: MySQL's implicit coercions and case-insensitive collations, Oracle's `'' = NULL`, integer division, rounding of declared scales, time functions. Two queries a prover calls equivalent under its semantics can still answer differently on MySQL and PostgreSQL, which is exactly the class of bug section 2 lists. And "equivalent up to a bound" (VeriEQL) is a bound on rows, not on values.

**What they can do.** VeriEQL's counterexample databases are an **input generator for views**: render the source view and the converted view into the prover's dialect (sqlglot), ask for a distinguishing database of a few rows, load it into both engines, run both views. If the engines agree on the counterexample, the prover's difference was a semantics the engines share; if they disagree, migkit has a minimal failing fixture. VeriEQL itself confirms counterexamples on MySQL - the same "prove by execution" rule. **Wrap VeriEQL as an optional input source for views (M), never as a verdict.** SQLSolver/QED's "EQ" could be recorded as supporting evidence beside an execution pass, never instead of one.

### 4.2 Test-data generation for queries (XData, EvoSQL, SQLFpc)

* **XData** (IIT Bombay; ICDE 2010, DBTest 2013, VLDB Journal 2015): generates small databases (MAX_TUPLES 32) that **kill mutants** of a query - join/outer-join, comparison operator, aggregate, GROUP BY, LIKE, subquery, set operator, DISTINCT mutations - with an SMT solver; for `R.C = 6` it makes datasets with `R.C < 6`, `= 6`, `> 6`. Complete for a defined mutation class under assumptions; number of datasets linear in the query size.
* **EvoSQL** (TU Delft, ICSE 2018; Apache-2.0 on GitHub *(licence not re-checked this pass)*): search-based (genetic algorithm with fitness from an instrumented engine's plan) data generation for **full predicate coverage** (SQLFpc, Tuya et al.); 98.6% of 2,135 real queries fully covered in seconds each; biased random search seeds constants taken from the query. MoeSQL (PPSN 2020) makes the data 40% smaller.
* **Horizon** (3): 2 random rows per table, then LLM-proposed rows until each script returns something non-empty.

**For migkit.** The two ideas that transfer without the machinery: (1) **seed inputs with the constants in the code** (every literal compared against a parameter or column, and its neighbours: `= 6` gives 5, 6, 7; `'CLOSED'` gives `'closed'`, `'CLOSED '`); (2) **mutation-style boundaries per predicate** (<, =, > for each comparison; NULL on each side of an outer join; an empty group for each aggregate). Both are cheap given sqlglot's AST of each statement. The SMT and GA machinery is not worth building while execution on real fixtures plus coverage feedback (4.4) reaches the branches.

### 4.3 Differential testing of engines (SQLancer and successors)

SQLancer (MIT, Java) finds logic bugs in *one* DBMS with oracles that compare a query to a transformed version of itself on the same engine: PQS (a pivot row must be returned), NoREC (optimised vs non-optimisable form), TLP (a query equals the union of its three partitions by `p`, `NOT p`, `p IS NULL`), DQE (SELECT/UPDATE/DELETE with one predicate touch the same rows), DQP (different plans, same result), CERT (performance). It generates schema and data, not procedures, and does not compare *across* engines. **What transfers:** TLP's three-way partition is the right shape for **predicate inputs** - for each `WHERE`/`IF` condition in a routine, make sure the inputs include rows where it is true, false and NULL (4.2's point, from another direction), and the pivot idea for a proof failure report: name the row.

### 4.4 Coverage: knowing when enough inputs were tried

| Engine | Tool | How | Licence | Runs in the sandbox |
|---|---|---|---|---|
| PostgreSQL (PL/pgSQL) | **`plpgsql_check`** profiler: `plpgsql_coverage_statements(name)`, `plpgsql_coverage_branches(name)` (since 1.9, 2020); also the static checker (`plpgsql_check_function`) | in-server profiler; shared memory optional | MIT-style text (packaging says BSD) | yes (PGDG packages, arm64) |
| PostgreSQL | piggly (Ruby, recompiles with instrumentation), plpgsql_coverage, pgcov (POSETTE 2026, no extension) | instrumentation | various | yes |
| MySQL | none built in; **cover_me** (MIT) rewrites routines to insert a tag into a MyISAM `cover_me.trace` table at each branch/block/loop (non-transactional, so it survives the rollback) - PostgreSQL variant emits `RAISE WARNING` | instrumentation of a copy | MIT | yes (on the sandbox copy only) |
| SQL Server | tSQLt (Apache-2.0; needs CLR) or tSQLt-edge (CLR-free, for SQL Edge/2019/2022) for tests; SQLCover / SQLServerCoverage (Extended Events) for statement/branch coverage | XEvents | tSQLt Apache-2.0; SQLCover unconfirmed | not on SQL Edge arm64 as far as found (XEvents support on Edge not documented) |
| Oracle | utPLSQL (Apache-2.0) with `DBMS_PLSQL_CODE_COVERAGE` (12.2+, block level) and `DBMS_PROFILER` | built in | utPLSQL Apache-2.0; the package ships with Oracle | Oracle Free 23ai arm64 (R12), within its 2 GB/2 CPU cap |
| any | pgTAP (PostgreSQL licence), tSQLt, utPLSQL | assertion frameworks - **the output format for the proof kept as tests on the target** (R11's "kept as tests on the target") | permissive | yes / SQL Edge / Oracle Free |

**For migkit.** Coverage is what turns "200 calls agreed" into a statement with a denominator: *"agreed on 143 generated calls; 100% of the target routine's statements and 11/12 branches reached; branch at line 42 (`IF v_rate > 1`) never taken."* The **target** side is PostgreSQL in the common case, so `plpgsql_check` gives coverage of the converted routine for free; the **source** side's coverage (MySQL via cover_me-style instrumentation of the sandbox copy) says whether the original's branches were exercised. A branch never reached is not a failure; it is written in the verdict and caps it at `warn` (section 6.3).

### 4.5 Property-based generation (Hypothesis)

Hypothesis (MPL-2.0; already a dev dependency, `pyproject.toml:87`, and used in two migkit tests) supplies what a home-made generator lacks: **strategies composed from types** (integers within a column's range, decimals at a scale, text over an alphabet that includes the edges), **targeted search** (`hypothesis.target(score)` to push toward uncovered branches, with coverage as the score) and above all **shrinking** - when a call differs between the engines, Hypothesis reduces the input to a minimal one, which is what an operator needs in the report. MPL-2.0 is file-level copyleft and compatible with an MIT package depending on it unmodified. **Wrap it** (move it to a runtime extra, `migkit[prove]`), with strategies built from `canon` classes and the column facts migkit already reads (`canon.capacity`, `canon.params`).

### 4.6 Determinism: what must be pinned before two runs can be compared

| Source of difference | MySQL | PostgreSQL | SQL Server | Oracle |
|---|---|---|---|---|
| the clock | `SET timestamp = <epoch>` fixes `NOW()`/`CURRENT_TIMESTAMP` for the session (`SYSDATE()` only with `--sysdate-is-now`) | a `now()`/`clock_timestamp()` shadow function in a schema placed before `pg_catalog` in `search_path` catches unqualified calls only - the `CURRENT_TIMESTAMP` keyword is not a name lookup and cannot be shadowed *(to be measured)*; the robust way in the sandbox is **libfaketime** (`LD_PRELOAD`, GPL-2, a separate program in the container) under both servers | none built in; libfaketime in the container *(untested on SQL Edge)*, else compare time-valued outputs by tolerance or mask them | `ALTER SYSTEM SET FIXED_DATE` fixes `SYSDATE` |
| random | `RAND(seed)` | `setseed()` | `RAND(seed)` | `DBMS_RANDOM.SEED` |
| generated keys | `AUTO_INCREMENT` not rolled back | sequences not rolled back | `IDENTITY` not rolled back | sequences not rolled back |
| order | unspecified without `ORDER BY` | same | same | same |
| session settings | `sql_mode`, `collation_connection`, `time_zone`, `div_precision_increment`, `group_concat_max_len` | `TimeZone`, `DateStyle`, `extra_float_digits`, `search_path` | `ANSI_NULLS`, `CONCAT_NULL_YIELDS_NULL`, `DATEFIRST`, `LANGUAGE` | NLS settings |

Ordering is handled by comparing result sets as multisets unless the routine's final statement has an `ORDER BY` (then as lists) - SSMA Tester compares row by row, which is wrong for unordered results. Generated keys are compared as *differences* (the routine advanced the counter by n on both sides), never as values. Session settings are read from the source (`@@sql_mode` and the routine's own creation-time settings in `information_schema.routines`: `sql_mode`, `character_set_client`, `collation_connection`, which migkit already reads for R9) and set on the source-side proof session to what the application uses.

---

## 5. How each vendor verifies a conversion (summary)

| Vendor / tool | Executes the converted code? | Executes the source? | Inputs | Side effects compared | Verdict |
|---|---|---|---|---|---|
| AWS SCT / DMS SC | no | no | - | - | action items; AI output "syntactically valid PL/pgSQL", marked INFO for review |
| AWS + Amazon Q Developer | tests generated by a model, run by the user | user | model-written scenarios, "vary between runs" | final table states | none built in |
| Google DMS + Gemini | on a staging instance, by the user | no | - | - | Gemini "quality assessment" (model review) |
| Microsoft SSMA Tester | **yes** | **yes (modifies the source)** | **typed by the user** per call | table changes (by added ROWID), OUT params, return values, result sets row by row | per test case report; per-column tolerances |
| Microsoft VS Code PostgreSQL extension | compiles in scratch schema + `plpgsql_check` | no | - | - | routines that fail the check are sent back to the model; tests by Copilot/user |
| Microsoft Horizon (research) | parse, compile, spurious-edit check, run on generated rows | yes, for queries/views | 2 random rows per table + LLM rows | not for procedures ("currently unsupported") | checks pass or iteration limit |
| Snowflake AI migration agent | **yes, on a fresh clone** | **yes (baseline capture)** | query logs, source data, or a synthetic testbed | result sets, OUT/INOUT, table changes | PASS / FAIL / ERROR / NO_BASELINE; fix loop re-runs failed cases |
| Ora2Pg | no (`TEST` counts functions) | no | - | - | object counts, row counts, first 10,000 rows |
| EDB Migration Portal | loads into EPAS | no | - | - | compatibility % over DDL statements |
| Ispirer | by its engineers (service) | by its engineers | the customer's test cases, or written by Ispirer | yes, as a service | project sign-off |
| Babelfish Compass, MOLT, Voyager | no | no | - | - | feature classification |
| **migkit today** | **yes, functions and views only** (`prove_converted`) | **yes, read-only** | fixed pool per class (`PROOF_INPUTS`) | none | ok / diff / warn / skip - with the "procedures answer nothing" hole (section 0) |

Only SSMA Tester and Snowflake's agent execute both sides and compare side effects. SSMA makes the user type the inputs and writes to the source; Snowflake needs the source reachable and runs on its own cloud. Nobody **generates inputs from the routine's code and data, measures coverage, and refuses a pass without it**. That is the gap R11 fills.

---

## 6. A design for R11: convert per routine, then prove by execution

### 6.1 Wrap or build, tool by tool

| Tool | Decision | Why |
|---|---|---|
| sqlglot (MIT) | **keep, as the expression rung** and as the statement translator inside procedural bodies | best open query-level transpiler (75.76% on TPC-DS in RISE's comparison); no procedural generator for PostgreSQL/MySQL/Oracle (1.8); add `eliminate_join_marks`, refuse `exp.Connect` for non-Oracle targets |
| Ora2Pg (GPL-3.0) | **wrap as a program** in a container, Oracle first, MySQL/SQL Server as an alternative rung | the widest open PL/SQL converter; GPL is fine run as a separate program; its `TEST` never runs code, so every object it writes goes through migkit's proof |
| SQLines (Apache-2.0) | **wrap as a program**, candidate rung for T-SQL and MySQL procedural bodies | unmeasured quality; earns its rank only from proof pass rates |
| pgloader cast rules | **no** (already covered by `canon`); take the `ON UPDATE CURRENT_TIMESTAMP` trigger as a migkit rule | pgloader converts no code |
| AWS SCT / DMS SC, Google DMS, SSMA, EDB, Ispirer, MOLT Convert | **no** (closed, cloud-bound or GUI) | take: action-item vocabulary, "fix once, propagate", SSMA Tester's four comparisons and per-column tolerances, EDB's lesson on uncounted drops |
| Babelfish Compass, Voyager analyzers | **no**; take the feature classification (`ReviewSemantics`) and the parse-tree walk per target version | |
| `plpgsql_check` (MIT/BSD) | **use in the sandbox** (an extension in the PostgreSQL container): static gate, fail closed, and statement/branch coverage | the only coverage source that costs nothing on the common target |
| pglast (GPL-3.0+) | **only out of process** (like `runners/second_reader.py`), or not at all | MIT package; `plpgsql_check` and the server's own parser cover the need |
| sqlfluff (MIT) | **use** to split T-SQL/PL/SQL/MySQL procedural bodies into statements where sqlglot gives `Command` | already considered for R9 |
| Hypothesis (MPL-2.0) | **wrap**, move to a `prove` extra | strategies from `canon` classes, coverage-targeted search, shrinking to a minimal failing call |
| VeriEQL (MIT) | **optional input source for views** | distinguishing databases for view pairs; never a verdict |
| SQLSolver (Apache-2.0), QED | **no** for now | standard-SQL semantics only; no counterexamples (SQLSolver) |
| pgTAP (PostgreSQL licence), utPLSQL (Apache-2.0), tSQLt (Apache-2.0), tSQLt-edge | **emit to**, as the proof kept as tests on the target | tSQLt installs a CLR assembly (`tSQLtCLR`, `PrepareServer.sql` enables CLR); SQL Edge has no CLR, so on Edge the target is **tSQLt-edge** (a CLR-free, mostly tSQLt-compatible framework for SQL Edge, 2019 and 2022) or plain T-SQL |
| cover_me (MIT) | **copy the technique** (tag inserts into a MyISAM trace table, on the sandbox copy only) | MySQL has no coverage tool of its own |
| libfaketime | **use in the sandbox containers** | one clock for both servers |
| LLMs (`assist.py`) | **keep, as the last rung**, turned into a loop with confined edits | no published accuracy justifies trusting them without execution (section 3) |

### 6.2 Convert: one routine at a time, chosen by the decision engine

The unit of work is a **routine** (view, function, procedure, trigger, event, package member). Its facts, read once from the source catalogue and parsed body:
* engines of the pair, kind, whether the body is one expression / one SELECT / procedural;
* **the construct inventory**: every construct found, as a code in section 2's vocabulary (`mysql.handler.continue`, `mysql.on-update-timestamp`, `tsql.try-catch`, `tsql.table-variable`, `tsql.temp-table`, `tsql.merge`, `oracle.connect-by`, `oracle.autonomous`, `oracle.package-state`, `oracle.empty-string`...), with the line - this replaces `handwork.py`'s single `server-side-code` count and is what `assess` prints;
* size (statements), the routines it calls (callees are converted and proved first; a caller's proof is only as good as its callees'), the tables it reads and writes (from sqlglot over each embedded statement), its creation-time settings (`sql_mode`, `collation_connection`...);
* a hash of the source definition, kept in the checkpoint so a re-run does not re-convert what has not changed (idempotent).

**The rungs** (each names what it needs, how it is proved, how it is costed - the P0 shape, `docs/backlog.md:123-150`):

| Rung | Gives | Needs | Proof = the probe |
|---|---|---|---|
| `by-hand` | the operator's own statement from `convert/<routine>.sql` | the file exists | the proof, like every rung - a person's version is not trusted more |
| `sqlglot` | views, one-expression functions (today's `converted_code`) | body parses without `Command`, no construct outside sqlglot's reach | proof |
| `migkit-rules` | procedural MySQL -> PostgreSQL and T-SQL -> PostgreSQL: a block parser (MySQL's compound-statement grammar is small; sqlfluff's tree for T-SQL) rewrites control flow, handlers, temp tables, identity and error functions by section 2's rules, and hands each embedded statement to sqlglot | every construct in the inventory has a rule; otherwise the rung is dropped and the missing rule named | proof |
| `ora2pg` | Oracle (all kinds), and MySQL/SQL Server as an alternative | the program's container, a reachable sandbox copy of the source | proof |
| `sqlines` | T-SQL/MySQL procedural, as an alternative | the program | proof |
| `model` | the residue: what no rule rung produced, or the unconverted parts inside a routine | `MIGKIT_AI` and `MIGKIT_AI_SHARE=code` (unchanged consent rule, `assist.py:1-30`) | compile, `plpgsql_check`, then the proof; failures fed back (compiler error, check finding, the **first differing call after shrinking**) for a bounded number of rounds; edits outside the residue rejected (Horizon's spurious-edit rule) |

**The climb.** Drop rungs that lack something the routine needs (a construct without a rule, a program not installed, no model configured). Rank the rest by **measured proof pass rate for this pair and this construct family** - from the fixture corpus (6.5) and from this migration's own earlier routines - then by cost. Take the top rung: gate (the target accepts the `CREATE`; on PostgreSQL `plpgsql_check_function` reports nothing, fail closed), then prove (6.3). A gate failure or a `diff` falls one rung and says why; the record keeps every rung tried and its evidence. When every rung falls, the routine is **by hand, with the evidence attached**: the construct codes, each rung's failure, and the minimal differing inputs - which is what a person needs to write it.

**Fix once, propagate (later, L).** When a person's `convert/<routine>.sql` passes where a rung failed, the AST difference between the rung's output and the person's (sqlglot's `diff`) is offered as a candidate rule for the same construct code; a rule is adopted only when it passes the proof on the fixture corpus.

### 6.3 Prove: differential execution in the sandbox

**Where.** Only in the docker sandbox, never on the user's source or target (SSMA's own warning: its Tester modifies the source and restoration "may be impossible"; migkit's rule: no real database for development and tests). Two containers - the source engine and the target engine - each loaded with the **same fixture**:
* a **key-closed sample** of the source (a few hundred rows per table, parents pulled in for every foreign key, plus every row whose key a routine's literal names), read once, read-only, and moved into both containers **by migkit's own move and proved equal by migkit's own check** - so a difference found later is the code's, never the data's;
* where the source cannot be read (or has no rows a routine needs), **generated rows** from the declared types and constraints (Horizon's "2 rows per table", then rows making each predicate true, false and NULL - 4.2/4.3).

**Inputs, per routine.**
1. The existing class pools (`hetero.py:2304` `PROOF_INPUTS`), extended by section 2.5's five kinds (NULL vs `''` for Oracle, case pairs under a case-insensitive source collation, the type's limits, leap days and zone edges, zero dates where `sql_mode` allows them).
2. **Constants from the body** and their neighbours (`= 6` -> 5, 6, 7; `'CLOSED'` -> `'closed'`, `'CLOSED '`), per EvoSQL's biased search and XData's boundaries.
3. **Values from the fixture** for each parameter compared with a column: existing keys, a missing key, NULL.
4. **Coverage-guided search** with Hypothesis: the score is the number of target branches reached (`plpgsql_coverage_branches`), plus source branches on a MySQL source instrumented in the sandbox copy (cover_me's technique); stop when coverage has not grown for a number of rounds or the time budget - sized from the routine's measured call time, not configured - is spent.
5. For triggers: generated `INSERT`/`UPDATE`/`DELETE` on the trigger's table from fixture rows, including a **no-op update** (it separates MySQL's `ON UPDATE CURRENT_TIMESTAMP` from pgloader's always-fire trigger, 2.2) and a multi-row statement (row vs statement triggers). For events: the body run as a procedure; the schedule compared as data (interval, start, time zone).

**Each call, on each side.**
1. **Isolate.** In a transaction, rolled back after - exact for InnoDB/PostgreSQL/SQL Server/Oracle data - unless the inventory says the routine does something a rollback does not undo: DDL or `COMMIT` (MySQL implicit commits, PostgreSQL procedures that commit), autonomous transactions (dblink, `pg_background`, Oracle `PRAGMA`), non-transactional tables (MyISAM). Those calls run against a fresh copy instead (PostgreSQL `CREATE DATABASE ... TEMPLATE fixture`; MySQL a reload of the fixture's dump; Oracle a restore point).
2. **Pin** the clock (libfaketime under both servers; MySQL `SET timestamp`; Oracle `FIXED_DATE`), seeds, and the session settings the routine was created with.
3. **Capture before**: per table in the routine's write set (and its triggers' write sets, transitively; the whole fixture if the write set cannot be computed - it is small), the rows by key through `canon`'s rendering; sequence and `AUTO_INCREMENT`/`IDENTITY` positions.
4. **Call**: a function by `SELECT`; a procedure by `CALL`/`EXEC` with OUT/INOUT parameters bound; every result set read (MySQL procedures return them unannounced - read until the driver has no next set; PostgreSQL `refcursor`s fetched inside the same transaction).
5. **Capture after**: return value, OUT values, result sets (compared as multisets unless the statement that produced them has `ORDER BY`), the **error** as a class (SQLSTATE class for MySQL/PostgreSQL/Oracle-mapped; a migkit table for T-SQL numbers - `2627/2601`->`23505`, `547`->`23503`, `515`->`23502`, `8134`->`22012`; an unmapped number compares as "errored on both, class unknown" and caps the verdict at `warn`), the **row changes** per table (inserted, deleted, changed columns, by key), the **counters advanced** (as differences, never values), and, for routines the inventory marks, **what survives the caller's rollback** (a second run: call inside a transaction, roll back, look).
6. **Compare** through `canon.render_value` (the same renderings the data check is held to); a tolerance or mask only where a rule states it with its reason (float last digit, a time value the routine reads from the clock), and every one is printed in the verdict.

**The verdict, per routine - never `ok` without execution evidence.**
* `ok` - executed on both sides; N calls, all equal in value, error class, row changes and counters; target statement coverage 100% and every branch reached (source coverage too where measurable). The sentence carries the numbers and the rung that produced the code.
* `warn` - every executed call equal, but something is short: a branch never reached (named, with its line), a tolerance or mask applied, an error class not mapped, source coverage not measurable.
* `diff` - a call differs; the **minimal** input after shrinking, and what differed (the value, the error, the row, the counter) on each side.
* `not proved` (`skip`) - one side could not execute it here (the engine is not in the sandbox; SQL Edge lacks the feature - `FORMAT()`, `hierarchyid`, CLR; Oracle not started within the VM's limits), with the reason.
* `missing` - not on the target.

The database's line is the worst of its routines, with the counts ("41 ok, 3 warn, 2 not proved") - which closes the hole in section 0 where procedures made the result `ok`.

**Kept as tests on the target** (R11's last line): the calls and the source's outcomes written as pgTAP (PostgreSQL), utPLSQL (Oracle), tSQLt (SQL Server with CLR) or tSQLt-edge (SQL Edge, no CLR), or a plain SQL script (MySQL, which has no framework in common use), so the proof keeps running after the cutover.

### 6.4 What runs in local docker on this machine (arm64, colima)

| Piece | Runs here | Notes |
|---|---|---|
| MySQL 8.x <-> PostgreSQL 16/17, both directions, functions, procedures, triggers, events | **yes** | the whole R11 loop can be built and tested on this pair first |
| `plpgsql_check`, pgTAP in the PostgreSQL container | **yes** | PGDG packages for arm64 |
| libfaketime in both containers | **yes** *(to be measured: MySQL's and PostgreSQL's use of the clock under `LD_PRELOAD`)* | |
| MySQL coverage by instrumentation of the sandbox copy | **yes** | |
| Hypothesis, VeriEQL | **yes** (Python) | VeriEQL needs Python 3.10+ and its solver |
| SQL Server as source or target | **partly**: SQL Edge (the last arm64 image; retired 2025-09-30, no updates) runs T-SQL procedures and triggers; no CLR, CLR-backed functions (`FORMAT`), `hierarchyid`, spatial, full-text, Service Broker, linked servers, Database Mail; XEvents-based coverage not documented; SQL Server 2022/2025 images are amd64 only | routines using what Edge lacks are `not proved` here; the full proof waits on an x86 runner (backlog item 27) |
| Oracle as source or target | **yes, within limits**: Oracle Free 23.5+ runs natively on arm64 (slim image), capped at 2 GB and 2 CPUs, the VM needs 4 GB+ (backlog R12); an earlier attempt filled the VM's disk (`docs/backlog.md:1832-1834`) | utPLSQL and `DBMS_PLSQL_CODE_COVERAGE` on it *(to be measured)* |
| Ora2Pg | **yes, to be built**: Perl with DBD::Oracle against Oracle's Instant Client for Linux ARM64, DBD::mysql from Debian, DBD::ODBC + Microsoft's ODBC driver for SQL Server *(arm64 availability of the ODBC driver to be checked)* | one image, run as a program |
| SQLines | **yes, to be built** from source (C++) | |
| SQL-ProcBench (fixture corpus) | PL/pgSQL half **yes** on PostgreSQL alone; T-SQL half on SQL Edge where it avoids Edge's gaps; PL/SQL half on Oracle Free | no MySQL dialect: the MySQL corpus for 2.2 has to be written |

### 6.5 Work, in order, with effort (S up to 2 days, M up to 2 weeks, L more)

| # | Piece | Effort | Testable here |
|---|---|---|---|
| 1 | The verdict hole: procedures, triggers, events not executed are `not proved`, never counted into `ok`; the database line is the worst routine with counts | **S** | yes |
| 2 | Per-call outcomes in `_call` (value or error class each), naming the first differing input | **S** | yes |
| 3 | `plpgsql_check` gate for PostgreSQL targets, fail closed | **S** | yes |
| 4 | sqlglot rung fixes: `eliminate_join_marks`, refuse `exp.Connect` to non-Oracle, `TRY_CAST` not to `CAST` | **S** | yes (parse-level; Oracle side for execution) |
| 5 | Inputs from body constants, fixture values and section 2.5's kinds | **S-M** | yes |
| 6 | The fixture: key-closed sample moved into both sandbox containers by migkit and proved equal; generated rows where there is none | **M** | yes |
| 7 | Procedure harness: isolation (transaction, or template/reload where the inventory says), pinning, before/after capture, OUT params, result sets, error classes, counters, survives-rollback run - MySQL and PostgreSQL | **M-L** | yes |
| 8 | Construct inventory per routine (the codes of section 2) feeding `assess`/`handwork` and the climb | **M** | yes |
| 9 | Coverage: `plpgsql_check` on the target, instrumentation on a MySQL source copy; coverage in the verdict | **M** | yes |
| 10 | Kept tests: pgTAP / plain SQL / utPLSQL / T-SQL emitters | **S-M** | yes (pgTAP, SQL); Oracle within limits |
| 11 | `model` rung as a loop with confined edits and proof feedback | **M** | yes (any provider, or a local model) |
| 12 | `migkit-rules` MySQL -> PostgreSQL procedural (block parser + 2.2's rules) with a written MySQL fixture corpus | **L** | yes |
| 13 | Triggers and events proved (generated DML, no-op update, statement vs row; event body as procedure, schedule as data) | **M** | yes |
| 14 | Hypothesis search targeted by coverage, and shrinking of a differing call | **M** | yes |
| 15 | Ora2Pg wrapped: image, per-object output, its cost units shown as Ora2Pg's beside migkit's measured time per routine (item 40) | **M** | yes (MySQL source now; Oracle within Free's limits) |
| 16 | `migkit-rules` T-SQL -> PostgreSQL (2.3), harness adapter for SQL Server | **L** | partly (SQL Edge) |
| 17 | Oracle harness adapter (restore points for autonomous writes, `FIXED_DATE`, utPLSQL coverage) | **M** | within Oracle Free's limits |
| 18 | SQLines wrapped as a rung, ranked by measured pass rate | **M** | yes |
| 19 | VeriEQL counterexamples as view inputs | **M** | yes |
| 20 | Fix once, propagate: a person's fix offered as a rule for the construct code | **L** | yes |

Items 1-3 are a day's work each and remove the only place where migkit's code verdict can be green without evidence. Items 5-9 on MySQL <-> PostgreSQL are the core of R11 and are fully testable here; they give migkit what no vendor ships - inputs generated from the routine's own code and data, side effects compared, coverage stated, and no pass without execution - before any new converter is added. Converters (11, 12, 15, 16, 18) then climb on measured pass rates instead of claims.

---

## Sources

Vendor and tool documentation: AWS DMS Schema Conversion "Converting database objects with generative AI" and assessment-report pages; AWS Database Blog posts "SQL Server to Aurora PostgreSQL conversion with AI agents for AWS DMS" and "Augment DMS SC with Amazon Q Developer for code conversion and test case generation"; AWS SCT user guide (assessment report, action items); AWS SQL Server to Aurora PostgreSQL Migration Playbook; Google Cloud DMS "Convert Oracle code and schema with Gemini assistance", conversion-workspace docs and the August 2026 blog "Accelerate PostgreSQL Migrations with Gemini in DMS"; Microsoft Learn SSMA for Oracle Tester pages (testing migrated objects, creating test cases, selecting objects to test, selecting affected objects) and SSMA package-variable emulation; Microsoft Learn "Oracle to PostgreSQL Migration - PostgreSQL extension for VS Code" and Tech Community posts on AI-assisted conversion and validation; the kloba/oracle-to-postgres-migration-lab repository; Ora2Pg README and repository (GPL-3.0), Dalibo blog posts on autonomous transactions, Gilles Darold's "Migration validation made easy with Ora2Pg" slides; Ispirer product pages and July 2026 validation blog; SQLines repository (Apache-2.0); EDB Migration Portal docs and "How EDB Migration Portal calculates compatibility percentage"; Babelfish Compass repository, user guide and AWS "Deep dive into Babelfish Compass"; CockroachDB MOLT Schema Conversion Tool docs; YugabyteDB Voyager assess-migration, analyze-schema and known-issues docs; sqlglot repository and the installed sqlglot 30.18.0 source (`.venv`); pgloader MySQL reference and issue #735; MySQL manual (DECLARE HANDLER, SIGNAL); PostgreSQL docs (PL/pgSQL errors, GET DIAGNOSTICS, search_path); pg_cron repository; Snowflake "Testing stored procedures and UDFs" (AI migration agent); plpgsql_check README/PGXN; cover_me repository; utPLSQL coverage docs; tSQLt, tSQLt-edge, SQLCover, pgTAP sites; Azure SQL Edge supported-features and retirement pages.

Papers: PARROT (arXiv 2509.23338); CrackSQL (SIGMOD 2025, arXiv 2504.00882); RISE (ICSE 2026, arXiv 2601.05579); Horizon (PVLDB 18(12) 2025, pp. 5259-5262); SQL-ProcBench (PVLDB 14(8) 2021, repository); arXiv 2609.14413 and 2605.28557 (Oracle to PostgreSQL with LLMs); PLSQLBench (arXiv 2608.15931); VeriEQL (OOPSLA 2024, arXiv 2403.03193) and its PVLDB 2024 demo; SQLSolver (SIGMOD 2024); QED (PVLDB 17, 2024); SPES (ICDE 2022); Cosette (CIDR 2017); XData (ICDE 2010, DBTest 2013, VLDB Journal 2015); EvoSQL (ICSE 2018); SQLancer (PQS OSDI 2020, TLP, NoREC).
