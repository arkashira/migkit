# AUD-0 helpers: call sites left for the other audit groups (2026-10-01)

Line numbers as of master at the AUD-0 merge; grep the names if they moved.

## durable (write_atomic / write_json / append_line / read_state / private_dir)
- Tail tokens: engines/hetero.py `_saved_token` and the token writes around it; marks.py (`saved`); tailctl.py (`saved`); engines/mongodb.py resume-token writes; engines/kafka.py position write.
- cli.py `_Checkpoint.save`.
- Records: freeze.py `_save`, jobs.py `_save`, cutover.py `_save`, tailtables.py state, lease.json (W1).
- repairlog.py and audit.append -> `append_line` / `read_lines`; the copybook index (ST6).
- `Hop.report_dir` -> `private_dir` (SEC10).
- Write-then-chmod: movers.py (several), guide.py.

## envkeys (flag / number / text)
- schedule.py, engines/hetero.py, machine.py, integrations.py, lease.py, stall.py, kept.py `_env`, model.py, engines/mysql.py, engines/postgres.py (KEEPALIVE_IDLE, CROSSCHECK, SUBSCRIBE_TIMEOUT), engines/pg_pass.py, legs.py, physical.py, cloudseed.py, movers.py, leaves.py, decide.py, engines/base.py.
- The start-up did-you-mean for unknown MIGKIT_* names belongs in `cli.main`.

## Endpoint.url / tls_forms
- engines/mysql.py `_schema_urls`, `_reladiff_url` (check which TLS form each client reads), `_client_tls` -> `client`.
- engines/hetero.py `_url`; leaves.py `_url`; movers streaming-copy / follow / loader URLs; users.py `_mongo_client`; engines/mongodb.py `_client`.
- legs.py `_sslmode` -> `far`; postgres `replicate_sql` conninfo (SEC13); MySQL native replication -> `source`/`master`; physical.py `clone_sql` -> `clone`; the page copy's PGSSLMODE -> `env`; movers `_my_tls` -> `dump`/`uri`; the change pipeline connector fields and JDBC URL -> `connector`/`jdbc`; second_reader; the row-sync DSNs.

## Redactor
- export.py `_SECRET`/`_plain`, imports.py `from_connector`, addresses.py `_shown`, the splits in users.py, the follow log.

## Retry
- MySQL DDL scripts -> `run(writes=True)`; users.py `_retry` -> `with_retry`.

## Table names (split_table / quote_name / qualify)
- engines/base.py `_qualified`, engines/postgres.py `_split`, movers.py `excluded_tables`, digests.py `_quoted`, engines/dbapi.py `_qualified`, engines/mssql.py (two places).

## Probes
- MongoDB deep check; MySQL deep checks (charset, generated columns, null/empty, float, LOB); PostgreSQL `_lob_sizes`.

## catalog
- drift.py `changes`, each engine's `column_catalog`, cli.py schema-compare block, cutover.py `_shape_rows`.

## Units and durations
- cli.py rate label ("MB/s" for 2^20 values), oracle_logminer.py and ddbexport.py units; machine/planner/guide `_took` -> `_duration`.
