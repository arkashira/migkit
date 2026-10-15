# Verify execution audit (P0-VERIFY-EXECUTION, 2026-10-16)

Read from the call paths of the installed entry points, not from whether a
name appears in a file. "called" = the path reaches it on every run;
"bypassed" = it exists and the path goes around it; "partial" = some
engines/branches only; "unsupported" = nothing on the path does it.

| Entry point -> query | Sizing | Pacing | Resource probes | Progress | Ledger / evidence | Safe recovery |
|---|---|---|---|---|---|---|
| `check` -> `cli._check` -> `run_checks` -> `ReadPool` -> `eng.check_<c>(db)` | called: `ReadPool` -> `sizing.estimate` | called: `sizing.Pace` over the databases | called: `sizing.resources` in `ReadPool` | called: `check-progress.jsonl`, `timer.eta_detailed` | called: `--resume` reuses an `ok` only where `_position` held and the scope fingerprint matches | partial: answer-level; a table interrupted mid-read is read again |
| `check --deep` (MySQL) -> `check_deep` -> charset / NULL-empty / float | called: `scanplan._slots` -> `sizing.estimate` | called: child `Slots` of the check's `ReadPool` (`ranges.current_coordinator`) | called through the same `Slots`/`Pace` | called: `deep-scan-progress.json` | partial: `scanplan.Evidence` needs change markers; MySQL has none, so nothing is reused | unsupported: a failed read stays `error`; no unit resume without a marker |
| `check --deep` (MySQL, other deep reads: grants, ownership, triggers, render, boundary) | bypassed: sequential catalogue queries | bypassed | unsupported | unsupported | unsupported | unsupported |
| `check --deep` (PostgreSQL NULL/empty + replacement characters, SQL Server NULL/empty + float + replacement characters) -> `_deep_scans` / `_value_scan` -> `scanplan.for_engine` | called: `scanplan._slots` -> `sizing.estimate` | called: child `Slots` of the check's `ReadPool`; one fused read a table a side (no key ranges cut on these engines yet) | called through the same `Slots`/`Pace` | called: `deep-scan-progress.json` | partial: PostgreSQL reuses values under unchanged markers (`Evidence`); SQL Server has no per-table marker on this path | unsupported: a failed read stays `error`, never a zero or a skip |
| `check --deep` (Oracle, ClickHouse, Teradata, BigQuery, pgwire and the rest) | bypassed: one query per column per side (depth cell `verify.value.one-plan` NOT_YET -> P0-VERIFY-EXECUTION) | bypassed: sequential | bypassed | unsupported | unsupported | unsupported |
| `check` data (MySQL `_own_check_data` -> `_read_pool` -> `_diff_table`) | called: `Engine._read_pool`, child of the run's pool | called: shared `Slots` + `Throttle` | called: `Throttle(probe=_health)` | partial: per-table lines | partial: `kept.proved_db` where proof is kept | partial: `checkpoint.Checkpoint` per range |
| row parity (`leaves.strategy`) | called: caller's workers | partial: `leaves.both` and localisation open private `ThreadPoolExecutor`s | unsupported in those pools | partial | called: `kept` leaves | partial |
| `sync --mode verify` -> `_check` | as `check` | as `check` | as `check` | as `check` | as `check` | as `check` |
| `watch --verify --delta` / `sync stream` -> `_delta_loop` -> `delta_in_scope` / `delta_verify` | bypassed: `for d in dbs` sequential, no `ReadPool` | bypassed | unsupported | called: a line per database, `delta-summary.json` | called: delta position and summary | called: resume from the last verified point |
| `move` -> `_move_full` -> `_copy_tables` | called: `sizing.fit`, `jointplan` | called: `ranges.active = Slots(most, Pace)` | called: `sizing.resources(_side_health, model)` | called: `planner.progress` with ETA | called: `checkpoint.json` per range | called: ranges not done are copied again; fencing tokens |
| `move` scans of Cassandra, DynamoDB, OpenSearch, Cosmos DB, Spanner, Bigtable, Firestore | partial: own segment counts | bypassed: private `ThreadPoolExecutor` per engine | unsupported | partial | partial | partial |
| `tail` (CDC) -> `tailctl` loop | unsupported: one reader per hop | partial: `applyway.Pace` for batch apply | unsupported | called: tail status/position | called: position, `tailtables` states | called: restart from the committed position |

## Changed by this ticket

- `migkit/scanplan.py`: every aggregate the deep value checks ask of a
  table in one plan: duplicates read once, fused into one bounded read
  (`MOST_EXPRS`) a side or a covering key range (`covering`), sides and
  tables side by side under the run's slots; the way is a climb of
  `READS` (`decide.attempt`), ranked by seconds a row measured end to end,
  "every row" the need; values kept under unchanged markers (`Evidence`)
  are not read again, never for a cutover's proof.
- MySQL `check_deep`: charset, NULL/empty and float through one plan
  (`_deep_scans`, `_value_scan`, `_value_aggregates`); `_charset_counts`'
  private `Slots` gone; the 30-column limits gone.

## Changed by W6C2

- `scanplan.for_engine`: the engine wiring (markers, cutover hold, scope,
  kept costs, progress, evidence) in one place; `count` lets SQL Server
  count with `count_big(*)`. MySQL `_value_scan` now calls it.
- PostgreSQL `check_deep`: NULL/empty and replacement characters through
  one plan (`_deep_scans`); the 30-column limit and the silent `continue`
  on a failed read gone - a column not read is an `error`, unknown.
- SQL Server: NULL/empty and float share one plan (`_deep_scans`,
  `_value_aggregates`, `_value_scan`) in `_deep_value_checks`; the charset
  check's replacement-character counts read through a plan of their own;
  limits and silent skips gone.
- Depth matrix item `verify.value.one-plan`: yes on PostgreSQL and SQL
  Server (tests/test_postgres_and_sql_server_deep_checks_share_one_plan.py),
  UNPROVED on the MySQL family, NOT_YET -> P0-VERIFY-EXECUTION elsewhere.
- Not done: `leaves` private pools, the delta loop's sequential databases,
  row parity as a costed ladder in `decide`, key-range cuts for PostgreSQL
  and SQL Server plans.
