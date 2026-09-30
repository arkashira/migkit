# What is still flat, slow or inconsistent outside the engines (audit 2026-10-01)

**Status: COMPLETE (2026-10-01).** Read at `5e30188` (138,136 lines in 169
modules, 546 test files). The owner asked: "evaluate everything else too -
not only the engines - whatever is still flat, not smart, not fast, not
consistent: make it the best; put it in the backlog, then implement".

**How it was read.** Ten areas, each read whole by one reader: the CLI
flows, config and options, state and resume, security, the wrappers around
programs and their install, schema/drift/users/grants, speed, logic kept in
two places, logging/dashboard/metrics/docs, and the test infrastructure.
Nothing was run against a database, no container was started and no test
was run. Where a number says **measured** it was taken in-process on a pure
function with synthetic input (CPython 3.12.13, Apple arm64, `timeit` best of
runs, or an AST/grep count); where it says **estimate** it is arithmetic
from the code. Round trips are counted from the code, not timed.

**Left out on purpose** (already known and being built): P0-SMART, P0-DEPTH,
P0-PARITY, the cutover gaps, R23's rest, engine reach, the one MySQL and
PostgreSQL table-way ladder, and the ad hoc fallback sites the fallback sweep
listed. Where a finding here touches one of them it says so.

**What one finding says:** where (file:function, lines), what is flat, slow
or inconsistent, the evidence, the best fix, its size (S/M/L) and priority:
**P0** a wrong answer, a false "same", a lost or doubled row, a secret or a
program name leaked, a guard bypassed; **P1** slow or inconsistent where
operators meet it; **P2** polish.

---

## 0. The answer in brief

1. **"Same" is decided in four places and they disagree** (V1, V2, LG1). An
   empty answer set is `same`; `report.render` rewrites a `diff` into `ok` in
   the very dicts `check` computes its exit code from (measured: verdict
   `different`, console "all green", exit 0); the dashboard and `/metrics`
   count every `skip` as a difference (measured: a clean two-database MySQL
   check pages `MigkitHopDifferent`); a watch cycle that raised is dropped and
   the hop is told "same". One `verdict.decide()` must feed the exit code,
   the report, the dashboard, metrics, notify, cutover and CI.
2. **Three write paths skip the guards `move` enforces** (V3-V5): `sync
   --mode migrate/stream --go` takes no position before its seed and
   re-copies PostgreSQL over what it loaded; `sync --go` repairs without an
   approval and only looks at the first result; `tail`/`replicate`/`sync
   stream` write real values into a hop that masks columns.
3. **Nothing is written durably** (ST1-ST6). About twenty write helpers,
   three fsyncs in the whole tree, no directory fsync; the tail's position is
   rewritten in place with `write_text` every batch; a power loss can apply
   two-way exact batches twice, drop held changes, or duplicate copybook
   records. Durability measured cheaper than what the tail already spends.
4. **The schema check compares by class** (SC1-SC4). `varchar(255)` to
   `varchar(10)`, `numeric(12,4)` to `numeric(12,2)` and `timestamptz` to
   `timestamp` read `ok` on the pair path (measured); SQL Server never
   compares a column; drift is (name, type string); PostgreSQL's "unchanged"
   marker does not move on `DROP COLUMN`.
5. **TLS the hop asks for is dropped on the main data paths** (SEC2) while
   `doctor` reports it verified - the default PostgreSQL copy, both native
   replications, CLONE, the page copy and the MySQL bulk programs.
6. **Typos load clean** (K1-K3): a hop with `approvals: {step: [cutover]}`,
   `mapping: {colums: ...}`, `schedule: {crn: ...}` loads with zero errors and
   leaves approvals off, the SSN column copied and no schedule (measured);
   duplicate YAML keys overwrite each other silently; `sync` overwrites the
   hop's `on_conflict`.
7. **Stopping migkit leaves programs writing** (W1-W2): SIGTERM is handled in
   three places; container writers have no name and survive their client; a
   remote page copy keeps writing after its ssh dies.
8. **One process or connection per catalogue query** (SP2, SC15): 251
   `_psql` call sites spawn a process each; MySQL opens a connection per
   query; the tail opens a target connection per batch. `drift.transient`
   is quadratic in tables (measured 587.6 ms at 5,000 tables, run per tail
   batch).
9. **The final suite as it stands is ~3.5-4.5 h, serial, with no timeout**
   (T1-T13, estimate from the last whole run: 6,835 s for 2,284 tests on
   2026-09-27; 3,614 test functions now). 281 modules start their own
   containers; one session fixture exists. Shared server profiles, xdist
   shards per profile, the VM shard beside the default one and a strict-skip
   mode bring it under an hour (estimate).
10. **The same logic lives in two to fifteen places** (DU1-DU26): fifteen
    URL builders, five redaction schemes, five duration formatters, seven
    PostgreSQL "user table" definitions, seven timestamp formats, the target
    database mapped twice at 33 PostgreSQL call sites.

Fixed in this audit (one-liners, section 11): doctor's hetero engine lookup,
the change-stream REST port bound to 127.0.0.1, the Homebrew licence,
`deploy/conf/` ignored.

---

## 1. The verdict, exit codes and the CLI flows

**V1. Four places decide "green", and the HTML renderer changes the exit
code** (P0, M). `cli.py:_check` 1214 writes `verdict.json`, 1227 calls
`write_report(hop, results)`, 1234 computes `bad`; `report.py:render` 155
runs `_reclassify(results)`, which edits the same dicts in place
(`"missing=0 extra=0 changed=0" in detail` -> `status = "ok"`);
`verdict.summarize` 387-396; `ui.py:_hop_data` 230-236 drives
`migkit_hop_status`. SQL Server joins every table into one detail
(`mssql.py:373,397`), so one hash-only table hides a real difference in
another. Measured in-process: `summarize` -> `different`; after `render`:
`ok | "checksum flicker settled"`, `bad: 0`; the console prints "verdict:
different" then "all green" and exits 0. A `warn` gives verdict `same` while
the CLI exits 1; `ui.py:234` counts every `skip` as `diff`. Fix: one
`verdict.decide(records) -> (status, exit_code, per-row severity)` used by
the check exit code, the report banner, the dashboard, metrics, `watch`,
`sync`, cutover's final check, `integrations.passed` and the evidence;
delete `_reclassify` and `report --refresh`'s rewrite; SQL Server emits `ok`
itself when its drilldown finds nothing; never hand live result dicts to a
renderer.

**V2. Nothing compared reads green** (P0, S). `verdict.summarize(hop, [])`
-> `same` (measured). `check` prints "all green (0 checks)" and exits 0 when
the database list is empty (MySQL `show databases` lists only what the user
may see, `mysql.py:1982`), when `--only` has a typo (`[c for c in
only.split(",") if c in allowed]` drops it silently, and 1180-1191 then
republish the old results under a fresh timestamp), or when every result is
`skip`. `watch --verify --delta` drops a database that raised
(`cli.py:3318-3324`, `continue`) and `_watched` tells the receivers "same",
resolving the incident. Fix: unknown `--only` names are a usage error (and in
`_monitor` 3364); zero results or zero databases is `incomplete`; a failed
cycle appends an `error` row; `incomplete` exits non-zero (V6).

**V3. `sync --mode migrate/stream --go` is a second move-and-follow path that
breaks what the `cutover` goal promises** (P0, M). `cli.py:_orchestrate`
1576-1633, `movers_pick_and_run` 1636-1652; `hopkeys.GOALS["cutover"]`
(hopkeys.py:508) recommends this command and says "the position is taken
before the copy". No position is taken before the seed; PostgreSQL then runs
`_replicate(..., copy_data=True)` (1605, `postgres.py:10292`) over tables the
seed loaded (duplicate keys, doubled keyless rows); the seed calls
`movers.run_via` directly, skipping `_copy_routed`, `_held_to_the_source`,
`_refuse_if_nothing_arrived`, `_stop_if_the_schema_moved`, `_record_copy`,
`refuse_unpushable_filters`, `refuse_unfit`, the fallback and
`_protect_target`; `migrate` exits 0 after "seed has residual diffs"; the dry
run names `migkit move ... --go` but `--go` runs other code. Fix: the
orchestration calls `_move_run(..., mode="full+cdc")` and the one check, and
nothing else; delete `movers_pick_and_run` and the hand-written CDC dispatch.

**V4. Write paths that skip masking, read-only goals, approvals and the
cutover path** (P0, S-M). Hidden aliases `tail` (3905-3913) and `replicate`
(3890-3898), `_orchestrate` 1601-1607; the only masking guard (`mode !=
"full" and mask_on_write` -> refuse) is inside `_move` (2908). So `tail
--go`, `replicate` and `sync --mode stream --go` stream real values into a
hop that masks columns; they skip `hopkeys.refuse_writes` (a `goal: verify`
hop gets written); `replicate --drop --go` tears down without
`approvals.require(hop, "cutover")` or `cutover.run`. Fix: one
`_guard_write(hop, op, mode, drop, go)` - refuse_writes, masking, approvals,
interlock, protect_target - called by every writing entry point; the aliases
become thin wrappers over `_move_run` and `_repair`.

**V5. `sync --go` is a third repair path** (P0, M). `cli.py:_sync_go`
1434-1503: `eng.check_data(d)[0]` and `check_autoinc(d)[0]` read only the
first result (MongoDB answers one per collection; PostgreSQL appends mapped
tables after its own), so a first `ok` prints "data: ok", repairs nothing and
exits 0; no `approvals.require(hop, "repair")` (the `--apply` path has it at
1314); no `_make_room_for_rows` (races migkit's own tail); schema never
repaired though `kind="all"`; exits 0 when the re-check still differs; the
restore point sits in `tempfile.mkdtemp` until line 1500 with no
`try/finally`, so a crash leaves the target changed and the undo in an
unreferenced `/tmp` directory (ST3). Fix: `--go` = restore point + `_repair(...,
do_apply=True)` + the one check; exit from the re-check.

**V6. Exit codes collapse; the schedule and the scheduler disagree** (P1, M).
Every `SystemExit("text")`, traceback and Ctrl-C exits 1, the same as a
difference; `doctor` always exits 0 even with FAIL rows; `watch` and `sync
--mode migrate/stream` always 0. A scheduled check counts exit 1 as success
(`ok = e.code in (None, 0, 1)`, 1020), so an unreachable target never pauses
the schedule; `schedule.run` (inside `report --serve`) counts any non-zero as
failure (`schedule.py:154,164`), so a real difference is retried and the
schedule pauses after three slots. Measured table of what each command
returns:

| Command | Exits 0 | Exits 1 |
|---|---|---|
| `check` | ok/skip only - including 0 rows, all-skip, narrowed runs, report-reclassified diffs, not due | diff, error, warn, stale, config errors, tracebacks, Ctrl-C |
| `assess` | no fail rows | any fail row |
| `doctor` | always | only on a crash |
| `move` | done, dry run, not due | every stop; tracebacks for unlisted drivers |
| `sync` reconcile | dry run with diffs; after `--apply` (unverified); after `--go` even if still different | refusals |
| `sync --mode verify/seed` | same | different |
| `sync --mode migrate/stream`, `watch` | always | - |
| `rollback` | including "nothing restored" | "no saved states" |

Fix: an exit-code table in `verdict` - 0 same/done, 1 different, 2 usage, 3
could not verify / incomplete, 4 refused by a guard, 130 interrupted -
mapped once in `_Step.invoke`; the scheduled check and `schedule.run` read the
same table.

**V7. Tracebacks and raw program errors reach the operator; only `move`
cleans them** (P1, M). `_move_run` 2775-2828 is the only wrapper; `check`,
`sync`, `watch`, `rollback`, `history`, `assess`, `doctor` raise through
`session.step` (re-raises, session.py:168-170). `_psql` raises
`RuntimeError((stdout+stderr)[-400:])` (postgres.py:7163) so "psql: error:"
lands in a traceback; the check's error row stores `str(e)` raw into
`verdict.json`, `report.html` and the AI prompt. Two driver lists:
`_refused_by_a_server` (2834) knows 9 driver modules, `decide._DRIVERS` 25.
Two scrub lists (measured): the console filters 16 names
(`movers.DRIVEN`), the evidence export 47 (`export._names()`); `mysqldump`
and `sqlcmd` are spawned but not in `DRIVEN`. Fix: the `_move_run` handling
moves into `_Step.invoke` for every command; one scrub (`export.words` +
`wording.database_words`, names from the tool registry, W14) for every
console line, `detail` and verdict string.

**V8. Raw text printed as Rich markup** (P1, S). Result lines 1174-1175,
repair plans 1389, replication statements 2445, `_delta_loop` 3323/3333,
`doctor` 514, `_setup_target` 815, the `[db]` prefixes 1079/1107/3321.
Measured: `'[dbo].[orders] src=10 dst=7'` prints `. src=10 dst=7`; text with
`[/x]` raises `MarkupError` from the as-completed loop before `summary.json`
is written; SQL Server repair statements are mangled in the dry run the
operator approves; output that is not a terminal is hard-wrapped at 80
columns, splitting "verdict: different" from its path in CI logs. Fix:
`Console(soft_wrap=True)`, one `say_raw()` with `markup=False` for data, SQL
and exception text, `\[{d}]` for the prefixes.

**V9. The dry run is computed by different code from `--go`** (P1, S-M).
`_cdc` 3155-3157 passes `where=where if go else None`, and
`decide.given_up(None)` is `{}` (decide.py:849), so the dry run can name a
way `--go` will not take; `move --mode full` dry run calls `_bulk_db`
directly (2978), `--go` goes through `decide.attempt(where=move.json)`
(3099); `refuse_unfit` (2155, 3027), `movers.options_missing` and
`_still_running` run only with `go`. Fix: `attempt(..., where=where,
record=go)` - always read, write only with `go` (additive; decide.py belongs
to P0-SMART); dry runs call the same guards in "say, don't stop" mode, as
`interlock.before_write` already does.

**V10. `check --resume` presents old answers as fresh; `--only --db` drops
sibling databases** (P1, S). 1057-1059, 1112-1116, 1186-1189: resumed rows
keep `ok` however old ("resume: skipped, was ok"); only `drift.shape`
guards, which sees source schema, not data or target changes (and SC3 shows
how little it sees); `keep(r)`: `not r.get("scope","").startswith(db)`
deletes `shop_archive` when `--db shop`. Fix: resumed rows carry
`reused_from`, and the verdict is `incomplete` unless the source's change
position has not moved since; parse the database from the scope with the one
helper report and ui use.

**V11. The same catalogue is read 5-7 times per database per `move`** (P1,
S). `eng.table_facts("src", d)` in `_table_plan` 1668, `_units` 1969 (twice),
`_source_rows` 1790, `_largest_first` 1881, `_copy_tables` 1917,
`machine.size_of` (twice), `legs.py:635/820`, `model.build:614`; only
`model.for_move` caches. PostgreSQL's `table_facts` runs `pg_table_size` and
`pg_indexes_size` per relation (postgres.py:488-515). `interlock.before_write`
(both identities and the cloud lookup) runs three times per database per
`move --go` (2944, 2961, 2129); `eng.databases()` 3-4 times. Fix: a per-run
memo inside `Engine` for `table_facts`, `databases` and `server_identity`
keyed (side, db), cleared by `forget_shapes()`; `interlock` keeps its answer
per (hop, db, run).

**V12. `doctor` and `init` are serial and repeat their work** (P1, M).
`doctor` (478-539) builds three engines per hop, reads `databases()` twice,
probes every hop and side one after another (an unreachable host costs its
whole connect wait each time); `init` asks for columns table by table up to
200 queries (guide.py:416), runs `dba.needs` twice (344, 576), `_eta` re-reads
`table_facts` it has (610). Fix: one engine per hop and a thread pool across
hops and sides in `doctor`; `init` passes the survey's facts to `plan` and
`_eta` and reads the columns in one catalogue query.

**V13. `watch --verify` and the `sync` stream loop are copies of `check`
and the delta loop** (P1, M). `_monitor` 3360-3396 and `_orchestrate`
1618-1633: serial, no stale/drift handling, a different `--only` set,
`_monitor` overwrites `report.html` with a partial cycle while
`verdict.json` describes the last full check; the `sync` loop writes no
`delta-summary.json` and notifies nobody. Fix: `run_checks()` (pure, returns
results) called by `check`, `watch --verify` and `sync`; the `sync` stream
step calls `_delta_loop`.

**V14. `watch` says "SAFE TO CUT OVER" from estimates** (P0 wording, S; the
decision itself may overlap the cutover gaps). `_cutover_verdict` 3399-3419:
samples are `n_live_tup` / `information_schema.tables.table_rows` (MySQL's
read without `fresh=True`, cached up to 24 h by
`information_schema_stats_expiry`, `mysql.py:7877-7883`); `dst >= src`
passes with extra target rows; updates and deletes are invisible; the MySQL
sample has no `src_tables`, so "source still HOT" never fires;
`docs/index.html` repeats the claim. Fix: "counts converging (estimates)";
SAFE only after a delta or consistent verify in that cycle, deciding from
`changes_made` and the tail's `behind`.

**V15. Flags silently ignored or reinterpreted; smaller flows** (P1, S).
`watch --teardown` without `--delta` starts an endless count watch instead
of dropping the delta slot (3502; the source keeps WAL); `move --mode full
--drop --go` asks for the cutover approval and ignores `--drop`; `full+cdc
--drop --go` tears down through `_replicate` outside `cutover.run`; `sync
--kind` ignored in modes; `--tag`/`--interval` ignored without
`--go`/`--serve`; `sync --mode seed` dry run runs three full checks and a
whole-database repair plan against a target it never loaded, then exits 1;
`rollback --apply` thaws the application's writes before restoring (3735-3739
vs 3751-3765), takes the lock three times, and its footer says "restore
sequences" only; `history` connects to the source to list databases
(traceback when it is down); `init` with the same two addresses adds
`shop-2`, and the `--force` it suggests rewrites the whole file (guide.py:69,
706); Ctrl-C waits for all in-flight work with no word; `report --open`
calls macOS `open`. Fix: per-command flag validation with
`click.UsageError`; rollback under one lock, thaw last; `history` from the
state store; `init` updates the hop with the same source and target.

**V16. Program names in user-facing CLI text** (P1, S). `advise` playbooks
name pg_dump/pg_restore, mydumper, xtrabackup, gh-ost, pt-osc, mongodump,
mongosync, RIOT, redis-shake, MirrorMaker2; the `schema` default prints
hand-run pg_dump/sqlcmd/mssql-scripter steps while `move` creates missing
tables itself; `_stream` 2716 "docker compose ... logs"; 917 "Flyway-style",
1534 "DMS-style"; `base.watch_sample` shows a class name. Fix: migkit's words
(and W3/W4 for the runtime leaks and the guard test).

**V17. `doctor`'s reader check and capability table disagree with the rest**
(P2, S-M). Reader flagged through PostgreSQL's private `_in_recovery`
(492-496) while `init` uses `sizing.capacity()["replica"]`; `doctor` shows
`tools.CAPABILITIES` (installed programs), the commands enforce
`capabilities.matrix` (engine methods). Fix: both from the same sources as
the commands.

---

## 2. State, resume and crash safety

**Inventory (measured by grep/AST).** About 20 write helpers, 213
`write_text`/`write_bytes` calls; fsync in three places
(`tailtables.Spool.add/rewrite`, `audit.append`, the copybook data file);
no directory fsync anywhere. Four state roots (`REPORTS/<hop>[/<db>]`,
`~/.migkit-state/<hop>`, `$TMPDIR/migkit-state-*`, `<prefix><hop>/run/` in
S3), plus `~/.migkit` for tools and `~/.cache` for the machine catalogue; the
tail position's path is computed in four places (`cli.py:2510`,
`hetero.py:2602`, `mongodb.py:1312`, `kafka.py:533`). Four token envelopes
(hetero `{"token","batch","rung","next"}`, tailctl/kafka `{"token"}`, Mongo
the bare resume token, delta verify bare `{"log_file","log_pos"}` in five
files). Only `checkpoint.json` carries a version, and it discards on
mismatch.

| Writer | Atomic | fsync |
|---|---|---|
| `tail-token.json` (hetero 3217, 3262, 3636, 3656, 3949, 3982; marks 327, 398; tailctl.begin 274; mongodb 2305-2390) | no, in place | no |
| `tail-token.json` (tailctl.follow 325-328, kafka 457-459) | rename | no |
| `move.json`, `tail-copy.json` (`cli._Checkpoint.save`) | fixed `.tmp` + rename | no |
| `checkpoint.json` | `mkstemp` + rename | no |
| `tail-tables.json`, `lease.json`, `*-ways.json`, freeze/jobs/cutover/interlock/sourcechange/physical/machine records | rename | no |
| `tail-accounts.json`, `tail-shape.json`, `tail-source.json`, `delta-*.json`, `schedule.json`, `copy-position.json`, `dump-position.json`, approvals requests | plain `write_text` | no |
| spools | append/rewrite | file only |
| `kept-leaves.sqlite` | WAL, `synchronous=full` | yes |
| `changelog.jsonl` | append under flock | yes |
| `*.repair.jsonl` | append | flush only |

**ST1. Nothing is durable; the tail's per-batch commit spans five files with
no barrier** (P0, M). `hetero.tail_apply` 3972-3990 -> `tables.commit`
(tailtables 999-1011) -> `keeper.saved` -> token `write_text` (3982) ->
`accounts.save` (4521-4530). The order is right against a killed process
(the failpoint tests prove that) but not against power loss: a newer token
beside an older `tail-tables.json` lets a table parked in batch N+1 revert to
`live` while its fsynced spool stays (`release()` drains only copying/parked
tables - rows lost); an in-place token overwrite can be empty, and
`_saved_token` then asks for a full re-copy; `TableSet.__init__` resets every
table state on a torn file (`except (OSError, ValueError): doc = {}`),
orphaning every spool. Measured per batch: token `write_text` 0.24 ms,
temp+rename 0.50 ms, temp+fsync+dir fsync 0.39 ms, `F_FULLFSYNC` 3.9 ms;
SQLite WAL `synchronous=full` token + one table state 0.055 ms; today's
`tail-tables.json` (2,000 tables) + `tail-accounts.json` rewrite 2.3 + 2.8 ms
of `dumps` and 350 KB a batch. Fix: `durable.write_atomic(path, data,
mode=0o600)` (unique `O_EXCL` temp, fsync, `os.replace`, directory fsync,
`F_FULLFSYNC` where asked), `append_line`, `read_state(kind)`; and the tail's
token, table states, accounts and spool frames as rows of the tail's SQLite
(`kept.Store`), one transaction per batch.

**ST2. Two-way exact batches can be applied twice after a power loss** (P0,
S). `twoway.committed_ahead` 162 recognises a target exactly one batch ahead
(`got["batch"] == batch + 1`); a token rolled back k batches falls through
to `return None` and the tail re-applies k batches, adding counters twice.
Fix: ST1's durable token; for the table rung accept `batch < got.batch <=
batch + K` (the mark carries its own token). Not a one-liner: `> batch`
would match marks of an earlier run (`_saved_batch` starts from a random
48-bit base). Touches `twoway.py`, which the conflict-resolution work owns.

**ST3. The `sync --go` restore point lives in `/tmp` until the end** (P0,
S) - folded into V5.

**ST4. A resumed `--mode full` copy takes a new change position, so the
later `--mode cdc` misses changes** (P0, S). `cli._move_full` 2163/2201,
`_record_copy` 2542-2556, `_bulk_db` 3035/3058: `point =
_position_before_copy(...)` is kept in memory and written after the whole
copy; a resume takes a fresh one; `recheck_done` (`base.py:3680`) re-digests
only tables marked `done`, so finished ranges of the table that was mid-copy
lose the changes between the runs. Fix: always `point = _first_point(ck,
point)` (2220-2228 persists `__before__` into `move.json` before copying) -
today only when `share_tables` is on.

**ST5. A second machine taking over a tail over S3 drops held changes** (P0,
S). `cli.RUN_FILES` 180-181 and `state.Mirror` 363-416 do not mirror
`tail-spool/*`, `tail-accounts.json` or `kept-leaves.sqlite`; the new machine
reads `parked`/`copying` from `tail-tables.json` and `Spool.frames()` returns
`[]` for the missing file; `Mirror.fetch` keeps any local file, so a stale
local token beats the newer remote; each file is pushed on its own every 2 s
while another thread writes it in place (torn remote token). Fix: the tail
state as one file (ST1's SQLite) or one manifest with a generation, mirrored
whole, the higher generation wins.

**ST6. The copybook target can duplicate records after a crash** (P0, S).
`engines/copybook.py` 588-612: data fsynced, then the batch index written
with plain `open(..., "w")`; a crash between leaves no entry (a torn index
reads `[]`), so the replay does not `truncate(begin)` and appends the batch
again. Fix: write `[batch, begin, None]` durably before the data; truncate to
`begin` on replay.

**ST7. The state root follows the working directory for an installed copy**
(P0 for pip/pipx/brew, S). `config._reports_root` 43-54 returns
`Path.cwd()/"reports"` while `CONF` falls back to `~/.config/migkit`. Run
from cron's `$HOME` or systemd's `/`, the same hop gets a fresh state
directory: no `lease.json` (two writers), no tail token or copy position (the
tail starts from now, and the warning at `_tail` 2596-2606 needs a
`move.json` to exist), no half-done cutover record to undo, a fresh schedule.
The deploy files set `MIGKIT_REPORTS`, which hides it there. Fix:
`$XDG_STATE_HOME/migkit` (or beside the resolved config), never the working
directory; the restore-point mirror under the same root; `doctor` prints
both paths.

**ST8. The tail, the follow loops and the cutover take no lock** (P0 for
cutover, P1 tail, M). `_tail` (cli.py 2579-2614), `tailctl.follow`, the Mongo
and Kafka tails never take the hop lease; `tailctl.Running.__enter__` (72-80)
overwrites `tail.pid` without checking and `__exit__` deletes the other
tail's `PID`/`PAUSED`/`BEAT`/`STOP`; two tails on one database both apply and
write the token. `cutover.run` (745-762) reads a `"running"` record as a
crashed run and undoes everything - the record (881-885) has no host, pid or
lease, so a second `--go` undoes a cutover in flight. Fix: one `Lease` per
scope (`lease-tail-<db>.json`, the hop lease for cutover); `tailctl.alive` =
the lease has a live holder; the per-batch `tail.beat` is the heartbeat.

**ST9. Four behaviours for one torn state file** (P1, S). Measured on the
same torn JSON: `cli._Checkpoint` raises `JSONDecodeError` (a traceback on
every move), `checkpoint.Checkpoint` silently starts over,
`tailctl.saved`/`hetero._saved_token` stop with a clear message,
`marks.saved` returns `{}`. Silent elsewhere: a torn `tail-shape.json` skips
the DDL gate for that start; a torn `tail-source.json` re-baselines the
source identity (defeats fail-over detection); `freeze.state`,
`jobs.recorded`, `cutover.record` return nothing, so thaw or undo finds
nothing; `decide._read` returns `{}` and a restart climbs a different rung
than the kept one. Fix: `durable.read_state(kind)` with one policy per kind -
positions and undo journals stop naming the file, caches read as absent.

**ST10. Four "is it still running?" checks** (P1, S). `lease._dead`/`holder`
(expiry + same-host pid; `PermissionError` = alive); `tailctl.process_alive`
(another host alive forever, no expiry, ignores the `tail.beat` it writes);
`schedule._alive` 77-82 (pid only, no host, `PermissionError` = dead); the
cutover record (none). On Kubernetes each restart is a new host, so a
set-aside index or trigger record from a killed pod is alive forever
(`setaside.records` 50) and never put back; a stale `tail.pid` makes
`tailctl.pause` time out and cutover and repair refuse. Fix: one
`liveness(record)` over the lease record (host, pid, expires); the beat age
decides for another host.

**ST11. Losing the lease is never signalled** (P1, S). `lease._renew`
168-184 swallows renewal failures (`except Exception: continue`) and returns
when another holder takes over; the operation keeps writing (split brain,
worst over S3). Fix: a `lost` event the write loops check between batches
and ranges, raising.

**ST12. Undo journals written ahead but not durably; one torn line makes a
repair record unreadable** (P1, S). `repairlog.Record.ready/applied` only
`flush()` (78-87); `repairlog.read` (101-114) `json.loads` every line, so a
torn last line fails rollback (`Spool.frames` already tolerates it);
`freeze._save` 66-71, `jobs._save` 109-112 and the cutover counters `_save`
194-197/1194-1196 record before the change (right) without fsync. Fix:
`durable.append_line`/`write_atomic`; tolerate a torn last line. (repairlog
may be touched by the repair-ladder work; freeze/cutover by the cutover
work.)

**ST13. Repairs crash when the hop encrypts at rest** (P1, S).
`evidence._LineSealer` 146-171 has no `flush`, and `repairlog.Record` opens
`undo/*.repair.jsonl` through `EvidencePath.open("a")`; measured:
`_LineSealer` has no `flush`, so `ready()` raises `AttributeError`. Fix:
`flush()` sealing the pending partial line, and `fileno()` for fsync.

**ST14. Five tail loops that behave differently** (P1, M).

| Loop | `tail.stop` | `follow_until`/slot end | transient retry | SIGTERM | beat | why it stopped | token write |
|---|---|---|---|---|---|---|---|
| `hetero.tail_apply` | yes | yes | yes | yes | yes | yes | in place |
| `tailctl.follow` (ClickHouse, OpenSearch, Cassandra) | no | no | yes | no | yes | yes | atomic |
| `mongodb.tail_apply` 2307-2390 | no | no | no | no | no | never | in place |
| `kafka.tail_apply` 446-481 | no | no | no | no | yes | yes | atomic |
| `movers._follow_until_asked` | yes | n/a | n/a | yes | n/a | n/a | program's own |

A scheduled follow on those engines never ends by itself: it holds its slot
until `max_duration` and the slot counts as failed. Fix: one
`tailctl.run_loop(look)` owning stop, until, pause, beat, retry, SIGTERM and
the save; each engine supplies `look(token) -> (after, n, settled)`.

**ST15. Permissions and two sealing schemes** (P1, S). `report_dir` plain
`mkdir` (0755, files 0644); restore points hold full-row undo in plain text
under `REPORTS/.../state/<ts>` and `~/.migkit-state/*.tar.gz` unless
`MIGKIT_STATE_KEY` is set; spools hold pickled values; `MIGKIT_STATE_KEY`
(Fernet) covers restore points and S3 run state, `at_rest` (age) covers
`undo`, `data-*` and spools - neither covers both. Fix: the durable helper
makes files 0600 and directories 0700; one sealing policy (`at_rest`) applied
inside it to every file that holds values.

**ST16. One torn audit line breaks the chain for good** (P1, S). Reproduced
in-process: tear the last line of `changelog.jsonl`, append twice; `verify`
returns `(3, 1, 'entry 2 is not a record')` forever - the next append is
glued onto the fragment. A crash is indistinguishable from tampering, and
nothing after it verifies. `_last_hash` reads the last 64 KB only; the S3
anchor holds the bundle's digests, not the chain head. Fix: before writing,
truncate to the last `\n` or write a `{"torn": ...}` marker; `verify` reports
a torn tail apart from a break; the head hash goes in the anchor. See SEC15
for the opening-stretch hole.

**ST17. The copy checkpoint rewrites the whole file twice per range under
one lock** (P1, M). `ranges.started`/`finished` call `ck.save` (299, 308);
`cli._Checkpoint.save` dumps the whole `move.json` with `indent=1` (which
disables the C encoder) under a process-wide `RLock`; `ranges_seen` keeps a
mark per range and is never pruned; `finished` rebuilds `set(ranges_done)`
each time. Measured (187-byte marks):

| Estate | File | Per save |
|---|---|---|
| 50 tables x 20 ranges | 0.26 MB | 2.9 ms |
| 200 x 50 | 2.56 MB | 42.5 ms (about 25 GB written over the copy) |
| 1,000 x 50 | 12.8 MB | 215 ms |
| 2,000 x 100 | 51 MB | 688 ms |

One 2-billion-row table in 20k ranges: 11.8 s of O(R^2) bookkeeping, ~14
minutes of serialised saves. `indent=1` alone: 46.4 ms vs 7.9 ms compact on a
4.3 MB file. Fix: an append-only per-table progress journal (or SQLite rows)
with O(1) appends, marks per range row, no O(R) scan; `separators=(",",
":")` at once.

**ST18. A table copied beside the tail re-reads and rewrites its whole spool
every batch** (P1, M). `TableSet.release` 837-870 reads, decompresses and
unpickles every frame; `commit` calls `Spool.rewrite`. Measured: 200k held
changes (2.5 MB) 259 ms + 126 ms a batch; 1M (12.8 MB) 2.49 s + 1.10 s,
super-linear; `SPOOL_MOST` is 1 GiB. Fix: frames indexed by the range they
wait on (ST1's SQLite), released rows deleted, not the file rewritten.

**ST19. Rewrites that need not happen** (P2, S). `TableSet.commit` saves
`tail-tables.json` every batch and every idle loop (hetero 3988-3989) though
nothing changed; `accounts.save` rewrites every table's totals;
`TableSet.begin` deep-copies `tail-copy.json` through JSON every batch
(784-786) and never prunes it; `tailctl.follow` writes the token every idle
second. Fix: dirty flags.

**ST20-ST25** (P2, S each). ST20: no schema version (`kept-leaves.sqlite`
has no `user_version` and `create table if not exists`; tokens migrated by
guessing, `marks.kept_rung` 280-289) - an envelope `{"v", "kind"}` plus
`MIGRATIONS[kind]`, `pragma user_version`. ST21: one `Position` envelope
`{"v", "engine", "source", "position", "batch", "rung", "next"}` in one file
(today `tail_seed`/`tail_start`/`tailctl.begin` write the token, delete
`tail-source.json`, write it again - a crash between re-baselines the
source). ST22: `LocalStore._commit` does `rmtree` then `copytree` (a crash
loses the older point) and names points to the second (two in one second
overwrite) - copy to `<ts>.partial`, rename, random suffix. ST23: two
`check` runs overwrite each other's `checkpoint.json`; fixed `.tmp` names in
`decide._write` (read-modify-write, no lock), `planner.progress`,
`model.save`, `machine`. ST24: `_Checkpoint.save`'s retry loop (cli.py 91-96)
raises `UnboundLocalError` after 20 retries; the retry exists because copier
threads mutate entries outside the lock - take the lock in `ranges.*`, drop
the retry. ST25: `tailtables.spool` 340-343 maps every other character to
`_`, so `a b` and `a_b` share a spool - add a short hash.

---

## 3. Config, options, environment, schedule and approvals

**The option registry holds.** An AST scan of all 169 modules, following
options into 20 helpers that return a sub-dict (`spec_of`, `rules`,
`twoway.settings`, `physical.settings`, `state._cfg`, `sandbox.spec`,
`model._endpoint`, `cloudseed._opts`, `tailctl.until`, ...) found no option
or sub-option key read that `hopkeys` does not declare. Undeclared below
sub-key level: `cutover.pooler.{kind,host,port,user,password,databases}` and
`mapping.columns.<t>.{keep,drop,rename}`. Load cost measured for 100 hops:
YAML 26.5 ms (pure-Python loader; `CSafeLoader` 0.5 ms for 40 hops against
4.3 ms), `hopkeys.check` 1.7 ms, `build_hop` 1.0 ms; the real cost is secret
resolution (K12).

**K1. Typos and bad values load clean, then disable safety or fail hours
in** (P0, M). `hopkeys.py:_keys` 801-811: an unknown sub-key is a `"warn"`
only `doctor` prints; values are checked for `goal`, `target_rows`,
`source_changes`, `notify.events` only; the rest are checked where first
used - `tailtables.policy` 86-92 at tail construction (after the whole copy
in `full+cdc`), `tailctl.until` 39-52, `twoway.policy` 63-80,
`cutover.settings` 85-121, topology/reverse at `cli.py:2310-2318`,
`Hop.tolerance` `float()` at config.py:497. Measured: a hop with
`approvals: {step: [cutover]}`, `notify: {too: ...}`, `schedule: {crn: ...}`,
`mapping: {colums: {public.users: {drop: [ssn]}}, columns: {public.orders:
{dorp: [...], renmae: {...}}}}`, `on_conflict: keep_target`, `reverse:
at-cutover`, `topology: two-way`, `ddl: parkk`, `follow_until: forever`,
`cutover: {budget: "30s", freeze: servr}` loads with zero errors; after it,
approvals for cutover: off; receivers: none; schedule: none; the `users`
column rules: `{}` (SSN copied); `dorp`/`renmae` say nothing. Fix: `Key`
gains `choices=` and `check=` (calling the owning module's validator at load,
its `SystemExit` turned into an error); unknown sub-keys are errors (the scan
shows the lists are complete); the nested subs declared.

**K2. Duplicate YAML keys overwrite each other** (P0, S). `config.py:677`
(and `tailtables.py:233`, `cli.py:562`, `diagnostics.py:73`): a hop with two
`options:` blocks loads as the second (measured: `protect_target` and
`approvals` gone, zero problems). Fix: one loader (a `CSafeLoader` subclass
refusing duplicate keys) for all four readers.

**K3. `sync` overwrites the hop's `on_conflict`** (P0, S). `--on-conflict`
defaults to `"source-wins"` (cli.py 1525-1528) and 1288/1441 assign it to
`hop.options`, so a hop's `keep-target` never holds in `sync` - the repair
overwrites rows the hop said to keep; readers compare `== "keep-target"`
(`base.py:2025`, `postgres.py:7197`, `mysql.py:5425`), so `keep_target` means
source-wins. Fix: CLI default `None`, `on_conflict or hop.options.get(...) or
"source-wins"`, choices at load.

**K4. `MIGKIT_SCHEDULED` is half honoured and turns verification into a
green skip** (P0, S-M). Only `check` and `move` gate (cli.py 1008, 2759);
`sync --mode verify/seed/migrate` `ctx.invoke(check)`, whose gate raises
`SystemExit(0)` when the hop's `run` is not `check` (no log line) or the slot
is not due (schedule.py 109-116); `_run_check` (1548-1555) returns `not
e.code` = True; `sync --mode verify` exits 0 having compared nothing and seed
prints GREEN; `MIGKIT_SCHEDULED=false` counts as on (106). Fix: gate once in
`_Step.invoke` for every command, "not due" as a `NotDue` exception, nested
invokes skip the gate, every skip logged, flags parsed by `envkeys` (K13).

**K5. Schedule liveness is a bare pid in an unlocked, non-atomic file**
(P0, S-M). `schedule.py` 77-82, 88-89, 117-132: in a container migkit is pid
1; after an OOM kill `running=1` stays and every later pod is pid 1 too, so
every minute says "still going" and exits 0 forever; two pods in the same
minute both run the slot. `lease.py:1-10` describes and fixes exactly this
elsewhere. Fix: hold the slot with `lease.Lease` (`schedule.lease`), state
written through the durable helper, host/pid/start recorded.

**K6. `report --serve`'s scheduler dies silently and runs hops one after
another** (P0, S). `schedule.py:170-189` has no try/except: a config mid-edit,
one hop's missing secret or a bad cron/zone ends the daemon thread
(`threading.excepthook` ignores `SystemExit`) while the dashboard keeps
serving; `run()` blocks up to `max_duration` (14,400 s), so another hop's
slot missed by more than `catchup` (1,800 s) is skipped. Fix: per-hop
try/except that logs and notifies; cron and zone validated at load; due slots
run concurrently, one per hop.

**K7. Secrets kept in options are never resolved** (P0, S). `_secret` runs
only on host/user/password (config.py 658-662) and in notify:
`schema_registry_password` (registry.py 114-115), `mine_password`
(oracle_logminer.py 596), `private_key_passphrase` (warehouse.py 837/1089),
the pooler's password (cutover.py 274-279) are read raw; `hopkeys.py:936-941`
tells the operator to write `env:NAME` there, which then goes out as the
literal text. Fix: `Key(kind="secret")`, every secret key resolved once and
lazily through `config.secret()`; the plain-text warning for all of them.

**K8. `doctor` says TLS options do nothing on cross-engine hops** (P0, S).
`hopkeys.py:1084-1085` and `1126-1128` called `ALIASES.get(x)` with no
default; `ALIASES` holds aliases only, so a canonical `mysql` became `None`.
Measured: for `mysql://...?ssl-mode=VERIFY_IDENTITY&ssl-ca=...` ->
`postgres://...?sslmode=verify-full`, doctor said `source.ssl_ca ... does
nothing on this None one` (and the same for `target.sslmode`, `server_id`,
`atlas`, `reladiff`, `checksum`) - an operator following it removes
certificate verification. **Fixed in this audit** (`ALIASES.get(x, x)`, as
every other caller does); after it the same hop draws no TLS remark.

**K9. Approvals can be bypassed** (P0, S-M). `sync --go` repairs without
`approvals.require` (V5); the requester is `getpass.getuser()`
(approvals.py 37-41, from `USER`/`LOGNAME`) compared against a signer
principal (usually an e-mail) at 106 - they never match, so "never the
asker's own" does not hold, and the request file's `asked_by` can be
rewritten and signed; `count: 0` approves with no signature (101, 109);
step names are not validated (`[Cutover]` never matches). Fix: approvals
required inside the one repair executor; the requester signs the request
(`MIGKIT_IDENTITY`, `evidence.py:54`) and that key's signatures are
excluded; `max(1, count)`; step names at load.

**K10. `state.backend` read two ways** (P0, S). `state.py:36-43` raises on
an unknown backend only when a store is needed; `run_state` 278-283 (the
lease's) tests `== "s3"` and otherwise falls back to local silently:
`backend: S3` keeps the lease local and two machines can both write. Fix:
choices at load (K1), one reader.

**K11. The state root follows the working directory** - see ST7.

**K12. Every hop's secrets resolved on every load, several loads per
command** (P1, M). `config.py:687-688` builds every hop; `_endpoint` resolves
each one's secrets (env, file, vault CLI, AWS/GCP/Azure APIs, keyring).
Measured: hop b's unset env var makes `get_hop("a")` fail with "secret env
var '...' is not set", naming neither hop nor side. Loads per command:
`check` 2, `sync --mode seed` 7, the dashboard on every unauthenticated
`/metrics` scrape and every `/api/data` poll, the scheduler every 30 s; no
cache. Fix: lazy resolution (generalise `KeychainEndpoint`, config.py
308-325) with a per-process cache; `get_hop` validates the file and builds
only the named hop; memo by (path, mtime_ns, size) as `tailtables._HopFile`
does; `CSafeLoader`; errors name `hops.<hop>.<side>.password`.

**K13. No registry of environment variables; four parsings of "true" and
four of a number** (P1, M). 71 distinct `MIGKIT_*` names in the code, ~25
read and documented nowhere (70 read / 37 in no doc by the docs reader).

| Rule for "on" | Variables |
|---|---|
| `1/true/yes/on` | `MIGKIT_CROSSCHECK` (postgres.py 5027), `MIGKIT_REPAIR_TEXT` (base.py 1401) |
| anything but `""`/`"0"` (`false` = on) | `MIGKIT_SCHEDULED`, `MIGKIT_TAIL_APPLY_ALL` (hetero.py 4290) |
| any non-empty (`0` = on) | `MIGKIT_SCHEDULE_RESUME` (schedule.py 91) |
| only `"1"` (`true` = off) | `MIGKIT_CATALOG_REFRESH` (machine.py 705) |
| anything but `"false"` | `MIGKIT_FAIL_ON_DIFFERENCE` (integrations.py 139) |

Bad numbers: a clean error (`MIGKIT_SUBSCRIBE_TIMEOUT`,
`MIGKIT_LEASE_SECONDS`), a silent fallback (`MIGKIT_STALL_SECONDS`,
`MIGKIT_GENERATIONS`, `kept._env`), a traceback (`MIGKIT_READ_TIMEOUT`,
`MIGKIT_SEED_START_WAIT`, `MIGKIT_CLOUD_*`, `MIGKIT_FOLLOW_TIMEOUT` after the
follow ran, `MIGKIT_PROBE_*` at import in model.py 64/70), or pasted raw into
a conninfo (`MIGKIT_KEEPALIVE_IDLE` at postgres.py 294 and legs.py 412, while
postgres.py 2073 and pg_pass.py 71 `int()` it). Unprefixed:
`SEQUENCE_BUFFER`, `GRANTS_IGNORE_ROLES` (which replaces PostgreSQL's default
list but adds to MySQL's), `MONGO_*`. Fix: `envkeys.py` like `hopkeys`: typed
`flag()`/`number()`/`text()`, one error style naming the variable, checked at
start-up, did-you-mean for unknown `MIGKIT_*` names, a generated reference,
and a test that fails on a read outside it.

**K14. `follow_until` defaults differ; the slot's end is honoured only by
the cross-engine tail** (P1, S-M). `tailctl.py:39-45` defaults to `stopped`;
`movers.py:6091-6098` says an unset leg "catches up and ends"; `SLOT_ENDS` is
read only at hetero.py 3734/3861 - a scheduled PostgreSQL/MySQL `move --mode
cdc` never ends by itself. Fix: one `until(hop, scheduled=...)` read by every
follow loop (ST14's loop).

**K15. Defaults written twice and disagreeing** (P1, M). `settle`: 2.0 s via
`float()` in leaves.py 837, 0 via `int()` in base.py 4189, registry `int`
(so 1.5 is refused); `workers`: 4 in config.py 354 and 749 while the
registry says it has no ceiling when left out - `sync --mode seed` copies
with an unsized 4 (cli.py 1649) where `move` sizes (3040); `DEFAULT_PORTS`
holds 6 engines (config.py 529-532), ~8 more inline in engines; a long-form
hetero side gets port 0 (measured 0/0 vs 3306/5432 from the address form);
the hetero side engines default in 8 places. Fix: `Key(default=...)` and
`hopkeys.opt()` as the one reader; one port table on the engine classes;
`build_hop` always sets the side engines.

**K16. Importing another tool's task can widen the scope silently** (P1,
S-M). The include list is applied only through `found["left_out"]`
(guide.py 199-205), empty when the source is unreachable (285-289) - the hop
then moves every table and the include list is never mentioned; wildcard
`database.include.list` patterns are dropped with no note; the include is
frozen into today's names. Fix: a declared `include:` key of patterns
`Hop.excluded` honours; a note for every dropped pattern.

**K17. An address whose secret reference holds `/` is misparsed** (P1, S).
`addresses.py:169-175` splits at the first `/` before reading the user part.
Measured: `postgres://app:file:/run/secrets/pw@db1/shop` parses as host
`'app:file'`, databases `['run']`, zero errors; vault/aws-sm/gcp-sm
references with `/` fail as "the port is not a number". Fix: match
`user:<reference>@` before splitting.

**K18. Cloud detection by roles that can be copied** (P1, M).
`clouds.py:115-138`: `rds_superuser` and friends survive `pg_dumpall
--roles-only`, so a self-managed server restored from RDS reads as "Amazon
RDS" and `physical._managed_src` (303-313) refuses the page copy; the first
role in an unordered `string_agg` wins; TencentDB by private address reads as
self-run and the DBA scripts say `ALTER SYSTEM`. Fix: vendor settings that
cannot be created by hand first (names to verify per service), roles as a
hint.

**K19. Business rules: a one-sided rule dropped, a quadratic diff** (P1,
S). `rules.py:40-45` (`if src and dst`) drops a rule that names one side;
97-98 list membership - measured 2,000 rows 0.21 s, 8,000 rows 2.61 s (~7
minutes at 100k). Fix: `Counter` subtraction; a one-sided rule is an error at
load.

**K20. `topology`/`reverse` normalised five ways** (P1, S). `movers.py
3305-3309` accepts `two-way`/`two_way`/`bidirectional`; `cutover.py:357` and
`freeze.py:352` only `two_way`; `cli.py:2310-2318` refuses others;
`hopkeys.py:660` compares `reverse` without lowering, `cutover.py:141-143`
lowers. `topology: two-way` (the goal's own spelling) is not refused by the
cutover preflight, is two-way to the masking plan, and is refused later by
`move`. Fix: `hopkeys.topology()`/`reverse()` and a check at load.

**K21-K24** (P1/P2, S). K21: `freeze.supported` (172-173) looks at the hop's
engine label, so MySQL->PostgreSQL hops never get `protect_target` from the
cutover goal (hopkeys 703-713) though freeze is per side - test the target
side's engine (cutover-work file). K22: Vault read through the CLI only
(config.py 624-638, docstring promises `VAULT_ADDR`/`VAULT_TOKEN`),
`["data"]["data"]` fits KV v2 only (v1 -> `KeyError`), stderr swallowed; GCP
and Azure errors unwrapped - read Vault over HTTP with v1/v2 detection, one
error format. K23: `export.left_anything` (603-618) reads the whole
`changelog.jsonl` and `decisions.jsonl` at the end of every command,
including each per-minute "not due" tick - a per-run marker. K24:
`tailtables._HopFile` reloads `exclude` with no validation (a string becomes
one pattern per character); `cutover.py:1165-1174` passes `keys: apart`
through the process-wide `SEQUENCE_BUFFER`; `zones.infer` detects only zones
ahead of UTC (38-50); `get_hop` has no did-you-mean for hop names.

---

## 4. Security

Checked against `docs/threat-model.md`, `SECURITY.md` and the backlog's
security items; only what is not already listed there, or where they claim
what the code does not do. Already fixed from the backlog's list: S5
(publication `FOR ALL TABLES`), S10 (dashboard token in the URL); S1 partly
(`tls.auto`, but SEC2); R17a partly (MySQL bulk programs take explicit TLS,
not the automatic answer).

**SEC1. The change-stream stack published its connector API, holding both
databases' passwords, on every interface** (P0, S). `movers.py` 6566
(`ports: ["8083:8083", ...]`), connector configs 6467-6472 and 6546-6548
(`"database.password": s.password`); Kafka Connect's REST API has no
authentication, returns the configs, and accepts new connectors; migkit
itself only calls `http://localhost:8083`. **Port bound to 127.0.0.1 in this
audit.** Left: the passwords out of the connector JSON (Kafka Connect's env
config provider, `${env:MIGKIT_SRC_PW}`, value through the container's
environment file) so neither the API nor the `migkit_connect_configs` topic
holds them.

**SEC2. The hop's TLS is dropped on the main data paths while `doctor` and
`assess` report it verified** (P0, M). `tls._pg` (212-214) says "verified"
when the hop sets `verify-full`; not passed on by: the streaming copy, clone
and follow URIs (movers.py 5148-5151, 5232-5240, 5626-5645, 5903-5906, env
5971 - no `sslmode` anywhere in 4700-6100); PostgreSQL native replication
(`postgres.py:10263-10264`, `f"host={host} port={port} dbname={db}
user={s.user} password={s.password}"`, while `legs.py:122-130/858-864`
carries `_sslmode`); MySQL native replication (`mysql.py:7560-7564`: no
`SOURCE_SSL*`, and `GET_SOURCE_PUBLIC_KEY = 1` fetches the RSA key
unauthenticated); CLONE (`physical.py:1623-1625`, no `REQUIRE SSL`); the
page copy scripts (`physical.py:1317-1330`, `_pg_prove` 628-632, no
`PGSSLMODE`); pgloader (4187-4188); Debezium (`database.ssl*` marked "not
yet" 6845-6849, 6874-6879; JDBC URL 6540); the second reader (88-93);
pt-table-sync DSNs (mysql.py 5162-5167); URL builders (mysql.py 2143-2146,
2573-2574; hetero.py 2382-2383; leaves.py 1729-1731; postgres.py 2826-2828);
`_my_tls` (movers.py 2413-2444) maps explicit options only, never
`tls.auto`. Fix: one place on `config.Endpoint` renders the resolved TLS
decision into each form (libpq env and URI query, subscription conninfo -
`legs._sslmode` moves there - MySQL `SOURCE_SSL*` without
`GET_SOURCE_PUBLIC_KEY` when SSL is on, `REQUIRE SSL` for CLONE, mydumper
flags from `mysql_tls()` including the automatic answer, Debezium fields and
JDBC `sslmode`); a guard test like `test_no_program_is_handed_a_password.py`:
every argv, env or URL naming a host carries the TLS keys.

**SEC3. User sync puts source account names raw into admin DDL on the
target** (P0, S). `users.py` `_plan` 1768, 1809-1815, rollback 2074,
`_ms_create` 637-663 and `_ms_rollback` 713 (`[{name}]` without doubling
`]`), `_ora_create` 1008/1043: `CREATE ROLE "{r}" ...`, `CREATE USER
'{u}'@'{h}'`, `GRANT "{m}" TO "{r}"`, run over an autocommit simple-protocol
connection (1943-1946). A source role named `x" SUPERUSER --` becomes a
superuser on the target at `users create --apply`. Fix:
`psycopg2.sql.Identifier` on PostgreSQL, parameters for MySQL's
`CREATE USER %s@%s`, the engine's own `_q` elsewhere (SQL Server's
`mssql.py:878` doubles `]`).

**SEC4. Raw identifier quoting in the PostgreSQL and MySQL engines** (P1,
M). `postgres.py` has `_quote_ident` (2610) and ~40 raw `f'"{name}"'` sites
(3432, 7182, 10099, 1084-1086, 6254-6257); `mysql.py` has three helpers (164,
3469, 8165) and 175 raw `` `{x}` `` interpolations (AST scan). Worst:
`_repair_rows_native` (7182-7227) writes names into a psql script, including
`\copy ... to '{undo}/{ts}-{tbl}.rows'`; a newline in a name starts a new
psql line (`\!` is a shell escape). Measured: `_quote_ident("a`b")` ->
``` `a``b` ``` while the inline form gives `` `a`b` ``. Fix: one quoting helper
per engine used everywhere, and a lint test failing on `f'"{` or ``f"`{``
under `engines/`.

**SEC5-SEC7. The dashboard** (P1, S each). SEC5: with
`MIGKIT_UI_USER_HEADER` set, any request carrying that header is signed in
(`ui.py` `_user`/`_authed` 820-841, `role` 755-776); on a hop with no
`access:` it is operator everywhere; any local process can read the CSRF
value from `/api/data` and hold or resume tails - require a proxy secret
header (`hmac.compare_digest`) or a Unix socket. SEC6: `where = REPORTS /
name / db` is checked only for being inside `REPORTS` (936-939), so
`db="../otherhop/app"` holds another hop's tail with the role of the first -
require `where.resolve().parent == (REPORTS / name).resolve()`. SEC7: `esc`
(104-105) escapes `& < > "` but not `'`, and the value runs as JavaScript in
an `onclick` (160-163) - a database named `x');import('//e/a.js');//` runs
script; build the button with DOM calls and send a CSP with the page's own
script hash.

**SEC8. The requester can approve their own step** - see K9 (P1, S).

**SEC9. PostgreSQL repair undo rows written in plain text despite
`at_rest`** (P1, M). `postgres.py:7222-7226`: psql's `\copy ... to` writes
the file itself, plain, 0644; `EvidencePath.open()` seals text appends only
(`open("w"/"wb")` return a plain handle; evidence.py 130-137); the
encryption test covers SQLite only. Fix: copy to stdout and write through
`EvidencePath.write_bytes`; `open()` seals every mode or refuses; a per-engine
guard test that every file under `undo/` or matching `HOLDS_VALUES` starts
with the age header.

**SEC10. Report directories readable by every local user; secret files
chmodded after writing** (P1, S). `config.py:387-393` plain `mkdir`, no
`umask` anywhere; write-then-chmod at movers.py 4202-4203 (pgloader file with
both passwords), 6596-6603 (the stream bundle), 3963-3964, 3987-3988,
guide.py 708-709. Fix: directories `0o700`; `durable.write_atomic(...,
mode=0o600)` / `O_CREAT|O_EXCL, 0o600` (the pattern `masking._salt` uses).

**SEC11. Secrets in options not resolved** - see K7 (P1, S).

**SEC12. Five redaction mechanisms, each missing something** (P1, M).
`util.without_secret` (by value), `wording.redact` (URI shape, debug log
only), `diagnostics.scrub` (24, 27), `export._SECRET` (77), `notify._clean`
(258), splits in `users.py`, `addresses._shown`, `imports.from_connector`
428. Measured: `redact('redis://:s3cret@cache')` unchanged; `redact` leaves
`password=` in conninfo and JDBC query strings; `scrub('mysql://app:pa/ss@db')`
unchanged and `scrub('postgres://u:p@ss@h')` -> `<removed>@ss@h`;
`notify._clean` leaves a URL-encoded password in text sent to Slack;
`from_connector` turns `mongodb://app:pa/ss@h1:27017/...` into
`mongodb://app:pa?replicaSet=rs0` (host lost, part of the password kept);
`export._plain` keeps `pwd`, `api_token`, `key`, `auth`,
`private_key_passphrase`. Fix: one `Redactor(hop)` holding every resolved
secret with its URL-encoded, shell-quoted and SQL-escaped forms, plus shape
rules (userinfo up to the last `@`, `password=`/`PWD=`, `IDENTIFIED BY '...'`,
`PASSWORD '...'`, `masterauth x`) - used by the debug log, console errors,
notifications, the diagnostics bundle and the export.

**SEC13. The native PostgreSQL subscription duplicates `legs.py`'s builder
without its safeguards** (P1, S). `postgres.py:10263-10293` vs `legs.py
409-423/858-864`: no sslmode, the password raw in a SQL literal (a `'` breaks
it and psycopg2's error quotes the statement, password included), no `set
log_statement = 'none'` (with `log_statement=ddl` the password lands in the
target's server log). Fix: `replicate_sql` uses the `legs` builder.

**SEC14. Downloaded programs and images never checked against a digest**
(P1, M). `tools.py` `IMAGES` 108-112 (`dimitri/pgcopydb:latest`,
`redpandadata/redpanda:latest`), `install_from_source` 522-526,
`install_vendor` 991-996, `VENDOR_EXTRAS` 1036-1041, the redo reader's build
(`curl` into sudo, 1246-1256), the `sudo sh -c curl ... > sources.list`
steps 382-391; 0 checksums in `tools.py`; `urlretrieve` without timeout; the
Dockerfile installs `atlas-...-latest` (the EULA build) with `|| true`,
skipping the terms question. Fix: W6's lock file (`{version, url, sha256,
size}` per program and platform) verified before extracting; images pinned
by `@sha256`; `pip --require-hashes` for the second reader.

**SEC15. The audit chain accepts an edited opening stretch** (P2, S).
`audit.py:87-97`: entries without a hash are allowed before the chain
starts, and the first hashed entry's `prev` is never checked; strip the
hashes of the first k entries, edit them, and `history` reports the chain
whole. Fix: the first hashed entry must have `prev == ""` and commit to a
SHA-256 of the older lines.

**SEC16. `/metrics` needs no sign-in and ignores `access:`** (P2, S).
`ui.py:877-879`, `prometheus()` 272-309 lists every hop and table. Fix:
filter by role; the cookie or a `MIGKIT_METRICS_TOKEN` bearer.

**SEC17. Every tunnel counts as "local" for TLS** (P2, S). `tls.local`
(40-49): a `command:` forwarder (socat, kubectl) or an SSH tunnel whose far
leg is plain turns automatic verification off and passes the posture check;
`cli.py:636` has a second definition. Fix: only an SSH tunnel is local; a
command tunnel is "unknown"; one definition.

**SEC18. The TLS posture covers 6 engine families** (P2, S). `tls._POSTURE`;
`base.py:2560-2562` skips `None`. ClickHouse defaults to plain HTTP with a
basic-auth password (`clickhouse.py:63`); Kafka, OpenSearch, Oracle, Db2
unreported. Fix: every networked engine reports, "unknown" as a warning.

**SEC19. File names built from table names** (P2, S). `base.py:2857-2858`
(`data-{name}.{kind}`), `postgres.py:7225`, the streaming copy's filter file
(movers.py 5611-5612, a newline adds an INI section): `/` or `..` writes
outside the report directory. Fix: one `report_name()` (the
`re.sub(r"[^A-Za-z0-9_.-]", "_")` already at movers.py 2531/5608) plus a
short hash.

**SEC20. What leaves the machine is not what the threat model says** (P2,
S). Failure notifications carry the error text, which can hold PostgreSQL's
`DETAIL: Key (...)=(...)`, unless `mask` is on (notify.py 258-270); a hop's
`code_model.url` sends `MIGKIT_AI_KEY` to any address, `http` included
(assist.py 47-55, storedcode/model.py 46-48); the AI assist is not in the
threat model. Fix: failure events send the error kind unless
`notify.detail`; refuse a non-HTTPS model URL; both documented.

**SEC21. Smaller** (P2, S). `pickle.loads` on the tail's spool
(tailtables.py 151; safe only once SEC10 holds - or JSON/msgpack frames);
`file:` secret references and `approvals.signers` not checked for owner or
world-writable; `_vault_read` needs the vault CLI (K22).

---

## 5. External programs, install, packaging and W8

**Counts (AST, measured).** 62 direct `subprocess.run`/`Popen` calls in 23
files plus two partial helpers - `util.run` (28 sites: retries, optional
timeout, not registered, the whole argv in its error) and `movers._sh` (24
sites: registered, no timeout, no stall check, no process group); 15 of 62
set a timeout; 2 of 20 `Popen` start a new process group (tunnel.py 214,
oracle_redo.py 421); 7 of 20 `Popen` are registered through `_spawned`;
`tool_env` at 23 of 62. Six version-probe paths with four parsers. 15 side
tables name 49 programs, 22 of them absent from `tooluse.REGISTRY` (pg_dump,
psql, mongodump, mongosync, pgloader, riotx, atlas, liquibase, mysqldump,
pt-table-sync, sqlcmd, pg_basebackup, ...).

**W1. SIGTERM skips cleanup almost everywhere** (P0, S). Turned into Ctrl-C
only at `movers.run_via` 7611-7625 (and only with `--go`),
`_follow_until_asked` 6119, `hetero.py` 3830. Elsewhere `kill` ends Python
without `finally` or `_spawned.__exit__`: the table copier's pool
(cli.py 2023), the default catch-up follow (`cli._follow` 2495), the page
copy, the bulk loader and snapshot reader, the index/trigger windows (the
target left with indexes dropped). Fix: SIGTERM -> `KeyboardInterrupt` once
in `cli._Step.invoke`; the per-site copies removed.

**W2. The "still running" guard and the kill miss most programs that
write** (P0, M). `movers._spawned` 460-503, `_still_running` 513-535: it
compares `ps -o comm=` to the recorded name, so Java programs (the bulk
loader, the cluster mirror, the snapshot reader, liquibase) read as `java`
and are dropped; the streaming copy container runs `docker run --rm` with no
`--name` (5238, 5635) - `_spawned` kills the docker client after 15 s and
the container keeps writing, and the next move starts beside it; per-table
copies (`pgcopydb_table`, `mydumper_table`, up to `workers` at once) are
never registered; `pgcopydb_follow` is a bare `Popen` (5983); the page copy
over ssh (`physical._on_target` 1137): a timeout kills the local ssh while
the remote copy keeps writing the target's data directory, and a rerun runs
`find "$D" -delete` under it; only the direct child is signalled. Fix: one
runner - `start_new_session=True`, `killpg(TERM)` then `killpg(KILL)`,
`PR_SET_PDEATHSIG` on Linux, named containers (`--name migkit-<run>-<n>
--init`) removed on exit and before the next start, `psutil` create time
instead of `comm`, every writer registered, remote scripts under `flock` with
a pidfile the next run checks.

**W3. Program names reach the operator at runtime** (P0, M).
`engines/base.py:2414` assess rows `f"{tool} against this target"` (stubbed
run printed `pg_dump against this target`, `pg_restore ...`, `psql ...`, at
warn, into `assess.json`; the unrecognised branch quotes the raw `--version`
text); `generic.py:221-259` names reladiff 4 times; `mssql.py:154`
"(mssql-scripter)"; `postgres.py:7886` "migkit's own pg_dump path";
`util.run:178-181` raises with `' '.join(cmd)` (a traceback with `psql -h
<host> -U <user> ... -c <SQL>`); mongosync's start failure passes raw stderr
(movers.py 4605); second-reader errors pass library text (second_reader.py
378, 421). Fix: scrub at the output points - `Result.__post_init__`
(detail, fix hint), the assess row builder, `session.step`, one top-level
handler (V7) - with one name list generated from the registry (W14); no argv
in `util.run`'s message.

**W4. The leak test cannot see those leaks** (P1, S).
`tests/test_the_report_does_not_name_its_tools.py` 42/157/181: FACING lacks
`add`, `*Error` constructors and plain dict rows (with `add` it finds 5
leaks today); it reads string constants only, so `f"{tool} ..."` and
`' '.join(cmd)` are invisible; TOOLS lacks psql, pt-table-sync,
mssql-scripter, vacuumdb, data_validation/ibis, pg_dumpall. Fix: TOOLS from
`export._names()` (the registry); runtime tests (assess with stubbed tools; a
`util.run` failure through `_Step`).

**W5. Database passwords can be baked into the container image** (P0, S).
The build context is the repository root with no `.dockerignore`;
`deploy/README.md:15` tells the operator to fill `deploy/conf/hops.yaml`, so
`COPY . /app` (`deploy/Dockerfile:7`) layers it in (with `.venv`, reports,
`.git`); only `conf/hops.yaml` was ignored. **`deploy/conf/` added to
`.gitignore` in this audit.** Left: a `.dockerignore` (`conf/`,
`deploy/conf/`, `.venv*`, `reports/`, `.git`).

**W6. Installs are not pinned or verified** (P1, M) - SEC14, plus six
measured programs (pg_dump, pg_restore, mydumper, myloader, mongodump,
mongorestore) from unpinned brew/apt. Fix: the lock file, read by
`tools.py`, the Dockerfile and the Homebrew formula alike.

**W7. W8 (offline signed bundle, container image, Windows) is not started**
(P1, L). Download URLs hard-coded (`VENDOR_URLS`, `SOURCE_BUILDS`,
`REDO_READER_BUILD`, `IBMI_*`) with no bundle override; images pulled at
first use (`stream_up`, `_image_options`); the second reader from PyPI; the
Dockerfile never built in CI; Windows blocked by `util.tool_env:73` (PATH
joined with `:`), `_still_running` (`ps -p`), `cutover.py:259` (`/bin/sh`),
`tools.py:1307` (`bash -c`), `PrivateFile` (`os.fchmod`). Fix: a bundle
builder that fetches the lock file's entries into a directory with
`SHA256SUMS`, signed through the existing `ssh-keygen -Y sign` path
(export.py 881); `MIGKIT_BUNDLE=<dir>` read by `install_vendor`,
`install_from_source`, the second reader (`pip --no-index --find-links`) and
`docker load`; a CI job that installs inside a network namespace with no way
out. (A CLI subcommand would be a new mode: the builder is `doctor
--install` with `MIGKIT_BUNDLE_BUILD=<dir>`, or a `tools/` script.)

**W8. `doctor --install` never replaces a wrong build** (P1, M).
`tools.install_missing` 779/795 skip anything on PATH; three messages
(movers.py 7550, 7603, 2211) promise "puts a build that does in place"; a
distro mydumper older than CI's 0.21-1.0.8 range fails `tool_flag` forever.
Fix: compare the installed version with the lock file's range, install the
pinned build under `~/.migkit/tools/<p>-<v>/bin` first in `TOOL_PATHS`.

**W9. "Switch to the next way" vs "stop" decided ad hoc per wrapper** (P1,
M). None of movers, redisshake, mirror, dsbulk, fromsnapshot or ddbexport
raises `decide.Blocked`; `RuntimeError` (switch) vs `SystemExit` (stop) by
habit - `mirror._wait` switches when the program exits and stops on a stall
(464 vs 481); a failed streaming-copy `ping` silently runs the dump inside
the same rung (5290-5299); a wrong password, permission denied or disk full
still count as "try the next way"; a negative return code (killed, OOM) gets
no message of its own. Fix: `runner.classify(rc, text)` -> `Refused` (stop
every way: 28P01, 42501, 53100, read-only target), `Blocked` (this way cannot
run), `Transient` (retry this way), using the SQLSTATE parsing in
`wording.database_words`.

**W10. The dump fallback's time is recorded as the streaming copy's rate**
(P1, S). `cli._record_run` 1737-1738, `movers.pgcopydb_move` 5252/5299: the
elapsed time is kept under `via="pgcopydb"` - the decision engine's costs
corrupted; `here = movers.PROGRAMS.get(via) != ("docker",)` uses a static
table, so a local run loses its CPU seconds. Fix: return and record the way
that ran; the fallback as a rung or a `Blocked`; `here =
movers.programs(via) != ("docker",)`.

**W11. `util.run` re-runs whole scripts that write** (P1, S). 159-184;
`mysql.py` 5190-5197, 5351: schema/grants/constraint repair sends a
multi-statement DDL script through the client with `retries=3` on a stderr
substring ("lost connection"); MySQL DDL commits as it goes, so a retry
re-applies the first half and fails on "Duplicate column", hiding the cause.
Fix: `retries=0` for anything that writes; retry only failures while
connecting.

**W12. No timeout or stall detection in the main runner** (P1, M). 27 `run`
calls and 18 of 20 `Popen` have no timeout; `stall.Guard` is used by 3 pipe
copies; the mirror has its own `STALL=300`; mongosync none; `pg_dump` has no
`--lock-wait-timeout`. Fix: the runner takes a stall limit fed by the
progress readers (`MIGKIT_STALL_SECONDS`) and a budget from the model.

**W13. Version probes repeated, parsed four ways** (P1, S-M).
`movers.program_version` 7466 and `base._client_tool_versions` 2393 are not
cached (`assess` on a PostgreSQL hop starts `pg_dump --version` and
`pg_restore --version` twice each); parsers `(\d+\.\d+(?:\.\d+)?)`,
`\(PostgreSQL\)\s+(\d+)`, `(\d+)\.\d+\.\d+`, a substring;
`_PGCOPYDB_HOW` (4758-4775) cached for the process life, so a long `sync
--serve` never sees an image pulled later. Fix: one probe cached on
(realpath, mtime_ns), its arguments and pattern in the registry.

**W14. The registry is not the one source of tool facts** (P1, L). 15 side
tables (`tools.PROGRAMS/PROGRAM_LICENCES/IMAGES/VENDOR/SOURCE_BUILDS/
NEEDS_JAVA/TERMS`, `movers.PROGRAMS/MEASURED/LOADERS/COPIERS/DRIVEN`,
`physical.PROGRAMS`, `toolversion.PATTERNS`, `storedcode.READS`); install
recipes repeated in the Dockerfile, the formula and CI; the code scan that
marks options "used" takes `"row"`/`"column"` as proof for `validate row`
and `validate column`. Fix: `tooluse.Tool` holds install recipe, lock entry,
version probe, role word, image digest and terms; the side tables, the
test's TOOLS, the Dockerfile and the formula generated from it.

**W15. One process per query where a driver is connected** (P1, M). 250
`self._psql(` sites in `postgres.py` (94 more elsewhere), 36 `sqlcmd` `_cmd`
sites in `mssql.py` (`check_data` starts two per table, serially); psycopg
and the SQL Server drivers are hard dependencies. Fix: `_psql`/`_cmd` go
through the engine's kept connection; the client program only for `\copy`
or meta-commands (SP2).

**W16. Wrappers that restart from zero or leave paid work behind** (P1, M).
`fromsnapshot.copy_index` 225-228 deletes `.migrations_working_state` in
`finally`, contradicting its docstring (a rerun "takes the pieces not
done"), and runs the reader one process at a time for up to 10,000 runs;
`ddbexport.export` 141-151 sends no `ClientToken` and keeps no ExportArn - a
Ctrl-C during the wait leaves a paid export and a full S3 copy, and a rerun
exports again; `mongosync_move` 536-540 drops the target collections and
restarts; `_LocalCopy.__enter__` deletes the dump at the start of every run.
Fix: keep working state on failure and run N reader processes; ExportArn in
the table's position file, `ClientToken` = hash(hop, db, table, run),
`try/finally forget`; dumps kept with a manifest (LSN or GTID, table list),
unfinished TOC entries reloaded (`pg_restore -L`).

**W17-W20** (P1/P2, S). W17: every `--go` re-runs the mover as a dry run to
collect its argv for `options_missing` (7489) - check the flags on the exact
argv about to start, raise `Blocked` before spawning. W18: human text parsed
where machine output exists - `_follow_sentinel` tests `"apply" in stdout`
while `follow_lag` uses `--json`; the bulk loader's counts from the console
table where `--monitoring.csv` exists; `fromsnapshot.flags_here:86` greps
the help text; MySQL Shell's binlog position regexed where the script could
print it in its JSON line. W19: secret files five ways with a race -
`_pgpass` uses the fixed name `streaming-table.pgpass` (5617) while tables
copy in parallel, so one table's `finally` unlinks it under another's
connections; pgloader's file written then chmodded (4199-4201) and left on
SIGKILL; `_MongoSecrets` in the report directory; container passwords in
`docker inspect`-able env - everything through `PrivateFile` (O_EXCL, unique,
tmpfs) and `--env-file`. W20: output unread - mongosync's stderr is a pipe
never read during an hours-long sync (a full 64 KB pipe blocks it);
`mongodump | mongorestore` reads the dump's stderr only at the end; `_sh`
buffers all output in memory; `dsbulk._run` ignores `log`; the page copy
runs without `--progress`; `second_reader.run` discards stderr and a stray
`print` corrupts its JSON reply; the RedisShake account keeps `log_file` "for
the words of a failure" but the work directory is deleted first (530 vs 645).

**W21. Every tagged release fails** (P1, S). `.github/workflows/release.yml:77`
reads `tomllib ...['project']['version']`, but `pyproject.toml` has `dynamic
= ["version"]` (measured `KeyError: 'version'`). Fix: read
`migkit/__init__.py`'s `__version__`.

**W22. `install_vendor` calls home and can hang** (P1, S). `tools.py:1068`
runs `atlas version` without `tool_env()` (util.py 34-45 measured that it
opens `vercheck.ariga.io`) and without a timeout. Fix: `env=tool_env(),
timeout=120`.

**W23-W24** (P2, S). Installing one package per `brew`/`apt-get` call with
no `apt-get update`, vendor fetches serial, replacement by `rmtree` then
`copytree` (a failed copy destroys a working install), `copyfile` into a
root-owned scripts directory raises a raw `PermissionError`;
`tarfile.extractall(filter="data")` needs Python 3.10.12+ while
`requires-python = ">=3.10"`; the Homebrew formula declared MIT (**fixed in
this audit**: AGPL-3.0-or-later).

---

## 6. Schema compare, convert and apply; drift; users and grants

**Short answers.** Type maps: `canon.TYPES`+`canon.DDL` for tables,
`storedcode/pg_mysql._TYPES`/`my_type` and `storedcode/mysql_pg._INT`/`pg_type`
for routines, `storedcode/sizing.UNBOUNDED`/`TEXT_MOST` a third set - and
they disagree (measured):

| Source type | Table path builds | Routine path builds |
|---|---|---|
| PG `integer` -> MySQL | `bigint` | `INT` |
| PG `text` -> MySQL | `varchar(1024)` | `LONGTEXT` in `utf8mb4_0900_bin` |
| PG bare `numeric` -> MySQL | `decimal(65,10)` | `DECIMAL(65,30)` |
| PG `timestamptz` -> MySQL | `datetime(6)` | refused |
| MySQL `int`/`smallint`/`tinyint`/`year` -> PG | `bigint` | `integer`/`smallint` |
| MySQL `bigint unsigned` -> PG | `bigint` | `numeric(20)` |
| MySQL `datetime` -> PG | `timestamp(6)` | `timestamp(0)` |

Schema readers: PostgreSQL same-engine five full-catalogue comparers per
database; MySQL three; SQL Server names plus definition hashes; Oracle, Db2,
ASE, ClickHouse, Cassandra, DuckDB and the warehouses the cross-engine check
(by class); `generic.check_schema` (declared type, length, nullability - the
smartest and used by nothing else); `drift` (column, type string); the
cutover gate (tables only across engines, nothing when any column is
mapped). DDL is applied statement by statement everywhere, never in one
transaction. Users: MySQL, PostgreSQL, SQL Server, Oracle, Redis, Cassandra
carry password hashes; MongoDB generates new ones; Kafka and OpenSearch take
a passwords file; cross-engine none; no `create` compares again after
applying.

**SC1. The pair's schema check compares by class and misses narrowing** (P0,
M). `hetero.py:check_schema` 232-297, `_schema_result` 353-395,
`base.py:_classify_columns` 2989 - the whole schema check for Oracle, Db2,
ASE, ClickHouse, DuckDB, the warehouses, Cassandra and mapped tables of
PostgreSQL/MySQL hops. Measured: MySQL->PG `varchar(255)`->`varchar(10)`,
`bigint unsigned`->`integer`, `timestamp`->`timestamptz`,
`tinyint(1)`->`smallint` -> `ok 5 columns, same names and the same kind of
value on both sides`; PG->PG `varchar(255)`->`varchar(10)`,
`numeric(12,4)`->`numeric(12,2)`, `timestamptz`->`timestamp` -> `ok`.
`_rules_drift` works only where `neutral_column_rules`/`neutral_indexes`
exist (MySQL, PostgreSQL, SQLite); `verdict.narrowing` and
`generic._lengths_and_nulls` compute this already. Fix: one comparer over a
neutral shape - class, capacity (chars, integer range, numeric p/s,
timestamp precision, zone) through `canon.capacity`/`narrower` extended
across engines, nullability, default (textual on same engine), unique and
plain indexes, foreign keys, collation - called by every engine's
`check_schema` and the cutover gate.

**SC2. SQL Server's `check_schema` never compares columns** (P0, S-M).
`mssql.py:_objects` 70-111, `check_schema` 133-156:
`hashbytes(isnull(object_definition(o.object_id),''))` - NULL for `U`, `PK`,
`F`, `UQ`, so every table, key and foreign key hashes as `''`; indexes
compare as `{name: (type_desc, is_unique)}` without key columns. A dropped,
narrowed or nullability-changed column, a collation change, an index over
other columns all give `ok N objects`. Fix: one query over `sys.columns`
(type, `max_length`, precision, scale, `is_nullable`, `collation_name`,
identity, default) plus `sys.index_columns`, `sys.foreign_key_columns`, into
SC1's comparer.

**SC3. Drift is (name, type string), and resume, the cutover gate and the
tail gate lean on it** (P0, S-M). `drift.changes` 135-152;
`postgres.column_catalog` 530-544 uses `information_schema.data_type`;
`cli.py:1095-1112`; `cutover.py:_shape_rows` 522-543. Invisible: PG
`varchar(50)`->`(100)`, `numeric(10,2)`->`(12,4)`, `int[]`->`text[]` (both
`ARRAY`), an enum swap (`USER-DEFINED`); NOT NULL, default, index,
constraint, collation changes on every engine - so `check` resumes "was ok"
after `ALTER ... ADD UNIQUE`/`SET NOT NULL`/`CONVERT TO CHARACTER SET`;
`_shape_rows` returns `[]` when `eng.maps_columns()` (one column rule removes
the cutover schema gate), tables only across engines; engines without
`column_catalog` (SQL Server, Db2, ASE, ClickHouse, Snowflake, BigQuery,
Redshift, DuckDB, Cassandra) no drift at all. Fix: `shape_catalog(side, db)`
- one query per side, a per-table fingerprint (PG:
`md5(string_agg(format_type, attnotnull, pg_get_expr(adbin), attcollation))`
plus index, constraint, trigger definitions); drift compares fingerprints and
explains the tables that differ; implemented per engine. (The cutover gate is
the cutover work's file.)

**SC4. PostgreSQL's "unchanged" marker ignores the table's shape, and is
N+1** (P0, S). `postgres.py:_change_marker` 6474-6519 (caller `markers()`
3696): `n_tup_ins/upd/del | relfilenode | pg_relation_size`; `DROP COLUMN`
or `RENAME COLUMN` on the target rewrites nothing, so `check data` says
"UNCHANGED since it was last proved equal (no rows read this run)" over a
table that lost a column's data; per table per side `show server_version`
plus the statistics query - four psql processes a table to decide to skip.
Fix: `relnatts` and an md5 over live `pg_attribute` rows in the marker, all
markers in one query per side, the version read once.

**SC5. The cross-engine build reads raw types while the compare reads
canonical ones** (P0, S). `hetero._target_shape` 2026-2039 uses raw
`neutral_columns`; `check_schema` 280-282, `_fit_plan` 764-766,
`_comparable_columns` use `canonical_type`. Measured: `type_class('postgres',
'mood')`, `'email_address'` (a domain) and `'citext'` are `None`, so a PG
table with an enum, domain or citext column cannot be built cross-engine
while `check` compares the column as text; `sizing.UNBOUNDED['postgres']`
lists `citext`, `canon.TYPES` does not; `assess` stops at the first such table
(`_unfit_result` 1023-1030). Fix: one `declared(engine, side, db, table)`
applying `canonical_type`, used by build, fit and compare; `citext: text`;
`assess` names every table it cannot convert.

**SC6. Table builders drop `key`, so the target validates one type and
builds another** (P0, S). `postgres.py:1230`, `sqlite.py:409`,
`cassandra.py:215`, `clickhouse.py:335`, `lake.py:241`, `warehouse.py:1353`
call `canon.ddl_type(eng, cls, nums)` without `key`; `mysql.py:564`,
`dbapi.py:381`, `hetero.py:2085` (validation) and 788 (fit) pass it. Measured:
`ddl_type('postgres', BSON, ('document',), True)` -> `jsonb`, without key ->
`json`; a MongoDB `_id` of mixed types is validated as `jsonb` and built as
`json` with `primary key("_id")`, which PostgreSQL rejects (json has no btree
class). Fix: `col[0] in key` at every builder (one-liners; five of the files
are P0-PARITY's).

**SC7. The cross-engine schema drops foreign keys, CHECKs and indexes;
`convert-schema --apply` loses indexes for good** (P0, M).
`hetero.converted_objects` 2858-2867 emits `CREATE TABLE` (primary key only)
plus routines and views; tables made by `convert-schema --apply` are no
longer missing, so `move` writes no `built-tables.json` and
`finish_created` never adds their indexes; `foreign_keys()` exists on MySQL,
PG, SQL Server, Oracle (base.py 4267) and is used for nothing schema; plain
indexes, FKs and CHECKs never compared. Fix: `converted_objects` emits a
post-data section from `neutral_indexes`, `foreign_keys` and the check
catalogue; `--apply` records `built-tables.json` as `move` does; SC1 compares
all three.

**SC8. `finish_created` is not idempotent, serial, with one name limit**
(P1, S). `hetero._build_index` 2192-2208: `for attempt in (name,
f"{leaf}_{name}"[:63])` - a run that died after some indexes were built
reruns into "already exists" and builds a second index over the same columns
under the other name; one build at a time (MySQL's `_MyIndexWindow` builds
side by side); `[:63]` is PostgreSQL's limit, applied to every target. Fix:
read the target's indexes once and skip any with the same (unique, columns),
`if not exists`, builds in parallel under the move's sizing, the limit from
an engine probe (SC18).

**SC9. PostgreSQL and MySQL user create: wrong order, partial state lost,
compare by names only** (P0, M). `users.compare` (PG: `skeys, tkeys, pw =
set(s), set(t), []`, line 139), `_plan` 1756-1818, `create` 1943-1956: PG
role attributes and `member_of` read and never compared; roles created
alphabetically, so `GRANT "readers" TO "app"` fails when `readers` comes
later, the exception skips the rest of `app`'s statements and `app` is not
recorded (cannot be rolled back); a rerun skips `app` and `verify` passes;
MySQL 8 roles (empty `authentication_string`) are skipped (1764-1766), so
every `GRANT role TO user` fails with ERROR 3523 and that user's other grants
never run. ClickHouse and Oracle already create roles first. Fix: roles
first in dependency order, then users, then grants; a per-statement ledger;
compare attributes, memberships, `role_edges`, `default_roles` (`SHOW CREATE
USER` for MySQL); `create` ends with `compare()` and fails on a gap.

**SC10. MySQL grants: holes and N+1** (P0 coverage / P1 speed, M).
`mysql.py:_grants_for_db` 4820-4883, `_check_grants` 4761, `_grant_repair`
5023: the pattern `\sON\s+`?db`?\s*\.` sees db-scoped grants only - global
(`*.*`), role grants, default roles, wildcard databases (`app\_%`.*) and
routine grants invisible; `users compare` for MySQL ignores grants;
`show grants for %s@%s` per user, per database, per side, each on a new
connection, and again in the repair. Fix: `mysql.user`, `mysql.db`,
`tables_priv`, `columns_priv`, `procs_priv`, `role_edges`, `default_roles`
read once per side (7 queries), cached per run.

**SC11. PostgreSQL grants: table and sequence ACLs only; tables migkit
creates carry none** (P0, M). `postgres._grant_gaps` 6577-6634; restore with
`--no-privileges` (movers.py 1454-1455). Not compared: schema
USAGE/CREATE (`nspacl`), database CONNECT/TEMP (`datacl`), function EXECUTE
(`proacl`), column ACLs (`attacl`), `pg_default_acl` (new tables after
cutover get no grants), `is_grantable`, PUBLIC. Fix: one `aclexplode` union
per side over all of them with `is_grantable`; the grant repair runs after a
create, or privileges restored when the roles exist.

**SC12. SQL Server permissions over-granted, and verify cannot see it** (P0
security, M). `users._ms_principals` 595-616, `_ms_create` 638, compare
168-174: `minor_id` never read, so a column grant becomes a table grant and
`CONTROL ON USER::x` / `ON CERTIFICATE::c` becomes `GRANT CONTROL TO [u]`
(database-wide); `GRANT_WITH_GRANT_OPTION` becomes a plain GRANT; compare
uses the same lossy strings; SIDs never compared (orphaned users unseen);
`CHECK_POLICY = OFF` forced; server-role gaps on existing logins never
repaired. Fix: render by `class_desc` with `minor_id` and the `USER::`,
`ROLE::`, `CERTIFICATE::` forms, keep WITH GRANT OPTION, compare SIDs, carry
`is_policy_checked`/`is_expiration_checked`.

**SC13. Routine types and collation disagree with the table path** (P1, M).
The table above; routines declare every string `utf8mb4_0900_bin` while
tables are built in the target's default (usually `_0900_ai_ci`), so a
routine comparing a parameter to a column compares in `_bin` and cannot use
the column's index; MariaDB and MySQL 5.7 lack `utf8mb4_0900_bin`. Fix:
storedcode declarations call `canon.ddl_type`; `canon` gains exact-width
integers per pair (`INT4_DDL` exists for MongoDB); one collation per column
from the source's semantics, asked of `information_schema.collations`.

**SC14. MariaDB defaults misread when building the target** (P0, S; from
MariaDB's documented behaviour, not run). `mysql.neutral_column_rules`
503-525 (used by `hetero._carried_rules` 2210) assumes MySQL 8's
`information_schema`; MariaDB 10.2.7+ returns literals already quoted, the
string `'NULL'` for a nullable column with no default, and has no
`DEFAULT_GENERATED` - a MariaDB->PG build gets `default 'NULL'`,
`default '''new'''`, and `curdate()` as a string, all accepted by
`default_works`. Fix: branch on `_brands()`.

**SC15. Catalogue reads are N+1 with no shape cache** (P1, M).
`hetero.check_schema` 6 round trips a table (columns x2, rules x2, indexes
x2), `check_data` 2 more; ~25 `neutral_columns` call sites; each MySQL `_q`
a new connection (1903-1919), each PG `_psql` a process and a connection
(299-310); `column_catalog` (one query for all columns) exists for MySQL, PG,
Oracle, SQLite and only drift uses it. For 2,000 tables `check` makes about
16,000 connections or spawns (estimate). Fix: `Engine.catalog(side, db)` -
one bulk read per side of columns (declared type, null, default, collation),
keys, indexes, foreign keys - cached per run, cleared by `forget_shapes()`;
every `neutral_*` reader takes from it.

**SC16. Same-engine schema checks: too many comparers, silent drops, and a
demotion that hides MySQL routine drift** (P1; P0 for the demotion, M).
PostgreSQL reads each side's catalogue about five times (dump, results,
INVENTORY, liquibase, atlas); MySQL dumps twice and runs atlas four times
(fix and revert); optional comparers vanish on timeout (`check_liquibase`
`timeout=180` returns None; `mysql._atlas` 2160-2164). When atlas says
"synced" the dump's text diff is demoted (`base._atlas_authoritative`
2146-2172), but the open build inspects no triggers, routines or views and
MySQL's object check compares names only - routine, trigger and view body
drift reads ok. Fix: one authoritative structural comparer per engine, the
others optional and shown "not run: why"; demote only the object kinds the
authority inspected.

**SC17. TLS and credentials differ between comparers of one check** (P1,
S). `mysql._dump_schema` 1997 runs without `self._client_tls(side)` (6869):
`REQUIRE X509` accounts fail and the CA is unverified; `mysql._schema_urls`
2139-2146 no TLS (atlas fails under `require_secure_transport` and silently
returns None); `postgres.check_structural_diff` 2826-2828 builds
`f"postgresql://{s.user}:{s.password}@..."` unquoted (an `@`, `:` or `/`
breaks it) without `_tls_query`, which `_schema_urls` 2923 has. Fix: one
`Endpoint.url(scheme, db, tls=True)` (DU8).

**SC18. Fixed values where the server could be asked** (P1, M).
`canon.KEY_TEXT["mysql"]="varchar(768)"` (1875) assumes utf8mb4 and DYNAMIC
rows (COMPACT or 5.7 without large prefix: 191; latin1: 3072);
`DDL["oracle"]["text"]="VARCHAR2(4000 CHAR)"` (1570) ignores
`max_string_size`, which `oracle.py:767-781` already reads; MySQL text built
`varchar(1024)` (1513) while `sizing.py:7-8` says "the target's widest text";
`[:63]`; three PostgreSQL role-exclusion lists (`users._pg_sysrole` 38-41,
`_login_roles` 7412-7422, `_grant_gaps` 6590-6600) so `assess`, `users` and
grants disagree about one role; `mysql_native_password` assumed on the target
(off by default in 8.4, gone in 9.0); storedcode's reserved words from
sqlglot, not the server's `information_schema.keywords`. Fix: an engine
`limits(side)` probe (identifier length, key bytes, row format, charset,
`max_string_size`, active auth plugins, keywords) that `ddl_type` and `users`
consume.

**SC19. Partition maintenance in the binlog read as a harmless "alter"**
(P0, S). `logddl.mysql` (`_RULES` 29-40), `tailtables.ddl` 923-976 ("alter"
returns None), `base.DDL_STOPS = ("truncate",)`. Measured: `ALTER TABLE t
TRUNCATE PARTITION p0`, `DROP PARTITION`, `EXCHANGE PARTITION p0 WITH TABLE
t2` each classify `[('db','t','alter',None)]` - rows removed or moved with no
row events, and the target keeps them; `ALTER TABLE t ADD COLUMN x INT,
RENAME TO t2` misses the rename. Fix: partition-level
`truncate|drop|exchange|reorganize|coalesce|remove` classified
truncate-like (copy again or stop), EXCHANGE for both tables, comma lists
split into rename plus alter.

**SC20. `ddl.needs_backfill` misses multi-clause ALTERs** (P1, S).
`ddl._ADD_COLUMN` 44-48. Measured: `ALTER TABLE t ADD COLUMN a int NOT NULL
DEFAULT 0, ADD COLUMN note text NOT NULL;` -> `[]` - atlas's usual MySQL
shape and the silent-fill case the module exists for. Fix: split at
top-level commas (`logddl._split_commas`) and test each clause.

**SC21. DDL applied without a transaction, post-data serial, N+1 creates**
(P1, S). `cli._convert_schema` 856-868 one connection per statement (a
PostgreSQL target could take `psql -1`); `movers._pg_schema` 1452-1455
restores post-data serially while the plan shown (`postgres.py:7370-7372`)
says `-j workers` - what runs is not what is shown; `_my_create_missing` a
`show create table` per table on a new connection (movers.py 2320). Fix:
`-j` on post-data, the PostgreSQL convert in one transaction, one MySQL
connection.

**SC22. Users: wording, hints, rollback and re-reads** (P1/P2, S).
`users.compare` 190-313: six phrasings and four JSON keys
(`roles_differ`, `rules_differ`, `grants_differ`, `attributes_differ`) for
one kind of gap; `mysql.py:4808` tells the operator "users create replays
their GRANTs" for users on both sides, which `create` never touches (PG
says `sync --kind schema --apply` for the same case); PostgreSQL rollback's
fallback `_pg_conn({**hop.target, "database": db})` (2083) raises
`TypeError` (`Endpoint` is a dataclass), is swallowed, and the REVOKEs never
run; Oracle and Db2 for i re-read the target after `compare`. Also: PG user
passwords compared as `pass` when the target hides `pg_authid`
(`_pg_hashes` returns `{}` on any error, 91-104, 290-296 - the usual
self-run -> RDS direction prints "password differs (0): []" and `pass`: P0,
S). Fix: one result shape (`missing / attributes_differ / grants_missing /
grants_extra / password_differs`), hints corrected, `_pg_conn(dbname=)`,
`compare` returns both sides, `None` when hashes cannot be read ("passwords
not compared").

---

## 7. Speed

Nothing here repeats R20 or `fast-python-hot-paths-2026-09-28.md`; where a
fix R20 names was not applied everywhere it says so. `import migkit.cli` is
fine: 60 ms with cached bytecode, 139 ms without (rich 18 ms, yaml 11 ms,
click 10 ms); no pandas, pyarrow, boto3, google, snowflake, z3, hypothesis or
sqlglot at module top.

**SP1. `drift.transient_among` is quadratic in tables and runs per tail
batch** (P0, S). `drift.py:54-92`: `others = {str(t) for t in among}` (71)
rebuilt per table; called by `PostgresEngine.neutral_tables` (526),
`MySQLEngine._tables` (2223), `drift.shape` (129, per tail batch through
`hetero._shape_now`); `neutral_tables` runs 3+ times per table moved
(cli.py 1854, hetero.py 1470-1472), so a whole move is cubic. Measured: 5.1
ms at 500 tables, 73.4 ms at 2,000, 587.6 ms at 5,000 (1.8 s CPU per table
moved and 0.6 s per tail batch at 5,000); the set built once: 5.0 ms at
5,003 tables, same answer.

**SP2. Catalogue lookups per table or per batch, uncached, each on a new
connection or process** (P0, M). `_psql` (postgres.py 299-310) behind 251
call sites (`neutral_tables`, `neutral_columns` 546, `neutral_key` 567,
`neutral_digest`, `fence_wait`); MySQL `_q` (1903-1919) connect-close per
query; per batch: `_read_query` asks the key on every `neutral_read`
(postgres 975, mysql 389), `neutral_write` asks it per batch (1085, 451),
`neutral_read/write/rows_by_key` open a connection each, `_batch_digest`
spawns two more psql; `_read_hstore` (731-746) registers hstore on every new
connection (only a negative answer cached). Measured: a process start alone
6.5 ms median (`util.run(["true"])`); each new connection adds TCP, TLS and
auth, 4-6 round trips - about 0.3 s a catalogue call over 50 ms (estimate).
Fix: SC15's per-run catalogue; one kept connection per side and thread; a
psycopg connection instead of `psql` for catalogue and fence queries (W15).

**SP3. The tail's "the copy already read it" check costs up to 87 us a
change, forever** (P0, S). `hetero._CopiedRanges.leave_out`/`_read`
(4342-4418) -> `ranges.place` (346-378) rebuilds the set of finished ranges
per change (24 us at 2,000 ranges, 83 us at 20,000); `mark_covers` re-parses
the snapshot mark per change (PG 37.4 us with 200 in-progress xids,
postgres.py 2514; MySQL 3.2 us with 6 server UUIDs, mysql.py 1737);
`move.json` is kept after the move, so every change to a copied table pays
it for the tail's life. Measured end to end: 86.8 us a change at 2,000
ranges, 4.2 us at 16 ranges with an empty mark - against the MySQL tail's
whole 15.3 us a change. Fix: precompute each entry's finished set and parsed
marks at `_load`; retire an entry once the tail's saved position is past
every range's mark.

**SP4. `_Checkpoint.save` forces the slow JSON encoder on every batch**
(P1, S) - ST17 (46.4 ms vs 7.9 ms on 4.3 MB; per batch at hetero.py 1639).

**SP5. The batch read-back re-renders every value on both sides unless the
key is one integer** (P1, M). `_batch_digest` gives up unless the key is one
integer column (hetero.py 1384); `_write_checked` -> `_verify_batch` ->
`_rows_differ` (1329) uses the per-value `canon.render_value` (also
`leaves.confirm`, `base._row_text`, `_key_of`). Measured (5,000 rows, 10
columns, uuid key): `_rows_differ` 12.5 us a row, 9.4 with a renderer per
column; `_by_key_map` 1.19 vs 0.58 us. Fix: the target's digest over the
batch's key set (`where key in (...)`), the batch folded in-process (~2 us a
row), row by row only on a mismatch; `render.renderer` in those helpers.

**SP6. The per-column renderer falls back to the slow path for common
types** (P1, S). `render.py:29-71`. Measured: UTC-aware timestamps (every
`timestamptz` from psycopg) 1.68 -> 0.75 us with a zero-offset branch; float
(`_float_text` imports and builds a context per call) 1.37 -> 0.50 us with
hoisted imports and a module-level context; uuid 0.40 -> 0.17 us with a
precompiled pattern; same answers on the values tried.

**SP7. Keeping the proof as the tail moves** (P1, M). `kept.Table.hashed`
(242-263) 8.7 us per 10-column image (1.9 us from SP6's slow paths; an
import inside the function; `_typed` on every value, 0.355 us even for
text; two `any()` scans); `Table.bucket` fallback 1.85 us (a renderer per
value, 0.47 us each, a dict per call); `Keeper._move` (427) one SQLite lookup
per new leaf, then `_commit` reads them again; tables copied or parked beside
the tail rewrite every leaf every batch (455-459) - JSON alone 2.6 us a leaf,
>= 173 ms a batch at 65,536 leaves. Fix: per-table column sets and
converters precomputed; batched leaf reads; one table-level "stale since
generation N" flag instead of rewriting leaves.

**SP8. Every tail batch re-reads the source's whole schema** (P1, S-M).
`hetero._shape_now` 3493-3531 (per batch at 3925): the whole column
catalogue (PG: a psql process against `information_schema.columns`), SP1's
quadratic check, then load/compare/rewrite `tail-shape.json`. Measured at
2,000 tables x 20 columns: 8.1 ms to parse and 23.5 ms to compare and
rewrite a batch, before the query. MySQL's binlog carries DDL, so the poll is
redundant there. Fix: one fingerprint row in SQL (SC3), the rest only when it
changes; no poll where the change stream carries DDL.

**SP9. The cross-engine data check reads both sides one after the other,
tables one at a time** (P1, S). `hetero._digests` 624-625;
`_neutral_rows` 455; the same-engine PG check does both sides at once
(postgres.py 3612) and tables over `hop.workers` (3739); R20.12's "both
sides at once" reached `pg_pass` and `_batch_digest`, not here. Fix: two
threads in `_digests`; a pool over tables under the existing throttle.

**SP10. Fixed sleeps on the fence and cutover path** (P1, S).
`hetero.fence_wait` checks the tail's position file once a second (2628);
the idle cross-engine tail sleeps a flat 1 s (4013) - every fence (cutover,
confirm pass, `kept.prove`, generations) waits ~1 s extra on average, up to
~2 s plus a batch; PG `fence_wait` polls every 0.25 s with a psql process
and connection each (8729). Fix: idle back-off from 20 ms to 1 s reset on
changes; the fence checks every 50 ms; PG fence on one kept connection.

**SP11. The tail opens a target connection for every applied batch** (P1,
S). `base._apply_session` 4848-4889 opens `_open_writer` (postgres 1576,
mysql 907) per batch and lane; a caught-up tail sending small batches pays
4-6 round trips each - lag at cutover. Fix: one writer per lane thread,
reconnect on error.

**SP12. The row filter's translation is recomputed on every call** (P1,
S). `hetero._row_scope` 2238-2278 (a column lookup plus a sqlglot parse and
translate, measured 113 us) per call; the tail's `_in_scope` (3303-3345)
calls it per filtered table per batch and asks the columns again. Fix: cache
per (db, table).

**SP13. Loading one hop builds every hop and resolves every secret** (P1,
S) - K12.

**SP14. The "plain columns need no conversion" fix was not applied
everywhere** (P1, S). `canon.sql_rows` (2043) still calls `write_value` on
every text or bytes value (0.875 and 1.05 us each) - measured 4.45 us a row
for 8 columns (4 text/bytes) vs 0.51 us letting `str`/`bytes` through (8.7x);
`MySQLEngine.neutral_write` (462) and `dbapi.py:275-283` call it on every
value, numbers included. Fix: a writer chosen once per column (identity for
plain classes and for `str`/`bytes` into SQL engines).

**SP15. Avro change encoding costs 53 us a change** (P1, S).
`avrostream.Encoder.encode` 92-148 rebuilds the envelope schema per change,
serialises it twice with sorted keys for the registry's cache key (9.1 us),
parses both schemas again (5.4 us); `_name`'s regex runs per field because
the `setdefault` argument is evaluated eagerly; decode 11.2 us vs 5.6 us with
the parsed schema cached per id. A Kafka Avro sink tops out around 19,000
changes/s (estimate from the measured cost). Fix: schema ids and parsed
schemas cached per topic and field-type signature.

**SP16. The deep capacity check scans a table once per narrowed column**
(P2, S). `base._capacity_gaps` 885-935. Fix: one scan per table with a
conditional count and max per column.

**SP17. The capability check imports every engine** (P2, S).
`capabilities.require(...)` -> `_has` -> `engines_with` imports all 28
engines and reads method sources with `inspect`/`ast`: 50-100 ms a command
with cached bytecode, 0.67 s with `PYTHONDONTWRITEBYTECODE=1` (measured).
Fix: probe the named engine's class only.

**SP18. Small per-row costs** (P2, S). Measured: `Engine._key_of` builds two
dicts a row (1.19 -> 0.58 us); `leaves._keep` `dict(s.cols)` and the slow
renderer (0.64 -> 0.125 us); `rowtext.encode` 0.605 -> 0.452 us with a list
comprehension; `canon.digest_step` 0.524 -> 0.432 us (digest bytes shifted,
import hoisted, same result); `canon.uuid_text` 0.40 -> 0.17 us;
`nanotime.parse` 1.77 -> 1.56 us; `leave_out` copies each change dict.

**SP19. Other fixed polls, each a full connect** (P2, S).
`freeze._pg_end_sessions` 0.1 s with a psql per poll (701), the MySQL
session-end wait 0.1 s (423), PG `pause_writer` 0.5 s (476), PG snapshot
visibility 0.2 s (2251/2281), MongoDB fence 1 s (mongodb.py 1334), Kafka
fence 1 s (547), Redis fence 0.2 s (1337), `tailctl.follow` idle 1.0 s,
`tailctl.pause`/`hold_if_asked` 0.5 s. Fine as they are: cutover's 0.25 s
writes window, `machine.observe` sampling, the generation wait, the retry
back-offs, the lease/checkpoint compare-and-swap retries, MySQL's server-side
`fence_wait`. Checked and not worth changing: `decide.attempt` 5.1 us a
call; `match_tables` 1.3 ms at 2,000 x 2,000.

---

## 8. One logic in two places

**DU1. DuckDB never compares sequences though its code exists** (P0, S).
`engines/duckdb.py:16` `checks = ("schema","counts","data")`, while
`duckdb.py:243` has a whole `check_autoinc` and `capabilities.matrix()` says
sequences "yes"; `cli.py:1034/1044` build the list from `eng.checks`, so
`--only autoinc` is silently dropped (V2) and a target sequence behind the
source passes green. Compared for all 28 engines: DuckDB is the only one
where the matrix says yes and `checks` leaves it out. Fix: add `"autoinc"`;
better, `Engine.checks` from `capabilities.implemented()`.

**DU2. Four places decide "green"** (P0, M) - V1. Measured beside it:

| results | verdict.json | `migkit check` | cutover final check |
|---|---|---|---|
| all skip | incomplete | exit 0, "all green" | flips, "found the two the same" |
| ok + warn | same | exit 1 | undo |
| ok + skip | same | exit 0 | flips |

A large-object headroom warn fails CI through `integrations.passed` while
`verdict.json` says same. All-skip must never let the cutover flip (cutover
work's file; it should call `verdict.decide`).

**DU3. PostgreSQL maps the target database twice at 33 call sites** (P0,
S-M). `_psql` maps the target through `_dsn` (295: `dbname={self._d(side,
db)}`), yet `_all_tables` 321, `table_facts` 493, `column_catalog` 534,
`neutral_columns` 556, `neutral_key` 571, `moved_nothing` 4840 and lines
3962, 3989, 4479, 5137, 6611-6614, 6809 call `_psql(side, self._d(side,
db))`, while `check_counts` 3250 passes `db`. Measured: map `{shop: shop_v2,
shop_v2: shop_v3}` - the double-mapped call opens `shop_v3`; a swap `{app:
app_blue, app_blue: app}` - counts read `app_blue` while tables, columns and
keys read `app`. Narrow trigger (chained or swapped maps), silent when it
hits. Fix: every engine helper takes the source name and maps inside.

**DU4. Filters imported from AWS DMS quote their own way** (P0, S).
`imports.py:298-343` `_q`/`_lit`: `_q` leaves any lowercase name unquoted -
on PostgreSQL a filter on a column named `user` becomes `user = 'bob'`,
i.e. `current_user = 'bob'`, usually zero rows on both sides, so "same" over
nothing; `order` a syntax error; `_lit` does not double backslashes (MySQL
reads `'C:\new'` with a newline). Measured: `_q('user','postgres')` ->
`user`; `_q('order','mysql')` -> `order` (engine: `` `order` ``);
`_lit(r'C:\new\table')` unchanged (MySQL's `_quote_literal`: doubled). Fix:
the predicate built through each side's `_quote_ident`/`_quote_literal`.

**DU5. "A user table on PostgreSQL" defined seven ways** (P1, M).
`INVENTORY_SQL` (18) and `USER_TABLES` (270, `relkind='r'`) exclude `pg_%`,
`__%`, `migkit\_%`; `_all_tables`/`neutral_tables` (315/518) read
`pg_tables` - partitioned parents and their partitions, `pg_temp_N` temp
tables, `__%` schemas and `migkit_*` included; `table_facts` 507 and
`row_sizes` 10046 two more; `movers.PG_USER_TABLES_SQL` (677) quoted names;
`_keep_tbl` 3217 adds `noise_prefix`, which `neutral_tables` ignores. So
`check_counts` and the fast checksum compare one set, neutral/hetero/second
reader/two-way another; a PG-to-X copy takes the parent and each partition;
another session's temp table breaks it. Fix: one `user_tables(side, db,
kinds=...)` in SQL.

**DU6. Engine names normalised in some places, not others** (P1, M).
`engines/__init__.py:1-44`, `hopkeys.sides` 813/883, `config.build_hop`
702-741, `DEFAULT_PORTS` 529, `users.compare/create/rollback` (127, 1886,
2035). Measured: `engine: mariadb`/`aurora-postgres`/`percona` keeps the raw
name and gets port 0; a long-form hetero hop port 0 on both sides; `engine:
postgresql`/`sqlserver`/`valkey` rejected while the same word as an address
scheme gives postgres:5432, mssql:1433, redis:6379; 14 names accepted only as
schemes, 16 only as `engine:`; `users` compares the raw name, so a mariadb or
aurora hop raises "not supported" while `capabilities.unavailable('mariadb',
'users')` is None. ~15 call sites repeat `ALIASES.get(hop.engine,
hop.engine)` (two did not - K8). Fix: `build_hop` stores the canonical name
(the brand kept apart); one table for aliases and schemes; `engines.canonical()`;
default ports on the engine classes.

**DU7. The Redis engine drops the ACL user name** (P1, S).
`engines/redis.py:22-26` and 71-75 pass only `password`;
`users._redis_client` (1823), the bulk mover (movers.py 4445) and RedisShake
(redisshake.py 219) pass `username=ep.user`. With an ACL user the move
succeeds and check, repair and tail authenticate as `default` (WRONGPASS, or
another key set). Fix: `username=ep.user or None` in both clients.

**DU8. Fifteen connection-string builders, each escaping differently** (P1,
M). Nothing escaped (`postgres.check_structural_diff` 2826); password but
not user (`postgres._schema_urls` 2926, `mysql._schema_urls` 2143,
`mysql._reladiff_url` 2573, `hetero._url` 2382, `movers.pgcopydb_follow`
5903, `pgloader_move` 4187); both (`leaves._url` 1730,
`movers.pgcopydb_move` 5148); `quote()` with default `safe='/'`
(`users._mongo_client` 1675); `quote_plus` (`mongodb._client` 24); TLS
dropped by 6. Measured with user `app@corp` and a password holding `/:@#`:
`check_structural_diff` parses to host `'corp'`; `_schema_urls` sends the
user unescaped; `users._mongo_client` parses host `'app%40corp'`, user None.
Fix: one `Endpoint.url(scheme, db, tls=True)` - `quote(..., safe='')` for
user and password, brackets for IPv6, the TLS query (SEC2).

**DU9. Five ways of hiding passwords** (P1, S-M) - SEC12.

**DU10. Five duration formatters that disagree** (P1, S).
`wording._duration` 185, `util.human_secs` 189, `machine._took` 353,
`planner._took` 272, `guide._took` 650 (callers `movers._took`,
`notify._took`). Measured: 59.6 s -> `1m00s` / `59s` / `60 seconds` / `60s`
/ `60 seconds`; 3599.7 s -> `1h00m` / `59m59s` / `60 minutes`; 0.4 s -> `1
seconds` (guide). Fix: `wording._duration` (it rounds) for all.

**DU11. Two retry helpers and none for PostgreSQL** (P1, M).
`util.with_retry` 124 (exponential from 0.8 s, transient only) used by MySQL
(5) and MongoDB (2); `users._retry` 44 retries everything, fixed 5 s, 4
attempts, printed to stdout; 20 more hand-written sleep loops (AST); PG
`_psql`/`_conn` never retry. Measured: "password authentication failed" ->
`users._retry` 4 attempts, 15 s of sleeps (a lockout risk), `with_retry` 1;
`is_transient("...:27017: timed out")` is False. Fix: `with_retry` the only
policy, including `users`.

**DU12. PostgreSQL connections opened 13 ways** (P1, M-L). postgres.py 590,
613, 684, 2069, 2160, 5295, 8659, 9834, 10484, 10506; `pg_pass:67`,
`legs:135`, `users:74`, `cutover:276`: connect timeouts 15, 5 or 3 s;
keepalives only in `_dsn`, 2069, `pg_pass`, `legs` (`legs.py:406` claims
"every connection"; `_conn`/`_conn3` have none, so a dropped tunnel hangs a
read ~2 h); `_psql` pins `TimeZone=UTC`, `_conn` does not, and `run_rule`
uses `_conn` (postgres 339; MySQL's 148 while its `_q_utc` at 1940 pins) -
business rules run in each server's own zone, false rule differences between
zones; `users._mysql_conn:62` has no `read_timeout` or keepalive. Fix: one
`_connect(side, db, purpose)` per engine owning timeouts, keepalive, session
settings and TLS.

**DU13. System user and role lists disagree** (P1, S). `users.MYSQL_SYS:22`
vs the MySQL assess filter (`mysql.py:5716`); `users._pg_sysrole:38`/`PG_SYS`
vs the PG grants filter (6590). Measured: `tencentroot`, `tencentdba`,
`AWS_*` are system for `users`, not for assess (assess on a Tencent source
always warns "missing" for accounts `users` never creates); `postgres`
system for `users`, compared by grants; `app_tencent_rw` compared by `users`,
ignored by grants; neither knows `cloudsqlsuperuser` or `azure_pg_admin`.
Fix: one `system_principals(engine)`, rendered into SQL for assess and
grants.

**DU14. Five ways of splitting `schema.table`; the mover and the check
disagree on exclusions** (P1, M). `partition` (`postgres._split` 2618),
`split(".",1)` (`base._qualified` 939, 74 sites), `rpartition` (25 sites),
every dot (`movers.excluded_tables` 1955, `digests._quoted` 151), last part
only (56 sites). Measured with `public."v1.orders"` and `exclude: [orders]`:
`movers.excluded_tables` leaves it out, `hop.excluded` says False - the mover
skips a table the check compares; `digests._quoted` renders
`"public"."v1"."orders"`. Fix: one `split_table(engine, name)`/`qualify()`.

**DU15. SQL Server's database list ignores `exclude`** (P1, S).
`mssql.databases` 43-49 (the PG, MySQL, Mongo, ClickHouse versions filter
with `hop.excluded`); `mssql.SKIP_DBS:8` unused, the SQL repeats it and lacks
`rdsadmin`. Fix: filter with `hop.excluded`, use `SKIP_DBS` plus `rdsadmin`.

**DU16. `noise_prefix` and other tools' tables in different classifiers**
(P1, S). `noise_prefix` only in `postgres._keep_tbl` 3226, not
`neutral_tables`, movers or MySQL; `drift.transient` (case-sensitive,
neighbouring tables) and `leftovers.bookkeeping` (lowercased, name only) both
claim gh-ost's `_ghc` tables with different rules (the merge the outward
work was told to do). Fix: both into `Hop.excluded`.

**DU17. Capability declarations disagree with the code** (P1, S). Measured:
`capabilities.stale()` -> `[('kafka','bulk-move')]` (declared not applicable
at `capabilities.py:82`, but `movers.supported('kafka','mirror')` is True);
`decide.uncovered()` lists iceberg and delta as undeclared for keyless,
no-unique-index and wide-key tables; `_monitor` filters `--only` on
`eng.checks` (drops `deep`, `params`); `decide.NOT_YET`/`NOT_APPLICABLE`/
`_STREAM_ONLY` duplicate `capabilities` with other words.
`tests/test_every_engine_declares_its_gaps.py` should already fail.

**DU18. Row estimates from three sources with three "unknown"s** (P1, S).
`_chunkable`/`BIG_TABLES_SQL` (postgres 3385) `coalesce(n_live_tup,0)`;
`table_facts` 495 reltuples with -1 as None; `row_sizes` 10030,
`group_candidates` 3478, `changes_made` 5139 `greatest(reltuples,0)`. After
a statistics reset or fail-over a billion-row table reads 0 rows, so no
chunked checkpointing. Fix: one `row_estimate(side, db)` returning None for
unknown.

**DU19-DU26** (P2 unless said). DU19: primary-key column order decided three
ways (`neutral_key` 567 and `_pk_cols_of` 7915 key order; `PK_COLS_SQL` 8056
by `attnum`, used by `_key_hash_expr` - the dependence `_row_hash_expr` 8111
warns against; `cli.py:952` and `_int_pk` 8931 unordered) - everyone calls
`neutral_key`. DU20: a double-quote helper exists 8 times
(`base._quote_ident`, `postgres._quote_ident`, `dbapi._q`, `freeze._q`,
`legs._q`, `cassandra._q`, `lake._quote`, `imports._q`); MySQL `_my_ident`
(163) copies `_quote_ident` (3469); 25 raw `'{sch}'`/`'{tbl}'` literals in PG
catalogue SQL though `_quote_literal` exists (SEC4). DU21: the `at` field in
seven formats (epoch, `%F %T`, `%Y-%m-%dT%H:%M:%S`, with `%z`, `%Y-%m-%d
%H:%M`, `datetime.now().isoformat()`, the session id), almost all local time
with no zone - one `util.stamp()` in UTC ISO with zone. DU22: ~20 atomic
writes in three styles (ST1). DU23: `unchanged.server_major:75` identical to
`toolversion.server_major:63`; `physical._series` 772 and `jobs._my_off` 86
parse versions two more ways; MariaDB detected five ways though
`variants.SIGNATURES` exists. DU24: `human_bytes(10**12)` prints "931.3 GB"
(1024s labelled GB); `machine` says GiB; AWS's published 78.75 MB/s prints
"75 MB/s"; inline `2**30` in `ddbexport`, `oracle_logminer`. DU25:
`redisshake._glob_re` (129) its own shell-pattern translation - keys holding
a newline match `fnmatch` but not the regex (4 cases measured), so the
program copies keys migkit's check excludes (P1). DU26: "migkit's own
tables" three ways - `config.MIGKIT_OWN` exact `migkit_origin`, PG
`'migkit\_%'`, MySQL `'migkit%%'` (also catches `migkitchen`); MySQL
`moved_nothing` 3386 has no filter (P1).

---

## 9. Logging, the dashboard, `/metrics`, wording, docs from code

**Shape (measured by AST).** No run log: 230 `console.print` in `cli.py` (0
escaped, 2 `markup=False`), 324 `log(...)` callbacks, 246 `say(...)`, 7
`print`; Python `logging` 3 times, only to silence libraries; no levels, no
machine-readable run output. 821 broad `except Exception` handlers, 394 of
them swallow (101 `pass`, 77 `return None`, 37 `continue`, 27 `return False`,
140 a fallback value), 76 of the silent ones without a word of why; nothing
enforces the `# noqa: BLE001` markers.

**LG1. The dashboard and `/metrics` contradict `verdict.json`** (P0, S-M).
`ui.py:228-235` (`_hop_data`), `prometheus` 272+: anything not `ok`/`error`
counts as `diff`. Measured with a clean two-database MySQL run in memory:
`cli.py:1116-1124` writes a `params` skip for every database after the first
on engines with `SETTINGS_PER_DATABASE=False` (mysql, mssql, mongodb, ...) ->
dashboard `diff`, `migkit_hop_status=1`, verdict `same`; a narrowed `--table`
run shows `ok` where the verdict says `incomplete` (the false "same"
`verdict.py:360-379` says was fixed). The shipped alert `MigkitHopDifferent`
(critical) pages on clean runs; the Grafana panel reads it as "0 same, 1
different". Fix: the dashboard and `/metrics` read `verdict.json`; a
state-set `migkit_verdict{status=...} 1`.

**LG2. Deep checks say `ok` over probes that failed** (P0, S-M).
MongoDB sharding (`mongodb.py check_deep` 1862-1897): `mongos()` returns
False on any exception -> "ok: not a sharded cluster"; the `balancerStatus`
failure `pass`ed yet "balancer idle"; the duplicate-`_id` aggregation `except
Exception: continue` -> "sharded: no dup _id" (duplicate ids are duplicated
documents). MySQL (charset 4210-4229 over `tcols[:30]` with `continue`;
generated STORED columns 4262-4275 `pass`; `_deep_nullempty` 4632/4654/4669;
`_deep_float` 4679/4687; `_lob_check` 3163-3166) and PostgreSQL
(`_lob_sizes` 4472-4474): a failed read drops the column and the result still
reads `ok`. Fix: one probe accumulator in `engines/base`: a result whose read
count is under its planned count becomes `skip`/`warn` ("compared 12 of 30
columns; not read: a.b (timeout)").

**LG3. The cutover's "is the source quiet" treats unreadable as quiet**
(P1, S; the cutover work's files). `cutover._quiet` 854-876 (an `open_work`
error becomes `left = []`, though its comment says "not readable is not
proof"), `freeze.writes_seen` 1233-1260 (`None` skips the write-count test;
RDS MySQL without Performance Insights has `performance_schema` off), 
`cutover._slot_rows` 605-617 (`[]` on error, so other consumers of the
source are not warned about). Fix: an unreadable probe is a gate row "could
not prove the source quiet: ...".

**LG4. Evidence and the zone pin lose information silently** (P1, S).
`digests.record` 84-89 swallows a digest it cannot write (the evidence lists
fewer tables, unmarked); `mysql._rows_utc` 1947-1950 swallows a failed `set
time_zone` (the zone-shift check runs unpinned). Fix: count failed digest
writes into the verdict and evidence; the zone failure raises.

**LG5. No single run log; run ids and timestamps inconsistent** (P1, L).
The run id (`session.current()`) is never printed (used in audit 62,
digests 74, cli 1196, verdict 406), so the console cannot be matched to
`evidence/<run>/`; two run-id schemes (`session.py:37` and `repairlog.RUN`,
fixed at import); seven timestamp formats (DU21); the diagnostics bundle
(60-101) leaves out `commands.log` (which `DebugLog` calls "what someone
attaches to a bug report") and `decisions.jsonl`. Fix: one run-log module -
each line a JSON record `{ts (UTC), run, hop, db, table, phase, level,
event, msg}` in `reports/<hop>/runs/<run>.jsonl`, the console rendered from
the same record; "run <id>" printed first; repair records stamped with
`session.current()`; the run log in diagnostics. (P0-SMART's decision record
goes into the same log.)

**LG6. Stream metrics are not per hop** (P1, S). `movers.stream_readings`
7361-7390 and `ui._stream_metrics` 519-545 read every connector MBean on
`localhost:8778` and sum/max them; the same values are attached to every hop
with a `stream/` directory, stopped ones included, so `MigkitStreamBehind`
fires on the wrong hop. Fix: filter by the `migkit-{hop}-source` connector;
a port per hop.

**LG7. Every `/metrics` scrape does live work** (P1, M). Each scrape runs
`load_hops()` (K12), reads each whole `changelog.jsonl` for its last line
(258) and all of `digests.jsonl`, spawns `pgcopydb ... sentinel get` (15 s
timeout) and opens a source connection per running follow leg
(movers.py 6277-6295), and makes three HTTP calls with 2 s timeouts per
streaming hop - serially, so a scrape can pass Prometheus's 10 s default and
be lost; a `hops.yaml` error raises `SystemExit` in the request thread (no
response). Fix: a background collector with a cached snapshot,
`migkit_collect_age_seconds`, `migkit_collect_errors{family}`.

**LG8. `migkit_table_same` and the evidence pair sides from different runs**
(P1, S). `digests.read_in` 113-130, `ui.py:458-477`, `export._tables`
349-365 keep the latest line per side across all runs; digests are salted per
run, so a cross-run pair always reads "different" - when one side's read
failed (`sqlite.py:918-921` records only the side that worked), a check was
killed mid-table, or `--table`; `MigkitTableDifferent` fires and the evidence
headline contradicts itself. Fix: pair within one run; a lone side is "one
side only".

**LG9. Metric naming** (P1, S-M). All 47 families typed gauge;
`migkit_check_total` a gauge with `_total`; monotonic `tail_changes_applied`
and `stream_events_seen` should be `_total` counters; `migkit_check_pass`'s
help says "Passing databases" but counts answers (the report's tiles count
databases); `migkit_move_eta_seconds` uses the reserved `quantile` label,
with `quantile="live"`; label values are sentences (`way`, `resource`,
`truth="measured on an earlier run"`), so rewording breaks alerts; some
families only ever emit 1 (`cutover_running`, `tail_stopped`,
`phase_never`); labels unescaped (`db="{d.name}"` and others) - one `"` in a
name invalidates the scrape; "fire when > 0" also fires for never-checked
hops (3). Fix: one sample helper that always escapes, stable key labels,
renames with a deprecation period, a parser-plus-lint test.

**LG10. The shipped deployment cannot be scraped** (P1, S).
`deploy/docker-compose.yml` publishes 8899 while `serve()` binds 127.0.0.1
inside the container; the Host check allows only localhost unless
`MIGKIT_UI_HOSTS`; the ui service mounts no `./conf`, so `load_hops` raises
on every request; `deploy/README` says "dashboard: http://<vm>:8899". Fix:
the bind address from an env var, compose fixed, the proxy secret (SEC5).

**LG11. Seven verdict vocabularies for one answer** (P1, M). `verdict.json`
same/different/incomplete/error; each answer ok/skip/warn/diff/error; the
console OK/DIFF, "all green", "N problems"; `report.html` PASS/FAIL; the
dashboard ok/diff/error/idle; metrics 0-3 relabelled by Grafana; evidence
same/different/"one side only"/"no check in this run"; notify never says
`incomplete`; watch `c:ok`, `c:DIFF`, `c:ERR`, "SAFE TO CUT OVER"; `users`
pass/gap; cutover "found the two the same". Fix: one `VERDICTS` table
(status -> word, exit code, colour, metric value) every output renders from,
and a test that fails on verdict words written anywhere else.

**LG12. Same event, different words; fallbacks outside the decision
record** (P2, S). A fallback is "could not run ... switching to"
(decide.py 966), "could not go on ... now" (notify), "fallen back"
(session); five fallbacks bypass the decision record, the `fallback`
notification and traces (cli.py 3150, movers.py 5254/5300, tailtables.py
683/692 - partly the sites the fallback sweep listed); "missing on target"
said 9 ways across 108 occurrences; notify's `event` values "tail stopped",
"table parked", "tail started" (504/517/526) are not in `EVENTS`; `-q`
filters by substring ("DIFF"/"ERROR") while SQLite streams lowercase
"diff"/"error" (901/926/933), hidden under `-q`; library warnings
(cassandra-driver, pymongo, botocore) reach stderr raw with their names -
only kafka, sqlglot and apprise are silenced. Fix: event words from one
table; a root logging handler that routes library records into the run log.

**LG13. Unbounded append-only files, read whole** (P2, S).
`changelog.jsonl` gets an "evidence" anchor per run (export.py 919-923) that
floods the dashboard's "recent writes"; `digests.jsonl`, `decisions.jsonl`,
`conflicts.jsonl`, `commands.log` and one `evidence/<run>/` per run grow
forever; `follow.log` (raw program output) is not redacted. Fix: rotation by
size and age with the chain head carried; tail reads from the end.

**LG14. The two generated-docs tests fail as the tree stands** (P1, S).
`troubleshooting.render()` does not match `docs/troubleshooting.md` (53,990
vs 52,794 characters): four documented refusals no longer raised (approval
wait, schema changed during a move, cutover undone, partition not copied),
new ones missing (`fromsnapshot.py`, `mirror.py`, `redisshake.py`, snapshot
visibility); `capabilities.stale()` returns `[('kafka','bulk-move')]` -
`test_the_troubleshooting_guide_is_current` and
`test_every_engine_declares_its_gaps` would fail. Fix: regenerate both; a
pre-commit hook that runs the generators.

**LG15. `goals-and-modes.md` says it comes "from one table in the code" but
is hand-copied** (P1, M). Four cells drifted (`copy.target`,
`cutover.guarantees` without "(the position is taken before the copy)",
`two-way.guarantees`, `cdc --drop`); the test
(`test_every_goal_and_mode_is_explained.py` 151-163) checks names only. Fix:
`tools/gen_docs.py` writing between markers, an exact-match test (R24).

**LG16. Engine and command lists in the README, the site and `--help` are
wrong** (P1, M). 26 engines in the code, 12 on the README badge (iceberg and
delta missing); "Schema comparison (JDBC): db2, h2, firebird, informix..."
and "databricks" exist nowhere in `migkit/`; oracle "not yet run" while the
matrix says all 16 capabilities yes; the site lists an alias `aurora` not in
`ALIASES` (a hop written from the site fails); ~12 engines missing from the
site; `cli.py:274-278` lists clickhouse and oracle under "generic"; "Eleven
commands" where 13 are visible. Also: 37 of 70 read `MIGKIT_*` variables in
no doc (incl. `SOURCE_PASSWORD`, `TARGET_PASSWORD`, `REPORTS`,
`FAIL_ON_DIFFERENCE`); no exit-code table; no metrics reference; undocumented
`check --exclude/--limit/--resume`, `report --metrics/--refresh`, `watch
--teardown`, `sync --tag`. Fix: R24's generator from `NAMES`/`ALIASES`/
`matrix()`, `envkeys`, the exit-code table and the metric families, with
tests.

---

## 10. The test infrastructure and the one final run

Read at `5e30188` by static scans of `tests/` (no test or container run).

| What | Count |
|---|---|
| test files / `def test_` functions | 546 / 3,614 |
| files that start containers themselves (`"docker", "run"`) | 281 |
| module-scoped fixtures / session-scoped | 308 / 1 (`pg_pair`) |
| files using the session `pg_pair` | 173 |
| files starting their own `postgres:16` | 92 (57 with no server argument, 11 more with only `wal_level=logical` - what `pg_pair` already is) |
| files starting their own MySQL (`mysql:8.4` 78, `mysql:8` 51, `mysql_pair` 8) | 137; two tags of one server pulled; 43 of them a stock server |
| files starting their own `mongo:7` | 37 |
| distinct images | 26 (three `:latest`) |
| distinct container names / names shared by several files | 508 / 9 |
| host ports named by more than one file | 36 |
| `-p` mappings not bound to 127.0.0.1 | 182 (`pg_pair` and `mysql_pair` among them) |
| files with their own `docker info` probe / own ready loop | 138 / 127 |
| `docker exec` SQL helper call sites (`psql(`, `_psql(`, `my(`, ...) | ~2,100, plus two `docker exec psql` wipes before every `pg_pair` test |
| `time.sleep`: polling inside a loop / fixed waits | 569 / 144 (140 literal, 397 s static; 79 directly in test bodies) |
| files that `docker build` an image | 10 |
| skips that turn an infrastructure failure into a pass-looking run | 8 ("kafka did not come up in this sandbox" and kin) + 17 "not installed here" |
| docker-backed files with no `docker` mark | 11 |
| pytest plugins installed | none (no xdist, no timeout) |

**How long the final run takes as it stands (estimate).** The last
whole-suite run on record (`/tmp/migkit-suite.log`, 2026-09-27, 336 files,
1,968 functions, 2,284 collected) took 6,835 s (1 h 54 min) with 28
failures. Scaled by functions (3,614 now, ~4,200 collected at the same
parametrize factor) that is ~12,500 s, 3.5 h, on the default profile - before
the Oracle and x86 files that need `--vm migkit` and before any failure is
re-run; one serial pass under the one lock, every other session held off
docker the whole time. A hung test stops it forever: no per-test timeout.

**T1. Every module starts its own servers** (P0 for the final run, L).
`tests/conftest.py:pg_pair` is the only session fixture; 281 files `docker
run` their own containers in module fixtures, 137 of them a MySQL whose
first start initialises a data directory (the ready loops allow 300 s); 68 of
the 92 own-PostgreSQL files ask for nothing `pg_pair` does not give; 43
MySQL files ask for a stock server. Server profiles actually used: PG stock,
`wal_level=logical`, TLS, replica pair (`hot_standby`, senders), a few
single settings; MySQL stock, GTID on, `binlog_row_metadata=FULL`,
`local_infile`, a few `innodb`/`performance_schema` sizings. Fix: one
`tests/servers.py` with session fixtures keyed by a server profile (image +
server arguments + TLS), started once and shared by every module naming the
same profile (PG stock/logical, PG TLS, PG replica pair; MySQL stock, GTID
on, row metadata full, local infile; MariaDB; Mongo replica set; Redis;
Kafka/Redpanda; Cassandra; ClickHouse; OpenSearch; DynamoDB local; MinIO;
SQL Edge; Oracle), each module isolated by its own database, schema,
keyspace, topic prefix or db index created in its fixture and dropped after.
A test that needs a server of its own (kill, fail-over, slow link, a setting
changed under it) keeps a private container and says why. Estimated:
container starts from ~400 to ~20; MySQL alone ~130 x 20-30 s.

**T2. The session pair leaks state between modules** (P0, S). `_WIPE`
drops `public`'s tables, sequences, routines, extensions and large objects in
database `postgres` only; 11 `pg_pair` files create databases and never drop
them, 12 create slots with no drop in the file, 5 publications, 5
subscriptions, 6 roles, 2 schemas. The 2026-09-27 run's failures show it:
three tests failed on `duplicate key value violates unique constraint
"pg_largeobject_metadata_oid_index"` - another test's large objects. A slot
left on the shared source holds WAL for the rest of a three-hour run. Fix:
the wipe also drops every non-template database but `postgres`, every slot,
publication, subscription, non-bootstrap role and non-system schema; better,
T1's per-module database.

**T3. No parallelism, no timeout** (P0 for the final run, M). No
pytest-xdist, no pytest-timeout (both MIT). Fix: both in the `dev` extra;
`--timeout 900 --timeout-method thread` (a stack dump of the hung test);
`xdist --dist loadgroup`, one group per server profile, each worker with its
own port block (`base + 100 * worker`) and container-name suffix, so profiles
run side by side (T4 blocks it today).

**T4. Fixed ports and names clash across files** (P1, M). 36 host ports and
9 container names used by more than one file (the stored-code files share
16080-16084, 16294-16297 and `migkit-test-code-pg`); a serial run survives
only because each module removes its containers. Fix: one allocator
(`port(profile, worker)`) in `tests/servers.py`, and a test failing on a
hard-coded host port in a test file.

**T5. 182 port mappings bind every interface** with password `test` (P1,
S); the owner's rule is containers on 127.0.0.1. Fix: T1's helper always maps
`127.0.0.1:{port}`; T4's test checks it.

**T6. The Oracle and x86 files carry no mark** (P0 for the final run, S).
`conftest.ENGINE_MARKS` marks `mssql`/`db2`/`ase` by file name for the x86
workflow; nothing marks what needs the `--vm migkit` profile
(`gvenzl/oracle-free`, SQL Server 2022, Db2, ASE under Rosetta), so one
`pytest tests/` on the default profile starts them there. Fix: a `vm` mark by
file name, skipped with a reason unless `DOCKER_CONTEXT=colima-migkit`; the
final run is two shards at once - `-m "not vm"` on the default profile and
`-m vm` under `with_docker_lock.py --vm migkit` - two daemons, each with its
own memory.

**T7. One final-run script** (P0, M). `tools/final_suite.py`: (1) pure tests
first, in parallel (`-m "not docker" -n auto`: 159 files have no container,
and many docker files hold pure tests) so a broken import fails in minutes;
(2) every image pulled once, in parallel, by digest; (3) the install state
checked so "not installed here" skips are known before the run; (4) the
docker shards of T3/T6 under the lock taken once; (5) `--junitxml` per shard,
`-rs` so every skip is listed with its reason, failures grouped by file; (6) a
re-run of only the failed node ids. Estimated: under an hour instead of
~3.5-4.5 h.

**T8. Infrastructure failures are skips** (P0 for the final run, S). "kafka
did not come up in this sandbox" and its siblings skip; a run where Kafka
never started reads green. Fix: `MIGKIT_TEST_STRICT=1` (set by the final run)
turns an infrastructure skip into a failure, and conftest counts skips by
reason in the terminal summary.

**T9. Fixed waits** (P1, M). 144 bare `time.sleep` calls wait for something
asynchronous rather than for its sign - `test_live_stream.py:46` 15 s,
`test_two_way_counters_add_on_both_sides.py:201` and
`test_two_way_marks_climb_the_ladder.py:263` 8 s, six files 6 s. On a loaded
machine each is a flake; on an idle one, waste. Fix: `tests/waits.py:until(
condition, timeout, what)` and the tail's own caught-up signal
(`follow_until`) instead of a nap.

**T10. One container helper, one SQL helper** (P1, M). 138 files probe
`docker info`, 127 write their own ready loop, ~2,100 call sites run SQL
through `docker exec` (a process and an exec per statement). Fix: T1's
`tests/servers.py` owns start, ready, SQL through the driver on the mapped
port (`docker exec` only for server-side programs), wipe and removal.

**T11. Images built per module** (P1, S). 10 files `docker build` a sandbox
image in their fixtures. Fix: built once per session, tagged by the hash of
its Dockerfile and inputs, reused while the hash holds.

**T12. CI runs a different suite from the owner's** (P1, S).
`.github/workflows/ci.yml` installs `pip install -e . faker pytest`, not
`.[dev]` - `moto` missing, so `test_a_cloud_copy_seeds_the_target.py`,
`test_dynamodb_copied_through_its_export.py`, `test_state.py` and
`test_the_machine_to_run_on.py` skip on `importorskip` there;
`release.yml`'s `verify` job gives the full suite `timeout-minutes: 30`
against a run that took 1 h 54 min two weeks ago and pre-pulls 3 of the 26
images. Fix: `.[dev]` everywhere; the release job runs T7's script with a
timeout from its measured time.

**T13. 11 docker-backed files carry no `docker` mark** (P2, S):
`test_a_clone_goes_on_exactly_mysql.py`,
`test_a_clone_plugin_offer_on_mysql.py`,
`test_a_keyless_table_resumes_by_its_rows_hash.py`,
`test_a_physical_seed_goes_on_exactly_pg.py`,
`test_cdc_after_a_separate_copy.py`, `test_db2_as_one_side_of_a_pair.py`,
`test_large_transactions_stream_to_the_subscription.py`,
`test_oracle_as_one_side_of_a_pair.py`,
`test_sql_server_as_one_side_of_a_pair.py`,
`test_the_native_replica_stays_in_the_hop.py`,
`test_which_schema_changes_a_native_replica_carries.py` (several ask for
`pg_pair` with no mark) - `-m "not docker"` starts containers. Fix: conftest
marks every test that asks for a server fixture as `docker` itself.

---

## 11. One-line fixes

**Made in this audit** (certain, no behaviour a hop relies on changes except
the wrong answer):

1. `migkit/hopkeys.py:said_by_doctor` - the hetero engine set and the per-side
   engine now use `ALIASES.get(x, x)`, as every other caller does; a
   cross-engine hop's TLS options are no longer called useless (K8; checked
   in-process: no remark on `ssl_ca`/`sslmode` for a MySQL->PostgreSQL hop).
2. `migkit/movers.py` (the change-stream bundle) - the connector REST port
   published on `127.0.0.1:8083` only, beside the monitoring port that
   already was (SEC1).
3. `packaging/homebrew/migkit.rb` - `license "AGPL-3.0-or-later"` (was MIT).
4. `.gitignore` - `deploy/conf/`, where `deploy/README.md` tells the operator
   to put the filled hops file (W5).

**Ready for the groups** (each certain, left to the owning group so it lands
with its test):

* `engines/duckdb.py:16` add `"autoinc"` to `checks` (DU1).
* `engines/redis.py:24` and `:73` `username=ep.user or None` (DU7).
* `engines/mssql.py:49` `if not self.hop.excluded(r[0])` (DU15).
* `wording.py:197` `(://[^:/@\s]*:)[^@\s]*@` so `redis://:pw@` is masked
  (SEC12).
* `verdict.py:393` `elif coverage or stale or not records:` (V2).
* `cli.py:3502` `if delta or teardown:` (V15); `cli.py:3653`
  `click.launch(...)`; `cli.py:18` `Console(highlight=False, soft_wrap=True)`;
  `cli.py:1737` `here = movers.programs(via) != ("docker",)` (W10);
  `cli.py:2163` `point = _first_point(ck, point)` when not a table (ST4);
  `cli._Checkpoint.save` `separators=(",", ":")` (SP4).
* `ui.py:234` `elif st == "diff" and pc["status"] != "error":` (LG1);
  `ui.py:937` the resolved parent check (SEC6).
* `postgres.py:1230`, `sqlite.py:409`, `cassandra.py:215`,
  `clickhouse.py:335` pass `col[0] in key` to `canon.ddl_type` (SC6).
* `canon.py` `TYPES["postgres"]["citext"] = "text"` (SC5).
* `mysql.py:1997` add `*self._client_tls(side)` to the schema dump (SC17);
  `mysql.py:7882-7883` `fresh=True` (V14).
* `movers.py:1455` `-j` on the post-data restore (SC21); `movers.py:5617`
  `passfile = work / "pgpass"` (W19).
* `approvals.py:101` `need = max(1, int(rule.get("count", 1)))` (K9).
* `schedule.py:106`, `:91`, `hetero.py:4290`, `machine.py:705`,
  `integrations.py:139` - through `envkeys.flag` (K13).
* `evidence._LineSealer.flush` (ST13); `schedule._alive` `PermissionError`
  = alive (ST10).
* `registry.py:115`, `oracle_logminer.py:596` resolve through `_secret` (K7).
* `.github/workflows/release.yml:77` read `migkit/__init__.py`'s
  `__version__` (W21); `tools.py:1068` `env=tool_env(), timeout=120` (W22);
  `util.py:178-181` no argv in the message (W3); `mssql.py:154` hint in
  migkit's words (W3); `pyproject.toml` `requires-python = ">=3.10.12"`
  (W24).

---

## 12. The groups

The same groups, with their files and items, are in `docs/backlog.md`
section "P0-AUDIT". Waves: wave 0 lays the shared helpers; the wave 1 groups
have pairwise disjoint files and run side by side; wave 2 takes files wave 1
held; the test migration and the sweep run last, when no other work edits
tests. Files that running work also edits are named in each group
(P0-SMART: `decide.py`; cutover gaps: `cutover.py`, `freeze.py`; two-way
conflicts: `twoway.py`; repair ladder: repair paths, `repairlog.py`;
P0-PARITY: the other engines, `canon.py`, `sizing.py`; the table-way ladder:
`movers.py`, `engines/pg_pass.py`, `engines/my_pass.py`).

| Group | Wave | Priority, size | What | IDs |
|---|---|---|---|---|
| AUD-0 | 0 | first, S-M | the shared helpers: durable writes, env registry, one redactor, `Endpoint` URL and TLS forms, catalogue shapes, probes, stamp, duration, split, retry | ST1, ST9, K13, SEC12, SEC2, DU8-DU11, DU14, DU21, DU24, LG2 |
| AUD-A | 1 | P0, L | one verdict, one exit code, one write guard; the CLI flows; schedule, approvals, the move checkpoint | V1-V17, K3-K6, K9, ST3, ST4, ST17, ST24, SP4, W1, W10, SC7, SC21 |
| AUD-B | 1 | P0, L | durable tail state, one follow loop, locks and liveness, audit chain, evidence sealing, tail speed | ST1-ST2, ST5-ST6, ST8-ST16, ST18-ST23, ST25, K10, K14, SEC9, SEC15, SEC21, SP3, SP7, SP8, SP10, SP12 |
| AUD-C | 1 | P0/P1, L | dashboard and metrics from the verdict, dashboard security, the run log, what leaves the machine, capabilities and the troubleshooting guide | LG1, LG4-LG10, LG13-LG14, SEC5-SEC7, SEC16-SEC18, SEC20, K23, DU17, SP17 |
| AUD-D | 1 | P0, M-L | options, env and addresses checked at load, secrets lazy and resolved everywhere, the state root, engine names | K1, K2, K7, K12, K13, K15-K20, K22, K24, ST7, SEC10, DU4, DU6, DU16, DU26 |
| AUD-E | 1 | P0/P1, L | one runner for programs, stop means stop, the registry the one source, pinned installs, W8 | W1-W9, W12-W20, W22-W23, SEC1, SEC2, SEC14, SEC19, DU25, LG6 |
| AUD-F | 1 | P0, L | drift by fingerprint, SQL Server schema and permissions, users and grants, logged DDL, canon's types | SC2, SC3, SC5, SC9, SC12-SC13, SC18-SC20, SC22, SEC3, DU13, DU15, DU23, SP1, SP14 |
| AUD-G1 | 1 | P0, L | PostgreSQL: one connection, one catalogue read, grants, quoting, TLS | DU3, DU5, DU12, DU18, DU19, SC4, SC6, SC11, SC15-SC17, SEC2, SEC4, SEC9, SEC13, SP2, W15 |
| AUD-G2 | 1 | P0, L | MySQL: the same | SC10, SC14-SC17, SEC2, SEC4, DU12, DU26, LG2, LG4, V14, W11, SP2, SP14 |
| AUD-T1 | 1 | P0, M | test infrastructure: server profiles, marks, strict skips, xdist and timeout, the final-run script, CI | T2, T3, T5-T8, T11-T13, W21, W24 |
| AUD-H | 2 | P0, L | the pair's core: one schema comparer, the catalogue consumers, both sides at once, the probe accumulator | SC1, SC5, SC7, SC8, SC15, V11, SP5, SP6, SP9, SP11, SP15, SP16, SP18, LG2, W3, SEC18, SEC19, DU1, DU7, DU20 |
| AUD-DOC | 2 | P1, M | the manual and the site from the code (R24) | LG15, LG16 |
| AUD-T2 | 3 | P0 for the final run, L | every test module on the shared servers, waits by sign, the final run measured | T1, T4, T9, T10 |
| AUD-Z | 3 | P1, M | the sweep onto the one helpers and the lint tests that keep them | ST12, ST23, K13, K20, K21, DU2, DU10, DU11, DU21, LG3, LG5, LG12, SC6, SEC2, SEC4, W3 |
