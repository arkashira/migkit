# Working in migkit

## Find before you search, look before you create

- **Where things are**: read `docs/agents/CODEMAP.md` first (every module in one line, and the hubs most changes touch). For a symbol, `grep -P '^name\t' .codemap/SYMBOLS.tsv` gives its `file:line`; the Serena tools (`find_symbol`, `find_referencing_symbols`) and `tags` answer the same by meaning. Search the tree only for what these do not hold.
- **What changed**: `python tools/gen_codemap.py --since <ref>` - modules and symbols added, removed or changed.
- **What migkit can do, and what is left**: `docs/agents/STATE.md` (the capability table and the decision engine's coverage for every engine, every cell not yet done, the backlog by status), written from the code on every commit; `.codemap/LOCAL.md` says what this machine last proved (the full suite, unpushed work, agents' branches kept). Never answer it from memory or a summary.
- **Who calls what** (the reach of a change): `grep -P '^name\t' .codemap/CALLERS.tsv`, or Serena's `find_referencing_symbols`.
- **Before a new file or name**: look it up in the map and the symbols. A new file goes only where the map has none; an existing file is read, then edited. A new method name is checked against its class.

## One place

Logic lives once; reach for the owner rather than a second reading: the scope is `Hop.excluded` / `Engine.in_scope` / `Engine.takes` / `Hop.carries` / `Hop.target_table_in`; the verdict is `verdict.decide`; a row filter is `rowfilter`; a column transform is `transforms`; hop keys are `hopkeys.HOP_KEYS` / `HOP_OPTIONS`; environment variables are `envkeys`.

## Adaptive migration rule

Owner rule, 2026-10-06. Applies to every current and future migration task,
phase, step and work unit, on every database engine and applicable engine pair:
discovery, assessment, configuration, scope/mapping, planning, conversion,
schema/objects/accounts, move, replication/DDL, validation, diagnosis,
authorized repair/undo, cutover/rollback, recovery, security, HA and reporting.

- Make the workflow dynamic and deep throughout: observe and measure, analyze
  facts and uncertainty, compose the best feasible strategy, execute, verify,
  adapt the remaining work and update its forecasts. A flat checklist, wrapper
  dispatch, fixed default or startup-only decision is insufficient.
- Use the shared decision engine and existing domain owners. Let dependencies,
  engine semantics, data shape, actual performance, resource pressure and
  evidence determine the method, ordering, depth, work-unit sizes and execution
  concurrency. Acquire relevant missing facts through bounded safe probes.
  Reconsider choices at safe boundaries when conditions change; do not duplicate
  decision logic per feature or impose arbitrary worker ceilings.
- Optimize for the best verified complete outcome and the fastest execution
  within required correctness, consistency, idempotency, scope and authorization
  guarantees. These are hard constraints. Never silently weaken them, skip a
  failed check, replace full verification with sampling or imply completeness
  from a successful command. Respect deterministic invariants and justify serial
  steps where appropriate. Report achievable guarantees and unresolved gaps.
- Every current and future task, phase, step and work unit must have a durable
  resume contract across every supported engine/pair. Reconcile actual state
  and uncertain effects before replay; preserve order and idempotency, advance
  checkpoints only after confirmed durable effects and never discard failed
  required units. Automatic recovery respects explicit pause/cancel and
  authorization. Prove interruption and repeated-resume behavior; expose
  genuinely impossible recovery as a safe blocker. See P0-RESUME-ALL.
- Data safety takes precedence over speed. Preserve applicable transaction
  boundaries, dependency and change/event ordering, full data/value fidelity,
  deletes and duplicate semantics. Admit only parallel schedules and fallback
  paths that maintain these guarantees. Reconcile uncertain effects before
  retrying and advance checkpoints only after durable corresponding effects.
  If required safety cannot be established, stop the affected unsafe action
  and report the gap; never declare readiness from counts or execution success
  alone. Prove crash, retry, ordering and corruption scenarios in the sandbox.
- Manage tasks, threads, processes, connections and applicable server-side
  parallelism automatically under shared resource budgets. Consider all relevant
  available source/target, local, network, workload, cost and service-limit facts.
  Scale up/down, balance skew, apply backpressure, checkpoint valid evidence and
  reconcile actual state before retrying. Automation does not expand authorized
  scope or authorize infrastructure provisioning or destructive actions.
- Assess and handle hazards across every task and engine, not only known timeout
  cases. Prevent, detect, contain and recover through shared decisions and
  enforced execution controls. Bound retries and resource growth; track owned
  operations and reconcile unknown effects before repetition. Protect dependent
  work and required evidence. Unknown failures must lead to a conservative safe
  state and an actionable blocker, never an assumed success. Maintain a failure
  catalogue with sandbox fault-injection proof, including controller failures.
- Automatically tune applicable execution dimensions from measured limits and
  failure causes across every task and engine. Retrying identical work is not
  recovery from a bad setting. Select compatible safe adjustments or alternative
  paths, reconcile partial effects and verify integrity after tuning. LOB/payload
  handling must preserve complete values; sampled maxima cannot certify an exact
  safety bound. Respect ownership and authorization for external settings.
- Require minimal irreducible user input. migkit owns discovery, planning,
  execution and recovery; normal operation must not require custom scripts or
  algorithm/worker tuning. Explain scope, progress, remaining work, ETA bounds,
  affected databases and concrete problems concisely, with detail on demand.
- Prove integrated behavior through sandbox end-to-end scenarios and measured
  comparisons, including skew, large values, changing resources, concurrency,
  failures and repeated execution. Cover engine-specific and cross-engine
  semantics. Method existence, simulated chooser tests or capability registration
  alone do not prove workflow support or superiority. Mark missing/unproved work
  explicitly and carry it in the shared backlog and capability/depth matrices.

Product telemetry must describe migkit behavior through a reviewed typed
allowlist, never export customer database content, schema/identifiers, SQL,
value-derived hashes, credentials or raw diagnostics. Keep sensitive evidence
local. Require explicit disclosure, effective disable/no-egress controls,
bounded overhead and privacy tests across normal, error and recovery paths.
Storage remains undecided; planning does not authorize transmission.

Product target: users supply minimal necessary intent and access; migkit owns
technical decisions and the complete verified migration workflow. Match the
applicable mechanisms of established products and prove improvements in depth,
recovery, correctness, performance and operator effort through comparable
end-to-end evidence. Do not claim superiority from architecture or feature
presence alone. Do not eliminate necessary business intent or authorization
by guessing; disclose unsupported outcomes safely.

Implementation acceptance is detailed in `docs/backlog.md`: P0-DECISION-ALL,
P0-WORKFLOW-DEPTH, P0-MINIMAL-INPUT, P0-AUTO-RESOURCES, P0-VALIDATE and the
platform-wide engine contract. This rule is mandatory design and review
criteria; it does not claim that current implementation meets them.

## Tests

Write tests with every change; run them once, at the end (`tools/final_suite.py`). Mid-way, run only `python -m compileall -q migkit tests` and the guards: `tests/test_the_report_does_not_name_its_tools.py`, `tests/test_no_method_is_said_twice.py`, `tests/test_the_code_map_is_current.py`, `tests/test_every_engine_is_as_deep_as_postgres_and_mysql.py` (the depth matrix: a way you add needs a test naming it in its module's `PROVES`, or an `UNPROVED` entry; a gap you close loses its declaration in `capabilities.DEPTH_GAPS`), and `tests/test_the_scope_reads_alike_everywhere.py::test_a_listed_table_is_asked_of_the_scope_one_way`. Never connect to a real database: the docker sandbox only.

## The public repo

- No organisation's own names, hosts, addresses, accounts or credentials: the pre-commit hook checks (`tools/check_no_secrets.py`, with the local deny-list); use `example.com`, `10.0.0.x`, `CHANGE_ME`.
- User-facing text never names a wrapped program.
- Commits: one line, no trailers. Never push - the owner pushes.
- The manual (`tools/gen_docs.py`) and the code map (`tools/gen_codemap.py`) are written from the code; when the hook stops a commit for them, add the written files and commit again.
