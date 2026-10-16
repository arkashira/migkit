# R21.9 next-step audit — 2026-10-15

Source baseline: `3c33f9a1c6ad382b35b95d33a280514791f0cebc` on `ext-nextstep`. Audit completed on 2026-10-16. This is an audit of operator text, not an implementation of R21.9.

## Scope and counting

Inspected all five requested modules: `migkit/cli.py`, `migkit/report.py`, `migkit/guide.py`, `migkit/doctor.py` and `migkit/verdict.py`. The backlog requirement is at `docs/backlog.md:8259-8260`; the index still labels it R21.9, row 402. The older index line reference no longer points at that paragraph.

Each inventory row is a source message site or a closely related finite text family. Locations are 1-based at this baseline. The text column retains literal wording or its source expression; `{...}` is a runtime substitution, not observed output. Long HTML is represented by its visible wording, and markup/formatting is omitted in manually transcribed families. A family may list several alternatives; it counts once, not once per runtime table, database, exception or branch. Returned text producers, direct callback registrations and their generic output sinks are listed and counted as separate audit sites, not claimed as unique rendered messages.

Counts are grouped by command surface. Shared handlers and verdicts have separate groups instead of being multiplied across every caller. `move` includes replication, seeding and cutover helpers; `watch / sync --serve` covers shared monitoring. `check / schema --drill` contains the drill helper reached by check. The standalone diagnosis API in doctor.py is inspected separately: cli.py does not directly call doctor.lines at this baseline, so its CLI reachability is not claimed.

**yes**: the message supplies the next command, statement, answer or concrete safe action. **partly**: it identifies a log, option, requirement or adjacent instruction, but omits a necessary next step or depends on runtime wording. **no**: it states a result/symptom without an action. Progress, headings and success messages are included: many legitimately need only “wait” or “continue,” but do not currently say that. A no on a heading is not ranked as severely as a blocked recovery. Actions in the next-step column are audit recommendations, not authorization to write. `HOP`, `DATABASE`, `STATE` and brace fields must be filled from the current command context.

Calls forwarding messages from other modules are listed as dynamic boundaries, with conservative judgments. This audit does not claim to enumerate every possible string produced in those other modules, external exceptions, SQL statements or generated diagnostics. CLI option help/docstrings and CSS/HTML scaffolding are reference/formatting rather than operational messages; prompts, emitted tables, report findings, JSON verdict wording and explicit exception messages are included.

Surrogate grounding: find.py and the index, search, knowledge, memory and incident endpoints were queried before project reads. No indexed references matched this worktree; the memory endpoint rejected the read or was unavailable. The requested paths and current CODEMAP supplied the bounded source locations. No implementation was delegated.

## Thirty highest-value fixes first

These stop a workflow, conceal missing proof, or invite an unsafe retry. Order favors cutover/rollback safety, then recoverability and first-run blockers. Proposed wording names the product, engine concepts and actions without exposing wrapped component names. It is copy guidance only; placeholders need current state and paths. No new recovery capability is implied. A dry-run is a review step, not a guarantee that every probe is side-effect free.

| Priority | Source | Current message | Why this comes first | Proposed wording |
|---|---|---|---|---|
| 1 | `migkit/cli.py:5008` | {d}: the change tail did not end | A cutover stop failed; no safe recovery is given. | Cutover stopped because the change tail did not end for {db}. Do not advance the cutover or release an existing write freeze. Inspect this hop's tail status and saved run log, resolve the stop failure, then rerun the cutover plan before approving another attempt. |
| 2 | `migkit/cli.py:5032` | {d}: the change tail did not pause | A failed pause leaves cutover coordination unresolved. | Cutover stopped because the change tail did not pause for {db}. Keep the cutover blocked. Inspect tail progress and the saved run log, resolve the pause failure, then rerun the cutover plan. |
| 3 | `migkit/cli.py:4721` | a program an earlier move of {d} started is still running (process {left[0][0]}) and writing; a move beside it would empty the tables under it | A retry can empty tables under an old writer. | An earlier move is still writing to {db} (process {pid}). Do not start another move. Inspect the saved run log and have the owner finish or safely stop that process. Confirm it has stopped, then rerun the original move command. |
| 4 | `migkit/cli.py:244` | the move's checkpoint kept changing under migkit while it saved it | Checkpoint contention stops execution without a recovery route. | The shared checkpoint kept changing while it was saved. Check which machines still own this move and reconcile their state before retrying. Keep the checkpoint; once contention is resolved, rerun the original move command. |
| 5 | `migkit/cli.py:2310` | {db}: {what} did not pause within {window}s (hop option repair_window), so nothing was applied. It has been told to carry on. | Repair coordination timed out; increasing a timeout alone is unsafe. | The writer did not pause within {window}s; no repair was applied and it was asked to resume. Confirm its current state, inspect why the pause failed, and resolve coordination before rerunning the repair plan. |
| 6 | `migkit/cli.py:6019` | state {ts} not retrievable from {store.kind} | Rollback cannot retrieve the selected restore point. | Saved state {ts} could not be retrieved from {backend}. Check access to that backend and confirm the saved artifacts still exist. Restore retrieval, then rerun the rollback plan for this state; do not apply a different state by guess. |
| 7 | `migkit/cli.py:5893` | [red]the record has been changed: {_esc(broken)}[/red] | An altered audit record lacks an investigation path. | The audit record has changed at {entry}. Preserve this record and the saved run logs. Investigate the integrity failure before relying on this history for rollback or audit; do not rewrite the record to clear this warning. |
| 8 | `migkit/cli.py:6011` | no saved states, run migkit sync --go first | Current advice asks for a new write just to obtain rollback state. | There is no saved counter state to restore. Run &#96;migkit history {hop}&#96; and confirm the intended recovery evidence. If no suitable state exists, stop this rollback and obtain an approved recovery plan. Save a restore point before future repairs. |
| 9 | `migkit/cli.py:1442` | {len(failed)} could not be created on the target + {'; nothing else was either - the schema is created in one transaction, whole or not at all' if whole else ''} + : + {'\n '.join(failed)} | Target schema creation failed; transactional and partial cases differ. | {count} target statements failed: {failures}. Review the failed statements and target permissions. {transaction_or_partial_state}. Reconcile any created objects, then rerun &#96;migkit schema {hop} --convert&#96; to review the remaining plan before applying it. |
| 10 | `migkit/cli.py:361` | {hop.name}: {d}: the hop asks to mask + {'; '.join((f'{n} ({why})' for (n, why) in refused))} + . Nothing has been started | Masking incompatibility stops a run without a supported resolution. | The named columns cannot be masked safely for this engine: {columns_and_reasons}. Nothing was started. Correct the masking/type mapping and rerun the move plan; keep the move blocked until every masked column is supported. |
| 11 | `migkit/cli.py:3617` | topology: two_way - {why}. Nothing was set up. | Two-way replication refuses a topology without a next step. | Two-way replication cannot start: {reason}. Nothing was set up. Resolve the named loop-prevention or topology prerequisite, then run &#96;migkit doctor&#96; and review the move plan again before authorizing replication. |
| 12 | `migkit/cli.py:3783` | {said} | A dynamic replication refusal may have no concrete action. | Replication was not set up: {reason}. Resolve the named scope, mapping or server prerequisite. Run &#96;migkit doctor&#96;, then rerun the same move command without &#96;--go&#96; to review the plan before executing it. |
| 13 | `migkit/cli.py:3060` | {d}: not started - {short} | A pre-copy refusal leaves the operator dependent on code. | {db}: copy not started: {reason}. Resolve the named prerequisite, run &#96;migkit doctor&#96;, then rerun the same move command without &#96;--go&#96; to review the copy plan. |
| 14 | `migkit/cli.py:2395` | {db}: {said} | A repair failure can leave uncertain partial effects. | {db}: repair stopped: {reason}. Read the saved repair journal and compare the target with the recorded applied statements before retrying. Resolve uncertain effects, then rerun the repair plan without &#96;--apply&#96; or &#96;--go&#96;. |
| 15 | `migkit/cli.py:1995` | [yellow]the source lists no database this hop covers, so nothing is compared - the user migkit signs in as may not see them[/yellow] | An empty visible scope can look like a successful no-op. | No source database visible to this login matches the hop. Check the configured database names and include/exclude rules, and ask the database owner to confirm listing/read permissions. Run &#96;migkit doctor&#96;, then &#96;migkit check {hop}&#96;. |
| 16 | `migkit/verdict.py:240` | nothing was compared: no answer at all | No answers means no verification; no recovery is supplied. | Nothing was compared. Run &#96;migkit doctor&#96;, check that the hop selects visible databases and tables, then run &#96;migkit check {hop}&#96;. Do not treat this run as verified. |
| 17 | `migkit/verdict.py:256` | every answer was skipped - nothing was compared: {names} | All checks skipped can strand a new operator. | Every answer was skipped: {names}. Read each skip reason in the report, resolve its missing prerequisite or unsupported comparison, then run &#96;migkit check {hop}&#96; again. Required skipped checks remain unproved. |
| 18 | `migkit/verdict.py:252` | {n} answers were reused from an earlier run and the source may have changed since: {names} | Reused answers need a fresh-run command. | These answers were reused and the source may have changed: {names}. Run &#96;migkit check {hop}&#96; without &#96;--resume&#96; to take fresh answers. |
| 19 | `migkit/verdict.py:259` | the run was narrowed to {coverage} | Narrowed evidence needs an exact full-hop route. | This run covered only {coverage}. To verify the whole configured hop, run &#96;migkit check {hop}&#96; without &#96;--db&#96;, &#96;--table&#96;, &#96;--only&#96; or extra &#96;--exclude&#96; narrowing. Review the hop's configured scope in the report. |
| 20 | `migkit/verdict.py:229` | {n} answers could not be taken: {names}; an answer migkit has no word for ({statuses}) is taken as not verified | Error/unknown-status verdicts name symptoms, not recovery. | Verification could not finish: {names}. Open the named findings in the report, resolve their connection or check errors, then run &#96;migkit check {hop}&#96;. For an unknown status, preserve the report and saved run log for investigation. |
| 21 | `migkit/cli.py:1111` | [red]{path} is not YAML migkit can read[/red]: {e} | Configuration parse errors need a location and retry command. | The configuration at {path} is not valid YAML: {reason}. Correct the reported location while preserving existing hops, then run &#96;migkit doctor&#96; again. |
| 22 | `migkit/guide.py:256` | no {side} address migkit could read | Address parsing exhausted retries without a recovery example. | The {side} address could not be read. Set {address_variable} to a supported database address, or run &#96;migkit init&#96; in a terminal and enter it again. |
| 23 | `migkit/guide.py:298` | [red]could not reach the source[/red]: {dba._last(e)} | Source discovery fails without a connectivity checklist or retry. | Could not reach the source: {reason}. Check the source address, network route, login and read permissions, then rerun &#96;migkit init&#96;. Discovery has not verified the source. |
| 24 | `migkit/cli.py:1016` | {_esc(name)} {side}: [red]FAIL[/red] {_esc(_clean(e, (ep.password,), raw=True))} | A side connection failure is raw error text. | {hop} {side}: connection failed: {reason}. Check this side's endpoint, network route, credentials and read permissions, then run &#96;migkit doctor&#96; again. |
| 25 | `migkit/cli.py:1172` | {name} {n.side}: {_esc(n.what)} - could not be asked ({_esc(n.now)}) | A failed permission probe gives no path to trustworthy readiness. | {hop} {side}: could not check {requirement}: {reason}. Ask the database owner to resolve the probe error or required permission, then rerun &#96;migkit doctor&#96; to regenerate the prerequisites. |
| 26 | `migkit/cli.py:1213` | FAIL connection NOT {verified/encrypted}: {detail} | A transport failure needs an actionable safe configuration route. | The {side} connection is not {encrypted_or_verified}: {reason}. Configure TLS and certificate verification for this endpoint, or the supported protected tunnel. Run &#96;migkit doctor&#96; again and resolve every transport failure before moving data. |
| 27 | `migkit/cli.py:1296` | link: its port does not answer ({got['tcp']}) | An unanswered port has no concrete next step. | The {side} port does not answer: {reason}. Check the configured host and port, network route, firewall and database listener, then rerun &#96;migkit doctor&#96;. |
| 28 | `migkit/cli.py:5330` | [yellow]the hop could not be read again ({_esc(_clean(e, raw=True))}); this cycle goes by the one read before[/yellow] | A long-running cycle continues with stale configuration. | This cycle could not reload the hop: {reason}; it is using the previous configuration. Correct the file or read access and confirm a later cycle reloads it. Do not approve a scope change or cutover from this stale cycle. |
| 29 | `migkit/verdict.py:662` | {table} partition {n}: missing on target, the source's holds {rows} / empty on the source | Missing target partitions have no repair planning route. | {table} partition {partition} is missing on the target ({rows}). Run &#96;migkit schema {hop}&#96; to review the supported schema plan for this engine, resolve the partition definition with the database owner, then verify the affected rows with &#96;migkit check {hop}&#96;. |
| 30 | `migkit/cli.py:1572` | {table} has no primary key for joining | Drilldown cannot join a keyless table; no supported fallback is named. | {table} has no primary key for row joining, so this drilldown cannot run. Run &#96;migkit check {hop} --db {db} --only data&#96; to request the engine's complete data comparison. If that comparison is unsupported, keep the table unproved and review a schema/key plan with the database owner. |

## Count by command

| Command surface | Sites/families | yes | partly | no |
|---|---:|---:|---:|---:|
| shared command handling | 32 | 5 | 5 | 22 |
| init / interactive prompts | 12 | 6 | 3 | 3 |
| init (guided setup) | 52 | 14 | 11 | 27 |
| doctor | 45 | 5 | 5 | 35 |
| doctor (diagnosis API) | 6 | 0 | 0 | 6 |
| advise | 2 | 0 | 0 | 2 |
| assess | 6 | 0 | 0 | 6 |
| schema | 22 | 4 | 2 | 16 |
| check / schema --drill | 6 | 2 | 0 | 4 |
| check | 35 | 6 | 1 | 28 |
| sync | 43 | 11 | 5 | 27 |
| move (copy, stream and cutover) | 186 | 32 | 34 | 120 |
| watch / sync --serve | 17 | 1 | 4 | 12 |
| report | 9 | 4 | 1 | 4 |
| report / check (HTML) | 18 | 2 | 4 | 12 |
| history | 10 | 0 | 0 | 10 |
| rollback | 17 | 1 | 5 | 11 |
| check / watch / report (shared verdict) | 55 | 2 | 10 | 43 |
| **Total** | **573** | **95** | **90** | **388** |

The priority table repeats 30 inventory entries; it is excluded from these totals. Counts measure audited source sites/families, not frequency or number of unique sentences.

## Message inventory

### shared command handling — 32 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:59` · _lock | [yellow]took the hop over from {was.get('host')} process {was.get('pid')}, whose lease had lapsed: it stopped without letting go[/yellow] | Allow the resumed move to reconcile saved work; monitor it and run migkit check HOP when it completes. | no |
| `migkit/cli.py:209` · _encoded | the move's checkpoint {self.path} kept changing while it was written; nothing past the last save is lost, and the next run goes on from it | Confirm the previous writer has stopped, then rerun the same move command to resume its last saved checkpoint. | partly |
| `migkit/cli.py:244` · _merge_into_remote | the move's checkpoint kept changing under migkit while it saved it | Confirm which machines still own the move, reconcile their state, then retry once checkpoint contention is resolved. | no |
| `migkit/cli.py:284` · _sharing | share_tables needs the s3 state backend (options.state.backend: s3): the machines sharing a move find each other's tables there | Set options.state.backend to s3 and configure shared state access consistently before retrying a shared move. | yes |
| `migkit/cli.py:334` · _interlock | {_esc(m)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:348` · _refuse_unmasked | {hop.name} masks columns on the way, and this engine's change stream would carry their real values into the target: a hop that masks is copied here (--mode full), not followed. Nothing has been started | Use the full-copy mode printed here; do not start an unmasked stream for this hop. | yes |
| `migkit/cli.py:354` · _refuse_unmasked | {hop.name} masks columns on the way, and a repair of rows on this engine writes the source's values as they are, unmasked: --kind sequences or --kind schema repairs the rest. Nothing has been started | Repair only the supported sequences/schema kinds shown here; keep masked row repair blocked. | yes |
| `migkit/cli.py:361` · _refuse_unmasked | {hop.name}: {d}: the hop asks to mask + {'; '.join((f'{n} ({why})' for (n, why) in refused))} + . Nothing has been started | Correct the named masking/type incompatibility and rerun the move plan before copying. | no |
| `migkit/cli.py:393` · _guard_write.stop | {said} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:394` · _guard_write.stop | with --go this stops: {_esc(said)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:419` · _guard_write | {m} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:425` · _guard_write | with --go this stops: {_esc(i)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:431` · _guard_write | {_esc(m)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:434` · _guard_write | with --go this waits for the {step} approval the hop asks for + {f' ({_esc(d)})' if d else ''} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:477` · _after_copy | {_esc(m)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:478` · _after_copy | {callback text from charsets.after_copy} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:479` · _after_copy | {callback text from tableengine.in_place} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:480` · _after_copy | {line} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:512` · _keep_out | {_esc(m)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:519` · _keep_out | {d}: every table this copy writes holds rows the hop keeps as they are; nothing to copy | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:530` · _require_configured | {hop.name}: {' and '.join(missing)} not configured yet fill host/user/password in conf/hops.yaml, then: migkit doctor | Fill the missing endpoint fields, then run migkit doctor. | yes |
| `migkit/cli.py:562` · invoke | [yellow]{_esc(m)}[/yellow] | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:572` · invoke | {code} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:658` · _stopped | [yellow]stopped - what was saved as it went is kept, and the next run goes on from it[/yellow] | Confirm the previous run has ended, then rerun the original command to resume. | partly |
| `migkit/cli.py:670` · _stopped | {said} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:692` · _stopped | {ctx.info_name} stopped: + {(said or type(e).__name__)[:600]} + {f'\n(the details are in {where})' if where else ''} | Read the saved debug log, resolve the error and reconcile partial effects before retrying. | partly |
| `migkit/cli.py:743` · _env_at_start | migkit: {line} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:751` · _env_at_start | {str(e)} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:787` · _hops_table | [yellow]{e}[/yellow] | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |
| `migkit/cli.py:790` · _hops_table | [yellow]no hops configured yet - migkit init[/yellow] | Run migkit init to configure the first hop. | yes |
| `migkit/cli.py:795` · _hops_table | {name} / {engine} / {source host} / {target host or (not set)} / {service} / {databases or auto} | Fill any missing target endpoint and run migkit doctor before checking or moving. | no |
| `migkit/cli.py:798` · _hops_table | {t} | Read the named blocker and run diagnostics with migkit doctor; reconcile saved state and any active writer before retrying the original command. | no |

### init / interactive prompts — 12 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:851` · init | {callback text from guide.run} | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:853` · init | migkit finds this file automatically. Override with MIGKIT_CONF=/path/to/hops.yaml | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. | no |
| `migkit/cli.py:857` · init | {path} already exists (use --force to overwrite) | Back up and edit the existing file, or intentionally overwrite it with migkit init --force. | partly |
| `migkit/cli.py:861` · init | wrote {path} (mode 600) | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. | no |
| `migkit/cli.py:862` · init | edit the endpoints, then: | Edit endpoint fields, then run the doctor and check commands printed below. | partly |
| `migkit/cli.py:863` · init | migkit doctor # capabilities + connectivity | Run migkit doctor after editing the endpoints. | yes |
| `migkit/cli.py:864` · init | migkit check my-hop # read-only comparison | Run the shown read-only comparison after doctor succeeds. | yes |
| `migkit/cli.py:866` · init | migkit finds this file automatically. Override with MIGKIT_CONF=/path/to/hops.yaml | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. | no |
| `migkit/cli.py:875` · _ask | {question} | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. | yes |
| `migkit/cli.py:881` · _ask_change | {question} | Finish the requested input, edit the configured endpoints, then run migkit doctor and migkit check HOP. | yes |
| `migkit/cli.py:891` · _ask_terms | [bold]{program}[/bold] is not open source: its terms are {terms.licence}. read them at {terms.url} migkit uses it for {terms.allows}. declined: {terms.otherwise}. | Read the displayed terms and decide whether to accept or decline. | yes |
| `migkit/cli.py:895` · _ask_terms | accept the terms of {program}? | Answer yes only if accepting the displayed license terms. | yes |

### init (guided setup) — 52 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/guide.py:40` · ASK_SOURCE | Where is the data now? An address like postgres://app@db1:5432/shop, or a task file from another tool | Enter the source address or task file. | yes |
| `migkit/guide.py:43` · ASK_TARGET | Where should it go? An address like mysql://app@db2/shop | Enter the target address. | yes |
| `migkit/guide.py:44` · ASK_KEEP | Keep the passwords in this machine's keychain (k), or read them from environment variables (e)? | Enter k or e according to the intended credential store. | yes |
| `migkit/guide.py:122` · run | {ASK_SOURCE} | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | yes |
| `migkit/guide.py:132` · run | [yellow]{e}[/yellow] | Correct the source input at the repeated prompt; in noninteractive use correct MIGKIT_SOURCE. | partly |
| `migkit/guide.py:136` · run | read {src} as hop {imported.name} | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | no |
| `migkit/guide.py:138` · run | - {note} | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | no |
| `migkit/guide.py:154` · run | {path} already has a hop {name} going somewhere else (MIGKIT_HOP=<another name> adds this one beside it) | Set MIGKIT_HOP to a different name and rerun migkit init to preserve the existing hop. | yes |
| `migkit/guide.py:158` · run | {path} has a hop {name} between these two already: it is updated in place | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | no |
| `migkit/guide.py:171` · run | Password for {who} (not shown; Enter if it signs in without one) | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | yes |
| `migkit/guide.py:178` · run | {ASK_KEEP} | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | yes |
| `migkit/guide.py:184` · run | this machine has no keychain migkit can use; keep the passwords in environment variables instead (MIGKIT_SECRETS=env) | Set MIGKIT_SECRETS=env, provide the password variables and rerun migkit init. | yes |
| `migkit/guide.py:205` · run | passwords: {said} | For environment-backed secrets, set the listed variables in every execution environment before doctor/check. | partly |
| `migkit/guide.py:212` · run | the task took {len(found['tables']) - len(left)} of the source's {len(found['tables'])} tables; left out: {', '.join(left[:8])}{' ...' if len(left) > 8 else ''} | Complete the setup prompts, review the discovered scope and plan, then run the goal-specific command printed at the end. | no |
| `migkit/guide.py:217` · run | MIGKIT_GOAL={goal}: one of {', '.join(hopkeys.GOALS)} | Set MIGKIT_GOAL to one of the printed values and rerun migkit init. | yes |
| `migkit/guide.py:228` · run | wrote hop {name} to {path} ({lines} lines) | Review the saved hop and run the goal-specific next command printed immediately below. | partly |
| `migkit/guide.py:230` · run | {step.format(hop=name)} | Run the printed goal-specific next command after prerequisites and authorization are satisfied. | yes |
| `migkit/guide.py:232` · run | migkit doctor - asks again once the DBA has run the script | After the DBA applies the approved script, run migkit doctor. | yes |
| `migkit/guide.py:243` · _address | {text} | Correct the named source/target address, set MIGKIT_SOURCE or MIGKIT_TARGET, and rerun migkit init. | yes |
| `migkit/guide.py:245` · _address | no {side} address: set MIGKIT_{side.upper()} or run &#96;migkit init&#96; in a terminal | Set the named address variable or run migkit init interactively. | yes |
| `migkit/guide.py:254` · _address | [yellow]{e}[/yellow] | Correct the target/source address at the repeated prompt. | partly |
| `migkit/guide.py:256` · _address | no {side} address migkit could read | Correct MIGKIT_SOURCE or MIGKIT_TARGET for the named side and rerun migkit init. | no |
| `migkit/guide.py:298` · survey | [red]could not reach the source[/red]: {dba._last(e)} | Check source address, route, credentials and permissions; rerun migkit init once it is reachable. | no |
| `migkit/guide.py:480` · _say_found | [bold]what is there[/bold] | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:484` · _say_found | {side}: [red]could not be asked[/red] - {got['error']} | Resolve the named side probe error and rerun setup/doctor before trusting discovery. | no |
| `migkit/guide.py:490` · _say_found | {side}: {software}, {where}, {role} | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:492` · _say_found | {out['count']} tables in {', '.join(map(str, out.get('dbs') or []))}: about {out['rows']} rows{_size_words(out)} | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:497` · _say_found | {len(keyless)} without a key ({', '.join(keyless[:5])}{' ...' if len(keyless) > 5 else ''}) - copied whole and compared whole; a change to one row cannot be told apart from another | Review whether a stable key can be added through an approved schema change, or use a complete full-copy/full-comparison path. | partly |
| `migkit/guide.py:505` · _say_found | {len(care)} columns need care across the two engines{part}: | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:508` · _say_found | {table}.{col} {declared}: {why} | Review the named target type mapping and preserve source values before loading. | no |
| `migkit/guide.py:510` · _say_found | every column has a type on the target that keeps its values{part} | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:516` · _say_found | the source's changes can + {'be followed' if not off else 'not be followed yet: ' + '; '.join((n.now for n in off))} | Have the DBA review the capture prerequisites/script and rerun migkit doctor. | partly |
| `migkit/guide.py:522` · _say_found | {without} | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:526` · _say_found | the users lack: + {'; '.join((f'{n.side} - {n.what} ({n.now})' for n in grants[:6]))} | Have the DBA apply approved missing grants from the generated script, then run migkit doctor. | partly |
| `migkit/guide.py:529` · _say_found | the users may read and write what migkit needs | Review this discovery result before accepting the plan; resolve any named key, type, capture or permission gap first. | no |
| `migkit/guide.py:564` · plan | [bold]the plan[/bold]: {goal} - {hopkeys.GOALS[goal]['what']} | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:569` · plan | {line} | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:574` · plan | carries {carried['count']} tables, about {rows} rows + {f" and {human_bytes(size)}; the indexes ({human_bytes(carried['index_bytes'])}) are built on the target" if size is not None else ''} | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:583` · plan | {line} | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:585` · plan | time: {measured} | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:588` · plan | time: at least {_took(secs)} - {rate} MiB/s is what the slowest link measured carried; the first copy measures the real rate and every plan after it uses that | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:593` · plan | time: no rate measured yet; the first copy measures it and every plan after it uses that | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:596` · plan | compares {len(found.get('tables') or [])} tables, about {int(found.get('rows') or 0)} rows | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:608` · plan | what the users may do could not be asked: {dba._last(err)} | Resolve the permission probe error, then rerun migkit doctor to regenerate requirements. | no |
| `migkit/guide.py:614` · plan | before it can: {len(lack)} thing(s) the DBA must do - | Have the DBA review the following side-specific script and apply approved prerequisites. | partly |
| `migkit/guide.py:616` · plan | {n.side}: {n.what} ({n.now}) | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:618` · plan | the {side}'s script, for the DBA: {p} | Have the DBA review/run the named script, then rerun migkit doctor. | yes |
| `migkit/guide.py:620` · plan | {n.side}: {n.what} - could not be asked ({n.now}) | Resolve the named probe error and rerun migkit doctor before moving. | no |
| `migkit/guide.py:622` · plan | nothing for the DBA to do first | Review the plan and any DBA script, arrange the required changes, then rerun migkit doctor before moving. | no |
| `migkit/guide.py:700` · _existing | {p} is not YAML migkit can read; use --force to write a new one | Back up and repair the existing YAML; use --force only if intentionally replacing the file. | partly |
| `migkit/guide.py:703` · _existing | {p} holds more than hops; add the hop by hand, or use --force to write a new file | Back up the existing file and add the hop without discarding other content; replace only intentionally. | partly |
| `migkit/guide.py:820` · _write | the hop could not be added to {p} as it is written ({type(e).__name__}); nothing was changed - use --force to write a new file | Back up and repair the existing file structure; use --force only for an intentional full replacement. | partly |

### doctor — 45 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:920` · doctor | diagnostics: {bundle} ({len(got)} files; passwords, tokens, addresses' credentials and every finding's values left out) | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:925` · doctor | configuration: {_esc(str(_config.CONF))} reports and run state: {_esc(str(_config.REPORTS))} | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:932` · doctor | {m} | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:940` · doctor | {capability} / {what it does} / ready, reduced or unavailable | For missing capabilities, follow the adjacent install instruction; rerun migkit doctor afterward. | partly |
| `migkit/cli.py:943` · doctor | {t} | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:946` · doctor | note: {said} | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:948` · doctor | to enable everything on this machine: [bold]migkit doctor --install[/bold] | Run migkit doctor --install, then rerun migkit doctor. | yes |
| `migkit/cli.py:954` · doctor | it cannot install these here; they need installing by hand: {', '.join(manual)} | Obtain an approved installation procedure for the missing capability, then rerun migkit doctor. | partly |
| `migkit/cli.py:971` · doctor | {line} | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:1001` · _doctor_side | {_esc(name)} {side}: [green]ok[/green] ({_esc(ep.host)}, {note}){role} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1006` · _doctor_side | {said} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1010` · _doctor_side | {said} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1013` · _doctor_side | {_esc(said)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1016` · _doctor_side | {_esc(name)} {side}: [red]FAIL[/red] {_esc(_clean(e, (ep.password,), raw=True))} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1027` · _doctor_hop | {_esc(name)}: [yellow]{_esc(line)}[/yellow] | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1031` · _doctor_hop | {_esc(name)}: [red]FAIL[/red] {_esc(_clean(e, (hop.source.password, hop.target.password), raw=True))} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1047` · _doctor_hop | {_esc(name)}: {said} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1053` · _doctor_hop | {_esc(name)}: what the servers allow could not be asked - {_esc(_clean(why, raw=True)[:160])} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1062` · _doctor_hop | {_esc(name)}: {_esc(line)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1070` · _doctor_hop | {_esc(name)}: not on {_esc(hop.engine)} hops - + {'; '.join(cannot)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1073` · _doctor_hop | {_esc(name)}: {_esc(line)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1078` · _doctor_hop | {_esc(name)} [yellow]cutover[/yellow]: {_esc(line)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1080` · _doctor_hop | {_esc(name)} [yellow]source[/yellow]: {_esc(line)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1085` · _doctor_hop | {_esc(name)}: {_esc(what)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1091` · _doctor_hop | {_esc(name)} dst: {_esc(line)}; tearing the stream down at cutover, or rollback --apply, gives it back | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1111` · _doctor_config | [red]{path} is not YAML migkit can read[/red]: {e} | Correct the YAML in the printed configuration path, then rerun migkit doctor. | no |
| `migkit/cli.py:1118` · _doctor_config | [yellow]{p}[/yellow] | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:1135` · _decisions_said | {work}: the last plan ({e.get('at', '')}) stood on {factbook.said(facts)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/cli.py:1154` · _doctor_setup | {name} {side}: {_esc(wheres[side].label)} (from {_esc(' and '.join(wheres[side].evidence))}) | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:1164` · _doctor_setup | {name}: [green]the servers allow everything goal {goal} needs[/green] | Resolve the named connectivity, permissions, configuration or capability gap, then rerun migkit doctor. | no |
| `migkit/cli.py:1167` · _doctor_setup | {name} {n.side}: [red]missing[/red] {_esc(n.what)} ({_esc(n.now)}) | Have the DBA review the script printed next, apply approved prerequisites and rerun migkit doctor. | partly |
| `migkit/cli.py:1170` · _doctor_setup | what the DBA must run on the {side}: {_esc(p)} | Have the DBA review/run the indicated side-specific script, then rerun migkit doctor. | yes |
| `migkit/cli.py:1172` · _doctor_setup | {name} {n.side}: {_esc(n.what)} - could not be asked ({_esc(n.now)}) | Resolve the probe error/privilege gap for the named side and rerun migkit doctor. | no |
| `migkit/cli.py:1196` · _placement_said | placement: {said} | Review the measured network placement before starting the copy. | no |
| `migkit/cli.py:1213` · _leg_said | FAIL connection NOT {verified/encrypted}: {detail} | Configure encrypted transport and certificate verification for the named side, then rerun migkit doctor. | no |
| `migkit/cli.py:1221` · _leg_said | verified: {pos.detail} | No transport change is required by this result; continue with the remaining doctor findings. | no |
| `migkit/cli.py:1230` · _leg_said | encrypted: {how}; the certificate verified / not verified on: {left} | Enable certificate verification on every named unverified leg, then rerun migkit doctor. | no |
| `migkit/cli.py:1235` · _leg_said | not encrypted by the database; carried through the hop's SSH tunnel to {through}, which is what protects it on the way | Confirm the intended tunnel and continue reviewing the remaining diagnostic results. | no |
| `migkit/cli.py:1239` · _leg_said | not encrypted by the database; carried by the tunnel's own command to {through}, which migkit cannot vouch for. Ask the server for TLS, or carry it through an SSH tunnel | Configure verified TLS or the supported protected tunnel, then rerun migkit doctor. | partly |
| `migkit/cli.py:1243` · _leg_said | not encrypted: every row crosses the network as it is. Ask the server for TLS and verify its certificate (sslmode verify-full, ssl-mode VERIFY_IDENTITY), or carry the connection through a tunnel | Configure verified TLS or a protected tunnel, then rerun migkit doctor. | yes |
| `migkit/cli.py:1259` · _two_way_said | {marks.said(...)} | Review the two-way write marks and outstanding safety requirements before starting replication. | partly |
| `migkit/cli.py:1282` · _link_line | link: STALLED - its port answers, and signing in to the {name} never finishes ... lower that tunnel's MTU (1280 is safe) or clamp TCP MSS on it | Have the network owner correct the packet-size path; rerun migkit doctor to confirm connectivity. | yes |
| `migkit/cli.py:1289` · _link_line | link: STALLED - {requests/replies} of {size} bytes or more never arrive ... lower that tunnel's MTU (1280 is safe) or clamp TCP MSS on it. A copy over this link hangs | Have the network owner correct the packet-size path; rerun migkit doctor. | yes |
| `migkit/cli.py:1296` · _link_line | link: its port does not answer ({got['tcp']}) | Check the named endpoint, network route, firewall and listener, then rerun migkit doctor. | no |
| `migkit/cli.py:1305` · _link_line | link: {rtt_ms} ms round trip, replies up to {replies} bytes and requests up to {requests} bytes arrive, {mb_s} MB/s | Continue reviewing doctor results; this measures transport, not data equality. | no |

### doctor (diagnosis API) — 6 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/doctor.py:108` · diagnose_sides | could not diagnose {side_name}; {exception} | Resolve the reported side-specific probe error and rerun the diagnosis. | no |
| `migkit/doctor.py:133` · lines | leg: {chosen_leg.said} (footprint {chosen_leg.footprint}; leaves {chosen_leg.leaves}) | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/doctor.py:135` · lines | leg: no server-to-server leg holds | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/doctor.py:138` · lines | blocked leg {leg.said}: {'; '.join(leg.blockers)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/doctor.py:143` · lines | {side}: stands on {active.said} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |
| `migkit/doctor.py:146` · lines | blocked {side} rung {s.said}: {'; '.join(s.blockers)} | Resolve the named diagnostic blocker, then rerun migkit doctor; an available rung alone does not prove a completed migration. | no |

### advise — 2 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:1315` · advise | [yellow]{why}; migkit's own route:[/yellow] | Review the hop-specific playbook and follow its first prerequisite before starting the move. | no |
| `migkit/cli.py:1317` · advise | {pb} | Review the hop-specific playbook and follow its first prerequisite before starting the move. | no |

### assess — 6 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:1336` · assess | [yellow]assessed from the configuration alone:[/yellow] {why} | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |
| `migkit/cli.py:1348` · assess | {line} | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |
| `migkit/cli.py:1350` · assess | {line} | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |
| `migkit/cli.py:1357` · assess | by hand: | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |
| `migkit/cli.py:1359` · assess | {i.when}: {i} | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |
| `migkit/cli.py:1361` · assess | {counts['pass']} pass, {counts['warn']} warn, {counts['fail']} fail | Resolve failed readiness items and required manual steps; rerun migkit assess HOP before starting the move. | no |

### schema — 22 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:1373` · _say_goal | [bold]what each side sees[/bold] | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1375` · _say_goal | {mode_line} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1377` · _say_goal | {line} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1385` · _setup_target | target preparation for {hop_name} (dry-run, run these yourself in order): | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1388` · _setup_target | [bold]{_esc(d)}[/bold] | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1390` · _setup_target | {step} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1392` · _setup_target | after this: migkit check --only schema, then start the mover (see migkit advise) | Run migkit check HOP --only schema, then follow migkit advise HOP. | partly |
| `migkit/cli.py:1401` · _convert_schema | --convert is for hetero hops, same-engine hops use migkit schema HOP | Use migkit schema HOP for a same-engine hop. | yes |
| `migkit/cli.py:1410` · _convert_schema | [bold]{_esc(d)}[/bold]: {len(stmts)} statements -> {_esc(out)} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1413` · _convert_schema | {_esc(stmt.splitlines()[0])} ... | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1418` · _convert_schema | [yellow]{_esc(s.splitlines()[0][3:])}[/yellow] | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1423` · _convert_schema | effort: {_esc(cost)} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1436` · _convert_schema | [green]{len(made)} created on the target[/green] | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1439` · _convert_schema | already on the target, left as they are: {_esc(', '.join(kept))} | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1442` · _convert_schema | {len(failed)} could not be created on the target + {'; nothing else was either - the schema is created in one transaction, whole or not at all' if whole else ''} + : + {'\n '.join(failed)} | Inspect each failed target statement, reconcile any partial creation on nontransactional engines and rerun the schema plan. | no |
| `migkit/cli.py:1448` · _convert_schema | dry-run, review the file then add --apply | Review the generated file, then add --apply only for the approved schema conversion. | yes |
| `migkit/cli.py:1493` · _gen_migration | --migration supports postgres and mysql hops | Use --migration only on supported PostgreSQL/MySQL hops; review the supported schema path for this engine. | partly |
| `migkit/cli.py:1503` · _gen_migration | {d}: {e} | Inspect the database-specific schema generation error, resolve its prerequisite and retry generation. | no |
| `migkit/cli.py:1505` · _gen_migration | generating migration DDL needs a component that is not installed on this machine: migkit doctor --install puts it in place | Run migkit doctor --install and retry schema generation. | yes |
| `migkit/cli.py:1510` · _gen_migration | {d}: schemas already in sync, nothing to generate | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1517` · _gen_migration | {d}: [green]{vf}[/green] (+ undo {uf.name}, {len(fwd.splitlines())} lines) | Review the generated schema statements and warnings; apply only the intended changes, then run migkit check HOP --only schema. | no |
| `migkit/cli.py:1520` · _gen_migration | review the files, commit to git, apply when ready | Review and version the forward/undo files before an authorized schema change. | yes |

### check / schema --drill — 6 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:1547` · _drill | --drill needs --db and --table (schema.table) | Supply --db DATABASE and --table SCHEMA.TABLE with --drill. | yes |
| `migkit/cli.py:1552` · _drill | --drill supports postgres and mysql hops | Correct the drill/check inputs or prerequisites and rerun the same read-only command for the intended database and table. | no |
| `migkit/cli.py:1556` · _drill | the row-by-row drilldown needs a component that is not installed here: migkit doctor --install puts it in place | Run migkit doctor --install and retry the drilldown. | yes |
| `migkit/cli.py:1572` · _drill | {table} has no primary key for joining | Choose a keyed table or obtain another complete comparison path; do not add a key to live data without review. | no |
| `migkit/cli.py:1579` · _drill | {line} | Correct the drill/check inputs or prerequisites and rerun the same read-only command for the intended database and table. | no |
| `migkit/cli.py:1585` · _drill | {extra} | Correct the drill/check inputs or prerequisites and rerun the same read-only command for the intended database and table. | no |

### check — 35 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:1643` · _checks_for | --only {', '.join(odd) or only}: not a check of this hop's engine; its checks are {', '.join(allowed)} | Replace --only with checks listed as available for this engine. | yes |
| `migkit/cli.py:1690` · _said_row | [{color}]{_esc(str(r.get('check', '')))} {_esc(r.get('scope', ''))}: {_esc(st.upper())}[/{color}] {_esc(r.get('detail', ''))} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1742` · run_checks.stream.s | {_esc('[' + str(d) + ']')} {_esc(line)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1777` · run_checks.run_db | {_esc('[' + str(d) + ']')} the source's schema changed since the run being resumed ( + {_esc('; '.join(since[:3]))} + {'; ...' if len(since) > 3 else ''} + ), so nothing of it is reused | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1844` · run_checks.run_db | [yellow]{_esc(m)}[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1893` · run_checks | progress {done_jobs}/{total_jobs} ({active} active), elapsed {human_secs(timer.elapsed())}, eta {timer.eta(done_jobs, total_jobs)}{conf} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1899` · run_checks | [yellow]stopping: the checks of the {running} databases in flight end first, and no other starts[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1921` · _collided | the hop lands more than one table on one name on the target ... Give them separate names on the target (mapping.tables, mapping.names) | Give each source table a distinct target name, then rerun the check before moving. | yes |
| `migkit/cli.py:1987` · _check | {hop_name}: {clash}. Nothing has been compared | Correct the named ambiguous mapping/name collision and rerun migkit check HOP. | no |
| `migkit/cli.py:1993` · _check | {_esc(m)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:1995` · _check | [yellow]the source lists no database this hop covers, so nothing is compared - the user migkit signs in as may not see them[/yellow] | Check include/exclude rules and database-list permissions with migkit doctor, then rerun migkit check HOP. | no |
| `migkit/cli.py:2026` · _check | the hop's scope changed since the run being resumed, so its {len(other)} answers taken under the scope it had then are taken again | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2032` · _check | checking {_esc(hop_name)}: {len(dbs)} dbs x {checks}, {workers} workers + {' (counts merged into the checksum pass)' if _merged(eng, checks, table) else ''} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2038` · _check | within the hop's scope: it takes only {len(hop.include)} names + {f' and leaves out {len(hop.exclude)}' if hop.exclude else ''} + (the report lists them) | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2045` · _check | changes carried: {_esc(line)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2069` · _check | {len(other)} answers of the last check were taken under another scope than the hop's now and are not kept beside this run's - a whole check takes them again | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2115` · _check | {_esc(servicecheck.said(theirs))} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2117` · _check | [red]the service missed[/red] {_esc(name)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2119` · _check | [yellow]{_esc(m)}[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2125` · _check | explained by {assist.provider()} - a proposal, not part of the verdict: | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2127` · _check | {said} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2136` · _check | [yellow]{len(kept)} answers hold only what the hop keeps by the changes it does not carry (mapping.ops) - said as kept in the report[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2142` · _check | [yellow]{len(notes)} databases answer the source's reads more slowly on the target - the data is not in question; &#96;performance: gate&#96; in the hop's options makes this stop the check[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2147` · _check | verdict: [{colour}]{decided.status}[/{colour}] + {' (identical to the previous run)' if was_same else ''} + {_esc(verdict_path)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2151` · _check | {_esc(decided.why)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2154` · _check | [yellow]the server was under load: backed off {ld['waits']}x ({ld['waited_seconds']}s), concurrency {ld['concurrency_from']} -> {ld['concurrency_to']}[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2158` · _check | report: {_esc(report_path)} | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2161` · _check | [yellow]{len(stale)} answers were overtaken by a schema change on the source while they were being taken, so they are not a verdict: + {_esc(', '.join(sorted({str(r['scope']) for r in stale})[:6]))} + . Check again once the change has settled.[/yellow] | Wait for source schema changes to settle, then run migkit check HOP again. | yes |
| `migkit/cli.py:2168` · _check | [green]all green ({len(results)} checks, {human_secs(timer.elapsed())})[/green] | Review the report scope; continue only with the separately authorized next migration step. | no |
| `migkit/cli.py:2174` · _check | [yellow]{len(bad)} problems, summary: {_esc(summary_path)}[/yellow] | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2181` · _check | next steps: | Read the named findings in the report, resolve errors or lag, and rerun migkit check HOP before deciding whether a repair is needed. | no |
| `migkit/cli.py:2183` · _check | # data / sequence diffs (re-check first: many are transient replication lag, not real) | Recheck after replication catches up before considering row/sequence repair. | yes |
| `migkit/cli.py:2185` · _check | migkit sync {_esc(hop_name)} --apply # all diffed dbs at once | Review a dry-run repair plan and its scope before executing the displayed write command. | partly |
| `migkit/cli.py:2188` · _check | # schema diffs -> generate reviewable DDL files, then apply them yourself | Generate schema DDL, review it and apply only approved statements. | yes |
| `migkit/cli.py:2190` · _check | migkit schema {_esc(hop_name)} --migration # writes migrations/V*.sql per db | Run the shown schema migration generator and review its files before applying them. | yes |

### sync — 43 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:2205` · _on_conflict | on_conflict: {got} - source-wins or keep-target | Set on_conflict to source-wins or keep-target according to the intended conflict policy. | yes |
| `migkit/cli.py:2247` · _repair | run migkit check first, then sync uses its diffs | Run migkit check HOP before planning sync. | yes |
| `migkit/cli.py:2250` · _repair | nothing to repair (no matching diffs last check) | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2252` · _repair | repairing {len(dbs)} databases with diffs: {_esc(', '.join(dbs))} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2265` · _repair_one | {_esc(db)}: nothing to repair | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2272` · _repair_one | [yellow]an earlier repair stopped after statement {upto} of {planned}: {_esc(repairlog.said(path))}[/yellow] | Read the repair journal, reconcile statements with uncertain effects and review the remaining repair before retrying. | partly |
| `migkit/cli.py:2281` · _repair_one | {_esc(what)} was asked to resume; it carries on from where it paused, on top of the repair | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2296` · _make_room_for_rows | {db}: {', '.join(foreign)} is writing to this target, and a row repair beside it would race it - the row that survives is whichever lands last. Nothing was applied. Stop it, repair, and start it again, or let it catch up and check again. | Stop the competing writer with its owner before repair, or wait for catch-up and recheck. | yes |
| `migkit/cli.py:2305` · _make_room_for_rows | {_esc(what)} is running; asking it to pause for the repair | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2310` · _make_room_for_rows | {db}: {what} did not pause within {window}s (hop option repair_window), so nothing was applied. It has been told to carry on. | Inspect why the writer did not pause; confirm it resumed, resolve coordination and rerun the repair plan. | no |
| `migkit/cli.py:2323` · _lend_console | [yellow]{_esc(m)}[/yellow] | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2334` · _table_copier | {callback text from eng.load_window} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:2349` · _ladder | [yellow]{_esc(m)}[/yellow] | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2358` · _said_outcome | applied - {repairways.said(outcome)} | Read the post-repair proof; resolve any remaining differences and run a fresh check. | partly |
| `migkit/cli.py:2365` · _apply_actions | [bold]{_esc(a.kind)} on {_esc(a.scope)}[/bold] {_esc(a.note)} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2371` · _apply_actions | {s} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2378` · _apply_actions | ... {len(a.statements) - 20} more, every statement in {_esc(str(whole))} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2385` · _apply_actions | {ladder.foreseen()} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2393` · _apply_actions | {_esc(said)} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2395` · _apply_actions | {db}: {said} | Read the saved repair outcome/journal, reconcile partial effects and resolve the named failure before retrying. | no |
| `migkit/cli.py:2400` · _apply_actions | undo saved: {_esc(undo_file)} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2430` · _apply_actions | {_said_outcome(outcome) if outcome else ' [green]applied[/green]'} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2433` · _apply_actions | dry-run only, add --apply to execute | Review the dry-run statements, then add --apply for an authorized repair. | yes |
| `migkit/cli.py:2435` · _apply_actions | re-run migkit check to confirm | Run migkit check HOP to confirm the repair. | yes |
| `migkit/cli.py:2465` · _sync_go | what differs is nothing a repair of {kind} puts right | Read the last check and choose a repair kind that addresses the finding, or resolve it outside sync. | no |
| `migkit/cli.py:2473` · _sync_go | [bold]{_esc(d)}[/bold] | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2481` · _sync_go | state saved before the repair: {store.kind}:{kept[d]} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2486` · _sync_go | state backend: {store.kind} (migkit history / rollback to use it) | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2543` · sync | --kind picks what a repair of the last check's differences puts right; --mode {mode} runs a whole flow and repairs whatever it finds | Choose either --kind for last-check repair or --mode for a flow; omit the conflicting option. | partly |
| `migkit/cli.py:2547` · sync | --tag names the restore point --go takes; without --go nothing is saved to name | Remove --tag from a dry-run, or use it with an authorized --go restore-point repair. | yes |
| `migkit/cli.py:2550` · sync | --interval is the time between the cycles of --serve | Use --interval with --serve, or remove --interval. | yes |
| `migkit/cli.py:2552` · sync | --serve keeps verifying a stream: --mode stream or --mode migrate | Use --serve with --mode stream or --mode migrate. | yes |
| `migkit/cli.py:2555` · sync | --apply repairs; --go repairs with a restore point taken first - one or the other | Choose one repair execution option: --apply or --go. | yes |
| `migkit/cli.py:2560` · sync | {hop_name} is a copy for testing (goal subset): it is taken afresh by migkit move {hop_name} --mode full --go, and proved by migkit check {hop_name}; sync makes a target equal to the whole source. Nothing was written | Use the printed fresh-copy command for a subset, then check that hop. | yes |
| `migkit/cli.py:2569` · sync | {hop_name}'s goal is {goal.GOAL}: its target holds rows this source does not, and sync makes a target equal to the source. It is carried by migkit move {hop_name} --mode full --go and proved by migkit check {hop_name}. Nothing was written | Use the printed goal-specific move command, then check the hop. | yes |
| `migkit/cli.py:2597` · _settle | [bold]repair[/bold]: what the check found different | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2599` · _settle | [bold]verify[/bold]: consistent snapshot | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2619` · _caught_up | {_esc(d)}: whether the target has caught up could not be asked: {_esc(_clean(e, raw=True))} | Resolve lag-probe access/connectivity before trusting target catch-up; rerun migkit watch HOP. | no |
| `migkit/cli.py:2623` · _caught_up | {_esc(d)}: the target has not caught up with the source yet; the check reads it as it is | Wait for catch-up and run a fresh comparison before any repair. | partly |
| `migkit/cli.py:2642` · _orchestrate | [bold]verify[/bold]: read-only, consistent snapshot | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2647` · _orchestrate | [bold]{mode}[/bold]: migkit move {_esc(hop_name)} --mode {move_mode} + {'' if go else ' (dry-run; add --apply/--go to execute)'} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2653` · _orchestrate | with --go: + {'the copy, then the check; what it finds different is repaired and checked again' if mode == 'seed' else 'the copy and its stream, then the check' if mode == 'migrate' else 'the stream, then each cycle verifies what changed'} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |
| `migkit/cli.py:2674` · _orchestrate | {_esc(why)} | Review the repair plan and any saved undo state; after an authorized repair, run migkit check HOP to confirm the result. | no |

### move (copy, stream and cutover) — 186 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:2713` · _table_plan | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:2733` · _table_plan | by table: {planner.lines}; time: {estimate}; size: {size}; {placement and resource plan} | Review per-table scope, resources and guarantees before authorizing execution. | no |
| `migkit/cli.py:2787` · _record_run | {d}: no rate is kept from this copy - {len(narrowed)} tables a row filter narrows are counted whole by the source's estimate | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:2797` · _record_run | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:2820` · _resource_plan | the resources could not be measured: {reason} | Resolve the resource probe error and rerun the move plan before relying on its forecast. | no |
| `migkit/cli.py:2822` · _resource_plan | {resource model lines}; {phase lines}; {joint plan lines} | Review measured limits and unmeasured facts before approving the move. | no |
| `migkit/cli.py:2825` · _resource_plan | memory: NOT ENOUGH - {short} | Resolve the measured memory shortfall or choose a feasible execution placement, then rerun the plan. | no |
| `migkit/cli.py:2840` · _joint_lines | {hopkeys.explain_limits(hop)} | Confirm that the plan fits the intended outage and resource limits. | partly |
| `migkit/cli.py:2850` · _joint_lines | plan: {jointplan.lines(jp)} | Review the estimated schedule, guarantees and limits before execution. | no |
| `migkit/cli.py:2854` · _joint_lines | {decision record line} | Review the recorded assumptions; resolve missing critical facts before execution. | no |
| `migkit/cli.py:2859` · _joint_lines | limits: {said} | Resolve any exceeded hop limit and rerun the plan before execution. | partly |
| `migkit/cli.py:2930` · _copy_routed | {callback text from eng.load_window} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:2962` · _copy_table_by | {callback text from eng.move_table} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:2967` · _copy_table_by | {callback text from pair.load_window} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:2968` · _copy_table_by | {callback text from pair.move_table} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:2970` · _copy_table_by | {callback text from pair.finish_created} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3005` · _joint_for_move | plan: {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3006` · _joint_for_move | {callback text from decide.record} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3057` · _copy_tables | {callback text from resource_model.for_move} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3060` · _copy_tables | {d}: not started - {short} | Read the pre-copy refusal and satisfy the named prerequisite, then rerun the move plan. | no |
| `migkit/cli.py:3061` · _copy_tables | {callback text from sizing.fit} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3104` · _copy_tables | {callback text from movers.builds_beside} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3117` · _copy_tables | {said} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3208` · _copy_tables_in | [yellow]stopping: the {running} tables in flight are let finish, and no other starts[/yellow] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3215` · _copy_tables_in | {said} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3231` · _refuse_if_nothing_arrived | {what} reported success and {d} is still empty on the target: + {', '.join(empty[:6])} + {' ...' if len(empty) > 6 else ''} + . Nothing has been marked as moved - look at the copy's output above, and at the target server's log | Inspect the saved copy output and target log; resolve missing writes and reconcile target state before retrying. | yes |
| `migkit/cli.py:3238` · _refuse_if_nothing_arrived | (this engine cannot confirm the rows landed; &#96;migkit check&#96; is what proves it) | Run migkit check HOP for proof that the rows arrived. | yes |
| `migkit/cli.py:3253` · _held_to_the_source | comparing what arrived with the source | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3258` · _held_to_the_source | the bulk copy finished and {len(bad)} of {d}'s tables do not hold what the source does: + {'; '.join((f'{r.scope}: {r.detail[:120]}' for r in bad[:4]))} + {' ...' if len(bad) > 4 else ''} + . Nothing has been marked as moved. A source written to while it was copied differs by what changed - move with --mode full+cdc for that; otherwise &#96;migkit sync&#96; puts the rows right | For an actively written source, review a full+cdc move; otherwise review the sync plan and verify after repair. | partly |
| `migkit/cli.py:3283` · _stop_if_the_schema_moved | [yellow]{m}[/yellow] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3285` · _stop_if_the_schema_moved | {db}: the source's schema changed while it was being moved - {changes}. The rows moved before and after it may not line up. Bring the target's schema level with the source's, then move again and run migkit check. | Review and align the target schema, rerun the move, then run migkit check HOP. | yes |
| `migkit/cli.py:3310` · _move_full | {_esc(d)}: {len(tables)} tables, {done} already done in checkpoint, chunk {chunk} rows | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3320` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3328` · _move_full | {sch}.{t}: {mark} + {f' - {tableway.outcome(way)}' if way and way.rung and (not st.get('done')) else ''} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3333` · _move_full | time: + {planner.estimate(hop, 'builtin', *_source_rows(eng, d))} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3338` · _move_full | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3343` · _move_full | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3364` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3380` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3384` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3386` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3398` · _move_full | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3404` · _move_full | {settled} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3407` · _move_full | [green]{d}: move complete[/green], run migkit check to verify | Run migkit check HOP to verify the copied target. | yes |
| `migkit/cli.py:3410` · _move_full | dry-run, add --go to copy | Review the copy plan, then add --go for an authorized copy. | yes |
| `migkit/cli.py:3488` · _share_tables | {key}: taken over from {was.get('host')} process {was.get('pid')}, whose lease had lapsed | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3492` · _share_tables | {callback text from eng.load_window} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3501` · _share_tables | {d}: {len(left)} tables are being copied by other machines; waiting for them, and for any whose machine stops renewing its lease | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3523` · _share_ranges | {callback text from shared.store_for} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3546` · _share_ranges | {callback text from pair.load_window} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3547` · _share_ranges | {callback text from pair.move_table} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3552` · _share_ranges | {callback text from claims.whole} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:3558` · _share_ranges | {d}: {len(left)} tables are being copied by other machines; waiting for them, and for any whose machine stops renewing its claim | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3570` · _finisher | {d}: every table is copied; another machine completed the move | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3576` · _finisher | {d}: every table is copied; another machine is completing the move | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3581` · _finisher | {d}: every table is copied; another machine completed the move | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3617` · _replicate | topology: two_way - {why}. Nothing was set up. | Read the two-way safety reason and correct the incompatible topology or prerequisites before planning again. | no |
| `migkit/cli.py:3626` · _replicate | [bold]the other way[/bold] + {' - two-way, as the hop says' if two_way else ' - from the new primary back to the old source, as the hop says (reverse: at_cutover)'} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3639` · _replicate | a row both sides change at once is not reconciled: each takes the other's change over its own, and the two can end holding different values - keep a row's writes on one side at a time, and &#96;migkit check&#96; finds any that differ | Keep writes for a given row on one side at a time and run migkit check HOP to find divergences. | yes |
| `migkit/cli.py:3714` · _replicate_one | the hop maps or masks columns or renames tables (mapping.columns, mask_on_write, mapping.tables), and the server's own replication carries whole rows under the source's names, as the source holds them. Nothing was set up. &#96;migkit move --mode cdc&#96; follows this hop with migkit's own change tail, which applies the mapping. | Review migkit move HOP --mode cdc so the hop mapping/masking remains enforced. | partly |
| `migkit/cli.py:3723` · _replicate_one | {refused}. Nothing was set up. &#96;migkit move --mode cdc&#96; follows this hop with migkit's own change tail, which reads the hop's scope and mapping for every change. | Review migkit move HOP --mode cdc, which applies the hop scope and mapping. | partly |
| `migkit/cli.py:3729` · _replicate_one | {why}. Nothing was set up. + {"&#96;migkit move --mode cdc&#96; follows this hop with migkit's own change tail, which stops on a schema change instead of carrying half of it." if hasattr(eng, 'tail_apply') or _tail_pair(eng) is not None else '&#96;migkit move --mode full&#96; copies what the hop covers, and &#96;migkit sync&#96; brings the target level again before cutover.'} | Review the printed safe alternative for the available engine, then verify before cutover. | partly |
| `migkit/cli.py:3759` · _replicate_one | [bold]{d}[/bold] carried by the replica {'removed' if do_drop else 'set up'} for {dbs[0]} - there is one per server | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3783` · _replicate_one | {said} | Resolve the replication refusal before retrying; inspect the dry-run explanation and current source/target state. | no |
| `migkit/cli.py:3784` · _replicate_one | with --go this stops: {_esc(said)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3787` · _replicate_one | [bold]{d}[/bold] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3799` · _replicate_one | note: {_esc(sql['note'])} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3803` · _replicate_one | {side}: {shown} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3812` · _replicate_one | left for when the source is written again: it is frozen | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3818` · _replicate_one | + {eng.replication_status(d, sql['status'])} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3828` · _replicate_one | dry-run, add --go to execute (monitor lag with: migkit watch) | Review replication SQL, add --go only when authorized, then monitor migkit watch HOP. | yes |
| `migkit/cli.py:3850` · _follow | [bold]{d}[/bold] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3854` · _follow | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3855` · _follow | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3860` · _follow | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3861` · _follow | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3865` · _follow | dry-run, add --go to execute | Review the follow plan and add --go only for the authorized stream. | yes |
| `migkit/cli.py:3867` · _follow | [green]caught up to the end position[/green] - verify: migkit check {hop.name} | Run the printed migkit check command to verify the caught-up target. | yes |
| `migkit/cli.py:3978` · _copy_entered | {_esc(d)}: the hop's scope changed since the copy of {_esc(rec.get('at'))} - {len(names)} tables it takes now were not copied as they are taken now ( + {_esc(', '.join(names[:4]))} + {' ...' if len(names) > 4 else ''} + {'), copied before the stream starts' if go else '); with --go they are copied before the stream starts'} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:3996` · _copy_entered | {callback text from eng.create_missing} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4000` · _copy_entered | {callback text from eng.load_window} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4027` · _tail | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4047` · _tail_held | {db}: starting from the position the copy of {rec['at']} holds, so nothing is applied twice and nothing is missed | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4051` · _tail_held | {db}: starting from where the source's log was before the copy of {rec['at']}, so what changed during it and since is carried too | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4059` · _tail_held | [yellow]{db} was copied by an earlier run that saved no change position. This tail starts from now, so what the source changed between that copy and now is carried by nothing - &#96;migkit check&#96; will show it, or move again with --mode full+cdc, which takes the position first[/yellow] | Review a new full+cdc seed to close the missing-change interval; run migkit check HOP before accepting the target. | partly |
| `migkit/cli.py:4069` · _tail_held | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4083` · _two_way_teardown | [bold]{d}[/bold] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4085` · _two_way_teardown | {d}: the tail is still running; stop it first, then take its marks away | Stop the active tail through its owning process/session, confirm it ended, then retry mark teardown. | partly |
| `migkit/cli.py:4089` · _two_way_teardown | {s} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4093` · _two_way_teardown | dry-run, add --go to execute | Review teardown SQL, then add --go only when authorized. | yes |
| `migkit/cli.py:4123` · _tail_before_copy | {db}: an earlier copy saved no change position, so what changed on its tables since cannot be carried - copying them again | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4155` · _stream | would tear down the streaming pipeline for {hop.name} (dry-run, add --go) | Review teardown scope, then add --go only when authorized. | yes |
| `migkit/cli.py:4158` · _stream | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4160` · _stream | [green]streaming pipeline torn down[/green] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4162` · _stream | streaming pipeline prepared: {out} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4164` · _stream | dry-run, add --go to start it | Review the prepared stream and add --go only when authorized. | yes |
| `migkit/cli.py:4166` · _stream | starting the streaming pipeline ... | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4167` · _stream | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4168` · _stream | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4169` · _stream | the streaming pipeline did not come up (its stack is written under {out}); migkit doctor says what this machine is missing | Run migkit doctor and inspect the saved pipeline files to resolve startup prerequisites. | yes |
| `migkit/cli.py:4172` · _stream | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4174` · _stream | + {movers.stream_status(c)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4177` · _stream | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4179` · _stream | [green]cdc streaming[/green] - progress: migkit watch {hop.name} verify: migkit watch {hop.name} --verify --delta | Run the displayed watch command for progress and watch --verify --delta for continuing verification. | yes |
| `migkit/cli.py:4215` · move | --drop tears the stream down: migkit move {hop_name} --mode cdc --drop | Use the shown --mode cdc --drop command for teardown. | yes |
| `migkit/cli.py:4218` · move | --table narrows a copy; a stream follows the whole database (--mode full or full+cdc) | Use a copy mode for --table, or remove --table from a whole-database stream. | partly |
| `migkit/cli.py:4221` · move | --chunk sizes the ranges of a copy; --mode cdc copies nothing | Remove --chunk from cdc mode or use a copy mode. | yes |
| `migkit/cli.py:4237` · _move_run | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4257` · _move_worded | {without_programs(str(e), DRIVEN)} + + {f'{why}\n' if why else ''} + Nothing past the last finished table is marked as moved. The commands that ran are in the run's commands.log. | Inspect commands.log and the named cause; reconcile active writers and partial effects before rerunning the move. | partly |
| `migkit/cli.py:4268` · _move_worded | the move stopped: no space left on this machine for {getattr(e, 'filename', None) or 'its report directory'}. Free space there and move again; the checkpoint is as it was before the write that did not fit, and the next run goes on from it. | Free space at the named path and rerun the same move to resume its checkpoint. | yes |
| `migkit/cli.py:4289` · _move_worded | the copy stopped: {without_programs(str(e), DRIVEN)} + {f'{why}\n' if why else ''} + Nothing past the last finished table is marked as moved. The commands that ran are in the run's commands.log. | Read commands.log, resolve the copy failure and reconcile unfinished work before retrying. | partly |
| `migkit/cli.py:4322` · _protect_target | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4326` · _protect_target | {callback text from freeze.thaw} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4330` · _protect_target | {callback text from freeze.protect} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4337` · _ask_access | {question} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | yes |
| `migkit/cli.py:4353` · _carried_physically | {hop.name}: a seed from a copy of the source's pages cannot be held to the number of the machine that holds the hop, so it is not run with standby on. Seed it from one machine with standby off, then turn standby on for the change stream (--mode cdc). Nothing was written | Review a seed from one machine with standby off, then enable standby for the cdc stream. | yes |
| `migkit/cli.py:4362` · _carried_physically | [bold]physical seed[/bold] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4366` · _carried_physically | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4374` · _carried_physically | dry-run, add --go to execute | Review the physical seed plan and add --go only when authorized. | yes |
| `migkit/cli.py:4386` · _carried_physically | {d}: its changes go on from the copy's own position - migkit move {hop.name} --mode cdc --db {d} --go | Run the printed cdc command to continue changes from the seed position. | yes |
| `migkit/cli.py:4390` · _carried_physically | [green]seeded[/green] - verify: migkit check {hop.name} | Run migkit check HOP to verify the seeded target. | yes |
| `migkit/cli.py:4405` · _carried_on_media | [bold]seed on media[/bold] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4407` · _carried_on_media | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4414` · _carried_on_media | dry-run, add --go to execute | Review the media seed plan and add --go only when authorized. | yes |
| `migkit/cli.py:4418` · _carried_on_media | {d}: {got.said} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4421` · _carried_on_media | {d}: ship the carrier, then run the same command where the target is: migkit move {hop.name} --mode {mode} --go | Transport the carrier and run the printed import command at the target. | yes |
| `migkit/cli.py:4427` · _carried_on_media | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4431` · _carried_on_media | {settled} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4441` · _carried_on_media | {d}: its changes go on from the seed's position - migkit move {hop.name} --mode cdc --db {d} --go | Run the printed cdc command from the seed position. | yes |
| `migkit/cli.py:4445` · _carried_on_media | [green]seeded[/green] - verify: migkit check {hop.name} | Run migkit check HOP to verify the seeded target. | yes |
| `migkit/cli.py:4465` · _move | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4467` · _move | + {hopkeys.explain_mode('move', mode, True)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4489` · _move | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4491` · _move | the case could not be worked out: {upgrade._quiet(str(e).strip()[:160])} | Inspect the case-detection error and run migkit doctor; resolve uncertainty before approving execution. | no |
| `migkit/cli.py:4509` · _move | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4511` · _move | upgrade: the way could not be worked out: {upgrade._quiet(str(e).strip()[:160])} | Inspect upgrade prerequisites and the saved reason; resolve the missing facts before approving an upgrade. | no |
| `migkit/cli.py:4516` · _move | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4521` · _move | {_esc(m)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4527` · _move | {_esc(m)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4534` · _move | {_esc(m)} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4569` · _move | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4573` · _move | the rows go through this machine: {passed} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4575` · _move | {why}, so the tables are copied one by one | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4621` · _move | {db}: the copy was finished before this machine took the move over; the change stream goes on from where it stood | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4633` · _move | with --go the copy is followed by the change tail, from a position taken before the copy starts | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4638` · _move | moving and then following a {hop.engine} source is not set up on this machine; migkit doctor says what is missing | Run migkit doctor to identify missing copy-and-follow prerequisites. | yes |
| `migkit/cli.py:4657` · _bulk_db | [bold]{_esc(d)}[/bold] bulk copy: | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4658` · _bulk_db | {_esc(movers.after_a_stop(v))} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4661` · _bulk_db | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4670` · _bulk_db | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4673` · _bulk_db | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4680` · _bulk_db | {s0} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4685` · _bulk_db | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4688` · _bulk_db | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4689` · _bulk_db | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4696` · _bulk_db | {settled} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4698` · _bulk_db | [green]{d}: bulk copy complete[/green], run migkit check to verify | Run migkit check HOP to verify the bulk copy. | yes |
| `migkit/cli.py:4709` · _would_stop | with --go this stops: + {_esc(e.code.strip().splitlines()[0])} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4721` · _programs_ready | a program an earlier move of {d} started is still running (process {left[0][0]}) and writing; a move beside it would empty the tables under it | Inspect the named process and saved run log; let its owner finish or stop it safely, confirm it stopped, then retry. | no |
| `migkit/cli.py:4726` · _programs_ready | the programs installed here do not take options the move passes them - migkit doctor --install puts builds that do in place | Run migkit doctor --install, then retry the move plan. | yes |
| `migkit/cli.py:4769` · _bulk_or_copier | [yellow]{_esc(m)}[/yellow] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4851` · _cdc.run | [yellow]native change tail needs a driver this machine does not have - using migkit's streaming pipeline instead[/yellow] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4861` · _cdc | [yellow]{_esc(m)}[/yellow] | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4872` · _cdc | no way to follow a {hop.engine} source is set up on this machine + {f' - {held[0]}' if held else '; migkit doctor says what is missing'} | Run migkit doctor and resolve the named missing follow prerequisite before retrying the stream plan. | partly |
| `migkit/cli.py:4916` · _follow_group | {hop.name}: a group's changes are followed whole - leave out --table | Remove --table to follow the whole group. | yes |
| `migkit/cli.py:4920` · _follow_group | {hop.name}: the follow of a group runs one database a process - --db one of {', '.join(dbs)} | Specify --db with one of the listed databases. | yes |
| `migkit/cli.py:4927` · _follow_group | [{dbs[0]}] its group: {len(g.sources)} sources ({', '.join((s.name for s in g.sources))}) onto {len(g.targets)} targets ({', '.join((t.name for t in g.targets))}) | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4931` · _follow_group | {callback text from reshard.begin} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4933` · _follow_group | [bold]{_esc(name)}[/bold] - its copy, before the group's changes are followed | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4936` · _follow_group | {callback text from reshard.follow} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4949` · _cutover.approve | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:4954` · _cutover | {callback text from reshard.cut_over} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4958` · _cutover | {callback text from cutover.run} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:4965` · _cutover | {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5008` · _stream_leg.tail_end | {d}: the change tail did not end | Keep cutover blocked; inspect tail progress and saved logs, confirm its state and resolve the failed stop before retrying. | no |
| `migkit/cli.py:5009` · _stream_leg.tail_end | {d}: the change tail ended | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5032` · _stream_leg.tail_down | {d}: the change tail did not pause | Keep cutover blocked; inspect tail progress/logs and resolve the failed pause before retrying. | no |
| `migkit/cli.py:5071` · _stream_leg.back_up | [bold]the other way[/bold] - from the new primary back to the old source, as the hop says (reverse: at_cutover) | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5087` · _stream_leg.back_arm | [bold]the other way[/bold] - made ahead, idle until the flip (reverse: at_cutover) | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5137` · _origin_away | the table of migkit's batches stays on the target: the hop streams both ways, and the other way reads it | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5146` · _origin_away | {d}: the table of migkit's batches stays on the target: a tail of the hop still runs, and reads its last batch back from it after a stop | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5152` · _origin_away | {d}: target: {s} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5191` · _follow_leg.up | {d}: the change leg run from here ended at the drain; &#96;migkit move {hop.name} --mode cdc --go&#96; starts it again from its slot, which kept every change since | Run the printed cdc command to resume changes after the drain if that is the intended recovery path. | yes |
| `migkit/cli.py:5198` · _follow_leg.after_flip | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5199` · _follow_leg.after_flip | {d}: {line} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |
| `migkit/cli.py:5305` · _pipeline_leg.after_flip | {m} | Review the current copy/stream state; wait while it is active, then run migkit check HOP before treating the target as verified. | no |

### watch / sync --serve — 17 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:5330` · _hop_again | [yellow]the hop could not be read again ({_esc(_clean(e, raw=True))}); this cycle goes by the one read before[/yellow] | Correct the hop configuration/read access and confirm a later cycle loads it; stop relying on stale configuration for decisions. | no |
| `migkit/cli.py:5343` · _hop_again | [yellow]the hop's scope changed since the last cycle: what it takes is read afresh from here, and the rates and quiet start again[/yellow] | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5372` · _delta_loop | {_esc(d)}: [yellow]{_esc(_clean(e, raw=True))}[/yellow] | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5378` · _delta_loop | {_esc(d)}: delta state removed | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5400` · _delta_loop | {_esc('[' + str(d) + ']')} {_esc(m)} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5412` · _delta_loop | {stamp} cycle {n} {_esc(d)}: [{color}]{_esc(st.upper())}[/{color}] {_esc(head.get('detail', ''))} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5417` · _delta_loop | [yellow]{_esc(r.get('scope', ''))}: {_esc(r.get('detail', ''))}[/yellow] + {f" fix: {_esc(r['fix_hint'])}" if r.get('fix_hint') else ''} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5428` · _delta_loop | [green]SAFE TO CUT OVER[/green]: verified the same this cycle, the source made no change since the last + {', and the tail has nothing left to apply' if st_tail is not None else ''} | Review the intended cutover plan and approval requirements; this cycle alone does not authorize a flip. | no |
| `migkit/cli.py:5434` · _delta_loop | same as far as verified; + {'the tail has changes still to apply' if behind else 'the source is still being written' if position is not None and last.get(d) else 'whether the source is still being written is known from the next cycle'} | Wait for the next cycle or for the tail to catch up; reverify before cutover. | partly |
| `migkit/cli.py:5463` · _watched | {callback text from notify.verdict} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:5465` · _watched | {callback text from notify.lags} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:5511` · _monitor | {stamp} cycle {n} {_esc(d)}: + {' '.join(statuses)} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5646` · watch | --only names the checks --verify runs each cycle | Use --only with --verify, or remove --only. | yes |
| `migkit/cli.py:5648` · watch | --teardown drops what --delta keeps; --verify alone keeps nothing to drop | Use --teardown for delta state, not a verify-only run. | partly |
| `migkit/cli.py:5696` · _sample_watch | {_esc(d)}: [red]{_esc(s['error'])}[/red] | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5746` · _sample_watch | {line} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |
| `migkit/cli.py:5752` · _sample_watch | {_esc(one)} | Let the next monitoring cycle complete; investigate any named error or lag and verify again before cutover. | no |

### report — 9 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:5801` · report | --serve and --metrics are for every hop; --refresh, --open and a hop name are for one hop's report | Choose the all-hop serve/metrics form or the single-hop report form; remove conflicting flags. | yes |
| `migkit/cli.py:5804` · report | --port is where --serve listens | Use --port with --serve, or remove --port. | yes |
| `migkit/cli.py:5807` · report | {prometheus(load_hops())} | Open the named report/evidence file; run migkit check HOP for fresh answers. | no |
| `migkit/cli.py:5819` · report | which hop? reports exist for: + {', '.join(have) or 'none'} | Run migkit report HOP with a listed hop; if none exist, run migkit check HOP first. | partly |
| `migkit/cli.py:5827` · report | rendered again from the last check's answers, as it took them - &#96;migkit check&#96; takes them afresh | Run migkit check HOP to obtain fresh answers rather than only rendering saved ones. | yes |
| `migkit/cli.py:5834` · report | report ({generated}): {path} | Open the named report/evidence file; run migkit check HOP for fresh answers. | no |
| `migkit/cli.py:5836` · report | no check yet, so no check report: &#96;migkit check {hop.name}&#96; writes one | Run migkit check HOP to create the first check report. | yes |
| `migkit/cli.py:5838` · report | [yellow]{m}[/yellow] | Open the named report/evidence file; run migkit check HOP for fresh answers. | no |
| `migkit/cli.py:5840` · report | evidence: {out / 'evidence.html'} (and evidence.md, evidence.json, tables.csv beside it) | Open the named report/evidence file; run migkit check HOP for fresh answers. | no |

### report / check (HTML) — 18 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/report.py:79` · _chip | OK / DIFF / ERROR / WARN / SKIP (KEPT at render:282) | Read the finding detail and any fix hint; a status chip alone gives no next action. | no |
| `migkit/report.py:98` · _excerpt | {report filename}: {first 40 lines} | Open the full referenced artifact if the excerpt is insufficient; resolve the finding and check again. | partly |
| `migkit/report.py:132` · _objects_section | - (no object inventory cell) | Run a fresh schema check for the intended scope to obtain missing object evidence. | no |
| `migkit/report.py:134` · _objects_section | {source object count} / {source count}/{target count}; tooltip {missing and extra names} | Review missing/extra objects and generate a schema repair plan before applying changes. | no |
| `migkit/report.py:142` · _objects_section | objects (source count, src/dst when they differ) | Compare the counts and inspect names of missing/extra objects. | no |
| `migkit/report.py:170` · _scope_section.rows_of | {scope label}: {up to 40 names} | Confirm that this scope matches the intended migration; inspect saved scope evidence for truncated names. | no |
| `migkit/report.py:174` · _scope_section | took everything, whole | Continue reviewing findings; this is scope context, not an instruction. | no |
| `migkit/report.py:178` · _scope_section | the hop now takes another scope than these answers were taken under ... migkit check takes them under the one below | Run migkit check HOP to verify the current scope. | yes |
| `migkit/report.py:195` · _service_section | it called the same; migkit found them different / it called different; migkit found the same / it gave its word; migkit's answer was neither the same nor different / it passed over / it never checked / migkit found beyond any table check / it checked; the hop does not take / agreed | Inspect the named tables and current findings; resolve differences or missing proof with a fresh full-scope check. | no |
| `migkit/report.py:208` · _service_section | {servicecheck.said(got)}; {comparison rows} | Review disagreements and uncovered tables before accepting the migration as verified. | partly |
| `migkit/report.py:257` · render | {passed}/{total} {check label} | Open findings for failing or unverified checks; repeat verification after correcting them. | no |
| `migkit/report.py:264` · render | {database}: {status chip or -} | Read detailed findings for each non-OK or absent cell. | no |
| `migkit/report.py:275` · render | {fix_hint} (optional) | Follow the finding-specific hint only after reviewing its scope and authorization, then verify again. | partly |
| `migkit/report.py:279` · render | {check} · {scope}: {detail}; {artifact_origin} artifact; {optional fix and excerpt} | Resolve the named finding using its hint; if no hint exists, inspect its referenced artifact and rerun the relevant check. | partly |
| `migkit/report.py:288` · render | none (issues) | Review the banner and scope; absence of issue cards does not itself prove a complete comparison. | no |
| `migkit/report.py:293` · render | SAME — {n_all} checks {why} / {status} — {findings} of {n_all} checks {why} | For SAME, review scope; otherwise resolve each blocker or difference and run migkit check HOP again. | no |
| `migkit/report.py:315` · render | migkit report - {hop}; migkit check / {engine} / {service} / {generated}; {source} → {target}; status; issues; {grouped database summary and comparison HTML} | Confirm hop, route and timestamp; inspect findings, and run migkit check HOP for current evidence. | no |
| `migkit/report.py:351` · from_summary | no summary at {p}, run migkit check first | Run migkit check HOP, then migkit report HOP. | yes |

### history — 10 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:5872` · history | saved states ({backend}): {state timestamp} / {db} / {captured} / {op} | Inspect the intended state with migkit history HOP --show STATE before choosing a rollback. | no |
| `migkit/cli.py:5875` · history | {t} | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5882` · history | [bold]{_esc(d)}/{_esc(m['ts'])}[/bold] | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5884` · history | {jl.read_text().strip()} | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5889` · history | local changelog (last 15): | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5891` · history | {line} | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5893` · history | [red]the record has been changed: {_esc(broken)}[/red] | Preserve the altered record and investigate its integrity before relying on it for rollback or audit. | no |
| `migkit/cli.py:5896` · history | {chained} of {n} entries chained, and the chain is whole + {f' ({n - chained} from before the chain began)' if n > chained else ''} | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5902` · history | {line} | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |
| `migkit/cli.py:5904` · history | no history yet, nothing has been applied | Inspect the named saved state or journal before choosing a rollback; do not treat history as a fresh verification. | no |

### rollback — 17 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/cli.py:5949` · rollback | {callback text from cutover.back_plan} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:5962` · rollback | row repairs to put back, newest first: | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5964` · rollback | {_esc(repairlog.said(path))} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5969` · rollback | {_esc(m)} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5971` · rollback | {callback text from cutover.back_apply} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. Read the producer-specific blocker before proceeding. | partly |
| `migkit/cli.py:5979` · rollback | {_esc(m)} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5981` · rollback | the application's roles are still kept from writing to the target; --apply gives their writes back, after the rest | Review rollback scope; --apply restores application writes only as part of the authorized rollback. | partly |
| `migkit/cli.py:5991` · rollback | row-level undo (run per manifest line): | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5992` · rollback | {manifest.read_text().strip()} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:5994` · rollback | dry-run, add --apply to put back what is listed above | Review every listed undo action and add --apply only for the authorized rollback. | yes |
| `migkit/cli.py:6007` · _restore_target | no saved state of the target's counters: they are left as they are | Inspect target counters against source/retained evidence; no saved counter snapshot is available for this rollback. | no |
| `migkit/cli.py:6011` · _restore_target | no saved states, run migkit sync --go first | Run migkit history HOP; use an existing saved state, and plan future restore points before repair. Do not repair merely to create a rollback state. | partly |
| `migkit/cli.py:6014` · _restore_target | no state matching '{state_ts}', have: {', '.join(states)} | Choose the intended state from those listed and rerun the rollback plan. | partly |
| `migkit/cli.py:6019` · _restore_target | state {ts} not retrievable from {store.kind} | Check state backend access and retained artifacts; restore retrieval before retrying rollback. | no |
| `migkit/cli.py:6052` · _restore_target | state {ts}: {len(stmts)} values in snapshot, {len(changed)} differ from current target, {same} already match | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:6056` · _restore_target | {s} | Review the selected saved state and undo statements; apply only an authorized rollback, then run migkit check HOP. | no |
| `migkit/cli.py:6071` · _restore_target | [green]values restored to snapshot[/green] | Run migkit check HOP to verify the restored target. | no |

### check / watch / report (shared verdict) — 55 sites/families

| File:line · owner | Current text / source template | What the operator must do next | Already says it? |
|---|---|---|---|
| `migkit/verdict.py:48` · VERDICTS | same / different / could not verify / incomplete | Read the verdict reason; review scope on same, resolve findings on different/error, and obtain missing proof on incomplete. | no |
| `migkit/verdict.py:63` · ROWS | same / not compared / to read / different / could not verify | Read the corresponding finding and obtain any missing comparison evidence. | no |
| `migkit/verdict.py:68` · MEANS.same | every answer the run set out to take was taken, and the two sides hold the same | Confirm the scope covers the intended migration before continuing. | no |
| `migkit/verdict.py:70` · MEANS.done | a command that is not a comparison did what it was asked; a plan without --go or --apply was shown | Review a dry-run plan before authorizing execution; verify completed work with migkit check HOP. | partly |
| `migkit/verdict.py:72` · MEANS.different | a difference was found - the check did its work; the report names which table, row and column | Open the report, resolve lag first and review an appropriate repair plan. | partly |
| `migkit/verdict.py:74` · MEANS.usage | flags that do not go together, or a name that is not there (a check --only does not know, a variable's value) | Correct the named flag combination or value and rerun the command. | no |
| `migkit/verdict.py:76` · MEANS.error | an answer could not be taken: a server that did not answer, a check that failed | Restore connectivity or the failed prerequisite, then rerun migkit check HOP. | no |
| `migkit/verdict.py:78` · MEANS.incomplete | nothing differs, and not everything was looked at: a narrowed run, answers to read, answers reused from an earlier run, a schema change mid-run, nothing compared | Resolve the named gap and run a fresh check covering the intended scope. | no |
| `migkit/verdict.py:82` · MEANS.failed | the command stopped part way, said why in words; the details are in the hop's commands.log | Read the saved run log, reconcile partial effects and active writers, then retry only after resolving the cause. | partly |
| `migkit/verdict.py:84` · MEANS.refused | a guard stopped the command before anything was written: the goal, a hop that masks, an approval, the target not the one the plan showed, a check known to fail | Satisfy the named guard without weakening its safety condition, then rerun the plan. | no |
| `migkit/verdict.py:87` · MEANS.interrupted | stopped by Ctrl-C or SIGTERM; what was saved as it went is kept | Confirm the previous run has stopped, then rerun the same command to resume saved work. | partly |
| `migkit/verdict.py:229` · decide | {n} answers could not be taken: {names}; an answer migkit has no word for ({statuses}) is taken as not verified | Inspect the named failing findings, fix the cause and run a fresh check; escalate unknown statuses with the saved evidence. | no |
| `migkit/verdict.py:237` · decide | {n} differ: {names} | Read the named findings, wait out lag and review a repair plan before applying changes. | no |
| `migkit/verdict.py:240` · decide | nothing was compared: no answer at all | Check scope, visibility and permissions with migkit doctor, then rerun migkit check HOP. | no |
| `migkit/verdict.py:243` · decide | {n} answers were taken while the source's schema changed: {names} | Wait for source DDL to settle, then rerun migkit check HOP. | no |
| `migkit/verdict.py:247` · decide | {n} answers to read before the two are called the same: {names} | Read each warning, resolve its unproved condition and run a fresh check. | partly |
| `migkit/verdict.py:252` · decide | {n} answers were reused from an earlier run and the source may have changed since: {names} | Run migkit check HOP without --resume for fresh answers. | no |
| `migkit/verdict.py:256` · decide | every answer was skipped - nothing was compared: {names} | Read each skip reason, resolve unsupported or missing prerequisites, and check the intended scope again. | no |
| `migkit/verdict.py:259` · decide | the run was narrowed to {coverage} | Run migkit check HOP without --only/--db/--table narrowing to verify the full configured hop. | no |
| `migkit/verdict.py:263` · decide | {n} compared the same; not compared: {names} | Read the skips and obtain missing proof if those checks are required. | no |
| `migkit/verdict.py:265` · decide | {n} hold only what the hop keeps by the changes it does not carry: {names} | Confirm mapping.ops represents the intended retained differences; do not repair deliberately kept rows. | no |
| `migkit/verdict.py:286` · difference_kind | rows-missing / rows-extra / rows-replaced / values-changed (detail vocabulary) | Inspect the named rows in a data check before planning repair. | no |
| `migkit/verdict.py:408` · held | {at least}{rows} {kept kind} - {why} | Confirm the retained change policy matches the intended target behavior. | no |
| `migkit/verdict.py:411` · held | {at least}{rows} {kind} are a difference - {reason} | Review actual differing rows and an authorized repair plan. | no |
| `migkit/verdict.py:420` · held | kept by the hop: {details} | Confirm mapping.ops; retain rows the hop intentionally keeps. | no |
| `migkit/verdict.py:422` · held | {whys} for this table, and the rows apart are not proved to be only the ones it keeps ... a data check names them | Run migkit check HOP --only data for row-level proof, then a full check. | partly |
| `migkit/verdict.py:549` · _keep / hold | {kept detail}; {original detail} (fix_hint cleared); read a second way, the same rows at 628 | Review the kept-row policy rather than applying a whole-source repair. | no |
| `migkit/verdict.py:607` · hold | {held explanation}; {original detail} / {original detail}; {held explanation} at 609 | Follow the held explanation; obtain row-level proof where counts alone were inconclusive. | partly |
| `migkit/verdict.py:658` · partition_differences | {table} partition {n}: named {d} on target | Confirm the intended partition naming and bound mapping. | no |
| `migkit/verdict.py:662` · partition_differences | {table} partition {n}: missing on target, the source's holds {rows} / empty on the source | Review a schema plan for the missing partition, then copy affected rows and verify. | no |
| `migkit/verdict.py:667` · partition_differences | {table} partition {n}: extra on target, holding {rows} / empty | Confirm whether the target partition is intentional before any removal; compare its rows. | no |
| `migkit/verdict.py:674` · partition_differences | {table} partition {sn}: bound differs src({bound}) dst({bound}) | Review and correct the target partition bounds before moving or redistributing rows. | no |
| `migkit/verdict.py:677` · partition_differences | {table} partition {sn}: empty on target, the source's holds {rows} | Check copy/stream progress and repair the missing partition rows after reviewing scope. | no |
| `migkit/verdict.py:680` · partition_differences | {table} partition {sn}: {rows} stranded in the catch-all on target (src={a} dst={b}) | Correct intended partition bounds and review redistribution of stranded rows, then verify. | no |
| `migkit/verdict.py:684` · partition_differences | {table} partition {sn}: src={a} dst={b} rows | Wait out replication lag, then compare affected rows before planning repair. | no |
| `migkit/verdict.py:687` · partition_differences | {table} partition {sn}: {rows} both sides, content differs | Run a data comparison to identify changed values before repair. | no |
| `migkit/verdict.py:712` · uniform_shift | {name}: every row shifted {secs}s (~{hours}h) | Review source/target timezone handling and the accompanying timeshift fix hint before correcting values. | no |
| `migkit/verdict.py:717` · uniform_shift | {name}: every row shifted {a}s or {b}s (~{hours}h, a zone with daylight saving) | Review timezone and daylight-saving conversion; use the accompanying finding to plan a verified correction. | no |
| `migkit/verdict.py:756` · narrowing | {src_type} -> {dst_type} (longer values are cut) [also 762] | Widen the target type to preserve source values before loading, then verify. | no |
| `migkit/verdict.py:765` · narrowing | {src_type} -> {dst_type} (overflow) | Widen the target numeric type before loading. | no |
| `migkit/verdict.py:770` · narrowing | {src_type} -> {dst_type} (any precision -> {digits} digits: overflow or rounds) | Choose a target precision that preserves all source values before loading. | no |
| `migkit/verdict.py:773` · narrowing | {src_type} -> {dst_type} (scale {a} -> {b}: rounds) | Preserve the source scale in the target before loading. | no |
| `migkit/verdict.py:776` · narrowing | {src_type} -> {dst_type} (integer digits {a} -> {b}: overflow) | Widen target integer precision before loading. | no |
| `migkit/verdict.py:807` · compared_clause | {n} exactly (one engine, one type): {columns}; {n} by value: {columns}; {n} not at all: {columns}; of {n} columns, compared ... | Review every not-at-all column and resolve the unproved comparison before accepting completeness. | no |
| `migkit/verdict.py:823` · narrowing_result | {n} target columns NARROWER than source (silent truncation/overflow risk): {columns}; widen the target column to match source before loading, or values are cut/rounded/overflowed | Widen the named target columns before loading and rerun verification. | yes |
| `migkit/verdict.py:829` · narrowing_result | no target column narrower than source | Continue reviewing the remaining checks; no widening is required by this finding. | no |
| `migkit/verdict.py:838` · timeshift_result | uniform timezone offset (systematic, not row-level corruption): {shifts}; the target stored a non-UTC wall clock; re-load with the source's session timezone, or convert the column by the offset | Review the timezone correction/reload plan, apply only with authorization and verify timestamps again. | partly |
| `migkit/verdict.py:844` · timeshift_result | no uniform timestamp offset detected | Continue reviewing other value checks; this does not exclude irregular timestamp corruption. | no |
| `migkit/verdict.py:875` · options_result | storage options of every table match | Continue reviewing the remaining checks. | no |
| `migkit/verdict.py:878` · options_result | {n} storage options differ, {n} of them put the target's data at risk / (performance, not data): {table}: {option} src={a} dst={b}; {optional fix} | Review unsafe durability options first; have the database owner correct intended target settings and check again. | partly |
| `migkit/verdict.py:917` · mark_stale | taken while the source's schema changed ({said}); this is no longer an answer - check again once it has settled. It read: {status}, {detail} | Wait for source DDL to settle, then rerun migkit check HOP. | yes |
| `migkit/verdict.py:1000` · SCOPE_WORDS | takes only / leaves out / rows filtered in / columns chosen in / values masked in / renamed / names written / databases renamed / changes carried / objects not carried / only these objects | Review the configured scope before accepting a verdict. | no |
| `migkit/verdict.py:1032` · contract_said | {table}: {value} (unparseable operation rule) | Correct the table operation rule and rerun migkit doctor before checking. | no |
| `migkit/verdict.py:1034` · contract_said | {table}: {carried operations}; {op}s are not carried - the target keeps the rows the source deletes / the rows the source inserts stay off the target / the target's rows keep the values the source's updates change | Confirm the intended carried/retained changes; correct mapping.ops only if the business intent differs. | no |
| `migkit/verdict.py:1055` · _classify_finding / summarize | {detail}, {report}, {fix_hint}, {kept_by_hop}; envelope {status}, {why}, {coverage}, {stale} | Follow each finding-specific hint and verdict reason; incomplete or stale evidence requires fresh verification. | partly |

## Raise and forwarding reconciliation

Every explicit raise in the requested files was inspected. Numeric exit codes are not additional messages; their previously emitted findings are audited above. Bare rethrows and exception wrapping forward existing text to shared handling. Callback wrappers add indentation/markup but no independent instruction. These sites are excluded from the message totals:

| Source | Reason / expression |
|---|---|
| `migkit/cli.py:37` | forwarding console.print |
| `migkit/cli.py:42` | forwarding console.print |
| `migkit/cli.py:321` | raise |
| `migkit/cli.py:324` | raise Refused(e.code) from e |
| `migkit/cli.py:325` | raise |
| `migkit/cli.py:567` | raise |
| `migkit/cli.py:569` | raise |
| `migkit/cli.py:592` | raise KeyboardInterrupt |
| `migkit/cli.py:786` | raise |
| `migkit/cli.py:978` | raise SystemExit(EXIT['failed']) |
| `migkit/cli.py:1364` | raise SystemExit(1) |
| `migkit/cli.py:1391` | blank separator |
| `migkit/cli.py:1627` | raise SystemExit(got.exit) |
| `migkit/cli.py:1903` | raise |
| `migkit/cli.py:2583` | raise SystemExit(got.exit) |
| `migkit/cli.py:2846` | raise |
| `migkit/cli.py:2999` | raise |
| `migkit/cli.py:3051` | forwarding console.print |
| `migkit/cli.py:3156` | forwarding console.print |
| `migkit/cli.py:3210` | raise |
| `migkit/cli.py:3217` | raise failed |
| `migkit/cli.py:3469` | forwarding console.print |
| `migkit/cli.py:3521` | forwarding console.print |
| `migkit/cli.py:3810` | raise |
| `migkit/cli.py:3995` | forwarding console.print |
| `migkit/cli.py:4265` | raise |
| `migkit/cli.py:4279` | raise |
| `migkit/cli.py:4319` | forwarding console.print |
| `migkit/cli.py:4866` | raise |
| `migkit/cli.py:4925` | forwarding console.print |
| `migkit/cli.py:4986` | forwarding console.print |
| `migkit/cli.py:5461` | forwarding console.print |
| `migkit/cli.py:5659` | raise SystemExit(got.exit) |
| `migkit/guide.py:131` | raise |
| `migkit/guide.py:253` | raise |
| `migkit/guide.py:290` | blank separator |
| `migkit/guide.py:295` | raise |
| `migkit/guide.py:335` | raise |
| `migkit/guide.py:817` | raise |

`report.py` has one explicit raise, included at line 351. `doctor.py` and `verdict.py` contain no explicit raises or direct log/say/echo calls: their returned diagnostic lines, verdict reasons and Result details are audited as producers. Future work should enforce actionable context at both the producer and the generic error boundary; fixing only a final sink will not explain engine-specific recovery.

## Findings and limits

- The largest practical gap is a failed or incomplete run whose reason names a condition but gives neither a recovery command nor the state that must be reconciled before retrying. A generic “retry” is insufficient where old writers, partial DDL/repair, cutover freezes or restore points are involved.
- `check` prints next steps only when hints are enabled and the verdict is different (`cli.py:2176`). Error and incomplete outcomes can end without an equivalent route. The HTML report displays fix hints only when supplied (`report.py:274-286`), and its banner has no independent next action.
- Dynamic callback and exception text cannot be judged yes as a whole without a contract on the producer. Keep current redaction and component-name masking when adding an action; avoid leaking original commands or secrets into proposed operator copy.
- Setup has useful final commands, but discovery failures and “could not be asked” branches often precede those commands without explaining how to make the prerequisite succeed. Preserve the DBA review boundary.
- Success/progress should say whether to wait, inspect the report, verify, or proceed with an already approved step. “SAFE TO CUT OVER” is a cycle result; any next step still needs the configured cutover guards and authorization.
- No application code, command behavior, state, databases, configuration, backlog status or generated maps were changed. Implementing the wording and proving recovery behavior are follow-up work. This audit does not close the underlying product gap.

## Verification

Inventory derived from Python AST traversal of all raise sites and direct console/log/say/echo/prompt calls in the five files, with manual review of returned strings, tables, HTML sections and verdict branches. Verified source locations, per-command totals, 30 distinct priority references, and single-file commit scope. No live servers, database endpoints, containers, installation or full test suite were used. Guard results are recorded below.

- Syntax compilation: passed (`python -m compileall -q migkit tests`).
- Required five guard selections: **48 passed, 3 failed**. All three failures are in `tests/test_every_engine_is_as_deep_as_postgres_and_mysql.py`: `test_no_way_goes_without_a_test_unsaid` (13 untested entries), `test_no_declaration_is_one_the_code_or_a_test_has_closed` (218 stale declarations), and `test_every_declaration_and_proof_names_an_item_and_an_engine` (5 unknown entries). These are baseline capability/depth inconsistencies; no tracked source or test file changed in this audit. The output-name, duplicate-method, current-code-map and selected scope guards passed.
- Not done: operator wording changes, capability/depth baseline repairs, runtime recovery proof, and underlying R21.9 closure. The ticket explicitly restricts the commit to this report, so no backlog or generated-document edits are included.
- Coverage reconciliation: all 103 explicit raises and 356 direct output/prompt calls are represented in the inventory or forwarding/exclusion table. The 573 inventory rows total 95 yes, 90 partly and 388 no; the 30 priority sources are distinct.
- Secret scan of the staged report and staged whitespace check passed; generated manual check passed.

## Applied and skipped (W12N)

Applied in `migkit/verdict.py`, which no W12 ticket owns: priorities 16-20 and 29. Each keeps the phrase the tests pin (`the run was narrowed to`, `nothing was compared`, `missing on target, the source's holds`) and adds the exact command and the fact that triggered it. Priority 29 adds no command: the partition line is built in `partition_differences`, which receives no hop name, so the `migkit schema <hop>` route needs the name passed in from `migkit/engines/base.py` (W12E).

## Skipped (owned)

These sites sit in files owned by another W12 ticket (`W12-OWNERS.md`), so W12N did not edit them. Proposed wording is copied from the priority table above.

| Priority | Source | Owner | Proposed wording |
|---:|---|---|---|
| 1 | `migkit/cli.py:5008` | W12L (migkit/cli.py) | Cutover stopped because the change tail did not end for {db}. Do not advance the cutover or release an existing write freeze. Inspect this hop's tail status and saved run log, resolve the stop failure, then rerun the cutover plan before approving another attempt. |
| 2 | `migkit/cli.py:5032` | W12L (migkit/cli.py) | Cutover stopped because the change tail did not pause for {db}. Keep the cutover blocked. Inspect tail progress and the saved run log, resolve the pause failure, then rerun the cutover plan. |
| 3 | `migkit/cli.py:4721` | W12L (migkit/cli.py) | An earlier move is still writing to {db} (process {pid}). Do not start another move. Inspect the saved run log and have the owner finish or safely stop that process. Confirm it has stopped, then rerun the original move command. |
| 4 | `migkit/cli.py:244` | W12L (migkit/cli.py) | The shared checkpoint kept changing while it was saved. Check which machines still own this move and reconcile their state before retrying. Keep the checkpoint; once contention is resolved, rerun the original move command. |
| 5 | `migkit/cli.py:2310` | W12L (migkit/cli.py) | The writer did not pause within {window}s; no repair was applied and it was asked to resume. Confirm its current state, inspect why the pause failed, and resolve coordination before rerunning the repair plan. |
| 6 | `migkit/cli.py:6019` | W12L (migkit/cli.py) | Saved state {ts} could not be retrieved from {backend}. Check access to that backend and confirm the saved artifacts still exist. Restore retrieval, then rerun the rollback plan for this state; do not apply a different state by guess. |
| 7 | `migkit/cli.py:5893` | W12L (migkit/cli.py) | The audit record has changed at {entry}. Preserve this record and the saved run logs. Investigate the integrity failure before relying on this history for rollback or audit; do not rewrite the record to clear this warning. |
| 8 | `migkit/cli.py:6011` | W12L (migkit/cli.py) | There is no saved counter state to restore. Run &#96;migkit history {hop}&#96; and confirm the intended recovery evidence. If no suitable state exists, stop this rollback and obtain an approved recovery plan. Save a restore point before future repairs. |
| 9 | `migkit/cli.py:1442` | W12L (migkit/cli.py) | {count} target statements failed: {failures}. Review the failed statements and target permissions. {transaction_or_partial_state}. Reconcile any created objects, then rerun &#96;migkit schema {hop} --convert&#96; to review the remaining plan before applying it. |
| 10 | `migkit/cli.py:361` | W12L (migkit/cli.py) | The named columns cannot be masked safely for this engine: {columns_and_reasons}. Nothing was started. Correct the masking/type mapping and rerun the move plan; keep the move blocked until every masked column is supported. |
| 11 | `migkit/cli.py:3617` | W12L (migkit/cli.py) | Two-way replication cannot start: {reason}. Nothing was set up. Resolve the named loop-prevention or topology prerequisite, then run &#96;migkit doctor&#96; and review the move plan again before authorizing replication. |
| 12 | `migkit/cli.py:3783` | W12L (migkit/cli.py) | Replication was not set up: {reason}. Resolve the named scope, mapping or server prerequisite. Run &#96;migkit doctor&#96;, then rerun the same move command without &#96;--go&#96; to review the plan before executing it. |
| 13 | `migkit/cli.py:3060` | W12L (migkit/cli.py) | {db}: copy not started: {reason}. Resolve the named prerequisite, run &#96;migkit doctor&#96;, then rerun the same move command without &#96;--go&#96; to review the copy plan. |
| 14 | `migkit/cli.py:2395` | W12L (migkit/cli.py) | {db}: repair stopped: {reason}. Read the saved repair journal and compare the target with the recorded applied statements before retrying. Resolve uncertain effects, then rerun the repair plan without &#96;--apply&#96; or &#96;--go&#96;. |
| 15 | `migkit/cli.py:1995` | W12L (migkit/cli.py) | No source database visible to this login matches the hop. Check the configured database names and include/exclude rules, and ask the database owner to confirm listing/read permissions. Run &#96;migkit doctor&#96;, then &#96;migkit check {hop}&#96;. |
| 21 | `migkit/cli.py:1111` | W12L (migkit/cli.py) | The configuration at {path} is not valid YAML: {reason}. Correct the reported location while preserving existing hops, then run &#96;migkit doctor&#96; again. |
| 22 | `migkit/guide.py:256` | W12I (migkit/guide.py) | The {side} address could not be read. Set {address_variable} to a supported database address, or run &#96;migkit init&#96; in a terminal and enter it again. |
| 23 | `migkit/guide.py:298` | W12I (migkit/guide.py) | Could not reach the source: {reason}. Check the source address, network route, login and read permissions, then rerun &#96;migkit init&#96;. Discovery has not verified the source. |
| 24 | `migkit/cli.py:1016` | W12L (migkit/cli.py) | {hop} {side}: connection failed: {reason}. Check this side's endpoint, network route, credentials and read permissions, then run &#96;migkit doctor&#96; again. |
| 25 | `migkit/cli.py:1172` | W12L (migkit/cli.py) | {hop} {side}: could not check {requirement}: {reason}. Ask the database owner to resolve the probe error or required permission, then rerun &#96;migkit doctor&#96; to regenerate the prerequisites. |
| 26 | `migkit/cli.py:1213` | W12L (migkit/cli.py) | The {side} connection is not {encrypted_or_verified}: {reason}. Configure TLS and certificate verification for this endpoint, or the supported protected tunnel. Run &#96;migkit doctor&#96; again and resolve every transport failure before moving data. |
| 27 | `migkit/cli.py:1296` | W12L (migkit/cli.py) | The {side} port does not answer: {reason}. Check the configured host and port, network route, firewall and database listener, then rerun &#96;migkit doctor&#96;. |
| 28 | `migkit/cli.py:5330` | W12L (migkit/cli.py) | This cycle could not reload the hop: {reason}; it is using the previous configuration. Correct the file or read access and confirm a later cycle reloads it. Do not approve a scope change or cutover from this stale cycle. |
| 30 | `migkit/cli.py:1572` | W12L (migkit/cli.py) | {table} has no primary key for row joining, so this drilldown cannot run. Run &#96;migkit check {hop} --db {db} --only data&#96; to request the engine's complete data comparison. If that comparison is unsupported, keep the table unproved and review a schema/key plan with the database owner. |

## Applied (W16H)

Counted as the audit counted: the thirty priority sources against the inventory rows, before and after. Before: 0 yes, 1 partly, 29 no. After: 29 yes, 0 partly, 1 no (24). The whole inventory was not recounted: 95 yes / 90 partly / 388 no above, of which these thirty move 29 rows to yes (124 / 89 / 359). Every other stop that ends the command without a step now gets one at the command's end (`report.stop_next`), and every verdict carries `next`; those rows are not recounted here.

| Priority | Status | Where |
|---:|---|---|
| 1 | Applied 2026-10-16 on e891aeea | cli `_stream_leg` tail_end |
| 2 | Applied 2026-10-16 on e891aeea | cli `_stream_leg` tail_down |
| 3 | Applied 2026-10-16 on e891aeea | cli `_programs_ready` |
| 4 | Applied 2026-10-16 on e891aeea | cli `_Checkpoint._merge_into_remote` |
| 5 | Applied 2026-10-16 on e891aeea | cli `_make_room_for_rows` |
| 6 | Applied 2026-10-16 on e891aeea | report.restore_unreachable |
| 7 | Applied 2026-10-16 on e891aeea | report.record_changed |
| 8 | Applied 2026-10-16 on e891aeea | report.no_saved_states |
| 9 | Applied 2026-10-16 on e891aeea | report.schema_failed |
| 10 | Applied 2026-10-16 on e891aeea | report.mask_refused |
| 11 | Applied 2026-10-16 on e891aeea | report.two_way_refused |
| 12 | Applied 2026-10-16 on e891aeea | report.replication_refused |
| 13 | Applied 2026-10-16 on e891aeea | report.copy_not_started |
| 14 | Applied 2026-10-16 on e891aeea | report.repair_stopped |
| 15 | Applied 2026-10-16 on e891aeea | report.scope_empty |
| 16 | Applied 2026-10-16 on e891aeea | verdict.decide (`Decision.next`) |
| 17 | Applied 2026-10-16 on e891aeea | verdict.decide (`Decision.next`) |
| 18 | Applied 2026-10-16 on e891aeea | verdict.decide (`Decision.next`) |
| 19 | Applied 2026-10-16 on e891aeea | verdict.decide (`Decision.next`) |
| 20 | Applied 2026-10-16 on e891aeea | verdict.decide (`Decision.next`) |
| 21 | Applied 2026-10-16 on e891aeea | report.config_unreadable |
| 22 | Applied 2026-10-16 on e891aeea | report.address_unreadable (guide) |
| 23 | Applied 2026-10-16 on e891aeea | report.source_unreachable (guide) |
| 24 | Not applied: the doctor's own side-failure line is in the region another ticket (W16C) owns | cli `_doctor_side` |
| 25 | Applied 2026-10-16 on e891aeea | report.probe_unasked |
| 26 | Applied 2026-10-16 on e891aeea | report.transport_failed |
| 27 | Applied 2026-10-16 on e891aeea | report.port_silent |
| 28 | Applied 2026-10-16 on e891aeea | report.hop_not_reread |
| 29 | Applied 2026-10-16 on e891aeea | verdict.partition_differences |
| 30 | Applied 2026-10-16 on e891aeea | report.no_join_key |
