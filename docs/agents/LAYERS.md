# migkit layers

Written by `tools/gen_codemap.py` from `LAYERS` and `KNOWN_UPWARD` in it; the pre-commit hook stops a commit that leaves it behind. `tests/test_the_layers_import_downward.py` holds every import in `migkit/` to it. The packages are the plan's (`docs/research/source-hierarchy-2026-10-06.md`); which file is in which is [INVENTORY.md](INVENTORY.md).

## The rule

A package imports its own rank and the ranks below it, never one above. A rank of two lists packages beside each other. An import inside a function counts as one at the top of the module. A module that must reach a higher package is a defect to move, and until it is moved it is listed under `KNOWN_UPWARD` with its reason: the list only shrinks (the test fails on an import added and on one listed that is gone).

## Ranks, lowest first

| rank | package | responsibility |
|---|---|---|
| 1 | configuration | what a hop says: its keys, the environment, the endpoints and the addresses it is built from |
| 2 | scope | which objects and rows a hop carries |
| 3 | persistence | state that outlives a process: checkpoints, the audit record, the run's own files, the undo |
| 4 | safety | who may write, what must not leak, what keeps a mover from writing twice |
| 5 | execution | the running of work: processes, ranges, retries, pacing, stalls, sizing of what runs at once |
| 6 | conversion | a value or an object as another engine takes it: canonical text, transforms, types, stored code |
| 7 | reporting | what the operator reads: the verdict, the result shape, the log, the metrics, the notices |
| 8 | decision | choosing: the rungs, the cost model, the joint plan, what each engine can do |
| 9 | engines | the contract every engine fills and each engine's own adapter |
| 9 | integrations | the programs, libraries and provider APIs migkit wraps, and the other tools it meets |
| 10 | workflows | a move's phases and what they do around the move: copy, follow, prove, repair, cut over, recover |
| 11 | cli | the command line and the guide that asks for two addresses |
| 11 | web | the web view and its API |

Outside the rule: `tools` (the generators, the full suite and the checks run by the maintainers; they import migkit and migkit never imports them); `data` (the files migkit ships as resources).

## Upward imports still listed: 409

### (not listed)

- `migkit/checkpoint.py:33` persistence -> decision (`sizing`)
- `migkit/config.py:446` configuration -> execution (`keepalive`)
- `migkit/effect_checkpoint.py:52` persistence -> decision (`decide`)
- `migkit/engines/mysql.py:11567` engines -> workflows (`ratchet`)
- `migkit/engines/postgres.py:7995` engines -> workflows (`verifiers`)
- `migkit/engines/postgres.py:12696` engines -> workflows (`ratchet`)
- `migkit/engines/synapse.py:101` engines -> workflows (`users`)
- `migkit/exit_portability.py:16` configuration -> persistence (`checkpoint`)
- `migkit/exit_portability.py:17` configuration -> persistence (`durable`)
- `migkit/exit_portability.py:18` configuration -> scope (`resume_scope`)
- `migkit/init.py:1` configuration -> cli (`guide`)
- `migkit/managed.py:1119` configuration -> decision (`tune`)
- `migkit/partial_images.py:8` conversion -> workflows (`verifiers`)
- `migkit/query_guard.py:280` reporting -> decision (`decide`)
- `migkit/resume_scope.py:8` scope -> persistence (`checkpoint`)
- `migkit/resume_scope.py:9` scope -> persistence (`durable`)
- `migkit/scanplan.py:854` decision -> engines (`engines.base`)
- `migkit/scanplan.py:1016` decision -> workflows (`leaves`)
- `migkit/schema.py:1` conversion -> workflows (`indexes`)
- `migkit/schema_drift.py:480` conversion -> decision (`sizing`)
- `migkit/schema_drift.py:1287` conversion -> decision (`decide`)
- `migkit/schema_epoch_plan.py:537` conversion -> decision (`decide`)

### a brand's confidence is a rung (`decide.brand_confidence`) the configuration reads once the server's signs are in; the rung moves below configuration

- `migkit/variants.py:397` configuration -> decision (`decide`)

### a column is hidden by the way `decide.hide_column` picks from its facts; the rung moves below conversion

- `migkit/transforms.py:430` conversion -> decision (`decide`)

### a conversion asks the decision engine (`decide`) which way to take; the choice is handed in as a fact

- `migkit/charsets.py:1427` conversion -> decision (`decide`)
- `migkit/pieces.py:155` conversion -> decision (`decide`)
- `migkit/storedcode/collations.py:78` conversion -> decision (`decide`)
- `migkit/storedcode/rungs.py:22` conversion -> decision (`decide`)

### a conversion asks the decision engine (`tuning`) which way to take; the choice is handed in as a fact

- `migkit/canon.py:224` conversion -> decision (`tuning`)

### a conversion asks the decision engine (`workload`) which way to take; the choice is handed in as a fact

- `migkit/storedcode/statements.py:725` conversion -> decision (`workload`)

### a conversion calls `changetopic`, the workflow it serves; the call is inverted into a hook

- `migkit/described.py:91` conversion -> workflows (`changetopic`)

### a conversion calls `consolidate`, the workflow it serves; the call is inverted into a hook

- `migkit/charsets.py:1613` conversion -> workflows (`consolidate`)

### a conversion calls `cutover`, the workflow it serves; the call is inverted into a hook

- `migkit/storedcode/reference.py:69` conversion -> workflows (`cutover`)

### a conversion calls `movers`, the workflow it serves; the call is inverted into a hook

- `migkit/storedcode/statements.py:419` conversion -> workflows (`movers`)

### a conversion calls `physical`, the workflow it serves; the call is inverted into a hook

- `migkit/charsets.py:1253` conversion -> workflows (`physical`)

### a conversion calls `streamout`, the workflow it serves; the call is inverted into a hook

- `migkit/schemaformats.py:178` conversion -> workflows (`streamout`)

### a conversion drives a wrapped tool or provider (`assist`); the call goes through an integration contract below conversion

- `migkit/storedcode/convert.py:482` conversion -> integrations (`assist`)
- `migkit/storedcode/model.py:44` conversion -> integrations (`assist`)

### a conversion drives a wrapped tool or provider (`avrostream`); the call goes through an integration contract below conversion

- `migkit/schemaformats.py:86` conversion -> integrations (`avrostream`)

### a conversion drives a wrapped tool or provider (`registry`); the call goes through an integration contract below conversion

- `migkit/schemaformats.py:49` conversion -> integrations (`registry`)

### a conversion drives a wrapped tool or provider (`tools`); the call goes through an integration contract below conversion

- `migkit/storedcode/programs.py:43` conversion -> integrations (`tools`)

### a conversion drives a wrapped tool or provider (`tooluse`); the call goes through an integration contract below conversion

- `migkit/storedcode/appscan.py:449` conversion -> integrations (`tooluse`)
- `migkit/storedcode/indexadvice.py:242` conversion -> integrations (`tooluse`)
- `migkit/storedcode/programs.py:220` conversion -> integrations (`tooluse`)

### a conversion reads an engine (`engines.base`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/charsets.py:261` conversion -> engines (`engines.base`)
- `migkit/pieces.py:581` conversion -> engines (`engines.base`)
- `migkit/schemaformats.py:1079` conversion -> engines (`engines.base`)
- `migkit/storedcode/builtins.py:378` conversion -> engines (`engines.base`)
- `migkit/storedcode/collations.py:1410` conversion -> engines (`engines.base`)
- `migkit/storedcode/constraints.py:2363` conversion -> engines (`engines.base`)
- `migkit/storedcode/convert.py:1084` conversion -> engines (`engines.base`)
- `migkit/storedcode/partitions.py:250` conversion -> engines (`engines.base`)
- `migkit/storedcode/prove.py:306` conversion -> engines (`engines.base`)
- `migkit/storedcode/reference.py:114` conversion -> engines (`engines.base`)
- `migkit/storedcode/sizing.py:220` conversion -> engines (`engines.base`)
- `migkit/storedcode/statements.py:885` conversion -> engines (`engines.base`)
- `migkit/zones.py:39` conversion -> engines (`engines.base`)

### a conversion reads an engine (`engines.dbapi`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/pieces.py:741` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/constraints.py:952` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/corpus.py:253` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/history.py:337` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/packages.py:30` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/partitions.py:51` conversion -> engines (`engines.dbapi`)
- `migkit/storedcode/sandbox.py:159` conversion -> engines (`engines.dbapi`)

### a conversion reads an engine (`engines.hetero`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/charsets.py:316` conversion -> engines (`engines.hetero`)
- `migkit/storedcode/sandbox.py:314` conversion -> engines (`engines.hetero`)

### a conversion reads an engine (`engines.mssql`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/storedcode/corpus.py:148` conversion -> engines (`engines.mssql`)

### a conversion reads an engine (`engines.oracle`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/storedcode/constraints.py:1012` conversion -> engines (`engines.oracle`)
- `migkit/storedcode/packages.py:31` conversion -> engines (`engines.oracle`)

### a conversion reads an engine (`engines`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/canon.py:359` conversion -> engines (`engines`)
- `migkit/charsets.py:234` conversion -> engines (`engines`)
- `migkit/commercial.py:84` conversion -> engines (`engines`)
- `migkit/storedcode/sandbox.py:313` conversion -> engines (`engines`)

### a conversion reads an engine (`logddl`) for its catalogue or connection; the engine contract moves below conversion in the engine split

- `migkit/ddl.py:76` conversion -> engines (`logddl`)

### a durable record uses the executor's `attempts`; the shared piece moves below persistence

- `migkit/claims.py:496` persistence -> execution (`attempts`)

### a durable record uses the executor's `failpoint`; the shared piece moves below persistence

- `migkit/claims.py:49` persistence -> execution (`failpoint`)

### a durable record uses the executor's `locks`; the shared piece moves below persistence

- `migkit/revert.py:30` persistence -> execution (`locks`)

### a durable record uses the executor's `ranges`; the shared piece moves below persistence

- `migkit/checkpoint.py:388` persistence -> execution (`ranges`)
- `migkit/claims.py:417` persistence -> execution (`ranges`)

### a durable record uses the executor's `util`; the shared piece moves below persistence

- `migkit/digests.py:159` persistence -> execution (`util`)
- `migkit/runlog.py:49` persistence -> execution (`util`)

### a durable write goes through safety's `egress`; the order of the ranks is to be settled with the safety package

- `migkit/anchor.py:391` persistence -> safety (`egress`)
- `migkit/runlog.py:91` persistence -> safety (`egress`)

### a durable write goes through safety's `fencing`; the order of the ranks is to be settled with the safety package

- `migkit/claims.py:49` persistence -> safety (`fencing`)

### a durable write goes through safety's `lease`; the order of the ranks is to be settled with the safety package

- `migkit/claims.py:201` persistence -> safety (`lease`)
- `migkit/setaside.py:69` persistence -> safety (`lease`)

### a durable write goes through safety's `redact`; the order of the ranks is to be settled with the safety package

- `migkit/anchor.py:221` persistence -> safety (`redact`)

### a forwarder asks the decision engine (`decide.cloud_forwarder`) which way in to take; the choice is handed in as a fact

- `migkit/tunnel.py:449` execution -> decision (`decide`)

### a hop is checked against the engines (`engines.base`) it names; ends when the engine registry is read through a contract

- `migkit/hopkeys.py:2437` configuration -> engines (`engines.base`)

### a hop is checked against the engines (`engines.saas`) it names; ends when the engine registry is read through a contract

- `migkit/addresses.py:163` configuration -> engines (`engines.saas`)

### a hop is checked against the engines (`engines`) it names; ends when the engine registry is read through a contract

- `migkit/addresses.py:92` configuration -> engines (`engines`)
- `migkit/config.py:9` configuration -> engines (`engines`)
- `migkit/engineat.py:393` configuration -> engines (`engines`)
- `migkit/hopkeys.py:1991` configuration -> engines (`engines`)
- `migkit/managed.py:190` configuration -> engines (`engines`)

### a hop is checked by safety's `egress`; the check moves to the key table

- `migkit/hopkeys.py:2407` configuration -> safety (`egress`)

### a hop is checked by safety's `freeze`; the check moves to the key table

- `migkit/hopkeys.py:1954` configuration -> safety (`freeze`)
- `migkit/managed.py:444` configuration -> safety (`freeze`)

### a hop is checked by safety's `interlock`; the check moves to the key table

- `migkit/hopkeys.py:2925` configuration -> safety (`interlock`)

### a hop is checked by safety's `leftovers`; the check moves to the key table

- `migkit/config.py:1359` configuration -> safety (`leftovers`)

### a hop is checked by safety's `tls`; the check moves to the key table

- `migkit/config.py:484` configuration -> safety (`tls`)

### a hop key's check or default asks `archive` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:3014` configuration -> workflows (`archive`)

### a hop key's check or default asks `consolidate` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:3014` configuration -> workflows (`consolidate`)

### a hop key's check or default asks `cutover` what it accepts; ends when the key table is read from the owner

- `migkit/managed.py:970` configuration -> workflows (`cutover`)

### a hop key's check or default asks `physical` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:2368` configuration -> workflows (`physical`)

### a hop key's check or default asks `reshard` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:3014` configuration -> workflows (`reshard`)

### a hop key's check or default asks `split` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:3014` configuration -> workflows (`split`)

### a hop key's check or default asks `subset` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:2358` configuration -> workflows (`subset`)

### a hop key's check or default asks `tableengine` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:3014` configuration -> workflows (`tableengine`)

### a hop key's check or default asks `tailtables` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:2452` configuration -> workflows (`tailtables`)

### a hop key's check or default asks `twoway` what it accepts; ends when the key table is read from the owner

- `migkit/hopkeys.py:2423` configuration -> workflows (`twoway`)

### a hop option is explained by the decision engine (`dba`); the explanation moves to the key table

- `migkit/managed.py:963` configuration -> decision (`dba`)

### a hop option is explained by the decision engine (`decide`); the explanation moves to the key table

- `migkit/engineat.py:453` configuration -> decision (`decide`)
- `migkit/managed.py:441` configuration -> decision (`decide`)

### a hop option is explained by the decision engine (`machine`); the explanation moves to the key table

- `migkit/hopkeys.py:3130` configuration -> decision (`machine`)

### a hop option is explained by the decision engine (`ready`); the explanation moves to the key table

- `migkit/managed.py:926` configuration -> decision (`ready`)

### a hop option is explained by the decision engine (`scenarios`); the explanation moves to the key table

- `migkit/hopkeys.py:3014` configuration -> decision (`scenarios`)

### a hop option is explained by the decision engine (`sourcechange`); the explanation moves to the key table

- `migkit/hopkeys.py:3179` configuration -> decision (`sourcechange`)

### a record is rendered by conversion's `canon`; the rendering moves below persistence

- `migkit/digests.py:47` persistence -> conversion (`canon`)
- `migkit/kept.py:78` persistence -> conversion (`canon`)
- `migkit/repairlog.py:221` persistence -> conversion (`canon`)

### a record is rendered by conversion's `render`; the rendering moves below persistence

- `migkit/kept.py:288` persistence -> conversion (`render`)

### a record is rendered by conversion's `rowtext`; the rendering moves below persistence

- `migkit/digests.py:47` persistence -> conversion (`rowtext`)
- `migkit/kept.py:304` persistence -> conversion (`rowtext`)

### a record is written in the shape `fleet` gives it; the shape moves below persistence

- `migkit/claims.py:544` persistence -> workflows (`fleet`)

### a record is written in the shape `ha` gives it; the shape moves below persistence

- `migkit/claims.py:200` persistence -> workflows (`ha`)
- `migkit/state.py:613` persistence -> workflows (`ha`)

### a record is written in the shape `indexes` gives it; the shape moves below persistence

- `migkit/setaside.py:125` persistence -> workflows (`indexes`)

### a record is written in the shape `leaves` gives it; the shape moves below persistence

- `migkit/kept.py:78` persistence -> workflows (`leaves`)

### a record is written in the shape `tailctl` gives it; the shape moves below persistence

- `migkit/kept.py:847` persistence -> workflows (`tailctl`)
- `migkit/setaside.py:87` persistence -> workflows (`tailctl`)
- `migkit/state.py:617` persistence -> workflows (`tailctl`)

### a record is written in the shape `twoway` gives it; the shape moves below persistence

- `migkit/kept.py:258` persistence -> workflows (`twoway`)

### a record keeps what the decision engine (`decide`) measured; the fact is handed down

- `migkit/session.py:159` persistence -> decision (`decide`)
- `migkit/shared.py:345` persistence -> decision (`decide`)

### a record keeps what the decision engine (`model`) measured; the fact is handed down

- `migkit/claims.py:369` persistence -> decision (`model`)

### a record keeps what the decision engine (`sizing`) measured; the fact is handed down

- `migkit/claims.py:392` persistence -> decision (`sizing`)

### a record notifies or exports through `export`; the call is inverted into an event

- `migkit/anchor.py:272` persistence -> reporting (`export`)
- `migkit/session.py:231` persistence -> reporting (`export`)

### a record notifies or exports through `notify`; the call is inverted into an event

- `migkit/session.py:135` persistence -> reporting (`notify`)

### a record notifies or exports through `telemetry`; the call is inverted into an event

- `migkit/session.py:159` persistence -> reporting (`telemetry`)

### a record notifies or exports through `traces`; the call is inverted into an event

- `migkit/session.py:131` persistence -> reporting (`traces`)

### a report asks an engine (`engines.base`) for a name or a fact; the engine contract moves below reporting

- `migkit/telemetry.py:423` reporting -> engines (`engines.base`)
- `migkit/verdict.py:30` reporting -> engines (`engines.base`)

### a report asks an engine (`engines`) for a name or a fact; the engine contract moves below reporting

- `migkit/export.py:97` reporting -> engines (`engines`)
- `migkit/report.py:891` reporting -> engines (`engines`)
- `migkit/telemetry.py:146` reporting -> engines (`engines`)
- `migkit/troubleshooting.py:62` reporting -> engines (`engines`)

### a report names a wrapped tool's account (`tools`); the account is read through a contract below reporting

- `migkit/export.py:96` reporting -> integrations (`tools`)

### a report names a wrapped tool's account (`tooluse`); the account is read through a contract below reporting

- `migkit/export.py:96` reporting -> integrations (`tooluse`)
- `migkit/notify.py:644` reporting -> integrations (`tooluse`)
- `migkit/telemetry.py:1642` reporting -> integrations (`tooluse`)
- `migkit/traces.py:287` reporting -> integrations (`tooluse`)

### a report reads `autonomous` for the state of the running workflow; the state moves below reporting

- `migkit/telemetry.py:202` reporting -> workflows (`autonomous`)

### a report reads `ha` for the state of the running workflow; the state moves below reporting

- `migkit/export.py:690` reporting -> workflows (`ha`)

### a report reads `leaves` for the state of the running workflow; the state moves below reporting

- `migkit/export.py:129` reporting -> workflows (`leaves`)

### a report reads `movers` for the state of the running workflow; the state moves below reporting

- `migkit/export.py:129` reporting -> workflows (`movers`)
- `migkit/report.py:890` reporting -> workflows (`movers`)

### a report reads `physical` for the state of the running workflow; the state moves below reporting

- `migkit/export.py:96` reporting -> workflows (`physical`)

### a report reads `tailctl` for the state of the running workflow; the state moves below reporting

- `migkit/notify.py:413` reporting -> workflows (`tailctl`)

### a report reads `twoway` for the state of the running workflow; the state moves below reporting

- `migkit/export.py:617` reporting -> workflows (`twoway`)

### a report states what the decision engine (`capabilities`) chose; the choice is handed down as a fact

- `migkit/diagnostics.py:107` reporting -> decision (`capabilities`)
- `migkit/report.py:890` reporting -> decision (`capabilities`)

### a report states what the decision engine (`decide`) chose; the choice is handed down as a fact

- `migkit/report.py:890` reporting -> decision (`decide`)
- `migkit/telemetry.py:180` reporting -> decision (`decide`)

### a report states what the decision engine (`faults`) chose; the choice is handed down as a fact

- `migkit/telemetry.py:192` reporting -> decision (`faults`)

### a report states what the decision engine (`guarantees`) chose; the choice is handed down as a fact

- `migkit/telemetry.py:186` reporting -> decision (`guarantees`)

### a report states what the decision engine (`sizing`) chose; the choice is handed down as a fact

- `migkit/report.py:926` reporting -> decision (`sizing`)

### a report states what the decision engine (`waysaid`) chose; the choice is handed down as a fact

- `migkit/report.py:890` reporting -> decision (`waysaid`)

### a report states what the decision engine (`workload`) chose; the choice is handed down as a fact

- `migkit/verdict.py:167` reporting -> decision (`workload`)

### a rung or plan reads `consolidate` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/scenarios.py:1333` decision -> workflows (`consolidate`)

### a rung or plan reads `cutover` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/planner.py:786` decision -> workflows (`cutover`)
- `migkit/ready.py:444` decision -> workflows (`cutover`)
- `migkit/repairways.py:381` decision -> workflows (`cutover`)
- `migkit/workload.py:338` decision -> workflows (`cutover`)

### a rung or plan reads `drainmark` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/capabilities.py:1214` decision -> workflows (`drainmark`)

### a rung or plan reads `drift` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/jointplan.py:1267` decision -> workflows (`drift`)

### a rung or plan reads `jobs` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/capabilities.py:1220` decision -> workflows (`jobs`)

### a rung or plan reads `leaves` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/repairways.py:516` decision -> workflows (`leaves`)

### a rung or plan reads `movers` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/bulkways.py:72` decision -> workflows (`movers`)
- `migkit/capabilities.py:630` decision -> workflows (`movers`)
- `migkit/legs.py:182` decision -> workflows (`movers`)
- `migkit/machine.py:467` decision -> workflows (`movers`)
- `migkit/planner.py:64` decision -> workflows (`movers`)
- `migkit/repairways.py:463` decision -> workflows (`movers`)
- `migkit/waysaid.py:22` decision -> workflows (`movers`)

### a rung or plan reads `physical` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/costmodel.py:373` decision -> workflows (`physical`)

### a rung or plan reads `reshard` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/scenarios.py:763` decision -> workflows (`reshard`)
- `migkit/topology.py:19` decision -> workflows (`reshard`)

### a rung or plan reads `subset` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/ready.py:521` decision -> workflows (`subset`)

### a rung or plan reads `tableengine` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/scenarios.py:1248` decision -> workflows (`tableengine`)

### a rung or plan reads `topicstates` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/capabilities.py:1156` decision -> workflows (`topicstates`)

### a rung or plan reads `unchanged` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/scanplan.py:149` decision -> workflows (`unchanged`)

### a rung or plan reads `upgrade` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/lacks.py:174` decision -> workflows (`upgrade`)
- `migkit/ready.py:521` decision -> workflows (`upgrade`)
- `migkit/workload.py:87` decision -> workflows (`upgrade`)

### a rung or plan reads `users` for what the workflow can do; ends when the workflow publishes its abilities below decision

- `migkit/capabilities.py:602` decision -> workflows (`users`)

### a rung reads a wrapped tool's surface (`mirror`); the surface is a fact handed down

- `migkit/bulkways.py:72` decision -> integrations (`mirror`)

### a rung reads a wrapped tool's surface (`redisshake`); the surface is a fact handed down

- `migkit/bulkways.py:72` decision -> integrations (`redisshake`)

### a rung reads a wrapped tool's surface (`ship`); the surface is a fact handed down

- `migkit/machine.py:988` decision -> integrations (`ship`)

### a safety check reads `barriers` for the step it guards; the step reports its own state down

- `migkit/freeze.py:6903` safety -> workflows (`barriers`)

### a safety check reads `cutover` for the step it guards; the step reports its own state down

- `migkit/approvals.py:246` safety -> workflows (`cutover`)
- `migkit/freeze.py:1006` safety -> workflows (`cutover`)

### a safety check reads `fleet` for the step it guards; the step reports its own state down

- `migkit/lease.py:201` safety -> workflows (`fleet`)

### a safety check reads `tailctl` for the step it guards; the step reports its own state down

- `migkit/approvals.py:246` safety -> workflows (`tailctl`)
- `migkit/freeze.py:3258` safety -> workflows (`tailctl`)

### a safety check reads `users` for the step it guards; the step reports its own state down

- `migkit/freeze.py:107` safety -> workflows (`users`)

### a safety module asks the decision engine (`decide.lift_hold`) whether a hold a run left may be lifted; the holds and their ages are to be handed up as facts by the command

- `migkit/leftovers.py:377` safety -> decision (`decide`)

### a safety refusal is said by reporting's `export`; the wording moves below safety

- `migkit/egress.py:234` safety -> reporting (`export`)

### a safety refusal is said by reporting's `notify`; the wording moves below safety

- `migkit/approvals.py:327` safety -> reporting (`notify`)

### a safety refusal is said by reporting's `verdict`; the wording moves below safety

- `migkit/approvals.py:218` safety -> reporting (`verdict`)

### a safety refusal is said by reporting's `wording`; the wording moves below safety

- `migkit/interlock.py:231` safety -> reporting (`wording`)

### a sampled check folds the drawn rows of both sides through the one folder (`canon.fold_rows`), so the sample compares as the full check does

- `migkit/ranges.py:1914` execution -> conversion (`canon`)

### a scope check asks `editions` what the workflow takes; ends when the workflow publishes it below scope

- `migkit/case.py:123` scope -> workflows (`editions`)

### a scope check asks `physical` what the workflow takes; ends when the workflow publishes it below scope

- `migkit/case.py:335` scope -> workflows (`physical`)

### a scope check asks `upgrade` what the workflow takes; ends when the workflow publishes it below scope

- `migkit/case.py:178` scope -> workflows (`upgrade`)

### a wrapper reports through `crossmove`, the workflow that drives it; the shared piece moves below both

- `migkit/tools.py:561` integrations -> workflows (`crossmove`)

### a wrapper reports through `editions`, the workflow that drives it; the shared piece moves below both

- `migkit/backupseed.py:107` integrations -> workflows (`editions`)

### a wrapper reports through `leaves`, the workflow that drives it; the shared piece moves below both

- `migkit/ship.py:110` integrations -> workflows (`leaves`)

### a wrapper reports through `movers`, the workflow that drives it; the shared piece moves below both

- `migkit/redisshake.py:520` integrations -> workflows (`movers`)
- `migkit/ship.py:2625` integrations -> workflows (`movers`)
- `migkit/tooluse.py:47` integrations -> workflows (`movers`)

### a wrapper reports through `physical`, the workflow that drives it; the shared piece moves below both

- `migkit/backupseed.py:43` integrations -> workflows (`physical`)
- `migkit/cloudseed.py:83` integrations -> workflows (`physical`)

### a wrapper reports through `replicasum`, the workflow that drives it; the shared piece moves below both

- `migkit/tooluse.py:54` integrations -> workflows (`replicasum`)

### a wrapper reports through `second_reader`, the workflow that drives it; the shared piece moves below both

- `migkit/tools.py:550` integrations -> workflows (`second_reader`)

### a wrapper reports through `tailtables`, the workflow that drives it; the shared piece moves below both

- `migkit/imports.py:792` integrations -> workflows (`tailtables`)

### a wrapper reports through `upgrade`, the workflow that drives it; the shared piece moves below both

- `migkit/cloudseed.py:1072` integrations -> workflows (`upgrade`)
- `migkit/tooluse.py:54` integrations -> workflows (`upgrade`)

### a wrapper reports through `verifiers`, the workflow that drives it; the shared piece moves below both

- `migkit/tooluse.py:47` integrations -> workflows (`verifiers`)

### an engine calls `applyway` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:7177` engines -> workflows (`applyway`)

### an engine calls `appversions` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2019` engines -> workflows (`appversions`)
- `migkit/engines/mysql.py:3692` engines -> workflows (`appversions`)
- `migkit/engines/postgres.py:5259` engines -> workflows (`appversions`)

### an engine calls `barriers` for the origin mark its atomic unit carries; the engine split gives the mark a contract below the engines

- `migkit/engines/dynamodb.py:1054` engines -> workflows (`barriers`)
- `migkit/engines/kafka.py:1328` engines -> workflows (`barriers`)
- `migkit/engines/mongodb.py:5349` engines -> workflows (`barriers`)
- `migkit/engines/redis.py:2966` engines -> workflows (`barriers`)

### an engine calls `changetopic` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/firestore.py:932` engines -> workflows (`changetopic`)

### an engine calls `crossmove` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/hetero.py:1972` engines -> workflows (`crossmove`)

### an engine calls `drift` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/hetero.py:4432` engines -> workflows (`drift`)
- `migkit/engines/mssql.py:402` engines -> workflows (`drift`)
- `migkit/engines/mysql.py:3776` engines -> workflows (`drift`)
- `migkit/engines/postgres.py:1358` engines -> workflows (`drift`)

### an engine calls `indexes` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/postgres.py:10267` engines -> workflows (`indexes`)

### an engine calls `leaves` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:4532` engines -> workflows (`leaves`)
- `migkit/engines/hetero.py:780` engines -> workflows (`leaves`)
- `migkit/engines/mongodb.py:242` engines -> workflows (`leaves`)

### an engine calls `marks` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/cassandra.py:2690` engines -> workflows (`marks`)
- `migkit/engines/dynamodb.py:508` engines -> workflows (`marks`)
- `migkit/engines/hetero.py:4067` engines -> workflows (`marks`)
- `migkit/engines/kafka.py:1189` engines -> workflows (`marks`)
- `migkit/engines/mongodb.py:4736` engines -> workflows (`marks`)
- `migkit/engines/mysql.py:1591` engines -> workflows (`marks`)
- `migkit/engines/postgres.py:3027` engines -> workflows (`marks`)
- `migkit/engines/redis.py:2965` engines -> workflows (`marks`)

### an engine calls `movers` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2051` engines -> workflows (`movers`)
- `migkit/engines/cassandra.py:1988` engines -> workflows (`movers`)
- `migkit/engines/generic.py:2051` engines -> workflows (`movers`)
- `migkit/engines/hetero.py:223` engines -> workflows (`movers`)
- `migkit/engines/mongodb.py:892` engines -> workflows (`movers`)
- `migkit/engines/mysql.py:2818` engines -> workflows (`movers`)
- `migkit/engines/pg_pass.py:448` engines -> workflows (`movers`)
- `migkit/engines/postgres.py:1879` engines -> workflows (`movers`)

### an engine calls `physical` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/postgres.py:13044` engines -> workflows (`physical`)

### an engine calls `replicasum` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/mysql.py:6348` engines -> workflows (`replicasum`)

### an engine calls `rules` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2540` engines -> workflows (`rules`)

### an engine calls `second_reader` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2541` engines -> workflows (`second_reader`)
- `migkit/engines/hetero.py:3399` engines -> workflows (`second_reader`)
- `migkit/engines/mysql.py:11540` engines -> workflows (`second_reader`)
- `migkit/engines/postgres.py:7433` engines -> workflows (`second_reader`)

### an engine calls `streamout` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/kafka.py:527` engines -> workflows (`streamout`)

### an engine calls `tableengine` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2491` engines -> workflows (`tableengine`)
- `migkit/engines/mysql.py:523` engines -> workflows (`tableengine`)
- `migkit/engines/postgres.py:1265` engines -> workflows (`tableengine`)

### an engine calls `tailctl` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:2332` engines -> workflows (`tailctl`)
- `migkit/engines/cassandra.py:2853` engines -> workflows (`tailctl`)
- `migkit/engines/generic.py:2348` engines -> workflows (`tailctl`)
- `migkit/engines/hetero.py:3418` engines -> workflows (`tailctl`)
- `migkit/engines/kafka.py:192` engines -> workflows (`tailctl`)
- `migkit/engines/mongodb.py:2748` engines -> workflows (`tailctl`)
- `migkit/engines/mssql.py:4244` engines -> workflows (`tailctl`)
- `migkit/engines/mysql.py:2243` engines -> workflows (`tailctl`)
- `migkit/engines/neo4j.py:95` engines -> workflows (`tailctl`)
- `migkit/engines/oracle.py:2440` engines -> workflows (`tailctl`)
- `migkit/engines/postgres.py:3621` engines -> workflows (`tailctl`)
- `migkit/engines/redis.py:24` engines -> workflows (`tailctl`)

### an engine calls `tailtables` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/hetero.py:6` engines -> workflows (`tailtables`)
- `migkit/engines/mongodb.py:4469` engines -> workflows (`tailtables`)
- `migkit/engines/postgres.py:4436` engines -> workflows (`tailtables`)

### an engine calls `topicstates` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/kafka.py:1189` engines -> workflows (`topicstates`)

### an engine calls `twoway` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/base.py:6742` engines -> workflows (`twoway`)
- `migkit/engines/dynamodb.py:934` engines -> workflows (`twoway`)
- `migkit/engines/hetero.py:4034` engines -> workflows (`twoway`)
- `migkit/engines/mysql.py:1591` engines -> workflows (`twoway`)
- `migkit/engines/postgres.py:3027` engines -> workflows (`twoway`)

### an engine calls `unchanged` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/mssql.py:778` engines -> workflows (`unchanged`)
- `migkit/engines/mysql.py:4427` engines -> workflows (`unchanged`)
- `migkit/engines/postgres.py:6469` engines -> workflows (`unchanged`)

### an engine calls `upgrade` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/mysql.py:504` engines -> workflows (`upgrade`)
- `migkit/engines/postgres.py:1224` engines -> workflows (`upgrade`)

### an engine calls `users` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/cassandra.py:1213` engines -> workflows (`users`)
- `migkit/engines/kafka.py:2152` engines -> workflows (`users`)
- `migkit/engines/mongodb.py:3703` engines -> workflows (`users`)
- `migkit/engines/mssql.py:2162` engines -> workflows (`users`)
- `migkit/engines/mysql.py:7706` engines -> workflows (`users`)
- `migkit/engines/postgres.py:10124` engines -> workflows (`users`)
- `migkit/engines/redis.py:291` engines -> workflows (`users`)

### an engine calls `verifiers` for a step it cannot do itself; the engine split gives that step a contract below the engines

- `migkit/engines/cassandra.py:495` engines -> workflows (`verifiers`)
- `migkit/engines/distmysql.py:200` engines -> workflows (`verifiers`)
- `migkit/engines/dynamodb.py:552` engines -> workflows (`verifiers`)
- `migkit/engines/firestore.py:519` engines -> workflows (`verifiers`)
- `migkit/engines/generic.py:151` engines -> workflows (`verifiers`)
- `migkit/engines/hetero.py:5034` engines -> workflows (`verifiers`)
- `migkit/engines/kafka.py:2400` engines -> workflows (`verifiers`)
- `migkit/engines/mysql.py:4197` engines -> workflows (`verifiers`)
- `migkit/engines/oceanbase.py:237` engines -> workflows (`verifiers`)

### an executor asks the decision engine (`decide`) how many or how fast; the call becomes a fact handed down

- `migkit/attempts.py:786` execution -> decision (`decide`)
- `migkit/ranges.py:1098` execution -> decision (`decide`)
- `migkit/runner.py:55` execution -> decision (`decide`)
- `migkit/util.py:663` execution -> decision (`decide`)

### an executor asks the decision engine (`faults`) how many or how fast; the call becomes a fact handed down

- `migkit/failpoint.py:53` execution -> decision (`faults`)
- `migkit/util.py:178` execution -> decision (`faults`)

### an executor asks the decision engine (`sizing`) how many or how fast; the call becomes a fact handed down

- `migkit/ranges.py:58` execution -> decision (`sizing`)

### an executor asks the decision engine (`tuning`) how many or how fast; the call becomes a fact handed down

- `migkit/ranges.py:1085` execution -> decision (`tuning`)
- `migkit/throttle.py:53` execution -> decision (`tuning`)

### an executor calls `fleet` for a workflow step; the step is passed in as a hook

- `migkit/attempts.py:317` execution -> workflows (`fleet`)
- `migkit/ranges.py:707` execution -> workflows (`fleet`)

### an executor calls `leaves` for a workflow step; the step is passed in as a hook

- `migkit/ranges.py:1179` execution -> workflows (`leaves`)

### an executor calls `unchanged` for a workflow step; the step is passed in as a hook

- `migkit/ranges.py:1582` execution -> workflows (`unchanged`)

### an executor says its progress through `notify`; the notice goes through an event below execution

- `migkit/schedule.py:280` execution -> reporting (`notify`)

### an executor says its progress through `verdict`; the notice goes through an event below execution

- `migkit/schedule.py:64` execution -> reporting (`verdict`)

### an executor says its progress through `wording`; the notice goes through an event below execution

- `migkit/runner.py:122` execution -> reporting (`wording`)
- `migkit/util.py:281` execution -> reporting (`wording`)

### configuration reaches `audit` (persistence); the shared piece moves below both, or the call is inverted

- `migkit/managed.py:766` configuration -> persistence (`audit`)

### configuration reaches `carriers` (integrations); the shared piece moves below both, or the call is inverted

- `migkit/hopkeys.py:3130` configuration -> integrations (`carriers`)

### configuration reaches `charsets` (conversion); the shared piece moves below both, or the call is inverted

- `migkit/hopkeys.py:3014` configuration -> conversion (`charsets`)

### configuration reaches `cloudapi` (integrations); the shared piece moves below both, or the call is inverted

- `migkit/managed.py:647` configuration -> integrations (`cloudapi`)

### configuration reaches `durable` (persistence); the shared piece moves below both, or the call is inverted

- `migkit/config.py:87` configuration -> persistence (`durable`)

### configuration reaches `evidence` (persistence); the shared piece moves below both, or the call is inverted

- `migkit/config.py:1315` configuration -> persistence (`evidence`)

### configuration reaches `guide` (cli); the shared piece moves below both, or the call is inverted

- `migkit/managed.py:272` configuration -> cli (`guide`)

### configuration reaches `rowfilter` (scope); the shared piece moves below both, or the call is inverted

- `migkit/hopkeys.py:2578` configuration -> scope (`rowfilter`)

### configuration reaches `transforms` (conversion); the shared piece moves below both, or the call is inverted

- `migkit/config.py:1700` configuration -> conversion (`transforms`)
- `migkit/hopkeys.py:2639` configuration -> conversion (`transforms`)

### configuration reaches `util` (execution); the shared piece moves below both, or the call is inverted

- `migkit/config.py:1428` configuration -> execution (`util`)

### configuration reaches `verdict` (reporting); the shared piece moves below both, or the call is inverted

- `migkit/config.py:417` configuration -> reporting (`verdict`)

### configuration reaches `web.roles` (web); the shared piece moves below both, or the call is inverted

- `migkit/hopkeys.py:2201` configuration -> web (`web.roles`)

### engines reaches `cli` (cli); the shared piece moves below both, or the call is inverted

- `migkit/engines/base.py:7958` engines -> cli (`cli`)
- `migkit/engines/oracle_pump.py:227` engines -> cli (`cli`)

### every provider call waits on the egress gate (`cloudapi.gate`, `egress.decide`) before its client is made; the gate moves to a contract below persistence

- `migkit/anchor.py:130` persistence -> integrations (`cloudapi`)
- `migkit/dba.py:1141` decision -> integrations (`cloudapi`)
- `migkit/freeze.py:6440` safety -> integrations (`cloudapi`)
- `migkit/machine.py:620` decision -> integrations (`cloudapi`)
- `migkit/state.py:333` persistence -> integrations (`cloudapi`)
- `migkit/variants.py:503` configuration -> integrations (`cloudapi`)
- `migkit/variants.py:503` configuration -> safety (`egress`)

### execution reaches `engines` (engines); the shared piece moves below both, or the call is inverted

- `migkit/attempts.py:221` execution -> engines (`engines`)
- `migkit/util.py:1279` execution -> engines (`engines`)

### execution reaches `tools` (integrations); the shared piece moves below both, or the call is inverted

- `migkit/runner.py:116` execution -> integrations (`tools`)

### execution reaches `toolversion` (integrations); the shared piece moves below both, or the call is inverted

- `migkit/runner.py:562` execution -> integrations (`toolversion`)

### integrations reaches `cli` (cli); the shared piece moves below both, or the call is inverted

- `migkit/integrations.py:57` integrations -> cli (`cli`)
- `migkit/ship.py:790` integrations -> cli (`cli`)

### integrations reaches `web.uses` (web); the shared piece moves below both, or the call is inverted

- `migkit/tooluse.py:58` integrations -> web (`web.uses`)

### persistence reaches `engines.base` (engines); the shared piece moves below both, or the call is inverted

- `migkit/kept.py:984` persistence -> engines (`engines.base`)

### persistence reaches `engines` (engines); the shared piece moves below both, or the call is inverted

- `migkit/shared.py:323` persistence -> engines (`engines`)

### persistence reaches `tooluse` (integrations); the shared piece moves below both, or the call is inverted

- `migkit/anchor.py:712` persistence -> integrations (`tooluse`)
- `migkit/shared.py:513` persistence -> integrations (`tooluse`)

### reporting reaches `cli` (cli); the shared piece moves below both, or the call is inverted

- `migkit/telemetry.py:151` reporting -> cli (`cli`)

### safety reaches `decide` (decision); the shared piece moves below both, or the call is inverted

- `migkit/freeze.py:665` safety -> decision (`decide`)

### safety reaches `engines.dbapi` (engines); the shared piece moves below both, or the call is inverted

- `migkit/freeze.py:4656` safety -> engines (`engines.dbapi`)

### safety reaches `engines` (engines); the shared piece moves below both, or the call is inverted

- `migkit/freeze.py:176` safety -> engines (`engines`)

### safety reaches `sourcechange` (decision); the shared piece moves below both, or the call is inverted

- `migkit/freeze.py:957` safety -> decision (`sourcechange`)

### safety reaches `web.approve` (web); the shared piece moves below both, or the call is inverted

- `migkit/approvals.py:315` safety -> web (`web.approve`)

### scope reaches `interlock` (safety); the shared piece moves below both, or the call is inverted

- `migkit/case.py:77` scope -> safety (`interlock`)

### scope reaches `lacks` (decision); the shared piece moves below both, or the call is inverted

- `migkit/case.py:30` scope -> decision (`lacks`)

### scope reaches `leftovers` (safety); the shared piece moves below both, or the call is inverted

- `migkit/result_scope.py:186` scope -> safety (`leftovers`)

### scope reaches `util` (execution); the shared piece moves below both, or the call is inverted

- `migkit/rowfilter.py:55` scope -> execution (`util`)

### scope reaches `verdict` (reporting); the shared piece moves below both, or the call is inverted

- `migkit/result_scope.py:405` scope -> reporting (`verdict`)

### the anchor's interval is a rung (`decide.anchor_interval`) on the measured entry rate; the anchor hands the rate in as a fact

- `migkit/anchor.py:452` persistence -> decision (`decide`)

### the decision engine asks an engine (`engines.base`) what it can do; ends when the engine contract is a package below decision

- `migkit/capabilities.py:34` decision -> engines (`engines.base`)
- `migkit/decide.py:2778` decision -> engines (`engines.base`)
- `migkit/model.py:1111` decision -> engines (`engines.base`)
- `migkit/scenarios.py:505` decision -> engines (`engines.base`)
- `migkit/workload.py:133` decision -> engines (`engines.base`)

### the decision engine asks an engine (`engines.dbapi`) what it can do; ends when the engine contract is a package below decision

- `migkit/dba.py:725` decision -> engines (`engines.dbapi`)
- `migkit/lacks.py:583` decision -> engines (`engines.dbapi`)

### the decision engine asks an engine (`engines.hetero`) what it can do; ends when the engine contract is a package below decision

- `migkit/capabilities.py:976` decision -> engines (`engines.hetero`)

### the decision engine asks an engine (`engines.mysql`) what it can do; ends when the engine contract is a package below decision

- `migkit/costmodel.py:352` decision -> engines (`engines.mysql`)
- `migkit/dba.py:525` decision -> engines (`engines.mysql`)

### the decision engine asks an engine (`engines.oracle_logminer`) what it can do; ends when the engine contract is a package below decision

- `migkit/dba.py:883` decision -> engines (`engines.oracle_logminer`)

### the decision engine asks an engine (`engines.oracle_redo`) what it can do; ends when the engine contract is a package below decision

- `migkit/dba.py:991` decision -> engines (`engines.oracle_redo`)

### the decision engine asks an engine (`engines.pg_pass`) what it can do; ends when the engine contract is a package below decision

- `migkit/model.py:320` decision -> engines (`engines.pg_pass`)

### the decision engine asks an engine (`engines.postgres`) what it can do; ends when the engine contract is a package below decision

- `migkit/legs.py:141` decision -> engines (`engines.postgres`)

### the decision engine asks an engine (`engines.sapodp`) what it can do; ends when the engine contract is a package below decision

- `migkit/ready.py:509` decision -> engines (`engines.sapodp`)

### the decision engine asks an engine (`engines`) what it can do; ends when the engine contract is a package below decision

- `migkit/capabilities.py:33` decision -> engines (`engines`)
- `migkit/dba.py:311` decision -> engines (`engines`)
- `migkit/decide.py:2777` decision -> engines (`engines`)
- `migkit/machine.py:948` decision -> engines (`engines`)
- `migkit/tableway.py:132` decision -> engines (`engines`)
- `migkit/waysaid.py:71` decision -> engines (`engines`)

### the deep check compares Agent jobs through `jobs.shapes`; the job shapes move below engines

- `migkit/engines/mssql.py:2076` engines -> workflows (`jobs`)

### the fence's way and place are rungs (`decide.fence_way`, `decide.fence_place`) on the account's rights the engine reads; a finished range is named a ledger unit through the freeze's writer; a broker's transaction start is bounded by the lease's own term

- `migkit/claims.py:544` persistence -> safety (`freeze`)
- `migkit/engines/kafka.py:725` engines -> workflows (`ha`)
- `migkit/fencing.py:551` safety -> decision (`decide`)

### the hop check refuses prefixes that meet through `topology.overlaps`; the key spaces move below configuration

- `migkit/hopkeys.py:3093` configuration -> decision (`topology`)

### the hop's sign-in and the readiness audit ask the cloud provider's own API (`cloudapi`); the provider calls move to a contract below configuration and decision

- `migkit/cloud_readiness.py:232` decision -> integrations (`cloudapi`)
- `migkit/config.py:904` configuration -> integrations (`cloudapi`)

### workflows reaches `cli` (cli); the shared piece moves below both, or the call is inverted

- `migkit/ha.py:100` workflows -> cli (`cli`)
- `migkit/tailtables.py:1067` workflows -> cli (`cli`)
