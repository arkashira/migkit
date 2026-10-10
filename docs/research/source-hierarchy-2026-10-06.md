# Source hierarchy plan

Status: proposed, not implemented. Researched 2026-10-06. This work must not interrupt active validation.

## Problem and evidence

The current package has 248 Python files, including 149 directly under migkit/. PostgreSQL has 12,976 physical lines and MySQL 9,173. Counts include blank lines and comments. Navigation mixes lifecycle work, engine details, execution infrastructure and presentation. File size is a review signal, not a mandatory split threshold.

## Open-source references

Direct GitHub repository directory listings were inspected, including GitHub Contents API when tree pages could not be fetched. These observations establish layout, not architectural quality or performance.

| Project | Observed hierarchy | Apply to migkit | Do not copy blindly |
|---|---|---|---|
| [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy/tree/main/lib/sqlalchemy) | engine, dialects, sql, pool, orm, testing | Keep common engine contracts distinct from engine-specific implementations; organize by responsibility | Its ORM and SQL expression hierarchy do not match migration workflows |
| [Prefect](https://github.com/PrefectHQ/prefect/tree/main/src/prefect) | cli, client, server, concurrency, workers, runner, runtime, settings, events, testing | Make entry points, execution and resource control discoverable | Do not adopt its many product-specific packages or create distributed services for folder symmetry |
| [Airbyte](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations) | bases, connectors, db-harness-lib, developer-docs, scripts; repository also separates CDK and integrations | Separate shared contracts from concrete integrations and reusable test harnesses | Do not reproduce a large multi-language monorepo or split every engine into a separately released package |
| [dbt-core](https://github.com/dbt-labs/dbt-core) | Current root has crates and lib; the previously familiar core/dbt path was unavailable | Verify current source layouts and choose relevant versions before using examples | The current root is not evidence for the historical Python hierarchy; no unsupported comparison |

## Proposed hierarchy

Keep the current package root initially. A src/ layout is a separate packaging decision after the internal move is stable.

```text
migkit/
  __init__.py
  cli/                commands and entry-point composition
  configuration/      hop parsing, options, environment keys, endpoint configuration
  scope/              authoritative selection, mapping and scope identity
  decision/           facts, strategy choice, cost, joint planning, prioritization
  execution/          scheduling, resources, pressure, retries, owned processes
  workflows/          assessment, move, replication, validation, cutover, recovery
  engines/            common contracts and engine-specific adapter packages
    contracts.py
    registry.py
    postgres/         connection, catalog, schema, validation, copy, replication
    mysql/            connection, catalog, schema, validation, copy, replication
    ...
  conversion/         types, canonical values, filters, transforms, stored code
  integrations/       wrapped programs and provider-specific adapters
  persistence/        state, checkpoints, durable evidence, undo and audit storage
  safety/             authorization, fencing, guarantees, interlocks, secrets
  reporting/          result model, verdict, rendering and diagnostics
  web/                web entry points and presentation
  data/               shipped package resources

tests/
  unit/               mirrors the owning package
  integration/        engine and integration contracts
  workflows/          complete migration scenarios and failure recovery
  fixtures/           reusable sandbox servers and data generation

docs/
  architecture/       hierarchy, dependency rules, ownership and navigation
  guides/             operator and contributor guides
  research/           dated source-grounded research
  agents/             generated code/state maps
  backlog.md
  backlog-index.md
```

These are proposed ownership groups, not instructions to create empty directories. Reuse existing storedcode, runners and web packages where their contracts fit; change their location only with the complete mapping. Start with existing real responsibility clusters and avoid generic core, common, helpers and utils dumping grounds.

## Initial file mapping

| Current files | Proposed owner | Required clarification |
|---|---|---|
| config, hopkeys, envkeys, addresses | configuration | Hop selection semantics belong to scope; parsing remains configuration |
| decide, costmodel, jointplan, factbook | decision | One strategy authority; scheduling executes decisions |
| runner, schedule, throttle, sizing, machine, stall | execution | Shared nested resource budgets and ownership, not duplicated pools |
| movers, cutover, tailctl, verifiers | workflows | Separate workflow coordination from engine SQL and integration commands |
| engines/base, engines registry | engines contracts and registry | Extract by concrete responsibilities; avoid replacing one giant base with a generic giant core |
| engines/postgres, engines/mysql | per-engine packages | Stable facade; explicit collaborators rather than a large collection of mixins |
| canon, rowfilter, transforms, storedcode | conversion | Scope owns selection; conversion owns value transformations |
| tools, tooluse, toolversion, cloudapi, cloudseed, imports | integrations | Task importers and provider clients need named subpackages once dependency inventory confirms boundaries |
| state, checkpoint, durable, repairlog, revert, audit | persistence | Workflows own recovery policy; persistence owns durable records and replay contracts |
| approvals, interlock, fencing, redact, egress, tls | safety | Transport may own connection mechanics; security policy remains authoritative |
| model, verdict, evidence, report, render, diagnostics | reporting and explicit domain owners | Split mixed model responsibilities only after caller/type inventory; evidence storage belongs to persistence |
| cli, ui, web | cli and web | Thin entry points use workflow interfaces |

Every tracked source file must appear in a machine-checkable old-to-new map before moves begin. Resolve ambiguous files from their callers and responsibilities, not their names alone.

## Dependency and navigation rules

- Entry points call workflows. Workflows compose decision, execution, engine contracts, persistence and reporting through explicit interfaces.
- Decision selects methods from measured facts and guarantees; it does not import CLI/web or perform engine I/O while selecting. Bounded probes execute through the workflow/execution layer.
- Engine and integration adapters implement contracts. They do not import presentation or start nested unmanaged worker pools.
- Shared scope and result/verdict rules retain one owner. Moving files must not create alternative readers.
- Small stable contract/type modules are placed with their owner. No new generic dependency hub.
- Record and enforce permitted package dependencies; no new import cycles. Historical cycles need an explicit removal plan.
- Keep normal browsing shallow: responsibility, then engine or capability, then implementation. Additional nesting requires a concrete purpose.
- Maintain a generated code map and a contributor index: where to change validation, movement, decision, configuration, results and one engine.

## Rollout and acceptance

1. Inventory every tracked source file, imports, dynamic imports, registries, package resources, subprocess entry points and tests. Capture existing cycles and installed entry points. Finalize the complete mapping and dependency rules.
2. Move a small coherent existing cluster with explicit compatibility imports that preserve type identity. Do not mix behavioral changes with mechanical moves. Mark compatibility paths and removal conditions.
3. Extract PostgreSQL/MySQL responsibility modules behind existing facades. Preserve connections, transactions, checkpoints, ordering and engine contracts. Avoid arbitrary line-count cuts and excessive mixins.
4. Migrate remaining packages and test locations by ownership. Update packaging, resources, CLI entry points, generated docs/code maps and navigation links. Address src/ layout separately with installed-wheel proof.
5. Remove internal use of compatibility imports once every caller is migrated. Run the repository-required final suite once at the agreed campaign end; prove installed wheel CLI, resources and representative sandbox workflows.

Acceptance: every source file has a justified owner; navigation exercises find the implementation for representative tasks within two minutes; no new cycles or duplicated rules; public CLI/configuration/result contracts preserved; checkpoint and resume compatibility proved; no customer data introduced; measured runtime/import regressions investigated. A tidy tree alone does not satisfy completion. Do not move running validation code until its process and evidence are safely complete.
