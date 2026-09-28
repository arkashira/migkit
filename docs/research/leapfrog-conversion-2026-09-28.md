# Leapfrogging the paid converters: conversion as search plus proof (research as of 2026-09-28)

## Status
- **complete** (2026-09-28): sections 0 scope, 1 thesis, 2 search + proof (CEGIS, verified lifting, test-driven repair, cross-engine differential testing, symbolic execution, reduction), 3 emulation layers, 4 application SQL and the workload corpus, 5 schema design beyond 1:1 with constraint proofs, 6 learning rules from fixes, 7 the leap design (P1-P21 with licence, effort, docker), 8 capability table, status caveats, sources.
- partial: none. Not yet: none. Points not confirmed from a primary page are listed under "Status caveats" before the sources and marked *(to be confirmed / to be checked / not found)* in place.
- Public research only; no company data. Local disk read-only except this file.

## 0. Scope and what this file does not repeat

The owner's ask: in schema and stored-code conversion migkit is behind nearly every paid converter (AWS DMS Schema Conversion + generative AI, Google DMS conversion workspaces with Gemini, SSMA, EDB, Ispirer, SQLines, SnowConvert, Datafold, Huawei UGO). The goal here is not to list what to copy but to find mechanisms that put migkit **past** them.

Already written up, not repeated here (read them first):
- `stored-code-conversion-2026-09-28.md`: what every converter does and how it checks (s.1, s.5), the silent-difference rule tables per pair (s.2), PARROT / CrackSQL / RISE / Horizon / SQL-ProcBench (s.3), equivalence provers Cosette / SPES / SQLSolver / QED / VeriEQL, XData, EvoSQL, SQLancer, coverage tools per engine, Hypothesis, determinism pinning (s.4), the R11 rung ladder and harness (s.6).
- `paid-cloud-products-2026-09-28.md`: s.2.9 Datafold DMA, s.2.10 SQLines, s.2.12 Ispirer, s.2.13 EDB, s.2.19 DMS SC, s.3 the conversion-depth table and the three-class residue, s.5 gaps 1-5 and 12.
- `docs/backlog.md`: R11 (`:4325`), F3 (`:2588`), W3 (`:2687`).

This file adds: the search-and-proof literature beyond SQL (CEGIS, verified lifting, test-driven translation repair), cross-engine differential testing, symbolic execution of database code, program reduction, rule learning from fixes, emulation layers as a *candidate* rather than a product choice, application SQL and real workloads as the corpus, schema design beyond 1:1 with a constraint proof, and one design that combines them.

---

## 1. Thesis: where the paid converters are structurally stuck, and the lever each one leaves

Read across the two earlier reports, every paid converter has the same shape: a **vendor-owned rule base** (grown by the vendor's engineers - Ispirer "3-5 business days", EDB `ERH-nnnn` handlers, AWS action items), a **model for the residue** (Bedrock, Gemini, Copilot, SnowConvert AI), and a **weak check** (syntax for AWS, compile for EDB/UGO/MOLT, a model grading a model for Google). The three that execute (SSMA Tester, SnowConvert, Datafold DMA) either make the user type the inputs, write to the source, or sell the loop as supervised service. None of them can close the gap by adding rules, because the constraint is not the rules. It is that:

| Structural limit of the paid tools | Why they cannot easily remove it | The lever migkit has instead |
|---|---|---|
| **One candidate per object**, produced by a fixed pipeline (rules, then model) | the pipeline *is* the product; a second generator is a competitor's product | migkit is a harness, not a converter: it can run *every* open generator (sqlglot, Ora2Pg, SQLines, its own rules, emulation, a model) on the same object and let the proof choose (section 7) |
| **No oracle**: AWS "doesn't validate semantic correctness"; Google asks Gemini | they run in the target cloud without the source engine and without the data | migkit already runs both engines in docker and already moves and proves data; the **source engine on a proven-equal fixture is the oracle** (the VERT idea, section 2.2) |
| **Inputs typed or model-written** (SSMA Tester call values; Amazon Q tests "vary between runs") | no access to the routine's real call history, no coverage instrument | inputs from the source's **own call history** (statement digests, Query Store, bind capture - section 4.2), from constants in the body, from coverage-guided search, from constraint-violation generation (section 5.4) |
| **Failures reported as a diff of a whole object** | no reducer | **two-sided reduction**: the input shrunk (Hypothesis) *and* the program shrunk (delta debugging over statements) to a minimal repro - the thing a rule learner and a model need (section 2.6) |
| **Rule base closed**; per-customer rules by vendor staff | the rule base is the moat | rules as **data with proof attached**, learned from accepted fixes by anti-unification (section 6), re-proved on a public fixture corpus on every change - the moat becomes a shared, testable asset |
| **Emulation is a product decision** (EDB -> EPAS, Babelfish -> Aurora, extension packs always installed) | each vendor sells one target | emulation as **one more candidate per object**, kept only where it proves equal and measures cheaper (section 3) |
| **Application SQL scanned statically** (SCT) or not at all | no workload, no second engine | the application's **real statements replayed on both engines** and compared (Percona `pt-upgrade`'s method, made cross-engine; section 4) |

The leap is therefore not "more rules" but **conversion as search plus proof**: generate several candidates, execute them against the source engine as the oracle on generated and real inputs, keep the counterexamples, shrink them, learn rules from what passed, and report residue with the evidence. Every piece below is judged by whether it can be built from open, licence-compatible parts and tested in the local docker sandbox.

---

## 2. Conversion as search plus proof: the literature beyond SQL query translation

### 2.1 Counterexample-guided synthesis (CEGIS) with a model as the proposer

- **Classic CEGIS** (Solar-Lezama's sketching): a synthesiser proposes, a verifier checks against a specification, a failing check returns a concrete counterexample that constrains the next proposal. With a model as the proposer the verifier can be a solver, a compiler, a static checker or tests.
- **Orvalho, Janota, Manquinho, AAAI 2025** ("Counterexample Guided Program Repair Using Zero-Shot Learning and MaxSAT-based Fault Localization"; code `pmorvalho/LLM-CEGIS-Repair`): MaxSAT fault localisation removes the suspected statements, the model fills the **sketch** (holes only), the result runs against tests, a failing test is returned as the counterexample. On 1,431 incorrect student programs it improved every one of six models and beat symbolic repair tools. *Transfer:* the model should be asked to fill a hole in a routine whose other statements are already proved, not to rewrite the routine - the same idea as Horizon's spurious-edit penalty, but constructive.
- **Li, Parsert, Polgreen 2024** (arXiv 2403.03997): an enumerative synthesiser inside CEGIS, guided by a probabilistic grammar learned from the model's wrong answers. *Transfer:* for small expression-level gaps (a built-in function with no counterpart), enumeration over a grammar of target built-ins, checked on the counterexample set, can beat asking a model.
- **The failure mode to design against** (AutoCedar, 2026, arXiv 2607.03656): classic CEGIS keeps a symbolic version space; a model keeps none and **can reintroduce an earlier failure** in its next sample. A 2025 invariant-repair study (arXiv 2511.06552) found GPT-4o repaired only 6% of invariants even with error information; the hybrid LLM+SMT invariant work (arXiv 2508.00419) caps its loop at 5 rounds because loops stall. *Consequence for migkit:* the loop's memory is **the accumulated counterexample set, re-run against every new candidate** (a regression suite that only grows), and a round limit; never "fix this one diff".

### 2.2 Verified lifting and oracle-backed translation

- **LLMLift** (Bhatia, Qiu, Hasabnis, Seshia, Cheung; NeurIPS 2024, arXiv 2406.03003): the model translates into a Python IR that encodes the target DSL's semantics *and* emits a proof (summary + loop invariants) that cvc5/z3 check; only verified programs are lowered to the DSL by pattern rules. Needed ~100 lines of prompt against 1,000+ lines of hand heuristics in C2TACO. *Transfer:* for scalar routines (numeric/text functions with loops over variables, no SQL inside), a formal proof is reachable; for routines that read and write tables it is not (section 2.5).
- **VERT** (Yang, Takashima, Paulsen, Dodds, Kroening; arXiv 2404.18852, ASE 2025 "Polyglot"): the source is compiled to WebAssembly and mechanically lifted to an unreadable but faithful Rust **oracle**; the model writes a readable candidate; candidate and oracle are compared by property-based testing, then by bounded model checking (Kani, 120 s), then full verification; a failure returns the **counterexample** to the model. With Claude-2 on 1,394 programs: PBT pass 31% -> 54%, bounded-model-check pass 1% -> 42% against the model alone. *Transfer - the central idea of this file:* a migration has a perfectly faithful oracle that VERT had to build - **the source engine itself**, running the original code in the sandbox. Every candidate, whatever produced it, is compared against that oracle; readability and speed of the candidate are secondary properties the harness can also rank.
- **Berkeley EECS-2025-174** (Cheung, "LLM-Based Code Translation Needs Formal Compositional Reasoning"): testing until the given tests pass does not establish equivalence; compose verified pieces. *Transfer:* prove callees first and treat a proved callee as trusted in the caller's proof (the dependency order R11 already implies), and report the evidence level per routine honestly (tested on N inputs with coverage C, not "equivalent").

### 2.3 Test-driven translation repair: what the loop is worth, measured

| Work | Loop | Published effect | What it says for migkit |
|---|---|---|---|
| **Lost in Translation** (Pan et al., ICSE 2024, arXiv 2308.03109; IBM + UIUC) | failed translation re-prompted with code, stack trace, error, failing input/expected output | 1,700 samples, correct translations 2.1%-47.3% by model; iterative prompting **+5.5 points on average, +12 for GPT-4**, stopped after 1-2 rounds (gain < 5%) | feedback helps but saturates in two rounds; the rest needs program analysis |
| **UniTrans** (Yang et al., FSE 2024, arXiv 2404.14646) | tests generated from the source first, translation with tests in the prompt, repair rounds | computational accuracy GPT-3.5 85.96% -> 89.31%, LLaMA-13B 36.61% -> 43.52%; later work (TransAgent) measured the repair stage alone as marginal (31.25% -> 31.90%) | tests *before* translation matter more than repair after |
| **TransAgent** (arXiv 2409.19894) | source and target split into blocks by control-flow graph, blocks aligned, both **instrumented at block entry/exit**, runtime values compared per aligned block; the first diverging block is localised and repaired with runtime values in the prompt | Python->Java 89.5% vs UniTrans 56.2%; C++->Java 91.0% vs 65.5%; block alignment 95.8-100% | **compare intermediate state, not only outputs**: for procedures, capture temp tables and variables at aligned points (section 2.6) |
| **Google, ICSE 2025 SEIP** (arXiv 2501.06972) and **FSE 2025** (arXiv 2504.09691) | model edits validated by build + tests in the monorepo, humans review | Int32->Int64: 80% of landed code changes AI-authored (74.45% of changes, 69.46% of edits in the FSE study); ~50% less total time; "the human needed to revert at least some changes" in most cases | at scale, value comes from the *validation harness* around the model; humans still revert |
| **Mallet** (Ngom, Kraska; aiDM@SIGMOD 2024) | the model writes **rules** (type mappings; function translations by native composition, a generated UDF, an extension, or "impossible"), each rule validated by running source and target on generated data; a diverging rule is re-prompted or regenerated fresh | on TPC-DS rules applied in 26 ms vs 16 s per query for GPT-4; handled all special functions where jOOQ and SQLines failed; names emulation and procedure translation as future work | the model belongs **off the critical path**, generating reusable rules that are proved, not translating each object |
| **RISE** (ICSE 2026) | reduce the query, model translates the reduced query, a rule is extracted and applied to the original | already in the earlier report (97.98% TPC-DS; CrackSQL only 4.55% on SQLProcBench per RISE) | reduction before translation is the strongest published device for SQL |

**Takeaway.** The published gains come from four things, in this order: an execution oracle, tests available *before* the model writes, localisation of the first divergence (block-level state comparison), and moving the model from per-object translation to rule generation. Free-form "regenerate until the diff is empty" (Datafold's description) is the weakest of the four and saturates in about two rounds.

### 2.4 Differential testing across engines, at scale

- **RAGS** (Slutz, VLDB 1998, Microsoft): random SQL run on several commercial engines, results compared; the paper already names "NULL handling, character handling, and numeric type coercions" as the reason cross-engine differential testing only works on a common subset. That list is migkit's rule table (earlier report s.2.5) - i.e. the noise of RAGS is exactly the *signal* of a migration.
- **SQLxDiff** (ISSTA 2025, arXiv 2501.01236): cross-DBMS differential testing against PostgreSQL 14 as the reference, with **clause mappings** that classify clauses as shared / failed / mappable and wrap known semantic differences (e.g. NULL handling) in `CASE WHEN` so they are not reported; 57 bugs (17 logic), 50 fixed. *Transfer:* a clause mapping *is* a translation rule with a declared semantic caveat; migkit's rule table should carry, per rule, the *known* difference and the input class that exposes it, so the harness can both test the rule and know which differences are intended.
- **SQLancer++** (arXiv 2503.21424): of bug-inducing test cases, only 48% were even valid on other engines - "features across DBMSs are mostly distinct". *Transfer:* a cross-engine test generator must be grammar-aware per engine; sqlglot's per-dialect generators give that for expressions.
- **SQuaLity** (arXiv 2410.21731): unified the test suites of SQLite, PostgreSQL and DuckDB; SQLite's tests are 99% standard SQL, PostgreSQL's 31%; running suites across engines found crashes, hangs and compatibility issues. **Sedar** (ICSE 2024) uses a model to transfer one DBMS's tests into another's dialect as fuzzing seeds. *Transfer - a cheap, large corpus:* the engines' own regression suites (PostgreSQL `src/test/regress`, MySQL `mysql-test`, sqllogictest) are thousands of built-in-function and expression cases with expected outputs; translated by sqlglot and run on the other engine in the sandbox, every disagreement is a **measured semantic-difference rule candidate** for the pair (section 6.3).
- The SQLancer oracles (TLP, NoREC, PQS, DQE) are in the earlier report (s.4.3); what this section adds is the cross-engine framing and the corpora.

### 2.5 Symbolic execution of database code: what exists, what is usable

| Work | Reach | Status | Usable by migkit? |
|---|---|---|---|
| **Marcozzi, Vanhoof, Hainaut**, "Relational symbolic execution of SQL code for unit testing of database programs" (Sci. Comp. Prog. 105, 2015; arXiv 1501.05265, 1501.05821) | Java methods with embedded transactional SQL under integrity constraints; tables as relational symbols, SQL as constraints in SMT-LIB, Z3 | research prototype; Z3 found inputs for 4 feasible paths in 3.5 s where Alloy took 38 min | the **encoding** (tables as symbolic relations bounded in size, constraints from the schema) is reusable; the tool is not |
| **Symbolic execution of stored procedures in DBMSs** (ASE 2016) and **Extending symbolic execution for automated testing of stored procedures** (Software Quality Journal 2019) | PostgreSQL stored procedures; dynamic symbolic execution, constraints extracted by **instrumenting PostgreSQL's execution plans**, Z3 generates table data and inputs | research; PostgreSQL only; no public maintained tool found *(not found)* | the idea "ask the engine's own planner for the predicates, solve for data that flips them" fits migkit, which already reads plans in `workload.py` |
| **Qex** (Veanes et al., Microsoft Research, "Symbolic query exploration") and Pex-based DB testing; Pan/Wu/Xie (ASE 2011, DBTest 2011); MODA (ASE 2010) | queries / C# apps with SQL | historical, not maintained | no |
| T-SQL, PL/SQL, MySQL routines | - | **no symbolic executor found** for any of them *(not found)* | - |

**Verdict.** No off-the-shelf symbolic executor exists for PL/SQL, T-SQL or MySQL procedures, and building one per dialect is L+ for each. The practical substitute is **concolic by execution**: run the routine with coverage on (section 4.4 of the earlier report: `plpgsql_check` coverage, cover_me-style tagging), read the *predicates* of the untaken branch from the parsed body (sqlglot for embedded SQL conditions, the block parser for `IF`), and solve only those predicates for input values with a small solver call (z3-solver is MIT) or with Hypothesis' targeted search. That is the TLP/XData boundary idea made coverage-directed. Full symbolic execution is reserved for **scalar routines** (no table access), where LLMLift/VERT-style bounded checking over a Python model of both bodies is feasible (M, and only after the execution harness exists).

### 2.6 Reduction: shrink the input *and* the program

| Tool | What it reduces | Licence | Fit |
|---|---|---|---|
| Hypothesis shrinking | the failing call's arguments and generated rows | MPL-2.0 (already a dev dependency) | input side (earlier report s.4.5) |
| **SQLreduce** (credativ, Christoph Berg; 2022) | a SQL query to the minimal query giving the same PostgreSQL error, over pglast's parse tree | MIT (repository); depends on pglast (GPL-3.0+), so run as a program, never imported | PostgreSQL-only; reduces to the same *error*, not to the same *result difference* - its tree-walk is the model for migkit's own reducer |
| **ddmin** (Zeller's delta debugging) via **picire** / **picireny** (hierarchical, grammar-aware over ANTLR) | any sequence / tree | picire BSD-3-Clause (parallel ddmin; hierarchical reduction with tree preprocessing and outcome caching); picireny builds on it | the **program side**: shrink a routine's statement list while "source and candidate still differ on input X" holds |
| C-Reduce, Perses (syntax-guided reduction, ICSE 2018) | C / any grammar | C-Reduce NCSA; Perses research | ideas only |
| RISE's query reduction | SQL query before translation | research | already noted |

**The two-sided reduction migkit can do and no converter does.** When candidate and source differ, (1) shrink the input with Hypothesis to a minimal call and minimal fixture rows; (2) with that input fixed, delete statements from *both* bodies in aligned pairs (the block alignment of TransAgent, 2.3) while the divergence persists, and (3) report the smallest pair of statement blocks and the smallest data that still disagree. That minimal pair is what goes to the model (a hole to fill, 2.1), to the rule learner (a before/after example, section 6), and to the person (a two-line repro instead of a 400-line procedure).

---

## 3. Semantic emulation: running the old code instead of converting it

### 3.1 The layers, what each emulates, licence, where it runs

| Layer | Pair | What it emulates | Mechanism | Licence | Managed availability | Local docker (arm64) |
|---|---|---|---|---|---|---|
| **orafce** | Oracle -> PostgreSQL | Oracle functions (dates, strings, `NVL`, `DECODE`-like), `VARCHAR2`/`NVARCHAR2` types, `DUAL`, packages `DBMS_OUTPUT`, `DBMS_PIPE`, `DBMS_ALERT`, `UTL_FILE`, partial `DBMS_SQL`; verified on Oracle 10g | ordinary extension, schemas `oracle`, `plvstr`, `dbms_*` | BSD (README; exact variant to be read from the repo) | RDS/Aurora (no `utl_file`), Azure Flexible (no `utl_file`/PLVlex at announcement), Cloud SQL (PostgreSQL 17 maintenance version listed) | yes (PGDG packages) |
| **AWS SCT extension packs** `aws_oracle_ext`, `aws_sqlserver_ext` (+`_data`), `aws_mysql_ext` | Oracle/SQL Server/MySQL -> PostgreSQL | built-ins, package variables (`set_package_variable`/`get_package_variable`), SQL Server Agent and Database Mail through Lambda/CloudWatch | schema of PL/pgSQL functions and tables installed by SCT before converted code | **no public licence text found**; practitioners describe it as usable only on AWS targets; Google DMS refuses sources that use them *(licence not confirmed)* | AWS only | no (not distributable) - **cannot be a migkit dependency** |
| **Babelfish for PostgreSQL** | SQL Server -> PostgreSQL | TDS wire protocol, T-SQL language and procedures, SQL Server system catalogues and semantics; bilingual cluster (TDS and PostgreSQL endpoints on the same data) | **patched PostgreSQL engine** (`postgresql_modified_for_babelfish`) + extensions `babelfishpg_tsql`, `babelfishpg_tds`, `babelfishpg_common`, `babelfishpg_money`; 5.x on PostgreSQL 17 (latest release 5.4.0) | Apache-2.0 and PostgreSQL licence | Aurora PostgreSQL only | community images (`jonathanpotts/babelfishpg`, default `BABEL_5_2_0__PG_17_5`; arm64 build *to be checked*), or built from source (Ubuntu instructions) |
| **IvorySQL** | Oracle -> PostgreSQL | PL/iSQL (PL/SQL syntax), packages (create/alter/describe), `%TYPE`/`%ROWTYPE`, `ROWID`, NLS parameters, Oracle types and conversion functions, dual parsers on two ports (`initdb -m oracle`, `ivorysql.compatible_mode`); imports and extends orafce | **fork of PostgreSQL** (5.6 on PostgreSQL 18.6, 2026-09-18) | **Apache-2.0** | none of the three big clouds *(not found)* | yes: packages for x86/ARM listed; Docker/K8s operator; ships `plpgsql_check` among supported extensions |
| **openHalo** | MySQL -> PostgreSQL | MySQL wire protocol (5.7.32/8.0) on port 3306 and MySQL dialect, same data readable from PostgreSQL clients | **fork of PostgreSQL 14.18**, beta (v1.0 beta1) | **GPL-3.0 per the mirrored LICENSE** *(to be confirmed on the main repo)* - **flag** | none | Pigsty packages (`pg_mode: mysql`); arm64 *to be checked* |
| **openGauss** A/B modes (+ `dolphin` for B) | Oracle -> openGauss (A: `''` is NULL, `DATE` -> `timestamp(0)`); MySQL -> openGauss (B: MySQL keywords, types, functions, `sql_mode`, case-insensitive `LIKE`) | per-database `DBCOMPATIBILITY`, fixed at `CREATE DATABASE` | own engine (PostgreSQL 9.2 lineage, heavily changed) | **Mulan PSL v2** (OSI-approved, permissive) | Huawei GaussDB (commercial sibling) | yes: official `opengauss/opengauss-server` multi-arch |
| **MariaDB `sql_mode=ORACLE`** | Oracle -> MariaDB | PL/SQL subset: packages (any mode from 11.4), `SYS_REFCURSOR` (12.0), associative arrays (12.1), `(+)` joins and `TO_DATE` (12.3 LTS), REF CURSOR types (13.0), `ROWNUM`, `ADD_MONTHS`, `TO_CHAR`; `''`-is-NULL only with `EMPTY_STRING_IS_NULL` | in-server parser mode | GPL-2.0 (server; migkit talks to it over the wire, no linking) | MariaDB SkySQL / cloud MariaDB | yes (official image multi-arch) |
| **EDB Postgres Advanced Server** | Oracle -> EPAS | PL/SQL, packages, `DBMS_*`, Oracle catalogue views | proprietary fork | proprietary | EDB BigAnimal / Hybrid Manager | needs an EDB subscription token - not in the public sandbox |
| **Oracle SQL Translation Framework** | Sybase ASE / SQL Server (limited Db2) -> Oracle | SQL translated **at run time** per statement; a *translation profile* stores captured statements with their translations or errors; custom translations override; unseen statements can be made to error and be logged | inside Oracle (`DBMS_SQL_TRANSLATOR`, translators as Java in the database) | proprietary (part of Oracle Database) | Oracle | Oracle Free has the package *(availability of the Sybase/SQL Server translators on Free to be checked)* |
| **Datometry Hyper-Q** | Teradata / Oracle -> Synapse, BigQuery, Redshift, Databricks | wire protocol + SQL translated in real time through an algebra (XTRA), missing features (procedures, macros, SET tables) emulated with metadata in `__DTM_MDSTORE`; claims 99.5% coverage and "bit-identical" results | proxy | proprietary | SaaS/marketplace | no |
| **Apache ShardingSphere-Proxy SQL translator** | MySQL protocol in front of PostgreSQL and the reverse | frontend protocol differs from backend type; `NATIVE` translator **passes SQL through unchanged** (issue #38043, Feb 2026: `LIMIT 0, 200` reached PostgreSQL); real translation needs the jOOQ provider in the plugin repository | proxy (Java) | Apache-2.0 (jOOQ commercial dialects are not) | - | yes, but not useful without jOOQ's commercial edition *(inferred)* |

### 3.2 When running the old code under emulation beats converting it

The trade is not "emulation or conversion" for a whole database - that is how the vendors sell it (EDB: move to EPAS; AWS: Babelfish on Aurora). It is a **per-object** decision with measurable inputs:

| Fact (measured per object or per hop) | Favours emulation | Favours conversion |
|---|---|---|
| The emulated object **passes the same differential proof** as a converted one (section 7) | yes | - |
| Construct inventory hits things the converters handle worst: T-SQL `TRY/CATCH` + `XACT_STATE`, table variables surviving rollback, `@@ROWCOUNT` order effects, Oracle package state, autonomous transactions, `CONNECT BY` pseudo-columns | yes, *if* the emulator implements them (Babelfish for T-SQL, IvorySQL for packages) - to be proved, not assumed | if the emulator does not |
| Target must be a managed service without the emulator (RDS PostgreSQL has orafce but not Babelfish/IvorySQL/openHalo) | only orafce-level emulation | yes |
| Performance: emulated wrappers are VOLATILE PL/pgSQL (e.g. `aws_oracle_ext.sysdate()` = `clock_timestamp()` wrapper), AWS itself calls extension-pack calls slow; Babelfish ignores T-SQL hints unless `enable_pg_hint`, and cannot show plans inside procedures | cold code | hot code: measure the object's call time both ways (`workload.py` already times statements) |
| Code still changing after cutover (developers keep editing it) | if the team keeps the old dialect | if the team moves to the target's dialect |
| Licence / fork lag: Babelfish and IvorySQL track PostgreSQL minors with a delay; openHalo is a beta fork of 14.18 under GPL-3 | short horizon | long horizon |
| Footprint rule (migkit's own): emulation adds schemas/extensions to the target | only in a migkit-named schema, listed by `leftovers.py`, removable by `rollback` (paid-cloud gap 12) | - |

### 3.3 How to prove emulation is safe, and the leap it enables

1. **Same harness, different candidate.** "Run the original text on the emulating target" is just another candidate generator (identity translation) in section 7. It goes through the same gate (does it `CREATE`?) and the same differential proof against the source oracle. Babelfish Compass's *ReviewSemantics* class and EDB's "compatibility %" say *it will run*; only execution says *it means the same*.
2. **Emulation as the oracle after cutover (move first, convert later).** Babelfish's bilingual cluster and IvorySQL's dual parsers let the old-dialect object and a native rewrite live **in the same database on the same rows**. Once the source engine is gone (after cutover), the emulated object becomes the oracle for each later native rewrite: migkit proves `tsql.proc_x` (TDS endpoint) against `plpgsql.proc_x` (PostgreSQL endpoint) inside one sandbox copy, with no fixture copying and no source access. No vendor offers a *proved* strangler path; Babelfish's own docs only recommend converting hot paths later.
3. **Emulation to separate "converter bug" from "engine difference".** Where a converted object fails the proof and the emulated one passes, the difference is in the conversion (a rule to fix); where both fail on the same input, the difference is below the language (collation, numeric semantics, clock) - a different fix (schema or session settings, section 5). This triage is free once both candidates run.
4. **Measured exit cost.** For every emulated object, record which emulation functions it calls (a dependency walk over the target catalogue, `pg_depend` + parsed bodies); the report then states "N objects depend on the emulation schema; M of them have a proved native replacement" - the extension-lock-in number that AWS's extension packs never show (a third-party `extensionmigrationassistant` exists just to find those wrappers).

**Licence verdicts for migkit (MIT):** orafce, Babelfish, IvorySQL, openGauss, MariaDB are usable as *sandbox engines* (containers talked to over the wire; nothing linked into migkit). openHalo is GPL-3.0 (to be confirmed) - fine as a separate server process, never vendored. AWS extension packs, EPAS, Hyper-Q, Oracle's translators: not part of the open path.

---

## 4. Application SQL: the real workload as the corpus

### 4.1 What the paid tools do with application code

| Tool | Languages / frameworks | Mechanism | Check |
|---|---|---|---|
| **AWS SCT application conversion** | dedicated converters for **Java (incl. MyBatis XML and annotations), C#, C++ (MSVC/GCC/Clang, user macro file), Pro*C** - Oracle -> PostgreSQL only; SQL*Plus -> psql; a *generic* converter for other pairs/languages | collects statements across functions, parameters and local variables (host-variable resolution), converts with the schema project's mapping rules; CLI `CreateGenAppProject` | none beyond conversion; report of what did not convert |
| **AWS Transform, full-stack Windows modernization** (GA Dec 2025; offline DDL upload Aug 2026) | .NET Core 6/7/8/10 only, **ADO.NET or Entity Framework** (EF 6.3-6.5, EF Core 1.0-8.0); SQL Server 2008 R2-2022 -> Aurora PostgreSQL 15+ | agent finds database references, EF models, connection strings, SQL patterns; rewrites data-access code and connection strings; commits to a new branch; waves of schema -> data -> app -> deploy | "checks functional equivalence" (AWS's wording, mechanism not published *(not found)*); "test results" in the summary; claims 5x faster |
| **Ispirer CodeWays** | Java, C#, COBOL, embedded SQL | rules + vendor engineers | service |
| **YugabyteDB Voyager** | none (reads `pg_stat_statements`) | parse-tree feature detection of the source's frequent statements | none (earlier report s.1.7) |
| **Oracle SQL Translation Framework** | JDBC/ODBC/ODP.NET apps from Sybase/SQL Server | translate at run time, store translations in a profile, custom overrides, make unseen statements error-and-log | the application running on Oracle |
| **Percona `pt-upgrade`** (open source, GPL-2.0; MySQL -> MySQL only) | none - it reads the *log* | runs every statement from slow/general/binary/tcpdump logs on two servers (or saved results vs a server); compares row counts, row data (whitespace and float precision significant), warnings, errors (one-sided "Query errors" vs both-sided "SQL errors"), query time; groups by fingerprint; clears warnings before each query | differential execution - but same engine, and "should never be run on production" |

Nobody combines the three things the migration needs: **the statements the application actually sends**, **translated for the target**, **executed on both engines over proven-equal data and compared**. `pt-upgrade` does the last two for one engine; SCT and AWS Transform do the translation without published equivalence evidence; Voyager only inspects.

### 4.2 Two ways to find the SQL, and why the log wins

**Static scan of the code** (what SCT does):
- *Parser*: **tree-sitter** grammars for Java, C#, Python, Go, JavaScript/TypeScript, Kotlin, PHP, Ruby; **ast-grep** (MIT, Rust, tree-sitter based; YAML rules with `pattern`/`inside`/`has`, `fix` for rewrites; pip `ast-grep-cli`) as the rule engine - one rule per sink: JDBC `prepareStatement($S)`, JPA `createNativeQuery($S)` / `@Query(value=$S, nativeQuery=true)`, MyBatis mapper XML `<select>/<insert>` (plain XML), EF Core `FromSqlRaw`/`ExecuteSqlRaw`/`SqlQueryRaw`, Dapper `Query($S)`, ADO.NET `CommandText = $S`, SQLAlchemy `text($S)`, Django `raw($S)`/`cursor.execute($S)`, Go `db.Query($S)`/`Exec`, GORM `Raw($S)`. The string argument is then parsed by **sqlglot** in the source dialect.
- *Limits*: strings built by concatenation or builders (constant-fold what is constant, mark the rest as holes); ORM-generated SQL (HQL/JPQL, LINQ, SQLAlchemy Core) is **not** in the code at all - but that part is converted by switching the ORM's dialect/provider (Hibernate dialect, EF Core provider, SQLAlchemy dialect), so the static scan's real job is the *residue*: native SQL, dialect-specific functions, hints, and ORM mapping choices that change behaviour on the new engine (identity vs sequence generation strategy, `nvarchar` sizes, boolean mapping, timestamp precision, case sensitivity).
- *Licences*: tree-sitter MIT, ast-grep MIT, sqlglot MIT. Semgrep's engine is LGPL-2.1 (usable as a program); CodeQL is not free for this use - avoid.

**The workload log** (what `pt-upgrade` and Voyager read):

| Engine | Source of real statements | Literals / parameters available? |
|---|---|---|
| MySQL 8.0.3+ | `performance_schema.events_statements_summary_by_digest`: `DIGEST_TEXT` (normalised) + **`QUERY_SAMPLE_TEXT` (a real statement with its literals**, usually the slowest recent one; truncated at `performance_schema_max_sql_text_length`, 1024 bytes default) + counts and latency quantiles; `events_statements_history_long` for sequences; slow log with `long_query_time=0` | yes (one sample per digest; more from history/slow log) |
| PostgreSQL | `pg_stat_statements` (normalised, `$n` placeholders, calls, time) - what `workload.py` already reads; the server log with `log_min_duration_statement=0` records extended-protocol parameters in a `DETAIL: parameters:` line | normalised only from the view; values from the log *(log parameter format as documented; to be measured)* |
| SQL Server | Query Store (`sys.query_store_query_text`, parameterised text, runtime stats); cached plan XML carries the **compiled (sniffed) parameter values** (`ParameterCompiledValue`); Extended Events `rpc_completed` with the full call | partly (sniffed values; full values with XEvents) |
| Oracle | `V$SQL` / AWR (text, counts), **`V$SQL_BIND_CAPTURE`** (sampled bind values), SQL Tuning Sets capture statements with binds | sampled |

**Why the log wins as the primary corpus.** It is (a) what the application *actually* sends, ORM-generated SQL included, (b) **weighted by frequency** - so a pass rate can be stated per execution, not per distinct statement, (c) engine-read, no per-language scanner, and (d) already partly wired: `migkit/workload.py:40` reads the busiest reads from `pg_stat_statements` / `performance_schema` and replays them on both sides for performance on same-engine hops. The static scan is kept for what the log cannot see: code paths not exercised in the capture window, and mapping each logged statement back to its code location. **sqlcommenter** (Google, donated to OpenTelemetry; Django, SQLAlchemy, Rails, Spring/Hibernate via `statement_inspector`, Knex, Sequelize, Prisma, Ent) appends `/*controller=..,route=..,traceparent=..*/` to each ORM statement; where the application already uses it, every logged statement names its route/controller, which closes the loop from "statement 17 differs" to "the `/orders/export` endpoint will change behaviour".

### 4.3 Replaying the real workload across engines (the proof for application SQL)

A cross-engine `pt-upgrade`, built on what migkit already has:

1. **Capture** the corpus read-only from the source's statistics (above); keep literal-bearing samples **inside the sandbox only** - they can hold personal data, so they never go to a model (`assist.shares`, `assist.py:105`) and are shown in reports only as normalised text (the rule `workload.py` already states).
2. **Parameters**: where only placeholders exist, synthesise values from the column's own distribution (`pg_stats.most_common_vals` / histogram bounds, MySQL histograms, or the sandbox fixture), plus the section 2.5 edge classes of the earlier report.
3. **Translate** each statement with sqlglot (source dialect -> target), applying the move's identifier renames (as `converted_code` already does through `_rename`, `hetero.py:2160-2200`). A statement sqlglot cannot translate is itself a finding: *the application will break here*.
4. **Execute** on the sandbox pair over the fixture that migkit moved and proved equal: reads compared as multisets (lists when the statement orders fully); writes inside a transaction rolled back, comparing affected-row counts and row changes (the R11 harness); errors compared as classes (one-sided error = finding; both-sided = unrelated to the migration).
5. **Group and weight** by fingerprint, as `pt-upgrade` does, and report **per execution count**: "the statements covering 97.3% of the application's executions answer the same; 3 digests covering 2.1% differ (minimal repro each); 1 digest (0.6%) does not parse on the target".
6. **Loop into conversion**: a differing statement is a counterexample for the rule that translated it (section 6), and its minimal repro (Hypothesis on the parameters, the reducer on the statement) goes to the rule learner.

Effort M on top of `workload.py` and the R11 harness; docker-testable on MySQL <-> PostgreSQL today. It turns an existing same-engine performance check into the application-compatibility proof none of the paid tools publishes.

---

## 5. Schema design beyond 1:1, and proving it keeps the constraints

The paid converters translate DDL 1:1 from declarations (AWS/Ora2Pg map `NUMBER(p,s)` by rule; MOLT adds best-practice notes). Two things are missing everywhere: **choices made from the data and the workload**, and **a proof that the target refuses and accepts exactly what the source did**. migkit already has the neutral type layer (`canon.py:903`), a measured planning scan, the workload reader (`workload.py`) and the data proof; the rest is below.

### 5.1 Types sized from the data - without silently narrowing the domain

- *Evidence* (the planning scan): per column, max length in characters **and** bytes, max precision/scale actually used, min/max, whether all values are integral, whether text is ASCII-only, time zone usage, fractional-second digits used.
- *Rule*: data evidence may **widen** a declared type freely (declared `NUMBER` holding only integers < 2^63 -> `bigint` is a *narrowing of the domain*, not of the data). A narrowing is allowed only when the declared domain is lost information (bare Oracle `NUMBER`, MySQL `TEXT` for a code column, SQLite/MongoDB/DynamoDB with no declaration) and is **recorded with the evidence** ("max 38 of 4000 declared; `varchar(64)` chosen; future writes over 64 will fail on the target and succeeded on the source") and proved by 5.4's negative test so the owner sees the behavioural change before it ships. Ora2Pg's `PG_INTEGER_TYPE` narrows by declaration only; migkit narrows by evidence *and* says what changes.

### 5.2 Choices where the target has several right answers

| Choice | Facts that decide it | Candidates | How the choice is proved |
|---|---|---|---|
| **Collation** | the source collation's behaviour classes (case, accent, trailing space, width, expansions like `ß`/`ss`, Turkish `I`) **and which of those classes occur in the column's real values** | PostgreSQL: deterministic ICU/libc, ICU nondeterministic (`und-u-ks-level2`, `deterministic=false`; `LIKE` allowed from PostgreSQL 18; B-tree deduplication off; PK/FK must share collation from 18), builtin `PG_UNICODE_FAST` + `casefold()` expression indexes (18), `citext` | a **collation agreement matrix**: generate string pairs per class (plus pairs drawn from the column's actual distinct values), compare `=`, `ORDER BY`, `GROUP BY`/`DISTINCT`, `LIKE` on source vs each candidate in the sandbox; pick the cheapest candidate that agrees on every class present in the data; name the classes where it differs (e.g. MySQL `utf8mb4_0900_ai_ci` is NO PAD while `utf8mb4_general_ci` is PAD SPACE - trailing spaces equal on one, not the other) |
| **Identity / sequence** | source semantics (MySQL `AUTO_INCREMENT` per table, `innodb_autoinc_lock_mode`; SQL Server `IDENTITY` cache jumps after restart; Oracle `CACHE`/`ORDER`); whether the app inserts explicit keys (seen in the workload corpus, section 4); `LAST_INSERT_ID`/`SCOPE_IDENTITY` use in code | `GENERATED BY DEFAULT AS IDENTITY` (explicit inserts allowed) vs `ALWAYS`; sequence shared across tables (Oracle pattern); UUIDv7 only by owner's choice | explicit-key inserts from the corpus succeed; next value > max(key) after cutover (migkit's sequence step); the value-returning idiom translated in 4.3 answers the same |
| **Partitioning** | source scheme (Oracle range/list/hash/interval/reference, MySQL range/list/hash/key), partition key in every unique key (MySQL requires it; PostgreSQL requires it for PK/unique on partitioned tables), row counts per partition | declarative partitioning 1:1; fewer partitions where the source's were for manageability; none for small tables | **row routing equivalence**: for boundary values of every partition bound (just below, at, just above, NULL, default), insert on both sides and compare which partition holds it (PostgreSQL `tableoid::regclass`, MySQL `information_schema.PARTITIONS` counts / `EXPLAIN` partitions); pruning compared through the workload's plans |
| **Indexes** | the translated workload (section 4) and its plans on the target; source-only index kinds (MySQL prefix indexes, Oracle bitmap/function-based, SQL Server clustered/`INCLUDE`/filtered) | expression index (`left(col,n)`, `lower`/`casefold`), `INCLUDE`, partial index, BRIN for append-only time columns | **HypoPG** (PostgreSQL licence) creates hypothetical indexes the planner sees without building them; **Dexter** (MIT) automates candidate selection from `pg_stat_statements` + HypoPG; `workload.py` already flags "reads whole on the target where the source uses an index" - the candidate that removes the finding at the lowest size wins, then is built and re-measured |
| **Constraints the target cannot express 1:1** | Oracle deferrable constraints, SQL Server unique index allowing a single NULL, Oracle composite unique treating all-NULL rows as absent, MySQL's `CHECK` enforced only from 8.0.16, SQLite's nullable PK | `UNIQUE NULLS NOT DISTINCT` (PostgreSQL 15+) for the SQL Server single-NULL rule; partial unique indexes; deferrable constraints | 5.4 |

### 5.3 Beyond 1:1: restructuring with a proof (research, L)

- **Mediator** (Wang, Dillig, Lahiri, Cook; POPL 2018, arXiv 1710.07660) proves two database programs over **different schemas** equivalent via bisimulation invariants over relational algebra with updates, encoded for Z3; 20/21 benchmarks, 10/11 real-world under 50 s on average.
- **Migrator** (Wang, Dong, Shah, Dillig; PLDI 2019; `utopia-group/migrator`) synthesises the new version of a database program for a refactored schema (value-correspondence enumeration, sketch generation, completion with conflict-driven learning from **minimum failing inputs**), verifying with Mediator; all 20 benchmarks. **Dynamite** (VLDB 2020) synthesises the data migration itself as Datalog from examples.
- *Transfer:* when the owner chooses a non-1:1 schema (split a wide table, fold an EAV table into columns, merge lookup tables), the stored code and the application statements that touch those tables must change too; Migrator's decomposition - *find the value correspondence, sketch, complete against failing inputs* - is the same loop as section 7 with the correspondence as an extra search dimension. Not a first step for migkit; recorded as the direction that makes "schema redesign during migration" provable instead of a separate project.

### 5.4 Proving the translated schema keeps the constraints: accept/reject equivalence by execution

- **SchemaAnalyst** (McMinn, Wright, Kinneer, McCurdy, Camara, Kapfhammer; ICSME 2016; `schemaanalyst/schemaanalyst`, Java; licence *not confirmed*, believed GPL-3 - **flag**, use as a method only): for every integrity constraint it builds test requirements (Integrity Constraint Coverage: one row that satisfies, one that violates; 9 criteria in all), generates rows by search, and scores them by **schema mutation** (mutate a constraint, check the tests notice). Its authors found DBMSs interpret constraints differently (SQLite accepts a NULL primary key) and that mutant counts differ per engine (178 vs 184 for one schema) - i.e. constraint semantics is engine-specific, which is exactly what a migration changes.
- **migkit's version** (S-M, fully docker-testable): for each table, generate from the *source* catalogue a small set of probe statements per constraint - satisfying and violating inserts/updates/deletes for NOT NULL, CHECK (boundary values of each comparison, NULL - a CHECK passes on NULL), UNIQUE (duplicate, duplicate with NULLs in each column), PK, FK (missing parent, parent delete under each action, deferred vs immediate), defaults, generated columns, length/precision limits, enum/set membership - run each in a rolled-back transaction on **both** engines in the sandbox, and compare *accepted vs rejected* (and the error class). A difference is a constraint the translation lost or tightened; the verdict names the probe. This is what `check_schema` (`hetero.py:179`, columns and classes only) lacks, and no paid tool does it: EDB counts DDL that *ran*, not DDL that *means the same*.
- **Optional static half** (S): CHECK constraints that sqlglot parses into arithmetic/comparison/boolean/`IN`/`BETWEEN` can be encoded in **z3-solver** (MIT) with SQL three-valued logic and checked for equivalence of the source and translated predicate; a counterexample becomes one more probe row. Anything outside the encodable subset falls back to the probes.

---

## 6. Learning rules from accepted fixes (the moat that grows with proof)

### 6.1 What the literature says works

| Work | Learns from | Mechanism | Published result | Lesson for migkit |
|---|---|---|---|---|
| **Refazer** (Rolim et al., ICSE 2017; Microsoft PROSE; `gustavoasoares/refazer`) | before/after edit pairs | programming-by-example over a DSL of AST transformations; rules with holes | intended transformation learned in **84% of 56 scenarios from 2.9 examples** on average; fixed 87% of students on 4 tasks | two or three accepted fixes of the same construct are enough to generalise |
| **Getafix** (Bader, Scott, Pradel, Chandra; OOPSLA 2019; deployed at Facebook) | human-written fixes | tree differencing -> concrete edits -> **hierarchical clustering by anti-unification** (differences become holes, climbing to more abstract patterns) -> ranking by context | 1,268 fixes, 6 bug classes: top suggestion equals the human fix **12%-91%** by class | anti-unification over sqlglot ASTs is the right generaliser; rank candidates by context (dialect pair, construct code, types) |
| **QueryBooster** (Bai, Alsudais, Li; PVLDB 16(11) 2023; `ISG-ICS/QueryBooster`) | example query pairs | generalises examples into rules in its **VarSQL** language and suggests the best ones | user study + workloads (performance rewriting) | a *human-readable rule language* over SQL with variables is what lets people review learned rules |
| **WeTune** (SIGMOD 2022) | enumeration of plan templates up to size 4 + SMT verification | superoptimisation-style discovery, verified | 1,106 rules in 36 h on 120 cores; **only 35 useful on real queries, 34 of them found from 8,518 real web-application queries** | enumerate blindly and 97% is noise: learn from the **real corpus's failures**, not from a grammar |
| **Mallet** (aiDM 2024) | model-written rules | RAG over docs + contributor expertise, validated by executing source and target on generated data | rules reusable at 26 ms vs 16 s per query | a model is a good *rule proposer* if every rule is executed before use |
| **RISE** (ICSE 2026) | reduced query + model translation | extracts a rule from the reduced pair, applies it to the original | 97.98% TPC-DS | reduction before generalisation |

### 6.2 migkit's rule loop

A **rule** is data, not code in a release: `(id, dialect pair, construct code, match pattern over the source AST with holes, rewrite template, preconditions on hole types/collations/settings, declared semantic caveat and the input class that exposes it, provenance, proof record)`. The existing sqlglot transforms are rules of this shape; the construct codes are those of the earlier report s.6.2.

1. **Where examples come from.** (a) A person's accepted `convert/<routine>.sql` that passed where a rung failed (the R11 "by-hand" rung); (b) a model's candidate that passed the proof; (c) a two-sided-reduced counterexample pair (section 2.6) with the fix that made it pass; (d) cross-engine regression-suite disagreements for built-ins (section 2.4).
2. **Generalise.** Diff the failing candidate and the accepted one at statement/expression level (`sqlglot.diff`), keep the minimal changed subtrees, and **anti-unify** them with other accepted edits of the same construct code (Getafix): identifiers and literals become holes, types become preconditions. With one example the rule stays *specific* (exact subtree); it generalises only when a second example agrees.
3. **Prove before adopting.** Apply the rule everywhere it matches in (i) the public fixture corpus (SQL-ProcBench, the MySQL corpus R11 has to write, cross-engine suite cases) and (ii) the current migration's objects; run the section 7 proof on every affected object. Adopt only with **zero regressions** and at least one new pass; otherwise keep it as a *proposal* with its counterexample.
4. **Rank** candidate rules for a new match by measured pass rate for that construct code and pair, then by specificity (Getafix's context ranking).
5. **Keep the evidence.** Every rule carries its proof record (objects proved, inputs, coverage) and the counterexamples that killed its predecessors - the version space a model loop lacks (section 2.1).
6. **Share, safely.** Built-in rules ship in the repository with their proof records and are re-proved in CI on every change (the public corpus is the regression suite). Rules learned from a user's code stay in that project's rule file (the paid-cloud report's "hop option naming a rules file") and are never sent anywhere by migkit; anti-unification already strips identifiers and literals, and contributing a rule upstream is the user's act, not migkit's. This follows the owner's confidentiality rule and `assist.shares` (`assist.py:105`).

**Why this beats the vendors' rule bases (measurably):** Ispirer adds a customer rule in 3-5 business days by an engineer; Google's Gemini "suggests the same fix" without executing it; AWS's rules are closed. migkit's learned rule arrives when the second agreeing fix is accepted, is *executed* on every match before adoption, and its regressions are zero by construction on the corpus. The measurable figures: *time from first manual fix to rule adopted*, *objects auto-fixed per accepted manual fix* (the "fix once, propagate" multiplier), *rule precision on the corpus* (passes / matches).

---

## 7. The leap design: convert per object by the best proved candidate

This sits on top of R11 as the earlier report designed it (rungs, sandbox harness, verdict classes, `docs/research/stored-code-conversion-2026-09-28.md` s.6). R11 is a *ladder*: take the top rung, fall on failure. The leap turns the ladder into a **search**: several candidates per object run in parallel against one oracle and one growing input set, and the result is chosen by evidence.

### 7.1 Per object, end to end

```
facts --> candidates --> gate --> proof vs oracle --> choose
  |           ^                        |  fail
  |           |                        v
  |      model fills holes <-- two-sided reduction --> rule learner
  |                                    |
  +------------------------------> residue with evidence
```

1. **Facts** (read once, hashed for idempotence): construct inventory with codes, callees (proved first; a proved callee is trusted in the caller's proof - compositional, section 2.2), tables read/written, creation-time settings, and **its real call history** from the workload corpus (section 4.2: `CALL`/`EXEC`/`SELECT f(..)` statements with their arguments, weighted by frequency).
2. **Candidates**, each a generator with a declared reach:
   - `emulate`: the original text on an emulating target (orafce functions on PostgreSQL; IvorySQL; Babelfish; MariaDB Oracle mode; openGauss A/B) - only when the hop's target *is* that engine or the owner allows the emulation schema;
   - `sqlglot` (views, expression functions, statements inside bodies);
   - `rules` (built-in + learned, section 6), block-level for procedural bodies;
   - `ora2pg` (GPL-3, as a program), `sqlines` (Apache-2.0, as a program);
   - `model` - never whole-object free generation first: it receives the best partial candidate with **holes** where rules could not produce a statement, plus the minimal counterexample (section 2.1, 2.6); a whole-object proposal only when nothing else produced anything;
   - `by-hand` (`convert/<object>.sql`).
3. **Gate** (fail closed): `CREATE` in a scratch schema; `plpgsql_check_function` on PostgreSQL targets; the engine's own compile on others.
4. **Oracle**: the source engine in the sandbox on a fixture that migkit moved and proved equal (R11 s.6.3); after cutover, the emulated object where one exists (section 3.3).
5. **Inputs - a portfolio, all fed into one bank per object**:
   I1 class pools (`PROOF_INPUTS`, `hetero.py:2304`, extended); I2 constants from the body and their neighbours, three-way predicate partitions (TLP/XData); I3 fixture values (existing, missing, NULL keys); **I4 the real call history** (arguments as the application sends them); I5 coverage-guided search (Hypothesis `target()` on `plpgsql_coverage_branches`) plus **concolic predicate solving** - for an untaken branch, take its condition from the parsed body and ask z3 (MIT) for arguments/rows that flip it (section 2.5); **I6 call sequences** (Hypothesis stateful testing) for package state, temp tables and session settings that only differ on the second call; **I7 the counterexample bank** - every input that ever separated any candidate of this object *or of any object with the same construct code*, re-run first on every new candidate (the version space a model lacks).
6. **Observe**: return values, OUT params, result sets (multiset unless fully ordered), error class, row changes, counters as differences, survives-caller-rollback - and, where instrumented, **aligned intermediate state** (TransAgent): at block boundaries matched between source and candidate, temp-table digests and variables (PostgreSQL `RAISE NOTICE` read from the driver's notices; MySQL a trace table on the sandbox copy, cover_me's technique), so a failure names the first diverging block, not just the final output.
7. **Choose** among passing candidates by: evidence (coverage reached, inputs agreed), then measured call time on the target (hot objects), then footprint (no emulation schema), then size and edit distance from the rule output (fewer spurious edits, Horizon's penalty). Every candidate's evidence is kept.
8. **On failure**: two-sided reduction (inputs by Hypothesis shrinking; statements by ddmin, picire's BSD implementation or migkit's own over sqlglot/block trees) -> minimal repro -> (a) model fills the hole with the repro in the prompt, bounded to a few rounds (published loops saturate at 2-5), every round re-run against the whole bank; (b) the repro and its eventual fix go to the rule learner.
9. **Residue**, per object and weighted by workload frequency: *proved* (rung, N inputs, coverage source/target); *proved with caveat* (branch unreached - named with line; tolerance or mask - named with reason); *proved under emulation* (and which emulation functions it depends on: exit cost); *converted, not proved* (engine not runnable here, reason); *not converted* (construct codes, each candidate's failure, the minimal repro). Never `ok` without execution evidence (the R11 rule).

### 7.2 What "better" means, measurably

A vendor's headline is "N% converted" (AWS "up to 90%", EDB "compatibility %", UGO "> 95% compilation pass") - syntax or compile rates. migkit's report states numbers none of them can, because none has the oracle and the harness:

| Metric | Definition | Why a vendor cannot publish it |
|---|---|---|
| **Proved rate** | objects with execution evidence on both engines and full target statement coverage / all objects | no source engine and no data in their pipeline |
| **Workload-weighted compatibility** | executions (from the corpus) whose statements/routines agree / all executions | no workload capture |
| **Harness mutation score** | seed known semantic bugs (every "silent difference" row of the earlier report's s.2 tables, as code mutants of a proved conversion) and count how many the proof catches - the XData/SchemaAnalyst idea turned on the harness itself | they do not execute, so there is nothing to score |
| **False-green rate** | objects reported `ok` later found to differ | structurally zero by the "no ok without evidence" rule; measured by the mutation score |
| **Repro size** | statements and rows in the minimal failing case | no reducer |
| **Fix multiplier** | objects auto-fixed per accepted manual fix (rule learning) | rules are vendor-written |
| **Exit cost** | objects depending on an emulation schema / with a proved native replacement | extension packs are the product |
| **Model cost per proved object** | model calls and tokens per object that ends `proved` | loops are sold as service time |

### 7.3 The pieces: feasibility, licence, effort, docker

Effort: S up to 2 days, M up to 2 weeks, L more (the earlier report's scale). Prerequisites are R11's items 1-9 (verdict hole, per-call outcomes, `plpgsql_check` gate, sqlglot fixes, inputs, fixture, procedure harness, inventory, coverage).

| # | Piece | Builds on | Licences involved | Effort | Docker-testable here (arm64) |
|---|---|---|---|---|---|
| P1 | Candidate portfolio with selection by evidence (replaces "take the top rung") | R11 rungs | own (MIT) | M | yes, MySQL <-> PostgreSQL |
| P2 | Counterexample bank per object and per construct code; re-run first on every candidate | R11 harness | own | S | yes |
| P3 | Model as hole-filler on the best partial candidate, bounded rounds, bank re-run, confined edits | `assist.py:112`, P2 | any provider / local model / stub for tests | M | yes (stub) |
| P4 | Two-sided reduction: Hypothesis shrinking + ddmin over statement blocks | R11 harness | Hypothesis MPL-2.0 (unmodified), picire BSD-3 or own | M | yes |
| P5 | Aligned intermediate-state capture (block alignment, notices / trace table) | P4's block trees | own | M | yes (PostgreSQL notices; MySQL trace table on the sandbox copy) |
| P6 | Input I4: real call history from the corpus | P10 | own | S | yes |
| P7 | Input I5: concolic predicate solving for untaken branches | `plpgsql_check` coverage | z3-solver MIT | M | yes (PostgreSQL target; MySQL source by instrumentation) |
| P8 | Input I6: stateful call sequences | Hypothesis stateful | MPL-2.0 | S-M | yes |
| P9 | Emulation candidates: orafce (S), MariaDB Oracle mode (S-M), IvorySQL (M), openGauss A/B (M), Babelfish (M), openHalo (optional) | P1 | orafce BSD, IvorySQL Apache-2.0, Babelfish Apache-2.0 + PostgreSQL, openGauss Mulan PSL v2, MariaDB GPL-2 (server over the wire), **openHalo GPL-3 (flag)** - all as separate servers | S-M each | orafce, MariaDB, IvorySQL, openGauss: yes; Babelfish: image to be built for arm64 *(to be measured)*; openHalo arm64 *(to be checked)* |
| P10 | Workload corpus capture + cross-engine replay (a cross-engine `pt-upgrade`), frequency-weighted | `workload.py:40`, R11 harness | own (pt-upgrade GPL-2 only as prior art) | M | yes |
| P11 | Static application scan for the residue (ast-grep rules per sink + sqlglot), sqlcommenter tags to routes | P10 | ast-grep MIT, tree-sitter MIT, sqlglot MIT | M | yes (no database needed) |
| P12 | Constraint accept/reject probes + z3 CHECK equivalence | `check_schema` `hetero.py:179` | own; z3 MIT; SchemaAnalyst as method only (licence unconfirmed) | S-M | yes |
| P13 | Collation agreement matrix from real values | canon, sandbox | own; ICU in the PostgreSQL image | S | yes (SQL Server collations on SQL Edge; Oracle NLS on Oracle Free) |
| P14 | Types sized from data with recorded domain changes | planning scan, P12 | own | S | yes |
| P15 | Index/partition choice on the translated workload (HypoPG / Dexter; routing equivalence) | P10, `workload.py` plans | HypoPG PostgreSQL licence, Dexter MIT (as a program) | M | yes (PGDG packages) |
| P16 | Rule learning by anti-unification, proved on the corpus before adoption, proof record per rule | P2, P4, R11 fixture corpus | own | L | yes |
| P17 | Built-in semantics mined from engines' own test suites (SQuaLity-style) into rule candidates with declared caveats (SQLxDiff-style) | P16 | PostgreSQL regress (PostgreSQL licence), sqllogictest (public domain), **MySQL `mysql-test` GPL-2 - run as data in the sandbox, never vendored** | M | yes |
| P18 | Harness mutation score (seeded semantic mutants from the rule tables) in CI | R11 harness | own | S-M | yes |
| P19 | Emulation-as-oracle after cutover (dual endpoint on Babelfish/IvorySQL) | P9 | as P9 | M | depends on P9's images |
| P20 | Scalar routines: bounded verification of a Python model of both bodies (LLMLift/VERT style) | P3 | z3 MIT | L (research) | yes |
| P21 | Schema restructuring with a proof (Migrator/Mediator style) | P1, P12 | research code licences unchecked | L+ (research) | yes |

**Engines in the sandbox.** MySQL and PostgreSQL native (the whole loop is built and measured there first). SQL Server: SQL Edge on arm64 for T-SQL bodies within Edge's gaps (no CLR, `FORMAT`, `hierarchyid`...), full SQL Server 2022/2025 only as amd64 under Rosetta, which colima does not use (waits on the owner's VM profile, backlog R12/F5). Oracle: Oracle Free 23.5+ native on arm64 within its 2 GB / 2 CPU cap. Emulators: see P9.

### 7.4 Order of work (after R11's items 1-9)

1. **P2 + P18** (S, S-M): the bank and the harness mutation score first - they turn every later piece into a measured improvement instead of a claim.
2. **P10 + P6** (M, S): the real workload and call history - the largest input gain and the application-SQL proof in one piece.
3. **P1 + P4 + P3** (M each): portfolio, reduction, model as hole-filler - the loop.
4. **P12 + P13 + P14** (S-M): schema proofs - cheap, fully testable, and ahead of every vendor on day one.
5. **P9 (orafce, MariaDB, IvorySQL)** then **P19**: emulation as a proved candidate.
6. **P7, P8, P5** (M): deeper inputs and localisation.
7. **P16 + P17** (L, M): the learning rule base.
8. **P11, P15** (M): static scan, index/partition advice.
9. **P20, P21**: research tracks, only after the above is measured.

---

## 8. Capability table: the best paid tool today against migkit's leap

| Capability | Best paid tool today | Its mechanism | migkit's leap (mechanism) | Why it is better (measurable) | Effort | Testable in docker |
|---|---|---|---|---|---|---|
| Procedural code conversion | AWS DMS SC + generative AI; SnowConvert AI; Ispirer | vendor rule engine, model for the residue; AWS checks syntax only ("doesn't validate semantic correctness") | **candidate portfolio per object** (sqlglot, built-in + learned rules, Ora2Pg, SQLines, emulation, model as hole-filler, by-hand) chosen by **differential proof against the source engine as oracle** (P1) | *proved rate* (execution evidence + full target coverage) instead of a converted-% of syntax; false-green structurally zero | M (on R11) | yes: MySQL<->PostgreSQL; T-SQL on SQL Edge within its gaps; Oracle Free |
| Semantic verification | Datafold DMA; SnowConvert (2-sided for SQL Server); SSMA Tester | frozen aligned inputs + value diff (Datafold); baselines from logs/synthetic (SnowConvert); **user-typed** calls, writes to the source (SSMA) | input portfolio into one bank: class pools, body constants, fixture values, **real call history**, coverage-guided + **concolic** (z3), **stateful call sequences**, counterexample bank; coverage in the verdict; sandbox only (P2, P6-P8) | branch coverage stated per object; **harness mutation score** published in CI (P18); no source writes | M-L | yes |
| AI repair loop | Datafold DMA; Google DMS + Gemini | regenerate until parity; Gemini reviews output and propagates a user's fix as a suggestion | model **fills holes** in the best partial candidate from a **two-sided-reduced** repro; every round re-run against the whole bank; round cap (loops saturate at 2-5 in the literature) (P3) | model calls per proved object; zero regressions across rounds (bank); edits confined | M | yes (stub or local model) |
| Failure diagnosis | SSMA Tester report; AWS action items | per-test or per-object difference; action-item codes | **two-sided reduction** (inputs by Hypothesis, statements by ddmin) + **aligned intermediate state** naming the first diverging block (P4, P5) | repro size (statements, rows) and time to repro, reported per failure | M | yes |
| Rule base growth | Ispirer (engineers, 3-5 business days); Google (fix propagation suggested) | human- or model-written rules, not executed on every match | **anti-unification over accepted fixes**, adopted only after zero regressions on the public corpus + current objects, proof record per rule (P16) | fix multiplier; rule precision on the corpus; hours from a fix to an adopted rule | L | yes |
| Built-in function semantics | EDB repair handlers (ERH codes); AWS rule tables; Mallet (research) | hand- or model-written mappings | **engines' own regression suites run across engines** (SQuaLity/Sedar idea) -> executed mappings with declared caveats (SQLxDiff clause mappings) (P17) | count of built-ins per pair with executed evidence and named caveats | M | yes (MySQL test suite run as data only - GPL-2) |
| Emulation | EDB EPAS; Babelfish on Aurora; AWS extension packs | whole-database product choice; extension packs AWS-only and slow by AWS's own guidance | **emulation as a proved per-object candidate**; emulated object as **oracle for later native rewrites** in the same database; exit cost counted (P9, P19) | objects cut over unchanged *with* proof; lock-in count and proved-replacement count | S-M each | orafce, MariaDB, IvorySQL, openGauss yes; Babelfish arm64 image to be built |
| Application-embedded SQL | AWS SCT (Java incl. MyBatis, C#, C++, Pro*C; Oracle->PostgreSQL); AWS Transform (.NET EF/ADO.NET -> Aurora PostgreSQL) | static scan + rewrite; equivalence mechanism not published | **real workload corpus** (digest samples, bind capture, Query Store) translated and **replayed on both engines** over proven-equal data, frequency-weighted (a cross-engine `pt-upgrade`); ast-grep scan for code paths the log missed; sqlcommenter routes (P10, P11) | % of the application's executions proved; any engine pair; no language limit for the logged part | M | yes |
| Types | AWS SCT, Ora2Pg, MOLT | declaration-based maps (`PG_INTEGER_TYPE`) | sizing from measured data, domain narrowings recorded and exercised by negative probes (P14) | zero mid-copy truncations; every narrowing listed with its evidence | S | yes |
| Constraints preserved | EDB (% of DDL that ran); Ora2Pg `TEST` (object counts) | compile rate; counts | **accept/reject probe equivalence** per constraint on both engines (SchemaAnalyst's method) + z3 equivalence of CHECK predicates (P12) | constraints with executed accept/reject evidence; each difference named with its probe | S-M | yes |
| Collation | vendors map to one default (or Babelfish's own CI collations) | fixed mapping | **collation agreement matrix** over behaviour classes and the column's real values; cheapest agreeing candidate (ICU nondeterministic, `casefold()` index, `citext`) (P13) | classes that agree/differ, measured on the data | S | yes |
| Indexes / partitions | 1:1 translation (SCT, Ora2Pg); Azure SKU sizing only | translate declared indexes and partitions | HypoPG/Dexter on the **translated real workload**; partition **routing equivalence** at every bound (P15) | plan regressions found by `workload.py` driven to zero; index size added | M | yes (PostgreSQL) |
| Residue report | SnowConvert EWI/FDM; AWS action items + effort tables; EDB % | codes, severities, vendor effort tables | evidence classes (proved / caveat / emulated / not proved / not converted), frequency-weighted, each with a minimal repro, exit cost, and effort only from the team's own recorded fix times | every object carries evidence or a reason; no green without execution | S | yes |
| Schema restructuring | none open (Datometry's schema step is proprietary) | - | Migrator/Mediator-style synthesis of the changed code with a proof (P21) | redesign during migration becomes provable | L+ (research) | yes |

---

## Status caveats (points not confirmed from a primary page)

*to be confirmed*: openHalo's licence (GPL-3.0 read from a mirror's LICENSE); SchemaAnalyst's licence; the AWS SCT extension packs' licence terms (no public text found); the sqlcommenter main repository's licence (Apache-2.0 believed); arm64 images for Babelfish and openHalo; the exact PostgreSQL log line format for bind parameters; availability of Oracle's SQL Server/Sybase translators on Oracle Free; AWS Transform's "checks functional equivalence" mechanism (not published); no symbolic executor for PL/SQL, T-SQL or MySQL routines was found.

---

## Sources

Papers (arXiv ids or DOIs): Mallet, aiDM@SIGMOD 2024, doi 10.1145/3663742.3663973 (summary read at linli1724647576.github.io); CrackSQL, SIGMOD 2025, arXiv 2504.00882; RISE, ICSE 2026, arXiv 2601.05579; PARROT, arXiv 2509.23338; Horizon, PVLDB 18(12) (vldb.org/pvldb/vol18/p5259-emani.pdf); PLSQLBench arXiv 2608.15931; ProcArena arXiv 2609.06527; LLMLift / "Verified Code Transpilation with LLMs", NeurIPS 2024, arXiv 2406.03003; VERT, arXiv 2404.18852 and ASE 2025; Cheung, "LLM-Based Code Translation Needs Formal Compositional Reasoning", Berkeley EECS-2025-174; Orvalho, Janota, Manquinho, AAAI 2025 (github.com/pmorvalho/LLM-CEGIS-Repair); Li, Parsert, Polgreen arXiv 2403.03997; AutoCedar arXiv 2607.03656; invariant repair arXiv 2511.06552; LLM+SMT invariants arXiv 2508.00419; "Lost in Translation", ICSE 2024, arXiv 2308.03109; UniTrans, FSE 2024, arXiv 2404.14646; TransAgent arXiv 2409.19894; "How is Google using AI for internal code migrations?" arXiv 2501.06972; "Migrating Code At Scale With LLMs At Google" arXiv 2504.09691; Slutz, "Massive Stochastic Testing of SQL", VLDB 1998 (vldb.org/conf/1998/p618.pdf); SQLxDiff arXiv 2501.01236; SQLancer++ arXiv 2503.21424; SQuaLity arXiv 2410.21731; Sedar, ICSE 2024, doi 10.1145/3597503.3639210; Marcozzi, Vanhoof, Hainaut arXiv 1501.05265 and 1501.05821; "Symbolic execution of stored procedures in database management systems", ASE 2016, doi 10.1145/2970276.2970318; "Extending symbolic execution for automated testing of stored procedures", Software Quality Journal 2019; Refazer, ICSE 2017, arXiv 1608.09000; Getafix, OOPSLA 2019, doi 10.1145/3360585; QueryBooster, PVLDB 16 (vldb.org/pvldb/vol16/p2911-bai.pdf); WeTune, SIGMOD 2022, doi 10.1145/3514221.3526125; Mediator, POPL 2018, arXiv 1710.07660; Migrator, PLDI 2019, arXiv 1904.05498 (github.com/utopia-group/migrator); SchemaAnalyst, ICSME 2016 (github.com/schemaanalyst/schemaanalyst).

Tools and documentation: github.com/credativ/sqlreduce and the credativ SQLreduce post; github.com/renatahodovan/picire; github.com/orafce/orafce and pgxn.org/dist/orafce; AWS RDS "Using functions from the orafce extension"; Google Cloud SQL extensions page; Azure orafce announcement; ivorysql.org and github.com/IvorySQL/IvorySQL releases (5.4, 5.6); postgresql.org news on IvorySQL 4.0/5.4 and openHalo; github.com/HaloTech-Co-Ltd/openHalo and pigsty.io openHalo docs; babelfishpg.org (release 5.1.0 blog, migration, stored-procedure limitations, FAQ), github.com/babelfish-for-postgresql, github.com/jonathanpotts/docker-babelfishpg, AWS Aurora "Babelfish limitations"; openGauss documentation (compatibility modes, Dolphin), hub.docker.com opengauss/opengauss-server; MariaDB "SQL_MODE=ORACLE", CREATE PACKAGE and DBMS_OUTPUT docs, MariaDB 12.x/13.0 announcements; Oracle "SQL Translation and Migration Guide" and DBMS_SQL_TRANSLATOR; Datometry Hyper-Q technical brief and SIGMOD 2016 paper; ShardingSphere SQL Translator docs and issue discussion; AWS SCT "Using extension packs", "Converting application SQL" (Java, C#, C++, Pro*C pages); AWS Prescriptive Guidance "Migrate Oracle native functions to PostgreSQL using extensions"; AWS Transform SQL Server modernization docs and What's New (Dec 2025, Aug 2026); Percona `pt-upgrade` documentation; MySQL 8.0 manual "Performance Schema Statement Digests and Sampling" and WL#9830; google.github.io/sqlcommenter and github.com/open-telemetry/opentelemetry-sqlcommenter; ast-grep.github.io and github.com/ast-grep/ast-grep; PostgreSQL 18 release notes, CREATE COLLATION and collation docs, depesz "Support LIKE with nondeterministic collations".

migkit working tree (read only, 2026-09-28): `migkit/engines/hetero.py` (`convert_ddl` :2044, `converted_code` :2160, `_proposed` :2284, `PROOF_INPUTS` :2304, `prove_converted` :2315, `_call` :2398, `check_schema` :179), `migkit/assist.py` (`shares` :105, `propose` :112), `migkit/workload.py` (:40 `compare`), `pyproject.toml` (MIT; sqlglot; hypothesis in `dev`), `docs/backlog.md` (R11 :4325, F3 :2588, W3 :2687).
