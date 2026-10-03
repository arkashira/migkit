# Working in migkit

## Find before you search, look before you create

- **Where things are**: read `docs/agents/CODEMAP.md` first (every module in one line, and the hubs most changes touch). For a symbol, `grep -P '^name\t' .codemap/SYMBOLS.tsv` gives its `file:line`; the Serena tools (`find_symbol`, `find_referencing_symbols`) and `tags` answer the same by meaning. Search the tree only for what these do not hold.
- **What changed**: `python tools/gen_codemap.py --since <ref>` - modules and symbols added, removed or changed.
- **What migkit can do, and what is left**: `docs/agents/STATE.md` (the capability table and the decision engine's coverage for every engine, every cell not yet done, the backlog by status), written from the code on every commit; `.codemap/LOCAL.md` says what this machine last proved (the full suite, unpushed work, agents' branches kept). Never answer it from memory or a summary.
- **Who calls what** (the reach of a change): `grep -P '^name\t' .codemap/CALLERS.tsv`, or Serena's `find_referencing_symbols`.
- **Before a new file or name**: look it up in the map and the symbols. A new file goes only where the map has none; an existing file is read, then edited. A new method name is checked against its class.

## One place

Logic lives once; reach for the owner rather than a second reading: the scope is `Hop.excluded` / `Engine.in_scope` / `Engine.takes` / `Hop.carries` / `Hop.target_table_in`; the verdict is `verdict.decide`; a row filter is `rowfilter`; a column transform is `transforms`; hop keys are `hopkeys.HOP_KEYS` / `HOP_OPTIONS`; environment variables are `envkeys`.

## Tests

Write tests with every change; run them once, at the end (`tools/final_suite.py`). Mid-way, run only `python -m compileall -q migkit tests` and the guards: `tests/test_the_report_does_not_name_its_tools.py`, `tests/test_no_method_is_said_twice.py`, `tests/test_the_code_map_is_current.py`, `tests/test_every_engine_is_as_deep_as_postgres_and_mysql.py` (the depth matrix: a way you add needs a test naming it in its module's `PROVES`, or an `UNPROVED` entry; a gap you close loses its declaration in `capabilities.DEPTH_GAPS`), and `tests/test_the_scope_reads_alike_everywhere.py::test_a_listed_table_is_asked_of_the_scope_one_way`. Never connect to a real database: the docker sandbox only.

## The public repo

- No organisation's own names, hosts, addresses, accounts or credentials: the pre-commit hook checks (`tools/check_no_secrets.py`, with the local deny-list); use `example.com`, `10.0.0.x`, `CHANGE_ME`.
- User-facing text never names a wrapped program.
- Commits: one line, no trailers. Never push - the owner pushes.
- The manual (`tools/gen_docs.py`) and the code map (`tools/gen_codemap.py`) are written from the code; when the hook stops a commit for them, add the written files and commit again.
