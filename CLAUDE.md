# Working in migkit

## Find before you search, look before you create

- **Where things are**: read `docs/agents/CODEMAP.md` first (every module in one line, and the hubs most changes touch). For a symbol, `grep -P '^name\t' .codemap/SYMBOLS.tsv` gives its `file:line`; the Serena tools (`find_symbol`, `find_referencing_symbols`) and `tags` answer the same by meaning. Search the tree only for what these do not hold.
- **What changed**: `python tools/gen_codemap.py --since <ref>` - modules and symbols added, removed or changed.
- **What migkit can do**: ask the code - `decide.coverage()` (every engine, cell by cell) and `capabilities.matrix()` - never memory or a summary.
- **Before a new file or name**: look it up in the map and the symbols. A new file goes only where the map has none; an existing file is read, then edited. A new method name is checked against its class.

## One place

Logic lives once; reach for the owner rather than a second reading: the scope is `Hop.excluded` / `Engine.in_scope` / `Engine.takes` / `Hop.carries` / `Hop.target_table_in`; the verdict is `verdict.decide`; a row filter is `rowfilter`; a column transform is `transforms`; hop keys are `hopkeys.HOP_KEYS` / `HOP_OPTIONS`; environment variables are `envkeys`.

## Tests

Write tests with every change; run them once, at the end (`tools/final_suite.py`). Mid-way, run only `python -m compileall -q migkit tests` and the guards: `tests/test_the_report_does_not_name_its_tools.py`, `tests/test_no_method_is_said_twice.py`, `tests/test_the_code_map_is_current.py`. Never connect to a real database: the docker sandbox only.

## The public repo

- No organisation's own names, hosts, addresses, accounts or credentials: the pre-commit hook checks (`tools/check_no_secrets.py`, with the local deny-list); use `example.com`, `10.0.0.x`, `CHANGE_ME`.
- User-facing text never names a wrapped program.
- Commits: one line, no trailers. Never push - the owner pushes.
- The manual (`tools/gen_docs.py`) and the code map (`tools/gen_codemap.py`) are written from the code; when the hook stops a commit for them, add the written files and commit again.
