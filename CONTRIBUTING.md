# Contributing to migkit

Thanks for helping make migkit the most complete database migration verifier.

## Getting set up

```bash
./bootstrap.sh && source .venv/bin/activate
.venv/bin/python -m pytest -q -m "not docker"   # unit + fail-case only, no docker
.venv/bin/python tools/final_suite.py           # the whole suite, as CI runs it
```

The test suite starts throwaway database containers, so a working Docker
daemon is needed for the full run. `tools/final_suite.py` runs the pure tests
side by side, then the docker tests in lanes on the default colima profile and
the migkit VM at once, and writes a report to `reports/final-suite/`. A single
docker test file runs through the lock:
`python tools/with_docker_lock.py python -m pytest -q tests/test_x.py`
(`--vm migkit` for Oracle, SQL Server and the x86 images). New tests take
their servers from `tests/servers.py` (the pattern is at its top) and wait
with `tests/waits.py:until`.

## Adding a check

Every check lives inside the engine it applies to (`migkit/engines/*.py`) and is
auto-discovered from the connected hop. The rule is: no new user-facing modes or
flags. A `check` should just start doing the right thing on the databases it is
pointed at.

- Deep, table/row-level checks go in the engine's `check_deep(db)` and return
  `Result(...)` objects (`ok` / `diff`), most-actionable message first.
- Source-side readiness and operational health go in `assess()`.
- Each check should distinguish real corruption from cosmetic difference, and
  its message should name what is wrong and how to fix it.
- Add a docker test under `tests/` that reproduces the failure and asserts the
  check catches it. Reproduce the corruption, run the check, assert on the
  `Result`.

## Code style

- Match the surrounding code: minimal comments, no decorative punctuation.
- Keep rationale in docs or the commit body, not scattered through the file.
- No em-dashes, arrows, or emoji in code, comments, or docs.

## Commits and pull requests

- One logical change per pull request; squash to a single commit before review.
- Use a conventional, one-line subject (`feat:`, `fix:`, `docs:`, `test:`).
- Never include AI-attribution or `Co-Authored-By` trailers.
- Never commit real credentials. `conf/hops.yaml` is gitignored; use
  `env:` / `file:` / `vault:` references, never plaintext.
- Run the full suite and confirm it is green before opening the PR.

## Reporting problems

Use the issue templates. For anything security-sensitive, see
[SECURITY.md](SECURITY.md) instead of opening a public issue.
