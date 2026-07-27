## What & why
<!-- what this changes and the problem it solves -->

## How tested
<!-- commands run, engines covered (postgres / mysql / mongo), new or updated tests -->

## Checklist
- [ ] `pytest -q` green locally
- [ ] read-only paths stay read-only; every write is dry-run unless `--apply`/`--go`, saves undo, and is re-runnable
- [ ] no secrets or credentials committed (`conf/hops.yaml` stays gitignored)
- [ ] docs / README updated if a flag or behavior changed
