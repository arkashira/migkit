# Merge gates

Owner rule 2026-10-17: a backlog row becomes DONE only after an independent
review (the critic, a different model from the builder) recorded here with a
PASS verdict. `tools/check_merge_gate.py --staged` runs in the pre-commit hook
and stops a commit that sets a row DONE with no PASS line for it. A row that
fails review is set to REVIEW with the finding in its status cell; the lead
re-dispatches it with the finding and the row comes back here when it passes.

| row | date | verdict | reviewer | commit or branch | note |
|---|---|---|---|---|---|
| 616 | 2026-10-17 | PASS | critic-reviewer | master | 13/13 tests pass; keyless effect atomic checkpointer; pure unix |
| 617 | 2026-10-17 | PASS | critic-reviewer | master | 16/16 tests pass; schema epoch dependency planner; pure unix |
| 618 | 2026-10-17 | PASS | critic-reviewer | master | 19/19 tests pass; shadow resync coordinator; pure unix |
| 672 | 2026-10-17 | PASS | critic-reviewer | master | 15/15 tests pass; partial access scope honest coverage; pure unix |
| 673 | 2026-10-17 | PASS | critic-reviewer | master | 17/17 tests pass; adaptive bounds envelope validator; pure unix |
| 675 | 2026-10-17 | PASS | critic-reviewer | master | 25/25 tests pass; dynamic lineage evidence auditor; pure unix |
| 676 | 2026-10-17 | PASS | critic-reviewer | master | 12/12 tests pass; demand benchmark harness; pure unix |
