"""A backlog row goes DONE only through a recorded review.

Owner rule 2026-10-17: every row that becomes DONE in a commit has a line in
docs/agents/GATES.md saying who reviewed it and that the verdict was PASS.
The pre-commit hook runs this with --staged; a row set DONE without its gate
line stops the commit. A row set to REVIEW, PARTLY or OPEN needs nothing.

GATES.md rows: | row | date | verdict | reviewer | commit or branch | note |
"""
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
INDEX = "docs/backlog-index.md"
GATES = "docs/agents/GATES.md"


def _rows(text):
    """{row id: status word} of a backlog index."""
    out = {}
    for line in text.splitlines():
        cells = [c.strip() for c in line.split("|")]
        if len(cells) > 5 and cells[1].isdigit():
            out[cells[1]] = cells[4].split()[0] if cells[4] else ""
    return out


def _show(ref, path):
    got = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT,
                         capture_output=True, text=True)
    return got.stdout if got.returncode == 0 else ""


def passed_rows(gates_text):
    """Row ids with a PASS verdict recorded in GATES.md."""
    rows = set()
    for line in gates_text.splitlines():
        cells = [c.strip() for c in line.split("|")]
        if len(cells) > 4 and cells[1].isdigit() and cells[3].upper().startswith("PASS"):
            rows.add(cells[1])
    return rows


def ungated(before, after, gates):
    """Rows that became DONE between two index texts without a PASS gate."""
    old, new = _rows(before), _rows(after)
    ok = passed_rows(gates)
    return sorted((r for r, st in new.items()
                   if st == "DONE" and old.get(r) != "DONE" and r not in ok),
                  key=int)


def main(argv):
    staged = "--staged" in argv
    after = _show(":", INDEX) if staged else (ROOT / INDEX).read_text()
    gates = _show(":", GATES) if staged else (ROOT / GATES).read_text() \
        if (ROOT / GATES).exists() else ""
    if not after:
        return 0
    bad = ungated(_show("HEAD", INDEX), after, gates)
    if bad:
        print("merge gate: rows set DONE without a PASS line in "
              f"{GATES}: {', '.join(bad)}")
        print("record the critic's verdict there (row | date | PASS | "
              "reviewer | commit | note), stage it, and commit again; "
              "a row the critic did not pass stays REVIEW.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
