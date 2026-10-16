"""Write the measured table of docs/research/autonomous-comparison.md from
the scenario harness's lines (`MIGKIT_MEASURE_OUT` of
tests/test_autonomous_scenarios.py), the same way every run:

    python tools/measures_table.py MEASURES.jsonl [MORE.jsonl ...]

The cells are `autonomous.comparison`'s - the last run of a scenario on an
engine counts; a row no run measured, and the managed-service column, stay
as they were - so `test_the_table_is_the_runs` reads the page back the same.
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/research/autonomous-comparison.md"


def write(lines, doc=DOC):
    """The page with its table filled from `lines`; True when it changed."""
    sys.path.insert(0, str(ROOT))
    from migkit import autonomous
    before = doc.read_text()
    after = autonomous.comparison(lines, before)
    doc.write_text(after)
    return after != before


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measures", nargs="+", type=Path)
    parser.add_argument("--doc", type=Path, default=DOC)
    args = parser.parse_args(argv)
    lines = [line for f in args.measures for line in f.read_text().splitlines()]
    changed = write(lines, args.doc)
    print(f"{args.doc.name}: {'rewritten' if changed else 'unchanged'}"
          f" from {len([x for x in lines if x.strip()])} measured lines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
