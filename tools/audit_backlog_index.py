"""Check the DONE rows of docs/backlog-index.md against the code.

Each DONE row's title and detail give the distinctive words the work
names. A word is found when some module, class, function or constant of
`migkit/` has it as a part of its name (split on underscores and case).
A row whose words are mostly not found names a capability the code does
not show, and is printed for a person to check and reopen.

    python tools/audit_backlog_index.py [--below 0.5] [--min 3]

A row that is not DONE and says a thing is missing ("missing:", "does not
exist", "not in the code", "no X") is read the other way too: a backticked
module (`x.py`) or name in that clause which the package defines is printed
as a stale row, to be rewritten from the code.
"""
import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STOP = set("""
about above after again against all also and any are because been before
being between both but can cannot could does each either else every few for
from gets given has have having here how into its itself just keep keeps
kept made make makes many may more most must never not now off once only
onto other our out over own per same say says said should since some such
than that the their them then there these they this those through under
until upon use used uses using very was were what when where which while
who will with within without would yet your done missing built tests test
written grep commit commits merged merge state states row rows item items
one two three four five six seven eight nine ten first second third last
next real every thing things way ways work works worked
""".split())


def parts(name):
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    return {w.lower() for w in re.split(r"[^A-Za-z0-9]+", spaced) if w}


def names():
    """Every name defined in the package, split into its parts."""
    got = set()
    for path in (ROOT / "migkit").rglob("*.py"):
        got |= parts(path.stem)
        text = path.read_text(errors="replace")
        for m in re.finditer(r"^\s*(?:def|class)\s+(\w+)", text, re.M):
            got |= parts(m.group(1))
        for m in re.finditer(r"^([A-Z][A-Z0-9_]+)\s*=", text, re.M):
            got |= parts(m.group(1))
        for m in re.finditer(r"^\s*(\w+)\s*(?::[^=\n]+)?=", text, re.M):
            got |= parts(m.group(1))
    return got


def stem(word):
    for tail in ("ing", "ed", "es", "s"):
        if word.endswith(tail) and len(word) - len(tail) >= 4:
            return word[:-len(tail)]
    return word


def rows():
    for n, line in enumerate((ROOT / "docs/backlog-index.md")
                             .read_text().splitlines(), 1):
        cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
        if len(cells) >= 5 and cells[0].isdigit() \
                and cells[3].startswith("DONE"):
            yield n, cells


def defined():
    """Every module stem and every function, class or constant name the
    package defines, whole (not split into parts)."""
    got = set()
    for path in (ROOT / "migkit").rglob("*.py"):
        got.add(path.stem)
        text = path.read_text(errors="replace")
        got |= set(re.findall(r"^\s*(?:def|class)\s+(\w+)", text, re.M))
        got |= set(re.findall(r"^([A-Z][A-Z0-9_]+)\s*=", text, re.M))
    return got


SAYS_MISSING = re.compile(
    r"(missing:|does not exist|not in the code|is not in the code|"
    r"are not in the code|\bno `)[^;]*", re.I)


def stale_rows():
    """[(line, row id, [names])]: rows not DONE whose 'missing' clause
    names something the package has."""
    have = defined()
    out = []
    for n, line in enumerate((ROOT / "docs/backlog-index.md")
                             .read_text().splitlines(), 1):
        cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
        if len(cells) < 5 or not cells[0].isdigit() \
                or cells[3].startswith("DONE"):
            continue
        found = []
        for clause in SAYS_MISSING.finditer(cells[3]):
            for tok in re.findall(r"`([^`]+)`", clause.group(0)):
                name = tok[:-3] if tok.endswith(".py") else \
                    tok.split("(")[0].split(".")[-1]
                if name in have and len(name) > 4 and (
                        "_" in name or tok.endswith(".py") or "." in tok):
                    found.append(tok)
        if found:
            out.append((n, cells[1], sorted(set(found))))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--below", type=float, default=0.5)
    ap.add_argument("--min", type=int, default=3)
    args = ap.parse_args()
    have = {stem(w) for w in names()}
    out = []
    for n, cells in rows():
        title = re.sub(r"`[^`]*`", " ", cells[2])
        words = {stem(w) for w in re.findall(r"[a-z]{5,}", title.lower())}
        words = {w for w in words if w not in STOP}
        if len(words) < args.min:
            continue
        found = {w for w in words if w in have}
        score = len(found) / len(words)
        if score < args.below:
            out.append((score, n, cells[0], cells[1], sorted(words - found)))
    for score, n, num, key, lacking in sorted(out):
        print(f"{score:.2f} line {n} row {num} {key}: not in code: "
              + ", ".join(lacking))
    print(f"{len(out)} rows below {args.below}", file=sys.stderr)
    stale = stale_rows()
    for n, key, toks in stale:
        print(f"stale line {n} row {key}: says missing, the code has: "
              + ", ".join(toks))
    print(f"{len(stale)} stale rows", file=sys.stderr)
    dup = {}
    for n, line in enumerate((ROOT / "docs/backlog-index.md")
                             .read_text().splitlines(), 1):
        cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
        if len(cells) < 5 or not cells[0].isdigit():
            continue
        dup.setdefault(cells[0], []).append(n)
    for num, lines in dup.items():
        if len(lines) > 1:
            print(f"row number {num} used on lines {lines}")


if __name__ == "__main__":
    main()
