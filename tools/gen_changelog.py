#!/usr/bin/env python3
"""Build the release notes for a tag from the commits since the previous tag.

Commit subjects here follow `area: what changed`, so the area is the natural
grouping and no extra metadata is needed. Anything without a recognised prefix
lands under "Other" rather than being dropped - a release note that silently
omits a change is worse than an untidy one.

  gen_changelog.py <tag>              notes for that tag, to stdout
  gen_changelog.py <tag> --update     also prepend them to CHANGELOG.md
"""
import re
import subprocess
import sys
from pathlib import Path

SECTIONS = [
    ("feat", "Added"),
    ("feature", "Added"),
    ("fix", "Fixed"),
    ("perf", "Performance"),
    ("refactor", "Changed"),
    ("style", "Changed"),
    ("chore", "Maintenance"),
    ("ci", "Maintenance"),
    ("build", "Maintenance"),
    ("test", "Tests"),
    ("tests", "Tests"),
    ("docs", "Documentation"),
    ("guard", "Security"),
    ("security", "Security"),
]
ORDER = ["Added", "Fixed", "Changed", "Performance", "Security", "Tests",
         "Documentation", "Maintenance"]
# a prefix that is not a conventional type is the area of the code it touched,
# which makes a better heading than dumping it in a catch-all
AREA_NAMES = {"pg": "PostgreSQL", "postgres": "PostgreSQL", "mysql": "MySQL",
              "mongo": "MongoDB", "mongodb": "MongoDB", "mssql": "SQL Server",
              "cli": "CLI", "api": "API", "ci": "Maintenance"}


def sh(*args):
    p = subprocess.run(args, capture_output=True, text=True)
    return p.stdout.strip() if p.returncode == 0 else ""


def previous_tag(tag):
    """the tag before this one, or nothing when this is the first"""
    tags = sh("git", "tag", "--sort=-creatordate").splitlines()
    if tag in tags:
        i = tags.index(tag)
        return tags[i + 1] if i + 1 < len(tags) else ""
    return tags[0] if tags else ""


def commits(since, until):
    rng = f"{since}..{until}" if since else until
    out = sh("git", "log", "--no-merges", "--format=%s|%h", rng)
    return [l.split("|", 1) for l in out.splitlines() if "|" in l]


def group(entries):
    buckets = {}
    for subject, sha in entries:
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_-]*)(\([^)]*\))?!?:\s*(.+)$", subject)
        if m:
            prefix = m.group(1).lower()
            head = next((t for p, t in SECTIONS if p == prefix), None)
            if head is None:
                head = AREA_NAMES.get(prefix, prefix.replace("-", " ").title())
            text = m.group(3)
        else:
            head, text = "Other", subject
        buckets.setdefault(head, []).append((text, sha))
    return buckets


def headings(buckets):
    """known headings in a fixed order, then the areas, then the leftovers"""
    known = [h for h in ORDER if h in buckets]
    areas = sorted(h for h in buckets if h not in ORDER and h != "Other")
    return known + areas + (["Other"] if "Other" in buckets else [])


def render(tag, entries):
    date = sh("git", "log", "-1", "--format=%ad", "--date=short", tag) or ""
    lines = [f"## {tag}" + (f" - {date}" if date else ""), ""]
    if not entries:
        lines += ["No changes recorded.", ""]
        return "\n".join(lines)
    buckets = group(entries)
    for head in headings(buckets):
        lines.append(f"### {head}")
        for text, sha in buckets[head]:
            lines.append(f"- {text} ({sha})")
        lines.append("")
    return "\n".join(lines)


def main(tag, update):
    prev = previous_tag(tag)
    entries = commits(prev, tag)
    notes = render(tag, entries)
    if prev:
        notes += f"\nFull comparison: {prev}...{tag}\n"
    print(notes)
    if update:
        p = Path("CHANGELOG.md")
        old = p.read_text() if p.exists() else "# Changelog\n\n"
        head, _, rest = old.partition("\n\n")
        p.write_text(f"{head}\n\n{notes}\n{rest}")
        print(f"prepended to {p}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: gen_changelog.py <tag> [--update]", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1], "--update" in sys.argv))
