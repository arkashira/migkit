#!/usr/bin/env python3
"""Guard for an open-source repo: fail if anything organisation-specific is about
to be committed. migkit is a generic tool - real hostnames, IPs, account ids,
cluster names and credentials belong in the operator's own conf/hops.yaml
(gitignored), never in the source.

Usage:
  python tools/check_no_secrets.py            scan the working tree
  python tools/check_no_secrets.py --staged   scan what git has staged (pre-commit)
Exit 1 with the offending file:line if anything matches.
"""
import re, subprocess, sys, pathlib

PATTERNS = [
    (r"\b(?:10|172|192)\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", "private IP address"),
    (r"[a-z0-9-]+\.(?:rds|docdb)\.amazonaws\.com", "real RDS/DocDB endpoint"),
    (r"(?<![\w.\-])\d{12}(?![\w\-])", "AWS account id"),
    (r"\bcdb-[0-9a-z]{8}\b", "TencentDB instance id"),
    (r"\b(?:postgres|cmgo|crs)-[0-9a-z]{8}\b", "Tencent instance id"),
    (r"AKIA[0-9A-Z]{16}", "AWS access key"),
    (r"(?i)password\s*[:=]\s*[\"'][^\"'<>{}\s]{6,}[\"']", "hardcoded password"),
]
SKIP_FILES = {"check_no_secrets.py"}
#: the names of one's own organisation - its projects, databases, tables,
#: services - one word or phrase a line, kept on this machine only (never
#: in the repo, where the list would itself be the leak); a word matches
#: case-insensitively as a whole word
DENY_FILE = pathlib.Path(
    __import__("os").environ.get("MIGKIT_DENY_WORDS_FILE")
    or pathlib.Path.home() / ".config" / "migkit" / "deny-words.txt")


def deny_words():
    try:
        lines = DENY_FILE.read_text().splitlines()
    except OSError:
        return []
    return [w.strip() for w in lines
            if w.strip() and not w.strip().startswith("#")]


def deny_pattern(words):
    if not words:
        return None
    return re.compile(r"(?i)(?<![A-Za-z0-9])(?:"
                      + "|".join(re.escape(w) for w in words)
                      + r")(?![A-Za-z0-9])")
# deliberately fake values used in docs and examples
ALLOW = re.compile(
    r"xxxx|example|placeholder|<[a-z_-]+>|CHANGE_ME|your-|my-|dummy|sample"
    r"|0\.0\.0\.0|127\.0\.0\.1|localhost"
    r"|10\.0\.0\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.16\.\d{1,3}\.\d{1,3}"
    r"|192\.0\.2\.\d{1,3}"
    r"|password:\s*\"secret\"|password=\"secret\"")

def files(staged):
    """only what git actually carries - a gitignored conf/hops.yaml holds the
    operator's real endpoints on purpose and is not a leak"""
    cmd = (["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"] if staged
           else ["git", "ls-files"])
    out = subprocess.run(cmd, capture_output=True, text=True).stdout.split()
    return [pathlib.Path(f) for f in out if pathlib.Path(f).is_file()]


def is_binary(p):
    try:
        return b"\0" in p.open("rb").read(2048)
    except Exception:
        return True


def flagged(line, deny=None):
    if deny is not None and deny.search(line):
        # one's own names are never an example
        return "a name of one's own organisation"
    if ALLOW.search(line):
        return None
    return next((what for pat, what in PATTERNS if re.search(pat, line)), None)


def main(staged):
    hits = []
    deny = deny_pattern(deny_words())
    for p in files(staged):
        if p.name in SKIP_FILES or is_binary(p):
            continue
        try:
            text = p.read_text(errors="ignore")
        except Exception:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            what = flagged(line, deny)
            if what:
                hits.append((p, i, what, line.strip()[:70]))
    for p, i, what, line in hits:
        print(f"  {p}:{i}  {what}\n      {line}")
    if hits:
        print(f"\n{len(hits)} organisation-specific value(s) found - keep them in "
              f"conf/hops.yaml (gitignored), not in the source.")
        return 1
    print("clean: nothing organisation-specific in the source")
    return 0


if __name__ == "__main__":
    sys.exit(main("--staged" in sys.argv))
