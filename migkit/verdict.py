"""The one result shape, whatever engine produced it.

A check run against MongoDB and a check run against PostgreSQL answer the same
question - is the target the same as the source - so they should answer it in
the same words. Per-engine wording belongs in `detail`, for a human to read;
everything a machine or a report groups by lives in `category` and `status`,
which mean the same thing on every engine.

`verdict.json` is written next to `summary.json` on every check. summary.json
stays as it is, so anything already reading it keeps working.

Bump `FORMAT_VERSION` when a category is renamed or removed; adding one is not
a breaking change.
"""
import hashlib
import json
import time

from . import __version__
from .engines.base import STATUSES

FORMAT_VERSION = 1


def _counts(records, key):
    out = {}
    for r in records:
        k = r.get(key) or "unknown"
        st = r.get("status") or "unknown"
        out.setdefault(k, {})[st] = out.setdefault(k, {}).get(st, 0) + 1
    return out


def fingerprint(records):
    """Stable hash of the findings, ignoring wording and ordering.

    Only (category, scope, status) goes in: `detail` carries row counts and
    timestamps that move between runs without the verdict changing, so
    including it would make every fingerprint unique and useless.
    """
    rows = sorted((r.get("category", ""), str(r.get("scope", "")),
                   r.get("status", "")) for r in records)
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def summarize(hop, records, engine=None):
    """Build the normalized envelope for one check run."""
    totals = {s: 0 for s in STATUSES}
    for r in records:
        st = r.get("status", "unknown")
        totals[st] = totals.get(st, 0) + 1
    if totals.get("error"):
        status = "error"
    elif totals.get("diff"):
        status = "different"
    elif totals.get("skip") and not (totals.get("ok") or totals.get("warn")):
        status = "incomplete"
    else:
        status = "same"
    return {
        "format_version": FORMAT_VERSION,
        "tool": "migkit",
        "tool_version": __version__,
        "hop": hop.name,
        "engine": engine or hop.engine,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "status": status,
        "has_differences": bool(totals.get("diff") or totals.get("error")),
        "totals": totals,
        "by_category": _counts(records, "category"),
        "fingerprint": fingerprint(records),
        "findings": [
            {"category": r.get("category", ""),
             "check": r.get("check", ""),
             "scope": r.get("scope", ""),
             "status": r.get("status", ""),
             "detail": r.get("detail", ""),
             "report": r.get("report", ""),
             "fix_hint": r.get("fix_hint", "")}
            for r in records
            if r.get("status") not in ("ok", "skip")
        ],
    }


def write(hop, records, engine=None):
    """Write verdict.json for this hop and return (path, envelope)."""
    env = summarize(hop, records, engine)
    p = hop.report_dir() / "verdict.json"
    p.write_text(json.dumps(env, indent=1, sort_keys=True, default=str))
    return p, env


def unchanged_since(hop, records):
    """True when this run's findings match the last one exactly.

    Lets a repeated check say "nothing moved" without the reader having to
    diff two reports by eye.
    """
    p = hop.report_dir() / "verdict.json"
    if not p.exists():
        return False
    try:
        prev = json.loads(p.read_text())
    except ValueError:
        return False
    return prev.get("fingerprint") == fingerprint(records)
