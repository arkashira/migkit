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


def difference_kind(count_a, key_a, count_b, key_b):
    """Name the shape of a difference from counts and primary-key hashes.

    Engine-independent on purpose. PostgreSQL sums per-row md5 as numeric and
    MySQL folds it with BIT_XOR, but the *reasoning* is the same in both, and
    the last time this kind of logic existed twice the two copies drifted -
    one of them planned key ranges from `min(pk)` and silently skipped every
    row below it.

    Either key hash may be None, for a table with no primary key: then there
    is nothing to reason with and the honest answer is no answer.

        keys differ, counts differ  -> rows are missing or extra
        keys differ, counts equal   -> rows were replaced
        keys equal                  -> values were edited in place
    """
    if key_a is None or key_b is None:
        return ""
    if str(key_a) != str(key_b):
        if count_a != count_b:
            n = abs(int(count_a) - int(count_b))
            side = "missing" if int(count_a) > int(count_b) else "extra"
            return f"rows-{side} by={n}"
        return "rows-replaced"
    return "values-changed"


def difference_kind_from_counts(missing, extra, changed):
    """The same vocabulary, for an engine that counts instead of inferring.

    MongoDB compares by `_id` set, so it *knows* how many documents are
    missing, extra and changed; the SQL engines infer the shape from key and
    row hashes. Two genuinely different situations, deliberately sharing one
    module so the words cannot drift apart - a MySQL `rows-missing` and a
    MongoDB `rows-missing` have to mean the same thing to be worth
    aggregating.
    """
    missing, extra, changed = int(missing), int(extra), int(changed)
    if missing and extra:
        if missing == extra:
            return "rows-replaced"
        return f"rows-missing by={missing},rows-extra by={extra}"
    if missing:
        return f"rows-missing by={missing}"
    if extra:
        return f"rows-extra by={extra}"
    if changed:
        return "values-changed"
    return ""


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


def summarize(hop, records, engine=None, load=None, coverage=None):
    """Build the normalized envelope for one check run.

    `load` is what the throttle did, when it did anything: a run that took
    three times as long because the server was busy should say so, not just
    be slow.

    `coverage` is what the run was narrowed to, and it exists because this
    file is read by a machine rather than a person. Measured before it did:
    a `--table public.good --only data` run over a database whose *other*
    table was missing 60 of its 100 rows produced an envelope
    **byte-identical** to a full run over a healthy database - same
    `status: same`, same `has_differences: false`, same totals. A gate
    reading `has_differences` passed both. The information existed in
    `summary.json` beside it and stopped short of the file automation
    reads.

    A narrowed run that found nothing therefore reports `incomplete`
    rather than `same`: the vocabulary already had a word for "nothing
    found, not everything looked at". `has_differences` stays `false`,
    because no difference *was* found and saying otherwise would be a
    second lie in the other direction.
    """
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
    elif coverage:
        status = "incomplete"
    else:
        status = "same"
    return {
        **({"coverage": coverage} if coverage else {}),
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
        **({"load": load} if load else {}),
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


def write(hop, records, engine=None, load=None, coverage=None):
    """Write verdict.json for this hop and return (path, envelope)."""
    env = summarize(hop, records, engine, load, coverage)
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
