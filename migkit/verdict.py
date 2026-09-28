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

    Engine-independent on purpose. PostgreSQL and MySQL each sum per-row
    md5 prefixes in their own way, but the *reasoning* is the same in both, and
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


def _rows(n):
    return f"{n} row{'' if n == 1 else 's'}"


def partition_differences(table, src, dst):
    """What differs between one partitioned table's partitions, a line each.

    `src` and `dst` are `{partition: {"rows", "digest", "bound",
    "catchall"}}` as each engine reads them - PostgreSQL's leaf partitions,
    MySQL's `PARTITION (p)` - so the words are the same on both. A digest
    of None was not computed, and only the counts are held to each other.

    Measured on a DTS leg: the current month's partitions arrived empty
    while the parents' totals looked plausible, the rows sitting in the
    catch-all or nowhere. A partition is paired by name, and one renamed on
    the way by its bound.
    """
    lines = []
    only_src = {n: p for n, p in src.items() if n not in dst}
    only_dst = {n: p for n, p in dst.items() if n not in src}
    pairs = [(n, n) for n in sorted(set(src) & set(dst))]
    by_bound = {}
    for n, p in only_dst.items():
        if p.get("bound"):
            by_bound.setdefault(p["bound"], []).append(n)
    for n in sorted(only_src):
        there = by_bound.get(only_src[n].get("bound") or "")
        if there and len(there) == 1 and there[0] in only_dst:
            d = there[0]
            pairs.append((n, d))
            lines.append((2, f"{table} partition {n}: named {d} on target"))
            del only_dst[d]
    for n in sorted(set(only_src) - {s for s, _ in pairs}):
        rows = int(only_src[n].get("rows") or 0)
        lines.append((0, f"{table} partition {n}: missing on target"
                         + (f", the source's holds {_rows(rows)}" if rows
                            else " (empty on the source)")))
    for n in sorted(only_dst):
        rows = int(only_dst[n].get("rows") or 0)
        lines.append((1, f"{table} partition {n}: extra on target"
                         + (f", holding {_rows(rows)}" if rows
                            else " (empty)")))
    for sn, dn in pairs:
        s, d = src[sn], dst[dn]
        a, b = int(s.get("rows") or 0), int(d.get("rows") or 0)
        if s.get("bound") and d.get("bound") and s["bound"] != d["bound"]:
            lines.append((1, f"{table} partition {sn}: bound differs"
                             f" src({s['bound']}) dst({d['bound']})"))
        if a and not b:
            lines.append((0, f"{table} partition {sn}: empty on target, the"
                             f" source's holds {_rows(a)}"))
        elif b > a and (s.get("catchall") or d.get("catchall")):
            lines.append((0, f"{table} partition {sn}: {_rows(b - a)}"
                             f" stranded in the catch-all on target"
                             f" (src={a} dst={b})"))
        elif a != b:
            lines.append((1, f"{table} partition {sn}: src={a} dst={b} rows"))
        elif s.get("digest") is not None and d.get("digest") is not None \
                and str(s["digest"]) != str(d["digest"]):
            lines.append((1, f"{table} partition {sn}: {_rows(a)} both"
                             " sides, content differs"))
    return [line for _, line in sorted(lines, key=lambda x: x[0])]


def uniform_shift(name, src, dst, least=3):
    """The line naming a timestamp column every row of which the target
    holds shifted by a timezone's offset, or None.

    `src` and `dst` map a row's key to its instant as epoch seconds, read
    with the session pinned to UTC on both sides, so a faithful copy
    differs by 0. A single delta on every row is a conversion applied to
    the whole column - a mover's non-UTC session - not rows corrupted one
    by one. So are two deltas an hour apart, neither zero: a zone with
    daylight saving, whose offset depends on the row's date. Anything else
    is left to the row comparison, which names the rows.
    """
    deltas = [float(dst[k]) - float(src[k]) for k in src if k in dst]
    if len(deltas) < least:
        return None
    if max(deltas) - min(deltas) < 1:
        avg = sum(deltas) / len(deltas)
        if abs(avg) < 1:
            return None
        secs = round(avg)
        return (f"{name}: every row shifted {secs}s"
                f" (~{secs / 3600:.1f}h)")
    seen = sorted({round(x) for x in deltas})
    if len(seen) == 2 and seen[1] - seen[0] == 3600 and 0 not in seen \
            and all(x % 900 == 0 for x in seen):
        return (f"{name}: every row shifted {seen[0]}s or {seen[1]}s"
                f" (~{seen[0] / 3600:.1f}h/{seen[1] / 3600:.1f}h, a zone"
                " with daylight saving)")
    return None


def _capacity(engine, declared):
    """`canon.capacity`, and a length or precision nobody set read as the
    unlimited one it is rather than as unmeasured."""
    from . import canon
    cap = canon.capacity(engine, declared)
    if cap is None:
        name = str(declared).lower().split("(")[0].strip()
        if name in canon.CHAR_TYPES.get(engine, ()) and "(" not in \
                str(declared):
            return ("chars", None)
        if name in canon.NUMERIC_TYPES.get(engine, ()) and "(" not in \
                str(declared):
            return ("numeric", None, None)
    return cap


def narrowing(engine, src_type, dst_type):
    """Why a value that fits the source's column can fail to fit the
    target's, in words - or None. The deep checks of every SQL engine ask
    here, over `canon.capacity`, which is what counts the rows that would
    not fit (`_capacity_gaps`).

    A character limit and a byte limit (MySQL's TEXT family) are compared
    only where the answer is certain whatever the charset: more characters
    than the other side has bytes, or more bytes than it has characters.
    """
    from . import canon
    s, d = _capacity(engine, src_type), _capacity(engine, dst_type)
    if s is None or d is None:
        return None
    kinds = (s[0], d[0])
    if kinds in (("chars", "bytes"), ("bytes", "chars")):
        if d[1] is not None and (s[1] is None or s[1] > d[1]):
            return f"{src_type} -> {dst_type} (longer values are cut)"
        return None
    if s[0] != d[0]:
        return None
    if s[0] in ("chars", "bytes"):
        if d[1] is not None and (s[1] is None or s[1] > d[1]):
            return f"{src_type} -> {dst_type} (longer values are cut)"
        return None
    if s[0] == "int":
        return (f"{src_type} -> {dst_type} (overflow)"
                if canon.narrower(s, d) else None)
    if d[1] is None:
        return None
    if s[1] is None:
        return (f"{src_type} -> {dst_type} (any precision -> {d[1]}"
                " digits: overflow or rounds)")
    if s[2] > d[2]:
        return (f"{src_type} -> {dst_type} (scale {s[2]} -> {d[2]}:"
                " rounds)")
    if s[1] - s[2] > d[1] - d[2]:
        return (f"{src_type} -> {dst_type} (integer digits"
                f" {s[1] - s[2]} -> {d[1] - d[2]}: overflow)")
    return None


def narrowing_result(db, narrow):
    """The deep check's answer about narrower target columns, from
    `narrowing`'s lines, in one set of words for every engine."""
    from .engines.base import Result
    if narrow:
        return Result("deep", f"{db} narrowing", "diff",
                      f"{len(narrow)} target columns NARROWER than"
                      " source (silent truncation/overflow risk): "
                      + "; ".join(narrow[:6]), "",
                      "widen the target column to match source before"
                      " loading, or values are cut/rounded/overflowed")
    return Result("deep", f"{db} narrowing", "ok",
                  "no target column narrower than source")


def timeshift_result(db, shifts):
    """The deep check's answer about timestamps shifted as a whole, from
    `uniform_shift`'s lines."""
    from .engines.base import Result
    if shifts:
        return Result("deep", f"{db} timeshift", "diff",
                      "uniform timezone offset (systematic, not"
                      " row-level corruption): " + "; ".join(shifts[:5]),
                      "", "the target stored a non-UTC wall clock; re-load"
                      " with the source's session timezone, or convert the"
                      " column by the offset")
    return Result("deep", f"{db} timeshift", "ok",
                  "no uniform timestamp offset detected")


#: the checks whose answer is about the tables' shape or contents, which a
#: DDL on the source in the middle of the run leaves describing a table
#: that is no longer there
TAKEN_ACROSS = ("schema", "counts", "data")


def mark_stale(records, db, tables, said):
    """Take back the answers a DDL on the source overtook.

    `records` are one database's. Every schema answer, since each is
    about the database's shape, and every count and data answer about
    `db` as a whole or about one of `tables`, stops being a verdict: its
    status becomes `skip` and the record carries `stale` with what
    changed, and what it had read. migration-verifier fails the whole run
    for the same reason; count and data answers about tables the DDL did
    not touch keep their verdicts. An `error` stays an error, and a
    `skip` has nothing to take back.
    """
    for r in records:
        if r.get("check") not in TAKEN_ACROSS or \
                r.get("status") not in ("ok", "diff", "warn"):
            continue
        scope = str(r.get("scope", ""))
        if r["check"] != "schema" and scope != db and not any(
                scope.endswith("." + t)
                                   or scope.endswith(" " + t)
                                   for t in tables):
            continue
        r["stale"] = said
        r["detail"] = (f"taken while the source's schema changed ({said});"
                       " this is no longer an answer - check again once"
                       f" it has settled. It read: {r.get('status')}"
                       + (f", {r['detail']}" if r.get("detail") else ""))
        r["status"] = "skip"
    return records


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

    A run whose answers a DDL on the source overtook (`mark_stale`) is
    `incomplete` for the same reason, and names them under `stale`.
    """
    totals = {s: 0 for s in STATUSES}
    stale = sorted({str(r.get("scope", "")) for r in records
                    if r.get("stale")})
    for r in records:
        st = r.get("status", "unknown")
        totals[st] = totals.get(st, 0) + 1
    if totals.get("error"):
        status = "error"
    elif totals.get("diff"):
        status = "different"
    elif totals.get("skip") and not (totals.get("ok") or totals.get("warn")):
        status = "incomplete"
    elif coverage or stale:
        status = "incomplete"
    else:
        status = "same"
    return {
        **({"coverage": coverage} if coverage else {}),
        **({"stale": stale} if stale else {}),
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
            if r.get("status") not in ("ok", "skip") or r.get("stale")
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
