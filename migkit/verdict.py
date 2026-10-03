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

What a run ends as is decided here and nowhere else (`decide`), and said in
the words of one table (`VERDICTS`): the check's exit code, the report's
banner, `watch`, `sync`, the scheduler, CI and the cutover read it. Before
it, four places decided "same" and disagreed - measured, a check whose
verdict was `different` printed "all green" and exited 0, because the
report's renderer had rewritten a `diff` as `ok` in the rows the exit code
was counted from; an empty answer set read `same`.
"""
import hashlib
import json
import time
from dataclasses import dataclass

from . import __version__
from .engines.base import STATUSES

FORMAT_VERSION = 1


@dataclass(frozen=True)
class Verdict:
    word: str       # what every output says
    exit: int       # what the command ends with
    colour: str     # what the console and the report show it in
    metric: int     # what `/metrics` carries


#: every verdict a run ends as. `error` is a run that could not take an
#: answer it set out to take; `incomplete` one that found no difference
#: and did not look at everything - narrowed, overtaken by a DDL, answers
#: reused or to be read, or nothing compared at all
VERDICTS = {
    "same": Verdict("same", 0, "green", 0),
    "different": Verdict("different", 1, "yellow", 1),
    "error": Verdict("could not verify", 3, "red", 2),
    "incomplete": Verdict("incomplete", 3, "yellow", 4),
}

#: the exit codes of what is not a verdict: a command that did what it was
#: asked (`done`), one given flags that do not go together (`usage`, which
#: click's own errors end with too), one that stopped short (`failed`, the
#: code of a verdict that could not be taken), one a guard stopped before
#: it wrote (`refused`), and one stopped by Ctrl-C or SIGTERM
EXIT = {"done": 0, "usage": 2, "failed": 3, "refused": 4,
        "interrupted": 130}

#: each answer on its own, as the verdict counts it
ROWS = {"ok": "same", "skip": "not compared", "warn": "to read",
        "diff": "different", "error": "could not verify"}

#: when each verdict and each end is said, in the operator's words
MEANS = {
    "same": "every answer the run set out to take was taken, and the two"
            " sides hold the same",
    "done": "a command that is not a comparison did what it was asked; a"
            " plan without --go or --apply was shown",
    "different": "a difference was found - the check did its work; the"
                 " report names which table, row and column",
    "usage": "flags that do not go together, or a name that is not there"
             " (a check --only does not know, a variable's value)",
    "error": "an answer could not be taken: a server that did not answer,"
             " a check that failed",
    "incomplete": "nothing differs, and not everything was looked at: a"
                  " narrowed run, answers to read, answers reused from"
                  " an earlier run, a schema change mid-run, nothing"
                  " compared",
    "failed": "the command stopped part way, said why in words; the"
              " details are in the hop's commands.log",
    "refused": "a guard stopped the command before anything was written:"
               " the goal, a hop that masks, an approval, the target not"
               " the one the plan showed, a check known to fail",
    "interrupted": "stopped by Ctrl-C or SIGTERM; what was saved as it"
                   " went is kept",
}


def exit_codes():
    """[(code, [what ends with it])] - every exit code a command ends with,
    lowest first, each with the verdicts and ends it says in words
    (`MEANS`): the table the manual shows, read off the ones the code
    ends by."""
    got = {}
    for status, v in VERDICTS.items():
        got.setdefault(v.exit, []).append((v.word, MEANS[status]))
    for end, code in EXIT.items():
        got.setdefault(code, []).append((end, MEANS[end]))
    return sorted(got.items())


class Refused(SystemExit):
    """A guard stopped a command before it wrote anything: said in its
    own words, the command ending with `EXIT["refused"]`."""


class Usage(SystemExit):
    """What the command was given does not go together, or names what is
    not there: said, the command ending with `EXIT["usage"]`."""


def ran(code):
    """Whether a command that ended with `code` did its work - a check
    that found a difference did; one that could not compare, a guard that
    stopped it, a stop part way did not. What a schedule counts as a slot
    that failed."""
    return code in (EXIT["done"], VERDICTS["different"].exit)


def exit_code(status):
    return VERDICTS[status].exit


def db_of(scope):
    """The database an answer is about, from its scope (`shop`, `shop
    narrowing`, `shop.public.orders`) - the one reading the report, the
    repair and `--only --db` share, so `--db shop` is never taken for
    `shop_archive`."""
    first = (str(scope or "").split() or [""])[0]
    return first.split(".")[0].replace("(migra)", "").strip()


@dataclass(frozen=True)
class Decision:
    status: str
    why: str
    #: each record's weight in the verdict (`ROWS`, or "note" for a warn
    #: the hop says does not stop the check), in the records' order
    severity: tuple

    @property
    def exit(self):
        return VERDICTS[self.status].exit

    @property
    def word(self):
        return VERDICTS[self.status].word


class _Gated:
    """A hop that gates on performance, for asking `workload.gate` which
    answers are about performance at all."""
    options = {"performance": "gate"}


def _notes(hop, records):
    """The ids of the warns the hop says do not stop the check: a slower
    target holds the same data, and stops it only under `performance:
    gate` (`workload.gate`)."""
    from . import workload
    slow = workload.gate(_Gated, records)
    gated = workload.gate(hop, records) if hasattr(hop, "options") else []
    kept = {id(r) for r in gated}
    return {id(r) for r in slow if id(r) not in kept}


def _narrowed(coverage):
    return "; ".join(
        f"{k} " + (", ".join(map(str, v)) if isinstance(v, (list, tuple))
                   else str(v))
        for k, v in sorted(coverage.items()))


def _named(rows, n=4):
    scopes = sorted({str(r.get("scope", "")) for r in rows})
    return ", ".join(scopes[:n]) + (" ..." if len(scopes) > n else "")


def decide(records, hop=None, coverage=None):
    """What a run's answers add up to (`Decision`): one rule for every
    reader. Where a difference or an error is found it is said, whatever
    else ran. Nothing reads `same` that did not compare everything it set
    out to, in words that say why not: no answer at all, answers a DDL
    overtook, a `warn` (something to read - rows the role cannot see,
    columns no comparison covers), answers reused from an earlier run the
    source may have moved under since, only skips, a narrowed run. A skip
    among compared answers is said, not hidden: `same` names what was not
    compared. A status the table has no word for counts as an error -
    never as a pass."""
    records = list(records)
    notes = _notes(hop, records)
    sev = []
    bad, diff, stale, warn, reused, skip, ok = [], [], [], [], [], [], []
    for r in records:
        st = r.get("status")
        if st not in STATUSES:
            bad.append(r)
            sev.append(ROWS["error"])
            continue
        if st == "warn" and id(r) in notes:
            ok.append(r)
            sev.append("note")
            continue
        sev.append(ROWS[st])
        if st == "error":
            bad.append(r)
        elif st == "diff":
            diff.append(r)
        elif r.get("stale"):
            stale.append(r)
        elif st == "warn":
            warn.append(r)
        elif st == "skip":
            skip.append(r)
        else:
            ok.append(r)
            if r.get("reused_from") and not r.get("reuse_held"):
                reused.append(r)
    if bad:
        odd = [r for r in bad if r.get("status") not in STATUSES]
        why = (f"{len(bad)} answer{'s' if len(bad) > 1 else ''} could not"
               f" be taken: {_named(bad)}")
        if odd:
            why += ("; an answer migkit has no word for ("
                    + ", ".join(sorted({str(r.get('status')) for r in odd}))
                    + ") is taken as not verified")
        return Decision("error", why, tuple(sev))
    if diff:
        return Decision("different", f"{len(diff)} differ: {_named(diff)}",
                        tuple(sev))
    if not records:
        return Decision("incomplete", "nothing was compared: no answer at"
                        " all", tuple(sev))
    if stale:
        return Decision("incomplete", f"{len(stale)} answers were taken"
                        " while the source's schema changed:"
                        f" {_named(stale)}", tuple(sev))
    if warn:
        return Decision("incomplete", f"{len(warn)} answer"
                        f"{'s' if len(warn) > 1 else ''} to read before the"
                        f" two are called the same: {_named(warn)}",
                        tuple(sev))
    if reused:
        return Decision("incomplete", f"{len(reused)} answers were reused"
                        " from an earlier run and the source may have"
                        f" changed since: {_named(reused)}", tuple(sev))
    if not ok:
        return Decision("incomplete", "every answer was skipped - nothing"
                        f" was compared: {_named(skip)}", tuple(sev))
    if coverage:
        return Decision("incomplete", "the run was narrowed to "
                        + _narrowed(coverage), tuple(sev))
    return Decision("same", (f"{len(ok)} compared the same; not compared:"
                             f" {_named(skip)}" if skip else ""), tuple(sev))


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


#: names a group of columns lists before it says how many more
NAMED = 12


def compared_clause(how):
    """The clause every cross-engine verdict ends with: which columns were
    compared exactly, which by value and as what, and which not at all and
    why (problems D15) - from `Engine._comparable_columns(how=...)`, every
    column of the pair in one of the three.

    A group names its first `NAMED` columns and counts the rest; the
    counts are always whole, so a column is never in no group."""
    if not how:
        return ""
    groups = {"exactly": [], "by value": [], "not at all": []}
    for name in sorted(how):
        kind, why = how[name]
        groups.setdefault(kind, []).append((name, why))

    def listed(items, with_why):
        shown = [f"{n} ({w})" if with_why else n for n, w in items[:NAMED]]
        more = len(items) - NAMED
        return ", ".join(shown) + (f" and {more:,} more" if more > 0 else "")
    parts = []
    exact = groups["exactly"]
    if exact:
        parts.append(f"{len(exact)} exactly (one engine, one type):"
                     f" {listed(exact, False)}")
    value = groups["by value"]
    if value:
        parts.append(f"{len(value)} by value: {listed(value, True)}")
    none = groups["not at all"]
    if none:
        parts.append(f"{len(none)} not at all: {listed(none, True)}")
    return f"; of {len(how)} columns, compared " + "; ".join(parts)


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


def fingerprint(records, scope=None):
    """Stable hash of the findings, ignoring wording and ordering.

    Only (category, scope, status) goes in: `detail` carries row counts and
    timestamps that move between runs without the verdict changing, so
    including it would make every fingerprint unique and useless.

    `scope` is the hop's scope the answers were taken under
    (`Hop.scope_fingerprint`), where the hop narrows what it takes: the
    same findings about other tables, rows or columns are not the same
    verdict. A hop that takes everything whole passes none, and its
    fingerprint is what it always was.
    """
    rows = sorted((r.get("category", ""), str(r.get("scope", "")),
                   r.get("status", "")) for r in records)
    canonical = json.dumps([rows, scope] if scope else rows,
                           separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hop_scope(hop):
    """What the hop itself takes, where it takes less than everything: the
    `include` and `exclude` it was checked within, and the tables whose
    rows or columns it narrows. Unlike `coverage`, not a narrowed run -
    the hop's own definition, a `same` within it is a `same` - and said
    beside the verdict so whoever reads `same` reads what it covers.
    Empty for a hop that takes everything whole."""
    mapping = getattr(hop, "mapping", None) or {}
    out = {k: [str(x) for x in getattr(hop, k, None) or []]
           for k in ("include", "exclude")}
    out["row_filters"] = sorted((mapping.get("where") or {}))
    out["column_rules"] = sorted((mapping.get("columns") or {}))
    out["renamed"] = [f"{k} -> {v}" for k, v in
                      sorted((mapping.get("tables") or {}).items())]
    from .config import objects_of
    try:
        said = objects_of(hop)
    except SystemExit:
        said = {}
    out["objects_not_carried"] = sorted(k for k, v in said.items()
                                        if v is False)
    out["objects_named"] = [f"{k}: {', '.join(v)}" for k, v in
                            sorted(said.items()) if isinstance(v, list)]
    out["changes_carried"] = [
        f"{k}: {', '.join(v) if isinstance(v, (list, tuple)) else v}"
        for k, v in sorted((mapping.get("ops") or {}).items())]
    return {k: v for k, v in out.items() if v}


#: what each part of `hop_scope` is called where it is said
SCOPE_WORDS = {"include": "takes only", "exclude": "leaves out",
               "row_filters": "rows filtered in",
               "column_rules": "columns chosen in",
               "renamed": "renamed", "changes_carried": "changes carried",
               "objects_not_carried": "objects not carried",
               "objects_named": "only these objects"}


def scope_said(scope):
    """[(what, names)] of a `hop_scope`, in the words a report says it."""
    return [(SCOPE_WORDS.get(k, k), [str(x) for x in v])
            for k, v in (scope or {}).items() if v]


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
    `incomplete` for the same reason, and names them under `stale`. The
    status is `decide`'s, and `why` says why it is not `same`.
    """
    totals = {s: 0 for s in STATUSES}
    stale = sorted({str(r.get("scope", "")) for r in records
                    if r.get("stale")})
    for r in records:
        st = r.get("status", "unknown")
        totals[st] = totals.get(st, 0) + 1
    got = decide(records, hop, coverage)
    from . import digests, session
    # digests the check took and could not keep: the answers stand, and
    # the evidence lists as many tables fewer - said here, not swallowed
    unwritten = digests.unwritten(session.current())
    scope = hop_scope(hop)
    from . import servicecheck
    theirs = servicecheck.of(hop, records)
    return {
        **({"coverage": coverage} if coverage else {}),
        **({"scope": scope} if scope else {}),
        **({"service_check": theirs} if theirs else {}),
        **({"scope_fingerprint": hop.scope_fingerprint()}
           if hasattr(hop, "scope_fingerprint") else {}),
        **({"stale": stale} if stale else {}),
        "format_version": FORMAT_VERSION,
        "tool": "migkit",
        "tool_version": __version__,
        "hop": hop.name,
        # the run that took it, for its evidence (`export`)
        "session": session.current(),
        "engine": engine or hop.engine,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "status": got.status,
        **({"why": got.why} if got.why else {}),
        "has_differences": bool(totals.get("diff") or totals.get("error")
                                or got.status == "error"),
        "totals": totals,
        "by_category": _counts(records, "category"),
        "fingerprint": fingerprint(records, _narrowing(hop, scope)),
        **({"load": load} if load else {}),
        **({"digests_unwritten": unwritten} if unwritten else {}),
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


def _narrowing(hop, scope=None):
    """The hop's scope fingerprint where it narrows what it takes
    (`hop_scope`), else None - what `fingerprint` mixes in."""
    scope = hop_scope(hop) if scope is None else scope
    if not scope or not hasattr(hop, "scope_fingerprint"):
        return None
    return hop.scope_fingerprint()


def unchanged_since(hop, records):
    """True when this run's findings match the last one exactly, about
    the same scope.

    Lets a repeated check say "nothing moved" without the reader having to
    diff two reports by eye. The same findings taken under another scope
    (`Hop.scope_changed`) are not "identical to the previous run": they
    are about other tables, rows or columns.
    """
    p = hop.report_dir() / "verdict.json"
    if not p.exists():
        return False
    try:
        prev = json.loads(p.read_text())
    except ValueError:
        return False
    if hasattr(hop, "scope_changed") and hop.scope_changed(prev):
        return False
    return prev.get("fingerprint") == fingerprint(records, _narrowing(hop))
