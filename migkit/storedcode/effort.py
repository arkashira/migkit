"""What converting the stored code will cost a person, from two readings
set side by side and never merged (backlog 40, research item 15).

* The PL/SQL converter's own estimate: it scores each routine from a
  table of weights per construct, in units of work it values at five
  minutes of an expert's time. That table is the converter's, calibrated
  on its authors' migrations; it is shown as the converter's figure, with
  its unit, and nothing of migkit's is derived from it.
* migkit's own measure, from the proof: per routine, whether it was
  proved, proved with a caveat, or not converted - and what that leaves a
  person, counted, not guessed: the statements of a routine no candidate
  proved (only the part that still differs, where the reducer cut it),
  the lines no call reached in one proved with a caveat, and the seconds
  migkit itself spent converting and proving it, measured.

migkit puts no rate on a person's time (`handwork` refuses the same
guess): a count of statements left and lines to check is what an
operator can apply their own team's rate to.
"""
import json
import re

#: minutes a unit of the converter's estimate stands for, by its own
#: table (its `COST_UNIT_VALUE`)
UNIT_MINUTES = 5


def units(output):
    """(units, {construct: units}) the converter's estimate gives one
    routine, read from what it wrote with its cost estimate on; None
    where it gave none."""
    total, detail = None, {}
    for line in str(output or "").splitlines():
        m = re.search(r"(?i)estimated\s+cost[^:\n]*:\s*([0-9]+(?:\.[0-9]+)?)",
                      line)
        if m and not re.search(r"(?i)\btotal\b", line):
            total = (total or 0) + float(m.group(1))
            continue
        m = re.match(r"^\s*--\s*([A-Z_][A-Z0-9_ ()%.-]*?)\s*=>\s*"
                     r"([0-9]+(?:\.[0-9]+)?)\s*$", line)
        if m:
            detail[m.group(1).strip()] = detail.get(
                m.group(1).strip(), 0) + float(m.group(2))
    if total is None:
        return None
    return total, detail


def statements(text, dialect):
    """How many statements a body holds, as its own dialect reads them:
    the statement ends it has, one where it has none."""
    from sqlglot import tokens
    from sqlglot.dialects.dialect import Dialect
    try:
        toks = Dialect.get_or_raise(dialect or "mysql").tokenize(text or "")
    except Exception:  # noqa: BLE001 - counted by its lines instead
        return max(1, len([ln for ln in str(text or "").splitlines()
                           if ln.strip()]))
    return max(1, sum(1 for t in toks
                      if t.token_type == tokens.TokenType.SEMICOLON))


#: the part of a reason that names the smallest differing part
_PART = re.compile(r"the smallest part of the source that still differs:"
                   r" (.*)$")


def left(record, routine, dialect):
    """(statements left for a person, lines to check) for one routine's
    conversion record."""
    cls = record.get("class")
    if cls == "proved":
        return 0, 0
    if cls == "proved with caveats":
        reason = str((record.get("evidence") or {}).get("reason") or "")
        lines = re.findall(r"line (\d+)", reason)
        return 0, max(1, len(set(lines)))
    parts = [m.group(1) for x in record.get("reasons") or []
             for m in [_PART.search(str(x))] if m]
    if parts:
        return min(statements(p, dialect) for p in parts), 0
    body = routine.body or routine.settings.get("expression") or \
        routine.definition or ""
    return statements(body, dialect), 0


def summary(records, routines, costs, seconds, dialect):
    """The effort section of the conversion's record: the converter's
    figure (where it scored any routine) and migkit's measure."""
    by = {r.name: r for r in routines}
    measured = {"routines": 0, "proved": 0, "proved with caveats": 0,
                "converted, not proved": 0, "not converted": 0,
                "statements left": 0, "lines to check": 0,
                "seconds": round(sum(seconds.values()), 3)}
    per = {}
    for rec in records:
        name = rec["routine"].split(" ", 1)[1]
        r = by.get(name)
        if r is None:
            continue
        measured["routines"] += 1
        cls = rec.get("class")
        if cls in measured:
            measured[cls] += 1
        stmts, lines = left(rec, r, dialect)
        measured["statements left"] += stmts
        measured["lines to check"] += lines
        one = {"class": cls, "statements left": stmts,
               "lines to check": lines}
        if name in seconds:
            one["seconds"] = round(seconds[name], 3)
        if costs.get(name):
            one["converter units"] = costs[name][0]
        per[name] = one
    out = {"measured": measured, "per routine": per}
    scored = {n: c for n, c in costs.items() if c}
    if scored:
        total = sum(c[0] for c in scored.values())
        out["converter"] = {
            "units": total, "minutes": total * UNIT_MINUTES,
            "routines scored": len(scored),
            "unit": f"{UNIT_MINUTES} minutes of an expert's work, by the"
                    " converter's own table",
            "by construct": _merged(c[1] for c in scored.values())}
    return out


def _merged(details):
    out = {}
    for d in details:
        for k, v in (d or {}).items():
            out[k] = out.get(k, 0) + v
    return dict(sorted(out.items(), key=lambda x: -x[1]))


def said(hop, db):
    """One line of what converting the database's code leaves a person,
    from its conversion record; None where there is none."""
    try:
        got = json.loads((hop.report_dir(db) / "code-conversion.json")
                         .read_text()).get("effort")
    except (OSError, ValueError, AttributeError):
        return None
    if not got:
        return None
    m = got["measured"]
    if not m.get("routines"):
        return None
    line = (f"{m['proved']} of {m['routines']} routines proved"
            f" ({m['proved with caveats']} with a caveat); left for a"
            f" person: {m['statements left']} statements to write"
            f" and {m['lines to check']} lines no call reached, to check;"
            f" migkit's own time {m['seconds']:.0f}s")
    conv = got.get("converter")
    if conv:
        line += (f"; the PL/SQL converter's own estimate:"
                 f" {conv['units']:g} units ({conv['minutes'] / 60:.1f}"
                 f" hours at {UNIT_MINUTES} minutes a unit) over"
                 f" {conv['routines scored']} routines")
    return line
