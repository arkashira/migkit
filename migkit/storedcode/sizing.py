"""Columns sized from the data they hold, every change of domain that
makes recorded with its evidence, and proved by what the target now
refuses (leapfrog research P14, the hop option `types_from_data`).

A table migkit builds takes its types from the source's declarations,
widened where a declaration does not carry its numbers: an INT is built
as a 64-bit integer, a text of no declared length as the target's widest
text. Wider cannot truncate. With `types_from_data`, the data decides
where the declaration says nothing - and only there:

* a decimal declared with no precision or scale (Oracle's bare `NUMBER`,
  PostgreSQL's bare `numeric`) whose every value is a whole number within
  64 bits is built as a 64-bit integer;
* a text of no declared length (`text`, MySQL's `TEXT`, `nvarchar(max)`,
  a `CLOB`, a document store's strings) is built with a length: the next
  power of two at or above the longest value, at least 16 characters.

Each is a narrowing of the domain, not of the data: every value the
source holds fits, and a value the source would take later may not. So
each is written down with its evidence (`type-sizing.json` beside the
hop's reports: the rows read, the longest value, what the target now
refuses) and - where the hop names a sandbox - proved by execution on
the fixture (`prove`): the longest value seen goes in on both sides, and
the first value past the new domain goes in on the source and is refused
or changed on the target, as recorded. A declared length, precision or
scale is never narrowed.
"""
import json
import math
from decimal import Decimal

#: the fewest characters a text sized from its data is built with
TEXT_LEAST = 16
#: per target engine, the longest text with a declared length it builds
TEXT_MOST = {"postgres": 10485760, "mysql": 16383, "mssql": 4000,
             "oracle": 4000, "db2": 32704, "duckdb": 10485760,
             "clickhouse": 0, "sqlite": 0}
#: text types that declare no length, by engine (lower case, without
#: their parentheses)
UNBOUNDED = {
    "postgres": ("text", "character varying", "varchar", "citext"),
    "mysql": ("text", "mediumtext", "longtext"),
    "mssql": ("text", "ntext", "varchar(max)", "nvarchar(max)"),
    "oracle": ("clob", "nclob"),
    "sqlite": ("text", "varchar"),
    "mongodb": ("string",),
    "duckdb": ("varchar", "text"),
}
#: decimals that declare neither precision nor scale
BARE_DECIMAL = {"postgres": ("numeric", "decimal"), "oracle": ("number",),
                "duckdb": (), "mongodb": ("decimal",)}
#: the 64-bit integer's range
INT64 = (-(2 ** 63), 2 ** 63 - 1)
#: the file the changes are kept in, beside the hop's reports
RECORD = "type-sizing.json"


def _base(declared):
    text = " ".join(str(declared or "").lower().split())
    if text.endswith("(max)"):
        return text
    return text.split("(")[0].strip()


def unbounded_text(engine, declared):
    base = _base(declared)
    if engine == "postgres" and base in ("character varying", "varchar"):
        return "(" not in str(declared)
    return base in UNBOUNDED.get(engine, ())


def bare_decimal(engine, declared):
    return _base(declared) in BARE_DECIMAL.get(engine, ()) and \
        "(" not in str(declared or "")


def evidence(pair, db, table, columns):
    """{column: {"rows", "values", "most_chars", "most_bytes",
    "integral", "least", "most"}} over every row of the source's table the
    hop carries, read in one pass (`columns`: [(name, class)])."""
    got = {n: {"rows": 0, "values": 0, "most_chars": 0, "most_bytes": 0,
               "integral": True, "least": None, "most": None}
           for n, _ in columns}
    where = pair._row_scope(db, table)[0]
    for rows in pair.src_engine._every_row("src", db, table, columns,
                                           where):
        for row in rows:
            for (name, cls), v in zip(columns, row):
                e = got[name]
                e["rows"] += 1
                if v is None:
                    continue
                e["values"] += 1
                if cls == "text":
                    text = str(v)
                    e["most_chars"] = max(e["most_chars"], len(text))
                    e["most_bytes"] = max(e["most_bytes"],
                                          len(text.encode("utf-8")))
                else:
                    if hasattr(v, "to_decimal"):
                        v = v.to_decimal()
                    d = v if isinstance(v, Decimal) else Decimal(str(v))
                    if not d.is_finite() or d != d.to_integral_value():
                        e["integral"] = False
                        continue
                    n = int(d)
                    e["least"] = n if e["least"] is None else \
                        min(e["least"], n)
                    e["most"] = n if e["most"] is None else max(e["most"], n)
    return got


def _read(pair, db, table, name, cls):
    """One column's evidence, every column of the table the sizing asks
    about read in the same pass and kept for the run."""
    known = pair.__dict__.setdefault("_sizing_evidence", {})
    at = (db, str(table))
    if at not in known:
        wanted = pair.__dict__.setdefault("_sizing_wanted", {}).get(at) or []
        cols = list(dict.fromkeys(wanted + [(name, cls)]))
        known[at] = evidence(pair, db, table, cols)
    if name not in known[at]:
        known[at].update(evidence(pair, db, table, [(name, cls)]))
    return known[at][name]


def wanted(pair, db, table, declared):
    """Note the columns of a table the sizing will ask about, so one pass
    reads them all (`declared`: {source column: declared type})."""
    from .. import canon
    src = pair.src_engine.CANON_ENGINE
    cols = []
    for n, t in declared.items():
        cls = canon.type_class(src, t)
        if cls == "text" and unbounded_text(src, t):
            cols.append((n, "text"))
        elif cls == "decimal" and bare_decimal(src, t):
            cols.append((n, "decimal"))
    pair.__dict__.setdefault("_sizing_wanted", {})[(db, str(table))] = cols


def sized(pair, db, table, column, built_as, declared, cls, numbers):
    """(class, numbers) to build one column with: the data's where the
    declaration leaves the domain open and the hop turns
    `types_from_data` on, recorded (`RECORD`); else as given."""
    if not (pair.hop.options or {}).get("types_from_data"):
        return cls, numbers
    src = pair.src_engine.CANON_ENGINE
    dst = pair.dst_engine.CANON_ENGINE
    if cls == "text" and not numbers and unbounded_text(src, declared):
        e = _read(pair, db, table, column, "text")
        most = TEXT_MOST.get(dst, 0)
        if not e["values"] or not most:
            return cls, numbers
        n = max(TEXT_LEAST, 2 ** math.ceil(math.log2(max(1,
                                                         e["most_chars"]))))
        if n > most or n < e["most_chars"]:
            return cls, numbers
        _record(pair, db, table, column, built_as, declared, {
            "class": "text", "built": f"text of at most {n} characters",
            "length": n, "evidence": {
                "rows": e["rows"], "values": e["values"],
                "most_chars": e["most_chars"],
                "most_bytes": e["most_bytes"]},
            "refuses": f"a value longer than {n} characters, which the"
                       " source takes"})
        return "text", (n,)
    if cls == "decimal" and not numbers and bare_decimal(src, declared):
        e = _read(pair, db, table, column, "decimal")
        if not e["values"] or not e["integral"] or \
                e["least"] < INT64[0] or e["most"] > INT64[1]:
            return cls, numbers
        _record(pair, db, table, column, built_as, declared, {
            "class": "integer", "built": "a 64-bit whole number",
            "evidence": {"rows": e["rows"], "values": e["values"],
                         "least": e["least"], "most": e["most"]},
            "refuses": "a fraction, which the source keeps and the target"
                       " rounds or refuses"})
        return "integer", ()
    return cls, numbers


def _record(pair, db, table, column, built_as, declared, change):
    path = pair.hop.report_dir(db) / RECORD
    try:
        kept = json.loads(path.read_text())
    except (OSError, ValueError):
        kept = {}
    kept[f"{table}.{column}"] = {"table": str(table), "column": column,
                                 "target column": built_as,
                                 "declared": str(declared), **change}
    try:
        path.write_text(json.dumps(kept, indent=1, sort_keys=True,
                                   default=str))
    except OSError:
        pass


def recorded(hop, db):
    try:
        return json.loads((hop.report_dir(db) / RECORD).read_text())
    except (OSError, ValueError):
        return {}


def _longest(n):
    """A value of `n` characters, every one of them one byte and all
    alike, so only its length is on trial."""
    return "m" * n


def prove(bench):
    """[Result]: every change of domain recorded for this database, proved
    on the sandbox's fixture - the longest value seen goes in on both
    sides, and the first value past the new domain goes in on the source
    and not, as it was, on the target. `warn` naming each change proved
    (the owner's to accept: a later write the source takes may not be
    taken); `diff` where the target refuses a value the source holds; an
    unproved change is said, never passed."""
    from ..engines.base import Result
    from . import constraints
    from .harness import differences
    kept = recorded(bench.hop, bench.db)
    if not kept:
        return []
    proved, apart, unproved = [], [], []
    for key, c in sorted(kept.items()):
        table = next((t for t in bench.tables
                      if str(t) == c["table"] or str(t).split(".")[-1]
                      == str(c["table"]).split(".")[-1]), None)
        if table is None:
            unproved.append(f"{key} (its table is not in the fixture)")
            continue
        column = c["column"]
        if c["class"] == "text":
            fits, past = (_longest(c["evidence"]["most_chars"]),
                          _longest(int(c["length"]) + 1))
        else:
            fits, past = c["evidence"]["most"], Decimal("0.5")
        try:
            x, y, _ = constraints.probe_row(bench, table, {column: fits})
            px, py, _ = constraints.probe_row(bench, table, {column: past})
        except Exception as e:  # noqa: BLE001 - said, never passed
            first = (str(e).splitlines() or [""])[0][:80]
            unproved.append(f"{key} ({first})")
            continue
        if x.error is None and differences(x, y):
            apart.append(f"{key}: a value the source holds ({c['class']},"
                         f" {len(fits) if isinstance(fits, str) else fits})"
                         " is not taken as it is by the target: "
                         f"{differences(x, y)[0]}")
        elif x.error is not None:
            unproved.append(f"{key} (the source refuses the probe row:"
                            f" {x.error[0]})")
        elif px.error is None and differences(px, py):
            proved.append(f"{key} built as {c['built']} (the source's"
                          f" {c['declared']}; {c['evidence']['values']:,}"
                          " values read): the target now refuses or changes"
                          f" {c['refuses'].split(',')[0]}")
        else:
            unproved.append(f"{key} (the target takes what the record says"
                            " it refuses: its column is not the one"
                            " recorded)")
    scope = f"{bench.db} sized types"
    if apart:
        return [Result("schema", scope, "diff", "; ".join(apart[:4]), "",
                       "build the column as the source declares it (the hop"
                       " option types_from_data off, or the column created"
                       " by hand) and move again")]
    status = "warn" if proved or unproved else "ok"
    detail = "; ".join(proved[:6] + [f"not proved: {u}"
                                     for u in unproved[:4]])
    return [Result("schema", scope, status, detail, "",
                   "each narrowing is recorded with its evidence in"
                   f" {RECORD} beside the hop's reports: accept it, or build"
                   " that column as the source declares it")]
