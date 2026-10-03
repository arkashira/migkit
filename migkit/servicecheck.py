"""What another tool's own check said, held table by table against what
migkit found (backlog P0-SCOPE.5).

A migration service checks its own work - Tencent DTS's data check and
its report, AWS DMS's validation, Alibaba DTS's data check - and the
operator is told "consistent". That claim is about what the service
compared, the way it compared it (a sample, row counts), when it did.
Given the service's report as the hop's `compare_with` (or
`MIGKIT_COMPARE_WITH` for one run), a check says, beside its own
verdict:

    missed       the service said the same; migkit found them different
    agreed       both found them different, or both the same
    not found    the service said different; migkit found them the same
                 (fixed since, or a difference of the moment)
    skipped      the service passed the table over, and why
    unchecked    migkit compared it; the service's check never named it
    beyond       what migkit found that no table check covers: schemas,
                 sequences, users and grants, objects

migkit's verdict is its own and stays so: what the service said is
evidence beside it, never an input to it. Every name is read as the service wrote it -
the source's database, schema and table - and matched to migkit's
answers by database and table, the schema too where both name one.
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

SAME, DIFFERENT, SKIPPED = "same", "different", "skipped"

#: the words the services use, as one of the three
_SAID = {"same": SAME, "consistent": SAME, "validated": SAME, "pass": SAME,
         "passed": SAME, "ok": SAME, "success": SAME, "identical": SAME,
         "different": DIFFERENT, "inconsistent": DIFFERENT,
         "mismatched records": DIFFERENT, "table error": DIFFERENT,
         "failed": DIFFERENT, "fail": DIFFERENT, "diff": DIFFERENT,
         "error": DIFFERENT, "skip": SKIPPED, "skipped": SKIPPED,
         "skipall": SKIPPED, "not enabled": SKIPPED,
         "pending records": SKIPPED, "pending revalidation": SKIPPED,
         "no primary key": SKIPPED, "suspended records": SKIPPED,
         "pending": SKIPPED, "notstarted": SKIPPED}
#: keys a service names a table's database, schema and table by
_DB = ("Db", "DbName", "DBName", "Database", "DatabaseName", "SourceDbName",
       "SourceDb")
_SCHEMA = ("Schema", "SchemaName", "SourceSchemaName")
_TABLE = ("Table", "TableName", "SourceTbName", "SourceTableName",
          "Collection")


@dataclass
class Claims:
    service: str = ""
    #: the service's own conclusion over everything it checked
    overall: str = ""
    #: how it compared: full, a sample (and of how much), counts
    method: str = ""
    #: {(db, schema, table): (said, why)}
    tables: dict = field(default_factory=dict)
    #: what the file was, for the evidence
    source: str = ""


def path_of(hop):
    """The report a run holds its answers against: `MIGKIT_COMPARE_WITH`,
    else the hop's `compare_with` option, else none."""
    got = os.environ.get("MIGKIT_COMPARE_WITH") or \
        (getattr(hop, "options", None) or {}).get("compare_with")
    return str(got) if got else ""


def read(path):
    """The claims a service's report makes, whichever service wrote it."""
    p = Path(str(path)).expanduser()
    data = json.loads(p.read_text())
    if isinstance(data, dict) and isinstance(data.get("Response"), dict):
        data = data["Response"]
    got = Claims(source=str(p))
    got.service = _service(data)
    got.overall, got.method = _overall(data)
    _walk(data, got, ())
    return got


def _service(data):
    text = json.dumps(data)[:20000]
    if "TableStatistics" in text or "ReplicationTaskArn" in text:
        return "AWS DMS"
    if "SourceTbName" in text or "DtsJobId" in text:
        return "Alibaba Cloud DTS"
    if "CompareTaskId" in text or "Abstract" in text or "DiffType" in text \
            or "ChunkId" in text or "Conclusion" in text:
        return "Tencent Cloud DTS"
    return "the service"


def _overall(data):
    if not isinstance(data, dict):
        return "", ""
    head = data.get("Abstract") if isinstance(data.get("Abstract"),
                                              dict) else data
    said = str(head.get("Conclusion") or head.get("Status") or "")
    opts = head.get("Options") if isinstance(head.get("Options"),
                                             dict) else {}
    method = str(head.get("Method") or opts.get("Method") or "")
    rate = opts.get("SampleRate")
    if method == "sampleDataCheck" and rate is not None:
        method = f"a sample of {rate}% of the rows"
    elif method == "rowsCount":
        method = "row counts only"
    elif method == "dataCheck":
        method = "every row"
    return _SAID.get(said.lower(), said), method


def _pick(d, keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v, (str, int)) and str(v) != "":
            return str(v)
    return ""


def _walk(node, got, path):
    """Every table the report names, under whichever key it sits: a
    difference wherever it is listed among differences, a skip among
    skips, a table's own conclusion where it has one."""
    if isinstance(node, list):
        for x in node:
            _walk(x, got, path)
        return
    if not isinstance(node, dict):
        return
    db, table = _pick(node, _DB), _pick(node, _TABLE)
    schema = _pick(node, _SCHEMA)
    if table and (db or schema) and table != db:
        # a report that names a schema and no database (AWS DMS's table
        # statistics) is of the endpoint's database, whichever it is
        said, why = _judge(node, path)
        if said:
            key = (db, schema, table)
            was = got.tables.get(key)
            # a table named twice keeps its worst word
            order = (SAME, SKIPPED, DIFFERENT)
            if was is None or order.index(said) > order.index(was[0]):
                got.tables[key] = (said, why)
    for k, v in node.items():
        if isinstance(v, (dict, list)):
            _walk(v, got, path + (str(k),))


def _judge(node, path):
    """(said, why) for one table's entry, or ("", "") for an entry that
    says nothing about the table's data."""
    where = " ".join(path).lower()
    if "skip" in where:
        return SKIPPED, str(node.get("Reason") or node.get("Message")
                            or "passed over")
    state = node.get("ValidationState")
    if state is not None:
        failed = int(node.get("ValidationFailedRecords") or 0)
        said = _SAID.get(str(state).lower(), "")
        if failed:
            return DIFFERENT, f"{failed} records failed its validation"
        return said, str(state)
    if "DiffCount" in node:
        n = int(node.get("DiffCount") or 0)
        return (DIFFERENT, f"{n} rows differ") if n else (SAME, "")
    if "Conclusion" in node:
        said = _SAID.get(str(node["Conclusion"]).lower(), "")
        rows = node.get("RowCount")
        return said, (f"{rows} rows differ" if said == DIFFERENT and rows
                      else str(node["Conclusion"]))
    if "diff" in where or "difference" in where or node.get("DiffType"):
        kind = str(node.get("DiffType") or "")
        why = {"srcLack": "rows only on the target",
               "dstLack": "rows missing on the target",
               "data": "rows differ"}.get(kind, "")
        if "SrcCount" in node:
            why = f"rows {node.get('SrcCount')} on the source," \
                  f" {node.get('DstCount')} on the target"
        elif "SrcSchema" in node:
            why = "the table's structure differs"
        return DIFFERENT, why or "differs"
    return "", ""


def _parts(scope):
    return [p for p in str(scope).split(".") if p]


def _matches(key, parts, fold=False):
    """Whether a table the service named is one migkit answered for:
    database and table alike, the schema too where both name one. A
    service that named no database (its endpoint's) names the schema,
    which is the database on an engine without schemas. `fold`: names
    compared without case."""
    if fold:
        key = tuple(str(x).casefold() for x in key)
        parts = [str(p).casefold() for p in parts]
    db, schema, table = key
    if not parts or parts[-1] != table:
        return False
    if not db:
        return len(parts) >= 2 and parts[-2] == schema
    if parts[0] != db:
        return False
    return not (schema and len(parts) == 3 and parts[1] != schema)


#: migkit's answers, least to most telling: a table's worst one is its own
_WORST = ("ok", "skip", "warn", "error", "diff")


def _answers(records):
    """{dotted table name: (status, detail)} of a check's answers, each
    table once at its worst: a table's own answer, and each table a
    database's one answer says how it came out (`Result.tables`)."""
    answers = {}

    def put(name, st, detail):
        was = answers.get(name)
        if st in _WORST and (was is None or _WORST.index(st) >
                             _WORST.index(was[0])):
            answers[name] = (st, str(detail)[:120])
    for r in records:
        scope = str(r.get("scope", ""))
        db = scope.split(" ")[0].split(".")[0]
        for table, st in (r.get("tables") or {}).items():
            put(f"{db}.{table}", st, f"{r.get('check')}: {st}")
        table = str(r.get("table") or "")
        if table and db:
            put(f"{db}.{table}", r.get("status"), r.get("detail", ""))
            continue
        if " " in scope:
            # `shop public.orders`: a table named after its database
            head, _, rest = scope.partition(" ")
            if "." not in head and rest and " " not in rest.strip():
                put(f"{head}.{rest.strip()}", r.get("status"),
                    r.get("detail", ""))
            continue
        if len(_parts(scope)) >= 2:
            put(scope, r.get("status"), r.get("detail", ""))
    return answers


def reconcile(claims, records, fold=False):
    """The claims held against migkit's answers (`Claims` against the
    records a check returns): what falls in each of `missed`, `agreed`,
    `not found`, `unproved`, `skipped`, `unchecked`, `beyond` and `not in
    the hop`, each a sorted list of names with what was said. `unproved`:
    the service gave its word and migkit's own answer for the table was
    neither the same nor different - a warning, a skip, an error - so
    nothing is agreed. Names are matched as written, and without case
    where the engine's names have none (`fold`) or where only one answer
    is the name in another case."""
    answers = _answers(records)
    out = {k: [] for k in ("missed", "agreed", "not found", "unproved",
                           "skipped", "unchecked", "beyond",
                           "not in the hop")}
    seen = set()
    for key, (said, why) in sorted(claims.tables.items()):
        name = ".".join(x for x in key if x)
        hit = [s for s in answers if _matches(key, _parts(s), fold)]
        if not hit and not fold:
            folded = [s for s in answers if _matches(key, _parts(s), True)]
            hit = folded if len(folded) == 1 else []
        if not hit:
            out["not in the hop"].append(f"{name} ({said})")
            continue
        seen.update(hit)
        ours = max((answers[s] for s in hit),
                   key=lambda a: _WORST.index(a[0]))
        if said == SKIPPED:
            out["skipped"].append(f"{name}: {why}; migkit: {ours[0]}")
        elif ours[0] not in ("ok", "diff"):
            out["unproved"].append(
                f"{name}: the service said {said}; migkit {ours[0]}:"
                f" {ours[1]}")
        elif said == SAME and ours[0] == "diff":
            out["missed"].append(f"{name}: {ours[1]}")
        elif said == DIFFERENT and ours[0] == "ok":
            out["not found"].append(f"{name}: the service said {why}")
        else:
            out["agreed"].append(f"{name}: {said}")
    for s, (st, detail) in sorted(answers.items()):
        if s not in seen:
            out["unchecked"].append(f"{s}: migkit {st}")
    for r in records:
        scope = str(r.get("scope", ""))
        if r.get("status") == "diff" and not r.get("tables") and \
                not r.get("table") and (" " in scope
                                        or len(_parts(scope)) < 2):
            out["beyond"].append(f"{scope} ({r.get('check')}):"
                                 f" {str(r.get('detail', ''))[:100]}")
    return {k: v for k, v in out.items() if v}


def of(hop, records):
    """The section a check's evidence carries about the service's own
    check, or None where the hop names no report. A file that cannot be
    read is said, not skipped."""
    path = path_of(hop)
    if not path:
        return None
    try:
        got = read(path)
    except (OSError, ValueError) as e:
        return {"file": path, "error": f"could not be read: {e}"}
    fold = bool(getattr(hop, "folds_case", None) and hop.folds_case())
    return {"file": path, "service": got.service,
            "overall": got.overall, "method": got.method,
            "tables": len(got.tables), **reconcile(got, records, fold)}


def said(section):
    """One line for the console: the claim, and where it falls short."""
    if not section:
        return ""
    if section.get("error"):
        return f"the service's report {section['file']} {section['error']}"
    head = (f"{section.get('service') or 'the service'} said"
            f" {section.get('overall') or 'nothing overall'}"
            + (f" ({section['method']})" if section.get("method") else ""))
    parts = [f"{len(section[k])} {k}" for k in
             ("missed", "not found", "unproved", "skipped", "unchecked",
              "beyond", "not in the hop") if section.get(k)]
    agreed = len(section.get("agreed") or ())
    if parts:
        return head + ": " + ", ".join(parts) + (
            f"; {agreed} agreed" if agreed else "")
    if agreed:
        return head + f", and migkit agrees on each of its {agreed} tables"
    return head + ", naming no table to hold to migkit's answers"
