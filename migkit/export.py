"""The evidence of a migration, exported: what migkit did on a hop and
what it measured, in the forms audit and change processes take - HTML and
Markdown for people, JSON and CSV for tools (backlog R23.1).

Not a certificate. migkit is no authority and issues no standard; what
the paid services hand over are reports of what they ran, and so is this.
What makes it worth referencing is that anyone can check it again: every
table's rows and digest on each side are written with the exact way they
were taken - the statement each server ran, the run's salt, the rendering
of each value where the two sides are two engines (`digests`) - so the
reader runs the statement on each server, or `migkit check` again, and
gets the same numbers. It claims only what was measured.

Deeper than the reports it stands beside: where a validation report says
a table failed, this carries both sides' counts and digests, the finding
in the check's words (rows missing, extra or changed, and where the rows
are written), the repair with its statements and their undo kept (their
digests here, their contents where the hop keeps them - encrypted where
it asks); where a run report gives phases, this gives the ways the engine
chose for each unit of work and why every other was passed over, the
positions each copy and follow was fenced at, the seconds each phase of a
cutover took and the write outage measured, who approved each step, and
every row a two-way tail found both sides had changed - both versions,
the row before, the rule each column was settled by and what it took.

One bundle per run, written at the end of every command that left
anything (`write_run`, under `evidence/<run>/`), and one for the hop's
whole history (`write`, by `migkit report`, under `evidence/`): the JSON
(`evidence.json`), the page (`evidence.html`), the text (`evidence.md`)
and every table's numbers (`tables.csv`). Each is anchored: its SHA-256 is
added to the hop's record (`audit`), whose chain makes an entry changed
afterwards show; where the hop keeps its run state in a bucket, the same
digest is put there too (an object-locked bucket keeps it unchangeable);
where the hop names a key (`evidence: {sign: <private key>}`, or
`MIGKIT_EVIDENCE_KEY`), the JSON is signed with it (`ssh-keygen -Y`);
where the hop names recipients for what holds values (`at_rest`), the
files are written encrypted to them (`evidence`), since a finding's words
can quote a key.

No program migkit drives is named anywhere in it: a way is said as the
engine's ladder says it, and anything else is put through the same words
filter every message is (`wording.without_programs`). No secret is in
it: a record entry's key that names one (`redact.SECRET_KEY`) is not
carried out, and every file is cleaned of the hop's secrets in every
form and of any secret by its shape (`redact.Redactor`) as it is
written. Its digests leave the machine for the run state's bucket only
through the one gate of what leaves (`egress`).

A run's own part is read from where the files stood as it began
(`runlog.appended`), not from the hop's whole history: the export at the
end of every command, a schedule's per-minute "not due" tick included,
reads only what the run wrote.
"""
import csv
import hashlib
import html
import io
import json
import os
import subprocess
import time

from . import __version__

FORMAT_VERSION = 1
DIR = "evidence"
#: the files of one bundle, as the hop's at-rest encryption names them
FILES = ("evidence.json", "evidence.html", "evidence.md", "tables.csv")
#: the namespace a signature is made in and checked against
NAMESPACE = "migkit-evidence"

WHAT = ("What migkit did on this hop and what it measured: what was moved,"
        " every verdict with the counts and digests of both sides and the"
        " way to take each again, the positions, the ways the engine chose"
        " and why, approvals, repairs, timings and outages, and the record"
        " they are chained in. Not a certificate: it claims only what was"
        " measured, and anyone can take each number again.")

#: the files a copy or a follow is fenced at, per database
POSITIONS = ("copy-position.json", "dump-position.json", "tail-token.json",
             "delta-pos.json", "delta-token.json", "pair-delta-token.json",
             "delta-offsets.json", "delta-seqno.json")
#: record entries that took something away or left it behind
REMOVALS = ("follow-drop", "replicate-drop", "two-way-drop", "stream-down",
            "rollback", "rollback-counters")
#: record entries that changed the target's rows after the copy
REPAIRS = ("repair", "sync", "rollback", "rollback-counters")


# ---- words ------------------------------------------------------------------

def _names():
    """Every program and library migkit wraps, by the names its own
    registries know them by - what the words filter takes out. An
    engine's name is the database's, and stays."""
    from . import physical, tools, tooluse
    from .engines import ALIASES, NAMES
    from .storedcode import programs
    tooluse._load()
    engines = set(NAMES) | set(ALIASES)
    return (tools.names() | set(tools.IMAGES) | set(tooluse.REGISTRY)
            | set(programs.READS) | set(physical.PROGRAMS)
            | {n for n in set(tools.PROGRAM_LICENCES) | set(tools.PROGRAMS)
               if n not in engines and n != "docker"})


_NAMES = None


def words(text):
    """`text` with no program migkit drives named in it."""
    global _NAMES
    if text is None:
        return None
    if _NAMES is None:
        _NAMES = _names()
    from .wording import without_programs
    return without_programs(str(text), _NAMES)


_WAYS = None


def _ways():
    """{a way's label in the code: its words}, from the engine's own
    ladders."""
    global _WAYS
    if _WAYS is None:
        from . import leaves, movers
        got = {}
        rungs = list(movers._ladder()) + list(movers.stream_ways())
        for part in leaves.PASSES:
            rungs += list(part.rungs)
        for r in rungs:
            got.setdefault(r.name, r.said)
        _WAYS = got
    return _WAYS


def way(label):
    """A way the code names by its label, in the words an operator reads:
    its rung's `said`, a prefix kept (`cdc/follow`), or the label put
    through the words filter."""
    label = str(label or "")
    ways = _ways()
    if label in ways:
        return ways[label]
    for sep in ("/", ":", "-"):
        head, found, tail = label.rpartition(sep)
        if found and tail in ways:
            return f"{words(head)} {ways[tail]}".strip()
    return words(label)


def _op(entry):
    """A record entry's operation in words: a move by a way names the
    way as the ladder says it."""
    op = str(entry.get("op") or "")
    if op.startswith("move-") and op not in ("move-routed", "move-ddl-seen",
                                             "move-physical"):
        return f"move by {way(op[len('move-'):])}"
    return words(op)


def _plain(value, depth=0, drop=True):
    """A value from a record, carried out as it is where it is plain,
    with no program's name - and, from an entry anyone wrote (`drop`),
    no key that names a secret (`redact.SECRET_KEY`). migkit's own files
    of positions and timings name keys `token` and `keys` that hold a
    change position and a digest, and keep them."""
    from .redact import SECRET_KEY
    if depth > 6:
        return None
    if isinstance(value, dict):
        return {str(k): _plain(v, depth + 1, drop)
                for k, v in value.items()
                if not (drop and SECRET_KEY.search(str(k)))}
    if isinstance(value, (list, tuple)):
        return [_plain(v, depth + 1, drop) for v in value]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return words(value)


# ---- reading what the hop's reports hold -------------------------------------

def _json(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def _lines(path):
    out = []
    try:
        text = path.read_text()
    except OSError:
        return out
    for raw in text.splitlines():
        try:
            out.append(json.loads(raw))
        except ValueError:
            continue
    return out


def _dbs(root):
    try:
        return sorted(p for p in root.iterdir()
                      if p.is_dir() and p.name not in (DIR, "approvals",
                                                       "undo", "stream"))
    except OSError:
        return []


def _started(session):
    """When a run began, from its id (`session`), as the record writes a
    time; "" for none."""
    stamp = str(session or "").split("-", 1)[0]
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S",
                             time.strptime(stamp, "%Y%m%dT%H%M%S"))
    except ValueError:
        return ""


def _raw_sha(path):
    """The SHA-256 of a file as it is kept - encrypted or not - and
    whether it is encrypted."""
    from .evidence import AGE
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError:
        return None, False
    return hashlib.sha256(raw).hexdigest(), raw.startswith(AGE)


def _file_record(root, ref, statements=None):
    """A file a step kept (an undo, the statements a repair applied): its
    name under the reports, its digest as kept, and how many statements
    it holds where that can be read without a key."""
    if not isinstance(ref, str) or not ref:
        return None
    from pathlib import Path
    path = Path(str(ref))
    if not path.is_absolute():
        path = root / path
    sha, sealed = _raw_sha(path)
    try:
        name = str(path.relative_to(root))
    except ValueError:
        name = path.name
    out = {"file": name, "sha256": sha, "encrypted": sealed}
    if statements is not None:
        out["statements"] = statements
    elif sha and not sealed:
        try:
            out["statements"] = sum(1 for x in open(path).read().splitlines()
                                    if x.strip() and not x.startswith("--"))
        except OSError:
            pass
    return out


def _history(hop, session, name):
    """The lines of the hop's file `name`: the run `session`'s, read from
    where the file stood as it began where this process began it, else
    every part of the file (`runlog.parts`)."""
    from . import runlog
    root = hop.report_dir()
    if session:
        got = runlog.appended(hop, session, name)
        if got is not None:
            return [e for e in got if isinstance(e, dict)
                    and e.get("session") == session]
    out = []
    for part in runlog.parts(root / name):
        out += _lines(part)
    if session:
        out = [e for e in out if e.get("session") == session]
    return out


def _record(hop, session):
    from . import runlog
    root = hop.report_dir()
    n, chained, broken, parts = runlog.verify_parts(root / "changelog.jsonl")
    last = runlog.last_lines(root / "changelog.jsonl", 1)
    try:
        head = json.loads(last[0]).get("hash", "") if last else ""
    except ValueError:
        head = ""
    chain = {"file": "changelog.jsonl", "entries": n, "chained": chained,
             "whole": not broken, "broken": broken, "head": head,
             "parts": parts}
    return _history(hop, session, "changelog.jsonl"), chain


def _route(hop):
    opts = hop.options or {}

    def side(ep, engine):
        where = (f"{ep.user}@{ep.host}:{ep.port}" if ep.user
                 else f"{ep.host}:{ep.port}") if ep.host else ""
        return {"engine": engine, "address": words(where)}
    src_e = opts.get("source_engine", "mysql") \
        if hop.engine == "hetero" else hop.engine
    dst_e = opts.get("target_engine", "postgres") \
        if hop.engine == "hetero" else hop.engine
    return {"source": side(hop.source, src_e),
            "target": side(hop.target, dst_e),
            "databases": [str(d) for d in hop.databases or []],
            "left_alone": [str(x) for x in hop.exclude or []]}


def _assessment(root):
    got = _json(root / "assess.json", []) or []
    return [{"level": i.get("level"), "scope": words(i.get("scope")),
             "item": words(i.get("item")), "detail": words(i.get("detail"))}
            for i in got if isinstance(i, dict)]


def _moved(root, entries, session):
    dbs = {}
    for d in _dbs(root):
        ck = _json(d / "move.json", {}) or {}
        tables = {str(k): {"rows": v.get("moved"), "done": bool(
                  v.get("done"))} for k, v in ck.items()
                  if isinstance(v, dict) and ("moved" in v or "done" in v)}
        if tables:
            dbs[d.name] = {"tables": tables,
                           "rows": sum(int(t["rows"] or 0)
                                       for t in tables.values()),
                           "done": sum(1 for t in tables.values()
                                       if t["done"])}
    since = _started(session)
    runs = []
    for label, kept in sorted((_json(root / "throughput.json", {})
                               or {}).items()):
        if not isinstance(kept, list):
            continue
        for r in kept:
            if not isinstance(r, dict) or (since and str(
                    r.get("at", "")) < since):
                continue
            rows, secs = r.get("rows"), r.get("seconds")
            runs.append({"way": way(label), "rows": rows, "seconds": secs,
                         "bytes": r.get("bytes"), "at": r.get("at"),
                         "rows_per_second": round(rows / secs, 1)
                         if rows and secs else None})
    moves = [{"op": _op(e), "db": e.get("db"), "table": words(e.get(
        "table")), "at": e.get("at")} for e in entries
        if str(e.get("op", "")).startswith(("move", "replicate", "follow",
                                            "stream-up", "reverse",
                                            "two-way"))]
    return {"databases": dbs, "runs": runs, "steps": moves}


def _verdict(root, session):
    got = _json(root / "verdict.json")
    if not isinstance(got, dict):
        return None
    if session and got.get("session") != session:
        return None
    keep = ("status", "why", "generated", "has_differences", "totals",
            "fingerprint", "coverage", "stale", "load", "session",
            "tool_version", "by_category", "digests_unwritten")
    return {k: _plain(got[k], drop=False) for k in keep if k in got}


def _checks(root, verdict):
    if verdict is None:
        return []
    got = _json(root / "summary.json", []) or []
    return [{"check": r.get("check"), "scope": words(r.get("scope")),
             "category": r.get("category"), "status": r.get("status"),
             "detail": words(r.get("detail")),
             "fix": words(r.get("fix_hint")),
             **({"stale": words(r["stale"])} if r.get("stale") else {})}
            for r in got if isinstance(r, dict)]


def _tables(hop, session):
    from . import digests
    out = []
    for (db, table, where), sides in sorted(digests.read(hop, session)
                                            .items()):
        src, dst = sides.get("src"), sides.get("dst")
        if src and dst:
            same = (src.get("rows") == dst.get("rows")
                    and str(src.get("digest")) == str(dst.get("digest")))
            status = "same" if same else "different"
        else:
            status = "one side only"
        one = src or dst
        row = {"db": db, "table": words(table), "where": words(where),
               "status": status, "method": one.get("method"),
               "salt": one.get("salt"), "at": one.get("at"),
               "session": one.get("session")}
        for side, line in (("src", src), ("dst", dst)):
            if not line:
                continue
            row[side] = {"engine": line.get("engine"),
                         "rows": line.get("rows"),
                         "digest": line.get("digest"),
                         **({"keys": line["keys"]} if line.get("keys")
                            else {}),
                         "statement": words(digests.statement(line)),
                         "recompute": digests.recompute(line)}
            if line.get("columns"):
                row[side]["columns"] = [list(c) for c in line["columns"]]
        out.append(row)
    return out


def _method(tables):
    """How every digest above is taken, in words and in the code's own
    renderings - enough to take each again without migkit."""
    from . import canon, digests
    used = sorted({t.get("method") for t in tables if t.get("method")})
    words_of = digests.methods()
    out = {"methods": {m: words_of.get(m, m) for m in used},
           "salt": ("drawn for each run and written beside each table's"
                    " numbers: the same statement with the same salt gives"
                    " the same digest; a new run draws a new salt, so a"
                    " pair of rows that collided once does not collide"
                    " again"),
           "again": ("`migkit check` on the hop takes every number again,"
                     " with a new salt")}
    if digests.NEUTRAL in used:
        classes = sorted({c for t in tables for side in ("src", "dst")
                          for _, c in (t.get(side) or {}).get("columns", [])})
        rendering = {}
        for cls in classes:
            per = {}
            for engine in ("postgres", "mysql"):
                try:
                    per[engine] = canon.expr(engine, "<column>", cls)
                except ValueError:
                    continue
            rendering[cls] = per
        out["rendering"] = {
            "classes": rendering,
            "floats": (f"a float between {canon.FLOAT_MIN} and"
                       f" {canon.FLOAT_MAX} (absolute) is written as a"
                       f" decimal with {canon.FLOAT_SCALE} digits after the"
                       " point; outside that band, as the shortest text"
                       " that reads back as the same double"),
            "uncomparable": ("a value no rendering can hold (an infinity,"
                             " a NaN, a year before Christ) is written as"
                             " a mark of its own with its name, and meets"
                             " only the same value"),
            "digest": {e: canon.digest_expr(e, "<row text>")
                       for e in ("postgres", "mysql")},
            "reference": ("migkit's own source (AGPL): canon.render_value"
                          " renders a value, rowtext.encode joins a row,"
                          " canon.digest_step folds it")}
    return out


def _code(root):
    out = []
    for d in _dbs(root):
        proof = _json(d / "code-proof.json", {}) or {}
        for v in proof.get("verdicts") or []:
            if isinstance(v, dict):
                out.append({"db": d.name, "routine": words(v.get("routine")),
                            "status": v.get("status"),
                            "reason": words(v.get("reason")),
                            "coverage": _plain(v.get("coverage"),
                                               drop=False),
                            "counterexample": words(v.get("first"))
                            if v.get("first") else None})
        conv = _json(d / "code-conversion.json", {}) or {}
        for r in conv.get("routines") or []:
            if isinstance(r, dict):
                out.append({"db": d.name, "routine": words(r.get("routine")),
                            "class": words(r.get("class")),
                            "converted by": way(r.get("rung"))
                            if r.get("rung") else None})
    return out


def _positions(root):
    """{database, or "hop" for the hop's own: {file: position}}."""
    out = {}
    for d in [root] + _dbs(root):
        where = "hop" if d == root else d.name
        for name in POSITIONS + (("physical-seed.json",) if d == root
                                 else ()):
            got = _json(d / name)
            if got is not None:
                out.setdefault(where, {})[name] = _plain(got, drop=False)
    return out


def _decisions(hop, session):
    from .session import DECISIONS
    seen = {}
    for d in _history(hop, session, DECISIONS):
        key = (d.get("work"), d.get("way"), d.get("reason"))
        got = seen.setdefault(key, {
            "work": words(d.get("work")), "way": words(d.get("way")),
            "reason": words(d.get("reason")),
            "passed": [{"way": words(p.get("way")), "why": words(
                p.get("why"))} for p in d.get("passed") or []],
            "first": d.get("at"), "runs": set()})
        got["last"] = d.get("at")
        got["runs"].add(d.get("session"))
    out = []
    for got in seen.values():
        got["runs"] = len(got["runs"])
        out.append(got)
    return out


def _approvals(root, entries):
    out = []
    for e in entries:
        if e.get("op") != "approved":
            continue
        req = f"{e.get('step')}-{e.get('db') or 'all'}"
        where = root / e["db"] / "approvals" if e.get("db") else \
            root / "approvals"
        sigs = sorted(where.glob(f"{req}.*.sig")) if where.is_dir() else []
        out.append({"step": e.get("step"), "db": e.get("db"),
                    "approvers": list(e.get("approvers") or []),
                    "request": e.get("request"), "at": e.get("at"),
                    "run by": e.get("who"), "host": e.get("host"),
                    "signatures": [{"file": s.name,
                                    "sha256": _raw_sha(s)[0]}
                                   for s in sigs]})
    return out


def _repairs(root, entries):
    out = []
    for e in entries:
        if e.get("op") not in REPAIRS:
            continue
        out.append({"op": _op(e), "db": e.get("db"),
                    "kind": words(e.get("kind")),
                    "detail": words(e.get("detail")), "at": e.get("at"),
                    "run by": e.get("who"), "host": e.get("host"),
                    "applied": _file_record(root, e.get("applied_ref"),
                                            e.get("statements")),
                    "undo": _file_record(root, e.get("undo_ref"))})
    return out


def _cutovers(root, entries, session):
    ids = {e.get("run") for e in entries if e.get("op") == "cutover"}
    out = []
    for rec in _lines(root / "cutover-runs.jsonl"):
        if session and rec.get("id") not in ids:
            continue
        conf = rec.get("settings") or {}
        out.append({"id": rec.get("id"), "state": rec.get("state"),
                    "began": _when(rec.get("began")),
                    "ended": _when(rec.get("ended")),
                    "outage_seconds": rec.get("outage"),
                    "budget_seconds": conf.get("budget"),
                    "freeze": words(rec.get("freeze")),
                    "phases_seconds": _plain(rec.get("took") or {},
                                             drop=False),
                    "approvers": list(rec.get("approvers") or []),
                    "databases": list(rec.get("dbs") or []),
                    "why": words(rec.get("why")),
                    "left_on_source": {d: len(s) for d, s in (
                        rec.get("left_on_source") or {}).items()}})
    return out


#: the conflicts the human views list one by one; the JSON carries all
CONFLICTS_SHOWN = 500


def _conflicts(hop, root, session):
    """Every row a two-way tail found both sides had changed (`twoway`):
    its kind, the row before, both sides' versions, each settled column's
    rule and what it took, what the row ended as and what was decided -
    values masked where the hop masks them - and the rows parked now."""
    from . import masking, twoway

    def shown(table, row):
        if not isinstance(row, dict):
            return row
        return {k: masking.shown(hop, table, k, v) for k, v in row.items()}
    rows, parked, sealed = [], {}, 0
    for d in _dbs(root):
        try:
            got = _lines(d / twoway.RECORD)
        except SystemExit:
            # sealed to the hop's recipients, and no key here to open it
            got, sealed = [], sealed + 1
        for rec in got:
            if session and rec.get("session") != session:
                continue
            t = rec.get("table")
            key = rec.get("key") or {}
            if masking.keys(hop, t):
                key = {k: masking.token(hop, v) for k, v in key.items()}
            rows.append({"db": d.name, "table": t, "key": key,
                         "kind": rec.get("kind"), "ops": rec.get("ops"),
                         "rule": rec.get("policy"),
                         "decision": rec.get("decision"),
                         "columns": [{"columns": r.get("columns"),
                                      "rule": r.get("rule"),
                                      "took": r.get("took"),
                                      "tie": r.get("tie")}
                                     for r in rec.get("rules") or []],
                         "before": shown(t, rec.get("remote_before")),
                         "source": shown(t, rec.get("remote")),
                         "target": shown(t, rec.get("local")),
                         "result": shown(t, rec.get("result")),
                         "at": rec.get("at")})
        counts = _json(d / twoway.COUNTS, {}) or {}
        for t, n in (counts.get("parked") or {}).items():
            if n:
                parked[f"{d.name}.{t}"] = n
    return {"rows": rows, "parked": parked,
            "unreadable": sealed}


def _when(epoch):
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S",
                             time.localtime(float(epoch)))
    except (TypeError, ValueError):
        return None


def _timings(root, session):
    got = _json(root / "check-timings.json", {}) or {}
    if session and got.get("session") != session:
        got = {}
    return {"checks_seconds": _plain(got.get("databases") or {},
                                     drop=False),
            "checks_at": got.get("at")}


def _steps(entries):
    out = []
    for e in entries:
        step = _plain({k: v for k, v in e.items()
                       if k not in ("prev", "hash", "op", "undo_ref",
                                    "applied_ref")})
        step["op"] = _op(e)
        out.append(step)
    return out


def _movers(hop):
    """Which machine wrote which batch and which range, and the proof of
    one writer at a time, where the hop's machines work together (`ha`)."""
    from . import ha
    try:
        rows = ha.evidence(hop)
    except Exception:  # noqa: BLE001 - nothing kept to say
        return []
    return [{k: words(v) if isinstance(v, str) else _plain(v)
             for k, v in r.items()} for r in rows]


def gather(hop, session=None):
    """The machine view: the hop's whole history, or the run `session`'s
    part of it."""
    from . import digests, evidence
    root = hop.report_dir()
    entries, chain = _record(hop, session)
    verdict = _verdict(root, session)
    tables = _tables(hop, session)
    return {
        "format_version": FORMAT_VERSION, "what": WHAT, "tool": "migkit",
        "tool_version": __version__, "hop": hop.name,
        "goal": getattr(hop, "goal", "") or "copy",
        "scope": "run" if session else "hop", "session": session,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "route": _route(hop),
        "assessment": _assessment(root) if not session else [],
        "moved": _moved(root, entries, session),
        "verdict": verdict,
        "checks": _checks(root, verdict),
        "tables": tables,
        "method": _method(tables),
        "code": _code(root) if not session else [],
        "positions": _positions(root),
        "decisions": _decisions(hop, session),
        "approvals": _approvals(root, entries),
        "repairs": _repairs(root, entries),
        "cutovers": _cutovers(root, entries, session),
        "conflicts": _conflicts(hop, root, session),
        "timings": _timings(root, session),
        "removed": [{"op": _op(e), "db": e.get("db"), "at": e.get("at")}
                    for e in entries if e.get("op") in REMOVALS],
        "steps": _steps(entries),
        "movers": _movers(hop),
        "record": chain,
        "at_rest": {"encrypted_to": len(evidence.recipients_of(hop))},
        # digests the run took and could not keep: the tables above are
        # fewer than the check compared by as many
        **({"digests_unwritten": digests.unwritten(session)}
           if session and digests.unwritten(session) else {}),
    }


def left_anything(hop, session):
    """Whether the run `session` left anything to export: an entry in
    the record (an export's own anchor, a rotation's carried head are
    none), a way chosen, a digest taken, a verdict written - read from
    where each file stood as the run began (`runlog.appended`), so a run
    that left nothing costs a few `stat`s."""
    from . import digests, runlog
    from .session import DECISIONS
    root = hop.report_dir()
    if any(e.get("op") not in ("evidence", "rotated")
           for e in _history(hop, session, "changelog.jsonl")):
        return True
    if _history(hop, session, DECISIONS):
        return True
    if digests.read(hop, session):
        return True
    if digests.unwritten(session):
        return True
    if runlog.verdict_written(hop, session) is False:
        return False
    got = _json(root / "verdict.json")
    return isinstance(got, dict) and got.get("session") == session


# ---- the human views ----------------------------------------------------------

def _cell(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:,}"
    return str(value)


def _md_table(head, rows):
    if not rows:
        return "none\n"
    out = ["| " + " | ".join(head) + " |",
           "|" + "|".join("---" for _ in head) + "|"]
    for r in rows:
        out.append("| " + " | ".join(_cell(x).replace("|", "\\|")
                                     .replace("\n", " ") for x in r) + " |")
    return "\n".join(out) + "\n"


def _sections(doc):
    """(title, head, rows) of every table both views show, in order."""
    moved = doc["moved"]
    out = []
    out.append(("what was moved", ["db", "tables", "done", "rows"],
                [(db, len(m["tables"]), m["done"], m["rows"])
                 for db, m in moved["databases"].items()]))
    out.append(("copies timed", ["way", "rows", "seconds", "rows a second",
                                 "at"],
                [(r["way"], r["rows"], r["seconds"], r["rows_per_second"],
                  r["at"]) for r in moved["runs"]]))
    out.append(("checks", ["check", "scope", "status", "what was found"],
                [(c["check"], c["scope"], c["status"], c["detail"])
                 for c in doc["checks"]]))
    out.append(("tables: counts and digests of both sides",
                ["db", "table", "status", "source rows", "source digest",
                 "target rows", "target digest", "method"],
                [(t["db"], t["table"] + (f" ({t['where']})" if t.get(
                    "where") else ""), t["status"],
                  (t.get("src") or {}).get("rows"),
                  (t.get("src") or {}).get("digest"),
                  (t.get("dst") or {}).get("rows"),
                  (t.get("dst") or {}).get("digest"), t["method"])
                 for t in doc["tables"]]))
    out.append(("ways the engine chose", ["work", "way", "why",
                                          "passed over"],
                [(d["work"], d["way"], d["reason"],
                  "; ".join(f"{p['way']}: {p['why']}"
                            for p in d["passed"]))
                 for d in doc["decisions"]]))
    out.append(("approvals", ["step", "db", "approved by", "at",
                              "signatures"],
                [(a["step"], a["db"], ", ".join(a["approvers"]), a["at"],
                  len(a["signatures"])) for a in doc["approvals"]]))
    out.append(("repairs", ["what", "db", "kind", "at", "statements",
                            "applied (sha256)", "undo (sha256)"],
                [(r["op"], r["db"], r["kind"], r["at"],
                  (r["applied"] or {}).get("statements"),
                  (r["applied"] or {}).get("sha256"),
                  (r["undo"] or {}).get("sha256"))
                 for r in doc["repairs"]]))
    out.append(("cutovers and the write outage measured",
                ["run", "state", "began", "outage (s)", "budget (s)",
                 "phases (s)", "approved by"],
                [(c["id"], c["state"], c["began"], c["outage_seconds"],
                  c["budget_seconds"], ", ".join(
                      f"{p} {s}" for p, s in c["phases_seconds"].items()),
                  ", ".join(c["approvers"])) for c in doc["cutovers"]]))
    conflicts = doc.get("conflicts") or {}
    listed = conflicts.get("rows") or []
    rows = [(c["db"], c["table"], c["kind"], "; ".join(
                f"{', '.join(r['columns'] or [])}: {r['rule']} took"
                f" {r['took']}" + (" (a tie)" if r.get("tie") else "")
                for r in c["columns"]) or c["rule"], c["decision"], c["at"])
            for c in listed[:CONFLICTS_SHOWN]]
    if len(listed) > CONFLICTS_SHOWN:
        rows.append(("", "", f"and {len(listed) - CONFLICTS_SHOWN:,} more,"
                     " every one in evidence.json", "", "", ""))
    rows += [(k.partition(".")[0], k.partition(".")[2],
              f"{n:,} row(s) parked now", "", "parked", "")
             for k, n in sorted((conflicts.get("parked") or {}).items())]
    out.append(("rows both sides changed, and how each was settled",
                ["db", "table", "kind", "columns: rule took", "decided",
                 "at"], rows))
    out.append(("stored code", ["db", "routine", "status", "why",
                                "counterexample"],
                [(c["db"], c["routine"], c.get("status") or c.get("class"),
                  c.get("reason") or c.get("converted by"),
                  c.get("counterexample")) for c in doc["code"]]))
    out.append(("assessment before the move", ["level", "scope", "item",
                                                "detail"],
                [(a["level"], a["scope"], a["item"], a["detail"])
                 for a in doc["assessment"]]))
    out.append(("who wrote what", ["db", "what", "machine", "number",
                                   "from", "to"],
                [(m.get("db"), m.get("what"), m.get("by"), m.get("number"),
                  m.get("from"), m.get("to")) for m in doc.get("movers")
                 or []]))
    out.append(("left and removed", ["what", "db", "at"],
                [(r["op"], r["db"], r["at"]) for r in doc["removed"]]))
    out.append(("the record", ["at", "op", "db", "who", "host"],
                [(s.get("at"), s.get("op"), s.get("db"), s.get("who"),
                  s.get("host")) for s in doc["steps"]]))
    return out


def _headline(doc):
    """(the verdict's key, its words, the tables not the same): the
    verdict said as every output says it (`verdict.VERDICTS`)."""
    from .verdict import VERDICTS
    v = doc.get("verdict") or {}
    key = v.get("status")
    said = VERDICTS[key].word if key in VERDICTS else (
        "no check in this run" if doc["scope"] == "run" else "not checked")
    diff = sum(1 for t in doc["tables"] if t["status"] != "same")
    return key, said, diff


def _unwritten(doc):
    n = doc.get("digests_unwritten") or 0
    return (f"; {n} digest{'s' if n > 1 else ''} the check took could not"
            " be kept, so as many tables are missing here" if n else "")


def markdown(doc):
    _, status, diff = _headline(doc)
    route = doc["route"]
    chain = doc["record"]
    out = [f"# migkit evidence: {doc['hop']}",
           "",
           doc["what"],
           "",
           f"- scope: {doc['scope']}"
           + (f" (run {doc['session']})" if doc.get("session") else ""),
           f"- generated: {doc['generated']} by migkit {doc['tool_version']}",
           f"- route: {route['source']['engine']} {route['source']['address']}"
           f" -> {route['target']['engine']} {route['target']['address']}",
           f"- verdict: **{status}**; {len(doc['tables'])} tables with"
           f" digests, {diff} not the same{_unwritten(doc)}",
           f"- record: {chain['entries']} entries, {chain['chained']}"
           " chained, " + ("the chain is whole" if chain["whole"] else
                          f"the chain is broken: {chain['broken']}")
           + (f", head {chain['head'][:16]}" if chain["head"] else ""),
           ""]
    for title, head, rows in _sections(doc):
        out += [f"## {title}", "", _md_table(head, rows)]
    out += ["## how to take each number again", ""]
    m = doc["method"]
    for name, said in m["methods"].items():
        out.append(f"- **{name}**: {said}")
    out += [f"- salt: {m['salt']}", f"- again: {m['again']}", ""]
    for t in doc["tables"]:
        for side in ("src", "dst"):
            s = t.get(side) or {}
            if s.get("statement"):
                out += [f"`{t['db']}` `{t['table']}` {side}:", "",
                        "```sql", s["statement"], "```", ""]
    if m.get("rendering"):
        out += ["### the rendering of each value", ""]
        for cls, per in m["rendering"]["classes"].items():
            for engine, sql in per.items():
                out.append(f"- {cls} on {engine}: `{sql}`")
        r = m["rendering"]
        out += ["", f"- floats: {r['floats']}",
                f"- uncomparable: {r['uncomparable']}",
                f"- reference: {r['reference']}", ""]
    if doc.get("signature"):
        out += ["## signature", "", doc["signature"]["check"], ""]
    return "\n".join(out) + "\n"


def page(doc):
    """The HTML view, in the look of migkit's other reports."""
    from .report import CSS
    e = html.escape
    key, status, diff = _headline(doc)
    route = doc["route"]
    chain = doc["record"]
    ok = key == "same" and not diff and chain["whole"] \
        and not doc.get("digests_unwritten")
    banner = (f'<div class="banner {"pass" if ok else "fail"}">'
              f'<span class="dot {"ok" if ok else "error"}"></span>'
              f"{e(str(status))} &mdash; {len(doc['tables'])} tables with"
              f" digests, {diff} not the same{e(_unwritten(doc))}</div>")
    tiles = [(sum(m["rows"] for m in doc["moved"]["databases"].values()),
              "rows moved"),
             (len(doc["tables"]), "tables digested"),
             (len(doc["repairs"]), "repairs"),
             (len(doc["approvals"]), "approvals"),
             (max((c["outage_seconds"] or 0 for c in doc["cutovers"]),
                  default=0), "outage (s)"),
             (chain["entries"], "record entries")]
    cards = []
    for title, head, rows in _sections(doc):
        if rows:
            body = "".join(
                "<tr>" + "".join(f"<td>{e(_cell(x))}</td>" for x in r)
                + "</tr>" for r in rows)
            table = (f"<table><thead><tr>"
                     + "".join(f"<th>{e(h)}</th>" for h in head)
                     + f"</tr></thead><tbody>{body}</tbody></table>")
        else:
            table = '<div class="sub">none</div>'
        cards.append(f'<div class="card"><h2>{e(title)}</h2>'
                     f'<div style="overflow-x:auto">{table}</div></div>')
    m = doc["method"]
    how = "".join(f"<p><b>{e(n)}</b>: {e(s)}</p>"
                  for n, s in m["methods"].items())
    how += f"<p>salt: {e(m['salt'])}</p><p>{e(m['again'])}</p>"
    for t in doc["tables"]:
        for side in ("src", "dst"):
            s = t.get(side) or {}
            if s.get("statement"):
                how += (f"<details><summary>{e(t['db'])} {e(t['table'])}"
                        f" {side}</summary><pre>{e(s['statement'])}</pre>"
                        "</details>")
    if m.get("rendering"):
        r = m["rendering"]
        for cls, per in r["classes"].items():
            for engine, sql in per.items():
                how += (f"<details><summary>{e(cls)} on {e(engine)}"
                        f"</summary><pre>{e(sql)}</pre></details>")
        how += (f"<p>floats: {e(r['floats'])}</p><p>uncomparable:"
                f" {e(r['uncomparable'])}</p><p>{e(r['reference'])}</p>")
    if doc.get("signature"):
        how += (f"<p>signature: <code>{e(doc['signature']['check'])}"
                "</code></p>")
    record = (f"{chain['entries']} entries, {chain['chained']} chained, "
              + ("the chain is whole" if chain["whole"]
                 else f"the chain is broken: {chain['broken']}")
              + (f", head {chain['head'][:16]}" if chain["head"] else ""))
    sub = " / ".join((
        "migkit evidence", doc["scope"] + (f" {doc['session']}"
                                           if doc.get("session") else ""),
        doc["generated"], f"migkit {doc['tool_version']}"))
    line = (f"{route['source']['engine']} {route['source']['address']}"
            f" -> {route['target']['engine']}"
            f" {route['target']['address']}")
    shown = "".join(f'<div class="tile"><div class="n">{e(_cell(n))}</div>'
                    f'<div class="l">{e(label)}</div></div>'
                    for n, label in tiles)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit evidence - {e(doc['hop'])}</title>
<style>{CSS}</style></head><body><div class="wrap">
<div class="card"><h1>{e(doc['hop'])}</h1>
<div class="sub">{e(sub)}</div>
<div class="route">{e(line)}</div>
<div class="sub">{e(doc['what'])}</div></div>
{banner}
<div class="tiles">{shown}</div>
{"".join(cards)}
<div class="card"><h2>how to take each number again</h2>{how}</div>
<div class="card"><h2>the record</h2><div class="sub">{e(record)}</div></div>
<footer>claims only what was measured; nothing here is a certificate</footer>
</div></body></html>"""


def table_rows(doc):
    """Every table's numbers as CSV, a row a table and filter."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["db", "table", "where", "status", "method", "src_engine",
                "src_rows", "src_digest", "src_keys", "dst_engine",
                "dst_rows", "dst_digest", "dst_keys", "salt", "at",
                "session"])
    for t in doc["tables"]:
        s, d = t.get("src") or {}, t.get("dst") or {}
        w.writerow([t["db"], t["table"], t.get("where") or "", t["status"],
                    t["method"], s.get("engine"), s.get("rows"),
                    s.get("digest"), s.get("keys"), d.get("engine"),
                    d.get("rows"), d.get("digest"), d.get("keys"),
                    t.get("salt"), t.get("at"), t.get("session")])
    return buf.getvalue()


# ---- writing and anchoring ------------------------------------------------------

def _signing_key(hop):
    got = (hop.options or {}).get("evidence") or {}
    key = got.get("sign") if isinstance(got, dict) else None
    key = key or os.environ.get("MIGKIT_EVIDENCE_KEY")
    return os.path.expanduser(str(key)) if key else None


def _sign(path, key, namespace=NAMESPACE):
    """`path` signed with the private SSH key `key` (`ssh-keygen -Y`) in
    `namespace`, beside it as `<path>.sig`; raises where it could not be.
    A namespace a signature is made for is checked with it: an evidence
    signature is no seed manifest's (`ship`)."""
    got = subprocess.run(["ssh-keygen", "-Y", "sign", "-n", namespace,
                          "-f", key, str(path)], capture_output=True,
                         text=True, stdin=subprocess.DEVNULL, timeout=60)
    if got.returncode != 0:
        said = (got.stderr or got.stdout).strip().splitlines()
        raise RuntimeError(said[-1] if said else "could not sign")


def _verify(path, signature, allowed, namespace=NAMESPACE):
    """The signer `signature` (`_sign`'s) names for `path`, from the
    allowed signers file `allowed`; raises where no signer there made it
    or the file is not what was signed."""
    found = subprocess.run(["ssh-keygen", "-Y", "find-principals", "-s",
                            str(signature), "-f", str(allowed)],
                           capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, timeout=60)
    who = (found.stdout or "").strip().splitlines()
    if found.returncode != 0 or not who:
        raise RuntimeError("no signer the allowed signers name made this"
                           " signature")
    with open(path, "rb") as f:
        got = subprocess.run(["ssh-keygen", "-Y", "verify", "-f",
                              str(allowed), "-I", who[0], "-n", namespace,
                              "-s", str(signature)], stdin=f,
                             capture_output=True, text=True, timeout=60)
    if got.returncode != 0:
        said = (got.stderr or got.stdout).strip().splitlines()
        raise RuntimeError(said[-1] if said else "the signature does not"
                           " hold")
    return who[0]


def write(hop, session=None, log=None):
    """The bundle of the hop (`session` None: its whole history) or of
    one run, written under the hop's reports, anchored in its record;
    the directory it is in."""
    from . import audit
    from .redact import Redactor
    say = log or (lambda m: None)
    clean = Redactor(hop)
    root = hop.report_dir()
    out = root / DIR / session if session else root / DIR
    out.mkdir(parents=True, exist_ok=True)
    doc = _cleaned(gather(hop, session), clean)
    key = _signing_key(hop)
    if key:
        doc["signature"] = {"namespace": NAMESPACE,
                            "file": "evidence.json.sig",
                            "check": _sign_line()}
    body = json.dumps(doc, indent=1, sort_keys=True, default=str)
    (out / "evidence.json").write_text(body)
    if key:
        try:
            _sign(out / "evidence.json", key)
        except Exception as e:  # noqa: BLE001 - said; the bundle stands
            say(f"the evidence could not be signed ({clean(str(e))[:160]});"
                " it is written unsigned")
            (out / "evidence.json.sig").unlink(missing_ok=True)
            doc.pop("signature")
            body = json.dumps(doc, indent=1, sort_keys=True, default=str)
            (out / "evidence.json").write_text(body)
    (out / "evidence.html").write_text(page(doc))
    (out / "evidence.md").write_text(markdown(doc))
    (out / "tables.csv").write_text(table_rows(doc))
    anchor = {"op": "evidence", "scope": doc["scope"],
              "files": {name: _raw_sha(out / name)[0] for name in FILES},
              "plain_sha256": hashlib.sha256(body.encode()).hexdigest(),
              "dir": str(out.relative_to(root))}
    audit.append(root / "changelog.jsonl", anchor)
    _anchor_remote(hop, out, anchor, say)
    return out


#: keys whose values are numbers or digests migkit took, which anyone
#: takes again to check them: never changed by the secrets' cleaning (a
#: short password's value found inside a digest would change the digest)
_TAKEN = ("digest", "keys", "salt", "sha256", "plain_sha256", "head",
          "hash", "prev", "fingerprint", "rows", "session")


def _cleaned(value, clean, key=""):
    """The bundle with every text cleaned of the hop's secrets, in every
    form and by their shape (`redact.Redactor`), before any view of it is
    rendered - the numbers and digests in it as they were taken."""
    if isinstance(value, dict):
        return {k: _cleaned(v, clean, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_cleaned(v, clean, key) for v in value]
    if isinstance(value, str) and key not in _TAKEN:
        return clean(value)
    return value


def _sign_line():
    return (f"ssh-keygen -Y verify -f allowed_signers -I <signer>"
            f" -n {NAMESPACE} -s evidence.json.sig < evidence.json")


def _anchor_remote(hop, out, anchor, say):
    """The bundle's digests put in the hop's run-state bucket, where it
    keeps one and the egress lets them go there: an object-locked bucket
    keeps them as written."""
    from . import egress, state
    if not egress.allows(hop, "evidence", say):
        return
    try:
        remote = state.run_state(hop)
    except Exception:  # noqa: BLE001 - no bucket to reach is no anchor
        return
    if remote is None:
        return
    conf = (hop.options or {}).get("state") or {}
    where = (conf.get("endpoint_url") if isinstance(conf, dict) else None) \
        or "https://s3.amazonaws.com"
    if not egress.permit(hop, "evidence", where, log=say):
        return
    try:
        remote.put(out / "evidence.sha256.json",
                   json.dumps({**anchor, "at": time.strftime(
                       "%Y-%m-%dT%H:%M:%S%z")}, sort_keys=True))
    except Exception as e:  # noqa: BLE001 - said; the bundle stands
        say("the evidence's digests could not be put in the run state's"
            f" bucket ({type(e).__name__})")


def write_run(hop, session, log=None):
    """The run's bundle, where the run left anything; None where not."""
    if not left_anything(hop, session):
        return None
    return write(hop, session, log)
