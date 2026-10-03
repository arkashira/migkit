"""What the pages and the API show of a hop, read once for both - the
page is the API's answer laid out, never a second reading (backlog W2).

Everything is read from what migkit's own commands keep and through the
functions they use: the goal's words (`hopkeys.explain_goal`), the plan
each move keeps (`model.load`, its phases with their p50 and p90), the
outage held to the hop's limits (`jointplan.outage_line`), a move's
progress (`planner.read_progress`), a tail's state (`tailctl.state`), the
verdict (`verdict.json`), the metrics `/metrics` serves (`ui.prometheus`),
the evidence (`export`), the record (`audit.verify`), the standing
approvals (`approvals.standing`) and offers (`sourcechange.standing`), the
decision record (`decide.recorded`) and the receivers (`notify.settings`).

What leaves here is said the way the console says it: the hop's secrets
hidden (`redact`), no program migkit drives named (`export.words`), and
the decision record only when it is asked for.
"""
import json
import re
import threading
import time
from pathlib import Path

_LOADED = {"key": None, "hops": {}, "error": ""}
_LOCK = threading.Lock()


def _say(hop, text):
    """Text as it leaves the view: secrets hidden, no program named."""
    from .. import export
    from ..redact import Redactor
    if text is None:
        return None
    return export.words(Redactor(hop)(str(text)))


def _said(hop, value):
    """A record, at any depth, the same way."""
    if isinstance(value, dict):
        return {str(k): _said(hop, v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_said(hop, v) for v in value]
    if isinstance(value, str):
        return _say(hop, value)
    return value


def loaded():
    """(the hops, "" or why they could not be read), read again only when
    the file changes - reading one resolves its secret references, which
    is not done for every page."""
    from .. import config
    path = Path(config.CONF)
    try:
        st = path.stat()
        key = (str(path), st.st_mtime_ns, st.st_size)
    except OSError:
        key = (str(path), None, None)
    with _LOCK:
        if _LOADED["key"] == key:
            return _LOADED["hops"], _LOADED["error"]
    try:
        hops, error = config.load_hops(), ""
    except SystemExit as e:
        from ..redact import shapes
        hops, error = {}, shapes(str(e))
    with _LOCK:
        _LOADED.update(key=key, hops=hops, error=error)
    return hops, error


def visible(principal):
    """[(name, hop, role)] of the hops the person may see."""
    from . import roles
    hops, _ = loaded()
    out = []
    for name, hop in sorted(hops.items()):
        got = roles.role(principal, hop)
        if got is not None:
            out.append((name, hop, got))
    return out


def one(principal, name):
    """(hop, role) of a hop the person may see, or (None, None) - a hop
    they may not see is one that is not there."""
    from . import roles
    hops, _ = loaded()
    hop = hops.get(name)
    if hop is None:
        return None, None
    got = roles.role(principal, hop)
    return (hop, got) if got is not None else (None, None)


def _json_at(path):
    from .. import durable
    try:
        return durable.read_state(path, "cache")
    except SystemExit:
        return None


def _root(hop):
    from .. import config
    return Path(config.REPORTS) / hop.name


#: the files a database's reports hold once a plan, a move or a tail ran
KEPT_PER_DB = ("model.json", "progress.json", "tail.pid", "tail.stopped")


def databases(hop):
    """The hop's databases: those it names, and those its reports hold."""
    names = [str(d) for d in hop.databases or []]
    root = _root(hop)
    try:
        for p in sorted(root.iterdir()):
            if p.is_dir() and p.name not in names and any(
                    (p / f).exists() for f in KEPT_PER_DB):
                names.append(p.name)
    except OSError:
        pass
    return names


def verdict(hop):
    got = _json_at(_root(hop) / "verdict.json")
    if not isinstance(got, dict):
        return None
    return {"status": got.get("status"), "generated": got.get("generated"),
            "why": _say(hop, got.get("why")),
            "has_differences": got.get("has_differences"),
            "totals": got.get("totals") or {},
            "coverage": got.get("coverage"), "session": got.get("session"),
            "findings": [_said(hop, f) for f in (got.get("findings")
                                                 or [])[:200]]}


def card(name, hop, role):
    """A hop as the list shows it."""
    from . import runs
    v = verdict(hop)
    ends = f"{hop.source.host or '?'} -> " \
           f"{hop.target.host or hop.target.options.get('hosts', '?')}"
    try:
        going = [r for r in runs.listing(hop, 20)
                 if r["state"] in ("queued", "running")]
    except OSError:
        going = []
    return {"name": name, "engine": hop.engine,
            "goal": getattr(hop, "goal", "") or "copy",
            "route": _say(hop, ends), "role": role,
            "verdict": v["status"] if v else None,
            "checked": v["generated"] if v else None,
            "running": [{"id": r["id"], "action": r["action"],
                         "by": r["by"]} for r in going],
            "approvals": len(approvals(hop, None)),
            "offers": len(offers(hop)["offered"])}


# ---- what the hop does, and how long it would take ----------------------

def _phase(p):
    return {"name": p.name, "p50": p.p50, "p90": p.p90, "binds": p.binds,
            "truth": p.truth, "detail": p.detail, "never": p.never,
            "said": p.line()}


def outcome(hop):
    """What the hop does to each side and guarantees, and - for each
    database a plan was made for - its phases with their spread and the
    outage held to the hop's limits."""
    from .. import hopkeys, jointplan, model
    out = {"goal": [_say(hop, x) for x in hopkeys.explain_goal(hop)],
           "limits": [_say(hop, x) for x in hopkeys.explain_limits(hop)],
           "databases": []}
    for db in databases(hop):
        m = model.load(_root(hop) / db)
        if m is None or not m.planned:
            continue
        phases = list(m.planned)
        try:
            outage = jointplan.outage_line(hop, phases)
        except SystemExit as e:
            outage = str(e)
        cut = next((p for p in phases if p.name == "cutover"), None)
        total = sum(p.p50 or 0 for p in phases if not p.never)
        spread = sum((p.p90 or p.p50 or 0) for p in phases if not p.never)
        out["databases"].append({
            "db": db, "phases": [_said(hop, _phase(p)) for p in phases],
            "p50": total or None, "p90": spread or None,
            "outage": {"p50": cut.p50, "p90": cut.p90} if cut else None,
            "limits": _say(hop, outage) if outage else ""})
    return out


# ---- where it stands now ---------------------------------------------------

_SAMPLE = re.compile(r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)'
                     r'(?:\{(?P<labels>.*)\})?\s+(?P<value>\S+)')
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def metrics(hop):
    """The samples `/metrics` serves for this hop, with each family's
    help: [{name, labels, value, help}]."""
    from .. import ui
    try:
        text = ui.prometheus({hop.name: hop})
    except Exception:  # noqa: BLE001 - the page says what it can
        return []
    helps, out = {}, []
    for line in text.splitlines():
        if line.startswith("# HELP "):
            name, _, words = line[7:].partition(" ")
            helps[name] = words
            continue
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE.match(line)
        if not m:
            continue
        labels = dict(_LABEL.findall(m["labels"] or ""))
        if labels.get("hop") != hop.name:
            continue
        labels.pop("hop", None)
        labels.pop("engine", None)
        try:
            value = float(m["value"])
        except ValueError:
            continue
        out.append({"name": m["name"], "labels": _said(hop, labels),
                    "value": value, "help": helps.get(m["name"], "")})
    return out


def progress(hop):
    """Where the hop stands: the verdict, each database's move and tail,
    and the metrics - what the dashboard and `/metrics` read."""
    from .. import planner, tailctl
    dbs = []
    for db in databases(hop):
        where = _root(hop) / db
        tail = tailctl.state(where) if where.is_dir() else None
        dbs.append({"db": db, "move": _said(hop, planner.read_progress(
            where)), "tail": _said(hop, tail)})
    return {"at": time.time(), "verdict": verdict(hop), "databases": dbs,
            "metrics": metrics(hop)}


# ---- what waits for someone ------------------------------------------------

def _lines_of(hop, detail, depth=0):
    """A request's `detail` as lines to read."""
    pad = "  " * depth
    if isinstance(detail, dict):
        out = []
        for k, v in detail.items():
            if isinstance(v, (dict, list)):
                out.append(f"{pad}{k}:")
                out += _lines_of(hop, v, depth + 1)
            else:
                out.append(f"{pad}{k}: {_say(hop, v)}")
        return out
    if isinstance(detail, list):
        out = []
        for v in detail:
            if isinstance(v, (dict, list)):
                out += _lines_of(hop, v, depth + 1)
            else:
                out.append(f"{pad}- {_say(hop, v)}")
        return out
    return [] if detail is None else [f"{pad}{_say(hop, detail)}"]


def approvals(hop, principal):
    """The requests standing on the hop: the step, what it will do, what
    approving lets happen, how many it needs, who has approved, whether
    the person asking may approve it - and why not."""
    from .. import approvals as gate
    from . import approve, roles
    try:
        standing = gate.standing(hop)
    except Exception:  # noqa: BLE001 - nothing readable stands
        return []
    counts = approve.accepts(hop)
    out = []
    for path, req, need, have in standing:
        mine = principal is not None and str(req.get("asked_by", "")
                                             ).lower() == principal.id.lower()
        given = [g.get("by") for g in approve.given(path)]
        may = principal is not None and roles.may(principal, hop, "approver")
        why = ("" if may and counts and not mine and principal.id not in have
               else "you asked for this step; someone else approves it"
               if mine else "you have approved it" if principal is not None
               and principal.id in have
               else "approvals given in the view do not count for this hop:"
                    " its access names no approver" if not counts
               else "approving needs the approver role on this hop")
        out.append({
            "id": path.name[:-len(".request.json")],
            "db": req.get("db") or "", "step": req.get("step"),
            "asked_by": req.get("asked_by"),
            "expires": req.get("expires"),
            "lets": gate.LETS.get(req.get("step"), ""),
            "detail": _lines_of(hop, req.get("detail")),
            "need": need, "have": sorted(have),
            "given_in_view": [g for g in given if g],
            "may": not why, "why_not": why})
    return out


def request_path(hop, request_id, db=""):
    """The standing request `request_id` of the hop, or None."""
    from .. import approvals as gate
    for path, req, _, _ in gate.standing(hop):
        if path.name[:-len(".request.json")] == request_id \
                and (req.get("db") or "") == (db or ""):
            return path
    return None


def offers(hop):
    """The changes on the source offered and not answered, with what each
    buys and what not taking it means; the answers standing; and what is
    the DBA's to make."""
    from .. import sourcechange
    try:
        got = sourcechange.standing(hop)
    except Exception:  # noqa: BLE001 - nothing readable stands
        got = {"offered": {}, "answers": {}, "handed": {}}
    return {"offered": [{"change": k, "said": _say(hop, v.get("said")),
                         "for": _say(hop, v.get("for")),
                         "lines": [_say(hop, x) for x in v.get("lines") or []],
                         "at": v.get("at")}
                        for k, v in sorted(got["offered"].items())],
            "answers": [{"change": k, "answer": a, "where": _say(hop, w)}
                        for k, (a, w) in sorted(got["answers"].items())],
            "handed": [{"change": k, "said": _say(hop, v.get("said")),
                        "why": _say(hop, v.get("why")),
                        "make": [_say(hop, x) for x in v.get("make") or []],
                        "undo": [_say(hop, x) for x in v.get("undo") or []]}
                       for k, v in sorted(got["handed"].items())]}


# ---- the evidence ------------------------------------------------------------

def evidence(hop):
    """The hop's evidence bundles, newest first, and its record's chain."""
    from .. import audit, export
    root = _root(hop) / export.DIR
    bundles = []
    if (root / "evidence.json").exists():
        bundles.append(_bundle(root, "", "the hop's whole history"))
    try:
        runs = sorted((p for p in root.iterdir() if p.is_dir()),
                      reverse=True)
    except OSError:
        runs = []
    for p in runs[:100]:
        bundles.append(_bundle(p, p.name, "one run"))
    n, chained, broken = audit.verify(_root(hop) / "changelog.jsonl")
    return {"bundles": bundles,
            "record": {"entries": n, "chained": chained, "whole": not broken,
                       "broken": broken}}


def _bundle(where, run, scope):
    from .. import export
    files = [f for f in export.FILES + ("evidence.json.sig",)
             if (where / f).is_file()]
    try:
        at = (where / "evidence.json").stat().st_mtime
    except OSError:
        at = None
    return {"run": run, "scope": scope, "files": files, "at": at}


def evidence_file(hop, run, name):
    """The bytes of one evidence file as written - encrypted where the hop
    keeps them so - or None where it is not one."""
    from .. import export
    if name not in export.FILES + ("evidence.json.sig",):
        return None
    if run and not re.match(r"^[\w.-]{1,80}$", run):
        return None
    root = _root(hop) / export.DIR
    path = (root / run / name) if run else root / name
    try:
        if not path.resolve().is_relative_to(root.resolve()):
            return None
        return Path(str(path)).read_bytes()
    except OSError:
        return None


def summary(hop, run=""):
    """The parts of a bundle a page shows, where it can be read here."""
    from .. import export
    if run and not re.match(r"^[\w.-]{1,80}$", run):
        return None
    # through the hop's own report path, which opens a file kept sealed
    # where this machine holds the key
    root = hop.report_dir() / export.DIR
    where = (root / run) if run else root
    try:
        doc = json.loads((where / "evidence.json").read_text())
    except Exception:  # noqa: BLE001 - sealed for someone else, or none
        return None
    keep = ("verdict", "checks", "method", "approvals", "repairs",
            "cutovers", "timings", "removed", "record", "generated",
            "scope", "session", "route")
    got = {k: doc.get(k) for k in keep}
    got["tables"] = (doc.get("tables") or [])[:200]
    return _said(hop, got)


# ---- the decision record, when asked ---------------------------------------

def decisions(hop, last=200):
    """The decision record's last entries: each work, the way taken and
    why, and every way passed over with why - in the words an operator
    reads."""
    from .. import decide
    out = []
    for d in reversed(decide.recorded(hop, last)):
        out.append({"at": d.get("at"), "run": d.get("session"),
                    "work": _say(hop, d.get("work")),
                    "way": _say(hop, d.get("way")),
                    "reason": _say(hop, d.get("reason")),
                    "passed": [{"way": _say(hop, p.get("way")),
                                "why": _say(hop, p.get("why"))}
                               for p in d.get("passed") or []],
                    "detail": [_say(hop, x) for x in decide.explained(
                        d.get("detail"))] if isinstance(
                        d.get("detail"), dict) else []})
    return out


# ---- the receivers ---------------------------------------------------------

def notifications(hop):
    """Who is told what: each receiver by its kind only (an address is its
    own password), the events and the lag said, and what each was last
    told."""
    from .. import addresses, notify
    conf = notify.settings(hop)
    to = []
    for r in conf["to"]:
        kind = r.split(":", 1)[0] if ":" in r else "address"
        to.append({"kind": kind if not addresses.is_reference(r)
                   else "kept by reference",
                   "written_out": not addresses.is_reference(
                       r.split(":", 1)[1] if r.startswith("pagerduty:")
                       else r)})
    told = _json_at(_root(hop) / "notified.json") or {}
    return {"receivers": to, "events": sorted(conf["events"]),
            "lag": conf["lag"],
            "told": {k: _say(hop, v) for k, v in told.items()
                     if not isinstance(v, (dict, list))}}


# ---- the configuration -----------------------------------------------------

def configuration(hop):
    """The hop as its file writes it, every secret hidden and the options
    named for a program left out."""
    import yaml

    from .. import config, hopkeys
    from ..redact import Redactor
    try:
        raw = (yaml.safe_load(Path(config.CONF).read_text()) or {}).get(
            "hops", {}).get(hop.name) or {}
    except (OSError, yaml.YAMLError):
        raw = {}
    raw = dict(raw)
    opts = dict(raw.get("options") or {})
    hidden = [k for k in opts if k in hopkeys.HOP_OPTIONS
              and hopkeys.HOP_OPTIONS[k].named_for_a_program]
    for k in hidden:
        opts.pop(k)
    if opts or "options" in raw:
        raw["options"] = opts
    shown = Redactor(hop, userinfo="password").value(raw)
    return {"hop": hop.name, "config": _said(hop, shown),
            "left_out": len(hidden),
            "goal": [_say(hop, x) for x in hopkeys.explain_goal(hop)]}


def validate(name):
    """What is wrong with the hop in its file, errors first, and what
    `doctor` would say of its keys."""
    import yaml

    from .. import config, hopkeys
    try:
        data = yaml.safe_load(Path(config.CONF).read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        return {"hop": name, "problems": [{"level": "error",
                                           "where": "the file",
                                           "text": str(e)[:300]}],
                "notes": []}
    raw = (data.get("hops") or {}).get(name)
    problems = [{"level": p.level, "where": p.where, "text": p.text}
                for p in hopkeys.check_hop(name, raw)]
    notes = []
    hops, _ = loaded()
    if name in hops and isinstance(raw, dict):
        notes = [{"key": k, "text": t}
                 for k, t in hopkeys.said_by_doctor(hops[name], raw)]
    return {"hop": name, "problems": problems, "notes": notes}
