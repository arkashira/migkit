"""The REST API, version 1: one table of routes (`ROUTES`) that the
server dispatches on and the OpenAPI document is written from - a route
cannot be served without being described, or described without being
served (backlog W2).

Every route is over the functions the command line uses: the hops as the
hops file says them (read and checked, never written - the API writes no
configuration and no secret), a run of any command (`runs`), where it
stands (`views.progress`, what `watch`, the dashboard and `/metrics`
read), the evidence, the approvals and the offers standing, the decision
record, the receivers.

Each route says the role it needs on the hop (`roles`): a hop the caller
may not see answers 404, as a hop that is not there; a role too low
answers 403 and says which it needs. Every call that changes anything is
written to the hop's record (and the view's own), with who made it and
whether through the API or a page.
"""
import time
from dataclasses import dataclass, field

from .. import __version__

VERSION = "v1"
BASE = f"/api/{VERSION}"


@dataclass(frozen=True)
class Route:
    method: str
    path: str                 #: werkzeug's form: /hops/<hop>/runs
    handler: str
    summary: str
    need: str = "viewer"      #: role on the hop; "signed-in"; "global admin"
    recorded: bool = False   #: written to the view's record
    body: dict = None         #: the JSON schema of the request's body
    answer: dict = field(default_factory=lambda: {"type": "object"})
    tag: str = "hops"
    raw: str = ""             #: a media type answered instead of JSON

    @property
    def rule(self):
        return BASE + self.path


_S = {"type": "string"}
_B = {"type": "boolean"}
_RUN_BODY = {"type": "object", "required": ["action"], "properties": {
    "action": {"type": "string", "enum": ["plan", "assess", "check", "move",
                                          "sync", "cutover", "rollback",
                                          "report"]},
    "db": _S, "table": _S, "mode": _S, "only": _S, "deep": _B, "kind": _S,
    "on_conflict": _S, "tag": _S, "state": _S, "apply": _B, "go": _B},
    "additionalProperties": False}
_RUN = {"type": "object", "properties": {
    "id": _S, "hop": _S, "action": _S, "argv": {"type": "array",
                                                "items": _S},
    "state": {"type": "string", "enum": ["queued", "running", "ended",
                                         "lost"]},
    "exit": {"type": ["integer", "null"]}, "by": _S, "run": _S}}

ROUTES = (
    Route("GET", "/me", "me", "who is signed in, and their role on each hop",
          need="signed-in", tag="people"),
    Route("GET", "/hops", "hops", "the hops the caller may see, each with its"
                                  " verdict, runs going on and what waits",
          need="signed-in"),
    Route("GET", "/hops/<hop>", "hop", "the hop as its file says it, every"
                                       " secret hidden, and what its goal"
                                       " does to each side"),
    Route("GET", "/hops/<hop>/validation", "validation",
          "what is wrong with the hop in its file, and what doctor would say"
          " of its keys"),
    Route("GET", "/hops/<hop>/outcome", "outcome",
          "what the hop does to each side and guarantees; each planned"
          " database's phases with their p50 and p90, and the outage held"
          " to the hop's limits"),
    Route("GET", "/hops/<hop>/progress", "progress",
          "where the hop stands: the verdict, each move's progress and"
          " tail, and the metrics /metrics serves for it"),
    Route("POST", "/hops/<hop>/plan", "plan",
          "a dry run of the move: what it would do and how long it would"
          " take - a run of its own, nothing written to either side",
          need="operator", recorded=True, answer=_RUN, body={
              "type": "object", "properties": {
                  "mode": _S, "db": _S, "table": _S},
              "additionalProperties": False}),
    Route("GET", "/hops/<hop>/runs", "runs", "the runs started from the view"
                                             " and the API, newest first",
          answer={"type": "array", "items": _RUN}),
    Route("POST", "/hops/<hop>/runs", "start",
          "start a run: the plan, assess, a check, the move, a sync, the"
          " cutover, a rollback or the evidence export - the command line's"
          " own command, with the hop's guards and approvals",
          need="operator", recorded=True, body=_RUN_BODY, answer=_RUN),
    Route("GET", "/hops/<hop>/runs/<run>", "run", "one run: its state, exit,"
                                                  " verdict and evidence",
          answer=_RUN),
    Route("GET", "/hops/<hop>/runs/<run>/output", "output",
          "a run's output from `offset`, secrets hidden"),
    Route("POST", "/hops/<hop>/runs/<run>/stop", "stop",
          "stop a run as ctrl-c at its terminal would", need="operator",
          recorded=True, answer=_RUN),
    Route("GET", "/hops/<hop>/approvals", "approvals",
          "the steps waiting for approval: what each will do, what"
          " approving lets happen, who has approved"),
    Route("POST", "/hops/<hop>/approvals/<request>", "approve",
          "approve a step as the person signed in - never one they asked"
          " for", need="approver", recorded=True,
          body={"type": "object", "properties": {"db": _S},
                "additionalProperties": False}),
    Route("GET", "/hops/<hop>/offers", "offers",
          "the changes on the source a faster way needs, offered and not"
          " answered: what each buys and what not taking it means"),
    Route("POST", "/hops/<hop>/offers/<change>", "answer",
          "answer an offered change on the source: yes, no or dba",
          need="operator", recorded=True, body={
              "type": "object", "required": ["answer"], "properties": {
                  "answer": {"type": "string",
                             "enum": ["yes", "no", "dba"]}},
              "additionalProperties": False}),
    Route("GET", "/hops/<hop>/evidence", "evidence",
          "the hop's evidence bundles and its record's chain"),
    Route("GET", "/hops/<hop>/evidence/<name>", "evidence_file",
          "one file of the hop's whole-history bundle, as written",
          raw="application/octet-stream", recorded=True),
    Route("GET", "/hops/<hop>/evidence/<run>/<name>", "evidence_run_file",
          "one file of a run's bundle, as written",
          raw="application/octet-stream", recorded=True),
    Route("GET", "/hops/<hop>/decisions", "decisions",
          "the decision record, on request: each work, the way taken and"
          " why, every way passed over and why"),
    Route("GET", "/hops/<hop>/notifications", "notifications",
          "who is told what: the receivers by kind, the events, the lag"),
    Route("POST", "/hops/<hop>/notifications/test", "notify_test",
          "send the hop's receivers a test message", need="admin",
          recorded=True),
    Route("POST", "/hops/<hop>/tails/<db>/hold", "hold",
          "hold a change tail between batches, its position saved",
          need="operator", recorded=True),
    Route("POST", "/hops/<hop>/tails/<db>/resume", "resume",
          "let a held change tail go on", need="operator", recorded=True),
    Route("POST", "/hops/<hop>/tails/<db>/stop", "tail_stop",
          "end a change tail at its next batch, its position saved",
          need="operator", recorded=True),
    Route("GET", "/tokens", "tokens", "the caller's API tokens",
          need="signed-in", tag="people"),
    Route("POST", "/tokens", "token_new",
          "a new API token for the caller, shown once", need="signed-in",
          recorded=True, tag="people", body={
              "type": "object", "required": ["name"], "properties": {
                  "name": _S, "days": {"type": "number"}},
              "additionalProperties": False}),
    Route("DELETE", "/tokens/<token>", "token_revoke",
          "end one of the caller's API tokens", need="signed-in",
          recorded=True, tag="people"),
    Route("GET", "/admin/people", "people",
          "the people and groups the identity provider provisioned, and the"
          " local account", need="global admin", tag="people"),
    Route("GET", "/admin/sessions", "sessions", "every session open",
          need="global admin", tag="people"),
    Route("GET", "/admin/record", "record", "the view's own record: sign-ins,"
                                            " provisioning, tokens, refusals",
          need="global admin", tag="people"),
    Route("GET", "/openapi.json", "openapi", "this document",
          need="anyone", tag="api"),
)


class Answer(Exception):
    """An answer other than 200, with its words."""

    def __init__(self, status, detail):
        super().__init__(detail)
        self.status, self.detail = int(status), detail


@dataclass
class Ctx:
    """One call: who makes it, how (`api` with a token, `web` from a
    page), the hop and their role on it, and the body."""
    app: object
    principal: object
    through: str
    body: dict
    args: dict
    hop: object = None
    role: str = None
    prefs: dict = field(default_factory=dict)
    #: the caller came with an API token, not a signed-in page
    token: bool = False

    def record(self, entry):
        """The action in the hop's record, with who and how."""
        from .. import audit
        audit.append(self.hop.report_dir() / "changelog.jsonl",
                     {**entry, "by": self.principal.id,
                      "issuer": self.principal.issuer,
                      "through": self.through})


# ---- the handlers -----------------------------------------------------------

def me(ctx):
    from . import roles, views
    return {"id": ctx.principal.id, "name": ctx.principal.name,
            "via": ctx.principal.via, "groups": list(ctx.principal.groups),
            "admin": roles.is_admin(ctx.principal),
            "roles": {n: r for n, _, r in views.visible(ctx.principal)},
            "explain": bool(ctx.prefs.get("explain",
                                          ctx.app.settings.explain))}


def hops(ctx):
    from . import views
    got, error = views.loaded()
    out = {"hops": [views.card(n, h, r)
                    for n, h, r in views.visible(ctx.principal)]}
    if error:
        out["error"] = error
    return out


def hop(ctx):
    from . import views
    return {**views.configuration(ctx.hop), "role": ctx.role}


def validation(ctx):
    from . import views
    return views.validate(ctx.hop.name)


def outcome(ctx):
    from . import views
    return views.outcome(ctx.hop)


def progress(ctx):
    from . import views
    return views.progress(ctx.hop)


def _start(ctx, action, params):
    from . import runs
    try:
        rec = runs.start(ctx.hop, action, params, ctx.principal,
                         ctx.through)
    except runs.Refused as e:
        raise Answer(e.status, str(e))
    return 202, rec


def plan(ctx):
    return _start(ctx, "plan", ctx.body)


def runs_(ctx):
    from . import runs
    return runs.listing(ctx.hop)


def start(ctx):
    body = dict(ctx.body)
    action = body.pop("action", None)
    if not action:
        raise Answer(400, "which run: action is one of plan, assess, check,"
                          " move, sync, cutover, rollback, report")
    return _start(ctx, str(action), body)


def _run(ctx, rid):
    from . import runs
    rec = runs.read(ctx.hop, rid)
    if rec is None:
        raise Answer(404, "no such run")
    rec["verdict"] = runs.verdict_of(ctx.hop, rec)
    rec["evidence"] = runs.evidence_of(ctx.hop, rec)
    return rec


def run(ctx, rid):
    return _run(ctx, rid)


def output(ctx, rid):
    from . import runs
    _run(ctx, rid)
    try:
        offset = max(0, int(ctx.args.get("offset") or 0))
    except ValueError:
        raise Answer(400, "offset is a number of bytes")
    text, end = runs.output(ctx.hop, rid, offset)
    rec = runs.read(ctx.hop, rid)
    return {"text": text, "offset": end, "state": rec["state"],
            "exit": rec.get("exit")}


def stop(ctx, rid):
    from . import runs
    try:
        got = runs.cancel(ctx.hop, rid, ctx.principal, ctx.through)
    except runs.Refused as e:
        raise Answer(e.status, str(e))
    if got is None:
        raise Answer(404, "no such run")
    return got


def approvals(ctx):
    from . import views
    return views.approvals(ctx.hop, ctx.principal)


def approve(ctx, request):
    from . import approve as view
    from . import views
    db = str(ctx.body.get("db") or "")
    path = views.request_path(ctx.hop, request, db)
    if path is None:
        raise Answer(404, "no such request standing - it expired, or the"
                          " step went ahead")
    if not view.accepts(ctx.hop):
        raise Answer(409, "approvals given in the view do not count for this"
                          " hop: its access names no approver")
    try:
        view.give(ctx.hop, path, ctx.principal, ctx.role,
                  ctx.app.settings.state)
    except ValueError as e:
        raise Answer(403, str(e))
    ctx.record({"op": "approval-given", "request": request, "db": db})
    got = [a for a in views.approvals(ctx.hop, ctx.principal)
           if a["id"] == request and a["db"] == db]
    return got[0] if got else {"id": request, "db": db}


def offers(ctx):
    from . import views
    return views.offers(ctx.hop)


def answer(ctx, change):
    from .. import sourcechange
    word = ctx.body.get("answer")
    try:
        got = sourcechange.answer_from_view(ctx.hop, change, word,
                                            ctx.principal.id)
    except ValueError as e:
        raise Answer(409, str(e))
    return {"change": change, "answer": got,
            "then": "yes: the next run makes it, behind the hop's approvals"
                    " (source-setting), and takes it off again after; no:"
                    " the source is left as it is and the next way runs;"
                    " dba: it goes to the DBA's script"}


def evidence(ctx):
    from . import views
    return views.evidence(ctx.hop)


def _file(ctx, run, name):
    from . import views
    got = views.evidence_file(ctx.hop, run, name)
    if got is None:
        raise Answer(404, "no such file")
    ctx.record({"op": "evidence-download", "run": run or "", "file": name})
    kind = {"json": "application/json", "html": "text/html",
            "md": "text/markdown", "csv": "text/csv"}.get(
        name.rsplit(".", 1)[-1], "application/octet-stream")
    return 200, got, name, kind


def evidence_file(ctx, name):
    return _file(ctx, "", name)


def evidence_run_file(ctx, rid, name):
    return _file(ctx, rid, name)


def decisions(ctx):
    from . import views
    return views.decisions(ctx.hop)


def notifications(ctx):
    from . import views
    return views.notifications(ctx.hop)


def notify_test(ctx):
    from .. import notify
    said = []
    sent = notify.send(ctx.hop, f"migkit: a test message for {ctx.hop.name},"
                                f" sent from the web view by"
                                f" {ctx.principal.id}",
                       {"event": "test", "hop": ctx.hop.name}, False,
                       f"migkit-{ctx.hop.name}-test", said.append,
                       severity="info")
    ctx.record({"op": "notify-test", "sent": sent})
    from . import views
    return {"sent": sent, "said": [views._say(ctx.hop, s) for s in said]}


def _tail(ctx, db):
    from . import views
    if db not in views.databases(ctx.hop):
        raise Answer(404, "no such database on this hop")
    where = ctx.hop.report_dir(db)
    return where


def hold(ctx, db):
    from .. import tailctl
    where = _tail(ctx, db)
    if not tailctl.alive(where):
        raise Answer(409, "no change tail runs for this database")
    held = tailctl.pause(where, HOLD_WAIT)
    ctx.record({"op": "tail-hold", "db": db, "held": held})
    if not held:
        raise Answer(504, f"the tail did not hold within {HOLD_WAIT:g}"
                          " seconds; it goes on, and the request was"
                          " withdrawn")
    return {"db": db, "held": True}


#: seconds a hold is waited for before the request is withdrawn
HOLD_WAIT = 20.0


def resume(ctx, db):
    from .. import tailctl
    where = _tail(ctx, db)
    tailctl.resume(where)
    ctx.record({"op": "tail-resume", "db": db})
    return {"db": db, "held": False}


def tail_stop(ctx, db):
    from .. import tailctl
    where = _tail(ctx, db)
    if not tailctl.alive(where):
        raise Answer(409, "no change tail runs for this database")
    tailctl.stop(where)
    ctx.record({"op": "tail-stop", "db": db})
    return {"db": db, "asked": "the tail ends at its next batch, its"
                               " position saved"}


def tokens(ctx):
    return ctx.app.store.tokens_of(ctx.principal.id)


def token_new(ctx):
    if ctx.token:
        raise Answer(403, "a token makes no tokens; sign in to make one")
    name = str(ctx.body.get("name") or "").strip()
    if not name:
        raise Answer(400, "a name for the token, to tell it from others")
    most = ctx.app.settings.token_days
    try:
        days = min(float(ctx.body.get("days") or most), most)
    except (TypeError, ValueError):
        raise Answer(400, "days is a number")
    if days <= 0:
        raise Answer(400, "days is more than 0")
    tid, value = ctx.app.store.new_token(ctx.principal.as_dict(), name, days)
    ctx.app.record({"op": "token-made", "by": ctx.principal.id, "token": tid,
                    "name": name, "days": days})
    return 201, {"id": tid, "token": value, "name": name,
                 "expires": time.time() + days * 86400,
                 "shown": "once - it is kept only as its digest"}


def token_revoke(ctx, token):
    if not ctx.app.store.revoke_token(ctx.principal.id, token):
        raise Answer(404, "no such token of yours")
    ctx.app.record({"op": "token-ended", "by": ctx.principal.id,
                    "token": token})
    return {"id": token, "ended": True}


def people(ctx):
    store = ctx.app.store
    users = store.rows("select id, user_name, display_name, email, active,"
                       " modified from users order by user_name")
    groups = store.rows("select g.id, g.display_name, count(m.user_id) as"
                        " members from groups g left join members m on"
                        " m.group_id = g.id group by g.id order by 2")
    local = store.local_account()
    return {"provisioned": ctx.app.settings.scim, "users": users,
            "groups": groups, "local": local[0] if local else None}


def sessions(ctx):
    return ctx.app.store.sessions()


def record(ctx):
    from .. import audit
    from ..durable import read_lines
    path = ctx.app.record_path
    n, chained, broken = audit.verify(path)
    try:
        entries = read_lines(path, "cache")[-500:]
    except SystemExit:
        entries = []
    return {"entries": list(reversed(entries)),
            "chain": {"entries": n, "chained": chained, "whole": not broken,
                      "broken": broken}}


def openapi(ctx=None):
    return document()


HANDLERS = {"me": me, "hops": hops, "hop": hop, "validation": validation,
            "outcome": outcome, "progress": progress, "plan": plan,
            "runs": runs_, "start": start, "run": run, "output": output,
            "stop": stop, "approvals": approvals, "approve": approve,
            "offers": offers, "answer": answer, "evidence": evidence,
            "evidence_file": evidence_file,
            "evidence_run_file": evidence_run_file,
            "decisions": decisions, "notifications": notifications,
            "notify_test": notify_test, "hold": hold, "resume": resume,
            "tail_stop": tail_stop, "tokens": tokens,
            "token_new": token_new, "token_revoke": token_revoke,
            "people": people, "sessions": sessions, "record": record,
            "openapi": openapi}

#: the route's path parameter each handler takes it by
PARAMS = {"run": "rid"}


# ---- the document ------------------------------------------------------------

def _openapi_path(path):
    import re
    return re.sub(r"<(?:\w+:)?(\w+)>", r"{\1}", BASE + path)


def document():
    """The OpenAPI 3.1 document of `ROUTES`."""
    import re
    paths = {}
    for r in ROUTES:
        op = {"summary": r.summary, "operationId": r.handler,
              "tags": [r.tag],
              "responses": {"200": {"description": "the answer",
                                    "content": {r.raw or "application/json":
                                                {"schema": r.answer}}},
                            "401": {"description": "not signed in"},
                            "403": {"description": "a role too low, or not"
                                                   " from this view's page"},
                            "404": {"description": "not there, or not the"
                                                   " caller's to see"}}}
        if r.need not in ("anyone",):
            op["security"] = [{"token": []}, {"session": [], "csrf": []}]
        else:
            op["security"] = []
        op["x-migkit-role"] = r.need
        params = [{"name": n, "in": "path", "required": True,
                   "schema": {"type": "string"}}
                  for n in re.findall(r"<(?:\w+:)?(\w+)>", r.path)]
        if r.handler == "output":
            params.append({"name": "offset", "in": "query",
                           "schema": {"type": "integer", "minimum": 0}})
        if params:
            op["parameters"] = params
        if r.body is not None:
            op["requestBody"] = {"required": True, "content": {
                "application/json": {"schema": r.body}}}
        if r.handler in ("start", "plan"):
            op["responses"]["202"] = op["responses"].pop("200")
            op["responses"]["409"] = {"description": "a run that writes is"
                                                     " going on"}
        paths.setdefault(_openapi_path(r.path), {})[r.method.lower()] = op
    return {"openapi": "3.1.0",
            "info": {"title": "migkit", "version": f"{VERSION}"
                                                   f" ({__version__})",
                     "description": __doc__.split("\n\n")[0].replace(
                         "\n", " "),
                     "license": {"name": "AGPL-3.0-or-later",
                                 "identifier": "AGPL-3.0-or-later"}},
            "servers": [{"url": "/"}],
            "components": {"securitySchemes": {
                "token": {"type": "http", "scheme": "bearer",
                          "description": "an API token made on the account"
                                         " page (mkt_...)"},
                "session": {"type": "apiKey", "in": "cookie",
                            "name": "migkit_session",
                            "description": "a signed-in page's session"},
                "csrf": {"type": "apiKey", "in": "header",
                         "name": "X-CSRF-Token",
                         "description": "the page's token, on every call"
                                        " that changes something"}}},
            "paths": paths}
