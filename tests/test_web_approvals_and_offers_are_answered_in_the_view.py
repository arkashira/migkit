"""The steps a hop holds back for approval, and the changes on the source
a faster way offers, are answered in the web view by the person whose
role it is - recorded with who answered (backlog W2).

Approvals: the request is the one `approvals.require` makes; an approver
signed in approves it, and `require` - the one place approvals are
counted - counts it. It does not count for the one who asked, for
someone without the approver's role, for a request made again since,
once the file takes the role away, or once the approval's file is
changed by hand; and a hop whose access names no approver counts none.

Offers: the offer the plan makes and cannot ask at a terminal stands for
the view, with what it buys and what not taking it means; an operator's
answer is recorded as a terminal's is, by name, and the next plan reads
it; a viewer cannot answer, and the hop's own file wins over the view.
"""
import json

import pytest

from migkit import approvals, sourcechange
from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import HOPS, browser, call, get, place, signed_in, \
    view


def _hop(name="h"):
    from migkit.web import views
    views._LOADED["key"] = None
    hops, error = views.loaded()
    assert not error, error
    return hops[name]


def _ask(monkeypatch, who="ann@example.com", detail=None):
    """The cutover's step asking, as a run started by `who` asks."""
    monkeypatch.setenv("MIGKIT_ACTOR", who)
    try:
        return approvals.require(_hop(), "cutover", "app",
                                 detail=detail or {"freeze": "10 s"})
    finally:
        monkeypatch.delenv("MIGKIT_ACTOR")


def _as(web, who):
    c = browser(web)
    return c, signed_in(web, c, {"id": who, "via": "oidc",
                                 "issuer": "oidc:stand-in"})


def test_an_approver_approves_in_the_view_and_the_step_goes_ahead(
        web, monkeypatch):
    with pytest.raises(SystemExit) as stop:
        _ask(monkeypatch)
    # what to do is said, the view included
    assert "approves it in the web view" in str(stop.value)
    c, csrf = _as(web, "bob@example.com")
    standing = get(c, "/api/v1/hops/h/approvals").get_json()
    assert len(standing) == 1
    a = standing[0]
    assert (a["id"], a["db"], a["step"]) == ("cutover-app", "app", "cutover")
    assert a["asked_by"] == "ann@example.com" and a["may"] is True
    assert a["need"] == 1 and a["have"] == []
    assert "application moved to the target" in a["lets"]
    assert "freeze: 10 s" in a["detail"]
    r = call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
             {"db": "app"})
    assert r.status_code == 200, r.data
    assert r.get_json()["have"] == ["bob@example.com"]
    # the run asked again goes ahead, approved by bob
    assert _ask(monkeypatch) == ["bob@example.com"]
    record = [json.loads(line) for line in (
        _hop().report_dir() / "changelog.jsonl").read_text().splitlines()]
    given = [e for e in record if e["op"] == "approval-given"]
    assert given and given[-1]["by"] == "bob@example.com"
    assert given[-1]["through"] == "web"
    assert [e for e in record if e["op"] == "approved"][-1]["approvers"] \
        == ["bob@example.com"]


def test_nobody_approves_their_own_step(web, monkeypatch):
    with pytest.raises(SystemExit):
        _ask(monkeypatch, who="ann@example.com")
    c, csrf = _as(web, "ann@example.com")
    a = get(c, "/api/v1/hops/h/approvals").get_json()[0]
    assert a["may"] is False and "you asked for this step" in a["why_not"]
    r = call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
             {"db": "app"})
    assert r.status_code == 403
    assert "does not approve their own" in r.get_json()["error"]
    with pytest.raises(SystemExit):
        _ask(monkeypatch, who="ann@example.com")


def test_a_viewer_cannot_approve(web, monkeypatch):
    with pytest.raises(SystemExit):
        _ask(monkeypatch)
    c, csrf = _as(web, "carol@example.com")
    a = get(c, "/api/v1/hops/h/approvals").get_json()[0]
    assert a["may"] is False and "approver role" in a["why_not"]
    r = call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
             {"db": "app"})
    assert r.status_code == 403
    with pytest.raises(SystemExit):
        _ask(monkeypatch)


def test_an_approval_counts_for_its_request_only(web, monkeypatch):
    with pytest.raises(SystemExit):
        _ask(monkeypatch, detail={"freeze": "10 s"})
    c, csrf = _as(web, "bob@example.com")
    assert call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
                {"db": "app"}).status_code == 200
    # the plan changed: the request is made again, and the approval of
    # the old one counts for nothing
    with pytest.raises(SystemExit) as stop:
        _ask(monkeypatch, detail={"freeze": "90 s"})
    assert "has 0" in str(stop.value)


def test_a_changed_approval_counts_for_nothing(web, monkeypatch):
    with pytest.raises(SystemExit):
        _ask(monkeypatch)
    c, csrf = _as(web, "bob@example.com")
    assert call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
                {"db": "app"}).status_code == 200
    where = _hop().report_dir("app") / "approvals"
    (given,) = list(where.glob("cutover-app.*.view.json"))
    body = json.loads(given.read_text())
    # the signature holds while nothing is changed
    assert _ask(monkeypatch) == ["bob@example.com"]
    body["by"] = "root@example.com"
    given.write_text(json.dumps(body))
    with pytest.raises(SystemExit):
        _ask(monkeypatch)
    # and a file written by hand, never signed, counts for nothing either
    body["by"], body["signature"] = "bob@example.com", ""
    given.write_text(json.dumps(body))
    with pytest.raises(SystemExit):
        _ask(monkeypatch)


def test_a_role_taken_away_takes_the_approval_with_it(web, monkeypatch,
                                                      tmp_path):
    with pytest.raises(SystemExit):
        _ask(monkeypatch)
    c, csrf = _as(web, "bob@example.com")
    assert call(c, "POST", "/api/v1/hops/h/approvals/cutover-app", csrf,
                {"db": "app"}).status_code == 200
    assert _ask(monkeypatch) == ["bob@example.com"]
    from migkit import config
    config.CONF.write_text(HOPS.replace("approver: [bob@example.com, "
                                        "ann@example.com]",
                                        "approver: [ann@example.com]"))
    with pytest.raises(SystemExit):
        _ask(monkeypatch)


def test_a_hop_that_names_no_approver_counts_none_from_the_view(
        tmp_path, monkeypatch):
    hops = ("hops:\n  h:\n    engine: postgres\n"
            "    source: {host: 10.0.0.1, port: 5432, user: app}\n"
            "    target: {host: 10.0.0.2, port: 5432, user: app}\n"
            "    options:\n      approvals: {steps: [cutover]}\n"
            "      access: {operator: [ann@example.com]}\n")
    place(tmp_path, monkeypatch, hops)
    app = view(tmp_path)
    try:
        with pytest.raises(SystemExit) as stop:
            approvals.require(_hop(), "cutover", "app")
        # as before the view: the list of who may approve is missing
        assert "is not there" in str(stop.value)
        assert "web view" in str(stop.value)
    finally:
        app.store.close()


# ---- the offer of a change on the source ----------------------------------

SEED = """\
access:
  groups:
    all:
      hops: ["*"]
      viewer: ["*@example.com"]
      operator: [ann@example.com]
hops:
  seed:
    engine: mysql
    source: {host: 10.0.0.1, port: 3306, user: app, password: CHANGE_ME}
    target: {host: 10.0.0.2, port: 3306, user: app, password: CHANGE_ME}
    options:
      physical: {source: "10.0.0.1:3306", restart: "ssh 10.0.0.2 restart"}
"""


@pytest.fixture
def seed(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch, SEED)
    app = view(tmp_path)
    yield app
    app.store.close()


def _offer(hop):
    """The plan's offer of the clone plugin, made where no terminal is
    there to ask it - as a dry run, or a run from the view, makes it."""
    from migkit import decide
    from tests.test_a_source_change_is_offered_never_assumed import (
        _clone, _facts)
    facts = _facts(hop)
    sourcechange.put_answers(hop, facts, [_clone()])
    off = decide.offered("the seed", [_clone()], facts)
    if off is None:
        return None, facts
    said = []
    got = sourcechange.settle(hop, off, facts, sourcechange.Saving(
        faster=600, slower=3600, how="measured on this hop",
        instead="the logical copy"), False, None, said.append)
    assert got is None
    return said, facts


def test_an_offer_stands_with_its_consequences_and_an_operator_answers(
        seed):
    hop = _hop("seed")
    said, _ = _offer(hop)
    assert said and "offered, never assumed" in said[0]
    c, csrf = _as(seed, "ann@example.com")
    got = get(c, "/api/v1/hops/seed/offers").get_json()
    assert [o["change"] for o in got["offered"]] == ["clone_plugin"]
    lines = "\n".join(got["offered"][0]["lines"])
    # what it buys and what not taking it means, as the plan said it
    assert "it saves about 50 minutes" in lines
    assert "without it: the logical copy, about 1.0 hours" in lines
    r = call(c, "POST", "/api/v1/hops/seed/offers/clone_plugin", csrf,
             {"answer": "yes"})
    assert r.status_code == 200 and r.get_json()["answer"] == "yes"
    answer, where = sourcechange.answer_of(hop, "clone_plugin")
    assert answer == "yes"
    assert where.startswith("answered in the web view by ann@example.com")
    got = get(c, "/api/v1/hops/seed/offers").get_json()
    assert got["offered"] == []
    # the next plan reads the answer, and offers nothing again
    assert _offer(hop)[0] is None
    record = [json.loads(line) for line in (
        hop.report_dir() / "changelog.jsonl").read_text().splitlines()]
    answered = [e for e in record if e["op"] == "source-change-answer"]
    assert answered[-1]["by"] == "ann@example.com"
    assert answered[-1]["through"] == "web"


def test_a_viewer_does_not_answer_an_offer(seed):
    hop = _hop("seed")
    _offer(hop)
    c, csrf = _as(seed, "carol@example.com")
    r = call(c, "POST", "/api/v1/hops/seed/offers/clone_plugin", csrf,
             {"answer": "yes"})
    assert r.status_code == 403
    assert sourcechange.answer_of(hop, "clone_plugin")[0] is None


def test_only_an_offer_standing_and_a_real_answer_are_taken(seed):
    hop = _hop("seed")
    _offer(hop)
    c, csrf = _as(seed, "ann@example.com")
    r = call(c, "POST", "/api/v1/hops/seed/offers/clone_plugin", csrf,
             {"answer": "maybe"})
    assert r.status_code == 409 and "not yes, no or dba" in \
        r.get_json()["error"]
    r = call(c, "POST", "/api/v1/hops/seed/offers/wal_level", csrf,
             {"answer": "yes"})
    assert r.status_code == 409 and "no change" in r.get_json()["error"]
    assert sourcechange.answer_of(hop, "clone_plugin")[0] is None


def test_the_hops_own_answer_wins_over_the_view(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch, SEED)
    app = view(tmp_path)
    try:
        hop = _hop("seed")
        _offer(hop)
        from migkit import config
        config.CONF.write_text(SEED.replace(
            'restart: "ssh 10.0.0.2 restart"}',
            'restart: "ssh 10.0.0.2 restart"}\n      source_changes:'
            ' {clone_plugin: no}'))
        hop = _hop("seed")
        c, csrf = _as(app, "ann@example.com")
        r = call(c, "POST", "/api/v1/hops/seed/offers/clone_plugin", csrf,
                 {"answer": "yes"})
        assert r.status_code == 409
        assert sourcechange.answer_of(hop, "clone_plugin")[0] == "no"
    finally:
        app.store.close()
