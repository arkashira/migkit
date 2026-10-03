"""The web view shows what the console shows - what the hop does to each
side, the time and the outage predicted with their spread, where it
stands, the evidence - and never the name of a program migkit drives;
the decision record only when asked (backlog W2, the owner's rule).

Rendered without a browser: the pages and the API as the view serves
them, over reports laid down the way migkit's own commands lay them
(`model.Model.save`, `progress.json`, `verdict.json`, the decision record,
an evidence bundle). Each page is scanned for every program name the
report test knows; the scan is proved able to find one.
"""
import json
import time

import pytest

from tests.test_the_report_does_not_name_its_tools import TOOLS
from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import browser, call, get, signed_in

#: a decision whose words name a program, as a program's own message can
WAY = "the copy of app.public.orders"


@pytest.fixture
def laid(web):
    """Reports for hop h as a plan, a move, a check and an export left
    them."""
    from migkit import export, model, planner
    from migkit.config import REPORTS
    from migkit.web import views
    hop = views.loaded()[0]["h"]
    db = REPORTS / "h" / "app"
    db.mkdir(parents=True, exist_ok=True)
    model.Model(planned=[
        planner.Phase("copy", 120.0, 180.0, "the source's disk",
                      "measured on this hop"),
        planner.Phase("check", 30.0, 45.0, "", "estimated"),
        planner.Phase("cutover", 4.0, 9.0, "", "")]).save(db)
    (db / "progress.json").write_text(json.dumps({
        "rows_done": 1500, "rows_total": 4000, "eta_copy": 300,
        "done": False, "binding": "the network", "at": time.time()}))
    (REPORTS / "h" / "verdict.json").write_text(json.dumps({
        "status": "different", "generated": "2026-10-01T06:00:00+0700",
        "has_differences": True, "totals": {"ok": 3, "diff": 1},
        "session": "20261001T060000-abcdef",
        "findings": [{"check": "counts", "scope": "app.public.orders",
                      "status": "diff",
                      "detail": "rows-missing by=3, read by pgcopydb"}]}))
    with open(REPORTS / "h" / "decisions.jsonl", "a") as f:
        f.write(json.dumps({
            "session": "20261001T060000-abcdef", "at": "2026-10-01 06:00",
            "work": WAY, "way": "pgcopydb in parallel",
            "reason": "measured 3x faster than mydumper here",
            "passed": [{"way": "the table copier",
                        "why": "slower by 2 minutes"}]}) + "\n")
    export.write(hop)
    return hop


def _leaks(text):
    low = text.lower()
    return sorted({t for t in TOOLS if t in low})


def _viewer(web):
    c = browser(web)
    csrf = signed_in(web, c, {"id": "carol@example.com", "via": "oidc"})
    return c, csrf


def test_the_hop_page_shows_each_sides_outcome_and_the_spread(laid, web):
    c, _ = _viewer(web)
    page = get(c, "/hops/h").data.decode()
    # what the goal does to each side, in the console's words
    assert "on the source:" in page and "on the target:" in page
    assert "it guarantees:" in page
    # the phases with their p50 and p90, and the outage held to them
    assert "<td>copy</td>" in page and "2 minutes" in page
    assert "3 minutes" in page and "the source&#39;s disk" in page
    assert "The outage at the cutover:</b> about 4 seconds, at most" \
        " 9 seconds" in page
    # where it stands, as the reports say
    assert "1,500 of 4,000 rows" in page and "about 5 minutes left" in page
    assert "different" in page and "rows-missing by=3" in page


def test_no_page_and_no_answer_names_a_program(laid, web):
    c, csrf = _viewer(web)
    web.store.set_prefs(c.get_cookie(web.cookie, domain="127.0.0.1").value,
                        {"explain": True})
    seen = []
    for path in ("/", "/hops/h", "/hops/h/decisions", "/hops/h/evidence/all",
                 "/account", "/api", "/api/v1/hops", "/api/v1/hops/h",
                 "/api/v1/hops/h/outcome", "/api/v1/hops/h/progress",
                 "/api/v1/hops/h/decisions", "/api/v1/hops/h/evidence",
                 "/api/v1/hops/h/notifications", "/api/v1/openapi.json"):
        r = get(c, path)
        assert r.status_code == 200, path
        text = r.data.decode()
        assert not _leaks(text), (path, _leaks(text))
        seen.append(text)
    # the decision was there to be shown, in migkit's words
    assert any("the copy in parallel" in t for t in seen)
    # the scan finds a program where one is
    assert _leaks("read by pgcopydb") == ["pgcopydb"]


def test_the_decision_record_is_shown_only_when_asked(laid, web):
    c, csrf = _viewer(web)
    assert WAY not in get(c, "/hops/h").data.decode()
    r = c.post("/account/prefs", base_url="http://127.0.0.1:8899",
               data={"csrf": csrf, "explain": "1", "next": "/hops/h"},
               headers={"Origin": "http://127.0.0.1:8899"})
    assert r.status_code == 303 and r.headers["Location"] == "/hops/h"
    page = get(c, "/hops/h").data.decode()
    assert WAY in page and "slower by 2 minutes" in page
    # and asked for directly, it is there for anyone who may see the hop
    other, _ = _viewer(web)
    assert WAY in get(other, "/hops/h/decisions").data.decode()
    got = get(other, "/api/v1/hops/h/decisions").get_json()
    assert got[0]["work"] == WAY and got[0]["passed"][0]["why"] == \
        "slower by 2 minutes"


def test_the_configuration_shown_hides_its_secrets(laid, web):
    c, _ = _viewer(web)
    got = get(c, "/api/v1/hops/h").get_json()
    shown = json.dumps(got)
    assert "s3cret-pass-word" not in shown
    assert got["config"]["source"]["password"] == "***"
    assert got["config"]["source"]["host"] == "10.0.0.1"
    page = get(c, "/hops/h").data.decode()
    assert "s3cret-pass-word" not in page


def test_the_evidence_is_downloaded_as_written_and_the_download_recorded(
        laid, web):
    c, _ = _viewer(web)
    listed = get(c, "/api/v1/hops/h/evidence").get_json()
    assert listed["record"]["whole"] is True
    (bundle,) = [b for b in listed["bundles"] if b["run"] == ""]
    assert set(bundle["files"]) >= {"evidence.json", "evidence.html",
                                    "evidence.md", "tables.csv"}
    r = get(c, "/api/v1/hops/h/evidence/evidence.json")
    assert r.status_code == 200
    from migkit.config import REPORTS
    assert r.data == (REPORTS / "h" / "evidence" / "evidence.json"
                      ).read_bytes()
    assert r.headers["Content-Disposition"].startswith("attachment")
    assert r.headers["Content-Security-Policy"].startswith("sandbox")
    # nothing outside the bundle is served
    assert get(c, "/api/v1/hops/h/evidence/changelog.jsonl"
               ).status_code == 404
    assert get(c, "/api/v1/hops/h/evidence/..%2Fverdict.json"
               ).status_code == 404
    record = [json.loads(line) for line in (
        REPORTS / "h" / "changelog.jsonl").read_text().splitlines()]
    assert record[-1]["op"] == "evidence-download"
    assert record[-1]["by"] == "carol@example.com"


def test_a_run_page_shows_its_output_as_the_api_reads_it(laid, web,
                                                         monkeypatch):
    from migkit.web import runs, views
    from migkit.web.identity import Principal

    class P:
        pid = 999_999

        def wait(self):
            return 0
    monkeypatch.setattr(runs.subprocess, "Popen", lambda *a, **k: P())
    hop, _ = views.one(Principal(id="ann@example.com", via="oidc"), "h")
    rid = runs.start(hop, "check", {}, Principal(id="ann@example.com",
                                                 via="oidc"), "web")["id"]
    (hop.report_dir() / runs.DIR / f"{rid}.log").write_text(
        "mydumper: 3 tables\nchecked: same\n")
    c, _ = _viewer(web)
    page = get(c, f"/hops/h/runs/{rid}").data.decode()
    assert "checked: same" in page and "3 tables" in page
    assert not _leaks(page)
    assert "migkit check h" in page


def test_the_api_is_described_by_its_own_routes():
    """The OpenAPI document is written from the table the server
    dispatches on: every route in it, each with the role it needs."""
    from migkit.web import api
    doc = api.document()
    assert doc["openapi"].startswith("3.1")
    ops = {(m.upper(), p) for p, item in doc["paths"].items()
           for m in item}
    want = {(r.method, api._openapi_path(r.path)) for r in api.ROUTES}
    assert ops == want and len(want) == len(api.ROUTES)
    for r in api.ROUTES:
        op = doc["paths"][api._openapi_path(r.path)][r.method.lower()]
        assert op["x-migkit-role"] == r.need
        assert r.handler in api.HANDLERS
    run = doc["paths"]["/api/v1/hops/{hop}/runs"]["post"]
    assert set(run["requestBody"]["content"]["application/json"]["schema"]
               ["properties"]["action"]["enum"]) == set(
        __import__("migkit.web.runs", fromlist=["ACTIONS"]).ACTIONS)


def test_a_page_signs_in_first(web):
    c = browser(web)
    r = get(c, "/hops/h")
    assert r.status_code == 303
    assert r.headers["Location"] == "/signin?next=%2Fhops%2Fh"
    assert get(c, "/api/v1/hops").status_code == 401
    assert get(c, "/api/v1/openapi.json").status_code == 200


def test_starting_a_run_from_the_page_needs_its_role(laid, web,
                                                      monkeypatch):
    from migkit.web import runs
    started = []
    monkeypatch.setattr(runs.subprocess, "Popen",
                        lambda *a, **k: started.append(a) or type(
                            "P", (), {"pid": 1, "wait": lambda self: 0})())
    c = browser(web)
    csrf = signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    page = get(c, "/hops/h").data.decode()
    assert 'data-api="/api/v1/hops/h/runs"' in page
    r = call(c, "POST", "/api/v1/hops/h/runs", csrf, {"action": "check",
                                                     "db": "app"})
    assert r.status_code == 202 and r.get_json()["argv"] == [
        "check", "h", "--db", "app"]
    assert len(started) == 1
    viewer, _ = _viewer(web)
    assert 'data-api="/api/v1/hops/h/runs"' not in get(
        viewer, "/hops/h").data.decode()
