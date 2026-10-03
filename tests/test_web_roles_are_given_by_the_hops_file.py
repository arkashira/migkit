"""Who may do what on which hop is written in the hops file - for every
hop, a group of hops, or one hop - and nothing gives a role the file
does not (backlog W2).

Held: the four roles each hold the ones before them; a person matched by
name (any case, shell patterns) or by a group; the highest role any of
the three places gives; nobody signed in through a provider has a role
the file does not name; the single local operator is the admin of every
hop; a wrong `access` stops the file loading, as any wrong key does; and
through the API, a hop not the caller's to see is not there, and a role
too low is refused and written to the hop's record.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.web import roles
from migkit.web.identity import Principal
from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import browser, call, get, signed_in

ACCESS = {"admin": ["root@example.com", "group:migkit-admins"],
          "groups": {"payments": {"hops": ["pay-*", "orders"],
                                  "viewer": ["*@example.com"],
                                  "operator": ["group:dba"],
                                  "approver": ["group:change-board"]}}}


def _hop(name, access=None):
    return Hop(name=name, engine="postgres", source=Endpoint(host="10.0.0.1"),
               target=Endpoint(host="10.0.0.2"),
               options={"access": access} if access else {})


def _p(pid, *groups, via="oidc"):
    return Principal(id=pid, groups=tuple(groups), via=via)


@pytest.mark.parametrize("who,hop,want", [
    (_p("ann@example.com"), _hop("pay-eu"), "viewer"),
    (_p("ANN@Example.com"), _hop("orders"), "viewer"),
    (_p("ann@example.com", "dba"), _hop("pay-eu"), "operator"),
    (_p("ann@example.com", "DBA"), _hop("pay-eu"), "operator"),
    (_p("ann@example.com", "dba", "change-board"), _hop("orders"),
     "approver"),
    (_p("root@example.com"), _hop("anything"), "admin"),
    (_p("eve@other.example"), _hop("anything"), None),
    (_p("sam@example.com", "migkit-admins"), _hop("x"), "admin"),
    (_p("ann@elsewhere.example"), _hop("pay-eu"), None),
    (_p("ann@example.com"), _hop("billing"), None),
    # the hop's own access adds to the file's, the highest wins
    (_p("ann@example.com"), _hop("pay-eu", {"operator": ["ann@example.com"]}),
     "operator"),
    (_p("ann@example.com", "change-board"),
     _hop("pay-eu", {"viewer": ["ann@example.com"]}), "approver"),
    (_p("kim@example.com"), _hop("billing", {"viewer": ["kim@*"]}),
     "viewer"),
    # the one local operator of an install without a provider
    (_p("admin", via="local"), _hop("billing"), "admin"),
])
def test_a_role_is_the_highest_the_file_gives(who, hop, want):
    assert roles.role(who, hop, ACCESS) == want


def test_each_role_holds_the_ones_before_it():
    hop = _hop("h", {"approver": ["bob@example.com"]})
    bob = _p("bob@example.com")
    assert [roles.may(bob, hop, r, {}) for r in roles.ROLES] == [
        True, True, True, False]
    assert not roles.may(_p("eve@example.com"), hop, "viewer", {})


def test_nobody_has_a_role_where_the_file_names_nobody():
    hop = _hop("h")
    assert roles.role(_p("ann@example.com"), hop, {}) is None
    assert roles.anyone_approves(hop, {}) is False
    # named approver or admin anywhere that reaches the hop: approvals
    # given in the view count for it
    assert roles.anyone_approves(_hop("h", {"approver": ["x@example.com"]}),
                                 {}) is True
    assert roles.anyone_approves(hop, {"admin": ["root@example.com"]}) \
        is True
    assert roles.anyone_approves(_hop("pay-1"), ACCESS) is True


@pytest.mark.parametrize("access,where", [
    ({"groups": {"g": {"hops": ["x"], "boss": ["a@example.com"]}}},
     "access.groups.g.boss"),
    ({"groups": {"g": {"viewer": ["a@example.com"]}}}, "access.groups.g"),
    ({"admins": ["a@example.com"]}, "access.admins"),
    ({"admin": {"a": 1}}, "access.admin"),
    ("everyone", "access"),
])
def test_a_wrong_access_stops_the_file_loading(tmp_path, access, where):
    from migkit import hopkeys
    problems = hopkeys.check({"access": access, "hops": {}})
    errors = [p for p in problems if p.level == "error"]
    assert [p.where for p in errors] == [where], problems
    assert not [p for p in hopkeys.check({"access": ACCESS, "hops": {}})
                if p.level == "error"]
    # a hop's own access takes the four roles and nothing else
    bad = hopkeys.check_hop("h", {"engine": "postgres",
                                  "source": {"host": "10.0.0.1"},
                                  "target": {"host": "10.0.0.2"},
                                  "options": {"access": {"owner": ["x"]}}})
    assert [p.where for p in bad if p.level == "error"] == [
        "hops.h.options.access.owner"]


def test_through_the_api_a_hop_not_yours_is_not_there(web):
    c = browser(web)
    signed_in(web, c, {"id": "dan@partner.example", "via": "oidc"})
    got = get(c, "/api/v1/hops").get_json()
    assert [h["name"] for h in got["hops"]] == ["other"]
    assert get(c, "/api/v1/hops/h").status_code == 404
    assert get(c, "/api/v1/hops/other").status_code == 200
    assert get(c, "/api/v1/hops/nowhere").status_code == 404
    page = get(c, "/hops/h")
    assert page.status_code == 404


def test_a_role_too_low_is_refused_said_and_recorded(web):
    c = browser(web)
    csrf = signed_in(web, c, {"id": "carol@example.com", "via": "oidc"})
    r = call(c, "POST", "/api/v1/hops/h/runs", csrf, {"action": "check"})
    assert r.status_code == 403
    assert r.get_json()["error"] == ("your role on h is viewer; this needs"
                                     " operator")
    import json
    from migkit.config import REPORTS
    record = [json.loads(line) for line in
              (REPORTS / "h" / "changelog.jsonl").read_text().splitlines()]
    assert record[-1]["op"] == "refused"
    assert record[-1]["by"] == "carol@example.com"
    assert record[-1]["through"] == "web"
    # the viewer reads what a viewer reads
    assert get(c, "/api/v1/hops/h/outcome").status_code == 200
    # a notification test is an admin's
    r = call(c, "POST", "/api/v1/hops/h/notifications/test", csrf)
    assert r.status_code == 403


def test_the_global_pages_are_a_global_admins(web):
    c = browser(web)
    signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    assert get(c, "/api/v1/admin/people").status_code == 403
    assert get(c, "/admin").status_code == 403
    root = browser(web)
    signed_in(web, root, {"id": "root@example.com", "via": "oidc"})
    assert get(root, "/api/v1/admin/people").status_code == 200
    assert get(root, "/admin").status_code == 200
