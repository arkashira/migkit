"""The identity provider provisions the people and groups who may use the
web view over SCIM 2.0, and takes them away - at once (backlog W2).

Driven the way provisioning connectors drive it: create, find by filter,
patch (`Replace`, `"False"`), put, delete, groups and their members. What
a change does is held too: a group given decides a role on a hop, a
person deactivated loses every session and API token in the same
request, and a filter on an attribute the directory does not keep is
refused rather than read as no filter at all - which would list
everyone.
"""
import pytest

from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import (URL, browser, call, get, place, signed_in,
                                 view)

TOKEN = "scim-token-" + "t" * 30
PATCH = "urn:ietf:params:scim:api:messages:2.0:PatchOp"


@pytest.fixture
def scim(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    # provisioning beside a provider: the local account is not there
    app = view(tmp_path, MIGKIT_WEB_SCIM_TOKEN=TOKEN,
               MIGKIT_WEB_OIDC_ISSUER="http://127.0.0.1:9",
               MIGKIT_WEB_OIDC_CLIENT_ID="migkit",
               MIGKIT_WEB_OIDC_CLIENT_SECRET="client-secret-0123456789")
    yield app
    app.store.close()


def _scim(c, method, path, body=None, token=TOKEN, **query):
    kw = {"headers": {"Authorization": f"Bearer {token}"}, "base_url": URL}
    if body is not None:
        kw["json"] = body
    if query:
        kw["query_string"] = query
    return c.open(f"/scim/v2{path}", method=method, **kw)


def _user(c, name, **more):
    r = _scim(c, "POST", "/Users", {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": name, "emails": [{"value": name, "primary": True}],
        **more})
    assert r.status_code == 201, r.data
    return r.get_json()["id"]


def test_nothing_is_provisioned_without_the_token(scim):
    c = browser(scim)
    assert _scim(c, "GET", "/Users", token="wrong-token").status_code == 401
    r = c.get("/scim/v2/Users", base_url=URL)
    assert r.status_code == 401
    assert r.get_json()["schemas"] == [
        "urn:ietf:params:scim:api:messages:2.0:Error"]
    # and the refusal is in the view's record
    from migkit.durable import read_lines
    assert any(e["op"] == "refused" and "provisioning" in e["why"]
               for e in read_lines(scim.record_path, "cache"))


def test_people_are_found_by_the_filters_providers_send(scim):
    c = browser(scim)
    ann = _user(c, "ann@example.com", externalId="e-ann")
    _user(c, "bob@example.com", active=False)
    got = _scim(c, "GET", "/Users", filter='userName eq "ANN@example.com"')
    body = got.get_json()
    # user names are compared in any case, as SCIM says
    assert got.status_code == 200 and body["totalResults"] == 1
    assert body["Resources"][0]["id"] == ann
    assert body["Resources"][0]["emails"][0]["value"] == "ann@example.com"
    got = _scim(c, "GET", "/Users", filter='externalId eq "e-ann"').get_json()
    assert [u["userName"] for u in got["Resources"]] == ["ann@example.com"]
    got = _scim(c, "GET", "/Users", filter="active eq false").get_json()
    assert [u["userName"] for u in got["Resources"]] == ["bob@example.com"]
    got = _scim(c, "GET", "/Users", filter='userName sw "b"').get_json()
    assert [u["userName"] for u in got["Resources"]] == ["bob@example.com"]
    # a page at a time, every person on exactly one page
    first = _scim(c, "GET", "/Users", startIndex="1", count="1").get_json()
    second = _scim(c, "GET", "/Users", startIndex="2", count="1").get_json()
    assert first["totalResults"] == second["totalResults"] == 2
    assert first["itemsPerPage"] == second["itemsPerPage"] == 1
    assert {first["Resources"][0]["userName"],
            second["Resources"][0]["userName"]} == {"ann@example.com",
                                                     "bob@example.com"}


def test_a_filter_on_what_is_not_kept_is_refused_not_ignored(scim):
    c = browser(scim)
    _user(c, "ann@example.com")
    _user(c, "bob@example.com")
    r = _scim(c, "GET", "/Users", filter='title eq "boss"')
    assert r.status_code == 400
    assert r.get_json()["scimType"] == "invalidFilter"
    r = _scim(c, "GET", "/Users", filter='userName eq "a" or title eq "b"')
    assert r.status_code == 400
    r = _scim(c, "GET", "/Users", filter="userName eq")
    assert r.status_code == 400
    # the same filter on what is kept finds what it names, not everyone
    r = _scim(c, "GET", "/Users", filter='userName eq "ann@example.com"')
    assert r.status_code == 200 and r.get_json()["totalResults"] == 1


def test_the_same_name_twice_is_a_conflict(scim):
    c = browser(scim)
    _user(c, "ann@example.com")
    r = _scim(c, "POST", "/Users", {"userName": "Ann@Example.com"})
    assert r.status_code == 409 and r.get_json()["scimType"] == "uniqueness"


def test_a_group_given_decides_a_role_and_taken_away_undoes_it(scim):
    """ann is a viewer of h by the file's hop group; the file makes
    `group:dba` an operator there, and the provider putting her in dba
    makes her one - taking her out, not."""
    c = browser(scim)
    ann = _user(c, "ann-two@example.com")
    me = browser(scim)
    signed_in(scim, me, {"id": "ann-two@example.com", "via": "oidc"})
    assert get(me, "/api/v1/me").get_json()["roles"]["h"] == "viewer"
    r = _scim(c, "POST", "/Groups", {"displayName": "dba",
                                     "members": [{"value": ann}]})
    assert r.status_code == 201, r.data
    gid = r.get_json()["id"]
    assert r.get_json()["members"][0]["display"] == "ann-two@example.com"
    got = get(me, "/api/v1/me").get_json()
    assert "dba" in got["groups"] and got["roles"]["h"] == "operator"
    user = _scim(c, "GET", f"/Users/{ann}").get_json()
    assert [g["display"] for g in user["groups"]] == ["dba"]
    found = _scim(c, "GET", "/Groups",
                  filter=f'members[value eq "{ann}"]').get_json()
    assert [g["id"] for g in found["Resources"]] == [gid]
    r = _scim(c, "PATCH", f"/Groups/{gid}", {
        "schemas": [PATCH], "Operations": [
            {"op": "remove", "path": f'members[value eq "{ann}"]'}]})
    assert r.status_code == 200 and r.get_json()["members"] == []
    assert get(me, "/api/v1/me").get_json()["roles"]["h"] == "viewer"


def test_deactivated_or_deleted_the_person_opens_nothing_any_more(scim):
    c = browser(scim)
    ann = _user(c, "ann@example.com")
    bob = _user(c, "bob@example.com")
    ann_page, bob_page = browser(scim), browser(scim)
    csrf = signed_in(scim, ann_page, {"id": "ann@example.com",
                                      "via": "oidc"})
    signed_in(scim, bob_page, {"id": "bob@example.com", "via": "oidc"})
    made = call(ann_page, "POST", "/api/v1/tokens", csrf, {"name": "ci"})
    assert made.status_code == 201
    token = made.get_json()["token"]
    api = browser(scim)
    assert get(api, "/api/v1/me", headers={
        "Authorization": f"Bearer {token}"}).status_code == 200
    r = _scim(c, "PATCH", f"/Users/{ann}", {
        "schemas": [PATCH], "Operations": [
            {"op": "Replace", "path": "active", "value": "False"}]})
    assert r.status_code == 200 and r.get_json()["active"] is False
    assert get(ann_page, "/api/v1/me").status_code == 401
    assert get(api, "/api/v1/me", headers={
        "Authorization": f"Bearer {token}"}).status_code == 401
    # bob is untouched until he is deleted
    assert get(bob_page, "/api/v1/me").status_code == 200
    assert _scim(c, "DELETE", f"/Users/{bob}").status_code == 204
    assert get(bob_page, "/api/v1/me").status_code == 401
    assert _scim(c, "GET", f"/Users/{bob}").status_code == 404
    from migkit.durable import read_lines
    ops = [e["op"] for e in read_lines(scim.record_path, "cache")]
    assert "scim-user-change" in ops and "scim-user-delete" in ops
    assert ops.count("signed-out") == 2


def test_a_put_replaces_the_person(scim):
    c = browser(scim)
    ann = _user(c, "ann@example.com", displayName="Ann")
    r = _scim(c, "PUT", f"/Users/{ann}", {
        "userName": "ann@example.com", "displayName": "Ann Example",
        "active": True, "name": {"givenName": "Ann"}})
    body = r.get_json()
    assert r.status_code == 200 and body["displayName"] == "Ann Example"
    assert body["name"] == {"givenName": "Ann"}
    assert body["meta"]["version"] == 'W/"2"'
    assert "emails" not in body


def test_the_service_says_what_it_takes(scim):
    c = browser(scim)
    got = _scim(c, "GET", "/ServiceProviderConfig").get_json()
    assert got["patch"]["supported"] is True
    assert got["filter"]["supported"] is True
    assert got["bulk"]["supported"] is False
    kinds = _scim(c, "GET", "/ResourceTypes").get_json()
    assert {r["name"] for r in kinds["Resources"]} == {"User", "Group"}


def test_without_its_token_set_there_is_no_provisioning(web):
    assert browser(web).get("/scim/v2/Users",
                            base_url=URL).status_code == 404

