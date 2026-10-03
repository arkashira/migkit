"""OpenSearch: roles, users and who holds which role carried through the
security plugin's own REST API, the index names in a role as the target
calls them, and taken back - a definition the target had put back as it
was.

Measured before: `users` on an OpenSearch hop said the engine was not
supported. The security API shows no password, not even as its hash, so
a user is carried with the password the passwords file gives, or skipped
and said.
"""
import base64
import json
import ssl
import subprocess
import time
import urllib.error
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.users", "opensearch")]

pytestmark = needs_docker

SRC = ("migkit-test-eng-os-src", 16013)
DST = ("migkit-test-eng-os-dst", 16014)
ADMIN = "CHANGE_ME-Os9!admin-Qx"
ANA = "CHANGE_ME-ana-Pw7!zq"
API = "/_plugins/_security/api"


def _http(port, method, path, body=None, user="admin", password=ADMIN):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(
        f"https://127.0.0.1:{port}{path}", method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization":
                 "Basic " + base64.b64encode(
                     f"{user}:{password}".encode()).decode()})
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@pytest.fixture(scope="module")
def clusters():
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:9200", "-e", "discovery.type=single-node",
                        "-e", f"OPENSEARCH_INITIAL_ADMIN_PASSWORD={ADMIN}",
                        "-e", "OPENSEARCH_JAVA_OPTS=-Xms384m -Xmx384m",
                        "opensearchproject/opensearch:2.19.1"], check=True,
                       capture_output=True)
    try:
        for name, port in (SRC, DST):
            end = time.time() + 420
            while True:
                try:
                    code, got = _http(port, "GET", "/_cluster/health")
                    if code == 200 and got.get("status") in ("green",
                                                             "yellow"):
                        code, _ = _http(port, "GET", f"{API}/roles")
                        if code == 200:
                            break
                except Exception:
                    pass
                if time.time() > end:
                    said = subprocess.run(["docker", "logs", "--tail", "15",
                                           name], capture_output=True,
                                          text=True)
                    raise AssertionError(f"security on {port} never came"
                                         f" up: {said.stdout[-1500:]}"
                                         f" {said.stderr[-1500:]}")
                time.sleep(3)
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="admin",
                        password=ADMIN, options={"tls": True,
                                                 "verify": False})
    hop = Hop(name="ossec", engine="opensearch", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["shop"],
              db_map={"shop": "shopcopy"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_roles_users_and_who_holds_them_are_carried(clusters, tmp_path):
    from migkit import users
    s, d = SRC[1], DST[1]
    assert _http(s, "PUT", f"{API}/roles/reader", {
        "cluster_permissions": ["cluster_composite_ops_ro"],
        "index_permissions": [{"index_patterns": ["shop.*"],
                               "allowed_actions": ["read"]}]})[0] in (200,
                                                                      201)
    for port, action in ((s, "write"), (d, "read")):
        _http(port, "PUT", f"{API}/roles/writer", {
            "index_permissions": [{"index_patterns": ["shop.*"],
                                   "allowed_actions": [action]}]})
    assert _http(s, "PUT", f"{API}/internalusers/ana", {
        "password": ANA, "backend_roles": ["analysts"],
        "attributes": {"team": "a"}})[0] in (200, 201)
    _http(s, "PUT", f"{API}/internalusers/nopw",
          {"password": "CHANGE_ME-nopw-Pw7!zq"})
    _http(s, "PUT", f"{API}/rolesmapping/reader",
          {"users": ["ana"], "backend_roles": ["analysts"]})
    hop = _hop(tmp_path)
    out, _ = users.compare(hop, [].append)
    assert out["plugin"] == {"source": "present", "target": "present"}
    assert {"role:reader", "user:ana", "user:nopw", "mapping:reader"} == \
        set(out["missing_on_target"]), out
    assert out["attributes_differ"] == ["role:writer"], out
    said = []
    # nopw is skipped and said, and the gap it leaves fails the create
    with pytest.raises(SystemExit, match="still differs.*nopw"):
        users.create(hop, apply=True, passwords={"ana": ANA},
                     say=said.append)
    assert any("skipped user nopw" in m for m in said), said
    assert not any(ANA in m or ADMIN in m for m in said), said
    out, _ = users.compare(hop, [].append)
    assert out["missing_on_target"] == ["user:nopw"], out
    assert out["attributes_differ"] == [], out
    got = _http(d, "GET", f"{API}/roles/reader")[1]["reader"]
    assert got["index_permissions"][0]["index_patterns"] == ["shopcopy.*"]
    # the password given is the one ana signs in to the target with
    code, who = _http(d, "GET", "/_plugins/_security/authinfo", user="ana",
                      password=ANA)
    assert code == 200 and "reader" in who["roles"], who
    users.rollback(hop, apply=True, say=[].append)
    for kind, name in (("roles", "reader"), ("internalusers", "ana"),
                       ("rolesmapping", "reader")):
        assert _http(d, "GET", f"{API}/{kind}/{name}")[0] == 404, name
    back = _http(d, "GET", f"{API}/roles/writer")[1]["writer"]
    assert back["index_permissions"][0]["allowed_actions"] == ["read"], back
