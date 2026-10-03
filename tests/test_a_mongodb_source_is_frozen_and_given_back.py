"""A MongoDB source frozen at cutover, proved, and given back (research:
cutover, section 2.1).

The whole server, where the hop's databases are all of it: user writes
blocked (`setUserWriteBlockMode`), which every account obeys but one that
may bypass it - migkit's own, whose tail still has to read. One database
of a server it shares: the application's accounts lose the roles that
write there for `read`, and get them back exactly. Either way their
sessions are ended, so a transaction they held open is aborted rather
than committed after the position the drain waits for.
"""
import subprocess
import time

import pymongo
import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.freeze", "mongodb")]

pytestmark = pytest.mark.docker

NAME, PORT = "migkit-test-cut-mongo", 16075


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    start = ("openssl rand -base64 756 > /tmp/key && chmod 400 /tmp/key &&"
             " chown 999:999 /tmp/key && exec docker-entrypoint.sh mongod"
             " --replSet rs0 --keyFile /tmp/key --bind_ip_all")
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:27017", "-e",
                    "MONGO_INITDB_ROOT_USERNAME=root", "-e",
                    "MONGO_INITDB_ROOT_PASSWORD=test", "--entrypoint", "bash",
                    "mongo:7", "-c", start], check=True, capture_output=True)
    try:
        for _ in range(90):
            got = subprocess.run(
                ["docker", "exec", NAME, "mongosh", "-u", "root", "-p",
                 "test", "--quiet", "--eval",
                 "try { rs.status().ok } catch (e) { rs.initiate({_id:"
                 " 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
                 ".ok }"], capture_output=True, text=True)
            if got.stdout.strip().endswith("1"):
                break
            time.sleep(2)
        root = _client("root", "test")
        for _ in range(60):
            try:
                if root.admin.command("hello").get("isWritablePrimary"):
                    break
            except pymongo.errors.PyMongoError:
                pass
            time.sleep(1)
        yield root
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _client(user, password, db="admin"):
    return pymongo.MongoClient(
        f"mongodb://{user}:{password}@127.0.0.1:{PORT}/{db}"
        "?directConnection=true", serverSelectionTimeoutMS=10000)


def _hop(tmp_path):
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test",
                  options={"uri_options": "directConnection=true"})
    hop = Hop(name="mgc", engine="mongodb", source=ep, target=ep,
              databases=["app"], options={"cutover": True})
    root = tmp_path / "reports"

    def report_dir(db=""):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


@pytest.fixture
def app(server):
    for db in ("app", "other"):
        server.drop_database(db)
        for u in server[db].command("usersInfo")["users"]:
            server[db].command("dropUser", u["user"])
    server.app.orders.insert_one({"tag": "seed"})
    server.app.command("createUser", "app", pwd="CHANGE_ME",
                       roles=[{"role": "readWrite", "db": "app"}])
    yield _client("app", "CHANGE_ME", "app")
    server.admin.command({"setUserWriteBlockMode": 1, "global": False})
    server.drop_database("other")


def _roles(server, user, db):
    got = server[db].command("usersInfo", user)["users"][0]["roles"]
    return sorted((r["role"], r["db"]) for r in got)


def test_the_whole_server_is_blocked_proved_and_given_back(server, app,
                                                           tmp_path):
    from migkit import freeze
    from migkit.engines.mongodb import MongoEngine
    hop = _hop(tmp_path)
    eng = MongoEngine(hop)
    assert freeze.scope(hop, eng, ["app"])[0] == "server"
    # a transaction the application holds open across the freeze
    session = app.start_session()
    session.start_transaction()
    app.app.orders.insert_one({"tag": "in flight"}, session=session)
    assert freeze.open_work(hop, eng, "app", "src", None), \
        "the open transaction is seen"
    before = freeze.writes_seen(hop, eng, "app")
    said = []
    freeze.freeze_source(hop, eng, ["app"], said.append, "server")
    held = freeze.proof(hop, eng, "app", "server")
    assert all(ok for ok, _ in held), held
    with pytest.raises(pymongo.errors.PyMongoError):
        session.commit_transaction()
    with pytest.raises(pymongo.errors.PyMongoError):
        app.app.orders.insert_one({"tag": "while frozen"})
    # migkit's own account still writes: its tail and its undo need to
    server.app.marks.insert_one({"x": 1})
    assert freeze.writes_seen(hop, eng, "app") > before
    freeze.thaw_source(hop, eng, ["app"], said.append)
    app.app.orders.insert_one({"tag": "after"})
    assert sorted(d["tag"] for d in server.app.orders.find()) == [
        "after", "seed"]
    assert freeze.server_state(hop) is None
    assert freeze.state(hop, "app", "src") is None


def test_one_database_of_a_shared_server_is_frozen_by_its_roles(
        server, app, tmp_path):
    from migkit import freeze
    from migkit.engines.mongodb import MongoEngine
    server.other.t.insert_one({"x": 1})
    server.other.command("createUser", "otherapp", pwd="CHANGE_ME",
                         roles=[{"role": "readWrite", "db": "other"}])
    other = _client("otherapp", "CHANGE_ME", "other")
    hop = _hop(tmp_path)
    eng = MongoEngine(hop)
    how, why = freeze.scope(hop, eng, ["app"])
    assert how == "database" and "other" in why, why
    roles = _roles(server, "app", "app")
    said = []
    freeze.freeze_source(hop, eng, ["app"], said.append, "database")
    held = freeze.proof(hop, eng, "app", "database")
    assert all(ok for ok, _ in held), held
    fresh = _client("app", "CHANGE_ME", "app")
    with pytest.raises(pymongo.errors.PyMongoError):
        fresh.app.orders.insert_one({"tag": "while frozen"})
    assert fresh.app.orders.count_documents({}) == 1, "reading still works"
    other.other.t.insert_one({"x": 2})
    freeze.thaw_source(hop, eng, ["app"], said.append)
    assert _roles(server, "app", "app") == roles
    _client("app", "CHANGE_ME", "app").app.orders.insert_one({"tag": "a"})
    assert freeze.state(hop, "app", "src") is None
