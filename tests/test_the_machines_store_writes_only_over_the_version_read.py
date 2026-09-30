"""Where the machines of a hop agree: a record written only over the
version that was read, in the run state's bucket, a directory every
machine sees, or rows of the target's `migkit_origin` (backlog W1,
`shared`).

Which store is read from the hop file alone - every machine must find
the same - and the one found is proved before it is trusted: a store that
lets a record be made twice, or written over a version already gone, is
refused with the reason. The stores themselves run here too: the
directory in a temporary one, the target on a PostgreSQL and a MySQL
server and the bucket on an S3-compatible store in containers of their
own.
"""
import socket
import subprocess
import threading
import time

import pytest

from migkit import ha, hopkeys, shared, tooluse
from migkit.config import Endpoint, Hop
from migkit.state import Taken

PG, PG_PORT = "migkit-test-w1-store-pg", 16424
MY, MY_PORT = "migkit-test-w1-store-my", 16425
S3, S3_PORT = "migkit-test-w1-store-s3", 16426


def _hop(tmp_path, name="st", engine="postgres", options=None, **ends):
    hop = Hop(name=name, engine=engine,
              source=ends.get("source") or Endpoint(host="127.0.0.1",
                                                    port=1, user="x",
                                                    password="x"),
              target=ends.get("target") or Endpoint(host="127.0.0.1",
                                                    port=1, user="x",
                                                    password="x"),
              databases=list(ends.get("databases") or ["app"]),
              options=dict(options or {}))
    root = tmp_path / "reports"

    def report_dir(db=None):
        d = root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


def _cas(store, hop):
    """Made once, written over the version read, refused over a version
    gone: the three things a store must do."""
    path = hop.report_dir("app") / "cas.json"
    store.write_record(path, {"n": 1}, None)
    with pytest.raises(Taken):
        store.write_record(path, {"n": 2}, None)
    got, v = store.read_record(path)
    assert got == {"n": 1}
    store.write_record(path, {"n": 3}, v)
    with pytest.raises(Taken):
        store.write_record(path, {"n": 4}, v)
    assert store.read_record(path)[0] == {"n": 3}
    store.put(hop.report_dir("app") / "text.json", "hello")
    assert store.get(hop.report_dir("app") / "text.json") == "hello"
    store.delete(path)
    assert store.read_record(path) == (None, None)


def _racing(store, hop, threads=8, each=25):
    """Increments from many threads, each read and written over its
    version: none lost."""
    path = hop.report_dir("app") / "count.json"

    def bump():
        for _ in range(each):
            ha._change(store, path, lambda have: {
                "n": int((have or {}).get("n") or 0) + 1}, sealed=False)
    ts = [threading.Thread(target=bump) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert store.read_record(path)[0] == {"n": threads * each}


def test_a_directory_every_machine_sees(tmp_path):
    hop = _hop(tmp_path)
    store = shared.Directory(hop, tmp_path / "shared")
    _cas(store, hop)
    _racing(store, hop)
    assert shared.prove(store, hop) is None
    digest = store.put_blob(b"\x00\x01kept")
    assert store.get_blob(digest) == b"\x00\x01kept"


class _Lying(shared.Directory):
    """A store that takes every write, whatever version it names - the
    S3-compatible store that ignores a condition."""

    def write_record(self, path, record, version, sealed=False):
        _, have = self.read_record(path)
        super().write_record(path, record, have, sealed)


class _Refusing(shared.Directory):
    """One that refuses a write over the very version it just read."""

    def write_record(self, path, record, version, sealed=False):
        if version is not None:
            raise Taken()
        super().write_record(path, record, version, sealed)


def test_the_proof_says_different_for_a_store_that_ignores_conditions(
        tmp_path):
    hop = _hop(tmp_path)
    why = shared.prove(_Lying(hop, tmp_path / "lying"), hop)
    assert why and "was not refused" in why, why
    why = shared.prove(_Refusing(hop, tmp_path / "refusing"), hop)
    assert why == "a write over the version just read was refused", why


def test_the_store_is_read_from_the_hop_file_alone(tmp_path, monkeypatch):
    shared.forget()
    bucket = _hop(tmp_path, "b", options={
        "standby": True, "state": {"backend": "s3", "bucket": "x",
                                   "endpoint_url": "http://127.0.0.1:1"}})
    got, why = shared.choose(bucket)
    assert got.kind == "bucket", why
    shared_dir = _hop(tmp_path, "d", options={
        "standby": True, "state": {"shared": str(tmp_path / "s")}})
    got, why = shared.choose(shared_dir)
    assert got.kind == "directory" and why.path == "directory"
    # the bucket passed over, and why
    assert "not in a bucket" in why.reason, why.reason
    # machines sharing tables only: the target keeps their records
    monkeypatch.setattr(shared, "_target_of", lambda hop: (object(), "app"))
    ranges = _hop(tmp_path, "r", options={"share_tables": "ranges"})
    got, why = shared.choose(ranges)
    assert got.kind == "target", why
    # a standby needs the tail's files kept, which the target does not
    with pytest.raises(SystemExit) as e:
        shared.choose(_hop(tmp_path, "s", options={"standby": True}))
    said = str(e.value)
    assert "files that are not text" in said and "state: {shared" in said
    # and a hop run on one machine keeps its run state as before
    assert shared.run_state(_hop(tmp_path, "one")) is None
    shared.forget()


def test_the_keys_are_declared_and_said(tmp_path):
    raw = {"hops": {"h": {
        "engine": "postgres",
        "source": {"host": "h", "user": "u", "password": "CHANGE_ME"},
        "target": {"host": "h", "user": "u", "password": "CHANGE_ME"},
        "options": {"standby": True, "share_tables": "ranges",
                    "state": {"shared": "/mnt/migkit"}}}}}
    problems = [p for p in hopkeys.check(raw) if "options" in p.where]
    assert problems == [], problems
    raw["hops"]["h"]["options"]["share_tables"] = "range"
    bad = [p for p in hopkeys.check(raw) if p.level == "error"]
    assert any("share_tables" in p.where for p in bad), bad
    hop = _hop(tmp_path, options={"standby": True,
                                  "share_tables": "ranges"})
    said = "\n".join(hopkeys.explain_goal(hop))
    assert "standby: the same move run on two or more machines" in said
    assert "share_tables: ranges: every machine running the move" in said
    assert not hopkeys.machines_lines(_hop(tmp_path, "plain"))


def test_the_conditional_writes_are_accounted_for():
    assert tooluse.unexplained("boto3-s3-conditional") == []
    assert tooluse.stale("boto3-s3-conditional") == []
    used = tooluse.used("boto3-s3-conditional", "s3")
    assert {"IfMatch", "IfNoneMatch", "ETag", "put_object"} <= set(used), \
        used


# ---- the stores on real servers ---------------------------------------------

def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _wait(port, ok=None, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0 and (
                    ok is None or ok()):
                return
        time.sleep(2)
    pytest.fail(f"nothing answered on {port}")


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for name in (PG, MY, S3):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-e", "MYSQL_DATABASE=app",
                    "-p", f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", S3, "-e",
                    "MINIO_ROOT_USER=minioadmin", "-e",
                    "MINIO_ROOT_PASSWORD=minioadmin", "-p",
                    f"127.0.0.1:{S3_PORT}:9000", "bitnamilegacy/minio:latest"],
                   check=True, capture_output=True)
    try:
        _wait(PG_PORT, lambda: subprocess.run(
            ["docker", "exec", PG, "pg_isready", "-U", "postgres"],
            capture_output=True).returncode == 0)
        subprocess.run(["docker", "exec", PG, "psql", "-U", "postgres",
                        "-c", "create database app"], capture_output=True)
        _wait(MY_PORT, lambda: subprocess.run(
            ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
             "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
            capture_output=True).returncode == 0)
        _wait(S3_PORT)
        yield
    finally:
        for name in (PG, MY, S3):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.mark.docker
@pytest.mark.parametrize("engine,port,user", [("postgres", PG_PORT,
                                               "postgres"),
                                              ("mysql", MY_PORT, "root")])
def test_rows_of_migkit_origin_on_the_target(servers, tmp_path, engine,
                                            port, user):
    from migkit.engines import engine_named
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password="test")
    hop = _hop(tmp_path, f"t{engine[:2]}", engine, source=ep, target=ep)
    store = shared.Target(hop, engine_named(engine, hop), "app")
    _cas(store, hop)
    _racing(store, hop, threads=6, each=10)
    assert shared.prove(store, hop) is None
    assert store.blobs is False


@pytest.mark.docker
def test_the_run_states_bucket(servers, tmp_path, monkeypatch):
    import boto3
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    client = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{S3_PORT}",
                          region_name="us-east-1")
    for _ in range(30):
        try:
            client.create_bucket(Bucket="w1-state")
            break
        except Exception:
            time.sleep(1)
    shared.forget()
    hop = _hop(tmp_path, "bk", options={
        "standby": True, "state": {
            "backend": "s3", "bucket": "w1-state",
            "endpoint_url": f"http://127.0.0.1:{S3_PORT}"}})
    store, _ = shared.choose(hop)
    _cas(store, hop)
    _racing(store, hop, threads=4, each=10)
    assert shared.prove(store, hop) is None
    digest = store.put_blob(b"kept proof")
    # a digest already there holds the same bytes, and is not an error
    assert store.put_blob(b"kept proof") == digest
    assert store.get_blob(digest) == b"kept proof"
    shared.forget()
