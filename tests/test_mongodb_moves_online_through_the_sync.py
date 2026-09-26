"""MongoDB to MongoDB through the vendor's cluster-to-cluster sync
(backlog 12).

A dump of a source that is still taking writes is neither online nor
consistent as of any one moment. The sync copies online, applies what the
source changes while it runs, and is committed once it has caught up.
Measured with 1.21 between two 7.0 replica sets: started with
`preExistingDestinationData` and the hop's database as its only namespace,
it wrote nothing to the source and asked for no user there.

It is the bulk path for MongoDB once installed (`doctor --install` fetches
it from its vendor), and it gives way to the dump where the hop or the
servers cannot take it: a database mapped to another name, a side that is
not a replica set, a server older than 6.0.
"""
import io
import json
import os
import subprocess
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-sync-src", 15801), ("migkit-test-sync-dst", 15802)


def _sh(name, port, db, js):
    got = subprocess.run(["docker", "exec", name, "mongosh", "--quiet",
                          "--port", str(port), db, "--eval", js],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not movers.which("mongosync"):
        pytest.skip("the online sync is not installed here")
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                            f"{port}:{port}", "mongo:7", "--replSet",
                            f"rs{port}", "--bind_ip_all", "--port",
                            str(port)], check=True, capture_output=True)
        for name, port in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "mongosh",
                                   "--quiet", "--port", str(port), "--eval",
                                   "1"], capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            _sh(name, port, "admin",
                f"rs.initiate({{_id: 'rs{port}', members: [{{_id: 0,"
                f" host: '127.0.0.1:{port}'}}]}}).ok")
        for name, port in (SRC, DST):
            for _ in range(60):
                if _sh(name, port, "admin", "db.hello().isWritablePrimary") \
                        == "true":
                    break
                time.sleep(1)
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path, **extra):
    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="", password="")
    hop = Hop(name="sync", engine="mongodb", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["app"], **extra)
    hop.report_dir = lambda db=None: tmp_path
    return hop


@needs_docker
def test_the_database_arrives_and_the_source_is_not_written(pair, tmp_path,
                                                           monkeypatch):
    # started from somewhere else: it wrote its metrics into whatever
    # directory it was started from
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    _sh(*SRC, "app", "for (let i = 0; i < 2000; i++) db.orders.insertOne("
                     "{_id: i, n: NumberLong(String(i)), dec:"
                     " NumberDecimal('1.50')}); db.audit.insertOne({_id: 1})")
    # the target already had something the copy must replace
    _sh(*DST, "app", "db.orders.insertOne({_id: 999999, stale: true})")
    before = _sh(*SRC, "admin", "db.adminCommand({listDatabases: 1})"
                                ".databases.map(d => d.name).join(',')")
    said = []
    hop = _hop(tmp_path, exclude=["audit"])
    movers.mongosync_move(hop, "app", 1, True, said.append)
    assert _sh(*DST, "app", "db.orders.countDocuments()") == "2000", said
    assert _sh(*DST, "app", "db.orders.findOne({_id: 5}).n.constructor"
                            ".name") in ("Long", "NumberLong"), said
    assert _sh(*DST, "app", "db.orders.findOne({_id: 5}).dec.toString()") \
        == "1.50"
    # the excluded collection was not carried
    assert _sh(*DST, "app", "db.getCollectionNames().includes('audit')") \
        == "false"
    # nothing appeared on the source
    assert _sh(*SRC, "admin", "db.adminCommand({listDatabases: 1})"
                              ".databases.map(d => d.name).join(',')") \
        == before
    assert any("committed" in s for s in said), said
    # the process ended, and its configuration file went with it
    assert not subprocess.run(["pgrep", "-f", "mongosync --config"],
                              capture_output=True).stdout
    assert not list(tmp_path.glob("running-programs.json"))
    # its logs and metrics in the reports, nothing where it was started
    assert list(elsewhere.iterdir()) == []
    assert (tmp_path / "sync-metrics").is_dir()


@needs_docker
def test_it_gives_way_where_the_hop_or_servers_cannot_take_it(pair,
                                                              tmp_path):
    via, why = movers.fitted(_hop(tmp_path), "mongodb", "mongosync")
    assert (via, why) == ("mongosync", None)
    renamed = _hop(tmp_path, db_map={"app": "app2"})
    via, why = movers.fitted(renamed, "mongodb", "mongosync")
    assert via != "mongosync" and "keeps each database's name" in why
    gone = Hop(name="x", engine="mongodb",
               source=Endpoint(host="127.0.0.1", port=1, user="",
                               password="",
                               options={"uri_options":
                                        "serverSelectionTimeoutMS=500"}),
               target=Endpoint(host="127.0.0.1", port=DST[1], user="",
                               password=""), databases=["app"])
    via, why = movers.fitted(gone, "mongodb", "mongosync")
    assert via != "mongosync" and "could not be asked" in why, why


def test_it_is_the_bulk_path_once_installed(monkeypatch):
    monkeypatch.setattr(movers, "which",
                        lambda p: "/x/" + p if p == "mongosync" else None)
    assert movers.pick("mongodb") == "mongosync"


def test_the_vendor_build_is_fetched_and_checked(tmp_path, monkeypatch):
    from migkit import tools
    def build(version):
        body = io.BytesIO()
        with zipfile.ZipFile(body, "w") as z:
            info = zipfile.ZipInfo("mongosync-x/bin/mongosync")
            info.external_attr = 0o755 << 16
            z.writestr(info, f"#!/bin/sh\necho 'version: {version}'\n")
        return body.getvalue()
    served = build("1.21.0")

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(served)
    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setattr(tools, "VENDOR_URL",
                            f"http://127.0.0.1:{server.server_port}/")
        monkeypatch.setattr(tools, "vendor_file",
                            lambda p: "mongosync-macos-arm-arm64-1.21.0.zip")
        said = []
        assert tools.install_vendor("mongosync", said.append, into=tmp_path)
        assert os.access(tmp_path / "mongosync", os.X_OK)
        # a build that says another version is not the one asked for
        served = build("9.99.9")
        (tmp_path / "mongosync").unlink()
        assert not tools.install_vendor("mongosync", said.append,
                                        into=tmp_path)
    finally:
        server.shutdown()
        server.server_close()
    assert json.dumps(said).find("mongosync") == -1, said
