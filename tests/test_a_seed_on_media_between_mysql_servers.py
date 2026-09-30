"""A seed shipped on media between two MySQL servers: where the target
already has the tables, a table with an integer key goes as ranges of the
bulk load's lines - rendered by the source under one consistent snapshot a
range, its digest line first - and each is loaded by the target's own bulk
load in one transaction, committed only where the target's digest of the
range is the manifest's; a part changed on the way is read again from the
source over the link.

Two servers in containers of this test's own (127.0.0.1:16412, 16413),
removed after.
"""
import socket
import subprocess
import time

import pytest

from migkit import carriers, ship
from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker

pytestmark = [needs_docker,
              pytest.mark.skipif(not which("mysql"),
                                 reason="the source's own client is not"
                                        " installed here")]

SRC, DST = "migkit-ship-my-src", "migkit-ship-my-dst"
PORT = {SRC: 16412, DST: 16413}

TABLE = ("create table orders (id bigint primary key, item varchar(40)"
         " not null, qty int, at datetime(6), note varbinary(16),"
         " amount decimal(12,2))")


def _my(name, sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, most=300):
    end = time.time() + most
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            up = s.connect_ex(("127.0.0.1", PORT[name])) == 0
        if up and subprocess.run(
                ["docker", "exec", name, "mysql", "-uroot", "-ptest",
                 "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                capture_output=True).returncode == 0:
            return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture
def pair():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                   capture_output=True)
    try:
        for name in (SRC, DST):
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{PORT[name]}:3306", "mysql:8.4",
                            "--local-infile=1"],
                           check=True, capture_output=True)
        for name in (SRC, DST):
            _ready(name)
            _my(name, "create database app", "mysql")
            _my(name, TABLE)
        _my(SRC, "insert into orders with recursive g(n) as (select 1 union"
                 " all select n + 1 from g where n < 12000) select n,"
                 " concat('item-', n), n % 7, timestamp('2026-10-01') +"
                 " interval n minute, unhex(md5(n)), n / 3 from g")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                       capture_output=True)


@pytest.fixture
def hop(pair, tmp_path, monkeypatch):
    import pyrage
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    monkeypatch.setattr(ship, "PART_BYTES", 64 * 1024)
    monkeypatch.setattr(ship._Work, "workers", lambda self, units: 1)
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    got = Hop(name="myseed", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=PORT[SRC], user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT[DST], user="root",
                              password="test"),
              databases=["app"],
              options={"at_rest": {"recipients": [str(who.to_public())]},
                       "target_rows": "replace",
                       "ship": {"carrier": "disk",
                                "path": str(tmp_path / "carrier"),
                                "shipping": "1d"}})
    got.report_dir = report_dir
    return got


def _carry(hop):
    from migkit.engines.mysql import MySQLEngine
    said = []
    got = ship.carry(hop, MySQLEngine(hop), "app", "full", True, said.append,
                     via="builtin")
    return got, said


def _same():
    sql = ("select count(*), md5(group_concat(concat_ws('|', id, item, qty,"
           " at, hex(note), amount) order by id separator ',')) from orders")
    return _my(SRC, "set session group_concat_max_len = 1073741824;" + sql) \
        == _my(DST, "set session group_concat_max_len = 1073741824;" + sql)


def test_ranges_go_as_the_bulk_loads_lines_and_are_proved_by_the_target(
        hop):
    got, said = _carry(hop)
    assert got["app"].half == "export", said
    from migkit.engines.mysql import MySQLEngine
    store = carriers.LocalStore(hop.options["ship"]["path"])
    w = ship._Work(hop, MySQLEngine(hop), "app", "full", lambda m: None)
    manifest = ship.read_manifest(w, store, ship.seal_at(store, hop, "app"))
    (orders,) = manifest["tables"]
    assert (orders["cut"], orders["format"]) == ("ranges", "load lines")
    assert len(orders["parts"]) > 2
    # nothing is on the target yet
    assert _my(DST, "select count(*) from orders") == "0"
    got, said = _carry(hop)
    assert got["app"].half == "load", said
    assert _same()
    # a part changed on the way is read again from the source, and the
    # target ends as the source is
    part = orders["parts"]["1"]
    path = store.local(part["name"])
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF
    path.write_bytes(bytes(raw))
    # loaded again from its start, as a far end with no record of it would
    (hop.report_dir("app") / ship.LOAD_STATE).unlink()
    got, said = _carry(hop)
    assert any("read again from the source over the link" in m
               for m in said), said
    assert _same()
