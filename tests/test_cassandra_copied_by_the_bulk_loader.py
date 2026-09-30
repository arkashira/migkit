"""A Cassandra table copied by the bulk loader (backlog F7.dsbulk), beside
migkit's own token ranges.

One node holds both keyspaces (the target's under the hop's database
map). A table of one-cell columns goes by the loader: every row arrives,
each cell with the time left to it and its own write time - held by the
same deep check that holds migkit's copier to them - counted on the
target by the loader itself. A table with a collection that is not frozen
goes by migkit's token ranges, with the reason: its cells have no single
time to keep. A copy stopped after its read-out goes on from the rows
read out - kept in a directory of this user's only - and does not read the
source again. Skipped where the loader is not installed:
`migkit doctor --install` puts it in place.
"""
import os
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker

pytestmark = [needs_docker, pytest.mark.skipif(
    not which("dsbulk"), reason="the bulk loader is not installed here")]

NAME, PORT = "migkit-test-dsbulk", 16288


def _session(wait=300):
    from cassandra.cluster import Cluster
    end = time.time() + wait
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", PORT)) != 0:
                    raise OSError("not listening")
            return Cluster(["127.0.0.1"], port=PORT,
                           connect_timeout=10).connect()
        except Exception:
            if time.time() > end:
                raise
            time.sleep(3)


@pytest.fixture(scope="module")
def node():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:9042", "-e", "MAX_HEAP_SIZE=512M",
                    "-e", "HEAP_NEWSIZE=128M", "cassandra:4.1"], check=True,
                   capture_output=True)
    try:
        s = _session()
        for ks in ("shop", "shop_copy"):
            s.execute(f"create keyspace if not exists {ks} with replication"
                      " = {'class': 'SimpleStrategy', 'replication_factor':"
                      " 1}")
        s.execute("create table shop.orders (id int, line int, sku text,"
                  " qty int, price decimal, note text, primary key (id,"
                  " line))")
        s.execute("create table shop.carts (id int primary key, items"
                  " list<text>, owner text)")
        ins = s.prepare("insert into shop.orders (id, line, sku, qty,"
                        " price, note) values (?, ?, ?, ?, ?, ?) using ttl ?"
                        " and timestamp ?")
        from decimal import Decimal
        for i in range(3000):
            s.execute(ins, (i // 3, i % 3, f"sku{i}", i, Decimal(f"{i}.25"),
                            "" if i % 7 == 0 else None if i % 5 == 0
                            else f"n{i}", 86_400 if i % 2 else 0,
                            1_700_000_000_000_000 + i))
        for i in range(50):
            s.execute("insert into shop.carts (id, items, owner) values"
                      " (%s, %s, %s)", (i, [f"a{i}", f"b{i}"], f"o{i}"))
        yield s
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _engine(tmp_path):
    from migkit.engines.cassandra import CassandraEngine
    ep = Endpoint(host="127.0.0.1", port=PORT)
    hop = Hop(name="dsbulk", engine="cassandra", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_copy"})
    hop.report_dir = lambda db=None: tmp_path / (db or "")
    return CassandraEngine(hop)


def _rows(s, ks, table):
    return sorted(tuple(r) for r in s.execute(f"select * from {ks}.{table}"))


def test_a_table_of_single_cells_goes_by_the_loader(node, tmp_path):
    eng = _engine(tmp_path)
    said = []
    eng.native_bulk("shop", ["orders", "carts"], True, said.append)
    assert any("orders: 3,000 rows copied by the bulk loader" in m
               for m in said), said
    assert any(m.startswith("carts:") and "token ranges" in m
               for m in said), said
    assert any("hold many cells each" in m for m in said), said
    for t in ("orders", "carts"):
        assert _rows(node, "shop", t) == _rows(node, "shop_copy", t), t
    times = [r for r in eng.check_deep("shop") if "orders" in r.scope]
    assert times and all(r.status == "ok" for r in times), \
        [(r.scope, r.status, r.detail) for r in times]
    # the rows read out are gone once the table is proved
    assert not (tmp_path / "shop" / "bulk-loader" / "orders").exists()


def test_a_copy_stopped_after_its_read_out_goes_on_from_it(node,
                                                             tmp_path,
                                                             monkeypatch):
    from migkit import dsbulk
    eng = _engine(tmp_path)
    real = dsbulk._run
    calls = []

    def stop_the_load(argv, conf, log):
        calls.append(argv[1])
        if argv[1] == "load" and calls.count("load") == 1:
            raise KeyboardInterrupt
        return real(argv, conf, log)
    monkeypatch.setattr(dsbulk, "_run", stop_the_load)
    with pytest.raises(KeyboardInterrupt):
        dsbulk.copy_table(eng, "shop", "orders", 2, None)
    work = tmp_path / "shop" / "bulk-loader" / "orders"
    assert (work / "unloaded.json").exists()
    assert os.stat(work).st_mode & 0o077 == 0
    moved = dsbulk.copy_table(eng, "shop", "orders", 2, None)
    assert moved == 3000
    # read out once: the second run went on from the files
    assert calls.count("unload") == 1, calls
    assert _rows(node, "shop", "orders") == _rows(node, "shop_copy",
                                                  "orders")
