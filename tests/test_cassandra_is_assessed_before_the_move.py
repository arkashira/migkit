"""A Cassandra hop is assessed before anything is moved (P0-PARITY.all):
which software answers on each side and what it cannot do, both servers'
versions held to each other, each side's schema agreed among its nodes,
the target keyspace's copies, whether the tables can be followed after the
copy, what the move will stop on, what it will not do - counted and
named - and the room the node a side reached can give a move.

No server: the cluster is `tests/cassandra_fake.py`. `system.local`'s
`release_version` and `schema_version`, `system.peers`, `system_views`'
clients and thread pools are statements this engine has not yet sent to a
real cluster, so the cells stand in `capabilities.UNPROVED` and this
module names none in `PROVES`.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

COLS = {"id": "int", "name": "text"}


def _clusters(**kw):
    src, dst = fake.Cluster(**kw), fake.Cluster(**kw)
    src.table("shop", "orders", COLS, ["id"])
    dst.table("shop", "orders", COLS, ["id"])
    return src, dst


def _engine(tmp_path, monkeypatch, src, dst, **kw):
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop)


def _rows(eng):
    return {(r["scope"], r["item"]): r for r in eng.assess()}


def _item(rows, word):
    got = [r for (_, item), r in rows.items() if word in item]
    assert got, [i for _, i in rows]
    return got[0]


# ---- which software, which version -------------------------------------------------

def test_each_side_is_named_with_the_release_it_reports(tmp_path,
                                                        monkeypatch):
    src, dst = _clusters(release="4.1.5")
    dst.release = "5.0.2"
    eng = _engine(tmp_path, monkeypatch, src, dst)
    s, d = eng._brands()
    assert (s.name, s.version) == ("cassandra", "4.1.5")
    assert (d.name, d.version) == ("cassandra", "5.0.2")
    assert eng._server_versions() == ("4.1.5", "5.0.2")
    rows = _rows(eng)
    assert _item(rows, "server version match")["level"] == "warn"


def test_scylladb_is_told_apart_and_its_number_is_not_read_as_its_own(
        tmp_path, monkeypatch):
    src, dst = _clusters(scylla=True, release="3.0.8")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    s, _ = eng._brands()
    assert s.name == "scylladb" and s.version == ""
    assert eng._server_versions() == (None, None)
    row = _item(_rows(eng), "server version match")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def test_a_cluster_that_cannot_be_asked_is_unknown_not_named(tmp_path,
                                                             monkeypatch):
    src, dst = _clusters()
    src.broken.add("system.local")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    s, d = eng._brands()
    assert s.name == "unknown" and d.name == "cassandra"


def test_what_a_cassandra_cluster_cannot_do_is_said_with_its_brand(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst)
    row = _item(_rows(eng), "cannot be relied on for a position")
    assert row["level"] == "warn" and "commit log" in row["detail"]


# ---- readiness --------------------------------------------------------------------------

def test_a_side_whose_nodes_disagree_on_the_schema_fails_the_assessment(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    dst.peers = ["node-1", "node-2"]
    dst.schema_versions = ["v1", "v1", "v2"]
    eng = _engine(tmp_path, monkeypatch, src, dst)
    rows = _rows(eng)
    assert _item(rows, "source schema agreed")["level"] == "pass"
    got = _item(rows, "target schema agreed")
    assert got["level"] == "fail" and "2 schema versions" in got["detail"]


def test_a_target_keyspace_that_is_not_there_needs_its_replication_said(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    dst.keyspaces.clear()
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _item(_rows(eng), "target keyspace replication")
    assert got["level"] == "fail" and "not migkit's to choose" \
        in got["detail"]
    given = _engine(tmp_path, monkeypatch, src, dst)
    given.hop.target.options["replication"] = (
        "{'class': 'SimpleStrategy', 'replication_factor': 3}")
    assert _item(_rows(given), "target keyspace replication")["level"] \
        == "pass"


def test_a_target_that_keeps_one_copy_is_a_warning(tmp_path, monkeypatch):
    src, dst = _clusters()
    dst.replication["replication_factor"] = "1"
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _item(_rows(eng), "target keyspace replication")
    assert got["level"] == "warn" and "1 copy" in got["detail"]


def test_a_source_that_cannot_be_followed_says_why(tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _item(_rows(eng), "tables cannot be followed")
    assert got["level"] == "warn" and "Cassandra keeps its changes" \
        in got["detail"]


def test_a_scylladb_source_with_its_log_on_can_be_followed(tmp_path,
                                                           monkeypatch):
    src, dst = _clusters(scylla=True)
    src.table("shop", "orders_scylla_cdc_log", COLS, ["id"])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _item(_rows(eng), "tables can be followed")
    assert got["level"] == "pass"


# ---- what the move will stop on, found before it starts --------------------------------

def test_a_counter_table_is_found_before_the_move_stops_on_it(tmp_path,
                                                              monkeypatch):
    src, dst = _clusters()
    src.table("shop", "hits", {"id": "int", "n": "counter"}, ["id"])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = eng._prerequisites("shop")
    assert got.status == "diff" and "hits" in got.detail
    rows = _rows(eng)
    pre = [r for r in rows.values() if r["scope"] == "before the move"
           and r["item"] == "shop objects"][0]
    assert pre["level"] == "fail"


def test_a_hop_with_nothing_to_stop_on_passes_the_preflight(tmp_path,
                                                            monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert eng._prerequisites("shop").status == "ok"


# ---- what the move will not do ------------------------------------------------------------

def test_the_work_the_move_will_not_do_is_counted_and_named(tmp_path,
                                                            monkeypatch):
    src, dst = _clusters(scylla=True)
    src.table("shop", "orders_scylla_cdc_log", COLS, ["id"])
    src.table("shop", "hits", {"id": "int", "n": "counter"}, ["id"])
    src.add_function("shop", "double_it", [("a", "int")], "int")
    src.add_view("shop", "recent", "orders")
    src.keyspaces["shop"]["orders"].triggers["audit"] = {"class": "Audit"}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    inv = eng._handwork()
    text = " ".join(r["detail"] for r in inv.rows())
    for word in ("double_it(int)", "recent", "Audit (on orders)", "hits",
                 "orders"):
        assert word in text, word
    rows = _rows(eng)
    assert any("routines, triggers and views a data mover does not carry"
               in item for _, item in rows)


def test_a_kind_that_could_not_be_read_is_unknown_not_none(tmp_path,
                                                           monkeypatch):
    src, dst = _clusters()
    src.broken.add("system_schema.functions")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    text = " ".join(r["detail"] for r in eng._handwork().rows())
    assert "UNKNOWN" in text and "cannot read functions" in text


# ---- the room a side can give ---------------------------------------------------------------

def test_the_room_a_node_gives_is_read_from_its_own_tables(tmp_path,
                                                           monkeypatch):
    src, dst = _clusters()
    src.settings["native_transport_max_concurrent_connections"] = "100"
    src.clients = 30
    src.pools = {"ReadStage": 3, "MutationStage": 4,
                 "Native-Transport-Requests": 2, "CompactionExecutor": 9}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert eng.capacity("src", "shop") == {"free_connections": 70,
                                           "running": 9}


def test_a_limit_the_node_does_not_have_is_left_out_not_guessed(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.settings["native_transport_max_concurrent_connections"] = "-1"
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert "free_connections" not in eng.capacity("src", "shop")
    assert eng.capacity("src", "shop")["running"] == 0
    dst.broken.add("system_views.thread_pools")
    assert "running" not in eng.capacity("dst", "shop")
