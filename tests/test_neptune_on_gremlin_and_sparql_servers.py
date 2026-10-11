"""Amazon Neptune as a side of a pair (backlog P0-CLOUDS), its property
graph against Apache TinkerPop's Gremlin Server (two of them, source and
target) and its RDF store against Apache Jena Fuseki (two datasets),
standing in for a cluster. Neptune's own signature, its openCypher
endpoint and its streams are proved against recorded answers
(`test_the_cloud_native_services_as_sides`) and on a real cluster only.

Vertices and edges copied element for element by id - a set of values a
property holds, an edge's ends - and compared; a value changed on the
target is a difference, and an edge whose vertices never come is refused
by name. Quads copied, the default graph's and a named graph's, and a
quad missing on the target is a difference.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("move.table-copy", "neptune"),
]

pytestmark = needs_docker

GREMLIN = "tinkerpop/gremlin-server:3.7.3"
SRC, SRC_PORT = "migkit-test-neptune-src", 16459
DST, DST_PORT = "migkit-test-neptune-dst", 16458
FUSEKI, FUSEKI_PORT = "migkit-test-neptune-rdf", 16450
FUSEKI_IMAGE = "stain/jena-fuseki:latest"


def _g(port):
    from gremlin_python.driver.driver_remote_connection import \
        DriverRemoteConnection
    from gremlin_python.process.anonymous_traversal import traversal
    conn = DriverRemoteConnection(f"ws://127.0.0.1:{port}/gremlin", "g")
    return conn, traversal().with_remote(conn)


@pytest.fixture(scope="module")
def graphs():
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"127.0.0.1:{port}:8182", GREMLIN], check=True,
                       capture_output=True)
    held = []
    try:
        for port in (SRC_PORT, DST_PORT):
            end = time.time() + 180
            g = None
            while time.time() < end and g is None:
                try:
                    conn, g = _g(port)
                    g.V().limit(1).to_list()
                    held.append(conn)
                except Exception:
                    g = None
                    time.sleep(2)
            assert g is not None
        yield
    finally:
        for c in held:
            c.close()
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture(scope="module")
def fuseki():
    import urllib.request
    subprocess.run(["docker", "rm", "-f", "-v", FUSEKI], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", FUSEKI, "-e",
                    "ADMIN_PASSWORD=migkit", "-e", "FUSEKI_DATASET_1=src",
                    "-e", "FUSEKI_DATASET_2=dst", "-p",
                    f"127.0.0.1:{FUSEKI_PORT}:3030", FUSEKI_IMAGE],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        ready = False
        while time.time() < end and not ready:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{FUSEKI_PORT}/$/ping", timeout=3)
                ready = True
            except Exception:
                time.sleep(2)
        assert ready
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", FUSEKI],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _graph_engine(tmp_path):
    from migkit.engines.neptune import NeptuneEngine
    hop = Hop(name="nep", engine="neptune",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              options={"tls": False}),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              options={"tls": False}),
              databases=["graph"])
    hop.report_dir = lambda db=None: tmp_path
    return NeptuneEngine(hop)


def _rdf_engine(tmp_path):
    from migkit.engines.neptune import NeptuneEngine
    base = f"http://127.0.0.1:{FUSEKI_PORT}"

    def ep(ds):
        return Endpoint(host="127.0.0.1", port=FUSEKI_PORT, options={
            "tls": False, "sparql": f"{base}/{ds}/sparql",
            "sparql_update": f"{base}/{ds}/update",
            "rdf_default": "separate"})
    hop = Hop(name="rdf", engine="neptune", source=ep("src"),
              target=ep("dst"), databases=["rdf"])
    hop.report_dir = lambda db=None: tmp_path
    return NeptuneEngine(hop)


def _move(eng, tmp_path, db, tables=None):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    for sch, t in eng.list_move_tables(db):
        if tables is None or t in tables:
            eng.move_table(db, sch, t, 3, ck, lambda m: None)


def _data(eng, db):
    return {r.scope: r.status for r in eng.check_data(db)
            if r.check == "data"}


@pytest.fixture
def people(graphs):
    from gremlin_python.process.traversal import Cardinality, T
    for port in (SRC_PORT, DST_PORT):
        conn, g = _g(port)
        g.V().drop().iterate()
        conn.close()
    conn, g = _g(SRC_PORT)
    for i in range(1, 7):
        (g.addV("person").property(T.id, f"p{i}").property("name", f"n{i}")
         .property(Cardinality.set_, "tag", "a")
         .property(Cardinality.set_, "tag", f"t{i}").iterate())
    for i in range(1, 6):
        (g.V(f"p{i}").addE("knows").to(g.V(f"p{i + 1}").next())
         .property(T.id, f"e{i}").property("since", 2000 + i).iterate())
    conn.close()


def test_vertices_and_edges_are_copied_by_id_and_compared(people,
                                                          tmp_path):
    eng = _graph_engine(tmp_path)
    _move(eng, tmp_path, "graph", ["vertices"])
    _move(eng, tmp_path, "graph", ["edges"])
    got = _data(eng, "graph")
    assert len(got) == 2 and set(got.values()) == {"ok"}, got
    rows = eng.neutral_rows_by_key("dst", "graph", "vertices",
                                   eng.neutral_columns("dst", "graph",
                                                       "vertices"),
                                   ["id"], [("p3",)])
    (row,) = rows.values()
    assert row[2]["tag"] == [["string", "a"], ["string", "t3"]]
    conn, g = _g(DST_PORT)
    g.V("p2").property("name", "renamed").iterate()
    conn.close()
    got = _data(_graph_engine(tmp_path), "graph")
    assert sorted(got.values()) == ["diff", "ok"], got
    assert [s for s, v in got.items() if v == "diff"][0].endswith(
        "vertices"), got


def test_an_edge_whose_vertices_never_come_is_refused_by_name(
        people, tmp_path, monkeypatch):
    from migkit.engines import neptune
    monkeypatch.setattr(neptune, "ENDS_WAIT", 1)
    eng = _graph_engine(tmp_path)
    with pytest.raises(SystemExit, match="copy the vertices first"):
        eng._write_edges("dst", [["e1", "knows", "p1", "p2", {}]])


def test_quads_are_copied_graph_by_graph_and_compared(fuseki, tmp_path):
    eng = _rdf_engine(tmp_path)
    for side in ("src", "dst"):
        eng._sparql(side, "clear all", update=True)
    eng._sparql("src", 'insert data { <http://x/a> <http://x/p> "one"@en .'
                       ' <http://x/a> <http://x/q> "5"^^<http://www.w3.org/'
                       '2001/XMLSchema#integer> . graph <http://x/g> {'
                       ' <http://x/b> <http://x/p> <http://x/a> . } }',
                update=True)
    _move(eng, tmp_path, "rdf")
    got = _data(eng, "rdf")
    assert len(got) == 1 and set(got.values()) == {"ok"}, got
    eng._sparql("dst", "delete data { graph <http://x/g> { <http://x/b>"
                       " <http://x/p> <http://x/a> . } }", update=True)
    got = _data(_rdf_engine(tmp_path), "rdf")
    assert list(got.values()) == ["diff"], got
