"""Pure-logic unit tests, no database or docker needed."""
import textwrap

from migkit.config import _endpoint, load_hops
from migkit.engines import ALIASES, get_engine


def test_endpoint_flattens_nested_options():
    ep = _endpoint("sqlite", {"options": {"path": "/tmp/x.db"}})
    assert ep.options["path"] == "/tmp/x.db"
    assert ep.configured()


def test_endpoint_default_port():
    assert _endpoint("postgres", {"host": "h"}).port == 5432
    assert _endpoint("mysql", {"host": "h"}).port == 3306
    assert _endpoint("mongodb", {"host": "h"}).port == 27017


def test_endpoint_configured_variants():
    assert _endpoint("postgres", {"host": "h"}).configured()
    assert _endpoint("mongodb", {"hosts": "a:1,b:1"}).configured()
    assert _endpoint("generic", {"url": "x://y"}).configured()
    assert not _endpoint("postgres", {}).configured()


def test_hop_loads_and_scopes(tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent("""
        hops:
          h1:
            engine: postgres
            source: {host: s, user: u, password: p}
            target: {host: t, user: u, password: p}
            databases: [a, b]
            workers: 3
    """))
    hops = load_hops(conf)
    assert set(hops) == {"h1"}
    assert hops["h1"].databases == ["a", "b"]
    assert hops["h1"].workers == 3
    assert hops["h1"].source.host == "s"


def test_aliases_resolve():
    assert ALIASES["mariadb"] == "mysql"
    assert ALIASES["documentdb"] == "mongodb"
    assert ALIASES["aurora-postgres"] == "postgres"


def _fake_hop(engine):
    from migkit.config import Endpoint, Hop
    return Hop(name="t", engine=engine,
               source=Endpoint(host="s", user="u", password="p"),
               target=Endpoint(host="t", user="u", password="p"))


def test_every_engine_instantiates():
    for e in ("postgres", "mysql", "mssql", "mongodb", "sqlite",
              "redis", "kafka"):
        eng = get_engine(_fake_hop(e))
        assert eng.checks
        assert hasattr(eng, "check_counts")


def test_hetero_rejects_unbuilt_pair():
    from migkit.config import Endpoint, Hop
    hop = Hop(name="t", engine="hetero",
              source=Endpoint(host="s", user="u", password="p"),
              target=Endpoint(host="t", user="u", password="p"),
              options={"source_engine": "oracle", "target_engine": "mysql"})
    try:
        get_engine(hop)
        assert False, "should reject unbuilt pair"
    except SystemExit:
        pass


def test_report_renders_without_findings():
    from migkit.report import render
    hop = _fake_hop("postgres")
    html = render(hop, [{"check": "counts", "scope": "db1", "status": "ok",
                         "detail": "rows 5==5"}])
    assert "PASS" in html
    assert "db1" in html


def test_report_flags_diff():
    from migkit.report import render
    hop = _fake_hop("postgres")
    html = render(hop, [{"check": "data", "scope": "db1", "status": "diff",
                         "detail": "missing=3"}])
    assert "FAIL" in html
    assert "missing=3" in html


def test_mysql_ddl_type_mapping():
    from migkit.engines.hetero import HeteroEngine
    import re
    sql = "CREATE TABLE t (id INT AUTO_INCREMENT, v DOUBLE, b TINYINT(1))"
    for pat, rep in HeteroEngine.TYPE_FIX:
        sql = re.sub(pat, rep, sql, flags=re.I)
    assert "DOUBLE PRECISION" in sql
    assert "DOUBLE PRECISION PRECISION" not in sql
    assert "IDENTITY" in sql
    assert "BOOLEAN" in sql
