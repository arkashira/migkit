"""A consolidation verified source by source (`consolidate.slice_verify`):
each source's slice of the target compared by key at the position its
follow kept, a difference read while the follow was behind compared again
once it caught up (`decide.slice_recheck`), a row under the wrong value
named in the verdict of the source it is of, one verdict for the whole;
`prefix` keys held by the hop-key check and refused where two sources'
prefixes meet (`topology.overlaps`). The pg lane for the follow."""
from click.testing import CliRunner

from migkit import decide, hopkeys, topology
from tests import servers
from tests.conftest import needs_docker

pg = servers.fixture("pg")


def test_two_prefixes_meet_only_where_one_begins_the_other():
    a, b = {"t": {"prefix": "eu:"}}, {"t": {"prefix": "us:"}}
    assert topology.overlaps({"a": a, "b": b}) == []
    assert topology.overlaps({"a": a, "c": {"t": {"prefix": "eu:"}}}) == [
        ("t", "a", "c", "eu:")]
    assert topology.common({"prefix": "eu:"}, {"range": ["eu:5", None]}) \
        == "eu:5"
    assert topology.common({"prefix": "eu:"}, {"range": ["f", "g"]}) is None
    assert topology.common({"prefix": "eu:"}, {"range": [1, 9]}) is None


def _conf(**hops):
    return {"hops": {n: {"engine": "postgres", "goal": "consolidate",
                         "source": {"host": "10.0.0.1"},
                         "target": {"host": "10.0.0.9"},
                         "databases": [n], "db_map": {n: "all"},
                         "options": {"consolidate": {
                             "column": "tenant", "keys": "prefix",
                             "value": v, "with": [o for o in hops
                                                  if o != n]}}}
                     for n, v in hops.items()}}


def test_prefix_keys_pass_the_hop_check_and_a_shared_prefix_is_refused():
    errors = [p for p in hopkeys.check(_conf(a="eu", b="us"))
              if p.level == "error"]
    assert errors == [], errors
    got = [str(p) for p in hopkeys.check(_conf(a="eu", b="eu"))
           if p.level == "error"]
    assert any("their keys would be one" in g and "'eu:'" in g
               for g in got), got
    got = [str(p) for p in hopkeys.check(_conf(a="e:u", b="us"))]
    assert any("holds ':'" in g for g in got), got
    got = [str(p) for p in hopkeys.check(_conf(a="x" * 70, b="us"))]
    assert any("widens a led key column by 64" in g for g in got), got


def test_a_recheck_runs_only_while_differences_fall_and_a_follow_is_there():
    def path(**f):
        return decide.slice_recheck(dict({"unsettled": 3, "catch_up": True,
                                          "before": None}, **f)).path
    assert path() == "recheck"
    assert path(unsettled=0) == "settle"
    assert path(catch_up=False) == "settle"
    assert path(before=2) == "settle"
    assert path(before=5) == "recheck"


def _psql(s, sql, db="postgres"):
    got = s.exec("psql", "-U", s.user, "-d", db, "-At", "-v",
                 "ON_ERROR_STOP=1", "-c", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@needs_docker
def test_two_sources_written_during_the_follow_are_each_verified(
        pg, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit.engines import get_engine
    src, dst = pg.src, pg.dst
    for db in ("w17k_a", "w17k_b"):
        _psql(src, f"drop database if exists {db}")
        _psql(src, f"create database {db}")
        _psql(src, "create table items (id int primary key, name text);"
                   " insert into items select g, 'i' || g from"
                   " generate_series(1, 30) g", db)
    _psql(dst, "drop database if exists w17k_all")
    _psql(dst, "create database w17k_all")
    side = "{{host: {h}, port: {p}, user: {u}, password: {w}}}"
    path = tmp_path / "hops.yaml"
    path.write_text(
        "hops:\n  c:\n    engine: postgres\n    goal: consolidate\n"
        f"    source: {side.format(h=src.host, p=src.port, u=src.user, w=src.password)}\n"
        f"    target: {side.format(h=dst.host, p=dst.port, u=dst.user, w=dst.password)}\n"
        "    databases: [w17k_a, w17k_b]\n"
        "    db_map: {w17k_a: w17k_all, w17k_b: w17k_all}\n"
        "    options:\n      follow_until: caught_up\n"
        "      consolidate: {column: tenant_id, values: {w17k_a: a,"
        " w17k_b: b}, keys: tenant}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    from migkit import cli, consolidate

    def run(*argv):
        got = CliRunner().invoke(cli.main, list(argv))
        assert got.exit_code == 0, got.output
    try:
        run("move", "c", "--mode", "full+cdc", "--db", "w17k_a", "--go")
        # written past the follow's position: the target is behind both
        _psql(src, "insert into items values (31, 'new'); update items set"
                   " name = 'x' where id = 1; delete from items where id = 2",
              "w17k_a")
        _psql(src, "update items set name = 'y' where id = 3", "w17k_b")
        calls = []

        def catch_up():
            if not calls:   # more lands while the verify reads
                _psql(src, "insert into items values (32, 'late')",
                      "w17k_b")
            calls.append(1)
            run("move", "c", "--mode", "cdc", "--db", "w17k_a", "--go")
        hop = cfg.get_hop("c")
        got = consolidate.slice_verify(hop, get_engine(hop), "w17k_a",
                                       catch_up)
        assert calls and got.rounds >= 2, got
        assert set(got.per_source) == {"c.w17k_a", "c.w17k_b"} or len(
            got.per_source) == 2, got.per_source
        assert all(d.status == "same" for d in got.per_source.values()), [
            (r.scope, r.status, r.detail) for r in got.results]
        assert got.whole.status == "same", got.whole
        assert all("at its follow's position" in r.detail
                   for r in got.results if r.status == "ok"
                   and "tenant_id=" in r.scope)
        # a's row 31 put under b on the target: named in a's verdict
        _psql(dst, "update items set tenant_id = 'b' where tenant_id = 'a'"
                   " and id = 31", "w17k_all")
        got = consolidate.slice_verify(hop, get_engine(hop), "w17k_a")
        mine = [r for r in got.results if "tenant_id=a" in r.scope]
        assert mine and mine[0].status == "diff", mine
        assert "(31) is on the target under tenant_id = 'b', not 'a'" in \
            mine[0].detail, mine[0].detail
        theirs = [r for r in got.results if "tenant_id=b" in r.scope]
        assert "rows the source does not have ((31))" in theirs[0].detail
        assert got.whole.status != "same"
    finally:
        for db in ("w17k_a", "w17k_b"):
            _psql(src, f"drop database if exists {db} with (force)")
        _psql(dst, "drop database if exists w17k_all with (force)")
