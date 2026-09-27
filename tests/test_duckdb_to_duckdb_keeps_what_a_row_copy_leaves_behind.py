"""DuckDB to DuckDB: what a copy of rows leaves behind is named - the
sequences that number new rows, the indexes, keys and views - and a
snapshot of the target is taken before a repair.

Measured before: a DuckDB hop compared rows only. A target whose
sequence was behind the source's (so the application's next insert took
a number a copied row held) and that had none of the source's indexes or
views came back with nothing to say about either.
"""
import duckdb
import pytest

from migkit.config import Endpoint, Hop


def _engine(tmp_path, src, dst):
    from migkit.engines.duckdb import DuckDBEngine
    hop = Hop(name="dd", engine="duckdb", source=Endpoint(host=str(src)),
              target=Endpoint(host=str(dst)), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return DuckDBEngine(hop)


@pytest.fixture
def files(tmp_path):
    src, dst = tmp_path / "src.duckdb", tmp_path / "dst.duckdb"
    with duckdb.connect(str(src)) as c:
        c.execute("create sequence ids start 1; create table t (id int"
                  " primary key default nextval('ids'), v text unique);"
                  " insert into t (v) select 'v' || range from range(50);"
                  " create index by_v on t(v); create view firsts as select"
                  " * from t where id < 10")
    with duckdb.connect(str(dst)) as c:
        c.execute("create sequence ids start 1; create table t (id int"
                  " primary key, v text); insert into t select * from"
                  " (values (1, 'v0'))")
    return src, dst


def test_a_sequence_behind_the_source_is_named(files, tmp_path):
    got = _engine(tmp_path, *files).check_autoinc("main")
    assert got[0].status == "diff", got[0].detail
    assert got[0].detail == "ids src=51 dst=1", got[0].detail
    with duckdb.connect(str(files[1])) as c:
        c.execute("drop table t; drop sequence ids; create sequence ids"
                  " start 51")
    got = _engine(tmp_path, *files).check_autoinc("main")
    assert got[0].status == "ok", got[0].detail


def test_indexes_keys_and_views_left_behind_are_named(files, tmp_path):
    got = _engine(tmp_path, *files).check_deep("main")
    objects = [r for r in got if r.scope == "main objects"][0]
    assert objects.status == "diff", objects.detail
    for name in ("index by_v", "t UNIQUE UNIQUE(v)", "view firsts"):
        assert name in objects.detail, objects.detail
    assert "PRIMARY KEY" not in objects.detail


def test_the_source_file_is_never_written(files, tmp_path):
    """Opened for writing, a source whose last writes were still in its
    log beside it had them checkpointed into it when migkit closed it."""
    src, dst = files
    with duckdb.connect(str(src)) as c:
        c.execute("pragma disable_checkpoint_on_shutdown;"
                  " set wal_autocheckpoint = '1TB'")
        c.execute("insert into t (v) values ('late')")
    wal = src.with_name(src.name + ".wal")
    before = (src.read_bytes(), wal.read_bytes())
    eng = _engine(tmp_path, src, dst)
    eng.check_autoinc("main")
    eng.check_deep("main")
    got = eng.neutral_read("src", "main", "t", [("id", "integer"),
                                                ("v", "text")], None, 100)
    assert "late" in [r[1] for r in got[0]]
    assert (src.read_bytes(), wal.read_bytes()) == before


def test_a_snapshot_is_the_target_as_it_was(files, tmp_path):
    src, dst = files
    point = tmp_path / "point"
    point.mkdir()
    _engine(tmp_path, src, dst).snapshot_state("main", point)
    with duckdb.connect(str(dst)) as c:
        c.execute("delete from t")
    with duckdb.connect(str(point / "dst.duckdb"), read_only=True) as c:
        assert c.execute("select * from t").fetchall() == [(1, "v0")]
        assert c.execute("select count(*) from duckdb_sequences()"
                         " where sequence_name = 'ids'").fetchone()[0] == 1
