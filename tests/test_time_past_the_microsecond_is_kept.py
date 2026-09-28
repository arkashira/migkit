"""The digits of a second past the sixth are read, compared and carried
(type-fidelity G3).

Python's date and time types stop at microseconds, and DuckDB's library
reads a `TIMESTAMP_NS` back at six digits - measured, `...00.123456789` as
`...00.123456`. Both sides of a comparison were cut alike, so a target
holding `...00.123456000` against a source holding `...00.123456789`
compared equal, and a DuckDB to DuckDB move that wrote six digits into a
nine-digit column passed its own check.
"""
import duckdb
import pytest

from migkit.config import Endpoint, Hop
from tests.typepair import Checkpoint

NS = ("2024-01-01 00:00:00.123456789", "2024-01-01 00:00:00.000000001",
      "1999-12-31 23:59:59.999999999", "2024-01-01 00:00:00")


def _engine(tmp_path, src, dst):
    from migkit.engines.duckdb import DuckDBEngine
    hop = Hop(name="ns", engine="duckdb", source=Endpoint(host=str(src)),
              target=Endpoint(host=str(dst)), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return DuckDBEngine(hop)


def _nines(path):
    with duckdb.connect(str(path), read_only=True) as c:
        return [r[0] for r in c.execute(
            "select strftime(ts, '%Y-%m-%d %H:%M:%S.%n') from t"
            " order by id").fetchall()]


@pytest.fixture
def files(tmp_path):
    src, dst = tmp_path / "src.duckdb", tmp_path / "dst.duckdb"
    rows = ", ".join(f"({i}, '{v}')" for i, v in enumerate(NS))
    with duckdb.connect(str(src)) as c:
        c.execute("create table t (id int primary key, ts timestamp_ns);"
                  f" insert into t values {rows}")
    return src, dst


def test_a_target_cut_at_the_microsecond_is_a_difference(files, tmp_path):
    src, dst = files
    with duckdb.connect(str(dst)) as c:
        c.execute("create table t (id int primary key, ts timestamp_ns)")
        c.execute(f"attach '{src}' as s (read_only)")
        c.execute("insert into t select id, date_trunc('microsecond', ts)"
                  " from s.t")
    # the premise: the driver reads both the same
    with duckdb.connect(str(src), read_only=True) as a, \
            duckdb.connect(str(dst), read_only=True) as b:
        assert a.execute("select ts from t where id = 0").fetchone() == \
            b.execute("select ts from t where id = 0").fetchone()
    got = [r for r in _engine(tmp_path, src, dst).check_data("main")
           if r.check == "data"]
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]


def test_a_move_keeps_all_nine_digits(files, tmp_path):
    src, dst = files
    eng = _engine(tmp_path, src, dst)
    eng.move_table("main", "", "t", 1000, Checkpoint(), lambda m: None)
    assert _nines(dst) == [v if "." in v else v + ".000000000" for v in NS]
    got = [r for r in _engine(tmp_path, src, dst).check_data("main")
           if r.check == "data"]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


def test_a_target_that_keeps_six_is_refused_before_the_move(files,
                                                             tmp_path):
    src, dst = files
    with duckdb.connect(str(dst)) as c:
        c.execute("create table t (id int primary key, ts timestamp)")
    with pytest.raises(SystemExit) as got:
        _engine(tmp_path, src, dst).move_table(
            "main", "", "t", 1000, Checkpoint(), lambda m: None)
    said = " ".join(str(got.value).split())
    assert "ts: 3 rows hold more than 6 digits of a second" in said, said
    assert "id 0, 1, 2" in said, said
    with duckdb.connect(str(dst), read_only=True) as c:
        assert c.execute("select count(*) from t").fetchone()[0] == 0
