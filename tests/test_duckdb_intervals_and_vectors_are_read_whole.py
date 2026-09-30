"""DuckDB's intervals, embeddings and lists read and written as the values
they are - through the library itself, which runs in this process, so no
server is started.

Measured on DuckDB 1.5 before: the library hands an `INTERVAL` over as a
Python `timedelta`, a month as 30 days - `1 year 2 months 3 days
04:05:06.5` came back as 423 days and 04:05:06.5 - so a move wrote
another interval and a comparison of two different ones could meet. Now
an interval is read as its months, days and microseconds, and written
back as them.
"""
import pytest

from migkit import canon, intervals
from migkit.config import Endpoint, Hop


def _engine(path):
    from migkit.engines.duckdb import DuckDBEngine
    ep = Endpoint(host="", port=0, options={"path": str(path)})
    return DuckDBEngine(Hop(name="dk", engine="hetero", source=ep,
                            target=ep, databases=["main"],
                            options={"source_engine": "duckdb",
                                     "target_engine": "duckdb"}))


@pytest.fixture
def db(tmp_path):
    import duckdb
    path = tmp_path / "t.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("create table t (id integer primary key, iv interval,"
                 " e float[3], l bigint[])")
    conn.execute("insert into t values"
                 " (1, interval '1 year 2 months 3 days 04:05:06.5',"
                 # the literal -0.0 is read as 0; its text keeps the sign
                 " [0.1, '-0.0'::float, 2.5]::float[3], [1, null, 3]),"
                 " (2, interval '-1 month 30 days', null, []),"
                 " (3, null, null, null)")
    conn.close()
    return path


def test_an_interval_is_read_as_its_three_parts(db):
    eng = _engine(db)
    cols = [("id", "integer"), ("iv", "interval"), ("e", "vector"),
            ("l", "json")]
    rows, _ = eng.neutral_read("src", "main", "t", cols)
    got = {r[0]: r for r in rows}
    assert tuple(intervals.of(got[1][1])) == (14, 3, 14706500000000)
    # a month and thirty days are two values, not one
    assert tuple(intervals.of(got[2][1])) == (-1, 30, 0)
    assert canon.render_value("interval", got[2][1]) == "P-1M30DT0S"
    assert got[3][1] is None
    assert canon.render_value("vector", got[1][2]) \
        == "[0.10000000149011612,-0.0,2.5]"
    assert canon.render_value("json", got[1][3]) == "[1, null, 3]"
    assert canon.render_value("json", got[2][3]) == "[]"


def test_an_interval_is_written_back_as_the_same_three_parts(db, tmp_path):
    import duckdb
    eng = _engine(db)
    cols = [("id", "integer"), ("iv", "interval"), ("e", "vector"),
            ("l", "json")]
    rows, _ = eng.neutral_read("src", "main", "t", cols)
    out = tmp_path / "out.duckdb"
    conn = duckdb.connect(str(out))
    conn.execute("create table t (id integer primary key, iv interval,"
                 " e float[3], l bigint[])")
    conn.close()
    _engine(out).neutral_write("dst", "main", "t", cols, rows)
    conn = duckdb.connect(str(out))
    got = conn.execute("select id, datepart('month', iv), datepart('day',"
                       " iv), datepart('microseconds', iv), e::varchar,"
                       " l::varchar from t order by id").fetchall()
    conn.close()
    assert got[0] == (1, 2, 3, 6500000, "[0.1, -0.0, 2.5]", "[1, NULL, 3]")
    assert got[1] == (2, -1, 30, 0, None, "[]")
    again, _ = _engine(out).neutral_read("src", "main", "t", cols)
    assert [[canon.render_value(c, v) for (_, c), v in zip(cols, r)]
            for r in again] == [[canon.render_value(c, v)
                                 for (_, c), v in zip(cols, r)]
                                for r in rows]
