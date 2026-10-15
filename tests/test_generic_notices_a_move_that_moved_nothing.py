"""The generic engine's guard: a table with rows on the source and none on
the target is named, a side that cannot be read is no answer, and every
cell of its capability row is decided."""
from migkit import capabilities
from migkit.capabilities import NOT_APPLICABLE, NOT_YET
from migkit.engines.generic import GenericEngine


class Conn:
    def __init__(self, rows, broken=False):
        self.rows, self.broken, self.asked = rows, broken, []

    def query(self, sql, res_type):
        self.asked.append(sql)
        if self.broken:
            raise RuntimeError("permission denied")
        table = sql.rsplit(" ", 1)[-1]
        return self.rows.get(table, 0)


def engine(src, dst):
    eng = GenericEngine.__new__(GenericEngine)
    eng._tables = lambda: ["a", "b", "c"]
    eng._from = lambda conn, t: t
    eng._conns = {"src": src, "dst": dst}
    eng._connect = lambda side: eng._conns[side]
    closed = []
    eng._close = lambda: closed.append(True)
    return eng, closed


def test_a_table_the_source_fills_and_the_target_lacks_is_named():
    src = Conn({"a": 5, "b": 0, "c": 7})
    dst = Conn({"a": 5, "b": 0, "c": 0})
    eng, closed = engine(src, dst)
    assert eng.moved_nothing("db") == ["c"]
    assert all(q.startswith("SELECT COUNT(*) FROM") for q in src.asked)
    assert closed == [True]


def test_a_side_that_cannot_be_read_is_no_answer_never_an_empty_list():
    eng, closed = engine(Conn({"a": 5}), Conn({}, broken=True))
    assert eng.moved_nothing("db") is None
    assert closed == [True]


def test_every_generic_capability_is_decided():
    row = capabilities.matrix()["generic"]
    assert row["guard"] == "yes"
    assert "undeclared" not in row.values()
    for cap in ("sequences", "params", "users", "statistics", "snapshot",
                "bulk-move"):
        assert row[cap] == NOT_APPLICABLE
        assert "standard driver" in capabilities.GAPS["generic"][cap][1]
    for cap in ("table-copy", "stream", "fence", "confirm", "delta"):
        assert row[cap] == NOT_YET
        assert capabilities.GAPS["generic"][cap][1] == "R13-GENERIC"
