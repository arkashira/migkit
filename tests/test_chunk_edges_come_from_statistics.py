"""Chunk edges (backlog F7.edges): a table's ranges are cut from its
statistics while they hold, from a sample of the live key when they are
stale, coarse or not about the rows asked for, and from the key in order
last - never from the key's minimum and maximum divided - and refined
from the rows the finished ranges held."""
import pytest

from migkit import decide, ranges


def quote(name):
    return f'"{name}"'


class Server:
    """A fake source answering the statements `ranges.cut` asks."""

    def __init__(self, stats=None, sample=None, keys=None):
        self.stats, self.sample, self.keys = stats or [], sample, keys
        self.asked = []

    def __call__(self, sql):
        self.asked.append(sql)
        if "pg_stats" in sql:
            return self.stats
        if "tablesample" in sql or "rand()" in sql:
            if isinstance(self.sample, Exception):
                raise self.sample
            return [(v,) for v in self.sample or ()]
        if "row_number()" in sql:
            return [(v,) for v in self.keys or ()]
        raise AssertionError(sql)

    def way(self, needle):
        return [s for s in self.asked if needle in s]


def histogram(n_buckets, modified, typ="bigint", step=1000):
    return [(str(i * step), i + 1, modified, typ)
            for i in range(n_buckets + 1)]


def run(server, rows=100_000, every=10_000, kind="postgresql", **kw):
    said, entry = [], {}
    got = ranges.cut(server, kind, quote, "public.t", "id", rows, every,
                     into=entry, log=said.append, label="t", **kw)
    return got, said, entry


def test_fresh_statistics_cut_the_ranges_and_no_key_is_read():
    server = Server(stats=histogram(100, modified=10))
    got, said, entry = run(server)
    assert got == [i * 10_000 for i in range(1, 10)]
    assert not server.way("row_number") and not server.way("tablesample")
    assert entry["edges_from"] == "statistics" and entry["every"] == 10_000
    assert any("statistics" in m for m in said)


def test_statistics_the_table_moved_past_half_a_range_are_not_trusted():
    server = Server(stats=histogram(100, modified=5_001),
                    sample=list(range(0, 100_000, 50)))
    got, said, entry = run(server)
    assert entry["edges_from"] == "sample"
    assert len(got) == 9 and got == sorted(got)
    assert any("5,001" in m and "not trusted" in m for m in said)
    assert not server.way("row_number")


def test_statistics_exactly_half_a_range_behind_still_hold():
    server = Server(stats=histogram(100, modified=5_000))
    assert run(server)[2]["edges_from"] == "statistics"


def test_a_row_filter_skips_the_statistics_that_counted_other_rows():
    server = Server(stats=histogram(100, modified=0),
                    sample=list(range(0, 100_000, 50)))
    _, _, entry = run(server, where="region = 1")
    assert entry["edges_from"] == "sample"
    assert not server.way("pg_stats")
    assert "region = 1" in server.way("tablesample")[0]


def test_statistics_coarser_than_the_ranges_fall_to_a_sample():
    server = Server(stats=histogram(3, modified=0),
                    sample=list(range(0, 100_000, 50)))
    assert run(server)[2]["edges_from"] == "sample"


def test_no_statistics_and_no_sample_dialect_read_the_key_in_order():
    server = Server(keys=[10_000 * i for i in range(1, 10)])
    got, _, entry = run(server, kind=None)
    assert entry["edges_from"] == "key order"
    assert got == [10_000 * i for i in range(1, 10)]


def test_a_sample_that_cannot_run_hands_on_to_the_key_in_order():
    server = Server(sample=RuntimeError("no sampling here"),
                    keys=[10_000 * i for i in range(1, 10)])
    got, said, entry = run(server)
    assert entry["edges_from"] == "key order"
    assert any("could not run" in m for m in said)


def test_a_sample_with_fewer_keys_than_ranges_hands_on():
    server = Server(sample=[1, 2, 3], keys=[10_000 * i for i in range(1, 10)])
    assert run(server)[2]["edges_from"] == "key order"


def test_a_string_key_keeps_the_order_the_server_gave():
    keys = ["b-10", "B-2", "a-9", "a-10", "Z-1", "z-0"]
    server = Server(sample=[k for k in keys for _ in range(40)])
    got, _, _ = run(server, rows=60_000, every=10_000)
    assert got == keys[1:]
    assert got != sorted(got)


def test_integer_edges_are_in_numeric_order_whatever_order_they_came():
    assert ranges.quantile_edges([30, 5, 200, 7, 1000, 12], 3) == [12, 200]
    assert ranges.quantile_edges([1, 2], 5) is None


def test_the_chosen_way_is_the_decision_engines():
    choice = decide.chunk_edges("t", {"sample": True})
    assert choice.path == "sample"
    why = dict((r.name, w) for r, w in choice.passed)
    assert "no statistics" in why["statistics"]
    stale = decide.chunk_edges("t", {"statistics": True, "sample": True,
                                     "statistics stale": "moved 9 rows"})
    assert stale.path == "sample"
    assert "moved 9 rows" in dict(
        (r.name, w) for r, w in stale.passed)["statistics"]


def test_a_histogram_with_a_heavy_value_is_cut_by_rows_not_by_bounds():
    points = [(10, 0.05), (20, 0.10), (30, 0.90), (40, 1.0)]
    assert ranges.cumulative_edges(points, 2) == [30]
    assert ranges.cumulative_edges(points, 9) is None


def test_the_statements_per_dialect_read_the_engines_own_statistics():
    assert "n_mod_since_analyze" in ranges.statistics_sql(
        "postgresql", "public.t", "id")[0]
    assert "dm_db_stats_properties" in ranges.statistics_sql(
        "mssql", "dbo.t", "id")[0]
    assert "COLUMN_STATISTICS" in ranges.statistics_sql(
        "mysql", "`d`.`t`", "id", ("d", "t"))[0]
    assert "all_tab_modifications" in ranges.statistics_sql(
        "oracle", "s.t", "id", ("S", "T"))[0]
    assert ranges.statistics_sql("sqlite", "t", "id") is None


def test_each_dialect_samples_the_live_rows_in_key_order():
    for kind, word in (("postgresql", "tablesample"), ("mysql", "rand()"),
                       ("mssql", "newid()"), ("oracle", "sample (")):
        sql = ranges.sample_sql(kind, quote, "t", "id", 1_000_000, 1_600,
                                where="a = 1")
        assert word in sql and sql.endswith('order by "id"')
        assert "a = 1" in sql
    assert ranges.sample_sql("postgresql", quote, "t", "id", 100, 1_600) \
        is None


def test_a_composite_key_ranges_are_lexicographic():
    got = ranges.range_where(quote, ["a", "b"], ("x", 5), ("y", 9))
    assert got == ('(("a" > \'x\') or ("a" = \'x\' and "b" > 5)) and not'
                   ' (("a" > \'y\') or ("a" = \'y\' and "b" > 9))')
    assert ranges.range_where(quote, "id", None, None) == "1 = 1"
    assert ranges.bounds_sql(quote, "t", ["a", "b"], 10).startswith(
        'select "a", "b" from (select "a", "b", row_number() over (order by'
        ' "a", "b")')


def test_open_ended_ranges_need_no_minimum_or_maximum():
    assert ranges.split_keyed(["m", "t"]) == [(None, "m"), ("m", "t"),
                                             ("t", None)]
    assert ranges.split_keyed([]) == [(None, None)]


def test_a_string_with_a_backslash_is_not_written_into_a_statement():
    with pytest.raises(ValueError):
        ranges.range_where(quote, "id", "a\\b", None)


def plan_of(spans, every=100, done=(), seen=(), held=None):
    return {"ranges": [list(s) for s in spans], "every": every,
            "ranges_done": list(done), "ranges_seen": {str(a): None
                                                       for a in seen},
            "ranges_rows": dict(held or {})}


def test_a_fat_pending_range_is_split_by_the_density_finished_ones_showed():
    st = plan_of([(0, 100), (100, 1100)], held={"0": 100})
    assert ranges.refine(st)
    assert st["ranges"][0] == [0, 100]
    rest = st["ranges"][1:]
    assert len(rest) == 10 and rest[0][0] == 100 and rest[-1][1] == 1100
    assert all(a[1] == b[0] for a, b in zip(rest, rest[1:]))


def test_thin_pending_neighbours_are_merged_up_to_one_share():
    st = plan_of([(0, 100), (100, 120), (120, 140), (140, 160), (160, 400)],
                 held={"0": 100})
    assert ranges.refine(st)
    assert [100, 160] in st["ranges"]
    assert st["ranges"][0] == [0, 100]
    assert st["ranges"][-1][1] == 400


def test_a_range_done_or_begun_is_never_cut_again():
    st = plan_of([(0, 100), (100, 1100), (1100, 2100)], done=[0],
                 seen=[100], held={"0": 100})
    assert ranges.refine(st)
    assert [100, 1100] in st["ranges"]
    assert [0, 100] in st["ranges"]
    assert len(st["ranges"]) > 3


def test_nothing_is_refined_before_a_range_was_measured():
    st = plan_of([(0, 100), (100, 1100)])
    assert not ranges.refine(st)
    assert st["ranges"] == [[0, 100], [100, 1100]]


def test_the_refined_plan_covers_the_same_keys_without_a_gap():
    st = plan_of([(0, 100), (100, 1100), (1100, 1130), (1130, 1150),
                  (1150, 1300)], held={"0": 100})
    assert ranges.refine(st)
    rs = st["ranges"]
    assert rs[0][0] == 0 and rs[-1][1] == 1300
    assert all(a[1] == b[0] for a, b in zip(rs, rs[1:]))


def test_finished_keeps_the_rows_a_range_held():
    st = {"ranges": [[0, 100]], "ranges_done": []}
    ranges.finished(st, 0, lambda: None, rows=87)
    assert st["ranges_rows"] == {"0": 87}
    ranges.finished({"ranges": [[0, 1]], "ranges_done": []}, 0,
                    lambda: None, rows="n/a")
