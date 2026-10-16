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


# --- keys that are not whole numbers, read by the copiers (backlog F7.edges)

import base64
import json


def _again(st):
    return json.loads(json.dumps(st))


def test_a_text_key_is_planned_from_its_edges_alone_and_resumes():
    st, saved = {}, []
    todo = ranges.plan(st, None, None, lambda: ["g", "p"],
                       lambda: saved.append(1), table="t")
    assert todo == [(None, "g"), ("g", "p"), ("p", None)] and saved
    ranges.started(st, None, "m1", lambda: None)
    assert st["ranges_seen"] == {"null": "m1"}
    ranges.finished(st, None, lambda: None, rows=40)
    assert st["ranges_rows"] == {"null": 40}
    back = _again(st)
    again = ranges.plan(back, None, None, lambda: 1 / 0, lambda: None)
    assert again == [("g", "p"), ("p", None)]
    ranges.finished(back, "g", lambda: None)
    assert ranges.plan(_again(back), None, None, lambda: 1 / 0,
                       lambda: None) == [("p", None)]


def test_a_composite_key_range_survives_the_checkpoint_file():
    st = {}
    todo = ranges.plan(st, None, None, lambda: [(1, "a"), (2, "b")],
                       lambda: None)
    assert todo == [(None, (1, "a")), ((1, "a"), (2, "b")), ((2, "b"), None)]
    ranges.finished(st, todo[0][0], lambda: None)
    ranges.finished(st, todo[1][0], lambda: None)
    back = _again(st)
    assert ranges.plan(back, None, None, lambda: 1 / 0, lambda: None) == [
        ((2, "b"), None)]
    assert back["last"] == [2, "b"]


def test_an_edge_that_cannot_be_written_is_refused_before_a_range_begins():
    for bad in (b"\x01", "a\\b"):
        st = {}
        with pytest.raises(ValueError):
            ranges.plan(st, None, None, lambda: ["a", bad], lambda: None)
        assert "ranges" not in st


def test_dates_and_uuids_are_kept_as_the_text_the_server_wrote():
    import datetime
    import uuid
    st = {}
    ranges.plan(st, None, None, lambda: [datetime.date(2024, 1, 2),
                                         uuid.UUID(int=5)], lambda: None)
    assert st["ranges"][0] == [None, "2024-01-02"]
    assert st["ranges"][1][1] == str(uuid.UUID(int=5))
    json.dumps(st)


def test_one_predicate_reads_a_range_of_any_key():
    assert ranges.pred(quote, "id", (5, 10)) == '"id" > 5 and "id" <= 10'
    assert ranges.pred(quote, "id", (None, 10)) == '"id" <= 10'
    assert ranges.pred(quote, "id", (None, None)) == "1 = 1"
    assert ranges.pred(quote, "id", (5, 10), where="r = 1") == \
        '("id" > 5 and "id" <= 10) and (r = 1)'
    text = ranges.pred(quote, "name", ("g", "p"))
    assert text == ranges.range_where(quote, "name", "g", "p")
    assert "'g'" in text and "'p'" in text and "not" in text
    assert "<=" not in text
    composite = ranges.pred(quote, ["a", "b"], ([1, "x"], None))
    assert composite == ranges.range_where(quote, ["a", "b"], (1, "x"), None)
    assert ranges.describe("id", (4, 10)) == "id 5 to 10"
    assert ranges.describe("name", (None, "g")) == "name up to g"
    assert ranges.describe("name", ("p", None)) == "name after p"


def test_every_row_of_a_text_key_falls_in_one_range():
    edges = ["g", "p"]
    spans = ranges.split_keyed(edges)

    def inside(key, span):
        after, upto = span
        return (after is None or key > after) and (upto is None
                                                  or key <= upto)
    for key in ("a", "g", "h", "p", "q", "zz", ""):
        assert sum(inside(key, s) for s in spans) == 1


def test_the_tail_applies_changes_to_keys_it_cannot_place():
    st = {}
    ranges.plan(st, None, None, lambda: ["g", "p"], lambda: None)
    assert ranges.place(st, ["h", 5]) == [("outside", None)] * 2
    nums = {}
    ranges.plan(nums, None, None, lambda: [10, 20], lambda: None)
    assert ranges.place(nums, [5, 10, 11, 25]) == [("todo", None)] * 4
    ranges.started(nums, None, "m", lambda: None)
    ranges.finished(nums, None, lambda: None)
    got = ranges.place(nums, [5, 25])
    assert got == [("read", "m"), ("todo", None)]


def test_layout_writes_the_plan_a_copier_keeps_itself():
    assert ranges.layout(1, 100, [50]) == [[0, 50], [50, 100]]
    assert ranges.layout(None, None, ["g", "p"]) == [
        [None, "g"], ["g", "p"], ["p", None]]


def test_edges_from_a_source_that_speaks_no_sql():
    said = []
    got = ranges.cut_with(
        1_000, 250, lambda: ["b", "d", "f"],
        sample=lambda: [f"k{i:03d}" for i in range(64)], log=said.append)
    assert len(got) == 3 and got == sorted(got)
    assert all(k.startswith("k") for k in got)
    only = ranges.cut_with(1_000, 250, lambda: ["b", "d", "f"])
    assert only == ["b", "d", "f"]


# --- the counter trails an analyze (backlog W12J) -------------------------

POINTS = [(i * 1000, i / 100) for i in range(101)]


def sequence(*reads):
    left = list(reads)
    return lambda: left.pop(0) if len(left) > 1 else left[0]


def settle(first, then, **kw):
    waits, said, entry = [], [], {}
    got = ranges.cut_with(
        100_000, 10_000, lambda: [1], into=entry, log=said.append,
        read=sequence(first, then),
        sample=lambda: list(range(0, 100_000, 50)), wait=waits.append, **kw)
    return got, waits, said, entry


def test_a_counter_high_a_moment_after_an_analyze_is_read_again():
    got, waits, said, entry = settle(
        ranges.Stats(POINTS, modified=90_000, analyzed_ago=0.4),
        ranges.Stats(POINTS, modified=3, analyzed_ago=2.1))
    assert entry["edges_from"] == "statistics" and len(got) == 9
    assert waits == [pytest.approx(ranges.COUNTER_LAG - 0.4)]
    assert any("read again" in m for m in said)


def test_a_counter_still_high_after_the_lag_is_believed():
    got, waits, said, entry = settle(
        ranges.Stats(POINTS, modified=90_000, analyzed_ago=0.4),
        ranges.Stats(POINTS, modified=88_000, analyzed_ago=2.1))
    assert entry["edges_from"] == "sample" and len(waits) == 1
    assert any("88,000" in m and "not trusted" in m for m in said)


def test_a_counter_high_long_after_an_analyze_is_not_waited_for():
    _, waits, _, entry = settle(
        ranges.Stats(POINTS, modified=90_000, analyzed_ago=600.0),
        ranges.Stats(POINTS, modified=0, analyzed_ago=600.0))
    assert waits == [] and entry["edges_from"] == "sample"


def test_a_low_counter_after_an_analyze_waits_for_nothing():
    _, waits, _, entry = settle(
        ranges.Stats(POINTS, modified=10, analyzed_ago=0.1),
        ranges.Stats(POINTS, modified=10, analyzed_ago=0.1))
    assert waits == [] and entry["edges_from"] == "statistics"


def test_the_postgresql_reader_carries_the_time_of_the_last_analyze():
    sql = ranges.statistics_sql("postgresql", "public.t", "id")[0]
    assert "last_analyze" in sql and "last_autoanalyze" in sql
    rows = [("0", 1, 7, "bigint", "0.42"), ("1000", 2, 7, "bigint", "0.42")]
    got = ranges.read_statistics("postgresql", lambda q: rows, "public.t",
                                 "id")
    assert got.analyzed_ago == pytest.approx(0.42) and got.modified == 7
    old = [r[:4] for r in rows]
    assert ranges.read_statistics("postgresql", lambda q: old, "public.t",
                                  "id").analyzed_ago is None


# --- MySQL and CockroachDB statistics freshness (backlog R20.11) -----------

def b64(text):
    return "base64:type16:" + base64.b64encode(text.encode()).decode()


def mysql_server(touched, counter):
    hist = json.dumps({"buckets": [
        [b64("a"), b64("f"), 0.25, 10], [b64("g"), b64("m"), 0.5, 10],
        [b64("n"), b64("s"), 0.75, 10], [b64("t"), b64("z"), 1.0, 10]]})

    def ask(sql):
        if "COLUMN_STATISTICS" in sql and "UPDATE_TIME" in sql:
            return [(touched,)]
        if "COLUMN_STATISTICS" in sql:
            return [(hist,)]
        if "INNODB_TABLESTATS" in sql:
            return [(counter,)]
        raise AssertionError(sql)
    return ask


def test_a_text_histogram_bound_is_decoded_into_the_key_it_stands_for():
    got = ranges.read_statistics("mysql", mysql_server(0, 0), "`d`.`t`",
                                 "name", ("d", "t"))
    assert [p[0] for p in got.points] == ["f", "m", "s", "z"]
    assert got.touched is False and got.floor == 0
    edges = ranges.cut(mysql_server(0, 0),
                       "mysql", quote, "`d`.`t`", "name", 2_000, 1_000,
                       names=("d", "t"))
    assert edges == ["m"]


def test_rows_innodb_counted_modified_keep_stale_histograms_out():
    ask = mysql_server(1, 900)
    ask_sample = lambda sql: ([(f"k{i:02d}",) for i in range(40)]  # noqa
                              if "rand()" in sql else ask(sql))
    said, entry = [], {}
    ranges.cut(ask_sample, "mysql", quote, "`d`.`t`", "name", 2_000, 1_000,
               names=("d", "t"), into=entry, log=said.append)
    assert entry["edges_from"] == "sample"
    assert any("at least 900" in m for m in said)
    ask = mysql_server(1, 10)
    ask_sample = lambda sql: ([(f"k{i:02d}",) for i in range(40)]  # noqa
                              if "rand()" in sql else ask(sql))
    entry = {}
    ranges.cut(ask_sample, "mysql", quote, "`d`.`t`", "name", 2_000, 1_000,
               names=("d", "t"), into=entry)
    # written since, and nothing says how much: not trusted either
    assert entry["edges_from"] == "sample"


def cockroach_server(counted, sample=()):
    def ask(sql):
        if "show statistics" in sql:
            return [(77, counted, 3.0)]
        if "show histogram 77" in sql:
            return [("b", 250), ("d", 250), ("f", 250), ("h", 250)]
        if "random()" in sql:
            return [(k,) for k in sample]
        raise AssertionError(sql)
    return ask


def test_cockroachdb_statistics_are_fresh_while_the_rows_still_add_up():
    entry = {}
    got = ranges.cut(cockroach_server(1_000), "cockroachdb", quote, "t",
                     "name", 1_000, 500, into=entry)
    assert got == ["d"] and entry["edges_from"] == "statistics"


def test_cockroachdb_statistics_the_rows_have_left_behind_hand_on():
    entry, said = {}, []
    keys = [f"k{i:03d}" for i in range(64)]
    ranges.cut(cockroach_server(1_000, keys), "cockroachdb", quote, "t",
               "name", 1_700, 500, into=entry, log=said.append)
    assert entry["edges_from"] == "sample"
    assert any("700" in m and "not trusted" in m for m in said)


def test_a_cockroachdb_source_is_read_in_its_own_dialect():
    class CockroachDBEngine:
        pass
    assert ranges.kind_of(CockroachDBEngine()) == "cockroachdb"
    sql = ranges.sample_sql("cockroachdb", quote, "t", "id", 1_000_000, 1_600)
    assert "random()" in sql and "tablesample" not in sql


# --- the MySQL counters a table is skipped on (backlog R20.11) -------------

def test_mysql_counters_are_read_and_a_table_is_judged_on_them():
    from migkit import unchanged
    entry = unchanged.COUNTERS["mysql"]
    assert not entry.owed and "mysql" not in unchanged.UNPROVED
    ok = {"started": 1_760_000_000, "instrument": 1, "read_only": 0}
    assert entry.refusal(ok, "src") == ""
    assert "instrument" in entry.refusal({**ok, "instrument": 0}, "src")
    assert "read-only" in entry.refusal({**ok, "read_only": 1}, "src")
    assert entry.refusal({**ok, "read_only": 1}, "dst") == ""
    assert "start" in entry.refusal({**ok, "started": None}, "src")

    def mark(ins=5, upd=2, dele=1, table_id=88):
        return unchanged.marker([1_760_000_000, ins, upd, dele,
                                 "2026-10-01 00:00:00", table_id, "3:ab"])
    stored = unchanged.record(mark(), mark())
    assert unchanged.judge("mysql", stored, mark(), mark()).skip
    moved = unchanged.judge("mysql", stored, mark(ins=6), mark())
    assert not moved.skip and "rows were inserted (5 -> 6)" in moved.why
    reset = unchanged.judge("mysql", stored, mark(ins=0, upd=0, dele=0),
                            mark())
    assert not reset.skip and "cleared" in reset.why
    emptied = unchanged.judge("mysql", stored, mark(table_id=91), mark())
    assert not emptied.skip and "emptied or rebuilt" in emptied.why
    nothing = unchanged.marker([1, "NULL", 0, 0, "x", 8, "1:a"])
    assert not unchanged.judge("mysql", stored, nothing, mark()).skip
    assert "no row counter" in entry.dead(nothing)


def test_the_modified_counter_statement_quotes_what_it_is_given():
    from migkit import unchanged
    sql = unchanged.COUNTERS["mysql"].modified_sql("d'b", "t")
    assert "'d''b/t'" in sql and "INNODB_TABLESTATS" in sql


def test_cockroachdb_is_still_refused_and_says_what_it_lacks():
    from migkit import unchanged
    why = unchanged.UNPROVED["cockroachdb"]
    assert "no counter of rows modified" in why
    assert not unchanged.judge("cockroachdb", {"src": "a", "dst": "a"},
                               "a", "a").skip
