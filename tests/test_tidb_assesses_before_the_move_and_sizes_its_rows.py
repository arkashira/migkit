"""What TiDB tells before a move and while it is sized (P0-PARITY), without
a server: the engine's own assessment, counters and row sizes over what a
TiDB catalogue answers.

* both servers' versions are read as TiDB's (`7.5.1` of
  `8.0.11-TiDB-v7.5.1`), held to the minor, and one that would not say is
  unknown, not clean;
* the pre-flight counts the rows a narrower target column has no room for,
  before anything moves;
* an AUTO_INCREMENT column's next id is held past the source's highest
  (parity) and past the target's own (usable), from `SHOW TABLE ...
  NEXT_ROW_ID` because each TiDB server hands out ids from its own cached
  range;
* a table with a large column is sized row by row on a sample, one without
  from its declared widths, and `exact` says which;
* what the move does not do is counted and named. That one is not named in
  `PROVES`: the catalogue statements for sequences, TiFlash replicas and
  placement policies are new and have not run against a TiDB
  (`capabilities.UNPROVED`).
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import mysql
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("assess.versions", "tidb"),
    ("assess.preflight", "tidb"),
    ("verify.identity.sequence-collision", "tidb"),
    ("verify.identity.sequence-parity", "tidb"),
    ("move.sized", "tidb"),
]

EP = Endpoint(host="x", port=1, user="x", password="x")
SRC_VERSION = "8.0.11-TiDB-v7.5.1"


def _norm(sql):
    return " ".join(sql.lower().split())


@pytest.fixture
def server(monkeypatch):
    """A catalogue the test fills: `answers` is a list of (tokens, rows or
    a function of (side, sql, args)), the first whose tokens all appear in
    the statement answering."""
    class S:
        answers = []
        asked = []
    s = S()
    s.answers, s.asked = [], []

    def q(self, side, sql, args=None, fresh=False):
        text = _norm(sql)
        s.asked.append((side, text, tuple(args or ())))
        for tokens, rows in s.answers:
            if all(t in text for t in tokens):
                return rows(side, sql, args) if callable(rows) else rows
        return []
    monkeypatch.setattr(mysql.MySQLEngine, "_q", q)
    monkeypatch.setattr(mysql.MySQLEngine, "_all_tables",
                        lambda self, side, db: ["orders", "docs", "plain"])
    monkeypatch.setattr(TiDBEngine, "databases", lambda self: ["d"])
    return s


def _engine(tmp_path, **kw):
    hop = Hop(name="t", engine="tidb", source=EP, target=EP,
              databases=["d"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return TiDBEngine(hop)


def _version(side, version):
    return (("select version()",),
            lambda s, sql, a: [(version if s == side else SRC_VERSION,
                                "TiDB Server (Apache License 2.0)")])


# ---- the two versions ------------------------------------------------------

def test_the_version_is_tidbs_and_not_the_mysql_it_answers_as():
    assert TiDBEngine._tidb_version("8.0.11-TiDB-v7.5.1") == "7.5.1"
    assert TiDBEngine._tidb_version("5.7.25-TiDB-v6.6.0-alpha") == "6.6.0"
    assert TiDBEngine._tidb_version(None) is None


def test_two_servers_of_one_release_train_match(tmp_path, server):
    server.answers.append((("select version()",),
                           [("8.0.11-TiDB-v7.5.1", "TiDB Server")]))
    rows = {r["item"]: r for r in _engine(tmp_path).assess()}
    assert rows["server version match"]["level"] == "pass"
    assert rows["server version match"]["detail"] == "src 7.5.1 / dst 7.5.1"


def test_a_target_of_another_release_train_is_said(tmp_path, server):
    server.answers.append(
        (("select version()",),
         lambda side, sql, args: [("8.0.11-TiDB-v7.5.1" if side == "src"
                                   else "8.0.11-TiDB-v8.1.0", "TiDB")]))
    rows = {r["item"]: r for r in _engine(tmp_path).assess()}
    assert rows["server version match"]["level"] == "warn"
    assert rows["server version match"]["detail"] == "src 7.5.1 / dst 8.1.0"


def test_a_server_that_would_not_say_is_unknown_not_clean(tmp_path, server):
    def refuse(side, sql, args):
        if side == "dst":
            raise RuntimeError("access denied")
        return [("8.0.11-TiDB-v7.5.1", "TiDB")]
    server.answers.append((("select version()",), refuse))
    rows = _engine(tmp_path).assess()
    said = [r for r in rows if "version" in r["item"]]
    assert said and all(r["level"] == "warn" for r in said), said
    assert any("unknown, not clean" in r["detail"] for r in said)


# ---- the pre-flight --------------------------------------------------------

def test_the_preflight_counts_the_rows_a_narrower_column_has_no_room_for(
        tmp_path, server, monkeypatch):
    monkeypatch.setattr(
        TiDBEngine, "neutral_columns",
        lambda self, side, db, table: [("note", "varchar(200)" if side ==
                                        "src" else "varchar(20)")]
        if table == "orders" else [])
    server.answers.append((("select count(*),",), [(3, 120)]))
    server.answers.append((("select 1",), [(1,)]))
    rows = {r["item"]: r for r in _engine(tmp_path).assess()
            if r["scope"] == "before the move"}
    got = rows["d target capacity"]
    assert got["level"] == "fail", got
    assert "orders.note" in got["detail"] and "3 rows" in got["detail"]
    assert "largest 120" in got["detail"]


def test_what_the_preflight_asks_is_the_wires_and_not_one_server_alone(
        tmp_path):
    assert TiDBEngine.PREFLIGHT == ("_capacity_gaps", "_temporal_meaning",
                                    "_mojibake")


# ---- the counters ----------------------------------------------------------

def _counter(server, src_max, dst_max, next_id):
    server.answers += [
        (("extra like",), [("orders", "id")]),
        (("select max(`id`) from `d`.`orders`",),
         lambda side, sql, args: [(src_max if side == "src" else dst_max,)]),
        (("show table",), [("d", "orders", "id", str(next_id),
                            "AUTO_INCREMENT")]),
    ]


def test_a_next_id_past_both_highest_is_clear(tmp_path, server):
    _counter(server, 100, 100, 5000)
    got = {r.scope: r for r in _engine(tmp_path).check_autoinc("d")}
    assert got["d.orders"].status == "ok"
    assert got["d usable"].status == "ok"


def test_a_next_id_not_past_the_sources_highest_is_a_parity_finding(
        tmp_path, server):
    _counter(server, 100, 20, 21)
    got = {r.scope: r for r in _engine(tmp_path).check_autoinc("d")}
    assert got["d.orders"].status == "diff"
    assert got["d.orders"].category == "identity.sequence-parity"
    assert "alter table `d`.`orders` auto_increment = 101" in \
        got["d.orders"].fix_hint
    # the target's own rows are not run into: 21 is past its highest, 20
    assert got["d usable"].status == "ok"


def test_a_next_id_a_row_already_holds_is_a_collision(tmp_path, server):
    _counter(server, 100, 150, 120)
    got = {r.scope: r for r in _engine(tmp_path).check_autoinc("d")}
    assert got["d usable"].status == "diff"
    assert got["d usable"].category == "identity.sequence-collision"
    assert "orders.id: next id 120 <= highest 150" in got["d usable"].detail
    assert "auto_increment = 151" in got["d usable"].fix_hint


def test_a_counter_that_cannot_be_read_is_an_error_and_not_a_pass(
        tmp_path, server):
    def refuse(side, sql, args):
        raise RuntimeError("table does not exist")
    server.answers += [
        (("extra like",), [("orders", "id")]),
        (("show table",), refuse),
        (("select max(",), [(7,)]),
    ]
    got = {r.scope: r for r in _engine(tmp_path).check_autoinc("d")}
    assert got["d.orders"].status == "error"
    assert got["d.orders"].category == "identity.sequence-parity"


# ---- the rows sized where they lie ------------------------------------------

COLUMNS = [("docs", "id", "bigint", 8, 1000, 3000),
           ("docs", "body", "longtext", 4294967295, 1000, 3000),
           ("plain", "id", "int", 4, 50, 12),
           ("plain", "name", "varchar", 80, 50, 12)]


def test_a_table_with_a_large_column_is_sized_on_every_row_it_holds(
        tmp_path, server):
    """Its thousand rows are fewer than the sample, so each is read and the
    widest is the widest there is."""
    lengths = [(120 + i,) for i in range(999)] + [(4000,)]
    server.answers += [
        (("from information_schema.columns c",), COLUMNS),
        (("coalesce(length(", "from `d`.`docs` limit"), lengths)]
    got = _engine(tmp_path).row_sizes("src", "d")
    assert got["docs"]["largest"] == 4000
    assert got["docs"]["rows"] == 1000
    assert got["docs"]["exact"] is True


def test_a_sample_that_is_not_every_row_never_certifies_the_widest(
        tmp_path, server, monkeypatch):
    """The sample read is all the table was asked for, and the table holds
    more: the widest of it is a lower bound, said as not exact."""
    monkeypatch.setattr(TiDBEngine, "SIZE_SAMPLE", 10)
    server.answers += [
        (("from information_schema.columns c",), COLUMNS),
        (("coalesce(length(", "from `d`.`docs` limit"),
         [(120 + i,) for i in range(10)])]
    got = _engine(tmp_path).row_sizes("src", "d")
    assert got["docs"]["exact"] is False
    assert [a for a in server.asked if "limit 10" in a[1]]


def test_a_table_without_one_is_sized_from_its_declared_widths(
        tmp_path, server):
    server.answers.append((("from information_schema.columns c",), COLUMNS))
    got = _engine(tmp_path).row_sizes("src", "d", tables=["plain"])
    assert list(got) == ["plain"]
    assert got["plain"]["largest"] == 4 + 80 + 2
    assert got["plain"]["exact"] is False
    # and nothing was read from the table itself
    assert not [a for a in server.asked if "`d`.`plain`" in a[1]]


# ---- what the move does not do (not in PROVES) ------------------------------

def test_what_the_move_does_not_do_is_counted_and_named(tmp_path, server):
    server.answers += [
        (("information_schema.statistics",), [("logs",)]),
        (("information_schema.sequences",), [("order_seq",)]),
        (("information_schema.tiflash_replica",), [("orders",)]),
        (("information_schema.placement_policies",),
         lambda side, sql, args: [("hot",), ("cold",)] if side == "src"
         else [("cold",)]),
    ]
    rows = {(r["scope"], r["item"]): r for r in
            _engine(tmp_path)._handwork().rows()}
    keyless = rows[("d", "tables with no primary key or unique index")]
    assert keyless["level"] == "warn" and "logs" in keyless["detail"]
    carried = rows[("d", "objects whose contents no data mover copies")]
    assert "sequences (CREATE SEQUENCE): 1 (order_seq)" in carried["detail"]
    assert "TiFlash replicas: 1 (orders)" in carried["detail"]
    prereq = rows[("instance",
                   "things that must exist on the target before the load")]
    assert prereq["level"] == "fail" and "hot" in prereq["detail"]
    assert "cold" not in prereq["detail"]


def test_a_catalogue_that_cannot_be_read_is_unknown_and_never_none(
        tmp_path, server):
    def refuse(side, sql, args):
        raise RuntimeError("no such table: information_schema.sequences")
    server.answers.append((("information_schema.sequences",), refuse))
    rows = _engine(tmp_path)._handwork().rows()
    unknown = [r for r in rows if "UNKNOWN" in r["detail"]]
    assert unknown and "Not zero" in unknown[0]["detail"], rows


def test_the_assessment_carries_the_hand_work(tmp_path, server):
    server.answers.append((("information_schema.sequences",),
                           [("order_seq",)]))
    rows = _engine(tmp_path).assess()
    assert [r for r in rows if r["scope"] == "manual work"]
