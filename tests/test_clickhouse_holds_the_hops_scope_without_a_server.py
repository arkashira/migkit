"""ClickHouse reads the hop's scope in every path it has, and its findings
say what they are about (P0-PARITY.scope, P0-PARITY.verify).

No server: the catalogue reads are answered in this process, as the deep
checks' scope tests answer every engine's. What is held here is what
migkit decides before and after a server answers -

* the tables a listing, a copy, a repair and a follow take are the hop's
  (`Engine.in_scope`, `_drills_in_scope`, `movers.stream_cannot_carry`);
* a table the hop filters, renames or maps is compared row by row through
  the rule, never by the fingerprints that read none, and a table made
  under the source's name is not made for one the hop renames;
* a data finding names its table as the source names it, how the table
  came out, and the rows it counted apart.

The statements these paths send are the ones the server-backed tests
already run (`test_clickhouse_*`); nothing is new here.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import LEFT_OUT, Result
from migkit.engines.clickhouse import ClickHouseEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "clickhouse"),
    ("scope.copy", "clickhouse"),
    ("scope.repair", "clickhouse"),
    ("scope.ops", "clickhouse"),
    ("scope.row-filter", "clickhouse"),
    ("scope.names", "clickhouse"),
    ("verify.table", "clickhouse"),
    ("verify.rows-apart", "clickhouse"),
    ("verify.tables", "clickhouse"),
    ("assess.capacity", "clickhouse"),
]

TABLES = ["audit_log", "lines", "orders"]


def _hop(tmp_path, **kw):
    hop = Hop(name="ch", engine="clickhouse",
              source=Endpoint(host="10.0.0.1", port=8123, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=8123, user="u",
                              password="CHANGE_ME"),
              databases=["app"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.fixture
def server(monkeypatch):
    """Every ClickHouseEngine - the hop's own and the pair's two - reads
    its catalogue from here: `server.tables`, whatever else is asked is
    answered by `server.answer`."""
    class Server:
        tables = TABLES
        reads = []
        columns = {t: [("id", "UInt64"), ("region", "String")]
                   for t in TABLES}
        answer = staticmethod(lambda side, sql, params: [])

        def rows(self, eng, side, db, sql, params=None, settings=None):
            self.reads.append((side, sql))
            if "from system.tables where database = {d:String} and not" \
                    " is_temporary and engine not in" in sql:
                return [(t,) for t in self.tables]
            if "from system.databases" in sql:
                return [(1,)]
            if "from system.columns" in sql and params and "t" in params:
                return [(n, t) for n, t in self.columns[params["t"]]]
            if "select sorting_key from system.tables" in sql:
                return [("id",)]
            return self.answer(side, sql, params)
    s = Server()
    s.reads = []
    monkeypatch.setattr(ClickHouseEngine, "_rows",
                        lambda eng, side, db, sql, params=None,
                        settings=None: s.rows(eng, side, db, sql, params,
                                              settings))
    return s


# ---- the listing, the copy, the repair --------------------------------------

def test_a_listing_is_the_hops_tables(tmp_path, server):
    """`exclude` and `include` are read by the one scope answer, in the
    listing every check, copy and follow starts from."""
    eng = ClickHouseEngine(_hop(tmp_path, exclude=["app.audit_log"]))
    assert eng.neutral_tables("src", "app") == ["lines", "orders"]
    assert ClickHouseEngine(_hop(tmp_path, include=["app.orders"])
                            ).neutral_tables("dst", "app") == ["orders"]
    assert ClickHouseEngine(_hop(tmp_path)).neutral_tables("src", "app") \
        == TABLES


def test_the_copy_lists_only_the_hops_tables(tmp_path, server):
    eng = ClickHouseEngine(_hop(tmp_path, exclude=["app.audit_log"]))
    assert eng.list_move_tables("app") == [("", "lines"), ("", "orders")]
    assert ClickHouseEngine(_hop(tmp_path, include=["app.orders"])
                            ).list_move_tables("app") == [("", "orders")]


def test_a_repair_acts_only_on_the_tables_the_hop_takes(tmp_path, server):
    """A check run before `exclude: [audit_log]` was added left
    `data-audit_log.extra`: the repair would have deleted the target's own
    rows. The plan names the table it leaves alone and carries the rest."""
    (tmp_path / "data-orders.missing").write_text('["1"]\n')
    (tmp_path / "data-audit_log.extra").write_text('["7"]\n')
    eng = ClickHouseEngine(_hop(tmp_path, exclude=["app.audit_log"]))
    plan = eng.repair_plan("app", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    left = [a for a in plan if a.kind == LEFT_OUT]
    assert left and "audit_log" in left[0].note
    assert not left[0].statements and not left[0].undo
    # a hop that takes everything plans both
    whole = ClickHouseEngine(_hop(tmp_path)).repair_plan("app", "rows")
    assert sorted(a.table for a in whole) == ["audit_log", "orders"]


def test_a_row_is_deleted_by_its_key_and_never_without_one(tmp_path,
                                                          monkeypatch):
    """The delete a repair makes, and the undo of a repaired row: typed by
    the key's values as a read by key types them; the target only."""
    sent = []

    class Client:
        def command(self, sql, parameters=None, settings=None):
            sent.append((sql, parameters, settings))

        def close(self):
            pass
    monkeypatch.setattr(ClickHouseEngine, "_client",
                        lambda self, side, db=None, wait=None: Client())
    eng = ClickHouseEngine(_hop(tmp_path))
    eng._apply_delete("dst", "app", "orders", {"id": 7, "region": "apac"})
    sql, params, settings = sent[0]
    assert sql == ("delete from `app`.`orders` where `id` = {k0:Int64} and"
                   " `region` = {k1:String}")
    assert params == {"k0": 7, "k1": "apac"}
    assert settings == {"lightweight_deletes_sync": 2}
    with pytest.raises(SystemExit, match="deleted by its key"):
        eng._apply_delete("dst", "app", "orders", {})
    with pytest.raises(SystemExit, match="on a source"):
        eng._apply_delete("src", "app", "orders", {"id": 1})
    assert len(sent) == 1


# ---- the follow -------------------------------------------------------------

def _followed(monkeypatch):
    from migkit import tailctl
    ran = []
    monkeypatch.setattr(tailctl, "follow", lambda *a, **k: ran.append(a))
    return ran


def test_a_follow_by_partitions_refuses_a_table_that_carries_fewer_changes(
        tmp_path, server, monkeypatch):
    """A partition that differs is copied again whole, a delete with it: it
    cannot leave a kind of change out, so a hop that asks for that is
    refused before anything is followed."""
    ran = _followed(monkeypatch)
    eng = ClickHouseEngine(_hop(
        tmp_path, mapping={"ops": {"app.audit_log": "insert,update"}}))
    with pytest.raises(SystemExit) as stopped:
        eng.tail_apply("app", True, tmp_path / "token.json", print)
    said = str(stopped.value)
    assert "audit_log (it carries only inserts, updates)" in said
    assert "Nothing was followed" in said
    assert ran == []


def test_a_follow_refuses_a_table_it_would_carry_unfiltered_or_by_another_name(
        tmp_path, server, monkeypatch):
    ran = _followed(monkeypatch)
    eng = ClickHouseEngine(_hop(
        tmp_path, mapping={"where": {"orders": "region = 'apac'"},
                           "tables": {"lines": "order_lines"}}))
    with pytest.raises(SystemExit) as stopped:
        eng.tail_apply("app", True, tmp_path / "token.json", print)
    said = str(stopped.value)
    assert "orders (its rows are filtered)" in said, said
    assert "lines (it is renamed on the target)" in said, said
    assert ran == []


def test_a_follow_of_a_hop_that_asks_for_nothing_of_the_kind_goes_on(
        tmp_path, server, monkeypatch):
    ran = _followed(monkeypatch)
    ClickHouseEngine(_hop(tmp_path)).tail_apply(
        "app", True, tmp_path / "token.json", print)
    assert len(ran) == 1


# ---- a table read through a rule --------------------------------------------

def test_a_filtered_table_is_read_through_its_filter_on_both_sides(
        tmp_path, server):
    """The copier hands the filter to every read it makes of the table
    (`where`), and the pair translates it into the target's SQL under the
    target's names."""
    hop = _hop(tmp_path, mapping={"where": {"orders": "region = 'apac'"}})
    eng = ClickHouseEngine(hop)
    cols = [("id", "integer"), ("region", "text")]
    sql, params, key, names = eng._read_query(
        "src", "app", "orders", cols, None, 1000, "region = 'apac'")
    assert " where (region = 'apac')" in sql and key == ["id"]
    pair = eng._as_pair()
    assert pair._row_scope("app", "orders") == ("region = 'apac'",
                                                "region = 'apac'")
    # the other tables are read whole
    assert pair._row_scope("app", "lines") == (None, None)


def test_a_filtered_or_renamed_table_is_compared_by_the_pair_not_the_fingerprints(
        tmp_path, server, monkeypatch):
    """The fingerprints read every row of one name on both servers: a table
    the hop filters, renames or maps compares through the rule, row by
    row. A stale table of the source's name on the target would otherwise
    be compared in its place."""
    from migkit.engines.hetero import HeteroEngine
    fingerprinted, compared = [], []
    monkeypatch.setattr(ClickHouseEngine, "_fingerprints",
                        lambda self, db, t, only=None: (
                            fingerprinted.append(t) or ({}, {})))
    monkeypatch.setattr(
        HeteroEngine, "check_data",
        lambda self, db, t=None, stream=None, with_counts=False: (
            compared.append(t) or [Result("data", f"{db}.{t}", "ok", "same")]))
    eng = ClickHouseEngine(_hop(
        tmp_path, mapping={"where": {"orders": "region = 'apac'"},
                           "tables": {"lines": "order_lines"},
                           "columns": {"audit_log": {"drop": ["id"]}}}))
    got = eng.check_data("app")
    assert sorted(compared) == ["audit_log", "lines", "orders"]
    assert fingerprinted == []
    assert [r.status for r in got] == ["ok"] * 3
    # a table with no rule is fingerprinted, as before
    ClickHouseEngine(_hop(tmp_path)).check_data("app", "orders")
    assert fingerprinted == ["orders"]


def test_a_table_the_hop_renames_is_not_made_under_the_sources_name(
        tmp_path, server, monkeypatch):
    """`create_missing` makes a table as the source made it, except one the
    hop renames or maps - the pair builds that one, under its new name."""
    made, by_pair = [], []
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(ClickHouseEngine, "_made_from_source",
                        lambda self, db, t: made.append(t))
    monkeypatch.setattr(ClickHouseEngine, "prepare_target",
                        lambda self, db: None)
    monkeypatch.setattr(HeteroEngine, "create_missing",
                        lambda self, db, log=None: by_pair.append(db))

    def answer(side, sql, params):
        if "select engine from system.tables" in sql:
            return [("MergeTree",)]
        return []
    server.answer = answer
    server.tables = ["lines", "orders"]
    # nothing on the target yet
    real = server.rows

    def rows(eng, side, db, sql, params=None, settings=None):
        if side == "dst" and "engine not in" in sql:
            return []
        return real(eng, side, db, sql, params, settings)
    server.rows = rows
    ClickHouseEngine(_hop(
        tmp_path, mapping={"tables": {"lines": "order_lines"}})
    ).create_missing("app")
    assert made == ["orders"] and by_pair == ["app"]


def test_the_delta_leaves_a_table_it_cannot_read_through_its_rule_to_the_check(
        tmp_path, server):
    server.answer = lambda side, sql, params: []
    eng = ClickHouseEngine(_hop(
        tmp_path, mapping={"where": {"orders": "region = 'apac'"}}))
    got = {r.scope: r for r in eng.delta_verify("app")}
    assert got["app.orders"].status == "error"
    assert "row filter" in got["app.orders"].detail
    # the baseline did not move on a table nobody compared
    assert not (tmp_path / "delta-parts.json").exists()


# ---- what a finding says ----------------------------------------------------

def test_a_data_finding_names_its_table_how_it_came_out_and_the_rows_apart(
        tmp_path, server, monkeypatch):
    from migkit.engines.hetero import HeteroEngine

    def fingerprints(self, db, t, only=None):
        if t == "lines":
            return {"p": (5, "h")}, {"p": (5, "h")}
        raise RuntimeError("a side the server will not sum")
    monkeypatch.setattr(ClickHouseEngine, "_fingerprints", fingerprints)
    monkeypatch.setattr(
        HeteroEngine, "check_data",
        lambda self, db, t=None, stream=None, with_counts=False: [Result(
            "data", f"{db}.{t}", "diff",
            "1 missing on the target (3); 2 with different values (5, 8);"
            " missing=1 extra=0 changed=2")])
    eng = ClickHouseEngine(_hop(tmp_path, exclude=["app.audit_log"]))
    got = {r.scope: r for r in eng.check_data("app")}
    ok, diff = got["app.lines"], got["app.orders"]
    assert (ok.table, ok.tables, ok.rows_apart) == ("lines",
                                                   {"lines": "ok"}, {})
    assert diff.status == "diff" and diff.table == "orders"
    assert diff.tables == {"orders": "diff"}
    assert diff.rows_apart == {"orders": {"missing": 1, "extra": 0,
                                          "changed": 2, "exact": True}}


def test_a_difference_the_walk_did_not_count_claims_no_rows_apart(
        tmp_path, server, monkeypatch):
    """A walk that stopped short says "there may be more" and prints no
    exact triple: nothing is claimed counted."""
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(ClickHouseEngine, "_fingerprints",
                        lambda self, db, t, only=None: ({"p": (1, "a")},
                                                        {"p": (1, "b")}))
    monkeypatch.setattr(
        HeteroEngine, "check_data",
        lambda self, db, t=None, stream=None, with_counts=False: [Result(
            "data", f"{db}.{t}", "diff",
            "20,000 with different values; stopped after 20,000 rows on the"
            " source, so there may be more")])
    got = ClickHouseEngine(_hop(tmp_path)).check_data("app", "orders")
    assert got[0].rows_apart == {} and got[0].table == "orders"
    assert got[0].tables == {"orders": "diff"}


# ---- what the server gives a move ---------------------------------------------

def test_the_servers_room_is_what_the_server_reports(tmp_path, server):
    def answer(side, sql, params):
        if "name = 'max_threads'" in sql:
            return [("auto(16)",)]
        if "metric = 'Query'" in sql:
            return [(3,)]
        if "name = 'max_concurrent_queries'" in sql:
            return [(100,)]
        return []
    server.answer = answer
    got = ClickHouseEngine(_hop(tmp_path)).capacity("dst", "app")
    assert got == {"cpus": 16, "running": 3, "free_connections": 97}
    server.answer = lambda side, sql, params: (_ for _ in ()).throw(
        RuntimeError("no access"))
    assert ClickHouseEngine(_hop(tmp_path)).capacity("dst", "app") == {}
