"""What SQL Server already did, held to the same words as PostgreSQL and
MySQL (P0-PARITY): the keys a scope leaves out are said before the move, a
target ahead of the source is found, a repair names what it will change and
carries its undo, a target is kept from the application and given back
exactly, and a change the hop does not carry is left out and counted.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads. The statements here are the ones that ran on servers
already (`tests/test_sql_server_checks_against_a_server.py`,
`tests/test_a_sql_server_source_is_held_and_given_back.py`); what is held
is the logic around them.
"""
import json

from migkit import canon, freeze, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.base import Result
from migkit.engines.hetero import HeteroEngine, _Accounts
from tests.mssql_stub import Server, engine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("schema.foreign-keys", "mssql"),
    ("verify.movement.target-ahead", "mssql"),
    ("verify.repair", "mssql"),
    ("verify.repair-undo", "mssql"),
    ("cutover.read-only-target", "mssql"),
    ("scope.ops", "mssql"),
]


# ---- the keys a scope leaves out ------------------------------------------

def test_what_the_taken_tables_need_is_said_before_the_move(monkeypatch,
                                                             tmp_path):
    s = Server()
    s.on("sys.foreign_keys f",
         [("fk_o_c", "dbo.orders", "customer_id", "dbo.customers", "id")])
    s.on("select s.name \\+ '\\.' \\+ t.name from sys.tables t",
         [("dbo.orders",), ("dbo.customers",), ("dbo.audit_log",)])
    eng = engine(monkeypatch, tmp_path, s,
                 exclude=["appdb.dbo.customers", "appdb.dbo.audit_log"])
    assert eng.foreign_keys("src", "appdb") == [
        ("dbo.orders", ["customer_id"], "dbo.customers", ["id"])]
    items = [i for i in eng._include_items() if i["item"] == "dependencies"]
    said = " | ".join(i["detail"] for i in items if i["level"] == "warn")
    assert "dbo.orders (customer_id) -> dbo.customers" in said, said
    assert "take appdb.dbo.customers out of exclude" in said
    # a hop that takes the parent too has nothing to say of the key
    eng = engine(monkeypatch, tmp_path, s, exclude=["appdb.dbo.audit_log"])
    items = [i for i in eng._include_items() if i["item"] == "dependencies"]
    assert not [i for i in items if "foreign keys across" in i["detail"]]


# ---- a target ahead of the source ------------------------------------------

def _deep(monkeypatch, tmp_path, server):
    import migkit.workload as workload
    eng = engine(monkeypatch, tmp_path, server)
    eng._pk_cols = lambda db, t: ["id"]
    eng._row_json = lambda db, t: ("x", [])
    monkeypatch.setattr(workload, "compare",
                        lambda e, db: Result("deep", f"{db} performance",
                                             "skip"))
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    return eng


def test_a_target_whose_key_runs_past_the_source_s_is_found(monkeypatch,
                                                              tmp_path):
    s = Server()
    s.on("ty\\.name in \\('int'", [("dbo.ahead", "id"), ("dbo.fine", "id")])
    s.on("select isnull\\(max\\(",
         lambda side, sql: [(3 if side == "src" else
                             100 if "ahead" in sql else 3,)])
    eng = _deep(monkeypatch, tmp_path, s)
    (got,) = [r for r in eng.check_deep("appdb")
              if r.scope == "appdb boundary"]
    assert got.status == "diff" and got.category == "movement.target-ahead"
    assert "AHEAD of source on 1" in got.detail
    assert "dbo.ahead src_max=3 dst_max=100" in got.detail


def test_a_target_behind_the_source_is_lag_not_ahead(monkeypatch, tmp_path):
    s = Server()
    s.on("ty\\.name in \\('int'", [("dbo.t", "id")])
    s.on("select isnull\\(max\\(",
         lambda side, sql: [(10 if side == "src" else 4,)])
    eng = _deep(monkeypatch, tmp_path, s)
    (got,) = [r for r in eng.check_deep("appdb")
              if r.scope == "appdb boundary"]
    assert got.status == "ok" and "1 behind (replication lag)" in got.detail


# ---- a repair that says what it changes and carries its undo ----------------

IDENTITY = "objectproperty\\(t\\.object_id,'TableHasIdentity'\\)=1"


def test_a_counter_repair_carries_its_undo_exactly(monkeypatch, tmp_path):
    s = Server().on(IDENTITY, lambda side, sql: [("dbo.t", 10 if side == "src"
                                                  else 4)])
    eng = engine(monkeypatch, tmp_path, s)
    (action,) = eng.repair_plan("appdb", "sequences")
    assert action.kind == "sequences"
    assert action.statements == [
        "dbcc checkident ('[dbo].[t]', reseed, 10);  -- dst now 4"]
    assert action.undo == ["dbcc checkident ('[dbo].[t]', reseed, 4);"]
    # carried out as the statement alone, its note left out
    eng.apply("appdb", action)
    ran = s.sent("dbcc checkident", "dst")
    assert ran == ["dbcc checkident ('[dbo].[t]', reseed, 10);"]


def test_a_counter_the_target_is_past_is_never_lowered(monkeypatch, tmp_path):
    s = Server().on(IDENTITY, lambda side, sql: [("dbo.t", 4 if side == "src"
                                                  else 10)])
    eng = engine(monkeypatch, tmp_path, s)
    (action,) = eng.repair_plan("appdb", "sequences")
    assert action.statements == [] and action.undo == []
    assert "never lowered" in action.note
    eng.apply("appdb", action)
    assert not s.sent("dbcc checkident", "dst")


def test_a_repair_of_rows_names_each_table_and_keeps_what_it_removes(
        monkeypatch, tmp_path):
    (tmp_path / "data-dbo.orders.missing").write_text("1\n2\n")
    (tmp_path / "data-dbo.orders.extra").write_text("9\n")
    (tmp_path / "data-dbo.audit.extra").write_text("7\n")
    eng = engine(monkeypatch, tmp_path, Server(),
                 exclude=["appdb.dbo.audit"])
    plan = eng.repair_plan("appdb", "rows")
    rows = [a for a in plan if a.kind == "rows"]
    assert [a.table for a in rows] == ["dbo.orders"]
    assert "2 missing, 0 changed, 1 extra" in rows[0].note
    assert "written to the undo file first" in rows[0].note
    # the table the hop leaves out is said, never acted on
    assert any("dbo.audit" in a.note for a in plan if a.kind != "rows")


# ---- the target kept from the application, and given back exactly ------------

def _held(monkeypatch, tmp_path, **rules):
    s = Server()
    s.on("select convert\\(nvarchar\\(30\\), sysdatetime", [("2026-01-01",)])
    s.on("select name from sys\\.database_principals", [("app",), ("report",)])
    s.on("select permission_name, state from sys\\.database_permissions",
         lambda side, sql: [("INSERT", "G"), ("UPDATE", "W")]
         if "'app'" in sql else [("INSERT", "G")])
    s.on("sys\\.dm_exec_sessions", [])
    for pattern, rows in rules.items():
        s.on(pattern, rows)
    eng = engine(monkeypatch, tmp_path, s)
    h = freeze.MSSQLHold(eng.hop, eng, "dst")
    h.ran = []
    saw = []

    def run(db, *sql):
        for statement in sql:
            record = freeze.state(eng.hop, db, "dst")
            # recorded before it is done: what each user held is on disk
            saw.append((statement, json.loads(json.dumps(record))))
        h.ran.extend(sql)
    h.run = run
    h.saw = saw
    return h, s


def test_a_target_is_kept_by_denied_writes_recorded_before_they_are_made(
        monkeypatch, tmp_path):
    h, _ = _held(monkeypatch, tmp_path)
    said = []
    h.keep("appdb", "mssql_deny", said.append)
    assert h.ran == ["deny insert, update, delete to [app]",
                     "deny insert, update, delete to [report]"]
    for statement, record in h.saw:
        user = statement.split("to ")[1].strip("[]")
        assert record["roles"][user]["prior"], statement
    assert freeze.state(h.hop, "appdb", "dst")["roles"]["app"]["prior"] == {
        "INSERT": "G", "UPDATE": "W"}
    assert any("2 users denied every write" in m for m in said)


def test_a_target_kept_is_given_back_as_every_right_was(monkeypatch,
                                                         tmp_path):
    h, _ = _held(monkeypatch, tmp_path)
    h.keep("appdb", "mssql_deny", lambda m: None)
    h.ran.clear()
    record = freeze.state(h.hop, "appdb", "dst")
    h.release("appdb", record, lambda m: None)
    # a right it held is granted again - with its grant option where it had
    # it - and one it did not hold is only unlocked of the deny
    assert h.ran == [
        "grant insert to [app]", "grant update to [app] with grant option",
        "revoke delete to [app]", "grant insert to [report]",
        "revoke update to [report]", "revoke delete to [report]"]


def test_the_read_back_says_when_a_write_is_not_denied(monkeypatch,
                                                        tmp_path):
    h, s = _held(monkeypatch, tmp_path)
    h.keep("appdb", "mssql_deny", lambda m: None)
    record = freeze.state(h.hop, "appdb", "dst")
    # every denied write: held
    s.rules.insert(0, (None, __import__("re").compile(
        "state = 'D'", __import__("re").S), [(3,)]))
    assert all(ok for ok, _ in h.kept("appdb", record))
    # one write not denied for a user: read back as not held
    s.rules[0] = (None, s.rules[0][1], [(2,)])
    assert not all(ok for ok, _ in h.kept("appdb", record))


def test_a_database_read_only_for_everyone_is_switched_back_as_it_was(
        monkeypatch, tmp_path):
    h, _ = _held(monkeypatch, tmp_path,
                 **{"cast\\(is_read_only as int\\)": [(0,)]})
    h.keep("appdb", "mssql_read_only", lambda m: None)
    assert h.ran == ["alter database [appdb] set read_only with rollback"
                     " immediate"]
    h.ran.clear()
    h.release("appdb", freeze.state(h.hop, "appdb", "dst"),
              lambda m: None)
    assert h.ran == ["alter database [appdb] set read_write with rollback"
                     " immediate"]


def test_a_database_read_only_already_is_left_so(monkeypatch, tmp_path):
    h, _ = _held(monkeypatch, tmp_path,
                 **{"cast\\(is_read_only as int\\)": [(1,)]})
    h.keep("appdb", "mssql_read_only", lambda m: None)
    assert h.ran == []
    h.release("appdb", freeze.state(h.hop, "appdb", "dst"),
              lambda m: None)
    assert h.ran == []


# ---- a change the hop does not carry ---------------------------------------

EP = Endpoint(host="x", port=1, user="x", password="x")


def _pair(monkeypatch, tmp_path, **kw):
    eng = engine(monkeypatch, tmp_path, Server())
    eng.neutral_columns = lambda side, db, table: [("id", "int"),
                                                   ("v", "nvarchar(20)")]
    hop = Hop(name="c", engine="hetero", source=EP, target=EP,
              databases=["d"], options={"source_engine": "mssql",
                                        "target_engine": "mssql"}, **kw)
    got = HeteroEngine.__new__(HeteroEngine)
    got.hop, got.src_name, got.dst_name = hop, "mssql", "mssql"
    got.src_engine, got.dst_engine = eng, eng
    got._row_scope = lambda db, t: ((hop.row_filter(db, *t.split(".")),) * 2)
    return got


def _change(op, i, table="dbo.orders"):
    if op == "delete":
        return canon.change("delete", table, {"id": i})
    return canon.change(op, table, {"id": i}, {"id": i, "v": "x"})


def test_a_change_the_hop_does_not_carry_is_left_out_and_counted(
        monkeypatch, tmp_path):
    p = _pair(monkeypatch, tmp_path,
              mapping={"ops": {"dbo.orders": "insert, update"}})
    acc = _Accounts(tailtables.Store.of(tmp_path))
    batch = [_change("insert", 1), _change("delete", 2),
             _change("update", 3), _change("delete", 4, "dbo.other")]
    acc.read(batch)
    out, _ = p._to_target("d", batch, {}, acc)
    assert [(c["op"], c["key"]["id"]) for c in out] == [
        ("insert", 1), ("update", 3), ("delete", 4)]
    acc.check("d", out, "0/1")
    assert acc.rows["dbo.orders"][tailtables.NOT_CARRIED] == 1
    assert acc.rows["dbo.orders"]["applied"] == 2


def test_a_hop_that_carries_everything_is_as_it_was(monkeypatch, tmp_path):
    p = _pair(monkeypatch, tmp_path)
    batch = [_change("insert", 1), _change("delete", 2),
             _change("update", 3)]
    out, _ = p._to_target("d", batch, {"dbo.orders": "dbo.orders"})
    assert out == batch
