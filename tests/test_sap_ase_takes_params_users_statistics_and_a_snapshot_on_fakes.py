"""SAP ASE's settings, identity counters, accounts, statistics, rollback
point, the server's own insert-select and the delta by timestamp
(backlog R13-ASE), each asked of a fake answering the catalogue rows the
ASE 16 reference documents - no ASE server runs here (its image is x86
only), so every way is "built; not run on a real SAP ASE server"
(`ase.UNPROVED_ON`) and these tests prove what migkit does with the rows,
not the server.
"""
import pytest

from migkit import capabilities, decide
from migkit.config import Endpoint, Hop


class Cur:
    def __init__(self, conn):
        self.conn, self.rows = conn, []

    def execute(self, sql, args=()):
        self.conn.ran.append((sql, tuple(args or ())))
        low = sql.lower()
        for key, answer in self.conn.answers:
            if key in low:
                got = answer(sql, args) if callable(answer) else answer
                if isinstance(got, Exception):
                    raise got
                self.rows = [list(r) for r in got]
                return
        self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class Conn:
    def __init__(self, answers=()):
        self.answers, self.ran = list(answers), []

    def cursor(self):
        return Cur(self)

    def commit(self):
        self.ran.append(("commit", ()))

    def rollback(self):
        self.ran.append(("rollback", ()))

    def close(self):
        pass


def engine(src=(), dst=(), same=True, target="ase"):
    from migkit.engines.ase import AseEngine
    a = Endpoint(host="10.0.0.8", port=5000, user="u", password="CHANGE_ME")
    b = Endpoint(host="10.0.0.8" if same else "10.0.0.9", port=5000,
                 user="u", password="CHANGE_ME")
    opts = {} if target == "ase" else {"source_engine": "ase",
                                       "target_engine": target}
    eng = AseEngine(Hop(name="a", engine="ase" if target == "ase" else
                        "hetero", source=a, target=b, databases=["app"],
                        options=opts))
    conns = {"src": Conn(src), "dst": Conn(dst)}
    eng._connect = lambda side, db: conns[side]
    eng.through_pair = lambda db, t: False
    eng.conns = conns
    return eng


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    monkeypatch.setattr(Hop, "report_dir", lambda self, db="": tmp_path)


SETTINGS = "union all select '@@version_number'"
CFG = [("default character set id", "190"), ("default sortorder id", "50"),
       ("number of user connections", "100"), ("@@version_number", "16000")]


def test_a_sort_order_the_target_keeps_otherwise_fails_the_settings():
    same = engine(src=[(SETTINGS, CFG)], dst=[(SETTINGS, CFG)])
    assert same.check_params("app")[0].status == "ok"
    eng = engine(src=[(SETTINGS, CFG)], dst=[(SETTINGS, [
        ("default character set id", "190"), ("default sortorder id", "52"),
        ("number of user connections", "500"),
        ("@@version_number", "16000")])])
    got = eng.check_params("app")[0]
    assert got.status == "diff"
    assert "default sortorder id src=50 dst=52" in got.detail
    # a limit of the instance alone differs and is not the failing one
    assert "number of user connections src" not in got.detail
    assert "master..syscurconfigs" in eng.conns["src"].ran[0][0]


def test_settings_that_cannot_be_read_are_said_not_agreed():
    eng = engine(src=[(SETTINGS, CFG)], dst=[(SETTINGS, RuntimeError(
        "Msg 10330 permission denied"))])
    got = eng.check_params("app")[0]
    assert got.status == "error" and "10330" in got.detail


IDS = "next_identity("


def test_an_identity_column_behind_the_source_says_how_it_is_moved_on():
    eng = engine(src=[(IDS, [("dbo.orders", "id", "1001"),
                             ("dbo.items", "id", "7")])],
                 dst=[(IDS, [("dbo.orders", "id", "1"),
                             ("dbo.items", "id", "7")])])
    got = eng.check_autoinc("app")[0]
    assert got.status == "diff" and "dbo.orders.id next 1 < source 1001" \
        in got.detail and "items" not in got.detail
    assert "sp_chgattribute 'dbo.orders', 'identity_burn_max', 0," \
           " '1000'" in got.fix_hint
    ok = engine(src=[(IDS, [("dbo.orders", "id", "5")])],
                dst=[(IDS, [("dbo.orders", "id", "9")])])
    assert ok.check_autoinc("app")[0].status == "ok"


def test_the_counters_are_the_moves_where_the_target_is_another_engine():
    eng = engine(src=[(IDS, [("dbo.orders", "id", "5")])], target="postgres")
    assert eng.check_autoinc("app")[0].status == "skip"
    assert eng.conns["src"].ran == []


ACCOUNTS = [
    ("master..syslogins l where u.suid", [("ann", "ann", 0),
                                          ("bob", "bob_login", 2),
                                          ("dbo", "sa", 0)]),
    ("where uid = gid", [("clerks",)]),
    ("u.gid = g.uid", [("ann", "clerks")]),
    ("sysloginroles", [("ann", "auditor"), ("ann", "sa_role"),
                       ("nobody", "auditor")]),
    ("sysprotects", [("ann", "dbo.orders", 193), ("clerks", "dbo.orders",
                                                  195),
                     ("public", "dbo.orders", 193),
                     ("ann", "dbo.orders", 999)]),
]


def test_logins_users_groups_and_roles_read_in_one_shape():
    got = engine(src=ACCOUNTS).accounts("src")
    assert set(got) == {"user:ann", "user:bob", "role:clerks",
                        "role:auditor"}
    assert got["user:ann"]["login"] is True
    # a locked login does not sign in
    assert got["user:bob"]["login"] is False
    assert got["user:ann"]["roles"] == ["auditor", "clerks"]
    assert got["user:ann"]["grants"] == ["SELECT ON app.dbo.orders"]
    assert got["role:clerks"]["grants"] == ["INSERT ON app.dbo.orders"]
    assert got["role:clerks"]["attrs"]["kind"] == "group"
    assert got["role:auditor"]["attrs"]["kind"] == "server role"
    assert got["user:ann"]["hash"] is None


def test_the_steps_make_what_the_target_lacks_and_say_what_they_cannot():
    eng = engine(src=ACCOUNTS)
    src = eng.accounts("src")
    steps, skipped = eng.account_steps(src, {}, {"ann": "s3cret"})
    runs = [s["run"] for s in steps]
    assert 'create role "auditor"' in runs
    assert "exec sp_addgroup 'clerks'" in runs
    login = next(s for s in steps if s["run"].startswith("create login"))
    assert login["given"] and "s3cret" not in login["shown"]
    assert login["undo"] == 'drop login "ann"'
    assert "exec sp_adduser 'ann'" in runs
    assert "exec sp_changegroup 'clerks', 'ann'" in runs
    assert 'grant role "auditor" to "ann"' in runs
    assert 'grant select on "dbo"."orders" to "ann"' in runs
    # groups and roles are made before the users who hold them
    assert runs.index("exec sp_addgroup 'clerks'") < runs.index(
        "exec sp_changegroup 'clerks', 'ann'")
    assert [k for k, _ in skipped] == ["user:bob"]
    # a target that has it all needs nothing
    again, _ = eng.account_steps(src, src, {})
    assert again == []


def test_statistics_are_decided_by_how_much_changed_since_them():
    ratio = 0.10
    got = decide.ase_statistics({"changed": 40.0, "gathered": 3,
                                 "indexes": 2, "ratio": ratio})
    assert got.path == "index"
    got = decide.ase_statistics({"changed": 40.0, "gathered": 3,
                                 "indexes": 0, "ratio": ratio})
    assert got.path == "table"
    got = decide.ase_statistics({"changed": 2.0, "gathered": 3,
                                 "indexes": 2, "ratio": ratio})
    assert got.path == "current"
    # never gathered is stale, whatever the change says
    got = decide.ase_statistics({"changed": 0.0, "gathered": 0,
                                 "indexes": 1, "ratio": ratio})
    assert got.path == "index"


def test_the_target_is_settled_table_by_table():
    eng = engine(dst=[("from sysobjects where type = 'u' order by 1",
                       [("dbo.orders",), ("dbo.notes",), ("dbo.kept",)]),
                      ("datachange(", [("dbo.orders", 100.0, 2, 0),
                                       ("dbo.notes", 55.0, 0, 4),
                                       ("dbo.kept", 1.0, 1, 4)])])
    said = eng.settle_target("app")
    ran = [s for s, _ in eng.conns["dst"].ran]
    assert 'update index statistics "dbo"."orders"' in ran
    assert 'update statistics "dbo"."notes"' in ran
    assert not any('"kept"' in s for s in ran)
    assert said == ("statistics updated on 1 tables (every index's"
                    " columns), 1 tables (the table)")


def test_the_rollback_point_keeps_the_counters_and_the_database_timestamp(
        tmp_path):
    eng = engine(dst=[(IDS, [("dbo.orders", "id", "1001")]),
                      ("@@dbts", [(bytes.fromhex("0000000000001f40"),)])])
    eng.snapshot_state("app", tmp_path)
    ran = [s for s, _ in eng.conns["dst"].ran]
    assert ran[0] == "set transaction isolation level 3"
    assert (tmp_path / "dst-ase-counters.txt").read_text() == \
        "dbo.orders|1000\n"
    mark = (tmp_path / "dst-ase-mark.txt").read_text()
    assert mark.startswith("timestamp mark|0000000000001f40|")
    assert eng.restore_sequences("app", tmp_path) == [
        "exec sp_chgattribute 'dbo.orders', 'identity_burn_max', 0,"
        " '1000';"]


def test_a_timestamp_not_read_leaves_the_counters_alone_in_the_mark(
        tmp_path):
    eng = engine(dst=[(IDS, [("dbo.orders", "id", "3")]),
                      ("@@dbts", RuntimeError("Msg 137 must declare"))])
    eng.snapshot_state("app", tmp_path)
    mark = (tmp_path / "dst-ase-mark.txt").read_text()
    # no timestamp kept, and why said in the mark
    assert mark.startswith("together||") and "Msg 137" in mark
    assert decide.ase_snapshot_mark({"dbts": "Msg 137"}).path == "together"


BASE = dict(rows=50000, pages=900, page_size=4096, log_free=5000,
            empty=True, mapped=False, same_place=True)


def test_the_servers_own_insert_is_taken_only_where_it_is_safe():
    assert decide.ase_bulk_way(BASE).path == "insert-select"
    for change, why in ((dict(same_place=False), "two servers"),
                        (dict(log_free=100), "would fill it"),
                        (dict(log_free=None), "unread number"),
                        (dict(empty=False), "holds rows"),
                        (dict(mapped=True), "renames"),
                        (dict(rows=None), "unknown rows")):
        got = decide.ase_bulk_way(dict(BASE, **change))
        assert got.path == "copier", change
        assert why in got.reason, (change, got.reason)


def _bulk(counts):
    return [("row_count(", [("dbo.orders", 50000, 900)]),
            ("lct_admin(", [(4096, 5000)]),
            ("select top 1 1 from", []),
            ("from syscolumns c join systypes", [("id", "int"),
                                                 ("note", "varchar(20)")]),
            ("status & 128 = 128", [(1,)]),
            ("select count(*) from", counts)]


def test_a_table_is_inserted_by_the_server_and_its_count_checked():
    eng = engine(src=_bulk([]), dst=_bulk(lambda sql, a: [(50000,)]))
    steps = eng.native_bulk("app", ["dbo.orders"], True, None)
    assert steps[0].startswith("dbo.orders: the server's own insert")
    ran = [s for s, _ in eng.conns["dst"].ran]
    ins = next(s for s in ran if s.startswith("insert into"))
    # the source by its database's name, the identity values kept
    assert 'from "app"."dbo"."orders"' in ins
    assert ran.index('set identity_insert "dbo"."orders" on') < \
        ran.index(ins) < ran.index('set identity_insert "dbo"."orders" off')
    assert ("commit", ()) in eng.conns["dst"].ran


def test_an_insert_whose_count_differs_is_rolled_back_and_stops():
    counts = iter([(49999,), (50000,)])
    eng = engine(src=_bulk([]), dst=_bulk(lambda sql, a: [next(counts)]))
    with pytest.raises(SystemExit, match="inserted 49999 rows of 50000"):
        eng.native_bulk("app", ["dbo.orders"], True, None)
    assert ("rollback", ()) in eng.conns["dst"].ran
    assert ("commit", ()) not in eng.conns["dst"].ran


def test_a_plan_across_two_servers_names_the_copier():
    eng = engine(src=_bulk([(0,)]), dst=_bulk([(0,)]), same=False)
    steps = eng.native_bulk("app", ["dbo.orders"], False, None)
    assert steps == ["dbo.orders: the table copier - " + decide.ase_bulk_way(
        dict(BASE, same_place=False)).reason]


DELTA = [("from sysobjects where type = 'u' order by 1", [("dbo.orders",),
                                                          ("dbo.plain",)]),
         ("t.name = 'timestamp'", [("dbo.orders", "ts")]),
         ("status3", [(0,)]),
         ("@@dbts", [(bytes.fromhex("00000000000000aa"),)]),
         ("index_col(", [("id",)]),
         ("from syscolumns c join systypes", [("id", "int"),
                                              ("v", "varchar(9)"),
                                              ("ts", "timestamp")])]


def test_the_delta_reads_the_rows_stamped_since_and_compares_them_by_key():
    changed = [(1, "a"), (2, "b")]
    eng = engine(src=[("> ?", changed)] + DELTA,
                 dst=[('where "id" = ?', lambda sql, a: [
                     (1, "a")] if a[0] == 1 else [])] + DELTA)
    first = eng.delta_verify("app")
    assert [r.status for r in first] == ["skip", "ok"]
    assert "no timestamp column" in first[0].detail
    again = eng.delta_verify("app")
    got = again[-1]
    assert got.status == "diff" and "1 missing" in got.detail
    read = next((s, a) for s, a in eng.conns["src"].ran if "> ?" in s)
    # the timestamp is the server's own on each side: never compared
    assert '"ts"' not in read[0].split("from")[0]
    assert read[1] == (bytes.fromhex("00000000000000aa"),)


def test_no_timestamp_column_is_said_with_the_replication_agent():
    eng = engine(src=[("t.name = 'timestamp'", []), ("status3", [(1,)])]
                 + DELTA[:1])
    got = eng.delta_verify("app")
    assert got[0].status == "skip"
    assert "replication agent runs" in got[0].detail


def test_the_cells_built_are_declared_built_and_unproved():
    from migkit.engines import ase
    assert capabilities.GAPS["ase"] == {"stream": (capabilities.NOT_YET,
                                                   "34")}
    row = capabilities.matrix()["ase"]
    for cap in ("sequences", "params", "bulk-move", "fence", "confirm",
                "delta", "users", "statistics", "snapshot"):
        assert row[cap] == "yes", cap
    assert ase.UNPROVED_ON == "built; not run on a real SAP ASE server"
    for item in ("move.bulk", "verify.users", "verify.delta",
                 "cutover.rollback", "cutover.counters"):
        assert item in ase.UNPROVED_ITEMS.split()
        assert capabilities.depth_why("ase", item) == ase.UNPROVED_ON
