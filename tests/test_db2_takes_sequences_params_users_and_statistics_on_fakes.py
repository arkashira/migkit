"""Db2's settings, accounts, statistics and counters on all three
platforms (backlog R13-DB2), each asked of a fake that answers the
catalogue rows IBM documents - no Db2 server exists here, so every way is
"built; not run on a real Db2 server" (each module's `UNPROVED_ON`) and
these tests prove what migkit does with the rows, not the server.
"""
import pytest

from migkit import decide
from migkit.config import Endpoint, Hop


class Cur:
    def __init__(self, conn):
        self.conn, self.rows, self.description = conn, [], None

    def execute(self, sql, args=()):
        self.conn.ran.append((sql, tuple(args or ())))
        low = sql.lower()
        for key, answer in self.conn.answers:
            if key in low:
                got = answer(sql, args) if callable(answer) else answer
                if isinstance(got, Exception):
                    raise got
                if isinstance(got, dict):
                    self.description = [(c,) for c in got["cols"]]
                    got = got["rows"]
                self.rows = [list(r) for r in got]
                return
        self.rows = []

    def fetchall(self):
        return self.rows

    def callproc(self, name, params):
        self.conn.calls.append((name, tuple(params)))
        return self.conn.proc(params)


class Conn:
    def __init__(self, answers=(), proc=None):
        self.answers, self.ran, self.calls = list(answers), [], []
        self.proc = proc or (lambda p: tuple(p[:3]) + (0,))

    def cursor(self):
        return Cur(self)

    def commit(self):
        self.ran.append(("commit", ()))

    def close(self):
        pass


def engine(kind="db2", src=(), dst=(), proc=None, same=True):
    from migkit.engines.db2 import Db2Engine
    from migkit.engines.db2i import Db2iEngine
    from migkit.engines.db2zos import Db2ZosEngine
    cls = {"db2": Db2Engine, "db2i": Db2iEngine, "db2zos": Db2ZosEngine}[kind]
    a = Endpoint(host="10.0.0.6", port=50000, user="u", password="CHANGE_ME",
                 options={"database": "SAMPLE"})
    b = Endpoint(host="10.0.0.6" if same else "10.0.0.7", port=50000,
                 user="u", password="CHANGE_ME",
                 options={"database": "SAMPLE"})
    eng = cls(Hop(name="d", engine=kind, source=a, target=b,
                  databases=["app"]))
    conns = {"src": Conn(src, proc), "dst": Conn(dst, proc)}
    eng._connect = lambda side, db: conns[side]
    eng.conns = conns
    return eng


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    monkeypatch.setattr(Hop, "report_dir", lambda self, db="": tmp_path)


CFG = [("codepage", "1208"), ("territory", "US"), ("logbufsz", "2048")]


def test_a_code_page_the_target_keeps_otherwise_fails_the_settings():
    same = engine(src=[("sysibmadm.dbcfg", CFG)], dst=[("sysibmadm.dbcfg",
                                                         CFG)])
    assert same.check_params("app")[0].status == "ok"
    eng = engine(src=[("sysibmadm.dbcfg", CFG)], dst=[(
        "sysibmadm.dbcfg", [("codepage", "819"), ("territory", "US"),
                            ("logbufsz", "4096")])])
    got = eng.check_params("app")[0]
    assert got.status == "diff" and "codepage src=1208 dst=819" in got.detail
    assert "member = current member" in eng.conns["src"].ran[0][0]


def test_a_side_whose_settings_cannot_be_read_is_said_not_agreed():
    eng = engine(src=[("sysibmadm.dbcfg", CFG)], dst=[(
        "sysibmadm.dbcfg", RuntimeError("SQL0551N not authorized"))])
    got = eng.check_params("app")[0]
    assert got.status == "error" and "SQL0551N" in got.detail


def test_z_os_and_ibm_i_read_their_own_settings():
    z = engine("db2zos", src=[("sysdummy1", [("encoding", "E")])],
               dst=[("sysdummy1", [("encoding", "U")])])
    assert z.check_params("app")[0].status == "diff"
    assert "current application encoding scheme" in z.conns["src"].ran[0][0]
    i = engine("db2i", src=[("system_value_info", [("QCCSID", "37")])],
               dst=[("system_value_info", [("QCCSID", "65535")])])
    assert "QCCSID src=37 dst=65535" in i.check_params("app")[0].detail


LUW_USERS = [
    ("from syscat.roles", [("APPREAD",), ("SYSTS_MGR",)]),
    ("from syscat.dbauth", [("ALICE", "U", "CONNECT", ""),
                            ("ALICE", "U", "DATAACCESS", ""),
                            ("OPS", "G", "DBADM", ""),
                            ("PUBLIC", "G", "CONNECT", "")]),
    ("from syscat.tabauth", [("APPREAD", "R", "SELECT", "ORDERS")]),
    ("from syscat.roleauth", [("ALICE", "U", "APPREAD")]),
]


def test_luw_accounts_carry_authorities_roles_and_grants_never_a_password():
    eng = engine(src=LUW_USERS, dst=[])
    src = eng.accounts("src")
    assert set(src) == {"user:ALICE", "role:APPREAD"}
    assert src["user:ALICE"] == {"login": True, "roles": ["APPREAD"],
                                 "grants": ["DATAACCESS ON *.*"],
                                 "hash": None, "attrs": {}}
    assert src["role:APPREAD"]["grants"] == ["SELECT ON app.orders"]
    steps, skipped = eng.account_steps(src, eng.accounts("dst"), {})
    runs = [s["run"] for s in steps]
    assert 'create role "APPREAD"' in runs
    assert 'grant select on table "APP"."ORDERS" to role "APPREAD"' in runs
    assert 'grant role "APPREAD" to user "ALICE"' in runs
    assert 'grant dataaccess on database to user "ALICE"' in runs
    assert [s["undo"] for s in steps if s["made"]] == ['drop role "APPREAD"']
    assert [k for k, _ in skipped] == ["user:ALICE"]


def test_z_os_accounts_are_system_privileges_and_table_grants():
    eng = engine("db2zos", src=[
        ("from sysibm.sysroles", []),
        ("from sysibm.sysuserauth", [("BOB", "U", "BINDADD", "")]),
        ("from sysibm.systabauth", [("BOB", "U", "SELECT", "ORDERS")])])
    src = eng.accounts("src")
    assert src["user:BOB"]["login"] is True
    steps, _ = eng.account_steps(src, {}, {})
    assert [s["run"] for s in steps] == [
        'grant bindadd to "BOB"',
        'grant select on table "APP"."ORDERS" to "BOB"']
    assert "tcreator = 'APP'" in eng.conns["src"].ran[-1][0]


def test_ibm_i_accounts_stay_the_user_profiles_users_reads():
    from migkit.engines.base import Engine
    from migkit.engines.db2i import Db2iEngine
    assert Db2iEngine.accounts is Engine.accounts


STATS = [("select t.tabname, t.card", [("NEVER", -1, 2), ("FRESH", 100, 1),
                                        ("BARE", 5, 0)]),
         ("count_big", lambda sql, a: [(100 if "FRESH" in sql else 900,)])]


def test_statistics_are_gathered_by_what_the_catalogue_holds():
    eng = engine(dst=STATS)
    eng.neutral_tables = lambda side, db: ["never", "fresh", "bare"]
    line = eng.settle_target("app")
    cmds = [a[0] for s, a in eng.conns["dst"].ran if "admin_cmd" in s]
    assert cmds == [
        'runstats on table "APP"."NEVER" with distribution and detailed'
        ' indexes all', 'runstats on table "APP"."BARE"']
    assert line == ("statistics gathered on 1 tables (with distribution"
                    " and indexes), 1 tables (rows)")


def test_statistics_rungs_read_the_catalogue_against_the_rows():
    def path(**f):
        return decide.db2_statistics(dict({"ratio": 0.1}, **f)).path
    assert path(rows=1000, card=None, indexes=1) == "distribution"
    assert path(rows=1000, card=995, indexes=3) == "current"
    assert path(rows=1000, card=500, indexes=0) == "table"


def test_z_os_statistics_run_the_utility_on_the_tables_table_space():
    eng = engine("db2zos", dst=[
        ("select t.name, t.cardf", [("T1", -1.0, 1, "DBAPP", "TSAPP")]),
        ("count_big", [(10,)])])
    eng.neutral_tables = lambda side, db: ["t1"]
    eng.settle_target("app")
    (name, params), = eng.conns["dst"].calls
    assert name == "SYSPROC.DSNUTILU" and params[1] == "NO"
    assert params[2] == ('runstats tablespace DBAPP.TSAPP table("APP"."T1")'
                         ' index(all keycard)')
    bad = engine("db2zos", dst=eng.conns["dst"].answers,
                 proc=lambda p: tuple(p[:3]) + (8,))
    bad.neutral_tables = lambda side, db: ["t1"]
    with pytest.raises(SystemExit, match="return code 8"):
        bad.settle_target("app")


def test_ibm_i_leaves_statistics_to_its_query_engine():
    from migkit import capabilities
    assert engine("db2i").settle_target("app") is None
    assert capabilities.matrix()["db2i"]["statistics"] == "not applicable"


def test_every_db2_sequence_and_identity_is_read_from_its_platforms_catalogue():
    luw = [s for s, _ in engine()._counter_sql("app")]
    assert "syscat.sequences" in luw[0] and luw[0].endswith(" with rr")
    assert "syscat.colidentattributes" in luw[1]
    z = [s for s, _ in engine("db2zos")._counter_sql("app")]
    assert "sysibm.syssequences" in z[0] and "syssequencesdep" in z[1]
    assert "qsys2.syssequences" in engine("db2i")._counter_sql("app")[0][0]


def test_the_cells_built_are_declared_unproved_with_their_reason():
    from migkit import capabilities
    from migkit.engines import db2, db2i, db2zos
    assert db2.UNPROVED_ON == "built; not run on a real Db2 LUW server"
    assert db2i.UNPROVED_ON == "built; not run on a real IBM i"
    assert db2zos.UNPROVED_ON == ("built; not run on a real Db2 for z/OS"
                                  " subsystem")
    for mod, eng in ((db2, "db2"), (db2i, "db2i"), (db2zos, "db2zos")):
        for item in mod.UNPROVED_ITEMS:
            assert capabilities.depth_cell(eng, item)[0] == "not yet", item
    m = capabilities.matrix()
    assert {c for c, v in m["db2"].items() if v != "yes"} == {"stream"}
    assert capabilities.GAPS["db2"] == {"stream": ("not yet", "LT.R6")}
