"""The cloud-native services' accounts compared and carried (backlog
P0-CLOUDS, CN2), without a server: the services that keep another's
accounts read as that one's (OceanBase, PolarDB-X and TDSQL as MySQL's,
OceanBase's Oracle mode as Oracle's, GeminiDB's APIs as their engines'),
GaussDB's roles with its own administrators, a Spanner database's roles
and their privileges, Lindorm's users and grants - each plan said, run a
statement at a time into the ledger, and undone from it. And a Lindorm
target kept from its application by its users' rights, given back exactly.
"""
import json

from migkit import users
from migkit.config import Endpoint, Hop

PROVES = [
    ("verify.users", ("dcdb", "gaussdb", "spanner", "lindorm", "oceanbase",
                      "polardbx", "oceanbase-oracle", "geminidb-cassandra",
                      "geminidb-redis", "geminidb-mongo")),
    ("cutover.read-only-target", "lindorm"),
]


def _hop(engine, tmp_path, **kw):
    hop = Hop(name="acc", engine=engine, source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _quiet(*_):
    pass


def test_a_service_keeping_anothers_accounts_is_read_as_that_ones(tmp_path):
    from migkit import capabilities
    hop = _hop("oceanbase", tmp_path)
    got = users._speaking(hop)
    assert (got.engine, got._accounts_of) == ("mysql", "oceanbase")
    assert got.report_dir() == tmp_path and hop.engine == "oceanbase"
    assert users._speaking(_hop("postgres", tmp_path)).engine == "postgres"
    covered = capabilities._users_engines()
    for name in ("oceanbase", "polardbx", "dcdb", "oceanbase-oracle",
                 "geminidb-cassandra", "geminidb-redis", "geminidb-mongo",
                 "gaussdb", "spanner", "lindorm"):
        assert name in covered, name
    # Oracle's accounts reached through the engine that keeps them
    from migkit.engines.oceanbase import OceanBaseOracleEngine
    assert type(users._ora_engine(users._speaking(_hop(
        "oceanbase-oracle", tmp_path)))) is OceanBaseOracleEngine


def test_a_tdsql_hops_accounts_are_compared_as_mysqls(monkeypatch, tmp_path):
    hop = _hop("dcdb", tmp_path)
    seen = []

    def read(ep):
        seen.append(ep.host)
        return {("app", "%"): {"auth": "*HASH", "attrs": "", "ssl": "",
                               "default_roles": [], "plugin": "x",
                               "grants": ["SELECT ON `shop`.*"]}} \
            if ep is hop.source else {}
    monkeypatch.setattr(users, "_mysql_users", read)
    out, _ = users.compare(hop, say=_quiet)
    assert out["engine"] == "mysql" and seen == ["10.0.0.1", "10.0.0.2"]
    assert out["missing_on_target"] == ["app@%"]


#: each service on another's API, the reader of that API's accounts and
#: what it reads them by, an account each side has and the source's one the
#: target lacks: (reader, by the side's endpoint, source, target)
_ON_ANOTHERS = {
    "oceanbase": ("_mysql_users", True,
                  {("app", "%"): {"auth": "*A", "attrs": "", "ssl": "",
                                  "default_roles": [], "plugin": "x",
                                  "grants": ["SELECT ON `shop`.*",
                                             "INSERT ON `shop`.*"]},
                   ("rpt", "%"): {"auth": "*B", "attrs": "", "ssl": "",
                                  "default_roles": [], "plugin": "x",
                                  "grants": []}},
                  {("app", "%"): {"auth": "*A", "attrs": "", "ssl": "",
                                  "default_roles": [], "plugin": "x",
                                  "grants": ["SELECT ON `shop`.*"]}}),
    "geminidb-mongo": ("_mongo_users", True,
                       {("shop", "app"): [("readWrite", "shop")],
                        ("shop", "rpt"): [("read", "shop")]},
                       {("shop", "app"): [("read", "shop")]}),
    "geminidb-redis": ("_redis_users", True,
                       {"app": {"hashes": ["#a"], "rules": "+@all ~*"},
                        "rpt": {"hashes": ["#b"], "rules": "+@read ~*"}},
                       {"app": {"hashes": ["#a"], "rules": "+@read ~*"}}),
    "geminidb-cassandra": ("_cs_roles", False,
                           {"app": {"hash": "h", "login": True,
                                    "superuser": False, "member_of": [],
                                    "grants": ["SELECT ON ks", "MODIFY ON"
                                               " ks"]},
                            "rpt": {"hash": "r", "login": True,
                                    "superuser": False, "member_of": [],
                                    "grants": []}},
                           {"app": {"hash": "h", "login": True,
                                    "superuser": False, "member_of": [],
                                    "grants": ["SELECT ON ks"]}}),
    "oceanbase-oracle": ("_ora_principals", False,
                         {"user:APP": {"hash": "h", "roles": ["CONNECT",
                                                              "RESOURCE"]},
                          "user:RPT": {"hash": "r", "roles": []}},
                         {"user:APP": {"hash": "h", "roles": ["CONNECT"]}}),
}
_ON_ANOTHERS["polardbx"] = _ON_ANOTHERS["oceanbase"]


def test_a_service_on_anothers_api_has_its_accounts_compared_as_that_ones(
        monkeypatch, tmp_path):
    """OceanBase and PolarDB-X as MySQL's, OceanBase's Oracle mode as
    Oracle's, GeminiDB's APIs as MongoDB's, Redis's and Cassandra's: each
    side's accounts read by that one's reader, from that side, and the gap
    found - an account the target lacks and a grant it is short of."""
    for engine, (reader, by_ep, src, dst) in _ON_ANOTHERS.items():
        hop = _hop(engine, tmp_path)
        seen = []

        def read(*args, src=src, dst=dst, by_ep=by_ep, seen=seen):
            far = args[0].host if by_ep else args[1]
            seen.append(far)
            return dict(src if far in ("10.0.0.1", "src") else dst)
        monkeypatch.setattr(users, reader, read)
        out, _ = users.compare(hop, say=_quiet)
        assert out["engine"] == users.ACCOUNTS_AS[engine], engine
        assert len(seen) == 2 and seen[0] != seen[1], (engine, seen)
        assert len(out["missing_on_target"]) == 1, (engine, out)
        assert "rpt" in out["missing_on_target"][0].lower(), (engine, out)
        assert len(out["grants_missing"]) == 1, (engine, out)
        assert "app" in out["grants_missing"][0].lower(), (engine, out)
        assert out["result"] == "gap", engine
        assert hop.engine == engine


class _Cursor:
    def __init__(self, rows, ran):
        self.rows, self.ran = rows, ran

    def execute(self, sql, args=None):
        self.ran.append((sql, args))

    def fetchall(self):
        return list(self.rows)


class _Conn:
    def __init__(self, rows, ran):
        self.rows, self.ran = rows, ran

    def cursor(self):
        return _Cursor(self.rows, self.ran)

    def close(self):
        pass


def _gauss_row(name, login=True, sysadmin=False, createdb=False,
               member_of=()):
    return (name, login, sysadmin, createdb, False, False, True, False,
            False, False, False, -1, None, list(member_of))


def test_gaussdb_roles_are_its_own_administrators_and_memberships(
        monkeypatch, tmp_path):
    hop = _hop("gaussdb", tmp_path)
    src = [_gauss_row("app", sysadmin=True, member_of=["reader"]),
           _gauss_row("reader", login=False), _gauss_row("omm"),
           _gauss_row("gs_role_copy_files", login=False)]
    dst = [_gauss_row("reader", login=False), _gauss_row("omm")]
    ran = []
    monkeypatch.setattr(users, "_pg_conn", lambda ep, dbname="postgres":
                        _Conn(src if ep is hop.source else dst, ran))
    s = users._gauss_roles(hop.source)
    assert set(s) == {"app", "reader"}
    assert s["app"]["sysadmin"] is True and s["app"]["member_of"] == [
        "reader"]
    out, _ = users.compare(hop, say=_quiet)
    assert out["missing_on_target"] == ["app"]
    assert out["password_differs"] is None
    t = users._gauss_roles(hop.target)
    steps, skipped = users._pg_plan(out, s, t, {}, {"app": "CHANGE_ME"},
                                    users.GAUSS_ATTRS, users._gauss_options)
    made = steps[0]
    assert made["sql"] == '''CREATE ROLE "app" LOGIN SYSADMIN PASSWORD %s'''
    assert made["secret"] == "CHANGE_ME" and "CHANGE_ME" not in made["shown"]
    assert steps[1]["sql"] == 'GRANT "reader" TO "app"' and not skipped
    # a login role without a password in the file is said, not made
    steps, skipped = users._pg_plan(out, s, t, {}, {}, users.GAUSS_ATTRS,
                                    users._gauss_options)
    assert skipped and skipped[0][0] == "app"


class _Spanner:
    """A Spanner side's information schema, by the view a query reads."""

    def __init__(self, views, pg=False):
        self.views, self.pg, self.ddl = views, pg, []

    def _execute(self, side, db, sql):
        for name, rows in self.views[side].items():
            if f"information_schema.{name}" in sql.replace(" ", "") \
                    or f"information_schema.{name}" in sql:
                return rows
        raise AssertionError(sql)

    def _qd(self, side, db, name):
        return f'"{name}"' if self.pg else f"`{name}`"

    def _pg(self, side, db):
        return self.pg

    def execute_ddl(self, side, db, sql):
        self.ddl.append((db, sql))


def test_a_spanner_databases_roles_and_their_privileges_are_carried(
        monkeypatch, tmp_path):
    hop = _hop("spanner", tmp_path)
    fake = _Spanner({
        "src": {"roles": [("public", True), ("analyst", False),
                          ("reader", False)],
                "table_privileges": [("analyst", "", "orders", "SELECT")],
                "column_privileges": [("reader", "", "orders", "total",
                                       "SELECT")],
                "change_stream_privileges": [("analyst", "", "cs",
                                              "SELECT")],
                "role_grantees": [("reader", "analyst")]},
        "dst": {"roles": [("public", True), ("reader", False)],
                "table_privileges": [],
                "column_privileges": [("reader", "", "orders", "total",
                                       "SELECT")],
                "change_stream_privileges": [], "role_grantees": []}})
    monkeypatch.setattr(users, "_sp_engine", lambda hop: fake)
    s = users._sp_roles(hop, "src")
    assert s["shop.analyst"] == {
        "grants": ["SELECT ON CHANGE STREAM `cs`",
                   "SELECT ON TABLE `orders`"],
        "member_of": ["reader"]}
    out, _ = users.compare(hop, say=_quiet)
    assert out["missing_on_target"] == ["shop.analyst"]
    assert out["grants_missing"] == [] and out["result"] == "gap"
    # the stand-in's target does not change: the comparison after is not
    # this test's
    monkeypatch.setattr(users, "_after", lambda *a, **kw: None)
    users.create(hop, apply=True, say=_quiet)
    assert [sql for _, sql in fake.ddl] == [
        "CREATE ROLE `analyst`",
        "GRANT SELECT ON CHANGE STREAM `cs` TO ROLE `analyst`",
        "GRANT SELECT ON TABLE `orders` TO ROLE `analyst`",
        "GRANT ROLE `reader` TO ROLE `analyst`"]
    kept = json.loads((tmp_path / "user-sync-created.json").read_text())
    assert kept[-1]["engine"] == "spanner" and all(
        st["ok"] for st in kept[-1]["steps"])
    fake.ddl.clear()
    users.rollback(hop, apply=True, say=_quiet)
    assert [sql for _, sql in fake.ddl] == [
        "REVOKE ROLE `reader` FROM ROLE `analyst`",
        "REVOKE SELECT ON TABLE `orders` FROM ROLE `analyst`",
        "REVOKE SELECT ON CHANGE STREAM `cs` FROM ROLE `analyst`",
        "DROP ROLE `analyst`"]
    assert (tmp_path / "user-sync-created.json.rolled-back").exists()


class _Lindorm:
    def __init__(self, answers):
        self.answers, self.ran = answers, []

    def _answered(self, side, db, sql):
        return self.answers[side][sql]

    def _q(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def _connect(self, side, db):
        return _Conn([], self.ran)


def test_lindorm_users_made_with_the_files_password_and_their_grants(
        monkeypatch, tmp_path):
    hop = _hop("lindorm", tmp_path, db_map={"shop": "shop_copy"})
    fake = _Lindorm({
        "src": {"show users": [{"user_name": "root"}, {"user_name": "app"}],
                "show privileges": [
                    {"user_name": "app", "privilege": "READ",
                     "resource_type": "DATABASE", "resource_name": "shop"},
                    {"user_name": "app", "privilege": "WRITE",
                     "resource_type": "TABLE", "resource_name": "orders"}]},
        "dst": {"show users": [{"user_name": "root"}],
                "show privileges": []}})
    monkeypatch.setattr(users, "_ld_engine", lambda hop: fake)
    s = users._ld_users(hop, "src")
    assert s == {"app": {"grants": ["READ ON DATABASE `shop`",
                                    "WRITE ON TABLE `orders`"]}}
    out, _ = users.compare(hop, say=_quiet)
    assert out["missing_on_target"] == ["app"]
    monkeypatch.setattr(users, "_after", lambda *a, **kw: None)
    users.create(hop, apply=True, passwords={"app": "CHANGE_ME"},
                 say=_quiet)
    assert fake.ran == [
        ("CREATE USER `app` IDENTIFIED BY %s", ("CHANGE_ME",)),
        ("GRANT READ ON DATABASE `shop_copy` TO `app`", ()),
        ("GRANT WRITE ON TABLE `orders` TO `app`", ())]
    fake.ran.clear()
    users.rollback(hop, apply=True, say=_quiet)
    assert [sql for sql, _ in fake.ran] == [
        "REVOKE WRITE ON TABLE `orders` FROM `app`",
        "REVOKE READ ON DATABASE `shop_copy` FROM `app`",
        "DROP USER IF EXISTS `app`"]
    # the target's grants on its own database name compare as the source's
    fake.answers["dst"]["show privileges"] = [
        {"user_name": "app", "privilege": "READ",
         "resource_type": "DATABASE", "resource_name": "shop_copy"}]
    fake.answers["dst"]["show users"].append({"user_name": "app"})
    t = users._ld_users(hop, "dst")
    assert t["app"]["grants"] == ["READ ON DATABASE `shop`"]


def test_lindorm_table_grants_are_named_for_the_targets_database(tmp_path):
    hop = _hop("lindorm", tmp_path, db_map={"shop": "shop_copy"})
    assert users._ld_there(hop, "WRITE ON TABLE `shop`.`orders`") == \
        "WRITE ON TABLE `shop_copy`.`orders`"
    assert users._ld_there(hop, "WRITE ON TABLE `orders`") == \
        "WRITE ON TABLE `orders`"
    assert users._ld_said(hop, "dst", "TABLE", "shop_copy.orders") == \
        "shop.orders"
    assert users._ld_on(_Lindorm({}), "TABLE", "shop.orders") == \
        "`shop`.`orders`"
    assert users._ld_on(_Lindorm({}), "GLOBAL", None) == ""


# ---- Lindorm's target kept by its users' rights ------------------------------

class _LindormRights:
    """A Lindorm instance's rights as `SHOW PRIVILEGES` says them, changed
    by each statement a hold runs through its connection - a revoke of a
    right it does not hold is an error, as the server's is."""

    def __init__(self, rows):
        self.rows = [dict(r) for r in rows]
        self.ran = []

    def _answered(self, side, db, sql):
        assert sql == "show privileges", sql
        return [dict(r) for r in self.rows]

    def _ep(self, side):
        return Endpoint(host="10.0.0.2", user="migkit")

    def _q(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def _d(self, side, db):
        return db

    def in_scope(self, db, table):
        return table != "audit"

    def _connect(self, side, db):
        server = self

        class Cur:
            def execute(self, sql, args=None):
                import re
                server.ran.append(sql)
                kind, priv, scope, on, who = re.match(
                    r"(revoke|grant) (\w+) on (\w+) (.+) (?:from|to)"
                    r" `(\w+)`$", sql).groups()
                row = {"user_name": who, "privilege": priv.upper(),
                       "resource_type": scope.upper(),
                       "resource_name": on.replace("`", "")}
                if kind == "revoke":
                    server.rows.remove(row)
                else:
                    server.rows.append(row)

        class Conn:
            autocommit = True

            def cursor(self):
                return Cur()

            def close(self):
                pass
        return Conn()


def _right(who, priv, scope, on=None):
    return {"user_name": who, "privilege": priv, "resource_type": scope,
            "resource_name": on}


def test_a_lindorm_target_is_kept_by_its_users_rights_and_given_back(
        tmp_path):
    from migkit import freeze
    hop = _hop("lindorm", tmp_path, options={"protect_target": True})
    server = _LindormRights([
        _right("app", "WRITE", "DATABASE", "shop"),
        _right("app", "READ", "DATABASE", "shop"),
        _right("app", "TRASH", "TABLE", "shop.orders"),
        # a table the hop leaves out, another database, a read: not taken
        _right("app", "WRITE", "TABLE", "shop.audit"),
        _right("app", "WRITE", "DATABASE", "other"),
        _right("rpt", "READ", "TABLE", "shop.orders"),
        # migkit's own and the system's rights are theirs to keep
        _right("migkit", "ALL", "DATABASE", "shop"),
        _right("root", "ALL", "GLOBAL"),
        # a right on the whole instance: named, never taken
        _right("ops", "WRITE", "GLOBAL")])
    before = [dict(r) for r in server.rows]
    h = freeze.KEEPS["lindorm"](hop, server, "dst")
    assert type(h) is freeze.LindormHold
    assert h.accounts("shop") == ["app"]
    said = []
    h.keep("shop", h.WAY, said.append)
    assert sorted(server.ran) == [
        "revoke trash on table `shop`.`orders` from `app`",
        "revoke write on database `shop` from `app`"]
    record = freeze.state(hop, "shop", "dst")
    assert sorted(map(tuple, record["roles"]["app"]["revoked"])) == [
        ("TRASH", "TABLE", "shop.orders"), ("WRITE", "DATABASE", "shop")]
    assert any("ops may write in every database" in s for s in said), said
    assert all(ok for ok, _ in h.kept("shop", record))
    # a right granted since is read back as not kept
    server.rows.append(_right("rpt", "WRITE", "TABLE", "orders"))
    got = {what.split(":")[0]: ok for ok, what in h.kept("shop", record)}
    assert got == {"app": True, "rpt": False}
    server.rows.pop()
    server.ran.clear()
    h.release("shop", record, said.append)
    assert sorted(server.ran) == [
        "grant trash on table `shop`.`orders` to `app`",
        "grant write on database `shop` to `app`"]

    def key(r):
        return sorted((k, str(v)) for k, v in r.items())
    assert sorted(map(key, server.rows)) == sorted(map(key, before))
