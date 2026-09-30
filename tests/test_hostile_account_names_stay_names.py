"""An account's name read off the source stays a name on the target.

`users create` writes the source's account and role names into statements
run on the target with an administrator's rights. Written raw, a source
role called `x" SUPERUSER --` became `CREATE ROLE "x" SUPERUSER --" ...`:
a superuser on the target. Each name now goes through the engine's own
quoting and each value is bound; these prove it by the text each engine
is handed and, on real servers, by what the target holds afterwards -
names with quotes, semicolons, comments, a `%`, and look-alike characters
(a fullwidth quotation mark) are created as themselves and nothing else
runs.
"""
import socket
import subprocess
import time
from types import SimpleNamespace

import pytest

HOSTILE_PG = ['x" SUPERUSER --', "a;b", "evil'--", "100%s",
              "ｘ＂ SUPERUSER"]


# ---- the text each engine is handed ------------------------------------------

def test_postgres_names_are_quoted_and_a_percent_survives_formatting():
    from migkit import users
    s = {r: {"login": False, "createdb": False, "createrole": False,
             "connlimit": -1, "superuser": False, "replication": False,
             "bypassrls": False, "inherit": True, "valid_until": None,
             "member_of": ["zreaders"]} for r in HOSTILE_PG}
    s["zreaders"] = dict(s["a;b"], member_of=[])
    out = {"missing_on_target": sorted(s), "grants_missing": [],
           "attributes_differ": []}
    steps, skipped = users._pg_plan(out, s, {}, {}, {})
    assert skipped == []
    ran = [st["sql"] % tuple(st["args"]) for st in steps]
    # every role made before any membership is granted: `zreaders` sorts
    # last and is granted to the others
    first_grant = next(i for i, x in enumerate(ran) if x.startswith("GRANT"))
    assert all(x.startswith("CREATE ROLE") for x in ran[:first_grant])
    assert len(ran[:first_grant]) == len(s)
    assert 'CREATE ROLE "x"" SUPERUSER --"' in ran
    assert 'GRANT "zreaders" TO "x"" SUPERUSER --"' in ran
    assert 'CREATE ROLE "100%s"' in ran
    assert "CREATE ROLE \"evil'--\"" in ran
    assert 'CREATE ROLE "ｘ＂ SUPERUSER"' in ran
    # the only SUPERUSER anywhere is inside a quoted name
    for x in ran:
        assert "SUPERUSER" not in x.replace('"x"" SUPERUSER --"', "") \
            .replace('"ｘ＂ SUPERUSER"', ""), x


def test_postgres_password_is_bound_and_never_shown():
    from migkit import users
    a = {"login": True, "createdb": False, "createrole": False,
         "connlimit": 0, "superuser": False, "replication": False,
         "bypassrls": False, "inherit": False,
         "valid_until": "2030-01-01 00:00:00+00", "member_of": []}
    out = {"missing_on_target": ["app"], "grants_missing": [],
           "attributes_differ": []}
    steps, _ = users._pg_plan(out, {"app": a}, {}, {}, {"app": "CHANGE_ME'x"})
    (st,) = steps
    assert st["sql"] == ('CREATE ROLE "app" LOGIN NOINHERIT CONNECTION LIMIT'
                         ' 0 VALID UNTIL %s PASSWORD %s')
    assert st["args"] == ["2030-01-01 00:00:00+00"]
    assert st["secret"] == "CHANGE_ME'x"
    assert "CHANGE_ME" not in st["shown"] and "CHANGE_ME" not in str(
        st["undo"])


def _my_account(grants=(), role=False, locked="N", auth=b"$A$005$salt"):
    return {"plugin": "caching_sha2_password", "auth": auth,
            "attrs": {"account_locked": locked, "password_expired": "N",
                      "ssl_type": "", "max_questions": "0",
                      "max_updates": "0", "max_connections": "0",
                      "max_user_connections": "0"},
            "ssl": {"ssl_cipher": "", "x509_issuer": "", "x509_subject": ""},
            "role": role, "maria": False, "default_roles": [],
            "grants": list(grants)}


def test_mysql_account_names_are_bound_never_written():
    import pymysql.converters as conv

    from migkit import users
    evil = ("x' IDENTIFIED BY 'p", "%")
    s = {evil: _my_account(), ("100%s", "%"): _my_account(),
         ("a`b", "10.0.0.%"): _my_account()}
    out = {"missing_on_target": [f"{u}@{h}" for u, h in s],
           "grants_missing": [], "attributes_differ": []}
    steps, skipped = users._my_plan(out, s, {})
    assert skipped == []
    for st in steps:
        assert st["sql"].startswith("CREATE USER %s@%s IDENTIFIED WITH"
                                    " caching_sha2_password AS 0x")
        u, h = st["args"][:2]
        # the name is an argument, and nowhere in the text
        assert u not in st["sql"].replace("%s", "") or u == "100%s"
        rendered = st["sql"] % tuple(conv.escape_item(a, "utf8mb4")
                                     for a in st["args"])
        assert rendered.startswith(
            f"CREATE USER {conv.escape_item(u, 'utf8mb4')}@"
            f"{conv.escape_item(h, 'utf8mb4')} IDENTIFIED WITH")
        assert "0x" + b"$A$005$salt".hex() in rendered
        assert "salt" not in st["shown"]


def test_mysql_grant_lines_are_replayed_as_the_server_wrote_them():
    from migkit import users
    line = "GRANT SELECT ON `app`.* TO `100%s`@`%`"
    s = {("100%s", "%"): _my_account([line])}
    t = {("100%s", "%"): _my_account([])}
    out = {"missing_on_target": [], "grants_missing": ["100%s@%"],
           "attributes_differ": []}
    steps, _ = users._my_plan(out, s, t)
    (st,) = steps
    # doubled so the driver's formatting gives the line back unchanged
    assert st["sql"] % () == line
    assert st["undo"][0] % () == "REVOKE SELECT ON `app`.* FROM `100%s`@`%`"


def test_mysql_revoke_reads_only_the_statement_own_words():
    from migkit import users
    assert users._my_revoke(
        "GRANT SELECT (`a ON b`) ON `x TO y`.* TO `u`@`%` WITH GRANT OPTION"
    ) == "REVOKE SELECT (`a ON b`), GRANT OPTION ON `x TO y`.* FROM `u`@`%`"
    assert users._my_revoke("GRANT `r;--`@`%` TO `u`@`%`") == \
        "REVOKE `r;--`@`%` FROM `u`@`%`"
    assert users._my_revoke("REVOKE x") is None


def test_sql_server_names_are_bracketed_with_the_bracket_doubled():
    from migkit import users
    hop = SimpleNamespace(databases=["shop"], target_db=lambda d: d)
    evil = "a]; DROP LOGIN sa; --"
    s = {f"login:{evil}": {
             "name": evil, "kind": "S", "sid": "01ab", "hash": "0200ff",
             "disabled": False, "default_db": "shop", "policy": True,
             "expiration": False, "roles": ["dbcreator"], "grants": []},
         f"user:shop.{evil}": {
             "name": evil, "db": "shop", "kind": "S", "login": evil,
             "schema": "s]x", "roles": ["db_datareader"], "unwritten": [],
             "grants": ["GRANT SELECT ON OBJECT::[dbo].[t]]x] ([c]]])"
                        " WITH GRANT OPTION"]}}
    out = {"missing_on_target": sorted(s), "grants_missing": [],
           "attributes_differ": []}
    steps, skipped = users._ms_plan(hop, out, s, {})
    assert skipped == []
    ran = [st["sql"] for st in steps]
    q = "[a]]; DROP LOGIN sa; --]"
    assert ran == [
        f"CREATE LOGIN {q} WITH PASSWORD = 0x0200ff HASHED, SID = 0x01ab,"
        " DEFAULT_DATABASE = [shop], CHECK_POLICY = ON, CHECK_EXPIRATION ="
        " OFF",
        f"ALTER SERVER ROLE [dbcreator] ADD MEMBER {q}",
        f"CREATE USER {q} FOR LOGIN {q} WITH DEFAULT_SCHEMA = [s]]x]",
        f"ALTER ROLE [db_datareader] ADD MEMBER {q}",
        f"GRANT SELECT ON OBJECT::[dbo].[t]]x] ([c]]]) TO {q} WITH GRANT"
        " OPTION"]
    assert "0200ff" not in steps[0]["shown"]
    assert steps[0]["undo"] == ["master", f"DROP LOGIN {q}"]


def test_sql_server_permissions_keep_their_column_class_and_option():
    from migkit import users
    on = users._ms_securable(1, "dbo", "t", "c", None)
    assert on == "OBJECT::[dbo].[t] ([c])"
    assert users._ms_securable(4, None, "u", None, "S") == "USER::[u]"
    assert users._ms_securable(4, None, "r", None, "R") == "ROLE::[r]"
    assert users._ms_securable(25, None, "c", None, None) == \
        "CERTIFICATE::[c]"
    assert users._ms_securable(0, None, None, None, None) == ""
    # a column grant is not a table grant, nor CONTROL on one user the
    # whole database's
    assert users._ms_permission("GRANT", "SELECT", on) == \
        "GRANT SELECT ON OBJECT::[dbo].[t] ([c])"
    assert users._ms_permission("GRANT_WITH_GRANT_OPTION", "CONTROL",
                                "USER::[u]") == \
        "GRANT CONTROL ON USER::[u] WITH GRANT OPTION"
    assert users._ms_permission("GRANT", "SELECT; DROP", on) is None
    assert users._ms_permission("GRANT", "SELECT", None) is None
    assert users._ms_revoke("GRANT CONTROL ON USER::[u] WITH GRANT OPTION",
                            "[v]") == "REVOKE CONTROL ON USER::[u] FROM [v]" \
                                      " CASCADE"
    assert users._ms_revoke("DENY SELECT ON OBJECT::[dbo].[t]", "[v]") == \
        "REVOKE SELECT ON OBJECT::[dbo].[t] FROM [v]"


def test_other_engines_quote_by_their_own_rule():
    from migkit import users
    assert users._cs_ident('r" WITH SUPERUSER = true --') == \
        '"r"" WITH SUPERUSER = true --"'
    assert users._ora_ident("APP;X") == '"APP;X"'
    with pytest.raises(ValueError):
        users._ora_ident('X" IDENTIFIED BY y --')
    assert users._os_name("../_security/api/roles?x") == \
        "..%2F_security%2Fapi%2Froles%3Fx"
    assert users._os_pointer("a/b~c") == "a~1b~0c"


def test_one_list_of_system_principals():
    from migkit import users
    for eng, name in (("mysql", "tencentroot"), ("mysql", "AWS_x"),
                      ("mysql", "cloudsqlsuperuser"), ("postgres", "postgres"),
                      ("postgres", "azure_pg_admin"),
                      ("postgres", "cloudsqlsuperuser"),
                      ("postgres", "rds_superuser"), ("mssql", "##MS_x##"),
                      ("mssql", "nt authority\\system")):
        assert users.system_principal(eng, name), (eng, name)
    for eng, name in (("postgres", "app_tencent_rw"), ("mysql", "app"),
                      ("mssql", "app")):
        assert not users.system_principal(eng, name), (eng, name)
    sql = users.system_principals_sql("postgres", "rolname")
    assert "rolname like 'pg\\_%'" in sql and "'postgres'" in sql
    assert users._pg_sysrole("pg_monitor") and users._mysql_sysuser("root")


# ---- on real servers ----------------------------------------------------------

def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


docker = pytest.mark.skipif(not _docker(), reason="docker not available")


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _up(names_ports, image, env, inner, ready):
    for n, p in names_ports:
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n,
                        *[x for k, v in env.items() for x in ("-e",
                                                              f"{k}={v}")],
                        "-p", f"127.0.0.1:{p}:{inner}", image],
                       check=True, capture_output=True)
    for n, p in names_ports:
        assert _wait(p), f"{n} never listened"
        for _ in range(90):
            try:
                ready(p)
                break
            except Exception:
                time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")


def _down(names_ports):
    for n, _ in names_ports:
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


PG = (("migkit-test-hostile-pg-src", 16470),
      ("migkit-test-hostile-pg-dst", 16471))
MY = (("migkit-test-hostile-my-src", 16472),
      ("migkit-test-hostile-my-dst", 16473))


def _pg(port, user="postgres", password="CHANGE_ME-pg", db="postgres"):
    import psycopg2
    c = psycopg2.connect(host="127.0.0.1", port=port, user=user,
                         password=password, dbname=db, connect_timeout=5)
    c.autocommit = True
    return c


def _pg_hop(tmp_path, target_user="postgres", target_pw="CHANGE_ME-pg"):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="hostile", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PG[0][1],
                              user="postgres", password="CHANGE_ME-pg"),
              target=Endpoint(host="127.0.0.1", port=PG[1][1],
                              user=target_user, password=target_pw),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@docker
@pytest.mark.docker
class TestPostgres:
    @pytest.fixture(scope="class")
    def pair(self):
        _up(PG, "postgres:16", {"POSTGRES_PASSWORD": "CHANGE_ME-pg"}, 5432,
            lambda p: _pg(p).close())
        from psycopg2 import sql
        c = _pg(PG[0][1])
        cur = c.cursor()
        cur.execute("create role zreaders")
        for i, r in enumerate(HOSTILE_PG):
            cur.execute(sql.SQL("create role {} login password {}").format(
                sql.Identifier(r), sql.Literal(f"CHANGE_ME-{i}")))
            cur.execute(sql.SQL("grant zreaders to {}").format(
                sql.Identifier(r)))
        # a group on both sides, a member of zreaders on the source only:
        # the membership is carried, and taken back by a rollback that
        # leaves the group itself where it was
        cur.execute("create role app_group")
        cur.execute("grant zreaders to app_group")
        c.close()
        c = _pg(PG[1][1])
        c.cursor().execute("create role app_group")
        c.close()
        yield
        _down(PG)

    def test_hostile_roles_land_as_themselves_and_nothing_else(self, pair,
                                                               tmp_path):
        from migkit import users
        hop = _pg_hop(tmp_path)
        out, _ = users.compare(hop, [].append)
        assert set(HOSTILE_PG) | {"zreaders"} <= set(
            out["missing_on_target"]), out
        users.create(hop, apply=True, say=[].append)
        c = _pg(PG[1][1])
        cur = c.cursor()
        cur.execute("select rolname, rolsuper from pg_roles")
        roles = dict(cur.fetchall())
        # every name there as itself, none a superuser, and no role a
        # piece of a name made
        assert set(HOSTILE_PG) | {"zreaders"} <= set(roles), roles
        assert [r for r, sup in roles.items() if sup] == ["postgres"], roles
        assert not {"x", "a", "b", "evil", "100"} & set(roles), roles
        cur.execute("select m.rolname from pg_auth_members a join pg_roles"
                    " g on g.oid = a.roleid join pg_roles m on m.oid ="
                    " a.member where g.rolname = 'zreaders'")
        assert {r for (r,) in cur.fetchall()} == set(HOSTILE_PG)
        c.close()
        # the hash carried signs the hostile role in with its own password
        _pg(PG[1][1], user=HOSTILE_PG[0], password="CHANGE_ME-0").close()
        out, _ = users.compare(hop, [].append)
        assert out["result"] == "pass", out
        c = _pg(PG[1][1])
        cur = c.cursor()
        cur.execute("select pg_has_role('app_group', 'zreaders', 'member')")
        assert cur.fetchone()[0] is True
        c.close()
        users.rollback(hop, apply=True, say=[].append)
        c = _pg(PG[1][1])
        cur = c.cursor()
        cur.execute("select rolname from pg_roles")
        left = {r for (r,) in cur.fetchall()}
        assert not (set(HOSTILE_PG) | {"zreaders"}) & left, left
        # the group that was there before is there still, without the
        # membership create gave it
        assert "app_group" in left
        c.close()

    def test_a_target_that_hides_its_hashes_is_not_a_pass(self, pair,
                                                          tmp_path):
        """The source's hashes are read and the target's are not: whether
        they match is unknown, never `pass` (the usual self-run to managed
        direction printed "password differs (0)" and passed)."""
        from psycopg2 import sql

        from migkit import users
        c = _pg(PG[1][1])
        cur = c.cursor()
        cur.execute("drop role if exists migkit_ro")
        cur.execute("create role migkit_ro login password 'CHANGE_ME-ro'")
        # on both sides, with another password here
        cur.execute(sql.SQL("create role {} login password"
                            " 'CHANGE_ME-other'").format(
            sql.Identifier(HOSTILE_PG[0])))
        c.close()
        hop = _pg_hop(tmp_path, "migkit_ro", "CHANGE_ME-ro")
        out, _ = users.compare(hop, [].append)
        assert out["password_differs"] is None, out
        assert out["result"] == "unknown", out
        assert any("hides pg_authid" in w for w in out["not_compared"])


def _my(port, user="root", password="CHANGE_ME-my"):
    import pymysql
    return pymysql.connect(host="127.0.0.1", port=port, user=user,
                           password=password, autocommit=True,
                           connect_timeout=5)


HOSTILE_MY = [("x' IDENTIFIED BY 'p", "%"), ("a`b", "%"), ("100%s", "%"),
              ("ｘ＇ --", "10.0.0.%")]


@docker
@pytest.mark.docker
class TestMySQL:
    @pytest.fixture(scope="class")
    def pair(self):
        _up(MY, "mysql:8.4", {"MYSQL_ROOT_PASSWORD": "CHANGE_ME-my"}, 3306,
            lambda p: _my(p).close())
        c = _my(MY[0][1])
        cur = c.cursor()
        cur.execute("create database app")
        cur.execute("create role %s@%s", ("r;--", "%"))
        cur.execute("grant select on app.* to %s@%s", ("r;--", "%"))
        for i, (u, h) in enumerate(HOSTILE_MY):
            cur.execute("create user %s@%s identified by %s",
                        (u, h, f"CHANGE_ME-{i}"))
            cur.execute("grant %s@%s to %s@%s", ("r;--", "%", u, h))
            cur.execute("alter user %s@%s default role %s@%s",
                        (u, h, "r;--", "%"))
            cur.execute("grant insert on app.* to %s@%s", (u, h))
        c.close()
        c = _my(MY[1][1])
        c.cursor().execute("create database app")
        c.close()
        yield
        _down(MY)

    def _accounts(self, port):
        c = _my(port)
        cur = c.cursor()
        cur.execute("select user, host from mysql.user")
        got = {(u, h) for u, h in cur.fetchall()}
        c.close()
        return got

    def test_hostile_accounts_land_as_themselves_and_nothing_else(
            self, pair, tmp_path):
        from migkit import users
        from migkit.config import Endpoint, Hop
        hop = Hop(name="hostile", engine="mysql",
                  source=Endpoint(host="127.0.0.1", port=MY[0][1],
                                  user="root", password="CHANGE_ME-my"),
                  target=Endpoint(host="127.0.0.1", port=MY[1][1],
                                  user="root", password="CHANGE_ME-my"),
                  databases=["app"])
        hop.report_dir = lambda db=None: tmp_path
        before = self._accounts(MY[1][1])
        users.create(hop, apply=True, say=[].append)
        made = self._accounts(MY[1][1]) - before
        # exactly the source's accounts, the role first among them
        assert made == set(HOSTILE_MY) | {("r;--", "%")}, made
        out, _ = users.compare(hop, [].append)
        assert out["result"] == "pass", out
        # the role, its grant and the default role came with each account
        c = _my(MY[1][1])
        cur = c.cursor()
        cur.execute("select from_user, to_user from mysql.role_edges")
        assert {t for f, t in cur.fetchall() if f == "r;--"} == {
            u for u, _ in HOSTILE_MY}
        cur.execute("select count(*) from mysql.default_roles where"
                    " default_role_user = 'r;--'")
        assert cur.fetchone()[0] == len(HOSTILE_MY)
        c.close()
        # the hash carried signs the hostile account in with its password
        _my(MY[1][1], user=HOSTILE_MY[0][0], password="CHANGE_ME-0").close()
        users.rollback(hop, apply=True, say=[].append)
        assert self._accounts(MY[1][1]) == before
