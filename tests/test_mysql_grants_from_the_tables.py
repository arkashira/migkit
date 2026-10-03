"""MySQL's grants read from the grant tables, every kind of them, in one
statement a table a side, and repaired statement by statement with what
undoes each (audit 2026-10-01, SC10).

The grants were read by `SHOW GRANTS`, an account, a database and a side
at a time, each on a connection of its own, and only the lines naming the
database were kept: a global grant, a role and its default, a grant on a
pattern of databases (`app\\_%.*`) and a routine's were invisible to the
check, so an application that lost any of them on the target read as
having the same rights.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine
from tests import servers
from tests.conftest import needs_docker

pair = servers.fixture("mysql")


def _engine(**kw):
    return MySQLEngine(Hop(name="g", engine="mysql", source=Endpoint(),
                           target=Endpoint(), **kw))


# ---- pure ----------------------------------------------------------------

def test_a_privilege_column_is_named_as_grant_names_it():
    p = MySQLEngine._priv_of
    assert p("Select_priv") == "SELECT"
    assert p("Create_tmp_table_priv") == "CREATE TEMPORARY TABLES"
    assert p("Repl_slave_priv") == "REPLICATION SLAVE"
    assert p("Show_db_priv") == "SHOW DATABASES"
    assert p("Create_routine_priv") == "CREATE ROUTINE"
    assert p("Grant_priv") == MySQLEngine.OPTION
    assert MySQLEngine._privs_in("Select,Insert,Grant") == {
        "SELECT", "INSERT", MySQLEngine.OPTION}
    assert MySQLEngine._privs_in("Execute,Alter Routine") == {
        "EXECUTE", "ALTER ROUTINE"}
    assert MySQLEngine._privs_in("") == set()


@pytest.mark.parametrize("pattern,name,covers", [
    ("shop", "shop", True),
    ("shop", "shops", False),
    ("app\\_%", "app_one", True),
    ("app\\_%", "appXone", False),
    ("app_%", "appXone", True),          # an unescaped _ is any character
    ("%", "anything", True),
    ("a\\%b", "a%b", True),
    ("a\\%b", "axb", False),
])
def test_a_database_grant_covers_what_the_server_says_it_covers(
        pattern, name, covers):
    assert MySQLEngine._db_matches(pattern, name) is covers


def _items(**accounts):
    return {who.replace("_at_", "@"): set(items)
            for who, items in accounts.items()}


def _with(src, dst, accounts=("app@%",), roles=(), defaults=None,
          error=""):
    eng = _engine()

    def privileges(side, fresh=False):
        return {"accounts": set(accounts), "roles": set(roles),
                "items": src if side == "src" else dst,
                "default": (defaults or {}).get(side, {}),
                "error": error if side == "src" else ""}
    eng._privileges = privileges
    eng._side_brand = lambda side: type("B", (), {"name": "mysql"})()
    return eng


ALL_KINDS = {
    ("global", "", "*.*", "PROCESS"),
    ("global", "", "*.*", "BACKUP_ADMIN"),
    ("db", "shop", "*", "SELECT"),
    ("db", "sho%", "*", "INSERT"),
    ("table", "shop", "orders", "UPDATE"),
    ("column", "shop", "orders.note", "UPDATE"),
    ("routine", "shop", "PROCEDURE settle", "EXECUTE"),
    ("role", "", "reader@%", "ROLE"),
}


def test_every_kind_of_grant_the_target_lacks_is_a_difference():
    for item in sorted(ALL_KINDS):
        eng = _with({"app@%": set(ALL_KINDS)},
                    {"app@%": set(ALL_KINDS) - {item}})
        got = eng._check_grants("shop")
        assert got[0].status == "diff", (item, got[0].detail)
        assert "missing on target" in got[0].detail, (item, got[0].detail)
    same = _with({"app@%": set(ALL_KINDS)}, {"app@%": set(ALL_KINDS)})
    assert same._check_grants("shop")[0].status == "ok"


def test_a_default_role_the_target_lacks_is_a_difference():
    held = {"app@%": {("role", "", "reader@%", "ROLE")}}
    eng = _with(held, held, defaults={
        "src": {"app@%": frozenset({"reader@%"})}, "dst": {}})
    got = eng._check_grants("shop")
    assert got[0].status == "diff" and "DEFAULT ROLE reader@%" in \
        got[0].detail, got[0].detail


def test_another_databases_grants_are_not_this_ones():
    eng = _with({"app@%": {("db", "shop", "*", "SELECT"),
                           ("table", "other", "t", "SELECT")}},
                {"app@%": {("db", "shop", "*", "SELECT")}})
    assert eng._check_grants("shop")[0].status == "ok"


def test_unreadable_grant_tables_are_never_the_same():
    eng = _with({}, {}, error="OperationalError: (1142, 'denied')")
    got = eng._check_grants("shop")
    assert got[0].status == "warn" and "could not read" in got[0].detail


def test_a_hop_that_names_its_accounts_compares_those():
    eng = _with({"app@%": {("db", "shop", "*", "SELECT")},
                 "ops@%": {("db", "shop", "*", "SELECT")}},
                {"app@%": {("db", "shop", "*", "SELECT")}},
                accounts=("app@%", "ops@%"))
    eng.hop.objects = {"roles": ["app"]}
    assert eng._check_grants("shop")[0].status == "ok"


def test_the_repair_grants_each_kind_and_undoes_each():
    eng = _with({"app@%": set(ALL_KINDS)}, {"app@%": set()},
                defaults={"src": {"app@%": frozenset({"reader@%"})}})
    act = eng._grant_repair("shop")
    said = "\n".join(act.statements)
    assert "GRANT BACKUP_ADMIN, PROCESS ON *.* TO 'app'@'%';" in said, said
    assert "GRANT SELECT ON `shop`.* TO 'app'@'%';" in said
    assert "GRANT INSERT ON `sho%`.* TO 'app'@'%';" in said
    assert "GRANT UPDATE ON `shop`.`orders` TO 'app'@'%';" in said
    assert "GRANT UPDATE (`note`) ON `shop`.`orders` TO 'app'@'%';" in said
    assert "GRANT EXECUTE ON PROCEDURE `shop`.`settle` TO 'app'@'%';" \
        in said
    assert "GRANT 'reader'@'%' TO 'app'@'%';" in said
    assert "SET DEFAULT ROLE 'reader'@'%' TO 'app'@'%';" in said
    undone = "\n".join(act.undo)
    assert "REVOKE BACKUP_ADMIN, PROCESS ON *.* FROM 'app'@'%';" in undone, \
        undone
    assert "REVOKE 'reader'@'%' FROM 'app'@'%';" in undone
    assert "SET DEFAULT ROLE NONE TO 'app'@'%';" in undone
    # the default role set back before its role is taken away
    assert act.undo.index("SET DEFAULT ROLE NONE TO 'app'@'%';") < \
        act.undo.index("REVOKE 'reader'@'%' FROM 'app'@'%';")
    # an account's own grants are planned once a run, not once a database
    again = eng._grant_repair("shop")
    assert again is not None and not any(
        "PROCESS" in s or "reader" in s for s in again.statements), \
        again.statements


def test_a_grant_option_alone_is_given_with_what_is_held_and_taken_back():
    have = {("db", "shop", "*", "SELECT")}
    eng = _with({"app@%": have | {("db", "shop", "*", "GRANT OPTION")}},
                {"app@%": set(have)})
    act = eng._grant_repair("shop")
    assert act.statements == [
        "GRANT SELECT ON `shop`.* TO 'app'@'%' WITH GRANT OPTION;"]
    assert act.undo == ["REVOKE GRANT OPTION ON `shop`.* FROM 'app'@'%';"]


def test_an_account_name_stays_a_name():
    hostile = "x'@'%' identified by 'p'; --@%"
    eng = _with({hostile: {("db", "shop", "*", "SELECT")}},
                {hostile: set()}, accounts=(hostile,))
    act = eng._grant_repair("shop")
    assert act.statements == [
        "GRANT SELECT ON `shop`.* TO 'x''@''%'' identified by ''p''; --'"
        "@'%';"], act.statements


def test_the_ignored_accounts_are_declared():
    from migkit import envkeys
    assert envkeys.declared("GRANTS_IGNORE_ROLES").kind == "list"
    assert envkeys.items("GRANTS_IGNORE_ROLES",
                         {"GRANTS_IGNORE_ROLES": "a, b"}) == ["a", "b"]


# ---- on a server -----------------------------------------------------------

@needs_docker
def test_grants_of_every_kind_are_read_compared_and_repaired(pair,
                                                            tmp_path):
    """Each kind given on the source, none on the target: read from the
    tables, called different one by one, granted by the repair, the
    same after, and the undo puts the target back as it was."""
    db = pair.name
    for s in (pair.src, pair.dst):
        s.sql("drop role if exists 'reader'; create role 'reader';"
              " drop user if exists 'app'@'%'; create user 'app'@'%'"
              " identified by 'CHANGE_ME'", db)
        s.sql("create table orders (id int primary key, note text);"
              " create procedure settle() begin select 1; end", db)
    pair.src.sql(f"grant process on *.* to 'app'@'%';"
                 f" grant select on `{db}`.* to 'app'@'%';"
                 f" grant update (note) on `{db}`.orders to 'app'@'%';"
                 f" grant execute on procedure `{db}`.settle to 'app'@'%';"
                 " grant 'reader' to 'app'@'%';"
                 " set default role 'reader' to 'app'@'%'")
    hop = Hop(name="g", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    eng = MySQLEngine(hop)
    got = eng._check_grants(db)[0]
    assert got.status == "diff", got.detail
    for said in ("GRANT PROCESS ON *.*", "GRANT SELECT ON <db>.*",
                 "GRANT EXECUTE ON PROCEDURE <db>.settle",
                 "GRANT UPDATE (note) ON <db>.orders", "GRANT reader@%",
                 "DEFAULT ROLE reader@%"):
        assert said in str(eng._grants_for_db("src", db)[0]["app@%"]), said
    act = eng._grant_repair(db)
    for stmt in act.statements:
        pair.dst.sql(stmt.rstrip(";"))
    again = MySQLEngine(hop)
    assert again._check_grants(db)[0].status == "ok", \
        again._check_grants(db)[0].detail
    for stmt in act.undo:
        pair.dst.sql(stmt.rstrip(";"))
    back = MySQLEngine(hop)._grants_for_db("dst", db)[0]
    assert not back.get("app@%"), back
