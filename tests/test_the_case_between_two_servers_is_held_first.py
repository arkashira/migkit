"""The case between a hop's two servers (backlog P0-SCENARIOS): a fork
change, a version down, an edition change - read from the servers, named
in the plan, and held to before the first write, every refusal with its
fix.

The rules and each reading run on stand-ins here, each proved able to say
both "missing" and "there"; the servers themselves in the docker tests
beside this one: PostgreSQL 17 into 14, MySQL 8.4 into MariaDB 10.11,
Redis with its JSON module into Valkey, Elasticsearch beside OpenSearch,
and SQL Server's editions on the x86 VM.
"""
import pytest

from migkit import case, decide, editions, lacks, physical, upgrade, variants
from migkit.config import Endpoint, Hop

E = editions.Edition
GB = editions.GB


def _hop(engine="mysql", goal="cutover", options=None, **kw):
    return Hop(name="h", engine=engine,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               goal=goal, options=options or {}, databases=["app"], **kw)


# --- what a brand has, by its own version -------------------------------------------

def test_a_feature_is_had_from_a_brands_own_version():
    f = lacks.Feature("x", "x things", lambda e, d: [],
                      {"mysql": (8, 0, 13), "mariadb": None, "*": (9,)})
    assert lacks.had(f, lacks.Side("mysql", (8, 0, 36))) is True
    assert lacks.had(f, lacks.Side("mysql", (8, 0, 12))) is False
    assert lacks.had(f, lacks.Side("mariadb", (11, 4, 2))) is False
    # "*" is read only by a brand numbered as the family's own software
    assert lacks.had(f, lacks.Side("tidb", (8, 0, 11), real=False)) is None
    assert lacks.had(f, lacks.Side("yugabytedb", (9, 1, 0))) is True
    assert lacks.had(f, lacks.Side("yugabytedb", (8, 1, 0))) is False
    assert lacks.had(f, lacks.Side("mysql", None)) is None
    only = lacks.Feature("y", "y", lambda e, d: [], {"mysql": (8, 0, 0)})
    assert lacks.had(only, lacks.Side("percona", (8, 0, 36))) is None


def test_what_the_source_uses_and_the_target_lacks_is_named():
    def finds(eng, db):
        return [f"{db}.t.c", f"{db}.u.d"]

    def never(eng, db):
        raise AssertionError("asked of a source that cannot hold it")
    table = (
        lacks.Feature("inv", "invisible columns", finds,
                      {"mysql": (8, 0, 23), "mariadb": (10, 3, 3)},
                      "make them visible"),
        lacks.Feature("fk", "indexes on expressions", never,
                      {"mysql": (8, 0, 13), "mariadb": None}),
        lacks.Feature("srid", "spatial columns", finds, {"mysql": (8, 0, 0)}))
    src = lacks.Side("mariadb", (11, 4, 2), label="MariaDB 11.4.2")
    dst = lacks.Side("mysql", (5, 7, 44), label="MySQL 5.7.44")
    out, looked = lacks._features(table, None, "app", src, dst)
    rows = {r["item"]: r for r in out}
    inv = rows["invisible columns"]
    assert inv["level"] == "fail"
    assert "2 on the source: app.t.c, app.u.d" in inv["detail"], inv
    assert "MySQL 5.7.44 has none (it has them from 8.0.23)" in inv["detail"]
    assert inv["detail"].endswith("[fix] make them visible")
    assert rows["spatial columns"]["level"] == "fail"
    assert "indexes on expressions" not in rows
    assert looked == 2
    # a target that has it: nothing asked, nothing said
    newer = lacks.Side("mysql", (8, 4, 2), label="MySQL 8.4.2")
    assert lacks._features(table[:1], None, "app", src, newer) == ([], 0)
    # nothing on the source uses it: looked for, and nothing said
    none = (lacks.Feature("inv", "invisible columns", lambda e, d: [],
                          {"mysql": (8, 0, 23)}),)
    assert lacks._features(none, None, "app", newer, dst) == ([], 1)
    # a brand the table does not name: unknown, never a pass
    tidb = lacks.Side("tidb", (8, 0, 11), real=False, label="TiDB")
    out, _ = lacks._features(table[:1], None, "app", src, tidb)
    assert out[0]["level"] == "warn"
    assert "unknown, not clean" in out[0]["detail"]


def test_a_pass_is_said_only_where_nothing_is_missing(monkeypatch):
    class Eng:
        CANON_ENGINE = "mysql"
    side = lacks.Side("mysql", (8, 4, 0), label="MySQL 8.4.0")
    monkeypatch.setitem(lacks.READERS, "mysql",
                        lambda hop, eng, db, src, dst: ([], 3))
    got = lacks.checks(_hop(), Eng(), "app", side, side)
    assert [r["level"] for r in got] == ["pass"]
    assert "3 features the target could lack" in got[0]["detail"]
    bad = lacks._row("fail", "vector columns", "1 on the source")
    monkeypatch.setitem(lacks.READERS, "mysql",
                        lambda hop, eng, db, src, dst: ([bad], 3))
    got = lacks.checks(_hop(), Eng(), "app", side, side)
    assert [r["level"] for r in got] == ["fail"]


def test_a_collation_is_asked_of_the_target_in_an_expression(monkeypatch):
    asked = []

    def my(eng, side, sql, args=None):
        if side == "src":
            return [("utf8mb4_0900_ai_ci", "utf8mb4"),
                    ("utf8mb4_uca1400_ai_ci", "utf8mb4"),
                    ("latin1_swedish_ci", "latin1")]
        if "information_schema.collations" in sql:
            return [("latin1_swedish_ci",)]
        asked.append(sql)
        if "utf8mb4_0900_ai_ci" in sql:
            return [("",)]
        raise RuntimeError("(1273, \"Unknown collation:"
                           " 'utf8mb4_uca1400_ai_ci'\")")
    monkeypatch.setattr(upgrade, "_my", my)

    class Eng:
        CANON_ENGINE = "mysql"

        def _d(self, side, db):
            return db
    got = lacks._my_collations(Eng(), ["app"])
    assert got[0]["level"] == "fail", got
    assert "utf8mb4_uca1400_ai_ci" in got[0]["detail"]
    # taken by the target as an alias, though not in its list
    assert "utf8mb4_0900_ai_ci" not in got[0]["detail"]
    assert len(asked) == 2 and "latin1_swedish_ci" not in " ".join(asked)

    def listed(eng, side, sql, args=None):
        if side == "src":
            return [("latin1_swedish_ci", "latin1")]
        return [("latin1_swedish_ci",)]
    monkeypatch.setattr(upgrade, "_my", listed)
    assert lacks._my_collations(Eng(), ["app"])[0]["level"] == "pass"


def _fake_pg(monkeypatch, answers):
    """`upgrade._pg` answering by side and a few words of the statement."""
    def ask(eng, side, db, sql):
        for (s, words), rows in answers.items():
            if s == side and words in " ".join(sql.split()):
                return rows
        return []
    monkeypatch.setattr(upgrade, "_pg", ask)


def test_a_type_or_a_collation_the_target_has_not_is_asked_of_it(
        monkeypatch):
    _fake_pg(monkeypatch, {
        ("src", "join pg_type t"): [["public.t.a", "int4"],
                                    ["public.t.r", "int4multirange"]],
        ("dst", "from pg_type where"): [["int4"]],
        ("src", "join pg_collation k"): [["public.t.n", "en-US-x-icu"]],
        ("dst", "from pg_collation where"): []})
    got = lacks._pg_types(None, "app")[0]
    assert got["level"] == "fail"
    assert "no type int4multirange: public.t.r" in got["detail"], got
    got = lacks._pg_collations(None, "app")[0]
    assert got["level"] == "fail" and "en-US-x-icu" in got["detail"]
    _fake_pg(monkeypatch, {
        ("src", "join pg_type t"): [["public.t.a", "int4"]],
        ("dst", "from pg_type where"): [["int4"]],
        ("src", "join pg_collation k"): [["public.t.n", "en-US-x-icu"]],
        ("dst", "from pg_collation where"): [["en-US-x-icu"]]})
    assert lacks._pg_types(None, "app")[0]["level"] == "pass"
    assert lacks._pg_collations(None, "app")[0]["level"] == "pass"


class _Dbapi:
    """A DB-API engine of the stand-ins, answering by side and words."""

    def __init__(self, engine, answers):
        self.CANON_ENGINE, self.answers = engine, answers

    def _owner(self, side, db):
        return db.upper()

    def _rows(self, side, db, sql, args=None):
        for (s, words), rows in self.answers.items():
            if s == side and words in sql:
                return rows
        return []


def test_sql_servers_types_and_collations_are_asked_of_the_target():
    src = lacks.Side("mssql", (17, 0, 100), label="SQL Server 17")
    dst = lacks.Side("mssql", (16, 0, 4135), label="SQL Server 16")
    eng = _Dbapi("mssql", {
        ("src", "ty.is_user_defined = 0"): [("dbo.t.v", "nvarchar"),
                                            ("dbo.t.j", "json")],
        ("dst", "from sys.types"): [("nvarchar",)],
        ("src", "c.collation_name"): [
            ("dbo.t.v", "Latin1_General_100_CI_AS_SC_UTF8"),
            ("(the database)", "SQL_Latin1_General_CP1_CI_AS")],
        ("dst", "fn_helpcollations"): [("SQL_Latin1_General_CP1_CI_AS",)]})
    by = {r["item"]: r for r in lacks._ms(None, eng, "app", src, dst)[0]}
    assert by["types"]["level"] == "fail"
    assert "no type json: dbo.t.j" in by["types"]["detail"]
    assert by["collations"]["level"] == "fail"
    assert "Latin1_General_100_CI_AS_SC_UTF8" in by["collations"]["detail"]
    eng.answers[("dst", "from sys.types")] = [("nvarchar",), ("json",)]
    eng.answers[("dst", "fn_helpcollations")] = [
        ("SQL_Latin1_General_CP1_CI_AS",),
        ("Latin1_General_100_CI_AS_SC_UTF8",)]
    by = {r["item"]: r for r in lacks._ms(None, eng, "app", src, dst)[0]}
    assert by["types"]["level"] == "pass"
    assert by["collations"]["level"] == "pass"


def test_long_text_is_held_to_the_targets_own_string_size():
    eng = _Dbapi("oracle", {("src", "data_length > 4000"): [("T.NOTE",)],
                            ("dst", "max_string_size"): [("STANDARD",)]})
    got = lacks._ora_long_text(eng, "app")[0]
    assert got["level"] == "fail" and "T.NOTE" in got["detail"]
    eng.answers[("dst", "max_string_size")] = [("EXTENDED",)]
    assert lacks._ora_long_text(eng, "app")[0]["level"] == "pass"
    eng.answers[("src", "data_length > 4000")] = []
    assert lacks._ora_long_text(eng, "app") == []


# --- editions -------------------------------------------------------------------------

def test_editions_are_read_as_each_server_says_them():
    ms = editions.ms_name
    assert ms("Enterprise Edition: Core-based Licensing (64-bit)", 3) == \
        "enterprise"
    assert ms("Developer Edition (64-bit)", 3) == "developer"
    assert ms("Enterprise Evaluation Edition (64-bit)", 3) == "evaluation"
    assert ms("Web Edition (64-bit)", 2) == "web"
    assert ms("Standard Edition (64-bit)", 2) == "standard"
    assert ms("Express Edition with Advanced Services (64-bit)", 4) == \
        "express"
    assert ms("SQL Azure", 5) == "azure sql database"
    assert ms("Azure SQL Edge Developer (64-bit)", 9) == "edge"
    assert ms("", None) == "unknown"
    ora = editions.ora_name
    assert ora("Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 -"
               " Production") == "ee"
    assert ora("Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 -"
               " Production") == "se2"
    assert ora("Oracle Database 21c Express Edition Release 21.0.0.0.0 -"
               " Production") == "xe"
    assert ora("Oracle Database 23ai Free Release 23.0.0.0.0 - Develop,"
               " Learn, and Run for Free") == "free"
    assert ora("nothing said") == "unknown"


def test_an_edition_change_has_a_direction():
    web, ent, std, dev = (E("mssql", n, editions.MS_RANKS[n])
                          for n in ("web", "enterprise", "standard",
                                    "developer"))
    assert editions.direction(web, ent) == "up"
    assert editions.direction(ent, std) == "down"
    assert editions.direction(ent, dev) == "another"
    assert editions.direction(ent, ent) == "same"
    assert editions.direction(ent, E("mssql")) is None
    said = editions.words(ent, std)
    assert said.startswith("an edition down: SQL Server Enterprise to SQL"
                           " Server Standard") and "licence-driven" in said
    assert editions.words(web, ent).startswith("an edition up: SQL Server"
                                               " Web to SQL Server"
                                               " Enterprise")
    assert editions.words(ent, ent) == ""


def test_what_an_edition_has_follows_its_version():
    f = editions.MS_FEATURES
    std14 = E("mssql", "standard", 2, version=(12, 0, 6024))
    std16rtm = E("mssql", "standard", 2, version=(13, 0, 1601))
    std16sp1 = E("mssql", "standard", 2, version=(13, 0, 4001))
    std17 = E("mssql", "standard", 2, version=(14, 0, 1000))
    std19 = E("mssql", "standard", 2, version=(15, 0, 2000))
    exp22 = E("mssql", "express", 0, version=(16, 0, 1000))
    web22 = E("mssql", "web", 1, version=(16, 0, 1000))
    assert f["Compression"].has(std14) is False
    assert f["Compression"].has(std16rtm) is False
    assert f["Compression"].has(std16sp1) is True
    assert f["Partitioning"].has(exp22) is True
    assert f["TransparentDataEncryption"].has(std17) is False
    assert f["TransparentDataEncryption"].has(std19) is True
    assert f["TransparentDataEncryption"].has(web22) is False
    assert f["ChangeCapture"].has(web22) is False
    assert f["ChangeCapture"].has(std16sp1) is True
    assert f["Compression"].has(E("mssql", "edge", None,
                                  version=(15, 0, 2000))) is None
    o = editions.ORA_FEATURES
    assert o["partitioning"].has(E("oracle", "se2", 1,
                                   version=(19, 0, 0))) is False
    assert o["partitioning"].has(E("oracle", "ee", 2,
                                   version=(19, 0, 0))) is True
    assert o["partitioning"].has(E("oracle", "xe", 0,
                                   version=(21, 0, 0))) is True
    assert o["partitioning"].has(E("oracle", "xe", 0,
                                   version=(11, 2, 0))) is False


def test_an_editions_caps_follow_its_version():
    cap = editions.caps
    assert cap(E("mssql", "express", 0, version=(16, 0, 1)))["database"] \
        == 10 * GB
    assert cap(E("mssql", "express", 0, version=(17, 0, 1)))["database"] \
        == 50 * GB
    assert cap(E("mssql", "standard", 2, version=(16, 0, 1)))["cores"] == 24
    assert cap(E("mssql", "standard", 2, version=(12, 0, 1)))["cores"] == 16
    assert cap(E("mssql", "enterprise", 3, version=(16, 0, 1))) == {}
    assert cap(E("oracle", "xe", 0, version=(21, 0, 0)))["database"] == \
        12 * GB
    assert cap(E("oracle", "se2", 1, version=(19, 0, 0)))["cores"] == 16


def test_an_edition_down_is_held_to_what_the_source_uses(monkeypatch):
    ent = E("mssql", "enterprise", 3, "Enterprise Edition", (16, 0, 4135))
    std17 = E("mssql", "standard", 2, "Standard Edition", (14, 0, 3456))
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (ent, std17))
    monkeypatch.setattr(editions, "used", lambda eng, db, src: [
        editions.Use("TransparentDataEncryption", (db,)),
        editions.Use("Compression", (db,))])
    monkeypatch.setattr(editions, "load", lambda eng, db, engine: {
        "database": 5 * GB})
    by = {r["item"]: r for r in editions.checks(_hop("mssql"), object(),
                                                ["app"])}
    assert by["editions"]["level"] == "warn"
    assert "licence-driven" in by["editions"]["detail"]
    tde = by["transparent data encryption"]
    assert tde["level"] == "fail" and "lost on the target" in tde["detail"]
    assert "edition: {accept: [TransparentDataEncryption]}" in tde["detail"]
    # Standard has had compression from 2016 SP1: not missing
    assert "data compression" not in by
    # the loss accepted by the hop: said, not refused
    hop = _hop("mssql", options={"edition": {
        "accept": ["TransparentDataEncryption"]}})
    by = {r["item"]: r for r in editions.checks(hop, object(), ["app"])}
    assert by["transparent data encryption"]["level"] == "warn"
    assert "accepted by the hop" in by["transparent data encryption"][
        "detail"]


def test_an_editions_caps_are_held_to_the_source(monkeypatch):
    ent = E("mssql", "enterprise", 3, "Enterprise Edition", (16, 0, 4135))
    exp = E("mssql", "express", 0, "Express Edition", (16, 0, 1000))
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (ent, exp))
    monkeypatch.setattr(editions, "used", lambda eng, db, src: [
        editions.Use("Compression", (db,))])
    monkeypatch.setattr(editions, "load", lambda eng, db, engine: {
        "database": 12 * GB, "memory": 20 * GB, "cores": 8})
    by = {r["item"]: r for r in editions.checks(_hop("mssql"), object(),
                                                ["app"])}
    assert by["the edition's size"]["level"] == "fail"
    assert "12.0 GiB of data" in by["the edition's size"]["detail"]
    assert by["the edition's memory"]["level"] == "warn"
    assert by["the edition's cores"]["level"] == "warn"
    assert "data compression" not in by
    monkeypatch.setattr(editions, "load", lambda eng, db, engine: {
        "database": 2 * GB, "memory": GB, "cores": 2})
    by = {r["item"]: r for r in editions.checks(_hop("mssql"), object(),
                                                ["app"])}
    assert by["the edition's size"]["level"] == "pass"
    assert "the edition's memory" not in by and "the edition's cores" \
        not in by


def test_what_a_logical_copy_leaves_behind_is_stripped(monkeypatch):
    ent14 = E("mssql", "enterprise", 3, "Enterprise Edition", (12, 0, 6024))
    std14 = E("mssql", "standard", 2, "Standard Edition", (12, 0, 6024))
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (ent14,
                                                                std14))
    monkeypatch.setattr(editions, "used", lambda eng, db, src: [
        editions.Use("Compression", (db,))])
    monkeypatch.setattr(editions, "load", lambda eng, db, engine: {})
    by = {r["item"]: r for r in editions.checks(_hop("mssql"), object(),
                                                ["app"])}
    assert by["data compression"]["level"] == "warn"
    assert "the rows the same" in by["data compression"]["detail"]
    assert editions.stripped(object(), "app", ent14, std14) == [
        "data compression"]
    assert editions.refusals(object(), "app", ent14, std14) == [
        "data compression (Compression)"]
    # the same edition on both: nothing asked of what the source uses
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (ent14,
                                                                ent14))
    assert [r["level"] for r in editions.checks(_hop("mssql"), object(),
                                                ["app"])] == ["pass"]
    assert editions.refusals(object(), "app", ent14, ent14) == []


def test_a_mysql_target_that_runs_the_plugin_lacks_nothing(monkeypatch):
    ent = E("mysql", "enterprise", 1, extra={
        "brand": "mysql", "plugins": ["audit_log", "thread_pool"],
        "components": []})
    com = E("mysql", "community", 0, extra={"brand": "mysql",
                                            "plugins": [], "components": []})
    com_audit = E("mysql", "community", 0, extra={
        "brand": "mysql", "plugins": ["audit_log"], "components": []})
    perc = E("mysql", "percona server", 0, extra={
        "brand": "percona", "plugins": [], "components": []})
    monkeypatch.setattr(editions, "used", lambda eng, db, s: [
        editions.Use("audit", ("(the server)",)),
        editions.Use("thread pool", ("(the server)",))])
    got = {f.key: have for f, u, have in editions.lacking(object(), "app",
                                                          ent, com)}
    assert got == {"audit": False, "thread pool": False}
    got = {f.key for f, u, have in editions.lacking(object(), "app", ent,
                                                    com_audit)}
    assert got == {"thread pool"}
    # Percona Server has its own audit log and thread pool
    assert editions.lacking(object(), "app", ent, perc) == []
    assert editions._level(editions.MY_FEATURES["audit"][1], False,
                           _hop()) == "fail"
    assert editions._level(editions.MY_FEATURES["thread pool"][1], False,
                           _hop()) == "warn"


def test_what_uses_an_enterprise_plugin_is_found(monkeypatch):
    def my(eng, side, sql, args=None):
        if "mysql.user" in sql:
            return [("ldap_app@%", "authentication_ldap_sasl")]
        if "information_schema.routines" in sql:
            return [("view masked", "select mask_inner(card, 4, 4) from t"),
                    ("view plain", "select card from t")]
        return []
    monkeypatch.setattr(upgrade, "_my", my)

    class Eng:
        CANON_ENGINE = "mysql"

        def _d(self, side, db):
            return db
    src = E("mysql", "enterprise", 1, extra={"plugins": ["data_masking"],
                                             "components": []})
    got = {u.key: u.where for u in editions._my_used(Eng(), "app", src)}
    assert got["masking"] == ("view masked",)
    assert got["sign-in authentication_ldap_sasl"] == ("ldap_app@%",)
    assert "audit" not in got
    f = editions.feature("mysql", "sign-in authentication_ldap_sasl")
    assert f.fate == editions.LOST
    assert f.has(E("mysql", "community", 0, extra={"brand": "mysql"})) is \
        False
    assert f.has(E("mysql", "percona server", 0,
                   extra={"brand": "percona"})) is True


# --- the case ------------------------------------------------------------------------

class _Brands:
    """An engine of the stand-ins: its brands as `variants` reads them."""

    def __init__(self, engine, src, dst):
        self.CANON_ENGINE = self.ENGINE_FAMILY = engine
        self._b = (variants.identify(engine, src),
                   variants.identify(engine, dst))

    def _brands(self):
        return self._b


_MY84 = {"version": "8.4.2", "version_comment": "MySQL Community Server -"
                                                " GPL"}
_MY80 = {"version": "8.0.36", "version_comment": "MySQL Community Server -"
                                                 " GPL"}
_MARIA = {"version": "11.4.2-MariaDB-ubu2404",
          "version_comment": "mariadb.org binary distribution"}


def _pg(v):
    return {"version": f"PostgreSQL {v} on x86_64-pc-linux-gnu",
            "server_version": v}


@pytest.fixture
def standing(monkeypatch):
    """The servers' own readings the case asks besides the brands, as the
    stand-ins give them."""
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (None, None))
    monkeypatch.setattr(upgrade, "versions", lambda h, e, d: (None, None))
    monkeypatch.setattr(case, "_distribution", lambda eng, hop, side: "")


def test_the_case_is_named_from_both_servers(standing, monkeypatch):
    c = case.recognise(_hop(), _Brands("mysql", _MY80, _MARIA), "app")
    assert c.fork and c.version is None
    assert c.words().startswith("a fork change: MySQL 8.0.36 to MariaDB"
                                " 11.4.2")
    c = case.recognise(_hop(), _Brands("mysql", _MY84, _MY80), "app")
    assert not c.fork and c.version == "down"
    assert c.words() == "a version down: 8.4.2 to 8.0.36"
    c = case.recognise(_hop(), _Brands("mysql", _MY80, _MY84), "app")
    assert c.version == "up"
    c = case.recognise(_hop(), _Brands("mysql", _MY84, _MY84), "app")
    assert c.version == "same" and not c.fork
    assert c.words().startswith("one server to another of the same kind")
    # a distribution the server says it is: a fork, its numbers its own
    monkeypatch.setattr(case, "_distribution",
                        lambda eng, hop, side: "aurora" if side == "dst"
                        else "")
    c = case.recognise(_hop("postgres"), _Brands("postgres", _pg("16.4"),
                                                 _pg("15.8")), "app")
    assert c.fork and c.version == "down"
    assert "PostgreSQL 15.8 (Amazon Aurora)" in c.words()
    assert "a version down: 16.4 to 15.8" in c.words()


def test_a_side_nobody_can_name_is_said_never_guessed(standing,
                                                      monkeypatch):
    monkeypatch.setattr(case, "applies", lambda hop, eng: True)
    monkeypatch.setattr(lacks, "checks", lambda *a, **k: [])
    eng = _Brands("mongodb", {"version": "5.0.0"},
                  {"version": "7.0.12", "gitVersion": "x",
                   "storageEngines": ["wiredTiger"]})
    rows = case.checks(_hop("mongodb"), eng, ["app"])
    by = {r["item"]: r for r in rows}
    assert by["the source's software"]["level"] == "warn"
    assert "cannot say which software" in by["the source's software"][
        "detail"]
    c = case.recognise(_hop("mongodb"), eng, "app")
    assert not c.fork and c.version is None


def test_a_version_down_refuses_the_copy_of_the_pages_up_front(
        standing, monkeypatch):
    monkeypatch.setattr(case, "applies", lambda hop, eng: True)
    monkeypatch.setattr(lacks, "checks", lambda *a, **k: [])
    monkeypatch.setattr(lacks, "gate_only", lambda *a, **k: [])
    phys = {"physical": {"data": "/d", "run": "sh -c", "stop": "true",
                         "start": "true"}}

    def eng():
        return _Brands("postgres", _pg("17.2"), _pg("15.8"))
    rows = case.checks(_hop("postgres", options=phys), eng(), ["app"])
    by = {r["item"]: r for r in rows}
    assert by["what the two servers are"]["detail"] == \
        "a version down: 17.2 to 15.8"
    assert "refused before anything runs" in by["a copy of the pages"][
        "detail"]
    # without a copy of the pages allowed, nothing to refuse
    rows = case.checks(_hop("postgres"), eng(), ["app"])
    assert "a copy of the pages" not in {r["item"] for r in rows}
    # the same version: nothing refused either
    same = _Brands("postgres", _pg("16.4"), _pg("16.2"))
    rows = case.checks(_hop("postgres", options=phys), same, ["app"])
    assert "a copy of the pages" not in {r["item"] for r in rows}
    monkeypatch.setattr(physical, "choose", lambda h, e, m: (
        None, "the source is PostgreSQL 17 and the target 15"))
    lines = case.plan_lines(_hop("postgres", options=phys), eng(), "app")
    assert lines[0] == "the case: a version down: 17.2 to 15.8"
    assert any("a copy of the pages is refused" in ln for ln in lines)
    assert any(ln.strip() == "the way: the logical copy - the source is"
               " PostgreSQL 17 and the target 15" for ln in lines), lines


def test_what_the_target_lacks_is_asked_only_where_the_servers_differ(
        standing, monkeypatch):
    asked = []
    monkeypatch.setattr(case, "applies", lambda hop, eng: True)
    monkeypatch.setattr(lacks, "checks", lambda *a, **k: asked.append(a)
                        or [])
    case.checks(_hop(), _Brands("mysql", _MY84, _MY84), ["app"])
    assert asked == []
    case.checks(_hop(), _Brands("mysql", _MY84, _MY80), ["app"])
    assert len(asked) == 1
    case.checks(_hop(), _Brands("mysql", _MY80, _MARIA), ["app"])
    assert len(asked) == 2


def test_the_gate_holds_to_the_case(monkeypatch):
    from migkit import ready
    monkeypatch.setattr(case, "checks", lambda hop, eng, dbs, quick=False: [
        {"level": "fail", "scope": "the case", "item": "types",
         "detail": "the target has no type vector  [fix] a newer target"},
        {"level": "warn", "scope": "the case", "item": "JSON as text",
         "detail": "2 JSON columns"}])
    got = ready._case(_hop(), object(), ["app"], True)
    assert got[0].gates and got[0].level == "fail"
    assert got[0].what == "the case: types" and got[0].fix == \
        "a newer target"
    assert not got[1].gates


def test_a_hop_of_two_engines_or_nothing_written_has_no_case():
    class Eng:
        CANON_ENGINE = "mysql"
    assert case.checks(_hop(goal="verify"), Eng(), ["app"]) == []
    assert case.checks(_hop("hetero"), Eng(), ["app"]) == []
    # nothing answers at the addresses: the step says it, soon
    nobody = Hop(name="n", engine="mysql", goal="cutover",
                 source=Endpoint(host="127.0.0.1", port=1),
                 target=Endpoint(host="127.0.0.1", port=2),
                 databases=["app"])
    assert case.checks(nobody, Eng(), ["app"]) == []
    assert case.plan_lines(nobody, Eng(), "app") == []


# --- the ways ------------------------------------------------------------------------

def test_an_edition_up_is_an_upgrade():
    same = ("16.0.4135.4", "16.0.4135.4")

    def climb(**facts):
        return decide.climb("the upgrade", upgrade.ways(),
                            decide.Facts(facts))
    got = climb(seed=(None, ""), settings={}, versions=same,
                edition_up=True, step=False)
    assert got.rung.name == "logical", got
    got = climb(seed=(object(), "pages"), settings={}, versions=same,
                edition_up=True, step=False)
    assert got.rung.name == "seeded", got
    # a version step after the copy, and nothing said of it
    got = climb(seed=(object(), "pages"), settings={}, versions=(
        "16.4", "17.2"), edition_up=False, step=True)
    assert got.rung.name == "logical"
    assert "does not say how the target is upgraded" in got.reason
    # the same version and edition: no way is an upgrade
    got = climb(seed=(None, ""), settings={}, versions=same,
                edition_up=False, step=True)
    assert got.rung is None and "or a higher edition" in got.reason


def test_an_upgrade_of_the_edition_alone_is_told_apart(monkeypatch):
    class Eng:
        CANON_ENGINE = "mssql"

        def databases(self):
            return ["app"]
    hop = _hop("mssql", goal="upgrade")
    web, ent, std = (E("mssql", n, editions.MS_RANKS[n])
                     for n in ("web", "enterprise", "standard"))
    monkeypatch.setattr(upgrade, "versions", lambda h, e, d: (
        "16.0.4135.4", "16.0.4135.4"))
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (web, ent))
    assert upgrade.edition_only(hop, Eng()) is True
    row = upgrade.checks(hop, Eng(), "app")[0]
    assert row["level"] == "pass" and "an upgrade of the edition" in \
        row["detail"]
    assert "an edition up: SQL Server Web to SQL Server Enterprise" in \
        row["detail"]
    named = _hop("mssql", goal="upgrade", options={"physical": {
        "upgrade": {"version": "17.0"}}})
    assert upgrade.edition_only(named, Eng()) is False
    assert upgrade.edition_only(_hop("mssql", goal="cutover"), Eng()) is \
        False
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (ent, std))
    assert upgrade.edition_only(hop, Eng()) is False
    monkeypatch.setattr(editions, "pair", lambda eng, db=None: (web, ent))
    monkeypatch.setattr(upgrade, "versions", lambda h, e, d: (
        "15.0.2000.5", "16.0.4135.4"))
    assert upgrade.edition_only(hop, Eng()) is False


def test_a_clone_needs_every_plugin_its_source_runs():
    need = next(n for n in physical._my_needs()
                if n.what.startswith("every plugin"))
    rung = decide.Rung("physical-clone", "the target cloning itself")
    short = {"src_active": {"InnoDB", "clone", "audit_log", "thread_pool"},
             "dst_active": {"InnoDB"}}
    assert physical._plugins_short(short) == ["audit_log", "thread_pool"]
    assert not need.holds(short)
    assert "audit_log" in need.missing(rung, short)
    fine = {"src_active": {"InnoDB", "clone"}, "dst_active": {"InnoDB"}}
    assert need.holds(fine)


class _Facts(dict):
    def __init__(self, hop, mode="full", **kw):
        super().__init__(kw)
        self.hop, self.mode = hop, mode


def _backup_facts(hop, mode="full", **change):
    ent = E("mssql", "enterprise", 3, version=(16, 0, 4135))
    about = {"version": (16, 0, 4135), "text": "16.0.4135.4",
             "engine_edition": 3}
    base = dict(options={"backup": "/var/opt/mssql/backup"},
                src_about=dict(about), dst_about=dict(about),
                followed=["app"], dst_databases={"master"}, our_seed=False,
                editions=(ent, ent), refused={"app": []},
                allocated={"app": 2 * GB}, backup_rights={"app": True},
                create_right=True, tracking={"app": True})
    base.update(change)
    return _Facts(hop, mode, **base)


def test_sql_servers_own_backup_holds_only_where_the_target_takes_it():
    from migkit import backupseed
    rung = backupseed.rungs()[0]
    hop = _hop("mssql", options={"physical": {
        "backup": "/var/opt/mssql/backup"}})

    def why(**change):
        mode = change.pop("mode", "full")
        kept, out = decide.prune([rung], _backup_facts(hop, mode, **change))
        return None if kept else out[0][1]
    assert why() is None
    older = {"version": (15, 0, 2000), "text": "15.0.2000.5",
             "engine_edition": 3}
    assert "older than the source's 16.0.4135.4" in why(dst_about=older)
    ent = E("mssql", "enterprise", 3, version=(16, 0, 4135))
    std = E("mssql", "standard", 2, version=(16, 0, 4135))
    got = why(editions=(ent, std), refused={"app": [
        "transparent data encryption (TransparentDataEncryption)"]})
    assert "does not have what the source's databases hold" in got
    assert "app holds transparent data encryption" in got
    exp = E("mssql", "express", 0, version=(16, 0, 4135))
    got = why(editions=(ent, exp), allocated={"app": 11 * GB})
    assert "10.0 GiB of data files a database" in got and "app (11.0 GiB)" \
        in got
    assert why(editions=(ent, exp), allocated={"app": 9 * GB}) is None
    assert "already holds app" in why(dst_databases={"master", "app"})
    assert why(dst_databases={"master", "app"}, our_seed=True) is None
    assert "change tracking is off for app" in why(
        mode="full+cdc", tracking={"app": False})
    assert why(tracking={"app": False}) is None
    azure = {"version": (12, 0, 2000), "text": "12.0.2000.8",
             "engine_edition": 5}
    assert "backs up to no file" in why(src_about=azure)
    old = {"version": (9, 0, 5000), "text": "9.0.5000.0",
           "engine_edition": 3}
    assert "restores no backup older than version 10" in why(
        src_about=old)
    assert "may not back up app" in why(backup_rights={"app": False})
    assert "may not make a database" in why(create_right=False)
    left = _hop("mssql", options={"physical": {
        "backup": "/var/opt/mssql/backup"}}, exclude=["app.dbo.audit"])
    kept, out = decide.prune([rung], _backup_facts(left))
    assert not kept and "a backup carries every table" in out[0][1]


def test_a_backup_path_is_written_as_its_server_writes_it():
    from migkit import backupseed
    assert backupseed.join("/var/opt/mssql/backup/", "a.bak") == \
        "/var/opt/mssql/backup/a.bak"
    assert backupseed.join("D:\\Backups", "a.bak") == "D:\\Backups\\a.bak"
    assert backupseed.oldest_read(16) == 10
    assert backupseed.oldest_read(13) == 9
    assert backupseed.oldest_read(17) == 11


# --- brands --------------------------------------------------------------------------

def test_forks_are_named_by_what_only_they_say():
    from migkit import unchanged
    b = variants.identify("opensearch", {
        "number": "2.15.0", "distribution": "opensearch",
        "tagline": "The OpenSearch Project: https://opensearch.org/"})
    assert (b.name, b.version) == ("opensearch", "2.15.0")
    b = variants.identify("opensearch", {
        "number": "8.15.0", "build_flavor": "default",
        "tagline": "You Know, for Search"})
    assert (b.name, b.version) == ("elasticsearch", "8.15.0")
    b = variants.identify("opensearch", {
        "number": "7.10.2", "build_flavor": "oss",
        "tagline": "You Know, for Search"})
    assert b.name == "elasticsearch"
    assert variants.identify("opensearch", {"number": "1.0"}).name == \
        "unknown"
    b = variants.identify("postgres", {
        "version": "PostgreSQL 16.4 on x86_64", "server_version": "16.4",
        "citus": "12.1-1"})
    assert (b.name, b.version) == ("citus", "12.1-1")
    assert not unchanged.usable_brand(b)
    assert variants.identify("postgres", {
        "version": "PostgreSQL 16.4 on x86_64",
        "server_version": "16.4"}).name == "postgres"
    b = variants.identify("mongodb", {
        "version": "7.0.12", "gitVersion": "x",
        "storageEngines": ["wiredTiger"], "psmdbVersion": "7.0.12-7"})
    assert (b.name, b.version) == ("percona-server-mongodb", "7.0.12-7")
    assert variants.identify("mongodb", {
        "version": "4.2.0", "cosmos_request_charge": "1.0"}).name == \
        "cosmosdb"
    # an absence names nothing
    assert variants.identify("mongodb", {"version": "5.0.0"}).name == \
        "unknown"


_MAPPING = {"properties": {
    "a": {"type": "flattened"},
    "b": {"type": "text", "fields": {"k": {"type": "keyword"}}},
    "c": {"properties": {"v": {"type": "dense_vector"}}},
    "d": {"type": "knn_vector"}}}


def test_field_types_one_search_engine_has_and_the_other_lacks():
    assert lacks._field_types(_MAPPING) == {
        "a": "flattened", "b": "text", "b.k": "keyword",
        "c.v": "dense_vector", "d": "knn_vector"}

    class Search:
        CANON_ENGINE = "opensearch"

        def neutral_tables(self, side, db):
            return ["events"]

        def _index(self, side, db, t):
            return f"{db}.{t}"

        def _call(self, side, method, path, body=None, ndjson=False,
                  missing_ok=False):
            assert path == "/app.events/_mapping"
            return {"app.events": {"mappings": _MAPPING}}
    es = lacks.Side("elasticsearch", (8, 15, 0), label="Elasticsearch 8.15.0")
    os_ = lacks.Side("opensearch", (2, 11, 0), label="OpenSearch 2.11.0")
    by = {r["item"]: r for r in lacks.checks(_hop("opensearch"), Search(),
                                             "app", es, os_)}
    assert by["flattened fields"]["level"] == "fail"
    assert "events.a" in by["flattened fields"]["detail"]
    assert by["dense_vector fields"]["level"] == "fail"
    assert "knn_vector fields" not in by
    by = {r["item"]: r for r in lacks.checks(_hop("opensearch"), Search(),
                                             "app", os_, es)}
    assert by["knn_vector fields"]["level"] == "fail"
    assert "flattened fields" not in by


class _Redis:
    """A Redis client of the stand-ins."""

    def __init__(self, modules=(), missing=(), keys=None, size=10):
        self.modules, self.missing = modules, missing
        self.keys, self.size = dict(keys or {}), size

    def execute_command(self, *args):
        if args[0] == "COMMAND INFO":
            return [None if c in self.missing else ["x"] for c in args[1:]]
        if args[0] == "MODULE LIST":
            return [["name", m, "ver", 20609] for m in self.modules]
        return []

    def dbsize(self):
        return self.size

    def scan(self, cursor=0, count=None, _type=None):
        return 0, [k for k, t in self.keys.items() if t == _type]


class _RedisEng:
    CANON_ENGINE = "redis"

    def __init__(self, src, dst):
        self.c = {"src": src, "dst": dst}

    def _client(self, side, db=0, decode=True):
        return self.c[side]


def test_a_redis_target_is_asked_for_each_command_and_module():
    got = lacks._redis_commands(_RedisEng(_Redis(), _Redis(
        missing=("restore",))), "0")[0]
    assert got["level"] == "fail" and "no RESTORE" in got["detail"]
    got = lacks._redis_commands(_RedisEng(_Redis(), _Redis()), "0")[0]
    assert got["level"] == "pass"
    used = _Redis(modules=("ReJSON",), keys={"doc:1": "ReJSON-RL"})
    got = lacks._redis_modules(_RedisEng(used, _Redis()), "0")[0]
    assert got["level"] == "fail"
    assert "keys of its types are refused by the target" in got["detail"]
    unused = _Redis(modules=("ReJSON",), keys={"k": "string"})
    got = lacks._redis_modules(_RedisEng(unused, _Redis()), "0")[0]
    assert got["level"] == "pass"
    big = _Redis(modules=("ReJSON",), size=lacks.SCAN_MOST + 1)
    got = lacks._redis_modules(_RedisEng(big, _Redis()), "0")[0]
    assert got["level"] == "fail" and "not proved unused" in got["detail"]
    # the target loads it: nothing said
    assert lacks._redis_modules(_RedisEng(used, _Redis(
        modules=("ReJSON",))), "0") == []


class _MongoColl:
    """A collection that is not there, on a target of the stand-ins: its
    parser refuses the stages and operators it is told it lacks."""

    def __init__(self, lacks_=(), code=40324):
        self.lacks, self.code = set(lacks_), code

    def aggregate(self, pipeline):
        from pymongo.errors import OperationFailure
        for stage in pipeline:
            if next(iter(stage)) in self.lacks:
                raise OperationFailure(f"Unrecognized pipeline stage name:"
                                       f" '{next(iter(stage))}'",
                                       code=self.code)
        return iter([])

    def find(self, flt):
        from pymongo.errors import OperationFailure
        for op in flt:
            if op in self.lacks:
                raise OperationFailure(f"unknown top level operator: {op}",
                                       code=self.code)
        return self

    def limit(self, n):
        return iter([])


class _MongoEng:
    CANON_ENGINE = "mongodb"
    SOURCE = [{"name": "v", "type": "view", "options": {
                  "viewOn": "t", "pipeline": [{"$densify": {
                      "field": "at", "range": {"step": 1,
                                               "bounds": "full"}}}]}},
              {"name": "t", "type": "collection", "options": {
                  "validator": {"$jsonSchema": {"required": ["a"]}}}}]

    def __init__(self, target):
        self.target = target

    def _d(self, side, db):
        return db

    def _client(self, side):
        eng = self

        class Db:
            def list_collections(self):
                return list(eng.SOURCE)

            def __getitem__(self, name):
                return eng.target
        return {"app": Db()}


def test_a_mongo_target_is_asked_each_pipeline_and_validator():
    from pymongo.errors import OperationFailure
    got = lacks._mongo_asked(_MongoEng(_MongoColl(["$densify"])), "app")[0]
    assert got["level"] == "fail", got
    assert "1 of 2 the target does not take: view v" in got["detail"]
    got = lacks._mongo_asked(_MongoEng(_MongoColl(["$jsonSchema"])),
                             "app")[0]
    assert got["level"] == "fail" and "the validator of t" in got["detail"]
    got = lacks._mongo_asked(_MongoEng(_MongoColl()), "app")[0]
    assert got["level"] == "pass" and "2 asked of the target" in \
        got["detail"]
    # a right the target's user lacks says nothing of the pipeline
    with pytest.raises(OperationFailure):
        lacks._mongo_asked(_MongoEng(_MongoColl(["$densify"], code=13)),
                           "app")


# --- Elasticsearch's own security API ----------------------------------------------------

def test_elasticsearch_keeps_its_users_in_its_own_api(monkeypatch):
    from migkit import users
    monkeypatch.setattr(users, "_es_side", lambda hop, side: side == "dst")
    hop = _hop("opensearch", db_map={"app": "shop"})
    assert users._os_paths(hop, "dst")["role"] == "/_security/role"
    assert users._os_paths(hop, "dst")["mapping"] == \
        "/_security/role_mapping"
    assert users._os_paths(hop, "src")["role"] == \
        "/_plugins/_security/api/roles"
    rule = {"indices": [{"names": ["app.*", "other"],
                         "privileges": ["read"]}]}
    assert users._os_patterns(hop, rule)["indices"][0]["names"] == [
        "shop.*", "other"]
    said = []
    out = {"missing_on_target": []}
    users._os_compared(hop, out, {"role:r": {"cluster": ["all"]}}, {},
                       said.append)
    assert out["apis_differ"] and out["result"] == "gap"
    assert any("made on the target by hand" in s for s in said)
    monkeypatch.setattr(users, "_es_side", lambda hop, side: True)
    out = {"missing_on_target": []}
    users._os_compared(hop, out, {"role:r": {"cluster": ["all"]}},
                       {"role:r": {"cluster": ["all"]}}, said.append)
    assert not out["apis_differ"] and out["result"] == "pass"


def test_goal_upgrade_says_an_edition_up():
    from migkit import hopkeys
    assert "a higher edition" in hopkeys.GOALS["upgrade"]["what"]
    assert "what the new edition lacks" in hopkeys.GOALS["upgrade"][
        "guarantees"]
    assert "accept" in hopkeys.HOP_OPTIONS["edition"].subs
    assert "backup" in hopkeys.HOP_OPTIONS["physical"].subs
