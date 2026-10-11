"""A managed side - any provider's, either end of the pair - as migkit
reads it and holds it, without a server (`managed`, `freeze`, `dba`,
`sourcechange`, `interlock`): which service a side is and how that is
known, a setting in its provider's own words, the freeze ladder climbed
over what its admin may do (no superuser, no ALTER SYSTEM, no SUPER, a
service's own kill procedure, a MongoDB-compatible service without a
write block), `read_only` through the provider's parameter group offered
and given back, change streams a service keeps off until asked, the
restore points each side's provider takes, and the target read against
the source - its time zone, collations and connections.

Every server answer here is a stand-in; the servers themselves are in
`test_a_managed_source_is_held_on_a_server_like_its_service.py` and the
release gate's scenario.
"""
import pytest

from migkit import decide, freeze, managed
from migkit.clouds import Where
from migkit.config import Endpoint, Hop


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path / "reports"


def _hop(engine="postgres", src_host="10.0.0.1", dst_host="10.0.0.2",
         src_opts=None, dst_opts=None, **options):
    return Hop(name="mx", engine=engine,
               source=Endpoint(host=src_host, port=5432, user="migkit",
                               password="CHANGE_ME",
                               options=dict(src_opts or {})),
               target=Endpoint(host=dst_host, port=5432, user="migkit",
                               password="CHANGE_ME",
                               options=dict(dst_opts or {})),
               databases=["app"], options=dict(options))


class Pg:
    """A PostgreSQL side engine that answers each question by the first
    words it holds, on either side."""
    CANON_ENGINE = "postgres"

    def __init__(self, answers):
        self.answers = answers
        self.asked = []

    def _psql(self, side, db, sql):
        self.asked.append((side, sql))
        for needle, got in self.answers.items():
            if needle in sql:
                return got(side) if callable(got) else got
        raise AssertionError(f"{side}: {sql}")

    def _d(self, side, db):
        return db


def _svc(provider="aws", family="postgres", **admin):
    return managed.Service(id=f"{provider}-x-{family}", provider=provider,
                           where="x", label=f"a {family} of {provider}'s",
                           family=family, admin=admin,
                           api={"product": "rds"})


# --- which service a side is --------------------------------------------------

def _entry(provider, where, family):
    got = managed.find(provider, where, family)
    assert got is not None, (provider, where, family)
    return got


def test_the_servers_word_names_a_side_before_the_hop_and_the_address():
    rds = _entry("aws", "rds", "postgres")
    tencent = _entry("tencent", "tencentdb", "postgres")
    says_rds = Pg({"pg_roles where": "rds_superuser\x1ffalse"})
    got = managed.side(_hop(), says_rds, "source")
    assert got.service.id == rds.id
    assert got.how == "the server's own answer" and not got.disagree
    # the hop names another service: the server's word stands, and the
    # two are said to disagree
    hop = _hop(src_opts={"managed": {"service": tencent.id}})
    got = managed.side(hop, says_rds, "source")
    assert got.service.id == rds.id, got
    assert rds.label in got.disagree and tencent.label in got.disagree
    assert "server's word stands" in got.disagree
    # a private address and a server that says nothing: the hop's word,
    # and the DBA's words follow it
    silent = Pg({"pg_roles where": "\x1ffalse"})
    got = managed.side(_hop(src_opts={"managed": {"service": tencent.id}}),
                       silent, "source")
    assert got.service.id == tencent.id
    assert got.how == "the hop's managed option"
    assert (got.where.cloud, got.where.service) == ("tencent", "tencentdb")
    # the address alone, where nothing else says
    got = managed.side(_hop(src_host="sh-postgres-1.sql.tencentcdb.com"),
                       silent, "source")
    assert got.service.id == tencent.id and got.how == "its address"
    # nothing says anything: run by its owner
    got = managed.side(_hop(), silent, "source")
    assert not got.service.managed
    assert got.service.said() == "a PostgreSQL run by its owner"


def test_a_service_the_catalogue_does_not_have_is_refused_by_name():
    with pytest.raises(SystemExit, match="not a managed service"):
        managed.get("nobody-nothing-postgres")


def test_the_pair_is_named_as_one():
    rds = _entry("aws", "rds", "postgres")
    tencent = _entry("tencent", "tencentdb", "postgres")
    said = managed.pair_said(rds, tencent)
    assert said == (f"a managed PostgreSQL on AWS ({rds.label}) to a managed"
                    f" PostgreSQL on Tencent Cloud ({tencent.label})"), said
    assert managed.plan(rds, tencent).route == "postgres"
    my = _entry("aws", "rds", "mysql")
    assert managed.plan(my, tencent).route == "hetero"


# --- a setting in the provider's words -----------------------------------------

def test_a_setting_is_spelled_in_each_providers_words():
    _, rds = managed.spell("postgres", Where("aws", "rds"), "wal_level",
                           "logical", True, "the source")
    assert rds[0] == ("in the source's DB parameter group:"
                      " rds.logical_replication = 1 (the server restarts to"
                      " take it)"), rds
    assert "ApplyMethod=pending-reboot" in rds[1]
    _, tencent = managed.spell("postgres", Where("tencent", "tencentdb"),
                               "wal_level", "logical", False, "the target")
    assert tencent[0].startswith("TencentDB console") and \
        "wal_level = logical" in tencent[0], tencent
    # a server run by its owner is changed on the server itself
    assert managed.spell("postgres", Where(), "wal_level", "logical", True,
                         "the source") is None


def test_a_template_fills_only_its_own_placeholders():
    got = managed.fill("""--Parameters '{"{name}":"{value}"}'{restart_flag}""",
                       {"name": "binlog_format", "value": "ROW",
                        "restart_flag": ""})
    assert got == """--Parameters '{"binlog_format":"ROW"}'"""
    stmts, said = managed.statement("grant rds_replication to {who};",
                                    "app")
    assert stmts == ["grant rds_replication to app;"] and said == []
    assert managed.undo_of(stmts[0]) == "revoke rds_replication from app;"
    stmts, said = managed.statement("console: the account's replication"
                                    " right")
    assert stmts == [] and said == ["the account's replication right"]
    assert managed.undo_of("alter role app with replication;") == \
        "alter role app with noreplication;"


# --- the freeze its admin allows ---------------------------------------------------

def test_a_managed_postgresql_keeps_an_owner_out_by_its_connection_limit():
    # no superuser, no ALTER SYSTEM, databases not its own: roles it may
    # change
    way, why = managed.freeze_way(_svc(createrole=True,
                                       signal_backend=True))
    assert way == "pg_role_limit", why
    assert "not migkit's account's to take the right to connect" in why
    # a database of its own: the right to connect is taken as before
    way, _ = managed.freeze_way(_svc(createrole=True, owns_databases=True))
    assert way == "pg_database"
    # neither: no way, and why
    way, why = managed.freeze_way(_svc())
    assert way is None and "CREATEROLE" in why, why


def _pg_rungs():
    class E:
        CANON_ENGINE = "postgres"
    return freeze.PostgresHold(None, E()).rungs()


def test_a_connection_limit_that_would_reach_another_database_is_refused():
    facts = decide.Facts({"others": [], "alter system": False,
                          "owner roles": True, "connect revoke": False,
                          "alter roles": True,
                          "roles elsewhere": ["billing"]})
    got = decide.climb("t", _pg_rungs(), facts, prove=False)
    assert got.rung is None and "billing" in got.reason, got.reason
    # a role that writes only through its grants needs no keeping out
    facts = decide.Facts({"others": [], "alter system": False,
                          "owner roles": False, "connect revoke": False,
                          "alter roles": False, "roles elsewhere": []})
    assert decide.climb("t", _pg_rungs(), facts,
                        prove=False).rung.name == "pg_database"


def test_a_managed_mysql_is_read_only_only_where_the_operator_says_yes():
    svc = _svc(family="mysql", kill="call mysql.rds_kill({id})",
               read_only_by_api=True, grant_option=True)
    way, _ = managed.freeze_way(svc)
    assert way == "my_database"
    offer = managed.freeze_offer(svc)
    assert "param_read_only" in offer and "parameter group" in offer, offer
    rungs = freeze.MySQLHold(None, type("E", (), {
        "CANON_ENGINE": "mysql"})()).rungs()
    facts = decide.Facts(managed.stand_in(svc))
    facts[decide.answer_key("param_read_only")] = "yes"
    got = decide.climb("t", rungs, facts, prove=False)
    assert got.rung.name == "my_param_read_only", got.reason
    facts = decide.Facts(managed.stand_in(svc))
    facts[decide.answer_key("param_read_only")] = "no"
    got = decide.climb("t", rungs, facts, prove=False)
    assert got.rung.name == "my_database"
    assert any("the answer is no" in why for _, why in got.passed), \
        got.passed
    # an admin that can end no other account's session: nothing holds
    way, why = managed.freeze_way(_svc(family="mysql"))
    assert way is None and "CONNECTION_ADMIN" in why, why


def test_a_mongodb_compatible_service_without_a_block_is_held_per_database():
    svc = _svc(family="mongodb", superuser=False, user_roles=True,
               version="5.0",
               commands_missing=["setUserWriteBlockMode", "fsync",
                                 "fsyncUnlock", "killSessions"])
    way, why = managed.freeze_way(svc)
    assert way == "mongo_database", why
    # one that does not let its admin change a user's roles: none
    svc.admin["commands_missing"].append("grantRolesToUser")
    way, why = managed.freeze_way(svc)
    assert way is None and "grantRolesToUser" in why, why


def test_the_ladders_read_as_before_where_nothing_new_is_asked(monkeypatch):
    """A server of before - its facts without the managed ones - is held
    the way it was."""
    for cls, given, want in (
            (freeze.PostgresHold, {"alter system": False}, "pg_database"),
            (freeze.MySQLHold, {"read only switch": False}, "my_database"),
            (freeze.MongoHold, {"version": (7, 0), "actions": []},
             "mongo_database")):
        monkeypatch.setattr(cls, "others", lambda self, dbs: [])
        monkeypatch.setattr(cls, "readers", lambda self, dbs, g=given: {
            k: (lambda v=v: v) for k, v in g.items()})
        kind = {"PostgresHold": "postgres", "MySQLHold": "mysql",
                "MongoHold": "mongodb"}[cls.__name__]
        hop = _hop(kind, cutover={"budget": 30})
        eng = type("E", (), {"CANON_ENGINE": kind})()
        assert freeze.choose(hop, eng, ["app"]).rung.name == want


def test_the_hops_answer_takes_the_parameter_group_way(monkeypatch):
    monkeypatch.setattr(freeze.MySQLHold, "others", lambda self, dbs: [])
    monkeypatch.setattr(freeze.MySQLHold, "readers", lambda self, dbs: {
        "read only switch": lambda: False, "kill": lambda: "mysql.rds_kill",
        "where": lambda: Where("aws", "rds"),
        "param read_only": lambda: {"state": "possible", "before": "0"}})
    eng = type("E", (), {"CANON_ENGINE": "mysql"})()
    hop = _hop("mysql", cutover={"budget": 30})
    assert freeze.choose(hop, eng, ["app"]).rung.name == "my_database"
    got, _ = freeze.offer(hop, eng, ["app"])
    assert [c.name for c in got.changes] == ["param_read_only"]
    assert got.rung.name == "my_param_read_only"
    assert got.instead.rung.name == "my_database"
    hop = _hop("mysql", cutover={"budget": 30},
               source_changes={"param_read_only": "yes"})
    assert freeze.choose(hop, eng, ["app"]).rung.name == \
        "my_param_read_only"
    # a group other instances share is a DBA's to split first
    monkeypatch.setattr(freeze.MySQLHold, "readers", lambda self, dbs: {
        "read only switch": lambda: False, "kill": lambda: "kill",
        "where": lambda: Where("aws", "rds"),
        "param read_only": lambda: {"state": "dba", "why": "shared"}})
    got = freeze.choose(hop, eng, ["app"])
    assert got.rung.name == "my_database"
    assert any("only a DBA can make here" in w for _, w in got.passed)


class My:
    """A MySQL side engine: `read_only` as the provider last set it, the
    statements it was asked, and a kill procedure of its service's."""
    CANON_ENGINE = "mysql"

    def __init__(self, grants=("GRANT SELECT, INSERT ON *.* TO `admin`@`%`"
                               " WITH GRANT OPTION",), procedure=True):
        self.read_only = "0"
        self.grants = grants
        self.procedure = procedure
        self.ran = []

    def _q(self, side, sql, args=None):
        self.ran.append(sql)
        if sql.startswith("show grants"):
            return [(g,) for g in self.grants]
        if "information_schema.routines" in sql:
            return [(1 if self.procedure else 0,)]
        if "@@global.read_only" in sql:
            return [(self.read_only,)]
        if "information_schema.processlist where id in" in sql:
            return [(0,)]
        return []


class Provider:
    """The provider's API for the source: `read_only` in its group."""

    def __init__(self, server):
        self.server, self.calls = server, []

    def set_parameters(self, values, log):
        self.calls.append(dict(values))
        before = {"read_only": self.server.read_only}
        self.server.read_only = values["read_only"]
        return before


def test_read_only_goes_on_through_the_provider_and_comes_off_again(
        monkeypatch):
    from migkit import sourcechange
    server = My()
    api = Provider(server)
    monkeypatch.setattr(managed, "adapter", lambda hop, eng, which: api)
    monkeypatch.setattr(managed, "side", lambda hop, eng, which:
                        managed.Side(which, _entry("aws", "rds", "mysql"),
                                     Where("aws", "rds")))
    monkeypatch.setattr(freeze, "_my_param", lambda hop, eng, side="src": {
        "state": "possible", "before": "0"})
    hop = _hop("mysql", cutover={"budget": 30})
    said = []
    freeze._my_param_freeze(hop, server, said.append, wait=1)
    assert api.calls == [{"read_only": "1"}]
    rec = freeze.server_state(hop)
    assert rec["way"] == "my_param_read_only" and rec["on"]
    assert rec["before"] == {"read_only": "0"}
    # made as a change on the source: recorded, and named by doctor
    left = sourcechange.left_on_source(hop)
    assert [e["change"] for e in left] == ["param_read_only"]
    assert "modify-db-parameter-group" in left[0]["make"][0]
    # the proof reads what the provider set, not super_read_only
    held = freeze._my_proof(hop, server, "app", "server")
    assert held[0] == (True, "read_only is on"), held
    freeze.hold_for(hop, server).release_server(rec, said.append)
    assert api.calls[-1] == {"read_only": "0"} and server.read_only == "0"
    assert sourcechange.left_on_source(hop) == []
    held = freeze._my_proof(hop, server, "app", "server")
    assert held[0] == (False, "read_only is off"), held


def test_a_managed_mysql_ends_sessions_through_its_own_procedure():
    server = My()
    assert freeze._my_kill_way(server, "src") == "mysql.rds_kill"
    freeze._my_kill(server, "src", [(42, "app")], wait=0.1)
    assert "call mysql.rds_kill(42)" in server.ran
    assert "kill 42" not in server.ran
    # CONNECTION_ADMIN: its own KILL
    admin = My(grants=("GRANT CONNECTION_ADMIN ON *.* TO `admin`@`%`",))
    freeze._my_kill(admin, "src", [(7, "app")], wait=0.1)
    assert "kill 7" in admin.ran
    # neither: said by the preflight, and no way is taken
    none = My(procedure=False)
    assert freeze._my_kill_way(none, "src") is None
    hop = _hop("mysql", cutover={"budget": 30})
    said = freeze.hold_for(hop, none).cannot("app", "my_database")
    assert any("cannot end other accounts' sessions" in s for s in said)


# --- change streams a service keeps off ------------------------------------------

class Admin:
    def __init__(self, listed):
        self.listed = listed

    def command(self, name, **kw):
        if name == "connectionStatus":
            return {"authInfo": {
                "authenticatedUsers": [{"user": "admin", "db": "admin"}],
                "authenticatedUserPrivileges": [
                    {"resource": {"db": "", "collection": ""},
                     "actions": ["find", "changeStream"]}]}}
        if name == "hello":
            return {"setName": "rs0"}
        raise AssertionError(name)

    def aggregate(self, pipeline):
        from pymongo.errors import OperationFailure
        assert pipeline == [{"$listChangeStreams": 1}], pipeline
        if self.listed is None:
            raise OperationFailure("Unrecognized pipeline stage name")
        return iter(self.listed)


class Client:
    def __init__(self, listed):
        self.admin = Admin(listed)


class Mongo:
    CANON_ENGINE = "mongodb"

    def __init__(self, listed):
        self.client = Client(listed)

    def _client(self, key):
        return self.client


def test_change_streams_a_service_keeps_off_are_read_from_the_server():
    from migkit import dba
    hop = _hop("mongodb")
    eng = Mongo([{"database": "app", "collection": ""}])
    got = dba._mongo(hop, eng, "src", "source", ["app", "other"], Where(),
                     False, False, True, False)
    by = {n.what: n for n in got}
    assert by["follow app's changes"].ok is True
    off = by["follow other's changes"]
    assert off.ok is False
    assert off.now == "change streams are off for it on the server"
    assert any("modifyChangeStreams" in s and '"other"' in s
               for s in off.fix), off.fix
    # one turned on for every database
    eng = Mongo([{"database": "", "collection": ""}])
    got = dba._mongo(hop, eng, "src", "source", ["other"], Where(), False,
                     False, True, False)
    assert [n.ok for n in got if n.what.startswith("follow")] == [True]
    # a replica set's are always on: nothing to turn on, whatever the
    # address says
    eng = Mongo(None)
    got = dba._mongo(hop, eng, "src", "source", ["other"],
                     Where("aws", "documentdb"), False, False, True, False)
    stream = [n for n in got if n.what.startswith("follow")][0]
    assert stream.ok is True
    assert not any("modifyChangeStreams" in s for s in stream.fix)


# --- a managed MySQL's replica and its log's keeping ------------------------------

class Routines:
    """A MySQL server's procedures, as `MySQLEngine`'s readers ask."""

    def __init__(self, names, config=None, host="10.0.0.2"):
        from migkit.engines.mysql import MySQLEngine
        self.names, self.config = names, config
        self.hop = _hop("mysql", dst_host=host)
        self.REPLICA_PROCEDURES = MySQLEngine.REPLICA_PROCEDURES
        self.OWN_PURGE = MySQLEngine.OWN_PURGE
        self.managed_retention = lambda side: MySQLEngine.managed_retention(
            self, side)

    def _q(self, side, sql, args=None):
        if "routine_name in" in sql:
            return [(n,) for n in self.names]
        if "rds_show_configuration'" in sql:
            return [(1 if "rds_show_configuration" in self.names else 0,)]
        if sql.startswith("call mysql.rds_show_configuration"):
            return [("binlog retention hours", self.config, "")]
        raise AssertionError(sql)


def test_a_managed_mysql_takes_its_replicas_source_through_its_procedure():
    from migkit.engines.mysql import MySQLEngine
    got = MySQLEngine._replica_procedure(
        Routines(["rds_set_external_source"]), "dst")
    assert got == "mysql.rds_set_external_source"
    got = MySQLEngine._replica_procedure(
        Routines(["rds_set_external_master"]), "dst")
    assert got == "mysql.rds_set_external_master"
    # the address says RDS and the server does not: the server's word
    assert MySQLEngine._replica_procedure(
        Routines([], host="db.example.eu-west-1.rds.amazonaws.com"),
        "dst") is None


def test_a_managed_mysql_keeps_its_log_by_its_own_setting():
    from migkit.engines.mysql import MySQLEngine
    eng = Routines(["rds_show_configuration"], config="24")
    assert MySQLEngine._own_retention(eng, "src") == 24 * 3600
    eng.config = None
    assert MySQLEngine._own_retention(eng, "src") == MySQLEngine.OWN_PURGE
    assert MySQLEngine._own_retention(Routines([]), "src") is None


# --- production named by a managed instance ---------------------------------------

def test_production_is_named_by_a_managed_instance_and_its_address():
    from migkit import interlock
    hop = _hop(dst_host="SH-CDB-ABC.sql.tencentcdb.com.",
               dst_opts={"managed": {"id": "cdb-prod1"}},
               production=["cdb-prod*"])
    hit = interlock.production(hop, {"name": "10.0.0.94"})
    assert hit[:2] == ("cdb-prod1", "cdb-prod*") and hit[2] is False
    hop = _hop(dst_host="SH-CDB-ABC.sql.tencentcdb.com.",
               production=["*.tencentcdb.com"],
               production_target="sh-cdb-abc.sql.tencentcdb.com")
    hit = interlock.production(hop, {})
    assert hit[1] == "*.tencentcdb.com" and hit[2] is True
    hop = _hop(dst_host="10.0.0.2", production=["cdb-prod*"])
    assert interlock.production(hop, {"name": "10.0.0.2"}) is None


# --- restore points ------------------------------------------------------------------

class Point:
    def __init__(self, id, kind="snapshot"):
        self.id, self.kind, self.at, self.ready = id, kind, "", True


class Api:
    def __init__(self, conf):
        self.conf, self.made = conf, []

    def credentials(self):
        return object(), "the environment"

    def find(self, host, port=None):
        return None

    def restore_point(self, name, log):
        self.made.append(name)
        return Point(f"{self.conf['id']}-{len(self.made)}")


def test_a_restore_point_of_each_side_is_taken_through_its_provider(
        monkeypatch, reports):
    import json

    from migkit import cloudapi
    made = []

    def adapter(api, conf):
        got = Api(conf)
        made.append((api, got))
        return got
    monkeypatch.setattr(cloudapi, "adapter", adapter)
    monkeypatch.setattr(cloudapi, "offered", lambda a, p, w: True)
    rds = _entry("aws", "rds", "postgres")
    tencent = _entry("tencent", "tencentdb", "postgres")
    hop = _hop(src_opts={"managed": {"service": rds.id, "id": "app-db",
                                     "region": "eu-west-1"}},
               dst_opts={"managed": {"service": tencent.id,
                                     "id": "postgres-1",
                                     "region": "ap-bangkok"}})
    silent = Pg({"pg_roles where": "\x1ffalse"})
    assert managed.restore_ready(hop, silent, "source") == ""
    said = []
    got = managed.restore_points(hop, silent, ["source", "target"],
                                 said.append)
    assert got["source"]["id"] == "app-db-1"
    assert got["target"]["id"] == "postgres-1-1"
    assert [(api, a.conf["region"]) for api, a in made[-2:]] == [
        ("aws", "eu-west-1"), ("tencent", "ap-bangkok")]
    names = [a.made[0] for _, a in made[-2:]]
    assert names[0].startswith("migkit-mx-source-") and \
        names[1].startswith("migkit-mx-target-"), names
    log = (reports / "mx" / "changelog.jsonl").read_text().splitlines()
    ops = [json.loads(line) for line in log]
    assert [o["side"] for o in ops if o["op"] == "restore-point"] == [
        "source", "target"]
    # a side its address does not find and the hop does not name
    bare = _hop(src_opts={"managed": {"service": rds.id}})
    why = managed.restore_ready(bare, silent, "source")
    assert "name it under the side's managed option" in why, why
    # a server run by its owner has no provider's API
    why = managed.restore_ready(_hop(), silent, "source")
    assert "run by its owner" in why, why


def test_the_cutover_names_the_sides_it_takes_restore_points_of():
    from migkit import cutover
    hop = _hop(cutover={"restore_points": ["source", "target"]})
    assert cutover.settings(hop)["restore_points"] == ["source", "target"]
    assert cutover.settings(_hop(cutover=True))["restore_points"] == []
    with pytest.raises(SystemExit, match="restore_points"):
        cutover.settings(_hop(cutover={"restore_points": ["both"]}))
    steps = " ".join(cutover.plan(hop, ["app"], cutover.settings(hop)))
    assert "a restore point of the source and the target taken through"\
        " its provider's API" in steps, steps


# --- the target read against the source --------------------------------------------

def _pair(src, dst):
    def answer(needles):
        return lambda side: needles[side]
    return Pg({k: answer({"src": a, "dst": b})
               for k, (a, b) in {
                   "show timezone": (src["tz"], dst["tz"]),
                   "datname = current_database()": (src["enc"], dst["enc"]),
                   "datname = 'template1'": (src["made"], dst["made"]),
                   "from pg_collation": (src["locales"], dst["locales"]),
                   "show max_connections": (src["max"], dst["max"]),
                   "backend_type = 'client backend'": (src["now"],
                                                       dst["now"]),
                   "pg_roles where": ("\x1ffalse", "\x1ffalse")}.items()})


SRC = {"tz": "UTC", "enc": "UTF8\x1fen_US.UTF-8\x1fen_US.UTF-8",
       "made": "UTF8\x1fen_US.UTF-8",
       "locales": "C\x1fen_US.utf8", "max": "500", "now": "180"}


def _owned():
    return (managed.Side("source", managed.owner("postgres")),
            managed.Side("target", managed.owner("postgres")))


def test_the_target_is_read_against_the_source():
    items = managed._server_items(_hop(), _pair(SRC, dict(SRC)), ["app"],
                                  *_owned(), False)
    assert [(i.level, i.what) for i in items] == [("pass", "connections")]
    dst = {**SRC, "tz": "Asia/Bangkok", "locales": "C\x1fC.utf8",
           "made": "SQL_ASCII\x1fC", "max": "150", "now": "3"}
    items = managed._server_items(_hop(), _pair(SRC, dst), ["app"],
                                  *_owned(), False)
    by = {i.what: i for i in items}
    lack = by["app: collations"]
    assert lack.level == "fail" and lack.gates, lack
    assert "en_US.UTF-8" in lack.detail
    assert by["time zone"].level == "warn"
    assert "UTC" in by["time zone"].detail and \
        "Asia/Bangkok" in by["time zone"].detail
    assert "set timezone = UTC on the target" in by["time zone"].fix
    conn = by["connections"]
    assert conn.level == "fail" and "150" in conn.detail and \
        "180" in conn.detail, conn
    # the gate before a copy asks only what would stop it
    quick = managed._server_items(_hop(), _pair(SRC, dst), ["app"],
                                  *_owned(), True)
    assert [i.what for i in quick] == ["app: collations"]


def test_a_managed_target_is_told_how_its_setting_changes():
    rds = _entry("aws", "rds", "postgres")
    side = managed.Side("target", rds, Where("aws", "rds"))
    fix = managed._fix(side, "postgres", "max_connections", 400)
    assert "DB parameter group: max_connections = 400" in fix, fix
