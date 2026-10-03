"""One hold per engine (`freeze.Hold`), its ways a ladder (`decide`), without
a server: which way each source allows, what each records before it
acts, and that what it gives back is exactly what it took - and that the
read back can say "not held".

The servers themselves are in the docker tests of each engine
(`test_a_*_source_is_held_and_given_back.py`); here every server answer
is a stand-in, so the choice and the records are held to what they are
given.
"""
import json

import pytest

from migkit import decide, freeze
from migkit.config import Endpoint, Hop


def _hop(tmp_path, engine, **options):
    hop = Hop(name="hx", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=1, user="migkit",
                              password="CHANGE_ME"),
              databases=["app"], options={"cutover": {"budget": 30},
                                          **options})
    root = tmp_path / "reports"

    def report_dir(db=""):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


class _Eng:
    """A side engine of `kind` that answers nothing: every fact a hold
    reads from it comes from the test."""

    def __init__(self, kind):
        self.CANON_ENGINE = kind


def _facts(monkeypatch, cls, **given):
    """The hold's facts replaced by `given`."""
    monkeypatch.setattr(cls, "others", lambda self, dbs: given.get(
        "others", []))
    monkeypatch.setattr(cls, "readers", lambda self, dbs: {
        k: (lambda v=v: v) for k, v in given.items() if k != "others"})


# --- the ladder --------------------------------------------------------------

def test_every_way_is_a_rung_with_one_scope_and_its_class_is_declared():
    for kind, cls in freeze.HOLDS.items():
        rungs = cls(None, _Eng(kind)).rungs()
        assert rungs, kind
        for r in rungs:
            assert r.name in freeze.SCOPE, r.name
            scope = {freeze.SERVER, freeze.DATABASE} & r.gives
            assert len(scope) == 1, (kind, r.name, r.gives)
            want = freeze.SERVER if freeze.SCOPE[r.name] == "server" \
                else freeze.DATABASE
            assert want in r.gives, (kind, r.name)
        # declared where `decide.coverage` sees it, and resolving to it
        assert decide.cell(kind, "writes held") == (
            "yes", f"freeze.{cls.__name__}"), kind
    assert freeze.FREEZES == tuple(freeze.HOLDS)
    assert not [c for c in decide.uncovered() if c[1] == "writes held"]


def test_an_engine_migkit_does_not_hold_says_why():
    assert decide.cell("dynamodb", "writes held")[0] == decide.NOT_APPLICABLE
    assert "access policies" in freeze.not_held("dynamodb")
    # a target kept by grants, whose source migkit cannot freeze: no
    # session ends another's there
    assert decide.cell("dsql", "writes held")[0] == decide.NOT_APPLICABLE
    assert "no session ends another's" in freeze.not_held("dsql")
    hop = Hop(name="x", engine="dsql", source=Endpoint(), target=Endpoint())
    assert freeze.hold_for(hop, _Eng("dsql")).rungs() == ()
    assert [r.name for r in freeze.hold_for(hop, _Eng("dsql"),
                                            "dst").keep_rungs()] == [
        "pgw_grants"]
    # an engine nobody declared falls to the gap's item, never to a way
    assert decide.WAYS["writes held"]["*"] == (decide.NOT_YET, "F2")


def test_the_whole_server_goes_first_where_it_is_all_the_hops(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "postgres")
    _facts(monkeypatch, freeze.PostgresHold, **{"alter system": True})
    got = freeze.choose(hop, _Eng("postgres"), ["app"])
    assert got.rung.name == "pg_server", got
    assert freeze.scope(hop, _Eng("postgres"), ["app"])[0] == "server"
    # a database on the server the hop does not move: per database, and
    # the reason names it
    _facts(monkeypatch, freeze.PostgresHold, others=["other"],
           **{"alter system": True})
    how, why = freeze.scope(hop, _Eng("postgres"), ["app"])
    assert how == "database" and "other" in why, why
    # freeze: server where it cannot be: no way, and why
    got = freeze.choose(hop, _Eng("postgres"), ["app"], "server")
    assert got.rung is None and "other" in got.reason, got


def test_a_drain_by_a_mark_takes_only_a_way_migkit_still_writes_through(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "mysql")
    _facts(monkeypatch, freeze.MySQLHold, **{"read only switch": True})
    assert freeze.choose(hop, _Eng("mysql"), ["app"]).rung.name == \
        "my_server"
    # super_read_only stops migkit's own account too: its mark could not
    # be written
    got = freeze.choose(hop, _Eng("mysql"), ["app"], need=(freeze.OURS,))
    assert got.rung.name == "my_database", got
    hop = _hop(tmp_path, "postgres")
    _facts(monkeypatch, freeze.PostgresHold, **{"alter system": True})
    got = freeze.choose(hop, _Eng("postgres"), ["app"], need=(freeze.OURS,))
    assert got.rung.name == "pg_server", got


def test_redis_holds_the_server_or_refuses_the_accounts(tmp_path,
                                                        monkeypatch):
    hop = _hop(tmp_path, "redis")
    _facts(monkeypatch, freeze.RedisHold, version=(7, 2), acl=True,
           **{"one server": True},
           **{"stream back": False})
    assert freeze.choose(hop, _Eng("redis"), ["0"]).rung.name == \
        "redis_pause"
    assert freeze.choose(hop, _Eng("redis"), ["0"], "database").rung.name \
        == "redis_accounts"
    # a stream back applies on the old source as writes: not held there
    _facts(monkeypatch, freeze.RedisHold, version=(7, 2), acl=True,
           **{"one server": True},
           **{"stream back": True})
    got = freeze.choose(hop, _Eng("redis"), ["0"])
    assert got.rung.name == "redis_accounts" and "stream back" in \
        got.reason, got
    # 6.0: neither - holding writes alone needs 6.2, reading an account's
    # refusal back 7.0
    _facts(monkeypatch, freeze.RedisHold, version=(6, 0), acl=True,
           **{"one server": True},
           **{"stream back": False})
    got = freeze.choose(hop, _Eng("redis"), ["0"])
    assert got.rung is None and "6.2" in got.reason and "7.0" in \
        got.reason, got
    # neither lets migkit write a mark: a drain by one finds no way
    _facts(monkeypatch, freeze.RedisHold, version=(7, 2), acl=True,
           **{"one server": True},
           **{"stream back": False})
    assert freeze.choose(hop, _Eng("redis"), ["0"],
                         need=(freeze.OURS,)).rung is None


def test_sql_server_is_read_only_unless_its_group_forbids(tmp_path,
                                                          monkeypatch):
    hop = _hop(tmp_path, "mssql")
    _facts(monkeypatch, freeze.MSSQLHold, alter=True,
           control=True, kill=True, **{"alone in its group": True})
    assert freeze.choose(hop, _Eng("mssql"), ["app"]).rung.name == \
        "mssql_read_only"
    _facts(monkeypatch, freeze.MSSQLHold, alter=True,
           control=True, kill=True, **{"alone in its group": False})
    got = freeze.choose(hop, _Eng("mssql"), ["app"])
    assert got.rung.name == "mssql_deny" and "availability group" in \
        got.reason, got
    _facts(monkeypatch, freeze.MSSQLHold, alter=True,
           control=False, kill=True, **{"alone in its group": False})
    got = freeze.choose(hop, _Eng("mssql"), ["app"])
    assert got.rung is None and "CONTROL" in got.reason, got
    # server: SQL Server has no server-wide way
    assert freeze.choose(hop, _Eng("mssql"), ["app"], "server").rung is None


def test_cassandra_waits_its_cache_out_within_the_budget(tmp_path,
                                                         monkeypatch):
    hop = _hop(tmp_path, "cassandra")
    _facts(monkeypatch, freeze.CassandraHold, authorizer=True,
           authorize=True, cache=2.0, budget=30.0)
    assert freeze.choose(hop, _Eng("cassandra"), ["app"]).rung.name == \
        "cql_revoke"
    _facts(monkeypatch, freeze.CassandraHold, authorizer=True,
           authorize=True, cache=40.0, budget=30.0)
    got = freeze.choose(hop, _Eng("cassandra"), ["app"])
    assert got.rung is None and "40s" in got.reason, got
    _facts(monkeypatch, freeze.CassandraHold, authorizer=False,
           authorize=True, cache=2.0, budget=30.0)
    got = freeze.choose(hop, _Eng("cassandra"), ["app"])
    assert got.rung is None and "authorizer" in got.reason, got
    assert freeze._cql_seconds("2000ms") == 2.0
    assert freeze._cql_seconds("3s") == 3.0
    assert freeze._cql_seconds('"10000"') == 10.0


def test_kafka_and_opensearch_hold_the_cluster_only_when_it_is_all_theirs(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "kafka")
    _facts(monkeypatch, freeze.KafkaHold, authorizer=True)
    assert freeze.choose(hop, _Eng("kafka"), ["cluster"]).rung.name == \
        "kafka_cluster"
    _facts(monkeypatch, freeze.KafkaHold, others=["audit"], authorizer=True)
    assert freeze.choose(hop, _Eng("kafka"), ["cluster"]).rung.name == \
        "kafka_topics"
    _facts(monkeypatch, freeze.KafkaHold, authorizer=False)
    got = freeze.choose(hop, _Eng("kafka"), ["cluster"])
    assert got.rung is None and "no authorizer" in got.reason, got
    hop = _hop(tmp_path, "opensearch")
    _facts(monkeypatch, freeze.OpenSearchHold)
    assert freeze.choose(hop, _Eng("opensearch"), ["shop"]).rung.name == \
        "os_cluster"
    _facts(monkeypatch, freeze.OpenSearchHold, others=["logs-1"])
    assert freeze.choose(hop, _Eng("opensearch"), ["shop"]).rung.name == \
        "os_indices"


# --- what each records, and gives back exactly -------------------------------

class _Redis:
    """A Redis client that answers `ACL LIST` as given and records every
    command sent."""

    def __init__(self, rules, dry=None):
        self.rules, self.sent, self.dry = list(rules), [], dict(dry or {})

    def execute_command(self, *args):
        self.sent.append(tuple(str(a) for a in args))
        if args[:2] == ("ACL", "LIST"):
            return list(self.rules)
        if args[:2] == ("ACL", "DRYRUN"):
            return self.dry.get(args[2], "This user has no permissions")
        return "OK"

    def client_id(self):
        return 1

    def client_list(self):
        return [{"id": "1", "flags": "N", "user": "default", "age": "0"},
                {"id": "7", "flags": "N", "user": "app", "age": "900"},
                {"id": "9", "flags": "S", "user": "migkit_repl", "age": "900"}]

    def config_get(self, name):
        return {"aclfile": ""}

    def info(self, section):
        return {}

    def command(self):
        return {"set": {"flags": ["write"]}, "get": {"flags": ["readonly"]}}


class _RedisEng:
    REPL_USER = "migkit_repl"
    CANON_ENGINE = "redis"

    def __init__(self, client):
        self.client = client

    def _client(self, side, db=0):
        return self.client


RULES = ["user default on nopass sanitize-payload ~* &* +@all",
         "user app on #9a0b ~app:* &* +@all -flushall (~other:* +get)",
         "user migkit_repl on #1c2d +psync +replconf +ping"]


def test_redis_accounts_are_refused_and_given_back_their_rules_whole(
        tmp_path):
    hop = _hop(tmp_path, "redis")
    c = _Redis(RULES)
    h = freeze.RedisHold(hop, _RedisEng(c))
    h.hold(["0"], "redis_accounts", lambda m: None)
    rec = freeze.server_state(hop)
    # recorded before it acted: every account's rules as listed, the
    # selector in its parentheses one word
    assert rec["rules"]["app"] == ["on", "#9a0b", "~app:*", "&*", "+@all",
                                   "-flushall", "(~other:* +get)"]
    assert "migkit_repl" not in rec["rules"]
    assert ("ACL", "SETUSER", "app", "-@write") in c.sent
    assert ("ACL", "SETUSER", "default", "-@write") in c.sent
    assert not [x for x in c.sent if x[:3] == ("ACL", "SETUSER",
                                               "migkit_repl")]
    # the connection of the application's account from before the hold
    # is ended; the replica's is not
    assert ("CLIENT", "KILL", "ID", "7", "SKIPME", "yes") in c.sent
    assert not [x for x in c.sent if x[:4] == ("CLIENT", "KILL", "ID", "9")]
    c.sent.clear()
    h.release_server(rec, lambda m: None)
    assert ("ACL", "SETUSER", "app", "reset", "on", "#9a0b", "~app:*", "&*",
            "+@all", "-flushall", "(~other:* +get)") in c.sent
    assert ("ACL", "SETUSER", "default", "reset", "on", "nopass",
            "sanitize-payload", "~*", "&*", "+@all") in c.sent


def test_redis_read_back_says_when_an_account_still_writes(tmp_path,
                                                          monkeypatch):
    hop = _hop(tmp_path, "redis")
    monkeypatch.setattr(freeze, "_held_still",
                        lambda *a, **k: (True, "no write"))
    c = _Redis(RULES)
    h = freeze.RedisHold(hop, _RedisEng(c))
    h.hold(["0"], "redis_accounts", lambda m: None)
    held = h.held("0", "redis_accounts")
    assert [ok for ok, _ in held][0] is True, held
    c.dry = {"app": "OK"}
    held = h.held("0", "redis_accounts")
    assert held[0] == (False, "still able to write: app"), held


def test_the_pause_is_lifted_only_where_it_was_put_on(tmp_path):
    hop = _hop(tmp_path, "redis")
    c = _Redis(RULES)
    h = freeze.RedisHold(hop, _RedisEng(c))
    h.hold(["0"], "redis_pause", lambda m: None)
    assert ("CLIENT", "PAUSE", str(freeze.PAUSED_FOR_MS), "WRITE") in c.sent
    rec = freeze.server_state(hop)
    assert rec["on"] and rec["way"] == "redis_pause"
    c.sent.clear()
    h.release_server({**rec, "on": False}, lambda m: None)
    assert ("CLIENT", "UNPAUSE") not in c.sent
    h.release_server(rec, lambda m: None)
    assert ("CLIENT", "UNPAUSE") in c.sent


def test_sql_server_gives_each_right_back_as_it_was(tmp_path, monkeypatch):
    hop = _hop(tmp_path, "mssql")

    class Eng:
        CANON_ENGINE = "mssql"

        def _q(self, name):
            return "[" + name + "]"

        def _d(self, side, db):
            return db
    ran = []
    h = freeze.MSSQLHold(hop, Eng())
    monkeypatch.setattr(freeze.MSSQLHold, "run",
                        lambda self, db, *sql: ran.extend(sql))
    record = {"db": "app", "way": "mssql_deny", "roles": {
        "app": {"prior": {"INSERT": "G", "UPDATE": "W"}},
        "audit": {"prior": {"INSERT": "D", "UPDATE": "D", "DELETE": "D"}}}}
    h.release("app", record, lambda m: None)
    assert ran == ["grant insert to [app]",
                   "grant update to [app] with grant option",
                   "revoke delete to [app]"], ran
    ran.clear()
    h.release("app", {"db": "app", "way": "mssql_read_only", "was": 1},
              lambda m: None)
    assert ran == [], "a database read-only before is left so"
    h.release("app", {"db": "app", "way": "mssql_read_only", "was": 0},
              lambda m: None)
    assert ran == ["alter database [app] set read_write with rollback"
                   " immediate"]


class _Admin:
    """A Kafka admin client holding denies, and refusing some."""

    def __init__(self, have=(), refuse=False):
        self.denies = set(have)
        self.refuse = refuse
        self.created, self.deleted = [], []

    def describe_acls(self, f):
        key = (f.resource_pattern.resource_type.name,
               f.resource_pattern.resource_name)
        return ([key] if key in self.denies else []), None

    def create_acls(self, acls):
        if self.refuse:
            return {"succeeded": [], "failed": [(acls[0], "denied")]}
        for a in acls:
            key = (a.resource_pattern.resource_type.name,
                   a.resource_pattern.resource_name)
            self.created.append(key)
            self.denies.add(key)
        return {"succeeded": acls, "failed": []}

    def delete_acls(self, filters):
        for f in filters:
            key = (f.resource_pattern.resource_type.name,
                   f.resource_pattern.resource_name)
            self.deleted.append(key)
            self.denies.discard(key)

    def close(self):
        pass


def test_kafka_deletes_exactly_the_denies_it_made(tmp_path, monkeypatch):
    pytest.importorskip("kafka")
    hop = _hop(tmp_path, "kafka")
    admin = _Admin(have={("TOPIC", "orders")})

    class Eng:
        CANON_ENGINE = "kafka"

        def _admin(self, side):
            return admin

        def list_move_tables(self, db):
            return [("", "orders"), ("", "payments")]
    h = freeze.KafkaHold(hop, Eng())
    h.hold(["cluster"], "kafka_cluster", lambda m: None)
    # a deny the operator had made already is neither made nor taken away
    assert admin.created == [("TOPIC", "payments"),
                             ("TRANSACTIONAL_ID", "*")], admin.created
    rec = freeze.server_state(hop)
    assert rec["made"] == [["TOPIC", "payments"], ["TRANSACTIONAL_ID", "*"]]
    h.release_server(rec, lambda m: None)
    assert sorted(admin.deleted) == [("TOPIC", "payments"),
                                     ("TRANSACTIONAL_ID", "*")]
    assert admin.denies == {("TOPIC", "orders")}
    # refused: raised, and what was recorded is what the release deletes
    freeze._server_path(hop).unlink()
    refused = _Admin(refuse=True)
    Eng._admin = lambda self, side: refused
    with pytest.raises(RuntimeError, match="refused a deny"):
        freeze.KafkaHold(hop, Eng()).hold(["cluster"], "kafka_topics",
                                          lambda m: None)
    assert freeze.server_state(hop)["made"] == [["TOPIC", "orders"],
                                                ["TOPIC", "payments"]]


def test_opensearch_blocks_record_what_each_index_had(tmp_path):
    hop = _hop(tmp_path, "opensearch")
    calls, settings = [], {"shop.orders": None, "shop.carts": "false"}

    class Eng:
        CANON_ENGINE = "opensearch"

        def neutral_tables(self, side, db):
            return ["orders", "carts"]

        def _index(self, side, db, t):
            return f"shop.{t}"

        def _call(self, side, method, path, body=None):
            calls.append((method, path, body))
            if path.endswith("_block/write"):
                ack = path != "/shop.carts/_block/write" or not refuse
                return {"acknowledged": ack, "shards_acknowledged": ack}
            if "index.blocks.write" in path:
                index = path.split("/")[1]
                v = settings[index]
                return {index: {"settings": {} if v is None else
                                {"index.blocks.write": v}}}
            if path.startswith("/_cluster/settings") and method == "GET":
                return {"persistent": {}, "transient": {}}
            return {}
    refuse = False
    h = freeze.OpenSearchHold(hop, Eng())
    h.hold(["shop"], "os_cluster", lambda m: None)
    rec = freeze.state(hop, "shop", "src")
    assert rec["indices"] == {"shop.orders": None, "shop.carts": "false"}
    assert ("PUT", "/_cluster/settings",
            {"persistent": {"cluster.blocks.read_only": True}}) in calls
    calls.clear()
    # the cluster's switch first: index settings do not change under it
    freeze.thaw_source(hop, Eng(), ["shop"], lambda m: None)
    puts = [c for c in calls if c[0] == "PUT"]
    assert puts[0] == ("PUT", "/_cluster/settings",
                       {"persistent": {"cluster.blocks.read_only": None}})
    assert ("PUT", "/shop.orders/_settings",
            {"index.blocks.write": None}) in puts
    assert ("PUT", "/shop.carts/_settings",
            {"index.blocks.write": "false"}) in puts
    assert freeze.state(hop, "shop", "src") is None
    assert freeze.server_state(hop) is None
    refuse = True
    with pytest.raises(RuntimeError, match="not acknowledged"):
        freeze.OpenSearchHold(hop, Eng()).hold(["shop"], "os_indices",
                                               lambda m: None)


def test_cassandra_revokes_only_what_the_role_holds_on_the_keyspace(
        tmp_path):
    hop = _hop(tmp_path, "cassandra")
    sent = []

    class Row:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class Session:
        def execute(self, q, args=None):
            sent.append(q)
            if q == "LIST ROLES":
                return [Row(role="app", login=True, super=False),
                        Row(role="migkit", login=True, super=True)]
            if q.startswith("LIST MODIFY OF"):
                return [Row(role="app", resource="<keyspace app>"),
                        Row(role="app", resource="<table app.orders>"),
                        Row(role="app", resource="<keyspace other>"),
                        Row(role="app", resource="<all keyspaces>")]
            if "permissions_validity" in q:
                # a cache that holds nothing: the test does not wait
                return [("0ms",)]
            return []

    class Eng:
        CANON_ENGINE = "cassandra"

        def _session(self, side):
            return Session()

        def _ks(self, side, db):
            return db
    h = freeze.CassandraHold(hop, Eng())
    h.hold(["app"], "cql_revoke", lambda m: None)
    revoked = [q for q in sent if q.startswith("REVOKE")]
    assert revoked == ['REVOKE MODIFY ON KEYSPACE "app" FROM "app"',
                       'REVOKE MODIFY ON TABLE "app"."orders" FROM "app"']
    rec = freeze.state(hop, "app", "src")
    assert rec["roles"]["app"]["revoked"] == ["<keyspace app>",
                                              "<table app.orders>"]
    sent.clear()
    h.release("app", rec, lambda m: None)
    assert [q for q in sent if q.startswith("GRANT")] == [
        'GRANT MODIFY ON KEYSPACE "app" TO "app"',
        'GRANT MODIFY ON TABLE "app"."orders" TO "app"']
    # the grant on every keyspace a revoke here does not reach: named
    said = h.exempt("app", "cql_revoke")
    assert any("app may write <all keyspaces>" in s and "per-database" in s
               for s in said), said


def test_a_thaw_goes_through_the_hold_of_the_side_it_was_taken_on(
        tmp_path, monkeypatch):
    """`thaw` of a record hands it to its engine's hold, and forgets it -
    the one way a record is given back, for the source and the target."""
    hop = _hop(tmp_path, "redis")
    given = []
    monkeypatch.setattr(freeze.RedisHold, "release",
                        lambda self, db, record, say: given.append(
                            (self.side, db, record["way"])))
    for side in ("src", "dst"):
        freeze._save(hop, "0", {"db": "0", "way": "redis_accounts"}, side)
        freeze.thaw(hop, _Eng("redis"), "0", lambda m: None, side)
        assert freeze.state(hop, "0", side) is None
    assert given == [("src", "0", "redis_accounts"),
                     ("dst", "0", "redis_accounts")]
    # the server's record of each side is its own file
    freeze._server_save(hop, {"way": "redis_pause"}, "dst")
    assert freeze.server_state(hop) is None
    assert json.loads(freeze._server_path(hop, "dst").read_text())[
        "way"] == "redis_pause"
