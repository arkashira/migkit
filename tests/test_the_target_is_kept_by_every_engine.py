"""The target kept from the application while migkit moves into it, on
every engine (`protect_target`, backlog BAR-3), and the source's writes
held at cutover on every engine that grants rights (F2.freeze) - one hold
per engine (`freeze.Hold`) for both sides.

Without a server: every server answer is a stand-in, so what each hold
records before it acts, what it gives back, and what its read back says
are held to what they are given - and each read back is shown able to
say "not held". The servers themselves are in the docker tests
(`test_a_target_is_kept_on_its_server.py`).
"""
import json

import pytest

from migkit import decide, freeze
from migkit.config import Endpoint, Hop
from migkit.engines import NAMES


def _hop(tmp_path, engine, **options):
    hop = Hop(name="kt", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=1, user="migkit",
                              password="CHANGE_ME"),
              databases=["app"], options=dict(options))
    root = tmp_path / "reports"

    def report_dir(db=""):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


class _Eng:
    """A side engine of `kind` that answers what the test gives it."""

    def __init__(self, kind, **given):
        self.CANON_ENGINE = kind
        for k, v in given.items():
            setattr(self, k, v)


# --- every engine says what it does with both shapes -------------------------

def test_every_engine_keeps_a_target_or_says_why_it_cannot():
    for name in NAMES:
        state, detail = decide.cell(name, "read-only target")
        assert state in ("yes", decide.NOT_APPLICABLE), (name, state)
        if name == "hetero":
            assert detail == "freeze.protect"
        elif state == "yes":
            cls = freeze.KEEPS[name]
            assert detail == f"freeze.{cls.__name__}", name
            assert cls.TARGET in ("held", "offered", "named"), name
        else:
            assert detail and name not in freeze.KEEPS, name
    assert not [c for c in decide.uncovered() if c[1] == "read-only target"]


def test_every_engine_holds_a_source_or_says_why_it_cannot():
    for name in NAMES:
        state, detail = decide.cell(name, "writes held")
        assert state in ("yes", decide.NOT_APPLICABLE), (name, state)
        if name == "hetero":
            assert detail == "freeze.hold_for"
        elif state == "yes":
            assert detail == f"freeze.{freeze.HOLDS[name].__name__}", name
        else:
            assert detail and name not in freeze.HOLDS, name


def test_every_way_of_keeping_a_target_leaves_migkit_writing():
    for kind, cls in freeze.KEEPS.items():
        rungs = cls(None, _Eng(kind), "dst").keep_rungs()
        assert rungs, kind
        for r in rungs:
            assert r.lacks(freeze.KEEP) is None, (kind, r.name)
            assert freeze.SCOPE[r.name] == "database", (kind, r.name)


def test_a_goal_turns_it_on_only_where_migkit_keeps_the_target_itself():
    for kind, cls in freeze.KEEPS.items():
        hop = Hop(name="x", engine=kind, source=Endpoint(),
                  target=Endpoint())
        assert freeze.supported(hop) == (cls.TARGET in ("held", "named")), \
            kind
    # an access policy changed on an answer is no goal's to give
    assert not freeze.supported(Hop(name="x", engine="dynamodb",
                                    source=Endpoint(), target=Endpoint()))
    assert not freeze.supported(Hop(name="x", engine="sqlite",
                                    source=Endpoint(), target=Endpoint()))
    pair = Hop(name="x", engine="hetero", source=Endpoint(),
               target=Endpoint(),
               options={"source_engine": "mysql",
                        "target_engine": "clickhouse"})
    assert freeze.target_kind(pair) == "clickhouse" and \
        freeze.supported(pair)
    pair.options["target_engine"] = "bigquery"
    assert not freeze.supported(pair)


# --- a target nothing keeps ----------------------------------------------------

def test_a_target_nothing_keeps_stops_the_move_or_is_said_for_a_goal(
        tmp_path):
    hop = _hop(tmp_path, "sqlite", protect_target=True)
    said = []
    with pytest.raises(SystemExit) as got:
        freeze.protect(hop, _Eng("sqlite"), "app", said.append)
    assert "not available for sqlite targets" in str(got.value)
    assert "nothing has been copied" in str(got.value)
    assert freeze.state(hop, "app", "dst") is None
    # what `ready` says before anything runs, in the same words
    assert "not available for sqlite targets" in freeze.not_protected(
        hop, _Eng("sqlite"), "app")
    # turned on by the hop's goal, not the hop: said, and moved into
    vars(hop)["_from_goal"] = ["protect_target"]
    assert freeze.protect(hop, _Eng("sqlite"), "app", said.append) is None
    assert "moved into as before" in said[-1], said
    assert freeze.not_protected(hop, _Eng("sqlite"), "app") is None


def test_a_hop_without_it_reads_nothing(tmp_path):
    hop = _hop(tmp_path, "sqlite")
    assert freeze.not_protected(hop, None, "app") is None
    rows = freeze.kept_rows(hop, None, "app", "fail")
    assert rows[0][0] == "warn" and "protect_target" in rows[0][1]


# --- one logic for the engines that grant rights ------------------------------

class _Fake(freeze._Grants):
    """An engine that grants rights, on an in-memory server: every
    statement is checked to come after its record."""
    WAY, SAID, SOURCE = "pgw_grants", "the grants taken", True

    def __init__(self, hop, server, side="dst"):
        super().__init__(hop, _Eng("cockroachdb"), side)
        self.server = server

    def name(self, db):
        return db

    def accounts(self, db):
        return sorted(self.server["accounts"])

    def grants(self, db, who):
        return [list(g) for g in sorted(self.server["grants"].get(who, ()))]

    def take(self, db, who, grant):
        return [("revoke", who, tuple(grant))]

    def give(self, db, who, grant):
        return [("grant", who, tuple(grant))]

    def run(self, db, *stmts):
        for kind, who, g in stmts:
            rec = freeze.state(self.hop, db, self.side) or {}
            if kind == "revoke":
                # recorded before it is done
                assert list(g) in rec["roles"][who]["revoked"]
            self.server["log"].append((kind, who, g))
            held = self.server["grants"].setdefault(who, set())
            (held.discard if kind == "revoke" else held.add)(g)

    def end(self, db, who):
        self.server["ended"].append(who)
        return 1

    def clock(self, db):
        return "T0"

    def left(self, db, who, at):
        n = len(self.server["sessions"])
        return (n == 0, f"{n} sessions left from before {at}")

    def open(self, db, older_than):
        return list(self.server["open"])

    def wider(self, db):
        return [("an owner writes whatever is revoked", True),
                ("a superuser writes everything", False)]


def _server():
    return {"accounts": ["app", "report"],
            "grants": {"app": {("t", "INSERT"), ("u", "DELETE")},
                       "report": set()},
            "log": [], "ended": [], "sessions": [], "open": []}


def test_a_target_held_by_rights_is_given_back_exactly(tmp_path):
    hop = _hop(tmp_path, "cockroachdb", protect_target=True)
    server = _server()
    before = {k: set(v) for k, v in server["grants"].items()}
    h = _Fake(hop, server)
    said = []
    h.keep("app", "pgw_grants", said.append)
    assert server["grants"]["app"] == set()
    record = freeze.state(hop, "app", "dst")
    assert sorted(map(tuple, record["roles"]["app"]["revoked"])) == sorted(
        before["app"])
    # a target's sessions are ended only where something was taken
    assert server["ended"] == ["app"]
    assert any("an owner writes" in s for s in said)
    assert not any("per-database freeze" in s for s in said), said
    assert all(ok for ok, _ in h.kept("app", record))
    # a grant made since is read back as not held
    server["grants"]["report"].add(("t", "UPDATE"))
    got = h.kept("app", record)
    assert [ok for ok, what in got if what.startswith("report")] == [False]
    server["grants"]["report"].clear()
    # taken again: nothing twice
    h.keep("app", "pgw_grants", said.append)
    assert sum(1 for k, *_ in server["log"] if k == "revoke") == 2
    h.release("app", record, said.append)
    assert server["grants"] == before


def test_a_source_held_by_rights_ends_every_session_and_reads_back(
        tmp_path):
    hop = _hop(tmp_path, "cockroachdb")
    server = _server()
    h = _Fake(hop, server, "src")
    h.hold(["app"], "pgw_grants", lambda m: None)
    # on a source every account's sessions go, one with nothing to take
    assert sorted(server["ended"]) == ["app", "report"]
    assert freeze.state(hop, "app", "src")["at"] == "T0"
    assert all(ok for ok, _ in h.held("app", "pgw_grants"))
    server["sessions"].append("app 7")
    assert not all(ok for ok, _ in h.held("app", "pgw_grants"))
    server["sessions"].clear()
    server["open"].append("write transaction app 9, open 3s")
    got = h.held("app", "pgw_grants")
    assert [ok for ok, what in got if "write transaction" in what] == [False]
    # what a per-database freeze leaves stops the cutover; a superuser
    # is said
    lines = h.exempt("app", "pgw_grants")
    assert lines[0].endswith(freeze.LEAVES)
    assert not lines[1].endswith(freeze.LEAVES)
    # the source's way needs its sessions ended and its open work seen
    names = [n.what for n in h.rungs()[0].needs]
    assert "a way to end the application's sessions" in names
    assert "the application's open work in sight" in names
    facts = decide.Facts({"ends": True, "sees": None, "others": []})
    got = decide.climb("t", h.rungs(), facts, (freeze.OURS,), prove=False)
    assert got.rung is None and "could not be read" in got.reason


# --- each engine's statements ---------------------------------------------------

def _ch():
    from migkit.engines.clickhouse import ClickHouseEngine
    return _Eng("clickhouse", _d=lambda side, db: db,
                _q=ClickHouseEngine._q)


def test_clickhouse_revokes_and_gives_back_each_scope(tmp_path):
    h = freeze.ClickHouseHold(_hop(tmp_path, "clickhouse"), _ch(), "dst")
    every = ["ALL", None, None, None, True, ["INSERT"]]
    assert h.take("app", "u", every) == [
        "revoke INSERT, ALTER UPDATE, ALTER DELETE, TRUNCATE on `app`.*"
        " from `u`"]
    # the partial revoke there before is left as it was
    assert h.give("app", "u", every) == [
        "grant ALTER UPDATE, ALTER DELETE, TRUNCATE on `app`.* to `u`"
        " with grant option"]
    table = ["INSERT", "app", "t", None, False, []]
    assert h.take("app", "u", table) == ["revoke INSERT on `app`.`t` from"
                                         " `u`"]
    assert h.give("app", "u", table) == ["grant INSERT on `app`.`t` to `u`"]
    column = ["INSERT", "app", "t", "c", False, []]
    assert h.take("app", "u", column) == ["revoke INSERT(`c`) on `app`.`t`"
                                          " from `u`"]
    alter = ["ALTER", "app", None, None, False, []]
    assert h.take("app", "u", alter) == ["revoke ALTER UPDATE, ALTER DELETE"
                                         " on `app`.* from `u`"]
    assert h.give("app", "u", alter) == ["grant ALTER on `app`.* to `u`"]


def test_clickhouse_reads_back_a_write_left_through_a_role(tmp_path,
                                                           monkeypatch):
    h = freeze.ClickHouseHold(_hop(tmp_path, "clickhouse"), _ch(), "dst")
    monkeypatch.setattr(h, "users", lambda: {"u": "local_directory"})
    monkeypatch.setattr(h, "read_only", lambda who: False)
    monkeypatch.setattr(h, "held_roles", lambda who: {"writer"}
                        if who == "u" else set())
    rows = [["u", None, "ALL", None, None, 0]]
    rows += [["u", None, w, "app", None, 1] for w in freeze._CH_WRITE]
    monkeypatch.setattr(h, "rows", lambda db, sql, args=(): rows)
    assert h.still("app", "u") == []
    # the role still writes: read back as not held
    rows.append([None, "writer", "INSERT", "app", None, 0])
    assert h.still("app", "u") == ["insert (writer)"]


def _db2(kind="db2"):
    return _Eng(kind, _owner=lambda side, db: "APP",
                _d=lambda side, db: db)


def test_db2_takes_tables_packages_and_routines(tmp_path):
    h = freeze.Db2Hold(_hop(tmp_path, "db2"), _db2(), "dst")
    table = ["TABLE", "T", "U", "INSERT", True, ""]
    assert h.take("app", "BOB", table) == [
        'revoke insert on table "APP"."T" from user "BOB"']
    assert h.give("app", "BOB", table) == [
        'grant insert on table "APP"."T" to user "BOB" with grant option']
    control = ["TABLE", "T", "U", "CONTROL", True, ""]
    assert h.give("app", "BOB", control) == [
        'grant control on table "APP"."T" to user "BOB"']
    package = ["PACKAGE", "S.P1", "G", "EXECUTE", False, ""]
    assert h.take("app", "APPS", package) == [
        'revoke execute on package "S"."P1" from group "APPS"']
    routine = ["ROUTINE", "SQL1", "R", "EXECUTE", False, "F"]
    assert h.take("app", "W", routine) == [
        'revoke execute on specific function "APP"."SQL1" from role "W"']
    assert h.take("app", "PUBLIC", table) == [
        'revoke insert on table "APP"."T" from public']
    z = freeze.Db2ZosHold(_hop(tmp_path, "db2zos"), _db2("db2zos"), "dst")
    assert z.take("app", "BOB", ["TABLE", "T", "", "UPDATE", False, ""]) == [
        'revoke update on table "APP"."T" from "BOB" by all']
    assert z.take("app", "W", ["TABLE", "T", "L", "UPDATE", False, ""]) == [
        'revoke update on table "APP"."T" from role "W" by all']


def test_db2_one_privilege_of_several_grantors_is_one_revoke(tmp_path,
                                                             monkeypatch):
    h = freeze.Db2Hold(_hop(tmp_path, "db2"), _db2(), "dst")
    monkeypatch.setattr(h, "privileges", lambda db: [
        ("BOB", "U", "TABLE", "T", "INSERT", "Y", ""),
        ("BOB", "U", "TABLE", "T", "INSERT", "G", ""),
        ("ANN", "U", "TABLE", "T", "INSERT", "Y", "")])
    assert h.grants("app", "BOB") == [["TABLE", "T", "U", "INSERT", True,
                                       ""]]


def test_oracle_snowflake_redshift_ase_teradata_and_the_pg_wire_say_theirs(
        tmp_path):
    o = freeze.OracleHold(_hop(tmp_path, "oracle"), _Eng(
        "oracle", _owner=lambda side, db: "APP"), "dst")
    assert o.take("app", "BOB", ["T", "INSERT", True]) == [
        'revoke insert on "APP"."T" from "BOB"']
    assert o.give("app", "BOB", ["P", "EXECUTE", True]) == [
        'grant execute on "APP"."P" to "BOB" with grant option']
    s = freeze.SnowflakeHold(_hop(tmp_path, "snowflake"), _Eng(
        "snowflake", _owner=lambda side, db: "S",
        _database=lambda side: "DB"), "dst")
    assert s.take("app", "R", ["future", None, "INSERT", False]) == [
        'revoke insert on future tables in schema "DB"."S" from role "R"']
    assert s.give("app", "R", ["table", "T", "DELETE", True]) == [
        'grant delete on table "DB"."S"."T" to role "R" with grant option']
    r = freeze.RedshiftHold(_hop(tmp_path, "redshift"), _Eng(
        "redshift", _d=lambda side, db: "app"), "dst")
    assert r.take("app", "g", ["group", "t", "INSERT", True]) == [
        'revoke insert on "app"."t" from group "g"']
    # a group or a role holds no grant option
    assert r.give("app", "g", ["group", "t", "INSERT", True]) == [
        'grant insert on "app"."t" to group "g"']
    assert r.give("app", "u", ["user", "t", "INSERT", True]) == [
        'grant insert on "app"."t" to "u" with grant option']
    a = freeze.AseHold(_hop(tmp_path, "ase"), _Eng("ase"), "dst")
    assert a.give("app", "u", ["dbo", "t", "insert", True]) == [
        'grant insert on "dbo"."t" to "u" with grant option']
    assert a.take("app", "u", ["dbo", "p", "execute", False]) == [
        'revoke execute on "dbo"."p" from "u"']
    t = freeze.TeradataHold(_hop(tmp_path, "teradata"), _Eng(
        "teradata", _d=lambda side, db: "APPDB"), "dst")
    assert t.take("app", "U", ["All", "insert", False]) == [
        'revoke insert on "APPDB" from "U"']
    assert t.give("app", "U", ["t", "update", True]) == [
        'grant update on "APPDB"."t" to "U" with grant option']
    p = freeze.PgWireHold(_hop(tmp_path, "cockroachdb"),
                          _Eng("cockroachdb"), "dst")
    assert p.take("app", "app", ["public", "t", "INSERT", True]) == [
        'revoke insert on table "public"."t" from "app"']


def test_oracle_never_locks_an_owner_whose_state_it_cannot_read(
        tmp_path, monkeypatch):
    o = freeze.OracleHold(_hop(tmp_path, "oracle"), _Eng(
        "oracle", _owner=lambda side, db: "APP"), "dst")
    ran = []
    monkeypatch.setattr(o, "status", lambda db, who: None)
    monkeypatch.setattr(o, "run", lambda db, *s: ran.extend(s))
    entry = {"revoked": []}
    said = o.more("app", "APP", entry, lambda: None)
    assert ran == [] and "status" not in entry and "not locked" in said
    # read: locked, and unlocked again only where it was open
    monkeypatch.setattr(o, "status", lambda db, who: "OPEN")
    o.more("app", "APP", entry, lambda: None)
    assert ran == ['alter user "APP" account lock']
    o.less("app", "APP", entry)
    assert ran[-1] == 'alter user "APP" account unlock'
    ran.clear()
    o.less("app", "APP", {"status": "LOCKED"})
    assert ran == []


# --- Redis, Kafka, MongoDB and Cassandra on a target ----------------------------

def test_redis_leaves_migkits_own_account_and_needs_one(tmp_path,
                                                        monkeypatch):
    hop = _hop(tmp_path, "redis")
    eng = _Eng("redis", REPL_USER="migkit_repl")
    h = freeze.RedisHold(hop, eng, "dst")
    monkeypatch.setattr(h, "rules", lambda: {
        "default": ["on"], "app": ["on"], "migkit": ["on"],
        "migkit_repl": ["on"]})
    assert h.accounts() == ["app", "default"]
    # the same account on a source is held, migkit's among them
    src = freeze.RedisHold(hop, eng, "src")
    monkeypatch.setattr(src, "rules", h.rules)
    assert src.accounts() == ["app", "default", "migkit"]
    rung = h.keep_rungs()[0]
    facts = decide.Facts({"version": (7, 2), "acl": True,
                          "one server": True, "own account": False})
    got = decide.climb("t", (rung,), facts, freeze.KEEP, prove=False)
    assert got.rung is None and "default account" in got.reason
    facts = decide.Facts({"version": (7, 2), "acl": True,
                          "one server": False, "own account": True})
    got = decide.climb("t", (rung,), facts, freeze.KEEP, prove=False)
    assert got.rung is None and "cluster" in got.reason


class _Admin:
    """A Kafka cluster's ACLs, in memory."""

    def __init__(self):
        self.acls = set()

    def describe_acls(self, f):
        key = (f.resource_pattern.resource_name, f.principal)
        return ([key] if key in self.acls else []), None

    def create_acls(self, acls):
        for a in acls:
            self.acls.add((a.resource_pattern.resource_name, a.principal))
        return {"failed": []}

    def delete_acls(self, filters):
        for f in filters:
            self.acls.discard((f.resource_pattern.resource_name,
                               f.principal))

    def close(self):
        pass


def test_kafka_denies_each_principal_but_migkits_and_reads_back_a_new_one(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "kafka")
    admin = _Admin()
    h = freeze.KafkaHold(hop, _Eng("kafka", _admin=lambda side: admin),
                         "dst")
    allowed = {"User:app", "User:migkit", "User:ops"}
    monkeypatch.setattr(h, "topics", lambda dbs, every=False: ["orders"])
    monkeypatch.setattr(h, "allowed", lambda topics: set(allowed))
    monkeypatch.setattr(h, "super_users", lambda: ["User:ops"])
    monkeypatch.setattr(h, "principal", lambda: "User:migkit")
    h.keep("app", "kafka_principals", lambda m: None)
    assert admin.acls == {("orders", "User:app")}
    rec = freeze.server_state(hop, "dst")
    assert rec["made"] == [["TOPIC", "orders", "User:app"]]
    assert all(ok for ok, _ in h.kept("app", rec))
    allowed.add("User:new")
    got = h.kept("app", rec)
    assert not got[-1][0] and "User:new" in got[-1][1]
    h.release_server(rec, lambda m: None)
    assert admin.acls == set()


def test_mongodb_is_not_held_where_the_server_checks_nobody():
    rungs = freeze.MongoHold(None, _Eng("mongodb"), "dst").keep_rungs()
    facts = decide.Facts({"others": [], "commands": [
        "grantRolesToUser", "revokeRolesFromUser"], "auth": False})
    got = decide.climb("t", rungs, facts, freeze.KEEP, prove=False)
    assert got.rung is None and "authorization off" in got.reason
    facts = decide.Facts({"others": [], "commands": [
        "grantRolesToUser", "revokeRolesFromUser"], "auth": True})
    assert decide.climb("t", rungs, facts, freeze.KEEP,
                        prove=False).rung.name == "mongo_database"


def test_cassandra_keeps_a_target_whatever_its_cache_or_budget():
    rungs = freeze.CassandraHold(None, _Eng("cassandra"), "dst").keep_rungs()
    facts = decide.Facts({"authorizer": True, "authorize": True,
                          "cache": 300.0, "budget": None})
    assert decide.climb("t", rungs, facts, freeze.KEEP,
                        prove=False).rung.name == "cql_revoke"


# --- the writes named where nothing holds a writer --------------------------------

def test_opensearch_names_the_writes_that_are_not_migkits(tmp_path,
                                                          monkeypatch):
    hop = _hop(tmp_path, "opensearch", protect_target=True)
    h = freeze.OpenSearchHold(hop, _Eng("opensearch"), "dst")
    ops = {"app.t": ["id1", 10]}
    monkeypatch.setattr(freeze.OpenSearchHold, "ops", lambda self, db: {
        k: list(v) for k, v in ops.items()})
    # nothing counted before the target's writes are named
    freeze.counted(hop, {"app.t": 1})
    assert not (hop.report_dir() / freeze.OURS_FILE).exists()
    h.keep("app", "os_named", lambda m: None)
    record = freeze.state(hop, "app", "dst")
    assert record["base"] == {"app.t": ["id1", 10]}
    # migkit's own three, counted by the writer, and three numbered
    freeze.counted(hop, {"app.t": 3})
    ops["app.t"][1] = 13
    assert all(ok for ok, _ in h.kept("app", record))
    # one more numbered than migkit was told of: named
    ops["app.t"][1] = 14
    got = h.kept("app", record)
    assert not got[0][0] and "1 writes that are not migkit's" in got[0][1]
    # an index made since starts at nothing; one made again is named
    ops["app.u"] = ["id9", 2]
    got = h.kept("app", record)
    assert any("app.u: 2 writes" in w for ok, w in got if not ok), got
    ops.pop("app.u")
    ops["app.t"] = ["id2", 13]
    got = h.kept("app", record)
    assert any("made again" in w for ok, w in got if not ok), got
    # the cutover's preflight says it as drift, at the gate's level
    hop.options["protect_target"] = True
    rows = freeze.kept_rows(hop, _Eng("opensearch"), "app", "fail")
    assert "fail" in [lvl for lvl, _ in rows]
    h.release("app", record, lambda m: None)
    assert not (hop.report_dir() / freeze.OURS_FILE).exists()


# --- a change of the target's access policy, offered ----------------------------

class _PolicyNotFoundException(Exception):
    pass


class _Ddb:
    """DynamoDB's tables' own policies, in memory."""

    def __init__(self):
        self.policies, self.rev = {}, 0

    def get_resource_policy(self, ResourceArn):
        if ResourceArn not in self.policies:
            raise _PolicyNotFoundException("no policy")
        text, rev = self.policies[ResourceArn]
        return {"Policy": text, "RevisionId": rev}

    def put_resource_policy(self, ResourceArn, Policy,
                            ExpectedRevisionId=None):
        now = self.policies.get(ResourceArn)
        if ExpectedRevisionId == "NO_POLICY":
            assert now is None
        elif ExpectedRevisionId is not None:
            assert now and now[1] == ExpectedRevisionId
        self.rev += 1
        self.policies[ResourceArn] = (Policy, str(self.rev))

    def delete_resource_policy(self, ResourceArn):
        self.policies.pop(ResourceArn)


ROLE = "arn:aws:iam::ACCOUNT:role/migkit"


def _ddb(tmp_path, monkeypatch, **options):
    hop = _hop(tmp_path, "dynamodb", protect_target=True, **options)
    client = _Ddb()
    eng = _Eng("dynamodb", _client=lambda side: client)
    monkeypatch.setattr(freeze.DynamoDBHold, "tables", lambda self, db: {
        "app.t": "arn:t", "app.u": "arn:u"})
    monkeypatch.setattr(freeze.DynamoDBHold, "principals",
                        lambda self: [ROLE])
    return hop, eng, client


def test_dynamodb_asks_before_it_changes_a_tables_policy(tmp_path,
                                                         monkeypatch):
    hop, eng, client = _ddb(tmp_path, monkeypatch)
    # not answered yet: `ready` lets --go at a terminal ask it
    assert freeze.not_protected(hop, eng, "app") is None
    said = []
    with pytest.raises(SystemExit) as got:
        freeze.protect(hop, eng, "app", said.append, ask=lambda q: "no")
    assert "the answer to the change of the target's access is no" in str(
        got.value)
    assert any("offered, never assumed" in s for s in said), said
    assert client.policies == {} and freeze.answered(hop) == "no"
    # answered no: `ready` says it before anything runs
    assert "the answer to the change of the target's access is no" in \
        freeze.not_protected(hop, eng, "app")
    # unanswered and nobody to ask: it says how to answer, and changes
    # nothing
    (hop.report_dir() / freeze.ACCESS_FILE).unlink()
    with pytest.raises(SystemExit) as got:
        freeze.protect(hop, eng, "app", said.append)
    assert "target_access: yes" in str(got.value)
    assert client.policies == {}


def test_dynamodb_denies_every_principal_but_migkits_and_gives_back(
        tmp_path, monkeypatch):
    hop, eng, client = _ddb(tmp_path, monkeypatch, target_access="yes")
    theirs = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Sid": "Theirs", "Effect": "Allow", "Principal": "*",
         "Action": "dynamodb:GetItem", "Resource": "arn:u"}]})
    client.policies["arn:u"] = (theirs, "r0")
    freeze.protect(hop, eng, "app", lambda m: None)
    t = json.loads(client.policies["arn:t"][0])["Statement"]
    assert [s["Sid"] for s in t] == [freeze._DDB_SID]
    assert t[0]["Effect"] == "Deny" and t[0]["Condition"]["ArnNotLike"][
        "aws:PrincipalArn"] == [ROLE]
    u = json.loads(client.policies["arn:u"][0])["Statement"]
    assert [s["Sid"] for s in u] == ["Theirs", freeze._DDB_SID]
    record = freeze.state(hop, "app", "dst")
    h = freeze.hold_for(hop, eng, "dst")
    assert all(ok for ok, _ in h.kept("app", record))
    # a deny taken off by someone else is read back as not held
    client.policies["arn:u"] = (theirs, "r9")
    assert not all(ok for ok, _ in h.kept("app", record))
    freeze.thaw(hop, eng, "app", lambda m: None)
    assert client.policies == {"arn:u": (theirs, client.policies["arn:u"][
        1])} and json.loads(client.policies["arn:u"][0]) == json.loads(
        theirs)


# --- the record, the preflight and doctor --------------------------------------

def test_the_preflight_reads_a_hold_back_and_doctor_says_it(tmp_path,
                                                            monkeypatch):
    hop = _hop(tmp_path, "cockroachdb", protect_target=True)
    rows = freeze.kept_rows(hop, None, "app", "fail")
    assert rows == [("warn", "app: protect_target is set and nothing holds"
                             " the target: the application may have written"
                             " there")]
    freeze._save(hop, "app", {"db": "app", "way": "pgw_grants",
                              "roles": {"app": {"revoked": []}}}, "dst")
    monkeypatch.setattr(freeze.PgWireHold, "kept",
                        lambda self, db, rec: [(True, "app: writes nothing"),
                                               (False, "ann: can still"
                                                       " write t")])
    rows = freeze.kept_rows(hop, _Eng("cockroachdb"), "app", "fail")
    # a hold that leaves a writer is a warning, not drift
    assert [lvl for lvl, _ in rows] == ["ok", "warn"]
    assert freeze.kept_line(hop, "app") == ("app is kept from the"
                                            " application's writes - app")


def test_a_target_held_through_the_servers_accounts_gives_them_back_once(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "redis", protect_target=True)
    hop.databases = ["0", "1"]
    given = []
    monkeypatch.setattr(freeze.RedisHold, "release_server",
                        lambda self, rec, say: given.append(rec["way"]))
    freeze._server_save(hop, {"engine": "redis", "way": "redis_accounts",
                              "rules": {"app": ["on"]}}, "dst")
    for d in hop.databases:
        freeze._save(hop, d, {"db": d, "server": True,
                              "way": "redis_accounts"}, "dst")
    eng = _Eng("redis", REPL_USER="migkit_repl")
    freeze.thaw(hop, eng, "0", lambda m: None)
    freeze.thaw(hop, eng, "1", lambda m: None)
    assert given == ["redis_accounts"]
    assert freeze.server_state(hop, "dst") is None


# --- TiDB's own switch ------------------------------------------------------------

def test_tidb_has_its_own_switch_and_mysqls_per_database_way():
    rungs = freeze.TiDBHold(None, _Eng("tidb")).rungs()
    assert [r.name for r in rungs] == ["tidb_server", "my_database"]
    facts = decide.Facts({"others": [], "version": (6, 1),
                          "read only switch": True, "kill": "kill",
                          "grant option": True})
    got = decide.climb("t", rungs, facts, prove=False)
    assert got.rung.name == "my_database" and "6.2" in got.reason
    facts["version"] = (7, 5)
    assert decide.climb("t", rungs, facts, prove=False).rung.name == \
        "tidb_server"


def test_tidb_reads_its_version_from_the_server():
    eng = _Eng("tidb", _q=lambda side, sql: [["8.0.11-TiDB-v7.5.1"]])
    assert freeze._tidb_version(eng) == (7, 5)
    eng = _Eng("tidb", _q=lambda side, sql: [["8.0.36"]])
    assert freeze._tidb_version(eng) is None


# --- the move, its plan and the interlock -------------------------------------------

def test_the_dry_run_says_the_way_and_changes_nothing(tmp_path, monkeypatch):
    from migkit import cli
    hop = _hop(tmp_path, "cockroachdb", protect_target=True)
    planned, kept = [], []
    monkeypatch.setattr(freeze, "protect_plan",
                        lambda h, e, dbs: planned.append(dbs) or ["said"])
    monkeypatch.setattr(freeze, "protect",
                        lambda *a, **k: kept.append(a))

    class Eng:
        def databases(self):
            return ["app"]
    cli._protect_target(hop, Eng(), None, go=False)
    assert planned == [["app"]] and kept == []
    # after a copy, taken again only where it was taken before
    cli._protect_target(hop, Eng(), None, again=True)
    assert kept == []
    freeze._save(hop, "app", {"db": "app", "way": "pgw_grants",
                              "roles": {}}, "dst")
    cli._protect_target(hop, Eng(), None, again=True)
    assert len(kept) == 1


def test_ready_names_a_target_the_hop_asks_to_keep_and_nothing_keeps(
        tmp_path, monkeypatch):
    from migkit import interlock
    hop = _hop(tmp_path, "sqlite", protect_target=True)
    monkeypatch.setattr(interlock, "identities", lambda h, e, d: (
        {"engine": "sqlite", "address": "/a", "database": "app"},
        {"engine": "sqlite", "address": "/b", "database": "app"}))
    got = interlock.refusals(hop, _Eng("sqlite"), "app")
    assert any("protect_target is set" in w and "sqlite" in w for w in got)
    hop.options = {}
    assert interlock.refusals(hop, _Eng("sqlite"), "app") == []


def test_the_option_that_answers_the_offer_is_declared():
    from migkit import hopkeys
    key = hopkeys.HOP_OPTIONS["target_access"]
    assert set(key.engines) == {"dynamodb", "bigquery"}
    assert "protect_target" in hopkeys.HOP_OPTIONS


# --- Couchbase and BigQuery, on stand-ins of their APIs -------------------------

def _cb_parse(text):
    out = []
    for part in [p for p in text.split(",") if p]:
        name, _, scope = part.partition("[")
        keys = ("bucket_name", "scope_name", "collection_name")
        r = {"role": name, "origins": [{"type": "user"}]}
        r.update(dict(zip(keys, scope.rstrip("]").split(":")))
                 if scope else {})
        out.append(r)
    return out


def test_couchbase_swaps_the_roles_that_write_and_puts_the_user_back_whole(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, "couchbase", protect_target=True)
    users = {
        "app": {"id": "app", "name": "App", "groups": ["g1"],
                "roles": _cb_parse("data_writer[app:_default:_default],"
                                   "query_select[app]")},
        "ops": {"id": "ops", "name": "", "groups": [],
                "roles": _cb_parse("admin")},
        "migkit": {"id": "migkit", "name": "", "groups": [],
                   "roles": _cb_parse("admin")}}
    puts = []

    def call(self, method, path, form=None):
        if method == "GET":
            return [dict(u) for u in users.values()]
        uid = path.rsplit("/", 1)[-1]
        # the user is recorded before it is changed
        assert uid in (freeze.state(hop, "app", "dst") or {}).get(
            "roles", {})
        puts.append(form)
        users[uid] = {**users[uid], "name": form["name"],
                      "groups": [g for g in form["groups"].split(",") if g],
                      "roles": _cb_parse(form["roles"])}
        return {}
    monkeypatch.setattr(freeze.CouchbaseHold, "call", call)
    eng = _Eng("couchbase", _ep=lambda side: Endpoint(user="migkit"),
               _d=lambda side, db: db)
    h = freeze.hold_for(hop, eng, "dst")
    h.keep("app", "cb_roles", lambda m: None)
    roles = [freeze._cb_role(r) for r in users["app"]["roles"]]
    assert roles == ["query_select[app]", "data_reader[app:_default:"
                                          "_default]"]
    assert users["app"]["groups"] == ["g1"]
    record = freeze.state(hop, "app", "dst")
    got = h.kept("app", record)
    # an administrator writes whatever one user loses: never read as held
    assert [w for ok, w in got if not ok] == ["ops still writes here"
                                              " (admin)"]
    freeze.thaw(hop, eng, "app", lambda m: None)
    assert [freeze._cb_role(r) for r in users["app"]["roles"]] == [
        "data_writer[app:_default:_default]", "query_select[app]"]
    assert users["app"]["groups"] == ["g1"] and len(puts) == 2


class _Bq:
    """A project's dataset's access, in memory."""

    class _Creds:
        service_account_email = "migkit@example.com"

    def __init__(self, entries):
        from types import SimpleNamespace
        self._credentials = self._Creds()
        self.ds = SimpleNamespace(access_entries=list(entries))

    def get_dataset(self, ref):
        from types import SimpleNamespace
        return SimpleNamespace(access_entries=list(self.ds.access_entries))

    def update_dataset(self, ds, fields):
        assert fields == ["access_entries"]
        self.ds.access_entries = list(ds.access_entries)


def test_bigquery_makes_the_writers_readers_and_gives_back_exactly(
        tmp_path):
    from google.cloud.bigquery import AccessEntry
    hop = _hop(tmp_path, "bigquery", protect_target=True,
               target_access=True)
    start = [AccessEntry("WRITER", "userByEmail", "app@example.com"),
             AccessEntry("OWNER", "specialGroup", "projectOwners"),
             AccessEntry("READER", "userByEmail", "viewer@example.com")]
    client = _Bq(start)
    eng = _Eng("bigquery", _client=lambda side: client,
               _database=lambda side: "proj", _d=lambda side, db: db)
    freeze.protect(hop, eng, "app", lambda m: None)
    now = {(e.role, e.entity_type, e.entity_id)
           for e in client.ds.access_entries}
    assert now == {("READER", "userByEmail", "app@example.com"),
                   ("READER", "specialGroup", "projectOwners"),
                   ("READER", "userByEmail", "viewer@example.com"),
                   ("WRITER", "userByEmail", "migkit@example.com")}
    h = freeze.hold_for(hop, eng, "dst")
    record = freeze.state(hop, "app", "dst")
    assert all(ok for ok, _ in h.kept("app", record))
    late = AccessEntry("WRITER", "userByEmail", "new@example.com")
    client.ds.access_entries.append(late)
    assert not all(ok for ok, _ in h.kept("app", record))
    freeze.thaw(hop, eng, "app", lambda m: None)
    # what was taken given back, what was added gone, what came since left
    assert {(e.role, e.entity_type, e.entity_id)
            for e in client.ds.access_entries} == {
        (e.role, e.entity_type, e.entity_id) for e in start + [late]}
