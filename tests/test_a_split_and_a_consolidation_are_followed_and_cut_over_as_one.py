"""A split's parts and a consolidation's sources are followed as one
group and cut over at once (backlog W7, on what P0-SCENARIOS landed):
their rows still copied by each hop's own move, then every change of the
source - or of every source - followed in one loop, a row a rule moves
between parts removed from the part it left before it is written on the
one it reaches, a consolidation's changes under each source's value with
its keys shifted as its copy shifted them; the whole group frozen,
drained, proved and flipped on the cutover's one path.

The shaping of a consolidation's changes, the group's hold, its counters
and its drain run on stand-ins; the rest splits one PostgreSQL 16
database by region into two and follows a customer moving between them,
and consolidates two databases into one under a tenant key.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import consolidate, freeze, reshard, tailctl
from migkit.config import Endpoint, Hop
from migkit.engines.base import RepairAction
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-groups-src", 16534), ("migkit-test-groups-dst",
                                              16535)


def _chop(name="c", keys="offset", **more):
    return Hop(name=name, engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432),
               target=Endpoint(host="10.0.0.9", port=5432),
               goal="consolidate", databases=["shop"],
               options={"consolidate": {"column": "tenant_id", "value": "eu",
                                        "keys": keys, **more}})


# --- a consolidation's changes as its target holds its rows ------------------------

class _Dst:
    CANON_ENGINE = "postgres"

    def __init__(self, held):
        self.held = held

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("tenant_id", "text")]

    def neutral_rows_by_key(self, side, db, table, cols, key, keys):
        return {(str(k[0]),): [k[0], self.held[k[0]]] for k in keys
                if k[0] in self.held}


class _Pair:
    def __init__(self, held=None):
        self.dst_engine = _Dst(held or {})


def test_a_change_is_shifted_and_put_under_its_sources_value():
    me = consolidate.Source(_chop(), "shop", "eu")
    plan = consolidate.TablePlan("public.orders", "orders", ["id"], "offset",
                                 {"customer_id": "public.customers"})
    shape = {"public.orders": (plan, {"public.orders": 1000,
                                      "public.customers": 5000},
                               (1000, 2000))}
    got = consolidate.follow_changes(shape, _Pair(), me, [
        {"op": "insert", "table": "public.orders", "key": {"id": 5},
         "values": {"id": 5, "customer_id": 7, "note": "x"}},
        {"op": "update", "table": "public.orders", "key": {"id": "6"},
         "values": {"id": "6", "customer_id": None, "note": "y"}},
        {"op": "delete", "table": "public.orders", "key": {"id": 9}}])
    assert got[0]["key"] == {"id": 1005}
    assert got[0]["values"] == {"id": 1005, "customer_id": 5007,
                                "note": "x", "tenant_id": "eu"}
    # a key a change log hands over as text is shifted as a number, and a
    # reference that holds NULL stays NULL
    assert got[1]["key"] == {"id": 1006}
    assert got[1]["values"]["customer_id"] is None
    assert got[2] == {"op": "delete", "table": "public.orders",
                      "key": {"id": 1009}}
    # a key that outgrew its source's range would land among the next
    # source's keys: the follow stops before it
    with pytest.raises(SystemExit) as e:
        consolidate.follow_changes(shape, _Pair(), me, [
            {"op": "insert", "table": "public.orders", "key": {"id": 1500},
             "values": {"id": 1500, "customer_id": 1, "note": ""}}])
    assert "past its range" in str(e.value)


def test_a_tenant_key_leads_with_the_value_and_a_key_kept_as_it_is_is_held():
    me = consolidate.Source(_chop(keys="tenant"), "shop", "eu")
    plan = consolidate.TablePlan("public.items", "items", ["id"], "tenant")
    got = consolidate.follow_changes(
        {"public.items": (plan, {"public.items": 0}, None)}, _Pair(), me,
        [{"op": "delete", "table": "public.items", "key": {"id": 3}}])
    assert got[0]["key"] == {"tenant_id": "eu", "id": 3}
    apart = consolidate.TablePlan("public.items", "items", ["id"], "apart")
    shape = {"public.items": (apart, {"public.items": 0}, None)}
    # a key another source holds on the target arrived since the move
    with pytest.raises(SystemExit) as e:
        consolidate.follow_changes(shape, _Pair({3: "us"}), me, [
            {"op": "insert", "table": "public.items", "key": {"id": 3},
             "values": {"id": 3}}])
    assert "another source holds on the target" in str(e.value)
    # its own row under its own value goes on
    got = consolidate.follow_changes(shape, _Pair({3: "eu"}), me, [
        {"op": "update", "table": "public.items", "key": {"id": 3},
         "values": {"id": 3}}])
    assert got[0]["values"] == {"id": 3, "tenant_id": "eu"}


# --- the group's hold, its counters and its drain ---------------------------------

def _hop(name):
    return Hop(name=name, engine="postgres",
               source=Endpoint(host="10.0.0.1"),
               target=Endpoint(host="10.0.0.9"), databases=["shop"])


class _Group:
    CANON_ENGINE = freeze.GROUP

    def __init__(self, parts):
        self.parts = parts


class _Cluster:
    def __init__(self, parts):
        self.src_engine = _Group(parts)
        self.dst_engine = _Group(parts)


class _Choice:
    def __init__(self, ok, why=""):
        self.rung, self.reason = (object() if ok else None), why


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def test_a_group_is_held_where_every_side_is_and_said_where_one_is_not(
        reports, monkeypatch):
    from migkit import decide
    lead, a, b = _hop("lead"), _hop("a"), _hop("b")
    cluster = _Cluster([("a", a, "ea", "shop"), ("b", b, "eb", "shop")])
    h = freeze.hold_for(lead, cluster, "src")
    assert isinstance(h, freeze.GroupHold)
    server = {"a": True, "b": False}
    monkeypatch.setattr(freeze, "choose", lambda hop, eng, dbs, how,
                        side="src": _Choice(how != "server"
                                            or server[hop.name],
                                            f"{hop.name} may not"))
    got = decide.climb("t", h.rungs(), h.facts(["shop"]))
    # b's server cannot be held whole: every side held per database
    assert got.rung.name == "group_database", got.reason
    assert "b: b may not" in got.reason
    server["b"] = True
    got = decide.climb("t", h.rungs(), freeze.hold_for(
        lead, cluster, "src").facts(["shop"]))
    assert got.rung.name == "group_server"
    assert freeze.SCOPE["group_server"] == "server"
    done = []
    monkeypatch.setattr(freeze, "freeze_source",
                        lambda hop, eng, dbs, say, how:
                        done.append(("freeze", hop.name, eng, dbs, how)))
    h.hold(["shop"], "group_database", lambda m: None)
    assert done == [("freeze", "a", "ea", ["shop"], "database"),
                    ("freeze", "b", "eb", ["shop"], "database")]
    # the group's own record, so a thaw of the group reaches every side
    record = freeze.state(lead, "shop", "src")
    assert record["way"] == "group_database" and record["group"]
    monkeypatch.setattr(freeze, "thaw_source",
                        lambda hop, eng, dbs, say:
                        done.append(("thaw", hop.name, dbs)))
    h.release("shop", record, lambda m: None)
    assert done[-2:] == [("thaw", "a", ["shop"]), ("thaw", "b", ["shop"])]
    monkeypatch.setattr(freeze, "proof", lambda hop, eng, db, how:
                        [(hop.name == "a", f"{how} held")])
    assert h.held("shop", "group_database") == [
        (True, "a: database held"), (False, "b: database held")]
    monkeypatch.setattr(freeze, "writes_seen", lambda hop, eng, db, side:
                        {"a": 3, "b": None}[hop.name])
    # a side whose count cannot be read: no count, never "quiet"
    assert h.writes("shop") is None


class _Pairs:
    def __init__(self):
        self.applied = []

    def repair_plan(self, db, kind):
        return [RepairAction(db, "sequences", ["select 1"], [], "raised"),
                RepairAction(db, "rows", [], [], "not a counter")]

    def apply(self, db, action):
        self.applied.append((db, action.by))


def test_the_counters_are_each_targets_own(monkeypatch):
    lead = _hop("lead")
    pairs = [_Pairs(), _Pairs()]
    g = reshard.Group("reshard", lead, [lead],
                      [reshard.Side("lead", lead, "shop")],
                      [reshard.Side("a", lead, "shop"),
                       reshard.Side("b", lead, "shop")], {})
    g._pairs.update({(0, 0): pairs[0], (0, 1): pairs[1]})
    c = reshard.Cluster(g)
    plan = c.repair_plan("shop", "sequences")
    assert [(a.by, a.note) for a in plan] == [("0", "a: raised"),
                                              ("1", "b: raised")]
    for a in plan:
        c.apply("shop", a)
    assert pairs[0].applied == [("shop", "0")]
    assert pairs[1].applied == [("shop", "1")]


class _Reader:
    def __init__(self, reached):
        self.reached = reached

    def position_reached(self, have, want):
        return self.reached(have, want)


def test_the_drain_waits_for_every_sources_kept_position(reports,
                                                          monkeypatch):
    lead = _hop("lead")
    g = reshard.Group("reshard", lead, [lead],
                      [reshard.Side("s1", lead, "shop"),
                       reshard.Side("s2", lead, "shop")],
                      [reshard.Side("t", lead, "shop")], {})
    readers = {0: _Reader(lambda have, want: have >= want),
               1: _Reader(lambda have, want: have >= want)}
    monkeypatch.setattr(reshard.Group, "reader",
                        lambda self, i: readers[i])
    for i, at in ((0, 10), (1, 4)):
        tailctl.keep(reshard._token_path(g, i), {"token": at})
    fence = reshard._Fence(g)
    assert fence.fence_wait("shop", {"s1": 10, "s2": 4}, timeout=1) is True
    # one source's follow is behind where its source stood: not drained
    assert fence.fence_wait("shop", {"s1": 10, "s2": 5},
                            timeout=0.2) is False
    readers[1] = _Reader(lambda have, want: None)
    assert fence.fence_wait("shop", {"s1": 10, "s2": 4}, timeout=1) is None
    # no follow runs: there is no position to drain to
    assert fence.src_lsn("shop") is None
    leg = reshard.leg(g, lambda m: None)
    assert leg.kind == "none"


# --- PostgreSQL: a split by region, and two databases under a tenant key ----------

def _sql(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SPLIT_SEED = """
create table customers (id int primary key, region text not null,
                        name text);
insert into customers select g, case when g % 2 = 0 then 'eu' else 'us' end,
  'c' || g from generate_series(1, 40) g;
"""
SHOP_SEED = """
create table items (id int primary key, name text);
insert into items select g, 'i' || g from generate_series(1, 20) g;
"""


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                            "wal_level=logical"], check=True,
                           capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"], capture_output=True
                                  ).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
        _sql(SRC[0], "create database shop")
        _sql(SRC[0], SPLIT_SEED, "shop")
        for db in ("shop_a", "shop_b"):
            _sql(SRC[0], f"create database {db}")
            _sql(SRC[0], SHOP_SEED, db)
        for db in ("shop_eu", "shop_us", "allshops"):
            _sql(DST[0], f"create database {db}")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


SIDE = "{{host: 127.0.0.1, port: {port}, user: postgres, password: test}}"


def _conf(tmp_path, monkeypatch, text):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    path.write_text(text)
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


def _ids(db, table="customers", where=""):
    got = _sql(DST[0], f"select id from {table} {where} order by id", db)
    return [int(x) for x in got.splitlines() if x]


@needs_docker
def test_a_customer_moving_between_parts_leaves_one_before_the_other(
        pair, tmp_path, monkeypatch):
    part = ("    engine: postgres\n    goal: split\n"
            f"    source: {SIDE.format(port=SRC[1])}\n"
            f"    target: {SIDE.format(port=DST[1])}\n"
            "    databases: [shop]\n")
    _conf(tmp_path, monkeypatch,
          "hops:\n"
          "  eu:\n" + part + "    db_map: {shop: shop_eu}\n"
          "    mapping: {where: {customers: \"region = 'eu'\"}}\n"
          "    options: {follow_until: caught_up, split: {with: [us]}}\n"
          "  us:\n" + part + "    db_map: {shop: shop_us}\n"
          "    mapping: {where: {customers: \"region = 'us'\"}}\n"
          "    options: {follow_until: caught_up, split: {with: [eu]}}\n")
    got, out = _run("move", "eu", "--mode", "full+cdc", "--db", "shop",
                    "--go")
    assert got.exit_code == 0, out
    assert "its group: 1 sources (eu) onto 2 targets (eu, us)" in out, out
    assert _ids("shop_eu") == list(range(2, 41, 2))
    assert _ids("shop_us") == list(range(1, 41, 2))
    # customer 2 moves to the us part, 3 to eu; 41 arrives in eu; 5 goes
    _sql(SRC[0], "update customers set region = 'us' where id = 2;"
                 " update customers set region = 'eu' where id = 3;"
                 " insert into customers values (41, 'eu', 'new');"
                 " delete from customers where id = 5", "shop")
    got, out = _run("move", "us", "--mode", "cdc", "--db", "shop", "--go")
    assert got.exit_code == 0, out
    eu, us = set(_ids("shop_eu")), set(_ids("shop_us"))
    assert not eu & us, eu & us
    assert 2 in us and 2 not in eu and 3 in eu and 3 not in us
    assert 41 in eu and 5 not in us
    assert eu | us == {int(x) for x in _sql(
        SRC[0], "select id from customers", "shop").split()}
    for hop in ("eu", "us"):
        got, out = _run("check", hop, "--db", "shop")
        assert got.exit_code == 0, out
        assert "exactly one part's target" in out, out


@needs_docker
def test_two_databases_followed_into_one_under_their_values(
        pair, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch,
          "hops:\n"
          "  c:\n    engine: postgres\n    goal: consolidate\n"
          f"    source: {SIDE.format(port=SRC[1])}\n"
          f"    target: {SIDE.format(port=DST[1])}\n"
          "    databases: [shop_a, shop_b]\n"
          "    db_map: {shop_a: allshops, shop_b: allshops}\n"
          "    options:\n      follow_until: caught_up\n"
          "      consolidate: {column: tenant_id, values: {shop_a: a,"
          " shop_b: b}, keys: tenant}\n")
    got, out = _run("move", "c", "--mode", "full+cdc", "--db", "shop_a",
                    "--go")
    assert got.exit_code == 0, out
    assert _sql(DST[0], "select tenant_id, count(*) from items group by 1"
                        " order by 1", "allshops") == "a|20\nb|20"
    _sql(SRC[0], "update items set name = 'changed' where id = 1;"
                 " insert into items values (21, 'new');"
                 " delete from items where id = 2", "shop_a")
    _sql(SRC[0], "update items set name = 'other' where id = 1", "shop_b")
    got, out = _run("move", "c", "--mode", "cdc", "--db", "shop_a", "--go")
    assert got.exit_code == 0, out
    assert _sql(DST[0], "select tenant_id, name from items where id = 1"
                        " order by 1", "allshops") == "a|changed\nb|other"
    # a's row 2 removed and its row 21 added under a, b's untouched
    assert _sql(DST[0], "select tenant_id || ':' || id from items where id"
                        " in (2, 21) order by 1", "allshops") == "a:21\nb:2"
    got, out = _run("check", "c", "--db", "shop_a")
    assert got.exit_code == 0, out
