"""Every target in the sandbox holds a mover's number the strongest way it
has, and a stopped mover's lower number is refused by the target itself
(backlog W1).

On each lane: the way and its placement are rungs the decision engine
climbs on the facts the engine read of the target (named here by flipping
one: an account that may not make migkit's record in the target's own
database places it beside, and one that may make it nowhere is refused
with the statement to grant); `fencing.prove` passes; a holder armed under
a number writes, a taker raises the number, and the old holder's next
write is refused - by the target's own compare (a find-and-modify, a
script, a lightweight transaction, the broker's producer epoch, a row
locked for update) or, on ClickHouse, by the read of the highest number
the target keeps, which is said as the weaker hold. A bulk write through
the engine's own bulk path after the raise fails at the target and leaves
nothing (PostgreSQL in the transaction, MongoDB before the batch).

Without a server: the rungs on facts alone, and the fleet audit telling a
range lost from a table lost through the ledger's units.
"""
import pytest

from migkit import claims, decide, engines, fencing, fleet
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("operations.fencing", ("mongodb", "redis", "cassandra",
                                  "clickhouse", "oracle"))]

mongo = servers.fixture("mongo")
redis_ = servers.fixture("redis")
kafka = servers.fixture("kafka")
cassandra = servers.fixture("cassandra")
clickhouse = servers.fixture("clickhouse")
oracle = servers.fixture("oracle")
pg = servers.fixture("pg")


def _eng(space, engine):
    try:
        src, dst = space.src, space.dst
    except AttributeError:          # a profile of one server (Oracle's)
        src = dst = space.one
    hop = Hop(name=f"w18a-{engine}", engine=engine,
              source=src.endpoint(), target=dst.endpoint(),
              databases=[space.name])
    return engines.get_engine(hop)


def _taken_over(eng, db, way, place="in", refuses=True):
    """The way chosen and placed, proved, and an old holder's write refused
    once a taker raised the number."""
    if way:
        got = fencing.way(eng, "dst", db)
        assert isinstance(got, fencing.Way), got
        assert (got.choice.path, got.place) == (way, place)
        assert got.target_refuses is refuses
        assert got.choice.asked                 # the facts it read are kept
    assert fencing.prove(eng, "dst", db) is None
    row = f"{eng.hop.name}#t"
    old = fencing.Token(row, 7, "old-mover", "store")
    new = fencing.Token(row, 8, "taker", "store")
    assert fencing.raise_once(eng, "dst", db, old) == 7
    fencing.arm(eng.hop, old)
    try:
        fencing.hold(eng, "dst", db, None)      # the holder writes
        assert fencing.raise_once(eng, "dst", db, new) == 8
        with pytest.raises(fencing.Fenced, match="refused a write"):
            fencing.hold(eng, "dst", db, None)
        # a lower number raised again does not take the row back
        assert fencing.raise_to(eng, "dst", db, old)[0] == 8
    finally:
        fencing.disarm(eng.hop)
    assert fencing.read(eng, "dst", db, row)[0] == 8
    return old


@needs_docker
def test_mongodb_holds_it_in_a_guarded_document_and_its_bulk_path_is_refused(
        mongo):
    eng = _eng(mongo, "mongodb")
    old = _taken_over(eng, mongo.name, "guarded-document")
    fencing.arm(eng.hop, old)
    try:
        with pytest.raises(fencing.Fenced, match="refused a write"):
            eng.neutral_write("dst", mongo.name, "w18a_t",
                              [("_id", "text"), ("v", "text")],
                              [["a", "1"], ["b", "2"]])
    finally:
        fencing.disarm(eng.hop)
    assert mongo.dst.client()[mongo.name]["w18a_t"].count_documents({}) == 0


@needs_docker
def test_mongodb_places_it_beside_or_refuses_by_the_accounts_rights(mongo):
    eng = _eng(mongo, "mongodb")
    real = eng.fence_facts
    eng.fence_facts = lambda s, d: {**real(s, d), "kept_in": False,
                                    "create_in": False}
    _taken_over(eng, mongo.name, "guarded-document", place="beside")
    assert fencing.OWN in mongo.dst.client()[fencing.BESIDE] \
        .list_collection_names()
    eng = _eng(mongo, "mongodb")
    eng.fence_facts = lambda s, d: {**real(s, d), "kept_in": False,
                                    "create_in": False,
                                    "create_beside": False}
    why = fencing.prove(eng, "dst", mongo.name)
    assert "no place for migkit's own record" in why
    assert f"on database {mongo.name}" in why     # the statement to grant


@needs_docker
def test_redis_holds_it_by_a_script_that_compares_and_sets(redis_):
    eng = _eng(redis_, "redis")
    _taken_over(eng, "0", "compared-key")
    real = eng.fence_facts
    eng = _eng(redis_, "redis")
    eng.fence_facts = lambda s, d: {**real(s, d), "scripts": False}
    assert "may not run scripts" in fencing.prove(eng, "dst", "0")


@needs_docker
def test_cassandra_holds_it_by_a_lightweight_transaction(cassandra):
    eng = _eng(cassandra, "cassandra")
    _taken_over(eng, cassandra.name, "lightweight-transaction")
    real = eng.fence_facts
    eng = _eng(cassandra, "cassandra")
    eng.fence_facts = lambda s, d: {**real(s, d), "kept_in": False,
                                    "create_in": False}
    _taken_over(eng, cassandra.name, "lightweight-transaction",
                place="beside")


@needs_docker
def test_kafka_is_refused_where_the_broker_takes_no_transactional_producer(
        kafka):
    """The sandbox's broker keeps its transaction log at more replicas than
    it has: a transactional producer is never started, so the fact read is
    False and the standby is refused with what to allow - the epoch fence
    itself is built and declared unproved (`engines.kafka`)."""
    eng = _eng(kafka, "kafka")
    facts = eng.fence_facts("dst", eng.CLUSTER)
    assert facts["create_in"] and facts["transactions"] is False
    why = fencing.prove(eng, "dst", eng.CLUSTER)
    assert "takes no transactional producer" in why


@needs_docker
def test_clickhouse_keeps_the_highest_number_and_says_the_hold_is_weaker(
        clickhouse):
    eng = _eng(clickhouse, "clickhouse")
    _taken_over(eng, clickhouse.name, "replacing-row", refuses=False)
    got = fencing.way(eng, "dst", clickhouse.name)
    assert "weaker" in got.choice.said


@needs_docker
def test_oracle_holds_it_in_its_own_table_under_a_row_lock(oracle):
    eng = _eng(oracle, "oracle")
    _taken_over(eng, oracle.name, None)


@needs_docker
def test_a_bulk_write_on_pg_wire_after_the_raise_fails_in_its_transaction(pg):
    eng = _eng(pg, "postgres")
    pg.dst.sql("create table if not exists public.w18a_t"
               " (id int primary key, v text)", pg.name)
    old = _taken_over(eng, pg.name, None)
    cols = eng.neutral_columns("dst", pg.name, "public.w18a_t")
    fencing.arm(eng.hop, old)
    try:
        with pytest.raises(fencing.Fenced, match="refused a write"):
            eng.neutral_write("dst", pg.name, "public.w18a_t", cols,
                              [[1, "a"], [2, "b"]])
    finally:
        fencing.disarm(eng.hop)
    assert pg.dst.sql("select count(*) from public.w18a_t",
                      pg.name)[0][0] == 0


def test_the_way_is_a_rung_on_what_the_target_speaks_and_allows():
    assert decide.fence_way({"ways": ["compared-key"],
                             "scripts": True}).path == "compared-key"
    no = decide.fence_way({"ways": ["compared-key"], "scripts": False})
    assert no.rung is None and "may not run scripts" in no.reason
    assert decide.fence_way({"ways": ["producer-epoch"]}).rung is None
    assert decide.fence_way({"ways": []}).rung is None     # an object store
    assert decide.fence_place({"create_in": True}).path == "in"
    assert decide.fence_place({"kept_beside": True}).path == "beside"
    none = decide.fence_place({"grant_in": "grant create on keyspace k"})
    assert none.rung is None and "grant create on keyspace k" in none.reason


def test_a_target_with_no_way_refuses_the_standby_with_its_reason():
    class Lake:
        class hop:
            name = "w18a-lake"
    assert "keeps no table of migkit's" in fencing.prove(Lake(), "dst", "x")


def test_the_audit_names_a_range_lost_not_the_table():
    rec = {"key": "orders", "ranges": [[0, 100], [100, 200], [200, 300]],
           "numbers": 3,
           "state": {str(a): {"state": claims.DONE, "number": i + 1,
                              "by": "m1"}
                     for i, a in enumerate((0, 100, 200))}}
    # the record alone says the table is whole
    assert fleet.audit(rec) == []
    named = [{"table": "orders", "kind": "range",
              "unit": fleet.unit_name("orders", a)} for a in (0, 200)]
    got = fleet.audit(rec, named)
    assert len(got) == 1 and got[0].startswith("orders r:100:")
    assert "this range was lost, not the table" in got[0]
