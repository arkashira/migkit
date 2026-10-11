"""On Redis: a hop that carries no deletes is checked and repaired as what it
is, and is not handed to the server's own replication, which applies every
change of the source as it is (P0-PARITY.scope). A database's keys are the
one table a Redis has, `db0`, and a rule names it so. Run against a real
server, started here."""
from migkit import cli, verdict
from migkit.engines.base import LEFT_OUT
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.ops", "redis")]

pytestmark = needs_server

NO_DELETES = {"ops": {"db0": "insert, update"}}


def _seed(pair):
    src, dst = (p.client() for p in pair)
    for k in "abc":
        src.set(k, k)
        dst.set(k, k)
    src.delete("a")                  # the source deleted it: the target keeps
    src.set("new", "n")              # the source inserted it
    dst.set("x", "x")                # and the target's own


def test_the_check_calls_the_kept_keys_kept(clean, tmp_path):
    _seed(clean)
    eng = engine(clean, tmp_path, mapping=NO_DELETES,
                 options={"deep": True})
    rows = [r.__dict__ for r in eng.check_data("0")]
    got = verdict.decide(rows, eng.hop)
    # `new` is missing on the target and inserts are carried: a difference
    assert got.status != "same", rows
    extra = [r for r in rows if r["scope"] == "db0 extra keys"][0]
    assert extra["kept_by_hop"] == ("kept by the hop: 2 rows the source"
                                    " deleted - deletes are not carried")


def test_the_repair_leaves_the_kept_keys_and_carries_the_missing_one(
        clean, tmp_path):
    _seed(clean)
    src, dst = (p.client() for p in clean)
    eng = engine(clean, tmp_path, mapping=NO_DELETES,
                 options={"deep": True})
    eng.check_data("0")
    plan = eng.repair_plan("0", "rows")
    assert [a.kind for a in plan] == [LEFT_OUT, "rows"], plan
    assert "2 rows the source deleted left as they are" in plan[0].note
    for action in plan:
        if action.kind != LEFT_OUT:
            eng.apply("0", action)
    assert dst.get("new") == "n"
    assert dst.get("a") == "a" and dst.get("x") == "x"


def test_a_hop_that_carries_every_change_repairs_as_before(clean, tmp_path):
    _seed(clean)
    dst = clean[1].client()
    eng = engine(clean, tmp_path, options={"deep": True})
    eng.check_data("0")
    for action in eng.repair_plan("0", "rows"):
        eng.apply("0", action)
    assert dst.get("a") is None and dst.get("x") is None
    assert dst.get("new") == "n"


def test_the_servers_own_replication_is_not_used_for_it(clean, tmp_path):
    eng = engine(clean, tmp_path, mapping=NO_DELETES)
    said = cli._own_refused(eng.hop, eng, "0")
    assert said == "", said          # it lists no tables to hold the hop to
    why = eng.native_replica_unsafe()
    assert why and "changes carried" in why, why
    assert engine(clean, tmp_path).native_replica_unsafe() is None
