"""`migkit setup` on a cross-engine hop, for pairs other than one.

The command prints a list of steps for an operator to carry out; it runs
nothing. It refused every combination except MySQL to PostgreSQL:

    SystemExit: hetero postgres->sqlite: the target setup plan is still
                written for mysql->postgres only. `check` works on this
                pair; this does not.

Honest, and a refusal all the same - on a dry run, for pairs migkit can
already convert, move, tail and repair. The steps are read off what the
pairing can actually do now, which `_pair_capabilities` answers from the
classes without opening a connection, so none of this needs a server.

A step that cannot be taken is replaced by the reason rather than left in the
list to fail later, and a pair with no row-shaped path at all gets a short
answer instead of a list of things to try.
"""
import pytest

from migkit.config import Endpoint, Hop


def _plan(src, dst, name="demo", db="shop"):
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    hop = Hop(name=name, engine="hetero", source=ep, target=ep,
              options={"source_engine": src, "target_engine": dst})
    return HeteroEngine(hop).setup_target_plan(db)


def _joined(plan):
    return "\n".join(plan)


def test_a_pair_that_can_do_the_work_gets_the_steps():
    text = _joined(_plan("postgres", "sqlite"))
    assert "postgres -> sqlite" in text
    assert "migkit convert-schema demo --db shop" in text
    assert "migkit move demo --db shop --go" in text
    assert "migkit move demo --mode cdc --go" in text
    assert "migkit check demo --db shop" in text
    # sqlite's database is a file and migkit makes it, so nothing to do here
    assert "migkit makes the sqlite target itself" in text


def test_a_target_that_makes_its_own_objects_is_not_asked_for(mongo=None):
    text = _joined(_plan("postgres", "mongodb"))
    assert "creates the collection on the first write" in text
    assert "create the target database" not in text


def test_a_server_database_is_left_to_the_operator():
    """Encoding, collation and ownership are decisions migkit will not make
    on someone's behalf - the same line `prepare_target` draws."""
    text = _joined(_plan("sqlite", "postgres"))
    assert "create the target database on postgres yourself" in text
    assert "encoding and collation" in text


def test_a_pair_that_cannot_tail_says_what_to_do_instead():
    text = _joined(_plan("sqlite", "postgres"))
    assert "--mode cdc" not in text, text
    assert "cutover with writes stopped" in text


def test_a_pair_with_no_row_shaped_path_says_so_rather_than_listing_steps():
    for pair in (("mysql", "redis"), ("postgres", "kafka")):
        plan = _plan(*pair)
        text = _joined(plan)
        assert "no table-shaped path" in text, text
        assert "migkit move" not in text, text
        assert "convert-schema" not in text, text
        assert "migkit assess demo" in text, text
        assert len(plan) <= 3, plan


def test_the_mysql_to_postgres_plan_still_reads_as_it_did():
    text = _joined(_plan("mysql", "postgres"))
    assert "migkit convert-schema demo --db shop" in text
    assert "the target's tables from the source's" in text
    # migkit's own steps, not the programs it wraps
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    assert not [t for t in TOOLS + ("sqlglot",) if t in text.lower()], text
    assert "resumable chunked data copy" in text
    assert "migkit move demo --mode cdc --go" in text


def test_the_plan_names_the_hop_it_was_asked_about():
    """It used to print the literal `<hop>`, which is not a command anyone
    can run."""
    text = _joined(_plan("postgres", "sqlite", name="cart-to-file"))
    assert "<hop>" not in text, text
    assert "migkit move cart-to-file --db shop --go" in text


@pytest.mark.parametrize("pair", [
    ("postgres", "sqlite"), ("postgres", "mongodb"), ("sqlite", "postgres"),
    ("mongodb", "mysql"), ("mysql", "redis"), ("postgres", "kafka"),
])
def test_no_step_promises_something_the_pair_cannot_do(pair):
    """The plan and the capability rows are two descriptions of one pair, so
    they are checked against each other rather than against a list written
    here."""
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    hop = Hop(name="demo", engine="hetero", source=ep, target=ep,
              options={"source_engine": pair[0], "target_engine": pair[1]})
    engine = HeteroEngine(hop)
    can = {row["item"]: row["level"] == "pass"
           for row in engine._pair_capabilities()}
    text = _joined(engine.setup_target_plan("shop"))

    suggests_move = "migkit move demo --db shop --go" in text
    suggests_cdc = "--mode cdc" in text
    suggests_ddl = "convert-schema" in text
    assert suggests_move == can["this pair can move rows"], (pair, text)
    assert suggests_cdc == can["this pair can tail changes"], (pair, text)
    if not can["this pair can move rows"]:
        assert not suggests_ddl, (pair, text)
