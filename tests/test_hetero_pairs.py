"""A cross-engine hop whose pair is configuration rather than code.

The matching rules are decided before any server is touched, so they are
tested here without one. The pairing that used to be impossible - PostgreSQL
as the source and MySQL as the target - is measured against containers in
`test_hetero_reverse.py`.
"""
import pytest

from migkit.engines.hetero import HeteroEngine


def _hop(src, dst):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    return Hop(name="h", engine="hetero", source=ep, target=ep,
               options={"source_engine": src, "target_engine": dst})


def test_a_pair_that_used_to_be_refused_now_builds():
    """`hetero postgres->mysql` exited with "not built yet". Nothing about
    the comparison was ever specific to the direction."""
    eng = HeteroEngine(_hop("postgres", "mysql"))
    assert eng.src_engine.CANON_ENGINE == "postgres"
    assert eng.dst_engine.CANON_ENGINE == "mysql"
    assert eng._can_compare_neutrally()


def test_the_original_pair_still_resolves_to_the_written_paths():
    """mysql->postgres keeps the mover, the DDL conversion and the binlog
    tail; those are still written for it alone."""
    eng = HeteroEngine(_hop("mysql", "postgres"))
    assert eng.my is eng.src_engine and eng.pg is eng.dst_engine


def test_an_alias_resolves_to_the_engine_behind_it():
    eng = HeteroEngine(_hop("mariadb", "aurora-postgres"))
    assert eng.src_engine.CANON_ENGINE == "mysql"
    assert eng.dst_engine.CANON_ENGINE == "postgres"


def test_a_pair_with_no_rendering_is_refused_rather_than_compared():
    """Redis has no canonical rendering, so there is no text to hash on that
    side. Reporting the two sides as equal would be inventing the answer."""
    eng = HeteroEngine(_hop("mysql", "redis"))
    assert not eng._can_compare_neutrally()
    out = eng.check_counts("db")
    assert out[0].status == "error"
    assert "no canonical rendering" in out[0].detail


def test_hetero_cannot_be_one_of_its_own_sides():
    for pair in (("hetero", "postgres"), ("mysql", "hetero")):
        with pytest.raises(SystemExit):
            HeteroEngine(_hop(*pair))


def test_the_pair_specific_paths_say_which_pair_they_are_for():
    """An AttributeError on `self.my` would read as a migkit bug. This reads
    as the sentence it is. Only the paths that are still written for one pair
    refuse - converting DDL and the target setup plan. Moving rows crosses
    now, so it is no longer in this list."""
    eng = HeteroEngine(_hop("postgres", "mysql"))
    for call in (lambda: eng.convert_ddl("db"),
                 lambda: eng.setup_target_plan("db")):
        with pytest.raises(SystemExit) as e:
            call()
        assert "mysql->postgres only" in str(e.value)
        assert "postgres->mysql" in str(e.value)


def test_moving_and_comparing_are_answered_per_pair_without_connecting():
    """Both capability questions are answered from the classes. A probe that
    opened a connection would turn "can this pair move" into "is the database
    up right now" - two questions with different answers and different
    things to tell the operator."""
    cases = {("postgres", "mysql"): (True, True),
             ("mysql", "postgres"): (True, True),
             # sqlite and mongodb compare but have no writer yet
             ("sqlite", "postgres"): (False, True),
             ("mongodb", "postgres"): (False, True),
             ("mysql", "redis"): (False, False)}
    for pair, (can_move, can_compare) in cases.items():
        eng = HeteroEngine(_hop(*pair))
        assert eng._can_move_neutrally() is can_move, pair
        assert eng._can_compare_neutrally() is can_compare, pair


def test_a_pair_that_cannot_move_still_says_which_pair_it_is():
    eng = HeteroEngine(_hop("mongodb", "postgres"))
    with pytest.raises(SystemExit) as e:
        eng.move_table("db", "", "t", 1, {}, print)
    assert "mongodb->postgres" in str(e.value)


def test_tables_are_matched_on_the_name_without_its_qualifier():
    """MySQL answers `orders`; PostgreSQL answers `public.orders`."""
    pairs, s_only, d_only, amb = HeteroEngine.match_tables(
        ["orders", "items", "gone"],
        ["public.orders", "public.items", "public.extra"])
    assert pairs == [("items", "public.items"), ("orders", "public.orders")]
    assert s_only == ["gone"]
    assert d_only == ["public.extra"]
    assert amb == []


def test_a_name_that_appears_twice_on_one_side_is_not_guessed_at():
    """The same table name in two schemas. Choosing one would compare a table
    against a namesake and report the verdict as if it were about the one the
    operator meant."""
    pairs, s_only, d_only, amb = HeteroEngine.match_tables(
        ["orders"], ["public.orders", "archive.orders"])
    assert pairs == []
    assert amb and "orders" in amb[0]
    assert s_only == [] and d_only == []


def test_matching_is_stable_whatever_order_the_servers_answer_in():
    a = HeteroEngine.match_tables(["b", "a"], ["public.a", "public.b"])
    b = HeteroEngine.match_tables(["a", "b"], ["public.b", "public.a"])
    assert a == b
