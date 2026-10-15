import inspect

from migkit.engines import pg_pass
from migkit.engines.base import Engine
from migkit.engines.postgres import PostgresEngine


def test_the_cutover_hold_is_read_in_one_place():
    assert "_held_for_cutover" not in PostgresEngine.__dict__
    assert PostgresEngine._held_for_cutover is Engine._held_for_cutover


def test_a_range_held_different_is_given_up_through_the_same_attempt():
    assert not hasattr(pg_pass.Way, "instead")
    src = inspect.getsource(PostgresEngine)
    assert ".instead(" not in src
    assert src.count("give_up_passing(") == 1
    assert 'raise decide.Blocked(f"it was held different: {why}")' in src
