"""What an engine still lacks is read off the code (`decide.still_lacking`): the
cells of the decision shapes, the capability probes and the depth matrix,
and the state sets a tail makes are found by one protocol
(`capabilities._state_sets`), so a Redis tail's key spaces count as a
tail kept in states of their own.
"""
from migkit import capabilities, decide
from migkit.engines import NAMES
from migkit.engines.redis import KeyspaceStates
from migkit.tailtables import TableSet
from migkit.topicstates import TopicSet


def test_the_state_sets_a_tail_makes_are_found_by_one_protocol():
    for cls in (TableSet, TopicSet, KeyspaceStates):
        assert capabilities._state_set(cls), cls.__name__
    assert not capabilities._state_set(dict)
    assert not capabilities._state_set(decide.Rung)
    assert not capabilities._state_set("TableSet")


def test_each_tail_names_the_state_set_it_makes():
    from migkit.engines import _class_for
    assert KeyspaceStates in capabilities._state_sets(_class_for("redis"))
    assert TableSet in capabilities._state_sets(_class_for("mssql"))
    assert TopicSet in capabilities._state_sets(_class_for("kafka"))
    assert capabilities._state_sets(_class_for("sqlite")) == set()


def test_a_redis_tail_keeps_its_key_spaces_in_states_of_their_own():
    assert capabilities.depth_has("redis", "tail.table-states")
    assert not capabilities.depth_has("sqlite", "tail.table-states")


def test_every_engine_has_a_list_and_a_gap_is_in_it_with_its_row():
    got = decide.still_lacking()
    assert set(got) == set(NAMES)
    rows = capabilities.backlog_rows()
    waiting = [r for lack in got.values() for r in lack
               if r[2] == decide.NOT_YET]
    assert waiting, "no engine lacks anything: the list reads nothing"
    for table, name, state, why in waiting:
        assert why in rows, (table, name, why)


def test_a_cell_the_engine_answers_is_not_listed_and_a_gap_is(monkeypatch):
    assert not any(r[0] == "shape" and r[1] == "keyless"
                   for r in decide.still_lacking("postgres")["postgres"])
    monkeypatch.setitem(decide.WAYS["keyless"], "postgres",
                        (decide.NOT_YET, "P0-PARITY.all"))
    assert ("shape", "keyless", decide.NOT_YET, "P0-PARITY.all") in \
        decide.still_lacking("postgres")["postgres"]


def test_a_not_applicable_cell_is_no_lack(monkeypatch):
    monkeypatch.setitem(decide.WAYS["keyless"], "postgres",
                        (decide.NOT_APPLICABLE, "no keys here"))
    assert not any(r[0] == "shape" and r[1] == "keyless"
                   for r in decide.still_lacking("postgres")["postgres"])


def test_a_way_declared_for_no_engine_or_no_shape_is_found(monkeypatch):
    assert decide.orphan_ways() == []
    monkeypatch.setitem(decide.WAYS["keyless"], "nosuchengine",
                        ("yes", "@nothing"))
    monkeypatch.setitem(decide.WAYS, "no such shape",
                        {"postgres": ("yes", "@nothing")})
    assert set(decide.orphan_ways()) == {("keyless", "nosuchengine"),
                                         ("no such shape", "postgres")}
