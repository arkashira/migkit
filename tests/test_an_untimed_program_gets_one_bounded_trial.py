"""A program never measured here is held below migkit's own copier by what
the rungs declare (`Rung.external`, `Need.programs`, `Rung.native_copier`),
never by a name or a word, and is owed one bounded trial on a work of many
units (`decide.Trials`) so a way is never left never measured, never run."""
from migkit import decide
from migkit import replicasum as rs
from migkit.decide import Need, Rung


def _prog():
    return Rung("prog", "the bulk program", external=True)


def _copier():
    return Rung("cop", "the copier", native_copier=True)


def test_the_kind_is_declared_never_read_from_a_name_or_words():
    assert not decide._is_native_copier(Rung("builtin", "the table copier"))
    assert not decide._is_native_copier(Rung("x", "migkit's own digest"))
    assert decide._is_native_copier(Rung("x", "anything", native_copier=True))
    assert not decide._is_external_program(Rung("pgloader", "the load"))
    assert decide._is_external_program(Rung("x", "a way", external=True))
    needs = (Need("installed", lambda f: True, programs=("tool",)),)
    assert decide._is_external_program(Rung("x", "a way", needs=needs))
    assert not decide._is_external_program(
        Rung("x", "a way", needs=(Need("plain", lambda f: True),)))


def test_the_own_digest_declares_itself_no_copier():
    digest = rs.RUNGS[1]
    assert digest.native_copier is False
    assert "migkit's own" in digest.said
    assert decide.rank(rs.RUNGS)[0].name == "replica checksum"


def test_an_untimed_program_is_tried_once_on_a_small_unit():
    said = []
    costs = decide.Costs({"cop": 1e-3})
    t = decide.Trials("app.t", [_prog(), _copier()], costs=costs,
                      log=said.append, ahead=[10, 100, 1000])
    got = t.pick({"rows": 10})
    assert got.path == "prog"
    assert got.trial == decide.TRIAL_TIMES * 1e-3 * 10
    assert "never ran here" in got.reason and "boxed in" in got.reason
    assert said and "the bulk program" in said[0]
    # the trial is had once: the next unit goes by the copier
    assert t.pick({"rows": 10}).path == "cop"
    t.took("prog", 10, 0.001)
    # measured faster, it earns its rank by speed and takes the next unit
    nxt = t.pick({"rows": 10})
    assert nxt.path == "prog" and nxt.trial == 0.0


def test_no_trial_on_a_large_unit_or_on_the_critical_path():
    costs = decide.Costs({"cop": 1e-3})
    t = decide.Trials("app.t", [_prog(), _copier()], costs=costs,
                      ahead=[10, 100, 1000])
    got = t.pick({"rows": 1000})
    assert got.path == "cop" and got.trial == 0.0
    c = decide.Trials("app.t", [_prog(), _copier()], costs=costs,
                      critical=True)
    assert c.pick({"rows": 1}).path == "cop"


def test_a_trial_past_its_box_is_given_up_and_said():
    said = []
    t = decide.Trials("app.t", [_prog(), _copier()],
                      costs=decide.Costs({"cop": 1e-3}), log=said.append)
    got = t.pick({"rows": 10})
    assert got.path == "prog"
    t.took("prog", 10, got.trial * 2)
    assert "past the" in t.given_up["prog"]
    assert any("not tried again" in s for s in said)
    for _ in range(3):
        assert t.pick({"rows": 10}).path == "cop"


def test_a_failed_trial_is_said_and_never_tried_again():
    said = []
    t = decide.Trials("app.t", [_prog(), _copier()], log=said.append)
    assert t.pick({"rows": 5}).path == "prog"
    t.give_up("prog", "it exited 1")
    assert any("failed its one trial: it exited 1" in s for s in said)
    assert t.pick({"rows": 5}).path == "cop"


def test_nothing_is_owed_where_no_copier_holds_it_back():
    assert decide.untried([_prog(), Rung("other", "another way")]) == []
    assert [r.name for r in decide.untried([_prog(), _copier()])] == ["prog"]
    timed = decide.Costs({"prog": 1e-4})
    assert decide.untried([_prog(), _copier()], timed) == []
