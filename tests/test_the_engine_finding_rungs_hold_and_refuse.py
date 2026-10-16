"""The rungs the engine at an address is known by - each family's own
knock, each brand its version text names after sign-in, the operator's
answer, and the family an address's scheme already holds - reached under
`watched()` on both their branches on fakes (backlog
AUD-Z.rungs-outside-owed): nothing that does not answer is taken for an
engine, a brand stands only on a sign-in, and the operator is asked only
at a terminal."""
from migkit import engineat
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("engineat", "_ladder", "<f'{family}/{name}'>", "holds"),
    ("engineat", "_ladder", "<f'{family}/{name}'>", "refuses"),
    ("engineat", "identify", "<k.family>", "holds"),
    ("engineat", "identify", "<k.family>", "refuses"),
    ("engineat", "identify", "asked", "holds"),
    ("engineat", "identify", "asked", "refuses"),
    ("engineat", "rebrand", "<family>", "holds"),
]


def _silent(monkeypatch, reached):
    monkeypatch.setattr(engineat, "_reachable", lambda *a, **k: reached)

    def nothing(*a, **k):
        raise OSError("connection reset")
    monkeypatch.setattr(engineat, "_exchange", nothing)


def test_nothing_that_does_not_answer_is_taken_for_an_engine(monkeypatch):
    _silent(monkeypatch, reached=False)
    family = engineat._ordered(3306)[0].family
    with watched() as seen:
        choice, heard = engineat.identify("10.0.0.7", 3306, can_ask=False)
    assert choice.rung is None and heard is None
    assert branch(seen, family) == [REFUSES]
    assert why(seen, family) == "nothing answers at 10.0.0.7:3306"
    assert branch(seen, "asked") == [REFUSES]
    assert "a terminal to ask at" in why(seen, "asked")


def test_a_server_reached_that_answers_no_knock_is_asked_of_the_operator(
        monkeypatch):
    _silent(monkeypatch, reached=True)
    family = engineat._ordered(3306)[0].family
    brand = f"{family}/{engineat._brands(family)[0]}" \
        if engineat._brands(family) else None
    with watched() as seen:
        choice, heard = engineat.identify("10.0.0.7", 3306, can_ask=True)
    assert choice.path == "asked" and heard is None
    # the knock held, then its proof (the exchange) failed: it is not the
    # need that refused it
    assert branch(seen, family) == [HOLDS]
    assert branch(seen, "asked") == [HOLDS]
    if brand:
        assert branch(seen, brand) == [REFUSES]
        assert "nobody has signed in yet" in why(seen, brand)


def test_a_brand_stands_only_on_a_sign_in():
    family = next(f for f in ("mysql", "postgres") if engineat._brands(f))
    name = engineat._brands(family)[0]
    rung = f"{family}/{name}"
    heard = {}
    with watched() as seen:
        from migkit import decide
        ladder = engineat._ladder(family, lambda f: None, lambda: "refused",
                                  heard)
        kept, _ = decide.prune(ladder, {"sign_in": True})
        assert rung in [r.name for r in kept]
        kept, out = decide.prune(ladder, {"sign_in": False})
    assert not kept and rung in [r.name for r, _ in out]
    assert branch(seen, rung) == [HOLDS, REFUSES]
    assert "the family it greets as stands" in why(seen, rung)
    # a sign-in that was refused proves no brand
    proved = next(r for r in ladder if r.name == rung).prove({})
    assert "signing in to read its version failed: refused" == proved


def test_an_address_whose_sign_in_names_no_brand_keeps_its_family(
        monkeypatch):
    family = next(f for f in ("mysql", "postgres") if engineat._brands(f))
    monkeypatch.setattr(engineat, "_reachable", lambda *a, **k: True)
    monkeypatch.setattr(engineat, "_said_after_sign_in",
                        lambda *a: "access denied")
    said = []
    text = f"{family}://app@10.0.0.7:4000/app"
    with watched() as seen:
        assert engineat.rebrand(text, "CHANGE_ME", said.append) == text
    assert branch(seen, family) == [HOLDS]
    assert said and "access denied" in said[0]
    assert f"kept as the {family} it greeted as" in said[0]
