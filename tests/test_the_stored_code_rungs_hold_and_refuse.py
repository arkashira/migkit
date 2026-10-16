"""The rungs a routine's conversion and a column's collation on the
target are chosen by, each reached on its branches on fakes (backlog
AUD-Z.rungs-outside): a converter is held only for the pair and kind it
converts, and refused by any need of its own; a collation the target's
server must make is held where it makes it and refused, with the
server's words, where it cannot - one it already has needs nothing."""
import types

from migkit import decide
from migkit.storedcode import collations
from migkit.storedcode.rungs import Candidate
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("storedcode.rungs", "Candidate", "<name>", "holds"),
    ("storedcode.rungs", "Candidate", "<name>", "refuses"),
    ("storedcode.collations", "_climb", "<c.sig>", "holds"),
    ("storedcode.collations", "_climb", "<c.sig>", "refuses"),
]

PROC = {"src": "oracle", "dst": "postgres", "kind": "procedure"}


def test_a_converter_holds_for_the_pair_and_kind_it_converts():
    c = Candidate("rules", "migkit's rules",
                  gives={"oracle->postgres:procedure"})
    with watched() as seen:
        assert c.lacks(PROC) is None
        got = c.lacks(dict(PROC, kind="trigger"))
    assert branch(seen, "rules") == [HOLDS, REFUSES]
    assert got == "migkit's rules does not convert a trigger from oracle" \
                  " to postgres" == why(seen, "rules")


def test_a_converters_own_need_refuses_it_after_the_pair():
    c = Candidate("emulate", "the target's emulation",
                  gives={"oracle->postgres:procedure"},
                  needs=(decide.Need("the emulation installed",
                                     lambda f: f["emulation"],
                                     "the emulation is not installed"),))
    with watched() as seen:
        assert c.lacks(dict(PROC, emulation=True)) is None
        got = c.lacks(dict(PROC, emulation=False))
    assert branch(seen, "emulate") == [HOLDS, REFUSES]
    assert got == "the emulation is not installed" == why(seen, "emulate")


class _Server:
    """The target's side: two collations it has, one it can make, one
    it cannot."""

    def __init__(self):
        mk = types.SimpleNamespace
        self.cands = [
            (mk(sig="made-icu", name="made_icu", said="made_icu",
                body="CREATE COLLATION made_icu"), 1, "made to agree"),
            (mk(sig="broken", name="broken", said="broken",
                body="CREATE COLLATION broken"), 1, "made to agree"),
            (mk(sig="builtin", name="C", said="C", body=None), 0,
             "the server has it"),
        ]

    def candidates(self, di, own):
        return self.cands

    def make(self, c):
        return "no ICU provider" if c.sig == "broken" else None


def test_a_collation_the_server_cannot_make_is_refused_with_its_words(
        monkeypatch):
    asked = []
    monkeypatch.setattr(collations, "_ask",
                        lambda b, di, c, lay: asked.append(c.sig) or [c.sig])
    monkeypatch.setattr(collations, "compare", lambda src, rd, lay:
                        types.SimpleNamespace(count=0, classes=(),
                                              kinds=()))
    monkeypatch.setattr(collations, "present", lambda rds, lay: set())
    with watched() as seen:
        got, by = collations._climb(_Server(), "di", "own", "lay", "src",
                                    set(), {}, "app.t.name: collation",
                                    False)
    assert branch(seen, "made-icu") == [HOLDS]
    assert branch(seen, "builtin") == [HOLDS]
    assert branch(seen, "broken") == [REFUSES]
    assert why(seen, "broken") == ("the target's server cannot make broken:"
                                   " no ICU provider")
    # what changes least is asked first, and the one refused never is
    assert got.rung.name == "builtin" and asked == ["builtin"]
    assert set(by) == {"made-icu", "broken", "builtin"}


def test_a_collation_that_parts_from_the_source_falls_to_the_next(
        monkeypatch):
    # a reading is the rows read back; a string is why none was
    monkeypatch.setattr(collations, "_ask", lambda b, di, c, lay: [c.sig])
    monkeypatch.setattr(collations, "compare", lambda src, rd, lay:
                        types.SimpleNamespace(
                            count=2 if rd == ["builtin"] else 0,
                            classes=(), kinds=("case",)))
    monkeypatch.setattr(collations, "_said", lambda kinds: "case")
    monkeypatch.setattr(collations, "present", lambda rds, lay: set())
    with watched() as seen:
        got, _ = collations._climb(_Server(), "di", "own", "lay", "src",
                                   set(), {}, "app.t.name: collation",
                                   False)
    assert got.rung.name == "made-icu"
    assert "compare or sort differently under it" in got.reason
    assert branch(seen, "broken") == [REFUSES]
