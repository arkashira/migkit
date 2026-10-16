"""The migrate command's steps say the pair they move between (backlog
P0-DECISION-ALL.engines): `autonomous.pair` reads (source engine, target
engine) off the hop, never guessed - a side the hop does not name is None
and its gaps are not said as though it were known - the flow keeps the pair
as a decision of its record and says it to the operator, and the code map
writes the per-pair inventory (`autonomous.gaps`) into STATE.md for every
pair the registry carries, the counts as `autonomous.gaps` gives them."""
import re
import sys
from pathlib import Path
from types import SimpleNamespace

from migkit import autonomous
from tests.test_every_phase_declares_its_contract import Fake, _run

ROOT = Path(__file__).resolve().parent.parent


def _hop(engine, **options):
    return SimpleNamespace(engine=engine, options=options)


def test_a_same_engine_hop_is_its_engine_on_both_sides():
    assert autonomous.pair(_hop("postgres"), SimpleNamespace()) == (
        "postgres", "postgres")
    # an alias is the engine it stands for
    assert autonomous.pair(_hop("mariadb"), None) == ("mysql", "mysql")


def test_a_hop_between_two_engines_is_read_off_what_it_names():
    eng = SimpleNamespace(src_name="mysql", dst_name="postgres",
                          src_engine=object(), dst_engine=object())
    assert autonomous.pair(_hop("hetero"), eng) == ("mysql", "postgres")
    # read off the hop's options where its engine keeps no names
    hop = _hop("hetero", source_engine="percona", target_engine="mssql")
    assert autonomous.pair(hop, None) == ("mysql", "mssql")
    # inner engines, each with a hop of its own
    eng = SimpleNamespace(src_engine=SimpleNamespace(hop=_hop("oracle")),
                          dst_engine=SimpleNamespace(hop=_hop("postgres")))
    assert autonomous.pair(_hop("hetero"), eng) == ("oracle", "postgres")


def test_a_side_the_hop_does_not_name_is_not_guessed():
    # an inner engine whose hop says no engine, and a pair hop naming none
    eng = SimpleNamespace(src_engine=SimpleNamespace(hop=None),
                          dst_engine=SimpleNamespace(hop=_hop("postgres")))
    assert autonomous.pair(_hop("hetero"), eng) == (None, "postgres")
    assert autonomous.pair(_hop("hetero"), None) == (None, None)


def test_the_flow_says_the_pair_and_keeps_it_in_its_record(tmp_path):
    ops = Fake(("mysql", "postgres"), dbs=("a",))
    got = _run("copy", ops, tmp_path / "wf.json")
    lines = autonomous.gap_lines("mysql", "postgres")
    said = [x for x in ops.log if isinstance(x, str)
            and x.startswith("the pair mysql -> postgres")]
    assert said == [f"the pair mysql -> postgres: {len(lines)} things it"
                    " lacks said before any step"]
    first = min(i for i, x in enumerate(ops.log) if isinstance(x, tuple))
    assert ops.log.index(said[0]) < first
    rec = next(d for d in got.workflow.decisions if d["what"] == "pair")
    assert (rec["source"], rec["target"], rec["way"]) == (
        "mysql", "postgres", "mysql->postgres")
    gaps = next(d for d in got.workflow.decisions if d["what"] == "gaps")
    assert gaps["pair"] == ["mysql", "postgres"]
    # the record on disk keeps it, read back as the evidence
    kept = [d for d in autonomous.evidence(tmp_path / "wf.json")
            if d["what"] == "pair"]
    assert kept and kept[0]["source"] == "mysql"


def test_a_pair_not_known_says_so_and_says_no_gaps(tmp_path):
    ops = Fake((None, "postgres"), dbs=("a",))
    got = _run("copy", ops, tmp_path / "wf.json")
    assert not any(isinstance(x, str) and " lacks the " in x
                   for x in ops.log)
    assert ("the pair is not known from the hop (? -> postgres): what it"
            " lacks cannot be said") in ops.log
    rec = next(d for d in got.workflow.decisions if d["what"] == "pair")
    assert rec["source"] is None and rec["way"] == "?->postgres"
    assert not any(d["what"] == "gaps" for d in got.workflow.decisions)


def _gen():
    sys.path.insert(0, str(ROOT / "tools"))
    import gen_codemap
    return gen_codemap


def test_the_inventory_counts_what_each_engine_of_a_pair_lacks():
    gen = _gen()
    lacking = {"a": [("shape", "s", "", ""), ("depth", "d", "", ""),
                     ("depth", "e", "", "")],
               "b": [("capability", "c", "", "")]}
    real = gen._registry_pairs
    gen._registry_pairs = lambda: [("a", "a"), ("a", "b"), ("b", "a")]
    try:
        lines = gen._pair_inventory(lacking)
    finally:
        gen._registry_pairs = real
    rows = [x for x in lines if x.startswith("| a ") or x.startswith("| b ")]
    # an engine to itself once; a pair both engines' lacks
    assert rows == ["| a -> a | 1 | 0 | 2 | 3 |", "| a -> b | 1 | 1 | 2 | 4 |",
                    "| b -> a | 1 | 1 | 2 | 4 |"]
    assert "11 lacks in all" in " ".join(lines)


def test_the_state_says_each_registry_pair_s_gaps_as_the_flow_does():
    gen = _gen()
    state = (ROOT / "docs" / "agents" / "STATE.md").read_text()
    body = state.split("## Gaps by pair", 1)[1].split("\n## ", 1)[0]
    rows = dict(re.findall(r"^\| (\S+ -> \S+) \| .* \| (\d+) \|$", body,
                           re.M))
    pairs = gen._registry_pairs()
    assert sorted(rows) == sorted(f"{s} -> {d}" for s, d in pairs)
    assert ("mysql", "postgres") in pairs and ("postgres", "postgres") in pairs
    for src, dst in (("mysql", "postgres"), ("postgres", "postgres"),
                     ("postgres", "mysql")):
        assert int(rows[f"{src} -> {dst}"]) == len(autonomous.gaps(src, dst))
