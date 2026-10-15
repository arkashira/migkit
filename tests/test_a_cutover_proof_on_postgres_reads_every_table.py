"""The PostgreSQL check passes a table over only on `unchanged.judge`, and
never when its proof goes into a cutover; a range that cannot pass server
to server climbs to the text copy through the one decision engine, a
refusal stopping it; and a foreign key a load took off the target and did
not put back stops the cutover and is said by the check.

Without a server: the engine is built bare and fed what the servers would
have answered."""
import json
from types import SimpleNamespace

import pytest

from migkit import cutover, decide, freeze, indexes, setaside, unchanged
from migkit.engines.postgres import PostgresEngine
from migkit.variants import Brand


def _eng(src="postgres", dst="postgres", hop=None):
    eng = PostgresEngine.__new__(PostgresEngine)
    eng._brand_cache = (Brand(name=src, family="postgres"),
                        Brand(name=dst, family="postgres"))
    eng.hop = hop or SimpleNamespace(options={})
    return eng


LIVE = unchanged.marker([5, 1, 0, 16384, 8192, "3:abc"])
DEAD = unchanged.marker([5, 1, 0, 0, 8192, "3:abc"])


def test_a_quiet_table_is_passed_over_on_counters_that_hold():
    got = _eng()._skip_judged({"src": LIVE, "dst": LIVE}, LIVE, LIVE)
    assert got.skip


def test_a_marker_with_no_file_identity_never_passes_a_table_over():
    """relfilenode 0 is a marker that is the same string forever: an
    emptied table would look unchanged. `usable_brand` let it through."""
    got = _eng()._skip_judged({"src": DEAD, "dst": DEAD}, DEAD, DEAD)
    assert not got.skip and "file identity" in got.why


def test_a_kind_whose_counters_are_not_read_never_passes_a_table_over():
    got = _eng(src="cockroachdb")._skip_judged(
        {"src": LIVE, "dst": LIVE}, LIVE, LIVE)
    assert not got.skip and "relfilenode" in got.why
    got = _eng(src="unknown")._skip_judged(
        {"src": LIVE, "dst": LIVE}, LIVE, LIVE)
    assert not got.skip


def test_a_counter_that_moved_is_named_when_the_table_is_read_again():
    moved = unchanged.marker([6, 1, 0, 16384, 8192, "3:abc"])
    got = _eng()._skip_judged({"src": LIVE, "dst": LIVE}, moved, LIVE)
    assert not got.skip and "rows were inserted" in got.why


def test_a_proof_held_for_a_cutover_passes_nothing_over():
    got = _eng()._skip_judged({"src": LIVE, "dst": LIVE}, LIVE, LIVE,
                              held="the proof goes into a cutover")
    assert not got.skip and "cutover" in got.why


def test_the_cutover_check_and_a_frozen_source_hold_every_table(
        monkeypatch):
    eng = _eng()
    monkeypatch.setattr(freeze, "source_frozen", lambda hop, dbs: False)
    assert eng._held_for_cutover("d") == ""
    eng.cutover_proof = True
    assert "cutover" in eng._held_for_cutover("d")
    eng.cutover_proof = False
    monkeypatch.setattr(freeze, "source_frozen", lambda hop, dbs: True)
    assert "held for a cutover" in eng._held_for_cutover("d")

    def broken(hop, dbs):
        raise OSError("no such file")
    monkeypatch.setattr(freeze, "source_frozen", broken)
    assert "could not be read" in eng._held_for_cutover("d")


class _CutEng:
    """What the cutover's last check asks of an engine, and what it saw."""
    cutover_proof = False

    def __init__(self, off=None):
        self.off, self.seen = off, []

    def check_data(self, d):
        self.seen.append(self.cutover_proof)
        return [SimpleNamespace(status="ok", scope="t", detail="")]

    def foreign_keys_off(self, d):
        if isinstance(self.off, Exception):
            raise self.off
        return self.off or ([], [])


def _final(eng):
    hop = SimpleNamespace(options={}, report_dir=lambda d=None: None)
    said = []
    cutover._final_check(hop, eng, "d", None, lambda: 5.0, said.append)
    return said


KEY = indexes.fk_key("public", "b", "b_a_fk")


def test_the_cutover_check_is_read_as_a_cutover_proof_and_then_let_go():
    eng = _CutEng()
    said = _final(eng)
    assert eng.seen == [True]
    assert eng.cutover_proof is False
    assert "found the two the same" in said[-1]


def test_a_foreign_key_still_off_stops_the_cutover():
    with pytest.raises(cutover._Undo, match="foreign keys"):
        _final(_CutEng(off=([(KEY, "/r/x.json", False)], [])))
    with pytest.raises(cutover._Undo, match="not back"):
        _final(_CutEng(off=([(KEY, "/r/x.json", True)], [])))
    with pytest.raises(cutover._Undo, match="cannot be read"):
        _final(_CutEng(off=([], ["/r/dropped-fks.h.1.1.json"])))
    with pytest.raises(cutover._Undo, match="could not be read"):
        _final(_CutEng(off=RuntimeError("connection refused")))


def test_the_preflight_fails_on_a_key_still_off_and_passes_with_none():
    hop = SimpleNamespace(options={}, report_dir=lambda d=None: None)
    off = _CutEng(off=([(KEY, "/r/x.json", False)], []))
    assert cutover._keys_off(hop, off, "d")
    assert cutover._keys_off(hop, _CutEng(), "d") == ""


FK = {KEY: {"on": "public.b", "name": "b_a_fk",
            "def": "FOREIGN KEY (a) REFERENCES a(id)", "valid": True}}


def _record(tmp_path, monkeypatch, text):
    monkeypatch.setattr(setaside.SetAside, "_alive",
                        lambda self, host, pid: False)
    path = tmp_path / f"{indexes.FKS_SET_ASIDE}.elsewhere-host.4242.1.json"
    path.write_text(text)
    return path


def _fk_eng(tmp_path, present):
    hop = SimpleNamespace(options={}, name="h",
                          report_dir=lambda db=None: tmp_path)
    eng = _eng(hop=hop)
    out = chr(30).join(chr(31).join(
        ["public", "b", "b_a_fk", "t" if valid else "f",
         "FOREIGN KEY (a) REFERENCES a(id)", "public", "a"])
        for valid in present)
    eng._psql = lambda side, db, sql: out
    return eng


def test_the_check_says_a_key_a_dead_load_left_off(tmp_path, monkeypatch):
    path = _record(tmp_path, monkeypatch, json.dumps(FK))
    eng = _fk_eng(tmp_path, present=[])
    off, unread = eng.foreign_keys_off("d")
    assert [k for k, _, _ in off] == [KEY] and not unread
    got = eng.set_aside("d")
    assert got.status == "diff" and "never put back" in got.detail
    assert str(path) in got.detail


def test_a_key_back_but_not_validated_is_still_off(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, json.dumps(FK))
    off, _ = _fk_eng(tmp_path, present=[False]).foreign_keys_off("d")
    assert off


def test_a_key_back_and_valid_is_not_said(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, json.dumps(FK))
    eng = _fk_eng(tmp_path, present=[True])
    assert eng.foreign_keys_off("d") == ([], [])
    assert eng.set_aside("d") is None


def test_no_record_reads_nothing_of_the_target(tmp_path):
    eng = _fk_eng(tmp_path, present=[])

    def never(*a):
        raise AssertionError("the target was read with no record")
    eng._psql = never
    assert eng.set_aside("d") is None


def test_an_unreadable_record_is_said(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, "{not json")
    got = _fk_eng(tmp_path, present=[]).set_aside("d")
    assert got.status == "diff" and "cannot be read" in got.detail


class _Ctx:
    def __init__(self, error):
        self.error, self.released = error, []
        self.snapshot = SimpleNamespace(
            release=self.released.append, lost=lambda e: None)

    def copy(self, *a, **kw):
        raise self.error


def _passing(error):
    passing = decide.Rung(name="pass-through", said="server to server")
    text = decide.Rung(name="pipe", said="as text")
    gave_up = []
    ways = SimpleNamespace(
        passing={"pass-through"},
        trials=SimpleNamespace(rungs=(passing, text), facts=decide.Facts()),
        give_up_passing=gave_up.append, pick=lambda begin: "the text way")
    way = SimpleNamespace(ctx=_Ctx(error), ways=ways, name="pass-through",
                          choice=SimpleNamespace(rung=passing),
                          export="snap-1", begin=None)
    eng = _eng()
    copied = []
    eng._copy_checked = lambda *a, **kw: copied.append(kw["way"]) or 7
    said = []
    got = eng._passed("d", "public.t", [], "", "", "public.t [0,10)",
                      said.append, way, None, 0.0)
    return got, copied, gave_up, said, way.ctx


def test_a_range_that_cannot_pass_goes_as_text_and_the_switch_is_said():
    got, copied, gave_up, said, ctx = _passing(
        RuntimeError("ERROR: wrong binary format for type x"))
    assert got == 7 and copied == ["the text way"]
    assert gave_up and "binary format" in gave_up[0]
    assert any("switching to as text" in s for s in said)
    assert ctx.released == ["snap-1"]


def test_a_refusal_stops_the_range_rather_than_copy_it_as_text():
    with pytest.raises(SystemExit, match="permission denied"):
        _passing(RuntimeError("ERROR: permission denied for table t"))


def test_a_fault_of_migkit_is_raised_not_hidden_by_the_text_copy():
    with pytest.raises(TypeError):
        _passing(TypeError("unsupported operand"))
