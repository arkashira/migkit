"""Every table digest takes the hash the pair chose once (backlog R20.10):
the check's two sides, a batch's ranges, a table done earlier, a seed's
parts and a tail's kept digest all digest by it; the hash is kept beside
the digest, and two digests by different hashes are never compared.
All on fakes: no server.
"""
import json
import types

import pytest

from migkit import digests, export, leaves, runlog, ship, tailtables
from migkit.engines.hetero import HeteroEngine, _Masks
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine
from migkit.engines.sqlite import SQLiteEngine


class _Hashing:
    """A side whose digest takes a row hash, as PostgreSQL's does."""
    CANON_ENGINE = "postgres"

    def __init__(self):
        self.asked = []

    def neutral_digest(self, side, db, table, columns, where=None,
                       way="md5"):
        self.asked.append((side, table, way, where))
        return 3, f"{way}-{side}"


class _Plain:
    """A side whose digest takes md5 alone."""
    CANON_ENGINE = "sqlite"

    def __init__(self):
        self.asked = []

    def neutral_digest(self, side, db, table, columns, where=None):
        self.asked.append((side, table, where))
        return 3, f"md5-{side}"


def test_each_engine_says_the_hashes_its_digest_takes():
    assert leaves.digest_ways(object.__new__(PostgresEngine)) == (
        "md5", "sha256")
    assert leaves.digest_ways(object.__new__(MySQLEngine)) == (
        "md5", "sha256", "sha1")
    assert leaves.digest_ways(object.__new__(SQLiteEngine)) == ("md5",)
    assert leaves.digest_ways(_Plain()) == ("md5",)


def test_a_masked_side_takes_a_hash_only_where_its_engine_does():
    masked_pg = type("PostgresEngine", (_Masks, PostgresEngine), {})
    masked_lite = type("SQLiteEngine", (_Masks, SQLiteEngine), {})
    assert "sha256" in leaves.digest_ways(object.__new__(masked_pg))
    assert leaves.digest_ways(object.__new__(masked_lite)) == ("md5",)


def test_a_digest_is_asked_by_the_hash_or_refused_with_why():
    hashing, plain = _Hashing(), _Plain()
    assert leaves.digest_with(hashing, "dst", "d", "t", [], "sha256",
                              where="id < 5") == (3, "sha256-dst")
    assert hashing.asked == [("dst", "t", "sha256", "id < 5")]
    assert leaves.digest_with(plain, "dst", "d", "t", []) == (3, "md5-dst")
    with pytest.raises(SystemExit) as got:
        leaves.digest_with(plain, "dst", "d", "t", [], "sha256")
    assert "takes no sha256" in str(got.value)
    assert "taken by sha256" in str(got.value) and "md5" in str(got.value)
    assert plain.asked == [("dst", "t", None)]


def test_two_digests_of_different_hashes_are_not_compared():
    assert digests.unlike("sha256", "sha256") is None
    assert digests.unlike(None, "md5") is None
    why = digests.unlike("sha256", None)
    assert "sha256" in why and "md5" in why and "migkit check" in why


def test_the_pair_hash_is_md5_untimed_where_one_side_takes_no_other(
        monkeypatch):
    monkeypatch.setattr(leaves, "sides", lambda *a: pytest.fail(
        "a pair sharing md5 alone is never timed"))
    pair = types.SimpleNamespace(src_engine=_Hashing(), dst_engine=_Plain())
    assert leaves.pair_hash(pair, "d", "t", "t", [], []) == "md5"


def test_the_pair_hash_is_timed_among_the_hashes_both_take(monkeypatch):
    said = []
    monkeypatch.setattr(leaves, "sides", lambda *a: ("a", "b"))

    def fastest(work, a, b, salt, costs=None, among=None):
        said.append((work, salt, list(among)))
        return "sha256", None
    monkeypatch.setattr(leaves, "fastest_hash", fastest)
    pair = types.SimpleNamespace(
        src_engine=object.__new__(MySQLEngine),
        dst_engine=object.__new__(PostgresEngine),
        _leaf=lambda t: t, digest_salt=lambda: "s")
    assert leaves.pair_hash(pair, "d", "t", "t", [], []) == "sha256"
    assert said == [("d.t", "s", ["md5", "sha256"])]


def test_the_fastest_hash_climbs_only_among_those_given(monkeypatch):
    monkeypatch.setattr(runlog, "event", lambda *a, **k: None)

    def probe(s, spec, ways, n=leaves.PROBE_ROWS):
        took = {"md5": 0.4, "sha256": 0.2, "sha1": 0.1}
        return 1000, {w: (took[w], None) for w in ways}
    monkeypatch.setattr(leaves, "probe", probe)
    eng = _Hashing()
    a = leaves.Side(eng, "src", "d", "t", [("id", "integer")], ["id"])
    b = leaves.Side(eng, "dst", "d", "t", [("id", "integer")], ["id"])
    assert leaves.fastest_hash("d.t", a, b, "s")[0] == "sha1"
    assert leaves.fastest_hash("d.t", a, b, "s",
                               among=("md5", "sha256"))[0] == "sha256"


def test_the_check_digests_both_sides_by_the_chosen_hash_and_keeps_it(
        tmp_path):
    src, dst = _Hashing(), _Hashing()
    fake = types.SimpleNamespace(
        src_engine=src, dst_engine=dst,
        hop=types.SimpleNamespace(report_dir=lambda d=None: tmp_path),
        _masks=lambda db, t: False, digest_salt=lambda: "s",
        digest_way=lambda *a, **k: "sha256")
    a, b = HeteroEngine._digests(fake, "d", "t", [("id", "integer")], "t",
                                 [("id", "integer")], (None, None))
    assert (a, b) == ((3, "sha256-src"), (3, "sha256-dst"))
    assert [w for _, _, w, _ in src.asked + dst.asked] == ["sha256"] * 2
    lines = [json.loads(x) for x in
             (tmp_path / digests.FILE).read_text().splitlines()]
    assert {(x["side"], x["way"]) for x in lines} == {("src", "sha256"),
                                                      ("dst", "sha256")}


def test_the_hash_is_chosen_once_a_table_and_md5_where_untimed(
        monkeypatch):
    asked = []

    def pair_hash(pair, db, src_t, dst_t, sc, dc):
        asked.append(src_t)
        if src_t == "bad":
            raise RuntimeError("probe failed")
        return "sha256"
    monkeypatch.setattr(leaves, "pair_hash", pair_hash)
    monkeypatch.setattr(runlog, "event", lambda *a, **k: None)
    fake = types.SimpleNamespace(_leaf=HeteroEngine._leaf)
    way = HeteroEngine.digest_way
    assert way(fake, "d", "t", "t", [], []) == "sha256"
    assert way(fake, "d", "t", "t", [], []) == "sha256"
    assert way(fake, "d", "t") == "sha256"
    assert way(fake, "d", "u") == "md5"
    assert way(fake, "d", "bad", "bad", [], []) == "md5"
    assert asked == ["t", "bad"]


def test_a_seed_part_is_folded_by_the_manifests_hash():
    assert ship._way({}) == "md5"
    assert ship._way({"way": "sha256"}) == "sha256"
    rows = [[1, "a"], [2, "b"]]
    by = {}
    for way in ("md5", "sha256"):
        stats = {}
        list(ship._rows_pieces([rows], [0, 1], ["integer", "text"], stats,
                               way))
        by[way] = stats["digest"]
    assert by["md5"] != by["sha256"]


def test_a_tails_kept_digest_is_taken_again_by_its_own_hash():
    eng = _Hashing()
    me = types.SimpleNamespace(db="d")
    got = tailtables.TableSet._digest(me, eng, "dst", "t",
                                      [["id", "integer"]], "sha256")
    assert got == [3, "sha256-dst", "sha256"]
    assert tailtables.TableSet._kept_way(got) == "sha256"
    assert tailtables.TableSet._kept_way([3, "x"]) == "md5"
    assert tailtables.TableSet._digest(me, eng, "dst", "t", []) is None


def test_the_export_refuses_to_compare_digests_of_different_hashes(
        monkeypatch):
    line = {"rows": 3, "digest": "7", "method": "neutral"}
    monkeypatch.setattr(digests, "read", lambda hop, s, left=None: {
        ("d", "t", None): {"src": {**line, "way": "sha256"},
                           "dst": dict(line)}})
    row, = export._tables(None, None)
    assert row["status"] == "not comparable"
    assert "sha256" in row["why"] and "md5" in row["why"]
