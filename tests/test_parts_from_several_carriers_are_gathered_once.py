"""A seed whose parts travelled on more than one carrier is gathered once
before it is loaded (DEC1001.media): every place listed, each part taken
from the first place that holds the bytes the manifest names, a copy held
in two places kept once and the one whose digest is not the manifest's
refused, two parts of the manifest that digest alike refused, and a load
that goes on does not look again. Run on files: SQLite on both sides and
directories as the carriers.
"""
import shutil

import pytest

from migkit import ship
from tests.test_a_seed_is_shipped_on_media import (  # noqa: F401
    _carry, _manifest, _rows, seed)


def _spread(tmp_path, manifest):
    """The parts moved onto two other carriers, and a good and a bad copy
    of one part left in a third place."""
    a = tmp_path / "carrier"
    b, c = tmp_path / "carrier-b", tmp_path / "carrier-c"
    names = sorted(p["name"] for e in manifest["tables"]
                   for p in e["parts"].values())
    for i, name in enumerate(names):
        if i % 3 == 1:
            (b / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.move(a / name, b / name)
    twice = names[0]
    (c / twice).parent.mkdir(parents=True, exist_ok=True)
    raw = bytearray((a / twice).read_bytes())
    raw[-1] ^= 0xFF
    (c / twice).write_bytes(bytes(raw))
    return b, c, twice, names


def test_parts_on_several_carriers_are_gathered_and_loaded_once(seed):
    tmp_path, src, dst, hop = seed
    got, said = _carry(hop)
    assert got["main"].half == "export", said
    manifest, store, _ = _manifest(tmp_path, hop)
    b, c, twice, names = _spread(tmp_path, manifest)
    hop.options["ship"]["gather"] = [str(b), str(c)]
    got, said = _carry(hop)
    assert got["main"].half == "load", said
    line = next(m for m in said if "gathered from 3 places" in m)
    assert f"{len(names):,} of {len(names):,} parts" in line, line
    # the damaged copy of a part also held elsewhere is refused by its
    # digest; nothing was read again over the link
    assert "1 copies refused" in line and twice in line, line
    assert not any("read again from the source" in m for m in said), said
    for t in ("orders", "tags", "log"):
        assert _rows(dst, t) == _rows(src, t), t


def test_a_part_on_no_carrier_here_is_read_again_from_the_source(seed):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, store, _ = _manifest(tmp_path, hop)
    lost = sorted(p["name"] for e in manifest["tables"]
                  for p in e["parts"].values())[2]
    (tmp_path / "carrier" / lost).unlink()
    hop.options["ship"]["gather"] = [str(tmp_path / "carrier-b")]
    got, said = _carry(hop)
    assert any("on no place here" in m for m in said), said
    assert any("read again from the source" in m for m in said), said
    for t in ("orders", "tags", "log"):
        assert _rows(dst, t) == _rows(src, t), t


def test_a_gather_that_goes_on_looks_at_no_part_again(seed, monkeypatch):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, store, seal = _manifest(tmp_path, hop)
    b, c, twice, _ = _spread(tmp_path, manifest)
    w = ship._Work(hop, ship_eng(hop), "main", "full", lambda m: None)
    first = ship.gather(w, store, manifest, seal, [
        ship.carriers.LocalStore(b), ship.carriers.LocalStore(c)])
    monkeypatch.setattr(ship, "_held", lambda *a: pytest.fail("read again"))
    again = ship.gather(w, store, manifest, seal, [
        ship.carriers.LocalStore(b), ship.carriers.LocalStore(c)])
    assert {k: v.said for k, v in first.items()} \
        == {k: v.said for k, v in again.items()}


def ship_eng(hop):
    from migkit.engines.sqlite import SQLiteEngine
    return SQLiteEngine(hop)


def test_two_parts_that_digest_alike_refuse_the_seed(seed):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, store, seal = _manifest(tmp_path, hop)
    parts = [p for e in manifest["tables"] for p in e["parts"].values()]
    parts[1]["sha256"] = parts[0]["sha256"]
    w = ship._Work(hop, ship_eng(hop), "main", "full", lambda m: None)
    with pytest.raises(SystemExit) as e:
        ship.gather(w, store, manifest, seal, [])
    assert "same digest" in str(e.value), e.value
