"""A seed carried as a volume snapshot's changed blocks (backlog LT.P2):
only the blocks that differ from the snapshot the target holds travel,
each held to its digest and checkpointed after it reads back, the whole
target held to the snapshot before the follow starts at the position
recorded at the snapshot. On fakes: a volume as a list of blocks, its
manifests, the kind's changed-block list; the real snapshot kinds are
declared unproved (`blockseed.UNPROVED_ON`)."""
import pytest

from migkit import blockseed, decide, ship
from migkit.blockseed import Refused

SIZE = 16


class Volume:
    """A fake volume: blocks of SIZE bytes, every write and read counted."""

    def __init__(self, blocks):
        self.blocks = [bytes(b) for b in blocks]
        self.writes, self.reads = [], []

    def read(self, i):
        self.reads.append(i)
        return self.blocks[i]

    def write(self, i, data):
        self.writes.append(i)
        self.blocks[i] = bytes(data)


def block(c):
    return bytes([ord(c)]) * SIZE


def snapshots():
    old = Volume([block(c) for c in "abcdefgh"])
    base = blockseed.manifest("zfs", "tank/pg", "s1", SIZE, old.read, 8,
                              "0/100")
    new = Volume([block(c) for c in "abXdeYgZ"])
    snap = blockseed.manifest("zfs", "tank/pg", "s2", SIZE, new.read, 8,
                              "0/2A0", parent="s1")
    target = Volume(old.blocks)
    return base, snap, new, target


def test_only_the_changed_blocks_are_written_and_the_follow_starts_there(
        tmp_path):
    base, snap, src, target = snapshots()
    assert blockseed.changed(snap, base) == [2, 5, 7]
    src.reads.clear()
    got = blockseed.carry(snap, base, src.read, target.write, target.read,
                          tmp_path / "seed.json")
    assert target.writes == [2, 5, 7]
    assert sorted(set(src.reads)) == [2, 5, 7]
    assert target.blocks == src.blocks
    assert (got.blocks, got.unchanged, got.bytes) == (3, 5, 3 * SIZE)
    started = []
    blockseed.fast_forward(got, started.append)
    assert started == ["0/2A0"]


def test_an_interrupted_seed_resumes_and_writes_no_checkpointed_block_again(
        tmp_path):
    base, snap, src, target = snapshots()
    state = tmp_path / "seed.json"

    def dies_on_seven(i, data):
        if i == 7:
            raise ConnectionError("the target went away")
        target.write(i, data)
    with pytest.raises(ConnectionError):
        blockseed.carry(snap, base, src.read, dies_on_seven, target.read,
                        state)
    assert target.writes == [2, 5]
    target.writes.clear()
    got = blockseed.carry(snap, base, src.read, target.write, target.read,
                          state)
    assert target.writes == [7]
    assert (got.blocks, got.resumed) == (1, 2)
    assert target.blocks == src.blocks
    # a third run carries nothing and still holds the whole target
    target.writes.clear()
    again = blockseed.carry(snap, base, src.read, target.write, target.read,
                            state)
    assert target.writes == [] and again.resumed == 3


def test_a_kinds_list_that_misses_a_changed_block_is_refused():
    base, snap, _, _ = snapshots()
    assert blockseed.changed(snap, base, listed=[2, 5, 7, 3]) == [2, 5, 7]
    with pytest.raises(Refused, match="misses 1 that changed"):
        blockseed.changed(snap, base, listed=[2, 5])


def test_a_snapshot_not_following_the_targets_is_refused():
    base, snap, _, _ = snapshots()
    with pytest.raises(Refused, match="not the target's s1"):
        blockseed.changed(dict(snap, parent="s0"), base)
    with pytest.raises(Refused, match="another block_size"):
        blockseed.changed(dict(snap, block_size=8), base)
    with pytest.raises(Refused, match="whole volume is the seed"):
        blockseed.changed(snap, None)


def test_a_snapshot_without_a_recorded_position_seeds_nothing():
    v = Volume([block("a")])
    with pytest.raises(Refused, match="records no position"):
        blockseed.manifest("ebs", "vol-1", "snap-1", SIZE, v.read, 1, "")
    with pytest.raises(Refused, match="not one a seed reads"):
        blockseed.manifest("btrfs", "v", "s", SIZE, v.read, 1, "0/1")


def test_a_block_damaged_on_read_or_on_the_target_stops_the_seed(tmp_path):
    base, snap, src, target = snapshots()
    src.blocks[5] = block("Q")
    with pytest.raises(Refused, match="block 5 read from snapshot"):
        blockseed.carry(snap, base, src.read, target.write, target.read,
                        tmp_path / "a.json")
    assert 5 not in target.writes
    base, snap, src, target = snapshots()
    with pytest.raises(Refused, match="reads back other"):
        blockseed.carry(snap, base, src.read, lambda i, d: None,
                        target.read, tmp_path / "b.json")
    # an unchanged block written outside the seed is caught by the whole
    # target's check, never assumed
    base, snap, src, target = snapshots()
    target.blocks[0] = block("z")
    with pytest.raises(Refused, match="block 0 on the target"):
        blockseed.carry(snap, base, src.read, target.write, target.read,
                        tmp_path / "c.json")


def test_the_carrier_is_placed_by_the_measured_seconds_against_the_others():
    assert ship.SEEDS["blocks"] is decide.SEED_BLOCKS
    base, snap, _, _ = snapshots()
    f = blockseed.facts(snap, base, rate=SIZE)  # a block a second
    assert (f["blocks ok"], f["blocks"]) == (True, 3.0)
    got = decide.seed_carrier(dict(f, network=8.0, media=20.0, ship=True))
    assert got.path == "blocks"
    # the link measured faster than the blocks
    got = decide.seed_carrier(dict(f, blocks=30.0, network=8.0, ship=False))
    assert got.path == "network"
    # no parent the target holds: the block seed is passed over, saying why
    gone = blockseed.facts(dict(snap, parent="s0"), base, rate=SIZE)
    got = decide.seed_carrier(dict(gone, network=8.0, ship=False))
    assert got.path == "network"
    assert "not the target's s1" in got.reason
    # past the deadline, every way is refused and each says so
    got = decide.seed_carrier(dict(f, blocks=500.0, network=900.0,
                                   deadline=60.0, ship=False))
    assert got.rung is None and "past the deadline" in got.reason


def test_the_real_snapshot_kinds_are_declared_unproved():
    assert set(blockseed.UNPROVED_ON) == set(blockseed.KINDS)
    assert set(blockseed.UNPROVED_ITEMS) == set(blockseed.KINDS)
