"""A seed carried as the blocks of a volume snapshot (backlog LT.P2).

Where the source's data sits on a volume that takes snapshots - a ZFS
dataset, an LVM thin volume, an EBS volume - and the target already holds
an earlier snapshot of the same volume, only the blocks that changed
between the two travel, and the change follow starts from the position the
source recorded when the newer snapshot was taken. It is a rung of the
seed (`ship.SEEDS["blocks"]`), placed against the seed on media and the
copy over the link by `decide.seed_carrier` from what each would take.

A snapshot is described by its manifest (`manifest`): its volume, its
parent snapshot, the block size, every block's digest and the position.
What changed is the blocks whose digests differ from the parent's
(`changed`); a list the snapshot kind gives of its own is checked against
them and refused where it misses one, so a stale block is never left
behind. Each block is held to its digest as it is read, written, read back
and checkpointed only then (`carry`); a resume writes again what was not
checkpointed (the same bytes at the same place) and the whole target is
held to the snapshot's digests before the follow is let start
(`fast_forward`). Nothing here assumes success: a digest that differs, a
parent that is not the target's, a position not recorded stops the seed
and says why."""
import hashlib
from dataclasses import dataclass

from . import durable

#: the snapshot kinds and how each names its changed blocks
KINDS = {
    "zfs": "an incremental send between the two snapshots",
    "lvm": "the thin pool's delta between the two snapshots",
    "ebs": "the volume service's list of blocks changed between them",
}

UNPROVED_ON = {
    "zfs": "built on fakes; not run against a real ZFS dataset",
    "lvm": "built on fakes; not run against a real LVM thin volume",
    "ebs": "built on fakes; not run against a real EBS snapshot",
}
UNPROVED_ITEMS = {
    "zfs": "seed.blocks seed.changed-list seed.fast-forward",
    "lvm": "seed.blocks seed.changed-list seed.fast-forward",
    "ebs": "seed.blocks seed.changed-list seed.fast-forward",
}


class Refused(SystemExit):
    """A block seed that cannot be carried safely, and why."""


def digest(data):
    return hashlib.sha256(bytes(data)).hexdigest()


def manifest(kind, volume, snapshot, block_size, read_block, blocks,
             position, parent=None):
    """The manifest of `snapshot` of `volume`: every one of its `blocks`
    blocks of `block_size` bytes read through `read_block(i)` and digested,
    with the source's `position` recorded at the snapshot and the
    snapshot it follows (`parent`)."""
    if kind not in KINDS:
        raise Refused(f"a {kind} snapshot is not one a seed reads: "
                      + ", ".join(sorted(KINDS)))
    if not position:
        raise Refused(f"snapshot {snapshot} of {volume} records no position"
                      " of the source: the follow could not start from it,"
                      " so it seeds nothing")
    digests = {}
    for i in range(int(blocks)):
        data = read_block(i)
        if len(data) != int(block_size):
            raise Refused(f"block {i} of snapshot {snapshot} reads"
                          f" {len(data)} bytes, not {block_size}")
        digests[str(i)] = digest(data)
    return {"kind": kind, "volume": volume, "snapshot": snapshot,
            "parent": parent, "block_size": int(block_size),
            "blocks": int(blocks), "digests": digests,
            "position": position}


def changed(snap, base, listed=None):
    """The blocks of `snap` the target holding `base` lacks, in order:
    those whose digest differs from the parent's, and those beyond its end.
    `listed` is the snapshot kind's own list of changed blocks, where it
    gives one: a block it misses is refused, never trusted away."""
    if base is None:
        raise Refused(f"the target holds no snapshot of {snap['volume']}:"
                      " the whole volume is the seed, not its changed"
                      " blocks")
    for k in ("kind", "volume", "block_size"):
        if snap[k] != base[k]:
            raise Refused(f"the target's snapshot {base['snapshot']} is of"
                          f" another {k} ({base[k]}, not {snap[k]}): its"
                          " blocks are not this volume's")
    if snap.get("parent") != base["snapshot"]:
        raise Refused(f"snapshot {snap['snapshot']} follows"
                      f" {snap.get('parent')}, not the target's"
                      f" {base['snapshot']}: the blocks between them are"
                      " not known, so the whole volume is the seed")
    out = [i for i in range(snap["blocks"])
           if base["digests"].get(str(i)) != snap["digests"][str(i)]]
    if listed is not None:
        missed = sorted(set(out) - {int(i) for i in listed})
        if missed:
            raise Refused(f"the {snap['kind']} list of changed blocks misses"
                          f" {len(missed)} that changed (block {missed[0]}"
                          " first): it is not trusted, and the blocks are"
                          " told apart by digest")
    return out


@dataclass
class Carried:
    """What a block seed carried."""
    blocks: int = 0         #: changed blocks written in this run
    resumed: int = 0        #: changed blocks a run before had checkpointed
    bytes: int = 0
    unchanged: int = 0      #: blocks the target held already
    position: str = ""      #: where the follow starts


def _state(path, snap):
    got = durable.read_state(path, "state")
    if not got or got.get("snapshot") != snap["snapshot"]:
        return {"snapshot": snap["snapshot"], "done": []}
    return got


def carry(snap, base, read_block, write_block, read_target, state_path,
          listed=None):
    """The changed blocks of `snap` written over a target holding `base`:
    each read through `read_block(i)` and held to its digest, written
    through `write_block(i, data)`, read back through `read_target(i)` and
    held again, and only then checkpointed in `state_path`. A resume skips
    what was checkpointed; then every block of the target is held to the
    snapshot's digests, the unchanged ones too. The `Carried`."""
    from pathlib import Path
    state_path = Path(state_path)
    todo = changed(snap, base, listed)
    state = _state(state_path, snap)
    done = set(state["done"])
    out = Carried(resumed=len(done & set(todo)),
                  unchanged=snap["blocks"] - len(todo),
                  position=snap["position"])
    for i in todo:
        if i in done:
            continue
        want = snap["digests"][str(i)]
        data = read_block(i)
        if digest(data) != want:
            raise Refused(f"block {i} read from snapshot {snap['snapshot']}"
                          " is not the one its manifest names: the snapshot"
                          " changed or the read was damaged, and nothing"
                          " past it is written")
        write_block(i, data)
        if digest(read_target(i)) != want:
            raise Refused(f"block {i} on the target reads back other than it"
                          " was written: the target's volume did not keep"
                          " it, and the seed stops there")
        done.add(i)
        state["done"] = sorted(done)
        durable.write_json(state_path, state)
        out.blocks += 1
        out.bytes += len(data)
    verify(snap, read_target)
    return out


def verify(snap, read_target):
    """Every block of the target held to the snapshot's digest; the first
    that differs is refused by its number."""
    for i in range(snap["blocks"]):
        if digest(read_target(i)) != snap["digests"][str(i)]:
            raise Refused(f"block {i} on the target is not snapshot"
                          f" {snap['snapshot']}'s: the target was written"
                          " outside the seed, so the follow is not started"
                          " on it")


def fast_forward(carried, follow):
    """The follow started from the position the snapshot recorded, once
    the target holds every block of it (`carry`)."""
    if not carried.position:
        raise Refused("the seed carries no position to follow from")
    return follow(carried.position)


def facts(snap, base, rate, listed=None):
    """What `decide.seed_carrier` reads of a block seed: whether one can be
    carried and why not, and the seconds its changed blocks take at the
    measured `rate` (bytes a second; None where nothing measured)."""
    try:
        todo = changed(snap, base, listed)
    except Refused as e:
        return {"blocks ok": str(e), "blocks": None}
    size = len(todo) * snap["block_size"]
    return {"blocks ok": True, "blocks bytes": size,
            "blocks": size / rate if rate else None}
