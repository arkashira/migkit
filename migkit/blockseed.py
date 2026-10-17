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
Its digests are kept on disk, one record a block, and read in step a
record at a time (`_digests`): a volume's size never decides what is held
in memory. What changed is the blocks whose digests differ from the
parent's (`changed`); a list the snapshot kind gives of its own is checked against
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


#: the bytes of one block's digest record (sha256)
RECORD = hashlib.sha256().digest_size


def _digests(m):
    """Each block's digest of manifest `m`, in order, read from its file
    one record at a time."""
    with open(m["digests"], "rb") as f:
        while True:
            got = f.read(RECORD)
            if not got:
                return
            yield got.hex()


def _digest_of(m, i):
    with open(m["digests"], "rb") as f:
        f.seek(int(i) * RECORD)
        return f.read(RECORD).hex()


def manifest(kind, volume, snapshot, block_size, read_block, blocks,
             position=None, parent=None, store=None, read_position=None):
    """The manifest of `snapshot` of `volume`: every one of its `blocks`
    blocks of `block_size` bytes read through `read_block(i)` and digested,
    with the source's `position` recorded at the snapshot and the
    snapshot it follows (`parent`). The digests go to `store` (a file;
    None: a file of the system's temporary directory) as they are taken,
    never held together in memory."""
    if kind not in KINDS:
        raise Refused(f"a {kind} snapshot is not one a seed reads: "
                      + ", ".join(sorted(KINDS)))
    if callable(position):
        position = position()
    elif not position and callable(read_position):
        position = read_position()
    if not position:
        raise Refused(f"snapshot {snapshot} of {volume} records no position"
                      " of the source: the follow could not start from it,"
                      " so it seeds nothing")
    import os
    import tempfile
    if store is None:
        fd, store = tempfile.mkstemp(prefix=f"{snapshot}.", suffix=".digests")
        os.close(fd)
    with open(store, "wb") as f:
        for i in range(int(blocks)):
            data = read_block(i)
            if len(data) != int(block_size):
                raise Refused(f"block {i} of snapshot {snapshot} reads"
                              f" {len(data)} bytes, not {block_size}")
            f.write(hashlib.sha256(bytes(data)).digest())
    return {"kind": kind, "volume": volume, "snapshot": snapshot,
            "parent": parent, "block_size": int(block_size),
            "blocks": int(blocks), "digests": str(store),
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
    if base["blocks"] > snap["blocks"]:
        raise Refused(f"the target's snapshot {base['snapshot']} holds"
                      f" {base['blocks']} blocks, more than the"
                      f" {snap['blocks']} of {snap['snapshot']}: what lies"
                      " past its end would stay on the target, so the"
                      " whole volume is the seed")
    old = _digests(base)
    out = [i for i, want in enumerate(_digests(snap))
           if next(old, None) != want]
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


def _block_io(fn_or_path, block_size, mode="rb"):
    if callable(fn_or_path):
        return fn_or_path
    if fn_or_path:
        from pathlib import Path
        p = Path(fn_or_path)
        if mode == "write":
            def write_b(i, data):
                with open(p, "r+b") as f:
                    f.seek(int(i) * int(block_size))
                    f.write(data)
            return write_b
        def read_b(i):
            with open(p, "rb") as f:
                f.seek(int(i) * int(block_size))
                return f.read(int(block_size))
        return read_b
    return None


def carry(snap, base, read_block, write_block, read_target, state_path,
          listed=None, read_position=None):
    """The changed blocks of `snap` written over a target holding `base`:
    each read through `read_block(i)` and held to its digest, written
    through `write_block(i, data)`, read back through `read_target(i)` and
    held again, and only then checkpointed in `state_path`. A resume skips
    what was checkpointed; then every block of the target is held to the
    snapshot's digests, the unchanged ones too. The `Carried`."""
    from pathlib import Path
    state_path = Path(state_path)
    if not snap.get("position") and callable(read_position):
        snap["position"] = read_position()
    if not callable(read_block):
        read_block = _block_io(read_block or snap.get("volume"), snap["block_size"])
    if not callable(write_block):
        write_block = _block_io(write_block or (base and base.get("volume")), snap["block_size"], mode="write")
    if not callable(read_target):
        read_target = _block_io(read_target or (base and base.get("volume")), snap["block_size"])
    todo = changed(snap, base, listed)
    state = _state(state_path, snap)
    done = set(state["done"])
    out = Carried(resumed=len(done & set(todo)),
                  unchanged=snap["blocks"] - len(todo),
                  position=snap.get("position") or "")
    for i in todo:
        if i in done:
            continue
        want = _digest_of(snap, i)
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
    verify(snap, read_target, base["blocks"])
    return out


def verify(snap, read_target, target_blocks=None):
    """Every block of the target held to the snapshot's digest; the first
    that differs is refused by its number, and a target holding more
    blocks than the snapshot (`target_blocks`) is refused: its blocks past
    the end are no snapshot's."""
    if target_blocks is not None and int(target_blocks) > snap["blocks"]:
        raise Refused(f"the target holds {target_blocks} blocks, more than"
                      f" the {snap['blocks']} of snapshot"
                      f" {snap['snapshot']}: the follow is not started on"
                      " it")
    for i, want in enumerate(_digests(snap)):
        if digest(read_target(i)) != want:
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


def facts(snap, base, rate, listed=None, read_rate=None):
    """What `decide.seed_carrier` reads of a block seed: whether one can be
    carried and why not, and the seconds it takes - its changed blocks at
    the measured link `rate` and then every block of the target read back
    at the target's measured `read_rate` (`verify`; bytes a second). Either
    not measured: no seconds, and `blocks unmeasured` says which."""
    try:
        todo = changed(snap, base, listed)
    except Refused as e:
        return {"blocks ok": str(e), "blocks": None}
    size = len(todo) * snap["block_size"]
    whole = snap["blocks"] * snap["block_size"]
    lacking = [w for w, r in (("the link's rate", rate),
                              ("the target's read rate", read_rate)) if not r]
    return {"blocks ok": True, "blocks bytes": size,
            "blocks read back": whole,
            "blocks unmeasured": (" and ".join(lacking) + " not measured:"
                                  " the block seed is not costed"
                                  if lacking else None),
            "blocks": None if lacking else size / rate + whole / read_rate}
