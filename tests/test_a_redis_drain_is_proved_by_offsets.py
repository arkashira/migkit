"""A Redis cutover proves its drain by the stream's own position: the
source's replication id and offset, taken after the freeze, and waited for
on the target that replicates it (P0-PARITY.all). Run against a real
source and a real replica, started here."""
import time

from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.drain", "redis")]

pytestmark = needs_server


def _replicate(src, dst):
    c = dst.client()
    c.replicaof("127.0.0.1", src.port)
    end = time.time() + 20
    while time.time() < end:
        if c.info("replication").get("master_link_status") == "up":
            return
        time.sleep(0.1)
    raise AssertionError("the replica's link did not come up")


def test_everything_written_before_the_freeze_is_on_the_replica(
        clean, tmp_path):
    src, dst = clean
    eng = engine(clean, tmp_path)
    _replicate(src, dst)
    try:
        for i in range(200):
            src.client().set(f"k{i}", i)
        # the freeze has held the writes; the position is taken after it
        at = eng.src_lsn("0")
        assert eng.fence_wait("0", at, timeout=20) is True
        assert eng.position_reached(eng.log_position("dst", "0"), at)
        assert dst.client().dbsize() == 200
    finally:
        dst.client().execute_command("REPLICAOF", "NO", "ONE")


def test_a_position_not_yet_reached_is_not_a_drain(clean, tmp_path):
    src, dst = clean
    eng = engine(clean, tmp_path)
    _replicate(src, dst)
    try:
        replid, _, offset = eng.src_lsn("0").rpartition(":")
        ahead = f"{replid}:{int(offset) + 10 ** 6}"
        assert eng.fence_wait("0", ahead, timeout=1) is False
        assert eng.position_reached(eng.log_position("dst", "0"),
                                    ahead) is False
    finally:
        dst.client().execute_command("REPLICAOF", "NO", "ONE")


def test_a_target_that_replicates_nothing_cannot_prove_a_drain(
        clean, tmp_path):
    eng = engine(clean, tmp_path)
    at = eng.src_lsn("0")
    assert eng.fence_wait("0", at, timeout=1) is None
    # positions of two streams do not compare
    assert eng.position_reached(at, "otherid:5") is None
    assert eng.position_reached("junk", at) is None
