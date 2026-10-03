"""Every tail goes round one loop (audit 2026-10-01, ST8, ST10, ST11,
ST14, K14, SP10).

Five loops behaved five ways: the cross-engine tail stopped when asked,
ended at its slot's end, tried a lost connection again and took SIGTERM
as ctrl-c; the loop of the engines that look again (ClickHouse,
OpenSearch, Cassandra) and Kafka's did not end at a slot, MongoDB's did
none of it. `tailctl.run_loop` is the one now, held here without a
server: the stop, the slot's end, an end once caught up, a connection
lost, a lease taken over, SIGTERM, the beat, an idle wait that grows and
comes back, and the position saved only after the batch is on the target
- proved by stopping a process dead between the two.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from migkit import lease, schedule, setaside, tailctl
from migkit.config import Endpoint, Hop


def _hop(where, **options):
    hop = Hop(name="loop", engine="postgres", source=Endpoint(),
              target=Endpoint(), databases=["d"], options=options)
    hop.report_dir = lambda db=None: where
    return hop


class _Looks:
    """A source of `n` batches a look, `last` of them, then nothing."""

    def __init__(self, where, last=3, n=5, stop_at=None, raise_at=None):
        self.where, self.last, self.n = where, last, n
        self.stop_at, self.raise_at = stop_at, raise_at
        self.calls = []

    def __call__(self, token):
        self.calls.append(token)
        k = len(self.calls)
        if self.raise_at and k in self.raise_at:
            raise ConnectionError(self.raise_at[k])
        if self.stop_at == k:
            tailctl.stop(self.where)
        if token < self.last:
            return token + 1, self.n, False
        return token, 0, True


def test_it_stops_between_batches_with_the_position_saved(tmp_path):
    looks = _Looks(tmp_path, last=10, stop_at=2)
    saved, said = [], []
    seen, why = tailctl.run_loop(tmp_path, "d", looks, said.append,
                                 "changes", hop=_hop(tmp_path), token=0,
                                 save=saved.append)
    assert why == "asked" and seen == 10 and saved == [1, 2]
    assert any("stopped as asked after 10 changes" in s for s in said)
    # a stop asked of this run is not asked of the next
    assert not (tmp_path / tailctl.STOP).exists()
    assert not tailctl.alive(tmp_path)


def test_a_slots_run_ends_at_its_end_whatever_the_hop_says(tmp_path,
                                                            monkeypatch):
    monkeypatch.setenv(schedule.SLOT_ENDS, str(time.time() + 0.3))
    looks = _Looks(tmp_path, last=10 ** 9, n=1)
    seen, why = tailctl.run_loop(tmp_path, "d", looks, lambda m: None,
                                 "changes", token=0, save=lambda a: None,
                                 hop=_hop(tmp_path,
                                          follow_until="stopped"))
    assert why == "slot" and seen > 0


def test_a_scheduled_run_ends_once_caught_up(tmp_path, monkeypatch):
    monkeypatch.setenv(schedule.SLOT_ENDS, str(time.time() + 600))
    saved = []
    seen, why = tailctl.run_loop(tmp_path, "d", _Looks(tmp_path), print,
                                 "changes", token=0, save=saved.append,
                                 hop=_hop(tmp_path))
    # caught up: the source's position compared to where the tail is, or
    # an empty read where it cannot be
    assert why == "caught up" and seen == 15 and saved == [1, 2, 3]
    assert tailctl.until(_hop(tmp_path)) == "caught_up"
    monkeypatch.delenv(schedule.SLOT_ENDS)
    assert tailctl.until(_hop(tmp_path)) == "stopped"
    # and where the position says it has not reached the fence, it goes on
    reached = iter([False, False, True])
    seen, why = tailctl.run_loop(
        tmp_path, "d", _Looks(tmp_path, last=0), print, "changes", token=0,
        save=lambda a: None, reached=lambda at: next(reached),
        hop=_hop(tmp_path, follow_until="caught_up"))
    assert why == "caught up"


def test_a_lost_connection_is_read_again_from_the_saved_position(
        tmp_path, monkeypatch):
    naps = []
    monkeypatch.setattr(tailctl, "_nap", lambda where, s, *a: naps.append(s))
    looks = _Looks(tmp_path, last=3,
                   raise_at={2: "connection refused (password CHANGE_ME)"})
    said = []
    ep = Endpoint(password="CHANGE_ME")
    hop = Hop(name="loop", engine="postgres", source=ep, target=ep,
              options={"follow_until": "caught_up"})
    hop.report_dir = lambda db=None: tmp_path
    seen, why = tailctl.run_loop(tmp_path, "d", looks, said.append,
                                 "changes", hop=hop, token=0,
                                 save=lambda a: None,
                                 lost=lambda e, at: 1,
                                 secrets=("CHANGE_ME",))
    assert why == "caught up"
    # the look after the lost one began from where `lost` said
    assert looks.calls[:3] == [0, 1, 1], looks.calls
    assert naps and naps[0] == 2
    line = next(s for s in said if "lost a connection" in s)
    assert "CHANGE_ME" not in line
    # anything else stops the follow
    with pytest.raises(ValueError):
        tailctl.run_loop(tmp_path, "d",
                         lambda at: (_ for _ in ()).throw(ValueError("x")),
                         print, "changes", hop=_hop(tmp_path), token=0)
    assert tailctl.stopped(tmp_path)["kind"] == "ValueError"


def test_a_lease_taken_over_stops_the_writing_between_batches(tmp_path):
    def look(at):
        if at == 2:
            # another machine took the tail's lease while this one was not
            # heard from
            rec = json.loads((tmp_path / tailctl.PID).read_text())
            (tmp_path / tailctl.PID).write_text(json.dumps(
                {**rec, "holder": "far:1:x", "host": "far.example.com",
                 "expires": time.time() + 60}))
            running = [r for r in _RUNNING if r]
            for r in running:
                r.lease.renewed = time.monotonic() - 10 ** 6
        return at + 1, 1, False
    _RUNNING.clear()
    real = tailctl.Running.__enter__

    def enter(self):
        got = real(self)
        _RUNNING.append(got)
        return got
    tailctl.Running.__enter__ = enter
    try:
        with pytest.raises(lease.Lost):
            tailctl.run_loop(tmp_path, "d", look, print, "changes",
                             hop=_hop(tmp_path), token=0,
                             save=lambda a: None)
    finally:
        tailctl.Running.__enter__ = real
    # its end did not take the other's lease away
    assert json.loads((tmp_path / tailctl.PID).read_text())["host"] == \
        "far.example.com"


_RUNNING = []


def test_sigterm_ends_the_loop_as_ctrl_c_with_the_position_saved(tmp_path):
    saved = []

    def look(at):
        if at == 2:
            os.kill(os.getpid(), signal.SIGTERM)
        return at + 1, 1, False
    before = signal.getsignal(signal.SIGTERM)
    seen, why = tailctl.run_loop(tmp_path, "d", look, print, "changes",
                                 hop=_hop(tmp_path), token=0,
                                 save=saved.append)
    # the batch the signal came in is not saved: it is read again
    assert why == "interrupted" and saved == [1, 2], saved
    # a person's stop: no error kept, and the handler put back
    assert tailctl.stopped(tmp_path) is None
    assert signal.getsignal(signal.SIGTERM) == before


def test_an_idle_wait_grows_and_comes_back_at_the_next_change(tmp_path,
                                                              monkeypatch):
    naps = []
    monkeypatch.setattr(tailctl, "_nap", lambda where, s, *a: naps.append(s))
    plan = iter([0, 0, 0, 0, 0, 0, 0, 3, 0, 0])

    def look(at):
        n = next(plan, None)
        if n is None:
            tailctl.stop(tmp_path)
            n = 0
        return at + n, n, True
    tailctl.run_loop(tmp_path, "d", look, print, "changes",
                     hop=_hop(tmp_path), token=0, save=lambda a: None)
    assert naps[:7] == [0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.0], naps
    # a change: the next wait is the shortest again
    assert naps[7] == 0.02, naps


def test_a_second_tail_of_one_database_is_refused(tmp_path):
    first = tailctl.Running(tmp_path).__enter__()
    try:
        with pytest.raises(lease.Held) as e:
            tailctl.Running(tmp_path).__enter__()
        assert "change tail" in str(e.value)
        assert f"process {os.getpid()}" in str(e.value)
        assert tailctl.alive(tmp_path)
    finally:
        first.__exit__()
    assert not tailctl.alive(tmp_path)


def test_one_answer_to_is_it_still_running(tmp_path):
    here = socket.gethostname()
    dead = subprocess.run([sys.executable, "-c", "import os;"
                           " print(os.getpid())"], capture_output=True,
                          text=True).stdout.strip()
    path = tmp_path / tailctl.PID
    # this machine's: its process decides, whatever the term
    path.write_text(json.dumps({"holder": "x", "host": here,
                                "pid": os.getpid(), "expires": 0}))
    assert tailctl.alive(tmp_path)
    path.write_text(json.dumps({"holder": "x", "host": here, "pid": dead,
                                "expires": time.time() + 600}))
    assert not tailctl.alive(tmp_path)
    # another machine's: its term, renewed by its heartbeat - a pod
    # replaced long ago is not running for good
    path.write_text(json.dumps({"holder": "x", "host": "pod-1.example.com",
                                "pid": 7, "expires": time.time() + 30}))
    assert tailctl.alive(tmp_path)
    path.write_text(json.dumps({"holder": "x", "host": "pod-1.example.com",
                                "pid": 7, "expires": time.time() - 1}))
    assert not tailctl.alive(tmp_path)
    # an earlier version's "host pid": its beat's age is its term
    path.write_text("pod-2.example.com 9")
    (tmp_path / tailctl.BEAT).write_text(json.dumps(
        {"at": time.time() - 10 * lease.term(), "caught_up_at": 0}))
    assert not tailctl.alive(tmp_path)
    (tmp_path / tailctl.BEAT).write_text(json.dumps(
        {"at": time.time(), "caught_up_at": 0}))
    assert tailctl.alive(tmp_path)
    assert lease.liveness({"holder": "x", "host": here, "pid": dead}) \
        is False


def test_a_set_aside_record_lives_as_long_as_its_loads_lease(tmp_path):
    hop = _hop(tmp_path)
    rec = tmp_path / "dropped-triggers.pod-3.example.com.42.1.json"
    rec.write_text(json.dumps({"trg": "create trigger ..."}))
    aside = setaside.SetAside(hop, "d", "dropped-triggers")
    # its load holds the hop's lease, renewed: running
    (tmp_path / "lease.json").write_text(json.dumps(
        {"holder": "pod-3:42:x", "host": "pod-3.example.com", "pid": 42,
         "expires": time.time() + 30}))
    assert aside.records()[0][1] is True
    # the lease lapsed - the pod was replaced: put back by the next load
    (tmp_path / "lease.json").write_text(json.dumps(
        {"holder": "pod-3:42:x", "host": "pod-3.example.com", "pid": 42,
         "expires": time.time() - 5}))
    assert aside.records()[0][1] is False
    defs, files = aside.left_behind()
    assert defs == {"trg": "create trigger ..."} and files == [rec]
    # no lease names it at all: it stopped
    (tmp_path / "lease.json").unlink()
    assert aside.records()[0][1] is False


_FOLLOWER = textwrap.dedent("""
    import json, sys
    from pathlib import Path
    from migkit import tailctl
    from migkit.config import Endpoint, Hop

    where = Path(sys.argv[1])
    target = where / "target.json"

    class Eng:
        hop = Hop(name="f", engine="opensearch", source=Endpoint(),
                  target=Endpoint(),
                  options={"follow_until": "caught_up"})
        hop.report_dir = lambda db=None: where

        def stream_identity(self, side, db):
            return None

        def position_lost(self, side, db, token):
            return None

    def look(at):
        # the source's documents 0..29, five a look, put on the target by
        # key - and each look counted where it lands, to see it twice
        n = at["n"]
        if n >= 30:
            return at, 0, True
        got = json.loads(target.read_text()) if target.exists() else {}
        for k in range(n, n + 5):
            got.setdefault(str(k), 0)
            got[str(k)] += 1
        target.write_text(json.dumps(got))
        return {"n": n + 5}, 5, False
    tailctl.follow(Eng(), "d", True, where / "tail-token.json", print,
                   "documents", look, lambda at: "")
""")


def _follow(where, failpoint=None):
    from pathlib import Path
    env = dict(os.environ)
    env.pop("MIGKIT_FAILPOINT", None)
    # the migkit these tests run, not another one installed
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    if failpoint:
        env["MIGKIT_FAILPOINT"] = failpoint
    return subprocess.run([sys.executable, "-c", _FOLLOWER, str(where)],
                          env=env, capture_output=True, text=True,
                          timeout=120)


def test_the_position_is_saved_only_after_the_batch_is_on_the_target(
        tmp_path):
    """Stopped dead between the target taking a batch and its position
    being saved, the follow goes on from before the batch - the batch on
    the target twice by key, which a write by key converges on - and
    never from after a batch the target does not have."""
    (tmp_path / "tail-token.json").write_text(json.dumps({"token":
                                                          {"n": 0}}))
    got = _follow(tmp_path, "tail.applied:3:exit")
    assert got.returncode == 137, got.stderr
    on_target = json.loads((tmp_path / "target.json").read_text())
    saved = tailctl.saved(tmp_path / "tail-token.json")
    # the third batch is on the target; its position was not saved
    assert len(on_target) == 15 and saved == {"n": 10}, (on_target, saved)
    # a dead process's lease is not waited for
    got = _follow(tmp_path)
    assert got.returncode == 0, got.stderr
    on_target = json.loads((tmp_path / "target.json").read_text())
    assert sorted(map(int, on_target)) == list(range(30))
    # the batch whose position was lost went again, nothing else did
    assert {k for k, n in on_target.items() if n > 1} == \
        {"10", "11", "12", "13", "14"}
    assert tailctl.saved(tmp_path / "tail-token.json") == {"n": 30}
    # stopped dead after the position: nothing goes twice
    (tmp_path / "target.json").unlink()
    (tmp_path / "tail-token.json").write_text(json.dumps({"token":
                                                          {"n": 0}}))
    assert _follow(tmp_path, "tail.saved:2:exit").returncode == 137
    assert _follow(tmp_path).returncode == 0
    on_target = json.loads((tmp_path / "target.json").read_text())
    assert set(on_target.values()) == {1} and len(on_target) == 30
