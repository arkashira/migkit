"""Restore points, the run state another machine reads and a file target's
own index are kept whole (audit 2026-10-01, ST5, ST6, ST15, ST22, ST23,
K10, SEC10).

* The state backend is read one way: `backend: S3` kept the lease on this
  machine while restore points went to the bucket.
* A restore point is put in place by a rename under a name of its own -
  it was the older point removed and the copy made after, and two in one
  second were one - and what leaves the working directory is sealed by
  the hop's `at_rest` where it names recipients.
* The tail's files go to the run state as one record with the position's
  generation (`state.TailMirror`): written only over a lower one, put in
  place only where higher than this machine's.
* Two checks of one database keep each other's tables in the checkpoint,
  and two runs keeping a part beside one file each keep theirs: every
  read-modify-write is held under the file's own lock, and no file is
  written through a temporary name another writer shares.
* A copybook target's batch is named in its index before its records: a
  crash between left no entry, and the batch was appended again.
* A hop's report directory is its user's only.
"""
import json
import os
import stat

import pytest

from migkit import checkpoint, durable, state, tailctl, tailtables
from migkit.config import Endpoint, Hop


def _hop(options=None, name="rp"):
    return Hop(name=name, engine="postgres", source=Endpoint(),
               target=Endpoint(), databases=["d"],
               options=dict(options or {}))


def test_the_state_backend_is_read_one_way():
    assert state.backend(_hop({"state": {"backend": "S3 "}})) == "s3"
    assert state.backend(_hop()) == "local"
    for read in (state.get_store, state.run_state):
        with pytest.raises(SystemExit) as e:
            read(_hop({"state": {"backend": "nfs"}}))
        assert "nfs" in str(e.value), read


def test_a_restore_point_lands_whole_under_a_name_of_its_own(tmp_path,
                                                             monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    store = state.LocalStore(_hop(), {"mirror": str(tmp_path / "mirror")})
    kept = []
    for i in range(2):
        point = state._Point(store, "d", "20261001-120000", "")
        (point.dir / "undo-t.sql").write_text(f"insert {i};")
        kept.append(point.commit("2026-10-01 12:00:00"))
    # two in one second are two
    assert kept == ["20261001-120000", "20261001-120000-2"]
    root = tmp_path / "reports" / "rp" / "d" / "state"
    assert (root / kept[0] / "undo-t.sql").read_text() == "insert 0;"
    assert (root / kept[1] / "undo-t.sql").read_text() == "insert 1;"
    assert [m["ts"] for m in store.list("d")] == kept
    # a copy a crash left half made is not a point
    (root / ".20261001-130000.partial").mkdir()
    assert [m["ts"] for m in store.list("d")] == kept
    assert stat.S_IMODE((root / kept[0] / "undo-t.sql").stat().st_mode) \
        == 0o600
    assert stat.S_IMODE(root.stat().st_mode) == 0o700


def test_a_restore_point_leaves_sealed_to_the_hops_recipients(tmp_path,
                                                              monkeypatch):
    import pyrage

    from migkit import evidence
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    monkeypatch.delenv("MIGKIT_STATE_KEY", raising=False)
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop({"at_rest": {"recipients": [str(who.to_public())]}})
    store = state.LocalStore(hop, {"mirror": str(tmp_path / "mirror")})
    point = store.new_point("d")
    (point.dir / "undo-t.sql").write_text("insert into t values ('v');")
    (point.dir / "dst-files" / "t").mkdir(parents=True)
    (point.dir / "dst-files" / "t" / "part-0").write_bytes(b"row 'v'")
    ts = point.commit("now")
    tar = tmp_path / "mirror" / f"d-{ts}.tar.gz"
    raw = tar.read_bytes()
    assert raw.startswith(evidence.AGE) and b"insert" not in raw
    # the copy kept here is sealed file by file too - what the point is
    # apart, so points are listed without a key
    local = tmp_path / "reports" / "rp" / "d" / "state" / ts
    for f in (local / "undo-t.sql", local / "dst-files" / "t" / "part-0"):
        assert f.read_bytes().startswith(evidence.AGE), f
    assert json.loads((local / "meta.json").read_text())["ts"] == ts
    assert [m["ts"] for m in store.list("d")] == [ts]
    # read through a directory of the run's own, opened
    got = store.fetch("d", ts)
    assert got != local and stat.S_IMODE(got.stat().st_mode) == 0o700
    assert (got / "undo-t.sql").read_text() == "insert into t values ('v');"
    assert (got / "dst-files" / "t" / "part-0").read_bytes() == b"row 'v'"
    # read back from the mirror alone: kept here sealed again
    import shutil
    shutil.rmtree(local)
    got = store.fetch("d", ts)
    assert (got / "undo-t.sql").read_text() == "insert into t values ('v');"
    assert (local / "undo-t.sql").read_bytes().startswith(evidence.AGE)
    # a hop that names no recipients keeps and reads its points as before
    plain = state.LocalStore(_hop(name="clear"),
                             {"mirror": str(tmp_path / "mirror2")})
    point = plain.new_point("d")
    (point.dir / "undo-t.sql").write_text("insert 1;")
    ts = point.commit("now")
    assert plain.fetch("d", ts) == \
        tmp_path / "reports" / "clear" / "d" / "state" / ts


class _Shared:
    """A store machines share, in memory: records written over the
    version read, blobs by digest."""
    blobs = True

    def __init__(self):
        self.records, self.versions, self.kept = {}, {}, {}

    @staticmethod
    def _k(path):
        # under the hop's reports, the same on every machine
        from pathlib import Path
        return "/".join(Path(path).parts[-2:])

    def read_record(self, path, sealed=False):
        k = self._k(path)
        return self.records.get(k), self.versions.get(k)

    def write_record(self, path, record, version, sealed=False):
        k = self._k(path)
        if self.versions.get(k) != version:
            raise state.Taken()
        self.records[k] = json.loads(json.dumps(record))
        self.versions[k] = (version or 0) + 1

    def put_blob(self, data):
        import hashlib
        d = hashlib.sha256(data).hexdigest()
        self.kept[d] = data
        return d

    def get_blob(self, digest):
        return self.kept[digest]

    def get(self, path):
        return None


def _tail_at(where, gen, parked=False):
    tailctl.keep(where / tailctl.TOKEN, {"id": "s1", "token": f"0/{gen}",
                                         "gen": gen})
    st = tailtables.Store.of(where)
    st.reset("s1", gen)
    with st.batch() as s:
        s.put("tables", "a", {"state": "parked" if parked else "live",
                              "name": "a"})
        if parked:
            s.add_frame("a", f'"0/{gen}"', [{"op": "insert", "table": "a",
                                             "key": {"id": gen}}], [])


def test_the_tails_files_go_whole_and_the_higher_generation_wins(
        tmp_path):
    remote = _Shared()
    one, two = tmp_path / "one" / "d", tmp_path / "two" / "d"
    for w in (one, two):
        w.mkdir(parents=True)
    # this machine's tail at generation 7, a table parked with a change
    # set aside: all of it goes, in one record
    _tail_at(one, 7, parked=True)
    m = state.TailMirror(remote, [one / n for n in ("tail-token.json",)])
    m.push(force=True)
    rec = next(iter(remote.records.values()))
    assert rec["gen"] == 7 and tailtables.STORE in rec["blobs"]
    # another machine with an older position of its own takes the newer
    _tail_at(two, 3)
    state.TailMirror(remote, [two / "tail-token.json"]).fetch()
    assert tailctl.position(two / tailctl.TOKEN)["gen"] == 7
    st = tailtables.Store.of(two)
    assert st.tables()["a"]["state"] == "parked"
    assert tailtables.Spool(st, "a").changes()[0]["key"] == {"id": 7}
    # and an older record never goes over a newer one
    _tail_at(one, 2)
    state.TailMirror(remote, [one / "tail-token.json"]).push(force=True)
    assert next(iter(remote.records.values()))["gen"] == 7
    # nor beats a newer position here
    _tail_at(two, 9)
    state.TailMirror(remote, [two / "tail-token.json"]).fetch()
    assert tailctl.position(two / tailctl.TOKEN)["gen"] == 9


def test_two_checks_of_one_database_keep_each_others_tables(tmp_path):
    path = str(tmp_path / "checkpoint.json")
    a, b = checkpoint.Checkpoint(path), checkpoint.Checkpoint(path)
    a.begin("t1", "x", [(0, 10)])
    a.record("t1", 0, 10, 5, 7)
    b.begin("t2", "x", [(0, 10)])
    b.record("t2", 0, 10, 3, 4)
    a.record("t1", 10, 20, 1, 1)
    got = checkpoint.Checkpoint(path)
    assert got.total("t1") == (6, "8") and got.total("t2") == (3, "4")
    b.clear("t2")
    got = checkpoint.Checkpoint(path)
    assert got.total("t2") == (0, "0") and got.total("t1") == (6, "8")


def test_a_copybook_batch_is_named_before_its_records(tmp_path,
                                                      monkeypatch):
    from tests.test_copybook_files_as_a_side_of_a_pair import (
        _cols, _engine, _files)
    src, dst = _files(tmp_path)
    eng = _engine(tmp_path)
    cols = _cols(eng, "account")
    rows = [r for b in eng.neutral_batches("src", "bank", "account", cols)
            for r in b]
    eng.neutral_create("dst", "bank", "account", [(n, c, ())
                                                  for n, c in cols])
    eng.neutral_write("dst", "bank", "account", cols, rows[:1])
    index = dst / "account.dat.migkit.json"
    # stopped dead once the records are on the disk, before the batch's
    # entry is finished
    real = durable.write_json
    calls = []

    def dies(path, value, *a, **kw):
        calls.append(value)
        if len(calls) == 2:
            raise SystemExit("power lost")
        return real(path, value, *a, **kw)
    monkeypatch.setattr(durable, "write_json", dies)
    with pytest.raises(SystemExit):
        eng.neutral_write("dst", "bank", "account", cols, rows[1:])
    monkeypatch.setattr(durable, "write_json", real)
    entry = json.loads(index.read_text())[-1]
    assert entry[2] is None, entry
    # written again after the restart: the unfinished batch cut first, so
    # its records are there once
    eng.neutral_write("dst", "bank", "account", cols, rows[1:])
    assert (dst / "account.dat").read_bytes() == \
        (src / "account.dat").read_bytes()
    assert all(e[2] is not None for e in json.loads(index.read_text()))
    # a torn index is a stop, never "no batch written"
    index.write_text('[["a", 0, ')
    with pytest.raises(durable.Torn) as e:
        eng.neutral_write("dst", "bank", "account", cols, rows[1:])
    assert str(index) in str(e.value)


def test_a_hops_report_directory_is_its_users_only(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop(name="private")
    for d in (hop.report_dir(), hop.report_dir("d")):
        assert stat.S_IMODE(os.stat(d).st_mode) == 0o700, d


def test_two_runs_keeping_a_part_beside_one_position_keep_both(tmp_path):
    """The kept ways were read, added to and written back under a lock
    only the run's own threads took, through a temporary file both runs
    named alike: two runs beside one position kept the part written last
    (ST23)."""
    import subprocess
    import sys

    import migkit
    from migkit import decide
    where = tmp_path / "move.json"
    where.write_text("{}")
    code = ("import sys\nfrom migkit import decide\nfor i in range(40):\n"
            "    decide.remember(sys.argv[1], f'{sys.argv[2]}-{i}', 'r')\n")
    env = dict(os.environ, PYTHONPATH=os.path.dirname(
        os.path.dirname(migkit.__file__)))
    runs = [subprocess.Popen([sys.executable, "-c", code, str(where), w],
                             env=env) for w in "ab"]
    assert [r.wait(120) for r in runs] == [0, 0]
    works = {f"{w}-{i}" for w in "ab" for i in range(40)}
    assert {w for w in works if decide.remembered(str(where), w) == "r"} \
        == works


def test_the_ways_kept_beside_a_position_stop_where_torn(tmp_path):
    """Read as none, a restart climbed another rung than the one its work
    stood on (ST9)."""
    from migkit import decide
    where = tmp_path / "move.json"
    where.write_text("{}")
    decide.remember(str(where), "t", "fast")
    assert decide.remembered(str(where), "t") == "fast"
    ways = tmp_path / "move-ways.json"
    ways.write_text('{"t": "fa')
    for read in (lambda: decide.remembered(str(where), "t"),
                 lambda: decide.remember(str(where), "u", "slow")):
        with pytest.raises(durable.Torn) as e:
            read()
        assert str(ways) in str(e.value)


def test_a_change_of_a_kept_file_reads_what_is_there_first(tmp_path):
    path = tmp_path / "load.json"
    for n in range(3):
        durable.change_json(path, lambda got, n=n: got.append(n),
                            empty=list)
    assert json.loads(path.read_text()) == [0, 1, 2]
    # a cache torn by an earlier version's write starts empty, never a
    # traceback; another shape is the empty one too
    path.write_text("[0, 1")
    assert durable.change_json(path, lambda got: got.append(9),
                               empty=list) == [9]
    path.write_text('{"a": 1}')
    assert durable.change_json(path, lambda got: got.append(8),
                               empty=list) == [8]
    # a position is never read past torn
    path.write_text("[0, 1")
    with pytest.raises(durable.Torn):
        durable.change_json(path, lambda got: None, kind="position",
                            empty=list)


def test_no_file_is_written_through_a_temporary_name_another_shares():
    import pathlib
    import re

    import migkit
    root = pathlib.Path(migkit.__file__).parent
    fixed = re.compile(r'with_suffix\([^)]*"\.tmp"\)|\+ "\.tmp"')
    hits = [f"{p.relative_to(root)}:{n}" for p in sorted(root.rglob("*.py"))
            for n, line in enumerate(p.read_text().splitlines(), 1)
            if fixed.search(line)]
    assert hits == []
