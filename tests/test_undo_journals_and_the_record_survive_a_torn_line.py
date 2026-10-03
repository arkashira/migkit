"""What undoing a change needs, and the record of what migkit did, survive
a torn line and are sealed where the hop asks (audit 2026-10-01, ST9,
ST12, ST13, ST15, ST16, SEC9, SEC15).

* A repair's record is on the disk before its first statement lands, and
  a last line a crash cut is left out instead of stopping every rollback
  of it; a line broken anywhere else stops, naming the file.
* Under `at_rest` the record a line at a time sealed: the sealer flushes
  (it had no `flush`, and every repair of such a hop stopped) and every
  way a file is opened to be written seals or is refused.
* The hop's record (`changelog.jsonl`): a torn last line is taken off and
  said in the chain, not glued to the next entry for good; its opening
  stretch is bound by the first hashed entry; a rotated part's head is
  accepted as the start of a chain.
* freeze, jobs and cutover records are written whole, and one that
  cannot be read whole stops, naming it, instead of reading as nothing
  done - so a thaw or an undo is never of nothing.
"""
import hashlib
import json

import pytest

from migkit import audit, durable, evidence, repairlog


def _sealed_root(tmp_path, monkeypatch):
    import pyrage
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    root = tmp_path / "reports" / "h"
    root.mkdir(parents=True)

    class Hop:
        options = {"at_rest": {"recipients": [str(who.to_public())]}}
    return evidence.report_path(Hop(), root)


def test_a_repair_record_is_on_the_disk_and_reads_past_a_torn_line(
        tmp_path, monkeypatch):
    import os
    synced = []
    real = os.fsync
    monkeypatch.setattr(os, "fsync",
                        lambda fd: synced.append(fd) or real(fd))
    rec = repairlog.Record(tmp_path / "undo", "public.t", "canon",
                           [("id", "integer")], ["id"])
    rec.planned("write", [1], None, ["1"])
    rec.planned("delete", [2], ["2"], None)
    before = len(synced)
    rec.ready()
    # the record reached the disk before a statement landed
    assert len(synced) == before + 1, synced
    rec.applied(1)
    rec.close()
    with open(rec.path, "a") as f:
        f.write('{"applied": 2')
    head, stmts, upto = repairlog.read(rec.path)
    assert head["table"] == "public.t" and len(stmts) == 2 and upto == 1
    assert repairlog.stopped(tmp_path / "undo") == [(rec.path, 1, 2)]
    # broken in the middle: not a guess
    text = rec.path.read_text().replace('"op": "delete"', 'XX', 1)
    rec.path.write_text(text)
    with pytest.raises(durable.Torn) as e:
        repairlog.read(rec.path)
    assert str(rec.path) in str(e.value) and "line 3" in str(e.value)


def test_under_at_rest_the_record_is_sealed_and_flushes(tmp_path,
                                                         monkeypatch):
    root = _sealed_root(tmp_path, monkeypatch)
    rec = repairlog.Record(root / "undo", "public.t", "canon",
                           [("id", "integer")], ["id"])
    rec.planned("write", [1], None, ["secret-value"])
    rec.ready()
    rec.applied(1)
    rec.close()
    raw = rec.path.read_bytes()
    assert b"secret-value" not in raw and raw.startswith(evidence.LINE.encode())
    head, stmts, upto = repairlog.read(rec.path)
    assert stmts[0]["after"] == ["secret-value"] and upto == 1
    # a torn sealed line: left out as a torn plain one is
    with open(rec.path, "ab") as f:
        f.write(b"age:AAAA")
    assert repairlog.read(rec.path)[2] == 1


def test_every_way_a_value_file_is_opened_seals_or_is_refused(
        tmp_path, monkeypatch):
    root = _sealed_root(tmp_path, monkeypatch)
    with (root / "undo.sql").open("w") as f:
        f.write("insert into t values ('secret-value');\n")
    with (root / "data-t.bin").open("wb") as f:
        f.write(b"secret-value")
    for name in ("undo.sql", "data-t.bin"):
        raw = (root / name).read_bytes()
        assert raw.startswith(evidence.AGE) and b"secret" not in raw, name
    assert (root / "data-t.bin").read_bytes() == b"secret-value"
    with (root / "data-t.bin").open("rb") as f:
        assert f.read() == b"secret-value"
    for mode in ("ab", "r+", "w+"):
        with pytest.raises(SystemExit) as e:
            (root / "data-t.bin").open(mode)
        assert "at_rest" in str(e.value), mode
    # a file migkit writes whole through `durable` is sealed too
    durable.write_json(root / "conflicts-parked.json", {"rows": "secret"})
    assert (root / "conflicts-parked.json").read_bytes().startswith(
        evidence.AGE)
    assert durable.read_state(root / "conflicts-parked.json", "state") == \
        {"rows": "secret"}
    durable.append_lines(root / "conflicts.jsonl", ['{"k": "secret"}'])
    assert b"secret" not in (root / "conflicts.jsonl").read_bytes()
    assert durable.read_lines(root / "conflicts.jsonl", "journal") == \
        [{"k": "secret"}]
    # a file that holds no values is left plain
    durable.write_json(root / "lease.json", {"holder": "x"})
    assert json.loads((root / "lease.json").read_text()) == {"holder": "x"}


def test_a_torn_record_line_is_taken_off_and_said_in_the_chain(tmp_path):
    path = tmp_path / "changelog.jsonl"
    for i in range(3):
        audit.append(path, {"op": "move", "table": f"t{i}"})
    cut = '{"at": "2026-10-01", "op": "re'
    with open(path, "a") as f:
        f.write(cut)
    torn = []
    # a crash, not tampering: the chain up to it is whole
    assert audit.verify(path, torn) == (3, 3, "") and torn == [len(cut)]
    head = audit.append(path, {"op": "after"})
    n, chained, broken = audit.verify(path)
    assert (n, chained, broken) == (5, 5, "")
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert lines[3]["op"] == "torn" and lines[3]["bytes"] == len(cut)
    assert lines[3]["sha256"] == hashlib.sha256(cut.encode()).hexdigest()
    assert head == lines[-1]["hash"] == audit.head(path)


def test_the_opening_stretch_is_bound_by_the_first_hashed_entry(tmp_path):
    path = tmp_path / "changelog.jsonl"
    path.write_text(json.dumps({"at": "2026-01-01", "op": "old"}) + "\n")
    for i in range(3):
        audit.append(path, {"op": "move", "table": f"t{i}"})
    assert audit.verify(path) == (4, 3, "")
    lines = path.read_text().splitlines()
    # the entry before the chain edited
    edited = json.dumps({"at": "2026-01-01", "op": "older"})
    path.write_text("\n".join([edited] + lines[1:]) + "\n")
    assert "do not bind" in audit.verify(path)[2] or \
        "does not bind" in audit.verify(path)[2]
    # the first hashed entries stripped of their hashes and edited
    first = json.loads(lines[1])
    first.pop("hash")
    first["table"] = "elsewhere"
    path.write_text("\n".join([lines[0], json.dumps(first)] + lines[2:])
                    + "\n")
    broken = audit.verify(path)[2]
    assert broken and "begins the chain" in broken, broken


def test_a_rotated_records_head_is_where_a_chain_begins(tmp_path):
    from migkit import runlog
    path = tmp_path / "changelog.jsonl"
    for i in range(3):
        audit.append(path, {"op": "move", "table": f"t{i}"})
    runlog.rotate(path, chained=True)
    audit.append(path, {"op": "after"})
    assert audit.verify(path) == (2, 2, "")
    assert runlog.verify_parts(path)[2] == ""


@pytest.mark.parametrize("module,read,path", [
    ("freeze", lambda m, hop: m.state(hop, "d"), "d/freeze.json"),
    ("jobs", lambda m, hop: m.recorded(hop), "cutover-jobs.json"),
    ("cutover", lambda m, hop: m.record(hop), "cutover.json")])
def test_a_record_of_a_change_that_cannot_be_read_stops(tmp_path, module,
                                                        read, path):
    import importlib
    m = importlib.import_module(f"migkit.{module}")

    class Hop:
        options = {}

        def report_dir(self, db=""):
            d = tmp_path / db if db else tmp_path
            d.mkdir(parents=True, exist_ok=True)
            return d
    hop = Hop()
    assert read(m, hop) in (None, [])
    (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / path).write_text('{"phase": "fro')
    with pytest.raises(durable.Torn) as e:
        read(m, hop)
    assert str(tmp_path / path) in str(e.value)
