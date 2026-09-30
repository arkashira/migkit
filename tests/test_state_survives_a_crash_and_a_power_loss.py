"""What migkit keeps between runs is written one durable way and read with
one policy for a torn file per kind (audit 2026-10-01, ST1, ST9, ST15,
SEC10).

About twenty writers, three fsyncs in the tree and none of a directory;
four behaviours for one torn file - a traceback, a silent start over, a
clear stop, `{}`. `migkit.durable` is the one writer (a private temp
file, fsynced, renamed, the directory fsynced; the mode from the first
byte) and the one reader.
"""
import json
import os
import stat

import pytest

from migkit import durable


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_a_write_is_whole_private_and_leaves_nothing_beside_it(tmp_path):
    where = tmp_path / "hop" / "db" / "tail-token.json"
    durable.write_json(where, {"token": 42})
    assert json.loads(where.read_text()) == {"token": 42}
    assert _mode(where) == 0o600
    # the directories it made are this user's only
    assert _mode(where.parent) == 0o700 and _mode(where.parent.parent) == \
        0o700
    # an older file readable by everyone is replaced by a private one
    old = tmp_path / "move.json"
    old.write_text("{}")
    os.chmod(old, 0o644)
    durable.write_atomic(old, b'{"done": 1}')
    assert old.read_bytes() == b'{"done": 1}' and _mode(old) == 0o600
    assert sorted(p.name for p in tmp_path.iterdir()) == ["hop", "move.json"]


def test_a_write_that_fails_leaves_the_old_file_and_no_temp(tmp_path,
                                                            monkeypatch):
    where = tmp_path / "checkpoint.json"
    durable.write_json(where, {"n": 1})

    def refused(*a):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(durable.os, "replace", refused)
    with pytest.raises(OSError):
        durable.write_json(where, {"n": 2})
    assert json.loads(where.read_text()) == {"n": 1}
    assert [p.name for p in tmp_path.iterdir()] == ["checkpoint.json"]


def test_the_file_and_its_directory_reach_the_disk(tmp_path, monkeypatch):
    synced = []
    real = os.fsync

    def fsync(fd):
        synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
        return real(fd)
    monkeypatch.setattr(durable.os, "fsync", fsync)
    durable.write_atomic(tmp_path / "a.json", "{}")
    # the file's bytes first, then the directory's entry for the rename
    assert synced == [False, True], synced


def test_full_flushes_through_the_drives_cache_where_asked(tmp_path,
                                                           monkeypatch):
    import fcntl
    asked = []
    monkeypatch.setattr(fcntl, "F_FULLFSYNC", 51, raising=False)
    monkeypatch.setattr(fcntl, "fcntl",
                        lambda fd, op, *a: asked.append(op) or 0)
    durable.write_atomic(tmp_path / "a.json", "{}", full=True)
    assert asked == [51]
    asked.clear()
    durable.write_atomic(tmp_path / "b.json", "{}")
    assert asked == []


@pytest.mark.parametrize("kind", ["position", "journal", "state"])
def test_a_torn_position_journal_or_state_stops_naming_the_file(tmp_path,
                                                                kind):
    where = tmp_path / "tail-token.json"
    assert durable.read_state(where, kind) is None      # nothing yet
    for torn in ('{"token": 4', ""):
        where.write_text(torn)
        with pytest.raises(durable.Torn) as e:
            durable.read_state(where, kind)
        assert str(where) in str(e.value), e.value
    where.write_text('{"token": 4}')
    assert durable.read_state(where, kind) == {"token": 4}


def test_a_torn_cache_reads_as_absent(tmp_path):
    where = tmp_path / "catalogue.json"
    where.write_text('{"tables": [')
    assert durable.read_state(where, "cache") is None
    with pytest.raises(ValueError):
        durable.read_state(where, "whatever")


def test_a_line_appended_after_a_torn_one_is_readable(tmp_path):
    journal = tmp_path / "undo" / "x.repair.jsonl"
    assert durable.append_line(journal, json.dumps({"n": 1})) == 0
    # a crash cut the next write
    with open(journal, "a") as f:
        f.write('{"n": 2, "rows"')
    assert durable.read_lines(journal, "journal") == [{"n": 1}]
    dropped = durable.append_line(journal, json.dumps({"n": 3}))
    assert dropped == len('{"n": 2, "rows"')
    assert durable.read_lines(journal, "journal") == [{"n": 1}, {"n": 3}]
    assert journal.read_text() == '{"n": 1}\n{"n": 3}\n'
    assert _mode(journal) == 0o600
    with pytest.raises(ValueError):
        durable.append_line(journal, "two\nlines")


def test_a_broken_line_inside_a_journal_stops_it(tmp_path):
    journal = tmp_path / "x.jsonl"
    journal.write_text('{"n": 1}\nnot a record\n{"n": 3}\n')
    with pytest.raises(durable.Torn) as e:
        durable.read_lines(journal, "journal")
    assert "line 2" in str(e.value), e.value
    assert durable.read_lines(journal, "cache") == [{"n": 1}, {"n": 3}]


def test_a_directory_left_open_is_closed(tmp_path):
    d = tmp_path / "reports" / "hop"
    d.mkdir(parents=True)
    os.chmod(d, 0o755)
    durable.private_dir(d)
    assert _mode(d) == 0o700


def test_the_checkpoint_and_the_debug_log_are_written_this_way(tmp_path):
    from migkit.checkpoint import Checkpoint
    from migkit.wording import DebugLog
    path = tmp_path / "cp" / "checkpoint.json"
    cp = Checkpoint(str(path))
    cp.begin("t", "x", [(0, 10)])
    cp.record("t", 0, 10, 5, 7)
    assert _mode(path) == 0o600
    assert Checkpoint(str(path)).total("t") == (5, "7")
    # a torn checkpoint is a cache: started over, never a traceback
    path.write_text('{"format_version": 1, "tab')
    assert Checkpoint(str(path)).total("t") == (0, "0")
    log = tmp_path / "run" / "commands.log"
    log.parent.mkdir()
    log.write_text("2026-10-01T00:00:00+00:00 $ cut sho")
    DebugLog(log).command(["x", "--password=CHANGE_ME"])
    lines = log.read_text().splitlines()
    assert len(lines) == 1 and lines[0].endswith("$ x --password=***"), lines
    assert "+00:00 $" in lines[0], lines
