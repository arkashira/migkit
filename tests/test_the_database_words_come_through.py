"""A failed copy says what the database said, not the program's last lines.

Measured with a target whose disk was 150 MB and a copy of about 150 MB:
the move stopped with the last 500 characters of the copy program's log -
`Sub-process exited with code 12`, `Some COPY worker process(es) have
exited with error, see above` - and the lines above them, the only ones
that said what happened, were cut off. The target then stopped altogether
(`PANIC: could not write to file "pg_wal/..."`), and the next move's log
ended the same way over a refused connection.

The lines below are those logs, as the program wrote them.
"""
from migkit import wording

DISK_FULL = """\
2026-09-25 08:08:26.394 28900 ERROR  pgsql.c:3317              [TARGET 114] [53100] ERROR:  could not extend file "base/16384/16404" with FileFallocate(): No space left on device
2026-09-25 08:08:26.395 28900 ERROR  pgsql.c:3324              [TARGET 114] HINT:  Check free disk space.
2026-09-25 08:08:26.396 28900 ERROR  pgsql.c:3324              [TARGET 114] CONTEXT:  COPY big, line 175845
2026-09-25 08:08:26.397 28900 ERROR  pgsql.c:3331              [TARGET 114] Context: Failed to copy data to target
2026-09-25 08:08:26.398 28900 ERROR  table-data.c:840          Failed to copy data for table with oid 16385 and part number 0, see above for details
2026-09-25 08:08:26.503 28895 ERROR  copydb.c:778              Sub-process 28900 exited with code 12
2026-09-25 08:08:26.503 28895 ERROR  table-data.c:387          Some COPY worker process(es) have exited with error, see above for details
"""

GONE = """\
2026-09-25 08:10:41.254 29119 ERROR  pgsql.c:463               Connection to database failed: connection to server at "127.0.0.1", port 15790 failed: Connection refused
2026-09-25 08:10:41.256 29119 ERROR  pgsql.c:467               \tIs the server running on that host and accepting TCP/IP connections?
2026-09-25 08:10:41.260 29119 FATAL  table-data.c:789          Failed to set our GUC settings on the target connection, see above for details
2026-09-25 08:10:41.292 29114 ERROR  copydb.c:778              Sub-process 29119 exited with code 12
"""


def test_a_full_disk_is_said_in_the_databases_words():
    said = wording.database_words(DISK_FULL)
    assert said.startswith("the target ran out of disk space: "), said
    assert "No space left on device (SQLSTATE 53100)" in said, said
    assert "hint: Check free disk space." in said, said
    assert "context: COPY big, line 175845" in said, said
    # the program's own bookkeeping is not the database's words
    assert "Sub-process" not in said and "pgsql.c" not in said, said
    assert "Failed to copy data to target" not in said, said


def test_the_old_tail_had_lost_it():
    """The control: what the operator was shown before."""
    assert "No space left on device" not in DISK_FULL[-500:]


def test_a_server_that_is_gone_is_named_by_address():
    said = wording.database_words(GONE)
    assert said == ("the database at 127.0.0.1:15790 could not be reached:"
                    " Connection refused"), said


def test_nothing_recognised_leaves_the_tail_to_speak():
    assert wording.database_words("Thread 4 - ERROR 1062: Duplicate entry"
                                  " '1' for key 'PRIMARY'") == ""
    assert "disk ran out of space" in wording.database_words(
        "write failed: No space left on device")


def test_a_move_that_fails_says_it(monkeypatch, tmp_path):
    """Through the one place every program is run from."""
    import pytest

    from migkit import movers
    script = tmp_path / "fails.sh"
    script.write_text("#!/bin/sh\ncat <<'X' >&2\n" + DISK_FULL + "X\nexit 12\n")
    script.chmod(0o755)
    with pytest.raises(RuntimeError) as e:
        movers._sh([str(script)])
    assert str(e.value).startswith("the target ran out of disk space"), \
        e.value
