"""Rows a DDL changes with no row event for them are never passed over.

`ALTER TABLE t TRUNCATE PARTITION p0`, `DROP PARTITION` and `EXCHANGE
PARTITION ... WITH TABLE t2` remove or move rows, and the binlog carries
only the statement. Each was read as `('db', 't', 'alter', None)` - the
tail went on, the target kept the rows. Now each is a `reload` of every
table it touches: copied again where an emptied table would be, stopped
where the hop's `ddl` stops one - and never emptied in place, which would
lose the rest of the table's rows.
"""
import pytest

from migkit import canon, logddl
from tests.test_a_tail_s_tables_move_on_their_own import _tables


def test_a_partition_emptied_is_copied_again_never_emptied_whole(tmp_path):
    t = _tables(tmp_path, "apply")
    states = t.begin()
    (d, table, kind, to), = logddl.mysql(
        "ALTER TABLE a TRUNCATE PARTITION p0", "d")
    assert kind == "reload"
    # no ("truncate", ...) for the target to be emptied: copied again
    assert t.ddl(states, canon.ddl(table, kind), "0/1") is None
    assert states["a"]["state"] == "copying", states["a"]
    assert "partition" in states["a"]["why"]
    # the table beside it goes on as it was
    assert states["b"]["state"] == "live"


def test_an_exchange_copies_both_tables_again(tmp_path):
    t = _tables(tmp_path, "park")
    states = t.begin()
    for d, table, kind, _ in logddl.mysql(
            "alter table a exchange partition p1 with table b", "d"):
        assert kind == "reload"
        t.ddl(states, canon.ddl(table, kind), "0/1")
    assert states["a"]["state"] == states["b"]["state"] == "copying"


def test_under_stop_rows_gone_stop_the_tail(tmp_path):
    t = _tables(tmp_path)
    states = t.begin()
    with pytest.raises(SystemExit) as e:
        t.ddl(states, canon.ddl("a", "reload"), "0/1")
    assert "no row event" in str(e.value) and "ddl: apply" in str(e.value)


def test_reload_is_a_kind_a_change_log_may_carry():
    assert canon.ddl("t", "reload")["kind"] == "reload"
    with pytest.raises(ValueError):
        canon.ddl("t", "partition")
