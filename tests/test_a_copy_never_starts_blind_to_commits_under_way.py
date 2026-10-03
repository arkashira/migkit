"""A copy followed by the change tail never starts blind to the commits
under way.

MySQL writes a transaction to its binary log before the storage engine
commits it, so the position a copy starts the tail from can be past a
commit no read can see yet; `copy_point` waits out every session that is
committing. An account without `PROCESS` sees only its own session, and
that wait waited for nothing: a commit in the log before the position
and not yet visible was in neither the copy nor the tail - silently. It
is refused now, with the grant that lets it see, and the DBA's script
asks for that grant beside the replication ones.

Pure: the server's answers are given in-process.
"""
import pytest

from migkit import sourcechange
from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine


def _eng(seen, connected, committing=()):
    eng = MySQLEngine(Hop(name="h", engine="mysql", source=Endpoint(),
                          target=Endpoint()))
    asked = []

    def q(side, sql, args=None):
        asked.append(sql)
        if "count(*) from information_schema.processlist" in sql:
            return [(seen,)]
        if "Threads" in sql:
            return [("Threads_connected", str(connected))]
        if "like '%commit%'" in sql:
            return [(t,) for t in committing]
        return []
    eng._q = q
    return eng, asked


def test_an_account_that_sees_every_session_waits_them_out():
    eng, asked = _eng(seen=7, connected=5)
    assert eng.sees_every_session("src")
    eng._commits_under_way("src")
    assert any("like '%commit%'" in sql for sql in asked)


def test_an_account_that_sees_only_itself_is_refused_saying_the_grant():
    eng, _ = _eng(seen=1, connected=12)
    assert not eng.sees_every_session("src")
    with pytest.raises(SystemExit, match="grant process on"):
        eng._commits_under_way("src")


def test_a_server_where_it_is_the_only_session_needs_nothing_more():
    eng, _ = _eng(seen=1, connected=1)
    assert eng.sees_every_session("src")
    eng._commits_under_way("src")


def test_the_dbas_script_asks_for_the_grant():
    d = sourcechange.DEFINITIONS["process_grant"]
    assert d.engine == "mysql"

    class Ctx:
        who = "'app'@'%'"
    assert d.make(Ctx()) == ["grant process on *.* to 'app'@'%';"]
    assert "refused" in d.without
