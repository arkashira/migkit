"""Oracle's change reader on its own lane (backlog LT.C4, LT.C5): Oracle
Free in the VM, archiving its redo (`tests/test_oracle_is_a_whole_side.py`'s
sandbox).

* followed, stopped and resumed from the SCN its position file saved, no
  row is applied twice: the target's own trigger writes down every row
  inserted into it, so a change handed again after the resume - which the
  keyed apply would hide, deleting and inserting it again - shows as a
  second line;
* the redo is mined on a standby where one is named: the climb of where
  to mine takes it, and a named database that is not a physical standby
  of this source is refused in words, against the real server's role.
  The lane runs one Oracle Free, no Data Guard: mining on a real standby
  is not proved here.
"""
import json

import pytest

from migkit.config import Hop
from tests.conftest import needs_docker
from tests.test_oracle_is_a_whole_side import (  # noqa: F401 - fixtures
    DST, ORA_PORT, SRC, _at, _ep, _same, _stop, _tail, _why, alone, engine,
    fresh, ora, reports, sandbox)

pytestmark = needs_docker


@pytest.fixture(scope="module")
def lane(sandbox):
    """The sandbox, and migkit's sessions ended after this module: the
    next module's setup drops and makes the account again, refused while
    a session of it is open (ORA-01920)."""
    yield sandbox
    for pdb in (SRC, DST):
        ora("begin for s in (select sid, serial# from v$session where"
            " username = 'MIGKIT') loop execute immediate 'alter system"
            " kill session ''' || s.sid || ',' || s.serial# || '''"
            " immediate'; end loop; end;", pdb=pdb, user="sys",
            sysdba=True)


def _seen():
    return [r[0] for r in ora("select id from app.rs_seen order by id",
                              pdb=DST)]


def test_resumed_from_its_saved_scn_no_row_arrives_twice(lane, reports):
    alone("rs", "rs_seen")
    fresh("rs", "(id number(10) primary key, s varchar2(40))")
    _drop_seen = "begin execute immediate 'drop table app.rs_seen purge';" \
                 " exception when others then null; end;"
    ora(_drop_seen, pdb=DST)
    ora("create table app.rs_seen (id number(10))", pdb=DST)
    ora("create or replace trigger app.rs_seen_t after insert on app.rs"
        " for each row begin insert into app.rs_seen values (:new.id);"
        " end;", pdb=DST)
    eng = engine()
    token = reports / "ora" / "app" / "tail-token.json"
    eng.tail_start("app", token)
    said = []
    thread, ended = _tail(eng, token, said)
    try:
        for i in range(1, 41):
            ora("insert into app.rs values (:1, :2)", i, f"first {i}")
        at = _at(eng, "app", ended)
        assert eng.fence_wait("app", at, timeout=180) is True, \
            (said[-5:], _why(ended))
    finally:
        _stop(thread)
    saved = json.loads(token.read_text())
    assert saved, "no position was saved"
    # written while nothing follows: only these are owed on the resume
    ora("insert into app.rs select level + 40, 'while stopped' from dual"
        " connect by level <= 40")
    ora("update app.rs set s = 'moved on' where id = 1")
    thread, ended = _tail(eng, token, said)
    try:
        at = _at(eng, "app", ended)
        assert eng.fence_wait("app", at, timeout=180) is True, \
            (said[-5:], _why(ended))
        a, b = _same("rs")
        assert a == b and len(a) == 80, (len(a), len(b))
    finally:
        _stop(thread)
    seen = _seen()
    twice = sorted({i for i in seen if seen.count(i) > 1 and i != 1})
    assert not twice, f"rows applied twice after the resume: {twice}"
    assert sorted(set(seen)) == list(range(1, 81)), seen
    after = json.loads(token.read_text())
    assert after != saved, "the position did not move on the resume"


def test_a_named_standby_is_where_the_redo_is_mined_or_refused_in_words(
        lane):
    from migkit.engines import oracle_logminer as miner
    from migkit.engines.oracle import OracleEngine
    plain = OracleEngine(Hop(name="ora", engine="oracle", source=_ep(SRC),
                             target=_ep(DST), databases=["app"], options={}))
    got = miner.place(plain, "src", "app")
    assert got.rung.name != miner.STANDBY, got
    assert any(r.name == miner.STANDBY and "mine_standby" in why
               for r, why in got.passed), got.passed
    # the lane's one instance named as the standby: it is the primary
    named = OracleEngine(Hop(
        name="ora", engine="oracle",
        source=_ep(SRC, mine_standby=f"127.0.0.1:{ORA_PORT}/{SRC}"),
        target=_ep(DST), databases=["app"], options={}))
    got = miner.place(named, "src", "app")
    assert got.rung.name == miner.STANDBY, got
    m = miner.Miner(named, "src", "app")
    with pytest.raises(SystemExit) as e:
        m._open()
    m.close()
    assert "not a physical standby (it is PRIMARY)" in str(e.value), e.value
