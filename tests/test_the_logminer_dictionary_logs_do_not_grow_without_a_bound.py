"""Mining on migkit's own mining database adds every archived log since
the dictionary the read stands on. That set is bounded by what is
measured - this machine's memory and the reader's pace against the
source's archive switch - and where it is over, a newer dictionary is
written (only where the hop allows that change on the source), or the
read goes on and `assess` says it. Stand-ins only: no Oracle server runs
here."""
import datetime

import pytest

from migkit import sizing
from migkit.config import Endpoint, Hop
from migkit.engines import oracle_logminer as lm

MB = 2 ** 20


def _logs(n, size=100 * MB, every=60.0, first=1000):
    t0 = datetime.datetime(2026, 1, 1)
    return [{"thread": 1, "seq": i + 1, "first": first + i * 10,
             "next": first + (i + 1) * 10, "name": f"/a/{i}.arc",
             "bytes": size, "dict": "YES" if i == 0 else "NO",
             "at": t0 + datetime.timedelta(seconds=i * every)}
            for i in range(n)]


class _Conn:
    def __init__(self, ran):
        self.ran = ran

    def cursor(self):
        return self

    def execute(self, sql, *a):
        self.ran.append(sql)

    def close(self):
        self.ran.append("closed")


class _Eng:
    def __init__(self, newest=1000, can=True, break_build=False, **opts):
        self.hop = Hop(name="lm-dict", engine="oracle", source=Endpoint(),
                       target=Endpoint(), databases=["app"], options=opts)
        self.newest, self.can, self.ran = newest, can, []
        self.break_build = break_build

    def _rows(self, side, db, sql, args=()):
        if "max(first_change#)" in sql:
            return [(self.newest,)]
        if "all_tab_privs" in sql:
            return [(1 if self.can else 0,)]
        raise AssertionError(sql)

    def _dedicated(self, side, **kw):
        if self.break_build:
            raise RuntimeError("ORA-01017: the account was refused")
        return _Conn(self.ran)


@pytest.fixture(autouse=True)
def _clean(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    lm._SCANS.clear()
    lm._DICTIONARIES.clear()
    yield
    lm._SCANS.clear()
    lm._DICTIONARIES.clear()


def _host(monkeypatch, memory):
    monkeypatch.setattr(sizing, "host", lambda: {"cpus": 2, "memory":
                                                memory, "load": 0.1})


def test_the_bound_is_the_least_of_the_memory_and_the_reader_s_pace(
        monkeypatch):
    eng = _Eng()
    _host(monkeypatch, 64 * 1024 * MB)
    logs = _logs(5)
    # nothing timed yet: the memory alone bounds it
    assert lm.dictionary_bound(eng, "src", "app", logs) == (
        32 * 1024 * MB, "half this machine's free memory")
    # the reader shipped and mined 500 MB in 10 s: 50 MB a second, over
    # half of a 60 s switch
    lm._SCANS[(id(eng.hop), "src", "app")] = [(500 * MB, 10.0)]
    got, why = lm.dictionary_bound(eng, "src", "app", logs)
    assert got == 50 * MB * 30 and "archive switch" in why, (got, why)
    # a smaller machine: the memory binds again
    _host(monkeypatch, 2 * 1024 * MB)
    assert lm.dictionary_bound(eng, "src", "app", logs)[0] == 1024 * MB
    _host(monkeypatch, None)
    lm._SCANS.clear()
    assert lm.dictionary_bound(eng, "src", "app", logs)[0] is None


def test_logs_that_fit_are_kept_and_nothing_is_written(monkeypatch):
    eng = _Eng()
    _host(monkeypatch, 64 * 1024 * MB)
    got = lm.dictionary_way(eng, "src", "app", _logs(5), 1040)
    assert got.rung.name == lm.KEEP and eng.ran == []
    assert lm._DICTIONARIES[(id(eng.hop), "src", "app")]["way"] == lm.KEEP


def test_logs_over_the_bound_write_a_newer_dictionary_where_it_is_allowed(
        monkeypatch):
    eng = _Eng(source_changes={"dictionary": "yes"})
    _host(monkeypatch, 400 * MB)
    got = lm.dictionary_way(eng, "src", "app", _logs(6), 1040)
    assert got.rung.name == lm.REBUILD, got.reason
    assert eng.ran == ["begin dbms_logmnr_d.build(options =>"
                       " dbms_logmnr_d.store_in_redo_logs); end;",
                       "alter system archive log current", "closed"]
    said = lm._DICTIONARIES[(id(eng.hop), "src", "app")]
    assert said["way"] == lm.REBUILD and said["size"] == 600 * MB and \
        said["bound"] == 200 * MB


def test_a_newer_dictionary_already_written_is_waited_for_not_built_again(
        monkeypatch):
    eng = _Eng(newest=1050, source_changes={"dictionary": "yes"})
    _host(monkeypatch, 400 * MB)
    got = lm.dictionary_way(eng, "src", "app", _logs(6), 1040)
    assert got.rung.name == lm.KEEP and eng.ran == []


def test_without_the_hop_s_leave_the_read_goes_on_and_says_so(monkeypatch):
    eng = _Eng()
    _host(monkeypatch, 400 * MB)
    got = lm.dictionary_way(eng, "src", "app", _logs(6), 1040)
    assert got.rung.name == lm.CARRY and eng.ran == []
    assert "dictionary" in got.reason and "source_changes" in got.reason, \
        got.reason
    refused = _Eng(source_changes={"dictionary": "no"})
    assert lm.dictionary_way(refused, "src", "app", _logs(6),
                             1040).rung.name == lm.CARRY
    assert refused.ran == []
    # an account that may not run the build: a DBA's, never made here
    dba = _Eng(can=False, source_changes={"dictionary": "yes"})
    got = lm.dictionary_way(dba, "src", "app", _logs(6), 1040)
    assert got.rung.name == lm.CARRY and dba.ran == []


def test_a_build_that_fails_is_said_and_never_stops_the_read(monkeypatch):
    eng = _Eng(break_build=True, source_changes={"dictionary": "yes"})
    _host(monkeypatch, 400 * MB)
    got = lm.dictionary_way(eng, "src", "app", _logs(6), 1040)
    assert got.rung.name == lm.REBUILD
    note = lm._DICTIONARIES[(id(eng.hop), "src", "app")]["note"]
    assert "not built" in note and "refused" in note, note


def test_assess_says_the_logs_against_the_bound(monkeypatch):
    eng = _Eng()
    _host(monkeypatch, 400 * MB)
    rows = []
    lm._dictionary_assess(eng, "src", "app", _logs(6),
                          lambda *a: rows.append(a))
    level, item, detail = rows[0]
    assert level == "warn" and item == "the logs since the dictionary"
    assert "600 MB in 6 logs" in detail and "200 MB" in detail
    assert "kept over what the reader holds" in detail, detail
    rows.clear()
    _host(monkeypatch, 64 * 1024 * MB)
    lm._dictionary_assess(eng, "src", "app", _logs(6),
                          lambda *a: rows.append(a))
    assert rows[0][0] == "pass" and "kept as they are" in rows[0][2]
