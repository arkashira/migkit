"""YugabyteDB as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): its logical replication followed on the release and slots it says, its own LSN. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import yugabyte


@pytest.fixture
def eng(monkeypatch):
    got = yugabyte.YugabyteEngine.__new__(yugabyte.YugabyteEngine)
    got.hop = types.SimpleNamespace(
        engine="yugabytedb", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "yugabyte-aeon" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="yugabytedb")
    assert users._speaking(hop).engine == "postgres"


def _server(version, slots=(10, 1), refuse=False):
    def scalar(side, db, sql):
        if "version()" in sql:
            return version
        if refuse:
            raise RuntimeError("replication commands are off\nmore")
        return slots[0] if "max_replication_slots" in sql else slots[1]
    return scalar


def test_it_is_followed_from_the_release_that_serves_it(eng):
    eng._scalar = _server("PostgreSQL 11.2-YB-2.20.1.0-b0")
    got = eng.follow_way("app")
    assert got.path is None and "before 2024.1.1" in got.reason
    eng._scalar = _server("PostgreSQL 15.2-YB-2024.2.0.0-b0")
    assert eng.follow_way("app").path == "logical"
    eng._scalar = _server("PostgreSQL 15.2-YB-2024.2.0.0-b0", (2, 2))
    assert "no replication slot is free" in eng.follow_way("app").reason
    eng._scalar = _server("PostgreSQL 15.2-YB-2024.2.0.0-b0", refuse=True)
    assert "do not answer: replication commands are off" in \
        eng.follow_way("app").reason
    with pytest.raises(SystemExit, match="no way to follow"):
        eng.tail_start("app", None)


def test_its_position_is_its_hybrid_time_lsn(eng):
    eng._scalar = lambda side, db, sql: (5 << 32) + 255
    assert eng.src_lsn("app") == "5/FF"
