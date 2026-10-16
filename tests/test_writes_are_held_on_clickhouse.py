"""A ClickHouse source's writes are held at cutover on a server of the
test lanes by its rights (`ch_grants`): the default user may manage users
there (the lane's profile grants it), so the decision engine climbs to the
way of rights from that privilege read off the server; an application
user's insert is refused while held, a hold whose process was killed
outright is found and lifted exactly as taken - its grant read back as it
was - and the application's writes flow again."""
import pytest

from migkit import freeze
from tests import holdkit, servers, waits
from tests.conftest import needs_docker

PROVES = [("cutover.freeze", "clickhouse")]

pytestmark = needs_docker

pair = servers.fixture("clickhouse")

APP_PW = "CHANGE_ME-app"


def _app():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=pair_host[0], port=pair_host[1], username="app",
        password=APP_PW)


pair_host = [None, None]


def _grants(src):
    return sorted(tuple(r) for r in src.sql(
        "select access_type, database, is_partial_revoke from"
        " system.grants where user_name = 'app'"))


def test_a_hold_whose_process_was_killed_is_found_and_lifted(pair, tmp_path):
    from migkit.engines.clickhouse import ClickHouseEngine
    src = pair.src
    pair_host[:] = [src.host, src.port]
    src.sql("drop user if exists app")
    src.sql("drop database if exists app")
    src.sql("create database app")
    src.sql("create table app.t (id UInt32) engine = MergeTree order by id")
    src.sql(f"create user app identified by '{APP_PW}'")
    src.sql("grant select, insert on app.* to app")
    before = _grants(src)
    hop = holdkit.hop_of("clickhouse", src, pair.dst, ["app"], tmp_path)
    eng = ClickHouseEngine(hop)
    try:
        got = freeze.choose(hop, eng, ["app"], "database")
        assert got.rung is not None and got.rung.name == "ch_grants", \
            got.reason
        _app().command("insert into app.t values (1)")

        def refuses():
            with pytest.raises(Exception) as e:
                _app().command("insert into app.t values (2)")
            assert "ACCESS_DENIED" in str(e.value) or \
                "Not enough privileges" in str(e.value), e.value

        def accepts():
            waits.until(lambda: _app().command(
                "insert into app.t values (3)") or True, 30,
                "the application's insert is taken again")
            assert _grants(src) == before, "the grant is back as it was"
        holdkit.lifted_after_a_crash(hop, eng, "ch_grants", ["app"],
                                     accepts, refuses)
        ids = sorted(r[0] for r in src.sql("select id from app.t"))
        assert ids == [1, 3]
    finally:
        src.sql("drop user if exists app")
        src.sql("drop database if exists app")
