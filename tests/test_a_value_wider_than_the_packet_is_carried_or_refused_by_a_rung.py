"""A value wider than the target's packet: carried by raising the packet
for the run where the hop allows it and the target's account may, the old
value put back after - also after a run killed before its end, from the
leftovers entry - or refused at readiness, before the copy, with the
statement the operator can run (`decide.packet_way`). On the MySQL
sandbox pair (lane 7)."""
import pytest

from migkit import decide, leftovers
from migkit.engines.mysql import MySQLEngine
from migkit.hopkeys import HOP_OPTIONS

from . import servers
from .test_autonomous_scenarios import Side, _conf

rowmeta = servers.fixture("mysql-rowmeta")

SMALL = 4 * 1024 * 1024


def _hop(tmp_path, side, allow):
    from migkit import config
    extra = {"options": "{target_settings: {max_allowed_packet: %s}}"
             % ("yes" if allow else "no")}
    conf = _conf(tmp_path, side, **extra)
    return config.load_hops(str(conf))["t"]


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    from migkit import config
    monkeypatch.setattr(config, "REPORTS", tmp_path / "reports")


@pytest.fixture
def side(rowmeta):
    s = Side("mysql", rowmeta.src, rowmeta.dst)
    s.seed(rows=10, lob=8 << 20)
    s.sql("dst", f"set global max_allowed_packet = {SMALL}")
    yield s
    s.sql("dst", "set global max_allowed_packet = 67108864")


def _packet(side):
    return int(side.sql("dst", "select @@global.max_allowed_packet"))


def _item(eng):
    return next(i for i in eng._packet_items() if "larger than" in
                i["detail"])


def test_the_key_is_one_the_hop_may_say():
    assert "target_settings" in HOP_OPTIONS


def test_the_rungs_read_the_facts():
    base = {"widest": 8 << 20, "exact": True, "packet": SMALL,
            "slack": 65536, "want": 18 << 20, "ceiling": 1 << 30,
            "authorized": True, "may set": True}
    assert decide.packet_way(base).path == "raised"
    assert decide.packet_way({**base, "authorized": False}).path == "refused"
    assert decide.packet_way({**base, "may set": False}).path == "refused"
    assert decide.packet_way({**base, "exact": False}).path == "refused"
    assert decide.packet_way({**base, "packet": 64 << 20}).path == "kept"


def test_raised_for_the_run_and_put_back(side, tmp_path):
    hop = _hop(tmp_path, side, allow=True)
    eng = MySQLEngine(hop)
    item = _item(eng)
    assert item["level"] == "warn" and "raises it to" in item["detail"]
    with leftovers.settings_held(hop, eng, side.db, print) as held:
        assert held and held[0]["old"] == SMALL
        assert _packet(side) == held[0]["new"] >= 2 * (8 << 20)
        assert leftovers.settings_left(hop)
    assert _packet(side) == SMALL
    assert not leftovers.settings_left(hop)


def test_a_killed_run_has_its_packet_put_back_by_the_next(side, tmp_path):
    hop = _hop(tmp_path, side, allow=True)
    eng = MySQLEngine(hop)
    held = leftovers.settings_held(hop, eng, side.db)
    held.__enter__()  # the run killed inside its block: no exit is run
    assert _packet(side) > SMALL
    # the leftovers command, or the next start, puts it back
    back = leftovers.put_back_settings(hop, MySQLEngine(hop))
    assert [b["old"] for b in back] == [SMALL]
    assert _packet(side) == SMALL and not leftovers.settings_left(hop)


def test_refused_before_the_copy_where_the_hop_does_not_allow(side,
                                                              tmp_path):
    hop = _hop(tmp_path, side, allow=False)
    eng = MySQLEngine(hop)
    item = _item(eng)
    assert item["level"] == "fail", item
    assert "SET GLOBAL max_allowed_packet = " in item["detail"]
    assert side.db in item["scope"] and " o " in f" {item['detail']}"
    assert eng.settings_for_run(side.db) == []
    with leftovers.settings_held(hop, eng, side.db):
        assert _packet(side) == SMALL


def test_refused_where_the_account_may_not_set_it(side, tmp_path,
                                                  monkeypatch):
    hop = _hop(tmp_path, side, allow=True)
    eng = MySQLEngine(hop)
    monkeypatch.setattr(eng, "_may_set_globals", lambda: False)
    item = _item(eng)
    assert item["level"] == "fail" and "SYSTEM_VARIABLES_ADMIN" in \
        item["detail"], item
