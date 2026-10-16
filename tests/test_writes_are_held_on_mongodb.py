"""A MongoDB source of the test lanes checks no accounts (the lane runs a
replica set without authorization), so no way of holding its writes holds
every writer: the decision engine reads that off the server and refuses
every rung with that fact as its reason, and nothing is recorded to lift.
The hold taken, killed and lifted on a server that checks accounts is
owed: the lane's profile needs authorization and a replica set key."""
from migkit import freeze, leftovers
from tests import holdkit, servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("mongo")


def test_a_server_that_checks_no_accounts_is_not_held_and_says_why(
        pair, tmp_path):
    from migkit.engines.mongodb import MongoEngine
    hop = holdkit.hop_of("mongodb", pair.src, pair.dst, ["held"], tmp_path)
    eng = MongoEngine(hop)
    got = freeze.choose(hop, eng, ["held"])
    assert got.rung is None
    assert "checks no accounts (authorization off)" in got.reason, \
        got.reason
    assert "auth" in got.asked, got.asked
    assert leftovers.holds(hop, ["held"]) == []
