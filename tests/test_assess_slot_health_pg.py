"""An inactive/abandoned replication slot pins WAL and can silently fill the
source disk - a source outage mid-migration. assess must flag it (source-side
operational health), not just count slots."""
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="a", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    return PostgresEngine(hop)


def test_inactive_slot_flagged_by_assess(pg_pair):
    # a logical slot with no consumer is inactive and retains WAL
    psql(pg_pair["src"],
         "select pg_create_logical_replication_slot('deadslot','test_decoding')")
    try:
        items = _engine(pg_pair).assess()
    finally:
        psql(pg_pair["src"], "select pg_drop_replication_slot('deadslot')")

    hit = [i for i in items if "deadslot" in i["item"]]
    assert hit, [i for i in items if i["scope"] == "instance"]
    assert hit[0]["level"] == "warn"
    assert "inactive" in hit[0]["detail"].lower()
