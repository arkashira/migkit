"""Live end-to-end Debezium supervision — NOT run in the normal suite: it
pulls the ~1GB debezium/connect + redpanda images and boots a broker. Gated
behind MIGKIT_LIVE_E2E=1 so it is ready to run in a dedicated CI job / on a VM
with the infra, without slowing everyday testing.

    MIGKIT_LIVE_E2E=1 pytest tests/test_live_debezium.py -q -s
"""
import os
import time

import pytest

from tests.conftest import needs_docker

pytestmark = [
    needs_docker,
    pytest.mark.skipif(os.environ.get("MIGKIT_LIVE_E2E") != "1",
                       reason="set MIGKIT_LIVE_E2E=1 to run (pulls ~1GB images)"),
]


def test_debezium_supervision_launches_and_streams(pg_pair, tmp_path):
    """Generate configs, launch the stack, register connectors, insert on
    source, and prove delta verify sees the streamed rows on target."""
    import migkit.config as cfg
    import migkit.movers as movers
    from migkit.config import Endpoint, Hop
    from tests.conftest import psql

    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="dbz", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    psql(pg_pair["src"], "create table t (id int primary key, v int)")
    psql(pg_pair["dst"], "create table t (id int primary key, v int)")

    out = movers.debezium_codegen(hop, ["postgres"], "postgres")
    try:
        movers.debezium_up(out, print)
        assert movers.debezium_wait(timeout=240, log=print), "Connect not up"
        movers.debezium_register(out, log=print)
        # let the snapshot + streaming settle, then write a row on source
        time.sleep(15)
        psql(pg_pair["src"], "insert into t values (1, 100)")
        deadline = time.time() + 60
        got = None
        while time.time() < deadline:
            r = psql(pg_pair["dst"], "select v from t where id = 1").stdout.strip()
            if r == "100":
                got = r
                break
            time.sleep(3)
        assert got == "100", "row did not stream to target via debezium"
        status = movers.debezium_status(f"migkit-{hop.name}-source")
        assert "RUNNING" in status
    finally:
        movers.debezium_down(out, print)
