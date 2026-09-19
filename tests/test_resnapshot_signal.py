"""Asking a running stream to read one table again.

Debezium can re-snapshot a single table while the stream keeps running -
an ad-hoc incremental snapshot. migkit wraps Debezium already; it just
never opened the channel. Measured before this existed, on the config
migkit generates:

    signal keys today: NONE

So a table that drifted mid-stream had one remedy: tear the pipeline down
and start the whole hop again.

**Which channel, and why it matters here.** Debezium's `source` channel
reads the request from a signalling table it expects to find *in the
source database*. migkit writes to the source nowhere - that invariant is
tested in `test_mojibake_repair.py` and is the reason the text repair
works on the target only - and creating a table there to enable a feature
would spend it. The broker is already in the generated compose file, so
the **Kafka channel** buys the same capability and keeps the source
read-only. That is the whole argument for the choice, and
`test_no_table_is_asked_for_on_the_source` is what holds it.
"""
import json

import pytest

from migkit.config import Endpoint, Hop


def _codegen(tmp_path, engine="postgres"):
    from migkit import movers
    hop = Hop(name="sig", engine=engine,
              source=Endpoint(host="127.0.0.1", port=15551, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=15552, user="u",
                              password="p"),
              databases=["appdb"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    out = movers.stream_codegen(hop, ["appdb"], engine)
    return json.loads((out / "source-connector.json").read_text())["config"]


@pytest.mark.parametrize("engine", ["postgres", "mysql"])
def test_the_signal_channel_is_in_the_generated_config(tmp_path, engine):
    cfg = _codegen(tmp_path, engine)
    assert cfg["signal.enabled.channels"] == "kafka", cfg
    assert cfg["signal.kafka.topic"] == "migkit-sig-signal", cfg
    assert cfg["signal.kafka.bootstrap.servers"] == "redpanda:9092", cfg
    # the broker the compose file already runs, not a second one
    assert "redpanda" in cfg["signal.kafka.bootstrap.servers"]


def test_no_table_is_asked_for_on_the_source(tmp_path):
    """The reason the Kafka channel was chosen. `signal.data.collection`
    is the key that names a table in the source database; if it ever
    appears here, migkit has started asking to write to a source."""
    cfg = _codegen(tmp_path)
    assert "signal.data.collection" not in cfg, cfg
    assert "source" not in cfg["signal.enabled.channels"].split(","), cfg


def test_the_signal_names_the_connector_that_should_act_on_it(tmp_path):
    """The signal topic is shared. Debezium matches on the key against its
    own `topic.prefix`, so a key that drifts from the prefix means the
    request is read by nobody and silently does nothing."""
    from migkit import movers
    topic, key, value = movers.resnapshot_message("sig", ["public.orders"])
    cfg = _codegen(tmp_path)
    assert key == cfg["topic.prefix"], (key, cfg["topic.prefix"])
    assert topic == cfg["signal.kafka.topic"], (topic, cfg)


def test_the_request_is_the_shape_debezium_reads():
    from migkit import movers
    _, _, value = movers.resnapshot_message(
        "sig", ["public.orders", "public.lines"])
    assert value["type"] == "execute-snapshot", value
    assert value["data"]["data-collections"] == ["public.orders",
                                                 "public.lines"], value
    # BLOCKING by default, and that is measured rather than preferred -
    # see test_incremental_is_not_the_default_and_the_reason_is_measured
    assert value["data"]["type"] == "BLOCKING", value
    assert json.loads(json.dumps(value)) == value

    _, _, inc = movers.resnapshot_message("sig", ["t"], kind="incremental")
    assert inc["data"]["type"] == "INCREMENTAL", inc


def test_incremental_is_not_the_default_and_the_reason_is_measured():
    """Run against Debezium 3.9 on the pipeline migkit generates, with the
    Kafka signal channel and no signalling table on the source:

        INCREMENTAL  Requested 'INCREMENTAL' snapshot of data collections
                     '[public.orders]'
                     Action execute-snapshot failed
                     DebeziumException: Incremental snapshot is not
                       properly configured, either sinalling data
                       collection is not provided ...
                     topic high watermark unchanged at 50

        BLOCKING     Finished exporting 50 records for table
                       'public.orders'
                     snapshot=BLOCKING snapshot_completed=true
                     topic high watermark 50 -> 100

    An incremental snapshot brackets each chunk with watermark rows
    written into a signalling table *in the source database*. migkit does
    not write to a source, so incremental is not available to it by
    default - not a preference, a consequence.
    """
    from migkit import movers
    _, _, default = movers.resnapshot_message("h", ["public.t"])
    assert default["data"]["type"] == "BLOCKING", default
