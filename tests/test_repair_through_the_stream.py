"""When a stream is running, the repair goes through it.

`migkit sync --kind rows` writes the differing rows straight onto the
target. That is right for a hop that was loaded once and is now still.
It is wrong for a hop with a connector running: the connector is already
writing those keys, migkit would be writing the same keys, and the row
that survives is whichever landed last. A repair that can quietly lose to
the thing it is repairing is not a repair.

Debezium can be asked to read the table again - the ad-hoc snapshot opened
in `test_resnapshot_signal.py` - which applies the same fix through the
writer that is already there. So the presence of a generated pipeline is
what decides the shape of the plan, and these tests pin both shapes.

There is deliberately no undo. The connector re-reads the source and the
sink upserts what arrives; the target ends up holding what the source
holds, and nothing of the target's own is being preserved to put back.
Writing an undo file that restored rows the source no longer has would be
a lie with a filename.
"""
import json

import pytest

from migkit.config import Endpoint, Hop


def _engine(tmp_path, engine="postgres"):
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="st", engine=engine,
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="p"),
              databases=["appdb"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    cls = PostgresEngine if engine == "postgres" else MySQLEngine
    return cls(hop)


def _drill(tmp_path, table="public.orders"):
    (tmp_path / f"data-{table}.missing").write_text('["1"]\n["2"]\n')
    (tmp_path / f"data-{table}.changed").write_text('["7"]\n')


def _pipeline(tmp_path):
    out = tmp_path / "stream"
    out.mkdir(exist_ok=True)
    (out / "source-connector.json").write_text(json.dumps(
        {"name": "migkit-st-source",
         "config": {"topic.prefix": "migkit-st",
                    "signal.enabled.channels": "kafka"}}))
    (out / "docker-compose.yml").write_text("services: {}\n")
    return out


def test_without_a_stream_the_plan_is_still_row_by_row(tmp_path):
    """The control. Nothing about the existing repair changes for a hop
    that is not streaming."""
    _drill(tmp_path)
    actions = _engine(tmp_path)._rows_plan("appdb", "from the source")
    assert [a.kind for a in actions] == ["rows"], actions
    assert "undo file" in actions[0].note, actions[0].note


def test_with_a_stream_the_plan_goes_through_the_connector(tmp_path):
    _drill(tmp_path)
    _pipeline(tmp_path)
    actions = _engine(tmp_path)._rows_plan("appdb", "from the source")
    assert [a.kind for a in actions] == ["resnapshot"], actions
    note = actions[0].note
    # the counts the check found are still what the operator is shown
    assert "2 missing, 1 changed, 0 extra" in note, note
    # and the reason it is not writing the rows itself
    assert "race the connector" in note, note
    # the cost, where the person paying it will read it
    assert "pauses while the table is read" in note, note


def test_the_stream_shaped_action_carries_no_undo(tmp_path):
    """Not an oversight. There is nothing of the target's to put back."""
    _drill(tmp_path)
    _pipeline(tmp_path)
    action = _engine(tmp_path)._rows_plan("appdb", "x")[0]
    assert action.undo == [], action.undo
    assert action.statements == ["execute-snapshot public.orders"], action


@pytest.mark.parametrize("engine", ["postgres", "mysql"])
def test_both_streaming_engines_dispatch_it(tmp_path, engine, monkeypatch):
    """One implementation on the base, reached from both. Asserted rather
    than assumed - `apply` is written per engine and an unknown kind is
    supposed to raise, so a missing branch would look like a typo."""
    _drill(tmp_path)
    _pipeline(tmp_path)
    eng = _engine(tmp_path, engine)
    sent = {}

    def fake(out, hop_name, tables, kind="blocking", log=None):
        sent.update(out=out, hop_name=hop_name, tables=tables, kind=kind)
        return "migkit-st-signal"

    import migkit.movers as movers
    monkeypatch.setattr(movers, "send_resnapshot", fake)
    action = eng._rows_plan("appdb", "x")[0]
    eng.apply("appdb", action)
    assert sent["tables"] == ["public.orders"], sent
    assert sent["hop_name"] == "st", sent
    assert sent["kind"] == "blocking", sent


def test_an_unknown_kind_still_refuses(tmp_path):
    from migkit.engines.base import RepairAction
    eng = _engine(tmp_path)
    with pytest.raises(RuntimeError, match="no way to apply"):
        eng.apply("appdb", RepairAction("appdb", "invented", ["x"], [], ""))


def test_applying_without_a_pipeline_says_how_to_get_one(tmp_path):
    """The plan and the apply can be separated by a teardown. Failing with
    the command that fixes it beats failing with a missing file."""
    from migkit.engines.base import RepairAction
    eng = _engine(tmp_path)
    with pytest.raises(RuntimeError, match="no streaming pipeline"):
        eng.apply("appdb", RepairAction("appdb.public.orders", "resnapshot",
                                        ["execute-snapshot public.orders"],
                                        [], ""))


def test_the_signal_goes_through_compose_not_an_exposed_port(tmp_path,
                                                               monkeypatch):
    """The broker publishes no port to the host - only the connector's
    REST API is exposed. Opening 9092 so the tool could connect directly
    would widen the pipeline for migkit's convenience."""
    from migkit import movers
    seen = {}

    class P:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        seen["input"] = kw.get("input")
        return P()

    monkeypatch.setattr(movers, "run", fake_run)
    out = _pipeline(tmp_path)
    topic = movers.send_resnapshot(out, "st", ["public.orders"])

    assert topic == "migkit-st-signal", topic
    assert seen["cmd"][:3] == ["docker", "compose", "-f"], seen["cmd"]
    assert "exec" in seen["cmd"] and "redpanda" in seen["cmd"], seen["cmd"]
    key, _, payload = seen["input"].partition("\t")
    assert key == "migkit-st", seen["input"]
    assert json.loads(payload)["data"]["type"] == "BLOCKING", payload


def test_a_broker_that_cannot_be_reached_is_an_error_not_a_success(
        tmp_path, monkeypatch):
    """The worst outcome here is reporting a repair that was never sent."""
    from migkit import movers

    class P:
        returncode = 1
        stderr = "no such service: redpanda"

    monkeypatch.setattr(movers, "run", lambda cmd, **kw: P())
    with pytest.raises(RuntimeError, match="could not reach the signal"):
        movers.send_resnapshot(_pipeline(tmp_path), "st", ["t"])
