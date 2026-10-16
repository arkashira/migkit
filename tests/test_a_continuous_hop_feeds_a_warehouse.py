"""A continuous hop into a warehouse (backlog LT.P3) and the provider's
zero-ETL integration beside it (S4).

The tail's next read is sized from its measured rate and the landing's
measured start and cost a row (`decide.warehouse_landing`), a late row
never overwrites a newer version of its key, and the choice between the
provider's integration and migkit's own hop keeps every promise the hop
asks before it weighs lag. No server is reached: the warehouse's reads and
the provider's API are stand-ins answering in their shape.
"""
import types

import pytest

from migkit import cloudapi, decide
from migkit.engines import warehouse


# ---- the batch a landing keeps up with --------------------------------------

def test_a_batch_is_the_fewest_rows_whose_landing_keeps_up():
    rows, interval, lag = decide.batch_for(rate=1000, startup=2.0,
                                           per_row=0.0001)
    # 2s start, 0.1 ms a row, 1000 rows/s: the landing takes half of
    # the time the tail takes to write the batch
    assert rows == 5000
    assert interval == pytest.approx(5.0)
    assert lag == pytest.approx(5.0 + 2.0 + 0.5)
    assert (2.0 + 0.0001 * rows) <= 0.5 * interval + 1e-9


def test_a_landing_slower_a_row_than_the_tail_never_keeps_up():
    assert decide.batch_for(rate=10000, startup=1.0, per_row=0.0001) is None
    assert decide.batch_for(rate=None, startup=1.0, per_row=0.0) is None


def test_the_landing_with_the_lowest_lag_is_chosen():
    choice, (rows, _, lag) = decide.warehouse_landing({
        "rate": 2000, "ledger": True, "stage": True,
        "stream_offsets": True,
        "stream_startup": 0.2, "stream_per_row": 0.0002,
        "files_startup": 8.0, "files_per_row": 0.00001,
        "merge_startup": 3.0, "merge_per_row": 0.0001})
    assert choice.path == "stream"
    assert rows == decide.batch_for(2000, 0.2, 0.0002)[0]
    assert lag < decide.batch_for(2000, 8.0, 0.00001)[2]


def test_a_landing_that_would_fall_behind_is_passed_over_with_why():
    choice, sized = decide.warehouse_landing({
        "rate": 5000, "ledger": True, "stage": True,
        "stream_offsets": True,
        "stream_startup": 0.1, "stream_per_row": 0.001,
        "files_startup": 6.0, "files_per_row": 0.00001})
    assert choice.path == "files"
    assert sized is not None
    why = dict((r.name, w) for r, w in choice.passed)
    assert "fall behind" in why["stream"]
    assert "not measured" in why["merge"]


def test_no_landing_without_the_ledger_or_a_streaming_offset():
    choice, sized = decide.warehouse_landing({
        "rate": 100, "ledger": False, "stage": True,
        "stream_offsets": False,
        "files_startup": 1.0, "files_per_row": 0.0001,
        "merge_startup": 1.0, "merge_per_row": 0.0001})
    assert choice.rung is None and sized is None


# ---- the warehouse times its landings and sizes the tail ---------------------

def _engine(cls, monkeypatch, **opts):
    e = object.__new__(cls)
    e.hop = types.SimpleNamespace(name="h", versions=opts.pop("versions",
                                                              None))
    monkeypatch.setattr(e, "_endpoint", lambda side: types.SimpleNamespace(
        options=opts, user="u"), raising=False)
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    return e


def test_the_fit_tells_a_start_from_a_rows_cost():
    assert warehouse._Warehouse.landing_fit([(100, 1.0), (100, 1.1)]) is None
    start, per_row = warehouse._Warehouse.landing_fit(
        [(1000, 2.1), (2000, 2.2), (4000, 2.4)])
    assert start == pytest.approx(2.0)
    assert per_row == pytest.approx(0.0001)


def test_the_tails_read_follows_the_measured_landing(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch)
    monkeypatch.setattr(e, "_stage", lambda side: None)
    assert e._landing_way("dst") == "merge"
    for rows, secs in ((1000, 2.1), (2000, 2.2), (4000, 2.4)):
        e._landed(rows, secs)
    # before the tail's rate is known its own size stands
    assert e.tail_limit("dst", "d", 1000, 100.0, 1000) == 1000
    got = e.tail_limit("dst", "d", 1000, 101.0, 1000)
    assert got == decide.batch_for(1000, 2.0, 0.0001)[0]
    assert got != 1000


def test_each_warehouse_says_its_own_landing(monkeypatch):
    rs = _engine(warehouse.RedshiftEngine, monkeypatch, stage="s3://b/p",
                 iam_role="arn:aws:iam::000000000000:role/r")
    assert rs._landing_way("dst") == "files" and rs._has_stage("dst")
    sf = _engine(warehouse.SnowflakeEngine, monkeypatch,
                 private_key_file="/k")
    assert sf._landing_way("dst") == "stream" and sf._stream_offsets("dst")
    sf2 = _engine(warehouse.SnowflakeEngine, monkeypatch)
    assert sf2._landing_way("dst") == "files"
    bq = _engine(warehouse.BigQueryEngine, monkeypatch)
    assert bq._landing_way("dst") == "stream" and bq._stream_offsets("dst")


def test_samples_are_the_most_recent(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch)
    monkeypatch.setattr(e, "_stage", lambda side: None)
    for i in range(e.LANDING_SAMPLES + 5):
        e._landed(i + 1, 1.0)
    got = e._landings["merge"]
    assert len(got) == e.LANDING_SAMPLES and got[-1][0] == e.LANDING_SAMPLES + 5


# ---- late and out-of-order rows ---------------------------------------------

def test_a_batch_out_of_order_ends_on_its_newest_version(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch, versions={"t": "v"})
    monkeypatch.setattr(e, "local_table", lambda t: t)
    ch = [{"op": "upsert", "table": "t", "key": {"id": 1},
           "values": {"id": 1, "v": 3, "x": "new"}},
          {"op": "upsert", "table": "u", "key": {"id": 9},
           "values": {"id": 9}},
          {"op": "upsert", "table": "t", "key": {"id": 1},
           "values": {"id": 1, "v": 2, "x": "old"}}]
    got = e._in_version_order(ch)
    assert [c["values"].get("x") for c in got] == ["old", None, "new"]
    assert got[1]["table"] == "u"


def test_a_late_row_never_overwrites_a_newer_one(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch, versions={"t": "v"})
    asked = []

    def rows(side, db, sql, args=None):
        asked.append((sql, args))
        return [(1, 5), (2, 1)]
    monkeypatch.setattr(e, "_rows", rows)
    monkeypatch.setattr(e, "_qualified", lambda s, d, t: f"{d}.{t}")
    batch = [("upsert", {"id": 1}, {"id": 1, "v": 4}),
             ("upsert", {"id": 2}, {"id": 2, "v": 2}),
             ("upsert", {"id": 3}, {"id": 3, "v": 1}),
             ("delete", {"id": 1}, {})]
    kept = e._not_late("dst", "d", "t", batch)
    assert [(k, r["id"]) for k, r, _ in kept] == [
        ("upsert", 2), ("upsert", 3), ("delete", 1)]
    assert asked and asked[0][1] == [1, 2, 3]


def test_versions_of_two_kinds_stop_rather_than_guess(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch, versions={"t": "v"})
    monkeypatch.setattr(e, "_rows", lambda *a, **k: [(1, "x")])
    monkeypatch.setattr(e, "_qualified", lambda s, d, t: t)
    with pytest.raises(SystemExit, match="newer cannot be told"):
        e._not_late("dst", "d", "t", [("upsert", {"id": 1},
                                       {"id": 1, "v": 4})])


def test_an_unversioned_table_is_taken_as_the_tail_gave_it(monkeypatch):
    e = _engine(warehouse.RedshiftEngine, monkeypatch)
    batch = [("upsert", {"id": 1}, {"id": 1})]
    assert e._not_late("dst", "d", "t", batch) is batch
    assert e._in_version_order([{"table": "t"}]) == [{"table": "t"}]


# ---- the provider's zero-ETL beside migkit's own hop -------------------------

def _inst(provider, product, engine=""):
    return cloudapi.Instance(provider=provider, product=product, id="x",
                             engine=engine)


def test_the_provider_integration_is_found_for_its_pairs_only():
    z = cloudapi.zero_etl(_inst("aws", "rds", "aurora-mysql"),
                          _inst("aws", "redshift"))
    assert z.name == "aws-zero-etl"
    assert cloudapi.zero_etl(_inst("aws", "rds", "oracle-ee"),
                             _inst("aws", "redshift")) is None
    assert cloudapi.zero_etl(_inst("aws", "rds", "postgres"),
                             _inst("gcp", "bigquery")) is None
    assert cloudapi.zero_etl(_inst("gcp", "alloydb"),
                             _inst("gcp", "bigquery")).runs \
        == "run_alloydb_bigquery"


def test_a_gap_on_the_source_takes_every_row_from_the_provider():
    s, d = _inst("aws", "rds", "mysql"), _inst("aws", "redshift")
    whole = cloudapi.feed_facts(s, d)
    assert "every row" in whole["provider_keeps"] and whole["runs"]
    gap = cloudapi.feed_facts(s, d, gaps=("a table with no primary key",))
    assert "every row" not in gap["provider_keeps"]
    f = decide.warehouse_feed({**gap, "tail": True,
                               "own_lag": 30.0})
    assert f.path == "own"
    assert "every row" in dict((r.name, w) for r, w in f.passed)["provider"]


def test_the_lower_lag_wins_where_both_keep_every_promise():
    s, d = _inst("aws", "rds", "aurora-postgresql"), _inst("aws", "redshift")
    facts = cloudapi.feed_facts(s, d, measured_lag=4.0)
    assert decide.warehouse_feed({**facts, "tail": True, "own_lag": 20.0,
                                  "provider_lag": facts["provider_lag"]}
                                 ).path == "provider"
    assert decide.warehouse_feed({**facts, "tail": True, "own_lag": 2.0}
                                 ).path == "own"


def test_an_integration_migkit_does_not_run_is_never_chosen():
    s, d = _inst("gcp", "alloydb"), _inst("gcp", "bigquery")
    facts = cloudapi.feed_facts(s, d, adapter=types.SimpleNamespace(
        runner=lambda z: None))
    f = decide.warehouse_feed({**facts, "tail": True,
                               "promises": ("deletes",), "own_lag": 99.0})
    assert f.path == "own"
    assert "does not yet run" in dict((r.name, w)
                                      for r, w in f.passed)["provider"]


class _Rds:
    def __init__(self, states):
        self.states, self.made = list(states), []

    def describe_integrations(self, IntegrationIdentifier):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        if state is None:
            return {"Integrations": []}
        return {"Integrations": [{"Status": state, "IntegrationArn": "arn:i",
                                  "Errors": [{"ErrorMessage": "boom"}]
                                  if state == "failed" else []}]}

    def create_integration(self, **kw):
        self.made.append(kw)


def test_the_integration_is_made_once_and_waited_for(monkeypatch):
    monkeypatch.setenv("MIGKIT_CLOUD_POLL", "0")
    a = cloudapi.Aws({"product": "rds", "id": "db"})
    rds = _Rds([None, "creating", "syncing", "active"])
    monkeypatch.setattr(a, "_rds", lambda: rds)
    got = a.run_zero_etl("arn:s", "arn:t", "feed", lambda *_: None)
    assert got == {"arn": "arn:i", "status": "active"}
    assert rds.made == [{"SourceArn": "arn:s", "TargetArn": "arn:t",
                         "IntegrationName": "feed"}]
    again = _Rds(["active"])
    monkeypatch.setattr(a, "_rds", lambda: again)
    a.run_zero_etl("arn:s", "arn:t", "feed", lambda *_: None)
    assert again.made == []


def test_a_failed_integration_stops_with_its_errors(monkeypatch):
    monkeypatch.setenv("MIGKIT_CLOUD_POLL", "0")
    a = cloudapi.Aws({"product": "rds", "id": "db"})
    monkeypatch.setattr(a, "_rds", lambda: _Rds(["creating", "failed"]))
    with pytest.raises(SystemExit, match="boom"):
        a.run_zero_etl("arn:s", "arn:t", "feed", lambda *_: None)
