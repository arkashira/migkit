"""A warehouse lands each batch through the way `decide.warehouse_landing`
chose, trying a landing not yet timed before it can be chosen and changing
way at a batch boundary only past the decision's tie; the provider's
zero-ETL lag is measured by a marker's round trip, and its Azure and Google
integrations are made, watched and refused by the exact grant they lack."""
import types

import pytest

from migkit import cloudapi, decide
from migkit.engines import warehouse

def _engine(cls, monkeypatch, **opts):
    e = object.__new__(cls)
    e.hop = types.SimpleNamespace(name="h", versions=None)
    monkeypatch.setattr(e, "_endpoint", lambda side: types.SimpleNamespace(
        options=opts, user="u"), raising=False)
    monkeypatch.setattr(e, "_provider_feed", lambda side, db, log: {
        "offered": False, "not_offered": "fake"}, raising=False)
    said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log, **k: said.append(k))
    return e, said


def _staged_redshift(monkeypatch):
    e, said = _engine(warehouse.RedshiftEngine, monkeypatch)
    monkeypatch.setattr(e, "_stage", lambda side: ("b", "p", "r"))
    return e, said


def test_an_untimed_landing_is_tried_at_two_sizes_then_the_decision_lands(
        monkeypatch):
    e, _ = _staged_redshift(monkeypatch)
    assert e._landing_way("dst") == "files"
    for rows, secs in ((1000, 30.0), (2000, 30.1)):
        e._landed(rows, secs)
    e.tail_limit("dst", "d", 1000, 100.0, 1000)
    first = e.tail_limit("dst", "d", 1000, 101.0, 1000)
    assert e._landing_way("dst") == "merge" and first == 1000
    e._landed(first, 0.5 + 0.0001 * first)
    second = e.tail_limit("dst", "d", 1000, 102.0, 1000)
    assert e._landing_way("dst") == "merge" and second == 500
    e._landed(second, 0.5 + 0.0001 * second)
    got = e.tail_limit("dst", "d", 1000, 103.0, 1000)
    # merge starts in half a second against the stage's thirty: it is taken
    assert e._landing_way("dst") == "merge"
    assert got == decide.batch_for(1000, 0.5, 0.0001)[0]


def test_a_landing_within_the_tie_is_kept(monkeypatch):
    e, _ = _staged_redshift(monkeypatch)
    for rows, secs in ((1000, 1.0), (2000, 1.1)):
        e._landed(rows, secs)
    e._landing_now = "merge"
    for rows, secs in ((1000, 0.99), (2000, 1.09)):
        e._landed(rows, secs)
    e._landing_now = "files"
    e._landing_tried = {"merge": e.LANDING_PROBES}
    e.tail_limit("dst", "d", 1000, 100.0, 1000)
    e.tail_limit("dst", "d", 1000, 101.0, 1000)
    assert e._landing_way("dst") == "files"


def test_a_landing_that_falls_behind_is_left(monkeypatch):
    e, said = _staged_redshift(monkeypatch)
    for rows, secs in ((1000, 20.0), (2000, 40.0)):
        e._landed(rows, secs)
    e._landing_now = "merge"
    for rows, secs in ((1000, 1.0), (2000, 1.1)):
        e._landed(rows, secs)
    e._landing_now = "files"
    e.tail_limit("dst", "d", 1000, 100.0, 1000)
    e.tail_limit("dst", "d", 1000, 101.0, 1000)
    assert e._landing_way("dst") == "merge"
    assert any("lag" in k.get("reason", "") for k in said)


def test_a_probe_is_bounded(monkeypatch):
    e, _ = _staged_redshift(monkeypatch)
    for rows, secs in ((1000, 1.0), (2000, 1.1)):
        e._landed(rows, secs)
    e.tail_limit("dst", "d", 1000, 100.0, 1000)
    for i in range(e.LANDING_PROBES + 3):
        e.tail_limit("dst", "d", 1000, 101.0 + i, 1000)
        e._landed(1000, 1.0)   # one size only: never fit
    assert e._landing_tried["merge"] == e.LANDING_PROBES


def test_a_bigquery_landing_without_load_quota_offers_no_files(monkeypatch):
    e, _ = _engine(warehouse.BigQueryEngine, monkeypatch)
    monkeypatch.setattr(e, "_loads_left", lambda side: 0)
    assert e._landings_offered("dst") == ("stream",)
    monkeypatch.setattr(e, "_loads_left", lambda side: None)
    assert e._landings_offered("dst") == ("stream", "files")


def test_the_versions_are_read_by_name(monkeypatch):
    e, _ = _engine(warehouse.RedshiftEngine, monkeypatch)
    e.hop.versions = {"t": "v"}
    assert e.versions() == {"t": "v"}


# ---- the provider's zero-ETL: measured, and run through its API -------------

def test_the_lag_is_a_markers_round_trip(monkeypatch):
    clock = iter([10.0, 13.5])
    monkeypatch.setattr(cloudapi.time, "monotonic", lambda: next(clock))
    put = []
    got = cloudapi.zero_etl_lag(lambda: {"status": "active"}, put.append,
                                lambda t: t in put, lambda m: None)
    assert got == pytest.approx(3.5) and put[0].startswith("migkit-zetl-")


def test_no_lag_is_measured_before_the_integration_runs():
    assert cloudapi.zero_etl_lag(lambda: {"status": "creating"},
                                 pytest.fail, pytest.fail,
                                 lambda m: None) is None
    assert cloudapi.zero_etl_lag(lambda: None, pytest.fail, pytest.fail,
                                 lambda m: None) is None


def test_azure_and_google_integrations_are_run():
    by = {z.name: z for zs in cloudapi.ZERO_ETL.values() for z in zs}
    assert by["azure-mi-link"].runs == "run_mi_link"
    assert by["gcp-alloydb-bigquery"].runs == "run_alloydb_bigquery"
    assert not by["azure-mi-link"].why_not


def test_a_missing_name_is_refused_by_the_option():
    z = cloudapi.ZERO_ETL["azure"][0]
    fake = types.SimpleNamespace(
        WHO="Azure", run_mi_link=lambda managed_instance, partner_endpoint,
        partner_ag, instance_ag, name, log: {"status": "synchronizing"})
    with pytest.raises(SystemExit, match="partner_endpoint, partner_ag"):
        cloudapi.run_feed(fake, z, {"managed_instance": "mi", "name": "n",
                                    "instance_ag": "i"}, print)
    got = cloudapi.run_feed(fake, z, {
        "managed_instance": "mi", "name": "n", "instance_ag": "i",
        "partner_endpoint": "tcp://10.0.0.1:5022", "partner_ag": "ag"},
        print)
    assert got == {"status": "synchronizing"}


def test_a_refused_mi_link_names_the_grant():
    a = object.__new__(cloudapi.Azure)
    e = types.SimpleNamespace(status_code=403, error=None)
    got = a._mi_refused(e, "the link")
    assert "distributedAvailabilityGroups/write" in str(got)
    e = types.SimpleNamespace(status_code=409, error=types.SimpleNamespace(
        code="MissingSubscriptionRegistration"))
    assert "Microsoft.Sql resource provider" in str(a._mi_refused(e, "x"))
    assert a._mi_refused(types.SimpleNamespace(status_code=500, error=None),
                         "x") is None


def test_a_refused_stream_names_the_api_or_permission(monkeypatch):
    g = object.__new__(cloudapi.Gcp)
    monkeypatch.setattr(g, "_project", lambda: "p", raising=False)
    off = types.SimpleNamespace(resp=types.SimpleNamespace(status=403),
                                content=b"SERVICE_DISABLED")
    assert "enabled datastream.googleapis.com" in str(
        g._stream_refused(off, "s"))
    denied = types.SimpleNamespace(resp=types.SimpleNamespace(status=403),
                                   content=b"PERMISSION_DENIED")
    assert "datastream.streams.create" in str(g._stream_refused(denied, "s"))


def test_the_feed_is_decided_as_the_tail_starts(monkeypatch):
    e, said = _engine(warehouse.RedshiftEngine, monkeypatch)
    monkeypatch.setattr(e, "_stage", lambda side: None)
    e.tail_limit("dst", "d", 10, 1.0, 1000)
    e.tail_limit("dst", "d", 10, 2.0, 1000)
    assert list(e._fed) == ["d"] and e._fed["d"].path == "own"
