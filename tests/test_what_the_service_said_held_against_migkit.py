"""A migration service's own check, held table by table against migkit's.

The service's report says "consistent". migkit says, beside its own
verdict and without changing it, where that claim falls short: a table it
called the same that migkit found different, one it skipped, one it never
checked, and what migkit found that no table check of the service's
covers. The reports here were written for these tests (no real system's),
in the shapes the services print.
"""
import json

import pytest

from migkit import servicecheck, verdict

TENCENT = {"Response": {
    "Abstract": {"Conclusion": "same",
                 "Options": {"Method": "sampleDataCheck", "SampleRate": 10}},
    "Detail": {
        "Difference": {"TotalCount": 1, "Items": [
            {"Db": "shop", "Table": "lines", "Chunk": 3,
             "SrcItem": "a", "DstItem": "b"}]},
        "Skipped": {"TotalCount": 1, "Items": [
            {"Db": "shop", "Table": "blobs", "Reason": "no primary key"}]},
        "Tables": [{"Db": "shop", "Table": "orders", "Conclusion": "same"},
                   {"Db": "shop", "Table": "carts", "Conclusion": "same"}]}}}

ANSWERS = [
    {"check": "data", "scope": "shop.orders", "status": "diff",
     "detail": "kind=rows-missing by=3"},
    {"check": "data", "scope": "shop.carts", "status": "ok"},
    {"check": "data", "scope": "shop.lines", "status": "ok"},
    {"check": "data", "scope": "shop.blobs", "status": "ok"},
    {"check": "data", "scope": "shop.users", "status": "ok"},
    {"check": "autoinc", "scope": "shop", "status": "diff",
     "detail": "2 sequences behind max(id)"},
]


class _Hop:
    def __init__(self, path):
        self.options = {"compare_with": str(path)}


@pytest.fixture
def report(tmp_path):
    p = tmp_path / "dts-report.json"
    p.write_text(json.dumps(TENCENT))
    return p


def test_each_table_falls_where_the_two_answers_put_it(report):
    got = servicecheck.of(_Hop(report), ANSWERS)
    assert got["service"] == "Tencent Cloud DTS"
    assert got["overall"] == "same"
    assert got["method"] == "a sample of 10% of the rows"
    assert got["missed"] == ["shop.orders: kind=rows-missing by=3"]
    assert got["agreed"] == ["shop.carts: same"]
    assert [x.split(":")[0] for x in got["not found"]] == ["shop.lines"]
    assert got["skipped"][0].startswith("shop.blobs: no primary key")
    assert got["unchecked"] == ["shop.users: migkit ok"]
    assert got["beyond"][0].startswith("shop (autoinc)")
    said = servicecheck.said(got)
    assert said.startswith("Tencent Cloud DTS said same")
    assert "1 missed" in said


def test_migkits_verdict_is_its_own(report, monkeypatch):
    """The service's word is evidence beside the verdict, never an input:
    a table it called the same that migkit found different stays
    different."""
    got = verdict.decide(ANSWERS)
    assert got.status == "different"


def test_a_table_named_with_its_schema_matches_only_that_schema(tmp_path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({"Tables": [
        {"Db": "appdb", "Schema": "sales", "Table": "orders",
         "Conclusion": "same"}]}))
    got = servicecheck.of(_Hop(p), [
        {"check": "data", "scope": "appdb.crm.orders", "status": "diff"},
        {"check": "data", "scope": "appdb.sales.orders", "status": "ok"}])
    assert got["agreed"] == ["appdb.sales.orders: same"]
    assert got["unchecked"] == ["appdb.crm.orders: migkit diff"]


def test_aws_dms_table_statistics(tmp_path):
    p = tmp_path / "stats.json"
    p.write_text(json.dumps({"ReplicationTaskArn": "arn:example",
                             "TableStatistics": [
        {"SchemaName": "shop", "TableName": "orders",
         "ValidationState": "Validated", "ValidationFailedRecords": 0},
        {"SchemaName": "shop", "TableName": "lines",
         "ValidationState": "Mismatched records",
         "ValidationFailedRecords": 4},
        {"SchemaName": "shop", "TableName": "notes",
         "ValidationState": "No primary key"}]}))
    got = servicecheck.read(p)
    assert got.service == "AWS DMS"
    assert got.tables[("", "shop", "orders")][0] == "same"
    assert got.tables[("", "shop", "lines")] == (
        "different", "4 records failed its validation")
    assert got.tables[("", "shop", "notes")][0] == "skipped"
    # the schema DMS names is the database where there are no schemas
    held = servicecheck.reconcile(got, [
        {"check": "data", "scope": "shop.orders", "status": "diff"},
        {"check": "data", "scope": "appdb.shop.lines", "status": "diff"}])
    assert [x.split(":")[0] for x in held["missed"]] == ["shop.orders"]
    assert [x.split(":")[0] for x in held["agreed"]] == ["shop.lines"]


def test_a_report_that_cannot_be_read_is_said(tmp_path):
    p = tmp_path / "broken.json"
    p.write_text("{not json")
    got = servicecheck.of(_Hop(p), ANSWERS)
    assert "could not be read" in got["error"]
    assert "could not be read" in servicecheck.said(got)


def test_no_report_no_section(monkeypatch):
    monkeypatch.delenv("MIGKIT_COMPARE_WITH", raising=False)

    class Bare:
        options = {}
    assert servicecheck.of(Bare, ANSWERS) is None


def test_a_run_may_name_the_report_in_its_environment(report, monkeypatch):
    monkeypatch.setenv("MIGKIT_COMPARE_WITH", str(report))

    class Bare:
        options = {}
    assert servicecheck.of(Bare, ANSWERS)["missed"]
