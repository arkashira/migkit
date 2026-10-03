"""A DynamoDB table copied through the service's own export to S3 and its
import from S3 (backlog F7.ddbexport), against moto - no real account is
ever touched.

A table the target lacks is made by the import from the source's export,
every item arriving as the service held it (bytes, maps, lists, sets,
numbers at their scale); the export's files are removed after. A table the
target has already takes the export's items through migkit's own writer,
the target's own item gone. The export's count is the proof where no
manifest is written, and a read that comes up short says so and the table
goes by the scan. A table whose point-in-time recovery is off is scanned:
turning it on is the source's setting.
"""
import base64
import json

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.bulk", "dynamodb")]

moto = pytest.importorskip("moto")

REGION = "us-east-1"
ITEMS = [
    {"pk": {"S": "a"}, "sk": {"N": "1"}, "blob": {"B": b"\x00\xff\x10"},
     "price": {"N": "10.50"}, "tags": {"SS": ["x", "y"]},
     "doc": {"M": {"n": {"N": "1"}, "l": {"L": [{"S": "q"},
                                              {"BOOL": True}]}}}},
    {"pk": {"S": "a"}, "sk": {"N": "2"}, "nums": {"NS": ["1", "2.5"]},
     "none": {"NULL": True}},
    {"pk": {"S": "b"}, "sk": {"N": "1"}, "text": {"S": ""}},
] + [{"pk": {"S": f"k{i}"}, "sk": {"N": str(i)}, "v": {"S": "x" * 50}}
     for i in range(40)]


@pytest.fixture
def aws(tmp_path, monkeypatch):
    import boto3

    import migkit.config as cfg
    from migkit import ddbexport
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    # every table here is small; the line is what is under test
    monkeypatch.setattr(ddbexport, "EXPORT_FROM", 0)
    with moto.mock_aws():
        ddb = boto3.client("dynamodb", region_name=REGION)
        boto3.client("s3", region_name=REGION).create_bucket(
            Bucket="exports")
        ddb.create_table(
            TableName="app.orders",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                       {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk",
                                   "AttributeType": "S"},
                                  {"AttributeName": "sk",
                                   "AttributeType": "N"}],
            BillingMode="PAY_PER_REQUEST")
        for item in ITEMS:
            ddb.put_item(TableName="app.orders", Item=item)
        yield ddb


def _pitr(ddb, on=True):
    ddb.update_continuous_backups(
        TableName="app.orders",
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": on})


def _hop(tmp_path, **opts):
    ep = dict(host="", port=0, user="CHANGE_ME", password="CHANGE_ME",
              options={"region": REGION})
    hop = Hop(name="ddbexp", engine="dynamodb", source=Endpoint(**ep),
              target=Endpoint(**{**ep, "options": {"region": REGION}}),
              databases=["app"], db_map={"app": "copy"},
              options={"export_bucket": "s3://exports/migkit", **opts})
    hop.report_dir = lambda db=None: (tmp_path / db if db else tmp_path)
    return hop


def _items(ddb, table):
    def key(item):
        return json.dumps(item, sort_keys=True, default=lambda b: base64
                          .b64encode(bytes(b)).decode())
    got = ddb.scan(TableName=table)["Items"]
    for item in got:
        for v in item.values():
            for kind in ("SS", "NS"):
                if kind in v:
                    v[kind] = sorted(v[kind])
    return sorted(key(i) for i in got)


def _copy(tmp_path, **opts):
    from migkit.engines.dynamodb import DynamoDBEngine
    said = []
    eng = DynamoDBEngine(_hop(tmp_path, **opts))
    eng.native_bulk("app", ["orders"], True, said.append)
    return said


def _left(prefix="migkit"):
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    return s3.list_objects_v2(Bucket="exports", Prefix=prefix).get(
        "KeyCount", 0)


def test_a_missing_table_is_made_by_the_import_of_the_export(aws,
                                                             tmp_path):
    _pitr(aws)
    said = _copy(tmp_path)
    assert any("by the service's export and import" in s for s in said), \
        said
    assert _items(aws, "copy.orders") == _items(aws, "app.orders")
    assert len(_items(aws, "copy.orders")) == len(ITEMS)
    # the export's files, a copy of the table, are gone
    assert _left() == 0


def test_a_table_the_target_has_takes_the_exports_items(aws, tmp_path):
    _pitr(aws)
    aws.create_table(
        TableName="copy.orders",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST")
    aws.put_item(TableName="copy.orders",
                 Item={"pk": {"S": "only on the target"}, "sk": {"N": "0"}})
    said = _copy(tmp_path)
    assert any("from the service's export" in s for s in said), said
    assert _items(aws, "copy.orders") == _items(aws, "app.orders")
    assert _left() == 0


def test_an_export_read_short_is_said_and_the_scan_carries_it(
        aws, tmp_path, monkeypatch):
    """The proof can say "different": a data file lost between the export
    and the read leaves fewer items than the export counted."""
    from migkit import ddbexport
    _pitr(aws)
    real = ddbexport.files
    monkeypatch.setattr(ddbexport, "files",
                        lambda *a: real(*a)[1:] if len(real(*a)) > 1 else [])
    monkeypatch.setattr(ddbexport, "import_unfit",
                        lambda eng, db, t: "the import is not under test")
    said = _copy(tmp_path)
    short = [s for s in said if "items read from the export where it"
             " counted" in s]
    assert short, said
    assert any("segments" in s for s in said), said
    assert _items(aws, "copy.orders") == _items(aws, "app.orders")
    # the failed way's export is removed too
    assert _left() == 0


def test_a_failed_import_gives_the_table_to_the_next_way(aws, tmp_path,
                                                         monkeypatch):
    from migkit import ddbexport
    _pitr(aws)

    def refused(*a, **kw):
        raise RuntimeError("the import ended failed with 0 of 43 items")
    monkeypatch.setattr(ddbexport, "import_", refused)
    said = _copy(tmp_path)
    assert any("could not run (the import ended failed" in s
               for s in said), said
    assert any("from the service's export" in s for s in said), said
    assert _items(aws, "copy.orders") == _items(aws, "app.orders")


def test_recovery_off_is_the_sources_setting_and_the_table_is_scanned(
        aws, tmp_path):
    _pitr(aws, on=False)
    from migkit import ddbexport
    from migkit.engines.dynamodb import DynamoDBEngine
    eng = DynamoDBEngine(_hop(tmp_path))
    assert "point-in-time recovery is off" in ddbexport.export_unfit(
        eng, "app", "orders")
    said = _copy(tmp_path)
    assert any("segments" in s for s in said), said
    assert not any("export" in s for s in said), said
    assert _items(aws, "copy.orders") == _items(aws, "app.orders")
    # and the source was left as it was
    got = aws.describe_continuous_backups(TableName="app.orders")
    assert got["ContinuousBackupsDescription"][
        "PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"] \
        == "DISABLED"


def test_no_place_named_no_export(aws, tmp_path):
    _pitr(aws)
    from migkit import ddbexport
    from migkit.engines.dynamodb import DynamoDBEngine
    hop = _hop(tmp_path)
    hop.options = {}
    assert "names no export_bucket" in ddbexport.export_unfit(
        DynamoDBEngine(hop), "app", "orders")
