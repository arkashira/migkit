"""DynamoDB reached as the service itself answers it, by the library that
stands in for it (`moto`): the same calls migkit makes of the real service,
with no account and no server. Shared by the tests that prove DynamoDB's
depth - a table's items, its stream, its indexes, its settings.

What moto does not do is said where a test needs it: it keeps no
point-in-time recovery of its own, expires no item, and trims no stream.
"""
import contextlib
import ctypes
import threading

import pytest

from migkit.config import Endpoint, Hop

REGION = "us-east-1"
VIEW = "NEW_AND_OLD_IMAGES"


def need_moto():
    return pytest.importorskip("moto")


@contextlib.contextmanager
def service(tmp_path, monkeypatch):
    """The service up, migkit's reports under `tmp_path`; yields the client."""
    moto = need_moto()
    import boto3

    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with moto.mock_aws():
        yield boto3.client("dynamodb", region_name=REGION)


def table(ddb, name, stream=True, kind="N", sort=None, indexes=(), **kw):
    """A table keyed by `id` (of `kind`), and by `sort` (a text attribute)
    where given; billed by request, with its stream on by default."""
    keys = [{"AttributeName": "id", "KeyType": "HASH"}]
    defs = [{"AttributeName": "id", "AttributeType": kind}]
    if sort:
        keys.append({"AttributeName": sort, "KeyType": "RANGE"})
        defs.append({"AttributeName": sort, "AttributeType": "S"})
    if indexes:
        kw["GlobalSecondaryIndexes"] = [
            {"IndexName": n, "KeySchema": [{"AttributeName": a,
                                            "KeyType": "HASH"}],
             "Projection": {"ProjectionType": "ALL"}} for n, a in indexes]
        defs += [{"AttributeName": a, "AttributeType": "S"}
                 for _, a in indexes]
    if stream:
        kw["StreamSpecification"] = {"StreamEnabled": True,
                                     "StreamViewType": VIEW}
    ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                     KeySchema=keys, AttributeDefinitions=defs, **kw)


def put(ddb, name, id, **attrs):
    """An item with a number `id` and text attributes; a value that is
    already an attribute (`{"NULL": True}`) goes as it is."""
    item = {"id": {"N": str(id)}}
    for k, v in attrs.items():
        item[k] = v if isinstance(v, dict) else {"S": str(v)}
    ddb.put_item(TableName=name, Item=item)


def items(ddb, name):
    """{id: {attribute: value}} of a table, text and numbers as written."""
    out, start = {}, None
    while True:
        page = ddb.scan(TableName=name, **({"ExclusiveStartKey": start}
                                           if start else {}))
        for i in page["Items"]:
            out[int(i["id"]["N"])] = {k: next(iter(v.values()))
                                      for k, v in i.items() if k != "id"}
        start = page.get("LastEvaluatedKey")
        if not start:
            return out


def hop(tmp_path, **kw):
    """DynamoDB to DynamoDB: `shop.*` tables copied to `shopcopy.*`."""
    ep = dict(host="", port=0, user="CHANGE_ME", password="CHANGE_ME",
              options={"region": REGION})
    kw.setdefault("databases", ["shop"])
    kw.setdefault("db_map", {"shop": "shopcopy"})
    h = Hop(name="ddb", engine="dynamodb", source=Endpoint(**ep),
            target=Endpoint(**{**ep, "options": {"region": REGION}}), **kw)
    h.report_dir = lambda db=None: tmp_path
    return h


def engine(tmp_path, **kw):
    from migkit.engines.dynamodb import DynamoDBEngine
    return DynamoDBEngine(hop(tmp_path, **kw))


def copy(eng, db="shop"):
    """Every table in scope made on the target and copied, one pass each."""
    from migkit.cli import _Checkpoint
    eng.create_missing(db, lambda m: None)
    ck = _Checkpoint(eng.hop.report_dir(db) / "move.json")
    for sch, t in eng.list_move_tables(db):
        eng.move_table(db, sch, t, 1000, ck, lambda m: None)


class Tail:
    """The change tail of a hop, run on a thread: `with Tail(eng, db) as t`
    follows until the block ends; `t.fence()` waits until it has applied
    everything the source's streams hold now."""

    def __init__(self, eng, db="shop", log=None):
        self.eng, self.db, self.log = eng, db, log or (lambda m: None)
        self.ended = {}

    def __enter__(self):
        self.token = self.eng.hop.report_dir(self.db) / "tail-token.json"

        def run():
            try:
                self.eng.tail_apply(self.db, True, self.token, self.log)
            except BaseException as e:  # noqa: BLE001 - kept for the test
                self.ended["e"] = e
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        return self

    def fence(self, timeout=60):
        import time
        time.sleep(0.5)
        at = self.eng.src_lsn(self.db)
        assert at is not None, self.ended
        return self.eng.fence_wait(self.db, at, timeout=timeout)

    def __exit__(self, *exc):
        if self.thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(self.thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        self.thread.join(timeout=30)
        return False
