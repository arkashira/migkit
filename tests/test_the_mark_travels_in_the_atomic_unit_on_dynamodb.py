"""The mark migkit's own writes carry on DynamoDB, written in the engine's
atomic unit (TransactWriteItems) and read back from the table's stream.

Proved on a server: the rung is chosen by the decision engine from a probe
of the side itself (a probe ran, the Choice names the cost of the rung it
took), a unit migkit writes carries the mark and an application's write
beside it carries none, a reader that applies what it reads applies the
application's and leaves migkit's out (counts asserted), a unit that fails
in the middle leaves neither its mark nor its rows, `doctor` names the rung,
and teardown takes away what the mark left.
"""
import pytest

from migkit import engines, failpoint, marks
from migkit.config import Endpoint, Hop
from tests import servers
from tests.conftest import needs_docker

# `cutover.two-way` on dynamodb is proved here once the depth probe (capabilities.py) reads `mark_unit` as the way

pytestmark = needs_docker

space = servers.fixture("dynamodb")

import json

DB = "w15h"
# the local stand-in takes any key made of letters and digits
KEY = "w15hlocal"
T = "w15h.items"


def _eng(space):
    ep = Endpoint(host=f"http://{space.one.host}", port=space.one.port,
                  user=KEY, password=KEY,
                  options={"endpoint_url":
                           f"http://{space.one.host}:{space.one.port}"})
    hop = Hop(name="w15h", engine="dynamodb", databases=[DB], source=ep,
              target=ep, options={"two_way": True})
    return engines.get_engine(hop)


def _table(space):
    c = space.one.client()
    try:
        c.delete_table(TableName=T)
    except c.exceptions.ResourceNotFoundException:
        pass
    c.create_table(TableName=T, BillingMode="PAY_PER_REQUEST",
                   KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                   AttributeDefinitions=[{"AttributeName": "id",
                                          "AttributeType": "S"}],
                   StreamSpecification={"StreamEnabled": True,
                                        "StreamViewType":
                                        "NEW_AND_OLD_IMAGES"})
    return c


def test_the_rung_is_chosen_from_a_probe_and_the_choice_names_its_cost(space):
    eng = _eng(space)
    marks.MEASURED.clear()
    got = marks.choice(eng, "dst", DB)
    assert got.path == "item" and eng._mark_rung == "item"
    assert got.cost().measured and got.cost().mean >= 0.0
    assert ("dynamodb", "dst", "item") in marks.MEASURED         # a probe ran


def test_a_transaction_carries_the_mark_and_a_reader_skips_only_its_own(
        space):
    c, eng = _table(space), _eng(space)
    c.put_item(TableName=T, Item={"id": {"S": "old"}})
    eng.mark_unit("dst", DB, "item", 5,
                  [("put", T, {"id": {"S": f"m{i}"}}, None)
                   for i in range(3)]
                  + [("delete", T, {"id": {"S": "old"}})], run_id="r1")
    c.put_item(TableName=T, Item={"id": {"S": "app1"}})
    c.put_item(TableName=T, Item={"id": {"S": "app2"}})
    units = eng.mark_units("dst")
    own = [(r["eventName"], eng.mark_own(T, r, units))
           for r in eng.mark_stream("dst", T)]
    assert sum(1 for _, o in own if o) == 4        # 3 puts + the delete
    assert [o for n, o in own if n == "INSERT"].count(False) == 2 + 1  # old
    assert c.get_item(TableName=T, Key={"id": {"S": "old"}}).get("Item") \
        is None
    origin = c.get_item(TableName="migkit_origin",
                        Key={"origin": {"S": "r1:5"}})["Item"]
    assert origin["batch"]["N"] == "5"


def test_a_unit_whose_condition_fails_leaves_neither_mark_nor_rows(space):
    c, eng = _table(space), _eng(space)
    c.put_item(TableName=T, Item={"id": {"S": "taken"}})
    eng._mark_table("dst")
    with pytest.raises(c.exceptions.TransactionCanceledException):
        eng.mark_unit("dst", DB, "item", 6,
                      [("put", T, {"id": {"S": "fresh"}}, "id"),
                       ("put", T, {"id": {"S": "taken"}}, "id")],
                      run_id="r2")
    assert "Item" not in c.get_item(TableName=T, Key={"id": {"S": "fresh"}})
    assert "Item" not in c.get_item(TableName="migkit_origin",
                                    Key={"origin": {"S": "r2:6"}})


def test_a_failpoint_before_the_send_writes_nothing(space, monkeypatch):
    c, eng = _table(space), _eng(space)
    eng._mark_table("dst")
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mark.unit:2:raise")
    failpoint.reset()
    with pytest.raises(RuntimeError):
        eng.mark_unit("dst", DB, "item", 7,
                      [("put", T, {"id": {"S": "a"}}, None),
                       ("put", T, {"id": {"S": "b"}}, None)], run_id="r3")
    assert c.scan(TableName=T)["Count"] == 0
    assert "Item" not in c.get_item(TableName="migkit_origin",
                                    Key={"origin": {"S": "r3:7"}})


def test_doctor_names_the_rung_and_teardown_drops_the_origin_table(
        space, tmp_path):
    c, eng = _table(space), _eng(space)
    marks.choice(eng, "dst", DB)
    token = str(tmp_path / "tail.token")
    marks._keep_rung(token, eng._mark_rung)
    assert "migkit_origin" in marks.said(eng, token, DB)
    assert "migkit_origin" in c.list_tables()["TableNames"]
    marks.teardown(eng, "dst", DB)
    assert "migkit_origin" not in c.list_tables()["TableNames"]
    assert "_mark_rung" not in eng.__dict__
