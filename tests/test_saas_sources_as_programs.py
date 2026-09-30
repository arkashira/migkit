"""SaaS sources read by programs (backlog ER7): one adapter for a Singer
tap, a connector of the Airbyte protocol and a dlt source, each written
here as its protocol defines it, over an "application" that is a JSON
file the test changes. No SaaS account is used.

What is held, for each kind: the catalogue as the program gives it -
columns, key, whether a stream is read incrementally; a copy into SQLite
that is the follow's start, its state and the hashes of its rows kept; the
follow's changes - a changed row, a new one, a row gone from a stream read
whole, a row the program marks deleted - applied and proved equal; a row
the program hands over again unchanged not carried again; the same
changes read again where the tail had not saved the position after them,
and none once it had; a delete the program cannot see found by check, the
verdict saying why; a connector refused where its terms are not accepted,
and its renewed settings kept beside the reports. And every protocol's
options used or explained.
"""
import json
import os
import sqlite3
import stat
import sys
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

TAP = r'''#!PYTHON
import argparse, json, sys
p = argparse.ArgumentParser()
for flag in ("--config", "--catalog", "--properties", "--state", "--format"):
    p.add_argument(flag)
p.add_argument("--discover", action="store_true")
p.add_argument("--about", action="store_true")
a = p.parse_args()
if a.about:
    print(json.dumps({"name": "tap-shop",
                      "capabilities": ["catalog", "state", "discover"]}))
    sys.exit(0)
app = json.load(open(json.load(open(a.config))["app"]))
users = {"type": "object", "properties": {
    "id": {"type": "integer"}, "name": {"type": ["null", "string"]},
    "updated_at": {"type": "string", "format": "date-time"},
    "balance": {"type": ["null", "number"]}}}
plans = {"type": "object", "properties": {
    "code": {"type": "string", "maxLength": 8},
    "price": {"type": ["null", "number"]}}}
streams = [
    {"tap_stream_id": "users", "stream": "users", "schema": users,
     "key_properties": ["id"], "metadata": [
         {"breadcrumb": [], "metadata": {
             "replication-method": "INCREMENTAL",
             "replication-key": "updated_at"}}]},
    {"tap_stream_id": "plans", "stream": "plans", "schema": plans,
     "key_properties": ["code"], "metadata": [
         {"breadcrumb": [], "metadata": {
             "forced-replication-method": "FULL_TABLE"}}]}]
if a.discover:
    print(json.dumps({"streams": streams}))
    sys.exit(0)
catalog = json.load(open(a.catalog))
chosen = {s["tap_stream_id"] for s in catalog["streams"]
          if any(not m["breadcrumb"] and m["metadata"].get("selected")
                 for m in s["metadata"])}
state = json.load(open(a.state)) if a.state else {}
marks = state.get("bookmarks", {})
print("tap-shop: starting")
if "users" in chosen:
    print(json.dumps({"type": "SCHEMA", "stream": "users", "schema": users,
                      "key_properties": ["id"]}))
    since = marks.get("users", {}).get("updated_at")
    last = since
    for u in sorted(app["users"], key=lambda u: u["updated_at"]):
        if since is None or u["updated_at"] >= since:
            print(json.dumps({"type": "RECORD", "stream": "users",
                              "record": u}))
            last = u["updated_at"]
    marks["users"] = {"updated_at": last}
    print(json.dumps({"type": "STATE", "value": {"bookmarks": marks}}))
if "plans" in chosen:
    for row in app["plans"]:
        print(json.dumps({"type": "RECORD", "stream": "plans",
                          "record": row}))
    print(json.dumps({"type": "STATE", "value": {"bookmarks": marks}}))
'''

CONNECTOR = r'''#!PYTHON
import argparse, json, sys
cmd = sys.argv[1]
p = argparse.ArgumentParser()
for flag in ("--config", "--catalog", "--state"):
    p.add_argument(flag)
a = p.parse_args(sys.argv[2:])
cfg = json.load(open(a.config))
app = json.load(open(cfg["app"]))
schema = {"type": "object", "properties": {
    "id": {"type": "integer"}, "subject": {"type": ["null", "string"]},
    "updated_at": {"type": "string", "format": "date-time",
                   "airbyte_type": "timestamp_with_timezone"},
    "_ab_cdc_deleted_at": {"type": ["null", "string"],
                           "format": "date-time"}}}
stream = {"name": "tickets", "json_schema": schema,
          "supported_sync_modes": ["full_refresh", "incremental"],
          "source_defined_cursor": True, "default_cursor_field":
          ["updated_at"], "source_defined_primary_key": [["id"]]}
if cmd == "discover":
    print(json.dumps({"type": "LOG", "log": {"level": "INFO",
                                             "message": "discovering"}}))
    print(json.dumps({"type": "CATALOG", "catalog": {"streams": [stream]}}))
    sys.exit(0)
since = None
if a.state:
    for msg in json.load(open(a.state)):
        if msg.get("type") == "STREAM":
            since = msg["stream"]["stream_state"]["updated_at"]
last = since
for t in sorted(app["tickets"], key=lambda t: t["updated_at"]):
    if since is None or t["updated_at"] >= since:
        data = {k: v for k, v in t.items() if k != "deleted"}
        data["_ab_cdc_deleted_at"] = t["updated_at"] if t.get("deleted") \
            else None
        print(json.dumps({"type": "RECORD", "record": {
            "stream": "tickets", "data": data, "emitted_at": 0}}))
        last = t["updated_at"]
print(json.dumps({"type": "STATE", "state": {"type": "STREAM", "stream": {
    "stream_descriptor": {"name": "tickets"},
    "stream_state": {"updated_at": last}}}}))
if cfg.get("api_key") == "old":
    print(json.dumps({"type": "CONTROL", "control": {
        "type": "CONNECTOR_CONFIG", "emitted_at": 0,
        "connectorConfig": {"config": dict(cfg, api_key="renewed")}}}))
'''

DLT_SOURCE = r'''import json

import dlt


@dlt.source(name="shop")
def shop_source(app):
    @dlt.resource(primary_key="id", write_disposition="merge",
                  columns={"id": {"data_type": "bigint"},
                           "total": {"data_type": "decimal"},
                           "gone": {"data_type": "bool",
                                    "hard_delete": True},
                           "updated_at": {"data_type": "timestamp"}})
    def orders(updated_at=dlt.sources.incremental(
            "updated_at", initial_value="1970-01-01T00:00:00+00:00")):
        for row in json.load(open(app))["orders"]:
            if row["updated_at"] >= updated_at.last_value:
                yield row
    return orders
'''


def _program(root, name, text):
    path = root / name
    path.write_text(text.replace("#!PYTHON", "#!" + sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _app(root, **data):
    path = root / "app.json"
    path.write_text(json.dumps(data))
    return path


def _hop(root, kind, program, config, streams=None):
    target = root / "target.db"
    if not target.exists():
        sqlite3.connect(target).close()
    cfg = root / "config.json"
    cfg.write_text(json.dumps(config))
    opts = {"kind": kind, "program": str(program), "config": str(cfg)}
    if streams:
        opts["streams"] = streams
    hop = Hop(name="app", engine="hetero",
              source=Endpoint(options=opts),
              target=Endpoint(options={"path": str(target)}),
              databases=["shop"],
              options={"source_engine": "saas", "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: root
    return hop


def _copy(hop):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(hop)
    ck = _Checkpoint(hop.report_dir() / "move.json")
    said = []
    eng.create_missing("shop", said.append)
    for sch, t in eng.list_move_tables("shop"):
        eng.move_table("shop", sch, t, 500, ck, said.append)
    return eng


def _check(hop, monkeypatch):
    from migkit.engines.hetero import HeteroEngine
    # the drill says which rows differ; the verdict is decided before it
    monkeypatch.setattr(HeteroEngine, "_drill", lambda self, *a, **k: "")
    return {r.scope: r for r in HeteroEngine(hop).check_data("shop")}


def _kinds(changes):
    return [(c["op"], c["table"], next(iter(c["key"].values())))
            for c in changes]


# ---- a Singer tap ----------------------------------------------------------

USERS = [{"id": 1, "name": "Ann", "updated_at": "2026-09-01T00:00:00Z",
          "balance": 10.5},
         {"id": 2, "name": "Bo", "updated_at": "2026-09-02T00:00:00Z",
          "balance": 20},
         {"id": 3, "name": "Cy", "updated_at": "2026-09-03T00:00:00Z",
          "balance": None}]
PLANS = [{"code": "A", "price": 1.25}, {"code": "B", "price": 2.5}]


def test_a_tap_is_copied_and_its_changes_followed(tmp_path, monkeypatch):
    from migkit import canon
    from migkit.engines.saas import SaasEngine
    app = _app(tmp_path, users=USERS, plans=PLANS)
    tap = _program(tmp_path, "tap-shop", TAP)
    hop = _hop(tmp_path, "singer", tap, {"app": str(app)})
    src = SaasEngine(hop)
    assert src.neutral_tables("src", "shop") == ["plans", "users"]
    assert dict(src.neutral_columns("src", "shop", "users")) == {
        "id": "integer", "name": "text", "updated_at": "timestamptz",
        "balance": "number"}
    assert dict(src.neutral_columns("src", "shop", "plans"))["code"] == \
        "text(8)"
    assert canon.type_class("saas", "text(8)") == "text"
    assert src.neutral_key("src", "shop", "users") == ["id"]
    assert src._stream("src", "users").incremental
    assert not src._stream("src", "plans").incremental
    start = src.copy_point("src", "shop")
    eng = _copy(hop)
    kept = json.loads((tmp_path / "program-follow.json").read_text())
    assert kept["streams"]["users"]["state"] == {
        "bookmarks": {"users": {"updated_at": "2026-09-03T00:00:00Z"}}}
    got = _check(hop, monkeypatch)
    assert {k: v.status for k, v in got.items()} == {
        "shop.plans": "ok", "shop.users": "ok"}, got
    # the application changes: a user renamed, one deleted - which a tap
    # reading only what changed cannot see - a plan gone, a plan added
    data = json.loads(app.read_text())
    data["users"][1].update(name="Bea", updated_at="2026-09-05T00:00:00Z")
    del data["users"][2]
    data["plans"] = [PLANS[0], {"code": "C", "price": 3}]
    app.write_text(json.dumps(data))
    changes, token = SaasEngine(hop).neutral_changes("src", "shop", start)
    want = [("update", "users", 2), ("insert", "plans", "C"),
            ("delete", "plans", "B")]
    assert _kinds(changes) == want
    assert changes[0]["values"]["name"] == "Bea"
    # the tail stopped before saving the position after them: read again
    # from the one it had saved, the same changes, none passed over
    again, token = SaasEngine(hop).neutral_changes("src", "shop", start)
    assert _kinds(again) == want
    eng.dst_engine.neutral_apply("dst", "shop", again)
    # saved and handed back: the user the tap hands over again unchanged
    # (its bookmark is inclusive) is no change, and nothing is left
    after, token2 = SaasEngine(hop).neutral_changes("src", "shop", token)
    assert after == []
    assert token2["gen"] == token["gen"]
    got = _check(hop, monkeypatch)
    assert got["shop.plans"].status == "ok"
    # the delete the tap could not see is on the target still: found, and
    # the verdict says why the follow did not carry it
    users = got["shop.users"]
    assert users.status == "diff", users.detail
    assert "deleted at the source is not carried" in users.detail


def test_a_tap_as_an_address(tmp_path):
    from migkit.config import build_hop
    hop = build_hop("app", {"source": "singer://tap-shop?config=/etc/t.json",
                            "target": "sqlite:///tmp/t.db"})
    assert hop.engine == "hetero"
    assert hop.options["source_engine"] == "saas"
    assert hop.source.options == {"kind": "singer", "program": "tap-shop",
                                  "config": "/etc/t.json"}
    assert hop.databases == ["tap-shop"]
    assert hop.source.configured()


# ---- a connector of the Airbyte protocol ------------------------------------

TICKETS = [{"id": 1, "subject": "a", "updated_at": "2026-09-01T00:00:00Z"},
           {"id": 2, "subject": "b", "updated_at": "2026-09-02T00:00:00Z"},
           {"id": 3, "subject": "c", "updated_at": "2026-09-03T00:00:00Z"}]


def test_a_connector_runs_only_under_its_accepted_terms(tmp_path,
                                                        monkeypatch):
    from migkit import tools
    from migkit.engines.saas import SaasEngine
    app = _app(tmp_path, tickets=TICKETS)
    conn = _program(tmp_path, "source-tickets", CONNECTOR)
    hop = _hop(tmp_path, "airbyte", conn, {"app": str(app), "api_key": "x"})
    monkeypatch.setattr(tools, "terms_accepted", lambda n: False)
    with pytest.raises(SystemExit) as e:
        SaasEngine(hop).neutral_tables("src", "shop")
    said = str(e.value)
    assert "migkit doctor --install" in said
    assert "airbyte" not in said.lower()
    monkeypatch.setattr(tools, "terms_accepted", lambda n: True)
    assert SaasEngine(hop).neutral_tables("src", "shop") == ["tickets"]


def test_a_connector_is_copied_and_its_marked_deletes_carried(
        tmp_path, monkeypatch):
    from migkit import tools
    from migkit.engines.saas import SaasEngine
    monkeypatch.setattr(tools, "terms_accepted", lambda n: True)
    app = _app(tmp_path, tickets=TICKETS)
    conn = _program(tmp_path, "source-tickets", CONNECTOR)
    hop = _hop(tmp_path, "airbyte", conn, {"app": str(app),
                                           "api_key": "old"})
    src = SaasEngine(hop)
    stream = src._stream("src", "tickets")
    assert stream.key == ["id"] and stream.incremental
    assert stream.soft_delete == ("_ab_cdc_deleted_at",)
    start = src.copy_point("src", "shop")
    eng = _copy(hop)
    # the connector's renewed settings kept beside the reports, the side's
    # own file left as it was
    assert json.loads((tmp_path / "program-config.json").read_text())[
        "api_key"] == "renewed"
    assert json.loads((tmp_path / "config.json").read_text())["api_key"] \
        == "old"
    got = _check(hop, monkeypatch)
    assert got["shop.tickets"].status == "ok", got
    data = json.loads(app.read_text())
    data["tickets"][0].update(subject="A", updated_at="2026-09-04T00:00:00Z")
    data["tickets"][1].update(deleted=True,
                              updated_at="2026-09-05T00:00:00Z")
    data["tickets"].append({"id": 4, "subject": "d",
                            "updated_at": "2026-09-06T00:00:00Z"})
    app.write_text(json.dumps(data))
    changes, token = SaasEngine(hop).neutral_changes("src", "shop", start)
    assert _kinds(changes) == [("update", "tickets", 1),
                               ("delete", "tickets", 2),
                               ("insert", "tickets", 4)]
    eng.dst_engine.neutral_apply("dst", "shop", changes)
    got = _check(hop, monkeypatch)
    assert got["shop.tickets"].status == "ok", got
    # the deleted ticket is gone from the target as from what the
    # connector hands over whole
    con = sqlite3.connect(tmp_path / "target.db")
    ids = sorted(r[0] for r in con.execute('select "id" from "tickets"'))
    con.close()
    assert ids == [1, 3, 4]
    assert token["state"][0]["stream"]["stream_state"]["updated_at"] == \
        "2026-09-06T00:00:00Z"


# ---- a dlt source ------------------------------------------------------------

ORDERS = [{"id": 1, "total": "10.50", "gone": False, "note": "x",
           "updated_at": "2026-09-01T00:00:00+00:00"},
          {"id": 2, "total": "20.00", "gone": False, "note": "y",
           "updated_at": "2026-09-02T00:00:00+00:00"},
          {"id": 3, "total": "30.25", "gone": False, "note": "z",
           "updated_at": "2026-09-03T00:00:00+00:00"}]


def test_a_dlt_source_is_copied_and_followed_by_its_own_state(
        tmp_path, monkeypatch):
    from migkit.engines.saas import SaasEngine
    (tmp_path / "shop_dlt.py").write_text(DLT_SOURCE)
    monkeypatch.syspath_prepend(str(tmp_path))
    app = _app(tmp_path, orders=ORDERS)
    hop = _hop(tmp_path, "dlt", "shop_dlt:shop_source", {"app": str(app)})
    src = SaasEngine(hop)
    assert src.neutral_tables("src", "shop") == ["orders"]
    cols = dict(src.neutral_columns("src", "shop", "orders"))
    assert cols["id"] == "integer" and cols["total"] == "number"
    assert cols["gone"] == "boolean" and cols["note"] == "text"
    assert cols["updated_at"] == "timestamptz"
    stream = src._stream("src", "orders")
    assert stream.key == ["id"] and stream.incremental
    assert "gone" in stream.soft_delete
    start = src.copy_point("src", "shop")
    eng = _copy(hop)
    got = _check(hop, monkeypatch)
    assert got["shop.orders"].status == "ok", got
    data = json.loads(app.read_text())
    data["orders"][1].update(total="21.00",
                             updated_at="2026-09-04T00:00:00+00:00")
    data["orders"][2].update(gone=True,
                             updated_at="2026-09-05T00:00:00+00:00")
    data["orders"].append({"id": 4, "total": "4.00", "gone": False,
                           "note": "w",
                           "updated_at": "2026-09-06T00:00:00+00:00"})
    app.write_text(json.dumps(data))
    changes, token = SaasEngine(hop).neutral_changes("src", "shop", start)
    assert _kinds(changes) == [("update", "orders", 2),
                               ("delete", "orders", 3),
                               ("insert", "orders", 4)]
    assert changes[0]["values"]["total"] == Decimal("21.00")
    eng.dst_engine.neutral_apply("dst", "shop", changes)
    # the order the source marks deleted is neither on the target nor in
    # what the source is read as
    got = _check(hop, monkeypatch)
    assert got["shop.orders"].status == "ok", got
    con = sqlite3.connect(tmp_path / "target.db")
    ids = sorted(r[0] for r in con.execute('select "id" from "orders"'))
    con.close()
    assert ids == [1, 2, 4]
    # the source's own cursor is the position
    cursor = token["state"]["sources"]["shop"]["resources"]["orders"][
        "incremental"]["updated_at"]["last_value"]
    assert cursor == "2026-09-06T00:00:00+00:00"


# ---- every protocol's options -------------------------------------------------

@pytest.mark.parametrize("tool", ["singer-tap", "airbyte-connectors", "dlt"])
def test_every_protocols_options_are_used_or_explained(tool):
    from migkit import tooluse
    import migkit.engines.saas  # noqa: F401
    assert tooluse.unexplained(tool) == []
    assert tooluse.stale(tool) == []
    assert tooluse.used(tool), tool


def test_a_program_that_is_not_here_says_how_to_install_it(tmp_path):
    from migkit.engines.saas import SaasEngine
    hop = _hop(tmp_path, "singer", "tap-nowhere-" + os.urandom(4).hex(), {})
    with pytest.raises(SystemExit) as e:
        SaasEngine(hop).neutral_tables("src", "shop")
    assert "install" in str(e.value)
