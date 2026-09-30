"""Salesforce as a program source (backlog ER7), held to a local REST API
and a local change-event service speaking the Pub/Sub API's messages -
the field numbers its documentation gives - with Avro payloads of a
change event's schema. No Salesforce org is used.

What is held: signing in by a connected app's client credentials, the org
known from the answer; an object's fields as columns, a compound field by
its parts, a file's bytes and a formula field said as what is not
compared; a copy by the bulk query job, and by the REST query's pages
where the job is refused, said; the application's own count held to the
records read, and a read that missed a record found by it; the follow's
changes - an UPDATE by its changed and nulled fields, a compound's part
among them, a CREATE, a DELETE, a GAP read again as the record is now,
an event the copy already holds passed over - applied and proved equal;
the replay id kept and asked again; a copy older than the 72 hours
events are kept refused.
"""
import datetime
import io
import json
import re
import sqlite3
import threading
from concurrent import futures
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from migkit.config import Endpoint, Hop

ORG = "00D000000000001AAA"
BASE = "/services/data/v62.0"

FIELDS = [
    {"name": "Id", "type": "id", "length": 18},
    {"name": "Name", "type": "string", "length": 255},
    {"name": "AnnualRevenue", "type": "currency", "precision": 18,
     "scale": 2},
    {"name": "IsActive__c", "type": "boolean"},
    {"name": "BillingStreet", "type": "textarea", "length": 255},
    {"name": "BillingCity", "type": "string", "length": 40},
    {"name": "BillingAddress", "type": "address"},
    {"name": "Logo__c", "type": "base64"},
    {"name": "Score__c", "type": "double", "precision": 18, "scale": 2,
     "calculated": True},
    {"name": "LastModifiedDate", "type": "datetime"},
]

SCHEMA = {
    "type": "record", "name": "AccountChangeEvent",
    "namespace": "com.sforce.eventbus",
    "fields": [
        {"name": "ChangeEventHeader", "type": {
            "type": "record", "name": "ChangeEventHeader", "fields": [
                {"name": "entityName", "type": "string"},
                {"name": "recordIds", "type": {"type": "array",
                                               "items": "string"}},
                {"name": "changeType", "type": {
                    "type": "enum", "name": "ChangeType", "symbols": [
                        "CREATE", "UPDATE", "DELETE", "UNDELETE",
                        "GAP_CREATE", "GAP_UPDATE", "GAP_DELETE",
                        "GAP_UNDELETE", "GAP_OVERFLOW"]}},
                {"name": "changeOrigin", "type": "string"},
                {"name": "transactionKey", "type": "string"},
                {"name": "sequenceNumber", "type": "int"},
                {"name": "commitTimestamp", "type": "long"},
                {"name": "commitNumber", "type": "long"},
                {"name": "commitUser", "type": "string"},
                {"name": "nulledFields", "type": {"type": "array",
                                                  "items": "string"}},
                {"name": "diffFields", "type": {"type": "array",
                                                "items": "string"}},
                {"name": "changedFields", "type": {"type": "array",
                                                   "items": "string"}}]}},
        {"name": "Name", "type": ["null", "string"], "default": None},
        {"name": "AnnualRevenue", "type": ["null", "double"],
         "default": None},
        {"name": "IsActive__c", "type": ["null", "boolean"],
         "default": None},
        {"name": "BillingAddress", "type": ["null", {
            "type": "record", "name": "Address", "fields": [
                {"name": "Street", "type": ["null", "string"],
                 "default": None},
                {"name": "City", "type": ["null", "string"],
                 "default": None}]}], "default": None},
        {"name": "LastModifiedDate", "type": ["null", "long"],
         "default": None}]}

WHEN = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)


def _stamp(at):
    return at.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


class Org:
    def __init__(self):
        self.records = {
            "001A": {"Id": "001A", "Name": "Acme", "AnnualRevenue": 1000.5,
                     "IsActive__c": True, "BillingStreet": "1 Main",
                     "BillingCity": "Bangkok", "Score__c": 1.5,
                     "LastModifiedDate": _stamp(WHEN)},
            "001B": {"Id": "001B", "Name": "Beta", "AnnualRevenue": 20,
                     "IsActive__c": False, "BillingStreet": None,
                     "BillingCity": "Hanoi", "Score__c": None,
                     "LastModifiedDate": _stamp(WHEN)},
            "001D": {"Id": "001D", "Name": "Delta", "AnnualRevenue": None,
                     "IsActive__c": True, "BillingStreet": "4 Way",
                     "BillingCity": "Tokyo", "Score__c": 2.25,
                     "LastModifiedDate": _stamp(WHEN)}}
        self.extra_count = 0
        self.bulk = False
        self.asked = []
        self.events = []
        self.fetches = []
        self.metadata = []


def _serve(org):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, body, status=200, kind="application/json",
                  headers=()):
            raw = (body if isinstance(body, str) else
                   json.dumps(body)).encode()
            self.send_response(status)
            self.send_header("Content-Type", kind)
            for k, v in headers:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _base(self):
            return f"http://127.0.0.1:{self.server.server_port}"

        def do_POST(self):
            parts = urlsplit(self.path)
            org.asked.append(("POST", parts.path))
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            if parts.path == "/services/oauth2/token":
                return self._send({"access_token": "tok",
                                   "instance_url": self._base(),
                                   "id": f"{self._base()}/id/{ORG}/005X"})
            if parts.path == f"{BASE}/jobs/query":
                if not org.bulk:
                    return self._send([{"errorCode": "API_DISABLED_FOR_ORG",
                                        "message": "bulk refused"}], 400)
                return self._send({"id": "750J", "state": "UploadComplete"})
            return self._send({"error": parts.path}, 404)

        def do_GET(self):
            parts = urlsplit(self.path)
            q = {k: v[0] for k, v in parse_qs(parts.query).items()}
            org.asked.append(("GET", parts.path, q.get("q", "")))
            if self.headers.get("Authorization") != "Bearer tok":
                return self._send([{"errorCode": "INVALID_SESSION_ID"}], 401)
            if parts.path == f"{BASE}/sobjects/Account/describe":
                return self._send({"name": "Account", "fields": FIELDS})
            if parts.path == f"{BASE}/jobs/query/750J":
                return self._send({"id": "750J", "state": "JobComplete"})
            if parts.path == f"{BASE}/jobs/query/750J/results":
                cols = [f["name"] for f in FIELDS
                        if f["type"] not in ("address", "base64")]
                out = io.StringIO()
                out.write(",".join(cols) + "\n")
                for r in org.records.values():
                    out.write(",".join(
                        "" if r.get(c) is None else
                        str(r[c]).lower() if isinstance(r[c], bool) else
                        f'"{r[c]}"' for c in cols) + "\n")
                return self._send(out.getvalue(), kind="text/csv",
                                  headers=[("Sforce-Locator", "null")])
            if parts.path.startswith(f"{BASE}/query/next-"):
                at = int(parts.path.rsplit("-", 1)[1])
                return self._page(sorted(org.records.values(),
                                         key=lambda r: r["Id"]), at)
            if parts.path == f"{BASE}/query":
                soql = q["q"]
                if soql.startswith("SELECT COUNT()"):
                    return self._send({"totalSize": len(org.records)
                                       + org.extra_count, "done": True,
                                       "records": []})
                m = re.search(r"WHERE Id IN \((.*)\)", soql)
                rows = sorted(org.records.values(), key=lambda r: r["Id"])
                if m:
                    ids = set(re.findall(r"'([^']*)'", m.group(1)))
                    return self._send({"totalSize": 0, "done": True,
                                       "records": [dict(r, attributes={})
                                                   for r in rows
                                                   if r["Id"] in ids]})
                return self._page(rows, 0)
            return self._send({"error": parts.path}, 404)

        def _page(self, rows, at):
            page = rows[at:at + 2]
            body = {"totalSize": len(rows), "done": at + 2 >= len(rows),
                    "records": [dict(r, attributes={"type": "Account"})
                                for r in page]}
            if at + 2 < len(rows):
                body["nextRecordsUrl"] = f"{BASE}/query/next-{at + 2}"
            return self._send(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _events(org):
    import grpc

    from migkit.engines.saas_salesforce import pubsub_messages
    m = pubsub_messages()

    def subscribe(requests, context):
        org.metadata.append(dict(context.invocation_metadata()))
        for req in requests:
            org.fetches.append(req)
            start = 0 if req.replay_preset == m["EARLIEST"] else \
                int.from_bytes(bytes(req.replay_id), "big")
            evs = org.events[start:start + req.num_requested]
            yield m["FetchResponse"](
                events=[m["ConsumerEvent"](
                    event=m["ProducerEvent"](id=f"e{start + i}",
                                             schema_id="S1", payload=p),
                    replay_id=(start + i + 1).to_bytes(8, "big"))
                    for i, p in enumerate(evs)],
                latest_replay_id=(start + len(evs)).to_bytes(8, "big"),
                rpc_id="r", pending_num_requested=req.num_requested
                - len(evs))

    def schema(req, context):
        return m["SchemaInfo"](schema_json=json.dumps(SCHEMA), schema_id="S1")
    server = grpc.server(futures.ThreadPoolExecutor(4))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "eventbus.v1.PubSub", {
            "Subscribe": grpc.stream_stream_rpc_method_handler(
                subscribe,
                request_deserializer=m["FetchRequest"].FromString,
                response_serializer=m["FetchResponse"].SerializeToString),
            "GetSchema": grpc.unary_unary_rpc_method_handler(
                schema, request_deserializer=m["SchemaRequest"].FromString,
                response_serializer=m["SchemaInfo"].SerializeToString)}),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def _event(kind, ids, at, changed=(), nulled=(), diff=(), **values):
    import fastavro
    record = {"ChangeEventHeader": {
        "entityName": "Account", "recordIds": list(ids), "changeType": kind,
        "changeOrigin": "", "transactionKey": "t", "sequenceNumber": 1,
        "commitTimestamp": int(at.timestamp() * 1000), "commitNumber": 1,
        "commitUser": "005X", "nulledFields": list(nulled),
        "diffFields": list(diff), "changedFields": list(changed)},
        "Name": None, "AnnualRevenue": None, "IsActive__c": None,
        "BillingAddress": None, "LastModifiedDate": None}
    record.update(values)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(SCHEMA), record)
    return buf.getvalue()


@pytest.fixture
def org():
    o = Org()
    srv = _serve(o)
    grpc_server, grpc_port = _events(o)
    o.base = f"http://127.0.0.1:{srv.server_port}"
    o.grpc = f"127.0.0.1:{grpc_port}"
    yield o
    grpc_server.stop(0)
    srv.shutdown()
    srv.server_close()


def _hop(org, root):
    target = root / "target.db"
    if not target.exists():
        sqlite3.connect(target).close()
    cfg = root / "sf.json"
    cfg.write_text(json.dumps({"client_id": "CHANGE_ME",
                               "client_secret": "CHANGE_ME"}))
    hop = Hop(name="sf", engine="hetero",
              source=Endpoint(options={
                  "kind": "salesforce", "program": org.base,
                  "config": str(cfg), "streams": ["Account"],
                  "pubsub_endpoint": org.grpc, "pubsub_tls": False}),
              target=Endpoint(options={"path": str(target)}),
              databases=["salesforce"],
              options={"source_engine": "saas", "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: root
    return hop


def _copy(hop):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(hop)
    ck = _Checkpoint(hop.report_dir() / "move.json")
    eng.create_missing("salesforce", print)
    for sch, t in eng.list_move_tables("salesforce"):
        eng.move_table("salesforce", sch, t, 500, ck, print)
    return eng


def _check(hop, monkeypatch):
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(HeteroEngine, "_drill", lambda self, *a, **k: "")
    return {r.scope: r for r in HeteroEngine(hop).check_data("salesforce")}


def test_an_objects_fields_are_its_columns(org, tmp_path):
    from migkit.engines.saas import SaasEngine
    eng = SaasEngine(_hop(org, tmp_path))
    cols = dict(eng.neutral_columns("src", "salesforce", "Account"))
    assert cols == {"Id": "text(18)", "Name": "text(255)",
                    "AnnualRevenue": "decimal(18,2)", "IsActive__c": "boolean",
                    "BillingStreet": "text(255)", "BillingCity": "text(40)",
                    "Score__c": "decimal(18,2)",
                    "LastModifiedDate": "timestamptz"}
    assert eng.neutral_key("src", "salesforce", "Account") == ["Id"]
    said = eng.not_compared("salesforce", "Account")
    assert "Logo__c" in said and "Score__c: a formula field" in said
    # signed in by the connected app, the org read off the answer
    assert ("POST", "/services/oauth2/token") in org.asked
    assert eng.program("src")._auth()["org"] == ORG


def test_a_copy_goes_by_rest_where_the_bulk_job_is_refused_said(
        org, tmp_path, monkeypatch, capsys):
    hop = _hop(org, tmp_path)
    _copy(hop)
    out = capsys.readouterr().out
    assert "switching to the REST query's pages" in out, out
    got = _check(hop, monkeypatch)
    row = got["salesforce.Account"]
    assert row.status == "ok", row.detail
    assert "own count (3) agrees" in row.detail
    assert "Score__c: a formula field" in row.detail


def test_a_copy_by_the_bulk_job(org, tmp_path, monkeypatch):
    org.bulk = True
    hop = _hop(org, tmp_path)
    _copy(hop)
    assert ("GET", f"{BASE}/jobs/query/750J/results", "") in org.asked
    got = _check(hop, monkeypatch)
    assert got["salesforce.Account"].status == "ok", got


def test_a_read_that_missed_a_record_is_found_by_the_orgs_own_count(
        org, tmp_path, monkeypatch):
    hop = _hop(org, tmp_path)
    _copy(hop)
    org.extra_count = 1
    got = _check(hop, monkeypatch)
    row = got["salesforce.Account"]
    assert row.status == "diff", row.detail
    assert "counts 4 records and 3 were read" in row.detail


def test_the_follow_carries_the_orgs_change_events(org, tmp_path,
                                                   monkeypatch):
    from migkit.engines.saas import SaasEngine
    hop = _hop(org, tmp_path)
    src = SaasEngine(hop)
    start = src.copy_point("src", "salesforce")
    began = datetime.datetime.fromisoformat(start["start"])
    eng = _copy(hop)
    after = began + datetime.timedelta(seconds=5)
    # before the copy began: in the copy already, passed over
    org.events.append(_event("UPDATE", ["001A"],
                             began - datetime.timedelta(minutes=10),
                             changed=["0x2"], Name="Old"))
    # Name and BillingAddress.City changed, AnnualRevenue set to NULL
    org.records["001A"].update(Name="Acme Corp", BillingCity="Chiang Mai",
                               AnnualRevenue=None)
    org.events.append(_event("UPDATE", ["001A"], after,
                             changed=["0x12", "4-0x2"], nulled=["0x4"],
                             Name="Acme Corp",
                             BillingAddress={"Street": None,
                                             "City": "Chiang Mai"}))
    new_at = datetime.datetime(2026, 9, 30, 12, 0,
                               tzinfo=datetime.timezone.utc)
    org.records["001C"] = {"Id": "001C", "Name": "New",
                           "AnnualRevenue": 7.25, "IsActive__c": True,
                           "BillingStreet": "9 Rd", "BillingCity": "Phuket",
                           "Score__c": None,
                           "LastModifiedDate": _stamp(new_at)}
    org.events.append(_event(
        "CREATE", ["001C"], after, Name="New", AnnualRevenue=7.25,
        IsActive__c=True, BillingAddress={"Street": "9 Rd",
                                          "City": "Phuket"},
        LastModifiedDate=int(new_at.timestamp() * 1000)))
    del org.records["001B"]
    org.events.append(_event("DELETE", ["001B"], after))
    org.records["001D"]["BillingCity"] = "Osaka"
    org.events.append(_event("GAP_UPDATE", ["001D"], after))
    changes, token = SaasEngine(hop).neutral_changes("src", "salesforce",
                                                     start)
    kinds = [(c["op"], c["key"]["Id"]) for c in changes]
    assert kinds == [("update", "001A"), ("insert", "001C"),
                     ("delete", "001B"), ("update", "001D")], kinds
    assert changes[0]["values"] == {"Name": "Acme Corp",
                                    "BillingCity": "Chiang Mai",
                                    "AnnualRevenue": None}
    assert changes[1]["values"]["AnnualRevenue"] == Decimal("7.25")
    assert changes[1]["values"]["BillingStreet"] == "9 Rd"
    assert changes[3]["values"]["BillingCity"] == "Osaka"
    # from the oldest event kept, the org's own tenant named
    assert org.fetches[0].replay_preset == 1
    assert org.metadata[0]["tenantid"] == ORG
    assert token["events"]["Account"]["replay"] == \
        (5).to_bytes(8, "big").hex()
    assert "read_at" in token
    eng.dst_engine.neutral_apply("dst", "salesforce", changes)
    got = _check(hop, monkeypatch)
    assert got["salesforce.Account"].status == "ok", \
        got["salesforce.Account"].detail
    # and on from the replay id it kept: nothing more
    more, _ = SaasEngine(hop).neutral_changes("src", "salesforce", token)
    assert more == []
    assert org.fetches[-1].replay_preset == 2
    assert bytes(org.fetches[-1].replay_id) == (5).to_bytes(8, "big")


def test_a_copy_older_than_the_events_kept_cannot_be_followed(org,
                                                              tmp_path):
    from migkit.engines.saas import SaasEngine
    hop = _hop(org, tmp_path)
    old = (datetime.datetime.now(datetime.timezone.utc)
           - datetime.timedelta(days=4)).isoformat()
    with pytest.raises(SystemExit) as e:
        SaasEngine(hop).neutral_changes("src", "salesforce", {
            "state": None, "gen": 0,
            "events": {"Account": {"replay": None, "since": old}}})
    assert "more than 72 hours" in str(e.value)


def test_a_record_is_asked_by_its_key(org, tmp_path):
    from migkit.engines.saas import SaasEngine
    eng = SaasEngine(_hop(org, tmp_path))
    got = eng.neutral_rows_by_key("src", "salesforce", "Account",
                                  [("Id", "text"), ("Name", "text")],
                                  ["Id"], [("001B",), ("001Z",)])
    assert [v[1] for v in got.values()] == ["Beta"]
    assert any("WHERE Id IN ('001B', '001Z')" in a[2] for a in org.asked
               if a[0] == "GET")


def test_the_apis_every_option_is_used_or_explained():
    from migkit import tooluse
    import migkit.engines.saas_salesforce  # noqa: F401
    assert tooluse.unexplained("salesforce-api") == []
    assert tooluse.stale("salesforce-api") == []
    assert "Subscribe" in tooluse.used("salesforce-api", "pubsub")
