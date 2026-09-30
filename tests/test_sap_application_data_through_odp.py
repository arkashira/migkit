"""SAP application data through ODP over OData (backlog ER6), held to a
local service that answers as Gateway's ODP services are documented to:
metadata with SAP's annotations, pages by `odata.maxpagesize` and
`__next`, a delta initialisation by `odata.track-changes` whose last page
carries `__delta`, deltas by `!deltatoken` with `ODQ_CHANGEMODE` and
`ODQ_ENTITYCNTR`, `DeltaLinksOf<entity set>`, `SubscribedTo<entity set>`,
`$count`, and an aggregate entity type's sums. No SAP system was
reachable; what is held is what migkit makes of those shapes.

What is held: an entity set's columns, key and types as its metadata
gives them; a copy into SQLite that is the extractor's delta
initialisation, its token kept for the follow; the follow's changes - a
before image passed over, a delete, a new row - applied and proved equal;
the fence by the queue's own request times; a follow with no
initialisation after its point refused; a subscription ended said as
lost; a key read by `$filter`, and read whole where the service refuses
the filter, said; and a read that missed a row - which a copy and a check
through the same pages would both miss - found by the service's own
count, and a sum found by its own sums.
"""
import datetime
import email.utils
import json
import re
import sqlite3
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from migkit.config import Endpoint, Hop

ROOT = "/sap/opu/odata/sap/ZSALES_SRV/"

METADATA = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0"
 xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx"
 xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
 xmlns:sap="http://www.sap.com/Protocols/SAPData">
 <edmx:DataServices m:DataServiceVersion="2.0">
  <Schema Namespace="ZSALES_SRV"
   xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
   <EntityType Name="EntityOfSalesDocType">
    <Key><PropertyRef Name="VBELN"/><PropertyRef Name="POSNR"/></Key>
    <Property Name="VBELN" Type="Edm.String" Nullable="false"
     MaxLength="10"/>
    <Property Name="POSNR" Type="Edm.String" Nullable="false"
     MaxLength="6"/>
    <Property Name="ERDAT" Type="Edm.DateTime" Precision="0"
     sap:display-format="Date"/>
    <Property Name="ERZET" Type="Edm.Time" Precision="0"/>
    <Property Name="NETWR" Type="Edm.Decimal" Precision="15" Scale="2"/>
    <Property Name="WAERK" Type="Edm.String" MaxLength="5"/>
    <Property Name="ODQ_CHANGEMODE" Type="Edm.String" MaxLength="1"/>
    <Property Name="ODQ_ENTITYCNTR" Type="Edm.Decimal" Precision="19"
     Scale="0"/>
   </EntityType>
   <EntityType Name="DeltaLinksOfEntityOfSalesDocType">
    <Key><PropertyRef Name="DeltaToken"/></Key>
    <Property Name="DeltaToken" Type="Edm.String" Nullable="false"
     MaxLength="32"/>
    <Property Name="CreatedAt" Type="Edm.DateTime" Precision="0"/>
    <Property Name="IsInitialLoad" Type="Edm.Boolean"/>
   </EntityType>
   <EntityType Name="FactsOfRevenueType" sap:semantics="aggregate">
    <Key><PropertyRef Name="ID"/></Key>
    <Property Name="ID" Type="Edm.String" Nullable="false" MaxLength="10"/>
    <Property Name="REGION" Type="Edm.String" MaxLength="4"
     sap:aggregation-role="dimension"/>
    <Property Name="REVENUE" Type="Edm.Decimal" Precision="15" Scale="2"
     sap:aggregation-role="measure"/>
   </EntityType>
   <EntityContainer Name="ZSALES_SRV_Entities"
    m:IsDefaultEntityContainer="true">
    <EntitySet Name="EntityOfSalesDoc"
     EntityType="ZSALES_SRV.EntityOfSalesDocType"/>
    <EntitySet Name="DeltaLinksOfEntityOfSalesDoc"
     EntityType="ZSALES_SRV.DeltaLinksOfEntityOfSalesDocType"/>
    <EntitySet Name="FactsOfRevenue"
     EntityType="ZSALES_SRV.FactsOfRevenueType"/>
    <FunctionImport Name="SubscribedToEntityOfSalesDoc"
     ReturnType="Edm.Boolean" m:HttpMethod="GET"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>
"""


def _ms(day):
    at = datetime.datetime(2024, 1, day, tzinfo=datetime.timezone.utc)
    return f"/Date({int(at.timestamp() * 1000)})/"


class Gateway:
    """The service's state: rows, the delta queue's log, the delta links
    issued, and what a test sets to misbehave."""

    def __init__(self):
        self.now = datetime.datetime(2026, 10, 1, 8, 0, 0,
                                     tzinfo=datetime.timezone.utc)
        self.rows = {}
        for i in range(1, 8):
            self.rows[(f"{i:010d}", "000010")] = {
                "VBELN": f"{i:010d}", "POSNR": "000010", "ERDAT": _ms(i),
                "ERZET": f"PT{i:02d}H30M00S", "NETWR": f"{i}00.50",
                "WAERK": "THB"}
        self.facts = {f"R{i}": {"ID": f"R{i}", "REGION": "APAC",
                                "REVENUE": f"{i}.25"} for i in range(1, 5)}
        self.log, self.seq, self.links = [], 0, []
        self.subscribed = False
        self.short = False          # full reads pass a row over
        self.sum_off = False        # the service's own sums are wrong
        self.refuse_filter = False
        self.page = 3
        self.asked = []

    def tick(self, seconds=5):
        self.now += datetime.timedelta(seconds=seconds)

    def token(self):
        return f"D{self.now:%Y%m%d%H%M%S}_{self.seq:09d}"

    def change(self, mode, row, counter="1"):
        self.seq += 1
        self.log.append((self.seq, dict(row, ODQ_CHANGEMODE=mode,
                                        ODQ_ENTITYCNTR=counter)))

    def update(self, key, **values):
        before = dict(self.rows[key])
        self.change("U", before, "-1")
        self.rows[key].update(values)
        self.change("U", self.rows[key])

    def delete(self, key):
        self.change("D", self.rows.pop(key), "-1")

    def insert(self, row):
        self.rows[(row["VBELN"], row["POSNR"])] = row
        self.change("C", row)


def _serve(gw):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def date_time_string(self, timestamp=None):
            return email.utils.formatdate(gw.now.timestamp(), usegmt=True)

        def _send(self, body, status=200, kind="application/json"):
            raw = body if isinstance(body, bytes) else (
                body if isinstance(body, str) else json.dumps(body)).encode()
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            parts = urlsplit(self.path)
            q = {k: v[0] for k, v in parse_qs(parts.query).items()}
            prefer = self.headers.get("Prefer") or ""
            gw.asked.append((parts.path, q, prefer))
            name = parts.path[len(ROOT):] if parts.path.startswith(ROOT) \
                else None
            if name is None:
                return self._send({"error": "no such service"}, 404)
            if name == "":
                return self._send({"d": {"EntitySets": []}})
            if name == "$metadata":
                return self._send(METADATA, kind="application/xml")
            if name == "SubscribedToEntityOfSalesDoc":
                return self._send({"d": {"SubscribedToEntityOfSalesDoc":
                                         gw.subscribed}})
            if name == "DeltaLinksOfEntityOfSalesDoc":
                return self._send({"d": {"results": [
                    {"DeltaToken": t, "CreatedAt": c, "IsInitialLoad": i}
                    for t, c, i in gw.links]}})
            if name.endswith("/$count"):
                table = name[:-len("/$count")]
                n = len(gw.rows) if table == "EntityOfSalesDoc" \
                    else len(gw.facts)
                return self._send(str(n), kind="text/plain")
            if name == "FactsOfRevenue":
                if q.get("$select") == "REVENUE":
                    total = sum(Decimal(r["REVENUE"])
                                for r in gw.facts.values())
                    if gw.sum_off:
                        total += 1
                    return self._send({"d": {"results": [
                        {"REVENUE": str(total)}]}})
                return self._send({"d": {"results": list(
                    gw.facts.values())}})
            if name != "EntityOfSalesDoc":
                return self._send({"error": name}, 404)
            if "!deltatoken" in q:
                at = int(q["!deltatoken"].strip("'").split("_")[1])
                rows = [r for s, r in gw.log if s > at]
                gw.links.append((gw.token(), _now_ms(gw), False))
                return self._send({"d": {"results": rows,
                                         "__delta": f"{ROOT}EntityOfSalesDoc"
                                                    f"?!deltatoken="
                                                    f"'{gw.token()}'"}})
            rows = [gw.rows[k] for k in sorted(gw.rows)]
            if "$filter" in q:
                if gw.refuse_filter:
                    return self._send({"error": "filter not supported"}, 400)
                want = set()
                for clause in q["$filter"].split(" or "):
                    got = dict(re.findall(r"(\w+) eq '([^']*)'", clause))
                    want.add((got["VBELN"], got["POSNR"]))
                rows = [r for r in rows if (r["VBELN"], r["POSNR"]) in want]
                return self._send({"d": {"results": rows}})
            if gw.short:
                rows = rows[:3] + rows[4:]
            skip = int(q.get("$skiptoken", 0))
            page = rows[skip:skip + gw.page]
            body = {"results": page}
            if skip + gw.page < len(rows):
                body["__next"] = (f"http://127.0.0.1:{self.server.server_port}"
                                  f"{ROOT}EntityOfSalesDoc?$skiptoken="
                                  f"{skip + gw.page}")
            elif "odata.track-changes" in prefer:
                gw.subscribed = True
                gw.links.append((gw.token(), _now_ms(gw), True))
                body["__delta"] = (f"{ROOT}EntityOfSalesDoc?!deltatoken="
                                   f"'{gw.token()}'")
            return self._send({"d": body})

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _now_ms(gw):
    return f"/Date({int(gw.now.timestamp() * 1000)})/"


@pytest.fixture
def gateway():
    gw = Gateway()
    srv = _serve(gw)
    gw.port = srv.server_port
    yield gw
    srv.shutdown()
    srv.server_close()


def _hop(gw, root, exclude=()):
    target = root / "target.db"
    sqlite3.connect(target).close()
    hop = Hop(name="sap", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=gw.port, user="MIGKIT",
                              password="CHANGE_ME", options={"tls": False}),
              target=Endpoint(options={"path": str(target)}),
              databases=["ZSALES_SRV"], exclude=list(exclude),
              options={"source_engine": "sapodp",
                       "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: root
    return hop


def _copy(hop):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(hop)
    ck = _Checkpoint(hop.report_dir() / "move.json")
    said = []
    eng.create_missing("ZSALES_SRV", said.append)
    for sch, t in eng.list_move_tables("ZSALES_SRV"):
        eng.move_table("ZSALES_SRV", sch, t, 500, ck, said.append)
    return eng, said


def _check(hop, monkeypatch=None):
    from migkit.engines.hetero import HeteroEngine
    if monkeypatch is not None:
        # the drill says which rows differ; the verdict is decided before
        monkeypatch.setattr(HeteroEngine, "_drill", lambda self, *a, **k: "")
    return {r.scope: r for r in HeteroEngine(hop).check_data("ZSALES_SRV")}


def test_an_entity_set_is_its_metadata(gateway, tmp_path):
    from migkit import canon
    from migkit.engines.sapodp import SapOdpEngine
    eng = SapOdpEngine(_hop(gateway, tmp_path))
    assert eng.neutral_tables("src", "ZSALES_SRV") == ["EntityOfSalesDoc",
                                                       "FactsOfRevenue"]
    cols = dict(eng.neutral_columns("src", "ZSALES_SRV", "EntityOfSalesDoc"))
    assert cols == {"VBELN": "Edm.String(10)", "POSNR": "Edm.String(6)",
                    "ERDAT": "Edm.Date", "ERZET": "Edm.Time",
                    "NETWR": "Edm.Decimal(15,2)", "WAERK": "Edm.String(5)"}
    assert {n: canon.type_class("sapodp", t) for n, t in cols.items()} == {
        "VBELN": "text", "POSNR": "text", "ERDAT": "date", "ERZET": "time",
        "NETWR": "decimal", "WAERK": "text"}
    assert eng.neutral_key("src", "ZSALES_SRV", "EntityOfSalesDoc") == [
        "VBELN", "POSNR"]
    assert eng.count("src", "ZSALES_SRV", "EntityOfSalesDoc") == 7
    rows = [r for b in eng.neutral_batches(
        "src", "ZSALES_SRV", "EntityOfSalesDoc",
        [(n, canon.type_class("sapodp", t)) for n, t in cols.items()])
        for r in b]
    assert len(rows) == 7
    first = rows[0]
    assert first[2] == datetime.date(2024, 1, 1)
    assert first[3] == datetime.time(1, 30)
    assert first[4] == Decimal("100.50")
    # pages were read as the service hands them out, and no read that
    # was not asked to start a follow tracked changes
    assert all("track-changes" not in p for _, _, p in gateway.asked)
    assert any("$skiptoken" in q for _, q, _ in gateway.asked)


def test_a_copy_starts_the_follow_and_the_follow_carries_the_changes(
        gateway, tmp_path):
    from migkit.engines.sapodp import SapOdpEngine
    # an entity set that tracks no changes cannot be followed: refused,
    # named, before anything is read
    with pytest.raises(SystemExit) as e:
        SapOdpEngine(_hop(gateway, tmp_path)).copy_point("src",
                                                        "ZSALES_SRV")
    assert "FactsOfRevenue track no changes" in str(e.value)
    hop = _hop(gateway, tmp_path, exclude=["FactsOfRevenue"])
    src = SapOdpEngine(hop)
    start = src.copy_point("src", "ZSALES_SRV")
    assert start == {"EntityOfSalesDoc": {"init": "2026-10-01T08:00:00"}}
    gateway.tick()
    eng, _ = _copy(hop)
    # the copy's read was the delta initialisation, and its token is kept
    assert any("track-changes" in p for _, _, p in gateway.asked)
    kept = json.loads((tmp_path / "odp-initial-deltas.json").read_text())
    assert kept["EntityOfSalesDoc"]["token"] == "D20261001080005_000000000"
    got = _check(hop)
    assert got["ZSALES_SRV.EntityOfSalesDoc"].status == "ok", got
    assert "count (7) agree" in got["ZSALES_SRV.EntityOfSalesDoc"].detail
    # the source changes: an update (a before and an after image), a
    # delete, a new row
    gateway.tick()
    gateway.update(("0000000002", "000010"), NETWR="999.99")
    gateway.delete(("0000000003", "000010"))
    gateway.insert({"VBELN": "0000000009", "POSNR": "000020",
                    "ERDAT": _ms(9), "ERZET": "PT09H00M00S",
                    "NETWR": "9.00", "WAERK": "USD"})
    gateway.tick()
    want = SapOdpEngine(hop).log_position("src", "ZSALES_SRV")
    changes, token = SapOdpEngine(hop).neutral_changes(
        "src", "ZSALES_SRV", start)
    kinds = [(c["op"], c["key"]["VBELN"]) for c in changes]
    # the before image is passed over: the after image is the row
    assert kinds == [("update", "0000000002"), ("delete", "0000000003"),
                     ("insert", "0000000009")], kinds
    assert changes[0]["values"]["NETWR"] == Decimal("999.99")
    assert token["EntityOfSalesDoc"]["at"].endswith("_000000004")
    # held against the fence: a token of a request made after the moment
    assert SapOdpEngine.position_reached(token, want) is True
    later = {t: {"at": "D20261001090000_000000000", "next": None}
             for t in want}
    assert SapOdpEngine.position_reached(token, later) is False
    assert SapOdpEngine.position_reached(
        {t: {"at": "not a token"} for t in want}, want) is None
    eng.dst_engine.neutral_apply("dst", "ZSALES_SRV", changes)
    got = _check(hop)
    assert got["ZSALES_SRV.EntityOfSalesDoc"].status == "ok", got
    # read again from the same token, the delta repeats and changes nothing
    again, _ = SapOdpEngine(hop).neutral_changes("src", "ZSALES_SRV", {
        t: {"at": kept["EntityOfSalesDoc"]["token"], "next": None}
        for t in ("EntityOfSalesDoc",)})
    assert [(c["op"], c["key"]["VBELN"]) for c in again] == kinds


def test_a_value_different_on_the_target_is_a_difference(
        gateway, tmp_path, monkeypatch):
    hop = _hop(gateway, tmp_path)
    _copy(hop)
    con = sqlite3.connect(tmp_path / "target.db")
    con.execute('update "EntityOfSalesDoc" set "WAERK" = \'EUR\' where'
                ' "VBELN" = \'0000000005\'')
    con.commit()
    con.close()
    got = _check(hop, monkeypatch)
    assert got["ZSALES_SRV.EntityOfSalesDoc"].status == "diff", got


def test_a_read_that_missed_a_row_is_found_by_the_services_own_count(
        gateway, tmp_path, monkeypatch):
    """A copy and a check through the same pages both miss the row the
    pages pass over, and agree: only the service's own count says the read
    was short."""
    gateway.short = True
    hop = _hop(gateway, tmp_path)
    _copy(hop)
    got = _check(hop, monkeypatch)
    row = got["ZSALES_SRV.EntityOfSalesDoc"]
    assert row.status == "diff", row.detail
    assert "counts 7 rows and 6 were read" in row.detail
    assert "equal on both sides as read" in row.detail


def test_an_aggregates_sums_are_held_to_the_rows_read(gateway, tmp_path,
                                                      monkeypatch):
    hop = _hop(gateway, tmp_path)
    _copy(hop)
    got = _check(hop, monkeypatch)
    assert got["ZSALES_SRV.FactsOfRevenue"].status == "ok"
    assert "sums of REVENUE" in got["ZSALES_SRV.FactsOfRevenue"].detail
    gateway.sum_off = True
    got = _check(hop, monkeypatch)
    row = got["ZSALES_SRV.FactsOfRevenue"]
    assert row.status == "diff", row.detail
    assert "sums REVENUE to 12.00" in row.detail
    assert "the rows read sum to 11.00" in row.detail


def test_a_follow_with_no_initialisation_after_its_point_is_refused(
        gateway, tmp_path):
    from migkit.engines.sapodp import SapOdpEngine
    hop = _hop(gateway, tmp_path, exclude=["FactsOfRevenue"])
    src = SapOdpEngine(hop)
    # an initialisation made before the follow's point is not its start
    list(src.neutral_batches("src", "ZSALES_SRV", "EntityOfSalesDoc",
                             [("VBELN", "text")]))
    gateway.links.append(("D20261001075959_000000000",
                          "/Date(1790000000000)/", True))
    gateway.tick(60)
    start = src.copy_point("src", "ZSALES_SRV")
    with pytest.raises(SystemExit) as e:
        SapOdpEngine(hop).neutral_changes("src", "ZSALES_SRV", start)
    assert "no delta initialisation" in str(e.value)
    assert "--mode full+cdc" in str(e.value)


def test_a_subscription_that_was_ended_is_said_as_lost(gateway, tmp_path):
    from migkit.engines.sapodp import SapOdpEngine
    eng = SapOdpEngine(_hop(gateway, tmp_path))
    token = {"EntityOfSalesDoc": {"at": "D20261001080000_000000000",
                                  "next": None}}
    gateway.subscribed = True
    assert eng.position_lost("src", "ZSALES_SRV", token) is None
    gateway.subscribed = False
    said = eng.position_lost("src", "ZSALES_SRV", token)
    assert said and "no longer subscribed" in said and "ODQMON" in said


def test_a_key_is_asked_by_filter_or_read_whole_where_refused(gateway,
                                                              tmp_path):
    from migkit.engines.sapodp import SapOdpEngine
    eng = SapOdpEngine(_hop(gateway, tmp_path))
    cols = [("VBELN", "text"), ("POSNR", "text"), ("WAERK", "text")]
    keys = [("0000000004", "000010"), ("0000000099", "000010")]
    got = eng.neutral_rows_by_key("src", "ZSALES_SRV", "EntityOfSalesDoc",
                                  cols, ["VBELN", "POSNR"], keys)
    assert [v[2] for v in got.values()] == ["THB"]
    assert any("$filter" in q for _, q, _ in gateway.asked)
    gateway.refuse_filter = True
    lines = []
    eng = SapOdpEngine(_hop(gateway, tmp_path))
    eng.lend_log(lines.append)
    got = eng.neutral_rows_by_key("src", "ZSALES_SRV", "EntityOfSalesDoc",
                                  cols, ["VBELN", "POSNR"], keys)
    assert [v[2] for v in got.values()] == ["THB"]
    assert any("switching to the entity set read whole" in s
               for s in lines), lines


def test_the_librarys_every_feature_is_used_or_explained():
    from migkit import tooluse
    import migkit.engines.sapodp  # noqa: F401
    assert tooluse.unexplained("pyodata") == []
    assert tooluse.stale("pyodata") == []
    assert "count" in tooluse.used("pyodata", "request")
