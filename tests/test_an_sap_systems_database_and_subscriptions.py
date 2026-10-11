"""The two things an SAP source still lacked (backlog ER6).

* **The runtime licence.** A database bought through SAP is used under a
  runtime licence, which allows no program that is not SAP's to read or
  write it (SAP Note 581312). A side of Oracle, SQL Server, Db2, ASE or
  HANA holding an SAP system's tables - its clients and its data
  dictionary together, `T000` and `DD02L` - is refused before anything
  reads it (in `assess`, the gate before a step, and every check), unless
  the hop says its licence was bought apart from SAP (`sap_licence:
  full-use`); `runtime` refuses it whatever else is said.
* **The subscription.** The delta subscriptions an ODP follow opens are
  the one thing of migkit's own on the SAP system: once the follow ends
  for good - the cutover's point of no return - each is ended through the
  service's `TerminateDeltasFor<entity set>`, a POST carrying the CSRF
  token Gateway hands out; one the service offers no way to end is said,
  left for ODQMON, and one it refuses is said with what it answered.

Held without an SAP system: the catalogues and the service answered from
recorded shapes.
"""
import json
from types import SimpleNamespace

import pytest

from migkit.config import Endpoint, Hop


def _hop(tmp_path, engine="oracle", **options):
    hop = Hop(name="erp", engine=engine,
              source=Endpoint(host="10.0.0.7", user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.8", user="migkit",
                              password="CHANGE_ME"),
              databases=["SAPSR3"], options=options)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


@pytest.fixture
def catalogue(monkeypatch):
    """Which schemas each side's catalogue answers as holding an SAP
    system, and every statement asked."""
    got = {"src": [("SAPSR3",)], "dst": [], "asked": []}
    from migkit.engines.oracle import OracleEngine

    def rows(self, side, db, sql, args=None):
        got["asked"].append((side, sql))
        return list(got[side])
    monkeypatch.setattr(OracleEngine, "_rows", rows)
    return got


def test_a_side_holding_an_sap_system_is_refused_without_a_licence(
        tmp_path, catalogue):
    from migkit.engines import get_engine, sapodp
    hop = _hop(tmp_path)
    eng = get_engine(hop)
    rows = sapodp.licence_rows(hop, eng)
    assert [r["level"] for r in rows] == ["fail"], rows
    assert "SAPSR3" in rows[0]["detail"]
    assert "runtime licence" in rows[0]["detail"]
    assert "[fix]" in rows[0]["detail"] and "sap_licence: full-use" in \
        rows[0]["detail"]
    # the clients and the dictionary together, asked of each side once
    assert all("'T000'" in sql and "'DD02L'" in sql
               for _, sql in catalogue["asked"])
    with pytest.raises(SystemExit, match="nothing was read"):
        sapodp.refuse_runtime(hop, eng)


def test_full_use_says_it_and_goes_on(tmp_path, catalogue):
    from migkit.engines import get_engine, sapodp
    hop = _hop(tmp_path, sap_licence="full-use")
    eng = get_engine(hop)
    rows = sapodp.licence_rows(hop, eng)
    assert [r["level"] for r in rows] == ["pass"]
    sapodp.refuse_runtime(hop, eng)


def test_runtime_is_refused_whatever_else_is_said(tmp_path, catalogue):
    from migkit.engines import get_engine, sapodp
    hop = _hop(tmp_path, sap_licence="runtime")
    rows = sapodp.licence_rows(hop, get_engine(hop))
    assert rows[0]["level"] == "fail"
    assert "(sap_licence: runtime)" in rows[0]["detail"]


def test_a_database_with_no_sap_system_is_not_named(tmp_path, catalogue):
    from migkit.engines import get_engine, sapodp
    catalogue["src"] = []
    hop = _hop(tmp_path)
    assert sapodp.licence_rows(hop, get_engine(hop)) == []


def test_a_pairs_source_is_asked_by_its_own_engine(tmp_path, catalogue):
    from migkit.engines import get_engine, sapodp
    hop = _hop(tmp_path, engine="hetero", source_engine="oracle",
               target_engine="postgres")
    rows = sapodp.licence_rows(hop, get_engine(hop))
    assert [r["item"] for r in rows] == ["source: a direct read"]


def test_the_gate_before_a_step_holds_to_it(tmp_path, catalogue):
    from migkit import ready
    from migkit.engines import get_engine
    hop = _hop(tmp_path)
    got = ready._sap_licence(hop, get_engine(hop))
    assert got and got[0].gates and got[0].level == "fail"
    assert got[0].fix.startswith("read the application's data")


def test_a_licence_of_another_word_is_refused_at_load(tmp_path):
    from migkit.engines import sapodp
    with pytest.raises(SystemExit, match="full-use or runtime"):
        sapodp.licence(_hop(tmp_path, sap_licence="maybe"))
    assert sapodp.licence(_hop(tmp_path, sap_licence="Full_Use")) == \
        "full-use"


# ---- the subscriptions ended at the point of no return ------------------

class _Answer:
    def __init__(self, status=200, text="", headers=None):
        self.status_code, self.text = status, text
        self.headers = headers or {}


class _Session:
    def __init__(self, refuse=()):
        self.calls, self.refuse = [], set(refuse)

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(("GET", url, dict(headers or {})))
        if (headers or {}).get("X-CSRF-Token") == "Fetch":
            return _Answer(headers={"x-csrf-token": "tok-1"})
        return _Answer()

    def post(self, url, params=None, headers=None, timeout=None):
        self.calls.append(("POST", url, dict(headers or {})))
        if any(url.endswith(r) for r in self.refuse):
            return _Answer(403, "not authorised for ODQ")
        return _Answer(204)


def _sap(tmp_path, monkeypatch, imports, session):
    from migkit.engines.sapodp import INITIAL, SapOdpEngine
    hop = Hop(name="odp", engine="hetero",
              source=Endpoint(host="erp.example.com", user="EXTRACT",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.8", user="migkit",
                              password="CHANGE_ME"),
              databases=["ZSALES_SRV"],
              options={"source_engine": "sapodp",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    (tmp_path / INITIAL).write_text(json.dumps({
        "FactsOfOrders": {"asked": "2026-10-04T01:00:00",
                          "token": "D20261004010000_000001"},
        "FactsOfItems": {"asked": "2026-10-04T01:00:00",
                         "token": "D20261004010000_000002"},
        "AttrOfCustomer": {"asked": "2026-10-04T01:00:00",
                           "token": "D20261004010000_000003"},
        "TextOfRegion": {"asked": "2026-10-04T01:00:00", "token": None}}))
    eng = SapOdpEngine(hop)
    schema = SimpleNamespace(function_imports=[
        SimpleNamespace(name=n, http_method=m) for n, m in imports])
    monkeypatch.setattr(SapOdpEngine, "_service",
                        lambda self, side, db: (SimpleNamespace(
                            schema=schema), None))
    monkeypatch.setattr(SapOdpEngine, "_session",
                        lambda self, side: session)
    return eng


def test_each_subscription_the_follow_opened_is_ended(tmp_path,
                                                      monkeypatch):
    from migkit.engines.sapodp import INITIAL
    session = _Session(refuse=("TerminateDeltasForFactsOfItems",))
    eng = _sap(tmp_path, monkeypatch,
               [("TerminateDeltasForFactsOfOrders", "POST"),
                ("TerminateDeltasForFactsOfItems", "POST")], session)
    ended, left = eng.follow_left("ZSALES_SRV")
    assert ended == ["FactsOfOrders"]
    # a refusal said with what the service answered; one the service
    # offers no way to end left for ODQMON; one never initialised not
    # touched
    assert any(x.startswith("FactsOfItems:") and "403" in x for x in left)
    assert any(x.startswith("AttrOfCustomer:") and "ODQMON" in x
               for x in left)
    assert not any("TextOfRegion" in c[1] for c in session.calls)
    # the POST carries the token a GET asked for
    posts = [c for c in session.calls if c[0] == "POST"]
    assert posts[0][1].endswith("TerminateDeltasForFactsOfOrders")
    assert posts[0][2]["X-CSRF-Token"] == "tok-1"
    kept = json.loads((tmp_path / INITIAL).read_text())
    assert kept["FactsOfOrders"].get("ended")
    # ended once: a second run of the point of no return asks no more
    session.calls.clear()
    ended, _ = eng.follow_left("ZSALES_SRV")
    assert ended == []
    assert not any(c[1].endswith("TerminateDeltasForFactsOfOrders")
                   for c in session.calls)


def test_the_point_of_no_return_ends_them_through_the_source_engine(
        tmp_path):
    from migkit import cutover
    said, kept = [], []

    class Source:
        def follow_left(self, db):
            return (["FactsOfOrders"], ["AttrOfCustomer: end it in ODQMON"])

    pair = SimpleNamespace(src_engine=Source(), dst_engine=None,
                           CANON_ENGINE="hetero")
    hop = _hop(tmp_path, engine="hetero", source_engine="sapodp",
               target_engine="postgres")
    got = cutover._follow_left(hop, pair, ["ZSALES_SRV"], kept, said.append)
    assert got == {"ZSALES_SRV": ["FactsOfOrders"]}
    assert kept == ["ZSALES_SRV: AttrOfCustomer: end it in ODQMON"]
    assert any("subscriptions on the old source ended" in s for s in said)
    lines = cutover._no_return_plan(hop, ["ZSALES_SRV"],
                                    SimpleNamespace(end=None, origin=None),
                                    {}, False)
    assert any("delta subscriptions" in x for x in lines)
