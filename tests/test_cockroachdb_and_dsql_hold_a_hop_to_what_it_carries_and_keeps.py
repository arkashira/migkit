"""CockroachDB and Aurora DSQL hold a hop to what it carries and keeps
(P0-PARITY.scope, .verify), without a server.

* the change tail applies the operations the hop's `mapping.ops` carries and
  no others, to the pair each engine is carried by;
* a target kept read-only by an Aurora DSQL hop has the application's write
  grants taken and given back as the engine that keeps CockroachDB's does
  (`freeze.PgWireHold`), sessions it cannot end left alone and said;
* a table with no key, a column of another type, nullability or default,
  and a narrower column are found from what the catalogue answers - in
  CockroachDB's words for a keyless table (the hidden `rowid` key is no
  key of its rows) and in DSQL's.

Every statement here is answered by a stub: it proves the logic over the
catalogue's answers, not a server's answers.
"""
import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from migkit.engines import pgwire
from migkit.engines.cockroachdb import CockroachEngine
from migkit.engines.dsql import DsqlEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.ops", ("cockroachdb", "dsql")),
    ("cutover.read-only-target", "dsql"),
    ("verify.structure.keys", "cockroachdb"),
    ("verify.structure.columns", "cockroachdb"),
    ("verify.value.narrowing", "cockroachdb"),
]

EP = Endpoint(host="x", port=1, user="x", password="x")
ENGINES = [("cockroachdb", CockroachEngine), ("dsql", DsqlEngine)]


def _engine(tmp_path, name, cls, **hop):
    hop = Hop(name="t", engine=name, source=EP, target=EP, databases=["d"],
              **hop)
    hop.report_dir = lambda db=None: tmp_path
    return cls(hop)


def _ins(i):
    return canon.change("insert", "public.orders", {"id": i},
                        {"id": i, "v": "x"})


def _upd(i):
    return canon.change("update", "public.orders", {"id": i},
                        {"id": i, "v": "y"})


def _del(i):
    return canon.change("delete", "public.orders", {"id": i})


@pytest.mark.parametrize("name,cls", ENGINES)
def test_the_tail_applies_only_the_operations_the_hop_carries(
        tmp_path, name, cls):
    eng = _engine(tmp_path, name, cls,
                  mapping={"ops": {"public.orders": "insert, update"}})
    pair = eng._as_pair()
    assert isinstance(pair.src_engine, cls) and isinstance(pair.dst_engine,
                                                           cls)
    batch = [_ins(1), _del(2), _upd(3)]
    out = pair._carried("d", batch)
    # decided on the source's own operation, table by table: this one keeps
    # what the source deletes
    assert [(c["op"], c["key"]["id"]) for c in out] == [("insert", 1),
                                                        ("update", 3)]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_table_the_rule_does_not_name_carries_everything(tmp_path, name,
                                                           cls):
    eng = _engine(tmp_path, name, cls,
                  mapping={"ops": {"public.orders": "insert, update"}})
    other = canon.change("delete", "public.other", {"id": 1})
    assert eng._as_pair()._carried("d", [other]) == [other]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_hop_that_carries_everything_applies_everything(tmp_path, name,
                                                          cls):
    eng = _engine(tmp_path, name, cls)
    batch = [_ins(1), _del(2), _upd(3)]
    assert eng._as_pair()._carried("d", batch) == batch


# --- a target kept read-only ---------------------------------------------------

class _Eng:
    def __init__(self, kind):
        self.CANON_ENGINE = kind

    def _d(self, side, db):
        return db


class _Server:
    """An in-memory Aurora DSQL: roles with write grants on tables."""

    def __init__(self):
        self.grants = {"app": {("public", "t", "INSERT", False),
                               ("public", "u", "DELETE", False)},
                       "report": set()}
        self.log = []


def _held(tmp_path, server):
    from migkit import freeze
    hop = Hop(name="kt", engine="dsql", source=EP, target=EP,
              databases=["app"], options={"protect_target": True})
    hop.report_dir = lambda db="": tmp_path
    hold = freeze.PgWireHold(hop, _Eng("dsql"), "dst")
    hold.accounts = lambda db: sorted(server.grants)
    hold.grants = lambda db, who: [list(g) for g in
                                   sorted(server.grants.get(who, ()))]

    def run(db, *stmts):
        for sql in stmts:
            server.log.append(sql)
            verb, rest = sql.split(" ", 1)
            what = rest.split(" on table ")[1].split(" from ")[0].split(
                " to ")[0]
            sch, tbl = (p.strip('"') for p in what.split("."))
            priv = rest.split(" on table ")[0].upper()
            held = server.grants.setdefault(
                rest.split(" from ")[-1].split(" to ")[-1].split(
                    " with ")[0].strip('"'), set())
            row = (sch, tbl, priv, False)
            (held.discard if verb == "revoke" else held.add)(row)
    hold.run = run
    hold.wider = lambda db: []
    return hold


def test_a_dsql_target_is_kept_by_taking_the_applications_write_grants(
        tmp_path):
    from migkit import freeze
    server = _Server()
    before = {k: set(v) for k, v in server.grants.items()}
    hold = _held(tmp_path, server)
    said = []
    hold.keep("app", "pgw_grants", said.append)
    assert server.grants["app"] == set()
    record = freeze.state(hold.hop, "app", "dst")
    assert sorted(map(tuple, record["roles"]["app"]["revoked"])) == \
        sorted(before["app"])
    assert server.log == ['revoke insert on table "public"."t" from "app"',
                          'revoke delete on table "public"."u" from "app"']
    # it ends no session: DSQL lets none end another's
    assert hold.end("app", "app") == 0
    hold.release("app", record, said.append)
    assert server.grants == before


def test_dsql_is_kept_as_a_target_only(tmp_path):
    from migkit import decide, freeze
    assert freeze.KEEPS["dsql"] is freeze.PgWireHold
    assert "dsql" not in freeze.HOLDS
    assert decide.cell("dsql", "read-only target") == (
        "yes", "freeze.PgWireHold")


# --- the keys, columns and types the catalogue answers -------------------------

def _catalogue(monkeypatch, tables, columns, keys, rules):
    """Answer `neutral_*` from these dicts instead of a server."""
    def tables_of(self, side, db):
        return [t for t in tables if self.in_scope(db, t)]

    def cols(self, side, db, t):
        return list(columns[(side, t)])

    def key(self, side, db, t):
        return list(keys.get((side, t), []))

    def rule(self, side, db, t):
        return dict(rules.get((side, t), {}))
    monkeypatch.setattr(pgwire.PgWireEngine, "neutral_tables", tables_of)
    monkeypatch.setattr(pgwire.PgWireEngine, "neutral_columns", cols)
    monkeypatch.setattr(pgwire.PgWireEngine, "neutral_key", key)
    monkeypatch.setattr(pgwire.PgWireEngine, "neutral_column_rules", rule)


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_table_with_no_key_is_said_by_name(tmp_path, monkeypatch, name,
                                             cls):
    _catalogue(monkeypatch, ["public.a", "public.b"], {}, {
        ("src", "public.a"): ["id"]}, {})
    eng = _engine(tmp_path, name, cls)
    got = eng._deep_keys("d", [])
    assert got.status == "diff" and "public.b" in got.detail
    assert "public.a" not in got.detail and got.category == \
        "structure.keys"


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_column_that_differs_or_is_narrower_is_found(tmp_path,
                                                       monkeypatch, name,
                                                       cls):
    cols = {("src", "public.t"): [("id", "bigint"), ("n", "text"),
                                  ("amt", "numeric(12,2)"),
                                  ("code", "character varying(40)")],
            ("dst", "public.t"): [("id", "bigint"), ("n", "text"),
                                  ("amt", "numeric(12,2)"),
                                  ("code", "character varying(10)")]}
    rules = {("src", "public.t"): {"n": {"null": True, "default": "'x'"},
                                   "id": {"null": False}},
             ("dst", "public.t"): {"n": {"null": False, "default": None},
                                   "id": {"null": False}}}
    _catalogue(monkeypatch, ["public.t"], cols, {}, rules)
    eng = _engine(tmp_path, name, cls)
    columns, narrowing = eng._deep_columns("d", [("public.t", "public.t")])
    assert columns.status == "diff" and columns.category == \
        "structure.columns"
    assert "public.t.n: null allowed -> refused, default 'x' -> None" in \
        columns.detail
    assert "public.t.code: type character varying(40) -> character " \
        "varying(10)" in columns.detail
    assert narrowing.status == "diff" and narrowing.category == \
        "value.narrowing"
    assert "longer values are cut" in narrowing.detail


@pytest.mark.parametrize("name,cls", ENGINES)
def test_columns_that_match_are_said_to(tmp_path, monkeypatch, name, cls):
    cols = {("src", "public.t"): [("id", "bigint")],
            ("dst", "public.t"): [("id", "bigint")]}
    _catalogue(monkeypatch, ["public.t"], cols, {}, {})
    eng = _engine(tmp_path, name, cls)
    columns, narrowing = eng._deep_columns("d", [("public.t", "public.t")])
    assert (columns.status, narrowing.status) == ("ok", "ok")
