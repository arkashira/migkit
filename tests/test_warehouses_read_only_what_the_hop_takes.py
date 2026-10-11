"""Redshift and Snowflake read only the tables the hop takes, say which
server answers, and hold a source's writes: what runs without a warehouse.

No warehouse runs here - each needs an account this machine has none of -
so a warehouse is a real engine whose catalogue reads and statements are
answered in-process, and what is pinned is what migkit asks of the scope
and says to the servers:

* the listing every check, the copy and a repair read their tables from
  asks the hop's scope (`Engine.in_scope`), for the engine that folds its
  names to capitals as for the one that does not;
* the copy lists, and the repair plans, only the tables the hop takes,
  through the pair every engine is compared by;
* the versions and the brand each side reports are read and held to each
  other;
* a source held at the cutover (`freeze`) is revoked, ended, read back and
  given back exactly - on an in-memory server that answers the statements
  the hold asks and takes the ones it makes.

Not proved here, and owed a real account: that either server accepts any
statement below as written.
"""
import re

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from migkit.engines.warehouse import RedshiftEngine, SnowflakeEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", ("redshift", "snowflake")),
    ("scope.copy", ("redshift", "snowflake")),
    ("scope.repair", ("redshift", "snowflake")),
    ("scope.ops", ("redshift", "snowflake")),
    ("assess.versions", ("redshift", "snowflake")),
    ("assess.brands", ("redshift", "snowflake")),
    ("assess.engine-checks", "snowflake"),
    ("cutover.freeze", ("redshift", "snowflake")),
]

ENGINES = {"redshift": RedshiftEngine, "snowflake": SnowflakeEngine}
#: how each names the same three tables of the hop's one schema
LISTED = {"redshift": ["orders", "event_log", "_migkit_ledger"],
          "snowflake": ["ORDERS", "EVENT_LOG", "_MIGKIT_LEDGER"]}


def _hop(name, tmp_path, **kw):
    ep = Endpoint(user="u", password="CHANGE_ME", options={
        "database": "analytics", "account": "acme"})
    hop = Hop(name="w", engine=name, source=ep, target=ep,
              databases=["sales"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _engine(name, tmp_path, **kw):
    return ENGINES[name](_hop(name, tmp_path, **kw))


def _answers(monkeypatch, name, source=None, target=None):
    """The tables each side lists, by the catalogue read that lists them."""
    def rows(self, side, db, sql, args=None):
        assert "information_schema.tables" in sql, sql
        return [[t] for t in (source if side == "src" and source
                              else target if target else LISTED[name])]
    monkeypatch.setattr(ENGINES[name], "_rows", rows)


@pytest.mark.parametrize("name", ["redshift", "snowflake"])
def test_a_listing_is_asked_of_the_scope_and_leaves_migkits_own_out(
        name, tmp_path, monkeypatch):
    _answers(monkeypatch, name)
    eng = _engine(name, tmp_path, exclude=["event_log"])
    # the scope is asked in the name the engine says, however it stores it
    assert eng.neutral_tables("src", "sales") == ["orders"]
    assert eng.neutral_tables("dst", "sales") == ["orders"]
    # a hop that takes everything reads every table of the application's
    assert _engine(name, tmp_path).neutral_tables("src", "sales") == [
        "orders", "event_log"]
    taking = _engine(name, tmp_path, include=["sales.orders"])
    assert taking.neutral_tables("src", "sales") == ["orders"]


@pytest.mark.parametrize("name", ["redshift", "snowflake"])
def test_the_copy_lists_only_the_tables_the_hop_takes(name, tmp_path,
                                                      monkeypatch):
    """Through the pair a same-engine hop is seen as: a table the target
    lacks is listed too (the copier makes it), one the hop leaves out is
    not, on either side."""
    there = {"redshift": ["orders", "event_log"],
             "snowflake": ["ORDERS", "EVENT_LOG"]}[name]
    here = there + {"redshift": ["parcels", "audit_log"],
                    "snowflake": ["PARCELS", "AUDIT_LOG"]}[name]
    _answers(monkeypatch, name, source=here, target=there)
    eng = _engine(name, tmp_path, exclude=["event_log", "audit_log"])
    assert sorted(eng.list_move_tables("sales")) == [("", "orders"),
                                                     ("", "parcels")]


@pytest.mark.parametrize("name", ["redshift", "snowflake"])
def test_a_repair_plans_only_the_tables_the_hop_takes(name, tmp_path,
                                                      monkeypatch):
    """A drilldown a check left under another scope - `audit_log` was
    excluded since - is said and never acted on."""
    from migkit.engines.base import LEFT_OUT
    (tmp_path / "data-orders.missing").write_text('["1"]\n')
    (tmp_path / "data-audit_log.extra").write_text('["7"]\n')
    _answers(monkeypatch, name, source={"redshift": ["orders", "audit_log"],
                                        "snowflake": ["ORDERS",
                                                      "AUDIT_LOG"]}[name])
    eng = _engine(name, tmp_path, exclude=["audit_log"])
    plan = eng.repair_plan("sales", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert "audit_log" in said[0].note and not said[0].statements
    # the plan is the pair's: the rows the engine's `apply` carries
    assert [a.table for a in eng._as_pair().repair_plan("sales", "rows")
            if a.kind == "rows"] == ["orders"]


# ---- the changes a tail applies are the hop's --------------------------------

@pytest.mark.parametrize("name", ["redshift", "snowflake"])
def test_a_delete_the_hop_does_not_carry_never_reaches_the_warehouse(
        name, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    from tests.test_the_tail_carries_only_what_the_hop_carries import (
        _Source, _del, _ins, _upd)
    dst = _engine(name, tmp_path)
    dst.neutral_columns = lambda side, db, table: [("id", "integer"),
                                                   ("v", "text")]
    hop = Hop(name="c", engine="hetero",
              source=Endpoint(host="x", port=1, user="x", password="x"),
              target=Endpoint(host="x", port=1, user="x", password="x"),
              databases=["d"], mapping={"ops": {"public.orders":
                                                "insert, update"}},
              options={"source_engine": "postgres", "target_engine": name})
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop, pair.src_name, pair.dst_name = hop, "postgres", name
    pair.src_engine, pair.dst_engine = _Source(), dst
    pair._row_scope = lambda db, t: ((None, None))
    out, _ = pair._to_target("d", [_ins(1), _del(2), _upd(3)], {})
    landed = []
    dst._apply_batch = lambda side, db, by_table, seen: landed.append(
        by_table)
    dst.local_table = lambda table: str(table)
    assert dst.neutral_apply("dst", "d", out) == 2
    assert [[k for k, _, _ in rows] for rows in landed[0].values()] == [
        ["upsert", "upsert"]]


# ---- the servers, said ---------------------------------------------------------

REDSHIFT_BANNER = ("PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC"
                   " gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3),"
                   " Redshift 1.0.77467")


def _says(monkeypatch, name, source, target):
    def rows(self, side, db, sql, args=None):
        got = source if side == "src" else target
        if isinstance(got, Exception):
            raise got
        return [[got]]
    monkeypatch.setattr(ENGINES[name], "_rows", rows)


def test_redshift_reads_its_own_release_from_the_banner_of_postgresql_8(
        tmp_path, monkeypatch):
    _says(monkeypatch, "redshift", REDSHIFT_BANNER,
          REDSHIFT_BANNER.replace("77467", "80001"))
    eng = _engine("redshift", tmp_path)
    assert eng._server_versions() == ("1.0.77467", "1.0.80001")
    src, dst = eng._brands()
    assert (src.name, dst.name) == ("redshift", "redshift")
    assert src.version == "1.0.77467"
    # named, and never measured: not a clean bill
    assert not src.measured
    row = eng._version_row(*eng._server_versions(), parts=3)
    assert row["level"] == "warn" and "1.0.77467" in row["detail"]


def test_a_banner_that_names_no_redshift_is_not_called_one(tmp_path,
                                                           monkeypatch):
    _says(monkeypatch, "redshift", "PostgreSQL 16.2 on x86_64-pc-linux-gnu",
          REDSHIFT_BANNER)
    eng = _engine("redshift", tmp_path)
    assert eng._server_versions() == (None, "1.0.77467")
    assert eng._brands()[0].name == "unknown"
    assert "unknown, not clean" in eng._brand_rows()[0]["detail"]
    row = eng._version_row(*eng._server_versions())
    assert row["level"] == "warn" and "would not say" in row["detail"]


def test_snowflake_reads_its_version_and_a_side_that_would_not_say_is_unknown(
        tmp_path, monkeypatch):
    _says(monkeypatch, "snowflake", "9.1.0", RuntimeError("no session"))
    eng = _engine("snowflake", tmp_path)
    assert eng._server_versions() == ("9.1.0", None)
    src, dst = eng._brands()
    assert (src.name, src.version, dst.name) == ("snowflake", "9.1.0",
                                                 "unknown")
    row = eng._version_row(*eng._server_versions())
    assert row["level"] == "warn" and "src 9.1.0 / dst ?" in row["detail"]


@pytest.mark.parametrize("name", ["redshift", "snowflake"])
def test_assess_says_what_the_move_will_not_do_and_the_tables_it_keeps(
        name, tmp_path, monkeypatch):
    """A target's own tables are named (`_migkit_`), as a pair asks each
    side for; a source's work left to hands is counted from its catalogue
    and an object it could not list is unknown, never zero."""
    from migkit import handwork
    _answers(monkeypatch, name)
    eng = _engine(name, tmp_path)
    eng._handwork = lambda side="src": handwork.Inventory()
    said = eng._assess_extra()
    assert said[0]["item"] == "sales: migkit's own tables"
    assert "_migkit_ledger" in said[0]["detail"]
    assert eng.reach_items("dst") == said
    assert eng.reach_items("src") == []


# ---- a source held at the cutover ----------------------------------------------

class _Server:
    """What the hold asks of a warehouse and what it changes: an in-memory
    set of write grants, sessions and open transactions."""

    def __init__(self, grants, sessions=0, locks=()):
        self.grants = set(grants)
        self.sessions = sessions
        self.locks = list(locks)
        self.log = []


def _hold(monkeypatch, cls, server, answer, take, give):
    """`cls.rows` answered by `answer(server, sql)` and `cls.run` taking the
    statements `take` and `give` match."""
    def rows(self, db, sql, args=()):
        got = answer(server, " ".join(sql.split()), tuple(args))
        assert got is not None, f"unanswered: {sql}"
        return got

    def run(self, db, *stmts):
        for s in stmts:
            record = freeze.state(self.hop, db, self.side) or {}
            server.log.append(s)
            revoke, grant = re.match(take, s), re.match(give, s)
            assert revoke or grant, f"a statement not known: {s}"
            if revoke:
                taken = server.parse(revoke)
                # recorded before it is done
                assert any(server.recorded(g, taken)
                           for e in record["roles"].values()
                           for g in e["revoked"]), (s, record)
                server.grants.discard(taken)
            else:
                server.grants.add(server.parse(grant))
    monkeypatch.setattr(cls, "rows", rows)
    monkeypatch.setattr(cls, "run", run)


def _redshift_server():
    s = _Server({("t", "INSERT", "app"), ("t", "UPDATE", "app"),
                 ("u", "DELETE", "app")}, sessions=1)
    s.parse = lambda m: (m.group(2), m.group(1).upper(), m.group(3))
    s.recorded = lambda g, grant: (g[1], g[2]) == (grant[0], grant[1])
    return s


def _redshift_answer(s, sql, args):
    if sql == "select current_user":
        return [["migkit"]]
    if "from svv_user_grants" in sql:
        return []
    if "from svv_relation_privileges" in sql:
        return [[t, p, who, "user", "f"] for t, p, who in sorted(s.grants)]
    if "usesuper" in sql:
        return [["rdsdb"]]
    if "pg_terminate_backend" in sql:
        n, s.sessions = s.sessions, 0
        return [[n]]
    if "from pg_tables" in sql:
        return []
    if "getdate()::varchar" in sql:
        return [["2026-10-11 10:00:00"]]
    if "from stv_sessions" in sql:
        return [[s.sessions]]
    if "from svv_transactions" in sql:
        return []
    return None


def test_a_redshift_source_is_revoked_its_sessions_ended_and_given_back(
        tmp_path, monkeypatch):
    server = _redshift_server()
    _hold(monkeypatch, freeze.RedshiftHold, server, _redshift_answer,
          r'revoke (\w+) on "sales"\."(\w+)" from "(\w+)"$',
          r'grant (\w+) on "sales"\."(\w+)" to "(\w+)"$')
    before = set(server.grants)
    hop = _hop("redshift", tmp_path)
    eng = RedshiftEngine(hop)
    said = []
    freeze.freeze_source(hop, eng, ["sales"], said.append, "rs_grants")
    assert server.grants == set()
    assert server.sessions == 0
    assert any("3 write grants revoked" in line and "1 open sessions ended"
               in line for line in said), said
    # the freeze read back from the server: no right left, no session of
    # the application's from before it, nothing held open
    held = freeze.proof(hop, eng, "sales", "rs_grants")
    assert held and all(ok for ok, _ in held), held
    # a session that came in since, and a right granted since, are not held
    server.sessions = 2
    assert not all(ok for ok, _ in freeze.proof(hop, eng, "sales",
                                                "rs_grants"))
    server.sessions = 0
    server.grants.add(("t", "INSERT", "app"))
    assert not all(ok for ok, _ in freeze.proof(hop, eng, "sales",
                                                "rs_grants"))
    server.grants.clear()
    freeze.thaw_source(hop, eng, ["sales"], said.append)
    assert server.grants == before
    assert freeze.state(hop, "sales", "src") is None or \
        not freeze.state(hop, "sales", "src").get("roles")


def _snowflake_server():
    s = _Server({("ORDERS", "INSERT", "APP_RW"), ("ORDERS", "DELETE",
                                                  "APP_RW"),
                 ("*", "INSERT", "APP_RW")},
                locks=[("ANALYTICS.SALES.ORDERS", "x", 77, "t0")])
    s.parse = lambda m: (m.group(2) or "*", m.group(1).upper(), m.group(3))
    s.recorded = lambda g, grant: g[2] == grant[1] and (
        g[0] == "future" if grant[0] == "*" else g[1] == grant[0])
    return s


def _snowflake_answer(s, sql, args):
    if sql.startswith("select current_available_roles()"):
        return [['["MIGKIT_ROLE"]']]
    if "information_schema.table_privileges" in sql:
        return [[who, t, p, "NO"] for t, p, who in sorted(s.grants)
                if t != "*"] + [["OWNER_ROLE", "ORDERS", "OWNERSHIP", "YES"]]
    if sql.startswith("show future grants"):
        return [["t0", p, "TABLE", "x", "ROLE", who, "false"]
                for t, p, who in sorted(s.grants) if t == "*"]
    if sql == "show locks in account":
        return [list(lock) for lock in s.locks]
    if "system$abort_transaction" in sql:
        n = int(re.search(r"\((\d+)\)", sql).group(1))
        s.locks = [lock for lock in s.locks if lock[2] != n]
        return [["aborted"]]
    return None


def test_a_snowflake_source_is_revoked_its_transactions_aborted_and_given_back(
        tmp_path, monkeypatch):
    server = _snowflake_server()
    _hold(monkeypatch, freeze.SnowflakeHold, server, _snowflake_answer,
          r'revoke (\w+) on (?:table "analytics"\."SALES"\."(\w+)"|'
          r'future tables in schema "analytics"\."SALES") from role '
          r'"(\w+)"$',
          r'grant (\w+) on (?:table "analytics"\."SALES"\."(\w+)"|'
          r'future tables in schema "analytics"\."SALES") to role '
          r'"(\w+)"$')
    before = set(server.grants)
    hop = _hop("snowflake", tmp_path)
    eng = SnowflakeEngine(hop)
    said = []
    freeze.freeze_source(hop, eng, ["sales"], said.append, "sf_grants")
    # the table's grants and the schema's future grants are taken; the
    # role that owns a table is named, not touched
    assert server.grants == set()
    assert server.locks == []
    assert any("1 transactions holding the schema's tables aborted" in line
               for line in said), said
    held = freeze.proof(hop, eng, "sales", "sf_grants")
    assert held and all(ok for ok, _ in held), held
    server.locks.append(("ANALYTICS.SALES.ORDERS", "x", 78, "t1"))
    got = freeze.proof(hop, eng, "sales", "sf_grants")
    assert [ok for ok, what in got if "write transaction" in what] == [False]
    server.locks.clear()
    freeze.thaw_source(hop, eng, ["sales"], said.append)
    assert server.grants == before
    assert any("owns" in line for line in
               freeze.hold_for(hop, eng, "src").exempt("sales", "sf_grants"))
