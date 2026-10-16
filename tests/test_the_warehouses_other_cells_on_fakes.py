"""The warehouses' remaining cells (ER1.acct), each way chosen by a rung on
what the account answers and its statements run on fakes answering each
catalogue: Snowflake followed through its tables' change tracking (or said
to follow only by the landing's position where the account reads no
changes, or refused before the copy where its history is shorter than the
copy), users and roles made on Redshift and Snowflake, BigQuery's IAM
bindings granted with no password, a writer's number refused by the target
when lower, settings and counters compared, and Snowflake's rollback point
kept in its time travel.

No warehouse runs here: that an account takes each statement as written is
owed (`warehouse.UNPROVED_ON`, every cell in its `UNPROVED_ITEMS`).
"""
import datetime
import types

import pytest

from migkit import capabilities, fencing
from migkit.config import Endpoint, Hop
from migkit.engines.warehouse import (BigQueryEngine, RedshiftEngine,
                                      SnowflakeEngine)

UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
ENGINES = {"redshift": RedshiftEngine, "snowflake": SnowflakeEngine,
           "bigquery": BigQueryEngine}


class World:
    """Each side a list of (needle, answer): the first needle a statement
    holds answers it - rows, a function of the arguments, or an exception
    to raise."""

    def __init__(self, src, dst=()):
        self.sides = {"src": list(src), "dst": list(dst)}
        self.asked = []

    def ask(self, side, sql, args):
        low = " ".join(sql.split()).lower()
        self.asked.append((side, low, args))
        for needle, answer in self.sides[side]:
            if needle in low:
                if isinstance(answer, Exception):
                    raise answer
                return answer(args) if callable(answer) else answer
        raise AssertionError(f"unanswered on the {side}: {low}")


class Run:
    """A connection whose statements are recorded; a fence table kept in
    memory, as a warehouse answers its update's row count."""

    def __init__(self):
        self.ran, self.fence = [], {}
        self.rowcount, self._got = 0, None

    def cursor(self):
        return self

    def execute(self, sql, args=()):
        low = " ".join(sql.split()).lower()
        self.ran.append(low)
        self._got = None
        if "_migkit_fence" not in low:
            return
        if low.startswith("update") and "number <" in low:
            n, seen, row, _ = args
            have = self.fence.get(row)
            self.rowcount = int(have is not None and have[0] < n)
            if self.rowcount:
                self.fence[row] = (n, seen)
        elif low.startswith("update"):
            row, n = args
            self.rowcount = int(self.fence.get(row, (None,))[0] == n)
        elif low.startswith("insert"):
            row, n, seen = args
            self.fence[row] = (n, seen)
        elif low.startswith("select count"):
            self._got = (int(args[0] in self.fence),)
        elif low.startswith("select number"):
            self._got = self.fence.get(args[0])

    def fetchone(self):
        return self._got

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _engine(name, world, tmp_path, run=None):
    ep = Endpoint(user="u", password="CHANGE_ME", options={
        "database": "ANALYTICS", "account": "acme"})
    hop = Hop(name="w", engine=name, source=ep, target=ep,
              databases=["sales"])
    hop.report_dir = lambda db=None: tmp_path
    eng = ENGINES[name](hop)
    eng._rows = lambda side, db, sql, args=None: [
        list(r) for r in world.ask(side, sql, args)]
    eng._listing = lambda side, db, sql, args=None: [
        dict(r) for r in world.ask(side, sql, args)]
    run = run or Run()
    eng._connect = lambda side, db: run
    return eng, run


# ---- every cell of the three is done or not applicable, with its reason ----


def test_no_cell_of_the_three_is_left_not_yet():
    m = capabilities.matrix()
    for name in ENGINES:
        assert set(m[name].values()) <= {"yes", capabilities.NOT_APPLICABLE}
        for cap, state in m[name].items():
            if state != "yes":
                assert capabilities.GAPS[name][cap][1]
    for name in ENGINES:
        assert capabilities.depth_why(name, "verify.users").startswith(
            "built; not run on a real Redshift cluster, Snowflake account")
    assert "Snowflake account" in SnowflakeEngine.UNPROVED_ON
    assert "Redshift cluster" in RedshiftEngine.UNPROVED_ON
    assert "BigQuery project" in BigQueryEngine.UNPROVED_ON


# ---- Snowflake followed, by what the account answers -----------------------

def _tables(*rows):
    return ("show tables in schema", [
        {"name": n, "change_tracking": ct, "retention_time": days}
        for n, ct, days in rows])


def _snowflake_source(tmp_path, *, changes=True, days=1, tracking="ON"):
    world = World([
        ("information_schema.tables where", [["ORDERS"]]),
        ("changes(information => default) at(offset => -1)",
         [] if changes else Exception(
             "002003: change tracking is not supported by this edition")),
        _tables(("ORDERS", tracking, days)),
        ("select current_timestamp()", [[NOW]]),
    ])
    eng, _ = _engine("snowflake", world, tmp_path)
    eng.neutral_key = lambda side, db, t: ["id"]
    return eng, world


def test_an_account_that_reads_no_changes_is_followed_by_the_landing(
        tmp_path):
    eng, _ = _snowflake_source(tmp_path, changes=False)
    way = eng.follow_way("src", "sales")
    assert way.path == "landing" and "this edition" in way.reason
    blocked = eng.tail_blockers("src", "sales")
    assert blocked and "landing's position" in blocked[0]
    with pytest.raises(SystemExit, match="cannot be followed"):
        eng.change_point("src", "sales")


def test_a_history_shorter_than_the_copy_is_refused_before_it(tmp_path):
    eng, _ = _snowflake_source(tmp_path, days=1)
    eng.copy_seconds = 2 * 86400
    with pytest.raises(SystemExit) as got:
        eng.change_point("src", "sales")
    said = str(got.value)
    assert "24.0 hours of change history" in said and "48.0 hours" in said
    eng.copy_seconds = 3600
    assert eng.follow_way("src", "sales").path == "changes"
    assert eng.change_point("src", "sales") == {
        "at": NOW.isoformat(timespec="microseconds")}


def test_a_table_tracking_no_changes_is_named_with_its_statement(tmp_path):
    eng, _ = _snowflake_source(tmp_path, tracking="OFF")
    way = eng.follow_way("src", "sales")
    assert way.path == "none"
    assert "set change_tracking = true" in way.reason and "orders" in \
        way.reason


def test_a_window_is_read_as_its_end_held_and_read_again_the_same(tmp_path):
    eng, world = _snowflake_source(tmp_path)
    start = NOW - datetime.timedelta(hours=2)
    world.sides["src"].insert(0, (
        "changes(information => default) at(timestamp =>", [
            {"_migkit_k0": 1, "_migkit_here": True, "ID": 1, "AMOUNT": 5},
            {"_migkit_k0": 2, "_migkit_here": None, "ID": None,
             "AMOUNT": None}]))
    eng._followed = lambda side, db: [("orders", ["id"], [
        ("id", "int"), ("amount", "int")])]
    eng._fix_read = lambda side, db, t, cols, rows: rows
    token = {"at": start.isoformat()}
    got, pos = eng.neutral_changes("src", "sales", token)
    assert [(c["op"], c["values"]) for c in got] == [
        ("update", {"id": 1, "amount": 5}), ("delete", {})]
    asked = [q for _, q, _ in world.asked if "end(timestamp" in q][0]
    assert f"at(timestamp => '{(NOW - datetime.timedelta(seconds=60)).isoformat().lower()}'" \
        in asked
    eng.release_changes()
    again, pos2 = eng.neutral_changes("src", "sales", token)
    assert again == got and pos2 == pos


# ---- users and roles made; BigQuery's are IAM bindings ---------------------

@pytest.mark.parametrize("name, made", [
    ("redshift", 'create role "analyst"'),
    ("snowflake", 'create role "ANALYST"')])
def test_a_role_and_its_grant_are_made_on_the_target(name, made, tmp_path):
    rows = {"redshift": [("from pg_user", [["rdsdb"], ["ann"]]),
                         ("from svv_roles", [["analyst"]]),
                         ("from svv_user_grants", [["ann", "analyst"]]),
                         ("svv_relation_privileges",
                          [["analyst", "orders", "SELECT"]])],
            "snowflake": [("account_usage.users", [["ANN"]]),
                          ("grants_to_users", [["ANN", "ANALYST"]]),
                          ("account_usage.roles", [["ANALYST"],
                                                   ["SYSADMIN"]]),
                          ("table_privileges",
                           [["ANALYST", "ORDERS", "SELECT"]])]}[name]
    empty = [(needle, []) for needle, _ in rows]
    eng, run = _engine(name, World(rows, empty), tmp_path)
    src, dst = eng.accounts("src"), eng.accounts("dst")
    role = "role:analyst"
    assert src[role]["grants"] == ["SELECT ON sales.orders"]
    assert src["user:ann"]["roles"] == ["analyst"] and dst == {}
    steps, skipped = eng.account_steps(src, dst, {"ann": "s3cret"})
    assert steps[0]["run"] == made and steps[0]["made"]
    user = next(s for s in steps if s["key"] == "user:ann" and s["made"])
    assert "s3cret" in user["run"] and "s3cret" not in user["shown"]
    assert any(s["run"].startswith("grant select on table") and
               "orders" in s["run"].lower() for s in steps)
    for s in steps:
        eng.account_run(s["run"])
    assert run.ran[0] == made.lower()
    if name == "snowflake":
        # a table's privilege is a role's: the user's own is skipped, said
        src["user:ann"]["grants"] = ["INSERT ON sales.orders"]
        _, skipped = eng.account_steps(src, dst, {})
        assert skipped and "to roles only" in skipped[0][1]


def test_bigquery_grants_iam_bindings_and_takes_no_password(tmp_path):
    rows = [("object_privileges", [
        ["ds_sales", "roles/bigquery.dataViewer", "user:ann@example.com"],
        ["orders", "roles/bigquery.dataEditor", "group:bi@example.com"]])]
    world = World(rows, [("object_privileges", [])])
    eng, _ = _engine("bigquery", world, tmp_path)
    queries = []
    client = types.SimpleNamespace(
        project="proj",
        get_dataset=lambda name: types.SimpleNamespace(location="EU"),
        query=lambda sql: queries.append(sql) or types.SimpleNamespace(
            result=lambda: []))
    eng._client = lambda side: client
    eng.side_database = lambda side, db: "ds_sales"
    eng._d = lambda side, db: "proj.ds_sales"
    src, dst = eng.accounts("src"), eng.accounts("dst")
    assert src["user:user:ann@example.com"]["grants"] == [
        "roles/bigquery.dataViewer ON sales.*"]
    assert src["role:group:bi@example.com"]["grants"] == [
        "roles/bigquery.dataEditor ON sales.orders"]
    steps, skipped = eng.account_steps(src, dst,
                                       {"user:ann@example.com": "x"})
    assert skipped and "IAM" in skipped[0][1]
    assert not any(s["made"] for s in steps)
    viewer = next(s for s in steps if "dataViewer" in s["run"])
    assert viewer["run"] == ("grant `roles/bigquery.dataViewer` on schema"
                             " `proj.ds_sales` to 'user:ann@example.com'")
    assert viewer["undo"].startswith("revoke ")
    eng.account_run(viewer["run"])
    assert queries == [viewer["run"]]
    assert all("`proj`.`region-eu`.information_schema.object_privileges"
               " where object_schema = 'ds_sales'" in q
               for _, q, _ in world.asked)


# ---- a writer's number, held and refused by the target ---------------------

@pytest.mark.parametrize("name, way, isolation", [
    ("redshift", "serializable", [["serializable"]]),
    ("snowflake", "transaction", None)])
def test_a_lower_number_is_refused_by_the_target(name, way, isolation,
                                                 tmp_path):
    world = World([], [("stv_db_isolation_level", isolation or [])])
    eng, run = _engine(name, world, tmp_path)
    assert eng.fence_way("dst", "sales").path == way
    assert eng.fence_raise("dst", "sales", "hop:w", 3, "m1") == 3
    assert eng.fence_raise("dst", "sales", "hop:w", 2, "m0") == 3
    _, table = eng.own_table("dst", "sales")
    eng.fence_hold(run, table, "hop:w", 3)
    with pytest.raises(fencing.Fenced, match="holds 3 .*not this writer's 2"):
        eng.fence_hold(run, table, "hop:w", 2)
    assert eng.fence_raise("dst", "sales", "hop:w", 5, "m2") == 5
    with pytest.raises(fencing.Fenced, match="a newer holder raised it"):
        eng.fence_hold(run, table, "hop:w", 3)


def test_redshift_in_snapshot_isolation_is_held_by_its_write_lock(tmp_path):
    world = World([], [("stv_db_isolation_level", [["snapshot isolation"]])])
    eng, _ = _engine("redshift", world, tmp_path)
    assert eng.fence_way("dst", "sales").path == "transaction"


# ---- settings, counters and the rollback point -----------------------------

def test_snowflake_settings_differing_fail_and_its_size_is_said(tmp_path):
    def side(tz):
        return [("show parameters", [{"key": "TIMEZONE", "value": tz},
                                     {"key": "STATEMENT_TIMEOUT_IN_SECONDS",
                                      "value": "60"}]),
                ("current_warehouse()", [["WH"]]),
                ("show warehouses like", [{"size": "X-Small",
                                           "min_cluster_count": 1,
                                           "max_cluster_count": 2}])]
    eng, _ = _engine("snowflake", World(side("UTC"), side("Asia/Bangkok")),
                     tmp_path)
    got = eng.check_params("sales")
    assert got[0].status == "diff" and "TIMEZONE" in got[0].detail
    assert "size X-Small, clusters 1-2" in got[1].detail
    assert "does not change it" in got[1].detail


def test_redshift_slots_are_read_and_said(tmp_path):
    side = [("from pg_settings", [["timezone", "UTC"]]),
            ("stv_wlm_service_class_config", [[6, 5], [7, 10]])]
    eng, _ = _engine("redshift", World(side, side), tmp_path)
    got = eng.check_params("sales")
    assert got[0].status == "ok"
    assert "queue 6: 5 slots, queue 7: 10 slots" in got[1].detail


def test_a_snowflake_sequence_behind_the_source_is_said_with_its_fix(
        tmp_path):
    def seqs(n):
        return [("show sequences in schema", [
            {"name": "ORDER_SEQ", "next_value": n, "interval": 1}])]
    eng, _ = _engine("snowflake", World(seqs(500), seqs(10)), tmp_path)
    got = eng.check_autoinc("sales")[0]
    assert got.status == "diff" and "next 10 < source 500" in got.detail
    assert "start = 500 increment = 1" in got.fix_hint
    eng, _ = _engine("snowflake", World(seqs(500), seqs(500)), tmp_path)
    assert eng.check_autoinc("sales")[0].status == "ok"


def test_a_redshift_identity_behind_the_source_is_said(tmp_path):
    def side(top):
        return [("pg_attrdef", [["orders", "id"]]),
                ("select max(", [[top]])]
    eng, _ = _engine("redshift", World(side(90), side(40)), tmp_path)
    got = eng.check_autoinc("sales")[0]
    assert got.status == "diff" and "orders.id 40 < source 90" in got.detail


@pytest.mark.parametrize("days, path", [(1, "travel"), (0, "counts")])
def test_snowflakes_rollback_point_is_its_time_travel(days, path, tmp_path):
    world = World([], [_tables(("ORDERS", "OFF", days)),
                       ("select current_timestamp()", [[NOW]]),
                       ("select count(*)", [[7]])])
    eng, _ = _engine("snowflake", world, tmp_path)
    got = eng.snapshot_state("sales", tmp_path)
    assert got.path == path
    sql = (tmp_path / "dst-snapshots.sql").read_text()
    if path == "travel":
        assert f"at(timestamp => '{NOW.isoformat()}'::timestamp_tz)" in sql
        assert "swap with" in sql
    else:
        assert "7 rows, no time travel" in sql
    assert eng.snapshot_state("sales", tmp_path, "sequences") is None
