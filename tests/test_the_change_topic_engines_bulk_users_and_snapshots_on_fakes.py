"""The change-topic engines' bulk ways, accounts, snapshots and TiCDC's
other protocols (backlog ER4, ER5, P0-PARITY.lv.new), on fakes only: no
server of any of them is reached. Every way here is declared unproved on
its real server in its engine's module (`UNPROVED_ON`), so this module
proves no depth cell (no `PROVES`): it holds the logic to what each
engine's own answers make of it."""
import json
import struct

from migkit import capabilities, changetopic, decide
from migkit.config import Endpoint, Hop

FIVE = ("cockroachdb", "dsql", "tidb", "couchbase", "cosmosdb")


def _hop(engine, **target):
    ep = Endpoint(host="10.0.0.1", port=1, user="u", password="CHANGE_ME")
    return Hop(name="h", engine=engine, source=ep,
               target=Endpoint(host="10.0.0.2", port=1, user="u",
                               password="CHANGE_ME", options=target),
               databases=["s"], db_map={"s": "d"})


def test_every_cell_of_the_five_is_built_or_says_why_not():
    m = capabilities.matrix()
    for e in FIVE:
        for cap, state in m[e].items():
            assert state in ("yes", capabilities.NOT_APPLICABLE), (e, cap)
            if state != "yes":
                assert capabilities.GAPS[e][cap][1], (e, cap)
        for shape, state in decide.coverage()[e].items():
            assert state in ("yes", decide.NOT_APPLICABLE), (e, shape)
    assert "IAM" in capabilities.unavailable("dsql", "users")
    for e in FIVE:
        assert capabilities.UNPROVED_WHY[e].startswith("built; not run on")
        for item in ("cutover.rollback",):
            assert capabilities.depth_cell(e, item)[0] == decide.NOT_YET


# ---- TiCDC: three protocols, one set of change rows -------------------------

def _frame(b):
    return struct.pack(">q", len(b)) + b


def _zz(n):
    n = (n << 1) ^ (n >> 63)
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | 0x80]) if n else bytes([b])
        if not n:
            return out


def _avro(sid, *parts):
    return b"\x00" + sid.to_bytes(4, "big") + b"".join(parts)


def _avro_str(s):
    return _zz(len(s.encode())) + s.encode()


KEY_SCHEMA = {"type": "record", "name": "k",
              "fields": [{"name": "id", "type": "long"}]}
VALUE_SCHEMA = {"type": "record", "name": "v", "fields": [
    {"name": "id", "type": "long"},
    {"name": "qty", "type": ["null", "long"]},
    {"name": "price", "type": {"type": "bytes", "logicalType": "decimal",
                               "precision": 10, "scale": 2}},
    {"name": "_tidb_op", "type": "string"},
    {"name": "_tidb_commit_ts", "type": "long"}]}


def _canal():
    def ev(kind, ts, rows, old=None):
        m = {"type": kind, "database": "shop", "table": "orders",
             "pkNames": ["id"], "data": rows, "_tidb": {"commitTs": ts}}
        if old:
            m["old"] = old
        return (None, json.dumps(m).encode())
    return [ev("INSERT", 100, [{"id": "1", "qty": "2"}]),
            ev("UPDATE", 110, [{"id": "2", "qty": "3"}], [{"id": "1"}]),
            ev("DELETE", 120, [{"id": "2", "qty": "3"}]),
            (None, json.dumps({"type": "TIDB_WATERMARK",
                               "_tidb": {"watermarkTs": 130}}).encode())]


def _open():
    def ev(ts, value):
        key = json.dumps({"ts": ts, "scm": "shop", "tbl": "orders",
                          "t": 1}).encode()
        return (struct.pack(">q", 1) + _frame(key),
                _frame(json.dumps(value).encode()))
    col = lambda v, h=False: {"t": 8, "v": v, **({"h": True} if h else {})}  # noqa: E731
    resolved = (struct.pack(">q", 1) + _frame(json.dumps(
        {"ts": 130, "t": 3}).encode()), b"")
    return [ev(100, {"u": {"id": col(1, True), "qty": col(2)}}),
            ev(110, {"u": {"id": col(2, True), "qty": col(3)},
                     "p": {"id": col(1, True), "qty": col(2)}}),
            ev(120, {"d": {"id": col(2, True), "qty": col(3)}}), resolved]


def _avro_msgs():
    def row(i, qty, ts, op):
        price = (1234).to_bytes(2, "big", signed=True)
        return (_avro(1, _zz(i)),
                _avro(2, _zz(i), _zz(1) + _zz(qty), _zz(len(price)) + price,
                      _avro_str(op), _zz(ts)))
    return [row(1, 2, 100, "c"),
            # the key changed by an update: TiCDC's Avro sends the old
            # key's delete and the new key's row
            (_avro(1, _zz(1)), None), row(2, 3, 110, "u"),
            (_avro(1, _zz(2)), None),
            (None, b"\x03" + (130).to_bytes(8, "big"))]


def _decoded(msgs, topic=None):
    env = changetopic.envelope("ticdc")
    env.registry = {1: KEY_SCHEMA, 2: json.dumps(VALUE_SCHEMA)}.get
    out = []
    for k, v in msgs:
        out += env.decode(k, v, topic)
    return env.chosen.name, out


def test_a_ticdc_topic_is_read_in_the_protocol_its_first_message_is():
    rows = {}
    for msgs, topic in ((_canal(), None), (_open(), None),
                        (_avro_msgs(), "shop.orders")):
        name, got = _decoded(msgs, topic)
        marks = [m.version for m in got if isinstance(m, changetopic.Mark)]
        assert marks == [130], name
        rows[name] = [r for r in got if isinstance(r, changetopic.Row)]
    assert set(rows) == {"canal-json", "open-protocol", "avro"}
    # canal-json and the open protocol carry each change's own commit
    want = [("upsert", 100, (("id", "1"),)), ("upsert", 110, (("id", "2"),)),
            ("delete", 110, (("id", "1"),)), ("delete", 120, (("id", "2"),))]
    for name in ("canal-json", "open-protocol"):
        got = [(r.op, r.version, r.ident()[1]) for r in rows[name]]
        assert got == want, name
        assert {r.table for r in rows[name]} == {"orders"}
    # Avro's tombstone has no timestamp: the key is changed as of the last
    # one seen and read back at the watermark - the same keys reach the
    # same read back, below the same mark
    avro = rows["avro"]
    assert {r.ident()[1] for r in avro} == {w[2] for w in want}
    assert all(r.version <= 130 for r in avro)
    assert [r.op for r in avro] == ["upsert", "changed", "upsert", "changed"]
    assert changetopic.envelope("ticdc").READ_BACK


def test_an_avro_decimal_is_read_exactly():
    from decimal import Decimal
    price = (-1234).to_bytes(2, "big", signed=True)
    got, _ = changetopic._avro_read(VALUE_SCHEMA["fields"][2]["type"],
                                    _zz(len(price)) + price, 0)
    assert got == Decimal("-12.34")


def test_a_topic_of_no_ticdc_protocol_is_refused_by_name():
    import pytest
    with pytest.raises(SystemExit, match="no protocol of TiCDC"):
        _decoded([(None, b'{"hello": 1}')])


# ---- bulk ways --------------------------------------------------------------

def _climbed(rungs, **facts):
    return decide.climb("a table", rungs, decide.Facts(facts), prove=False)


def test_a_cockroachdb_table_is_imported_through_a_stage_given_and_copied_else():
    full = dict(stage="s3://example/landing?AUTH=implicit", same=True,
                key=True, empty=True)
    assert _climbed(decide.crdb_bulk_ways(), **full).rung.name == "import"
    for lack, said in (("stage", "no stage is given"),
                       ("key", "no key"), ("empty", "holds rows")):
        got = _climbed(decide.crdb_bulk_ways(), **{**full, lack: None})
        assert got.rung.name == "copier"
        assert said in dict((r.name, why) for r, why in got.passed)["import"]


def test_the_import_exports_into_a_place_of_its_own_and_imports_from_it(
        tmp_path):
    from migkit.engines.cockroachdb import CockroachEngine
    eng = CockroachEngine(_hop("cockroachdb",
                               stage="s3://example/landing?AUTH=implicit"))
    ran = []
    eng._alone = lambda side, db, sql, args=None: (
        ran.append((side, sql)) or [(1, "succeeded", 1.0, 7, 0, 99)])
    eng.neutral_columns = lambda side, db, t: [("id", "int8"), ("v", "text")]
    eng._qualified = lambda side, db, t: f"{side}.{t}"
    assert eng._imported("s", "public.t", eng.hop.target.options["stage"],
                         lambda m: None) == 7
    (s1, export), (s2, imp) = ran
    assert (s1, s2) == ("src", "dst")
    place = export.split("'")[1].split("?")[0]
    assert export.startswith("export into csv") and "nullas" in export
    assert f"('{place}/*.csv?AUTH=implicit')" in imp and "nullif" in imp
    assert imp.startswith("import into dst.public.t (")


def test_a_tidb_table_is_loaded_whole_into_an_empty_table_only():
    assert _climbed(decide.tidb_bulk_ways(), empty=True).rung.name == "load"
    got = _climbed(decide.tidb_bulk_ways(), empty=False)
    assert got.rung.name == "copier" and "holds rows" in got.passed[0][1]


def test_couchbase_loads_by_its_importer_where_installed_else_by_upserts():
    # a program never timed here ranks below the copier until its trial
    # (`tableway.each`) or a run measured it cheaper
    on = _climbed(decide.couchbase_bulk_ways(), loader=True)
    assert [r.name for r in on.ranked] == ["copier", "loader"]
    timed = decide.climb("a table", decide.couchbase_bulk_ways(),
                         decide.Facts({"loader": True}), prove=False,
                         costs={"loader": 0.1, "copier": 1.0})
    assert timed.rung.name == "loader" and timed.rung.external
    off = _climbed(decide.couchbase_bulk_ways(), loader=False)
    assert [r.name for r in off.ranked] == ["copier"]
    assert "cbimport" not in timed.rung.said


def test_cosmos_writes_in_bulk_where_the_partition_key_is_read():
    assert _climbed(decide.cosmos_bulk_ways(), pk=["/pk"]).rung.name == "bulk"
    assert _climbed(decide.cosmos_bulk_ways(), pk=[]).rung.name == "copier"


# ---- accounts ----------------------------------------------------------------

def test_a_cockroachdb_role_and_user_are_made_with_the_hash_it_keeps():
    from migkit.engines.cockroachdb import CockroachEngine
    eng = CockroachEngine(_hop("cockroachdb"))
    eng._qualified = lambda side, db, t: f"{side}.{t}"
    src = {"role:reader": {"login": False, "roles": [], "hash": None,
                           "grants": ["SELECT ON s.public.t"], "attrs": {}},
           "user:app": {"login": True, "roles": ["reader"], "grants": [],
                        "hash": "SCRAM-SHA-256$4096:abc", "attrs": {}},
           "user:nohash": {"login": True, "roles": [], "grants": [],
                           "hash": None, "attrs": {}}}
    steps, skipped = eng.account_steps(src, {}, {})
    runs = [s["run"] for s in steps]
    assert runs[0] == 'create role "reader" with nologin'
    assert 'grant select on table dst.public.t to "reader"' in runs
    assert ("create role \"app\" with login password"
            " 'SCRAM-SHA-256$4096:abc'") in runs
    assert 'grant "reader" to "app"' in runs
    assert all("SCRAM" not in s["shown"] for s in steps)
    assert [k for k, _ in skipped] == ["user:nohash"]
    steps, _ = eng.account_steps(src, {}, {"nohash": "CHANGE_ME"})
    given = [s for s in steps if s["key"] == "user:nohash"][0]
    assert given["given"] and "CHANGE_ME" not in given["shown"]


def test_a_tidb_user_is_made_by_its_plugin_and_hash():
    from migkit.engines.tidb import TiDBEngine
    eng = TiDBEngine(_hop("tidb"))
    src = {"role:r@%": {"login": False, "roles": [], "hash": None,
                        "grants": ["SELECT ON s.*"], "attrs": {}},
           "user:app@%": {"login": True, "roles": ["r@%"], "grants": [],
                          "hash": "*ABC", "attrs": {
                              "plugin": "mysql_native_password"}}}
    eng._d = lambda side, db: "d"
    runs = [s["run"] for s in eng.account_steps(src, {}, {})[0]]
    assert runs == ["create role 'r'@'%'", "grant select on `d`.* to 'r'@'%'",
                    "create user 'app'@'%' identified with"
                    " 'mysql_native_password' as '*ABC'",
                    "grant 'r'@'%' to 'app'@'%'"]


def test_a_couchbase_user_is_written_whole_with_the_passwords_files_word():
    from migkit.engines.couchbase import CouchbaseEngine
    eng = CouchbaseEngine(_hop("couchbase"))
    src = {"user:app": {"login": True, "roles": ["readers"], "hash": None,
                        "grants": ["DATA_READER ON s.inv.items"],
                        "attrs": {}}}
    steps, skipped = eng.account_steps(src, {}, {})
    assert not steps and skipped[0][0] == "user:app"
    steps, _ = eng.account_steps(src, {}, {"app": "CHANGE_ME"})
    run = json.loads(steps[0]["run"])
    assert run["path"] == "/settings/rbac/users/local/app"
    assert run["form"] == {"roles": "data_reader[d:inv:items]",
                           "groups": "readers", "password": "CHANGE_ME"}
    assert "CHANGE_ME" not in steps[0]["shown"]
    assert json.loads(steps[0]["undo"])["method"] == "DELETE"


# ---- snapshots ---------------------------------------------------------------

def test_a_cockroachdb_snapshot_keeps_the_clusters_time_and_definitions(
        tmp_path):
    from migkit.engines.cockroachdb import CockroachEngine
    eng = CockroachEngine(_hop("cockroachdb"))
    eng._rows = lambda side, db, sql, args=None: (
        [("1701102296662969433.0000000012",)] if "logical" in sql
        else [("t", "CREATE TABLE t (id INT8 PRIMARY KEY)")])
    eng.neutral_tables = lambda side, db: ["public.t"]
    eng._qualified = lambda side, db, t: t
    eng.snapshot_state("s", tmp_path)
    assert (tmp_path / "dst-mark.txt").read_text().startswith("17011022966")
    assert "PRIMARY KEY" in (tmp_path / "dst-schema.sql").read_text()


class _Feed:
    """A container's change feed, one log a feed range, a continuation the
    count of entries read - as the SDK's etag stands for a position."""
    id = "items"

    def __init__(self):
        self.log = {0: [], 1: []}
        self.client_connection = type("C", (), {})()
        self.client_connection.last_response_headers = {}

    def read_feed_ranges(self):
        return [{"r": 0}, {"r": 1}]

    def query_items_change_feed(self, max_item_count=1000, continuation=None,
                                feed_range=None, start_time=None, mode=None,
                                **kw):
        if continuation:
            r, at = (int(x) for x in continuation.split(":"))
        else:
            r, at = feed_range["r"], len(self.log[feed_range["r"]])
        page = self.log[r][at:at + max_item_count]
        self.client_connection.last_response_headers["etag"] = \
            f"{r}:{at + len(page)}"
        feed = self

        class _P:
            def by_page(self):
                return iter([page])

            def __iter__(self):
                return iter(page)
        del feed
        return _P()


def test_a_cosmos_continuation_resumes_without_an_item_twice():
    from migkit.engines.cosmosdb import CosmosLanes
    c = _Feed()
    start = CosmosLanes(c, "LatestVersion").ends()
    c.log[0] += [{"id": "a"}, {"id": "b"}]
    c.log[1] += [{"id": "c"}]
    got, at, _ = CosmosLanes(c, "LatestVersion").read(start, 10, 5)
    assert sorted(i[3]["id"] for i in got) == ["a", "b", "c"]
    c.log[0] += [{"id": "d"}]
    # a reader started again from the saved continuations
    got, _, _ = CosmosLanes(c, "LatestVersion").read(json.loads(
        json.dumps(at)), 10, 5)
    assert [i[3]["id"] for i in got] == ["d"]


# ---- large values: read whole, a piece at a time, by the row's key -----------

class _Cur:
    def __init__(self, value):
        self.value, self.asked, self.got = value, [], None

    def fetchone(self):
        return self.got


def _large(engine_cls, hop):
    from migkit import pieces
    eng = engine_cls(hop)
    value = bytes(range(256)) * 40
    cur = _Cur(value)

    class _Conn:
        def cursor(self):
            return cur

        def close(self):
            pass

        def rollback(self):
            pass

    def run(c, sql, args=None):
        import re
        c.asked.append((sql, args))
        if "octet_length" in sql:
            c.got = (len(value),)
            return
        at, n = (int(x) for x in re.search(r"from (\d+) for (\d+)",
                                           sql).groups())
        c.got = (value[at - 1:at - 1 + n],)
    eng._connect = lambda side, db: _Conn()
    eng._run = run
    eng.neutral_key = lambda side, db, t: ["id"]
    eng.neutral_columns = lambda side, db, t: [("id", "int8"),
                                               ("doc", "bytea")]
    eng._qualified = lambda side, db, t: t
    eng.piece_limits = lambda side, db: []
    plan = pieces.ReadPlan(eng, "src", "s", "t", [("id", "int"),
                                                  ("doc", "bytes")],
                           limit=1024)
    assert "octet_length" in plan.select and "case when" in plan.select
    rows = plan.finish([(7, None, len(value))])
    return rows, value, cur


def test_a_large_value_is_read_whole_in_pieces_on_cockroachdb_and_dsql():
    from migkit.engines.cockroachdb import CockroachEngine
    from migkit.engines.dsql import DsqlEngine
    for cls, engine in ((CockroachEngine, "cockroachdb"),
                        (DsqlEngine, "dsql")):
        rows, value, cur = _large(cls, _hop(engine))
        assert rows == [[7, value]], engine
        assert len(cur.asked) >= 2
        assert all(a == [7] or a == (7,) for _, a in cur.asked)
        assert decide.cell(engine, "large values")[0] == "yes"
