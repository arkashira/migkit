"""SQL Server as a full target: rows go in through its own bulk load.

The bulk load commits on a connection of its own, so it loads a stage - a
global temporary table - and the stage is promoted in one transaction of
migkit's: a batch lands whole or not at all, and a batch run again
replaces itself. A move between two SQL Servers reads the source as Arrow
and loads it as it was read; the table copier's batches go by whichever
way costs less for their size. What arrives is held to the source by the
server's own rendering of every row and by the bytes of its columns:
identity values, NULL apart from empty, varchar apart from nvarchar,
datetime2(7), time(7) and datetimeoffset to the last digit, decimal(38,x),
varbinary(max) and uniqueidentifier.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import mssql_run, needs_docker

MS, PORT, PW = "migkit-test-ms-bulk", 16131, "CHANGE_ME-Bulk-9x!"

TYPES = ("id int identity(5,3) primary key,"
         " v varchar(20) collate Latin1_General_CI_AS null,"
         " nv nvarchar(20) null, d2 datetime2(7) null,"
         " dec decimal(38,10) null, vb varbinary(max) null,"
         " u uniqueidentifier null, dto datetimeoffset(7) null,"
         " tm time(7) null")
COLS = "v, nv, d2, dec, vb, u, dto, tm"
ROWS = ("('caf' + char(233), N'ไทย€', '2024-02-29 13:14:15.1234567',"
        " 1234567890123456789012345678.1234567891, 0x00ff00,"
        " 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',"
        " '2024-02-29 13:14:15.1234567 +07:00', '23:59:59.9999999'),"
        " ('', N'', '9999-12-31 23:59:59.9999999', -0.0000000001, 0x,"
        " cast(cast(0 as binary(16)) as uniqueidentifier),"
        " '0001-01-01 00:00:00.0000001 -14:00', '00:00:00.0000001'),"
        " (null, null, null, null, null, null, null, null)")


def _sql(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(PORT), user="sa",
                           password=PW, database=db, autocommit=True,
                           tds_version="7.4", login_timeout=10)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall() if cur.description else None
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)
    subprocess.run(mssql_run(MS, PORT, PW, memory_mb=2048), check=True,
                   capture_output=True)
    try:
        end = time.time() + 600
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                up = s.connect_ex(("127.0.0.1", PORT)) == 0
            if up:
                try:
                    _sql("master", "select 1")
                    break
                except Exception:
                    pass
            time.sleep(2)
        else:
            pytest.fail("SQL Server never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


def _engine(tmp_path):
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    eng = MSSQLEngine(Hop(name="bulk", engine="mssql", source=ep, target=ep,
                          databases=["a"], db_map={"a": "b"}))
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


@pytest.fixture
def eng(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("a", "b"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
    return _engine(tmp_path)


def _typed(eng):
    """The typed table on both sides, filled on the source only - with a
    gap in its identity, which a load that numbered rows itself would
    close."""
    for db in ("a", "b"):
        _sql(db, f"create table dbo.t ({TYPES})")
    _sql("a", f"insert into dbo.t ({COLS}) values {ROWS}",
         "delete from dbo.t where id = 8",
         f"insert into dbo.t ({COLS}) values {ROWS}")


def _bytes(db):
    """Each row's columns as the bytes the server stores."""
    return _sql(db, "select id, cast(v as varbinary(max)),"
                    " cast(nv as varbinary(max)), cast(d2 as varbinary(max)),"
                    " cast(dec as varbinary(max)), vb,"
                    " cast(u as varbinary(max)), cast(dto as"
                    " varbinary(max)), cast(tm as varbinary(max)), case when"
                    " v is null then 1 else 0 end from dbo.t order by id")


def _data(eng):
    return [(r.status, r.detail) for r in eng.check_data("a")
            if r.check == "data"]


def _load_first(monkeypatch):
    """What each way was measured to cost, as though statements were the
    dearer at any size: every batch goes by the bulk load until it is
    timed here dearer still."""
    from migkit.engines.mssql import MSSQLEngine
    monkeypatch.setattr(MSSQLEngine, "WRITE_COSTS",
                        {"statements": (1.0, 1.0), "bulk load": (0.0, 0.0)})


def _stages():
    return _sql("tempdb", "select count(*) from sys.tables where name like"
                          " '##migkit_stage%'")[0][0]


@needs_docker
def test_a_move_between_servers_arrives_byte_for_byte(eng):
    _typed(eng)
    said = []
    eng.native_bulk("a", ["dbo.t"], True, said.append)
    assert any("dbo.t: copied, 5 rows" in s for s in said), said
    src, dst = _bytes("a"), _bytes("b")
    assert [r[0] for r in src] == [5, 11, 14, 17, 20], src
    assert dst == src
    assert _data(eng) == [("ok", "1 tables, counts and every row's hash"
                                 " equal both sides")]
    assert _stages() == 0
    # the same comparison says so when one digit of one value differs
    _sql("b", "update dbo.t set d2 = '2024-02-29 13:14:15.1234568'"
              " where id = 5")
    assert _bytes("b") != src
    assert [s for s, _ in _data(eng)] == ["diff"]


@needs_docker
def test_a_batch_goes_through_the_stage_whole_and_run_again_the_same(
        eng, monkeypatch):
    _typed(eng)
    _load_first(monkeypatch)
    cols = [(n, c) for n, c in (("d2", "timestamp"), ("dec", "decimal"),
                                ("id", "integer"), ("nv", "text"),
                                ("tm", "time"), ("u", "text"), ("v", "text"),
                                ("vb", "bytes"))]
    rows = eng.neutral_read("src", "a", "dbo.t", cols, None, 100)[0]
    assert eng.write_way("dst", "a", "dbo.t", cols, len(rows))[0] == \
        "bulk load"
    for _ in range(2):
        assert eng.neutral_write("dst", "a", "dbo.t", cols, rows) == 5
    got = _bytes("b")
    assert len(got) == 5 and [r[0] for r in got] == [r[0] for r in
                                                     _bytes("a")]
    # every column written is the source's to the byte - datetime2(7) and
    # time(7) to the seventh digit, which crossed the load as the server's
    # text of them; the one column not written is left NULL
    assert [r[:7] + r[8:] for r in got] == [r[:7] + r[8:]
                                           for r in _bytes("a")]
    assert _sql("b", "select count(*) from dbo.t where dto is not null") \
        == [(0,)]
    assert _stages() == 0
    # by the load both times: a refusal would have sent them by statements
    # and kept the table off the load from then on
    assert eng.write_way("dst", "a", "dbo.t", cols, len(rows))[0] == \
        "bulk load"
    # and timed here, which the decision layer reads
    got = [w for w in eng.measured_ways()
           if (w["work"], w["table"]) == ("rows", "dbo.t")]
    assert got and got[0]["chosen"] == "bulk load", got
    assert got[0]["seconds_a_row"]["bulk load"] > 0, got


@needs_docker
def test_a_table_the_load_is_not_proved_on_goes_table_by_table(eng):
    """A varchar of a UTF-8 collation, which the load refuses (it counts
    30 characters in four): the table is copied table by table, and its
    batches by statements, and it arrives."""
    for db in ("a", "b"):
        _sql(db, "create table dbo.w (id int primary key, u8 varchar(20)"
                 " collate Latin1_General_100_CI_AS_SC_UTF8 null)")
    _sql("a", "insert into dbo.w values (1, N'ไทย😀'), (2, ''), (3, null)")
    said = []
    eng.native_bulk("a", ["dbo.w"], True, said.append)
    assert "dbo.w: copied table by table - u8 is of a type the bulk load" \
           " is not proved on" in said, said
    q = "select id, cast(u8 as varbinary(max)) from dbo.w order by id"
    assert _sql("b", q) == _sql("a", q)
    assert _sql("b", q)[0][1] == "ไทย😀".encode()


@needs_docker
def test_a_promote_that_fails_leaves_the_table_as_it_was(eng,
                                                        monkeypatch):
    for db in ("a", "b"):
        _sql(db, "create table dbo.c (id int primary key, n int check"
                 " (n >= 0))")
    _sql("b", "insert into dbo.c values (1, 10), (2, 20)")
    _load_first(monkeypatch)
    cols = [("id", "integer"), ("n", "integer")]
    with pytest.raises(Exception) as e:
        eng.neutral_write("dst", "a", "dbo.c", cols,
                          [[1, 11], [3, 30], [4, -1]])
    assert "CHECK" in str(e.value).upper(), e.value
    # the stage had taken all three rows; none of them reached the table
    assert _sql("b", "select id, n from dbo.c order by id") == [(1, 10),
                                                                (2, 20)]
    assert _stages() == 0
    eng.neutral_write("dst", "a", "dbo.c", cols, [[1, 11], [3, 30]])
    assert _sql("b", "select id, n from dbo.c order by id") == [
        (1, 11), (2, 20), (3, 30)]


@needs_docker
def test_a_batch_the_load_refuses_goes_by_statements(eng, monkeypatch):
    """Where the bulk load does not fill its stage, nothing has reached the
    table: the batch lands by statements, and the rest of the table goes
    that way too, saying why."""
    from migkit.engines import mssql
    for db in ("a", "b"):
        _sql(db, "create table dbo.c (id int primary key, n int)")
    _sql("b", "insert into dbo.c values (1, 1)")
    _load_first(monkeypatch)
    cols = [("id", "integer"), ("n", "integer")]
    assert eng.write_way("dst", "a", "dbo.c", cols, 2)[0] == "bulk load"
    loads = []

    def refused(self, rows):
        loads.append(self.name)
        raise mssql._NotLoaded("invalid data for column n")
    monkeypatch.setattr(mssql._Stage, "load", refused)
    assert eng.neutral_write("dst", "a", "dbo.c", cols,
                             [[1, 12], [2, 13]]) == 2
    assert loads and _sql("b", "select id, n from dbo.c order by id") == [
        (1, 12), (2, 13)]
    assert eng.write_way("dst", "a", "dbo.c", cols, 2) == (
        "statements", "the bulk load refused a value of it: invalid data"
                      " for column n")
    assert eng.neutral_write("dst", "a", "dbo.c", cols, [[3, 14]]) == 1
    assert len(loads) == 1
    assert _stages() == 0


@needs_docker
def test_a_whole_move_goes_by_the_bulk_load_and_is_held_to_the_source(
        server, tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, movers
    for db in ("a", "b"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
    _typed(None)
    _sql("a", "create table dbo.big (id bigint primary key, name"
              " nvarchar(40), amount decimal(18,2), at datetime2(7))",
         "with n as (select top 20000 row_number() over (order by (select"
         " null)) i from sys.all_objects a cross join sys.all_objects b)"
         " insert into dbo.big select i, concat(N'name-', i), i * 1.25,"
         " dateadd(second, i, '2024-01-01') from n")
    _sql("b", "create table dbo.big (id bigint primary key, name"
              " nvarchar(40), amount decimal(18,2), at datetime2(7))",
         "insert into dbo.big values (99999, N'stray', 1, '2024-01-01')")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  bulk:\n    engine: mssql\n"
        f"    source: {{host: 127.0.0.1, port: {PORT}, user: sa,"
        f" password: '{PW}'}}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT}, user: sa,"
        f" password: '{PW}'}}\n"
        "    databases: [a]\n    db_map: {a: b}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    assert movers.pick("mssql") == "native"
    got = CliRunner().invoke(cli.main, ["move", "bulk", "--mode", "full",
                                        "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    # the plan's estimate is the catalogue's, and the rate is kept for the
    # next plan and the next choice
    assert "dbo.big: bulk copy - the fastest path here (about 20,000 rows)" \
        in said, said
    assert "dbo.big: copied, 20,000 rows" in said, said
    assert "emptied 1 rows" in said, said
    from migkit import planner
    assert planner.measured_rate(cli.get_hop("bulk"), "native") > 0
    assert _sql("b", "select count(*), sum(amount) from dbo.big") == \
        _sql("a", "select count(*), sum(amount) from dbo.big")
    assert _bytes("b") == _bytes("a")


def _unit(monkeypatch):
    """An engine whose target table is described without a server."""
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="10.0.0.5", port=1433, user="sa", password="CHANGE_ME")
    eng = MSSQLEngine(Hop(name="w", engine="mssql", source=ep, target=ep,
                          databases=["a"]))
    shape = {"declared": {"id": "int", "at": "datetime2(7)",
                          "doc": "sql_variant", "name": "varchar(20)"},
             "key": ["id"], "identity": None, "unwritable": set(),
             "utf8": {"name"}}
    monkeypatch.setattr(MSSQLEngine, "_shape",
                        lambda self, side, db, table: shape)
    monkeypatch.setattr(MSSQLEngine, "_bulk_unusable",
                        staticmethod(lambda: None))
    return eng


def test_the_cheaper_way_for_the_rows_and_the_types(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    eng = _unit(monkeypatch)
    cols = [("id", "integer"), ("at", "timestamp")]
    small = eng.write_way("dst", "a", "dbo.t", cols, 10)
    large = eng.write_way("dst", "a", "dbo.t", cols, 50_000)
    assert small[0] == "statements" and large[0] == "bulk load", (small,
                                                                  large)
    assert large[1] == "the way a row cost less for batches of about" \
                       " 32,768 rows", large
    # a column the load is not proved on keeps the whole batch on
    # statements, and says which
    way, why = eng.write_way("dst", "a", "dbo.t",
                             cols + [("doc", None)], 50_000)
    assert (way, why) == ("statements", "doc is of a type the bulk load is"
                                        " not proved on")
    way, why = eng.write_way("dst", "a", "dbo.t",
                             cols + [("name", "text")], 50_000)
    assert (way, why) == ("statements", "name is of a type the bulk load is"
                                        " not proved on")
    monkeypatch.setattr(MSSQLEngine, "_bulk_unusable",
                        staticmethod(lambda: "its driver does not load"))
    assert eng.write_way("dst", "a", "dbo.t", cols, 50_000) == (
        "statements", "its driver does not load")


def test_what_a_way_cost_here_moves_the_choice(monkeypatch):
    """Measured where it was built, the bulk load wins at 50,000 rows; on
    a run where it has cost a thousand times that a row, statements win -
    and what was chosen, from which costs, is there for the decision
    layer to read."""
    from migkit.engines.mssql import MSSQLEngine
    eng = _unit(monkeypatch)
    cols = [("id", "integer")]
    assert eng.write_way("dst", "a", "dbo.t", cols, 50_000)[0] == \
        "bulk load"
    _, row = MSSQLEngine.WRITE_COSTS["bulk load"]
    key = eng._work_key("rows", "dbo.t", 50_000)
    eng._took(key, "bulk load", 50_000 * row * 1000, 50_000)
    assert eng.write_way("dst", "a", "dbo.t", cols, 50_000)[0] == \
        "statements"
    # a batch of another size is costed on its own
    assert eng.write_way("dst", "a", "dbo.t", cols, 200_000)[0] == \
        "bulk load"
    got = {w["rows_from"]: w for w in eng.measured_ways()
           if (w["work"], w["table"]) == ("rows", "dbo.t")}
    assert got[32768]["chosen"] == "statements", got
    assert got[131072]["chosen"] == "bulk load", got
