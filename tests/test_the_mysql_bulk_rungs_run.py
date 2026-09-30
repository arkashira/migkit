"""The MySQL bulk rungs against two servers: each program's own checksums
held to the source and able to say "different", the straight copy's rung
with and without the target's local loads, masking on the way, a stopped
resumable load going on only while the source is where its dump left it,
and the per-table choice sending a table to the table copier end to end.

Measured on mysql:8.4 in docker, the programs on the host (mydumper
1.0.5, the straight copy 26.7.1).
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = "migkit-test-mytools-src", "migkit-test-mytools-dst"
SRC_PORT, DST_PORT = 16110, 16111
USER, SECRET = "mover", "not-a-real-secret-42"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

have_dump = pytest.mark.skipif(
    not (movers.which("mydumper") and movers.which("myloader")),
    reason="the dump and load programs are not installed here")
have_shell = pytest.mark.skipif(not movers.which("mysqlsh"),
                                reason="the straight copy is not installed")


def _sql(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    names = (SRC, DST)
    try:
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{p}:3306", "mysql:8.4",
                            "--local-infile=1"],
                           check=True, capture_output=True)
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                     "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
            _sql(n, f"create user '{USER}'@'%' identified by '{SECRET}';"
                    f" grant all on *.* to '{USER}'@'%';")
        yield
    finally:
        for n in names:
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, options=None, name="my"):
    hop = Hop(name=name, engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user=USER,
                              password=SECRET),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user=USER,
                              password=SECRET),
              databases=["appdb"], options=dict(options or {}), workers=2)
    root = tmp_path / "reports"

    def report_dir(db=None):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


SCHEMA = ("create table orders (id int primary key, amount decimal(12,2),"
          " note varchar(40), key kn (note));"
          " create table people (id int primary key, email varchar(80),"
          " name varchar(40), photo varbinary(64));")


def _seed(amount_type="decimal(12,2)", rows=2000):
    _sql(SRC, "drop database if exists appdb; create database appdb;"
              f" use appdb; {SCHEMA}"
              " set session cte_max_recursion_depth = 100000;"
              " insert into orders with recursive s(i) as (select 1 union all"
              f" select i + 1 from s where i < {rows}) select i, i / 7,"
              " concat('n', i) from s;"
              " insert into people with recursive s(i) as (select 1 union all"
              f" select i + 1 from s where i < {rows}) select i,"
              " concat('p', i, '@example.com'), concat('name', i),"
              " unhex(md5(i)) from s;")
    _sql(DST, "drop database if exists appdb; create database appdb;"
              f" use appdb; {SCHEMA.replace('decimal(12,2)', amount_type)}")


def _digest(name, table):
    return _sql(name, f"select count(*), sum(crc32(concat_ws('#', id,"
                      f" {'amount, note' if table == 'orders' else 'email, name, hex(photo)'})))"
                      f" from appdb.{table}")


def _dump_only(monkeypatch):
    """The table ladder held to the parallel dump and load: what these
    tests are of, whichever way the numbers would give a table - one sent
    to the copier is the copier's after the move, which a direct call of
    the move does not run."""
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "TABLE_WAYS", tuple(
        r for r in MySQLEngine.TABLE_WAYS if r.name == movers.DUMP_LOAD))


def _no_tool(text):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    low = str(text).lower()
    return not [t for t in TOOLS + ("mysqlsh",) if t in low]


# ---- the dump and load ------------------------------------------------------

@needs_docker
@have_dump
def test_each_table_is_held_to_its_own_checksum_pair(pair, tmp_path,
                                                     monkeypatch):
    from migkit import tableway
    _dump_only(monkeypatch)
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    _seed()
    hop = _hop(tmp_path)
    said = []
    movers.mydumper_move(hop, "appdb", 2, True, said.append)
    for t in ("orders", "people"):
        assert _digest(SRC, t) == _digest(DST, t), t
    joined = "\n".join(said)
    assert "appdb.orders: loading" in joined, joined
    assert "dumped and loaded 4,000 rows in" in joined, joined
    # each table's own rate, from the programs' events
    for t in ("orders", "people"):
        assert f"appdb.{t}: 2,000 rows, " in joined, joined
    assert _no_tool(joined), joined
    # each table's time, kept to count once the move is done
    # each table's rows, bytes and time, to teach the cost model once the
    # move is done
    pending = json.loads((hop.report_dir("appdb")
                          / tableway.PENDING).read_text())
    got = {rows: secs for rows, _, secs in pending[movers.DUMP_LOAD]}
    assert set(got) == {2000} and all(s > 0 for s in got.values()), got
    assert len(pending[movers.DUMP_LOAD]) == 2, pending
    # each table's way, in the decision record and not on the console
    from migkit import decide
    ways = {e["work"]: e["way"] for e in decide.recorded(hop)}
    assert ways["appdb.orders"] == "dumped in parallel chunks and loaded"
    assert not any("dumped in parallel chunks" in m for m in said), said


@needs_docker
@have_dump
def test_the_copys_own_checksum_says_different(pair, tmp_path,
                                                monkeypatch):
    """A target column one decimal place narrower takes every value
    rounded, under a strict sql_mode too (a note, not an error): the load
    says it finished, and the dump's checksum and the load's disagree."""
    _dump_only(monkeypatch)
    _seed(amount_type="decimal(12,1)")
    with pytest.raises(SystemExit) as e:
        movers.mydumper_move(_hop(tmp_path), "appdb", 2, True, None)
    said = str(e.value)
    assert "checksum of 1 tables differs" in said and "orders" in said, said
    assert "people" not in said, said
    assert _no_tool(said), said


# ---- the straight copy ------------------------------------------------------

@needs_docker
@have_shell
@pytest.mark.parametrize("disk", ["room", "short"])
def test_the_straight_copy_carries_every_table(pair, tmp_path, monkeypatch,
                                               disk):
    """Through its own dump where the disk holds one - measured faster -
    and read and written at once where it does not."""
    _seed()
    if disk == "short":
        monkeypatch.setattr(movers, "_disk_free", lambda path: 1)
    hop = _hop(tmp_path)
    said = []
    steps = movers.mysqlsh_move(hop, "appdb", 2, True, said.append)
    for t in ("orders", "people"):
        assert _digest(SRC, t) == _digest(DST, t), t
    how = "straight across" if disk == "short" else "through a dump"
    assert any(f"copied 4,000 rows {how} in" in s for s in said), said
    if disk == "short":
        assert any("copying tables straight from source to target" in s
                   for s in steps), steps
    else:
        assert any("reading the source into a local copy" in s
                   for s in steps), steps
    at = json.loads((hop.report_dir("appdb")
                     / "dump-position.json").read_text())
    assert at["log_file"] and at["log_pos"] > 0, at
    # nothing of the source is left on this machine
    assert not (hop.report_dir("appdb") / "straight-copy").exists()
    assert _no_tool("\n".join(said + [str(s) for s in steps])), said


@needs_docker
@have_shell
@pytest.mark.parametrize("disk", ["room", "short"])
def test_the_straight_copys_checksum_says_different(pair, tmp_path,
                                                    monkeypatch, disk):
    """Measured: its local load takes a rounded value with a warning under
    a strict sql_mode too - the server cannot stop a client mid-file - so
    its checksum and migkit's digest are what catch it."""
    _seed(amount_type="decimal(12,1)")
    if disk == "short":
        monkeypatch.setattr(movers, "_disk_free", lambda path: 1)
    with pytest.raises(SystemExit) as e:
        movers.mysqlsh_move(_hop(tmp_path), "appdb", 2, True, None)
    said = str(e.value)
    assert "differs from the source's: orders" in said, said
    assert "warnings" in said, said
    assert _no_tool(said), said


@needs_docker
@have_shell
def test_a_target_without_local_loads_is_asked_never_changed(pair,
                                                             tmp_path):
    _seed()
    _sql(DST, "set global local_infile = 0")
    try:
        from migkit import tableway
        from migkit.engines.mysql import MySQLEngine
        hop = _hop(tmp_path)
        got = tableway.choose(MySQLEngine(hop), "appdb", "mysqlsh",
                              fresh=True)
        assert {p.rung for p in got.values()} == {"rows"}, got
        assert all("local_infile is off" in p.reason
                   for p in got.values()), got
        steps = movers.mysqlsh_move(hop, "appdb", 2, False, None)
        assert not any("straight from source" in s for s in steps), steps
        from migkit import planner
        plan = planner.plan(hop, "appdb", "mysqlsh", ["orders", "people"],
                            None)
        assert {d.path for d in plan} == {planner.COPIER}, plan
        assert _sql(DST, "select @@global.local_infile") == "0"
    finally:
        _sql(DST, "set global local_infile = 1")


# ---- masking on the way ----------------------------------------------------

@needs_docker
@have_dump
def test_the_masked_columns_are_masked_and_held_to_the_rule(pair, tmp_path,
                                                          monkeypatch):
    _dump_only(monkeypatch)
    _seed()
    hop = _hop(tmp_path, {"mask_on_write": {
        "people.email": "null", "people.name": "constant anonymous"}})
    steps = movers.mydumper_move(hop, "appdb", 2, True, None)
    assert _sql(DST, "select count(*) from appdb.people where email is not"
                     " null or name <> 'anonymous'") == "0"
    assert _sql(DST, "select count(*) from appdb.people") == "2000"
    assert _digest(SRC, "orders") == _digest(DST, "orders")
    # the photo, which no rule names, is the source's
    assert _sql(SRC, "select sum(crc32(photo)) from appdb.people") == \
        _sql(DST, "select sum(crc32(photo)) from appdb.people")
    assert movers.masked_tables(hop, "appdb") == {"appdb.people"}
    assert any("masked on the way" in str(s) for s in steps), steps


@needs_docker
@have_dump
def test_a_column_left_unmasked_is_said(pair, tmp_path, monkeypatch):
    """The hold on masked tables can say they are not what the rule makes
    them: here nothing masked them."""
    _dump_only(monkeypatch)
    _seed()
    hop = _hop(tmp_path)
    movers.mydumper_move(hop, "appdb", 2, True, None)
    got = movers._my_masked_held(hop, "appdb", {"people": {
        "email": ("null", ""), "name": ("constant", "anonymous")}})
    assert any("people.email: 2,000 rows not masked" in g for g in got), got
    assert any("people.name: 2,000 rows not masked" in g for g in got), got
    _sql(DST, "delete from appdb.people where id <= 10")
    got = movers._my_masked_held(hop, "appdb", {"people": {}})
    assert got == ["people: 1,990 rows where the source has 2,000"], got


@needs_docker
@have_dump
def test_a_rule_on_a_key_stops_the_move_before_it_writes(pair, tmp_path):
    _seed()
    _sql(DST, "insert into appdb.people values (99999, 'kept', 'kept',"
              " null)")
    hop = _hop(tmp_path, {"mask_on_write": {"people.id": "null"}})
    with pytest.raises(SystemExit) as e:
        movers.mydumper_move(hop, "appdb", 2, True, None)
    assert "part of a key" in str(e.value) and "Nothing has been written" \
        in str(e.value)
    assert _sql(DST, "select count(*) from appdb.people") == "1"


# ---- a resumable load --------------------------------------------------------

@needs_docker
@have_shell
def test_a_stopped_load_goes_on_only_while_the_source_is_unchanged(
        pair, tmp_path, monkeypatch):
    _seed()
    real = movers._shell_run
    ran = []

    def stopping(hop, db, spec, script, log=None):
        ran.append(spec["what"])
        if spec["what"] == "load" and stopping.stop:
            raise RuntimeError("the load was stopped")
        return real(hop, db, spec, script, log)
    stopping.stop = True
    monkeypatch.setattr(movers, "_shell_run", stopping)
    hop = _hop(tmp_path)
    with pytest.raises(RuntimeError):
        movers.mysqlsh_move(hop, "appdb", 2, True, None)
    assert ran == ["dump", "load"], ran
    record = hop.report_dir("appdb") / "bulk-resume.json"
    assert record.exists()
    assert (hop.report_dir("appdb") / "straight-copy" / "@.done.json").exists()
    # the next move goes on from the dump: no second dump
    ran.clear()
    stopping.stop = False
    said = []
    movers.mysqlsh_move(hop, "appdb", 2, True, said.append)
    assert ran == ["load"], ran
    assert any("go on loading" in s for s in said), said
    for t in ("orders", "people"):
        assert _digest(SRC, t) == _digest(DST, t), t
    assert not record.exists()
    assert not (hop.report_dir("appdb") / "straight-copy").exists()
    # stopped again, and the source written to since: dumped again
    ran.clear()
    stopping.stop = True
    with pytest.raises(RuntimeError):
        movers.mysqlsh_move(hop, "appdb", 2, True, None)
    _sql(SRC, "update appdb.orders set note = 'changed' where id = 1")
    ran.clear()
    stopping.stop = False
    movers.mysqlsh_move(hop, "appdb", 2, True, None)
    assert ran == ["dump", "load"], ran
    assert _sql(DST, "select note from appdb.orders where id = 1") == \
        "changed"


# ---- the choice, end to end -------------------------------------------------

@needs_docker
@have_dump
def test_a_table_measured_faster_table_by_table_goes_that_way(pair,
                                                             tmp_path):
    """Through the command: what the hop measured says the table copier's
    rows are fastest for a table like `orders` and the dump for one like
    `people`, so `orders` is left out of the dump and carried by the
    copier after it, on the way the move chose for it, and the move is
    held to the source whole."""
    from migkit import costmodel, session
    _seed()
    reports = tmp_path / "reports"
    (reports / "bench").mkdir(parents=True)
    # the hop's fits: each way's time on each table, as a run keeps them
    orders = {"rows": 2000, "bytes": 98_304}
    people = {"rows": 2000, "bytes": 212_992}
    fits = {}
    for way, (a, b) in {"rows": (0.001, 100.0),
                        "pass-through": (100.0, 100.0),
                        movers.STREAM_COPY: (100.0, 100.0),
                        movers.DUMP_LOAD: (100.0, 0.001)}.items():
        f = costmodel.Fit()
        for _ in range(20):
            f.update(costmodel.features(orders), a)
            f.update(costmodel.features(people), b)
        fits[way] = f.to_dict()
    (reports / "bench" / costmodel.HOP_FILE).write_text(json.dumps({
        "mysql->mysql table copy": fits}))
    conf = tmp_path / "hops.yaml"
    conf.write_text(f"""hops:
  bench:
    engine: mysql
    databases: [appdb]
    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: {USER}, password: {SECRET}}}
    target: {{host: 127.0.0.1, port: {DST_PORT}, user: {USER}, password: {SECRET}}}
""")
    # its own knowledge base: no other test's ways are a prior here
    env = dict(os.environ, MIGKIT_CONF=str(conf), MIGKIT_REPORTS=str(reports),
               MIGKIT_MOVER="mydumper", MIGKIT_EXPLAIN="",
               MIGKIT_KNOWLEDGE=str(tmp_path / "knowledge.json"))
    got = subprocess.run([sys.executable, "-c",
                          "from migkit.cli import main; main()", "move",
                          "bench", "--db", "appdb", "--go"],
                         env=env, capture_output=True, text=True, cwd=ROOT)
    out = got.stdout + got.stderr
    assert got.returncode == 0, out
    assert "orders: table by table" in out, out
    assert "bulk copy complete" in out, out
    omitted = (reports / "bench" / "appdb" / "omit-tables.txt").read_text()
    assert "appdb.orders" in omitted and "people" not in omitted, omitted
    # decided once: the copier took the way the move chose, and said so in
    # the decision record, not on the console
    rec = [json.loads(x) for x in (reports / "bench" / session.DECISIONS)
           .read_text().splitlines()]
    ways = [e["way"] for e in rec if e.get("work") == "appdb.orders"]
    assert len(ways) >= 2 and set(ways) == {"the table copier's rows"}, ways
    assert "the table copier's rows" not in out, out
    for t in ("orders", "people"):
        assert _digest(SRC, t) == _digest(DST, t), t
    assert _no_tool(out), out
