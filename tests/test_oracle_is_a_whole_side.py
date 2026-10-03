"""Oracle as a whole side (backlog 11, F5), against Oracle Free 23.26 in
the sandbox: the source in one pluggable database (FREEPDB1), the target
in another (FREEPDB2), PostgreSQL 16 beside them for the pairs.

The container's own init script archives the redo and turns on minimal
supplemental logging - the sandbox's settings, which a change reader needs
from any database it reads; migkit never changes them. Run through
`tools/with_docker_lock.py --vm migkit`, which starts the profile Oracle
Free fits in.

What is held:
* the server sums each row as every engine renders it - the number this
  process folds and PostgreSQL sums - and two copies of a row add, they
  do not cancel;
* large values are read and written in pieces, a batch goes by direct
  path load or array DML - whichever the table measured cheaper - and a
  time with a zone keeps its zone;
* sequences and identity columns, settings, users with their password
  hashes, the target's restore point;
* Oracle to Oracle by Data Pump over a database link, and by the copier
  where Data Pump cannot run;
* the redo followed by migkit's LogMiner reader - whole transactions,
  large ones in parts, a large value, a stop and a restart - fenced,
  confirmed and verified by what changed;
* Oracle to PostgreSQL and PostgreSQL to Oracle, moved, checked, followed.
"""
import ctypes
import datetime
import json
import os
import pathlib
import subprocess
import tempfile
import threading
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

ORA, ORA_PORT = "migkit-test-ora", 16140
PG, PG_PORT = "migkit-test-ora-pg", 16141
READER, READER_PORT = "migkit-test-ora-redo", 16142
#: the database's files, redo and archived redo among them, in a volume a
#: reader run beside it can mount
DATA = "migkit-test-ora-data"
PW = "CHANGE_ME_ora1"
SRC, DST = "FREEPDB1", "FREEPDB2"

INIT = """#!/bin/bash
mkdir -p /opt/oracle/oradata/fra
sqlplus -s / as sysdba <<'SQL'
whenever sqlerror exit failure
alter system set db_recovery_file_dest_size = 8G scope = both;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/fra'
  scope = both;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
alter pluggable database all open;
alter database add supplemental log data;
exit
SQL
"""


def _docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def _running(name):
    got = _docker("inspect", "-f", "{{.State.Running}}", name)
    return got.returncode == 0 and got.stdout.strip() == "true"


def ora_conn(user="migkit", pdb=SRC, sysdba=False, root=False):
    import oracledb
    oracledb.defaults.fetch_lobs = False
    kw = {"mode": oracledb.AUTH_MODE_SYSDBA} if sysdba else {}
    conn = oracledb.connect(user=user, password=PW,
                            dsn=f"127.0.0.1:{ORA_PORT}/"
                                f"{'FREE' if root else pdb}", **kw)
    conn.autocommit = True
    conn.cursor().execute("alter session set time_zone = 'UTC'")
    return conn


def ora(sql, *args, pdb=SRC, user="migkit", sysdba=False, root=False,
        many=None):
    conn = ora_conn(user, pdb, sysdba, root)
    try:
        cur = conn.cursor()
        if many is not None:
            cur.executemany(sql, many)
            return None
        cur.execute(sql, list(args))
        return [tuple(r) for r in cur.fetchall()] if cur.description \
            else None
    finally:
        conn.close()


def pg(sql, db="shop"):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _wait_ready(since, timeout=900):
    end = time.time() + timeout
    while time.time() < end:
        logs = _docker("logs", "--since", since, ORA)
        if "DATABASE IS READY TO USE" in logs.stdout + logs.stderr:
            return
        time.sleep(5)
    pytest.fail("Oracle Free never said it was ready")


def _drop(pdb, *statements):
    for s in statements:
        try:
            ora(s, pdb=pdb, user="sys", sysdba=True)
        except Exception:  # noqa: BLE001 - not there
            pass


@pytest.fixture(scope="module")
def sandbox():
    keep = os.environ.get("MIGKIT_TEST_KEEP_ORACLE") == "1"
    if not (keep and _running(ORA)):
        _docker("rm", "-f", "-v", ORA)
        _docker("volume", "rm", "-f", DATA)
        made = _docker("create", "--name", ORA,
                       "-v", f"{DATA}:/opt/oracle/oradata", "-p",
                       f"127.0.0.1:{ORA_PORT}:1521",
                       # where a redo-file reader run beside it listens
                       "-p", f"127.0.0.1:{READER_PORT}:9000", "-e",
                       f"ORACLE_PASSWORD={PW}", "--shm-size=1g",
                       "gvenzl/oracle-free:slim-faststart")
        assert made.returncode == 0, made.stderr
        with tempfile.NamedTemporaryFile("w", suffix=".sh",
                                         delete=False) as f:
            f.write(INIT)
        os.chmod(f.name, 0o755)
        assert _docker("cp", f.name, f"{ORA}:/container-entrypoint-initdb.d/"
                       "01-archivelog.sh").returncode == 0
        os.unlink(f.name)
        since = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        assert _docker("start", ORA).returncode == 0
        _wait_ready(since)
    if not (keep and _running(PG)):
        _docker("rm", "-f", "-v", PG)
        made = _docker("run", "-d", "--name", PG, "-p",
                       f"127.0.0.1:{PG_PORT}:5432", "-e",
                       "POSTGRES_PASSWORD=test", "postgres:16", "-c",
                       "wal_level=logical")
        assert made.returncode == 0, made.stderr
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(2)
    # the target's own pluggable database, made once, and waited for
    # until its service answers
    have = ora("select count(*) from v$pdbs where name = :1", DST,
               user="sys", sysdba=True, root=True)[0][0]
    if not have:
        ora(f"create pluggable database {DST} admin user pdbadmin"
            f" identified by {PW} file_name_convert ="
            " ('/opt/oracle/oradata/FREE/pdbseed/',"
            f" '/opt/oracle/oradata/FREE/{DST}/')", user="sys", sysdba=True,
            root=True)
    ora(f"alter pluggable database {DST} open", user="sys", sysdba=True,
        root=True) if ora("select open_mode from v$pdbs where name = :1",
                          DST, user="sys", sysdba=True,
                          root=True)[0][0] != "READ WRITE" else None
    for _ in range(60):
        try:
            ora("select 1 from dual", pdb=DST, user="sys", sysdba=True)
            break
        except Exception:  # noqa: BLE001 - not registered yet
            time.sleep(2)
    # made from the seed, it has no USERS tablespace (measured, ORA-00959)
    if not ora("select count(*) from dba_tablespaces where tablespace_name"
               " = 'USERS'", pdb=DST, user="sys", sysdba=True)[0][0]:
        ora("create tablespace users datafile"
            f" '/opt/oracle/oradata/FREE/{DST}/users01.dbf' size 64m"
            " autoextend on next 64m", pdb=DST, user="sys", sysdba=True)
        ora("alter pluggable database default tablespace users", pdb=DST,
            user="sys", sysdba=True)
    for pdb in (SRC, DST):
        _drop(pdb, "drop user migkit cascade", "drop user app cascade",
              "drop user reporter cascade", "drop role migkit_r1")
        for stmt in (
                f"create user migkit identified by {PW}",
                "grant dba, logmining, select any transaction,"
                " select_catalog_role, flashback any table,"
                " create database link, datapump_imp_full_database,"
                " datapump_exp_full_database, exp_full_database to migkit",
                "grant execute on sys.dbms_logmnr to migkit",
                "grant execute on sys.dbms_flashback to migkit",
                # the password hashes: on 23.26 only an account given this
                # reads them (measured, ORA-41900 for SYSTEM; the users'
                # DDL holds them for SYS alone)
                "grant read on sys.user$ to migkit",
                "alter user migkit quota unlimited on users",
                f"create user app identified by {PW} quota unlimited on"
                " users",
                "grant create session, create table, create sequence to"
                " app"):
            ora(stmt, pdb=pdb, user="sys", sysdba=True)
    subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql",
                    "-U", "postgres", "-c", "drop database if exists shop"],
                   capture_output=True)
    subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql",
                    "-U", "postgres", "-c", "create database shop"],
                   capture_output=True)
    yield
    if not keep:
        _docker("rm", "-f", "-v", READER)
        _docker("rm", "-f", "-v", ORA)
        _docker("rm", "-f", "-v", PG)
        _docker("volume", "rm", "-f", DATA)


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    return tmp_path


def _ep(pdb, **options):
    return Endpoint(host="127.0.0.1", port=ORA_PORT, user="migkit",
                    password=PW, options={"service": pdb, **options})


def ora_hop(**options):
    return Hop(name="ora", engine="oracle",
               source=_ep(SRC, link_dsn=f"localhost:1521/{SRC}"),
               target=_ep(DST), databases=["app"], options=options)


def engine(**options):
    from migkit.engines.oracle import OracleEngine
    return OracleEngine(ora_hop(**options))


def fresh(table, ddl, pdbs=(SRC, DST), logged=True):
    """The table made again in APP on each side; its key in the redo on
    the source."""
    for pdb in (SRC, DST):
        _drop(pdb, f"drop table app.{table} purge")
    for pdb in pdbs:
        ora(f"create table app.{table} {ddl}", pdb=pdb)
    if logged and SRC in pdbs:
        ora(f"alter table app.{table} add supplemental log data (primary"
            " key) columns")


def alone(*keep):
    """Every other table of APP gone from both sides: a change feed is
    refused while any table in the hop's scope cannot be followed, and
    earlier tests leave such tables behind on purpose."""
    for pdb in (SRC, DST):
        for (t,) in ora("select table_name from all_tables where owner ="
                        " 'APP'", pdb=pdb):
            if t.lower() not in keep:
                _drop(pdb, f'drop table app."{t}" purge')


def rows(table, pdb=SRC, order="1"):
    return ora(f"select * from app.{table} order by {order}", pdb=pdb)


def columns(eng, side, table):
    from migkit import canon
    return sorted((n, canon.comparable("oracle", t)[0])
                  for n, t in eng.neutral_columns(side, "app", table))


def folded(eng, side, table, cols):
    from migkit import canon
    total, n = 0, 0
    for batch in eng.neutral_batches(side, "app", table, cols, 1000):
        k, total = canon.fold_rows([c for _, c in cols], batch, total)
        n += k
    return (n, str(total))


SALT = "0f1e2d3c4b5a6978"


def sha_folded(eng, side, table, cols, salt=SALT):
    """The Oracle-to-Oracle sum worked out here, apart from the engine's
    own fold: each row's text as the renderer writes it, SHA-256 of the
    salt and it, the first sixteen hex digits summed."""
    import hashlib

    from migkit import canon, rowtext
    total, n = 0, 0
    for batch in eng.neutral_batches(side, "app", table, cols, 1000):
        for row in batch:
            text = rowtext.encode([canon.render_value(c, v)
                                   for (_, c), v in zip(cols, row)])
            total += int(hashlib.sha256((salt + text).encode()).hexdigest()
                         [:16], 16)
            n += 1
    return (n, str(total))


class _Checkpoint(dict):
    def save(self):
        pass


# ---- the digest ------------------------------------------------------------

RENDERED = ("(id number(10) primary key, n number, d number(10,2),"
            " i number(12,0), s varchar2(40), c char(6), nv nvarchar2(20),"
            " r raw(8), dt date, ts timestamp(9), tz timestamp with time"
            " zone, ltz timestamp with local time zone)")


def _rendered_rows(pdb=SRC):
    ora("insert into app.r values (1, 0.5, 12.5, 7, unistr('C\\00e9line"
        " \\D834\\DD1E'), 'ab', unistr('na\\00efve'), hextoraw('00ff'),"
        " timestamp '2024-02-29 13:14:15', timestamp '2024-02-29"
        " 13:14:15.123456789', timestamp '2024-02-29 13:14:15.5"
        " Europe/Paris', timestamp '2024-02-29 13:14:15.5 +07:00')", pdb=pdb)
    ora("insert into app.r values (2, -0.01, -0.01, -5, 'x''y|z', 'abcdef',"
        " null, hextoraw('01'), timestamp '0999-01-02 00:00:00',"
        " timestamp '1999-12-31 23:59:59.999999', timestamp '2024-01-01"
        " 00:00:00 +00:00', null)", pdb=pdb)
    ora("insert into app.r (id, n, s) values (3, 1/3, '')", pdb=pdb)
    ora("insert into app.r (id, n, i) values (4,"
        " 123456789012345678901234567890.123, 0)", pdb=pdb)


def _renderings(eng, col):
    """[(id, the server's text, this process's text)] where they part."""
    from migkit import canon
    fact = {f["name"]: f for f in eng._facts("src", "app", "r")}[col[0]]
    expr = eng._render_sql(fact, col[1])[0]
    server = dict(ora(f"select id, {expr} from app.r"))
    here = {}
    for batch in eng.neutral_batches("src", "app", "r",
                                     [("id", "decimal"), col], 100):
        for i, v in batch:
            here[int(i)] = canon.render_value(col[1], v)
    return [(i, server.get(i), here.get(i)) for i in sorted(here)
            if server.get(i) != here.get(i)]


def test_the_server_sums_each_row_as_this_process_folds_it(sandbox):
    """Every renderable kind at once - a number of no scale (0.5, 1/3,
    thirty-three digits), one of a declared scale, text past the first
    plane, padded and national text, bytes, a DATE, nine places of a
    TIMESTAMP, a time with a named zone - summed by the server and folded
    here from the driver's values: the same number."""
    fresh("r", RENDERED, pdbs=(SRC,))
    _rendered_rows()
    eng = engine()
    cols = columns(eng, "src", "r")
    assert len(cols) == 12 and all(c for _, c in cols), cols
    got = eng.neutral_digest("src", "app", "r", cols)
    assert eng.__dict__.get("digested_in_server") == 1, eng.__dict__
    assert not eng.__dict__.get("digested_here"), eng.__dict__
    # each column alone first, so a rendering that parts is named
    for col in cols:
        if col[0] == "id":
            continue
        pair = sorted([("id", "decimal"), col])
        alone_col = eng.neutral_digest("src", "app", "r", pair)
        assert alone_col == folded(eng, "src", "r", pair), \
            (col, _renderings(eng, col))
    assert got == folded(eng, "src", "r", cols), got
    assert got[0] == 4
    # Oracle against Oracle: the salted SHA-256 sum, in the server as here
    before = eng.__dict__.get("digested_in_server", 0)
    sha = eng.same_engine_digest("src", "app", "r", cols, salt=SALT)
    assert eng.__dict__.get("digested_in_server") == before + 1, \
        eng.__dict__
    assert sha == sha_folded(eng, "src", "r", cols) and sha != got, sha
    other = eng.same_engine_digest("src", "app", "r", cols,
                                   salt="ffffffffffffffff")
    assert other[0] == 4 and other != sha, other
    # and one character different is a different number, both ways
    ora("update app.r set s = unistr('Celine \\D834\\DD1E') where id = 1")
    again = eng.neutral_digest("src", "app", "r", cols)
    assert again != got and again[0] == 4, (again, got)
    assert again == folded(eng, "src", "r", cols)
    moved = eng.same_engine_digest("src", "app", "r", cols, salt=SALT)
    assert moved != sha and moved == sha_folded(eng, "src", "r", cols)


def test_two_copies_of_a_row_add_they_do_not_cancel(sandbox):
    """Oracle's CHECKSUM folds by exclusive or: two copies of one row
    cancel, and two copies of another read the same. A sum does not."""
    fresh("dup", "(k number, v varchar2(10))", logged=False)
    ora("insert into app.dup values (1, 'x')")
    ora("insert into app.dup values (1, 'x')")
    ora("insert into app.dup values (2, 'y')", pdb=DST)
    ora("insert into app.dup values (2, 'y')", pdb=DST)
    eng = engine()
    cols = columns(eng, "src", "dup")
    a = eng.neutral_digest("src", "app", "dup", cols)
    b = eng.neutral_digest("dst", "app", "dup", cols)
    assert a[0] == b[0] == 2 and a != b, (a, b)
    a = eng.same_engine_digest("src", "app", "dup", cols, salt=SALT)
    b = eng.same_engine_digest("dst", "app", "dup", cols, salt=SALT)
    assert a[0] == b[0] == 2 and a != b, (a, b)
    # two copies are twice one: a sum, not an exclusive or
    one = sha_folded(eng, "src", "dup", cols)
    assert int(a[1]) == int(one[1]) and int(a[1]) % 2 == 0, (a, one)
    xor = ora("select checksum(k || v) from app.dup")[0][0]
    xor_dst = ora("select checksum(k || v) from app.dup", pdb=DST)[0][0]
    # measured: both sides the same to CHECKSUM, which is why it is not used
    assert xor == xor_dst, (xor, xor_dst)


def test_a_value_past_what_the_server_renders_is_folded_here(sandbox):
    """A number of no scale past sixty-two digits is written by Oracle in
    an exponent; that table is folded here, and still to the right
    number - the one the renderer makes of the value itself."""
    from migkit import canon
    fresh("wide", "(id number(10) primary key, n number)", pdbs=(SRC,))
    ora("insert into app.wide values (1, 1e100)")
    eng = engine()
    cols = columns(eng, "src", "wide")
    got = eng.neutral_digest("src", "app", "wide", cols)
    assert eng.__dict__.get("digested_here") == 1, eng.__dict__
    want = canon.fold_rows([c for _, c in cols],
                           [[1, Decimal("1E+100")]])
    assert got == (1, str(want[1])), (got, want)


# ---- writing -----------------------------------------------------------------

def test_large_values_are_written_and_read_in_pieces(sandbox, monkeypatch):
    from migkit.engines import oracle
    monkeypatch.setattr(oracle, "PIECE", 70000)
    fresh("lob", "(id number(10) primary key, b blob, c clob)",
          pdbs=(DST,))
    eng = engine()
    pieces = {"in": 0, "out": 0}
    wrote, read = oracle.OracleEngine._insert_in_pieces, \
        oracle.OracleEngine._read_in_pieces

    def counted_in(self, *a):
        pieces["in"] += 1
        return wrote(self, *a)

    def counted_out(self, *a):
        pieces["out"] += 1
        return read(self, *a)
    monkeypatch.setattr(oracle.OracleEngine, "_insert_in_pieces", counted_in)
    monkeypatch.setattr(oracle.OracleEngine, "_read_in_pieces", counted_out)
    big = bytes(range(256)) * 1200
    text = ("a" * 8059 + "\U0001D11E" + "é") * 20
    cols = [("b", "bytes"), ("c", "text"), ("id", "decimal")]
    data = [[big, text, 1], [b"\x01", "small", 2], [None, None, 3]]
    eng.neutral_write("dst", "app", "lob", cols, data)
    assert pieces["in"] == 1, pieces
    got, _ = eng.neutral_read("dst", "app", "lob", cols, None, 10)
    assert pieces["out"] == 2, pieces
    assert [r[:2] for r in got] == [[big, text], [b"\x01", "small"],
                                    [None, None]]


def test_a_batch_goes_the_cheaper_way_and_a_zone_is_kept(sandbox):
    """The first large batch by direct path load into the session's stage
    and a MERGE, the second by array DML, the rest the cheaper of the two
    by the row; the same rows either way, the stage gone after, and a
    time written in its zone read back in it."""
    from migkit import nanotime
    from migkit.engines.oracle import Zoned
    fresh("wb", "(id number(10) primary key, a varchar2(100), n number(12,2),"
          " tz timestamp(9) with time zone, t9 timestamp(9))", pdbs=(DST,))
    eng = engine()
    cols = [("a", "text"), ("id", "decimal"), ("n", "decimal"),
            ("t9", "timestamp"), ("tz", "timestamp")]
    paris = Zoned.at(nanotime.of(datetime.datetime(2024, 5, 1, 8, 0, 0, 1),
                                 234), "EUROPE/PARIS")

    def batch(lo):
        return [[f"row {i}", i, (Decimal(i) / 4).quantize(Decimal("0.01")),
                 nanotime.of(datetime.datetime(2024, 1, 1, 0, 0, 0, i), 9),
                 paris if i % 2 else datetime.datetime(
                     2024, 1, 1, 12, tzinfo=datetime.timezone.utc)]
                for i in range(lo, lo + 2500)]
    with eng.load_window("app"):
        for lo in (0, 2500, 5000):
            eng.neutral_write("dst", "app", "wb", cols, batch(lo))
        # the first batch again, the fast way: replaced, not doubled
        key = eng._write_key("app", "wb", 2500)
        eng._took(key, "direct", 0.0, 2500)
        eng._took(key, "array", 1.0, 2500)
        eng.neutral_write("dst", "app", "wb", cols, batch(0))
    costs = eng.__dict__["_way_cost"][key]
    assert set(costs) == {"direct", "array"}, costs
    # nine digits of a second kept, both ways
    assert ora("select to_char(t9, 'FF9') from app.wb where id in (1, 2501)"
               " order by id", pdb=DST) == [("000001009",), ("002501009",)]
    assert ora("select to_char(tz, 'FF9') from app.wb where id = 1",
               pdb=DST) == [("000001234",)]
    assert ora("select count(*), count(distinct id) from app.wb",
               pdb=DST) == [(7500, 7500)]
    assert ora("select to_char(tz, 'TZR'), to_char(sys_extract_utc(tz),"
               " 'HH24:MI') from app.wb where id = 1", pdb=DST) == \
        [("EUROPE/PARIS", "08:00")]
    assert ora("select count(*) from all_tables where owner = 'APP' and"
               " table_name like 'MIGKIT$%'", pdb=DST) == [(0,)]
    from migkit import canon
    want = canon.fold_rows([c for _, c in cols],
                           batch(0) + batch(2500) + batch(5000))
    assert eng.neutral_digest("dst", "app", "wb", cols) == \
        (7500, str(want[1]))


# ---- sequences, settings, users, the restore point ----------------------------

def test_sequences_and_identity_are_carried_and_put_back(sandbox, tmp_path,
                                                       reports):
    from migkit.engines.base import RepairAction
    for pdb in (SRC, DST):
        _drop(pdb, "drop sequence app.s1")
    ora("create sequence app.s1 start with 1000")
    ora("create sequence app.s1 start with 1", pdb=DST)
    fresh("it", "(id number generated by default as identity primary"
          " key, v varchar2(10))")
    for pdb in (SRC, DST):
        ora("insert into app.it (v) values ('a')", pdb=pdb)
        ora("insert into app.it (id, v) values (100, 'b')", pdb=pdb)
    eng = engine()
    got = {r.scope: r.status for r in eng.check_autoinc("app")}
    assert got == {"app usable": "diff", "app parity": "diff"}, got
    eng.snapshot_state("app", tmp_path, "sequences")
    for a in eng.repair_plan("app", "sequences"):
        eng.apply("app", a)
    got = {r.scope: r.status for r in eng.check_autoinc("app")}
    assert got == {"app usable": "ok", "app parity": "ok"}, got
    ora("insert into app.it (v) values ('c')", pdb=DST)
    assert ora("select max(id) from app.it", pdb=DST)[0][0] > 100
    back = eng.restore_sequences("app", tmp_path)
    assert any("S1" in s and "start with 1" in s for s in back), back
    eng.apply("app", RepairAction("app", "sequences", back, [], "back"))
    got = {r.scope: r.status for r in eng.check_autoinc("app")}
    assert got["app parity"] == "diff", got
    assert (tmp_path / "dst-scn.txt").read_text().strip().isdigit()


def test_the_targets_restore_point_puts_its_rows_back(sandbox, tmp_path,
                                                      reports):
    """The snapshot records the SCN after letting each table move its
    rows, so the statements it leaves put a table back as it was."""
    fresh("snap", "(id number(10) primary key, s varchar2(10))",
          pdbs=(DST,))
    ora("insert into app.snap values (1, 'before')", pdb=DST)
    eng = engine()
    eng.snapshot_state("app", tmp_path, "all")
    ora("update app.snap set s = 'after' where id = 1", pdb=DST)
    ora("insert into app.snap values (2, 'new')", pdb=DST)
    back = [line.rstrip(";") for line in (tmp_path / "dst-flashback.sql"
                                          ).read_text().splitlines()
            if '"SNAP"' in line]
    assert back, (tmp_path / "dst-flashback.sql").read_text()
    ora(back[0], pdb=DST)
    assert rows("snap", DST) == [(1, "before")]
    assert "SNAP" in (tmp_path / "dst-row-movement.txt").read_text()


def test_settings_are_compared_and_a_difference_said(sandbox, reports,
                                                     monkeypatch):
    eng = engine()
    got = eng.check_params("app")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    real = eng._settings

    def other(side, db):
        out = real(side, db)
        if side == "dst":
            out["nls_length_semantics"] = "CHAR"
        return out
    monkeypatch.setattr(eng, "_settings", other)
    got = eng.check_params("app")
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    assert "nls_length_semantics" in got[0].detail


def test_users_roles_and_grants_are_carried_and_taken_back(sandbox,
                                                          reports):
    from migkit import users
    fresh("t_users", "(id number primary key)")
    for stmt in ("create role migkit_r1",
                 f"create user reporter identified by {PW}",
                 "grant create session, migkit_r1 to reporter",
                 "grant select on app.t_users to reporter",
                 "alter user reporter quota 10M on users"):
        ora(stmt, user="sys", sysdba=True)
    hop = ora_hop()
    hop.engine = "oracle"
    said = []
    out, _ = users.compare(hop, said.append)
    assert {"user:REPORTER", "role:MIGKIT_R1"} <= set(
        out["missing_on_target"]), out
    assert out["result"] == "gap"
    # the accounts the sandbox made on each side by hand: the same
    # password, each hashed with a salt of its own, so a hash cannot say
    # they agree - said as differing, never as the same
    made_here = {"user:MIGKIT", "user:APP"}
    assert "user:REPORTER" not in out["password_differs"]
    assert made_here <= set(out["password_differs"]), out
    # the sandbox's own accounts keep passwords that differ, and create
    # ends by saying so
    with pytest.raises(SystemExit, match="still differs"):
        users.create(hop, apply=True, say=said.append)
    out, s = users.compare(hop, said.append)
    assert not {"user:REPORTER", "role:MIGKIT_R1"} & set(
        out["missing_on_target"] + out["password_differs"]
        + out["grants_missing"]), (out, said)
    assert "user:REPORTER" not in out["password_differs"], out
    assert s["user:REPORTER"]["hash"], "the password hash was not read"
    # the same password, carried as its hash: it signs in on the target
    ora("select 1 from dual", pdb=DST, user="reporter")
    users.rollback(hop, apply=True, say=said.append)
    out, _ = users.compare(hop, said.append)
    assert {"user:REPORTER", "role:MIGKIT_R1"} <= set(
        out["missing_on_target"]), out


# ---- Oracle to Oracle ---------------------------------------------------------

def _orders():
    alone("orders", "notes")
    fresh("orders", "(id number(10) primary key, who varchar2(30),"
          " amount number(12,2), at timestamp with time zone)", pdbs=(SRC,))
    ora("insert into app.orders select level, 'c' || level, level / 8,"
        " timestamp '2024-03-01 10:00:00 Asia/Bangkok' + numtodsinterval("
        "level, 'MINUTE') from dual connect by level <= 3000")
    fresh("notes", "(k number, v varchar2(20))", pdbs=(SRC,), logged=False)
    ora("insert into app.notes values (1, 'same')")
    ora("insert into app.notes values (1, 'same')")


def test_oracle_to_oracle_is_pulled_by_the_targets_data_pump(sandbox,
                                                             reports):
    from migkit.engines import oracle_pump
    _orders()
    eng = engine()
    assert oracle_pump.why_not(eng, "app") is None
    said = []
    steps = eng.native_bulk("app", ["orders", "notes"], True, said.append)
    assert all("Data Pump" in s for s in steps), steps
    assert ora("select count(*) from app.orders", pdb=DST) == [(3000,)]
    assert ora("select count(*) from app.notes", pdb=DST) == [(2,)]
    assert ora("select to_char(at, 'TZR') from app.orders where id = 1",
               pdb=DST) == [("ASIA/BANGKOK",)]
    assert json.loads((reports / "ora" / "app" / "dump-position.json"
                       ).read_text())["scn"] > 0
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]
    ora("update app.orders set amount = amount + 0.01 where id = 7",
        pdb=DST)
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert any(r.status == "diff" for r in data), [r.detail for r in data]


def test_where_data_pump_cannot_run_the_copier_carries_it(sandbox, reports,
                                                          monkeypatch):
    from migkit.engines import oracle_pump
    _orders()
    monkeypatch.setattr(oracle_pump, "why_not",
                        lambda eng, db: "the target has no Data Pump")
    eng = engine()
    steps = eng.native_bulk("app", ["orders"], True, lambda m: None)
    assert steps == ["orders: copied table by table (the target has no"
                     " Data Pump)"], steps
    assert ora("select count(*) from app.orders", pdb=DST) == [(3000,)]
    data = [r for r in eng.check_data("app", "orders") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]


# ---- the redo, followed ---------------------------------------------------------

def _tail(eng, token_path, said):
    ended = {}

    def run():
        try:
            eng.tail_apply("app", True, token_path, said.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt too
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _at(eng, db, ended=None):
    """The source's position for a fence, once the tail runs: before, the
    pair has none to give."""
    assert _until(lambda: eng.src_lsn(db) is not None or bool(ended), 90) \
        and not ended, ("the tail never said it runs", _why(ended or {}))
    return eng.src_lsn(db)


def _why(ended):
    return {k: f"{type(v).__name__}: {v}" for k, v in ended.items()}


def _stop(thread):
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=60)


def _same(table, order="id", select="*"):
    """Both sides' rows, a time with a zone as text: the driver cannot
    fetch one whose zone is a region."""
    return [ora(f"select {select} from app.{table} order by {order}",
                pdb=pdb) for pdb in (SRC, DST)]


EV = ("id, s, n, c, to_char(at, 'YYYY-MM-DD HH24:MI:SS.FF TZR')")


def _until(check, seconds=120):
    end = time.time() + seconds
    while time.time() < end:
        if check():
            return True
        time.sleep(1)
    return check()


def test_the_redo_is_followed_whole_and_fenced(sandbox, reports,
                                               monkeypatch):
    """Inserts, updates of some columns, a key moved, deletes, a large
    value, a time in its zone, a transaction rolled back, one committed
    without waiting for its redo, one larger than the reader holds - all
    on the target by the time the fence passes, the rolled-back one never.
    A tail stopped and started again goes on from its position: nothing
    twice, nothing missed."""
    from migkit.engines import oracle, oracle_logminer
    monkeypatch.setattr(oracle, "PIECE", 70000)
    monkeypatch.setattr(oracle_logminer.Miner, "LARGEST_HELD", 50)
    ddl = ("(id number(10) primary key, s varchar2(40), n number(10,2),"
           " c clob, at timestamp with time zone)")
    alone("ev")
    fresh("ev", ddl)
    eng = engine()
    token = reports / "ora" / "app" / "tail-token.json"
    eng.tail_start("app", token)
    said = []
    thread, ended = _tail(eng, token, said)
    try:
        ora("insert into app.ev values (1, 'one', 1.5, 'short',"
            " timestamp '2024-01-01 09:00:00 Asia/Bangkok')")
        ora("insert into app.ev (id, s) values (2, 'two')")
        conn = ora_conn()
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("insert into app.ev (id, s) values (3, 'gone')")
        conn.rollback()
        cur.execute("update app.ev set n = 9.25 where id = 1")
        cur.execute("update app.ev set id = 20 where id = 2")
        cur.execute("insert into app.ev (id, s, c) values (4, 'big', :1)",
                    ["x" * 150000 + "\U0001D11E"])
        conn.commit()
        cur.execute("insert into app.ev (id, s) values (5, 'nowait')")
        cur.execute("commit write nowait")
        cur.executemany("insert into app.ev (id, s, n) values (:1, :2, :3)",
                        [(100 + i, f"bulk {i}", i) for i in range(120)])
        conn.commit()
        cur.execute("delete from app.ev where id between 100 and 109")
        conn.commit()
        conn.close()
        at = _at(eng, "app", ended)
        assert at is not None
        assert eng.fence_wait("app", at, timeout=180) is True, \
            (said[-5:], _why(ended))
        a, b = _same("ev", select=EV)
        assert a == b, (a, b)
        assert not ora("select * from app.ev where id = 3", pdb=DST)
        assert ora("select to_char(at, 'TZR') from app.ev where id = 1",
                   pdb=DST) == [("ASIA/BANGKOK",)]
    finally:
        _stop(thread)
    assert "e" not in ended or isinstance(ended["e"], KeyboardInterrupt), \
        ended
    ora("update app.ev set s = 'while stopped' where id = 1")
    ora("delete from app.ev where id = 110")
    thread, ended = _tail(eng, token, said)
    try:
        at = _at(eng, "app", ended)
        assert eng.fence_wait("app", at, timeout=180) is True, \
            (said[-5:], _why(ended))
        a, b = _same("ev", select=EV)
        assert a == b and len(a) == 113, (len(a), len(b))
        data = [r for r in eng.check_data("app", "ev") if r.check == "data"]
        assert data and all(r.status == "ok" for r in data), \
            [r.detail for r in data]
    finally:
        _stop(thread)


def test_a_burst_is_followed_and_its_rate_said(sandbox, reports, capsys):
    """Forty thousand rows in one transaction and forty thousand more in
    four hundred: on the target, every one, by the time the fence passes;
    the rate the reader and the applier kept is printed for the docstrings
    that quote it."""
    alone("burst")
    fresh("burst", "(id number(10) primary key, s varchar2(60),"
          " n number(12,2), at date)")
    eng = engine()
    token = reports / "ora" / "app" / "tail-token.json"
    eng.tail_start("app", token)
    said = []
    thread, ended = _tail(eng, token, said)
    try:
        began = time.monotonic()
        ora("insert into app.burst select level, rpad('x', 50, 'y'),"
            " level / 4, sysdate from dual connect by level <= 40000")
        conn = ora_conn()
        conn.autocommit = False
        cur = conn.cursor()
        for lo in range(40000, 80000, 100):
            cur.execute("insert into app.burst select level + :1, 'z',"
                        " level, sysdate from dual connect by level <= 100",
                        [lo])
            conn.commit()
        conn.close()
        written = time.monotonic() - began
        at = _at(eng, "app", ended)
        assert eng.fence_wait("app", at, timeout=600) is True, \
            (said[-5:], _why(ended))
        took = time.monotonic() - began
    finally:
        _stop(thread)
    assert ora("select count(*) from app.burst", pdb=DST) == [(80000,)]
    data = [r for r in eng.check_data("app", "burst") if r.check == "data"]
    assert [r.status for r in data] == ["ok"], [r.detail for r in data]
    with capsys.disabled():
        print(f"\nburst: 80,000 rows written in {written:.1f}s, on the"
              f" target {took:.1f}s after the first ({80000 / took:,.0f}"
              " rows a second end to end)")


def test_a_difference_still_arriving_is_confirmed_not_called_wrong(
        sandbox, reports):
    from migkit import tailctl
    alone("cf")
    fresh("cf", "(id number(10) primary key, s varchar2(20))")
    eng = engine()
    token = reports / "ora" / "app" / "tail-token.json"
    eng.tail_start("app", token)
    said = []
    thread, _ = _tail(eng, token, said)
    try:
        ora("insert into app.cf values (1, 'a')")
        assert _until(lambda: _same("cf")[0] == _same("cf")[1])
        where = reports / "ora" / "app"
        assert tailctl.pause(where, 30)
        ora("update app.cf set s = 'b' where id = 1")
        ora("insert into app.cf values (2, 'c')")
        threading.Timer(4, tailctl.resume, (where,)).start()
        data = [r for r in eng.check_data("app", "cf") if r.check == "data"]
        assert [r.status for r in data] == ["ok"], [r.detail for r in data]
    finally:
        _stop(thread)
    # and a difference that is not arriving is still one
    ora("update app.cf set s = 'wrong' where id = 2", pdb=DST)
    data = [r for r in eng.check_data("app", "cf") if r.check == "data"]
    assert any(r.status == "diff" for r in data), [r.detail for r in data]


def test_only_what_changed_is_verified(sandbox, reports):
    alone("dv")
    fresh("dv", "(id number(10) primary key, s varchar2(20))")
    for pdb in (SRC, DST):
        ora("insert into app.dv select level, 'v' || level from dual"
            " connect by level <= 50", pdb=pdb)
    eng = engine()
    first = eng.delta_verify("app")
    assert first[0].status == "ok" and "baseline" in first[0].detail
    ora("update app.dv set s = 'new' where id = 3")
    ora("update app.dv set s = 'new' where id = 3", pdb=DST)
    got = eng.delta_verify("app")
    assert got[0].status == "ok", [r.detail for r in got]
    assert any("1 changed rows verified equal" in r.detail for r in got), \
        [r.detail for r in got]
    ora("update app.dv set s = 'newer' where id = 4")
    got = eng.delta_verify("app")
    assert got[0].status == "diff", [r.detail for r in got]


def test_a_position_the_redo_no_longer_holds_is_said(sandbox):
    alone()
    eng = engine()
    said = eng.position_lost("src", "app", {"scn": 1})
    assert said and said.startswith("the redo no longer reaches SCN 1"), said
    assert eng.position_lost("src", "app", eng.change_point("src",
                                                            "app")) is None


def test_a_table_whose_key_is_not_in_the_redo_is_refused(sandbox, reports):
    alone("nolog")
    fresh("nolog", "(id number(10) primary key, s varchar2(10))",
          logged=False)
    eng = engine()
    with pytest.raises(SystemExit) as e:
        eng.change_point("src", "app")
    assert "NOLOG" in str(e.value).upper() and "supplemental log data" \
        in str(e.value), e.value
    _drop(SRC, "drop table app.nolog purge")


def test_assess_measures_the_reader_against_the_redo(sandbox, capsys):
    alone()
    ora("alter system archive log current", user="sys", sysdba=True,
        root=True)
    eng = engine()
    items = {i["item"]: i for i in eng.assess()}
    rate = items.get("change feed from the source: reader rate")
    assert rate and rate["level"] in ("pass", "warn", "fail"), items
    assert "MB of redo a second" in rate["detail"], rate
    archived = items["change feed from the source: redo archived"]
    assert archived["level"] == "pass", archived
    with capsys.disabled():
        print(f"\nreader rate: {rate['level']}: {rate['detail']}")


# ---- with PostgreSQL -------------------------------------------------------------

def _pair_hop(src, dst):
    pgep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    if src == "oracle":
        # a time written in a named zone keeps its instant on PostgreSQL
        # and not its zone, which the hop accepts (`commercial`)
        return Hop(name="o2p", engine="hetero", source=_ep(SRC),
                   target=pgep, databases=["app"], db_map={"app": "shop"},
                   options={"source_engine": "oracle",
                            "target_engine": "postgres",
                            "accept_changes": ["cust.tz"]})
    return Hop(name="p2o", engine="hetero", source=pgep, target=_ep(DST),
               databases=["shop"], db_map={"shop": "app"},
               options={"source_engine": "postgres",
                        "target_engine": "oracle"})


def test_oracle_to_postgresql_is_moved_checked_and_followed(sandbox,
                                                           reports):
    from migkit.engines.hetero import HeteroEngine
    alone("cust")
    fresh("cust", "(id number(10) primary key, name varchar2(50),"
          " amount number(12,2), at timestamp(6), tz timestamp with time"
          " zone, bin raw(8), note clob)", pdbs=(SRC,))
    ora("insert into app.cust select level, unistr('n\\00e9 ') || level,"
        " level / 4, timestamp '2024-01-01 00:00:00' + numtodsinterval("
        "level, 'SECOND'), timestamp '2024-06-01 12:00:00 Europe/Paris',"
        " hextoraw('0A0B'), 'note ' || level from dual connect by level <="
        " 500")
    pg("drop table if exists cust")
    eng = HeteroEngine(_pair_hop("oracle", "postgres"))
    eng.move_table("app", "", "cust", 200, _Checkpoint(), lambda m: None)
    assert pg("select count(*) from cust") == "500"
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]
    token = reports / "o2p" / "app" / "tail-token.json"
    eng.tail_start("app", token)
    said = []
    thread, ended = _tail(eng, token, said)
    try:
        ora("update app.cust set amount = 99.99, note = 'changed' where"
            " id = 5")
        ora("delete from app.cust where id = 6")
        ora("insert into app.cust (id, name, tz) values (1000, 'new',"
            " timestamp '2024-06-01 12:00:00 +07:00')")
        at = _at(eng, "app", ended)
        assert eng.fence_wait("app", at, timeout=180) is True, \
            (said[-5:], _why(ended))
    finally:
        _stop(thread)
    assert pg("select amount, note from cust where id = 5") == \
        "99.99|changed"
    assert pg("select count(*) from cust where id = 6") == "0"
    assert pg("select extract(epoch from tz)::bigint from cust where id ="
              " 1000") == str(int(datetime.datetime(
                  2024, 6, 1, 5, tzinfo=datetime.timezone.utc).timestamp()))
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]
    pg("update cust set name = 'drifted' where id = 7")
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert any(r.status == "diff" for r in data), [r.detail for r in data]


def test_postgresql_to_oracle_is_moved_checked_and_followed(sandbox,
                                                           reports):
    from migkit.engines.hetero import HeteroEngine
    alone()
    pg("drop table if exists cust")
    pg("drop table if exists acct")
    pg("create table acct (id bigint primary key, name text,"
       " amount numeric(12,2), at timestamptz, b bytea)")
    pg("insert into acct select g, 'a' || g, g / 3.0, '2024-01-01"
       " 00:00:00+07'::timestamptz + g * interval '1 minute',"
       " decode('00ff', 'hex') from generate_series(1, 3000) g")
    _drop(DST, "drop table app.acct purge")
    eng = HeteroEngine(_pair_hop("postgres", "oracle"))
    with eng.dst_engine.load_window("app"):
        eng.move_table("shop", "public", "acct", 5000, _Checkpoint(),
                       lambda m: None)
    assert ora("select count(*) from app.acct", pdb=DST) == [(3000,)]
    data = [r for r in eng.check_data("shop") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]
    token = reports / "p2o" / "shop" / "tail-token.json"
    eng.tail_start("shop", token)
    said = []
    thread, ended = _tail_db(eng, token, said, "shop")
    try:
        pg("update acct set amount = 1.25, name = 'moved' where id = 3")
        pg("delete from acct where id = 4")
        pg("insert into acct values (9000, 'late', 7.5, now(),"
           " decode('ff', 'hex'))")
        at = _at(eng, "shop", ended)
        assert eng.fence_wait("shop", at, timeout=180) is True, \
            (said[-5:], _why(ended))
    finally:
        _stop(thread)
    assert ora("select name, amount from app.acct where id = 3",
               pdb=DST) == [("moved", Decimal("1.25"))]
    assert ora("select count(*) from app.acct where id in (4, 9000)",
               pdb=DST) == [(1,)]
    data = [r for r in eng.check_data("shop") if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), (
        [r.detail for r in data],
        pg("select id, name, amount, at::text, encode(b, 'hex') from acct"
           " where id = 9000"),
        ora("select id, name, amount, to_char(at, 'YYYY-MM-DD"
            " HH24:MI:SS.FF6'), rawtohex(dbms_lob.substr(b, 2000, 1)) from"
            " app.acct where id = 9000",
            pdb=DST))
    ora("update app.acct set name = 'drifted' where id = 8", pdb=DST)
    data = [r for r in eng.check_data("shop") if r.check == "data"]
    assert any(r.status == "diff" for r in data), [r.detail for r in data]


def _tail_db(eng, token_path, said, db):
    ended = {}

    def run():
        try:
            eng.tail_apply(db, True, token_path, said.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt too
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


# ---- the redo-file reader, run beside the redo ------------------------------

READER_IMAGE = "migkit-test-ora-redo:2.0.0"
BUILD_LOG = pathlib.Path(tempfile.gettempdir()) / "migkit-redo-reader-build.log"


def _reader_image():
    """The redo-file reader, built once by the recipe `doctor --install`
    runs, in a Linux container of this machine's own architecture."""
    if _docker("image", "inspect", READER_IMAGE).returncode == 0:
        return True
    import platform

    from migkit import tools
    build = tools.REDO_READER_BUILD
    name = f"{READER}-build"
    _docker("rm", "-f", name)
    # every file it is built from fetched as `doctor --install` fetches it,
    # held to the lock (a file no vendor publishes a digest of, to the one
    # it had the first time: kept in a place of the test's own), and handed
    # to the build, which fetches nothing itself
    files = pathlib.Path(tempfile.mkdtemp(prefix="migkit-redo-files-"))
    arch = ("arm64" if platform.machine().lower() in ("arm64", "aarch64")
            else "x64")
    before = os.environ.get("MIGKIT_TOOLS")
    os.environ["MIGKIT_TOOLS"] = str(files / "tools")
    try:
        for local, url in tools.redo_reader_files(arch).items():
            assert tools.fetch(url, str(files / local), print), url
    finally:
        if before is None:
            os.environ.pop("MIGKIT_TOOLS", None)
        else:
            os.environ["MIGKIT_TOOLS"] = before
    subprocess.run(
        ["docker", "create", "--name", name, "debian:12", "bash", "-c",
         tools.REDO_READER_SCRIPT, "build", "/opt/redo-reader", "/tmp/build",
         build["version"]], check=True, capture_output=True)
    subprocess.run(["docker", "cp", f"{files}/.", f"{name}:/tmp/build"],
                   check=True, capture_output=True)
    made = subprocess.run(["docker", "start", "-a", name],
                          capture_output=True, text=True, timeout=3600)
    BUILD_LOG.write_text(
        made.stdout[-20000:] + made.stderr[-20000:])
    if made.returncode != 0:
        _docker("rm", "-f", name)
        return False
    _docker("commit", name, READER_IMAGE)
    _docker("rm", "-f", name)
    return True


def test_the_redo_files_are_read_beside_the_redo(sandbox, reports):
    """The rung above LogMiner: the redo-file reader, built by the recipe
    `doctor --install` runs and started beside the database with the
    configuration migkit writes for it, reads the redo files themselves;
    migkit speaks its protocol, reads its stream back as changes, confirms
    what it applied, and the target follows as it does through LogMiner.
    The program is published for Linux on x86-64; this sandbox runs it
    natively on Linux aarch64, beside Oracle Free for aarch64."""
    from migkit.engines import oracle_redo
    assert _reader_image(), BUILD_LOG.read_text()[-3000:]
    alone("rf")
    fresh("rf", "(id number(10) primary key, s varchar2(40),"
          " n number(10,2))")
    hop = ora_hop()
    hop.source.options.update(redo_reader=f"127.0.0.1:{READER_PORT}",
                              redo_reader_dsn=f"localhost:1521/{SRC}")
    from migkit import dba
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(hop)
    assert oracle_redo.chosen(eng, "src", "app") == oracle_redo.RUNG
    # what the DBA is handed for it: missing, then granted as written
    need = dba._ora_redo_files(eng, "src", "app", '"MIGKIT"')
    assert need.ok is False and need.fix, need
    for stmt in need.fix:
        if not stmt.startswith("--"):
            ora(stmt.rstrip(";"), user="sys", sysdba=True)
    # and each table XML's own list names, as the comment says
    for (suffix,) in ora("select toksuf from xdb.xdb$ttset", user="sys",
                         sysdba=True):
        for part in ("NM", "QN", "PT"):
            ora(f"grant select, flashback on xdb.x${part}{suffix} to"
                " migkit", user="sys", sysdba=True)
    assert dba._ora_redo_files(eng, "src", "app", '"MIGKIT"').ok is True
    conf = reports / "reader"
    conf.mkdir()
    (conf / "OpenLogReplicator.json").write_text(json.dumps(
        oracle_redo.config(eng, "src", "app", "0.0.0.0:9000",
                           "/opt/reader/checkpoint")))
    _docker("rm", "-f", READER)
    made = _docker("create", "--name", READER, "--user", "0",
                   "--network", f"container:{ORA}",
                   "-v", f"{DATA}:/opt/oracle/oradata:ro",
                   "-w", "/opt/reader", READER_IMAGE,
                   "sh", "-c", "mkdir -p /opt/reader/checkpoint && exec"
                   " /opt/redo-reader/bin/OpenLogReplicator -f"
                   " /opt/reader/OpenLogReplicator.json")
    assert made.returncode == 0, made.stderr
    assert _docker("cp", str(conf / "OpenLogReplicator.json"),
                   f"{READER}:/opt/reader/").returncode == 0
    assert _docker("start", READER).returncode == 0
    token = reports / "ora" / "app" / "tail-token.json"
    said = []
    try:
        eng.tail_start("app", token)
        thread, ended = _tail(eng, token, said)
        try:
            ora("insert into app.rf values (1, 'one', 1.5)")
            ora("insert into app.rf values (2, 'two', 2.5)")
            ora("update app.rf set s = 'uno', n = null where id = 1")
            ora("delete from app.rf where id = 2")
            ora("insert into app.rf values (3, unistr('tr\\00e8s ?'), 3)")
            at = _at(eng, "app", ended)
            passed = eng.fence_wait("app", at, timeout=240)
            logs = _docker("logs", READER)
            assert passed is True, (said, logs.stdout[-2000:],
                                    logs.stderr[-2000:])
        finally:
            _stop(thread)
        assert rows("rf", DST, "id") == rows("rf", SRC, "id") == \
            [(1, "uno", None), (3, "trè" + "s ?", 3)], rows("rf", DST)
        # the tail read through it: the writer streamed to a client
        logs = _docker("logs", READER)
        assert "streaming to client" in logs.stdout + logs.stderr, \
            (logs.stdout + logs.stderr)[-2000:]
    finally:
        _docker("rm", "-f", READER)


def test_the_dba_is_handed_what_following_needs(sandbox, reports):
    """An account that owns the schema and nothing more: what following
    it needs is written as the statements that grant it, and once run as
    written, asked again it is there."""
    from migkit import dba
    from migkit.engines.oracle import OracleEngine
    alone("dbat")
    fresh("dbat", "(id number(10) primary key)", logged=False)
    hop = Hop(name="ora", engine="oracle",
              source=Endpoint(host="127.0.0.1", port=ORA_PORT, user="app",
                              password=PW, options={"service": SRC}),
              target=_ep(DST), databases=["app"])
    eng = OracleEngine(hop)

    def lacking():
        return {n.what: n for n in dba.missing(dba.needs(hop, eng,
                                                         "cutover"))
                if n.side == "source"}
    lack = lacking()
    assert "read the redo back" in lack, lack
    assert 'grant logmining to "APP";' in lack["read the redo back"].fix
    logs = lack["every table of APP logs its key"]
    assert logs.fix == ['alter table "APP"."DBAT" add supplemental log data'
                        ' (primary key) columns;'], logs.fix
    assert "the redo archived" not in lack and \
        "minimal supplemental logging" not in lack, lack
    for n in lack.values():
        for stmt in n.fix:
            ora(stmt.rstrip(";"), user="sys", sysdba=True)
    assert not {"read the redo back", "every table of APP logs its key"} \
        & set(lacking()), lacking()
    script = dba.script(hop, eng, "source", dba.needs(hop, eng, "cutover"))
    assert script is None or "logmining" not in script


def test_the_target_says_which_server_and_schema_it_is(sandbox):
    """What the interlock is told: the two pluggable databases share the
    instance, the address and the container database's id, never the
    service or the container's name, so the target is not taken for the
    source; a schema not made yet is said to be absent, one there by its
    bytes."""
    from migkit import interlock
    eng = engine()
    src = eng.server_identity("src", "app")
    dst = eng.server_identity("dst", "app")
    assert src["id"]["container"] == SRC.upper(), src
    assert dst["id"]["container"] == DST.upper(), dst
    assert src["id"]["database"] == dst["id"]["database"], (src, dst)
    assert src["running"] == dst["running"] and src["version"], src
    assert "absent" not in dst and dst["bytes"] >= 0, dst
    assert eng.server_identity("dst", "nobody_here").get("absent") is True
    # one address for both: the service tells them apart
    hop = ora_hop()
    s = interlock.identity(hop, eng, "src", "app")
    d = interlock.identity(hop, eng, "dst", "app")
    assert (s["database"], d["database"]) == (f"{SRC}/APP", f"{DST}/APP")
    assert s["address"] == d["address"], (s, d)
    assert interlock.itself(s, d) is None, (s, d)
    assert interlock.itself(s, s) is not None
