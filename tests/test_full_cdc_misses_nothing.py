"""A copy followed by a tail misses nothing the source changed during the copy.

`move --mode full+cdc` on a cross-engine hop copied the tables and then
started the tail from wherever the change log was once the copy had
finished. A change made while the rows were being read was in neither: the
copy had read its table before it, and the tail began after it. Nothing
reported it - `check` would find the row later, if anyone ran it before the
cutover. The position is now taken before the copy, on every source with a
change log, and a copy an earlier run left behind with no position saved is
copied again rather than trusted.
"""
import json
import socket
import subprocess
import time

import pytest

MY, PG = "migkit-test-fcdc-my", "migkit-test-fcdc-pg"
MY_PORT, PG_PORT = 15672, 15673


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def docker(test):
    test = pytest.mark.skipif(not _docker(),
                              reason="docker not available")(test)
    return pytest.mark.docker(test)


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def pg(sql, db="cx"):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def my(sql, db="cx"):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    got = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8", "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"],
                   check=True, capture_output=True)
    try:
        assert _wait(MY_PORT) and _wait(PG_PORT)
        for probe in (lambda: my("select 1", None),
                      lambda: pg("select 1", "postgres")):
            for _ in range(60):
                try:
                    probe()
                    break
                except AssertionError:
                    time.sleep(2)
            else:
                pytest.fail("a server never answered")
        yield
    finally:
        for n in (MY, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _fresh(src):
    """Two tables on the source, the same two empty on the target."""
    my("drop database if exists cx; create database cx", None)
    pg("drop database if exists cx with (force)", "postgres")
    pg("create database cx", "postgres")
    for table in ("a", "b"):
        ddl = f"create table {table} (id bigint primary key, v varchar(20))"
        my(ddl)
        pg(ddl)
    fill = "insert into {} values " + ", ".join(
        f"({i}, 'v{i}')" for i in range(1, 21))
    run = my if src == "mysql" else pg
    for table in ("a", "b"):
        run(fill.format(table))
    if src == "postgres":
        # a slot a previous test left would hold changes from before this one
        pg("select pg_drop_replication_slot(slot_name) from"
           " pg_replication_slots")


def _conf(tmp_path, monkeypatch, src, extra=""):
    import migkit.config as cfg
    ends = {"mysql": f"{{host: 127.0.0.1, port: {MY_PORT}, user: root,"
                     " password: test}",
            "postgres": f"{{host: 127.0.0.1, port: {PG_PORT},"
                        " user: postgres, password: test}"}
    dst = "postgres" if src == "mysql" else "mysql"
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  fc:\n    engine: hetero\n"
        f"    source: {ends[src]}\n    target: {ends[dst]}\n"
        "    databases: [cx]\n"
        f"    options: {{source_engine: {src}, target_engine: {dst}}}\n"
        + extra)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _move(monkeypatch, mode, during=None):
    """`migkit move fc` with `during(table)` run on the source after each
    table is copied, and the tail stopped once it has nothing left - the way
    an operator's ctrl-c stops it."""
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.hetero import HeteroEngine
    real_move = HeteroEngine.move_table

    def copy_then_write(self, db, sch, tbl, chunk, ck, log):
        real_move(self, db, sch, tbl, chunk, ck, log)
        if during:
            during(tbl or sch)

    monkeypatch.setattr(HeteroEngine, "move_table", copy_then_write)
    real_tail = HeteroEngine.tail_apply

    def tail_until_quiet(self, db, go, token_path, log):
        engine = self.src_engine
        real_changes = type(engine).neutral_changes

        def changes(side, db, token=None, limit=1000):
            got, token = real_changes(engine, side, db, token, limit)
            if not got:
                raise KeyboardInterrupt
            return got, token

        monkeypatch.setattr(engine, "neutral_changes", changes)
        return real_tail(self, db, go, token_path, log)

    monkeypatch.setattr(HeteroEngine, "tail_apply", tail_until_quiet)
    got = CliRunner().invoke(cli.main, ["move", "fc", "--mode", mode,
                                        "--db", "cx", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _rows(side_sql, table):
    # concat so both clients print a row the same way
    return side_sql(f"select concat(id, ':', v) from {table} order by id")


def _same(src):
    ssql, dsql = (my, pg) if src == "mysql" else (pg, my)
    for table in ("a", "b"):
        assert _rows(dsql, table) == _rows(ssql, table), table


def _write_on(src):
    run = my if src == "mysql" else pg

    def during(table):
        # after its own copy: only the tail can carry these now
        run(f"update {table} set v = 'changed' where id = 1")
        run(f"delete from {table} where id = 2")
        run(f"insert into {table} values (100, 'new')")
    return during


@pytest.mark.parametrize("src", ["mysql", "postgres"])
@pytest.mark.usefixtures("servers")
@docker
def test_what_changed_during_the_copy_arrives(src, tmp_path, monkeypatch):
    _fresh(src)
    _conf(tmp_path, monkeypatch, src)
    got, said = _move(monkeypatch, "full+cdc", _write_on(src))
    assert got.exit_code == 0, said
    assert "stopped after" in said, said
    _same(src)
    dst = pg if src == "mysql" else my
    assert dst("select v from a where id = 1") == "changed"
    assert dst("select count(*) from a where id in (2, 100)") == "1"


@pytest.mark.parametrize("src", ["mysql", "postgres"])
@pytest.mark.usefixtures("servers")
@docker
def test_a_copy_that_kept_no_position_is_copied_again(src, tmp_path,
                                                     monkeypatch):
    """`--mode full` first, a change, then `--mode full+cdc`: the finished
    tables used to be skipped as done and the tail began at the change's
    far side, so the change was carried by nothing."""
    _fresh(src)
    _conf(tmp_path, monkeypatch, src)
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _move(monkeypatch, "full")
    assert got.exit_code == 0, said
    run = my if src == "mysql" else pg
    run("update b set v = 'between' where id = 3")
    run("delete from b where id = 4")
    got, said = _move(monkeypatch, "full+cdc")
    assert got.exit_code == 0, said
    assert "saved no change position" in said, said
    _same(src)


@pytest.mark.usefixtures("servers")
@docker
def test_a_saved_position_is_kept_on_the_next_run(tmp_path, monkeypatch):
    """A second full+cdc starts no later than the first one's position:
    later would skip, earlier only replays."""
    _fresh("mysql")
    _conf(tmp_path, monkeypatch, "mysql")
    got, said = _move(monkeypatch, "full+cdc")
    assert got.exit_code == 0, said
    path = next((tmp_path / "reports").rglob("tail-token.json"))
    first = json.loads(path.read_text())["token"]
    assert first and first.get("log_file"), first
    my("insert into a values (200, 'x')")
    got, said = _move(monkeypatch, "full+cdc")
    assert got.exit_code == 0, said
    assert "saved no change position" not in said, said
    _same("mysql")


@pytest.mark.usefixtures("servers")
@docker
def test_cdc_after_a_copy_with_no_position_says_what_it_cannot_carry(
        tmp_path, monkeypatch):
    """`--mode cdc` on its own cannot recopy anything, and starting from now
    after a copy is the hole; it says so rather than tailing in silence.
    (A copy that noted where the log was before it no longer leaves the
    hole - `test_a_separate_cdc_starts_before_the_copy` below - so the note
    is taken away here to reach the case it cannot.)"""
    import json
    _fresh("mysql")
    _conf(tmp_path, monkeypatch, "mysql")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _move(monkeypatch, "full")
    assert got.exit_code == 0, said
    assert "carried by nothing" not in said, said
    record = next((tmp_path / "reports").rglob("copy-position.json"))
    record.write_text(json.dumps({**json.loads(record.read_text()),
                                  "before": None}))
    got, said = _move(monkeypatch, "cdc")
    assert got.exit_code == 0, said
    assert "carried by nothing" in said, said
    # the first tail saved where it started, even with nothing to carry,
    # so the next one resumes there and has no hole to warn about
    got, said = _move(monkeypatch, "cdc")
    assert got.exit_code == 0, said
    assert "carried by nothing" not in said, said


@pytest.mark.usefixtures("servers")
@docker
def test_a_tail_stopped_before_anything_arrived_resumes_where_it_began(
        tmp_path, monkeypatch):
    """The position used to be saved only once a change had been applied,
    so a tail stopped on a quiet source started the next run from a later
    now - and what was written between the two runs was in neither."""
    _fresh("mysql")
    _conf(tmp_path, monkeypatch, "mysql")
    got, said = _move(monkeypatch, "cdc")
    assert got.exit_code == 0, said
    assert "stopped after 0 changes" in said, said
    my("insert into a values (300, 'between runs')")
    got, said = _move(monkeypatch, "cdc")
    assert got.exit_code == 0, said
    assert pg("select v from a where id = 300") == "between runs"


@pytest.mark.parametrize("src", ["mysql", "postgres"])
@pytest.mark.usefixtures("servers")
@docker
def test_the_tail_leaves_an_excluded_table_alone(src, tmp_path,
                                                 monkeypatch):
    """The move left `b` and `c` alone and the tail carried their changes
    anyway - into a table the target owns - and a keyless excluded table
    stopped it outright."""
    _fresh(src)
    run, dst = (my, pg) if src == "mysql" else (pg, my)
    run("create table c (v varchar(20))")
    dst("insert into b values (999, 'mine')")
    _conf(tmp_path, monkeypatch, src, "    exclude: [b, c]\n")

    def during(table):
        _write_on(src)(table)
        run("update b set v = 'from the source' where id = 1")
        run("insert into c values ('keyless')")
    got, said = _move(monkeypatch, "full+cdc", during)
    assert got.exit_code == 0, said
    assert "no primary key" not in said, said
    assert _rows(dst, "a") == _rows(run, "a")
    assert _rows(dst, "b") == "999:mine", _rows(dst, "b")


@pytest.mark.parametrize("src", ["mysql", "postgres"])
@pytest.mark.usefixtures("servers")
@docker
def test_the_tail_writes_where_the_move_wrote(src, tmp_path, monkeypatch):
    """A table the hop renames was copied under its new name and then kept
    up to date under its old one."""
    _fresh(src)
    run, dst = (my, pg) if src == "mysql" else (pg, my)
    dst("alter table a rename to a_new")
    _conf(tmp_path, monkeypatch, src, "    mapping: {tables: {a: a_new}}\n")
    got, said = _move(monkeypatch, "full+cdc", _write_on(src))
    assert got.exit_code == 0, said
    assert _rows(dst, "a_new") == _rows(run, "a")
    assert dst("select v from a_new where id = 1") == "changed"


@pytest.mark.parametrize("mode", ["full", "full+cdc"])
@pytest.mark.usefixtures("servers")
@docker
def test_a_row_filter_is_applied_by_the_copy_and_the_tail(
        mode, tmp_path, monkeypatch):
    """Neither cross-engine copier took a predicate, and the tail could
    not judge a change against one, so this used to stop before the copy.
    Both read through the filter now: the copy carries only what it
    selects, and a change the tail meets outside it is not applied
    (`test_the_pair_honours_the_row_filter.py` has the tail's cases)."""
    _fresh("mysql")
    _conf(tmp_path, monkeypatch, "mysql",
          "    mapping: {where: {a: 'id > 5'}}\n")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")

    def during(table):
        if table == "a":
            my("insert into a values (0, 'out'), (30, 'in')")
    got, said = _move(monkeypatch, mode, during)
    assert got.exit_code == 0, said
    # 6..20 by the copy; 30 only where the tail ran after it
    want = "16" if mode == "full+cdc" else "15"
    assert pg("select count(*) from a") == want, said
    assert pg("select count(*) from a where id <= 5") == "0", said


def test_an_unreadable_position_is_not_read_as_none(tmp_path):
    """Read as nothing, a tail would start from now and skip everything
    since the file was written."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(Hop(
        name="u", engine="hetero",
        source=Endpoint(host="127.0.0.1", port=1, user="root",
                        password="CHANGE_ME"),
        target=Endpoint(host="127.0.0.1", port=1, user="postgres",
                        password="CHANGE_ME"),
        databases=["cx"],
        options={"source_engine": "mysql", "target_engine": "postgres"}))
    path = tmp_path / "tail-token.json"
    path.write_text("{not json")
    with pytest.raises(SystemExit) as e:
        eng.tail_apply("cx", True, path, lambda m: None)
    assert "cannot be read" in str(e.value), e.value


def test_full_cdc_without_a_database_stops_before_copying(tmp_path,
                                                         monkeypatch):
    """The tail needs one database; finding that out after copying all of
    them wastes the copy."""
    import migkit.config as cfg
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.hetero import HeteroEngine
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  fc:\n    engine: hetero\n"
        "    source: {host: 127.0.0.1, port: 1, user: root,"
        " password: CHANGE_ME}\n"
        "    target: {host: 127.0.0.1, port: 1, user: postgres,"
        " password: CHANGE_ME}\n"
        "    databases: [cx]\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    copied = []
    monkeypatch.setattr(HeteroEngine, "move_table",
                        lambda self, *a: copied.append(a))
    got = CliRunner().invoke(cli.main, ["move", "fc", "--mode", "full+cdc",
                                        "--go"])
    said = got.output + str(got.exception or "")
    assert got.exit_code != 0, said
    assert "needs --db" in said, said
    assert copied == []


MG, MG_PORT = "migkit-test-fcdc-mg", 15674


def mongosh(script, db="cx"):
    return subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", db,
                           "--eval", script], capture_output=True, text=True)


@pytest.fixture(scope="module")
def mongo():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    try:
        assert _wait(MG_PORT)
        for _ in range(40):
            if mongosh("db.runCommand({ping:1}).ok", "admin").returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mongo never answered")
        mongosh('rs.initiate({_id:"rs0",members:'
                '[{_id:0,host:"127.0.0.1:27017"}]})', "admin")
        for _ in range(30):
            if "PRIMARY" in mongosh(
                    "rs.status().myState === 1 ? 'PRIMARY' : 'no'",
                    "admin").stdout:
                break
            time.sleep(2)
        else:
            pytest.fail("the replica set never became primary")
        from migkit.config import Endpoint, Hop
        from migkit.engines.mongodb import MongoEngine
        ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                      options={"uri_options": "directConnection=true"})
        yield MongoEngine(Hop(name="fcmg", engine="mongodb", source=ep,
                              target=ep, db_map={"cx": "cx"}))
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


@docker
def test_a_mongo_position_taken_before_the_writes_reads_them(mongo):
    """The same contract on the third change log: a resume token for now,
    taken without reading an event, and every write after it comes back."""
    assert mongosh("db.t.insertMany([{_id: 1, v: 'a'}, {_id: 2, v: 'b'}])"
                   ).returncode == 0
    point = mongo.change_point("src", "cx")
    assert point and isinstance(point, str), point
    assert mongosh("db.t.updateOne({_id: 1}, {$set: {v: 'changed'}});"
                   " db.t.deleteOne({_id: 2});"
                   " db.t.insertOne({_id: 3, v: 'new'})").returncode == 0
    got, token = mongo.neutral_changes("src", "cx", point)
    seen = [(c["op"], c["key"]["_id"]) for c in got]
    assert seen == [("update", 1), ("delete", 2), ("insert", 3)], seen
    assert token and token != point
    # and nothing from before the point: the two inserts are not in it
    assert ("insert", 1) not in seen


def _mongo_pair(mongo, exclude=()):
    """The same server, a second database as the target."""
    from migkit.engines.mongodb import MongoEngine
    hop = mongo.hop
    from migkit.config import Hop
    return MongoEngine(Hop(name="fcmg2", engine="mongodb", source=hop.source,
                           target=hop.target, db_map={"cx": "cy"},
                           exclude=list(exclude)))


def _run_tail(eng, path, go, during=None, seconds=4):
    """The same-engine tail in a thread, `during()` run while it watches,
    then interrupted the way a person would."""
    import ctypes
    import threading
    lines, done = [], threading.Event()

    def run():
        try:
            eng.tail_apply("cx", go, path, lines.append)
        except BaseException as e:
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(2)
    if during:
        during()
    done.wait(timeout=seconds)
    if not done.is_set():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
        done.wait(timeout=20)
    return lines


@docker
def test_a_mongo_count_only_tail_leaves_the_position_alone(mongo, tmp_path):
    """A dry run that saved its position every hundred events sent the next
    `--go` past events nobody applied."""
    eng = _mongo_pair(mongo)
    path = tmp_path / "tail-token.json"
    lines = _run_tail(eng, path, False, lambda: mongosh(
        "db.q.insertMany([...Array(150).keys()].map(i => ({_id: 1000 + i})))"))
    assert any("count-only" in m for m in lines), lines
    assert not path.exists(), path.read_text()


@docker
def test_a_quiet_mongo_tail_resumes_where_it_began(mongo, tmp_path):
    eng = _mongo_pair(mongo)
    path = tmp_path / "tail-token.json"
    _run_tail(eng, path, True)
    assert path.exists()
    assert mongosh("db.q.insertOne({_id: 500, v: 'between runs'})"
                   ).returncode == 0
    _run_tail(eng, path, True)
    got = mongosh("(db.q.findOne({_id: 500}) || {}).v", "cy").stdout.strip()
    assert got == "between runs", got


@docker
def test_a_mongo_tail_stops_at_a_dropped_collection(mongo, tmp_path):
    """A drop used to go by unremarked: the target kept the collection and
    the tail carried on as though nothing had happened."""
    eng = _mongo_pair(mongo)
    assert mongosh("db.gone.insertOne({_id: 1})").returncode == 0
    path = tmp_path / "tail-token.json"
    lines = _run_tail(eng, path, True, lambda: mongosh("db.gone.drop()"))
    said = " ".join(lines)
    assert "exit: SystemExit" in said and "'drop'" in said, said


def test_every_engine_with_a_change_log_can_say_where_it_is_now():
    """A change log with no way to name "now" is the hole above waiting for
    the next engine: its tail could only start after the copy."""
    from migkit.engines import NAMES, _class_for
    from migkit.engines.base import Engine
    with_log = []
    for name in NAMES:
        cls = _class_for(name)
        if cls.neutral_changes is not Engine.neutral_changes:
            with_log.append(name)
            assert cls.change_point is not Engine.change_point, name
    assert set(with_log) >= {"mysql", "postgres", "mongodb"}, with_log


@docker
def test_a_mongo_tail_leaves_an_excluded_collection_alone(mongo, tmp_path):
    """Written or dropped, a collection the hop excludes is the target's
    business: neither carried nor a reason to stop."""
    eng = _mongo_pair(mongo, exclude=["cache"])
    path = tmp_path / "tail-token.json"
    lines = _run_tail(eng, path, True, lambda: mongosh(
        "db.cache.insertOne({_id: 1}); db.cache.drop();"
        " db.q.insertOne({_id: 900})"))
    said = " ".join(lines)
    assert "SystemExit" not in said, said
    assert mongosh("db.cache.countDocuments()", "cy").stdout.strip() == "0"
    got = mongosh("(db.q.findOne({_id: 900}) || {})._id", "cy").stdout
    assert got.strip() == "900", got


@docker
def test_the_cross_engine_mongo_reader_leaves_it_alone_too(mongo):
    eng = _mongo_pair(mongo, exclude=["cache"])
    point = eng.change_point("src", "cx")
    assert mongosh("db.cache.insertOne({_id: 2}); db.cache.drop();"
                   " db.q.insertOne({_id: 901})").returncode == 0
    got, _ = eng.neutral_changes("src", "cx", point)
    assert [(c["table"], c["key"]["_id"]) for c in got] == [("q", 901)], got


@pytest.mark.usefixtures("servers")
@docker
def test_a_separate_cdc_starts_before_the_copy(tmp_path, monkeypatch):
    """A full copy on its own notes where the source's log was before it;
    a `--mode cdc` run later starts there, so a row written between the two
    arrives instead of being carried by nothing."""
    _fresh("mysql")
    _conf(tmp_path, monkeypatch, "mysql")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _move(monkeypatch, "full")
    assert got.exit_code == 0, said
    my("insert into a values (301, 'gap')")
    got, said = _move(monkeypatch, "cdc")
    assert got.exit_code == 0, said
    assert "before the copy of" in said, said
    assert "carried by nothing" not in said, said
    assert pg("select v from a where id = 301") == \
        "gap"
