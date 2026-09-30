"""Stored code proved by running it, never by its absence (backlog R11,
research round 2026-09-28 F0).

The proof of converted code said `ok` when every routine was a procedure
it never ran ("answer nothing to compare"), looked at no trigger and no
event, and sent every call of a function in one query - so one input that
failed on the target failed the whole side and the verdict named no input.
Here each of those is held: a procedure nobody ran is not `ok`; a function
that parts from the source on one input names that input; a trigger is
fired on both sides and the rows it wrote compared; an event's body is run
on both; a procedure's OUT values, result sets and writes are compared -
all on copies in a sandbox, while the hop's own servers are only read.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-code-my", "migkit-test-code-pg"
SB_MY, SB_PG = "migkit-test-code-sbmy", "migkit-test-code-sbpg"
MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT = 16080, 16081, 16082, 16083
#: small servers: four of them run at once for these tests
#: the target's PostgreSQL: 17 has `to_oct`, the fix a person makes for
#: MySQL's `OCT` below
PG_MAJOR = 17
#: the scheduler off: the source's event below (every hour, from the
#: moment it is made) would otherwise run on the source at once and delete
#: the row the event's proof tells the two versions apart by
MY_SMALL = ["--innodb-buffer-pool-size=16M", "--performance-schema=OFF",
            "--log-bin-trust-function-creators=1", "--event-scheduler=OFF"]


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, port_name=PG, db="cx"):
    got = _sh("docker", "exec", "-i", port_name, "psql", "-U", "postgres",
              "-d", db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, port=MY_PORT, db="cx"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=port, user="root",
                           password="test", database=db, autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


def _up_mysql(name, port):
    _sh("docker", "rm", "-f", "-v", name)
    _sh("docker", "run", "-d", "--name", name, "-e",
        "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
        "mysql:8.4", *MY_SMALL)


def _up_pg(name, port, image):
    _sh("docker", "rm", "-f", "-v", name)
    _sh("docker", "run", "-d", "--name", name, "-e",
        "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)


def _wait_mysql(name):
    for _ in range(90):
        if _sh("docker", "exec", name, "mysql", "-uroot", "-ptest",
               "-h127.0.0.1", "--protocol=tcp", "-e",
               "select 1").returncode == 0:
            return
        time.sleep(2)
    raise AssertionError(f"{name} did not come up")


def _wait_pg(name):
    for _ in range(60):
        if _sh("docker", "exec", name, "pg_isready", "-U",
               "postgres").returncode == 0 and _sh(
                "docker", "exec", name, "psql", "-U", "postgres", "-c",
                "select 1").returncode == 0:
            return
        time.sleep(1)
    raise AssertionError(f"{name} did not come up")


def _sandbox_image():
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(PG_MAJOR)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        built = _sh("docker", "build", "-t", tag, "-",
                    input=PG_DOCKERFILE.format(v=PG_MAJOR), timeout=900)
        assert built.returncode == 0, built.stderr[-400:]
    return tag


SOURCE = [
    "create table orders (id int primary key auto_increment,"
    " amount decimal(10,2) not null, cust varchar(20), status"
    " varchar(10) not null default 'new')",
    "create table audit (id int primary key auto_increment, order_id int,"
    " what varchar(40))",
    "insert into orders (id, amount, cust) values (1, 5.00, 'ann'),"
    " (2, 20.50, 'bob'), (3, 10.00, 'Céline'), (4, 99.99, null)",
    # answers NULL for a zero divisor; the target's hand-written version
    # below raises there instead, and answers the same everywhere else
    "create function per_unit(total int, n int) returns int deterministic"
    " return if(n = 0, null, total div n)",
    "create procedure ping() begin select 1 as one; end",
    "create procedure add_order(in amt decimal(10,2), in who varchar(20),"
    " out made int) begin insert into orders (amount, cust) values"
    " (amt, who); set made = last_insert_id(); select count(*) as n from"
    " orders; end",
    "create trigger orders_bi before insert on orders for each row"
    " set new.cust = upper(new.cust)",
    "create trigger orders_au after update on orders for each row"
    " insert into audit (order_id, what) values (new.id,"
    " concat('amount ', old.amount, ' -> ', new.amount))",
    "create event purge_small on schedule every 1 hour do delete from"
    " orders where amount < 6",
]


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    image = _sandbox_image()
    _up_mysql(MY, MY_PORT)
    _up_mysql(SB_MY, SB_MY_PORT)
    _up_pg(PG, PG_PORT, f"postgres:{PG_MAJOR}")
    _up_pg(SB_PG, SB_PG_PORT, image)
    try:
        for n in (MY, SB_MY):
            _wait_mysql(n)
        for n in (PG, SB_PG):
            _wait_pg(n)
        for port in (MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _my("create database cx", db=None)
        _my(*SOURCE)
        _pg("create database cx", db="postgres")
        _pg("""
            create table orders (id integer generated by default as
              identity primary key, amount numeric(10,2) not null,
              cust varchar(20), status varchar(10) not null default 'new');
            create table audit (id integer generated by default as identity
              primary key, order_id integer, what varchar(40));
            insert into orders (id, amount, cust) overriding system value
              values (1, 5.00, 'ann'), (2, 20.50, 'bob'),
              (3, 10.00, 'Céline'), (4, 99.99, null);
            select setval(pg_get_serial_sequence('orders', 'id'), 4);
            """)
        yield
    finally:
        for n in (MY, PG, SB_MY, SB_PG):
            _sh("docker", "rm", "-f", "-v", n)


def _hop(tmp_path, sandbox=True):
    options = {"source_engine": "mysql", "target_engine": "postgres"}
    if sandbox:
        options["proof_sandbox"] = {
            "source": {"host": "127.0.0.1", "port": SB_MY_PORT,
                       "user": "root", "password": "test"},
            "target": {"host": "127.0.0.1", "port": SB_PG_PORT,
                       "user": "postgres", "password": "test"}}
    hop = Hop(name="code", engine="hetero", options=options,
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _engine(tmp_path, sandbox=True):
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(_hop(tmp_path, sandbox))


def _verdicts(tmp_path):
    import json
    got = json.loads((tmp_path / "code-proof.json").read_text())
    return {v["routine"]: v for v in got["verdicts"]}


def _source_state():
    return _my("select id, amount, cust, status from orders order by id",
               "select count(*) from audit",
               "select count(*) from information_schema.routines where"
               " routine_schema = 'cx'")


def _target_as_written_by_hand():
    """The target's versions, as a person would write them - two of them
    wrong on purpose, one input's worth each."""
    _pg("""
        create or replace function per_unit(total integer, n integer)
          returns integer language sql as $$ select total / n $$;
        create or replace procedure ping(inout rs refcursor default null)
          language plpgsql as $$ begin open rs for select 1 as one; end $$;
        create or replace procedure add_order(amt numeric, who varchar,
          inout made integer default null,
          inout rs refcursor default null) language plpgsql as $$
        begin
          insert into orders (amount, cust) values (amt, who)
            returning id into made;
          open rs for select count(*) as n from orders;
        end $$;
        create or replace function orders_bi() returns trigger
          language plpgsql as $$ begin new.cust := lower(new.cust);
          return new; end $$;
        drop trigger if exists orders_bi on orders;
        create trigger orders_bi before insert on orders for each row
          execute function orders_bi();
        create or replace function orders_au() returns trigger
          language plpgsql as $$ begin insert into audit (order_id, what)
          values (new.id, 'amount ' || old.amount || ' -> ' || new.amount);
          return null; end $$;
        drop trigger if exists orders_au on orders;
        create trigger orders_au after update on orders for each row
          execute function orders_au();
        create or replace procedure purge_small() language sql as
          $$ delete from orders where amount < 5 $$;
        """)


def test_a_procedure_nobody_ran_is_not_ok(servers, tmp_path):
    """No sandbox named: nothing but functions that write nothing may be
    asked, and those only on the hop's own servers, read-only. The old
    proof said `ok` here - "procedures ... answer nothing to compare"."""
    _target_as_written_by_hand()
    before = _source_state()
    got = _engine(tmp_path, sandbox=False).prove_converted("cx")
    assert got.status != "ok", got.__dict__
    by = _verdicts(tmp_path)
    for name in ("procedure ping", "procedure add_order",
                 "trigger orders_bi", "trigger orders_au",
                 "event purge_small"):
        assert by[name]["status"] == "not proved", by[name]
        assert "no sandbox is named" in by[name]["reason"], by[name]
    assert _source_state() == before


def test_a_function_that_parts_on_one_input_names_it(servers, tmp_path):
    """The source answers NULL for a zero divisor; the target's version
    raises there and answers the same for every other input. The old
    proof sent every call in one query: the target's side failed whole
    and the verdict named no input."""
    _target_as_written_by_hand()
    got = _engine(tmp_path, sandbox=False).prove_converted("cx")
    assert got.status == "diff", got.__dict__
    by = _verdicts(tmp_path)
    v = by["function per_unit"]
    assert v["status"] == "diff", v
    assert v["reason"] == ("called with (total=0, n=0): error: no error"
                           " against 22012 (division by zero)"), v


def test_triggers_and_events_are_run_and_compared(servers, tmp_path):
    """In the sandbox: the trigger that lowers a name where the source
    raises it, and the event whose body deletes other rows, are found by
    what they wrote; the trigger written right is not."""
    _target_as_written_by_hand()
    before = _source_state()
    got = _engine(tmp_path).prove_converted("cx")
    # the fixture: a table this small carried whole, into both sandboxes,
    # and every call's writes rolled back there
    assert _my("select id from orders order by id", port=SB_MY_PORT) == \
        ((1,), (2,), (3,), (4,))
    assert _pg("select string_agg(id::text, ',' order by id) from orders",
               SB_PG) == "1,2,3,4"
    assert got.status == "diff", got.__dict__
    by = _verdicts(tmp_path)
    assert by["trigger orders_bi"]["status"] == "diff", by["trigger orders_bi"]
    assert by["trigger orders_bi"]["reason"].startswith(
        "a new row: rows changed in orders"), by["trigger orders_bi"]
    assert by["event purge_small"]["status"] == "diff", by["event purge_small"]
    assert "rows changed in orders" in by["event purge_small"]["reason"], \
        by["event purge_small"]
    assert by["trigger orders_au"]["status"] in ("ok", "warn"), \
        by["trigger orders_au"]
    assert by["procedure ping"]["status"] in ("ok", "warn"), \
        by["procedure ping"]
    # the hop's own source was only read
    assert _source_state() == before
    assert _pg("select count(*) from audit") == "0"


# ---- converted by the candidate that proves itself ----------------------------

CONVERT_SOURCE = [
    "create table items (id int primary key auto_increment, name"
    " varchar(20) not null, qty int not null default 0, price"
    " decimal(8,2))",
    "create table moves (id int primary key auto_increment, item_id int not"
    " null, delta int not null, note varchar(40))",
    "insert into items values (1, 'bolt', 10, 0.25), (2, 'nut', 0, 0.10),"
    " (3, 'Gear', 5, 12.50)",
    "create function steps(x int) returns int deterministic begin declare y"
    " int; set y = x + 1; return y; end",
    "create function grade(s int) returns varchar(4) deterministic begin if"
    " s >= 90 then return 'A'; elseif s >= 80 then return 'B'; end if;"
    " return 'C'; end",
    "create trigger items_bu before update on items for each row begin if"
    " new.qty < 0 then set new.qty = 0; end if; end",
    "create procedure restock(in item int, in n int, out left_qty int)"
    " begin if n is null or n <= 0 then signal sqlstate '45000' set"
    " message_text = 'nothing to add'; end if; update items set qty = qty +"
    " n where id = item; insert into moves (item_id, delta, note) values"
    " (item, n, concat('restock ', n)); select qty into left_qty from items"
    " where id = item; end",
    # the same without the NULL guard - measured: for n NULL MySQL writes
    # 0 where `qty + n` is NULL and keeps it when the insert after it
    # fails; PostgreSQL refuses the NULL and its call writes nothing. No
    # candidate's version does what MySQL does, and none is taken
    "create procedure restock_loose(in item int, in n int) begin if n <= 0"
    " then signal sqlstate '45000' set message_text = 'nothing to add';"
    " end if; update items set qty = qty + n where id = item; insert into"
    " moves (item_id, delta, note) values (item, n, concat('restock ',"
    " n)); end",
    "create procedure total_value(out v decimal(12,2)) begin declare done"
    " int default 0; declare q int; declare p decimal(8,2); declare c cursor"
    " for select qty, price from items order by id; declare continue"
    " handler for not found set done = 1; set v = 0; open c; l: loop fetch"
    " c into q, p; if done then leave l; end if; set v = v + q *"
    " coalesce(p, 0); end loop; close c; end",
    "create event nightly on schedule every 1 day do update items set qty ="
    " qty where qty > 100",
    "create procedure dyn() begin set @s = 'select 1 as one'; prepare st"
    " from @s; execute st; deallocate prepare st; end",
    # OCT has no PostgreSQL function of that name: the rules' versions
    # fail until a person's fix for one of them teaches the rule
    "create function octal(n bigint) returns varchar(30) deterministic"
    " return oct(n)",
    "create function octal2(n bigint) returns varchar(40) deterministic"
    " return concat('0o', oct(n))",
]

#: a model, played by a stand-in: wrong on the first round, right once it
#: is told why
MODEL_ANSWERS = [
    "```sql\ncreate procedure dyn(inout rs refcursor default null) language"
    " plpgsql as $$ begin open rs for select 2 as one; end $$;\n```",
    "```sql\ncreate procedure dyn(inout rs refcursor default null) language"
    " plpgsql as $$ begin open rs for select 1 as one; end $$;\n```",
]


@pytest.fixture
def model(monkeypatch):
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    asked, others = [], []

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            prompt = json.loads(self.rfile.read(n))["messages"][0]["content"]
            if ", dyn," in prompt:
                asked.append(prompt)
                answer = MODEL_ANSWERS[min(len(asked),
                                           len(MODEL_ANSWERS)) - 1]
            else:
                # asked of anything else: an answer with no statement
                others.append(prompt)
                answer = "I cannot convert this."
            out = json.dumps({"choices": [{"message": {
                "content": answer}}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)
    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_URL",
                       f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "stand-in")
    monkeypatch.setenv("MIGKIT_AI_SHARE", "code")
    yield asked
    server.shutdown()
    server.server_close()


@pytest.fixture(scope="module")
def convertible(servers):
    _my("create database cv", db=None)
    _my(*CONVERT_SOURCE, db="cv")
    _pg("create database cv", db="postgres")
    _pg("""
        create table items (id integer generated by default as identity
          primary key, name varchar(20) not null, qty integer not null
          default 0, price numeric(8,2));
        create table moves (id integer generated by default as identity
          primary key, item_id integer not null, delta integer not null,
          note varchar(40));
        insert into items overriding system value values
          (1, 'bolt', 10, 0.25), (2, 'nut', 0, 0.10), (3, 'Gear', 5, 12.50);
        select setval(pg_get_serial_sequence('items', 'id'), 3);
        """, db="cv")
    yield


def _convert_engine(tmp_path, code_dir):
    from migkit.engines.hetero import HeteroEngine
    hop = _hop(tmp_path)
    hop.databases = ["cv"]
    hop.options["code_dir"] = str(code_dir)
    return HeteroEngine(hop)


def test_each_routine_by_the_candidate_that_proves_itself(
        convertible, tmp_path, model):
    import json
    code_dir = tmp_path / "convert"
    code_dir.mkdir()
    # the operator's own files: one wrong (it adds two), one right
    (code_dir / "steps.sql").write_text(
        "create function steps(x integer) returns integer language sql as"
        " $$ select x + 2 $$;")
    (code_dir / "octal.sql").write_text(
        "create function octal(n bigint) returns varchar(30) language sql"
        " as $$ select to_oct(n) $$;")
    (code_dir / "grade.sql").write_text(
        "create function grade(s integer) returns varchar(4) language sql as"
        " $$ select case when s >= 90 then 'A' when s >= 80 then 'B' else"
        " 'C' end $$;")
    eng = _convert_engine(tmp_path, code_dir)
    got = {n: s for n, s, _ in eng.converted_code("cv")}
    conv = json.loads((tmp_path / "code-conversion.json").read_text())
    # the named sandbox was used (else this says why not)
    assert conv["sandbox"] == "used", conv["sandbox"]
    record = {r["routine"]: r for r in conv["routines"]}

    # the operator's file for it is wrong: another candidate's version,
    # proved, is taken instead
    assert record["function steps"]["class"] == "proved", record
    assert record["function steps"]["rung"] not in (None, "by-hand"), record
    assert any(f.startswith("the operator's own file: called with (x=")
               for f in record["function steps"]["fell"]), record
    assert record["function grade"]["rung"] == "by-hand", record
    # the proof that chose it tells the small changes of it apart
    score = record["function grade"]["mutants"]
    assert score["tried"] >= 3 and score["caught"] == score["tried"], score
    for name in ("procedure restock", "procedure total_value",
                 "trigger items_bu"):
        assert record[name]["rung"] == "rules", record[name]
        assert record[name]["class"] in ("proved",
                                         "proved with caveats"), record[name]
    loose = record["procedure restock_loose"]
    assert loose["class"] == "not converted", loose
    assert any(x.startswith("migkit's rules: called with (item=") and
               "n=None" in x and "rows changed in items" in x
               for x in loose["reasons"]), loose
    # and the part of the source that still differs, alone
    assert "migkit's rules: the smallest part of the source that still" \
        " differs: update items set qty = qty + n where id = item" in \
        loose["reasons"], loose
    assert got["restock_loose"].startswith(
        "-- procedure restock_loose not converted"), got["restock_loose"]
    assert record["event nightly"]["class"] == "proved with caveats", \
        record["event nightly"]
    assert "schedule" in got["nightly"], got["nightly"]
    # the model: wrong first, told why, right the second time
    assert record["procedure dyn"]["rung"] == "model", record
    assert len(model) == 2, model
    assert "Why it failed:" in model[1] and "result sets" in model[1], model
    # code went, never a row of the source's
    assert not any("bolt" in p or "Gear" in p for p in model), model
    # the pass rates kept for the next routine's ranking
    rates = json.loads((tmp_path / "code-rungs.json").read_text())
    assert rates["mysql->postgres:function"]["by-hand"] == {
        "tried": 3, "proved": 2}, rates
    # a person's fix for one routine, learned as a rule, proved on the
    # other before it was adopted, and converting it
    learned = json.loads((tmp_path / "code-rules.json").read_text())
    assert [(r["from"], r["to"]) for r in learned["adopted"]] == [
        ("oct", "to_oct")], learned
    assert record["function octal"]["rung"] == "by-hand", record
    assert record["function octal2"]["rung"] == "rules", record
    # what was chosen carries its proof, and nothing unproved is in it
    assert "converted by migkit's rules and proved" in got["restock"], got

    # and the target, built from it, proves the same
    for name, stmt in got.items():
        if not stmt.startswith("--"):
            eng.dst_engine.neutral_create_code("dst", "cv", stmt)
    result = eng.prove_converted("cv")
    by = _verdicts(tmp_path)
    assert result.status == "warn", result.__dict__
    assert by["event nightly"]["status"] == "warn", by["event nightly"]
    assert by["procedure restock_loose"]["status"] == "missing", by
    assert all(v["status"] in ("ok", "warn") for k, v in by.items()
               if k != "procedure restock_loose"), by

    # the proof kept as tests on the target: they pass there now, and
    # fail the day someone changes what the routine answers
    kept = (tmp_path / "code-tests.sql").read_text()
    assert '"grade"(' in kept, kept
    assert _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
               "cv", "-v", "ON_ERROR_STOP=1", input=kept).returncode == 0
    _pg("create or replace function grade(s integer) returns varchar(4)"
        " language sql as $$ select case when s > 90 then 'A' when s >= 80"
        " then 'B' else 'C' end $$", db="cv")
    broke = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
                "cv", "-v", "ON_ERROR_STOP=1", input=kept)
    assert broke.returncode != 0, broke.stdout
    assert "grade(90) no longer answers as the source did" in broke.stderr, \
        broke.stderr


def test_the_proof_can_say_different(convertible, tmp_path):
    """Its own mutation score: every small change to a proved version -
    a comparison across its boundary, a constant off by one, a statement
    left out - is caught."""
    from migkit.storedcode import mutants, mysql_pg
    from migkit.storedcode.prove import Proof
    eng = _convert_engine(tmp_path, tmp_path / "none")
    proof = Proof(eng, "cv")
    proof.open(True)
    assert proof.sandbox is not None, proof.why_not
    try:
        proof.begin([])
        r = next(x for x in proof.src_list if x.name == "grade")
        made = mysql_pg.convert(r)
        verdict, _ = proof.try_version(r, made.statements)
        assert verdict.status == "ok", verdict
        caught, tried, survived = mutants.score(proof, r, made.statements)
        assert tried >= 4, (caught, tried, survived)
        assert caught == tried, survived
    finally:
        proof.end()
        proof.close()
