"""A translated schema held to the rows it accepts and refuses (leapfrog
research P12, backlog CV.P12).

The schema check compared a table by its columns and their classes and by
the column rules it keeps; a CHECK loosened in translation, a unique key
dropped or a foreign key that no longer cascades passed it, and the first
the owner heard of it was the application's first write after cutover.
Here the constraints are probed on both sides of the sandbox and the two
answers compared: first the static half - two sides' CHECK predicates
solved in SQL's three values for a row they disagree on - then the
verdict, over stand-in servers that keep each side's rules, then on real
MySQL and PostgreSQL servers: a faithful target answers every probe
alike, a loosened one is named by the first probe it answers otherwise.
"""
import re
import socket
import subprocess
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from migkit.storedcode import constraints, harness
from migkit.storedcode.constraints import Column, equivalent
from migkit.storedcode.harness import Outcome, Watched
from tests.conftest import needs_docker

# ---- the static half: two sides' CHECKs in SQL's three values --------------


def test_a_boundary_the_target_moved_is_the_row_they_disagree_on():
    assert equivalent(["amount >= 0"], ["amount > 0"],
                      {"amount": "decimal"}) == (False, {"amount": 0})


def test_between_and_its_two_comparisons_are_one_predicate():
    assert equivalent(["qty between 1 and 5"], ["qty >= 1 and qty <= 5"],
                      {"qty": "integer"}) == (True, None)


def test_a_null_the_source_passes_and_the_target_refuses():
    """`x > 0` is UNKNOWN for NULL, and a CHECK passes on UNKNOWN;
    `coalesce(x, 0) > 0` is FALSE. Where the column takes no NULL on
    either side, nothing parts them."""
    assert equivalent(["x > 0"], ["coalesce(x, 0) > 0"],
                      {"x": "integer"}) == (False, {"x": None})
    assert equivalent(["x > 0"], ["coalesce(x, 0) > 0"],
                      {"x": Column("integer", nullable=False)}) == \
        (True, None)


def test_a_predicate_outside_what_is_solved_is_said_never_taken_as_equal():
    same, why = equivalent(["upper(s) = 'A'"], ["s = 'A'"], {"s": "text"})
    assert same is None
    assert "UPPER" in why, why
    same, why = equivalent(["a / b > 1"], ["a > b"],
                           {"a": "integer", "b": "integer"})
    assert same is None
    assert "divides" in why, why


def test_a_list_the_target_widened_is_named_by_the_value_it_added():
    assert equivalent(["status in ('a','b')"], ["status in ('a','b','c')"],
                      {"status": "text"}) == (False, {"status": "c"})


def test_the_catalogues_own_spellings_are_read():
    """As the servers hand the predicates back: MySQL's backquotes and
    charset introducers, PostgreSQL's casts, `= ANY (ARRAY[...])` for IN,
    and a negative constant as `('-100'::integer)`."""
    amount = {"amount": Column("decimal", False, ("numeric", 8, 2))}
    assert equivalent(["(`amount` >= 0)"], ["(amount >= (0)::numeric)"],
                      amount) == (True, None)
    same, row = equivalent(["(`amount` >= 0)"],
                           ["(amount > ('-100'::integer)::numeric)"], amount)
    assert same is False
    # a value the target takes and the source refuses, and one a
    # decimal(8,2) holds
    assert -100 < row["amount"] < 0, row
    assert row["amount"] == row["amount"].quantize(Decimal("0.01")), row
    assert equivalent(
        ["(`status` in (_utf8mb4'a',_utf8mb4'b'))"],
        ["((status)::text = ANY ((ARRAY['a'::character varying,"
         " 'b'::character varying])::text[]))"],
        {"status": "text"}) == (True, None)


def test_three_values_through_a_case():
    """CASE takes UNKNOWN as not TRUE: for a NULL `a` the CASE is 0, the
    first predicate FALSE and the second UNKNOWN."""
    assert equivalent(["case when a > 0 then 1 else 0 end = 1"], ["a > 0"],
                      {"a": "integer"}) == (False, {"a": None})


def test_every_engine_is_read_in_its_own_dialect():
    assert equivalent(['"AMOUNT" >= 0'], ["([amount]>=(0))"],
                      {"amount": "integer"}, "oracle", "tsql") == (True, None)
    assert equivalent(["amount >= 0"], ["amt > 0"], {"amount": "integer"},
                      "mysql", "postgres", {"amt": "amount"}) == \
        (False, {"amount": 0})


# ---- the verdict, over stand-in servers that keep each side's rules --------

#: the fixture both stand-ins hold
FIXTURE = {"customers": [[1, "ann@example.com", "ann"],
                         [2, "bob@example.com", "bob"]],
           "orders": [[10, 1, Decimal("5.00"), "new", None],
                      [11, 2, Decimal("7.50"), "paid", "gift"]]}
COLUMNS = {"customers": [("id", "integer"), ("email", "text"),
                         ("name", "text")],
           "orders": [("id", "integer"), ("customer_id", "integer"),
                      ("amount", "decimal"), ("status", "text"),
                      ("note", "text")]}


def _rules(kind, table, target, sandbox):
    """One side's constraints: the source's MySQL; the target's
    PostgreSQL, as `target` says - "faithful", "loose" (the CHECK
    loosened, the unique dropped, no cascade), or "lost" (faithful on the
    server, its sandbox's foreign key without the cascade)."""
    pg = kind == "postgres"
    loose = pg and target == "loose"
    if table == "customers":
        return constraints.Rules(
            declared={"id": "int", "email": "varchar(40)",
                      "name": "varchar(10)"},
            nullable={"id": False, "email": False, "name": False},
            uniques=[constraints.Unique("PRIMARY", ["id"], True)] + (
                [] if loose else [constraints.Unique("uq_email",
                                                     ["email"])]))
    cascade = not loose and not (pg and target == "lost" and sandbox)
    return constraints.Rules(
        declared={"id": "int", "customer_id": "int",
                  "amount": "decimal(8,2)", "note": "varchar(10)",
                  "status": "order_status" if pg
                  else "enum('new','paid','shipped')"},
        nullable={"id": False, "customer_id": False, "amount": False,
                  "status": False, "note": True},
        checks=[constraints.Check(
            "amount_ck", "(amount > ('-100'::integer)::numeric)" if loose
            else "(amount >= (0)::numeric)" if pg else "(`amount` >= 0)")],
        uniques=[constraints.Unique("PRIMARY", ["id"], True)],
        foreign=[constraints.Foreign(
            "orders_customer_fk", ["customer_id"], "customers", ["id"],
            "cascade" if cascade else "no action")],
        members={"status": ["new", "paid", "shipped"]})


class _Server:
    """A runner that answers one statement by its side's rules, as MySQL
    or PostgreSQL would, through the harness - MySQL's CHECK and enum
    refusals with the class the harness gives the HY000 and 01000 MySQL
    sends them with (`harness.MYSQL_STATES`), its other integrity
    refusals as 23000."""

    def __init__(self, engine, target):
        self.engine, self.target = engine, target
        self.watched = {t: Watched(t, c, ["id"], "id", [])
                        for t, c in COLUMNS.items()}
        self.raw = {t: [list(r) for r in rows] for t, rows in FIXTURE.items()}
        self.base = {t: [[harness.render(c, v) for (_, c), v in
                          zip(COLUMNS[t], r)] for r in rows]
                     for t, rows in self.raw.items()}
        self.conn = self

    def q(self, name):
        return f"`{name}`" if self.engine == "mysql" else f'"{name}"'

    def table(self, name):
        return self.q(name)

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def execute(self, sql, args=None):
        pass

    def fetchone(self):
        return ("STRICT_TRANS_TABLES",)

    def commit(self):
        pass

    def rollback(self):
        pass

    def _no(self, state, words):
        if self.engine == "mysql" and state not in ("23514", "22P02"):
            state = "23000" if state.startswith("23") else state
        return (state, words)

    def run(self, statement, params, tables):
        rows = {t: [list(r) for r in got] for t, got in self.raw.items()}
        m = re.match(r"insert into \S+?(\w+)\W* \((.*?)\) values (.*)$",
                     statement)
        if m:
            table = m.group(1)
            cols = [c.strip(' `"') for c in m.group(2).split(",")]
            error = None
            for k in range(m.group(3).count("(")):
                error = error or self._insert(table, dict(zip(
                    cols, params[k * len(cols):(k + 1) * len(cols)])), rows)
        else:
            key = params[0]
            fk = _rules(self.engine, "orders", self.target, True).foreign[0]
            error = None
            if any(r[1] == key for r in rows["orders"]) and \
                    fk.on_delete != "cascade":
                error = self._no("23503", "foreign key violated")
            else:
                rows["orders"] = [r for r in rows["orders"] if r[1] != key]
                rows["customers"] = [r for r in rows["customers"]
                                     if r[0] != key]
        out = Outcome(error=error)
        if error is None:
            for t in tables:
                now = [[harness.render(c, v) for (_, c), v in
                        zip(COLUMNS[t], r)] for r in rows[t]]
                got = harness.row_changes(self.base[t], now, [0], 0)
                if got:
                    out.changes[t] = got
        return out

    def _insert(self, table, values, rows):
        r = _rules(self.engine, table, self.target, True)
        names = [c for c, _ in COLUMNS[table]]
        row = [values.get(n) for n in names]
        if "id" not in values:
            row[0] = max(x[0] for x in rows[table]) + 1
        for n, v in zip(names, row):
            if v is None and not r.nullable[n]:
                return self._no("23502", f"null value in {n}")
            size = re.search(r"varchar\((\d+)\)", r.declared[n])
            if v is not None and size and len(v) > int(size.group(1)):
                return self._no("22001", "value too long")
            if v is not None and r.declared[n] == "int" and \
                    not -2 ** 31 <= v < 2 ** 31:
                return self._no("22003", "out of range")
            if v is not None and n == "amount" and abs(v) >= 10 ** 6:
                return self._no("22003", "out of range")
            if v is not None and n in r.members and v not in r.members[n]:
                return self._no("22P02", "Data truncated for column"
                                         f" '{n}' at row 1")
        for c in r.checks:
            a = row[names.index("amount")]
            if not (a > -100 if "-100" in c.text else a >= 0):
                return self._no("23514", f"Check constraint '{c.name}' is"
                                         " violated.")
        for u in r.uniques:
            at = [names.index(x) for x in u.columns]
            if any(tuple(x[i] for i in at) == tuple(row[i] for i in at)
                   for x in rows[table]):
                return self._no("23505", "duplicate key value")
        for f in r.foreign:
            v = row[names.index(f.columns[0])]
            if not any(x[0] == v for x in rows["customers"]):
                return self._no("23503", "foreign key violated")
        rows[table].append(row)
        return None


class _Engine:
    def __init__(self, kind):
        self.CANON_ENGINE = self.SQL_DIALECT = kind

    def _q(self, side, sql, args=None):
        return [("STRICT_TRANS_TABLES",)]


class _Bench:
    def __init__(self, target):
        self.db = "cx"
        self.tables = {"customers": "customers", "orders": "orders"}
        self.a = _Server("mysql", target)
        self.b = _Server("postgres", target)
        self.pair = type("Pair", (), {})()
        self.pair.src_engine = _Engine("mysql")
        self.pair.dst_engine = _Engine("postgres")
        self.sandbox = type("Sandbox", (), {})()
        self.sandbox.src = _Engine("mysql")
        self.sandbox.dst = _Engine("postgres")
        self.sandbox.tdb = "cx"


@pytest.fixture
def stand_in(monkeypatch):
    def bench(target):
        b = _Bench(target)

        def read_rules(eng, side, db, table):
            return _rules(eng.CANON_ENGINE, table, target,
                          eng in (b.sandbox.src, b.sandbox.dst))
        monkeypatch.setattr(constraints, "read_rules", read_rules)
        return b
    return bench


def test_every_probe_answered_alike_is_ok_with_the_counts(stand_in):
    got = constraints.prove(stand_in("faithful"))
    assert [r.status for r in got] == ["ok"], got
    assert got[0].scope == "cx constraints"
    assert got[0].detail == (
        "33 probes over 2 tables answered alike: 2 plain inserts, 5 NOT"
        " NULL, 3 CHECK, 3 unique, 2 foreign key, 17 declared limits,"
        " 1 enum"), got[0].detail


def test_the_first_probe_answered_otherwise_is_named_and_the_rest_listed(
        stand_in):
    """Tables by name, and in each the probes in one order: the unique
    key the target dropped (customers) comes before the CHECK it loosened
    and the cascade it lost (orders)."""
    got = constraints.prove(stand_in("loose"))
    assert [r.status for r in got] == ["diff"], got
    detail = got[0].detail
    assert detail.startswith(
        "an insert into customers repeating an existing row's email:"
        " refused on the source (23000) and accepted on the target; "), \
        detail
    assert "an insert into orders with amount = -0.01" in detail, detail
    assert "a delete of a customers row that orders rows point at" in \
        detail, detail


def test_a_constraint_the_sandbox_lost_is_named_and_not_probed(stand_in):
    """The target's server cascades and its sandbox's copy does not: the
    copy would answer for a constraint the server does not have, so the
    delete is not run, and the verdict is not `ok`."""
    got = constraints.prove(stand_in("lost"))
    assert [r.status for r in got] == ["warn"], got
    assert "is in its sandbox without its ON DELETE CASCADE" in \
        got[0].detail, got[0].detail
    assert "1 foreign key" in got[0].detail, got[0].detail


def test_one_row_the_source_refuses_and_the_loosened_target_takes(stand_in):
    x, y, what = constraints.probe_row(stand_in("loose"), "orders",
                                       {"amount": Decimal("-1")})
    assert what == "an insert into orders with amount = -1"
    # MySQL sends HY000 for a CHECK; the harness gives it PostgreSQL's
    assert x.error is not None and x.error[0] == "23514", x
    assert y.error is None, y
    assert harness.differences(x, y)


# ---- on real servers --------------------------------------------------------

MY, PG = "migkit-test-cons-my", "migkit-test-cons-pg"
SB_MY, SB_PG = "migkit-test-cons-sbmy", "migkit-test-cons-sbpg"
MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT = 16294, 16295, 16296, 16297
PG_MAJOR = 17
#: small servers: four run at once here
MY_SMALL = ["--log-bin-trust-function-creators=1",
            "--performance-schema=OFF", "--innodb-buffer-pool-size=16M"]

SOURCE = [
    "create table customers (id int primary key auto_increment, email"
    " varchar(40) not null, name varchar(10) not null, unique key uq_email"
    " (email))",
    "create table orders (id int primary key auto_increment, customer_id"
    " int not null, amount decimal(8,2) not null, status"
    " enum('new','paid','shipped') not null default 'new', note"
    " varchar(10), constraint amount_ck check (amount >= 0), constraint"
    " orders_customer_fk foreign key (customer_id) references customers"
    " (id) on delete cascade)",
    "insert into customers values (1, 'ann@example.com', 'ann'),"
    " (2, 'bob@example.com', 'bob'), (3, 'cy@example.com', 'cy')",
    "insert into orders values (10, 1, 5.00, 'new', null),"
    " (11, 2, 7.50, 'paid', 'gift'), (12, 1, 1.25, 'shipped', 'x')",
]


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, db="cx"):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, db="cx"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database=db, autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


def _wait(name, probe):
    for _ in range(90):
        if _sh("docker", "exec", name, *probe).returncode == 0:
            return
        time.sleep(2)
    raise AssertionError(f"{name} did not come up")


def _sandbox_image():
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(PG_MAJOR)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        built = _sh("docker", "build", "-t", tag, "-",
                    input=PG_DOCKERFILE.format(v=PG_MAJOR), timeout=900)
        assert built.returncode == 0, built.stderr[-400:]
    return tag


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    image = _sandbox_image()
    for name, port, img in ((MY, MY_PORT, "mysql:8.4"),
                            (SB_MY, SB_MY_PORT, "mysql:8.4")):
        _sh("docker", "rm", "-f", "-v", name)
        _sh("docker", "run", "-d", "--name", name, "-e",
            "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306", img,
            *MY_SMALL)
    for name, port, img in ((PG, PG_PORT, f"postgres:{PG_MAJOR}"),
                            (SB_PG, SB_PG_PORT, image)):
        _sh("docker", "rm", "-f", "-v", name)
        _sh("docker", "run", "-d", "--name", name, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", img)
    try:
        for name in (MY, SB_MY):
            _wait(name, ["mysql", "-uroot", "-ptest", "-h127.0.0.1",
                         "--protocol=tcp", "-e", "select 1"])
        for name in (PG, SB_PG):
            _wait(name, ["psql", "-U", "postgres", "-c", "select 1"])
        for port in (MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _my("create database cx", db=None)
        _my(*SOURCE)
        _pg("create database cx", db="postgres")
        yield
    finally:
        for name in (MY, PG, SB_MY, SB_PG):
            _sh("docker", "rm", "-f", "-v", name)


def _target(loose):
    """The target written by hand: faithful, or with the CHECK loosened
    to amount > -100, the unique key dropped and the foreign key left
    without its cascade."""
    _pg(f"""
        drop table if exists orders;
        drop table if exists customers;
        drop type if exists order_status;
        create type order_status as enum ('new', 'paid', 'shipped');
        create table customers (id integer generated by default as identity
          primary key, email varchar(40) not null, name varchar(10) not null
          {'' if loose else ', constraint uq_email unique (email)'});
        create table orders (id integer generated by default as identity
          primary key, customer_id integer not null, amount numeric(8,2)
          not null, status order_status not null default 'new',
          note varchar(10),
          constraint amount_ck check
            ({'amount > -100' if loose else 'amount >= 0'}),
          constraint orders_customer_fk foreign key (customer_id)
            references customers (id)
            {'' if loose else 'on delete cascade'});
        """)


def _hop(tmp_path):
    hop = Hop(name="cons", engine="hetero",
              options={"source_engine": "mysql", "target_engine": "postgres",
                       "proof_sandbox": {
                           "source": {"host": "127.0.0.1",
                                      "port": SB_MY_PORT, "user": "root",
                                      "password": "test"},
                           "target": {"host": "127.0.0.1",
                                      "port": SB_PG_PORT,
                                      "user": "postgres",
                                      "password": "test"}}},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _on_a_bench(tmp_path, work):
    from migkit.engines.hetero import HeteroEngine
    from migkit.storedcode.bench import Bench
    from migkit.storedcode.prove import Proof
    p = Proof(HeteroEngine(_hop(tmp_path)), "cx")
    p.open(True)
    assert p.sandbox is not None, p.why_not
    bench = Bench(p)
    try:
        return work(bench)
    finally:
        bench.close()
        p.close()


def _source_state():
    return _my("select * from customers order by id",
               "select * from orders order by id")


@needs_docker
def test_a_faithful_target_answers_every_probe_alike(servers, tmp_path):
    _target(loose=False)
    before = _source_state()
    got = _on_a_bench(tmp_path, constraints.prove)
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    m = re.match(r"(\d+) probes over 2 tables answered alike: ",
                 got[0].detail)
    assert m and int(m.group(1)) > 0, got[0].detail
    # the hop's own servers were only read
    assert _source_state() == before
    assert _pg("select count(*) from orders") == "0"


@needs_docker
def test_a_loosened_target_is_named_by_its_first_differing_probe(
        servers, tmp_path):
    _target(loose=True)
    got = _on_a_bench(tmp_path, constraints.prove)
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    detail = got[0].detail
    assert detail.startswith(
        "an insert into customers repeating an existing row's email:"
        " refused on the source (23000) and accepted on the target; "), \
        detail
    assert "an insert into orders with amount = -0.01" in detail, detail
    assert "a delete of a customers row that orders rows point at" in \
        detail, detail


@needs_docker
def test_a_value_the_source_refuses_the_loosened_target_takes(
        servers, tmp_path):
    _target(loose=True)
    x, y, what = _on_a_bench(tmp_path, lambda bench: constraints.probe_row(
        bench, "orders", {"amount": Decimal("-1")}))
    assert what == "an insert into orders with amount = -1"
    assert x.error is not None and x.error[0] == "23514", x
    assert y.error is None, y
    assert "+" in y.changes.get("orders", {}), y
