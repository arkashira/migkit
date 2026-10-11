"""A column written other than as it was read, and proved as written.

A masked column used to be either compared (and every row called
different) or left out (and nothing proved of it). Each transform is one
function both readers take: the copy writes it, the check asks it of the
source's value and compares with the target's. Its SQL forms are held to
the Python one on the same values - on paper here, on the servers below.
"""
import hashlib

import pytest

from migkit import transforms
from tests import servers
from tests.conftest import needs_docker

KEY = "k3y-for-tests"
VALUES = ["alice@example.com", "", "ข้อมูลไทย", "O'Brien", None]


def test_each_transform_writes_what_it_says():
    assert transforms.apply("null", "x") is None
    assert transforms.apply("constant masked@example.com", "x") == \
        "masked@example.com"
    assert transforms.apply("constant masked@example.com", None) == \
        "masked@example.com"
    assert transforms.apply("truncate 3", "abcdef") == "abc"
    assert transforms.apply("truncate 3", None) is None
    want = hashlib.sha256((KEY + "alice").encode()).hexdigest()
    assert transforms.apply("hash", "alice", KEY) == want
    # the same value, the same digest: a join on it still lines up
    assert transforms.apply("hash", "alice", KEY) == \
        transforms.apply("hash", "alice", KEY)
    assert transforms.apply("hash", None, KEY) is None


def test_what_cannot_be_proved_or_is_said_wrong_is_refused():
    for rule in ("random", "shuffle", "constant", "constant it's",
                 "truncate", "truncate 0", "truncate x", "hash sha1",
                 "null now"):
        with pytest.raises(SystemExit):
            transforms.parse(rule)
    with pytest.raises(SystemExit, match="mask_key"):
        transforms.apply("hash", "alice", None)


def test_the_sql_forms_say_the_same_function():
    # a NULL of the column's own type, so its class renders it as it would
    assert transforms.sql("null", '"c"', "postgres") == \
        'CASE WHEN 1 = 0 THEN "c" END'
    assert "utf8mb4" in transforms.sql("hash", "`c`", "mysql", KEY)
    assert transforms.sql("constant x", '"c"', "mysql") == "'x'"
    assert transforms.sql("truncate 4", "`c`", "mysql") == "LEFT(`c`, 4)"
    assert "sha256" in transforms.sql("hash", '"c"', "postgres", KEY)
    # a key is never left out of a digest, and an engine whose SQL cannot
    # take it alike leaves it to Python
    assert transforms.sql("hash", '"c"', "postgres") is None
    assert transforms.sql("hash", "[c]", "mssql", KEY) is None
    assert transforms.text_only("hash") and not transforms.text_only("null")


pg = servers.fixture("pg")
my = servers.fixture("mysql")


@needs_docker
@pytest.mark.parametrize("rule", ["hash", "truncate 4", "null",
                                  "constant masked"])
def test_postgres_computes_what_python_computes(pg, rule):
    for v in VALUES:
        lit = "null" if v is None else "'" + v.replace("'", "''") + "'"
        got = pg.src.sql(f"select {transforms.sql(rule, f'({lit}::text)', 'postgres', KEY)}",
                         pg.name)[0][0]
        assert got == transforms.apply(rule, v, KEY), (rule, v)


@needs_docker
@pytest.mark.parametrize("rule", ["hash", "truncate 4", "null",
                                  "constant masked"])
def test_mysql_computes_what_python_computes(my, rule):
    for v in VALUES:
        lit = "null" if v is None else "'" + v.replace("'", "''") + "'"
        got = my.src.sql(f"select {transforms.sql(rule, f'CAST({lit} AS CHAR CHARACTER SET utf8mb4)', 'mysql', KEY)}",
                         my.name)[0][0]
        assert got == transforms.apply(rule, v, KEY), (rule, v)


# ---- one masking rule language, read in one place (`Hop.masks`) ------------

def _hop(engine="postgres", masks=None, **options):
    from migkit.config import Endpoint, Hop
    opts = dict(options)
    if masks is not None:
        opts["mask_on_write"] = masks
    return Hop(name="m", engine=engine, source=Endpoint(host="10.0.0.1"),
               target=Endpoint(host="10.0.0.2"), databases=["appdb"],
               options=opts)


def test_a_rule_names_its_table_as_every_name_in_the_hop_is_named():
    """`public.orders.email` names PostgreSQL's appdb.public.orders: read as
    `db.table.column` and skipped, it masked nothing and the real values
    moved."""
    hop = _hop(masks={"public.orders.email": "null"})
    assert hop.masks("appdb", "public", "orders") == {"email": ("null", "")}
    assert hop.masks("appdb", "public.orders") == {"email": ("null", "")}
    assert hop.masks("appdb", "crm", "orders") == {}
    for key in ("orders.email", "appdb.public.orders.email", "*.email",
                "public.ord*.email"):
        assert _hop(masks={key: "null"}).masks("appdb", "public", "orders") \
            == {"email": ("null", "")}, key
    my = _hop("mysql", masks={"shop.people.email": "constant x"})
    assert my.masks("shop", "people") == {"email": ("constant", "x")}
    assert my.masks("crm", "people") == {}


def test_the_most_particular_rule_wins_per_column():
    hop = _hop(masks={"*.email": "null", "public.orders.email": "truncate 3",
                      "orders.name": "constant n"})
    assert hop.masks("appdb", "public", "orders") == {
        "email": ("truncate", 3), "name": ("constant", "n")}
    assert hop.masks("appdb", "public", "people") == {"email": ("null", "")}


def test_names_without_case_where_the_engine_has_none():
    ora = _hop("oracle", masks={"hr.people.email": "null"})
    assert ora.masks("db", "HR", "PEOPLE") == {"email": ("null", "")}
    assert ora.masks("db", "HR", "PEOPLE", columns=["ID", "EMAIL"]) == {
        "EMAIL": ("null", "")}
    pg = _hop(masks={"public.people.email": "null"})
    assert pg.masks("appdb", "public", "PEOPLE") == {}
    assert pg.masks("appdb", "public", "people", columns=["Email"]) == {}


def test_an_unknown_kind_raises_and_the_old_spelling_is_read():
    with pytest.raises(SystemExit):
        _hop(masks={"orders.email": "shuffle"}).masks("appdb", "public",
                                                      "orders")
    assert _hop(masks={"orders.email": "constant:x y"}).masks(
        "appdb", "public", "orders") == {"email": ("constant", "x y")}
    assert transforms.parse(("truncate", 4)) == ("truncate", 4)
    assert transforms.apply("truncate 2", b"abc") == "ab"
    assert transforms.apply("hash", "a".encode(), KEY) == \
        transforms.apply("hash", "a", KEY)


def test_a_hop_that_masks_nothing_is_read_as_it_was():
    plain = _hop()
    assert plain.masks("appdb", "public", "orders") == {}
    assert not plain.masks_in("appdb")
    assert "masks" not in plain.scope_record()
    masked = _hop(masks={"orders.email": "null"})
    assert masked.scope_record()["masks"] == {"orders.email": "null"}
    assert masked.scope_fingerprint() != plain.scope_fingerprint()
    # a record taken without masking is read back as unmasked
    assert masked.under(plain.scope_record()).masks("appdb", "public",
                                                    "orders") == {}


def test_masks_in_asks_only_of_rules_that_may_name_the_database():
    my = _hop("mysql", masks={"shop.people.email": "null"})
    assert my.masks_in("shop") and not my.masks_in("crm")
    assert _hop("mysql", masks={"people.email": "null"}).masks_in("crm")


def test_the_key_is_a_secret_reference_read_once_and_hidden(monkeypatch):
    from migkit import redact
    monkeypatch.setenv("MIGKIT_TEST_MASK_KEY", KEY)
    hop = _hop(masks={"orders.email": "hash"},
               mask_key="env:MIGKIT_TEST_MASK_KEY")
    assert hop.mask_key() == KEY
    # once read, it is hidden wherever a statement holding it is said
    said = transforms.sql("hash", '"c"', "postgres", hop.mask_key())
    assert KEY in said and KEY not in redact.Redactor(hop)(said)
    assert _hop().mask_key() is None


def test_the_hop_file_is_checked_for_its_masking():
    from migkit import hopkeys

    def problems(options):
        return [(p.level, p.where, p.text) for p in hopkeys.check_hop(
            "h", {"source": "postgres://u@db.example.com/appdb",
                  "target": "postgres://u@db2.example.com/appdb",
                  "options": options})
                if "mask" in p.where]
    assert problems({"mask_on_write": {"orders.email": "null"}}) == []
    got = problems({"mask_on_write": {"orders.email": "hash"}})
    assert got and got[0][0] == "error" and "mask_key" in got[0][2], got
    got = problems({"mask_on_write": {"email": "null"}})
    assert got and "names no table" in got[0][2], got
    got = problems({"mask_on_write": {"orders.email": "random"}})
    assert got and got[0][0] == "error", got
    got = problems({"mask_on_write": {"orders.email": "hash"},
                    "mask_key": "plain-text-key"})
    assert [g for g in got if g[0] == "warn"], got
    assert problems({"mask_on_write": {"orders.email": "hash"},
                     "mask_key": "env:MASK_KEY"}) == []


# ---- every way, the pair's transforms ----------------------------------------

class _Rows:
    """A source engine as the pair reads it: rows of (id, email, name)."""
    CANON_ENGINE = "postgres"
    ROWS = [[1, "a@example.com", "alice"], [2, None, "bob"]]

    def __init__(self, hop):
        self.hop = hop

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        return [list(r) for r in self.ROWS], None

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        # some engines read their batches as reads: masked once, still
        rows, _ = self.neutral_read(side, db, table, columns)
        yield rows

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        return {(r[0],): list(r) for r in self.ROWS if (r[0],) in keys}

    def neutral_digest(self, side, db, table, columns, where=None):
        from migkit import canon
        return canon.fold_rows([c for _, c in columns], self.ROWS)


COLS = [("id", "integer"), ("email", "text"), ("name", "text")]


def test_the_pair_reads_its_source_as_the_target_is_to_hold_it():
    from migkit import canon
    from migkit.engines import hetero
    hop = _hop(masks={"public.people.email": "hash",
                      "public.people.name": "truncate 2"}, mask_key=KEY)
    hop._mask_key = KEY
    eng = hetero.masking(_Rows(hop), hop)
    want = [[1, transforms.apply("hash", "a@example.com", KEY), "al"],
            [2, None, "bo"]]
    assert eng.neutral_read("src", "appdb", "public.people", COLS)[0] == want
    # a batch read as reads is masked once, not twice
    assert list(eng.neutral_batches("src", "appdb", "public.people",
                                    COLS)) == [want]
    assert eng.neutral_rows_by_key("src", "appdb", "public.people", COLS,
                                   ["id"], [(2,)]) == {(2,): want[1]}
    assert eng.neutral_digest("src", "appdb", "public.people", COLS) == \
        canon.fold_rows([c for _, c in COLS], want)
    # the target side, and a table the hop does not mask, read as they are
    assert eng.neutral_read("dst", "appdb", "public.people", COLS)[0] == \
        _Rows.ROWS
    assert eng.neutral_read("src", "appdb", "public.orders", COLS)[0] == \
        _Rows.ROWS
    assert eng.masks_table("appdb", "public.people")
    assert not eng.masks_table("appdb", "public.orders")
    assert hetero._own_class(eng) is _Rows
    # a hop that masks nothing leaves the engine as it is
    plain = _Rows(_hop())
    assert hetero.masking(plain, plain.hop).__class__ is _Rows


def test_a_change_the_tail_carries_is_masked_as_the_copy_writes_it():
    from migkit.engines.hetero import HeteroEngine
    hop = _hop("hetero", masks={"people.email": "hash"}, mask_key=KEY,
               source_engine="postgres", target_engine="mysql")
    hop._mask_key = KEY
    eng = HeteroEngine(hop)
    got = eng._masked_change("appdb", {
        "op": "update", "table": "public.people", "key": {"id": 1},
        "values": {"id": 1, "email": "b@example.com"},
        "before": {"id": 1, "email": "a@example.com"}})
    assert got["values"]["email"] == transforms.apply("hash",
                                                      "b@example.com", KEY)
    assert got["before"]["email"] == transforms.apply("hash",
                                                      "a@example.com", KEY)
    assert got["key"] == {"id": 1}
    same = {"op": "insert", "table": "public.orders", "key": {"id": 1},
            "values": {"id": 1, "email": "x"}}
    assert eng._masked_change("appdb", same) is same


def test_the_servers_own_replication_is_refused_a_masked_table():
    from migkit import movers
    hop = _hop(masks={"orders.email": "null"})
    assert movers.stream_cannot_carry(hop, "appdb", ["public.orders",
                                                     "public.people"]) == [
        ("public.orders", "its columns are masked")]


def test_a_masked_table_goes_through_the_pair_on_every_engine():
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mssql import MSSQLEngine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    pg = PostgresEngine(_hop(masks={"orders.email": "null"}))
    assert pg.through_pair("appdb", "public.orders")
    assert not pg.made_by_pair("appdb", "public.orders")
    assert not pg.through_pair("appdb", "public.people")
    assert pg.maps_columns() and pg.copier_masks() and pg.tail_masks()
    my = MySQLEngine(_hop("mysql", masks={"orders.email": "null"}))
    assert my.through_pair("appdb", "orders") and my.tail_masks()
    ms = MSSQLEngine(_hop("mssql", masks={"orders.email": "null"}))
    assert ms.copier_masks() and ms.tail_masks()
    mongo = MongoEngine(_hop("mongodb", masks={"orders.email": "null"}))
    assert not mongo.copier_masks() and not mongo.tail_masks()
    # a hop that masks nothing goes every way it went
    assert not PostgresEngine(_hop()).maps_columns()


def test_a_copy_that_cannot_mask_is_refused_before_it_writes():
    from migkit import movers
    hop = _hop("mongodb", masks={"orders.email": "null"})
    with pytest.raises(SystemExit, match="writes what it reads"):
        movers.refuse_unmasked(hop, "appdb", "builtin")
    # a hop that masks nothing is not asked
    movers.refuse_unmasked(_hop("mongodb"), "appdb", "mongodump")


def test_an_own_repair_leaves_a_masked_table_and_says_so():
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql", masks={"dbo.orders.email": "null"}))
    left, said = eng._drills_in_scope("appdb", ["dbo.orders", "dbo.people"])
    assert left == ["dbo.people"]
    assert said is not None and "the hop masks" in said.note, said


def test_the_ways_the_servers_pass_rows_between_them_are_not_taken():
    from migkit import crossmove
    facts = {"masked": True, "held": False}
    for r in crossmove.ways():
        if r.name == crossmove.COPIER:
            continue
        need = [n for n in r.needs if "masked" in n.what]
        assert need and not need[0].holds(facts), r.name


def test_the_second_reading_leaves_a_masked_column_to_the_first():
    """It reads both sides as they are: a masked column would read as a
    difference that is the hop's own."""
    hop = _hop(masks={"orders.email": "null"})
    assert set(hop.masks("appdb", "public", "orders",
                         columns=["id", "email"])) == {"email"}


# ---- on the servers -----------------------------------------------------------

@needs_docker
def test_postgres_a_masked_table_is_copied_and_proved_through_the_pair(
        pg_pair, tmp_path, monkeypatch):
    from migkit import cli
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    from tests.conftest import psql
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists people; create table people"
                   " (id int primary key, email text, name text)")
    psql(pg_pair["src"], "insert into people values (1, 'a@example.com',"
                         " 'alice'), (2, null, 'bob'), (3, 'ข้อมูล', 'คน')")
    monkeypatch.setenv("MIGKIT_TEST_MASK_KEY", KEY)
    hop = Hop(name="masked", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              options={"mask_on_write": {"public.people.email": "hash",
                                         "people.name": "truncate 1"},
                       "mask_key": "env:MIGKIT_TEST_MASK_KEY"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    eng = PostgresEngine(hop)
    ck = cli._checkpoint(hop, tmp_path / "move.json")
    cli._copy_table_by(eng, "postgres", "public", "people", 100, ck,
                       lambda m: None)
    got = psql(pg_pair["dst"], "select email, name from people where"
                               " id = 1").stdout.strip()
    assert got == f"{transforms.apply('hash', 'a@example.com', KEY)}|a", got
    res = eng.check_data("postgres")
    assert res and all(r.status == "ok" for r in res), \
        [(r.scope, r.status, r.detail) for r in res]
    # a value written as the source holds it is a difference
    psql(pg_pair["dst"], "update people set email = 'a@example.com'"
                         " where id = 1")
    res = eng.check_data("postgres")
    assert any(r.status == "diff" for r in res), \
        [(r.scope, r.status, r.detail) for r in res]
