"""The leaves' own JSON fold is the PostgreSQL server's digest (backlog
P0-DEPTH.json): a side whose server writes canon's JSON text hashes its
JSON in SQL, one whose server cannot is folded here and says why, and the
two give the same hash of the same document.

The leaves folded every JSON table in this process, reading each
document as the driver parsed it: a JSON string `"5"` came as the text
`5`, which the fold then read as the number 5, and a JSON `null` as SQL's
NULL - two different documents one digest. A document holding a number
of 29 or more digits is folded here on every side, since this process
keeps 28 of them; MySQL's JSON in SQL is not yet held to canon's text.
"""
from decimal import Decimal

import pytest

from migkit import canon, leaves, rowtext, runlog


class _PG:
    """A PostgreSQL-like side: its server's JSON as canon's text
    (`_json_in_sql`), a document of too many digits where `rounds`."""
    CANON_ENGINE = "postgres"
    SQL_DIALECT = "postgres"

    def __init__(self, rounds=False, json_ok=True, rows=()):
        self.rounds, self.json_ok, self.rows = rounds, json_ok, list(rows)
        self.sqls, self.as_text = [], None

    def _json_in_sql(self, side, db, table, columns):
        return [n for n, c in columns if c == "json"] if self.json_ok \
            else None

    def run_rule(self, side, db, sql):
        self.sqls.append(sql)
        return [[1]] if self.rounds and "[0-9.]{29,}" in sql else []

    def _qualified(self, side, db, table):
        return f'"public"."{table}"'

    def _quote_ident(self, name):
        return f'"{name}"'

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None, as_text=()):
        self.as_text = tuple(as_text)
        yield [list(r) for r in self.rows]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        raise AssertionError("a folded JSON side is read as text")


class _MySQL(_PG):
    CANON_ENGINE = "mysql"
    SQL_DIALECT = "mysql"


COLS = [("id", "integer"), ("j", "json")]


def _side(eng):
    return leaves.Side(eng, "src", "db", "t", list(COLS), ["id"])


@pytest.fixture
def said(monkeypatch):
    got = []
    monkeypatch.setattr(runlog, "event",
                        lambda name, msg="", **kw: got.append((name, msg,
                                                               kw)))
    return got


def test_the_json_text_in_sql_is_written_in_one_place():
    from migkit.engines.postgres import PostgresEngine
    sql = canon.json_sql("postgres", '"j"')
    assert sql == canon.expr("postgres", "j", "json")
    assert sql == PostgresEngine._json_text_sql('"j"')
    assert sql.startswith("regexp_replace(regexp_replace(to_jsonb(")
    assert canon.json_sql("mysql", "`j`") is None
    # one engine's own JSON keeps its own text
    assert canon.expr("postgres", "j", canon.JSON_ONE) == \
        'to_jsonb("j")::text'


def test_a_string_five_is_not_the_number_five():
    assert canon.json_text('"5"') == '"5"'
    assert canon.json_text("5") == "5"
    assert canon.json_text('{"v": "5"}') != canon.json_text('{"v": 5}')
    h = leaves.Hasher(leaves.Spec("hash", bits=4, salt="ab"),
                      ["integer", "json"], [0])
    assert h.of([1, '{"v": "5"}'])[1] != h.of([1, '{"v": 5}'])[1]
    assert h.of([1, '"5"'])[1] != h.of([1, "5"])[1]
    # the same document in either server's text is one hash: MySQL's
    # `{"v": "5"}` and `jsonb`'s, keys in another order and spacing
    assert h.of([1, '{"w":1.10,"v":"5"}'])[1] == \
        h.of([1, '{"v": "5", "w": 1.1}'])[1]


def test_a_json_null_is_not_an_sql_null():
    assert canon.render_value("json", "null") == "null"
    assert canon.render_value("json", None) is None
    assert rowtext.encode([canon.render_value("json", "null")]) == "4:null"
    assert rowtext.encode([None]) == "N:"
    h = leaves.Hasher(leaves.Spec("hash", salt="ab"), ["integer", "json"],
                      [0])
    assert h.of([1, "null"])[1] != h.of([1, None])[1]


def test_a_side_whose_server_writes_canons_json_hashes_it_in_sql(said):
    eng = _PG()
    s = _side(eng)
    assert s.sql and s.folded is None
    asked, = eng.sqls
    assert "[0-9.]{29,}" in asked and "limit 1" in asked
    inner = leaves._inner(s, leaves.Spec("hash", salt="ab"))
    assert canon.json_sql("postgres", '"j"') in inner
    assert said == []
    # decided once a side
    assert s.sql and len(eng.sqls) == 1


def test_a_29_digit_number_falls_to_python_and_says_why(said):
    long = "1234567890.1234567890123456789"
    assert len(long.replace(".", "")) == 29
    assert canon.json_rounds('{"n": %s}' % long)
    # a run of 28 digits and points stays in SQL
    assert not canon.json_rounds('{"n": 123456789.123456789012345678}')
    assert canon.json_rounds("[%s]" % long.replace(".", ""))
    # this process keeps 28 digits of a fraction, the server every one
    assert canon.json_text(long) != long
    assert canon.json_text(long) == canon.render_value(canon.NUMBER,
                                                       Decimal(long))
    s = _side(_PG(rounds=True))
    assert not s.sql
    assert s.folded == canon.JSON_ROUNDED
    assert "29 or more digits" in s.folded and "28" in s.folded
    (name, msg, kw), = said
    assert name == "leaf json" and kw["reason"] == canon.JSON_ROUNDED
    assert "folded in this process" in msg and kw["side"] == "src"


def test_mysql_json_is_folded_here_and_says_why(said):
    eng = _MySQL()
    s = _side(eng)
    assert not s.sql
    assert s.folded == canon.JSON_UNPROVED["mysql"]
    assert eng.sqls == []           # nothing asked of a server not proved
    assert said[0][2]["reason"] == canon.JSON_UNPROVED["mysql"]


def test_a_side_of_another_class_folded_here_says_its_class(said):
    s = leaves.Side(_PG(), "dst", "db", "t", [("id", "integer"),
                                              ("x", "xml")], ["id"])
    assert not s.sql
    assert "xml" in s.folded


def test_a_folded_side_reads_its_json_as_the_servers_text(said):
    eng = _PG(json_ok=False, rows=[(1, '"5"'), (2, "null"), (3, None)])
    s = _side(eng)
    assert not s.sql
    assert "declared type or the database's encoding" in s.folded
    spec = leaves.Spec("hash", bits=2, salt="ab")
    tree = leaves.scan(s, spec)
    assert eng.as_text == ("j",)    # never the key
    assert tree.root[0] == 3
    h = leaves.Hasher(spec, ["integer", "json"], [0])
    want = leaves.Tree()
    for row in eng.rows:
        b, rh, kh = h.of(list(row))
        want.add(b, 1, rh, kh)
    assert tree.leaves == want.leaves
    # the number 5 is another leaf sum than the string "5"
    five = _PG(json_ok=False, rows=[(1, "5"), (2, "null"), (3, None)])
    assert leaves.scan(_side(five), spec).root != tree.root


def test_a_wrapper_handing_keywords_on_reads_as_its_engine_does():
    class Base:
        def neutral_batches(self, side, db, table, columns, size=1000,
                            where=None, as_text=()):
            yield []

    class Masked(Base):
        def neutral_batches(self, side, db, table, columns, *a, **kw):
            yield from super().neutral_batches(side, db, table, columns,
                                               *a, **kw)

    class Old:
        def neutral_batches(self, side, db, table, columns, size=1000,
                            where=None):
            yield []
    assert leaves._reads_text(Base()) and leaves._reads_text(Masked())
    assert not leaves._reads_text(Old())


def test_canon_says_where_a_server_writes_json():
    assert canon.json_on_server("postgres", ["json", "JSONB", "hstore"]) \
        == (True, None)
    ok, why = canon.json_on_server("postgres", ["text"])
    assert not ok and "text" in why
    ok, why = canon.json_on_server("postgres", ["jsonb"], "LATIN1")
    assert not ok and "LATIN1" in why
    assert canon.json_on_server("postgres", ["jsonb"], "UTF8")[0]
    assert canon.json_on_server("mysql", ["json"]) == \
        (False, canon.JSON_UNPROVED["mysql"])
    assert canon.limits("postgres").json_digits == canon.JSON_DIGITS == 29
    assert canon.limits("mysql").json_types is None
    assert canon.json_guard("mysql", "j") is None


@pytest.mark.parametrize("engine,way,want", [
    ("postgres", "md5", "md5(convert_to(r, 'UTF8'))"),
    ("postgres", "sha256", "encode(sha256(convert_to(r, 'UTF8')), 'hex')"),
    ("mysql", "md5", "md5(r)"), ("mysql", "sha256", "sha2(r, 256)"),
    ("mysql", "sha1", "sha1(r)"),
])
def test_the_table_digest_follows_the_hash_chosen(engine, way, want):
    assert want in canon.digest_expr(engine, "r", way)
    assert canon.digest_expr(engine, "r") == canon.digest_expr(engine, "r",
                                                               "md5")


@pytest.mark.parametrize("engine,way", [("postgres", "sha1"),
                                        ("sqlite", "sha256")])
def test_a_hash_the_sql_has_not_is_refused(engine, way):
    with pytest.raises(ValueError):
        canon.digest_expr(engine, "r", way)


def test_the_leaves_and_the_table_digest_hash_alike():
    for engine, hashes in canon.DIGEST_HASHES.items():
        for way in hashes:
            assert leaves._h(engine, "r", "", way) is not None
            assert way in leaves.HASHES
    assert leaves._h("postgres", "r", "", "sha1") is None


# --- a MySQL and PostgreSQL pair (docker; not run mid-way) -------------------

from tests.conftest import needs_docker  # noqa: E402
from tests.test_one_scan_leaves_on_servers import (  # noqa: E402,F401
    InProcess, _hetero, _twin, servers)

#: (id, document): row 2 is the string "5" on the source and the number 5
#: on the target; every other the same document in each server's text
PAIR_DOCS = [
    (1, '{"b": 1, "a": 2}'), (2, '{"v": "5"}'), (3, 'null'),
    (4, '{"p": 1.10, "q": [1.0, 100, -0.50]}'), (5, '"top level"'),
    (6, '{"é": "ü", "k": {"z": true, "y": null}}'), (7, '[]'),
]


def _docs_sql(rows, cast):
    return ", ".join("({}, {})".format(i, cast.format(
        "'" + d.replace("'", "''") + "'")) for i, d in rows)


def _pair(tmp_path, target_docs):
    _twin("create table jd (id int primary key, j json);"
          f" insert into jd values {_docs_sql(PAIR_DOCS, '{}')};",
          "create table public.jd (id bigint primary key, j jsonb);"
          " insert into public.jd values"
          f" {_docs_sql(target_docs, '{}::jsonb')};")
    eng = _hetero(tmp_path)
    sc, dc, _ = eng._column_plan("jd", "public.jd", "cx")
    return leaves.sides(eng, "cx", "jd", "public.jd", sc, dc, (None, None))


def _here(s):
    return leaves.Side(InProcess(s.eng), s.side, s.db, s.table, s.cols,
                       s.key)


@needs_docker
def test_a_string_five_and_the_number_five_differ_across_the_pair(
        servers, tmp_path, said):
    target = [(i, '{"v": 5}' if i == 2 else d) for i, d in PAIR_DOCS]
    a, b = _pair(tmp_path, target)
    assert not a.sql and a.folded == canon.JSON_UNPROVED["mysql"]
    assert b.sql and b.folded is None
    spec = leaves.Spec("hash", bits=3, salt="c0ffee")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    # the target's leaves in SQL are its fold here
    assert tb.leaves == leaves.scan(_here(b), spec).leaves
    assert len(ta.differing(tb)) == 1
    found = leaves.leaf_rows(a, spec, ta.differing(tb))
    assert [k[0] for k in found.keyed] == ["2"]


@needs_docker
def test_the_same_documents_are_one_digest_both_ways(servers, tmp_path,
                                                     said):
    a, b = _pair(tmp_path, PAIR_DOCS)
    spec = leaves.Spec("hash", bits=3, salt="c0ffee")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    assert ta.root[0] == tb.root[0] == len(PAIR_DOCS)
    assert ta.leaves == tb.leaves == leaves.scan(_here(b), spec).leaves


@needs_docker
def test_a_29_digit_number_on_the_server_is_folded_here_and_said(
        servers, tmp_path, said):
    long = '{"n": 1234567890.1234567890123456789}'
    docs = PAIR_DOCS + [(8, long)]
    _twin("create table jd (id int primary key, j json);"
          f" insert into jd values {_docs_sql(docs, '{}')};",
          "create table public.jd (id bigint primary key, j jsonb);"
          f" insert into public.jd values {_docs_sql(docs, '{}::jsonb')};")
    eng = _hetero(tmp_path)
    sc, dc, _ = eng._column_plan("jd", "public.jd", "cx")
    a, b = leaves.sides(eng, "cx", "jd", "public.jd", sc, dc, (None, None))
    assert not b.sql and b.folded == canon.JSON_ROUNDED
    assert any(kw.get("reason") == canon.JSON_ROUNDED for _, _, kw in said)
    spec = leaves.Spec("hash", bits=3, salt="c0ffee")
    assert leaves.scan(a, spec).leaves == leaves.scan(b, spec).leaves
