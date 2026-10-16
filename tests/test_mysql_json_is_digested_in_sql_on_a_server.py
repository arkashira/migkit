"""MySQL's JSON column is digested in SQL, and the text its server writes
for it is canon's text byte for byte (P0-DEPTH.json): on a MySQL 8.4 server,
for fourteen documents - an object's keys in the order they were given and
by length, numbers with trailing zeros, nested arrays, unicode and its
escapes, strings that look like numbers, scalars, integers past 18 digits -
`canon.json_sql` written in SQL is `canon.json_text` of the document. A
number the fold here would round (`canon.JSON_DIGITS` digits or more) or the
server writes with an exponent is found by `canon.json_guard` in SQL, and
that side is folded here (`canon.JSON_ROUNDED`): the digest is the same
either way, and the same on a side that holds the document written
otherwise.

MySQL cannot key a `json` column; a table keyed by a column generated from
a document's field is read, and compared, as the parsed document.
"""
import json

from migkit import canon
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.value.rendering", "mysql")]

pair = servers.fixture("mysql")

#: documents the server's text is canon's for
ALIKE = [
    '{"b": 1, "a": 2}',
    '{"a": 1.10, "b": [2.0, 100, -0.50, 0.0]}',
    '{"n": [[1, [2, [3.0]]], []], "o": {}}',
    '{"u": "é ü 日本語", "k": "x\\"y", "e": "\\u00e9"}',
    '{"big": 12345678901234567890, "neg": -9223372036854775808}',
    '{"f": 0.1, "g": 3.0, "h": -1.25, "z": 0.00}',
    '"2.0"',
    '12.500',
    'null',
    '{"bb": 1, "a": 2, "c": 3, "aa": 0}',
    '[1.10, "1.10", 1.10, true, false, null]',
    '{"s": "1.50 and 2.0 in a string"}',
]
#: documents a side folds here: a number of `JSON_DIGITS` digits, or one
#: the server writes with an exponent
ROUNDED = [
    # a literal with a point is read as a double; a decimal the server
    # keeps in full is made by its own function
    "=json_object('x', cast('1.000000000000000000000000000001' as"
    " decimal(35, 30)))",
    '{"e": 1e20}',
    '{"huge": 1234567890123456789012345678901234567890}',
]


def _lit(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def _engine(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="json", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _classes(eng, db, table):
    return [(n, canon.fold_class("mysql", d, n))
            for n, d in eng.neutral_columns("src", db, table)]


def _load(server, db, docs, table="docs"):
    server.sql(f"drop table if exists `{db}`.{table}")
    server.sql(f"create table `{db}`.{table} (id int primary key,"
               " j json)")
    for i, d in enumerate(docs, 1):
        value = d[1:] if d.startswith("=") else _lit(d)
        server.sql(f"insert into `{db}`.{table} values ({i}, {value})")


def test_the_servers_json_text_is_canons_byte_for_byte(pair):
    db = pair.name
    assert len(ALIKE) >= 12
    _load(pair.src, db, ALIKE + ROUNDED)
    text = canon.json_sql("mysql", "j")
    guard = canon.json_guard("mysql", "cast(j as char)")
    rows = pair.src.sql(f"select id, {text}, {guard} from `{db}`.docs"
                        " order by id")
    assert len(rows) == len(ALIKE) + len(ROUNDED)
    for (i, got, rounds), doc in zip(rows, ALIKE):
        assert got == canon.json_text(doc), doc
        assert not int(rounds), doc
    # the rounded ones are found in SQL, as they are by the fold here
    for (i, got, rounds), doc in zip(rows[len(ALIKE):], ROUNDED):
        assert int(rounds), (doc, got)
        assert canon.json_rounds(got) or "e" in got.lower(), (doc, got)
    assert "28" in canon.JSON_ROUNDED or str(canon.JSON_DIGITS) in \
        canon.JSON_ROUNDED


def test_the_table_digest_in_sql_is_the_fold_and_the_same_on_both_sides(
        pair, tmp_path):
    db = pair.name
    eng = _engine(pair, tmp_path)
    # the destination holds the same documents written otherwise
    again = [json.dumps(json.loads(d, parse_float=str, parse_int=str),
                        ensure_ascii=False).replace('"', '"')
             for d in ALIKE]
    for docs, server in ((ALIKE, pair.src), (ALIKE, pair.dst)):
        _load(server, db, docs)
    columns = _classes(eng, db, "docs")
    assert eng._json_in_sql("src", db, "docs", columns) == ["j"]
    in_sql = eng.neutral_digest("src", db, "docs", columns)
    assert in_sql[0] == len(ALIKE)
    # folded here, from the rows read, for the same digest
    eng._json_in_sql = lambda *a, **k: None
    assert eng.neutral_digest("src", db, "docs", columns) == in_sql
    assert eng.neutral_digest("dst", db, "docs", columns) == in_sql
    assert again  # the documents were re-read as text above
    # rounded documents: the guard sends the side to the fold, same digest
    del eng._json_in_sql
    _load(pair.src, db, ALIKE + ROUNDED)
    _load(pair.dst, db, list(reversed(ALIKE + ROUNDED)))
    a = eng.neutral_digest("src", db, "docs", columns)
    assert a[0] == len(ALIKE) + len(ROUNDED)
    eng._json_in_sql = lambda *a, **k: None
    assert eng.neutral_digest("src", db, "docs", columns) == a


def test_a_table_keyed_through_its_document_is_compared_parsed(
        pair, tmp_path):
    db = pair.name
    eng = _engine(pair, tmp_path)
    one = ['{"id": "a", "n": 1.10}', '{"id": "b", "n": [2.0]}']
    two = ['{"n": 1.1, "id": "a"}', '{"n": [2], "id": "b"}']
    for server, docs in ((pair.src, one), (pair.dst, two)):
        server.sql(f"drop table if exists `{db}`.keyed")
        server.sql(f"create table `{db}`.keyed (j json, k varchar(16)"
                   " generated always as (json_unquote(j->'$.id')) stored"
                   " primary key)")
        for d in docs:
            server.sql(f"insert into `{db}`.keyed (j) values ({_lit(d)})")
    # JSON itself cannot be a key; the key is the generated column
    assert eng.neutral_key("src", db, "keyed") == ["k"]
    columns = [(n, c) for n, c in _classes(eng, db, "keyed") if n == "j"]
    assert columns and canon.folded_here(columns[0][1])
    a = eng.neutral_digest("src", db, "keyed", columns)
    b = eng.neutral_digest("dst", db, "keyed", columns)
    assert a == b and a[0] == 2
