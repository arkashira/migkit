"""MySQL digests its JSON on the server where its version writes canon's
text, the table digest takes the hash the pair's probe chose, and a JSON
key is read as the server's text (backlog P0-DEPTH 7, R20.10).

MySQL's JSON was folded in this process on every proof: its `json`
column's text orders an object's keys as `jsonb` does, and from 8.0.4 its
`regexp_replace` writes each number as its value. A double it writes with
an exponent (`1e20`), or a run of digits the fold here rounds, is folded
here, as is a server before 8.0.4 or MariaDB, with the reason said.
"""
import hashlib
import re

import pytest

from migkit import canon, leaves, runlog


def _literals(sql):
    """The string constants of a MySQL statement, as the server reads
    them under its default sql_mode."""
    return [x.replace("''", "'").replace("\\\\", "\\")
            for x in re.findall(r"'((?:[^'\\]|\\.|'')*)'", sql)]


def _replaced(sql, text):
    """MySQL's two replaces of `json_sql`, run here on a server's text."""
    one, both, two, again = _literals(sql)
    assert both == again == "$1$2"
    sub = lambda m: (m.group(1) or "") + (m.group(2) or "")  # noqa: E731
    return re.sub(two, sub, re.sub(one, sub, text))


@pytest.mark.parametrize("version,ok", [
    ("8.4.3", True), ("8.0.4", True), ("8.0.36-log", True),
    ("8.0.3-rc", False), ("5.7.44", False), ("11.4.2-MariaDB", False),
    ("8.0.11-TiDB-v7.5.0", False), ("", False), (None, False),
])
def test_mysql_writes_json_from_8_0_4(version, ok):
    got, why = canon.json_on_server("mysql", ["json"], version=version)
    assert got is ok
    assert (why is None) is ok
    if not ok:
        assert why == canon.JSON_UNPROVED["mysql"]
    # a text column holding JSON is not one the server orders
    assert not canon.json_on_server("mysql", ["longtext"],
                                    version="8.4.3")[0]


def test_the_mysql_json_text_in_sql_is_canons_text():
    sql = canon.json_sql("mysql", "`j`")
    assert sql == canon.expr("mysql", "j", "json")
    assert sql.startswith("regexp_replace(regexp_replace(cast(`j` as char)")
    for served in ['{"a": 1.10, "b": [2.0, 100, -0.50, 0.0]}',
                   '{"s": "1.50 and 2.0 in a string", "t": "x\\"1.0"}',
                   '12.500', '[]', '{}', 'null', '"2.0"',
                   '{"k": {"y": null, "z": true}, "é": "ü"}']:
        assert _replaced(sql, served) == canon.json_text(served), served
    # a number in a string is never touched
    assert _replaced(sql, '"1.50"') == '"1.50"'


def test_a_number_mysql_writes_otherwise_is_folded_here():
    guard = canon.json_guard("mysql", "t")
    assert guard.startswith("(t regexp ")
    find, = _literals(guard)
    assert re.search(find, '{"n": 1e20}')
    assert re.search(find, '[1.5e-7]')
    assert re.search(find, "1" * canon.JSON_DIGITS)
    assert not re.search(find, '{"a": 1.5, "b": "x"}')
    # canon writes the digits a double's exponent stands for
    assert canon.json_text("1e20") == "100000000000000000000"
    assert canon.json_guard("postgres", "t") == "(t ~ '[0-9.]{29,}')"


def test_the_fold_takes_the_chosen_hash():
    text = "5:hello"
    for way in ("md5", "sha256", "sha1"):
        assert canon.digest_step(0, text, way) == \
            leaves.text_hash("", text, way)
    assert canon.digest_step(0, text, "sha256") != canon.digest_step(0, text)
    rows = [[1, "a"], [2, "b"]]
    cols = [("id", "integer"), ("v", "text")]
    md5 = canon.fold_batches(cols, [rows])
    sha = canon.fold_batches(cols, [rows], "sha256")
    assert md5 == canon.fold_batches(cols, [rows], "md5")
    assert md5[0] == sha[0] == 2 and md5[1] != sha[1]
    want = hashlib.sha256(b"x").hexdigest()[:canon.DIGEST_HEX]
    assert canon.digest_step(0, "x", "sha256") == int(want, 16)


class _Ran:
    """A MySQL engine's server, answering what a test says."""

    def __init__(self, version, rounds=0, digest=("3", "42")):
        self.version, self.rounds, self.digest = version, rounds, digest
        self.sqls = []

    def __call__(self, side, sql, *a):
        self.sqls.append(sql)
        if sql == "select version()":
            return [(self.version,)]
        return [(self.digest[0], self.digest[1], self.rounds)]


def _mysql(monkeypatch, version, rows=(), **kw):
    from migkit.engines.mysql import MySQLEngine
    eng = object.__new__(MySQLEngine)
    ran = _Ran(version, **kw)
    monkeypatch.setattr(eng, "_q", ran, raising=False)
    monkeypatch.setattr(eng, "_d", lambda side, db: db, raising=False)
    monkeypatch.setattr(eng, "_where", lambda where: "", raising=False)
    monkeypatch.setattr(eng, "neutral_columns",
                        lambda side, db, t: [("id", "int"), ("j", "json")],
                        raising=False)
    monkeypatch.setattr(eng, "neutral_batches",
                        lambda *a, **k: iter([[list(r) for r in rows]]),
                        raising=False)
    return eng, ran


COLS = [("id", "integer"), ("j", "json")]
ROWS = [(1, '{"a": 1.1}'), (2, '"5"'), (3, None)]


@pytest.mark.parametrize("way,hashed", [("md5", "md5("),
                                        ("sha256", "sha2(")])
def test_mysql_digests_json_in_sql_by_the_hash_chosen(monkeypatch, way,
                                                      hashed):
    eng, ran = _mysql(monkeypatch, "8.4.3")
    assert eng.neutral_digest("src", "db", "t", COLS, way=way) == (3, "42")
    asked = ran.sqls[-1]
    assert hashed in asked and "regexp_replace(" in asked
    assert "cast(`j` as char) regexp" in asked
    assert ran.sqls.count("select version()") == 1


def test_mysql_folds_a_document_its_server_writes_otherwise(monkeypatch):
    eng, _ = _mysql(monkeypatch, "8.4.3", rows=ROWS, rounds=1)
    assert eng.neutral_digest("src", "db", "t", COLS, way="sha256") == \
        canon.fold_batches(COLS, [[list(r) for r in ROWS]], "sha256")


def test_mysql_before_8_0_4_is_folded_here(monkeypatch):
    eng, ran = _mysql(monkeypatch, "5.7.44", rows=ROWS)
    assert eng.neutral_digest("src", "db", "t", COLS) == \
        canon.fold_batches(COLS, [[list(r) for r in ROWS]])
    assert ran.sqls == ["select version()"]


def test_postgres_takes_the_chosen_hash_and_canons_json(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    eng = object.__new__(PostgresEngine)
    said = []
    monkeypatch.setattr(eng, "_json_in_sql", lambda *a: ["j"], raising=False)
    monkeypatch.setattr(eng, "_psql", lambda side, db, sql: said.append(sql)
                        or "3\x1f42\x1ffalse", raising=False)
    assert eng.neutral_digest("src", "db", "public.t", COLS,
                              way="sha256") == (3, "42")
    assert "sha256(convert_to(" in said[0]
    assert canon.json_sql("postgres", '"j"') in said[0]
    assert canon.json_guard("postgres", '"j"') in said[0]
    # folded here by the same hash
    monkeypatch.setattr(eng, "_json_in_sql", lambda *a: None, raising=False)
    monkeypatch.setattr(eng, "neutral_columns", lambda *a: [("j", "jsonb")],
                        raising=False)
    monkeypatch.setattr(eng, "neutral_batches",
                        lambda *a, **k: iter([[list(r) for r in ROWS]]),
                        raising=False)
    assert eng.neutral_digest("src", "db", "public.t", COLS,
                              way="sha256") == \
        canon.fold_batches(COLS, [[list(r) for r in ROWS]], "sha256")


def test_postgres_reads_constants_from_canon():
    from migkit.engines.postgres import PostgresEngine
    for gone in ("JSON_IN_SQL", "JSON_DIGITS", "_json_text_sql"):
        assert not hasattr(PostgresEngine, gone)


def test_a_json_key_read_as_text_orders_by_its_value(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    eng = object.__new__(PostgresEngine)
    monkeypatch.setattr(eng, "neutral_key", lambda *a: ["j"], raising=False)
    sql, _, _, _ = eng._read_query("src", "db", "public.t", [("j", "json")],
                                   None, None, None, ("j",))
    assert '"j"::text' in sql
    assert sql.endswith('order by "public"."t"."j"')


class _MySide:
    CANON_ENGINE = "mysql"
    SQL_DIALECT = "mysql"

    def __init__(self, version):
        self.version, self.sqls = version, []

    def json_version(self, side):
        return self.version

    def _json_in_sql(self, side, db, table, columns):
        ok, _ = canon.json_on_server("mysql", ["json"],
                                     version=self.version)
        return [n for n, c in columns if c == "json"] if ok else None

    def run_rule(self, side, db, sql):
        self.sqls.append(sql)
        return []

    def _qualified(self, side, db, table):
        return f"`db`.`{table}`"

    def _quote_ident(self, name):
        return f"`{name}`"


@pytest.fixture
def said(monkeypatch):
    got = []
    monkeypatch.setattr(runlog, "event",
                        lambda name, msg="", **kw: got.append(kw))
    return got


def test_a_mysql_side_of_8_hashes_its_json_in_sql(said):
    eng = _MySide("8.4.3")
    s = leaves.Side(eng, "src", "db", "t", list(COLS), ["id"])
    assert s.sql and s.folded is None and said == []
    asked, = eng.sqls
    assert "regexp" in asked and "limit 1" in asked
    inner = leaves._inner(s, leaves.Spec("hash", salt="ab"))
    assert canon.json_sql("mysql", "`j`") in inner


def test_a_mysql_side_of_5_7_is_folded_and_says_why(said):
    eng = _MySide("5.7.44")
    s = leaves.Side(eng, "src", "db", "t", list(COLS), ["id"])
    assert not s.sql
    assert s.folded == canon.JSON_UNPROVED["mysql"]
    assert eng.sqls == []
    assert said[0]["reason"] == canon.JSON_UNPROVED["mysql"]


def test_a_json_key_is_read_as_the_servers_text():
    s = leaves.Side(None, "src", "db", "t", [("j", "json"), ("v", "json"),
                                             ("n", "integer")], ["j"])
    assert s.as_text() == ("j", "v")


# --- a MySQL server (docker; not run mid-way) --------------------------------

from tests import servers  # noqa: E402
from tests.conftest import needs_docker  # noqa: E402

pair = servers.fixture("mysql")

#: documents whose text MySQL's rendering and canon's could disagree on
DOCS = [
    '{"b": 1, "a": 2}', '{"a": 1.10, "aa": [1.0, 100, -0.50]}',
    '{"k": "1.50 in a string", "q": "a \\"quoted\\" \\\\ back"}',
    '{"nested": {"z": {"y": [true, false, null]}, "é": "ü"}}',
    '{"dup": 1, "dup": 2}', '"top level string"', '12.500', '[]', '{}',
    '{"tab": "a\\tb", "nl": "x\\ny", "ctl": "\\u0001"}', 'null',
    '{"long": "' + "x" * 300 + '", "s": "short"}',
]
#: a double MySQL writes with an exponent: folded here, said by the guard
EXPONENT = '{"exp": 1.5e10, "small": 1e-7, "big": 100000000000000000000}'


@pytest.fixture
def my(pair, tmp_path):
    from migkit.config import Hop
    from migkit.engines.mysql import MySQLEngine
    pair.src.sql("drop table if exists docs; create table docs (id int"
                 " primary key, j json)", pair.name)
    for i, d in enumerate(DOCS + [EXPONENT]):
        doc = "'" + d.replace("\\", "\\\\").replace("'", "''") + "'"
        pair.src.sql(f"insert into docs values ({i}, {doc})", pair.name)
    hop = Hop(name="json", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda d=None: tmp_path
    got = MySQLEngine(hop)
    yield got
    got.release_kept()


@needs_docker
def test_the_mysql_servers_json_is_canons_text(pair, my):
    assert canon.json_on_server("mysql", ["json"],
                                version=my.json_version("src"))[0]
    rows = pair.src.sql("select cast(j as char), "
                        + canon.json_sql("mysql", "`j`") + ", "
                        + canon.json_guard("mysql", "cast(j as char)")
                        + " from docs order by id", pair.name)
    flagged = [raw for raw, _, guard in rows if int(guard)]
    assert len(flagged) == 1 and "e" in flagged[0]
    for raw, served, guard in rows:
        if not int(guard):
            assert served == canon.json_text(raw), (raw, served)


@needs_docker
@pytest.mark.parametrize("way", ["md5", "sha256"])
def test_the_mysql_server_digests_json_as_the_fold_does(pair, my,
                                                        monkeypatch, way):
    cols = [("id", "integer"), ("j", "json")]
    where = f"id < {len(DOCS)}"
    on_server = my.neutral_digest("src", pair.name, "docs", cols,
                                  where=where, way=way)
    with monkeypatch.context() as m:
        m.setattr(my, "_json_in_sql", lambda *a: None)
        folded = my.neutral_digest("src", pair.name, "docs", cols,
                                   where=where, way=way)
        whole_folded = my.neutral_digest("src", pair.name, "docs", cols,
                                         way=way)
    assert on_server == folded and on_server[0] == len(DOCS)
    # the exponent document is folded here, and the digest is the fold's
    assert my.neutral_digest("src", pair.name, "docs", cols,
                             way=way) == whole_folded


@needs_docker
@pytest.mark.parametrize("row, changed", [
    (0, '{"b": 1, "a": 3}'),
    (1, '{"a": 1.2, "aa": [1.0, 100, -0.50]}'),
])
def test_a_changed_document_changes_the_digest(pair, my, monkeypatch, row,
                                               changed):
    """A document the server writes, changed, changes the table's digest,
    and the digest stays the fold's."""
    cols = [("id", "integer"), ("j", "json")]
    where = f"id < {len(DOCS)}"
    before = my.neutral_digest("src", pair.name, "docs", cols, where=where)
    pair.src.sql(f"update docs set j = '{changed}' where id = {row}",
                 pair.name)
    after = my.neutral_digest("src", pair.name, "docs", cols, where=where)
    assert after != before and after[0] == before[0]
    monkeypatch.setattr(my, "_json_in_sql", lambda *a: None)
    assert my.neutral_digest("src", pair.name, "docs", cols,
                             where=where) == after
