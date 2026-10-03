"""PostgreSQL digests its JSON on the server, in the text canon defines,
and the digest is the one the fold in this process gives (backlog
P0-DEPTH 7); a row repair's undo is sealed where the hop keeps values
encrypted (audit 2026-10-01, SEC9).

JSON was hashed in this process on every cross-engine proof - at a
million rows the proof took three times the copy. The server writes it
now: `jsonb`'s own text, each number as its value (`1.10` is `1.1`). A
number the fold would round, or a value `jsonb` cannot hold, is folded
here, and the digest is the same either way - and a different document
is a different digest.

The undo of a row repair was written by the client program to a file of
its own, plain, whatever `at_rest` said.
"""
import pytest

from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("pg")

#: documents whose text the two renderings could disagree on
DOCS = [
    '{"b": 1, "a": 2}', '{"a": 1.10, "aa": [1.0, 100, 1e2, -0.50]}',
    '{"k": "1.50 in a string", "q": "a \\"quoted\\" \\\\ back"}',
    '{"nested": {"z": {"y": [true, false, null]}, "é": "ü"}}',
    '{"dup": 1, "dup": 2}', '"top level string"', '12.500', '[]', '{}',
    '{"tab": "a\\tb", "nl": "x\\ny", "ctl": "\\u0001"}', 'null',
    '{"exp": 1.5e10, "small": 1e-7, "zero": 0.000}',
]


def _fresh(pair):
    for server in (pair.src, pair.dst):
        server.sql(f'drop database if exists "{pair.name}" with (force)')
        server.sql(f'create database "{pair.name}"')


def _hop(pair, tmp_path, **extra):
    from migkit.config import Hop
    hop = Hop(name="json", engine="postgres", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name], **extra)
    hop.report_dir = lambda d=None: tmp_path
    return hop


@pytest.fixture
def run(pair):
    """`run(sql)`: SQL on the source's database, from outside migkit."""
    return lambda sql: pair.src.sql(sql, pair.name)


@pytest.fixture
def eng(pair, run, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _fresh(pair)
    run("create extension hstore")
    run("create table public.docs (id int primary key, j json, b jsonb,"
        " h hstore)")
    for i, d in enumerate(DOCS):
        doc = "'" + d.replace("'", "''") + "'"
        run(f"insert into public.docs values ({i}, {doc}::json,"
            f" {doc}::jsonb, 'a=>1, b=>\"two words\", c=>NULL')")
    got = PostgresEngine(_hop(pair, tmp_path))
    yield got
    got.release_kept()


COLUMNS = [("id", "integer"), ("j", "json"), ("b", "json"), ("h", "json")]


def test_the_servers_json_is_canons_text(eng, run):
    from migkit import canon
    rows = run("select b::text, " + eng._json_text_sql('"b"')
               + " from public.docs order by id")
    for raw, served in rows:
        assert served == canon.json_text(raw), (raw, served)


def test_the_server_digests_json_as_the_fold_does(pair, eng,
                                                   monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    folded = []
    real = PostgresEngine._folded_digest

    def counted(self, *a):
        folded.append(a)
        return real(self, *a)
    monkeypatch.setattr(PostgresEngine, "_folded_digest", counted)
    served = eng.neutral_digest("src", pair.name, "public.docs", COLUMNS)
    assert folded == [], "the server did not digest it"
    assert served == real(eng, "src", pair.name, "public.docs", COLUMNS,
                          None)
    assert served[0] == len(DOCS)


def test_a_different_document_is_a_different_digest(pair, eng, run):
    def digest():
        return eng.neutral_digest("src", pair.name, "public.docs", COLUMNS)
    before = digest()
    run("update public.docs set b = '{\"a\": 1.11, \"aa\": [1.0, 100,"
        " 1e2, -0.50]}' where id = 1")
    after = digest()
    assert after[0] == before[0] and after[1] != before[1]
    # one number written two ways is one value: no difference
    run("update public.docs set b = '{\"a\": 1.11, \"aa\": [1, 100.0,"
        " 100, -0.5]}' where id = 1")
    once = digest()
    run("update public.docs set b = '{\"aa\": [1.000, 1e2, 100, -0.500],"
        " \"a\": 1.110}' where id = 1")
    assert digest() == once


def test_what_the_server_cannot_say_alike_is_folded_here(pair, eng, run,
                                                          monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    folded = []
    real = PostgresEngine._folded_digest
    monkeypatch.setattr(PostgresEngine, "_folded_digest",
                        lambda self, *a: folded.append(a) or real(self, *a))
    # more digits than the fold keeps, and a `json` value `jsonb` refuses
    run("insert into public.docs values (100,"
        " '{\"x\": 0.12345678901234567890123456789}',"
        " '{\"x\": 0.12345678901234567890123456789}', null)")
    got = eng.neutral_digest("src", pair.name, "public.docs", COLUMNS)
    assert len(folded) == 1 and got[0] == len(DOCS) + 1
    run("delete from public.docs where id = 100; insert into public.docs"
        " values (101, '{\"nul\": \"\\u0000\"}', '{}', null)")
    got = eng.neutral_digest("src", pair.name, "public.docs", COLUMNS)
    assert len(folded) == 2 and got[0] == len(DOCS) + 1


def test_a_repairs_undo_is_sealed_where_values_are_kept_encrypted(
        pair, tmp_path, monkeypatch):
    """The rows a repair replaces, kept to put back, were written plain by
    a program whatever the hop's `at_rest` said."""
    import pyrage

    from migkit import evidence
    from migkit.engines.postgres import PostgresEngine
    me = pyrage.x25519.Identity.generate()
    key = tmp_path / "identity.txt"
    key.write_text(str(me) + "\n")
    # the operator's key, which reads the drilldown's sealed keys back
    monkeypatch.setenv("MIGKIT_IDENTITY", str(key))
    _fresh(pair)
    for server, rows in ((pair.src, "(1, 'a'), (2, 'b')"),
                         (pair.dst, "(1, 'stale'), (2, 'b')")):
        server.sql("create table public.t (id int primary key, v text);"
                   f" insert into public.t values {rows}", pair.name)
    hop = _hop(pair, tmp_path, options={"at_rest": {"recipients": [
        str(me.to_public())]}})
    reports = tmp_path / "reports"
    reports.mkdir()
    hop.report_dir = lambda db=None: evidence.report_path(hop, reports)
    eng = PostgresEngine(hop)
    try:
        (eng._report(pair.name) / "data-public.t.changed").write_text(
            "1\n")
        eng._repair_rows_native(pair.name, "public.t")
    finally:
        eng.release_kept()
    assert pair.dst.sql("select v from public.t where id = 1",
                        pair.name) == [("a",)]
    undo = list((reports / "undo").glob("*.rows"))
    assert len(undo) == 1, undo
    raw = undo[0].read_bytes()
    assert raw.startswith(evidence.AGE), raw[:40]
    assert b"stale" not in raw
    assert b"stale" in pyrage.decrypt(raw, [me])
