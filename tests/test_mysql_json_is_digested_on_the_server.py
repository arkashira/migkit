"""A MySQL JSON column is digested by the server where the server writes
a document as canon does, and folded in this process only where it does
not - the digest the same either way (backlog P0-DEPTH 7).

Every cross-engine proof read a JSON column's every row into this
process: at a million rows the copy took 8 s and its proof 24-26 s.
MySQL keeps an object's keys by their length and then their bytes and
writes `", "` and `": "` between - canon's order and spelling - so a
document of whole numbers, plain strings and literals is canon's text as
the server writes it; one holding a fraction, an exponent or an escape is
folded here (`MySQLEngine._json_as_is`).
"""
import pytest

from migkit import canon
from migkit.engines.mysql import MySQLEngine
from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("mysql")

DOCS = [
    '{"b": 1, "a": [1, 2, {"zz": null, "y": true}], "ccc": "x"}',
    '{"name": "über", "n": -17, "big": 18446744073709551615}',
    '[]', '{}', 'null', '"just text"', '12',
    # folded here: a fraction, an exponent, an escape, MySQL's double
    '{"price": 1.10}', '{"e": 1e20}', '{"s": "line\\nbreak"}',
    '{"q": "say \\"hi\\""}', '[3.0, 2]',
    # a string that only looks like a number is folded too, never
    # differently
    '{"version": "1.2"}',
    # the duplicate key: the last given wins, as canon reads it
    '{"k": 1, "k": 2}',
]


@pytest.fixture
def eng(pair, tmp_path):
    from migkit.config import Hop
    pair.src.sql("drop table if exists j; create table j (id int primary"
                 " key, doc json)", pair.name)
    for i, doc in enumerate(DOCS):
        # the document as a string literal: a backslash is MySQL's escape
        text = doc.replace("\\", "\\\\").replace("'", "''")
        pair.src.sql(f"insert into j values ({i}, '{text}')", pair.name)
    pair.src.sql(f"insert into j values ({len(DOCS)}, null)", pair.name)
    hop = Hop(name="j", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda d=None: tmp_path
    got = MySQLEngine(hop)
    yield got
    got.release_kept()


COLS = [("id", "integer"), ("doc", "json")]


def _folded(eng, db, where=None):
    """The digest every row folded in this process: what it was."""
    return canon.fold_batches(COLS, eng.neutral_batches(
        "src", db, "j", COLS, 5000, where=where))


def test_the_server_and_the_fold_give_one_digest(eng, pair, monkeypatch):
    db = pair.name
    folded = _folded(eng, db)
    read = []
    real = MySQLEngine.neutral_batches

    def counted(self, *a, **k):
        for batch in real(self, *a, **k):
            read.extend(batch)
            yield batch
    monkeypatch.setattr(MySQLEngine, "neutral_batches", counted)
    got = eng.neutral_digest("src", db, "j", COLS)
    assert (int(got[0]), str(got[1])) == (int(folded[0]), str(folded[1]))
    assert got[0] == len(DOCS) + 1
    # only the documents the server does not write as canon came here
    assert 0 < len(read) <= 8, read
    assert {r[0] for r in read} >= {7, 8, 9, 10, 11}, read


def test_a_row_filter_is_held_on_both_parts(eng, pair):
    db = pair.name
    where = "`id` between 2 and 9"
    assert eng.neutral_digest("src", db, "j", COLS, where=where) == \
        _folded(eng, db, where)


@pytest.mark.parametrize("changed", [
    '{"b": 1, "a": [1, 2, {"zz": null, "y": false}], "ccc": "x"}',
    '{"price": 1.2}',
])
def test_a_changed_document_changes_the_digest(eng, pair, changed):
    """Where the server digests the row and where it is folded here."""
    db = pair.name
    before = eng.neutral_digest("src", db, "j", COLS)
    row = 0 if '"zz"' in changed else 7
    pair.src.sql(f"update j set doc = '{changed}' where id = {row}", db)
    after = eng.neutral_digest("src", db, "j", COLS)
    assert after != before
    assert after == _folded(eng, db)


def test_a_server_whose_json_is_not_mysqls_folds_it_all(eng, pair,
                                                        monkeypatch):
    db = pair.name
    monkeypatch.setattr(eng, "_json_on_server", lambda side: False)
    assert eng.neutral_digest("src", db, "j", COLS) == _folded(eng, db)
