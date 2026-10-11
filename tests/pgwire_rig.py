"""Engines of PostgreSQL's wire over a throwaway PostgreSQL server
(`tests/pg_local.py`): a source and a target as two databases of one
cluster, read and written by the engine's own class.

`dsql` is `DsqlEngine` as it is. `cockroachdb` is `CockroachEngine` with the
members PostgreSQL cannot answer put back to the shared ones (the
`shared_cockroach` fixture, which a test module imports) - the hidden
column filter (`is_hidden`, a column only CockroachDB's `information_schema`
has) and the write and the emptying of a table, which are CockroachDB's own
statements (`UPSERT`, `DELETE ... LIMIT`) - so what runs is the shared code of
`pgwire`, never CockroachDB's own statements.
"""
import itertools
import pathlib
import tempfile

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.cockroachdb import CockroachEngine
from migkit.engines.dsql import DsqlEngine

_COUNT = itertools.count(1)


@pytest.fixture(scope="module", autouse=True)
def shared_cockroach():
    """`CockroachEngine` on the surface it shares with PostgreSQL, for the
    module: the pair every engine is carried by makes its own engines of
    each side's class, so the class itself is put on the shared members -
    and put back after."""
    patch = pytest.MonkeyPatch()
    patch.setattr(CockroachEngine, "_visible", lambda self: "")
    for name in ("neutral_write", "neutral_empty", "_tried", "_pieces",
                 "_bytes"):
        patch.setattr(CockroachEngine, name, DsqlEngine.__dict__[name],
                      raising=False)
    patch.setattr(CockroachEngine, "LANES_FROM", 10 ** 9, raising=False)
    try:
        yield
    finally:
        patch.undo()


ENGINES = {"dsql": DsqlEngine, "cockroachdb": CockroachEngine}


def pair(server, tmp, name="dsql", **options):
    """(engine, source database, target database): two fresh databases of
    `server`, and the engine of `name` reading the first and writing the
    second."""
    n = next(_COUNT)
    src, dst = f"src{n}", f"dst{n}"
    server.make(src, dst)
    ep = Endpoint(host="127.0.0.1", port=server.port, user="postgres",
                  password="x")
    hop = Hop(name="rig", engine=name, source=ep, target=ep,
              databases=[src], db_map={src: dst}, **options)
    report = pathlib.Path(tmp) if tmp else pathlib.Path(tempfile.mkdtemp())

    def report_dir(db=None):
        report.mkdir(parents=True, exist_ok=True)
        return report
    hop.report_dir = report_dir
    return ENGINES[name](hop), src, dst
