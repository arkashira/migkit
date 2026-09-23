"""Reading the columns of a table that has no rows yet.

`GenericEngine` is the engine for everything reladiff speaks - snowflake,
bigquery, redshift, clickhouse, oracle, trino, presto, duckdb, vertica -
and it borrows reladiff's own connection, because migkit has no driver of
its own for any of them. `_schema` turns that connection's answer into the
type classes every dialect shares, and `_apply_rows_borrowed` reads it
before it writes a single row.

It refused an empty table. Measured on PostgreSQL 16, the same DDL on both
sides, one row on the source and none on the target:

    src   Integer / String_VaryingAlphanum / Decimal / TimestampTZ
    dst   cannot read the columns of t: Table ('public','t') appears to be
          empty

So a row repair onto a target that had not been loaded yet died, with a
message that reads as though the table were missing - and an unloaded
target is the ordinary state of the thing a repair is pointed at. Measured
end to end with one row to copy: the old code refused and left 0 rows on
the target; this one copies it.

The fix is not a guess about sqeleton's internals, it is what its own
source does. `_process_table_schema` builds the column dict from
`dialect.parse_type` **first**, then samples rows to sharpen text subtypes,
and only then raises if there were no rows:

    col_dict = {row[0]: self.dialect.parse_type(path, *row) ...}
    samples = self._refine_coltypes(path, col_dict, where)
    if samples is not None and not samples:
        raise ValueError(f"Table {path} appears to be empty")
    return col_dict

With no rows there is nothing to sharpen, so the same two calls in the same
order give the same answer, minus a distinction an empty table does not
have. That shows up below as `Text` where a populated table says
`String_VaryingAlphanum`, and it is pinned rather than hidden.

Why it went unnoticed: every existing generic test seeds *both* sides -
`_seed` runs the same DDL and the same inserts against the source and the
target - so the target always had rows. The gap was not a skipped suite, it
was a state no test set up.
"""
import json

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

try:  # the library, which is all `_schema` and the repair need
    import reladiff.databases  # noqa: F401
    HAVE_RELADIFF_LIB = True
except Exception:
    HAVE_RELADIFF_LIB = False

needs_reladiff_lib = pytest.mark.skipif(
    not HAVE_RELADIFF_LIB, reason="the reladiff library is not installed")

SRC_DDL = ("create table if not exists gempty (id bigint primary key,"
           " v varchar(50), amt numeric(12,2), ts timestamptz,"
           " flag boolean)")
DST_DDL = ("create table if not exists gempty (id bigint primary key,"
           " v varchar(50), amt numeric(12,2), ts timestamptz,"
           " flag boolean)")


def _engine(pg_pair, tmp_path, table="gempty", key="id"):
    from migkit.engines.generic import GenericEngine

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": [table], "key": key})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return GenericEngine(hop)


@pytest.fixture
def seeded(pg_pair):
    """Source with a row, target with the same columns and none."""
    psql(pg_pair["src"], "drop table if exists gempty")
    psql(pg_pair["dst"], "drop table if exists gempty")
    assert psql(pg_pair["src"], SRC_DDL).returncode == 0
    assert psql(pg_pair["dst"], DST_DDL).returncode == 0
    assert psql(pg_pair["src"], "insert into gempty values (1,'a',1.50,"
                                "now(),true)").returncode == 0
    assert psql(pg_pair["dst"], "select count(*) from gempty"
                ).stdout.strip() == "0"
    yield pg_pair
    psql(pg_pair["src"], "drop table if exists gempty")
    psql(pg_pair["dst"], "drop table if exists gempty")


@needs_reladiff_lib
def test_an_empty_table_still_has_columns(seeded, tmp_path):
    """The whole bug in one assertion."""
    eng = _engine(seeded, tmp_path)
    try:
        got = eng._schema(eng._connect("dst"), "gempty")
    finally:
        eng._close()
    assert sorted(got) == ["amt", "flag", "id", "ts", "v"], sorted(got)


@needs_reladiff_lib
def test_the_types_are_the_same_apart_from_what_rows_would_sharpen(seeded,
                                                                   tmp_path):
    """Pinned rather than glossed over: the one difference an empty table
    causes is the text subtype, which is refined from sampled values."""
    from sqeleton.abcs import Boolean, NumericType, StringType
    eng = _engine(seeded, tmp_path)
    try:
        src = eng._schema(eng._connect("src"), "gempty")
        dst = eng._schema(eng._connect("dst"), "gempty")
    finally:
        eng._close()
    assert sorted(src) == sorted(dst)
    for col, kind in (("id", NumericType), ("amt", NumericType),
                      ("flag", Boolean), ("v", StringType)):
        assert isinstance(src[col], kind), (col, src[col])
        assert isinstance(dst[col], kind), (col, dst[col])
    # and the difference that is real, stated
    assert type(src["v"]).__name__ == "String_VaryingAlphanum"
    assert type(dst["v"]).__name__ == "Text"


@needs_reladiff_lib
def test_a_table_that_is_really_missing_still_says_so(seeded, tmp_path):
    """The fallback must not turn a missing table into an empty one - that
    would be the false negative this replaces the bug with."""
    eng = _engine(seeded, tmp_path)
    try:
        with pytest.raises(SystemExit) as e:
            eng._schema(eng._connect("src"), "no_such_table_here")
    finally:
        eng._close()
    said = str(e.value)
    assert "cannot read the columns" in said, said
    assert "does not exist" in said, said
    assert "appears to be empty" not in said, said


@needs_reladiff_lib
def test_rows_can_be_repaired_onto_a_target_that_is_still_empty(seeded,
                                                                tmp_path):
    """End to end on the path that was broken. The drilldown files are
    written by hand here for one reason: `_drill` shells out to the
    `reladiff` binary, and gating this on that binary is exactly what hid
    the bug. The format is the documented handover between a check and
    `migkit sync` - one JSON list per line, the canonical text of a key.
    """
    eng = _engine(seeded, tmp_path)
    (tmp_path / "data-gempty.missing").write_text(json.dumps(["1"]) + "\n")
    plan = eng.repair_plan("-", "rows")
    assert plan and "copy 1 rows" in plan[0].statements[0], plan
    eng.apply("-", plan[0])
    got = psql(seeded["dst"],
               "select id||'|'||v||'|'||amt||'|'||flag from gempty")
    assert got.stdout.strip() == "1|a|1.50|true", got.stdout


@needs_reladiff_lib
def test_the_repair_reads_the_targets_columns_before_writing(seeded,
                                                             tmp_path):
    """Why the two are connected, asserted rather than asserted-in-prose:
    the repair asks the *target* for its schema, so a target it cannot read
    is a repair that cannot run."""
    import ast
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "generic.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef)
              and n.name == "_apply_rows_borrowed")
    body = "\n".join(ast.dump(s) for s in fn.body)
    assert "_schema" in body


def test_this_path_needs_the_library_and_not_the_subprocess():
    """Which is why the tests above can run wherever the library is
    installed. `_probe` is the one that genuinely shells out, and it is
    asserted too so this is discriminating rather than vacuous."""
    import ast
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "generic.py").read_text()
    tree = ast.parse(src)
    for name in ("_connect", "_schema", "_apply_rows_borrowed"):
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == name)
        # a call to `which(...)`, not the word anywhere in a docstring
        called = [n.func.id for n in ast.walk(fn)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        assert "which" not in called, f"{name} gates on a binary"
    # and `_probe`, which genuinely does need the CLI, still asks for it -
    # so the assertion above is discriminating rather than vacuous
    probe = next(n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == "_probe")
    assert "which" in [n.func.id for n in ast.walk(probe)
                       if isinstance(n, ast.Call)
                       and isinstance(n.func, ast.Name)]
