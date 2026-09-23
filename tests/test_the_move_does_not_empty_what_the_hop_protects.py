"""The move emptied the one table the hop said to protect.

`exclude` is documented on `Hop.excluded` as protecting "target-owned tables
(rows written on the target, not the source) from being deleted". The dump
honoured it - `pg_dump -T`, and pgcopydb's filter file - so those tables were
never carried. The step *before* the dump did not: the target was emptied by
one statement built straight from the catalogue, with no idea the hop existed.

Measured on a pair whose target held two rows no source ever had:

    target audit_log before: 2
    $ migkit move pg --go
      # 1 tables the hop excludes are not dumped at all
      pg_dump ... -T public.audit_log
      pg_restore ...
      pgdump reported success and appdb is still empty on the target:
      public.audit_log
    target audit_log after: 0

Three things in one run: the rows the setting exists to keep were deleted, the
plan announced it was protecting the table while doing it, and the closing
line blamed the copy for a table the copy had been told to skip.

**Leaving a table out of the statement is not enough.** The truncate carries
`cascade`, and PostgreSQL follows foreign keys into tables nobody named:

    truncate table public.orders cascade;
    NOTICE:  truncate cascades to table "audit_log"

- a notice it emits *while* emptying it. The same catalogue answers the
question beforehand, so the move can stop instead. It refuses rather than
choosing for the operator, because both ways out are decisions about their
data: the table is not really target-owned, or the reference has to go.
"""
import ast
import pathlib

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _hop(pg_pair, exclude=(), tmp_path=None):
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], exclude=list(exclude))
    if tmp_path is not None:
        hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _seed(pg_pair):
    """A source with one row per table, and a target holding rows of its own
    in the table the hop protects."""
    for sql in ("create table orders (id int primary key, v text)",
                "create table audit_log (id int primary key, note text)"):
        assert psql(pg_pair["src"], sql).returncode == 0
        assert psql(pg_pair["dst"], sql).returncode == 0
    psql(pg_pair["src"], "insert into orders values (1,'src')")
    psql(pg_pair["src"], "insert into audit_log values (1,'src-audit')")
    psql(pg_pair["dst"], "insert into audit_log values (7,'own'),(8,'own2')")


def _count(pg_pair, table):
    got = psql(pg_pair["dst"], f'select count(*) from public."{table}"')
    assert got.returncode == 0, got.stderr
    return int(got.stdout.strip())


def test_an_excluded_table_keeps_the_rows_it_owns(pg_pair):
    """The regression. Before this, the same call left it at 0."""
    _seed(pg_pair)
    assert _count(pg_pair, "audit_log") == 2
    movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]), "postgres")
    assert _count(pg_pair, "audit_log") == 2, "the protected rows were deleted"
    assert _count(pg_pair, "orders") == 0, "the carried table was not emptied"


def test_a_whole_move_carries_one_table_and_leaves_the_other_alone(pg_pair,
                                                                   tmp_path):
    """End to end, not just the truncate: the excluded table must still be
    holding its own rows once the copy has run."""
    _seed(pg_pair)
    hop = _hop(pg_pair, ["audit_log"], tmp_path)
    movers.pgdump_move(hop, "postgres", 2, True, None)
    assert _count(pg_pair, "audit_log") == 2
    assert _count(pg_pair, "orders") == 1, "the source row was not carried"


def test_a_hop_with_no_exclude_still_empties_everything(pg_pair):
    """The feature nobody turned on has to behave exactly as it did."""
    _seed(pg_pair)
    stmt = movers._pg_truncate_target(_hop(pg_pair), "postgres")
    assert "public.orders" in stmt and "public.audit_log" in stmt, stmt
    assert stmt.startswith("truncate table ") and stmt.endswith(" cascade")
    assert _count(pg_pair, "audit_log") == 0
    assert _count(pg_pair, "orders") == 0


def test_a_target_with_no_tables_produces_no_statement(pg_pair):
    assert movers._pg_truncate_target(_hop(pg_pair), "postgres") == ""


def test_everything_excluded_leaves_the_target_untouched(pg_pair):
    _seed(pg_pair)
    assert movers._pg_truncate_target(_hop(pg_pair, ["*"]), "postgres") == ""
    assert _count(pg_pair, "audit_log") == 2
    assert _count(pg_pair, "orders") == 0


def test_a_cascade_reaching_the_excluded_table_is_refused(pg_pair):
    """PostgreSQL would empty it through the foreign key and say so
    afterwards. Asked first, it says so in time."""
    _seed(pg_pair)
    assert psql(pg_pair["dst"], "alter table audit_log add column oid int"
                " references orders(id)").returncode == 0
    with pytest.raises(SystemExit) as e:
        movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]), "postgres")
    said = str(e.value)
    assert "public.audit_log" in said, said
    assert "would be emptied anyway" in said, said
    assert "exclude list" in said, said
    assert _count(pg_pair, "audit_log") == 2, "it refused too late"
    assert _count(pg_pair, "orders") == 0, "nothing else was emptied either"


def test_the_cascade_is_followed_through_more_than_one_hop(pg_pair):
    """`a -> b -> c`: emptying `c` reaches `a`, which no single join finds."""
    _seed(pg_pair)
    for sql in ("create table mid (id int primary key,"
                " oid int references orders(id))",
                "alter table audit_log add column mid int references mid(id)"):
        assert psql(pg_pair["dst"], sql).returncode == 0
    with pytest.raises(SystemExit) as e:
        movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]), "postgres")
    assert "public.audit_log" in str(e.value)


def test_a_reference_between_two_carried_tables_is_not_a_refusal(pg_pair):
    """Both ends are being replaced, so the cascade changes nothing. A check
    that fired here would block every ordinary schema."""
    _seed(pg_pair)
    assert psql(pg_pair["dst"], "create table lines (id int primary key,"
                " oid int references orders(id))").returncode == 0
    stmt = movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]),
                                      "postgres")
    assert "public.lines" in stmt and "public.audit_log" not in stmt, stmt
    assert _count(pg_pair, "audit_log") == 2


def test_a_reference_out_of_the_excluded_table_is_not_a_refusal(pg_pair):
    """Direction matters: the protected table *being pointed at* by a table
    that gets emptied costs it nothing."""
    _seed(pg_pair)
    assert psql(pg_pair["dst"], "alter table orders add column aid int"
                " references audit_log(id)").returncode == 0
    movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]), "postgres")
    assert _count(pg_pair, "audit_log") == 2


def test_a_table_name_holding_a_quote_survives(pg_pair):
    """The names go into the cascade query as a literal, because `psql -c`
    does not interpolate `:'var'` - and a table name may contain the quote
    character."""
    _seed(pg_pair)
    for port in (pg_pair["src"], pg_pair["dst"]):
        assert psql(port, "create table \"it's\" (id int primary key)"
                    ).returncode == 0
    stmt = movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]),
                                      "postgres")
    assert "\"it's\"" in stmt, stmt
    assert _count(pg_pair, "audit_log") == 2


def test_the_literal_doubles_the_quote():
    assert movers._pg_literal("a") == "'a'"
    assert movers._pg_literal("it's") == "'it''s'"
    assert movers._pg_literal("") == "''"


# ---- the plan, and where the answer is decided ----

def test_the_plan_line_tracks_what_the_truncate_does():
    """A plan that says "all user tables" beside a run that skips some is how
    a reader concludes the protected table was refilled."""
    plain = movers._truncate_step(Hop(name="x", engine="postgres",
                                      source=None, target=None))
    assert "all the target's" in plain, plain
    guarded = movers._truncate_step(Hop(name="x", engine="postgres",
                                        source=None, target=None,
                                        exclude=["audit_log"]))
    assert "except the ones the hop excludes" in guarded, guarded


def _callers_of(name):
    """Which functions in `movers.py` call `name`.

    Counting the substring instead reads the `def` line as a call site, and
    the assertion then passes or fails for a reason that has nothing to do
    with either bulk path.
    """
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    out = set()
    for fn in ast.walk(ast.parse(src)):
        if not isinstance(fn, ast.FunctionDef):
            continue
        for node in ast.walk(fn):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == name):
                out.add(fn.name)
    return out


def test_both_bulk_paths_use_the_one_wording():
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    assert "truncate all user tables" not in src, "a second wording is back"
    assert _callers_of("_truncate_step") == {"pgdump_move", "pgcopydb_move",
                                             "mydumper_move"}


def test_the_exclusion_is_resolved_through_the_shared_reader():
    """`hop.exclude` is fnmatch; the dump and `check` resolve it through
    `excluded_tables`. A second reading here is how the dump skips one set
    and the truncate empties another."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef)
              and n.name == "_pg_truncate_target")
    called = {n.func.id for n in ast.walk(fn) if isinstance(n, ast.Call)
              and isinstance(n.func, ast.Name)}
    assert "excluded_tables" in called, sorted(called)


def test_both_bulk_paths_still_empty_the_target():
    """The truncate is what keeps a data-only load from doubling every row.
    Dropping the call would make every test above pass."""
    assert _callers_of("_pg_truncate_target") == {"pgdump_move",
                                                  "pgcopydb_move"}


def test_the_refusal_names_no_tool(pg_pair):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    _seed(pg_pair)
    assert psql(pg_pair["dst"], "alter table audit_log add column oid int"
                " references orders(id)").returncode == 0
    with pytest.raises(SystemExit) as e:
        movers._pg_truncate_target(_hop(pg_pair, ["audit_log"]), "postgres")
    said = str(e.value).lower()
    assert not [t for t in TOOLS if t in said], said


# ---- the index window, which touched the same tables ----

def _indexed(pg_pair):
    _seed(pg_pair)
    for sql in ("create index orders_v on orders (v)",
                "create unique index audit_note_u on audit_log (note)"):
        assert psql(pg_pair["dst"], sql).returncode == 0


def test_the_index_window_leaves_an_excluded_table_alone(pg_pair, tmp_path):
    """Measured before the fix, with the application writing to the table
    it owns while the window was open:

        REBUILD FAILED for audit_ref_u: Key (ref)=(r7) is duplicated.
        2 of 3 indexes rebuilt; STILL MISSING: audit_ref_u

    A unique index built with `CREATE UNIQUE INDEX` is not a constraint, so
    the window took it off the one table the hop said not to touch, let
    the duplicate in, and could not put it back."""
    _indexed(pg_pair)
    lines = []
    with movers._IndexWindow(_hop(pg_pair, ["audit_log"], tmp_path),
                             "postgres", 2, lines.append) as w:
        got = psql(pg_pair["dst"], "insert into audit_log values (9,'own')")
        assert got.returncode != 0, "the duplicate got in while it was open"
        assert "duplicate key" in got.stderr, got.stderr
    assert w.dropped == ["orders_v"], w.dropped
    assert any("tables the hop excludes were left in place" in ln
               for ln in lines), lines


def test_without_an_exclude_the_window_is_what_it_was(pg_pair, tmp_path):
    """The hop nobody changed still gets the faster load on every table."""
    _indexed(pg_pair)
    with movers._IndexWindow(_hop(pg_pair, (), tmp_path), "postgres", 2,
                             None) as w:
        pass
    assert sorted(w.dropped) == ["audit_note_u", "orders_v"], w.dropped


def test_both_index_windows_consult_the_exclusion():
    """Scoped to the class: both windows name their entry `__enter__`, so a
    search by function name could not tell which one it had found."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    tree = ast.parse(src)
    for cls in ("_IndexWindow", "_MyIndexWindow"):
        node = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.ClassDef) and n.name == cls)
        calls = {c.func.id for c in ast.walk(node)
                 if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)}
        assert "_outside_exclusion" in calls, cls
