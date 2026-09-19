"""The row filter is pushed down, not applied afterwards.

A filter the mover does not know about means every row crosses the wire
and is thrown away at the far end - and, worse, that the load and the
check are working from two different ideas of which rows belong.

mydumper's `--where` is one predicate for the whole dump. The per-table
form lives in a config file, and it works: verified against mydumper
v1.0.5 on a live MySQL, with a rule on `orders` and none on `people`.

    [`appdb`.`orders`]
    where = region = 'apac'

    appdb.orders  INSERT INTO `orders` VALUES(1,"apac"),(3,"apac");
    appdb.people  INSERT INTO `people` VALUES(1,"a"),(2,"b");

Three of the three `orders` rows existed; two were dumped. `people`, which
no rule names, came through whole. That measurement is what this file
pins, plus the thing that matters more: a hop with no mapping builds the
command it has always built.
"""
import pytest

from migkit.config import Endpoint, Hop


def _hop(mapping=None, tmp_path=None):
    hop = Hop(name="mv", engine="mysql",
              source=Endpoint(host="s", port=3306, user="u", password="p"),
              target=Endpoint(host="t", port=3306, user="u", password="p"),
              databases=["appdb"], mapping=mapping or {})
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    return hop


def test_no_mapping_writes_no_config_at_all():
    from migkit import movers
    assert movers.mydumper_defaults(_hop(), "appdb") is None


def test_a_rule_becomes_a_section_mydumper_reads():
    from migkit import movers
    got = movers.mydumper_defaults(
        _hop({"where": {"orders": "region = 'apac'"}}), "appdb")
    assert got.startswith("[mydumper]\n"), got
    assert "[`appdb`.`orders`]" in got, got
    assert "where = region = 'apac'" in got, got


def test_a_rule_about_another_database_is_left_alone():
    """`where` keys are right-anchored like every other name in a hop, so
    `other.orders` is not this database's orders."""
    from migkit import movers
    got = movers.mydumper_defaults(
        _hop({"where": {"other.orders": "x", "appdb.people": "active"}}),
        "appdb")
    assert "people" in got, got
    assert "orders" not in got, got


def test_the_dry_run_shows_the_flag_and_says_why(tmp_path):
    """The plan an operator reads before `--go` has to mention it, or the
    filtering is a surprise discovered in the row counts."""
    from migkit import movers
    steps = movers.mydumper_move(
        _hop({"where": {"orders": "region = 'apac'"}}, tmp_path),
        "appdb", 2, False, None)
    dump = steps[0]
    assert "--defaults-file" in dump, dump
    assert "row filters from the hop's mapping" in dump, dump


def test_without_a_mapping_the_command_is_exactly_what_it_was(tmp_path):
    """The invariant that matters most here: every hop in use has no
    mapping, and none of them may grow a flag because this exists."""
    from migkit import movers
    steps = movers.mydumper_move(_hop(None, tmp_path), "appdb", 2, False,
                                 None)
    assert "--defaults-file" not in steps[0], steps[0]
    assert steps[0].endswith("--no-schemas --trx-consistency-only"), steps[0]


def test_the_config_is_written_beside_the_dump_not_inside_it(tmp_path):
    """myloader is pointed at the dump directory. A file it does not read
    has no business being in there."""
    from migkit import movers
    hop = _hop({"where": {"orders": "region = 'apac'"}}, tmp_path)
    movers.mydumper_move(hop, "appdb", 2, False, None)
    steps = movers.mydumper_move(hop, "appdb", 2, False, None)
    path = [w for w in steps[0].split() if w.endswith(".cnf")][0]
    assert path.endswith("mydumper-filters.cnf"), path
    assert "/mydumper/" not in path, path


def test_the_predicate_is_passed_through_unchanged():
    """Quoting a predicate for the operator would be migkit deciding what
    their SQL means. It goes in as written and the server parses it."""
    from migkit import movers
    hairy = "created_at >= '2024-01-01' and status in ('a','b')"
    got = movers.mydumper_defaults(_hop({"where": {"orders": hairy}}),
                                   "appdb")
    assert f"where = {hairy}" in got, got


def test_postgres_movers_have_no_row_filter_and_the_move_refuses():
    """Measured, not assumed. `pg_dump --help` on 18.6 offers `-t`, `-T`,
    `--exclude-table-data` and `--filter`; `pgcopydb clone --help` on 0.18
    offers `--filters`. Every one of them selects *tables*. Neither tool
    has a row predicate anywhere.

    The quiet failure that makes this a refusal rather than a warning: the
    mover would copy every row, and `check` - reading the same mapping -
    would then compare a filtered source against a full target and report
    the difference for ever. The move appears to work and the verification
    never goes green.
    """
    from migkit import movers
    hop = _hop({"where": {"orders": "region = 'apac'"}})
    for via in ("pgdump", "pgcopydb"):
        with pytest.raises(SystemExit) as e:
            movers.refuse_unpushable_filters(hop, "appdb", via)
        said = str(e.value)
        assert "orders" in said, said
        assert "never by row" in said or "not rows" in said, said
        # and it offers the way out rather than only saying no
        assert "view the hop points at" in said, said


def test_the_mover_that_can_do_it_is_not_refused():
    from migkit import movers
    hop = _hop({"where": {"orders": "region = 'apac'"}})
    movers.refuse_unpushable_filters(hop, "appdb", "mydumper")


def test_the_builtin_mover_is_refused_too_and_says_so_plainly():
    """migkit's own copy path applies no predicate either. Naming pg_dump
    in that message would be telling the operator about a tool that is not
    running."""
    from migkit import movers
    with pytest.raises(SystemExit) as e:
        movers.refuse_unpushable_filters(
            _hop({"where": {"orders": "x"}}), "appdb", "builtin")
    said = str(e.value)
    assert "builtin mover applies no row predicate" in said, said
    assert "pg_dump" not in said, said


def test_a_hop_with_no_row_filters_is_never_refused():
    """Which is every hop in use. The refusal must be reachable only by
    asking for something the tool cannot do."""
    from migkit import movers
    for via in ("pgdump", "pgcopydb", "mydumper", "builtin"):
        movers.refuse_unpushable_filters(_hop(), "appdb", via)


def test_a_filter_on_another_database_does_not_block_this_one():
    from migkit import movers
    hop = _hop({"where": {"other.orders": "x"}})
    movers.refuse_unpushable_filters(hop, "appdb", "pgdump")


def _pghop(exclude=(), tmp_path=None):
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=15545, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=15546, user="postgres",
                              password="test"),
              databases=["appdb"], exclude=list(exclude))
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    return hop


def test_the_exclude_list_becomes_a_pgcopydb_filter_file():
    """Verified against pgcopydb 0.18 on a live source: with
    `[exclude-table] public.audit_log`, `pgcopydb list tables --filters`
    returned two of the three tables; without the file it returned three.
    """
    from migkit import movers
    got = movers.pgcopydb_filters(
        _pghop(["audit_log", "public.tmp_*"]), "appdb",
        ["public.orders", "public.audit_log", "public.tmp_a",
         "public.people"])
    assert got.startswith("[exclude-table]\n"), got
    assert "public.audit_log" in got and "public.tmp_a" in got, got
    assert "public.orders" not in got and "public.people" not in got, got


def test_patterns_are_resolved_by_the_hop_not_re_interpreted():
    """`exclude` already has a meaning - right-anchored, shell wildcards -
    and pgcopydb wants concrete names. Resolving through `hop.excluded()`
    is what keeps one rule instead of two that can disagree."""
    from migkit import movers
    hop = _pghop(["appdb.public.*"])
    got = movers.pgcopydb_filters(hop, "appdb", ["public.orders"])
    assert "public.orders" in got, got
    # the same pattern against a different database matches nothing
    assert movers.pgcopydb_filters(hop, "other", ["public.orders"]) is None


def test_a_hop_with_no_exclude_writes_no_filter_file():
    from migkit import movers
    assert movers.pgcopydb_filters(_pghop(), "appdb",
                                   ["public.orders"]) is None


def test_a_pattern_matching_nothing_produces_no_file():
    from migkit import movers
    assert movers.pgcopydb_filters(_pghop(["ghost"]), "appdb",
                                   ["public.orders"]) is None


def test_pg_dump_gets_the_same_excluded_tables(tmp_path, monkeypatch):
    """`pg_dump -T` verified live: `-T public.audit_log --data-only`
    dumped `COPY public.orders` and nothing from audit_log.

    It is given resolved names rather than the hop's patterns on purpose.
    `pg_dump -T` has its own pattern language and pgcopydb's filter file
    has none, so passing the raw pattern to each would give the two movers
    two different sets - and `check`, which asks `hop.excluded()`, a third.
    """
    from migkit import movers
    hop = _pghop(["audit_log"], tmp_path)

    class FakeEngine:
        def __init__(self, hop):
            pass

        def neutral_tables(self, side, db):
            return ["public.orders", "public.audit_log"]

    import migkit.engines.postgres as pg
    monkeypatch.setattr(pg, "PostgresEngine", FakeEngine)
    steps = movers.pgdump_move(hop, "appdb", 2, False, None)
    dump = [s for s in steps if s.startswith("pg_dump")][0]
    assert "-T public.audit_log" in dump, dump
    assert "-T public.orders" not in dump, dump
    assert any("1 tables the hop excludes are not dumped" in s
               for s in steps), steps


def test_pg_dump_without_an_exclude_list_is_untouched(tmp_path):
    from migkit import movers
    steps = movers.pgdump_move(_pghop([], tmp_path), "appdb", 2, False, None)
    dump = [s for s in steps if s.startswith("pg_dump")][0]
    assert " -T " not in dump, dump


def test_a_source_that_cannot_be_listed_says_so_instead_of_filtering(
        tmp_path, monkeypatch):
    """Silently dumping everything when the hop asked for less is the
    failure worth a line in the plan."""
    from migkit import movers

    class Boom:
        def __init__(self, hop):
            pass

        def neutral_tables(self, side, db):
            raise RuntimeError("connection refused")

    import migkit.engines.postgres as pg
    monkeypatch.setattr(pg, "PostgresEngine", Boom)
    steps = movers.pgdump_move(_pghop(["audit_log"], tmp_path), "appdb", 2,
                               False, None)
    assert any("could not list the source's tables" in s for s in steps), steps
    dump = [s for s in steps if s.startswith("pg_dump")][0]
    assert " -T " not in dump, dump


def test_both_postgres_movers_exclude_the_same_set(tmp_path):
    """The point of resolving once. Whatever `check` skips, both movers
    skip, and they skip it identically."""
    from migkit import movers
    hop = _pghop(["audit_log", "public.tmp_*"], tmp_path)
    tables = ["public.orders", "public.audit_log", "public.tmp_a"]
    names = movers.excluded_tables(hop, "appdb", tables)
    ini = movers.pgcopydb_filters(hop, "appdb", tables)
    assert names == ["public.audit_log", "public.tmp_a"], names
    for n in names:
        assert n in ini, (n, ini)
        assert hop.excluded("appdb", *n.split("."))
