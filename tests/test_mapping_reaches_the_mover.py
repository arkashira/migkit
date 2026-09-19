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
