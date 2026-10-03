"""A repair never takes back what the hop keeps, and a delta verify does
not hold its position back for it.

A hop whose `mapping.ops` carries no deletes keeps on the target the rows
the source deleted; one that carries no inserts leaves the source's new
rows off it. The check names those rows as it names every other (the
drilldown's key files), and every repair read those files whole: `sync`
deleted the archive's rows the hop exists to keep, and a span or a table
copied again whole took them back with everything else. The rows a hop
keeps (`verdict.kept_kinds`) are left out of every repair - by key, by
span, whole - and said in the plan; a delta cycle whose only rows apart
are those is clean, and its position moves on.
"""
import json

from migkit import repairways
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine, Result


def _hop(tmp_path, ops=None, engine="postgres"):
    h = Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
            mapping={"ops": ops} if ops else {})
    h.report_dir = lambda db="": tmp_path
    return h


class _Rows(Engine):
    """An engine that keeps a drilldown and nothing else."""
    CANON_ENGINE = "postgres"


def _named(tmp_path, table, **kinds):
    for kind, keys in kinds.items():
        (tmp_path / f"data-{table}.{kind}").write_text(
            "".join(json.dumps([k]) + "\n" for k in keys))


NO_DELETES = {"public.orders": "insert, update",
              "public.lines": "insert, update"}


def test_the_rows_named_for_a_repair_leave_the_kept_ones_out(tmp_path):
    eng = _Rows(_hop(tmp_path, NO_DELETES))
    _named(tmp_path, "public.orders", extra=["7", "8"], missing=["1"])
    got = eng.repair_found("db", "public.orders")
    assert got["extra"] == [] and got["missing"] == [("1",)]
    # a hop that carries every change repairs every row named
    whole = _Rows(_hop(tmp_path)).repair_found("db", "public.orders")
    assert whole["extra"] == [("7",), ("8",)]


def test_the_plan_skips_a_table_whose_rows_are_all_kept(tmp_path):
    eng = _Rows(_hop(tmp_path, NO_DELETES))
    _named(tmp_path, "public.orders", extra=["7", "8"])
    _named(tmp_path, "public.lines", extra=["3"], missing=["4"])
    plan = eng._rows_plan("db", "from the source")
    assert [a.table for a in plan] == ["public.lines"], plan
    said = " | ".join(plan[0].statements)
    assert "1 row the source deleted left as they are - deletes are not" \
           " carried" in said
    assert "delete" not in said.replace("deleted", "").replace(
        "deletes", ""), said
    assert "copy 1 rows" in said


def test_inserts_not_carried_are_never_added(tmp_path):
    eng = _Rows(_hop(tmp_path, {"public.orders": "update, delete"}))
    _named(tmp_path, "public.orders", missing=["1"], extra=["9"])
    plan = eng._rows_plan("db", "from the source")
    said = " | ".join(plan[0].statements)
    assert "copy" not in said and "delete 1 rows" in said, said
    assert "the source inserted left as they are" in said


def test_a_span_or_the_table_copied_again_is_ruled_out(tmp_path):
    """A span emptied and copied again takes back what the hop keeps:
    such a table is repaired row by row, as `newer_wins` keeps one."""
    class Stub:
        pass
    ladder = Stub()
    ladder.db, ladder.table = "db", "public.orders"
    ladder.target = _Rows(_hop(tmp_path, NO_DELETES))
    why = repairways.Ladder._per_row(ladder)
    assert "deletes are not carried" in why and "row by row" in why
    ladder.target = _Rows(_hop(tmp_path))
    assert repairways.Ladder._per_row(ladder) == ""


def test_a_delta_cycle_whose_rows_apart_are_kept_moves_on(tmp_path):
    eng = _Rows(_hop(tmp_path, {"public.d": "insert, update"}))
    got, held_back = eng._delta_held(Result(
        "delta", "db.public.d", "diff",
        "of 3 touched rows: missing=0 extra=1 changed=0"))
    assert not held_back and got.status == "warn"
    assert got.kept_by_hop.startswith("kept by the hop: 1 row the source"
                                      " deleted")
    assert eng._delta_said(got) == "kept by the hop"
    got, held_back = eng._delta_held(Result(
        "delta", "db.public.d", "diff",
        "of 3 touched rows: missing=1 extra=1 changed=0"))
    assert held_back and got.status == "diff"
    # a hop that carries every change is held back as before
    got, held_back = _Rows(_hop(tmp_path))._delta_held(Result(
        "delta", "db.public.d", "diff",
        "of 3 touched rows: missing=0 extra=1 changed=0"))
    assert held_back and got.status == "diff"
