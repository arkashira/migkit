"""A second implementation, asked the same question.

migkit's verifier is the part of this tool that everything else rests on,
and it is the part nobody audits - five false negatives were found inside
it in a single day. pgcopydb ships its own `compare data`, reading the same
two databases by a different route. Running both and comparing the two
*verdicts* is the cheapest audit available, and it needs no new dependency:
pgcopydb is already wrapped as a mover.

Measured on pgcopydb 0.18 before this was written, on four pairs, and it
agreed every time:

    identical rows                   both: same
    numeric 1.0 against 1.00         both: differ  (equal by `=`, not as
                                                    stored)
    same columns, different order    both: same    (each sorts them)
    no primary key, a missing row    both: differ

That agreement is the finding. The disagreement path is built and tested
because the day it fires, one of the two is wrong about real data - but
nothing measured so far suggests it is ours.

Off unless `MIGKIT_CROSSCHECK` asks for it: it reads both databases in
full a second time, and that is a real cost to put on every run.
"""
import pytest

from migkit.config import Endpoint, Hop

SAMPLE = """
                    Table Name | ! |    Source Checksum |    Target Checksum
-------------------------------+---+--------------------+--------------------
        postgres.public.orders |   | aaaa               | aaaa
          postgres.public.nopk | ! | bbbb               | cccc
"""


def _engine(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_the_marker_is_what_is_read_not_the_checksums(tmp_path):
    """pgcopydb already decided; reading its two hex strings again would
    be a second opinion about its own answer."""
    got = _engine(tmp_path)._crosscheck_verdicts(SAMPLE)
    assert got == {"public.orders": True, "public.nopk": False}, got


def test_the_header_and_rule_lines_are_not_tables(tmp_path):
    got = _engine(tmp_path)._crosscheck_verdicts(SAMPLE)
    assert not any("Table Name" in k or set(k) <= set("- ") for k in got), got


def test_agreement_is_reported_with_the_count(tmp_path):
    eng = _engine(tmp_path)
    got = eng._crosscheck_result("db", {"public.orders": True},
                                 {"public.orders": True}, True)
    assert got.status == "ok", got.detail
    assert "1 tables" in got.detail and "same verdict" in got.detail


def test_a_disagreement_names_both_verdicts(tmp_path):
    """The whole point. When it fires, one of the two verifiers is wrong
    about real data, and the line has to say which said what."""
    eng = _engine(tmp_path)
    got = eng._crosscheck_result("db", {"public.orders": True},
                                 {"public.orders": False}, True)
    assert got.status == "diff", got.detail
    assert "matching one way" in got.detail, got.detail
    assert "differing the other" in got.detail, got.detail
    assert "one of the two readings is wrong" in got.detail, got.detail
    assert "migkit check" in got.fix_hint, got.fix_hint


def test_a_table_only_one_of_them_saw_is_not_a_disagreement(tmp_path):
    """A filtered hop would otherwise report a clash on every table the
    other tool did not look at."""
    eng = _engine(tmp_path)
    got = eng._crosscheck_result("db", {"a": True, "b": False},
                                 {"a": True}, True)
    assert got.status == "ok", got.detail
    assert "1 tables" in got.detail, got.detail


def test_no_shared_table_is_a_skip_not_a_pass(tmp_path):
    got = _engine(tmp_path)._crosscheck_result("db", {"a": True}, {"b": True},
                                               True)
    assert got.status == "skip", got.detail
    assert got.status != "ok"


def test_pgcopydb_missing_is_a_skip_that_says_why(tmp_path):
    got = _engine(tmp_path)._crosscheck_result("db", {}, {}, False)
    assert got.status == "skip", got.detail
    assert "compared once" in got.detail, got.detail


def test_migkits_own_verdict_is_read_from_the_file_it_already_wrote(
        tmp_path):
    """Running the pass again to compare against itself would be two
    answers about the same question taken at two moments - the mistake
    this check exists to catch."""
    (tmp_path / "data-evidence.txt").write_text(
        "public.orders: OK rows=5\n"
        "public.nopk: DIFF src=3|a dst=2|b\n"
        "\n")
    got = _engine(tmp_path)._mine_from_evidence("postgres")
    assert got == {"public.orders": True, "public.nopk": False}, got


def test_without_the_evidence_file_there_is_nothing_to_second_guess(
        tmp_path):
    assert _engine(tmp_path)._mine_from_evidence("postgres") == {}


def test_it_is_off_unless_asked_for(tmp_path, monkeypatch):
    """Every existing run reads both databases once. This would make it
    twice, so it is opt-in - and an environment variable rather than a
    flag, the way `MIGKIT_MOVER` is."""
    monkeypatch.delenv("MIGKIT_CROSSCHECK", raising=False)
    assert _engine(tmp_path)._crosscheck("postgres") is None
    for value in ("0", "", "no"):
        monkeypatch.setenv("MIGKIT_CROSSCHECK", value)
        assert _engine(tmp_path)._crosscheck("postgres") is None
