"""The normalized result contract: one vocabulary, whatever the engine."""
import json

from migkit.engines.base import Result, categorize
from migkit import verdict


class _Hop:
    name = "h"
    engine = "postgres"

    def __init__(self, tmp):
        self._tmp = tmp

    def report_dir(self, db=None):
        return self._tmp


def test_engine_synonyms_land_on_one_category():
    # the same failure, named differently by each engine
    assert (categorize("deep", "db encoding")
            == categorize("deep", "db charset")
            == "value.charset")
    assert (categorize("deep", "db nullempty")
            == categorize("deep", "db null-missing")
            == "value.null-empty")


def test_every_known_subcheck_is_classified():
    subs = ["boundary", "bson-types", "capped", "charset", "checks",
            "collation", "columns", "deferrable", "encoding", "extensions",
            "fk", "float", "generated", "grants", "indexes", "keys",
            "matviews", "narrowing", "null-missing", "nullempty",
            "partitions", "render", "rls", "seq-grants", "sharding",
            "timeshift", "triggers"]
    unclassified = [s for s in subs
                    if categorize("deep", f"db {s}").endswith("unclassified")]
    assert unclassified == []


def test_sequence_collision_is_distinct_from_parity():
    # a sequence that will collide is an outage; one that merely lags is not
    assert categorize("autoinc", "db usable") == "identity.sequence-collision"
    assert categorize("autoinc", "db parity") == "identity.sequence-parity"


def test_unknown_subcheck_falls_back_to_the_check_family():
    assert categorize("counts", "db") == "parity.row-count"
    assert categorize("newthing", "db whatever") == "newthing.unclassified"


def test_result_populates_category_and_respects_an_explicit_one():
    assert Result("deep", "db grants", "diff").category == "access.table-grants"
    assert Result("deep", "db grants", "diff",
                  category="custom.x").category == "custom.x"


def test_fingerprint_ignores_order_and_wording():
    a = [{"category": "parity.row-count", "scope": "db", "status": "diff",
          "detail": "src=5 dst=4"},
         {"category": "access.table-grants", "scope": "db g", "status": "ok"}]
    b = list(reversed([dict(r) for r in a]))
    b[-1]["detail"] = "src=9 dst=8"          # same verdict, different numbers
    assert verdict.fingerprint(a) == verdict.fingerprint(b)


def test_fingerprint_changes_when_a_status_changes():
    a = [{"category": "parity.row-count", "scope": "db", "status": "ok"}]
    b = [{"category": "parity.row-count", "scope": "db", "status": "diff"}]
    assert verdict.fingerprint(a) != verdict.fingerprint(b)


def test_status_derivation(tmp_path):
    hop = _Hop(tmp_path)
    def st(records):
        return verdict.summarize(hop, records)["status"]
    assert st([{"status": "ok", "category": "c"}]) == "same"
    assert st([{"status": "ok"}, {"status": "diff"}]) == "different"
    assert st([{"status": "diff"}, {"status": "error"}]) == "error"
    assert st([{"status": "skip"}]) == "incomplete"
    # a warn on its own is readable, not a mismatch
    assert st([{"status": "warn"}]) == "same"


def test_findings_exclude_passes_and_envelope_is_self_describing(tmp_path):
    hop = _Hop(tmp_path)
    records = [{"check": "deep", "scope": "db grants", "status": "diff",
                "detail": "12 missing", "category": "access.table-grants"},
               {"check": "counts", "scope": "db", "status": "ok",
                "category": "parity.row-count"}]
    p, env = verdict.write(hop, records)
    assert p.exists()
    on_disk = json.loads(p.read_text())
    assert on_disk == env
    assert env["format_version"] == verdict.FORMAT_VERSION
    assert env["tool"] == "migkit"
    assert env["has_differences"] is True
    assert [f["category"] for f in env["findings"]] == ["access.table-grants"]
    assert env["by_category"]["parity.row-count"] == {"ok": 1}


def test_unchanged_since_detects_a_repeat_run(tmp_path):
    hop = _Hop(tmp_path)
    records = [{"check": "counts", "scope": "db", "status": "diff",
                "detail": "src=5 dst=4", "category": "parity.row-count"}]
    assert verdict.unchanged_since(hop, records) is False   # nothing written yet
    verdict.write(hop, records)
    assert verdict.unchanged_since(hop, records) is True
    moved = [dict(records[0], detail="src=9 dst=9", status="ok")]
    assert verdict.unchanged_since(hop, moved) is False


# ---- the shared difference-shape classifier ----

def test_difference_kind_names_each_shape():
    from migkit.verdict import difference_kind
    # same keys, different values
    assert difference_kind(10, "K", 10, "K") == "values-changed"
    # different keys, same count
    assert difference_kind(10, "K", 10, "J") == "rows-replaced"
    # different keys, source has more
    assert difference_kind(10, "K", 8, "J") == "rows-missing by=2"
    # different keys, target has more
    assert difference_kind(8, "K", 10, "J") == "rows-extra by=2"


def test_difference_kind_refuses_to_guess_without_a_key():
    """A table with no primary key gives no key hash, and inventing a verdict
    from counts alone would be a guess dressed as a finding."""
    from migkit.verdict import difference_kind
    assert difference_kind(10, None, 8, None) == ""
    assert difference_kind(10, "K", 8, None) == ""
    assert difference_kind(10, None, 8, "J") == ""


def test_difference_kind_compares_hashes_as_text_across_engines():
    """PostgreSQL returns a numeric sum and MySQL a BIT_XOR integer; both
    arrive as whatever the driver produced, so the comparison is on text."""
    from migkit.verdict import difference_kind
    assert difference_kind(5, 123, 5, "123") == "values-changed"
    assert difference_kind(5, 123, 5, 124) == "rows-replaced"


def test_counted_and_inferred_kinds_use_the_same_words():
    """A MongoDB rows-missing and a MySQL rows-missing have to mean the same
    thing, or aggregating findings across engines is meaningless."""
    from migkit.verdict import difference_kind, difference_kind_from_counts
    # inferred from hashes (SQL engines)
    assert difference_kind(10, "K", 8, "J") == "rows-missing by=2"
    assert difference_kind(10, "K", 10, "J") == "rows-replaced"
    assert difference_kind(10, "K", 10, "K") == "values-changed"
    # counted directly (MongoDB)
    assert difference_kind_from_counts(2, 0, 0) == "rows-missing by=2"
    assert difference_kind_from_counts(3, 3, 0) == "rows-replaced"
    assert difference_kind_from_counts(0, 0, 4) == "values-changed"
    assert difference_kind_from_counts(0, 0, 0) == ""


def test_counted_kind_reports_both_sides_when_they_are_unequal():
    """Two missing and five extra is not a replacement, and flattening it to
    one label would lose the fact."""
    from migkit.verdict import difference_kind_from_counts
    assert difference_kind_from_counts(2, 5, 0) == \
        "rows-missing by=2,rows-extra by=5"
