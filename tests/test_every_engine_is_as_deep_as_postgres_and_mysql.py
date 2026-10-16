"""Every engine as deep as PostgreSQL and MySQL, or the gap said.

The owner (2026-10-04): "complete, all of it deep, all of it good" - and
an engine left thinner than PostgreSQL and MySQL is a red test, never a
silent gap (backlog P0-PARITY.guard). The depth matrix
(`capabilities.depth`) reads every item off the code each engine runs -
the scope held in every path, every deep-check category, delta, fence,
confirm, repair with its undo, users, the fields a verdict reads, the
change tail's table states, DDL and renames, the assessment, the schema,
the move, the cutover, TLS, managed services, fencing - and a way is one
only where a test proves it on that engine (`PROVES` in the test module,
read from its source, never imported).

Static: no server is reached; it runs with the guards, before a commit.
"""
import pytest

from migkit import capabilities as caps
from migkit.engines import NAMES
from tests.test_the_report_does_not_name_its_tools import TOOLS


@pytest.fixture(scope="module")
def failing():
    return caps.depth_failing()


def test_no_engine_lacks_an_item_without_saying_so(failing):
    assert failing["undeclared"] == []


def test_no_way_goes_without_a_test_unsaid(failing):
    """A way the code has and no test proves on that engine is not yet a
    way: a test's `PROVES` names it, or `UNPROVED` says its test is owed."""
    assert failing["untested"] == []


def test_no_declaration_is_one_the_code_or_a_test_has_closed(failing):
    assert failing["stale"] == []


def test_no_test_proves_a_way_the_code_does_not_show(failing):
    """A test's claim and the code's probe disagree: the probe misses the
    way, or the claim is wrong."""
    assert failing["claimed"] == []


def test_every_not_yet_waits_on_a_row_of_the_backlog(failing):
    assert failing["unrowed"] == []
    rows = caps.backlog_rows()
    for area, (_, row) in caps.AREAS.items():
        assert row in rows, (area, row)


def test_every_declaration_and_proof_names_an_item_and_an_engine(failing):
    assert failing["unknown"] == []


def test_every_way_the_decision_engine_declares_can_be_reached():
    from migkit import decide
    assert decide.orphan_ways() == []


def test_every_item_is_in_an_area_and_said_in_words():
    for item in caps.DEPTH:
        area = item.split(".", 1)[0]
        assert area in caps.AREAS, item
        words = caps.depth_words(item)
        assert len(words.split()) >= 3, (item, words)
        low = words.lower()
        assert not [t for t in TOOLS if t in low], (item, words)
    for item, (kind, name) in caps.LINKED.items():
        assert item in caps.DEPTH and kind in ("capability", "shape"), item


def test_a_not_applicable_says_why_in_the_operators_words():
    for item, gaps in caps.DEPTH_GAPS.items():
        for engine, (state, why) in gaps.items():
            assert state in (caps.NOT_APPLICABLE, caps.NOT_YET), \
                (item, engine)
            if state == caps.NOT_APPLICABLE:
                assert len(why.split()) >= 4, (item, engine, why)
                low = why.lower()
                assert not [t for t in TOOLS if t in low], (item, why)


def test_the_matrix_covers_every_engine_and_every_item():
    got = caps.depth()
    assert set(got) == set(NAMES)
    for engine, row in got.items():
        assert set(row) == set(caps.DEPTH), engine


# ---- the probes have teeth ------------------------------------------------

def _without(monkeypatch, engine, **methods):
    """The engine's class with `methods` replaced, as the matrix reads
    it: what it says then is what the code would say."""
    from migkit.engines import _class_for
    real = _class_for(engine)
    stub = type(real.__name__, (real,), methods)
    monkeypatch.setattr(caps, "_class_for",
                        lambda n: stub if n == engine else _class_for(n))
    return stub


def test_a_way_taken_away_is_noticed(monkeypatch):
    """PostgreSQL's tail stops marking what it has made visible: a table
    added to a running tail no longer joins at an exact seam, nobody
    declared that, and the test that proved it now proves nothing the
    code does - the guard fails either way."""
    from migkit.engines.base import Engine
    assert caps.depth_has("postgres", "tail.added")
    _without(monkeypatch, "postgres", mark_covers=Engine.mark_covers)
    assert not caps.depth_has("postgres", "tail.added")
    assert caps.depth_cell("postgres", "tail.added")[0] in (
        "undeclared", "stale", "claimed")


def test_a_method_that_only_refuses_is_not_a_way(monkeypatch):
    from migkit.engines.base import Result

    def check_deep(self, db):
        return [Result("deep", db, "skip", "cannot")]
    _without(monkeypatch, "postgres", check_deep=check_deep)
    assert not caps.depth_has("postgres", "scope.deep")


def test_a_finding_of_a_category_is_read_off_the_scope_it_says(monkeypatch):
    """ClickHouse's deep check names stuck mutations, which no category
    reads; a finding ending in `fk` is read as foreign keys."""
    from migkit.engines.base import Result
    assert not caps.depth_has("clickhouse",
                              "verify.structure.foreign-keys")

    def check_deep(self, db):
        return [Result("deep", f"{db} fk", "ok", "no orphan")]
    _without(monkeypatch, "clickhouse", check_deep=check_deep)
    assert caps.depth_has("clickhouse", "verify.structure.foreign-keys")


def test_a_proof_without_a_way_is_a_claim(monkeypatch):
    monkeypatch.setattr(caps, "proofs", lambda: {
        ("verify.structure.foreign-keys", "redis"): ["test_x.py"]})
    assert caps.depth_cell("redis", "verify.structure.foreign-keys")[0] \
        == "claimed"


def test_a_way_without_its_test_is_untested(monkeypatch):
    monkeypatch.setattr(caps, "proofs", lambda: {})
    monkeypatch.setattr(caps, "UNPROVED", {})
    assert caps.depth_cell("postgres", "scope.deep")[0] == "untested"


def test_a_way_whose_test_is_owed_is_not_yet_and_says_its_row(monkeypatch):
    monkeypatch.setattr(caps, "proofs", lambda: {})
    monkeypatch.setattr(caps, "UNPROVED", {"scope.deep": "postgres"})
    assert caps.depth_cell("postgres", "scope.deep") == (
        caps.NOT_YET, "P0-PARITY.scope")


def test_a_test_written_for_an_owed_way_closes_the_declaration(monkeypatch):
    monkeypatch.setattr(caps, "proofs", lambda: {
        ("scope.deep", "postgres"): ["test_x.py"]})
    monkeypatch.setattr(caps, "UNPROVED", {"scope.deep": "postgres"})
    assert caps.depth_cell("postgres", "scope.deep")[0] == "stale"


def test_a_gap_the_code_closes_goes_stale(monkeypatch):
    """Give Redis a foreign-key finding and its not-applicable reason is
    one the code has overtaken."""
    from migkit.engines.base import Result

    def check_deep(self, db):
        return [Result("deep", f"{db} fk", "ok", "")]
    _without(monkeypatch, "redis", check_deep=check_deep)
    assert caps.depth_cell("redis", "verify.structure.foreign-keys")[0] \
        == "stale"


def test_the_registry_is_read_without_importing_the_tests(tmp_path):
    """`PROVES` is literal and read from the module's source - a module
    that would fail to import is read all the same: an item and an
    engine, or an item and several engines."""
    (tmp_path / "test_one.py").write_text(
        'raise RuntimeError("imported")\n'
        'PROVES = [("scope.deep", "postgres"),\n'
        '          ("verify.delta", ("mysql", "tidb"))]\n')
    (tmp_path / "test_two.py").write_text(
        'PROVES = [("scope.deep", "postgres")]\n')
    (tmp_path / "helper.py").write_text(
        'PROVES = [("scope.copy", "postgres")]\n')
    got = caps.proofs(tmp_path)
    assert got == {("scope.deep", "postgres"): ["test_one.py",
                                                "test_two.py"],
                   ("verify.delta", "mysql"): ["test_one.py"],
                   ("verify.delta", "tidb"): ["test_one.py"]}


# ---- built, unproved: an engine module says which server it was not run on

def _declaring(monkeypatch, tmp_path, source, served=("postgres",)):
    """The engines in `served` are defined in a module with `source`; the
    merge of the declarations is run on an empty `UNPROVED`."""
    path = tmp_path / "engine_module.py"
    path.write_text(source)
    monkeypatch.setattr(caps, "proofs", lambda: {})
    monkeypatch.setattr(caps, "UNPROVED", {})
    monkeypatch.setattr(caps, "UNPROVED_WHY", {})
    monkeypatch.setattr(caps, "_BAD_DECLARATIONS", [])
    monkeypatch.setattr(caps, "_engine_source",
                        lambda n: str(path) if n in served else None)
    caps._merge_declared()


def test_a_cell_a_module_declares_is_built_unproved_and_says_why(
        monkeypatch, tmp_path):
    _declaring(monkeypatch, tmp_path,
               'raise RuntimeError("imported")\n'
               'UNPROVED_ON = "built; not run on a real Example server"\n'
               'UNPROVED_ITEMS = ("scope.deep "\n"verify.delta")\n')
    assert caps.depth_cell("postgres", "scope.deep") == (
        caps.NOT_YET, "P0-PARITY.scope")
    assert caps.depth_why("postgres", "scope.deep") == \
        "built; not run on a real Example server"
    assert caps.depth_cell("postgres", "verify.delta")[0] == caps.NOT_YET
    # a cell the module does not list is not borrowed from its neighbours
    assert caps.depth_cell("postgres", "scope.copy")[0] == "untested"
    assert caps.depth_why("postgres", "scope.copy") == ""


def test_a_cell_with_no_declaration_stays_a_test_owed(monkeypatch, tmp_path):
    """Only `UNPROVED_ITEMS`, no `UNPROVED_ON`: the module is not read, the
    cell has no reason - it is a test merely not written, and the central
    line is what owes it."""
    _declaring(monkeypatch, tmp_path, 'UNPROVED_ITEMS = "scope.deep"\n')
    assert caps.depth_cell("postgres", "scope.deep")[0] == "untested"
    monkeypatch.setattr(caps, "UNPROVED", {"scope.deep": "postgres"})
    assert caps.depth_cell("postgres", "scope.deep") == (
        caps.NOT_YET, "P0-PARITY.scope")
    assert caps.depth_why("postgres", "scope.deep") == ""


def test_a_declaring_engine_is_read_from_its_module_alone(
        monkeypatch, tmp_path):
    """The central lines never add an engine whose module declares."""
    _declaring(monkeypatch, tmp_path,
               'UNPROVED_ON = "built; not run on a real Example server"\n'
               'UNPROVED_ITEMS = "scope.deep"\n')
    monkeypatch.setattr(caps, "UNPROVED", {
        "scope.deep": "postgres mysql", "scope.copy": "postgres"})
    caps._merge_declared()
    assert caps.UNPROVED["scope.deep"].split() == ["mysql", "postgres"]
    assert "postgres" not in caps.UNPROVED["scope.copy"].split()


def test_a_declared_cell_whose_test_now_exists_is_stale(
        monkeypatch, tmp_path):
    _declaring(monkeypatch, tmp_path,
               'UNPROVED_ON = "built; not run on a real Example server"\n'
               'UNPROVED_ITEMS = "scope.deep"\n')
    monkeypatch.setattr(caps, "proofs", lambda: {
        ("scope.deep", "postgres"): ["test_x.py"]})
    assert caps.depth_cell("postgres", "scope.deep")[0] == "stale"


def test_a_module_serving_several_engines_names_a_cell_per_engine(
        monkeypatch, tmp_path):
    _declaring(monkeypatch, tmp_path,
               'UNPROVED_ON = "built; not run on a real Example server"\n'
               'UNPROVED_ITEMS = "scope.deep mysql:scope.copy other:x"\n',
               served=("postgres", "mysql"))
    assert caps.UNPROVED["scope.deep"].split() == ["postgres", "mysql"]
    assert caps.UNPROVED["scope.copy"].split() == ["mysql"]
    assert caps._BAD_DECLARATIONS == [("other", "x", "not served here")]


def test_every_declared_reason_is_one_line_in_the_owners_words(failing):
    """Strict for the engines that declare: the reason is one line and says
    what was not done on which server (`built; not run on a real <server>`),
    and the declaration lists a cell - one that names nothing is a mistake."""
    assert failing["unknown"] == []
    assert caps.UNPROVED_WHY, "no engine module declares UNPROVED_ON yet"
    for engine, why in caps.UNPROVED_WHY.items():
        assert why.startswith("built; not run on a real "), (engine, why)
        assert "\n" not in why and len(why.split()) >= 7, (engine, why)
        assert any(caps.depth_why(engine, i) for i in caps.DEPTH), engine


def test_an_engine_with_no_sandbox_says_which_server_it_was_not_run_on():
    """An engine with no profile in `tests/servers.PROFILES` and a cell that
    is "test owed" should declare `UNPROVED_ON` in its module. A warning
    line, not a red test, until each family ticket lands its declaration
    (wave 17 B-I); strict for the engines that declare (the test above) -
    wave 18 makes this strict once every such engine declares."""
    import warnings
    from tests.servers import PROFILES
    # a profile names its server's family ("mongo"); the engine is "mongodb"
    with_server = {{"mongo": "mongodb"}.get(p.engine, p.engine)
                   for p in PROFILES.values()}
    owed = {}
    for item, engines in caps.UNPROVED.items():
        for n in engines.split():
            if n in NAMES and n not in with_server \
                    and n not in caps.UNPROVED_WHY \
                    and caps.depth_cell(n, item)[0] == caps.NOT_YET:
                owed.setdefault(n, []).append(item)
    if owed:
        warnings.warn(
            "engines with no sandbox and a test-owed cell, and no"
            " UNPROVED_ON in their module: "
            + ", ".join(f"{n} ({len(v)})" for n, v in sorted(owed.items())),
            stacklevel=1)
