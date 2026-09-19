"""The repair the check used to describe and hand back.

`check --deep` already found this and said the right thing:

    1 columns hold both double-encoded and correct text: public.notes.body
      3 double-encoded and 2 genuinely accented (e.g. 'Ã©clair' is really
      'éclair') - converting the whole column repairs the first kind and
      destroys the second
    fix: repair row by row, matching only the values that re-encode to
      valid UTF-8

and then `repair_plan` offered nothing at all. The hardest part of the job -
telling the two kinds of row apart - was left with the engineer, next to a
warning that the one-line version corrupts data. Measured on the target
below, the one-liner does not even get that far:

    select convert_from(convert_to(body,'LATIN1'),'UTF8') from notes
    ERROR:  invalid byte sequence for encoding "UTF8": 0xe9

0xe9 is the `é` in `café` - a row that was never broken, killing the
statement meant to repair the three beside it.

The repair reads the **target**, not the source: migkit writes to the source
nowhere, and the target is the copy it is answerable for. That has a price,
and the price is the reason the statements are withheld by default: a
repaired target no longer matches the source it came from, and the resync
repair standing next to it in the same plan would copy the broken text back.
So the action is always *listed*, with the consequence in its note, and
carries statements only when `MIGKIT_REPAIR_TEXT` says so. An automated
reconcile loop therefore rewrites no text on its own.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15529, 15530
NAMES = {SRC: "migkit-test-mjr-src", DST: "migkit-test-mjr-dst"}

#: What an application sending UTF-8 through a latin1 connection stored,
#: beside text that is simply accented and correct. Written as a round trip
#: rather than as literals so the file says which kind each one is.
BROKEN = {1: "éclair", 5: "über", 6: "日本語"}
FINE = {2: "café", 3: "plain ascii", 4: "£100"}


def _stored(text):
    return text.encode("utf-8").decode("latin-1")


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def _seed_sql():
    rows = []
    for i, t in sorted({**{k: _stored(v) for k, v in BROKEN.items()},
                        **FINE}.items()):
        rows.append(f"insert into notes values ({i}, '{t}');")
    return ("drop table if exists notes; drop table if exists nokey;"
            " create table notes (id int primary key, body text);"
            + "".join(rows)
            + " create table nokey (body text);"
            f" insert into nokey values ('{_stored('ü')}');")


@pytest.fixture(scope="module")
def mjr_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


@pytest.fixture
def seeded(mjr_pair):
    """Both sides carrying the same mixed column, which is what a faithful
    move leaves behind."""
    for port in NAMES:
        got = q(port, _seed_sql())
        assert got.returncode == 0, got.stderr
    return mjr_pair


def _engine(pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="mj", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _bodies(port):
    out = q(port, "select id||chr(31)||body from notes order by id").stdout
    return dict(line.split("\x1f", 1) for line in out.splitlines() if line)


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("MIGKIT_REPAIR_TEXT", "1")


def test_the_one_line_conversion_really_does_fail_here(seeded):
    """The control, and the reason a row-level repair has to exist. If the
    blanket conversion worked on this column, everything below would be
    solving a problem nobody has."""
    got = q(seeded["dst"],
            "select convert_from(convert_to(body,'LATIN1'),'UTF8') from notes")
    assert got.returncode != 0
    assert "invalid byte sequence" in got.stderr, got.stderr
    # and the byte it chokes on belongs to a row that was never broken
    assert "0xe9" in got.stderr, got.stderr


def test_it_repairs_the_broken_rows_and_leaves_the_rest_byte_identical(
        seeded, tmp_path, enabled):
    before = _bodies(seeded["dst"])
    eng = _engine(seeded, tmp_path)
    action = eng._mojibake_repair("postgres")
    eng.apply("postgres", action)

    after = _bodies(seeded["dst"])
    for i, real in BROKEN.items():
        assert after[str(i)] == real, (i, after[str(i)])
    for i, untouched in FINE.items():
        assert after[str(i)] == untouched == before[str(i)], i


def test_the_default_carries_no_statements_and_says_why(seeded, tmp_path,
                                                          monkeypatch):
    """Every other repair here moves the target towards the source. This one
    moves it away on purpose, so an automated `sync --apply` must describe
    it rather than perform it."""
    monkeypatch.delenv("MIGKIT_REPAIR_TEXT", raising=False)
    before = _bodies(seeded["dst"])
    eng = _engine(seeded, tmp_path)
    action = eng._mojibake_repair("postgres")

    assert action is not None, "the finding must still be listed"
    assert action.statements == [], action.statements
    assert action.undo == [], action.undo
    assert "REFUSED" in action.note, action.note
    assert "MIGKIT_REPAIR_TEXT=1" in action.note, action.note
    # the consequence is named in the plan, not discovered at the next check
    assert "no longer matches the source" in action.note, action.note
    assert "sync --kind rows" in action.note, action.note
    # and the counts are real even though nothing will run
    assert "3 double-encoded values" in action.note, action.note
    assert "2 genuinely accented" in action.note, action.note

    eng.apply("postgres", action)
    assert _bodies(seeded["dst"]) == before


def test_undo_puts_every_repaired_row_back(seeded, tmp_path, enabled):
    from migkit.engines.base import RepairAction
    before = _bodies(seeded["dst"])
    eng = _engine(seeded, tmp_path)
    action = eng._mojibake_repair("postgres")
    assert len(action.undo) == len(action.statements) == 3, action.statements

    eng.apply("postgres", action)
    assert _bodies(seeded["dst"]) != before
    eng.apply("postgres", RepairAction("postgres", "text", action.undo, [],
                                       "undo"))
    assert _bodies(seeded["dst"]) == before


def test_a_row_that_changed_since_the_plan_was_read_is_left_alone(
        seeded, tmp_path, enabled):
    """Every update matches the old value as well as the key. A plan held
    over a coffee break must not overwrite somebody's edit with a repair of
    text that is no longer there."""
    eng = _engine(seeded, tmp_path)
    action = eng._mojibake_repair("postgres")
    assert q(seeded["dst"], "update notes set body = 'edited by someone'"
                            " where id = 1").returncode == 0
    eng.apply("postgres", action)

    after = _bodies(seeded["dst"])
    assert after["1"] == "edited by someone", after["1"]
    # the rows nobody touched were still repaired
    assert after["5"] == BROKEN[5] and after["6"] == BROKEN[6], after


def test_a_second_pass_finds_nothing_left(seeded, tmp_path, enabled):
    """Repaired text no longer classifies as double-encoded, so the work
    converges instead of breaking what it just fixed - which is what running
    the column conversion twice does."""
    eng = _engine(seeded, tmp_path)
    eng.apply("postgres", eng._mojibake_repair("postgres"))
    assert eng._mojibake_repair("postgres") is None


def test_the_rows_it_cannot_address_are_named(seeded, tmp_path, enabled):
    """`nokey` holds broken text and has no primary key. Repairing it by
    value would hit every row that happens to match, so it is refused - out
    loud, because a silent skip is a row nobody ever comes back to."""
    assert q(seeded["dst"], "select count(*) from nokey").stdout.strip() == "1"
    action = _engine(seeded, tmp_path)._mojibake_repair("postgres")
    assert "public.nokey" in action.note, action.note
    assert "no primary key" in action.note, action.note
    assert all("nokey" not in s for s in action.statements), action.statements


def test_the_broken_column_being_the_key_is_refused(seeded, tmp_path,
                                                      enabled):
    """Rewriting a primary key moves the row every foreign key points at.
    The repair declines and says which column, rather than quietly dropping
    it from the plan."""
    assert q(seeded["dst"], "create table tags (name text primary key,"
                            f" label text); insert into tags values"
                            f" ('{_stored('café')}', 'x')").returncode == 0
    try:
        note = _engine(seeded, tmp_path)._mojibake_repair("postgres").note
        assert "public.tags.name" in note, note
        assert "the key itself" in note, note
    finally:
        q(seeded["dst"], "drop table tags")


def test_it_writes_to_the_target_only(seeded, tmp_path, enabled):
    """The source is somebody's live database. migkit repairs its copy."""
    before = _bodies(seeded["src"])
    eng = _engine(seeded, tmp_path)
    action = eng._mojibake_repair("postgres")
    assert all('"public"."notes"' in s for s in action.statements), \
        action.statements
    eng.apply("postgres", action)
    assert _bodies(seeded["src"]) == before
    assert _bodies(seeded["dst"]) != before


def test_the_text_repair_comes_after_the_resync_in_the_plan(seeded, tmp_path,
                                                             enabled):
    """A recopy takes its rows from the source, where the broken text still
    lives. Applied in the other order, the repair beside it would be undone
    by the action next to it."""
    (tmp_path / "data-public.notes.missing").write_text("1\n")
    kinds = [a.kind for a in _engine(seeded, tmp_path).repair_plan(
        "postgres", "all")]
    assert "rows" in kinds and "text" in kinds, kinds
    assert kinds.index("text") > kinds.index("rows"), kinds


def test_an_unknown_kind_still_refuses(seeded, tmp_path):
    """The dispatch grew a branch. It must not have grown a fallthrough."""
    from migkit.engines.base import RepairAction
    eng = _engine(seeded, tmp_path)
    with pytest.raises(RuntimeError, match="no way to apply"):
        eng.apply("postgres", RepairAction("postgres", "invented", ["x"], [],
                                           ""))


def test_the_statements_need_no_server(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="m", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    rows = [[1, _stored("éclair")], [2, "café"], [3, "ascii"],
            [None, _stored("über")]]
    stmts, undo, cols, rep, clean, skipped, left = eng._mojibake_updates(
        "x", "public.t", ["id"], ["body"], rows)
    assert rep == 1 and clean == 1 and skipped == 1 and left == 0
    assert cols == {"public.t.body"}
    assert stmts == ['update "public"."t" set "body" = \'éclair\''
                     ' where "id" = \'1\' and "body" = \'Ã©clair\';']
    assert undo == ['update "public"."t" set "body" = \'Ã©clair\''
                    ' where "id" = \'1\' and "body" = \'éclair\';']

    # the cap is a floor that is reported, not a silent truncation
    eng.MOJIBAKE_REPAIR_CAP = 1
    try:
        many = [[i, _stored("éclair")] for i in range(5)]
        stmts, _, _, rep, _, _, left = eng._mojibake_updates(
            "x", "public.t", ["id"], ["body"], many)
        assert (rep, left, len(stmts)) == (1, 4, 1)
        note = eng._mojibake_repair_note(rep, {"public.t.body"}, 0, 0, left,
                                         [], True)
        assert "at least 4 beyond" in note, note
    finally:
        del eng.MOJIBAKE_REPAIR_CAP

    # a value carrying a quote survives the literal it is written into
    stmts, _, _, rep, _, _, _ = eng._mojibake_updates(
        "x", "public.t", ["id"], ["body"], [[1, _stored("d'été")]])
    assert rep == 1 and "''" in stmts[0], stmts

    # MySQL reads a backslash as an escape, which is why the quoting is a
    # method rather than one expression written inline
    assert eng._quote_literal("a\\b") == "'a\\b'"
    assert MySQLEngine._quote_literal(None, "a\\b") == "'a\\\\b'"


def test_off_by_default_is_the_env_var_alone(monkeypatch):
    from migkit.engines.base import Engine
    for value, want in (("1", True), ("true", True), ("on", True),
                        ("0", False), ("", False), ("no", False)):
        monkeypatch.setenv("MIGKIT_REPAIR_TEXT", value)
        assert Engine._repair_text_enabled() is want, value
    monkeypatch.delenv("MIGKIT_REPAIR_TEXT")
    assert Engine._repair_text_enabled() is False
