"""The manual and the site say what the code does, because the code writes
them (backlog R24, audit LG15, LG16).

The site described a version months old, the README badge named 12
engines of 34, the README listed engines nothing in `migkit/` has, and
`docs/goals-and-modes.md` said it came "from one table in the code" while
four of its cells had drifted from it. `tools/gen_docs.py` now writes every
page from the registries the code runs on; this fails when a page on disk
is not what it writes (run `python tools/gen_docs.py`), when any page
names a program migkit drives, claims a capability, a command or an option
the code does not have, or leaves out an engine, a goal, a mode, an option
or a variable the code has.
"""
import html
import re
import sys
from pathlib import Path

import pytest

from tests.test_the_report_does_not_name_its_tools import TOOLS

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import gen_docs  # noqa: E402


@pytest.fixture(scope="module")
def pages():
    return gen_docs.outputs()


def _named(text):
    """The programs migkit drives that `text` names: the report test's
    list and the programs the movers start."""
    from migkit.movers import DRIVEN
    low = text.lower()
    return sorted({n for n in {t.lower() for t in TOOLS}
                   | {d.lower() for d in DRIVEN} if n in low})


def _page(pages, slug):
    return pages[str(gen_docs.SITE / f"{slug}.html")]


def test_every_page_is_what_the_code_writes(pages):
    behind = gen_docs.stale(pages)
    assert not behind, ("behind the code - run `python tools/gen_docs.py`: "
                        + ", ".join(behind))


def test_a_change_in_the_code_makes_the_pages_stale(monkeypatch):
    """The staleness check can say "different": a goal's words changed
    in the registry leave the goals page, the README and
    `goals-and-modes.md` behind."""
    from migkit import hopkeys
    monkeypatch.setitem(hopkeys.GOALS, "copy", dict(
        hopkeys.GOALS["copy"], what="a sentence nobody wrote"))
    behind = gen_docs.stale()
    for path in (str(gen_docs.SITE / "goals.html"), "README.md",
                 "docs/goals-and-modes.md"):
        assert path in behind, behind


def test_no_page_names_a_program(pages):
    leaks = {path: _named(text) for path, text in pages.items()
             if _named(text)}
    assert not leaks, leaks


def test_the_scan_would_notice(pages):
    """A scan that matched nothing would pass for the wrong reason."""
    page = _page(pages, "engines")
    assert _named(page) == []
    assert _named(page + " copied by pgcopydb ") == ["pgcopydb"]
    assert len(pages) >= 15, sorted(pages)


def test_every_page_shows_the_version(pages):
    from migkit import __version__
    for path, text in pages.items():
        if path.endswith(".html"):
            assert f"migkit {__version__}" in text, path


def _cells(page, prefix):
    """{row id: [cell classes]} of a table whose rows are named."""
    out = {}
    for m in re.finditer(rf'<tr id="{prefix}-([\w-]+)">(.*?)</tr>', page,
                         re.S):
        out[m.group(1)] = re.findall(r'<td(?: class="(\w+)")?', m.group(2))
    return out


def test_the_engine_page_is_the_matrix(pages):
    """Every engine, and each capability as the code says it: no cell
    claims more or less than `capabilities.matrix()`."""
    from migkit import capabilities
    from migkit.engines import ALIASES, NAMES, TITLES
    page = _page(pages, "engines")
    rows = _cells(page, "cap")
    matrix = capabilities.matrix()
    assert sorted(rows) == sorted(NAMES)
    for name in NAMES:
        assert name in TITLES, name
        said = rows[name][1:]
        assert said.count("y") == list(matrix[name].values()).count("yes"), \
            name
        for cap, cls in zip(capabilities.CAPABILITIES, said):
            want = {"yes": "y", capabilities.NOT_APPLICABLE: "na",
                    capabilities.NOT_YET: "p"}[matrix[name][cap]]
            assert cls == want, (name, cap, cls)
    for alias in ALIASES:
        assert f"<code>{alias}</code>" in page, alias


def test_a_gap_in_the_code_is_a_gap_on_the_page(monkeypatch):
    """The page follows the code, not the other way: PostgreSQL made
    unable to compare its data reads "not yet" on its row."""
    from migkit import capabilities
    real = capabilities.implemented
    monkeypatch.setattr(capabilities, "implemented",
                        lambda n, c: False if (n, c) == ("postgres", "data")
                        else real(n, c))
    monkeypatch.setitem(capabilities.GAPS["postgres"], "data",
                        (capabilities.NOT_YET, "0"))
    at = list(capabilities.CAPABILITIES).index("data")
    before = _cells(gen_docs.page_html(gen_docs.page_engines(), []), "cap")
    assert before["mysql"][1:][at] == "y"
    cells = before["postgres"][1:]
    assert cells[at] == "p", cells
    assert "not yet" in gen_docs.page_html(gen_docs.page_engines(), [])


def test_every_goal_mode_and_option_is_on_its_page(pages):
    from migkit import hopkeys
    goals, options = _page(pages, "goals"), _page(pages, "options")
    for g in hopkeys.GOALS:
        assert f'id="goal-{g}"' in goals, g
    for c, m in hopkeys.MODES:
        assert f"<code>{c} --mode {m}</code>" in goals, (c, m)
    for words in hopkeys.CUTOVER["adds"]:
        assert html.escape(words, quote=False) in goals, words
    for slug, registry in (("hop", hopkeys.HOP_KEYS),
                           ("options", hopkeys.HOP_OPTIONS),
                           ("side", hopkeys.ENDPOINT_KEYS),
                           ("side-options", hopkeys.ENDPOINT_OPTIONS)):
        listed, left = gen_docs.options_listed(registry)
        assert set(listed) | set(left) == set(registry)
        for k in listed:
            assert f'id="opt-{slug}-{k}"' in options, (slug, k)
        for k in left:
            assert f'id="opt-{slug}-{k}"' not in options, (slug, k)
    # the keys named for a program are the ones left out, and said so
    assert "atlas" in gen_docs.options_listed(hopkeys.HOP_OPTIONS)[1]
    assert "more keys turn" in options or "more key turns" in options


def test_the_goals_doc_is_the_registry_between_its_markers():
    """LG15: the table the doc says comes from the code does."""
    from migkit import hopkeys
    doc = (ROOT / "docs" / "goals-and-modes.md").read_text()
    for name, body in gen_docs.goals_md_sections().items():
        start = f"<!-- gen_docs: {name} -->\n"
        assert start + body + "\n<!-- /gen_docs: " in doc, name
    for g, v in hopkeys.GOALS.items():
        assert v["guarantees"].replace("|", "\\|") in doc, g


def test_every_command_and_option_shown_exists(pages):
    """A page may not show a command or an option that is not there:
    every `migkit <command> --option` in code on every page is held to
    the CLI."""
    from migkit import cli
    shown, bad = 0, []
    for path, text in pages.items():
        if not path.endswith(".html"):
            continue
        for m in re.finditer(r"<code>(.*?)</code>", text, re.S):
            for line in html.unescape(m.group(1)).split("\n"):
                for c in re.finditer(r"(?<![\w-])migkit ([a-z][a-z-]*)"
                                     r"([^;`]*)", line):
                    shown += 1
                    cmd = cli.main.commands.get(c.group(1))
                    if cmd is None:
                        bad.append((path, line))
                        continue
                    opts = {o for p in cmd.params
                            for o in getattr(p, "opts", ())}
                    bad += [(path, line) for o in
                            re.findall(r"(?<![\w-])(--[a-z][a-z-]*)",
                                       c.group(2)) if o not in opts]
    assert shown > 50, shown
    assert not bad, bad


def test_every_command_is_on_the_commands_page(pages):
    from migkit import cli
    page = _page(pages, "commands")
    for name, cmd in cli.main.commands.items():
        if cmd.hidden:
            assert f"<code>{name}</code>" in page, name
            continue
        assert f'id="cmd-{name}"' in page, name
        for p in cmd.params:
            if p.param_type_name == "option":
                assert f'id="opt-{name}-{p.name}"' in page, (name, p.name)
    for family, _, _ in gen_docs.metrics():
        assert f'id="metric-{family}"' in page, family
    assert len(gen_docs.metrics()) > 20


def test_the_help_lists_every_engine():
    """LG16: `migkit --help` named 7 engines and put two of them under the
    generic one."""
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines import NAMES
    said = CliRunner().invoke(cli.main, ["--help"]).output
    for name in NAMES:
        if name not in ("hetero", "generic"):
            assert re.search(rf"(?<![\w-]){name}(?![\w-])", said), name
    assert "(hetero)" in said and "(generic)" in said


def test_every_variable_the_code_reads_is_documented(pages):
    """Every `MIGKIT_*` read through `os.environ`, found by a scan of its
    own, is on the page - unless the registry marks it as migkit's own
    (set for a process it starts, or the tests') or it names a program."""
    page = _page(pages, "environment")
    envkeys = gen_docs.optional("envkeys")
    read = set()
    for path in (ROOT / "migkit").rglob("*.py"):
        read |= set(re.findall(r"(?:environ\.get|getenv|environ\[|envkeys\."
                               r"(?:flag|number|text|items))\(?\s*"
                               r"[\"'](MIGKIT_[A-Z0-9_]+)[\"']",
                               path.read_text()))
    assert len(read) > 40, len(read)
    for name in sorted(read):
        var = envkeys.VARS.get(name) if envkeys else None
        mine = var is not None and (var.internal or var.tests
                                    or var.named_for_a_program)
        if mine or (envkeys is None and gen_docs.names_a_program(name)):
            assert f'id="env-{name}"' not in page, name
            continue
        assert f'id="env-{name}"' in page, name
    if envkeys is not None:
        # the words are the registry's, not a blank
        for name, var in envkeys.VARS.items():
            if f'id="env-{name}"' in page:
                words = max(html.escape(var.what, quote=False).split("`"),
                            key=len)
                assert len(words) > 3 and words in page, name


def test_every_refusal_is_on_the_troubleshooting_page(pages):
    from migkit import troubleshooting
    page = html.unescape(_page(pages, "troubleshooting"))
    got = troubleshooting.refusals()
    assert len(got) > 100
    for _module, _fn, text in got:
        plain = text.replace("`", "")
        assert plain in re.sub(r"</?code>", "", page), text[:80]
    # grouped by what an operator looks for, never by a module's path
    heads = re.findall(r"<h2[^>]*>(.*?)</h2>", page)
    assert len(heads) > 20, heads
    assert not [h for h in heads if "migkit/" in h or ".py" in h], heads


def test_the_troubleshooting_guide_names_no_module():
    """A module's path named a program for some (`redisshake.py`)."""
    from migkit import troubleshooting
    text = troubleshooting.render()
    assert "## `migkit/" not in text
    assert troubleshooting.area("migkit/engines/redis.py") == "Redis"
    assert troubleshooting.area("migkit/engines/oracle_redo.py") == \
        "Oracle Database"
    assert troubleshooting.area("migkit/redisshake.py") == "Moving the data"
    assert troubleshooting.area("migkit/no_such_part.py") == \
        troubleshooting.ELSEWHERE


def test_the_recipes_are_hops_the_code_takes():
    """Each recipe's hop passes the configuration's own check, and a
    recipe leaning on what the code lacks is not written."""
    from migkit import hopkeys
    for r in gen_docs.RECIPES:
        assert not [p for p in hopkeys.check({"hops": r.hops})
                    if p.level == "error"], r.anchor
        assert gen_docs.recipe_blocks(r)


def test_a_recipe_the_code_cannot_do_is_refused(monkeypatch):
    from migkit import capabilities
    real = capabilities.implemented
    monkeypatch.setattr(capabilities, "implemented",
                        lambda n, c: False if (n, c) == ("postgres", "stream")
                        else real(n, c))
    recipe = next(r for r in gen_docs.RECIPES
                  if r.anchor == "managed-postgres-across-clouds")
    with pytest.raises(SystemExit) as e:
        gen_docs.recipe_blocks(recipe)
    assert "keeping the target following the source" in str(e.value)


def test_the_stored_code_pairs_are_the_converters():
    """Oracle and SQL Server into PostgreSQL are carried by migkit's
    converters; what every pair has (the operator's own file, a model) is
    said once, apart from them."""
    sql, shared, special = gen_docs.stored_code()
    got = {(a, b): kinds for a, b, kinds in special}
    assert "procedure" in got[("oracle", "postgres")]
    assert "procedure" in got[("mssql", "postgres")]
    assert ("postgres", "postgres") not in got
    assert "oracle" in sql and "redis" not in sql
    said = dict(shared)
    assert said["the operator's own file"] == ["view", "function",
                                               "procedure", "trigger",
                                               "event"]
    # a way only some pairs have is never said as every pair's
    assert not [s for s, _ in shared if "Oracle" in s]


def test_the_releases_are_the_tagged_history(pages):
    got = gen_docs.releases()
    if got is None:
        pytest.skip("this copy has no tags to read releases from")
    page = _page(pages, "releases")
    for tag, _, buckets in got:
        assert f'id="release-{tag}"' in page, tag
        assert sum(len(v) for v in buckets.values()) > 0, tag


def test_a_page_from_another_checkout_is_not_read(tmp_path, monkeypatch):
    """An editable install of another checkout answers an import of a
    module this tree lacks with that checkout's: the pages are read only
    from modules this tree has."""
    monkeypatch.setattr(gen_docs, "ROOT", tmp_path)
    (tmp_path / "migkit").mkdir()
    assert gen_docs.optional("envkeys") is None
    assert gen_docs.optional("managed") is None


def test_the_hook_writes_the_pages_again():
    """The hook checks, writes the pages again where one is behind, and
    stops the commit - on the check's own exit code, so a check that
    failed for another reason is not taken for a page behind."""
    hook = (ROOT / ".githooks" / "pre-commit").read_text()
    assert '"$docs" tools/gen_docs.py --check' in hook
    assert re.search(r'^\s*"\$docs" tools/gen_docs\.py$', hook, re.M), hook
    assert re.search(rf"^\s*{gen_docs.BEHIND}\)", hook, re.M), hook
    assert gen_docs.BEHIND != 1


def test_the_check_says_behind_with_its_own_exit_code(monkeypatch, capsys):
    monkeypatch.setattr(gen_docs, "outputs", lambda: {"README.md": "x"})
    assert gen_docs.main(["--check"]) == gen_docs.BEHIND
    assert "README.md is behind the code" in capsys.readouterr().out
    monkeypatch.setattr(gen_docs, "outputs", lambda: {
        "README.md": (ROOT / "README.md").read_text()})
    assert gen_docs.main(["--check"]) == 0
