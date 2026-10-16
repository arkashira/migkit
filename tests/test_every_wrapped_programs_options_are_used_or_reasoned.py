"""Every option a wrapped program documents is passed somewhere in migkit's
code or given the reason migkit does not need it (backlog 0f).

The documented option lists are recorded in `tests/fixtures/whole_use/`,
one file a program, written from its reference (the programs are not
installed where these tests run). The account is `migkit.tooluse`'s: an
option is used where its token is in the code that builds the command
line, and every other has a reason row. These hold the account to the
recorded list both ways - an option the program documents with neither a
use nor a reason is *unaccounted*, an entry for an option the program does
not document is stale - and reject a reason that only says the feature
again.
"""
import re
from pathlib import Path

import pytest

from migkit import tooluse

FIXTURES = Path(__file__).parent / "fixtures" / "whole_use"
PROGRAMS = sorted(p.stem for p in FIXTURES.glob("*.txt"))
_OPTION = re.compile(r"^\s+(-[^\s,]+(?:,\s*-[^\s,]+)*)(?:\s+(.*))?$")
_WORD = re.compile(r"[a-z][a-z-]{3,}")
#: words that carry no claim of their own
_FILLER = {"that", "this", "with", "from", "into", "have", "when", "then",
           "than", "the", "and", "for", "its", "not", "are", "was", "does",
           "each", "which", "only", "also", "more", "most", "them", "they"}


def parse(text):
    """[(aliases, description)] of a recorded list: an option line's
    dashed names first, a `feature: NAME :: description` line as its name."""
    out = []
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        if line.startswith("feature:"):
            name, _, desc = line[len("feature:"):].partition("::")
            out.append(([name.strip()], desc.strip()))
            continue
        got = _OPTION.match(line)
        assert got, f"a line the recorded list does not parse: {line!r}"
        names = [re.sub(r"=.*$", "", n.strip())
                 for n in got.group(1).split(",")]
        rest = re.split(r"\s{2,}", (got.group(2) or "").strip())
        out.append((names, rest[-1] if rest[-1] else ""))
    return out


def recorded(program):
    return parse((FIXTURES / f"{program}.txt").read_text())


def key_of(aliases, declared):
    """The one alias the program's account is written under."""
    keys = [a for a in aliases if a in declared]
    assert len(keys) == 1, (aliases, keys)
    return keys[0]


def reason_of(program, key):
    """(state, why) of a written reason, followed through `see <option>`."""
    options = tooluse.REGISTRY[program].options
    got = options.get(("", key))
    seen = set()
    while got and got[1].startswith("see ") \
            and ("", got[1][4:]) in options:
        nxt = got[1][4:]
        assert nxt not in seen, f"{program} {key}: reasons that see each other"
        seen.add(nxt)
        got = options.get(("", nxt))
    return got


def restates(why, description, key):
    """Whether a reason only says the feature again: nearly every word of it
    is already in the option's own name and description."""
    said = set(_WORD.findall(f"{description} {key.strip('-')}".lower()))
    mine = [w for w in _WORD.findall(why.lower()) if w not in _FILLER]
    if len(mine) < 3:
        return True
    return sum(w in said for w in mine) / len(mine) >= 0.75


def audit(program):
    """{"unaccounted": [..], "restated": [..], "undocumented": [..],
    "unrecorded": [..]} for a program against its recorded list."""
    tooluse._load()
    declared = tooluse.surface(program)[""]
    used = set(tooluse.used(program))
    items = recorded(program)
    keys = [key_of(a, declared) for a, _ in items]
    out = {"unaccounted": [], "restated": [],
           "undocumented": sorted(declared - set(keys)),
           "unrecorded": sorted(k for k in keys if k not in declared)}
    for key, (aliases, desc) in zip(keys, items):
        if key in used:
            continue
        got = reason_of(program, key)
        if not got:
            out["unaccounted"].append(key)
        elif restates(got[1], desc, key):
            out["restated"].append(key)
    return out


@pytest.mark.parametrize("program", PROGRAMS)
def test_a_recorded_option_is_used_or_given_its_reason(program):
    got = audit(program)
    assert got["unaccounted"] == [], (
        f"{program}: {len(got['unaccounted'])} documented and neither used"
        " nor reasoned")
    assert got["restated"] == [], f"{program}: a reason that restates it"


@pytest.mark.parametrize("program", PROGRAMS)
def test_the_account_lists_what_the_program_documents_and_no_more(program):
    got = audit(program)
    assert got["undocumented"] == [], f"{program}: not in the recorded list"
    assert got["unrecorded"] == []
    assert tooluse.stale(program) == []
    assert tooluse.unexplained(program) == []


@pytest.mark.parametrize("program", PROGRAMS)
def test_a_use_is_found_in_the_code_and_the_unused_are_not(program):
    """Not an empty account: the program is used with several options, and
    no option is both used and written as unused."""
    tooluse._load()
    used = set(tooluse.used(program))
    written = {k for (_, k), (state, _) in
               tooluse.REGISTRY[program].options.items()
               if state == tooluse.UNUSED}
    assert len(used) >= 3, program
    assert not used & written


def test_every_wrapped_program_has_a_recorded_list_or_is_said_to_lack_one():
    """The programs without a recorded list yet are named, so the gap is a
    list to shrink and not a thing to forget (backlog 0f-REST)."""
    tooluse._load()
    lacking = sorted(
        n for n, t in tooluse.REGISTRY.items()
        if t.declared and n not in PROGRAMS)
    assert set(PROGRAMS) >= {"dvt", "reladiff", "datacompy", "pg_dump",
                             "pg_restore"}
    assert lacking, "every list is recorded: this guard can be tightened"


# --- the checks hold: made-up accounts the audit must reject ---------------

def test_a_reason_that_restates_the_feature_is_rejected():
    assert restates("sample a random set of rows", "compare a random sample"
                    " of rows", "--use-random-row")
    assert not restates(
        "the batch file already says what was read, in migkit's own words",
        "print the validation instead of running it", "--dry-run")


def test_a_documented_option_without_a_use_or_a_reason_is_unaccounted(
        monkeypatch):
    text = (FIXTURES / "reladiff.txt").read_text() \
        + "  --made-up-switch               an option no account knows\n"
    monkeypatch.setitem(
        tooluse.REGISTRY["reladiff"].declared, "",
        frozenset({*tooluse.REGISTRY["reladiff"].declared[""],
                   "--made-up-switch"}))
    monkeypatch.setattr(tooluse.REGISTRY["reladiff"], "installed", None)
    monkeypatch.setitem(globals(), "recorded", lambda p: parse(text))
    assert audit("reladiff")["unaccounted"] == ["--made-up-switch"]


def test_the_recorded_list_parses_aliases_and_features():
    got = parse("# note\n  -k, --key-columns TEXT   columns of a row\n"
                "feature: PandasCompare :: compare two frames\n")
    assert got == [(["-k", "--key-columns"], "columns of a row"),
                   (["PandasCompare"], "compare two frames")]
