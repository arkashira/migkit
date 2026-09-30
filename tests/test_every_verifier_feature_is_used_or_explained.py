"""Every documented feature of the three verifiers migkit wraps is either
used, read off the code, or given the reason it is not (backlog 0f).

The table is the tool-use registry's (`migkit.tooluse`), the one every
wrapped tool's account is in; `migkit.verifiers` registers these three,
their uses read off the code that drives them. These hold it to the code
both ways, and hold the documented list to the flags the installed
programs really have - a flag added in a new version with neither a use
nor a reason fails here rather than going unnoticed.
"""
import re
import subprocess
import sys
from pathlib import Path

import pytest

from migkit import second_reader as sr
from migkit import tooluse
from migkit import verifiers as v

HELP = Path(__file__).parent / "reader_help.py"
VERIFIERS = ("dvt", "reladiff", "datacompy")


def _documented(tool):
    return set(tooluse.surface(tool)[""])


def test_no_documented_feature_lacks_a_use_or_a_reason():
    for tool in VERIFIERS:
        assert tooluse.unexplained(tool) == [], tool


def test_no_reason_is_given_for_a_feature_the_code_uses():
    for tool in VERIFIERS:
        assert tooluse.stale(tool) == [], tool


def test_the_table_is_the_registrys_and_nowhere_else():
    """One registry: the verifiers keep no table of their own to ask."""
    for gone in ("TOOLS", "matrix", "undeclared", "stale", "used"):
        assert not hasattr(v, gone), gone
    rows = tooluse.tools_used()
    for tool in VERIFIERS:
        row = rows[tool]
        assert row["licence"] and row["terms"] == "open", tool
        used = set(tooluse.used(tool))
        assert used, tool
        unused = {o for o in row[tooluse.UNUSED]}
        assert used | unused == _documented(tool), tool
        assert not used & unused, tool
        # a use is read off the code, never written
        assert not [k for k, (state, _) in
                    tooluse.REGISTRY[tool].options.items()
                    if state == tooluse.USED], tool
        assert tooluse.REGISTRY[tool].release, tool
    from migkit import tools
    assert rows["dvt"]["licence"] == tools.SECOND_READER_LICENCE


def test_what_0f_asked_for_is_used(monkeypatch):
    """The features the owner named for the second reader, used - read off
    the code, not declared."""
    used = set(tooluse.used("dvt"))
    for feature in ("--hash", "--primary-keys", "--comparison-fields",
                    "--grouped-columns", "--threshold", "--bit_xor",
                    "--std", "--avg", "--use-random-row",
                    "--random-row-batch-size", "--filter-status",
                    "--exclude-columns", "--case-insensitive-match",
                    "--config-file", "configs run",
                    "validate custom-query column", "--labels"):
        assert feature in used, feature
    assert {"all_mismatch", "column_stats", "df1_unq_rows"} <= set(
        tooluse.used("datacompy"))
    assert {"--bisection-factor", "--bisection-threshold",
            "--threads"} <= set(tooluse.used("reladiff"))


def test_taking_a_use_away_is_noticed(monkeypatch):
    """The reading has teeth: the code no longer asking for grouped
    aggregates leaves a documented feature with neither use nor reason,
    and the code starting to use one given a reason makes the reason
    stale."""
    real = tooluse._tokens
    monkeypatch.setattr(tooluse, "_tokens", lambda files: real(files)
                        - {"--grouped-columns"})
    assert ("", "--grouped-columns") in tooluse.unexplained("dvt")
    assert ("", "--grouped-columns") in tooluse.stale("dvt")
    assert "--grouped-columns" not in tooluse.used("dvt")
    monkeypatch.setattr(tooluse, "_tokens", lambda files: real(files)
                        | {"--concat"})
    assert ("", "--concat") in tooluse.stale("dvt")
    assert tooluse.entry("dvt", "", "--concat") == (tooluse.USED,
                                                    tooluse.READ_OFF)


def _reader_flags(python):
    got = subprocess.run([python, str(HELP)], capture_output=True,
                         text=True, timeout=300, cwd="/")
    assert got.returncode == 0, got.stderr[-400:]
    return set(got.stdout.split())


@pytest.mark.skipif(not sr.interpreter(),
                    reason="the second reader is not installed")
def test_every_flag_the_installed_reader_has_is_in_the_table():
    flags = _reader_flags(sr.interpreter()) - {"--help"}
    assert len(flags) > 40, flags
    documented = _documented("dvt")
    assert sorted(flags - documented) == []


def _comparison_program():
    found = Path(sys.executable).parent / "reladiff"
    return str(found) if found.exists() else None


@pytest.mark.skipif(not _comparison_program(),
                    reason="the comparison program is not installed")
def test_every_option_the_comparison_program_has_is_in_the_table():
    said = subprocess.run([_comparison_program(), "--help"],
                          capture_output=True, text=True, timeout=120).stdout
    flags = set(re.findall(r"(?<![\w-])(--[a-z][a-z-]*)", said)) - {"--help"}
    assert len(flags) > 20, flags
    assert sorted(flags - _documented("reladiff")) == []


@pytest.mark.skipif(not _comparison_program(),
                    reason="the comparison program is not installed")
def test_every_flag_migkit_hands_the_comparison_program_is_one_it_has(
        tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.generic import GenericEngine
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              options={"url": "postgresql://10.0.0.1/x"})
    said = subprocess.run([_comparison_program(), "--help"],
                          capture_output=True, text=True, timeout=120).stdout
    cmd = GenericEngine(hop)._reladiff_cmd("t", ["-c", "%", "--json"], jobs=2)
    longs = {a for a in cmd if isinstance(a, str) and a.startswith("--")}
    assert {"--bisection-factor", "--bisection-threshold"} <= longs
    for flag in longs:
        assert re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", said), flag
    # and with nothing known of the table, its own defaults, spelled out
    assert cmd[cmd.index("--bisection-factor") + 1] == str(v.FACTOR)
    assert cmd[cmd.index("--bisection-threshold") + 1] == str(v.THRESHOLD)


def test_every_public_method_of_the_frame_comparison_is_in_the_table():
    import datacompy
    methods = {n for n in dir(datacompy.PandasCompare)
               if not n.startswith("_")
               and callable(getattr(datacompy.PandasCompare, n))}
    assert len(methods) > 10, methods
    assert sorted(methods - _documented("datacompy")) == []


# --- the comparison program as a rung --------------------------------------

def test_the_rung_is_sized_from_the_tables_facts():
    bare = v.reladiff_rung()
    assert (bare["bisection_factor"], bare["bisection_threshold"],
            bare["depth"]) == (v.FACTOR, v.THRESHOLD, 0)
    # 10,000,000 rows of 100 bytes: a leaf of 8 MB is 83,886 rows, held to
    # 65,536; 10,000,000 / 65,536 = 152.6, and 32 ** 2 > 152.6 > 32
    big = v.reladiff_rung(10_000_000, 1_000_000_000, threads=3)
    assert big["bisection_threshold"] == 65536
    assert big["depth"] == 2 and big["threads"] == 3
    # 1,000,000 rows of 4,000 bytes: 2,097 rows a leaf
    wide = v.reladiff_rung(1_000_000, 4_000_000_000)
    assert wide["bisection_threshold"] == 2097
    assert wide["depth"] == math_ceil_log(1_000_000 / 2097, 32)
    for rung in (bare, big, wide, v.reladiff_rung(10, 10**12)):
        # measured: the program refuses to start otherwise
        assert rung["bisection_factor"] < rung["bisection_threshold"], rung
    assert v.reladiff_rung(threads=0)["threads"] == 1


def math_ceil_log(x, base):
    import math
    return math.ceil(math.log(x) / math.log(base))


def test_the_rung_says_where_it_is_chosen_and_why_not_elsewhere():
    across = v.reladiff_rung(engines=("mysql", "postgres"))
    assert across["chosen"] is False and "digest" in across["reason"]
    pair = v.reladiff_rung(engines=("postgres", "postgres"))
    assert pair["chosen"] is None and "verify passes" in pair["reason"]
    assert v.reladiff_rung(engines=("generic",))["chosen"] is True
    assert v.reladiff_rung(engines=("mysql", "mysql"),
                           asked=True)["chosen"] is True
    gone = v.reladiff_rung(engines=("generic",), installed=False)
    assert gone["chosen"] is False and "not installed" in gone["reason"]
    assert any("lower than the threshold" in f for f in pair["facts"])
    assert any("3.14" in f for f in pair["facts"])
