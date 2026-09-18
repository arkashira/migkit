"""Client tools running ahead of the server they are pointed at.

The rule is one-sided on purpose: a client older than its server is normal and
fails loudly when it matters, a client newer emits settings the server has
never heard of and the transaction aborts halfway. Only the second direction
is reported.
"""
from migkit import toolversion as tv


def test_real_version_strings_parse():
    """The two formats, copied from the tools as they actually print."""
    assert tv.major("pg_dump (PostgreSQL) 18.6", "pg_dump") == 18
    assert tv.major("psql (PostgreSQL) 16.15", "psql") == 16
    assert tv.major("mysqldump  Ver 9.7.1 for macos26.4 on arm64 (Homebrew)",
                    "mysqldump") == 9
    assert tv.major("mysql  Ver 8.0.42 for Linux on x86_64", "mysql") == 8


def test_an_unrecognised_string_is_none_not_a_guess():
    assert tv.major("some other tool v3", "pg_dump") is None
    assert tv.major("", "pg_dump") is None
    assert tv.major("pg_dump (PostgreSQL) 18.6", "not-a-tool") is None


def test_server_version_parses_from_what_the_engine_reports():
    assert tv.server_major("16.15") == 16
    assert tv.server_major("8.0.42") == 8
    assert tv.server_major("8.0.30-txsql") == 8
    assert tv.server_major(None) is None
    assert tv.server_major("unknown") is None


def test_only_a_newer_client_is_reported():
    assert tv.skew(18, 16)              # ahead - the direction that breaks
    assert tv.skew(16, 16) == ""        # equal
    assert tv.skew(14, 16) == ""        # behind is normal, and says so itself


def test_nothing_is_claimed_when_either_side_is_unknown():
    assert tv.skew(None, 16) == ""
    assert tv.skew(18, None) == ""


def test_the_finding_names_both_versions_and_when_it_fails():
    why = tv.skew(18, 16)
    assert "client is 18" in why and "server is 16" in why
    # the shape of the failure matters: it is not a connection error
    assert "mid-transaction" in why


def test_report_covers_only_this_engines_tools():
    tools = {"pg_dump": "pg_dump (PostgreSQL) 18.6",
             "mysqldump": "mysqldump  Ver 9.7.1 for x on y"}
    pg = tv.report(tools, "16.15", "postgres")
    assert [t for _, t, _ in pg] == ["pg_dump"]
    my = tv.report(tools, "8.0.42", "mysql")
    assert [t for _, t, _ in my] == ["mysqldump"]


def test_a_matching_pair_passes_and_an_ahead_pair_warns():
    ahead = tv.report({"pg_dump": "pg_dump (PostgreSQL) 18.6"},
                      "16.15", "postgres")
    assert ahead[0][0] == "warn", ahead
    ok = tv.report({"pg_dump": "pg_dump (PostgreSQL) 16.9"},
                   "16.15", "postgres")
    assert ok[0][0] == "pass", ok


def test_a_tool_that_is_not_installed_is_left_to_doctor():
    assert tv.report({"pg_dump": None}, "16.15", "postgres") == []


def test_an_unparseable_client_version_is_unknown_not_clean():
    out = tv.report({"pg_dump": "pg_dump (weird build)"}, "16.15", "postgres")
    assert out[0][0] == "warn"
    assert "unknown, not clean" in out[0][2]
