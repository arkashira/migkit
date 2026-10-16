"""A managed transfer service's own table on the target is not the
operator's data (field run 2026-10-16: a MySQL -> PostgreSQL run behind
Tencent DTS read `__dst__tencentdb__sync_<id>` on the target as a table of
the application's). One map (`leftovers.PROVIDER_TABLES`) feeds the scope
(`Hop.excluded` through `bookkeeping`), the listing (`whose`, `group`) and
the line said (`provider_table`)."""
from migkit import leftovers
from migkit.config import Endpoint, Hop

SYNC = "__dst__tencentdb__sync_6fve1pd0"


def _hop():
    return Hop(name="dts", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="u",
                               password="CHANGE_ME"),
               databases=["shop"])


def test_the_sync_back_table_is_left_out_of_the_scope():
    hop = _hop()
    assert hop.excluded("shop", "public", SYNC)
    assert hop.excluded("shop", SYNC)
    # the application's tables, even ones a word of the provider's begins
    assert not hop.excluded("shop", "public", "orders")
    assert not hop.excluded("shop", "public", "dst_sync_jobs")


def test_the_listing_names_the_provider():
    assert leftovers.whose(SYNC) == "Tencent DTS"
    found = leftovers.group([("table", f"public.{SYNC}"),
                             ("table", "public.orders")])
    assert found == {"Tencent DTS": [f"table public.{SYNC}"]}


def test_the_line_says_whose_it_is_and_what_is_left_out():
    from migkit import cli

    class Target:
        def neutral_tables(self, side, db):
            assert side == "dst"
            return ["orders", "__dst__tencentdb__sync_6fve1pd0",
                    "awsdms_apply_exceptions"]
    said = cli._not_your_data(Target(), ["shop"])
    assert said[0] == ("__dst__tencentdb__sync_6fve1pd0: the provider's"
                       " sync-back table (Tencent DTS), not your data: left"
                       " out of scope, counts and the check")
    # a provider whose tables are not known by name narrows nothing: its
    # line is a note beside the listing, not a table left out
    assert not any("not told apart" in line for line in said)
    for p in leftovers.PROVIDERS_UNVERIFIED:
        assert any(line.startswith(f"a sync-back or control table of {p} is"
                                   " not told apart")
                   for line in leftovers.providers_untold())
    assert not any(line.startswith("orders") for line in said)
    # a target with none of a provider's tables leaves nothing out, so the
    # check it runs is whole (the verdict reads no narrowing from it)
    class Clean:
        def neutral_tables(self, side, db):
            return ["orders", "log"]
    assert cli._not_your_data(Clean(), ["shop"]) == []

    class Refusing:
        def neutral_tables(self, side, db):
            raise RuntimeError("permission denied")
    (line,) = [x for x in cli._not_your_data(Refusing(), ["shop"])
               if x.startswith("shop:")]
    assert "could not be listed (permission denied)" in line
    lines = leftovers.provider_tables(["orders", SYNC,
                                       "awsdms_apply_exceptions"])
    assert lines == [
        f"{SYNC}: the provider's sync-back table (Tencent DTS), not your"
        " data: left out of scope, counts and the check",
        "awsdms_apply_exceptions: the provider's control table (AWS DMS),"
        " not your data: left out of scope, counts and the check"]
    assert leftovers.provider_table("orders") is None


def test_one_map_feeds_the_scope_and_the_line():
    for mover, pat, _ in leftovers.PROVIDER_TABLES:
        assert (mover, pat) in leftovers.NOT_DATA_TABLES


def test_scope_check_and_report_say_the_same_provider_lines(monkeypatch):
    """`migkit scope` takes the provider lines from the one place the
    check takes them (`cli._provider_lines`), and the report reads back
    what the check saved: the lines the verdict was narrowed by."""
    from click.testing import CliRunner

    from migkit import cli, verdict

    class Target:
        def neutral_tables(self, side, db):
            return ["orders", SYNC]

        def databases(self):
            return ["shop"]
    hop = _hop()
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "_require_configured", lambda h: None)
    monkeypatch.setattr(cli, "get_engine", lambda h: Target())
    got = CliRunner().invoke(cli.main, ["scope", "dts"])
    assert got.exit_code == 0, got.output
    said = []
    theirs = cli._provider_lines(Target(), ["shop"], said.append)
    assert theirs == cli._not_your_data(Target(), ["shop"]) and theirs
    # the check's lines are these, then the providers it cannot tell apart
    assert said == theirs + leftovers.providers_untold()
    flat = " ".join(got.output.split())
    for line in said:
        assert " ".join(line.split()) in flat, (line, got.output)
    assert "takes everything, whole" in flat
    # the report reads them back from the verdict the check saved
    why = verdict.decide([{"check": "data", "scope": "shop", "status": "ok",
                           "detail": ""}], hop,
                         {"not your data": theirs}).why
    assert "not your data" in why
    # a target with none leaves nothing to say, in scope as in check
    class Clean(Target):
        def neutral_tables(self, side, db):
            return ["orders"]
    assert cli._provider_lines(Clean(), ["shop"], said.append) == []
    monkeypatch.setattr(cli, "get_engine", lambda h: Clean())
    clean = CliRunner().invoke(cli.main, ["scope", "dts"])
    assert "not your data" not in clean.output
