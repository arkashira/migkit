"""Hiding a password that is not there.

`migkit replicate` prints the SQL it is about to run, with the source
password masked, because `CREATE SUBSCRIPTION` carries that password in
its connection string. The masking was `stmt.replace(password, "****")`,
and `str.replace("", ...)` inserts the replacement between every
character:

    create subscription s connection 'host=h ...'
    ****c****r****e****a****t****e**** ****s****u****b****s****...

An empty password is not an odd case. It is what a hop gets when the
operator authenticates by `trust`, by `.pgpass`, or by a client
certificate - the arrangements that keep a password out of the config file
in the first place. The more careful the setup, the less readable migkit
made the plan it was about to execute.

The rest of this file pins the thing worth knowing about that plan: the
password really is in the statement, so masking it is not decoration.
Measured on PostgreSQL 16 - after `CREATE SUBSCRIPTION`, the target
stores it verbatim:

    select subconninfo from pg_subscription
    host=10.0.0.5 port=5432 dbname=postgres user=postgres password=CHANGE_ME

Readable by a superuser on the target only; `reader`, a plain login role,
got `permission denied for table pg_subscription`. Worth stating at that
size and no larger.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.util import without_secret

STMT = ("create subscription s connection 'host=h port=5432 dbname=d"
        " user=u password=CHANGE_ME' publication p;")


def test_a_real_password_is_hidden():
    got = without_secret(STMT, "CHANGE_ME")
    assert "CHANGE_ME" not in got, got
    assert "password=****" in got, got
    # and nothing else about the statement moved
    assert got.startswith("create subscription s connection"), got


def test_an_empty_password_leaves_the_statement_alone():
    """The bug. Without the guard this comes back shredded."""
    got = without_secret(STMT, "")
    assert got == STMT, got
    assert "****" not in got, got


def test_a_missing_password_leaves_the_statement_alone():
    """`Endpoint.password` defaults to `""`, but a hop read from somewhere
    else could hand over `None`, and `str.replace(None, ...)` raises."""
    assert without_secret(STMT, None) == STMT


def test_it_is_the_helper_the_replicate_plan_uses():
    """One implementation. A second `stmt.replace(...)` somewhere else is
    how the same bug comes back under another name."""
    import pathlib
    cli = pathlib.Path(__file__).resolve().parents[1] / "migkit" / "cli.py"
    text = cli.read_text()
    assert "without_secret(stmt, hop.source.password)" in text
    assert 'replace(hop.source.password' not in text


def test_the_plan_for_a_passwordless_hop_is_readable(tmp_path):
    """End to end on the statement migkit actually generates, for a hop
    with no password in the config at all."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="r", engine="postgres",
              source=Endpoint(host="src", port=5432, user="postgres",
                              password=""),
              target=Endpoint(host="dst", port=5432, user="postgres",
                              password=""),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    sql = PostgresEngine(hop).replicate_sql("appdb")
    for stmt in sql["src"] + sql["dst"]:
        shown = without_secret(stmt, hop.source.password)
        assert shown == stmt, shown
        assert "****" not in shown, shown
    assert any("create subscription" in s for s in sql["dst"]), sql


def test_a_hop_with_a_password_still_has_it_hidden(tmp_path):
    """The other half: the guard must not have turned the masking off."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="r", engine="postgres",
              source=Endpoint(host="src", port=5432, user="postgres",
                              password="CHANGE_ME"),
              target=Endpoint(host="dst", port=5432, user="postgres",
                              password="CHANGE_ME"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    sql = PostgresEngine(hop).replicate_sql("appdb")
    joined = " ".join(sql["src"] + sql["dst"])
    assert "CHANGE_ME" in joined, "the statement really does carry it"
    shown = without_secret(joined, hop.source.password)
    assert "CHANGE_ME" not in shown, shown
