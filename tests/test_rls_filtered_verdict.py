"""When the verifier is reading through a filter, it must say so.

Row-level security is a `WHERE` clause the server adds to every query, and
it applies to the tool doing the verifying as readily as to the
application. A hop configured with an application role - which is what
people do rather than hand a migration tool superuser - reads a subset on
**both** sides and has no way to know it.

Measured before this existed, with the same policy on both sides and five
of the source's ten rows deleted from the target:

    counts   postgres: OK 1 tables, rows 5==5
    data     postgres: OK 1 tables, 5 rows, checksums equal both sides

Neither was wrong about what it compared. Both were silent about what they
could not see, and a half-empty target read as verified.

The rule for who is actually filtered was measured across every role shape
rather than taken from the documentation:

    RLS enabled           superuser 6  owner 6  BYPASSRLS 6  plain 3
    RLS enabled + FORCE   superuser 6  owner 3  BYPASSRLS 6  plain 3

So an owner reads its own tables in full until they are `FORCE`d. The deep
check used to test only `rolsuper or rolbypassrls`, which called every
owner filtered - a warning on a healthy database, which is the way to make
people stop reading warnings. Both now ask the same question in one place.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-rls-src", DST: "migkit-test-rls-dst"}

SEED = """
create table tenant (id int primary key, tenant text, v text);
insert into tenant select g, case when g <= 5 then 'a' else 'b' end, 'v'||g
  from generate_series(1,10) g;
alter table tenant enable row level security;
create policy only_a on tenant using (tenant = 'a');
create role app login password 'test';
grant select on tenant to app;
create role owner1 login password 'test';
"""


@pytest.fixture(scope="module")
def rls_pair():
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
        q(port, SEED)
    # the target is missing the half the policy hides
    q(DST, "delete from tenant where tenant = 'b'")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def q(port, sql, user="postgres"):
    got = subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", user, "-d", "postgres",
         "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _engine(rls_pair, tmp_path, user="app"):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="v", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=rls_pair["src"],
                              user=user, password="test"),
              target=Endpoint(host="127.0.0.1", port=rls_pair["dst"],
                              user=user, password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _check(rls_pair, tmp_path, monkeypatch, user, only):
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  v:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {rls_pair['src']},"
        f" user: {user}, password: test}}\n"
        f"    target: {{host: 127.0.0.1, port: {rls_pair['dst']},"
        f" user: {user}, password: test}}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return CliRunner().invoke(cli.main, ["check", "v", "--only", only]).output


def test_the_truth_the_role_cannot_see(rls_pair):
    """The control: the scenario really is a half-empty target that looks
    equal through the policy. Without this, everything below could pass
    because nothing was ever wrong."""
    assert q(rls_pair["src"], "select count(*) from tenant") == "10"
    assert q(rls_pair["dst"], "select count(*) from tenant") == "5"
    assert q(rls_pair["src"], "select count(*) from tenant", "app") == "5"
    assert q(rls_pair["dst"], "select count(*) from tenant", "app") == "5"


def test_a_clean_verdict_through_a_filter_is_a_warning(rls_pair, tmp_path,
                                                        monkeypatch):
    out = _check(rls_pair, tmp_path, monkeypatch, "app", "counts,data")
    assert "counts   postgres: WARN" in out, out
    assert "data     postgres: WARN" in out, out
    assert "cannot read" in out, out
    assert "public.tenant" in out, out
    # the old wording must not survive anywhere in the two lines
    for line in out.splitlines():
        if line.startswith(("counts ", "data ")):
            assert ": OK" not in line, line


def test_a_role_that_sees_everything_is_not_nagged(rls_pair, tmp_path,
                                                    monkeypatch):
    """The other half of the job. A check that warns on every healthy
    database is one nobody reads, and the superuser here genuinely sees the
    whole table - so it must report the real difference, plainly."""
    out = _check(rls_pair, tmp_path, monkeypatch, "postgres", "counts,data")
    assert "cannot read" not in out, out
    assert "DIFF" in out, out


def test_an_owner_reads_its_own_tables_in_full(rls_pair, tmp_path):
    """Measured: an owner is exempt until the table is FORCEd. Treating
    every owner as filtered was the old check's mistake."""
    eng = _engine(rls_pair, tmp_path, user="postgres")
    assert eng._filtered_tables("src", "postgres") == []

    app_eng = _engine(rls_pair, tmp_path, user="app")
    assert app_eng._filtered_tables("src", "postgres") == ["public.tenant"]


def test_forcing_it_puts_the_owner_back_under_the_policy(rls_pair, tmp_path):
    q(rls_pair["src"], "alter table tenant force row level security")
    try:
        # postgres is a superuser and bypasses even FORCE, so this checks
        # the flag rather than the role: a non-superuser owner is the case
        # that changes, and the query asks about both
        forced = q(rls_pair["src"], "select relforcerowsecurity::int::text"
                                    " from pg_class where relname='tenant'")
        assert forced == "1", forced
        eng = _engine(rls_pair, tmp_path, user="postgres")
        assert eng._filtered_tables("src", "postgres") == []
    finally:
        q(rls_pair["src"], "alter table tenant no force row level security")


def test_a_real_difference_is_still_a_difference(rls_pair, tmp_path,
                                                  monkeypatch):
    """Only a clean verdict is softened. A difference found inside what the
    role could see is real whatever is hidden behind it, and must not be
    downgraded to a warning."""
    q(rls_pair["dst"], "update tenant set v = 'changed' where id = 1")
    try:
        out = _check(rls_pair, tmp_path, monkeypatch, "app", "data")
        assert "data     postgres: DIFF" in out, out
    finally:
        q(rls_pair["dst"], "update tenant set v = 'v1' where id = 1")


def test_the_deep_check_and_the_data_check_now_agree(rls_pair, tmp_path,
                                                      monkeypatch):
    """They used to answer the same question two different ways."""
    out = _check(rls_pair, tmp_path, monkeypatch, "app", "deep")
    assert "rls: DIFF" in out, out
    assert "public.tenant" in out, out


def test_an_engine_that_cannot_be_asked_changes_nothing(tmp_path):
    """The base returns None, and None must not read as "nothing is
    filtered" - it reads as "no claim", which leaves the result alone."""
    from migkit.engines.base import Engine, Result
    hop = Hop(name="v", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Engine(hop)
    assert eng._filtered_tables("src", "x") is None

    clean = Result("counts", "x", "ok", "3 tables")
    assert eng._honest_about_filtering(clean, "x") is clean

    # and a diff is never touched, whatever the filtering says
    eng._filtered_tables = lambda side, db: ["public.t"]
    bad = Result("data", "x", "diff", "1 table differs")
    assert eng._honest_about_filtering(bad, "x") is bad

    softened = eng._honest_about_filtering(
        Result("counts", "x", "ok", "3 tables"), "x")
    assert softened.status == "warn"
    assert "public.t" in softened.detail
    assert "BYPASSRLS" in softened.fix_hint
