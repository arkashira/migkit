"""The order the steps go in, and what it costs to get it wrong.

`migkit schema --setup` prints a plan an operator runs by hand. It used to
restore the whole schema in one piece, which puts every secondary index on
the table *before* the data arrives - so the load maintains them one row at
a time. Measured on 1,000,000 rows with three secondary indexes, on the
sandbox's two shared cores:

    indexes already present, then load      4.48 s    275 MB
    load, then build the same indexes       2.53 s    218 MB
                                          (0.91 + 1.62)

Not quite twice the time, and the part that does not go away: the table
loaded with its indexes in place is **26% larger on disk**. An index
maintained one row at a time does not pack the way one built in a single
pass does, and nothing later reclaims that.

What this tick also measured, and did *not* ship: raising
`maintenance_work_mem` from the 64 MB default to 1 GB and
`max_parallel_maintenance_workers` from 2 to 4 made the same three index
builds take **1.65 s against 1.54 s** - no better, slightly worse, on two
vCPUs. The 21.99 s / 12.82 s figures in the research notes came from an
article on other hardware. They are not reproducible here, so migkit does
not recommend the setting.

And the premise that sent this tick looking: **migkit never builds an index
itself.** There is no `CREATE INDEX` in any code path - only in hint text.
The one place indexes get built under migkit's guidance is this printed
plan, which is why this is where the ordering had to be fixed.
"""
import subprocess

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="plan", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_the_data_step_sits_between_the_two_schema_halves(pg_pair, tmp_path):
    steps = _engine(pg_pair, tmp_path).setup_target_plan("postgres")
    joined = "\n".join(steps)
    pre = next(i for i, s in enumerate(steps) if "--section=pre-data" in s)
    load = next(i for i, s in enumerate(steps) if "migkit move" in s)
    post = next(i for i, s in enumerate(steps) if "--section=post-data" in s)
    assert pre < load < post, joined

    # the old advice to disable FKs and triggers by hand is gone, because
    # post-data means they are not there during the load in the first place
    assert "disable FK" not in joined, joined
    assert "drop or disable" not in joined, joined


def test_the_plan_carries_the_number_that_justifies_the_order(pg_pair,
                                                               tmp_path):
    """An operator asked to run six commands in a particular order deserves
    to be told why that order, in figures they can check."""
    joined = "\n".join(_engine(pg_pair, tmp_path).setup_target_plan("postgres"))
    assert "2.53 s against 4.48 s" in joined, joined
    assert "218 MB instead of 275 MB" in joined, joined


def test_the_plan_names_this_hop_rather_than_a_placeholder(pg_pair,
                                                            tmp_path):
    joined = "\n".join(_engine(pg_pair, tmp_path).setup_target_plan("postgres"))
    assert "migkit move plan --mode full --go" in joined, joined
    # and the post-data build uses the hop's own parallelism
    assert "--section=post-data -j 2" in joined, joined


def test_the_split_really_does_what_the_plan_says(pg_pair):
    """The steps are only worth printing if they work. This runs the two
    halves against a live pair and checks the seam: no indexes after
    pre-data, indexes after post-data, data intact throughout."""
    src, dst = pg_pair["src"], pg_pair["dst"]
    got = psql(src, "drop table if exists public.plan_t;"
                    " create table public.plan_t (id bigint primary key,"
                    " a text);"
                    " create index plan_t_a on public.plan_t (a);"
                    " insert into public.plan_t select g, 'v'||g from"
                    " generate_series(1,500) g;")
    assert got.returncode == 0, got.stderr
    psql(dst, "drop table if exists public.plan_t;")

    subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", "migkit-test-pg-src",
         "pg_dump", "-U", "postgres", "-d", "postgres", "-t", "public.plan_t",
         "-Fc", "--schema-only", "-f", "/tmp/plan.dump"],
        check=True, capture_output=True)
    subprocess.run(["docker", "cp", "migkit-test-pg-src:/tmp/plan.dump",
                    "/tmp/plan.dump"], check=True, capture_output=True)
    subprocess.run(["docker", "cp", "/tmp/plan.dump",
                    "migkit-test-pg-dst:/tmp/plan.dump"], check=True,
                   capture_output=True)

    def restore(section):
        return subprocess.run(
            ["docker", "exec", "-e", "PGPASSWORD=test", "migkit-test-pg-dst",
             "pg_restore", "-U", "postgres", "-d", "postgres", "--no-owner",
             f"--section={section}", "/tmp/plan.dump"],
            capture_output=True, text=True)

    def indexes():
        return psql(dst, "select count(*) from pg_indexes where tablename ="
                         " 'plan_t'").stdout.strip()

    assert restore("pre-data").returncode == 0
    assert indexes() == "0", indexes()

    # the data lands into a table with no index to maintain
    loaded = subprocess.run(
        ["docker", "exec", "-i", "-e", "PGPASSWORD=test",
         "migkit-test-pg-dst", "psql", "-U", "postgres", "-c",
         "copy public.plan_t (id, a) from stdin"],
        input="".join(f"{i}\tv{i}\n" for i in range(1, 501)),
        capture_output=True, text=True)
    assert loaded.returncode == 0, loaded.stderr

    assert restore("post-data").returncode == 0
    assert indexes() == "2", indexes()   # the primary key and plan_t_a
    rows = psql(dst, "select count(*) from public.plan_t").stdout.strip()
    assert rows == "500", rows


def test_migkit_still_builds_no_index_of_its_own():
    """The premise this tick started from, pinned so it cannot rot quietly.
    If migkit ever does build an index, the measurements above become its
    problem rather than the operator's, and this test is where that gets
    noticed."""
    import pathlib
    import re
    root = pathlib.Path(__file__).resolve().parent.parent / "migkit"
    building = []
    for path in root.rglob("*.py"):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            # a statement being executed, not a word inside a hint or a
            # docstring: look for it in an f-string or a quoted SQL literal
            # that is not obviously prose
            if re.search(r"""(execute|_psql|_sh|_q)\(.*create index""",
                         line, re.I):
                building.append(f"{path.name}:{n}")
    assert building == [], building
