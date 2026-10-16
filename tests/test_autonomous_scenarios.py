"""The proof harness of `migkit migrate` (docs/research/autonomous-comparison.md):
healthy, recoverable-failure and unrecoverable runs on the MySQL and the
PostgreSQL sandboxes, and the unattended cases P0-VALIDATE.unattended.proof
names that this sandbox can shape - a target capped below its source,
injected defects, repeated interruption and resume, a value above the
target's packet size - each through the installed command, from a hop file
holding only the two addresses, the credentials and the databases, and the
goal on the command line.

Each case is measured the same way - operator actions, completion time,
the source's load read from its own views (connections, rows read a
second), retry waste (recoveries that failed again, units run more than
once) and the forecast's error at a quarter, half and three quarters of
the run - and written as one JSON line a case to `MIGKIT_MEASURE_OUT`
where it is set, so the comparison table is filled from runs, never from
claims (`test_the_table_is_the_runs`)."""
import json
import os
import re
import subprocess
import textwrap
import threading
import time
from pathlib import Path

import pytest
import yaml

from tests import servers
from tests.conftest import needs_docker, migkit_cmd
from tests.mysql_pair import my_pair  # noqa: F401 - a fixture
from tools import ease_measure as ease

pytestmark = needs_docker

ROWS = 20000
#: this checkout: the installed command runs the code under test, not
#: whatever checkout the environment was installed from
ROOT = str(Path(__file__).resolve().parent.parent)
#: the harness's own variables, never handed to the command (migkit
#: refuses a variable of its prefix it does not read)
OWN = ("MIGKIT_MEASURE_OUT",)
DOC = Path(ROOT) / "docs/research/autonomous-comparison.md"
#: what a hop file of an unattended case may hold: the connection fields
#: and the scope, nothing that sizes, times or deepens the work
HOP_KEYS = {"engine", "source", "target", "databases"}
END_KEYS = {"host", "port", "user", "password"}
#: board notes of the bugs these runs exposed in files this harness does
#: not own; the cases they stop are strict xfails until the owner fixes
PG16 = ("W15J-PG16: the bulk copy on the PostgreSQL 16 pair stops on a"
        " parameter of a newer server, and the fallback copier reports the"
        " same cause (movers.py, W15E)")
MYBULK = ("W15J-MYBULK: the MySQL bulk copy's one trial on the rowmeta pair"
          " stops with 'a failure no class here describes' and the copy"
          " is held - no fallback to migkit's own copier in the run")

# MySQL is measured on the profile whose source keeps full row images; a
# copy on the default profile, whose changes cannot be read (D0), is
# `test_a_copy_needs_no_change_reading`
rowmeta = servers.fixture("mysql-rowmeta")
capped = servers.fixture("pg-capped")


class Side:
    """One engine's sandbox: seed the source, read either side."""

    def __init__(self, engine, src, dst):
        self.engine, self.src, self.dst = engine, src, dst
        self.ports = {"src": src.port, "dst": dst.port}
        self.db = "postgres" if engine == "postgres" else "w9c"

    def sql(self, role, sql):
        s = self.src if role == "src" else self.dst
        if self.engine == "postgres":
            got = s.exec("psql", "-U", "postgres", "-d", "postgres", "-At",
                         "-v", "ON_ERROR_STOP=1", "-c", sql)
        else:
            got = s.exec("mysql", "-uroot", "-ptest", "-N", "-B",
                         input=sql)
        assert got.returncode == 0, got.stderr
        return got.stdout.strip().replace("\t", "|")

    def seed(self, rows=ROWS, lob=0, extra=""):
        """`rows` keys in `o`; `lob` bytes in one more row's JSON value."""
        if self.engine == "postgres":
            # the pair is the module's: what an earlier case left on
            # either side is gone, so a case starts from its own seed
            for role in ("src", "dst"):
                self.sql(role, "drop schema public cascade;"
                         " create schema public")
            self.sql("src", "create table o (id int primary key, v text,"
                     " j jsonb); insert into o select g, md5(g::text),"
                     " null from generate_series(1, %d) g" % rows
                     + (";insert into o values (0, 'lob', jsonb_build_object("
                        "'v', repeat('x', %d)))" % lob if lob else "")
                     + extra)
            return
        self.sql("src", f"set session cte_max_recursion_depth = {rows + 1};"
                 " drop database if exists w9c; create database w9c;"
                 " create table w9c.o (id int primary key, v text, j json);"
                 " insert into w9c.o with recursive g(n) as (select 1 union"
                 f" all select n + 1 from g where n < {rows}) select n,"
                 " md5(n), null from g;"
                 + (" insert into w9c.o values (0, 'lob', json_object('v',"
                    f" repeat('x', {lob})));" if lob else "") + extra)
        self.sql("dst", "drop database if exists w9c; create database w9c;")

    def digest(self, role):
        """(rows, distinct keys, a digest of every value) on one side."""
        if self.engine == "postgres":
            sql = ("select count(*), count(distinct id), md5(coalesce("
                   "string_agg(id || ':' || md5(coalesce(v, '') ||"
                   " coalesce(j::text, '')), ',' order by id), '')) from o")
        else:
            sql = ("set session group_concat_max_len = 1073741824; select"
                   " count(*), count(distinct id), md5(coalesce(group_concat("
                   "concat(id, ':', md5(concat(coalesce(v, ''),"
                   " coalesce(cast(j as char), '')))) order by id), ''))"
                   " from w9c.o")
        try:
            a, b, d = self.sql(role, sql).split("|")
            return int(a), int(b), d
        except (AssertionError, ValueError):
            return None

    def load(self):
        """(client connections, rows read so far) on the source, from the
        server's own views."""
        if self.engine == "postgres":
            got = self.sql("src", "select (select count(*) - 1 from"
                           " pg_stat_activity where backend_type ="
                           " 'client backend'), (select coalesce(sum("
                           "seq_tup_read + coalesce(idx_tup_fetch, 0)), 0)"
                           " from pg_stat_user_tables)")
        else:
            got = self.sql("src", "select (select count(*) - 1 from"
                           " information_schema.processlist where command"
                           " <> 'Daemon'), (select coalesce(sum(count_read),"
                           " 0) from performance_schema."
                           "table_io_waits_summary_by_table where"
                           " object_schema = 'w9c')")
        a, b = got.split("|")
        return int(a), int(b)


def _conf(tmp_path, side, password="test", **expert):
    user = "postgres" if side.engine == "postgres" else "root"
    c = tmp_path / "hops.yaml"
    c.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: {side.engine}
            source: {{host: 127.0.0.1, port: {side.ports['src']}, user: {user}, password: test}}
            target: {{host: 127.0.0.1, port: {side.ports['dst']}, user: {user}, password: {password}}}
            databases: [{side.db}]
    """) + "".join(f"    {k}: {v}\n" for k, v in expert.items()))
    if not expert:
        _minimal(c)
    return c


def _minimal(conf):
    """The hop file holds the connection fields and the scope only."""
    hop = yaml.safe_load(conf.read_text())["hops"]["t"]
    assert set(hop) <= HOP_KEYS, set(hop) - HOP_KEYS
    for end in ("source", "target"):
        assert set(hop[end]) <= END_KEYS, set(hop[end]) - END_KEYS


#: a command still running this long is stopped and counted as failed, so a
#: run that waits on itself fails its case instead of holding the harness
#: (the slowest whole case measured took under a minute)
HANG = 600
CUT = ("W16N-CUT: the run is not cut where MIGKIT_FAILPOINT says: the copy"
       " goes by the bulk way, which passes no range.committed, table.done"
       " or checkpoint.written point, so the first run ends whole (MySQL:"
       " 0 of 3 runs cut on either engine, the copy then whole; the crash"
       " case fails at the same step)")
DEFECTS = ("W16N-DEFECTS: the copy before the defects are injected does not"
           " complete when the source also holds a sequence, a role and its"
           " grant. PostgreSQL: the structural schema check (engines/"
           "postgres.py check_structural_diff) errors 'keys must be str, int,"
           " float, bool or None, not tuple' with privileges compared, so the"
           " proof is held - raised before the diff's record is taken, the"
           " exact call not yet traced. MySQL: the proof ends 'w9c is"
           " different after its repair' (the differing finding not read)")
LOB_MYSQL = ("W16N-LOB: on MySQL the 8 MiB value does not arrive whole on a"
             " target holding a 4 MiB packet (the run exits non-zero in 23 s)")

_LEFT = re.compile(r"~([\d.,]+)\s*(s|min|h)\b[^;\n]*\bleft")
_BAND = re.compile(r"([\d.,]+)\s*(?:-|to)\s*([\d.,]+)\s*(s|min|h)\b[^;\n]*"
                   r"\bleft")
_UNIT = {"s": 1, "min": 60, "h": 3600}


def _secs(n, unit):
    return float(n.replace(",", "")) * _UNIT[unit]


def _run(conf, side, *args, **env):
    """One command, its output lines timed as they came and the source's
    load sampled while it ran."""
    full = {k: v for k, v in os.environ.items() if k not in OWN}
    full.update(MIGKIT_CONF=str(conf), PYTHONPATH=ROOT,
                MIGKIT_REPORTS=str(conf.parent / "reports"), **env)
    before = side.load() if side else (0, 0)
    peak, done = [0], threading.Event()

    def sample():
        while not done.wait(0.25):
            try:
                peak[0] = max(peak[0], side.load()[0])
            except Exception:  # noqa: BLE001 - a sample missed, not a run
                pass
    if side:
        threading.Thread(target=sample, daemon=True).start()
    t0 = time.time()
    p = subprocess.Popen(migkit_cmd() + list(args), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, env=full,
                         stdin=subprocess.DEVNULL)
    stop = threading.Timer(HANG, p.kill)
    stop.start()
    lines = [(time.time() - t0, line) for line in p.stdout]
    err = p.stderr.read()
    p.wait()
    stop.cancel()
    took = time.time() - t0
    done.set()
    after = side.load() if side else (0, 0)
    r = subprocess.CompletedProcess(p.args, p.returncode,
                                    "".join(x for _, x in lines), err)
    return {"r": r, "took": took, "args": args, "lines": lines,
            "connections": peak[0], "read": after[1] - before[1]}


def _forecast(run):
    """{quarter: {error, band}} - the last forecast said by 25/50/75% of
    the run, its error |when it said + what it said - took| / took, and
    the band it stated as a fraction of its own figure; None where the
    run said none by then."""
    took, out = run["took"], {}
    for q in (0.25, 0.5, 0.75):
        said = None
        for at, line in run["lines"]:
            if at > q * took:
                break
            b, m = _BAND.search(line), _LEFT.search(line)
            if b:
                lo, hi = _secs(b[1], b[3]), _secs(b[2], b[3])
                mid = (lo + hi) / 2
                said = (at, mid, (hi - lo) / 2 / mid if mid else None)
            elif m:
                said = (at, _secs(m[1], m[2]), None)
        key = f"{int(q * 100)}%"
        out[key] = None
        if said and took:
            at, left, band = said
            out[key] = {"error": round(abs(at + left - took) / took, 3),
                        "band": band}
    return out


def _workflow(conf):
    for p in (conf.parent / "reports").rglob("workflow.json"):
        return json.loads(p.read_text())
    return {}


def _progress(conf):
    """The worker counts the move's own progress files said (`planner.
    read_progress`), one a report directory."""
    from migkit import planner
    got = [planner.read_progress(p.parent)
           for p in (conf.parent / "reports").rglob("progress.json")]
    return [int(g["workers"]) for g in got
            if g and str(g.get("workers", "")).isdigit()]


def _units(conf):
    return {k: u.get("state")
            for k, u in _workflow(conf).get("units", {}).items()}


def _measure(name, conf, runs, side, rows=ROWS, lob=0, **more):
    """The case's numbers, kept where the final suite gathers them."""
    decisions = _workflow(conf).get("decisions") or []
    rec = [d for d in decisions if d.get("what") == "recover"]
    held = [d for d in rec if d.get("held") is True]
    done = {}
    for d in decisions:
        if d.get("what") == "done":
            done[d.get("unit")] = done.get(d.get("unit"), 0) + 1
    src, got = side.digest("src"), side.digest("dst")
    last = runs[-1]["r"]
    took = sum(r["took"] for r in runs)
    m = {"scenario": name, "engine": side.engine,
         # the arguments of the first command, and every run after it the
         # operator had to start
         "inputs": len(runs[0]["args"]), "interventions": len(runs) - 1,
         "operator_actions": len(runs),
         "completed": (last.returncode == 0 and got is not None
                       and got == src and got[0] == got[1] == rows + lob),
         "recoveries": len(rec),
         "recovered": len(held) / len(rec) if rec else None,
         # tries a recovery spent that failed again
         "retry_waste": len(rec) - len(held),
         "units_run_twice": sum(n - 1 for n in done.values() if n > 1),
         "seconds": round(took, 2),
         "load": {"connections": max(r["connections"] for r in runs),
                  "rows_read_per_s": (round(sum(r["read"] for r in runs)
                                            / took) if took else None)},
         "forecast": _forecast(max(runs, key=lambda r: r["took"])),
         "claimed": "proved equal" in last.stdout, **more}
    # the comparison's flat cells: the forecast's error where the run said
    # one (half-way first), the workers the move chose, the questions the
    # operator was asked (the ease measure's own count)
    said = next((m["forecast"][q] for q in ("50%", "75%", "25%")
                 if m["forecast"][q]), None)
    m["forecast_error"] = said["error"] if said else "no forecast said"
    m["forecast_band"] = said["band"] if said else None
    workers = _progress(conf)
    m["workers"] = max(workers) if workers else "not said"
    m["questions"] = sum(ease.measure_transcript(
        r["r"].stdout + r["r"].stderr, r["took"])["questions"] for r in runs)
    out = os.environ.get("MIGKIT_MEASURE_OUT")
    if out:
        with open(out, "a") as f:
            f.write(json.dumps(m) + "\n")
    return m


def _gate(m):
    """A forecast that stated a band is held to it at half the run."""
    half = m["forecast"]["50%"]
    if half and half["band"] is not None:
        assert half["error"] <= half["band"], m["forecast"]


def _pg(src, dst):
    return Side("postgres", src, dst)


def _either(request):
    if request.param == "postgres":
        request.getfixturevalue("pg_pair")
        got = {s.role: s for s in servers.servers_of("pg")}
        return _pg(got["src"], got["dst"])
    space = request.getfixturevalue("rowmeta")
    return Side("mysql", space.src, space.dst)


@pytest.fixture(params=["postgres", "mysql"])
def side(request):
    """A pair whose copy completes."""
    return _either(request)


@pytest.fixture(params=["postgres", "mysql"])
def any_side(request):
    """A pair for a case that ends before any copy."""
    return _either(request)


GO = ("migrate", "--hop", "t", "--goal", "copy", "--go")


def test_a_healthy_copy_completes_in_one_command(side, tmp_path):
    conf = _conf(tmp_path, side)
    side.seed()
    runs = [_run(conf, side, *GO)]
    m = _measure("healthy", conf, runs, side)
    r = runs[0]["r"]
    assert r.returncode == 0, r.stdout + r.stderr
    assert m["completed"] and m["operator_actions"] == 1
    assert "copied and proved equal" in r.stdout
    assert m["retry_waste"] == 0 and m["units_run_twice"] == 0
    _gate(m)


def test_the_baseline_is_the_same_copy_pinned_to_one_worker(side, tmp_path):
    """No switch to fixed defaults exists: the baseline pins the workers
    to 1 through the expert override, the one hop key it adds."""
    conf = _conf(tmp_path, side, workers=1)
    side.seed()
    runs = [_run(conf, side, *GO)]
    m = _measure("baseline, workers pinned to 1", conf, runs, side)
    assert runs[0]["r"].returncode == 0, runs[0]["r"].stdout
    assert m["completed"]


def test_a_fault_mid_copy_is_recovered_inside_the_run(side, tmp_path):
    conf = _conf(tmp_path, side)
    side.seed()
    runs = [_run(conf, side, *GO,
                 MIGKIT_FAILPOINT="range.committed:1:fault=connectivity")]
    m = _measure("recoverable in the run", conf, runs, side)
    r = runs[0]["r"]
    assert r.returncode == 0, r.stdout + r.stderr
    # every key once: the work after the last kept position was not doubled
    assert m["completed"] and m["interventions"] == 0
    assert m["recovered"] in (None, 1.0)


@pytest.mark.xfail(strict=True, reason=CUT)
def test_a_crash_mid_copy_completes_on_the_next_run(side, tmp_path):
    conf = _conf(tmp_path, side)
    side.seed()
    runs = [_run(conf, side, *GO, MIGKIT_FAILPOINT="range.committed:1:exit")]
    assert runs[0]["r"].returncode != 0
    assert "proved equal" not in runs[0]["r"].stdout
    runs.append(_run(conf, side, *GO))
    m = _measure("recoverable by a run again", conf, runs, side)
    assert runs[-1]["r"].returncode == 0, runs[-1]["r"].stdout
    assert m["completed"] and m["interventions"] == 1


@pytest.mark.xfail(strict=True, reason=CUT)
def test_three_interruptions_end_where_a_straight_run_does(side, tmp_path):
    """Cut at three phases - a range committed, a table done, a checkpoint
    written - and run again each time: the same digest and the same units
    as one straight run, no unit finished twice."""
    straight, cut = tmp_path / "straight", tmp_path / "cut"
    straight.mkdir()
    cut.mkdir()
    conf = _conf(straight, side)
    side.seed()
    first = _run(conf, side, *GO)
    assert first["r"].returncode == 0, first["r"].stdout
    want, units = side.digest("dst"), _units(conf)
    conf = _conf(cut, side)
    side.seed()
    runs = [_run(conf, side, *GO, MIGKIT_FAILPOINT=f"{point}:1:exit")
            for point in ("range.committed", "table.done",
                          "checkpoint.written")]
    runs.append(_run(conf, side, *GO))
    m = _measure("interrupted three times", conf, runs, side,
                 cut_short=sum(r["r"].returncode != 0 for r in runs[:-1]))
    assert m["cut_short"] == 3, [r["r"].stdout[-400:] for r in runs]
    assert runs[-1]["r"].returncode == 0, runs[-1]["r"].stdout
    assert side.digest("dst") == want
    assert _units(conf) == units
    assert m["units_run_twice"] == 0


def test_a_value_above_the_targets_packet_size_arrives_whole(side, tmp_path,
                                                            request):
    """One JSON value larger than the target accepts in one packet, in the
    same run as the rest of the table."""
    if side.engine == "mysql":
        request.node.add_marker(pytest.mark.xfail(strict=True,
                                                  reason=LOB_MYSQL))
        side.sql("dst", "set global max_allowed_packet = 4194304")
    conf = _conf(tmp_path, side)
    side.seed(lob=8 << 20)
    runs = [_run(conf, side, *GO)]
    m = _measure("a value above the packet size", conf, runs, side, lob=1)
    assert runs[0]["r"].returncode == 0, runs[0]["r"].stdout
    assert m["completed"]


@pytest.mark.xfail(strict=True, reason=DEFECTS)
def test_injected_defects_are_each_found_and_named(side, tmp_path):
    """After a proved copy: a sequence set behind, a grant missing on one
    table and a row changed on the target - the check names each."""
    if side.engine == "postgres":
        extra = ("; create sequence seq_behind; select setval('seq_behind',"
                 " 5000); do $$ begin create role app_reader; exception"
                 " when duplicate_object then null; end $$;"
                 " grant select on o to app_reader")
        breaks = ("select setval('seq_behind', 1); revoke select on o from"
                  " app_reader; update o set v = 'changed' where id = 7")
    else:
        extra = (" create table w9c.seq_behind (id int auto_increment"
                 " primary key) auto_increment = 5000; create user if not"
                 " exists app_reader identified by 'CHANGE_ME';"
                 " grant select on w9c.o to app_reader;")
        breaks = ("alter table w9c.seq_behind auto_increment = 1; create user"
                  " if not exists app_reader identified by 'CHANGE_ME';"
                  " revoke all on w9c.o from app_reader; update w9c.o set"
                  " v = 'changed' where id = 7")
    conf = _conf(tmp_path, side)
    side.seed(extra=extra)
    first = _run(conf, side, *GO)
    assert first["r"].returncode == 0, first["r"].stdout
    side.sql("dst", breaks)
    runs = [_run(conf, side, "check", "--hop", "t")]
    out = runs[-1]["r"].stdout
    found = {"sequence": "seq_behind" in out, "grant": "app_reader" in out,
             "row": bool(re.search(r"(?im)^\s*data\b.*\b(fail|differ)",
                                   out))}
    _measure("injected defects", conf, runs, side, found=found)
    assert runs[-1]["r"].returncode != 0
    assert all(found.values()), (found, out[-3000:])


def test_a_target_capped_below_its_source_is_sized_to_it(pg_pair, capped,
                                                         tmp_path):
    """The target capped to 1 CPU and 512 MiB by its profile, the source
    not: the copy completes in one command."""
    src = next(s for s in servers.servers_of("pg") if s.role == "src")
    side = _pg(src, capped.one)
    conf = _conf(tmp_path, side)
    side.seed()
    runs = [_run(conf, side, *GO)]
    m = _measure("asymmetric capacity", conf, runs, side)
    assert runs[0]["r"].returncode == 0, runs[0]["r"].stdout
    assert m["completed"]
    _gate(m)


def test_a_copy_needs_no_change_reading(my_pair, tmp_path):  # noqa: F811
    from tests import mysql_pair
    side = Side("mysql", mysql_pair._side("src"), mysql_pair._side("dst"))
    conf = _conf(tmp_path, side)
    side.seed(100)
    run = _run(conf, side, *GO)
    assert run["r"].returncode == 0, run["r"].stdout


def test_a_refused_target_stops_safely_and_claims_nothing(any_side,
                                                          tmp_path):
    side = any_side
    conf = _conf(tmp_path, side, password="CHANGE_ME")
    side.seed(10)
    runs = [_run(conf, side, *GO)]
    m = _measure("unrecoverable", conf, runs, side, rows=10)
    r = runs[0]["r"]
    assert r.returncode != 0
    assert not m["claimed"] and not m["completed"]
    # stopped, not retried into a storm
    assert m["retry_waste"] <= 1
    assert "migrate --hop t --why" in r.stdout


@pytest.mark.xfail(strict=True, reason=PG16)
def test_the_evidence_is_on_demand(pg_pair, tmp_path):
    got = {s.role: s for s in servers.servers_of("pg")}
    side = _pg(got["src"], got["dst"])
    conf = _conf(tmp_path, side)
    side.seed(100)
    _run(conf, side, *GO)
    r = _run(conf, None, "migrate", "--hop", "t", "--why")["r"]
    assert "move:postgres: done" in r.stdout


def test_the_table_is_the_runs():
    """The comparison's numbers are the runs' (`autonomous.comparison`
    over the lines a run wrote): a hand-edited cell fails here."""
    out = os.environ.get("MIGKIT_MEASURE_OUT")
    if not out or not os.path.exists(out):
        pytest.skip("no measured lines in this run")
    from migkit import autonomous
    table = DOC.read_text()
    assert autonomous.comparison(open(out).read().splitlines(),
                                 table) == table
