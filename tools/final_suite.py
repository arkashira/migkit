"""The one final run of the whole suite, in the order that makes it fast
and a record that makes it trustworthy (audit T3, T7).

1. Preflight: the test plugins, what programs this machine has (so a "not
   installed here" is known before the run, not found in it), the docker
   daemons and their free memory.
2. Collect once, with every test's marks (`tests/conftest.py` marks what
   needs docker and what needs the migkit VM by what the test reaches).
3. The pure tests, side by side (`-n`, by CPUs and free memory): a broken
   import fails in minutes. The images the docker lanes run are pulled
   meanwhile, and their digests recorded.
4. The docker tests in lanes under the docker lock taken once for the
   whole of docker (`tools/with_docker_lock.py:exclusive`): lanes on the
   default profile and on the migkit VM at once, as many as each daemon's
   memory holds. Files that fix the same host port or container name run
   in one lane; files naming lane 0's PostgreSQL pair run on lane 0; files
   with a test for the VM run in the VM lane; heavy files only where there
   is room. Within a lane, files are balanced by their measured times from
   the last run (`reports/final-suite/times.json`).
5. Every test under `MIGKIT_TEST_STRICT=1` (a missing server, program or
   module fails; `--strict server` where programs may be missing, as on
   CI) and a timeout; a lane that stops answering past its
   test's timeout is ended, its containers removed, and the rest of its
   files run on.
6. The failed tests run again once, alone: failed twice is failed; passed
   the second time is listed as flaky.
7. A report (`reports/final-suite/<stamp>/report.md` and `.json`): time
   per module and per lane, failures with their first words, skips by
   reason, images by digest, what was not installed. No container, network
   or volume the run made is left behind.

    python tools/final_suite.py                  # everything
    python tools/final_suite.py --plan           # the lanes, nothing run
    python tools/final_suite.py --only unit      # CI: one phase
    python tools/final_suite.py --only docker -m "not mssql"

Measured: see `ESTIMATE` below; the numbers there are replaced by the
first full run's (`report.md` prints the run's own).
"""
import argparse
import collections
import concurrent.futures
import datetime
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

import with_docker_lock as lock  # noqa: E402
from tests import servers  # noqa: E402

PY = sys.executable
TIMEOUT = 900
#: what fails instead of skipping (`tests/conftest.py:_strict`)
STRICT = "1"
#: pytest arguments every phase adds (`-k`)
EXTRA = []
#: after `-p`: no cache written by lanes at once, and one root and one
#: configuration whatever the files, so a test's id always names its file
#: from the checkout's root
PYTEST = ["no:cacheprovider", "--rootdir", str(ROOT), "-c",
          str(ROOT / "pyproject.toml")]
#: a lane that writes nothing for its test's timeout and this long is hung
GRACE = 300
#: memory of a lane, MB: one that may run heavy files, one that may not,
#: and the VM lane; kept back on each daemon for itself
ANY_LANE_MB, LIGHT_LANE_MB, VM_LANE_MB, RESERVE_MB = 2400, 1000, 3000, 512
#: a file whose containers hold more than this runs only in a lane that
#: may run heavy files
HEAVY_MB = 1200
#: the VM profile's size, as `with_docker_lock.VM` starts it
VM_MB, VM_CPUS = 6144, 4
#: what a file costs where no run has measured it, seconds: a start, a
#: test, a container it starts itself (up to six). Fitted so the docker
#: files sum to the one whole run on record - 6,835 s serial for 2,284
#: tests (2026-09-27), ~12,700 s for the 1,874 docker tests of 4,440 now;
#: `times.json` replaces it file by file after a run
ESTIMATE = {"file": 5, "test": 3, "container": 12}
TIMES = ROOT / "reports" / "final-suite" / "times.json"
#: the x86 engines the x86 workflow runs, one job each
X86 = ("mssql", "db2", "ase")


def rel(f):
    """A test file as pytest names it, from the checkout's root."""
    try:
        return str(Path(f).resolve().relative_to(ROOT))
    except ValueError:
        return str(f)


def child_env(**extra):
    """The environment of a pytest this script starts: its own, less
    what would make the child think it is an xdist worker or inside a
    test (`tests/servers.py:lane`, the conftest's record)."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("PYTEST_XDIST") and k != "PYTEST_CURRENT_TEST"}
    env.update(extra)
    return env


def file_of(test_id):
    """The file of a pytest node id, which names it from the root."""
    return (ROOT / test_id.split("::")[0]).resolve()


def log(msg):
    print(f"[final-suite {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def docker(ctx, *argv, timeout=120):
    env = dict(os.environ)
    if ctx:
        env["DOCKER_CONTEXT"] = ctx
    else:
        env.pop("DOCKER_CONTEXT", None)
    try:
        return subprocess.run(["docker", *argv], capture_output=True,
                              text=True, timeout=timeout, env=env)
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(e.cmd, 124, "", "timed out")
    except OSError as e:
        return subprocess.CompletedProcess(argv, 127, "", str(e))


# ---- 1. preflight -----------------------------------------------------------

def one_tree():
    """The checkout every process of the run imports migkit from: this
    one. Children get it first on PYTHONPATH; a module this checkout lacks
    would still come from the tree an editable install points at, so a
    gap between the two stops the run before it mixes them."""
    import importlib.metadata as md
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT)] + [p for p in os.environ.get("PYTHONPATH", "").split(
            os.pathsep) if p and p != str(ROOT)])
    try:
        url = json.loads(md.distribution("migkit").read_text(
            "direct_url.json") or "{}").get("url", "")
    except md.PackageNotFoundError:
        url = ""
    installed = Path(url[len("file://"):]).resolve() \
        if url.startswith("file://") else None
    if installed and installed != ROOT and (installed / "migkit").is_dir():
        def modules(top):
            return {str(p.relative_to(top)) for p in top.rglob("*.py")}
        gap = sorted(modules(installed / "migkit") - modules(ROOT / "migkit"))
        if gap:
            raise SystemExit(
                f"migkit is installed from {installed}, which has modules"
                f" this checkout ({ROOT}) lacks: {', '.join(gap[:10])} - a"
                " run here would import those from there. Merge, or run"
                " from that checkout.")
    got = subprocess.run([PY, "-c", "import migkit; print(migkit.__file__)"],
                         capture_output=True, text=True, cwd="/")
    where = Path(got.stdout.strip()).resolve()
    if ROOT not in where.parents:
        raise SystemExit(f"a child process imports migkit from {where},"
                         f" not from {ROOT}")
    return {"root": str(ROOT), "installed": str(installed or "")}


def preflight():
    got = {"python": sys.version.split()[0], "tree": one_tree()}
    for mod in ("pytest", "xdist", "pytest_timeout"):
        try:
            m = __import__(mod)
            got[mod] = getattr(m, "__version__", "present")
        except ImportError:
            raise SystemExit(f"{mod} is missing: pip install -e '.[dev]'")
    try:
        from migkit import tools
        got["missing_programs"] = sorted(
            c for c in tools.PROGRAMS if not tools._present(c))
        got["capabilities"] = [
            {"name": n, "state": s, "missing": m}
            for n, _, s, m in tools.capabilities() if s != "ready"]
    except Exception as e:  # noqa: BLE001 - recorded, not fatal
        got["install_state_error"] = f"{type(e).__name__}: {e}"
    return got


def daemon_room(ctx):
    """(free MB, CPUs, architecture) of a daemon: its memory less what
    containers other than the tests' hold and `RESERVE_MB`, or None where
    it does not answer."""
    info = docker(ctx, "info", "--format",
                  "{{.MemTotal}} {{.NCPU}} {{.Architecture}}", timeout=30)
    if info.returncode != 0:
        return None
    total, cpus, arch = info.stdout.split()
    stats = docker(ctx, "stats", "--no-stream", "--format",
                   "{{.Name}}\t{{.MemUsage}}", timeout=60)
    used = 0.0
    for line in stats.stdout.splitlines():
        name, _, usage = line.partition("\t")
        if name.startswith("migkit-"):
            continue
        m = re.match(r"([\d.]+)\s*([KMG]i?B)", usage)
        if m:
            used += float(m.group(1)) * {"K": 1 / 1024, "M": 1, "G": 1024}[
                m.group(2)[0]]
    return int(total) / 2 ** 20 - used - RESERVE_MB, int(cpus), arch


# ---- 2. collect -------------------------------------------------------------

def collect(report, mark):
    out = report / "collected.json"
    argv = [PY, "-m", "pytest", "--collect-only", "-q", "-p",
            *PYTEST, "--continue-on-collection-errors", "tests"]
    if mark:
        argv += ["-m", mark]
    argv += EXTRA
    env = child_env(MIGKIT_TEST_COLLECT_OUT=str(out))
    got = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True,
                         env=env)
    (report / "collect.log").write_text(got.stdout + got.stderr)
    errors = re.findall(r"^ERROR (\S+)", got.stdout, re.M)
    if not out.exists():
        raise SystemExit(f"collection wrote nothing:\n{got.stdout[-3000:]}"
                         f"{got.stderr[-3000:]}")
    return json.loads(out.read_text()), errors


# ---- 4. lanes ---------------------------------------------------------------

class Lane:
    def __init__(self, key, ctx, kind):
        self.key, self.ctx, self.kind = key, ctx, kind
        self.number = None
        self.files, self.load = [], 0.0
        self.ran = []           # (segment, returncode, seconds)
        self.hung = []          # (test id, seconds)
        self.missed = []        # files not run, with why

    def takes(self, heavy):
        return self.kind in ("any", "vm") or not heavy

    def __repr__(self):
        return f"lane {self.number} ({self.key}, {self.kind})"


def lanes_for(rooms, want_vm, only, override):
    """The lanes each daemon holds: the default profile first (its first
    lane may run heavy files), then the VM's lane for the VM files and
    light lanes beside it."""
    lanes = []
    home = lock.home_context()
    counts = dict(re.findall(r"(\w+)=(\d+)", override or ""))
    if "docker" in only:
        room = rooms.get("home")
        if room:
            free, cpus, _ = room
            n = int(counts.get("colima", counts.get("home", 0))) or max(
                1, min(cpus, 1 + int((free - ANY_LANE_MB) // LIGHT_LANE_MB)))
            lanes += [Lane("home", home, "any" if i == 0 else "light")
                      for i in range(n)]
    if want_vm and "vm" in only:
        vm_ctx = "colima-migkit" if lock._has_colima() else home
        lanes.append(Lane("vm", vm_ctx, "vm"))
        if "docker" in only and vm_ctx != home:
            n = int(counts.get("migkit", -1))
            if n < 0:
                n = max(0, min(VM_CPUS - 1, (VM_MB - RESERVE_MB - VM_LANE_MB)
                               // LIGHT_LANE_MB))
            lanes += [Lane("vm", vm_ctx, "light") for _ in range(n)]
    return lanes


def plan(items, only, rooms, override):
    """Files into lanes: conflicting files together, pinned ones on lane
    0, VM files in the VM lane, the heaviest first onto the least
    loaded lane that may take them."""
    by_file = collections.defaultdict(list)
    for it in items:
        if "docker" in it["marks"]:
            by_file[it["file"]].append(it)
    vm_files = {f for f, its in by_file.items()
                if any("vm" in i["marks"] for i in its)}
    times = json.loads(TIMES.read_text()) if TIMES.exists() else {}
    holds = {f: servers.scan(f) for f in by_file}

    def estimate(f):
        if rel(f) in times:
            return times[rel(f)]
        return ESTIMATE["file"] + ESTIMATE["test"] * len(by_file[f]) + \
            ESTIMATE["container"] * min(holds[f].runs, 6)

    parent = {f: f for f in by_file}

    def root(f):
        while parent[f] != f:
            parent[f] = parent[parent[f]]
            f = parent[f]
        return f
    owner = {}
    for f, h in holds.items():
        for k in h.keys():
            if k in owner:
                parent[root(f)] = root(owner[k])
            else:
                owner[k] = f
    groups = collections.defaultdict(list)
    for f in by_file:
        groups[root(f)].append(f)
    lanes = lanes_for(rooms, bool(vm_files), only, override)
    missed = []
    comps = []
    for fs in groups.values():
        comps.append({"files": sorted(fs),
                      "vm": any(f in vm_files for f in fs),
                      "pinned": any(holds[f].pinned for f in fs),
                      "heavy": max(holds[f].mb for f in fs) > HEAVY_MB,
                      "seconds": sum(estimate(f) for f in fs)})
    comps.sort(key=lambda c: (not c["pinned"], -c["seconds"]))
    for c in comps:
        if ("vm" if c["vm"] else "docker") not in only:
            continue
        fit = [ln for ln in lanes if (ln.kind == "vm" if c["vm"] else
                                      ln.takes(c["heavy"]))]
        if not fit:
            missed.append(dict(files=c["files"], why="no lane may run it:"
                               " no docker answered" if not lanes else
                               "no lane may run it"))
            continue
        if c["pinned"]:
            fit = [next((ln for ln in fit if ln.number == 0), fit[0])]
            fit[0].number = 0
        best = min(fit, key=lambda ln: ln.load)
        best.files += c["files"]
        best.load += c["seconds"]
    free = [n for n in range(servers.LANES)
            if n not in {ln.number for ln in lanes}]
    for ln in lanes:
        if ln.number is None:
            ln.number = free.pop(0)
        # files on one profile together, so the shared servers stay warm
        ln.files.sort(key=lambda f: (sorted(holds[f].profiles),
                                     Path(f).name))
    for c in comps:
        c.pop("files", None)
    return [ln for ln in lanes if ln.files], missed


# ---- 3. the pure tests and the images ---------------------------------------

def unit_phase(report, mark):
    try:
        import psutil
        avail = psutil.virtual_memory().available / 2 ** 20
    except Exception:  # noqa: BLE001
        avail = 4000
    workers = max(1, min(os.cpu_count() or 2, int(avail // 500)))
    expr = "not docker" + (f" and ({mark})" if mark else "")
    argv = [PY, "-m", "pytest", "tests", "-m", expr, "-n", str(workers),
            "-p", *PYTEST, "-q", "-rs", "--timeout", str(TIMEOUT),
            "--junitxml", str(report / "unit.xml"), *EXTRA]
    log(f"pure tests, {workers} at once")
    began = time.monotonic()
    with open(report / "unit.log", "w") as out:
        code = subprocess.call(argv, cwd=ROOT, stdout=out,
                               stderr=subprocess.STDOUT,
                               env=child_env(MIGKIT_TEST_STRICT=STRICT))
    return {"returncode": code, "seconds": round(time.monotonic() - began),
            "workers": workers}


def images_of(files):
    got = set()
    for f in files:
        got |= servers.scan(f).images
    return sorted(got)


def pull(ctx, images):
    """Every image once, four at a time, and the digest each ran as."""
    def one(img):
        got = docker(ctx, "pull", "-q", img, timeout=1800)
        digest = docker(ctx, "image", "inspect", "--format",
                        "{{index .RepoDigests 0}}", img).stdout.strip()
        return img, {"pulled": got.returncode == 0,
                     "digest": digest or None,
                     "error": None if got.returncode == 0 else
                     got.stderr.strip()[-300:]}
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        return dict(pool.map(one, images))


# ---- 5. running a lane ------------------------------------------------------

def progress(path):
    rows = []
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows


def run_lane(lane, report, mark, run_id, stop):
    """The lane's files in one pytest; if it hangs, end it, clear its
    containers and go on with the files it had not finished."""
    todo = list(lane.files)
    segment = 0
    expr = "docker" + (f" and ({mark})" if mark else "")
    while todo and not stop.is_set():
        segment += 1
        was = list(todo)
        prog = report / f"lane-{lane.number}.jsonl"
        args = report / f"lane-{lane.number}-{segment}.args"
        args.write_text("\n".join(rel(f)
                                  for f in todo) + "\n")
        argv = [PY, "-m", "pytest", f"@{args}", "-m", expr, "-p",
                *PYTEST, "-q", "-rs", "--timeout", str(TIMEOUT),
                "--junitxml",
                str(report / f"lane-{lane.number}-{segment}.xml"),
                *EXTRA]
        env = child_env(MIGKIT_TEST_LANE=str(lane.number),
                   MIGKIT_TEST_RUN=run_id, MIGKIT_TEST_STRICT=STRICT,
                   MIGKIT_TEST_PROGRESS=str(prog))
        if lane.ctx:
            env["DOCKER_CONTEXT"] = lane.ctx
        else:
            env.pop("DOCKER_CONTEXT", None)
        began = time.monotonic()
        seen = len(progress(prog))
        with open(report / f"lane-{lane.number}.log", "a") as out:
            proc = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=out,
                                    stderr=subprocess.STDOUT,
                                    start_new_session=True)
            hung = None
            while proc.poll() is None:
                time.sleep(5)
                rows = progress(prog)[seen:]
                started = [r for r in rows if r["ev"] == "start"]
                quiet = time.time() - rows[-1]["t"] if rows \
                    else time.monotonic() - began
                limit = (started[-1].get("timeout") if started else None) \
                    or TIMEOUT
                if quiet > limit + GRACE or stop.is_set():
                    hung = started[-1]["id"] if started else "(collection)"
                    _end(proc)
                    break
        lane.ran.append((segment, proc.returncode,
                         round(time.monotonic() - began)))
        rows = progress(prog)[seen:]
        done = {file_of(r["id"]) for r in rows
                if r["ev"] == "end" and r.get("last")}
        todo = [f for f in todo if Path(f).resolve() not in done]
        if hung is None:
            if todo and proc.returncode not in (0, 1, 5):
                lane.missed += [dict(file=f, why=f"pytest ended with"
                                     f" {proc.returncode} before it")
                                for f in todo]
            break
        log(f"{lane}: {hung} stopped answering; ended, going on")
        lane.hung.append((hung, round(time.monotonic() - began)))
        sweep(lane.ctx, None, lane=lane.number)
        todo = [f for f in todo if Path(f).resolve() != file_of(hung)]
        if todo == was:
            # stopped before any test began: nothing to step past
            lane.missed += [dict(file=f, why="the lane stopped answering"
                                 " before its first test") for f in todo]
            break
    return lane


def _end(proc):
    """SIGTERM first (the conftest turns it into Ctrl-C, so fixtures tear
    down), SIGKILL a minute later."""
    for sig, wait in ((signal.SIGTERM, 60), (signal.SIGKILL, 10)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(wait)
            return
        except subprocess.TimeoutExpired:
            continue


# ---- 7. never leave anything behind -----------------------------------------

def inventory(ctx):
    got = {}
    for kind, argv in (("container", ["ps", "-a", "--format", "{{.Names}}"]),
                       ("network", ["network", "ls", "--format",
                                    "{{.Name}}"]),
                       ("volume", ["volume", "ls", "--format", "{{.Name}}"])):
        got[kind] = set(docker(ctx, *argv).stdout.split())
    return got


def sweep(ctx, before, lane=None, run_id=None):
    """Remove what the run made on a daemon: containers of a lane or of
    the run by label, and anything named `migkit-*` that was not there
    before the run began. Never another session's (`k3d-*` and anything
    older than the run)."""
    gone = []
    labels = []
    if lane is not None:
        labels.append(f"{servers.LABEL}.lane={lane * servers.WORKERS}")
    if run_id:
        labels.append(f"{servers.LABEL}.run={run_id}")
    for label in labels:
        ids = docker(ctx, "ps", "-aq", "--filter", f"label={label}")
        if ids.stdout.split():
            docker(ctx, "rm", "-f", "-v", *ids.stdout.split())
            gone += ids.stdout.split()
    if before is not None:
        now = inventory(ctx)
        for kind, verb in (("container", ["rm", "-f", "-v"]),
                           ("network", ["network", "rm"]),
                           ("volume", ["volume", "rm", "-f"])):
            new = sorted(n for n in now[kind] - before[kind]
                         if n.startswith("migkit-"))
            if new:
                docker(ctx, *verb, *new)
                gone += new
    return gone


# ---- 6. and 7. the record ---------------------------------------------------

def junit_rows(path):
    rows = []
    try:
        tree = ET.parse(path)
    except (OSError, ET.ParseError):
        return rows
    for case in tree.iter("testcase"):
        cls = case.get("classname", "")
        parts = cls.split(".")
        file = "/".join(parts[:2]) + ".py" if len(parts) >= 2 else cls
        outcome, message = "passed", ""
        for tag in ("failure", "error", "skipped"):
            el = case.find(tag)
            if el is not None:
                outcome = {"failure": "failed", "error": "failed",
                           "skipped": "skipped"}[tag]
                message = (el.get("message") or el.text or "")[:500]
                break
        rows.append({"id": f"{file}::{case.get('name')}", "file": file,
                     "outcome": outcome, "message": message,
                     "seconds": float(case.get("time") or 0)})
    return rows


def lane_rows(path):
    """Per test from a lane's progress: its outcome over every phase, its
    time, its message; per module the time from its first start to its
    last end."""
    tests, spans = {}, collections.defaultdict(list)
    for r in progress(path):
        tid = r.get("id", "")
        file = tid.split("::")[0]
        if r["ev"] in ("start", "end"):
            spans[file].append(r["t"])
        if r["ev"] == "report":
            t = tests.setdefault(tid, {"id": tid, "file": file,
                                       "outcome": "passed", "message": "",
                                       "seconds": 0.0})
            t["seconds"] += r.get("duration", 0)
            if r["outcome"] == "failed":
                t["outcome"], t["message"] = "failed", r.get("message", "")
            elif r["outcome"] == "skipped" and t["outcome"] == "passed":
                t["outcome"], t["message"] = "skipped", r.get("reason", "")
        if r["ev"] == "collect-error":
            tests[tid] = {"id": tid, "file": file, "outcome": "failed",
                          "message": r.get("message", ""), "seconds": 0.0}
    module_s = {f: round(max(ts) - min(ts), 1) for f, ts in spans.items()}
    return list(tests.values()), module_s


def rerun(ids, report, name, env_extra, ctx):
    if not ids:
        return {}
    args = report / f"rerun-{name}.args"
    args.write_text("\n".join(ids) + "\n")
    xml = report / f"rerun-{name}.xml"
    env = child_env(MIGKIT_TEST_STRICT=STRICT, **env_extra)
    if ctx:
        env["DOCKER_CONTEXT"] = ctx
    with open(report / f"rerun-{name}.log", "w") as out:
        subprocess.call([PY, "-m", "pytest", f"@{args}", "-p",
                         *PYTEST, "-q", "-rs", "--timeout",
                         str(TIMEOUT), "--junitxml", str(xml)], cwd=ROOT,
                        env=env, stdout=out, stderr=subprocess.STDOUT)
    return {r["id"]: r["outcome"] for r in junit_rows(xml)}


def write_report(report, record):
    (report / "report.json").write_text(json.dumps(record, indent=1,
                                                   default=str))
    tests = record["tests"]
    counts = collections.Counter(t["outcome"] for t in tests)
    lines = [f"# Final suite {record['run']}", "",
             f"Commit {record['commit']}; {record['seconds']} s"
             f" ({record['seconds'] / 60:.0f} min) in all.", "",
             "| outcome | tests |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    lines += ["", "## Phases and lanes", ""]
    if record.get("unit"):
        u = record["unit"]
        lines.append(f"* pure tests: {u['seconds']} s with {u['workers']}"
                     f" workers (exit {u['returncode']})")
    for ln in record.get("lanes", []):
        lines.append(f"* lane {ln['number']} ({ln['daemon']}, {ln['kind']}):"
                     f" {len(ln['files'])} files, {ln['seconds']} s run,"
                     f" {ln['estimate']} s estimated"
                     + (f"; hung: {ln['hung']}" if ln["hung"] else ""))
    for m in record.get("missed", []):
        lines.append(f"* not run: {m}")
    failed = [t for t in tests if t["outcome"] == "failed"]
    flaky = [t for t in tests if t["outcome"] == "flaky"]
    for title, rows in (("Failed (twice)", failed), ("Flaky (passed the"
                                                      " second time)", flaky)):
        if rows:
            lines += ["", f"## {title}", ""]
            by = collections.defaultdict(list)
            for t in rows:
                by[t["file"]].append(t)
            for f, ts in sorted(by.items()):
                lines.append(f"* {f}")
                for t in ts:
                    first = (t["message"] or "").strip().splitlines()[-1:] \
                        or [""]
                    lines.append(f"  * {t['id'].split('::', 1)[-1]}:"
                                 f" {first[0][:200]}")
    skips = collections.Counter(t["message"][:160] for t in tests
                                if t["outcome"] == "skipped")
    if skips:
        lines += ["", "## Skips by reason", ""]
        lines += [f"* {n} x {r}" for r, n in skips.most_common()]
    slow = sorted(record["modules"].items(), key=lambda kv: -kv[1])[:30]
    lines += ["", "## Slowest modules", ""]
    lines += [f"* {s:.0f} s {f}" for f, s in slow]
    pre = record.get("preflight", {})
    if pre.get("missing_programs"):
        lines += ["", "## Not installed here", "",
                  ", ".join(pre["missing_programs"])]
    if record.get("images"):
        lines += ["", "## Images", ""]
        lines += [f"* {img}: {v.get('digest') or v.get('error')}"
                  for img, v in sorted(record["images"].items())]
    if record.get("left"):
        lines += ["", "## Removed after the run", "",
                  ", ".join(record["left"])]
    (report / "report.md").write_text("\n".join(lines) + "\n")


class _VMUp:
    """The migkit VM for as long as its lanes run, where they are on it;
    yields why it could not start, or None."""

    def __init__(self, lanes):
        self.need = any(ln.ctx and ln.ctx.startswith("colima-")
                        for ln in lanes)
        self.cm = None

    def __enter__(self):
        if not self.need:
            return None
        self.cm = lock.vm_profile("migkit")
        try:
            self.cm.__enter__()
        except (SystemExit, subprocess.CalledProcessError) as e:
            self.cm = None
            return f"the migkit VM did not start: {e}"
        return None

    def __exit__(self, *exc):
        if self.cm:
            self.cm.__exit__(*exc)
        return False


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", default="unit,docker,vm",
                    help="phases: unit, docker, vm (comma separated)")
    ap.add_argument("-m", dest="mark", default="",
                    help="a pytest mark expression every phase adds")
    ap.add_argument("--lanes", default=os.environ.get("MIGKIT_TEST_LANES"),
                    help="lanes per daemon, e.g. colima=1,migkit=2")
    ap.add_argument("--plan", action="store_true",
                    help="lay the lanes out and print them; run nothing")
    ap.add_argument("-k", dest="k", default="",
                    help="a pytest -k expression every phase adds")
    ap.add_argument("--no-rerun", action="store_true")
    ap.add_argument("--no-pull", action="store_true",
                    help="run on the images already here")
    ap.add_argument("--strict", default="1",
                    help="MIGKIT_TEST_STRICT: 1 (every missing server,"
                         " program, module fails) or e.g. `server`")
    a = ap.parse_args(argv)
    global STRICT
    STRICT = a.strict
    EXTRA[:] = ["-k", a.k] if a.k else []
    only = set(a.only.split(","))
    began = time.monotonic()
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ")
    report = ROOT / "reports" / "final-suite" / run_id
    report.mkdir(parents=True, exist_ok=True)
    record = {"run": run_id, "only": sorted(only), "mark": a.mark,
              "commit": subprocess.run(["git", "rev-parse", "--short",
                                        "HEAD"], cwd=ROOT, capture_output=True,
                                       text=True).stdout.strip(),
              "tests": [], "modules": {}, "lanes": [], "missed": []}
    record["preflight"] = pre = preflight()
    log(f"report in {report}")
    if pre.get("missing_programs"):
        log(f"not installed here: {', '.join(pre['missing_programs'])}")
    items, errors = collect(report, a.mark)
    record["collection_errors"] = errors
    log(f"{len(items)} tests collected, {len(errors)} files failed to"
        " collect")
    rooms = {}
    if only & {"docker", "vm"}:
        rooms["home"] = daemon_room(lock.home_context())
    lanes, missed = plan(items, only, rooms, a.lanes) \
        if only & {"docker", "vm"} else ([], [])
    record["missed"] += missed
    for ln in lanes:
        log(f"{ln}: {len(ln.files)} files, ~{ln.load / 60:.0f} min")
    if a.plan:
        for ln in lanes:
            print(f"\n{ln} ~{ln.load:.0f} s")
            for f in ln.files:
                print(f"  {rel(f)}")
        print(f"\nmissed: {missed}")
        return 0

    pulls = {}
    home_files = [f for ln in lanes if ln.key == "home" for f in ln.files]
    puller = None
    if home_files and not a.no_pull:
        pool = concurrent.futures.ThreadPoolExecutor(1)
        puller = pool.submit(pull, lock.home_context(), images_of(home_files))
    unit_rows = []
    if "unit" in only:
        record["unit"] = unit_phase(report, a.mark)
        unit_rows = junit_rows(report / "unit.xml")
        log(f"pure tests done in {record['unit']['seconds']} s")
    if puller:
        pulls.update(puller.result())

    lane_tests, stop = [], threading.Event()
    if lanes:
        with lock.exclusive():
            contexts = {ln.ctx for ln in lanes}
            with _VMUp(lanes) as vm_error:
                if vm_error:
                    for ln in [x for x in lanes if x.key == "vm"]:
                        record["missed"] += [dict(file=f, why=vm_error)
                                             for f in ln.files]
                    lanes = [x for x in lanes if x.key != "vm"]
                before = {c: inventory(c) for c in contexts}
                on_vm = [ln for ln in lanes if ln.key == "vm"]
                if on_vm and not a.no_pull:
                    pulls.update(pull(on_vm[0].ctx, images_of(
                        [f for ln in on_vm for f in ln.files])))
                try:
                    with concurrent.futures.ThreadPoolExecutor(
                            len(lanes)) as pool:
                        list(pool.map(lambda ln: run_lane(
                            ln, report, a.mark, run_id, stop), lanes))
                except KeyboardInterrupt:
                    stop.set()
                    raise
                finally:
                    left = []
                    for c in contexts:
                        left += sweep(c, before[c], run_id=run_id)
                    record["left"] = left
                for ln in lanes:
                    rows, spans = lane_rows(report
                                            / f"lane-{ln.number}.jsonl")
                    for r in rows:
                        r["lane"] = ln.number
                    lane_tests += rows
                    record["modules"].update(spans)
                    record["missed"] += ln.missed
                    record["lanes"].append({
                        "number": ln.number, "daemon": ln.ctx or "local",
                        "kind": ln.kind,
                        "files": [rel(f) for f in ln.files],
                        "estimate": round(ln.load),
                        "seconds": sum(s for _, _, s in ln.ran),
                        "hung": ln.hung})
                if not a.no_rerun:
                    by_lane = {ln.number: ln for ln in lanes}
                    for kind in ("home", "vm"):
                        ids = [t["id"] for t in lane_tests
                               if t["outcome"] == "failed" and
                               by_lane[t["lane"]].key == kind]
                        ctx = next((ln.ctx for ln in lanes
                                    if ln.key == kind), None)
                        before2 = inventory(ctx) if ids else None
                        again = rerun(ids, report, kind, {
                            "MIGKIT_TEST_LANE": "0",
                            "MIGKIT_TEST_RUN": run_id}, ctx)
                        if ids:
                            record.setdefault("left", []).extend(
                                sweep(ctx, before2, run_id=run_id))
                        for t in lane_tests:
                            if again.get(t["id"]) == "passed":
                                t["outcome"] = "flaky"
    if unit_rows and not a.no_rerun:
        ids = [t["id"] for t in unit_rows if t["outcome"] == "failed"]
        again = rerun(ids, report, "unit", {}, None)
        for t in unit_rows:
            if again.get(t["id"]) == "passed":
                t["outcome"] = "flaky"
    for t in unit_rows:
        record["modules"][t["file"]] = round(
            record["modules"].get(t["file"], 0) + t["seconds"], 1)
    record["tests"] = unit_rows + lane_tests
    record["images"] = pulls
    record["seconds"] = round(time.monotonic() - began)
    write_report(report, record)
    if only == {"unit", "docker", "vm"} and not a.mark:
        TIMES.parent.mkdir(parents=True, exist_ok=True)
        old = json.loads(TIMES.read_text()) if TIMES.exists() else {}
        old.update({f: s for f, s in record["modules"].items()
                    if f.startswith("tests/")})
        TIMES.write_text(json.dumps(old, indent=1, sort_keys=True))
    failed = [t for t in record["tests"] if t["outcome"] == "failed"]
    log(f"done in {record['seconds']} s: {len(failed)} failed,"
        f" {sum(t['outcome'] == 'flaky' for t in record['tests'])} flaky;"
        f" {report / 'report.md'}")
    return 1 if failed or errors or record["missed"] else 0


if __name__ == "__main__":
    sys.exit(main())
