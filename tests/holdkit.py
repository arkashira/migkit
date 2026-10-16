"""What every `test_writes_are_held_on_<engine>` module proves the same way:
a hold taken by a process that is then killed outright (-9) is found by
`leftovers.holds`, lifted by `leftovers.lift` from its record, and the
write another session was refused is accepted again; `doctor` names the
rung the writes are held on. Written once here; each module brings only
its engine's own writes and refusals."""
import dataclasses
import json
import os
import signal
import subprocess
import sys

from migkit import leftovers
from migkit.config import Endpoint, Hop

CHILD = r"""
import json, sys, time
from migkit import freeze
from migkit.config import Endpoint, Hop
from migkit.engines import get_engine
spec = json.loads(sys.argv[1])
hop = Hop(name=spec["name"], engine=spec["engine"],
          source=Endpoint(**spec["source"]), target=Endpoint(**spec["target"]),
          databases=spec["databases"], options=spec["options"])
import pathlib
root = pathlib.Path(spec["reports"])
def report_dir(d=""):
    out = root / d if d else root
    out.mkdir(parents=True, exist_ok=True)
    return out
hop.report_dir = report_dir
eng = get_engine(hop)
freeze.freeze_source(hop, eng, spec["databases"], lambda m: None, spec["way"])
print("HELD", flush=True)
time.sleep(600)
"""


def hop_of(engine, src, dst, databases, tmp_path, **options):
    """A hop between two servers of the test lanes, its reports in
    `tmp_path`, its cutover on."""
    hop = Hop(name="held", engine=engine, source=src.endpoint(),
              target=dst.endpoint(), databases=list(databases),
              options={"cutover": True, **options})
    root = tmp_path / "reports"

    def report_dir(d=""):
        out = root / d if d else root
        out.mkdir(parents=True, exist_ok=True)
        return out
    hop.report_dir = report_dir
    hop.reports_at = str(root)
    return hop


def holder(hop, way):
    """A process of its own that holds the hop's source on `way` and waits;
    returned once it says it holds."""
    spec = {"name": hop.name, "engine": hop.engine,
            "source": dataclasses.asdict(hop.source),
            "target": dataclasses.asdict(hop.target),
            "databases": list(hop.databases), "options": dict(hop.options),
            "reports": hop.reports_at, "way": way}
    p = subprocess.Popen([sys.executable, "-c", CHILD, json.dumps(spec)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, env={**os.environ})
    line = p.stdout.readline()
    if line.strip() != "HELD":
        p.kill()
        raise AssertionError(f"the holder did not hold: {p.stderr.read()}")
    return p


def crash(proc):
    """Kill -9 the holder and wait until it is gone."""
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=30)


def lifted_after_a_crash(hop, eng, way, dbs, accepts, refuses):
    """The shared proof: a holder holds on `way`; `refuses()` shows a second
    session's write refused or waiting; the holder is killed; the hold is
    found left behind by a process that is gone, and lifted; `accepts()`
    shows the write taken again; nothing is left."""
    from migkit.cli import _rungs_said
    proc = holder(hop, way)
    try:
        refuses()
        left = leftovers.holds(hop, dbs)
        assert left and all(h["way"] == way for h in left), left
        assert all(h["gone"] is False for h in left), \
            "a hold of a running process is not a leftover"
        # doctor reads the ladder as it climbs it: the rung it names is
        # the one the decision engine takes for this source
        from migkit import freeze
        top = freeze.choose(hop, eng, dbs, side="src").rung.name
        said = _rungs_said(hop, eng, {"src"})
        assert any(f"its writes are held on {top} -" in line
                   for line in said), said
        crash(proc)
        left = leftovers.holds(hop, dbs)
        assert left and all(h["gone"] is True for h in left), left
        got, kept = leftovers.lift(hop, eng, lambda m: None, dbs)
        assert got and not kept, (got, kept)
        accepts()
        assert leftovers.holds(hop, dbs) == []
    finally:
        if proc.poll() is None:
            proc.kill()
