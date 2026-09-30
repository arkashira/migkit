"""A bundle to report a problem with, holding nothing to hand over.

`MIGKIT_DIAGNOSE=<file>.zip migkit doctor` writes it (an environment
variable, so `doctor` keeps its one flag). In it:

* `about.json`: migkit's version, Python, the platform, and what each
  engine can do here (`capabilities.matrix`)
* `hops.yaml`: the configuration, with every password, token, key and
  notification address replaced, and credentials taken out of any
  address that carries them
* for each hop: its last verdict with every finding's detail removed -
  the detail is where keys and values are - its change log, the ways the
  engine chose and why (`decisions.jsonl`), the logs of its last runs
  (`runs/`, `runlog`) and each database's `commands.log`, what was run

What is removed is removed before anything is written: a secret that
never reaches the file cannot be left in it. One cleaner does it for
every file (`redact`): by the keys that name a secret, by the shape a
secret has wherever it stands (an address's credentials, `password=`,
`IDENTIFIED BY`), and by value - every secret written out in the
configuration and every secret variable of the environment, in each form
it reaches text in. Measured before it had one: `mysql://app:pa/ss@db`
kept its password and `postgres://u:p@ss@h` kept `ss`; the run's own
record of commands, "what someone attaches to a bug report", was left
out.
"""
import json
import os
import platform
import time
import zipfile

HIDDEN = "<removed>"
#: the last lines of a hop's long files, and the last run logs, carried
LINES = 200
RUNS = 20


def scrub(value, key=""):
    """The configuration with its secrets taken out, at any depth: the
    one redactor's (`redact.scrub`), which also finds a password holding
    `/` or `@` in an address - `mysql://app:pa/ss@db` was left whole and
    `postgres://u:p@ss@h` came out `<removed>@ss@h`."""
    from . import redact
    return redact.scrub(value, HIDDEN, key)


def _written(raw):
    """Every secret written out in the configuration - the value of a key
    that names one, and not a reference to where it is kept - for the
    cleaner to find by value in the files carried."""
    from . import addresses
    from .redact import HOLDS_SECRET
    out = []

    def walk(value, key=""):
        if isinstance(value, dict) and key == "notify":
            # a receiver's address is its own password
            walk(value.get("to"), "notify")
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, str(k))
        elif isinstance(value, list):
            for v in value:
                walk(v, key)
        elif isinstance(value, str) and value and (
                HOLDS_SECRET.search(key) or key == "notify") \
                and not addresses.is_reference(value):
            out.append(value)
    walk(raw)
    return out


def _verdict(path):
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    for finding in got.get("findings") or []:
        # the detail names keys and shows values; the rest says what kind
        # of finding it is and where
        finding.pop("detail", None)
        finding.pop("fix_hint", None)
    return got


def _last(path, n=LINES):
    from . import runlog
    return runlog.last_lines(path, n)


def write(target, conf, reports):
    """Writes the bundle to `target`; returns the names it holds."""
    import yaml

    from . import __version__, capabilities, runlog
    from .redact import Redactor
    names = []
    try:
        raw = yaml.safe_load(conf.read_text()) or {}
    except (OSError, yaml.YAMLError):
        raw = None
    clean = Redactor(secrets=_written(raw), mask=HIDDEN, userinfo="all",
                     environ=os.environ)

    def put(name, text):
        z.writestr(name, clean(text))
        names.append(name)

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        put("about.json", json.dumps({
            "migkit": __version__, "python": platform.python_version(),
            "platform": platform.platform(),
            "written": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "capabilities": capabilities.matrix()}, indent=1))
        if raw is not None:
            put("hops.yaml", yaml.safe_dump(scrub(raw), sort_keys=False))
        hops = (raw or {}).get("hops") or {}
        for hop in sorted(hops):
            where = reports / hop
            got = _verdict(where / "verdict.json")
            if got is not None:
                put(f"{hop}/verdict.json", json.dumps(got, indent=1))
            for name in ("changelog.jsonl", "decisions.jsonl"):
                lines = []
                for line in _last(where / name):
                    try:
                        entry = scrub(json.loads(line))
                    except ValueError:
                        continue
                    # who ran it, and where, is not the problem's
                    for k in ("who", "host", "prev", "hash"):
                        entry.pop(k, None)
                    lines.append(json.dumps(entry))
                if lines:
                    put(f"{hop}/{name}", "\n".join(lines) + "\n")
            try:
                runs = sorted((where / runlog.DIR).glob("*.jsonl"),
                              key=lambda p: p.stat().st_mtime)[-RUNS:]
            except OSError:
                runs = []
            for run in runs:
                lines = []
                for line in _last(run, 10 * LINES):
                    try:
                        lines.append(json.dumps(scrub(json.loads(line))))
                    except ValueError:
                        continue
                put(f"{hop}/{runlog.DIR}/{run.name}", "\n".join(lines)
                    + "\n")
            try:
                dbs = sorted(d for d in where.iterdir() if d.is_dir())
            except OSError:
                dbs = []
            for d in dbs:
                log = d / "commands.log"
                if log.exists():
                    put(f"{hop}/{d.name}/commands.log",
                        "\n".join(_last(log)) + "\n")
    return names
