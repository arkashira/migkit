"""A bundle to report a problem with, holding nothing to hand over.

`MIGKIT_DIAGNOSE=<file>.zip migkit doctor` writes it (an environment
variable, so `doctor` keeps its one flag). In it:

* `about.json`: migkit's version, Python, the platform, and what each
  engine can do here (`capabilities.matrix`)
* `hops.yaml`: the configuration, with every password, token, key and
  notification address replaced, and credentials taken out of any
  address that carries them
* for each hop: its last verdict with every finding's detail removed -
  the detail is where keys and values are - and its change log

What is removed is removed before anything is written: a secret that
never reaches the file cannot be left in it.
"""
import json
import platform
import time
import zipfile

HIDDEN = "<removed>"


def scrub(value, key=""):
    """The configuration with its secrets taken out, at any depth: the
    one redactor's (`redact.scrub`), which also finds a password holding
    `/` or `@` in an address - `mysql://app:pa/ss@db` was left whole and
    `postgres://u:p@ss@h` came out `<removed>@ss@h`."""
    from . import redact
    return redact.scrub(value, HIDDEN, key)


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


def write(target, conf, reports):
    """Writes the bundle to `target`; returns the names it holds."""
    import yaml

    from . import __version__, capabilities
    names = []
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("about.json", json.dumps({
            "migkit": __version__, "python": platform.python_version(),
            "platform": platform.platform(),
            "written": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "capabilities": capabilities.matrix()}, indent=1))
        names.append("about.json")
        try:
            raw = yaml.safe_load(conf.read_text()) or {}
        except (OSError, yaml.YAMLError):
            raw = None
        if raw is not None:
            z.writestr("hops.yaml", yaml.safe_dump(scrub(raw),
                                                    sort_keys=False))
            names.append("hops.yaml")
        hops = (raw or {}).get("hops") or {}
        for hop in sorted(hops):
            where = reports / hop
            got = _verdict(where / "verdict.json")
            if got is not None:
                z.writestr(f"{hop}/verdict.json", json.dumps(got, indent=1))
                names.append(f"{hop}/verdict.json")
            log = where / "changelog.jsonl"
            if log.exists():
                lines = []
                for line in log.read_text().splitlines()[-200:]:
                    try:
                        entry = scrub(json.loads(line))
                    except ValueError:
                        continue
                    # who ran it, and where, is not the problem's
                    for k in ("who", "host", "prev", "hash"):
                        entry.pop(k, None)
                    lines.append(json.dumps(entry))
                z.writestr(f"{hop}/changelog.jsonl", "\n".join(lines) + "\n")
                names.append(f"{hop}/changelog.jsonl")
    return names
