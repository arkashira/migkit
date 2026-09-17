"""What migkit can do on this machine, and how to make it able to do more.

Capabilities are the unit the operator sees. Which program provides one is an
implementation detail: migkit drives external programs rather than bundling
them (so copyleft tools like pt-table-sync stay at arm's length), but nobody
using migkit should have to know that to read `doctor`.

`doctor --install` brings a fresh machine up to speed.
"""
import platform
import shutil
import subprocess

from .util import which

# (command, brew formula, apt package). apt package "" means not in the
# default apt repos (needs a vendor step) - reported, not attempted.
PROGRAMS = {
    "psql": ("libpq", "postgresql-client"),
    "pg_dump": ("libpq", "postgresql-client"),
    "pg_restore": ("libpq", "postgresql-client"),
    "mysql": ("mysql-client", "default-mysql-client"),
    "mysqldump": ("mysql-client", "default-mysql-client"),
    "mongodump": ("mongodb-database-tools", ""),
    "mongorestore": ("mongodb-database-tools", ""),
    "mydumper": ("mydumper", "mydumper"),
    "myloader": ("mydumper", "mydumper"),
    "pgloader": ("pgloader", "pgloader"),
    "pt-table-sync": ("percona-toolkit", "percona-toolkit"),
    "atlas": ("ariga/tap/atlas", ""),
    "liquibase": ("liquibase", ""),
    "sqlcmd": ("sqlcmd", ""),
    "docker": ("", ""),
}

# name, what it lets the operator do, programs required, optional faster path.
# "needs" empty means the capability ships with migkit itself.
CAPABILITIES = [
    ("PostgreSQL: verify and repair",
     "compare every row and object, repair with undo",
     ["psql"], []),
    ("PostgreSQL: bulk move",
     "parallel load between PostgreSQL databases",
     ["pg_dump", "pg_restore"], []),
    ("PostgreSQL: schema diff and DDL repair",
     "authoritative schema comparison, generated repair DDL",
     [], ["atlas", "liquibase"]),
    ("MySQL: verify and repair",
     "compare every row and object, repair with undo",
     ["mysql"], ["pt-table-sync"]),
    ("MySQL: bulk move",
     "parallel load between MySQL databases",
     ["mysqldump"], ["mydumper", "myloader"]),
    ("MongoDB: verify and repair",
     "compare documents and indexes",
     [], []),
    ("MongoDB: bulk move",
     "collection load between MongoDB deployments",
     ["mongodump", "mongorestore"], []),
    ("SQL Server: verify",
     "compare rows and objects",
     [], ["sqlcmd"]),
    ("Redis / Kafka: verify",
     "compare keyspaces and topic offsets",
     [], []),
    ("Cross-engine (MySQL to PostgreSQL)",
     "schema transpile, resumable copy, cross-dialect verify",
     [], ["pgloader"]),
    ("Change streaming",
     "follow live changes until cutover",
     [], ["docker"]),
    ("Any engine: row-level diff",
     "cross-dialect row comparison and drilldown",
     [], []),
]


def capabilities():
    """(name, what, state, missing) per capability.

    state: ready | reduced | unavailable
      ready        everything is here
      reduced      core works, an optional faster/deeper path is missing
      unavailable  a required program is missing
    """
    out = []
    for name, what, needs, optional in CAPABILITIES:
        missing_req = [c for c in needs if not which(c)]
        missing_opt = [c for c in optional if not which(c)]
        if missing_req:
            state = "unavailable"
        elif missing_opt and len(missing_opt) == len(optional):
            state = "reduced"
        else:
            state = "ready"
        out.append((name, what, state, missing_req + missing_opt))
    return out


def install_hint(programs):
    """One install line for the given programs, for this machine."""
    mac = platform.system() == "Darwin"
    mgr = "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)
    pkgs, manual = [], []
    for c in programs:
        formula, apt = PROGRAMS.get(c, ("", ""))
        pkg = formula if mgr == "brew" else apt
        if mgr and pkg:
            pkgs.append(pkg)
        else:
            manual.append(c)
    parts = []
    if pkgs:
        uniq = sorted(set(pkgs))
        parts.append("brew install " + " ".join(uniq) if mgr == "brew"
                     else "sudo apt-get install -y " + " ".join(uniq))
    if manual:
        parts.append("install manually: " + ", ".join(sorted(set(manual))))
    return "; ".join(parts)


def install_missing(log=print):
    """Install every missing program with the platform package manager.
    Returns the capabilities still short afterwards."""
    mac = platform.system() == "Darwin"
    mgr = "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)
    if not mgr:
        log("no supported package manager (brew/apt) here; install by hand")
    wanted = []
    for _, _, needs, optional in CAPABILITIES:
        wanted += needs + optional
    seen = set()
    for cmd in wanted:
        if which(cmd) or cmd in seen:
            continue
        formula, apt = PROGRAMS.get(cmd, ("", ""))
        pkg = formula if mgr == "brew" else apt
        if not mgr or not pkg:
            continue
        seen.add(pkg)
        log(f"installing {pkg} ...")
        cmd_line = (["brew", "install", pkg] if mgr == "brew"
                    else ["sudo", "apt-get", "install", "-y", pkg])
        p = subprocess.run(cmd_line, capture_output=True, text=True)
        if p.returncode != 0:
            tail = (p.stderr or "").strip().splitlines()[-1:]
            log(f"  failed: {tail[0] if tail else 'see output'}")
    return [(n, m) for n, _, st, m in capabilities() if st != "ready"]
