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
    ("SQLite: verify, repair and copy",
     "compare rows and schema, repair with undo, copy table by table",
     [], []),
    ("Redis / Kafka: verify",
     "compare keyspaces and topic offsets",
     [], []),
    ("Cross-engine (any pair)",
     "create missing tables, resumable copy, cross-dialect verify;"
     " MySQL to PostgreSQL also in one bulk pass",
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


def by_hand(programs):
    """The programs `doctor --install` cannot put in place on this machine.

    The only ones an operator is ever told the names of: with no package
    manager migkit can drive, or no package for a program, there is no way
    to get it but by hand, and a name is the one thing that helps.
    Everything else is `migkit doctor --install`'s business.
    """
    mac = platform.system() == "Darwin"
    mgr = "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)
    out = []
    for c in programs:
        formula, apt = PROGRAMS.get(c, ("", ""))
        if not (mgr and (formula if mgr == "brew" else apt)):
            out.append(c)
    return sorted(set(out))


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
    pkgs = []
    for cmd in wanted:
        if which(cmd):
            continue
        formula, apt = PROGRAMS.get(cmd, ("", ""))
        pkg = formula if mgr == "brew" else apt
        if mgr and pkg and pkg not in pkgs:
            pkgs.append(pkg)
    # counted, not named: which package provides a capability is migkit's
    # business, and the line used to read `installing mydumper ...`
    for n, pkg in enumerate(pkgs, 1):
        log(f"installing component {n} of {len(pkgs)} ...")
        cmd_line = (["brew", "install", pkg] if mgr == "brew"
                    else ["sudo", "apt-get", "install", "-y", pkg])
        p = subprocess.run(cmd_line, capture_output=True, text=True)
        if p.returncode != 0:
            log(f"  component {n} did not install; run migkit doctor again"
                " to see which capability is still short")
    return [(n, m) for n, _, st, m in capabilities() if st != "ready"]
