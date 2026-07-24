"""The external programs migkit drives, and how to install them. They are
always separate programs (never bundled), so GPL tools like pt-table-sync
stay at arm's length. `doctor --install` brings a fresh machine up to speed."""
import platform
import shutil
import subprocess

from .util import which

# (command, brew formula, apt package, what it powers). apt package "" means
# not in the default apt repos (needs a vendor step) — warned, not attempted.
TOOLS = [
    ("psql", "libpq", "postgresql-client", "postgres verify/repair"),
    ("pg_dump", "libpq", "postgresql-client", "postgres schema + move"),
    ("pg_restore", "libpq", "postgresql-client", "postgres parallel restore"),
    ("mysql", "mysql-client", "default-mysql-client", "mysql cli"),
    ("mysqldump", "mysql-client", "default-mysql-client", "mysql schema dump"),
    ("mongodump", "mongodb-database-tools", "", "mongo move"),
    ("mongorestore", "mongodb-database-tools", "", "mongo move"),
    ("mydumper", "mydumper", "mydumper", "parallel mysql move"),
    ("pgloader", "pgloader", "pgloader", "mysql->pg move"),
    ("pt-table-sync", "percona-toolkit", "percona-toolkit", "mysql row repair"),
    ("atlas", "ariga/tap/atlas", "", "authoritative schema diff + repair DDL"),
    ("liquibase", "liquibase", "", "schema diff (4th opinion)"),
    ("reladiff", "", "", "cross-engine data diff (pip, in .venv-tools)"),
    ("migra", "", "", "postgres schema diff (pip)"),
    ("sqlcmd", "sqlcmd", "", "mssql (needs microsoft tap/repo)"),
]


def status():
    return [(cmd, which(cmd), formula, apt, purpose)
            for cmd, formula, apt, purpose in TOOLS]


def install_missing(log=print):
    """Install every missing tool with the platform package manager. Returns
    the list still missing afterwards (e.g. pip-only or vendor-tap ones)."""
    mac = platform.system() == "Darwin"
    mgr = "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)
    if not mgr:
        log("no supported package manager (brew/apt) found; install by hand")
    seen, still = set(), []
    for cmd, formula, apt, purpose in TOOLS:
        if which(cmd):
            continue
        pkg = formula if mgr == "brew" else apt
        if not mgr or not pkg:
            still.append((cmd, purpose))
            log(f"MISSING {cmd} ({purpose}) — install manually")
            continue
        if pkg in seen:
            continue
        seen.add(pkg)
        log(f"installing {pkg} for {cmd} ...")
        cmd_line = (["brew", "install", pkg] if mgr == "brew"
                    else ["sudo", "apt-get", "install", "-y", pkg])
        p = subprocess.run(cmd_line, capture_output=True, text=True)
        if p.returncode != 0:
            still.append((cmd, purpose))
            log(f"  failed: {p.stderr.strip().splitlines()[-1:] or ''}")
    return still
