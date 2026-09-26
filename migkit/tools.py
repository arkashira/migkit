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
    # not in any package manager: fetched from its vendor (`install_vendor`)
    "mongosync": ("", ""),
    "mydumper": ("mydumper", "mydumper"),
    "myloader": ("mydumper", "mydumper"),
    "pgloader": ("pgloader", "pgloader"),
    "pt-table-sync": ("percona-toolkit", "percona-toolkit"),
    "atlas": ("ariga/tap/atlas", ""),
    "liquibase": ("liquibase", ""),
    "sqlcmd": ("sqlcmd", ""),
    "docker": ("", ""),
}

#: not a program on PATH: the second reader lives in an environment of its
#: own (`migkit.second_reader`), because it pins an older numeric stack
SECOND_READER = "second-reader"
#: the version measured to write nothing to either side
SECOND_READER_PACKAGE = "google-pso-data-validator==8.9.3"


def second_reader_present():
    from . import second_reader
    return second_reader.interpreter() is not None


def _present(cmd):
    if cmd == SECOND_READER:
        return second_reader_present()
    return which(cmd)


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
     ["mongodump", "mongorestore"], ["mongosync"]),
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
    ("Independent second reading",
     "read the data a second way, through different code, where one"
     " reading is not enough",
     [], [SECOND_READER]),
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
        missing_req = [c for c in needs if not _present(c)]
        missing_opt = [c for c in optional if not _present(c)]
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
        if c == SECOND_READER:
            continue    # `doctor --install` builds it wherever Python runs
        if c in VENDOR and vendor_file(c):
            continue    # fetched from its vendor (`install_vendor`)
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
        if cmd == SECOND_READER or which(cmd):
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
    for n, cmd in enumerate([c for c in dict.fromkeys(wanted)
                             if c in VENDOR and not which(c)], 1):
        log(f"fetching vendor component {n} ...")
        if not install_vendor(cmd, log):
            log(f"  vendor component {n} did not install; run migkit"
                " doctor again to see which capability is still short")
    if not _present(SECOND_READER):
        install_second_reader(log)
    return [(n, m) for n, _, st, m in capabilities() if st != "ready"]


#: programs no package manager carries, fetched from their vendor at the
#: build measured here: {program: version}
VENDOR = {"mongosync": "1.21.0"}
VENDOR_URL = "https://fastdl.mongodb.org/tools/mongosync/"


def vendor_file(program):
    """The vendor's file of `program` for this machine, or None where it
    publishes none - then it is one to install by hand."""
    if program != "mongosync":
        return None
    version = VENDOR[program]
    system, machine = platform.system(), platform.machine().lower()
    if system == "Darwin":
        arch = "arm-arm64" if machine in ("arm64", "aarch64") else "x86_64"
        return f"mongosync-macos-{arch}-{version}.zip"
    if system != "Linux" or machine not in ("x86_64", "amd64"):
        return None
    release = {}
    try:
        for line in open("/etc/os-release"):
            k, _, v = line.strip().partition("=")
            release[k] = v.strip('"')
    except OSError:
        return None
    ident, ver = release.get("ID", ""), release.get("VERSION_ID", "")
    # the builds the vendor publishes, measured by asking for each
    name = {("ubuntu", "20.04"): "ubuntu2004", ("ubuntu", "22.04"): "ubuntu2204",
            ("ubuntu", "24.04"): "ubuntu2404", ("amzn", "2023"): "amazon2023"
            }.get((ident, ver))
    if name is None and ident in ("rhel", "rocky", "almalinux") \
            and ver.startswith("9"):
        name = "rhel90"
    return f"mongosync-{name}-x86_64-{version}.tgz" if name else None


def install_vendor(program, log=print, into=None):
    """Fetch `program` from its vendor and put it beside migkit's own
    programs. True once `--version` of the installed copy says the build
    asked for."""
    import os
    import sysconfig
    import tarfile
    import tempfile
    import urllib.request
    import zipfile
    name = vendor_file(program)
    if not name:
        log("  its vendor publishes no build for this machine; install it"
            " by hand")
        return False
    into = into or sysconfig.get_path("scripts")
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, name)
        try:
            urllib.request.urlretrieve(VENDOR_URL + name, archive)
        except OSError as e:
            log(f"  could not fetch it ({type(e).__name__})")
            return False
        opener = (zipfile.ZipFile(archive) if name.endswith(".zip")
                  else tarfile.open(archive))
        with opener as box:
            if isinstance(box, tarfile.TarFile):
                box.extractall(work, filter="data")
            else:
                box.extractall(work)
        found = [os.path.join(root, program)
                 for root, _, files in os.walk(work)
                 if program in files and root.endswith("bin")]
        if not found:
            log("  the fetched archive holds no program by that name")
            return False
        target = os.path.join(into, program)
        shutil.copyfile(found[0], target)
        os.chmod(target, 0o755)
    got = subprocess.run([target, "--version"], capture_output=True,
                         text=True)
    return VENDOR[program] in (got.stdout + got.stderr)


def install_second_reader(log=print):
    """Build the second reader its own environment, beside migkit's.

    From this interpreter, so it is the Python migkit already runs on, and
    at the version measured to write nothing to either side. Said in
    migkit's words; the package is nobody's business but migkit's.
    """
    import sys
    from pathlib import Path
    home = Path.home() / ".migkit" / "second-reader"
    log("installing the independent second reading in its own environment"
        " (a large download, once) ...")
    made = subprocess.run([sys.executable, "-m", "venv", str(home)],
                          capture_output=True, text=True)
    if made.returncode == 0:
        made = subprocess.run([str(home / "bin" / "pip"), "install", "-q",
                               SECOND_READER_PACKAGE],
                              capture_output=True, text=True)
    if made.returncode != 0:
        log("  the second reading did not install; everything else still"
            " works, and migkit doctor --install tries again")
        return False
    return True
