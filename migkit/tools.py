"""What migkit can do on this machine, and how to make it able to do more.

Capabilities are the unit the operator sees. Which program provides one is an
implementation detail: migkit drives external programs rather than bundling
them, but nobody using migkit should have to know that to read `doctor`.

`doctor --install` brings a fresh machine up to speed. A program whose
licence is not open source (`TERMS`) is installed only once the operator
has accepted its terms - asked at the terminal, or named in
`MIGKIT_ACCEPT_TERMS` for an unattended install - and never silently; where
they are declined, the open build or the open path takes its place, and
that is said.
"""
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

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
    # fetched from its vendor, at the build its terms decide (`build_of`)
    "atlas": ("", ""),
    "liquibase": ("", ""),
    "riotx": ("", ""),
    "sqlcmd": ("sqlcmd", ""),
    "docker": ("", ""),
}

#: each program's licence, as SPDX names it. Every one is run as a program
#: of its own, never imported, so a copyleft licence is no bar to it; one
#: that is not open source at all is in `TERMS` and runs only on the
#: operator's acceptance (`tests/test_every_dependency_may_ship_with_
#: migkit.py` holds both)
PROGRAM_LICENCES = {
    "psql": "PostgreSQL", "pg_dump": "PostgreSQL", "pg_restore": "PostgreSQL",
    "mysql": "GPL-2.0-only", "mysqldump": "GPL-2.0-only",
    "mongodump": "Apache-2.0", "mongorestore": "Apache-2.0",
    "mongosync": "LicenseRef-MongoDB-Customer-Agreement",
    "mydumper": "GPL-3.0-or-later", "myloader": "GPL-3.0-or-later",
    "pgloader": "PostgreSQL", "pt-table-sync": "GPL-2.0-only",
    "atlas": "LicenseRef-Atlas-EULA", "liquibase": "FSL-1.1-ALv2",
    "riotx": "BUSL-1.1", "sqlcmd": "MIT", "docker": "Apache-2.0",
    # the image migkit runs its version-matched PostgreSQL copy from
    "pgcopydb": "PostgreSQL",
}

#: not a program on PATH: the second reader lives in an environment of its
#: own (`migkit.second_reader`), because it pins an older numeric stack
SECOND_READER = "second-reader"
#: the version measured to write nothing to either side
SECOND_READER_PACKAGE = "google-pso-data-validator==8.9.3"
SECOND_READER_LICENCE = "Apache-2.0"


@dataclass(frozen=True)
class Terms:
    """A program's licence where it is not open source, in the words the
    operator is asked to accept it in."""
    #: the licence, named
    licence: str
    #: where to read it
    url: str
    #: what migkit uses the program for, which its terms allow
    allows: str
    #: what is installed or runs instead where the terms are declined
    otherwise: str
    #: the open build installed instead, as `VENDOR` names versions; ""
    #: where there is none and the open path does the work
    open_build: str = ""


#: The programs whose licence is not open source. Each is installed and
#: used only once the operator has accepted its terms (`accept_terms`), for
#: what those terms allow; declined, the open build or the open path takes
#: its place and migkit says so.
TERMS = {
    "atlas": Terms(
        "the Atlas EULA (the default build)",
        "https://ariga.io/legal/atlas/eula",
        "a second, structural reading of each schema and the DDL that"
        " aligns a target's tables with the source's",
        "its Community build (Apache-2.0) is installed instead. Neither"
        " build compares views, functions, procedures or triggers without"
        " an Atlas sign-in (measured on 1.2.4 and on Community 1.3.0: the"
        " same table DDL, no view and no trigger) - migkit's own object"
        " check compares those either way", open_build="1.3.0"),
    "liquibase": Terms(
        "the Functional Source License 1.1 (Liquibase 5; each release"
        " becomes Apache-2.0 two years after it)",
        "https://github.com/liquibase/liquibase/blob/master/LICENSE.txt",
        "a third reading of the objects of a PostgreSQL schema, by name",
        "Liquibase 4.33 (Apache-2.0) is installed instead, which reads"
        " the same objects", open_build="4.33.0"),
    "mongosync": Terms(
        "MongoDB's terms for its cluster-to-cluster sync: free with"
        " MongoDB Atlas or Enterprise Advanced, the MongoDB Customer"
        " Agreement otherwise",
        "https://www.mongodb.com/products/tools/mongosync",
        "an online MongoDB move for a hop that declares an Atlas or"
        " Enterprise Advanced licence (`mongodb_entitlement`)",
        "MongoDB moves take the open path: the dump and load programs, or"
        " migkit's own copier and change stream"),
    "riotx": Terms(
        "the Business Source License 1.1 (Redis Ltd.): production use only"
        " with Redis Community Edition, Redis Cloud or Redis Software, and"
        " not in combination with a product that overlaps its own or"
        " Redis's capabilities - whether migkit is one is the operator's"
        " to judge",
        "https://github.com/redis/riotx-dist/blob/main/LICENSE.md",
        "a Redis copy whose target is Redis Community Edition, Redis Cloud"
        " or Redis Software",
        "Redis copies take migkit's own keyspace copier, which every other"
        " target takes anyway"),
}

#: where the operator's answers are kept, one file per machine
TERMS_FILE = "accepted-terms.json"


def _home():
    return Path.home() / ".migkit"


def accepted():
    """{program: record} of the terms accepted on this machine."""
    import json
    try:
        got = json.loads((_home() / TERMS_FILE).read_text())
    except (OSError, ValueError):
        return {}
    return {k: v for k, v in got.items() if isinstance(v, dict)
            and k in TERMS and v.get("licence") == TERMS[k].licence}


def terms_accepted(program):
    """Whether this machine's operator has accepted `program`'s terms - the
    licence as it reads now: a record of an older wording is asked again."""
    return program in accepted()


def _record(program, how):
    import datetime
    import getpass
    import json
    import os
    home = _home()
    home.mkdir(parents=True, exist_ok=True)
    path = home / TERMS_FILE
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        got = {}
    got[program] = {"licence": TERMS[program].licence,
                    "url": TERMS[program].url, "how": how,
                    "by": getpass.getuser(),
                    "at": datetime.datetime.now(datetime.timezone.utc)
                    .isoformat(timespec="seconds")}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(got, indent=1))
    os.replace(tmp, path)


def accept_terms(programs, ask=None, log=print):
    """{program: True accepted | False declined} for each program of
    `programs` whose licence is not open.

    Accepted once, then remembered: an earlier answer on this machine, or
    `MIGKIT_ACCEPT_TERMS` (`atlas,liquibase` or `all`) for an unattended
    install, or `ask(program, terms)` - a question at the terminal, which
    shows the licence and where to read it. Nothing is accepted without one
    of the three: with no terminal and no variable, each is declined and
    the open build or path is said instead."""
    import os
    said = {s.strip().lower() for s in
            os.environ.get("MIGKIT_ACCEPT_TERMS", "").split(",") if s.strip()}
    out = {}
    for program in dict.fromkeys(programs):
        terms = TERMS.get(program)
        if terms is None:
            continue
        if terms_accepted(program):
            out[program] = True
            continue
        if "all" in said or program in said:
            _record(program, "MIGKIT_ACCEPT_TERMS")
            out[program] = True
        elif ask is not None and ask(program, terms):
            _record(program, "asked at doctor --install")
            out[program] = True
        else:
            out[program] = False
        if not out[program]:
            log(f"{program}: its terms ({terms.licence}) were not accepted,"
                f" so {terms.otherwise}. To accept them: MIGKIT_ACCEPT_TERMS"
                f"={program} migkit doctor --install - read them first at"
                f" {terms.url}")
    return out


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
    ("Redis: copy",
     "keyspace copy, every node of a cluster, expiry and access history"
     " kept; a faster path into Redis's own products",
     [], ["riotx"]),
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


def install_missing(log=print, ask=None):
    """Install every missing program with the platform package manager, and
    from its vendor what no package manager carries - a program that is not
    open source only once its terms are accepted (`accept_terms`, `ask`
    the question at a terminal). Returns the capabilities still short
    afterwards."""
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
    fetch = [c for c in dict.fromkeys(wanted) if c in VENDOR and not which(c)]
    # the terms first, all of them, before anything is fetched: a question
    # asked halfway through a long download is one nobody is there for
    answer = accept_terms(fetch, ask, log)
    for n, cmd in enumerate(fetch, 1):
        open_build = cmd in TERMS and not answer.get(cmd)
        if open_build and not TERMS[cmd].open_build:
            continue    # declined, and the open path needs nothing fetched
        if cmd in NEEDS_JAVA and mgr and not _java_home():
            pkg = NEEDS_JAVA[cmd][0 if mgr == "brew" else 1]
            log(f"installing what vendor component {n} runs on ...")
            subprocess.run(["brew", "install", pkg] if mgr == "brew"
                           else ["sudo", "apt-get", "install", "-y", pkg],
                           capture_output=True, text=True)
        log(f"fetching vendor component {n} ...")
        ok = (install_vendor(cmd, log, open_build=True) if open_build
              else install_vendor(cmd, log))
        if not ok:
            log(f"  vendor component {n} did not install; run migkit"
                " doctor again to see which capability is still short")
    if not _present(SECOND_READER):
        install_second_reader(log)
    return [(n, m) for n, _, st, m in capabilities() if st != "ready"]


#: programs no package manager carries, fetched from their vendor at the
#: build measured here: {program: version}
VENDOR = {"atlas": "1.3.0", "liquibase": "5.0.4", "mongosync": "1.21.0",
          "riotx": "1.15.1"}
VENDOR_URL = "https://fastdl.mongodb.org/tools/mongosync/"
#: where each other vendor publishes its builds, by version
VENDOR_URLS = {
    "atlas": "https://release.ariga.io/atlas/",
    "liquibase": "https://github.com/liquibase/liquibase/releases/download/"
                 "v{version}/",
    "riotx": "https://github.com/redis/riotx-dist/releases/download/"
             "v{version}/",
}
#: how each asks its build for its version, where not `--version`
VERSION_ARGS = {"atlas": ["version"]}


def _vendor_url(program, version):
    if program == "mongosync":
        return VENDOR_URL
    return VENDOR_URLS[program].format(version=version)


def _machine():
    system, machine = platform.system(), platform.machine().lower()
    arm = machine in ("arm64", "aarch64")
    x86 = machine in ("x86_64", "amd64")
    return system, arm, x86


def vendor_file(program, open_build=False):
    """The vendor's file of `program` for this machine, or None where it
    publishes none - then it is one to install by hand. `open_build`: the
    open build its terms declined leave (`Terms.open_build`)."""
    if program not in VENDOR:
        return None
    system, arm, x86 = _machine()
    version = (TERMS[program].open_build if open_build else VENDOR[program])
    if program == "atlas":
        # one binary, no archive: the Community build is the same file
        # under `community-`
        name = {"Darwin": "darwin", "Linux": "linux"}.get(system)
        arch = "arm64" if arm else "amd64" if x86 else None
        if not (name and arch):
            return None
        return (f"atlas-{'community-' if open_build else ''}{name}-{arch}"
                f"-v{version}")
    if program == "liquibase":
        # Java, the same archive everywhere; it needs Java 17 on the path
        return f"liquibase-{version}.tar.gz"
    if program == "riotx":
        # built with its own runtime, per system
        name = {"Darwin": "osx", "Linux": "linux"}.get(system)
        arch = "aarch64" if arm else "x86_64" if x86 else None
        if not (name and arch):
            return None
        return f"riotx-standalone-{version}-{name}-{arch}.zip"
    if system == "Darwin":
        arch = "arm-arm64" if arm else "x86_64"
        return f"mongosync-macos-{arch}-{version}.zip"
    if system != "Linux" or not x86:
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


def install_vendor(program, log=print, into=None, open_build=False):
    """Fetch `program` from its vendor and put it beside migkit's own
    programs. True once the installed copy says the build asked for.

    A single file is copied in; an archive whose program needs the files
    beside it (a Java program and its libraries, a runtime of its own) is
    unpacked whole under `~/.migkit/tools` and linked in."""
    import os
    import sysconfig
    import tarfile
    import tempfile
    import urllib.request
    import zipfile
    name = (vendor_file(program, open_build=True) if open_build
            else vendor_file(program))
    if not name:
        log("  its vendor publishes no build for this machine; install it"
            " by hand")
        return False
    version = TERMS[program].open_build if open_build else VENDOR[program]
    into = into or sysconfig.get_path("scripts")
    target = os.path.join(into, program)
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, name)
        try:
            urllib.request.urlretrieve(_vendor_url(program, version) + name,
                                       archive)
        except OSError as e:
            log(f"  could not fetch it ({type(e).__name__})")
            return False
        if not name.endswith((".zip", ".tgz", ".tar.gz")):
            shutil.copyfile(archive, target)
            os.chmod(target, 0o755)
        else:
            opener = (zipfile.ZipFile(archive) if name.endswith(".zip")
                      else tarfile.open(archive))
            # apart from the archive, which is not to be kept beside it
            tree = os.path.join(work, "tree")
            with opener as box:
                if isinstance(box, tarfile.TarFile):
                    box.extractall(tree, filter="data")
                else:
                    box.extractall(tree)
            # the shallowest: an archive whose program is at its top (the
            # Java ones) may carry another of the name further in
            found = sorted((os.path.join(root, program)
                            for root, _, files in os.walk(tree)
                            if program in files and (root.endswith("bin")
                                                     or program in NEEDS_JAVA)),
                           key=lambda p: p.count(os.sep))
            if not found:
                log("  the fetched archive holds no program by that name")
                return False
            if program == "mongosync":
                shutil.copyfile(found[0], target)
            else:
                # its libraries stay beside it
                home = _home() / "tools" / f"{program}-{version}"
                shutil.rmtree(home, ignore_errors=True)
                home.parent.mkdir(parents=True, exist_ok=True)
                top = os.path.dirname(found[0])
                if os.path.basename(top) == "bin":
                    top = os.path.dirname(top)
                shutil.copytree(top, home, symlinks=True)
                for extra, base, folder, _ in ([] if open_build else
                                               VENDOR_EXTRAS.get(program,
                                                                 [])):
                    (home / folder).mkdir(parents=True, exist_ok=True)
                    try:
                        urllib.request.urlretrieve(base + extra,
                                                   home / folder / extra)
                    except OSError as e:
                        log(f"  could not fetch what it needs beside it"
                            f" ({type(e).__name__})")
                        return False
                real = home / os.path.relpath(found[0], top)
                os.chmod(real, os.stat(real).st_mode | 0o755)
                if os.path.lexists(target):
                    os.unlink(target)
                if program in NEEDS_JAVA:
                    # started with the Java found here, unless the operator
                    # names one of their own
                    java = _java_home() or ""
                    with open(target, "w") as f:
                        f.write("#!/bin/sh\n"
                                f"[ -n \"$JAVA_HOME\" ] || JAVA_HOME='{java}'"
                                "\nexport JAVA_HOME\n"
                                f"exec '{real}' \"$@\"\n")
                else:
                    os.symlink(real, target)
            os.chmod(os.path.realpath(target),
                     os.stat(os.path.realpath(target)).st_mode | 0o755)
    got = subprocess.run([target, *VERSION_ARGS.get(program, ["--version"])],
                         capture_output=True, text=True)
    return version in (got.stdout + got.stderr)


#: what a build needs beside it that its archive does not carry:
#: {program: [(file, where it is published, the folder of the install it
#: goes in, its licence)]}, for the build its terms cover only. Liquibase
#: 5 "ships without extensions, drivers, and many other packages" (its
#: 5.0.0 release notes; measured: its archive is 8.8 MB where 4.33's is
#: 354 MB), and migkit reads PostgreSQL through it
VENDOR_EXTRAS = {"liquibase": [
    ("postgresql-42.7.10.jar",
     "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/",
     "lib", "BSD-2-Clause")]}


#: vendor programs that run on Java, and the package that brings a Java
#: 17 - what the Liquibase 5 archive asks for - where none is found:
#: (brew formula, apt package)
NEEDS_JAVA = {"liquibase": ("openjdk", "openjdk-17-jre-headless")}


def _java_home():
    """A Java for a Java program fetched as an archive: the operator's own
    `JAVA_HOME`, the system's, or the one a package manager keeps off the
    path; None where there is none."""
    import os
    cands = [os.environ.get("JAVA_HOME", "")]
    if platform.system() == "Darwin":
        try:
            cands.append(subprocess.run(
                ["/usr/libexec/java_home", "-v", "17+"], capture_output=True,
                text=True, timeout=20).stdout.strip())
        except (OSError, subprocess.TimeoutExpired):
            pass
        # where Homebrew's wrapper of the open build points it (measured:
        # `/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home`)
        cands += [f"{b}/opt/openjdk{s}" for b in ("/opt/homebrew", "/usr/local")
                  for s in ("/libexec/openjdk.jdk/Contents/Home", "")]
    java = shutil.which("java")
    if java:
        cands.append(str(Path(os.path.realpath(java)).parent.parent))
    return next((c for c in cands if c and os.access(
        os.path.join(c, "bin", "java"), os.X_OK)), None)


def build_of(program):
    """Which build of `program` is installed: "open" for the open build its
    declined terms leave in its place, "full" for the one they cover, None
    where it is not installed or does not say. Asked of the program once."""
    path = which(program)
    if not path or program not in TERMS or not TERMS[program].open_build:
        return None
    return _build_of(program, path)


def _build_of(program, path):
    import os
    import re
    from .util import tool_env
    try:
        stamp = os.stat(path).st_mtime_ns
    except OSError:
        return None
    key = (program, path, stamp)
    if key not in _BUILDS:
        try:
            got = subprocess.run([path, *VERSION_ARGS.get(program,
                                                          ["--version"])],
                                 capture_output=True, text=True, timeout=120,
                                 env=tool_env())
            said = got.stdout + got.stderr
        except (OSError, subprocess.TimeoutExpired):
            said = ""
        if program == "atlas":
            # measured: the Community build says `atlas community version
            # v1.3.0`, the default one `atlas version v1.2.4-...`
            _BUILDS[key] = ("open" if "community version" in said
                            else "full" if "version" in said else None)
        else:
            m = re.search(r"(\d+)\.\d+\.\d+", said)
            _BUILDS[key] = (None if not m else
                            "open" if int(m.group(1)) < 5 else "full")
    return _BUILDS[key]


_BUILDS = {}


def schema_reading_note():
    """What the structural schema reading on this machine leaves to
    migkit's own object check, said in `doctor`: "" where it reads
    nothing less than the build its terms cover."""
    if build_of("atlas") != "open":
        return ""
    return ("the structural schema reading here is the open build: tables,"
            " columns, indexes and keys - views, functions, procedures and"
            " triggers are compared by migkit's own object check")


def install_second_reader(log=print):
    """Build the second reader its own environment, beside migkit's.

    From this interpreter, so it is the Python migkit already runs on, and
    at the version measured to write nothing to either side. Said in
    migkit's words; the package is nobody's business but migkit's.
    """
    import sys
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


# ---- licences ----------------------------------------------------------------

#: licences whose code may be combined with migkit's own under the AGPL
#: 3.0 or later - what an imported package may carry. GPL-2.0-only is not
#: among them: that code cannot be combined with GPL 3 code; a program
#: under it is run beside migkit, never imported
AGPL_COMPATIBLE = {
    "0BSD", "AGPL-3.0-only", "AGPL-3.0-or-later", "Apache-2.0",
    "BSD-2-Clause", "BSD-3-Clause", "BSL-1.0", "CC0-1.0", "GPL-2.0-or-later",
    "GPL-3.0-only", "GPL-3.0-or-later", "HPND", "ISC", "LGPL-2.1-only",
    "LGPL-2.1-or-later", "LGPL-3.0-only", "LGPL-3.0-or-later", "MIT",
    "MIT-0", "MPL-2.0", "PSF-2.0", "Python-2.0", "UPL-1.0", "Unlicense",
    "Zlib", "PostgreSQL",
}
#: open licences a program run beside migkit may carry, as a separate
#: program: the compatible ones, and GPL 2 only
OPEN_LICENCES = AGPL_COMPATIBLE | {"GPL-2.0-only", "LGPL-2.0-only",
                                   "EPL-2.0"}

#: what the free-text licence fields and classifiers of packages say, in
#: SPDX's words
_SPDX = [
    (r"^apache(?: software)?(?: license)?,?(?: version)?\s*2(\.0)?$|^apache$"
     r"|^apache software license$", "Apache-2.0"),
    (r"^(?:the )?mit(?: license)?(?: \(mit\))?$", "MIT"),
    (r"^mit-0$", "MIT-0"),
    (r"^bsd(?: license)?$|^bsd[- ]3[- ]clause(?: license)?$"
     r"|^3-clause bsd license$|^bsd 3-clause license$|^new bsd license$",
     "BSD-3-Clause"),
    (r"^bsd[- ]2[- ]clause(?: license)?$", "BSD-2-Clause"),
    (r"^isc(?: license)?(?: \(iscl\))?$", "ISC"),
    (r"^mpl[- ]2\.0$|^mozilla public license 2\.0 \(mpl 2\.0\)$", "MPL-2.0"),
    (r"^psf(?:-2\.0)?$|^python software foundation license$", "PSF-2.0"),
    (r"^gnu (?:library or )?lesser general public license \(lgpl\)$"
     r"|^gnu lesser general public license$", "LGPL-2.1-or-later"),
    # psycopg2's own words for its LGPL 3 with an exception for OpenSSL
    (r"^lgpl with exceptions$", "LGPL-3.0-or-later"),
    (r"^gnu general public license v3 or later \(gplv3\+\)$",
     "GPL-3.0-or-later"),
]


def spdx_of(text):
    """SPDX's name for a licence as a package's metadata writes it, or the
    text as it is where it is not one of the known spellings."""
    import re
    t = " ".join(str(text or "").split()).strip()
    low = t.lower()
    for pat, name in _SPDX:
        if re.match(pat, low):
            return name
    return t


def package_licences(requirements):
    """{package: [SPDX name, ...]} for every package the requirement
    strings bring in, the packages they need in turn included, as the
    installed copies say: an SPDX expression where the package gives one,
    else its classifiers, its licence field, or the first line of its
    licence file. A package not installed here is `[None]`."""
    import importlib.metadata as md
    import re
    from packaging.requirements import Requirement
    out, todo = {}, [(Requirement(r), frozenset()) for r in requirements]
    while todo:
        req, _ = todo.pop()
        key = re.sub(r"[-_.]+", "-", req.name).lower()
        if key in out:
            continue
        try:
            dist = md.distribution(req.name)
        except md.PackageNotFoundError:
            out[key] = [None]
            continue
        out[key] = _licences_of(dist)
        for raw in dist.requires or []:
            nxt = Requirement(raw)
            wanted = [""] + sorted(req.extras)
            if nxt.marker and not any(nxt.marker.evaluate({"extra": e})
                                      for e in wanted):
                continue
            todo.append((nxt, frozenset()))
    return out


def _licences_of(dist):
    import re
    meta = dist.metadata
    expr = meta.get("License-Expression")
    if expr:
        # `A OR B` is either, at the user's choice; `A AND B` is both
        alts = [a.strip("() ") for a in re.split(r"\s+OR\s+", expr)]
        if len(alts) > 1:
            return [next((a for a in alts if a in AGPL_COMPATIBLE),
                         alts[0])]
        return [p.strip("() ") for p in re.split(r"\s+AND\s+", expr)]
    found = [spdx_of(c.split(" :: ")[-1])
             for c in meta.get_all("Classifier") or []
             if c.startswith("License ::") and "OSI Approved" != c.split(
                 " :: ")[-1]]
    # the free-text field says what the classifiers do not; where they say
    # it, the field is often an author's line (`Copyright (c) ...`) or
    # `Dual License`, which names no licence
    field = spdx_of((meta.get("License") or "").strip())
    if field and len(field) < 80 and "\n" not in field and (
            not found or field in OPEN_LICENCES):
        found.append(field)
    if not found:
        for f in dist.files or []:
            if "licen" in str(f).lower() or "copying" in str(f).lower():
                try:
                    head = (dist.locate_file(f).read_text(errors="replace")
                            .strip().splitlines() or [""])[0]
                except OSError:
                    continue
                found.append(spdx_of(head))
                break
    return sorted(set(found)) or [None]
