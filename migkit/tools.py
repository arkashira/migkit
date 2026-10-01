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

Every program and image migkit runs is one entry of `PROGRAM`: its licence,
the packages that install it, the words a report calls it by, how its build
is asked its version, the build measured with migkit and the oldest taken
as it is, the version fetched from its vendor, the image it runs from. The
tables the rest reads - `PROGRAMS`, `PROGRAM_LICENCES`, `IMAGES`, `VENDOR`,
`movers.DRIVEN`, `toolversion`'s patterns, the words filter, the container
image's recipe - are read off it, so a fact about a program is written
once.

What is fetched is pinned twice over (`LOCK`, `data/programs.lock.json`):
by version, and by the digest of every file for every machine and of every
image, read from where each vendor publishes it (`tools/pin_programs.py`
writes it). A file is held to its digest before anything of it is unpacked
or run; one whose vendor publishes none is held to the digest it had the
first time this machine fetched it (`PINS`), and said. `MIGKIT_BUNDLE_BUILD`
on `doctor --install` puts all of it - the files, migkit's wheels, the
second reading's and the images - in one directory with its sums, signed;
`MIGKIT_BUNDLE` installs from one with no network at all.
"""
import platform
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path

from .util import which


@dataclass(frozen=True)
class Program:
    """Everything migkit knows about one program or image it runs."""
    #: its licence, as SPDX names it
    licence: str
    #: the Homebrew formula and the apt package that install it; "" where
    #: no package manager carries it (fetched from its vendor, built from
    #: its source, pulled as an image or put in place by hand)
    brew: str = ""
    apt: str = ""
    #: what a report calls it - never by its own name
    role: str = "copy program"
    #: the engine family whose client it is, for the version check
    #: against the server (`toolversion`)
    client_of: str = ""
    #: how its build is asked its version - () where it is asked nothing
    #: and its install proves the build - and the version in the answer
    version_args: tuple = ("--version",)
    version_pattern: str = r"(\d+\.\d+(?:\.\d+)?)"
    #: the build migkit was measured with (backlog 46)
    measured: str = ""
    #: the oldest build taken as it is: an older one is replaced where the
    #: lock has a build for this machine, and said where it has none
    lowest: str = ""
    #: the version fetched from its vendor
    vendor: str = ""
    #: the image it runs from, by name and tag; the digest is the lock's
    image: str = ""
    #: the package that brings a Java 17 where it runs on one: (brew
    #: formula, apt package)
    java: tuple = ()
    #: the other names it goes by in what it prints
    also: tuple = ()


_PG = r"\(PostgreSQL\)\s+(\d+(?:\.\d+)*)"
_MY = r"Ver\s+(\d+(?:\.\d+)*)"
_JAVA = ("openjdk", "openjdk-17-jre-headless")

#: every program and image migkit runs. The PostgreSQL and MySQL clients
#: print their versions each its own way (`pg_dump (PostgreSQL) 18.6`,
#: `mysqldump  Ver 9.7.1 for macos26.4`); the rest print theirs as the
#: first dotted number
PROGRAM = {
    "psql": Program("PostgreSQL", "libpq", "postgresql-client",
                    "client program", "postgres", version_pattern=_PG,
                    lowest="14"),
    "pg_dump": Program("PostgreSQL", "libpq", "postgresql-client",
                       "dump program", "postgres", version_pattern=_PG,
                       measured="18.6", lowest="14"),
    "pg_restore": Program("PostgreSQL", "libpq", "postgresql-client",
                          "load program", "postgres", version_pattern=_PG,
                          measured="18.6", lowest="14"),
    "pg_dumpall": Program("PostgreSQL", "libpq", "postgresql-client",
                          "dump program", "postgres", version_pattern=_PG),
    "vacuumdb": Program("PostgreSQL", "libpq", "postgresql-client",
                        "client program", "postgres", version_pattern=_PG),
    "pg_basebackup": Program("PostgreSQL", "libpq", "postgresql-client",
                             "page copy program", "postgres",
                             version_pattern=_PG),
    "pg_verifybackup": Program("PostgreSQL", "libpq", "postgresql-client",
                               "page copy program", "postgres",
                               version_pattern=_PG),
    "mysql": Program("GPL-2.0-only", "mysql-client", "default-mysql-client",
                     "client program", "mysql", version_pattern=_MY),
    "mysqldump": Program("GPL-2.0-only", "mysql-client",
                         "default-mysql-client", "dump program", "mysql",
                         version_pattern=_MY),
    "mongodump": Program("Apache-2.0", "mongodb-database-tools", "",
                         "dump program", measured="100.16.1",
                         lowest="100.9"),
    "mongorestore": Program("Apache-2.0", "mongodb-database-tools", "",
                            "load program", measured="100.16.1",
                            lowest="100.9"),
    # not in any package manager: fetched from its vendor
    "mongosync": Program("LicenseRef-MongoDB-Customer-Agreement",
                         role="sync program", measured="1.21.0",
                         vendor="1.21.0"),
    "mydumper": Program("GPL-3.0-or-later", "mydumper", "mydumper",
                        "dump program", measured="1.0.5", lowest="0.21"),
    "myloader": Program("GPL-3.0-or-later", "mydumper", "mydumper",
                        "load program", measured="1.0.5", lowest="0.21"),
    "pgloader": Program("PostgreSQL", "pgloader", "pgloader"),
    "pt-table-sync": Program("GPL-2.0-only", "percona-toolkit",
                             "percona-toolkit", "repair program"),
    # fetched from its vendor, at the build its terms decide (`build_of`)
    "atlas": Program("LicenseRef-Atlas-EULA", role="schema reader",
                     version_args=("version",), vendor="1.3.0"),
    "liquibase": Program("FSL-1.1-ALv2", role="schema reader",
                         version_pattern=r"(?i)liquibase version:?\s*v?"
                                         r"(\d+\.\d+\.\d+)",
                         vendor="5.0.4", java=_JAVA),
    "riotx": Program("BUSL-1.1", vendor="1.15.1"),
    # fetched from their vendors too: the Redis copy by the replica
    # protocol (asked nothing - it takes only a configuration file, so the
    # archive fetched by its versioned name is the build), the Cassandra
    # bulk loader, and Kafka's own cluster mirror (the start script of the
    # Kafka release, which takes only its properties file; also run from
    # Kafka's own image)
    "redis-shake": Program("MIT", vendor="4.6.2", version_args=()),
    "dsbulk": Program("Apache-2.0", vendor="1.11.0", java=_JAVA),
    "connect-mirror-maker.sh": Program(
        "Apache-2.0", vendor="3.8.0", java=_JAVA, version_args=(),
        image="apache/kafka:3.8.0", also=("MirrorMaker",
                                           "connect-mirror-maker")),
    # GPLv2 with the Universal FOSS Exception, driven as a program: its
    # Python API lives only inside its own process (no package carries
    # it). The brew package is an installer that asks for an
    # administrator's password, and no apt repository of the defaults
    # carries it: fetched from its vendor
    "mysqlsh": Program("GPL-2.0-only", measured="26.7.1", vendor="26.7.1",
                       also=("MySQL Shell",)),
    "sqlcmd": Program("MIT", "sqlcmd", "", "client program"),
    "docker": Program("Apache-2.0", role="container runtime"),
    # the images migkit runs: its version-matched PostgreSQL copy, and the
    # streaming pipeline's connectors and its log
    "pgcopydb": Program("PostgreSQL", measured="0.18",
                        image="dimitri/pgcopydb:v0.18"),
    "debezium": Program("Apache-2.0", role="change stream",
                        image="quay.io/debezium/connect:3.0"),
    "redpanda": Program("BUSL-1.1", role="change log",
                        image="redpandadata/redpanda:v26.2.3"),
    # built from source where the redo is (`install_redo_reader`), with
    # Oracle's client it connects through
    "OpenLogReplicator": Program("AGPL-3.0-or-later", role="redo reader"),
    "oracle-instant-client": Program("LicenseRef-Oracle-Free-Use-Terms",
                                     role="client library"),
    # not a program: FreeTDS's ODBC driver and the ODBC manager it loads
    # through, which SAP ASE is reached by (`ASE_DRIVER`) - FreeTDS under
    # the Library GPL 2 "or any later version", taken as 2.1; the ODBC
    # manager under the LGPL 2.1
    "ase-odbc": Program("LGPL-2.1-or-later", "freetds unixodbc",
                        "tdsodbc unixodbc", "driver"),
    # not programs: IBM's ODBC driver for IBM i, from IBM's own package
    # repository (`install_ibm`), and the operator's Db2 Connect licence
    # put beside the IBM data server driver - IBM's, not open source:
    # each is used on the operator's acceptance of IBM's terms (`TERMS`)
    "ibmi-odbc": Program("LicenseRef-IBM-i-Access-Client-Solutions",
                         role="driver"),
    "db2-connect": Program("LicenseRef-IBM-Db2-Connect", role="licence"),
    # built here from its source (`SOURCE_BUILDS`), never on PATH by
    # another name
    "reindex-from-snapshot": Program(
        "Apache-2.0", role="snapshot reader",
        also=("DocumentsFromSnapshotMigration",)),
    # not programs either: Teradata's own driver for Python, installed into
    # migkit's environment (`install_teradata`) - Teradata's licence, free
    # to use and not open source - and the connectors of the program
    # sources' container kind, which docker pulls where a hop runs one
    # (`engines.saas`), under the Elastic License 2.0 they carry: any use
    # but offering them to others as a managed service
    "teradatasql": Program("LicenseRef-Teradata-SQL-Driver", role="driver"),
    "airbyte-connectors": Program("Elastic-2.0", role="connectors"),
}

#: (brew formula, apt package) of each program: "" where no package
#: manager carries it - reported, not attempted. The images, which docker
#: pulls where they run, are not among them
PROGRAMS = {n: (p.brew, p.apt) for n, p in PROGRAM.items()
            if not p.image or p.vendor}
#: each program's licence, as SPDX names it. Every one is run as a program
#: of its own, never imported, so a copyleft licence is no bar to it; one
#: that is not open source at all is in `TERMS` and runs only on the
#: operator's acceptance (`tests/test_every_dependency_may_ship_with_
#: migkit.py` holds both)
PROGRAM_LICENCES = {n: p.licence for n, p in PROGRAM.items()}
#: programs no package manager carries, fetched from their vendor at the
#: build measured here: {program: version}
VENDOR = {n: p.vendor for n, p in PROGRAM.items() if p.vendor}
#: vendor programs that run on Java, and the package that brings a Java 17
#: - what the Liquibase 5 archive asks for - where none is found: (brew
#: formula, apt package)
NEEDS_JAVA = {n: p.java for n, p in PROGRAM.items() if p.java}
#: how each asks its build for its version, where not `--version`
VERSION_ARGS = {n: list(p.version_args) for n, p in PROGRAM.items()
                if p.version_args and p.version_args != ("--version",)}
#: the programs a report calls the dump's, the load's and the copy's
ROLES = {n: p.role for n, p in PROGRAM.items()}

#: not a program on PATH: the second reader lives in an environment of its
#: own (`migkit.second_reader`), because it pins an older numeric stack
SECOND_READER = "second-reader"
#: the version measured to write nothing to either side
SECOND_READER_PACKAGE = "google-pso-data-validator==8.9.3"
SECOND_READER_LICENCE = "Apache-2.0"


def names():
    """Every name a program migkit runs goes by, in the registry and in
    what the programs print - what no operator is shown. A database's own
    name stays (its client is called what the database is: `mysql`), and
    so does the container runtime's, which the operator installs."""
    from .engines import ALIASES, NAMES
    engines = set(NAMES) | set(ALIASES)
    out = set()
    for n, p in PROGRAM.items():
        if n == "docker" or n in engines:
            continue
        out.add(n)
        out.update(p.also)
    out |= set(SOURCE_BUILDS) | {SECOND_READER, "data-validation",
                                 "data_validation", RFS_SCRIPT}
    return out


# ---- the lock: every file and image by version and digest --------------------

#: where the lock is kept, beside migkit's other data
LOCK_FILE = Path(__file__).parent / "data" / "programs.lock.json"
_LOCK = None


def lock():
    """{"files": {url: {"program", "version", "digest", "size"}},
    "images": {name: "ref@digest"}}: what `tools/pin_programs.py` read of
    each vendor's published digests. A digest is `sha256:<hex>` (or the
    `sha512:`/`sha1:` a vendor publishes instead), None where the vendor
    publishes none."""
    global _LOCK
    if _LOCK is None:
        import json
        try:
            _LOCK = json.loads(LOCK_FILE.read_text())
        except (OSError, ValueError):
            _LOCK = {"files": {}, "images": {}}
    return _LOCK


def pinned(url):
    """The lock's entry for a file fetched from `url`, or {}."""
    return dict((lock().get("files") or {}).get(url) or {})


#: the image migkit's own image is built on (`deploy/Dockerfile`), pinned
#: in the lock by digest as the images it runs are
BASE_IMAGE = "python:3.12-slim-bookworm"
#: the Python packages migkit needs, each by the digests of its files: what
#: its image installs and an offline bundle carries
REQUIREMENTS_LOCK = Path(__file__).parent / "data" / "requirements.txt"


def image(name):
    """The image `name` runs from, pinned by digest where the lock has
    one (`name:tag@sha256:...`), by its tag where it has not - or, loaded
    from an offline bundle here, by the id docker loaded it as, which a
    run finds with no registry to ask (`_load_images`)."""
    import json
    ref = PROGRAM[name].image
    got = (lock().get("images") or {}).get(name)
    pinned_ref = got if got and got.split("@")[0] == ref else ref
    try:
        loaded = json.loads((tools_home() / LOADED).read_text()).get(name)
    except (OSError, ValueError, AttributeError):
        loaded = None
    if loaded and loaded.get("ref") == pinned_ref and loaded.get("id"):
        return loaded["id"]
    return pinned_ref


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


#: The programs and images whose licence is not open source. Each is
#: installed (an image: pulled) and used only once the operator has
#: accepted its terms (`accept_terms`), for what those terms allow;
#: declined, the open build or the open path takes its place and migkit
#: says so.
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
    # an image the streaming pipeline pulls, not a program installed here:
    # its terms are asked for here all the same, and the pipeline is not
    # started until they are accepted (`movers.stream_ways`)
    "redpanda": Terms(
        "the Business Source License 1.1 (Redpanda Data): any use but"
        " offering it to others as a streaming or queuing service",
        "https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md",
        "the log of migkit's streaming pipeline, run on this machine for one"
        " hop's changes - never a streaming service hosted for others",
        "a hop's changes follow its source by the ways that need no"
        " pipeline - migkit's change tail or the servers' own replication -"
        " and one with neither is not followed"),
    "ibmi-odbc": Terms(
        "IBM's licence for IBM i Access Client Solutions, of which the"
        " ODBC driver is part (no charge)",
        "https://www.ibm.com/support/pages/ibm-i-access-client-solutions",
        "reading, writing and following Db2 for i (IBM i) tables over"
        " ODBC, their types as IBM i keeps them",
        "IBM i is reached through the Mapepire server where the operator"
        " runs it on the system, or over DRDA under a Db2 Connect licence;"
        " with neither, not from this machine"),
    "db2-connect": Terms(
        "IBM Db2 Connect: the operator's own licence from IBM - its file"
        " on this machine, named by MIGKIT_DB2_CONNECT_LICENSE, or the"
        " server activated for it by its administrator",
        "https://www.ibm.com/products/db2-connect",
        "reaching Db2 for z/OS, and Db2 for i where its ODBC driver is not"
        " installed, over DRDA through the IBM data server driver",
        "Db2 for z/OS is reached only where its subsystem is activated for"
        " Db2 Connect by its administrator; Db2 for i through its ODBC"
        " driver or its Mapepire server"),
    "teradatasql": Terms(
        "Teradata's licence for its SQL driver for Python (no charge; its"
        " early-release features, FastLoad and FastExport among them,"
        " provided as they are)",
        "https://github.com/Teradata/python-driver/blob/master/LICENSE",
        "reading, writing and comparing Teradata tables, in its parallel"
        " sessions where a table is large",
        "Teradata is not reached from this machine: a hop with a Teradata"
        " side stops, saying so"),
    "airbyte-connectors": Terms(
        "the Elastic License 2.0 of the certified connectors for SaaS"
        " applications: any use but providing them to others as a managed"
        " service",
        "https://www.elastic.co/licensing/elastic-license",
        "running a connector a hop names, as a program or a container on"
        " this machine, for that hop's own copy and follow",
        "a hop whose source is such a connector stops, saying so; the"
        " other kinds of program source need no terms"),
    # the redo-file reader is open (AGPL); the client library it is built
    # against to reach the database is Oracle's own, free to use under
    # these terms and not open source
    "oracle-instant-client": Terms(
        "Oracle's Free Use Terms and Conditions for its Instant Client",
        "https://www.oracle.com/downloads/licenses/"
        "oracle-free-license.html",
        "the redo-file reader's connection to the Oracle database it reads,"
        " for the dictionary it names the redo's tables by",
        "the redo-file reader is not built, and migkit's own LogMiner"
        " reader follows Oracle, which needs nothing installed"),
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


#: not a program either: the extensions the compiled cross-engine mover
#: attaches servers and files through (`crossmove`), installed into the
#: analytical engine migkit already depends on
COMPILED_MOVER = "compiled-mover"


def compiled_mover_present():
    from . import crossmove
    return not (crossmove.missing_extensions(tuple(crossmove.EXTENSIONS))
                or set(crossmove.LAKES) - crossmove.installed_extensions())


#: not a program on PATH either: the driver `engines.ase` connects through
ASE_DRIVER = "ase-odbc"


def ase_driver_present():
    """FreeTDS's ODBC driver where `engines.ase` looks for it, and an ODBC
    manager the driver module loads - it installs without one, and then
    fails to import (measured: `libodbc.2.dylib ... no such file`)."""
    import os

    from .engines.ase import DRIVERS
    if not any(os.path.exists(p) for p in DRIVERS):
        return False
    try:
        import pyodbc  # noqa: F401
    except ImportError:
        return False
    return True


#: IBM's components, which are not programs on PATH either: the ODBC
#: driver `engines.db2i` connects through, and the Db2 Connect licence
#: `engines.db2zos` (and DRDA to IBM i) needs
IBMI_ODBC, DB2_CONNECT = "ibmi-odbc", "db2-connect"


def ibmi_odbc_present():
    from .engines.db2i import odbc_driver
    return odbc_driver() is not None


def db2_connect_present():
    from .engines.db2 import db2_connect_licence
    return db2_connect_licence() is not None


#: Teradata's driver, and the terms the certified SaaS connectors run
#: under: not programs on PATH
TERADATA_DRIVER, SAAS_CONNECTORS = "teradatasql", "airbyte-connectors"


def teradata_driver_present():
    import importlib.util
    return importlib.util.find_spec("teradatasql") is not None


def install_teradata(log=print):
    """Teradata's driver into migkit's own environment, once its terms are
    accepted: by the package manager migkit runs under where there is
    one, else by pip. True once it imports."""
    import sys
    uv = which("uv")
    step = ([uv, "pip", "install", "--python", sys.executable,
             TERADATA_DRIVER] if uv else
            [sys.executable, "-m", "pip", "install", "-q", TERADATA_DRIVER])
    if not _step(step, log, "it did not install"):
        return False
    import importlib
    importlib.invalidate_caches()
    return teradata_driver_present()


def install_ibm(component, log=print):
    """IBM's ODBC driver for IBM i from IBM's own package repository, with
    the ODBC manager it loads through; or the operator's Db2 Connect
    licence file (`MIGKIT_DB2_CONNECT_LICENSE`) copied beside the IBM data
    server driver. Called once the terms are accepted. True once it is in
    place. The repository's list is fetched and held to the lock like any
    other file, then put where the package manager reads it."""
    import os
    if component == DB2_CONNECT:
        path = os.environ.get("MIGKIT_DB2_CONNECT_LICENSE", "")
        if not path:
            log("  no licence file named: MIGKIT_DB2_CONNECT_LICENSE=<its"
                " db2consv_*.lic> migkit doctor --install puts it in place"
                " - a server activated for Db2 Connect needs none")
            return False
        import ibm_db
        into = os.path.join(os.path.dirname(os.path.abspath(ibm_db.__file__)),
                            "clidriver", "license")
        if not os.path.isfile(path) or not os.path.basename(path) \
                .startswith("db2consv_"):
            log("  the file MIGKIT_DB2_CONNECT_LICENSE names is not a Db2"
                " Connect licence (db2consv_*.lic)")
            return False
        os.makedirs(into, exist_ok=True)
        shutil.copyfile(path, os.path.join(into, os.path.basename(path)))
        return db2_connect_present()
    mac = platform.system() == "Darwin"
    if mac and shutil.which("brew"):
        steps = [["brew", "tap", "ibm/iaccess", IBMI_TAP],
                 ["brew", "install", "unixodbc", "ibm-iaccess"]]
    elif shutil.which("apt-get"):
        steps = [(IBMI_APT, "/etc/apt/sources.list.d/ibmi-acs-1.1.0.list"),
                 _sudo(["apt-get", "update"]),
                 _sudo(["apt-get", "install", "-y", "unixodbc",
                        "ibm-iaccess"])]
    elif shutil.which("dnf"):
        steps = [(IBMI_RPM, "/etc/yum.repos.d/ibmi-acs.repo"),
                 _sudo(["dnf", "install", "-y", "--refresh", "unixODBC",
                        "ibm-iaccess"])]
    else:
        log("  no package manager here that IBM publishes the driver for")
        return False
    for step in steps:
        ok = (_placed(*step, log) if isinstance(step, tuple)
              else _step(step, log, "it did not install"))
        if not ok:
            return False
    return ibmi_odbc_present()


def _sudo(argv):
    """`argv` as root: through sudo where this user is not."""
    import os
    return argv if getattr(os, "geteuid", lambda: 0)() == 0 \
        else ["sudo", *argv]


def _step(argv, log, failed, cwd=None, env=None):
    """One install step through the runner; its failure said in its last
    words, program names out (`runner.said`). True where it worked."""
    from . import runner
    try:
        got = runner.run(argv, env, check=False, cwd=cwd, check_flags=False)
    except runner.Blocked as e:
        log(f"  {failed}: {e}")
        return False
    if got.returncode != 0:
        said = runner.said((got.stderr or "") + (got.stdout or ""))
        log(f"  {failed}: {(said.splitlines() or [''])[-1][:120]}")
        return False
    return True


def _placed(url, dest, log):
    """The file at `url`, held to the lock, put at `dest` as root."""
    import os
    import tempfile
    with tempfile.TemporaryDirectory() as work:
        got = os.path.join(work, os.path.basename(dest))
        if not fetch(url, got, log):
            return False
        return _step(_sudo(["install", "-m", "0644", got, dest]), log,
                     "its repository could not be put in place")


#: where IBM publishes the IBM i ODBC driver's packages
IBMI_TAP = "https://public.dhe.ibm.com/software/ibmi/products/odbc/macos/tap/"
IBMI_APT = ("https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/"
            "dists/1.1.0/ibmi-acs-1.1.0.list")
IBMI_RPM = ("https://public.dhe.ibm.com/software/ibmi/products/odbc/rpms/"
            "ibmi-acs.repo")


def _present(cmd):
    if cmd == SECOND_READER:
        return second_reader_present()
    if cmd == COMPILED_MOVER:
        return compiled_mover_present()
    if cmd == "OpenLogReplicator":
        from .engines import oracle_redo
        return oracle_redo.program()
    if cmd == ASE_DRIVER:
        return ase_driver_present()
    if cmd == IBMI_ODBC:
        return ibmi_odbc_present()
    if cmd == DB2_CONNECT:
        return db2_connect_present()
    if cmd == TERADATA_DRIVER:
        return teradata_driver_present()
    if cmd == SAAS_CONNECTORS:
        return terms_accepted(SAAS_CONNECTORS)
    if cmd in SOURCE_BUILDS:
        return built_program(cmd) is not None
    return which(cmd)


#: programs no package manager carries for this machine, built here from
#: their own published source, at the revision measured with migkit:
#: {program: (revision, source archive)}
SOURCE_BUILDS = {
    "ora2pg": ("v25.0", "https://github.com/darold/ora2pg/archive/refs/tags"
                        "/v25.0.tar.gz"),
    # the snapshot reader of OpenSearch's migration project: its releases
    # carry the assistant's charts and no build of the reader, which its
    # own Gradle build makes (Java 17, the build's wrapper fetching Gradle
    # itself)
    "reindex-from-snapshot": (
        "3.3.8", "https://github.com/opensearch-project/opensearch-migrations"
                 "/archive/refs/tags/3.3.8.tar.gz"),
    "sqlines": ("4d0d87896cead95481fc4656f831a8ac6d7ec291",
                "https://github.com/dmtolpeko/sqlines/archive/"
                "4d0d87896cead95481fc4656f831a8ac6d7ec291.tar.gz"),
}


#: what the pinned SQLines source needs to build under clang and outside
#: Linux, measured on macOS arm64: a Linux-only header it never uses, three
#: pointers compared with `false`, and the commercial licence check the
#: open repository leaves out (a class answering "no check needed")
SQLINES_PATCHES = {
    "sqlparser/file.cpp": [("#include <sys/io.h>", "")],
    "sqlines/file.cpp": [("#include <sys/io.h>", "")],
    "sqlines/filelist.cpp": [("#include <sys/io.h>", "")],
    "sqlparser/oracle.cpp": [("(first == false) ? second->Compare",
                              "(first == NULL) ? second->Compare")],
    "sqlparser/select.cpp": [("GetNextCharToken(',', L',') == false",
                              "GetNextCharToken(',', L',') == NULL")],
    "sqlparser/statements.cpp": [("if(next_begin == false)",
                                  "if(next_begin == NULL)")],
}
SQLINES_LICENSE_H = """#ifndef MIGKIT_SQLINES_LICENSE_H
#define MIGKIT_SQLINES_LICENSE_H
#include <string>
class License {
public:
    void Set(const char *) {}
    bool IsLicenseCheckRequired() { return false; }
    bool IsEmpty() { return true; }
    std::string GetName() { return ""; }
};
#endif
"""


def tools_home():
    """Where migkit keeps the programs it builds for itself."""
    import os
    from pathlib import Path
    return Path(os.environ.get("MIGKIT_TOOLS") or
                Path.home() / ".migkit" / "tools")


def built_program(name):
    """(the command line that runs `name`, the environment it needs), or
    None where it is neither built here nor on PATH."""
    home = tools_home() / name
    if name == "ora2pg" and home.exists():
        # its installer lays itself out as the local Perl does (bin and
        # lib under usr/local on Linux, Library/Perl on macOS): found by
        # what it installed, not by where it was asked to
        script = next((p for p in home.rglob("ora2pg")
                       if p.parent.name == "bin" and p.is_file()), None)
        module = next(home.rglob("Ora2Pg.pm"), None)
        if script and module:
            return (["perl", str(script)],
                    {"PERL5LIB": str(module.parent)})
    if name == "sqlines":
        program = home / "sqlines"
        if program.exists():
            return ([str(program)], {})
    if name == "reindex-from-snapshot":
        program = home / "bin" / RFS_SCRIPT
        if program.exists():
            return ([str(program)], {"JAVA_HOME": _java_home() or ""})
        return None
    found = which(name)
    return ([found], {}) if found else None

def install_from_source(name, log=print):
    """Fetch `name`'s source at the revision measured here - held to the
    lock before it is unpacked - and build it under `tools_home()`. True
    once the built program answers."""
    import os
    import tempfile
    revision, url = SOURCE_BUILDS[name]
    home = tools_home() / name
    home.mkdir(parents=True, exist_ok=True)
    if name == "reindex-from-snapshot" and _bundle() is not None:
        # its build fetches what it needs as it goes; the bundle carries it
        # built, which Java runs wherever it runs
        return _unbundle_build(name, home, log)
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, "src.tar.gz")
        if not fetch(url, archive, log):
            return False
        extract(archive, work)
        top = next(os.path.join(work, d) for d in os.listdir(work)
                   if os.path.isdir(os.path.join(work, d)))

        def step(cmd, cwd):
            return _step(cmd, log, "the build stopped", cwd=cwd)
        if name == "ora2pg":
            # Perl with the DBI module; nothing of it is loaded into
            # migkit - it runs as a program of its own
            ok = step(["perl", "Makefile.PL", f"INSTALL_BASE={home}"], top) \
                and step(["make"], top) and step(["make", "install"], top)
        elif name == "reindex-from-snapshot":
            ok = _build_rfs(top, home, log)
        else:
            for rel, edits in SQLINES_PATCHES.items():
                path = os.path.join(top, rel)
                text = open(path, encoding="latin-1").read()
                for old, new in edits:
                    text = text.replace(old, new)
                open(path, "w", encoding="latin-1").write(text)
            open(os.path.join(top, "sqlines", "license.h"), "w").write(
                SQLINES_LICENSE_H)
            parser = os.path.join(top, "sqlparser")
            cpp = sorted(f for f in os.listdir(parser) if f.endswith(".cpp"))
            ok = step(["c++", "-O2", "-w", "-c"] + cpp, parser) and step(
                ["ar", "rcs", "sqlparser.a"] +
                [f[:-4] + ".o" for f in cpp], parser)
            main = os.path.join(top, "sqlines")
            # its own str.cpp is the parser's again: linked twice, every
            # symbol in it is defined twice
            ok = ok and step(["c++", "-O2", "-w", "applog.cpp", "file.cpp",
                              "filelist.cpp", "main.cpp", "os.cpp",
                              "parameters.cpp", "sqlines.cpp",
                              "../sqlparser/sqlparser.a", "-o",
                              str(home / "sqlines")], main)
        if not ok:
            return False
    return built_program(name) is not None




#: the start script the snapshot reader's Gradle build lays out
RFS_SCRIPT = "DocumentsFromSnapshotMigration"




def _build_rfs(top, home, log):
    """The snapshot reader built by its own Gradle wrapper into a
    distribution - its start script and every jar it needs - and kept
    whole under `home`, put in place in one step (`_swap`). Java 17 is
    what its build asks for."""
    import os
    java = _java_home()
    if not java:
        log("  the build needs a Java 17, and none was found")
        return False
    if not _step(["./gradlew", "--no-daemon", "-q",
                  f":{RFS_SCRIPT}:installDist"], log, "the build stopped",
                 cwd=top, env={"JAVA_HOME": java}):
        return False
    built = os.path.join(top, RFS_SCRIPT, "build", "install", RFS_SCRIPT)
    if not os.path.isdir(built):
        return False
    _swap(built, home)
    return True


def _swap(tree, home):
    """`tree` copied to `home` in one step: beside it first, then renamed
    over it - a copy that fails part way leaves the working install where
    it was, where removing it first left nothing."""
    import os
    home = Path(home)
    home.parent.mkdir(parents=True, exist_ok=True)
    new = home.with_name(home.name + ".new")
    old = home.with_name(home.name + ".old")
    shutil.rmtree(new, ignore_errors=True)
    shutil.copytree(tree, new, symlinks=True)
    shutil.rmtree(old, ignore_errors=True)
    if home.exists() or home.is_symlink():
        os.replace(home, old)
    os.replace(new, home)
    shutil.rmtree(old, ignore_errors=True)




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
     ["mysqldump"], ["mydumper", "myloader", "mysqlsh"]),
    ("MongoDB: verify and repair",
     "compare documents and indexes",
     [], []),
    ("MongoDB: bulk move",
     "collection load between MongoDB deployments",
     ["mongodump", "mongorestore"], ["mongosync"]),
    ("SQL Server: verify",
     "compare rows and objects",
     [], ["sqlcmd"]),
    ("Db2 for i (IBM i): verify, copy and follow",
     "compare and copy tables, and follow their journal; its own driver"
     " carries the types as IBM i keeps them",
     [], [IBMI_ODBC]),
    ("Db2 for z/OS: verify and copy",
     "compare and copy tables over DRDA",
     [], [DB2_CONNECT]),
    ("Mainframe files: verify and copy",
     "read, compare and write files described by COBOL copybooks, EBCDIC"
     " and packed decimals exactly",
     [], []),
    ("SAP ASE: verify and copy",
     "compare and copy tables through ASE's own protocol",
     [ASE_DRIVER], []),
    ("Teradata: verify, copy and load",
     "compare tables on the servers, copy them in parallel sessions, load"
     " whole batches",
     [TERADATA_DRIVER], []),
    ("SAP application data: copy, follow and verify",
     "read extractors and CDS views through their OData services, follow"
     " their delta queue, count on the SAP side",
     [], []),
    ("SaaS applications: copy, follow and verify",
     "run a connector as a program, copy and follow what it reads, follow"
     " Salesforce's own change events; containers need docker",
     [], [SAAS_CONNECTORS, "docker"]),
    ("TiDB, CockroachDB, Aurora DSQL: verify, copy and follow",
     "compare and copy tables over the MySQL or PostgreSQL wire, and follow"
     " what each publishes of its changes, in commit order",
     [], []),
    ("Couchbase, Cosmos DB: verify, copy and follow",
     "compare and copy documents through each one's own SDK, and follow"
     " its change feed",
     [], []),
    ("SQLite: verify, repair and copy",
     "compare rows and schema, repair with undo, copy table by table",
     [], []),
    ("Redis / Kafka: verify",
     "compare keyspaces and topic offsets",
     [], []),
    ("Redis: copy",
     "keyspace copy, every node of a cluster, expiry and access history"
     " kept; a faster path by the replica protocol, and one into Redis's"
     " own products",
     [], ["redis-shake", "riotx"]),
    ("Kafka: copy",
     "topic copy with every group's position translated; a faster path"
     " through the cluster mirror Kafka ships",
     [], ["connect-mirror-maker.sh"]),
    ("Cassandra / ScyllaDB: bulk copy",
     "tables copied with each cell's time to live and write time; a"
     " faster path through the bulk loader, resumable where it stopped",
     [], ["dsbulk"]),
    ("OpenSearch: copy from a snapshot",
     "indexes copied from the source's own snapshot, the cluster itself"
     " left unread",
     [], ["reindex-from-snapshot"]),
    ("Cross-engine (any pair)",
     "create missing tables, resumable copy, cross-dialect verify;"
     " MySQL to PostgreSQL also in one bulk pass",
     [], ["pgloader"]),
    ("Cross-engine: server to server copy",
     "MySQL into PostgreSQL and back, and SQLite, DuckDB, Parquet, Delta"
     " and Iceberg files into PostgreSQL, range by range with no row"
     " through migkit, each range proved by both servers",
     [], [COMPILED_MOVER]),
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
    ("Stored code: more converters",
     "procedures, functions and triggers converted more than one way,"
     " each version proved against the source before it is chosen",
     [], ["ora2pg", "sqlines"]),
]


#: the redo-file reader for Oracle: built from source where it runs
#: (`install_redo_reader`), so a capability only on Linux x86-64 - it runs
#: beside the redo files, and `doctor --install` on that host puts it there
REDO_READER = "OpenLogReplicator"
#: Oracle's client library it is built against: not open source, so
#: fetched only on the operator's acceptance of its terms
REDO_READER_CLIENT = "oracle-instant-client"


def _redo_reader_here():
    from .engines import oracle_redo
    return oracle_redo.platform_runs_it()


def _capabilities():
    if not _redo_reader_here():
        return CAPABILITIES
    return CAPABILITIES + [
        ("Oracle: follow at high change rates",
         "read the redo files where they are, with no mining in the"
         " database", [], [REDO_READER])]


def capabilities():
    """(name, what, state, missing) per capability.

    state: ready | reduced | unavailable
      ready        everything is here
      reduced      core works, an optional faster/deeper path is missing
      unavailable  a required program is missing
    """
    out = []
    for name, what, needs, optional in _capabilities():
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
        if c in (SECOND_READER, COMPILED_MOVER):
            continue    # `doctor --install` builds it wherever Python runs
        if c == REDO_READER:
            continue    # built from source by `doctor --install`
        if c in VENDOR and vendor_file(c):
            continue    # fetched from its vendor (`install_vendor`)
        if c in SOURCE_BUILDS:
            continue    # built from its source (`install_from_source`)
        if c in (IBMI_ODBC, DB2_CONNECT):
            continue    # IBM's, put in place by `install_ibm`
        if c in (TERADATA_DRIVER, SAAS_CONNECTORS):
            continue    # put in place, or accepted, by `install_missing`
        formula, apt = PROGRAMS.get(c, ("", ""))
        if not (mgr and (formula if mgr == "brew" else apt)):
            out.append(c)
    return sorted(set(out))



def _manager():
    """The package manager migkit drives here: "brew", "apt", or None."""
    mac = platform.system() == "Darwin"
    return "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)


def _packages(mgr, pkgs, log):
    """Every package in one call of the package manager - its index read
    first where it keeps one - and, where that call fails, each on its
    own, so one that cannot be had does not keep the rest out. Counted,
    not named: which package provides a capability is migkit's business,
    and the line used to read `installing mydumper ...`."""
    if not (mgr and pkgs):
        return
    log(f"installing {len(pkgs)} components with the package manager ...")
    quiet = (lambda m: None)
    if mgr == "apt":
        _step(_sudo(["apt-get", "update"]), quiet, "")

    def one(names):
        return (["brew", "install", *names] if mgr == "brew"
                else _sudo(["apt-get", "install", "-y", *names]))
    if _step(one(pkgs), quiet, ""):
        return
    for n, pkg in enumerate(pkgs, 1):
        if not _step(one([pkg]), quiet, ""):
            log(f"  component {n} did not install; run migkit doctor again"
                " to see which capability is still short")


def _version_here(program):
    """(whether `program` is on the path here, the version its build there
    says - None where it says none, or is asked nothing), asked once per
    build (`toolversion`)."""
    import os
    import re

    from . import toolversion
    path = which(program)
    if not path:
        return False, None
    facts = PROGRAM.get(program)
    args = tuple(facts.version_args) if facts else ("--version",)
    try:
        real = os.path.realpath(path)
        stamp = os.stat(real).st_mtime_ns
    except OSError:
        return True, None
    if not args:
        return True, None
    said = toolversion._said(real, stamp, args) or ""
    m = re.search(facts.version_pattern if facts
                  else r"(\d+\.\d+(?:\.\d+)?)", said)
    return True, (m.group(1) if m else None)


def _pinned_here(program):
    """Whether the build of vendor program `program` on the path here is
    one migkit takes: its own install of a pinned version, a build that
    says one - the full one or the open one its terms leave - or one that
    says no version, which is left as it is. A build that says another is
    replaced: `doctor --install` skipped anything on the path, and a
    build other than the pinned one was never put right."""
    import os
    here, have = _version_here(program)
    if not here:
        return False
    versions = {VENDOR[program]}
    if program in TERMS and TERMS[program].open_build:
        versions.add(TERMS[program].open_build)
    real = Path(os.path.realpath(which(program)))
    if any(tools_home() / f"{program}-{v}" in real.parents
           for v in versions):
        return True
    return have is None or have in versions


def install_missing(log=print, ask=None):
    """Install every missing program with the platform package manager, and
    from its vendor what no package manager carries - a program that is not
    open source only once its terms are accepted (`accept_terms`, `ask`
    the question at a terminal), asked in the one question with the terms
    of the images migkit pulls.

    A program already here is replaced where it is not a build migkit
    takes: a vendor's build other than the one pinned, a packaged one
    older than its `lowest` where the lock has a build for this machine
    (`install_pinned`) - said where it has none. The package manager is
    called once for all its packages, the vendors' builds are fetched side
    by side, and each is put in place in one step. From an offline bundle
    (`MIGKIT_BUNDLE`) every file and image comes from it, never from the
    network; with `MIGKIT_BUNDLE_BUILD` nothing is installed here - the
    bundle is built there instead (`build_bundle`). Returns the
    capabilities still short afterwards."""
    import os
    from concurrent.futures import ThreadPoolExecutor
    from . import envkeys
    where = os.path.expanduser(envkeys.text("MIGKIT_BUNDLE_BUILD"))
    if where:
        build_bundle(where, log, ask)
        return [(n, m) for n, _, st, m in capabilities() if st != "ready"]
    mgr = _manager()
    if not mgr:
        log("no supported package manager (brew/apt) here; install by hand")
    wanted = []
    for _, _, needs, optional in _capabilities():
        wanted += needs + optional
    wanted = list(dict.fromkeys(wanted))
    pkgs = []
    for cmd in wanted:
        if cmd in (SECOND_READER, COMPILED_MOVER) or cmd in SOURCE_BUILDS \
                or _present(cmd):
            continue
        formula, apt = PROGRAMS.get(cmd, ("", ""))
        for pkg in ((formula if mgr == "brew" else apt) or "").split():
            if mgr and pkg not in pkgs:
                pkgs.append(pkg)
    _packages(mgr, pkgs, log)
    fetch_them = [c for c in wanted if c in VENDOR and not _pinned_here(c)]
    ibm = [c for c in wanted if c in (IBMI_ODBC, DB2_CONNECT)
           and not _present(c)]
    # Teradata's driver, installed here; the SaaS connectors' terms, asked
    # for here and used where a hop runs one - docker pulls a container
    # where it runs, as it pulls the pipeline's images
    teradata = TERADATA_DRIVER in wanted and not _present(TERADATA_DRIVER)
    redo = REDO_READER in wanted and not _present(REDO_READER)
    # the terms first, all of them, before anything is fetched or built: a
    # question asked halfway through a long download is one nobody is
    # there for. An image under closed terms is asked for here too: docker
    # pulls it where it runs, and migkit runs it only once they are
    # accepted (`tooluse.allowed`)
    answer = accept_terms(fetch_them + ibm + [i for i in IMAGES if i in TERMS]
                          + ([REDO_READER_CLIENT] if redo else [])
                          + ([TERADATA_DRIVER] if teradata else [])
                          + [SAAS_CONNECTORS], ask, log)
    if _bundle() is not None:
        _load_images(answer, log)
    java = sorted({NEEDS_JAVA[c][0 if mgr == "brew" else 1]
                   for c in fetch_them if c in NEEDS_JAVA})
    if java and mgr and not _java_home():
        log("installing what the vendor components run on ...")
        _packages(mgr, java, log)
    jobs = []
    for n, cmd in enumerate(fetch_them, 1):
        open_build = cmd in TERMS and not answer.get(cmd)
        if open_build and not TERMS[cmd].open_build:
            continue    # declined, and the open path needs nothing fetched
        jobs.append((n, cmd, open_build))

    def fetch_one(job):
        n, cmd, open_build = job
        log(f"fetching vendor component {n} ...")
        ok = (install_vendor(cmd, log, open_build=True) if open_build
              else install_vendor(cmd, log))
        if not ok:
            log(f"  vendor component {n} did not install; run migkit"
                " doctor again to see which capability is still short")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(fetch_one, jobs))
    _replace_old_builds(wanted, log)
    if teradata and answer.get(TERADATA_DRIVER):
        log("installing the Teradata driver ...")
        if not install_teradata(log):
            log("  the Teradata driver is not in place; run migkit doctor"
                " again to see which capability is still short")
    for n, cmd in enumerate([c for c in ibm if answer.get(c)], 1):
        log(f"putting IBM component {n} in place ...")
        if not install_ibm(cmd, log):
            log(f"  IBM component {n} is not in place; run migkit doctor"
                " again to see which capability is still short")
    for n, cmd in enumerate([c for c in wanted
                             if c in SOURCE_BUILDS and not _present(c)], 1):
        log(f"building component {n} from its source ...")
        if not install_from_source(cmd, log):
            log(f"  component {n} did not build; run migkit doctor again"
                " to see which capability is still short")
    if not _present(SECOND_READER):
        install_second_reader(log)
    if redo and answer.get(REDO_READER_CLIENT):
        install_redo_reader(log)
    if not _present(COMPILED_MOVER):
        from . import crossmove
        log("installing the server to server copy's parts ...")
        crossmove.install_extensions(log)
    return [(n, m) for n, _, st, m in capabilities() if st != "ready"]


def _replace_old_builds(wanted, log):
    """Each packaged program of `wanted` on the path here that is older
    than the oldest build migkit takes, replaced by the build the lock
    pins for this machine - once for the programs one release carries -
    and said where the lock has none."""
    from . import toolversion
    done = set()
    for cmd in wanted:
        facts = PROGRAM.get(cmd)
        if facts is None or not facts.lowest:
            continue
        here, have = _version_here(cmd)
        if not here or not toolversion.older(have, facts.lowest):
            continue
        head = _package_head(cmd)
        if head in done:
            continue
        done.add(head)
        if head and install_pinned(head, log):
            continue
        log(f"  the installed {facts.role} is {have}, older than the"
            f" {facts.lowest} migkit takes, and no build of it is pinned for"
            " this machine: upgrade it with the package manager")


#: packaged programs whose own releases publish builds migkit puts in place
#: where the package manager's is older than it takes: {the first of the
#: programs one release carries: (all of them, the release, where its
#: files are)}
PACKAGE_BUILDS = {
    "mydumper": (("mydumper", "myloader"), "1.0.5-1",
                 "https://github.com/mydumper/mydumper/releases/download/"
                 "v{version}/"),
    "mongodump": (("mongodump", "mongorestore"), "100.16.1",
                  "https://fastdl.mongodb.org/tools/db/"),
}


def _package_head(program):
    return next((h for h, (family, _, _) in PACKAGE_BUILDS.items()
                 if program in family), None)


#: the Debian and Ubuntu releases the pinned packages are built for, by the
#: version their os-release gives
_CODENAMES = {("ubuntu", "20.04"): "focal", ("ubuntu", "22.04"): "jammy",
              ("ubuntu", "24.04"): "noble", ("debian", "11"): "bullseye",
              ("debian", "12"): "bookworm", ("debian", "13"): "trixie"}


def package_file(head):
    """The file of `head`'s pinned release for this machine (or the one a
    bundle is built for), or None where its release has none."""
    _, version, _ = PACKAGE_BUILDS[head]
    system, arm, x86 = _machine()
    release = _os_release()
    ident, ver = release.get("ID", ""), release.get("VERSION_ID", "")
    if head == "mydumper":
        if system != "Linux" or not (arm or x86):
            return None
        code = release.get("VERSION_CODENAME") or _CODENAMES.get(
            (ident, ver))
        if ident in ("ubuntu", "debian") and code:
            return (f"mydumper_{version}.{code}_"
                    f"{'arm64' if arm else 'amd64'}.deb")
        if ident in ("rhel", "rocky", "almalinux", "centos") and ver:
            return (f"mydumper-{version}.el{ver.split('.')[0]}."
                    f"{'aarch64' if arm else 'x86_64'}.rpm")
        return None
    arch = "arm64" if arm else "x86_64" if x86 else None
    if arch is None:
        return None
    if system == "Darwin":
        return f"mongodb-database-tools-macos-{arch}-{version}.zip"
    if system != "Linux":
        return None
    name = {("ubuntu", "20.04"): "ubuntu2004", ("ubuntu", "22.04"):
            "ubuntu2204", ("ubuntu", "24.04"): "ubuntu2404",
            ("debian", "11"): "debian11", ("debian", "12"): "debian12",
            ("amzn", "2023"): "amazon2023"}.get((ident, ver))
    if name is None or (arm and name.startswith("debian")):
        return None
    if arm and name == "amazon2023":
        arch = "aarch64"
    return f"mongodb-database-tools-{name}-{arch}-{version}.tgz"


def install_pinned(head, log=print):
    """The pinned release of the programs `head` leads, for this machine,
    held to the lock and put in place: a package through the package
    manager (which brings what it links against), an archive kept whole
    under `tools_home()` and linked in first on migkit's path. True once
    each of them says the release's version."""
    import os
    import tempfile
    family, version, base = PACKAGE_BUILDS[head]
    name = package_file(head)
    if not name:
        return False
    url = base.format(version=version) + name
    with tempfile.TemporaryDirectory() as work:
        got = os.path.join(work, name)
        if not fetch(url, got, log):
            return False
        if name.endswith(".deb"):
            ok = _step(_sudo(["apt-get", "install", "-y", got]), log,
                       "the pinned build did not install")
        elif name.endswith(".rpm"):
            ok = _step(_sudo(["dnf", "install", "-y", got]), log,
                       "the pinned build did not install")
        else:
            tree = os.path.join(work, "tree")
            extract(got, tree)
            top = next((root for root, _, files in os.walk(tree)
                        if family[0] in files), None)
            if top is None:
                log("  the fetched archive holds no program by that name")
                return False
            if os.path.basename(top) == "bin":
                top = os.path.dirname(top)
            home = tools_home() / f"{head}-{version}"
            _swap(top, home)
            ok = True
            for program in family:
                real = next(home.rglob(program), None)
                if real is None:
                    ok = False
                    continue
                os.chmod(real, os.stat(real).st_mode | 0o755)
                _link(real, tools_home() / "bin" / program)
    if not ok:
        return False
    from . import toolversion
    return all(not toolversion.older(toolversion.version(p),
                                     version.split("-")[0])
               for p in family)


VENDOR_URL = "https://fastdl.mongodb.org/tools/mongosync/"
#: where each other vendor publishes its builds, by version
VENDOR_URLS = {
    "mysqlsh": "https://dev.mysql.com/get/Downloads/MySQL-Shell/",
    "atlas": "https://release.ariga.io/atlas/",
    "liquibase": "https://github.com/liquibase/liquibase/releases/download/"
                 "v{version}/",
    "riotx": "https://github.com/redis/riotx-dist/releases/download/"
             "v{version}/",
    "redis-shake": "https://github.com/tair-opensource/RedisShake/releases/"
                   "download/v{version}/",
    "dsbulk": "https://github.com/datastax/dsbulk/releases/download/"
              "{version}/",
    "connect-mirror-maker.sh": "https://archive.apache.org/dist/kafka/"
                               "{version}/",
}
#: builds that are not asked their version: the file of their own tree
#: whose name carries it proves the build. The cluster mirror's start
#: script takes only its properties file; its jars are named by release
VERSION_FILES = {"connect-mirror-maker.sh":
                 "libs/connect-mirror-{version}.jar"}
#: builds whose program sits at the top of the archive, not under `bin`:
#: the replica-protocol copy's release is its binary and its sample
#: configuration side by side. It takes only a configuration file, so it
#: is asked nothing either: the archive fetched by its versioned name is
#: the build
AT_TOP = ("redis-shake",)


def _vendor_url(program, version):
    if program == "mongosync":
        return VENDOR_URL
    return VENDOR_URLS[program].format(version=version)


#: the machine files are chosen for where it is not this one: a bundle
#: built for another (`MIGKIT_BUNDLE_PLATFORM`, `build_bundle`)
_TARGET = {}


def _machine():
    system = _TARGET.get("system") or platform.system()
    machine = (_TARGET.get("machine") or platform.machine()).lower()
    arm = machine in ("arm64", "aarch64")
    x86 = machine in ("x86_64", "amd64")
    return system, arm, x86


def _os_release():
    """{ID, VERSION_ID, VERSION_CODENAME} of this Linux (or the one a
    bundle is built for); {} where there is none to read."""
    if "release" in _TARGET:
        return dict(_TARGET["release"])
    release = {}
    try:
        for line in open("/etc/os-release"):
            k, _, v = line.strip().partition("=")
            release[k] = v.strip('"')
    except OSError:
        return {}
    return release


def _glibc_at_least(major, minor):
    if _TARGET:
        # a bundle's Linux is one the vendors build for: glibc 2.28 or later
        return _TARGET.get("system") == "Linux"
    libc, ver = platform.libc_ver()
    try:
        return libc == "glibc" and tuple(
            int(x) for x in ver.split(".")[:2]) >= (major, minor)
    except ValueError:
        return False


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
    if program == "mysqlsh":
        # measured by asking the vendor for each (2026-09-28): one build
        # per architecture for macOS 15, and for glibc 2.28 and later;
        # kept whole, its libraries beside it (`install_vendor`)
        if system == "Darwin":
            return (f"mysql-shell-{version}-macos15-"
                    f"{'arm64' if arm else 'x86-64bit'}.tar.gz")
        if system != "Linux" or not (arm or x86) \
                or not _glibc_at_least(2, 28):
            return None
        return (f"mysql-shell-{version}-linux-glibc2.28-"
                f"{'arm-64bit' if arm else 'x86-64bit'}.tar.gz")
    if program == "riotx":
        # built with its own runtime, per system
        name = {"Darwin": "osx", "Linux": "linux"}.get(system)
        arch = "aarch64" if arm else "x86_64" if x86 else None
        if not (name and arch):
            return None
        return f"riotx-standalone-{version}-{name}-{arch}.zip"
    if program == "redis-shake":
        # one static binary per system and architecture
        name = {"Darwin": "darwin", "Linux": "linux"}.get(system)
        arch = "arm64" if arm else "amd64" if x86 else None
        if not (name and arch):
            return None
        return f"redis-shake-v{version}-{name}-{arch}.tar.gz"
    if program == "dsbulk":
        # Java, the same archive everywhere
        return f"dsbulk-{version}.tar.gz"
    if program == "connect-mirror-maker.sh":
        # the Kafka release itself, built for Scala 2.13, on Java 17
        return f"kafka_2.13-{version}.tgz"
    if system == "Darwin":
        arch = "arm-arm64" if arm else "x86_64"
        return f"mongosync-macos-{arch}-{version}.zip"
    if system != "Linux" or not x86:
        return None
    release = _os_release()
    if not release:
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
    """Fetch `program` from its vendor - held to the lock before anything
    of it is unpacked (`fetch`) - and put it beside migkit's own programs.
    True once the installed copy says the build asked for.

    Every build is kept whole under `tools_home()`, by name and version -
    a single file, or an archive's program with the files it needs beside
    it (a Java program and its libraries, a runtime of its own) - put in
    place in one step (`_swap`), and linked into `into`: `tools_home()/bin`,
    first on migkit's path and this user's, so nothing is written where
    only root may. A Java program is started through a script there that
    gives it its Java."""
    import os
    import tempfile
    name = (vendor_file(program, open_build=True) if open_build
            else vendor_file(program))
    if not name:
        log("  its vendor publishes no build for this machine; install it"
            " by hand")
        return False
    version = TERMS[program].open_build if open_build else VENDOR[program]
    into = Path(into) if into else tools_home() / "bin"
    into.mkdir(parents=True, exist_ok=True)
    target = into / program
    home = tools_home() / f"{program}-{version}"
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, name)
        if not fetch(_vendor_url(program, version) + name, archive, log):
            return False
        tree = os.path.join(work, "tree")
        if not name.endswith((".zip", ".tgz", ".tar.gz")):
            os.makedirs(tree)
            found = os.path.join(tree, program)
            shutil.copyfile(archive, found)
        else:
            extract(archive, tree)
            # the shallowest: an archive whose program is at its top (the
            # Java ones) may carry another of the name further in
            hits = sorted((os.path.join(root, program)
                           for root, _, files in os.walk(tree)
                           if program in files and (root.endswith("bin")
                                                    or program in NEEDS_JAVA
                                                    or program in AT_TOP)),
                          key=lambda p: p.count(os.sep))
            if not hits:
                log("  the fetched archive holds no program by that name")
                return False
            found = hits[0]
        top = os.path.dirname(found)
        if os.path.basename(top) == "bin" and top != tree:
            top = os.path.dirname(top)
        # its libraries stay beside it, and what its archive does not
        # carry goes in with them
        for extra, base, folder, _ in ([] if open_build else
                                       VENDOR_EXTRAS.get(program, [])):
            os.makedirs(os.path.join(top, folder), exist_ok=True)
            if not fetch(base + extra, os.path.join(top, folder, extra),
                         log):
                log("  could not fetch what it needs beside it")
                return False
        rel = os.path.relpath(found, top)
        _swap(top, home)
    real = home / rel
    os.chmod(real, os.stat(real).st_mode | 0o755)
    if program in NEEDS_JAVA:
        # started with the Java found here, unless the operator names one
        # of their own
        java = _java_home() or ""
        _write_executable(target, "#!/bin/sh\n"
                          f"[ -n \"$JAVA_HOME\" ] || JAVA_HOME='{java}'"
                          "\nexport JAVA_HOME\n"
                          f"exec '{real}' \"$@\"\n")
    else:
        _link(real, target)
    if program in VERSION_FILES:
        return (home / VERSION_FILES[program].format(version=version)
                ).exists() and os.access(target, os.X_OK)
    if program in AT_TOP:
        return os.access(target, os.X_OK)
    from . import runner
    got = runner.run([str(target), *VERSION_ARGS.get(program,
                                                     ["--version"])],
                     check=False, timeout=120, check_flags=False,
                     what="the version")
    return version in (got.stdout + got.stderr)


def _link(real, target):
    """`target` made a link to `real` in one step."""
    import os
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".new")
    if os.path.lexists(tmp):
        os.unlink(tmp)
    os.symlink(real, tmp)
    os.replace(tmp, target)


def _write_executable(target, text):
    """`text` as the executable `target`, in one step."""
    import os
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".new")
    tmp.write_text(text)
    os.chmod(tmp, 0o755)
    os.replace(tmp, target)


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



def _java_home():
    """A Java for a Java program fetched as an archive: the operator's own
    `JAVA_HOME`, the system's, or the one a package manager keeps off the
    path; None where there is none."""
    import os
    cands = [os.environ.get("JAVA_HOME", "")]
    if platform.system() == "Darwin":
        from . import runner
        try:
            cands.append(runner.run(
                ["/usr/libexec/java_home", "-v", "17+"], check=False,
                timeout=20, check_flags=False).stdout.strip())
        except Exception:  # noqa: BLE001 - no Java to name
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

    from . import toolversion
    try:
        real = os.path.realpath(path)
        stamp = os.stat(real).st_mtime_ns
    except OSError:
        return None
    key = (program, real, stamp)
    if key not in _BUILDS:
        # asked once per build, as every version is (`toolversion`)
        said = toolversion._said(real, stamp, tuple(
            VERSION_ARGS.get(program, ["--version"]))) or ""
        if program == "atlas":
            # measured: the Community build says `atlas community version
            # v1.3.0`, the default one `atlas version v1.2.4-...`
            _BUILDS[key] = ("open" if "community version" in said
                            else "full" if "version" in said else None)
        else:
            m = re.search(PROGRAM[program].version_pattern, said)
            _BUILDS[key] = (None if not m else
                            "open" if int(m.group(1).split(".")[0]) < 5
                            else "full")
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


#: the second reading's packages, each with the digests of its files
#: (`uv pip compile --generate-hashes`), so the installer takes nothing
#: else (`--require-hashes`)
SECOND_READER_LOCK = Path(__file__).parent / "data" / "second-reader.txt"


def install_second_reader(log=print):
    """Build the second reader its own environment, beside migkit's.

    Its own, because its dependencies would break migkit's (measured on
    8.9.3: numpy 1.26.4, pandas 2.3.3 and sqlglot 19.9 against migkit's
    2.5, 3.0 and 30.18), at the version measured to write nothing to
    either side. On Python 3.12 where the package manager can fetch one:
    numpy 1.26.4 has no wheels past it, so a newer interpreter would
    compile it. Measured with a warm cache: 3.9 s and 422 MB. Every
    package held to its digest where the lock of them is here
    (`SECOND_READER_LOCK`), and taken from the offline bundle with no
    index at all where one is named. Said in migkit's words; the package
    is nobody's business but migkit's.
    """
    import sys
    home = Path.home() / ".migkit" / "second-reader"
    log("installing the independent second reading in its own environment"
        " (a large download, once) ...")
    what = (["-r", str(SECOND_READER_LOCK), "--require-hashes"]
            if SECOND_READER_LOCK.exists() else [SECOND_READER_PACKAGE])
    bundle = _bundle()
    if bundle is not None:
        what += ["--no-index", "--find-links",
                 str(bundle.root / "second-reader")]
    uv = which("uv")
    failed = "the second reading did not install"
    quiet = (lambda m: None)
    if uv:
        made = _step([uv, "venv", "-q", "--python", "3.12", str(home)],
                     quiet, failed) and _step(
            [uv, "pip", "install", "-q", "--python",
             str(home / "bin" / "python"), *what], quiet, failed)
    else:
        made = _step([sys.executable, "-m", "venv", str(home)], quiet,
                     failed) and _step(
            [str(home / "bin" / "pip"), "install", "-q", *what], quiet,
            failed)
    if not made:
        log("  the second reading did not install; everything else still"
            " works, and migkit doctor --install tries again")
        return False
    return True


#: what the redo-file reader is built from, each at the version migkit
#: speaks to: its source, the JSON headers it compiles against (with the
#: fix its own build applies), and Oracle's client for its connection
REDO_READER_BUILD = {
    "version": "2.0.0",
    "source": "https://github.com/bersler/OpenLogReplicator/archive/refs/"
              "tags/v{version}.tar.gz",
    "rapidjson": "https://github.com/Tencent/rapidjson/archive/refs/tags/"
                 "v1.1.0.tar.gz",
    "rapidjson_fix": "https://github.com/Tencent/rapidjson/commit/"
                     "3b2441b87f99ab65f37b141a7b548ebadb607b96.diff",
    "client": "https://download.oracle.com/otn_software/linux/instantclient/"
              "2326000/instantclient-{part}-linux.{arch}-23.26.0.0.0.zip",
}


def redo_reader_files(arch="x64"):
    """{the name the build script reads it by: its url}: every file the
    redo-file reader's build is made from, for `arch` (Oracle's `x64` or
    `arm64`)."""
    b = REDO_READER_BUILD
    return {"source.tgz": b["source"].format(version=b["version"]),
            "rapidjson.tgz": b["rapidjson"],
            "fix.diff": b["rapidjson_fix"],
            "ic-basic.zip": b["client"].format(part="basic", arch=arch),
            "ic-sdk.zip": b["client"].format(part="sdk", arch=arch)}


#: the build, as the program's own container build does it, into
#: `tools_home()`: the packages it compiles with from the system's
#: package manager (Protocol Buffers for its network writer among them),
#: the rest fetched by migkit and held to the lock before this runs - the
#: script reads each from its work directory and fetches nothing
REDO_READER_SCRIPT = r"""set -eu
home="$1"; work="$2"; version="$3"
sudo=""; [ "$(id -u)" != 0 ] && sudo=sudo
if command -v apt-get >/dev/null; then
  $sudo apt-get update -qq
  $sudo apt-get install -y -qq g++ make cmake unzip patch libz-dev \
    libprotobuf-dev protobuf-compiler libaio1t64 >/dev/null 2>&1 \
  || $sudo apt-get install -y -qq g++ make cmake unzip patch libz-dev \
    libprotobuf-dev protobuf-compiler libaio1 >/dev/null
elif command -v dnf >/dev/null; then
  $sudo dnf install -y -q gcc-c++ make cmake unzip patch zlib-devel \
    protobuf-devel protobuf-compiler libaio
else
  echo "no package manager this build knows (apt, dnf)" >&2; exit 2
fi
cd "$work"
tar xzf rapidjson.tgz && mv rapidjson-1.1.0 rapidjson
(cd rapidjson && patch -p1 < ../fix.diff)
for part in basic sdk; do
  unzip -qo "ic-$part.zip"
done
client="$(ls -d "$work"/instantclient_* | head -1)"
tar xzf source.tgz
cd "OpenLogReplicator-$version/proto"
protoc OraProtoBuf.proto --cpp_out=.
mv OraProtoBuf.pb.cc ../src/common/OraProtoBuf.pb.cpp
mv OraProtoBuf.pb.h ../src/common/OraProtoBuf.pb.h
cd .. && mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_RAPIDJSON="$work/rapidjson" \
  -DWITH_OCI="$client" -DWITH_PROTOBUF=/usr -S .. -B . >/dev/null
cmake --build . --target OpenLogReplicator -j "$(nproc)" >/dev/null
mkdir -p "$home/bin" "$home/libexec" "$home/client"
cp ./OpenLogReplicator "$home/libexec/"
cp -a "$client"/. "$home/client/"
cat > "$home/bin/OpenLogReplicator" <<EOF
#!/bin/sh
LD_LIBRARY_PATH="$home/client\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}" \
  exec "$home/libexec/OpenLogReplicator" "\$@"
EOF
chmod 755 "$home/bin/OpenLogReplicator"
"$home/bin/OpenLogReplicator" --version
"""


def install_redo_reader(log=print, home=None):
    """Build the redo-file reader where it runs - Linux on x86-64, beside
    the redo - under `tools_home()`, and prove the build answers with the
    version migkit speaks to. Every file it is built from is fetched first
    and held to the lock (`redo_reader_files`). Said in migkit's words:
    which program reads the redo is migkit's business."""
    import os
    import tempfile

    from .engines import oracle_redo
    if not oracle_redo.platform_runs_it():
        log("  the redo-file reader runs on Linux x86-64, beside the redo;"
            " run migkit doctor --install on that host")
        return False
    if not terms_accepted(REDO_READER_CLIENT):
        instead = TERMS[REDO_READER_CLIENT].otherwise
        log("  the redo-file reader is built with Oracle's client, whose"
            f" terms were not accepted, so {instead}")
        return False
    home = home or oracle_redo.home()
    version = REDO_READER_BUILD["version"]
    log("building the redo-file reader from source (once; several"
        " minutes) ...")
    _, arm, _ = _machine()
    with tempfile.TemporaryDirectory() as work:
        for local, url in redo_reader_files("arm64" if arm else "x64") \
                .items():
            if not fetch(url, os.path.join(work, local), log):
                return False
        from . import runner
        built = runner.run(["bash", "-c", REDO_READER_SCRIPT, "build",
                            str(home), work, version], check=False,
                           check_flags=False, what="the build")
    if built.returncode != 0 or version not in built.stdout + built.stderr:
        log("  the redo-file reader did not build; LogMiner still follows"
            " the source, and migkit doctor --install tries again")
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


def _expression(expr):
    """The licences an SPDX expression names: `A OR B` is either, at the
    user's choice; `A AND B` is both."""
    import re
    alts = [a.strip("() ") for a in re.split(r"\s+OR\s+", expr)]
    if len(alts) > 1:
        return [next((a for a in alts if a in AGPL_COMPATIBLE), alts[0])]
    return [p.strip("() ") for p in re.split(r"\s+AND\s+", expr)]


def _licences_of(dist):
    import re
    meta = dist.metadata
    expr = meta.get("License-Expression")
    raw = (meta.get("License") or "").strip()
    if not expr and re.fullmatch(r"[\w.+-]+(\s+OR\s+[\w.+-]+)+", raw):
        # an SPDX expression of alternatives written in the free-text
        # field (simplejson's `MIT OR AFL-2.1`)
        expr = raw
    if expr:
        return _expression(expr)
    found = [spdx_of(c.split(" :: ")[-1])
             for c in meta.get_all("Classifier") or []
             if c.startswith("License ::") and "OSI Approved" != c.split(
                 " :: ")[-1]]
    # the free-text field says what the classifiers do not; where they say
    # it, the field is often an author's line (`Copyright (c) ...`) or
    # `Dual License`, which names no licence
    field = spdx_of((meta.get("License") or "").strip())
    if re.fullmatch(r"[A-Za-z0-9.+-]+(\s+(AND|OR)\s+[A-Za-z0-9.+-]+)+",
                    field):
        # an SPDX expression written in the free-text field (aiohttp's
        # `Apache-2.0 AND MIT`), read as the expression it is
        found += _expression(field)
    elif field and len(field) < 80 and "\n" not in field and (
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


# ---- fetching, held to the lock ---------------------------------------------------

#: the file under `tools_home()` keeping the digest each file a vendor
#: publishes none for had the first time this machine fetched it
PINS = "pins.json"
#: seconds a fetch may go without a byte
FETCH_TIMEOUT = 120
_PINS_LOCK = threading.Lock()


def _digest(path, algo="sha256"):
    import hashlib
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return f"{algo}:{h.hexdigest()}"


def _pins():
    import json
    try:
        return json.loads((tools_home() / PINS).read_text())
    except (OSError, ValueError):
        return {}


def _pin(url, digest):
    from .durable import write_json
    with _PINS_LOCK:
        got = _pins()
        got[url] = digest
        write_json(tools_home() / PINS, dict(sorted(got.items())),
                   indent=1)


def fetch(url, dest, log=print):
    """The file at `url` into `dest`, held to its digest before anything
    else reads it. True once `dest` holds exactly the file pinned; False,
    said, and nothing at `dest` otherwise.

    The digest is the lock's (`pinned`); a url the lock does not know is
    not fetched at all. Where the vendor publishes no digest (the lock's
    is None) the one this machine kept the first time it fetched the file
    holds (`PINS`), and the first fetch is said. From the offline bundle
    (`MIGKIT_BUNDLE`) the file is its copy, held to the bundle's sums as
    well, and the network is never asked."""
    import os
    import urllib.request
    entry = pinned(url)
    if not entry:
        log("  it is not pinned in migkit's lock of what it fetches, so it"
            " is not fetched")
        return False
    want, size = entry.get("digest"), entry.get("size")
    tmp = f"{dest}.part"
    bundle = _bundle()
    try:
        if bundle is not None:
            if not bundle.copy(url, tmp):
                log("  the offline bundle holds no copy of it")
                return False
        else:
            from . import __version__
            req = urllib.request.Request(
                url, headers={"User-Agent": f"migkit/{__version__}"})
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r, \
                    open(tmp, "wb") as out:
                shutil.copyfileobj(r, out, 1 << 20)
    except (OSError, ValueError) as e:
        log(f"  could not fetch it ({type(e).__name__})")
        _gone(tmp)
        return False
    if size and os.path.getsize(tmp) != int(size):
        log("  the file fetched is not the one pinned (its size differs);"
            " nothing of it is used")
        _gone(tmp)
        return False
    if want:
        got = _digest(tmp, want.split(":", 1)[0])
        if got != want:
            log("  the file fetched is not the one pinned (its digest"
                " differs); nothing of it is used")
            _gone(tmp)
            return False
    else:
        got = _digest(tmp)
        kept = _pins().get(url)
        if kept and kept != got:
            log("  the file fetched is not the one fetched here before (its"
                " digest differs); nothing of it is used")
            _gone(tmp)
            return False
        if not kept:
            _pin(url, got)
            log("  its vendor publishes no digest of it: the one fetched"
                " now is kept, and every later fetch here is held to it")
    os.replace(tmp, dest)
    return True


def _gone(path):
    import os
    try:
        os.unlink(path)
    except OSError:
        pass


def extract(archive, into):
    """An archive's files under `into`, none of them outside it: a `.zip`
    with the modes its entries carry (the program's own executable bit
    among them), a tar with the standard library's data filter - or, on
    an interpreter older than 3.10.12 that has none, with every member
    whose path or link would leave `into` refused."""
    import os
    import tarfile
    import zipfile
    into = os.path.realpath(into)
    os.makedirs(into, exist_ok=True)
    if str(archive).endswith(".zip"):
        with zipfile.ZipFile(archive) as box:
            for info in box.infolist():
                if not _inside(into, info.filename):
                    raise OSError(f"an entry of the archive would be"
                                  f" written outside it: {info.filename}")
            box.extractall(into)
            for info in box.infolist():
                mode = (info.external_attr >> 16) & 0o777
                path = os.path.join(into, info.filename)
                if mode and not info.is_dir() and os.path.isfile(path):
                    os.chmod(path, mode)
        return
    with tarfile.open(archive) as box:
        if hasattr(tarfile, "data_filter"):
            box.extractall(into, filter="data")
            return
        members = box.getmembers()
        for m in members:
            link = (os.path.join(os.path.dirname(m.name), m.linkname)
                    if m.issym() else m.linkname if m.islnk() else "")
            if not _inside(into, m.name) or (link and not _inside(into,
                                                                   link)):
                raise OSError("an entry of the archive would be written"
                              f" outside it: {m.name}")
            if m.isdev():
                raise OSError(f"the archive holds a device: {m.name}")
        box.extractall(into, members=members)


def _inside(root, name):
    import os
    if os.path.isabs(name):
        return False
    path = os.path.realpath(os.path.join(root, name))
    return path == root or path.startswith(root + os.sep)


# ---- the offline bundle -------------------------------------------------------

#: the namespace a bundle's sums are signed under (`ssh-keygen -Y`)
BUNDLE_NAMESPACE = "migkit-bundle"


class Bundle:
    """An offline bundle (`build_bundle`) as an install reads it: its sums
    (`SHA256SUMS`), their signature checked against the signers the
    operator names (`MIGKIT_BUNDLE_SIGNERS`, an `allowed_signers` file),
    and each file held to its sum as it is copied out."""

    def __init__(self, root, log=print):
        import json
        self.root = Path(root)
        sums = self.root / "SHA256SUMS"
        try:
            text = sums.read_text()
        except OSError:
            raise SystemExit(f"{root} is not an offline bundle migkit made:"
                             " it has no sums") from None
        self.sums = {}
        for line in text.splitlines():
            digest, _, rel = line.partition("  ")
            if digest and rel:
                self.sums[rel] = digest
        self._check_signature(log)
        index = self.root / "files.json"
        self.files = (json.loads(self._read(index)) if index.exists()
                      else {})

    def _check_signature(self, log):
        import os
        from . import envkeys
        signers = os.path.expanduser(envkeys.text("MIGKIT_BUNDLE_SIGNERS"))
        sig = self.root / "SHA256SUMS.sig"
        if not signers:
            if sig.exists():
                log("the offline bundle's signature is not checked:"
                    " MIGKIT_BUNDLE_SIGNERS names the file of those who may"
                    " sign one")
            return
        if not sig.exists():
            raise SystemExit("the offline bundle is not signed, and"
                             " MIGKIT_BUNDLE_SIGNERS asks for a signed one."
                             " Nothing has been installed from it")
        from . import runner
        found = runner.run(["ssh-keygen", "-Y", "find-principals", "-s",
                            str(sig), "-f", signers], check=False,
                           timeout=60, check_flags=False)
        who = (found.stdout or "").strip().splitlines()
        ok = found.returncode == 0 and who and runner.run(
            ["ssh-keygen", "-Y", "verify", "-f", signers, "-I", who[0],
             "-n", BUNDLE_NAMESPACE, "-s", str(sig)],
            input=(self.root / "SHA256SUMS").read_text(), check=False,
            timeout=60, check_flags=False).returncode == 0
        if not ok:
            raise SystemExit("the offline bundle's sums are not signed by"
                             " anyone MIGKIT_BUNDLE_SIGNERS names. Nothing"
                             " has been installed from it")

    def verified(self, rel):
        """The bundle's file `rel`, once it holds what its sum says; None
        where it has no such file."""
        path = self.root / rel
        want = self.sums.get(rel)
        if want is None or not path.is_file():
            return None
        if _digest(path).split(":", 1)[1] != want:
            raise SystemExit(f"the offline bundle's {rel} is not what its"
                             " sums say. Nothing has been installed from"
                             " it")
        return path

    def _read(self, path):
        rel = str(Path(path).relative_to(self.root))
        got = self.verified(rel)
        if got is None:
            raise SystemExit(f"the offline bundle's {rel} is not in its"
                             " sums")
        return got.read_text()

    def copy(self, url, dest):
        """The bundle's copy of the file at `url` into `dest`; False where
        it holds none."""
        rel = self.files.get(url)
        got = self.verified(rel) if rel else None
        if got is None:
            return False
        shutil.copyfile(got, dest)
        return True


_BUNDLES = {}


def _bundle(log=print):
    """The offline bundle `MIGKIT_BUNDLE` names, read once; None where it
    names none."""
    import os
    from . import envkeys
    root = os.path.expanduser(envkeys.text("MIGKIT_BUNDLE"))
    if not root:
        return None
    if root not in _BUNDLES:
        _BUNDLES[root] = Bundle(root, log)
    return _BUNDLES[root]


def _load_images(answer, log):
    """The bundle's images put into docker - each held to its sum first -
    and kept as the image its name runs as here (`image`): by its id,
    which is the digest of what was loaded."""
    import json
    bundle = _bundle()
    listed = bundle.root / "images.json"
    if not listed.exists():
        return
    from . import runner
    for name, rec in json.loads(bundle._read(listed)).items():
        if name in TERMS and not answer.get(name):
            continue
        path = bundle.verified(rec["file"])
        if path is None:
            continue
        loaded = runner.run(["docker", "load", "-i", str(path)],
                            check=False, timeout=1800, check_flags=False)
        if loaded.returncode != 0:
            log("  an image of the offline bundle did not load")
            continue
        got = runner.run(["docker", "image", "inspect", "--format",
                          "{{.Id}}", rec["id"]], check=False, timeout=60,
                         check_flags=False)
        if got.returncode == 0 and got.stdout.strip() == rec["id"]:
            _loaded(name, rec["ref"], rec["id"])


#: the images loaded from an offline bundle, under `tools_home()`: {name:
#: {"ref", "id"}}
LOADED = "images.json"


def _loaded(name, ref, image_id):
    import json
    path = tools_home() / LOADED
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        got = {}
    got[name] = {"ref": ref, "id": image_id}
    from .durable import write_json
    write_json(path, got, indent=1)


def _platform(text):
    """{system, machine, release} of a bundle's machine as
    `MIGKIT_BUNDLE_PLATFORM` names it: `linux-x86_64`, `darwin-arm64`, or
    with its Linux, `linux-x86_64-ubuntu-22.04`."""
    parts = str(text).strip().lower().split("-")
    if len(parts) < 2 or parts[0] not in ("linux", "darwin"):
        raise SystemExit(f"MIGKIT_BUNDLE_PLATFORM={text} names no machine"
                         " migkit builds bundles for: linux-x86_64,"
                         " linux-arm64, darwin-arm64, darwin-x86_64, a Linux"
                         " named after them (linux-x86_64-ubuntu-22.04)")
    out = {"system": "Linux" if parts[0] == "linux" else "Darwin",
           "machine": parts[1]}
    if len(parts) >= 4:
        ident, ver = parts[2], parts[3]
        out["release"] = {"ID": ident, "VERSION_ID": ver,
                          "VERSION_CODENAME": _CODENAMES.get((ident, ver),
                                                             "")}
    elif out["system"] == "Linux":
        out["release"] = {}
    return out


def _wheel_tags(target):
    """The wheel platforms pip is asked for, for a bundle built for another
    machine: [] for this one."""
    if not target:
        return []
    arch = "aarch64" if target["machine"] in ("arm64", "aarch64") \
        else "x86_64"
    if target["system"] == "Linux":
        return [f"manylinux2014_{arch}", f"manylinux_2_17_{arch}",
                f"manylinux_2_28_{arch}", f"linux_{arch}"]
    mac = "arm64" if arch == "aarch64" else "x86_64"
    return [f"macosx_11_0_{mac}", f"macosx_12_0_{mac}",
            f"macosx_13_0_{mac}", f"macosx_14_0_{mac}"]


def _pip():
    """How pip is run here: this interpreter's, or through uv; None."""
    import importlib.util
    import sys
    if importlib.util.find_spec("pip") is not None:
        return [sys.executable, "-m", "pip"]
    if shutil.which("uvx"):
        return ["uvx", "pip"]
    return None


def _requirements():
    """What migkit needs installed beside it, as its own metadata says -
    the extras left out."""
    import importlib.metadata as md
    from packaging.requirements import Requirement
    out = []
    for raw in md.requires("migkit") or []:
        req = Requirement(raw)
        if req.marker and not req.marker.evaluate({"extra": ""}):
            continue
        out.append(str(req))
    return out


def build_bundle(where, log=print, ask=None):
    """An offline bundle in the directory `where`: every file `doctor
    --install` would fetch for the bundle's machine (`MIGKIT_BUNDLE_PLATFORM`,
    this one's unless named), each held to the lock as it is fetched; the
    snapshot reader built here, where it is (Java runs it anywhere, and
    its build fetches as it goes); migkit's wheels and its dependencies',
    and the second reading's; the images, saved from docker - then the
    sums of all of it (`SHA256SUMS`), signed with the SSH key
    `MIGKIT_BUNDLE_KEY` names (`ssh-keygen -Y sign`). The terms of what is
    not open source are asked as an install asks them, and what they do
    not cover is left out. Returns the directory."""
    import json
    out = Path(where).expanduser().resolve()
    (out / "files").mkdir(parents=True, exist_ok=True)
    from . import envkeys
    named = envkeys.text("MIGKIT_BUNDLE_PLATFORM")
    target = _platform(named) if named else None
    log(f"building an offline bundle in {out} ...")
    wanted = []
    for _, _, needs, optional in _capabilities():
        wanted += needs + optional
    wanted = list(dict.fromkeys(wanted))
    vendor = [c for c in wanted if c in VENDOR]
    answer = accept_terms(vendor + [i for i in IMAGES if i in TERMS]
                          + [REDO_READER_CLIENT], ask, log)
    urls = {}
    before = dict(_TARGET)
    _TARGET.clear()
    _TARGET.update(target or {})
    try:
        for cmd in vendor:
            open_build = cmd in TERMS and not answer.get(cmd)
            if open_build and not TERMS[cmd].open_build:
                continue
            version = (TERMS[cmd].open_build if open_build
                       else VENDOR[cmd])
            name = vendor_file(cmd, open_build=open_build)
            if name:
                urls[_vendor_url(cmd, version) + name] = name
            for extra, base, _, _ in ([] if open_build else
                                      VENDOR_EXTRAS.get(cmd, [])):
                urls[base + extra] = extra
        for head, (_, version, base) in PACKAGE_BUILDS.items():
            name = package_file(head)
            if name:
                urls[base.format(version=version) + name] = name
        for program, (revision, url) in SOURCE_BUILDS.items():
            if program != "reindex-from-snapshot":
                urls[url] = f"{program}-{revision}.tar.gz"
        system, arm, x86 = _machine()
        if system == "Linux" and x86 and answer.get(REDO_READER_CLIENT):
            for local, url in redo_reader_files("x64").items():
                urls[url] = f"redo-reader-{local}"
    finally:
        _TARGET.clear()
        _TARGET.update(before)
    files = {}
    for n, (url, name) in enumerate(sorted(urls.items()), 1):
        log(f"fetching file {n} of {len(urls)} ...")
        if fetch(url, out / "files" / name, log):
            files[url] = f"files/{name}"
    (out / "files.json").write_text(json.dumps(files, indent=1,
                                               sort_keys=True))
    _bundle_build(out, log)
    _bundle_wheels(out, target, log)
    _bundle_images(out, answer, log)
    _sign_bundle(out, log)
    log(f"the offline bundle is in {out}. On the machine with no network:"
        f" pip install --no-index --find-links {out}/wheels migkit, then"
        f" MIGKIT_BUNDLE={out} migkit doctor --install")
    return out


def _bundle_build(out, log):
    """The snapshot reader as built here, where it is: its tree, for an
    install from the bundle to put in place."""
    import tarfile
    home = tools_home() / "reindex-from-snapshot"
    if not (home / "bin" / RFS_SCRIPT).exists():
        return
    (out / "builds").mkdir(exist_ok=True)
    with tarfile.open(out / "builds" / "reindex-from-snapshot.tar.gz",
                      "w:gz") as tar:
        tar.add(home, arcname="reindex-from-snapshot")


def _unbundle_build(name, home, log):
    """A build the bundle carries, put in place from it."""
    import tempfile
    bundle = _bundle()
    got = bundle.verified(f"builds/{name}.tar.gz")
    if got is None:
        log("  the offline bundle carries no build of it")
        return False
    with tempfile.TemporaryDirectory() as work:
        extract(got, work)
        _swap(Path(work) / name, home)
    return built_program(name) is not None


def _bundle_wheels(out, target, log):
    """migkit's wheel, its dependencies' and the second reading's, for the
    bundle's machine."""
    from .util import _BASE
    pip = _pip()
    if pip is None:
        log("  no pip here, nor uv to run one: the bundle carries no"
            " wheels")
        return
    wheels = out / "wheels"
    wheels.mkdir(exist_ok=True)
    tags = [a for t in _wheel_tags(target) for a in ("--platform", t)]
    pinned_to = (["--only-binary=:all:", "--python-version", "3.12",
                  "--implementation", "cp", *tags] if tags else [])
    quiet = (lambda m: None)
    if (_BASE / "pyproject.toml").exists():
        ok = _step([*pip, "wheel", "--no-deps", "-w", str(wheels),
                    str(_BASE)], quiet, "migkit's own wheel was not built")
    else:
        from . import __version__
        ok = _step([*pip, "download", "--no-deps", "-d", str(wheels),
                    f"migkit=={__version__}", *pinned_to], quiet,
                   "migkit's own wheel was not fetched")
    deps = ((["-r", str(REQUIREMENTS_LOCK), "--require-hashes"]
             if REQUIREMENTS_LOCK.exists() else _requirements()))
    ok = _step([*pip, "download", "-d", str(wheels), *pinned_to, *deps],
               log, "its dependencies' wheels were not all fetched") and ok
    reader = out / "second-reader"
    reader.mkdir(exist_ok=True)
    what = (["-r", str(SECOND_READER_LOCK), "--require-hashes"]
            if SECOND_READER_LOCK.exists() else [SECOND_READER_PACKAGE])
    ok = _step([*pip, "download", "-d", str(reader), "--only-binary=:all:",
                "--python-version", "3.12", "--implementation", "cp",
                *tags, *what], log,
               "the second reading's wheels were not all fetched") and ok
    if not ok:
        log("  the bundle is short of wheels: the machine it is for"
            " installs what it lacks from an index, or not at all")


def _bundle_images(out, answer, log):
    """Each image migkit runs, pulled by its digest and saved into the
    bundle, with the id it loads as - the ones whose terms were declined
    left out."""
    import json

    from . import runner
    if not shutil.which("docker"):
        log("  no docker here: the bundle carries no images")
        return
    (out / "images").mkdir(exist_ok=True)
    listed = {}
    for name, ref in sorted(IMAGES.items()):
        if name in TERMS and not answer.get(name):
            continue
        pulled = runner.run(["docker", "pull", ref], check=False,
                            timeout=3600, check_flags=False)
        got = runner.run(["docker", "image", "inspect", "--format",
                          "{{.Id}}", ref], check=False, timeout=60,
                         check_flags=False)
        if pulled.returncode != 0 or got.returncode != 0:
            log("  an image could not be pulled; the bundle is without it")
            continue
        image_id = got.stdout.strip()
        rel = f"images/{name}.tar"
        saved = runner.run(["docker", "save", "-o", str(out / rel),
                            image_id], check=False, timeout=3600,
                           check_flags=False)
        if saved.returncode != 0:
            log("  an image could not be saved; the bundle is without it")
            continue
        listed[name] = {"ref": ref, "id": image_id, "file": rel}
    (out / "images.json").write_text(json.dumps(listed, indent=1,
                                                sort_keys=True))


def _sign_bundle(out, log):
    """`SHA256SUMS` over every file of the bundle, and its signature where
    `MIGKIT_BUNDLE_KEY` names the key to sign it with."""
    import os
    rows = []
    for path in sorted(p for p in out.rglob("*") if p.is_file()):
        rel = str(path.relative_to(out))
        if rel in ("SHA256SUMS", "SHA256SUMS.sig"):
            continue
        rows.append(f"{_digest(path).split(':', 1)[1]}  {rel}")
    (out / "SHA256SUMS").write_text("\n".join(rows) + "\n")
    (out / "SHA256SUMS.sig").unlink(missing_ok=True)
    from . import envkeys
    key = envkeys.text("MIGKIT_BUNDLE_KEY")
    if not key:
        log("  the bundle is not signed: MIGKIT_BUNDLE_KEY names the SSH key"
            " to sign its sums with")
        return
    from .export import _sign
    try:
        _sign(out / "SHA256SUMS", os.path.expanduser(key), BUNDLE_NAMESPACE)
    except Exception as e:  # noqa: BLE001 - said; the bundle stands
        log(f"  the bundle could not be signed ({str(e)[:160]})")


#: the container images migkit runs, pulled where they are run rather than
#: installed: {name as `PROGRAM_LICENCES` and `TERMS` know it: image}, each
#: by digest. Pulled, not installed, and a closed one's terms accepted with
#: every other's all the same (`install_missing`)
IMAGES = {n: image(n) for n, p in PROGRAM.items() if p.image}
