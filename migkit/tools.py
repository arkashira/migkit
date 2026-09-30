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
    # fetched from their vendors too: the Redis copy by the replica
    # protocol, the Cassandra bulk loader, and Kafka's own cluster mirror
    # (the script of the Kafka release that starts it)
    "redis-shake": ("", ""),
    "dsbulk": ("", ""),
    "connect-mirror-maker.sh": ("", ""),
    # the brew package is an installer that asks for an administrator's
    # password, and no apt repository of the defaults carries it
    "mysqlsh": ("", ""),
    "sqlcmd": ("sqlcmd", ""),
    "docker": ("", ""),
    # built from source where the redo is (`install_redo_reader`), with
    # Oracle's client it connects through
    "OpenLogReplicator": ("", ""),
    "oracle-instant-client": ("", ""),
    # not a program: FreeTDS's ODBC driver and the ODBC manager it loads
    # through, which SAP ASE is reached by (`ASE_DRIVER`)
    "ase-odbc": ("freetds unixodbc", "tdsodbc unixodbc"),
    # not programs: IBM's ODBC driver for IBM i, from IBM's own package
    # repository (`install_ibm`), and the operator's Db2 Connect licence
    # put beside the IBM data server driver
    "ibmi-odbc": ("", ""),
    "db2-connect": ("", ""),
    # not programs either: Teradata's own driver for Python, installed into
    # migkit's environment (`install_teradata`), and the connectors of the
    # program sources' container kind, which docker pulls where a hop
    # runs one (`engines.saas`)
    "teradatasql": ("", ""),
    "airbyte-connectors": ("", ""),
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
    # GPLv2 with the Universal FOSS Exception, driven as a program: its
    # Python API lives only inside its own process (no package carries it)
    "mysqlsh": "GPL-2.0-only",
    # the images migkit pulls (`IMAGES`): its version-matched PostgreSQL
    # copy, and the streaming pipeline's connectors and its log
    "pgcopydb": "PostgreSQL", "debezium": "Apache-2.0",
    "redpanda": "BUSL-1.1",
    "OpenLogReplicator": "AGPL-3.0-or-later",
    "oracle-instant-client": "LicenseRef-Oracle-Free-Use-Terms",
    # FreeTDS under the Library GPL 2 "or any later version", taken as
    # 2.1; the ODBC manager under the LGPL 2.1
    "ase-odbc": "LGPL-2.1-or-later",
    # IBM's, not open source: each is used on the operator's acceptance
    # of IBM's terms (`TERMS`)
    "ibmi-odbc": "LicenseRef-IBM-i-Access-Client-Solutions",
    "db2-connect": "LicenseRef-IBM-Db2-Connect",
    # Teradata's licence for its driver: free to use, not open source
    "teradatasql": "LicenseRef-Teradata-SQL-Driver",
    # the Elastic License 2.0 its certified connectors carry: any use but
    # offering them to others as a managed service
    "airbyte-connectors": "Elastic-2.0",
    "redis-shake": "MIT", "dsbulk": "Apache-2.0",
    "connect-mirror-maker.sh": "Apache-2.0",
    # built here from its source (`SOURCE_BUILDS`), never on PATH by
    # another name
    "reindex-from-snapshot": "Apache-2.0",
}

#: the container images migkit runs, pulled by docker where they are run
#: rather than installed: {name as `PROGRAM_LICENCES` and `TERMS` know it:
#: image}. Pulled, not installed, and a closed one's terms accepted with
#: every other's all the same (`install_missing`)
IMAGES = {"pgcopydb": "dimitri/pgcopydb:latest",
          "debezium": "quay.io/debezium/connect:3.0",
          "redpanda": "redpandadata/redpanda:latest",
          # Kafka's own, which carries the cluster mirror's start script:
          # the build `VENDOR` pins, where the release is not installed
          "connect-mirror-maker.sh": "apache/kafka:3.8.0"}

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
    got = subprocess.run(step, capture_output=True, text=True)
    if got.returncode != 0:
        log("  it did not install: "
            f"{(got.stderr.strip().splitlines() or [''])[-1][:120]}")
        return False
    import importlib
    importlib.invalidate_caches()
    return teradata_driver_present()


def install_ibm(component, log=print):
    """IBM's ODBC driver for IBM i from IBM's own package repository, with
    the ODBC manager it loads through; or the operator's Db2 Connect
    licence file (`MIGKIT_DB2_CONNECT_LICENSE`) copied beside the IBM data
    server driver. Called once the terms are accepted. True once it is in
    place."""
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
        steps = [["sudo", "sh", "-c", f"curl -fsSL {IBMI_APT} >"
                  " /etc/apt/sources.list.d/ibmi-acs-1.1.0.list"],
                 ["sudo", "apt-get", "update"],
                 ["sudo", "apt-get", "install", "-y", "unixodbc",
                  "ibm-iaccess"]]
    elif shutil.which("dnf"):
        steps = [["sudo", "sh", "-c", f"curl -fsSL {IBMI_RPM} >"
                  " /etc/yum.repos.d/ibmi-acs.repo"],
                 ["sudo", "dnf", "install", "-y", "--refresh", "unixODBC",
                  "ibm-iaccess"]]
    else:
        log("  no package manager here that IBM publishes the driver for")
        return False
    for step in steps:
        got = subprocess.run(step, capture_output=True, text=True)
        if got.returncode != 0:
            log("  it did not install: "
                f"{(got.stderr.strip().splitlines() or [''])[-1][:120]}")
            return False
    return ibmi_odbc_present()


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
    """Fetch `name`'s source at the revision measured here and build it
    under `tools_home()`. True once the built program answers."""
    import os
    import tarfile
    import tempfile
    import urllib.request
    revision, url = SOURCE_BUILDS[name]
    home = tools_home() / name
    home.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, "src.tar.gz")
        try:
            urllib.request.urlretrieve(url, archive)
        except OSError as e:
            log(f"  could not fetch it ({type(e).__name__})")
            return False
        with tarfile.open(archive) as box:
            box.extractall(work, filter="data")
        top = next(os.path.join(work, d) for d in os.listdir(work)
                   if os.path.isdir(os.path.join(work, d)))

        def step(cmd, cwd):
            got = subprocess.run(cmd, cwd=cwd, capture_output=True,
                                 text=True)
            if got.returncode != 0:
                log(f"  the build stopped: {(got.stderr.strip().splitlines() or [''])[-1][:120]}")
            return got.returncode == 0
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
    whole under `home`. Java 17 is what its build asks for."""
    import os
    java = _java_home()
    if not java:
        log("  the build needs a Java 17, and none was found")
        return False
    got = subprocess.run(["./gradlew", "--no-daemon", "-q",
                          f":{RFS_SCRIPT}:installDist"], cwd=top,
                         capture_output=True, text=True,
                         env={**os.environ, "JAVA_HOME": java})
    if got.returncode != 0:
        log("  the build stopped: "
            + (got.stderr.strip().splitlines() or [""])[-1][:120])
        return False
    built = os.path.join(top, RFS_SCRIPT, "build", "install", RFS_SCRIPT)
    if not os.path.isdir(built):
        return False
    shutil.rmtree(home, ignore_errors=True)
    shutil.copytree(built, home, symlinks=True)
    return True


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


def install_missing(log=print, ask=None):
    """Install every missing program with the platform package manager, and
    from its vendor what no package manager carries - a program that is not
    open source only once its terms are accepted (`accept_terms`, `ask`
    the question at a terminal), asked in the one question with the terms
    of the images migkit pulls. Returns the capabilities still short
    afterwards."""
    mac = platform.system() == "Darwin"
    mgr = "brew" if (mac and shutil.which("brew")) else (
        "apt" if shutil.which("apt-get") else None)
    if not mgr:
        log("no supported package manager (brew/apt) here; install by hand")
    wanted = []
    for _, _, needs, optional in _capabilities():
        wanted += needs + optional
    pkgs = []
    for cmd in wanted:
        if cmd in (SECOND_READER, COMPILED_MOVER) or cmd in SOURCE_BUILDS \
                or _present(cmd):
            continue
        formula, apt = PROGRAMS.get(cmd, ("", ""))
        pkg = formula if mgr == "brew" else apt
        if mgr and pkg and pkg not in pkgs:
            pkgs.append(pkg)
    # counted, not named: which package provides a capability is migkit's
    # business, and the line used to read `installing mydumper ...`
    for n, pkg in enumerate(pkgs, 1):
        log(f"installing component {n} of {len(pkgs)} ...")
        cmd_line = (["brew", "install", *pkg.split()] if mgr == "brew"
                    else ["sudo", "apt-get", "install", "-y", *pkg.split()])
        p = subprocess.run(cmd_line, capture_output=True, text=True)
        if p.returncode != 0:
            log(f"  component {n} did not install; run migkit doctor again"
                " to see which capability is still short")
    fetch = [c for c in dict.fromkeys(wanted) if c in VENDOR and not which(c)]
    ibm = [c for c in dict.fromkeys(wanted) if c in (IBMI_ODBC, DB2_CONNECT)
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
    answer = accept_terms(fetch + ibm + [i for i in IMAGES if i in TERMS]
                          + ([REDO_READER_CLIENT] if redo else [])
                          + ([TERADATA_DRIVER] if teradata else [])
                          + [SAAS_CONNECTORS], ask, log)
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
    for n, cmd in enumerate([c for c in dict.fromkeys(wanted)
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


#: programs no package manager carries, fetched from their vendor at the
#: build measured here: {program: version}
VENDOR = {"atlas": "1.3.0", "liquibase": "5.0.4", "mysqlsh": "26.7.1",
          "mongosync": "1.21.0", "riotx": "1.15.1", "redis-shake": "4.6.2",
          "dsbulk": "1.11.0", "connect-mirror-maker.sh": "3.8.0"}
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
#: how each asks its build for its version, where not `--version`
VERSION_ARGS = {"atlas": ["version"]}
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
    if program == "mysqlsh":
        # measured by asking the vendor for each (2026-09-28): one build
        # per architecture for macOS 15, and for glibc 2.28 and later;
        # kept whole, its libraries beside it (`install_vendor`)
        if system == "Darwin":
            return (f"mysql-shell-{version}-macos15-"
                    f"{'arm64' if arm else 'x86-64bit'}.tar.gz")
        libc, ver = platform.libc_ver()
        try:
            new_enough = libc == "glibc" and tuple(
                int(x) for x in ver.split(".")[:2]) >= (2, 28)
        except ValueError:
            new_enough = False
        if system != "Linux" or not (arm or x86) or not new_enough:
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
                                                     or program in NEEDS_JAVA
                                                     or program in AT_TOP)),
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
    if program in VERSION_FILES:
        home = _home() / "tools" / f"{program}-{version}"
        return (home / VERSION_FILES[program].format(version=version)
                ).exists() and os.access(target, os.X_OK)
    if program in AT_TOP:
        return os.access(target, os.X_OK)
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
NEEDS_JAVA = {"liquibase": ("openjdk", "openjdk-17-jre-headless"),
              "dsbulk": ("openjdk", "openjdk-17-jre-headless"),
              "connect-mirror-maker.sh": ("openjdk",
                                          "openjdk-17-jre-headless")}


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

    Its own, because its dependencies would break migkit's (measured on
    8.9.3: numpy 1.26.4, pandas 2.3.3 and sqlglot 19.9 against migkit's
    2.5, 3.0 and 30.18), at the version measured to write nothing to
    either side. On Python 3.12 where the package manager can fetch one:
    numpy 1.26.4 has no wheels past it, so a newer interpreter would
    compile it. Measured with a warm cache: 3.9 s and 422 MB. Said in
    migkit's words; the package is nobody's business but migkit's.
    """
    import sys
    home = Path.home() / ".migkit" / "second-reader"
    log("installing the independent second reading in its own environment"
        " (a large download, once) ...")
    uv = which("uv")
    if uv:
        made = subprocess.run([uv, "venv", "-q", "--python", "3.12",
                               str(home)], capture_output=True, text=True)
        if made.returncode == 0:
            made = subprocess.run([uv, "pip", "install", "-q", "--python",
                                   str(home / "bin" / "python"),
                                   SECOND_READER_PACKAGE],
                                  capture_output=True, text=True)
    else:
        made = subprocess.run([sys.executable, "-m", "venv", str(home)],
                              capture_output=True, text=True)
        if made.returncode == 0:
            made = subprocess.run([str(home / "bin" / "pip"), "install",
                                   "-q", SECOND_READER_PACKAGE],
                                  capture_output=True, text=True)
    if made.returncode != 0:
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

#: the build, as the program's own container build does it, into
#: `tools_home()`: the packages it compiles with from the system's
#: package manager (Protocol Buffers for its network writer among them),
#: the rest fetched from each publisher
REDO_READER_SCRIPT = r"""set -eu
home="$1"; work="$2"; version="$3"
mkdir -p "$work"
sudo=""; [ "$(id -u)" != 0 ] && sudo=sudo
if command -v apt-get >/dev/null; then
  $sudo apt-get update -qq
  $sudo apt-get install -y -qq g++ make cmake unzip curl patch libz-dev \
    libprotobuf-dev protobuf-compiler libaio1t64 >/dev/null 2>&1 \
  || $sudo apt-get install -y -qq g++ make cmake unzip curl patch libz-dev \
    libprotobuf-dev protobuf-compiler libaio1 >/dev/null
elif command -v dnf >/dev/null; then
  $sudo dnf install -y -q gcc-c++ make cmake unzip curl patch zlib-devel \
    protobuf-devel protobuf-compiler libaio
else
  echo "no package manager this build knows (apt, dnf)" >&2; exit 2
fi
cd "$work"
curl -fsSL -o rapidjson.tgz "$RAPIDJSON"
tar xzf rapidjson.tgz && mv rapidjson-1.1.0 rapidjson
curl -fsSL -o fix.diff "$RAPIDJSON_FIX" && (cd rapidjson && patch -p1 < ../fix.diff)
case "$(uname -m)" in x86_64|amd64) ic=x64;; aarch64|arm64) ic=arm64;;
  *) echo "no client for $(uname -m)" >&2; exit 2;; esac
for part in basic sdk; do
  curl -fsSL -o "ic-$part.zip" "$(printf "$CLIENT" "$part" "$ic")"
  unzip -qo "ic-$part.zip"
done
client="$(ls -d "$work"/instantclient_* | head -1)"
curl -fsSL -o source.tgz "$SOURCE"
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
    the redo - under `tools_home()`, and prove the build answers
    with the version migkit speaks to. Said in migkit's words: which
    program reads the redo is migkit's business."""
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
    env = dict(os.environ,
               SOURCE=REDO_READER_BUILD["source"].format(version=version),
               RAPIDJSON=REDO_READER_BUILD["rapidjson"],
               RAPIDJSON_FIX=REDO_READER_BUILD["rapidjson_fix"],
               CLIENT=REDO_READER_BUILD["client"].replace(
                   "{part}", "%s").replace("{arch}", "%s"))
    with tempfile.TemporaryDirectory() as work:
        built = subprocess.run(["bash", "-c", REDO_READER_SCRIPT, "build",
                                home, work, version], env=env,
                               capture_output=True, text=True)
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
