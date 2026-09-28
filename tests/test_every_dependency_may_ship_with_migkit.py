"""Every dependency may be combined with migkit, and a program that is not
open source is installed only on the operator's word.

migkit is AGPL-3.0-or-later, so a package it imports must carry a licence
whose code may be combined with it: GPL 2 only may not, a source-available
licence may not. A program it runs beside itself may carry any open
licence. A program that is not open source at all - the default Atlas
build, Liquibase 5, the MongoDB sync, RIOT-X - is registered with its
terms, installed only once they are accepted (asked at the terminal, or
`MIGKIT_ACCEPT_TERMS` for an unattended install), and never silently;
declined, the open build or the open path takes its place and that is
said.
"""
import io
import json
import os
import tarfile
import threading
import tomllib
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from migkit import tools

ROOT = Path(__file__).resolve().parents[1]


def _requirements():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())[
        "project"]["dependencies"]


def test_every_package_migkit_imports_may_be_combined_with_it():
    got = tools.package_licences(_requirements())
    # the scan reaches what it is about: the drivers and what they need
    assert len(got) > 60, len(got)
    for name in ("psycopg2-binary", "pymssql", "pglast", "pyrage",
                 "results", "polars", "certifi"):
        assert name in got, name
    missing = sorted(n for n, lic in got.items() if lic == [None])
    assert not missing, f"not installed here, so unread: {missing}"
    bad = {n: lic for n, lic in got.items()
           if n not in tools.TERMS
           and not all(x in tools.AGPL_COMPATIBLE for x in lic)}
    assert not bad, bad


def test_the_licence_reading_would_notice():
    """A package under GPL 2 only, or source-available, is refused - and
    the spellings metadata uses are read as the licences they are."""
    for refused in ("GPL-2.0-only", "BUSL-1.1", "FSL-1.1-ALv2", "SSPL-1.0",
                    "Dual License", "LicenseRef-Atlas-EULA"):
        assert refused not in tools.AGPL_COMPATIBLE
    assert tools.spdx_of("Apache Software License") == "Apache-2.0"
    assert tools.spdx_of("Apache License, Version 2.0") == "Apache-2.0"
    assert tools.spdx_of("The MIT License (MIT)") == "MIT"
    assert tools.spdx_of("GNU LESSER GENERAL PUBLIC LICENSE") \
        == "LGPL-2.1-or-later"
    assert tools.spdx_of("LGPL with exceptions") == "LGPL-3.0-or-later"
    # a GPL 3 library is combined freely now: pglast, through the schema
    # differ
    assert tools.package_licences(["pglast"])["pglast"] == \
        ["GPL-3.0-or-later"]


def test_every_program_is_open_or_has_its_terms_registered():
    programs = set(tools.PROGRAMS) | set(tools.VENDOR)
    unknown = sorted(p for p in programs if p not in tools.PROGRAM_LICENCES)
    assert not unknown, unknown
    closed = {p for p in programs
              if tools.PROGRAM_LICENCES[p] not in tools.OPEN_LICENCES}
    # exactly the ones whose terms are asked for, no more and no fewer
    assert closed == set(tools.TERMS), (closed, set(tools.TERMS))
    assert tools.SECOND_READER_LICENCE in tools.OPEN_LICENCES
    # what a build needs beside it, fetched with it
    for program, extras in tools.VENDOR_EXTRAS.items():
        for name, _, _, licence in extras:
            assert licence in tools.OPEN_LICENCES, (program, name, licence)
    for program, terms in tools.TERMS.items():
        assert terms.url.startswith("https://"), program
        assert terms.licence and terms.allows and terms.otherwise, program


# ---- the terms, asked -------------------------------------------------------

@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("MIGKIT_ACCEPT_TERMS", raising=False)
    return tmp_path


def _record(home):
    path = home / ".migkit" / tools.TERMS_FILE
    return json.loads(path.read_text()) if path.exists() else {}


def test_nothing_is_accepted_with_nobody_to_ask(home):
    said = []
    got = tools.accept_terms(["atlas", "mongosync", "pg_dump"], None,
                             said.append)
    assert got == {"atlas": False, "mongosync": False}
    assert _record(home) == {}
    # never silent: each names its licence, where to read it, and what
    # takes its place
    text = " ".join(said)
    assert "Atlas EULA" in text and tools.TERMS["atlas"].url in text
    assert "Community build" in text and "open path" in text
    assert "MIGKIT_ACCEPT_TERMS=mongosync" in text


def test_an_unattended_install_accepts_what_it_names(home, monkeypatch):
    monkeypatch.setenv("MIGKIT_ACCEPT_TERMS", "atlas, riotx")
    got = tools.accept_terms(["atlas", "liquibase", "riotx"], None,
                             lambda m: None)
    assert got == {"atlas": True, "liquibase": False, "riotx": True}
    rec = _record(home)
    assert set(rec) == {"atlas", "riotx"}
    assert rec["atlas"]["how"] == "MIGKIT_ACCEPT_TERMS"
    assert rec["atlas"]["licence"] == tools.TERMS["atlas"].licence
    assert rec["atlas"]["by"] and rec["atlas"]["at"]
    monkeypatch.setenv("MIGKIT_ACCEPT_TERMS", "all")
    assert tools.accept_terms(["liquibase"], None, lambda m: None) == \
        {"liquibase": True}


def test_the_question_is_asked_once_and_remembered(home):
    asked = []

    def ask(program, terms):
        asked.append((program, terms.licence))
        return program == "liquibase"
    got = tools.accept_terms(["liquibase", "mongosync"], ask, lambda m: None)
    assert got == {"liquibase": True, "mongosync": False}
    assert [p for p, _ in asked] == ["liquibase", "mongosync"]
    assert "Functional Source License" in asked[0][1]
    # accepted is remembered; declined is asked again next time
    asked.clear()
    tools.accept_terms(["liquibase", "mongosync"], ask, lambda m: None)
    assert [p for p, _ in asked] == ["mongosync"]
    assert tools.terms_accepted("liquibase")


def test_terms_that_changed_are_asked_again(home, monkeypatch):
    tools.accept_terms(["atlas"], lambda p, t: True, lambda m: None)
    assert tools.terms_accepted("atlas")
    changed = tools.Terms("a new EULA", "https://example.com/eula",
                          "x", "y", "1.3.0")
    monkeypatch.setitem(tools.TERMS, "atlas", changed)
    assert not tools.terms_accepted("atlas")


def _install(monkeypatch, home, accept=""):
    """`install_missing` on a machine short of every vendor program, with
    the package manager and the fetch stood in for."""
    monkeypatch.setattr(tools, "which", lambda n: None)
    monkeypatch.setattr(tools.shutil, "which", lambda n: None)
    monkeypatch.setattr(tools, "second_reader_present", lambda: True)
    monkeypatch.setattr(tools, "_java_home", lambda: "/jdk")
    if accept:
        monkeypatch.setenv("MIGKIT_ACCEPT_TERMS", accept)
    fetched = []
    monkeypatch.setattr(tools, "install_vendor",
                        lambda program, log, open_build=False:
                        fetched.append((program, open_build)) or True)
    said = []
    tools.install_missing(said.append)
    return fetched, said


def test_declined_the_open_build_is_installed_or_nothing(home, monkeypatch):
    fetched, said = _install(monkeypatch, home)
    assert ("atlas", True) in fetched and ("liquibase", True) in fetched
    # no open build of these: the open path runs, nothing is fetched
    assert not [p for p, _ in fetched if p in ("mongosync", "riotx")]
    assert _record(home) == {}


def test_accepted_the_build_its_terms_cover_is_installed(home, monkeypatch):
    fetched, _ = _install(monkeypatch, home, accept="all")
    assert sorted(fetched) == sorted((p, False) for p in tools.VENDOR)
    assert set(_record(home)) == set(tools.TERMS)


# ---- which file, from where -------------------------------------------------

@pytest.mark.parametrize("system,machine,program,open_build,want", [
    ("Darwin", "arm64", "atlas", False, "atlas-darwin-arm64-v1.3.0"),
    ("Darwin", "arm64", "atlas", True, "atlas-community-darwin-arm64-v1.3.0"),
    ("Linux", "x86_64", "atlas", True, "atlas-community-linux-amd64-v1.3.0"),
    ("Linux", "x86_64", "liquibase", False, "liquibase-5.0.4.tar.gz"),
    ("Linux", "x86_64", "liquibase", True, "liquibase-4.33.0.tar.gz"),
    ("Darwin", "arm64", "riotx", False,
     "riotx-standalone-1.15.1-osx-aarch64.zip"),
    ("Linux", "x86_64", "riotx", False,
     "riotx-standalone-1.15.1-linux-x86_64.zip"),
])
def test_the_vendor_file_for_this_machine(monkeypatch, system, machine,
                                          program, open_build, want):
    monkeypatch.setattr(tools.platform, "system", lambda: system)
    monkeypatch.setattr(tools.platform, "machine", lambda: machine)
    assert tools.vendor_file(program, open_build=open_build) == want
    version = (tools.TERMS[program].open_build if open_build
               else tools.VENDOR[program])
    assert version in tools._vendor_url(program, version) + want


@pytest.fixture
def vendor(monkeypatch, home):
    """A vendor that serves whatever the test puts in `files`."""
    files = {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            body = files.get(self.path.rsplit("/", 1)[-1])
            self.send_response(200 if body is not None else 404)
            self.end_headers()
            self.wfile.write(body or b"")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}/"
    monkeypatch.setattr(tools, "VENDOR_URLS", {
        p: base + "{version}/" for p in tools.VENDOR_URLS})
    monkeypatch.setattr(tools.platform, "system", lambda: "Linux")
    monkeypatch.setattr(tools.platform, "machine", lambda: "x86_64")
    yield files
    srv.shutdown()
    srv.server_close()


def _script(text):
    return f"#!/bin/sh\n{text}\n".encode()


def test_a_single_file_build_is_copied_in(vendor, tmp_path):
    vendor["atlas-community-linux-amd64-v1.3.0"] = _script(
        'echo "atlas community version v1.3.0"')
    into = tmp_path / "bin"
    into.mkdir()
    assert tools.install_vendor("atlas", print, into=str(into),
                                open_build=True)
    assert os.access(into / "atlas", os.X_OK)


def test_a_java_archive_is_kept_whole_and_started_with_a_java(
        vendor, tmp_path, monkeypatch):
    monkeypatch.setattr(tools, "_java_home", lambda: "/opt/jdk-17")
    body = io.BytesIO()
    with tarfile.open(fileobj=body, mode="w:gz") as tar:
        for name, data, mode in (
                ("liquibase", _script(
                    'echo "JAVA_HOME=$JAVA_HOME"; cat "$(dirname "$0")'
                    '/internal/lib/version"'), 0o755),
                ("internal/lib/version", b"Liquibase Version: 4.33.0\n",
                 0o644)):
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), mode
            tar.addfile(info, io.BytesIO(data))
    vendor["liquibase-4.33.0.tar.gz"] = body.getvalue()
    into = tmp_path / "bin"
    into.mkdir()
    assert tools.install_vendor("liquibase", print, into=str(into),
                                open_build=True)
    kept = tmp_path / ".migkit" / "tools" / "liquibase-4.33.0"
    assert (kept / "internal" / "lib" / "version").exists()
    # the archive itself is not kept beside it
    assert not list(kept.glob("*.tar.gz"))
    import subprocess
    got = subprocess.run([str(into / "liquibase")], capture_output=True,
                         text=True, env={"PATH": os.environ["PATH"]})
    assert "JAVA_HOME=/opt/jdk-17" in got.stdout, got
    assert "4.33.0" in got.stdout


def test_the_build_without_drivers_gets_the_one_migkit_reads_through(
        vendor, tmp_path, monkeypatch):
    """Liquibase 5 ships without JDBC drivers; its PostgreSQL one is
    fetched beside it. The open 4.33 build carries its own."""
    monkeypatch.setattr(tools, "_java_home", lambda: "/opt/jdk-17")
    base = tools.VENDOR_URLS["liquibase"].format(version="x")[:-len("x/")]
    monkeypatch.setattr(tools, "VENDOR_EXTRAS", {"liquibase": [
        ("postgresql-42.7.10.jar", base, "lib", "BSD-2-Clause")]})
    vendor["postgresql-42.7.10.jar"] = b"PK driver"
    for version in ("5.0.4", "4.33.0"):
        body = io.BytesIO()
        with tarfile.open(fileobj=body, mode="w:gz") as tar:
            data = _script(f'echo "Liquibase Version: {version}"')
            info = tarfile.TarInfo("liquibase")
            info.size, info.mode = len(data), 0o755
            tar.addfile(info, io.BytesIO(data))
        vendor[f"liquibase-{version}.tar.gz"] = body.getvalue()
    into = tmp_path / "bin"
    into.mkdir()
    assert tools.install_vendor("liquibase", print, into=str(into))
    full = tmp_path / ".migkit" / "tools" / "liquibase-5.0.4"
    assert (full / "lib" / "postgresql-42.7.10.jar").read_bytes() == \
        b"PK driver"
    assert tools.install_vendor("liquibase", print, into=str(into),
                                open_build=True)
    assert not (tmp_path / ".migkit" / "tools" / "liquibase-4.33.0" / "lib"
                / "postgresql-42.7.10.jar").exists()


def test_a_build_with_its_own_runtime_is_linked_in(vendor, tmp_path):
    body = io.BytesIO()
    top = "riotx-standalone-1.15.1-linux-x86_64"
    with zipfile.ZipFile(body, "w") as z:
        info = zipfile.ZipInfo(f"{top}/bin/riotx")
        info.external_attr = 0o755 << 16
        z.writestr(info, _script('echo "riotx 1.15.1"'))
        z.writestr(f"{top}/lib/modules", b"runtime")
    vendor["riotx-standalone-1.15.1-linux-x86_64.zip"] = body.getvalue()
    into = tmp_path / "bin"
    into.mkdir()
    assert tools.install_vendor("riotx", print, into=str(into))
    link = into / "riotx"
    assert link.is_symlink()
    assert (Path(os.path.realpath(link)).parent.parent / "lib"
            / "modules").exists()


def test_a_build_that_says_another_version_is_not_taken(vendor, tmp_path):
    vendor["atlas-linux-amd64-v1.3.0"] = _script('echo "atlas version v9.9.9"')
    into = tmp_path / "bin"
    into.mkdir()
    assert not tools.install_vendor("atlas", print, into=str(into))


# ---- what the open build leaves to migkit ----------------------------------

def test_the_open_schema_reading_is_said_without_a_name(tmp_path,
                                                        monkeypatch):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    stub = tmp_path / "atlas"
    stub.write_text('#!/bin/sh\necho "atlas community version v1.3.0"\n')
    stub.chmod(0o755)
    monkeypatch.setattr(tools, "which", lambda n: str(stub))
    tools._BUILDS.clear()
    assert tools.build_of("atlas") == "open"
    note = tools.schema_reading_note()
    assert "views, functions, procedures and triggers" in note
    assert not [t for t in TOOLS if t in note.lower()], note
    stub.write_text('#!/bin/sh\necho "atlas version v1.2.4-9b670a5"\n')
    os.utime(stub, ns=(1, 1))
    assert tools.build_of("atlas") == "full"
    assert tools.schema_reading_note() == ""


def test_doctor_asks_with_the_licence_and_where_to_read_it(monkeypatch):
    from migkit import cli
    shown, asked = [], []
    monkeypatch.setattr(cli.console, "print",
                        lambda *a, **k: shown.append(" ".join(map(str, a))))
    monkeypatch.setattr(cli.click, "confirm",
                        lambda q, default=None: asked.append(
                            (q, default)) or True)
    assert cli._ask_terms("riotx", tools.TERMS["riotx"])
    text = " ".join(shown)
    assert "Business Source License" in text
    assert tools.TERMS["riotx"].url in text
    assert "Competitive" in text or "overlaps" in text
    # never yes by default
    assert asked == [("  accept the terms of riotx?", False)]
