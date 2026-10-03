"""SaaS applications as sources, read by programs (backlog ER7): one
adapter for every kind of connector, feeding migkit's own writers and its
own proof.

    source: singer://tap-github?config=/etc/migkit/github.json
    source: airbyte://airbyte/source-hubspot:6.9.3?config=/etc/migkit/hs.json
    source: dlt://mysources.shop:shop_source?config=/etc/migkit/shop.json
    source: salesforce://login.salesforce.com?config=/etc/migkit/sf.json

A side's `kind` is how its program is spoken to, its `program` what is
run - a tap's command, a connector's image or command, a dlt source's
`module:function`, an org's sign-in host - and `config` the file of the
program's own settings, credentials included, which migkit hands on and
never shows. A connector not on this machine is installed into an
environment of its own from `install` (a pip requirement): its
dependencies are its own, never migkit's.

The hop's database is the program's catalogue, named after it; a table is
a stream (a resource, an object); a stream's columns are its schema's, in
words `canon` maps (`schema_word`), and its key the one the program
declares. Every kind is asked the same three things (`Program`): its
catalogue, its records read from a state (none: from the start), and the
state it reached. A stream is copied in one pass: no connector resumes a
read part way.

**Follow.** Each kind's own state is the position. A follow starts from
the state the copy's own read of each stream ended with - kept beside the
hop's reports as the copy reads (`copy_point` asks for it) - and each read
after runs the program from the state the last one reached, all streams
at once. What it hands over is held to what the target was given before,
row by row: a hash of each row as migkit renders it, kept by key
(`Seen`), so a record the program hands over again unchanged is not a
change, and a stream read whole every time - a full-table stream -
carries the rows that are gone as deletes. The hashes of a read move on
only once the tail has saved the position after it, which is proof its
changes were applied; a tail that goes on from an earlier position reads
from that one's. A stream read incrementally names only rows that
changed, so a row deleted at the source is carried only where the program
marks it (`_sdc_deleted_at`, `_ab_cdc_deleted_at`, a column dlt calls
`hard_delete`); where it does not, the verdict says so and check finds
the row. Salesforce follows through its own change events instead
(`saas_salesforce`).

**Proved.** A comparison reads the stream again through the program and
compares every row with the target's through migkit's renderer; where the
application counts its own records (Salesforce), that count, taken before
and after the read, is held to the rows read (`read_proved`). What cannot
be compared is said with the verdict (`not_compared`).

Written 2026-10-01 with programs of each kind written for the tests - a
tap and a connector as the protocols define them, a dlt source, a local
REST and change-event service standing in for Salesforce
(`tests/test_saas_sources_as_programs.py`,
`tests/test_salesforce_changes_through_its_events.py`) - and no SaaS
account, which migkit may not use here.
"""
import base64
import datetime
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import threading
from dataclasses import dataclass, field
from decimal import Decimal

from .base import Engine

KINDS = ("dlt", "singer", "airbyte", "salesforce")
#: fields a program marks a deleted row with, and the row is then carried
#: as a delete
SOFT_DELETES = ("_sdc_deleted_at", "_ab_cdc_deleted_at")
#: beside the hop's reports: the follow's start as the copy read it, and
#: the hashes of the rows the target was given
FOLLOW = "program-follow.json"
ROWS = "program-rows.sqlite"


def database_name(kind, program):
    """The database a program's streams are listed under: the program's
    own name, without its path, image tag or function."""
    if kind == "salesforce":
        return "salesforce"
    name = str(program or kind).rstrip("/")
    name = name.split("/")[-1].split(":")[0] if kind != "dlt" else \
        name.split(":")[-1]
    return name or kind


@dataclass
class Stream:
    """One stream of a program's catalogue."""
    name: str
    columns: list                 #: [(name, declared word)]
    key: list                     #: its key columns, [] where it has none
    incremental: bool             #: read from a state, not whole each time
    raw: object = None            #: the program's own description of it
    soft_delete: tuple = ()       #: fields marking a deleted row
    #: columns migkit could not compare, and why, for the verdict
    unread: list = field(default_factory=list)


def schema_word(schema):
    """A JSON schema's property as the word `canon` maps (`saas`): the
    type its values are, the nulls it allows left aside."""
    schema = schema or {}
    types = schema.get("type") or []
    if isinstance(types, str):
        types = [types]
    types = [t for t in types if t != "null"]
    if not types and schema.get("anyOf"):
        inner = [schema_word(s) for s in schema["anyOf"]
                 if s.get("type") != "null"]
        return inner[0] if len(set(inner)) == 1 else "json"
    if len(types) != 1:
        return "json"
    t = types[0]
    fmt = schema.get("format")
    kind = schema.get("airbyte_type")
    if t == "string":
        if fmt == "date-time":
            return ("timestamp" if kind == "timestamp_without_timezone"
                    else "timestamptz")
        if fmt == "date":
            return "date"
        if fmt == "time" or kind in ("time_without_timezone",
                                     "time_with_timezone"):
            return "time"
        if schema.get("contentEncoding") == "base64":
            return "bytes"
        if fmt == "uuid":
            return "uuid"
        width = schema.get("maxLength")
        return f"text({int(width)})" if width else "text"
    if t == "integer":
        return "integer"
    if t == "number":
        return "number"
    if t == "boolean":
        return "boolean"
    return "json"


def _instant(text, zoned):
    """An ISO 8601 date-time as the value it is: an instant where `zoned`
    (a naive text read as UTC, as the protocols say), else the clock."""
    got = datetime.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    if zoned:
        return got if got.tzinfo else got.replace(
            tzinfo=datetime.timezone.utc)
    return got.replace(tzinfo=None) if got.tzinfo is None else \
        got.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def value_of(word, value):
    """A record's value as its column means it; one that is not - a date
    that does not parse - as uncomparable, never passed for a match."""
    from .. import canon
    if value is None:
        return None
    base = str(word).split("(")[0]
    try:
        if base == "timestamptz":
            return value if isinstance(value, datetime.datetime) else \
                _instant(value, True)
        if base == "timestamp":
            return value if isinstance(value, datetime.datetime) else \
                _instant(value, False)
        if base == "date":
            if isinstance(value, datetime.datetime):
                return value.date()
            return value if isinstance(value, datetime.date) else \
                datetime.date.fromisoformat(str(value)[:10])
        if base == "time":
            return value if isinstance(value, datetime.time) else \
                datetime.time.fromisoformat(str(value).replace("Z", ""))
        if base == "bytes":
            return value if isinstance(value, (bytes, bytearray)) else \
                base64.b64decode(value)
        if base in ("number", "decimal"):
            return value if isinstance(value, Decimal) else Decimal(str(value))
        if base == "integer":
            return int(value)
        if base == "float":
            return float(value)
        if base == "boolean":
            return value if isinstance(value, bool) else \
                str(value).lower() in ("true", "1")
        if base == "text":
            return value if isinstance(value, str) else json.dumps(value)
    except (ValueError, TypeError, ArithmeticError):
        return canon.uncomparable(str(value))
    return value


def merge_state(kind, a, b):
    """Two states of one program, each reached by a read of some of its
    streams, as one - what each kind keeps per stream, kept per stream."""
    if a is None:
        return b
    if b is None:
        return a
    if kind == "singer":
        out = dict(a)
        out["bookmarks"] = {**(a.get("bookmarks") or {}),
                            **(b.get("bookmarks") or {})}
        return out
    if kind == "airbyte":
        by = {}
        for msg in list(a) + list(b):
            by[_state_key(msg)] = msg
        return list(by.values())
    if kind == "dlt":
        out = json.loads(json.dumps(a, default=str))
        for name, src in (b.get("sources") or {}).items():
            have = out.setdefault("sources", {}).setdefault(name, {})
            have.setdefault("resources", {}).update(
                src.get("resources") or {})
        return out
    return b


def _state_key(msg):
    """A state message's stream, or its kind where it is not a stream's:
    a later message of the same key replaces an earlier one."""
    kind = msg.get("type", "LEGACY")
    if kind == "STREAM":
        d = (msg.get("stream") or {}).get("stream_descriptor") or {}
        return f"STREAM:{d.get('namespace') or ''}.{d.get('name')}"
    return kind


# ---- the programs ------------------------------------------------------------

class Program:
    """How one kind of connector is spoken to. `catalog` its streams;
    `read(streams, state)` yields ("record", stream, {field: value}) and
    ("state", the state reached) in the order the program says them;
    `count(stream)` the application's own count, where it keeps one."""
    kind = ""
    #: what a verdict calls it
    said = "the connector"

    def __init__(self, engine, side):
        self.engine, self.side = engine, side
        self.ep = engine._ep(side)
        self.options = self.ep.options or {}

    def config(self):
        """The program's own settings, from the file the side names."""
        path = self.options.get("config")
        if not path:
            return {}
        try:
            with open(os.path.expanduser(str(path))) as f:
                return json.load(f)
        except (OSError, ValueError) as e:
            raise SystemExit(f"the {self.engine._side_word(self.side)}'s"
                             f" config {path} could not be read"
                             f" ({type(e).__name__})") from None

    def catalog(self):
        raise NotImplementedError

    def read(self, streams, state):
        raise NotImplementedError

    def count(self, stream):
        return None

    def rows_by_key(self, stream, keys):
        """The records of these keys, where the application can be asked
        for them; None where only a whole read can find them."""
        return None

    def not_compared(self, stream):
        return []


def _command(engine, side, program):
    """The command line that runs `program`: installed into its own
    environment from the side's `install` where it is not on this
    machine, never into migkit's."""
    from .. import tools
    found = shutil.which(program) or (program if os.path.isfile(program)
                                      else None)
    if found:
        return [found]
    ep = engine._ep(side)
    requirement = (ep.options or {}).get("install")
    home = tools.tools_home() / "programs" / database_name("singer",
                                                           program)
    here = home / "bin" / os.path.basename(program)
    if here.exists():
        return [str(here)]
    if not requirement:
        raise SystemExit(
            f"the {engine._side_word(side)}'s program {program} is not on"
            " this machine: install it, or name what installs it with"
            " `install` in the side's options (a pip requirement), which"
            " migkit installs into an environment of its own")
    uv = shutil.which("uv")
    steps = ([[uv, "venv", "-q", str(home)],
              [uv, "pip", "install", "-q", "--python",
               str(home / "bin" / "python"), str(requirement)]] if uv else
             [["python3", "-m", "venv", str(home)],
              [str(home / "bin" / "pip"), "install", "-q", str(requirement)]])
    for step in steps:
        got = subprocess.run(step, capture_output=True, text=True)
        if got.returncode != 0:
            raise SystemExit(
                f"the program {program} did not install from"
                f" {requirement}:"
                f" {(got.stderr.strip().splitlines() or [''])[-1][:160]}")
    if not here.exists():
        raise SystemExit(f"{requirement} installed no program called"
                         f" {os.path.basename(program)}")
    return [str(here)]


def _lines(argv, cwd=None, env=None):
    """Each line the program writes, as it writes it; a program that ends
    in failure raises with the last of what it said."""
    err = tempfile.TemporaryFile(mode="w+")
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=err,
                            text=True, cwd=cwd, env=env, bufsize=1)
    try:
        for line in proc.stdout:
            yield line
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    if proc.returncode:
        err.seek(0)
        tail = [x for x in err.read().strip().splitlines() if x.strip()]
        raise RuntimeError(f"the program ended with {proc.returncode}: "
                           f"{(tail or [''])[-1][:200]}")


def _messages(lines):
    """Each JSON message of a program's output; its other lines (a tap's
    stray print) passed over, as the protocols allow."""
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            yield json.loads(line, parse_float=Decimal)
        except ValueError:
            continue


class Singer(Program):
    """A Singer tap, run as the program it is: `--discover` for its
    catalogue, `--catalog` with the streams selected and `--state` for a
    read (`--properties` for a tap that knows only that); its RECORD and
    STATE messages, and BATCH files where a tap writes its records into
    them (`singer_argv`, `singer_events`)."""
    kind = "singer"
    said = "the tap"

    def _run(self, work, *args):
        argv = _command(self.engine, self.side,
                        str(self.options.get("program"))) + list(args)
        return _lines(argv, cwd=work)

    def _config_file(self, work):
        path = os.path.join(work, "config.json")
        with open(path, "w") as f:
            json.dump(self.config(), f)
        return path

    def catalog(self):
        with tempfile.TemporaryDirectory() as work:
            text = "".join(self._run(work, *singer_argv(
                "discover", self._config_file(work))))
        try:
            got = json.loads(text[text.index("{"):], parse_float=Decimal)
        except ValueError:
            raise SystemExit("the tap's catalogue is not JSON") from None
        return singer_streams(got)

    def _catalog_flag(self):
        """`--catalog` or, for a tap older than it, `--properties`: what its
        `--about` says it takes, and `--catalog` where it says nothing."""
        known = self.engine.__dict__.setdefault("_catalog_flags", {})
        if self.side not in known:
            flag = "catalog"
            try:
                with tempfile.TemporaryDirectory() as work:
                    text = "".join(self._run(work, *singer_argv("about")))
                caps = json.loads(text[text.index("{"):]).get(
                    "capabilities") or []
                if "properties" in caps and "catalog" not in caps:
                    flag = "properties"
            except (RuntimeError, ValueError, OSError):
                pass
            known[self.side] = flag
        return known[self.side]

    def read(self, streams, state):
        by_name = {s.name: s for s in streams}
        with tempfile.TemporaryDirectory() as work:
            cfg = self._config_file(work)
            cat = os.path.join(work, "catalog.json")
            with open(cat, "w") as f:
                json.dump(singer_selected(
                    self.engine._catalog(self.side).values(), by_name), f,
                    default=str)
            path = None
            if state is not None:
                path = os.path.join(work, "state.json")
                with open(path, "w") as f:
                    json.dump(state, f, default=str)
            yield from singer_events(
                _messages(self._run(work, *singer_argv(
                    "read", cfg, cat, self._catalog_flag(), path))),
                by_name)


def singer_argv(what, config=None, catalog=None, flag="catalog",
                state=None):
    """A tap's command line: its catalogue discovered, what it can do, or
    a read of the catalogue given from a state."""
    if what == "about":
        return ["--about", "--format", "json"]
    if what == "discover":
        return ["--config", config, "--discover"]
    out = ["--config", config,
           "--properties" if flag == "properties" else "--catalog", catalog]
    return out + (["--state", state] if state else [])


def singer_streams(catalog):
    """{stream: Stream} of a tap's catalogue: its schema's columns, its key
    (`key_properties`, or the metadata's), whether it is read from a
    bookmark, and the fields it cannot read."""
    out = {}
    for s in catalog.get("streams") or []:
        name = s.get("tap_stream_id") or s.get("stream")
        meta = {tuple(m.get("breadcrumb") or ()): m.get("metadata") or {}
                for m in s.get("metadata") or []}
        top = meta.get((), {})
        key = list(s.get("key_properties") or
                   top.get("table-key-properties") or [])
        method = str(top.get("forced-replication-method")
                     or top.get("replication-method") or "").upper()
        props = (s.get("schema") or {}).get("properties") or {}
        cols, unread = [], []
        for col, schema in props.items():
            if meta.get(("properties", col), {}).get("inclusion") == \
                    "unsupported":
                unread.append((col, "the tap cannot read it"))
                continue
            cols.append((col, schema_word(schema)))
        out[name] = Stream(
            name, cols, key,
            method in ("INCREMENTAL", "LOG_BASED")
            or bool(top.get("replication-key")), s,
            tuple(c for c in SOFT_DELETES if c in props), unread)
    return out


def singer_selected(streams, chosen):
    """The catalogue a tap reads by: the streams asked for selected with
    every field they can read, the others not."""
    out = []
    for s in streams:
        raw = json.loads(json.dumps(s.raw, default=str))
        on = s.name in chosen
        metas = raw.setdefault("metadata", [])
        if not any(not (m.get("breadcrumb") or []) for m in metas):
            metas.append({"breadcrumb": [], "metadata": {}})
        for m in metas:
            md = m.setdefault("metadata", {})
            if not (m.get("breadcrumb") or []) or \
                    md.get("inclusion") != "unsupported":
                md["selected"] = on
        out.append(raw)
    return {"streams": out}


def singer_events(messages, by_name):
    """("record", stream, record) and ("state", value) of a tap's
    messages: its RECORD, STATE and BATCH ones."""
    for msg in messages:
        kind = msg.get("type")
        if kind == "RECORD" and msg.get("stream") in by_name:
            yield ("record", msg["stream"], msg.get("record") or {})
        elif kind == "STATE":
            yield ("state", msg.get("value"))
        elif kind == "BATCH" and msg.get("stream") in by_name:
            for rec in _batch_records(msg):
                yield ("record", msg["stream"], rec)


def _batch_records(msg):
    """The records of a BATCH message: JSON lines in the files it names,
    gzipped where its encoding says so."""
    import gzip
    from urllib.parse import urlsplit
    enc = msg.get("encoding") or {}
    if str(enc.get("format", "jsonl")) != "jsonl":
        raise SystemExit(f"the tap wrote its records as {enc.get('format')},"
                         " which migkit does not read yet")
    for uri in msg.get("manifest") or []:
        path = urlsplit(uri).path if str(uri).startswith("file:") else uri
        opener = gzip.open if enc.get("compression") == "gzip" else open
        with opener(path, "rt") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line, parse_float=Decimal)


class Airbyte(Program):
    """A connector of the Airbyte protocol, run as a container where the
    side names an image (or says `container`), or as the program it is:
    `discover` for its catalogue, `read` with a configured catalogue and
    its state messages (`airbyte_argv`, `airbyte_events`). Settings it
    hands back renewed are kept beside the hop's reports and used from
    then on, the side's own file never written. Its terms are accepted
    once, at `migkit doctor --install` (`tools.TERMS`)."""
    kind = "airbyte"

    def _allowed(self):
        from .. import tooluse
        ok, why = tooluse.allowed("airbyte-connectors", self.engine.hop)
        if not ok:
            raise SystemExit(f"the {self.engine._side_word(self.side)} is a"
                             f" connector run under terms not accepted: {why}")

    def _container(self):
        program = str(self.options.get("program"))
        said = self.options.get("container")
        return bool(said) if said is not None else ("/" in program
                                                    and ":" in program)

    def _renewed(self):
        return self.engine.hop.report_dir() / "program-config.json"

    def config(self):
        path = self._renewed()
        if path.exists():
            try:
                return json.loads(path.read_text())
            except ValueError:
                pass
        return super().config()

    def _run(self, work, args):
        self._allowed()
        program = str(self.options.get("program"))
        if self._container():
            from ..util import which
            if not which("docker"):
                raise SystemExit("the connector is an image, and docker is"
                                 " not on this machine: migkit doctor"
                                 " --install says what is short")
            argv = ["docker", "run", "--rm", "-i", "-v", f"{work}:/secrets",
                    "-w", "/secrets", program]
            args = [a.replace(work, "/secrets") for a in args]
        else:
            argv = _command(self.engine, self.side, program)
        return _messages(_lines(argv + list(args), cwd=work))

    def _config_file(self, work):
        path = os.path.join(work, "config.json")
        with open(path, "w") as f:
            json.dump(self.config(), f)
        return path

    def _renew(self, config):
        self._renewed().write_text(json.dumps(config))

    def catalog(self):
        with tempfile.TemporaryDirectory() as work:
            got = None
            for event in airbyte_events(
                    self._run(work, airbyte_argv(
                        "discover", self._config_file(work))), {}, None,
                    self._renew):
                if event[0] == "catalog":
                    got = event[1]
        if got is None:
            raise SystemExit("the connector gave no catalogue")
        return airbyte_streams(got)

    def read(self, streams, state):
        by_name = {s.name: s for s in streams}
        with tempfile.TemporaryDirectory() as work:
            cfg = self._config_file(work)
            cat = os.path.join(work, "catalog.json")
            with open(cat, "w") as f:
                json.dump(airbyte_configured(streams), f, default=str)
            path = None
            if state:
                path = os.path.join(work, "state.json")
                with open(path, "w") as f:
                    json.dump(state, f, default=str)
            yield from airbyte_events(
                self._run(work, airbyte_argv("read", cfg, cat, path)),
                by_name, state, self._renew)


def airbyte_argv(what, config, catalog=None, state=None):
    """A connector's command line: `discover`, or `read` of a configured
    catalogue from its state."""
    if what == "discover":
        return ["discover", "--config", config]
    return ["read", "--config", config, "--catalog", catalog] + (
        ["--state", state] if state else [])


def airbyte_streams(catalog):
    """{stream: Stream} of a connector's catalogue: its JSON schema's
    columns, the key it defines (a key inside a document said, never
    guessed), and whether it reads incrementally."""
    out = {}
    for s in catalog.get("streams") or []:
        name = s.get("name")
        props = (s.get("json_schema") or {}).get("properties") or {}
        pk = s.get("source_defined_primary_key") or []
        nested = [k for k in pk if len(k) != 1]
        cols = [(c, schema_word(v)) for c, v in props.items()]
        out[name] = Stream(
            name, cols, [k[0] for k in pk] if not nested else [],
            "incremental" in (s.get("supported_sync_modes") or []), s,
            tuple(c for c in SOFT_DELETES if c in props),
            [(".".join(k), "a key inside a document is not a column")
             for k in nested])
    return out


def airbyte_configured(streams):
    return {"streams": [
        {"stream": s.raw,
         "sync_mode": "incremental" if s.incremental else "full_refresh",
         "cursor_field": s.raw.get("default_cursor_field") or [],
         "destination_sync_mode": "append",
         "primary_key": [[k] for k in s.key]} for s in streams]}


def airbyte_events(messages, by_name, state, renew):
    """("record", stream, data), ("state", every stream's latest state)
    and ("catalog", it) of a connector's messages; a TRACE of an error
    ends the read with its words, and a CONTROL's renewed settings are
    kept (`renew`)."""
    held = list(state or [])
    for msg in messages:
        kind = msg.get("type")
        if kind == "RECORD":
            rec = msg.get("record") or {}
            if rec.get("stream") in by_name:
                yield ("record", rec["stream"], rec.get("data") or {})
        elif kind == "STATE":
            held = merge_state("airbyte", held, [msg["state"]])
            yield ("state", held)
        elif kind == "CATALOG":
            yield ("catalog", msg.get("catalog") or {})
        elif kind == "TRACE" and (msg.get("trace") or {}).get("type") == \
                "ERROR":
            err = msg["trace"].get("error") or {}
            raise RuntimeError("the connector failed:"
                               f" {str(err.get('message') or '')[:200]}")
        elif kind == "CONTROL":
            got = (msg.get("control") or {}).get("connectorConfig") or {}
            if got.get("config"):
                renew(got["config"])


class Dlt(Program):
    """A dlt source, run in this process: its `module:function` called with
    the side's config as its arguments, each resource read on its own with
    the source's state given to it - its incremental cursors are the
    position - and its hints read for columns, key and deletes
    (`dlt_source`, `dlt_streams`, `dlt_read`)."""
    kind = "dlt"
    said = "the source"

    def catalog(self):
        return dlt_streams(lambda: dlt_source(self.options.get("program"),
                                              self.config()))

    def read(self, streams, state):
        return dlt_read(lambda: dlt_source(self.options.get("program"),
                                           self.config()), streams, state)


def dlt_source(target, config):
    """The source `module:function` names, called with `config`; a lone
    resource made a source of one."""
    import importlib

    import dlt
    from dlt.extract.source import DltSource
    module, _, fn = str(target).partition(":")
    if not fn:
        raise SystemExit(f"a dlt source is named module:function, not"
                         f" {target}")
    got = getattr(importlib.import_module(module), fn)(**config)
    if not isinstance(got, DltSource):
        got = dlt.source(lambda: got, name=fn)()
    return got


def dlt_streams(make):
    """{stream: Stream} of a source's resources: each one's declared
    columns and types, its primary key, its `hard_delete` columns, and
    whether it reads incrementally; a column it declares no type for
    typed as dlt infers one from its first rows."""
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import StateInjectableContext
    out = {}
    with Container().injectable_context(StateInjectableContext(state={})):
        src = make()
        for name, res in src.resources.items():
            schema = res.compute_table_schema()
            cols = schema.get("columns") or {}
            words = {c: _dlt_word(v.get("data_type"))
                     for c, v in cols.items() if v.get("data_type")}
            key = [c for c, v in cols.items() if v.get("primary_key")]
            gone = tuple(c for c, v in cols.items() if v.get("hard_delete"))
            if len(words) < len(cols) or not cols:
                for row in _first(res, 50):
                    for c, v in row.items():
                        if c not in words and v is not None:
                            words[c] = _infer(v)
            # a resource with an incremental cursor names only what
            # changed; one without is read whole every time
            out[name] = Stream(name, list(words.items()), key,
                               any(v.get("incremental")
                                   for v in cols.values()), name,
                               gone + tuple(c for c in SOFT_DELETES
                                            if c in words))
    return out


def dlt_read(make, streams, state):
    """Each resource read with the source's state given to it; the state
    after each, as the position."""
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import StateInjectableContext
    st = json.loads(json.dumps(state or {}, default=str))
    for s in streams:
        with Container().injectable_context(StateInjectableContext(
                state=st)):
            src = make()
            for item in src.with_resources(s.name):
                for row in _items(item):
                    yield ("record", s.name, row)
        yield ("state", json.loads(json.dumps(st, default=str)))


def _items(item):
    """Rows of what a resource yields: a mapping, or an Arrow table, a
    record batch or a data frame of them."""
    if isinstance(item, dict):
        yield item
        return
    to_pylist = getattr(item, "to_pylist", None)
    if to_pylist is not None:
        yield from to_pylist()
        return
    to_dict = getattr(item, "to_dict", None)
    if to_dict is not None:
        yield from to_dict("records")
        return
    yield {"value": item}


def _first(resource, n):
    got = []
    for item in resource:
        for row in _items(item):
            got.append(row)
            if len(got) >= n:
                return got
    return got


#: dlt's data types, in the words `canon` maps
DLT_WORDS = {"text": "text", "double": "float", "bool": "boolean",
             "timestamp": "timestamptz", "bigint": "integer",
             "binary": "bytes", "json": "json", "complex": "json",
             "decimal": "number", "wei": "number", "date": "date",
             "time": "time"}


def _dlt_word(data_type):
    return DLT_WORDS.get(str(data_type), "json")


def _infer(value):
    """dlt's own inference of a value's type, in `canon`'s words."""
    from dlt.common.data_types import py_type_to_sc_type
    try:
        return _dlt_word(py_type_to_sc_type(type(value)))
    except Exception:  # noqa: BLE001 - a type dlt does not know: JSON
        return "json"


def program_for(engine, side):
    kind = str((engine._ep(side).options or {}).get("kind") or "").lower()
    if kind == "singer":
        return Singer(engine, side)
    if kind == "airbyte":
        return Airbyte(engine, side)
    if kind == "dlt":
        return Dlt(engine, side)
    if kind == "salesforce":
        from .saas_salesforce import Salesforce
        return Salesforce(engine, side)
    raise SystemExit(f"a program source's kind is one of {', '.join(KINDS)},"
                     f" not {kind or 'none'}")


# ---- what the target was given ------------------------------------------------

class Seen:
    """The hash of each row the target was given, by stream and key, at a
    generation; a read's hashes staged as the next generation and made the
    hashes only once its position is handed back - proof its changes were
    applied - or dropped where an earlier position is."""

    def __init__(self, path):
        self.path = str(path)
        self.lock = threading.Lock()
        with self._db() as c:
            c.execute("create table if not exists rows (stream text, key"
                      " text, hash text, primary key (stream, key))")
            c.execute("create table if not exists staged (stream text, key"
                      " text, hash text, primary key (stream, key))")
            c.execute("create table if not exists meta (name text primary"
                      " key, value text)")

    def _db(self):
        return sqlite3.connect(self.path)

    def _meta(self, c, name, default=None):
        got = c.execute("select value from meta where name = ?",
                        (name,)).fetchone()
        return json.loads(got[0]) if got else default

    def _set(self, c, name, value):
        c.execute("insert or replace into meta values (?, ?)",
                  (name, json.dumps(value)))

    def generation(self):
        with self._db() as c:
            return self._meta(c, "generation", 0)

    def settle(self, gen):
        """Where the position handed back is: its generation's staged
        hashes made the hashes where they are that generation's, dropped
        where they are not."""
        with self.lock, self._db() as c:
            staged = self._meta(c, "staged")
            if staged is not None and int(staged) == int(gen):
                c.execute("delete from rows where (stream, key) in (select"
                          " stream, key from staged where hash is null)")
                c.execute("insert or replace into rows select * from staged"
                          " where hash is not null")
                self._set(c, "generation", int(gen))
            c.execute("delete from staged")
            self._set(c, "staged", None)
            return self._meta(c, "generation", 0)

    def hashes(self, stream):
        with self._db() as c:
            return dict(c.execute("select key, hash from rows where stream"
                                  " = ?", (stream,)))

    def stage(self, gen, changed):
        """[(stream, key, hash or None for gone)] as generation `gen`."""
        with self.lock, self._db() as c:
            c.execute("delete from staged")
            c.executemany("insert or replace into staged values (?, ?, ?)",
                          changed)
            self._set(c, "staged", int(gen))

    def keep(self, stream, hashes, gen=0):
        """A copy's hashes of one stream, as the given generation's."""
        with self.lock, self._db() as c:
            c.execute("delete from rows where stream = ?", (stream,))
            c.executemany("insert into rows values (?, ?, ?)",
                          [(stream, k, h) for k, h in hashes.items()])
            self._set(c, "generation", int(gen))


def _deleted(stream, rec):
    """Whether the program marks this record deleted."""
    return any(rec.get(c) not in (None, False, "", 0)
               for c in stream.soft_delete)


def row_hash(columns, row):
    """A row's hash as migkit renders it: a change the program hands over
    that renders the same is no change."""
    from .. import rowtext
    from ..render import digest_renderer
    text = rowtext.encode([digest_renderer(c)(v)
                           for (_, c), v in zip(columns, row)])
    return hashlib.sha256(text.encode(), usedforsecurity=False).hexdigest()


class SaasEngine(Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "saas"
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is read through a program, with no digest of its"
                      " own")
    OVER_NETWORK = True
    #: no connector resumes a read part way
    RESUMES_BY_KEY = False
    #: a read of changes moves the hashes the follow keeps: a delta check
    #: would take the follow's changes
    CHANGE_POINT_READS_ONLY = False

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    @staticmethod
    def _side_word(side):
        return "source" if side == "src" else "target"

    def program(self, side):
        known = self.__dict__.setdefault("_programs", {})
        if side not in known:
            known[side] = program_for(self, side)
        return known[side]

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        o = self.hop.source.options or {}
        return [database_name(str(o.get("kind") or ""), o.get("program"))]

    def _catalog(self, side):
        known = self.__dict__.setdefault("_catalogs", {})
        if side not in known:
            got = self.program(side).catalog()
            only = (self._ep(side).options or {}).get("streams")
            if only:
                only = [only] if isinstance(only, str) else list(only)
                missing = [s for s in only if s not in got]
                if missing:
                    raise SystemExit(f"the program has no stream"
                                     f" {', '.join(missing[:4])}")
                got = {k: v for k, v in got.items() if k in only}
            known[side] = got
        return known[side]

    def _stream(self, side, table):
        got = self._catalog(side).get(str(table))
        if got is None:
            raise SystemExit(f"{table} is not a stream of the"
                             f" {self._side_word(side)}'s program")
        return got

    def neutral_tables(self, side, db):
        return [n for n in sorted(self._catalog(side))
                if not self.hop.excluded(db, n)]

    def neutral_columns(self, side, db, table):
        return list(self._stream(side, table).columns)

    def neutral_key(self, side, db, table):
        return list(self._stream(side, table).key)

    def table_facts(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            try:
                rows = self.program(side).count(self._stream(side, t))
            except Exception:  # noqa: BLE001 - unknown, never guessed
                rows = None
            out[t] = {"rows": rows, "bytes": None,
                      "key": bool(self.neutral_key(side, db, t))}
        return out

    # ---- reading ---------------------------------------------------------------

    def _row(self, stream, columns, rec):
        words = dict(stream.columns)
        return [value_of(words.get(n, "json"), rec.get(n))
                for n, _ in columns]

    def _records(self, side, db, table, columns, state=None, keep=False):
        """(state reached, batches of rows) of one stream read from
        `state`; where `keep`, this read is the copy a follow starts from,
        and its state and row hashes are kept for it."""
        stream = self._stream(side, table)
        reached = {"state": state}
        hashes = {} if keep else None
        key = stream.key
        classes = self._classes(side, db, table)

        def rows():
            batch = []
            for event in self.program(side).read([stream], state):
                if event[0] == "state":
                    reached["state"] = event[1]
                    continue
                if _deleted(stream, event[2]):
                    # a row the program marks deleted is not the
                    # application's any more: neither copied nor compared
                    continue
                row = self._row(stream, columns, event[2])
                if hashes is not None and key:
                    hashes[self._key_text(stream, event[2])] = row_hash(
                        classes, self._row(stream, classes, event[2]))
                batch.append(row)
                if len(batch) >= 1000:
                    yield batch
                    batch = []
            if batch:
                yield batch
            if keep:
                self._copied(db, table, reached["state"], hashes)
        return reached, rows()

    def _classes(self, side, db, table):
        from .. import canon
        return [(n, canon.type_class(self.CANON_ENGINE, t) or "json")
                for n, t in self.neutral_columns(side, db, table)]

    @staticmethod
    def _key_text(stream, rec):
        return json.dumps([str(rec.get(k)) for k in stream.key])

    def _follow(self, db):
        path = self.hop.report_dir(db) / FOLLOW
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return {}

    def _keep_follow(self, db, got):
        from .. import durable
        durable.write_json(self.hop.report_dir(db) / FOLLOW, got, indent=1)

    def _seen(self, db):
        return Seen(self.hop.report_dir(db) / ROWS)

    def _copied(self, db, table, state, hashes):
        got = self._follow(db)
        entry = got.setdefault("streams", {}).setdefault(str(table), {})
        if entry.get("read"):
            return
        entry.update(read=True, state=state)
        if hashes is not None:
            self._seen(db).keep(str(table), hashes, 0)
        self._keep_follow(db, got)

    def _starts_follow(self, db, table):
        got = self._follow(db)
        entry = (got.get("streams") or {}).get(str(table)) or {}
        return bool(got.get("asked")) and not entry.get("read")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The stream read whole. The first read of a stream a follow
        asked a start for (`copy_point`) is the copy it starts from: its
        state and its rows' hashes are kept."""
        if where:
            raise SystemExit("a row filter is SQL, and a program source"
                             " takes none")
        # a program that follows events of its own keeps no hashes
        keep = side == "src" and self._starts_follow(db, table) and \
            not hasattr(self.program(side), "changes")
        _, rows = self._records(side, db, table, columns, keep=keep)
        for batch in rows:
            for i in range(0, len(batch), max(1, int(size))):
                yield batch[i:i + size]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, which a program does not keep: the stream read
        whole and sorted, for the few callers that ask for a row or two."""
        names = [n for n, _ in columns]
        rows = [r for b in self.neutral_batches(side, db, table, columns)
                for r in b]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        if not key:
            return rows, None
        at = [names.index(k) for k in key]
        rows.sort(key=lambda r: tuple(str(r[i]) for i in at))
        if after is not None:
            rows = [r for r in rows
                    if tuple(str(r[i]) for i in at) > tuple(map(str, after))]
        rows = rows[:limit] if limit else rows
        return rows, (tuple(rows[-1][i] for i in at) if rows else None)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """Asked of the application where it answers by key (Salesforce);
        the stream read whole and the rows picked otherwise."""
        if not key or not keys:
            return {}
        stream = self._stream(side, table)
        names = [n for n, _ in columns]
        got = self.program(side).rows_by_key(stream, [tuple(k)
                                                      for k in keys])
        if got is not None:
            rows = [self._row(stream, columns, rec) for rec in got]
        else:
            _, batches = self._records(side, db, table, columns)
            rows = [r for b in batches for r in b]
        found = self._by_key_map(columns, key, rows)
        want = {self._key_of(columns, key, [dict(zip(key, k)).get(n)
                                            for n in names]) for k in keys}
        return {k: v for k, v in found.items() if k in want}

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here over a whole read, with the application's own
        count - where it keeps one - asked before and after it, for
        `read_proved` to hold the read to."""
        from .. import canon
        if where:
            raise SystemExit("a row filter is SQL, and a program source"
                             " takes none")
        stream = self._stream(side, table)

        def own():
            try:
                return self.program(side).count(stream)
            except Exception:  # noqa: BLE001 - not counted
                return None
        before = own()
        classes = [c for _, c in columns]
        total, n = 0, 0
        _, batches = self._records(side, db, table, columns)
        for batch in batches:
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        after = own()
        self.__dict__.setdefault("_read_totals", {})[(side, db, str(table))] \
            = {"rows": n, "before": before, "after": after}
        return (n, str(total))

    def read_proved(self, side, db, table):
        """(status, words) holding the last whole read to the
        application's own count, or None where it keeps none."""
        got = (self.__dict__.get("_read_totals") or {}).get(
            (side, db, str(table)))
        if not got or (got["before"] is None and got["after"] is None):
            return None
        if got["before"] != got["after"]:
            return ("warn", "the application's count changed while it was"
                            f" read ({got['before']} before,"
                            f" {got['after']} after), so it could not be"
                            " held to the rows read")
        if got["before"] != got["rows"]:
            return ("diff", f"the application counts {got['before']:,}"
                            f" records and {got['rows']:,} were read - the"
                            " read missed or doubled records; read it again")
        return ("ok", f"the application's own count ({got['before']:,})"
                      " agrees with the records read")

    def not_compared(self, db, table):
        """What a comparison of this stream cannot prove, in words."""
        stream = self._stream("src", table)
        out = [f"{c}: {why}" for c, why in stream.unread]
        out += self.program("src").not_compared(stream)
        if stream.incremental and not stream.soft_delete and \
                self.program("src").kind != "salesforce":
            out.append("a record deleted at the source is not carried by the"
                       " follow, which reads only what changed; check finds"
                       " it on the target")
        return "; ".join(out)

    # ---- following ---------------------------------------------------------------

    def copy_point(self, side, db):
        """Where a follow starts: the state each stream's copy ends with,
        and the hashes of what it read - asked for here and kept as the
        copy reads (`neutral_batches`)."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._keep_follow(db, {"asked": now, "streams": {}})
        # a new follow: nothing a target was given before is this one's
        (self.hop.report_dir(db) / ROWS).unlink(missing_ok=True)
        start = self.program(side).start(now) if hasattr(
            self.program(side), "start") else None
        return {"start": now, "gen": 0, **({"events": start}
                                            if start is not None else {})}

    def change_point(self, side, db):
        return self.copy_point(side, db)

    def _resolved(self, db, token):
        """A follow's start as a position: the copies' states merged, and
        where a stream's copy was not kept here, the state it would start
        from with nothing - read whole once, and said."""
        if "start" not in token:
            return token
        if hasattr(self.program("src"), "changes"):
            # a program that follows events of its own starts from them
            return {"state": None, "gen": 0,
                    "events": token.get("events") or {}}
        got = self._follow(db)
        kind = self.program("src").kind
        state, fresh = None, []
        for t in self.neutral_tables("src", db):
            entry = (got.get("streams") or {}).get(t) or {}
            if entry.get("read"):
                state = merge_state(kind, state, entry.get("state"))
            else:
                fresh.append(t)
        for t in fresh:
            # nothing kept of what the target holds: every row is a change
            self._seen(db).keep(t, {}, 0)
        if fresh:
            self._say(f"{', '.join(fresh[:6])}: the copy of these was not"
                      " kept on this machine, so the follow reads them"
                      " whole once, and a row deleted before that read is"
                      " not carried")
        return {"state": state, "gen": 0,
                **({"events": token["events"]} if "events" in token
                   else {})}

    def _say(self, message):
        """A line an operator reads, in the log the copy or the tail lent
        (`lend_log`)."""
        (self.__dict__.get("_lent_log") or print)(message)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """The changes since `token`: the program run from its state over
        every stream, what it hands over held to the hashes of what the
        target was given (`Seen`); Salesforce's own events where the
        program follows them (`changes`)."""
        if token is None:
            return [], self.change_point(side, db)
        token = self._resolved(db, token)
        prog = self.program(side)
        if hasattr(prog, "changes"):
            return prog.changes(db, token, limit)
        seen = self._seen(db)
        gen = seen.settle(token.get("gen", 0))
        began = datetime.datetime.now(datetime.timezone.utc).isoformat()
        streams = [self._stream(side, t) for t in self.neutral_tables(side,
                                                                      db)]
        from .. import canon
        out, staged, state = [], [], token.get("state")
        known = {s.name: seen.hashes(s.name) for s in streams}
        met = {s.name: set() for s in streams}
        by_name = {s.name: s for s in streams}
        shapes = {s.name: self._classes(side, db, s.name) for s in streams}
        for event in prog.read(streams, state):
            if event[0] == "state":
                state = event[1]
                continue
            _, name, rec = event
            stream = by_name[name]
            if not stream.key:
                continue
            classes = shapes[name]
            row = self._row(stream, classes, rec)
            k = self._key_text(stream, rec)
            key = {c: row[[n for n, _ in classes].index(c)]
                   for c in stream.key}
            met[name].add(k)
            if _deleted(stream, rec):
                if k in known[name]:
                    out.append(canon.change("delete", name, key))
                    staged.append((name, k, None))
                continue
            h = row_hash(classes, row)
            if known[name].get(k) != h:
                out.append(canon.change("update" if k in known[name]
                                        else "insert", name, key,
                                        dict(zip([n for n, _ in classes],
                                                 row))))
                staged.append((name, k, h))
        for s in streams:
            if s.incremental or not s.key:
                continue
            # a stream read whole: a key it no longer hands over is gone
            for k in set(known[s.name]) - met[s.name]:
                vals = json.loads(k)
                out.append(canon.change("delete", s.name, {
                    c: value_of(dict(s.columns).get(c, "text"), v)
                    for c, v in zip(s.key, vals)}))
                staged.append((s.name, k, None))
        if not staged and state == token.get("state"):
            return [], {**token, "gen": gen, "read_at": began}
        seen.stage(gen + 1, staged)
        return out, {"state": state, "gen": gen + 1, "read_at": began}

    def log_position(self, side, db):
        """Now, as the moment a read that begins after it covers."""
        return {"read_at": datetime.datetime.now(
            datetime.timezone.utc).isoformat()}

    @staticmethod
    def position_reached(have, want):
        """Whether the follow's last read began at or after `want`'s
        moment: a read that began later has everything the application
        held then."""
        if not have or not want or "read_at" not in want:
            return None
        if "read_at" not in have:
            return False
        return have["read_at"] >= want["read_at"]

    def stream_identity(self, side, db):
        o = self._ep(side).options or {}
        return {"kind": str(o.get("kind") or ""),
                "program": str(o.get("program") or "")}

    def server_identity(self, side, db):
        o = self._ep(side).options or {}
        return {"id": f"{o.get('kind')}:{o.get('program')}",
                "name": str(o.get("program") or "")}

    # ---- as a hop's engine: two applications compared -----------------------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def reach_items(self, side):
        """What `assess` says of a program source: its streams, which are
        read incrementally, whose deletes are carried."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "reach", "item": item,
                          "detail": str(detail)})
        try:
            got = self._catalog(side)
        except (SystemExit, Exception) as e:  # noqa: BLE001 - said
            add("fail", "the program's catalogue",
                str(e).splitlines()[0][:160] if str(e) else
                type(e).__name__)
            return items
        inc = [s for s in got.values() if s.incremental]
        keyless = sorted(s.name for s in got.values() if not s.key)
        add("pass", "the program's catalogue",
            f"{len(got)} streams, {len(inc)} read incrementally")
        silent = sorted(s.name for s in inc if not s.soft_delete)
        if silent and self.program(side).kind != "salesforce":
            add("warn", "deletes", f"{', '.join(silent[:6])} are read"
                                   " incrementally with no mark of a deleted"
                                   " row: a delete is not carried, and check"
                                   " finds it")
        if keyless:
            add("warn", "streams with no key", f"{', '.join(keyless[:6])}:"
                                               " copied whole, never"
                                               " followed")
        return items

    def _assess_extra(self):
        return self.reach_items("src")


# ---- what migkit asks of each kind's protocol and library (backlog 0f) ------

def _register():
    from .. import tooluse
    unused = "unused"
    tooluse.register(
        "singer-tap", licence="AGPL-3.0-or-later", release="0.3",
        declared={"": frozenset(("--config", "--discover", "--catalog",
                                 "--properties", "--state", "--about",
                                 "--format", "--input")),
                  "message": frozenset(("SCHEMA", "RECORD", "STATE",
                                        "ACTIVATE_VERSION", "BATCH"))},
        reads=(("engines/saas.py::singer_argv",
                "engines/saas.py::singer_events"),
               {**{("", f): f for f in ("--config", "--discover",
                                        "--catalog", "--properties",
                                        "--state", "--about", "--format")},
                **{("message", m): m for m in ("RECORD", "STATE",
                                               "BATCH")}}),
        options={
            ("", "--input"): (unused, "a tap reads the source it is for,"
                                      " not a stream given to it"),
            ("message", "SCHEMA"): (unused, "a stream's columns are read"
                                            " from the catalogue, before any"
                                            " record"),
            ("message", "ACTIVATE_VERSION"): (unused, "a stream read whole is"
                                                      " held to the hashes of"
                                                      " what the target was"
                                                      " given, which finds"
                                                      " the rows gone"),
        })
    tooluse.register(
        "airbyte-connectors", release="0.5",
        declared={"": frozenset(("spec", "check", "discover", "read",
                                 "--config", "--catalog", "--state")),
                  "message": frozenset(("RECORD", "STATE", "LOG", "TRACE",
                                        "CONTROL", "CATALOG", "SPEC",
                                        "CONNECTION_STATUS"))},
        reads=(("engines/saas.py::airbyte_argv",
                "engines/saas.py::airbyte_events"),
               {**{("", f): f for f in ("spec", "check", "discover", "read",
                                        "--config", "--catalog",
                                        "--state")},
                **{("message", m): m for m in ("RECORD", "STATE", "TRACE",
                                               "CONTROL", "CATALOG")}}),
        options={
            ("", "spec"): (unused, "the settings are the operator's file,"
                                   " which the connector checks itself"),
            ("", "check"): (unused, "a discover signs in as a check does,"
                                    " and says the same failure"),
            ("message", "LOG"): (unused, "a connector's own log lines say"
                                         " nothing its records do not"),
            ("message", "SPEC"): (unused, "the settings are the"
                                          " operator's file, which the"
                                          " connector checks itself"),
            ("message", "CONNECTION_STATUS"): (unused, "a discover signs in"
                                                       " as a check does"),
        })
    dlt_surface = frozenset((
        "source", "resource", "with_resources", "resources",
        "compute_table_schema", "StateInjectableContext", "Container",
        "injectable_context", "py_type_to_sc_type", "primary_key",
        "hard_delete", "data_type", "incremental", "pipeline", "run",
        "extract", "normalize", "load", "destination", "merge_key",
        "write_disposition", "add_limit", "parallelized"))
    tooluse.register(
        "dlt", licence="Apache-2.0", release="1.30",
        declared={"": dlt_surface},
        reads=(("engines/saas.py::dlt_source",
                "engines/saas.py::dlt_streams", "engines/saas.py::dlt_read",
                "engines/saas.py::_infer"),
               {("", o): o for o in dlt_surface}),
        options={
            ("", "resource"): (unused, "a resource is the source's own, read"
                                       " as it declares it"),
            ("", "pipeline"): (unused, "migkit's own writers take the rows"
                                       " and its proof holds them to the"
                                       " source: a pipeline would be a"
                                       " second writer"),
            ("", "run"): (unused, "see pipeline"),
            ("", "extract"): (unused, "see pipeline"),
            ("", "normalize"): (unused, "see pipeline"),
            ("", "load"): (unused, "see pipeline"),
            ("", "destination"): (unused, "see pipeline"),
            ("", "merge_key"): (unused, "rows are merged by their primary"
                                        " key, on migkit's writers"),
            ("", "write_disposition"): (unused, "a read whole is found by"
                                                " the stream's own"
                                                " incremental cursor, or its"
                                                " absence"),
            ("", "add_limit"): (unused, "a copy reads every row"),
            ("", "parallelized"): (unused, "a resource runs as its source"
                                           " declares it"),
        })


_register()
