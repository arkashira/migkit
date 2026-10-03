import contextlib
import json
import os
import socket
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
from rich.console import Console
from rich.table import Table

from . import advisors, capabilities
from .config import get_hop, load_hops
from .engines import get_engine
from .util import without_secret, Timer, human_int, human_secs, which
from . import __version__

#: soft-wrapped: output that is not a terminal was cut at 80 columns, and a
#: CI log read "verdict: different" on one line and its path on the next
console = Console(highlight=False, soft_wrap=True)
QUIET = False


def _esc(text):
    """Text from a server, a file or an exception, shown as it is: a
    bracket in it is not markup - `[dbo].[orders] src=10 dst=7` printed as
    `. src=10 dst=7`, and a `[/x]` in one raised before `summary.json` was
    written."""
    from rich.markup import escape
    return escape(str(text))


def say_raw(text):
    """A line of data, SQL or a server's words, printed as it is."""
    console.print(str(text), markup=False, highlight=False)


def chat(msg):
    if not QUIET:
        console.print(msg)


def _lock(hop):
    """The hop's write lease, held while the operation runs and renewed
    by a heartbeat (`migkit.lease`). `.unlink()` releases it. Inside a
    move that holds a hop with a standby (`ha`), the holding itself."""
    from . import ha
    got = ha.held(hop)
    if got is not None:
        return got.nested()
    from .lease import Lease
    from .state import run_state
    lease = Lease(hop.report_dir() / "lease.json",
                  remote=run_state(hop)).acquire()
    if lease.took_over:
        was = lease.took_over
        console.print(f"[yellow]took the hop over from"
                      f" {was.get('host')} process {was.get('pid')}, whose"
                      " lease had lapsed: it stopped without letting go"
                      "[/yellow]")
    return lease


def _changelog(hop, entry, eng=None):
    # audit is local-only: migkit never writes bookkeeping into the
    # destination, so the target stays a faithful copy of the source and
    # schema verification never trips over our own table. Chained, so an
    # entry changed afterwards shows (`migkit.audit`)
    from . import audit
    audit.append(hop.report_dir() / "changelog.jsonl", entry)


class _Checkpoint(dict):
    """How far a copy got. Kept in the hop's run state as well where it has
    one (the s3 backend), so a run another machine takes over resumes from
    the last chunk saved rather than from the start (backlog 30)."""

    def __init__(self, path, remote=None):
        super().__init__()
        import threading
        self.path = path
        self.remote = remote
        self._lock = threading.RLock()
        # the changes made under the lock (`changed`), and the last of them
        # a save has written
        self._gen = self._saved = 0
        # the entries this process's copiers write into: theirs, whatever
        # another machine saved since
        self._mine = set()
        if not path.exists() and remote is not None:
            text = remote.get(path)
            if text:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
        if path.exists():
            self.update(json.loads(path.read_text()))

    def setdefault(self, key, default=None):
        self._mine.add(key)
        return super().setdefault(key, default)

    def __setitem__(self, key, value):
        self._mine.add(key)
        super().__setitem__(key, value)

    def changed(self):
        """One change made under the lock (`ranges.started`,
        `ranges.finished`): its number, for `save(upto=...)`."""
        with self._lock:
            self._gen += 1
            return self._gen

    def save(self, upto=None):
        """Written whole or not at all - to a file beside it, then put in
        its place: a crash while writing left half a checkpoint, which the
        next run could not read. Tables copied side by side save through
        one lock; `upto`, a change made under it (`changed`), is not
        written again where a save that began after it has written it -
        ranges finishing side by side while one save writes are written by
        the next, one save for all of them, not one each.

        Compact: indented, the encoder falls back to its slow pure-Python
        path - measured (CPython 3.12, in-process) on the checkpoint of 200
        tables of 50 ranges each, 1.7 MB: 26.5 ms a save indented, 4.2 ms
        compact, and such a copy saves twice a range."""
        from . import failpoint
        with self._lock:
            if upto is not None and self._saved >= upto:
                return
            if self.remote is not None:
                self._merge_into_remote()
            gen = self._gen
            text = self._encoded()
            tmp = self.path.with_name(
                f"{self.path.name}.{os.getpid()}.{threading.get_ident()}"
                ".tmp")
            tmp.write_text(text)
            failpoint.hit("checkpoint.written")
            tmp.replace(self.path)
            self._saved = gen

    def _encoded(self):
        """The checkpoint as JSON. A copier that adds a field to its own
        table's entry outside the lock while it is encoded makes the
        encoder stop (`dictionary changed size`); encoded again, a bounded
        number of times - the loop that did this ended in an unbound name
        on its last turn and took the move down with it."""
        for _ in range(100):
            try:
                return json.dumps(self, separators=(",", ":"), default=str)
            except RuntimeError:
                time.sleep(0.001)
        raise SystemExit(f"the move's checkpoint {self.path} kept changing"
                         " while it was written; nothing past the last save"
                         " is lost, and the next run goes on from it")

    def discard(self):
        self.path.unlink(missing_ok=True)
        if self.remote is not None:
            self.remote.delete(self.path)

    def _merge_into_remote(self):
        """Machines sharing a move each save the tables they copy: the
        stored checkpoint is written only over the version read, with
        every other machine's entries kept."""
        from .state import Taken
        for _ in range(50):
            have, version = self.remote.read_record(self.path, sealed=True)
            merged = dict(have or {})
            for key, mine in self.items():
                theirs = merged.get(key)
                if key in self._mine or not isinstance(theirs, dict) \
                        or (isinstance(mine, dict) and _further(mine, theirs)):
                    merged[key] = mine
            try:
                self.remote.write_record(self.path, merged, version,
                                         sealed=True)
            except Taken:
                time.sleep(0.05)
                continue
            for key, value in merged.items():
                if key not in self._mine:
                    dict.__setitem__(self, key, value)
            return
        raise SystemExit("the move's checkpoint kept changing under migkit"
                         " while it saved it")

    def reload(self, key=None):
        """What every machine sharing the move has saved by now. `key` is
        a table about to be copied here: another machine's entry for it
        replaces this one's, which has not started."""
        if self.remote is None:
            return self
        have, _ = self.remote.read_record(self.path, sealed=True)
        for k, theirs in (have or {}).items():
            mine = self.get(k)
            if k == key or (k not in self._mine and (
                    not isinstance(mine, dict) or _further(theirs, mine))):
                dict.__setitem__(self, k, theirs)
                self._mine.discard(k)
        return self


def _further(a, b):
    """Whether checkpoint entry `a` went further than `b`."""
    if a.get("done") or b.get("done"):
        return bool(a.get("done")) and not b.get("done")
    return int(a.get("moved") or 0) >= int(b.get("moved") or 0)


def _checkpoint(hop, path):
    from .state import run_state
    return _Checkpoint(path, run_state(hop))


def _sharing(hop):
    """The run state tables are shared through, for a hop that says
    `share_tables: true` - machines running the same move each take the
    tables no other machine holds (backlog 31)."""
    if not (hop.options or {}).get("share_tables"):
        return None
    from .state import run_state
    remote = run_state(hop)
    if remote is None:
        raise SystemExit("share_tables needs the s3 state backend"
                         " (options.state.backend: s3): the machines sharing"
                         " a move find each other's tables there")
    return remote


#: a database's run files another machine needs to carry a stream on
RUN_FILES = ("tail-token.json", "tail-shape.json", "tail-source.json",
             "copy-position.json", "tail-tables.json", "tail-copy.json",
             "copy-before.json")


def _shared(hop, d):
    """This database's run files, fetched from the hop's run state where
    this machine lacks them - or None without one."""
    from . import ha
    from .state import Mirror, run_state
    pushed = ha.mirror(hop, d)
    if pushed is not None:
        # a hop with a standby: its files go with the holding's own pushes,
        # each over no newer holder's
        return pushed
    remote = run_state(hop)
    if remote is None:
        return None
    return Mirror(remote, [hop.report_dir(d) / n for n in RUN_FILES]).fetch()


@contextlib.contextmanager
def _refusing():
    """A guard's stop, as the refusal it is (`verdict.Refused`: said in
    its own words, exit 4) rather than a command that failed."""
    from .verdict import Refused, Usage
    try:
        yield
    except (Refused, Usage):
        raise
    except SystemExit as e:
        if isinstance(e.code, str) and e.code.strip():
            raise Refused(e.code) from e
        raise


def _interlock(hop, eng, d, go):
    """The interlock on the target, at the start of a write step
    (`interlock.before_write`)."""
    from . import interlock
    with _refusing():
        return interlock.before_write(hop, eng, d, go,
                                      lambda m: console.print(
                                          f"  {_esc(m)}"))


def _refuse_unmasked(hop, stream):
    """What a write that carries rows as the source holds them does to a
    hop that masks columns on the way: refused, in its words."""
    from .verdict import Refused
    if stream:
        # the masking is the dump's; a change read after it is the
        # source's own row, and would land unmasked
        raise Refused(f"{hop.name} masks columns on the way, and a change"
                      " stream would carry their real values into the"
                      " target: a hop that masks is copied (--mode full),"
                      " not followed. Nothing has been started")
    raise Refused(f"{hop.name} masks columns on the way, and a repair of"
                  " rows writes the source's values as they are, unmasked:"
                  " move those tables again with --mode full, which masks"
                  " them; --kind sequences or --kind schema repairs the"
                  " rest. Nothing has been started")


def _guard_write(hop, eng, what, dbs, go, mode="", drop=False, step=None,
                 approve=None, carries_rows=False, preflight=None, db=""):
    """Every command that writes passes here before its first write - a
    move, a stream and its teardown, a repair, `sync`'s flows and sweep, a
    rollback, a converted schema - so none skips what another holds. Three
    did: `sync --mode migrate/stream --go`, `sync --go`, and `tail` and
    `replicate`, which streamed real values into a hop that masks, wrote
    to a `goal: verify` hop and tore a stream down with no approval.

    In order: the hop's goal allows writing (`hopkeys.refuse_writes`); a
    hop that masks columns is not written rows as the source holds them -
    a stream (`mode` cdc or full+cdc, not a teardown) or a repair of rows
    (`carries_rows`); what would stop the step before its end, known now
    (`ready`, where `preflight` names the mode); the approvals the hop
    asks for `step`, for each of `approve` (`dbs` where not given); the
    target held to what the plan showed for each of `dbs`
    (`interlock.before_write`).

    With `go`, the first that says no stops the command before anything
    is written (`verdict.Refused`, exit 4); without, the plan says what
    would stop it ("with --go this stops: ...") and goes on - a dry run
    asks what `--go` asks."""
    from . import approvals, hopkeys, ready
    from .verdict import Refused

    def stop(said):
        if go:
            raise Refused(said)
        console.print(f"  with --go this stops: {_esc(said)}")
    try:
        with _refusing():
            hopkeys.refuse_writes(hop, what)
    except Refused as e:
        stop(str(e.code))
    stream = mode in ("cdc", "full+cdc") and not drop
    if (hop.options or {}).get("mask_on_write") and (stream or carries_rows):
        try:
            _refuse_unmasked(hop, stream)
        except Refused as e:
            stop(str(e.code))
    if preflight is not None:
        if go:
            with _refusing():
                ready.gate(hop, eng, [db] if db else None, preflight, drop,
                           lambda m: console.print(f"  {m}"))
        else:
            # the plan says them where the run would stop on them, and
            # what a person does before it
            for i in ready.stops(hop, eng, [db] if db else None,
                                 preflight, drop):
                console.print(f"  with --go this stops: {_esc(i)}")
    if step:
        for d in (approve if approve is not None else (dbs or [db])):
            if go:
                with _refusing():
                    approvals.require(hop, step, d,
                                      lambda m: console.print(
                                          f"  {_esc(m)}"))
            elif approvals.needed(hop, step):
                console.print(f"  with --go this waits for the {step}"
                              " approval the hop asks for"
                              + (f" ({_esc(d)})" if d else ""))
    for d in dbs or ():
        _interlock(hop, eng, d, go)


def _scenario_checks(hop, eng, d, out, table, checks):
    """A database's check results with the scenarios' proofs: a change of
    character set proved in the new one (in place of a data check of each
    side's own bytes, for the tables it covers whole), a change of storage
    engine, and a split's rows each placed once - where the data is
    checked, and the whole database."""
    if table or "data" not in checks:
        return out
    from . import charsets, split, tableengine
    from .engines.base import Result
    for name, module, run in (
            ("charset", charsets, lambda o: charsets.check(hop, eng, d, o)),
            ("engine", tableengine,
             lambda o: tableengine.check(hop, eng, d, o)),
            ("split", split, lambda o: o + ([r.__dict__ for r in
                                             split.prove(hop, eng, d)]
                                            if split.settings(hop)
                                            is not None else []))):
        try:
            out = run(out)
        except Exception as e:  # noqa: BLE001 - said, as the hop asked it
            # an error where the hop asks for the change; where it only
            # may be one, the check stands as it did before without it
            asked = module.settings(hop) is not None
            out = out + [Result(name, d, "error" if asked else "skip",
                                "could not be proved: "
                                f"{str(e).strip()[:160]}").__dict__]
    return out


def _after_copy(hop, eng, d):
    """Once a database's rows are in: the character set the hop asks for
    made by the target's own conversion, double-encoded text repaired
    (`charsets.after_copy`), and a table the target has in an old engine
    converted in place (`tableengine.in_place`)."""
    from . import charsets, tableengine
    say = lambda m: console.print(f"  {_esc(m)}")  # noqa: E731
    for line in (charsets.after_copy(hop, eng, d, say)
                 + tableengine.in_place(hop, eng, d, say)):
        say(line)


def _tables_to_copy(hop, eng, d, table=""):
    """What a copy of `d` writes, as `list_move_tables` says it, or None
    where the engine has no copier to ask or a side does not answer - the
    move then says why in its own words."""
    from . import interlock
    if table:
        return [tuple(table.split(".", 1)) if "." in table
                else ("", table)]
    if not interlock.reached(hop):
        return None
    try:
        return eng.list_move_tables(d)
    except (AttributeError, NotImplementedError):
        return None


def _keep_out(hop, eng, d, tables, go, whole=False):
    """The interlock's word on a copy of `d` (`interlock.before_copy`):
    the tables whose rows the hop keeps as they are are left out of it.
    The tables still to copy, or None where that leaves nothing of `d`."""
    from . import interlock
    if tables is None:
        _interlock(hop, eng, d, go)
        return []
    with _refusing():
        kept = interlock.before_copy(hop, eng, d, tables, go,
                                     lambda m: console.print(f"  {_esc(m)}"),
                                     whole)
    if not kept:
        return list(tables)
    interlock.leave_out(hop, d, kept)
    left = [t for t in tables if tuple(t) not in kept]
    if all(hop.excluded(d, *t) for t in left):
        console.print(f"  {d}: every table this copy writes holds rows the"
                      " hop keeps as they are; nothing to copy")
        return None
    return left


def _require_configured(hop):
    missing = [side for side, ep in (("source", hop.source),
                                     ("target", hop.target))
               if not ep.configured()]
    if missing:
        raise SystemExit(
            f"{hop.name}: {' and '.join(missing)} not configured yet\n"
            f"fill host/user/password in conf/hops.yaml, then: migkit doctor")


class _Step(click.Command):
    """Every command as one run (`session.step`): its trace, the ways
    chosen in it kept with its evidence, and the hop's receivers told of
    a failure, a fallback or a writing run done.

    And every command ends here, the one place that says how: its
    schedule's gate, asked once before it runs (`schedule.gate`; a command
    another invokes is not asked again, and a slot not due is said and
    ends 0); SIGTERM taken as Ctrl-C, so a service manager's stop runs the
    same cleanup; and the exit code from one table (`verdict.EXIT`,
    `verdict.VERDICTS`) - a verdict's own, 2 for flags that do not go
    together, 3 for a stop part way, 4 for a guard that refused, 130 for
    an interrupt - where every stop, traceback and Ctrl-C ended 1, the
    code of a difference. What stopped it is said in migkit's words: a
    program's name taken out, a password never, the traceback kept in the
    hop's debug log rather than shown (`_stopped`)."""

    def invoke(self, ctx):
        from . import schedule, session
        slot, code = None, 0
        hop = None
        with _term_as_interrupt():
            try:
                hop = _scheduled_hop(ctx)
                if hop is not None:
                    slot = schedule.gate(hop, ctx.info_name, console.print)
                with session.step(ctx.info_name, ctx.params,
                                  lambda m: console.print(
                                      f"[yellow]{_esc(m)}[/yellow]")):
                    return super().invoke(ctx)
            except click.exceptions.ClickException as e:
                code = e.exit_code
                raise
            except (click.exceptions.Exit, click.exceptions.Abort):
                raise
            except BaseException as e:  # noqa: BLE001 - every end, mapped
                code = _stopped(e, ctx)
                raise SystemExit(code) from None
            finally:
                if slot is not None:
                    from .verdict import ran
                    slot.finished(ran(code))


@contextlib.contextmanager
def _term_as_interrupt():
    """SIGTERM - a service manager's stop, `kill` - taken as Ctrl-C for the
    whole command, so every `finally` and every started program's stop
    runs: it ended Python where it stood, leaving a copier's pool and a
    target's dropped indexes behind. Only the main thread can take a
    signal; elsewhere nothing changes."""
    import signal
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def stop(signum, frame):
        raise KeyboardInterrupt
    try:
        before = signal.signal(signal.SIGTERM, stop)
    except ValueError:
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, before)


def _scheduled_hop(ctx):
    """The hop a command run by its schedule (`MIGKIT_SCHEDULED=1`) is
    for; None for a command run by hand, or one with no hop."""
    from . import schedule
    name = ctx.params.get("hop_name")
    if not name or not schedule.flag("MIGKIT_SCHEDULED"):
        return None
    return get_hop(name)


def _debug_log(ctx):
    """The run's debug log, where the traceback of a stop is kept: the
    hop's reports where the command has a hop, else beside every hop's."""
    from . import config as _config
    name = ctx.params.get("hop_name")
    try:
        root = get_hop(name).report_dir() if name else _config.REPORTS
    except BaseException:  # noqa: BLE001 - a hop that does not load
        root = _config.REPORTS
    return Path(root) / "commands.log"


def _secrets(ctx):
    """The hop's passwords, for taking out of what is said."""
    name = ctx.params.get("hop_name") if ctx is not None else None
    if not name:
        return ()
    try:
        hop = get_hop(name)
        return (hop.source.password, hop.target.password)
    except BaseException:  # noqa: BLE001 - nothing more to hide then
        return ()


def _clean(text, secrets=(), raw=False):
    """Words that reach the operator: no program migkit drives named, and
    no password - the hop's own or one in an address; for a program's or
    a driver's own message (`raw`), the database's words where its log
    holds them."""
    from . import export
    from .wording import database_words, redact
    text = str(text)
    said = database_words(text) if raw else ""
    return export.words(redact([said or text], secrets))


def _stopped(e, ctx):
    """The exit code a command ends with for `e`, having said what
    stopped it: the one table every command's end reads."""
    import traceback

    from . import schedule, verdict
    from .lease import Held
    if isinstance(e, KeyboardInterrupt):
        console.print("[yellow]stopped - what was saved as it went is"
                      " kept, and the next run goes on from it[/yellow]")
        return verdict.EXIT["interrupted"]
    if isinstance(e, schedule.NotDue):
        return verdict.EXIT["done"]
    if isinstance(e, SystemExit):
        if e.code is None:
            return verdict.EXIT["done"]
        if isinstance(e.code, int) and not isinstance(e.code, bool):
            return e.code
        said = _clean(e.code, _secrets(ctx))
        if said:
            click.echo(said, err=True)
        if isinstance(e, verdict.Usage):
            return verdict.EXIT["usage"]
        if isinstance(e, (verdict.Refused, Held)):
            return verdict.EXIT["refused"]
        return verdict.EXIT["failed"]
    # anything else: the database's or the program's words, the traceback
    # to the debug log - nobody reads a traceback for a server's refusal
    where, secrets = _debug_log(ctx), _secrets(ctx)
    try:
        where.parent.mkdir(parents=True, exist_ok=True)
        with where.open("a") as f:
            f.write(time.strftime("%Y-%m-%dT%H:%M:%S") + " "
                    + _clean("".join(traceback.format_exception(
                        type(e), e, e.__traceback__)), secrets) + "\n")
    except OSError:
        where = None
    said = _clean(str(e), secrets, raw=True)
    if said == _clean(str(e), secrets):
        # no words of the database's in it: its last line, which is where
        # a program's or a driver's message says what went wrong
        said = (said.strip().splitlines() or [""])[-1]
    click.echo(f"{ctx.info_name} stopped: "
               + (said or type(e).__name__)[:600]
               + (f"\n(the details are in {where})" if where else ""),
               err=True)
    return verdict.EXIT["failed"]


class _Commands(click.Group):
    command_class = _Step


@click.group(cls=_Commands)
@click.version_option(__version__, "-V", "--version", prog_name="migkit")
@click.option("-q", "--quiet", is_flag=True,
              help="suppress per-table chatter and progress lines,"
                   " keep diffs, errors and summaries")
def main(quiet):
    """Database migration toolkit: prepare the target, move the data,
    watch the load, prove every row landed, and repair what did not.

    \b
    {engines}

    \b
    typical flow:
      migkit doctor                 tools + hops + connectivity
      migkit advise HOP             playbook for the hop's mover
      migkit schema HOP             target schema plan (or --convert/--migration)
      migkit check HOP              read-only, layered, exit 1 on diff
      migkit move HOP --mode full   resumable copy (cdc / full+cdc for streams)
      migkit watch HOP              live progress (--verify = re-check loop)
      migkit sync HOP --go          check + repair with state checkpoints
      migkit rollback HOP --db X    restore any saved state

    every check is read-only and rerunnable. every repair is dry-run
    unless --apply/--go, saves undo first, and converges to the same
    end state on re-run (safe to repeat). legacy command names (repair,
    replicate, tail, setup-target, ui, state, monitor, ...) still work."""
    global QUIET
    QUIET = quiet


def _engine_lines():
    """The engines of `migkit --help`, read off `engines.NAMES`: the
    lines written by hand named 7 engines and put ClickHouse and Oracle
    under the generic one while the code had 34 (audit LG16)."""
    from .engines import NAMES
    lines, line = [], "engines:"
    for name in (n for n in NAMES if n not in ("hetero", "generic")):
        if len(line) + 1 + len(name) > 72:
            lines.append(line)
            line = " " * 12
        line += " " + name
    # the docstring's lines keep their indent until click takes it off
    return "\n".join(lines + [line,
                              " " * 13 + "any two of them as a pair (hetero),"
                              " and any other SQL",
                              " " * 13 + "engine by its URL (generic)"])


main.help = main.help.replace("{engines}", _engine_lines())


def _hops_table(required=True):
    """Print the hop table.

    `required=False` for `doctor`: that is what you run *before* you have a
    configuration, so a missing one is a note, not a failure. Asking for the
    hop list directly still fails, because there is nothing to list.
    """
    try:
        hops = load_hops()
    except SystemExit as e:
        if required:
            raise
        console.print(f"[yellow]{e}[/yellow]\n")
        return
    if not hops:
        console.print("[yellow]no hops configured yet - migkit init"
                      "[/yellow]\n")
        return
    t = Table("hop", "engine", "source", "target", "service", "dbs")
    for name, hop in hops.items():
        t.add_row(name, hop.engine, hop.source.host or "-",
                  hop.target.host or "(not set)", hop.service or "-",
                  ",".join(map(str, hop.databases)) or "auto")
    console.print(t)


STARTER_CONFIG = """\
# migkit hops. A hop is one source -> target pair: two addresses, and
# migkit decides the rest (the engine, the port and the databases are read
# from the addresses; how many workers, how big a range, which way each
# table goes, from the servers).
#
#   migkit init            in a terminal: asks for the two addresses,
#                          connects, and writes the hop for you
#   migkit doctor          what this machine can do, and whether hops connect
#   migkit assess  my-hop  readiness before anything moves
#   migkit check   my-hop  read-only comparison, exit 1 on any difference
hops:
  my-hop:
    # scheme://user@host:port/database - postgres, mysql, mongodb, mssql,
    # oracle, redis, kafka, sqlite, clickhouse, ...; TLS in the query
    # (?sslmode=verify-full). No password in an address: `migkit init`
    # keeps it in this machine's keychain, or write a reference in its
    # place - postgres://app:env:SRC_PASSWORD@source.example.com/shop
    source: postgres://app@source.example.com:5432/shop
    target: postgres://app@target.example.com:5432/shop
    # what the hop is for: copy (left out) | cutover | verify | two-way |
    # keep-in-sync
    # goal: cutover
"""


@main.command()
@click.option("--here", is_flag=True,
              help="write ./conf/hops.yaml in this directory instead of the"
                   " user config directory")
@click.option("--force", is_flag=True, help="overwrite an existing file")
def init(here, force):
    """Set up a hop: in a terminal, asks for the two addresses and the
    passwords, connects, says what it found and what it will do, and
    writes the hop. Without one, reads MIGKIT_SOURCE and MIGKIT_TARGET
    (MIGKIT_SOURCE_PASSWORD, MIGKIT_TARGET_PASSWORD, MIGKIT_GOAL,
    MIGKIT_HOP), or writes a template to edit.

    Installed from a package there is no checkout to copy an example out of,
    so migkit writes the template itself. Mode 600, because what goes in it
    next can be a secret's reference.
    """
    import os
    import sys

    from .config import user_config_path
    path = (Path.cwd() / "conf" / "hops.yaml") if here else user_config_path()
    tty = sys.stdin.isatty() and sys.stdout.isatty()
    if tty or os.environ.get("MIGKIT_SOURCE"):
        from . import guide
        guide.run(_ask if tty else None, console.print, path, force)
        if not here:
            console.print("\nmigkit finds this file automatically."
                          " Override with MIGKIT_CONF=/path/to/hops.yaml")
        return
    if path.exists() and not force:
        raise SystemExit(f"{path} already exists (use --force to overwrite)")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(STARTER_CONFIG)
    path.chmod(0o600)
    console.print(f"wrote {path} (mode 600)")
    console.print("\nedit the endpoints, then:")
    console.print("  migkit doctor            # capabilities + connectivity")
    console.print("  migkit check my-hop      # read-only comparison")
    if not here:
        console.print(f"\nmigkit finds this file automatically."
                      " Override with MIGKIT_CONF=/path/to/hops.yaml")


def _ask(question, secret=False, default=""):
    """One question of the guide, in the terminal."""
    if secret:
        import getpass
        return getpass.getpass(f"{question}: ")
    return click.prompt(question, default=default, show_default=bool(
        default))


def _ask_change(question):
    """The offer of a change on the source, answered at the terminal."""
    return click.prompt(f"  {question}",
                        type=click.Choice(["yes", "no", "dba"]))


def _ask_terms(program, terms):
    """The one question `doctor --install` asks: whether the operator
    accepts the terms of a program that is not open source. It names the
    program - terms of a program nobody is told the name of cannot be
    accepted - and the licence, where to read it, what migkit uses it for
    and what happens if the answer is no."""
    console.print(f"\n[bold]{program}[/bold] is not open source: its terms"
                  f" are {terms.licence}.\n  read them at {terms.url}\n"
                  f"  migkit uses it for {terms.allows}.\n"
                  f"  declined: {terms.otherwise}.")
    return click.confirm(f"  accept the terms of {program}?", default=False)


@main.command()
@click.option("--install", is_flag=True,
              help="auto-install every missing external tool via the platform"
                   " package manager (brew/apt) - get a fresh machine ready")
def doctor(install):
    """What this machine can do, and whether the configured hops connect.

    Safe to run before anything is configured - that is the point of it.

    Every check/repair/move step migkit runs is a wrapper over a proven
    external program; `--install` pulls whichever are missing so any machine
    or teammate is one command from a full toolchain."""
    import os

    from . import tools as _tools
    bundle = os.environ.get("MIGKIT_DIAGNOSE", "").strip()
    if bundle:
        # a problem reported with this carries nothing to hand over
        # (`migkit.diagnostics`)
        from . import config as _config
        from . import diagnostics
        got = diagnostics.write(bundle, Path(_config.CONF), _config.REPORTS)
        console.print(f"diagnostics: {bundle} ({len(got)} files; passwords,"
                      " tokens, addresses' credentials and every finding's"
                      " values left out)")
    _hops_table(required=False)
    if install:
        import sys
        # asked only of a person at a terminal: an install with nobody
        # there accepts nothing, unless MIGKIT_ACCEPT_TERMS says so
        _tools.install_missing(lambda m: console.print(f"  {m}"),
                               ask=_ask_terms if sys.stdin.isatty() else None)
    t = Table("capability", "what it does", "status")
    mark = {"ready": "[green]ready[/green]",
            "reduced": "[yellow]reduced[/yellow]",
            "unavailable": "[red]unavailable[/red]"}
    short = []
    for name, what, state, missing in _tools.capabilities():
        t.add_row(name, what, mark[state])
        if state != "ready":
            short += missing
    console.print(t)
    said = _tools.schema_reading_note()
    if said:
        console.print(f"note: {said}")
    if short:
        console.print("\nto enable everything on this machine:"
                      " [bold]migkit doctor --install[/bold]")
        manual = _tools.by_hand(short)
        if manual:
            # nothing on this machine can install these, so the names are
            # the one thing that helps
            console.print("  it cannot install these here; they need"
                          f" installing by hand: {', '.join(manual)}")
    hops, raw = _doctor_config()
    failed = False
    if hops:
        # every hop at once, and each hop's two sides at once: a host that
        # does not answer costs its connect wait once, not once for every
        # side of every hop after it; what each says is kept to its hop
        with ThreadPoolExecutor(max_workers=min(8, len(hops))) as pool:
            said = list(pool.map(
                lambda item: _doctor_hop(item[0], item[1],
                                         raw.get(item[0]) or {}),
                hops.items()))
        for lines, bad in said:
            for line in lines:
                console.print(line)
            failed = failed or bad
    if failed:
        from .verdict import EXIT
        # a side that did not answer, or a connection that is not what the
        # hop asks for: said above, and the command ends as a stop - it
        # ended 0 whatever doctor found
        raise SystemExit(EXIT["failed"])


def _doctor_side(name, hop, eng, side, ep):
    """(lines, round trip ms or None, reached, failed) of one side."""
    from . import sizing
    from .guide import _looking_at
    out = []
    if not ep.configured():
        return [f"{_esc(name)} {side}: not configured"], None, False, False
    try:
        dbs = eng.databases() if side == "src" else None
        note = f"{len(dbs)} dbs" if dbs else "reachable"
        one = (hop.databases or [dbs[0] if dbs else ""])[0]
        # a reader by what the server says of itself, as `init` asks it
        # (`sizing.capacity`), on every engine that says
        e, key = _looking_at(eng, hop, "source" if side == "src"
                             else "target")
        replica = sizing.capacity(e, key, one if side == "src"
                                  else hop.target_db(one) if one
                                  and key == "src" else one).get("replica")
        role = {True: " [red]READER/read-only[/red]",
                False: " [dim]writer[/dim]"}.get(replica, "")
        out.append(f"{_esc(name)} {side}: [green]ok[/green]"
                   f" ({_esc(ep.host)}, {note}){role}")
        said, rtt = _link_said(eng, side, one,
                               "source" if side == "src" else "target")
        if said:
            out.append(f"  {said}")
        said = _leg_said(eng, side, one, ep)
        bad = said.startswith("[red]FAIL")
        if said:
            out.append(f"  {said}")
        said = _two_way_said(hop, eng, one) if side == "dst" else ""
        if said:
            out.append(f"  {_esc(said)}")
        return out, rtt, True, bad
    except Exception as e:  # noqa: BLE001 - said, and doctor ends failed
        out.append(f"{_esc(name)} {side}: [red]FAIL[/red]"
                   f" {_esc(_clean(e, (ep.password,), raw=True))}")
        return out, None, False, True


def _doctor_hop(name, hop, raw_hop):
    """(lines, failed) of one hop for doctor: one engine for all it asks,
    both sides asked at once."""
    from . import capabilities, hopkeys
    out, failed = [], False
    for _, line in hopkeys.said_by_doctor(hop, raw_hop):
        out.append(f"{_esc(name)}: [yellow]{_esc(line)}[/yellow]")
    try:
        eng = get_engine(hop)
    except Exception as e:  # noqa: BLE001 - said, and doctor ends failed
        out.append(f"{_esc(name)}: [red]FAIL[/red]"
                   f" {_esc(_clean(e, (hop.source.password, hop.target.password), raw=True))}")
        return out, True
    rtt, reached = {}, []
    with ThreadPoolExecutor(max_workers=2) as pool:
        sides = list(pool.map(
            lambda sd: (sd[0],) + _doctor_side(name, hop, eng, *sd),
            (("src", hop.source), ("dst", hop.target))))
    for side, lines, took, ok, bad in sides:
        out += lines
        rtt[side] = took
        if ok:
            reached.append(side)
        failed = failed or bad
    said = _placement_said(rtt, hop)
    if said:
        out.append(f"{_esc(name)}: {said}")
    if len(reached) == 2:
        try:
            _doctor_setup(name, hop, eng, out.append)
        except Exception as e:  # noqa: BLE001 - a probe never fails it
            why = (str(e).splitlines() or [type(e).__name__])[-1]
            out.append(f"{_esc(name)}: what the servers allow could not"
                       f" be asked - {_esc(_clean(why, raw=True)[:160])}")
    # what the commands will refuse on this engine, from the table they
    # refuse by (`capabilities`) - not from the programs installed here
    cannot = [capabilities.CAPABILITIES[c] for c in capabilities.CAPABILITIES
              if capabilities.unavailable(hop.engine, c)]
    known = not str(capabilities.unavailable(hop.engine, "schema")
                    or "").startswith("migkit does not know")
    if cannot and known:
        out.append(f"{_esc(name)}: not on {_esc(hop.engine)} hops - "
                   + "; ".join(cannot))
    for line in _decisions_said(hop):
        out.append(f"{_esc(name)}: {_esc(line)}")
    # read from what the freeze recorded, so it is said even when the
    # target cannot be reached right now
    from . import cutover, freeze, sourcechange
    for line in cutover.said_by_doctor(hop):
        out.append(f"{_esc(name)} [yellow]cutover[/yellow]: {_esc(line)}")
    for line in sourcechange.said_by_doctor(hop):
        out.append(f"{_esc(name)} [yellow]source[/yellow]: {_esc(line)}")
    if hop.source.configured() and hop.target.configured():
        try:
            _, what = cutover.summary(hop, eng,
                                      hop.databases or eng.databases())
            out.append(f"{_esc(name)}: {_esc(what)}")
        except Exception:  # noqa: BLE001 - a probe never fails doctor
            pass
    for d in (hop.databases or []):
        held = freeze.state(hop, d)
        if held and held.get("roles"):
            out.append(f"{_esc(name)} dst: {_esc(d)} is kept from the"
                       " application's writes -"
                       f" {_esc(', '.join(sorted(held['roles'])))};"
                       " tearing the stream down at cutover, or"
                       " rollback --apply, gives them back")
    return out, failed


def _doctor_config():
    """(hops, their mappings as written) for doctor, which says what is
    wrong with a configuration rather than stopping on it: an error was
    said by the hop table above, and only the warnings are left to say."""
    import yaml

    from . import config as _config
    from . import hopkeys
    path = Path(_config.CONF)
    if not path.exists():
        return {}, {}
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        console.print(f"[red]{path} is not YAML migkit can read[/red]: {e}")
        return {}, {}
    try:
        hops = load_hops()
    except SystemExit:
        return {}, {}
    for p in hopkeys.check(data):
        console.print(f"[yellow]{p}[/yellow]")
    return hops, (data.get("hops") or {}) if isinstance(data, dict) else {}


def _decisions_said(hop):
    """What each database's last plan stood on (`factbook.account`), from
    the decision record: how many facts were known, measured, predicted,
    and which of the plan's factors nobody knew - the ones that would
    change it once known."""
    from . import decide, factbook
    out, seen = [], set()
    for e in reversed(decide.recorded(hop)):
        facts = (e.get("detail") or {}).get("facts")
        work = e.get("work") or ""
        if not isinstance(facts, dict) or work in seen:
            continue
        seen.add(work)
        out.append(f"{work}: the last plan ({e.get('at', '')}) stood on"
                   f" {factbook.said(facts)}")
    return out[::-1]


def _doctor_setup(name, hop, eng, say=None):
    """Who runs each side, and whether what the hop's goal needs of the
    two servers is there - asked again each time, the DBA's script
    written again for what is still missing, or taken away."""
    from . import clouds, dba
    from .guide import _looking_at, record
    say = say or console.print
    name = _esc(name)
    wheres = {}
    for side in ("source", "target"):
        e, key = _looking_at(eng, hop, side)
        ep = hop.source if side == "source" else hop.target
        wheres[side] = clouds.of(e, key, ep)
        if wheres[side].cloud:
            say(f"{name} {side}: {_esc(wheres[side].label)} (from"
                f" {_esc(' and '.join(wheres[side].evidence))})")
    record(hop, {s: {"where": w.as_dict()} for s, w in wheres.items()})
    found = dba.needs(hop, eng, None, wheres)
    if not found:
        return
    written = dba.write(hop, eng, found, wheres)
    lack = dba.missing(found)
    goal = hop.goal or "copy"
    if not lack:
        say(f"{name}: [green]the servers allow everything goal"
            f" {goal} needs[/green]")
    for n in lack:
        say(f"{name} {n.side}: [red]missing[/red] {_esc(n.what)}"
            f" ({_esc(n.now)})")
    for side, p in written.items():
        say(f"  what the DBA must run on the {side}: {_esc(p)}")
    for n in (x for x in found if x.ok is None):
        say(f"{name} {n.side}: {_esc(n.what)} - could not be asked"
            f" ({_esc(n.now)})")


def _placement_said(rtt, hop=None):
    """Where this machine is between the two sides, where both round
    trips were measured (`legs.placement`): far from both means each row
    crosses a wide network twice, once in and once out; beside the source
    and far from the target, that every write waits on the link."""
    from . import legs
    direct, seen, may = False, None, False
    if hop is not None:
        from . import movers
        from .engines import ALIASES
        may = legs.allowed(hop)
        seen = legs.servers_seen(hop)
        if may:
            try:
                direct = legs.direct(movers.for_hop(
                    hop, ALIASES.get(hop.engine, hop.engine))[0])
            except Exception:  # noqa: BLE001 - a probe never fails doctor
                direct = False
    said = legs.placement(rtt.get("src"), rtt.get("dst"), seen, direct,
                          False, may)
    return f"[yellow]placement[/yellow]: {said}" if said else ""


def _leg_said(eng, side, db, ep):
    """Whether the connection to a side is encrypted, as the database
    itself says of it, and what carries it: nothing is said of a side on
    this machine, where no row crosses a network."""
    from . import tls
    if tls.on_this_machine(ep):
        return ""
    # whether the certificate is checked is migkit's side of the
    # connection, which no server can say: a failure every run until it is
    try:
        pos = tls.posture(ep, eng.ENGINE_FAMILY or eng.CANON_ENGINE)
    except Exception:  # noqa: BLE001 - a probe never fails doctor
        pos = None
    if pos and pos["state"] in ("unverified", "plain"):
        return (f"[red]FAIL[/red] connection NOT"
                f" {'verified' if pos['state'] == 'unverified' else 'encrypted'}"
                f": {pos['detail']}")
    try:
        got = eng.leg_encryption(side, db)
    except Exception:  # noqa: BLE001 - a probe never fails doctor
        got = None
    if got is None:
        return (f"verified: {pos['detail']}" if pos
                and pos["state"] == "verified" else "")
    if got["encrypted"]:
        # the engine says what this connection used where it can; the
        # hop's own settings speak only for an engine that cannot
        checked = got.get("verified")
        if checked is None:
            checked = bool(pos and pos["state"] == "verified")
        left = got.get("unverified") or []
        return (f"encrypted: {got['how']}"
                + (", the certificate verified" if checked else "")
                + (f"; not verified on: {', '.join(left)}" if left else ""))
    through = (ep.options or {}).get("tunnel_to")
    if through and tls.tunnel_kind(ep) == "ssh":
        return (f"not encrypted by the database; carried through the"
                f" hop's SSH tunnel to {through}, which is what protects it"
                " on the way")
    if through:
        return (f"[yellow]not encrypted by the database[/yellow]; carried"
                f" by the tunnel's own command to {through}, which migkit"
                " cannot vouch for. Ask the server for TLS, or carry it"
                " through an SSH tunnel")
    return ("[yellow]not encrypted[/yellow]: every row crosses the network"
            " as it is. Ask the server for TLS and verify its certificate"
            " (sslmode verify-full, ssl-mode VERIFY_IDENTITY), or carry the"
            " connection through a tunnel")


def _two_way_said(hop, eng, db):
    """How a two-way hop's tail marks what it applies on the target, and
    what that leaves there (`marks.said`); nothing for any other hop."""
    from . import marks, twoway
    if twoway.settings(hop) is None or not db:
        return ""
    try:
        tailer = eng if hasattr(eng, "dst_engine") else _tail_pair(eng)
        if tailer is None:
            return ""
        return marks.said(tailer.dst_engine, _tail_token(hop, db), db)
    except Exception:  # noqa: BLE001 - a probe never fails doctor
        return ""


def _link_said(eng, side, db, name):
    """`Engine.link_probe` in a line, and the round trip it measured (ms,
    or None)."""
    fn = getattr(eng, "link_probe", None)
    if fn is None:
        return "", None
    try:
        got = fn(side, db)
    except Exception:  # noqa: BLE001 - a probe never fails doctor
        return "", None
    return _link_line(got, name), got.get("rtt_ms")


def _link_line(got, name):
    stalled = got.get("stalled")
    if stalled:
        way, size = stalled
        if way == "connecting":
            return (f"[red]link: STALLED[/red] - its port answers, and"
                    f" signing in to the {name} never finishes: the first"
                    " full-sized packets of the handshake do not arrive. A"
                    " path that drops large packets (an MTU black hole - a"
                    " VPN or tunnel set too high): lower that tunnel's MTU"
                    " (1280 is safe) or clamp TCP MSS on it")
        what = ("replies from" if way == "replies" else "requests to")
        return (f"[red]link: STALLED[/red] - {what} the {name} of"
                f" {size:,} bytes or more never arrive, smaller ones do. A"
                " path that drops large packets (an MTU black hole - a VPN"
                " or tunnel set too high): lower that tunnel's MTU (1280 is"
                " safe) or clamp TCP MSS on it. A copy over this link"
                " hangs")
    if got.get("tcp"):
        return f"[red]link: its port does not answer[/red] ({got['tcp']})"
    parts = []
    if got.get("rtt_ms") is not None:
        parts.append(f"{got['rtt_ms']} ms round trip")
    if got.get("replies"):
        parts.append(f"replies up to {got['replies']:,} bytes and requests"
                     f" up to {got.get('requests', 0):,} bytes arrive")
    if got.get("mb_s"):
        parts.append(f"{got['mb_s']} MB/s")
    return ("link: " + ", ".join(parts)) if parts else ""


@main.command()
@click.argument("hop_name")
def advise(hop_name):
    """Show the migration playbook for this hop's service."""
    hop = get_hop(hop_name)
    pb = advisors.playbook(hop.service or "native")
    if not pb:
        raise SystemExit(f"no playbook for service '{hop.service}',"
                         f" have: {', '.join(advisors.PLAYBOOKS)}")
    console.print(pb)


@main.command()
@click.argument("hop_name")
def assess(hop_name):
    """Premigration readiness check: CDC prerequisites, risky tables,
    encoding, extensions, accounts. Run before starting any mover."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    # everything the hop's goal needs, in one place, each with its fix
    # (`ready`): the servers, what they allow, the target, the resources,
    # the goal's own checks, the rehearsal and its forecast, and what a
    # person does by hand and when
    from . import ready, rehearsal
    rehearsal.mark(hop)
    got = ready.items(hop, eng)
    items = [i.row() for i in got]
    color = {"pass": "green", "warn": "yellow", "fail": "red"}
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for it in items:
        counts[it["level"]] = counts.get(it["level"], 0) + 1
        c = color.get(it["level"], "white")
        line = (f"[{c}]{it['level']:5s}[/{c}] {it['scope']:20s}"
                f" {it['item']}  {it['detail']}")
        if it["level"] == "pass":
            chat(line)
        else:
            console.print(line)
    (hop.report_dir() / "assess.json").write_text(
        json.dumps(items, indent=1))
    hand = [i for i in got if i.by_hand and i.level != "pass"]
    if hand:
        # what a person still does, each at its moment - the plan says it
        # again then; there is no other document to keep
        console.print("\nby hand:")
        for i in hand:
            console.print(f"  {i.when}: {i}")
    _say_goal(hop)
    console.print(f"\n{counts['pass']} pass, {counts['warn']} warn,"
                  f" {counts['fail']} fail")
    if counts["fail"]:
        raise SystemExit(1)


def _say_goal(hop, mode_line=""):
    """What the hop's goal - and the mode about to run - does on the
    source and the target and guarantees, as `hopkeys` writes it once for
    `init`, `assess` and every dry run; a hop cut over without `cutover:`
    is told every time what it does by hand and what turning it on adds."""
    from . import hopkeys
    console.print("\n[bold]what each side sees[/bold]")
    if mode_line:
        console.print(f"  {mode_line}")
    for line in hopkeys.explain_goal(hop):
        console.print(f"  {line}")


def _setup_target(hop_name, db):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    dbs = [db] if db else eng.databases()
    console.print(f"target preparation for {hop_name}"
                  f" (dry-run, run these yourself in order):\n")
    for d in dbs:
        console.print(f"[bold]{_esc(d)}[/bold]")
        for step in eng.setup_target_plan(d):
            say_raw(f"  {step}")
        console.print("")
    console.print("after this: migkit check --only schema, then start the mover"
                  " (see migkit advise)")


def _convert_schema(hop_name, db, do_apply):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "convert_ddl"):
        raise SystemExit("--convert is for hetero hops,"
                         " same-engine hops use migkit schema HOP")
    dbs = [db] if db else eng.databases()
    _guard_write(hop, eng, "creating the target's schema", dbs, do_apply)
    for d in dbs:
        objects = eng.converted_objects(d)
        stmts = [s for _, s in objects]
        out = hop.report_dir(d) / "converted-schema.sql"
        out.write_text("\n\n".join(stmts) + "\n")
        console.print(f"[bold]{_esc(d)}[/bold]: {len(stmts)} statements ->"
                      f" {_esc(out)}")
        for stmt in stmts[:3]:
            chat(f"  {_esc(stmt.splitlines()[0])} ...")
        left = [s for s in stmts if s.startswith("--")]
        for s in left:
            # the first line says why; a version kept under it, commented
            # out, is in the file for a person to start from
            console.print(f"  [yellow]{_esc(s.splitlines()[0][3:])}"
                          "[/yellow]")
        from .storedcode import effort
        cost = effort.said(hop, d)
        if cost:
            console.print(f"  effort: {_esc(cost)}")
        if do_apply:
            have = eng.target_names(d)
            # the target's own, whatever it holds: `check` asks it the
            # same questions as the source instead
            kept = [n for n, st in objects if not st.startswith("--")
                    and n in have]
            todo = [(n, st) for n, st in objects if not st.startswith("--")
                    and n not in have]
            made, failed, whole = _create_converted(eng, d, todo)
            _record_built(hop, eng, d, made)
            _changelog(hop, {"op": "convert-schema", "db": d,
                             "n": len(made)})
            console.print(f"  [green]{len(made)} created on the"
                          " target[/green]")
            if kept:
                console.print(f"  already on the target, left as they are:"
                              f" {_esc(', '.join(kept))}")
            if failed:
                raise SystemExit(
                    f"{len(failed)} could not be created on the target"
                    + ("; nothing else was either - the schema is created"
                       " in one transaction, whole or not at all" if whole
                       else "") + ":\n  " + "\n  ".join(failed))
    if not do_apply:
        console.print("\ndry-run, review the file then add --apply")


def _create_converted(eng, d, todo):
    """([name made], [why each failed], whether it was one transaction):
    the converted statements run on the target. A PostgreSQL target takes
    them in one transaction on one connection, each under a savepoint so
    every failure is named, and keeps none of them where one fails - it
    took each on a connection of its own and kept half a schema. A target
    whose DDL commits as it runs (MySQL) takes them one by one."""
    target = eng.dst_engine

    def said(e):
        return (str(e).strip().splitlines() or [""])[0]
    if getattr(target, "CANON_ENGINE", "") != "postgres" or \
            not hasattr(target, "_conn"):
        made, failed = [], []
        for name, stmt in todo:
            try:
                target.neutral_create_code("dst", d, stmt)
                made.append(name)
            except Exception as e:  # noqa: BLE001 - listed by the caller
                failed.append(f"{name}: {said(e)}")
        return made, failed, False
    made, failed = [], []
    conn = target._conn("dst", target._d("dst", d))
    try:
        with conn.cursor() as cur:
            for i, (name, stmt) in enumerate(todo):
                cur.execute(f"savepoint migkit_convert_{i}")
                try:
                    cur.execute(stmt)
                    made.append(name)
                except Exception as e:  # noqa: BLE001 - listed, then undone
                    cur.execute(f"rollback to savepoint migkit_convert_{i}")
                    failed.append(f"{name}: {said(e)}")
        if failed:
            conn.rollback()
            made = []
        else:
            conn.commit()
    finally:
        conn.close()
    return made, failed, True


def _record_built(hop, eng, d, made):
    """The tables `convert-schema --apply` made, recorded as `move`
    records those it builds (`built-tables.json`), so the copy after it
    adds their indexes once the rows are in (`finish_created`): a table
    it made was no longer missing when the move came, and its indexes
    were lost for good."""
    if not made:
        return
    try:
        leaves = {eng._leaf(t): str(t)
                  for t in eng.src_engine.neutral_tables("src", d)}
    except Exception:  # noqa: BLE001 - nothing to record then
        return
    built = {n: leaves[n] for n in made if n in leaves}
    if not built:
        return
    record = hop.report_dir(d) / "built-tables.json"
    try:
        have = json.loads(record.read_text())
    except (OSError, ValueError):
        have = {}
    have.update(built)
    record.write_text(json.dumps(have))


def _gen_migration(hop_name, db, out):
    from pathlib import Path

    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "migration_pair"):
        raise SystemExit("--migration supports postgres and mysql hops")
    outdir = Path(out)
    outdir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d%H%M%S")
    made = 0
    for d in ([db] if db else eng.databases()):
        fwd, undo = eng.migration_pair(d)
        if fwd is None:
            raise SystemExit(
                "generating migration DDL needs a component that is not"
                " installed on this machine: migkit doctor --install"
                " puts it in place")
        if not fwd:
            console.print(f"{d}: schemas already in sync, nothing to generate")
            continue
        vf = outdir / f"V{ts}__sync_{d}.sql"
        uf = outdir / f"U{ts}__sync_{d}.sql"
        vf.write_text(fwd + "\n")
        uf.write_text((undo or "-- no automatic undo available") + "\n")
        made += 1
        console.print(f"{d}: [green]{vf}[/green] (+ undo {uf.name},"
                      f" {len(fwd.splitlines())} lines)")
    if made:
        console.print("review the files, commit to git, apply when ready")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="single database")
@click.option("--convert", "do_convert", is_flag=True,
              help="transpile source DDL to the target dialect (hetero hops)")
@click.option("--migration", "do_migration", is_flag=True,
              help="generate versioned V/U migration files from the live"
                   " schema diff")
@click.option("--apply", "do_apply", is_flag=True,
              help="with --convert: execute the DDL on target")
@click.option("--out", default="migrations",
              help="with --migration: output directory")
def schema(hop_name, db, do_convert, do_migration, do_apply, out):
    """Target schema workflows: preparation plan (default), cross-engine
    DDL conversion (--convert), versioned migration files (--migration)."""
    if do_convert:
        return _convert_schema(hop_name, db, do_apply)
    if do_migration:
        return _gen_migration(hop_name, db, out)
    return _setup_target(hop_name, db)


def _drill(hop_name, db, table, limit):
    if not db or not table:
        raise SystemExit("--drill needs --db and --table (schema.table)")
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "fetch_sample_df"):
        raise SystemExit("--drill supports postgres and mysql hops")
    try:
        import datacompy  # noqa: F401 - what the explanation is built on
    except ImportError:
        raise SystemExit(
            "the row-by-row drilldown needs a component that is not"
            " installed here: migkit doctor --install puts it in place")
    a = eng.fetch_sample_df("src", db, table, limit)
    b = eng.fetch_sample_df("dst", db, table, limit)
    t = table.split(".", 1)[-1]
    if hasattr(eng, "_pk_cols"):
        keys = eng._pk_cols(db, t)
    else:
        sch, tbl = table.split(".", 1) if "." in table else ("public", table)
        keys = [c for c in eng._psql("src", db,
                "select a.attname from pg_index i join pg_attribute a"
                " on a.attrelid = i.indrelid and a.attnum = any(i.indkey)"
                f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
                " and i.indisprimary").splitlines() if c]
    if not keys:
        raise SystemExit(f"{table} has no primary key for joining")
    # compared as shown: a masked column's values are tokens, equal where
    # the values are, so the rows still line up and still differ. In
    # migkit's words: the comparison's own report opens with its name
    from . import second_reader
    for line in second_reader.explain(a, b, keys, hop, table,
                                      limit=max(limit, 1), per_column=10):
        console.print(line, markup=False, highlight=False)
    # the explanation shows the values as they are, which for an invisible
    # difference is two identical-looking strings. The count was never the
    # hard part.
    extra = eng._invisible_section(a, b, keys, table)
    if extra:
        console.print(extra)


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="single database")
@click.option("--table", default="", help="single table (schema.table)")
@click.option("--only", default="",
              help="comma list: schema,counts,autoinc,data,deep,params")
@click.option("--deep", "do_deep", is_flag=True,
              help="add deep checks: fk orphans, disabled triggers, column"
                   " drift, matview/grants parity, boundary freshness")
@click.option("--drill", is_flag=True,
              help="column-level sample diff for one table"
                   " (needs --db and --table)")
@click.option("--limit", default=1000, help="with --drill: sample rows")
@click.option("--consistent", is_flag=True,
              help="checksum every table of a db inside one repeatable-read"
                   " transaction per side, with the src LSN captured as the"
                   " convergence fence (postgres)")
@click.option("--workers", default=0, help="parallel databases, default from conf")
@click.option("--resume", is_flag=True, help="skip checks already ok in last summary")
@click.option("--exclude", default="", help="comma list of db or db.schema.table"
                                            " patterns to skip, added to the hop's")
def check(hop_name, db, table, only, do_deep, drill, limit, consistent,
          workers, resume, exclude):
    """Read-only validation of target vs source. Never writes to either side.

    When counts and data both run, row counts ride along with the checksum
    query, so nothing is scanned twice. Suspect rows are proven in-flight
    or real via the replication fence (LSN-based) instead of guesswork.

    Ends 0 where the two are the same, 1 where they differ, 3 where the
    run could not say (an answer not taken, nothing compared, a narrowed
    run) - `verdict.json` says which and why."""
    if drill:
        return _drill(hop_name, db, table, limit)
    hop = get_hop(hop_name)
    _require_configured(hop)
    got, _ = _check(hop, db, table, only, do_deep, consistent, workers,
                    resume, exclude)
    if got.exit:
        raise SystemExit(got.exit)


def _checks_for(eng, only, do_deep=False):
    """(the checks a run takes, every check this engine has): `--only`'s,
    each one this engine has - a name it does not have is said as a usage
    error, where it was dropped and a typo compared nothing and read
    green - or the whole battery, the deep checks and the server's
    settings with it."""
    from .verdict import Usage
    allowed = list(dict.fromkeys(list(eng.checks) + ["deep", "params"]
                                 + list(eng.planned_checks())))
    if only:
        asked = [c.strip() for c in only.split(",") if c.strip()]
        odd = [c for c in asked if c not in allowed]
        if odd or not asked:
            raise Usage(f"--only {', '.join(odd) or only}: not a check of"
                        f" this hop's engine; its checks are"
                        f" {', '.join(allowed)}")
        checks = list(dict.fromkeys(asked))
    else:
        checks = list(allowed)
    if do_deep and "deep" not in checks:
        checks.append("deep")
    return checks, allowed


def _loud(line):
    """Whether a line a check streams is one `-q` keeps: what differs,
    failed or asks to be read, in whatever case the engine says it."""
    import re
    return re.search(r"(?i)\b(diff|differ|error|warn|fail|missing|extra"
                     r"|mismatch|stale)", str(line)) is not None


def _merged(eng, checks, table):
    """Whether the row counts ride along with the checksum pass."""
    return ("counts" in checks and "data" in checks and not table
            and getattr(eng, "counts_from_data", False))


def _position(eng, d):
    """How many changes the source has made in `d`, and over how long, as
    it counts them (`Engine.changes_made`) - what a resumed answer is held
    to - or None where it keeps no count."""
    try:
        got = eng.changes_made("src", d)
    except Exception:  # noqa: BLE001 - no count, nothing reused as held
        return None
    return [int(got[0]), float(got[1])] if got else None


def _held_since(then, now):
    """Whether the source made no change between two `_position`s: the
    same count, and no reset of it between (the time it covers grew)."""
    return bool(then and now) and int(then[0]) == int(now[0]) \
        and float(now[1]) >= float(then[1])


def _said_row(r):
    st = str(r.get("status", ""))
    color = {"ok": "green", "diff": "yellow", "error": "red",
             "warn": "yellow"}.get(st, "white")
    console.print(f"[{color}]{_esc(str(r.get('check', ''))):8s}"
                  f" {_esc(r.get('scope', ''))}: {_esc(st.upper())}"
                  f"[/{color}] {_esc(r.get('detail', ''))}")


def run_checks(hop, eng, dbs, checks, table="", consistent=False,
               workers=1, prev=None, prev_at=None, whole=True, show=None,
               timer=None):
    """Every check of `checks` over every database of `dbs`, as many
    databases at once as `workers`: ([answer], {db: {check: seconds}}).
    The one run of the checks `check`, `watch --verify` and `sync` share -
    each database's source shape read before and after it, an answer a
    DDL overtook taken back (`verdict.mark_stale`), the server's settings
    compared once where they are the server's, a whole battery's time kept
    for the next plan. A check that raises is an `error` answer, never a
    database left out.

    `prev`: {(check, db): answer} of the run being resumed, from
    `prev_at`; an `ok` of it is reused, marked `reused_from`, and held
    (`reuse_held`) only where the source has made no change since it was
    taken (`_position`) - it read as fresh however old it was. An answer
    taken under another scope than the hop's now is never reused, and
    every answer carries the scope it was taken under
    (`Hop.scope_fingerprint`). `show(answer)` as each database's come in.
    Ctrl-C lets the databases in flight end, says so, and starts no
    more."""
    from . import drift, traces
    from . import verdict as _verdict
    from .engines.base import Engine
    scope = (hop.scope_fingerprint()
             if hasattr(hop, "scope_fingerprint") else None)
    prev = {k: r for k, r in (prev or {}).items()
            if not (scope and hop.scope_changed(r))}
    merge = _merged(eng, checks, table)
    local_map = {}
    for d in dbs:
        local = list(checks)
        if merge and (prev.get(("data", d)) or {}).get("status") != "ok":
            local.remove("counts")
        local_map[d] = local
    timings = {}
    total_jobs = sum(len(v) for v in local_map.values())
    done_jobs = 0

    def stream(d):
        def s(line):
            if QUIET and not _loud(line):
                return
            console.print(f"  {_esc('[' + str(d) + ']')} {_esc(line)}")
        return s

    from . import archive, consolidate, rehearsal, subset

    def run_db(d):
        if subset.settings(hop) is not None:
            # a copy for testing holds the rows chosen, not the source's
            # every row: it is proved against what it was to hold
            return [r.__dict__ for r in subset.check(hop, eng, d)]
        for goal in (consolidate, archive):
            if goal.settings(hop) is not None:
                # the target holds other sources' rows, or rows the source
                # no longer has: each is proved against what it holds
                return [r.__dict__ for r in goal.check(hop, eng, d)]
        out = []
        began = time.time()
        position = _position(eng, d)
        # the source's shape before and after this database's checks, where
        # it is one query: an answer a DDL overtook is taken back, and a
        # resumed run reuses nothing the source has changed under since
        watch = (drift.reader(eng)
                 if set(local_map[d]) & set(_verdict.TAKEN_ACROSS) else None)
        before = drift.shape(watch, "src", d) if watch else None
        shape_file = hop.report_dir(d) / "check-shape.json"
        reuse = dict(prev)
        if prev and before is not None:
            try:
                saved = {t: [tuple(c) for c in cols] for t, cols in
                         json.loads(shape_file.read_text()).items()}
            except (OSError, ValueError):
                saved = None
            since = drift.changes(saved, before)
            if since:
                reuse = {}
                console.print(f"  {_esc('[' + str(d) + ']')} the source's"
                              " schema changed since the run being resumed"
                              " (" + _esc("; ".join(since[:3]))
                              + ("; ..." if len(since) > 3 else "")
                              + "), so nothing of it is reused")
        for c in local_map[d]:
            old = reuse.get((c, d)) or {}
            if old.get("status") == "ok":
                was = old.get("position")
                out.append({"check": c, "scope": d, "status": "ok",
                            "detail": "resume: reused, was ok"
                            + ("" if _held_since(was, position) else
                               " - the source may have changed since"),
                            "reused_from": old.get("reused_from")
                            or prev_at or "an earlier run",
                            "position": was,
                            "reuse_held": _held_since(was, position)})
                continue
            if (c == "params" and d != dbs[0]
                    and not eng.SETTINGS_PER_DATABASE
                    and type(eng).check_params is not Engine.check_params):
                # the server's settings, not this database's: compared once,
                # rather than the same finding repeated for every database
                out.append({"check": c, "scope": d, "status": "skip",
                            "detail": "server settings are the same for every"
                                      f" database; compared under {dbs[0]}"})
                continue
            fn = getattr(eng, f"check_{c}")
            t0 = time.monotonic()
            with traces.span(f"check {c}", db=d, check=c) as span:
                try:
                    if c == "data":
                        kw = {"stream": stream(d)}
                        if merge and "counts" not in local_map[d]:
                            kw["with_counts"] = True
                        if consistent and hasattr(eng, "_fast_consistent"):
                            kw["consistent"] = True
                        rs = [r.__dict__ for r in fn(d, table or None, **kw)]
                    else:
                        rs = [r.__dict__ for r in fn(d)]
                except Exception as e:
                    rs = [{"check": c, "scope": d, "status": "error",
                           "detail": _clean(e, (hop.source.password,
                                                hop.target.password),
                                            raw=True)}]
                traces.answered(span, rs)
            timings.setdefault(d, {})[c] = round(time.monotonic() - t0, 3)
            if position is not None:
                for r in rs:
                    r.setdefault("position", position)
            out.extend(rs)
        if before is not None:
            after = drift.shape(watch, "src", d)
            moved = drift.changed_tables(before, after)
            if moved:
                said = drift.changes(before, after)
                _verdict.mark_stale(out, d, moved, "; ".join(said[:3])
                                    + ("; ..." if len(said) > 3 else ""))
                from . import notify
                notify.drift(hop, d, said, lambda m: console.print(
                    f"[yellow]{_esc(m)}[/yellow]"))
            if after is not None:
                shape_file.parent.mkdir(parents=True, exist_ok=True)
                shape_file.write_text(json.dumps(after))
        if whole and not any(r.get("reused_from") for r in out):
            # the whole battery over the whole database: its time is the
            # check phase of the next plan, this hop's or the one it
            # rehearses (`rehearsal.record_check`)
            rehearsal.record_check(hop, eng, d, time.time() - began)
        # held to the changes the hop carries before they are said or
        # kept: rows it keeps are a note (`verdict.hold`)
        return _verdict.hold(hop, _scenario_checks(hop, eng, d, out, table,
                                                   checks))

    results = []
    pool = ThreadPoolExecutor(max_workers=max(1, int(workers or 1)))
    futs = {pool.submit(run_db, d): d for d in dbs}
    try:
        for fut in as_completed(futs):
            d = futs[fut]
            rs = fut.result()
            if scope:
                for r in rs:
                    r.setdefault("scope_fingerprint", scope)
            results.extend(rs)
            done_jobs += len(local_map[d])
            for r in rs:
                if show is not None:
                    show(r)
            if timer is not None:
                chat(f"  progress {done_jobs}/{total_jobs},"
                     f" elapsed {human_secs(timer.elapsed())},"
                     f" eta {timer.eta(done_jobs, total_jobs)}")
    except KeyboardInterrupt:
        running = sum(1 for f in futs if f.running())
        if running:
            console.print(f"[yellow]stopping: the checks of the {running}"
                          " databases in flight end first, and no other"
                          " starts[/yellow]")
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return results, timings


def _check(hop, db, table, only, do_deep, consistent, workers, resume,
           exclude, hints=True):
    """`check`: the checks run, their answers kept (`summary.json`,
    `verdict.json`, the report), said and told - and what they add up to
    (`verdict.decide`, the one rule), with the answers, for the caller to
    end on. `hints`: the next steps said where they differ."""
    from . import verdict as _verdict
    hop_name = hop.name
    if exclude:
        hop.exclude = list(hop.exclude) + [p.strip() for p in exclude.split(",") if p.strip()]
    eng = get_engine(hop)
    checks, allowed = _checks_for(eng, only, do_deep)
    dbs = [db] if db else eng.databases()
    if workers:
        # asked for on the command line: a ceiling, as a hop's own is
        hop.workers, hop.workers_set = int(workers), True
    if dbs:
        from . import sizing
        sizing.fit(hop, eng, dbs[0], lambda m: console.print(f"  {_esc(m)}"))
    else:
        console.print("[yellow]the source lists no database this hop"
                      " covers, so nothing is compared - the user migkit"
                      " signs in as may not see them[/yellow]")
    workers = hop.workers
    summary_path = hop.report_dir() / "summary.json"
    prev, prev_at = {}, None
    if resume and summary_path.exists():
        prev = {(r["check"], r["scope"]): r
                for r in json.loads(summary_path.read_text())}
        prev_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(
            summary_path.stat().st_mtime))
        # an answer is about the scope it was taken under: one taken under
        # another is taken again, whatever the source's schema did since
        other = [r for r in prev.values() if hop.scope_changed(r)]
        if other:
            prev = {k: r for k, r in prev.items()
                    if not hop.scope_changed(r)}
            console.print(f"  the hop's scope changed since the run being"
                          f" resumed, so its {len(other)} answers taken"
                          " under the scope it had then are taken again")
    timer = Timer()
    from . import rehearsal
    rehearsal.mark(hop)
    console.print(f"checking {_esc(hop_name)}: {len(dbs)} dbs x {checks},"
                  f" {workers} workers"
                  + (" (counts merged into the checksum pass)"
                     if _merged(eng, checks, table) else ""))
    if getattr(hop, "include", None):
        # the verdict is about what the hop takes; said before it is
        console.print(f"  within the hop's scope: it takes only"
                      f" {len(hop.include)} names"
                      + (f" and leaves out {len(hop.exclude)}"
                         if hop.exclude else "")
                      + " (the report lists them)")
    for line in _verdict.hop_scope(hop).get("changes_carried", []):
        # a target that keeps what the source deletes is held to that
        console.print(f"  changes carried: {_esc(line)}")
    results, timings = run_checks(
        hop, eng, dbs, checks, table, consistent, workers, prev, prev_at,
        whole=not table and not only, show=_said_row, timer=timer)

    if only and summary_path.exists():
        try:
            prev_all = json.loads(summary_path.read_text())
        except ValueError:
            prev_all = []

        def keep(r):
            # the answers of checks this run did not take, and of the
            # databases it did not look at - by the database the answer is
            # about, so `--db shop` keeps `shop_archive`'s
            if r.get("check") not in checks:
                return True
            return bool(db) and _verdict.db_of(r.get("scope", "")) != db

        # only answers taken under the scope the hop has now: one taken
        # under another stood beside this run's as if it were about the
        # same tables, rows and columns
        other = [r for r in prev_all if keep(r) and hop.scope_changed(r)]
        if other:
            console.print(f"  {len(other)} answers of the last check were"
                          " taken under another scope than the hop's now"
                          " and are not kept beside this run's - a whole"
                          " check takes them again")
        results = [r for r in prev_all if keep(r)
                   and not hop.scope_changed(r)] + results
    was_same = _verdict.unchanged_since(hop, results)
    summary_path.write_text(json.dumps(results, indent=1, default=str))
    from . import session
    (hop.report_dir() / "check-timings.json").write_text(json.dumps(
        {"session": session.current(), "at": time.strftime("%F %T"),
         "databases": timings}, indent=1))
    # What this run did *not* look at, for the file a CI job reads. Only
    # what genuinely narrows it: `--db` on a single-database hop narrows
    # nothing, and no `--only` means the full battery ran. The hop's own
    # exclude list is deliberately absent - that is the hop's definition
    # rather than a narrowing of it, and a verdict covering the hop fully
    # is complete.
    coverage = {}
    if table:
        coverage["table"] = table
    if db and len(eng.databases()) > 1:
        coverage["db"] = db
    if only and set(checks) < set(allowed):
        coverage["checks"] = sorted(checks)
    if exclude:
        coverage["exclude"] = [p.strip() for p in exclude.split(",")
                               if p.strip()]
    decided = _verdict.decide(results, hop, coverage or None)
    verdict_path, env = _verdict.write(
        hop, results, engine=eng.__class__.__name__,
        load=getattr(eng, "_last_throttle", None), coverage=coverage or None)
    if env.get("service_check"):
        from . import servicecheck
        theirs = env["service_check"]
        console.print(f"  {_esc(servicecheck.said(theirs))}")
        for name in (theirs.get("missed") or [])[:8]:
            console.print(f"    [red]the service missed[/red] {_esc(name)}")
    from . import notify
    notify.verdict(hop, env, lambda m: console.print(
        f"[yellow]{_esc(m)}[/yellow]"))
    if env.get("findings"):
        from . import assist
        said = assist.explain(hop, env)
        if said:
            console.print(f"\nexplained by {assist.provider()} - a proposal,"
                          " not part of the verdict:")
            console.print(said, markup=False)
    from .report import write_report
    report_path = write_report(hop, results, decided=decided)
    notes = [r for r, s in zip(results, decided.severity)
             if s == "note" and not r.get("kept_by_hop")]
    kept = [r for r in results if r.get("kept_by_hop")]
    if kept:
        # the rows a hop keeps by the changes it does not carry: said as
        # kept, never as the same
        console.print(f"[yellow]{len(kept)} answers hold only what the hop"
                      " keeps by the changes it does not carry"
                      " (mapping.ops) - said as kept in the report[/yellow]")
    if notes:
        # a slow target holds the same data: said, and it stops the check
        # only where the hop asks for that (`performance: gate`)
        console.print(f"[yellow]{len(notes)} databases answer the source's"
                      " reads more slowly on the target - the data is not"
                      " in question; `performance: gate` in the hop's"
                      " options makes this stop the check[/yellow]")
    colour = _verdict.VERDICTS[decided.status].colour
    console.print(f"\nverdict: [{colour}]{decided.status}[/{colour}]"
                  + (" (identical to the previous run)" if was_same else "")
                  + f"  {_esc(verdict_path)}")
    if decided.why:
        console.print(f"  {_esc(decided.why)}")
    if env.get("load"):
        ld = env["load"]
        console.print(f"[yellow]the server was under load: backed off"
                      f" {ld['waits']}x ({ld['waited_seconds']}s), concurrency"
                      f" {ld['concurrency_from']} -> {ld['concurrency_to']}"
                      "[/yellow]")
    console.print(f"report: {_esc(report_path)}")
    stale = [r for r in results if r.get("stale")]
    if stale:
        console.print(f"[yellow]{len(stale)} answers were overtaken by a"
                      " schema change on the source while they were being"
                      " taken, so they are not a verdict: "
                      + _esc(", ".join(sorted({str(r["scope"])
                                                for r in stale})[:6]))
                      + ". Check again once the change has settled.[/yellow]")
    if decided.status == "same":
        console.print(f"[green]all green ({len(results)} checks,"
                      f" {human_secs(timer.elapsed())})[/green]")
        return decided, results
    bad = [r for r, s in zip(results, decided.severity)
           if s not in ("same", "not compared", "note")]
    if bad:
        console.print(f"[yellow]{len(bad)} problems, summary:"
                      f" {_esc(summary_path)}[/yellow]")
    if hints and decided.status == "different":
        data_dbs = sorted({_verdict.db_of(r["scope"]) for r in bad
                           if r["check"] in ("counts", "autoinc", "data")})
        schema_dbs = sorted({_verdict.db_of(r["scope"]) for r in bad
                             if r["check"] == "schema"})
        console.print("\nnext steps:")
        if data_dbs:
            console.print("  # data / sequence diffs (re-check first: many are"
                          " transient replication lag, not real)")
            console.print(f"  migkit sync {_esc(hop_name)} --apply"
                          f"            # all diffed dbs at once")
        if schema_dbs:
            console.print("  # schema diffs -> generate reviewable DDL files,"
                          " then apply them yourself")
            console.print(f"  migkit schema {_esc(hop_name)} --migration"
                          f"   # writes migrations/V*.sql per db")
    return decided, results


def _on_conflict(hop, given=None):
    """How a repair settles a row both sides hold differently: the command
    line's where it says, else the hop's `on_conflict`, else the source's
    row. `sync` overwrote the hop's with its own default, so a hop's
    keep-target never held in a repair; and `keep_target`, compared as
    written, meant source-wins."""
    from .verdict import Usage
    got = str(given or (hop.options or {}).get("on_conflict")
              or "source-wins").strip().lower().replace("_", "-")
    if got not in ("source-wins", "keep-target"):
        raise Usage(f"on_conflict: {got} - source-wins or keep-target")
    hop.options["on_conflict"] = got
    return got


def _repair_wants(kind):
    """The checks whose differences a repair of `kind` puts right. Grants
    are reported by the deep check, so a database whose only difference
    is a missing grant is reached by the repair that fixes it."""
    want = {"counts", "autoinc", "data"}
    if kind in ("schema", "all"):
        want |= {"schema", "deep"}
    return want


def _repair_dbs(results, kind):
    """The databases a check's answers found different where a repair of
    `kind` reaches - by the database each answer is about."""
    from .verdict import db_of
    want = _repair_wants(kind)
    return sorted({db_of(r.get("scope", "")) for r in results
                   if r.get("status") == "diff" and r.get("check") in want})


def _repair(hop_name, db, kind, do_apply, on_conflict=None, dbs=None,
            state=None, guarded=False):
    """The one repair: the databases the last check found different (or
    `dbs`, or `--db`), every write guard passed first for all of them
    (`_guard_write` - the goal, the masking, the approvals, the target's
    identity), then each database's plan shown and, with `do_apply`,
    applied with its undo kept. `state` is the restore point `sync --go`
    took before it, named in the record; `guarded`, that `sync --go` has
    passed the guards for every database already."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    _on_conflict(hop, on_conflict)
    eng = get_engine(hop)
    if dbs is None and db:
        dbs = [db]
    if dbs is None:
        summary = hop.report_dir() / "summary.json"
        if not summary.exists():
            raise SystemExit("run migkit check first, then sync uses its diffs")
        dbs = _repair_dbs(json.loads(summary.read_text()), kind)
        if not dbs:
            console.print("nothing to repair (no matching diffs last check)")
            return
        console.print(f"repairing {len(dbs)} databases with diffs:"
                      f" {_esc(', '.join(dbs))}\n")
    if not guarded:
        _guard_write(hop, eng, "a repair", dbs, do_apply, step="repair",
                     carries_rows=kind in ("rows", "all"))
    for d in dbs:
        _repair_one(hop, eng, d, kind, do_apply, state)


def _repair_one(hop, eng, db, kind, do_apply, state=None):
    from . import repairlog
    actions = eng.repair_plan(db, kind)
    if not actions:
        console.print(f"{_esc(db)}: nothing to repair")
        return
    undo_dir = hop.report_dir(db) / "undo"
    undo_dir.mkdir(parents=True, exist_ok=True)
    for path, upto, planned in repairlog.stopped(undo_dir)[:5]:
        # what an earlier repair had applied when it stopped is in its
        # record, statement by statement; this one plans from the check
        console.print(f"  [yellow]an earlier repair stopped after statement"
                      f" {upto} of {planned}: {_esc(repairlog.said(path))}"
                      "[/yellow]")
    paused = _make_room_for_rows(hop, eng, db, actions) if do_apply else []
    try:
        _apply_actions(hop, eng, db, actions, do_apply, undo_dir, state)
    finally:
        for what in paused:
            eng.resume_writer(db, what)
            console.print(f"  {_esc(what)} was asked to resume; it carries"
                          " on from where it paused, on top of the repair")


def _make_room_for_rows(hop, eng, db, actions):
    """Rows written by a repair while another writer applies changes to the
    same target race it, and whichever lands last survives. migkit's own
    tail is paused for the repair (DMS's shape: pause, repair, resume) and
    replays afterwards; replication migkit does not drive is named, and the
    repair refuses. Returns what was paused, to be resumed."""
    if not any(a.kind == "rows" for a in actions):
        return []
    writers = eng.stream_writers(db)
    foreign = [w for w, pausable in writers if not pausable]
    if foreign:
        raise SystemExit(
            f"{db}: {', '.join(foreign)} is writing to this target, and a"
            " row repair beside it would race it - the row that survives is"
            " whichever lands last. Nothing was applied. Stop it, repair,"
            " and start it again, or let it catch up and check again.")
    window = int((hop.options or {}).get("repair_window", 300))
    paused = []
    for what in [w for w, _ in writers]:
        console.print(f"  {_esc(what)} is running; asking it to pause for"
                      " the repair")
        if not eng.pause_writer(db, what, window):
            for back in paused:
                eng.resume_writer(db, back)
            raise SystemExit(
                f"{db}: {what} did not pause within {window}s (hop option"
                " repair_window), so nothing was applied. It has been told"
                " to carry on.")
        paused.append(what)
    return paused


def _lend_console(eng):
    """A way of the engine's that fails as it repairs is switched from by
    itself (`Engine._attempt`) and said here."""
    lend = getattr(eng, "lend_log", None)
    if lend is not None:
        lend(lambda m: console.print(f"  [yellow]{_esc(m)}[/yellow]"))


def _table_copier(hop, eng, db):
    """How a repair copies one table again whole (`repairways`): the move's
    own copier (`_copy_table`) at the move's own chunk, on a checkpoint of
    its own begun afresh, inside the target's load window."""
    def copy(sch, t, log):
        path = hop.report_dir(db) / "repair-reload.json"
        path.unlink(missing_ok=True)
        try:
            with eng.load_window(db, log, {t}):
                _copy_table(eng, db, sch, t, 500000, _Checkpoint(path), log)
        finally:
            path.unlink(missing_ok=True)
    return copy


def _ladder(hop, eng, db, action):
    """The way a row repair's table goes, climbed for it (`repairways`);
    None for any other repair, which the engine applies as it is."""
    if action.kind != "rows" or not action.table:
        return None
    from . import repairways
    return repairways.Ladder(
        eng, db, action,
        log=lambda m: console.print(f"  [yellow]{_esc(m)}[/yellow]"),
        copy_table=_table_copier(hop, eng, db))


def _said_outcome(outcome):
    """A row repair's line: what it did, and what the check after it said."""
    from . import repairways
    colour = {"ok": "green", "diff": "yellow"}.get(outcome.proof, "red")
    head = "applied - " if outcome.way else ""
    return f"  [{colour}]{head}{repairways.said(outcome)}[/{colour}]"


def _apply_actions(hop, eng, db, actions, do_apply, undo_dir, state=None):
    from .engines.base import LEFT_OUT
    _lend_console(eng)
    for i, a in enumerate(actions):
        console.print(f"\n[bold]{_esc(a.kind)} on {_esc(a.scope)}[/bold]"
                      f"  {_esc(a.note)}")
        # the statements as they run, never read as markup: a SQL Server
        # name in brackets was taken for a tag and cut out of the plan the
        # operator approved
        for s in a.statements[:20]:
            say_raw(f"  {s}")
        if len(a.statements) > 20:
            console.print(f"  ... {len(a.statements) - 20} more")
        if a.kind == LEFT_OUT:
            # said with the plan, nothing to carry out
            continue
        ladder = _ladder(hop, eng, db, a)
        if ladder is not None and ladder.ready:
            console.print(f"  {ladder.foreseen()}")
        if not do_apply:
            continue
        # the fix and its undo, run on a scratch copy first where the
        # engine can make one
        usable, said = (eng.rehearse(db, a) if hasattr(eng, "rehearse")
                        else (True, ""))
        if said:
            console.print(f"  {_esc(said)}")
        if not usable:
            raise SystemExit(f"{db}: {said}")
        ts = time.strftime("%Y%m%d-%H%M%S")
        if a.undo:
            undo_file = undo_dir / f"{ts}-{a.kind}-{i}.sql"
            undo_file.write_text("\n".join(a.undo) + "\n")
            console.print(f"  undo saved: {_esc(undo_file)}")
        # the statements themselves, kept beside their undo (encrypted
        # with it where the hop asks), their digest in the record: the
        # repair's statement-level evidence
        applied = undo_dir / f"{ts}-{a.kind}-{i}.applied.sql"
        applied.write_text("\n".join(a.statements) + "\n")
        lk = _lock(hop)
        outcome = None
        try:
            if ladder is not None:
                # the table checked afresh after its repair: what is said
                # of it is that check's, never the repair's own word
                outcome = ladder.run(prove=True)
            else:
                eng.apply(db, a)
        finally:
            lk.unlink()
        import hashlib
        _changelog(hop, {"op": "repair", "db": db, "kind": a.kind,
                         "detail": a.note,
                         "undo_ref": str(undo_file) if a.undo else None,
                         "applied_ref": str(applied),
                         "statements": len(a.statements),
                         "applied_sha256": hashlib.sha256(
                             ("\n".join(a.statements) + "\n").encode()
                         ).hexdigest(),
                         **({"repair": outcome.record()} if outcome
                            else {}),
                         **({"state": state} if state else {})},
                   eng)
        console.print(_said_outcome(outcome) if outcome
                      else "  [green]applied[/green]")
    if not do_apply:
        console.print("\ndry-run only, add --apply to execute")
    else:
        console.print("\nre-run migkit check to confirm")


def _sync_go(hop_name, db, tag, on_conflict=None, kind="all"):
    """`sync --go`: the one check; for every database it finds different,
    a restore point taken and kept - committed where the state backend
    keeps them before anything is written, where it waited in a temporary
    directory until the end and a crash left the target changed and its
    undo nowhere; the one repair (`_repair`, its approvals and guards, a
    tail of migkit's paused beside a row repair); and the check again,
    whose verdict the command ends with. It was a third repair of its own:
    it read only each check's first answer - a first `ok` repaired nothing
    and said "data: ok" - asked no approval, never repaired a schema, and
    ended 0 over a target still different."""
    from .state import get_store
    hop = get_hop(hop_name)
    _require_configured(hop)
    _on_conflict(hop, on_conflict)
    eng = get_engine(hop)
    _lend_console(eng)
    capabilities.require(
        hop.engine, "snapshot",
        "use migkit sync --apply, which repairs without the snapshot --go"
        " takes first")
    first, results = _check(hop, db, "", "", False, False, 0, False, "",
                            hints=False)
    if first.status != "different":
        return first
    dbs = _repair_dbs(results, kind)
    if not dbs:
        console.print("what differs is nothing a repair of"
                      f" {kind} puts right")
        return first
    _guard_write(hop, eng, "a repair", dbs, True, step="repair",
                 carries_rows=kind in ("rows", "all"))
    store = get_store(hop)
    kept = {}
    for d in dbs:
        console.print(f"[bold]{_esc(d)}[/bold]")
        point = store.new_point(d, tag)
        eng.snapshot_state(d, point.dir, kind)
        with point.path("journal.jsonl").open("a") as f:
            f.write(json.dumps({"ts": time.strftime("%H:%M:%S"), "db": d,
                                "event": "snapshot", "kind": kind}) + "\n")
        point.set_meta(op="sync", db=d)
        kept[d] = point.commit(time.strftime("%F %T"))
        console.print(f"  state saved before the repair:"
                      f" {store.kind}:{kept[d]}")
    for d in dbs:
        _repair(hop_name, "", kind, True, on_conflict, dbs=[d],
                state=kept[d], guarded=True)
    console.print(f"\nstate backend: {store.kind}"
                  " (migkit history / rollback to use it)")
    again, _ = _check(get_hop(hop_name), db, "", "", False, False, 0, False,
                      "", hints=True)
    return again


def _given(name):
    """Whether the command line gave the option `name`, not its default."""
    from click.core import ParameterSource
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return False
    return ctx.get_parameter_source(name) not in (
        None, ParameterSource.DEFAULT, ParameterSource.DEFAULT_MAP)


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="one database, or all diffed dbs if omitted")
@click.option("--kind", default="all",
              type=click.Choice(["sequences", "rows", "schema", "all"]))
@click.option("--mode", type=click.Choice(
    ["reconcile", "verify", "seed", "stream", "migrate"]),
    default="reconcile",
    help="run a whole flow: verify (read-only assurance), seed (initial"
         " load, then the check and a repair of what differs), stream"
         " (follow live changes + delta verify), migrate (load and follow,"
         " then the check). default reconcile = repair the last check's"
         " diffs")
@click.option("--apply", "do_apply", is_flag=True,
              help="execute the repair plan, default is dry-run")
@click.option("--go", is_flag=True,
              help="checkpointed check+repair sweep with rollback state")
@click.option("--serve", is_flag=True,
              help="stream and migrate: run the verify loop forever (VM/service)")
@click.option("--interval", default=60, help="--serve: seconds between cycles")
@click.option("--on-conflict", type=click.Choice(["source-wins", "keep-target"]),
              default=None,
              help="rows that differ: source-wins overwrites the target"
                   " (the default, source is truth); keep-target preserves"
                   " them and fixes only missing/extra. The hop's own"
                   " on_conflict where not given")
@click.option("--tag", default="", help="with --go: label this state, e.g. pre-cutover")
def sync(hop_name, db, kind, mode, do_apply, go, serve, interval,
         on_conflict, tag):
    """Make target equal to source, or run a whole migration flow.

    Default (reconcile) shows/repairs the last check's diffs; --apply
    executes, --go checkpoints for rollback. --mode runs the whole flow
    end to end with verification wrapped around every step, so migkit can
    stand in for a managed migration service on a trusted VM - the same
    move, the same repair and the same check as the commands of their
    own, and the exit code the last check's."""
    from . import archive, consolidate, subset
    from .verdict import Usage
    if kind != "all" and mode != "reconcile" and _given("kind"):
        raise Usage(f"--kind picks what a repair of the last check's"
                    f" differences puts right; --mode {mode} runs a whole"
                    " flow and repairs whatever it finds")
    if tag and not go:
        raise Usage("--tag names the restore point --go takes; without"
                    " --go nothing is saved to name")
    if _given("interval") and not serve:
        raise Usage("--interval is the time between the cycles of --serve")
    if serve and mode not in ("stream", "migrate"):
        raise Usage("--serve keeps verifying a stream: --mode stream or"
                    " --mode migrate")
    if go and do_apply and mode == "reconcile":
        raise Usage("--apply repairs; --go repairs with a restore point"
                    " taken first - one or the other")
    if mode != "verify" and subset.settings(get_hop(hop_name)) is not None:
        # a copy for testing holds the rows chosen, masked: made equal to
        # the whole source it would not be one any more
        raise SystemExit(f"{hop_name} is a copy for testing (goal subset):"
                         " it is taken afresh by migkit move"
                         f" {hop_name} --mode full --go, and proved by"
                         f" migkit check {hop_name}; sync makes a target"
                         " equal to the whole source. Nothing was written")
    for goal in (consolidate, archive):
        if mode != "verify" and goal.settings(get_hop(hop_name)) is not None:
            # the target holds other sources' rows, or rows the source no
            # longer has: made equal to this source it would lose them
            raise SystemExit(f"{hop_name}'s goal is {goal.GOAL}: its target"
                             " holds rows this source does not, and sync"
                             " makes a target equal to the source. It is"
                             f" carried by migkit move {hop_name} --mode"
                             f" full --go and proved by migkit check"
                             f" {hop_name}. Nothing was written")
    if mode != "reconcile":
        got = _orchestrate(hop_name, db, mode, do_apply or go, serve,
                           interval, on_conflict)
    elif go:
        got = _sync_go(hop_name, db, tag, on_conflict, kind)
    else:
        return _repair(hop_name, db, kind, do_apply, on_conflict)
    if got is not None and got.exit:
        raise SystemExit(got.exit)


def _settle(hop_name, db, on_conflict):
    """After a copy: the one check; what it finds different put right by
    the one repair, behind its guards and approvals; and the check again,
    whose verdict the flow ends with."""
    first, results = _check(get_hop(hop_name), db, "", "", False, False, 0,
                            False, "", hints=False)
    if first.status != "different":
        return first
    dbs = _repair_dbs(results, "all")
    if not dbs:
        return first
    console.print("\n[bold]repair[/bold]: what the check found different")
    _repair(hop_name, "", "all", True, on_conflict, dbs=dbs)
    console.print("\n[bold]verify[/bold]: consistent snapshot")
    again, _ = _check(get_hop(hop_name), db, "", "", False, True, 0, False,
                      "", hints=True)
    return again


def _caught_up(hop, db):
    """The stream a flow set up, waited for until the target has applied
    what the source held a moment ago (`fence_wait`), where the engine
    can prove it - so the check after it does not read a stream still
    catching up as a difference. Said, not waited on forever."""
    eng = get_engine(hop)
    if capabilities.unavailable(hop.engine, "fence") or \
            not hasattr(eng, "fence_wait"):
        return
    for d in ([db] if db else eng.databases()):
        try:
            at = eng.src_lsn(d)
            ok = eng.fence_wait(d, at) if at is not None else None
        except Exception as e:  # noqa: BLE001 - the check says the rest
            console.print(f"  {_esc(d)}: whether the target has caught up"
                          f" could not be asked: {_esc(_clean(e, raw=True))}")
            continue
        if ok is False:
            console.print(f"  {_esc(d)}: the target has not caught up with"
                          " the source yet; the check reads it as it is")


def _orchestrate(hop_name, db, mode, go, serve, interval, on_conflict=None):
    """`sync --mode`: the whole flow, of the commands' own steps and
    nothing else - the move (`_move_run`: its position taken before the
    copy, its guards, its proofs), the one check, the one repair, and the
    delta loop - so what the flow promises is what the commands do. It
    had a move and a stream of its own: no position before its seed, a
    subscription copying again over the tables the seed had loaded, and
    the guards `move` holds skipped."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    if not go and mode == "verify":
        # the other flows' plans are the move's, which says it
        from . import hopkeys
        _say_goal(hop, hopkeys.explain_mode("sync", mode))
    if mode == "verify":
        console.print("[bold]verify[/bold]: read-only, consistent snapshot")
        got, _ = _check(hop, db, "", "", False, True, 0, False, "",
                        hints=False)
        return got
    move_mode = {"seed": "full", "migrate": "full+cdc", "stream": "cdc"}[mode]
    console.print(f"[bold]{mode}[/bold]: migkit move {_esc(hop_name)}"
                  f" --mode {move_mode}"
                  + ("" if go else "  (dry-run; add --apply/--go to"
                     " execute)"))
    _move_run(hop_name, db, "", move_mode, 500000, False, go)
    if not go:
        console.print("with --go: "
                      + ("the copy, then the check; what it finds"
                         " different is repaired and checked again"
                         if mode == "seed" else
                         "the copy and its stream, then the check"
                         if mode == "migrate" else
                         "the stream, then each cycle verifies what"
                         " changed"))
        return None
    if mode == "seed":
        return _settle(hop_name, db, on_conflict)
    got = None
    if mode == "migrate":
        _caught_up(hop, db)
        got, _ = _check(get_hop(hop_name), db, "", "", False, False, 0,
                        False, "", hints=True)
        if not serve:
            return got
    why = capabilities.unavailable(hop.engine, "delta")
    if why:
        # no verify of what changed here: the flow ends as its check did
        console.print(f"   {_esc(why)}")
        return got
    return _delta_loop(hop_name, db, interval, 0 if serve else 1, False)


def _table_plan(hop, eng, d, via, go=False):
    """Where each table goes and why, before anything is copied.

    Read from the source's catalogue. A catalogue that cannot be read
    costs the operator this view and nothing else: the copy asks the same
    questions again, and stops on its own if they cannot be answered.
    """
    from . import movers, planner
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    try:
        lister = getattr(eng, "_all_tables", None)
        tables = lister("src", d) if lister else eng.neutral_tables("src", d)
        facts = eng.table_facts("src", d)
        # a partition planned as its table is (`movers._pg_roots`)
        roots = (movers._pg_roots(hop, d, eng=eng)
                 if hasattr(eng, "partition_roots") else None)
    except NotImplementedError:
        # a keyspace or a cluster's topics: no table to plan
        return []
    except Exception as e:
        from .movers import DRIVEN
        from .wording import without_programs
        why = (str(e).strip().splitlines()[-1][:100] if str(e).strip()
               else type(e).__name__)
        return ["  (the plan by table could not be read: "
                f"{without_programs(why, DRIVEN)})"]
    decisions = planner.plan(hop, d, via, tables,
                             "public" if engine == "postgres" else None,
                             facts, roots=roots)
    # the time each table the copier carries takes; its way and why go to
    # the decision record (`decide.record`)
    from . import tableway
    decisions = tableway.annotate(eng, d, decisions,
                                  log=lambda m: console.print(f"  {m}"))
    # what the move carries, not the whole database: a few tables taken
    # of many were timed as all of them
    taken, narrowed = _carried(eng, d, facts)
    rows = sum(int(f.get("rows") or 0) for f in taken.values()
               if isinstance(f, dict))
    try:
        free = eng.free_bytes("dst", d)
    except Exception:  # noqa: BLE001 - a plan line, not a reason to stop
        free = None
    def kept(grows):
        try:
            return eng.log_kept(d, grows)
        except Exception:  # noqa: BLE001 - the plan says what it knows
            return None
    size = planner.size_line(decisions, facts,
                             getattr(eng, "dst_name", engine),
                             (hop.options or {}).get("transfer_price_per_gb"),
                             free, kept, narrowed)
    carried = [d_.table for d_ in decisions if d_.path != planner.LEFT]
    return (["  by table:"] + planner.lines(decisions)
            + [f"  time: {planner.estimate(hop, via, rows, narrowed)}"]
            + ([f"  size: {size}"] if size else [])
            + _where_and_machine(hop, eng, d, via)
            + _resource_plan(hop, eng, d, via, facts,
                             [t for t in facts if t in carried
                              or any(str(t).endswith("." + c)
                                     for c in carried)], go))


def _where_and_machine(hop, eng, d, via):
    """The plan's lines about where migkit should run and the machine to
    run it on (R22): from the round trips measured now, and from the
    stages measured on this path."""
    from . import legs, machine
    out = []
    try:
        where = legs.plan_line(hop, via, d)
    except Exception:  # noqa: BLE001 - a plan line, not a reason to stop
        where = ""
    if where:
        out.append(f"  where: {where}")
    # a deadline the hop gives wrong stops here, said (SystemExit)
    try:
        said = machine.plan_line(hop, via, machine.size_of(eng, d, hop),
                                 legs.direct(via), d)
    except Exception:  # noqa: BLE001 - a plan line, not a reason to stop
        said = ""
    if said:
        out.append(f"  machine: {said}")
    # the copy over the link against a seed shipped on media, both timed,
    # where the hop names a carrier or the link would miss its deadline
    from . import ship
    seed = ship.plan_line(hop, eng, d, via)
    if seed:
        out.append(f"  seed: {seed}")
    return out


def _record_run(hop, eng, d, via, rows, began, spent, narrowed=()):
    """One finished copy of a whole database kept with the hop's rates:
    its rows and seconds, the bytes it carried, and the CPU seconds this
    machine spent - where the copy ran here and not in a container, whose
    CPU this process does not see. `rows` are what it carried
    (`_source_rows`); where a row filter narrows tables of it (`narrowed`)
    they are only the most it carried, and no rate is kept from them: a
    rate kept too high is read by every later plan, other hops' too."""
    from . import machine, movers, planner, rehearsal, sizing
    # the programs this machine runs for the way, not the static table: a
    # local build of the streaming copy was taken for its image, and its
    # CPU seconds were lost
    here = movers.programs(via) != ("docker",)
    took = time.time() - began
    if narrowed:
        chat(f"  {d}: no rate is kept from this copy - {len(narrowed)}"
             " tables a row filter narrows are counted whole by the"
             " source's estimate")
        rows = None
    planner.record_rate(hop, via, rows, took,
                        machine.size_of(eng, d, hop),
                        spent.seconds() if here else None,
                        sizing.host().get("cpus") if here else None)
    # the copy held to the forecast it started with, where it kept one
    for line in rehearsal.held_to(hop, d, "copy", took):
        console.print(f"  {line}")



def _resource_plan(hop, eng, d, via, facts=None, tables=None, go=False):
    """The move's resources in the plan's words - which one binds, how many
    at once carry it, what the widest rows take of this machine's memory -
    and each phase's time with its reason (`model`, `planner.phases`).
    Measured once a run: the copy that follows goes by the same model. A
    plan alone writes nothing to the target, not even the write probe's
    rolled-back rows: the interlock has not held the target to the plan
    yet (`interlock.before_write`)."""
    from . import model as resource_model
    from . import planner, sizing
    try:
        m = resource_model.for_move(hop, eng, d, facts, tables, write=go)
        facts = facts if facts is not None else eng.table_facts("src", d)
        n = sizing.estimate(hop, eng, d, _units(eng, d), model=m).start
    except Exception as e:  # noqa: BLE001 - the plan says what it knows
        from .movers import DRIVEN
        from .wording import without_programs
        why = (str(e).strip().splitlines()[-1][:100] if str(e).strip()
               else type(e).__name__)
        return [f"  (the resources could not be measured:"
                f" {without_programs(why, DRIVEN)})"]
    out = [f"  {line}" for line in m.lines(n)]
    short = m.too_wide(sizing._memory())
    if short:
        out.append(f"  memory: NOT ENOUGH - {short}")
    phases = _planned(hop, m, facts, tables, n, via, d, go)
    return (out + planner.phase_lines(phases)
            + _joint_lines(hop, eng, m, facts, tables, d, phases))


def _joint_lines(hop, eng, m, facts, tables, d, phases=()):
    """The move's tables planned as one schedule (`jointplan`), in what
    it means for the operator: the end with its spread, what sets it,
    whether it keeps to the hop's limits. The decision itself - each
    table's way, the order, the workers, the facts it stood on and the
    ones it did not know - goes to the run's decision record, and to the
    console only where MIGKIT_EXPLAIN=1 (the owner, 2026-09-30: the
    choices stay inside, the outcomes are said)."""
    from . import decide, hopkeys, jointplan, sizing
    out = [f"  {line}" for line in hopkeys.explain_limits(hop)]
    try:
        most = sizing.estimate(hop, eng, d, _units(eng, d), model=m).most
        jp = jointplan.for_move(hop, m, facts, tables, most, d)
    except SystemExit:
        # a limit written wrong stops the command, said
        raise
    except Exception:  # noqa: BLE001 - the plan says what it knows
        jp = None
    if jp is not None:
        out += [f"  plan: {line}" for line in jointplan.lines(jp)]
        decide.record(hop, {
            "work": f"the move of {d}", "plan": jp.as_evidence(),
            "facts": jointplan.account(hop, m, facts, jp)},
            lambda line: out.append(f"  {line}"),
            way="the tables planned as one",
            reason="; ".join(jointplan.lines(jp)))
    said = jointplan.outage_line(hop, phases)
    if said:
        out.append(f"  limits: {said}")
    return out


def _planned(hop, m, facts, tables, n, via, d, go=False):
    """The phases of the move from its model, kept with it for `watch` and
    the metrics - and, as a run starts on a hop that rehearses or is
    rehearsed, as the forecast the run is held to as each phase ends
    (`rehearsal.keep_forecast`)."""
    from . import planner, rehearsal
    m.planned = planner.phases(hop, m, facts, tables, n, via)
    try:
        m.save(hop.report_dir(d))
    except OSError:
        pass
    if go and (rehearsal.settings(hop) or rehearsal.of(hop)):
        rehearsal.keep_forecast(hop, d, m.planned, via)
    return m.planned


def _source_rows(eng, d):
    """(the rows a move carries out of a database, by the source's own
    estimate; the tables in it a row filter narrows): only the tables the
    hop takes (`Engine.carried`), a narrowed one counted whole - the rows
    are then the most the move carries. (0, []) where the source has no
    estimate."""
    try:
        facts, narrowed = _carried(eng, d)
        return sum(int(f.get("rows") or 0) for f in facts.values()
                   if isinstance(f, dict)), narrowed
    except Exception:
        return 0, []


def _carried(eng, d, facts=None):
    """`Engine.carried`, or every table of the catalogue where the engine
    has no such answer."""
    ask = getattr(eng, "carried", None)
    if callable(ask):
        return ask(d, facts)
    return (eng.table_facts("src", d) if facts is None else facts), []


def _copy_routed(hop, eng, d, via, chunk, log):
    """Carry the tables the bulk path left to the table copier.

    `movers.routed_to_copier` decides which, and the bulk path already left
    exactly those out of its copy - one function, so the two cannot
    disagree. The copier applies the hop's row filter on both sides.

    A fresh checkpoint every time: the bulk path empties these tables on
    each run, and a checkpoint that remembered one as done would skip it
    and leave it empty.
    """
    from . import movers
    if not hasattr(eng, "move_table"):
        return
    try:
        tables = eng.neutral_tables("src", d)
    except NotImplementedError:
        # a keyspace or a cluster's topics: nothing a bulk path leaves to
        # the copier table by table
        return
    routed = movers.routed_to_copier(hop, d, via, tables,
                                     movers.qualifier_for(hop))
    if not routed:
        return
    path = hop.report_dir(d) / "move-routed.json"
    ck = _checkpoint(hop, path)
    ck.clear()
    ck.discard()
    with eng.load_window(d, log, {n.rpartition(".")[2] for n in routed}):
        for name in routed:
            sch, _, tbl = name.rpartition(".")
            _copy_table(eng, d, sch, tbl, chunk, ck, log)
            _changelog(hop, {"op": "move-routed", "db": d, "table": name})


def _copy_table(eng, d, sch, t, chunk, ck, log):
    """One table by the engine's own copier, or through the pair machinery
    where the hop maps its columns and the own copier would carry the whole
    row. The pair's writes go through its own target driver, so it opens
    the target's load window for itself; nested inside the engine's, it
    finds nothing left to set aside. A span of the run's trace, with the
    rows the checkpoint says it carried."""
    from . import traces
    with traces.span("copy table", db=d,
                     table=f"{sch}.{t}" if sch else t) as span:
        try:
            return _copy_table_by(eng, d, sch, t, chunk, ck, log)
        finally:
            got = ck.get(eng.move_key(d, sch, t)) if isinstance(
                ck, dict) else None
            if isinstance(got, dict):
                traces.attributes(span, rows=got.get("moved"),
                                  done=bool(got.get("done")))


def _copy_table_by(eng, d, sch, t, chunk, ck, log):
    from . import ha
    if not eng.through_pair(d, f"{sch}.{t}" if sch else t) \
            and (getattr(eng, "src_engine", None) is not None
                 or ha.copier(eng.hop) == "own"):
        return eng.move_table(d, sch, t, chunk, ck, log)
    pair = eng.columns_pair()
    src = next((n for n in pair.src_engine.neutral_tables("src", d)
                if pair._leaf(n) == t), t)
    psch, _, ptbl = src.rpartition(".")
    with pair.load_window(d, log, {src}):
        pair.move_table(d, psch, ptbl, chunk, ck, log)
    # a table the pair built gets its indexes once its rows are in
    pair.finish_created(d, log)


def _side_health(eng, side, d):
    """What a side says about its load, for the gate; None where it will
    not say."""
    import inspect
    fn = getattr(eng, "_health", None)
    if fn is None:
        return None
    try:
        takes_db = len(inspect.signature(fn).parameters) > 1
        return fn(side, d) if takes_db else fn(side)
    except Exception:  # noqa: BLE001 - a probe never stops the move
        return None


def _joint_for_move(hop, eng, m, d, names, most, log):
    """The joint plan the move goes by, kept beside its reports and in the
    decision record; None where the model measured too little to plan.
    Its end and what sets it are said; its choices only where
    MIGKIT_EXPLAIN=1."""
    from . import decide, jointplan
    if not getattr(m, "measured", False):
        return None
    try:
        facts = eng.table_facts("src", d)
        jp = jointplan.for_move(hop, m, facts, set(names), most, d)
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 - the move goes on as sized
        return None
    if jp is None:
        return None
    for line in jointplan.lines(jp):
        log(f"plan: {line}")
    decide.record(hop, {
        "work": f"the move of {d}", "plan": jp.as_evidence(),
        "facts": jointplan.account(hop, m, facts, jp)}, log,
        way="the tables planned as one",
        reason="; ".join(jointplan.lines(jp)))
    return jp


def _largest_first(eng, d, tables):
    """The tables in the order that finishes soonest side by side: as the
    joint plan kept for this move starts them (`jointplan.order_of`),
    else the largest started first, so one is not left running alone at
    the end."""
    from . import jointplan
    hop = getattr(eng, "hop", None)
    names = [f"{sch}.{t}" if sch else t for sch, t in tables]
    planned = (jointplan.order_of(hop, d, names) if hop is not None
               else None)
    if planned is not None:
        at = {n: i for i, n in enumerate(planned)}
        return sorted(tables, key=lambda x: at.get(
            f"{x[0]}.{x[1]}" if x[0] else x[1], len(at)))
    try:
        facts = eng.table_facts("src", d)
    except Exception:  # noqa: BLE001 - the order they came in
        return list(tables)

    def size(item):
        sch, t = item
        f = facts.get(f"{sch}.{t}" if sch else t) or facts.get(t) or {}
        return -int(f.get("bytes") or f.get("rows") or 0)
    return sorted(tables, key=size)


def _copy_tables(hop, eng, d, tables, chunk, ck):
    """The tables copied side by side, as many at once as the hop's
    `workers` - and only as many as both servers take: the gate asks each
    side how it is doing before a table starts, and narrows when either
    is under load. A target that takes one writer at a time (a file) is
    written one table after another. A table that fails lets the others
    in flight finish, starts no more, and is said."""
    from . import model as resource_model
    from . import planner, ranges, sizing

    def log(m):
        console.print(f"  {m}")
    target = getattr(eng, "dst_engine", eng)
    # how many at once is worked out, not asked for: every resource
    # measured at the start (`model`), then kept where the rows go fastest,
    # each resource backing off on its own signal (`sizing`)
    names = _catalogue_names(eng, d, tables)
    m = resource_model.for_move(hop, eng, d, tables=names, log=log)
    short = m.too_wide(sizing._memory())
    if short:
        raise SystemExit(f"{d}: not started - {short}")
    plan = sizing.fit(hop, eng, d, log, units=_units(eng, d, tables),
                      model=m)
    if not m.planned:
        try:
            _planned(hop, m, eng.table_facts("src", d), names, plan.start,
                     "builtin", d, go=True)
        except Exception:  # noqa: BLE001 - the move goes on without it
            pass
    most = plan.most
    if not getattr(target, "WRITES_IN_PARALLEL", True):
        most = 1
    # the tables planned as one (`jointplan`): the order they start in
    # (`_largest_first` reads it) and the fewest workers that end as soon
    # as the most allowed would - where the model measured enough to plan
    start = plan.start
    joint = _joint_for_move(hop, eng, m, d, names, most, log)
    if joint is not None and joint.workers:
        start = joint.workers
    total, _ = _source_rows(eng, d)
    where = hop.report_dir(d)
    ways = getattr(eng, "measured_ways", None) or (lambda: [])
    pace = sizing.Pace(
        max(1, min(start, most)), most,
        resources=sizing.resources(eng, d, hop, _side_health, m),
        memory=sizing.Memory(m.worker_memory),
        report=lambda p: planner.progress(where, p, total, m, ways=ways()))
    before = ranges.active
    # the ranges every table in flight copies share the move's workers
    ranges.active = ranges.Slots(most, pace=pace)
    try:
        # indexes a copy that was killed had set aside, built again at the
        # end of this one - whichever tables it copies
        window = getattr(target, "table_index_window", None)
        from . import movers
        with (window(d, None, log) if window
              else contextlib.nullcontext()):
            # a finished table's indexes are built while the next copies
            # as many tables at once as the joint plan found ends soonest
            # (fewer lets the first tables' index builds run beside the
            # rest's copy), their ranges sharing every worker
            at_once = min(most, len(tables))
            if joint is not None and joint.tables:
                at_once = max(1, min(at_once, joint.tables))
            with movers.builds_beside(log):
                _copy_tables_in(hop, eng, d, tables, chunk, ck, at_once)
        # what the copy measured of itself, which the next plan stands on
        m.observe(ways(), pace.total, pace.took(), pace.limit)
        resource_model.tune_probe_floor(hop, m, pace.total, pace.took())
        try:
            m.save(where)
        except OSError:
            pass
    finally:
        ranges.active = before
        said = pace.line()
        if said:
            log(said)
        # the whole copy's rate, for whoever holds the model to it
        try:
            planner.progress(where, pace, total, m, done=True, ways=ways())
        except OSError:
            pass


def _units(eng, d, tables=None):
    """How many pieces of work a database's copy can be split into: a
    range for every `ranges.LEAST` rows of a table, one at least - of the
    tables the move carries (`Engine.carried`) where none are named."""
    from . import ranges
    try:
        facts = eng.table_facts("src", d)
    except Exception:  # noqa: BLE001 - a table each, then
        return len(tables) if tables else None
    names = ([f"{sch}.{t}" if sch else t for sch, t in tables]
             if tables is not None else list(_carried(eng, d, facts)[0]))
    n = 0
    for name in names:
        f = facts.get(name) or facts.get(name.split(".")[-1]) or {}
        n += max(1, int(f.get("rows") or 0) // ranges.LEAST)
    return n or None


def _catalogue_names(eng, d, tables):
    """[(schema, table)] as the source's catalogue names them
    (`table_facts`' keys): `schema.table`, or the table alone where the
    engine has no schema."""
    return [f"{sch}.{t}" if sch else t for sch, t in tables]


def _copy_tables_in(hop, eng, d, tables, chunk, ck, workers):
    import concurrent.futures as cf

    from .throttle import Throttle

    def log(m):
        console.print(f"  {m}")
    if workers < 2:
        from . import failpoint, jointplan
        # one table at a time: in the joint plan's order where it planned
        # this move's tables, as they came otherwise
        names = _catalogue_names(eng, d, tables)
        planned = jointplan.order_of(hop, d, names)
        if planned is not None:
            at = {n: i for i, n in enumerate(planned)}
            tables = sorted(tables, key=lambda x: at[
                f"{x[0]}.{x[1]}" if x[0] else x[1]])
        for sch, t in tables:
            _copy_table(eng, d, sch, t, chunk, ck, log)
            failpoint.hit("table.done")
            _changelog(hop, {"op": "move", "db": d, "table": f"{sch}.{t}"})
        return

    def health():
        seen = None
        for side in ("src", "dst"):
            h = _side_health(eng, side, d)
            if h is not None and h.stressed():
                return h
            seen = seen or h
        return seen
    gate = Throttle(workers, probe=health)

    def one(sch, t):
        from . import failpoint
        gate.gate()
        try:
            _copy_table(eng, d, sch, t, chunk, ck, log)
            failpoint.hit("table.done")
            _changelog(hop, {"op": "move", "db": d, "table": f"{sch}.{t}"})
        finally:
            gate.done()
    failed = None
    pool = cf.ThreadPoolExecutor(workers)
    try:
        futs = [pool.submit(one, sch, t)
                for sch, t in _largest_first(eng, d, tables)]
        for f in cf.as_completed(futs):
            if f.cancelled():
                continue
            if f.exception() is not None and failed is None:
                failed = f.exception()
                for g in futs:
                    g.cancel()
    except KeyboardInterrupt:
        # it waited for every table in flight without a word
        running = sum(1 for g in futs if g.running())
        if running:
            log(f"[yellow]stopping: the {running} tables in flight are let"
                " finish, and no other starts[/yellow]")
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    said = gate.line()
    if said:
        log(said)
    if failed is not None:
        raise failed


def _refuse_if_nothing_arrived(hop, eng, d, what, log):
    """Every copy path ends here: a copy can exit cleanly and leave the
    target empty, and saying "complete" over that is the one thing a
    migration tool must never do."""
    empty = eng.moved_nothing(d)
    if empty:
        # the copy was told to skip what the hop excludes, so a table it
        # skipped is not one it failed to fill - counting it made a correct
        # move report itself failed and go unrecorded
        empty = [t for t in empty if not hop.excluded(d, *t.split("."))]
    if empty:
        raise SystemExit(
            f"{what} reported success and {d} is still empty on the"
            " target: " + ", ".join(empty[:6])
            + (" ..." if len(empty) > 6 else "")
            + ". Nothing has been marked as moved - look at the copy's"
              " output above, and at the target server's log")
    if empty is None:
        log("(this engine cannot confirm the rows landed; `migkit check` is"
            " what proves it)")


def _held_to_the_source(hop, eng, d, log):
    """A bulk copy's whole result compared with the source before it is
    called complete: a program that loads a database can say it did and
    leave rows out or change them, and the table copier checks every range
    as it writes where a program copying on its own cannot be asked to.
    Skipped where the hop says `verify_batches: false`."""
    if not (hop.options or {}).get("verify_batches", True):
        return
    log("comparing what arrived with the source")
    from .movers import masked_tables
    # a table the hop masks on the way is not a copy of its source: the
    # move held it to the masking rule and counted its rows already
    masked = masked_tables(hop, d)
    bad = [r for r in eng.check_data(d)
           if r.check == "data" and r.status in ("diff", "error")
           and r.scope not in masked]
    if not bad:
        return
    raise SystemExit(
        f"the bulk copy finished and {len(bad)} of {d}'s tables do not hold"
        " what the source does: "
        + "; ".join(f"{r.scope}: {r.detail[:120]}" for r in bad[:4])
        + (" ..." if len(bad) > 4 else "")
        + ". Nothing has been marked as moved. A source written to while it"
          " was copied differs by what changed - move with --mode full+cdc"
          " for that; otherwise `migkit sync` puts the rows right")


def _stop_if_the_schema_moved(hop, eng, d, before):
    """Every move path ends here: a DDL on the source during the move means
    the rows on either side of it belong to two different tables, and
    saying "complete" over that would be believed."""
    from . import drift
    changed = drift.changes(before, drift.shape(eng, "src", d))
    if not changed:
        return
    _changelog(hop, {"op": "move-ddl-seen", "db": d, "changes": changed})
    stop = (f"{d}: the source's schema changed while it was being moved - "
            + "; ".join(changed[:6]) + (" ..." if len(changed) > 6 else "")
            + ". The rows moved before and after it may not line up. Bring"
            " the target's schema level with the source's, then move again"
            " and run migkit check.")
    from . import notify
    notify.drift(hop, d, changed, lambda m: console.print(
        f"[yellow]{m}[/yellow]"), stop=stop)
    raise SystemExit(stop)


def _move_full(hop, eng, db, table, chunk, go):
    from . import movers
    from .engines import ALIASES
    capabilities.require(hop.engine, "table-copy")
    dbs = [db] if db else eng.databases()
    for d in dbs:
        # the bulk path asked this and the table copier did not, so a
        # filter the copier cannot apply moved every row under "complete"
        movers.refuse_unpushable_filters(hop, d, "builtin",
                                         ALIASES.get(hop.engine, hop.engine))
        ck = _checkpoint(hop, hop.report_dir(d) / "move.json")
        if table:
            tables = [tuple(table.split(".", 1)) if "." in table
                      else ("", table)]
        else:
            tables = eng.list_move_tables(d)
        tables = _keep_out(hop, eng, d, tables, go)
        if tables is None:
            continue
        if not go:
            done = sum(1 for sch, t in tables
                       if ck.get(eng.move_key(d, sch, t), {}).get("done"))
            console.print(f"{_esc(d)}: {len(tables)} tables, {done} already"
                          f" done in checkpoint, chunk {chunk:,} rows")
            _would_stop(lambda: eng.refuse_unfit(
                d, [f"{s}.{t}" if s else t for s, t in tables
                    if not ck.get(eng.move_key(d, s, t))]))
            # each table's time from the one table ladder the copy climbs
            # when its turn comes; its way and why go to the decision
            # record (`tableway`)
            from . import tableway
            ways = tableway.plan_for(eng, d, tables[:20], ck,
                                     lambda m: console.print(f"  {m}"))
            for sch, t in tables[:20]:
                st = ck.get(eng.move_key(d, sch, t), {})
                last = st.get("last")
                mark = "done" if st.get("done") else \
                    f"resume at {last:,}" if isinstance(last, int) else \
                    f"resume after {last}" if "last" in st else "todo"
                way = ways.get((sch, t))
                console.print(f"  {sch}.{t}: {mark}"
                              + (f" - {tableway.outcome(way)}"
                                 if way and way.rung
                                 and not st.get("done") else ""))
            from . import planner
            console.print("  time: " + planner.estimate(
                hop, "builtin", *_source_rows(eng, d)))
            for line in (_where_and_machine(hop, eng, d, "builtin")
                         + _resource_plan(hop, eng, d, "builtin", None,
                                          _catalogue_names(eng, d, tables))):
                console.print(line)
            # where each machine works, where the hop's machines work
            # together (`ha`)
            from . import ha
            for line in ha.plan_lines(hop, eng, d):
                console.print(f"  {line}")
            continue
        # a value the target would round, cut or refuse stops the move
        # here, before the first table is copied; a table an earlier run
        # began was asked then
        eng.refuse_unfit(d, [f"{s}.{t}" if s else t for s, t in tables
                             if not ck.get(eng.move_key(d, s, t))])
        from . import drift
        share = _sharing(hop) if not table else None
        before = drift.shape(eng, "src", d)
        began, (rows, narrowed) = time.time(), _source_rows(eng, d)
        from . import machine
        spent = machine.Spent()
        point = _kept_point(hop, eng, d, ck) if not table else None
        _mark_move(hop, eng, d)
        lk = _lock(hop)
        try:
            # the table copier fills tables and made none: measured onto an
            # empty target, the move stopped on `relation ... does not
            # exist`, where the bulk paths create what is missing
            if not table:
                eng.create_missing(d, lambda m: console.print(f"  {m}"))
            if share:
                # the hop is held for the setup only; from here each table
                # is held on its own, by whichever machine takes it
                lk.unlink()
                lk = None
                point = _first_point(ck, point)
                from . import claims
                if claims.shared_ranges(hop):
                    _share_ranges(hop, eng, d, tables, chunk, ck)
                else:
                    _share_tables(hop, eng, d, tables, chunk, ck, share)
                lk = _finisher(hop, d, ck)
                if lk is None:
                    continue
            else:
                with eng.load_window(d, lambda m: console.print(f"  {m}"),
                                     {t for _, t in tables}):
                    _copy_tables(hop, eng, d, tables, chunk, ck)
            if not table:
                eng.finish_created(d, lambda m: console.print(f"  {m}"))
                eng.carry_database_objects(
                    d, lambda m: console.print(f"  {m}"))
        finally:
            if lk is not None:
                lk.unlink()
        if not table and not share:
            # a whole database's rows over its time; one table's run is not
            # a rate for the rest, and neither is one machine's share
            _record_run(hop, eng, d, "builtin", rows, began, spent,
                        narrowed)
        _stop_if_the_schema_moved(hop, eng, d, before)
        if not table:
            _refuse_if_nothing_arrived(hop, eng, d, "the table copy",
                                       lambda m: console.print(f"  {m}"))
            _record_copy(hop, d, point)
        # the table copier leaves the statistics behind it exactly as a
        # bulk load does; only the bulk path used to put them right
        settled = eng.settle_target(d)
        if settled:
            console.print(f"  {settled}")
        if not table:
            _after_copy(hop, eng, d)
        console.print(f"[green]{d}: move complete[/green],"
                      " run migkit check to verify")
    if not go:
        console.print("\ndry-run, add --go to copy")


#: where the source's log was before the first run of a copy began
POINT_FILE = "copy-before.json"


def _kept_point(hop, eng, d, ck):
    """The source's position before the first run of this copy, taken and
    kept before a row is copied, and read back by every run that resumes
    it: a resumed copy took a new one, and a `--mode cdc` started from it
    missed what changed on the ranges the first run had finished. A copy
    resumed from a run that kept none has none - which the stream says,
    rather than a guess."""
    path = hop.report_dir(d) / POINT_FILE
    shared = _shared(hop, d)
    begun = any(isinstance(v, dict) for k, v in ck.items()
                if k not in (BEFORE, COMPLETED))
    if begun:
        try:
            return json.loads(path.read_text()).get("point")
        except (OSError, ValueError, AttributeError):
            return None
    point = _position_before_copy(eng, d)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"point": point, "at": time.strftime(
        "%Y-%m-%d %H:%M:%S")}, default=str))
    if shared:
        shared.push(force=True)
    return point


#: the checkpoint entry holding where the source's log was before the
#: first machine sharing a move began copying
BEFORE = "__before__"
#: the entry the machine completing a shared move leaves
COMPLETED = "__completed__"


def _first_point(ck, point):
    """The position taken before the first machine's copy: a later one
    would miss what changed on the tables copied before it."""
    have = ck.reload().get(BEFORE)
    if isinstance(have, dict) and "point" in have:
        return have["point"]
    ck[BEFORE] = {"point": point, "done": True}
    ck.save()
    return ck[BEFORE]["point"]


def _share_tables(hop, eng, d, tables, chunk, ck, remote):
    """Every table not done, taken one at a time by whichever machine asks
    first. A table another machine holds is passed over and asked for
    again, so one whose machine died is taken over once its lease lapses,
    from that machine's last saved chunk. Returns once every table is
    done, by whichever machine."""
    from .lease import Held, Lease, term

    def log(m):
        console.print(f"  {m}")
    said_waiting = False
    while True:
        left = [(s, t) for s, t in tables
                if not ck.reload().get(eng.move_key(d, s, t), {}).get("done")]
        if not left:
            return
        moved = False
        for sch, t in left:
            key = eng.move_key(d, sch, t)
            lease = Lease(hop.report_dir(d) / f"lease-{key}.json",
                          f"copy of {key}", remote)
            try:
                lease.acquire()
            except Held:
                continue
            try:
                if lease.took_over:
                    was = lease.took_over
                    log(f"{key}: taken over from {was.get('host')} process"
                        f" {was.get('pid')}, whose lease had lapsed")
                if ck.reload(key).get(key, {}).get("done"):
                    continue
                with eng.load_window(d, log, {t}):
                    _copy_table(eng, d, sch, t, chunk, ck, log)
                _changelog(hop, {"op": "move", "db": d,
                                 "table": f"{sch}.{t}", "shared": True})
                moved = True
            finally:
                lease.release()
        if not moved:
            if not said_waiting:
                log(f"{d}: {len(left)} tables are being copied by other"
                    " machines; waiting for them, and for any whose"
                    " machine stops renewing its lease")
                said_waiting = True
            time.sleep(min(term() / 3, 5))


def _share_ranges(hop, eng, d, tables, chunk, ck):
    """Every table copied by every machine that runs the move
    (`share_tables: ranges`, `claims`): a table large enough is split once,
    by the first machine to reach it, and its ranges taken by every
    machine's workers; the rest are taken whole, one machine each, and
    taken over from where their machine left them once it stops renewing.
    Through the table copier every pair shares, whose every write is held
    to the number its taker holds (`fencing`). Returns once every table
    is done, by whichever machines."""
    from . import claims, ha, shared
    from .engines.hetero import HeteroEngine

    def log(m):
        console.print(f"  {m}")
    pair = eng if isinstance(eng, HeteroEngine) else eng._as_pair()
    store = shared.store_for(hop, log)
    there = pair.src_engine.neutral_tables("src", d)
    names = {t: next((n for n in there if pair._leaf(n) == t), t)
             for _, t in tables}
    keys = {t: pair.move_key(d, "", pair._leaf(names[t])) for t in names}
    # each machine starts at a table of its own, so the tables taken whole
    # are not all asked for by every machine at once
    turn = abs(hash(ha.mover())) % max(len(names), 1)
    said = False
    while True:
        ck.reload()
        left = [t for t in names if not (ck.get(keys[t]) or {}).get("done")]
        if not left:
            return
        left = left[turn % len(left):] + left[:turn % len(left)]
        moved = False
        for t in left:
            src = names[t]
            psch, _, ptbl = src.rpartition(".")
            way = claims.way(hop, d, keys[t], store,
                             lambda s=src: pair.splits_across_machines(d, s))

            def copy(s=src, a=psch, b=ptbl, k=keys[t]):
                with pair.load_window(d, log, {s}):
                    pair.move_table(d, a, b, chunk, ck, log)
                return int((ck.get(k) or {}).get("moved") or 0)
            if way == "ranges":
                copy()
                moved = True
            elif claims.whole(hop, d, keys[t], store, copy, log):
                _changelog(hop, {"op": "move", "db": d, "table": src,
                                 "shared": True})
                moved = True
        if not moved:
            if not said:
                log(f"{d}: {len(left)} tables are being copied by other"
                    " machines; waiting for them, and for any whose"
                    " machine stops renewing its claim")
                said = True
            time.sleep(min(ha.term() / 3, 5))


def _finisher(hop, d, ck):
    """The hop's lease for the one machine that completes a shared move,
    or None where another has completed it or is completing it."""
    from .lease import Held
    if ck.reload().get(COMPLETED):
        console.print(f"  {d}: every table is copied; another machine"
                      " completed the move")
        return None
    try:
        lk = _lock(hop)
    except Held:
        console.print(f"  {d}: every table is copied; another machine is"
                      " completing the move")
        return None
    if ck.reload().get(COMPLETED):
        lk.unlink()
        console.print(f"  {d}: every table is copied; another machine"
                      " completed the move")
        return None
    ck[COMPLETED] = {"done": True, "by": socket.gethostname()}
    ck.save()
    return lk


def _topology(hop):
    """(a stream back at cutover, streams both ways), from the hop's own
    options - off unless it says so, since both write to the source.

        reverse: at_cutover    tearing the stream down starts one back
        topology: two_way      both ways, all the time
    """
    opts = hop.options or {}
    rev = str(opts.get("reverse", "") or "").lower()
    top = str(opts.get("topology", "") or "").lower()
    if rev not in ("", "at_cutover"):
        raise SystemExit(f"reverse: {rev} - the one there is, is"
                         " at_cutover")
    if top not in ("", "one_way", "two_way"):
        raise SystemExit(f"topology: {top} - one_way or two_way")
    return rev == "at_cutover", top == "two_way"


def _the_other_way(hop, eng):
    """The hop and engine of the stream back, loop-safe where the engine
    has to be told."""
    from .engines import get_engine as _get
    back = hop.reversed()
    rev = _get(back)
    rev.loop_safe = True
    return back, rev


def _replicate(hop, eng, db, copy_data, do_drop, go):
    back_at_cutover, two_way = _topology(hop)
    if two_way and hasattr(eng, "loops_prevented"):
        why = eng.loops_prevented(db or (eng.databases() or [""])[0])
        if why:
            raise SystemExit(f"topology: two_way - {why}. Nothing was set"
                             " up.")
    if two_way:
        eng.loop_safe = True
    _replicate_one(hop, eng, db, copy_data, do_drop, go)
    if not (two_way or (back_at_cutover and do_drop)):
        return
    back, rev = _the_other_way(hop, eng)
    tdb = hop.target_db(db) if db else ""
    console.print("\n[bold]the other way[/bold]"
                  + (" - two-way, as the hop says" if two_way else
                     " - from the new primary back to the old source, as"
                     " the hop says (reverse: at_cutover)"))
    # at cutover the stream forward is gone first, and the one back starts
    # from now: what the application writes on the target from here on
    # reaches the old source, and nothing goes round
    _replicate_one(back, rev, tdb, False, do_drop and two_way, go)
    if two_way and not do_drop:
        # neither server's own replication reconciles a row both sides
        # change at once: each applies the other's change over its own, and
        # the two end holding each other's values (measured on MySQL 8.4
        # and PostgreSQL 16)
        console.print("  a row both sides change at once is not reconciled:"
                      " each takes the other's change over its own, and the"
                      " two can end holding different values - keep a row's"
                      " writes on one side at a time, and `migkit check`"
                      " finds any that differ")
    if go:
        _changelog(hop, {"op": "reverse" if not two_way else "two-way",
                         "db": db, "drop": bool(do_drop and two_way)})


def _own_refused(hop, eng, db):
    """Why the servers' own replication - a subscription, a replica -
    would carry this hop's changes wrongly, in words, or "": a table it
    filters, renames, maps the columns of or carries only some changes of
    (`movers.stream_cannot_carry` - the publication and the replica apply
    whole rows under the source's names, so a PostgreSQL hop with
    `where: {orders: "region='TH'"}` was carried whole, and a renamed table
    landed in a namesake), or a scope its own filters cannot hold
    (`native_scope_unsafe`). Asked only of a hop that says any of those;
    the ladder of change ways then takes migkit's own tail
    (`movers.stream_ways`)."""
    from . import movers
    from .config import scoped
    mapping = hop.mapping or {}
    said = []
    dbs = [db] if db else eng.databases()
    if any(mapping.get(k) for k in ("where", "tables", "columns", "ops")):
        refused = []
        for d in dbs:
            refused += movers.stream_cannot_carry(
                hop, d, eng.neutral_tables("src", d))
        if refused:
            said.append(
                "the servers' own replication applies whole rows under the"
                " source's names, and cannot carry what the hop asks for "
                + "; ".join(f"{t} ({why})" for t, why in refused[:5])
                + (f" and {len(refused) - 5} more" if len(refused) > 5
                   else ""))
    if scoped(hop):
        why = eng.native_scope_unsafe()
        if why:
            said.append(why)
    return "; ".join(said)


def _replicate_released(hop, eng, db, copy_data, do_drop, go):
    """`_replicate`, then - where tearing down starts a stream back - the
    target given back to the application, once that stream runs."""
    _replicate(hop, eng, db, copy_data, do_drop, go)
    if go and do_drop and _topology(hop)[0]:
        _protect_target(hop, eng, db, release=True)


def _replicate_one(hop, eng, db, copy_data, do_drop, go, left=None,
                   part=None):
    """`left`, where given, collects the source's half of a teardown that
    a read-only source refused - the cutover tears the stream down while
    the source is frozen, and runs those once it is written again.

    `part` runs one part of a stream made ahead (`replicate_sql`'s
    `arm_*`, `fire_*`): "arm" makes it and leaves it idle, "fire" starts
    it, "unarm" takes it away as `do_drop` does and its slot with it."""
    import secrets
    if part == "unarm":
        do_drop = True
    if eng.maps_columns() and not do_drop:
        # a subscription or a replica applies whole rows by the source's
        # column names: the first change to a mapped table stops it on a
        # column the target does not have, or lands in the wrong one - and
        # a renamed table's in the table it was renamed from
        raise SystemExit(
            "the hop maps columns or renames tables (mapping.columns,"
            " mapping.tables), and the server's own replication carries"
            " whole rows under the source's names."
            " Nothing was set up. `migkit move --mode cdc` follows this hop"
            " with migkit's own change tail, which applies the mapping.")
    refused = "" if do_drop else _own_refused(hop, eng, db)
    if refused:
        raise SystemExit(
            f"{refused}. Nothing was set up. `migkit move --mode cdc`"
            " follows this hop with migkit's own change tail, which reads"
            " the hop's scope and mapping for every change.")
    why = None if do_drop else eng.native_replica_unsafe()
    if why:
        raise SystemExit(
            f"{why}. Nothing was set up. "
            + ("`migkit move --mode cdc` follows this hop with migkit's own"
               " change tail, which stops on a schema change instead of"
               " carrying half of it." if hasattr(eng, "tail_apply")
               or _tail_pair(eng) is not None else
               "`migkit move --mode full` copies what the hop covers, and"
               " `migkit sync` brings the target level again before"
               " cutover."))
    dbs = [db] if db else eng.databases()
    # a plan that is only shown keeps the placeholder for the person who
    # will run it; one migkit runs gets a password nobody has seen - one per
    # run: it was drawn per database, and the second database's plan gave
    # the source's replication user a password the replica set up for the
    # first no longer had
    secret = secrets.token_urlsafe(24) if go and not do_drop else None
    for d in dbs:
        # every database before the first is written: a replica of the
        # whole server writes them all
        if copy_data and not do_drop:
            _keep_out(hop, eng, d, _tables_to_copy(hop, eng, d), go,
                      whole=True)
        else:
            _interlock(hop, eng, d, go)
    once = getattr(eng, "REPLICATES_THE_SERVER", False)
    for i, d in enumerate(dbs):
        if once and i:
            # one replica per server, scoped to every database of the hop;
            # setting it up again for the next one failed on a replica
            # that was already running
            console.print(f"[bold]{d}[/bold]\n  carried by the replica"
                          f" {'removed' if do_drop else 'set up'} for {dbs[0]}"
                          " - there is one per server")
            continue
        # a stream started on its own, after a copy migkit made: the plan is
        # told about that copy, so it can start where it ended - or say it
        # cannot
        copied = (_copy_record(hop, d)
                  if not copy_data and not do_drop else None)
        entered = _entered(hop, eng, d, copied) if copied else []
        if entered:
            # the server's replication starts where the copy stood and
            # carries what changes: a table the scope took in since would
            # hold only the rows changed from here
            said = (f"{d}: the hop's scope changed since the copy of"
                    f" {copied.get('at')} - {len(entered)} tables it takes"
                    " now were not copied as they are taken now ("
                    + ", ".join(f"{s}.{t}" if s else t
                                for s, t in entered[:4])
                    + (" ..." if len(entered) > 4 else "")
                    + "), and replication from the copy's position would"
                    " carry only what changes in them. Move the database"
                    " again with --mode full+cdc")
            if go:
                raise SystemExit(said)
            console.print(f"  with --go this stops: {_esc(said)}")
        sql = eng.replicate_sql(d, copy_data=copy_data, secret=secret,
                                copied=copied)
        console.print(f"[bold]{d}[/bold]")
        if part in ("arm", "fire"):
            plan = [("src", x) for x in sql.get(f"{part}_src") or []] + \
                   [("dst", x) for x in sql.get(f"{part}_dst") or []]
        elif do_drop:
            plan = [("dst", x) for x in sql["drop_dst"]] + \
                   [("src", x) for x in sql["drop_src"]
                    + (sql.get("unarm_src") or [] if part else [])]
        else:
            plan = [("src", x) for x in sql["src"]] + \
                   [("dst", x) for x in sql["dst"]]
        if sql.get("note"):
            console.print(f"  note: {_esc(sql['note'])}")
        for side, stmt in plan:
            shown = without_secret(stmt, hop.source.password)
            shown = without_secret(shown, secret)
            say_raw(f"  {side}: {shown}")
            if not go:
                continue
            try:
                eng.apply_replication_stmt(side, d, stmt)
            except Exception as e:  # noqa: BLE001 - only a frozen source's
                if left is None or side != "src" or not _read_only(e):
                    raise
                left.append(stmt)
                console.print("    left for when the source is written"
                              " again: it is frozen")
        if go and not do_drop:
            if part != "arm":
                # made ahead, it does not replicate yet: `armed_status`
                # says whether it stands ready instead
                console.print("  " + eng.replication_status(d,
                                                            sql["status"]))
            _changelog(hop, {"op": "replicate", "db": d,
                             **({"part": part} if part else {})})
            if copied:
                _copy_record_used(hop, d)
        if go and do_drop:
            _changelog(hop, {"op": "replicate-drop", "db": d,
                             **({"part": part} if part else {})})
    if not go:
        console.print("\ndry-run, add --go to execute"
                      " (monitor lag with: migkit watch)")


def _read_only(e):
    """Whether a server refused a statement for being read-only: MySQL's
    1290 and 1836, PostgreSQL's 25006."""
    said = " ".join(str(x) for x in getattr(e, "args", ()) or [e]).lower()
    return any(w in said for w in ("read-only", "read only", "read_only",
                                   "1290", "1836", "25006"))


def _follow(hop, eng, db, do_drop, go):
    """The CDC leg that runs from here rather than from the target.

    Reached by `MIGKIT_CDC=follow`, never by a flag: both paths carry the
    same changes, and which one is possible is a fact about the network
    between the two servers, not a preference.
    """
    from . import movers
    dbs = [db] if db else eng.databases()
    for d in dbs:
        console.print(f"[bold]{d}[/bold]")
        _interlock(hop, eng, d, go)
        if do_drop:
            for line in movers.follow_teardown(hop, d, go,
                                               lambda m: chat(f"  {m}")):
                console.print(f"  {line}")
            if go:
                _changelog(hop, {"op": "follow-drop", "db": d})
            continue
        for line in movers.pgcopydb_follow(hop, d, go,
                                           lambda m: chat(f"  {m}")):
            console.print(f"  {line}")
        if go:
            _changelog(hop, {"op": "follow", "db": d})
    if not go:
        console.print("\ndry-run, add --go to execute")
    elif not do_drop:
        console.print("[green]caught up to the end position[/green] -"
                      f" verify: migkit check {hop.name}")


def _tail_token(hop, db):
    if not db:
        raise SystemExit("cdc tail needs --db")
    return hop.report_dir(db) / "tail-token.json"


def _mark_move(hop, eng, d):
    """What the target says about now, kept for the check that asks later
    who wrote the rows only the target has (`who_wrote`)."""
    try:
        mark = eng.target_mark(d)
    except Exception:
        mark = {}
    (hop.report_dir(d) / "move-began.json").write_text(json.dumps(
        {"at": time.strftime("%Y-%m-%d %H:%M:%S"), "epoch": time.time(),
         **(mark or {})}))


def _position_before_copy(eng, d):
    """Where the source's change log is, taken before a full copy, where
    asking costs the source nothing - so that a later `--mode cdc` can start
    from it rather than from whenever it happens to be run."""
    src = getattr(eng, "src_engine", None) or eng
    if not getattr(src, "CHANGE_POINT_READS_ONLY", False):
        return None
    try:
        return src.copy_point("src", d)
    except (Exception, SystemExit):
        return None


def _copy_record_path(hop, d):
    return hop.report_dir(d) / "copy-position.json"


def _record_copy(hop, d, before):
    """What a later `--mode cdc` needs to know about the copy that just
    finished: when, the change position taken before it, and - where the
    copy was a snapshot at a position of its own - that position; and the
    scope it copied (`Hop.scope_record`), so a stream started after the
    scope took more in copies that first (`_entered`)."""
    rec = {"at": time.strftime("%Y-%m-%d %H:%M:%S"), "before": before,
           "scope_fingerprint": hop.scope_fingerprint(),
           "scope": hop.scope_record()}
    dumped = hop.report_dir(d) / "dump-position.json"
    if dumped.exists():
        try:
            rec["exact"] = json.loads(dumped.read_text())
        except ValueError:
            pass
        dumped.unlink()
    _copy_record_path(hop, d).write_text(json.dumps(rec, default=str))
    shared = _shared(hop, d)
    if shared:
        shared.push(force=True)


def _copy_record(hop, d):
    try:
        return json.loads(_copy_record_path(hop, d).read_text())
    except (OSError, ValueError):
        return None


def _entered(hop, eng, d, rec):
    """[(schema, table)] the hop takes now that the copy `rec` did not
    carry as they are taken now: tables its scope then left out, or
    filtered, chose the columns of or named otherwise - every table where
    the database lands elsewhere now. [] where the copy was taken under
    this scope, or kept none (it predates scopes, and is taken as it
    always was)."""
    if not rec or not hop.scope_changed(rec):
        return []
    then = hop.under(rec)
    if then is None:
        return []
    tables = [(sch, t) for sch, t in eng.list_move_tables(d)
              if not hop.excluded(d, *[p for p in (sch, t) if p])]
    if then.target_db(d) != hop.target_db(d):
        return tables
    out = []
    for sch, t in tables:
        parts = [p for p in (sch, t) if p]
        if (then.excluded(d, *parts)
                or then.row_filter(d, *parts) != hop.row_filter(d, *parts)
                or then.column_rules(d, *parts) != hop.column_rules(d, *parts)
                or then.target_table_in(d, *parts)
                != hop.target_table_in(d, *parts)):
            out.append((sch, t))
    return out


def _copy_entered(hop, eng, d, rec, go):
    """The tables the scope took in since the copy `rec` (`_entered`),
    copied before the stream that follows it starts: a stream carries
    only what changes, and a table it began to carry with no copy held
    only the rows changed since. The tail replays by key, on top of them,
    what changed since the copy's position, which converges. Returns how
    many."""
    entered = _entered(hop, eng, d, rec)
    if not entered:
        return 0
    names = [f"{s}.{t}" if s else t for s, t in entered]
    console.print(f"  {_esc(d)}: the hop's scope changed since the copy of"
                  f" {_esc(rec.get('at'))} - {len(names)} tables it takes"
                  " now were not copied as they are taken now ("
                  + _esc(", ".join(names[:4]))
                  + (" ..." if len(names) > 4 else "")
                  + ("), copied before the stream starts" if go else
                     "); with --go they are copied before the stream"
                     " starts"))
    if not go:
        return len(entered)
    # the rows the target already holds in them, as the hop says to treat
    # them: a table that was left out was the target's own
    entered = _keep_out(hop, eng, d, entered, go)
    if not entered:
        return 0

    def log(m):
        console.print(f"  {m}")
    eng.create_missing(d, log)
    ck = _checkpoint(hop, hop.report_dir(d) / "move-entered.json")
    ck.clear()
    ck.discard()
    with eng.load_window(d, log, {t for _, t in entered}):
        for sch, t in entered:
            _copy_table(eng, d, sch, t, 500000, ck, log)
            _changelog(hop, {"op": "move-entered", "db": d,
                             "table": f"{sch}.{t}" if sch else t})
    ck.discard()
    return len(entered)


def _copy_record_used(hop, d):
    """A position is where one stream starts, once: a second `--mode cdc`
    started from it would replay what the first already applied."""
    path = _copy_record_path(hop, d)
    if path.exists():
        path.rename(path.with_name("copy-position.used.json"))
    from .state import run_state
    remote = run_state(hop)
    if remote is not None:
        remote.delete(path)


def _tail(hop, eng, db, go):
    from . import ha
    if go and ha.standby(hop) and ha.held(hop) is None:
        # the machines shared the copy; the stream is one machine's, the
        # others standing by
        return ha.run_led(hop, lambda: _tail_held(hop, eng, db, go),
                          lambda m: console.print(m))
    return _tail_held(hop, eng, db, go)


def _tail_held(hop, eng, db, go):
    path = _tail_token(hop, db)
    _interlock(hop, eng, db, go)
    shared = _shared(hop, db)
    rec = _copy_record(hop, db)
    if not path.exists() and rec and rec.get("before") is not None \
            and hasattr(eng, "tail_seed"):
        # the position taken before the copy: what changed while it ran
        # and since is replayed by key on top of it, which converges -
        # and on top of the tables the scope took in since, copied first
        _copy_entered(hop, eng, db, rec, go)
        if go:
            eng.tail_seed(db, path, rec["before"])
            _copy_record_used(hop, db)
        if rec.get("physical"):
            # a copy of the pages holds the source to one position exactly
            console.print(f"  {db}: starting from the position the copy of"
                          f" {rec['at']} holds, so nothing is applied twice"
                          " and nothing is missed")
        else:
            console.print(f"  {db}: starting from where the source's log was"
                          f" before the copy of {rec['at']}, so what changed"
                          " during it and since is carried too")
    elif not path.exists() and not getattr(
            eng, "TAIL_GOES_ON_FROM_THE_COPY", False) and (rec or _checkpoint(
            hop, hop.report_dir(db) / "move.json")):
        # a copy with no position is the hole full+cdc closes; on its own,
        # cdc can only say it is there
        console.print(
            f"[yellow]{db} was copied by an earlier run that saved no change"
            " position. This tail starts from now, so what the source changed"
            " between that copy and now is carried by nothing - `migkit"
            " check` will show it, or move again with --mode full+cdc, which"
            " takes the position first[/yellow]")
    if shared and go:
        shared.push()
        shared.start()
    try:
        eng.tail_apply(db, go, path, lambda m: console.print(m))
    finally:
        if shared and go:
            shared.stop()


def _two_way_teardown(hop, eng, db, go):
    """`move --mode cdc --drop` of a two-way tail: what its marks left on
    the target taken away (`marks`) - after the tail has stopped, since a
    tail still applying would only put it back."""
    from . import tailctl
    tailer = eng if hasattr(eng, "two_way_teardown") else _tail_pair(eng)
    for d in ([db] if db else (hop.databases or eng.databases())):
        path = _tail_token(hop, d)
        console.print(f"[bold]{d}[/bold]")
        if go and tailctl.alive(path.parent):
            raise SystemExit(f"{d}: the tail is still running; stop it"
                             " first, then take its marks away")
        steps = tailer.two_way_teardown(d, path, go)
        for s in steps or ["nothing of its marks stays on the target"]:
            console.print(f"  {s}")
        if go:
            _changelog(hop, {"op": "two-way-drop", "db": d})
    if not go:
        console.print("\ndry-run, add --go to execute")


def _tail_pair(eng):
    """The change tail for an engine that reads its own change log and can
    apply one, run as a pair of itself - or None where it cannot."""
    from .engines.base import Engine
    if (type(eng).neutral_changes is Engine.neutral_changes
            or type(eng)._apply_upsert is Engine._apply_upsert):
        return None
    return eng._as_pair()


def _tail_before_copy(hop, eng, db):
    """Where the tail starts, fixed before the copy it follows.

    Taken after the copy, it skips everything the source changed while the
    rows were being read: the tail begins at the end of the log, the copy
    read the table before those changes, and nothing downstream can tell
    such a row from one that never changed.

    A copy an earlier run finished with no position saved has the same hole
    from the other side - what changed on its tables since is in no log this
    tail will read - so those tables are copied again rather than trusted.
    """
    path = _tail_token(hop, db)
    shared = _shared(hop, db)
    ck = _checkpoint(hop, hop.report_dir(db) / "move.json")
    if ck and not path.exists() and not getattr(
            eng, "TAIL_GOES_ON_FROM_THE_COPY", False):
        console.print(f"  {db}: an earlier copy saved no change position, so"
                      " what changed on its tables since cannot be carried -"
                      " copying them again")
        ck.discard()
    eng.tail_start(db, path)
    if shared:
        shared.push()


def _tail_ready(eng):
    """The in-process change tail needs an optional driver on some engines.
    When it is missing, migkit uses its own streaming pipeline rather than
    telling the operator to go and pip install something."""
    mod = getattr(eng, "tail_requires", "")
    if not mod:
        return True
    import importlib.util
    return importlib.util.find_spec(mod) is not None


def _stream(hop, eng, db, do_drop, go, engine):
    """migkit's own streaming pipeline, used when the engine has no native
    CDC path. Nothing about the runtime underneath is a user-facing choice:
    migkit writes it, starts it, registers the connectors and tears it down."""
    from . import movers
    dbs = [db] if db else eng.databases()
    for d in dbs:
        _interlock(hop, eng, d, go)
    out = movers.stream_codegen(hop, dbs, engine)
    name = f"migkit-{hop.name}"
    if do_drop:
        if not go:
            console.print(f"would tear down the streaming pipeline for"
                          f" {hop.name} (dry-run, add --go)")
            return
        movers.stream_down(out, lambda m: chat(f"  {m}"))
        _changelog(hop, {"op": "stream-down", "db": ",".join(dbs)})
        console.print("[green]streaming pipeline torn down[/green]")
        return
    console.print(f"streaming pipeline prepared: {out}")
    if not go:
        console.print("dry-run, add --go to start it")
        return
    console.print("starting the streaming pipeline ...")
    movers.stream_up(out, lambda m: chat(f"  {m}"))
    if not movers.stream_wait(log=lambda m: chat(f"  {m}")):
        raise SystemExit("the streaming pipeline did not come up (its stack"
                         f" is written under {out}); migkit doctor says what"
                         " this machine is missing")
    movers.stream_register(out, log=lambda m: console.print(f"  {m}"))
    for c in (f"{name}-source", f"{name}-sink"):
        console.print("  " + movers.stream_status(c))
    # a pipeline started again says how far its reads have got
    for line in movers.stream_progress(out, hop.name)[0]:
        console.print(f"  {line}")
    _changelog(hop, {"op": "stream-up", "db": ",".join(dbs)})
    console.print(f"[green]cdc streaming[/green] - progress: migkit watch"
                  f" {hop.name}   verify: migkit watch {hop.name}"
                  " --verify --delta")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--table", default="", help="schema.table (full mode)")
@click.option("--mode", type=click.Choice(["full", "cdc", "full+cdc"]),
              default="full",
              help="full = bulk copy, cdc = follow live changes,"
                   " full+cdc = initial load plus stream until cutover")
@click.option("--chunk", default=500000,
              help="rows per resumable chunk, when a resumable copy is used")
@click.option("--drop", "do_drop", is_flag=True,
              help="cdc modes: tear down replication")
@click.option("--go", is_flag=True, help="actually run, default shows the plan")
def move(hop_name, db, table, mode, chunk, do_drop, go):
    """Move data for this hop. migkit picks how.

    full: the fastest bulk path available for the engine, falling back to a
    chunked copy that resumes after a crash. A single table always takes the
    resumable path - restartability matters more than raw speed there.
    cdc: the engine's native change feed where it has one, otherwise migkit
    stands up its own streaming pipeline.

    There is nothing to choose: the same command does the right thing on every
    engine, and `migkit doctor` says what this machine can do. Whatever moves
    the data, migkit verifies it.

    Use only over a trusted network (or run migkit on a cloud VM)."""
    from .verdict import Usage
    if do_drop and mode != "cdc":
        # it asked for the cutover's approval and tore nothing down (full),
        # or tore the stream down outside the cutover (full+cdc)
        raise Usage(f"--drop tears the stream down: migkit move {hop_name}"
                    " --mode cdc --drop")
    if table and mode == "cdc":
        raise Usage("--table narrows a copy; a stream follows the whole"
                    " database (--mode full or full+cdc)")
    if _given("chunk") and mode == "cdc":
        raise Usage("--chunk sizes the ranges of a copy; --mode cdc copies"
                    " nothing")
    return _move_run(hop_name, db, table, mode, chunk, do_drop, go)


def _move_run(hop_name, db, table, mode, chunk, do_drop, go):
    """`move`, with what stopped it said in migkit's words - on a hop with
    a standby, by the machine that holds it, this one standing by until
    it does (`ha`). Where the machines share the copy, only the change
    stream after it is held so (`_tail`)."""
    from . import ha
    hop = get_hop(hop_name)
    if go and not do_drop and ha.standby(hop) \
            and not (hop.options or {}).get("share_tables"):
        return ha.run_led(hop, lambda: _move_worded(
            hop_name, db, table, mode, chunk, do_drop, go),
            lambda m: console.print(m))
    return _move_worded(hop_name, db, table, mode, chunk, do_drop, go)


def _move_worded(hop_name, db, table, mode, chunk, do_drop, go):
    from . import decide
    try:
        return _move(hop_name, db, table, mode, chunk, do_drop, go)
    except decide.Exhausted as e:
        # every way was tried and each said why; the last failure's own
        # words are asked of the engine as any stopped copy's are
        from .movers import DRIVEN
        from .wording import without_programs
        why = ""
        if e.__cause__ is not None:
            try:
                why = get_engine(get_hop(hop_name)).why_it_stopped(
                    str(e.__cause__))
            except Exception:  # noqa: BLE001 - no more to say
                why = ""
        raise SystemExit(
            without_programs(str(e), DRIVEN) + "\n"
            + (f"{why}\n" if why else "")
            + "Nothing past the last finished table is marked as moved. The"
            " commands that ran are in the run's commands.log.") from e
    except OSError as e:
        import errno
        if e.errno != errno.ENOSPC:
            raise
        # this machine's own disk, not a server's: the checkpoint is written
        # beside itself and put in place whole, so what it said stands
        raise SystemExit(
            f"the move stopped: no space left on this machine for"
            f" {getattr(e, 'filename', None) or 'its report directory'}."
            " Free space there and move again; the checkpoint is as it was"
            " before the write that did not fit, and the next run goes on"
            " from it.")
    except Exception as e:
        # a program underneath failed, or a server refused a row: the
        # message keeps the database's words and loses any program's name,
        # and nobody gets a traceback
        if not (isinstance(e, RuntimeError) or _refused_by_a_server(e)):
            raise
        from .movers import DRIVEN
        from .wording import without_programs
        if _refused_by_a_server(e) and len(e.args) == 2 \
                and isinstance(e.args[0], int):
            e = RuntimeError(f"ERROR {e.args[0]}: {e.args[1]}")
        try:
            why = get_engine(get_hop(hop_name)).why_it_stopped(str(e))
        except Exception:
            why = ""
        raise SystemExit(
            f"the copy stopped: {without_programs(str(e), DRIVEN)}\n"
            + (f"{why}\n" if why else "")
            + "Nothing past the last finished table is marked as moved. The"
            " commands that ran are in the run's commands.log.")


def _refused_by_a_server(e):
    """An error a database driver raised: the server's own refusal, whose
    words are the message, not a fault in migkit to show a traceback for."""
    return (type(e).__module__ or "").split(".")[0] in (
        "pymysql", "psycopg", "psycopg2", "pymongo", "sqlite3", "pymssql",
        "pyodbc", "redis", "kafka")


def _protect_target(hop, eng, db, release=False):
    """The hop's `protect_target`: the application's roles cannot write to
    the target while migkit moves data into it, and can again once the
    stream is torn down at cutover (or on `rollback --apply`)."""
    from . import freeze
    if not (hop.options or {}).get("protect_target"):
        return
    if not freeze.supported(hop):
        raise SystemExit(
            f"protect_target is set, and keeping the application from"
            f" writing to the target is not available for {hop.engine} hops"
            " yet - nothing has been copied. Remove the option to move"
            " without it.")
    for d in ([db] if db else eng.databases()):
        (freeze.thaw if release else freeze.freeze)(
            hop, eng, d, lambda m: console.print(f"  {m}"))


def _carried_physically(hop, eng, db, mode, go):
    """The move seeded from a copy of the source's pages or a cloud's own
    copy, where the hop allows it and a physical rung is chosen
    (`physical`) - True where it was, and the move is done. A hop without
    `physical:` never reaches the servers here."""
    from . import physical
    if mode not in ("full", "full+cdc") or not physical.settings(hop):
        return False
    from . import ha
    if go and ha.standby(hop):
        # a copy of the pages goes around every writer migkit holds to
        # the holder's number, so a machine that lost the hop could not be
        # refused on the target
        raise SystemExit(
            f"{hop.name}: a seed from a copy of the source's pages cannot"
            " be held to the number of the machine that holds the hop, so"
            " it is not run with standby on. Seed it from one machine"
            " with standby off, then turn standby on for the change stream"
            " (--mode cdc). Nothing was written")
    import sys
    lk = _lock(hop) if go else None
    try:
        console.print("[bold]physical seed[/bold]")
        # a change on the source a faster way needs is asked here, and
        # only at a terminal: the one question migkit asks (`sourcechange`)
        if not physical.carry(hop, eng, db, mode, go,
                              lambda m: console.print(f"  {m}"),
                              ask=_ask_change if sys.stdin.isatty()
                              else None):
            return False
    finally:
        if lk:
            lk.unlink()
    if not go:
        console.print("\ndry-run, add --go to execute")
        return True
    _changelog(hop, {"op": "move-physical", "db": db, "mode": mode})
    # the copy carried the source's roles and grants, and none of what a
    # freeze of the old target had done
    _protect_target(hop, eng, db)
    if mode == "full+cdc" and not getattr(eng, "REPLICA_COPIES", False):
        tailer = eng if hasattr(eng, "tail_apply") else _tail_pair(eng)
        if db and tailer is not None:
            _tail(hop, tailer, db, go)
        else:
            for d in eng.databases():
                console.print(f"  {d}: its changes go on from the copy's own"
                              f" position - migkit move {hop.name} --mode"
                              f" cdc --db {d} --go")
        return True
    console.print("[green]seeded[/green] - verify: migkit check"
                  f" {hop.name}")
    return True


def _carried_on_media(hop, eng, db, mode, go):
    """The move seeded on media, where the hop names a carrier and the
    decision engine takes it (`ship`): the export where no sealed seed is
    found, the load where one is - True where it was, and the move is
    this one's. A hop without `ship:` never reaches it."""
    from . import ship
    if mode not in ("full", "full+cdc") or not ship.settings(hop):
        return False
    lk = _lock(hop) if go else None
    try:
        console.print("[bold]seed on media[/bold]")
        done = ship.carry(hop, eng, db, mode, go,
                          lambda m: console.print(f"  {m}"))
    finally:
        if lk:
            lk.unlink()
    if not done:
        return False
    if not go:
        console.print("\ndry-run, add --go to execute")
        return True
    loaded = []
    for d, got in done.items():
        console.print(f"  {d}: {got.said}")
        if got.half == "export":
            _changelog(hop, {"op": "move-seed-export", "db": d})
            console.print(f"  {d}: ship the carrier, then run the same"
                          " command where the target is: migkit move"
                          f" {hop.name} --mode {mode} --go")
            continue
        _changelog(hop, {"op": "move-seed-load", "db": d})
        _refuse_if_nothing_arrived(hop, eng, d, "the seed on media",
                                   lambda m: console.print(f"  {m}"))
        _record_copy(hop, d, got.position)
        settled = eng.settle_target(d)
        if settled:
            console.print(f"  {settled}")
        loaded.append(d)
    if loaded:
        _protect_target(hop, eng, db)
    if mode == "full+cdc" and loaded:
        tailer = eng if hasattr(eng, "tail_apply") else _tail_pair(eng)
        if db and tailer is not None:
            _tail(hop, tailer, db, go)
        else:
            for d in loaded:
                console.print(f"  {d}: its changes go on from the seed's"
                              f" position - migkit move {hop.name} --mode"
                              f" cdc --db {d} --go")
    elif loaded:
        console.print("[green]seeded[/green] - verify: migkit check"
                      f" {hop.name}")
    return True


def _move(hop_name, db, table, mode, chunk, do_drop, go):
    from . import cutover, hopkeys, movers
    from . import model as resource_model
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    # measured once for this run, and afresh for the next
    resource_model.forget()
    teardown = mode == "cdc" and do_drop
    if not go and not teardown:
        _say_goal(hop, hopkeys.explain_mode("move", mode))
    elif not teardown and hop.goal == "cutover" \
            and not hopkeys.cutover_on(hop):
        # every time: the goal is a cutover and migkit will not drive it
        for line in hopkeys.cutover_lines(hop):
            console.print(f"  {line}")
    elif teardown:
        console.print("  " + hopkeys.explain_mode("move", mode, True))
    back_at_cutover, two_way = _topology(hop)
    from . import twoway
    two_way = two_way or twoway.settings(hop) is not None
    if mode == "cdc" and do_drop and (cutover.settings(hop) or not two_way):
        # tearing the stream down is the cutover (`migkit.cutover`), which
        # holds its own approvals and the target's identity; a hop
        # streaming both ways has no side to move from, and tears down as
        # it always has
        _guard_write(hop, eng, "a move", None, go, mode=mode, drop=True)
        from .engines import ALIASES
        return _cutover(hop, eng, db, go, ALIASES.get(hop.engine, hop.engine))
    from . import rehearsal, subset
    rehearsal.mark(hop)
    if not go and mode != "cdc":
        # the case between the two servers first - a fork change, a version
        # down, an edition change - and the way it decides
        from . import case as server_case
        from . import upgrade
        try:
            for line in server_case.plan_lines(
                    hop, eng, db or (eng.databases() or [""])[0]):
                console.print(f"  {line}")
        except Exception as e:  # noqa: BLE001 - the plan goes on
            console.print(f"  the case could not be worked out:"
                          f" {upgrade._quiet(str(e).strip()[:160])}")
    # everything that would stop this step - the goal, a hop that masks,
    # what is known now to stop it before its end, the approval tearing a
    # stream down is, the target's identity with its grants - before the
    # target is written to at all (`_guard_write`)
    _guard_write(hop, eng, "a move", [db] if db else eng.databases(), go,
                 mode=mode, drop=do_drop,
                 step="cutover" if do_drop else None, approve=[db],
                 preflight=mode, db=db)
    if not go:
        from . import upgrade
        if upgrade.settings(hop) is not None and mode != "cdc":
            # the way the upgrade goes, and what an upgrade in place on the
            # source would have cost
            try:
                for line in upgrade.plan_lines(
                        hop, eng, db or (eng.databases() or [""])[0]):
                    console.print(f"  {line}")
            except Exception as e:  # noqa: BLE001 - the plan goes on
                console.print(f"  upgrade: the way could not be worked"
                              f" out: {upgrade._quiet(str(e).strip()[:160])}")
    if subset.settings(hop) is not None:
        return subset.move(hop, eng, [db] if db else eng.databases(),
                           mode, table, go,
                           lambda m: console.print(f"  {m}"))
    from . import archive, consolidate
    if consolidate.settings(hop) is not None:
        return consolidate.move(hop, eng, [db] if db else eng.databases(),
                                mode, table, go,
                                lambda m: console.print(f"  {_esc(m)}"))
    if archive.settings(hop) is not None:
        import sys
        # the source's leave for its archived rows is asked only at a
        # terminal (`sourcechange.settle_named`)
        return archive.move(hop, eng, [db] if db else eng.databases(),
                            mode, table, go,
                            lambda m: console.print(f"  {_esc(m)}"),
                            ask=_ask_change if sys.stdin.isatty() else None)
    if not table and not do_drop and _carried_physically(hop, eng, db, mode,
                                                         go):
        return
    if not table and not do_drop and _carried_on_media(hop, eng, db, mode,
                                                       go):
        return
    if go and not (do_drop and mode == "cdc" and back_at_cutover):
        # before anything is copied, so no write of the application's lands
        # between the copy and the check; released by tearing the stream
        # down, which is the cutover - after the stream back is running,
        # where the hop asks for one, so no write of the application's
        # lands before it
        _protect_target(hop, eng, db, release=do_drop and mode == "cdc")
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    if mode == "full":
        # before the mover is chosen: a table the hop keeps as it is has to
        # be left out, and a mover that cannot leave one out is not chosen
        dbs = [d for d in ([db] if db else eng.databases())
               if _keep_out(hop, eng, d,
                            _tables_to_copy(hop, eng, d, table),
                            go) is not None]
        from . import legs
        base, passed = movers.for_hop(hop, engine, table, go)
        v, why = movers.fitted(hop, engine, base)
        if v != "builtin":
            # a hop with a standby is copied only by a way whose every
            # write the target holds to the holder's number (`ha`)
            from . import ha
            v = ha.copier(hop, v, "the bulk load",
                          lambda m: console.print(f"  {m}"))
        if passed and not legs.direct(v) and not table:
            # the hop asked for the servers to pull from each other: why
            # they do not is the first thing the operator reads
            console.print(f"  the rows go through this machine: {passed}")
        if why:
            console.print(f"  {why}, so the tables are copied one by one")
        if v != "builtin":
            # the dry run climbs the ways `--go` climbs, reading what
            # earlier runs gave up on and writing nothing: it named the
            # bulk way where `--go` would take the copier (`decide.attempt`)
            for d in dbs:
                _bulk_or_copier(hop, eng, d, v, table, chunk, engine, go)
            return
        return _move_full(hop, eng, db, table, chunk, go)
    has_repl = hasattr(eng, "replicate_sql")
    has_tail = hasattr(eng, "tail_apply")
    from . import twoway
    if mode == "cdc" and do_drop and twoway.settings(hop) is not None \
            and (has_tail or _tail_pair(eng) is not None):
        return _two_way_teardown(hop, eng, db, go)
    if mode == "cdc":
        return _cdc(hop, eng, db, do_drop, go, engine)
    # full+cdc
    if has_repl and getattr(eng, "REPLICA_COPIES", False) \
            and not eng.maps_columns() \
            and (do_drop or not _own_refused(hop, eng, db)):
        # a subscription with copy_data=true, or a Redis replica, copies
        # first and follows after: the native full+cdc - never for a hop
        # it would copy whole and follow whole where the hop filters or
        # renames (`_own_refused`), which the copy and the tail below take
        return _replicate(hop, eng, db, True, do_drop, go)
    # MySQL used to print a replica plan here, from a position taken before
    # the copy. The copy is not a snapshot at that position, and a replica
    # started from it stopped on the first row the copy had already carried
    # (1062, measured on 8.4) and applied nothing after. The tail applies
    # by key, so replaying that stretch converges instead.
    tailer = eng if has_tail else _tail_pair(eng)
    if tailer is not None and hasattr(eng, "move_table"):
        from . import ha
        if go and ha.copied(hop, db):
            # taken over after the copy was finished: the stream goes on
            # from where the last holder stood, as a restart of it would -
            # the tables are not checked again against a source that has
            # moved on since, which would copy them again
            console.print(f"  {db}: the copy was finished before this"
                          " machine took the move over; the change stream"
                          " goes on from where it stood")
            return _tail(hop, tailer, db, go)
        if go:
            _tail_before_copy(hop, tailer, db)
        _move_full(hop, eng, db, table, chunk, go)
        if go:
            return _tail(hop, tailer, db, go)
        # not "then run --mode cdc": a tail started after the copy has
        # skipped what changed during it
        console.print("with --go the copy is followed by the change tail,"
                      " from a position taken before the copy starts")
        return
    capabilities.require(hop.engine, "stream")
    capabilities.require(hop.engine, "table-copy")
    raise SystemExit(f"moving and then following a {hop.engine} source is"
                     " not set up on this machine; migkit doctor says what"
                     " is missing")


def _bulk_db(hop, eng, d, v, chunk, go, engine):
    """One database by the bulk way `v`: its plan, and with `go` the copy,
    the tables it routes to the copier, and the proofs after it."""
    from . import drift, machine, movers, sizing
    from . import model as resource_model
    # before anything is copied: a filter this mover cannot apply would
    # silently move every row
    movers.refuse_unpushable_filters(hop, d, v, engine)
    if go:
        eng.refuse_unfit(d)
    else:
        # what `--go` stops on before it writes, said by the plan
        _would_stop(lambda: eng.refuse_unfit(d))
        _would_stop(lambda: _programs_ready(hop, d, v))
    console.print(f"[bold]{_esc(d)}[/bold] bulk copy:")
    before = drift.shape(eng, "src", d) if go else None
    for line in _table_plan(hop, eng, d, v, go):
        console.print(line)
    began, (rows, narrowed) = time.time(), (_source_rows(eng, d) if go
                                             else (0, []))
    spent = machine.Spent()
    point = _position_before_copy(eng, d) if go else None
    if go:
        _mark_move(hop, eng, d)
    # the model the plan above measured; the dump's and the load's jobs
    # each sized to their own side
    sizing.fit(hop, eng, d, lambda m: chat(f"  {m}"), units=_units(eng, d),
               model=resource_model.for_move(hop, eng, d))
    steps = movers.run_via(v, hop, d, hop.workers, go,
                           lambda m: chat(f"  {m}"))
    # the way that ran, which is another where the one asked for handed
    # its work on (`movers.Ran`): its rate and its record are that way's
    ran = getattr(steps, "via", v)
    if go:
        _record_run(hop, eng, d, ran, rows, began, spent, narrowed)
    for s0 in steps:
        console.print(f"  {s0}")
    if not go:
        return
    # before the "moved nothing" guard: the bulk copy left these tables
    # out on purpose, so until the copier has run they are empty by design
    _copy_routed(hop, eng, d, v, chunk, lambda m: chat(f"  {m}"))
    _stop_if_the_schema_moved(hop, eng, d, before)
    _refuse_if_nothing_arrived(hop, eng, d, "the bulk copy",
                               lambda m: chat(f"  {m}"))
    _held_to_the_source(hop, eng, d, lambda m: chat(f"  {m}"))
    _changelog(hop, {"op": f"move-{ran}", "db": d})
    _record_copy(hop, d, point)
    # the load left the statistics behind it; the engine puts them right
    # before anybody queries the target
    settled = eng.settle_target(d)
    if settled:
        console.print(f"  {settled}")
    _after_copy(hop, eng, d)
    console.print(f"[green]{d}: bulk copy complete[/green], run migkit"
                  " check to verify")


def _would_stop(fn):
    """A guard `--go` stops on, asked by the plan: what it would say,
    said, and the plan goes on."""
    try:
        fn()
    except SystemExit as e:
        if isinstance(e.code, str) and e.code.strip():
            console.print("  with --go this stops: "
                          + _esc(e.code.strip().splitlines()[0]))


def _programs_ready(hop, d, v):
    """What `--go` asks of the programs way `v` runs before it starts them
    (`movers.run_via`), asked here for the plan: one an earlier move
    started and left running, and options the installed build does not
    take."""
    from . import movers
    left = movers._still_running(hop.report_dir(d) / "running-programs.json")
    if left:
        raise SystemExit(f"a program an earlier move of {d} started is"
                         f" still running (process {left[0][0]}) and"
                         " writing; a move beside it would empty the"
                         " tables under it")
    if movers.options_missing(hop, d, v):
        raise SystemExit("the programs installed here do not take options"
                         " the move passes them - migkit doctor --install"
                         " puts builds that do in place")


def _bulk_or_copier(hop, eng, d, v, table, chunk, engine, go=True):
    """One database by the bulk way chosen, or by the table copier where
    that way fails as it runs - a program that exits or crashes, a server
    that refuses it, a connection lost (`decide.attempt`): switched to by
    itself and said, never asked.

    Only the copier is under it, not the next bulk way: the copier
    replaces each range by key, so it converges on whatever a failed load
    left in a table, where another bulk way would load over it. What the
    copy is held to - the filters, the rows the hop keeps, the proofs
    after it - is the same on both. The bulk way is kept given up beside
    the database's checkpoint until its program's version changes.

    Without `go` the plan climbs the same ladder, reading the ways given
    up and writing nothing (`record`), so it names the way `--go` takes."""
    from . import decide, movers
    ladder = {r.name: r for r in movers._ladder()}
    ran = decide.Need(
        "its programs as installed now", lambda f: f["programs"] is not None)
    rungs = (decide.Rung(v, ladder[v].said if v in ladder else v,
                         needs=(ran,)),
             decide.Rung("builtin", ladder["builtin"].said))
    facts = decide.Facts(programs=lambda: {
        p: movers.program_version(p) if movers.which(p) else None
        for p in movers.programs(v)})

    def run(choice):
        if choice.rung.name == "builtin":
            return _move_full(hop, eng, d, table, chunk, go)
        if not go:
            return _bulk_db(hop, eng, d, v, chunk, False, engine)
        lk = _lock(hop)
        try:
            return _bulk_db(hop, eng, d, v, chunk, True, engine)
        finally:
            lk.unlink()
    decide.attempt(f"the copy of {d}", rungs, run, facts,
                   where=hop.report_dir(d) / "move.json",
                   log=lambda m: console.print(f"  [yellow]{_esc(m)}[/yellow]"),
                   record=go)


def _cdc(hop, eng, db, do_drop, go, engine):
    """The hop's change stream set up, or torn down, on the way the
    ladder of ways gives (`movers.stream_ways`)."""
    from . import movers
    has_repl = hasattr(eng, "replicate_sql")
    has_tail = hasattr(eng, "tail_apply")
    if eng.maps_columns():
        # the servers' own replication carries whole rows; the pair's tail
        # applies the mapping to every change (`_replicate` says the same)
        tailer = _tail_pair(eng)
        if tailer is None:
            capabilities.require(hop.engine, "stream")
        return _tail(hop, tailer, db, go)
    from . import decide

    def risk():
        # a table taken into a running publication below the fixed minor
        # releases: migkit's own tail alters no publication
        if not hasattr(eng, "publication_add_risk"):
            return ""
        return "; ".join(r for d in ([db] if db else eng.databases())
                         for r in [eng.publication_add_risk(d)] if r)

    # what the hop asks that the servers' own replication cannot carry,
    # read once; migkit's own tail of the engine as a pair of itself then
    # follows the hop (`_tail_pair`), as it does a hop that maps columns
    said = {}

    def refused():
        if "why" not in said:
            said["why"] = "" if do_drop else _own_refused(hop, eng, db)
        return said["why"]

    def by_pair():
        if has_tail or not refused():
            return False
        pair = _tail_pair(eng)
        return pair is not None and _tail_ready(pair)
    facts, rungs = _stream_facts(hop, eng, engine, has_repl, has_tail,
                                  do_drop, risk, refused, by_pair)
    where = hop.report_dir(db) / "stream.json"
    work = f"following {hop.name}'s source"
    # a way refused as it starts - a target that cannot dial the source -
    # is switched from by itself; one that fails after it began carrying
    # changes stops, since its position is its own (`decide.only_blocked`)
    facts["own replication failed"] = any(
        n in decide.given_up(where, work) for n in ("subscription",
                                                    "replica"))

    def undo(rung, error):
        if rung.name in ("subscription", "replica"):
            facts["own replication failed"] = True

    def run(choice):
        path = choice.rung.name
        if path == "follow":
            return _follow(hop, eng, db, do_drop, go)
        if path in ("subscription", "replica"):
            return _replicate_released(hop, eng, db, False, do_drop, go)
        if path == "tail":
            return _tail(hop, eng if has_tail else _tail_pair(eng), db, go)
        if has_tail:
            console.print("[yellow]native change tail needs a driver this"
                          " machine does not have - using migkit's"
                          " streaming pipeline instead[/yellow]")
        return _stream(hop, eng, db, do_drop, go, engine)
    try:
        # the dry run reads the ways given up here and writes nothing:
        # it passed `where` over and could name a way `--go` would not take
        return decide.attempt(
            work, rungs, run, facts, costs=facts["costs"],
            where=where, undo=undo,
            log=lambda m: console.print(f"[yellow]{_esc(m)}[/yellow]"),
            switch=decide.only_blocked, record=go)[1]
    except decide.Exhausted as e:
        if e.__cause__ is not None:
            # ways were tried and each said why
            raise
        capabilities.require(hop.engine, "stream")
        # the pipeline the last way there was, passed over for its terms:
        # the reason, not only that nothing is set up
        held = [why for r, why in e.reasons if r.name == "pipeline"
                and why.startswith(movers.PIPELINE_SAID + ":")]
        raise SystemExit(f"no way to follow a {hop.engine} source is set up"
                         " on this machine" + (f" - {held[0]}" if held else
                                               "; migkit doctor says what"
                                               " is missing"))


def _stream_facts(hop, eng, engine, has_repl, has_tail, drop=False,
                  risk=None, refused=None, by_pair=None):
    """(facts, rungs) of the ways this hop's changes may follow its source
    (`movers.stream_ways`, read as `movers.stream_way` reads them), the leg
    run from here held also where the servers' own replication could not
    start here: what a target that cannot dial the source falls to.
    `risk()` says why the servers' own replication would lose a table's
    changes (`PostgresEngine.publication_add_risk`), `refused()` why it
    would carry the hop's tables wrongly (`_own_refused`), and `by_pair()`
    whether migkit's own tail of the engine as a pair then follows it."""
    import dataclasses

    from . import decide, movers
    facts = movers.stream_facts(hop, engine, has_repl, has_tail,
                                has_tail and _tail_ready(eng), drop, risk,
                                refused, by_pair)
    rungs = []
    for r in movers.stream_ways():
        if r.name == "follow":
            r = dataclasses.replace(r, needs=(r.needs[0], decide.Need(
                "the target unable to dial the source",
                lambda f: movers.follow_selected() == "follow"
                or bool(f.get("own replication failed")),
                "the target can dial the source, so its own replication"
                " carries the changes")) + tuple(r.needs[2:]))
        rungs.append(r)
    return facts, rungs


def _cutover(hop, eng, db, go, engine):
    """`move --mode cdc --drop`: the cutover, as `migkit.cutover` runs it
    - and once one has flipped, its point of no return."""
    from . import approvals, cutover
    dbs = [db] if db else eng.databases()

    def approve(detail, step="cutover"):
        return approvals.require(hop, step, db,
                                 lambda m: console.print(f"  {m}"),
                                 detail=detail)
    leg = _stream_leg(hop, eng, db, dbs, engine)
    from . import rehearsal
    rehearsal.mark(hop)
    got = cutover.run(hop, eng, dbs, go, leg, console.print, approve)
    last = cutover.last_run(hop) if go else None
    if last and last.get("state") == "flipped" \
            and last.get("outage") is not None and dbs:
        # the outage held to the forecast the move started with
        for line in rehearsal.held_to(hop, dbs[0], "cutover",
                                      float(last["outage"])):
            console.print(f"  {line}")
    return got


def _stream_leg(hop, eng, db, dbs, engine):
    """What carries this hop's changes now, as the cutover drains it,
    tears it down and puts it back (`cutover.Leg`); `manual` is the
    teardown a hop without `cutover:` has always had.

    For a hop that turns `cutover:` on, the change leg run from here and
    migkit's streaming pipeline go through the cutover as well, each
    drained by its own positions (`_follow_leg`, `_pipeline_leg`); a hop
    without it finds its stream as it always did."""
    from . import cutover, hopkeys, movers, tailctl
    conf = cutover.settings(hop)
    back_at_cutover, _ = _topology(hop)
    has_repl = hasattr(eng, "replicate_sql")
    tailer = eng if hasattr(eng, "tail_apply") else _tail_pair(eng)
    once = getattr(eng, "REPLICATES_THE_SERVER", False)

    def say(m):
        console.print(f"  {m}")

    def manual(go):
        if go and not back_at_cutover:
            _protect_target(hop, eng, db, release=True)
        got = _cdc(hop, eng, db, True, go, engine)
        if go and hopkeys.origin_dropped(hop):
            _origin_away(hop, eng, dbs, say)
        return got

    def tail_end(d):
        """The tail ended for good, past the flip: it has nothing left to
        carry from a frozen source, and ends between batches."""
        where = hop.report_dir(d)
        if not tailctl.alive(where):
            return
        tailctl.stop(where)
        tailctl.resume(where)
        end = time.monotonic() + 60
        while tailctl.alive(where) and time.monotonic() < end:
            time.sleep(0.5)
        if tailctl.alive(where):
            raise SystemExit(f"{d}: the change tail did not end")
        say(f"{d}: the change tail ended")

    def ours(d):
        """Whether a stream of migkit's already writes into the target."""
        try:
            return any(own for _, own in eng.stream_writers(d))
        except Exception:  # noqa: BLE001 - asked again by setting it up
            return False

    def native_down(d):
        if once and d != dbs[0]:
            return []
        left = []
        _replicate_one(hop, eng, d, False, True, True, left=left)
        return left

    def native_up(d):
        if (once and d != dbs[0]) or ours(d):
            return
        _replicate_one(hop, eng, d, False, False, True)

    def tail_down(d):
        if not tailctl.pause(hop.report_dir(d), 60):
            raise SystemExit(f"{d}: the change tail did not pause")
        return []

    def tail_up(d):
        tailctl.resume(hop.report_dir(d))

    alive = [d for d in dbs if tailctl.alive(hop.report_dir(d))]
    follow = engine == "postgres" and movers.follow_selected() == "follow"
    # the leg run from here marks itself running where a tail would: its
    # own process is what tells it apart
    following = conf is not None and engine == "postgres" and any(
        movers.follow_lag(hop, d) is not None for d in dbs)
    if following:
        leg = _follow_leg(hop, eng, dbs)
    elif alive and tailer is not None:
        leg = cutover.Leg("tail", fence=tailer, down=tail_down, up=tail_up,
                          end=tail_end)
    elif conf is not None and _pipeline_runs(hop):
        leg = _pipeline_leg(hop, eng, dbs)
    elif has_repl and not follow and not eng.maps_columns() \
            and not _own_refused(hop, eng, db):
        leg = cutover.Leg("native", fence=eng, down=native_down,
                          up=native_up,
                          after_thaw=engine == "mysql" and _by_gtid(eng, dbs))
    elif follow:
        leg = cutover.Leg(
            "follow", why="the CDC leg driven from here (MIGKIT_CDC=follow)"
                          " runs to a position and ends: nothing streams to"
                          " tear down at a moment the cutover chooses")
    else:
        leg = cutover.Leg(
            "none", why="no stream of migkit's or the servers' own carries"
                        " this hop's changes now")
    if back_at_cutover and has_repl and leg.kind in ("native", "tail"):
        back, rev = _the_other_way(hop, eng)

        def back_up(d):
            if once and d != dbs[0]:
                return
            console.print("\n[bold]the other way[/bold] - from the new"
                          " primary back to the old source, as the hop says"
                          " (reverse: at_cutover)")
            _replicate_one(back, rev, hop.target_db(d), False, False, True)
            _changelog(hop, {"op": "reverse", "db": d, "drop": False})

        def back_down(d):
            if once and d != dbs[0]:
                return
            # made ahead, its slot is its own: taken away with it
            _replicate_one(back, rev, hop.target_db(d), False, True, True,
                           part="unarm")

        def back_arm(d):
            if once and d != dbs[0]:
                return
            console.print("\n[bold]the other way[/bold] - made ahead, idle"
                          " until the flip (reverse: at_cutover)")
            _replicate_one(back, rev, hop.target_db(d), False, False, True,
                           part="arm")

        def back_fire(d):
            if once and d != dbs[0]:
                return
            _replicate_one(back, rev, hop.target_db(d), False, False, True,
                           part="fire")
            _changelog(hop, {"op": "reverse", "db": d, "drop": False})

        def back_proof(d):
            if once and d != dbs[0]:
                return ""
            tdb = hop.target_db(d)
            return rev.armed_status(tdb, rev.replicate_sql(
                tdb, copy_data=False))
        leg.back_up, leg.back_down, leg.back_fence = back_up, back_down, rev
        if hasattr(rev, "armed_status"):
            leg.back_arm, leg.back_fire = back_arm, back_fire
            leg.back_proof = back_proof
    if hopkeys.origin_dropped(hop) and tailer is not None:
        leg.origin = lambda d: _origin_away(hop, eng, [d], say)
    leg.manual = manual
    return leg


def _by_gtid(eng, dbs):
    """Whether a MySQL source's replica goes on by GTID: put back after
    the source is writable again, it starts from the target's own
    executed set and misses nothing. Without GTIDs it starts from the
    source's position of that moment, so it has to be put back before
    the source takes a write - which a drain by a mark allows, since its
    freeze leaves migkit's own account writing (`freeze.OURS`). Not
    readable is taken as by GTID, the way it always was."""
    try:
        return eng.src_lsn(dbs[0]) is not None
    except Exception:  # noqa: BLE001 - as it always was
        return True


def _origin_away(hop, eng, dbs, say):
    """migkit's batch table taken off a one-way target where the hop asks
    (`origin_table: drop`), through the pair's own teardown of its marks
    - never while a tail of the hop runs, which reads its last batch back
    from it after a stop, and never on a hop streaming both ways, whose
    other way reads it too."""
    from . import tailctl, twoway
    if twoway.settings(hop) is not None or _topology(hop)[1]:
        say("the table of migkit's batches stays on the target: the hop"
            " streams both ways, and the other way reads it")
        return []
    tailer = eng if hasattr(eng, "two_way_teardown") else _tail_pair(eng)
    if tailer is None:
        return []
    out = []
    for d in dbs:
        if tailctl.alive(hop.report_dir(d)):
            say(f"{d}: the table of migkit's batches stays on the target: a"
                " tail of the hop still runs, and reads its last batch back"
                " from it after a stop")
            continue
        steps = tailer.two_way_teardown(d, _tail_token(hop, d), True)
        for s in steps:
            say(f"{d}: target: {s}")
        if steps:
            _changelog(hop, {"op": "origin-drop", "db": d})
        out += steps
    return out


def _follow_leg(hop, eng, dbs):
    """The change leg run from here (`movers.pgcopydb_follow`) running
    until asked (`follow_until: stopped`), as the cutover drains it: the
    slot it reads is its position - confirmed as it receives, which says
    how far behind it is - and after the freeze it is asked to end
    (`tailctl.stop`), naming its end at the source's position of that
    moment, and waited for until it has ended there (`settle`). Its slot
    stays until the flip is done, so an undone cutover leaves the leg to
    be started again from where it stopped; after the flip its slot,
    publication and origin are taken away (`after_flip`)."""
    from . import cutover, movers, tailctl

    class Fence:
        def src_lsn(self, d):
            return eng.src_lsn(d)

        def fence_wait(self, d, pos, timeout=300):
            return eng.fence_wait(d, pos, timeout)

    def settle(d, timeout):
        where = hop.report_dir(d)
        tailctl.stop(where)
        end = time.monotonic() + timeout
        while True:
            if movers.follow_lag(hop, d) is None \
                    and not tailctl.alive(where):
                return True
            if time.monotonic() >= end:
                return False
            time.sleep(0.5)

    def up(d):
        console.print(f"  {d}: the change leg run from here ended at the"
                      f" drain; `migkit move {hop.name} --mode cdc --go`"
                      " starts it again from its slot, which kept every"
                      " change since")

    def after_flip(d):
        for line in movers.follow_teardown(hop, d, True,
                                           lambda m: chat(f"  {m}")):
            console.print(f"  {d}: {line}")
        return []
    return cutover.Leg("follow", fence=Fence(), down=lambda d: [], up=up,
                       settle=settle, after_flip=after_flip)


def _pipeline_out(hop):
    return hop.report_dir() / "stream"


def _pipeline_runs(hop):
    """Whether migkit's streaming pipeline for the hop runs here: its
    stack written, and its source connector running."""
    from . import movers
    out = _pipeline_out(hop)
    if not (out / "docker-compose.yml").exists():
        return False
    return "connector=RUNNING" in movers.stream_status(
        f"migkit-{hop.name}-source")


def _rpk(out, *args, timeout=30):
    """The pipeline's broker asked through its compose file: it publishes
    no port but the connector runtime's (`movers.send_resnapshot`)."""
    from .util import run
    return run(["docker", "compose", "-f", str(out / "docker-compose.yml"),
                "exec", "-T", "redpanda", "rpk", *args], check=False,
               timeout=timeout)


def _pipeline_drained(text, topic):
    """Whether the sink's group, as the broker describes it, has applied
    every message it reads - every partition's committed offset at the
    log's end - and has taken in the mark's topic `topic`."""
    rows, head = [], None
    for line in (text or "").splitlines():
        words = line.split()
        if words and words[0] == "TOPIC" and "LAG" in words:
            head = words
            continue
        if head and len(words) >= len(head) - 3:
            rows.append(dict(zip(head, words)))
    if not rows or not any(r.get("TOPIC") == topic for r in rows):
        return False
    for r in rows:
        lag = r.get("LAG", "")
        if lag.isdigit():
            if int(lag):
                return False
        elif r.get("LOG-END-OFFSET") != r.get("LOG-START-OFFSET"):
            # nothing committed on a partition that holds messages
            return False
    return True


def _pipeline_leg(hop, eng, dbs):
    """migkit's streaming pipeline (`movers.stream_codegen`) as the
    cutover drains it. It keeps no position migkit can hold it to, so the
    drain is a mark (`drainmark`): found in its own topic once the
    connector has read the source past the freeze (`seen`), and then its
    sink's committed offsets at the end of every topic it reads, the
    mark's among them (`settle`) - the offsets the runtime commits at an
    interval of its own, which the drain waits for. Torn down only once
    the flip is done: before it, the connectors are paused and resumed
    (`down`, `up`), since a stack taken down reads the whole source again
    when it is started. Its slot on a PostgreSQL source is left for when
    the old source is written again (`after_flip`)."""
    from . import cutover, drainmark, freeze, movers
    out = _pipeline_out(hop)
    name = f"migkit-{hop.name}"
    kind = freeze.side_engine(hop, eng, "src")[0]
    done = []

    def topic(d):
        return f"{name}.{d if kind == 'mysql' else 'public'}.{drainmark.TABLE}"

    def seen(d, token):
        p = _rpk(out, "topic", "consume", topic(d), "--offset", ":end",
                 "--format", "%v\\n")
        return p.returncode == 0 and token in (p.stdout or "")

    def settle(d, timeout):
        end = time.monotonic() + timeout
        while True:
            p = _rpk(out, "group", "describe", f"connect-{name}-sink")
            if p.returncode == 0 and _pipeline_drained(p.stdout, topic(d)):
                return True
            if time.monotonic() >= end:
                return False
            time.sleep(0.5)

    def connectors(verb):
        for c in (f"{name}-source", f"{name}-sink"):
            movers._connect_api("PUT", f"/connectors/{c}/{verb}")

    def down(d):
        if d == dbs[0]:
            connectors("pause")
        return []

    def up(d):
        if d == dbs[0]:
            connectors("resume")

    def after_flip(d):
        if not done:
            movers.stream_down(out, lambda m: chat(f"  {m}"))
            _changelog(hop, {"op": "stream-down", "db": ",".join(dbs)})
            done.append(True)
        if kind != "postgres":
            return []
        slot = name.replace("-", "_")
        return [f"select pg_drop_replication_slot(slot_name) from"
                f" pg_replication_slots where slot_name = '{slot}' and not"
                " active"]
    return cutover.Leg("pipeline", seen=seen, settle=settle, down=down,
                       up=up, after_flip=after_flip)


def _hop_again(hop_name, hop, eng, db, dbs):
    """The hop read again for a watching loop's next cycle: (hop, engine,
    databases, whether its scope changed). Read once, a loop that ran for
    days verified and summed the scope it started with, and its engine
    kept the listings it read then. Where the scope is the same (its
    fingerprint, `Hop.scope_fingerprint`), all is kept as it was; where
    it changed, a new engine - nothing it cached carried over - and the
    databases listed again, said. A hop that cannot be read now is the
    one the loop has, said."""
    try:
        now = get_hop(hop_name)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - kept, said
        console.print(f"[yellow]the hop could not be read again"
                      f" ({_esc(_clean(e, raw=True))}); this cycle goes by"
                      " the one read before[/yellow]")
        return hop, eng, dbs, False

    def scope(h):
        fp = getattr(h, "scope_fingerprint", None)
        return fp() if callable(fp) else None
    if scope(now) == scope(hop):
        return hop, eng, dbs, False
    _require_configured(now)
    eng = get_engine(now)
    dbs = [db] if db else eng.databases()
    console.print("[yellow]the hop's scope changed since the last cycle:"
                  " what it takes is read afresh from here, and the rates"
                  " and quiet start again[/yellow]")
    return now, eng, dbs, True


def _delta_loop(hop_name, db, interval, cycles, teardown):
    """`watch --verify --delta`, and the verifying step of `sync --mode
    stream/migrate`: each cycle verifies what changed since the last
    verified point, keeps its answers (`delta-summary.json`), tells the
    hop's receivers, and says SAFE TO CUT OVER for a database only where
    that cycle verified it the same, the source made no change since the
    cycle before (`_position`) and the tail - where one runs here - has
    nothing left to apply. A database whose verify raises is an `error`
    answer of the cycle: it was left out, and the receivers were told
    "same". Returns the last cycle's `verdict.Decision`."""
    from . import tailctl
    from . import verdict as _verdict
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    dbs = [db] if db else eng.databases()
    if teardown:
        from .engines.base import Engine
        for d in dbs:
            if hasattr(eng, "delta_teardown"):
                try:
                    eng.delta_teardown(d)
                except Exception as e:
                    console.print(f"{_esc(d)}: [yellow]"
                                  f"{_esc(_clean(e, raw=True))}[/yellow]")
            for f in Engine.DELTA_FILES + (Engine.DELTA_SCOPE,):
                p = hop.report_dir(d) / f
                if p.exists():
                    p.unlink()
            console.print(f"{_esc(d)}: delta state removed")
        return None
    capabilities.require(hop.engine, "delta")
    secrets = (hop.source.password, hop.target.password)
    n, last, got = 0, {}, None
    while True:
        n += 1
        if n > 1:
            # the hop as it is now: a scope changed while this runs is
            # verified from here, the source's quiet measured afresh
            hop, eng, dbs, moved = _hop_again(hop_name, hop, eng, db, dbs)
            if moved:
                last = {}
                secrets = (hop.source.password, hop.target.password)
        stamp = time.strftime("%H:%M:%S")
        all_results = []
        for d in dbs:
            position = _position(eng, d)
            # the position held to the scope it was taken under
            verify = getattr(eng, "delta_in_scope", None) or eng.delta_verify
            try:
                rs = [r.__dict__ for r in verify(
                    d, log=lambda m, d=d: chat(
                        f"  {_esc('[' + str(d) + ']')} {_esc(m)}"))]
            except Exception as e:
                rs = [{"check": "delta", "scope": d, "status": "error",
                       "detail": _clean(e, secrets, raw=True)}]
            if not rs:
                rs = [{"check": "delta", "scope": d, "status": "error",
                       "detail": "the verify gave no answer"}]
            head = rs[0]
            st = str(head.get("status", ""))
            color = {"ok": "green", "diff": "yellow",
                     "error": "red"}.get(st, "white")
            console.print(f"{stamp} cycle {n} {_esc(d)}:"
                          f" [{color}]{_esc(st.upper())}[/{color}]"
                          f" {_esc(head.get('detail', ''))}")
            for r in rs[1:]:
                if r.get("status") not in ("ok", "skip"):
                    console.print(
                        f"    [yellow]{_esc(r.get('scope', ''))}:"
                        f" {_esc(r.get('detail', ''))}[/yellow]"
                        + (f"  fix: {_esc(r['fix_hint'])}"
                           if r.get("fix_hint") else ""))
            mine = _verdict.decide(rs, hop)
            st_tail = tailctl.state(hop.report_dir(d)) \
                if tailctl.alive(hop.report_dir(d)) else None
            behind = (st_tail or {}).get("behind")
            quiet = _held_since(last.get(d), position)
            if mine.status == "same" and quiet and not behind:
                console.print(
                    "    [green]SAFE TO CUT OVER[/green]: verified the same"
                    " this cycle, the source made no change since the last"
                    + (", and the tail has nothing left to apply"
                       if st_tail is not None else ""))
            elif mine.status == "same":
                console.print(
                    "    same as far as verified; "
                    + ("the tail has changes still to apply" if behind
                       else "the source is still being written"
                       if position is not None and last.get(d)
                       else "whether the source is still being written is"
                            " known from the next cycle"))
            last[d] = position
            all_results.extend(rs)
        (hop.report_dir() / "delta-summary.json").write_text(
            json.dumps(all_results, indent=1, default=str))
        _watched(hop, dbs, all_results)
        got = _verdict.decide(all_results, hop)
        if cycles and n >= cycles:
            break
        time.sleep(interval)
    return got


def _watched(hop, dbs, results=None):
    """What a watching loop tells the hop's receivers each cycle: a
    difference the continuous check finds, or its going (`drift`), and a
    change tail behind by more than the hop allows (`lag`)."""
    from . import notify
    from . import verdict as _verdict

    def say(m):
        console.print(f"[yellow]{_esc(m)}[/yellow]")
    if results is not None:
        notify.verdict(hop, _verdict.summarize(hop, results), say,
                       key="watch")
    notify.lags(hop, dbs, say)


#: what `watch --verify` keeps of its last cycle, beside the check's own
WATCH_SUMMARY = "watch-summary.json"
WATCH_REPORT = "watch-report.html"


def _monitor(hop_name, db, interval, only, cycles):
    """`watch --verify`: the check's own run (`run_checks`) each cycle -
    its answers kept under names of its own, where it wrote a partial
    cycle over the check's `report.html` while `verdict.json` described
    the last whole check. Returns the last cycle's `verdict.Decision`."""
    from . import verdict as _verdict
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    checks, _ = _checks_for(eng, only)
    dbs = [db] if db else eng.databases()
    n, got = 0, None
    while True:
        n += 1
        if n > 1:
            # each cycle checks what the hop takes now (`_hop_again`)
            hop, eng, dbs, moved = _hop_again(hop_name, hop, eng, db, dbs)
            if moved:
                checks, _ = _checks_for(eng, only)
        stamp = time.strftime("%H:%M:%S")
        all_results, _ = run_checks(hop, eng, dbs, checks,
                                    workers=hop.workers, whole=False)
        for d in dbs:
            statuses = []
            for c in checks:
                rs = [r for r in all_results if r.get("check") == c
                      and _verdict.db_of(r.get("scope", "")) == d]
                bad = [r for r in rs if r.get("status") == "error"]
                odd = [r for r in rs if r.get("status") not in
                       ("ok", "skip", "error") and not r.get("kept_by_hop")]
                if bad:
                    statuses.append(f"[red]{c}:ERR[/red]"
                                    f" {_esc(str(bad[0].get('detail'))[:60])}")
                elif odd:
                    statuses.append(f"[yellow]{c}:DIFF[/yellow]"
                                    f" {_esc(str(odd[0].get('detail'))[:60])}")
                elif rs:
                    statuses.append(f"[green]{c}:ok[/green]")
            console.print(f"{stamp} cycle {n} {_esc(d)}: "
                          + "  ".join(statuses))
        got = _verdict.decide(all_results, hop)
        try:
            (hop.report_dir() / WATCH_SUMMARY).write_text(
                json.dumps(all_results, indent=1, default=str))
            from .report import write_report
            write_report(hop, all_results,
                         hop.report_dir() / WATCH_REPORT, decided=got)
        except OSError:
            pass
        _watched(hop, dbs, all_results)
        if cycles and n >= cycles:
            break
        time.sleep(interval)
    return got


def _cutover_verdict(prev, cur, stable_cycles):
    """What two consecutive count samples say about a cutover - never that
    it is safe. The counts are the servers' estimates (MySQL's cached up
    to a day), a target with extra rows passes `dst >= src`, and an update
    or a delete moves no count: "SAFE TO CUT OVER" said from them was a
    guess. SAFE is said only by a cycle that verified what changed
    (`_delta_loop`). The trap named here is a source that looks caught up
    while a queue or a worker keeps writing to it: its hot tables."""
    src, dst = cur.get("src_rows", 0), cur.get("dst_rows", 0)
    pt = (prev or {}).get("src_tables") or {}
    hot = [f"{t} +{c - pt[t]}" for t, c in (cur.get("src_tables") or {}).items()
           if t in pt and c > pt[t]]
    if hot:
        return ("NOT SAFE", "yellow",
                "source still HOT (writers active): " + ", ".join(hot[:4])
                + " - pause them to converge")
    if src > 0 and dst > src:
        return ("counts differ (estimates)", "yellow",
                "the target counts more rows than the source - migkit"
                " check says which are extra")
    if dst == src and src > 0 and stable_cycles >= 2:
        return ("counts converging (estimates)", "cyan",
                f"source quiet {stable_cycles} cycles and the counts agree;"
                " `watch --verify --delta` verifies what changed and says"
                " when a flip is safe")
    if dst >= src and src > 0:
        return ("converging", "cyan",
                "dst caught up; waiting for source to settle before verdict")
    return None


def _move_said(hop, d):
    """What a move running now says of itself (`progress.json`: the recent
    rate, the time left of the copy, how many at a time and what holds them
    down) and what its plan said of the phases after the copy
    (`model.json`), for `watch`."""
    from . import model as resource_model
    from . import planner
    where = hop.report_dir(d)
    out = []
    p = planner.read_progress(where)
    if p and time.time() - float(p.get("at") or 0) < 600:
        out.append(
            f"move: {p.get('workers')} at a time"
            + (f", ~{human_secs(p['eta_copy'])} of the copy left at the"
               f" recent {human_int(int(p['rate']))} rows/s"
               if p.get("eta_copy") is not None and p.get("rate") else "")
            + (f", {p['binding']} binding" if p.get("binding") else "")
            + (f"; held down by {p['held_by']}" if p.get("held_by")
               else ""))
    m = resource_model.load(where)
    for ph in (m.planned if m is not None else ()):
        if ph.name != "copy":
            out.append(f"then {ph.line()}")
    return out


def _catch_up(hop, eng, d):
    """The catch-up's sum for `watch` while a tail runs here: the source's
    own count of changes against the tail's heartbeat (`planner.CatchUp`)."""
    from . import planner, tailctl
    where = hop.report_dir(d)

    def tail():
        st = tailctl.state(where)
        return st if st and st.get("beat_age") is not None else None

    def arrived():
        if tail() is None:
            return None
        got = eng.changes_made("src", d)
        return got[0] if got else None

    def applied():
        st = tail()
        return st["changes"] if st else None

    def held():
        st = tail()
        return ((st.get("room") or {}).get("held_bytes") if st else None)

    def behind():
        st = tail()
        return st["behind"] if st else None
    return planner.CatchUp(arrived, applied, held, behind)


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--interval", default=0,
              help="seconds between samples (default 30, or 300 with --verify)")
@click.option("--count", default=0, help="samples/cycles then exit, 0 = forever")
@click.option("--verify", is_flag=True,
              help="re-run read-only checks each cycle instead of sampling"
                   " row counts (continuous validation)")
@click.option("--only", default="counts,autoinc",
              help="with --verify: checks per cycle, add data for full"
                   " checksum each cycle")
@click.option("--delta", is_flag=True,
              help="with --verify: O(changes) mode - each cycle re-verifies"
                   " only the rows touched since the last verified point"
                   " (WAL slot / binlog / change stream driven)")
@click.option("--teardown", is_flag=True,
              help="drop the delta slot/position --delta keeps, and exit")
def watch(hop_name, db, interval, count, verify, only, delta, teardown):
    """Watch a running migration load: row counts, rate, ETA, replication
    state. --verify turns it into a continuous validation loop: transient
    diffs that heal next cycle are replication lag, persistent ones are
    real. --verify --delta verifies only what changed, so it can run
    forever against billion-row databases, and says when a flip is safe.
    With --count it ends as its last cycle's verdict does."""
    from .verdict import Usage
    if _given("only") and not verify:
        raise Usage("--only names the checks --verify runs each cycle")
    if teardown and verify and not delta:
        raise Usage("--teardown drops what --delta keeps; --verify alone"
                    " keeps nothing to drop")
    if delta or teardown:
        # --teardown alone started an endless count watch, and the delta
        # slot it was asked to drop kept the source's log growing
        got = _delta_loop(hop_name, db, interval or 60, count, teardown)
    elif verify:
        got = _monitor(hop_name, db, interval or 300, only, count)
    else:
        return _sample_watch(hop_name, db, interval, count)
    if got is not None and got.exit:
        raise SystemExit(got.exit)


def _sample_watch(hop_name, db, interval, count):
    """`watch` without --verify: the counts sampled, their rate and what
    they say about a cutover (`_cutover_verdict`)."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    interval = interval or 30
    dbs = [db] if db else eng.databases()
    last = {}
    stable = {}
    from . import planner
    # the time left from the recent rate, not the last sample's alone: one
    # sample's rows swing with the ranges that happened to commit in it
    recent = {d: planner.Ewma() for d in dbs}
    ahead = {d: _catch_up(hop, eng, d) for d in dbs}
    n = 0
    # the hop's streaming pipeline, where it runs one
    piped = hop.report_dir() / "stream"
    piped = piped if (piped / "docker-compose.yml").exists() else None
    notes, held = 0, False
    while True:
        n += 1
        if n > 1:
            # each sample sums what the hop takes now (`_hop_again`); a
            # scope changed since is a new count, its rate and its quiet
            # measured afresh
            hop, eng, dbs, moved = _hop_again(hop_name, hop, eng, db, dbs)
            if moved:
                last, stable = {}, {}
                recent = {d: planner.Ewma() for d in dbs}
                ahead = {d: _catch_up(hop, eng, d) for d in dbs}
        for d in dbs:
            s = eng.watch_sample(d)
            if "error" in s:
                console.print(f"{_esc(d)}: [red]{_esc(s['error'])}[/red]")
                continue
            src, dst = s.get("src_rows", 0), s.get("dst_rows", 0)
            line = f"{_esc(d)}: src~{human_int(src)} dst~{human_int(dst)}"
            # a table a row filter narrows is counted whole on the source:
            # the target never reaches it, and no share or time left is
            # said from it
            narrowed = s.get("narrowed") or []
            if src and not narrowed:
                line += f" ({dst * 100 // src}%)"
            prev = last.get(d)
            if prev and s["ts"] > prev["ts"]:
                rate = (dst - prev["dst_rows"]) / (s["ts"] - prev["ts"])
                if rate > 0:
                    avg = recent[d].add(rate)
                    line += (f" rate {human_int(int(rate))}/s"
                             f" (recent {human_int(int(avg))}/s)"
                             + ("" if narrowed else
                                f" eta {human_secs(max(src - dst, 0) / avg)}"))
            if narrowed:
                line += (f"\n    the source is counted whole in"
                         f" {len(narrowed)} tables a row filter narrows"
                         f" ({_esc(', '.join(narrowed[:3]))}"
                         + (" ..." if len(narrowed) > 3 else "")
                         + "), so it is more than the target will hold")
            for more in _move_said(hop, d):
                line += f"\n    {_esc(more)}"
            # the machines that move it, where they work together (`ha`)
            from . import ha
            for more in ha.health(hop, d):
                line += f"\n    {_esc(more)}"
            said = ahead[d].line()
            if said:
                line += f"\n    {_esc(said)}"
            # source hot = it grew (aggregate or any table) since last cycle
            grew = bool(prev) and (
                src > prev.get("src_rows", 0)
                or any(c > (prev.get("src_tables") or {}).get(t, 0)
                       for t, c in (s.get("src_tables") or {}).items()
                       if t in (prev.get("src_tables") or {})))
            stable[d] = 0 if grew else stable.get(d, 0) + 1
            for slot in s.get("replication_slots", []):
                line += f"\n    slot {_esc(slot)}"
            for conn in s.get("replication_conns", []):
                line += f"\n    conn {_esc(conn)}"
            v = _cutover_verdict(prev, s, stable[d])
            if v:
                label, style, detail = v
                line += (f"\n    [{style}]{_esc(label)}[/{style}]:"
                         f" {_esc(detail)}")
            console.print(line)
            last[d] = s
        if piped:
            said, notes, held = _watch_stream(hop, eng, dbs, piped, notes,
                                              held)
            for one in said:
                console.print(f"  {_esc(one)}")
        _watched(hop, dbs)
        if count and n >= count:
            break
        time.sleep(interval)


def _watch_stream(hop, eng, dbs, out, seen, held):
    """One look at the hop's streaming pipeline for `watch`: its reads as
    the connector reported them since the last look, and a re-read beside
    the stream held while the source is under strain (`stream_hold`).
    ([line], notifications read, held)."""
    from . import movers, sizing
    lines, seen = movers.stream_progress(out, hop.name, seen)
    try:
        held = movers.stream_hold(out, hop.name,
                                  sizing.strain(eng, dbs[0]) if dbs else "",
                                  held, lines.append)
    except RuntimeError:
        # what the broker's client said names it; this does not
        lines.append("the re-read could not be held or let go: the"
                     " pipeline's signal topic could not be reached")
    return lines, seen, held


@main.command()
@click.argument("hop_name", required=False)
@click.option("--open", "do_open", is_flag=True, help="open in browser")
@click.option("--refresh", is_flag=True,
              help="render the report again from the last check's answers"
                   " on disk (no re-check); the answers stay as the check"
                   " took them")
@click.option("--serve", is_flag=True,
              help="serve the web view and the API for every hop - signed"
                   " in, with roles - instead of writing a file")
@click.option("--metrics", is_flag=True,
              help="print Prometheus metrics for every hop and exit"
                   " (the dashboard also exposes them at /metrics)")
@click.option("--port", default=8899, help="with --serve: listen port")
def report(hop_name, do_open, refresh, serve, metrics, port):
    """Render the HTML report from the last check run and export the
    hop's evidence (what moved, every verdict with both sides' counts and
    digests and how to take them again, decisions, approvals, repairs,
    outages - HTML, Markdown, JSON, CSV), or --serve the web view: every
    hop's outcomes, progress, approvals, offers and evidence for the
    people each hop names, the REST API, and a Prometheus /metrics
    endpoint (`migkit.web`)."""
    from .verdict import Usage
    if (serve or metrics) and (refresh or do_open or hop_name):
        raise Usage("--serve and --metrics are for every hop; --refresh,"
                    " --open and a hop name are for one hop's report")
    if _given("port") and not serve:
        raise Usage("--port is where --serve listens")
    if metrics:
        from .ui import prometheus
        print(prometheus(load_hops()), end="")
        return
    if serve:
        from .web import serve as _serve
        return _serve(port)
    if not hop_name:
        from .config import REPORTS
        have = [n for n in load_hops()
                if (REPORTS / n / "summary.json").exists()]
        if len(have) == 1:
            hop_name = have[0]
        else:
            raise SystemExit("which hop? reports exist for: "
                             + (", ".join(have) or "none"))
    hop = get_hop(hop_name)
    summary_path = hop.report_dir() / "summary.json"
    if refresh and summary_path.exists():
        # the answers are the check's, and nothing here changes them: it
        # rewrote a `diff` whose detail said no row differed as an `ok`,
        # in the file the dashboard and the next repair read
        console.print("rendered again from the last check's answers, as it"
                      " took them - `migkit check` takes them afresh")
    from . import export
    from .report import from_summary
    path = None
    if summary_path.exists():
        path, generated = from_summary(hop)
        console.print(f"report ({generated}): {path}")
    else:
        console.print("no check yet, so no check report: `migkit check"
                      f" {hop.name}` writes one")
    out = export.write(hop, log=lambda m: console.print(
        f"[yellow]{m}[/yellow]"))
    console.print(f"evidence: {out / 'evidence.html'} (and evidence.md,"
                  " evidence.json, tables.csv beside it)")
    if do_open:
        # the platform's own opener, not one system's command
        click.launch(str(path or out / "evidence.html"))


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--show", "show_ts", default="", help="print one state in detail")
def history(hop_name, db, show_ts):
    """Audit trail: saved rollback states and every write migkit made, read
    from the local changelog and state journals. migkit writes no bookkeeping
    into the destination, so the audit is local-only."""
    from .state import get_store
    hop = get_hop(hop_name)
    root = hop.report_dir()
    store = get_store(hop)
    # from what migkit kept, never the source: it connected to list the
    # databases, and a source that was down stopped the audit trail
    dbs = [db] if db else _dbs_with_history(hop)
    t = Table("state", "db", "captured", "op", title=f"saved states"
              f" ({store.kind})")
    found = []
    for d in dbs:
        try:
            points = store.list(d)
        except Exception:
            points = []
        for m in points:
            found.append((d, m))
            t.add_row(m.get("ts", "?"), d, str(m.get("created", "")),
                      m.get("op", ""))
    if found:
        console.print(t)
    if show_ts:
        for d, m in found:
            if show_ts in m.get("ts", ""):
                sdir = store.fetch(d, m["ts"])
                jl = sdir / "journal.jsonl" if sdir else None
                if jl and jl.exists():
                    console.print(f"\n[bold]{_esc(d)}/{_esc(m['ts'])}"
                                  "[/bold]")
                    say_raw(jl.read_text().strip())
    cl = root / "changelog.jsonl"
    if cl.exists():
        from . import audit
        n, chained, broken = audit.verify(cl)
        console.print("\nlocal changelog (last 15):")
        for line in cl.read_text().splitlines()[-15:]:
            say_raw(f"  {line}")
        if broken:
            console.print(f"[red]the record has been changed:"
                          f" {_esc(broken)}[/red]")
        else:
            console.print(f"  {chained} of {n} entries chained, and the"
                          " chain is whole"
                          + (f" ({n - chained} from before the chain began)"
                             if n > chained else ""))
    elif not found:
        console.print("no history yet, nothing has been applied")


def _dbs_with_history(hop):
    """The databases migkit kept anything for - a restore point, a
    repair's undo - by the hop's reports, and the hop's own list."""
    got = set(map(str, hop.databases or []))
    try:
        for d in hop.report_dir().iterdir():
            if d.is_dir() and ((d / "state").is_dir()
                               or (d / "undo").is_dir()):
                got.add(d.name)
    except OSError:
        pass
    return sorted(got)


@main.command()
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--state", "state_ts", default="",
              help="state timestamp or tag, default latest")
@click.option("--apply", "do_apply", is_flag=True)
def rollback(hop_name, db, state_ts, do_apply):
    """Restore target sequences from a saved state, put back the rows the
    repairs since it wrote, where the target still holds what they wrote;
    where the hop streamed back or cut over, the old source's counters
    raised past the new primary's. With --apply, under one hold of the
    hop, the application's writes given back last - after everything they
    would write over is put back."""
    hop = get_hop(hop_name)
    eng = get_engine(hop)
    from . import cutover, freeze, repairlog
    _guard_write(hop, eng, "a rollback", [db], do_apply, step="rollback")
    # read before the target's own counters are put back: the new
    # primary's as they stand clear every key it handed out, even one
    # whose row it deleted since
    back = cutover.back_plan(hop, eng, db, console.print)
    # the row repairs since the state gone back to, or the last run's where
    # none was saved, each by its statement-level record (`repairlog`)
    from .state import get_store
    states = sorted(m.get("ts", "") for m in get_store(hop).list(db))
    chosen = [x for x in states if state_ts in x] if state_ts else states
    rows_back = repairlog.to_reverse(hop.report_dir(db) / "undo",
                                     chosen[-1] if chosen else None)
    lk = _lock(hop) if do_apply else None
    try:
        _restore_target(hop, eng, db, state_ts, do_apply,
                        back is None and not rows_back, locked=True)
        if rows_back:
            console.print("\nrow repairs to put back, newest first:")
            for path in rows_back:
                console.print(f"  {_esc(repairlog.said(path))}")
            if do_apply:
                for path in rows_back:
                    repairlog.reverse(
                        path, eng.repair_io(db, repairlog.read(path)[0]),
                        log=lambda m: console.print(f"  {_esc(m)}"))
        if back is not None:
            cutover.back_apply(hop, eng, db, back, do_apply, console.print)
        if freeze.state(hop, db):
            # what migkit took from the application's roles comes back
            # last: given back first, they wrote while the rest was put
            # back under them
            if do_apply:
                freeze.thaw(hop, eng, db,
                            lambda m: console.print(f"  {_esc(m)}"))
            else:
                console.print("  the application's roles are still kept"
                              " from writing to the target; --apply gives"
                              " their writes back, after the rest")
    finally:
        if lk is not None:
            lk.unlink()
    manifest = None
    if hasattr(eng, "_report"):
        manifest = eng._report(db) / "undo" / "manifest.txt"
    if manifest and manifest.exists():
        console.print("\nrow-level undo (run per manifest line):")
        say_raw(manifest.read_text().strip())
    if not do_apply:
        console.print("\ndry-run, add --apply to put back what is listed"
                      " above")


def _restore_target(hop, eng, db, state_ts, do_apply, required,
                    locked=False):
    """The target's counters put back to a saved state. Without one, an
    error where that was all there was to do; said, where going back also
    raises the old source's. `locked`: the caller holds the hop."""
    from .state import get_store
    store = get_store(hop)
    states = sorted(m.get("ts", "") for m in store.list(db))
    if not states and not required:
        console.print("  no saved state of the target's counters: they are"
                      " left as they are")
        return
    if not states:
        raise SystemExit("no saved states, run migkit sync --go first")
    matches = [x for x in states if state_ts in x] if state_ts else states
    if not matches:
        raise SystemExit(f"no state matching '{state_ts}',"
                         f" have: {', '.join(states)}")
    ts = matches[-1]
    state = store.fetch(db, ts)  # pulls from s3/tar mirror if not local
    if state is None:
        raise SystemExit(f"state {ts} not retrievable from {store.kind}")
    stmts = []
    seqf = state / "dst-sequences.txt"
    aif = state / "dst-autoinc.txt"
    if hasattr(eng, "restore_sequences"):
        stmts = eng.restore_sequences(db, state)
    elif seqf.exists():
        for line in seqf.read_text().splitlines():
            if line:
                name, val, called = line.rsplit("|", 2)
                stmts.append(f"select setval('{name}', {val}, {called});")
    elif aif.exists():
        for line in aif.read_text().splitlines():
            if line:
                t, v = line.rsplit("|", 1)
                stmts.append(f"alter table `{db}`.`{t}` auto_increment = {v};")
    changed, same = [], 0
    current = {}
    if seqf.exists() and hasattr(eng, "_psql"):
        out = eng._psql("dst", db,
                        "select schemaname||'.'||sequencename||'|'||"
                        "coalesce(last_value,1)||'|'||(last_value is not null)"
                        " from pg_sequences"
                        " where schemaname not like '\\_\\_%'")
        current = {l.rsplit("|", 2)[0]: l for l in out.splitlines() if l}
        for line in seqf.read_text().splitlines():
            if not line:
                continue
            name = line.rsplit("|", 2)[0]
            if current.get(name) == line:
                same += 1
            else:
                changed.append(name)
    console.print(f"state {ts}: {len(stmts)} values in snapshot,"
                  f" {len(changed)} differ from current target,"
                  f" {same} already match")
    for s in stmts[:10]:
        say_raw(f"  {s}")
    if do_apply and stmts:
        lk = None if locked else _lock(hop)
        try:
            if seqf.exists():
                eng._psql("dst", db, "\n".join(stmts))
            else:
                from .engines.base import RepairAction
                eng.apply(db, RepairAction(db, "sequences", stmts, [],
                                           "rollback restore"))
        finally:
            if lk is not None:
                lk.unlink()
        _changelog(hop, {"op": "rollback", "db": db, "state": ts,
                         "n": len(stmts)})
        console.print("[green]values restored to snapshot[/green]")


# --- legacy command names, kept as hidden aliases ---

@main.command(hidden=True)
def hops():
    """(alias) List configured hops - now part of migkit doctor."""
    _hops_table()


@main.command("setup-target", hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
def setup_target(hop_name, db):
    """(alias) Now: migkit schema HOP."""
    _setup_target(hop_name, db)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--kind", default="all",
              type=click.Choice(["sequences", "rows", "schema", "all"]))
@click.option("--apply", "do_apply", is_flag=True)
def repair(hop_name, db, kind, do_apply):
    """(alias) Now: migkit sync HOP [--apply]."""
    return _repair(hop_name, db, kind, do_apply)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--no-copy-data", is_flag=True)
@click.option("--drop", "do_drop", is_flag=True)
@click.option("--go", is_flag=True)
def replicate(hop_name, db, no_copy_data, do_drop, go):
    """(alias) Now: migkit move HOP --mode cdc / full+cdc.

    The move itself, with every guard it holds: this alias set a stream
    up on its own - into a hop that masks, a `goal: verify` hop, and torn
    down with no approval and outside the cutover."""
    mode = "cdc" if (no_copy_data or do_drop) else "full+cdc"
    return _move_run(hop_name, db, "", mode, 500000, do_drop, go)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--go", is_flag=True)
def tail(hop_name, db, go):
    """(alias) Now: migkit move HOP --mode cdc --db X - the move itself,
    with every guard it holds."""
    return _move_run(hop_name, db, "", "cdc", 500000, False, go)


@main.command("convert-schema", hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--apply", "do_apply", is_flag=True)
def convert_schema(hop_name, db, do_apply):
    """(alias) Now: migkit schema HOP --convert."""
    _convert_schema(hop_name, db, do_apply)


@main.command("gen-migration", hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--out", default="migrations")
def gen_migration(hop_name, db, out):
    """(alias) Now: migkit schema HOP --migration."""
    _gen_migration(hop_name, db, out)


@main.command("sample-diff", hidden=True)
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--table", required=True)
@click.option("--limit", default=1000)
def sample_diff(hop_name, db, table, limit):
    """(alias) Now: migkit check HOP --drill --db X --table Y."""
    _drill(hop_name, db, table, limit)


@main.command(hidden=True)
@click.option("--port", default=8899)
def ui(port):
    """(alias) Now: migkit report --serve."""
    from .ui import serve as _serve
    _serve(port)


@main.command("state", hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--show", "show_ts", default="")
@click.pass_context
def state_cmd(ctx, hop_name, db, show_ts):
    """(alias) Now: migkit history HOP."""
    ctx.invoke(history, hop_name=hop_name, db=db, show_ts=show_ts)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--interval", default=300)
@click.option("--only", default="counts,autoinc")
@click.option("--cycles", default=0)
def monitor(hop_name, db, interval, only, cycles):
    """(alias) Now: migkit watch HOP --verify."""
    _monitor(hop_name, db, interval, only, cycles)


@main.command("users")
@click.argument("hop_name")
@click.argument("mode", type=click.Choice(["test", "create", "verify", "setpw",
                                          "rollback"]))
@click.option("--apply", "do_apply", is_flag=True,
              help="execute on the target (default is dry-run)")
@click.option("--passwords", "pw_file", default="",
              help="yaml file `role: password` for postgres create and for "
                   "mongo setpw (managed sources lock password hashes; mysql "
                   "needs no file - the hash is copied so the password stays "
                   "identical)")
def users_cmd(hop_name, mode, do_apply, pw_file):
    """Sync logins source -> target keeping the same password.

    test    compare users (read-only), write users.json to the report dir
    setpw   mongo only: set the real passwords on users that already exist on
            the target, from --passwords (mongo credentials cannot be copied:
            SCRAM is one-way and DocumentDB blocks writes to system.users)
    create  make the missing users on the target (dry-run unless --apply);
            mysql copies the password hash = same password, grants replayed
            from the source; postgres reads passwords from --passwords
    verify  re-compare; exit 0 only when no gap remains
    rollback drop only the users this command created (dry-run unless --apply)

    Cloud system accounts (AWS_*, rds*, tencent*) are excluded on both sides.
    Never touches the source.
    """
    from .users import run as _users_run
    if mode in ("create", "setpw", "rollback"):
        hop = get_hop(hop_name)
        _require_configured(hop)
        _guard_write(hop, get_engine(hop), "carrying users", [""],
                     do_apply)
    _users_run(hop_name, mode, do_apply, pw_file)


if __name__ == "__main__":
    main()
