"""One run of migkit: the command an operator started on a hop, and what
is kept of it (backlog R23.1, R23.8, R23.10).

Every command is a run with an id of its own (`current`). What a run
leaves is stamped with that id - the entries of the hop's record
(`audit`), the ways the engine chose and why (`Decisions`), the digests a
check took (`digests`), the verdict it wrote - so the evidence of one run
can be told apart from the hop's whole history (`export`).

Around every command (`step`, which `cli` wraps each command in):
* the run's trace is opened, and closed at the end (`traces`)
* the run's log is begun and ended, beside the hop's reports (`runlog`)
* each way the engine chooses is kept with its reason, and a way given up
  for another as the run goes is said to the hop's receivers in one line
* a run that stops on an error is said to them, and a run that wrote
  (`--go`, `--apply`) is said done (`notify`)
* the run's evidence is exported beside the hop's reports, where the run
  left anything to export

Nothing here decides anything or changes what a command does: a
receiver that cannot be reached, a trace that cannot be sent, an export
that cannot be written is said in one line and the command's own answer
stands.
"""
import contextlib
import json
import secrets
import threading
import time

_ID = None
_LOCK = threading.Lock()

#: the file each run's choices are kept in, beside the hop's reports
DECISIONS = "decisions.jsonl"


def _new():
    return time.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(3)


def current():
    """The id of the run in progress; one is drawn for a process that
    started none (a library caller, a test)."""
    global _ID
    with _LOCK:
        if _ID is None:
            _ID = _new()
        return _ID


def begin():
    """A new run, and its id."""
    global _ID
    with _LOCK:
        _ID = _new()
        return _ID


def _said(rung):
    return getattr(rung, "said", None) or "no way here"


def choice_record(got):
    """A `decide.Choice` or `decide.Strategy` as the run keeps it: the
    work, the way taken, why, and every way passed over with why - each
    in the words an operator reads (a rung's `said`), never the label the
    code uses for it, which can be a program's name."""
    parts = getattr(got, "parts", None)
    if parts is not None:
        return {"work": str(got.work),
                "way": ", ".join(f"{p}: {_said(r)}" for p, r in
                                 parts.items()) or "no way here",
                "reason": "; ".join(f"{p}: {got.reasons.get(p)}"
                                    for p in parts
                                    if got.reasons.get(p))
                or got.why_not or "",
                "passed": []}
    return {"work": str(got.work),
            "way": _said(got.rung) if got.rung is not None
            else "no way here",
            "reason": str(got.reason or ""),
            "passed": [{"way": _said(r), "why": str(why)}
                       for r, why in (got.passed or ())]}


class Decisions:
    """What the engine chose in this run, kept as it is chosen
    (`decide.LISTENERS`): each distinct (work, way, reason) once, a line
    in the hop's `decisions.jsonl`. A work that stood on one way and now
    stands on another it passed over has fallen back, which the hop's
    receivers are told in one line."""

    def __init__(self, hop, log=None):
        self.hop, self.log = hop, log or (lambda m: None)
        self.path = hop.report_dir() / DECISIONS
        self.seen, self.chosen = set(), {}
        self.lock = threading.Lock()

    def heard(self, got):
        rec = choice_record(got)
        key = (rec["work"], rec["way"], rec["reason"])
        switched = None
        with self.lock:
            before = self.chosen.get(rec["work"])
            self.chosen[rec["work"]] = rec["way"]
            gave_up = {p["way"]: p["why"] for p in rec["passed"]}
            if before and before != rec["way"] and before in gave_up:
                switched = (before, gave_up[before])
            fresh = key not in self.seen
            self.seen.add(key)
            if fresh:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.path, "a") as f:
                    f.write(json.dumps({"session": current(),
                                        "at": time.strftime("%F %T"),
                                        **rec}, default=str) + "\n")
        from . import traces
        traces.event("decision", work=rec["work"], way=rec["way"],
                     reason=rec["reason"][:500])
        if switched:
            from . import notify
            notify.fallback(self.hop, rec["work"], switched[0],
                            switched[1], rec["way"], self.log)


def _hop(name):
    if not name:
        return None
    from .config import get_hop
    try:
        return get_hop(name)
    except (SystemExit, Exception):  # noqa: BLE001 - the command says it
        return None


def _first_line(text):
    text = str(text or "").strip()
    return text.splitlines()[0][:300] if text else ""


@contextlib.contextmanager
def step(command, params, log=None):
    """One command as one run: see the module's docstring. `params` are
    the command's own (`hop_name`, `go`, `do_apply` read from them)."""
    from . import decide, notify, runlog, traces
    log = log or (lambda m: None)
    run = begin()
    hop = _hop(params.get("hop_name"))
    kept = None
    if hop is not None:
        try:
            kept = runlog.begin(hop, run, command)
        except Exception as e:  # noqa: BLE001 - said; the run goes on
            log(f"the run's log could not be begun ({type(e).__name__})")
    writes = bool(params.get("go") or params.get("do_apply"))
    listening = Decisions(hop, log) if hop is not None else None
    if listening is not None:
        decide.LISTENERS.append(listening.heard)
    began = time.time()
    failed, ok, ended = None, False, 0
    try:
        with traces.command(command, hop, params, run):
            try:
                yield run
                ok = True
            except SystemExit as e:
                # a number is a verdict's exit (a difference found); words
                # are migkit stopping, which is a failure to be told
                if isinstance(e.code, str) and e.code.strip():
                    failed = _first_line(e.code)
                else:
                    ended = e.code
                raise
            except KeyboardInterrupt:
                failed = "interrupted"
                raise
            except Exception as e:
                failed = f"{type(e).__name__}: {_first_line(e)}"
                raise
    finally:
        if listening is not None:
            with contextlib.suppress(ValueError):
                decide.LISTENERS.remove(listening.heard)
        if hop is not None:
            try:
                if failed not in (None, "interrupted"):
                    notify.failed(hop, command, failed, log)
                elif writes and ok:
                    notify.done(hop, command, time.time() - began, log)
            except Exception as e:  # noqa: BLE001 - the answer stands
                log(f"the hop's receivers could not be told"
                    f" ({type(e).__name__})")
            _export(hop, run, log)
        runlog.end(kept, "interrupted" if failed == "interrupted" else
                   "stopped" if failed else "done", time.time() - began,
                   failed, ended)
        traces.flush()


def _export(hop, run, log):
    from . import export
    try:
        export.write_run(hop, run)
    except Exception as e:  # noqa: BLE001 - said; the run's answer stands
        log(f"the run's evidence could not be exported"
            f" ({type(e).__name__}: {_first_line(e)[:120]})")
