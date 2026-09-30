"""Telling the team, wherever it is, what a migration is doing (backlog 32,
R23.10).

Receivers come from two places, and both are used:
* the hop option `notify`: an address, a list of them, or

      notify:
        to: [env:SLACK_HOOK, keyring:migkit-line, pagerduty:env:PD_KEY]
        events: [verdict, drift, lag, cutover, approval, failure,
                 fallback, done]        # all of them when left out
        lag: 300                        # seconds behind before it is said

* `MIGKIT_NOTIFY`: addresses separated by commas, for every hop

An address is written where the hop keeps its passwords - `env:NAME`,
`keyring:name`, `file:/path`, `vault:...` (`config._secret`) - since a
webhook's address is its password; one written out in the file is sent to
and warned about (`hopkeys`), and never printed or written anywhere.

What each address is:
* any address the notification library behind migkit reaches - about a
  hundred services behind one scheme each: Slack (a webhook, or
  `slack://`), Microsoft Teams (a workflow), Google Chat, Discord,
  Lark/Feishu, LINE (its Messaging API: LINE Notify closed in 2025),
  Telegram, email (`mailto://`), SNS, Opsgenie, Mattermost, Rocket.Chat,
  DingTalk, WeCom, ntfy, Pushover, Matrix, Signal, Firebase Cloud
  Messaging to phones, JSON/form webhooks (`json://`, `form://`) - each
  given migkit's message as a title and a text, and shaped by the channel
  (a card coloured by what happened where the channel has cards)
* `pagerduty:<routing key>`: a PagerDuty event, one incident per hop and
  per change tail, triggered while something is wrong and resolved when
  it is right again
* any other `http(s)://` address: the message as `text`, with the facts
  beside it (`event`, `hop`, `status`, ...) for anything that reads JSON

What is told, each an event a hop may keep or leave out (`events`):
* `verdict`: a check's verdict moving between `same`, `different` and
  `error`, compared with what was last told (`notified.json`); a run that
  says `incomplete` is no answer either way, and tells nothing
* `drift`: the source's schema changing under a check or a move, and a
  continuous check (`watch --verify`) finding a difference or losing one
* `lag`: a change tail further behind the source than `lag` seconds, and
  back under it
* `cutover`: each step of the cutover, the write outage measured at the
  flip, and an undo with why
* `approval`: a step waiting for its approvers, and approved
* `failure`: a command stopping on an error, a change tail stopping (or
  setting one table aside), and running again
* `fallback`: a way the engine gave up for another as it ran, in one line
* `done`: a command that wrote (`--go`, `--apply`) finishing

What is sent never carries a row's values: a check, a scope, a status, a
table's name, never a finding's detail; an error that stopped something is
named only by its kind where the hop masks values (`mask`), and never with
a password or a program migkit drives in it. A receiver that cannot be
reached is said, and the run goes on: the run's answer is its own, not the
delivery of it.
"""
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request

PAGERDUTY = "https://events.pagerduty.com/v2/enqueue"
TIMEOUT = 10
#: the verdicts a receiver is told about; `incomplete` is not an answer
TOLD = ("same", "different", "error")
#: every event a hop may be told of
EVENTS = ("verdict", "drift", "lag", "cutover", "approval", "failure",
          "fallback", "done")
#: seconds a change tail may be behind before `lag` is said
LAG = 300

# the library says what it could not deliver in its own log, which would
# print where the operator reads migkit - and can carry parts of an
# address. migkit says it instead, by receiver number
_quiet = logging.getLogger("apprise")
_quiet.addHandler(logging.NullHandler())
_quiet.propagate = False

#: stops already told as something else (a drift that stopped a move), so
#: the failure they end in is not told twice
_ALREADY = set()


def settings(hop):
    """{"to": [address as written], "events": {event}, "lag": seconds}."""
    got = (hop.options or {}).get("notify") if hop else None
    events, lag = set(EVENTS), LAG
    if isinstance(got, dict):
        if got.get("events"):
            events = {str(e) for e in got["events"]} & set(EVENTS)
        try:
            lag = float(got.get("lag", LAG))
        except (TypeError, ValueError):
            lag = LAG
        got = got.get("to")
    if isinstance(got, str):
        got = [got]
    out = []
    for r in list(got or []) + os.environ.get("MIGKIT_NOTIFY", "").split(","):
        r = str(r).strip()
        if r and r not in out:
            out.append(r)
    return {"to": out, "events": events, "lag": lag}


def receivers(hop):
    """The hop's receivers as written - references not read yet."""
    return settings(hop)["to"]


def _address(receiver):
    """The receiver's real address: a reference read from where it is
    kept, a PagerDuty routing key's too."""
    from . import addresses
    from .config import _secret
    if receiver.startswith("pagerduty:"):
        key = receiver.split(":", 1)[1]
        return "pagerduty:" + (_secret(key) if addresses.is_reference(key)
                               else key)
    return _secret(receiver) if addresses.is_reference(receiver) \
        else receiver


def _channel(address):
    """The library's sender for an address, or None where it reaches no
    service by it. Every message goes out under migkit's own name - the
    sender a channel shows, an email's `X-Application` - with no image of
    the library's: measured over twenty channels with every request
    caught, none carried the library's name once this was set."""
    import apprise
    asset = apprise.AppriseAsset(
        app_id="migkit", app_desc="migkit",
        app_url="https://github.com/arkashira/migkit",
        image_url_mask=None, image_url_logo=None)
    try:
        return apprise.Apprise.instantiate(address, asset=asset)
    except Exception:  # noqa: BLE001 - an address it cannot read
        return None


def body(receiver, text, facts, trouble, dedup, severity="error"):
    """(url, JSON body) for an address migkit sends to itself - PagerDuty
    and a plain JSON webhook - or None for one the library reaches, or
    none can."""
    if receiver.startswith("pagerduty:"):
        event = {"routing_key": receiver.split(":", 1)[1],
                 "event_action": "trigger" if trouble else "resolve",
                 "dedup_key": dedup}
        if trouble:
            event["payload"] = {"summary": text[:1024], "source": "migkit",
                                "severity": severity,
                                "custom_details": facts}
        return PAGERDUTY, event
    if not receiver.startswith(("http://", "https://")) \
            or _channel(receiver) is not None:
        return None
    return receiver, {**facts, "text": text}


#: how a message is coloured where the channel colours them
_KIND = {"error": "FAILURE", "warning": "WARNING", "info": "INFO",
         "ok": "SUCCESS"}


def _title(hop, facts):
    return f"migkit {facts.get('event', 'news')}: {hop.name}"


def send(hop, text, facts, trouble, dedup, log, severity="error",
         event=None):
    """Sends to every receiver the hop's `events` let hear `event`; how
    many took it."""
    conf = settings(hop)
    if event is not None and event not in conf["events"]:
        return 0
    sent = 0
    kind = "ok" if not trouble else severity
    for i, receiver in enumerate(conf["to"], 1):
        try:
            address = _address(receiver)
        except SystemExit as e:
            log(f"notify receiver {i} could not be read from where it is"
                f" kept ({e}); the run goes on")
            continue
        got = body(address, text, facts, trouble, dedup, severity)
        if got is None:
            channel = _channel(address)
            if channel is None:
                log(f"notify receiver {i} is not an address migkit can send"
                    " to, so nothing was sent to it")
                continue
            sent += _by_channel(channel, i, _title(hop, facts), text, kind,
                                log)
            continue
        sent += _posted(i, *got, log)
    return sent


def _by_channel(channel, i, title, text, kind, log):
    import apprise
    try:
        ok = channel.notify(body=text, title=title,
                            notify_type=getattr(apprise.NotifyType,
                                                _KIND.get(kind, "INFO")),
                            body_format=apprise.NotifyFormat.TEXT)
    except Exception as e:  # noqa: BLE001 - said; the run goes on
        log(f"notify receiver {i} ({channel.service_name}) failed"
            f" ({type(e).__name__}); the run goes on")
        return 0
    if not ok:
        log(f"notify receiver {i} ({channel.service_name}) did not take"
            " the message; the run goes on")
    return int(bool(ok))


def _posted(i, url, payload, log):
    req = urllib.request.Request(
        url, data=json.dumps(payload, default=str).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            resp.read()
        return 1
    except urllib.error.HTTPError as e:
        log(f"notify receiver {i} answered {e.code}; the run goes on")
    except (urllib.error.URLError, OSError) as e:
        why = getattr(e, "reason", None) or e
        log(f"notify receiver {i} was not reached"
            f" ({type(why).__name__}); the run goes on")
    return 0


# ---- what is remembered between runs ---------------------------------------

def _told(hop):
    return hop.report_dir() / "notified.json"


def _memory(hop):
    try:
        got = json.loads(_told(hop).read_text())
    except (OSError, ValueError):
        got = {}
    return got if isinstance(got, dict) else {}


def _remember(hop, **kept):
    got = _memory(hop)
    got.update(kept)
    _told(hop).write_text(json.dumps(got, default=str))


def _clean(hop, text):
    """An error's words as a receiver may read them: never a password or a
    program migkit drives, and only its kind where the hop masks values
    (the words of an error can quote the row it was writing)."""
    from . import masking
    from .util import without_secret
    text = str(text or "")
    if masking.active(hop):
        kind = text.split(":", 1)[0].strip() if ":" in text else "an error"
        return (f"{kind}; the hop masks values, so what it said is only in"
                " migkit's own output")
    for secret in (hop.source.password, hop.target.password):
        text = without_secret(text, secret)
    from .export import words
    return words(text)


# ---- the events --------------------------------------------------------------

def verdict(hop, env, log, key="verdict"):
    """After a check (`key` "verdict") or a watching check's cycle
    ("watch", told as `drift`): sends when the verdict is not what
    receivers were last told. Returns how many receivers took it (0 when
    nothing was due)."""
    now = env.get("status")
    if now not in TOLD or not receivers(hop):
        return 0
    memory = _memory(hop)
    told = (memory.get("status") if key == "verdict"
            else (memory.get(key) or {}).get("status"))
    # nobody told anything yet is as good as told it was the same
    if now == (told or "same"):
        return 0
    findings = env.get("findings") or []
    listed = "; ".join(f"{f.get('status')} {f.get('check')}"
                       f" {f.get('scope')}" for f in findings[:5])
    what = "is" if key == "verdict" else "is now, as the watch checks it,"
    text = (f"migkit: {hop.name} {what} {now}"
            + (f", was {told}" if told else "")
            + (f": {listed}" if listed else "")
            + (f" and {len(findings) - 5} more" if len(findings) > 5
               else ""))
    facts = {"event": "verdict" if key == "verdict" else "drift",
             "hop": hop.name, "status": now, "was": told,
             "totals": env.get("totals"),
             "findings": [{k: f.get(k) for k in
                           ("status", "check", "scope", "category")}
                          for f in findings[:20]]}
    sent = send(hop, text, facts, now != "same",
                f"migkit-{hop.name}" + ("" if key == "verdict"
                                        else f"-{key}"), log,
                event=facts["event"])
    if sent:
        # only what somebody was told: a receiver that was down hears
        # about it on the next check
        mark = {"status": now, "at": time.time()}
        if key == "verdict":
            _remember(hop, **mark)
        else:
            _remember(hop, **{key: mark})
    return sent


def already(stop):
    """The words a command is about to stop with, told already as what
    they are (a drift, an approval asked, a cutover undone): not told
    again as a failure."""
    if stop and str(stop).strip():
        _ALREADY.add(str(stop).strip().splitlines()[0][:300])


def drift(hop, db, said, log, stop=None):
    """The source's schema changed under a check or a move: `said` is
    what changed, in the drift reader's words (names, never values).
    `stop` is the words a move stops with, told here and not again as a
    failure."""
    already(stop)
    changed = list(said or [])
    text = (f"migkit: the source of {hop.name} {db} changed its schema"
            + (" during a move, which stopped" if stop else
               " during a check, whose answers it overtook")
            + ": " + "; ".join(changed[:5])
            + (f" and {len(changed) - 5} more" if len(changed) > 5 else ""))
    return send(hop, text, {"event": "drift", "hop": hop.name, "db": db,
                            "changed": changed[:20]},
                True, f"migkit-{hop.name}-{db}-drift", log,
                severity="warning", event="drift")


def lags(hop, dbs, log):
    """Each database whose change tail is further behind than the hop's
    `lag` allows, told once as it goes past and once as it comes back.
    How far behind is the tail's own heartbeat (`tailctl`)."""
    from . import tailctl
    if not receivers(hop):
        return 0
    limit = settings(hop)["lag"]
    kept = dict(_memory(hop).get("lag") or {})
    sent = 0
    for db in dbs:
        st = tailctl.state(hop.report_dir(db))
        if not st or st.get("behind") is None:
            continue
        behind = float(st["behind"])
        over = behind > limit
        if over == bool(kept.get(db)):
            continue
        text = (f"migkit: the change tail of {hop.name} {db} is"
                f" {behind:,.0f}s behind the source, past the {limit:g}s"
                " the hop allows" if over else
                f"migkit: the change tail of {hop.name} {db} is back within"
                f" {limit:g}s of the source ({behind:,.0f}s)")
        got = send(hop, text, {"event": "lag", "hop": hop.name, "db": db,
                               "behind_seconds": round(behind, 1),
                               "limit_seconds": limit},
                   over, f"migkit-{hop.name}-{db}-lag", log,
                   severity="warning", event="lag")
        if got:
            kept[db] = over
            sent += got
    if sent:
        _remember(hop, lag=kept)
    return sent


def cutover(hop, rec, entry, log=None):
    """One step of the cutover as its record writes it (`cutover._audit`):
    begun, a phase done with its seconds, flipped with the write outage
    measured, or undone with why."""
    log = log or _stderr
    what = entry.get("event")
    phase = entry.get("phase")
    run = rec.get("id")
    if what == "began":
        text = (f"migkit: the cutover of {hop.name} began"
                f" ({', '.join(map(str, entry.get('dbs') or []))}), a"
                f" {entry.get('budget')}s budget for the write outage")
    elif what == "flipped":
        outage = float(entry.get("outage") or 0)
        text = (f"migkit: {hop.name} is cut over, {outage:.2f}s of write"
                " outage")
    elif what in ("undone", "undoing"):
        text = (f"migkit: the cutover of {hop.name} was undone:"
                f" {entry.get('why') or 'it did not finish'}; the source is"
                " writable and has what it had")
    elif phase:
        text = (f"migkit: the cutover of {hop.name}: {phase}"
                f" {what or 'done'}"
                + (f" in {entry.get('seconds')}s" if entry.get("seconds")
                   is not None else ""))
    else:
        return 0
    trouble = what in ("undone", "undoing")
    return send(hop, text, {"event": "cutover", "hop": hop.name, "run": run,
                            "step": phase or what,
                            "seconds": entry.get("seconds"),
                            "outage": entry.get("outage")},
                trouble, f"migkit-{hop.name}-cutover", log,
                severity="error" if trouble else "info", event="cutover")


def approval_asked(hop, step, db, need, have, until, request, log=None,
                   stop=None):
    """A step waits for its approvers: told once for each request (its
    nonce), however often the step is run again while it waits, and
    again only once an approver has signed."""
    already(stop)
    asked = dict(_memory(hop).get("asked") or {})
    if asked.get(str(request)) == have:
        return 0
    text = (f"migkit: {step} on {hop.name}"
            + (f" {db}" if db else "")
            + f" waits for {need} approval{'s' if need > 1 else ''}"
            f" ({have} so far), until"
            f" {time.strftime('%F %T', time.localtime(until))}")
    sent = send(hop, text, {"event": "approval", "hop": hop.name,
                            "step": step, "db": db, "need": need,
                            "have": have},
                True, f"migkit-{hop.name}-{step}-{db or 'all'}-approval",
                log or _stderr, severity="warning", event="approval")
    if sent:
        asked[str(request)] = have
        _remember(hop, asked=dict(list(asked.items())[-50:]))
    return sent


def approved(hop, step, db, who, log=None):
    text = (f"migkit: {step} on {hop.name}" + (f" {db}" if db else "")
            + f" approved by {', '.join(who)}")
    return send(hop, text, {"event": "approval", "hop": hop.name,
                            "step": step, "db": db, "approvers": list(who)},
                False, f"migkit-{hop.name}-{step}-{db or 'all'}-approval",
                log or _stderr, event="approval")


def failed(hop, command, why, log):
    """A command stopped on an error: said once, in one line."""
    first = str(why or "").strip().splitlines()[0][:300] if why else ""
    if first in _ALREADY:
        _ALREADY.discard(first)
        return 0
    text = f"migkit: {command} on {hop.name} stopped: {_clean(hop, first)}"
    return send(hop, text, {"event": "failure", "hop": hop.name,
                            "command": command},
                True, f"migkit-{hop.name}-{command}", log,
                event="failure")


def fallback(hop, work, was, why, now, log):
    """A way given up for another as the run went: one line."""
    text = (f"migkit: {hop.name} {work}: {was} could not go on"
            f" ({_clean(hop, why)}) - now {now}")
    return send(hop, text, {"event": "fallback", "hop": hop.name,
                            "work": work, "from": was, "to": now},
                False, f"migkit-{hop.name}-fallback", log,
                severity="warning", event="fallback")


def done(hop, command, seconds, log):
    """A command that wrote finished."""
    text = f"migkit: {command} on {hop.name} done in {_took(seconds)}"
    return send(hop, text, {"event": "done", "hop": hop.name,
                            "command": command,
                            "seconds": round(seconds, 1)},
                False, f"migkit-{hop.name}-{command}", log, event="done")


def _took(seconds):
    from .util import human_secs
    return human_secs(seconds)


def _stderr(message):
    import sys
    print(message, file=sys.stderr)


def tail_stopped(hop, db, stopped, log):
    """`stopped` is what the tail kept (`tailctl.stopped`)."""
    from . import masking
    why = stopped.get("why") or stopped.get("kind") or "an error"
    if masking.active(hop):
        # an error from the target can quote the row it was writing
        why = (f"{stopped.get('kind') or 'an error'}; the hop masks values,"
               " so what it said is only in the tail's own output")
    text = f"migkit: the change tail of {hop.name} {db} stopped: {why}"
    return send(hop, text, {"event": "tail stopped", "hop": hop.name,
                            "db": db, "why": why},
                True, f"migkit-{hop.name}-{db}-tail", log, event="failure")


def table_parked(hop, db, table, why, log):
    """One table of a running tail set aside (`tailtables`): the tail goes
    on for every other table, and this one waits for its target."""
    from . import masking
    if masking.active(hop):
        why = "its target does not follow its source's shape"
    text = (f"migkit: the change tail of {hop.name} {db} parked {table}:"
            f" {why}. The other tables go on")
    return send(hop, text, {"event": "table parked", "hop": hop.name,
                            "db": db, "table": table, "why": why},
                True, f"migkit-{hop.name}-{db}-{table}-parked", log,
                event="failure")


def tail_started(hop, db, log):
    """A tail that had stopped with an error is running again."""
    text = f"migkit: the change tail of {hop.name} {db} is running again"
    return send(hop, text, {"event": "tail started", "hop": hop.name,
                            "db": db},
                False, f"migkit-{hop.name}-{db}-tail", log,
                event="failure")


# ---- the library's account (`tooluse`, backlog 0f) ---------------------------
# what of it is used is read off the two functions that drive it; only the
# reasons for the rest are written

_FEATURES = {"instantiate": "instantiate", "notify": "notify",
             "NotifyType": "NotifyType", "NotifyFormat": "NotifyFormat",
             "title": "title", "body_format": "body_format",
             "service_name": "service_name", "attach": "attach",
             "tag": "tag", "AppriseConfig": "AppriseConfig",
             "AppriseAsset": "AppriseAsset", "async_notify": "async_notify",
             "persistent storage": "PersistentStore"}


def _account():
    from . import tooluse
    unused = {
        "attach": ("nothing is attached: the evidence stays beside the"
                   " hop's reports, under its encryption and its access"),
        "tag": ("which receiver hears which event is the hop's own"
                " `events`, one list for every receiver"),
        "AppriseConfig": ("receivers are the hop's option and MIGKIT_NOTIFY,"
                          " kept where the hop keeps its passwords - not a"
                          " second configuration file"),
        "async_notify": ("receivers are told one after another, so a"
                         " failure is said by its receiver's number"),
        "persistent storage": ("what each receiver was last told is"
                               " migkit's own record, kept with the hop"
                               " (notified.json)"),
    }
    tooluse.register(
        "apprise", licence="BSD-2-Clause", release="2.0.0",
        declared={"": frozenset(_FEATURES)},
        reads=(("notify.py::_channel", "notify.py::_by_channel"),
               {("", f): t for f, t in _FEATURES.items()}),
        options={("", f): (tooluse.UNUSED, why)
                 for f, why in unused.items()})


_account()
