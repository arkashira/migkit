"""Rehearse, then forecast (backlog R23, 2).

The owner's intent: right the first time in production. A rehearsal on a
copy - UAT holding production's shape, a clone, a sample - is production's
dry run: every engine timed, every problem met and fixed there, and the
two sides proved equal before the real run starts.

A hop whose goal is `rehearse` runs the whole path the hop it rehearses
will run - the copy, the changes followed, the check, the cutover - into a
target of its own, and names that hop:

    hops:
      orders-uat:
        goal: rehearse
        source: postgres://app@uat-db.example.com/orders_uat
        target: postgres://app@rehearsal.example.com/orders_uat
        options:
          rehearses: orders      # or {hop: orders, databases: {orders_uat: orders}}

Nothing here estimates anything. What the rehearsal measures is kept where
every plan already reads its numbers from - each database's resource model
(`model`: the probes' stages, and the seconds a row the copy settled on),
each copy's rows and seconds (`planner.record_rate`), the check's time
(`CHECK`, `record_check`), each cutover's outage and phases (`cutover`) -
and the hop it rehearses is planned the way any hop is (`planner.phases`),
the rehearsal's numbers taken before any other hop's and named ("measured
on the rehearsal orders-uat"), on that hop's own size from its own
catalogue. Where the real hop's servers are probed now, their measurements
stand, and the rehearsal's fill in what a probe cannot take without
writing: the target's writes, the check, the cutover.

**The confidence of a sample of that size.** A rehearsal on less data than
the real move says less about it: a cache that held the whole sample may
hold a tenth of the real tables, and a rate measured over minutes says
nothing of the hours after. So the forecast's p90 is widened by how many
times larger the real move is (`sample_spread`) - a rule, not a
measurement - and the plan says the share the rehearsal carried. A
rehearsal of at least the real size is taken like for like.

**Repeating a path already proved.** The rehearsal keeps what decided its
path (`path_of`: the engines, what the hop leaves out and maps, the ways it
may take, the cutover's settings); the real hop's plan says every one that
differs (`differences`), since a time measured on another path is not this
one's. Its last verdict and its last cutover are what `ready` holds the
real run to: a rehearsal that ended with differences is a problem not yet
fixed.

**The real run held to its forecast.** When a real move starts, the phases
it was forecast are kept beside it (`forecast.json`); as the copy and the
cutover end, each is said against its p50 and p90 and kept
(`forecast-runs.jsonl`), so the next forecast can say how the last one
held.
"""
import json
import math
import time
from dataclasses import asdict

GOAL = "rehearse"
FILE = "rehearsal.json"
#: the path a check's whole time is kept under with the hop's rates
CHECK = "check"
#: how far the p90 widens for each factor of ten the real move is larger
#: than the rehearsal, and the most it widens - a rule, not a measurement
PER_DECADE, WIDEST = 0.15, 0.6

#: the options that decide the path a move takes, which a rehearsal must
#: share with the hop it rehearses for its times to be that hop's
PATH_OPTIONS = ("cutover", "reverse", "topology", "two_way",
                "protect_target", "physical", "server_to_server",
                "mask_on_write", "verify_batches", "keep_proof",
                "checksum", "two_readings", "accept_changes",
                "source_engine", "target_engine", "target_rows")
#: what under them names a place or a secret rather than a way
PLACES = {"cutover": ("flip", "flip_undo", "pooler"),
          "physical": ("data", "run", "stop", "start", "restart", "source",
                       "source_id", "target_id", "region", "target_region",
                       "target_role_arn", "kms_key", "subnet_group",
                       "security_groups", "subscription", "resource_group",
                       "location", "project", "address")}


def settings(hop):
    """{"hop": the hop rehearsed, "databases": {this hop's database: that
    hop's}} for a hop whose goal is `rehearse`; None for any other."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("rehearses")
    if isinstance(raw, dict):
        name = str(raw.get("hop") or "")
        dbs = {str(k): str(v)
               for k, v in (raw.get("databases") or {}).items()}
    else:
        name, dbs = str(raw or ""), {}
    return {"hop": name, "databases": dbs}


def _local(name, dbs):
    """A table's name without the database in front of it, where one of
    `dbs` is: `orders_uat.public.t` and `orders.public.t` are one table
    to two hops whose databases are named apart."""
    name = str(name)
    for d in dbs:
        if name.startswith(f"{d}."):
            return name[len(d) + 1:]
    return name


def path_of(hop):
    """What decided this hop's path, in a form two hops are compared by:
    the engines, what it leaves out, maps and filters (without the
    database names, which a rehearsal's differ by), and the options that
    choose its ways without the places and secrets under them."""
    from . import planner
    dbs = [str(d) for d in hop.databases or []]
    mapping = {}
    for part, rules in sorted((hop.mapping or {}).items()):
        if isinstance(rules, dict):
            mapping[part] = {_local(k, dbs): v for k, v in
                             sorted(rules.items())}
    ways = {}
    for k, v in sorted((hop.options or {}).items()):
        if k not in PATH_OPTIONS:
            continue
        if isinstance(v, dict):
            v = {s: x for s, x in sorted(v.items())
                 if s not in PLACES.get(k, ())}
        ways[k] = v
    return {"kind": planner._kind(hop),
            "exclude": sorted(_local(x, dbs) for x in hop.exclude or []),
            "include": sorted(_local(x, dbs) for x in
                              getattr(hop, "include", None) or []),
            "mapping": mapping, "options": ways}


def differences(was, now):
    """[what differs, in words] between two `path_of`s: the rehearsal's
    and the hop it rehearses."""
    out = []
    if was.get("kind") != now.get("kind"):
        out.append(f"the engines: the rehearsal's {was.get('kind')}, this"
                   f" hop's {now.get('kind')}")
    if was.get("exclude") != now.get("exclude"):
        out.append("what is left out (exclude)")
    if (was.get("include") or []) != (now.get("include") or []):
        out.append("what is taken (include)")
    for part in sorted(set(was.get("mapping") or {})
                       | set(now.get("mapping") or {})):
        if (was.get("mapping") or {}).get(part) != \
                (now.get("mapping") or {}).get(part):
            out.append(f"mapping.{part}")
    a, b = was.get("options") or {}, now.get("options") or {}
    for k in sorted(set(a) | set(b)):
        if a.get(k) != b.get(k):
            out.append(f"options.{k}")
    return out


def mark(hop):
    """Kept in the rehearsal's report directory each time it runs: which
    hop it rehearses, its databases paired with that hop's, and what
    decided its path. What `of` finds it by - the reports, not the
    configuration, so a rehearsal run from another machine's reports is
    found as well."""
    got = settings(hop)
    if got is None or not got["hop"]:
        return None
    rec = {"of": got["hop"], "databases": got["databases"],
           "own": [str(d) for d in hop.databases or []],
           "path": path_of(hop), "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    path = hop.report_dir() / FILE
    try:
        path.write_text(json.dumps(rec, indent=1, default=str))
    except OSError:
        return None
    return rec


def of(hop):
    """The latest rehearsal of `hop` in the reports, as {"name", "of",
    "databases", "own", "path", "at", "dir"} - or None where no hop has
    rehearsed it."""
    from . import config
    try:
        dirs = [p for p in config.REPORTS.iterdir() if p.is_dir()]
    except OSError:
        return None
    best = None
    for d in dirs:
        if d.name == hop.name:
            continue
        try:
            rec = json.loads((d / FILE).read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(rec, dict) or rec.get("of") != hop.name:
            continue
        rec["name"], rec["dir"] = d.name, d
        if best is None or str(rec.get("at", "")) > str(best.get("at", "")):
            best = rec
    return best


def db_of(rec, db):
    """The rehearsal's database standing for `db` of the hop it rehearses:
    the one its `databases` pairs with it, the one of the same name, or
    its only one; None where none does."""
    if rec is None:
        return None
    for mine, theirs in (rec.get("databases") or {}).items():
        if theirs == str(db):
            return mine
    own = [str(x) for x in rec.get("own") or []]
    if str(db) in own or (rec["dir"] / str(db)).is_dir():
        return str(db)
    return own[0] if len(own) == 1 else None


def model_dir(rec, db):
    """The rehearsal's report directory for `db`, or None."""
    mine = db_of(rec, db)
    return rec["dir"] / mine if mine else None


def sample_spread(had, now):
    """How far a forecast from a rehearsal that carried `had` rows is
    widened for a move of `now`: `PER_DECADE` for each factor of ten
    between them, at most `WIDEST`; nothing where the rehearsal carried at
    least as much, or either is not known."""
    try:
        had, now = float(had or 0), float(now or 0)
    except (TypeError, ValueError):
        return 0.0
    if had <= 0 or now <= 0 or had >= now:
        return 0.0
    return min(WIDEST, PER_DECADE * math.log10(now / had))


def share_said(had, now):
    """"it carried ~X rows of the ~Y here (Z%)", or ""."""
    if not had or not now:
        return ""
    return (f"it carried ~{int(had):,} rows of the ~{int(now):,} here"
            f" ({min(float(had) / float(now), 9.99):.1%})"
            + ("" if had < now else ", as many or more"))


def line(hop, rec, had=None, now=None):
    """The plan's line saying the forecast stands on the rehearsal: which,
    when, the share it carried, and the path it differs in."""
    if rec is None:
        return ""
    share = share_said(had, now)
    diff = differences(rec.get("path") or {}, path_of(hop))
    widened = sample_spread(had, now)
    return (f"forecast from the rehearsal {rec['name']}"
            f" ({str(rec.get('at', ''))[:10]})"
            + (f": {share}" if share else "")
            + (f"; the p90 widened by {widened:.0%} for a sample that size"
               if widened else "")
            + (f"; it went another way in {', '.join(diff)}, so its times"
               " are not all this hop's" if diff else ""))


def outcome(rec):
    """(verdict status or None, the last cutover's state or None, its
    outage) of the rehearsal - what `ready` holds the real run to."""
    if rec is None:
        return None, None, None
    try:
        verdict = json.loads((rec["dir"] / "verdict.json").read_text())
    except (OSError, ValueError):
        verdict = {}
    last = last_cutover(rec)
    return (verdict.get("status"), (last or {}).get("state"),
            (last or {}).get("outage"))


def last_cutover(rec):
    """The rehearsal's last cutover that ended, as `cutover.last_run`
    reads a hop's own."""
    if rec is None:
        return None
    try:
        lines = (rec["dir"] / "cutover-runs.jsonl").read_text().splitlines()
    except OSError:
        return None
    for text in reversed(lines):
        try:
            return json.loads(text)
        except ValueError:
            continue
    return None


# --- the check's own time ----------------------------------------------------

def record_check(hop, eng, db, seconds):
    """A whole check of `db` kept with the hop's rates (`CHECK`): the rows
    and bytes it read on each side in `seconds` - what the check phase of
    the next plan, this hop's or the one it rehearses, is timed by."""
    from . import planner
    if settings(hop) is None and of(hop) is None:
        # a hop neither rehearsing nor rehearsed plans its check as it
        # always has, from the resource model's stages
        return
    try:
        facts = eng.table_facts("src", db)
    except Exception:  # noqa: BLE001 - no sizes, nothing kept
        return
    rows = size = 0
    for name, f in (facts or {}).items():
        if isinstance(f, dict) and hop.takes_table(db, name):
            rows += int(f.get("rows") or 0)
            size += int(f.get("bytes") or 0)
    if rows and seconds > 0:
        planner.record_rate(hop, CHECK, rows, seconds, size or None)


# --- the forecast, and the run held to it ----------------------------------

def keep_forecast(hop, db, phases, via=""):
    """The phases a real run starts with, kept beside it."""
    rec = of(hop)
    got = {"at": time.time(), "when": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "from": rec["name"] if rec else "", "via": via,
           "phases": [asdict(p) for p in phases or ()]}
    try:
        from . import durable
        durable.write_json(hop.report_dir(db) / "forecast.json", got,
                           indent=1)
    except OSError:
        pass
    return got


def forecast(hop, db):
    try:
        return json.loads((hop.report_dir(db) / "forecast.json").read_text())
    except (OSError, ValueError):
        return None


def _took(secs):
    from .planner import _took as took
    return took(secs)


#: a phase that ended in less than this share of its p50 is said as
#: faster than forecast: the forecast was not wrong in the safe direction
#: only by chance
FASTER = 0.5


def held_to(hop, db, phase, took):
    """[line] saying how `phase` of this run, which took `took` seconds,
    held to its forecast - and kept in `forecast-runs.jsonl`. [] where
    the run was not forecast."""
    got = forecast(hop, db)
    if not got or took is None or phase in (got.get("held") or {}):
        # no forecast, or one this phase was already held to: a forecast
        # speaks for the run that kept it, once
        return []
    mine = next((p for p in got.get("phases") or ()
                 if p.get("name") == phase), None)
    if mine is None:
        return []
    got.setdefault("held", {})[phase] = round(took, 3)
    try:
        from . import durable
        durable.write_json(hop.report_dir(db) / "forecast.json", got,
                           indent=1)
    except OSError:
        pass
    p50, p90 = mine.get("p50"), mine.get("p90")
    whose = (f", forecast from the rehearsal {got['from']}"
             if got.get("from") else "")
    if p50 is None:
        said = (f"{phase}: took {_took(took)}; it was not forecast"
                f" ({mine.get('detail') or 'nothing measured'}){whose}")
        how = "not forecast"
    else:
        top = p90 if p90 and p90 >= p50 else p50
        how = ("slower than its p90" if took > top else
               "faster than half its p50" if took < FASTER * p50 else
               "within its forecast")
        said = (f"{phase}: took {_took(took)} against a forecast of"
                f" ~{_took(p50)}" + (f" (p90 ~{_took(top)})" if top > p50
                                      else "")
                + f"{whose} - {how}")
    try:
        with (hop.report_dir(db) / "forecast-runs.jsonl").open("a") as f:
            f.write(json.dumps({"phase": phase, "took": round(took, 3),
                                "p50": p50, "p90": p90, "held": how,
                                "from": got.get("from") or "",
                                "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
                    + "\n")
    except OSError:
        pass
    return [said]


def last_held(hop, db):
    """How the last forecast of this hop held, phase by phase: {phase:
    "within its forecast" | ...} from `forecast-runs.jsonl`."""
    out = {}
    try:
        lines = (hop.report_dir(db) / "forecast-runs.jsonl").read_text() \
            .splitlines()
    except OSError:
        return out
    for text in lines:
        try:
            r = json.loads(text)
        except ValueError:
            continue
        out[r.get("phase")] = r.get("held")
    return out


def forecast_lines(hop, eng, db):
    """The real hop's phases as its rehearsal forecasts them, for `assess`
    and `ready`: the resource model built from the catalogue and the
    rehearsal's stages, no probe run (`model.build(probe=False)`), and
    `planner.phases` over it. [] where nothing rehearses the hop."""
    from . import model as resource_model
    from . import planner
    rec = of(hop)
    if rec is None:
        return []
    try:
        facts = eng.table_facts("src", db)
        m = resource_model.build(hop, eng, db, facts, probe=False,
                                 write=False)
        phases = planner.phases(hop, m, facts)
    except Exception as e:  # noqa: BLE001 - said, the rest stands
        return [f"the forecast could not be made: "
                f"{(str(e).strip().splitlines() or [''])[-1][:100]}"]
    return [p.line() for p in phases] + [n for n in m.notes
                                         if n.startswith("forecast from")]
