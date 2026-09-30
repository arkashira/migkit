"""The simulations the smart decision engine's designs were chosen by
(backlog P0-SMART). No server: every number here is a model's, drawn
from the seed given, so a run of this file gives the numbers the
docstrings of `migkit.jointplan` and `migkit.costmodel` quote.

    python bench/smart_decisions.py schedule [--cpsat] [--moves 200]
    python bench/smart_decisions.py costs [--trials 400]
    python bench/smart_decisions.py explore [--trials 400]

`schedule` sets the joint plan (`jointplan.joint`) against the per-choice
greedy migkit ran before (`jointplan.greedy`) on simulated moves - and,
with `--cpsat` where OR-Tools is importable, against CP-SAT's schedule,
all three simulated on the same flows. `costs` sets the cost model
(`costmodel`) against one number a rung. `explore` sets safe exploration
(`decide.Trials` with a model) against trying every rung once.
"""
import math
import random
import statistics
import sys
import time

sys.path.insert(0, __file__.rsplit("/bench/", 1)[0])

from migkit import jointplan as jp  # noqa: E402

MB = 2 ** 20


# --- schedule -----------------------------------------------------------------

def workload(rng):
    """One simulated move: its tables, their ways and the limits."""
    n = rng.choice([5, 12, 30, 60, 150])
    s = rng.uniform(0.5, 1.5)
    pas = jp.Way("pass", per_stream=40 * MB * s,
                 uses={"source": 2, "target": 1}, startup=0.2, spread=0.15)
    pipe = jp.Way("pipe", per_stream=25 * MB * rng.uniform(0.5, 1.5),
                  uses={"source": 1, "link:src": 1, "cpu": 2, "link:dst": 2,
                        "target": 2}, startup=0.05, spread=0.2,
                  bills=rng.choice([0.0, 0.0, 0.09 / 2 ** 30]))
    bulk = jp.Way("bulk", per_stream=60 * MB * rng.uniform(0.5, 1.5),
                  uses={"source": 1, "target": 1.3}, startup=1.5,
                  spread=0.25, risk=rng.choice([0.0, 0.02, 0.1]), lost=1.0)
    check = jp.Way("digest", per_stream=80 * MB, uses={"source": 1,
                                                        "target": 1})
    jobs = []
    for i in range(n):
        size = math.exp(rng.gauss(math.log(200 * MB), 1.8))
        width = math.exp(rng.gauss(math.log(200), 1.0))
        rows = max(1, int(size / width))
        keyed = rng.random() > 0.1
        ways = (pas, pipe, bulk) if keyed else (pipe, bulk)
        jobs.append(jp.Job(f"t{i}", size, rows,
                           size * 0.3 * rng.uniform(0, 1.2),
                           max(1, rows // 100_000) if keyed else 1, ways,
                           check))
    lim = jp.Limits(
        caps={"source": rng.uniform(100, 600) * MB,
              "target": rng.uniform(60, 400) * MB,
              "link:src": rng.uniform(50, 1250) * MB,
              "link:dst": rng.uniform(50, 1250) * MB,
              "cpu": rng.uniform(100, 800) * MB},
        workers=rng.choice([4, 8, 16]), index_rate=30 * MB,
        index_at_once=2,
        source_share=rng.choice([None, None, 0.3]))
    if pipe.bills and rng.random() < 0.5:
        # half the hops that bill a way say how much they may spend
        total = sum(j.bytes for j in jobs)
        lim.money = 0.3 * total * pipe.bills
    return jobs, lim


def cpsat(jobs, lim, seconds=5.0):
    """CP-SAT's schedule of the same move: each table's copy an optional
    interval per way at its rate alone, its index builds and its check
    after it, every resource and the workers a cumulative - the way and
    the order it gives, simulated on the same flows."""
    from ortools.sat.python import cp_model
    W = lim.workers
    caps = {r: lim.cap(r) for r in lim.caps if lim.cap(r)}
    alone = {(j.name, w.name): jp.alone(j, w, lim) for j in jobs
             for w in j.ways}
    horizon_s = sum(min(alone[(j.name, w.name)] for w in j.ways) for j in
                    jobs) * 3 + 10
    scale = max(horizon_s / 20000.0, 1e-3)
    H = int(horizon_s / scale) + 1
    m = cp_model.CpModel()
    ends, starts, pres = [], {}, {}
    cum = {r: ([], []) for r in list(caps) + ["workers", "index"]}
    for j in jobs:
        copies = []
        cend = m.NewIntVar(0, H, f"ce{j.name}")
        for w in j.ways:
            k = min(w.most or j.ranges, j.ranges, W)
            rate = k * w.per_stream
            for r, n in w.uses.items():
                if r in caps and n:
                    rate = min(rate, caps[r] / n)
            d = max(1, int(math.ceil(alone[(j.name, w.name)] / scale)))
            p = m.NewBoolVar(f"p{j.name}{w.name}")
            s = m.NewIntVar(0, H, f"s{j.name}{w.name}")
            e = m.NewIntVar(0, H, f"e{j.name}{w.name}")
            iv = m.NewOptionalIntervalVar(s, d, e, p, f"i{j.name}{w.name}")
            m.Add(cend == e).OnlyEnforceIf(p)
            copies.append(p)
            pres[(j.name, w.name)] = p
            starts[(j.name, w.name)] = s
            cum["workers"][0].append(iv)
            cum["workers"][1].append(k)
            for r, n in w.uses.items():
                if r in caps and n:
                    cum[r][0].append(iv)
                    cum[r][1].append(max(1, int(1000 * rate * n / caps[r])))
        m.AddExactlyOne(copies)
        last = cend
        if j.index_bytes and lim.index_rate:
            d = max(1, int(math.ceil(j.index_bytes / lim.index_rate / scale)))
            s = m.NewIntVar(0, H, f"is{j.name}")
            e = m.NewIntVar(0, H, f"ie{j.name}")
            iv = m.NewIntervalVar(s, d, e, f"ii{j.name}")
            m.Add(s >= cend)
            cum["index"][0].append(iv)
            cum["index"][1].append(1)
            if "target" in caps:
                cum["target"][0].append(iv)
                cum["target"][1].append(max(1, int(
                    1000 * lim.index_rate / caps["target"])))
            last = e
        if j.check is not None:
            c = j.check
            k = min(c.most or j.ranges, j.ranges, W)
            rate = k * c.per_stream
            for r, n in c.uses.items():
                if r in caps and n:
                    rate = min(rate, caps[r] / n)
            d = max(1, int(math.ceil(j.bytes / rate / scale)))
            s = m.NewIntVar(0, H, f"cs{j.name}")
            e = m.NewIntVar(0, H, f"cf{j.name}")
            iv = m.NewIntervalVar(s, d, e, f"ci{j.name}")
            m.Add(s >= last)
            cum["workers"][0].append(iv)
            cum["workers"][1].append(k)
            for r, n in c.uses.items():
                if r in caps and n:
                    cum[r][0].append(iv)
                    cum[r][1].append(max(1, int(1000 * rate * n / caps[r])))
            last = e
        ends.append(last)
    for r, (ivs, dem) in cum.items():
        if not ivs:
            continue
        cap = {"workers": W, "index": lim.index_at_once}.get(r, 1000)
        m.AddCumulative(ivs, dem, cap)
    if lim.money is not None:
        m.Add(sum(pres[(j.name, w.name)] * int(w.bills * j.bytes * 1e6)
                  for j in jobs for w in j.ways) <= int(lim.money * 1e6))
    mk = m.NewIntVar(0, H, "mk")
    m.AddMaxEquality(mk, ends)
    m.Minimize(mk)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = seconds
    solver.parameters.num_workers = 8
    st = solver.Solve(m)
    if st not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return None
    choice, first = {}, {}
    for j in jobs:
        for w in j.ways:
            if solver.Value(pres[(j.name, w.name)]):
                choice[j.name] = w
                first[j.name] = solver.Value(starts[(j.name, w.name)])
    order = sorted(first, key=first.get)
    # its ways and order, given the same choice of tables at once the
    # joint plan has
    return min((jp.simulate(jobs, choice, lim, order, tables=k)
                for k in jp._at_once(lim.workers)),
               key=lambda p: p.makespan)


def schedule(moves=200, seed=7, with_cpsat=False):
    rng = random.Random(seed)
    ratios, cps, slack, took_j, took_c, better = [], [], [], [], [], 0
    order_only = []
    over_money = 0
    for _ in range(moves):
        jobs, lim = workload(rng)
        g, gchoice = jp.greedy(jobs, lim)
        t0 = time.perf_counter()
        j = jp.joint(jobs, lim)
        took_j.append(time.perf_counter() - t0)
        slack.append(j.makespan / max(j.bound, 1e-9))
        if lim.money is not None and jp.money(jobs, gchoice) > lim.money:
            # the greedy spends more than the hop allows: no end to weigh
            # the joint plan's against
            over_money += 1
        else:
            ratios.append(j.makespan / g.makespan)
            order_only.append(jp.joint(jobs, lim, search=False).makespan
                              / g.makespan)
            if j.makespan < 0.95 * g.makespan:
                better += 1
        if with_cpsat:
            t0 = time.perf_counter()
            c = cpsat(jobs, lim)
            took_c.append(time.perf_counter() - t0)
            if c is not None:
                cps.append(c.makespan / j.makespan)
    q = statistics.quantiles
    out = {"moves": moves,
           "joint/greedy mean": round(statistics.mean(ratios), 3),
           "joint/greedy median": round(statistics.median(ratios), 3),
           "joint/greedy p10": round(q(ratios, n=10)[0], 3),
           "joint/greedy best": round(min(ratios), 3),
           "joint/greedy worst": round(max(ratios), 3),
           "order and overlap only/greedy mean": round(
               statistics.mean(order_only), 3),
           "joint 5% or more sooner": f"{better} of {len(ratios)}",
           "joint/lower bound p90": round(q(slack, n=10)[-1], 3),
           "greedy over the money limit": over_money,
           "joint/lower bound median": round(statistics.median(slack), 3),
           "joint ms median": round(1000 * statistics.median(took_j), 1),
           "joint ms max": round(1000 * max(took_j), 1)}
    if cps:
        out.update({"cpsat/joint mean": round(statistics.mean(cps), 3),
                    "cpsat/joint median": round(statistics.median(cps), 3),
                    "cpsat/joint best": round(min(cps), 3),
                    "cpsat s median": round(statistics.median(took_c), 2)})
    return out


# --- costs --------------------------------------------------------------------

#: three ways' true seconds: start, a MB, a thousand rows, a MB of large
#: values more, a keyless MB more, round trips a thousand rows, a busy MB
#: more - a server-to-server pass (fast bytes, slow on large values), a
#: pipe through migkit (a round trip a batch of 1,000 rows) and a bulk
#: program (slow to start, fast after)
TRUE = {"pass": (0.3, 1 / 120, 0.0005, 1 / 5, 0.0, 0.01, 1 / 120),
        "pipe": (0.05, 1 / 40, 0.002, 1 / 30, 0.0, 1.0, 1 / 200),
        "bulk": (1.2, 1 / 200, 0.0002, 1 / 20, 1 / 100, 0.001, 1 / 100)}

SCENARIOS = {
    "as trained": dict(mb=(1, 30), row=(100, 300), lob=0.0, rtt=0.0003,
                       load=(0.1, 0.3)),
    "500x larger": dict(mb=(4000, 8000), row=(100, 300), lob=0.0,
                        rtt=0.0003, load=(0.1, 0.3)),
    "large values": dict(mb=(1000, 3000), row=(2000, 6000), lob=0.5,
                         rtt=0.0003, load=(0.1, 0.3)),
    "a far link": dict(mb=(500, 1500), row=(100, 300), lob=0.0, rtt=0.05,
                       load=(0.1, 0.3)),
    "a busy source": dict(mb=(500, 1500), row=(100, 300), lob=0.0,
                          rtt=0.0003, load=(0.85, 0.95)),
}


def _true(coef, u):
    from migkit import costmodel as cm
    return float(cm.features(u) @ __import__("numpy").array(coef))


def _unit(rng, sc):
    mb = rng.uniform(*sc["mb"])
    row = rng.uniform(*sc["row"])
    b = mb * MB
    return {"bytes": b, "rows": int(b / row), "lob_bytes": sc["lob"] * b,
            "keyless": False, "rtt": sc["rtt"], "load": rng.uniform(
                *sc["load"])}


def costs(trials=400, seed=11, off=0.5, share=None):
    """Each way timed on four small units of the first tables, then the
    way chosen for a unit of each scenario by: the last seconds a row
    (what `Costs` kept), the mean seconds a byte, least squares on the
    features, the cost model with a prediction as its prior (the
    coefficients off by up to x1.6 either way, as a resource model's
    prediction is), and the prediction alone. Regret: how much longer
    the way chosen takes than the best."""
    import numpy as np

    from migkit import costmodel as cm
    rng = random.Random(seed)
    train = SCENARIOS["as trained"]
    names = list(TRUE)
    out = {}
    for sc_name, sc in SCENARIOS.items():
        regret = {k: [] for k in ("one number a row", "one number a byte",
                                  "least squares", "cost model",
                                  "prediction alone")}
        for _ in range(trials):
            coef = {n: tuple(c * math.exp(rng.gauss(0, 0.4)) for c in
                             TRUE[n]) for n in names}
            prior = {n: np.array([c * math.exp(rng.gauss(0, off))
                                  for c in coef[n]]) for n in names}
            seen = {n: [] for n in names}
            for n in names:
                for _ in range(4):
                    u = _unit(rng, train)
                    y = _true(coef[n], u) * math.exp(rng.gauss(0, 0.15))
                    seen[n].append((u, y))
            test = _unit(rng, sc)
            truth = {n: _true(coef[n], test) for n in names}
            best = min(truth.values())
            guess = {}
            guess["one number a row"] = {
                n: test["rows"] * seen[n][-1][1] / seen[n][-1][0]["rows"]
                for n in names}
            guess["one number a byte"] = {
                n: test["bytes"] * sum(y for _, y in seen[n])
                / sum(u["bytes"] for u, _ in seen[n]) for n in names}
            ls, bm, pa = {}, {}, {}
            for n in names:
                X = np.array([cm.features(u) for u, _ in seen[n]])
                y = np.array([y for _, y in seen[n]])
                w = np.linalg.lstsq(X, y, rcond=None)[0]
                ls[n] = float(cm.features(test) @ w)
                f = cm.Fit(prior[n], cm._spread(
                    prior[n], share or cm.PRIOR_SHARE))
                pa[n] = f.predict(cm.features(test))[0]
                for u, yy in seen[n]:
                    f.update(cm.features(u), yy)
                bm[n] = f.predict(cm.features(test))[0]
            guess["least squares"] = ls
            guess["cost model"] = bm
            guess["prediction alone"] = pa
            for k, g in guess.items():
                pick = min(names, key=lambda n: g[n])
                regret[k].append(truth[pick] / best - 1)
        out[sc_name] = {k: (round(100 * statistics.mean(v), 1),
                            round(100 * sum(x > 0.05 for x in v) / len(v)))
                        for k, v in regret.items()}
    return out


# --- explore ------------------------------------------------------------------

def explore(trials=300, seed=13, ranges=60):
    """One table's ranges, each on a way chosen by `decide.Trials`: as
    before (each way never timed tried once, in its list's order, then
    the cheapest a row), and safely - with a cost model, with and without
    a prediction for each way (its coefficients off by up to x1.6). The
    ranges' sizes are skewed (a lognormal of sigma 1 about 100,000 rows),
    one scenario a trial. Regret: the table's time over the time with
    every range on its fastest way."""
    import numpy as np

    from migkit import costmodel as cm
    from migkit import decide as d
    rng = random.Random(seed)
    names = list(TRUE)
    got = {"each once": [], "safe, nothing predicted": [],
           "safe, predicted": []}
    for _ in range(trials):
        sc = SCENARIOS[rng.choice(list(SCENARIOS))]
        coef = {n: tuple(c * math.exp(rng.gauss(0, 0.4)) for c in TRUE[n])
                for n in names}
        prior = {n: np.array([c * math.exp(rng.gauss(0, 0.5))
                              for c in coef[n]]) for n in names}
        row = rng.uniform(*sc["row"])
        units = []
        for _ in range(ranges):
            rows = int(100_000 * math.exp(rng.gauss(0, 1.0)))
            b = rows * row
            units.append({"rows": rows, "bytes": b,
                          "lob_bytes": sc["lob"] * b, "keyless": False,
                          "rtt": sc["rtt"], "load": rng.uniform(
                              *sc["load"])})
        noise = [math.exp(rng.gauss(0, 0.15)) for _ in units]
        best = sum(min(_true(coef[n], u) for n in names) for u in units)
        order = names[:]
        rng.shuffle(order)
        rungs = [d.Rung(n, n) for n in order]
        for k in got:
            if k == "each once":
                t = d.Trials("t", rungs)
            else:
                m = cm.CostModel("t", rungs, unit=units[0], knowledge=False)
                if k == "safe, predicted":
                    for n in names:
                        m.fits[n] = cm.Fit(prior[n], cm._spread(prior[n]),
                                           "the resource model")
                t = d.Trials("t", rungs, model=m, unit=units[0],
                             ahead=[u["rows"] for u in units])
            total = 0.0
            for u, e in zip(units, noise):
                way = (t.pick() if k == "each once" else t.pick(u)).path
                secs = _true(coef[way], u) * e
                total += secs
                if k == "each once":
                    t.took(way, u["rows"], secs)
                else:
                    t.took(way, u["rows"], secs, u)
            got[k].append(total / best - 1)
    return {k: {"regret mean %": round(100 * statistics.mean(v), 1),
                "regret median %": round(100 * statistics.median(v), 1),
                "regret p90 %": round(100 * statistics.quantiles(
                    v, n=10)[-1], 1)}
            for k, v in got.items()}


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "schedule"
    n = int(sys.argv[sys.argv.index("--moves") + 1]) if "--moves" in \
        sys.argv else 200
    if what == "schedule":
        print(schedule(n, with_cpsat="--cpsat" in sys.argv))
    elif what == "costs":
        # the prediction off by x1.6 at one sigma; then off by x2, as the
        # resource model's one run was, at its own wider spread
        for k, v in costs().items():
            print(k, v)
        if "--wide" in sys.argv:
            from migkit import costmodel as _cm
            for k, v in costs(off=0.7, share=_cm.MODEL_SHARE).items():
                print("off x2:", k, v)
    elif what == "explore":
        for k, v in explore().items():
            print(k, v)
