# Smart decisions: what the fields that choose well do, and what migkit took (2026-09-30)

For backlog P0-SMART ("is the decision engine smart or still flat - I want
it really smart"). Five fields that make the same kind of choice migkit
makes - which way, how many at once, in what order, when to try something
new - and what the paid migration tools do when they "choose". Each
section ends with what was taken and how it was measured; the numbers are
from `bench/smart_decisions.py` (in-process simulations, no server), and
are quoted where the code stands on them.

## 1. Query optimizers: a cost model with statistics, and re-planning

* **Cost-based choice from statistics** (System R, Selinger et al., SIGMOD
  1979): each plan is costed from table statistics - rows, pages, value
  distributions - through a formula per operator (I/O pages plus a
  weighted CPU term), and the cheapest plan is chosen. The formula is a
  function of the input's shape, not a number per operator.
* **The estimates are the weak link.** Leis et al., "How Good Are Query
  Optimizers, Really?" (VLDB 2015): cardinality errors grow by orders of
  magnitude with each join, and they - not the cost formula - decide
  whether the plan is good. A cost model with no measure of its own
  uncertainty picks confidently wrong.
* **Robust and least-expected-cost choice** (Chu, Halpern, Seshadri, PODS
  1999; Babcock & Chaudhuri, SIGMOD 2005): treat the estimate as a
  distribution and choose by expected cost, or by a percentile, rather
  than by the point estimate.
* **Re-planning while running.** Kabra & DeWitt (SIGMOD 1998) re-optimise
  mid-query when observed cardinalities differ from the estimates; POP,
  Markl et al. (SIGMOD 2004), puts CHECK points with a *validity range*
  on each plan and re-plans when an observed value leaves it; Oracle's
  adaptive plans (12c) and SQL Server's adaptive joins (2017) switch the
  join method at a point decided at run time; Oracle's statistics
  feedback keeps what was observed for the next run.
* **Learned optimizers.** Bao (Marcus et al., SIGMOD 2021) chooses among a
  few hint sets per query with Thompson sampling over a learned model -
  the search space stays the classical one, the choice learns.

**Taken:** a way's cost is a function of the unit's shape (bytes, rows,
large values, keyless bytes, round trips, the server's load) with its
spread (`costmodel`); a unit whose time leaves the predicted spread is
POP's validity range and triggers a re-climb of what is left
(`decide.Trials`); what a run measured is kept for the next (hop file
and knowledge base), as statistics feedback does.

## 2. Bandits with safety and risk bounds

* **Thompson sampling** (Thompson 1933; Agrawal & Goyal, COLT 2012) and
  **UCB1** (Auer, Cesa-Bianchi, Fischer, 2002) balance trying an arm
  against using the best one; **linear / contextual bandits** (LinUCB,
  Li et al., WWW 2010; linear Thompson sampling, Agrawal & Goyal, ICML
  2013) share what is learned across contexts through a linear model -
  the unit's shape is the context here.
* **Best-arm identification and successive elimination** (Even-Dar,
  Mannor, Mansour, JMLR 2006): stop sampling an arm once its confidence
  bound is beaten by another's - "stop when the lead is outside the
  spread".
* **Conservative bandits** (Wu, Shariff, Lattimore, Szepesvari, ICML
  2016): explore only while the cumulative reward stays within (1 - a) of
  a known safe baseline - a budget for exploring. **Safe exploration**
  (SafeOpt, Sui et al., ICML 2015) never samples where a safety function
  may be violated.

**Taken:** `Trials` with a cost model explores only on a unit no larger
than the median (a slow way is never tried on the biggest range), never
on a deadline's critical path, never where a way's p10 is above the
measured best (eliminated: its lead is outside the spread), times again
a measured way with fewer than two units whose p10 is below the best, and
keeps what exploring cost within 5% of the work's time (the conservative
budget). Measured on 300 simulated tables of 60 skewed ranges: each way
tried once then the least seconds a row, 60.6% longer than every range on
its best way; safe trials with nothing predicted 22.6%; with a
prediction off by up to x1.6, 5.7%. A 2% budget gave 23.7% and 10% gave
21.7% with nothing predicted, equal within 0.2% with a prediction: 5% is
the least that loses nothing measurable. Thompson sampling was not taken
as the rule: with a prior this informative the elimination rule already
stops sampling, and a sampled choice cannot be said in one reason.

## 3. Scheduling to a makespan under resources

* **List scheduling** is within 2 - 1/m of optimal on m machines, **LPT**
  (longest first) within 4/3 - 1/(3m) (Graham, 1966 and 1969). Tables
  split into ranges are *moldable* jobs (Turek, Wolf, Yu, SPAA 1992).
* **Multi-mode resource-constrained project scheduling** (MRCPSP): each
  job has modes with different durations and resource use; exact solvers
  are CP and MIP; OR-Tools' CP-SAT is the strongest open one on the
  PSPLIB benchmarks.
* **The fluid bound** - the most any resource carries over its capacity,
  and the longest job - is a lower bound on the makespan that a good list
  schedule comes close to when jobs are many.

**Taken:** `jointplan` - the ways as one mix by a local search on the
fluid bound (steepest switch first, money a hard limit), each table's
steps (copy, index builds, check) simulated as flows sharing the
resources max-min fair, three orders (longest alone, most of the binding
resource, largest), the number of tables at once (all of them at once
finishes no table early, so every index build waits for the end), and
the fewest workers within 2% of the most allowed - never later than the
per-choice greedy on the same model but for those fewer workers. On 200
simulated moves: 8.7% sooner on average, 18.4% at the tenth percentile,
32% at best, within 3.6% of the fluid bound at the median; 5 moves where
the greedy overspent the hop's money kept within it. **CP-SAT measured
and not taken**: on 40 of those moves with 5 s each, its ways and order -
fixed rates per mode - simulated on the same flows with the same choice
of tables at once ended 2.3% later on average (0.4% at the median, 3.2%
sooner at best), took 5 s against milliseconds, and the builds that
install beside migkit's cloud libraries pin an older protobuf (only 9.10
resolves with protobuf 7).

## 4. Online regression with uncertainty

* **Bayesian linear regression** with a normal-inverse-gamma prior is
  conjugate: the posterior is updated from sufficient statistics (X'X,
  X'y, y'y, n), and the predictive is a Student t whose spread grows with
  the distance from the data seen (Bishop, PRML ch. 3; Murphy, "Conjugate
  Bayesian analysis of the Gaussian distribution", 2007).
* **Recursive least squares with a forgetting factor** (and its Kalman
  filter reading) tracks coefficients that drift by discounting old data.
* **Relative errors**: times of units of very different sizes have
  errors in proportion to their size; weighting each unit by its own size
  makes the noise even.

**Taken:** `costmodel.Fit` - the NIG posterior of seven coefficients per
way and engine pair, each unit weighed by its size, the prior from this
hop's earlier runs, other hops here, the resource model or migkit's
benchmarks, and old data halved when a unit leaves its spread (forgetting
on a change, not always). Measured against one number a row (what
`Costs` kept) on 400 choices a scenario: as trained 30.9% regret against
1.3%; 500x larger 161.6% against 7.2%; large values 177.7% against 7.4%;
a 50 ms link 1174.6% against 4.6%; a busy source 71.3% against 5.8%.
Least squares without a prior was 38-437% (too few units for seven
numbers, nothing for what it never saw); the prediction alone 6-10%.
Shape classes ("buckets") were rejected: a bucket per shape starts every
new shape from nothing, where the features carry the shape and every
unit teaches the whole line.

## 5. What the paid migration tools do when they "choose"

From migkit's own research files (`mechanisms-aws-azure-google-snowflake-
2026-09-28.md`, `paid-cloud-products-2026-09-28.md`,
`mechanisms-dts-tidb-vitess-molt-voyager-2026-09-28.md`,
`mechanisms-goldengate-qlik-hvr-ibm-shareplex-2026-09-28.md`):

* **AWS DMS**: the operator picks the instance class (or a DCU range),
  `MaxFullLoadSubTasks` (default 8), `ParallelLoadThreads`, LOB mode
  (limited, which truncates past `LobMaxSize` with only a warning, or
  full); premigration assessments are rule checks. Serverless scales DCUs
  on utilisation thresholds after the fact. Enhanced full load segments
  tables by rules for two target pairs.
* **Alibaba DTS**: an instance class the operator picks; pre-checks are
  rules; parallelism is a setting.
* **Google DMS, Azure DMS**: fixed per-engine methods (native replication
  or dump), the operator's choice of offline or online.
* **GoldenGate, Qlik Replicate, HVR, SharePlex**: parallel apply and
  batching configured by hand per task; Qlik's "optimize" modes are
  switches.
* None costs its ways per table from measurement, none predicts a way it
  never ran, none plans the tables jointly under a deadline, money or a
  source-load cap, and none says the runner-up and why not.

**Taken:** nothing of how they choose - they choose by static rules and
operator knobs. What migkit does instead is the four sections above; the
operator sets limits (`deadline:`, `limits:`) and never a knob.

## Decisions

| Asked | Built | Why this and not the fancier |
|---|---|---|
| cost as a function of the unit | Bayesian linear regression, 7 features, relative errors | beats one number by 20-250x regret, and least squares by 5-90x; a neural model would need thousands of runs this never has |
| predictions where nothing is measured | prior from this hop's runs > other hops > resource model (spread 100%) > benchmark (50%) | P0-DEPTH: the resource model named the wrong resource 4 of 11 times - a prior, not the truth |
| safe exploration | elimination by p10, small units, never critical, 5% conservative budget | 60.6% -> 5.7% regret; Thompson sampling gives no reason in words |
| one joint plan | local search on the fluid bound + flow simulation + 3 orders + tables at once + fewest workers | 8.7% sooner than the greedy, within 3.6% of the lower bound; CP-SAT not better, 300x slower, a protobuf pin |
| re-planning | a unit outside its spread halves what was learned and re-climbs; a cut limit slows the ways that load it | POP's validity ranges and the pace's own limits: no second controller |
| risk | Beta(1, 19) per way, pair, version, service; expected time with the work a failure throws away | a way's history, not a guess |
| guarantees | a product of chains, the weakest part, lifts to a fixed point, lexicographic strongest | never traded: speed only among strategies reaching it |
| every way | depth-first over every combination, bounded by the best reachable guarantees; dominated by p10 over p90 | no cap by guessing; counts reported |
