import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
from rich.console import Console
from rich.table import Table

from . import advisors
from .config import get_hop, load_hops
from .engines import get_engine
from .util import Timer, human_int, human_secs, which

console = Console(highlight=False)
QUIET = False


def chat(msg):
    if not QUIET:
        console.print(msg)


def _lock(hop):
    import os
    lk = hop.report_dir() / ".lock"
    if lk.exists():
        try:
            pid = int(lk.read_text().split()[0])
            os.kill(pid, 0)
            raise SystemExit(f"another migkit write operation is running"
                             f" (pid {pid}), wait or remove {lk}")
        except (ProcessLookupError, ValueError):
            pass
    lk.write_text(f"{os.getpid()} {time.strftime('%F %T')}")
    return lk


def _changelog(hop, entry, eng=None):
    # audit is local-only: migkit never writes bookkeeping into the
    # destination, so the target stays a faithful copy of the source and
    # schema verification never trips over our own table
    with (hop.report_dir() / "changelog.jsonl").open("a") as f:
        f.write(json.dumps({"at": time.strftime("%F %T"), **entry},
                           default=str) + "\n")


class _Checkpoint(dict):
    def __init__(self, path):
        super().__init__()
        self.path = path
        if path.exists():
            self.update(json.loads(path.read_text()))

    def save(self):
        self.path.write_text(json.dumps(self, indent=1))


def _require_configured(hop):
    missing = [side for side, ep in (("source", hop.source),
                                     ("target", hop.target))
               if not ep.configured()]
    if missing:
        raise SystemExit(
            f"{hop.name}: {' and '.join(missing)} not configured yet\n"
            f"fill host/user/password in conf/hops.yaml, then: migkit doctor")


@click.group()
@click.option("-q", "--quiet", is_flag=True,
              help="suppress per-table chatter and progress lines,"
                   " keep diffs, errors and summaries")
def main(quiet):
    """Database migration toolkit: prepare the target, tell you when to run
    the mover (managed migration service or native tools), watch the load,
    validate everything, repair what the mover cannot carry.

    \b
    engines: postgres mysql mssql mongodb redis kafka
             generic (anything reladiff speaks: snowflake, bigquery,
             redshift, clickhouse, oracle, trino, duckdb, ...)

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


def _hops_table():
    t = Table("hop", "engine", "source", "target", "service", "dbs")
    for name, hop in load_hops().items():
        t.add_row(name, hop.engine, hop.source.host or "-",
                  hop.target.host or "(not set)", hop.service or "-",
                  ",".join(map(str, hop.databases)) or "auto")
    console.print(t)


@main.command()
def doctor():
    """Configured hops, local tools, and connectivity."""
    _hops_table()
    tools = ["psql", "pg_dump", "pg_restore", "mysqldump", "mysql", "sqlcmd",
             "mongodump", "mongorestore", "mydumper", "myloader", "pgloader",
             "reladiff", "migra", "liquibase", "atlas", "pt-table-sync"]
    t = Table("tool", "status")
    for name in tools:
        path = which(name)
        t.add_row(name, path or "[red]missing[/red]")
    console.print(t)
    for name, hop in load_hops().items():
        for side, ep in (("src", hop.source), ("dst", hop.target)):
            if not ep.configured():
                console.print(f"{name} {side}: not configured")
                continue
            try:
                eng = get_engine(hop)
                dbs = eng.databases() if side == "src" else None
                note = f"{len(dbs)} dbs" if dbs else "reachable"
                role = ""
                if hasattr(eng, "_in_recovery"):
                    try:
                        role = (" [red]READER/read-only[/red]"
                                if eng._in_recovery(side, "postgres")
                                else " [dim]writer[/dim]")
                    except Exception:
                        role = ""
                console.print(f"{name} {side}: [green]ok[/green]"
                              f" ({ep.host}, {note}){role}")
            except Exception as e:
                console.print(f"{name} {side}: [red]FAIL[/red] {e}")


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
    items = eng.assess()
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
    console.print(f"\n{counts['pass']} pass, {counts['warn']} warn,"
                  f" {counts['fail']} fail")
    if counts["fail"]:
        raise SystemExit(1)


def _setup_target(hop_name, db):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    dbs = [db] if db else eng.databases()
    console.print(f"target preparation for {hop_name}"
                  f" (dry-run, run these yourself in order):\n")
    for d in dbs:
        console.print(f"[bold]{d}[/bold]")
        for step in eng.setup_target_plan(d):
            console.print(f"  {step}")
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
    for d in ([db] if db else eng.databases()):
        stmts = eng.convert_ddl(d)
        out = hop.report_dir(d) / "converted-schema.sql"
        out.write_text("\n\n".join(stmts) + "\n")
        console.print(f"[bold]{d}[/bold]: {len(stmts)} statements -> {out}")
        for stmt in stmts[:3]:
            chat(f"  {stmt.splitlines()[0]} ...")
        if do_apply:
            for stmt in stmts:
                eng.pg._psql("dst", d, stmt)
            _changelog(hop, {"op": "convert-schema", "db": d,
                             "n": len(stmts)})
            console.print(f"  [green]applied on target[/green]")
    if not do_apply:
        console.print("\ndry-run, review the file then add --apply")


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
            raise SystemExit("atlas not found, run bootstrap.sh")
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
              help="generate Flyway-style V/U files from the live schema diff")
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
        import datacompy
    except ImportError:
        raise SystemExit("pip install datacompy")
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
    cmp = datacompy.PandasCompare(a, b,
                                  join_columns=[k.lower() for k in keys],
                                  df1_name="source", df2_name="target")
    console.print(cmp.report())


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="single database")
@click.option("--table", default="", help="single table (schema.table)")
@click.option("--only", default="",
              help="comma list: schema,counts,autoinc,data,deep")
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
def check(hop_name, db, table, only, do_deep, drill, limit, consistent,
          workers, resume):
    """Read-only validation of target vs source. Never writes to either side.

    When counts and data both run, row counts ride along with the checksum
    query, so nothing is scanned twice. Suspect rows are proven in-flight
    or real via the replication fence (LSN-based) instead of guesswork."""
    if drill:
        return _drill(hop_name, db, table, limit)
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    allowed = list(eng.checks) + ["deep"]
    checks = [c for c in (only.split(",") if only else list(eng.checks))
              if c in allowed]
    if do_deep and "deep" not in checks:
        checks.append("deep")
    dbs = [db] if db else eng.databases()
    workers = workers or hop.workers
    summary_path = hop.report_dir() / "summary.json"
    prev = {}
    if resume and summary_path.exists():
        prev = {(r["check"], r["scope"]): r["status"]
                for r in json.loads(summary_path.read_text())}

    merge = ("counts" in checks and "data" in checks and not table
             and getattr(eng, "counts_from_data", False))
    local_map = {}
    for d in dbs:
        local = list(checks)
        if merge and prev.get(("data", d)) != "ok":
            local.remove("counts")
        local_map[d] = local

    timer = Timer()
    results = []
    total_jobs = sum(len(v) for v in local_map.values())
    done_jobs = 0

    def _stream(d):
        def s(line):
            if QUIET and "DIFF" not in line and "ERROR" not in line:
                return
            console.print(f"  [{d}] {line}")
        return s

    def run_db(d):
        out = []
        for c in local_map[d]:
            if prev.get((c, d)) == "ok":
                out.append({"check": c, "scope": d, "status": "ok",
                            "detail": "resume: skipped, was ok"})
                continue
            fn = getattr(eng, f"check_{c}")
            try:
                if c == "data":
                    kw = {"stream": _stream(d)}
                    if merge and "counts" not in local_map[d]:
                        kw["with_counts"] = True
                    if consistent and hasattr(eng, "_fast_consistent"):
                        kw["consistent"] = True
                    rs = [r.__dict__ for r in fn(d, table or None, **kw)]
                else:
                    rs = [r.__dict__ for r in fn(d)]
            except Exception as e:
                rs = [{"check": c, "scope": d, "status": "error", "detail": str(e)}]
            out.extend(rs)
        return out

    console.print(f"checking {hop_name}: {len(dbs)} dbs x {checks},"
                  f" {workers} workers"
                  + (" (counts merged into the checksum pass)" if merge else ""))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(run_db, d): d for d in dbs}
        for fut in as_completed(futs):
            d = futs[fut]
            rs = fut.result()
            results.extend(rs)
            done_jobs += len(local_map[d])
            for r in rs:
                color = {"ok": "green", "diff": "yellow",
                         "error": "red"}.get(r["status"], "white")
                console.print(f"[{color}]{r['check']:8s} {r['scope']}:"
                              f" {r['status'].upper()}[/{color}] {r.get('detail', '')}")
            chat(f"  progress {done_jobs}/{total_jobs},"
                 f" elapsed {human_secs(timer.elapsed())},"
                 f" eta {timer.eta(done_jobs, total_jobs)}")

    if only and summary_path.exists():
        try:
            prev_all = json.loads(summary_path.read_text())
        except ValueError:
            prev_all = []

        def keep(r):
            if r.get("check") not in checks:
                return True
            return bool(db) and not r.get("scope", "").startswith(db)

        results = [r for r in prev_all if keep(r)] + results
    summary_path.write_text(json.dumps(results, indent=1, default=str))
    from .report import write_report
    report_path = write_report(hop, results)
    bad = [r for r in results if r["status"] not in ("ok", "skip")]
    console.print(f"\nreport: {report_path}")
    if bad:
        console.print(f"[yellow]{len(bad)} problems, summary: {summary_path}[/yellow]")
        data_dbs = sorted({r["scope"].split()[0].split(".")[0] for r in bad
                           if r["check"] in ("counts", "autoinc", "data")})
        schema_dbs = sorted({r["scope"].split()[0].split(".")[0] for r in bad
                             if r["check"] == "schema"})
        console.print("\nnext steps:")
        if data_dbs:
            console.print("  # data / sequence diffs (re-check first: many are"
                          " transient replication lag, not real)")
            console.print(f"  migkit sync {hop_name} --apply"
                          f"            # all diffed dbs at once")
        if schema_dbs:
            console.print("  # schema diffs -> generate reviewable DDL files,"
                          " then apply them yourself")
            console.print(f"  migkit schema {hop_name} --migration"
                          f"   # writes migrations/V*.sql per db")
        raise SystemExit(1)
    console.print(f"[green]all green ({len(results)} checks,"
                  f" {human_secs(timer.elapsed())})[/green]")


def _repair(hop_name, db, kind, do_apply):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if db:
        dbs = [db]
    else:
        summary = hop.report_dir() / "summary.json"
        if not summary.exists():
            raise SystemExit("run migkit check first, then sync uses its diffs")
        want = {"counts", "autoinc", "data"}
        if kind in ("schema", "all"):
            want.add("schema")
        dbs = sorted({r["scope"].split()[0].split(".")[0]
                      for r in json.loads(summary.read_text())
                      if r["status"] == "diff" and r["check"] in want})
        if not dbs:
            console.print("nothing to repair (no matching diffs last check)")
            return
        console.print(f"repairing {len(dbs)} databases with diffs:"
                      f" {', '.join(dbs)}\n")
    for d in dbs:
        _repair_one(hop, eng, d, kind, do_apply)


def _repair_one(hop, eng, db, kind, do_apply):
    actions = eng.repair_plan(db, kind)
    if not actions:
        console.print(f"{db}: nothing to repair")
        return
    undo_dir = hop.report_dir(db) / "undo"
    undo_dir.mkdir(parents=True, exist_ok=True)
    for i, a in enumerate(actions):
        console.print(f"\n[bold]{a.kind} on {a.scope}[/bold]  {a.note}")
        for s in a.statements[:20]:
            console.print(f"  {s}")
        if len(a.statements) > 20:
            console.print(f"  ... {len(a.statements) - 20} more")
        if not do_apply:
            continue
        if a.undo:
            ts = time.strftime("%Y%m%d-%H%M%S")
            undo_file = undo_dir / f"{ts}-{a.kind}-{i}.sql"
            undo_file.write_text("\n".join(a.undo) + "\n")
            console.print(f"  undo saved: {undo_file}")
        lk = _lock(hop)
        try:
            eng.apply(db, a)
        finally:
            lk.unlink()
        _changelog(hop, {"op": "repair", "db": db, "kind": a.kind,
                         "detail": a.note,
                         "undo_ref": str(undo_file) if a.undo else None},
                   eng)
        console.print("  [green]applied[/green]")
    if not do_apply:
        console.print("\ndry-run only, add --apply to execute")
    else:
        console.print("\nre-run migkit check to confirm")


def _sync_go(hop_name, db, tag):
    from .state import get_store

    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "snapshot_state"):
        raise SystemExit(f"--go not available for {hop.engine} yet,"
                         " use migkit sync --apply")
    store = get_store(hop)
    dbs = [db] if db else eng.databases()
    for d in dbs:
        console.print(f"[bold]{d}[/bold]")
        point = store.new_point(d, tag)
        eng.snapshot_state(d, point.dir)
        journal = point.path("journal.jsonl")

        def log(entry):
            with journal.open("a") as f:
                f.write(json.dumps({"ts": time.strftime("%H:%M:%S"), **entry},
                                   default=str) + "\n")

        log({"db": d, "event": "snapshot"})
        r = eng.check_autoinc(d)[0]
        console.print(f"  autoinc: {r.status}")
        if r.status == "diff":
            for a in eng.repair_plan(d, "sequences"):
                point.path("undo-sequences.sql").write_text(
                    "\n".join(a.undo) + "\n")
                lk = _lock(hop)
                try:
                    eng.apply(d, a)
                finally:
                    lk.unlink()
                _changelog(hop, {"op": "sync", "db": d,
                                 "kind": "sequences", "state": point.ts})
                again = eng.check_autoinc(d)[0].status
                log({"event": "repair-sequences", "n": len(a.statements),
                     "recheck": again})
                console.print(f"  autoinc: repaired {len(a.statements)},"
                              f" re-check {again}")
        r = eng.check_data(d)[0]
        console.print(f"  data: {r.status} {r.detail}")
        log({"event": "check-data", "status": r.status, "detail": r.detail})
        if r.status == "diff":
            for a in eng.repair_plan(d, "rows"):
                lk = _lock(hop)
                try:
                    eng.apply(d, a)
                finally:
                    lk.unlink()
                _changelog(hop, {"op": "sync", "db": d, "kind": "rows",
                                 "state": point.ts, "note": a.note})
                log({"event": "repair-rows", "note": a.note})
            again = eng.check_data(d)[0]
            console.print(f"  data re-check: {again.status} {again.detail}")
            log({"event": "recheck-data", "status": again.status})
        point.set_meta(op="sync", db=d)
        ts = point.commit(time.strftime("%F %T"))
        console.print(f"  state saved: {store.kind}:{ts}")
    console.print(f"\nstate backend: {store.kind}"
                  " (migkit history / rollback to use it)")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="one database, or all diffed dbs if omitted")
@click.option("--kind", default="all",
              type=click.Choice(["sequences", "rows", "schema", "all"]))
@click.option("--mode", type=click.Choice(
    ["reconcile", "verify", "seed", "stream", "migrate"]),
    default="reconcile",
    help="run a whole flow: verify (read-only assurance), seed (schema +"
         " initial load + reconcile), stream (follow live changes + delta"
         " verify), migrate (seed then stream). default reconcile = repair"
         " the last check's diffs")
@click.option("--apply", "do_apply", is_flag=True,
              help="execute the repair plan, default is dry-run")
@click.option("--go", is_flag=True,
              help="checkpointed check+repair sweep with rollback state")
@click.option("--serve", is_flag=True,
              help="incremental modes: run the verify loop forever (VM/service)")
@click.option("--interval", default=60, help="--serve: seconds between cycles")
@click.option("--tag", default="", help="with --go: label this state, e.g. pre-cutover")
@click.pass_context
def sync(ctx, hop_name, db, kind, mode, do_apply, go, serve, interval, tag):
    """Make target equal to source, or run a full DMS-style migration.

    Default (reconcile) shows/repairs the last check's diffs; --apply
    executes, --go checkpoints for rollback. --mode runs the whole flow
    end to end with verification wrapped around every step, so migkit can
    stand in for a managed migration service on a trusted VM."""
    if mode != "reconcile":
        return _orchestrate(ctx, hop_name, db, mode, do_apply or go,
                            serve, interval)
    if go:
        return _sync_go(hop_name, db, tag)
    return _repair(hop_name, db, kind, do_apply)


def _run_check(ctx, hop_name, db, only, consistent=False):
    """Invoke check, returning True when green (check exits 1 on diff)."""
    try:
        ctx.invoke(check, hop_name=hop_name, db=db, only=only,
                   consistent=consistent)
        return True
    except SystemExit as e:
        return not e.code


def _orchestrate(ctx, hop_name, db, mode, go, serve, interval):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    tail = "  (dry-run; add --apply/--go to execute)" if not go else ""

    if mode == "verify":
        console.print("[bold]verify[/bold]: read-only, consistent snapshot")
        ok = _run_check(ctx, hop_name, db, "", consistent=True)
        raise SystemExit(0 if ok else 1)

    if mode in ("seed", "migrate"):
        console.print(f"[bold]seed[/bold]{tail}")
        console.print("1/4 schema: align target objects to source")
        _run_check(ctx, hop_name, db, "schema")
        _repair(hop_name, db, "schema", go)
        console.print("2/4 load: bulk copy via the best installed mover")
        if go:
            movers_pick_and_run(hop, eng, db, go)
        else:
            console.print("   would run: migkit move"
                          f" {hop_name} --mode full --go")
        console.print("3/4 reconcile: repair residual rows + sequences")
        _run_check(ctx, hop_name, db, "counts,autoinc,data")
        _repair(hop_name, db, "all", go)
        console.print("4/4 verify: consistent snapshot")
        ok = _run_check(ctx, hop_name, db, "", consistent=True)
        console.print(f"[{'green' if ok else 'yellow'}]seed"
                      f" {'GREEN' if ok else 'has residual diffs'}[/]")
        if mode == "seed":
            raise SystemExit(0 if ok else 1)

    if mode in ("stream", "migrate"):
        console.print(f"[bold]stream[/bold]{tail}")
        from .engines import ALIASES
        engine = ALIASES.get(hop.engine, hop.engine)
        if go:
            if engine == "postgres" and hasattr(eng, "replicate_sql"):
                _replicate(hop, eng, db, True, False, True)
            elif hasattr(eng, "tail_apply") and db:
                _tail(hop, eng, db, True)
            else:
                console.print("   CDC start skipped: use migkit move --mode"
                              " cdc (or --via debezium) for this engine")
        else:
            console.print("   would start CDC: migkit move"
                          f" {hop_name} --mode cdc --go")
        if not hasattr(eng, "delta_verify"):
            console.print("   delta verify not available for this engine")
            return
        n = 0
        while True:
            n += 1
            for d in ([db] if db else eng.databases()):
                try:
                    rs = eng.delta_verify(d)
                    head = rs[0]
                    c = {"ok": "green", "diff": "yellow",
                         "error": "red"}.get(head.status, "white")
                    console.print(f"  cycle {n} {d}:"
                                  f" [{c}]{head.status.upper()}[/] {head.detail}")
                except Exception as e:
                    console.print(f"  cycle {n} {d}: [red]error[/] {e}")
            if not serve:
                break
            time.sleep(interval)


def movers_pick_and_run(hop, eng, db, go):
    from . import movers
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    via = movers.pick(engine)
    for d in ([db] if db else eng.databases()):
        if via == "builtin":
            _move_full(hop, eng, d, "", 500000, go)
        else:
            for line in movers.run_via(via, hop, d, hop.workers, go,
                                       lambda m: chat(f"   {m}")):
                chat(f"   {line}")
            _changelog(hop, {"op": f"move-{via}", "db": d})


def _move_full(hop, eng, db, table, chunk, go):
    if not hasattr(eng, "move_table"):
        raise SystemExit(f"move not available for {hop.engine} yet")
    dbs = [db] if db else eng.databases()
    for d in dbs:
        ck = _Checkpoint(hop.report_dir(d) / "move.json")
        if table:
            tables = [tuple(table.split(".", 1)) if "." in table
                      else ("", table)]
        else:
            tables = eng.list_move_tables(d)
        if not go:
            done = sum(1 for sch, t in tables
                       if ck.get(f"{sch}.{t}", {}).get("done"))
            console.print(f"{d}: {len(tables)} tables, {done} already done"
                          f" in checkpoint, chunk {chunk:,} rows")
            for sch, t in tables[:20]:
                st = ck.get(f"{sch}.{t}", {})
                mark = "done" if st.get("done") else \
                    f"resume at {st.get('last'):,}" if "last" in st else "todo"
                console.print(f"  {sch}.{t}: {mark}")
            continue
        lk = _lock(hop)
        try:
            for sch, t in tables:
                eng.move_table(d, sch, t, chunk, ck,
                               lambda m: console.print(f"  {m}"))
                _changelog(hop, {"op": "move", "db": d, "table": f"{sch}.{t}"})
        finally:
            lk.unlink()
        console.print(f"[green]{d}: move complete[/green],"
                      " run migkit check to verify")
    if not go:
        console.print("\ndry-run, add --go to copy")


def _replicate(hop, eng, db, copy_data, do_drop, go):
    dbs = [db] if db else eng.databases()
    for d in dbs:
        sql = eng.replicate_sql(d, copy_data=copy_data)
        console.print(f"[bold]{d}[/bold]")
        if do_drop:
            plan = [("dst", x) for x in sql["drop_dst"]] + \
                   [("src", x) for x in sql["drop_src"]]
        else:
            plan = [("src", x) for x in sql["src"]] + \
                   [("dst", x) for x in sql["dst"]]
        if sql.get("note"):
            console.print(f"  note: {sql['note']}")
        for side, stmt in plan:
            shown = stmt.replace(hop.source.password, "****")
            console.print(f"  {side}: {shown}")
            if go:
                if hasattr(eng, "_psql"):
                    eng._psql(side, d, stmt)
                else:
                    eng._q(side, stmt)
        if go and not do_drop:
            console.print("  " + eng._psql("dst", d, sql["status"]))
            _changelog(hop, {"op": "replicate", "db": d})
        if go and do_drop:
            _changelog(hop, {"op": "replicate-drop", "db": d})
    if not go:
        console.print("\ndry-run, add --go to execute"
                      " (monitor lag with: migkit watch)")


def _tail(hop, eng, db, go):
    if not db:
        raise SystemExit("cdc tail needs --db")
    eng.tail_apply(db, go, hop.report_dir(db) / "tail-token.json",
                   lambda m: console.print(m))


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--table", default="", help="schema.table (full mode)")
@click.option("--mode", type=click.Choice(["full", "cdc", "full+cdc"]),
              default="full",
              help="full = bulk copy, cdc = follow live changes,"
                   " full+cdc = initial load plus stream until cutover")
@click.option("--via", type=click.Choice(["auto", "builtin", "pgdump",
                                          "mydumper", "pgloader",
                                          "mongodump", "debezium"]),
              default="auto",
              help="which mover does the work: auto = fastest installed"
                   " (pg_dump -j / mydumper / pgloader / mongodump),"
                   " builtin = chunk-resumable copy,"
                   " debezium = generate Connect configs for platform CDC")
@click.option("--chunk", default=500000, help="rows per resumable chunk (builtin)")
@click.option("--drop", "do_drop", is_flag=True,
              help="cdc modes: tear down replication")
@click.option("--go", is_flag=True, help="actually run, default shows the plan")
def move(hop_name, db, table, mode, via, chunk, do_drop, go):
    """One mover for every engine, driving the best installed tool.

    full: pg_dump/pg_restore parallel jobs, mydumper/myloader, pgloader
    or mongodump/mongorestore when installed (--via auto), else the
    builtin chunked copy - the only mode with per-chunk crash resume.
    cdc: native mechanisms (pg logical replication, mysql binlog, mongo
    change streams) or --via debezium for platform-grade Connect configs.

    Use only over a trusted network (or run migkit on a cloud VM)."""
    from . import movers
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    if via == "debezium":
        if mode == "full":
            raise SystemExit("--via debezium is for cdc modes")
        if not movers.supported(engine, via):
            raise SystemExit(f"debezium codegen not built for {hop.engine}")
        dbs = [db] if db else eng.databases()
        out = movers.debezium_codegen(hop, dbs, engine)
        console.print(f"debezium connect configs generated: {out}")
        name = f"migkit-{hop.name}"
        if do_drop:
            if go:
                movers.debezium_down(out, lambda m: console.print(f"  {m}"))
                console.print("[green]debezium stack torn down[/green]")
            else:
                console.print(f"would run: docker compose -f {out}"
                              "/docker-compose.yml down -v")
            return
        if not go:
            console.print("review credentials, then --go to launch"
                          " (or follow README-debezium.md by hand)")
            return
        console.print("launching redpanda + kafka connect ...")
        movers.debezium_up(out, lambda m: chat(f"  {m}"))
        if not movers.debezium_wait(log=lambda m: chat(f"  {m}")):
            raise SystemExit("Kafka Connect did not come up (see docker logs)")
        movers.debezium_register(out, log=lambda m: console.print(f"  {m}"))
        for c in (f"{name}-source", f"{name}-sink"):
            console.print("  " + movers.debezium_status(c))
        _changelog(hop, {"op": "debezium-up", "db": ",".join(dbs)})
        console.print("[green]debezium CDC running[/green] — verify the stream"
                      f" with: migkit sync {hop_name} --mode stream --serve")
        return
    if mode == "full":
        v = movers.pick(engine, table) if via == "auto" else via
        if v != "builtin":
            if not movers.supported(engine, v):
                raise SystemExit(f"--via {v} does not apply to {hop.engine}")
            dbs = [db] if db else eng.databases()
            lk = _lock(hop) if go else None
            try:
                for d in dbs:
                    console.print(f"[bold]{d}[/bold] via {v}:")
                    steps = movers.run_via(v, hop, d, hop.workers, go,
                                           lambda m: chat(f"  {m}"))
                    for s0 in steps:
                        console.print(f"  {s0}")
                    if go:
                        _changelog(hop, {"op": f"move-{v}", "db": d})
                        console.print(f"[green]{d}: {v} move complete[/green],"
                                      " run migkit check to verify")
            finally:
                if lk:
                    lk.unlink()
            return
        return _move_full(hop, eng, db, table, chunk, go)
    has_repl = hasattr(eng, "replicate_sql")
    has_tail = hasattr(eng, "tail_apply")
    if mode == "cdc":
        if engine == "postgres" and has_repl:
            return _replicate(hop, eng, db, False, do_drop, go)
        if has_tail:
            return _tail(hop, eng, db, go)
        if has_repl:
            return _replicate(hop, eng, db, False, do_drop, go)
        raise SystemExit(f"cdc not available for {hop.engine},"
                         " see migkit advise")
    # full+cdc
    if engine == "postgres" and has_repl:
        # a subscription with copy_data=true is the native full+cdc
        return _replicate(hop, eng, db, True, do_drop, go)
    if engine == "mysql" and has_repl:
        console.print("mysql full+cdc: binlog coordinates below are captured"
                      " BEFORE the load, execute the replicate SQL after"
                      " the copy finishes\n")
        _replicate(hop, eng, db, True, False, False)
        return _move_full(hop, eng, db, table, chunk, go)
    if has_tail and hasattr(eng, "move_table"):
        _move_full(hop, eng, db, table, chunk, go)
        if go:
            return _tail(hop, eng, db, go)
        console.print("then: migkit move --mode cdc --db <db> --go"
                      " to stream changes")
        return
    raise SystemExit(f"full+cdc not available for {hop.engine},"
                     " see migkit advise")


def _delta_loop(hop_name, db, interval, cycles, teardown):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "delta_verify"):
        raise SystemExit(f"delta verify not available for {hop.engine} yet"
                         " (postgres, mysql, mongodb)")
    dbs = [db] if db else eng.databases()
    if teardown:
        for d in dbs:
            if hasattr(eng, "delta_teardown"):
                try:
                    eng.delta_teardown(d)
                except Exception as e:
                    console.print(f"{d}: [yellow]{e}[/yellow]")
            for f in ("delta-pos.json", "delta-token.json"):
                p = hop.report_dir(d) / f
                if p.exists():
                    p.unlink()
            console.print(f"{d}: delta state removed")
        return
    n = 0
    while True:
        n += 1
        stamp = time.strftime("%H:%M:%S")
        all_results = []
        for d in dbs:
            try:
                rs = eng.delta_verify(d, log=lambda m, d=d:
                                      chat(f"  [{d}] {m}"))
            except Exception as e:
                console.print(f"{stamp} {d}: [red]delta error[/red] {e}")
                continue
            head = rs[0]
            color = {"ok": "green", "diff": "yellow",
                     "error": "red"}.get(head.status, "white")
            console.print(f"{stamp} cycle {n} {d}:"
                          f" [{color}]{head.status.upper()}[/{color}]"
                          f" {head.detail}")
            for r in rs[1:]:
                if r.status not in ("ok", "skip"):
                    console.print(f"    [yellow]{r.scope}: {r.detail}[/yellow]"
                                  + (f"  fix: {r.fix_hint}" if r.fix_hint
                                     else ""))
            all_results.extend(r.__dict__ for r in rs)
        (hop.report_dir() / "delta-summary.json").write_text(
            json.dumps(all_results, indent=1, default=str))
        if cycles and n >= cycles:
            break
        time.sleep(interval)


def _monitor(hop_name, db, interval, only, cycles):
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    checks = [c for c in only.split(",") if c in eng.checks]
    dbs = [db] if db else eng.databases()
    n = 0
    while True:
        n += 1
        stamp = time.strftime("%H:%M:%S")
        all_results = []
        for d in dbs:
            statuses = []
            for c in checks:
                try:
                    rs = getattr(eng, f"check_{c}")(d)
                except Exception as e:
                    rs = []
                    statuses.append(f"[red]{c}:ERR[/red] {e}")
                for r in rs:
                    all_results.append(r.__dict__)
                bad = [r for r in rs if r.status not in ("ok", "skip")]
                if bad:
                    statuses.append(f"[yellow]{c}:DIFF[/yellow]"
                                    f" {bad[0].detail[:60]}")
                elif rs:
                    statuses.append(f"[green]{c}:ok[/green]")
            console.print(f"{stamp} cycle {n} {d}: " + "  ".join(statuses))
        try:
            from .report import write_report
            write_report(hop, all_results)
        except Exception:
            pass
        if cycles and n >= cycles:
            break
        time.sleep(interval)


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
              help="with --delta: drop the delta slot/position and exit")
def watch(hop_name, db, interval, count, verify, only, delta, teardown):
    """Watch a running migration load: row counts, rate, ETA, replication
    state. --verify turns it into a continuous validation loop: transient
    diffs that heal next cycle are replication lag, persistent ones are
    real. --verify --delta verifies only what changed, so it can run
    forever against billion-row databases."""
    if delta:
        return _delta_loop(hop_name, db, interval or 60, count, teardown)
    if verify:
        return _monitor(hop_name, db, interval or 300, only, count)
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    interval = interval or 30
    dbs = [db] if db else eng.databases()
    last = {}
    n = 0
    while True:
        n += 1
        for d in dbs:
            s = eng.watch_sample(d)
            if "error" in s:
                console.print(f"{d}: [red]{s['error']}[/red]")
                continue
            src, dst = s.get("src_rows", 0), s.get("dst_rows", 0)
            line = f"{d}: src~{human_int(src)} dst~{human_int(dst)}"
            if src:
                line += f" ({dst * 100 // src}%)"
            if d in last and s["ts"] > last[d]["ts"]:
                rate = (dst - last[d]["dst_rows"]) / (s["ts"] - last[d]["ts"])
                if rate > 0:
                    line += (f" rate {human_int(int(rate))}/s"
                             f" eta {human_secs((src - dst) / rate)}")
                elif dst >= src:
                    line += " caught up, check lag before cutover"
            for slot in s.get("replication_slots", []):
                line += f"\n    slot {slot}"
            for conn in s.get("replication_conns", []):
                line += f"\n    conn {conn}"
            console.print(line)
            last[d] = s
        if count and n >= count:
            break
        time.sleep(interval)


@main.command()
@click.argument("hop_name", required=False)
@click.option("--open", "do_open", is_flag=True, help="open in browser")
@click.option("--refresh", is_flag=True,
              help="re-derive summary from evidence on disk (no re-check):"
                   " settled checksum flickers become OK")
@click.option("--serve", is_flag=True,
              help="serve the live dashboard for every hop instead of"
                   " writing a file")
@click.option("--metrics", is_flag=True,
              help="print Prometheus metrics for every hop and exit"
                   " (the dashboard also exposes them at /metrics)")
@click.option("--port", default=8899, help="with --serve: listen port")
def report(hop_name, do_open, refresh, serve, metrics, port):
    """Render the HTML report from the last check run, or --serve the
    live localhost dashboard (with a Prometheus /metrics endpoint)."""
    import subprocess

    if metrics:
        from .ui import prometheus
        print(prometheus(load_hops()), end="")
        return
    if serve:
        from .ui import serve as _serve
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
        from .report import _reclassify
        results = _reclassify(json.loads(summary_path.read_text()))
        summary_path.write_text(json.dumps(results, indent=1, default=str))
        still = sum(1 for r in results if r["status"] not in ("ok", "skip"))
        console.print(f"refreshed from disk: {still} real problems remain"
                      f" (settled flickers cleared)")
    from .report import from_summary
    path, generated = from_summary(hop)
    console.print(f"report ({generated}): {path}")
    if do_open:
        subprocess.run(["open", str(path)])


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
    eng = get_engine(hop) if hop.source.configured() else None
    dbs = [db] if db else (eng.databases() if eng
                           and hasattr(eng, "databases") else [])
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
                    console.print(f"\n[bold]{d}/{m['ts']}[/bold]")
                    console.print(jl.read_text().strip())
    cl = root / "changelog.jsonl"
    if cl.exists():
        console.print("\nlocal changelog (last 15):")
        for line in cl.read_text().splitlines()[-15:]:
            console.print(f"  {line}")
    elif not found:
        console.print("no history yet, nothing has been applied")


@main.command()
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--state", "state_ts", default="",
              help="state timestamp or tag, default latest")
@click.option("--apply", "do_apply", is_flag=True)
def rollback(hop_name, db, state_ts, do_apply):
    """Restore target sequences from a saved state, show row-level undo steps."""
    from .state import get_store
    hop = get_hop(hop_name)
    eng = get_engine(hop)
    store = get_store(hop)
    states = sorted(m.get("ts", "") for m in store.list(db))
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
    if seqf.exists():
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
        console.print(f"  {s}")
    if do_apply and stmts:
        lk = _lock(hop)
        try:
            if seqf.exists():
                eng._psql("dst", db, "\n".join(stmts))
            else:
                from .engines.base import RepairAction
                eng.apply(db, RepairAction(db, "sequences", stmts, [],
                                           "rollback restore"))
        finally:
            lk.unlink()
        _changelog(hop, {"op": "rollback", "db": db, "state": ts,
                         "n": len(stmts)})
        console.print("[green]values restored to snapshot[/green]")
    manifest = None
    if hasattr(eng, "_report"):
        manifest = eng._report(db) / "undo" / "manifest.txt"
    if manifest and manifest.exists():
        console.print("\nrow-level undo (run per manifest line):")
        console.print(manifest.read_text().strip())
    if not do_apply:
        console.print("\ndry-run, add --apply to restore sequences")


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
    _repair(hop_name, db, kind, do_apply)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--no-copy-data", is_flag=True)
@click.option("--drop", "do_drop", is_flag=True)
@click.option("--go", is_flag=True)
def replicate(hop_name, db, no_copy_data, do_drop, go):
    """(alias) Now: migkit move HOP --mode cdc / full+cdc."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "replicate_sql"):
        raise SystemExit(f"replicate not available for {hop.engine},"
                         " use migkit move --mode cdc or the advise playbook")
    _replicate(hop, eng, db, not no_copy_data, do_drop, go)


@main.command(hidden=True)
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--go", is_flag=True)
def tail(hop_name, db, go):
    """(alias) Now: migkit move HOP --mode cdc --db X."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "tail_apply"):
        raise SystemExit("tail is for mongo/hetero hops,"
                         " use migkit move --mode cdc")
    _tail(hop, eng, db, go)


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


if __name__ == "__main__":
    main()
