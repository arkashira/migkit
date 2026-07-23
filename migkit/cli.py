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
    with (hop.report_dir() / "changelog.jsonl").open("a") as f:
        f.write(json.dumps({"at": time.strftime("%F %T"), **entry},
                           default=str) + "\n")
    if eng is not None and hasattr(eng, "record_ledger") and entry.get("db"):
        eng.record_ledger(entry["db"], entry)


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
def main():
    """Database migration toolkit: prepare the target, tell you when to run
    the mover (managed migration service or native tools), watch the load, validate everything,
    repair what the mover cannot carry.

    \b
    engines: postgres mysql mssql mongodb redis kafka
             generic (anything reladiff speaks: snowflake, bigquery,
             redshift, clickhouse, oracle, trino, duckdb, ...)

    \b
    typical flow:
      migkit doctor                 tools + connectivity
      migkit advise HOP             playbook for the hop's mover
      migkit setup-target HOP       target schema commands (dry-run)
      migkit check HOP              read-only, layered, exit 1 on diff
      migkit watch HOP              live progress while the mover runs
      migkit sync HOP --go          check + repair with state checkpoints
      migkit rollback HOP --db X    restore any saved state

    every check is read-only and rerunnable. every repair is dry-run
    unless --apply/--go, saves undo first, and converges to the same
    end state on re-run (safe to repeat)."""


@main.command()
def hops():
    """List configured hops."""
    t = Table("hop", "engine", "source", "target", "service", "dbs")
    for name, hop in load_hops().items():
        t.add_row(name, hop.engine, hop.source.host or "-",
                  hop.target.host or "(not set)", hop.service or "-",
                  ",".join(map(str, hop.databases)) or "auto")
    console.print(t)


@main.command()
def doctor():
    """Check local tools and hop connectivity."""
    tools = ["psql", "pg_dump", "pg_restore", "mysqldump", "mysql", "sqlcmd",
             "mongodump", "reladiff", "migra", "liquibase", "pt-table-sync"]
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
                console.print(f"{name} {side}: [green]ok[/green] ({ep.host}, {note})")
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
        console.print(f"[{c}]{it['level']:5s}[/{c}] {it['scope']:20s}"
                      f" {it['item']}  {it['detail']}")
    (hop.report_dir() / "assess.json").write_text(
        json.dumps(items, indent=1))
    console.print(f"\n{counts['pass']} pass, {counts['warn']} warn,"
                  f" {counts['fail']} fail")
    if counts["fail"]:
        raise SystemExit(1)


@main.command("setup-target")
@click.argument("hop_name")
@click.option("--db", default="", help="single database")
def setup_target(hop_name, db):
    """Print the commands that prepare the target before the mover runs."""
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


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="single database")
@click.option("--table", default="", help="single table (schema.table)")
@click.option("--only", default="", help="comma list: schema,counts,autoinc,data")
@click.option("--workers", default=0, help="parallel databases, default from conf")
@click.option("--resume", is_flag=True, help="skip checks already ok in last summary")
def check(hop_name, db, table, only, workers, resume):
    """Read-only validation of target vs source. Never writes to either side."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    checks = [c for c in (only.split(",") if only else eng.checks) if c in eng.checks]
    dbs = [db] if db else eng.databases()
    workers = workers or hop.workers
    summary_path = hop.report_dir() / "summary.json"
    prev = {}
    if resume and summary_path.exists():
        prev = {(r["check"], r["scope"]): r["status"]
                for r in json.loads(summary_path.read_text())}

    timer = Timer()
    results = []
    total_jobs = len(dbs) * len(checks)
    done_jobs = 0

    def run_db(d):
        out = []
        for c in checks:
            if prev.get((c, d)) == "ok":
                out.append({"check": c, "scope": d, "status": "ok",
                            "detail": "resume: skipped, was ok"})
                continue
            fn = getattr(eng, f"check_{c}")
            try:
                if c == "data":
                    rs = [r.__dict__ for r in
                          fn(d, table or None,
                             stream=lambda l, d=d: console.print(f"  [{d}] {l}"))]
                else:
                    rs = [r.__dict__ for r in fn(d)]
            except Exception as e:
                rs = [{"check": c, "scope": d, "status": "error", "detail": str(e)}]
            out.extend(rs)
        return out

    console.print(f"checking {hop_name}: {len(dbs)} dbs x {checks},"
                  f" {workers} workers")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(run_db, d): d for d in dbs}
        for fut in as_completed(futs):
            d = futs[fut]
            rs = fut.result()
            results.extend(rs)
            done_jobs += len(checks)
            for r in rs:
                color = {"ok": "green", "diff": "yellow",
                         "error": "red"}.get(r["status"], "white")
                console.print(f"[{color}]{r['check']:8s} {r['scope']}:"
                              f" {r['status'].upper()}[/{color}] {r.get('detail', '')}")
            console.print(f"  progress {done_jobs}/{total_jobs},"
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
            console.print(f"  migkit repair {hop_name} --apply"
                          f"          # all diffed dbs at once")
        if schema_dbs:
            console.print("  # schema diffs -> generate reviewable DDL files,"
                          " then apply them yourself")
            console.print(f"  migkit gen-migration {hop_name}"
                          f"   # writes migrations/V*.sql per db")
        raise SystemExit(1)
    console.print(f"[green]all green ({len(results)} checks,"
                  f" {human_secs(timer.elapsed())})[/green]")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="", help="one database, or all diffed dbs if omitted")
@click.option("--kind", default="all",
              type=click.Choice(["sequences", "rows", "schema", "all"]))
@click.option("--apply", "do_apply", is_flag=True,
              help="actually execute, default is dry-run")
def repair(hop_name, db, kind, do_apply):
    """Make target equal to source. Dry-run by default, --apply to execute.

    With no --db, repairs every database that had a diff in the last check,
    so you never copy-paste one command per table."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if db:
        dbs = [db]
    else:
        summary = hop.report_dir() / "summary.json"
        if not summary.exists():
            raise SystemExit("run migkit check first, then repair uses its diffs")
        dbs = sorted({r["scope"].split()[0].split(".")[0]
                      for r in json.loads(summary.read_text())
                      if r["status"] == "diff" and r["check"] in
                      ("counts", "autoinc", "data")})
        if not dbs:
            console.print("nothing to repair (no data/sequence diffs last check)")
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


@main.command()
@click.argument("hop_name", required=False)
@click.option("--open", "do_open", is_flag=True, help="open in browser")
@click.option("--refresh", is_flag=True,
              help="re-derive summary from evidence on disk (no re-check):"
                   " settled checksum flickers become OK")
def report(hop_name, do_open, refresh):
    """Render the HTML report from the last check run."""
    import subprocess

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
@click.option("--go", is_flag=True,
              help="repair while checking, default reports what it would do")
@click.option("--tag", default="", help="label this state, e.g. pre-cutover")
def sync(hop_name, db, go, tag):
    """Check and repair in one pass, checkpointing target state for rollback."""
    import shutil
    from pathlib import Path

    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "snapshot_state"):
        raise SystemExit(f"sync mode not available for {hop.engine} yet,"
                         " use check + repair")
    dbs = [db] if db else eng.databases()
    ts = time.strftime("%Y%m%d-%H%M%S") + (f"-{tag}" if tag else "")
    backup_root = Path.home() / ".migkit-state" / hop.name
    for d in dbs:
        console.print(f"[bold]{d}[/bold]")
        state = hop.report_dir(d) / "state" / ts
        state.mkdir(parents=True, exist_ok=True)
        eng.snapshot_state(d, state)
        journal = state / "journal.jsonl"

        def log(entry):
            with journal.open("a") as f:
                f.write(json.dumps({"ts": time.strftime("%H:%M:%S"), **entry},
                                   default=str) + "\n")

        log({"db": d, "event": "snapshot"})
        r = eng.check_autoinc(d)[0]
        console.print(f"  autoinc: {r.status}")
        if r.status == "diff":
            for a in eng.repair_plan(d, "sequences"):
                if go:
                    (state / "undo-sequences.sql").write_text(
                        "\n".join(a.undo) + "\n")
                    lk = _lock(hop)
                    try:
                        eng.apply(d, a)
                    finally:
                        lk.unlink()
                    _changelog(hop, {"op": "sync", "db": d,
                                     "kind": "sequences", "state": ts})
                    again = eng.check_autoinc(d)[0].status
                    log({"event": "repair-sequences", "n": len(a.statements),
                         "recheck": again})
                    console.print(f"  autoinc: repaired {len(a.statements)},"
                                  f" re-check {again}")
                else:
                    console.print(f"  would repair {len(a.statements)}"
                                  " sequences (--go)")
        r = eng.check_data(d)[0]
        console.print(f"  data: {r.status} {r.detail}")
        log({"event": "check-data", "status": r.status, "detail": r.detail})
        if r.status == "diff":
            if go:
                for a in eng.repair_plan(d, "rows"):
                    lk = _lock(hop)
                    try:
                        eng.apply(d, a)
                    finally:
                        lk.unlink()
                    _changelog(hop, {"op": "sync", "db": d, "kind": "rows",
                                     "state": ts, "note": a.note})
                    log({"event": "repair-rows", "note": a.note})
                again = eng.check_data(d)[0]
                console.print(f"  data re-check: {again.status} {again.detail}")
                log({"event": "recheck-data", "status": again.status})
            else:
                console.print("  would delete+recopy differing pks from source"
                              " (--go), target rows saved to undo first")
        backup_root.mkdir(parents=True, exist_ok=True)
        shutil.make_archive(str(backup_root / f"{d}-{ts}"), "gztar", state)
    console.print(f"\nstate: reports/{hop_name}/<db>/state/{ts}"
                  f" + backup {backup_root}")
    if not go:
        console.print("dry-run, add --go to repair while checking")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--table", default="", help="schema.table")
@click.option("--chunk", default=500000, help="rows per resumable chunk")
@click.option("--go", is_flag=True, help="actually copy, default shows plan")
def move(hop_name, db, table, chunk, go):
    """Resumable full load, chunk by chunk with a checkpoint file.

    Use only over a trusted network (or run migkit on a cloud VM).
    Every chunk is delete+copy in one transaction, so a crash at any
    point is safe: rerun and it continues from the checkpoint."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
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
                mark = "done" if st.get("done") else                     f"resume at {st.get('last'):,}" if "last" in st else "todo"
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


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--no-copy-data", is_flag=True,
              help="start CDC only, use after migkit move")
@click.option("--drop", "do_drop", is_flag=True, help="tear down replication")
@click.option("--go", is_flag=True, help="execute, default prints the SQL")
def replicate(hop_name, db, no_copy_data, do_drop, go):
    """Native postgres CDC: publication on source, subscription on target.

    Zero-downtime replication powered by postgres itself. Requires
    wal_level=logical on source (migkit assess checks it) and network
    from target to source. Resume is built into postgres: the slot
    keeps WAL until the subscriber catches up."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "replicate_sql"):
        raise SystemExit(f"replicate not available for {hop.engine},"
                         " use migkit tail (mongo) or the advise playbook")
    dbs = [db] if db else eng.databases()
    for d in dbs:
        sql = eng.replicate_sql(d, copy_data=not no_copy_data)
        console.print(f"[bold]{d}[/bold]")
        if do_drop:
            plan = [("dst", x) for x in sql["drop_dst"]] +                    [("src", x) for x in sql["drop_src"]]
        else:
            plan = [("src", x) for x in sql["src"]] +                    [("dst", x) for x in sql["dst"]]
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


@main.command()
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--go", is_flag=True, help="apply changes, default counts only")
def tail(hop_name, db, go):
    """Mongo CDC via change streams with a persisted resume token.

    Crash-safe by design: the token is saved every batch, rerun
    continues exactly where it stopped. Stop with ctrl-c."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "tail_apply"):
        raise SystemExit("tail is mongodb-only, use replicate for postgres")
    eng.tail_apply(db, go, hop.report_dir(db) / "tail-token.json",
                   lambda m: console.print(m))


@main.command("convert-schema")
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--apply", "do_apply", is_flag=True,
              help="execute the DDL on target, default prints it")
def convert_schema(hop_name, db, do_apply):
    """Cross-engine DDL conversion (hetero hops): transpile source
    schema to the target dialect, review, then --apply."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "convert_ddl"):
        raise SystemExit("convert-schema is for hetero hops,"
                         " same-engine hops use setup-target")
    for d in ([db] if db else eng.databases()):
        stmts = eng.convert_ddl(d)
        out = hop.report_dir(d) / "converted-schema.sql"
        out.write_text("\n\n".join(stmts) + "\n")
        console.print(f"[bold]{d}[/bold]: {len(stmts)} statements -> {out}")
        for stmt in stmts[:3]:
            console.print(f"  {stmt.splitlines()[0]} ...")
        if do_apply:
            for stmt in stmts:
                eng.pg._psql("dst", d, stmt)
            _changelog(hop, {"op": "convert-schema", "db": d,
                             "n": len(stmts)})
            console.print(f"  [green]applied on target[/green]")
    if not do_apply:
        console.print("\ndry-run, review the file then add --apply")


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
def history(hop_name, db):
    """Audit trail of every write migkit made, read from the target
    database itself (migkit_changelog table, like DATABASECHANGELOG)
    plus the local changelog. Survives losing this machine."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    dbs = [db] if db else (eng.databases() if hasattr(eng, "databases")
                           else [])
    shown = False
    for d in dbs:
        if not hasattr(eng, "read_ledger"):
            break
        rows = eng.read_ledger(d)
        if not rows:
            continue
        shown = True
        t = Table("ran_at", "author", "op", "scope", "detail",
                  title=f"{d} (in-database ledger)")
        for r in rows[-30:]:
            t.add_row(*[c[:50] for c in r])
        console.print(t)
    cl = hop.report_dir() / "changelog.jsonl"
    if cl.exists():
        console.print("\nlocal changelog (last 15):")
        for line in cl.read_text().splitlines()[-15:]:
            console.print(f"  {line}")
    elif not shown:
        console.print("no history yet, nothing has been applied")


@main.command("gen-migration")
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--out", default="migrations", help="output directory")
def gen_migration(hop_name, db, out):
    """Generate Flyway-style versioned files from the live schema diff:
    V<ts>__*.sql makes target match source, U<ts>__*.sql undoes it."""
    from pathlib import Path

    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "migration_pair"):
        raise SystemExit("gen-migration supports postgres and mysql hops")
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


@main.command("sample-diff")
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--table", required=True, help="schema.table or table")
@click.option("--limit", default=1000)
def sample_diff(hop_name, db, table, limit):
    """Column-level diff report on a row sample via datacompy:
    which columns differ, match rates, example mismatched values."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
    if not hasattr(eng, "fetch_sample_df"):
        raise SystemExit("sample-diff supports postgres and mysql hops")
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
@click.option("--port", default=8899)
def ui(port):
    """Local web dashboard: every hop's status, tiles, per-db state and
    reports on one auto-refreshing page. Read-only, binds localhost."""
    from .ui import serve
    serve(port)


@main.command("state")
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--show", "show_ts", default="", help="print one state in detail")
def state_cmd(hop_name, db, show_ts):
    """List saved rollback states, tags and the change log."""
    hop = get_hop(hop_name)
    root = hop.report_dir()
    t = Table("state", "db", "captured", "journal")
    found = []
    for sdir in sorted(root.glob("*/state/*")):
        d = sdir.parent.parent.name
        if db and d != db:
            continue
        files = ", ".join(sorted(f.name for f in sdir.iterdir()
                                 if f.name != "journal.jsonl"))
        jl = sdir / "journal.jsonl"
        nj = sum(1 for _ in jl.open()) if jl.exists() else 0
        found.append(sdir)
        t.add_row(sdir.name, d, files, str(nj))
    console.print(t)
    if show_ts:
        for sdir in found:
            if show_ts in sdir.name:
                console.print(f"\n[bold]{sdir}[/bold]")
                jl = sdir / "journal.jsonl"
                if jl.exists():
                    console.print(jl.read_text().strip())
    cl = root / "changelog.jsonl"
    if cl.exists():
        console.print("\nchangelog (last 10):")
        for line in cl.read_text().splitlines()[-10:]:
            console.print(f"  {line}")


@main.command()
@click.argument("hop_name")
@click.option("--db", required=True)
@click.option("--state", "state_ts", default="",
              help="state timestamp or tag, default latest")
@click.option("--apply", "do_apply", is_flag=True)
def rollback(hop_name, db, state_ts, do_apply):
    """Restore target sequences from a saved state, show row-level undo steps."""
    hop = get_hop(hop_name)
    eng = get_engine(hop)
    root = hop.report_dir(db) / "state"
    states = sorted(p.name for p in root.glob("*")) if root.exists() else []
    if not states:
        raise SystemExit("no saved states, run migkit sync first")
    matches = [x for x in states if state_ts in x] if state_ts else states
    if not matches:
        raise SystemExit(f"no state matching '{state_ts}',"
                         f" have: {', '.join(states)}")
    ts = matches[-1]
    state = root / ts
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


@main.command()
@click.argument("hop_name")
@click.option("--db", default="")
@click.option("--interval", default=300, help="seconds between cycles")
@click.option("--only", default="counts,autoinc",
              help="checks per cycle, add data for full checksum each cycle")
@click.option("--cycles", default=0, help="stop after N cycles, 0 = forever")
def monitor(hop_name, db, interval, only, cycles):
    """Continuous validation loop while replication runs.

    Same idea as managed validators but engine-agnostic: every cycle it
    re-runs the selected read-only checks, prints one line per database,
    and refreshes report.html. Transient diffs that heal on the next
    cycle are replication lag; diffs that persist are real."""
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
@click.option("--interval", default=30)
@click.option("--count", default=0, help="samples then exit, 0 = forever")
def watch(hop_name, db, interval, count):
    """Watch a running migration load: row counts, rate, ETA, replication state."""
    hop = get_hop(hop_name)
    _require_configured(hop)
    eng = get_engine(hop)
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


if __name__ == "__main__":
    main()
