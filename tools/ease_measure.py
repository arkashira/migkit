"""Measure the address-only workflow against isolated sandbox profiles."""
import argparse
import contextlib
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
QUESTION = re.compile(r"\?(?:\s|$)|\[(?:y/n|yes/no)\]|^(?:password|source address|target address).*:", re.I)
ACTION = re.compile(
    r"^(?:migkit\s|(?:please\s+)?(?:run|edit|set|export|install|create|grant|enable|disable|choose|enter|provide|use|retry|restart)\b)"
    r"|\b(?:you (?:must|need to|should)|needs? (?:to be|installing)|set wherever)\b", re.I)
SAMPLE = [(1, "alpha"), (2, "two words"), (3, "")]


def measure_transcript(transcript, elapsed, operator_config=""):
    lines = [ANSI.sub("", line).strip() for line in transcript.splitlines()]
    return {
        "elapsed_seconds": round(elapsed, 6),
        "operator_config_lines": sum(bool(line.strip()) and not line.lstrip().startswith("#")
                                     for line in operator_config.splitlines()),
        "questions": sum(bool(QUESTION.search(line)) for line in lines),
        "action_messages": sum(bool(ACTION.search(line)) for line in lines),
    }


def verified(steps, verdict, rows):
    return (len(steps) == 3 and all(step["exit"] == 0 for step in steps)
            and verdict.get("status") == "same" and rows == SAMPLE)


def colima_running():
    """Whether the docker sandbox answers - whichever runtime serves it."""
    for probe in (["docker", "info"], ["colima", "status"]):
        try:
            if subprocess.run(probe, capture_output=True, timeout=20).returncode == 0:
                return True
        except (OSError, subprocess.TimeoutExpired):
            continue
    return False


def pairs():
    from tests.servers import PROFILES
    base = [(key, key) for key, p in PROFILES.items()
            if p.roles == ("src", "dst") and not p.tls]
    return base + [("mysql", "pg"), ("pg", "mysql")]


def pair_label(source, target):
    from tests.servers import PROFILES
    def label(key):
        profile = PROFILES[key]
        engine = {"mongo": "MongoDB", "postgres": "PostgreSQL", "mysql": "MySQL"}.get(profile.engine, profile.engine)
        return f"{engine} (profile {profile.offset})"
    return f"{label(source)} to {label(target)}"


def address(server, database):
    scheme = {"mongo": "mongodb"}.get(server.profile.engine, server.profile.engine)
    if scheme == "redis":
        database = "0"
    user = server.user
    user = user or ("ease" if scheme == "mongodb" else "")
    auth = quote(user, safe="") + "@" if user else ""
    options = "?directConnection=true&authSource=admin" if scheme == "mongodb" else ""
    return f"{scheme}://{auth}{server.host}:{server.port}/{database}{options}"


def seed(server, database):
    engine = server.profile.engine
    if engine == "redis":
        with server.client() as client:
            client.mset({f"ease:{k}": v for k, v in SAMPLE})
        return
    if engine == "opensearch":
        with contextlib.closing(server.client()) as client:
            client.indices.create(index=database)
            for k, v in SAMPLE:
                client.index(index=database, id=str(k), body={"value": v}, refresh=True)
        return
    if engine == "kafka":
        from kafka import KafkaProducer
        from kafka.admin import NewTopic
        with contextlib.closing(server.client()) as client:
            client.create_topics([NewTopic(database, 1, 1)])
        producer = KafkaProducer(bootstrap_servers=f"{server.host}:{server.port}")
        try:
            for k, v in SAMPLE:
                producer.send(database, key=str(k).encode(), value=v.encode()).get(timeout=20)
        finally:
            producer.close()
        return
    if engine == "clickhouse":
        with contextlib.closing(server.client()) as client:
            client.command(f"create table {database}.ease (id Int32, value String) engine=MergeTree order by id")
            client.insert(f"{database}.ease", SAMPLE, column_names=["id", "value"])
        return
    if engine == "cassandra":
        server.sql("create table ease (id int primary key, value text)", database)
        for k, v in SAMPLE:
            server.sql(f"insert into ease (id, value) values ({k}, '{v}')", database)
        return
    if server.profile.engine == "mongo":
        with server.client() as client:
            client[database].ease.insert_many([{"_id": k, "value": v} for k, v in SAMPLE])
        return
    server.sql("create table ease (id integer primary key, value varchar(40) not null)", database)
    server.sql("insert into ease values (1, 'alpha'), (2, 'two words'), (3, '')", database)


def read_sample(server, database):
    engine = server.profile.engine
    if engine == "redis":
        with server.client() as client:
            return sorted((int(k.split(":")[1]), client.get(k)) for k in client.scan_iter("ease:*"))
    if engine == "opensearch":
        with contextlib.closing(server.client()) as client:
            hits = client.search(index=database, body={"query": {"match_all": {}}, "size": 100})["hits"]["hits"]
            return sorted((int(h["_id"]), h["_source"]["value"]) for h in hits)
    if engine == "kafka":
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(database, bootstrap_servers=f"{server.host}:{server.port}",
                                 auto_offset_reset="earliest", enable_auto_commit=False,
                                 consumer_timeout_ms=5000)
        try:
            return sorted((int(r.key), r.value.decode()) for r in consumer)
        finally:
            consumer.close()
    if engine == "cassandra":
        return sorted(server.sql("select id, value from ease", database))
    if server.profile.engine == "mongo":
        with server.client() as client:
            return [(r["_id"], r["value"]) for r in client[database].ease.find().sort("_id", 1)]
    return server.sql("select id, value from ease order by id", database)


def run_workflow(source, target, passwords, cwd, timeout):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("MIGKIT_") and k not in ("XDG_CONFIG_HOME", "XDG_STATE_HOME")}
    env.update(MIGKIT_SOURCE=source, MIGKIT_TARGET=target,
               MIGKIT_SOURCE_PASSWORD=passwords[0], MIGKIT_TARGET_PASSWORD=passwords[1],
               MIGKIT_HOP="ease", MIGKIT_CONF=str(cwd / "conf" / "hops.yaml"),
               MIGKIT_REPORTS=str(cwd / "reports"),
               XDG_CONFIG_HOME=str(cwd / "config"), XDG_STATE_HOME=str(cwd / "state"),
               PYTHONPATH=str(ROOT), NO_COLOR="1")
    steps, transcript = [], []
    start = time.monotonic()
    for args in (("init", "--here"), ("move", "ease", "--go"), ("check", "ease")):
        try:
            got = subprocess.run([sys.executable, "-m", "migkit", *args], cwd=cwd,
                                 env=env, input="", text=True, capture_output=True,
                                 timeout=timeout)
            code, output = got.returncode, got.stdout + got.stderr
        except subprocess.TimeoutExpired as exc:
            code = 124
            output = b"\n".join(x for x in (exc.stdout, exc.stderr) if x).decode(errors="replace")
        steps.append({"command": args[0], "exit": code})
        transcript.append(output)
        if code:
            break
    elapsed = time.monotonic() - start
    files = list((cwd / "reports").rglob("verdict.json"))
    verdict = json.loads(files[0].read_text()) if len(files) == 1 else {}
    return steps, verdict, measure_transcript("\n".join(transcript), elapsed)


def measure_pair(source_key, target_key, timeout):
    from tests import servers
    label = pair_label(source_key, target_key)
    with contextlib.ExitStack() as stack:
        source = stack.enter_context(servers.space(source_key, "ease_measure"))
        target = source if target_key == source_key else stack.enter_context(
            servers.space(target_key, "ease_measure"))
        src, dst = source.src, target.dst
        if src.profile.engine == "mongo":
            for server in (src, dst):
                with server.client() as client:
                    client.admin.command("createUser", "ease", pwd=servers.PASSWORD,
                                         roles=["root"])
        seed(src, source.name)
        with tempfile.TemporaryDirectory(prefix="migkit-ease-") as work:
            steps, verdict, metrics = run_workflow(
                address(src, source.name), address(dst, target.name),
                (src.password, dst.password),
                Path(work), timeout)
            rows = []
            if len(steps) == 3 and all(s["exit"] == 0 for s in steps):
                try:
                    rows = read_sample(dst, target.name)
                except Exception:
                    rows = []
            same = verified(steps, verdict, rows)
            return {"pair": label, "status": "verified" if same else "failed",
                    "steps": steps, "verdict": verdict.get("status", "missing"),
                    **metrics, "verified_seconds": metrics["elapsed_seconds"] if same else None}


def write_reports(rows, directory, date):
    directory.mkdir(parents=True, exist_ok=True)
    stem = directory / f"ease-measured-{date}"
    stem.with_suffix(".json").write_text(json.dumps({"date": date, "results": rows}, indent=2) + "\n")
    lines = ["# Address-only ease measurements", "",
             "Run `python tools/ease_measure.py` while the local sandbox is running.", "",
             "Each run starts with empty local configuration and state. Only two addresses and their passwords are supplied to init; no operator configuration is written. Generated configuration is not operator effort. Sandbox creation and seeding are outside the timer.", "",
             "Profiles with distinct source and target roles are measured, including alternate server variants. Single-server and primary/standby profiles do not provide an independent pair. PostgreSQL/MySQL is also measured in both directions.", "",
             "Time covers init, move and the complete check. Verified requires successful commands, a same verdict and exact fixture values at the target. Failed attempts have no verified time. Counts are transcript line counts: questions contain a question mark or prompt; action messages match commands, imperatives or requests for operator action. These are lexical counts, not a semantic claim. Raw output and credentials are never published.", "",
             "| Pair / profile | Status | Verified seconds | Attempt seconds | Operator config lines | Questions | Action messages |",
             "|---|---|---:|---:|---:|---:|---:|"]
    for row in rows:
        values = [row["pair"], row["status"], *(row.get(k) for k in (
            "verified_seconds", "elapsed_seconds", "operator_config_lines", "questions", "action_messages"))]
        lines.append("| " + " | ".join("—" if v is None else str(v) for v in values) + " |")
    stem.with_suffix(".md").write_text("\n".join(lines) + "\n")
    return stem


def harness_rows(lines):
    """The scenario harness's healthy runs (`MIGKIT_MEASURE_OUT`, one JSON
    line a case of tests/test_autonomous_scenarios.py) as rows of this
    report: the same timing - time to a verified move from a hop file of
    two addresses, their credentials and the databases - kept beside the
    address-only runs. A run that did not end whole has no verified time."""
    names = {"postgres": "PostgreSQL", "mysql": "MySQL"}
    rows = []
    for line in lines:
        m = json.loads(line) if line.strip() else {}
        if m.get("scenario") != "healthy":
            continue
        engine = names.get(m.get("engine"), m.get("engine"))
        rows.append({
            "pair": f"{engine} to {engine}, migrate from a minimal hop file",
            "status": "verified" if m.get("completed") else "failed",
            "verified_seconds": m.get("seconds") if m.get("completed") else None,
            "elapsed_seconds": m.get("seconds"),
            "operator_config_lines": None, "questions": m.get("questions"),
            "action_messages": None})
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/research")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--pairs", default="",
                        help="source:target profile keys, comma separated (all by default)")
    parser.add_argument("--measures", type=Path, action="append", default=[],
                        help="the scenario harness's lines of the same run")
    args = parser.parse_args(argv)
    sys.path.insert(0, str(ROOT))
    running = colima_running()
    rows = []
    wanted = {tuple(p.split(":")) for p in args.pairs.split(",") if p}
    for source, target in pairs():
        if wanted and (source, target) not in wanted:
            continue
        if not running:
            rows.append({"pair": pair_label(source, target), "status": "not measured: local sandbox stopped"})
            continue
        try:
            rows.append(measure_pair(source, target, args.timeout))
        except Exception:
            rows.append({"pair": pair_label(source, target), "status": "failed: sandbox setup or evidence unavailable"})
    rows += harness_rows([line for f in args.measures
                          for line in f.read_text().splitlines()])
    stem = write_reports(rows, args.output, datetime.date.today().isoformat())
    print(f"Ease measurements: {stem.name}.md and {stem.name}.json")
    return 0 if all(r["status"] == "verified" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
