"""Best-tool movers.

migkit does not compete with the battle-tested bulk movers, it drives
them: pg_dump/pg_restore parallel jobs, mydumper/myloader, pgloader,
mongodump/mongorestore, and generates ready-to-run Debezium Connect
configs for platform-grade CDC. The builtin chunked copy stays the
fallback (and the only mode with per-chunk crash resume).
"""
import subprocess
from urllib.parse import quote

from .util import run, tool_env, which

VIAS = ("auto", "builtin", "pgdump", "mydumper", "pgloader", "mongodump",
        "debezium")


def pick(engine, table=""):
    """--via auto: fastest installed tool for whole-db moves, builtin for
    single tables (chunk resume matters more than raw speed there)."""
    if table:
        return "builtin"
    if engine == "postgres" and which("pg_dump") and which("pg_restore"):
        return "pgdump"
    if engine == "mysql" and which("mydumper") and which("myloader"):
        return "mydumper"
    if engine == "hetero" and which("pgloader"):
        return "pgloader"
    if engine == "mongodb" and which("mongodump") and which("mongorestore"):
        return "mongodump"
    return "builtin"


def supported(engine, via):
    return {"pgdump": engine == "postgres",
            "mydumper": engine == "mysql",
            "pgloader": engine == "hetero",
            "mongodump": engine == "mongodb",
            "debezium": engine in ("postgres", "mysql", "hetero")}.get(via,
                                                                       True)


def _sh(cmd, env=None, log=None):
    if log:
        log("$ " + " ".join(str(c) for c in cmd))
    p = subprocess.run(cmd, env=tool_env(env), text=True,
                       capture_output=True)
    if p.returncode:
        raise RuntimeError((p.stderr or p.stdout)[-500:])
    return p


def pgdump_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    outdir = hop.report_dir(db) / "pgdump"
    trunc = ("select coalesce('truncate table '||string_agg("
             "format('%I.%I', n.nspname, c.relname), ', ')||' cascade', '')"
             " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
             " where c.relkind = 'r'"
             " and n.nspname not in ('pg_catalog','information_schema')"
             " and n.nspname not like 'pg\\_%'"
             " and n.nspname not like '\\_\\_%'"
             " and c.relname not like 'migkit\\_%'")
    steps = [
        f"# truncate all user tables on target {db} (generated from catalog)",
        f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fd"
        f" -j {workers} --data-only -f {outdir}",
        f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {db}"
        f" --data-only --disable-triggers -j {workers} {outdir}",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    p = _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", db, "-X", "-At", "-c", trunc], env_t)
    stmt = p.stdout.strip()
    if stmt:
        _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", db, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", stmt],
            env_t, log)
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    _sh(["pg_dump", "-h", s.host, "-p", str(s.port), "-U", s.user,
         "-d", db, "-Fd", "-j", str(workers), "--data-only",
         "-f", str(outdir)],
        {"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"}, log)
    try:
        _sh(["pg_restore", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", db, "--data-only", "--disable-triggers",
             "-j", str(workers), str(outdir)], env_t, log)
    except RuntimeError as e:
        # newer pg_dump emits SETs older servers reject; pg_restore exits 1
        # on those even when all rows landed - migkit check is the judge
        if "errors ignored on restore" not in str(e):
            raise
        if log:
            log("pg_restore ignored version-mismatch SET statements,"
                " data restored - verify with migkit check")
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def mydumper_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    outdir = hop.report_dir(db) / "mydumper"
    steps = [
        f"mydumper -h {s.host} -P {s.port} -u {s.user} -p *** -B {db}"
        f" -o {outdir} --threads {workers} --no-schemas --trx-consistency-only",
        f"myloader -h {t.host} -P {t.port} -u {t.user} -p *** -B {db}"
        f" -d {outdir} --threads {workers} --purge-mode TRUNCATE",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    _sh(["mydumper", "-h", s.host, "-P", str(s.port), "-u", s.user,
         f"-p{s.password}", "-B", db, "-o", str(outdir),
         "--threads", str(workers), "--no-schemas",
         "--trx-consistency-only"], None, log)
    _sh(["myloader", "-h", t.host, "-P", str(t.port), "-u", t.user,
         f"-p{t.password}", "-B", db, "-d", str(outdir),
         "--threads", str(workers), "--purge-mode", "TRUNCATE"], None, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def pgloader_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    loadfile = hop.report_dir(db) / "pgloader.load"
    body = f"""LOAD DATABASE
  FROM mysql://{s.user}:{quote(s.password, safe='')}@{s.host}:{s.port}/{db}
  INTO postgresql://{t.user}:{quote(t.password, safe='')}@{t.host}:{t.port}/{db}
WITH data only, workers = {workers}, concurrency = {min(workers, 4)},
     on error stop
ALTER SCHEMA '{db}' RENAME TO 'public';
"""
    loadfile.write_text(body)
    loadfile.chmod(0o600)
    steps = [f"pgloader {loadfile}   # data only, mysql -> postgres"]
    if not go:
        return steps + ["# dry-run, review the load file then add --go"]
    _sh(["pgloader", str(loadfile)], None, log)
    return steps


def _mongo_uri(ep):
    auth = (f"{quote(ep.user, safe='')}:{quote(ep.password, safe='')}@"
            if ep.user else "")
    hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
    uri = f"mongodb://{auth}{hosts}/"
    extra = ep.options.get("uri_options", "")
    if extra:
        uri += "?" + extra
    return uri


def mongodump_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    steps = [
        f"mongodump --uri=<src> --db={db} --archive"
        f" | mongorestore --uri=<dst> --archive --drop"
        f" --nsInclude='{db}.*' --numParallelCollections={workers}",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    dump = subprocess.Popen(
        ["mongodump", f"--uri={_mongo_uri(s)}", f"--db={db}", "--archive",
         "--quiet"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=tool_env())
    restore = subprocess.Popen(
        ["mongorestore", f"--uri={_mongo_uri(t)}", "--archive", "--drop",
         f"--nsInclude={db}.*", f"--numParallelCollections={workers}"],
        stdin=dump.stdout, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=tool_env())
    dump.stdout.close()
    _, err_r = restore.communicate()
    _, err_d = dump.communicate()
    if dump.returncode or restore.returncode:
        raise RuntimeError((err_d + err_r).decode()[-500:])
    if log:
        log(f"{db}: mongodump | mongorestore complete")
    return steps


def debezium_codegen(hop, dbs, engine):
    """Platform-grade CDC = Debezium. migkit does not reimplement it, it
    writes the exact Connect configs for this hop: single-broker redpanda
    + Kafka Connect with the Debezium source and JDBC sink, ready to
    docker compose up and register with two curls."""
    out = hop.report_dir() / "debezium"
    out.mkdir(parents=True, exist_ok=True)
    s, t = hop.source, hop.target
    src_is_mysql = engine in ("mysql", "hetero")
    connector_class = ("io.debezium.connector.mysql.MySqlConnector"
                       if src_is_mysql
                       else "io.debezium.connector.postgresql.PostgresConnector")
    name = f"migkit-{hop.name}"
    source = {
        "name": f"{name}-source",
        "config": {
            "connector.class": connector_class,
            "database.hostname": s.host,
            "database.port": str(s.port),
            "database.user": s.user,
            "database.password": s.password,
            "topic.prefix": name,
            "snapshot.mode": "initial",
        },
    }
    if src_is_mysql:
        source["config"].update({
            "database.include.list": ",".join(dbs),
            "database.server.id": "184054",
            "schema.history.internal.kafka.bootstrap.servers":
                "redpanda:9092",
            "schema.history.internal.kafka.topic": f"{name}-history",
        })
    else:
        source["config"].update({
            "database.dbname": dbs[0] if dbs else "postgres",
            "plugin.name": "pgoutput",
            "slot.name": name.replace("-", "_"),
        })
    dst_is_pg = engine in ("postgres", "hetero")
    jdbc = (f"jdbc:postgresql://{t.host}:{t.port}/" if dst_is_pg
            else f"jdbc:mysql://{t.host}:{t.port}/")
    sink = {
        "name": f"{name}-sink",
        "config": {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "topics.regex": f"{name}\\..*",
            "connection.url": jdbc + (dbs[0] if dbs else ""),
            "connection.username": t.user,
            "connection.password": t.password,
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "primary.key.mode": "record_key",
            "schema.evolution": "basic",
        },
    }
    compose = """services:
  redpanda:
    image: redpandadata/redpanda:latest
    command: redpanda start --overprovisioned --smp 1 --memory 1G
      --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092
      --advertise-kafka-addr PLAINTEXT://redpanda:9092
  connect:
    image: quay.io/debezium/connect:3.0
    depends_on: [redpanda]
    ports: ["8083:8083"]
    environment:
      BOOTSTRAP_SERVERS: redpanda:9092
      GROUP_ID: migkit
      CONFIG_STORAGE_TOPIC: migkit_connect_configs
      OFFSET_STORAGE_TOPIC: migkit_connect_offsets
      STATUS_STORAGE_TOPIC: migkit_connect_statuses
"""
    readme = f"""# Debezium CDC for hop {hop.name} (generated by migkit)

1. docker compose up -d
2. curl -s -X POST -H 'Content-Type: application/json' \\
     --data @source-connector.json http://localhost:8083/connectors
3. curl -s -X POST -H 'Content-Type: application/json' \\
     --data @sink-connector.json http://localhost:8083/connectors
4. watch progress:   curl -s localhost:8083/connectors/{name}-source/status
5. verify with:      migkit check {hop.name}
   continuous:       migkit watch {hop.name} --verify --delta

files contain credentials from hops.yaml - keep this directory private
(chmod 700, never commit). teardown: docker compose down -v and drop the
replication slot / binlog user on the source.
"""
    import json as _json
    (out / "docker-compose.yml").write_text(compose)
    (out / "source-connector.json").write_text(
        _json.dumps(source, indent=2) + "\n")
    (out / "sink-connector.json").write_text(
        _json.dumps(sink, indent=2) + "\n")
    (out / "README-debezium.md").write_text(readme)
    for f in out.iterdir():
        f.chmod(0o600)
    out.chmod(0o700)
    return out


def run_via(via, hop, db, workers, go, log):
    fn = {"pgdump": pgdump_move, "mydumper": mydumper_move,
          "pgloader": pgloader_move, "mongodump": mongodump_move}[via]
    tools = {"pgdump": ("pg_dump", "pg_restore"),
             "mydumper": ("mydumper", "myloader"),
             "pgloader": ("pgloader",),
             "mongodump": ("mongodump", "mongorestore")}[via]
    missing = [t for t in tools if not which(t)]
    if missing:
        raise SystemExit(f"--via {via} needs {', '.join(missing)}"
                         " installed (see bootstrap.sh)")
    return fn(hop, db, workers, go, log)
