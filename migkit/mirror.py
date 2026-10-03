"""A Kafka cluster copied through the cluster mirror Kafka ships (backlog
F7.mm2).

The mirror re-produces each message into the same partition with its key,
value, headers and time, as migkit's own copier does, from Kafka's own
Java clients, and keeps two tables on the target as it goes: where each
source offset
landed (its offset syncs) and where each consumer group stood, in the
target's numbers (its checkpoints). migkit tells it everything in a
properties file of its making (`properties`), readable by this user only
and gone when it ends, and does around it what it does not:

* the target's topics are made first by migkit (`_made_like_the_source`:
  the source's partition count and the settings that decide what a topic
  means), and a target with fewer partitions stops the copy;
* nothing is written to the source: the offset syncs are kept on the
  target (`offset-syncs.topic.location`), and no heartbeat is emitted;
* only committed messages are read, where the mirror's Java consumer
  would copy an aborted transaction's as data;
* the copy ends where the source stood when it began: each partition's
  last message then (read, so a transaction's marker is not waited for),
  reached when the mirror's own saved position passes it;
* each partition's position is then written where migkit's copier and
  tail keep theirs (`move.json`), so a tail after it sends nothing twice
  and skips nothing;
* the groups' positions are translated by migkit (`KafkaEngine._place`),
  from the mirror's tables where they point at the very message, by the
  message itself otherwise - the mirror's own translation deliberately
  lands a group earlier than it stood (KAFKA-12468; measured: a group at
  source offset 1000, its checkpoint said 1, and the message was at
  1001) - and put on the target at cutover, clamped
  (`KafkaEngine.group_positions`).

It runs from the Kafka release `doctor --install` puts in place, or from
Kafka's own image where docker is here (`runner`). Which of it and
migkit's copier carries a hop is climbed on the hop's measured rate
(`bulkways`). Measured 2026-09-30, one pair of Kafka 3.8.0 brokers in
docker, 200,000 messages of about 210 bytes in four partitions: migkit's
copier 13.1 s (15,300 a second); the mirror, from its image, 76.6 s start
to end with exactly-once on - about 15,200 a second once it ran, the rest
its start - which is why neither is taken on the list's word.
"""
import json
import re
import struct
import time

from . import tooluse
from .tools import IMAGES
from .tooluse import TOPPED, UNUSED, USED

PROGRAM = "connect-mirror-maker.sh"
#: the image the mirror runs from where the release is not installed
#: (`tools.IMAGES`)
IMAGE = IMAGES[PROGRAM]
#: the cluster names the properties give each side
SOURCE, TARGET = "source", "target"
#: the mirror's own topics, all on the target: its offset syncs, its
#: checkpoints, and the Connect worker's configuration, positions and
#: status
OWN = re.compile(r"^(mm2-(offset-syncs|configs|offsets|status)\.[^.]+"
                 r"\.internal|[^.]+\.checkpoints\.internal)$")
_RUNNER = None


def internal(topic):
    """Whether `topic` is one of the mirror's own on the target, which is
    its bookkeeping and never the application's."""
    return bool(OWN.match(str(topic)))


def runner():
    """How the mirror runs here: "local" (the release's script and a
    Java), "image" (Kafka's image, present in docker), or "" where
    neither is."""
    global _RUNNER
    if _RUNNER is not None:
        return _RUNNER
    from . import runner as programs
    from .tools import _java_home
    from .util import which
    how = ""
    if which(PROGRAM) and _java_home():
        how = "local"
    elif which("docker"):
        try:
            if programs.run(["docker", "image", "inspect", IMAGE],
                            check=False, timeout=20).returncode == 0:
                how = "image"
        except Exception:  # noqa: BLE001 - no docker to ask
            how = ""
    _RUNNER = how
    return how


# ---- what it is told -------------------------------------------------------

def _escaped(value):
    """A value as a properties file reads it back."""
    return str(value).replace("\\", "\\\\").replace("\n", "\\n")


def properties_text(props):
    return "".join(f"{k} = {_escaped(v)}\n" for k, v in props.items())


def _sign_in(ep, prefix):
    """The side's sign-in and TLS, as the Java clients read them."""
    opts = ep.options or {}
    protocol = str(opts.get("security_protocol") or "").upper()
    out = {}
    if not protocol:
        return out
    out[f"{prefix}.security.protocol"] = protocol
    if protocol.startswith("SASL"):
        mech = str(opts.get("sasl_mechanism") or "PLAIN").upper()
        module = ("org.apache.kafka.common.security.plain.PlainLoginModule"
                  if mech == "PLAIN" else
                  "org.apache.kafka.common.security.scram.ScramLoginModule")
        user = json.dumps(ep.user or "")
        password = json.dumps(ep.password or "")
        out[f"{prefix}.sasl.mechanism"] = mech
        out[f"{prefix}.sasl.jaas.config"] = (
            f"{module} required username={user} password={password};")
    if opts.get("ssl_cafile"):
        out[f"{prefix}.ssl.truststore.type"] = "PEM"
        out[f"{prefix}.ssl.truststore.location"] = str(opts["ssl_cafile"])
    return out


def _topics_regex(topics):
    return ",".join(re.escape(t) for t in sorted(topics))


def properties(hop, topics, workers, replicas):
    """{property: value}: the mirror's whole configuration for copying
    `topics` from the hop's source into its target, `workers` tasks, and
    `replicas` copies of every topic it makes.

    No heartbeat, in either direction: measured with the flow back turned
    off and its heartbeats left on, the mirror still ran that direction's
    workers, which made their three topics and a heartbeat topic in the
    source and wrote to them - the default the documentation gives for
    monitoring a flow, and a write to the source all the same."""
    from .engines.kafka import KafkaEngine
    eng = KafkaEngine(hop)
    flow = f"{SOURCE}->{TARGET}"
    out = {"clusters": f"{SOURCE}, {TARGET}"}
    for name, side, ep in ((SOURCE, "src", hop.source),
                           (TARGET, "dst", hop.target)):
        out[f"{name}.bootstrap.servers"] = ",".join(
            eng._connection(side)["bootstrap_servers"])
        out.update(_sign_in(ep, name))
    out.update({
        f"{flow}.enabled": "true",
        f"{TARGET}->{SOURCE}.enabled": "false",
        f"{flow}.topics": _topics_regex(topics),
        f"{flow}.groups": ".*",
        "replication.policy.class":
            "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
        "offset-syncs.topic.location": "target",
        "emit.heartbeats.enabled": "false",
        f"{flow}.emit.heartbeats.enabled": "false",
        f"{TARGET}->{SOURCE}.emit.heartbeats.enabled": "false",
        f"{flow}.emit.checkpoints.enabled": "true",
        f"{flow}.emit.checkpoints.interval.seconds": "5",
        f"{flow}.sync.group.offsets.enabled": "false",
        f"{flow}.sync.topic.acls.enabled": "false",
        f"{flow}.sync.topic.configs.enabled": "true",
        f"{flow}.refresh.topics.enabled": "false",
        f"{flow}.refresh.groups.interval.seconds": "10",
        f"{SOURCE}.consumer.isolation.level": "read_committed",
        "tasks.max": str(max(1, int(workers))),
        "offset.lag.max": "100",
        "offset.flush.interval.ms": "2000",
        "replication.factor": str(replicas),
        "checkpoints.topic.replication.factor": str(replicas),
        "offset-syncs.topic.replication.factor": str(replicas),
        "heartbeats.topic.replication.factor": str(replicas),
        "config.storage.replication.factor": str(replicas),
        "offset.storage.replication.factor": str(replicas),
        "status.storage.replication.factor": str(replicas),
    })
    return out


# ---- the tables it keeps on the target -------------------------------------

def _string(buf, at):
    (n,) = struct.unpack_from(">h", buf, at)
    at += 2
    if n < 0:
        return None, at
    return bytes(buf[at:at + n]).decode("utf-8"), at + n


def offset_sync(key, value):
    """(topic, partition, upstream offset, downstream offset) of one
    record of the offset-syncs topic: its key a topic and a partition, its
    value two offsets, in Kafka's own binary schema."""
    topic, at = _string(key, 0)
    (partition,) = struct.unpack_from(">i", key, at)
    up, down = struct.unpack_from(">qq", value, 0)
    return topic, partition, up, down


def checkpoint(key, value):
    """(group, topic, partition, upstream offset, downstream offset) of one
    record of the checkpoints topic: its value a version, then the two
    offsets and the group's metadata."""
    group, at = _string(key, 0)
    topic, at = _string(key, at)
    (partition,) = struct.unpack_from(">i", key, at)
    up, down = struct.unpack_from(">qq", value, 2)
    return group, topic, partition, up, down


def saved_position(key, value):
    """((topic, partition), upstream offset) of one record of the Connect
    worker's positions topic, or None for another connector's: the
    mirror's copy task saves the source offset of the last message it
    handed on, as JSON."""
    try:
        name, where = json.loads(key)
        got = json.loads(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    if name != "MirrorSourceConnector" or not isinstance(where, dict) \
            or not isinstance(got, dict):
        return None
    return (where.get("topic"), int(where.get("partition"))), \
        int(got.get("offset"))


def _topic_name(kind):
    return {"syncs": f"mm2-offset-syncs.{SOURCE}.internal",
            "checkpoints": f"{SOURCE}.checkpoints.internal",
            "positions": f"mm2-offsets.{SOURCE}.internal"}[kind]


def read_table(eng, kind):
    """Every record of one of the mirror's tables on the target, decoded,
    the latest for each key kept: `syncs` -> {(topic, p): [(up, down),
    ...]} in order, `checkpoints` -> {(group, topic, p): (up, down)},
    `positions` -> {(topic, p): up}. Empty where the target has no such
    topic - the mirror never ran into it."""
    from kafka import TopicPartition
    name = _topic_name(kind)
    consumer = eng._consumer("dst")
    try:
        if name not in consumer.topics():
            return {}
        tps = [TopicPartition(name, p)
               for p in eng._partitions(consumer, name)]
        consumer.assign(tps)
        ends = consumer.end_offsets(tps)
        for tp in tps:
            consumer.seek_to_beginning(tp)
        out = {}
        left = {tp for tp in tps if ends[tp] > 0}
        began = time.monotonic()
        while left and time.monotonic() - began < 60:
            for tp, msgs in consumer.poll(timeout_ms=2000).items():
                for m in msgs:
                    if m.key is None:
                        continue
                    if kind == "syncs" and m.value is not None:
                        t, p, up, down = offset_sync(m.key, m.value)
                        out.setdefault((t, p), []).append((up, down))
                    elif kind == "checkpoints" and m.value is not None:
                        g, t, p, up, down = checkpoint(m.key, m.value)
                        out[(g, t, p)] = (up, down)
                    elif kind == "positions":
                        got = saved_position(m.key, m.value)
                        if got:
                            out[got[0]] = got[1]
                    if m.offset + 1 >= ends[tp]:
                        left.discard(tp)
        return out
    finally:
        consumer.close()


def downstream(syncs, up):
    """The target offset of source offset `up` by the offset syncs, or
    None: the last sync at or before it, and as many messages on from it
    as `up` is - a candidate the caller proves by the message itself,
    since a gap in the source's offsets between the two moves it."""
    best = None
    for s_up, s_down in syncs or ():
        if s_up <= up and (best is None or s_up >= best[0]):
            best = (s_up, s_down)
    if best is None:
        return None
    return best[1] + (up - best[0])


# ---- running it -----------------------------------------------------------

def _last_message(consumer, tp, begin, end):
    """The offset of the last committed message below `end`, or None
    where the partition holds none: read back from the end in growing
    windows, since a transaction's marker takes an offset no reader is
    handed."""
    width = 1
    while True:
        start = max(begin, end - width)
        consumer.assign([tp])
        consumer.seek(tp, start)
        last, seen_to = None, start
        while seen_to < end:
            batch = consumer.poll(timeout_ms=3000)
            if not batch:
                break
            for msgs in batch.values():
                for m in msgs:
                    if m.offset < end:
                        last = m.offset
                    seen_to = max(seen_to, m.offset + 1)
            if consumer.position(tp) >= end:
                break
        if last is not None or start <= begin:
            return last
        width *= 16


def _replicas(eng):
    got = (eng.hop.options or {}).get("replication_factor")
    if got:
        return int(got)
    dst = eng._consumer("dst")
    try:
        return min(3, len(dst._client.cluster.brokers()) or 1)
    finally:
        dst.close()


def _start(how, text, path):
    """The mirror started - its release here, or Kafka's image in a
    container of the run's own name (`runner.start_container`) - with its
    output read as it comes (`runner._Reader`), so a full pipe never holds
    it up: (the `Proc`, the reader). The properties hold passwords: in a
    file of this user's for the release; for the image, handed in through
    the docker client's environment (`-e NAME`: they are more than one
    line, which an environment file cannot hold), written inside the
    container, never on a command line anyone can list or in a file its
    user may not read."""
    import subprocess

    from . import envkeys
    from . import runner as programs
    if how == "local":
        proc = programs.start([PROGRAM, path], stdin=subprocess.DEVNULL)
    else:
        env_name = "MIGKIT_MIRROR_PROPERTIES"
        script = (f'printf "%s" "${env_name}" > /tmp/mirror.properties &&'
                  f" exec /opt/kafka/bin/{PROGRAM} /tmp/mirror.properties")
        proc = programs.start_container(
            IMAGE, ["sh", "-c", script], passthrough={env_name: text},
            network=envkeys.text("MIGKIT_MIRROR_NETWORK"))
    return proc, programs._Reader(proc.popen.stdout, None, False)


def move(hop, db, workers, go, log):
    """Every topic the hop copies, through the cluster mirror, to where
    the source stood when it began; then each partition's position kept
    where migkit's copier keeps it. Raises where it fails; the caller holds
    the result to the source."""
    from kafka import TopicPartition

    from . import runner as programs
    from .engines.kafka import KafkaEngine
    from .util import PrivateFile
    from .wording import Step, phase, progress
    eng = KafkaEngine(hop)
    how = runner()
    step = Step(phase("mirror", workers=workers),
                [PROGRAM if how != "image" else "docker",
                 "<private properties>"])
    if not go:
        return [step, "# dry-run, add --go to execute"]
    if not how:
        raise SystemExit("this bulk path needs a component this machine does"
                         " not have - migkit doctor --install puts it in"
                         " place")
    # what the ladder holds it to (`bulkways`), held again where the way
    # was forced
    why = unfit(hop) or ("the hop asks each message to carry its source"
                         " position, which the cluster mirror cannot write"
                         if (hop.options or {}).get("offset_header")
                         else None)
    if why:
        raise SystemExit(f"{why}. Nothing has been copied")
    topics = [t for _, t in eng.list_move_tables(db)]
    src = eng._consumer("src", isolation_level="read_committed")
    dst = eng._consumer("dst")
    fence, before = {}, {}
    try:
        for t in topics:
            parts = eng._partitions(src, t)
            made = eng._made_like_the_source(t, len(parts))
            if made and log:
                log(made)
            if len(eng._partitions(dst, t)) < len(parts):
                raise SystemExit(
                    f"{t} has fewer partitions on the target than on the"
                    " source: a key's messages would land in another"
                    " partition, out of the order its consumers read them")
            for p in parts:
                tp = TopicPartition(t, p)
                begin = src.beginning_offsets([tp])[tp]
                end = src.end_offsets([tp])[tp]
                fence[(t, p)] = (begin, end,
                                 _last_message(src, tp, begin, end)
                                 if end > begin else None)
                before[(t, p)] = dst.end_offsets([tp])[tp]
    finally:
        src.close()
        dst.close()
    total = sum(e - b for b, e, _ in fence.values())
    props = properties(hop, topics, workers, _replicas(eng))
    text = properties_text(props)
    if log:
        log(step)
    began = time.monotonic()
    with PrivateFile(text, ".properties") as path:
        proc, said = _start(how, text, path)
        with programs.stopping(), proc:
            try:
                saved = _wait(eng, proc, fence, began, total, log, said,
                              progress)
            finally:
                # it never ends of itself: ended here, done or not
                proc.stop(grace=30)
    _keep_positions(eng, db, fence, before, saved)
    return [step]


def _wait(eng, proc, fence, began, total, log, said, progress):
    """The mirror's saved positions once every partition's last message
    at the start has been handed on. Where the mirror ends first, what it
    said is raised as the runner says any program's end
    (`runner.classify`); where no partition moves for the stall limit
    (`stall.seconds()`, `MIGKIT_STALL_SECONDS`), the copy is ended as any
    copy whose link passes nothing is (`stall.Stalled`) - both ways that
    could not run, which the next way or the next run goes on from."""
    from . import runner as programs
    from . import stall
    limit = stall.seconds()
    last_move, last_done = time.monotonic(), -1
    while True:
        if proc.poll() is not None:
            said.thread.join(5)
            raise programs.classify(
                proc.returncode or 1, "".join(said.lines),
                "the cluster mirror stopped before the copy was done")
        saved = read_table(eng, "positions")
        waiting = [k for k, (_, _, last) in fence.items()
                   if last is not None and saved.get(k, -1) < last]
        done = sum(min(e, saved.get(k, b - 1) + 1) - b
                   for k, (b, e, _) in fence.items())
        if log and done != last_done:
            log(progress("topics", max(0, done), total, began,
                         time.monotonic(), unit="messages"))
        if done != last_done:
            last_move, last_done = time.monotonic(), done
        if not waiting:
            return saved
        if time.monotonic() - last_move > limit:
            raise stall.Stalled(
                f"the cluster mirror carried nothing for {limit:.0f}s with"
                f" {len(waiting)} partitions still short of where the"
                " source stood: " + ", ".join(
                    f"{t}[{p}]" for t, p in sorted(waiting)[:6])
                + "; the copy was ended, and the next run goes on from"
                " where it stood")
        time.sleep(2)


def _keep_positions(eng, db, fence, before, saved):
    """Each partition's position written where migkit's copier keeps its
    own (`move.json`, `KafkaEngine._copy_topic`'s shape): the source
    offset after the last message the mirror handed on, and how many it
    wrote on the target - so the copier or the tail after it goes on from
    there and counts nothing twice."""
    from kafka import TopicPartition
    from . import checkpoint
    path = eng.hop.report_dir(db) / "move.json"
    state = checkpoint.read_move(path) or {}
    dst = eng._consumer("dst")
    try:
        for (t, p), (begin, end, _) in fence.items():
            tp = TopicPartition(t, p)
            now = dst.end_offsets([tp])[tp]
            st = state.setdefault(eng.move_key(db, "", t), {})
            st.setdefault("parts", {})[str(p)] = {
                "base": before[(t, p)], "copied": now - before[(t, p)],
                "next": max(begin, saved.get((t, p), begin - 1) + 1)}
            st["done"] = True
    finally:
        dst.close()
    checkpoint.write_move(path, state)


# ---- the ladder's facts ----------------------------------------------------

def unfit(hop):
    """Why the cluster mirror cannot carry this hop, or None."""
    for ep in (hop.source, hop.target):
        mech = str((ep.options or {}).get("sasl_mechanism") or "").upper()
        if mech == "AWS_MSK_IAM":
            return ("a side signs in with the cloud's own credentials, which"
                    " the cluster mirror Kafka ships cannot")
    if all((ep.options or {}).get("schema_registry")
           for ep in (hop.source, hop.target)):
        return ("each side has a schema registry of its own, and the"
                " cluster mirror carries the source's schema ids as they"
                " are, where migkit's copier registers each schema on the"
                " target and writes the target's id")
    return None


def needs():
    from .decide import Need
    return (Need("a hop it can carry", lambda f: f["mirror_unfit"] is None,
                 lambda f: f["mirror_unfit"]),)


def facts(hop):
    return {"mirror_unfit": lambda: unfit(hop)}


# ---- the account of what is used (backlog 0f) ----------------------------

FLOW = (
    "enabled", "topics", "topics.exclude", "groups", "groups.exclude",
    "emit.heartbeats.enabled", "emit.heartbeats.interval.seconds",
    "emit.checkpoints.enabled", "emit.checkpoints.interval.seconds",
    "sync.group.offsets.enabled", "sync.group.offsets.interval.seconds",
    "sync.topic.acls.enabled", "sync.topic.acls.interval.seconds",
    "sync.topic.configs.enabled", "sync.topic.configs.interval.seconds",
    "refresh.topics.enabled", "refresh.topics.interval.seconds",
    "refresh.groups.enabled", "refresh.groups.interval.seconds",
    "config.properties.exclude", "offset.lag.max",
    "replication.factor", "checkpoints.topic.replication.factor",
    "offset-syncs.topic.replication.factor",
    "heartbeats.topic.replication.factor", "offset-syncs.topic.location",
    "replication.policy.class", "replication.policy.separator",
    "topic.filter.class", "group.filter.class", "config.property.filter.class",
    "consumer.poll.timeout.ms", "admin.timeout.ms", "tasks.max",
    "add.source.alias.to.metrics")
WORKER = (
    "clusters", "bootstrap.servers", "security.protocol", "sasl.mechanism",
    "sasl.jaas.config", "ssl.truststore.type", "ssl.truststore.location",
    "ssl.keystore.location", "consumer.isolation.level",
    "producer.compression.type", "exactly.once.source.support",
    "dedicated.mode.enable.internal.rest", "listeners",
    "offset.flush.interval.ms", "config.storage.replication.factor",
    "offset.storage.replication.factor", "status.storage.replication.factor",
    "config.providers")
#: the pinned release's documented surface (3.8), the flow's settings and
#: the worker's, without their cluster or flow prefix
SURFACE = {"flow": frozenset(FLOW), "worker": frozenset(WORKER)}


def entry_of(key):
    """(where, option) of the account a property written by `properties`
    is: a flow's (`source->target.` or none, where the flow's setting is
    given for every flow) or the worker's (a cluster's prefix, or none)."""
    flow = f"{SOURCE}->{TARGET}."
    if key.startswith(flow) or key.startswith(f"{TARGET}->{SOURCE}."):
        return "flow", key.split(".", 1)[1] if "->" in key.split(".")[0] \
            else key
    for name in (SOURCE, TARGET):
        if key.startswith(name + "."):
            return "worker", key[len(name) + 1:]
    return ("flow" if key in FLOW else "worker"), key


def _account():
    o = {}

    def put(where, option, state, why):
        o[(where, option)] = (state, why)
    put("flow", "enabled", USED, "the source into the target only: nothing"
        " flows back")
    put("flow", "topics", USED, "the topics the hop copies, each named"
        " exactly: its exclusions and every cluster's internal topics are"
        " left out by migkit's own list")
    put("flow", "topics.exclude", UNUSED, "the topics are named one by one,"
        " so none needs excluding")
    put("flow", "groups", USED, "every group, for the checkpoints the"
        " groups' positions are proved against")
    put("flow", "groups.exclude", UNUSED, "its default leaves out the"
        " tools' own groups, which migkit does not carry either")
    put("flow", "emit.heartbeats.enabled", USED, "off, both ways and for"
        " every flow: left on, the flow back's heartbeat wrote a topic and"
        " its workers' three topics into the source (measured); migkit"
        " reads how far behind the copy is from each partition")
    put("flow", "emit.heartbeats.interval.seconds", UNUSED, "no heartbeat"
        " is emitted")
    put("flow", "emit.checkpoints.enabled", USED, "the groups' positions in"
        " the target's numbers, which migkit proves by the message before"
        " it uses one")
    put("flow", "emit.checkpoints.interval.seconds", USED, "five seconds,"
        " so a group's latest position is there when the copy ends")
    put("flow", "sync.group.offsets.enabled", TOPPED, "off: migkit puts each"
        " group on the target itself, by the very message and clamped to"
        " what the target holds, where the mirror's translation lands a"
        " group earlier than it stood by design")
    put("flow", "sync.group.offsets.interval.seconds", UNUSED, "the groups"
        " are put on the target by migkit")
    put("flow", "sync.topic.acls.enabled", TOPPED, "off: migkit carries the"
        " access rules by their whole key, where the mirror carries only"
        " literal topic rules and turns ALL into READ")
    put("flow", "sync.topic.acls.interval.seconds", UNUSED, "the access"
        " rules are carried by migkit")
    put("flow", "sync.topic.configs.enabled", USED, "on: the topics' other"
        " settings follow the source's, over what migkit set when it made"
        " them")
    put("flow", "sync.topic.configs.interval.seconds", UNUSED, "its default"
        " is often enough for a copy that ends where the source stood")
    put("flow", "refresh.topics.enabled", USED, "off: the topics are the"
        " ones migkit named and made")
    put("flow", "refresh.topics.interval.seconds", UNUSED, "the topics are"
        " not refreshed")
    put("flow", "refresh.groups.enabled", UNUSED, "left on: a group that"
        " appears during the copy gets its checkpoints too")
    put("flow", "refresh.groups.interval.seconds", USED, "ten seconds, so"
        " a group is seen within a copy's first moments")
    put("flow", "config.properties.exclude", UNUSED, "its default list is"
        " kept; the settings that decide what a topic means are set by"
        " migkit and compared by the check")
    put("flow", "offset.lag.max", USED, "the table of where offsets landed"
        " kept at its default spacing: migkit proves a position between two"
        " of its rows by the message, so a denser table would only double"
        " the writes")
    for f in ("replication.factor", "checkpoints.topic.replication.factor",
              "offset-syncs.topic.replication.factor",
              "heartbeats.topic.replication.factor"):
        put("flow", f, USED, "the hop's replication_factor, else as many as"
            " the target has brokers up to three")
    put("flow", "offset-syncs.topic.location", USED, "the target: migkit"
        " never writes to the source")
    put("flow", "replication.policy.class", USED, "the names kept as they"
        " are, as migkit's copier keeps them")
    put("flow", "replication.policy.separator", UNUSED, "names are kept as"
        " they are, so there is no alias to separate")
    for f in ("topic.filter.class", "group.filter.class",
              "config.property.filter.class"):
        put("flow", f, UNUSED, "the defaults; what is carried is named in"
            " the properties")
    put("flow", "consumer.poll.timeout.ms", UNUSED, "its default: the copy"
        " ends on migkit's reading of the saved positions")
    put("flow", "admin.timeout.ms", UNUSED, "its default: a cluster that"
        " does not answer stops the copy and is said")
    put("flow", "tasks.max", USED, "from migkit's sizing of this machine and"
        " both clusters, never set by hand")
    put("flow", "add.source.alias.to.metrics", UNUSED, "its metrics are not"
        " read: progress is read from its saved positions")
    put("worker", "clusters", USED, "the two sides, named source and"
        " target")
    put("worker", "bootstrap.servers", USED, "each side's brokers, from the"
        " hop's endpoint")
    for f in ("security.protocol", "sasl.mechanism", "sasl.jaas.config",
              "ssl.truststore.type", "ssl.truststore.location"):
        put("worker", f, USED, "each side's sign-in and TLS, from the hop's"
            " endpoint; the file is readable by this user only and removed"
            " when the copy ends")
    put("worker", "ssl.keystore.location", UNUSED, "not yet: a client"
        " certificate for Kafka is not among the hop's options")
    put("worker", "consumer.isolation.level", USED, "read_committed on the"
        " source: its Java consumer would copy an aborted transaction's"
        " messages as data")
    put("worker", "producer.compression.type", UNUSED, "each message is"
        " written as the producer's default batches it; the check compares"
        " messages, not batches")
    put("worker", "exactly.once.source.support", UNUSED, "off: its"
        " transactions leave a marker taking an offset for every batch on"
        " the target (measured: 427 over 200,000 messages in four"
        " partitions), so a partition's offsets no longer count its"
        " messages - and the count is how a message written twice by a"
        " restarted task is found")
    put("worker", "dedicated.mode.enable.internal.rest", UNUSED, "only"
        " exactly-once needs it of a mirror run on its own")
    put("worker", "listeners", UNUSED, "only the internal REST"
        " exactly-once needs listens")
    put("worker", "offset.flush.interval.ms", USED, "two seconds: the saved"
        " positions the copy's end is read from")
    for f in ("config.storage.replication.factor",
              "offset.storage.replication.factor",
              "status.storage.replication.factor"):
        put("worker", f, USED, "as many copies as the topics it makes")
    put("worker", "config.providers", UNUSED, "the passwords are in a file"
        " of this user's only, or handed to the container through its"
        " environment")
    return o


tooluse.register(PROGRAM, options=_account(), declared=SURFACE)
