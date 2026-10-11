import hashlib
import json
import re
import time

from .. import pieces
from .base import Engine, RepairAction, Result


class KafkaEngine(Engine):
    checks = ("schema", "counts", "data")

    # --- the destination of a change stream (backlog 35) -----------------
    # A hop `engine: hetero` with `target_engine: kafka` carries the source's
    # changes into topics, in the shapes and under the hop options
    # `streamout` describes.
    #: topics are made by the first message, and a message carries the
    #: fields it has - a column added on the source is simply in the next
    CREATES_ON_WRITE = True
    EXPRESSES_ABSENT = True

    #: how a cluster is signed in to, as the endpoint's options say it
    SECURITY = ("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
    #: AWS_MSK_IAM signs in to Amazon MSK with the machine's AWS
    #: credentials (the client signs the request itself, over SASL_SSL)
    MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "AWS_MSK_IAM")

    def _connection(self, side):
        """What every client of a side is given: where the cluster is, and
        how to sign in. Endpoint options:
          hosts: [h1:9092, h2:9092]      (else host and port)
          security_protocol: SASL_SSL    (PLAINTEXT, SSL, SASL_PLAINTEXT)
          sasl_mechanism: SCRAM-SHA-512  (PLAIN, SCRAM-SHA-256)
          ssl_cafile: /path/ca.pem
        with the endpoint's user and password as the SASL account. That is
        how Amazon MSK with SCRAM, Confluent Cloud and Azure Event Hubs'
        Kafka endpoint (user `$ConnectionString`, the connection string as
        the password) are reached."""
        ep = self.hop.source if side == "src" else self.hop.target
        hosts = ep.options.get("hosts") or [f"{ep.host}:{ep.port}"]
        out = {"bootstrap_servers": list(hosts)}
        protocol = str(ep.options.get("security_protocol", "") or "").upper()
        if not protocol:
            return out
        if protocol not in self.SECURITY:
            raise SystemExit(f"security_protocol: {protocol} is not one of"
                             f" {', '.join(self.SECURITY)}")
        out["security_protocol"] = protocol
        if protocol.startswith("SASL"):
            mech = str(ep.options.get("sasl_mechanism", "PLAIN")).upper()
            if mech not in self.MECHANISMS:
                raise SystemExit(f"sasl_mechanism: {mech} is not one of"
                                 f" {', '.join(self.MECHANISMS)}")
            if mech == "AWS_MSK_IAM":
                if protocol != "SASL_SSL":
                    raise SystemExit("sasl_mechanism: AWS_MSK_IAM signs in"
                                     " over SASL_SSL only")
                out.update(sasl_mechanism=mech)
            else:
                out.update(sasl_mechanism=mech, sasl_plain_username=ep.user,
                           sasl_plain_password=ep.password)
        if ep.options.get("ssl_cafile"):
            out["ssl_cafile"] = ep.options["ssl_cafile"]
        return out

    def _signed_in(self, side, make):
        """`make()`, with a refused sign-in said as one. The client retries
        a refused SASL handshake until it gives up, and then says only that
        it could not reach the cluster - measured, `Unable to bootstrap`
        after half a minute, the broker's `Invalid credentials` left in its
        log - which reads as a network fault."""
        import logging
        seen = []

        class Catch(logging.Handler):
            def emit(self, record):
                text = record.getMessage()
                if "SaslAuthenticationFailed" in text:
                    seen.append(text.rsplit(":", 1)[-1].strip())
        log = logging.getLogger("kafka")
        handler = Catch(level=logging.WARNING)
        log.addHandler(handler)
        try:
            return make()
        except Exception:
            if seen:
                raise SystemExit(
                    f"the {'source' if side == 'src' else 'target'} cluster"
                    f" refused the sign-in: {seen[-1]}. Check the endpoint's"
                    " user and password, and its sasl_mechanism") from None
            raise
        finally:
            log.removeHandler(handler)

    def _producer(self, **extra):
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise SystemExit("pip install 'migkit[kafka]' for kafka support")
        kw = {"acks": "all", "request_timeout_ms": 15000,
              "max_request_size": 16 * 2 ** 20, **extra}
        return self._signed_in("dst", lambda: KafkaProducer(
            **self._connection("dst"), **kw))

    #: large values - a message is as large as its topic takes
    #: (`max.message.bytes`, 1 MB by default and set per topic): carried
    #: whole, a message being the broker's unit, by a producer sized to the
    #: source topic's bound (`_sized_for`), every send's outcome checked
    #: before a position moves (a message the target refuses stops the copy
    #: there, never skipped), fetched by bytes, and hashed as it passes
    LARGE_VALUES = pieces.Way(
        "messages carried whole up to their topic's own bound, every send"
        " checked, fetched and hashed by bytes as they pass")
    #: what one worker of the topic copier holds, counted from the code, not
    #: yet measured: a fetch of up to 50 MB (the consumer's own bound), each
    #: message as the fetch parsed it and again in the producer's buffer
    #: until it is sent - three times the larger of the fetch and the
    #: widest message (`row_sizes`)
    COPY_MEMORY = {"own": 64 * 2 ** 20, "per_row": 3.0,
                   "batch": 50 * 2 ** 20}

    def _message_bound(self, topic):
        """The most a message of the source's `topic` can be, in bytes
        (`max.message.bytes` as the topic has it), or None where it will
        not say."""
        got = (self._topic_configs("src", [topic]) or {}).get(topic) or {}
        try:
            return int(got.get("max.message.bytes") or 0) or None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sized_for(most):
        """The producer's settings for messages of up to `most` bytes: a
        request that takes one - the client refuses a larger message
        before it is sent - and room for two of them in its buffer."""
        request = max(16 * 2 ** 20, int(most or 0) + 64 * 2 ** 10)
        return {"max_request_size": request,
                "buffer_memory": max(32 * 2 ** 20, 2 * request)}

    @staticmethod
    def _refused(topic, partition, sends):
        """Stop where a message of a flushed batch did not land: the
        client's flush waits for every send and raises for none, so a
        message the target refused - larger than its topic takes, say -
        was passed over while the positions moved on past it."""
        for offset, sent in sends:
            if sent.failed():
                raise SystemExit(
                    f"{topic}[{partition}]: the target refused the message"
                    f" at offset {offset:,} ({sent.exception}); nothing"
                    " past the last saved position is counted as copied. A"
                    " message larger than the target's topic takes needs"
                    " its max.message.bytes raised to the source topic's")

    def row_sizes(self, side, db, tables=None):
        """{topic: sizes}: a message's key and value as the broker keeps
        them, over the last messages of each partition - `SIZE_SAMPLE` a
        topic at most, each measured and let go, fetched by bytes."""
        from kafka import TopicPartition

        from .base import sizes_of
        consumer = self._consumer(side)
        out = {}
        try:
            for topic in (tables if tables is not None
                          else self._topics(consumer)):
                parts = self._partitions(consumer, topic)
                if not parts:
                    continue
                share = max(1, self.SIZE_SAMPLE // len(parts))
                lengths, whole = [], True
                for p in parts:
                    tp = TopicPartition(topic, p)
                    consumer.assign([tp])
                    end = consumer.end_offsets([tp])[tp]
                    beg = consumer.beginning_offsets([tp])[tp]
                    start = max(beg, end - share)
                    whole = whole and start == beg
                    if start >= end:
                        continue
                    consumer.seek(tp, start)
                    got = 0
                    while got < end - start:
                        batch = consumer.poll(timeout_ms=5000,
                                              max_records=500)
                        if not batch:
                            break
                        for msgs in batch.values():
                            for m in msgs:
                                if m.offset >= end:
                                    continue
                                lengths.append(
                                    max(m.serialized_key_size, 0)
                                    + max(m.serialized_value_size, 0) + 1)
                                got += 1
                if lengths:
                    out[topic] = sizes_of(lengths, exact=whole)
        finally:
            consumer.close()
        return out

    def _stream_options(self):
        from .. import streamout
        return streamout.options(self.hop)

    @staticmethod
    def _message(fmt, db, change, now_ms):
        from .. import streamout
        return streamout.message(fmt, db, change, now_ms)

    def neutral_apply(self, side, db, changes):
        """Every change as a message, in the order the source made them.

        Not collapsed as a table's rows are: a row's state is what a table
        keeps, and every change is what a stream's consumers read. The
        source's order is kept for each key, because a key's messages go
        to one partition."""
        from .. import streamout
        self._target_only(side, "write a change stream")
        if streamout.options(self.hop)[0] in streamout.REGISTRY_FORMATS:
            return self._apply_registered(db, changes)
        sent, skipped = streamout.encoded(self.hop, db, changes,
                                          int(time.time() * 1000))
        producer = self._producer()
        try:
            for topic, key, value in sent:
                producer.send(topic, value=value, key=key.encode())
            producer.flush()
        finally:
            producer.close()
        self._count_skipped(db, skipped)
        return len(changes)

    def _registry(self, side):
        """The schema registry a side's endpoint names, one a side for the
        engine's life (its cache of schemas and ids with it)."""
        from .. import registry
        held = self.__dict__.setdefault("_registries", {})
        if side not in held:
            held[side] = registry.of(self.hop.source if side == "src"
                                     else self.hop.target)
        return held[side]

    def _apply_registered(self, db, changes):
        """The changes through the target's registry, as Avro or as JSON
        Schema (`format`), in Debezium's envelope (`avrostream`,
        `schemaformats`), a tombstone after each delete unless the hop says
        `tombstones: false`."""
        from .. import avrostream, registry, schemaformats, streamout
        fmt = streamout.options(self.hop)[0]
        reg = self._registry("dst")
        if reg is None:
            raise SystemExit(f"format: {fmt} needs the target's"
                             " `schema_registry` - the URL of the registry"
                             " its consumers read schemas from")
        enc = self.__dict__.setdefault("_encoder", {}).setdefault(
            fmt, avrostream.Encoder(reg) if fmt == "avro"
            else schemaformats.JsonEncoder(reg))
        _, rule, _, most = streamout.options(self.hop)
        keep = (self.hop.options or {}).get("tombstones", True)
        now = int(time.time() * 1000)
        producer = self._producer()
        skipped = {}
        try:
            for c in changes:
                topic = rule.format(db=db, table=c["table"])
                try:
                    out = enc.encode(topic, db, c, now, tombstones=keep)
                except registry.Refused as e:
                    raise SystemExit(
                        f"{topic}: the schema registry refused the table's"
                        " new schema - its compatibility rule says the"
                        f" consumers of the topic could not read it: {e}")
                except ValueError as e:
                    raise SystemExit(f"{topic}: not written as {fmt}: {e}")
                for key, value in out:
                    if value is not None and len(value) > most:
                        skipped[topic] = skipped.get(topic, 0) + 1
                        continue
                    producer.send(topic, value=value, key=key)
            producer.flush()
        finally:
            producer.close()
        self._count_skipped(db, skipped)
        return len(changes)

    def _as_compared(self, side, raw):
        """A message's bytes as two clusters' copies of it compare: where
        the side has a registry, the schema's id - which each registry
        numbers its own way - stands as the schema's fingerprint."""
        from .. import registry
        reg = self._registry(side) if raw else None
        frame = registry.frame_of(raw) if reg is not None else None
        if frame is None:
            return raw
        return reg.fingerprint(frame[0]).encode() + frame[1]

    def _translated(self, topic, part, raw):
        """A message framed by the source's registry, framed again by the
        target's: its schema registered there under the same subject and
        its id changed to the one the target gave. The body is untouched.
        Without it a copy between clusters with registries of their own
        carries ids the target's consumers look up in the wrong
        registry."""
        import json

        from .. import registry
        src, dst = self._registry("src"), self._registry("dst")
        frame = (registry.frame_of(raw) if src is not None and dst is not None
                 else None)
        if frame is None:
            return raw
        sid, body = frame
        try:
            kind, text, refs = src.entry(sid)
        except registry.Missing as e:
            raise SystemExit(f"{topic}: a message names schema {sid}, which"
                             f" the source's registry does not hold: {e}")
        if refs:
            raise SystemExit(
                f"{topic}: schema {sid} refers to other schemas in the"
                " source's registry, which a copy does not carry over - the"
                " message is not copied, nor skipped")
        # an Avro schema goes as its parsed form, as it always has; the
        # others as their text, under the kind the source registry gave them
        try:
            return registry.framed(dst.register(
                f"{topic}-{part}", json.loads(text) if kind == "AVRO"
                else text, kind), body)
        except registry.Refused as e:
            raise SystemExit(
                f"{topic}: the target's registry refused schema {sid}"
                f" ({kind}) - its compatibility rule says the topic's"
                f" consumers could not read it: {e}")

    def _count_skipped(self, db, skipped):
        from .. import streamout
        streamout.count_skipped(self.hop, db, skipped)

    def _apply_upsert(self, side, db, table, key, values):
        self.neutral_apply(side, db, [{"op": "update", "table": table,
                                       "key": key, "values": values}])

    def _apply_delete(self, side, db, table, key):
        self.neutral_apply(side, db, [{"op": "delete", "table": table,
                                       "key": key}])

    def target_missing(self, db):
        """A stream's destination is named by the topic rule, not by what
        is there. Read off the topics that exist, a table was paired by
        its last name with a topic an earlier stream had made (`a.o` for
        `o`), and its changes went to `rule(a.o)` - measured, a second
        stream's four changes landed in a topic nobody read."""
        return True

    def _consumer(self, side, **extra):
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise SystemExit("pip install 'migkit[kafka]' for kafka support")
        return self._signed_in(side, lambda: KafkaConsumer(
            **self._connection(side), request_timeout_ms=15000,
            consumer_timeout_ms=10000, enable_auto_commit=False, **extra))

    def databases(self):
        return ["cluster"]

    # --- copying topics, message for message (backlog 0e) ---------------

    def list_move_tables(self, db):
        consumer = self._consumer("src")
        try:
            return [("", t) for t in self._topics(consumer)]
        finally:
            consumer.close()

    def move_key(self, db, sch, tbl):
        return f"topic.{tbl or sch}"

    def _topic_plan(self, topic, partitions):
        """What would make the target's `topic` like the source's, or None
        where the target has it: `{topic, partitions, replication_factor,
        configs}` - the source's partition count, its replication (no more
        than the target has brokers for) and the settings that decide what
        a topic means (`CRITICAL_CONFIGS`)."""
        dst = self._consumer("dst")
        try:
            if topic in dst.topics():
                return None
            brokers = len(dst._client.cluster.brokers()) or 1
        finally:
            dst.close()
        read = self._topic_configs("src", [topic])
        if read is None:
            raise SystemExit(
                f"{topic}: its settings could not be read on the source, and"
                " a topic made without them (compaction, retention) would"
                " mean something else on the target. Nothing was made")
        keep = {k: v for k, v in read.get(topic, {}).items()
                if k in self.CRITICAL_CONFIGS and v not in (None, "")}
        return {"topic": topic, "partitions": int(partitions),
                "replication_factor": self._replication_factor(topic,
                                                               brokers),
                "configs": keep}

    def _made_like_the_source(self, topic, partitions):
        """The target's topic, made as `_topic_plan` says, where it is
        missing."""
        from kafka.admin import NewTopic
        plan = self._topic_plan(topic, partitions)
        if plan is None:
            return None
        admin = self._admin("dst")
        try:
            admin.create_topics([NewTopic(
                topic, num_partitions=plan["partitions"],
                replication_factor=plan["replication_factor"],
                topic_configs=plan["configs"])])
        except Exception as e:  # noqa: BLE001 - another maker got there
            if "TopicAlreadyExists" not in type(e).__name__:
                raise
            return None
        finally:
            admin.close()
        return f"{topic}: made on the target with {partitions} partitions"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """What would make the topic `table` on the target, without making
        it, as text: the plan (`_topic_plan`) of the topic as the source
        has it. A topic has no columns, so `columns` and `key` say
        nothing. A topic the target has is never touched, and says so."""
        src = self._consumer("src")
        try:
            partitions = len(self._partitions(src, table))
        finally:
            src.close()
        if not partitions:
            raise SystemExit(f"{table} is not a topic on the source")
        return json.dumps(self._topic_plan(table, partitions)
                          or {"topic": table, "exists": True},
                          sort_keys=True)

    def neutral_create(self, side, db, table, columns, key=()):
        """Make the topic `table` on the target as `neutral_create_sql`
        says. An existing topic is never altered, emptied or replaced.
        Returns the plan it carried out."""
        said = self.neutral_create_sql(side, db, table, columns, key)
        plan = json.loads(said)
        if not plan.get("exists"):
            self._made_like_the_source(table, plan["partitions"])
        return said

    def _source_replication(self, topic):
        """The copies the source keeps of each partition of `topic` (the
        most any partition has), or None where the source will not say."""
        try:
            admin = self._admin("src")
        except Exception:  # noqa: BLE001 - not said, so not carried
            return None
        try:
            got = admin.describe_topics([topic])
        except Exception:  # noqa: BLE001 - not said, so not carried
            return None
        finally:
            admin.close()
        for described in got or ():
            if str(described.get("name") or described.get("topic")) != topic:
                continue
            counts = [len(part.get("replica_nodes")
                          or part.get("replicas") or ())
                      for part in described.get("partitions") or ()]
            return max(counts) if counts and max(counts) > 0 else None
        return None

    def _replication_factor(self, topic, brokers):
        """The copies a topic made on the target keeps of each partition:
        the hop's `replication_factor`; else the source topic's own, no
        more than the target has brokers for; else three, or the brokers
        there are."""
        said = (self.hop.options or {}).get("replication_factor")
        if said:
            return int(said)
        theirs = self._source_replication(topic)
        return min(theirs, brokers) if theirs else min(3, brokers)

    def _replicas(self):
        """The copies of a partition a topic made on the target keeps, for
        a figure before any is made (`_room_rows`): the hop's, else three
        or the brokers there are."""
        said = (self.hop.options or {}).get("replication_factor")
        if said:
            return int(said)
        dst = self._consumer("dst")
        try:
            return min(3, len(dst._client.cluster.brokers()) or 1)
        finally:
            dst.close()

    def create_missing(self, db, log=None):
        """The topics in scope the target lacks, made before the copier
        fills any: the source's partition count, its replication (no more
        than the target has brokers for) and the settings that decide what
        a topic means (`CRITICAL_CONFIGS`). One that is there is never
        touched - not altered, not emptied. The copy of a topic makes it
        too if it is still missing (`_copy_topic`): a topic made between
        the two is made once."""
        log = log or (lambda m: None)
        src = self._consumer("src")
        try:
            wanted = [(t, len(self._partitions(src, t)))
                      for _, t in self.list_move_tables(db)]
        finally:
            src.close()
        for topic, partitions in wanted:
            made = self._made_like_the_source(topic, partitions)
            if made:
                log(made)

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """One topic, each partition's messages into the same partition on
        the target, with their keys, values, headers and times: the times
        are what a group's position is translated by afterwards
        (`_translate`). Only committed messages are read.

        Resumed per partition from the checkpoint. A batch the target got
        and the checkpoint did not - a crash between the two - is counted
        off the target's own end, and not sent twice."""
        topic = tbl or sch
        st = ck.setdefault(self.move_key(db, sch, tbl), {})
        if st.pop("done", None):
            # a topic grows: what arrived since the copy goes on from the
            # positions it saved, rather than being skipped
            log(f"{topic}: done earlier; copying what arrived since")
        self._copy_topic(topic, st, chunk, ck.save, log)
        st["done"] = True
        ck.save()

    def _copy_topic(self, topic, st, chunk, save, log):
        """What `topic` holds past the positions in `st`, copied, and the
        positions moved on: the copy's whole work, and each round of the
        tail's. Returns how many messages went.

        Each partition is copied in the way it was planned when the copy
        reached it (`_plan_offsets`, kept in its state as `mode`):
        `identical` - the target's offsets are the source's, one message to
        one offset, every one proved as it lands; `mapped` - each batch and
        the source-to-target offsets it took are one transaction on the
        target (`_copy_mapped`); `plain` - sent and counted as before."""
        from kafka import TopicPartition

        from ..wording import progress

        # set when the consumer is made: its fetcher reads it only then
        src = self._consumer("src", isolation_level="read_committed")
        dst = self._consumer("dst")
        # sized to the largest message the source topic takes (`LARGE_VALUES`)
        sized = self._sized_for(self._message_bound(topic))
        producers = {}

        def producer_of(kind):
            """The producer of one kind, made when first asked: `plain`
            as it always was, `ident` idempotent (a retried send is not a
            second message, which would move every offset after it), `txn`
            transactional, its id one per target topic so a restarted copy
            fences the old one and ends what it left open."""
            if kind not in producers:
                extra = dict(sized, max_in_flight_requests_per_connection=1)
                if kind != "plain":
                    extra["enable_idempotence"] = True
                if kind == "txn":
                    extra["transactional_id"] = f"migkit-copy.{topic}"
                made = self._producer(**extra)
                if kind == "txn":
                    made.init_transactions()
                producers[kind] = made
            return producers[kind]
        marked = self._marks_positions()
        sent = 0
        try:
            parts = self._partitions(src, topic)
            made = self._made_like_the_source(topic, len(parts))
            if made:
                log(made)
            if len(self._partitions(dst, topic)) < len(parts):
                raise SystemExit(
                    f"{topic} has fewer partitions on the target than on the"
                    " source: a key's messages would land in another"
                    " partition, out of the order its consumers read them")
            began = time.monotonic()
            for p in parts:
                src_tp, dst_tp = TopicPartition(topic, p), \
                    TopicPartition(topic, p)
                pst = st.setdefault("parts", {}).setdefault(str(p), {})
                end = src.end_offsets([src_tp])[src_tp]
                there = dst.end_offsets([dst_tp])[dst_tp]
                if "base" not in pst:
                    plan = self._plan_offsets(src, dst, src_tp, dst_tp)
                    pst.update(base=there, copied=0,
                               next=src.beginning_offsets([src_tp])[src_tp],
                               mode=plan["mode"], first=plan["first"])
                    # kept before anything is sent: a stop before the
                    # first batch's position was saved took what that
                    # batch put on the target for messages held before
                    # the copy, and sent them again
                    save()
                    log(f"{topic}[{p}]: {plan['why']}")
                    if there and plan["mode"] == "plain":
                        log(f"{topic}[{p}]: the target held {there:,}"
                            " messages before the copy; offsets will differ,"
                            " and group positions are translated by message")
                begin = src.beginning_offsets([src_tp])[src_tp]
                if pst["next"] < begin:
                    self._cut_by_retention(topic, p, pst, begin, producer_of,
                                           save, log)
                self._partition_ready(topic, p, pst, src, dst, src_tp, dst_tp,
                                      end, producer_of, save, log)
                there = dst.end_offsets([dst_tp])[dst_tp]
                if pst["next"] >= end:
                    continue
                # what reached the target after the last checkpoint
                ahead = there - pst["base"] - pst["copied"]
                src.assign([src_tp])
                src.seek(src_tp, pst["next"])
                total = end - pst["next"]
                done = 0
                while pst["next"] < end:
                    batch = [m for msgs in src.poll(
                                 timeout_ms=5000,
                                 max_records=max(1, min(int(chunk), 5000))
                             ).values() for m in msgs if m.offset < end]
                    if not batch:
                        # past what a reader of committed messages is given
                        # - a transaction's marker, an aborted write - the
                        # consumer's own position says so; its end is not
                        # reached by any message
                        moved = max(pst["next"],
                                    min(src.position(src_tp), end))
                        if pst.get("mode") == "identical" \
                                and moved > pst["next"]:
                            # an offset with no message: the offsets are
                            # not one to one from here
                            self._identity_ends(
                                topic, p, pst, producer_of, save, log,
                                f"offset {pst['next']:,} holds no message a"
                                " reader of committed messages is given")
                        pst["next"] = moved
                        save()
                        break
                    if pst.get("mode") == "identical":
                        batch, went = self._copy_identical(
                            topic, p, pst, batch, producer_of, marked, save,
                            log)
                        sent += went
                        done += went
                        if went:
                            log(progress(f"{topic}[{p}]", done, total, began,
                                         time.monotonic(), unit="messages"))
                        if not batch:
                            continue
                        ahead = 0
                    if pst.get("mode") == "mapped":
                        went = self._copy_mapped(topic, p, pst, batch,
                                                 producer_of("txn"), marked,
                                                 save)
                        sent += went
                        done += went
                        log(progress(f"{topic}[{p}]", done, total, began,
                                     time.monotonic(), unit="messages"))
                        continue
                    producer = producer_of("plain")
                    sends, counted = [], 0
                    for m in batch:
                        if ahead > 0:
                            ahead -= 1
                        else:
                            sends.append((m.offset, producer.send(
                                topic, **self._carried(topic, p, m,
                                                       marked))))
                            sent += 1
                        counted += 1
                    producer.flush()
                    # a message the target refused stops the copy before a
                    # position moves past it
                    self._refused(topic, p, sends)
                    pst["copied"] += counted
                    done += counted
                    pst["next"] = batch[-1].offset + 1
                    save()
                    log(progress(f"{topic}[{p}]", done, total, began,
                                 time.monotonic(), unit="messages"))
        finally:
            for made in producers.values():
                made.close()
            src.close()
            dst.close()
        return sent

    def _cut_by_retention(self, topic, p, pst, begin, producer_of, save, log):
        """The source's log of a partition starts past the offset the copy
        had reached: retention, or a delete of records, took messages
        before they were copied. They cannot be; they are counted in the
        partition's state (`lost`) and said, and the copy goes on from the
        first message the log still holds. Read from the lost offset, a
        consumer is moved to the log's end by its reset, and everything
        between was passed over without a word. An identical partition
        keeps its offsets below where it stopped (`_identity_ends`)."""
        gone = begin - pst["next"]
        why = (f"{gone:,} messages ({pst['next']:,} to {begin - 1:,}) left"
               " the source's log before they were copied")
        if pst.get("mode") == "identical":
            self._identity_ends(topic, p, pst, producer_of, save, log, why)
        pst["lost"] = int(pst.get("lost") or 0) + gone
        pst["next"] = begin
        save()
        log(f"{topic}[{p}]: {why}; they cannot be copied, and the copy goes"
            f" on from {begin:,}")

    def moved_nothing(self, db):
        """Topics holding messages on the source and none on the target:
        what reached a partition is its end past its start."""
        from kafka import TopicPartition
        try:
            src, dst = self._consumer("src"), self._consumer("dst")
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None
        try:
            def held(consumer, topic):
                tps = [TopicPartition(topic, p)
                       for p in self._partitions(consumer, topic)]
                if not tps:
                    return False
                ends = consumer.end_offsets(tps)
                begins = consumer.beginning_offsets(tps)
                return any(ends[tp] > begins[tp] for tp in tps)
            return [topic for _, topic in self.list_move_tables(db)
                    if held(src, topic) and not held(dst, topic)]
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None
        finally:
            src.close()
            dst.close()

    # --- keeping the target following: the copier, round after round -----

    def _tail_state(self, db, token_path):
        """The positions the tail goes on from: its own, or - the first
        time - where the copy of the topics ended, so a tail after a copy
        sends nothing twice. Either that cannot be read whole stops, naming
        it: a tail started from anywhere else sends messages twice or
        skips them (`tailctl.position`, `checkpoint.read_move`)."""
        from .. import checkpoint, tailctl
        env = tailctl.position(token_path)
        if env is not None:
            return env.get("token") or {}
        state = checkpoint.read_move(self.hop.report_dir(db) / "move.json")
        for st in (state or {}).values():
            if isinstance(st, dict):
                st.pop("done", None)
        return state or {}

    #: a topic is a log that keeps its order: the tail goes on from the
    #: positions the copy saved, and nothing changes behind them
    TAIL_GOES_ON_FROM_THE_COPY = True

    def tail_start(self, db, token_path):
        """Nothing to fix before a copy (`TAIL_GOES_ON_FROM_THE_COPY`)."""
        return False

    def tail_apply(self, db, go, token_path, log):
        """Each round, every topic copied on from where the last round
        ended, round the loop every tail goes round (`tailctl.run_loop`):
        stopped when asked, ended once caught up where the hop or a
        schedule says so, a lost connection tried again from the saved
        positions. The positions are saved after each batch reached the
        target, as the copy saves them - and a message sent whose batch's
        position was not saved yet is known on the target by the header
        it carries (`HEADER`), so it is not sent twice."""
        import json

        from .. import failpoint, tailctl, topicstates
        state = self._tail_state(db, token_path)
        log("following the source's topics, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to copy)"))
        if not go:
            behind = self._behind(db, state)
            log(f"{sum(behind.values()):,} messages not on the target yet"
                + "".join(f"; {t} {n:,}" for t, n in sorted(behind.items())
                          if n))
            return
        env = [tailctl.position(token_path)]
        last = [json.dumps(state, sort_keys=True, default=str)
                if env[0] is not None else None]
        seen = [0]

        def save():
            text = json.dumps(state, sort_keys=True, default=str)
            if text == last[0]:
                # nothing moved: the file is not written again each round
                return
            failpoint.hit("tail.applied")
            env[0] = tailctl.moved(
                token_path, env[0] or tailctl.new_stream(self, None),
                json.loads(text))
            last[0] = text
            failpoint.hit("tail.saved")

        topics = topicstates.TopicSet(self, db, state, log)
        shown = [""]

        def look(at):
            went = self._tail_round(db, topics, save, log)
            if went:
                seen[0] += went
                log(f"{seen[0]} messages")
            if topics.said() != shown[0]:
                # the topics not live, said when they change
                shown[0] = topics.said()
                log("topics: " + (shown[0] or "all live"))
            return state, went, not went

        def lost(e, at):
            # the positions as saved: what a round sent after them is known
            # by its header, and not sent again
            state.clear()
            state.update(self._tail_state(db, token_path))
            return state
        tailctl.run_loop(token_path.parent, db, look, log, "messages",
                         hop=self.hop, go=go, token=state, lost=lost,
                         secrets=(self.hop.source.password,
                                  self.hop.target.password))

    #: what a round of the tail says of a topic it copies: only these
    #: (the copy's progress lines are for the copy)
    NEWS = ("left the source's log", "made on the target")

    def _tail_round(self, db, topics, save, log):
        """One round of the tail: the topics' states held to the hop's
        scope (`topicstates`) and saved before a message is copied, then
        each topic that is followed copied on from its saved offsets. A
        topic that left or is parked is not copied. Returns how many
        messages went."""
        state = topics.state
        src = self._consumer("src")
        try:
            listing = {t: len(self._partitions(src, t) or [])
                       for t in self._topics(src)}
            everywhere = set(src.topics())
        finally:
            src.close()
        if topics.boundary(listing, everywhere,
                           lambda names: self._ranges("src", names)):
            save()

        def news(m):
            if any(n in m for n in self.NEWS):
                log(m)
        went = 0
        for topic in topics.following():
            went += self._copy_topic(topic, state[topics.key(topic)], 5000,
                                     save, news)
            if topics.settle(topic):
                save()
        save()
        return went

    def _ranges(self, side, topics):
        """{topic: {"begin": {partition: offset}, "end": {...}}} as the
        copier reads them - first offset, and the end a reader of
        committed messages is given - of the topics that are on the side."""
        from kafka import TopicPartition
        consumer = self._consumer(side, isolation_level="read_committed")
        try:
            tps = {topic: [TopicPartition(topic, p)
                           for p in self._partitions(consumer, topic)]
                   for topic in topics}
            every = [tp for got in tps.values() for tp in got]
            # one ask for every partition, not one for each topic
            begins = consumer.beginning_offsets(every) if every else {}
            ends = consumer.end_offsets(every) if every else {}
            return {topic: {
                "begin": {str(tp.partition): begins[tp] for tp in got},
                "end": {str(tp.partition): ends[tp] for tp in got}}
                for topic, got in tps.items()}
        finally:
            consumer.close()

    #: a reader of its own can read a topic again from any offset the log
    #: still holds, without moving the tail's: a topic that left comes back
    #: with only the messages since (`topicstates`)
    REREADS_FROM_A_POSITION = True

    def snapshot_mark(self, side, db):
        """Where each of the hop's topics ends on a side now, for a reader
        of committed messages, as text a message can be held to
        (`mark_covers`): the seam a topic added to a running tail joins
        at - the messages before it are the copy's, those from it on the
        stream's."""
        topics = [t for _, t in self.list_move_tables(db)]
        return json.dumps({t: r["end"] for t, r in
                           self._ranges(side, topics).items()},
                          sort_keys=True)

    def mark_covers(self, side, db, mark, change):
        """Whether the message `change` names - `topic`, `partition`,
        `offset`, as a mapping or an object - lies below the mark's end of
        its partition (`snapshot_mark`): True, False, or None where the
        mark does not say."""
        try:
            seam = json.loads(mark)
            if isinstance(change, dict):
                topic, part, offset = (change["topic"], change["partition"],
                                       change["offset"])
            else:
                topic, part, offset = (change.topic, change.partition,
                                       change.offset)
            return int(offset) < int(seam[topic][str(part)])
        except (TypeError, ValueError, KeyError, AttributeError):
            return None

    def _behind(self, db, state):
        """{topic: messages on the source past the saved positions}."""
        from kafka import TopicPartition
        src = self._consumer("src")
        try:
            out = {}
            for _, topic in self.list_move_tables(db):
                parts = (state.get(self.move_key(db, "", topic)) or {}).get(
                    "parts", {})
                tps = [TopicPartition(topic, p)
                       for p in self._partitions(src, topic)]
                ends = src.end_offsets(tps) if tps else {}
                begins = src.beginning_offsets(tps) if tps else {}
                out[topic] = sum(
                    max(0, ends[tp] - (parts.get(str(tp.partition), {})
                                       .get("next", begins[tp])))
                    for tp in tps)
            return out
        finally:
            src.close()

    def log_position(self, side, db):
        """Where each of the hop's topics ends on one side now, every
        partition's end as the brokers hold it -
        `{topic: {partition: offset}}`, the form the tail's position is
        held to (`position_reached`). The end includes a transaction still
        open: the tail passes it only once that transaction is over, so a
        drain to it is never proved while a write from before the freeze
        is outstanding."""
        from kafka import TopicPartition
        consumer = self._consumer(side)
        try:
            out = {}
            for _, topic in self.list_move_tables(db):
                tps = [TopicPartition(topic, p)
                       for p in self._partitions(consumer, topic)]
                for tp, end in (consumer.end_offsets(tps)
                                if tps else {}).items():
                    out.setdefault(topic, {})[str(tp.partition)] = end
            return out
        finally:
            consumer.close()

    @staticmethod
    def position_reached(have, want):
        """Whether a tail whose saved position is `have` has copied every
        partition up to the ends in `want` (`log_position`): True or False,
        None where either is not a position."""
        if not isinstance(have, dict) or not isinstance(want, dict):
            return None
        for topic, parts in want.items():
            saved = (have.get(f"topic.{topic}") or {}).get("parts") or {}
            for p, end in (parts or {}).items():
                if int((saved.get(str(p)) or {}).get("next", -1)) < int(end):
                    return False
        return True

    def src_lsn(self, db):
        """Each source partition's end now, where a tail is running to
        reach it; None where none is."""
        from .. import tailctl
        if not tailctl.alive(self.hop.report_dir(db), self.hop):
            return None
        return self.log_position("src", db)

    def fence_wait(self, db, at, timeout=300):
        """Wait until the tail's saved position has reached `at` in every
        partition. True when it has, False on timeout, None where there is
        nothing to wait on."""
        from .. import tailctl
        if not at:
            return None
        path = tailctl.token_path(self.hop, db)
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                state = tailctl.saved(path)
            except SystemExit:
                return None
            if not isinstance(state, dict):
                return None
            if self.position_reached(state, at):
                # where the confirm pass compares up to
                self._fenced_at = at
                return True
            # looked at every 50 ms, not once a second (audit SP19)
            time.sleep(tailctl.POLL)
        return False

    def drain_report(self, db, at):
        """What the drain to `at` (`log_position`) still waits for, in
        words: the messages not yet on the target topic by topic, and how
        far behind each consumer group of the source stands - the group's
        committed offset to the partition's end, which is what its
        consumers have yet to read, all of it on the target once the tail
        has passed `at`."""
        from kafka import TopicPartition

        from .. import tailctl
        try:
            state = tailctl.saved(tailctl.token_path(self.hop, db))
        except SystemExit:
            state = None
        state = state if isinstance(state, dict) else {}
        behind = {}
        for topic, parts in (at or {}).items():
            saved = (state.get(f"topic.{topic}") or {}).get("parts") or {}
            left = sum(max(0, int(end) - int((saved.get(str(p)) or {})
                                             .get("next", 0)))
                       for p, end in parts.items())
            if left:
                behind[topic] = left
        lag = {}
        try:
            for group in sorted(self._groups("src")):
                held = self._group_offsets("src", group)
                ends = {tp: int(at[tp.topic][str(tp.partition)])
                        for tp in held if str(tp.partition)
                        in (at.get(tp.topic) or {})}
                if ends:
                    lag[group] = sum(max(0, ends[tp] - held[tp])
                                     for tp in ends)
        except Exception:  # noqa: BLE001 - the groups, said as unread
            lag = None
        said = (f"{sum(behind.values()):,} messages not on the target yet ("
                + ", ".join(f"{t} {n:,}"
                            for t, n in sorted(behind.items())[:5])
                + ")") if behind else "every message up to the position is"\
            " on the target"
        if lag is None:
            return said + "; the consumer groups' lag could not be read"
        if lag:
            said += "; consumer group lag at the position: " + ", ".join(
                f"{g} {n:,}" for g, n in sorted(lag.items())[:5])
        return said

    #: topics a cluster keeps for itself: the broker's own (`__`), a schema
    #: registry's store (`_schemas`), Confluent's and Redpanda's. Copied, a
    #: registry's store overwrote the target registry's schemas - measured
    #: between two clusters with registries of their own, and the check then
    #: compared the two registries' stores as data. A copy carries the
    #: schemas its messages use by registering them (`_translated`)
    INTERNAL = ("__", "_schemas", "_confluent", "_redpanda")

    def _topics(self, consumer):
        """Every topic migkit verifies: internal ones, the cluster mirror's
        own on the target (`mirror.internal`) and the ones the hop excludes
        left out. This engine used to ignore `exclude`."""
        from .. import mirror
        return sorted(t for t in consumer.topics()
                      if not t.startswith(self.INTERNAL)
                      and not mirror.internal(t)
                      and not self.hop.excluded("cluster", t))

    def _partitions(self, consumer, topic):
        return sorted(consumer.partitions_for_topic(topic) or [])

    def _admin(self, side):
        from kafka.admin import KafkaAdminClient
        return self._signed_in(side, lambda: KafkaAdminClient(
            **self._connection(side), request_timeout_ms=15000))

    def snapshot_state(self, db, state_dir, kind="all"):
        """Where the target stood before a repair: each topic's partition
        ends, every consumer group's committed offsets and the topics'
        settings. A message cannot be taken back out of a log; what a
        rollback puts back is where the groups read from, and the ends say
        which messages came after."""
        import json

        from kafka import TopicPartition
        consumer = self._consumer("dst")
        try:
            topics = self._topics(consumer)
            tps = [TopicPartition(t, p) for t in topics
                   for p in self._partitions(consumer, t)]
            ends = consumer.end_offsets(tps) if tps else {}
        finally:
            consumer.close()
        groups = {g: {f"{tp.topic}[{tp.partition}]": off for tp, off
                      in sorted(self._group_offsets("dst", g).items())}
                  for g in sorted(self._groups("dst"))}
        (state_dir / "dst-offsets.json").write_text(json.dumps({
            "ends": {f"{tp.topic}[{tp.partition}]": off
                     for tp, off in sorted(ends.items())},
            "groups": groups,
            "configs": self._topic_configs("dst", topics) if topics else {},
        }, indent=2, sort_keys=True, default=str))

    # semantics-critical topic configs: a cleanup.policy or retention
    # mismatch silently changes what the topic MEANS on the target
    CRITICAL_CONFIGS = ("cleanup.policy", "retention.ms", "retention.bytes",
                        "max.message.bytes", "min.insync.replicas",
                        "compression.type", "delete.retention.ms",
                        "segment.ms")

    @staticmethod
    def _described(resp):
        """{resource name: {setting: value}} from `describe_configs`.

        This client answers with nested dicts - measured on kafka-python
        3.0.11, `{'topic': {'orders': {'retention.ms': {'value': '1000',
        'config_source': 'DYNAMIC_TOPIC_CONFIG', 'is_sensitive': False,
        ...}}}}`. The reader written for the 2.x response objects iterated
        that dict, got its keys as strings, raised, and the topic settings
        comparison was skipped without a word. A sensitive value is never
        carried out of here.
        """
        out = {}
        for by_name in resp.values():
            for name, settings in by_name.items():
                out[str(name)] = {
                    str(k): ("***" if (v or {}).get("is_sensitive")
                             else str((v or {}).get("value")))
                    for k, v in settings.items()}
        return out

    def _topic_configs(self, side, topics):
        from kafka.admin import ConfigResource, ConfigResourceType
        out = {}
        try:
            admin = self._admin(side)
            try:
                for i in range(0, len(topics), 20):
                    got = self._described(admin.describe_configs(
                        [ConfigResource(ConfigResourceType.TOPIC, t)
                         for t in topics[i:i + 20]], config_filter="all"))
                    for name, entries in got.items():
                        out[name] = {k: entries.get(k, "")
                                     for k in self.CRITICAL_CONFIGS}
            finally:
                admin.close()
        except Exception:
            return None
        return out

    #: broker settings that change what a message or a topic means, rather
    #: than how fast the broker serves it; a difference in any of these
    #: fails the check
    CRITICAL_BROKER = ("auto.create.topics.enable", "log.retention.ms",
                       "log.retention.hours", "log.cleanup.policy",
                       "message.max.bytes", "min.insync.replicas",
                       "default.replication.factor", "num.partitions",
                       "compression.type", "log.message.timestamp.type")

    def _broker_configs(self, side):
        """{setting: value} of the first broker of a side, as the cluster
        describes it. Raises where the cluster will not say."""
        from kafka.admin import ConfigResource, ConfigResourceType
        admin = self._admin(side)
        try:
            # the attribute is `node_id` on this client (3.x) and was
            # `nodeId` on the 2.x one - measured, 3.0.11 has no `nodeId`
            # at all
            first = list(admin._client.cluster.brokers())[0]
            node = str(first.node_id if hasattr(first, "node_id")
                       else first.nodeId)
            resp = admin.describe_configs(
                [ConfigResource(ConfigResourceType.BROKER, node)],
                config_filter="all")
        finally:
            admin.close()
        return self._described(resp).get(node, {})

    def check_params(self, db):
        """The brokers' own settings, both sides, through the same report
        every engine's server settings go through. A broker's defaults are
        what a topic created by a producer - auto-create, a default
        partition count, a default retention - silently gets, so a target
        whose defaults differ turns the same traffic into different topics.
        """
        def pull(side):
            try:
                return self._broker_configs(side)
            except Exception as e:
                return {self.UNREADABLE: str(e).splitlines()[-1][:80]
                        if str(e) else type(e).__name__}
        return self._param_result(
            db, pull("src"), pull("dst"), self.CRITICAL_BROKER,
            "align the target brokers' defaults before producers reach it")

    ENGINE_FAMILY = "kafka"

    def _brand_probes(self):
        """The cluster id from each side.

        It is the one thing a Kafka client can read that ever carries a
        brand: measured, Redpanda answers `redpanda.<uuid>` and Apache Kafka
        answers a bare `5L6g3nShT-eMCtK--X86sw`. The bare form is what MSK
        and Confluent report too, so it names the protocol implementation and
        not the distribution - which is recorded as a limit rather than
        papered over.
        """
        def one(side):
            try:
                cid = self._cluster_id(side)
                return {"cluster_id": cid} if cid else {}
            except Exception:
                return {}
        return (one("src"), one("dst"))

    def _cluster_id(self, side):
        consumer = self._consumer(side)
        try:
            return consumer._client.cluster.cluster_id
        finally:
            consumer.close()

    def server_identity(self, side, db):
        """The cluster's id, which every broker of it answers with."""
        cid = self._cluster_id(side)
        return {"id": {"cluster": str(cid)}} if cid else {}

    def rows_held(self, db, tables):
        """None of them: a topic's copy goes on after what the target's
        topic holds, counted from its own end (`base`), and removes
        nothing - there is nothing held for it to write over."""
        return set()

    # --- the assessment: versions, limits, writers, room, hand work -----

    #: the broker setting that names the protocol version a cluster runs
    #: (`3.7-IV4`); a cluster that does not list it is read through the
    #: protocol versions its brokers accept, and the version then is said
    #: to be inferred
    VERSION_SETTING = "inter.broker.protocol.version"
    #: the oldest broker that has message headers, timestamps and
    #: transactions - what the copy carries
    OLDEST_BROKER = (0, 11)

    @staticmethod
    def _numbers(text):
        """(major, minor) of a version written `3.7`, `3.7-IV4` or
        `3.7.1`, or None."""
        got = re.match(r"\s*(\d+)\.(\d+)", str(text or ""))
        return (int(got.group(1)), int(got.group(2))) if got else None

    def _version_of(self, side):
        """(version as `3.7`, how it was read) of one side's brokers, or
        (None, ""): the brokers' own setting, else what the client library
        works out from the protocol versions they accept."""
        try:
            got = self._numbers(self._broker_configs(side).get(
                self.VERSION_SETTING))
        except Exception as e:  # noqa: BLE001 - asked another way below
            if type(e).__name__ == "NoBrokersAvailable":
                # no broker answered: asking again waits for nothing
                return None, ""
            got = None
        if got:
            return "%d.%d" % got, "the brokers' own setting"
        try:
            admin = self._admin(side)
        except Exception:  # noqa: BLE001 - unknown, said as such
            return None, ""
        try:
            seen = (getattr(admin, "config", None) or {}).get("api_version")
            if not seen:
                seen = getattr(getattr(getattr(admin, "_manager", None),
                                       "broker_version_data", None),
                               "broker_version", None)
        except Exception:  # noqa: BLE001 - unknown, said as such
            seen = None
        finally:
            admin.close()
        if seen:
            return (".".join(str(x) for x in tuple(seen)[:2]),
                    "inferred by the client library from the protocol"
                    " versions the brokers accept")
        return None, ""

    def _versions(self):
        """{side: (version, how)}, asked once per engine."""
        got = self.__dict__.get("_versions_cache")
        if got is None:
            got = self.__dict__["_versions_cache"] = {
                side: self._version_of(side) for side in ("src", "dst")}
        return got

    def _server_versions(self):
        """(source version, target version), each as `3.7`, None where the
        brokers would not say and the library could not work it out."""
        got = self._versions()
        return got["src"][0], got["dst"][0]

    def _version_row(self, sv, dv, parts=2):
        """Held to the minor version: 3.7 and 3.8 are not the same
        protocol."""
        return super()._version_row(sv, dv, parts)

    @staticmethod
    def _library():
        """(the client library's version, the newest broker it knows) -
        each None where it does not say."""
        have = newest = None
        try:
            from importlib import metadata
            have = metadata.version("kafka-python")
        except Exception:  # noqa: BLE001 - not said by this install
            have = None
        try:
            from kafka.protocol.broker_version_data import BROKER_API_VERSIONS
            newest = max(BROKER_API_VERSIONS)
        except Exception:  # noqa: BLE001 - this client does not list them
            newest = None
        return have, newest

    def _version_items(self):
        """What the two versions mean for this move, beside whether they
        match: how each was read, a broker too old for what the copy
        carries, a target older than the source, and a broker newer than
        the client library knows."""
        got = self._versions()
        items = []

        def add(level, item, detail):
            items.append({"level": level, "scope": "instance",
                          "item": item, "detail": detail})
        for side, name in (("src", "source"), ("dst", "target")):
            version, how = got[side]
            number = self._numbers(version)
            if number is None:
                continue
            if how.startswith("inferred"):
                add("warn", f"the {name} brokers' version is inferred",
                    f"{version}: {how}; the brokers did not name it in"
                    " their settings")
            if number < self.OLDEST_BROKER:
                add("fail", f"the {name} brokers are too old",
                    f"{version}: message headers, timestamps and"
                    " transactions, which the copy carries, need 0.11 or"
                    " newer")
        s, d = (self._numbers(got[x][0]) for x in ("src", "dst"))
        if s and d and d < s:
            add("warn", "the target brokers are older than the source's",
                f"src {got['src'][0]} / dst {got['dst'][0]}: a message"
                " format or compression the source's topics use may be one"
                " the target does not know (zstd needs 2.1), and a topic"
                " setting it does not have is refused when the topic is"
                " made")
        have, newest = self._library()
        if have is None:
            add("warn", "the client library's version is not known",
                "which brokers it speaks to is unknown, not clean")
        else:
            beyond = [f"{name} {got[side][0]}" for side, name in
                      (("src", "source"), ("dst", "target"))
                      if self._numbers(got[side][0]) and newest
                      and self._numbers(got[side][0]) > tuple(newest[:2])]
            add("warn" if beyond else "pass", "client library and brokers",
                f"client library {have}"
                + (", which knows brokers up to "
                   + ".".join(map(str, newest[:2])) if newest else "")
                + (f"; newer than that: {', '.join(beyond)} - the library"
                   " speaks the newest protocol versions both know"
                   if beyond else ""))
        return items

    def _assess_extra(self):
        """What has to line up before topics are moved: the topics the
        target will have to make, what each broker version means, the
        writers beside the move, the room on the target and the work the
        move leaves. Partition counts, which cannot be fixed afterwards,
        and message limits are the pre-flight's (`PREFLIGHT`)."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "instance",
                          "item": item, "detail": str(detail)})
        try:
            sc, dc = self._consumer("src"), self._consumer("dst")
            try:
                stops = set(self._topics(sc))
                dtops = set(self._topics(dc))
            finally:
                sc.close()
                dc.close()
        except Exception as e:
            add("warn", "cannot list topics on both sides",
                f"{str(e)[:90]} - unknown, not clean")
            return items

        missing = sorted(stops - dtops)
        add("pass", "every source topic exists on the target",
            f"{len(stops)} topics" if not missing else
            f"{len(missing)} not there yet, made as the move goes with the"
            " source's partitions, replication and settings:"
            f" {', '.join(missing[:5])}"
            + (f" and {len(missing) - 5} more" if len(missing) > 5 else ""))
        items += self._version_items()
        items += self._writer_rows()
        items += self._room_rows()
        inv = self._handwork()
        items += inv.rows() + inv.summary()
        return items

    #: checked by `assess` before anything moves (`Engine._preflight_items`)
    PREFLIGHT = ("_partitions_fit", "_message_limits")

    def _partitions_fit(self, db):
        """Partition counts, topic by topic, where the target has the
        topic: the one thing that cannot be fixed afterwards - a
        consumer's key-to-partition mapping depends on the count, so a
        target with a different number delivers the same key to a different
        consumer, and nothing errors."""
        sc, dc = self._consumer("src"), self._consumer("dst")
        try:
            stops, dtops = set(self._topics(sc)), set(self._topics(dc))
            drift = []
            for topic in sorted(stops & dtops):
                try:
                    sp = len(self._partitions(sc, topic) or [])
                    dp = len(self._partitions(dc, topic) or [])
                except Exception:  # noqa: BLE001 - not compared, not said ok
                    continue
                if sp != dp:
                    drift.append(f"{topic} {sp}->{dp}")
        finally:
            sc.close()
            dc.close()
        if not drift:
            return Result("assess", "partition counts match", "ok",
                          "all equal")
        return Result(
            "assess", "partition counts match", "diff",
            "; ".join(drift[:5]) + " - a key lands on a different partition"
            " when the count changes, so ordering and consumer assignment"
            " move with it and nothing errors", "",
            "make the target's topics with the source's partition counts, or"
            " leave them to be made by the move")

    def _message_limits(self, db):
        """The largest message each topic takes, source against target,
        where the target has the topic: a message over the target's limit
        is refused and the copy stops there. A sample of the source's
        messages cannot say none is that large, so the topic's limit is
        what is held."""
        sc, dc = self._consumer("src"), self._consumer("dst")
        try:
            common = sorted(set(self._topics(sc)) & set(self._topics(dc)))
        finally:
            sc.close()
            dc.close()
        if not common:
            return Result("assess", "message size limits", "ok",
                          "no topic is on both sides yet")
        src = self._topic_configs("src", common)
        dst = self._topic_configs("dst", common)
        if src is None or dst is None:
            return Result(
                "assess", "message size limits", "error",
                "the topics' settings could not be read on "
                + ("the source" if src is None else "the target")
                + " - unknown, not clean")

        def limit(configs, topic):
            try:
                return int((configs.get(topic) or {}).get(
                    "max.message.bytes") or 0) or None
            except (TypeError, ValueError):
                return None
        small = []
        for topic in common:
            a, b = limit(src, topic), limit(dst, topic)
            if a and b and b < a:
                small.append(f"{topic} takes {a:,} bytes on the source and"
                             f" {b:,} on the target")
        if not small:
            return Result("assess", "message size limits", "ok",
                          f"{len(common)} topics take on the target every"
                          " message they take on the source")
        return Result(
            "assess", "message size limits", "diff",
            "; ".join(small[:5]) + " - the target refuses a message over"
            " its limit, and the copy stops at the first", "",
            "raise max.message.bytes on those target topics to the source's")

    def stream_writers(self, db):
        """[(what, pausable)] writing into the target's topics besides a
        repair: migkit's own tail, and every producer the target's brokers
        still hold state for on the hop's topics that wrote within
        `WRITER_WINDOW` seconds (`_producers`) - none of which can be
        paused from here."""
        out = super().stream_writers(db)
        if out:
            # migkit's own tail writes there, and a producer id says
            # nothing of whose it is: they cannot be told apart
            return out
        found = self._producers("dst", db, self._migkit_wrote_at(db))
        return out + [(f"producer {pid} on {topics}", False)
                      for pid, topics in sorted(found.items())]

    #: a producer that wrote this recently, in seconds, is writing now
    WRITER_WINDOW = 300
    #: a producer whose last write is no later than migkit's own last
    #: activity plus this, in seconds, is taken for migkit's (an earlier
    #: run's producers are listed for as long as the brokers keep them)
    OWN_SLACK = 60

    def _migkit_wrote_at(self, db):
        """When migkit last wrote into this hop's target, in milliseconds,
        as far as its own files say: the copy's checkpoint and the tail's
        position - 0 where neither is there."""
        from .. import tailctl
        last = 0.0
        for path in (self.hop.report_dir(db) / "move.json",
                     tailctl.token_path(self.hop, db)):
            try:
                last = max(last, path.stat().st_mtime)
            except OSError:
                continue
        return last * 1000

    def _producers(self, side, db, own=0):
        """{producer id: "topic[partition], ..."} of the producers active
        on the hop's topics on one side that wrote within the window and
        after migkit's own last write (`own`, `OWN_SLACK`), from the
        brokers' own producer state (DescribeProducers). A producer that is
        neither idempotent nor transactional leaves no state to name. Raises
        where the client or the cluster does not have the call."""
        from kafka import TopicPartition
        consumer = self._consumer(side)
        try:
            tps = [TopicPartition(t, p) for _, t in
                   self.list_move_tables(db)
                   for p in self._partitions(consumer, t)]
        finally:
            consumer.close()
        if not tps:
            return {}
        admin = self._admin(side)
        try:
            states = admin.describe_producers(tps)
        finally:
            admin.close()
        now = time.time() * 1000
        found = {}
        for tp, state in states.items():
            for pr in getattr(state, "active_producers", None) or ():
                last = getattr(pr, "last_timestamp", None)
                if last is None or last < 0 \
                        or now - last > self.WRITER_WINDOW * 1000 \
                        or last <= own + self.OWN_SLACK * 1000:
                    continue
                found.setdefault(pr.producer_id, []).append(
                    f"{tp.topic}[{tp.partition}]")
        return {pid: ", ".join(sorted(where)[:3])
                + (f" and {len(where) - 3} more" if len(where) > 3 else "")
                for pid, where in found.items()}

    #: the ACL operations that let a principal write a topic
    WRITING = ("WRITE", "ALL", "IDEMPOTENT_WRITE")

    def _writer_rows(self):
        """Who else writes to the hop's topics, as far as the cluster
        tells: the producers active on the source, the principals its ACLs
        let write them, the client quotas that throttle producers - and
        what it did not tell, said as unknown, never as none."""
        from .. import users
        items = []

        def add(level, item, detail):
            items.append({"level": level, "scope": "cluster", "item": item,
                          "detail": detail})
        # the target's producers are the base's row (`stream_writers`)
        try:
            found = self._producers("src", "cluster")
        except Exception as e:  # noqa: BLE001 - unknown, not none
            add("warn", "producers writing the source's topics",
                f"not readable ({type(e).__name__}: {str(e)[:70]}) -"
                " unknown, not clean")
        else:
            add("pass" if not found else "warn",
                "producers writing the source's topics",
                f"no idempotent or transactional producer wrote in the last"
                f" {self.WRITER_WINDOW // 60} minutes (one that is neither"
                " leaves no state the brokers can name)"
                if not found else
                "; ".join(f"producer {pid} on {where}" for pid, where
                          in sorted(found.items())[:6])
                + " - the tail follows what they write; they are stopped"
                  " at cutover")
        try:
            mine = [t for _, t in self.list_move_tables("cluster")]
        except Exception:  # noqa: BLE001 - the ACLs are then read whole
            mine = None
        for side, name in (("src", "source"), ("dst", "target")):
            try:
                got = users._kafka_accounts(self.hop, side)
            except Exception as e:  # noqa: BLE001 - unknown, not none
                add("warn", f"who may write the {name}'s topics",
                    f"not readable ({type(e).__name__}) - unknown, not"
                    " clean")
                continue
            unread = got.get("unread") or {}
            if got.get("acls") is None:
                add("warn", f"who may write the {name}'s topics",
                    "the ACLs could not be read ("
                    + str(unread.get("acls") or "no answer")[:80]
                    + ") - a cluster that checks none lets any client"
                      " write; unknown, not clean")
            else:
                who = sorted({a[0] for a in got["acls"]
                              if a[2] in self.WRITING and a[3] == "ALLOW"
                              and a[4] == "TOPIC"
                              and self._acl_names(a, mine)})
                add("pass", f"who may write the {name}'s topics",
                    f"{len(who)} principals: {', '.join(who[:6])}"
                    + (" and more" if len(who) > 6 else "")
                    if who else "no principal is allowed by an ACL")
            quotas = got.get("quotas")
            if quotas is None:
                if unread.get("quotas"):
                    add("warn", f"client quotas on the {name}",
                        f"not readable ({str(unread['quotas'])[:70]}) -"
                        " unknown, not clean")
                continue
            throttled = sorted(
                users._quota_said(e, "producer_byte_rate")
                for e, q in quotas.items() if "producer_byte_rate" in q)
            if throttled:
                add("pass", f"client quotas on the {name}",
                    "producers throttled: " + "; ".join(throttled[:4]))
        return items

    @staticmethod
    def _acl_names(acl, topics):
        """Whether an ACL (`users._acl_key`) is on a topic of the hop: its
        name is `*`, one of them, or a prefix of one. Every ACL where the
        topics are not known."""
        name, kind = acl[5], acl[6]
        if topics is None or name == "*":
            return True
        if kind == "PREFIXED":
            return any(t.startswith(name) for t in topics)
        return name in topics

    def free_bytes(self, side, db):
        """What the side's brokers have free on their disks, summed, as
        the brokers report it (DescribeLogDirs, brokers 3.3 and newer); None
        where they will not say."""
        total = 0
        found = False
        for broker in self._log_dirs(side):
            for d in broker.get("log_dirs") or ():
                if d.get("error_code") or d.get("usable_bytes") is None:
                    continue
                total += max(int(d["usable_bytes"]), 0)
                found = True
        return total if found else None

    def _log_dirs(self, side):
        """[{broker, log_dirs: [{log_dir, topics: [{name, partitions}],
        usable_bytes}]}] of one side, or [] where the client or the cluster
        does not say."""
        try:
            admin = self._admin(side)
        except Exception:  # noqa: BLE001 - not readable, said by the caller
            return []
        try:
            got = admin.describe_log_dirs()
        except Exception:  # noqa: BLE001 - not readable, said by the caller
            return []
        finally:
            admin.close()
        return got if isinstance(got, list) else []

    def _logical_bytes(self, side, topics):
        """The bytes the topics hold, one replica of each partition (the
        largest any broker has), or None where the brokers will not say."""
        sizes = {}
        for broker in self._log_dirs(side):
            for d in broker.get("log_dirs") or ():
                if d.get("error_code"):
                    continue
                for t in d.get("topics") or ():
                    for part in t.get("partitions") or ():
                        key = (t["name"], part["partition_index"])
                        sizes[key] = max(sizes.get(key, 0),
                                         int(part.get("partition_size") or 0))
        if not sizes:
            return None
        return sum(n for (t, _), n in sizes.items() if t in topics)

    def _room_rows(self):
        """Whether the target has the room for the topics, and the replicas
        of each, where the brokers say what they have free."""
        item = "room on the target"
        try:
            topics = {t for _, t in self.list_move_tables("cluster")}
            have = self.free_bytes("dst", "cluster")
            need = self._logical_bytes("src", topics)
        except Exception as e:  # noqa: BLE001 - unknown, not clean
            return [{"level": "warn", "scope": "cluster", "item": item,
                     "detail": f"not readable ({type(e).__name__}) -"
                               " unknown, not clean"}]
        if have is None or need is None:
            return [{"level": "warn", "scope": "cluster", "item": item,
                     "detail": "the "
                     + ("target's free disk" if have is None
                        else "source topics' sizes")
                     + " could not be read from the brokers (it needs"
                       " brokers 3.3 or newer and the right to describe log"
                       " directories) - unknown, not clean"}]
        from ..wording import human_bytes
        replicas = self._replicas()
        wanted = need * replicas
        level = ("fail" if have < wanted else
                 "warn" if have < wanted * 1.25 else "pass")
        return [{"level": level, "scope": "cluster", "item": item,
                 "detail": f"{human_bytes(have)} free on the target's"
                           f" brokers for {human_bytes(need)} of topics in"
                           f" {replicas} replicas ({human_bytes(wanted)})"
                 + (" - a load that fills a broker's disk can stop it"
                    if level != "pass" else "")}]

    def _handwork(self):
        """The work the move leaves to hands, counted and named
        (`migkit.handwork`): the topic settings it does not carry, and the
        cluster's accounts - SCRAM users, ACLs, client quotas - which move
        by `migkit users`, not by the copy. A probe that could not run is
        unknown, never none."""
        from .. import handwork, users
        inv = handwork.Inventory()
        db = "cluster"
        try:
            topics = [t for _, t in self.list_move_tables(db)]
            set_apart = self._settings_not_carried(topics)
        except Exception as e:  # noqa: BLE001 - unknown, not none
            inv.unknown("decide-then-apply", db,
                        f"the topics' settings could not be read: "
                        f"{type(e).__name__}")
        else:
            if set_apart is None:
                inv.unknown("decide-then-apply", db,
                            "the topics' settings could not be read from"
                            " the source")
            else:
                inv.add("decide-then-apply", db,
                        "topic settings the copy does not carry",
                        [f"{t}: {', '.join(names)}"
                         for t, names in sorted(set_apart.items())])
        try:
            got = users._kafka_accounts(self.hop, "src")
        except Exception as e:  # noqa: BLE001 - unknown, not none
            inv.unknown("not-carried", db,
                        f"the accounts could not be read: {type(e).__name__}")
        else:
            unread = got.get("unread") or {}
            said = {
                "users": lambda v: sorted(v),
                "acls": lambda v: sorted(users._acl_said(k) for k in v),
                "quotas": lambda v: sorted(
                    users._quota_said(e, k) for e, q in v.items()
                    for k in q)}
            for key, what in (("users", "SCRAM users"), ("acls", "ACLs"),
                              ("quotas", "client quotas")):
                if got.get(key) is None:
                    inv.unknown("not-carried", db,
                                f"the {what} could not be read: "
                                f"{str(unread.get(key) or 'no answer')[:70]}")
                else:
                    inv.add("not-carried", db,
                            f"{what} (carried by migkit users, not by the"
                            " copy)", said[key](got[key]))
        return inv

    def _settings_not_carried(self, topics):
        """{topic: [setting, ...]} of the settings the source's topics set
        themselves (a topic-level override) that the copy does not carry
        (`CRITICAL_CONFIGS` are), or None where the source will not say."""
        from kafka.admin import ConfigResource, ConfigResourceType
        out = {}
        admin = self._admin("src")
        try:
            for i in range(0, len(topics), 20):
                resp = admin.describe_configs(
                    [ConfigResource(ConfigResourceType.TOPIC, t)
                     for t in topics[i:i + 20]], config_filter="all")
                for by_name in resp.values():
                    for name, settings in by_name.items():
                        names = sorted(
                            k for k, v in settings.items()
                            if (v or {}).get("config_source")
                            == "DYNAMIC_TOPIC_CONFIG"
                            and k not in self.CRITICAL_CONFIGS)
                        if names:
                            out[str(name)] = names
        except Exception:  # noqa: BLE001 - unknown, said by the caller
            return None
        finally:
            admin.close()
        return out

    def check_schema(self, db):
        sc, dc = self._consumer("src"), self._consumer("dst")
        src = {t: len(self._partitions(sc, t)) for t in self._topics(sc)}
        dst = {t: len(self._partitions(dc, t)) for t in self._topics(dc)}
        bad = []
        for t in sorted(set(src) | set(dst)):
            if t not in dst:
                bad.append(f"missing topic {t}")
            elif t not in src:
                bad.append(f"extra topic {t}")
            elif src[t] != dst[t]:
                bad.append(f"{t} partitions src={src[t]} dst={dst[t]}")
        res = []
        if bad:
            res.append(Result("schema", "topics", "diff",
                              "; ".join(bad[:10]), "",
                              "create the missing topics on the target with"
                              " the source's partition counts before copying"))
        else:
            res.append(Result("schema", "topics", "ok", f"{len(src)} topics"))
        common = sorted(set(src) & set(dst))
        ca = self._topic_configs("src", common)
        cb = self._topic_configs("dst", common)
        if common and (ca is None or cb is None):
            # not a skip: a comparison that did not happen is said, or it
            # reads as one that found nothing
            res.append(Result("schema", "topic-configs", "error",
                              "the topics' settings could not be read on"
                              f" {'the source' if ca is None else 'the target'}"
                              " - retention and cleanup were not compared"))
        if ca is not None and cb is not None:
            drift = []
            for t in common:
                for k in self.CRITICAL_CONFIGS:
                    a, b = ca.get(t, {}).get(k), cb.get(t, {}).get(k)
                    if a != b:
                        drift.append(f"{t} {k} src={a} dst={b}")
            if drift:
                res.append(Result("schema", "topic-configs", "diff",
                                  "; ".join(drift[:8]), "",
                                  "kafka-configs --alter on target:"
                                  " cleanup.policy/retention drift changes"
                                  " topic semantics"))
            else:
                res.append(Result("schema", "topic-configs", "ok",
                                  f"{len(self.CRITICAL_CONFIGS)} critical"
                                  f" configs match on {len(common)} topics"))
        return res

    # --- offsets: kept identical where they can be, otherwise mapped -----
    #
    # A consumer group's committed offset is a position in one cluster's
    # log. The copy gives it the same meaning on the target in one of two
    # exact ways, and says which for every partition:
    #
    # * `identical` (LT.C2). A source partition whose offsets run one to
    #   one from its first - no compaction hole, no transaction marker, no
    #   aborted write - is copied to a target partition whose first offset
    #   was made the source's (`_partition_ready`): the target is padded
    #   with throwaway records up to the source's first offset and
    #   `DeleteRecords` then moves its start there; Kafka has no call that
    #   sets a log's first offset, only this one, which cannot go below the
    #   log's end. The copy is not transactional - a transaction's marker
    #   would take an offset - and every message's landed offset is read
    #   back and held to the source's. Where the offsets stop being one to
    #   one, the copy says where and goes on the other way.
    # * `mapped` (LT.C3), where the hop says `offsets: map` and the client
    #   can run transactions. Each batch and the record of the source and
    #   target offsets it took are written in one producer transaction, so
    #   a crash leaves both or neither, and the copy goes on from the map
    #   itself, not from a counter that can be a batch behind. The map is
    #   read back whole at a cutover (`_read_offset_map`) to place a
    #   group's offsets (`_located`).
    #
    # Everything else is `plain`, as the copy always was: positions placed
    # by the message, or by the header `offset_header` asks for.

    #: the target's topic of the offset map: one partition, kept for good
    OFFSET_MAP = "__migkit_offset_map"
    #: the words `offsets` may be
    OFFSET_WAYS = ("auto", "map", "off")
    #: padding records sent, and checked, in one round
    PAD_ROUND = 50_000

    def _offsets_way(self):
        way = str((self.hop.options or {}).get("offsets") or "auto").lower()
        if way not in self.OFFSET_WAYS:
            raise SystemExit(f"offsets: {way} is not one of"
                             f" {', '.join(self.OFFSET_WAYS)}")
        return way

    @staticmethod
    def _can_transact():
        """Whether the client can run a producer transaction: a client
        that cannot is not asked to, and a map is not claimed."""
        try:
            from kafka import KafkaProducer
        except ImportError:
            return False
        return callable(getattr(KafkaProducer, "init_transactions", None))

    def _gap_free(self, src, tp, beg, end):
        """(True, "") where every offset from `beg` up to `end` holds one
        message a reader of committed messages is given; (False, why) at
        the first that does not - a compaction hole, a transaction's
        marker or an aborted write. Read, not inferred: only the log can
        say. A partition not read to its end is not gap-free."""
        if end <= beg:
            return True, ""
        src.assign([tp])
        src.seek(tp, beg)
        want = beg
        while want < end:
            got = [m for msgs in src.poll(timeout_ms=5000, max_records=5000)
                   .values() for m in msgs if m.offset < end]
            if not got:
                return False, (f"offset {want:,} holds no message a reader"
                               " of committed messages is given (a"
                               " compaction hole, a transaction's marker or"
                               " an aborted write), or could not be read")
            for m in got:
                if m.offset != want:
                    return False, (f"offsets {want:,} to {m.offset - 1:,}"
                                   " hold no message (compaction, a"
                                   " transaction's marker or an aborted"
                                   " write)")
                want += 1
        return True, ""

    @staticmethod
    def choose_offsets(way, *, sbeg, dbeg, dend, gap_free=None, gap_why="",
                       can_pad=True, pad_why="", transactions=False):
        """(mode, why) a partition is copied in, from the facts alone.

        `identical` only where the target partition is empty and its next
        offset is not past the source's first (offsets cannot go back),
        padding it is possible, and the source has no gap: `gap_free` True
        (read) or None (not read - nothing to pad, so the copy proves each
        offset as it goes). Anything else is not claimed as identical, and
        is `mapped` where the hop asks for the map and the client can
        write it, `plain` otherwise - with the reason said."""
        def otherwise(why):
            if way == "map" and transactions:
                return "mapped", (why + "; offsets are not kept, the source"
                                  " to target offsets are written in the"
                                  " data's transactions")
            if way == "map":
                return "plain", (why + "; this client cannot run"
                                 " transactions, so no offset map is"
                                 " written either: group positions are"
                                 " placed by message")
            return "plain", (why + "; group positions are placed by message"
                             " (or by the position header)")
        if way == "off":
            return "plain", "offsets: off - copied without keeping offsets"
        if dend != dbeg:
            return otherwise(f"the target already holds {dend - dbeg:,}"
                             " messages in this partition")
        if dend > sbeg:
            return otherwise(f"the target's next offset {dend:,} is past the"
                             f" source's first {sbeg:,}")
        if dend < sbeg and not can_pad:
            return otherwise(pad_why)
        if gap_free is False:
            return otherwise(f"the source's offsets are not consecutive:"
                             f" {gap_why}")
        pad = sbeg - dend
        return "identical", (
            f"offsets kept identical from {sbeg:,}"
            + (f", the target padded with {pad:,} records and then cut to"
               f" start there (the source was read to its end: consecutive)"
               if pad else ", the target starting at the same offset; every"
               " offset is checked as it lands"))

    def _plan_offsets(self, src, dst, src_tp, dst_tp):
        """{"mode", "why", "first"} for a partition the copy is reaching:
        what `choose_offsets` says of what the clusters show. The cheap
        facts first; the source is read to its end for gaps only where a
        padding would be wasted by one."""
        way = self._offsets_way()
        sbeg = src.beginning_offsets([src_tp])[src_tp]
        send = src.end_offsets([src_tp])[src_tp]
        dbeg = dst.beginning_offsets([dst_tp])[dst_tp]
        dend = dst.end_offsets([dst_tp])[dst_tp]
        facts = dict(sbeg=sbeg, dbeg=dbeg, dend=dend,
                     transactions=self._can_transact())
        if way != "off" and dend == dbeg and dend < sbeg:
            limit = (self.hop.options or {}).get("offsets_pad_limit")
            if limit is not None and sbeg - dend > int(limit):
                facts.update(can_pad=False, pad_why=(
                    f"making its first offset {sbeg:,} needs {sbeg - dend:,}"
                    f" records of padding, past offsets_pad_limit {limit}"))
            else:
                ok, why = self._may_delete_records(dst_tp, dbeg)
                if not ok:
                    facts.update(can_pad=False, pad_why=why)
                else:
                    ok, why = self._gap_free(src, src_tp, sbeg, send)
                    facts.update(gap_free=ok, gap_why=why)
        mode, why = self.choose_offsets(way, **facts)
        return {"mode": mode, "why": why, "first": sbeg}

    def _may_delete_records(self, tp, offset):
        """(True, "") where the target lets migkit move a partition's start
        up, asked by a request that deletes nothing (up to the start it
        already has); (False, why) where it does not. Nothing is padded
        that could not then be cut."""
        try:
            admin = self._admin("dst")
        except Exception as e:  # noqa: BLE001 - said
            return False, f"the target could not be reached to cut its" \
                          f" padding ({type(e).__name__})"
        try:
            admin.delete_records({tp: offset})
        except Exception as e:  # noqa: BLE001 - said, not guessed
            return False, ("the target does not allow its records to be"
                           f" deleted ({type(e).__name__}: {str(e)[:60]}),"
                           " so it cannot be padded and cut")
        finally:
            admin.close()
        return True, ""

    def _carried(self, topic, p, m, marked):
        """What `producer.send` is given for source message `m`."""
        headers = list(m.headers or [])
        if marked:
            headers = [h for h in headers if h[0] != self.HEADER] + [
                (self.HEADER, self._position(p, m.offset))]
        return dict(value=self._translated(topic, "value", m.value),
                    key=self._translated(topic, "key", m.key),
                    headers=headers, partition=p, timestamp_ms=m.timestamp)

    # -- the map's own topic and records ----

    def _map_topic_ready(self):
        """The target's offset-map topic, made where it is missing: one
        partition, nothing ever expired (it is read at a cutover, however
        long after)."""
        if getattr(self, "_map_topic_made", False):
            return
        from kafka.admin import NewTopic
        dst = self._consumer("dst")
        try:
            if self.OFFSET_MAP in dst.topics():
                self._map_topic_made = True
                return
            brokers = len(dst._client.cluster.brokers()) or 1
        finally:
            dst.close()
        rf = int((self.hop.options or {}).get("replication_factor",
                                                min(3, brokers)))
        admin = self._admin("dst")
        try:
            admin.create_topics([NewTopic(
                self.OFFSET_MAP, num_partitions=1, replication_factor=rf,
                topic_configs={"retention.ms": "-1",
                               "cleanup.policy": "delete"})])
        finally:
            admin.close()
        self._map_topic_made = True

    @staticmethod
    def _map_record(kind, topic, p, **fields):
        """(key, value) of one record of the offset map. The key says what
        the record is about, so a record said twice is one."""
        import json
        value = {"v": 1, "kind": kind, "topic": topic, "partition": int(p),
                 **fields}
        key = "\x1f".join([kind, topic, str(int(p))]
                          + ([str(fields["from"])] if kind == "batch"
                             else []))
        return key.encode(), json.dumps(value, sort_keys=True).encode()

    def _write_map(self, producer, topic, p, kind, **fields):
        """One record of the map, outside a transaction (the identity
        records), written and checked."""
        self._map_topic_ready()
        key, value = self._map_record(kind, topic, p, **fields)
        sent = producer.send(self.OFFSET_MAP, key=key, value=value,
                             partition=0)
        producer.flush()
        self._refused(self.OFFSET_MAP, 0, [(0, sent)])

    @staticmethod
    def _runs(pairs):
        """[[source offset, target offset, how many]] from (source, target)
        pairs in order: consecutive on both sides is one run. Exact for any
        shape - a batch with holes is a run for each stretch."""
        runs = []
        for s, d in pairs:
            if runs and runs[-1][0] + runs[-1][2] == s \
                    and runs[-1][1] + runs[-1][2] == d:
                runs[-1][2] += 1
            else:
                runs.append([s, d, 1])
        return runs

    def _read_offset_map(self):
        """{(topic, partition): {"identical": (from, until or None) or
        None, "runs": [(source, target, n)], "next": the source position
        after the last committed batch or None}} - everything the map holds,
        read as a reader of committed records is given it, so a batch that
        was written and not committed is not in it. {} where the target has
        no map. A map not read to its end, or one that disagrees with
        itself, stops the caller: a position is never placed from part of
        it."""
        import json

        from kafka import TopicPartition
        consumer = self._consumer("dst", isolation_level="read_committed")
        try:
            if self.OFFSET_MAP not in consumer.topics():
                return {}
            tp = TopicPartition(self.OFFSET_MAP, 0)
            consumer.assign([tp])
            consumer.seek_to_beginning(tp)
            end = consumer.end_offsets([tp])[tp]
            out = {}
            idle = time.monotonic()
            while consumer.position(tp) < end:
                got = consumer.poll(timeout_ms=2000)
                if not got:
                    if time.monotonic() - idle > 120:
                        raise SystemExit(
                            "the offset map on the target could not be read"
                            " to its end: no position is placed from part of"
                            " it")
                    continue
                idle = time.monotonic()
                for msgs in got.values():
                    for m in msgs:
                        self._take_map_record(out, json.loads(m.value))
            return out
        finally:
            consumer.close()

    @staticmethod
    def _take_map_record(out, rec):
        if rec.get("v") != 1:
            raise SystemExit(f"the offset map holds a record of version"
                             f" {rec.get('v')}, which this migkit does not"
                             " read: nothing is placed from it")
        entry = out.setdefault((rec["topic"], rec["partition"]), {
            "identical": None, "runs": [], "next": None})
        if rec["kind"] == "identical":
            entry["identical"] = (rec["from"], rec.get("until"))
            return
        for s, d, n in rec["runs"]:
            runs = entry["runs"]
            if runs and s < runs[-1][0] + runs[-1][2]:
                raise SystemExit(
                    f"the offset map disagrees with itself at {rec['topic']}"
                    f"[{rec['partition']}] source offset {s:,}: it is in two"
                    " committed batches; nothing is placed from it")
            if runs and runs[-1][0] + runs[-1][2] == s \
                    and runs[-1][1] + runs[-1][2] == d:
                runs[-1] = (runs[-1][0], runs[-1][1], runs[-1][2] + n)
            else:
                runs.append((s, d, n))
        entry["next"] = rec["next"]

    @staticmethod
    def _map_position(entry, offset, dst_end):
        """(the target's offset of the first message at or after source
        `offset`, how), or None where the map does not know it: an offset
        past the last message copied, or one the target does not hold yet.
        Exact: nothing here is searched for."""
        import bisect
        ident = entry.get("identical")
        if ident and offset >= ident[0] \
                and (ident[1] is None or offset < ident[1]):
            if offset < dst_end:
                return offset, "identical offsets, kept by the copy"
            return None
        runs = entry.get("runs") or []
        starts = [r[0] for r in runs]
        i = bisect.bisect_right(starts, offset) - 1
        if i >= 0 and offset < runs[i][0] + runs[i][2]:
            s, d, _ = runs[i]
            return d + (offset - s), ("the offset map written in the"
                                      " data's transactions")
        if i + 1 < len(runs):
            return runs[i + 1][1], ("the offset map written in the data's"
                                    " transactions (the offset holds no"
                                    " message: the next one)")
        return None

    def _mapped_partitions(self):
        """{(topic, partition)} the target holds a map for: partitions
        whose offsets are not the source's, so offset arithmetic between
        the two sides says nothing about them."""
        try:
            return {k for k, e in self._read_offset_map().items()
                    if e["runs"]}
        except SystemExit:
            return set()

    # -- a partition made ready, and the ways a batch goes ----

    def _partition_ready(self, topic, p, pst, src, dst, src_tp, dst_tp, end,
                         producer_of, save, log):
        """A partition made ready to be copied further, each round: an
        identical one padded and cut where that was not done, and its
        position put where the target's own end says it is; a mapped one
        put where the map says. A plain one is left."""
        mode = pst.get("mode")
        if mode == "identical":
            self._identity_ready(topic, p, pst, src, dst, src_tp, dst_tp,
                                 end, producer_of, save, log)
        elif mode == "mapped":
            self._map_reconciled(topic, p, pst, save, log)

    def _identity_ready(self, topic, p, pst, src, dst, src_tp, dst_tp, end,
                        producer_of, save, log):
        first = pst["first"]
        dbeg = dst.beginning_offsets([dst_tp])[dst_tp]
        dend = dst.end_offsets([dst_tp])[dst_tp]
        if not pst.get("padded"):
            if dend > first or dbeg > first:
                raise SystemExit(
                    f"{topic}[{p}]: the target's log is at {dbeg:,}-{dend:,},"
                    f" past the source's first offset {first:,}, and the"
                    " copy has not begun: someone else wrote to it. Nothing"
                    " is copied, and no offset is claimed identical")
            producer = producer_of("ident")
            if dend < first:
                log(f"{topic}[{p}]: padding the target with"
                    f" {first - dend:,} records, to cut away")
                self._pad(producer, topic, p, dend, first)
                dend = dst.end_offsets([dst_tp])[dst_tp]
                if dend != first:
                    raise SystemExit(
                        f"{topic}[{p}]: after the padding the target's end is"
                        f" {dend:,}, not {first:,}: someone else wrote to it."
                        " The padding stays until it is cut; nothing is"
                        " copied")
            if dbeg < first:
                admin = self._admin("dst")
                try:
                    admin.delete_records({dst_tp: first})
                finally:
                    admin.close()
                dbeg = dst.beginning_offsets([dst_tp])[dst_tp]
                if dbeg != first:
                    raise SystemExit(
                        f"{topic}[{p}]: the target's first offset is"
                        f" {dbeg:,} after DeleteRecords up to {first:,}:"
                        " the offsets are not identical, and nothing is"
                        " copied")
            self._write_map(producer, topic, p, "identical", **{
                "from": first, "until": None})
            pst["padded"] = True
            save()
            return
        if dend == pst["next"]:
            return
        if dend < pst["next"] or dend > end:
            raise SystemExit(
                f"{topic}[{p}]: the target's end is {dend:,} where the copy"
                f" has reached source offset {pst['next']:,} (the source"
                f" ends at {end:,}): identical offsets cannot be vouched"
                " for. Nothing more is copied")
        # a batch reached the target and the position was not saved: it
        # is the last one's identical message, or the copy stops
        if not self._same_message(src, dst, src_tp, dend - 1, dend - 1):
            raise SystemExit(
                f"{topic}[{p}]: the target's message at {dend - 1:,} is not"
                " the source's: the copy stops, and no offset is claimed"
                " identical")
        pst["copied"] += dend - pst["next"]
        pst["next"] = dend
        save()

    def _pad(self, producer, topic, p, have, want):
        """Records sent to partition `p` until it ends at `want`, nothing
        taken as sent that the target did not accept. The key is set (a
        compacted topic takes no message without one)."""
        while have < want:
            n = min(want - have, self.PAD_ROUND)
            sends = [(have + i, producer.send(topic, key=b"migkit-pad",
                                              value=b"", partition=p))
                     for i in range(n)]
            producer.flush()
            self._refused(topic, p, sends)
            have += n

    def _copy_identical(self, topic, p, pst, batch, producer_of, marked, save,
                        log):
        """(the part of `batch` not copied this way, how many went):
        the consecutive head of the batch is sent without a transaction
        and each message's landed offset held to its own; where the
        offsets stop being consecutive, or a message lands elsewhere, the
        identity ends there and is said."""
        expect = pst["next"]
        head = []
        for m in batch:
            if m.offset != expect + len(head):
                break
            head.append(m)
        went = 0
        if head:
            producer = producer_of("ident")
            sends = [(m.offset, producer.send(
                topic, **self._carried(topic, p, m, marked))) for m in head]
            producer.flush()
            self._refused(topic, p, sends)
            for m, (_, sent) in zip(head, sends):
                at = sent.get(15).offset
                if at != m.offset:
                    pst["copied"] += went
                    pst["next"] = expect + went
                    self._write_map(producer, topic, p, "identical", **{
                        "from": pst["first"], "until": m.offset})
                    save()
                    raise SystemExit(
                        f"{topic}[{p}]: source offset {m.offset:,} landed at"
                        f" {at:,} on the target - another writer is on the"
                        " partition. Identical offsets hold below"
                        f" {m.offset:,} and are not claimed past it;"
                        " nothing more is copied")
                went += 1
            pst["copied"] += went
            pst["next"] = head[-1].offset + 1
            save()
        rest = batch[len(head):]
        if rest:
            self._identity_ends(
                topic, p, pst, producer_of, save, log,
                f"source offset {pst['next']:,} is followed by"
                f" {rest[0].offset:,}")
        return rest, went

    def _identity_ends(self, topic, p, pst, producer_of, save, log, why):
        """The identical offsets of a partition end at its position: said
        in the map, and the partition goes on the way the hop asks for
        (`mapped` where it can, `plain`)."""
        until = pst["next"]
        mode = "mapped" if self._offsets_way() == "map" \
            and self._can_transact() else "plain"
        self._write_map(producer_of("ident"), topic, p, "identical", **{
            "from": pst["first"], "until": until})
        pst["mode"] = mode
        # what the copy counts as landed from here is counted off the
        # target's own end, as a plain copy does
        pst["base"] = until - pst["copied"]
        pst["identical_until"] = until
        save()
        log(f"{topic}[{p}]: offsets are identical below {until:,} and not"
            f" from there ({why}); the rest is copied "
            + ("with its offsets written in the data's transactions"
               if mode == "mapped" else
               "without keeping them, group positions placed by message"
               + ("; this client cannot run transactions, so no offset map"
                  " is written either" if self._offsets_way() == "map"
                  else "")))

    def _copy_mapped(self, topic, p, pst, batch, producer, marked, save):
        """One batch and the map of where it landed, in one transaction:
        the target holds both or neither. Every send is checked before the
        offsets are read from what the target answered, and the map's record
        is the last thing sent before the commit. A failure ends the
        transaction (aborted) and stops the copy; a stop that never got to
        say anything leaves it open, and the next producer with the same id
        ends it before it writes."""
        self._map_topic_ready()
        claimed = self.__dict__.get("_claimed", {}).get((topic, p))
        if claimed is not None and batch[0].offset < claimed:
            raise SystemExit(
                f"{topic}[{p}]: the saved position was {claimed:,} but the"
                f" offset map on the target knows messages only before"
                f" {pst['next']:,}: the map is missing what was copied. No"
                " offset is placed from it; nothing more is copied")
        producer.begin_transaction()
        try:
            sends = [(m.offset, producer.send(
                topic, **self._carried(topic, p, m, marked))) for m in batch]
            producer.flush()
            self._refused(topic, p, sends)
            pairs = [(off, sent.get(15).offset) for off, sent in sends]
            following = batch[-1].offset + 1
            key, value = self._map_record(
                "batch", topic, p, **{
                    "from": batch[0].offset, "next": following,
                    "runs": self._runs(pairs)})
            written = producer.send(self.OFFSET_MAP, key=key, value=value,
                                    partition=0)
            producer.flush()
            self._refused(self.OFFSET_MAP, 0, [(batch[0].offset, written)])
            producer.commit_transaction()
        except (Exception, SystemExit):
            # what the target holds of this partition is read again from
            # the map before it is written to again
            self.__dict__.setdefault("_settled", set()).discard((topic, p))
            try:
                producer.abort_transaction()
            except Exception:  # noqa: BLE001 - the first failure is the news
                pass
            raise
        self.__dict__.setdefault("_claimed", {}).pop((topic, p), None)
        pst["copied"] += len(batch)
        pst["next"] = following
        save()
        return len(batch)

    def _map_reconciled(self, topic, p, pst, save, log):
        """The position of a mapped partition put where the map on the
        target says: once for each partition in this process, and again
        after any failure. The map is what a crash leaves exact - a batch
        is in it exactly when it is on the target - so a batch committed
        and not saved is not sent again, and a position saved past the
        map is not trusted."""
        settled = self.__dict__.setdefault("_settled", set())
        if (topic, p) in settled:
            return
        entry = self._read_offset_map().get((topic, p))
        known = entry["next"] if entry else None
        if known is not None and known != pst["next"]:
            if known > pst["next"]:
                landed = sum(n for s, d, n in entry["runs"]
                             if s >= pst["next"])
                pst["copied"] += landed
                log(f"{topic}[{p}]: {landed:,} messages were committed with"
                    " their offsets and the position was not saved: counted,"
                    " not sent again")
            else:
                # the saved position is past the map: markers only, or a
                # map that lost batches - the next message read says which
                self.__dict__.setdefault("_claimed", {})[(topic, p)] = \
                    pst["next"]
            pst["next"] = known
            save()
        settled.add((topic, p))

    def _groups(self, side):
        """Consumer group ids on one side.

        `list_groups()` answers with dicts on this client - measured,
        `{'group_id': 'billing', 'protocol_type': 'consumer', 'group_state':
        'Empty', ...}`. The old reading indexed them as tuples and called
        `list_consumer_groups`, which is a kafka-python 2.x name: against the
        client migkit installs, `check --check deep` raised `AttributeError:
        'KafkaAdminClient' object has no attribute 'list_consumer_groups'`
        before it compared anything at all.
        """
        admin = self._admin(side)
        try:
            groups = admin.list_groups()
        finally:
            admin.close()
        out = set()
        for group in groups:
            name = (group.get("group_id") if isinstance(group, dict)
                    else group[0])
            if name and not name.startswith("_"):
                out.add(name)
        return out

    def _group_offsets(self, side, group):
        """{TopicPartition: committed offset} for one group."""
        admin = self._admin(side)
        try:
            got = admin.list_group_offsets(group)
        finally:
            admin.close()
        per_group = got.get(group, got) if isinstance(got, dict) else {}
        return {tp: meta.offset for tp, meta in per_group.items()
                if not tp.topic.startswith("__") and meta.offset >= 0}

    def _comparable(self, tps):
        """The partitions whose offsets mean the same thing on both sides.

        An offset is a position in one cluster's log, and two clusters only
        agree on what a number means when their logs start and end in the
        same place. Copying a committed offset between clusters that do not
        is how a consumer is silently moved past messages it never read, so
        migkit checks first and says which partitions it could not vouch for.
        """
        sc, dc = self._consumer("src"), self._consumer("dst")
        same, apart = [], []
        for tp in tps:
            try:
                ends = (sc.end_offsets([tp])[tp], dc.end_offsets([tp])[tp])
                begins = (sc.beginning_offsets([tp])[tp],
                          dc.beginning_offsets([tp])[tp])
            except Exception:
                apart.append(tp)
                continue
            (same if ends[0] == ends[1] and begins[0] == begins[1]
             else apart).append(tp)
        return same, apart

    #: target messages read, from the first at the committed message's
    #: time, looking for that message, at most
    TRANSLATE_SCAN = 1000

    def _message_at(self, consumer, tp, offset, side="src"):
        """(time, key, value) of the message at `offset`, or None - key and
        value as they compare across clusters (`_as_compared`)."""
        consumer.assign([tp])
        consumer.seek(tp, offset)
        end = time.time() + 15
        while time.time() < end:
            for msgs in consumer.poll(timeout_ms=2000).values():
                for m in msgs:
                    if m.offset >= offset:
                        return (m.timestamp,
                                self._as_compared(side, m.key),
                                self._as_compared(side, m.value))
        return None

    def _find(self, consumer, tp, message, side="dst"):
        """The target's offset of `message`: from the first offset at its
        time, the first message with its key and value. None where it is
        not there within `TRANSLATE_SCAN` messages."""
        at, key, value = message
        start = consumer.offsets_for_times({tp: at}).get(tp)
        if start is None:
            return None
        consumer.assign([tp])
        consumer.seek(tp, start.offset)
        seen = 0
        while seen < self.TRANSLATE_SCAN:
            batch = consumer.poll(timeout_ms=3000)
            if not batch:
                return None
            for msgs in batch.values():
                for m in msgs:
                    seen += 1
                    if (self._as_compared(side, m.key) == key
                            and self._as_compared(side, m.value) == value):
                        return m.offset
        return None

    #: the header migkit's copier writes each message's source position in,
    #: where the hop asks for it (`offset_header`): `<partition>:<offset>`
    HEADER = "migkit.source.offset"

    def _marks_positions(self):
        """Whether the hop asks each copied message to carry its source
        position (`offset_header`): a header more, which its consumers see,
        so only on the hop's word."""
        return bool((self.hop.options or {}).get("offset_header"))

    @staticmethod
    def _position(partition, offset):
        return f"{int(partition)}:{int(offset)}".encode()

    @classmethod
    def _position_of(cls, headers):
        """(partition, offset) a message's header says it was copied from,
        or None."""
        for name, value in headers or ():
            if name == cls.HEADER and value:
                try:
                    p, o = bytes(value).decode().split(":", 1)
                    return int(p), int(o)
                except (ValueError, UnicodeDecodeError):
                    return None
        return None

    def _by_header(self, dc, tp, offset, at):
        """The target's offset of the message copied from source `offset`
        of this partition, by the position its header carries: from the
        first message at its time (`at`), the first whose header says that
        offset or a later one - a transaction's marker takes an offset no
        message has. None where no message there carries the header."""
        start = dc.offsets_for_times({tp: at}).get(tp) if at is not None \
            else None
        dc.assign([tp])
        if start is None:
            dc.seek_to_beginning(tp)
        else:
            dc.seek(tp, start.offset)
        seen = 0
        while seen < self.TRANSLATE_SCAN:
            batch = dc.poll(timeout_ms=3000)
            if not batch:
                return None
            for msgs in batch.values():
                for m in msgs:
                    seen += 1
                    got = self._position_of(m.headers)
                    if got and got[0] == tp.partition and got[1] >= offset:
                        return m.offset
                    if got is None and at is not None and m.timestamp > at:
                        # past its time with no header: these messages
                        # were not copied with their positions
                        return None
        return None

    def _same_message(self, sc, dc, tp, offset, there):
        """Whether target offset `there` holds the source's message at
        `offset`: time, key and value alike."""
        a = self._message_at(sc, tp, offset)
        return a is not None and self._message_at(dc, tp, there, "dst") == a

    def _located(self, sc, dc, tp, offset, tables, group):
        """(the target's offset of the source's message at `offset`, how it
        was found), or (None, why). By the offset map the copier wrote on
        the target - identical offsets, or the source and target offsets
        of each batch, exact and not searched for; by the header the
        copier wrote where
        the target's messages carry one; by the cluster mirror's own tables
        where it copied them (`mirror.read_table`) - a checkpoint of this
        very group at this very offset, or the offset syncs around it - each
        proved by the message before it is taken; by the message itself
        (its time, key and value) otherwise."""
        from .. import mirror
        kept = (tables.get("map") or {}).get((tp.topic, tp.partition))
        if kept is not None:
            got = self._map_position(kept, offset,
                                     dc.end_offsets([tp])[tp])
            if got is not None:
                return got
        message = self._message_at(sc, tp, offset)
        if message is None:
            return None, "the message it reads next could not be read"
        at = self._by_header(dc, tp, offset, message[0])
        if at is not None:
            return at, "by the source position each message carries"
        cands = []
        point = (tables.get("checkpoints") or {}).get(
            (group, tp.topic, tp.partition))
        if point and point[0] == offset:
            cands.append(point[1])
        guess = mirror.downstream((tables.get("syncs") or {}).get(
            (tp.topic, tp.partition)), offset)
        if guess is not None:
            cands.append(guess)
        for there in dict.fromkeys(cands):
            if there >= 0 and self._message_at(dc, tp, there, "dst") \
                    == message:
                return there, "by the cluster mirror's record of where it" \
                              " landed, proved by the message"
        found = self._find(dc, tp, message)
        if found is not None:
            return found, "by the message it points at"
        return None, "the message it reads next is not on the target"

    def _place(self, sc, dc, tp, offset, tables=None, group=None):
        """(where on the target a group committed at source `offset` goes,
        how it was found, what was clamped) - or (None, why, "") where
        there is nowhere to put it.

        A committed offset is the next message the group reads. It goes to
        the target's offset of that same message; where the target does
        not hold it, right after the last message the group had read, if
        the target holds that one - never before what the group consumed,
        so it reads nothing twice, and where the next ones have not reached
        the target, at its end, so it skips none of them when they do. The
        result is clamped to what the target holds (backlog F7.kclamp):
        never past its end and never before its start, whichever way it
        was found - and a clamp is said. A position older than what the
        source still holds has no message to be found by, and is not
        guessed at: where its reader goes is its own reset policy's."""
        tables = tables or {}
        beg = sc.beginning_offsets([tp])[tp]
        end = sc.end_offsets([tp])[tp]
        note = ""
        if offset < beg:
            return None, "its position is older than the source's log", ""
        if end <= beg:
            return None, "the partition is empty on the source", ""
        if offset >= end:
            last, how = self._located(sc, dc, tp, end - 1, tables, group)
            if last is None:
                return None, "the source's last message is not on the" \
                             " target", ""
            at = last + 1
        else:
            at, how = self._located(sc, dc, tp, offset, tables, group)
            if at is None and offset > beg:
                read, how2 = self._located(sc, dc, tp, offset - 1, tables,
                                           group)
                if read is not None:
                    at, how = read + 1, how2 + ", after the last it read"
                    note = (note + "; " if note else "") + (
                        f"the target holds up to the last message it read,"
                        f" and the {end - offset:,} it reads next have not"
                        " reached the target yet")
            if at is None:
                return None, how, ""
        dbeg = dc.beginning_offsets([tp])[tp]
        dend = dc.end_offsets([tp])[tp]
        if at > dend:
            note = (note + "; " if note else "") + (
                f"clamped to the target's end {dend}: {at - dend} of the"
                " messages it had read are not on the target")
            at = dend
        if at < dbeg:
            note = (note + "; " if note else "") + (
                f"clamped to the target's first message {dbeg}")
            at = dbeg
        return at, how, note

    def group_positions(self, db):
        """([row], [unsure], checked): every group committed on the source,
        partition by partition, as it goes on the target - row {"group",
        "topic", "partition", "source", "target", "held", "how", "note"},
        `held` the target's committed offset now - and the partitions with
        nowhere to go, each with why. Where both logs start and end
        together the number means the same message and is taken as it is;
        elsewhere it is placed by the message (`_place`). Raises where the
        groups cannot be listed: none listed is not the same as none."""
        from .. import mirror
        rows, unsure, checked = [], [], 0
        groups = self._groups("src") | self._groups("dst")
        sc = dc = None
        tables = None
        # the offset map the copy wrote on the target, read whole or not
        # at all: a position is never placed from part of it
        kept = self._read_offset_map()
        try:
            for group in sorted(groups):
                try:
                    offs_s = self._group_offsets("src", group)
                except Exception:  # noqa: BLE001 - said, not guessed
                    unsure.append(f"{group} (its offsets could not be read)")
                    continue
                if not offs_s:
                    continue
                try:
                    offs_d = self._group_offsets("dst", group)
                except Exception:  # noqa: BLE001 - the target has none
                    offs_d = {}
                checked += 1
                same, apart = self._comparable(list(offs_s))
                # equal starts and ends on a partition copied with a map
                # are a coincidence of markers and holes, not one message
                same, apart = (
                    [tp for tp in same
                     if not (kept.get((tp.topic, tp.partition)) or {})
                     .get("runs")],
                    apart + [tp for tp in same
                             if (kept.get((tp.topic, tp.partition)) or {})
                             .get("runs")])
                for tp in same:
                    rows.append({"group": group, "topic": tp.topic,
                                 "partition": tp.partition,
                                 "source": offs_s[tp], "target": offs_s[tp],
                                 "held": offs_d.get(tp), "how": "number",
                                 "note": ""})
                for tp in apart:
                    if sc is None:
                        sc, dc = self._consumer("src"), self._consumer("dst")
                    if tables is None:
                        tables = {k: mirror.read_table(self, k)
                                  for k in ("syncs", "checkpoints")}
                        tables["map"] = kept
                    try:
                        there, how, note = self._place(sc, dc, tp,
                                                       offs_s[tp], tables,
                                                       group)
                    except Exception as e:  # noqa: BLE001 - said
                        there, how, note = (None, f"{type(e).__name__}:"
                                                  f" {str(e)[:60]}", "")
                    if there is None:
                        unsure.append(f"{group} {tp.topic}[{tp.partition}]"
                                      f" ({how})")
                        continue
                    rows.append({"group": group, "topic": tp.topic,
                                 "partition": tp.partition,
                                 "source": offs_s[tp], "target": there,
                                 "held": offs_d.get(tp), "how": how,
                                 "note": note})
        finally:
            for consumer in (sc, dc):
                if consumer is not None:
                    consumer.close()
        return rows, unsure, checked

    def check_deep(self, db):
        """Consumer-group parity: the classic kafka-migration failure is
        moving the data but not the committed offsets - every consumer
        restarts from earliest/latest and double-processes or drops.

        What it finds is written down the way every other engine writes it,
        so `migkit sync --kind sequences` can put it right: a committed
        offset is a counter that has to follow the data, which is the same
        thing a sequence is.
        """
        import json
        try:
            gs = self._groups("src")
            gd = self._groups("dst")
        except Exception as e:
            return [Result("deep", "groups", "error",
                           f"{type(e).__name__}: {str(e)[:110]} - the groups"
                           " could not be listed, which is not the same as"
                           " there being none")]
        missing = sorted(gs - gd)
        res = []
        if missing:
            res.append(Result("deep", "groups", "diff",
                              f"{len(missing)} consumer groups missing on"
                              f" target: {', '.join(missing[:6])}", "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences --apply, which puts each"
                              " group at the same message on the target"))
        else:
            res.append(Result("deep", "groups", "ok",
                              f"{len(gs)} consumer groups present on"
                              " target"))
        rows, unsure, checked = self.group_positions(db)
        behind, changed = [], []
        translated = sum(1 for r in rows if r["how"] != "number")
        for r in rows:
            if r["held"] == r["target"]:
                continue
            said = (f"{r['group']} {r['topic']}[{r['partition']}]"
                    f" src={r['source']} dst={r['held']}")
            if r["how"] != "number":
                said += (f" (the same message is at {r['target']} on the"
                         " target)")
            if r["note"]:
                said += f" ({r['note']})"
            behind.append(said)
            changed.append(json.dumps(
                {"group": r["group"], "topic": r["topic"],
                 "partition": r["partition"], "offset": r["target"]}))
        self._write_drill(db, "groups", missing=missing, changed=changed)
        # a pass has to say what it did not look at, or a group with one
        # untranslatable partition reads as a group that was checked
        aside = (f" ({len(unsure)} partitions not comparable, below)"
                 if unsure else "")
        res.append(Result(
            "deep", "group-offsets", "diff" if behind else "ok",
            "; ".join(behind[:6]) + aside if behind
            else f"{checked} groups, every comparable committed offset"
                 f" matches"
                 + (f" ({translated} found by the message they point at)"
                    if translated else "") + aside, "",
            f"migkit sync {self.hop.name} --db {db} --kind sequences --apply"
            if behind else ""))
        if unsure:
            res.append(Result(
                "deep", "group-offsets not comparable", "warn",
                f"{len(unsure)} partitions where an offset does not mean the"
                f" same thing on both sides: {', '.join(unsure[:6])} - the"
                " two logs do not start and end together, and the message"
                " the group reads next was not found on the target, so"
                " migkit will not copy a number between them", "",
                "copy the topic again so the target holds the message the"
                " group reads next, or reset the target's groups"
                " deliberately with kafka-consumer-groups --reset-offsets"))
        return res

    def repair_plan(self, db, kind):
        """Committed offsets are the counters this engine can put right.

        Not the messages: re-producing them would give them new offsets and
        new timestamps, and a topic is not a table that can be written into
        twice. Each group is placed afresh from where it stands on the
        source now (`group_positions`) - at a cutover the source has just
        been frozen, and a position read at the last check would be stale -
        clamped to what the target holds. The action's rows after its first
        line are what `apply` commits; its undo, the target's offsets as
        they were, is what a cutover that goes back commits again.
        """
        if kind not in ("sequences", "all"):
            return []
        import json
        rows, unsure, _ = self.group_positions(db)
        todo = [r for r in rows if r["held"] != r["target"]]
        if not todo:
            return []
        groups = sorted({r["group"] for r in todo})
        clamped = [f"{r['group']} {r['topic']}[{r['partition']}]:"
                   f" {r['note']}" for r in todo if r["note"]]
        return [RepairAction(
            db, "sequences",
            [f"commit {len(todo)} offsets on the target for"
             f" {len(groups)} groups: {', '.join(groups[:6])}"
             + (" ..." if len(groups) > 6 else "")]
            + [json.dumps({"group": r["group"], "topic": r["topic"],
                           "partition": r["partition"],
                           "offset": r["target"]}) for r in todo],
            [json.dumps({"group": r["group"], "topic": r["topic"],
                         "partition": r["partition"], "offset": r["held"]})
             for r in todo],
            "where the two logs do not line up, each offset is the one"
            " of the same message on the target; the target's current"
            " offsets go to the undo file first"
            + (f"; clamped: {'; '.join(clamped[:4])}" if clamped else "")
            + (f"; {len(unsure)} partitions with nowhere to go are left"
               f" as they are: {', '.join(unsure[:4])}" if unsure else ""))]

    def apply(self, db, action):
        """Set the target's committed offsets: the rows the action carries
        (`repair_plan`, or a cutover's undo of it), else the ones the last
        deep check wrote down.

        Measured: `alter_group_offsets` creates the group when it is not
        there, which is what a migrated cluster needs, and reports per
        partition - `{TopicPartition: NoError}` - so the result is read
        rather than assumed. A row with no offset is a group the target did
        not have: going back, that group is removed again, once none of its
        partitions has an offset to put back.
        """
        import json

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        carried = [json.loads(s) for s in (action.statements or [])
                   if str(s).startswith("{")]
        rows = carried or [json.loads(line) for line in
                           self._read_drill(db, "groups", "changed")]
        if not rows:
            return
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        by_group, gone = {}, {}
        for row in rows:
            tp = TopicPartition(row["topic"], row["partition"])
            if row.get("offset") is None:
                gone.setdefault(row["group"], []).append(tp)
            else:
                by_group.setdefault(row["group"], {})[tp] = row["offset"]
        admin = self._admin("dst")
        try:
            with (undo_dir / "group-offsets.jsonl").open("a") as handle:
                for group, wanted in sorted(by_group.items()):
                    held = self._group_offsets("dst", group)
                    for tp in wanted:
                        handle.write(json.dumps(
                            {"group": group, "topic": tp.topic,
                             "partition": tp.partition,
                             "offset": held.get(tp)}) + "\n")
                    got = admin.alter_group_offsets(
                        group, {tp: OffsetAndMetadata(offset, "", -1)
                                for tp, offset in wanted.items()})
                    bad = [f"{tp.topic}[{tp.partition}]: {err.__name__}"
                           for tp, err in (got or {}).items()
                           if err is not None
                           and getattr(err, "__name__", "") != "NoError"]
                    if bad:
                        raise SystemExit(
                            f"the target refused offsets for {group}:"
                            f" {'; '.join(bad[:6])}. Offsets committed"
                            " before this one stand; nothing was deleted")
            for group in sorted(g for g in gone if g not in by_group):
                admin.delete_consumer_groups([group])
        finally:
            admin.close()

    def check_counts(self, db):
        from kafka import TopicPartition
        sc, dc = self._consumer("src"), self._consumer("dst")
        bad = []
        n = 0
        total_a = total_b = 0
        mapped = self._mapped_partitions()
        apart = []
        for t in self._topics(sc):
            tps = [TopicPartition(t, p) for p in self._partitions(sc, t)]
            if any((t, tp.partition) in mapped for tp in tps):
                # the target's offsets carry transaction markers the source
                # has not: arithmetic on them says nothing
                apart.append(t)
                continue
            if not tps:
                continue
            a = sum(sc.end_offsets(tps)[tp] - sc.beginning_offsets(tps)[tp]
                    for tp in tps)
            if t not in dc.topics():
                bad.append(f"{t} missing on target")
                continue
            b = sum(dc.end_offsets(tps)[tp] - dc.beginning_offsets(tps)[tp]
                    for tp in tps)
            n += 1
            total_a += a
            total_b += b
            if a != b:
                bad.append(f"{t} messages src={a} dst={b}")
        res = []
        if apart:
            res.append(Result(
                "counts", "offset-map topics", "warn",
                f"{len(apart)} topics are copied with an offset map, their"
                " offsets not the source's, so end minus start is not their"
                " message count and was not compared: "
                + ", ".join(apart[:10]), "", "compare their messages by content (check --check data)"
                " until a count by the map is built"))
        if bad:
            return res + [Result(
                "counts", "messages", "diff", "; ".join(bad[:10]), "",
                "counts net of retention: small gaps are normal"
                " while mirror lags, big gaps mean missing data")]
        return res + [Result("counts", "messages", "ok",
                             f"{n} topics, messages {total_a:,}=={total_b:,}"
                             " (net of retention)")]

    def check_data(self, db, table=None, stream=None):
        from kafka import TopicPartition

        from ..throttle import Throttle
        sample = int(self.hop.options.get("sample", 200))
        sc, dc = self._consumer("src"), self._consumer("dst")
        topics = [table] if table else self._topics(sc)
        bad = []
        unread = []
        checked = 0
        # Fetching the tail of every partition on both clusters is the
        # heaviest thing this check does, and brokers are shared with whatever
        # else produces and consumes. Kafka publishes its own load over JMX,
        # which a client cannot reach, so there is no health probe here and
        # the throttle's latency signal carries it: if the fetches slow to
        # several times this run's fastest, the check waits instead of
        # pulling harder. The replication state below is reported as a
        # finding rather than used as a brake - a cluster missing a broker is
        # exactly when a quick honest answer matters most.
        gate = Throttle(1)
        mapped = self._mapped_partitions()
        skipped = []
        for t in topics:
            parts = self._partitions(sc, t)
            if not parts:
                unread.append((t, None, "no partitions on the source"))
                continue
            for p in parts:
                if (t, p) in mapped:
                    skipped.append(f"{t}[{p}]")
                    continue
                tp = TopicPartition(t, p)
                with gate.unit():
                    a, ea = self._tail_hash(sc, tp, sample, "src")
                    b, eb = self._tail_hash(dc, tp, sample, "dst")
                if ea or eb:
                    for side, err in (("source", ea), ("target", eb)):
                        if err:
                            unread.append((t, p, f"{side}: {err}"))
                    if stream:
                        stream(f"{t}[{p}]: unreadable")
                    continue
                checked += 1
                if stream:
                    stream(f"{t}[{p}]: {'ok' if a == b else 'DIFF'}")
                if a != b:
                    bad.append(f"{t}[{p}]")
        healed = self._confirmed(db, bad, stream) if bad else []
        bad = [b for b in bad if b not in healed]
        res = []
        if unread:
            named = "; ".join(f"{t}[{p}] {why}" if p is not None
                              else f"{t} {why}" for t, p, why in unread[:6])
            distinct = len({(t, p) for t, p, _ in unread})
            res.append(Result(
                "data", "tail-sample", "error",
                f"{distinct} partition{'' if distinct == 1 else 's'} could not"
                f" be read: {named}"
                + self._why_unreadable(unread), "",
                "a partition with no leader cannot be read at all - start the"
                " broker holding it and re-run; an unread partition is not a"
                " partition that matched"))
        if bad:
            res.append(Result("data", "tail-sample", "diff",
                              f"content differs in: {', '.join(bad[:10])}", "",
                              "re-mirror those topics, verify consumer-group"
                              " checkpoints before cutover"))
        if not res and not checked and not skipped:
            res.append(Result("data", "tail-sample", "error",
                              "no partitions were compared at all, which is"
                              " not the same as every partition matching"))
        elif not res and checked:
            res.append(Result("data", "tail-sample", "ok",
                              f"last {sample} messages hash-equal on"
                              f" {checked} partitions"
                              + (f"; {', '.join(healed)} differed while"
                                 " their changes were still arriving"
                                 if healed else "")))
        if skipped:
            res.append(Result(
                "data", "tail-sample offset-map", "warn",
                f"{len(skipped)} partitions are copied with an offset map:"
                " their messages are not at the source's offsets, so a tail"
                " window by offset is not the same messages on both sides"
                f" and was not compared ({', '.join(skipped[:6])})", "",
                "not verified for these partitions until a comparison by"
                " the map is built"))
        return res

    def _confirmed(self, db, bad, stream=None):
        """The partitions of `bad` (`topic[p]`) that were only behind: where
        a tail is running, it is waited on to reach the source's end and
        each differing partition compared again (the base's confirm pass);
        with none, what the hop's `settle` allows."""
        per = {}
        for b in bad:
            t, p = b[:-1].rsplit("[", 1)
            per.setdefault(t, []).append(p)
        for t, parts in per.items():
            self._write_pk_files(db, self.DRILL + t, [], [], parts)
        _, healed, _ = self._resolve_inflight(
            db, [self.DRILL + t for t in per], stream)
        out = []
        for name in healed:
            t = name[len(self.DRILL):]
            out += [f"{t}[{p}]" for p in per[t]]
        return out

    def _why_unreadable(self, unread):
        """Turn the client's exception into what the cluster says is wrong.

        `end_offsets` on a partition with no leader raises a bare
        `KeyError(TopicPartition(...))` - not a Kafka error class, and nothing
        a reader of the report could act on. The cluster itself is explicit:
        measured with one broker of two stopped, the partitions whose only
        replica lived there came back from `describe_topics` with
        `leader_id: -1`, `error_code: 5` and `offline_replicas: [3]`.
        """
        seen = {}
        for topic, part, _ in unread:
            if part is None:
                continue
            if topic not in seen:
                seen[topic] = self._partition_state("src", topic)
            why = seen[topic].get(part)
            if why:
                return f" - the cluster reports {topic}[{part}] {why}"
        return ""

    def _partition_state(self, side, topic):
        """Each troubled partition of one topic, as the cluster describes it.

        Measured with a broker stopped: an offline partition still lists the
        broker that is gone in `isr_nodes`, so in-sync-against-replicas does
        not find it. The leader does - `leader_id` is -1 - and that is the
        difference between a partition that is behind and one that is not
        there at all.
        """
        out = {}
        try:
            admin = self._admin(side)
            try:
                described = admin.describe_topics([topic])
            finally:
                admin.close()
        except Exception:
            return out
        for described_topic in described:
            for p in described_topic.get("partitions", []):
                idx = p.get("partition_index")
                replicas = list(p.get("replica_nodes") or [])
                isr = list(p.get("isr_nodes") or [])
                offline = list(p.get("offline_replicas") or [])
                if p.get("leader_id", -1) < 0:
                    out[idx] = ("has no leader"
                                + (f", replicas offline on broker(s)"
                                   f" {offline}" if offline else ""))
                elif offline or set(isr) < set(replicas):
                    out[idx] = (f"is under-replicated: in-sync {isr} of"
                                f" replicas {replicas}")
        return out

    def _tail_hash(self, consumer, tp, n, side=None):
        """(digest, error) for the last n messages of one partition.

        The error is handed back rather than folded into the digest. It used
        to be returned as the string `unavailable` in the digest's place, so
        two partitions nobody could read compared equal and the check
        reported the last messages hash-equal on both sides. Measured against
        a cluster with a broker stopped, this is not a hypothetical path:
        asking for the offsets of a partition whose leader is gone raises a
        bare `KeyError(TopicPartition(...))` while the client's metadata is
        still stale, and `KafkaTimeoutError` after the request timeout once it
        has caught up. Neither is a Kafka error class a caller could match on,
        which is why the text is carried out as it is.
        """
        try:
            consumer.assign([tp])
            end = consumer.end_offsets([tp])[tp]
            beg = consumer.beginning_offsets([tp])[tp]
        except Exception as e:
            # some of these already begin with their own class name
            text = str(e)[:80] or repr(e)
            name = type(e).__name__
            return None, text if text.startswith(name) else f"{name}: {text}"
        return self._window_hash(consumer, tp, max(beg, end - n), end,
                                 side), ""

    def _window_hash(self, consumer, tp, start, end, side=None):
        """`count|digest` of the messages of one partition from `start` up
        to, not including, `end` - key and value as they compare across
        clusters when `side` is given."""
        if start >= end:
            return "empty"
        consumer.assign([tp])
        consumer.seek(tp, start)
        h = hashlib.md5(usedforsecurity=False)
        got = 0
        while got < end - start:
            batch = consumer.poll(timeout_ms=5000)
            if not batch:
                break
            for msgs in batch.values():
                for m in msgs:
                    if m.offset >= end:
                        break
                    h.update(self._as_compared(side, m.key) or b""
                             if side else m.key or b"")
                    h.update(self._as_compared(side, m.value) or b""
                             if side else m.value or b"")
                    got += 1
        return f"{got}|{h.hexdigest()}"

    # --- the confirm pass (base `fenced_recheck`) ---------------------------
    #: the drilldown of a topic's differing partitions is named apart from
    #: the consumer groups' own (`data-groups.*`), which a topic called
    #: `groups` would otherwise have written over
    DRILL = "topic-"

    def _compare_pks(self, db, table, keys):
        """(missing, extra, changed) among these partitions of one topic,
        read again now: the source's last `sample` messages up to where the
        fence stood (its end now, without one), against the target's
        messages ending at the same message - found by its time, key and
        value, since offsets do not line up across clusters. What the
        target holds past that message must be what the source wrote after
        it, in order: a message the source never wrote is found there, not
        left outside the window. A partition whose last source message is
        not on the target is missing."""
        from kafka import TopicPartition
        topic = table[len(self.DRILL):]
        sample = int(self.hop.options.get("sample", 200))
        at = (getattr(self, "_fenced_at", None) or {}).get(topic, {})
        sc, dc = self._consumer("src"), self._consumer("dst")
        missing, extra, changed = [], [], []
        try:
            for p in sorted(keys, key=int):
                tp = TopicPartition(topic, int(p))
                beg = sc.beginning_offsets([tp])[tp]
                end = int(at.get(str(p), sc.end_offsets([tp])[tp]))
                if end <= beg:
                    if dc.end_offsets([tp])[tp] > \
                            dc.beginning_offsets([tp])[tp]:
                        extra.append(p)
                    continue
                last = self._message_at(sc, tp, end - 1)
                found = self._find(dc, tp, last) if last else None
                if found is None:
                    missing.append(p)
                    continue
                dbeg = dc.beginning_offsets([tp])[tp]
                a = self._window_hash(sc, tp, max(beg, end - sample), end,
                                      "src")
                b = self._window_hash(dc, tp, max(dbeg, found + 1 - sample),
                                      found + 1, "dst")
                after = self._messages(dc, tp, found + 1, sample, "dst")
                if a != b or (after and self._messages(
                        sc, tp, end, len(after), "src") != after):
                    changed.append(p)
        finally:
            sc.close()
            dc.close()
        return missing, extra, changed

    def _messages(self, consumer, tp, start, most, side):
        """Up to `most` messages of one partition from `start` to its end
        now, as (key, value) as they compare across clusters. Read by
        message, not by offset: a transaction's marker takes an offset and
        is never handed to a reader."""
        end = consumer.end_offsets([tp])[tp]
        if start >= end or most <= 0:
            return []
        consumer.assign([tp])
        consumer.seek(tp, start)
        out = []
        while len(out) < most:
            batch = consumer.poll(timeout_ms=3000)
            if not batch:
                break
            for msgs in batch.values():
                out += [(self._as_compared(side, m.key),
                         self._as_compared(side, m.value))
                        for m in msgs if m.offset < end]
            if msgs and msgs[-1].offset >= end - 1:
                break
        return out[:most]

    def _write_pk_files(self, db, table, missing, extra, changed):
        self._write_drill(db, table, missing=missing, extra=extra,
                          changed=changed)

    def delta_verify(self, db, limit=20000, log=None):
        """Offset-based delta: track each partition's end offset; the new
        messages since the baseline are re-hashed on both sides (offsets
        aren't preserved across clusters, so content, not offset numbers,
        is the check). Baseline advances only on a clean cycle.

        A partition whose offsets cannot be read is the case worth being
        careful about. It used to be dropped from the reading silently, and
        because the file written back is the reading, its old baseline was
        erased with it: when the broker came back the partition looked new,
        its stored offset defaulted to wherever it now was, and every message
        written in between was never compared by anyone. The baseline is
        carried forward and the cycle is not clean.
        """
        import json

        from kafka import TopicPartition
        state = self.hop.report_dir(db) / "delta-offsets.json"

        def ends(consumer):
            out, unread = {}, {}
            for t in self._topics(consumer):
                for p in self._partitions(consumer, t):
                    try:
                        tp = TopicPartition(t, p)
                        out[f"{t}/{p}"] = consumer.end_offsets([tp])[tp]
                    except Exception as e:
                        text = str(e)[:80] or repr(e)
                        name = type(e).__name__
                        unread[f"{t}/{p}"] = (
                            text if text.startswith(name)
                            else f"{name}: {text}")
            return out, unread

        sc, dc = self._consumer("src"), self._consumer("dst")
        cur, unread = ends(sc)
        if not state.exists():
            state.write_text(json.dumps(cur))
            first = [Result("delta", "cluster", "ok",
                            f"baseline offsets recorded for {len(cur)}"
                            " partitions, changes tracked from here")]
            for key, why in sorted(unread.items()):
                topic, _, part = key.rpartition("/")
                first.append(Result(
                    "delta", f"{topic}[{part}]", "error",
                    f"left out of the baseline - {why}; changes to this"
                    " partition from now on will not be noticed until it can"
                    " be read and a baseline recorded"))
            return first
        prev = json.loads(state.read_text())
        res, clean, total = [], True, 0
        for key, why in sorted(unread.items()):
            # keep whatever the baseline already said, so the messages written
            # while this partition was unreadable are still waiting to be
            # compared rather than skipped over
            clean = False
            if key in prev:
                cur[key] = prev[key]
            topic, _, part = key.rpartition("/")
            res.append(Result(
                "delta", f"{topic}[{part}]", "error",
                f"offsets could not be read - {why}"
                + self._why_unreadable([(topic, int(part), why)])
                + ("; its baseline is held where it was"
                   if key in prev else "")))
            if log:
                log(f"{topic}[{part}]: offsets unreadable")
        for key, s_end in cur.items():
            n = s_end - prev.get(key, s_end)
            if n <= 0:
                continue
            total += n
            t, p = key.rsplit("/", 1)
            tp = TopicPartition(t, int(p))
            a, ea = self._tail_hash(sc, tp, min(n, limit), "src")
            b, eb = self._tail_hash(dc, tp, min(n, limit), "dst")
            if ea or eb:
                # the baseline must not move past changes nobody could read:
                # those messages would never be looked at again, and the next
                # cycle would report a clean delta over a gap
                clean = False
                why = "; ".join(f"{side}: {err}" for side, err in
                                (("source", ea), ("target", eb)) if err)
                res.append(Result("delta", f"{t}[{p}]", "error",
                                  f"{n} new messages could not be read - {why}"
                                  + self._why_unreadable([(t, int(p), why)])))
                if log:
                    log(f"{t}[{p}]: {n} new, unreadable")
                continue
            ok = a == b
            clean = clean and ok
            res.append(Result("delta", f"{t}[{p}]", "ok" if ok else "diff",
                              f"{n} new messages, tail"
                              f" {'matches' if ok else 'DIFFERS'}"))
            if log:
                log(f"{t}[{p}]: {n} new, {'ok' if ok else 'DIFF'}")
        if clean:
            state.write_text(json.dumps(cur))
        blind = [r for r in res if r.status == "error"]
        res.insert(0, Result("delta", "cluster",
                             "error" if blind else "ok" if clean else "diff",
                             f"{total} new messages across {len(res)}"
                             f" partitions, offsets"
                             f" {'advanced' if clean else 'NOT advanced'}"
                             + (f", {len(blind)} partitions could not be read"
                                " at all" if blind else "")))
        return res

    def watch_sample(self, db):
        from kafka import TopicPartition
        sc, dc = self._consumer("src"), self._consumer("dst")
        total = {"src": 0, "dst": 0}
        for side, c in (("src", sc), ("dst", dc)):
            for t in self._topics(c):
                tps = [TopicPartition(t, p) for p in self._partitions(c, t)]
                if tps:
                    total[side] += sum(c.end_offsets(tps).values())
        return {"db": "cluster", "ts": time.time(),
                "src_rows": total["src"], "dst_rows": total["dst"]}
