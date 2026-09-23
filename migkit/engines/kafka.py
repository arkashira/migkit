import hashlib
import time

from .base import Engine, RepairAction, Result


class KafkaEngine(Engine):
    checks = ("schema", "counts", "data")

    def _consumer(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise SystemExit("pip install 'migkit[kafka]' for kafka support")
        return KafkaConsumer(bootstrap_servers=f"{ep.host}:{ep.port}",
                             request_timeout_ms=15000,
                             consumer_timeout_ms=10000,
                             enable_auto_commit=False)

    def databases(self):
        return ["cluster"]

    def _topics(self, consumer):
        """Every topic migkit verifies: internal ones and the ones the hop
        excludes left out. This engine used to ignore `exclude`."""
        return sorted(t for t in consumer.topics()
                      if not t.startswith("__")
                      and not self.hop.excluded("cluster", t))

    def _partitions(self, consumer, topic):
        return sorted(consumer.partitions_for_topic(topic) or [])

    def _admin(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        from kafka.admin import KafkaAdminClient
        return KafkaAdminClient(bootstrap_servers=f"{ep.host}:{ep.port}",
                                request_timeout_ms=15000)

    # semantics-critical topic configs: a cleanup.policy or retention
    # mismatch silently changes what the topic MEANS on the target
    CRITICAL_CONFIGS = ("cleanup.policy", "retention.ms", "retention.bytes",
                        "max.message.bytes", "min.insync.replicas",
                        "compression.type", "delete.retention.ms",
                        "segment.ms")

    def _topic_configs(self, side, topics):
        from kafka.admin import ConfigResource, ConfigResourceType
        out = {}
        try:
            admin = self._admin(side)
            for i in range(0, len(topics), 20):
                chunk = topics[i:i + 20]
                resp = admin.describe_configs(
                    [ConfigResource(ConfigResourceType.TOPIC, t)
                     for t in chunk])
                for r in resp:
                    for res in r.resources:
                        name = res[3]
                        entries = {e[0]: e[1] for e in res[4]}
                        out[name] = {k: entries.get(k, "")
                                     for k in self.CRITICAL_CONFIGS}
            admin.close()
        except Exception:
            return None
        return out

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
                consumer = self._consumer(side)
                try:
                    cid = consumer._client.cluster.cluster_id
                finally:
                    consumer.close()
                return {"cluster_id": cid} if cid else {}
            except Exception:
                return {}
        return (one("src"), one("dst"))

    def _server_versions(self):
        """Kafka does not publish a version over the client protocol the way
        a database does, so this says so rather than inventing one."""
        return (None, None)

    def _assess_extra(self):
        """What has to line up before topics are moved.

        Partition counts are the one that cannot be fixed afterwards: a
        consumer's key-to-partition mapping depends on the count, so a target
        with a different number of partitions delivers the same key to a
        different consumer - and nothing errors.
        """
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "instance",
                          "item": item, "detail": str(detail)})
        try:
            sc, dc = self._consumer("src"), self._consumer("dst")
            stops = set(self._topics(sc))
            dtops = set(self._topics(dc))
        except Exception as e:
            add("warn", "cannot list topics on both sides",
                f"{str(e)[:90]} - unknown, not clean")
            return items

        missing = sorted(stops - dtops)
        add("pass" if not missing else "fail",
            "every source topic exists on the target",
            f"{len(stops)} topics" if not missing
            else f"{len(missing)} missing: {', '.join(missing[:5])}")

        drift = []
        for topic in sorted(stops & dtops):
            try:
                sp = len(self._partitions(sc, topic) or [])
                dp = len(self._partitions(dc, topic) or [])
            except Exception:
                continue
            if sp != dp:
                drift.append(f"{topic} {sp}->{dp}")
        add("pass" if not drift else "fail",
            "partition counts match",
            "all equal" if not drift else
            "; ".join(drift[:5]) + " - a key lands on a different partition"
            " when the count changes, so ordering and consumer assignment"
            " move with it and nothing errors")
        return items

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
                              " --kind sequences --apply, or mirror them with"
                              " MirrorCheckpointConnector"))
        else:
            res.append(Result("deep", "groups", "ok",
                              f"{len(gs)} consumer groups present on"
                              " target"))
        behind, unsure, changed = [], [], []
        checked = 0
        for group in sorted(gs | gd):
            try:
                offs_s = self._group_offsets("src", group)
            except Exception:
                unsure.append(f"{group} (its offsets could not be read)")
                continue
            if not offs_s:
                continue
            try:
                offs_d = self._group_offsets("dst", group)
            except Exception:
                offs_d = {}
            checked += 1
            same, apart = self._comparable(list(offs_s))
            for tp in apart:
                unsure.append(f"{group} {tp.topic}[{tp.partition}]")
            for tp in same:
                if offs_d.get(tp) != offs_s[tp]:
                    behind.append(f"{group} {tp.topic}[{tp.partition}]"
                                  f" src={offs_s[tp]} dst={offs_d.get(tp)}")
                    changed.append(json.dumps(
                        {"group": group, "topic": tp.topic,
                         "partition": tp.partition, "offset": offs_s[tp]}))
        self._write_drill(db, "groups", missing=missing, changed=changed)
        # a pass has to say what it did not look at, or a group with one
        # untranslatable partition reads as a group that was checked
        aside = (f" ({len(unsure)} partitions not comparable, below)"
                 if unsure else "")
        res.append(Result(
            "deep", "group-offsets", "diff" if behind else "ok",
            "; ".join(behind[:6]) + aside if behind
            else f"{checked} groups, every comparable committed offset"
                 f" matches{aside}", "",
            f"migkit sync {self.hop.name} --db {db} --kind sequences --apply"
            if behind else ""))
        if unsure:
            res.append(Result(
                "deep", "group-offsets not comparable", "warn",
                f"{len(unsure)} partitions where an offset does not mean the"
                f" same thing on both sides: {', '.join(unsure[:6])} - the"
                " two logs do not start and end together, so migkit will not"
                " copy a number between them", "",
                "translate each group's position by message time rather than"
                " by number, or reset the target's groups deliberately with"
                " kafka-consumer-groups --reset-offsets"))
        return res

    def repair_plan(self, db, kind):
        """Committed offsets are the counters this engine can put right.

        Not the messages: re-producing them would give them new offsets and
        new timestamps, and a topic is not a table that can be written into
        twice. `check --check deep` is what fills the list this reads.
        """
        if kind not in ("sequences", "all"):
            return []
        changed = self._read_drill(db, "groups", "changed")
        if not changed:
            return []
        import json
        rows = [json.loads(line) for line in changed]
        groups = sorted({r["group"] for r in rows})
        return [RepairAction(
            db, "sequences",
            [f"commit {len(rows)} offsets on the target for"
             f" {len(groups)} groups: {', '.join(groups[:6])}"
             + (" ..." if len(groups) > 6 else "")],
            [], "only partitions whose logs begin and end together on the two"
                " sides are listed; the target's current offsets go to the"
                " undo file first")]

    def apply(self, db, action):
        """Set the target's committed offsets to the source's.

        Measured: `alter_group_offsets` creates the group when it is not
        there, which is what a migrated cluster needs, and reports per
        partition - `{TopicPartition: NoError}` - so the result is read
        rather than assumed.
        """
        import json

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        rows = [json.loads(line)
                for line in self._read_drill(db, "groups", "changed")]
        if not rows:
            return
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        by_group = {}
        for row in rows:
            by_group.setdefault(row["group"], {})[
                TopicPartition(row["topic"], row["partition"])] = row["offset"]
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
        finally:
            admin.close()

    def check_counts(self, db):
        from kafka import TopicPartition
        sc, dc = self._consumer("src"), self._consumer("dst")
        bad = []
        n = 0
        total_a = total_b = 0
        for t in self._topics(sc):
            tps = [TopicPartition(t, p) for p in self._partitions(sc, t)]
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
        if bad:
            return [Result("counts", "messages", "diff", "; ".join(bad[:10]), "",
                           "counts net of retention: small gaps are normal"
                           " while mirror lags, big gaps mean missing data")]
        return [Result("counts", "messages", "ok",
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
        for t in topics:
            parts = self._partitions(sc, t)
            if not parts:
                unread.append((t, None, "no partitions on the source"))
                continue
            for p in parts:
                tp = TopicPartition(t, p)
                with gate.unit():
                    a, ea = self._tail_hash(sc, tp, sample)
                    b, eb = self._tail_hash(dc, tp, sample)
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
        if not res and not checked:
            res.append(Result("data", "tail-sample", "error",
                              "no partitions were compared at all, which is"
                              " not the same as every partition matching"))
        elif not res:
            res.append(Result("data", "tail-sample", "ok",
                              f"last {sample} messages hash-equal on"
                              f" {checked} partitions"))
        return res

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

    def _tail_hash(self, consumer, tp, n):
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
        start = max(beg, end - n)
        if start >= end:
            return "empty", ""
        consumer.seek(tp, start)
        h = hashlib.md5()
        got = 0
        while got < end - start:
            batch = consumer.poll(timeout_ms=5000)
            if not batch:
                break
            for msgs in batch.values():
                for m in msgs:
                    if m.offset >= end:
                        break
                    h.update(m.key or b"")
                    h.update(m.value or b"")
                    got += 1
        return f"{got}|{h.hexdigest()}", ""

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
            a, ea = self._tail_hash(sc, tp, min(n, limit))
            b, eb = self._tail_hash(dc, tp, min(n, limit))
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
