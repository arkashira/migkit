import hashlib
import time

from .base import Engine, Result


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
        return sorted(t for t in consumer.topics() if not t.startswith("__"))

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
                              "align topics/partitions, mirror with"
                              " mirrormaker2"))
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

    def check_deep(self, db):
        """Consumer-group parity: the classic kafka-migration failure is
        moving the data but not the committed offsets - every consumer
        restarts from earliest/latest and double-processes or drops."""
        from kafka import TopicPartition
        try:
            admin_s, admin_d = self._admin("src"), self._admin("dst")
        except Exception as e:
            return [Result("deep", "groups", "error", str(e))]
        gs = {g[0] for g in admin_s.list_consumer_groups()
              if g[0] and not g[0].startswith("_")}
        gd = {g[0] for g in admin_d.list_consumer_groups()
              if g[0] and not g[0].startswith("_")}
        missing = sorted(gs - gd)
        res = []
        if missing:
            res.append(Result("deep", "groups", "diff",
                              f"{len(missing)} consumer groups missing on"
                              f" target: {', '.join(missing[:6])}", "",
                              "mirror offsets (mirrormaker2 checkpoint +"
                              " MirrorCheckpointConnector sync) or seed"
                              " kafka-consumer-groups --reset-offsets"))
        else:
            res.append(Result("deep", "groups", "ok",
                              f"{len(gs)} consumer groups present on"
                              " target"))
        sc, dc = self._consumer("src"), self._consumer("dst")
        stale = []
        checked = 0
        for g in sorted(gs & gd):
            try:
                offs_s = admin_s.list_consumer_group_offsets(g)
                offs_d = admin_d.list_consumer_group_offsets(g)
            except Exception:
                continue
            tps = [tp for tp in offs_s if not tp.topic.startswith("__")]
            if not tps:
                continue
            checked += 1
            lag_s = lag_d = 0
            try:
                end_s = sc.end_offsets(tps)
                end_d = dc.end_offsets(tps)
            except Exception:
                continue
            for tp in tps:
                lag_s += max(0, end_s.get(tp, 0) - offs_s[tp].offset)
                d_off = offs_d.get(tp)
                if d_off is None or d_off.offset < 0:
                    lag_d += max(0, end_d.get(tp, 0))
                else:
                    lag_d += max(0, end_d.get(tp, 0) - d_off.offset)
            if lag_d > lag_s + 10000:
                stale.append(f"{g} lag src={lag_s} dst={lag_d}"
                             " (offsets not translated?)")
        res.append(Result("deep", "group-lag",
                          "diff" if stale else "ok",
                          "; ".join(stale[:6]) if stale
                          else f"{checked} common groups, target lag in"
                          " line with source", "",
                          "translate offsets before cutover or consumers"
                          " will reprocess/skip" if stale else ""))
        return res

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
        sample = int(self.hop.options.get("sample", 200))
        sc, dc = self._consumer("src"), self._consumer("dst")
        topics = [table] if table else self._topics(sc)
        bad = []
        checked = 0
        for t in topics:
            for p in self._partitions(sc, t):
                tp = TopicPartition(t, p)
                a = self._tail_hash(sc, tp, sample)
                b = self._tail_hash(dc, tp, sample)
                checked += 1
                if stream:
                    stream(f"{t}[{p}]: {'ok' if a == b else 'DIFF'}")
                if a != b:
                    bad.append(f"{t}[{p}]")
        if bad:
            return [Result("data", "tail-sample", "diff",
                           f"content differs in: {', '.join(bad[:10])}", "",
                           "re-mirror those topics, verify consumer-group"
                           " checkpoints before cutover")]
        return [Result("data", "tail-sample", "ok",
                       f"last {sample} messages hash-equal on"
                       f" {checked} partitions")]

    def _tail_hash(self, consumer, tp, n):
        try:
            consumer.assign([tp])
            end = consumer.end_offsets([tp])[tp]
            beg = consumer.beginning_offsets([tp])[tp]
        except Exception:
            return "unavailable"
        start = max(beg, end - n)
        if start >= end:
            return "empty"
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
        return f"{got}|{h.hexdigest()}"

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
