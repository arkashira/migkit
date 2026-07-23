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
        if bad:
            return [Result("schema", "topics", "diff", "; ".join(bad[:10]), "",
                           "align topics/partitions, mirror with mirrormaker2")]
        return [Result("schema", "topics", "ok", f"{len(src)} topics")]

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
