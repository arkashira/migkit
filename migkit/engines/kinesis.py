"""Amazon Kinesis Data Streams as the destination of a change stream
(backlog 33).

A hop `engine: hetero` with `target_engine: kinesis` carries the source's
changes into streams, as messages in the shapes and under the hop options
`streamout` describes, the same as Kafka. The endpoint's `region` and
`endpoint_url` say where. A stream Kinesis does not have is created only
where the hop says `create_streams: true`, with `shards` shards (1 by
default); otherwise the delivery stops and names it, since a stream costs
money for as long as it exists.

A key's messages go to one shard, by the row's key as the partition key.
Kinesis keeps a shard's records in the order they were put, but a batch
can come back with some of its records refused and the others kept, which
would put a later change ahead of a refused earlier one. So a batch holds
each key at most once: a key's next change waits for the next batch, and a
refused record goes again before it.
"""
import time

from .base import Engine

#: records per request, and bytes in one, as the service allows
BATCH_RECORDS, BATCH_BYTES = 500, 5 * 2 ** 20


class KinesisEngine(Engine):
    checks = ()
    CANON_ENGINE = "kinesis"
    #: a stream is named by the hop's template, not paired with what exists
    CREATES_ON_WRITE = True
    EXPRESSES_ABSENT = True

    def _client(self):
        import boto3
        ep = self.hop.target
        kw = {"region_name": ep.options.get("region") or "us-east-1"}
        if ep.options.get("endpoint_url"):
            kw["endpoint_url"] = ep.options["endpoint_url"]
        if ep.user:
            kw.update(aws_access_key_id=ep.user,
                      aws_secret_access_key=ep.password)
        return boto3.client("kinesis", **kw)

    def databases(self):
        return list(self.hop.databases)

    def target_missing(self, db):
        return True

    def _ensure(self, client, names):
        """Every stream in `names` there and active, or created where the
        hop allows it."""
        have, start = set(), None
        while True:
            got = client.list_streams(**({"ExclusiveStartStreamName": start}
                                         if start else {}))
            have |= set(got.get("StreamNames", []))
            if not got.get("HasMoreStreams"):
                break
            start = got["StreamNames"][-1]
        missing = sorted(set(names) - have)
        if not missing:
            return
        opts = self.hop.options or {}
        if not opts.get("create_streams"):
            raise SystemExit(
                f"no stream {', '.join(missing[:6])} on the target, and a"
                " stream costs money for as long as it exists: create it,"
                " or say create_streams: true in the hop's options")
        for name in missing:
            client.create_stream(StreamName=name,
                                 ShardCount=int(opts.get("shards", 1)))
        for name in missing:
            client.get_waiter("stream_exists").wait(StreamName=name)

    def neutral_apply(self, side, db, changes):
        """Every change as a record, in the order the source made them."""
        from .. import streamout
        self._target_only(side, "write a change stream")
        # a record's data is at most a megabyte
        sent, skipped = streamout.encoded(self.hop, db, changes,
                                          int(time.time() * 1000),
                                          limit=2 ** 20)
        client = self._client()
        self._ensure(client, {stream for stream, _, _ in sent})
        by_stream = {}
        for stream, key, value in sent:
            # a partition key is at most 256 characters: a longer key goes
            # by its digest, which keeps it on one shard all the same
            if len(key) > 256:
                import hashlib
                key = hashlib.sha256(key.encode()).hexdigest()
            by_stream.setdefault(stream, []).append(
                {"Data": value, "PartitionKey": key})
        for stream, records in by_stream.items():
            self._put(client, stream, records)
        streamout.count_skipped(self.hop, db, skipped)
        return len(changes)

    @staticmethod
    def _put(client, stream, records):
        """Every record put, each key's in its order: a batch takes a key
        once, and what the service refused goes again ahead of the rest."""
        wait = 0.1
        pending = list(records)
        while pending:
            batch, keys, size, rest = [], set(), 0, []
            for r in pending:
                grows = len(r["Data"]) + len(r["PartitionKey"])
                if (r["PartitionKey"] in keys or len(batch) >= BATCH_RECORDS
                        or (batch and size + grows > BATCH_BYTES)):
                    rest.append(r)
                    continue
                batch.append(r)
                keys.add(r["PartitionKey"])
                size += grows
            got = client.put_records(StreamName=stream, Records=batch)
            refused = [r for r, res in zip(batch, got["Records"])
                       if res.get("ErrorCode")]
            if refused:
                time.sleep(wait)
                wait = min(wait * 2, 5)
            else:
                wait = 0.1
            pending = refused + rest

    def _apply_upsert(self, side, db, table, key, values):
        self.neutral_apply(side, db, [{"op": "update", "table": table,
                                       "key": key, "values": values}])

    def _apply_delete(self, side, db, table, key):
        self.neutral_apply(side, db, [{"op": "delete", "table": table,
                                       "key": key}])
