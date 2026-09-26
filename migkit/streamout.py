"""Changes delivered as messages into a stream, in the shapes consumers
expect (backlog 35): what a message is, whatever carries it - a Kafka
topic, a Kinesis stream.

Hop options, the same for every stream:
  format: json | debezium | canal     (json by default)
  topic: "{db}.{table}"               (a template naming each table's
                                       stream; the default)
  partition_by: <column>              (the row's key by default)
  max_message_bytes: 1048576          (a larger message is skipped and
                                       counted, not sent)
"""
import json

FORMATS = ("json", "debezium", "canal")


def jsonable(value):
    """One value in a message: text for what JSON has no type for, the way
    change-stream consumers read it - bytes as base64, times as ISO 8601,
    numbers of fixed precision as their exact text."""
    import base64
    import datetime
    import decimal
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return str(value)
    return str(value)


def options(hop, limit=1048576):
    """(format, stream name template, partition column, byte limit)."""
    opts = hop.options or {}
    fmt = str(opts.get("format", "json")).lower()
    if fmt not in FORMATS:
        raise SystemExit(f"format: {fmt} is not one of {', '.join(FORMATS)}")
    return (fmt, str(opts.get("topic", "{db}.{table}")),
            opts.get("partition_by"),
            int(opts.get("max_message_bytes", limit)))


def message(fmt, db, change, now_ms):
    """One change as the consumers of `fmt` expect it."""
    from . import canon
    op, table = change["op"], change["table"]
    key = dict(change.get("key") or {})
    after = {k: v for k, v in (change.get("values") or {}).items()
             if v is not canon.ABSENT}
    if fmt == "debezium":
        code = {"insert": "c", "update": "u", "delete": "d"}[op]
        return {"before": key if op != "insert" else None,
                "after": None if op == "delete" else after,
                "source": {"connector": "migkit", "db": db,
                           "table": table, "ts_ms": now_ms},
                "op": code, "ts_ms": now_ms}
    if fmt == "canal":
        return {"data": [key if op == "delete" else after],
                "database": db, "table": table, "pkNames": sorted(key),
                "isDdl": False, "old": None,
                "type": {"insert": "INSERT", "update": "UPDATE",
                         "delete": "DELETE"}[op],
                "es": now_ms, "ts": now_ms}
    return {"op": op, "db": db, "table": table, "key": key,
            "values": after if op != "delete" else None, "ts_ms": now_ms}


def encoded(hop, db, changes, now_ms, limit=None):
    """[(stream, partition key text, message bytes)] for `changes`, in
    their order, and {stream: messages skipped for size}."""
    fmt, rule, partition_by, most = options(hop)
    most = limit or most
    out, skipped = [], {}
    for c in changes:
        stream = rule.format(db=db, table=c["table"])
        value = json.dumps(message(fmt, db, c, now_ms), default=jsonable,
                           sort_keys=True).encode()
        if len(value) > most:
            skipped[stream] = skipped.get(stream, 0) + 1
            continue
        by = ({partition_by: (c.get("values") or c.get("key") or {})
               .get(partition_by)} if partition_by else c.get("key"))
        out.append((stream, json.dumps(by, default=jsonable,
                                       sort_keys=True), value))
    return out, skipped


def count_skipped(hop, db, skipped):
    """What was larger than `max_message_bytes`, by stream, added up across
    batches in the report directory - skipped, never lost without a
    count."""
    if not skipped:
        return
    path = hop.report_dir(db) / "stream-skipped.json"
    try:
        have = json.loads(path.read_text())
    except (OSError, ValueError):
        have = {}
    for stream, n in skipped.items():
        have[stream] = have.get(stream, 0) + n
    path.write_text(json.dumps(have, indent=1))
