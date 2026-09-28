"""DynamoDB as one side of a pair (backlog 34).

A database is a prefix on table names (`<db>.` by default, the endpoint's
`table_prefix` otherwise, with `{db}` in it). DynamoDB keeps a number
without the scale it was written with - `1.50` comes back `1.5` - and has
no time type, so what a column was is kept as tags on the table migkit
creates: the value is read back as that, a decimal at its scale and a
time from its text. Where the endpoint does not
take tags (DynamoDB Local), the same description is kept on this machine,
under the reports, by endpoint, where any hop reading that endpoint finds
it. A table migkit did not create is described
from its items, as MongoDB's collections are.

A key is the table's partition key, and its sort key where there is one:
a source table keyed by more than two columns, or by none, has no
DynamoDB table to go to and is refused by name. Items come back in no
order, so a table is read in one pass, and written by key, which replaces
an item already there.
"""
import datetime
import json
import time
from decimal import Decimal

from .base import Engine, NeutralCopier, Result

class RawAttr(str):
    """A DynamoDB value with no neutral class - a map, a list, a set -
    carried as the canonical JSON of the attribute (map keys and set
    members sorted, bytes in base64), so both sides render it alike, and
    written back as the attribute it was. Between two DynamoDB tables it
    crosses unchanged; any other engine is given the text."""

    @classmethod
    def of(cls, attr):
        return cls(json.dumps(_canonical(attr), sort_keys=True,
                              separators=(",", ":")))

    def attr(self):
        return _restored(json.loads(self))


def _canonical(attr):
    import base64
    kind, raw = next(iter(attr.items()))
    if kind == "M":
        return {"M": {k: _canonical(v) for k, v in raw.items()}}
    if kind == "L":
        return {"L": [_canonical(v) for v in raw]}
    if kind in ("B", "BS"):
        enc = [base64.b64encode(bytes(b)).decode() for b in
               (raw if kind == "BS" else [raw])]
        return {kind: sorted(enc) if kind == "BS" else enc[0]}
    if kind in ("SS", "NS"):
        return {kind: sorted(raw, key=(lambda n: Decimal(n))
                             if kind == "NS" else None)}
    return {kind: raw}


def _restored(attr):
    import base64
    kind, raw = next(iter(attr.items()))
    if kind == "M":
        return {"M": {k: _restored(v) for k, v in raw.items()}}
    if kind == "L":
        return {"L": [_restored(v) for v in raw]}
    if kind == "B":
        return {"B": base64.b64decode(raw)}
    if kind == "BS":
        return {"BS": [base64.b64decode(b) for b in raw]}
    return {kind: raw}


#: attribute kinds with no neutral class, carried whole (`RawAttr`)
RAW_KINDS = ("M", "L", "SS", "NS", "BS")

#: tag keys holding what migkit knows about a table it created
TAG_COLUMNS, TAG_KEY = "migkit:columns", "migkit:key"
#: a tag's value holds at most this many characters
TAG_ROOM = 256


class DynamoDBEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "dynamodb"
    FOLDED_BECAUSE = "has no way to hash items in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False

    def _client(self, side):
        import boto3
        ep = self.hop.source if side == "src" else self.hop.target
        kw = {}
        end = ep.options.get("endpoint_url") or (
            ep.host if str(ep.host).startswith("http") else None)
        if end:
            kw["endpoint_url"] = end
        kw["region_name"] = ep.options.get("region") or "us-east-1"
        if ep.user:
            kw.update(aws_access_key_id=ep.user,
                      aws_secret_access_key=ep.password)
        return boto3.client("dynamodb", **kw)

    def _prefix(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        name = self.hop.target_db(db) if side == "dst" else db
        return str(ep.options.get("table_prefix") or "{db}.").format(db=name)

    def _name(self, side, db, table):
        return self._prefix(side, db) + table

    def databases(self):
        return list(self.hop.databases or ["default"])

    # ---- what a table is -------------------------------------------------

    def neutral_tables(self, side, db):
        client, pre = self._client(side), self._prefix(side, db)
        out, start = [], None
        while True:
            got = client.list_tables(**({"ExclusiveStartTableName": start}
                                        if start else {}))
            out += [n[len(pre):] for n in got.get("TableNames", [])
                    if n.startswith(pre) and not self.hop.excluded(
                        db, n[len(pre):])]
            start = got.get("LastEvaluatedTableName")
            if not start:
                return sorted(out)

    def _kept_here(self, side):
        """Descriptions of the tables migkit made, for an endpoint that
        takes no tags: one file per endpoint, whichever hop made them."""
        import hashlib
        from pathlib import Path

        from .. import config
        ep = self.hop.source if side == "src" else self.hop.target
        where = str(ep.options.get("endpoint_url") or ep.host or "aws")
        d = Path(config.REPORTS) / "_dynamodb"
        d.mkdir(parents=True, exist_ok=True)
        return d / (hashlib.sha1(where.encode()).hexdigest()[:16] + ".json")

    def _described(self, side, db, table):
        """(columns [(name, declared)], key) from what migkit left when it
        made the table, or None for a table it did not make."""
        client = self._client(side)
        name = self._name(side, db, table)
        arn = client.describe_table(TableName=name)["Table"]["TableArn"]
        tags, start = {}, None
        try:
            while True:
                got = client.list_tags_of_resource(
                    ResourceArn=arn, **({"NextToken": start} if start
                                        else {}))
                tags.update({t["Key"]: t["Value"]
                             for t in got.get("Tags", [])})
                start = got.get("NextToken")
                if not start:
                    break
        except Exception as e:  # noqa: BLE001 - an endpoint without tags
            if "not currently supported" not in str(e):
                raise
        parts = sorted((k, v) for k, v in tags.items()
                       if k.startswith(TAG_COLUMNS))
        if parts:
            cols = json.loads("".join(v for _, v in parts))
            return [tuple(c) for c in cols], tags.get(TAG_KEY, "").split(",")
        try:
            kept = json.loads(self._kept_here(side).read_text()).get(arn)
        except (OSError, ValueError):
            kept = None
        if kept:
            return [tuple(c) for c in kept["columns"]], kept["key"]
        return None

    def _key_schema(self, side, db, table):
        got = self._client(side).describe_table(
            TableName=self._name(side, db, table))["Table"]
        order = {"HASH": 0, "RANGE": 1}
        return [k["AttributeName"] for k in
                sorted(got["KeySchema"], key=lambda k: order[k["KeyType"]])]

    def neutral_columns(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[0]
        # a table migkit did not make: what its items hold, all of them
        seen = {}
        for item in self._scan(side, db, table):
            for name, value in item.items():
                seen.setdefault(name, set()).add(next(iter(value)))
        out = []
        for name, kinds in sorted(seen.items()):
            kinds.discard("NULL")
            out.append((name, kinds.pop() if len(kinds) == 1
                        else "|".join(sorted(kinds)) or "NULL"))
        return out

    def neutral_key(self, side, db, table):
        return self._key_schema(side, db, table)

    # ---- values ---------------------------------------------------------

    @staticmethod
    def _to_attr(declared, value):
        """A value as DynamoDB holds it, for a column declared `declared`."""
        from .. import canon
        if value is None:
            return None
        if isinstance(value, RawAttr):
            return value.attr()
        cls = canon.type_class("dynamodb", declared)
        if cls is None:
            # a column of a table migkit did not make, holding more than
            # one kind: each value as the kind it is
            cls = ("boolean" if isinstance(value, bool) else
                   "decimal" if isinstance(value, (int, float, Decimal))
                   else "bytes" if isinstance(value, (bytes, bytearray))
                   else "text")
        if cls in ("integer", "decimal"):
            return {"N": str(Decimal(value) if not isinstance(value, Decimal)
                             else value)}
        if cls == "float":
            return {"N": repr(float(value))}
        if cls == "boolean":
            return {"BOOL": bool(value)}
        if cls == "bytes":
            return {"B": bytes(value)}
        if cls in ("timestamp", "date", "time"):
            return {"S": canon.render_value(cls, value)}
        return {"S": str(value)}

    @staticmethod
    def _from_attr(declared, attr):
        """A value back as the class it was written as."""
        from .. import canon
        if attr is None or "NULL" in attr:
            return None
        kind, raw = next(iter(attr.items()))
        if kind in RAW_KINDS:
            return RawAttr.of(attr)
        cls = canon.type_class("dynamodb", declared)
        if kind == "N":
            number = Decimal(raw)
            if cls == "integer":
                return int(number)
            if cls == "float":
                return float(number)
            scale = canon.params(declared)
            if cls == "decimal" and len(scale) == 2:
                return number.quantize(Decimal(1).scaleb(-scale[1]))
            return number
        if kind == "B":
            return bytes(raw)
        if kind == "BOOL":
            return bool(raw)
        if kind == "S" and cls == "timestamp":
            # all nine digits of a second where it was written with them
            # (`nanotime`); `fromisoformat` keeps six
            from .. import nanotime
            return nanotime.parse(raw)
        if kind == "S" and cls == "date":
            return datetime.date.fromisoformat(raw)
        if kind == "S" and cls == "time":
            from .. import nanotime
            return nanotime.parse(raw)
        return raw

    def _scan(self, side, db, table, names=None, limit=None):
        client, start = self._client(side), None
        kw = {"TableName": self._name(side, db, table)}
        if names:
            kw["ProjectionExpression"] = ", ".join(
                f"#c{i}" for i in range(len(names)))
            kw["ExpressionAttributeNames"] = {
                f"#c{i}": n for i, n in enumerate(names)}
        if limit:
            kw["Limit"] = limit
        while True:
            got = client.scan(**kw, **({"ExclusiveStartKey": start}
                                       if start else {}))
            yield from got.get("Items", [])
            start = got.get("LastEvaluatedKey")
            if not start or limit:
                return

    def _declared(self, side, db, table):
        return dict(self.neutral_columns(side, db, table))

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and DynamoDB takes none")
        declared = self._declared(side, db, table)
        names = [n for n, _ in columns]
        batch = []
        for item in self._scan(side, db, table, names):
            batch.append([self._from_attr(declared.get(n, ""), item.get(n))
                          for n in names])
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Items come back in no order: the copier reads a DynamoDB table
        in one pass (`RESUMES_BY_KEY`). This serves the callers that ask
        whether a table holds anything."""
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and DynamoDB"
                             " takes none")
        client = self._client(side)
        declared = self._declared(side, db, table)
        name = self._name(side, db, table)
        names = [n for n, _ in columns]
        found = []
        keys = list(keys)
        for i in range(0, len(keys), 100):
            want = {name: {"Keys": [
                {k: self._to_attr(declared.get(k, ""), v)
                 for k, v in zip(key, kk)} for kk in keys[i:i + 100]]}}
            while want:
                got = client.batch_get_item(RequestItems=want)
                for item in got.get("Responses", {}).get(name, []):
                    found.append([self._from_attr(declared.get(n, ""),
                                                  item.get(n))
                                  for n in names])
                want = got.get("UnprocessedKeys") or {}
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def table_facts(self, side, db):
        client = self._client(side)
        out = {}
        for t in self.neutral_tables(side, db):
            got = client.describe_table(
                TableName=self._name(side, db, t))["Table"]
            out[t] = {"rows": got.get("ItemCount"),
                      "bytes": got.get("TableSizeBytes"), "key": True}
        return out

    # ---- writing --------------------------------------------------------

    def _declared_of(self, col):
        from .. import canon
        name, cls, numbers = col[0], col[1], tuple(col[2] or ())
        if cls == canon.OWN and numbers:
            # DynamoDB to DynamoDB: the source's own kinds, as they are
            return str(numbers[0])
        if cls not in ("integer", "decimal", "float", "boolean", "text",
                       "bytes", "date", "timestamp", "time"):
            raise SystemExit(f"a {cls} column ({name}) has no DynamoDB"
                             " counterpart migkit writes; leave it out with"
                             " mapping.columns")
        return cls + (f"({numbers[0]},{numbers[1]})"
                      if cls == "decimal" and len(numbers) == 2 else "")

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key or len(key) > 2:
            raise SystemExit(
                f"{table} is keyed by {len(key)} columns, and a DynamoDB"
                " table is keyed by one, or by one and a sort key: give the"
                " table such a key, or leave it out of this hop")
        cols = [[c[0], self._declared_of(c)] for c in columns]
        return json.dumps({"table": self._name(side, db, table),
                           "key": list(key), "columns": cols})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        spec = json.loads(self.neutral_create_sql(side, db, table, columns,
                                                  key))
        declared = dict(spec["columns"])
        from .. import canon

        def attr_type(name):
            cls = canon.type_class("dynamodb", declared[name])
            return ("N" if cls in ("integer", "decimal", "float") else
                    "B" if cls == "bytes" else "S")
        client = self._client(side)
        client.create_table(
            TableName=spec["table"], BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": k, "KeyType": t}
                       for k, t in zip(spec["key"], ("HASH", "RANGE"))],
            AttributeDefinitions=[{"AttributeName": k,
                                   "AttributeType": attr_type(k)}
                                  for k in spec["key"]])
        client.get_waiter("table_exists").wait(TableName=spec["table"])
        text = json.dumps(spec["columns"], separators=(",", ":"))
        tags = [{"Key": f"{TAG_COLUMNS}:{i // TAG_ROOM:03d}",
                 "Value": text[i:i + TAG_ROOM]}
                for i in range(0, len(text), TAG_ROOM)]
        if len(tags) > 48:
            raise SystemExit(f"{table} has too many columns to describe in"
                             " a DynamoDB table's tags")
        arn = client.describe_table(TableName=spec["table"])["Table"][
            "TableArn"]
        try:
            client.tag_resource(ResourceArn=arn, Tags=tags + [
                {"Key": TAG_KEY, "Value": ",".join(spec["key"])}])
        except Exception as e:  # noqa: BLE001 - an endpoint without tags
            if "not currently supported" not in str(e):
                raise
            path = self._kept_here(side)
            try:
                kept = json.loads(path.read_text())
            except (OSError, ValueError):
                kept = {}
            kept[arn] = {"columns": spec["columns"], "key": spec["key"]}
            path.write_text(json.dumps(kept, indent=1))
        return json.dumps(spec)

    def _batch_write(self, client, name, requests):
        for i in range(0, len(requests), 25):
            want = {name: requests[i:i + 25]}
            wait = 0.05
            while want:
                got = client.batch_write_item(RequestItems=want)
                want = got.get("UnprocessedItems") or {}
                if want:
                    time.sleep(wait)
                    wait = min(wait * 2, 2)

    def neutral_write(self, side, db, table, columns, rows):
        """Each item put by its key, which replaces one already there: a
        batch written again after a restart lands on itself. A null is an
        attribute left out."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        declared = self._declared(side, db, table)
        names = [n for n, _ in columns]
        requests = []
        for r in rows:
            item = {}
            for n, v in zip(names, r):
                attr = self._to_attr(declared.get(n, "text"), v)
                if attr is not None:
                    item[n] = attr
            requests.append({"PutRequest": {"Item": item}})
        self._batch_write(self._client(side), self._name(side, db, table),
                          requests)
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and DynamoDB"
                             " takes none")
        key = self._key_schema(side, db, table)
        items = list(self._scan(side, db, table, key))
        self._batch_write(self._client(side), self._name(side, db, table),
                          [{"DeleteRequest": {"Key": i}} for i in items])
        return len(items)

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """Every table in scope active, and the target's with point-in-time
        recovery on: without it, the cutover has nothing to go back to."""
        out = []
        for side, who in (("src", "source"), ("dst", "target")):
            client = self._client(side)
            try:
                tables = self.neutral_tables(side, db)
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db} {who} tables", "error",
                                  f"could not list the {who}'s tables:"
                                  f" {str(e).splitlines()[0][:90]}"))
                continue
            busy, unprotected = [], []
            for t in tables:
                name = self._name(side, db, t)
                got = client.describe_table(TableName=name)["Table"]
                if got.get("TableStatus") != "ACTIVE":
                    busy.append(f"{t} ({got.get('TableStatus')})")
                if side == "dst":
                    try:
                        pitr = client.describe_continuous_backups(
                            TableName=name)["ContinuousBackupsDescription"]
                        state = pitr.get("PointInTimeRecoveryDescription",
                                         {}).get("PointInTimeRecoveryStatus")
                    except Exception:  # noqa: BLE001 - not every endpoint
                        state = None
                    if state == "DISABLED":
                        unprotected.append(t)
            if busy:
                out.append(Result("deep", f"{db} {who} tables", "warn",
                                  f"not active yet: {', '.join(busy[:6])} -"
                                  " what is read from them now is not"
                                  " settled", "", "wait, then check again"))
            else:
                out.append(Result("deep", f"{db} {who} tables", "ok",
                                  f"{len(tables)} tables, every one active"))
            if unprotected:
                out.append(Result(
                    "deep", f"{db} target recovery", "warn",
                    f"point-in-time recovery is off on"
                    f" {', '.join(unprotected[:6])}: a cutover that goes wrong"
                    " has no point to go back to", "",
                    "turn point-in-time recovery on for those tables"
                    " before the cutover"))
        return out

    # ---- the change log: DynamoDB Streams ----------------------------------
    # A table's stream is its change log: each write in order per key, kept
    # 24 hours, read without being consumed - so the same reading serves a
    # tail, a fence and a verify of only what changed. A table whose stream
    # is off is said, never turned on: that is a change to the source.

    CHANGE_POINT_READS_ONLY = True

    def _streams(self, side):
        import boto3
        ep = self.hop.source if side == "src" else self.hop.target
        kw = {"region_name": ep.options.get("region") or "us-east-1"}
        end = ep.options.get("endpoint_url") or (
            ep.host if str(ep.host).startswith("http") else None)
        if end:
            kw["endpoint_url"] = end
        if ep.user:
            kw.update(aws_access_key_id=ep.user,
                      aws_secret_access_key=ep.password)
        return boto3.client("dynamodbstreams", **kw)

    def _stream_of(self, side, db, table):
        """(stream ARN, view type) of a table, or refused where it has
        no stream on."""
        got = self._client(side).describe_table(
            TableName=self._name(side, db, table))["Table"]
        spec = got.get("StreamSpecification") or {}
        if not spec.get("StreamEnabled") or not got.get("LatestStreamArn"):
            raise SystemExit(
                f"{self._name(side, db, table)} has no stream on, and its"
                " stream is the change log migkit reads: turn it on for the"
                " table (NEW_IMAGE or NEW_AND_OLD_IMAGES) - migkit does not"
                " change the source")
        return got["LatestStreamArn"], spec.get("StreamViewType")

    def _shards(self, side, arn):
        """Every shard the stream holds, each after its parent: a key's
        writes move to a child when a shard splits, and are in order only
        read parent first."""
        client, start, shards = self._streams(side), None, []
        while True:
            got = client.describe_stream(
                StreamArn=arn, **({"ExclusiveStartShardId": start}
                                  if start else {}))["StreamDescription"]
            shards += got.get("Shards", [])
            start = got.get("LastEvaluatedShardId")
            if not start:
                break
        ids = {s["ShardId"] for s in shards}
        placed, out = set(), []
        while len(out) < len(shards):
            before = len(out)
            for s in shards:
                parent = s.get("ParentShardId")
                if s["ShardId"] not in placed and (
                        not parent or parent not in ids or parent in placed):
                    out.append(s)
                    placed.add(s["ShardId"])
            if len(out) == before:
                raise SystemExit(f"the shards of {arn} name each other as"
                                 " parents; the stream cannot be read in"
                                 " order")
        return out

    def _records(self, side, arn, shard, after, most=None):
        """(records, fully read) of one shard after sequence `after` (from
        the oldest it keeps with None): records until it is caught up, or
        `most` of them. A position the stream no longer holds is refused."""
        client = self._streams(side)
        try:
            it = client.get_shard_iterator(
                StreamArn=arn, ShardId=shard,
                **({"ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                    "SequenceNumber": after} if after else
                   {"ShardIteratorType": "TRIM_HORIZON"}))["ShardIterator"]
        except client.exceptions.TrimmedDataAccessException:
            raise SystemExit(
                f"the stream no longer holds the records after {after} of"
                f" {shard}: its 24 hours passed, and what was written then"
                " is carried by nothing. Move again with --mode full+cdc")
        out = []
        while it:
            got = client.get_records(ShardIterator=it, Limit=1000)
            out += got.get("Records", [])
            it = got.get("NextShardIterator")
            if most and len(out) >= most:
                return out[:most], False
            if not got.get("Records"):
                # nothing more now; an open shard stays open
                return out, it is None
        return out, True

    def change_point(self, side, db):
        """Now, in each table's stream: every shard read to its end, its
        last record's sequence kept - so a tail started here reads only
        what is written from here. Reading consumes nothing."""
        out = {}
        for t in self.neutral_tables(side, db):
            arn, _ = self._stream_of(side, db, t)
            shards, done = {}, []
            for s in self._shards(side, arn):
                recs, closed = self._records(side, arn, s["ShardId"], None)
                shards[s["ShardId"]] = (recs[-1]["dynamodb"]["SequenceNumber"]
                                        if recs else None)
                if closed:
                    done.append(s["ShardId"])
            out[t] = {"arn": arn, "shards": shards, "done": done}
        return {"streams": out}

    def log_position(self, side, db):
        return self.change_point(side, db)

    @staticmethod
    def position_reached(have, want):
        """Whether a tail at `have` has read every record `want` holds:
        each shard read as far, or finished."""
        if not have or not want:
            return None
        for t, w in (want.get("streams") or {}).items():
            h = (have.get("streams") or {}).get(t)
            if h is None or h.get("arn") != w.get("arn"):
                return False
            for shard, seq in (w.get("shards") or {}).items():
                if seq is None or shard in (h.get("done") or []):
                    continue
                got = (h.get("shards") or {}).get(shard)
                if got is None or int(got) < int(seq):
                    return False
        return True

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each write after `token`, per table in its stream's order - a
        shard read only once its parent is finished - as the item is now:
        the image the stream carries, or the item itself read where the
        stream keeps keys only. A table whose stream was turned off and on
        since has lost the records between, and is refused."""
        from .. import canon
        if token is None:
            return [], self.change_point(side, db)
        state = json.loads(json.dumps(token.get("streams") or {}))
        found = []
        for t in self.neutral_tables(side, db):
            arn, view = self._stream_of(side, db, t)
            st = state.setdefault(t, {"arn": arn, "shards": {}, "done": []})
            if st.get("arn") != arn:
                raise SystemExit(
                    f"{self._name(side, db, t)} has a new stream since the"
                    " saved position: the stream was turned off and on, and"
                    " the records between are gone. Move again with --mode"
                    " full+cdc")
            declared = self._declared(side, db, t)
            key = self._key_schema(side, db, t)
            done = set(st.get("done") or [])
            shards = self._shards(side, arn)
            held = {s["ShardId"] for s in shards}
            for s in shards:
                sid = s["ShardId"]
                if sid in done:
                    continue
                parent = s.get("ParentShardId")
                if parent and parent in held and parent not in done:
                    continue    # its parent is not finished yet
                room = limit - len(found)
                if room <= 0:
                    break
                recs, closed = self._records(side, arn, sid,
                                             st["shards"].get(sid), room)
                for r in recs:
                    found.append(self._change_of(side, db, t, r, view,
                                                 declared, key, canon))
                    st["shards"][sid] = r["dynamodb"]["SequenceNumber"]
                if closed and len(recs) < room:
                    done.add(sid)
                    st["shards"].setdefault(sid, None)
            st["done"] = sorted(done)
        return found, {"streams": state}

    def _change_of(self, side, db, table, record, view, declared, key,
                   canon):
        body = record["dynamodb"]
        ident = {k: self._from_attr(declared.get(k, ""),
                                    body["Keys"].get(k)) for k in key}
        if record["eventName"] == "REMOVE":
            return canon.change("delete", table, ident)
        image = body.get("NewImage")
        if image is None:
            # the stream keeps keys (or the old image) only: the item as
            # it is now, which a later record will bring up to date again
            image = self._client(side).get_item(
                TableName=self._name(side, db, table), Key=body["Keys"],
                ConsistentRead=True).get("Item")
            if image is None:
                return canon.change("delete", table, ident)
        values = {n: self._from_attr(declared.get(n, ""), a)
                  for n, a in image.items()}
        return canon.change("insert" if record["eventName"] == "INSERT"
                            else "update", table, ident, values)

    def _apply_upserts(self, side, db, table, rows):
        declared = self._declared(side, db, table)
        items = []
        for _, values in rows:
            item = {}
            for n, v in values.items():
                attr = self._to_attr(declared.get(n, "text"), v)
                if attr is not None:
                    item[n] = attr
            items.append({"PutRequest": {"Item": item}})
        self._batch_write(self._client(side), self._name(side, db, table),
                          items)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        declared = self._declared(side, db, table)
        self._batch_write(
            self._client(side), self._name(side, db, table),
            [{"DeleteRequest": {"Key": {
                n: self._to_attr(declared.get(n, "text"), v)
                for n, v in k.items()}}} for k in keys])

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    # the same-engine hop keeps its target following through the pair's
    # tail, which reads the stream above and applies by key

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_seed(self, db, token_path, point):
        return self._as_pair().tail_seed(db, token_path, point)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    # ---- settings, snapshot ------------------------------------------------

    #: what changes whether an item stays, or can be found: another time-to-
    #: live attribute expires other items, a missing index fails the
    #: queries that use it, another key is another table
    CRITICAL_PARAMS = ("ttl", "key", "indexes")

    def _settings(self, side, db):
        """{`table.setting`: value} of every table: key, billing, stream,
        encryption, class, deletion protection, indexes, time to live and
        point-in-time recovery. A setting the endpoint does not answer
        (DynamoDB Local has no backups) is said as not answered."""
        client, out = self._client(side), {}
        for t in self.neutral_tables(side, db):
            name = self._name(side, db, t)
            d = client.describe_table(TableName=name)["Table"]
            out[f"{t}.key"] = ",".join(
                f"{k['AttributeName']}:{k['KeyType']}"
                for k in d.get("KeySchema", []))
            out[f"{t}.billing"] = str((d.get("BillingModeSummary") or {})
                                      .get("BillingMode", "PROVISIONED"))
            spec = d.get("StreamSpecification") or {}
            out[f"{t}.stream"] = (str(spec.get("StreamViewType"))
                                  if spec.get("StreamEnabled") else "off")
            out[f"{t}.encryption"] = str((d.get("SSEDescription") or {})
                                         .get("SSEType", "owned"))
            out[f"{t}.class"] = str((d.get("TableClassSummary") or {})
                                    .get("TableClass", "STANDARD"))
            out[f"{t}.deletion_protection"] = str(
                d.get("DeletionProtectionEnabled", False))
            out[f"{t}.indexes"] = ";".join(sorted(
                f"{i['IndexName']}({','.join(k['AttributeName'] for k in i['KeySchema'])}"
                f"/{i.get('Projection', {}).get('ProjectionType')})"
                for i in (d.get("GlobalSecondaryIndexes") or [])
                + (d.get("LocalSecondaryIndexes") or [])))
            for setting, ask in (
                    ("ttl", lambda: client.describe_time_to_live(
                        TableName=name)["TimeToLiveDescription"]),
                    ("pitr", lambda: client.describe_continuous_backups(
                        TableName=name)["ContinuousBackupsDescription"])):
                try:
                    got = ask()
                except Exception as e:  # noqa: BLE001 - said as such
                    out[f"{t}.{setting}"] = ("not answered: "
                                             + str(e).split(":")[-1][:60])
                    continue
                if setting == "ttl":
                    out[f"{t}.ttl"] = (f"{got.get('AttributeName')}"
                                       if got.get("TimeToLiveStatus") in
                                       ("ENABLED", "ENABLING") else "off")
                else:
                    out[f"{t}.pitr"] = str(
                        (got.get("PointInTimeRecoveryDescription") or {})
                        .get("PointInTimeRecoveryStatus", "DISABLED"))
        return out

    def check_params(self, db):
        """Each table's own settings, both sides, through the report every
        engine's settings go through: a table has them, not a server."""
        def pull(side):
            try:
                return self._settings(side, db)
            except Exception as e:  # noqa: BLE001 - said by the report
                return {self.UNREADABLE: str(e).splitlines()[0][:80]}
        src, dst = pull("src"), pull("dst")
        crit = sorted({n for n in set(src) | set(dst)
                       if n.rsplit(".", 1)[-1] in self.CRITICAL_PARAMS})
        return self._param_result(
            db, src, dst, crit,
            "give the target's tables the source's time to live and"
            " indexes before cutover")

    def snapshot_state(self, db, state_dir, kind="all"):
        """A backup of every target table, taken by the service
        (`CreateBackup`), its ARN kept; where the endpoint takes none
        (DynamoDB Local), the table's settings and item count are kept and
        the reason said."""
        client, out = self._client("dst"), {}
        for t in self.neutral_tables("dst", db):
            name = self._name("dst", db, t)
            entry = {"items": sum(1 for _ in self._scan(
                "dst", db, t, self._key_schema("dst", db, t)))}
            try:
                got = client.create_backup(
                    TableName=name, BackupName=f"migkit-{state_dir.name}"
                    [:255])
                entry["backup"] = got["BackupDetails"]["BackupArn"]
            except Exception as e:  # noqa: BLE001 - recorded
                entry["no_backup"] = str(e).split(":")[-1].strip()[:160]
            out[t] = entry
        out["settings"] = self._settings("dst", db)
        (state_dir / "dst-tables.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    # ---- items copied as the service holds them ----------------------------

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table's items copied as the service holds them - no value
        converted, so a map, a list or a set arrives as it was - by a
        parallel scan, one segment per worker (as many as the move worked
        out it may run), each writing its items back 25 at a time. A table
        the target lacks is made with the source's key, billing and
        secondary indexes; one it has is emptied first."""
        steps = [f"{t}: items copied as they are, by a parallel scan"
                 for t in tables]
        if not go:
            return steps
        from concurrent.futures import ThreadPoolExecutor
        segments = max(1, int(getattr(self.hop, "workers_most", 0)
                              or self.hop.workers or 1))
        for t in list(shape_only) + list(tables):
            if t not in self.neutral_tables("dst", db):
                self._made_like(db, t)
        for t in tables:
            self.neutral_empty("dst", db, t)
            src, dst = self._name("src", db, t), self._name("dst", db, t)

            def segment(i, src=src, dst=dst):
                reader, writer = self._client("src"), self._client("dst")
                start, moved = None, 0
                while True:
                    got = reader.scan(TableName=src, Segment=i,
                                      TotalSegments=segments,
                                      **({"ExclusiveStartKey": start}
                                         if start else {}))
                    items = got.get("Items", [])
                    self._batch_write(writer, dst, [
                        {"PutRequest": {"Item": item}} for item in items])
                    moved += len(items)
                    start = got.get("LastEvaluatedKey")
                    if not start:
                        return moved
            with ThreadPoolExecutor(max_workers=segments) as pool:
                moved = sum(pool.map(segment, range(segments)))
            self._describe_like(db, t)
            if log:
                log(f"{t}: {moved:,} items copied in {segments} segments")
        return steps

    def _made_like(self, db, table):
        """The target table made with the source's key, billing and
        secondary indexes."""
        client = self._client("src")
        d = client.describe_table(TableName=self._name("src", db, table))[
            "Table"]
        billing = (d.get("BillingModeSummary") or {}).get(
            "BillingMode", "PROVISIONED")
        kw = {"TableName": self._name("dst", db, table),
              "KeySchema": d["KeySchema"],
              "AttributeDefinitions": d["AttributeDefinitions"],
              "BillingMode": billing}
        if billing == "PROVISIONED":
            p = d.get("ProvisionedThroughput") or {}
            kw["ProvisionedThroughput"] = {
                "ReadCapacityUnits": int(p.get("ReadCapacityUnits") or 5),
                "WriteCapacityUnits": int(p.get("WriteCapacityUnits") or 5)}
        for kind in ("GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
            if d.get(kind):
                kw[kind] = [{k: i[k] for k in ("IndexName", "KeySchema",
                                               "Projection")}
                            | ({"ProvisionedThroughput": {
                                "ReadCapacityUnits": int(
                                    i["ProvisionedThroughput"]
                                    ["ReadCapacityUnits"] or 5),
                                "WriteCapacityUnits": int(
                                    i["ProvisionedThroughput"]
                                    ["WriteCapacityUnits"] or 5)}}
                               if billing == "PROVISIONED"
                               and kind == "GlobalSecondaryIndexes"
                               else {})
                            for i in d[kind]]
        dst = self._client("dst")
        dst.create_table(**kw)
        dst.get_waiter("table_exists").wait(TableName=kw["TableName"])

    def _describe_like(self, db, table):
        """The source's description of a table migkit made, where it keeps
        one, given to the copy - so it reads its values back the same."""
        described = self._described("src", db, table)
        if described is None or self._described("dst", db, table):
            return
        src, client = self._client("src"), self._client("dst")
        arn = client.describe_table(TableName=self._name(
            "dst", db, table))["Table"]["TableArn"]
        try:
            tags = [t for t in src.list_tags_of_resource(
                ResourceArn=src.describe_table(TableName=self._name(
                    "src", db, table))["Table"]["TableArn"]).get("Tags", [])
                if t["Key"].startswith("migkit:")]
            if tags:
                client.tag_resource(ResourceArn=arn, Tags=tags)
                return
        except Exception as e:  # noqa: BLE001 - an endpoint without tags
            if "not currently supported" not in str(e):
                raise
        path = self._kept_here("dst")
        try:
            kept = json.loads(path.read_text())
        except (OSError, ValueError):
            kept = {}
        kept[arn] = {"columns": [list(c) for c in described[0]],
                     "key": list(described[1])}
        path.write_text(json.dumps(kept))
