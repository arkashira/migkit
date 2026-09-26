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
        cls = canon.type_class("dynamodb", declared)
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
            return datetime.datetime.fromisoformat(raw)
        if kind == "S" and cls == "date":
            return datetime.date.fromisoformat(raw)
        if kind == "S" and cls == "time":
            return datetime.time.fromisoformat(raw)
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
        from .. import canon, rowtext
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            for row in batch:
                total = canon.digest_step(total, rowtext.encode(
                    [canon.render_value(c, v) for c, v in zip(classes, row)]))
                n += 1
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
        name, cls, numbers = col[0], col[1], tuple(col[2] or ())
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
