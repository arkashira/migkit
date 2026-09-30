"""A DynamoDB table copied through the service's own export to S3, and its
import from S3 (backlog F7.ddbexp), beside migkit's own parallel scan.

The export reads the table as of one moment from its point-in-time
recovery and costs it no read capacity: a table under load, or one too
large to scan within its budget, is copied off it. What it wrote is then
taken into the target one of two ways - by the service's own import, which
makes the target table from the files and costs no write capacity (a new
table only, and never one with local secondary indexes, which the import
cannot make), or by migkit reading the files and writing their items into
the table it made (`load`). The hop names where the export goes
(`export_bucket: s3://bucket/prefix`); each export has a place of its own
under it, removed once the table is copied.

What migkit holds each to: the export's own manifest, where the service
writes one - every file's item count and MD5, read back - and its count of
items otherwise; the import's count of what it imported against the
export's, no error allowed (the import overwrites duplicates without
counting them, and a failed one leaves a partial table, which migkit
removes and copies another way); and after either, the comparison with the
source every bulk copy is held to (`cli._held_to_the_source`). An export
is as of its moment: a hop that follows the source goes on from the
position taken before the copy began, and a change applied twice lands on
itself by its key.

The service's prices are said before it runs: about $0.10 a GB of the
table exported and $0.15 a GB imported (us-east-1, 2026-09). No real
account is touched by migkit's tests: they run against moto, which models
both calls but writes no manifest - which is why the export's own count is
the proof where no manifest is.
"""
import base64
import gzip
import hashlib
import io
import json
import time

from . import tooluse
from .tooluse import UNUSED, USED

#: bytes a table has at least before its copy waits on an export: the
#: service gives no time for one (no SLA), and the quickest reported take
#: minutes, which a parallel scan of a smaller table beats. Not measured
#: here - no real account is touched - so it is a line drawn, not a cost
EXPORT_FROM = 2 ** 30
#: seconds between two looks at an export or an import under way, at most
POLL_MOST = 30


def where(hop):
    """(bucket, prefix) the hop names for its exports, or None."""
    raw = str((hop.options or {}).get("export_bucket") or "").strip()
    if not raw.startswith("s3://"):
        return None
    bucket, _, prefix = raw[5:].partition("/")
    return (bucket, prefix.strip("/")) if bucket else None


def _s3(eng, side):
    import boto3
    ep = eng.hop.source if side == "src" else eng.hop.target
    kw = {"region_name": ep.options.get("region") or "us-east-1"}
    end = ep.options.get("endpoint_url") or (
        ep.host if str(ep.host).startswith("http") else None)
    if end:
        kw["endpoint_url"] = end
    if ep.user:
        kw.update(aws_access_key_id=ep.user,
                  aws_secret_access_key=ep.password)
    return boto3.client("s3", **kw)


def export_unfit(eng, db, table):
    """Why the source's export cannot carry this table, or None: no place
    named for it, or the table's point-in-time recovery off - turning it on
    is a setting of the source, which migkit never changes."""
    if where(eng.hop) is None:
        return "the hop names no export_bucket for the source's export"
    got = eng._client("src").describe_continuous_backups(
        TableName=eng._name("src", db, table))
    pitr = ((got.get("ContinuousBackupsDescription") or {}).get(
        "PointInTimeRecoveryDescription") or {}).get(
        "PointInTimeRecoveryStatus")
    if pitr != "ENABLED":
        return ("the table's point-in-time recovery is off, and turning it"
                " on is the source's setting")
    return None


def import_unfit(eng, db, table):
    """Why the service's import cannot make this table, or None: it makes
    a new table only, and none with local secondary indexes."""
    if table in eng.neutral_tables("dst", db):
        return "the target has the table already, and the import makes a" \
               " new one only"
    d = eng._client("src").describe_table(
        TableName=eng._name("src", db, table))["Table"]
    if d.get("LocalSecondaryIndexes"):
        return "the table has local secondary indexes, which the import" \
               " cannot make"
    return None


def table_bytes(eng, db, table):
    return int(eng._client("src").describe_table(
        TableName=eng._name("src", db, table))["Table"].get(
        "TableSizeBytes") or 0)


def _wait(describe, key, status, label, log):
    """The description once its status is no longer IN_PROGRESS, looked
    at more seldom the longer it runs."""
    began, pause, said = time.monotonic(), 1.0, 0.0
    while True:
        got = describe()[key]
        if got.get(status) != "IN_PROGRESS":
            return got
        if log and time.monotonic() - said > 60:
            said = time.monotonic()
            log(f"{label}: still under way after"
                f" {int(time.monotonic() - began)}s")
        time.sleep(pause)
        pause = min(pause * 2, POLL_MOST)


def export(eng, db, table, log):
    """The source's export of `table` as of now, finished: (its
    description, the S3 prefix everything of it is under)."""
    bucket, prefix = where(eng.hop)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    place = "/".join(p for p in (prefix, f"migkit-{eng.hop.name}-{db}-"
                                         f"{table}-{stamp}") if p)
    client = eng._client("src")
    arn = client.describe_table(TableName=eng._name("src", db, table))[
        "Table"]["TableArn"]
    if log:
        size = table_bytes(eng, db, table)
        log(f"{table}: exported as of now ({size / 2 ** 30:.2f} GB, about"
            f" ${size / 2 ** 30 * 0.10:.2f} at the service's price)")
    got = client.export_table_to_point_in_time(
        TableArn=arn, S3Bucket=bucket, S3Prefix=place,
        ExportFormat="DYNAMODB_JSON", ExportType="FULL_EXPORT")
    ex_arn = got["ExportDescription"]["ExportArn"]
    done = _wait(lambda: client.describe_export(ExportArn=ex_arn),
                 "ExportDescription", "ExportStatus", table, log)
    if done.get("ExportStatus") != "COMPLETED":
        raise RuntimeError(f"{table}: the export ended"
                           f" {str(done.get('ExportStatus')).lower()}:"
                           f" {done.get('FailureMessage') or ''}".strip())
    return done, place


def files(eng, bucket, place, done):
    """[(key, items, md5 or None)] of the export's data files: from its
    manifest where the service wrote one (`manifest-files.json`, a line a
    file), every `.gz` under its place otherwise."""
    s3 = _s3(eng, "src")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=place + "/"):
        keys += [o["Key"] for o in page.get("Contents") or []]
    manifest = next((k for k in keys if k.endswith("manifest-files.json")),
                    None)
    if manifest:
        body = s3.get_object(Bucket=bucket, Key=manifest)["Body"].read()
        out = []
        for line in body.decode().splitlines():
            if line.strip():
                row = json.loads(line)
                out.append((row["dataFileS3Key"], int(row["itemCount"]),
                            row.get("md5Checksum")))
        return out
    return [(k, None, None) for k in sorted(keys) if k.endswith(".gz")]


def _items(s3, bucket, key, items, md5):
    """The items of one data file, held to its manifest's MD5 and count
    where it has them."""
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    if md5 and base64.b64encode(hashlib.md5(
            raw, usedforsecurity=False).digest()).decode() != md5:
        raise RuntimeError(f"{key}: the file read back is not the one the"
                           " export wrote (its MD5 differs)")
    from .engines.dynamodb import _restored
    # bytes arrive as their base64 text, which the client would encode
    # again: each attribute is given back as the service holds it
    out = [{k: _restored(v) for k, v in json.loads(line)["Item"].items()}
           for line in gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
           .decode().splitlines() if line.strip()]
    if items is not None and len(out) != items:
        raise RuntimeError(f"{key}: {len(out)} items read where the export"
                           f" wrote {items}")
    return out


def load(eng, db, table, bucket, place, done):
    """The export's items written into the target's table, 25 at a time;
    the items written. Their count must be the export's."""
    s3 = _s3(eng, "src")
    name = eng._name("dst", db, table)
    client = eng._client("dst")
    n = 0
    for key, items, md5 in files(eng, bucket, place, done):
        got = _items(s3, bucket, key, items, md5)
        eng._batch_write(client, name, [{"PutRequest": {"Item": i}}
                                        for i in got])
        n += len(got)
    want = int(done.get("ItemCount") or 0)
    if n != want:
        raise RuntimeError(f"{table}: {n:,} items read from the export"
                           f" where it counted {want:,}")
    return n


def _data_prefix(eng, bucket, place, done):
    """The common folder of the export's data files: what the import is
    pointed at, so it reads no manifest as data."""
    import os
    keys = [k for k, _, _ in files(eng, bucket, place, done)]
    if not keys:
        return place + "/"
    return os.path.commonprefix([k.rsplit("/", 1)[0] + "/" for k in keys])


def import_(eng, db, table, bucket, place, done, log):
    """The target's table made by the service's import of the export: the
    source's key, billing and global indexes, its items from the files.
    Its imported count must be the export's, with no error; a failed or
    short import leaves a partial table, which is removed. The items
    imported."""
    src = eng._client("src").describe_table(
        TableName=eng._name("src", db, table))["Table"]
    billing = (src.get("BillingModeSummary") or {}).get(
        "BillingMode", "PROVISIONED")
    params = {"TableName": eng._name("dst", db, table),
              "AttributeDefinitions": src["AttributeDefinitions"],
              "KeySchema": src["KeySchema"], "BillingMode": billing}
    if billing == "PROVISIONED":
        p = src.get("ProvisionedThroughput") or {}
        params["ProvisionedThroughput"] = {
            "ReadCapacityUnits": int(p.get("ReadCapacityUnits") or 5),
            "WriteCapacityUnits": int(p.get("WriteCapacityUnits") or 5)}
    if src.get("GlobalSecondaryIndexes"):
        params["GlobalSecondaryIndexes"] = [
            {k: i[k] for k in ("IndexName", "KeySchema", "Projection")}
            | ({"ProvisionedThroughput": {
                "ReadCapacityUnits": int(i["ProvisionedThroughput"]
                                         ["ReadCapacityUnits"] or 5),
                "WriteCapacityUnits": int(i["ProvisionedThroughput"]
                                          ["WriteCapacityUnits"] or 5)}}
               if billing == "PROVISIONED" else {})
            for i in src["GlobalSecondaryIndexes"]]
    client = eng._client("dst")
    if log:
        size = int(done.get("BilledSizeBytes") or 0)
        log(f"{table}: imported from the export ({size / 2 ** 30:.2f} GB,"
            f" about ${size / 2 ** 30 * 0.15:.2f} at the service's price)")
    got = client.import_table(
        S3BucketSource={"S3Bucket": bucket,
                        "S3KeyPrefix": _data_prefix(eng, bucket, place,
                                                    done)},
        InputFormat="DYNAMODB_JSON", InputCompressionType="GZIP",
        TableCreationParameters=params)
    arn = got["ImportTableDescription"]["ImportArn"]
    fin = _wait(lambda: client.describe_import(ImportArn=arn),
                "ImportTableDescription", "ImportStatus", table, log)
    want = int(done.get("ItemCount") or 0)
    ok = (fin.get("ImportStatus") == "COMPLETED"
          and not int(fin.get("ErrorCount") or 0)
          and int(fin.get("ImportedItemCount") or 0) == want)
    if not ok:
        try:
            client.delete_table(TableName=params["TableName"])
        except Exception:  # noqa: BLE001 - said below either way
            pass
        raise RuntimeError(
            f"{table}: the import ended {str(fin.get('ImportStatus')).lower()}"
            f" with {fin.get('ImportedItemCount')} of {want:,} items and"
            f" {fin.get('ErrorCount')} errors; the partial table it made is"
            " removed")
    client.get_waiter("table_exists").wait(TableName=params["TableName"])
    return want


def carry(eng, db, table, by_import, log):
    """The table exported and taken into the target - by the service's
    import (`by_import`), or by migkit's writer into the table it made -
    and the export's files removed however it ends. The items copied."""
    done, place = export(eng, db, table, log)
    bucket, _ = where(eng.hop)
    try:
        if by_import:
            return import_(eng, db, table, bucket, place, done, log)
        return load(eng, db, table, bucket, place, done)
    finally:
        forget(eng, place)


def forget(eng, place):
    """The export's files removed: they are a copy of the table, and the
    table is copied."""
    bucket, _ = where(eng.hop)
    s3 = _s3(eng, "src")
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=place + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents") or []]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


# ---- the account of what is used (backlog 0f) ----------------------------

SURFACE = {
    "ExportTableToPointInTime": frozenset({
        "TableArn", "ExportTime", "ClientToken", "S3Bucket",
        "S3BucketOwner", "S3Prefix", "S3SseAlgorithm", "S3SseKmsKeyId",
        "ExportFormat", "ExportType", "IncrementalExportSpecification"}),
    "ImportTable": frozenset({
        "ClientToken", "S3BucketSource", "InputFormat",
        "InputFormatOptions", "InputCompressionType",
        "TableCreationParameters"}),
}


def _account():
    o = {}

    def put(call, option, state, why):
        o[(call, option)] = (state, why)
    e, i = "ExportTableToPointInTime", "ImportTable"
    put(e, "TableArn", USED, "the source table, asked of the service")
    put(e, "ExportTime", UNUSED, "its default, now: the copy's position"
        " is taken just before it")
    put(e, "ClientToken", UNUSED, "one export a table a copy: a copy run"
        " again exports again, to a place of its own")
    put(e, "S3Bucket", USED, "the bucket the hop names in export_bucket")
    put(e, "S3BucketOwner", UNUSED, "not yet: a bucket of another account"
        " is reached by its own policy")
    put(e, "S3Prefix", USED, "a place of each export's own under the hop's"
        " prefix, removed once the table is copied")
    for f in ("S3SseAlgorithm", "S3SseKmsKeyId"):
        put(e, f, UNUSED, "the bucket's own encryption applies; migkit"
            " names no key of the operator's")
    put(e, "ExportFormat", USED, "DYNAMODB_JSON: every attribute as the"
        " service holds it, a map, a list or a set as it was")
    put(e, "ExportType", USED, "FULL_EXPORT: the table as of one moment;"
        " the changes after it are followed from the copy's position")
    put(e, "IncrementalExportSpecification", UNUSED, "not yet: the changes"
        " after the copy are followed through the table's stream")
    put(i, "ClientToken", UNUSED, "one import a table a copy, into a table"
        " that does not exist yet")
    put(i, "S3BucketSource", USED, "the export's data files alone, so no"
        " manifest is read as items")
    put(i, "InputFormat", USED, "DYNAMODB_JSON, as the export wrote it")
    put(i, "InputFormatOptions", UNUSED, "options of the CSV format, which"
        " no export writes")
    put(i, "InputCompressionType", USED, "GZIP, as the export wrote it")
    put(i, "TableCreationParameters", USED, "the source's key, billing and"
        " global indexes, as migkit makes a table itself")
    return o


tooluse.register("dynamodb-s3", licence="Apache-2.0", options=_account(),
                 declared=SURFACE)
