"""Generate a table with the shape real data has, and load it.

The point of this is the numbers in `docs/scale.md`: migkit is not allowed to
claim it works at scale until somebody has run it at scale and written down
what happened. This is the somebody.

Why the data is generated rather than copied from anywhere: no production
data leaves its home, and a benchmark on data nobody can share is a benchmark
nobody can repeat. Faker gives text with the awkward parts real text has -
accents, apostrophes, varying lengths, the occasional empty string.

Why a pool rather than a row at a time: Faker costs roughly a millisecond per
field, which is minutes per million rows spent inside Faker rather than
inside the database. The pool keeps the shape - length distribution, unicode,
cardinality - at a cost that does not dominate the measurement. The pool size
is recorded with the results, because it is the cardinality of the columns.

    python bench/seed.py --dsn postgresql://... --rows 1000000

Everything it creates is named `bench_*`, so a sandbox can be cleaned by
dropping those.
"""
import argparse
import datetime
import io
import random
import time


POOL = 20000


def pools(seed, size=POOL):
    """Columns' worth of values, generated once."""
    from faker import Faker
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    return {
        "name": [fake.name() for _ in range(size)],
        "email": [fake.email() for _ in range(size)],
        "city": [fake.city() for _ in range(size)],
        # free text with newlines and punctuation in it, which is what
        # makes a row encoding earn its keep
        "note": [fake.paragraph(nb_sentences=3) for _ in range(size // 10)],
        "tag": [fake.word() for _ in range(200)],
    }


DDL = """
create table if not exists bench_rows (
    id          bigint primary key,
    name        text not null,
    email       text not null,
    city        text,
    note        text,
    tag         text,
    amount      numeric(14,4),
    qty         integer,
    ok          boolean,
    created_at  timestamptz,
    payload     jsonb
);
"""


def rows(count, seed):
    """One COPY-ready line per row, as text."""
    pool = pools(seed)
    rnd = random.Random(seed)
    base = 1_600_000_000
    for i in range(1, count + 1):
        name = pool["name"][rnd.randrange(len(pool["name"]))]
        email = pool["email"][rnd.randrange(len(pool["email"]))]
        city = pool["city"][rnd.randrange(len(pool["city"]))]
        note = pool["note"][rnd.randrange(len(pool["note"]))]
        tag = pool["tag"][rnd.randrange(len(pool["tag"]))]
        # a tenth of the rows have a NULL city and an empty note, because a
        # table where every column is populated is not a table anybody has
        if i % 10 == 0:
            city = None
            note = ""
        amount = f"{rnd.randrange(0, 10_000_000) / 100:.4f}"
        created = datetime.datetime.fromtimestamp(
            base + i * 7, datetime.timezone.utc).isoformat()
        payload = ('{"src":"bench","n":%d,"t":"%s"}' % (i % 997, tag))
        yield (i, name, email, city, note, tag, amount,
               rnd.randrange(0, 1000), "true" if i % 3 else "false",
               created, payload)


def copy_text(batch):
    """Postgres COPY text format: tabs separate, backslash escapes, \\N is
    NULL - the same rules migkit's own key files had to learn."""
    out = io.StringIO()
    for row in batch:
        fields = []
        for value in row:
            if value is None:
                fields.append("\\N")
            else:
                text = str(value)
                fields.append(text.replace("\\", "\\\\").replace("\t", "\\t")
                              .replace("\n", "\\n").replace("\r", "\\r"))
        out.write("\t".join(fields) + "\n")
    return out.getvalue()


def load(dsn, count, seed, batch_size=50_000, quiet=False):
    import psycopg2
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    started = time.monotonic()
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("truncate bench_rows")
        batch, n = [], 0
        for row in rows(count, seed):
            batch.append(row)
            n += 1
            if len(batch) >= batch_size:
                _copy(cur, batch)
                batch = []
                if not quiet:
                    print(f"  {n:,} rows  {time.monotonic() - started:6.1f}s",
                          flush=True)
        if batch:
            _copy(cur, batch)
    with conn.cursor() as cur:
        cur.execute("analyze bench_rows")
        cur.execute("select count(*), pg_size_pretty("
                    "pg_total_relation_size('bench_rows')) from bench_rows")
        got, size = cur.fetchone()
    conn.close()
    return {"rows": got, "size": size,
            "seconds": round(time.monotonic() - started, 1)}


def _copy(cur, batch):
    buf = io.StringIO(copy_text(batch))
    cur.copy_expert(
        "copy bench_rows (id, name, email, city, note, tag, amount, qty,"
        " ok, created_at, payload) from stdin", buf)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--seed", type=int, default=20260919)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    got = load(args.dsn, args.rows, args.seed, quiet=args.quiet)
    print(f"loaded {got['rows']:,} rows, {got['size']}, "
          f"{got['seconds']}s")


if __name__ == "__main__":
    main()
