"""Rows counted and fingerprinted as they pass through a copy.

A copier that streams rows from one server to the other can say, once the
target has committed them, whether the target now holds what passed - by
reading the same rows back from the target and tallying them the same way.
Read-back rather than re-reading the source: a source that is being written
while it is copied has moved on since, and comparing against it would call
a correct copy wrong.

The tally is a count and a sum of per-row hashes, so the order the rows
arrive in does not matter: a target that hands them back in another order
is not a difference.
"""
import hashlib

#: a value longer than this is hashed as its rendering a piece at a time
PIECE = 1 << 20


def _md5(data=b""):
    """A row's digest: a fingerprint to compare by, not for security."""
    return hashlib.md5(data, usedforsecurity=False)


class Tally:
    def __init__(self):
        self.n = 0
        self.total = 0
        # the row a chunk ended inside, hashed as far as it has come
        self._open = None

    def row(self, data):
        """One row, as bytes or as values.

        Values are hashed as the tuple's rendering (`repr`), and a value
        longer than `PIECE` is rendered and hashed a piece at a time: the
        whole rendering of a byte value is several times its size - each
        byte that is not printable becomes four characters, then encoded
        once more - and was held whole for every row. Measured, a 100 MB
        random value: 646 MB held at once that way, 10 MB a piece at a
        time. The digest is the same either way."""
        if isinstance(data, (bytes, bytearray)):
            return self._add(_md5(bytes(data)))
        data = tuple(data)
        if not any(_long(v) for v in data):
            return self._add(_md5(repr(data).encode()))
        h = _md5(b"(")
        for i, v in enumerate(data):
            if i:
                h.update(b", ")
            for piece in (_rendered(v) if _long(v) else (repr(v),)):
                h.update(piece.encode())
        h.update(b",)" if len(data) == 1 else b")")
        self._add(h)

    def _add(self, digest):
        self.n += 1
        self.total += int(digest.hexdigest()[:16], 16)

    def feed(self, chunk):
        """A piece of a stream of newline-ended rows, as a COPY in text
        format writes them: a newline inside a value is written escaped,
        so each line is one row.

        A row a chunk ends inside is hashed as far as it has come, not
        kept: the rest of it used to be joined to every next chunk before
        splitting, so a row of a 300 MB value was copied whole once for
        each 64 KB piece of it - a quadratic time and the row twice over in
        memory: measured, a 64 MB row in 64 KB pieces took 16.5s that way
        and 0.11s hashed as it comes. The digest is the same either way."""
        lines = chunk.split(b"\n")
        last = lines.pop()
        if lines:
            first = lines[0]
            if self._open is not None:
                self._open.update(first)
                self._add(self._open)
                self._open = None
            else:
                self.row(first)
            for line in lines[1:]:
                self.row(line)
        if last:
            if self._open is None:
                self._open = _md5()
            self._open.update(last)

    def end(self):
        if self._open is not None:
            self._add(self._open)
            self._open = None
        return self

    def value(self):
        return (self.n, self.total)

    def __eq__(self, other):
        return isinstance(other, Tally) and self.value() == other.value()

    def __repr__(self):
        return f"Tally({self.n} rows)"


def _long(v):
    return type(v) in (bytes, bytearray, str) and len(v) > PIECE


def _rendered(v):
    """`repr(v)` of a long string or byte value, in pieces. Each piece is
    rendered on its own and put in the whole value's quotes: Python quotes
    with ' unless the value holds ' and no ", and a piece that chose the
    other quote leaves its ' bare where the whole value escapes it - but
    a bytearray escapes every ' whichever quote it chose."""
    kind = type(v)
    quote, other = ("'", '"') if kind is str else (b"'", b'"')
    q = '"' if quote in v and other not in v else "'"
    head, tail = {str: ("", 1), bytes: ("b", 1),
                  bytearray: ("bytearray(b", 2)}[kind]
    yield head + q
    for at in range(0, len(v), PIECE):
        r = repr(v[at:at + PIECE])
        inner = r[len(head) + 1:-tail]
        if kind is not bytearray and r[-1] != q:
            # the piece chose " for holding ' and no ": the whole value
            # holds a " somewhere, and escapes its '
            inner = inner.replace("'", "\\'")
        yield inner
    yield q + (")" if kind is bytearray else "")
