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


class Tally:
    def __init__(self):
        self.n = 0
        self.total = 0
        self._rest = b""

    def row(self, data):
        """One row, as bytes or as values."""
        if not isinstance(data, (bytes, bytearray)):
            data = repr(tuple(data)).encode()
        self.n += 1
        self.total += int(hashlib.md5(bytes(data)).hexdigest()[:16], 16)

    def feed(self, chunk):
        """A piece of a stream of newline-ended rows, as a COPY in text
        format writes them: a newline inside a value is written escaped,
        so each line is one row."""
        lines = (self._rest + chunk).split(b"\n")
        self._rest = lines.pop()
        for line in lines:
            self.row(line)

    def end(self):
        if self._rest:
            self.row(self._rest)
            self._rest = b""
        return self

    def value(self):
        return (self.n, self.total)

    def __eq__(self, other):
        return isinstance(other, Tally) and self.value() == other.value()

    def __repr__(self):
        return f"Tally({self.n} rows)"
