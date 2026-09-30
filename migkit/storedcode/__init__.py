"""Stored code carried to another engine and proved there (backlog R11).

A view, function, procedure, trigger or event is converted one at a time,
by whichever candidate proves itself on it (`rungs`), and proved by running
the source's and the target's on the same fixture in a sandbox and
comparing everything they did (`prove`). Nothing here runs a routine on the
source or the target themselves: the sandbox holds copies of both.
"""
