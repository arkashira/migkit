"""The topics of a running Kafka tail, each in a state of its own (backlog
P0-PARITY.tail; the SQL tail's are `tailtables`).

One tail, one saved position - each topic's partitions' next offsets, in
the tail's token - and beside every topic's offsets the state it is in:

    live      its messages copied as they arrive
    joining   added to the tail: copied up to the source's end offsets at
              the moment it joined (the seam, `Engine.snapshot_mark`),
              and live from there. A message is the copy's where the seam
              covers it (`Engine.mark_covers`) and the stream's where it
              does not - no offset is in both and none in neither
    left      out of the hop's scope or gone from the source: its saved
              offsets kept, and what its topic on the target held when it
              left, for a topic that comes back
    parked    a change of the topic the target does not follow yet (more
              partitions, the log recreated, a topic made while the hop's
              `ddl` parks): not copied, said when it parks and in the
              tail's count of states, and held against the drain - a
              cutover does not pass a topic that was left behind. One
              parked for its partitions is followed again once the target
              has them; the others when the hop's scope changes or the
              tail starts again, held to what they were parked for

A topic put back in the scope, or one that left and returns, is copied
from the offsets it saved: only the messages since. Kafka's log is read
again from any offset still in it, so nothing is copied twice and nothing
is read from the beginning. It resumes only if the target's topic is as
it was left (a message written there meanwhile would be taken for one
the copy sent); if the source's log was recreated or cut below the saved
offsets, or the target's lost messages, the topic is not resumed -
`ddl` says whether the tail stops or the topic is parked.

What a change of a topic does is the hop's `ddl` (`tailtables.policy`,
per topic like a table's):

    stop    the tail stops with the reason, before the topic is touched
    park    the topic is parked, and only it
    apply   what can be carried at its place: a topic made on the source
            is joined at its seam, one deleted leaves (the target's topic
            is never deleted); the rest parks - more partitions on the
            source (raising the target's count is left to the operator, a
            key would land in another partition meanwhile; the topic is
            followed again once the target has as many), a log recreated or
            cut, a topic back to a target that was written

With no `ddl` in the hop a topic made on the source is followed from its
first message, as the tail always did, and a topic that grew partitions or
was recreated stops the tail, as the copy always did.

The states are written with the offsets, in the one file that moves them,
before the topic is copied: a stop at any point resumes in the state it
was in.
"""
import json
import time

LIVE, JOINING, LEFT, PARKED = "live", "joining", "left", "parked"
STATES = (LIVE, JOINING, LEFT, PARKED)
PREFIX = "topic."


def _state(st):
    """A topic's state, `live` for one a copy left with none."""
    got = (st or {}).get("state") if isinstance(st, dict) else None
    return got if got in STATES else LIVE


class TopicSet:
    """The topics of one running tail and what each is waiting for."""

    def __init__(self, eng, db, state, log):
        from . import tailtables
        self.eng, self.db, self.state, self.log = eng, db, state, log
        self.hop = eng.hop
        #: the first round of this run: a topic with no state then was
        #: never copied, whatever the hop's `ddl` says of a topic made
        #: while the tail runs
        self.first = True
        self.file = tailtables._HopFile(self.hop)

    # --- reading ----------------------------------------------------------

    def key(self, topic):
        return self.eng.move_key(self.db, "", topic)

    def known(self):
        """{topic: its entry} the tail holds a state for."""
        return {k[len(PREFIX):]: v for k, v in self.state.items()
                if k.startswith(PREFIX) and isinstance(v, dict)}

    def of(self, topic):
        """A topic's state, or None for a topic the tail never saw."""
        st = self.state.get(self.key(topic))
        return _state(st) if isinstance(st, dict) else None

    def counts(self):
        """{state: how many topics}."""
        out = {}
        for st in self.known().values():
            out[_state(st)] = out.get(_state(st), 0) + 1
        return out

    def said(self):
        """The topics' states in a line, empty where all are live."""
        got = self.counts()
        if set(got) <= {LIVE}:
            return ""
        return ", ".join(f"{n} {s}" for s, n in sorted(got.items()))

    def following(self):
        """The topics copied this round, in order: live and joining."""
        return sorted(t for t, st in self.known().items()
                      if _state(st) in (LIVE, JOINING))

    # --- the hop's word ---------------------------------------------------

    def word(self, topic, kind):
        """What a change of `kind` does to `topic`: `stop`, `park`, `apply`
        or `leave` (a kind the hop's `ddl` skips). With no `ddl` in the hop
        the tail's own ways stand: a topic made is followed (`apply`), one
        that grew or was recreated stops the tail."""
        from . import tailtables
        skipped = tailtables.skipped(self.hop)
        if kind in skipped:
            return "leave"
        if (self.hop.options or {}).get("ddl") is None:
            return "apply" if kind in ("create", "drop") else "stop"
        return tailtables.policy(self.hop, [self.db, topic])

    # --- the round --------------------------------------------------------

    def boundary(self, listing, everywhere, ranges):
        """Between rounds, before anything is copied: the hop's scope held
        to the topics' states. `listing` is {topic: partitions} in the
        hop's scope on the source, `everywhere` every topic the source has,
        `ranges(topics)` the first and next-to-last offsets of topics
        (`Engine._ranges`). Returns whether any state changed (to be
        saved before a message is copied)."""
        changed = self.file.refresh()
        released = self.first or changed
        moved = False
        known = self.known()
        if released:
            for topic, st in known.items():
                if _state(st) == PARKED:
                    moved |= self._release(topic, st)
            known = self.known()
        # the topics whose shape is read: those in scope, and those that
        # come back or leave (their target is read by name)
        want = [t for t in listing if t not in known
                or _state(known[t]) in (LIVE, JOINING, LEFT)]
        facts = ranges(want) if want else {}
        for topic in sorted(set(known) | set(listing)):
            st = known.get(topic)
            here = topic in listing
            now = _state(st) if st is not None else None
            if now is None and here:
                moved |= self._new(topic, listing[topic], facts.get(topic),
                                   released)
            elif now in (LIVE, JOINING, PARKED) and not here:
                moved |= self._gone(topic, st, topic in everywhere)
            elif now == LEFT and here:
                moved |= self._back(topic, st, listing[topic],
                                    facts.get(topic), released)
            elif now in (LIVE, JOINING) and here:
                moved |= self._shape(topic, st, listing[topic],
                                     facts.get(topic))
            elif now == PARKED and here and st.get("parked_for") == "grow":
                moved |= self._regrown(topic, st, listing[topic])
        self.first = False
        return moved

    def settle(self, topic):
        """After the topic was copied: a joining topic whose copy has
        everything its seam covers is live from here."""
        st = self.state.get(self.key(topic))
        if not isinstance(st, dict) or _state(st) != JOINING:
            return False
        try:
            seam = json.loads(st.get("seam") or "{}").get(topic) or {}
        except ValueError:
            seam = {}
        for p in seam:
            pst = (st.get("parts") or {}).get(p)
            if pst is None:
                return False
            # the first message not copied is the stream's, not the seam's
            covered = self.eng.mark_covers(
                "src", self.db, st.get("seam"),
                {"topic": topic, "partition": int(p),
                 "offset": int(pst.get("next", 0))})
            if covered is not False:
                return False
        st["state"] = LIVE
        st["joined"] = {"at": time.time(), "seam": st.get("seam")}
        st.pop("seam", None)
        self.log(f"{topic}: joined; every message up to the seam is copied,"
                 " and it is followed from there")
        return True

    # --- events -----------------------------------------------------------

    def _enter(self, topic, state, why, **more):
        st = self.state.setdefault(self.key(topic), {})
        st["state"] = state
        st["why"] = why
        st.update(more)
        return st

    def _seam(self, topic, facts):
        """The mark a joining topic is held to: its end offsets now, in
        the form `Engine.snapshot_mark` gives them."""
        ends = (facts or {}).get("end") or {}
        return json.dumps({topic: {str(p): int(e) for p, e in ends.items()}},
                          sort_keys=True)

    def _join(self, topic, n, facts, why):
        st = self._enter(topic, JOINING, why, partitions=n,
                         seam=self._seam(topic, facts))
        st.pop("left", None)
        self.log(f"{topic}: {why}; copied up to its end offsets now, and"
                 " followed from there")
        return True

    def _new(self, topic, n, facts, added):
        """A topic in scope the tail has no state for."""
        if added:
            return self._join(topic, n, facts, "added to the tail")
        word = self.word(topic, "create")
        if word == "leave":
            self._enter(topic, LEFT, "made on the source, and the hop's ddl"
                        " leaves creates out", partitions=n, by_ddl=True)
            self.log(f"{topic}: made on the source; the hop's ddl leaves it"
                     " out")
            return True
        if word == "apply":
            return self._join(topic, n, facts, "made on the source")
        if word == "park":
            self._enter(topic, PARKED, "made on the source, and the hop's"
                        " ddl parks it", partitions=n)
            self.log(f"{topic}: made on the source; parked, not copied")
            return True
        raise SystemExit(
            f"{topic}: made on the source while the tail runs, and the"
            " hop's ddl says stop. Nothing of it is copied. Start the tail"
            " again to follow it, or set ddl to apply (follow it as it is"
            " made) or park (leave it aside)")

    def _gone(self, topic, st, exists):
        """A topic the tail followed, no longer in the hop's scope."""
        if _state(st) == PARKED:
            return False
        if not exists:
            # a listing that missed a topic for a moment is not a delete:
            # asked of the topic by name before it is called one
            exists = bool((self.eng._ranges("src", [topic]).get(topic)
                           or {}).get("end"))
        why = ("taken out of the hop's scope" if exists
               else "deleted on the source")
        if not exists:
            word = self.word(topic, "drop")
            if word == "stop":
                raise SystemExit(
                    f"{topic}: deleted on the source, and the hop's ddl says"
                    " stop. Its target topic is untouched; its saved"
                    " offsets are kept")
        ends = self._target_ends(topic)
        self._enter(topic, LEFT, why, left={"at": time.time(),
                                            "target_ends": ends,
                                            "was": _state(st)})
        self.log(f"{topic}: {why}; its target topic is kept as it is, and"
                 " its offsets are kept for its return")
        return True

    def _target_ends(self, topic):
        """{partition: the target topic's end now}, {} where it has none."""
        try:
            got = self.eng._ranges("dst", [topic]).get(topic) or {}
        except Exception:  # noqa: BLE001 - not known, said on return
            return None
        return {p: int(e) for p, e in (got.get("end") or {}).items()}

    def _back(self, topic, st, n, facts, released):
        """A topic that left and is in scope again."""
        if st.get("by_ddl"):
            # left out by the hop's ddl as it was made: it stays out until
            # the hop's scope is changed or the tail started, and joins
            # then as a topic added
            if not released:
                return False
            self.state.pop(self.key(topic), None)
            return self._join(topic, n, facts, "put back in the hop's scope")
        left = st.get("left") or {}
        before, now = left.get("target_ends"), self._target_ends(topic)
        if before is None or now is None:
            return self._unfit(
                topic, st, "what its target topic held when it left, or"
                " holds now, could not be read, so nothing proves the copy"
                " may go on from its saved offsets")
        if any(now.get(p, 0) != e for p, e in before.items()) \
                or any(e for p, e in now.items() if p not in before):
            fewer = [p for p, e in before.items() if now.get(p, 0) < e]
            return self._unfit(
                topic, st,
                "its target topic holds "
                + ("fewer messages than when it left" if fewer
                   else "messages written there since it left")
                + ": the copy's count of what landed would be wrong, and"
                  " nothing here can say whose they are")
        st["state"] = left.get("was") if left.get("was") in (
            LIVE, JOINING) else LIVE
        st["why"] = "put back"
        st.pop("left", None)
        st.pop("by_ddl", None)
        self._shape(topic, st, n, facts)
        if st["state"] in (LIVE, JOINING):
            self.log(f"{topic}: put back; only the messages since it left"
                     " are copied, from its saved offsets")
        return True

    def _unfit(self, topic, st, why):
        """A returning topic that cannot be resumed: the hop's `ddl`
        stops the tail or parks the topic."""
        if self.word(topic, "reset") in ("park", "apply"):
            st["state"], st["why"] = PARKED, why
            self.log(f"{topic}: parked - {why}")
            return True
        raise SystemExit(
            f"{topic}: came back to the tail, and {why}. Nothing of it is"
            " copied. Set ddl to park to leave it aside and go on with the"
            " other topics")

    def _shape(self, topic, st, n, facts):
        """A followed topic's partitions and log held to what the tail
        saved of it."""
        moved = False
        saved = int(st.get("partitions") or len(st.get("parts") or {}) or 0)
        if saved and n > saved:
            theirs = len(self.eng._ranges("dst", [topic]).get(topic, {})
                         .get("end") or {})
            if theirs >= n:
                self.log(f"{topic}: {saved} partitions on the source became"
                         f" {n}; the target has {theirs}, and the new ones"
                         " are copied from their first message")
            else:
                word = self.word(topic, "grow")
                why = (f"{saved} partitions on the source became {n}, the"
                       f" target's topic has {theirs}: a key would land in"
                       " another partition. Raise the target's partitions to"
                       f" {n}")
                if word in ("park", "apply"):
                    st["state"], st["why"] = PARKED, why
                    st["parked_for"] = "grow"
                    self.log(f"{topic}: parked - {why}")
                    return True
                raise SystemExit(f"{topic}: {why}, and start the tail again")
        if st.get("partitions") != n:
            st["partitions"] = n
            moved = True
        ends = (facts or {}).get("end") or {}
        cut = [p for p, pst in (st.get("parts") or {}).items()
               if p in ends and int(pst.get("next", 0)) > int(ends[p])]
        if cut:
            word = self.word(topic, "truncate")
            why = (f"its log on the source ends below the offsets the tail"
                   f" saved (partitions {', '.join(sorted(cut)[:5])}): it was"
                   " recreated or cut, and what the target holds of it is"
                   " not what the source holds now")
            if word == "leave":
                for p in cut:
                    st["parts"].pop(p, None)
                st["resets"] = int(st.get("resets") or 0) + 1
                self.log(f"{topic}: {why}; the hop's ddl leaves that out,"
                         " and its log is copied from its start, after what"
                         " the target holds")
                return True
            if word in ("park", "apply"):
                st["state"], st["why"] = PARKED, why
                self.log(f"{topic}: parked - {why}")
                return True
            raise SystemExit(f"{topic}: {why}. Nothing more of it is copied;"
                             " set ddl to park to leave it aside")
        return moved

    def _regrown(self, topic, st, n):
        """A topic parked for its partitions: followed again once the
        target has as many as the source."""
        theirs = len(self.eng._ranges("dst", [topic]).get(topic, {})
                     .get("end") or {})
        if theirs < n:
            return False
        st["state"], st["why"] = LIVE, "the target has its partitions"
        st.pop("parked_for", None)
        st["partitions"] = n
        self.log(f"{topic}: the target has {theirs} partitions now; copied"
                 " again, the new ones from their first message")
        return True

    def _release(self, topic, st):
        """A parked topic looked at again: the hop's scope changed or the
        tail started, so what parked it is read afresh - a topic that left
        and came back is held to the target it left again, one that was
        followed to the log it saved, one made while the hop parked creates
        is a topic added."""
        st.pop("parked_for", None)
        if st.get("left"):
            st["state"], st["why"] = LEFT, "released"
        elif st.get("parts"):
            st["state"], st["why"] = LIVE, "released"
        else:
            self.state.pop(self.key(topic), None)
        return True
