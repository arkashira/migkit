"""`exclude` on Kafka: an excluded topic is not verified.

Kafka read no exclude list, so a topic only the target's own producers
write to was reported as extra, and one the operator had excluded was
still compared. Every check here lists topics through `_topics()`, the
one place the list is narrowed; a live broker's listing is the input.
"""
from migkit.config import Endpoint, Hop


class _Consumer:
    def __init__(self, names):
        self.names = set(names)

    def topics(self):
        return self.names


def _engine(exclude=()):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="10.0.0.1", port=9092, user="",
                              password=""),
              target=Endpoint(host="10.0.0.2", port=9092, user="",
                              password=""),
              exclude=list(exclude))
    return KafkaEngine(hop)


LISTED = ["orders", "payments", "target.audit", "__consumer_offsets"]


def test_an_excluded_topic_is_not_listed():
    got = _engine(["target.*", "cluster.payments"])._topics(_Consumer(LISTED))
    assert got == ["orders"], got


def test_without_an_exclude_list_only_internal_topics_are_left_out():
    got = _engine()._topics(_Consumer(LISTED))
    assert got == ["orders", "payments", "target.audit"], got
