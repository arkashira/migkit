"""Google Cloud Pub/Sub as the destination of a change stream (backlog 33).

A hop `engine: hetero` with `target_engine: pubsub` carries the source's
changes into topics, as messages in the shapes and under the hop options
`streamout` describes, the same as Kafka and Kinesis. The target
endpoint's `project` option names the project and `credentials_file` a
service account's key, where the machine's own sign-in is not the one to
use; `host` and `port` name an emulator.

Every message carries its row's key as its ordering key, and the
publisher keeps order by it, so a subscription that asks for ordering
receives a key's changes in the order the source made them. A publish
that fails stops the delivery and says so; started again, the tail goes
on from the position it last saved.

A topic keeps a message only for the subscriptions it has when the
message is published: one with none drops what it is given, and the
publish still succeeds. So delivery into a topic that has no subscription
stops and names it, as does delivery into a topic that is not there -
migkit makes neither, since what reads them is not migkit's to decide.
"""
import time

from .base import Engine

#: bytes in one message, as the service allows
MOST_BYTES = 10 * 2 ** 20
#: bytes in an ordering key
KEY_BYTES = 1024


class PubSubEngine(Engine):
    checks = ()
    CANON_ENGINE = "pubsub"
    #: a topic is named by the hop's template, not paired with what exists
    CREATES_ON_WRITE = True
    EXPRESSES_ABSENT = True

    def _project(self):
        project = (self.hop.target.options or {}).get("project")
        if not project:
            raise SystemExit("the target names no project: say project: in"
                             " its options")
        return project

    def _publisher(self):
        """One publisher for the engine's life, keeping order by key."""
        got = self.__dict__.get("_client")
        if got is not None:
            return got
        from google.cloud import pubsub_v1
        ep = self.hop.target
        kw = {"publisher_options": pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True)}
        if ep.host:
            # an emulator speaks without TLS and without a sign-in
            import grpc
            from google.pubsub_v1.services.publisher.transports.grpc import \
                PublisherGrpcTransport
            kw["transport"] = PublisherGrpcTransport(
                channel=grpc.insecure_channel(f"{ep.host}:{ep.port}"))
        elif (ep.options or {}).get("credentials_file"):
            from google.oauth2 import service_account
            kw["credentials"] = (service_account.Credentials
                                 .from_service_account_file(
                                     ep.options["credentials_file"]))
        self.__dict__["_client"] = client = pubsub_v1.PublisherClient(**kw)
        return client

    def databases(self):
        return list(self.hop.databases)

    def target_missing(self, db):
        return True

    def _ensure(self, client, names):
        """Every topic in `names` there, with a subscription to keep what
        is published to it."""
        from google.api_core import exceptions
        ready = self.__dict__.setdefault("_ready", set())
        for name in sorted(set(names) - ready):
            path = client.topic_path(self._project(), name)
            try:
                # asked first: the list of a topic's subscriptions came back
                # empty for a topic that is not there (the emulator)
                client.get_topic(request={"topic": path})
            except exceptions.NotFound:
                raise SystemExit(
                    f"no topic {name} in project {self._project()}: create"
                    " it, and the subscription that will read it")
            subs = list(client.list_topic_subscriptions(
                request={"topic": path}))
            if not subs:
                raise SystemExit(
                    f"topic {name} has no subscription, and Pub/Sub keeps a"
                    " message only for the subscriptions a topic has when"
                    " it is published - these changes would be dropped."
                    " Create the subscription that will read them first")
            ready.add(name)

    def neutral_apply(self, side, db, changes):
        """Every change as a message, in the order the source made them."""
        import hashlib

        from .. import streamout
        self._target_only(side, "write a change stream")
        sent, skipped = streamout.encoded(
            self.hop, db, changes, int(time.time() * 1000),
            limit=min(streamout.options(self.hop)[3], MOST_BYTES))
        client = self._publisher()
        self._ensure(client, {topic for topic, _, _ in sent})
        waiting = []
        for topic, key, value in sent:
            # a longer key goes by its digest, which keeps one key in one
            # order all the same
            if len(key.encode()) > KEY_BYTES:
                key = hashlib.sha256(key.encode()).hexdigest()
            waiting.append((topic, key, client.publish(
                client.topic_path(self._project(), topic), value,
                ordering_key=key)))
        for topic, key, future in waiting:
            try:
                future.result(timeout=120)
            except Exception as e:
                # the client holds back the key's later messages once one
                # fails; they go again with the tail's next start
                client.resume_publish(client.topic_path(self._project(),
                                                        topic), key)
                raise SystemExit(f"Pub/Sub did not take a change for topic"
                                 f" {topic}: {str(e)[:160]}. Started again,"
                                 " the tail goes on from the position it"
                                 " last saved")
        streamout.count_skipped(self.hop, db, skipped)
        return len(changes)

    def _apply_upsert(self, side, db, table, key, values):
        self.neutral_apply(side, db, [{"op": "update", "table": table,
                                       "key": key, "values": values}])

    def _apply_delete(self, side, db, table, key):
        self.neutral_apply(side, db, [{"op": "delete", "table": table,
                                       "key": key}])
