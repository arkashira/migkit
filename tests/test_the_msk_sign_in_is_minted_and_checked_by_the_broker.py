"""Amazon MSK's IAM sign-in: the broker's advertised mechanisms read off
the wire, the sign-in rung chosen from them, and the token - a request to
`kafka-cluster:Connect` presigned with the provider's signature version 4
- minted through the egress gate, renewed before it lapses and checked by
a fake broker that recomputes its signature and expiry. No sandbox runs
MSK (`variants.UNPROVED_ON`): the broker is a fake."""
import base64
import datetime
import hashlib
import hmac
import logging
import socket
import struct
import threading
from urllib.parse import parse_qsl, quote, urlsplit

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop
from migkit.engines import kafka


ACCESS, SECRET = "AKIDEXAMPLE", "CANARY-secret-never-in-a-log-0123456789"


def broker_checks(token, now, secret=SECRET):
    """What the fake broker checks of a token: its shape, the action, the
    service in its scope, its signature recomputed, and its expiry."""
    url = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    assert parts.scheme == "https" and parts.path == "/"
    assert q["Action"] == "kafka-cluster:Connect"
    assert q["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
    access, day, region, service, term = q["X-Amz-Credential"].split("/")
    assert (service, term) == ("kafka-cluster", "aws4_request")
    assert parts.hostname == f"kafka.{region}.amazonaws.com"
    signed = {k: v for k, v in q.items()
              if k not in ("X-Amz-Signature", "User-Agent")}
    canon_q = "&".join(f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
                       for k, v in sorted(signed.items()))
    canon = (f"GET\n/\n{canon_q}\nhost:{parts.hostname}\n\nhost\n"
             "UNSIGNED-PAYLOAD")
    scope = f"{day}/{region}/kafka-cluster/aws4_request"
    to_sign = (f"AWS4-HMAC-SHA256\n{q['X-Amz-Date']}\n{scope}\n"
               + hashlib.sha256(canon.encode()).hexdigest())
    key = ("AWS4" + secret).encode()
    for part in (day, region, "kafka-cluster", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    want = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, q["X-Amz-Signature"]):
        return "signature"
    at = datetime.datetime.strptime(q["X-Amz-Date"], "%Y%m%dT%H%M%SZ"
                                    ).replace(tzinfo=datetime.timezone.utc)
    if now > at.timestamp() + int(q["X-Amz-Expires"]):
        return "expired"
    return ""


class FakeBroker:
    """A listener answering the SASL handshake with its mechanisms (or
    with the illegal-state error where `mechanisms` is None)."""

    def __init__(self, mechanisms):
        self.mechanisms = mechanisms
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        conn, _ = self.sock.accept()
        size = struct.unpack(">i", conn.recv(4))[0]
        req = conn.recv(size)
        key, version, cid = struct.unpack(">hhi", req[:8])
        assert (key, version) == (17, 1)
        mechs = self.mechanisms or []
        body = struct.pack(">ihi", cid, 34 if self.mechanisms is None else 0,
                           len(mechs))
        for m in mechs:
            body += struct.pack(">h", len(m)) + m.encode()
        conn.sendall(struct.pack(">i", len(body)) + body)
        conn.close()


def _hop():
    ep = Endpoint(host="b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com",
                  port=9098, user="", password="",
                  options={"security_protocol": "SASL_SSL",
                           "sasl_mechanism": "AWS_MSK_IAM"})
    return Hop(name="m", engine="kafka", source=ep, target=ep,
               db_map={"0": "0"})


def test_the_advertised_mechanisms_choose_the_sign_in():
    got = kafka.sasl_mechanisms("127.0.0.1",
                                FakeBroker(["AWS_MSK_IAM",
                                            "SCRAM-SHA-512"]).port)
    assert got == ["AWS_MSK_IAM", "SCRAM-SHA-512"]
    way = decide.kafka_sign_in({"advertised": got, "credentials": True,
                                "account": True})
    assert way.rung.name == "AWS_MSK_IAM"
    # no provider credentials here: the account's salted challenge
    way = decide.kafka_sign_in({"advertised": got, "credentials": False,
                                "account": True})
    assert way.rung.name == "SCRAM-SHA-512"
    # a listener without SASL answers the illegal-state error: none
    assert kafka.sasl_mechanisms("127.0.0.1", FakeBroker(None).port) == []
    assert decide.kafka_sign_in({"advertised": [], "account": True,
                                 "credentials": True}).rung is None
    # nothing answered: unread, and the side's own ask is kept
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    port = dead.getsockname()[1]
    dead.close()
    assert kafka.sasl_mechanisms("127.0.0.1", port, timeout=1) is None
    assert decide.kafka_sign_in({"advertised": None, "asked": "SCRAM-SHA-256",
                                 "account": True}).rung.name == "SCRAM-SHA-256"


def test_the_broker_verifies_the_token_and_its_expiry(monkeypatch, caplog):
    monkeypatch.setenv("MIGKIT_EGRESS", "provider")
    clock = [1_800_000_000.0]
    tok = kafka.MskToken(_hop(), "us-east-1",
                         credentials=lambda: (ACCESS, SECRET, "sess-1"),
                         clock=lambda: clock[0])
    with caplog.at_level(logging.DEBUG):
        value, expires = tok()
    assert expires == clock[0] + kafka.MSK_TOKEN_LIFETIME
    assert broker_checks(value, clock[0]) == ""
    assert broker_checks(value, clock[0], secret="another") == "signature"
    assert broker_checks(value, expires + 1) == "expired"
    # held while it holds, minted again before it lapses
    assert tok.token() == value
    clock[0] = expires - 1
    again = tok.token()
    assert again != value and broker_checks(again, clock[0]) == ""
    # the canary: neither the secret nor the token in a log or the repr
    for text in (caplog.text, repr(tok)):
        assert SECRET not in text and value not in text


def test_no_egress_refuses_the_mint(monkeypatch, caplog):
    monkeypatch.setenv("MIGKIT_EGRESS", "none")
    tok = kafka.MskToken(_hop(), "us-east-1",
                         credentials=lambda: (ACCESS, SECRET, None))
    with caplog.at_level(logging.DEBUG), pytest.raises(SystemExit) as e:
        tok.token()
    assert "not minted" in str(e.value) and tok._value is None
    assert SECRET not in caplog.text and SECRET not in str(e.value)


def test_the_side_signs_in_with_the_minted_token():
    eng = kafka.KafkaEngine(_hop())
    conn = eng._connection("src")
    assert conn["sasl_mechanism"] == "OAUTHBEARER"
    assert isinstance(conn["sasl_oauth_token_provider"], kafka.MskToken)
    assert conn["sasl_oauth_token_provider"].region == "us-east-1"
