import os
import socket
import tempfile
import threading
import time
from typing import Optional
import pytest

from migkit.config import Endpoint, Hop
from migkit import physical
from migkit.tunnel import (
    AutoBastionTunnelManager,
    BastionConfig,
    ForwardingHandle,
    WireGuardConfig,
    WireGuardMesh,
    parse_bastion_config,
    _free_port,
)


class DummyProcess:
    def __init__(self, pid=12345, returncode=None):
        self.pid = pid
        self._returncode = returncode

    def poll(self):
        return self._returncode

    def wait(self, timeout=None):
        return self._returncode or 0

    def terminate(self):
        self._returncode = -15

    def kill(self):
        self._returncode = -9


class LocalTcpEchoServer:
    def __init__(self, host="127.0.0.1", port=0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.host, self.port = self.sock.getsockname()
        self.sock.listen(16)
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.sock.settimeout(0.5)
                conn, _ = self.sock.accept()
            except (socket.timeout, OSError):
                continue
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            with conn:
                while not self._stop.is_set():
                    data = conn.recv(4096)
                    if not data:
                        break
                    conn.sendall(data)
        except OSError:
            pass

    def close(self):
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def test_parse_bastion_config_string():
    cfg1 = parse_bastion_config("bastion.example.com")
    assert cfg1.host == "bastion.example.com"
    assert cfg1.port == 22
    assert cfg1.user == ""

    cfg2 = parse_bastion_config("ec2-user@jump.internal:2222")
    assert cfg2.host == "jump.internal"
    assert cfg2.port == 2222
    assert cfg2.user == "ec2-user"

    cfg3 = parse_bastion_config("ssh://admin@gateway.corp:2200")
    assert cfg3.host == "gateway.corp"
    assert cfg3.port == 2200
    assert cfg3.user == "admin"


def test_parse_bastion_config_dict():
    raw = {
        "host": "bastion.prod.internal",
        "port": 2222,
        "user": "ubuntu",
        "identity_file": "~/.ssh/bastion_id",
        "jump": ["edge1.internal", "edge2.internal"],
        "multiplex": True,
        "keepalive_interval": 20,
        "keepalive_count_max": 5,
        "strict_host_key_checking": "no",
        "extra_options": {"Compression": "yes"},
    }
    cfg = parse_bastion_config(raw)
    assert cfg.host == "bastion.prod.internal"
    assert cfg.port == 2222
    assert cfg.user == "ubuntu"
    assert cfg.identity_file == "~/.ssh/bastion_id"
    assert cfg.jump_hosts == ["edge1.internal", "edge2.internal"]
    assert cfg.multiplex is True
    assert cfg.keepalive_interval == 20
    assert cfg.keepalive_count_max == 5
    assert cfg.strict_host_key_checking == "no"
    assert cfg.extra_options == {"Compression": "yes"}


def test_ephemeral_key_generation():
    mgr = AutoBastionTunnelManager(
        bastion_host="bastion.example.com",
        ephemeral_key=True,
        multiplex=False,
        reconnect=False,
    )
    try:
        assert mgr._ephemeral_key_file is not None
        assert os.path.exists(mgr._ephemeral_key_file)
        # Check permissions: 0o600 on private key
        mode = os.stat(mgr._ephemeral_key_file).st_mode & 0o777
        assert mode == 0o600
        assert mgr._ephemeral_pub_key is not None
        assert "ssh-ed25519" in mgr._ephemeral_pub_key or "migkit-ephemeral" in mgr._ephemeral_pub_key
    finally:
        key_file = mgr._ephemeral_key_file
        mgr.close()
        assert not os.path.exists(key_file)


def test_build_ssh_command_flags():
    mgr = AutoBastionTunnelManager(
        bastion_host="bastion.internal",
        bastion_port=2222,
        user="ops",
        identity_file="/var/keys/id_ed25519",
        jump_hosts=["jump1.internal", "jump2.internal"],
        multiplex=True,
        keepalive_interval=30,
        keepalive_count_max=4,
        strict_host_key_checking="yes",
        known_hosts="/var/keys/known_hosts",
        extra_options={"ConnectTimeout": "10"},
        reconnect=False,
    )
    try:
        handle = ForwardingHandle(
            tunnel_id="local_9999_10.0.0.1_5432",
            direction="local",
            local_host="127.0.0.1",
            local_port=9999,
            remote_host="10.0.0.1",
            remote_port=5432,
        )
        argv = mgr.build_ssh_command(handle)
        assert argv[0] == "ssh"
        assert "-N" in argv
        assert "-L" in argv
        assert "127.0.0.1:9999:10.0.0.1:5432" in argv
        assert "-p" in argv and "2222" in argv
        assert "-J" in argv and "jump1.internal,jump2.internal" in argv
        assert "-i" in argv and "/var/keys/id_ed25519" in argv
        assert "-o" in argv and "ServerAliveInterval=30" in argv
        assert "ServerAliveCountMax=4" in argv
        assert "StrictHostKeyChecking=yes" in argv
        assert "UserKnownHostsFile=/var/keys/known_hosts" in argv
        assert "ControlMaster=auto" in argv
        assert "ConnectTimeout=10" in argv
        assert argv[-1] == "ops@bastion.internal"

        # Remote forward
        handle_remote = ForwardingHandle(
            tunnel_id="remote_8080_127.0.0.1_8080",
            direction="remote",
            local_host="127.0.0.1",
            local_port=8080,
            remote_host="10.0.0.1",
            remote_port=8080,
        )
        argv_r = mgr.build_ssh_command(handle_remote)
        assert "-R" in argv_r
        assert "10.0.0.1:8080:127.0.0.1:8080" in argv_r
    finally:
        mgr.close()


def test_multiplex_control_path_safe_length():
    mgr = AutoBastionTunnelManager(
        bastion_host="very-long-subdomain-name-exceeding-standard-unix-domain-socket-limit-bastion.internal.zone.corp",
        multiplex=True,
        reconnect=False,
    )
    try:
        assert mgr.control_path is not None
        # Unix socket paths must be well under ~104 characters on macOS/BSD/Linux
        assert len(mgr.control_path) < 100
        assert mgr.control_path.endswith("c.sock")
    finally:
        mgr.close()


def test_local_forwarding_health_and_latency():
    server = LocalTcpEchoServer()
    try:
        def fake_launcher(handle, manager):
            return DummyProcess(pid=99999, returncode=None)

        mgr = AutoBastionTunnelManager(
            bastion_host="bastion.test",
            multiplex=False,
            reconnect=False,
            launcher=fake_launcher,
        )
        try:
            # Bind local forwarding to the echo server port
            port = mgr.forward_local("remote.db", 5432, local_port=server.port, wait_ready=1.0)
            assert port == server.port

            # Health check should pass
            assert mgr.check_health(timeout=1.0) is True

            # Latency measurement
            lat_ms = mgr.measure_latency(tries=3, timeout=1.0)
            assert lat_ms is not None
            assert 0.0 <= lat_ms < 500.0

            diag = mgr.diagnostics()
            assert diag["tunnels"][0]["local"] == f"127.0.0.1:{server.port}"
            assert diag["tunnels"][0]["last_health"] is True
            assert diag["tunnels"][0]["last_latency_ms"] is not None
        finally:
            mgr.close()
    finally:
        server.close()


def test_silent_reconnect_on_network_drop():
    server = LocalTcpEchoServer()
    try:
        proc_instances = [DummyProcess(pid=1001, returncode=1), DummyProcess(pid=1002, returncode=None)]
        idx = 0

        def drop_and_reconnect_launcher(handle, manager):
            nonlocal idx
            p = proc_instances[min(idx, len(proc_instances) - 1)]
            idx += 1
            return p

        mgr = AutoBastionTunnelManager(
            bastion_host="bastion.test",
            multiplex=False,
            reconnect=True,
            reconnect_delay=0.05,
            poll_interval=0.05,
            launcher=drop_and_reconnect_launcher,
        )
        try:
            port = mgr.forward_local("remote.db", 5432, local_port=server.port, wait_ready=0.5)
            # Give monitor loop a moment to detect drop and trigger reconnect
            time.sleep(0.3)
            with mgr._lock:
                handle = list(mgr._tunnels.values())[0]
                assert handle.drop_count >= 1
                assert handle.reconnect_count >= 1
                assert handle.proc.pid == 1002
        finally:
            mgr.close()
    finally:
        server.close()


def test_bind_and_unbind_hop():
    server = LocalTcpEchoServer()
    try:
        def fake_launcher(handle, manager):
            return DummyProcess(pid=8888, returncode=None)

        src_ep = Endpoint(host="prod-db-source.internal", port=5432, options={})
        dst_ep = Endpoint(host="prod-db-target.internal", port=5432, options={})
        hop = Hop(
            name="vpc-migration",
            engine="postgres",
            source=src_ep,
            target=dst_ep,
            options={
                "bastion": {
                    "host": "bastion.cloud.internal",
                    "port": 2222,
                    "user": "ec2-user",
                }
            },
        )

        mgr = AutoBastionTunnelManager(
            config=hop.options["bastion"],
            multiplex=False,
            reconnect=False,
            launcher=fake_launcher,
        )
        try:
            mgr.bind_hop(hop, wait_ready=0.0)
            assert hop.source.host == "127.0.0.1"
            assert hop.source.port > 0
            assert hop.source.options["tunnel_to"] == "prod-db-source.internal:5432"
            assert hop.source.options["tunnel_by"] == "auto-bastion"
            assert hop.source.options["_original_host"] == "prod-db-source.internal"
            assert hop.source.options["_original_port"] == 5432

            assert hop.target.host == "127.0.0.1"
            assert hop.target.port > 0
            assert hop.target.options["tunnel_to"] == "prod-db-target.internal:5432"
            assert hop.target.options["tunnel_by"] == "auto-bastion"
            assert getattr(hop, "tunnel_manager") is mgr

            mgr.unbind_hop(hop)
            assert hop.source.host == "prod-db-source.internal"
            assert hop.source.port == 5432
            assert hop.target.host == "prod-db-target.internal"
            assert hop.target.port == 5432
            assert getattr(hop, "tunnel_manager", None) is None
        finally:
            mgr.close()
    finally:
        server.close()


def test_from_hop_class_method():
    def fake_launcher(handle, manager):
        return DummyProcess(pid=7777, returncode=None)

    src_ep = Endpoint(host="db1.internal", port=3306, options={})
    dst_ep = Endpoint(host="db2.internal", port=3306, options={})
    hop = Hop(
        name="hop-mysql",
        engine="mysql",
        source=src_ep,
        target=dst_ep,
        options={"bastion": "admin@bastion.corp:2200"},
    )

    mgr = AutoBastionTunnelManager.from_hop(
        hop,
        multiplex=False,
        reconnect=False,
        launcher=fake_launcher,
    )
    try:
        assert mgr.config.host == "bastion.corp"
        assert mgr.config.user == "admin"
        assert mgr.config.port == 2200
        assert hop.source.host == "127.0.0.1"
        assert hop.target.host == "127.0.0.1"
    finally:
        mgr.close()


def test_wireguard_mesh_orchestration():
    priv, pub = WireGuardMesh.generate_keypair()
    assert isinstance(priv, str) and len(priv) >= 40
    assert isinstance(pub, str) and len(pub) >= 40

    peer_priv, peer_pub = WireGuardMesh.generate_keypair()
    cfg = WireGuardConfig(
        private_key=priv,
        public_key=pub,
        address="10.0.0.1/24",
        listen_port=51820,
        peers=[{
            "public_key": peer_pub,
            "endpoint": "198.51.100.10:51820",
            "allowed_ips": ["10.0.0.2/32", "192.168.1.0/24"],
            "persistent_keepalive": 25,
        }],
    )
    rendered = WireGuardMesh.render_config(cfg)
    assert "[Interface]" in rendered
    assert f"PrivateKey = {priv}" in rendered
    assert "Address = 10.0.0.1/24" in rendered
    assert "ListenPort = 51820" in rendered
    assert "[Peer]" in rendered
    assert f"PublicKey = {peer_pub}" in rendered
    assert "Endpoint = 198.51.100.10:51820" in rendered
    assert "AllowedIPs = 10.0.0.2/32,192.168.1.0/24" in rendered
    assert "PersistentKeepalive = 25" in rendered


def test_context_manager_clean_teardown():
    held_path = None
    with AutoBastionTunnelManager(
        bastion_host="bastion.internal",
        ephemeral_key=True,
        multiplex=True,
        reconnect=False,
    ) as mgr:
        assert mgr.control_path is not None
        assert mgr._ephemeral_key_file is not None
        held_path = mgr._ephemeral_key_file
        assert os.path.exists(held_path)

    # After exit, ephemeral files and manager should be closed
    assert not os.path.exists(held_path)
    assert mgr._closed is True


def test_physical_latency_reads_from_tunnel_manager():
    src_ep = Endpoint(host="127.0.0.1", port=5432, options={})
    dst_ep = Endpoint(host="127.0.0.1", port=5432, options={})
    hop = Hop(name="h", engine="postgres", source=src_ep, target=dst_ep)

    class MockTunnelManager:
        def measure_latency(self):
            # 1.0 ms -> 0.001 s (< NEAR 0.002 s)
            return 1.0

    hop.tunnel_manager = MockTunnelManager()
    rung, reason = physical.compression(hop, version=150000)
    assert rung is physical.PLAIN

    class FarTunnelManager:
        def measure_latency(self):
            # 10.0 ms -> 0.010 s (> NEAR 0.002 s)
            return 10.0

    hop.tunnel_manager = FarTunnelManager()
    rung_far, reason_far = physical.compression(hop, version=150000)
    assert rung_far is physical.ZSTD
