import os, sys, time, json, threading
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["BFT_DOCKER"] = ""

from config import (
    MsgType,
    MSG_PRE_PREPARE, MSG_PREPARE, MSG_COMMIT, MSG_CLIENT_REQUEST, MSG_CLIENT_REPLY,
    VOTE_COMMIT, VOTE_ABORT, NUM_SITES, QUORUM,
)
from crypto_utils import hash_message, sign_message_real, verify_signature_real

# Mock network functions trong module pbft de tranh loi TCP/key
from consensus import pbft as _pbft_mod
_orig_broadcast = _pbft_mod.net_broadcast
_orig_send = _pbft_mod.net_send
def _noop(*a, **kw): pass
_pbft_mod.net_broadcast = _noop
_pbft_mod.net_send = _noop


class MockStore:
    def __init__(self):
        self.db = {}
    def put_pbft_log(self, *a, **kw): pass
    def put_wal(self, *a, **kw): pass
    def load_checkpoint(self): return (0, {})
    def close(self): pass
    def get_balance(self, *a): return 100
    def put_state(self, *a, **kw): pass
    def transfer(self, s, d, amt): return True
    def put_ledger(self, *a, **kw): pass
    def delete_old_pbft_logs(self, *a): pass
    def update_watermarks(self, *a): pass


class MockLogger:
    def __init__(self):
        self.logs = []
    def info(self, tag, msg): self.logs.append(("INFO", tag, msg))
    def warning(self, tag, msg): self.logs.append(("WARN", tag, msg))
    def error(self, tag, msg): self.logs.append(("ERR", tag, msg))


class MockSequencer:
    def __init__(self):
        self.seq = 0
        self.low_water_mark = 0
        self.high_water_mark = 100
    def get_next_seq(self):
        self.seq += 1
        return self.seq


class MockViewChange:
    def __init__(self):
        self.timeout_forced = False
    def check_view_timeout(self, force=False):
        self.timeout_forced = force


class MockIncomingQueue(list):
    pass


class TestPBFTDigestChecks:
    """Digest verification trong PREPARE va COMMIT."""

    def setup_method(self):
        from consensus.pbft import PBFTConsensus
        self.store = MockStore()
        self.log = MockLogger()
        self.seq_mgr = MockSequencer()
        self.pbft = PBFTConsensus(99, MockIncomingQueue(), self.store, self.log, threading.Event())
        self.pbft.sequence_manager = self.seq_mgr
        self.pbft.requests = {}
        self.pbft.view = 0
        self.pbft.view_change = MockViewChange()

    def test_prepare_accepts_matching_digest(self):
        seq, digest = 1, "abc123"
        self.pbft.requests[seq] = {"digest": digest, "prepares": {}}
        msg = {"type": MSG_PREPARE, "sender": 1, "tx_id": 1, "seq": seq, "view": 0, "digest": digest}
        self.pbft._handle_prepare(msg)
        assert 1 in self.pbft.requests[seq]["prepares"]

    def test_prepare_rejects_mismatched_digest(self):
        seq = 1
        self.pbft.requests[seq] = {"digest": "correct", "prepares": {}}
        msg = {"type": MSG_PREPARE, "sender": 1, "tx_id": 1, "seq": seq, "view": 0, "digest": "wrong"}
        self.pbft._handle_prepare(msg)
        assert 1 not in self.pbft.requests[seq]["prepares"]
        assert any("DIGEST_MISMATCH" in str(l) for l in self.log.logs)

    def test_commit_accepts_matching_digest(self):
        seq, digest = 1, "abc123"
        self.pbft.requests[seq] = {"digest": digest, "commits": set(), "prepares": {}}
        msg = {"type": MSG_COMMIT, "sender": 1, "tx_id": 1, "seq": seq, "view": 0, "digest": digest}
        self.pbft._handle_commit(msg)
        assert 1 in self.pbft.requests[seq]["commits"]

    def test_commit_rejects_mismatched_digest(self):
        seq = 1
        self.pbft.requests[seq] = {"digest": "correct", "commits": set(), "prepares": {}}
        msg = {"type": MSG_COMMIT, "sender": 1, "tx_id": 1, "seq": seq, "view": 0, "digest": "wrong"}
        self.pbft._handle_commit(msg)
        assert 1 not in self.pbft.requests[seq]["commits"]
        assert any("DIGEST_MISMATCH" in str(l) for l in self.log.logs)

    def test_pre_prepare_verification(self):
        tx = {"tx_id": 1, "data": "A chuyen 10 cho B", "client_id": "test"}
        d = hash_message(tx)
        msg = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": 1, "view": 0,
               "digest": d, "request": tx}
        self.pbft._handle_pre_prepare(msg)
        assert 1 in self.pbft.requests

    def test_pre_prepare_rejects_bad_digest(self):
        tx = {"tx_id": 1, "data": "A chuyen 10 cho B", "client_id": "test"}
        msg = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": 1, "view": 0,
               "digest": "badbadbad", "request": tx}
        self.pbft._handle_pre_prepare(msg)
        assert 1 not in self.pbft.requests
        assert any("PBFT_ERROR" in str(l) for l in self.log.logs)


class TestEquivocation:
    def setup_method(self):
        from consensus.pbft import PBFTConsensus
        self.store = MockStore()
        self.log = MockLogger()
        self.seq_mgr = MockSequencer()
        self.pbft = PBFTConsensus(98, MockIncomingQueue(), self.store, self.log, threading.Event())
        self.pbft.sequence_manager = self.seq_mgr
        self.pbft.requests = {}
        self.pbft.view = 0
        self.vc = MockViewChange()
        self.pbft.view_change = self.vc

    def test_equivocation_rejected(self):
        seq = 1
        tx1 = {"tx_id": 1, "data": "A chuyen 10 cho B", "client_id": "c1"}
        tx2 = {"tx_id": 1, "data": "A chuyen 99 cho B", "client_id": "c2"}
        d1, d2 = hash_message(tx1), hash_message(tx2)
        assert d1 != d2

        msg1 = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": seq, "view": 0,
                "digest": d1, "request": tx1}
        self.pbft._handle_pre_prepare(msg1)
        assert seq in self.pbft.requests
        assert self.pbft.requests[seq]["digest"] == d1

        msg2 = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": seq, "view": 0,
                "digest": d2, "request": tx2}
        self.pbft._handle_pre_prepare(msg2)

        assert self.pbft.requests[seq]["digest"] == d1
        assert self.vc.timeout_forced, "Equivocation phai trigger view change"


class TestWatermark:
    def setup_method(self):
        from consensus.pbft import PBFTConsensus
        self.store = MockStore()
        self.log = MockLogger()
        self.seq_mgr = MockSequencer()
        self.pbft = PBFTConsensus(97, MockIncomingQueue(), self.store, self.log, threading.Event())
        self.pbft.sequence_manager = self.seq_mgr
        self.pbft.requests = {}
        self.pbft.view = 0

    def test_below_low_watermark(self):
        tx = {"tx_id": 1, "data": "test", "client_id": "c"}
        d = hash_message(tx)
        msg = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": -1, "view": 0,
               "digest": d, "request": tx}
        self.pbft._handle_pre_prepare(msg)
        assert -1 not in self.pbft.requests

    def test_above_high_watermark(self):
        tx = {"tx_id": 1, "data": "test", "client_id": "c"}
        d = hash_message(tx)
        msg = {"type": MSG_PRE_PREPARE, "sender": 0, "tx_id": 1, "seq": 999, "view": 0,
               "digest": d, "request": tx}
        self.pbft._handle_pre_prepare(msg)
        assert 999 not in self.pbft.requests


class TestClientIdempotency:
    def setup_method(self):
        from consensus.pbft import PBFTConsensus
        from crypto_utils import generate_deterministic_keypair
        self.store = MockStore()
        self.log = MockLogger()
        self.seq_mgr = MockSequencer()
        self.pbft = PBFTConsensus(96, MockIncomingQueue(), self.store, self.log, threading.Event())
        self.pbft.sequence_manager = self.seq_mgr
        self.pbft.requests = {}
        self.pbft.view = 0
        self.pbft.client_replies = {}
        self.client_sk, self.client_pk = generate_deterministic_keypair(b"0123456789abcdef0123456789abcdef")
        self.client_pk_hex = self.client_pk.hex()

    def _client_msg(self, client_id, ts, op):
        msg = {"type": int(MSG_CLIENT_REQUEST), "client_id": client_id,
               "timestamp": ts, "operation": op, "client_pubkey": self.client_pk_hex}
        sign_message_real(self.client_sk, msg)
        msg["_client_conn"] = None
        return msg

    def test_idempotent_reply(self):
        client_id = "test_client"
        self.pbft.client_replies[client_id] = (123.0, "cached_reply")
        msg = self._client_msg(client_id, 122.0, "A chuyen 10 cho B")
        self.pbft._handle_client_request(msg)
        assert self.pbft.client_replies[client_id] == (123.0, "cached_reply")

    def test_newer_timestamp_passes_cache(self):
        """Backup node khong cache reply, nhung bo qua cache cho newer ts."""
        client_id = "test_client"
        self.pbft.client_replies[client_id] = (100.0, "old_reply")
        msg = self._client_msg(client_id, 200.0, "B chuyen 3 cho C")
        self.pbft._handle_client_request(msg)
        # Khong bi cache return, tiep tuc den redirect hoac submit
        assert any("CLIENT_REQUEST" in str(l) for l in self.log.logs)


if __name__ == "__main__":
    tests = [
        TestPBFTDigestChecks(),
        TestEquivocation(),
        TestWatermark(),
        TestClientIdempotency(),
    ]

    passed = 0
    failed = 0
    for t in tests:
        cls_name = type(t).__name__
        for m_name in sorted(dir(t)):
            if not m_name.startswith("test_"):
                continue
            m = getattr(t, m_name)
            print(f"  {cls_name}.{m_name}...", end=" ")
            try:
                t.setup_method()
                m()
                print("PASS")
                passed += 1
            except Exception as e:
                print(f"FAIL ({e})")
                failed += 1

    print(f"\n{'='*40}")
    print(f"  {passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
