"""
[PHASE 1] PBFT — Practical Byzantine Fault Tolerance Protocol.

Day la giao thuc dong thuan PBFT day du gom 3 pha:
  1. PRE-PREPARE: Leader (primary) de xuat giao dich + sequence number + digest
  2. PREPARE: Cac node broadcast xac nhan de xuat
  3. COMMIT: Khi co 2f+1 PREPARE, broadcast COMMIT, thuc thi khi du 2f+1 COMMIT

Tich hop them Heartbeat, Checkpoint, va Khoi phuc sau Crash tu RocksDB.
"""

import os
import sys
import time
import random
import threading
import json
import logging

from config import (
    config, NUM_SITES, QUORUM, F,
    TIMEOUT, NET_MIN, NET_MAX, LISTEN_AFTER,
    VOTE_COMMIT, VOTE_ABORT,
    MSG_VOTE, MSG_REQ, MSG_RESP, MSG_DEC,
    MSG_PING, MSG_PONG, MSG_CHECKPOINT,
    MSG_CLIENT_REQUEST, MSG_CLIENT_REPLY,
)
from network import sign_message, verify_signature, net_send, net_broadcast
from crypto_utils import compute_digest, hash_message


# PBFT message types (imported from config)
from config import MsgType
MSG_PRE_PREPARE = MsgType.PRE_PREPARE  # 101
MSG_PREPARE = MsgType.PREPARE          # 102
MSG_COMMIT = MsgType.COMMIT            # 103
MSG_CHECKPOINT_TYPE = MsgType.CHECKPOINT # 104
MSG_VIEW_CHANGE = MsgType.VIEW_CHANGE  # 105
MSG_NEW_VIEW = MsgType.NEW_VIEW        # 106
MSG_PING_TYPE = MsgType.PING
MSG_PONG_TYPE = MsgType.PONG
MSG_CLIENT_REQUEST_TYPE = MsgType.CLIENT_REQUEST
MSG_CLIENT_REPLY_TYPE = MsgType.CLIENT_REPLY

logger = logging.getLogger(__name__)


# ============================================================
# HELPERS
# ============================================================


def get_leader(view: int) -> int:
    """Xac dinh leader cho mot view."""
    return view % NUM_SITES


def is_leader(sid: int, view: int) -> bool:
    """Kiem tra node co phai leader cua view hien tai khong."""
    return sid == get_leader(view)


# ============================================================
# PBFT SEQUENCE NUMBER MANAGEMENT
# ============================================================


class SequenceManager:
    """Quan ly sequence number cho PBFT."""

    def __init__(self):
        self.last_seq = 0
        self.low_water_mark = 0
        self.high_water_mark = 100
        self.lock = threading.RLock()

    def get_next_seq(self) -> int:
        with self.lock:
            self.last_seq += 1
            return self.last_seq

    def get_current_seq(self) -> int:
        with self.lock:
            return self.last_seq


# ============================================================
# PBFT CONSENSUS ENGINE
# ============================================================


class PBFTConsensus:
    """
    PBFT consensus engine cho mot node.
    """

    def __init__(self, sid: int, incoming_queue, store, log, shutdown_event, is_malicious: bool = False):
        self.sid = sid
        self.incoming_queue = incoming_queue
        self.store = store
        self.log = log
        self.shutdown = shutdown_event
        self.is_malicious = is_malicious
        self.view_change = None  # Se duoc set tu node.py

        # PBFT state
        self.view = 0
        self.sequence_manager = SequenceManager()
        self.last_executed_seq = 0

        # Per-request state: seq -> PBFT state dict
        self.requests = {}

        # Checkpoint tracking
        self.checkpoints = {}  # seq -> digest -> set of senders

        # View change tracking
        self.view_change_sent = False
        self.view_changes_received = set()
        self.view_change_timeout = TIMEOUT * 2
        self.last_request_time = time.time()

        # Heartbeat tracking
        self.last_seen = {i: time.time() for i in range(NUM_SITES)}

        # Lock cho thread safety
        self.lock = threading.RLock()

    def start(self):
        """Khoi dong PBFT engine: nap checkpoint va replay WAL."""
        accounts = ["A", "B", "C", "D"]
        
        # Khoi phuc balances tu checkpoint gan nhat
        checkpoint_seq, balances = self.store.load_checkpoint()
        if checkpoint_seq > 0:
            self.log.info("RECOVERY", f"Site {self.sid}: Nap checkpoint tai seq {checkpoint_seq}, balances: {balances}")
            for acc, bal in balances.items():
                self.store.put_state(acc, bal)
            self.last_executed_seq = checkpoint_seq
            self.sequence_manager.last_seq = checkpoint_seq
        else:
            # Nap ban dau neu chua co state
            for acc in accounts:
                if self.store.get_balance(acc) == 100:
                    self.store.put_state(acc, 100)

        # Replay WAL de thuc thi cac giao dich da commit sau checkpoint
        self.store.replay_wal_from(checkpoint_seq)
        
        # Cap nhat last_executed_seq tuong ung voi Ledger tren dia
        ledger = self.store.get_ledger()
        if ledger:
            max_tx_id = max(tx["tx_id"] for tx in ledger)
            self.last_executed_seq = max(self.last_executed_seq, max_tx_id)
            self.sequence_manager.last_seq = max(self.sequence_manager.last_seq, max_tx_id)

        self.log.info(
            "PBFT_START",
            "PBFT engine da khoi dong (N=%d, f=%d, quorum=%d, last_executed_seq=%d)"
            % (NUM_SITES, F, QUORUM, self.last_executed_seq)
        )
        
        leader = get_leader(self.view)
        role = "LEADER" if leader == self.sid else "BACKUP"
        self.log.info("PBFT_VIEW", "View %d, Leader=Node %d, Toi la %s" % (self.view, leader, role))

        # Khoi dong thread heartbeat
        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name=f"Heartbeat-{self.sid}")
        hb_thread.start()

    def _heartbeat_loop(self):
        """Gui PING dinh ky va kiem tra song/chet cua leader."""
        while not self.shutdown.is_set():
            time.sleep(1.5)
            # Broadcast PING den tat ca cac node
            net_broadcast(None, self.sid, MSG_PING, tx_id=0)

            # Kiem tra leader hien tai
            leader = get_leader(self.view)
            if leader != self.sid:
                now = time.time()
                time_since_leader_seen = now - self.last_seen.get(leader, now)
                if time_since_leader_seen > 4.5:
                    self.log.warning(
                        "HEARTBEAT_TIMEOUT",
                        "Leader Node %d khong phan hoi trong %.2f giay (co the da chet)"
                        % (leader, time_since_leader_seen),
                    )
                    # Giam last_request_time de view-change manager kich hoat som
                    self.last_request_time = now - (TIMEOUT * 3)

    def _handle_ping(self, msg: dict):
        """Tra loi tin nhan PING bang PONG."""
        sender = msg.get("sender")
        net_send(None, self.sid, sender, MSG_PONG, tx_id=0)

    def _handle_pong(self, msg: dict):
        """Cap nhat thoi gian phan hoi PONG cua mot node."""
        sender = msg.get("sender")
        self.last_seen[sender] = time.time()

    # ============================================================
    # HANDLE INCOMING MESSAGES
    # ============================================================

    def handle_message(self, msg: dict):
        """
        Tiep nhan va xu ly mot message.
        """
        msg_type = msg.get("type")

        # Xac thuc chu ky: Neu khong phai CLIENT_REQUEST thi kiem tra chu ky cua node
        if msg_type != MSG_CLIENT_REQUEST:
            if not verify_signature(msg):
                self.log.info("PBFT_SECURITY", "Chu ky KHONG HOP LE tu Node %d" % msg.get("sender", -1))
                return

        if msg_type == MSG_PRE_PREPARE:
            self._handle_pre_prepare(msg)
        elif msg_type == MSG_PREPARE:
            self._handle_prepare(msg)
        elif msg_type == MSG_COMMIT:
            self._handle_commit(msg)
        elif msg_type == MSG_CHECKPOINT:
            self._handle_checkpoint(msg)
        elif msg_type == MSG_VIEW_CHANGE:
            self._handle_view_change(msg)
        elif msg_type == MSG_NEW_VIEW:
            self._handle_new_view(msg)
        elif msg_type == MSG_REQ:
            self._handle_recovery_request(msg)
        elif msg_type == MSG_RESP:
            self._handle_recovery_response(msg)
        elif msg_type == MSG_PING:
            self._handle_ping(msg)
        elif msg_type == MSG_PONG:
            self._handle_pong(msg)
        elif msg_type == MSG_CLIENT_REQUEST:
            self._handle_client_request(msg)
        else:
            # Unknown message type
            pass

    # ============================================================
    # VIEW CHANGE DISPATCH
    # ============================================================

    def _handle_view_change(self, msg: dict):
        if self.view_change:
            self.view_change.handle_view_change(msg)

    def _handle_new_view(self, msg: dict):
        if self.view_change:
            self.view_change.handle_new_view(msg)

    # ============================================================
    # REQUEST FLOW (from client)
    # ============================================================

    def submit_request(self, tx: dict):
        """
        Gui mot de xuat giao dich vao he thong PBFT.
        """
        tx_id = tx["tx_id"]
        
        if is_leader(self.sid, self.view):
            seq = self.sequence_manager.get_next_seq()
            self._send_pre_prepare(tx, seq)
        else:
            self.log.info(
                "PBFT_WAIT",
                "tx_id=%d | Backup, cho PRE-PREPARE tu Leader Node %d"
                % (tx_id, get_leader(self.view)),
            )

    # ============================================================
    # PHASE 1: PRE-PREPARE
    # ============================================================

    def _send_pre_prepare(self, tx: dict, seq: int):
        """Leader broadcast PRE-PREPARE message."""
        tx_id = tx["tx_id"]
        tx_digest = hash_message(tx)
        self.log.info(
            "PBFT_PRE_PREPARE",
            "tx_id=%d | Leader: gui PRE-PREPARE (seq=%d, view=%d, digest=%s)" % (tx_id, seq, self.view, tx_digest),
        )

        msg = {
            "type": MSG_PRE_PREPARE,
            "sender": self.sid,
            "tx_id": tx_id,
            "seq": seq,
            "view": self.view,
            "digest": tx_digest,
            "request": tx,
        }

        with self.lock:
            self.requests[seq] = {
                "tx": tx,
                "tx_id": tx_id,
                "digest": tx_digest,
                "view": self.view,
                "pre_prepare": True,
                "prepares": {self.sid: True},  # self-prepare
                "commits": set(),
                "decision": None,
                "phase": "pre_prepare",
            }
            # Ghi PBFT log va WAL vao RocksDB
            self.store.put_pbft_log("pre_prepare", seq, tx_digest, self.sid, msg)
            self.store.put_wal(tx_id, "READY", VOTE_COMMIT, tx_data=tx)

        if self.is_malicious:
            # Equivocation: gui message sai/khac cho Node 3 de pha hoai dong thuan
            self.log.info("PBFT_BYZANTINE", f"Node {self.sid} (Byzantine): thuc hien EQUIVOCATION tai seq {seq}!")
            tx_fake = dict(tx)
            tx_fake["data"] = tx["data"] + " - FAKE BYZANTINE"
            digest_fake = hash_message(tx_fake)
            
            for d in range(NUM_SITES):
                if d == self.sid:
                    continue
                if d == 3:
                    net_send(
                        None, self.sid, d, MSG_PRE_PREPARE, tx_id=tx_id,
                        view=self.view, seq=seq, digest=digest_fake, request=tx_fake,
                    )
                else:
                    net_send(
                        None, self.sid, d, MSG_PRE_PREPARE, tx_id=tx_id,
                        view=self.view, seq=seq, digest=tx_digest, request=tx,
                    )
        else:
            # Broadcast binh thuong
            net_broadcast(
                None, self.sid, MSG_PRE_PREPARE,
                tx_id=tx_id, seq=seq, view=self.view, digest=tx_digest, request=tx,
            )

        # Tu dong broadcast PREPARE
        self._send_prepare(seq, tx_id, tx_digest)

    def _handle_pre_prepare(self, msg: dict):
        """Xu ly PRE-PREPARE tu leader."""
        sender = msg.get("sender")
        seq = msg.get("seq")
        msg_view = msg.get("view")
        tx = msg.get("request")
        tx_id = msg.get("tx_id")
        tx_digest = msg.get("digest")

        # Validation
        if sender != get_leader(msg_view):
            self.log.info("PBFT_ERROR", "PRE-PREPARE tu non-leader Node %d" % sender)
            return

        if msg_view != self.view:
            self.log.info("PBFT_ERROR", "PRE-PREPARE view %d != current view %d" % (msg_view, self.view))
            return

        if not tx:
            self.log.info("PBFT_ERROR", "PRE-PREPARE thieu request data")
            return

        computed_digest = hash_message(tx)
        if tx_digest != computed_digest:
            self.log.info("PBFT_ERROR", "PRE-PREPARE digest %s != computed digest %s" % (tx_digest, computed_digest))
            return

        self.log.info(
            "PBFT_PRE_PREPARE_RX",
            "tx_id=%d | Nhan PRE-PREPARE tu Leader (seq=%d, view=%d, digest=%s)" % (tx_id, seq, msg_view, tx_digest),
        )

        with self.lock:
            self.requests[seq] = {
                "tx": tx,
                "tx_id": tx_id,
                "digest": tx_digest,
                "view": msg_view,
                "pre_prepare": True,
                "prepares": {},
                "commits": set(),
                "decision": None,
                "phase": "pre_prepare",
            }
            # Ghi log va WAL
            self.store.put_pbft_log("pre_prepare", seq, tx_digest, sender, msg)
            self.store.put_wal(tx_id, "READY", VOTE_COMMIT, tx_data=tx)

        # Broadcast PREPARE
        self._send_prepare(seq, tx_id, tx_digest)

    def _handle_client_request(self, msg: dict):
        """Xu ly CLIENT_REQUEST tu client."""
        # 1. Verify client signature
        client_pubkey = msg.get("client_pubkey")
        if not client_pubkey:
            self.log.info("PBFT_ERROR", "CLIENT_REQUEST thieu client_pubkey")
            return
        
        from crypto_utils import verify_signature_real
        if not verify_signature_real(bytes.fromhex(client_pubkey), msg):
            self.log.info("PBFT_ERROR", "Chu ky client KHONG HOP LE")
            return

        op = msg.get("operation")
        client_id = msg.get("client_id")
        client_addr = msg.get("client_address")

        if is_leader(self.sid, self.view):
            # Leader: tao transaction va khoi dong PBFT
            seq = self.sequence_manager.get_next_seq()
            tx = {
                "tx_id": seq,
                "data": op,
                "client_id": client_id,
                "client_address": client_addr,
            }
            self.log.info("CLIENT_REQUEST", f"Leader nhan CLIENT_REQUEST tu {client_id}: {op} -> gan seq={seq}")
            self._send_pre_prepare(tx, seq)
        else:
            # Backup: gui REDIRECT ve client
            leader_id = get_leader(self.view)
            self.log.info("CLIENT_REQUEST", f"Backup nhan CLIENT_REQUEST, redirect ve Leader {leader_id}")
            if client_addr:
                host, port = client_addr
                reply = {
                    "type": MSG_CLIENT_REPLY,
                    "sender": self.sid,
                    "result": "REDIRECT",
                    "leader": leader_id,
                }
                sign_message(reply, self.sid)
                from network.tcp_client import send_message
                send_message(host, port, reply)

    # ============================================================
    # PHASE 2: PREPARE
    # ============================================================

    def _send_prepare(self, seq: int, tx_id: int, digest: str):
        """Broadcast PREPARE message."""
        self.log.info(
            "PBFT_PREPARE",
            "tx_id=%d | Gui PREPARE (seq=%d, view=%d, digest=%s)" % (tx_id, seq, self.view, digest),
        )

        msg = {
            "type": MSG_PREPARE,
            "sender": self.sid,
            "tx_id": tx_id,
            "seq": seq,
            "view": self.view,
            "digest": digest,
        }

        # Ghi log prepare cua chinh minh
        self.store.put_pbft_log("prepare", seq, digest, self.sid, msg)

        net_broadcast(
            None, self.sid, MSG_PREPARE,
            tx_id=tx_id, seq=seq, view=self.view, digest=digest,
        )

        with self.lock:
            if seq in self.requests:
                self.requests[seq]["prepares"][self.sid] = True
                self.requests[seq]["phase"] = "prepare"
                self._check_prepare_quorum(seq)

    def _handle_prepare(self, msg: dict):
        """Xu ly PREPARE message tu node khac."""
        sender = msg.get("sender")
        seq = msg.get("seq")
        msg_view = msg.get("view")
        tx_id = msg.get("tx_id")
        digest = msg.get("digest")

        if msg_view != self.view:
            return

        with self.lock:
            if seq not in self.requests:
                return

            self.store.put_pbft_log("prepare", seq, digest, sender, msg)

            if sender in self.requests[seq]["prepares"]:
                return

            self.requests[seq]["prepares"][sender] = True
            self.log.info(
                "PBFT_PREPARE_RX",
                "tx_id=%d | Nhan PREPARE tu Node %d (co %d/2f+1=%d)"
                % (tx_id, sender, len(self.requests[seq]["prepares"]), QUORUM),
            )

            self._check_prepare_quorum(seq)

    def _check_prepare_quorum(self, seq: int):
        """Kiem tra da du 2f+1 PREPARE chua, neu co thi broadcast COMMIT."""
        if seq not in self.requests:
            return

        req = self.requests[seq]
        if len(req["prepares"]) >= QUORUM and req["phase"] == "prepare":
            req["phase"] = "prepare_ready"
            self.log.info(
                "PBFT_PREPARE_QUORUM",
                "tx_id=%d | Da du %d PREPARE, chuyen sang COMMIT"
                % (req["tx_id"], QUORUM),
            )
            self._send_commit(seq, req["tx_id"], req["digest"])

    # ============================================================
    # PHASE 3: COMMIT
    # ============================================================

    def _send_commit(self, seq: int, tx_id: int, digest: str):
        """Broadcast COMMIT message."""
        self.log.info(
            "PBFT_COMMIT",
            "tx_id=%d | Gui COMMIT (seq=%d, view=%d, digest=%s)" % (tx_id, seq, self.view, digest),
        )

        msg = {
            "type": MSG_COMMIT,
            "sender": self.sid,
            "tx_id": tx_id,
            "seq": seq,
            "view": self.view,
            "digest": digest,
        }

        self.store.put_pbft_log("commit", seq, digest, self.sid, msg)

        net_broadcast(
            None, self.sid, MSG_COMMIT,
            tx_id=tx_id, seq=seq, view=self.view, digest=digest,
        )

        with self.lock:
            if seq in self.requests:
                self.requests[seq]["commits"].add(self.sid)
                self.requests[seq]["phase"] = "commit"
                self._check_commit_quorum(seq)

    def _handle_commit(self, msg: dict):
        """Xu ly COMMIT message tu node khac."""
        sender = msg.get("sender")
        seq = msg.get("seq")
        msg_view = msg.get("view")
        tx_id = msg.get("tx_id")
        digest = msg.get("digest")

        if msg_view != self.view:
            return

        with self.lock:
            if seq not in self.requests:
                return

            self.store.put_pbft_log("commit", seq, digest, sender, msg)

            if sender in self.requests[seq]["commits"]:
                return

            self.requests[seq]["commits"].add(sender)
            self.log.info(
                "PBFT_COMMIT_RX",
                "tx_id=%d | Nhan COMMIT tu Node %d (co %d/2f+1=%d)"
                % (tx_id, sender, len(self.requests[seq]["commits"]), QUORUM),
            )

            self._check_commit_quorum(seq)

    def _check_commit_quorum(self, seq: int):
        """Kiem tra da du 2f+1 COMMIT chua, neu co thi thuc thi."""
        if seq not in self.requests:
            return

        req = self.requests[seq]
        if len(req["commits"]) >= QUORUM and req["decision"] is None:
            req["decision"] = VOTE_COMMIT
            req["phase"] = "executed"
            self.store.put_wal(req["tx_id"], VOTE_COMMIT, VOTE_COMMIT, tx_data=req["tx"])
            self.log.info(
                "PBFT_EXECUTE",
                "tx_id=%d | DA DONG THUAN! Thuc thi giao dich..." % req["tx_id"],
            )
            self._execute_request(req["tx"], seq)

            # Checkpoint moi K = 2 giao dich
            if seq % 2 == 0:
                self._trigger_checkpoint(seq)

    # ============================================================
    # EXECUTION
    # ============================================================

    def _execute_request(self, tx: dict, seq: int):
        """Thuc thi giao dich da duoc dong thuan."""
        tx_id = tx["tx_id"]

        try:
            parts = tx["data"].split(" ")
            src = parts[0]
            amount = int(parts[2])
            dst = parts[4]
            success = self.store.transfer(src, dst, amount)
            if success:
                self.store.put_ledger(tx_id, tx)
                self.log.info(
                    "PBFT_STATE_UPDATE",
                    "Chuyen %d tu %s sang %s thanh cong. Balance %s: %d, %s: %d"
                    % (amount, src, dst, src, self.store.get_balance(src), dst, self.store.get_balance(dst)),
                )
            else:
                self.log.info("PBFT_STATE_FAIL", "Giao dich that bai do khong du so du!")
        except Exception as e:
            self.log.info("PBFT_ERROR", "Loi thuc thi: %s" % str(e))

        print(">>> PBFT SITE %d: TX %d DA DUOC THUC THI (seq=%d, view=%d) <<<" % (self.sid, tx_id, seq, self.view), flush=True)
        self.last_request_time = time.time()
        self.last_executed_seq = max(self.last_executed_seq, seq)

        # Gui CLIENT_REPLY neu day la request tu client thuc te
        client_addr = tx.get("client_address")
        if client_addr:
            host, port = client_addr
            reply = {
                "type": MSG_CLIENT_REPLY,
                "sender": self.sid,
                "seq": seq,
                "result": "SUCCESS",
            }
            sign_message(reply, self.sid)
            from network.tcp_client import send_message
            send_message(host, port, reply)
            self.log.info("CLIENT_REPLY", f"Site {self.sid}: Gui CLIENT_REPLY (SUCCESS) cho {tx.get('client_id')} tai {host}:{port}")

    # ============================================================
    # CHECKPOINT
    # ============================================================

    def _trigger_checkpoint(self, seq: int):
        """Khoi tao va phat tin nhan Checkpoint."""
        accounts = ["A", "B", "C", "D"]
        balances = {acc: self.store.get_balance(acc) for acc in accounts}
        balances_serialized = json.dumps(balances, sort_keys=True)
        state_digest = compute_digest(balances_serialized.encode("utf-8"))

        self.log.info(
            "CHECKPOINT_TRIGGER",
            "Site %d: Khoi tao checkpoint (seq=%d, digest=%s)"
            % (self.sid, seq, state_digest),
        )

        net_broadcast(
            None, self.sid, MSG_CHECKPOINT, tx_id=0,
            seq=seq, digest=state_digest,
        )

        # Tu bau phieu cho minh
        self._handle_checkpoint({
            "type": MSG_CHECKPOINT,
            "sender": self.sid,
            "seq": seq,
            "digest": state_digest,
        })

    def _handle_checkpoint(self, msg: dict):
        """Thu thap va xac thuc cac tin nhan checkpoint."""
        sender = msg.get("sender")
        seq = msg.get("seq")
        digest = msg.get("digest")

        if seq is None or digest is None:
            return

        with self.lock:
            if seq not in self.checkpoints:
                self.checkpoints[seq] = {}
            if digest not in self.checkpoints[seq]:
                self.checkpoints[seq][digest] = set()

            self.checkpoints[seq][digest].add(sender)
            votes_count = len(self.checkpoints[seq][digest])

            self.log.info(
                "CHECKPOINT_RX",
                "Nhan CHECKPOINT tu Node %d (seq=%d, digest=%s, co %d/%d)"
                % (sender, seq, digest, votes_count, QUORUM),
            )

            # Quorum 2f+1 dat duoc de tao stable checkpoint
            if votes_count >= QUORUM:
                last_stable, _ = self.store.load_checkpoint()
                if seq > last_stable:
                    accounts = ["A", "B", "C", "D"]
                    balances = {acc: self.store.get_balance(acc) for acc in accounts}
                    self.store.save_checkpoint(seq, balances)
                    
                    self.log.info("CHECKPOINT_STABLE", "Checkpoint tai seq %d da tro nen STABLE!" % seq)
                    
                    # Cat tia RocksDB WAL va PBFT logs cu duoi seq
                    self.store.delete_old_wal(seq)
                    self.store.delete_old_pbft_logs(seq)

    # ============================================================
    # RECOVERY SUPPORT
    # ============================================================

    def _handle_recovery_request(self, msg: dict):
        """Tra loi REQUEST_VOTES tu node dang phuc hoi."""
        sender = msg.get("sender")
        tx_id = msg.get("tx_id")
        self.log.info("PBFT_RECOVERY", "tx_id=%d | REQUEST_VOTES tu Node %d" % (tx_id, sender))
        net_send(None, self.sid, sender, MSG_RESP, tx_id, vote=VOTE_COMMIT)

    def _handle_recovery_response(self, msg: dict):
        """Xu ly RESPONSE tu node khac (ho tro phuc hoi)."""
        pass

    # ============================================================
    # MESSAGE PROCESSING LOOP
    # ============================================================

    def process_messages(self):
        """Vong lap xu ly message tu incoming_queue."""
        import queue as queue_module

        while not self.shutdown.is_set():
            try:
                msg = self.incoming_queue.get(timeout=0.5)
                self.handle_message(msg)
            except queue_module.Empty:
                continue
            except Exception as e:
                self.log.info("PBFT_ERROR", "Loi process_messages: %s" % str(e))
