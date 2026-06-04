"""
[PHASE 4] PBFT — Practical Byzantine Fault Tolerance Protocol.

Day la giao thuc dong thuan PBFT day du gom 3 pha:
  1. PRE-PREPARE: Leader (primary) de xuat giao dich + sequence number
  2. PREPARE: Cac node broadcast xac nhan de xuat
  3. COMMIT: Khi co 2f+1 PREPARE, broadcast COMMIT, thuc thi

Leader duoc chon theo view number: leader = view %% N
"""

import os
import sys
import time
import random
import threading
import json

from config import (
    config, NUM_SITES, QUORUM, F,
    TIMEOUT, NET_MIN, NET_MAX, LISTEN_AFTER,
    VOTE_COMMIT, VOTE_ABORT,
    MSG_VOTE, MSG_REQ, MSG_RESP, MSG_DEC,
)
from network import sign_message, verify_signature, net_send, net_broadcast


# PBFT message types (imported from config)
from config import MsgType
MSG_PRE_PREPARE = MsgType.PRE_PREPARE  # 101
MSG_PREPARE = MsgType.PREPARE          # 102
MSG_COMMIT = MsgType.COMMIT            # 103
MSG_VIEW_CHANGE = MsgType.VIEW_CHANGE  # 104
MSG_NEW_VIEW = MsgType.NEW_VIEW        # 105


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
    
    Quan ly cac request (transaction), thuc hien 3 pha PBFT,
    xu ly view change khi leader bi loi.
    """

    def __init__(self, sid: int, incoming_queue, store, log, shutdown_event):
        self.sid = sid
        self.incoming_queue = incoming_queue
        self.store = store
        self.log = log
        self.shutdown = shutdown_event

        # PBFT state
        self.view = 0
        self.sequence_manager = SequenceManager()

        # Per-request state: request_id -> PBFT state
        self.requests = {}  # seq -> PBFT state dict

        # View change tracking
        self.view_change_sent = False
        self.view_changes_received = set()  # set of (view, node_id)
        self.view_change_timeout = TIMEOUT * 2
        self.last_request_time = time.time()

        # Lock cho thread safety (Dung RLock de tranh self-deadlock)
        self.lock = threading.RLock()


    def start(self):
        """Khoi dong PBFT engine."""
        self.log.info("PBFT_START", "PBFT engine da khoi dong (N=%d, f=%d, quorum=%d)" % (NUM_SITES, F, QUORUM))
        leader = get_leader(self.view)
        role = "LEADER" if leader == self.sid else "BACKUP"
        self.log.info("PBFT_VIEW", "View %d, Leader=Node %d, Toi la %s" % (self.view, leader, role))

    # ============================================================
    # HANDLE INCOMING MESSAGES
    # ============================================================

    def handle_message(self, msg: dict):
        """
        Tiep nhan va xu ly mot message.
        Day la ham dispatch chinh cua PBFT engine.
        """
        msg_type = msg.get("type")

        # Xac thuc chu ky
        if not verify_signature(msg):
            self.log.info("PBFT_SECURITY", "Chu ky KHONG HOP LE tu Node %d" % msg.get("sender", -1))
            return

        if msg_type == MSG_PRE_PREPARE:
            self._handle_pre_prepare(msg)
        elif msg_type == MSG_PREPARE:
            self._handle_prepare(msg)
        elif msg_type == MSG_COMMIT:
            self._handle_commit(msg)
        elif msg_type == MSG_VIEW_CHANGE:
            self._handle_view_change(msg)
        elif msg_type == MSG_NEW_VIEW:
            self._handle_new_view(msg)
        elif msg_type == MSG_REQ:
            self._handle_recovery_request(msg)
        elif msg_type == MSG_RESP:
            self._handle_recovery_response(msg)
        else:
            # Unknown message type
            pass

    # ============================================================
    # REQUEST FLOW (from client)
    # ============================================================

    def submit_request(self, tx: dict):
        """
        Gui mot giao dich vao he thong PBFT.
       
        Neu node nay la leader: tao PRE-PREPARE va broadcast.
        Neu node la backup: gui request len leader.
        """
        tx_id = tx["tx_id"]
        
        if is_leader(self.sid, self.view):
            # Leader: tao PRE-PREPARE
            seq = self.sequence_manager.get_next_seq()
            self._send_pre_prepare(tx, seq)
        else:
            # Backup: forward to leader (hoac tu xu ly bang broadcast)
            # Trong implementation nay, backup cung tu xu ly
            # bang cach cho PRE-PREPARE tu leader
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
        self.log.info(
            "PBFT_PRE_PREPARE",
            "tx_id=%d | Leader: gui PRE-PREPARE (seq=%d, view=%d)" % (tx_id, seq, self.view),
        )

        # Luu trang thai request
        with self.lock:
            self.requests[seq] = {
                "tx": tx,
                "tx_id": tx_id,
                "view": self.view,
                "pre_prepare": True,
                "prepares": {self.sid: True},  # self-prepare
                "commits": set(),
                "decision": None,
                "phase": "pre_prepare",
            }
        self.store.put_wal(tx_id, "READY", VOTE_COMMIT)

        # Broadcast PRE-PREPARE to all
        net_broadcast(
            None, self.sid, MSG_PRE_PREPARE,
            tx_id=tx_id, seq=seq, view=self.view, tx_data=tx,
        )

        # Tu dong broadcast PREPARE (rule: nhan PRE-PREPARE -> broadcast PREPARE)
        self._send_prepare(seq, tx_id)

    def _handle_pre_prepare(self, msg: dict):
        """Xu ly PRE-PREPARE tu leader."""
        sender = msg.get("sender")
        seq = msg.get("seq")
        msg_view = msg.get("view")
        tx = msg.get("tx_data")
        tx_id = msg.get("tx_id")

        # Validation
        if sender != get_leader(msg_view):
            self.log.info("PBFT_ERROR", "PRE-PREPARE tu non-leader Node %d" % sender)
            return

        if msg_view != self.view:
            self.log.info("PBFT_ERROR", "PRE-PREPARE view %d != current view %d" % (msg_view, self.view))
            return

        if not tx:
            self.log.info("PBFT_ERROR", "PRE-PREPARE thieu tx_data")
            return

        self.log.info(
            "PBFT_PRE_PREPARE_RX",
            "tx_id=%d | Nhan PRE-PREPARE tu Leader (seq=%d, view=%d)" % (tx_id, seq, msg_view),
        )

        # Luu trang thai request
        with self.lock:
            self.requests[seq] = {
                "tx": tx,
                "tx_id": tx_id,
                "view": msg_view,
                "pre_prepare": True,
                "prepares": {},
                "commits": set(),
                "decision": None,
                "phase": "pre_prepare",
            }
        self.store.put_wal(tx_id, "READY", VOTE_COMMIT)


        # Broadcast PREPARE
        self._send_prepare(seq, tx_id)

    # ============================================================
    # PHASE 2: PREPARE
    # ============================================================

    def _send_prepare(self, seq: int, tx_id: int):
        """Broadcast PREPARE message."""
        self.log.info(
            "PBFT_PREPARE",
            "tx_id=%d | Gui PREPARE (seq=%d, view=%d)" % (tx_id, seq, self.view),
        )

        net_broadcast(
            None, self.sid, MSG_PREPARE,
            tx_id=tx_id, seq=seq, view=self.view,
        )

        # Tu ghi nhan PREPARE
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

        if msg_view != self.view:
            return  # Bo qua PREPARE tu view cu

        with self.lock:
            if seq not in self.requests:
                return
            if sender in self.requests[seq]["prepares"]:
                return  # Da nhan

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
        # PREPARE quorum = 2f+1 (bao gom self)
        if len(req["prepares"]) >= QUORUM and req["phase"] == "prepare":
            req["phase"] = "prepare_ready"
            self.log.info(
                "PBFT_PREPARE_QUORUM",
                "tx_id=%d | Da du %d PREPARE, chuyen sang COMMIT"
                % (req["tx_id"], QUORUM),
            )
            self._send_commit(seq, req["tx_id"])

    # ============================================================
    # PHASE 3: COMMIT
    # ============================================================

    def _send_commit(self, seq: int, tx_id: int):
        """Broadcast COMMIT message."""
        self.log.info(
            "PBFT_COMMIT",
            "tx_id=%d | Gui COMMIT (seq=%d, view=%d)" % (tx_id, seq, self.view),
        )

        net_broadcast(
            None, self.sid, MSG_COMMIT,
            tx_id=tx_id, seq=seq, view=self.view,
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

        if msg_view != self.view:
            return

        with self.lock:
            if seq not in self.requests:
                return
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
            self.store.put_wal(req["tx_id"], VOTE_COMMIT, VOTE_COMMIT)
            self.log.info(
                "PBFT_EXECUTE",
                "tx_id=%d | DA DONG THUAN! Thuc thi giao dich..." % req["tx_id"],
            )
            self._execute_request(req["tx"], seq)


    # ============================================================
    # EXECUTION
    # ============================================================

    def _execute_request(self, tx: dict, seq: int):
        """
        Thuc thi giao dich da duoc dong thuan.
        Cap nhat world state va ledger.
        """
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
                self.log.warning("PBFT_STATE_FAIL", "Giao dich that bai do khong du so du!")
        except Exception as e:
            self.log.info("PBFT_ERROR", "Loi thuc thi: %s" % str(e))

        print(">>> PBFT SITE %d: TX %d DA DUOC THUC THI (seq=%d, view=%d) <<<" % (self.sid, tx_id, seq, self.view), flush=True)
        self.last_request_time = time.time()

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
        pass  # Handled by old consensus logic for now

    # ============================================================
    # MESSAGE PROCESSING LOOP
    # ============================================================

    def process_messages(self):
        """
        Vong lap xu ly message tu incoming_queue.
        Chay trong mot luong rieng.
        """
        import queue as queue_module

        while not self.shutdown.is_set():
            try:
                msg = self.incoming_queue.get(timeout=0.5)
                self.handle_message(msg)
            except queue_module.Empty:
                continue
            except Exception as e:
                self.log.info("PBFT_ERROR", "Loi process_messages: %s" % str(e))
