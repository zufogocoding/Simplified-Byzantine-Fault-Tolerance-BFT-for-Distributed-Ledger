"""
[PHASE 1] View Change Protocol cho PBFT.

Khi leader bi loi (timeout/heartbeat), cac backup node thuc hien view change
bang cach gui bang chung prepared_certs va bau leader moi.
"""

import time
import threading
import logging

from config import config, NUM_SITES, QUORUM, F, TIMEOUT
from network import net_broadcast, net_send, sign_message, verify_signature

from config import MsgType
MSG_VIEW_CHANGE = MsgType.VIEW_CHANGE
MSG_NEW_VIEW = MsgType.NEW_VIEW

logger = logging.getLogger(__name__)


class ViewChangeManager:
    """
    Quan ly view change protocol voi day du bang chung (prepared_certs)
    va thiet lap view moi thong qua tin nhan NEW-VIEW.
    """

    def __init__(self, sid, log, pbft_engine):
        self.sid = sid
        self.log = log
        self.pbft = pbft_engine  # PBFTConsensus instance

        # View change state
        self.view_change_in_progress = False
        self.view_changes_collected = {}  # {view: {sender_id: msg}}
        self.view_change_timeout = TIMEOUT * 2.5
        self.lock = threading.RLock()

    def get_leader(self, view: int) -> int:
        """Xac dinh leader cho mot view."""
        return view % NUM_SITES

    def check_view_timeout(self, force=False):
        """Kiem tra xem da den luc can view change chua."""
        if self.pbft.shutdown.is_set():
            return

        now = time.time()
        time_since_last = now - self.pbft.last_request_time

        if force or time_since_last > self.view_change_timeout:
            if self.pbft.view_change_sent:
                return  # Da gui view change roi
            self._initiate_view_change()

    def _initiate_view_change(self):
        """Bat dau view change: broadcast VIEW-CHANGE message kem prepared_certs."""
        new_view = self.pbft.view + 1

        with self.lock:
            self.view_change_in_progress = True
            self.pbft.view_change_sent = True

        # Thu thap cac prepared_certs (chung chi chuan bi) tu log RocksDB/in-memory
        prepared_certs = {}
        last_executed = self.pbft.last_executed_seq

        with self.pbft.lock:
            for seq, req in self.pbft.requests.items():
                if seq > last_executed:
                    # Kiem tra neu da nhan du quorum prepares
                    prepares_dict = req.get("prepares", {})
                    if len(prepares_dict) >= QUORUM:
                        # Lay pre-prepare log tu RocksDB
                        digest = req["digest"]
                        pre_prepare_logs = self.pbft.store.get_pbft_logs("pre_prepare", seq, digest)
                        prepare_logs = self.pbft.store.get_pbft_logs("prepare", seq, digest)
                        prepared_certs[str(seq)] = {
                            "pre_prepare": pre_prepare_logs[0] if pre_prepare_logs else None,
                            "prepares": prepare_logs,
                        }

        self.log.info(
            "VIEW_CHANGE_START",
            "Node %d: Yeu cau view change tu %d -> %d (last_seq=%d, %d certs)"
            % (self.sid, self.pbft.view, new_view, last_executed, len(prepared_certs)),
        )

        # Broadcast VIEW-CHANGE
        net_broadcast(
            self.sid, MSG_VIEW_CHANGE, 0,
            new_view=new_view,
            old_view=self.pbft.view,
            last_seq=last_executed,
            prepared_certs=prepared_certs,
        )

        print(
            "*** VIEW CHANGE: Node %d yeu cau chuyen tu view %d -> %d ***"
            % (self.sid, self.pbft.view, new_view),
            flush=True,
        )

    def handle_view_change(self, msg: dict):
        """
        Xu ly VIEW-CHANGE message tu node khac.
        Neu node nay la leader moi, thu thap 2f+1 va broadcast NEW-VIEW.
        """
        sender = msg.get("sender")
        new_view = msg.get("new_view")

        if new_view is None:
            return

        # Chi nhan view_change cho view lon hon view hien tai
        if new_view <= self.pbft.view:
            return

        new_leader = self.get_leader(new_view)
        is_new_leader = (new_leader == self.sid)

        with self.lock:
            if new_view not in self.view_changes_collected:
                self.view_changes_collected[new_view] = {}
            self.view_changes_collected[new_view][sender] = msg
            collected = len(self.view_changes_collected[new_view])

        self.log.info(
            "VIEW_CHANGE_RX",
            "Nhan VIEW-CHANGE tu Node %d cho view %d (co %d/2f+1=%d)"
            % (sender, new_view, collected, QUORUM),
        )

        # Neu du 2f+1 va node la leader moi, broadcast NEW-VIEW
        if collected >= QUORUM and is_new_leader:
            self._send_new_view(new_view)

    def _send_new_view(self, new_view: int):
        """Leader moi broadcast NEW-VIEW message chua tap hop VIEW_CHANGE va cac de xuat cu."""
        self.log.info(
            "NEW_VIEW",
            "Leader moi View %d: bat dau phat hanh NEW-VIEW" % new_view,
        )

        # Thu thap V (danh sach cac tin nhan VIEW-CHANGE hop le)
        with self.lock:
            view_changes = list(self.view_changes_collected[new_view].values())

        # Xac dinh tap O (cac request da chuan bi) de lam lai de xuat
        O = {}
        for vc in view_changes:
            certs = vc.get("prepared_certs", {})
            for seq_str, cert in certs.items():
                # Kiem tra tinh hop le cua cert: phai co pre_prepare va >= 2f prepares
                pre_prepare = cert.get("pre_prepare")
                prepares = cert.get("prepares", [])
                if pre_prepare and len(prepares) >= (QUORUM - 1): # quorum phieu chu ky khac self
                    # Bieu quyet chon de xuat nay
                    O[seq_str] = pre_prepare

        net_broadcast(
            self.sid, MSG_NEW_VIEW, 0,
            new_view=new_view,
            V=view_changes,
            O=O,
        )

        print(
            "*** NEW VIEW: Node %d la leader cua view %d ***"
            % (self.sid, new_view),
            flush=True,
        )

        # Tu dong chuyen sang view moi va replay O
        self._apply_new_view(new_view, O)

    def handle_new_view(self, msg: dict):
        """Xu ly NEW-VIEW message tu leader moi."""
        new_view = msg.get("new_view")
        V = msg.get("V", [])
        O = msg.get("O", {})

        if new_view is None:
            return

        if new_view <= self.pbft.view:
            return # Bo qua tin nhan view cu

        # Xac minh tinh trung thuc cua V (it nhat 2f+1 tin nhan VIEW-CHANGE co chu ky hop le)
        valid_vc_count = 0
        for vc in V:
            if verify_signature(vc) and vc.get("new_view") == new_view:
                valid_vc_count += 1

        if valid_vc_count < QUORUM:
            self.log.error("NEW_VIEW_ERROR", "NEW-VIEW kem bang chung view changes khong du hoac khong hop le!")
            return

        self.log.info(
            "NEW_VIEW_RX",
            "Nhan NEW-VIEW tu Leader Node %d, chuyen sang view %d"
            % (msg.get("sender"), new_view),
        )

        self._apply_new_view(new_view, O)

    def _apply_new_view(self, new_view: int, O: dict):
        """
        Ap dung view moi: cap nhat view, reset trang thai va replay cac requests.
        """
        self.pbft.view = new_view
        self.pbft.view_change_sent = False
        self.pbft.last_request_time = time.time()
        # Persist view number vao RocksDB de phuc hoi sau restart
        self.pbft.store.save_view(new_view)

        with self.lock:
            self.view_change_in_progress = False
            # Xoa cac view changes cu hon view hien tai
            for v in list(self.view_changes_collected.keys()):
                if v < new_view:
                    del self.view_changes_collected[v]

        leader = self.get_leader(new_view)
        role = "LEADER" if leader == self.sid else "BACKUP"
        self.log.info(
            "VIEW_CHANGED",
            "Da chuyen sang view %d, Leader=Node %d, Toi la %s"
            % (new_view, leader, role),
        )

        print(
            ">>> SITE %d: DA CHUYEN SANG VIEW %d, LEADER = NODE %d <<<"
            % (self.sid, new_view, leader),
            flush=True,
        )

        # Exponential backoff cho timeout (gioi han toi da 8x base de tranh timeout vo han)
        backoff = min(2 ** new_view, 8)
        self.view_change_timeout = self.pbft.base_view_change_timeout * backoff

        # Null Request liveness (Neu O rong va la leader, gui 1 No-Op)
        if leader == self.sid and not O:
            self.log.info("NEW_VIEW_NULL", "Tap O rong, phat Null Request de khoi dong view moi")
            seq = self.pbft.sequence_manager.get_next_seq()
            tx = {"tx_id": seq, "data": "NOOP", "client_id": "__noop__"}
            self.pbft._send_pre_prepare(tx, seq)

        # Redo PRE-PREPARE cho cac request trong O
        for seq_str, pre_prepare in O.items():
            seq = int(seq_str)
            tx = pre_prepare.get("request")
            tx_id = pre_prepare.get("tx_id")
            if seq > self.pbft.last_executed_seq:
                self.log.info("VIEW_CHANGE_REDO", f"Leader phat lai PRE-PREPARE cho tx_id={tx_id} at seq={seq} trong view moi")
                if leader == self.sid:
                    self.pbft._send_pre_prepare(tx, seq)
