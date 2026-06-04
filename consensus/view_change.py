"""
[PHASE 4] View Change Protocol cho PBFT.

Khi leader bi loi (timeout, gui message sai), cac backup node
se thuc hien view change de bau leader moi.

Quy trinh:
  1. Backup phat hien leader khong hoat dong (timeout)
  2. Backup broadcast VIEW-CHANGE message
  3. Leader moi (view+1 % N) thu thap 2f+1 VIEW-CHANGE
  4. Leader moi broadcast NEW-VIEW message
  5. Tat ca node chuyen sang view moi
"""

import time
import threading

from config import config, NUM_SITES, QUORUM, F, TIMEOUT
from network import net_broadcast, net_send, sign_message

from config import MsgType
MSG_VIEW_CHANGE = MsgType.VIEW_CHANGE
MSG_NEW_VIEW = MsgType.NEW_VIEW


class ViewChangeManager:
    """
    Quan ly view change protocol.
    
    Khi phat hien leader hien tai khong the tien trien (timeout),
    backup node se khoi tao view change.
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

    def check_view_timeout(self):
        """
        Kiem tra xem da den luc can view change chua.
        Goi dinh ky tu message loop.
        """
        if self.pbft.shutdown.is_set():
            return

        now = time.time()
        time_since_last = now - self.pbft.last_request_time

        if time_since_last > self.view_change_timeout:
            if self.pbft.view_change_sent:
                return  # Da gui view change roi
            self._initiate_view_change()

    def _initiate_view_change(self):
        """Bat dau view change: broadcast VIEW-CHANGE message."""
        new_view = self.pbft.view + 1

        with self.lock:
            self.view_change_in_progress = True
            self.pbft.view_change_sent = True

        self.log.info(
            "VIEW_CHANGE_START",
            "Node %d: Bat dau view change tu view %d -> %d"
            % (self.sid, self.pbft.view, new_view),
        )

        # Broadcast VIEW-CHANGE
        net_broadcast(
            None, self.sid, MSG_VIEW_CHANGE, 0,
            new_view=new_view, old_view=self.pbft.view,
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

        # Kiem tra xem node nay co phai leader moi khong
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
        """Leader moi broadcast NEW-VIEW message."""
        self.log.info(
            "NEW_VIEW",
            "Leader moi View %d: broadcast NEW-VIEW" % new_view,
        )

        net_broadcast(
            None, self.sid, MSG_NEW_VIEW, 0,
            new_view=new_view,
        )


        print(
            "*** NEW VIEW: Node %d la leader cua view %d ***"
            % (self.sid, new_view),
            flush=True,
        )

        # Tu dong chuyen sang view moi
        self._apply_new_view(new_view)

    def handle_new_view(self, msg: dict):
        """Xu ly NEW-VIEW message tu leader moi."""
        new_view = msg.get("new_view")
        if new_view is None:
            return

        self.log.info(
            "NEW_VIEW_RX",
            "Nhan NEW-VIEW tu Leader Node %d, chuyen sang view %d"
            % (msg.get("sender"), new_view),
        )

        self._apply_new_view(new_view)

    def _apply_new_view(self, new_view: int):
        """
        Ap dung view moi: cap nhat view, reset trang thai.
        """
        self.pbft.view = new_view
        self.pbft.view_change_sent = False
        self.pbft.last_request_time = time.time()

        with self.lock:
            self.view_change_in_progress = False
            # Giu lai view_changes_collected cho view moi nhat
            to_keep = new_view
            for v in list(self.view_changes_collected.keys()):
                if v < to_keep:
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
