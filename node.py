"""
[PHASE 4] Node — Entry point su dung PBFT consensus.

Moi node chay:
  - TCPServer de nhan message
  - KVStore (RocksDB) de luu WAL, state, ledger
  - PBFTConsensus engine de dong thuan
"""

import os
import sys
import time
import random
import threading
import logging

# Cau hinh logging de in ra stdout/stderr cho tung node
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from config import (
    config,
    NUM_SITES,
    QUORUM,
    TIMEOUT,
    NET_MIN,
    NET_MAX,
    LISTEN_AFTER,
    NODE_ADDRESSES,
    TRANSACTIONS,
    VOTE_COMMIT,
    MSG_REQ,
)
from logger import Logger
from storage.rocksdb_store import KVStore
from network import (
    sign_message,
    verify_signature,
    net_send,
    net_broadcast,
    register_connection_pool,
)
from network.tcp_server import TCPServer
from consensus.pbft import PBFTConsensus
from consensus.view_change import ViewChangeManager


class Node:
    """
    Dai dien cho mot node BFT voi PBFT consensus.
    """

    def __init__(
        self,
        sid: int,
        is_malicious: bool = False,
        crash_on_tx: int = None,
        shutdown_event=None,
    ):
        self.sid = sid
        self.is_malicious = is_malicious
        self.crash_on_tx = crash_on_tx
        self.shutdown = shutdown_event or threading.Event()

        host, port = NODE_ADDRESSES[sid]
        self.tcp_server = TCPServer("0.0.0.0", port)

        self.log = Logger(sid)
        self.store = KVStore(sid)
        self.incoming_queue = self.tcp_server.incoming_queue

        # Khoi tao connection pool
        self.pool = register_connection_pool(sid, NODE_ADDRESSES)

        # PBFT engine
        self.pbft = PBFTConsensus(
            sid,
            self.incoming_queue,
            self.store,
            self.log,
            self.shutdown,
            is_malicious=self.is_malicious,
        )
        self.view_change = ViewChangeManager(sid, self.log, self.pbft)
        self.pbft.view_change = self.view_change

    def start(self):
        """Khoi dong node."""
        self.tcp_server.start()
        self.pool.start_connections()
        self.pbft.start()

        role = "BYZANTINE (equivocation)" if self.is_malicious else "Trung thuc"
        self.log.info("STARTUP", "Vai tro: %s | PBFT Engine: %s" % (role, self.pbft))
        print(
            "[Node %d] Khoi dong tai %s:%d, vai tro: %s"
            % (self.sid, *NODE_ADDRESSES[self.sid], role),
            flush=True,
        )

    def stop(self):
        """Dung node."""
        self.tcp_server.stop()
        self.pool.close()
        self.store.close()

    def process_transactions(self, tx_list):
        """
        Xu ly danh sach giao dich bang PBFT.
        Moi giao dich duoc submit vao PBFT engine.
        """
        if config.random_seed is not None:
            random.seed(config.random_seed + self.sid)
        else:
            random.seed(os.getpid())

        # Start message processing thread
        msg_thread = threading.Thread(target=self.pbft.process_messages, daemon=True)
        msg_thread.start()

        # Start view change checker thread
        def check_view_change():
            while not self.shutdown.is_set():
                self.view_change.check_view_timeout()
                time.sleep(1.0)

        vc_thread = threading.Thread(target=check_view_change, daemon=True)
        vc_thread.start()

        # Submit transactions one by one
        for tx in tx_list:
            if self.shutdown.is_set():
                break

            tx_id = tx["tx_id"]
            self.log.info("TX_START", "tx_id=%d | === BAT DAU PBFT ===" % tx_id)
            print(
                "[Node %d] === BAT DAU XU LY TX %d ===" % (self.sid, tx_id),
                flush=True,
            )

            # Cho leader submit request (moi node deu submit de dam bao test)
            # Trong thuc te, chi leader submit, nhung de test ca node deu submit
            self.pbft.submit_request(tx)

            # Doi PBFT hoan thanh
            time.sleep(TIMEOUT + 2)

        # Doi cac luong ket thuc
        time.sleep(2)

    def get_ledger(self) -> list:
        return self.store.get_ledger()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python node.py <node_id>")
        print("  node_id: 0, 1, 2, 3 (must match NODE_ADDRESSES)")
        sys.exit(1)

    sid = int(sys.argv[1])
    listen_mode = "--listen" in sys.argv

    if listen_mode:
        # Che do Docker/listen: luon chay honest de demo giao dich binh thuong
        # Muon demo Byzantine, dung: python main.py (chay qua orchestrator)
        is_malicious = False
    else:
        is_malicious = (sid in config.malicious_sites) and (
            "--no-byzantine" not in sys.argv
        )

    crash_on_tx = config.crash_config.get(sid)

    node = Node(sid, is_malicious=is_malicious, crash_on_tx=crash_on_tx)
    node.start()

    if "--listen" in sys.argv:
        # Che do chay lau dai cho client gui giao dich tu ben ngoai (Docker)
        print(f"[Node {sid}] Dang chay che do lang nghe lien tuc...", flush=True)

        # Start message processing thread
        msg_thread = threading.Thread(target=node.pbft.process_messages, daemon=True)
        msg_thread.start()

        # Start view change checker thread
        def check_view_change():
            while not node.shutdown.is_set():
                node.view_change.check_view_timeout()
                time.sleep(1.0)

        vc_thread = threading.Thread(target=check_view_change, daemon=True)
        vc_thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        # Che do demo cu chay qua danh sach giao dich co san va dung
        time.sleep(1)
        node.process_transactions(TRANSACTIONS)

        ledger = node.get_ledger()
        node.log.info("LEDGER_FINAL", "So cai: %d giao dich" % len(ledger))
        print(
            "[Node %d] Ket thuc. Ledger co %d giao dich." % (sid, len(ledger)),
            flush=True,
        )
        time.sleep(2)

    node.stop()
