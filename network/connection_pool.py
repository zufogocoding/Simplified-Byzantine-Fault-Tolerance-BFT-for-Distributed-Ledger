"""
[PHASE 2] Connection Pool — Duy tri ket noi TCP lien tuc va reconnect.
"""

import socket
import threading
import json
import logging
import time

logger = logging.getLogger(__name__)


class TCPConnectionPool:
    """
    Quan ly mot pool cac ket noi TCP den cac node khac.
    Giup giu cac socket mo lien tuc va tu dong ket noi lai khi mat.
    """

    def __init__(self, sid: int, node_addresses: dict):
        self.sid = sid
        self.node_addresses = node_addresses
        self.connections = {}  # dst_node_id -> socket object
        self.lock = threading.Lock()
        self.running = True

    def start_connections(self):
        """Khoi dong cac luong ket noi den tat ca cac node khac."""
        for dst, addr in self.node_addresses.items():
            if dst == self.sid:
                continue
            t = threading.Thread(
                target=self._keep_connected,
                args=(dst, addr),
                daemon=True,
                name=f"Pool-Conn-{self.sid}->{dst}"
            )
            t.start()

    def _keep_connected(self, dst: int, addr: tuple):
        """Luong duy tri ket noi TCP lau dai den mot node."""
        host, port = addr
        while self.running:
            with self.lock:
                sock = self.connections.get(dst)

            if sock is None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3.0)
                    s.connect((host, port))
                    with self.lock:
                        self.connections[dst] = s
                    logger.info(f"Node {self.sid}: Da ket noi TCP thanh cong den Node {dst} ({host}:{port})")
                except Exception:
                    # Thu lai sau 2 giay neu that bai
                    time.sleep(2.0)
                    continue
            
            # Neu da co socket, kiem tra dinh ky bang cach sleep 1s
            time.sleep(1.0)

    def send_message(self, dst: int, message: dict) -> bool:
        """
        Gui tin nhan den node dich qua ket noi co san.
        Neu socket loi, dong no va thu lai bang cach tao ket noi moi.
        """
        # Node khong can tu gui den chinh no qua TCP
        if dst == self.sid:
            return False

        host, port = self.node_addresses[dst]
        # Serialize tin nhan kem newline delimiter
        data = json.dumps(message, ensure_ascii=False) + "\n"
        data_bytes = data.encode("utf-8")

        for attempt in range(2):
            with self.lock:
                sock = self.connections.get(dst)

            if sock is not None:
                try:
                    sock.sendall(data_bytes)
                    return True
                except Exception as e:
                    logger.warning(f"Node {self.sid}: Loi gui den Node {dst}, se dong socket va ket noi lai: {e}")
                    with self.lock:
                        if dst in self.connections:
                            try:
                                self.connections[dst].close()
                            except Exception:
                                pass
                            del self.connections[dst]

            # Neu khong co socket hoac socket bi loi, thu tao moi lap tuc de gui lai
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3.0)
                s.connect((host, port))
                with self.lock:
                    self.connections[dst] = s
                s.sendall(data_bytes)
                return True
            except Exception as e:
                logger.error(f"Node {self.sid}: Ket noi/gui that bai den Node {dst} ({host}:{port}): {e}")
                time.sleep(0.5)

        return False

    def close(self):
        """Dong toan bo ket noi trong pool."""
        self.running = False
        with self.lock:
            for dst, sock in list(self.connections.items()):
                try:
                    sock.close()
                except Exception:
                    pass
                del self.connections[dst]
        logger.info(f"Node {self.sid}: Pool ket noi TCP da dong")
