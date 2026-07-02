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
        """Luong duy tri ket noi TCP lau dai den mot node.
        - Tu dong phat hien socket chet bang cach gui heartbeat moi 1 giay.
        - Neu socket chet (broken pipe / connection refused), dong va tao lai.
        """
        host, port = addr
        consecutive_failures = 0
        while self.running:
            with self.lock:
                sock = self.connections.get(dst)

            if sock is not None:
                # Health check: gui newline de kiem tra socket con song khong (tranh no-op)
                try:
                    sock.settimeout(0.5)
                    sock.send(b'\n')
                    sock.settimeout(3.0)
                    # Socket con song, reset dem loi
                    consecutive_failures = 0
                except Exception as e:
                    # Socket da chet (broken pipe, connection reset, etc.)
                    logger.warning(f"Node {self.sid}: Socket den Node {dst} KHONG CON SONG ({e}), tien hanh dong va tao lai...")
                    with self.lock:
                        if dst in self.connections:
                            try:
                                self.connections[dst].close()
                            except Exception:
                                pass
                            del self.connections[dst]
                    sock = None
                    consecutive_failures += 1

            if sock is None:
                try:
                    # Resolve DNS de lay IP moi nhat (quan trong voi Docker/Podman)
                    import socket as dns_resolver
                    resolved_addr = dns_resolver.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                    real_host = resolved_addr[0][4][0]
                    
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3.0)
                    s.connect((real_host, port))
                    with self.lock:
                        self.connections[dst] = s
                    logger.info(f"Node {self.sid}: Da ket noi TCP thanh cong den Node {dst} ({real_host}:{port})")
                    consecutive_failures = 0
                except Exception as e:
                    wait_time = min(2.0 * (1 + consecutive_failures), 10.0)  # Exponential backoff: 2s, 4s, 8s, 10s...
                    logger.debug(f"Node {self.sid}: Ket noi that bai den Node {dst}: {e}, thu lai sau {wait_time}s")
                    time.sleep(wait_time)
                    continue
            
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
