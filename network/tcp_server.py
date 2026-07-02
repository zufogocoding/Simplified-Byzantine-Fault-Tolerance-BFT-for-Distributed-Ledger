"""
TCP Server — Lang nghe ket noi tu cac node khac.

Moi node chay mot TCPServer trong mot luong (thread) rieng,
lang nghe tren mot cong TCP, nhan message JSON, day vao
hang doi noi bo de node chinh (node.py) xu ly.
"""

import json
import socket
import threading
import queue
import logging

logger = logging.getLogger(__name__)


class TCPServer:
    """
    TCP server lang nghe tren mot cong, nhan message JSON tu cac node khac.

    Moi message nhan duoc se duoc day vao hang doi noi bo (incoming_queue)
    de node chinh xu ly bat dong bo.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.incoming_queue = queue.Queue()
        self._server_socket = None
        self._running = False
        self._thread = None

    def start(self):
        """Khoi dong server trong mot luong rieng."""
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        logger.info(f"TCPServer da khoi dong: {self.host}:{self.port}")

    def stop(self):
        """Dung server."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        logger.info(f"TCPServer da dung: {self.host}:{self.port}")

    def _serve(self):
        """
        Vong lap chinh cua server: lang nghe ket noi, nhan message,
        day vao hang doi.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.settimeout(1.0)  # Timeout de kiem tra _running
        try:
            self._server_socket.bind((self.host, self.port))
            self._server_socket.listen(10)
        except OSError as e:
            logger.error(f"Khong the bind {self.host}:{self.port}: {e}")
            return

        while self._running:
            try:
                conn, addr = self._server_socket.accept()
                # Xu ly moi ket noi trong mot luong rieng
                client_thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True,
                )
                client_thread.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """
        Xu ly mot ket noi dai han: doc tung dong message JSON duoc phan tach
        bang ky tu xuong dong (\n) va day vao hang doi, lap lai cho den khi
        doi tac dong ket noi.

        Rieng CLIENT_REQUEST (type=107): giu ket noi de PBFT handler
        gui phan hoi tren cung socket, thay vi mo ket noi moi.
        """
        is_client_request = False
        try:
            conn.settimeout(15.0)  # Heartbeat gui moi 1.5s, 15s timeout la an toan
            buffer = b""
            while self._running:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line_bytes, _, rest = buffer.partition(b"\n")
                    buffer = rest
                    line = line_bytes.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        # An tin nhan PING/PONG kieu 109/110 de khoi ngap log console
                        if msg.get('type') not in (109, 110):
                            print(f"  [TCP Server {self.port}] Nhan message type={msg.get('type')} tu Node {msg.get('sender')}", flush=True)

                        # CLIENT_REQUEST: giu socket de gui phan hoi tren cung ket noi
                        if msg.get('type') == 107:  # MsgType.CLIENT_REQUEST
                            msg['_client_conn'] = conn
                            self.incoming_queue.put(msg)
                            is_client_request = True
                            return  # De PBFT handler quan ly socket nay

                        self.incoming_queue.put(msg)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"JSON loi tu {addr}: {e}, data={line[:100]}"
                        )
        except socket.timeout:
            logger.debug(f"Timeout khi nhan du lieu tu {addr}")
        except Exception as e:
            logger.debug(f"Loi khi xu ly ket noi tu {addr}: {e}")
        finally:
            if not is_client_request:
                try:
                    conn.close()
                except OSError:
                    pass
