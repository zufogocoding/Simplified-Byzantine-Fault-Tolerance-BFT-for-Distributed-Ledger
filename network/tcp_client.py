"""
TCP Client — Gui message JSON den cac node khac.

Cung cap ham send_message gui mot message JSON den mot node TCP.
Moi message duoc serialize thanh JSON + newline, dam bao ben nhan
co the phan tach cac message.
"""

import json
import socket
import logging

logger = logging.getLogger(__name__)


def send_message(host: str, port: int, message: dict) -> bool:
    """
    Gui mot message JSON den node tai (host, port).

    Args:
        host: Dia chi IP/dns cua node dich.
        port: Cong TCP cua node dich.
        message: Dict message can gui (se duoc serialize thanh JSON).

    Returns:
        True neu gui thanh cong, False neu that bai.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((host, port))

        # Serialize message thanh JSON + newline delimiter
        data = json.dumps(message, ensure_ascii=False) + "\n"
        sock.sendall(data.encode("utf-8"))
        sock.close()
        return True
    except socket.timeout:
        logger.warning(f"Timeout khi gui den {host}:{port}")
        return False
    except ConnectionRefusedError:
        logger.warning(f"Ket noi bi tu choi: {host}:{port}")
        return False
    except Exception as e:
        logger.error(f"Loi khi gui message den {host}:{port}: {e}")
        return False
