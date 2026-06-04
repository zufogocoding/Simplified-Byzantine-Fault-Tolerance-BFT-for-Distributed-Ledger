"""
[PHASE 3] BFT Distributed Ledger — Independent Client.

Su dung:
  python client.py --node localhost:5001 --op "A chuyen 10 cho B"
"""

import sys
import argparse
import socket
import threading
import json
import time

from config import NODE_ADDRESSES, PUBLIC_KEYS_BYTES, MsgType
from crypto_utils import generate_keypair, sign_message_real, verify_signature_real


class ClientReplyServer:
    """
    TCP server chay tam thoi phia client de nhan CLIENT_REPLY
    da ky tu cac node trong mang.
    """

    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.replies = []
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        # Lay cong thuc te duoc cap phat ngau nhien
        self.port = self.sock.getsockname()[1]
        self.running = True
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def start(self):
        self.sock.listen(5)
        self.thread.start()

    def _serve(self):
        self.sock.settimeout(1.0)
        while self.running:
            try:
                conn, addr = self.sock.accept()
                t = threading.Thread(target=self._handle, args=(conn,), daemon=True)
                t.start()
            except socket.timeout:
                continue
            except Exception:
                break

    def _handle(self, conn):
        try:
            conn.settimeout(3.0)
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            if data:
                lines = data.decode("utf-8").strip().split("\n")
                for line in lines:
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        with self.lock:
                            self.replies.append(msg)
                    except Exception:
                        pass
        except Exception:
            pass
        finally:
            conn.close()

    def stop(self):
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="BFT Distributed Ledger Client")
    parser.add_argument("--node", type=str, default="localhost:5000", help="Node target (host:port)")
    parser.add_argument("--key", type=str, default=None, help="Private key in hex format")
    parser.add_argument("--op", type=str, required=True, help="Transaction content")
    args = parser.parse_args()

    # 1. Khoi tao cap khoa cua Client
    if args.key:
        try:
            priv_key_bytes = bytes.fromhex(args.key)
            from cryptography.hazmat.primitives.asymmetric import ed25519
            priv_key = ed25519.Ed25519PrivateKey.from_private_bytes(priv_key_bytes)
            pub_key_bytes = priv_key.public_key().public_bytes_raw()
        except Exception as e:
            print(f"[Error] Private key hex khong hop le: {e}")
            sys.exit(1)
    else:
        priv_key_bytes, pub_key_bytes = generate_keypair()

    # 2. Khoi dong server nhan phan hoi tu Node
    server = ClientReplyServer(host="127.0.0.1", port=0)
    server.start()
    print(f"[Client] Dang lang nghe reply tai {server.host}:{server.port}...")

    # 3. Tao request
    client_id = f"client_{server.port}"
    req_msg = {
        "type": int(MsgType.CLIENT_REQUEST),
        "client_id": client_id,
        "client_address": [server.host, server.port],
        "client_pubkey": pub_key_bytes.hex(),
        "operation": args.op,
        "timestamp": time.time(),
    }
    # Ky giao dich bang Private Key cua client
    sign_message_real(priv_key_bytes, req_msg)

    # 4. Gui request den node chi dinh
    try:
        target_host, target_port_str = args.node.split(":")
        target_port = int(target_port_str)
    except Exception:
        print("[Error] Cu phap node target khong dung (phai la host:port)")
        server.stop()
        sys.exit(1)

    print(f"[Client] Gui giao dich den Node {target_host}:{target_port}...")
    from network.tcp_client import send_message
    
    sent = send_message(target_host, target_port, req_msg)
    if not sent:
        print("[Error] Khong the ket noi den Node!")
        server.stop()
        sys.exit(1)

    # 5. Cho phan hoi tu cac node trong he thong
    start_time = time.time()
    success_nodes = set()
    redirected = False

    while time.time() - start_time < 12.0:
        time.sleep(0.5)
        with server.lock:
            current_replies = list(server.replies)
            server.replies.clear()

        for reply in current_replies:
            sender = reply.get("sender")
            # Xac thuc chu ky phan hoi cua Node
            from network import verify_signature
            if not verify_signature(reply):
                print(f"[Client] Canh bao: Chu ky phan hoi tu Node {sender} khong hop le!")
                continue

            result = reply.get("result")
            if result == "REDIRECT":
                if redirected:
                    continue
                leader_id = reply.get("leader")
                if leader_id in NODE_ADDRESSES:
                    new_host, new_port = NODE_ADDRESSES[leader_id]
                    print(f"[Client] Redirect: Node thong bao gui ve Leader moi (Node {leader_id} o {new_host}:{new_port}). Gui lai...")
                    send_message(new_host, new_port, req_msg)
                    redirected = True
                    start_time = time.time()  # Reset thoi gian cho
            elif result == "SUCCESS":
                success_nodes.add(sender)
                print(f"[Client] Nhan SUCCESS tu Node {sender}!")

                # So phieu phan hoi can thiet de xac nhan (f+1 = 2 phieu tu 2 node khac nhau)
                if len(success_nodes) >= 2:
                    print(f"[Client] Giao dich hoan thanh thanh cong! Nhac du f+1 reply tu cac node: {list(success_nodes)}")
                    server.stop()
                    sys.exit(0)

    print(f"[Client] Qua thoi gian cho phan hoi. So node phan hoi thanh cong: {list(success_nodes)}")
    server.stop()
    sys.exit(1)


if __name__ == "__main__":
    main()
