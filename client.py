"""
[PHASE 3] BFT Distributed Ledger — Independent Client.

Su dung:
  python client.py --node localhost:5001 --op "A chuyen 10 cho B"

Client gui request va nhan phan hoi tren CUNG mot ket noi TCP
(khong can mo server rieng de lang nghe reply).
"""

import sys
import argparse
import socket
import json
import time

from config import NODE_ADDRESSES, PUBLIC_KEYS_BYTES, MsgType
from crypto_utils import generate_keypair, sign_message_real, verify_signature_real


def send_and_wait(host: str, port: int, message: dict, timeout: float = 30.0):
    """
    Gui message JSON den node va cho phan hoi tren cung ket noi TCP.

    Args:
        host: Dia chi IP/dns cua node dich.
        port: Cong TCP cua node dich.
        message: Dict message can gui.
        timeout: Thoi gian cho phan hoi toi da (giay).

    Returns:
        dict phan hoi hoac None neu het thoi gian cho.

    Raises:
        ConnectionRefusedError: Neu khong the ket noi den node.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))

        # Gui request
        data = json.dumps(message, ensure_ascii=False) + "\n"
        sock.sendall(data.encode("utf-8"))

        # Cho phan hoi tren cung ket noi
        buffer = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buffer += chunk
            if b"\n" in buffer:
                break

        if buffer:
            line = buffer.decode("utf-8").strip().split("\n")[0]
            return json.loads(line)
        return None
    except socket.timeout:
        return None
    finally:
        try:
            sock.close()
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

    # 2. Tao request (khong can client_address vi reply tren cung ket noi)
    client_id = f"client_{int(time.time() * 1000) % 100000}"
    req_msg = {
        "type": int(MsgType.CLIENT_REQUEST),
        "client_id": client_id,
        "client_pubkey": pub_key_bytes.hex(),
        "operation": args.op,
        "timestamp": time.time(),
    }
    # Ky giao dich bang Private Key cua client
    sign_message_real(priv_key_bytes, req_msg)

    # 3. Parse node target
    try:
        target_host, target_port_str = args.node.split(":")
        target_port = int(target_port_str)
    except Exception:
        print("[Error] Cu phap node target khong dung (phai la host:port)")
        sys.exit(1)

    # 4. Gui request va cho phan hoi tren cung ket noi TCP
    max_redirects = 3
    for attempt in range(max_redirects + 1):
        print(f"[Client] Gui giao dich den Node {target_host}:{target_port}...")

        try:
            reply = send_and_wait(target_host, target_port, req_msg, timeout=30.0)
        except ConnectionRefusedError:
            print("[Error] Khong the ket noi den Node!")
            sys.exit(1)
        except Exception as e:
            print(f"[Error] Loi ket noi: {e}")
            sys.exit(1)

        if reply is None:
            print("[Client] Qua thoi gian cho phan hoi tu Node.")
            sys.exit(1)

        sender = reply.get("sender")

        # Xac thuc chu ky phan hoi cua Node
        from network import verify_signature
        if not verify_signature(reply):
            print(f"[Client] Canh bao: Chu ky phan hoi tu Node {sender} khong hop le!")
            sys.exit(1)

        result = reply.get("result")

        if result == "REDIRECT":
            leader_id = reply.get("leader")
            if leader_id in NODE_ADDRESSES:
                target_host, target_port = NODE_ADDRESSES[leader_id]
                print(f"[Client] Redirect: Gui lai den Leader Node {leader_id} tai {target_host}:{target_port}")
                continue
            else:
                print(f"[Error] Leader ID {leader_id} khong hop le")
                sys.exit(1)
        elif result == "SUCCESS":
            print(f"[Client] Nhan SUCCESS tu Node {sender}!")
            print(f"[Client] Giao dich hoan thanh thanh cong!")
            sys.exit(0)
        else:
            print(f"[Error] Phan hoi khong xac dinh: {reply}")
            sys.exit(1)

    print("[Error] Qua nhieu lan redirect")
    sys.exit(1)


if __name__ == "__main__":
    main()
