import sys
import time
import json
import socket

from config import NODE_ADDRESSES, MsgType
from crypto_utils import generate_keypair, sign_message_real
from client import send_and_wait

def run_tui():
    print("="*60)
    print(" 🚀 BFT DISTRIBUTED LEDGER - INTERACTIVE CLIENT (TUI)")
    print("="*60)

    # 1. Chon Node
    node_input = input("Nhập địa chỉ Node mục tiêu [Nhấn Enter để dùng localhost:5000]: ").strip()
    if not node_input:
        node_input = "localhost:5000"
    
    try:
        target_host, target_port_str = node_input.split(":")
        target_port = int(target_port_str)
    except Exception:
        print("\n[Lỗi] Cú pháp không đúng (yêu cầu định dạng host:port). Thoát.")
        sys.exit(1)

    # 2. Xây dựng giao dịch
    print("\n--- TẠO GIAO DỊCH CHUYỂN TIỀN ---")
    while True:
        sender = input("1. Người gửi (VD: A, B, C): ").strip().upper()
        if sender: break
        print("   -> Người gửi không được để trống!")

    while True:
        receiver = input("2. Người nhận (VD: A, B, C): ").strip().upper()
        if receiver:
            if receiver == sender:
                print("   -> Người nhận phải khác người gửi!")
                continue
            break
        print("   -> Người nhận không được để trống!")
    
    while True:
        try:
            amount_str = input("3. Số lượng tiền (VD: 10, 50): ").strip()
            amount = int(amount_str)
            if amount <= 0:
                print("   -> Số lượng phải lớn hơn 0!")
                continue
            break
        except ValueError:
            print("   -> Lỗi: Vui lòng nhập một số nguyên!")

    # Tự động sinh chuỗi đúng chuẩn
    op_string = f"{sender} chuyen {amount} cho {receiver}"
    
    print("\n" + "-"*40)
    print(f"📦 Thông tin giao dịch chuẩn bị gửi:")
    print(f"   Chuỗi lệnh: '{op_string}'")
    print("-" * 40)

    confirm = input("Bạn có muốn ký và gửi giao dịch này không? (y/n): ").strip().lower()
    if confirm != 'y':
        print("\n❌ Đã hủy giao dịch.")
        sys.exit(0)

    # 3. Ký giao dịch
    print("\n[+] Đang tạo khóa bảo mật Ed25519 và ký giao dịch...")
    priv_key_bytes, pub_key_bytes = generate_keypair()
    client_id = f"client_tui_{int(time.time() * 1000) % 100000}"
    
    req_msg = {
        "type": int(MsgType.CLIENT_REQUEST),
        "client_id": client_id,
        "client_pubkey": pub_key_bytes.hex(),
        "operation": op_string,
        "timestamp": time.time(),
    }
    sign_message_real(priv_key_bytes, req_msg)

    # 4. Gửi và xử lý Redirect
    max_redirects = 3
    for attempt in range(max_redirects + 1):
        print(f"\n[Network] Gửi tới Node {target_host}:{target_port}...")

        try:
            reply = send_and_wait(target_host, target_port, req_msg, timeout=30.0)
        except ConnectionRefusedError:
            print("[Lỗi] Không thể kết nối đến Node. Node có đang chạy không?")
            sys.exit(1)
        except Exception as e:
            print(f"[Lỗi] Ngoại lệ kết nối: {e}")
            sys.exit(1)

        if reply is None:
            print("[Lỗi] Quá thời gian chờ phản hồi từ Node.")
            sys.exit(1)

        sender_id = reply.get("sender")
        
        # Xác thực chữ ký phản hồi
        from network import verify_signature
        if not verify_signature(reply):
            print(f"[Cảnh báo] Chữ ký phản hồi từ Node {sender_id} KHÔNG HỢP LỆ! Dừng giao dịch.")
            sys.exit(1)

        result = reply.get("result")

        if result == "REDIRECT":
            leader_id = reply.get("leader")
            if leader_id in NODE_ADDRESSES:
                target_host, target_port = NODE_ADDRESSES[leader_id]
                print(f"[Redirect] Nhầm Node! Đang tự động chuyển hướng giao dịch về Leader (Node {leader_id}) tại {target_host}:{target_port}...")
                continue
            else:
                print(f"[Lỗi] Lệnh Redirect chỉ định Leader ID {leader_id} không tồn tại.")
                sys.exit(1)
                
        elif result == "SUCCESS":
            print(f"\n✅ [Thành công] Nhận phản hồi SUCCESS từ Leader (Node {sender_id})!")
            print(f"✅ [Thành công] Giao dịch '{op_string}' đã được chốt (Commit) trên sổ cái phân tán!")
            sys.exit(0)
            
        else:
            print(f"\n[Lỗi] Nhận được phản hồi không xác định: {reply}")
            sys.exit(1)

    print("\n[Lỗi] Quá nhiều lần Redirect (Vượt quá 3 lần). Có thể mạng lưới đang bất ổn.")
    sys.exit(1)

if __name__ == "__main__":
    try:
        run_tui()
    except KeyboardInterrupt:
        print("\n\n❌ Người dùng đã hủy bằng Ctrl+C.")
        sys.exit(0)
