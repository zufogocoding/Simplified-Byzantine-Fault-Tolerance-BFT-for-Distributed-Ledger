#!/usr/bin/env python3
import os
import sys
import json
from rocksdict import Rdict, AccessType

def check_node(sid: int):
    db_path = f"rocksdb/site_{sid}"
    if not os.path.exists(db_path):
        print(f"[-] Node {sid}: Thư mục RocksDB không tồn tại: {db_path}")
        return False

    print(f"\n==================================================")
    print(f"  [NODE {sid}] Ledger & Account Balances ({db_path})")
    print(f"==================================================")

    try:
        # Mở database dưới dạng read-only để tránh xung đột khóa (lock) với container đang chạy
        db = Rdict(db_path, access_type=AccessType.read_only())
    except Exception as e:
        print(f"[-] Node {sid}: Không thể mở database (có thể do database chưa khởi tạo hoặc bị khóa): {e}")
        return False

    try:
        # 1. Đọc Sổ cái Ledger
        ledger_entries = []
        # 2. Đọc Số dư State
        balances = {}
        # 3. Đọc Checkpoint
        checkpoint = None

        for k, v in db.items():
            if k.startswith(b"ledger::"):
                try:
                    ledger_entries.append(json.loads(v))
                except Exception:
                    pass
            elif k.startswith(b"state::"):
                if k == b"state::checkpoint":
                    try:
                        checkpoint = json.loads(v)
                    except Exception:
                        pass
                elif k in (b"state::current_view", b"state::last_seq"):
                    pass  # Bo qua system keys
                else:
                    account = k[7:].decode("utf-8", errors="replace")
                    try:
                        balances[account] = json.loads(v).get("balance", 100)
                    except Exception:
                        balances[account] = 100

        # Sắp xếp ledger theo tx_id tăng dần
        ledger_entries.sort(key=lambda e: e.get("tx_id", 0))

        print("\n--- SỔ CÁI LEDGER ---")
        if not ledger_entries:
            print("  (Sổ cái trống)")
        for tx in ledger_entries:
            tx_id = tx.get("tx_id", "N/A")
            data = tx.get("data", "N/A")
            print(f"  TX {tx_id}: {data}")

        print("\n--- SỐ DƯ TÀI KHOẢN ---")
        # Đảm bảo in các tài khoản mặc định (A, B, C, D) ngay cả khi chưa có giao dịch
        for acc in ["A", "B", "C", "D"]:
            if acc not in balances:
                balances[acc] = 100
        
        for acc in sorted(balances.keys()):
            print(f"  Tài khoản {acc}: {balances[acc]}")

        if checkpoint:
            print("\n--- CHECKPOINT GẦN NHẤT ---")
            print(f"  Seq: {checkpoint.get('seq', 0)}")
            print(f"  Balances: {checkpoint.get('balances', {})}")

    except Exception as e:
        print(f"[-] Node {sid}: Xảy ra lỗi khi đọc dữ liệu: {e}")
    finally:
        db.close()
    return True

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.lower() in ["all", "--all", "-a"]:
            nodes = [0, 1, 2, 3]
        else:
            try:
                nodes = [int(arg)]
            except ValueError:
                print("Lỗi: Đối số phải là ID của node (0, 1, 2, 3) hoặc 'all'")
                sys.exit(1)
    else:
        # Mặc định: kiểm tra tất cả các node đang có dữ liệu
        nodes = []
        for i in range(4):
            if os.path.exists(f"rocksdb/site_{i}"):
                nodes.append(i)
        if not nodes:
            print("[-] Không tìm thấy thư mục rocksdb/site_* nào.")
            sys.exit(1)

    for sid in nodes:
        check_node(sid)
    print("\n==================================================")

if __name__ == "__main__":
    main()
