"""
World State Database giả lập để tối ưu hóa việc xác thực giao dịch và phục hồi.
Tránh việc quét toàn bộ WAL khi số lượng transaction lớn (ví dụ: 1 triệu).
"""
import os
import json

class WorldStateDB:
    def __init__(self, sid):
        self.sid = sid
        self.db_path = f"wal/site_{sid}_state.json"
        self.balances = {}
        self.last_checkpoint_tx_id = 0
        self.load_checkpoint()

    def load_checkpoint(self):
        """Khôi phục trạng thái gần nhất từ file checkpoint"""
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, "r") as f:
                    data = json.load(f)
                    self.balances = data.get("balances", {})
                    self.last_checkpoint_tx_id = data.get("last_checkpoint_tx_id", 0)
            except Exception:
                self.balances = {}
                self.last_checkpoint_tx_id = 0

    def save_checkpoint(self, last_tx_id):
        """Lưu trạng thái hiện tại xuống đĩa (Checkpointing)"""
        self.last_checkpoint_tx_id = last_tx_id
        with open(self.db_path, "w") as f:
            json.dump({
                "balances": self.balances,
                "last_checkpoint_tx_id": last_tx_id
            }, f)

    def get_balance(self, account):
        """Lấy số dư hiện tại O(1) thay vì quét log O(N)"""
        return self.balances.get(account, 100) # Mặc định cho mỗi acc 100 đồng để demo

    def transfer(self, src, dst, amount):
        """Thực hiện giao dịch chuyển tiền và cập nhật State"""
        if self.get_balance(src) >= amount:
            self.balances[src] = self.get_balance(src) - amount
            self.balances[dst] = self.get_balance(dst) + amount
            return True
        return False
