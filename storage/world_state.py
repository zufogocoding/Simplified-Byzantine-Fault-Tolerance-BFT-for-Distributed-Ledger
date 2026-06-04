"""
world_state.py — Quan ly so du va logic replay tu WAL/Checkpoint.
"""

import logging

logger = logging.getLogger(__name__)


class WorldState:
    """
    Quan ly trang thai so du (World State) cua he thong.
    Lien ket voi KVStore de thuc hien cac thao tac doc/ghi du lieu xuong RocksDB.
    """

    def __init__(self, store):
        self.store = store

    def get_balance(self, account: str) -> int:
        """Lay so du hien tai cua account."""
        return self.store.get_balance(account)

    def transfer(self, src: str, dst: str, amount: int) -> bool:
        """Thuc hien chuyen tien giua hai tai khoan."""
        return self.store.transfer(src, dst, amount)

    def save_checkpoint(self, seq: int, accounts: list):
        """Luu checkpoint tai sequence hien tai."""
        balances = {acc: self.get_balance(acc) for acc in accounts}
        self.store.save_checkpoint(seq, balances)
        logger.info(f"Saved world state checkpoint at seq {seq} for accounts: {accounts}")

    def load_checkpoint(self) -> tuple:
        """Tai checkpoint gan nhat. Tra ve (seq, balances)."""
        return self.store.load_checkpoint()

    def restore_and_replay(self, accounts: list):
        """Khoi phuc trang thai tu checkpoint gan nhat va replay WAL."""
        seq, balances = self.load_checkpoint()
        if seq > 0:
            logger.info(f"Restoring from checkpoint at seq {seq} with balances: {balances}")
            # Xoa toan bo state hien tai va nap state tu checkpoint
            # De don gian, ta ghi lai cac balances tu checkpoint vao DB state::
            for acc, bal in balances.items():
                self.store.put_state(acc, bal)
        else:
            logger.info("No stable checkpoint found. Initializing accounts with default balances.")
            for acc in accounts:
                self.store.put_state(acc, 100) # Mac dinh 100

        # Replay tat ca cac WAL commit sau seq
        self.store.replay_wal_from(seq)
