"""
[PHASE 3] RocksDB Store — Thay the WAL JSON file + WorldStateDB checkpoint.

Su dung rocksdict (RocksDB binding) de luu:
  - WAL entries (prefix "wal::")
  - World State balances (prefix "state::")
  - Ledger entries (prefix "ledger::")
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

_DB_INSTANCES = {}


class KVStore:
    """
    Key-Value store su dung RocksDB (rocksdict).
    Key format:
      wal::<tx_id>::<seq>  -> WAL entry (JSON)
      state::<account>     -> balance (int as JSON)
      ledger::<tx_id>       -> transaction data (JSON)
    """

    def __init__(self, sid: int, db_path: str = None):
        self.sid = sid
        if db_path is None:
            db_path = f"rocksdb/site_{sid}"
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)
        from rocksdict import Rdict
        self.db = Rdict(path=db_path)
        _DB_INSTANCES[sid] = self
        self._wal_seq = 0
        logger.info(f"KVStore (RocksDB) da khoi dong: {db_path}")

    def close(self):
        try:
            self.db.close()
        except Exception:
            pass
        if self.sid in _DB_INSTANCES:
            del _DB_INSTANCES[self.sid]

    def put_wal(self, tx_id: int, state: str, vote: str = None):
        """Ghi WAL entry. Key: wal::<tx_id>::<seq>"""
        self._wal_seq += 1
        key = f"wal::{tx_id}::{self._wal_seq}".encode()
        entry = {"tx_id": tx_id, "state": state, "vote": vote, "seq": self._wal_seq}
        self.db[key] = json.dumps(entry).encode()

    def read_last_wal_state(self, tx_id: int):
        """Doc trang thai va vote cuoi cung cua mot TX tu WAL."""
        prefix = f"wal::{tx_id}::".encode()
        last_state = None
        last_vote = None
        last_seq = -1
        for k, v in self.db.items():
            if k.startswith(prefix):
                try:
                    entry = json.loads(v)
                    seq = entry.get("seq", 0)
                    if seq > last_seq:
                        last_seq = seq
                        last_state = entry.get("state")
                        last_vote = entry.get("vote")
                except (json.JSONDecodeError, TypeError):
                    continue
        return last_state, last_vote

    def get_committed_tx_ids(self) -> set:
        """Lay tap hop tx_id da COMMIT."""
        committed = set()
        for k, v in self.db.items():
            if k.startswith(b"wal::"):
                try:
                    entry = json.loads(v)
                    if entry.get("state") == "COMMIT":
                        tx_id = entry.get("tx_id")
                        if tx_id is not None:
                            committed.add(tx_id)
                except (json.JSONDecodeError, TypeError):
                    continue
        return committed

    def put_state(self, account: str, balance: int):
        """Ghi so du account."""
        key = f"state::{account}".encode()
        self.db[key] = json.dumps({"balance": balance}).encode()

    def get_balance(self, account: str) -> int:
        """Doc so du account (mac dinh 100)."""
        key = f"state::{account}".encode()
        try:
            val = self.db[key]
            if val:
                data = json.loads(val)
                return data.get("balance", 100)
        except KeyError:
            pass
        return 100

    def transfer(self, src: str, dst: str, amount: int) -> bool:
        """Chuyen tien giua 2 account."""
        src_bal = self.get_balance(src)
        if src_bal < amount:
            return False
        dst_bal = self.get_balance(dst)
        self.put_state(src, src_bal - amount)
        self.put_state(dst, dst_bal + amount)
        return True

    def put_ledger(self, tx_id: int, tx_data: dict):
        """Ghi giao dich da duoc commit vao ledger."""
        key = f"ledger::{tx_id}".encode()
        self.db[key] = json.dumps(tx_data).encode()

    def get_ledger(self) -> list:
        """Lay tat ca giao dich tu ledger (theo thu tu tx_id)."""
        entries = []
        for k, v in self.db.items():
            if k.startswith(b"ledger::"):
                try:
                    entries.append(json.loads(v))
                except (json.JSONDecodeError, TypeError):
                    continue
        entries.sort(key=lambda e: e.get("tx_id", 0))
        return entries

    def clear_all(self):
        """Xoa toan bo du lieu."""
        keys = list(self.db.keys())
        with self.db.write_batch() as batch:
            for k in keys:
                batch.delete(k)

    def create_checkpoint(self, checkpoint_dir: str):
        """Tao RocksDB checkpoint."""
        try:
            from rocksdict import Checkpoint
            checkpoint = Checkpoint(self.db)
            checkpoint.create_checkpoint(checkpoint_dir)
            logger.info(f"Checkpoint created: {checkpoint_dir}")
            return True
        except Exception as e:
            logger.error(f"Checkpoint failed: {e}")
            return False


def cleanup_all():
    for sid, store in list(_DB_INSTANCES.items()):
        store.close()


def delete_db(sid: int):
    import shutil
    db_path = f"rocksdb/site_{sid}"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
