"""
[PHASE 0] RocksDB Store — Thread-safe WAL, state, ledger, checkpoint & replay.

Su dung rocksdict (RocksDB binding) de luu:
  - WAL entries (prefix "wal::")
  - World State balances (prefix "state::")
  - Ledger entries (prefix "ledger::")
  - Checkpoint state (key "state::checkpoint")
"""

import os
import json
import logging
import threading

logger = logging.getLogger(__name__)

_DB_INSTANCES = {}


class KVStore:
    """
    Key-Value store su dung RocksDB (rocksdict) co khoa ghi thread-safe.
    Key format:
      wal::<tx_id>::<seq>  -> WAL entry (JSON)
      state::<account>     -> balance (int as JSON)
      ledger::<tx_id>       -> transaction data (JSON)
      state::checkpoint    -> checkpoint data (JSON)
    """

    def __init__(self, sid: int, db_path: str = None):
        self.sid = sid
        if db_path is None:
            db_path = f"rocksdb/site_{sid}"
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)
        from rocksdict import Rdict
        self.db = Rdict(path=db_path)
        self.lock = threading.RLock()
        _DB_INSTANCES[sid] = self
        
        # Khoi phuc _wal_seq lon nhat tu DB
        max_seq = 0
        for k, v in self.db.items():
            if k.startswith(b"wal::"):
                try:
                    entry = json.loads(v)
                    seq = entry.get("seq", 0)
                    if seq > max_seq:
                        max_seq = seq
                except (json.JSONDecodeError, TypeError):
                    continue
        self._wal_seq = max_seq
        logger.info(f"KVStore (RocksDB) da khoi dong: {db_path} (wal_seq={self._wal_seq})")

    def close(self):
        try:
            self.db.close()
        except Exception:
            pass
        if self.sid in _DB_INSTANCES:
            del _DB_INSTANCES[self.sid]

    def put_wal(self, tx_id: int, state: str, vote: str = None, tx_data: dict = None):
        """Ghi WAL entry. Key: wal::<tx_id>::<seq>"""
        with self.lock:
            self._wal_seq += 1
            key = f"wal::{tx_id}::{self._wal_seq}".encode()
            entry = {
                "tx_id": tx_id,
                "state": state,
                "vote": vote,
                "seq": self._wal_seq,
                "tx_data": tx_data
            }
            self.db[key] = json.dumps(entry).encode()

    def read_last_wal_state(self, tx_id: int):
        """Doc trang thai va vote cuoi cung cua mot TX tu WAL."""
        with self.lock:
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
        with self.lock:
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
        with self.lock:
            key = f"state::{account}".encode()
            self.db[key] = json.dumps({"balance": balance}).encode()

    def get_balance(self, account: str) -> int:
        """Doc so du account (mac dinh 100)."""
        with self.lock:
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
        with self.lock:
            src_bal = self.get_balance(src)
            if src_bal < amount:
                return False
            dst_bal = self.get_balance(dst)
            self.put_state(src, src_bal - amount)
            self.put_state(dst, dst_bal + amount)
            return True

    def put_ledger(self, tx_id: int, tx_data: dict):
        """Ghi giao dich da duoc commit vao ledger."""
        with self.lock:
            key = f"ledger::{tx_id}".encode()
            self.db[key] = json.dumps(tx_data).encode()

    def get_ledger(self) -> list:
        """Lay tat ca giao dich tu ledger (theo thu tu tx_id)."""
        with self.lock:
            entries = []
            for k, v in self.db.items():
                if k.startswith(b"ledger::"):
                    try:
                        entries.append(json.loads(v))
                    except (json.JSONDecodeError, TypeError):
                        continue
            entries.sort(key=lambda e: e.get("tx_id", 0))
            return entries

    def save_checkpoint(self, seq: int, balances_dict: dict):
        """Luu checkpoint state: key state::checkpoint chua JSON {seq, balances}"""
        with self.lock:
            key = b"state::checkpoint"
            val = {"seq": seq, "balances": balances_dict}
            self.db[key] = json.dumps(val).encode()
            logger.info(f"Saved state checkpoint at seq {seq} for site {self.sid}")

    def load_checkpoint(self) -> tuple:
        """Nap checkpoint: tra ve (seq, balances) hoac (0, {}) neu khong co."""
        with self.lock:
            key = b"state::checkpoint"
            try:
                val = self.db[key]
                if val:
                    data = json.loads(val)
                    return data.get("seq", 0), data.get("balances", {})
            except KeyError:
                pass
            return 0, {}

    def replay_wal_from(self, seq: int):
        """Replay cac giao dich da COMMIT sau checkpoint seq."""
        with self.lock:
            committed_entries = []
            for k, v in self.db.items():
                if k.startswith(b"wal::"):
                    try:
                        entry = json.loads(v)
                        if entry.get("state") == "COMMIT" and entry.get("seq", 0) > seq:
                            committed_entries.append(entry)
                    except (json.JSONDecodeError, TypeError):
                        continue
            # Sap xep theo seq tang dan
            committed_entries.sort(key=lambda e: e.get("seq", 0))
            
            # Lay danh sach cac giao dich da co trong ledger de tranh trung lap
            existing_txs = set(t.get("tx_id") for t in self.get_ledger())

            for entry in committed_entries:
                tx_id = entry.get("tx_id")
                if tx_id in existing_txs:
                    continue

                tx = entry.get("tx_data")
                if tx and "data" in tx:
                    try:
                        parts = tx["data"].split(" ")
                        src = parts[0]
                        amount = int(parts[2])
                        dst = parts[4]
                        self.transfer(src, dst, amount)
                        self.put_ledger(tx_id, tx)
                        logger.info(f"Replayed WAL entry seq={entry.get('seq')}, tx_id={tx_id}")
                    except Exception as e:
                        logger.error(f"Error replaying WAL entry {entry}: {e}")

    def delete_old_wal(self, below_seq: int):
        """Xoa cac WAL entries cu duoi seq duoc chi dinh."""
        with self.lock:
            keys_to_delete = []
            for k, v in self.db.items():
                if k.startswith(b"wal::"):
                    try:
                        entry = json.loads(v)
                        if entry.get("seq", 0) < below_seq:
                            keys_to_delete.append(k)
                    except (json.JSONDecodeError, TypeError):
                        continue
            from rocksdict import WriteBatch
            batch = WriteBatch()
            for k in keys_to_delete:
                batch.delete(k)
            self.db.write(batch)
            logger.info(f"Deleted old WAL entries below seq {below_seq} (removed {len(keys_to_delete)} entries)")

    def put_pbft_log(self, log_type: str, seq: int, digest: str, sender: int, msg: dict):
        """Ghi PBFT log (pre_prepare, prepare, commit) vao RocksDB."""
        with self.lock:
            key = f"pbft_log::{log_type}::{seq}::{digest}::{sender}".encode()
            self.db[key] = json.dumps(msg).encode()

    def get_pbft_logs(self, log_type: str, seq: int, digest: str) -> list:
        """Doc danh sach tin nhan PBFT log cho mot (seq, digest)."""
        with self.lock:
            prefix = f"pbft_log::{log_type}::{seq}::{digest}::".encode()
            logs = []
            for k, v in self.db.items():
                if k.startswith(prefix):
                    try:
                        logs.append(json.loads(v))
                    except (json.JSONDecodeError, TypeError):
                        continue
            return logs

    def delete_old_pbft_logs(self, below_seq: int):
        """Xoa PBFT logs duoi sequence duoc chi dinh."""
        with self.lock:
            keys_to_delete = []
            for k in self.db.keys():
                if k.startswith(b"pbft_log::"):
                    try:
                        parts = k.decode("utf-8").split("::")
                        # parts = ["pbft_log", log_type, seq, digest, sender]
                        seq = int(parts[2])
                        if seq < below_seq:
                            keys_to_delete.append(k)
                    except Exception:
                        continue
            from rocksdict import WriteBatch
            batch = WriteBatch()
            for k in keys_to_delete:
                batch.delete(k)
            self.db.write(batch)
            logger.info(f"Deleted old PBFT logs below seq {below_seq} (removed {len(keys_to_delete)} entries)")

    def clear_all(self):
        """Xoa toan bo du lieu."""
        with self.lock:
            keys = list(self.db.keys())
            from rocksdict import WriteBatch
            batch = WriteBatch()
            for k in keys:
                batch.delete(k)
            self.db.write(batch)
            self._wal_seq = 0

    def save_view(self, view: int):
        """Luu view number hien tai vao RocksDB (key: state::current_view)."""
        with self.lock:
            self.db[b"state::current_view"] = json.dumps({"view": view}).encode()

    def load_view(self) -> int:
        """Doc view number da persist tu RocksDB. Tra ve 0 neu chua co."""
        with self.lock:
            try:
                val = self.db[b"state::current_view"]
                return json.loads(val).get("view", 0)
            except KeyError:
                return 0

    def save_last_seq(self, seq: int):
        """Luu last_executed_seq vao RocksDB (key: state::last_seq)."""
        with self.lock:
            self.db[b"state::last_seq"] = json.dumps({"last_seq": seq}).encode()

    def load_last_seq(self) -> int:
        """Doc last_executed_seq da persist. Tra ve 0 neu chua co."""
        with self.lock:
            try:
                val = self.db[b"state::last_seq"]
                return json.loads(val).get("last_seq", 0)
            except KeyError:
                return 0

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
