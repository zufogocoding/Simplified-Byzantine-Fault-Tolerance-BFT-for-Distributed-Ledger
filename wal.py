"""
[FIX 5] WAL — Write-Ahead Log co cau truc (JSON Lines).

Write-Ahead Log la co che dam bao durability trong database:
  - Truoc khi thay doi state, PHAI ghi WAL truoc (write-ahead)
  - Khi crash, doc WAL de khoi phuc trang thai cuoi cung
  - Tuong tu redo log trong MySQL / PostgreSQL

Format: JSON Lines (moi dong la mot JSON object)
  {"tx_id": 1, "state": "READY", "vote": "COMMIT", "checksum": "a1b2...", ...}

Uu diem so voi parse text log:
  - Doc chinh xac sau crash (json.loads, khong regex)
  - Khong phu thuoc vao format log
  - De mo rong them truong moi

[FIX 9] Checksum SHA-256 dam bao toan ven du lieu:
  - Phat hien entry bi hong (bit rot, ghi khong hoan chinh)
  - Trong thuc te, WAL cua PostgreSQL cung dung CRC-32 cho muc dich nay
"""

import os
import json
import hashlib
from datetime import datetime

from config import WAL_DIR, VOTE_COMMIT


def _compute_checksum(entry_dict: dict) -> str:
    """
    Tinh checksum SHA-256 (16 ky tu dau) cho mot WAL entry.

    Quy trinh:
      1. Serialize entry thanh JSON (sort_keys dam bao thu tu nhat quan)
      2. Hash bang SHA-256
      3. Lay 16 ky tu hex dau tien (64 bit — du de phat hien loi)

    Tai sao sort_keys=True?
      De dam bao cung mot dict luon cho ra cung mot hash,
      bat ke thu tu key khi tao dict.
    """
    payload = json.dumps(entry_dict, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _verify_checksum(entry_dict: dict) -> bool:
    """
    Xac thuc checksum cua mot WAL entry.
    Tra ve True neu checksum hop le hoac khong co checksum (entry cu).
    """
    stored = entry_dict.pop("checksum", None)
    if stored is None:
        return True  # Entry cu khong co checksum — chap nhan
    expected = _compute_checksum(entry_dict)
    return stored == expected


class WAL:
    """
    Write-Ahead Log co cau truc, ghi trang thai va phieu bau.

    Moi site co mot file WAL rieng: wal/site_X.wal
    Moi entry ghi lai mot buoc trong state machine:
      INIT -> READY -> COMMIT/ABORT

    [FIX 9] Moi entry co checksum SHA-256 de dam bao toan ven.
    """

    def __init__(self, sid: int):
        self.sid = sid
        self.path = os.path.join(WAL_DIR, "site_%d.wal" % sid)

    def write(self, tx_id: int, state: str, vote: str = None) -> None:
        """
        Ghi mot entry vao WAL va flush xuong disk ngay lap tuc.

        Quy trinh ghi (dam bao atomicity):
          1. Tao entry dict voi tx_id, state, vote, timestamp
          2. Tinh checksum SHA-256 tu entry
          3. Ghi entry + checksum thanh 1 dong JSON
          4. flush() -> day tu Python buffer sang OS buffer
          5. fsync() -> day tu OS buffer xuong disk vat ly

        Tai sao can ca flush() VA fsync()?
          flush() chi dam bao Python gui data cho OS.
          fsync() dam bao OS ghi THUC SU xuong disk.
          Neu chi flush() ma khong fsync(), mat dien van co the mat data.
        """
        entry = {
            "tx_id": tx_id,
            "state": state,
            "vote": vote,
            "timestamp": datetime.now().isoformat(),
        }
        # [FIX 9] Tinh checksum truoc khi ghi
        entry["checksum"] = _compute_checksum(entry)

        with open(self.path, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _read_entries(self) -> list:
        """
        Doc tat ca entry hop le tu WAL.
        [FIX 9] Bo qua entry co checksum khong hop le (data bi hong).
        """
        try:
            with open(self.path) as f:
                lines = [l.strip() for l in f if l.strip()]
        except FileNotFoundError:
            return []

        entries = []
        for line in lines:
            try:
                entry = json.loads(line)
                if _verify_checksum(entry):
                    entries.append(entry)
                # Neu checksum sai -> bo qua entry bi hong
            except json.JSONDecodeError:
                continue
        return entries

    def read_last_state(self, tx_id: int):
        """
        Doc trang thai va phieu bau cuoi cung cua mot giao dich.
        Duyet toan bo WAL, lay entry cuoi cung cua tx_id do.
        """
        entries = self._read_entries()

        state, vote = None, None
        for entry in entries:
            if entry.get("tx_id") == tx_id:
                state = entry.get("state")
                if entry.get("vote") is not None:
                    vote = entry["vote"]
        return state, vote

    def get_committed_tx_ids(self) -> set:
        """Lay tap hop tx_id da COMMIT tu WAL."""
        entries = self._read_entries()
        return {
            entry["tx_id"]
            for entry in entries
            if entry.get("state") == VOTE_COMMIT
        }

    def rebuild_ledger(self, all_transactions: list) -> list:
        """
        Khoi phuc ledger tu WAL: tra ve list TX da commit theo thu tu.
        Dung khi site khoi dong lai sau crash — doc WAL de biet
        nhung TX nao da duoc dong thuan thanh cong.
        """
        committed = self.get_committed_tx_ids()
        return [tx for tx in all_transactions if tx["tx_id"] in committed]

    def clear(self) -> None:
        """Xoa file WAL."""
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass
