"""
Cau hinh he thong BFT cho Distributed Ledger.

Su dung dataclass de gom tat ca tham so vao mot doi tuong co cau truc.
De thay doi cau hinh, chi can tao BFTConfig moi:
  config = BFTConfig(f=2)  -> tu dong tinh N=7, quorum=5

Su dung Enum de dam bao type-safe cho trang thai va loai message.
"""

import os
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional


# ============================================================
# ENUM — Trang thai va loai message (Type-safe)
# ============================================================
#
# Tai sao dung Enum thay vi string/int tho?
#   - Tranh loi chinh ta: TxState.INIT thay vi "INIT" (IDE bat loi ngay)
#   - Type-safe: khong the gan gia tri khong hop le
#   - Tu document: nhìn Enum la biet tat ca cac gia tri hop le
#
# Dung str/int Enum de tuong thich nguoc voi code cu:
#   Vote.COMMIT == "COMMIT"  -> True
#   MsgType.VOTE == 1        -> True
#


class TxState(str):
    """
    Trang thai cua mot giao dich trong BFT state machine.

    INIT  -> Site vua nhan giao dich, chua vote
    READY -> Site da vote va ghi WAL, san sang thu thap phieu
    """

    pass


class Vote(str):
    """
    Phieu bau va quyet dinh cuoi cung.

    COMMIT -> Chap nhan giao dich
    ABORT  -> Tu choi giao dich
    """

    pass


class MsgType(IntEnum):
    """
    Loai message trong giao thuc BFT.

    VOTE -> Phieu bau (COMMIT/ABORT) tu moi site
    REQ  -> Yeu cau phieu (khi site phuc hoi sau crash)
    RESP -> Phan hoi phieu (tra loi REQ)
    DEC  -> Thong bao quyet dinh cuoi cung
    """

    VOTE = 1
    REQ = 2
    RESP = 3
    DEC = 4


# Gia tri trang thai (backward-compatible: TxState la str subclass)
STATE_INIT = TxState("INIT")
STATE_READY = TxState("READY")

# Gia tri phieu bau (backward-compatible: Vote la str subclass)
VOTE_COMMIT = Vote("COMMIT")
VOTE_ABORT = Vote("ABORT")

# Loai message (backward-compatible: MsgType la IntEnum)
MSG_VOTE = MsgType.VOTE
MSG_REQ = MsgType.REQ
MSG_RESP = MsgType.RESP
MSG_DEC = MsgType.DEC


# ============================================================
# CAU HINH HE THONG (Dataclass)
# ============================================================
#
# Tai sao dung dataclass thay vi bien global?
#   - Gom tat ca tham so vao 1 cho, de quan ly
#   - Tinh toan tu dong: thay doi f -> N va quorum tu cap nhat
#   - De test: tao config khac nhau cho unit test
#   - Immutable: khong lo bi thay doi giua chung
#


@dataclass
class BFTConfig:
    """
    Cau hinh cho he thong BFT.

    Thay vi dung bien global roi rai, BFTConfig gom tat ca
    tham so vao mot doi tuong co cau truc.

    Vi du su dung:
        config = BFTConfig(f=1)  # N=4, quorum=3 (mac dinh)
        config = BFTConfig(f=2)  # N=7, quorum=5 (scale len)
    """

    # --- Tham so BFT ---
    f: int = 1  # So node Byzantine toi da

    # --- Tham so mang ---
    timeout: float = 4.0  # Timeout thu thap phieu (giay)
    net_min: float = 0.05  # Do tre mang toi thieu (giay)
    net_max: float = 0.2  # Do tre mang toi da (giay)
    listen_after: float = 6.0  # Thoi gian lang nghe phuc hoi (giay)

    # --- Cau hinh node ---
    malicious_sites: List[int] = field(default_factory=lambda: [0])
    # crash_config: {site_id: crash_at_tx_id}
    crash_config: Dict[int, int] = field(default_factory=lambda: {2: 1})

    # --- Reproducibility ---
    random_seed: Optional[int] = 42  # Seed de ket qua tai lap duoc

    # --- Thu muc ---
    log_dir: str = "logs"
    wal_dir: str = "wal"

    # --- Giao dich ---
    transactions: List[dict] = field(
        default_factory=lambda: [
            {"tx_id": 1, "data": "A chuyen 10 cho B"},
            {"tx_id": 2, "data": "B chuyen 5 cho C"},
        ]
    )

    @property
    def num_sites(self) -> int:
        """N = 3f + 1 (quy tac BFT)."""
        return 3 * self.f + 1

    @property
    def quorum(self) -> int:
        """Quorum = 2f + 1 (so phieu COMMIT can thiet)."""
        return 2 * self.f + 1


# ============================================================
# CAU HINH MAC DINH
# ============================================================
config = BFTConfig()

# --- Backward-compatible aliases ---
# Cac module khac import truc tiep: from config import NUM_SITES, QUORUM, ...
# Aliases nay dam bao khong can sua code cu khi chuyen sang dataclass.
F = config.f
NUM_SITES = config.num_sites
QUORUM = config.quorum
TIMEOUT = config.timeout
NET_MIN = config.net_min
NET_MAX = config.net_max
LISTEN_AFTER = config.listen_after
LOG_DIR = config.log_dir
WAL_DIR = config.wal_dir
TRANSACTIONS = config.transactions

MALICIOUS_SITE = config.malicious_sites[0]
CRASH_SITE = list(config.crash_config.keys())[0]   # 2
CRASH_ON_TX = list(config.crash_config.values())[0]  # 1

# Tao thu muc luu tru
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(WAL_DIR, exist_ok=True)
