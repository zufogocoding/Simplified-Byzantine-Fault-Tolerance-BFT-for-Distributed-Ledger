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
from typing import Dict, List, Optional, Tuple


# ============================================================
# ENUM — Trang thai va loai message (Type-safe)
# ============================================================


class TxState(str):
    """Trang thai cua mot giao dich trong BFT state machine."""
    pass


class Vote(str):
    """Phieu bau va quyet dinh cuoi cung."""
    pass


class MsgType(IntEnum):
    """Loai message trong giao thuc BFT."""
    # Original message types (Phase 1-3)
    VOTE = 1
    REQ = 2
    RESP = 3
    DEC = 4
    # PBFT message types (Phase 4)
    PRE_PREPARE = 101
    PREPARE = 102
    COMMIT = 103
    CHECKPOINT = 104
    VIEW_CHANGE = 105
    NEW_VIEW = 106
    CLIENT_REQUEST = 107
    CLIENT_REPLY = 108
    PING = 109
    PONG = 110
    SYNC_REQUEST = 111
    SYNC_RESPONSE = 112


# Gia tri trang thai (backward-compatible)
STATE_INIT = TxState("INIT")
STATE_READY = TxState("READY")

# Gia tri phieu bau (backward-compatible)
VOTE_COMMIT = Vote("COMMIT")
VOTE_ABORT = Vote("ABORT")

# Loai message (backward-compatible)
MSG_VOTE = MsgType.VOTE
MSG_REQ = MsgType.REQ
MSG_RESP = MsgType.RESP
MSG_DEC = MsgType.DEC
# PBFT aliases
MSG_PRE_PREPARE = MsgType.PRE_PREPARE
MSG_PREPARE = MsgType.PREPARE
MSG_COMMIT = MsgType.COMMIT
MSG_CHECKPOINT = MsgType.CHECKPOINT
MSG_VIEW_CHANGE = MsgType.VIEW_CHANGE
MSG_NEW_VIEW = MsgType.NEW_VIEW
MSG_CLIENT_REQUEST = MsgType.CLIENT_REQUEST
MSG_CLIENT_REPLY = MsgType.CLIENT_REPLY
MSG_PING = MsgType.PING
MSG_PONG = MsgType.PONG
MSG_SYNC_REQUEST = MsgType.SYNC_REQUEST
MSG_SYNC_RESPONSE = MsgType.SYNC_RESPONSE


# ============================================================
# CAU HINH HE THONG (Dataclass)
# ============================================================


@dataclass
class BFTConfig:
    """
    Cau hinh cho he thong BFT.

    Vi du su dung:
        config = BFTConfig(f=1)  # N=4, quorum=3 (mac dinh)
        config = BFTConfig(f=2)  # N=7, quorum=5 (scale len)
    """

    # --- Tham so BFT ---
    f: int = 1  # So node Byzantine toi da

    # --- Tham so mang TCP ---
    node_addresses: Dict[int, Tuple[str, int]] = field(
        default_factory=lambda: {
            0: ("node0" if os.environ.get("BFT_DOCKER") else "localhost", 5000),
            1: ("node1" if os.environ.get("BFT_DOCKER") else "localhost", 5001),
            2: ("node2" if os.environ.get("BFT_DOCKER") else "localhost", 5002),
            3: ("node3" if os.environ.get("BFT_DOCKER") else "localhost", 5003),
        }
    )
    timeout: float = 4.0  # Timeout thu thap phieu (giay)
    net_min: float = 0.05  # Do tre mang toi thieu (giay)
    net_max: float = 0.2  # Do tre mang toi da (giay)
    listen_after: float = 6.0  # Thoi gian lang nghe phuc hoi (giay)

    # --- Khoa Ed25519 (deterministic) ---
    private_keys: Dict[int, str] = field(
        default_factory=lambda: {
            0: "2a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40414243444546474849",
            1: "2a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40414243444546474848",
            2: "2a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4041424344454647484b",
            3: "2a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4041424344454647484a",
        }
    )
    public_keys: Dict[int, str] = field(
        default_factory=lambda: {
            0: "789d6666a79869029eefbf5c80cac50f9b20505619cd3a0a9cd144a95038dfef",
            1: "7b326aaad6d25bd7304edae7069b14770a0ae4d79302be842492791d843e7e07",
            2: "963e34ab95ff76c3b0eb76775b2b8ce76c5ad7e6667b642f3d12ea5901d5d4d7",
            3: "aa2e48367686583929edbc1387375b309d42d619a1fb06a7dce66eeddeb0e0b9",
        }
    )

    # --- Cau hinh node ---
    malicious_sites: List[int] = field(default_factory=lambda: [0])
    crash_config: Dict[int, int] = field(default_factory=lambda: {2: 1})

    # --- Reproducibility ---
    random_seed: Optional[int] = 42

    # --- Thu muc ---
    log_dir: str = "logs"
    wal_dir: str = "wal"

    # --- Giao dich ---
    transactions: List[dict] = field(
        default_factory=lambda: [
            {"tx_id": 1, "data": "A chuyen 10 cho B"},
            {"tx_id": 2, "data": "B chuyen 5 cho C"},
            {"tx_id": 3, "data": "C chuyen 3 cho D"},
            {"tx_id": 4, "data": "A chuyen 7 cho D"},
            {"tx_id": 5, "data": "B chuyen 2 cho A"},
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
NODE_ADDRESSES = config.node_addresses
PRIVATE_KEYS = config.private_keys
PUBLIC_KEYS = config.public_keys

PRIVATE_KEYS_BYTES = {
    sid: bytes.fromhex(hex_str) for sid, hex_str in config.private_keys.items()
}
PUBLIC_KEYS_BYTES = {
    sid: bytes.fromhex(hex_str) for sid, hex_str in config.public_keys.items()
}

MALICIOUS_SITE = config.malicious_sites[0]
CRASH_SITE = list(config.crash_config.keys())[0]   # 2
CRASH_ON_TX = list(config.crash_config.values())[0]  # 1

# Tao thu muc luu tru
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(WAL_DIR, exist_ok=True)
