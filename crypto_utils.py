"""
[PHASE 2] Crypto Utilities — Chu ky so Ed25519 thuc.

Cung cap cac ham:
  - generate_keypair() -> (private_key, public_key)
  - sign(private_key, message_bytes) -> signature_hex
  - verify(public_key_bytes, message_bytes, signature_hex) -> bool
  - pack_message(msg_dict) -> bytes (serialized JSON)

Su dung thu vien cryptography (Ed25519) thay vi chu ky gia lap.
"""

import json
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PrivateFormat,
    PublicFormat,
    NoEncryption,
    load_der_private_key,
    load_der_public_key,
)


# ============================================================
# SINH KHOA
# ============================================================


def generate_keypair():
    """
    Sinh cap khoa Ed25519.

    Returns:
        (private_key_bytes, public_key_bytes)
    """
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    private_bytes = private_key.private_bytes(
        encoding=Encoding.Raw,
        format=PrivateFormat.Raw,
        encryption_algorithm=NoEncryption(),
    )
    public_bytes = public_key.public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    return private_bytes, public_bytes


def private_key_to_bytes(private_key):
    """Serialize private key object to bytes."""
    return private_key.private_bytes(
        encoding=Encoding.Raw,
        format=PrivateFormat.Raw,
        encryption_algorithm=NoEncryption(),
    )


def public_key_to_bytes(public_key):
    """Serialize public key object to bytes."""
    return public_key.public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )


def load_private_key(private_bytes):
    """Load private key tu bytes."""
    return ed25519.Ed25519PrivateKey.from_private_bytes(private_bytes)


def load_public_key(public_bytes):
    """Load public key tu bytes."""
    return ed25519.Ed25519PublicKey.from_public_bytes(public_bytes)


# ============================================================
# KY & XAC THUC
# ============================================================


def pack_message(msg_dict: dict) -> bytes:
    """
    Serialize message dict thanh bytes de ky.
    Su dung sort_keys=True de dam bao deterministic.
    """
    return json.dumps(msg_dict, sort_keys=True, ensure_ascii=False).encode("utf-8")


def sign_message_real(private_key_bytes: bytes, message: dict) -> dict:
    """
    Ky mot message bang private key Ed25519.
    Them truong 'signature' (hex) vao message.

    Quy trinh:
      1. Tao ban copy message (bo truong signature neu co)
      2. Serialize bang pack_message
      3. Ky bang private key Ed25519
      4. Them signature (hex) vao message
    """
    # Loai bo signature cu (neu co) truoc khi ky
    msg_copy = {k: v for k, v in message.items() if k != "signature"}
    msg_bytes = pack_message(msg_copy)

    private_key = load_private_key(private_key_bytes)
    signature = private_key.sign(msg_bytes)

    message["signature"] = signature.hex()
    return message


def verify_signature_real(public_key_bytes: bytes, message: dict) -> bool:
    """
    Xac thuc chu ky cua mot message.

    Args:
        public_key_bytes: Public key cua sender (bytes)
        message: Dict message co chua 'signature'

    Returns:
        True neu chu ky hop le, False neu khong
    """
    signature_hex = message.get("signature")
    if not signature_hex:
        return False

    try:
        signature = bytes.fromhex(signature_hex)
    except ValueError:
        return False

    # Loai bo signature truoc khi xac thuc
    msg_copy = {k: v for k, v in message.items() if k != "signature"}
    msg_bytes = pack_message(msg_copy)

    try:
        public_key = load_public_key(public_key_bytes)
        public_key.verify(signature, msg_bytes)
        return True
    except Exception:
        return False


# ============================================================
# SINH KHOA CO DINH CHO HE THONG (dung de debug)
# ============================================================


def generate_deterministic_keypair(seed: bytes) -> tuple:
    """
    Sinh cap khoa tu seed (32 bytes) de ket qua tai lap duoc.
    Dung cho debug/test.
    """
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
    public_key = private_key.public_key()

    private_bytes = private_key.private_bytes(
        encoding=Encoding.Raw,
        format=PrivateFormat.Raw,
        encryption_algorithm=NoEncryption(),
    )
    public_bytes = public_key.public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    return private_bytes, public_bytes


# ============================================================
# HASHING HELPERS
# ============================================================


import hashlib


def compute_digest(request_data: bytes) -> str:
    """
    Tinh digest SHA-256 hex tu bytes.
    """
    return hashlib.sha256(request_data).hexdigest()


def hash_message(msg_dict: dict) -> str:
    """
    Tinh digest SHA-256 hex tu message dict (khong kem signature).
    """
    msg_copy = {k: v for k, v in msg_dict.items() if k != "signature"}
    return compute_digest(pack_message(msg_copy))

