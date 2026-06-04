"""
Tests cho crypto_utils: Ed25519 key generation, signing, verification.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from crypto_utils import (
    generate_keypair,
    sign_message_real,
    verify_signature_real,
    pack_message,
)


def test_key_generation():
    """Test sinh cap khoa Ed25519."""
    priv, pub = generate_keypair()
    assert len(priv) == 32, f"Private key phai 32 bytes, duoc {len(priv)}"
    assert len(pub) == 32, f"Public key phai 32 bytes, duoc {len(pub)}"
    print("  [OK] Key generation")


def test_sign_and_verify():
    """Test ky va xac thuc message."""
    priv, pub = generate_keypair()
    msg = {"type": 1, "sender": 0, "tx_id": 1, "vote": "COMMIT"}
    signed = sign_message_real(priv, msg)
    assert "signature" in signed, "Phai co truong signature"
    assert len(signed["signature"]) == 128, "Ed25519 signature = 64 bytes = 128 hex chars"
    assert verify_signature_real(pub, signed), "Chu ky phai hop le"
    print("  [OK] Sign and verify")


def test_tampered_message():
    """Test message bi tampered phai bi phat hien."""
    priv, pub = generate_keypair()
    msg = {"type": 1, "sender": 0, "tx_id": 1, "vote": "COMMIT"}
    signed = sign_message_real(priv, msg)
    tampered = dict(signed)
    tampered["vote"] = "ABORT"
    assert not verify_signature_real(pub, tampered), "Message tampered phai bi tu choi"
    print("  [OK] Tampered detection")


def test_wrong_public_key():
    """Test verify sai public key."""
    priv0, pub0 = generate_keypair()
    _, pub1 = generate_keypair()
    msg = {"type": 1, "sender": 0, "tx_id": 1, "vote": "COMMIT"}
    signed = sign_message_real(priv0, msg)
    assert not verify_signature_real(pub1, signed), "Verify bang sai key phai fail"
    print("  [OK] Wrong public key rejection")


def test_no_signature():
    """Test message khong co signature."""
    _, pub = generate_keypair()
    assert not verify_signature_real(pub, {"no": "signature"}), "Khong co signature phai fail"
    print("  [OK] No signature rejection")


def test_empty_data():
    """Test message empty."""
    priv, pub = generate_keypair()
    signed = sign_message_real(priv, {})
    assert verify_signature_real(pub, signed), "Empty message phai verify duoc"
    print("  [OK] Empty message sign/verify")


def test_deterministic_json():
    """Test pack_message deterministic."""
    m1 = {"b": 2, "a": 1}
    m2 = {"a": 1, "b": 2}
    b1 = pack_message(m1)
    b2 = pack_message(m2)
    assert b1 == b2, "sort_keys phai dam bao deterministic"
    print("  [OK] Deterministic JSON serialization")


if __name__ == "__main__":
    print("Testing crypto_utils...")
    test_key_generation()
    test_sign_and_verify()
    test_tampered_message()
    test_wrong_public_key()
    test_no_signature()
    test_empty_data()
    test_deterministic_json()
    print()
    print("All crypto tests PASSED!")
