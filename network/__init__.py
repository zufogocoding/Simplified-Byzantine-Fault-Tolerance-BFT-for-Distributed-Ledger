"""
[PHASE 2] Network package — TCP communication + Ed25519 signing.
"""

import time
import random

from config import NET_MIN, NET_MAX, NUM_SITES, NODE_ADDRESSES, PUBLIC_KEYS_BYTES, PRIVATE_KEYS_BYTES
from .tcp_server import TCPServer
from .tcp_client import send_message
from crypto_utils import sign_message_real, verify_signature_real

__all__ = ["TCPServer", "send_message", "sign_message", "verify_signature",
           "net_send", "net_broadcast", "register_connection_pool", "get_connection_pool"]


def sign_message(msg, sender_id):
    """
    Ky mot message bang private key Ed25519 cua node.
    """
    private_key = PRIVATE_KEYS_BYTES[sender_id]
    return sign_message_real(private_key, msg)


def verify_signature(msg):
    """
    Xac thuc chu ky Ed25519 cua mot message.
    """
    sender_id = msg.get("sender", -1)
    if sender_id < 0 or sender_id not in PUBLIC_KEYS_BYTES:
        return False
    public_key = PUBLIC_KEYS_BYTES[sender_id]
    return verify_signature_real(public_key, msg)


_CONNECTION_POOLS = {}


def register_connection_pool(sid: int, node_addresses: dict):
    """
    Khoi tao va dang ky TCPConnectionPool cho node.
    """
    from .connection_pool import TCPConnectionPool
    pool = TCPConnectionPool(sid, node_addresses)
    _CONNECTION_POOLS[sid] = pool
    return pool


def get_connection_pool(sid: int):
    """
    Lay TCPConnectionPool cua mot node.
    """
    return _CONNECTION_POOLS.get(sid)


def net_send(src, dst, mtype, tx_id, **kw):
    if src == dst:
        return

    time.sleep(random.uniform(NET_MIN, NET_MAX))
    msg = {"type": mtype, "sender": src, "tx_id": tx_id}
    msg.update(kw)
    sign_message(msg, src)

    pool = get_connection_pool(src)
    if pool:
        success = pool.send_message(dst, msg)
    else:
        host, port = NODE_ADDRESSES[dst]
        success = send_message(host, port, msg)
    if not success:
        pass  # TODO: retry logic


def net_broadcast(src, mtype, tx_id, **kw):
    """Broadcast tin nhan qua TCP den tat ca cac node."""
    for d in range(NUM_SITES):
        net_send(src, d, mtype, tx_id, **kw)

