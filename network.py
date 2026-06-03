"""
[FIX 6] MANG — Truyen thong voi chu ky so gia lap.

Cung cap cac ham gui/nhan tin nhan giua cac site,
voi co che chu ky so de chong gia mao sender.

Trong he thong BFT thuc te:
  - Moi node co cap khoa (public key, private key)
  - Khi gui message, node ky bang private key
  - Khi nhan message, node xac thuc bang public key cua sender
  - Neu khong co chu ky, node Byzantine co the gia mao bat ky node nao

Trong mo phong nay, chu ky duoc gia lap bang chuoi "SIG_SITE_X".
"""

import time
import random

from config import NET_MIN, NET_MAX, NUM_SITES


def sign_message(msg, sender_id):
    """
    Gia lap chu ky so cho message.
    Trong he thong BFT thuc te, moi message phai duoc ky bang
    private key (RSA/ECDSA) de node nhan co the xac thuc nguon goc.
    Neu khong co chu ky, node Byzantine co the gia mao sender.
    """
    msg["signature"] = "SIG_SITE_%d" % sender_id
    return msg


def verify_signature(msg):
    """
    Gia lap xac thuc chu ky.
    Trong thuc te: verify bang public key cua sender.
    Neu chu ky khong khop -> bo qua message (co the bi gia mao).
    """
    expected = "SIG_SITE_%d" % msg.get("sender", -1)
    return msg.get("signature") == expected


def net_send(qs, src, dst, mtype, tx_id, **kw):
    """
    Gui tin nhan co tre mang ngau nhien va chu ky so.
    Do tre ngau nhien (NET_MIN ~ NET_MAX) mo phong mang phan tan thuc te
    noi moi message co latency khac nhau.
    """
    time.sleep(random.uniform(NET_MIN, NET_MAX))
    msg = {"type": mtype, "sender": src, "tx_id": tx_id}
    msg.update(kw)
    sign_message(msg, src)
    qs[dst].put(msg)


def net_broadcast(qs, src, mtype, tx_id, **kw):
    """Broadcast tin nhan den tat ca cac site."""
    for d in range(NUM_SITES):
        net_send(qs, src, d, mtype, tx_id, **kw)
