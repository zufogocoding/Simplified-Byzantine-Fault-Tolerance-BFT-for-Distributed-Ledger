"""
[PHASE 3] BFT Consensus Protocol — su dung RocksDB KVStore.

Module nay chua TOAN BO logic dong thuan BFT, bao gom:
  - Broadcast phieu (trung thuc va Byzantine/equivocation)
  - Thu thap phieu tu cac site qua TCP
  - Ra quyet dinh bang quorum (2f+1)
  - Xu ly giao dich (luong state machine chinh)
  - Phuc hoi sau crash bang RocksDB
"""

import os
import sys
import time
import random
import queue as queue_module
import json

from config import (
    config,
    NUM_SITES, QUORUM,
    TIMEOUT, NET_MIN, NET_MAX, LISTEN_AFTER,
    TRANSACTIONS,
    VOTE_COMMIT, VOTE_ABORT,
    STATE_INIT, STATE_READY,
    MSG_VOTE, MSG_REQ, MSG_RESP, MSG_DEC,
)
from logger import Logger
from storage.rocksdb_store import KVStore
from network import sign_message, verify_signature, net_send, net_broadcast


# ============================================================
# BROADCAST — Gui phieu den cac site
# ============================================================


def honest_broadcast(sid, tx_id, vote, log):
    """Node trung thuc: broadcast CUNG MOT phieu cho TAT CA cac site."""
    log.info("BROADCAST", "tx_id=%d | Broadcast phieu %s (NHAT QUAN)" % (tx_id, vote))
    for t in range(NUM_SITES):
        time.sleep(random.uniform(NET_MIN, NET_MAX))
        net_send(None, sid, t, MSG_VOTE, tx_id, vote=vote)
        tag = "self" if t == sid else "Site %d" % t
        log.info("SEND", "tx_id=%d | %s | to=%s" % (tx_id, vote, tag))


def byzantine_broadcast(sid, tx_id, log):
    """Node Byzantine: EQUIVOCATION — gui phieu khac nhau cho cac site."""
    log.info("BYZANTINE", "tx_id=%d | !!! BAT DAU EQUIVOCATION !!!" % tx_id)
    log.info(
        "BYZANTINE",
        "tx_id=%d | Chien luoc: COMMIT -> site chan, ABORT -> site le" % tx_id,
    )
    for t in range(NUM_SITES):
        time.sleep(random.uniform(NET_MIN, NET_MAX))
        fake_vote = VOTE_COMMIT if t % 2 == 0 else VOTE_ABORT
        net_send(None, sid, t, MSG_VOTE, tx_id, vote=fake_vote)
        tag = "self" if t == sid else "Site %d" % t
        log.info(
            "SEND", "tx_id=%d | %s | to=%s [EQUIVOCATION]" % (tx_id, fake_vote, tag)
        )
    log.info("BYZANTINE", "tx_id=%d | !!! DA GUI PHIEU MAU THUAN !!!" % tx_id)


# ============================================================
# THU THAP PHIEU — Collect votes tu cac site
# ============================================================


def collect_votes(incoming_queue, sid, tx_id, votes, my_vote, is_recovery, log):
    """Thu thap phieu tu cac site khac qua TCP."""
    start = time.time()
    while len(votes) < NUM_SITES:
        rem = TIMEOUT - (time.time() - start)
        if rem <= 0:
            log.info(
                "TIMEOUT",
                "tx_id=%d | Timeout! Co %d/%d phieu" % (tx_id, len(votes), NUM_SITES),
            )
            break
        try:
            msg = incoming_queue.get(timeout=min(0.5, rem))
        except queue_module.Empty:
            continue

        if msg.get("tx_id") != tx_id:
            continue

        if not verify_signature(msg):
            log.info(
                "SECURITY",
                "tx_id=%d | Chu ky KHONG HOP LE tu Site %d!"
                % (tx_id, msg.get("sender", -1)),
            )
            continue

        mt, s = msg["type"], msg.get("sender")

        if mt == MSG_VOTE and not is_recovery:
            v = msg.get("vote")
            if s is not None and s not in votes:
                votes[s] = v
                log.info("RECEIVED", "tx_id=%d | %s | from=Site %d" % (tx_id, v, s))
                print(
                    "  [Site %d] Nhan %s tu Site %d (%d/%d)"
                    % (sid, v, s, len(votes), NUM_SITES),
                    flush=True,
                )

        elif mt == MSG_RESP:
            v = msg.get("vote")
            if s is not None and s not in votes:
                votes[s] = v
                log.info("RECEIVED", "tx_id=%d | VOTE_RESPONSE %s | from=Site %d" % (tx_id, v, s))
                print(
                    "  [Site %d] Nhan VOTE_RESPONSE %s tu Site %d (%d/%d)"
                    % (sid, v, s, len(votes), NUM_SITES),
                    flush=True,
                )

        elif mt == MSG_REQ:
            log.info("RECEIVED", "tx_id=%d | REQUEST_VOTES | from=Site %d" % (tx_id, s))
            net_send(None, sid, s, MSG_RESP, tx_id, vote=my_vote)
            log.info("SEND", "tx_id=%d | VOTE_RESPONSE %s | to=Site %d" % (tx_id, my_vote, s))
            print(
                "  [Site %d] Gui %s cho Site %d (ho tro phuc hoi)" % (sid, my_vote, s),
                flush=True,
            )

        elif mt == MSG_DEC:
            log.info(
                "RECEIVED",
                "tx_id=%d | DECISION %s | from=Site %d"
                % (tx_id, msg.get("decision"), s),
            )

    return votes


# ============================================================
# QUYET DINH — Ra quyet dinh bang quorum BFT
# ============================================================


def make_decision(votes, tx_id, log):
    """Ra quyet dinh bang cong thuc quorum BFT: QUORUM = 2f + 1"""
    cc = sum(1 for v in votes.values() if v == VOTE_COMMIT)
    total = len(votes)
    log.info(
        "DECISION_CALC",
        "tx_id=%d | COMMIT=%d ABORT=%d (tong=%d) | Quorum can: %d"
        % (tx_id, cc, total - cc, total, QUORUM),
    )
    if total < NUM_SITES:
        log.info(
            "TIMEOUT_NOTE",
            "tx_id=%d | Chi nhan %d/%d phieu (timeout)" % (tx_id, total, NUM_SITES),
        )
    decision = VOTE_COMMIT if cc >= QUORUM else VOTE_ABORT
    log.info(
        "QUORUM_CHECK",
        "tx_id=%d | %d >= %d (2f+1)? %s => %s"
        % (tx_id, cc, QUORUM, "DAT" if cc >= QUORUM else "KHONG DAT", decision),
    )
    return decision


# ============================================================
# LANG NGHE PHUC HOI
# ============================================================


def listen_for_recovery(incoming_queue, sid, tx_id, my_vote, shutdown, log):
    """Lang nghe REQUEST_VOTES tu cac site dang phuc hoi."""
    log.info(
        "LISTENING",
        "tx_id=%d | Lang nghe yeu cau phuc hoi (%.0fs)..." % (tx_id, LISTEN_AFTER),
    )
    until = time.time() + LISTEN_AFTER
    while time.time() < until:
        if shutdown.is_set():
            break
        try:
            msg = incoming_queue.get(timeout=0.5)
        except queue_module.Empty:
            continue
        if msg.get("tx_id") != tx_id or msg.get("type") != MSG_REQ:
            continue
        if not verify_signature(msg):
            continue
        s = msg["sender"]
        net_send(None, sid, s, MSG_RESP, tx_id, vote=my_vote)
        log.info(
            "SEND",
            "tx_id=%d | VOTE_RESPONSE %s | to=Site %d (ho tro)" % (tx_id, my_vote, s),
        )
        print("  [Site %d] Ho tro: gui %s cho Site %d" % (sid, my_vote, s), flush=True)


# ============================================================
# XU LY GIAO DICH — su dung RocksDB KVStore
# ============================================================


def process_transaction(
    sid, incoming_queue, tx, is_malicious, crash_after, shutdown, log, store
):
    """
    Xu ly mot giao dich voi RocksDB KVStore.
    
    Args:
        store: storage.rocksdb_store.KVStore instance
    """
    tx_id = tx["tx_id"]
    log.info("TX_START", "tx_id=%d | === BAT DAU XU LY ===" % tx_id)
    log.info("TX_DATA", "tx_id=%d | Giao dich: %s" % (tx_id, tx["data"]))

    # --- Buoc 1: Doc RocksDB WAL de quyet dinh che do ---
    last_state, my_vote = store.read_last_wal_state(tx_id)
    is_recovery = last_state == STATE_READY
    votes = {}

    if is_recovery:
        # ===== CHE DO PHUC HOI =====
        log.info(
            "RECOVERY_START", "tx_id=%d | Phat hien READY trong WAL -> PHUC HOI" % tx_id
        )
        log.info(
            "RECOVERY_VOTE",
            "tx_id=%d | Vote cua toi (doc tu WAL): %s" % (tx_id, my_vote),
        )
        store.put_wal(tx_id, STATE_READY, my_vote)

        if not my_vote:
            log.info("RECOVERY_FAIL", "tx_id=%d | Khong tim thay phieu trong WAL!" % tx_id)
            return

        votes[sid] = my_vote
        log.info("RECEIVED", "tx_id=%d | %s | from=self (WAL)" % (tx_id, my_vote))

        log.info(
            "REQUEST_VOTES",
            "tx_id=%d | Gui yeu cau phieu den cac site con song..." % tx_id,
        )
        for t in range(NUM_SITES):
            if t != sid:
                net_send(None, sid, t, MSG_REQ, tx_id)
                log.info("SEND", "tx_id=%d | REQUEST_VOTES | to=Site %d" % (tx_id, t))
    else:
        # ===== CHE DO BINH THUONG =====
        store.put_wal(tx_id, STATE_INIT)
        log.info("STATE", "tx_id=%d -> %s" % (tx_id, STATE_INIT))

        if is_malicious:
            my_vote = VOTE_COMMIT
            byzantine_broadcast(sid, tx_id, log)
        else:
            my_vote = VOTE_COMMIT
            log.info("VOTE", "tx_id=%d | Phieu: %s (Trung thuc)" % (tx_id, my_vote))
            honest_broadcast(sid, tx_id, my_vote, log)

        store.put_wal(tx_id, STATE_READY, my_vote)
        log.info("STATE", "tx_id=%d -> %s" % (tx_id, STATE_READY))

        if crash_after:
            log.info("CRASH", "tx_id=%d | !!! Site %d CRASH sau broadcast !!!" % (tx_id, sid))
            print("*** CRASH: Site %d crashed after broadcast TX %d! ***" % (sid, tx_id), flush=True)
            sys.stdout.flush()
            os._exit(0)

    # --- Buoc 3: Thu thap phieu ---
    votes = collect_votes(incoming_queue, sid, tx_id, votes, my_vote, is_recovery, log)

    # --- Buoc 4: Ra quyet dinh ---
    decision = make_decision(votes, tx_id, log)

    store.put_wal(tx_id, decision, my_vote)
    log.info("STATE", "tx_id=%d -> %s" % (tx_id, decision))
    log.info("FINAL_DECISION", "tx_id=%d | %s" % (tx_id, decision))
    print(">>> SITE %d: TX %d = %s <<<" % (sid, tx_id, decision), flush=True)

    # --- Buoc 5: Ghi vao ledger neu COMMIT ---
    if decision == VOTE_COMMIT:
        try:
            parts = tx["data"].split(" ")
            src = parts[0]
            amount = int(parts[2])
            dst = parts[4]
            success = store.transfer(src, dst, amount)
            if success:
                store.put_ledger(tx_id, tx)
                log.info(
                    "STATE_DB_UPDATE",
                    "Chuyen %d tu %s sang %s thanh cong. Balance %s: %d, %s: %d"
                    % (amount, src, dst, src, store.get_balance(src), dst, store.get_balance(dst)),
                )
                log.info("STATE_DB_CHECKPOINT", "Da ghi TX vao RocksDB ledger")
            else:
                log.warning("STATE_DB_FAIL", "Giao dich that bai do khong du so du!")
        except Exception as e:
            log.info("STATE_DB_ERROR", "Khong the parse transaction data: %s" % str(e))
    else:
        log.info("LEDGER_SKIP", "tx_id=%d | Khong ghi vao ledger (ABORT)" % tx_id)

    net_broadcast(None, sid, MSG_DEC, tx_id, decision=decision)

    # --- Buoc 6: Lang nghe yeu cau phuc hoi ---
    listen_for_recovery(incoming_queue, sid, tx_id, my_vote, shutdown, log)

    log.info("TX_END", "tx_id=%d | === KET THUC XU LY ===" % tx_id)


# ============================================================
# SITE MAIN — Entry point cho moi tien trinh (process)
# ============================================================


def site_main(sid, qs, is_malicious, tx_list, crash_on_tx, shutdown):
    """
    [PHASE 3] Tuong thich nguoc voi main.py cu.
    Su dung RocksDB KVStore thay vi WAL + WorldStateDB.
    """
    if config.random_seed is not None:
        random.seed(config.random_seed + sid)
    else:
        random.seed(os.getpid())
    log = Logger(sid)
    store = KVStore(sid)

    role = "BYZANTINE (equivocation)" if is_malicious else "Trung thuc"
    log.info("STARTUP", "Vai tro: %s" % role)

    for tx in tx_list:
        if shutdown.is_set():
            break

        tx_id = tx["tx_id"]
        last_state, _ = store.read_last_wal_state(tx_id)
        if last_state == VOTE_COMMIT:
            log.info("SKIP", "tx_id=%d | Da COMMIT trong WAL, bo qua" % tx_id)
            continue

        crash_after = (crash_on_tx == tx_id) if crash_on_tx else False
        process_transaction(
            sid, qs[sid], tx, is_malicious, crash_after, shutdown, log, store
        )

    ledger = store.get_ledger()
    log.info("LEDGER_FINAL", "So cai: %d giao dich" % len(ledger))
    print("[Site %d] Ket thuc. Ledger co %d giao dich." % (sid, len(ledger)), flush=True)
    store.close()
