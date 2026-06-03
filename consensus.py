"""
Simplified BFT Consensus Protocol — State Machine.

Module nay chua TOAN BO logic dong thuan BFT, bao gom:
  - Broadcast phieu (trung thuc va Byzantine/equivocation)
  - Thu thap phieu tu cac site
  - Ra quyet dinh bang quorum (2f+1)
  - Xu ly giao dich (luong state machine chinh)
  - Phuc hoi sau crash bang WAL

============================================================
STATE MACHINE cua moi giao dich tai moi site:
============================================================

    +------+   vote + broadcast   +-------+   collect + decide   +--------+
    | INIT | ------------------> | READY | ------------------> | COMMIT |
    +------+                     +-------+                     +--------+
                                    |                              |
                                    |  crash?                      |
                                    v                              |
                                +-----------+   req_votes +        |
                                | RECOVERY  | ----------> +--------+
                                +-----------+             | ABORT  |
                                                          +--------+

  - INIT:     Site vua nhan giao dich, chua vote
  - READY:    Site da vote va ghi WAL, dang thu thap phieu
  - COMMIT:   Du quorum (>= 2f+1 phieu COMMIT) -> chap nhan TX
  - ABORT:    Khong du quorum -> tu choi TX
  - RECOVERY: Site crash o READY, khoi dong lai, doc WAL, hoi lai phieu

Tai sao can BFT ma khong dung Paxos?
  Paxos chi xu ly crash fault (node ngung hoat dong).
  BFT xu ly Byzantine fault (node gui thong tin MAU THUAN — equivocation).
  Site 0 gui COMMIT cho mot so site, ABORT cho site khac.
  => Paxos KHONG chong duoc hanh vi nay!
"""

import os
import sys
import time
import random
import queue
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
from wal import WAL
from network import sign_message, verify_signature, net_send, net_broadcast


# ============================================================
# BROADCAST — Gui phieu den cac site
# ============================================================


def honest_broadcast(qs, sid, tx_id, vote, log):
    """
    Node trung thuc: broadcast CUNG MOT phieu cho TAT CA cac site.
    Day la hanh vi dung dan — moi node nhan duoc cung mot thong tin.
    """
    log.info("BROADCAST", "tx_id=%d | Broadcast phieu %s (NHAT QUAN)" % (tx_id, vote))
    for t in range(NUM_SITES):
        time.sleep(random.uniform(NET_MIN, NET_MAX))
        msg = {"type": MSG_VOTE, "sender": sid, "tx_id": tx_id, "vote": vote}
        sign_message(msg, sid)
        qs[t].put(msg)
        tag = "self" if t == sid else "Site %d" % t
        log.info("SEND", "tx_id=%d | %s | to=%s" % (tx_id, vote, tag))


def byzantine_broadcast(qs, sid, tx_id, log):
    """
    [FIX 1] Node Byzantine thuc hien EQUIVOCATION:
    - Gui COMMIT cho site co id CHAN (0, 2)
    - Gui ABORT  cho site co id LE  (1, 3)

    Day la BAN CHAT cua loi Byzantine: node doc hai gui thong tin
    MAU THUAN cho cac node khac nhau, co gang pha vo dong thuan.

    Tai sao Paxos khong xu ly duoc?
      Paxos GIA DINH moi node hoac trung thuc hoac crash.
      Neu mot node gui COMMIT cho A nhung ABORT cho B,
      Paxos khong co co che phat hien va xu ly.
      BFT (3f+1) dam bao: du co f node lam dieu nay,
      2f+1 node trung thuc van thong nhat quyet dinh.
    """
    log.info("BYZANTINE", "tx_id=%d | !!! BAT DAU EQUIVOCATION !!!" % tx_id)
    log.info(
        "BYZANTINE",
        "tx_id=%d | Chien luoc: COMMIT -> site chan, ABORT -> site le" % tx_id,
    )

    for t in range(NUM_SITES):
        time.sleep(random.uniform(NET_MIN, NET_MAX))
        # Equivocation: phieu khac nhau cho site khac nhau
        fake_vote = VOTE_COMMIT if t % 2 == 0 else VOTE_ABORT
        msg = {"type": MSG_VOTE, "sender": sid, "tx_id": tx_id, "vote": fake_vote}
        sign_message(msg, sid)
        qs[t].put(msg)
        tag = "self" if t == sid else "Site %d" % t
        log.info(
            "SEND", "tx_id=%d | %s | to=%s [EQUIVOCATION]" % (tx_id, fake_vote, tag)
        )

    log.info("BYZANTINE", "tx_id=%d | !!! DA GUI PHIEU MAU THUAN !!!" % tx_id)


# ============================================================
# THU THAP PHIEU — Collect votes tu cac site
# ============================================================


def collect_votes(qs, sid, tx_id, votes, my_vote, is_recovery, log):
    """
    Thu thap phieu tu cac site khac.
    [FIX 6] Xac thuc chu ky truoc khi chap nhan.
    [FIX 8] Bat queue.Empty cu the thay vi Exception.

    Xu ly 4 loai message:
      MSG_VOTE -> Phieu bau binh thuong
      MSG_RESP -> Phan hoi khi site khac ho tro phuc hoi
      MSG_REQ  -> Yeu cau phieu tu site dang phuc hoi (tra loi ngay)
      MSG_DEC  -> Thong bao quyet dinh (ghi log)
    """
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
            msg = qs[sid].get(timeout=min(0.5, rem))
        except queue.Empty:
            # [FIX 8] Bat cu the queue.Empty thay vi Exception chung
            continue

        if msg.get("tx_id") != tx_id:
            continue

        # [FIX 6] Xac thuc chu ky
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
                log.info(
                    "RECEIVED",
                    "tx_id=%d | VOTE_RESPONSE %s | from=Site %d" % (tx_id, v, s),
                )
                print(
                    "  [Site %d] Nhan VOTE_RESPONSE %s tu Site %d (%d/%d)"
                    % (sid, v, s, len(votes), NUM_SITES),
                    flush=True,
                )

        elif mt == MSG_REQ:
            log.info("RECEIVED", "tx_id=%d | REQUEST_VOTES | from=Site %d" % (tx_id, s))
            net_send(qs, sid, s, MSG_RESP, tx_id, vote=my_vote)
            log.info(
                "SEND", "tx_id=%d | VOTE_RESPONSE %s | to=Site %d" % (tx_id, my_vote, s)
            )
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
    """
    [FIX 2] Ra quyet dinh bang cong thuc quorum BFT:
      QUORUM = 2f + 1 (voi f=1 -> QUORUM=3)
    Thay vi hardcode 'cc >= 3', su dung bien QUORUM
    de code tong quat cho moi gia tri f.

    Cong thuc: COMMIT neu so phieu COMMIT >= QUORUM, nguoc lai ABORT.
    """
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

    # [FIX 2] Dung cong thuc: can >= QUORUM phieu COMMIT
    decision = VOTE_COMMIT if cc >= QUORUM else VOTE_ABORT
    log.info(
        "QUORUM_CHECK",
        "tx_id=%d | %d >= %d (2f+1)? %s => %s"
        % (tx_id, cc, QUORUM, "DAT" if cc >= QUORUM else "KHONG DAT", decision),
    )
    return decision


# ============================================================
# LANG NGHE PHUC HOI — Ho tro site crash khoi phuc dong thuan
# ============================================================


def listen_for_recovery(qs, sid, tx_id, my_vote, shutdown, log):
    """
    Lang nghe REQUEST_VOTES tu cac site dang phuc hoi.
    Gui lai phieu cua minh de ho tro site khoi phuc dong thuan.

    Khi site 2 crash va khoi dong lai, no gui MSG_REQ den tat ca.
    Cac site con song nhan MSG_REQ va gui lai phieu (MSG_RESP).
    Nho do site 2 co the thu thap du quorum va ra quyet dinh.
    """
    log.info(
        "LISTENING",
        "tx_id=%d | Lang nghe yeu cau phuc hoi (%.0fs)..." % (tx_id, LISTEN_AFTER),
    )
    until = time.time() + LISTEN_AFTER
    while time.time() < until:
        if shutdown.is_set():
            break
        try:
            msg = qs[sid].get(timeout=0.5)
        except queue.Empty:
            # [FIX 8] Bat cu the queue.Empty
            continue
        if msg.get("tx_id") != tx_id or msg.get("type") != MSG_REQ:
            continue
        if not verify_signature(msg):
            continue
        s = msg["sender"]
        net_send(qs, sid, s, MSG_RESP, tx_id, vote=my_vote)
        log.info(
            "SEND",
            "tx_id=%d | VOTE_RESPONSE %s | to=Site %d (ho tro)" % (tx_id, my_vote, s),
        )
        print("  [Site %d] Ho tro: gui %s cho Site %d" % (sid, my_vote, s), flush=True)


# ============================================================
# XU LY GIAO DICH — Logic BFT State Machine cho mot giao dich
# ============================================================
#
#  Luong xu ly (state transitions):
#
#   [Binh thuong]                      [Phuc hoi (crash recovery)]
#   INIT                               Doc WAL -> READY
#     |-- vote (COMMIT)                   |-- REQUEST_VOTES -> cac site
#     |-- broadcast (honest/byzantine)    |-- collect votes (MSG_RESP)
#     v                                   v
#   READY                              Thu thap phieu
#     |-- ghi WAL                         |
#     |-- crash? -> os._exit(0)           |
#     |-- collect votes                   |
#     v                                   v
#   make_decision                      make_decision
#     |-- COMMIT (>= 2f+1)               |-- COMMIT hoac ABORT
#     |-- ABORT  (< 2f+1)                |
#     v                                   v
#   Ghi ledger + broadcast decision    Ghi ledger + listen
#


def process_transaction(
    sid, qs, tx, is_malicious, crash_after, shutdown, log, wal, ledger
):
    """
    [FIX 7] Xu ly mot giao dich — da tach thanh cac ham con.
    Luong xu ly:
      1. Doc WAL -> binh thuong hoac phuc hoi
      2. Vote + broadcast (trung thuc hoac equivocation)
      3. Thu thap phieu
      4. Ra quyet dinh (quorum 2f+1)
      5. [FIX 3] Ghi vao ledger neu COMMIT
      6. Lang nghe ho tro phuc hoi
    """
    tx_id = tx["tx_id"]
    log.info("TX_START", "tx_id=%d | === BAT DAU XU LY ===" % tx_id)
    log.info("TX_DATA", "tx_id=%d | Giao dich: %s" % (tx_id, tx["data"]))

    # --- Buoc 1: Doc WAL de quyet dinh che do ---
    last_state, my_vote = wal.read_last_state(tx_id)
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
        wal.write(tx_id, STATE_READY, my_vote)

        if not my_vote:
            log.info(
                "RECOVERY_FAIL", "tx_id=%d | Khong tim thay phieu trong WAL!" % tx_id
            )
            return

        votes[sid] = my_vote
        log.info("RECEIVED", "tx_id=%d | %s | from=self (WAL)" % (tx_id, my_vote))

        # Gui REQUEST_VOTES den tat ca site khac
        log.info(
            "REQUEST_VOTES",
            "tx_id=%d | Gui yeu cau phieu den cac site con song..." % tx_id,
        )
        for t in range(NUM_SITES):
            if t != sid:
                net_send(qs, sid, t, MSG_REQ, tx_id)
                log.info("SEND", "tx_id=%d | REQUEST_VOTES | to=Site %d" % (tx_id, t))
    else:
        # ===== CHE DO BINH THUONG =====
        wal.write(tx_id, STATE_INIT)
        log.info("STATE", "tx_id=%d -> %s" % (tx_id, STATE_INIT))

        # Buoc 2: Vote va broadcast
        if is_malicious:
            # [FIX 1] Node Byzantine: EQUIVOCATION
            my_vote = VOTE_COMMIT  # Vote "noi bo" ghi WAL
            byzantine_broadcast(qs, sid, tx_id, log)
        else:
            # Node trung thuc: broadcast nhat quan
            my_vote = VOTE_COMMIT
            log.info("VOTE", "tx_id=%d | Phieu: %s (Trung thuc)" % (tx_id, my_vote))
            honest_broadcast(qs, sid, tx_id, my_vote, log)

        # Ghi READY vao WAL truoc khi crash (stable storage)
        wal.write(tx_id, STATE_READY, my_vote)
        log.info("STATE", "tx_id=%d -> %s" % (tx_id, STATE_READY))

        # Mo phong crash
        if crash_after:
            log.info(
                "CRASH", "tx_id=%d | !!! Site %d CRASH sau broadcast !!!" % (tx_id, sid)
            )
            print(
                "*** CRASH: Site %d crashed after broadcast TX %d! ***" % (sid, tx_id),
                flush=True,
            )
            sys.stdout.flush()
            os._exit(0)

    # --- Buoc 3: Thu thap phieu ---
    votes = collect_votes(qs, sid, tx_id, votes, my_vote, is_recovery, log)

    # --- Buoc 4: Ra quyet dinh bang cong thuc quorum ---
    decision = make_decision(votes, tx_id, log)

    wal.write(tx_id, decision, my_vote)
    log.info("STATE", "tx_id=%d -> %s" % (tx_id, decision))
    log.info("FINAL_DECISION", "tx_id=%d | %s" % (tx_id, decision))
    print(">>> SITE %d: TX %d = %s <<<" % (sid, tx_id, decision), flush=True)

    # --- Buoc 5: [FIX 3] Ghi vao ledger neu COMMIT ---
    if decision == VOTE_COMMIT:
        ledger.append(tx)
        log.info(
            "LEDGER_APPEND",
            "tx_id=%d | Da ghi vao ledger (size=%d)" % (tx_id, len(ledger)),
        )
    else:
        log.info("LEDGER_SKIP", "tx_id=%d | Khong ghi vao ledger (ABORT)" % tx_id)

    # Broadcast quyet dinh den cac site khac
    net_broadcast(qs, sid, MSG_DEC, tx_id, decision=decision)

    # --- Buoc 6: Lang nghe yeu cau phuc hoi ---
    listen_for_recovery(qs, sid, tx_id, my_vote, shutdown, log)

    log.info("TX_END", "tx_id=%d | === KET THUC XU LY ===" % tx_id)


# ============================================================
# SITE MAIN — Entry point cho moi tien trinh (process)
# ============================================================


def site_main(sid, qs, is_malicious, tx_list, crash_on_tx, shutdown):
    """
    [FIX 7] Ham chinh cua moi site — gon hon, uy quyen cho cac ham con.
    [FIX 4] Xu ly danh sach giao dich, khong chi 1 TX.
    [FIX 3] Duy tri ledger (so cai phan tan) xuyen suot.

    Moi site chay trong mot process rieng (multiprocessing).
    Khi khoi dong, doc WAL de khoi phuc ledger (neu co crash truoc do).
    """
    # [FIX 11] Dung random_seed tu config de ket qua tai lap duoc.
    # Moi site dung seed khac nhau (base + sid) de co latency khac nhau,
    # nhung ket qua tong the van deterministic khi chay lai.
    if config.random_seed is not None:
        random.seed(config.random_seed + sid)
    else:
        random.seed(os.getpid())
    log = Logger(sid)
    wal = WAL(sid)

    # [FIX 3] Khoi phuc ledger tu WAL (cho TX da commit truoc crash)
    ledger = wal.rebuild_ledger(TRANSACTIONS)

    role = "BYZANTINE (equivocation)" if is_malicious else "Trung thuc"
    log.info("STARTUP", "Vai tro: %s | Ledger hien tai: %d TX" % (role, len(ledger)))

    for tx in tx_list:
        if shutdown.is_set():
            break

        tx_id = tx["tx_id"]

        # Bo qua TX da commit (truoc khi crash)
        last_state, _ = wal.read_last_state(tx_id)
        if last_state == VOTE_COMMIT:
            log.info("SKIP", "tx_id=%d | Da COMMIT trong WAL, bo qua" % tx_id)
            continue

        crash_after = (crash_on_tx == tx_id) if crash_on_tx else False
        process_transaction(
            sid, qs, tx, is_malicious, crash_after, shutdown, log, wal, ledger
        )

    # In ledger cuoi cung
    log.info("LEDGER_FINAL", "So cai: %s" % json.dumps(ledger, ensure_ascii=False))
    log.info("EXIT", "Ket thuc. Ledger: %d giao dich" % len(ledger))
    print(
        "[Site %d] Ket thuc. Ledger co %d giao dich." % (sid, len(ledger)), flush=True
    )
