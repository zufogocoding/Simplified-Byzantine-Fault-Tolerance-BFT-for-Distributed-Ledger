"""
Simplified BFT cho Distributed Ledger — Orchestrator

Mo phong N nut (multiprocessing) voi co che dong thuan BFT (3f+1):
- Site 0: Byzantine — equivocation (gui phieu KHAC NHAU cho cac site)
- Site 1, 3: Trung thuc (luon COMMIT)
- Site 2: Trung thuc, crash sau TX 1, phuc hoi bang WAL

Tai sao can BFT ma khong dung Paxos?
  Paxos chi xu ly crash fault (node ngung hoat dong).
  BFT xu ly Byzantine fault (node gui thong tin MAU THUAN — equivocation).
  Site 0 gui COMMIT cho mot so site, ABORT cho site khac.
  => Paxos KHONG chong duoc hanh vi nay!

Cau truc project:
  config.py    — Hang so va tham so he thong
  logger.py    — Ghi log su kien (console + file)
  wal.py       — Write-Ahead Log (crash recovery)
  network.py   — Truyen thong + chu ky so gia lap
  consensus.py — ★ State Machine BFT (trai tim giao thuc)
  main.py      — Orchestrator (file nay) — dieu phoi mo phong

Chay: python main.py
"""

import sys
import time
import json
import queue
import multiprocessing

from config import (
    config,
    F, NUM_SITES, QUORUM,
    MALICIOUS_SITE, CRASH_SITE, CRASH_ON_TX,
    TIMEOUT, LISTEN_AFTER,
    TRANSACTIONS, VOTE_COMMIT,
)
from logger import Logger
from wal import WAL
from consensus import site_main


# ============================================================
# MAIN — Dieu phoi mo phong 2 phase
# ============================================================
if __name__ == "__main__":
    if sys.platform != "win32":
        try:
            multiprocessing.set_start_method("fork")
        except RuntimeError:
            pass

    # Xoa log va WAL cu
    for i in range(NUM_SITES):
        Logger(i).clear()
        WAL(i).clear()

    # Khoi tao queue + event
    # [FIX 10] Dung list thay vi dict — giam overhead, truy cap nhanh hon
    mgr = multiprocessing.Manager()
    qs = [mgr.Queue() for _ in range(NUM_SITES)]
    shutdown = mgr.Event()

    # ===== IN THONG TIN HE THONG =====
    print("=" * 60)
    print("  SIMPLIFIED BFT CHO DISTRIBUTED LEDGER")
    print("=" * 60)
    print()
    print("  Thong so BFT:")
    print("    f = %d (so node Byzantine toi da)" % F)
    print("    N = 3f+1 = %d (tong so node)" % NUM_SITES)
    print("    Quorum = 2f+1 = %d (so phieu COMMIT can thiet)" % QUORUM)
    print()
    print("  Giao dich:")
    for tx in TRANSACTIONS:
        print("    TX %d: %s" % (tx["tx_id"], tx["data"]))
    print()
    print("  Cau hinh node:")
    print("    Site 0: BYZANTINE (equivocation)")
    print("            -> Gui COMMIT cho site 0,2; ABORT cho site 1,3")
    print("    Site 1: Trung thuc")
    print("    Site 2: Trung thuc -> CRASH tai TX %d -> PHUC HOI" % CRASH_ON_TX)
    print("    Site 3: Trung thuc")
    print()
    print("  Tai sao BFT ma khong dung Paxos?")
    print("    Paxos chi xu ly crash fault (node dung hoat dong)")
    print("    BFT xu ly Byzantine fault (node gui thong tin MAU THUAN)")
    print("    => Site 0 se equivocate — Paxos KHONG chong duoc!")
    if config.random_seed is not None:
        print("  Random seed: %d (ket qua tai lap duoc)" % config.random_seed)
    print("=" * 60)
    print()

    # =============================================================
    # PHASE 1: TX 1 — Equivocation + Crash + Recovery
    # =============================================================
    print("PHASE 1: TX 1 (equivocation + crash + recovery)")
    print("-" * 60)
    print()

    tx1 = [TRANSACTIONS[0]]
    procs = []
    for sid in range(NUM_SITES):
        is_mal = sid == MALICIOUS_SITE
        crash = CRASH_ON_TX if sid == CRASH_SITE else None
        p = multiprocessing.Process(
            target=site_main, args=(sid, qs, is_mal, tx1, crash, shutdown)
        )
        p.start()
        procs.append(p)
        print("[Main] Khoi dong Site %d" % sid, flush=True)

    print()
    print("[Main] Cho Site %d broadcast va crash..." % CRASH_SITE)
    print()
    time.sleep(4)

    # Phat hien crash
    if procs[CRASH_SITE].is_alive():
        print("[Main] Terminate Site %d..." % CRASH_SITE, flush=True)
        procs[CRASH_SITE].terminate()
    else:
        print("[Main] Site %d da crash!" % CRASH_SITE, flush=True)
    procs[CRASH_SITE].join(timeout=2)

    print("[Main] Sites 0, 1, 3 dang dong thuan TX 1...")
    time.sleep(2)

    # Phuc hoi Site 2
    print()
    print("-" * 60)
    print("PHUC HOI: Khoi dong lai Site %d" % CRASH_SITE)
    print("  Doc WAL -> READY -> REQUEST_VOTES -> thu thap phieu")
    print("-" * 60)
    print()

    # [FIX 10] Drain queue cu truoc khi tao moi
    # Queue cu co the con message orphan tu truoc khi crash.
    # Neu khong drain, message cu se bi leak (memory leak nho).
    old_q = qs[CRASH_SITE]
    while not old_q.empty():
        try:
            old_q.get_nowait()
        except queue.Empty:
            break
    qs[CRASH_SITE] = mgr.Queue()
    p_recover = multiprocessing.Process(
        target=site_main, args=(CRASH_SITE, qs, False, tx1, None, shutdown)
    )
    p_recover.start()
    procs[CRASH_SITE] = p_recover
    print("[Main] Da khoi dong lai Site %d!" % CRASH_SITE, flush=True)
    print()

    # Cho tat ca hoan thanh Phase 1
    time.sleep(TIMEOUT + LISTEN_AFTER + 3)
    for i, p in enumerate(procs):
        p.join(timeout=3)
        if p.is_alive():
            p.terminate()
            p.join(timeout=1)

    # =============================================================
    # PHASE 2: TX 2 — Binh thuong (van co equivocation, khong crash)
    # =============================================================
    print()
    print("=" * 60)
    print("PHASE 2: TX 2 (binh thuong, khong crash)")
    print("-" * 60)
    print()

    # Tao queue moi cho Phase 2
    qs[:] = [mgr.Queue() for _ in range(NUM_SITES)]

    tx2 = [TRANSACTIONS[1]]
    procs2 = []
    for sid in range(NUM_SITES):
        is_mal = sid == MALICIOUS_SITE
        p = multiprocessing.Process(
            target=site_main, args=(sid, qs, is_mal, tx2, None, shutdown)
        )
        p.start()
        procs2.append(p)
        print("[Main] Khoi dong Site %d (Phase 2)" % sid, flush=True)

    print()

    # Cho Phase 2 hoan thanh
    time.sleep(TIMEOUT + LISTEN_AFTER + 3)
    shutdown.set()
    for p in procs2:
        p.join(timeout=3)
        if p.is_alive():
            p.terminate()
            p.join(timeout=1)

    # =============================================================
    # KET QUA CUOI CUNG
    # =============================================================
    print()
    print("=" * 60)
    print("  KET QUA CUOI CUNG")
    print("=" * 60)
    print()

    labels = {0: "BYZANTINE", 1: "Trung thuc", 2: "CRASH->PHUC HOI", 3: "Trung thuc"}

    for i in range(NUM_SITES):
        w = WAL(i)
        ledger = w.rebuild_ledger(TRANSACTIONS)
        print("  Site %d (%s):" % (i, labels[i]))
        for tx in TRANSACTIONS:
            st, vt = w.read_last_state(tx["tx_id"])
            icon = "\u2713" if st == VOTE_COMMIT else ("\u2717" if st else "\u2014")
            print(
                "    %s TX %d: state=%-8s vote=%-8s | %s"
                % (icon, tx["tx_id"], st or "?", vt or "?", tx["data"])
            )
        print("    Ledger: %d giao dich" % len(ledger))
        print()

    # Kiem tra dong thuan
    print("-" * 60)
    all_ok = True
    for tx in TRANSACTIONS:
        honest_states = []
        for i in range(NUM_SITES):
            if i == MALICIOUS_SITE:
                continue
            w = WAL(i)
            st, _ = w.read_last_state(tx["tx_id"])
            honest_states.append(st)

        if all(s == VOTE_COMMIT for s in honest_states):
            print(
                "  \u2713 TX %d: DONG THUAN — 3 site trung thuc deu COMMIT"
                % tx["tx_id"]
            )
        elif all(s is not None for s in honest_states):
            consistent = len(set(honest_states)) == 1
            if consistent:
                print(
                    "  \u2713 TX %d: DONG THUAN — 3 site trung thuc deu %s"
                    % (tx["tx_id"], honest_states[0])
                )
            else:
                print(
                    "  \u2717 TX %d: MAT DONG THUAN! States: %s"
                    % (tx["tx_id"], honest_states)
                )
                all_ok = False
        else:
            print(
                "  ? TX %d: CHUA HOAN THANH — States: %s" % (tx["tx_id"], honest_states)
            )
            all_ok = False

    print()
    if all_ok:
        print("  KET LUAN:")
        print(
            "  He thong BFT (N=3f+1=%d, quorum=2f+1=%d) da dam bao"
            % (NUM_SITES, QUORUM)
        )
        print("  DONG THUAN cho tat ca %d giao dich, bat chap:" % len(TRANSACTIONS))
        print("    1. Site 0 thuc hien EQUIVOCATION (gui phieu mau thuan)")
        print("    2. Site 2 bi CRASH va phai PHUC HOI tu WAL")
        print()
        print("  => CHUNG MINH: Can BFT (khong phai Paxos) vi Paxos")
        print("     KHONG chong duoc equivocation cua Byzantine node!")
    else:
        print("  CANH BAO: Co loi trong qua trinh dong thuan!")

    print()
    print("  Chi tiet: xem logs/ (event log) va wal/ (write-ahead log)")
    print("=" * 60)

    mgr.shutdown()
