"""
[PHASE 4] BFT Distributed Ledger — Orchestrator (PBFT)

Chay: python main.py
"""

import os
import sys
import time
import subprocess

from config import (
    config,
    F, NUM_SITES, QUORUM,
    MALICIOUS_SITE, CRASH_SITE, CRASH_ON_TX,
    TIMEOUT, LISTEN_AFTER, TRANSACTIONS,
    VOTE_COMMIT, NODE_ADDRESSES, LOG_DIR,
)

from logger import Logger
from storage.rocksdb_store import KVStore, delete_db


def main():
    # Xoa log va RocksDB cu
    for i in range(NUM_SITES):
        Logger(i).clear()
        delete_db(i)

    # ===== IN THONG TIN HE THONG =====
    print("=" * 60)
    print("  BFT DISTRIBUTED LEDGER — PBFT + ROCKSDB + TCP + ED25519")
    print("=" * 60)
    print()
    print("  Thong so BFT:")
    print("    f = %d (so node Byzantine toi da)" % F)
    print("    N = 3f+1 = %d (tong so node)" % NUM_SITES)
    print("    Quorum = 2f+1 = %d (so phieu can cho PREPARE/COMMIT)" % QUORUM)
    print()
    print("  Giao thuc dong thuan: PBFT (3 pha: Pre-prepare, Prepare, Commit)")
    print("  Leader chon theo view number: leader = view %% N")
    print()
    for sid, (host, port) in NODE_ADDRESSES.items():
        print("    Site %d: %s:%d" % (sid, host, port))
    print()
    print("  Giao dich:")
    for tx in TRANSACTIONS:
        print("    TX %d: %s" % (tx["tx_id"], tx["data"]))
    print()
    print("  Cau hinh node:")
    print("    Site 0: BYZANTINE (equivocation) - (trong PBFT, equivocation bi phat hien)")
    print("    Site 1: Trung thuc")
    print("    Site 2: Trung thuc -> CRASH tai TX %d -> PHUC HOI" % CRASH_ON_TX)
    print("    Site 3: Trung thuc")
    print()
    print("=" * 60)
    print()

    # Khoi dong tat ca node
    print("Khoi dong %d node PBFT..." % NUM_SITES)
    print()

    procs = []
    log_files = []
    for sid in range(NUM_SITES):
        log_f = open(os.path.join(LOG_DIR, "node_%d_stdout.log" % sid), "w")
        log_files.append(log_f)
        p = subprocess.Popen(
            [sys.executable, "node.py", str(sid)],
            stdout=log_f, stderr=subprocess.STDOUT,
            universal_newlines=True, bufsize=1,
        )
        procs.append(p)
        print("[Main] Khoi dong Node %d (PID %d) - Xem log tai logs/node_%d_stdout.log" % (sid, p.pid, sid), flush=True)

    print()
    print("[Main] Cho cac node xu ly PBFT...")
    print()
    time.sleep(22)


    print("[Main] Ket thuc...")
    print()

    for i, p in enumerate(procs):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    for f in log_files:
        try:
            f.close()
        except Exception:
            pass


    # In ket qua tu RocksDB
    print()
    print("=" * 60)
    print("  KET QUA PBFT")
    print("=" * 60)
    print()

    labels = {0: "BYZANTINE", 1: "Trung thuc", 2: "CRASH->PHUC HOI", 3: "Trung thuc"}

    for i in range(NUM_SITES):
        try:
            store = KVStore(i)
            ledger = store.get_ledger()
            print("  Node %d (%s):" % (i, labels[i]))
            for tx in TRANSACTIONS:
                st, vt = store.read_last_wal_state(tx["tx_id"])
                icon = "✓" if st == VOTE_COMMIT else ("✗" if st else "—")
                print(
                    "    %s TX %d: state=%-8s vote=%-8s | %s"
                    % (icon, tx["tx_id"], st or "?", vt or "?", tx["data"])
                )
            print("    Ledger: %d giao dich" % len(ledger))
            store.close()
        except Exception as e:
            print("  Node %d: Loi khi doc RocksDB: %s" % (i, e))
        print()

    # Kiem tra dong thuan
    print("-" * 60)
    all_ok = True
    for tx in TRANSACTIONS:
        honest_states = []
        for i in range(NUM_SITES):
            if i == MALICIOUS_SITE:
                continue
            try:
                store = KVStore(i)
                st, _ = store.read_last_wal_state(tx["tx_id"])
                honest_states.append(st)
                store.close()
            except Exception:
                honest_states.append(None)

        if all(s == VOTE_COMMIT for s in honest_states):
            print(
                "  ✓ TX %d: DONG THUAN — 3 site trung thuc deu COMMIT"
                % tx["tx_id"]
            )
        elif all(s is not None for s in honest_states):
            consistent = len(set(honest_states)) == 1
            if consistent:
                print(
                    "  ✓ TX %d: DONG THUAN — 3 site trung thuc deu %s"
                    % (tx["tx_id"], honest_states[0])
                )
            else:
                print(
                    "  ✗ TX %d: MAT DONG THUAN! States: %s"
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
        print("  KET LUAN: He thong PBFT da hoat dong!")
    else:
        print("  CANH BAO: Co loi trong qua trinh PBFT!")

    print("=" * 60)


if __name__ == "__main__":
    main()
