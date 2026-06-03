"""
Simplified BFT cho Distributed Ledger

Mo phong N nut (multiprocessing) voi co che dong thuan BFT (3f+1):
- Site 0: Byzantine — equivocation (gui phieu KHAC NHAU cho cac site)
- Site 1, 3: Trung thuc (luon COMMIT)
- Site 2: Trung thuc, crash sau TX 1, phuc hoi bang WAL

Tai sao can BFT ma khong dung Paxos?
  Paxos chi xu ly crash fault (node ngung hoat dong).
  BFT xu ly Byzantine fault (node gui thong tin MAU THUAN — equivocation).
  Site 0 gui COMMIT cho mot so site, ABORT cho site khac.
  => Paxos KHONG chong duoc hanh vi nay!

Chay: python main.py
"""

import multiprocessing
import queue
import os
import sys
import time
import random
import json
from datetime import datetime

# ============================================================
# CAU HINH HE THONG
# ============================================================
# [FIX 2] Quorum tinh tu cong thuc, khong hardcode
F = 1  # So node Byzantine toi da
NUM_SITES = 3 * F + 1  # Quy tac BFT: N = 3f + 1 = 4
QUORUM = 2 * F + 1  # Can 2f+1 = 3 phieu COMMIT de dong thuan

MALICIOUS_SITE = 0  # Node Byzantine
CRASH_SITE = 2  # Node se crash
CRASH_ON_TX = 1  # Crash o giao dich nao
TIMEOUT = 4.0  # Timeout thu thap phieu (giay)
NET_MIN, NET_MAX = 0.05, 0.2  # Do tre mang ngau nhien (giay)
LISTEN_AFTER = 6.0  # Thoi gian lang nghe phuc hoi (giay)
LOG_DIR = "logs"
WAL_DIR = "wal"  # [FIX 5] Thu muc Write-Ahead Log co cau truc

# [FIX 4] Ho tro nhieu giao dich
TRANSACTIONS = [
    {"tx_id": 1, "data": "A chuyen 10 cho B"},
    {"tx_id": 2, "data": "B chuyen 5 cho C"},
]

# Hang so giao thuc
VOTE_COMMIT, VOTE_ABORT = "COMMIT", "ABORT"
STATE_INIT, STATE_READY = "INIT", "READY"
MSG_VOTE, MSG_REQ, MSG_RESP, MSG_DEC = 1, 2, 3, 4

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(WAL_DIR, exist_ok=True)


# ============================================================
# LOGGER — Ghi log su kien ra file va console
# ============================================================
class Logger:
    """Ghi log su kien ra console va file cho moi site."""

    def __init__(self, sid):
        self.sid = sid
        self.path = os.path.join(LOG_DIR, "site_%d.log" % sid)

    def info(self, event, detail, do_print=True):
        """Ghi mot dong log voi timestamp, flush xuong disk ngay."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = "[%s] Site %d | %s | %s" % (ts, self.sid, event, detail)
        if do_print:
            print(line, flush=True)
        with open(self.path, "a") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def clear(self):
        """Xoa file log."""
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


# ============================================================
# [FIX 5] WAL — Write-Ahead Log co cau truc (JSON Lines)
# ============================================================
class WAL:
    """
    Write-Ahead Log co cau truc, ghi trang thai va phieu bau.

    Thay vi parse text log (de bi loi khi format thay doi),
    WAL ghi moi entry la mot dong JSON, dam bao:
    - Doc chinh xac sau crash
    - Khong phu thuoc vao format log
    - De mo rong them truong moi
    """

    def __init__(self, sid):
        self.sid = sid
        self.path = os.path.join(WAL_DIR, "site_%d.wal" % sid)

    def write(self, tx_id, state, vote=None):
        """Ghi mot entry vao WAL va flush xuong disk ngay lap tuc."""
        entry = {
            "tx_id": tx_id,
            "state": state,
            "vote": vote,
            "timestamp": datetime.now().isoformat(),
        }
        with open(self.path, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def read_last_state(self, tx_id):
        """Doc trang thai va phieu bau cuoi cung cua mot giao dich."""
        try:
            with open(self.path) as f:
                lines = [l.strip() for l in f if l.strip()]
        except FileNotFoundError:
            return None, None

        state, vote = None, None
        for line in lines:
            try:
                entry = json.loads(line)
                if entry.get("tx_id") == tx_id:
                    state = entry.get("state")
                    if entry.get("vote") is not None:
                        vote = entry["vote"]
            except json.JSONDecodeError:
                continue
        return state, vote

    def get_committed_tx_ids(self):
        """Lay tap hop tx_id da COMMIT tu WAL."""
        try:
            with open(self.path) as f:
                lines = [l.strip() for l in f if l.strip()]
        except FileNotFoundError:
            return set()

        committed = set()
        for line in lines:
            try:
                entry = json.loads(line)
                if entry.get("state") == VOTE_COMMIT:
                    committed.add(entry["tx_id"])
            except json.JSONDecodeError:
                continue
        return committed

    def rebuild_ledger(self, all_transactions):
        """Khoi phuc ledger tu WAL: tra ve list TX da commit theo thu tu."""
        committed = self.get_committed_tx_ids()
        return [tx for tx in all_transactions if tx["tx_id"] in committed]

    def clear(self):
        """Xoa file WAL."""
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


# ============================================================
# [FIX 6] MANG — Truyen thong voi chu ky so gia lap
# ============================================================
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
    """Gui tin nhan co tre mang ngau nhien va chu ky so."""
    time.sleep(random.uniform(NET_MIN, NET_MAX))
    msg = {"type": mtype, "sender": src, "tx_id": tx_id}
    msg.update(kw)
    sign_message(msg, src)
    qs[dst].put(msg)


def net_broadcast(qs, src, mtype, tx_id, **kw):
    """Broadcast tin nhan den tat ca cac site."""
    for d in range(NUM_SITES):
        net_send(qs, src, d, mtype, tx_id, **kw)


# ============================================================
# [FIX 7] TACH HAM — Cac ham xu ly rieng biet
# ============================================================


# --- Broadcast ---


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


# --- Thu thap phieu ---


def collect_votes(qs, sid, tx_id, votes, my_vote, is_recovery, log):
    """
    Thu thap phieu tu cac site khac.
    [FIX 6] Xac thuc chu ky truoc khi chap nhan.
    [FIX 8] Bat queue.Empty cu the thay vi Exception.
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


# --- Quyet dinh ---


def make_decision(votes, tx_id, log):
    """
    [FIX 2] Ra quyet dinh bang cong thuc quorum BFT:
      QUORUM = 2f + 1 (voi f=1 -> QUORUM=3)
    Thay vi hardcode 'cc >= 3', su dung bien QUORUM
    de code tong quat cho moi gia tri f.
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


# --- Lang nghe phuc hoi ---


def listen_for_recovery(qs, sid, tx_id, my_vote, shutdown, log):
    """
    Lang nghe REQUEST_VOTES tu cac site dang phuc hoi.
    Gui lai phieu cua minh de ho tro site khoi phuc dong thuan.
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
# XU LY GIAO DICH — Logic BFT cho mot giao dich
# ============================================================
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
# SITE MAIN — Entry point cho moi tien trinh
# ============================================================
def site_main(sid, qs, is_malicious, tx_list, crash_on_tx, shutdown):
    """
    [FIX 7] Ham chinh cua moi site — gon hon, uy quyen cho cac ham con.
    [FIX 4] Xu ly danh sach giao dich, khong chi 1 TX.
    [FIX 3] Duy tri ledger (so cai phan tan) xuyen suot.
    """
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


# ============================================================
# MAIN — Dieu phoi mo phong
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
    mgr = multiprocessing.Manager()
    qs = mgr.dict()
    for i in range(NUM_SITES):
        qs[i] = mgr.Queue()
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

    # Tao queue moi
    for i in range(NUM_SITES):
        qs[i] = mgr.Queue()

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
