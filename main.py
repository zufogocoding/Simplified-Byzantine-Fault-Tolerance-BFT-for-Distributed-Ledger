#!/usr/bin/env python3
"""
Simplified BFT cho Distributed Ledger
=====================================
Mo phong 4 nut BFT (3f+1, f=1):
- Site 0: doc hai (luon ABORT)
- Site 1,2,3: trung thuc (COMMIT)
- Site 2: crash sau broadcast, phuc hoi tu log + REQUEST_VOTES

Chay: python main.py
"""

import multiprocessing
import os
import sys
import time
import random
from datetime import datetime

NUM_SITES = 4
MALICIOUS_SITE = 0
CRASH_SITE = 2
TIMEOUT = 5.0
NET_MIN = 0.1
NET_MAX = 0.5
LISTEN_AFTER = 15.0

TX_ID = 1
TRANSACTION = {'tx_id': TX_ID, 'data': 'A transfer 10 to B'}

VOTE_COMMIT = 'COMMIT'
VOTE_ABORT = 'ABORT'
STATE_INIT = 'INIT'
STATE_READY = 'READY'
STATE_COMMIT = 'COMMIT'
STATE_ABORT = 'ABORT'

MSG_VOTE = 1
MSG_REQ = 2
MSG_RESP = 3
MSG_DEC = 4

class Logger:
    def __init__(self, sid):
        self.sid = sid
        self.fn = 'site_%d.log' % sid

    def log(self, event, detail, do_print=True):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        line = '[%s] Site %d | %s | %s' % (ts, self.sid, event, detail)
        if do_print:
            print(line, flush=True)
        with open(self.fn, 'a') as f:
            f.write(line + '\n')
            f.flush()
            os.fsync(f.fileno())

    def get_state_vote(self, tx_id):
        try:
            with open(self.fn) as f:
                entries = [l.strip() for l in f if l.strip()]
        except FileNotFoundError:
            return None, None
        state = None
        vote = None
        for e in entries:
            parts = e.split(' | ', 2)
            if len(parts) != 3:
                continue
            ev = parts[1]
            det = parts[2]
            if ev == 'STATE' and ('tx_id=%d' % tx_id) in det and '->' in det:
                state = det.split('->')[-1].strip()
            if ev == 'VOTE' and ('tx_id=%d' % tx_id) in det and 'Phieu:' in det:
                v = det.split('Phieu:')[-1].strip().split()[0]
                if v in (VOTE_COMMIT, VOTE_ABORT):
                    vote = v
        return state, vote

    def clear(self):
        try:
            os.remove(self.fn)
        except FileNotFoundError:
            pass



def net_send(qs, src, dst, mtype, tx_id, **kw):
    time.sleep(random.uniform(NET_MIN, NET_MAX))
    msg = {'type': mtype, 'sender': src, 'tx_id': tx_id}
    msg.update(kw)
    qs[dst].put(msg)

def net_broadcast(qs, src, mtype, tx_id, **kw):
    for d in range(NUM_SITES):
        net_send(qs, src, d, mtype, tx_id, **kw)

def site_main(sid, qs, is_malicious, crash_after, shutdown):
    random.seed(os.getpid())
    log = Logger(sid)
    tx_id = TX_ID

    last_state, my_vote = log.get_state_vote(tx_id)
    is_recovery = (last_state == STATE_READY)

    if is_recovery:
        log.log('RECOVERY_START',
                'tx_id=%d | Phat hien READY. Vote cua toi: %s' % (tx_id, my_vote))
        log.log('STATE', 'tx_id=%d -> %s (phuc hoi tu log)' % (tx_id, STATE_READY))
        votes = {}
        if my_vote:
            votes[sid] = my_vote
            log.log('RECEIVED', 'tx_id=%d | %s | from=self (tu log)' % (tx_id, my_vote))
            log.log('REQUEST_VOTES',
                    'tx_id=%d | Gui yeu cau phieu den cac site con song...' % tx_id)
            for t in range(NUM_SITES):
                if t != sid:
                    net_send(qs, sid, t, MSG_REQ, tx_id)
                    log.log('SEND', 'tx_id=%d | REQUEST_VOTES | to=Site %d' % (tx_id, t))
    else:
        log.log('STATE', 'tx_id=%d -> %s' % (tx_id, STATE_INIT))
        log.log('WAITING', 'tx_id=%d | Dang cho giao dich...' % tx_id)
        if is_malicious:
            my_vote = VOTE_ABORT
            log.log('VOTE', 'tx_id=%d | Phieu: %s (DOC HAI)' % (tx_id, my_vote))
        else:
            my_vote = VOTE_COMMIT
            log.log('VOTE', 'tx_id=%d | Phieu: %s (Trung thuc)' % (tx_id, my_vote))

        log.log('BROADCAST', 'tx_id=%d | Dang broadcast phieu %s' % (tx_id, my_vote))
        for t in range(NUM_SITES):
            time.sleep(random.uniform(NET_MIN, NET_MAX))
            qs[t].put({'type': MSG_VOTE, 'sender': sid, 'tx_id': tx_id, 'vote': my_vote})
            tag = 'self' if t == sid else 'Site %d' % t
            log.log('SEND', 'tx_id=%d | %s | to=%s' % (tx_id, my_vote, tag))

        log.log('STATE', 'tx_id=%d -> %s' % (tx_id, STATE_READY))
        votes = {}

        if crash_after:
            log.log('CRASH',
                    'tx_id=%d | Site %d CRASH! Phieu %s da broadcast xong.'
                    % (tx_id, sid, my_vote))
            print('*** CRASH: Site %d crashed after broadcast! ***\n' % sid, flush=True)
            sys.stdout.flush()
            os._exit(0)

    # === COLLECT VOTES ===
    start = time.time()
    while len(votes) < NUM_SITES:
        rem = TIMEOUT - (time.time() - start)
        if rem <= 0:
            log.log('TIMEOUT', 'tx_id=%d | Timeout. Co %d/%d phieu' % (tx_id, len(votes), NUM_SITES))
            break
        try:
            msg = qs[sid].get(timeout=min(0.5, rem))
        except Exception:
            continue
        if msg.get('tx_id') != tx_id:
            continue
        mt = msg.get('type')
        s = msg.get('sender')

        if mt == MSG_VOTE and not is_recovery:
            v = msg.get('vote')
            if s is not None and s not in votes:
                votes[s] = v
                log.log('RECEIVED', 'tx_id=%d | %s | from=Site %d' % (tx_id, v, s))
                print('  [Site %d] Nhan %s tu Site %d (%d/%d)'
                      % (sid, v, s, len(votes), NUM_SITES), flush=True)
        elif mt == MSG_RESP:
            v = msg.get('vote')
            if s is not None and s not in votes:
                votes[s] = v
                log.log('RECEIVED', 'tx_id=%d | VOTE_RESPONSE %s | from=Site %d' % (tx_id, v, s))
                print('  [Site %d] Nhan VOTE_RESPONSE %s tu Site %d (%d/%d)'
                      % (sid, v, s, len(votes), NUM_SITES), flush=True)
        elif mt == MSG_REQ:
            log.log('RECEIVED', 'tx_id=%d | REQUEST_VOTES | from=Site %d' % (tx_id, s))
            net_send(qs, sid, s, MSG_RESP, tx_id, vote=my_vote)
            log.log('SEND', 'tx_id=%d | VOTE_RESPONSE %s | to=Site %d' % (tx_id, my_vote, s))
            print('  [Site %d] Gui %s cho Site %d (ho tro phuc hoi)' % (sid, my_vote, s), flush=True)
        elif mt == MSG_DEC:
            log.log('RECEIVED', 'tx_id=%d | DECISION %s | from=Site %d'
                    % (tx_id, msg.get('decision'), s))

    # === DECIDE ===
    cc = sum(1 for v in votes.values() if v == VOTE_COMMIT)
    total = len(votes)
    log.log('DECISION_CALC', 'tx_id=%d | COMMIT=%d/%d ABORT=%d/%d'
            % (tx_id, cc, total, total - cc, total))

    if total >= NUM_SITES and cc >= 3:
        decision = VOTE_COMMIT
    elif total >= NUM_SITES and cc < 3:
        decision = VOTE_ABORT
    elif cc >= 3:
        decision = VOTE_COMMIT
        log.log('TIMEOUT_DECISION', 'tx_id=%d | Timeout nhung %d >= 3 COMMIT' % (tx_id, cc))
    else:
        decision = VOTE_ABORT
        log.log('TIMEOUT_DECISION', 'tx_id=%d | Timeout, thieu phieu' % tx_id)

    log.log('STATE', 'tx_id=%d -> %s' % (tx_id, decision))
    log.log('FINAL_DECISION', 'tx_id=%d | %s' % (tx_id, decision))
    print('\n>>> SITE %d: QUYET DINH CUOI CUNG = %s <<<\n' % (sid, decision), flush=True)

    net_broadcast(qs, sid, MSG_DEC, tx_id, decision=decision)

    # === LISTEN FOR RECOVERY REQUESTS ===
    log.log('LISTENING', 'tx_id=%d | Lang nghe yeu cau phuc hoi...' % tx_id)
    until = time.time() + LISTEN_AFTER
    while time.time() < until:
        if shutdown.is_set():
            break
        try:
            msg = qs[sid].get(timeout=0.5)
        except Exception:
            continue
        if msg.get('tx_id') != tx_id:
            continue
        if msg.get('type') == MSG_REQ:
            s = msg['sender']
            net_send(qs, sid, s, MSG_RESP, tx_id, vote=my_vote)
            log.log('SEND', 'tx_id=%d | VOTE_RESPONSE %s | to=Site %d (ho tro)'
                    % (tx_id, my_vote, s))
            print('  [Site %d] Ho tro: gui %s cho Site %d' % (sid, my_vote, s), flush=True)

    log.log('EXIT', 'tx_id=%d | Ket thuc' % tx_id)
    print('[Site %d] Ket thuc.' % sid, flush=True)

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('fork')
    except RuntimeError:
        pass

    for i in range(NUM_SITES):
        Logger(i).clear()

    mgr = multiprocessing.Manager()
    qs = mgr.dict()
    for i in range(NUM_SITES):
        qs[i] = mgr.Queue()
    shutdown = mgr.Event()

    print('=' * 72)
    print('  SIMPLIFIED BYZANTINE FAULT TOLERANCE')
    print('  CHO DISTRIBUTED LEDGER')
    print('=' * 72)
    print('  Transaction: %s' % TRANSACTION)
    print('  Cau hinh:')
    print('    Site 0: DOC HAI (luon ABORT)')
    print('    Site 1: Trung thuc (COMMIT)')
    print('    Site 2: Trung thuc -> CRASH -> PHUC HOI')
    print('    Site 3: Trung thuc (COMMIT)')
    print('  Rule: 3f+1 (f=1), can >= 3 COMMIT')
    print('=' * 72)
    print()

    procs = []
    cfgs = [(0, True, False), (1, False, False),
            (2, False, True), (3, False, False)]
    for sid, mal, crash in cfgs:
        p = multiprocessing.Process(
            target=site_main, args=(sid, qs, mal, crash, shutdown))
        p.start()
        procs.append(p)
        print('[Main] Da khoi dong Site %d' % sid, flush=True)

    print('\n[Main] Dang cho Site 2 broadcast va crash...\n', flush=True)
    time.sleep(5)

    if procs[CRASH_SITE].is_alive():
        print('[Main] Terminate Site %d...' % CRASH_SITE, flush=True)
        procs[CRASH_SITE].terminate()
    else:
        print('[Main] Phat hien Site %d da crash!' % CRASH_SITE, flush=True)
    procs[CRASH_SITE].join(timeout=2)

    print('[Main] Cac site 0, 1, 3 dang dong thuan...', flush=True)
    time.sleep(3)

    # Restart Site 2 - use fresh queue to force REQUEST_VOTES
    print('\n' + '-' * 60)
    print('  PHUC HOI: Khoi dong lai Site %d' % CRASH_SITE)
    print('  (Doc log -> RECOVERY_START -> REQUEST_VOTES -> collect)')
    print('-' * 60 + '\n', flush=True)

    qs[CRASH_SITE] = mgr.Queue()

    p2 = multiprocessing.Process(
        target=site_main, args=(CRASH_SITE, qs, False, False, shutdown))
    p2.start()
    procs[CRASH_SITE] = p2
    print('[Main] Da khoi dong lai Site %d!\n' % CRASH_SITE, flush=True)

    time.sleep(5)

    # Shutdown
    print('\n[Main] Ket thuc mo phong. Don dep...\n', flush=True)
    shutdown.set()
    for i, p in enumerate(procs):
        p.join(timeout=3)
        if p.is_alive():
            p.terminate()
            p.join(timeout=1)

    # Final results
    print('\n' + '=' * 72)
    print('  KET QUA CUOI CUNG')
    print('=' * 72)
    states = []
    for i in range(NUM_SITES):
        l = Logger(i)
        st, vt = l.get_state_vote(TX_ID)
        states.append(st)
        label = {0: 'DOC HAI', 1: 'Trung thuc',
                 2: 'CRASH->PHUC HOI', 3: 'Trung thuc'}[i]
        print('  Site %d (%s): %s | Vote: %s' % (i, label, st, vt))

    print()
    print('=' * 72)
    ok = all(states[i] == VOTE_COMMIT for i in range(NUM_SITES) if i != MALICIOUS_SITE)
    if ok:
        print('  ✓ DAT DONG THUAN: 3 site trung thuc deu COMMIT!')
        print()
        print('  *** He thong da dat dong thuan COMMIT, bo qua phieu ABORT')
        print('      cua site phan boi (Site 0) va phuc hoi thanh cong')
        print('      sau crash cua Site 2. ***')
    else:
        print('  ✗ THAT BAI. Trang thai: %s' % states)
    print('=' * 72)
    print()
    print('Chi tiet: xem site_*.log')

    mgr.shutdown()
