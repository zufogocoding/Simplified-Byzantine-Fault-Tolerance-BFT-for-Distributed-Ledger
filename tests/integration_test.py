import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import subprocess

from storage.rocksdb_store import KVStore, delete_db


def run_node_process(sid, listen=False, no_byzantine=False):
    cmd = [sys.executable, "node.py", str(sid)]
    if listen:
        cmd.append("--listen")
    if no_byzantine:
        cmd.append("--no-byzantine")
    log_file = open(f"logs/test_node_{sid}.log", "w")
    p = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    return p, log_file


def cleanup_processes(procs, log_files):
    for p in procs:
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=2)
            except Exception:
                p.kill()
    for f in log_files:
        try:
            f.close()
        except Exception:
            pass


def test_normal_consensus():
    """
    Tat ca 4 node deu trung thuc, client redirect ve leader.
    Tat ca node phai dong thuan (A=90, B=110, ledger=1).
    """
    print("--- Test 1: Normal Consensus (all honest) ---\n")
    
    for i in range(4):
        delete_db(i)

    procs, log_files = [], []
    for i in range(4):
        p, lf = run_node_process(i, listen=True, no_byzantine=True)
        procs.append(p)
        log_files.append(lf)

    print("[Test] Cho TCP start...")
    time.sleep(3)

    print("[Test] Gui den Backup Node 1...")
    r = subprocess.run([
        sys.executable, "client.py",
        "--node", "localhost:5001",
        "--op", "A chuyen 10 cho B"
    ], capture_output=True, text=True)
    print(r.stdout)
    assert r.returncode == 0, f"Client fail: {r.stderr}"

    time.sleep(2)
    cleanup_processes(procs, log_files)

    print("[Test] Kiem tra RocksDB...")
    for i in range(4):
        store = KVStore(sid=i)
        a, b = store.get_balance("A"), store.get_balance("B")
        n = len(store.get_ledger())
        print(f"  Node {i}: A={a}, B={b}, ledger={n}")
        assert a == 90, f"Node {i}: A={a} != 90"
        assert b == 110, f"Node {i}: B={b} != 110"
        assert n == 1, f"Node {i}: ledger={n} != 1"
        store.close()
    print("\n[PASS]\n")


def test_byzantine_equivocation():
    """
    Node 0 Byzantine: gui PRE-PREPARE khac cho Node 3.
    Toi thieu 3/4 node phai dong thuan (safety).
    """
    print("--- Test 2: Byzantine Equivocation ---\n")

    for i in range(4):
        delete_db(i)

    procs, log_files = [], []
    for i in range(4):
        p, lf = run_node_process(i, listen=True)  # Byzantine mac dinh
        procs.append(p)
        log_files.append(lf)

    print("[Test] Cho TCP start...")
    time.sleep(3)

    print("[Test] Gui den Leader Node 0...")
    r = subprocess.run([
        sys.executable, "client.py",
        "--node", "localhost:5000",
        "--op", "A chuyen 10 cho B"
    ], capture_output=True, text=True)
    print(r.stdout)

    time.sleep(2)
    cleanup_processes(procs, log_files)

    print("[Test] Kiem tra RocksDB...")
    stores = []
    for i in range(4):
        store = KVStore(sid=i)
        stores.append(store)
        a, b = store.get_balance("A"), store.get_balance("B")
        n = len(store.get_ledger())
        print(f"  Node {i}: A={a}, B={b}, ledger={n}")

    consensus = sum(1 for i in range(4)
                    if stores[i].get_balance("A") == 90 and stores[i].get_balance("B") == 110)
    print(f"\n  => {consensus}/4 dong thuan (A=90, B=110)")
    assert consensus >= 3, f"Chi {consensus}/4 dong thuan, can >=3"
    for s in stores:
        s.close()
    print("  Safety property: 2f+1 = 3 nodes dong thuan\n[PASS]\n")


def test_client_redirect_chain():
    """Gui den Node 3 -> redirect -> leader -> OK."""
    print("--- Test 3: Client Redirect Chain ---\n")

    for i in range(4):
        delete_db(i)

    procs, log_files = [], []
    for i in range(4):
        p, lf = run_node_process(i, listen=True, no_byzantine=True)
        procs.append(p)
        log_files.append(lf)

    print("[Test] Cho TCP start...")
    time.sleep(3)

    print("[Test] Gui den Node 3 (xa leader)...")
    r = subprocess.run([
        sys.executable, "client.py",
        "--node", "localhost:5003",
        "--op", "B chuyen 5 cho C"
    ], capture_output=True, text=True)
    print(r.stdout)
    assert r.returncode == 0, f"Client fail: {r.stderr}"

    time.sleep(2)
    cleanup_processes(procs, log_files)

    print("[Test] Kiem tra RocksDB...")
    for i in range(4):
        store = KVStore(sid=i)
        b, c = store.get_balance("B"), store.get_balance("C")
        n = len(store.get_ledger())
        print(f"  Node {i}: B={b}, C={c}, ledger={n}")
        assert b == 95, f"Node {i}: B={b} != 95"
        assert c == 105, f"Node {i}: C={c} != 105"
        assert n == 1, f"Node {i}: ledger={n} != 1"
        store.close()
    print("\n[PASS]\n")


def test_five_transactions():
    """
    5 giao dich lien tiep, khong co Byzantine.
    Tinh balance ky vong:
      TX1: A chuyen 10 cho B → A=90, B=110
      TX2: B chuyen 5 cho C  → B=105, C=105
      TX3: C chuyen 3 cho D  → C=102, D=103
      TX4: A chuyen 7 cho D  → A=83, D=110
      TX5: B chuyen 2 cho A  → B=103, A=85
    """
    print("--- Test 4: Five Transactions Chain ---\n")

    for i in range(4):
        delete_db(i)

    procs, log_files = [], []
    for i in range(4):
        p, lf = run_node_process(i, listen=True, no_byzantine=True)
        procs.append(p)
        log_files.append(lf)

    print("[Test] Cho TCP start...")
    time.sleep(3)

    ops = [
        "A chuyen 10 cho B",
        "B chuyen 5 cho C",
        "C chuyen 3 cho D",
        "A chuyen 7 cho D",
        "B chuyen 2 cho A",
    ]

    for idx, op in enumerate(ops):
        print(f"\n[Test] TX {idx+1}: {op}")
        r = subprocess.run([
            sys.executable, "client.py",
            "--node", "localhost:5000",
            "--op", op
        ], capture_output=True, text=True)
        print(r.stdout.strip())
        assert r.returncode == 0, f"TX {idx+1} fail: {r.stderr}"
        time.sleep(1)

    time.sleep(2)
    cleanup_processes(procs, log_files)

    expected = {"A": 85, "B": 103, "C": 102, "D": 110}
    print("\n[Test] Kiem tra RocksDB...")
    for i in range(4):
        store = KVStore(sid=i)
        ok = True
        for acct, bal in expected.items():
            actual = store.get_balance(acct)
            if actual != bal:
                print(f"  Node {i}: {acct}={actual} (expected {bal})")
                ok = False
        n = len(store.get_ledger())
        print(f"  Node {i}: ledger={n} tx(s), A={store.get_balance('A')}, B={store.get_balance('B')}, C={store.get_balance('C')}, D={store.get_balance('D')}")
        assert ok, f"Node {i}: balance mismatch"
        assert n == 5, f"Node {i}: ledger={n} != 5"
        store.close()

    print("\n[PASS]\n")


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    try:
        test_normal_consensus()
        test_byzantine_equivocation()
        test_client_redirect_chain()
        test_five_transactions()
        print("\n=== ALL PASSED ===")
        sys.exit(0)
    except AssertionError as e:
        print(f"\nFAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
