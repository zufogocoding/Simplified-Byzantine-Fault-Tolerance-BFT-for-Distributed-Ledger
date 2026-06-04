import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import subprocess

from storage.rocksdb_store import KVStore, delete_db


def run_node_process(sid, listen=False):
    """Khoi dong mot node Python subprocess."""
    cmd = [sys.executable, "node.py", str(sid)]
    if listen:
        cmd.append("--listen")
    log_file = open(f"logs/test_node_{sid}.log", "w")
    p = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    return p, log_file


def cleanup_processes(procs, log_files):
    """Don dep cac process con va log file handles."""
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


def test_integration_normal_consensus():
    """Test Case: Giao dich thong qua client doc lap va redirect ve leader."""
    print("--- Running Test: Normal Consensus & Client Redirection ---")
    
    # Don dep DB cu
    for i in range(4):
        delete_db(i)

    procs = []
    log_files = []
    
    # 1. Khoi dong 4 node
    for i in range(4):
        p, lf = run_node_process(i, listen=True)
        procs.append(p)
        log_files.append(lf)

    print("[Test] Cho 3 giay de cac node TCP start va ket noi pool...")
    time.sleep(3)

    # 2. Chay client.py gui den Node 1 (Backup) de xac minh REDIRECT ve Node 0 (Leader)
    print("[Test] Gui CLIENT_REQUEST den Backup Node 1...")
    client_proc = subprocess.run([
        sys.executable, "client.py",
        "--node", "localhost:5001",
        "--op", "A chuyen 10 cho B"
    ], capture_output=True, text=True)

    print("[Client Output]:")
    print(client_proc.stdout)
    
    # Kiem tra exit code cua client (0 la thanh cong)
    assert client_proc.returncode == 0, "Client submission failed!"

    # 3. Cho thuc thi ghi RocksDB
    time.sleep(2)
    
    # Don dep cac node truoc de giai phong khoa RocksDB (LOCK)
    cleanup_processes(procs, log_files)
    
    # Xac minh ledger va balances tren cac site trung thuc
    print("[Test] Xac minh du lieu ghi nhan trong RocksDB...")
    for i in range(4):
        store = KVStore(sid=i)
        bal_a = store.get_balance("A")
        bal_b = store.get_balance("B")
        ledger = store.get_ledger()
        
        print(f"  Node {i} - Balance A: {bal_a}, B: {bal_b}, Ledger: {len(ledger)} txs")
        assert bal_a == 90, f"Node {i}: Balance A phai la 90 thay vi {bal_a}"
        assert bal_b == 110, f"Node {i}: Balance B phai la 110 thay vi {bal_b}"
        assert len(ledger) == 1, f"Node {i}: Ledger length phai la 1"
        store.close()

    print("[Test] Normal Consensus & Redirect PASSED!")


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    try:
        test_integration_normal_consensus()
        print("\nAll integration tests PASSED!")
        sys.exit(0)
    except AssertionError as e:
        print(f"\nTest FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError running tests: {e}")
        sys.exit(1)
