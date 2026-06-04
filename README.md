# 🌐 BFT Distributed Ledger — PBFT + RocksDB + TCP + Ed25519

He thong phan tan chiu loi Byzantine (PBFT) thuc te chay tren mang TCP thực, ho tro chu ky so Ed25519, co so du lieu RocksDB (WAL, state, ledger), checkpointing, va client gui giao dich doc lap.

---

## 🛠️ Tinh Nang Noi Bat

1. **Giao thuc PBFT day du (3 pha)**: Pre-prepare, Prepare, Commit. Chon Leader dua tren view (`view % N`).
2. **Kiet tac bao mat (Ed25519)**: Toan bo cac tin nhan giua cac node deu duoc ky va xac thuc bang chu ky so Ed25519 that.
3. **Ket noi TCP ben vung (Connection Pool)**: Su dung connection pool duy tri socket TCP mo lien tuc, tu dong ket noi lai khi gap loi mang.
4. **Luu tru RocksDB**:
   - Ghi WAL, state so du (balances), va ledger cac giao dich da commit.
   - Luu va nap **State Checkpoint** moi $K=2$ giao dich de toi uu hoa khong gian luu tru va cat tia WAL/PBFT logs cu.
   - Khoi phuc sau Crash (Crash Recovery) bang cach khoi phuc balance tu checkpoint va replay cac giao dich da commit trong WAL.
5. **Heartbeat PING/PONG**: Dinh ky kiem tra suc khoe leader va tu dong nghi ngo de kich hoat View Change neu leader mat ket noi.
6. **Client doc lap (`client.py`)**: Ky va gui giao dich directly. Ho tro redirect ve leader moi neu client ket noi sai node backup.
7. **Kich ban kiem thu va tu dong hoa**: Makefile va `tests/integration_test.py` hoan toan tu dong.

---

## 📁 Cau Truc Thu Muc

```
csdlpt/
├── config.py                 # Cau hinh chung cua mang va crypto
├── crypto_utils.py           # Ma hoa chu ky Ed25519 va Hash SHA-256
├── logger.py                 # Logger ghi ra log file va console
├── network/
│   ├── __init__.py           # Giao tiep sign/verify va pool helpers
│   ├── tcp_server.py         # TCP Server lang nghe ket noi dai han
│   ├── tcp_client.py         # TCP Client gui tin nhan
│   └── connection_pool.py    # TCP Connection Pool giu ket noi
├── storage/
│   ├── __init__.py           # Expose store va WorldState
│   ├── rocksdb_store.py      # RocksDB backend (WAL, State, Checkpoint, Ledger)
│   └── world_state.py        # World State balances, checkpoint & replay logic
├── consensus/
│   ├── __init__.py
│   ├── pbft.py               # PBFT consensus state machine (3 pha, checkpoint)
│   └── view_change.py        # View Change manager (prepared_certs, NEW-VIEW)
├── node.py                   # Node entrypoint (chay che do demo hoac --listen)
├── client.py                 # Client doc lap gui transaction va nhan reply
├── main.py                   # Orchestrator chay demo mo phong localhost
├── Dockerfile                # Dockerfile dong goi node chay Linux
├── docker-compose.yml        # Khoi chay he thong 4 node qua Docker Compose
├── simulate_network.sh       # Script tc-netem gia lap mat goi va do tre
├── Makefile                  # Makefile tu dong hoa moi thao tac
└── tests/
    ├── test_crypto.py        # Unit tests cho crypto
    └── integration_test.py   # Integration tests cho he thong
```

---

## 🚀 Huong Dan Chay He Thong

### 1. Cai dat moi truong
Yeu cau Python 3.8+ va cac goi dependencies trong `requirements.txt`:
```bash
pip install -r requirements.txt
```

### 2. Chay demo mo phong cuc bo (Localhost)
De chay nhanh demo mo phong 4 node thuc hien dong thuan tu dong (Site 0 Byzantine, Site 2 crash va recovery):
```bash
python main.py
# Hoac dung Makefile:
make test
```

### 3. Chay bang Docker Compose va Client
Khoi chay he thong 4 container nodes duy tri listening:
```bash
# Build va start container
make build
make up
```

Gui giao dich thong qua client doc lap tu ben ngoai:
```bash
# Gui transaction (se tu dong redirect ve Leader Node 0 neu gui vao Node 1)
python client.py --node localhost:5001 --op "A chuyen 10 cho B"
```

Dung cum mang Docker:
```bash
make down
```

### 4. Demo tu dong hoa toan bo (1-Click Demo)
Script `run_demo.sh` se lam moi logs, khoi chay cum docker, dung client gui 3 giao dich va in ra ket qua ledger RocksDB cuoi cung:
```bash
make demo
```

---

## 🧪 Kịch Bản Kiểm Thử Tích Hợp (`tests/integration_test.py`)
Kịch bản test chay tu dong se:
1. Reset rocksdb va logs.
2. Spawn 4 nodes chay song song che do lang nghe.
3. Chay client gui CLIENT_REQUEST den Node 1 (Backup) va xac minh node tu dong REDIRECT ve Node 0 (Leader).
4. Xac minh client nhan du f+1 = 2 phieu SUCCESS hop le va commit.
5. Tat cac nodes, doc truc tiep balances cua RocksDB site 0..3 de kiem tra tinh nhat quan (A: 90, B: 110, ledger: 1 giao dich).

Chay kiem thu:
```bash
make test
```
