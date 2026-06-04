#!/bin/bash
# run_demo.sh - Tu dong hoa toan bo qua trinh chay thu PBFT tren Docker Compose

# Don dep log va database cu
rm -rf logs/* rocksdb/*

echo "=== 1. BUILD VA KHOI DONG CUM PBFT DOCKER COMPOSE ==="
docker-compose down
docker-compose build
docker-compose up -d

echo "Dang cho 5 giay cho cac node khoi dong..."
sleep 5

echo "=== 2. GUI GIAO DICH QUA CLIENT DOC LAP ==="
# Gui TX 1 den Node 1 (Backup) -> se tu redirect sang Node 0 (Leader)
python client.py --node localhost:5001 --op "A chuyen 10 cho B"
sleep 1

# Gui TX 2 den Node 2
python client.py --node localhost:5002 --op "B chuyen 5 cho C"
sleep 1

# Gui TX 3 den Node 3
python client.py --node localhost:5003 --op "C chuyen 3 cho D"
sleep 1

echo "=== 3. XEM LOG CUA NODE 1 DE XAC MINH CON SENT CHU KY ==="
docker-compose logs node1 | tail -n 25

echo "=== 4. XEM LEDGER TRONG ROCKSDB SITE 1 ==="
# Ta co the dung mot script Python nho de doc RocksDB truc tiep
python -c '
from storage.rocksdb_store import KVStore
import json
try:
    store = KVStore(1)
    print("Ledger tai Site 1:")
    for tx in store.get_ledger():
        print(f"  TX {tx[\"tx_id\"]}: {tx[\"data\"]}")
    print("Balance cuoi cung:")
    for acc in ["A", "B", "C", "D"]:
        print(f"  {acc}: {store.get_balance(acc)}")
    store.close()
except Exception as e:
    print("Loi:", e)
'

echo
echo "=== 5. DUNG CUM DOCKER COMPOSE ==="
docker-compose down
echo "=== DEMO PBFT HOAN THANH CO KET QUA TOT ==="
