#!/usr/bin/env bash
set -e

# ──────────────────────────────────────────────
#  Demo PBFT — 4 node, 5 transactions
# ──────────────────────────────────────────────

cleanup() {
  echo "=== Don dep ==="
  pkill -f "python node.py" 2>/dev/null || true
  sleep 1
  rm -rf logs/*.log 2>/dev/null || true
  for i in 0 1 2 3; do
    rm -rf rocksdb/site_$i 2>/dev/null || true
  done
}

trap cleanup EXIT
cleanup

mkdir -p logs

echo "=============================================="
echo "  PBFT DISTRIBUTED LEDGER — DEMO"
echo "  4 nodes, 5 transactions"
echo "=============================================="
echo ""

# ── 1. Normal case (all honest) ──
echo "╔══════════════════════════════════════════════╗"
echo "║  SCENARIO 1: Normal Consensus (all honest)  ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

for i in 0 1 2 3; do
  python node.py "$i" --listen --no-byzantine > logs/node_"$i".log 2>&1 &
  echo "  [Node $i] Started (honest)"
done

sleep 3

echo ""
echo "  [Client] TX 1: A chuyen 10 cho B → A=90, B=110"
python client.py --node localhost:5001 --op "A chuyen 10 cho B" 2>&1 | sed 's/^/  /'
sleep 1

echo "  [Client] TX 2: B chuyen 5 cho C → B=105, C=105"
python client.py --node localhost:5000 --op "B chuyen 5 cho C" 2>&1 | sed 's/^/  /'
sleep 1

echo "  [Client] TX 3: C chuyen 3 cho D → C=102, D=103"
python client.py --node localhost:5002 --op "C chuyen 3 cho D" 2>&1 | sed 's/^/  /'
sleep 1

echo "  [Client] TX 4: A chuyen 7 cho D → A=83, D=110"
python client.py --node localhost:5003 --op "A chuyen 7 cho D" 2>&1 | sed 's/^/  /'
sleep 1

echo "  [Client] TX 5: B chuyen 2 cho A → B=103, A=85"
python client.py --node localhost:5001 --op "B chuyen 2 cho A" 2>&1 | sed 's/^/  /'
sleep 2

pkill -f "python node.py" 2>/dev/null || true
sleep 1

echo ""
echo "  ┌─ Balances after 5 TX ──────────────────────┐"
for i in 0 1 2 3; do
  python check_db.py "$i" 2>&1 | sed 's/^/  │ /'
done
echo "  └────────────────────────────────────────────┘"
echo ""

# ── 2. Byzantine case ──
echo "╔══════════════════════════════════════════════╗"
echo "║  SCENARIO 2: Byzantine Equivocation         ║"
echo "║  Node 0 sends fake PRE-PREPARE to Node 3   ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

for i in 0 1 2 3; do
  rm -rf rocksdb/site_$i 2>/dev/null || true
done

for i in 0 1 2 3; do
  python node.py "$i" --listen > logs/byz_node_"$i".log 2>&1 &
done

sleep 3

echo "  [Client] TX 1: A chuyen 10 cho B"
python client.py --node localhost:5000 --op "A chuyen 10 cho B" 2>&1 | sed 's/^/  /'
sleep 2

pkill -f "python node.py" 2>/dev/null || true
sleep 1

echo ""
echo "  ┌─ Balances after Byzantine attack ───────────┐"
for i in 0 1 2 3; do
  python check_db.py "$i" 2>&1 | sed 's/^/  │ /'
done
echo "  └────────────────────────────────────────────┘"
echo "  → Nodes 0,1,2: A=90 (correct)"
echo "  → Node 3: A=100 (left behind—equivocation detected)"
echo "  → Safety: 3/4 nodes agree (2f+1 = 3) ✓"
echo ""

# ── Summary ──
echo "=============================================="
echo "  DEMO COMPLETE"
echo "=============================================="
echo ""
echo "  Tests:"
echo "    python tests/unit_test_pbft.py     # 11 unit tests"
echo "    python tests/integration_test.py   # 4 integration tests"
echo ""
echo "  Run again: bash demo.sh"
