# BFT Distributed Ledger — PBFT + RocksDB + TCP + Ed25519

Hệ thống sổ cái phân tán chịu lỗi Byzantine (Practical Byzantine Fault Tolerance — PBFT) chạy trên mạng TCP thực, tích hợp chữ ký số Ed25519, cơ sở dữ liệu RocksDB (WAL, State, Ledger), cơ chế Checkpoint tự động, View Change với Prepared Certs, và client giao dịch độc lập.

Hệ thống được xây dựng để phục vụ mục đích nghiên cứu và học thuật trong môn **Cơ sở dữ liệu phân tán**, có độ trung thực cao với đặc tả gốc của Castro & Liskov (1999).

---

## Tính năng nổi bật

| Tính năng | Mô tả |
|---|---|
| **PBFT 3 pha đầy đủ** | Pre-prepare, Prepare, Commit với kiểm tra Digest và Watermark nghiêm ngặt |
| **Chống Equivocation** | Phát hiện Leader Byzantine gửi Digest mâu thuẫn → kích hoạt View Change ngay lập tức |
| **Chữ ký số Ed25519** | Mọi thông điệp giữa node và client đều được ký và xác thực trước khi xử lý |
| **TCP Connection Pool** | Duy trì socket TCP mở liên tục, tự động kết nối lại khi mất mạng |
| **Lưu trữ RocksDB** | WAL, World State, Ledger, PBFT logs, Checkpoint được lưu trên đĩa |
| **Checkpoint + Pruning** | Chụp nhanh trạng thái mỗi K=2 giao dịch, cắt tỉa WAL cũ khi Stable Checkpoint đạt Quorum |
| **Crash Recovery** | Khởi động lại từ Checkpoint + Replay WAL, chống duplicate ledger entries |
| **View Change chuẩn** | VIEW-CHANGE kèm Prepared Certs, Leader mới phát lại PRE-PREPARE cho tập O |
| **Exponential Backoff** | Timeout View Change tăng dần (tối đa 8x) để tránh vòng lặp liên tục khi mạng nghẽn |
| **Client Idempotent** | Cache kết quả phía server, Client gửi lại request cũ nhận lại reply đã lưu |
| **Null Request (NOOP)** | Leader mới phát NOOP khi tập O rỗng để chứng minh liveness |
| **High/Low Watermarks** | Giới hạn cửa sổ Sequence Number, ngăn chặn Leader spam seq số rất lớn |
| **TUI Client** | Giao diện nhập liệu tương tác (`client_tui.py`), tránh lỗi typo khi demo |

---

## Cấu trúc thư mục

```
csdlpt/
├── config.py                 # Cấu hình chung: node addresses, crypto keys, MsgType enum
├── crypto_utils.py           # Ed25519 sign/verify thực, SHA-256 Digest
├── logger.py                 # Structured logger ghi ra file log và console
│
├── network/
│   ├── __init__.py           # Helper: sign_message, verify_signature, net_send, net_broadcast
│   ├── tcp_server.py         # TCP Server lắng nghe, buffer từng JSON line bằng delimiter \\n
│   ├── tcp_client.py         # TCP Client gửi thông điệp đơn lẻ (fallback)
│   └── connection_pool.py    # TCP Connection Pool duy trì socket liên tục, health-check bằng \\n
│
├── storage/
│   ├── __init__.py           # Export KVStore
│   └── rocksdb_store.py      # RocksDB backend: WAL, State, Ledger, Checkpoint, PBFT logs
│
├── consensus/
│   ├── __init__.py
│   ├── pbft.py               # PBFT Engine: 3 pha, Watermarks, Digest check, Client cache
│   └── view_change.py        # View Change Manager: Prepared Certs, NEW-VIEW, NOOP, Backoff
│
├── node.py                   # Node entry point: khởi động TCP server, PBFT, Heartbeat
├── client.py                 # Client giao dịch dòng lệnh (CLI)
├── client_tui.py             # Client giao dịch tương tác (TUI) — tránh typo khi demo
├── check_db.py               # Script kiểm tra trực tiếp RocksDB trên đĩa
├── main.py                   # Orchestrator chạy demo mô phỏng localhost
│
├── Dockerfile                # Docker image cho node
├── docker-compose.yml        # Khởi chạy cluster 4 node qua Docker Compose
├── simulate_network.sh       # Script tc-netem giả lập mất gói và độ trễ mạng
├── demo.sh                   # Script demo kịch bản thủ công
├── run_demo.sh               # Script 1-Click: reset, start Docker, gửi giao dịch
├── Makefile                  # Tự động hóa: build, test, clean, up, down, demo
└── tests/
    ├── test_crypto.py        # Unit tests: sign/verify, compute_digest
    └── integration_test.py   # Integration test: Happy Path, Redirect, RocksDB consistency
```

---

## Hướng dẫn chạy hệ thống

### 1. Cài đặt môi trường

Yêu cầu Python 3.8+ và các gói trong `requirements.txt`:
```bash
pip install -r requirements.txt
```

### 2. Chạy demo mô phỏng cục bộ (Localhost)

Chạy nhanh 4 node trên cùng máy, tự động thực hiện giao dịch và xác minh:
```bash
python main.py
# Hoặc dùng Makefile:
make test
```

### 3. Chạy bằng Docker Compose

Khởi động 4 container node lắng nghe TCP liên tục:
```bash
make build    # Build Docker images
make up       # Khởi chạy cluster 4 node
```

Gửi giao dịch qua CLI client:
```bash
python client.py --node localhost:5001 --op "A chuyen 10 cho B"
```

Gửi giao dịch qua TUI client (khuyến nghị khi demo):
```bash
python client_tui.py
```

Tắt cluster:
```bash
make down
```

### 4. Kiểm tra trực tiếp dữ liệu RocksDB

```bash
python check_db.py
```

### 5. Demo 1-Click tự động

Script `run_demo.sh` sẽ reset DB, khởi chạy Docker, gửi 3 giao dịch và in ledger cuối:
```bash
make demo
```

---

## Kịch bản kiểm thử tích hợp

File `tests/integration_test.py` tự động kiểm thử:

1. Reset RocksDB và logs.
2. Spawn 4 nodes lắng nghe TCP.
3. Client gửi `CLIENT_REQUEST` đến Node 1 (Backup), xác minh Node 1 tự động `REDIRECT` về Leader Node 0.
4. Xác minh client nhận đủ f+1 = 2 phiếu `SUCCESS` hợp lệ và commit thành công.
5. Tắt nodes, đọc trực tiếp RocksDB tất cả sites để kiểm tra tính nhất quán (A: 90, B: 110, Ledger: 1 giao dịch).

```bash
make test
```

---

## Thông số hệ thống

| Thông số | Giá trị |
|---|---|
| Số node | N = 4 |
| Số node lỗi tối đa chịu được | f = 1 |
| Quorum Prepare/Commit | 2f+1 = 3 |
| Quorum Checkpoint | 2f+1 = 3 |
| Checkpoint interval | K = 2 giao dịch |
| High Watermark | low + 100 sequences |
| View Change base timeout | 2 × TIMEOUT |
| View Change max backoff | 8 × base timeout |

---

## Lý thuyết nền tảng

Hệ thống tuân thủ các thuộc tính cốt lõi của PBFT:

- **Safety**: Không bao giờ có 2 node trung thực lưu trữ 2 kết quả khác nhau cho cùng một Sequence Number. Đảm bảo bởi Digest check nghiêm ngặt ở mọi pha và phát hiện Equivocation.
- **Liveness**: Hệ thống luôn tiến lên phía trước. Đảm bảo bởi View Change, Null Request (NOOP) và Exponential Backoff.
- **Byzantine Fault Tolerance**: Chịu được tối đa f = ⌊(N-1)/3⌋ node độc hại hoặc bị crash đồng thời, với N ≥ 3f+1 node.
