# 📋 KẾ HOẠCH TRIỂN KHAI HỆ THỐNG BFT DISTRIBUTED LEDGER THỰC TẾ (CẬP NHẬT)

> **Mục tiêu:** Biến mô phỏng BFT đơn giản thành hệ phân tán chịu lỗi Byzantine (PBFT) chạy trên TCP thực, với Ed25519, RocksDB, Docker, client độc lập và kiểm thử tự động.

---

## 📁 Cấu trúc thư mục đích

```
bft-ledger/
├── config.py                 # Cấu hình tập trung (node, crypto, mạng)
├── crypto_utils.py           # Ed25519 (key, sign, verify, digest)
├── network/
│   ├── tcp_server.py         # TCP server với connection pool
│   └── tcp_client.py         # Gửi tin nhắn qua pool
├── storage/
│   ├── rocksdb_store.py      # RocksDB với WAL, state, ledger
│   └── world_state.py        # Quản lý số dư, replay logic
├── consensus/
│   ├── pbft.py               # PBFT (Pre‑prepare, Prepare, Commit, Checkpoint, View‑change)
│   └── view_change.py        # View‑change với bằng chứng
├── node.py                   # Entry point (vòng lặp xử lý sự kiện)
├── client.py                 # Gửi giao dịch, nhận reply
├── main.py                   # Orchestrator (khởi tạo cluster, chạy demo)
├── docker-compose.yml        # 4 node + network (có thể thêm client)
├── tests/
│   ├── test_crypto.py        # Unit tests
│   ├── test_network.py
│   ├── test_pbft.py
│   └── integration_test.py   # End‑to‑end với Docker
├── run_demo.sh               # Tự động hóa toàn bộ
└── requirements.txt
```

---

## 🧩 CÁC GIAI ĐOẠN THỰC HIỆN

### PHASE 0 – Chuẩn bị hạ tầng và các lớp cơ bản

**Mục tiêu:** Đảm bảo các module nền tảng hoạt động độc lập và có thể kiểm thử.

1. **Crypto (`crypto_utils.py`)** đã có, cần bổ sung:
   - Hàm `compute_digest(request_data: bytes) -> str` (SHA‑256 hex).
   - Hàm `hash_message(msg_dict) -> str` để tính digest của message (dùng khi cần đối chiếu).

2. **RocksDB (`storage/rocksdb_store.py`)** hiện có, cần bổ sung:
   - Hàng đợi ghi (thread‑safe) – dùng `threading.Lock` cho mọi thao tác ghi.
   - Lưu checkpoint state: key `state::checkpoint` chứa JSON `{seq: n, balances: {...}}`.
   - Hàm `save_checkpoint(seq, balances_dict)` và `load_checkpoint() -> (seq, balances)`.
   - Hàm `replay_wal_from(seq)` để áp dụng các giao dịch đã commit sau checkpoint.
   - Hàm `delete_old_wal(below_seq)` để giải phóng không gian.

3. **Mạng TCP** hiện có nhưng cần nâng cấp:
   - Thay vì mở socket mới mỗi lần gửi, triển khai `TCPConnectionPool` (mở socket đến mỗi node một lần, giữ liên tục, tự động reconnect khi mất).
   - `tcp_server.py` xử lý message đọc đến hết dòng (`\n`) và giữ kết nối mở (đã làm khá tốt).
   - Thêm heartbeat: mỗi node định kỳ gửi `PING`, nếu không nhận `PONG` trong timeout → coi node đó đã chết (phục vụ view‑change nhanh hơn).

---

### PHASE 1 – Hoàn thiện giao thức PBFT

**Mục tiêu:** Triển khai đầy đủ PBFT với digest, checkpoint và view‑change an toàn.

#### 1.1 Định dạng thông điệp PBFT (cập nhật `MsgType`)

```python
PRE_PREPARE = 101   # {type, view, seq, digest, request, sender}
PREPARE     = 102   # {type, view, seq, digest, sender}
COMMIT      = 103   # {type, view, seq, digest, sender}
CHECKPOINT  = 104   # {type, seq, digest, sender}
VIEW_CHANGE = 105   # {type, view, new_view, last_seq, prepared_certs, sender}
NEW_VIEW    = 106   # {type, view, new_view, V, O, sender}  (V = tập view‑change, O = tập pre‑prepare)
CLIENT_REQ  = 107   # {type, client_id, seq, operation, timestamp, signature}
CLIENT_REPLY= 108   # {type, view, seq, result, sender, signature}
```

> `digest` = SHA‑256 của request/operation; `request` chứa dữ liệu giao dịch.

#### 1.2 Máy trạng thái PBFT (viết trong `consensus/pbft.py`)

- **Khởi tạo:**
  - `view = 0`, `last_executed_seq = 0`, `stable_checkpoint_seq = 0`.
  - Ba tập tin `request_store`, `prepare_log`, `commit_log` theo key là `(seq, digest)`.
  - Sử dụng RocksDB để lưu các log này (column family `pbft_log`).

- **Xử lý request:**
  - Khi node là leader của view hiện tại, nhận `CLIENT_REQ` → gán `seq` mới → tạo `PRE_PREPARE` (gồm view, seq, digest của request) → broadcast.
  - Backup node nhận `PRE_PREPARE`: kiểm tra view, seq (không trùng lặp), digest khớp với request → chấp nhận → broadcast `PREPARE`.

- **Chuẩn bị và Commit:**
  - Khi nhận được `2f+1` `PREPARE` khớp (cùng view, seq, digest) → node broadcast `COMMIT`.
  - Khi nhận `2f+1` `COMMIT` khớp → **thực thi** request (cập nhật state, ghi ledger), gửi `CLIENT_REPLY` cho client.

- **Checkpoint:**
  - Sau mỗi `K` giao dịch (vd 100), node tính digest của trạng thái hiện tại và broadcast `CHECKPOINT ⟨seq, digest⟩`.
  - Khi nhận `2f+1` CHECKPOINT cùng seq/digest → đánh dấu `stable_checkpoint = seq`, xóa các log PBFT cũ (bên dưới seq) và cắt tỉa RocksDB WAL.

- **View‑change (viết trong `consensus/view_change.py`):**
  - Khi timeout (không có tiến triển), node broadcast `VIEW_CHANGE` gửi kèm:
    - `last_seq` đã thực thi.
    - Tập hợp các `⟨PREPARE⟩` message đã nhận cho các request chưa commit (từ `last_seq+1` trở lên) làm bằng chứng (`prepared_certs`).
  - Leader mới (`new_view = v+1`) chờ `2f+1` VIEW_CHANGE. Sau đó:
    - Xác định tập `O` các request đã được chuẩn bị (có ít nhất `2f+1` PREPARE trong tập bằng chứng).
    - Phát `NEW_VIEW` chứa danh sách VIEW_CHANGE nhận được và tập `O` (các PRE‑PREPARE tương ứng).
    - Sau khi các node nhận NEW_VIEW, chúng cập nhật view mới và replay các request trong `O` nếu chưa thực thi.

> **Lưu ý:** Toàn bộ thông điệp được ký Ed25519 trước khi gửi.

#### 1.3 Tích hợp với RocksDB

- Column family `pbft_log`: lưu các message quan trọng (PRE_PREPARE, PREPARE, COMMIT) để khôi phục view‑change sau crash.  
- Khi node khởi động: nạp checkpoint state từ `state::checkpoint`, replay các giao dịch sau checkpoint bằng cách đọc `ledger` hoặc `pbft_log`.

---

### PHASE 2 – Kết nối mạng bền vững & mô phỏng điều kiện thực

**Mục tiêu:** Thay thế kiểu mở socket tạm bằng pool kết nối duy trì, thêm heartbeat.

1. **TCP Connection Pool (`network/connection_pool.py`):**
   - Khi khởi động, mỗi node mở socket đến tất cả các node khác. Nếu kết nối thất bại, định kỳ thử lại.
   - Các hàm `send_msg(dst, msg)` gửi qua kết nối có sẵn; nếu mất, chuyển sang hàng đợi chờ.

2. **Heartbeat:**
   - Mỗi 1 giây gửi `PING`, đối tác phải trả lời `PONG`. Nếu sau 3 giây không nhận được → đánh dấu node đó `suspected`, có thể kích hoạt view‑change nếu đó là leader.

3. **Mô phỏng mạng:**
   - Trong `docker-compose.yml`, thêm `cap_add: NET_ADMIN` cho mỗi service.
   - Cung cấp script `simulate_network.sh` dùng `tc` để thêm delay/packet loss theo tỉ lệ.

---

### PHASE 3 – Client độc lập

**Mục tiêu:** Tạo một chương trình client gửi giao dịch và xác minh kết quả.

1. `client.py`:
   - Nhận tham số dòng lệnh: `--node <host:port>`, `--key <private_key_hex>`, `--op "A chuyen 10 cho B"`.
   - Tạo `CLIENT_REQ` message (kèm timestamp, client_id), ký bằng private key.
   - Gửi request đến node chỉ định (có thể thử đến khi tìm đúng leader – node sẽ trả lời redirect nếu không phải leader).
   - Chờ `CLIENT_REPLY` từ các node. Khi nhận đủ `f+1` reply giống nhau (kết quả và chữ ký hợp lệ) → in ra thành công.

2. Node xử lý `CLIENT_REQ`:
   - Kiểm tra chữ ký, giao dịch hợp lệ.
   - Nếu không phải leader: trả lời `REDIRECT` chứa địa chỉ leader hiện tại.
   - Sau khi thực thi, gửi `CLIENT_REPLY` đã ký.

---

### PHASE 4 – Docker hóa và tự động hóa kiểm thử

**Mục tiêu:** Đóng gói, chạy được với một lệnh, và có kịch bản kiểm thử tích hợp.

1. **Dockerfile:** đã có, cần đảm bảo cài đúng dependencies (`rocksdict`, `cryptography`).
2. **docker-compose.yml:**  
   - 4 dịch vụ `node0..node3`, mạng bridge tĩnh.  
   - Service `client` (tùy chọn) có thể chạy script `run_client.sh`.  
   - Cấu hình healthcheck cho mỗi node dựa trên heartbeat.
3. **Script `run_demo.sh`:**
   ```bash
   docker-compose up -d
   sleep 5  # chờ các node khởi động
   python client.py --node node0:5000 --op "A chuyen 10 cho B"
   python client.py --node node1:5001 --op "B chuyen 5 cho C"
   # ... kiểm tra ledger từ container
   docker-compose logs node0
   ```
4. **Testing (`tests/integration_test.py`):**
   - Sử dụng `pytest` + `docker` Python SDK để:
     - Khởi tạo cluster.
     - Gửi giao dịch qua client, kiểm tra ledger đồng nhất.
     - Mô phỏng crash (`docker kill node2`) → khởi động lại → kiểm tra dữ liệu sau phục hồi.
     - Mô phỏng Byzantine (gửi message sai từ node0) → kiểm tra an toàn.
     - Mô phỏng network partition bằng cách tạm ngắt mạng của node0, sau đó khôi phục.

---

### PHASE 5 – Tài liệu và đóng gói

- Cập nhật `README.md`: mô tả kiến trúc, cách chạy, các kịch bản lỗi.
- Viết `ARCHITECTURAL_DECISIONS.md` (nếu cần) giải thích lý do chọn PBFT, RocksDB, Ed25519.
- Tạo `Makefile` để build, test, clean.
- Đảm bảo `requirements.txt` đầy đủ.

---

## ✅ TIÊU CHÍ HOÀN THÀNH

| Tiêu chí | Mô tả |
|----------|-------|
| **Mạng thực** | Các node giao tiếp qua TCP với connection pool, heartbeat |
| **Bảo mật** | Mọi thông điệp được ký Ed25519, xác minh chữ ký trước khi xử lý |
| **Đồng thuận** | PBFT đầy đủ: Pre‑prepare, Prepare, Commit, Checkpoint, View‑change an toàn |
| **Lưu trữ** | RocksDB với WAL, state checkpoint, replay khôi phục sau crash |
| **Khách hàng** | Client ký giao dịch, gửi đến node, nhận reply từ ≥ f+1 node |
| **Docker** | Cluster chạy trong Docker Compose, có thể mở rộng số node |
| **Kiểm thử** | Unit tests, integration tests tự động với các kịch bản lỗi |
| **Demo** | Chạy `run_demo.sh` hoàn chỉnh |
