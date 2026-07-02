# Báo cáo Kỹ thuật: Hệ thống PBFT Distributed Ledger thực tế

**Môn học:** Cơ sở dữ liệu phân tán  
**Mã sinh viên:** N23DCCN132  
**Họ tên:** Phạm Thành Nhựt Trọng  

---

## Chương I: Đặt vấn đề và Động lực nâng cấp

### 1.1 Giới hạn của mô hình mô phỏng lý thuyết ban đầu

Phiên bản đầu tiên của đề tài *"Simplified BFT cho Distributed Ledger"* được xây dựng dưới dạng mô phỏng in-memory thông qua thư viện `multiprocessing`:

- Các node giao tiếp bằng hàng đợi dùng chung bộ nhớ (`multiprocessing.Queue`).
- Chữ ký số được mô phỏng bằng chuỗi text thô dạng `"SIG_SITE_X"`.
- WAL chỉ là tệp JSON tuần tự đơn giản.
- Giao thức đồng thuận không tuân thủ đặc tả Castro & Liskov: thiếu digest check, thiếu watermarks, View Change không chạy lại 3 pha.

Mô hình này tuy làm nổi bật được logic cơ bản nhưng chưa phản ánh đúng bản chất của hệ phân tán thực tế, và đặc biệt **không đảm bảo thuộc tính Safety và Liveness** theo lý thuyết PBFT.

### 1.2 Mục tiêu nâng cấp

Hệ thống được nâng cấp toàn diện lên một **Replicated Distributed Ledger Engine** với các công nghệ cốt lõi:

- **Networking:** TCP Sockets thực, TCP Connection Pool, Heartbeat PING/PONG.
- **Cryptography:** Chữ ký số Ed25519 thực, hàm băm SHA-256.
- **Storage:** RocksDB (LSM-Tree) với WAL, Ledger, Checkpoint.
- **Protocol:** PBFT đúng đặc tả Castro-Liskov: Digest check, Watermarks, View Change 3 pha, Null Request, Exponential Backoff có giới hạn, Client Idempotency.

---

## Chương II: Kiến trúc hệ thống phân tán

Hệ thống được phân chia thành 4 tầng rõ rệt:

```
                  ┌────────────────────────────────┐
                  │  Client (client.py / client_tui.py) │
                  └──────────────┬─────────────────┘
                                 │ Ký Ed25519 → gửi CLIENT_REQUEST
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│ TẦNG GIAO TIẾP MẠNG                                          │
│  - TCPServer: buffer JSON bằng delimiter \\n                  │
│  - Connection Pool: socket TCP mở liên tục, health-check \\n │
│  - Heartbeat PING/PONG: phát hiện leader chết                │
└───────────────────────────────┬──────────────────────────────┘
                                │ Đưa vào incoming_queue
                                ▼
┌──────────────────────────────────────────────────────────────┐
│ TẦNG ĐỒNG THUẬN PBFT                                         │
│  - PRE-PREPARE: Leader gán seq, broadcast kèm Digest         │
│  - PREPARE: Backup kiểm tra Digest + Watermark, broadcast    │
│  - COMMIT: Khi 2f+1 PREPARE → broadcast COMMIT              │
│  - Quorum 2f+1 COMMIT → Execute + Checkpoint                 │
│  - View Change: Prepared Certs + NEW-VIEW + NOOP + Backoff   │
└───────────────────────────────┬──────────────────────────────┘
                                │ Cập nhật trạng thái
                                ▼
┌──────────────────────────────────────────────────────────────┐
│ TẦNG LƯU TRỮ CỤC BỘ (RocksDB LSM-Tree)                      │
│  - wal::<tx_id>::<seq>   — Write-Ahead Log                   │
│  - state::<account>      — World State (số dư tài khoản)     │
│  - ledger::<tx_id>       — Sổ cái giao dịch đã commit        │
│  - pbft_log::<...>       — PBFT message logs (prune được)    │
│  - state::checkpoint     — State snapshot định kỳ            │
└──────────────────────────────────────────────────────────────┘
```

---

## Chương III: Giao thức PBFT tuân thủ Castro-Liskov

### 3.1 Quy trình đồng thuận 3 pha

```
Client                Leader (Node 0)          Backup (Node 1,2,3)
  │──CLIENT_REQUEST──▶│                                │
  │                   │──PRE-PREPARE(view,seq,digest)─▶│
  │                   │                    ┌───────────┤
  │                   │◀──PREPARE(digest)──┤ verify    │
  │                   │──PREPARE──────────▶│ digest    │
  │                   │                    └───────────┤
  │          (2f+1 PREPARE đồng thuận)                 │
  │                   │──COMMIT(digest)───────────────▶│
  │                   │◀──COMMIT──────────────────────┤
  │          (2f+1 COMMIT đồng thuận)                  │
  │◀──CLIENT_REPLY────│ execute + ledger               │ execute
```

**Mỗi pha đều kiểm tra:**
- `msg["view"] == self.view` — từ chối message cũ
- `msg["digest"] == self.requests[seq]["digest"]` — từ chối message sai digest
- `seq` nằm trong cửa sổ `[low_water_mark, high_water_mark]`

### 3.2 Phát hiện Equivocation

Khi Leader Byzantine gửi 2 PRE-PREPARE khác nhau cùng một `seq`:
```
Node 1,2 nhận: PRE-PREPARE(seq=5, digest=d1)
Node 3 nhận:   PRE-PREPARE(seq=5, digest=d2)
```

**Node 3:** khi nhận PREPARE với `digest=d1` từ Node 1 → `d1 ≠ d2` → drop ngay.
**Node 3:** khi nhận PRE-PREPARE với `digest=d2` nhưng `seq=5` đã có `digest=d1` → Equivocation detected → kích hoạt View Change ngay lập tức (`force=True`).

Kết quả: **không node trung thực nào đạt quorum cho giao dịch giả mạo** → Safety được bảo đảm.

### 3.3 View Change chuẩn

```
Backup timeout           Backup (Node 1)        Leader mới (Node 2)
      │──VIEW-CHANGE(new_view=1, prepared_certs)──▶│
      │◀─────────────────────────────────────────  │ thu thập 2f+1
      │◀──────────NEW-VIEW(V={...}, O={...})───────│
      │                                            │
      │ nhận NEW-VIEW, _apply_new_view(1, O)        │ _apply_new_view(1, O)
      │ cập nhật view=1, chờ PRE-PREPARE           │ gọi _send_pre_prepare cho từng seq trong O
      │                                            │──PRE-PREPARE(view=1, seq, digest)──▶Node 1
```

**Điểm quan trọng:**
- Leader mới **không execute trực tiếp** tập O → phải chạy lại 3 pha
- Nếu O rỗng → gửi **Null Request (NOOP)** để chứng minh liveness
- Timeout tăng theo công thức: `base × min(2^view, 8)` (giới hạn 8× để tránh hệ thống đóng băng)

### 3.4 Checkpoint và Cắt tỉa WAL

Mỗi K=2 giao dịch:
1. Tính SHA-256 của World State → broadcast `CHECKPOINT(seq, digest)`
2. Khi 2f+1 node cùng vote → **Stable Checkpoint**
3. Cắt tỉa WAL và PBFT logs dưới `seq`
4. Tịnh tiến watermarks: `low = seq, high = seq + 100`

### 3.5 Crash Recovery

```
Node 1 restart
    │
    ├── Load Checkpoint từ RocksDB (seq, balances)
    ├── Restore World State về trạng thái tại checkpoint
    └── Replay WAL: chỉ các entry có state=COMMIT và seq > checkpoint_seq
        └── Kiểm tra tx_id đã có trong Ledger → skip duplicate
```

### 3.6 Client Idempotency

Server cache `client_id → (timestamp, reply)`. Nếu client gửi lại request với `timestamp ≤ cached_timestamp` → trả lại reply cũ mà không chạy lại 3 pha. Ngăn chặn hoàn toàn tình huống "trừ tiền 2 lần" do network retry.

---

## Chương IV: Thực nghiệm và Kết quả

### 4.1 Kịch bản kiểm thử tích hợp (Happy Path + Redirect)

**Kịch bản:**
1. Khởi chạy 4 node PBFT TCP (Node 0 = Leader View 0)
2. Client gửi giao dịch `"A chuyen 10 cho B"` đến Node 1 (Backup)
3. Xác minh Node 1 Redirect về Leader Node 0
4. Xác minh tất cả node cập nhật đúng số dư

**Kết quả thực tế:**
```
[Client] Redirect: Node thong bao gui ve Leader moi (Node 0). Gui lai...
[Client] Nhan SUCCESS tu Node 2!
[Client] Nhan SUCCESS tu Node 1!
[Client] Giao dich hoan thanh thanh cong! Cac node: [1, 2]

Node 0 - Balance A: 90, B: 110, Ledger: 1 txs  ✓
Node 1 - Balance A: 90, B: 110, Ledger: 1 txs  ✓
Node 2 - Balance A: 90, B: 110, Ledger: 1 txs  ✓
Node 3 - Balance A: 90, B: 110, Ledger: 1 txs  ✓
```

**Phân tích:**
- **Redirect:** Node 1 (Backup) từ chối và chuyển hướng đúng Leader.
- **Đồng nhất:** Tất cả 4 node lưu cùng trạng thái — thuộc tính Safety được giữ vững.
- **Ed25519:** Chữ ký client xác thực thành công, chữ ký reply xác thực thành công tại client.

### 4.2 Kịch bản Equivocation (Byzantine Leader)

**Thiết lập:** Node 0 chạy chế độ `is_malicious=True`:
- Gửi PRE-PREPARE(digest=d1) cho Node 1, 2
- Gửi PRE-PREPARE(digest=d2, FAKE BYZANTINE) cho Node 3

**Kết quả:**
- Node 3 nhận PREPARE với digest=d1 từ Node 1 → drop (`PBFT_DIGEST_MISMATCH`)
- Node 3 phát hiện Equivocation → kích hoạt View Change
- Hệ thống chuyển sang View 1, Leader = Node 1
- Giao dịch giả mạo không được thực thi trên bất kỳ node nào

### 4.3 Kịch bản Crash Recovery

**Thiết lập:** Tắt Node 1 giữa chừng, gửi thêm giao dịch, khởi động lại Node 1.

**Kết quả:**
- Hệ thống còn 3 node → vẫn đạt Quorum 2f+1=3 → tiếp tục commit bình thường
- Node 1 khởi động lại: load checkpoint, replay WAL → số dư đồng nhất với hệ thống
- Không có duplicate ledger entry (tx_id check trong replay_wal_from)

---

## Chương V: Code Review và Phân tích chất lượng

Hệ thống đã trải qua đợt code review đa trục (5 axes) theo chuẩn **code-review-and-quality**. Kết quả:

### 5.1 Correctness

| Vấn đề | Trạng thái |
|---|---|
| Digest check trong PREPARE/COMMIT | ✅ Đúng — drop ngay khi sai |
| Equivocation detection | ✅ Đúng — kích hoạt force View Change |
| Watermark check | ✅ Đúng — reject PRE-PREPARE ngoài cửa sổ |
| NOOP tăng last_executed_seq | ✅ Đúng |
| Exponential backoff quá lớn | ✅ Đã sửa — cap tại 8× base |
| sign_message mutate shared reply dict | ✅ Đã sửa — shallow copy trước khi gửi |
| NOOP client_id "system" pollution | ✅ Đã sửa — dùng `"__noop__"` |
| Dead attribute `view_changes_received` | ✅ Đã xóa |

### 5.2 Architecture

| Vấn đề | Trạng thái |
|---|---|
| `storage/__init__.py` export world_state | ✅ Sạch — không có |
| Dead reference `world_state.py` trong docs | ✅ Đã xóa khỏi README |
| net_send/net_broadcast call sites | ✅ Tất cả đúng signature |
| `get_ledger()` return type | ✅ list of dict, `.get()` hợp lệ |

### 5.3 Security (trong phạm vi academic demo)

| Vấn đề | Mức độ |
|---|---|
| Client TUI thiếu whitelist validate account | Low — có thể gây parse error |
| Private keys hardcode | Acceptable — intentional for demo |
| TCP buffer không giới hạn kích thước message | Low — không phải issue thực tế ở scale demo |

---

## Kết luận

Hệ thống đã được nâng cấp từ mô phỏng đơn giản lên một PBFT Distributed Ledger Engine với độ trung thực cao đối với đặc tả Castro-Liskov. Các thuộc tính cốt lõi của hệ phân tán BFT đều được đảm bảo:

- **Safety:** Digest check + Watermarks + Equivocation detection.
- **Liveness:** View Change đúng 3 pha + NOOP + Exponential Backoff có giới hạn.
- **Durability:** RocksDB WAL + Checkpoint + Crash Recovery chống duplicate.
- **Authenticity:** Ed25519 ký mọi thông điệp + Client Idempotency.

Hệ thống đủ điều kiện làm nền tảng cho các nghiên cứu sâu hơn về hệ phân tán Byzantine fault tolerant trong môi trường thực tế.
