# Quyết định Kiến trúc: PBFT Distributed Ledger

Tài liệu này ghi lại các quyết định kiến trúc cốt lõi, lý do lựa chọn công nghệ và các giải pháp kỹ thuật được áp dụng trong hệ thống **BFT Distributed Ledger** — một Replicated State Machine chịu lỗi Byzantine chạy trên mạng TCP thực.

---

## 1. Tại sao PBFT + RocksDB thay vì PostgreSQL Replication?

Các hệ thống CSDL truyền thống như PostgreSQL Streaming Replication chỉ hỗ trợ mô hình lỗi **CFT (Crash Fault Tolerance)**. Chúng giả định tất cả node là trung thực và chỉ có thể bị crash — không thể xử lý node độc hại gửi dữ liệu sai lệch (**Byzantine node**).

**Vấn đề của PostgreSQL trong môi trường Byzantine:**

- Nếu Master bị compromise, nó có thể gửi hai bản ghi WAL mâu thuẫn nhau cho hai nhóm Standby khác nhau (Equivocation).
- Không có cơ chế đồng thuận đa bên (Multi-party Consensus) để phát hiện mâu thuẫn này → Split-brain ngay lập tức.

**Lý do chọn RocksDB thay vì PostgreSQL làm local storage:**

- Cấu trúc **LSM-Tree** tối ưu hóa tốc độ ghi WAL tuần tự và PBFT logs.
- Cho phép can thiệp sâu vào tầng lưu trữ: lưu Checkpoint, Replay WAL, cắt tỉa log cũ — độc lập với tầng đồng thuận.
- Không có overhead của SQL parser, query planner, transaction isolation — giao dịch PBFT đã cung cấp serializable ordering ở tầng trên.

> **Kết luận:** Kiến trúc **Custom PBFT Consensus + RocksDB** là cách duy nhất để đạt được Byzantine Fault Tolerance thực sự (3f+1) mà không cần wrapper phức tạp bên ngoài PostgreSQL.

---

## 2. Quyết định giao thức: Tuân thủ đặc tả Castro & Liskov (1999)

### 2.1 Kiểm tra Digest nghiêm ngặt ở mọi pha

**Vấn đề gốc:** `_handle_prepare` và `_handle_commit` chỉ đọc `digest` từ message nhưng không so sánh với digest đã lưu. Một node Byzantine có thể gửi PREPARE với digest khác và vẫn được đếm vào quorum → Safety violation.

**Giải pháp:** Mọi message Prepare/Commit đến sẽ bị drop ngay lập tức nếu `msg["digest"] != self.requests[seq]["digest"]`. Phát hiện Equivocation (cùng seq, 2 digest khác nhau ở PRE-PREPARE) → kích hoạt View Change ngay lập tức bằng `force=True`.

```python
if digest != self.requests[seq].get("digest"):
    self.log.warning("PBFT_DIGEST_MISMATCH", ...)
    return
```

### 2.2 View Change phải chạy lại 3 pha cho tập O

**Vấn đề gốc:** `_apply_new_view` gọi trực tiếp `_execute_request(tx, seq)` — bỏ qua hoàn toàn 3 pha PRE-PREPARE → PREPARE → COMMIT. Đây là vi phạm nghiêm trọng thuộc tính **Safety**: một Backup có thể gửi giao dịch chưa được đồng thuận đầy đủ vào tập O.

**Giải pháp:** Leader mới **không được** execute trực tiếp. Thay vào đó, Leader mới phát lại `_send_pre_prepare(tx, seq)` cho từng giao dịch trong tập O. Các Backup chỉ cập nhật view và chờ PRE-PREPARE mới từ Leader.

### 2.3 Null Request (NOOP) đảm bảo Liveness

Khi tập O rỗng (không có giao dịch đang dang dở), Leader mới vẫn phải phát một giao dịch rỗng (NOOP) để:
1. Chứng minh nó đang sống và là Leader hợp lệ.
2. Giúp các node tụt hậu cập nhật Sequence Number.

### 2.4 Sequence Number Watermarks

**Vấn đề:** Không có giới hạn Sequence Number cho phép Leader Byzantine spam seq rất lớn, gây tràn bộ nhớ.

**Giải pháp:** `SequenceManager` quản lý `low_water_mark` và `high_water_mark` (cửa sổ 100 sequence). PRE-PREPARE với seq ngoài cửa sổ bị từ chối. Watermarks tịnh tiến khi đạt Stable Checkpoint.

```
[low_water_mark ... high_water_mark] — cửa sổ hợp lệ (100 slots)
```

---

## 3. Quyết định thiết kế kỹ thuật đã triển khai

### 3.1 TCP Connection Pool — Mạng bền vững

**Quyết định:** Duy trì socket TCP mở liên tục thay vì tạo mới cho mỗi message.

**Cơ chế:**
- Mỗi node spawn N-1 thread background `_keep_connected`, mỗi thread quản lý 1 socket.
- Health-check bằng cách gửi `b'\n'` (newline) định kỳ. Gửi empty byte `b''` là no-op ở Python — không sinh TCP packet → không phát hiện được Broken Pipe.
- Server phía nhận xử lý: `if not line: continue` → newline rỗng bị bỏ qua an toàn.
- Reconnect với Exponential Backoff: `min(2.0 * (1 + failures), 10.0)`.

**Đánh đổi:** Với N=4, mỗi node tạo 3 thread. Cần cân nhắc khi scale lên N ≥ 50.

### 3.2 Threading Model — RLock thay vì Lock

**Quyết định:** Sử dụng `threading.RLock` (Reentrant Lock) cho toàn bộ PBFT Engine.

**Lý do:** PBFT là giao thức đa pha đồng thời. Trong quá trình kiểm tra Quorum Prepare, thread xử lý chính cần gọi tiếp hàm gửi Commit và cập nhật state trong cùng lock context. `threading.Lock` gây **Self-Deadlock** (thread tự khóa chính mình). `RLock` cho phép acquire nhiều lần trên cùng thread an toàn.

### 3.3 Client Idempotency Cache

**Vấn đề:** Mạng chập chờn khiến Client retry request đã thực thi → trừ tiền 2 lần.

**Giải pháp:** Server lưu cache `client_id → (timestamp, reply)`. Nếu nhận request với `timestamp ≤ cached_timestamp`, trả lại reply cũ mà không chạy lại PBFT. Cache được lưu trong memory (không persist — phù hợp với academic demo; production cần RocksDB).

### 3.4 Checkpoint + WAL Pruning

**Cơ chế:** Mỗi K=2 giao dịch, node tính SHA-256 digest của World State và broadcast `CHECKPOINT`. Khi đạt 2f+1 vote cùng digest → Stable Checkpoint → cắt tỉa WAL và PBFT logs cũ → Watermark tịnh tiến.

**Crash Recovery:** Node khởi động lại đọc Checkpoint từ RocksDB → restore balances → Replay WAL (chỉ các entry có `state=COMMIT` và `seq > checkpoint_seq`). Kiểm tra `tx_id` đã có trong Ledger trước khi Replay để tránh duplicate.

### 3.5 View Change Exponential Backoff với giới hạn

**Công thức:** `timeout = base_timeout × min(2^view, 8)`

**Lý do giới hạn:** Không giới hạn thì tại view=10, timeout = base × 1024 (hơn 17 phút với base=1s). Điều này gây mất liveness nếu hệ thống trải qua nhiều View Changes liên tiếp. Giới hạn tối đa 8× (tức là base × 8) vừa đủ để chống vòng lặp View Change liên tục vừa không gây chờ quá lâu.

---

## 4. Bảng so sánh: Trước và sau khi nâng cấp

| Tiêu chí | Mô phỏng cũ (Legacy) | Hệ thống hiện tại |
|---|---|---|
| **Giao tiếp mạng** | `multiprocessing.Queue` | TCP Connection Pool + Health-check |
| **Mật mã** | Chuỗi giả lập `"SIG_SITE_X"` | Ed25519 thực (SHA-256 Digest) |
| **Storage** | File JSON tuần tự | RocksDB (WAL, State, Ledger, Checkpoint) |
| **Giao thức đồng thuận** | 1 pha, CFT-like | PBFT 3 pha đầy đủ + View Change chuẩn |
| **Kiểm tra Digest** | Không có | Nghiêm ngặt ở mọi pha, phát hiện Equivocation |
| **Safety** | Không đảm bảo | Đảm bảo qua Digest check + Watermarks |
| **Liveness** | Không đảm bảo | Đảm bảo qua View Change + NOOP + Backoff |
| **Client idempotency** | Không có | Cache reply theo `(client_id, timestamp)` |
| **Crash Recovery** | Không có | Checkpoint + WAL Replay + Duplicate check |
| **Heartbeat** | Không có | PING/PONG định kỳ phát hiện leader chết |
| **Client UX** | Gõ lệnh thủ công | TUI tương tác (`client_tui.py`) |
| **Đóng gói** | Script cục bộ | Docker Compose + Makefile + Integration Tests |

---

## 5. Hướng phát triển tiếp theo (Production roadmap)

| Hướng | Mô tả |
|---|---|
| **Dynamic Membership** | Thêm/bớt node không cần restart cluster (Reconfiguration Protocol) |
| **gRPC + Protobuf** | Thay TCP raw + JSON bằng gRPC để tối ưu hóa serialization |
| **TLS** | Mã hóa kênh truyền giữa các node, chống MitM |
| **Client-side quorum** | Client tự verify f+1 chữ ký thay vì trust một node |
| **Persistent client cache** | Lưu idempotency cache vào RocksDB qua restart |
| **Merkle State Proof** | Sinh proof về World State để cho phép Light Client |
