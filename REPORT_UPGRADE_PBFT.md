# 📝 BÁO CÁO CẬP NHẬT KIẾN TRÚC & TRIỂN KHAI HỆ THỐNG PBFT DISTRIBUTED LEDGER THỰC TẾ

Tài liệu này được biên soạn theo văn phong học thuật chuẩn để bổ sung vào báo cáo thuyết minh đồ án môn **Cơ sở dữ liệu phân tán** (hoặc báo cáo cuối kỳ). Nội dung mô tả chi tiết kiến trúc, thuật toán và kết quả thực nghiệm của hệ thống PBFT đã được nâng cấp thực tế.

---

## CHƯƠNG I: ĐẶT VẤN ĐỀ VÀ ĐỘNG LỰC NÂNG CẤP ĐỀ TÀI

### 1. Giới hạn của mô hình mô phỏng lý thuyết
Trong phiên bản ban đầu, đề tài *"Simplified BFT cho Distributed Ledger"* được xây dựng dưới dạng mô phỏng in-memory thông qua thư viện `multiprocessing`:
* Các node giao tiếp bằng hàng đợi dùng chung bộ nhớ (`multiprocessing.Queue`).
* Chữ ký số được mô phỏng bằng chuỗi text thô dạng `"SIG_SITE_X"`.
* Cơ sở dữ liệu và Write-Ahead Log (WAL) chỉ là tệp tin JSON tuần tự đơn giản.

Mô hình này tuy làm nổi bật được logic đồng thuận của thuật toán nhưng chưa phản ánh đúng bản chất của một **Hệ phân tán thực tế**, nơi có độ trễ mạng vật lý, mất gói tin, các cuộc tấn công giả mạo chữ ký (identity spoofing) và yêu cầu khắt khe về hiệu năng đọc/ghi đĩa của Storage Engine.

### 2. Định hướng nâng cấp chuẩn doanh nghiệp (Production-Grade)
Để tăng giá trị học thuật và ứng dụng thực tiễn, hệ thống đã được nâng cấp toàn diện lên một **Replicated Distributed Ledger Engine** chạy trên môi trường mạng TCP thật với các công nghệ cốt lõi:
* **Networking**: TCP Sockets cục bộ và container hóa kết hợp **TCP Connection Pool** cùng cơ chế **Heartbeat PING/PONG**.
* **Cryptography**: Chữ ký số **Ed25519** thật và hàm băm mật mã học SHA-256.
* **Storage Engine**: Cơ sở dữ liệu **RocksDB (LSM-Tree)** lưu trữ WAL, Ledger và State, kết hợp kỹ thuật **State Checkpointing** để tối ưu hóa bộ nhớ và tốc độ khôi phục.
* **Client Model**: Tách biệt Client độc lập gửi giao dịch, hỗ trợ tự động chuyển hướng (**Redirect**) về Leader.

---

## CHƯƠNG II: THIẾT KẾ KIẾN TRÚC HỆ THỐNG PHÂN TÁN MỚI

Kiến trúc hệ thống nâng cấp được phân chia thành 4 lớp rõ rệt:

```
                  ┌──────────────────────────────┐
                  │   Client App (client.py)     │
                  └──────────────┬───────────────┘
                                 │ Gửi CLIENT_REQUEST (Ký Ed25519)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. TẦNG GIAO TIẾP MẠNG (TCP Connection Pool & Server)                  │
│    - TCPServer lắng nghe kết nối dài hạn, buffer qua dòng "\n"          │
│    - Connection Pool duy trì socket kết nối liên tục đến các node       │
│    - Heartbeat PING/PONG phát hiện nhanh node chết (suspected)          │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ Đưa thông điệp vào Queue nội bộ
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. TẦNG ĐỒNG THUẬN (PBFT Consensus Engine)                             │
│    - Quy trình 3 pha: Pre-prepare (View, Seq, Digest) -> Prepare -> Commit│
│    - Đạt Quorum 2f+1 phiếu đồng thuận trên thread-safe RLock            │
│    - Quản lý View Change (Prepared Certs) khi leader lỗi                │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ Gọi thực thi cập nhật trạng thái
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. TẦNG LƯU TRỮ CỤC BỘ (RocksDB LSM-Tree Engine)                       │
│    - WAL Store (wal::<tx_id>::<seq>) đảm bảo tính bền vững dữ liệu      │
│    - World State Store (state::<account>) lưu trữ balances              │
│    - Ledger Store (ledger::<tx_id>) lưu trữ lịch sử chuỗi khối          │
│    - Checkpoint Store (state::checkpoint) chụp nhanh trạng thái balances │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1. Tầng giao tiếp mạng bền vững (TCP Connection Pool)
Để loại bỏ overhead kết nối liên tục, [network/connection_pool.py](file:///home/trongzufo/csdlpt/network/connection_pool.py) duy trì các socket kết nối TCP mở vĩnh viễn giữa các node.
* **Cơ chế tái kết nối**: Khi đường truyền bị lỗi, pool tự động giải phóng socket cũ và thử kết nối lại ngầm.
* **Server xử lý đa dòng**: [network/tcp_server.py](file:///home/trongzufo/csdlpt/network/tcp_server.py) sử dụng thuật toán buffer phân tách theo ký tự `\n` để đọc dồn dập nhiều gói tin JSON trên cùng một TCP stream.

### 2. Tầng bảo mật mật mã hóa (Ed25519 & SHA-256)
Mọi thông điệp trao đổi giữa các node và client đều bắt buộc phải ký bằng khóa bí mật Ed25519 (32 bytes) của bên gửi và được kiểm tra bởi bên nhận qua hàm `verify_signature_real` trước khi đưa vào hàng đợi xử lý.
* **SHA-256 Transaction Digest**: Client băm (hash) nội dung giao dịch thành chuỗi digest lục thập phân (hex string). Các bản tin chuẩn bị (Prepare) và cam kết (Commit) chỉ truyền tải digest này để tiết kiệm băng thông mạng.
* **SHA-256 State Digest**: Dùng để băm trạng thái số dư khi thực hiện checkpoint để kiểm tra tính nhất quán đồng thuận giữa các node.

### 3. Tầng lưu trữ cục bộ RocksDB & World State
Thay vì dùng file text thô, hệ thống lưu trữ dữ liệu phân mảnh bằng [storage/rocksdb_store.py](file:///home/trongzufo/csdlpt/storage/rocksdb_store.py) dựa trên RocksDB:
* **Write-Ahead Log (WAL)**: Ghi lại trạng thái giao dịch trước khi thực thi để phòng ngừa mất điện đột ngột.
* **Sổ cái Ledger**: Lưu danh sách giao dịch đã đạt đồng thuận COMMIT.
* **World State**: Bản ghi số dư của từng tài khoản được cập nhật nguyên tử thông qua phương thức `transfer`.

---

## CHƯƠNG III: THUẬT TOÁN ĐỒNG THUẬN PBFT & KHẢ NĂNG CHỊU LỖI

### 1. Quy trình đồng thuận 3 pha trong PBFT
Khi client gửi giao dịch `CLIENT_REQUEST`, Leader gán số thứ tự `seq` và kích hoạt đồng thuận:

1. **Pre-prepare**: Leader gửi bản tin `<PRE-PREPARE, view, seq, digest, request>` đến tất cả các node backup.
2. **Prepare**: Các node nhận bản tin, kiểm tra tính hợp lệ của chữ ký, view và digest. Nếu đúng, lưu log và gửi bản tin `<PREPARE, view, seq, digest>` đến toàn mạng.
3. **Commit**: Khi nhận được ít nhất $2f$ bản tin `PREPARE` từ các node khác nhau (tổng cộng $2f+1$ tính cả bản thân), node chuyển sang trạng thái `prepare_ready` và broadcast bản tin `<COMMIT, view, seq, digest>`.
4. **Execute**: Khi nhận đủ $2f+1$ bản tin `COMMIT`, node thực thi giao dịch, ghi Ledger, ghi WAL trạng thái "COMMIT" và gửi trả kết quả `CLIENT_REPLY` về cho client.

### 2. Cơ chế Checkpoint và Cắt tỉa (Pruning) WAL
Khi Sequence Number tăng lên (mỗi $K=2$ giao dịch):
1. Node tính toán `state_digest` của balances tài khoản và broadcast `<CHECKPOINT, seq, digest>`.
2. Khi nhận đủ $2f+1$ bản tin Checkpoint có cùng digest, checkpoint đó trở thành *Stable Checkpoint*.
3. Node lưu trạng thái stable này vào RocksDB (`state::checkpoint`), giải phóng và xóa bỏ toàn bộ các bản ghi PBFT logs và WAL cũ nằm dưới `seq` để tối ưu dung lượng đĩa.

### 3. Cơ chế Khôi phục sau sự cố (Crash Recovery)
Khi một node bị crash và khởi động lại:
1. Node đọc trạng thái checkpoint gần nhất từ `state::checkpoint` để khôi phục nhanh số dư tài khoản về thời điểm stable checkpoint.
2. Node quét các bản ghi WAL trong RocksDB có `seq > checkpoint_seq` và có trạng thái `COMMIT` để replay lại tuần tự các giao dịch bị bỏ lỡ, khôi phục World State về trạng thái nhất quán mới nhất với hệ thống.

---

## CHƯƠNG IV: THỰC NGHIỆM VÀ KẾT QUẢ VẬN HÀNH

Hệ thống được kiểm thử tự động thông qua kịch bản kiểm thử tích hợp [tests/integration_test.py](file:///home/trongzufo/csdlpt/tests/integration_test.py).

### Kịch bản thực nghiệm:
1. Khởi chạy 4 node PBFT TCP cục bộ (Site 0 Byzantine, Site 1, 2, 3 Honest).
2. Chạy Client gửi giao dịch đến Node 1 (Backup).
3. Đo lường quá trình Redirect và đạt đồng thuận.

### Kết quả Log Console thực tế:
```
--- Running Test: Normal Consensus & Client Redirection ---
[Test] Cho 3 giay de cac node TCP start va ket noi pool...
[Test] Gui CLIENT_REQUEST den Backup Node 1...
[Client Output]:
[Client] Dang lang nghe reply tai 127.0.0.1:60811...
[Client] Gui giao dich den Node localhost:5001...
[Client] Redirect: Node thong bao gui ve Leader moi (Node 0 o localhost:5000). Gui lai...
[Client] Nhan SUCCESS tu Node 2!
[Client] Nhan SUCCESS tu Node 1!
[Client] Giao dich hoan thanh thanh cong! Nhac du f+1 reply tu cac node: [1, 2]
[Test] Xac minh du lieu ghi nhan trong RocksDB...
  Node 0 - Balance A: 90, B: 110, Ledger: 1 txs
  Node 1 - Balance A: 90, B: 110, Ledger: 1 txs
  Node 2 - Balance A: 90, B: 110, Ledger: 1 txs
  Node 3 - Balance A: 90, B: 110, Ledger: 1 txs
[Test] Normal Consensus & Redirect PASSED!
All integration tests PASSED!
```

### Phân tích kết quả:
* **Redirect thành công**: Node 1 (Backup) đã từ chối xử lý trực tiếp và gửi bản tin REDIRECT chỉ định cổng `5000` (Node 0 - Leader) về cho Client. Client đã tự động kết nối lại và gửi thành công.
* **Đồng nhất số dư**: Sau đồng thuận, tất cả các node trung thực đều cập nhật chính xác số dư từ $100 \rightarrow 90$ cho tài khoản A và từ $100 \rightarrow 110$ cho tài khoản B.
* **Mật mã hóa hoạt động chính xác**: Chữ ký Ed25519 của Client được xác thực thành công tại Node 0, và chữ ký phản hồi của các node được xác thực thành công tại Client.
