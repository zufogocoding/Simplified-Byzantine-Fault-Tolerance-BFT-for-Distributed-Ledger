# 🏛️ ARCHITECTURAL DECISIONS: PBFT + ROCKSDB + TCP + ED25519 SYSTEM

Tài liệu này ghi lại các quyết định kiến trúc cốt lõi, so sánh công nghệ và giải pháp kỹ thuật thực tế được áp dụng trong quá trình nâng cấp hệ thống **BFT Distributed Ledger** từ mô phỏng IPC cục bộ lên hệ thống phân tán thực thụ chạy trên mạng TCP thực.

---

## 1. Bản chất kỹ thuật: Tại sao BFT Consensus + RocksDB tối ưu hơn PostgreSQL?

Trong các đề tài Cơ sở dữ liệu phân tán (CSDLPT) truyền thống, sinh viên thường cấu hình nhân bản (Replication) trên các hệ quản trị CSDL như MySQL hay PostgreSQL. Tuy nhiên, hướng tiếp cận đó gặp các giới hạn nghiêm trọng về mặt lý thuyết và thực tiễn:

* **Giới hạn CFT của PostgreSQL**: Các cơ chế nhân bản sẵn có của PostgreSQL (Streaming Replication, Logical Replication) chỉ hỗ trợ mô hình lỗi **CFT (Crash Fault Tolerance)**. Chúng giả định tất cả các node trong mạng là trung thực và chỉ có thể bị chết (crash) chứ không thể gửi dữ liệu sai lệch (Byzantine).
* **Equivocation trên Postgres**: Nếu Node Master bị hack (Byzantine Node), nó có thể gửi hai bản ghi WAL mâu thuẫn nhau cho hai Node Standby khác nhau. Không có cơ chế đồng thuận đa bên (Multi-party Consensus) nào trong Postgres để phát hiện sự mâu thuẫn này, dẫn đến trạng thái phân rã (split-brain) ngay lập tức.
* **Lợi thế của RocksDB (LSM-Tree Storage)**: RocksDB được chọn làm công cụ lưu trữ cục bộ cho từng Node thay vì PostgreSQL vì:
  1. **Hiệu năng ghi tuần tự**: Cấu trúc LSM-Tree (Log-Structured Merge-tree) của RocksDB tối ưu hóa tốc độ ghi WAL (Write-Ahead Log) và PBFT logs cực nhanh.
  2. **Kiểm soát chi tiết**: Giúp lập trình viên can thiệp sâu vào tầng logic lưu trữ (lưu checkpoint, replay WAL và cắt tỉa log) độc lập với tầng đồng thuận.

> 💡 **Kết luận kiến trúc**: Việc sử dụng **Custom PBFT Consensus Layer + RocksDB Local Engine** cung cấp khả năng chịu lỗi Byzantine thực sự ($3f+1$) mà các hệ thống PostgreSQL truyền thống không thể tự đạt được mà không có một tầng đồng thuận bao bọc phía trước.

---

## 2. Các Quyết định Thiết kế Kiến trúc Thực tế đã triển khai

### 2.1. Giao tiếp mạng qua TCP Connection Pool (`network/connection_pool.py`)
* **Quyết định**: Thay vì mở và đóng socket TCP cho mỗi tin nhắn truyền đi (gây overhead lớn và dễ cạn kiệt file descriptors khi tải cao), chúng tôi triển khai một TCP Connection Pool duy trì kết nối dài hạn.
* **Cơ chế**: Khi Node khởi chạy, nó song song hóa các luồng ngầm để kết nối đến các Node khác. Socket được giữ mở liên tục. Nếu một kết nối bị lỗi hoặc mất gói mạng, pool sẽ tự động đóng socket cũ, thiết lập socket mới và gửi lại tin nhắn một cách trong suốt.

### 2.2. Hỗ trợ Socket Server dài hạn (Persistent Server)
* **Quyết định**: Nâng cấp TCPServer để đọc liên tục từ các socket của connection pool bằng cơ chế tách dòng băm nhỏ (`\n` delimiter).
* **Cơ chế**: Sử dụng một buffer đệm ngầm để tích lũy dữ liệu thô từ socket, phân tách các chuỗi JSON thông qua `buffer.partition(b"\n")`. Điều này giúp xử lý được nhiều tin nhắn dồn dập trên cùng một đường truyền TCP.

### 2.3. Khóa ghi đệ quy Thread-Safe (`threading.RLock`)
* **Quyết định**: Đổi toàn bộ các khóa đồng bộ trong PBFT Engine và View Change Manager từ `threading.Lock` thành `threading.RLock` (Reentrant Lock).
* **Lý do**: PBFT là giao thức đa pha đồng thời. Trong quá trình kiểm tra Quorum phiếu Prepare, luồng xử lý chính cần gọi tiếp hàm gửi Commit và tự cập nhật trạng thái của mình. Sử dụng `Lock` thông thường sẽ gây ra lỗi **Self-Deadlock** (tự khóa chính mình vĩnh viễn trên cùng một thread). `RLock` cho phép acquire khóa nhiều lần trên cùng một thread an toàn.

### 2.4. Cơ chế Checkpoint trạng thái & Cắt tỉa RocksDB WAL
* **Quyết định**: Thực hiện lưu checkpoint trạng thái số dư (balances) định kỳ mỗi $K=2$ giao dịch.
* **Lý do**: Nếu chỉ lưu trữ WAL tuần tự mãi mãi, dung lượng đĩa sẽ phình to nhanh chóng và thời gian replay WAL khi khôi phục sau crash sẽ tăng tuyến tính. Khi $2f+1$ node đạt đồng thuận về cùng một State Checkpoint, checkpoint đó được đánh dấu là *Stable Checkpoint*. RocksDB sẽ thực hiện cắt tỉa (prune) các bản ghi WAL và PBFT logs cũ nằm dưới Sequence Number của checkpoint này để giải phóng hoàn toàn bộ nhớ.

---

## 3. Bản so sánh Công nghệ: Trước và Sau khi nâng cấp

| Tiêu chí | Mô phỏng Cũ (Legacy) | Hệ thống Thực tế Mới (Upgraded) |
| :--- | :--- | :--- |
| **Giao tiếp mạng** | `multiprocessing.Queue` (Shared Memory) | TCP Sockets thật với **TCP Connection Pool** |
| **Mật mã & Xác thực** | Chuỗi giả lập (`"SIG_SITE_X"`) | Chữ ký số **Ed25519** thật (SHA-256 Digest) |
| **Công cụ lưu trữ** | File log JSON Line tuần tự | **RocksDB** (Log, State, Checkpoint, Ledger) |
| **Giao thức đồng thuận** | Bỏ phiếu 1 pha (CFT-like) | **PBFT 3 pha** + **View Change** + **New View** |
| **Heartbeat & Phát hiện lỗi** | Không có (Giả lập delay ngẫu nhiên) | Luồng **Heartbeat PING/PONG** ngầm phát hiện leader lỗi |
| **Tách biệt vai trò Client** | Khởi chạy giao dịch cứng từ main process | Chương trình **Client độc lập (`client.py`)** ký gửi qua mạng |
| **Đóng gói & Chạy thử** | Chạy Script cục bộ đơn giản | **Docker Compose**, **Makefile** và **Integration Tests** tự động |
