# 🗺️ LỘ TRÌNH NÂNG CẤP DỰ ÁN BFT LÊN HỆ THỐNG PHÂN TÁN THỰC TẾ

Tài liệu này chi tiết hóa lộ trình chuyển đổi và nâng cấp hệ thống **Simplified BFT Consensus** từ một dự án mô phỏng tiến trình cục bộ (in-memory simulation) thành một hệ thống **Distributed Database Engine** hoàn chỉnh, bảo mật và chịu lỗi Byzantine thực sự.

---

## 🏗️ PHẦN 1: BẢN ĐẶC TẢ CÁC THÀNH PHẦN NÂNG CẤP

### 1. Giao tiếp mạng thực (Real Networking Layer)
* **Mục tiêu:** Loại bỏ hoàn toàn `multiprocessing.Queue` (Shared Memory IPC).
* **Kiến trúc đề xuất:**
  * Sử dụng thư viện **ZeroMQ (pyzmq)** hoặc **gRPC** để tối ưu hóa việc truyền thông điệp có cấu trúc.
  * Mỗi Node (Site) lắng nghe trên một địa chỉ IP và Port cụ thể (ví dụ: `127.0.0.1:5001`, `127.0.0.1:5002`).
  * Sử dụng **JSON/Protocol Buffers** làm định dạng serialize dữ liệu trước khi gửi qua socket TCP.
* **Mô phỏng mạng thực tế:** Sử dụng Linux `tc` (Traffic Control) kết hợp `netem` để áp đặt độ trễ (delay), rung pha (jitter) và tỉ lệ mất gói (packet loss).

### 2. Cryptography chuyên nghiệp (Xác thực Ed25519)
* **Mục tiêu:** Thay thế chữ ký giả lập `"SIG_SITE_X"` bằng mật mã hóa bất đối xứng thực tế.
* **Kiến trúc đề xuất:**
  * Dùng thư viện `cryptography` hoặc `PyNaCl` của Python để sinh cặp khóa **Ed25519** cho mỗi node.
  * Khởi tạo file cấu hình PKI (Public Key Infrastructure) tĩnh chứa địa chỉ IP và Public Key của tất cả 4 node trên đĩa.
  * Mỗi message gửi đi được ký bằng Private Key của node gửi: `signature = sign(private_key, message_bytes)`.
  * Node nhận thực hiện xác minh chữ ký bằng Public Key tương ứng trong file PKI để loại bỏ hoàn toàn khả năng giả danh (Spoofing) của node Byzantine.

### 3. Nâng cấp giao thức đồng thuận (PBFT / Tendermint)
* **Mục tiêu:** Chuyển đổi từ cơ chế Single-shot consensus thô sơ sang **State Machine Replication (SMR)** đầy đủ.
* **Kiến trúc đề xuất:**
  * Triển khai giao thức **PBFT** gồm 3 pha tiêu chuẩn:
    1. **Pre-Prepare:** Leader đề xuất giao dịch kèm số thứ tự (Sequence Number) và View hiện tại.
    2. **Prepare:** Các node broadcast phiếu Prepare chứng minh đã ghi nhận đề xuất của Leader.
    3. **Commit:** Đảm bảo đủ $2f+1$ node đã ghi nhận pha Prepare, chuyển sang pha thực thi và cập nhật trạng thái.
  * Bổ sung giao thức **View-change**: Khi Leader hiện tại bị lỗi (hoặc Byzantine không tiến triển), các node đếm timeout và tự động thực hiện bỏ phiếu bầu chọn Leader mới.

### 4. Container hóa hệ thống (Docker Compose)
* **Mục tiêu:** Cách ly hoàn toàn môi trường chạy của các Node trên các container độc lập.
* **Kiến trúc đề xuất:**
  * Đóng gói mã nguồn Node vào **Docker Image**.
  * Cấu hình file `docker-compose.yml` định nghĩa 4 dịch vụ (Site 0, 1, 2, 3) kết nối thông qua một mạng ảo `bridge` riêng biệt.
  * Sử dụng Docker Volume để ghi file WAL (`site_X.wal`) và State DB xuống đĩa cứng vật lý của máy host để bảo toàn dữ liệu khi container bị tắt.

### 5. Tách biệt Client và Replicated State Machine
* **Mục tiêu:** Tách biệt vai trò đề xuất giao dịch khỏi các node xử lý đồng thuận.
* **Kiến trúc đề xuất:**
  * Viết một script Client riêng biệt (hoặc một REST API nhỏ bằng Flask).
  * Client ký giao dịch bằng khóa của mình và gửi giao dịch tới Leader Node hiện tại qua TCP socket.
  * Client lắng nghe phản hồi xác thực giao dịch thành công từ ít nhất $f+1$ node khác nhau trên mạng để tin tưởng kết quả.

### 6. Sử dụng RocksDB / LevelDB làm Storage Engine
* **Mục tiêu:** Thay thế file log tuần tự và in-memory list bằng một Database chuyên nghiệp có hiệu năng cao.
* **Kiến trúc đề xuất:**
  * Tích hợp **RocksDB** (qua thư viện `python-rocksdb`) làm bộ lưu trữ cục bộ cho mỗi node.
  * RocksDB hỗ trợ cấu trúc LSM-Tree (Log-Structured Merge-tree) tối ưu hóa ghi tuần tự cực nhanh phù hợp cho WAL, đồng thời hỗ trợ lập chỉ mục (Index) hỗ trợ đọc ngẫu nhiên $O(1)$.
  * Thực hiện **Checkpointing/Snapshot** định kỳ để giải phóng dung lượng đĩa và tối ưu hóa tốc độ crash recovery.

---

## 📈 PHẦN 2: LỘ TRÌNH TRIỂN KHAI THEO GIAI ĐOẠN

```
Giai đoạn 1: Bảo mật & Mạng thực
  ├── Thay thế Queue bằng TCP Sockets
  └── Tích hợp chữ ký số Ed25519 thật
       ▼
Giai đoạn 2: Container hóa & State DB
  ├── Đóng gói các Node bằng Docker Compose
  └── Tích hợp SQLite/RocksDB thay thế log thô
       ▼
Giai đoạn 3: Hoàn thiện BFT giao thức
  ├── Triển khai PBFT 3 pha (Pre-prepare, Prepare, Commit)
  └── Tích hợp View-change protocol khi Leader Byzantine
```

### Đánh giá giá trị thực tiễn:
Lộ trình nâng cấp này hoàn toàn tiệm cận với cấu trúc của các blockchain doanh nghiệp và cơ sở dữ liệu phân tán hiện đại (như Tendermint/Cosmos SDK, Hyperledger Fabric). Khi hoàn thành, dự án sẽ giải quyết toàn diện các vấn đề về **bảo mật, hiệu năng lưu trữ dữ liệu lớn, độ trễ mạng thực và tính bền vững của hệ thống**.
