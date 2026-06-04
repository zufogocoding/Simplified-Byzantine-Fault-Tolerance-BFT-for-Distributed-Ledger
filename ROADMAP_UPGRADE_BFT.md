# 🗺️ MỐC LỘ TRÌNH ĐÃ HOÀN THÀNH: PHÁT TRIỂN HỆ THỐNG PHÂN TÁN PBFT THỰC TẾ

Tài liệu này tổng kết các mốc lộ trình đã triển khai thành công nhằm chuyển đổi hệ thống **Simplified BFT Consensus** từ một dự án mô phỏng tiến trình cục bộ (in-memory simulation) thành một hệ thống **Replicated Distributed Ledger Engine** chịu lỗi Byzantine thực sự chạy trên mạng TCP vật lý.

---

## 🏗️ PHẦN 1: CÁC MỐC MILESTONE ĐÃ HOÀN THÀNH VÀ BÀN GIAO

### 1. Triển khai mạng lưới TCP thực tế (TCP Connection Pool)
* **Kết quả**: Loại bỏ hoàn toàn giao tiếp qua bộ nhớ chia sẻ `multiprocessing.Queue`.
* **Chi tiết**:
  - Mỗi Node là một tiến trình chạy độc lập lắng nghe trên card mạng `0.0.0.0` qua các cổng TCP chỉ định (5000-5003).
  - Sử dụng [network/connection_pool.py](file:///home/trongzufo/csdlpt/network/connection_pool.py) để quản lý kết nối socket TCP dài hạn. Kết nối được tái sử dụng để truyền nhiều bản tin liên tục, giảm thiểu thời gian bắt tay TCP (handshake).
  - Triển khai luồng Heartbeat PING/PONG định kỳ gửi qua lại giữa các node để nhanh chóng phát hiện các lỗi mạng hoặc lỗi crash của Leader.

### 2. Tích hợp mật mã học Ed25519 & SHA-256 Digest
* **Kết quả**: Nâng cấp tính bảo mật và toàn vẹn của dữ liệu tin nhắn qua thuật toán mã hóa khóa công khai thực tế.
* **Chi tiết**:
  - Sử dụng thư viện `cryptography` để ký số Ed25519 thực cho mọi thông điệp truyền đi.
  - Các node sử dụng `PUBLIC_KEYS_BYTES` cấu hình tĩnh để xác minh chữ ký của node gửi trước khi xử lý tin nhắn.
  - Áp dụng SHA-256 để tính toán băm giao dịch (transaction digest) và băm trạng thái (state digest) phục vụ cho đối chiếu tính nhất quán.

### 3. Giao thức đồng thuận PBFT 3 pha đầy đủ
* **Kết quả**: Triển khai hoàn chỉnh State Machine Replication (SMR) chịu lỗi Byzantine ($3f+1$).
* **Chi tiết**:
  - Tầng PBFT Engine ([consensus/pbft.py](file:///home/trongzufo/csdlpt/consensus/pbft.py)) điều phối 3 pha: `PRE-PREPARE` (đề xuất kèm sequence number và digest), `PREPARE` (chứng thực đề xuất), và `COMMIT` (đồng thuận cam kết thực thi).
  - Quản lý View Change khi có sự cố Leader ([consensus/view_change.py](file:///home/trongzufo/csdlpt/consensus/view_change.py)): Khi nhận diện Leader lỗi hoặc Byzantine không sinh khối, các backup node timeout và bỏ phiếu bầu chọn Leader mới qua tin nhắn VIEW-CHANGE đính kèm chứng chỉ chuẩn bị `prepared_certs`, sau đó Leader mới gửi `NEW_VIEW` để tiếp tục tiến trình.

### 4. Cơ sở dữ liệu RocksDB (LSM-Tree Storage Engine)
* **Kết quả**: Tích hợp công cụ lưu trữ hiệu năng cao RocksDB thay thế cho việc ghi file log văn bản đơn giản.
* **Chi tiết**:
  - Lưu trữ nhật ký WAL (`wal::`), World State số dư (`state::`), và sổ cái lịch sử giao dịch (`ledger::`) vào RocksDB cục bộ.
  - Triển khai **State Checkpointing**: Mỗi khi xử lý qua $K=2$ giao dịch, node chụp trạng thái hiện tại (State Snapshot) và lưu lại. Khi đạt $2f+1$ phiếu checkpoint giống nhau, node thực hiện cắt tỉa (prune) các WAL và PBFT logs cũ để tiết kiệm dung lượng lưu trữ đĩa.
  - Hỗ trợ **Crash Recovery**: Khi node khởi động lại sau crash, nó tự động tải checkpoint trạng thái gần nhất từ RocksDB và đọc WAL để replay lại toàn bộ các giao dịch đã COMMIT kể sau checkpoint.

### 5. Client độc lập và Phân quyền chuyển hướng (Redirect)
* **Kết quả**: Tách biệt hoàn toàn Client đề xuất giao dịch khỏi mạng lưới nodes.
* **Chi tiết**:
  - Viết chương trình Client độc lập [client.py](file:///home/trongzufo/csdlpt/client.py) cho phép người dùng ký và gửi giao dịch đến bất kỳ node nào qua socket.
  - Node Backup tự động gửi bản tin `REDIRECT` chứa địa chỉ Leader hiện tại về cho client nếu client kết nối nhầm node.
  - Client tự động kết nối lại đến Leader và chờ đủ $f+1=2$ kết quả SUCCESS từ các node khác nhau để tin tưởng giao dịch đã hoàn thành.

---

## 📈 PHẦN 2: LỘ TRÌNH PHÁT TRIỂN TIẾP THEO (TÙY CHỌN CHO DOANH NGHIỆP)

Hệ thống hiện tại đã tiệm cận mô hình hoạt động của các hệ thống blockchain doanh nghiệp như Hyperledger Fabric. Hướng phát triển tiếp theo bao gồm:
1. **Dynamic Membership**: Cho phép thêm/bớt các node tham gia mạng lưới một cách động mà không cần restart toàn bộ cluster.
2. **gRPC & Protobuf**: Thay thế TCP sockets thô và chuỗi JSON bằng gRPC và Protocol Buffers để tối ưu hóa hiệu năng serialization dữ liệu và tăng tốc độ truyền tải mạng.
3. **Mã hóa kênh truyền (TLS)**: Thiết lập kênh truyền bảo mật SSL/TLS giữa các node để chống nghe trộm và tấn công xen giữa (MitM).
