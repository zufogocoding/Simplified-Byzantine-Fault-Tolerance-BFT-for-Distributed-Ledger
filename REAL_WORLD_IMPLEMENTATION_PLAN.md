# 📝 KẾ HOẠCH TRIỂN KHAI HỆ THỐNG BFT DISTRIBUTED LEDGER THỰC TẾ
*(Tài liệu đặc tả kỹ thuật dành cho AI Coding Agents)*

---

## 🎯 MỤC TIÊU TỔNG QUÁT

Từ mã nguồn mô phỏng đơn giản (dùng `multiprocessing.Queue`, chữ ký giả lập, WAL file JSON, giao thức broadcast-thu thập), phát triển thành một **hệ thống phân tán thực tế** chạy trên môi trường đa container với các yêu cầu kỹ thuật:
- Giao tiếp qua mạng TCP thật, hỗ trợ container hóa.
- Xác thực bằng chữ ký số mạnh Ed25519.
- Lưu trữ dữ liệu hiệu năng cao với RocksDB.
- Giao thức đồng thuận **PBFT** (Practical Byzantine Fault Tolerance) đầy đủ.
- Client tách biệt gửi giao dịch tới mạng lưới.
- Kiểm thử tích hợp toàn diện và đóng gói hoàn thiện.

---

## 📁 CẤU TRÚC THƯ MỤC DỰ KIẾN SAU KHI HOÀN THÀNH

```
bft-ledger/
├── config.py
├── crypto_utils.py          # Sinh khóa, ký, xác minh (Ed25519)
├── network/
│   ├── tcp_server.py        # Listener đa luồng
│   └── tcp_client.py        # Sender helper
├── storage/
│   ├── rocksdb_store.py      # Thay thế wal.py và storage.py cũ
│   └── world_state.py        # Quản lý trạng thái số dư tài khoản
├── consensus/
│   ├── pbft.py               # Giao thức PBFT (Pre-prepare, Prepare, Commit)
│   └── view_change.py        # Giao thức View Change phục hồi lỗi Leader
├── node.py                   # Entrypoint khởi chạy của một node
├── client.py                 # Client gửi giao dịch
├── main.py                   # Orchestrator (Khởi động và điều phối kiểm tra)
├── docker-compose.yml        # Định nghĩa cụm mạng phân tán
├── tests/
│   ├── test_crypto.py
│   ├── test_network.py
│   ├── test_pbft.py
│   └── integration_test.py
└── requirements.txt
```

---

## 🛠️ CHI TIẾT CÁC BƯỚC THỰC HIỆN THEO GIAI ĐOẠN (PHASES)

### 🛜 PHASE 1 – Mạng truyền thông thực tế (TCP Network)

**Mục tiêu:** Thay thế `multiprocessing.Queue` bằng giao thức socket TCP thực tế. Mỗi node chạy độc lập dưới dạng một process riêng và giao tiếp qua cổng TCP mạng.

**Các bước thực hiện:**
1. Xóa file `network.py` cũ, tạo thư mục `network/` chứa hai module:
   - `tcp_server.py`: Định nghĩa class `TCPServer` chạy trên một thread riêng biệt, lắng nghe trên cổng được chỉ định, nhận các thông điệp JSON, giải mã và đẩy vào một hàng đợi nội bộ (`queue.Queue`).
   - `tcp_client.py`: Định nghĩa hàm helper `send_message(host, port, message)` phục vụ gửi JSON qua TCP socket.
2. Cập nhật `config.py`:
   - Thêm cấu hình địa chỉ mạng tĩnh cho các node: `NODE_ADDRESSES = {0: ('localhost', 5000), 1: ('localhost', 5001), 2: ('localhost', 5002), 3: ('localhost', 5003)}`.
3. Cập nhật `node.py` (chuyển đổi từ `consensus.py` cũ):
   - Khởi động `TCPServer` trong một thread riêng ngay khi node khởi chạy để đón nhận message.
   - Thay thế toàn bộ các lời gọi `qs[t].put(msg)` cũ bằng hàm `send_message()` tới IP/Port tương ứng của node đích.
   - Sử dụng hàng đợi nội bộ (`queue.Queue`) của Server để thay thế cho `qs[sid]`.
4. **Giữ nguyên định dạng các message** (`VOTE`, `REQ`, `RESP`, `DEC`) và logic thu thập phiếu trong `collect_votes` và `listen_for_recovery` để đảm bảo hệ thống không bị xáo trộn thuật toán ở giai đoạn này.
5. Thêm cơ chế xử lý lỗi kết nối, tự động reconnect và xử lý timeout.
6. **Kiểm thử đơn vị:** Khởi chạy độc lập 4 node trên 4 cổng khác nhau của localhost, gửi 1 giao dịch và kiểm tra việc truyền nhận thành công.

---

### 🔑 PHASE 2 – Chữ ký số mã hóa thực tế (Ed25519)

**Mục tiêu:** Tích hợp cặp khóa Public/Private Key thực tế cho từng node để ký và xác thực thông điệp gửi đi.

**Các bước thực hiện:**
1. Tạo module `crypto_utils.py`:
   - Sử dụng thư viện `cryptography` (Ed25519) để viết hàm `generate_keypair()` sinh cặp khóa dạng bytes.
   - Viết hàm `sign(private_key, message_bytes)` sinh chữ ký số.
   - Viết hàm `verify(public_key, message_bytes, signature)` trả về kết quả xác thực.
   - Viết hàm `pack_message(msg_dict)` serialize thông điệp thành bytes (sử dụng `json.dumps(sort_keys=True)` để đảm bảo tính nhất quán của chuỗi bytes trước khi ký).
2. Cập nhật `config.py`:
   - Lưu trữ danh sách khóa công khai tĩnh: `PUBLIC_KEYS = {0: key0_bytes, 1: key1_bytes, ...}` phục vụ việc xác thực chéo giữa các node (PKI mô phỏng).
3. Cập nhật toàn bộ các điểm gọi `sign_message` và `verify_signature`:
   - Trực tiếp ký lên chuỗi bytes của message dict và đính kèm signature (dạng hex string) vào message.
   - Khi nhận message, node nhận sẽ tra cứu Public Key của sender ID tương ứng từ `PUBLIC_KEYS` trong config và thực hiện xác minh chữ ký thực tế.
   - Nếu xác minh thất bại, lập tức loại bỏ thông điệp để chống giả mạo thông tin.
4. **Kiểm thử:** Viết script test `tests/test_crypto.py` kiểm chứng việc ký/xác minh, giả lập sửa đổi nội dung message để đảm bảo việc xác thực thất bại đúng như kỳ vọng.

---

### 📦 PHASE 3 – Lưu trữ dữ liệu với RocksDB

**Mục tiêu:** Thay thế file WAL định dạng JSON Lines và file checkpoint thô sơ bằng RocksDB hiệu năng cao.

**Các bước thực hiện:**
1. Khai báo thư viện `python-rocksdb` (hoặc `pyrocksdb`) trong `requirements.txt`.
2. Tạo module `storage/rocksdb_store.py`:
   - Định nghĩa lớp `KVStore` quản lý kết nối cơ sở dữ liệu:
     - Khởi tạo mở cơ sở dữ liệu RocksDB cục bộ cho mỗi node, chia thành các Column Family độc lập: `cf_wal` (lưu log đồng thuận), `cf_state` (lưu số dư tài khoản hiện tại), `cf_ledger` (lưu chuỗi giao dịch đã commit).
     - Định nghĩa hàm `put_wal(tx_id, state, vote, timestamp)` để ghi log đồng thuận. Key có dạng `tx_id:sequence`.
     - Định nghĩa hàm `read_last_state(tx_id)` để lấy trạng thái đồng thuận gần nhất từ `cf_wal`.
     - Định nghĩa hàm `put_state(account, balance)` và `get_balance(account)` để quản lý tài khoản.
     - Định nghĩa các hàm quản lý ghi/đọc chuỗi giao dịch trên `cf_ledger`.
3. Sửa đổi `node.py`:
   - Thay thế hoàn toàn import `WAL` và `WorldStateDB` cũ bằng `KVStore`.
   - Cập nhật logic phục hồi trạng thái sau crash: node khởi chạy sẽ tự động đọc checkpoint từ `cf_state` của RocksDB và quét phần WAL còn lại để đồng bộ.
4. **Kiểm thử:** Viết script kiểm tra khả năng lưu trữ bền vững (Persistence) của RocksDB khi tắt và khởi chạy lại tiến trình node.

---

### 🤝 PHASE 4 – Giao thức PBFT tiêu chuẩn (Practical Byzantine Fault Tolerance)

**Mục tiêu:** Thay thế giao thức đồng thuận đơn giản bằng thuật toán PBFT chuẩn chỉnh với khả năng chịu Byzantine thực tế.

**Các bước thực hiện:**
1. Tạo module `consensus/pbft.py` để định nghĩa State Machine PBFT.
2. Triển khai thuật toán PBFT qua các pha:
   - **Pre-prepare:** Node Leader (được chọn dựa trên view number hiện tại) gán nhãn Sequence Number cho giao dịch và broadcast thông điệp Pre-prepare tới toàn mạng.
   - **Prepare:** Các node nhận thông điệp Pre-prepare, xác thực tính hợp lệ của giao dịch, sau đó broadcast thông điệp Prepare.
   - **Commit:** Khi mỗi node thu thập đủ $2f$ thông điệp Prepare hợp lệ khớp với đề xuất (tổng cộng $2f+1$ bao gồm chính nó), node broadcast thông điệp Commit.
   - **Execute:** Khi thu thập đủ $2f+1$ thông điệp Commit hợp lệ, node thực thi giao dịch, lưu vào `cf_ledger` và trả kết quả về cho client.
3. Triển khai View Change protocol (`consensus/view_change.py`):
   - Khi node phát hiện Leader không tiến triển trong khoảng thời gian timeout, nó sẽ broadcast thông điệp `VIEW-CHANGE` yêu cầu chuyển sang view tiếp theo.
   - Thu thập đủ $2f+1$ thông điệp `VIEW-CHANGE` để bầu chọn và xác nhận Leader mới phát hành thông điệp `NEW-VIEW`.
4. Lưu toàn bộ trạng thái tin nhắn PBFT vào Column Family `cf_pbft_log` trong RocksDB để đảm bảo tính khôi phục trạng thái chuẩn xác sau crash.
5. **Kiểm thử:** Mô phỏng lỗi Byzantine (Node 0 gửi các thông điệp Pre-prepare mâu thuẫn cho các node khác nhau), kiểm chứng hệ thống vẫn đạt đồng thuận an toàn và nhất quán dữ liệu trên các node trung thực.

---

### 🐳 PHASE 5 – Container hóa (Docker) và Client độc lập

**Mục tiêu:** Đóng gói toàn bộ hệ thống chạy trên Docker Compose và phát triển Client tương tác bên ngoài.

**Các bước thực hiện:**
1. Viết `Dockerfile` cho các node chạy trên môi trường Linux tối giản (ví dụ: `python:3.11-slim`), cài đặt RocksDB dependencies và các Python requirements.
2. Viết file `docker-compose.yml` để tự động khởi chạy 4 service node (`node0` đến `node3`) trên một mạng ảo bridge. Mount thư mục dữ liệu RocksDB ra ngoài máy host.
3. Tạo file `client.py` chạy độc lập ngoài container:
   - Nhận giao dịch từ bàn phím hoặc file, ký giao dịch bằng Private Key của client.
   - Gửi giao dịch đến cổng HTTP/TCP của Node Leader.
   - Chờ đợi và xác thực phản hồi (đã kèm chữ ký số) từ ít nhất $f+1$ node để đảm bảo giao dịch đã được commit thực tế.
4. Cập nhật `main.py` thành công cụ điều phối chạy thử: tự khởi chạy Docker, kích hoạt client gửi giao dịch demo và tổng hợp kết quả.

---

### 🧪 PHASE 6 – Kiểm thử tích hợp toàn diện (Integration Testing)

**Mục tiêu:** Viết các kịch bản kiểm thử tự động bao phủ toàn bộ các trường hợp lỗi mạng, crash và Byzantine.

**Các test case bắt buộc:**
- **Test Case 1: Lỗi mạng ngẫu nhiên (Network Latency & Loss):** Sử dụng `tc` hoặc công cụ mô phỏng để tạo trễ và mất gói tin giữa các node, kiểm tra khả năng đạt đồng thuận.
- **Test Case 2: Phục hồi sau Crash (Crash Recovery):** Tắt đột ngột (`docker stop`) một node trung thực trong lúc hệ thống đang đồng thuận giao dịch $\rightarrow$ khởi động lại node $\rightarrow$ kiểm tra dữ liệu của node đó được đồng bộ về trạng thái mới nhất từ RocksDB và các node khác.
- **Test Case 3: Chống tấn công Byzantine:** Mô phỏng node Byzantine thực hiện gửi thông tin mâu thuẫn, kiểm tra hệ thống có bị phân rã dữ liệu hay không.

---

### 📖 PHASE 7 – Tài liệu & Triển khai

**Mục tiêu:** Viết tài liệu hướng dẫn vận hành chi tiết.

**Các bước thực hiện:**
1. Tạo script `run_demo.sh` để người dùng chỉ cần chạy 1 click là có thể khởi tạo cụm mạng docker, chạy client gửi giao dịch và hiển thị ledger cuối cùng.
2. Cập nhật `README.md` hướng dẫn cấu hình tham số mạng, cài đặt môi trường và các lệnh chạy test case chi tiết.
