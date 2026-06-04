# 🎬 KỊCH BẢN VIDEO DEMO HỆ THỐNG PBFT DISTRIBUTED LEDGER THỰC TẾ
*(Độ dài dự kiến: 5 - 7 phút)*

Kịch bản này hướng dẫn bạn thực hiện các thao tác trên màn hình (Visual) kết hợp với lời thoại thuyết minh (Audio) chi tiết bằng tiếng Việt để quay video demo đồ án trước hội đồng.

---

## 📌 PHẦN 1: GIỚI THIỆU CHUNG (Thời gian: 0:00 - 1:00)

*   **Visual (Hành động trên màn hình):**
    *   Mở terminal hoặc trình soạn thảo mã nguồn VS Code, hiển thị cấu trúc thư mục của dự án `bft-ledger/`.
    *   Trỏ chuột vào các file cốt lõi: [config.py](file:///home/trongzufo/csdlpt/config.py), [consensus/pbft.py](file:///home/trongzufo/csdlpt/consensus/pbft.py), [storage/rocksdb_store.py](file:///home/trongzufo/csdlpt/storage/rocksdb_store.py), [network/tcp_server.py](file:///home/trongzufo/csdlpt/network/tcp_server.py).
*   **Audio (Lời thoại thuyết minh):**
    *   "Xin chào Thầy/Cô và các bạn. Em tên là Phạm Thành Nhựt Trọng. Hôm nay, em xin phép được demo sản phẩm đồ án môn **Cơ sở dữ liệu phân tán** với đề tài: **Hệ thống Sổ cái Phân tán chịu lỗi Byzantine (PBFT) thực tế**."
    *   "Từ phiên bản mô phỏng thô sơ ban đầu chạy trên các tiến trình dùng chung bộ nhớ và hàng đợi Queue, em đã nâng cấp toàn diện dự án lên một hệ thống phân tán thực tế chạy trên mạng TCP socket thật. Mạng lưới được thiết lập với chữ ký số Ed25519 để chống giả mạo, cơ sở dữ liệu RocksDB (LSM-Tree) làm công cụ lưu trữ ghi nhật ký WAL/Ledger và được đóng gói hoàn chỉnh bằng Docker Container."
    *   "Sau đây, em xin phép được khởi động hệ thống và trình diễn các tính năng vượt trội của đồ án."

---

## 📌 PHẦN 2: KHỞI CHẠY MẠNG LƯỚI PHÂN TÁN (Thời gian: 1:00 - 2:00)

*   **Visual (Hành động trên màn hình):**
    *   Mở terminal, gõ lệnh `cat docker-compose.yml` để show cấu trúc 4 node chạy độc lập.
    *   Gõ lệnh khởi động cụm node:
        ```bash
        docker-compose down && docker-compose up -d
        ```
    *   Chờ vài giây, gõ `docker-compose ps` để chứng minh cả 4 node (`node0`, `node1`, `node2`, `node3`) đang chạy nền bình thường trên các port TCP từ 5000 đến 5003.
*   **Audio (Lời thoại thuyết minh):**
    *   "Đầu tiên, hệ thống của chúng ta bao gồm N = 4 node. Em cấu hình 4 node này chạy độc lập dưới dạng các container Docker, tương ứng với 4 site phân tán thực sự. Node 0 đóng vai trò là Leader mặc định trong View 0."
    *   "Em sẽ sử dụng lệnh `docker-compose up -d` để khởi chạy mạng lưới. Như Thầy/Cô có thể thấy trên màn hình, cả 4 node đã khởi động thành công và đang lắng nghe kết nối TCP trên các cổng dịch vụ riêng biệt từ 5000 đến 5003."

---

## 📌 PHẦN 3: GỬI GIAO DỊCH & CƠ CHẾ REDIRECT (Thời gian: 2:00 - 3:30)

*   **Visual (Hành động trên màn hình):**
    *   Chạy Client gửi giao dịch đến `node1` (là Backup Node, không phải Leader):
        ```bash
        python client.py --node localhost:5001 --op "A chuyen 10 cho B"
        ```
    *   Chỉ tay vào các dòng log in ra trên Terminal của Client:
        *   `Redirect: Node thong bao gui ve Leader moi...`
        *   `Gui lai...`
        *   `Nhan SUCCESS tu Node 2, Node 1...`
        *   `Giao dich hoan thanh thanh cong!`
*   **Audio (Lời thoại thuyết minh):**
    *   "Tiếp theo, em sẽ chạy một tiến trình Client độc lập để gửi yêu cầu giao dịch: *'A chuyển 10 cho B'*. Client này sẽ ký giao dịch bằng thuật toán Ed25519 bằng khóa bí mật của chính nó."
    *   "Thay vì gửi trực tiếp đến Leader (Node 0), em cố tình gửi giao dịch này đến Node 1 là một nút Backup. Hãy quan sát phản hồi từ màn hình."
    *   "Rất tuyệt vời! Node 1 đã nhận diện nó không phải Leader, lập tức từ chối và trả về thông báo chuyển hướng (**Redirect**) chỉ định cổng của Leader là Node 0 (localhost:5000). Client đã tự động kết nối lại đến Node 0 và gửi lại giao dịch."
    *   "Leader nhận tin, xác thực chữ ký của Client, băm giao dịch và kích hoạt quy trình đồng thuận PBFT 3 pha chéo: Pre-prepare, Prepare, và Commit. Khi Client nhận đủ $f+1$ (tức là 2) chữ ký phản hồi xác nhận thành công từ các node trung thực, giao dịch chính thức được coi là hoàn tất."

---

## 📌 PHẦN 4: GIẢ LẬP SỰ CỐ CRASH & PHỤC HỒI ROCKSDB (Thời gian: 3:30 - 5:00)

*   **Visual (Hành động trên màn hình):**
    *   Thực hiện tắt Node 1 bằng lệnh:
        ```bash
        docker-compose stop node1
        ```
    *   Gửi tiếp một giao dịch mới qua Client để chứng minh hệ thống vẫn hoạt động:
        ```bash
        python client.py --node localhost:5000 --op "B chuyen 5 cho C"
        ```
    *   Bật lại Node 1 bằng lệnh:
        ```bash
        docker-compose start node1
        ```
    *   Xem log của Node 1 để chứng minh nó khôi phục trạng thái bằng RocksDB WAL:
        ```bash
        docker-compose logs node1 | grep -E "RESTART|RECOVERY|Stable Checkpoint"
        ```
*   **Audio (Lời thoại thuyết minh):**
    *   "Bây giờ, em sẽ giả lập một sự cố sập nguồn mạng thực tế. Em sẽ cưỡng bức tắt container Node 1 bằng lệnh `docker-compose stop node1`."
    *   "Lúc này hệ thống chỉ còn 3 nút hoạt động. Theo lý thuyết PBFT với $N=4, f=1$, hệ thống cần tối thiểu $2f+1=3$ nút trung thực để duy trì đồng thuận. Do đó, khi em gửi giao dịch mới *'B chuyển 5 cho C'*, hệ thống vẫn đạt Quorum thành công và cam kết giao dịch bình thường."
    *   "Tiếp theo, em khởi động lại Node 1 bằng lệnh `docker-compose start node1`."
    *   "Ngay sau khi khởi động, Node 1 phát hiện nó bị thiếu hụt dữ liệu so với mạng lưới. Nó tiến hành quét tệp lưu trữ bền vững RocksDB của mình, đọc stable checkpoint gần nhất, và thực thi lại các bản ghi nhật ký ghi trước (Write-Ahead Log - WAL) để khôi phục World State số dư tài khoản về trạng thái mới nhất."
    *   "Trên log của Node 1 hiển thị rõ quá trình khôi phục: đọc checkpoint, gửi request đồng bộ trạng thái và ghi đè dữ liệu thành công."

---

## 📌 PHẦN 5: XÁC MINH CƠ SỞ DỮ LIỆU ROCKSDB & TỔNG KẾT (Thời gian: 5:00 - 6:00)

*   **Visual (Hành động trên màn hình):**
    *   Gõ lệnh kiểm tra trực tiếp dữ liệu RocksDB trên đĩa của Node 1 (sử dụng đoạn script python đọc cơ sở dữ liệu):
        ```bash
        python -c '
        from storage.rocksdb_store import KVStore
        store = KVStore(1)
        print("Sổ cái Ledger tại Node 1:")
        for tx in store.get_ledger():
            print(f"  TX {tx[\"tx_id\"]}: {tx[\"data\"]}")
        print("Số dư tài khoản hiện tại:")
        for acc in ["A", "B", "C"]:
            print(f"  {acc}: {store.get_balance(acc)}")
        store.close()
        '
        ```
    *   Kết quả hiển thị trên màn hình:
        *   `A: 90`
        *   `B: 105`
        *   `C: 5`
*   **Audio (Lời thoại thuyết minh):**
    *   "Để chứng minh dữ liệu được lưu trữ bền vững và đồng nhất tuyệt đối, em sẽ chạy một đoạn mã Python truy vấn trực tiếp cơ sở dữ liệu RocksDB cục bộ của Node 1 trên đĩa."
    *   "Kết quả trả về cho thấy Sổ cái chứa đầy đủ cả hai giao dịch đã commit. Số dư tài khoản được cập nhật chính xác: Tài khoản A còn 90 do đã chuyển 10, Tài khoản B nhận 10 rồi chuyển 5 còn 105, và Tài khoản C nhận 5."
    *   "Như vậy, đồ án đã hoàn thành xuất sắc các yêu cầu kỹ thuật phân tán thực tế. Hệ thống giải quyết triệt để lỗi Byzantine độc hại nhờ giao thức PBFT 3 pha và bảo đảm an toàn lưu trữ đĩa cứng bền vững nhờ RocksDB LSM-Tree."
    *   "Em xin chân thành cảm ơn Thầy/Cô và các bạn đã lắng nghe phần thuyết trình của em!"

---

## 💡 Mẹo nhỏ khi quay video:
1.  **Chuẩn bị môi trường:** Hãy dọn dẹp các log và DB cũ bằng cách chạy `./run_demo.sh` một lần trước khi quay để đảm bảo các ổ đĩa sạch sẽ.
2.  **Tốc độ gõ:** Nên chuẩn bị sẵn các dòng lệnh ra file nháp (Notepad) để chỉ cần copy-paste vào Terminal nhằm tiết kiệm thời gian và tránh gõ sai cú pháp khi nói.
3.  **Hậu kỳ:** Bạn có thể phóng to (zoom) vùng Terminal hiển thị các log REDIRECT hoặc RECOVERY để người xem nhìn rõ các bằng chứng thực nghiệm quan trọng.
