# Kịch bản Video Demo: PBFT Distributed Ledger

*(Độ dài dự kiến: 6 - 8 phút)*

Kịch bản này hướng dẫn thực hiện các thao tác trên màn hình kết hợp lời thoại thuyết minh, bao gồm **3 kịch bản thực nghiệm** chứng minh đầy đủ tính năng của hệ thống.

---

## Chuẩn bị trước khi quay

```bash
# 1. Reset toàn bộ DB và logs
make down
make clean-db
make clean-logs

# 2. Build lại image Docker
make build

# 3. Mở 4 cửa sổ terminal sẵn:
#    Terminal A: logs Node 0 (Leader)
#    Terminal B: logs Node 1 (Backup)
#    Terminal C: logs Node 2, 3
#    Terminal D: chạy client TUI
```

---

## Phần 1: Giới thiệu hệ thống (0:00 - 1:00)

**Visual:**
- Hiển thị cấu trúc thư mục `csdlpt/` trong VS Code
- Trỏ vào các file cốt lõi: `consensus/pbft.py`, `consensus/view_change.py`, `storage/rocksdb_store.py`, `network/connection_pool.py`
- Chạy: `cat docker-compose.yml` để show cấu hình 4 container

**Lời thoại:**
> *"Em xin chào Thầy/Cô. Em tên Phạm Thành Nhựt Trọng, mã sinh viên N23DCCN132. Hôm nay em demo đề tài Cơ sở dữ liệu phân tán: Hệ thống Sổ cái Phân tán chịu lỗi Byzantine thực tế, triển khai giao thức PBFT theo đặc tả gốc Castro-Liskov 1999.*
>
> *Hệ thống gồm 4 tầng: Mạng TCP Connection Pool, Đồng thuận PBFT 3 pha, Lưu trữ RocksDB, và Client độc lập. Toàn bộ thông điệp được ký số bằng Ed25519. Em sẽ demo 3 kịch bản chính: giao dịch bình thường, crash và phục hồi, và phát hiện node Byzantine.*"

---

## Phần 2: Khởi động mạng lưới (1:00 - 1:45)

**Visual:**
```bash
# Terminal D:
make up
docker-compose ps
```
→ Hiển thị 4 container `node0..node3` đang `Up`

```bash
# Terminal A:
docker-compose logs -f node0
```
→ Thấy log: `PBFT engine started`, `TCP Connection Pool started`, `Heartbeat thread started`

**Lời thoại:**
> *"Hệ thống gồm N=4 node. Với f=1 node lỗi tối đa chịu được, Quorum cần 2f+1 = 3 phiếu đồng thuận. Node 0 là Leader mặc định ở View 0. Em dùng Docker Compose để khởi chạy 4 container độc lập.*
>
> *Có thể thấy trong log: Connection Pool đang thiết lập socket TCP liên tục đến các node còn lại. Heartbeat PING/PONG cũng đang chạy ngầm để phát hiện node chết.*"

---

## Phần 3: Kịch bản 1 — Giao dịch bình thường và Redirect (1:45 - 3:30)

**Visual:**
```bash
# Terminal D: Mở TUI client
python client_tui.py
```
→ Nhập: Node = `localhost:5001` (Backup), Sender = `A`, Receiver = `B`, Amount = `10`

Chỉ vào log Terminal B (Node 1):
```
>>> [TCP Server 5001] Nhan message type=107 tu Node client
>>> CLIENT_REQUEST nhan boi Backup Node 1 → REDIRECT ve Leader Node 0
```

Chỉ vào log Terminal A (Node 0):
```
>>> PBFT_PRE_PREPARE: tx_id=1 | Leader gui PRE-PREPARE (seq=1, view=0, digest=...)
>>> PBFT_PREPARE_RX: Nhan PREPARE tu Node 1 (co 2/3)
>>> PBFT_PREPARE_RX: Nhan PREPARE tu Node 2 (co 3/3)
>>> PBFT SITE 0: TX 1 DA DUOC THUC THI (seq=1, view=0)
```

```bash
# Kiểm tra DB sau giao dịch:
python check_db.py
```
→ `A: 90, B: 110, Ledger: 1 giao dịch, tất cả 4 node đồng nhất`

**Lời thoại:**
> *"Em cố tình gửi giao dịch đến Node 1 — đây là Backup, không phải Leader. Node 1 phát hiện ngay và gửi thông báo REDIRECT chỉ định cổng của Leader về cho Client. Client tự động kết nối lại đến Node 0.*
>
> *Leader Node 0 ký và broadcast PRE-PREPARE kèm digest SHA-256 của giao dịch. Các Backup kiểm tra chữ ký, kiểm tra digest, rồi broadcast PREPARE. Sau khi Leader nhận đủ 2f+1=3 phiếu PREPARE — chuyển sang pha COMMIT. Sau 3 COMMIT, giao dịch được thực thi và ghi vào RocksDB.*
>
> *Em chạy script kiểm tra DB. Kết quả: tất cả 4 node đều ghi nhận A còn 90, B được 110 — tính nhất quán hoàn hảo.*"

---

## Phần 4: Kịch bản 2 — Crash và Crash Recovery (3:30 - 5:30)

**Visual:**
```bash
# Terminal D: Tắt Node 1
docker-compose stop node1
docker-compose ps
```
→ Node 1 hiển thị `Exited`

```bash
# Gửi giao dịch tiếp theo (hệ thống còn 3 node vẫn đủ Quorum)
python client_tui.py
```
→ Nhập: Node `localhost:5000`, Sender `B`, Receiver `C`, Amount `5`

Chỉ vào log Terminal A:
```
>>> PBFT SITE 0: TX 2 DA DUOC THUC THI (seq=2, view=0)
>>> CHECKPOINT_TRIGGER: Khoi tao checkpoint (seq=2, digest=...)
>>> CHECKPOINT_STABLE: Checkpoint tai seq 2 da tro nen STABLE!
```

```bash
# Bật lại Node 1
docker-compose start node1
sleep 3
docker-compose logs --tail=20 node1
```
→ Thấy log:
```
>>> STARTUP: Phuc hoi tu checkpoint seq=0, balances={A:100,B:100,...}
>>> RECOVERY: Replay WAL entry seq=1, tx_id=1 (A->B: 10)
>>> RECOVERY: Replay WAL entry seq=2, tx_id=2 (B->C: 5)
>>> Node 1 da phuc hoi day du!
```

```bash
python check_db.py
```
→ `Node 1: A=90, B=105, C=5 — đồng nhất với phần còn lại`

**Lời thoại:**
> *"Em giả lập sự cố sập nguồn bằng cách cưỡng bức tắt container Node 1. Lúc này còn 3 node — vừa đủ Quorum 2f+1=3. Em gửi tiếp giao dịch 'B chuyển 5 cho C'. Hệ thống vẫn đạt đồng thuận và commit bình thường. Sau đó Checkpoint được kích hoạt và trở thành Stable Checkpoint.*
>
> *Em khởi động lại Node 1. Node 1 đọc Checkpoint mới nhất từ RocksDB, khôi phục World State, rồi scan WAL để replay lại hai giao dịch đã bỏ lỡ — theo đúng thứ tự sequence. Kết quả: Node 1 đồng nhất hoàn toàn với hệ thống trong vài giây.*"

---

## Phần 5: Kịch bản 3 — Byzantine Node và Safety (5:30 - 7:00)

**Visual:**
```bash
# Dừng cluster cũ, chạy cluster Byzantine (Node 0 is_malicious=True)
make down
docker-compose -f docker-compose-byzantine.yml up -d
docker-compose -f docker-compose-byzantine.yml logs -f
```
→ Thấy log Node 0:
```
>>> PBFT_BYZANTINE: Node 0 (Byzantine): thuc hien EQUIVOCATION tai seq 1!
>>> PRE-PREPARE(digest=d1) → Node 1, 2
>>> PRE-PREPARE(digest=d2, FAKE BYZANTINE) → Node 3
```

Thấy log Node 3:
```
>>> PBFT_DIGEST_MISMATCH: PREPARE digest sai tu Node 1 tai seq 1
>>> PBFT_EQUIVOCATION: Equivocation tai seq 1: d1 != d2!
>>> VIEW_CHANGE_START: Yeu cau view change tu 0 -> 1
```

Thấy log Node 1, 2, 3:
```
>>> NEW VIEW: Node 1 la leader cua view 1
>>> SITE 1: DA CHUYEN SANG VIEW 1, LEADER = NODE 1
```

**Lời thoại:**
> *"Kịch bản nghiêm trọng nhất: Leader Node 0 bị compromise, thực hiện tấn công Equivocation. Nó gửi hai PRE-PREPARE khác nhau cho cùng một Sequence Number — nội dung thật đến Node 1, 2 và nội dung giả đến Node 3.*
>
> *Kết quả: Node 3 nhận PREPARE từ Node 1 với digest sai → drop ngay lập tức, phát hiện Equivocation và kích hoạt View Change. Hệ thống bầu chọn Leader mới là Node 1 ở View 1. Giao dịch giả mạo không bao giờ đạt được Quorum — tính Safety được bảo đảm hoàn toàn.*"

---

## Phần 6: Tổng kết (7:00 - 8:00)

**Visual:**
- Hiển thị bảng so sánh Trước/Sau trong `ARCHITECTURAL_DECISIONS.md`
- Hiển thị `REPORT_UPGRADE_PBFT.md` Chương III tóm tắt

**Lời thoại:**
> *"Tóm lại, hệ thống đã triển khai đầy đủ giao thức PBFT theo đặc tả gốc với ba tính chất cốt lõi: Safety — không bao giờ có hai node trung thực lưu trữ kết quả khác nhau; Liveness — hệ thống luôn tiến lên dù có node lỗi; và Durability — dữ liệu không bao giờ mất qua crash.*
>
> *Em xin chân thành cảm ơn Thầy/Cô đã lắng nghe. Em sẵn sàng trả lời câu hỏi.*"

---

## Lưu ý kỹ thuật khi quay

1. **Zoom terminal** vào các dòng log `PBFT_DIGEST_MISMATCH`, `EQUIVOCATION`, `CHECKPOINT_STABLE` để khán giả thấy rõ.
2. **Dùng `python check_db.py`** thay vì script python inline — gọn hơn, dễ nhìn hơn.
3. **Chuẩn bị sẵn** các lệnh trong notepad, copy-paste để tránh typo.
4. **Khoảng dừng** 2-3 giây sau mỗi lệnh để log kịp hiển thị trước khi nói.
5. **Phần Byzantine:** nếu không có `docker-compose-byzantine.yml`, có thể sửa `config.py` set `is_malicious_nodes=[0]` và chạy `main.py` để reproduce.
