# 🏛️ QUYẾT ĐỊNH KIẾN TRÚC: CONSENSUS LAYER VS STORAGE ENGINE (BFT & POSTGRESQL)

Tài liệu này ghi lại phân tích kiến trúc, các đánh giá kỹ thuật và hướng tiếp cận thực tế khi xây dựng một hệ thống Cơ sở dữ liệu phân tán có khả năng chống lỗi Byzantine (Byzantine Fault Tolerance - BFT).

---

## 1. Bản chất kỹ thuật: Tại sao không thể "chỉ dùng" PostgreSQL?

Trong môi trường thực tế, khi xây dựng các cơ sở dữ liệu phân tán thế hệ mới (như CockroachDB, Google Spanner, YugabyteDB) hoặc các nền tảng Blockchain/Distributed Ledger (như Ethereum, Hyperledger Fabric):
* **PostgreSQL** là một **Storage Engine** cực kỳ xuất sắc. Nó cung cấp ACID ở mức single-node, quản lý transaction bằng WAL (Write-Ahead Log), thực hiện phân chỉ mục bằng B-Tree và dọn dẹp dữ liệu bằng cơ chế Checkpointing.
* Tuy nhiên, hệ sinh thái nhân bản (Replication) có sẵn của PostgreSQL chỉ hỗ trợ mô hình lỗi **CFT (Crash Fault Tolerance)**. Tức là các node trung thực tuyệt đối và chỉ có thể chết (crash) chứ không thể gửi dữ liệu sai lệch (nói dối/equivocate).

### Kịch bản lỗi Byzantine (Equivocation) trên Postgres:
Nếu một Node Master Postgres bị chiếm quyền kiểm soát (Byzantine Node):
1. Master gửi bản ghi WAL của giao dịch $TX_A$ tới Node Standby 1.
2. Cùng lúc đó, Master gửi bản ghi WAL của giao dịch $TX_B$ (mâu thuẫn với $TX_A$) tới Node Standby 2.
3. Bản thân Postgres không có giao thức đồng thuận nhiều bên (Multi-party Consensus) để phát hiện sự mâu thuẫn này. Hệ thống sẽ ngay lập tức bị phân rã trạng thái (split-brain) và mất tính nhất quán dữ liệu.

> 💡 **Kết luận kiến trúc:** Để đưa PostgreSQL vào một hệ phân tán BFT, ta **vẫn bắt buộc phải viết một lớp BFT Consensus (như Python core logic của dự án này) nằm phía trước Postgres**. Lớp này đóng vai trò quyết định thứ tự giao dịch đồng nhất trên toàn mạng, sau đó mới ra lệnh cho Postgres cục bộ thực hiện ghi đĩa.

---

## 2. So sánh Kiến trúc Thực tế vs. Mô phỏng Học thuật

| Tiêu chí | Mô phỏng Hiện tại (SQLite/Custom File State) | Hệ thống Sản xuất (Production BFT-backed Postgres) |
|---|---|---|
| **Consensus Layer** | Python Multiprocessing BFT (3f+1) | Tendermint Core / PBFT engine viết bằng Go/Rust |
| **Storage Engine** | File State JSON có cấu trúc + Checkpoint | **PostgreSQL** hoặc RocksDB/LevelDB gắn tại mỗi node |
| **Giao tiếp giữa các Node** | IPC Queue (Shared Memory) | gRPC / TCP sockets mã hóa SSL/TLS |
| **Tính di động (Portability)** | **Cực kỳ cao** (Chỉ cần chạy lệnh `python main.py`, không cần cấu hình môi trường) | **Thấp** (Yêu cầu cài đặt hạ tầng mạng, cấu hình Docker/Kubernetes cho từng Postgres instance) |
| **Mục đích thiết kế** | Tối ưu học tập, làm nổi bật logic đồng thuận bên trong (White-box) | Tối ưu hiệu năng đọc/ghi thực tế, bảo mật dữ liệu sản xuất (Black-box) |

---

## 3. Đề xuất Kiến trúc Scale-Up chuẩn Doanh nghiệp (Enterprise Scale-Up)

Nếu nâng cấp dự án này lên một cơ sở dữ liệu phân tán BFT chạy trong môi trường thực tế, kiến trúc sẽ được thiết kế như sau:

```
[ Client Applications ]
        │
        ▼ (Gửi SQL Query / Transactions)
┌─────────────────────────────────────────────────────────┐
│              1. TẦNG ĐỒNG THUẬN (BFT CONSENSUS)         │
│  - Nhận transaction từ Client.                          │
│  - Sử dụng giao thức BFT để thống nhất thứ tự TX.       │
│  - Xác thực chữ ký số thực tế (ECDSA/Secp256k1).        │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼ (Đẩy chuỗi giao dịch đã sắp xếp thứ tự)
┌─────────────────────────────────────────────────────────┐
│            2. TẦNG LƯU TRỮ CỤC BỘ (LOCAL STORAGE)       │
│  - Mỗi Node chạy độc lập một instance **PostgreSQL**.   │
│  - Thực thi các câu lệnh SQL đã được Consensus thông qua.│
│  - Lưu World State hiện tại, quản lý Index và Transaction.│
└─────────────────────────────────────────────────────────┘
```

### Tại sao kiến trúc này tối ưu cho 1 triệu transaction?
* **PostgreSQL** giải quyết hoàn toàn bài toán truy vấn phức tạp $O(1)$ thông qua chỉ mục (Index) và tối ưu hóa câu lệnh (Query Planner).
* **BFT Consensus** chỉ cần quan tâm đến việc đồng nhất thứ tự các transaction đầu vào, giải phóng nó khỏi việc quản lý dữ liệu trạng thái chi tiết.
* **Checkpointing** của Postgres (thông qua `bgwriter` và `checkpoint` processes) tự động đồng bộ dirty pages xuống đĩa và cắt ngắn WAL định kỳ, ngăn ngừa việc phình to dung lượng và giảm thời gian recovery xuống mức mili-giây.

---

## 4. Tổng kết Quyết định Thiết kế của Đồ án
* Giữ nguyên lớp đồng thuận tự viết bằng Python và cấu trúc World State DB/Checkpointing gọn nhẹ bằng file cục bộ nhằm mục đích **đảm bảo tính di động tối đa**, giúp đồ án dễ dàng kiểm thử và chấm điểm trên mọi môi trường.
* Đồng thời, cung cấp tài liệu kiến trúc này để chứng minh sinh viên hiểu rõ cấu trúc hạ tầng thực tế và có khả năng định hướng nâng cấp hệ thống sử dụng PostgreSQL khi chuyển dịch sang môi trường sản xuất quy mô lớn.
