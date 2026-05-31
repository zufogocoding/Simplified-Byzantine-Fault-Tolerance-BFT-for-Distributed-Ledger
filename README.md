# Simplified Byzantine Fault Tolerance (BFT) for Distributed Ledger

## Mô tả

Chương trình mô phỏng cơ chế đồng thuận **Byzantine Fault Tolerance (BFT)**
đơn giản hóa cho một Distributed Ledger, sử dụng thư viện `multiprocessing`
của Python.

## Kiến trúc

- **4 nút (site)**, mỗi nút là một tiến trình độc lập.
- **Site 0**: Nút độc hại luôn gửi phiếu ABORT.
- **Site 1, 2, 3**: Nút trung thực, gửi phiếu COMMIT.
- **Quy tắc 3f+1** (f=1): Cần ≥ 3 phiếu COMMIT để đạt đồng thuận.

## Kịch bản mô phỏng

1. Tất cả 4 nút nhận giao dịch và broadcast phiếu bầu.
2. Site 2 broadcast COMMIT thành công, sau đó **crash** ngay lập tức.
3. Site 0, 1, 3 thu thập đủ 4 phiếu (3 COMMIT + 1 ABORT) → **COMMIT**.
4. Site 2 được khởi động lại, đọc log phát hiện trạng thái READY → vào chế độ phục hồi.
5. Site 2 gửi **REQUEST_VOTES** đến các nút còn sống.
6. Site 0, 1, 3 gửi lại phiếu của mình (**VOTE_RESPONSE**).
7. Site 2 nhận đủ phiếu, đếm được 3 COMMIT → **COMMIT**.
8. Kết quả: Tất cả 3 nút trung thực đều COMMIT.

## Cách chạy

```bash
python main.py
```

## Output

- Console: Hiển thị chi tiết quá trình trao đổi phiếu, crash, phục hồi.
- File log `site_0.log` đến `site_3.log`: Ghi đầy đủ sự kiện (SEND, RECEIVED, STATE, CRASH, RECOVERY_START...).

## Yêu cầu

- Python 3.8+
- Không cần thư viện ngoài.
