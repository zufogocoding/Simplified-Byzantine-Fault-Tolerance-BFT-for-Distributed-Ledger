# Simplified BFT cho Distributed Ledger

Mo phong co che dong thuan **Byzantine Fault Tolerance (BFT)** don gian hoa
cho Distributed Ledger, su dung thu vien `multiprocessing` cua Python.

## Kien truc

- **4 nut (site)**, moi nut la mot tien trinh doc lap.
- **Site 0**: Nut doc hai luon gui phieu ABORT.
- **Site 1, 2, 3**: Nut trung thuc, gui phieu COMMIT.
- **Quy tac 3f+1** (f=1): Can >= 3 phieu COMMIT de dat dong thuan.

## Kich ban mo phong

1. Tat ca 4 nut nhan giao dich va broadcast phieu bau.
2. Site 2 broadcast COMMIT thanh cong, sau do crash ngay lap tuc.
3. Site 0, 1, 3 thu thap du 4 phieu (3 COMMIT + 1 ABORT) -> COMMIT.
4. Site 2 duoc khoi dong lai, doc log phat hien trang thai READY -> vao che do phuc hoi.
5. Site 2 gui **REQUEST_VOTES** den cac nut con song.
6. Site 0, 1, 3 gui lai phieu cua minh (**VOTE_RESPONSE**).
7. Site 2 nhan du phieu, dem duoc 3 COMMIT -> COMMIT.
8. Ket qua: Tat ca 3 nut trung thuc deu COMMIT.

## Cach chay

```bash
python main.py
```

## Output

- **Console**: Hien thi chi tiet qua trinh trao doi phieu, crash, phuc hoi.
- **File log** trong `logs/`: `site_0.log` den `site_3.log`.
  Cac su kien: SEND, RECEIVED, STATE, CRASH, RECOVERY_START, ...

## Log Management

- Moi site ghi log ra file rieng trong thu muc `logs/`.
- Log duoc flush xuong dia ngay lap tuc de tranh mat du lieu khi crash.
- Dinh dang: `[timestamp] Site <id> | EVENT | Chi tiet`

## Yeu cau

- Python 3.8+
- Khong can thu vien ben ngoai.
