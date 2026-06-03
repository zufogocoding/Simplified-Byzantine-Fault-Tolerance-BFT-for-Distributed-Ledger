# Simplified BFT cho Distributed Ledger

Mo phong co che dong thuan **Byzantine Fault Tolerance (BFT)** don gian hoa
cho Distributed Ledger, su dung thu vien `multiprocessing` cua Python.

## Tai sao BFT ma khong dung Paxos?

| | Paxos | BFT (3f+1) |
|--|-------|------------|
| **Loi xu ly** | Crash fault (node dung hoat dong) | Byzantine fault (node noi doi) |
| **Equivocation** | Khong chong duoc | Chong duoc |
| **So node** | 2f+1 | 3f+1 |
| **Quorum** | f+1 | 2f+1 |

**Equivocation** la khi node doc hai gui phieu KHAC NHAU cho cac node
khac nhau (vi du: COMMIT cho Site 0,2 nhung ABORT cho Site 1,3).
Paxos gia dinh dieu nay khong xay ra. BFT duoc thiet ke de chong lai no.

## Kien truc

- **4 nut (site)**, moi nut la mot tien trinh doc lap.
- **Site 0**: Byzantine — thuc hien **equivocation** (gui phieu mau thuan).
- **Site 1, 3**: Trung thuc, luon gui phieu COMMIT.
- **Site 2**: Trung thuc, crash sau TX 1, phuc hoi tu WAL.
- **Quy tac 3f+1** (f=1): Can >= 3 phieu COMMIT (quorum = 2f+1) de dong thuan.

## Cac thanh phan chinh

### 1. Write-Ahead Log (WAL)
- File JSON Lines trong thu muc `wal/`, thay vi parse text log.
- Ghi trang thai (`INIT`, `READY`, `COMMIT`) va phieu bau.
- Dung de phuc hoi sau crash — doc WAL thay vi parse log text.
- Flush + fsync dam bao ghi xuong disk truoc khi crash.

### 2. Ledger (So cai phan tan)
- Moi site duy tri mot ledger rieng (danh sach TX da commit).
- Sau khi dat dong thuan COMMIT, giao dich duoc ghi vao ledger.
- Sau crash, ledger duoc khoi phuc tu WAL.

### 3. Chu ky so (gia lap)
- Moi message duoc "ky" bang sender ID.
- Node nhan xac thuc chu ky truoc khi chap nhan.
- Trong thuc te se dung RSA/ECDSA.

## Kich ban mo phong

### Phase 1: TX 1 (equivocation + crash + recovery)

1. Tat ca 4 site nhan giao dich TX 1.
2. **Site 0** (Byzantine) thuc hien **equivocation**:
   - Gui `COMMIT` cho Site 0, 2 (site chan)
   - Gui `ABORT` cho Site 1, 3 (site le)
3. **Site 2** broadcast `COMMIT` thanh cong, sau do **crash**.
4. Site 0, 1, 3 thu thap phieu:
   - Site 1 nhan: ABORT(0) + COMMIT(1) + COMMIT(2) + COMMIT(3) = 3 COMMIT
   - 3 >= quorum(3) -> **COMMIT** (bat chap equivocation!)
5. Site 2 duoc khoi dong lai, doc **WAL** -> READY -> vao che do phuc hoi.
6. Site 2 gui `REQUEST_VOTES`, nhan lai phieu, dem 3 COMMIT -> **COMMIT**.

### Phase 2: TX 2 (binh thuong, khong crash)

7. Tat ca 4 site xu ly TX 2.
8. Site 0 van equivocate, nhung 3 site trung thuc van dat **COMMIT**.
9. **Ket qua**: Ledger cua 3 site trung thuc deu co 2 giao dich.

## Cach chay

```bash
python main.py
```

## Output

- **Console**: Chi tiet qua trinh equivocation, crash, phuc hoi.
- **File log** trong `logs/`: `site_0.log` den `site_3.log`.
- **WAL** trong `wal/`: `site_0.wal` den `site_3.wal` (JSON Lines).

## Cau truc thu muc

```
csdlpt/
├── config.py        # Hang so, tham so BFT, danh sach giao dich
├── logger.py        # Ghi log su kien (console + file)
├── wal.py           # Write-Ahead Log — crash recovery (JSON Lines)
├── network.py       # Truyen thong + chu ky so gia lap
├── consensus.py     # ★ State Machine BFT (trai tim giao thuc)
├── main.py          # Orchestrator — dieu phoi mo phong
├── README.md        # Tai lieu
├── logs/            # Event log (text, auto-generated)
│   ├── site_0.log
│   ├── site_1.log
│   ├── site_2.log
│   └── site_3.log
└── wal/             # Write-Ahead Log (JSON Lines, auto-generated)
    ├── site_0.wal
    ├── site_1.wal
    ├── site_2.wal
    └── site_3.wal
```

### Kien truc module

```
config.py ──> logger.py ──> consensus.py ──> main.py
    │                            ▲               │
    └──> wal.py ─────────────────┘               │
    │                            ▲               │
    └──> network.py ─────────────┘               │
                                                 │
    main.py import consensus.site_main() <───────┘
```

- **config.py**: Dinh nghia F, N, QUORUM, state constants
- **logger.py**: Ghi log co timestamp, flush xuong disk (fsync)
- **wal.py**: Write-Ahead Log — dam bao durability, crash recovery
- **network.py**: Truyen thong co chu ky so, do tre mang ngau nhien
- **consensus.py**: Toan bo logic BFT: broadcast, collect, decide, recover
- **main.py**: Chi dieu phoi process lifecycle, khong chua logic giao thuc

## Cong thuc BFT

```
f = 1                  # So node Byzantine toi da
N = 3f + 1 = 4         # Tong so node can thiet
Quorum = 2f + 1 = 3    # So phieu COMMIT can de dong thuan
```

Voi N=4 va f=1: du cho 1 node doc hai gui phieu mau thuan (equivocation),
3 node trung thuc van co du quorum (3 phieu COMMIT) de dat dong thuan.

## Yeu cau

- Python 3.8+
- Khong can thu vien ben ngoai.
