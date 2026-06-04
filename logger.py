"""
Logger — Ghi log su kien ra file va console.

Moi site co mot file log rieng trong thu muc logs/.
Log duoc flush xuong disk ngay lap tuc (os.fsync) de dam bao
khong mat du lieu khi crash.
"""

import os
from datetime import datetime

from config import LOG_DIR


class Logger:
    """Ghi log su kien ra console va file cho moi site."""

    def __init__(self, sid):
        self.sid = sid
        self.path = os.path.join(LOG_DIR, "site_%d.log" % sid)

    def info(self, event, detail, do_print=True):
        """Ghi mot dong log voi timestamp, flush xuong disk ngay."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = "[%s] Site %d | %s | %s" % (ts, self.sid, event, detail)
        if do_print:
            print(line, flush=True)
        with open(self.path, "a") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def warning(self, event, detail, do_print=True):
        """Ghi mot dong log warning."""
        self.info(f"WARNING_{event}", detail, do_print)

    def error(self, event, detail, do_print=True):
        """Ghi mot dong log error."""
        self.info(f"ERROR_{event}", detail, do_print)

    def clear(self):
        """Xoa file log."""
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass
