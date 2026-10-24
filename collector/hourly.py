"""Hourly JSON Lines files in the landing zone, shared by every collector.

    <landing>/<name>/date=YYYY-MM-DD/hour=HH.jsonl.part  (gzipped to .jsonl.gz when the hour ends)

<name> is the dataset: siri-et, siri-sx (receiver.py), vehicles (vehicles.py), datex-<feed>
(datex.py). Dates and hours are UTC. Closed files are never touched again. Standard library only.
"""

import gzip
import logging
import os
import queue
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("hourly")

BODY_CHUNK = 1 << 20                # spooled payloads are copied this much at a time


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class HourlyWriter:
    """Appends lines to <root>/<name>/date=.../hour=HH.jsonl.part.

    When the hour ends the .part is closed and handed to a background thread, which
    gzips it to hour=HH.jsonl.gz (via .gz.tmp, then an atomic rename) and removes the
    .part. Compression never blocks incoming lines.
    """

    def __init__(self, root, name):
        self.dir = Path(root) / name
        self.lock = threading.Lock()
        self.key = None
        self.fh = None
        self.pending = queue.Queue()
        threading.Thread(target=self._compressor, daemon=True).start()
        now = utcnow()
        self.finalize_stale(now)
        self.adopt_current(now)

    def _part(self, key):
        date, hour = key
        return self.dir / f"date={date}" / f"hour={hour}.jsonl.part"

    @staticmethod
    def _key(now):
        return now.strftime("%Y-%m-%d"), now.strftime("%H")

    def _close(self):
        if self.fh is None:
            return
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.fh = None
        self.pending.put(self._part(self.key))
        self.key = None

    def _compressor(self):
        while True:
            part = self.pending.get()
            try:
                finalize(part)
            except Exception:
                log.exception("finalizing %s failed; .part left in place", part)
            finally:
                self.pending.task_done()

    def finalize_stale(self, now):
        """Queue .part files from earlier hours, e.g. left behind by a restart or crash."""
        current = self._part(self._key(now))
        for part in sorted(self.dir.glob("date=*/hour=*.jsonl.part")):
            if part != current:
                self.pending.put(part)

    def adopt_current(self, now):
        """Take over this hour's .part left by a restart, so rotate() closes it when the hour
        ends even if no push arrives before then."""
        with self.lock:
            if self._part(self._key(now)).exists():
                self._file(now)

    def rotate(self, now):
        with self.lock:
            if self.key is not None and self.key != self._key(now):
                self._close()

    def _file(self, now):
        key = self._key(now)
        if self.key != key:
            self._close()
            part = self._part(key)
            part.parent.mkdir(parents=True, exist_ok=True)
            self.fh = open(part, "ab")
            self.key = key
        return self.fh

    def append(self, line, now):
        with self.lock:
            fh = self._file(now)
            fh.write(line.encode("utf-8"))
            fh.flush()

    def append_spooled(self, head, spool, tail, now):
        """Append head + the bytes of `spool` + tail as one line, streamed in chunks."""
        with self.lock:
            fh = self._file(now)
            fh.write(head.encode("utf-8"))
            shutil.copyfileobj(spool, fh, BODY_CHUNK)
            fh.write(tail.encode("utf-8"))
            fh.flush()

    def close(self):
        """Shutdown: flush and close, but keep .part (the hour may not be over)."""
        with self.lock:
            if self.fh is not None:
                self.fh.flush()
                os.fsync(self.fh.fileno())
                self.fh.close()
                self.fh = None
                self.key = None


def same_content(part, gz):
    with open(part, "rb") as a, gzip.open(gz, "rb") as b:
        while True:
            x, y = a.read(1 << 20), b.read(1 << 20)
            if x != y:
                return False
            if not x:
                return True


def finalize(part):
    """hour=HH.jsonl.part -> hour=HH.jsonl.gz. Never overwrites a closed file."""
    base = part.with_name(part.name[: -len(".part")])
    final = base.with_name(base.name + ".gz")
    if base.exists():
        # Closed uncompressed by an older version; leave the .part for a human to look at.
        log.error("not finalizing %s: %s already exists", part, base.name)
        return
    if final.exists():
        # Crash between rename and unlink: the .part is redundant only if identical.
        if same_content(part, final):
            part.unlink()
            log.info("removed leftover %s (already in %s)", part, final.name)
        else:
            log.error("not finalizing %s: %s already exists", part, final.name)
        return
    tmp = final.with_name(final.name + ".tmp")
    with open(part, "rb") as src, open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            shutil.copyfileobj(src, gz, 1 << 20)
        raw.flush()
        os.fsync(raw.fileno())
    os.rename(tmp, final)
    size = part.stat().st_size
    part.unlink()
    log.info("closed %s (%d -> %d bytes)", final, size, final.stat().st_size)
