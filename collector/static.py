#!/usr/bin/env python3
"""Daily archive of Entur's national timetable (NeTEx) and stop register (NSR).

Entur keeps only ~48 h of past exports, so a day not archived is lost for good. Once a day
(dat535-static.timer) each file is downloaded unchanged, with a <file>.meta.json beside it:

    <landing>/netex/date=YYYY-MM-DD/rb_norway-aggregated-netex.zip
    <landing>/nsr/date=YYYY-MM-DD/Current_latest.zip

Environment: DAT535_LANDING_DIR, STATIC_DATASETS (netex,nsr), NETEX_URL/NSR_URL (tests),
ET_CLIENT_NAME. Exit code 1 if any dataset failed.
"""

import hashlib
import http.client
import json
import logging
import os
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hourly import iso, utcnow

log = logging.getLogger("static")

BASE = "https://storage.googleapis.com/marduk-production"
DATASETS = {
    "netex": f"{BASE}/outbound/netex/rb_norway-aggregated-netex.zip",  # ~280 MB (2026-10-26)
    "nsr": f"{BASE}/tiamat/Current_latest.zip",  # ~41 MB
}
TIMEOUT_S = 60  # per socket operation
CHUNK = 1 << 20

# Entur allows one download per file per 24 h. The timer runs daily; this blocks reruns and
# restarts. (Skipping a day costs nothing: every export holds the coming days too.)
MIN_INTERVAL = timedelta(hours=20)


def url_for(dataset):
    return os.environ.get(f"{dataset.upper()}_URL", DATASETS[dataset])


def last_download(landing, dataset):
    """When the newest archived file of a dataset was written, or None."""
    files = [
        p
        for p in (Path(landing) / dataset).glob("date=*/*")
        if p.is_file() and not p.name.endswith((".part", ".json", ".tmp"))
    ]
    if not files:
        return None
    return datetime.fromtimestamp(max(p.stat().st_mtime for p in files), timezone.utc)


def archive(landing, dataset, client_name, now=None):
    """Download today's file unless it is there or the last download is too recent.

    Returns "archived", "present" or "too-soon". On failure it raises and leaves no file.
    """
    now = now or utcnow()
    url = url_for(dataset)
    folder = Path(landing) / dataset / f"date={now:%Y-%m-%d}"
    final = folder / url.rsplit("/", 1)[-1]
    if final.exists():
        return "present"
    last = last_download(landing, dataset)
    if last and now - last < MIN_INTERVAL:
        return "too-soon"
    # Stream to <file>.part, check it, then rename: readers never see a partial file.
    folder.mkdir(parents=True, exist_ok=True)
    part = final.with_name(final.name + ".part")
    meta_tmp = final.with_name(final.name + ".meta.json.tmp")
    started = utcnow()
    digest, size = hashlib.sha256(), 0
    req = urllib.request.Request(url, headers={"ET-Client-Name": client_name})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp, open(part, "wb") as out:
            expected = resp.headers.get("Content-Length")
            while chunk := resp.read(CHUNK):
                out.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            out.flush()
            os.fsync(out.fileno())
            headers = resp.headers
        # Complete, and a readable zip?
        if expected is not None and size != int(expected):
            raise ValueError(f"got {size} of {expected} bytes")
        with zipfile.ZipFile(part) as z:
            bad = z.testzip()
            if bad is not None:
                raise ValueError(f"corrupt member {bad}")

        meta = {
            "dataset": dataset,
            "url": url,
            "last_modified": headers.get("Last-Modified"),
            "etag": headers.get("ETag"),
            "bytes": size,
            "sha256": digest.hexdigest(),
            "started_at": iso(started),
            "finished_at": iso(utcnow()),
        }
        meta_tmp.write_text(json.dumps(meta, indent=1) + "\n")
        os.rename(part, final)
        os.rename(meta_tmp, final.with_name(final.name + ".meta.json"))
    except BaseException:
        for f in (part, meta_tmp):
            try:
                f.unlink()
            except FileNotFoundError:
                pass
        raise
    log.info(
        "%s: archived %s (%d bytes, Last-Modified %s)",
        dataset, final, size, meta["last_modified"],
    )
    return "archived"


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")
    datasets = os.environ.get("STATIC_DATASETS", ",".join(DATASETS)).split(",")
    datasets = [d.strip() for d in datasets if d.strip()]
    for d in datasets:
        if d not in DATASETS:
            sys.exit(f"unknown dataset: {d} (known: {', '.join(DATASETS)})")
    # A failed dataset is retried by systemd, at most twice, an hour apart.
    failed = []
    for d in datasets:
        try:
            result = archive(landing, d, client_name)
            if result == "present":
                log.info("%s: today's file is already archived; not downloading again", d)
            elif result == "too-soon":
                log.info(
                    "%s: last download less than %s ago; skipping (Entur allows one a day)",
                    d, MIN_INTERVAL,
                )
        except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError,
                zipfile.BadZipFile) as e:
            log.error("%s: download failed: %s", d, e)
            failed.append(d)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
