#!/usr/bin/env python3
"""Serve the monitor's status directory, read-only. Point a tunnel here, never at landing.

GET/HEAD only (anything else gets 501), no directory listings, no dotfiles.

Environment:
    MONITOR_STATUS_DIR   directory to serve (default: ./status)
    STATUS_BIND          default: 127.0.0.1
    STATUS_PORT          default: 8751
"""

import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class StatusHandler(SimpleHTTPRequestHandler):
    server_version = "status"
    sys_version = ""

    def list_directory(self, path):
        self.send_error(404)
        return None

    def send_head(self):
        name = self.path.split("?", 1)[0].split("#", 1)[0]
        if any(seg.startswith(".") for seg in name.split("/") if seg):
            self.send_error(404)
            return None
        return super().send_head()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy",
                         "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
                         "form-action 'none'; frame-ancestors 'none'")
        super().end_headers()


def main():
    directory = Path(os.environ.get("MONITOR_STATUS_DIR", "status")).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    bind = os.environ.get("STATUS_BIND", "127.0.0.1")
    port = int(os.environ.get("STATUS_PORT", "8751"))
    server = ThreadingHTTPServer((bind, port), partial(StatusHandler, directory=str(directory)))
    print(f"serving {directory} on {bind}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
