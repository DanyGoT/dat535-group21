#!/usr/bin/env python3
"""Front page server. Point a tunnel here, never at landing.

Serves the monitor's status directory (the main page) read-only and proxies /grafana/
to the local Grafana, which is read-only itself (anonymous viewers, no login).

Status directory: GET/HEAD only (anything else gets 501), no directory listings, no dotfiles.
/grafana/: GET, HEAD and POST are forwarded (Grafana's panels query with POST); no websockets.

Environment:
    MONITOR_STATUS_DIR   directory to serve (default: ./status)
    STATUS_BIND          default: 127.0.0.1
    STATUS_PORT          default: 8751
    GRAFANA_URL          upstream for /grafana/ (default: http://127.0.0.1:8753)
"""

import http.client
import os
import sys
import urllib.parse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PROXY_PREFIX = "/grafana"
MAX_PROXY_BODY = 4 * 1024 * 1024    # dashboard queries are small
PROXY_TIMEOUT_S = 60
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
              "trailer", "trailers", "transfer-encoding", "upgrade", "proxy-connection"}

PAGE_CSP = ("default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "frame-src 'self'; form-action 'none'; frame-ancestors 'none'")


class StatusHandler(SimpleHTTPRequestHandler):
    server_version = "status"
    sys_version = ""
    upstream = urllib.parse.urlsplit("http://127.0.0.1:8753")
    proxying = False

    def proxied(self):
        name = self.path.split("?", 1)[0]
        return name == PROXY_PREFIX or name.startswith(PROXY_PREFIX + "/")

    def do_GET(self):
        if self.proxied():
            return self.proxy()
        super().do_GET()

    def do_HEAD(self):
        if self.proxied():
            return self.proxy()
        super().do_HEAD()

    def do_POST(self):
        if self.proxied():
            return self.proxy()
        self.send_error(501)

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
        # Grafana sends its own headers; ours (CSP, no-store) would break it.
        if not self.proxying:
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", PAGE_CSP)
        super().end_headers()

    def proxy(self):
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            return self.send_error(411)
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return self.send_error(400)
        if length < 0 or length > MAX_PROXY_BODY:
            return self.send_error(413)
        body = self.rfile.read(length) if length else None

        # Host stays as the browser sent it: Grafana compares it with Origin on POST.
        forwarded = {"x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "expect"}
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP | forwarded}
        chain = self.headers.get("X-Forwarded-For")
        headers["X-Forwarded-For"] = f"{chain}, {self.client_address[0]}" if chain else self.client_address[0]
        headers["X-Forwarded-Host"] = self.headers.get("X-Forwarded-Host") or self.headers.get("Host", "")
        headers["X-Forwarded-Proto"] = self.headers.get("X-Forwarded-Proto") or "http"

        conn = http.client.HTTPConnection(self.upstream.hostname, self.upstream.port or 80,
                                          timeout=PROXY_TIMEOUT_S)
        try:
            conn.request(self.command, self.path, body=body, headers=headers)
            resp = conn.getresponse()
        except OSError as e:
            conn.close()
            print(f"grafana unreachable: {e}", file=sys.stderr, flush=True)
            return self.send_error(502, "grafana unreachable")

        try:
            self.proxying = True
            self.send_response(resp.status, resp.reason)
            for k, v in resp.getheaders():
                if k.lower() not in HOP_BY_HOP:
                    self.send_header(k, v)
            # HTTP/1.0 reply: the body ends when we close the connection.
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                while chunk := resp.read(64 * 1024):
                    self.wfile.write(chunk)
        except OSError:
            pass  # browser went away mid-response
        finally:
            self.close_connection = True
            conn.close()


def main():
    directory = Path(os.environ.get("MONITOR_STATUS_DIR", "status")).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    bind = os.environ.get("STATUS_BIND", "127.0.0.1")
    port = int(os.environ.get("STATUS_PORT", "8751"))
    StatusHandler.upstream = urllib.parse.urlsplit(os.environ.get("GRAFANA_URL", "http://127.0.0.1:8753"))
    server = ThreadingHTTPServer((bind, port), partial(StatusHandler, directory=str(directory)))
    server.daemon_threads = True
    print(f"serving {directory} on {bind}:{port}, {PROXY_PREFIX}/ -> "
          f"{StatusHandler.upstream.geturl()}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
