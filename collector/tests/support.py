"""What the collector tests share: the collector modules on sys.path and local HTTP servers.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def serve(test, handler):
    """A local HTTP server on a free port for one test, shut down and closed when the test ends."""
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    test.addCleanup(srv.server_close)
    test.addCleanup(srv.shutdown)
    return srv
