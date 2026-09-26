"""Concatenates several JMX-exporter /metrics endpoints into one.

LiveKafkaGateway scrapes a single jmx_exporter_base_url and filters by a
broker="N" label inside the response. Standard jmx_exporter (httpserver mode)
only ever connects to one JVM, so there's one exporter per broker; this
merges their output so the app's single-URL contract stays true without
touching live_gateway.py.

Upstreams are fetched in parallel so a scrape costs the slowest exporter,
not the sum of them, keeping it under LiveKafkaGateway's 10s httpx timeout.
If any upstream fails the whole scrape returns 502 with the reason, rather
than a partial body that would silently look like a broker with no metrics.
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

UPSTREAMS = [u.strip() for u in os.environ["UPSTREAM_METRICS_URLS"].split(",") if u.strip()]
if not UPSTREAMS:
    # Fail at startup, not per request: with no upstreams every scrape would
    # otherwise die inside the handler and drop the connection unanswered.
    sys.exit("UPSTREAM_METRICS_URLS contains no URLs")
UPSTREAM_TIMEOUT_SECONDS = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "8"))


def _fetch(url: str) -> str:
    with urlopen(url, timeout=UPSTREAM_TIMEOUT_SECONDS) as resp:
        body = resp.read().decode()
    return body if body.endswith("\n") else body + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        with ThreadPoolExecutor(max_workers=len(UPSTREAMS)) as pool:
            futures = [(url, pool.submit(_fetch, url)) for url in UPSTREAMS]
        chunks, errors = [], []
        for url, future in futures:
            try:
                chunks.append(future.result())
            # Any upstream failure (URLError, timeout, http.client's
            # IncompleteRead, which isn't an OSError) must become a 502.
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{url}: {exc!r}")
        if errors:
            self._send(502, "upstream scrape failed:\n" + "\n".join(errors) + "\n")
            print("\n".join(errors), file=sys.stderr, flush=True)
            return
        self._send(200, "".join(chunks), "text/plain; version=0.0.4")

    def _send(self, status: int, text: str, content_type: str = "text/plain") -> None:
        encoded = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    # Silences per-request access logs only. Upstream failures are printed
    # explicitly in do_GET, since BaseHTTPRequestHandler's own error path
    # (e.g. a TimeoutError it catches) also goes through here and was
    # previously how a timed-out scrape vanished without a trace.
    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 5559), Handler).serve_forever()
