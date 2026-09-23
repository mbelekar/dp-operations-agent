"""Concatenates several JMX-exporter /metrics endpoints into one.

LiveKafkaGateway scrapes a single jmx_exporter_base_url and filters by a
broker="N" label inside the response. Standard jmx_exporter (httpserver mode)
only ever connects to one JVM, so there's one exporter per broker; this
merges their output so the app's single-URL contract stays true without
touching live_gateway.py.
"""

import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import urlopen

UPSTREAMS = [u.strip() for u in os.environ["UPSTREAM_METRICS_URLS"].split(",") if u.strip()]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        chunks = []
        for url in UPSTREAMS:
            with urlopen(url, timeout=5) as resp:
                body = resp.read().decode()
                if not body.endswith("\n"):
                    body += "\n"
                chunks.append(body)
        encoded = "".join(chunks).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 5559), Handler).serve_forever()
