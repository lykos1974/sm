from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .collectors import collect_all_prices
from .store import PriceStore

ROOT = Path(__file__).resolve().parent
store = PriceStore()
store.replace(collect_all_prices())


class AppHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, file_path: Path, content_type: str) -> None:
        if not file_path.exists():
            self.send_error(404)
            return
        body = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            html = (ROOT / "templates/index.html").read_text(encoding="utf-8")
            self._send_html(html)
            return

        if path == "/static/style.css":
            self._serve_static(ROOT / "static/style.css", "text/css; charset=utf-8")
            return

        if path == "/static/app.js":
            self._serve_static(ROOT / "static/app.js", "application/javascript; charset=utf-8")
            return

        if path == "/api/categories":
            self._send_json({"categories": store.grouped_categories()})
            return

        if path == "/api/search":
            query = parse_qs(parsed.query).get("query", [""])[0]
            self._send_json({"query": query, "results": store.search(query)})
            return

        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        if parsed.path == "/api/refresh":
            store.replace(collect_all_prices())
            self._send_json({"ok": True, "offers": len(store.offers)})
            return

        if parsed.path == "/api/best-deals":
            payload = json.loads(body.decode("utf-8"))
            items = payload.get("items", [])
            deals = [store.best_deal_for_item(item) for item in items]
            deals = [deal for deal in deals if deal]
            total = round(sum(deal["best_offer"]["price"] for deal in deals), 2)
            self._send_json({"deals": deals, "total": total})
            return

        self.send_error(404)


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = HTTPServer((host, port), AppHandler)
    print(f"Server running on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
