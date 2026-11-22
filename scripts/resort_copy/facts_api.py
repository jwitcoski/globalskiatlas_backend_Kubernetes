"""Read-only JSON API over the resort facts table.

GET /resorts/facts?country=&state=&pass=Ikon&amenity=night_skiing&price_band=$$&tier=medium
GET /resorts/facts/stats  (same filters)
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .fact_store import DB_PATH, connect, fact_stats, query_facts


def _params(path: str) -> dict[str, str]:
    query = parse_qs(urlparse(path).query)
    return {key: values[-1] for key, values in query.items() if values}


def _query(db_path: Path, params: dict[str, str]) -> list[dict[str, Any]]:
    conn = connect(db_path)
    try:
        return query_facts(
            conn,
            country=params.get("country") or None,
            state=params.get("state") or None,
            pass_name=params.get("pass") or None,
            amenity=params.get("amenity") or None,
            price_band=params.get("price_band") or None,
            tier=params.get("tier") or None,
        )
    finally:
        conn.close()


def make_handler(db_path: Path) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            route = parsed.path.rstrip("/") or "/"
            params = _params(self.path)
            try:
                rows = _query(db_path, params)
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
                return
            if route == "/resorts/facts":
                self._send(200, {"resorts": rows, "n": len(rows)})
                return
            if route == "/resorts/facts/stats":
                self._send(200, fact_stats(rows))
                return
            self._send(404, {"error": "not found"})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    return Handler


def serve(host: str = "127.0.0.1", port: int = 8091, db_path: Path = DB_PATH) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(db_path))
    print(f"resort facts http://{host}:{port}/resorts/facts", flush=True)
    server.serve_forever()
