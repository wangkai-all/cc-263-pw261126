"""HTTP layer for SandboxLab.

Standard library only (``http.server``), so the whole thing runs with
``python3 -m sandboxlab``.  Routes are explicit; every error is mapped from
:class:`StoreError` to a JSON body with a stable code, because the UI needs to
distinguish "row erased" from "conflict pending" from "history pruned".
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .errors import RefreshConflict, StoreError
from .store import Store

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
API_PREFIX = "/api"


def _q(query: dict, key: str, default: str = "") -> str:
    values = query.get(key)
    return values[0] if values else default


def _q_int(query: dict, key: str) -> int | None:
    raw = _q(query, key)
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        raise StoreError(400, "invalid_query", f"参数 {key} 必须为整数。")


def _q_bool(query: dict, key: str, default: bool = False) -> bool:
    raw = _q(query, key)
    if raw == "":
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


class Api:
    def __init__(self, store: Store):
        self.store = store

    def handle(self, method: str, path: str, query: dict, body: dict | None):
        """Return (status, payload, headers).  Raises StoreError for the rest."""
        if path == "/api/state" and method == "GET":
            return 200, {
                "upstream": self.store.upstream_state(),
                "sandboxes": self.store.list_sandboxes(),
                "operations": self.store.list_operations(25),
            }, {}
        if path == "/api/operations" and method == "GET":
            return 200, {
                "operations": self.store.list_operations(
                    _q_int(query, "limit") or 30, _q(query, "sandbox_id") or None
                )
            }, {}
        if path == "/api/upstream" and method == "GET":
            return 200, self.store.upstream_state(), {}
        if path == "/api/upstream/simulate" and method == "POST":
            body = body or {}
            return 200, self.store.simulate_upstream(
                body.get("ops", 3), body.get("seed")
            ), {}
        if path == "/api/recover" and method == "POST":
            return 200, self.store.recover(), {}
        if path == "/api/erasure/preview" and method == "GET":
            row_id = _q(query, "row_id")
            if not row_id:
                raise StoreError(400, "missing_row_id", "缺少 row_id。")
            return 200, self.store.preview_erase(row_id), {}
        if path == "/api/upstream/erase" and method == "POST":
            body = body or {}
            return 200, self.store.erase_row(
                str(body.get("row_id", "")),
                str(body.get("reason", "")),
                acknowledge_hold=bool(body.get("acknowledge_hold")),
            ), {}
        if path == "/api/sandboxes" and method == "POST":
            return 201, self.store.create_sandbox((body or {}).get("label", "")), {}
        if path.startswith("/api/sandboxes/"):
            return self._sandbox(method, path, query, body or {})
        if path.startswith("/api/exports/"):
            return self._export(method, path)
        if path.startswith("/api/erasures/"):
            if method != "GET":
                raise StoreError(405, "method_not_allowed", "不支持的请求方法。")
            row_id = path.rsplit("/", 1)[-1]
            return 200, self.store.erasure_report(row_id), {}
        raise StoreError(404, "not_found", "接口不存在。")

    def _sandbox(self, method: str, path: str, query: dict, body: dict):
        parts = [p for p in path.split("/") if p]
        # api/sandboxes/<id>/...
        if len(parts) < 3:
            raise StoreError(404, "not_found", "接口不存在。")
        sandbox_id = parts[2]
        rest = parts[3:]
        if not rest and method == "GET":
            return 200, self.store.sandbox_detail(sandbox_id), {}
        if not rest and method == "DELETE":
            return 200, self.store.destroy_sandbox(
                sandbox_id, body.get("reason", "")
            ), {}
        if rest == ["rows"] and method == "GET":
            return 200, self.store.read_rows(
                sandbox_id,
                query=_q(query, "query"),
                region=_q(query, "region"),
                sort=_q(query, "sort", "row_id"),
                direction=_q(query, "direction", "asc"),
                include_deleted=_q_bool(query, "include_deleted"),
                as_of=_q_int(query, "as_of"),
                use_cache=not _q_bool(query, "nocache"),
            ), {}
        if len(rest) == 2 and rest[0] == "rows" and method == "PATCH":
            return 200, self.store.write_row(sandbox_id, rest[1], body), {}
        if len(rest) == 2 and rest[0] == "rows" and method == "DELETE":
            return 200, self.store.delete_row(sandbox_id, rest[1]), {}
        if len(rest) == 3 and rest[0] == "rows" and rest[2] == "restore":
            if method != "POST":
                raise StoreError(405, "method_not_allowed", "不支持的请求方法。")
            return 200, self.store.restore_row(sandbox_id, rest[1]), {}
        if rest == ["columns"] and method == "PATCH":
            return 200, self.store.update_column_meta(
                sandbox_id, body.get("columns", [])
            ), {}
        if rest == ["refresh", "preview"] and method == "GET":
            return 200, self.store.preview_refresh(sandbox_id), {}
        if rest == ["refresh"] and method == "POST":
            try:
                return 200, self.store.refresh(sandbox_id), {}
            except RefreshConflict as exc:
                return 409, {
                    "error": {
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.result,
                    }
                }, {}
        if rest == ["refresh", "apply"] and method == "POST":
            return 200, self.store.apply_refresh(sandbox_id), {}
        if rest == ["conflicts"] and method == "GET":
            return 200, self.store.conflicts(sandbox_id), {}
        if len(rest) == 2 and rest[0] == "conflicts" and method == "POST":
            return 200, self.store.resolve_conflict(
                sandbox_id, rest[1], body.get("resolution", "")
            ), {}
        if rest == ["exports"] and method == "POST":
            return 201, self.store.create_export(
                sandbox_id,
                query=body.get("query", ""),
                region=body.get("region", ""),
                include_deleted=bool(body.get("include_deleted")),
            ), {}
        if len(rest) == 1 and rest[0] == "history" and method == "GET":
            detail = self.store.sandbox_detail(sandbox_id)
            return 200, {
                "seq": detail["seq"],
                "horizon": detail["history_horizon"],
                "retention": detail["history_retention"],
                "bases": detail["base_history"],
                "operations": self.store.list_operations(50, sandbox_id),
            }, {}
        raise StoreError(404, "not_found", "接口不存在。")

    def _export(self, method: str, path: str):
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[3] == "download" and method == "GET":
            return 200, {"export_id": parts[2]}, {"__download__": parts[2]}
        raise StoreError(404, "not_found", "接口不存在。")


class Handler(BaseHTTPRequestHandler):
    server_version = "SandboxLab/0.1"
    protocol_version = "HTTP/1.1"

    @property
    def api(self) -> Api:
        return self.server.api  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # keep test output readable
        if os.environ.get("SANDBOXLAB_VERBOSE"):
            super().log_message(fmt, *args)

    # --------------------------------------------------------------- helpers

    def _send_json(self, status: int, payload, extra_headers: dict | None = None):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    def _send_error(self, exc: StoreError):
        payload = {"error": {"code": exc.code, "message": exc.message}}
        if exc.details is not None:
            payload["error"]["details"] = exc.details
        status = exc.status
        self._send_json(status, payload)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > 1_000_000:
            raise StoreError(413, "body_too_large", "请求体过大。")
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise StoreError(400, "invalid_json", "请求体不是合法 JSON。")
        if parsed is None:
            return {}
        if not isinstance(parsed, dict):
            raise StoreError(400, "invalid_json", "请求体必须是对象。")
        return parsed

    def _route(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/"):
            try:
                body = self._read_body() if method in {"POST", "PATCH", "PUT"} else {}
                status, payload, headers = self.api.handle(
                    method, path, parse_qs(parsed.query), body
                )
                download = headers.pop("__download__", None) if headers else None
                if download:
                    self._send_download(download)
                    return
                self._send_json(status, payload, headers)
            except StoreError as exc:
                self._send_error(exc)
            except Exception:  # pragma: no cover - last-resort guard
                traceback.print_exc()
                self._send_error(
                    StoreError(500, "internal_error", "服务端发生未预期错误。")
                )
            return
        self._serve_static(path)

    def _send_download(self, export_id: str):
        try:
            path, filename = self.api.store.export_file(export_id)
        except StoreError as exc:
            self._send_error(exc)
            return
        raw = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header(
            "Content-Disposition", f'attachment; filename="{filename}"'
        )
        self.end_headers()
        self.wfile.write(raw)

    def _serve_static(self, path: str):
        if path in {"", "/"}:
            path = "/index.html"
        target = (STATIC_DIR / path.lstrip("/")).resolve()
        if not str(target).startswith(str(STATIC_DIR.resolve())) or not target.is_file():
            payload = b"Not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        raw = target.read_bytes()
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        if target.suffix in {".html", ".css", ".js"}:
            ctype += "; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_PATCH(self):
        self._route("PATCH")

    def do_DELETE(self):
        self._route("DELETE")

    def do_PUT(self):
        self._route("PUT")

    def do_HEAD(self):
        self._route("GET")


def make_server(root: str, host: str = "127.0.0.1", port: int = 8000):
    store = Store(root)
    store.ensure_seeded()
    server = ThreadingHTTPServer((host, port), Handler)
    server.api = Api(store)  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server
