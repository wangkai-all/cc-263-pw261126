"""HTTP-level tests: route shapes, error codes, downloads, static traversal.

These boot a real ThreadingHTTPServer on an ephemeral port and speak HTTP.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from sandboxlab.app import make_server


class ServerCase(unittest.TestCase):
    """One fresh server (and data dir) per test: erasure tests mutate shared
    global state, so isolation matters as much here as in the product."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sandboxlab-http-"))
        self.server = make_server(str(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.root, ignore_errors=True)

    def call(self, method, path, body=None):
        path = urllib.parse.quote(path, safe="/?&=%:")
        url = f"http://127.0.0.1:{self.port}{path}"
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read()
                ctype = response.headers.get("content-type", "")
                headers = {k.lower(): v for k, v in response.headers.items()}
                if "application/json" in ctype:
                    return response.status, json.loads(raw), headers
                return response.status, raw, headers
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = raw
            return exc.code, payload, {k.lower(): v for k, v in exc.headers.items()}


class Routes(ServerCase):
    def test_state_and_root(self):
        status, payload, _ = self.call("GET", "/api/state")
        self.assertEqual(status, 200)
        self.assertEqual(payload["upstream"]["row_count"], 48)
        self.assertIn("sandboxes", payload)
        status, raw, headers = self.call("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", headers["content-type"])
        self.assertIn(b"SandboxLab", raw)

    def test_blocked_traversal_and_unknown_api(self):
        status, payload, _ = self.call("GET", "/api/does-not-exist")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/../sandboxlab/store.py"
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                self.fail(f"traversal succeeded: {response.status}")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 404)

    def test_full_lifecycle_over_http(self):
        status, sandbox, _ = self.call("POST", "/api/sandboxes", {"label": "HTTP A"})
        self.assertEqual(status, 201)
        sid = sandbox["sandbox_id"]
        self.assertEqual(sandbox["local_writes"], 0)

        status, view, _ = self.call("GET", f"/api/sandboxes/{sid}/rows?region=华东")
        self.assertEqual(status, 200)
        self.assertTrue(view["rows"])
        self.assertTrue(all(r["region"] == "华东" for r in view["rows"]))

        status, written, _ = self.call(
            "PATCH", f"/api/sandboxes/{sid}/rows/acct-001", {"notes": "HTTP 本地"}
        )
        self.assertEqual(status, 200)
        self.assertGreater(written["seq"], 0)

        status, cache_view, _ = self.call("GET", f"/api/sandboxes/{sid}/rows")
        target = [r for r in cache_view["rows"] if r["row_id"] == "acct-001"][0]
        self.assertEqual(target["notes"], "HTTP 本地")

        status, _, _ = self.call(
            "POST", "/api/upstream/simulate", {"ops": 3, "seed": 42}
        )
        self.assertEqual(status, 200)

        status, preview, _ = self.call(
            "GET", f"/api/sandboxes/{sid}/refresh/preview"
        )
        self.assertEqual(status, 200)
        self.assertIn("conflicts", preview)

        status, exported, _ = self.call(
            "POST", f"/api/sandboxes/{sid}/exports", {"query": "云杉"}
        )
        self.assertEqual(status, 201)
        status, raw, headers = self.call("GET", exported["download"])
        self.assertEqual(status, 200)
        self.assertIn("text/csv", headers["content-type"])
        self.assertTrue(raw.startswith("客户编号".encode()))

        status, _, _ = self.call("DELETE", f"/api/sandboxes/{sid}", {"reason": "用例结束"})
        self.assertEqual(status, 200)
        status, payload, _ = self.call("GET", f"/api/sandboxes/{sid}/rows")
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "sandbox_destroyed")
        status, payload, _ = self.call("GET", exported["download"])
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "export_revoked")

    def test_conflict_response_is_409_with_actionable_body(self):
        _, sandbox, _ = self.call("POST", "/api/sandboxes", {"label": "HTTP B"})
        sid = sandbox["sandbox_id"]
        self.call("PATCH", f"/api/sandboxes/{sid}/rows/acct-002", {"notes": "本地"})
        with self.server.api.store.data() as conn:
            from sandboxlab.schema import transaction

            with transaction(conn):
                conn.execute(
                    'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-002")
                )
                segment = self.server.api.store._publish_segment_from_rows(
                    conn, revision=500, note="http test"
                )
                self.server.api.store._set_meta(
                    conn, "current_segment_id", segment
                )
        status, payload, _ = self.call("POST", f"/api/sandboxes/{sid}/refresh")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "refresh_conflicts")
        self.assertEqual(
            [c["row_id"] for c in payload["error"]["details"]["conflicts"]],
            ["acct-002"],
        )
        status, detail, _ = self.call("GET", f"/api/sandboxes/{sid}")
        self.assertTrue(detail["pending_refresh"])
        status, applied, _ = self.call(
            "POST", f"/api/sandboxes/{sid}/refresh/apply"
        )
        self.assertEqual(status, 409)
        self.assertEqual(applied["error"]["code"], "conflicts_unresolved")
        self.call(
            "POST",
            f"/api/sandboxes/{sid}/conflicts/acct-002",
            {"resolution": "keep"},
        )
        status, applied, _ = self.call(
            "POST", f"/api/sandboxes/{sid}/refresh/apply"
        )
        self.assertEqual(status, 200)
        self.assertEqual(applied["stats"]["updated"], 1)

    def test_erasure_endpoint_and_report(self):
        _, sandbox, _ = self.call("POST", "/api/sandboxes", {"label": "HTTP C"})
        sid = sandbox["sandbox_id"]
        self.call("GET", f"/api/sandboxes/{sid}/rows")
        status, preview, _ = self.call(
            "GET", "/api/erasure/preview?row_id=acct-024"
        )
        self.assertEqual(status, 200)
        self.assertTrue(any(s["sandbox_id"] == sid for s in preview["affected_sandboxes"]))
        status, payload, _ = self.call(
            "POST",
            "/api/upstream/erase",
            {"row_id": "acct-024", "reason": "数据主体删除请求 HTTP"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(payload["report"]["completed"])
        status, report, _ = self.call("GET", "/api/erasures/acct-024")
        self.assertEqual(status, 200)
        self.assertTrue(report["clean"])
        status, view, _ = self.call("GET", f"/api/sandboxes/{sid}/rows")
        self.assertNotIn("acct-024", view["row_ids"])

    def test_bad_payloads(self):
        status, payload, _ = self.call("POST", "/api/sandboxes", {"label": ""})
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_label")
        status, payload, _ = self.call(
            "POST", "/api/upstream/erase", {"row_id": "acct-001", "reason": "x"}
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_reason")
        status, payload, _ = self.call("GET", "/api/sandboxes/nope/rows")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "sandbox_not_found")
        # Malformed JSON is a 400, not a stack trace.
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/sandboxes",
            data=b"{not json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=10)
            self.fail("expected 400")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 400)
            self.assertEqual(json.loads(exc.read())["error"]["code"], "invalid_json")


class StaticAssets(ServerCase):
    def test_assets_are_served_without_cdn(self):
        for path, needle in [
            ("/style.css", b"--panel"),
            ("/app.js", b"SandboxLab"),
        ]:
            status, raw, headers = self.call("GET", path)
            self.assertEqual(status, 200, path)
            self.assertIn(needle, raw)
        status, raw, _ = self.call("GET", "/index.html")
        self.assertNotIn(b"http://", raw.replace(b"http://www.w3.org", b""))
        self.assertNotIn(b"cdn.", raw)


if __name__ == "__main__":
    unittest.main()
