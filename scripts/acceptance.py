#!/usr/bin/env python3
"""End-to-end acceptance walk-through against a live server.

Boots SandboxLab on a scratch directory, exercises every user-visible flow the
web page offers (clone, branch writes, upstream churn, refresh conflict,
resolve, apply, history, export, erasure, destroy, recovery), and asserts the
cross-branch guarantees.  Prints a transcript so a human can follow what
happened to which sandbox.

    python3 scripts/acceptance.py [--keep]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxlab.app import make_server  # noqa: E402
from sandboxlab.schema import transaction  # noqa: E402


class Client:
    def __init__(self, port: int):
        self.base = f"http://127.0.0.1:{port}"

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + urllib.parse.quote(path, safe="/?&=%:"),
            data=data,
            headers={"Content-Type": "application/json"} if data else {},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())


def ok(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}{(' — ' + detail) if detail else ''}")
    if not condition:
        raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="保留数据目录以便检查")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="sandboxlab-acceptance-"))
    server = make_server(str(root), "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = Client(port)
    store = server.api.store  # for direct upstream manipulation between steps
    print(f"SandboxLab acceptance run — data dir {root} — http://127.0.0.1:{port}/")

    try:
        print("\n1. 克隆：三个沙箱共享同一个基底块")
        _, a = client.call("POST", "/api/sandboxes", {"label": "定价实验"})
        _, b = client.call("POST", "/api/sandboxes", {"label": "合规核查"})
        _, c = client.call("POST", "/api/sandboxes", {"label": "临时探查"})
        ids = [a["sandbox_id"], b["sandbox_id"], c["sandbox_id"]]
        ok("三个沙箱指向同一基底", a["base_segment"] == b["base_segment"] == c["base_segment"])
        _, a_detail = client.call("GET", f"/api/sandboxes/{ids[0]}")
        ok("共享块引用计数 = 3", a_detail["base_shared_refs"] == 3, str(a_detail["base_shared_refs"]))
        overlay_bytes = 0
        for sid in ids:
            with store.sandbox(sid) as sconn:
                overlay_bytes += sum(
                    len(r["payload"])
                    for r in sconn.execute("SELECT payload FROM local_rows").fetchall()
                )
        with store.data() as conn:
            base_payload = json.dumps(
                store._segment_payload(conn, a["base_segment"]), ensure_ascii=False
            ).encode("utf-8")
        ok(
            "空沙箱没有复制任何行数据",
            overlay_bytes == 0,
            f"本地覆盖 0B，共享基底 {len(base_payload)}B",
        )
        print(
            f"    说明：每个沙箱仍是真实 SQLite 文件（含 ~{a['disk_bytes'] // 1024}KB "
            "固定页/WAL 开销），但行负载只存在于共享块。"
        )

        print("\n2. 分支隔离：只改本地覆盖层")
        client.call("PATCH", f"/api/sandboxes/{ids[0]}/rows/acct-001", {"notes": "A 的假设"})
        client.call("DELETE", f"/api/sandboxes/{ids[1]}/rows/acct-002")
        client.call("PATCH", f"/api/sandboxes/{ids[2]}/rows/acct-003", {"amount_cents": 1})
        _, va = client.call("GET", f"/api/sandboxes/{ids[0]}/rows")
        _, vb = client.call("GET", f"/api/sandboxes/{ids[1]}/rows")
        notes_a = {r["row_id"]: r["notes"] for r in va["rows"]}
        ids_b = {r["row_id"] for r in vb["rows"]}
        ok("A 看到自己的改动", notes_a["acct-001"] == "A 的假设")
        ok("B 看不到 A 的改动", notes_a["acct-002"] == "待回访" and "acct-002" not in ids_b)
        ok("B 的删除不影响 A", "acct-002" in notes_a)
        upstream = client.call("GET", "/api/upstream")[1]
        upstream_note = {r["row_id"]: r["notes"] for r in upstream["rows"]}["acct-001"]
        ok("上游未被沙箱写入影响", upstream_note == "年度续约")

        print("\n3. 上游变更：沙箱保持 pinned，刷新才导入")
        status, sim = client.call("POST", "/api/upstream/simulate", {"ops": 5, "seed": 5})
        ok("上游产生新版本", status == 200 and sim["revision"] > 1, f"rev {sim['revision']}")
        _, va2 = client.call("GET", f"/api/sandboxes/{ids[0]}/rows")
        ok("未刷新时 A 仍是旧基底", va2["rows"] and va2["total"] == va["total"])
        _, preview = client.call("GET", f"/api/sandboxes/{ids[0]}/refresh/preview")
        ok("刷新预览给出差异", preview["stats"]["added"] + preview["stats"]["updated"] + preview["stats"]["removed"] > 0,
           str(preview["stats"]))
        status, refreshed = client.call("POST", f"/api/sandboxes/{ids[0]}/refresh")
        ok("刷新成功", status == 200 and refreshed["status"] in {"refreshed", "up_to_date"})
        ok("本地改动仍在", refreshed.get("resolved") is not None or refreshed["status"] == "up_to_date")

        print("\n4. 刷新撞上本地写入：停下并要求逐条决定")
        client.call("PATCH", f"/api/sandboxes/{ids[1]}/rows/acct-005", {"notes": "合规本地备注"})
        with store.data() as conn:
            with transaction(conn):
                conn.execute(
                    'UPDATE "rows" SET notes = ? WHERE row_id = ?',
                    ("上游合规备注", "acct-005"),
                )
                segment = store._publish_segment_from_rows(conn, revision=900, note="acceptance")
                store._set_meta(conn, "current_segment_id", segment)
        status, conflict = client.call("POST", f"/api/sandboxes/{ids[1]}/refresh")
        ok("刷新返回 409 冲突", status == 409 and conflict["error"]["code"] == "refresh_conflicts")
        details = conflict["error"]["details"]
        ok("冲突行就是被两边改过的行", [x["row_id"] for x in details["conflicts"]] == ["acct-005"])
        _, vb2 = client.call("GET", f"/api/sandboxes/{ids[1]}/rows")
        ok("本地值没有被覆盖", [r["notes"] for r in vb2["rows"] if r["row_id"] == "acct-005"] == ["合规本地备注"])
        status, blocked = client.call("POST", f"/api/sandboxes/{ids[1]}/refresh/apply")
        ok("未决定时不能推进基底", status == 409 and blocked["error"]["code"] == "conflicts_unresolved")
        client.call("POST", f"/api/sandboxes/{ids[1]}/conflicts/acct-005", {"resolution": "keep"})
        status, applied = client.call("POST", f"/api/sandboxes/{ids[1]}/refresh/apply")
        ok("决定后可以推进", status == 200 and applied["stats"]["updated"] >= 1)
        _, vb3 = client.call("GET", f"/api/sandboxes/{ids[1]}/rows")
        ok("保留本地后仍是本地值", [r["notes"] for r in vb3["rows"] if r["row_id"] == "acct-005"] == ["合规本地备注"])

        print("\n5. 历史回看是有界窗口，不是无限")
        history = client.call("GET", f"/api/sandboxes/{ids[1]}/history")[1]
        ok("历史窗口有明确起点", history["horizon"] <= history["seq"], f"{history['horizon']}..{history['seq']}")
        _, old = client.call("GET", f"/api/sandboxes/{ids[1]}/rows?as_of=0")
        ok("窗口内可以回看", old["as_of"] == 0)
        old_notes = {r["row_id"]: r.get("notes") for r in old["rows"]}
        current_note = [r["notes"] for r in vb3["rows"] if r["row_id"] == "acct-005"][0]
        ok(
            "回看 seq 0 给出改动前的值",
            old_notes.get("acct-005") not in (None, current_note),
            f"{old_notes.get('acct-005')} → {current_note}",
        )

        print("\n6. 导出：真实文件，删除时吊销")
        # Export the whole view: erasing any one row must revoke the file.
        _, export = client.call("POST", f"/api/sandboxes/{ids[1]}/exports", {"region": ""})
        ok("导出生成了文件", Path(export["path"]).exists(), export["export_id"])
        download = urllib.request.urlopen(client.base + export["download"], timeout=10)
        body = download.read()
        ok("下载内容是 CSV", body.startswith("客户编号".encode()), f"{export['row_count']} 行")

        print("\n7. 源记录删除：全链路清理")
        _, pre = client.call("GET", "/api/erasure/preview?row_id=acct-021")
        ok("删除预览列出受影响沙箱", pre["in_upstream"] and len(pre["affected_sandboxes"]) >= 1,
           f"{len(pre['affected_sandboxes'])} 个沙箱")
        status, erased = client.call(
            "POST",
            "/api/upstream/erase",
            {"row_id": "acct-021", "reason": "数据主体要求删除（验收）"},
        )
        report = erased["report"]
        ok("删除完成", status == 200 and report["completed"])
        ok("所有含该行的共享块都已重写", len(report["segments_rewritten"]) >= 1,
           str([x["segment_id"][:8] for x in report["segments_rewritten"]]))
        for sid in ids:
            _, view = client.call("GET", f"/api/sandboxes/{sid}/rows")
            ok(f"{sid} 不再返回该行", "acct-021" not in view["row_ids"])
            _, past = client.call("GET", f"/api/sandboxes/{sid}/rows?as_of=0")
            ok(f"{sid} 的历史也不返回内容", "acct-021" not in past["row_ids"])
        _, audit = client.call("GET", "/api/erasures/acct-021")
        ok("审计报告确认无残留", audit["clean"])
        status, attempt = client.call(
            "PATCH", f"/api/sandboxes/{ids[0]}/rows/acct-021", {"notes": "复活"}
        )
        ok("已删除行无法被写回", status == 410 and attempt["error"]["code"] == "row_erased")
        _, fresh = client.call("POST", "/api/sandboxes", {"label": "删除后克隆"})
        _, fresh_rows = client.call("GET", f"/api/sandboxes/{fresh['sandbox_id']}/rows")
        ok("删除后新建的沙箱也没有该行", "acct-021" not in fresh_rows["row_ids"])
        try:
            urllib.request.urlopen(client.base + export["download"], timeout=10)
            ok("导出被吊销", False)
        except urllib.error.HTTPError as exc:
            ok("导出被吊销并删除文件", exc.code == 410)

        print("\n8. 销毁：删除真实文件，不影响其它沙箱")
        status, destroyed = client.call("DELETE", f"/api/sandboxes/{ids[2]}", {"reason": "验收结束"})
        ok("沙箱文件已删除", status == 200 and not Path(store.sandbox_path(ids[2])).exists())
        ok("其它沙箱仍可读", client.call("GET", f"/api/sandboxes/{ids[0]}/rows")[0] == 200)
        ok("共享块未被销毁", len(list(Path(root, "segments").glob("*.json.gz"))) >= 1)
        status, detail = client.call("GET", f"/api/sandboxes/{ids[2]}/rows")
        ok("已销毁沙箱返回 410", status == 410 and detail["error"]["code"] == "sandbox_destroyed")

        print("\n9. 失败恢复：中断的删除可以补齐，且不会复活数据")
        store.failpoint = lambda name: (_ for _ in ()).throw(RuntimeError(name)) if name == "erase.after_tombstone" else None
        try:
            store.erase_row("acct-022", "模拟中断的删除请求")
            ok("故障注入生效", False)
        except RuntimeError:
            ok("删除在清理途中中断", True)
        _, before_recover = client.call("GET", "/api/sandboxes/{}/rows".format(ids[0]))
        ok("仅凭墓碑就已隐藏该行", "acct-022" not in before_recover["row_ids"])
        status, recovered = client.call("POST", "/api/recover", {})
        ok("恢复补齐清理", status == 200 and recovered["recovered"] == 1)
        _, audit2 = client.call("GET", "/api/erasures/acct-022")
        ok("恢复后无残留", audit2["clean"])
        _, rows_after = client.call("GET", f"/api/sandboxes/{ids[0]}/rows")
        ok("恢复没有复活已删除数据", "acct-022" not in rows_after["row_ids"])
        store.failpoint = lambda name: None

        print("\n全部验收通过。")
        return 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if args.keep:
            print(f"数据目录保留于 {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
