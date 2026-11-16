"""Functional acceptance tests for SandboxLab.

Plain ``unittest`` (no third-party deps): ``python3 -m unittest discover -s tests``.
Every test runs against a real on-disk SQLite + segment layout.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

from sandboxlab.errors import RefreshConflict, StoreError
from sandboxlab.schema import transaction
from sandboxlab.store import Store


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sandboxlab-test-"))
        self.store = Store(self.root)
        self.store.ensure_seeded()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    # ---------------------------------------------------------------- helpers

    def upstream_note(self, row_id: str) -> str:
        with self.store.data() as conn:
            row = conn.execute(
                'SELECT notes FROM "rows" WHERE row_id = ?', (row_id,)
            ).fetchone()
        return row["notes"] if row else None

    def upstream_publish(self, mutate) -> dict:
        """Apply arbitrary SQL through a transaction, then publish the base."""
        with self.store.data() as conn:
            with transaction(conn):
                mutate(conn)
                segment = self.store._publish_segment_from_rows(
                    conn, revision=999, note="test publish"
                )
                self.store._set_meta(conn, "current_segment_id", segment)
                self.store._set_meta(
                    conn,
                    "source_revision",
                    str(int(self.store._meta(conn, "source_revision", "0")) + 1),
                )
        return {"segment": segment}

    def rows(self, sandbox_id: str, **kwargs) -> dict[str, dict]:
        view = self.store.read_rows(sandbox_id, **kwargs)
        return {row["row_id"]: row for row in view["rows"]}

    def _payload_in_sandbox_files(self, sandbox_id: str, needle: str) -> bool:
        base = Path(self.store.sandbox_path(sandbox_id))
        for path in [base, Path(str(base) + "-wal"), Path(str(base) + "-shm")]:
            if path.exists() and needle.encode() in path.read_bytes():
                return True
        return False

    def make_three(self):
        a = self.store.create_sandbox("分析 A")["sandbox_id"]
        b = self.store.create_sandbox("分析 B")["sandbox_id"]
        c = self.store.create_sandbox("分析 C")["sandbox_id"]
        return a, b, c


class CloneAndIsolation(StoreCase):
    def test_clone_shares_base_instead_of_copying_rows(self):
        a, b, c = self.make_three()
        seg_id = self.store.sandbox_detail(a)["base_segment"]
        self.assertEqual(seg_id, self.store.sandbox_detail(b)["base_segment"])
        with self.store.data() as conn:
            payload = self.store._segment_payload(conn, seg_id)
        self.assertEqual(len(payload), 48)
        # The sandbox file must not contain row payloads.
        raw = Path(self.store.sandbox_path(a)).read_bytes()
        self.assertNotIn("云杉物流".encode(), raw)
        self.assertGreater(len(raw), 0)
        self.assertEqual(self.store.sandbox_detail(a)["base_shared_refs"], 3)

    def test_branch_writes_are_isolated(self):
        a, b, c = self.make_three()
        self.store.write_row(a, "acct-001", {"notes": "A 的假设"})
        self.store.delete_row(b, "acct-002")
        self.store.write_row(c, "acct-003", {"amount_cents": 1})
        self.assertEqual(self.rows(a)["acct-001"]["notes"], "A 的假设")
        self.assertEqual(self.rows(b)["acct-001"]["notes"], "年度续约")
        self.assertEqual(self.rows(c)["acct-001"]["notes"], "年度续约")
        self.assertNotIn("acct-002", self.rows(b))
        self.assertIn("acct-002", self.rows(a))
        self.assertEqual(self.rows(c)["acct-003"]["amount_cents"], 1)
        self.assertEqual(self.rows(a)["acct-003"]["amount_cents"], 1405500)
        # Upstream is untouched by any sandbox write.
        self.assertEqual(self.upstream_note("acct-001"), "年度续约")

    def test_column_overlay_is_local(self):
        a, b, _ = self.make_three()
        self.store.update_column_meta(
            a, [{"key": "notes", "label": "内部批注", "visible": False}]
        )
        columns_a = {c["key"]: c for c in self.store.sandbox_detail(a)["columns"]}
        columns_b = {c["key"]: c for c in self.store.sandbox_detail(b)["columns"]}
        self.assertEqual(columns_a["notes"]["label"], "内部批注")
        self.assertFalse(columns_a["notes"]["visible"])
        self.assertEqual(columns_b["notes"]["label"], "备注")
        self.assertTrue(columns_b["notes"]["visible"])

    def test_deleted_row_restore_brings_back_local_edit(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-005", {"notes": "本地修正"})
        self.store.delete_row(a, "acct-005")
        self.assertNotIn("acct-005", self.rows(a))
        deleted = self.rows(a, include_deleted=True)["acct-005"]
        self.assertTrue(deleted["_local_deleted"])
        self.store.restore_row(a, "acct-005")
        self.assertEqual(self.rows(a)["acct-005"]["notes"], "本地修正")

    def test_search_and_sort(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        view = self.store.read_rows(a, query="云杉", sort="amount_cents", direction="desc")
        self.assertGreaterEqual(view["total"], 1)
        amounts = [row["amount_cents"] for row in view["rows"]]
        self.assertEqual(amounts, sorted(amounts, reverse=True))
        region_view = self.store.read_rows(a, region="华东")
        self.assertTrue(all(r["region"] == "华东" for r in region_view["rows"]))


class RefreshSemantics(StoreCase):
    def test_refresh_imports_upstream_and_keeps_local_edits(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-001", {"notes": "本地保留"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游新备注", "acct-010")
            )
        )
        result = self.store.refresh(a)
        self.assertEqual(result["status"], "refreshed")
        rows = self.rows(a)
        self.assertEqual(rows["acct-001"]["notes"], "本地保留")
        self.assertEqual(rows["acct-010"]["notes"], "上游新备注")

    def test_pinned_base_hides_upstream_until_refresh(self):
        a, b, _ = self.make_three()
        original = self.upstream_note("acct-020")
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游已改", "acct-020")
            )
        )
        self.assertEqual(self.rows(a)["acct-020"]["notes"], original)
        self.store.refresh(a)
        self.assertEqual(self.rows(a)["acct-020"]["notes"], "上游已改")
        self.assertEqual(self.rows(b)["acct-020"]["notes"], original)

    def test_remove_upstream_disappears_on_refresh(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.upstream_publish(
            lambda conn: conn.execute('DELETE FROM "rows" WHERE row_id = ?', ("acct-030",))
        )
        self.assertIn("acct-030", self.rows(a))
        self.store.refresh(a)
        self.assertNotIn("acct-030", self.rows(a))

    def test_conflict_blocks_base_advance_until_decided(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-007", {"notes": "本地版本"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游版本", "acct-007")
            )
        )
        with self.assertRaises(RefreshConflict) as ctx:
            self.store.refresh(a)
        result = ctx.exception.result
        self.assertEqual([c["row_id"] for c in result["conflicts"]], ["acct-007"])
        detail = self.store.sandbox_detail(a)
        self.assertTrue(detail["pending_refresh"])
        self.assertEqual(detail["open_conflicts"], 1)
        # Local value still wins; base has not moved.
        self.assertEqual(self.rows(a)["acct-007"]["notes"], "本地版本")
        with self.assertRaises(StoreError) as apply_ctx:
            self.store.apply_refresh(a)
        self.assertEqual(apply_ctx.exception.code, "conflicts_unresolved")
        self.store.resolve_conflict(a, "acct-007", "keep")
        applied = self.store.apply_refresh(a)
        self.assertEqual(applied["segment_id"], result["candidate_segment"])
        self.assertEqual(self.rows(a)["acct-007"]["notes"], "本地版本")
        self.assertFalse(self.store.sandbox_detail(a)["pending_refresh"])

    def test_take_upstream_drops_local_overlay(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-008", {"notes": "本地"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-008")
            )
        )
        with self.assertRaises(RefreshConflict):
            self.store.refresh(a)
        self.store.resolve_conflict(a, "acct-008", "take")
        self.store.apply_refresh(a)
        self.assertEqual(self.rows(a)["acct-008"]["notes"], "上游")
        self.assertEqual(self.store.sandbox_detail(a)["local_writes"], 0)

    def test_delete_vs_upstream_delete_is_not_a_conflict(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.delete_row(a, "acct-009")
        self.upstream_publish(
            lambda conn: conn.execute('DELETE FROM "rows" WHERE row_id = ?', ("acct-009",))
        )
        result = self.store.refresh(a)
        self.assertEqual(result["status"], "refreshed")
        self.assertNotIn("acct-009", self.rows(a))

    def test_local_delete_vs_upstream_update_is_a_conflict(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.delete_row(a, "acct-011")
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-011")
            )
        )
        with self.assertRaises(RefreshConflict) as ctx:
            self.store.refresh(a)
        self.assertTrue(ctx.exception.result["conflicts"][0]["local_tombstone"])

    def test_preview_matches_apply(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-012", {"notes": "本地"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-012")
            )
        )
        preview = self.store.preview_refresh(a)
        self.assertEqual([c["row_id"] for c in preview["conflicts"]], ["acct-012"])
        with self.assertRaises(RefreshConflict) as ctx:
            self.store.refresh(a)
        self.assertEqual(
            [c["row_id"] for c in ctx.exception.result["conflicts"]],
            [c["row_id"] for c in preview["conflicts"]],
        )

    def test_refresh_is_idempotent_when_already_current(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        first = self.store.refresh(a)
        second = self.store.refresh(a)
        self.assertEqual(second["status"], "up_to_date")
        self.assertEqual(second["stats"]["added"], 0)
        self.assertEqual(self.store.sandbox_detail(a)["base_segment"], first.get("segment_id", self.store.sandbox_detail(a)["base_segment"]))


class HistoryAndCompaction(StoreCase):
    def test_as_of_reconstructs_overlay(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-001", {"notes": "v1"})
        self.store.write_row(a, "acct-001", {"notes": "v2"})
        self.assertEqual(self.rows(a)["acct-001"]["notes"], "v2")
        self.assertEqual(self.rows(a, as_of=1)["acct-001"]["notes"], "v1")
        self.assertEqual(self.rows(a, as_of=0)["acct-001"]["notes"], "年度续约")

    def test_history_is_bounded_and_old_points_are_refused(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        with self.store.data() as conn:
            with transaction(conn):
                self.store._set_meta(conn, "history_retention", "2")
        for i in range(8):
            self.store.write_row(a, "acct-001", {"notes": f"v{i}"})
        detail = self.store.sandbox_detail(a)
        self.assertGreater(detail["history_horizon"], 0)
        self.assertLessEqual(detail["history_entries"], 3)
        with self.assertRaises(StoreError) as ctx:
            self.store.read_rows(a, as_of=0)
        self.assertEqual(ctx.exception.code, "history_pruned")
        # Inside the window, answers remain exact.
        latest = detail["seq"]
        self.assertEqual(self.rows(a, as_of=latest)["acct-001"]["notes"], "v7")

    def test_as_of_respects_old_pinned_base(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        first_segment = self.store.sandbox_detail(a)["base_segment"]
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("rev2", "acct-001")
            )
        )
        refresh = self.store.refresh(a)
        self.assertEqual(self.rows(a)["acct-001"]["notes"], "rev2")
        as_of_before = self.rows(a, as_of=refresh["seq"] - 1)
        self.assertEqual(as_of_before["acct-001"]["notes"], "年度续约")
        self.assertEqual(
            self.store.sandbox_detail(a)["base_history"][0]["segment_id"], first_segment
        )


class Erasure(StoreCase):
    def test_erasure_removes_from_upstream_segments_and_all_sandboxes(self):
        a, b, c = self.make_three()
        # Materialise the row in each sandbox in a different way.
        self.store.read_rows(a)
        self.store.write_row(b, "acct-007", {"notes": "B 的本地副本"})
        self.store.create_export(c)
        export_b = self.store.create_export(b)
        self.store.read_rows(b)
        report = self.store.erase_row("acct-007", "监管要求删除该客户记录")["report"]
        self.assertTrue(report["completed"])
        self.assertEqual(len(report["segments_rewritten"]), 1)
        self.assertEqual(len(report["exports_revoked"]), 2)
        for sid in (a, b, c):
            self.assertNotIn("acct-007", self.rows(sid))
            self.assertNotIn("acct-007", self.rows(sid, as_of=0))
        with self.assertRaises(StoreError) as ctx:
            self.store.export_file(export_b["export_id"])
        self.assertEqual(ctx.exception.code, "export_revoked")
        residue = self.store.erasure_report("acct-007")
        self.assertTrue(residue["clean"], residue)
        # No live segment contains the payload any more.
        with self.store.data() as conn:
            for segment in conn.execute("SELECT segment_id FROM segments").fetchall():
                payload = self.store._segment_payload(conn, segment["segment_id"])
                if payload is not None:
                    self.assertFalse(any(r["row_id"] == "acct-007" for r in payload))

    def test_clone_after_erasure_cannot_resurrect(self):
        report = self.store.erase_row("acct-006", "客户行使删除权")["report"]
        self.assertTrue(report["upstream_removed"])
        fresh = self.store.create_sandbox("新分析")["sandbox_id"]
        self.assertNotIn("acct-006", self.rows(fresh))
        with self.assertRaises(StoreError) as ctx:
            self.store.write_row(fresh, "acct-006", {"notes": "试图复活"})
        self.assertEqual(ctx.exception.code, "row_erased")

    def test_refresh_cannot_resurrect_erased_row(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.erase_row("acct-013", "客户要求删除")
        result = self.store.refresh(a)
        # The pinned base resolves through the replacement chain, so the
        # sandbox may already be current; either way the row is gone.
        self.assertIn(result["status"], {"refreshed", "up_to_date"})
        self.assertNotIn("acct-013", self.rows(a))

    def test_erasing_row_absent_from_upstream_but_pinned(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        # Upstream drops the row without erasing it; sandbox still pins it.
        self.upstream_publish(
            lambda conn: conn.execute('DELETE FROM "rows" WHERE row_id = ?', ("acct-014",))
        )
        self.assertIn("acct-014", self.rows(a))
        report = self.store.erase_row("acct-014", "该记录在旧快照中仍可见")["report"]
        self.assertFalse(report["upstream_removed"])
        self.assertNotIn("acct-014", self.rows(a))
        self.assertTrue(self.store.erasure_report("acct-014")["clean"])

    def test_hold_requires_explicit_acknowledgement(self):
        with self.assertRaises(StoreError) as ctx:
            self.store.erase_row("acct-004", "合规调查要求删除该记录")
        self.assertEqual(ctx.exception.code, "record_hold")
        report = self.store.erase_row(
            "acct-004", "合规调查要求删除该记录", acknowledge_hold=True
        )["report"]
        self.assertTrue(report["hold_acknowledged"])

    def test_erasure_redacts_history_but_keeps_audit(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-015", {"notes": "历史里的秘密"})
        self.store.erase_row("acct-015", "监管要求彻底删除")
        with self.store.sandbox(a) as sconn:
            history = sconn.execute(
                "SELECT action, before_state, after_state, redacted, erased "
                "FROM row_history WHERE row_id = ?",
                ("acct-015",),
            ).fetchall()
        self.assertTrue(history)
        self.assertTrue(all(row["redacted"] == 1 and row["erased"] == 1 for row in history))
        self.assertTrue(all(row["before_state"] is None and row["after_state"] is None for row in history))
        past = self.store.read_rows(a, as_of=0)
        self.assertNotIn("acct-015", [r["row_id"] for r in past["rows"]])
        self.assertIn("acct-015", past["erased_hidden"])
        # Audit still records that the row existed and which digest was removed.
        report = self.store.erasure_report("acct-015")
        self.assertTrue(report["payload_digest"])

    def test_erasure_removes_payload_bytes_from_sandbox_files(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        unique = "ZZ-UNIQUE-PAYLOAD-4711"
        self.store.write_row(a, "acct-016", {"notes": unique})
        # Before erasure the sandbox files hold the payload (that is the point
        # of a real local copy).  WAL means it may live beside the main file.
        self.assertTrue(
            self._payload_in_sandbox_files(a, unique),
            "expected the payload to be materialised in the sandbox files",
        )
        self.store.erase_row("acct-016", "删除请求 ZX-4711")
        self.assertFalse(
            self._payload_in_sandbox_files(a, unique),
            "erasure left the payload readable in the sandbox files",
        )
        for path in Path(self.store.root).rglob("*"):
            if path.is_file():
                self.assertNotIn(unique.encode(), path.read_bytes(), path.name)

    def test_purge_is_idempotent(self):
        first = self.store.erase_row("acct-017", "首次删除请求")["report"]
        second = self.store.erase_row("acct-017", "首次删除请求")
        self.assertTrue(second["already_erased"])
        self.assertEqual(second["report"]["payload_digest"], first["payload_digest"])

    def test_pending_conflict_is_cleaned_by_erasure(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-018", {"notes": "本地"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-018")
            )
        )
        with self.assertRaises(RefreshConflict):
            self.store.refresh(a)
        self.store.erase_row("acct-018", "上游数据主体删除请求")
        self.assertEqual(self.store.conflicts(a)["open_conflicts"], 0)
        self.store.apply_refresh(a)
        self.assertNotIn("acct-018", self.rows(a))


class CrashRecovery(StoreCase):
    def test_crash_after_tombstone_is_completed_by_recover(self):
        class Boom(Exception):
            pass

        def failpoint(name):
            if name == "erase.after_tombstone":
                raise Boom(name)

        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-019", {"notes": "本地保留"})
        self.store.create_export(a)
        crashing = Store(self.root, failpoint=failpoint)
        with self.assertRaises(Boom):
            crashing.erase_row("acct-019", "崩溃恢复测试请求")
        # Tombstone is committed: reads hide the row even before the scrub.
        self.assertNotIn("acct-019", self.rows(a))
        self.assertFalse(self.store.erasure_report("acct-019")["clean"])
        recovered = self.store.recover()
        self.assertEqual(recovered["recovered"], 1)
        self.assertTrue(self.store.erasure_report("acct-019")["clean"])
        self.assertNotIn("acct-019", self.rows(a))

    def test_crash_before_swap_leaves_no_stale_pending_refresh(self):
        class Boom(Exception):
            pass

        def failpoint(name):
            if name == "refresh.after_conflict_stage":
                raise Boom(name)

        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.write_row(a, "acct-020", {"notes": "本地"})
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-020")
            )
        )
        crashing = Store(self.root, failpoint=failpoint)
        with self.assertRaises(Boom):
            crashing.refresh(a)
        # The pending refresh is committed before the failpoint; a retry must
        # refuse to overwrite it rather than silently stacking a second one.
        with self.assertRaises(StoreError) as ctx:
            self.store.refresh(a)
        self.assertEqual(ctx.exception.code, "refresh_pending")
        self.assertEqual(self.rows(a)["acct-020"]["notes"], "本地")

    def test_crash_during_apply_keeps_old_base(self):
        class Boom(Exception):
            pass

        def failpoint(name):
            if name == "refresh.after_swap":
                raise Boom(name)

        a = self.store.create_sandbox("A")["sandbox_id"]
        self.upstream_publish(
            lambda conn: conn.execute(
                'UPDATE "rows" SET notes = ? WHERE row_id = ?', ("上游", "acct-021")
            )
        )
        crashing = Store(self.root, failpoint=failpoint)
        with self.assertRaises(Boom):
            crashing.refresh(a)
        # Swap committed before the crash; the sandbox is consistent and the
        # operation can simply be replayed as up-to-date.
        self.assertEqual(self.rows(a)["acct-021"]["notes"], "上游")
        self.assertEqual(self.store.refresh(a)["status"], "up_to_date")


class DestroyAndExports(StoreCase):
    def test_destroy_removes_files_and_revokes_exports(self):
        a, b, _ = self.make_three()
        export = self.store.create_export(a)
        path = Path(export["path"])
        self.assertTrue(path.exists())
        result = self.store.destroy_sandbox(a, "实验结束")
        self.assertEqual(len(result["removed_files"]), 3)
        self.assertFalse(Path(self.store.sandbox_path(a)).exists())
        self.assertFalse(path.exists())
        with self.assertRaises(StoreError) as ctx:
            self.store.export_file(export["export_id"])
        self.assertEqual(ctx.exception.code, "export_revoked")
        with self.assertRaises(StoreError) as ctx:
            self.store.read_rows(a)
        self.assertEqual(ctx.exception.code, "sandbox_destroyed")
        # Other sandboxes remain readable and independent.
        self.store.write_row(b, "acct-001", {"notes": "B 仍可用"})
        self.assertEqual(self.rows(b)["acct-001"]["notes"], "B 仍可用")

    def test_destroying_one_sandbox_does_not_touch_shared_base(self):
        a, b, _ = self.make_three()
        before = set(
            p.name for p in Path(self.store.segments_dir).glob("*.json.gz")
        )
        self.store.destroy_sandbox(a, "清理")
        after = set(p.name for p in Path(self.store.segments_dir).glob("*.json.gz"))
        self.assertEqual(before, after)
        self.assertEqual(len(self.rows(b)), 48)

    def test_export_reflects_view_and_is_revoked_on_erasure(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.update_column_meta(a, [{"key": "email", "visible": False}])
        export = self.store.create_export(a, region="华东")
        raw = Path(export["path"]).read_text(encoding="utf-8")
        header = raw.splitlines()[0]
        self.assertNotIn("联系邮箱", header)
        self.assertIn("客户名称", header)
        row_id = self.store.read_rows(a, region="华东")["row_ids"][0]
        self.store.erase_row(row_id, "仅删除一条也要吊销整份导出")
        with self.assertRaises(StoreError) as ctx:
            self.store.export_file(export["export_id"])
        self.assertEqual(ctx.exception.code, "export_revoked")

    def test_recover_cleans_up_files_of_destroyed_sandbox(self):
        class Boom(Exception):
            pass

        def failpoint(name):
            if name == "destroy.before_unlink":
                raise Boom(name)

        a = self.store.create_sandbox("A")["sandbox_id"]
        crashing = Store(self.root, failpoint=failpoint)
        with self.assertRaises(Boom):
            crashing.destroy_sandbox(a, "中途崩溃")
        self.assertTrue(Path(self.store.sandbox_path(a)).exists())
        result = self.store.recover()
        self.assertTrue(result["removed_destroyed_files"])
        self.assertFalse(Path(self.store.sandbox_path(a)).exists())


class CacheAndConcurrency(StoreCase):
    def test_read_cache_is_invalidated_by_erasure(self):
        a, b, _ = self.make_three()
        first = self.store.read_rows(a)
        self.assertEqual(first["cache"], "miss")
        self.assertEqual(self.store.read_rows(a)["cache"], "hit")
        self.store.read_rows(b)
        self.store.erase_row(first["row_ids"][0], "删除后缓存不能复用")
        after = self.store.read_rows(a)
        self.assertEqual(after["cache"], "miss")
        self.assertNotIn(first["row_ids"][0], after["row_ids"])

    def test_local_write_invalidates_cache(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.read_rows(a)
        self.store.write_row(a, "acct-001", {"notes": "改了"})
        view = self.store.read_rows(a)
        self.assertEqual(view["cache"], "miss")
        self.assertEqual(
            [r["notes"] for r in view["rows"] if r["row_id"] == "acct-001"], ["改了"]
        )

    def test_concurrent_erasure_and_writes_never_leak_payload(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        secret = "CONCURRENT-SECRET-9812"
        self.store.write_row(a, "acct-022", {"notes": secret})
        errors = []

        def writer():
            try:
                for i in range(20):
                    try:
                        self.store.write_row(a, "acct-022", {"notes": secret + str(i)})
                    except StoreError as exc:
                        if exc.code != "row_erased":
                            raise
            except Exception as exc:  # noqa: BLE001 - collected for assertion
                errors.append(exc)

        def eraser():
            try:
                self.store.erase_row("acct-022", "并发删除测试")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer), threading.Thread(target=eraser)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors, errors)
        self.assertTrue(self.store.erasure_report("acct-022")["clean"])
        raw = Path(self.store.sandbox_path(a)).read_bytes()
        self.assertNotIn(secret.encode(), raw)
        self.assertNotIn("acct-022", self.rows(a))


class InputValidation(StoreCase):
    def test_rejects_bad_writes(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        for fields, code in [
            ({"name": ""}, "invalid_name"),
            ({"email": "not-an-email"}, "invalid_email"),
            ({"amount_cents": -1}, "invalid_amount"),
            ({"unknown": 1}, "no_fields"),
        ]:
            with self.assertRaises(StoreError) as ctx:
                self.store.write_row(a, "acct-001", fields)
            self.assertEqual(ctx.exception.code, code)

    def test_erasure_requires_a_substantive_reason(self):
        with self.assertRaises(StoreError) as ctx:
            self.store.erase_row("acct-001", "短")
        self.assertEqual(ctx.exception.code, "invalid_reason")

    def test_unknown_sandbox(self):
        with self.assertRaises(StoreError) as ctx:
            self.store.read_rows("sb-nope")
        self.assertEqual(ctx.exception.status, 404)


if __name__ == "__main__":
    unittest.main()


class ErasureResidue(StoreCase):
    """Payload bytes must not survive in *any* derived artifact."""

    def assert_no_bytes(self, needle: str):
        for path in Path(self.store.root).rglob("*"):
            if path.is_file():
                self.assertNotIn(needle.encode(), path.read_bytes(), str(path))

    def test_compacted_history_baseline_is_scrubbed(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        with self.store.data() as conn:
            with transaction(conn):
                self.store._set_meta(conn, "history_retention", "2")
        secret = "BASELINE-SECRET-5501"
        self.store.write_row(a, "acct-023", {"notes": secret})
        # Enough unrelated churn to fold the secret into the compaction baseline.
        for i in range(6):
            self.store.write_row(a, "acct-001", {"notes": f"churn {i}"})
        with self.store.sandbox(a) as sconn:
            baseline = sconn.execute("SELECT state FROM history_baseline").fetchone()
        self.assertIn(secret, baseline["state"])
        self.store.erase_row("acct-023", "清除基线中的历史副本")
        with self.store.sandbox(a) as sconn:
            baseline = sconn.execute("SELECT state FROM history_baseline").fetchone()
        self.assertNotIn(secret, baseline["state"] if baseline else "")
        self.assert_no_bytes(secret)

    def test_audit_report_detects_baseline_and_conflict_residue(self):
        # ``erasure_report`` is the independent residue check, so it must not
        # simply echo the stored purge report.  Re-insert residue by hand and
        # confirm the audit notices both derived-copy locations.
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.erase_row("acct-024", "审计复核测试删除请求")
        self.assertTrue(self.store.erasure_report("acct-024")["clean"])
        with self.store.sandbox(a) as sconn:
            with transaction(sconn):
                sconn.execute(
                    "INSERT INTO history_baseline(seq, state, created_at) VALUES(?,?,?)",
                    (
                        0,
                        json.dumps({"acct-024": {"kind": "local", "payload": {"notes": "x"}}}),
                        "2026-11-17T00:00:00Z",
                    ),
                )
                sconn.execute(
                    "INSERT INTO conflicts(row_id, detected_seq, detected_at, "
                    "upstream_payload, local_payload, status) "
                    "VALUES(?,?,?,?,?,'open')",
                    ("acct-024", 0, "2026-11-17T00:00:00Z", "{}", "{}"),
                )
        residue = self.store.erasure_report("acct-024")
        self.assertFalse(residue["clean"], residue)
        entry = [r for r in residue["sandbox_residue"] if r.get("state") == "仍有残留"][0]
        self.assertEqual(entry["baseline_copies"], 1)
        self.assertEqual(entry["conflict_payloads"], 1)

    def test_audit_report_detects_cached_and_exported_copies(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        self.store.read_rows(a)  # populates query_cache
        export = self.store.create_export(a)
        self.store.erase_row("acct-025", "残留复核：缓存与导出")
        self.assertTrue(self.store.erasure_report("acct-025")["clean"])
        with self.store.sandbox(a) as sconn:
            with transaction(sconn):
                sconn.execute(
                    "INSERT INTO query_cache(cache_key, request, body, epoch, created_at) "
                    "VALUES(?,?,?,?,?)",
                    (
                        "stale-key",
                        "{}",
                        json.dumps({"rows": [{"row_id": "acct-025", "notes": "x"}]}),
                        0,
                        "2026-11-17T00:00:00Z",
                    ),
                )
        residue = self.store.erasure_report("acct-025")
        self.assertFalse(residue["clean"], residue)
        entry = [r for r in residue["sandbox_residue"] if r.get("state") == "仍有残留"][0]
        self.assertEqual(entry["cache_copies"], 1)
        # The export was revoked and its file physically removed.
        self.assertEqual(residue["exports"][0]["export_id"], export["export_id"])
        self.assertEqual(residue["exports"][0]["status"], "revoked")
        self.assertFalse(Path(export["path"]).exists())

    def test_operation_logs_are_redacted_for_erased_row(self):
        a = self.store.create_sandbox("A")["sandbox_id"]
        # An upstream edit writes the row's name/amount into operations.detail.
        with self.store.data() as conn:
            with transaction(conn):
                conn.execute(
                    'UPDATE "rows" SET name = ? WHERE row_id = ?',
                    ("OPSLOG-SECRET-NAME", "acct-026"),
                )
                segment = self.store._publish_segment_from_rows(
                    conn, revision=901, note="ops log test"
                )
                self.store._set_meta(conn, "current_segment_id", segment)
                self.store._set_meta(conn, "source_revision", "901")
        with self.store.data() as conn:
            with transaction(conn):
                self.store._op_start(conn, "upstream_edit", None)
                conn.execute(
                    "UPDATE operations SET status='ok', detail=? WHERE kind='upstream_edit'",
                    (
                        json.dumps(
                            {
                                "changes": [
                                    {"op": "update", "row_id": "acct-026", "name": "OPSLOG-SECRET-NAME"},
                                    {"op": "update", "row_id": "acct-027", "name": "keep me"},
                                ]
                            }
                        ),
                    ),
                )
        # Before erasure the content legitimately exists in data.db (live row
        # plus the op log snapshot).
        self.assertIn(
            b"OPSLOG-SECRET-NAME",
            Path(self.store.root / "data.db").read_bytes()
            + b"".join(
                p.read_bytes()
                for p in Path(self.store.root).glob("data.db-wal")
            ),
        )
        self.store.erase_row("acct-026", "运营日志也不应保留内容")
        self.assert_no_bytes("OPSLOG-SECRET-NAME")
        with self.store.data() as conn:
            details = [
                json.loads(r["detail"])
                for r in conn.execute("SELECT detail FROM operations").fetchall()
            ]
        blob = json.dumps(details, ensure_ascii=False)
        # The id remains as an audit tombstone; the content does not.
        self.assertNotIn("OPSLOG-SECRET-NAME", blob)
        self.assertIn("keep me", blob)
        matching = [
            item
            for item in details
            if "changes" in item
        ][0]["changes"]
        redacted = [c for c in matching if c.get("row_id") == "acct-026"][0]
        self.assertEqual(redacted, {"row_id": "acct-026", "redacted": True})
