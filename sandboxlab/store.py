"""Core storage engine: shared content-addressed bases + per-sandbox overlays.

Public API is ``Store``.  Mutations are serialised through SQLite
``BEGIN IMMEDIATE`` transactions.  Erasure additionally takes a process-wide
lock so a clone or local write cannot slip a payload in after the scrub pass.

Terminology used throughout:

* **base** - the frozen upstream snapshot a sandbox pins (a segment file).
* **overlay** - this sandbox's own writes: upserts, tombstones, column meta.
* **seq** - the sandbox's logical clock.  Every user-visible mutation bumps it;
  "as of seq" selects the state after that mutation.
* **erasure** - source-record deletion.  Retroactive, global, and it overrides
  every history promise.
"""

from __future__ import annotations

import contextlib
import csv
import gzip
import hashlib
import io
import json
import os
import random
import re
import secrets
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .errors import StoreError
from .schema import (
    canonical_json,
    connect,
    digest,
    init_data_db,
    init_sandbox_db,
    now_iso,
    transaction,
)
from .seed import REGIONS, UpstreamSimulator, build_seed_rows

ROW_COLUMNS = [
    ("row_id", "客户编号"),
    ("name", "客户名称"),
    ("email", "联系邮箱"),
    ("region", "区域"),
    ("amount_cents", "金额（分）"),
    ("notes", "备注"),
    ("source_updated_at", "源更新时间"),
]
COLUMN_KEYS = [key for key, _ in ROW_COLUMNS]
DEFAULT_LABELS = dict(ROW_COLUMNS)
MAX_AMOUNT = 10**11
HISTORY_HARD_CAP = 500
VALID_SORTS = {"row_id", "name", "amount_cents", "region", "source_updated_at"}
WRITABLE_FIELDS = {"name", "email", "region", "amount_cents", "notes"}


def _json(value: Any, default=None):
    if value is None or value == "":
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _valid_row_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 64
        and all(c.isalnum() or c in "._-" for c in value)
    )


def _valid_email(value: str) -> bool:
    if "@" not in value or " " in value:
        return False
    local, _, domain = value.partition("@")
    return bool(local) and "." in domain and not domain.startswith(".")


def _state_json(
    local: dict | None,
    tombstone: dict | None,
    *,
    base: bool = False,
) -> str:
    """Encode an overlay state for the undo log.

    ``kind=base`` is explicit (rather than NULL) so a restore or an upstream
    takeover is replayable: it tells as-of reconstruction to drop the overlay
    entry and fall back to the segment.
    """
    if tombstone is not None:
        return canonical_json({"kind": "tombstone", "payload": tombstone})
    if local is not None:
        return canonical_json({"kind": "local", "payload": local})
    if base:
        return canonical_json({"kind": "base"})
    return canonical_json({"kind": "base"})


class Store:
    def __init__(self, root: str | os.PathLike, failpoint=None):
        """``failpoint(name)`` is invoked at named crash points; tests pass a
        function that raises to simulate a process death."""
        self.root = Path(root).resolve()
        self.segments_dir = self.root / "segments"
        self.sandboxes_dir = self.root / "sandboxes"
        self.exports_dir = self.root / "exports"
        for directory in (
            self.root,
            self.segments_dir,
            self.sandboxes_dir,
            self.exports_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self.data_path = self.root / "data.db"
        self.failpoint = failpoint or (lambda name: None)
        # Guards erasure against every payload-writing operation (clone, write,
        # refresh, destroy) in this process.
        self._erasure_lock = threading.RLock()

    # ------------------------------------------------------------------ paths

    def sandbox_path(self, sandbox_id: str) -> Path:
        return self.sandboxes_dir / f"{sandbox_id}.sqlite3"

    def segment_path(self, segment_id: str) -> Path:
        return self.segments_dir / f"{segment_id}.json.gz"

    def _segment_refcount(self, data_conn, segment_id: str) -> int:
        """How many active sandboxes resolve to this physical segment.  The
        number is the sharing win made visible: N sandboxes, one file."""
        count = 0
        for row in data_conn.execute(
            "SELECT sandbox_id FROM sandboxes WHERE status = 'active'"
        ).fetchall():
            path = self.sandbox_path(row["sandbox_id"])
            if not path.exists():
                continue
            try:
                with connect(path) as sconn:
                    base = sconn.execute(
                        "SELECT segment_id FROM base_ref"
                    ).fetchone()
            except sqlite3.DatabaseError:
                continue
            if base and self._resolve_segment(data_conn, base["segment_id"]) == segment_id:
                count += 1
        return count

    def _sqlite_files(self, path: Path) -> list[Path]:
        return [Path(str(path) + suffix) for suffix in ("", "-wal", "-shm")]

    # ---------------------------------------------------------------- opening

    @contextlib.contextmanager
    def data(self):
        conn = init_data_db(self.data_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def sandbox(self, sandbox_id: str, *, create: bool = False):
        path = self.sandbox_path(sandbox_id)
        if not path.exists() and not create:
            raise StoreError(404, "sandbox_not_found", "沙箱不存在或已销毁。")
        conn = init_sandbox_db(path, sandbox_id) if create else connect(path)
        try:
            yield conn
        finally:
            conn.close()

    def _meta(self, conn, key: str, default: str = "") -> str:
        row = conn.execute(
            "SELECT value FROM upstream_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def _set_meta(self, conn, key: str, value: str) -> None:
        conn.execute(
            "INSERT INTO upstream_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def _history_retention(self, conn) -> int:
        try:
            value = int(self._meta(conn, "history_retention", "25"))
        except ValueError:
            value = 25
        return max(1, min(value, HISTORY_HARD_CAP))

    def _next_seq(self, conn) -> int:
        row = conn.execute(
            "SELECT value FROM sandbox_info WHERE key = 'seq'"
        ).fetchone()
        return int(row["value"]) if row else 0

    def _bump_seq(self, conn) -> int:
        seq = self._next_seq(conn) + 1
        conn.execute(
            "INSERT INTO sandbox_info(key, value) VALUES('seq', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(seq),),
        )
        return seq

    def _op_start(self, conn, kind: str, sandbox_id: str | None) -> int:
        cur = conn.execute(
            "INSERT INTO operations(kind, sandbox_id, started_at, status) "
            "VALUES(?,?,?,'running')",
            (kind, sandbox_id, now_iso()),
        )
        return int(cur.lastrowid)

    def _op_finish(
        self, conn, op_id: int, status: str, detail: dict | None = None
    ) -> None:
        conn.execute(
            "UPDATE operations SET finished_at = ?, status = ?, detail = ? "
            "WHERE op_id = ?",
            (now_iso(), status, canonical_json(detail or {}), op_id),
        )

    # ------------------------------------------------------------------ setup

    def ensure_seeded(self, count: int = 48) -> dict:
        """Idempotently create the fictional upstream dataset and its initial
        shared segment.  Safe to call on every boot."""
        with self._erasure_lock, self.data() as conn:
            if self._meta(conn, "seeded") == "1":
                return self._upstream_summary(conn)
            rows = build_seed_rows(count)
            with transaction(conn):
                for row in rows:
                    self._upsert_upstream(conn, row)
                segment_id = self._publish_segment_from_rows(
                    conn, revision=1, note="initial seed"
                )
                self._set_meta(conn, "seeded", "1")
                self._set_meta(conn, "source_revision", "1")
                self._set_meta(conn, "current_segment_id", segment_id)
            return self._upstream_summary(conn)

    def _upstream_summary(self, conn) -> dict:
        count = conn.execute('SELECT COUNT(*) AS n FROM "rows"').fetchone()["n"]
        return {
            "row_count": count,
            "source_revision": int(self._meta(conn, "source_revision", "0")),
            "current_segment_id": self._meta(conn, "current_segment_id"),
            "erasure_epoch": int(self._meta(conn, "erasure_epoch", "0")),
            "erased_count": conn.execute(
                "SELECT COUNT(*) AS n FROM erased_rows"
            ).fetchone()["n"],
        }

    def _upsert_upstream(self, conn, row: dict) -> None:
        # Defence in depth: a tombstone always beats an upstream insert, even
        # if a simulator or import pipeline tries to recreate the same id.
        if conn.execute(
            "SELECT 1 FROM erased_rows WHERE row_id = ?", (row["row_id"],)
        ).fetchone():
            return
        conn.execute(
            'INSERT INTO "rows"(row_id, name, email, region, amount_cents, notes, '
            "source_updated_at) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(row_id) DO UPDATE SET name=excluded.name, "
            "email=excluded.email, region=excluded.region, "
            "amount_cents=excluded.amount_cents, notes=excluded.notes, "
            "source_updated_at=excluded.source_updated_at",
            (
                row["row_id"],
                row["name"],
                row["email"],
                row["region"],
                int(row["amount_cents"]),
                row["notes"],
                row["source_updated_at"],
            ),
        )

    # -------------------------------------------------------------- segments

    def _resolve_segment(self, conn, segment_id: str) -> str:
        """Follow replacement links to the newest segment id."""
        seen: set[str] = set()
        current = segment_id
        while current and current not in seen:
            seen.add(current)
            row = conn.execute(
                "SELECT replaced_by FROM segments WHERE segment_id = ?", (current,)
            ).fetchone()
            if row is None or not row["replaced_by"]:
                return current
            current = row["replaced_by"]
        return current

    def _segment_payload(self, conn, segment_id: str) -> list[dict] | None:
        """Read a segment through the erasure replacement chain.

        Returns None if the physical file was pruned; that means the rows are
        unavailable, which is the intended outcome of erasure + pruning.
        """
        resolved = self._resolve_segment(conn, segment_id)
        row = conn.execute(
            "SELECT pruned_at FROM segments WHERE segment_id = ?", (resolved,)
        ).fetchone()
        if row is None or row["pruned_at"]:
            return None
        path = self.segment_path(resolved)
        if not path.exists():
            return None
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)

    def _write_segment_file(self, rows: list[dict]) -> tuple[str, int, int]:
        payload = canonical_json(rows).encode("utf-8")
        segment_id = hashlib.sha256(payload).hexdigest()
        raw = gzip.compress(payload, mtime=0)
        path = self.segment_path(segment_id)
        if not path.exists():
            tmp = path.with_suffix(".tmp")
            with open(tmp, "wb") as fh:
                fh.write(raw)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        return segment_id, len(raw), len(payload)

    def _register_segment(
        self, conn, rows: list[dict], *, revision: int
    ) -> tuple[str, int, int]:
        segment_id, nbytes, raw_bytes = self._write_segment_file(rows)
        conn.execute(
            "INSERT OR IGNORE INTO segments(segment_id, created_at, row_count, "
            "sha256, bytes, raw_bytes, source_revision) VALUES(?,?,?,?,?,?,?)",
            (segment_id, now_iso(), len(rows), segment_id, nbytes, raw_bytes, revision),
        )
        conn.executemany(
            "INSERT OR IGNORE INTO segment_rows(segment_id, row_id) VALUES(?,?)",
            [(segment_id, row["row_id"]) for row in rows],
        )
        return segment_id, nbytes, raw_bytes

    def _publish_segment_from_rows(self, conn, *, revision: int, note: str) -> str:
        rows = [
            dict(r)
            for r in conn.execute(
                'SELECT row_id, name, email, region, amount_cents, notes, '
                'source_updated_at FROM "rows" ORDER BY row_id'
            ).fetchall()
        ]
        erased = {
            r["row_id"]
            for r in conn.execute("SELECT row_id FROM erased_rows").fetchall()
        }
        rows = [r for r in rows if r["row_id"] not in erased]
        segment_id, _, _ = self._register_segment(conn, rows, revision=revision)
        return segment_id

    def _upstream_rows(self, conn) -> list[dict]:
        return [
            dict(r)
            for r in conn.execute(
                'SELECT row_id, name, email, region, amount_cents, notes, '
                'source_updated_at FROM "rows" ORDER BY row_id'
            ).fetchall()
        ]

    # -------------------------------------------------------------- read path

    def _erased_ids(self, conn) -> set[str]:
        return {
            r["row_id"]
            for r in conn.execute("SELECT row_id FROM erased_rows").fetchall()
        }

    def _column_meta(self, sconn) -> list[dict]:
        meta = {
            r["column_name"]: dict(r)
            for r in sconn.execute("SELECT * FROM local_meta").fetchall()
        }
        columns = []
        for key, default_label in ROW_COLUMNS:
            entry = meta.get(key)
            columns.append(
                {
                    "key": key,
                    "label": (entry or {}).get("label") or default_label,
                    "visible": bool((entry or {}).get("visible", 1)),
                }
            )
        return columns

    def _base_history(self, sconn) -> list[dict]:
        return [
            dict(r)
            for r in sconn.execute(
                "SELECT seq, segment_id, created_at, note FROM base_history "
                "ORDER BY seq, id"
            ).fetchall()
        ]

    def _base_segment_at(self, sconn, target_seq: int) -> str:
        row = sconn.execute(
            "SELECT segment_id FROM base_history WHERE seq <= ? "
            "ORDER BY seq DESC, id DESC LIMIT 1",
            (target_seq,),
        ).fetchone()
        if row:
            return row["segment_id"]
        origin = sconn.execute(
            "SELECT segment_id FROM base_history ORDER BY seq, id LIMIT 1"
        ).fetchone()
        return origin["segment_id"] if origin else ""

    def _overlay_state(self, sconn) -> dict[str, dict]:
        state: dict[str, dict] = {}
        for r in sconn.execute("SELECT row_id, payload FROM local_rows").fetchall():
            payload = _json(r["payload"])
            if payload is None:
                continue
            state[r["row_id"]] = {"kind": "local", "payload": payload}
        for r in sconn.execute(
            "SELECT row_id, payload FROM local_tombstones"
        ).fetchall():
            state[r["row_id"]] = {
                "kind": "tombstone",
                "payload": _json(r["payload"]),
            }
        return state

    def _overlay_at(self, sconn, target_seq: int) -> tuple[dict[str, dict], int]:
        """Rebuild overlay state at ``target_seq`` from the compacted baseline
        plus the retained history tail.  Raises if the target is below the
        retention horizon (never silently answers a wrong state)."""
        baseline = sconn.execute(
            "SELECT seq, state FROM history_baseline ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        horizon = 0
        state: dict[str, dict] = {}
        if baseline:
            horizon = baseline["seq"]
            state = _json(baseline["state"], {})
        if target_seq < horizon:
            raise StoreError(
                409,
                "history_pruned",
                "该时间点早于历史保留窗口；压缩后无法保证准确回看。",
                {"horizon": horizon, "requested": target_seq},
            )
        rows = sconn.execute(
            "SELECT after_state FROM row_history WHERE seq > ? AND seq <= ? "
            "ORDER BY seq, id",
            (horizon, target_seq),
        ).fetchall()
        for r in rows:
            raw = r["after_state"]
            if raw is None:
                continue
            decoded = _json(raw)
            if not decoded:
                continue
            rid = (decoded.get("payload") or {}).get("row_id") or decoded.get("row_id")
            if not rid:
                continue
            if decoded.get("kind") == "base":
                state.pop(rid, None)
            else:
                state[rid] = decoded
        return state, horizon

    def _compose_rows(
        self,
        data_conn,
        sconn,
        *,
        base_segment_id: str,
        erased: set[str],
        overlay: dict[str, dict] | None = None,
        include_deleted: bool = False,
    ) -> list[dict]:
        base = self._segment_payload(data_conn, base_segment_id) or []
        rows: dict[str, dict] = {}
        flags: dict[str, str] = {}
        for row in base:
            if row["row_id"] in erased:
                continue
            rows[row["row_id"]] = dict(row)
        if overlay is None:
            overlay = self._overlay_state(sconn)
        for row_id, entry in overlay.items():
            if row_id in erased:
                continue
            kind = entry.get("kind")
            if kind == "local":
                payload = dict(entry.get("payload") or {})
                payload["row_id"] = row_id
                payload["_local"] = True
                rows[row_id] = payload
            elif kind == "tombstone":
                rows.pop(row_id, None)
                if include_deleted:
                    rows[row_id] = {
                        "row_id": row_id,
                        "_local_deleted": True,
                        "name": None,
                        "email": None,
                        "region": None,
                        "amount_cents": None,
                        "notes": None,
                        "source_updated_at": None,
                    }
        return list(rows.values())

    def _filter_rows(
        self,
        rows: list[dict],
        *,
        query: str = "",
        region: str = "",
        sort: str = "row_id",
        direction: str = "asc",
    ) -> list[dict]:
        query = (query or "").strip().casefold()
        if query:
            rows = [
                r
                for r in rows
                if any(
                    query in str(r.get(k) or "").casefold()
                    for k in ("row_id", "name", "email", "notes", "region")
                )
            ]
        if region:
            rows = [r for r in rows if r.get("region") == region]
        sort = sort if sort in VALID_SORTS else "row_id"
        reverse = direction == "desc"

        def key(row):
            value = row.get(sort)
            if value is None:
                return (1, "")
            if sort == "amount_cents":
                return (0, value)
            return (0, str(value))

        rows = sorted(rows, key=key, reverse=reverse)
        return rows

    # ------------------------------------------------------------------ clone

    def create_sandbox(self, label: str) -> dict:
        label = (label or "").strip()
        if not 1 <= len(label) <= 40:
            raise StoreError(400, "invalid_label", "沙箱名称需为 1–40 个字符。")
        with self._erasure_lock, self.data() as conn:
            self.ensure_seeded()
            current = self._meta(conn, "current_segment_id")
            if not current:
                raise StoreError(409, "no_upstream", "上游尚未生成共享数据块。")
            segment_id = self._resolve_segment(conn, current)
            payload = self._segment_payload(conn, segment_id) or []
            sandbox_id = self._new_sandbox_id(conn, label)
            with self.sandbox(sandbox_id, create=True) as sconn:
                with transaction(sconn):
                    seq = self._next_seq(sconn)
                    sconn.execute(
                        "INSERT INTO base_ref(id, segment_id, data_db, "
                        "pinned_row_count, pinned_at) VALUES(1,?,?,?,?)",
                        (
                            segment_id,
                            str(self.data_path),
                            len(payload),
                            now_iso(),
                        ),
                    )
                    sconn.execute(
                        "INSERT INTO base_history(seq, segment_id, created_at, note) "
                        "VALUES(?,?,?,?)",
                        (seq, segment_id, now_iso(), "clone"),
                    )
            with transaction(conn):
                op_id = self._op_start(conn, "clone", sandbox_id)
                conn.execute(
                    "INSERT INTO sandboxes(sandbox_id, label, created_at, status, "
                    "origin_segment_id) VALUES(?,?,?,'active',?)",
                    (sandbox_id, label, now_iso(), segment_id),
                )
                self._op_finish(
                    conn,
                    op_id,
                    "ok",
                    {
                        "segment_id": segment_id,
                        "pinned_rows": len(payload),
                        "strategy": "shared-base+overlay",
                    },
                )
            self.failpoint("clone.after_create")
            return self.sandbox_detail(sandbox_id)

    def _new_sandbox_id(self, conn, label: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")[:16]
        slug = slug or "sbx"
        for _ in range(50):
            candidate = f"sb-{slug}-{secrets.token_hex(2)}"
            exists = conn.execute(
                "SELECT 1 FROM sandboxes WHERE sandbox_id = ?", (candidate,)
            ).fetchone()
            if not exists and not self.sandbox_path(candidate).exists():
                return candidate
        raise StoreError(500, "id_collision", "无法分配沙箱编号，请重试。")

    # ------------------------------------------------------------ sandbox read

    def _catalog(self, data_conn, sandbox_id: str) -> dict:
        row = data_conn.execute(
            "SELECT * FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,)
        ).fetchone()
        if not row:
            raise StoreError(404, "sandbox_not_found", "沙箱不存在。")
        if row["status"] == "destroyed":
            raise StoreError(
                410,
                "sandbox_destroyed",
                "该沙箱已销毁，其本地副本与导出均已清除。",
                {"sandbox_id": sandbox_id, "destroyed_at": row["destroyed_at"]},
            )
        return dict(row)

    def sandbox_detail(self, sandbox_id: str) -> dict:
        with self.data() as data_conn:
            catalog = self._catalog(data_conn, sandbox_id)
            with self.sandbox(sandbox_id) as sconn:
                base = sconn.execute("SELECT * FROM base_ref").fetchone()
                if base is None:
                    raise StoreError(500, "corrupt_sandbox", "沙箱缺少共享基底引用。")
                effective = self._resolve_segment(data_conn, base["segment_id"])
                payload = self._segment_payload(data_conn, base["segment_id"]) or []
                overlay_state = self._overlay_state(sconn)
                local_writes = sum(
                    1 for v in overlay_state.values() if v["kind"] == "local"
                )
                local_deletes = sum(
                    1 for v in overlay_state.values() if v["kind"] == "tombstone"
                )
                conflicts = sconn.execute(
                    "SELECT COUNT(*) AS n FROM conflicts WHERE status = 'open'"
                ).fetchone()["n"]
                pending = sconn.execute(
                    "SELECT * FROM pending_refresh WHERE id = 1"
                ).fetchone()
                history_count = sconn.execute(
                    "SELECT COUNT(*) AS n FROM row_history"
                ).fetchone()["n"]
                horizon = sconn.execute(
                    "SELECT MAX(seq) AS s FROM history_baseline"
                ).fetchone()["s"] or 0
                cache_entries = sconn.execute(
                    "SELECT COUNT(*) AS n FROM query_cache"
                ).fetchone()["n"]
                db_size = sum(
                    p.stat().st_size
                    for p in self._sqlite_files(self.sandbox_path(sandbox_id))
                    if p.exists()
                )
                base_bytes = data_conn.execute(
                    "SELECT raw_bytes, bytes FROM segments WHERE segment_id = ?",
                    (effective,),
                ).fetchone()
                return {
                    "sandbox_id": sandbox_id,
                    "label": catalog["label"],
                    "status": catalog["status"],
                    "created_at": catalog["created_at"],
                    "base_segment": effective,
                    "base_pinned_at": base["pinned_at"],
                    "base_row_count": len(payload),
                    "base_shared_refs": self._segment_refcount(
                        data_conn, effective
                    ),
                    "seq": self._next_seq(sconn),
                    "local_writes": local_writes,
                    "local_deletes": local_deletes,
                    "open_conflicts": conflicts,
                    "pending_refresh": bool(pending),
                    "history_entries": history_count,
                    "history_horizon": horizon,
                    "history_retention": self._history_retention(data_conn),
                    "cache_entries": cache_entries,
                    "disk_bytes": db_size,
                    "base_bytes": (base_bytes["raw_bytes"] if base_bytes else 0),
                    "columns": self._column_meta(sconn),
                    "base_history": self._base_history(sconn),
                    "exports": self._exports_for(data_conn, sandbox_id),
                }

    def read_rows(
        self,
        sandbox_id: str,
        *,
        query: str = "",
        region: str = "",
        sort: str = "row_id",
        direction: str = "asc",
        include_deleted: bool = False,
        as_of: int | None = None,
        use_cache: bool = True,
    ) -> dict:
        request = {
            "query": query.strip(),
            "region": region,
            "sort": sort,
            "direction": direction,
            "include_deleted": bool(include_deleted),
            "as_of": as_of,
        }
        # Reads take the erasure lock too.  Without it a read could compose
        # rows just before an erasure commits and then persist a stale
        # read-through cache entry after the scrub pass has already run.
        with self._erasure_lock, self.data() as data_conn:
            catalog = self._catalog(data_conn, sandbox_id)
            epoch = int(self._meta(data_conn, "erasure_epoch", "0"))
            erased = self._erased_ids(data_conn)
            with self.sandbox(sandbox_id) as sconn:
                horizon = sconn.execute(
                    "SELECT MAX(seq) AS s FROM history_baseline"
                ).fetchone()["s"] or 0
                current_seq = self._next_seq(sconn)
                if as_of is not None:
                    try:
                        as_of = int(as_of)
                    except (TypeError, ValueError):
                        raise StoreError(400, "history_out_of_range", "时间点必须为整数。")
                    if not 0 <= as_of <= current_seq:
                        raise StoreError(
                            400,
                            "history_out_of_range",
                            "时间点需位于 0 与当前版本之间。",
                            {"current_seq": current_seq},
                        )
                cache_key = None
                if use_cache and as_of is None:
                    cache_key = hashlib.sha256(
                        canonical_json(
                            {"sandbox": sandbox_id, "req": request, "epoch": epoch}
                        ).encode("utf-8")
                    ).hexdigest()
                    cached = sconn.execute(
                        "SELECT body, epoch FROM query_cache WHERE cache_key = ?",
                        (cache_key,),
                    ).fetchone()
                    if cached is not None:
                        body = _json(cached["body"], {})
                        # Both conditions matter: the epoch catches any erasure
                        # (including a crashed one that has not scrubbed yet),
                        # and the id intersection catches a stale entry written
                        # before this sandbox knew about the erasure.
                        if cached["epoch"] == epoch and not (
                            set(body.get("row_ids", [])) & erased
                        ):
                            body["cache"] = "hit"
                            return body
                        sconn.execute(
                            "DELETE FROM query_cache WHERE cache_key = ?", (cache_key,)
                        )
                if as_of is None:
                    base_segment = sconn.execute(
                        "SELECT segment_id FROM base_ref"
                    ).fetchone()["segment_id"]
                    overlay = self._overlay_state(sconn)
                    view_seq = current_seq
                    horizon_used = horizon
                else:
                    base_segment = self._base_segment_at(sconn, as_of)
                    overlay, horizon_used = self._overlay_at(sconn, as_of)
                    view_seq = as_of
                history_ids = {
                    r["row_id"]
                    for r in sconn.execute(
                        "SELECT DISTINCT row_id FROM row_history"
                    ).fetchall()
                }
                hidden_erased = sorted(
                    erased
                    & (
                        set(overlay)
                        | history_ids
                        | {
                            r["row_id"]
                            for r in (
                                self._segment_payload(data_conn, base_segment) or []
                            )
                        }
                    )
                )
                rows = self._compose_rows(
                    data_conn,
                    sconn,
                    base_segment_id=base_segment,
                    erased=erased,
                    overlay=overlay,
                    include_deleted=include_deleted,
                )
                rows = self._filter_rows(
                    rows,
                    query=query,
                    region=region,
                    sort=sort,
                    direction=direction,
                )
                result = {
                    "sandbox_id": sandbox_id,
                    "label": catalog["label"],
                    "columns": self._column_meta(sconn),
                    "rows": rows,
                    "row_ids": [r["row_id"] for r in rows],
                    "total": len(rows),
                    "seq": view_seq,
                    "current_seq": current_seq,
                    "as_of": as_of,
                    "history_horizon": horizon_used,
                    "history_retention": self._history_retention(data_conn),
                    "erased_hidden": hidden_erased,
                    "cache": "miss" if cache_key else "off",
                    "regions": REGIONS,
                }
                if cache_key:
                    # Only cache if no local mutation landed while this view
                    # was composed.  Otherwise the cache would replay a stale
                    # overlay even though the seq has moved on.
                    with transaction(sconn):
                        if self._next_seq(sconn) == current_seq:
                            sconn.execute(
                                "INSERT OR REPLACE INTO query_cache(cache_key, "
                                "request, body, epoch, created_at) "
                                "VALUES(?,?,?,?,?)",
                                (
                                    cache_key,
                                    canonical_json(request),
                                    canonical_json(result),
                                    epoch,
                                    now_iso(),
                                ),
                            )
                        else:
                            result["cache"] = "off"
                if as_of is None:
                    self._record_exposure(
                        data_conn, sandbox_id, result["row_ids"], "query"
                    )
                return result

    def _record_exposure(self, data_conn, sandbox_id: str, row_ids, source: str):
        if not row_ids:
            return
        timestamp = now_iso()
        with transaction(data_conn):
            for row_id in row_ids:
                data_conn.execute(
                    "INSERT INTO exposure_log(sandbox_id, row_id, first_seen_at, "
                    "last_seen_at, source) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(sandbox_id, row_id) DO UPDATE SET "
                    "last_seen_at = excluded.last_seen_at, source = excluded.source",
                    (sandbox_id, row_id, timestamp, timestamp, source),
                )

    # ----------------------------------------------------------- overlay write

    def _require_base_row(self, data_conn, sconn, row_id: str) -> dict | None:
        base = sconn.execute("SELECT segment_id FROM base_ref").fetchone()
        if base is None:
            return None
        payload = self._segment_payload(data_conn, base["segment_id"]) or []
        for row in payload:
            if row["row_id"] == row_id:
                return row
        return None

    def _check_not_erased(self, data_conn, row_id: str) -> None:
        if data_conn.execute(
            "SELECT 1 FROM erased_rows WHERE row_id = ?", (row_id,)
        ).fetchone():
            raise StoreError(
                410,
                "row_erased",
                "该源记录已被要求删除，任何沙箱都不能再写入或恢复它。",
                {"row_id": row_id},
            )
        # Re-check with a fresh connection: the sandbox write transaction may
        # have waited behind an erasure that committed its tombstone after the
        # caller's first check.  This is the cross-process half of the guard
        # (the in-process half is ``_erasure_lock``).
        with self.data() as fresh:
            if fresh.execute(
                "SELECT 1 FROM erased_rows WHERE row_id = ?", (row_id,)
            ).fetchone():
                raise StoreError(
                    410,
                    "row_erased",
                    "该源记录刚被要求删除，本地写入已拒绝。",
                    {"row_id": row_id},
                )

    def write_row(self, sandbox_id: str, row_id: str, fields: dict) -> dict:
        if not _valid_row_id(row_id):
            raise StoreError(400, "invalid_row_id", "行编号格式不合法。")
        updates = {k: v for k, v in (fields or {}).items() if k in WRITABLE_FIELDS}
        if not updates:
            raise StoreError(400, "no_fields", "没有可写入的字段。")
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            self._check_not_erased(data_conn, row_id)
            with self.sandbox(sandbox_id) as sconn:
                base_row = self._require_base_row(data_conn, sconn, row_id)
                local = sconn.execute(
                    "SELECT payload FROM local_rows WHERE row_id = ?", (row_id,)
                ).fetchone()
                tomb = sconn.execute(
                    "SELECT payload FROM local_tombstones WHERE row_id = ?", (row_id,)
                ).fetchone()
                current = (
                    _json(local["payload"]) if local else (base_row if base_row else None)
                )
                if current is None and not tomb:
                    raise StoreError(
                        404, "row_not_found", "该行不在沙箱可见数据中。"
                    )
                if current is None and tomb:
                    current = _json(tomb["payload"]) or {"row_id": row_id}
                payload = dict(current or {"row_id": row_id})
                payload.update(updates)
                payload["row_id"] = row_id
                self._validate_payload(payload)
                payload["_local_updated_at"] = now_iso()
                before_state = _state_json(
                    _json(local["payload"]) if local else None,
                    _json(tomb["payload"]) if tomb else None,
                )
                after_state = _state_json(payload, None)
                with transaction(sconn):
                    seq = self._bump_seq(sconn)
                    sconn.execute(
                        "INSERT INTO local_rows(row_id, payload, updated_at) "
                        "VALUES(?,?,?) ON CONFLICT(row_id) DO UPDATE SET "
                        "payload = excluded.payload, updated_at = excluded.updated_at",
                        (row_id, canonical_json(payload), now_iso()),
                    )
                    sconn.execute(
                        "DELETE FROM local_tombstones WHERE row_id = ?", (row_id,)
                    )
                    self._record_history(
                        sconn,
                        seq,
                        row_id,
                        "write",
                        before_state,
                        after_state,
                    )
                    self._compact_history(sconn)
                    sconn.execute("DELETE FROM query_cache")
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "local_write", sandbox_id)
                self._op_finish(
                    data_conn, op_id, "ok", {"row_id": row_id, "seq": seq}
                )
            self._post_commit_erase_check(sandbox_id, row_id, seq)
            return {"ok": True, "sandbox_id": sandbox_id, "row_id": row_id, "seq": seq}

    def _post_commit_erase_check(self, sandbox_id: str, row_id: str, seq: int) -> None:
        """Close the cross-process race window.

        A writer that passed the pre-check just before an erasure committed can
        otherwise commit its payload after the scrub has swept this sandbox.
        Re-checking after commit turns that into a self-healing rejection: the
        payload and its history are removed and the file is checkpointed.

        Read-only restore (restoring a row whose tombstone carried no payload)
        is harmless and does not call this; it never materialises content.
        """
        with self.data() as fresh:
            erased = fresh.execute(
                "SELECT 1 FROM erased_rows WHERE row_id = ?", (row_id,)
            ).fetchone()
        if not erased:
            return
        with self.sandbox(sandbox_id) as sconn:
            with transaction(sconn):
                sconn.execute("DELETE FROM local_rows WHERE row_id = ?", (row_id,))
                sconn.execute("DELETE FROM local_tombstones WHERE row_id = ?", (row_id,))
                sconn.execute(
                    "UPDATE row_history SET before_state = NULL, after_state = NULL, "
                    "redacted = 1, erased = 1 WHERE row_id = ?",
                    (row_id,),
                )
                self._scrub_baseline(sconn, row_id)
                sconn.execute("DELETE FROM query_cache")
                sconn.execute(
                    "INSERT OR REPLACE INTO erased_rows(row_id, erased_at, reason) "
                    "VALUES(?,?,?)",
                    (row_id, now_iso(), "source erasure (post-write)"),
                )
            self._compact_db(sconn)
        raise StoreError(
            410,
            "row_erased",
            "写入提交后检测到该源记录已被删除，本地副本已回收，写入未生效。",
            {"row_id": row_id, "seq": seq},
        )

    def _validate_payload(self, payload: dict) -> None:
        name = payload.get("name")
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 40:
            raise StoreError(400, "invalid_name", "客户名称需为 1–40 个字符。")
        email = payload.get("email")
        if not isinstance(email, str) or not _valid_email(email) or len(email) > 120:
            raise StoreError(400, "invalid_email", "联系邮箱格式不正确。")
        region = payload.get("region")
        if not isinstance(region, str) or not 1 <= len(region.strip()) <= 20:
            raise StoreError(400, "invalid_region", "区域需为 1–20 个字符。")
        try:
            amount = int(payload.get("amount_cents"))
        except (TypeError, ValueError):
            raise StoreError(400, "invalid_amount", "金额必须为整数（分）。")
        if amount < 0 or amount > MAX_AMOUNT:
            raise StoreError(400, "invalid_amount", "金额超出允许范围。")
        notes = payload.get("notes", "")
        if not isinstance(notes, str) or len(notes) > 200:
            raise StoreError(400, "invalid_notes", "备注最多 200 个字符。")
        payload["amount_cents"] = amount

    def _record_history(
        self,
        sconn,
        seq: int,
        row_id: str,
        action: str,
        before_state: str | None,
        after_state: str | None,
        *,
        redacted: bool = False,
        erased: bool = False,
    ) -> None:
        sconn.execute(
            "INSERT INTO row_history(seq, row_id, action, before_state, after_state, "
            "redacted, erased, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                seq,
                row_id,
                action,
                before_state,
                after_state,
                int(redacted),
                int(erased),
                now_iso(),
            ),
        )

    def _compact_history(self, sconn, retention: int | None = None) -> None:
        """Keep only the newest N distinct seqs; collapse everything older into
        a baseline snapshot.  This is what makes lookback bounded."""
        if retention is None:
            with self.data() as data_conn:
                retention = self._history_retention(data_conn)
        retention = max(1, min(int(retention), HISTORY_HARD_CAP))
        baseline = sconn.execute(
            "SELECT seq, state FROM history_baseline ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        horizon = baseline["seq"] if baseline else 0
        seqs = [
            r["seq"]
            for r in sconn.execute(
                "SELECT DISTINCT seq FROM row_history WHERE seq > ? "
                "ORDER BY seq DESC LIMIT ?",
                (horizon, retention + 1),
            ).fetchall()
        ]
        if len(seqs) <= retention:
            return
        new_horizon = seqs[-1]
        if new_horizon <= horizon:
            return
        snapshot = _json(baseline["state"], {}) if baseline else {}
        rows = sconn.execute(
            "SELECT after_state FROM row_history WHERE seq > ? AND seq <= ? "
            "ORDER BY seq, id",
            (horizon, new_horizon),
        ).fetchall()
        for r in rows:
            decoded = _json(r["after_state"])
            if not decoded:
                continue
            rid = (decoded.get("payload") or {}).get("row_id") or decoded.get("row_id")
            if not rid:
                continue
            if decoded.get("kind") == "base":
                snapshot.pop(rid, None)
            else:
                snapshot[rid] = decoded
        sconn.execute(
            "INSERT OR REPLACE INTO history_baseline(seq, state, created_at) "
            "VALUES(?,?,?)",
            (new_horizon, canonical_json(snapshot), now_iso()),
        )
        sconn.execute("DELETE FROM row_history WHERE seq <= ?", (new_horizon,))

    def delete_row(self, sandbox_id: str, row_id: str) -> dict:
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            self._check_not_erased(data_conn, row_id)
            with self.sandbox(sandbox_id) as sconn:
                local = sconn.execute(
                    "SELECT payload FROM local_rows WHERE row_id = ?", (row_id,)
                ).fetchone()
                tomb = sconn.execute(
                    "SELECT 1 FROM local_tombstones WHERE row_id = ?", (row_id,)
                ).fetchone()
                if tomb:
                    return {"ok": True, "sandbox_id": sandbox_id, "row_id": row_id,
                            "already_deleted": True}
                base_row = self._require_base_row(data_conn, sconn, row_id)
                if not local and not base_row:
                    raise StoreError(404, "row_not_found", "该行不在沙箱可见数据中。")
                payload = _json(local["payload"]) if local else dict(base_row)
                before_state = _state_json(payload if local else None, None)
                after_state = _state_json(None, payload)
                with transaction(sconn):
                    seq = self._bump_seq(sconn)
                    sconn.execute(
                        "DELETE FROM local_rows WHERE row_id = ?", (row_id,)
                    )
                    sconn.execute(
                        "INSERT OR REPLACE INTO local_tombstones(row_id, deleted_at, "
                        "payload) VALUES(?,?,?)",
                        (row_id, now_iso(), canonical_json(payload)),
                    )
                    self._record_history(
                        sconn, seq, row_id, "delete", before_state, after_state
                    )
                    self._compact_history(sconn)
                    sconn.execute("DELETE FROM query_cache")
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "local_delete", sandbox_id)
                self._op_finish(data_conn, op_id, "ok", {"row_id": row_id, "seq": seq})
            self._post_commit_erase_check(sandbox_id, row_id, seq)
            return {"ok": True, "sandbox_id": sandbox_id, "row_id": row_id, "seq": seq}

    def restore_row(self, sandbox_id: str, row_id: str) -> dict:
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            self._check_not_erased(data_conn, row_id)
            with self.sandbox(sandbox_id) as sconn:
                tomb = sconn.execute(
                    "SELECT payload FROM local_tombstones WHERE row_id = ?", (row_id,)
                ).fetchone()
                if not tomb:
                    raise StoreError(409, "not_deleted", "该行没有本地删除记录。")
                payload = _json(tomb["payload"])
                before_state = _state_json(None, payload)
                after_state = (
                    _state_json(payload, None)
                    if payload
                    else _state_json(None, None, base=True)
                )
                with transaction(sconn):
                    seq = self._bump_seq(sconn)
                    sconn.execute(
                        "DELETE FROM local_tombstones WHERE row_id = ?", (row_id,)
                    )
                    if payload:
                        sconn.execute(
                            "INSERT OR REPLACE INTO local_rows(row_id, payload, "
                            "updated_at) VALUES(?,?,?)",
                            (row_id, canonical_json(payload), now_iso()),
                        )
                    self._record_history(
                        sconn, seq, row_id, "restore", before_state, after_state
                    )
                    self._compact_history(sconn)
                    sconn.execute("DELETE FROM query_cache")
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "local_restore", sandbox_id)
                self._op_finish(data_conn, op_id, "ok", {"row_id": row_id, "seq": seq})
            self._post_commit_erase_check(sandbox_id, row_id, seq)
            return {"ok": True, "sandbox_id": sandbox_id, "row_id": row_id, "seq": seq}

    def update_column_meta(self, sandbox_id: str, updates: list[dict]) -> dict:
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            with self.sandbox(sandbox_id) as sconn:
                with transaction(sconn):
                    for update in updates or []:
                        key = update.get("key")
                        if key not in COLUMN_KEYS or key == "row_id":
                            raise StoreError(
                                400, "invalid_column", f"列 {key!r} 不可覆盖。"
                            )
                        label = update.get("label")
                        if label is not None:
                            label = str(label).strip()
                            if not 1 <= len(label) <= 20:
                                raise StoreError(
                                    400, "invalid_label", "列别名需为 1–20 个字符。"
                                )
                        visible = update.get("visible")
                        existing = sconn.execute(
                            "SELECT * FROM local_meta WHERE column_name = ?", (key,)
                        ).fetchone()
                        sconn.execute(
                            "INSERT INTO local_meta(column_name, visible, label) "
                            "VALUES(?,?,?) ON CONFLICT(column_name) DO UPDATE SET "
                            "visible = excluded.visible, label = excluded.label",
                            (
                                key,
                                int(bool(visible))
                                if visible is not None
                                else (existing["visible"] if existing else 1),
                                label if label is not None else (
                                    existing["label"] if existing else None
                                ),
                            ),
                        )
                    sconn.execute("DELETE FROM query_cache")
                    seq = self._next_seq(sconn)
                    columns = self._column_meta(sconn)
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "meta", sandbox_id)
                self._op_finish(data_conn, op_id, "ok", {"columns": len(updates or [])})
            return {"ok": True, "columns": columns, "seq": seq}

    # ------------------------------------------------------------- operations

    def list_sandboxes(self) -> list[dict]:
        with self.data() as data_conn:
            catalog = data_conn.execute(
                "SELECT * FROM sandboxes ORDER BY created_at DESC, sandbox_id"
            ).fetchall()
            out = []
            for row in catalog:
                item = dict(row)
                if item["status"] == "active":
                    try:
                        detail = self.sandbox_detail(item["sandbox_id"])
                    except StoreError:
                        detail = None
                    if detail:
                        item.update(
                            {
                                "base_segment": detail["base_segment"],
                                "local_writes": detail["local_writes"],
                                "local_deletes": detail["local_deletes"],
                                "open_conflicts": detail["open_conflicts"],
                                "pending_refresh": detail["pending_refresh"],
                                "disk_bytes": detail["disk_bytes"],
                                "base_row_count": detail["base_row_count"],
                                "seq": detail["seq"],
                            }
                        )
                out.append(item)
            return out

    def upstream_state(self) -> dict:
        with self.data() as data_conn:
            summary = self._upstream_summary(data_conn)
            rows = self._upstream_rows(data_conn)
            erased = data_conn.execute(
                "SELECT row_id, erased_at, reason, payload_digest, purge_detail "
                "FROM erased_rows ORDER BY erased_at DESC"
            ).fetchall()
            segments = data_conn.execute(
                "SELECT segment_id, created_at, row_count, bytes, source_revision, "
                "replaced_by, pruned_at FROM segments ORDER BY created_at DESC"
            ).fetchall()
            return {
                **summary,
                "rows": rows,
                "erased": [dict(r) for r in erased],
                "segments": [dict(r) for r in segments],
                "history_retention": self._history_retention(data_conn),
                "active_sandboxes": data_conn.execute(
                    "SELECT COUNT(*) AS n FROM sandboxes WHERE status = 'active'"
                ).fetchone()["n"],
                "sandbox_bytes": sum(
                    sum(
                        p.stat().st_size
                        for p in self._sqlite_files(self.sandbox_path(r["sandbox_id"]))
                        if p.exists()
                    )
                    for r in data_conn.execute(
                        "SELECT sandbox_id FROM sandboxes WHERE status = 'active'"
                    ).fetchall()
                ),
                "segment_bytes": sum(
                    p.stat().st_size
                    for p in self.segments_dir.glob("*.json.gz")
                    if p.exists()
                ),
                "erasure_epoch": int(self._meta(data_conn, "erasure_epoch", "0")),
            }

    def simulate_upstream(self, ops: int | None = 3, seed: int | None = None) -> dict:
        """Mutate the real upstream table and publish a new shared base.

        Sandboxes do not see this until they refresh; that is the whole point
        of pinning a base.
        """
        if ops is not None:
            try:
                ops = int(ops)
            except (TypeError, ValueError):
                raise StoreError(400, "invalid_ops", "变更数量必须为整数。")
            if not 1 <= ops <= 10:
                raise StoreError(400, "invalid_ops", "变更数量需为 1–10。")
        with self._erasure_lock, self.data() as conn:
            self.ensure_seeded()
            with transaction(conn):
                op_id = self._op_start(conn, "upstream_edit", None)
                rows = self._upstream_rows(conn)
                simulator = UpstreamSimulator(
                    random.Random(seed) if seed is not None else random.Random()
                )
                changes = simulator.mutate(rows, ops=ops)
                for row in rows:
                    self._upsert_upstream(conn, row)
                revision = int(self._meta(conn, "source_revision", "0")) + 1
                segment_id = self._publish_segment_from_rows(
                    conn, revision=revision, note=f"upstream edit #{revision}"
                )
                self._set_meta(conn, "source_revision", str(revision))
                self._set_meta(conn, "current_segment_id", segment_id)
                result = {
                    "revision": revision,
                    "segment_id": segment_id,
                    "changes": changes,
                    "row_count": len(rows),
                }
                self._op_finish(conn, op_id, "ok", result)
            return result

    # --------------------------------------------------------------- refresh

    def _effective_segment(self, data_conn) -> str:
        current = self._meta(data_conn, "current_segment_id")
        return self._resolve_segment(data_conn, current)

    @staticmethod
    def _row_slice(row: dict | None) -> dict | None:
        if row is None:
            return None
        return {key: row.get(key) for key in COLUMN_KEYS}

    def _conflict_candidate(
        self,
        base_row: dict | None,
        upstream_row: dict | None,
        local_entry: dict | None,
    ) -> bool:
        """True when a local overlay edit collides with an upstream change."""
        if local_entry is None:
            return False
        upstream_changed = self._row_slice(base_row) != self._row_slice(upstream_row)
        if not upstream_changed:
            return False
        if local_entry["kind"] == "tombstone":
            # Deleting a row upstream also deleted is not a conflict.
            return upstream_row is not None
        local_payload = local_entry.get("payload") or {}
        if upstream_row is None:
            return True
        return self._row_slice(local_payload) != self._row_slice(upstream_row)

    def _pending(self, sconn) -> dict | None:
        row = sconn.execute("SELECT * FROM pending_refresh WHERE id = 1").fetchone()
        return dict(row) if row else None

    def refresh(self, sandbox_id: str) -> dict:
        """Re-pin the sandbox to the current upstream base, importing changes.

        Local edits are never overwritten: a row edited locally *and* changed
        upstream becomes an explicit conflict, and the base is not advanced
        until the user resolves it.
        """
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            candidate_id = self._effective_segment(data_conn)
            if not candidate_id:
                raise StoreError(409, "no_upstream", "上游尚未生成共享数据块。")
            revision = int(self._meta(data_conn, "source_revision", "0"))
            erased = self._erased_ids(data_conn)
            candidate_rows = {
                r["row_id"]: r
                for r in (self._segment_payload(data_conn, candidate_id) or [])
                if r["row_id"] not in erased
            }
            with self.sandbox(sandbox_id) as sconn:
                pending = self._pending(sconn)
                if pending:
                    raise StoreError(
                        409,
                        "refresh_pending",
                        "已有待处理的刷新冲突，请先逐条决定保留本地还是采用上游。",
                        {"pending": pending},
                    )
                base = sconn.execute("SELECT * FROM base_ref").fetchone()
                current_segment = base["segment_id"]
                if self._resolve_segment(data_conn, current_segment) == candidate_id:
                    with self.data() as conn2:
                        with transaction(conn2):
                            op_id = self._op_start(conn2, "refresh", sandbox_id)
                            self._op_finish(
                                conn2,
                                op_id,
                                "skipped",
                                {"reason": "already_current", "segment_id": candidate_id},
                            )
                    return {
                        "status": "up_to_date",
                        "sandbox_id": sandbox_id,
                        "segment_id": candidate_id,
                        "conflicts": [],
                        "stats": self._refresh_stats(candidate_rows, candidate_rows),
                    }
                base_rows = {
                    r["row_id"]: r
                    for r in (self._segment_payload(data_conn, current_segment) or [])
                    if r["row_id"] not in erased
                }
                overlay = self._overlay_state(sconn)
                conflicts: dict[str, dict] = {}
                for row_id, entry in overlay.items():
                    if row_id in erased:
                        continue
                    if self._conflict_candidate(
                        base_rows.get(row_id), candidate_rows.get(row_id), entry
                    ):
                        local_payload = None
                        tombstone = entry["kind"] == "tombstone"
                        if not tombstone:
                            local_payload = entry.get("payload")
                        conflicts[row_id] = {
                            "row_id": row_id,
                            "upstream_payload": candidate_rows.get(row_id),
                            "local_payload": local_payload,
                            "local_tombstone": tombstone,
                        }
                stats = self._refresh_stats(base_rows, candidate_rows)
                op_id = None
            if not conflicts:
                result = self._apply_refresh(
                    sandbox_id,
                    candidate_id,
                    revision,
                    conflicts={},
                    stats=stats,
                    note="refresh",
                )
                result["status"] = "refreshed"
                return result
            # Stage the candidate base and the conflicts atomically.  Nothing is
            # visible to reads until apply, and a crash before this commit just
            # means no pending refresh exists.
            with self.sandbox(sandbox_id) as sconn:
                with transaction(sconn):
                    seq = self._next_seq(sconn) + 1
                    sconn.execute(
                        "INSERT OR REPLACE INTO pending_refresh(id, seq, segment_id, "
                        "source_revision, detected_at, op_id) VALUES(1,?,?,?,?,NULL)",
                        (seq, candidate_id, revision, now_iso()),
                    )
                    for conflict in conflicts.values():
                        sconn.execute(
                            "INSERT OR REPLACE INTO conflicts(row_id, detected_seq, "
                            "detected_at, upstream_payload, local_payload, "
                            "local_tombstone, status) VALUES(?,?,?,?,?,?,'open')",
                            (
                                conflict["row_id"],
                                seq,
                                now_iso(),
                                canonical_json(conflict["upstream_payload"])
                                if conflict["upstream_payload"] is not None
                                else None,
                                canonical_json(conflict["local_payload"])
                                if conflict["local_payload"] is not None
                                else None,
                                int(conflict["local_tombstone"]),
                            ),
                        )
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "refresh", sandbox_id)
                self._set_op_conflict(
                    data_conn,
                    op_id,
                    {
                        "candidate_segment": candidate_id,
                        "source_revision": revision,
                        "conflicts": len(conflicts),
                        "stats": stats,
                    },
                )
            self.failpoint("refresh.after_conflict_stage")
            from .errors import RefreshConflict

            detail = self._conflict_payload(sandbox_id)
            raise RefreshConflict(
                {
                    "status": "conflict",
                    "sandbox_id": sandbox_id,
                    "candidate_segment": candidate_id,
                    "source_revision": revision,
                    "stats": stats,
                    **detail,
                }
            )

    def _set_op_conflict(self, conn, op_id: int, detail: dict) -> None:
        conn.execute(
            "UPDATE operations SET finished_at = ?, status = 'conflict', detail = ? "
            "WHERE op_id = ?",
            (now_iso(), canonical_json(detail), op_id),
        )

    @staticmethod
    def _refresh_stats(base_rows: dict, candidate_rows: dict) -> dict:
        added = [k for k in candidate_rows if k not in base_rows]
        removed = [k for k in base_rows if k not in candidate_rows]
        updated = [
            k
            for k in candidate_rows
            if k in base_rows
            and Store._row_slice(base_rows[k]) != Store._row_slice(candidate_rows[k])
        ]
        return {
            "added": len(added),
            "removed": len(removed),
            "updated": len(updated),
            "unchanged": len(candidate_rows) - len(added) - len(updated),
            "sample": (added[:3] + updated[:3] + removed[:3]),
        }

    def _conflict_payload(self, sandbox_id: str) -> dict:
        with self.data() as data_conn:
            erased = self._erased_ids(data_conn)
            with self.sandbox(sandbox_id) as sconn:
                pending = self._pending(sconn)
                conflicts = []
                for r in sconn.execute(
                    "SELECT * FROM conflicts WHERE status IN ('open', 'kept', 'taken') "
                    "ORDER BY row_id"
                ).fetchall():
                    if r["row_id"] in erased:
                        continue
                    conflicts.append(
                        {
                            "row_id": r["row_id"],
                            "upstream_payload": _json(r["upstream_payload"]),
                            "local_payload": _json(r["local_payload"]),
                            "local_tombstone": bool(r["local_tombstone"]),
                            "status": r["status"],
                            "detected_at": r["detected_at"],
                        }
                    )
                open_count = sum(1 for c in conflicts if c["status"] == "open")
                return {
                    "pending": pending,
                    "conflicts": conflicts,
                    "open_conflicts": open_count,
                }

    def conflicts(self, sandbox_id: str) -> dict:
        with self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
        return self._conflict_payload(sandbox_id)

    def resolve_conflict(self, sandbox_id: str, row_id: str, resolution: str) -> dict:
        if resolution not in {"keep", "take"}:
            raise StoreError(400, "invalid_resolution", "resolution 必须为 keep 或 take。")
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            if data_conn.execute(
                "SELECT 1 FROM erased_rows WHERE row_id = ?", (row_id,)
            ).fetchone():
                raise StoreError(410, "row_erased", "该记录已删除，不能对冲突做决定。")
            with self.sandbox(sandbox_id) as sconn:
                pending = self._pending(sconn)
                if not pending:
                    raise StoreError(409, "no_pending_refresh", "当前没有待处理的刷新。")
                row = sconn.execute(
                    "SELECT * FROM conflicts WHERE row_id = ?", (row_id,)
                ).fetchone()
                if row is None:
                    raise StoreError(404, "conflict_not_found", "该行没有待处理冲突。")
                if row["status"] != "open":
                    return {
                        "ok": True,
                        "row_id": row_id,
                        "status": row["status"],
                        "already_resolved": True,
                    }
                resolved = "kept" if resolution == "keep" else "taken"
                with transaction(sconn):
                    sconn.execute(
                        "UPDATE conflicts SET status = ?, resolved_at = ?, "
                        "resolution = ? WHERE row_id = ?",
                        (
                            resolved,
                            now_iso(),
                            "keep_local" if resolution == "keep" else "take_upstream",
                            row_id,
                        ),
                    )
                return {"ok": True, "row_id": row_id, "status": resolved}

    def apply_refresh(self, sandbox_id: str) -> dict:
        """Advance the pinned base once every conflict has a decision."""
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            with self.sandbox(sandbox_id) as sconn:
                pending = self._pending(sconn)
                if not pending:
                    raise StoreError(409, "no_pending_refresh", "当前没有待处理的刷新。")
                unresolved = sconn.execute(
                    "SELECT row_id FROM conflicts WHERE status = 'open' ORDER BY row_id"
                ).fetchall()
                if unresolved:
                    raise StoreError(
                        409,
                        "conflicts_unresolved",
                        "仍有冲突未决定，基底不会推进。",
                        {"open": [r["row_id"] for r in unresolved]},
                    )
                decisions = {
                    r["row_id"]: r["status"]
                    for r in sconn.execute(
                        "SELECT row_id, status FROM conflicts"
                    ).fetchall()
                }
                candidate_id = self._resolve_segment(data_conn, pending["segment_id"])
                candidate_payload = self._segment_payload(data_conn, candidate_id)
                if candidate_payload is None:
                    raise StoreError(
                        409,
                        "segment_unavailable",
                        "待应用的共享块已不可读，刷新未推进。请重新发起刷新。",
                        {"segment_id": candidate_id},
                    )
                candidate_rows = {r["row_id"]: r for r in candidate_payload}
                base_rows = {
                    r["row_id"]: r
                    for r in (
                        self._segment_payload(
                            data_conn,
                            sconn.execute(
                                "SELECT segment_id FROM base_ref"
                            ).fetchone()["segment_id"],
                        )
                        or []
                    )
                }
                stats = self._refresh_stats(base_rows, candidate_rows)
                revision = pending["source_revision"]
            return self._apply_refresh(
                sandbox_id,
                candidate_id,
                revision,
                conflicts=decisions,
                stats=stats,
                note="refresh",
                pinned_rows=len(candidate_rows),
            )

    def _apply_refresh(
        self,
        sandbox_id: str,
        candidate_id: str,
        revision: int,
        *,
        conflicts: dict[str, str],
        stats: dict,
        note: str,
        pinned_rows: int = 0,
    ) -> dict:
        self.failpoint("refresh.before_swap")
        taken = []
        with self.sandbox(sandbox_id) as sconn:
            with transaction(sconn):
                seq = self._bump_seq(sconn)
                for row_id, status in conflicts.items():
                    if status != "taken":
                        continue
                    local = sconn.execute(
                        "SELECT payload FROM local_rows WHERE row_id = ?", (row_id,)
                    ).fetchone()
                    tomb = sconn.execute(
                        "SELECT payload FROM local_tombstones WHERE row_id = ?",
                        (row_id,),
                    ).fetchone()
                    before = _state_json(
                        _json(local["payload"]) if local else None,
                        _json(tomb["payload"]) if tomb else None,
                    )
                    sconn.execute("DELETE FROM local_rows WHERE row_id = ?", (row_id,))
                    sconn.execute(
                        "DELETE FROM local_tombstones WHERE row_id = ?", (row_id,)
                    )
                    self._record_history(
                        sconn,
                        seq,
                        row_id,
                        "take_upstream",
                        before,
                        _state_json(None, None, base=True),
                    )
                    taken.append(row_id)
                sconn.execute(
                    "UPDATE base_ref SET segment_id = ?, pinned_at = ?, "
                    "pinned_row_count = ?",
                    (candidate_id, now_iso(), pinned_rows),
                )
                sconn.execute(
                    "INSERT INTO base_history(seq, segment_id, created_at, note) "
                    "VALUES(?,?,?,?)",
                    (seq, candidate_id, now_iso(), note),
                )
                sconn.execute("DELETE FROM pending_refresh")
                sconn.execute("UPDATE conflicts SET status = 'obsolete'")
                sconn.execute("DELETE FROM query_cache")
                self._compact_history(sconn)
        self.failpoint("refresh.after_swap")
        with self.data() as data_conn:
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "refresh", sandbox_id)
                result = {
                    "sandbox_id": sandbox_id,
                    "segment_id": candidate_id,
                    "source_revision": revision,
                    "stats": stats,
                    "pinned_rows": pinned_rows,
                    "resolved": conflicts,
                    "taken_upstream": taken,
                    "seq": seq,
                }
                self._op_finish(data_conn, op_id, "ok", result)
        return result

    # ---------------------------------------------------------------- destroy

    def destroy_sandbox(self, sandbox_id: str, reason: str = "") -> dict:
        """Destroy one sandbox: revoke exports, unlink its real DB files.

        Shared segments are refcounted by catalog references, never by this.
        """
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            revoked = self._revoke_exports(
                data_conn, sandbox_id, f"沙箱销毁：{reason or '用户请求'}"
            )
            with self.sandbox(sandbox_id) as sconn:
                local_payloads = [
                    r["payload"]
                    for r in sconn.execute("SELECT payload FROM local_rows").fetchall()
                ]
                # Clear the overlay before removing the file so a partially
                # failed unlink cannot leave readable payloads behind.
                with transaction(sconn):
                    sconn.execute("DELETE FROM local_rows")
                    sconn.execute("DELETE FROM local_tombstones")
                    sconn.execute("DELETE FROM row_history")
                    sconn.execute("DELETE FROM history_baseline")
                    sconn.execute("DELETE FROM query_cache")
                    sconn.execute("DELETE FROM local_meta")
                    sconn.execute("DELETE FROM conflicts")
                    sconn.execute("DELETE FROM base_history")
                    sconn.execute("DELETE FROM base_ref")
                sconn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            with transaction(data_conn):
                destroy_op = self._op_start(data_conn, "destroy", sandbox_id)
                data_conn.execute(
                    "UPDATE sandboxes SET status = 'destroyed', destroyed_at = ?, "
                    "destroyed_reason = ? WHERE sandbox_id = ?",
                    (now_iso(), reason or "用户请求", sandbox_id),
                )
            # Status is committed before the unlink: a crash here leaves an
            # empty, unreadable sandbox whose files recover() removes.
            self.failpoint("destroy.before_unlink")
            removed = []
            for path in self._sqlite_files(self.sandbox_path(sandbox_id)):
                if path.exists():
                    path.unlink()
                    removed.append(path.name)
            with transaction(data_conn):
                self._op_finish(
                    data_conn,
                    destroy_op,
                    "ok",
                    {
                        "removed_files": removed,
                        "revoked_exports": revoked,
                        "cleared_local_rows": len(local_payloads),
                    },
                )
            self.failpoint("destroy.after_unlink")
            return {
                "ok": True,
                "sandbox_id": sandbox_id,
                "removed_files": removed,
                "revoked_exports": revoked,
                "cleared_local_rows": len(local_payloads),
                "exports": [
                    {
                        "export_id": row["export_id"],
                        "status": row["status"],
                        "revoke_reason": row["revoke_reason"],
                    }
                    for row in data_conn.execute(
                        "SELECT export_id, status, revoke_reason FROM exports "
                        "WHERE sandbox_id = ? ORDER BY created_at DESC",
                        (sandbox_id,),
                    ).fetchall()
                ],
            }

    def _exports_for(self, data_conn, sandbox_id: str) -> list[dict]:
        rows = data_conn.execute(
            "SELECT export_id, created_at, status, path, row_count, revoked_at, "
            "revoke_reason FROM exports WHERE sandbox_id = ? ORDER BY created_at DESC",
            (sandbox_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def _revoke_exports(self, data_conn, sandbox_id: str, reason: str) -> int:
        rows = data_conn.execute(
            "SELECT export_id, path FROM exports WHERE sandbox_id = ? "
            "AND status = 'valid'",
            (sandbox_id,),
        ).fetchall()
        for row in rows:
            path = Path(row["path"])
            if path.exists():
                path.unlink()
            data_conn.execute(
                "UPDATE exports SET status = 'revoked', revoked_at = ?, "
                "revoke_reason = ? WHERE export_id = ?",
                (now_iso(), reason, row["export_id"]),
            )
        return len(rows)

    # ---------------------------------------------------------------- exports

    def create_export(
        self,
        sandbox_id: str,
        *,
        query: str = "",
        region: str = "",
        include_deleted: bool = False,
    ) -> dict:
        with self._erasure_lock, self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            view = self.read_rows(
                sandbox_id,
                query=query,
                region=region,
                include_deleted=include_deleted,
                use_cache=False,
            )
            columns = [c for c in view["columns"] if c["visible"]]
            export_id = f"exp-{secrets.token_hex(4)}"
            path = self.exports_dir / f"{export_id}.csv"
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow([c["label"] for c in columns])
            for row in view["rows"]:
                writer.writerow([row.get(c["key"]) for c in columns])
            tmp = path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8", newline="") as fh:
                fh.write(buffer.getvalue())
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            row_ids = view["row_ids"]
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "export", sandbox_id)
                data_conn.execute(
                    "INSERT INTO exports(export_id, sandbox_id, created_at, status, "
                    "path, row_ids, row_count, filters) VALUES(?,?,?,'valid',?,?,?,?)",
                    (
                        export_id,
                        sandbox_id,
                        now_iso(),
                        str(path),
                        canonical_json(row_ids),
                        len(row_ids),
                        canonical_json({"query": query, "region": region}),
                    ),
                )
                self._op_finish(
                    data_conn,
                    op_id,
                    "ok",
                    {"export_id": export_id, "row_count": len(row_ids)},
                )
            self.failpoint("export.after_write")
            self._record_exposure(data_conn, sandbox_id, row_ids, "export")
            return {
                "export_id": export_id,
                "sandbox_id": sandbox_id,
                "row_count": len(row_ids),
                "path": str(path),
                "download": f"/api/exports/{export_id}/download",
            }

    def export_file(self, export_id: str) -> tuple[Path, str]:
        with self.data() as data_conn:
            row = data_conn.execute(
                "SELECT * FROM exports WHERE export_id = ?", (export_id,)
            ).fetchone()
        if row is None:
            raise StoreError(404, "export_not_found", "导出不存在。")
        if row["status"] != "valid":
            raise StoreError(
                410,
                "export_revoked",
                "该导出已因源记录删除或沙箱销毁而被吊销，文件已删除。",
                {"revoked_at": row["revoked_at"], "reason": row["revoke_reason"]},
            )
        path = Path(row["path"])
        if not path.exists():
            raise StoreError(410, "export_missing", "导出文件已不在了。")
        return path, f"{export_id}.csv"

    # ---------------------------------------------------------------- erasure

    def erase_row(
        self,
        row_id: str,
        reason: str,
        *,
        acknowledge_hold: bool = False,
        reason_code: str = "source_request",
    ) -> dict:
        """Erase a source record everywhere: upstream, every shared segment,
        every sandbox overlay, every query cache entry, every export file.

        This is the operation the whole design exists to make trustworthy, so
        it is written to be re-runnable: each step is idempotent and the report
        lists exactly what was touched.  A crash part-way leaves the tombstone
        in place (that is committed first), and a recovery pass completes the
        scrub.  A refresh that started before the erasure cannot resurrect the
        row because segment resolution filters erased ids and clones consult the
        tombstone.
        """
        from .seed import RECORD_HOLDS

        reason = (reason or "").strip()
        if not 4 <= len(reason) <= 300:
            raise StoreError(
                400,
                "invalid_reason",
                "删除依据需为 4–300 个字符（用于审计）。",
            )
        if not _valid_row_id(row_id):
            raise StoreError(400, "invalid_row_id", "行编号格式不合法。")
        with self._erasure_lock, self.data() as data_conn:
            existing = data_conn.execute(
                "SELECT * FROM erased_rows WHERE row_id = ?", (row_id,)
            ).fetchone()
            if existing and existing["purge_detail"] and _json(
                existing["purge_detail"], {}
            ).get("completed"):
                return {
                    "ok": True,
                    "already_erased": True,
                    "report": _json(existing["purge_detail"], {}),
                }
            row = data_conn.execute(
                'SELECT * FROM "rows" WHERE row_id = ?', (row_id,)
            ).fetchone()
            in_pinned = data_conn.execute(
                "SELECT 1 FROM segment_rows WHERE row_id = ? LIMIT 1", (row_id,)
            ).fetchone()
            if row is None and existing is None and not in_pinned:
                raise StoreError(404, "row_not_found", "上游和任何共享块都没有这条记录。")
            hold = RECORD_HOLDS.get(row_id)
            if hold and not acknowledge_hold:
                raise StoreError(
                    409,
                    "record_hold",
                    "该记录存在合规保留义务，需显式确认后才能删除。",
                    {"row_id": row_id, "hold": hold},
                )
            payload = dict(row) if row else None
            payload_digest = digest({k: payload[k] for k in COLUMN_KEYS}) if payload else (
                existing["payload_digest"] if existing else digest({"row_id": row_id})
            )
            epoch = int(self._meta(data_conn, "erasure_epoch", "0")) + 1
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "erase", None)
                self._set_meta(data_conn, "erasure_epoch", str(epoch))
                data_conn.execute(
                    "INSERT INTO erased_rows(row_id, erased_at, reason, "
                    "payload_digest, purge_detail) VALUES(?,?,?,?, '{}') "
                    "ON CONFLICT(row_id) DO UPDATE SET erased_at = excluded.erased_at, "
                    "reason = excluded.reason",
                    (row_id, now_iso(), reason, payload_digest),
                )
                data_conn.execute('DELETE FROM "rows" WHERE row_id = ?', (row_id,))
            self.failpoint("erase.after_tombstone")
            report = {
                "row_id": row_id,
                "reason": reason,
                "reason_code": reason_code,
                "payload_digest": payload_digest,
                "erased_at": now_iso(),
                "epoch": epoch,
                "hold_acknowledged": bool(hold),
                "segments_rewritten": [],
                "sandboxes_scrubbed": [],
                "sandboxes_unreachable": [],
                "exports_revoked": [],
                "cache_entries_flushed": 0,
                "upstream_removed": payload is not None,
            }
            report["segments_rewritten"] = self._rewrite_segments_for_erasure(
                data_conn, row_id
            )
            with transaction(data_conn):
                data_conn.execute(
                    "DELETE FROM segment_rows WHERE row_id = ?", (row_id,)
                )
            self.failpoint("erase.after_segments")
            scrub = self._scrub_sandboxes(data_conn, row_id)
            with transaction(data_conn):
                report["operation_logs_redacted"] = self._scrub_operation_details(
                    data_conn, row_id
                )
            report["sandboxes_scrubbed"] = scrub["scrubbed"]
            report["sandboxes_unreachable"] = scrub["unreachable"]
            report["cache_entries_flushed"] = scrub["cache_entries_flushed"]
            report["exports_revoked"] = self._revoke_exports_for_row(
                data_conn, row_id
            )
            self.failpoint("erase.after_scrub")
            report["completed"] = True
            report["public_notes"] = (
                "共享基底已重写；本地覆盖层、读缓存与导出已清理；"
                "历史查看在该记录上返回已删除标记，不再返回内容。"
            )
            with transaction(data_conn):
                data_conn.execute(
                    "UPDATE erased_rows SET purge_detail = ? WHERE row_id = ?",
                    (canonical_json(report), row_id),
                )
                self._op_finish(data_conn, op_id, "ok", report)
            # The audit record (row id + digest) stays; content bytes do not.
            self._compact_db(data_conn)
            return {"ok": True, "report": report}

    def _rewrite_segments_for_erasure(self, data_conn, row_id: str) -> list[dict]:
        """Rewrite every live segment containing ``row_id`` and unlink the old
        file.  Ids stay stable for readers that pinned the old id: the old row
        in ``segments`` points at its replacement."""
        affected = [
            r["segment_id"]
            for r in data_conn.execute(
                "SELECT DISTINCT segment_id FROM segment_rows WHERE row_id = ?",
                (row_id,),
            ).fetchall()
        ]
        rewritten = []
        for segment_id in affected:
            info = data_conn.execute(
                "SELECT * FROM segments WHERE segment_id = ?", (segment_id,)
            ).fetchone()
            if info is None or info["replaced_by"] or info["pruned_at"]:
                resolved = self._resolve_segment(data_conn, segment_id)
                if resolved != segment_id:
                    rewritten.append(
                        {"segment_id": segment_id, "already_replaced_by": resolved}
                    )
                continue
            path = self.segment_path(segment_id)
            if not path.exists():
                continue
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                rows = json.load(fh)
            kept = [r for r in rows if r["row_id"] != row_id]
            new_id, nbytes, raw_bytes = self._register_segment(
                data_conn, kept, revision=info["source_revision"]
            )
            if new_id == segment_id:
                # File already clean (index was stale); never self-link.
                rewritten.append({"segment_id": segment_id, "already_clean": True})
                continue
            with transaction(data_conn):
                data_conn.execute(
                    "UPDATE segments SET replaced_by = ? WHERE segment_id = ?",
                    (new_id, segment_id),
                )
                data_conn.execute(
                    "UPDATE segments SET pruned_at = ? WHERE segment_id = ?",
                    (now_iso(), segment_id),
                )
                if self._meta(data_conn, "current_segment_id") == segment_id:
                    self._set_meta(data_conn, "current_segment_id", new_id)
            path.unlink(missing_ok=True)
            rewritten.append(
                {
                    "segment_id": segment_id,
                    "replacement": new_id,
                    "removed_rows": len(rows) - len(kept),
                }
            )
        return rewritten

    def _remove_destroyed_files(self, data_conn) -> list[str]:
        removed = []
        for dest in data_conn.execute(
            "SELECT sandbox_id FROM sandboxes WHERE status = 'destroyed'"
        ).fetchall():
            for path in self._sqlite_files(self.sandbox_path(dest["sandbox_id"])):
                if path.exists():
                    path.unlink()
                    removed.append(path.name)
        return removed

    @staticmethod
    def _cache_has_row(body: Any, row_id: str) -> bool:
        """True if a cached response still carries the erased row's payload."""
        payload = _json(body, {}) or {}
        return any(
            isinstance(row, dict) and row.get("row_id") == row_id
            for row in payload.get("rows", [])
        )

    @staticmethod
    def _scrub_baseline(sconn, row_id: str) -> None:
        """Remove an erased row from the compacted history baseline.

        The baseline is an aggregate JSON object of overlay states, so it is
        rewritten rather than row-deleted.  Dropping the entry is safe: any
        as-of query at or above the horizon reconstructs the overlay from the
        (redacted) log, and the row is filtered by the tombstone anyway.
        """
        # Every baseline row is scrubbed, not just the newest: compaction may
        # leave older snapshots behind, and any of them is a readable copy.
        for row in sconn.execute(
            "SELECT seq, state FROM history_baseline"
        ).fetchall():
            state = _json(row["state"], {}) or {}
            if row_id not in state:
                continue
            state.pop(row_id, None)
            sconn.execute(
                "UPDATE history_baseline SET state = ? WHERE seq = ?",
                (canonical_json(state), row["seq"]),
            )

    @staticmethod
    def _scrub_operation_details(data_conn, row_id: str) -> int:
        """Strip payload-bearing entries for an erased row from the op log.

        Operation details are JSON snapshots (e.g. an upstream simulation's
        change list includes names and amounts).  We keep the shape and the
        fact that something changed, but replace any dict that names the
        erased row with a redacted marker, so the log remains auditable
        without retaining content.
        """
        changed = 0

        def walk(value):
            nonlocal changed
            if isinstance(value, dict):
                if value.get("row_id") == row_id:
                    changed += 1
                    return {"row_id": row_id, "redacted": True}
                return {key: walk(item) for key, item in value.items()}
            if isinstance(value, list):
                return [walk(item) for item in value]
            return value

        rows = data_conn.execute(
            "SELECT op_id, detail FROM operations WHERE detail LIKE ?",
            (f'%"{row_id}"%',),
        ).fetchall()
        for row in rows:
            before = _json(row["detail"], {})
            after = walk(before)
            if after != before:
                data_conn.execute(
                    "UPDATE operations SET detail = ? WHERE op_id = ?",
                    (canonical_json(after), row["op_id"]),
                )
        return changed

    @staticmethod
    def _compact_db(conn) -> None:
        """Rewrite a database file and truncate its WAL after deleting content.

        ``secure_delete`` zeroes freed cells, but older WAL frames and free
        pages can still hold bytes.  VACUUM rewrites live pages and the
        checkpoint folds/truncates the WAL, so the payload is physically gone.
        """
        conn.execute("VACUUM")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _scrub_sandboxes(self, data_conn, row_id: str) -> dict:
        sandboxes = data_conn.execute(
            "SELECT sandbox_id, status FROM sandboxes"
        ).fetchall()
        scrubbed = []
        unreachable = []
        cache_entries = 0
        for sandbox in sandboxes:
            path = self.sandbox_path(sandbox["sandbox_id"])
            if not path.exists():
                unreachable.append(
                    {
                        "sandbox_id": sandbox["sandbox_id"],
                        "status": sandbox["status"],
                        "why": "沙箱数据库文件不存在，无法就地清理（已销毁）。",
                    }
                )
                continue
            try:
                with self.sandbox(sandbox["sandbox_id"]) as sconn:
                    with transaction(sconn):
                        local = sconn.execute(
                            "SELECT payload FROM local_rows WHERE row_id = ?", (row_id,)
                        ).fetchone()
                        tomb = sconn.execute(
                            "SELECT 1 FROM local_tombstones WHERE row_id = ?", (row_id,)
                        ).fetchone()
                        sconn.execute(
                            "DELETE FROM local_rows WHERE row_id = ?", (row_id,)
                        )
                        sconn.execute(
                            "DELETE FROM local_tombstones WHERE row_id = ?", (row_id,)
                        )
                        sconn.execute(
                            "UPDATE row_history SET before_state = NULL, "
                            "after_state = NULL, redacted = 1, erased = 1 "
                            "WHERE row_id = ? AND (before_state IS NOT NULL OR "
                            "after_state IS NOT NULL)",
                            (row_id,),
                        )
                        self._scrub_baseline(sconn, row_id)
                        sconn.execute(
                            "UPDATE conflicts SET upstream_payload = NULL, "
                            "local_payload = NULL, status = 'obsolete' "
                            "WHERE row_id = ?",
                            (row_id,),
                        )
                        sconn.execute(
                            "INSERT OR REPLACE INTO erased_rows(row_id, erased_at, "
                            "reason) VALUES(?,?,?)",
                            (row_id, now_iso(), "source erasure"),
                        )
                        flushed = sconn.execute(
                            "DELETE FROM query_cache"
                        ).rowcount
                        cache_entries += max(0, flushed)
                    # Rewrite the file, then truncate the WAL.  secure_delete
                    # zeroes freed cell content; VACUUM rewrites live pages;
                    # the checkpoint folds and truncates the WAL so no old
                    # frame still holds the payload.
                    self._compact_db(sconn)
                    if local or tomb:
                        scrubbed.append(
                            {
                                "sandbox_id": sandbox["sandbox_id"],
                                "had_local_copy": True,
                                "tombstone_removed": bool(tomb),
                            }
                        )
                    else:
                        scrubbed.append(
                            {
                                "sandbox_id": sandbox["sandbox_id"],
                                "had_local_copy": False,
                            }
                        )
            except sqlite3.DatabaseError as exc:
                unreachable.append(
                    {
                        "sandbox_id": sandbox["sandbox_id"],
                        "status": sandbox["status"],
                        "why": f"无法打开沙箱数据库：{exc}",
                    }
                )
        return {
            "scrubbed": scrubbed,
            "unreachable": unreachable,
            "cache_entries_flushed": cache_entries,
        }

    def _revoke_exports_for_row(self, data_conn, row_id: str) -> list[dict]:
        revoked = []
        rows = data_conn.execute(
            "SELECT * FROM exports WHERE status = 'valid'"
        ).fetchall()
        for row in rows:
            ids = _json(row["row_ids"], []) or []
            if row_id not in ids:
                continue
            path = Path(row["path"])
            if path.exists():
                path.unlink()
            with transaction(data_conn):
                data_conn.execute(
                    "UPDATE exports SET status = 'revoked', revoked_at = ?, "
                    "revoke_reason = ? WHERE export_id = ?",
                    (now_iso(), "源记录被要求删除", row["export_id"]),
                )
            revoked.append(
                {
                    "export_id": row["export_id"],
                    "sandbox_id": row["sandbox_id"],
                    "file_removed": True,
                    "row_count": row["row_count"],
                }
            )
        return revoked

    def recover(self) -> dict:
        """Finish any erasure that crashed between tombstone and scrub.

        The tombstone is the authority, so recovery is safe and convergent:
        every erased row is re-scrubbed.  It cannot resurrect deleted rows
        because nothing here can remove a tombstone.
        """
        with self._erasure_lock, self.data() as data_conn:
            pending = data_conn.execute(
                "SELECT row_id, reason, payload_digest, purge_detail, erased_at "
                "FROM erased_rows"
            ).fetchall()
            unfinished = [
                r
                for r in pending
                if not (_json(r["purge_detail"], {}) or {}).get("completed")
            ]
            reports = []
            for row in unfinished:
                scrub = self._scrub_sandboxes(data_conn, row["row_id"])
                exports = self._revoke_exports_for_row(data_conn, row["row_id"])
                replacement = {
                    "row_id": row["row_id"],
                    "reason": row["reason"],
                    "payload_digest": row["payload_digest"],
                    "erased_at": row["erased_at"],
                    "recovered": True,
                    "sandboxes_scrubbed": scrub["scrubbed"],
                    "sandboxes_unreachable": scrub["unreachable"],
                    "cache_entries_flushed": scrub["cache_entries_flushed"],
                    "exports_revoked": exports,
                    "segments_rewritten": self._rewrite_segments_for_erasure(
                        data_conn, row["row_id"]
                    ),
                    "completed": True,
                }
                with transaction(data_conn):
                    data_conn.execute(
                        "DELETE FROM segment_rows WHERE row_id = ?", (row["row_id"],)
                    )
                    replacement["operation_logs_redacted"] = (
                        self._scrub_operation_details(data_conn, row["row_id"])
                    )
                    data_conn.execute(
                        "UPDATE erased_rows SET purge_detail = ? WHERE row_id = ?",
                        (canonical_json(replacement), row["row_id"]),
                    )
                reports.append(replacement)
            removed_destroyed = self._remove_destroyed_files(data_conn)
            # Any sandbox with a stale cache epoch is flushed even if its
            # erased rows were already handled.
            with transaction(data_conn):
                op_id = self._op_start(data_conn, "recover", None)
                self._op_finish(
                    data_conn,
                    op_id,
                    "ok",
                    {
                        "unfinished": len(unfinished),
                        "reports": reports,
                        "removed_destroyed_files": removed_destroyed,
                    },
                )
            if unfinished:
                self._compact_db(data_conn)
            return {
                "recovered": len(unfinished),
                "reports": reports,
                "removed_destroyed_files": removed_destroyed,
            }

    # -------------------------------------------------------------- reporting

    def erasure_report(self, row_id: str) -> dict:
        with self.data() as data_conn:
            row = data_conn.execute(
                "SELECT * FROM erased_rows WHERE row_id = ?", (row_id,)
            ).fetchone()
            if row is None:
                raise StoreError(404, "row_not_erased", "该记录没有被删除。")
            exposures = data_conn.execute(
                "SELECT sandbox_id, first_seen_at, last_seen_at, source "
                "FROM exposure_log WHERE row_id = ? ORDER BY sandbox_id",
                (row_id,),
            ).fetchall()
            exports = data_conn.execute(
                "SELECT export_id, sandbox_id, status, revoked_at, revoke_reason "
                "FROM exports WHERE row_ids LIKE ?",
                (f'%"{row_id}"%',),
            ).fetchall()
            remaining = []
            for sb in data_conn.execute(
                "SELECT sandbox_id, status FROM sandboxes"
            ).fetchall():
                path = self.sandbox_path(sb["sandbox_id"])
                if not path.exists():
                    remaining.append(
                        {"sandbox_id": sb["sandbox_id"], "state": "文件已不在"}
                    )
                    continue
                with self.sandbox(sb["sandbox_id"]) as sconn:
                    hits = sconn.execute(
                        "SELECT (SELECT COUNT(*) FROM local_rows WHERE row_id = ?) AS l, "
                        "(SELECT COUNT(*) FROM local_tombstones WHERE row_id = ?) AS t, "
                        "(SELECT COUNT(*) FROM row_history WHERE row_id = ? AND "
                        "(before_state IS NOT NULL OR after_state IS NOT NULL)) AS h, "
                        "(SELECT COUNT(*) FROM conflicts WHERE row_id = ? AND "
                        "(upstream_payload IS NOT NULL OR local_payload IS NOT NULL)) AS c",
                        (row_id, row_id, row_id, row_id),
                    ).fetchone()
                    # Compacted baseline states are aggregate JSON, so a plain
                    # SQL count cannot see them; check the decoded objects.
                    baseline_copies = sum(
                        1
                        for snap in sconn.execute(
                            "SELECT state FROM history_baseline"
                        ).fetchall()
                        if row_id in (_json(snap["state"], {}) or {})
                    )
                    # The cache is flushed wholesale on erasure, but a stale
                    # entry that somehow survived is a still-queryable copy.
                    # Match on the cached rows themselves, not on a raw text
                    # search: ``erased_hidden`` legitimately lists the id as an
                    # audit marker and must not count as a payload copy.
                    cached_copies = sum(
                        1
                        for entry in sconn.execute(
                            "SELECT body FROM query_cache"
                        ).fetchall()
                        if self._cache_has_row(entry["body"], row_id)
                    )
                    if (
                        hits["l"]
                        or hits["t"]
                        or hits["h"]
                        or hits["c"]
                        or baseline_copies
                        or cached_copies
                    ):
                        remaining.append(
                            {
                                "sandbox_id": sb["sandbox_id"],
                                "state": "仍有残留",
                                "local_rows": hits["l"],
                                "tombstones": hits["t"],
                                "history_payloads": hits["h"],
                                "conflict_payloads": hits["c"],
                                "baseline_copies": baseline_copies,
                                "cache_copies": cached_copies,
                            }
                        )
            live_segments = [
                dict(r)
                for r in data_conn.execute(
                    "SELECT s.segment_id FROM segments s WHERE EXISTS ("
                    "SELECT 1 FROM segment_rows sr WHERE sr.segment_id = s.segment_id "
                    "AND sr.row_id = ?)",
                    (row_id,),
                ).fetchall()
            ]
            for segment in live_segments:
                path = self.segment_path(segment["segment_id"])
                resolved = self._resolve_segment(data_conn, segment["segment_id"])
                rows = self._segment_payload(data_conn, segment["segment_id"])
                segment["resolved"] = resolved
                segment["file_present"] = path.exists()
                segment["contains_row"] = bool(
                    rows and any(r["row_id"] == row_id for r in rows)
                )
            live_exports = [
                dict(r) for r in exports if r["status"] == "valid"
            ]
            return {
                "row_id": row_id,
                "erased_at": row["erased_at"],
                "reason": row["reason"],
                "payload_digest": row["payload_digest"],
                "purge": _json(row["purge_detail"], {}),
                "exposures": [dict(r) for r in exposures],
                "exports": [dict(r) for r in exports],
                "sandbox_residue": remaining,
                "segment_residue": live_segments,
                # "clean" means no still-queryable copy exists.  A sandbox
                # whose file is already gone is reported but is not residue;
                # a still-valid export is, because it is downloadable.
                "clean": not any(
                    item.get("state") == "仍有残留" for item in remaining
                )
                and not live_exports
                and not any(s["contains_row"] for s in live_segments),
            }

    # -------------------------------------------------------------- previews

    def list_operations(self, limit: int = 30, sandbox_id: str | None = None) -> list[dict]:
        limit = max(1, min(int(limit or 30), 200))
        with self.data() as data_conn:
            if sandbox_id:
                rows = data_conn.execute(
                    "SELECT * FROM operations WHERE sandbox_id = ? "
                    "ORDER BY op_id DESC LIMIT ?",
                    (sandbox_id, limit),
                ).fetchall()
            else:
                rows = data_conn.execute(
                    "SELECT * FROM operations ORDER BY op_id DESC LIMIT ?", (limit,)
                ).fetchall()
            return [{**dict(r), "detail": _json(r["detail"], {})} for r in rows]

    def preview_refresh(self, sandbox_id: str) -> dict:
        """What refresh would import and where it would collide, without
        staging anything.  This is what makes the trade-off inspectable before
        the user commits."""
        with self.data() as data_conn:
            self._catalog(data_conn, sandbox_id)
            candidate_id = self._effective_segment(data_conn)
            erased = self._erased_ids(data_conn)
            candidate_rows = {
                r["row_id"]: r
                for r in (self._segment_payload(data_conn, candidate_id) or [])
                if r["row_id"] not in erased
            }
            with self.sandbox(sandbox_id) as sconn:
                base_id = sconn.execute(
                    "SELECT segment_id FROM base_ref"
                ).fetchone()["segment_id"]
                base_rows = {
                    r["row_id"]: r
                    for r in (self._segment_payload(data_conn, base_id) or [])
                    if r["row_id"] not in erased
                }
                overlay = self._overlay_state(sconn)
                conflicts = []
                for row_id, entry in overlay.items():
                    if row_id in erased:
                        continue
                    if self._conflict_candidate(
                        base_rows.get(row_id), candidate_rows.get(row_id), entry
                    ):
                        conflicts.append(
                            {
                                "row_id": row_id,
                                "local_tombstone": entry["kind"] == "tombstone",
                                "upstream_payload": self._row_slice(
                                    candidate_rows.get(row_id)
                                ),
                                "local_payload": self._row_slice(entry.get("payload")),
                            }
                        )
                stats = self._refresh_stats(base_rows, candidate_rows)
                changes = []
                for row_id in sorted(set(base_rows) | set(candidate_rows)):
                    before = base_rows.get(row_id)
                    after = candidate_rows.get(row_id)
                    if self._row_slice(before) == self._row_slice(after):
                        continue
                    changes.append(
                        {
                            "row_id": row_id,
                            "before": self._row_slice(before),
                            "after": self._row_slice(after),
                            "locally_overlaid": row_id in overlay,
                        }
                    )
                resolved_base = self._resolve_segment(data_conn, base_id)
                return {
                    "sandbox_id": sandbox_id,
                    "current_segment": resolved_base,
                    "candidate_segment": candidate_id,
                    "up_to_date": resolved_base == candidate_id,
                    "stats": stats,
                    "conflicts": conflicts,
                    "changes": changes[:20],
                    "changes_total": len(changes),
                }

    def preview_erase(self, row_id: str) -> dict:
        """Show every place a row currently lives before erasing it."""
        with self.data() as data_conn:
            in_upstream = (
                data_conn.execute(
                    'SELECT 1 FROM "rows" WHERE row_id = ?', (row_id,)
                ).fetchone()
                is not None
            )
            segment_ids = {
                r["segment_id"]
                for r in data_conn.execute(
                    "SELECT DISTINCT segment_id FROM segment_rows WHERE row_id = ?",
                    (row_id,),
                ).fetchall()
            }
            affected = []
            for sb in data_conn.execute(
                "SELECT sandbox_id, label, status FROM sandboxes"
            ).fetchall():
                path = self.sandbox_path(sb["sandbox_id"])
                if not path.exists():
                    continue
                try:
                    with connect(path) as sconn:
                        base = sconn.execute(
                            "SELECT segment_id FROM base_ref"
                        ).fetchone()
                        local = sconn.execute(
                            "SELECT 1 FROM local_rows WHERE row_id = ?", (row_id,)
                        ).fetchone()
                        tomb = sconn.execute(
                            "SELECT 1 FROM local_tombstones WHERE row_id = ?", (row_id,)
                        ).fetchone()
                        cache = sconn.execute(
                            "SELECT COUNT(*) AS n FROM query_cache WHERE body LIKE ?",
                            (f'%"{row_id}"%',),
                        ).fetchone()["n"]
                except sqlite3.DatabaseError:
                    continue
                resolved = (
                    self._resolve_segment(data_conn, base["segment_id"]) if base else None
                )
                visible = resolved in segment_ids or (local and not tomb)
                if visible or cache:
                    affected.append(
                        {
                            "sandbox_id": sb["sandbox_id"],
                            "label": sb["label"],
                            "status": sb["status"],
                            "via_segment": resolved in segment_ids,
                            "has_local_copy": bool(local),
                            "locally_deleted": bool(tomb),
                            "cache_entries": cache,
                        }
                    )
            exports = []
            for row in data_conn.execute(
                "SELECT export_id, sandbox_id, status, path FROM exports "
                "WHERE row_ids LIKE ?",
                (f'%"{row_id}"%',),
            ).fetchall():
                exports.append(
                    {
                        "export_id": row["export_id"],
                        "sandbox_id": row["sandbox_id"],
                        "status": row["status"],
                        "file_present": Path(row["path"]).exists(),
                    }
                )
            return {
                "row_id": row_id,
                "in_upstream": in_upstream,
                "segments": len(segment_ids),
                "affected_sandboxes": affected,
                "exports": exports,
                "already_erased": data_conn.execute(
                    "SELECT 1 FROM erased_rows WHERE row_id = ?", (row_id,)
                ).fetchone()
                is not None,
            }
