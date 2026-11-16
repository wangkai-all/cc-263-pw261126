"""SQLite schema and connection helpers for SandboxLab.

Design in one paragraph
-----------------------
The upstream dataset is a real SQLite database (:file:`data.db`).  A sandbox is
*also* a real database file (:file:`sandboxes/<id>.sqlite3`) but it does not
copy the rows: at creation it points at an immutable, content-addressed,
gzip-compressed JSON segment stored under :file:`segments/`.  Writes made
inside a sandbox land in a local overlay composed of ``local_rows`` (upserted
rows), ``local_tombstones`` (deleted row ids), ``local_meta`` (column labels /
visibility), ``row_history`` (bounded undo log) and ``conflicts`` (refresh
collisions awaiting a human decision).  Reads are a *merge* of the local layer
over the frozen shared base.

Why this shape: it is the "shared blocks + overlay" model the task asks to
choose over a full copy.  Full copies are trivial but duplicate every byte per
sandbox and make erasure a game of whack-a-mole across N files.  With one
content-addressed base, erasure touches one authority (``erased_rows`` plus a
rewrite chain) and the bookkeeping that decides what can still be seen.

History and erasure deliberately conflict.  History is bounded (``seq`` based,
compacted into ``history_baseline``); erasure is retroactive and always wins.
No promise of unlimited as-of reads is made anywhere in this codebase.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 4

DATA_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS upstream_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "rows" (
    row_id            TEXT PRIMARY KEY,
    name              TEXT NOT NULL DEFAULT '',
    email             TEXT NOT NULL DEFAULT '',
    region            TEXT NOT NULL DEFAULT '',
    amount_cents      INTEGER NOT NULL DEFAULT 0,
    notes             TEXT NOT NULL DEFAULT '',
    source_updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS rows_name_idx ON "rows"(name);

-- Rows whose source record has been erased.  This is the single authority for
-- "the payload must not be visible anywhere".  The payload is gone (removed
-- from "rows" and rewritten out of every segment); only an id tombstone, a
-- digest and an audit trail remain so the id can never be resurrected by a
-- refresh, clone or failed restore.
CREATE TABLE IF NOT EXISTS erased_rows (
    row_id         TEXT PRIMARY KEY,
    erased_at      TEXT NOT NULL,
    reason         TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    purge_detail   TEXT NOT NULL DEFAULT '{}'
);

-- Content-addressed, immutable shared blocks.  A segment is a gzip JSON array
-- of row objects at segments/<segment_id>.json.gz, where segment_id is the
-- SHA-256 of the canonical row array.  Segments are never mutated: erasure
-- publishes a rewritten replacement and links the old id through replaced_by.
-- Readers resolve the chain, so every sandbox sees the erasure without every
-- sandbox file being touched.  The replaced file is unlinked; pruned_at
-- records that the bytes are physically gone.
CREATE TABLE IF NOT EXISTS segments (
    segment_id      TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    sha256          TEXT NOT NULL,
    bytes           INTEGER NOT NULL,
    raw_bytes       INTEGER NOT NULL DEFAULT 0,
    source_revision INTEGER NOT NULL DEFAULT 0,
    replaced_by     TEXT,
    pruned_at       TEXT
);

-- Index of segment contents.  Deliberately not foreign-keyed to "rows": a
-- source row may leave upstream while pinned segments still contain it, and
-- such a row must remain erasable.
CREATE TABLE IF NOT EXISTS segment_rows (
    segment_id TEXT NOT NULL,
    row_id     TEXT NOT NULL,
    PRIMARY KEY (segment_id, row_id)
);
CREATE INDEX IF NOT EXISTS segment_rows_row_idx ON segment_rows(row_id);

CREATE TABLE IF NOT EXISTS sandboxes (
    sandbox_id       TEXT PRIMARY KEY,
    label            TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'destroyed')),
    destroyed_at     TEXT,
    destroyed_reason TEXT,
    origin_segment_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operations (
    op_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    sandbox_id  TEXT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'ok', 'failed', 'skipped', 'conflict')),
    detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS operations_sandbox_idx
    ON operations(sandbox_id, op_id);

-- A materialised read cache.  Exports are first-class because erasure has to
-- revoke them and delete their bytes, not merely hide them in the UI.
CREATE TABLE IF NOT EXISTS exports (
    export_id     TEXT PRIMARY KEY,
    sandbox_id    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'valid'
                  CHECK (status IN ('valid', 'revoked')),
    revoked_at    TEXT,
    revoke_reason TEXT,
    path          TEXT NOT NULL,
    row_ids       TEXT NOT NULL,
    row_count     INTEGER NOT NULL DEFAULT 0,
    filters       TEXT NOT NULL DEFAULT '{}'
);

-- Which sandbox has actually materialised which row (query or export).  Used
-- by the erasure report to name every place that could have seen the record.
CREATE TABLE IF NOT EXISTS exposure_log (
    sandbox_id    TEXT NOT NULL,
    row_id        TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    source        TEXT NOT NULL,
    PRIMARY KEY (sandbox_id, row_id)
);
CREATE INDEX IF NOT EXISTS exposure_row_idx ON exposure_log(row_id);
"""

SANDBOX_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sandbox_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- The reference half of the clone: payload is shared, not copied.
CREATE TABLE IF NOT EXISTS base_ref (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    segment_id       TEXT NOT NULL,
    data_db          TEXT NOT NULL,
    pinned_row_count INTEGER NOT NULL,
    pinned_at        TEXT NOT NULL
);

-- Every base this sandbox has pinned.  Refresh appends; as-of reads pick the
-- newest entry with seq <= as_of.
CREATE TABLE IF NOT EXISTS base_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    seq        INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    note       TEXT
);
CREATE INDEX IF NOT EXISTS base_history_seq_idx ON base_history(seq);

-- The overlay half: only rows this sandbox changed.  A row is a full JSON
-- object so deletes/restores remain cheap and readable.
CREATE TABLE IF NOT EXISTS local_rows (
    row_id     TEXT PRIMARY KEY,
    payload    TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- ``payload`` keeps the overlay value that a delete discarded, so restore can
-- bring back the sandbox's own edit rather than silently reverting to base.
CREATE TABLE IF NOT EXISTS local_tombstones (
    row_id     TEXT PRIMARY KEY,
    deleted_at TEXT NOT NULL,
    payload    TEXT
);

CREATE TABLE IF NOT EXISTS local_meta (
    column_name TEXT PRIMARY KEY,
    visible     INTEGER NOT NULL DEFAULT 1,
    label       TEXT
);

-- Mirror of the primary DB's erasure tombstone.  Reads consult it so a
-- sandbox overlay that failed to scrub (e.g. a crash mid-erasure) still
-- cannot show the payload.
CREATE TABLE IF NOT EXISTS erased_rows (
    row_id    TEXT PRIMARY KEY,
    erased_at TEXT NOT NULL,
    reason    TEXT NOT NULL
);

-- A refresh that stopped on conflicts.  The candidate base is kept until every
-- conflict is resolved explicitly; a crash before the swap leaves no pending
-- row at all (the whole detection runs in one transaction).
CREATE TABLE IF NOT EXISTS pending_refresh (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    seq             INTEGER NOT NULL,
    segment_id      TEXT NOT NULL,
    source_revision INTEGER NOT NULL,
    detected_at     TEXT NOT NULL,
    op_id           INTEGER
);

-- Bounded undo log enabling "as of sequence" reads.  ``before_state`` and
-- ``after_state`` are NULL (base-only) or JSON {"local": {...}} /
-- {"tombstone": {...}}.  Payloads are redacted when the source row is erased;
-- the edit remains auditable.
CREATE TABLE IF NOT EXISTS row_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    seq          INTEGER NOT NULL,
    row_id       TEXT NOT NULL,
    action       TEXT NOT NULL
                 CHECK (action IN ('write', 'delete', 'restore', 'take_upstream')),
    before_state TEXT,
    after_state  TEXT,
    redacted     INTEGER NOT NULL DEFAULT 0,
    erased       INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS row_history_seq_idx ON row_history(seq, id);
CREATE INDEX IF NOT EXISTS row_history_row_idx ON row_history(row_id, seq);

-- Compaction baseline: composed overlay state at the oldest answerable seq.
-- Older history rows are deleted, so "as of" below the horizon is refused
-- rather than silently answered with a wrong state.
CREATE TABLE IF NOT EXISTS history_baseline (
    seq        INTEGER PRIMARY KEY,
    state      TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Divergence detected at refresh, awaiting an explicit user decision.
CREATE TABLE IF NOT EXISTS conflicts (
    row_id           TEXT PRIMARY KEY,
    detected_seq     INTEGER NOT NULL,
    detected_at      TEXT NOT NULL,
    upstream_payload TEXT,
    local_payload    TEXT,
    local_tombstone  INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open', 'kept', 'taken', 'obsolete')),
    resolved_at      TEXT,
    resolution       TEXT
);

-- Read-through result cache.  It is a real persisted copy of row payloads, so
-- erasure must invalidate it across every sandbox.  ``epoch`` is compared with
-- the primary DB's erasure_epoch; stale entries are deleted before use.
CREATE TABLE IF NOT EXISTS query_cache (
    cache_key  TEXT PRIMARY KEY,
    request    TEXT NOT NULL,
    body       TEXT NOT NULL,
    epoch      INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
"""


def now_iso() -> str:
    """UTC timestamp with millisecond precision, fixed width for sorting."""
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + (
        ".%03dZ" % int((t % 1) * 1000)
    )


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def connect(path: str | os.PathLike, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a connection tuned for auditability.

    ``secure_delete`` overwrites deleted cell content with zeros instead of
    leaving it in free pages, which is what makes "erasure" verifiable rather
    than merely logical.
    """
    if readonly:
        uri = f"file:{Path(path).as_posix()}?mode=ro"
        conn = sqlite3.connect(
            uri, uri=True, timeout=30, isolation_level=None, check_same_thread=False
        )
    else:
        conn = sqlite3.connect(
            str(path), timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA secure_delete = ON")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """IMMEDIATE transaction: fail fast instead of silently losing a write race."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextlib.contextmanager
def closing(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
    finally:
        conn.close()


def init_data_db(path: str | os.PathLike, *, fresh: bool = False) -> sqlite3.Connection:
    """Create the primary data database (upstream + catalog)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fresh and path.exists():
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                Path(str(path) + suffix).unlink()
    conn = connect(path)
    conn.executescript(DATA_SCHEMA)
    defaults = {
        "schema_version": str(SCHEMA_VERSION),
        "source_revision": "0",
        "current_segment_id": "",
        "seeded": "0",
        "erasure_epoch": "0",
        "history_retention": "25",
    }
    for key, value in defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO upstream_meta(key, value) VALUES(?, ?)",
            (key, value),
        )
    return conn


def init_sandbox_db(path: str | os.PathLike, sandbox_id: str) -> sqlite3.Connection:
    """A sandbox is a real database too, but holds only reference + local layer."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    conn.executescript(SANDBOX_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO sandbox_info(key, value) VALUES('sandbox_id', ?)",
        (sandbox_id,),
    )
    conn.execute(
        "INSERT OR IGNORE INTO sandbox_info(key, value) VALUES('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.execute(
        "INSERT OR IGNORE INTO sandbox_info(key, value) VALUES('seq', '0')"
    )
    return conn
