"""Typed errors surfaced to the HTTP layer."""

from __future__ import annotations


class StoreError(Exception):
    """An error with a stable machine code and a suggested HTTP status."""

    def __init__(self, status: int, code: str, message: str, details=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


class RefreshConflict(StoreError):
    """Refresh was blocked because local edits collide with upstream changes."""

    def __init__(self, result: dict):
        super().__init__(
            409,
            "refresh_conflicts",
            "刷新与本地改动冲突：未推进共享基底，请先明确选择保留本地或采用上游。",
            result,
        )
        self.result = result
