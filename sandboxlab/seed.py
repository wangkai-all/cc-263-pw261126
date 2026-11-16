"""Fictional source data and the single click that changes it upstream.

Everything here is invented.  ``example`` is a reserved TLD, so every address is
guaranteed unroutable.  The "upstream" is a real SQLite table; editing it is a
real write that refresh has to reconcile with sandbox overlays.
"""

from __future__ import annotations

import random

REGIONS = ["华东", "华南", "华北", "西南", "华中", "东北"]
NOTES = ["年度续约", "试点客户", "渠道推荐", "待回访", "合规复核", "价格敏感", "新签"]
PREFIXES = [
    ("云杉", "yunshan"),
    ("北辰", "beichen"),
    ("岚桥", "lanqiao"),
    ("青禾", "qinghe"),
    ("星野", "xingye"),
    ("白鲸", "baijing"),
    ("远帆", "yuanfan"),
    ("木棉", "mumian"),
    ("澄川", "chengchuan"),
    ("山隅", "shanyu"),
    ("鹿鸣", "luming"),
    ("栖湖", "qihu"),
    ("风与", "fengyu"),
    ("南屿", "nanyu"),
    ("花间", "huajian"),
    ("拾光", "shiguang"),
]
SUFFIXES = [
    ("物流", "logistics"),
    ("医疗", "health"),
    ("能源", "energy"),
    ("零售", "retail"),
    ("教育", "edu"),
    ("制造", "mfg"),
    ("数据", "data"),
    ("环保", "green"),
]
SURNAMES = ["陈", "林", "赵", "沈", "周", "许", "何", "苏", "顾", "唐", "叶", "孟"]
GIVEN = ["思远", "映真", "知秋", "牧之", "令仪", "书言", "望舒", "嘉树", "清和", "南乔"]

SEED_COUNT = 48
# acct-004 carries a retention obligation, so erasing it must be refused until
# an operator explicitly acknowledges the hold.  It exists to demonstrate that
# deletion is a policy decision, not just an UPDATE.
RECORD_HOLDS = {
    "acct-004": "合规调查保留 · 依据 A-17，禁止直接删除",
}


def build_seed_rows(count: int = SEED_COUNT) -> list[dict]:
    """Return ``count`` deterministic rows in stable row_id order."""
    if count > len(PREFIXES) * len(SUFFIXES):
        raise ValueError("seed only defines 128 distinct company names")
    rng = random.Random(20261116)
    rows: list[dict] = []
    for i in range(count):
        prefix, prefix_ascii = PREFIXES[i % len(PREFIXES)]
        suffix, suffix_ascii = SUFFIXES[(i // len(PREFIXES)) % len(SUFFIXES)]
        row_id = f"acct-{i + 1:03d}"
        notes = NOTES[(i * 3) % len(NOTES)]
        if row_id in RECORD_HOLDS:
            notes = NOTES[4]
        rows.append(
            {
                "row_id": row_id,
                "name": f"{prefix}{suffix}",
                "email": f"ops@{prefix_ascii}-{suffix_ascii}.example",
                "region": REGIONS[i % len(REGIONS)],
                "amount_cents": 80_000 + rng.randrange(0, 1_920_000, 100),
                "notes": notes,
                "source_updated_at": (
                    f"2026-09-{(i % 27) + 1:02d}T08:{i % 60:02d}:00.000Z"
                ),
            }
        )
    return rows


def _contact_name(index: int) -> str:
    return SURNAMES[index % len(SURNAMES)] + GIVEN[(index * 7) % len(GIVEN)]


class UpstreamSimulator:
    """Produces believable upstream churn on demand.

    Each call advances ``source_revision`` by one and returns a description of
    what changed, so the UI can show *what* a refresh would import.
    """

    def __init__(self, rng: random.Random | None = None):
        self.rng = rng or random.Random()

    def mutate(self, rows: list[dict], *, ops: int | None = None) -> list[dict]:
        """Mutate ``rows`` in place-ish and return a change log."""
        changes: list[dict] = []
        if not rows:
            return changes
        n_ops = ops if ops is not None else self.rng.randint(1, 4)
        next_index = max(
            (int(r["row_id"].split("-")[1]) for r in rows if r["row_id"].startswith("acct-")),
            default=0,
        )
        by_id = {r["row_id"]: r for r in rows}
        for _ in range(n_ops):
            kind = self.rng.choices(
                ["update", "update", "insert", "insert", "remove"], weights=[4, 4, 2, 2, 1]
            )[0]
            if kind == "insert" and next_index < 128:
                next_index += 1
                prefix, prefix_ascii = PREFIXES[next_index % len(PREFIXES)]
                suffix, suffix_ascii = SUFFIXES[(next_index // len(PREFIXES)) % len(SUFFIXES)]
                row_id = f"acct-{next_index:03d}"
                if row_id in by_id:
                    continue
                row = {
                    "row_id": row_id,
                    "name": f"{prefix}{suffix}",
                    "email": f"ops@{prefix_ascii}-{suffix_ascii}.example",
                    "region": REGIONS[next_index % len(REGIONS)],
                    "amount_cents": 120_000 + self.rng.randrange(0, 1_500_000, 100),
                    "notes": NOTES[next_index % len(NOTES)],
                    "source_updated_at": f"2026-11-{self.rng.randint(1, 28):02d}T09:00:00.000Z",
                }
                rows.append(row)
                by_id[row_id] = row
                changes.append({"op": "insert", "row_id": row_id, "name": row["name"]})
            elif kind == "remove" and len(rows) > 8:
                victim = self.rng.choice(rows)
                rows.remove(victim)
                by_id.pop(victim["row_id"], None)
                changes.append(
                    {"op": "remove", "row_id": victim["row_id"], "name": victim["name"]}
                )
            else:
                candidates = [r for r in rows if r["row_id"].startswith("acct-")]
                if not candidates:
                    continue
                row = self.rng.choice(candidates)
                before = int(row["amount_cents"])
                delta = self.rng.randrange(-60_000, 120_000, 100)
                row["amount_cents"] = max(10_000, before + delta)
                row["notes"] = NOTES[self.rng.randrange(len(NOTES))]
                row["source_updated_at"] = (
                    f"2026-11-{self.rng.randint(1, 28):02d}T10:30:00.000Z"
                )
                changes.append(
                    {
                        "op": "update",
                        "row_id": row["row_id"],
                        "name": row["name"],
                        "before_cents": before,
                        "after_cents": row["amount_cents"],
                    }
                )
        return changes
