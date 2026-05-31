from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(records: Iterable[dict], path: str | Path) -> int:
    p = Path(path)
    ensure_dir(p.parent)
    count = 0
    with p.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str) + "\n")
            count += 1
    return count


def read_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    rows = []
    with p.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append({"_malformed_record": line})
    return rows


def write_table(df: pd.DataFrame, path: str | Path) -> None:
    p = ensure_dir(path)
    file_path = p / "part-000.parquet"
    df.to_parquet(file_path, index=False)


def read_table(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    files = sorted(p.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(file) for file in files), ignore_index=True)
