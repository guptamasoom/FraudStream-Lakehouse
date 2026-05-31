from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fraudstream.pipeline.fraud.rules import score_transactions
from fraudstream.pipeline.quality.checks import fraud_checks
from fraudstream.pipeline.utils.io import read_table, write_table


def parse_bronze(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        try:
            payload = json.loads(row.get("raw_json") or "{}")
        except json.JSONDecodeError:
            payload = {"_malformed_record": row.get("raw_json")}
        payload.update(
            {
                "ingestion_timestamp": row.get("ingestion_timestamp"),
                "source_topic": row.get("source_topic"),
                "source_file": row.get("source_file"),
                "batch_id": row.get("batch_id"),
                "processing_date": row.get("processing_date"),
            }
        )
        rows.append(payload)
    return pd.DataFrame(rows)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "event_timestamp" in out:
        out["event_timestamp"] = pd.to_datetime(out["event_timestamp"], errors="coerce", utc=True)
        out["event_date"] = out["event_timestamp"].dt.date.astype("string")
        out["event_hour"] = out["event_timestamp"].dt.hour
    for column in ["amount", "chargeback_amount"]:
        if column in out:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ["is_card_present", "is_known_fraud", "mfa_passed", "is_new_device"]:
        if column in out:
            out[column] = out[column].fillna(False).astype(bool)
    if "event_id" in out and "event_timestamp" in out:
        out = out.sort_values("event_timestamp").drop_duplicates("event_id", keep="last")
    return out


def _read_topic_bronze(bronze_root: str, topic: str) -> pd.DataFrame:
    topic_path = Path(bronze_root) / topic
    parts = [read_table(partition) for partition in topic_path.glob("processing_date=*")] if topic_path.exists() else []
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def transform_topic(bronze_root: str, silver_root: str, quarantine_root: str, topic: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed = normalize(parse_bronze(_read_topic_bronze(bronze_root, topic)))
    runner = fraud_checks(topic, parsed)
    scorecard = runner.scorecard()
    quarantine = runner.quarantine()
    invalid_ids = set(quarantine["event_id"].dropna()) if "event_id" in quarantine else set()
    valid = parsed[~parsed["event_id"].isin(invalid_ids)].copy() if "event_id" in parsed else parsed
    write_table(valid, Path(silver_root) / topic)
    write_table(scorecard, Path(silver_root) / "_dq_results" / topic)
    if not quarantine.empty:
        write_table(quarantine, Path(quarantine_root) / "dq_failures" / topic)
    return valid, scorecard


def transform_all(bronze_root: str = "data/bronze", silver_root: str = "data/silver", quarantine_root: str = "data/quarantine") -> dict[str, int]:
    topics = [path.name for path in Path(bronze_root).iterdir() if path.is_dir()] if Path(bronze_root).exists() else []
    counts = {}
    for topic in topics:
        valid, _ = transform_topic(bronze_root, silver_root, quarantine_root, topic)
        counts[topic] = len(valid)

    transactions = read_table(Path(silver_root) / "fraud_transactions")
    logins = read_table(Path(silver_root) / "fraud_logins")
    devices = read_table(Path(silver_root) / "fraud_devices")
    scored = score_transactions(transactions, logins, devices)
    if not scored.empty:
        write_table(scored, Path(silver_root) / "scored_transactions")
        counts["scored_transactions"] = len(scored)
    return counts
