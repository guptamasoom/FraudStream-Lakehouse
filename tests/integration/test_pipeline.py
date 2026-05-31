from pathlib import Path

from fraudstream.pipeline.bronze.ingest import ingest_all
from fraudstream.pipeline.gold.aggregate import build_all
from fraudstream.pipeline.silver.transform import transform_all
from fraudstream.pipeline.utils.io import read_table, write_jsonl
from fraudstream.producers.transaction_simulator import build_event_batch


def test_offline_medallion_pipeline(tmp_path):
    landing = tmp_path / "landing"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    quarantine = tmp_path / "quarantine"

    events = build_event_batch(fraud_rate=100, bad_data_percentage=0)
    for topic, rows in events.items():
        write_jsonl(rows, landing / topic / "processing_date=2026-06-01" / "events.jsonl")

    bronze_counts = ingest_all(str(landing), str(bronze))
    silver_counts = transform_all(str(bronze), str(silver), str(quarantine))
    gold_counts = build_all(str(silver), str(gold))

    assert bronze_counts["fraud_transactions"] >= 1
    assert silver_counts["scored_transactions"] >= 1
    assert gold_counts["gold_daily_fraud_summary"] >= 1
    assert not read_table(Path(gold) / "gold_realtime_alerts").empty
