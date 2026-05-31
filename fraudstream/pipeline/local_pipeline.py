from __future__ import annotations

import argparse

from fraudstream.pipeline.bronze.ingest import ingest_all
from fraudstream.pipeline.gold.aggregate import build_all
from fraudstream.pipeline.silver.transform import transform_all


def run_local_pipeline(
    landing: str = "data/landing",
    bronze: str = "data/bronze",
    silver: str = "data/silver",
    gold: str = "data/gold",
    quarantine: str = "data/quarantine",
) -> dict:
    return {
        "bronze": ingest_all(landing, bronze),
        "silver": transform_all(bronze, silver, quarantine),
        "gold": build_all(silver, gold),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local Bronze, Silver, and Gold FraudStream pipeline.")
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--bronze", default="data/bronze")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--gold", default="data/gold")
    parser.add_argument("--quarantine", default="data/quarantine")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(run_local_pipeline(args.landing, args.bronze, args.silver, args.gold, args.quarantine))
