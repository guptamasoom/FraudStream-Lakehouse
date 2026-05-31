from __future__ import annotations

from pathlib import Path

import pandas as pd

from fraudstream.pipeline.utils.io import read_table, write_table


def _load(root: str, table: str) -> pd.DataFrame:
    return read_table(Path(root) / table)


def build_daily_fraud_summary(silver_root: str) -> pd.DataFrame:
    tx = _load(silver_root, "scored_transactions")
    if tx.empty:
        return pd.DataFrame(columns=["event_date", "transactions", "total_amount", "high_risk_transactions", "fraud_rate"])
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
    grouped = tx.groupby("event_date", dropna=False).agg(
        transactions=("transaction_id", "nunique"),
        total_amount=("amount", "sum"),
        high_risk_transactions=("risk_band", lambda s: s.eq("HIGH").sum()),
        avg_risk_score=("risk_score", "mean"),
    ).reset_index()
    grouped["fraud_rate"] = grouped["high_risk_transactions"] / grouped["transactions"].replace(0, pd.NA)
    return grouped


def build_high_risk_transactions(silver_root: str) -> pd.DataFrame:
    tx = _load(silver_root, "scored_transactions")
    if tx.empty:
        return pd.DataFrame()
    return tx[tx["risk_band"].eq("HIGH")].sort_values("risk_score", ascending=False)


def build_customer_risk_profile(silver_root: str) -> pd.DataFrame:
    tx = _load(silver_root, "scored_transactions")
    if tx.empty:
        return pd.DataFrame(columns=["customer_id", "transactions", "total_amount", "max_risk_score", "high_risk_transactions"])
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
    return tx.groupby("customer_id", dropna=False).agg(
        transactions=("transaction_id", "nunique"),
        total_amount=("amount", "sum"),
        avg_risk_score=("risk_score", "mean"),
        max_risk_score=("risk_score", "max"),
        high_risk_transactions=("risk_band", lambda s: s.eq("HIGH").sum()),
        countries=("ip_country", "nunique"),
        devices=("device_id", "nunique"),
    ).reset_index()


def build_merchant_risk_profile(silver_root: str) -> pd.DataFrame:
    tx = _load(silver_root, "scored_transactions")
    if tx.empty:
        return pd.DataFrame(columns=["merchant_id", "transactions", "total_amount", "avg_risk_score"])
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
    return tx.groupby(["merchant_id", "merchant_category"], dropna=False).agg(
        transactions=("transaction_id", "nunique"),
        total_amount=("amount", "sum"),
        avg_risk_score=("risk_score", "mean"),
        high_risk_transactions=("risk_band", lambda s: s.eq("HIGH").sum()),
    ).reset_index()


def build_fraud_rule_performance(silver_root: str) -> pd.DataFrame:
    tx = _load(silver_root, "scored_transactions")
    if tx.empty:
        return pd.DataFrame(columns=["rule_name", "triggered_count", "known_fraud_count"])
    rule_cols = [col for col in tx.columns if col.startswith("rule_")]
    rows = []
    for col in rule_cols:
        triggered = tx[tx[col].astype(bool)]
        rows.append(
            {
                "rule_name": col.replace("rule_", ""),
                "triggered_count": len(triggered),
                "known_fraud_count": int(triggered.get("is_known_fraud", pd.Series(dtype=bool)).fillna(False).sum()),
                "avg_risk_score": float(triggered["risk_score"].mean()) if len(triggered) else 0,
            }
        )
    return pd.DataFrame(rows)


def build_realtime_alerts(silver_root: str) -> pd.DataFrame:
    high = build_high_risk_transactions(silver_root)
    if high.empty:
        return pd.DataFrame()
    cols = [
        "event_timestamp",
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount",
        "risk_score",
        "risk_band",
        "fraud_reason_codes",
    ]
    return high[[col for col in cols if col in high.columns]].head(100)


def build_data_quality_scorecard(silver_root: str) -> pd.DataFrame:
    root = Path(silver_root) / "_dq_results"
    frames = [read_table(path) for path in root.iterdir() if path.is_dir()] if root.exists() else []
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_all(silver_root: str = "data/silver", gold_root: str = "data/gold") -> dict[str, int]:
    builders = {
        "gold_daily_fraud_summary": build_daily_fraud_summary,
        "gold_high_risk_transactions": build_high_risk_transactions,
        "gold_customer_risk_profile": build_customer_risk_profile,
        "gold_merchant_risk_profile": build_merchant_risk_profile,
        "gold_fraud_rule_performance": build_fraud_rule_performance,
        "gold_realtime_alerts": build_realtime_alerts,
        "gold_data_quality_scorecard": build_data_quality_scorecard,
    }
    counts = {}
    for name, builder in builders.items():
        df = builder(silver_root)
        write_table(df, Path(gold_root) / name)
        counts[name] = len(df)
    return counts
