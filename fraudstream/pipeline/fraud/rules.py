from __future__ import annotations

import pandas as pd


def _as_bool(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def add_velocity_features(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return transactions
    df = transactions.copy()
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)
    df = df.sort_values(["customer_id", "event_timestamp"])
    df["customer_txn_count"] = df.groupby("customer_id")["transaction_id"].transform("count")
    df["customer_total_amount"] = df.groupby("customer_id")["amount"].transform("sum")
    df["merchant_txn_count"] = df.groupby("merchant_id")["transaction_id"].transform("count")
    df["device_customer_count"] = df.groupby("device_id")["customer_id"].transform("nunique")
    return df


def score_transactions(transactions: pd.DataFrame, logins: pd.DataFrame | None = None, devices: pd.DataFrame | None = None) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame()
    df = add_velocity_features(transactions)
    if devices is not None and not devices.empty and {"device_id", "is_new_device"}.issubset(devices.columns):
        device_flags = devices.groupby("device_id", as_index=False).agg(is_new_device=("is_new_device", "max"))
        df = df.merge(device_flags, on="device_id", how="left", suffixes=("", "_device"))
    else:
        df["is_new_device"] = False

    if logins is not None and not logins.empty and {"customer_id", "event_type"}.issubset(logins.columns):
        failed = logins.assign(failed_login=logins["event_type"].eq("login_failed")).groupby("customer_id", as_index=False).agg(
            failed_login_count=("failed_login", "sum")
        )
        df = df.merge(failed, on="customer_id", how="left")
    else:
        df["failed_login_count"] = 0
    df["failed_login_count"] = df["failed_login_count"].fillna(0)

    df["rule_high_amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).ge(600)
    df["rule_country_mismatch"] = df["ip_country"].ne(df["billing_country"]) | df["shipping_country"].ne(df["billing_country"])
    is_new_device = df["is_new_device"].map(_as_bool)
    df["rule_new_device_high_amount"] = is_new_device & df["rule_high_amount"]
    df["rule_high_velocity"] = pd.to_numeric(df["customer_txn_count"], errors="coerce").fillna(0).ge(4)
    df["rule_many_customers_on_device"] = pd.to_numeric(df["device_customer_count"], errors="coerce").fillna(0).ge(3)
    df["rule_failed_login_before_purchase"] = pd.to_numeric(df["failed_login_count"], errors="coerce").fillna(0).ge(2)
    df["rule_card_not_present_risk"] = df["payment_method"].eq("card_not_present") & df["rule_country_mismatch"]

    weights = {
        "rule_high_amount": 20,
        "rule_country_mismatch": 20,
        "rule_new_device_high_amount": 20,
        "rule_high_velocity": 15,
        "rule_many_customers_on_device": 10,
        "rule_failed_login_before_purchase": 10,
        "rule_card_not_present_risk": 15,
    }
    df["risk_score"] = sum(df[col].astype(int) * weight for col, weight in weights.items()).clip(upper=100)
    df["risk_band"] = pd.cut(
        df["risk_score"],
        bins=[-1, 39, 69, 100],
        labels=["LOW", "MEDIUM", "HIGH"],
    ).astype("string")
    df["fraud_reason_codes"] = df.apply(
        lambda row: ",".join([col.replace("rule_", "").upper() for col in weights if bool(row[col])]),
        axis=1,
    )
    return df
