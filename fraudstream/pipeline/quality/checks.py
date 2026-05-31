from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass
class QualityCheck:
    name: str
    reason_code: str
    predicate: Callable[[pd.DataFrame], pd.Series]
    severity: str = "critical"


class QualityRunner:
    def __init__(self, topic: str, df: pd.DataFrame, checks: list[QualityCheck]) -> None:
        self.topic = topic
        self.df = df.copy()
        self.checks = checks

    def scorecard(self) -> pd.DataFrame:
        rows = []
        total = len(self.df)
        for check in self.checks:
            if total == 0:
                failed = 0
            else:
                failed = int((~check.predicate(self.df).fillna(False)).sum())
            passed = total - failed
            rows.append(
                {
                    "source_topic": self.topic,
                    "check_name": check.name,
                    "reason_code": check.reason_code,
                    "severity": check.severity,
                    "total_record_count": total,
                    "failed_record_count": failed,
                    "passed_record_count": passed,
                    "pass_percentage": 1.0 if total == 0 else passed / total,
                    "dq_status": "pass" if failed == 0 else "fail",
                }
            )
        return pd.DataFrame(rows)

    def quarantine(self) -> pd.DataFrame:
        frames = []
        for check in self.checks:
            mask = ~check.predicate(self.df).fillna(False)
            if mask.any():
                failed = self.df.loc[mask].copy()
                failed["check_name"] = check.name
                failed["reason_code"] = check.reason_code
                failed["severity"] = check.severity
                failed["source_topic"] = self.topic
                frames.append(failed)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _present(column: str) -> Callable[[pd.DataFrame], pd.Series]:
    def predicate(df: pd.DataFrame) -> pd.Series:
        if column not in df:
            return pd.Series(False, index=df.index)
        return df[column].notna() & df[column].astype("string").str.len().gt(0)

    return predicate


def _non_negative(column: str) -> Callable[[pd.DataFrame], pd.Series]:
    def predicate(df: pd.DataFrame) -> pd.Series:
        if column not in df:
            return pd.Series(True, index=df.index)
        return pd.to_numeric(df[column], errors="coerce").fillna(0).ge(0)

    return predicate


def _country_code(column: str) -> Callable[[pd.DataFrame], pd.Series]:
    def predicate(df: pd.DataFrame) -> pd.Series:
        if column not in df:
            return pd.Series(True, index=df.index)
        return df[column].astype("string").str.fullmatch(r"[A-Z]{2}").fillna(False)

    return predicate


def fraud_checks(topic: str, df: pd.DataFrame) -> QualityRunner:
    base = [
        QualityCheck("event_id_present", "MISSING_EVENT_ID", _present("event_id")),
        QualityCheck("event_timestamp_present", "MISSING_EVENT_TIMESTAMP", _present("event_timestamp")),
        QualityCheck("customer_id_present", "MISSING_CUSTOMER_ID", _present("customer_id")),
    ]
    if topic.endswith("transactions"):
        base.extend(
            [
                QualityCheck("transaction_id_present", "MISSING_TRANSACTION_ID", _present("transaction_id")),
                QualityCheck("amount_non_negative", "NEGATIVE_AMOUNT", _non_negative("amount")),
                QualityCheck("valid_ip_country", "INVALID_IP_COUNTRY", _country_code("ip_country")),
                QualityCheck("valid_billing_country", "INVALID_BILLING_COUNTRY", _country_code("billing_country")),
            ]
        )
    if topic.endswith("chargebacks"):
        base.append(QualityCheck("chargeback_amount_non_negative", "NEGATIVE_CHARGEBACK", _non_negative("chargeback_amount")))
    return QualityRunner(topic, df, base)
