import pandas as pd

from fraudstream.pipeline.quality.checks import fraud_checks


def test_quality_checks_quarantine_bad_transaction():
    df = pd.DataFrame(
        [
            {
                "event_id": "1",
                "event_timestamp": "2026-06-01T00:00:00Z",
                "transaction_id": "txn_1",
                "customer_id": "cust_1",
                "amount": 10,
                "ip_country": "US",
                "billing_country": "US",
            },
            {
                "event_id": "2",
                "event_timestamp": None,
                "transaction_id": "txn_2",
                "customer_id": None,
                "amount": -10,
                "ip_country": "???",
                "billing_country": "US",
            },
        ]
    )
    runner = fraud_checks("fraud_transactions", df)
    scorecard = runner.scorecard()
    quarantine = runner.quarantine()
    assert (scorecard["failed_record_count"] > 0).any()
    assert not quarantine.empty
    assert {"check_name", "reason_code", "severity"}.issubset(quarantine.columns)
