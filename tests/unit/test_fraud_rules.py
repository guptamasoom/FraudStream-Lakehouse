import pandas as pd

from fraudstream.pipeline.fraud.rules import score_transactions


def test_high_risk_transaction_scores_high():
    tx = pd.DataFrame(
        [
            {
                "event_timestamp": "2026-06-01T00:00:00Z",
                "transaction_id": "txn_1",
                "customer_id": "cust_1",
                "merchant_id": "merch_1",
                "amount": 1200,
                "payment_method": "card_not_present",
                "device_id": "dev_1",
                "ip_country": "NG",
                "billing_country": "US",
                "shipping_country": "NG",
            }
        ]
    )
    devices = pd.DataFrame([{"device_id": "dev_1", "is_new_device": True}])
    logins = pd.DataFrame(
        [
            {"customer_id": "cust_1", "event_type": "login_failed"},
            {"customer_id": "cust_1", "event_type": "login_failed"},
        ]
    )
    scored = score_transactions(tx, logins, devices)
    assert scored.loc[0, "risk_band"] == "HIGH"
    assert scored.loc[0, "risk_score"] >= 70
