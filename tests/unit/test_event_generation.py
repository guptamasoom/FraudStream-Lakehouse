from fraudstream.producers.transaction_simulator import build_event_batch
from fraudstream.topics import TOPIC_NAMES


def test_event_batch_contains_expected_topics():
    batch = build_event_batch(fraud_rate=100, bad_data_percentage=0)
    assert TOPIC_NAMES["transactions"] in batch
    assert TOPIC_NAMES["logins"] in batch
    assert TOPIC_NAMES["devices"] in batch
    assert batch[TOPIC_NAMES["transactions"]][0]["transaction_id"].startswith("txn_")
