from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from faker import Faker

try:
    from kafka import KafkaProducer
except Exception:  # pragma: no cover - Kafka is optional for pure local tests.
    KafkaProducer = None

from fraudstream.pipeline.utils.io import write_jsonl
from fraudstream.topics import TOPIC_NAMES

fake = Faker()

COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE", "BR"]
MERCHANT_CATEGORIES = ["grocery", "electronics", "travel", "gaming", "fashion", "fuel", "wallet"]
PAYMENT_METHODS = ["card_present", "card_not_present", "upi", "wallet", "netbanking"]


@dataclass
class SimulatorConfig:
    event_rate: int
    duration: int
    fraud_rate: float
    kafka_bootstrap_servers: str
    local_outbox: str | None


def _ts(offset_minutes: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()


def customer_id() -> str:
    return f"cust_{random.randint(1000, 9999)}"


def merchant_id() -> str:
    return f"merch_{random.randint(100, 999)}"


def device_id() -> str:
    return f"dev_{random.randint(10000, 99999)}"


def make_transaction(force_fraud: bool = False) -> dict:
    amount = round(random.lognormvariate(4.0, 0.8), 2)
    risk_country = random.choice(["RU", "NG", "BR", "AE"])
    normal_country = random.choice(COUNTRIES)
    is_fraud = force_fraud or random.random() < 0.08
    if is_fraud:
        amount = round(max(amount, random.uniform(600, 2500)), 2)
    payment_method = "card_not_present" if is_fraud else random.choice(PAYMENT_METHODS)
    ip_country = risk_country if is_fraud else normal_country
    shipping_country = risk_country if is_fraud and random.random() < 0.75 else normal_country
    return {
        "event_id": str(uuid4()),
        "event_type": "transaction",
        "event_timestamp": _ts(random.randint(-15, 2)),
        "transaction_id": f"txn_{uuid4().hex[:12]}",
        "customer_id": customer_id(),
        "merchant_id": merchant_id(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": amount,
        "currency": "USD",
        "payment_method": payment_method,
        "device_id": device_id(),
        "ip_country": ip_country,
        "billing_country": normal_country,
        "shipping_country": shipping_country,
        "is_card_present": False if payment_method == "card_not_present" else random.random() > 0.55,
        "auth_result": random.choice(["approved", "approved", "approved", "declined"]),
        "is_known_fraud": is_fraud,
    }


def make_login(customer: str | None = None, device: str | None = None, force_failed: bool = False) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "login_failed" if force_failed else random.choice(["login_success", "login_failed", "password_reset"]),
        "event_timestamp": _ts(random.randint(-20, 1)),
        "customer_id": customer or customer_id(),
        "device_id": device or device_id(),
        "ip_country": random.choice(COUNTRIES),
        "user_agent": fake.user_agent(),
        "mfa_passed": random.random() > 0.15,
    }


def make_device(customer: str | None = None, device: str | None = None, force_new: bool = False) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "new_device" if force_new else random.choice(["device_seen", "new_device", "device_blocked"]),
        "event_timestamp": _ts(random.randint(-60, 1)),
        "customer_id": customer or customer_id(),
        "device_id": device or device_id(),
        "device_type": random.choice(["ios", "android", "web", "tablet"]),
        "ip_country": random.choice(COUNTRIES),
        "is_new_device": True if force_new else random.random() < 0.22,
    }


def make_chargeback() -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "chargeback_opened",
        "event_timestamp": _ts(random.randint(-120, -5)),
        "transaction_id": f"txn_{uuid4().hex[:12]}",
        "customer_id": customer_id(),
        "merchant_id": merchant_id(),
        "chargeback_amount": round(random.uniform(20, 900), 2),
        "reason_code": random.choice(["fraud", "product_not_received", "duplicate", "customer_dispute"]),
    }


def inject_bad_data(event: dict) -> dict:
    mutated = dict(event)
    mode = random.choice(["missing_customer", "negative_amount", "missing_timestamp", "bad_country"])
    if mode == "missing_customer":
        mutated["customer_id"] = None
    elif mode == "negative_amount" and "amount" in mutated:
        mutated["amount"] = -abs(float(mutated["amount"]))
    elif mode == "missing_timestamp":
        mutated["event_timestamp"] = None
    elif mode == "bad_country":
        mutated["ip_country"] = "???"
    return mutated


def build_event_batch(fraud_rate: float, bad_data_percentage: float) -> dict[str, list[dict]]:
    force_fraud = random.random() < (fraud_rate / 100)
    transaction = make_transaction(force_fraud=force_fraud)
    login_events = [make_login(transaction["customer_id"], transaction["device_id"])]
    if force_fraud:
        login_events = [
            make_login(transaction["customer_id"], transaction["device_id"], force_failed=True),
            make_login(transaction["customer_id"], transaction["device_id"], force_failed=True),
        ]
    events = {
        TOPIC_NAMES["transactions"]: [transaction],
        TOPIC_NAMES["logins"]: login_events,
        TOPIC_NAMES["devices"]: [make_device(transaction["customer_id"], transaction["device_id"], force_new=force_fraud)],
        TOPIC_NAMES["chargebacks"]: [],
    }
    if force_fraud and random.random() < 0.25:
        events[TOPIC_NAMES["chargebacks"]].append(make_chargeback())
    for topic, topic_events in events.items():
        events[topic] = [
            inject_bad_data(event) if random.random() < (bad_data_percentage / 100) else event
            for event in topic_events
        ]
    return events


def emit_to_kafka(events: dict[str, list[dict]], bootstrap_servers: str) -> int:
    if KafkaProducer is None:
        raise RuntimeError("kafka-python is not installed.")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    count = 0
    for topic, rows in events.items():
        for row in rows:
            producer.send(topic, key=str(row.get("customer_id") or "unknown"), value=row)
            count += 1
    producer.flush()
    return count


def emit_to_local(events: dict[str, list[dict]], root: str) -> int:
    count = 0
    for topic, rows in events.items():
        if rows:
            count += write_jsonl(rows, f"{root}/{topic}/events.jsonl")
    return count


def run(config: SimulatorConfig, bad_data_percentage: float) -> int:
    total = 0
    start = time.time()
    while config.duration == 0 or time.time() - start < config.duration:
        events = build_event_batch(config.fraud_rate, bad_data_percentage)
        if config.local_outbox:
            total += emit_to_local(events, config.local_outbox)
        else:
            total += emit_to_kafka(events, config.kafka_bootstrap_servers)
        time.sleep(max(1 / max(config.event_rate, 1), 0.01))
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate realistic fintech fraud events.")
    parser.add_argument("--event-rate", type=int, default=10)
    parser.add_argument("--duration", type=int, default=30, help="Seconds. Use 0 for continuous.")
    parser.add_argument("--fraud-rate", type=float, default=8.0)
    parser.add_argument("--bad-data-percentage", type=float, default=5.0)
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--local-outbox", default=None, help="Write JSONL directly for offline demos.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    total = run(
        SimulatorConfig(
            event_rate=args.event_rate,
            duration=args.duration,
            fraud_rate=args.fraud_rate,
            kafka_bootstrap_servers=args.kafka_bootstrap_servers,
            local_outbox=args.local_outbox,
        ),
        bad_data_percentage=args.bad_data_percentage,
    )
    print(f"Generated {total} events")


if __name__ == "__main__":
    main()
