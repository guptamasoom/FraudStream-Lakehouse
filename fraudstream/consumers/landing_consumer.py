from __future__ import annotations

import argparse
import json
import os
from datetime import date

try:
    import boto3
    from kafka import KafkaConsumer
except Exception:  # pragma: no cover
    boto3 = None
    KafkaConsumer = None

from dotenv import load_dotenv

from fraudstream.topics import TOPIC_NAMES


load_dotenv()


def get_kafka_consumer(bootstrap_servers: str):
    if KafkaConsumer is None:
        raise RuntimeError("kafka-python is not installed.")

    return KafkaConsumer(
        *TOPIC_NAMES.values(),
        bootstrap_servers=bootstrap_servers,

        # Confluent Cloud
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
        sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME"),
        sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD"),

        # Consumer behaviour
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=8000,

        # JSON decoding
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


def get_s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 is not installed.")

    aws_profile = os.getenv("AWS_PROFILE")
    region = os.getenv("AWS_REGION", "us-east-1")

    if aws_profile:
        session = boto3.Session(
            profile_name=aws_profile,
            region_name=region,
        )
    else:
        session = boto3.Session(
            region_name=region,
        )

    return session.client("s3")


def write_events_to_s3(
    s3_client,
    bucket: str,
    topic: str,
    records: list[dict],
    processing_date: str,
) -> int:

    if not records:
        return 0

    prefix = os.getenv(
        "S3_LANDING_PREFIX",
        "landing/",
    ).rstrip("/")

    key = (
        f"{prefix}/"
        f"{topic}/"
        f"processing_date={processing_date}/"
        f"events.jsonl"
    )

    content = "\n".join(
        json.dumps(record, default=str)
        for record in records
    ) + "\n"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(
        f"Wrote {len(records)} events "
        f"to s3://{bucket}/{key}"
    )

    return len(records)


def consume_to_landing(
    bootstrap_servers: str,
    max_messages: int = 1000,
) -> int:

    consumer = get_kafka_consumer(bootstrap_servers)

    bucket = os.getenv("S3_BUCKET")

    if not bucket:
        raise RuntimeError(
            "S3_BUCKET is not configured in .env"
        )

    s3_client = get_s3_client()

    counts: dict[str, list[dict]] = {}

    total = 0

    print("Starting Kafka consumer...")
    print(f"Kafka: {bootstrap_servers}")
    print(f"S3 bucket: {bucket}")
    print(f"Topics: {list(TOPIC_NAMES.values())}")

    try:
        for message in consumer:

            counts.setdefault(
                message.topic,
                []
            ).append(message.value)

            total += 1

            print(
                f"Received event | "
                f"topic={message.topic} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )

            if total >= max_messages:
                break

    finally:
        consumer.close()

    processing_date = date.today().isoformat()

    written = 0

    for topic, records in counts.items():

        written += write_events_to_s3(
            s3_client=s3_client,
            bucket=bucket,
            topic=topic,
            records=records,
            processing_date=processing_date,
        )

    print(
        f"Kafka events consumed: {total}"
    )

    print(
        f"Events landed to S3: {written}"
    )

    return written


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Consume FraudStream events from "
            "Confluent Cloud Kafka and land them "
            "into AWS S3."
        )
    )

    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=1000,
    )

    return parser.parse_args()


def main() -> None:

    args = parse_args()

    if not args.kafka_bootstrap_servers:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS is not configured."
        )

    count = consume_to_landing(
        bootstrap_servers=args.kafka_bootstrap_servers,
        max_messages=args.max_messages,
    )

    print(
        f"Landing completed successfully: {count} events"
    )


if __name__ == "__main__":
    main()