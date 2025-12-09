#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import UnsupportedCodecError
from kafka.structs import TopicPartition


WARNINGS = []


def record_warning(message):
    """Add a warning once and return it."""
    if message not in WARNINGS:
        WARNINGS.append(message)
    return message

# Force-enable LZ4 support if the library is available (helps when kafka-python mis-detects).
HAS_LZ4 = False
try:
    import lz4.frame as _lz4_frame  # type: ignore
    import kafka.codec as _kafka_codec  # type: ignore

    _kafka_codec.lz4 = _lz4_frame
    _kafka_codec.lz4_decode = _lz4_frame.decompress
    _kafka_codec.has_lz4 = lambda: True
    HAS_LZ4 = True
except Exception:
    HAS_LZ4 = False

if not HAS_LZ4:
    print(
        "Warning: Could not enable LZ4 support (likely missing lz4 library).",
        file=sys.stderr,
    )
    record_warning("Could not enable LZ4 support (likely missing lz4 library).")


def get_topics(admin_client, include_internal=True):
    """Get all topics from the Kafka cluster."""
    topics = admin_client.list_topics()
    if not include_internal:
        topics = [t for t in topics if not t.startswith("_")]
    return sorted(topics)


def normalize_consumer_groups(groups_result):
    possible_groups = getattr(groups_result, "groups", groups_result)
    return list(possible_groups or [])


def extract_group_id(group_info):
    return str(group_info[0]).strip()


def get_consumer_groups(admin_client):
    """Get all consumer groups from the Kafka cluster."""
    consumer_groups = []
    try:
        groups_result = admin_client.list_consumer_groups()
        for group_info in normalize_consumer_groups(groups_result):
            group_id = extract_group_id(group_info)
            if group_id:
                consumer_groups.append(group_id)
    except Exception as exc:
        print(f"Warning: failed to list consumer groups: {exc}", file=sys.stderr)
        record_warning(f"Failed to list consumer groups: {exc}")

    return sorted(list(set(consumer_groups)))


def fetch_timestamp_for_offset(consumer, tp, offset):
    """Return datetime for the message at the given offset (offset must exist)."""
    # Try the target offset and a few before it in case of corruption/failure
    max_retries = 3

    for retry in range(max_retries):
        current_offset = offset - retry
        if current_offset < 0:
            break

        consumer.assign([tp])
        consumer.seek(tp, current_offset)

        try:
            # Poll for the message instead of using next() for better reliability
            msg_map = consumer.poll(timeout_ms=1000, max_records=1)
            msg = msg_map.get(tp, [None])[0]

            if msg and msg.timestamp is not None:
                return datetime.fromtimestamp(msg.timestamp / 1000)
            else:
                warning_msg = f"Could not fetch message/timestamp for {tp.topic} partition {tp.partition} at offset {current_offset}"
                record_warning(warning_msg)
                continue

        except UnsupportedCodecError as exc:
            # Accumulate error for later display and surface the codec error details
            warning_msg = (
                f"Topic '{tp.topic}' uses a compression codec that couldn't be decoded ({exc}). "
                "Install the matching codec (e.g., lz4, snappy) and retry. Skipping timestamp extraction."
            )
            record_warning(warning_msg)
            return None  # Fail immediately on codec error

        except Exception as exc:
            # Log specific message corruption/read errors, then try the next preceding offset
            warning_msg = f"Failed to fetch message for {tp} offset {current_offset}: {exc}. Trying previous offset..."
            record_warning(warning_msg)
            continue  # Try the next loop iteration (previous offset)

    # If all retries fail, record a warning and return None
    warning_msg = f"Could not find message/timestamp for {tp.topic} partition {tp.partition} at offset {offset}"
    record_warning(warning_msg)
    return None


def get_last_consumption_time_and_group(admin_client, consumer, topic, consumer_groups):
    """Get the last consumption time and consumer group for a topic."""
    last_consumption = None

    for group in consumer_groups:
        try:
            group_offsets = admin_client.list_consumer_group_offsets(group)
        except Exception as exc:
            warning_msg = f"Failed to fetch offsets for group {group}: {exc}"
            record_warning(warning_msg)
            continue

        group_last_times = []
        for tp, offset_data in group_offsets.items():
            if tp.topic != topic:
                continue
            if offset_data.offset is None or offset_data.offset <= 0:
                continue

            ts = fetch_timestamp_for_offset(consumer, tp, offset_data.offset - 1)
            if ts:
                group_last_times.append((ts, group))

        if group_last_times:
            latest = max(group_last_times, key=lambda x: x[0])
            if last_consumption is None or latest[0] > last_consumption[0]:
                last_consumption = latest

    return last_consumption if last_consumption else (None, None)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="List last consumed time per Kafka topic."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9091",
        help="Kafka bootstrap servers (default: localhost:9091)",
    )
    parser.add_argument(
        "--hide-internal",
        action="store_true",
        help="Hide internal topics (those starting with underscore).",
    )
    return parser


def main():
    args = build_arg_parser().parse_args()

    kafka_kwargs = {
        "bootstrap_servers": args.bootstrap_servers,
        # To enable security, add here:
        # "security_protocol": "SASL_SSL",
        # "sasl_mechanism": "PLAIN",
        # "sasl_plain_username": "...",
        # "sasl_plain_password": "...",
        # "ssl_cafile": "/path/to/ca-cert.pem",
        # etc.
    }

    admin_client = KafkaAdminClient(**kafka_kwargs)
    consumer = KafkaConsumer(
        **kafka_kwargs,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print("Fetching topics from Kafka cluster...")
    topics = get_topics(admin_client, include_internal=not args.hide_internal)
    print(f"\nFound {len(topics)} topics")

    print("Fetching consumer groups...")
    consumer_groups = get_consumer_groups(admin_client)
    print(f"Found {len(consumer_groups)} consumer groups: {consumer_groups}\n")
    print(f"{'Topic':<30} {'Last Consumed':<30} {'Consumer Group':<30}")
    print("-" * 90)

    for topic in topics:
        last_time, group = get_last_consumption_time_and_group(
            admin_client, consumer, topic, consumer_groups
        )
        if last_time:
            print(f"{topic:<30} {str(last_time):<30} {group:<30}")
        else:
            print(f"{topic:<30} {'No consumer offset found':<30} {'-':<30}")

    consumer.close()
    admin_client.close()

    if WARNINGS:
        print("\n" + "=" * 90, file=sys.stderr)
        print(f"Warnings ({len(WARNINGS)}):", file=sys.stderr)
        for i, warning in enumerate(WARNINGS, 1):
            print(f"{i}. {warning}", file=sys.stderr)


if __name__ == "__main__":
    main()
