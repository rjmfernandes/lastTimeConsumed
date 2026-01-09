#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime

from kafka import KafkaAdminClient, KafkaConsumer  # type: ignore
from kafka.errors import UnsupportedCodecError  # type: ignore

WARNINGS = []


def record_warning(message):
    """Add a warning once and return it."""
    if message not in WARNINGS:
        WARNINGS.append(message)
    return message


# When True, consumer groups originating from Confluent Control Center
# (prefix `_confluent-controlcenter`) will be ignored when scanning
# for last consumption times. Change to False to include them.
IGNORE_CONFLUENT_CONTROL_CENTER_GROUPS = True


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

# Force-enable Snappy support if the library is available.
HAS_SNAPPY = False
try:
    import snappy as _snappy  # type: ignore  # python-snappy
    import kafka.codec as _kafka_codec  # type: ignore

    _kafka_codec.snappy = _snappy
    _kafka_codec.snappy_decode = _snappy.decompress
    _kafka_codec.has_snappy = lambda: True
    HAS_SNAPPY = True
except Exception:
    HAS_SNAPPY = False

if not HAS_SNAPPY:
    print(
        "Warning: Could not enable Snappy support (likely missing python-snappy library).",
        file=sys.stderr,
    )
    record_warning(
        "Could not enable Snappy support (likely missing python-snappy library)."
    )


def get_topics(admin_client, include_internal=True):
    """Get all topics from the Kafka cluster."""
    topics = admin_client.list_topics()
    if not include_internal:
        topics = [t for t in topics if not t.startswith("_")]
        topics = [t for t in topics if "-changelog" not in t]
        topics = [t for t in topics if "-repartition" not in t]
        topics = [t for t in topics if "confluent-" not in t]
        topics = [t for t in topics if "connect-" not in t]
        topics = [t for t in topics if "replicator-" not in t]
        topics = [t for t in topics if "syslog_" not in t]
        topics = [t for t in topics if "-processing-log" not in t]
    return sorted(topics)


def normalize_consumer_groups(groups_result):
    possible_groups = getattr(groups_result, "groups", groups_result)
    return list(possible_groups or [])


def extract_group_id(group_info):
    # kafka-python sometimes returns tuples, sometimes objects depending on version
    try:
        return str(group_info[0]).strip()
    except Exception:
        return str(group_info).strip()


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

    # Optionally ignore Confluent Control Center internal consumer groups
    if IGNORE_CONFLUENT_CONTROL_CENTER_GROUPS:
        consumer_groups = [
            g for g in consumer_groups if not g.startswith("_confluent-controlcenter")
        ]

    return sorted(list(set(consumer_groups)))


def fetch_timestamp_for_offset(consumer, tp, offset):
    """
    Best-effort: return datetime for the first message at/after `offset`.

    Handles:
      - retention (offset older than beginning -> short-circuit)
      - compaction/gaps (exact offset may not exist -> accept first >= offset)
      - slow fetch (poll for a few seconds instead of a single 1s shot)

    Returns:
      datetime or None
    """
    if offset is None or offset < 0:
        return None

    # Bounds check: retention / truncation
    try:
        beginning = consumer.beginning_offsets([tp]).get(tp)
        end = consumer.end_offsets([tp]).get(tp)
    except Exception as exc:
        beginning, end = None, None
        record_warning(
            f"Failed to fetch beginning/end offsets for {tp.topic} partition {tp.partition}: {exc}"
        )

    if beginning is not None and offset < beginning:
        record_warning(
            f"{tp.topic} partition {tp.partition}: offset {offset} is older than retention "
            f"(earliest available offset is {beginning})."
        )
        return None

    if end is not None and offset >= end:
        # Clamp to last existing record if possible
        if end <= 0:
            record_warning(
                f"{tp.topic} partition {tp.partition}: topic appears empty (end offset {end})."
            )
            return None
        offset = end - 1

    # Try the target offset and a few before it
    max_retries = 3

    for retry in range(max_retries):
        current_offset = offset - retry
        if current_offset < 0:
            break

        consumer.assign([tp])
        try:
            consumer.seek(tp, current_offset)

            # Poll in a loop (not just one 1-second shot)
            total_wait_ms = 5000
            waited = 0
            msg = None

            while waited < total_wait_ms:
                msg_map = consumer.poll(timeout_ms=1000, max_records=1)
                recs = msg_map.get(tp) or []
                if recs:
                    msg = recs[0]
                    break
                waited += 1000

            if not msg:
                record_warning(
                    f"Could not fetch message/timestamp for {tp.topic} partition {tp.partition} "
                    f"at/after offset {current_offset} (no records after {total_wait_ms}ms)"
                )
                continue

            # Accept record at/after the requested offset (important for compaction gaps)
            if msg.timestamp is not None:
                return datetime.fromtimestamp(msg.timestamp / 1000)

            record_warning(
                f"Fetched record for {tp.topic} partition {tp.partition} at offset {msg.offset} "
                f"but it had no timestamp."
            )
            continue

        except UnsupportedCodecError as exc:
            warning_msg = (
                f"Topic '{tp.topic}' uses a compression codec that couldn't be decoded ({exc}). "
                "Install the matching codec (e.g., lz4, snappy) and retry. Skipping timestamp extraction."
            )
            record_warning(warning_msg)
            return None

        except Exception as exc:
            # Covers auth failures, timeouts, out-of-range, etc. without brittle imports
            record_warning(
                f"Failed to fetch message for {tp.topic} partition {tp.partition} offset {current_offset}: {exc}. "
                "Trying previous offset..."
            )
            continue

    record_warning(
        f"Could not find message/timestamp for {tp.topic} partition {tp.partition} near offset {offset}"
    )
    return None


def compute_last_consumption_for_topics(admin_client, consumer, topics, consumer_groups):
    """Compute last consumption timestamp and group for each topic efficiently.

    Strategy:
    - Iterate each consumer group once and call `list_consumer_group_offsets(group)` once per group.
    - For each TopicPartition offset encountered, fetch the message timestamp only once and cache it.
    - Track whether *any* offset was seen for each topic, so we can distinguish:
        - truly no offsets
        - offsets exist but timestamp couldn't be fetched (retention/ACL/codec/etc.)
    """
    # results: topic -> (latest_timestamp, consumer_group, saw_any_offset)
    results = {t: (None, None, False) for t in topics}

    # Cache timestamps for (topic, partition, offset)
    timestamp_cache = {}

    for group in consumer_groups:
        try:
            group_offsets = admin_client.list_consumer_group_offsets(group)
        except Exception as exc:
            record_warning(f"Failed to fetch offsets for group {group}: {exc}")
            continue

        for tp, offset_data in group_offsets.items():
            if tp.topic not in results:
                continue
            if offset_data.offset is None or offset_data.offset <= 0:
                continue

            # Mark that we saw an offset for this topic (even if later timestamp fetch fails)
            cur_ts, cur_group, _ = results[tp.topic]
            results[tp.topic] = (cur_ts, cur_group, True)

            target_offset = offset_data.offset - 1
            cache_key = (tp.topic, tp.partition, target_offset)

            if cache_key in timestamp_cache:
                ts = timestamp_cache[cache_key]
            else:
                ts = fetch_timestamp_for_offset(consumer, tp, target_offset)
                timestamp_cache[cache_key] = ts

            if not ts:
                continue

            current_ts, current_group, saw_offset = results[tp.topic]
            if current_ts is None or ts > current_ts:
                results[tp.topic] = (ts, group, saw_offset)

    return results


def build_arg_parser():
    parser = argparse.ArgumentParser(description="List last consumed time per Kafka topic.")
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

    print(f"{'Topic':<60} {'Last Consumed':<30} {'Consumer Group':<30}")
    print("-" * 120)

    # Compute last consumption for all topics in a single efficient pass
    topic_last_map = compute_last_consumption_for_topics(
        admin_client, consumer, topics, consumer_groups
    )

    for topic in topics:
        last_time, group, saw_offset = topic_last_map.get(topic, (None, None, False))
        if last_time:
            print(f"{topic:<60} {str(last_time):<30} {group:<30}")
        else:
            if saw_offset:
                print(f"{topic:<60} {'Offset found, timestamp unavailable':<30} {'-':<30}")
            else:
                print(f"{topic:<60} {'No consumer offset found':<30} {'-':<30}")

    consumer.close()
    admin_client.close()

    if WARNINGS:
        print("\n" + "=" * 120, file=sys.stderr)
        print(f"Warnings ({len(WARNINGS)}):", file=sys.stderr)
        for i, warning in enumerate(WARNINGS, 1):
            print(f"{i}. {warning}", file=sys.stderr)


if __name__ == "__main__":
    main()
