#!/usr/bin/env python3

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from datetime import datetime


def get_topics(bootstrap_servers):
    """Get all topics from the Kafka cluster."""
    # For secure cluster connections, add these optional parameters:
    # - security_protocol='SASL_SSL' or 'SSL' for encrypted connections
    # - sasl_mechanism='PLAIN' or 'SCRAM-SHA-256' or 'SCRAM-SHA-512' for authentication
    # - sasl_plain_username and sasl_plain_password for PLAIN authentication
    # - sasl_plain_password for SCRAM authentication with username in sasl_plain_username
    # - ssl_cafile='/path/to/ca-cert' for CA certificate verification
    # - ssl_certfile='/path/to/client-cert' for client certificate
    # - ssl_keyfile='/path/to/client-key' for client key
    # - ssl_check_hostname=True to verify broker hostname matches certificate
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    admin_client.close()
    return topics


def get_consumer_groups(bootstrap_servers):
    """Get all consumer groups from the Kafka cluster."""
    # For secure cluster connections, add these optional parameters:
    # - security_protocol='SASL_SSL' or 'SSL' for encrypted connections
    # - sasl_mechanism='PLAIN' or 'SCRAM-SHA-256' or 'SCRAM-SHA-512' for authentication
    # - sasl_plain_username and sasl_plain_password for PLAIN authentication
    # - ssl_cafile='/path/to/ca-cert' for CA certificate verification
    # - ssl_certfile='/path/to/client-cert' for client certificate
    # - ssl_keyfile='/path/to/client-key' for client key
    # - ssl_check_hostname=True to verify broker hostname matches certificate
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer_groups = []

    try:
        groups_result = admin_client.list_consumer_groups()

        # kafka-python can return slightly different shapes depending on version:
        # - ListConsumerGroupsResult with .groups attribute
        # - tuple ([ConsumerGroupListing...], errors)
        # - dict with "groups"
        possible_groups = []

        if hasattr(groups_result, "groups"):
            possible_groups = getattr(groups_result, "groups")
        elif isinstance(groups_result, tuple) and len(groups_result) > 0:
            possible_groups = groups_result[0]
        elif isinstance(groups_result, dict):
            possible_groups = groups_result.get("groups", [])
        elif groups_result:
            possible_groups = groups_result

        for group_info in possible_groups:
            if isinstance(group_info, tuple) and group_info:
                group_id = group_info[0]
            elif hasattr(group_info, "group_id"):
                group_id = group_info.group_id
            elif hasattr(group_info, "groupId"):
                group_id = group_info.groupId
            elif hasattr(group_info, "id"):
                group_id = group_info.id
            else:
                group_id = str(group_info)

            if group_id and str(group_id).strip():
                consumer_groups.append(str(group_id).strip())
    except Exception:
        pass
    finally:
        admin_client.close()

    # Remove duplicates and sort
    return sorted(list(set(consumer_groups)))


def get_last_consumption_time_and_group(bootstrap_servers, topic, consumer_groups):
    """Get the last consumption time and consumer group for a topic."""
    # For secure cluster connections, add these optional parameters:
    # - security_protocol='SASL_SSL' or 'SSL' for encrypted connections
    # - sasl_mechanism='PLAIN' or 'SCRAM-SHA-256' or 'SCRAM-SHA-512' for authentication
    # - sasl_plain_username and sasl_plain_password for PLAIN authentication
    # - ssl_cafile='/path/to/ca-cert' for CA certificate verification
    # - ssl_certfile='/path/to/client-cert' for client certificate
    # - ssl_keyfile='/path/to/client-key' for client key
    # - ssl_check_hostname=True to verify broker hostname matches certificate

    last_consumption = None
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Check consumer group offsets
    for group in consumer_groups:
        try:
            group_offsets = admin_client.list_consumer_group_offsets(group)
            group_last_times = []

            # Check if this group has offsets for this topic
            for tp, offset_data in group_offsets.items():
                if tp.topic != topic:
                    continue

                # offset is "next offset to read", so last consumed is offset - 1
                if offset_data.offset is not None and offset_data.offset > 0:
                    # Found an offset, now get the message timestamp
                    # For secure cluster connections, add these optional parameters:
                    # - security_protocol='SASL_SSL' or 'SSL' for encrypted connections
                    # - sasl_mechanism='PLAIN' or 'SCRAM-SHA-256' or 'SCRAM-SHA-512' for authentication
                    # - sasl_plain_username and sasl_plain_password for PLAIN authentication
                    # - ssl_cafile='/path/to/ca-cert' for CA certificate verification
                    # - ssl_certfile='/path/to/client-cert' for client certificate
                    # - ssl_keyfile='/path/to/client-key' for client key
                    # - ssl_check_hostname=True to verify broker hostname matches certificate
                    try:
                        consumer = KafkaConsumer(
                            bootstrap_servers=bootstrap_servers,
                            group_id=group,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                        )
                        consumer.assign([tp])
                        consumer.seek(tp, offset_data.offset - 1)
                        try:
                            msg = next(consumer)
                            timestamp = msg.timestamp
                            group_last_times.append(
                                (
                                    datetime.fromtimestamp(timestamp / 1000),
                                    group,
                                )
                            )
                        except StopIteration:
                            pass
                        finally:
                            consumer.close()
                    except Exception:
                        pass

            if group_last_times:
                latest = max(group_last_times, key=lambda x: x[0])
                if last_consumption is None or latest[0] > last_consumption[0]:
                    last_consumption = latest

        except Exception:
            pass

    admin_client.close()

    # If no consumer group has committed offsets, show the last message in the topic with no group attribution
    if last_consumption is None:
        try:
            # For secure cluster connections, add these optional parameters:
            # - security_protocol='SASL_SSL' or 'SSL' for encrypted connections
            # - sasl_mechanism='PLAIN' or 'SCRAM-SHA-256' or 'SCRAM-SHA-512' for authentication
            # - sasl_plain_username and sasl_plain_password for PLAIN authentication
            # - ssl_cafile='/path/to/ca-cert' for CA certificate verification
            # - ssl_certfile='/path/to/client-cert' for client certificate
            # - ssl_keyfile='/path/to/client-key' for client key
            # - ssl_check_hostname=True to verify broker hostname matches certificate
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id="temp-group-" + str(datetime.now().timestamp()),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )

            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                last_times = []
                for partition in partitions:
                    tp = TopicPartition(topic, partition)
                    consumer.assign([tp])
                    consumer.seek_to_end(tp)
                    last_offset = consumer.position(tp)

                    if last_offset > 0:
                        consumer.seek(tp, last_offset - 1)
                        try:
                            msg = next(consumer)
                            timestamp = msg.timestamp
                            last_times.append(datetime.fromtimestamp(timestamp / 1000))
                        except StopIteration:
                            pass

                if last_times:
                    last_consumption = (max(last_times), "No consumer group")

            consumer.close()

        except Exception:
            pass

    if last_consumption:
        return last_consumption
    return None, None


def main():
    # Kafka cluster connection settings
    bootstrap_servers = "localhost:9091"

    # If your Kafka cluster requires security/authentication, uncomment and configure:
    # Example for SASL_SSL with PLAIN authentication:
    # bootstrap_servers = "kafka-broker:9092"
    # security_protocol = "SASL_SSL"
    # sasl_mechanism = "PLAIN"
    # sasl_plain_username = "your-username"
    # sasl_plain_password = "your-password"
    # ssl_cafile = "/path/to/ca-cert.pem"
    #
    # Example for SSL with client certificates:
    # security_protocol = "SSL"
    # ssl_cafile = "/path/to/ca-cert.pem"
    # ssl_certfile = "/path/to/client-cert.pem"
    # ssl_keyfile = "/path/to/client-key.pem"
    # ssl_check_hostname = True
    #
    # Then pass these parameters to KafkaConsumer and KafkaAdminClient calls

    print("Fetching topics from Kafka cluster...")
    topics = get_topics(bootstrap_servers)

    # Include all topics (both user and internal/system topics)
    all_topics = sorted(topics)

    print(f"\nFound {len(all_topics)} topics")

    # Fetch consumer groups once
    print("Fetching consumer groups...")
    consumer_groups = get_consumer_groups(bootstrap_servers)
    print(f"Found {len(consumer_groups)} consumer groups: {consumer_groups}\n")
    print(f"{'Topic':<30} {'Last Consumed':<30} {'Consumer Group':<30}")
    print("-" * 90)

    for topic in all_topics:
        last_time, group = get_last_consumption_time_and_group(
            bootstrap_servers, topic, consumer_groups
        )
        if last_time:
            print(f"{topic:<30} {str(last_time):<30} {group:<30}")
        else:
            print(f"{topic:<30} {'No messages found':<30} {'-':<30}")


if __name__ == "__main__":
    main()
