# Read the last time each topic was consumed 

An example on fetching the last time each topic in a kafka cluster was consumed.

- [Read the last time each topic was consumed](#read-the-last-time-each-topic-was-consumed)
  - [Disclaimer](#disclaimer)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Connect](#connect)
    - [Create topics](#create-topics)
    - [Create Connectors](#create-connectors)
    - [Check Control Center](#check-control-center)
  - [Our Script](#our-script)
    - [Create Console Consumers](#create-console-consumers)
  - [Cleanup](#cleanup)

## Disclaimer

The code and/or instructions here available are **NOT** intended for production usage. 
It's only meant to serve as an example or reference and does not replace the need to follow actual and official documentation of referenced products.

## Setup

### Start Docker Compose

```bash
docker compose up -d
```

### Connect

If you already have the plugin folders you can jump to next step.

You can check the connector plugins available by executing:

```bash
curl localhost:8083/connector-plugins | jq
```

As you see we only have source connectors:

```text
[
  {
    "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "type": "source",
    "version": "7.6.0-ce"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "type": "source",
    "version": "7.6.0-ce"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "type": "source",
    "version": "7.6.0-ce"
  }
]
```

Let's install confluentinc/kafka-connect-datagen connector plugin for sink.

```shell
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
```

Restart connect:

```shell
docker compose restart connect
```

Now if we list our plugins again we should see new one corresponding to the Datagen connector.

### Create topics 

Let's create first our topics with two partitions each:

```shell
kafka-topics --bootstrap-server localhost:9091 --topic customers --create --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server localhost:9091 --topic orders --create --partitions 2 --replication-factor 1
```

### Create Connectors

Let's create our source connectors using datagen:

```bash
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/my-datagen-source2/config -d '{
    "name" : "my-datagen-source2",
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic" : "customers",
    "output.data.format" : "AVRO",
    "quickstart" : "SHOE_CUSTOMERS",
    "tasks.max" : "1"
}'
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/my-datagen-source3/config -d '{
    "name" : "my-datagen-source3",
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic" : "orders",
    "output.data.format" : "AVRO",
    "quickstart" : "SHOE_ORDERS",
    "tasks.max" : "1"
}'
```

### Check Control Center

Open http://localhost:9021 and check cluster is healthy including Kafka Connect.

## Our Script

Let's run:

```shell
python3 -m venv venv
source venv/bin/activate
pip install kafka-python lz4
python lastTimeConsumed.py
```

You should get something like:

```
Requirement already satisfied: kafka-python in ./venv/lib/python3.13/site-packages (2.3.0)
Requirement already satisfied: lz4 in ./venv/lib/python3.13/site-packages (4.4.5)

[notice] A new release of pip is available: 24.3.1 -> 25.3
[notice] To update, run: pip install --upgrade pip
Fetching topics from Kafka cluster...

Found 14 topics
Fetching consumer groups...
Found 5 consumer groups: ['ConfluentTelemetryReporterSampler--8964530507091506048', '_confluent-controlcenter-2-3-0-1', '_confluent-controlcenter-2-3-0-1-command', 'connect-group', 'schema-registry']

Topic                          Last Consumed                  Consumer Group                
------------------------------------------------------------------------------------------
__consumer_offsets             No consumer offset found       -                             
__internal_confluent_only_broker_info No consumer offset found       -                             
_confluent-alerts              No consumer offset found       -                             
_confluent-command             2025-12-08 17:21:57.631000     _confluent-controlcenter-2-3-0-1-command
_confluent-controlcenter-2-3-0-1-AlertHistoryStore-changelog No consumer offset found       -                             
_confluent-controlcenter-2-3-0-1-AlertHistoryStore-repartition No consumer offset found       -                             
_confluent-telemetry-metrics   No consumer offset found       -                             
_confluent_balancer_api_state  No consumer offset found       -                             
_schemas                       No consumer offset found       -                             
connect-configs                No consumer offset found       -                             
connect-offsets                No consumer offset found       -                             
connect-status                 No consumer offset found       -                             
customers                      No consumer offset found       -                             
orders                         No consumer offset found       -                                        
```

### Create Console Consumers

You can create console consumers for the `customers` and `orders` topics to track consumption. In separate terminal windows, run:

**Terminal 1 - Consume from customers topic:**
```bash
kafka-console-consumer --bootstrap-server localhost:9091 --topic customers --from-beginning --group console-customers
```

**Terminal 2 - Consume from orders topic:**
```bash
kafka-console-consumer --bootstrap-server localhost:9091 --topic orders --from-beginning --group console-orders
```

Now re-run the Python script to see the new consumer groups and their last consumption times:

```bash
python3 -m venv venv
source venv/bin/activate
pip install kafka-python lz4
python lastTimeConsumed.py
```

You should now see output similar to:

```
Requirement already satisfied: kafka-python in ./venv/lib/python3.13/site-packages (2.3.0)
Requirement already satisfied: lz4 in ./venv/lib/python3.13/site-packages (4.4.5)

[notice] A new release of pip is available: 24.3.1 -> 25.3
[notice] To update, run: pip install --upgrade pip
Fetching topics from Kafka cluster...

Found 14 topics
Fetching consumer groups...
Found 7 consumer groups: ['ConfluentTelemetryReporterSampler--8964530507091506048', '_confluent-controlcenter-2-3-0-1', '_confluent-controlcenter-2-3-0-1-command', 'connect-group', 'console-customers', 'console-orders', 'schema-registry']

Topic                          Last Consumed                  Consumer Group                
------------------------------------------------------------------------------------------
__consumer_offsets             No consumer offset found       -                             
__internal_confluent_only_broker_info No consumer offset found       -                             
_confluent-alerts              No consumer offset found       -                             
_confluent-command             2025-12-08 17:21:57.631000     _confluent-controlcenter-2-3-0-1-command
_confluent-controlcenter-2-3-0-1-AlertHistoryStore-changelog No consumer offset found       -                             
_confluent-controlcenter-2-3-0-1-AlertHistoryStore-repartition No consumer offset found       -                             
_confluent-telemetry-metrics   No consumer offset found       -                             
_confluent_balancer_api_state  No consumer offset found       -                             
_schemas                       No consumer offset found       -                             
connect-configs                No consumer offset found       -                             
connect-offsets                No consumer offset found       -                             
connect-status                 No consumer offset found       -                             
customers                      2025-12-08 17:28:52.258000     console-customers             
orders                         2025-12-08 17:28:54.659000     console-orders                            
```

The script will now display `console-customers` and `console-orders` as the consumer groups consuming these topics with their respective last consumption timestamps.

## Cleanup

```bash
docker compose down -v
rm -fr plugins
rm -fr venv
```