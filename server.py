"""
Kafka MCP Server - A Model Context Protocol Server for basic Kafka operations
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Optional

import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
from mcp.server.fastmcp import FastMCP, Context


@dataclass
class KafkaContext:
    """Container for Kafka clients that will be shared across requests"""

    producer: confluent_kafka.Producer
    admin_client: AdminClient
    consumer_factory: callable


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[KafkaContext]:
    """Initialize KafkaContext on startup and clean up on shutdown"""

    config = {
        "bootstrap.servers": "localhost:9092",  # TODO: Make this configurable
    }
    producer = confluent_kafka.Producer(config)
    admin_client = AdminClient(config)

    def create_consumer(group_id="mcp-kafka-server", auto_offset_reset="earliest"):
        consumer_config = config.copy()
        consumer_config.update(
            {
                "group_id": group_id,
                auto_offset_reset: auto_offset_reset,
            }
        )
        return confluent_kafka.Consumer(consumer_config)

    try:
        yield KafkaContext(
            producer=producer,
            admin_client=admin_client,
            consumer_factory=create_consumer,
        )
    finally:
        producer.flush()


mcp = FastMCP("Kafka Server", lifespan=lifespan)


@mcp.resource("kafka://topics")
def list_topics() -> str:
    """List all available topics"""
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    admin = kafka_ctx.admin_client

    topics_metadata = admin.list_topics(timeout=10)
    topics = list(topics_metadata.topics.keys())

    if not topics:
        return "No topics found"

    return "\n".join(
        [
            "# Kafka Topics",
            "",
            "Available Topics:",
            "",
            *[f"- {topic}" for topic in topics],
        ]
    )


@mcp.resource("kafka://topic/{topic}/info")
def get_topic_info(
    topic: str,
) -> str:
    """Get information about a specific topic"""
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    admin = kafka_ctx.admin_client

    try:
        topics_metadata = admin.list_topics(topic=topic, timeout=10)

        if topic not in topics_metadata:
            return f"Topic {topic} does not exist"

        topic_metadata = topics_metadata.topics[topic]
        partitions = topic_metadata.partitions

        return "\n".join(
            [
                f"# Topic: {topic}",
                "",
                f"Number of partitions: {partitions}",
                "",
                "## Partitions",
                *[
                    f"- Partition {p_id}: Leader: {p_meta.leader}, Replicas: {p_meta.replicas}"
                    for p_id, p_meta in partitions.items()
                ],
            ]
        )
    except Exception as e:
        return f"Error retrieving topic information: {str(e)}"


@mcp.resource("kafka://topic/{topic}/messages")
def get_topic_messages(topic: str) -> str:
    """Get recent messages from a kafka topic(last 10 messages)"""
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    consumer = kafka_ctx.consumer_factory()

    try:
        consumer.subscribe([topic])

        messages = []
        timeout = 1.0  # 5 seconds
        max_messages = 10

        while len(messages) < max_messages:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                # no more messages within timeout
                break

            if msg.error():
                return f"Error while retrieving message: {msg.error()}"

            try:
                value = msg.value.decode("utf-8")
                messages.append(value)
            except Exception:
                messages.append(f"[Binary data: {len(msg.value())} bytes]")

        if not messages:
            return f"No messages available in the topic: {topic}"

        return "\n".join(
            [
                f"Recent messages from topic: {topic}",
                "",
                *[f"- Message {i + 1}: {msg}" for i, msg in enumerate(messages)],
            ]
        )

    finally:
        consumer.close()


@mcp.tool()
def publish_message(topic: str, message: str) -> str:
    """Publish a message to Kafka broker"""
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    producer = kafka_ctx.producer

    try:
        message_bytes = message.encode("utf-8")

        producer.produce(topic, message_bytes)
        producer.flush(timeout=10)

        return f"Message successfully published to the topic: {topic}"
    except Exception as e:
        return f"Error publishing message: {message}"


@mcp.tool()
def create_topic(
    topic: str, num_partitions: int = 1, replication_factor: int = 1
) -> str:
    """Create a topic in the Kafka broker"""
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    admin = kafka_ctx.admin_client

    try:
        existing_topics = admin.list_topics(timeout=10)
        if topic in existing_topics:
            return f"Topic {topic} already exists in the broker"

        new_topic = NewTopic(
            topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        result = admin.create_topic([new_topic])
        for topic_name, future in result.items():
            try:
                future.result()
                return f"Topic {topic} created successfully"
            except Exception as e:
                return f"Error creating topic: {topic}, err:{str(e)}"

    except Exception as e:
        return f"Error creating topic: {topic}, err: {str(e)}"


@mcp.tool()
def delete_topic(topic: str) -> str:
    context = Context.current()
    kafka_ctx = context.request_context.lifespan_context
    admin = kafka_ctx.admin_client

    try:
        existing_topics = admin.list_topics(timeout=10)
        if topic not in existing_topics:
            return f"Topic {topic} not found in the broker"

        result = admin.delete_topics([topic])
        for topic_name, future in result.items():
            try:
                future.result()
                return f"Topic {topic} deleted successfully"
            except Exception as e:
                return f"Error while deleting the topic {topic}. err: {str(e)}"
        return f"Topic deletion request submitted for topic {topic} but no response recieved"
    except Exception as e:
        return f"Error deleting the topic {topic}. err: {str(e)}"


@mcp.prompt("monitor-kafka")
def monitor_kafka_prompt() -> str:
    """Create a prompt for monitoring kafka"""
    return """I would like to monitor kafka cluster.
Please help me:
1. List all available topics
2. For each topic show me details about its configuration
3. If there are any topics with recent messages, show me those.
"""


@mcp.prompt("kafka-produce-message")
def produce_message_prompt(topic: str = "") -> str:
    """Create a prompt for producing a message for kafka topic"""
    topic_part = f"to topic '{topic}'" if topic else ""
    return f"""I need to send a message to {topic_part}
Please help me:
1. Check if the topic exists or guide me to create if needed
2. Send the following message to the topic    

[Your message here]
"""


if __name__ == "__main__":
    mcp.run()
