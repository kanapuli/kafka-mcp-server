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
        "session.timeout.ms": 10000,
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
def list_topics(ctx: Context) -> str:
    """List all available topics"""
    kafka_ctx = ctx.request_context.lifespan_context
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
def get_topic_info(topic: str, ctx: Context) -> str:
    """Get information about a specific topic"""
    kafka_ctx = ctx.request_context.lifespan_context
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
def get_topic_messages(topic: str, ctx: Context) -> str:
    """Get recent messages from a kafka topic(last 10 messages)"""
    kafka_ctx = ctx.request_context.lifespan_context
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
def publish_message(topic: str, message: str, ctx: Context):
    """Publish a message to Kafka broker"""
    kafka_ctx = ctx.request_context.lifespan_context
    producer = kafka_ctx.producer

    try:
        message_bytes = message.encode("utf-8")

        producer.produce(topic, message_bytes)
        producer.flush(timeout=10)

        return f"Message successfully published to the topic: {topic}"
    except Exception as e:
        return f"Error publishing message: {message}"


if __name__ == "__main__":
    mcp.run()
