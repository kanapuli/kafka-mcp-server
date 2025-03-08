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

if __name__ == "__main__":
    mcp.run()
