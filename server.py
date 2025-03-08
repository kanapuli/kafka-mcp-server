"""
Kafka MCP Server - A Model Context Protocol Server for basic Kafka operations
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Optional

import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
from mcp.server.fastmcp import FastMCP, Context


mcp = FastMCP("Kafka Server")

if __name__ == "__main__":
    mcp.run()
