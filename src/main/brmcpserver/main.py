import logging
import sys

from mcp.server.fastmcp import FastMCP
from config import Config
from clients import BrontoClient
from tools import BrontoTools

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


if __name__ == "__main__":
    logger.info('Starting Bronto MCP server')
    mcp = FastMCP(
        "Bronto",
        stateless_http=True,
        streamable_http_path="/",
        json_response=True,
        instructions='Use this MCP server to interact with log data stored in Bronto as well as its metadata, datasets, '
                     'collections, keys, etc',
        dependencies=['pydantic', 'zstd']
    )
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    bronto_tools = BrontoTools(bronto_client)
    bronto_tools.register(mcp)
    logger.info('Bronto tools registered successfully')

    mcp.run(transport="streamable-http")
