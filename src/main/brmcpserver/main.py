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
                     'collections, keys, etc. The tools provided by this server help '
                     '- selecting datasets based on their tags or the keys that their data contains For instance, '
                     '  - when looking to select datasets based on the key they contain, the following process should be used:'
                     '    - list the datasets and check their tags. Select datasets whose tags match the keys under interest.'
                     '    - also list the keys of all datasets and select datasets that contain the key under interest.'
                     '  - when looking to select datasets based on the value of a key that they contain, then the '
                     '    following process should be used:'
                     '    - list the datasets and check their tags. Select datasets whose tags and values match the keys '
                     '      and values under interest.'
                     '    - also always list the keys of all datasets to look for keys that is relevant to the one under '
                     '      interest. Then retrieve the dataset IDs for datasets that contain at least one of those keys '
                     '      and so that the value of the key matches the provided value.'
                     '- searching log events in datasets, based on some filter. Filter are typically based on some of '
                     'keys and values that the dataset contains.'
                     '- computing metrics present in datasets, based on some filter, and grouping the results according '
                     'to some of the keys present in the dataset',
        dependencies=['pydantic']
    )
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    bronto_tools = BrontoTools(bronto_client)
    bronto_tools.register(mcp)
    logger.info('Bronto tools registered successfully')

    mcp.run(transport="streamable-http")
