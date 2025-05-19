from datetime import datetime
import logging

from mcp.server.fastmcp import FastMCP
from config import Config
from clients import BrontoClient

# Create an MCP server
mcp = FastMCP("Bronto Search")

logger = logging.getLogger()
# TODO: debugging issues is not easy. We try to get some visibility by logging to a file
# even this is proving difficult sometime. Logging an error as part of an exception being raised seems to always
# generate logs though.
handler = logging.FileHandler('/tmp/bronto_mcp.log')
handler.setLevel(logging.DEBUG)

# Define the log message format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Attach the handler to the logger
logger.addHandler(handler)

@mcp.tool()
def get_logs(timerange_start: int, timerange_end: int, log_ids: list[str]) -> list[str]:
    """Fetches log data. This tool returns a list of log events
    The prompt should be a question or statement that you want for log data to be retrieved,
    such as "Can you please retrieve some log data from datasets related to the Bronto ingestion system?".
    Only the raw data should be presented to the user. No summary or other details should be presented to them.
    timerange_start and timerange_end are Python integer, not floats.
    log IDs must be provided as a Python list of strings. Each string represents a UUID.
    """
    logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s', timerange_start, timerange_end, log_ids)
    try:
        config = Config()
        bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
        result = bronto_client.get_log_data(timerange_start, timerange_end, log_ids)
        return result.split('\r\n')[:100]
    except Exception as e:
        logger.error('Exception! exception=%s', e, exc_info=e)
        return ["Error."]

@mcp.tool()
def get_timestamp_as_unix_epoch(input_time: str) -> int:
    """Provides a unix timestamp (in milliseconds) since epoch representation of the input time. This tool
    takes 1 string as parameters, representing a time in the following format '%Y-%m-%d %H:%M:%S'. For instance with
    input_time='2025-05-01-00 00:00:00' then this tool returns 1746054000000. And with input_time='2025-05-01 01:00:00',
    then this tool returns 1746057600000)
    """
    return int(datetime.strptime(input_time, '%Y-%m-%d %H:%M:%S').timestamp()) * 1000


@mcp.tool()
def get_datasets() -> list[str]:
    """Fetches all dataset details. This tool returns a list strings. Each string provides
    - the name of the dataset
    - the collection it belongs to
    - its log ID, which is a UUID, i.e. a 36 character long string
    - a list of tags associated to the dataset. Each tag is a key-value pair. The key and the value are separated with an equal
    sign, i.e. the "=" character and the values are quoted with double quotes.
    """
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    datasets = bronto_client.get_datasets()
    result = []
    for dataset in datasets:
        tags_sub_sentence = 'has no tags'
        if len(dataset["tags"]) > 0:
            tags_sub_sentence = 'has the following tags: ' + ', '.join([f'{tag}="{dataset["tags"][tag]}' for tag in dataset["tags"]])
        result.append(f'Dataset with name {dataset["log"]}, in collection {dataset["logset"]} and log ID {dataset["log_id"]} {tags_sub_sentence}')
    return result


if __name__ == "__main__":
    mcp.run()
