from datetime import datetime, timezone
from typing import Dict, List
from mcp.server.fastmcp import FastMCP
from config import Config
from clients import BrontoClient, logger

# Create an MCP server
mcp = FastMCP("Bronto", stateless_http=True, streamable_http_path="/", json_response=True)


@mcp.tool(description="""Searches log data. This tool returns a list of log events and their attributes
    The prompt should be a question or statement that you want for log data to be searched,
    such as "Can you please search some log data from datasets related to the Bronto ingestion system?".
    Only the @raw field should be presented to the user. No summary or other details should be presented to them.
    timerange_start and timerange_end are Python integer, not floats. If no explicit timerange is requested by the user,
    please default to searching for data within the last 20 minutes.
    log IDs must be provided as a Python list of strings. Each string represents a UUID.
    The `search_filter` attribute can follow the syntax of an SQL `WHERE` clause. Unless the search filter is explicitly
     provided by the user, it is CRITICAL to use keys present in the dataset, e.g. "key_name"='key_value'. For this, the
      list of keys present in dataset can be retrieved via another tool exposed by this MCP server.

    In any case, following SQL syntax,
    - key names should be double-quoted
    - key value should be single-quoted if they are expected to be strings of characters
    - key value should not be quoted if they are expected to be numbers
    """)
def search_logs(timerange_start: int, timerange_end: int, log_ids: list[str], search_filter='') -> list[str]:
    logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s', timerange_start, timerange_end, log_ids)
    try:
        config = Config()
        bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
        result = bronto_client.search(timerange_start, timerange_end, log_ids, search_filter, _select=['*', '@raw'])
        return [item['@raw'] for item in result['events']]
    except Exception as e:
        logger.error('Exception! exception=%s', e, exc_info=e)
        return [f"Error. exc={e}"]


@mcp.tool(description="""Computes metric data from log data. This tool returns a list of data points for each key in the group_by_keys
    list. Each list represents the value of the computed metrics for a subset of the provided time range.

    The prompt should be a question or statement that you want for a metric to be computed. For instance for web access
    or CDN logs, the question could look like the following: "Can you please provide the sum of the average response
    time per path for the last hour?". The answer would then return the AVG(response_time) metric, grouped by URL path,
    and split into a list of data points, one per every 5 minutes of the provided time range.

    timerange_start and timerange_end are Python integer, not floats. If no explicit timerange is requested by the user,
    please default to searching for data within the last 20 minutes.
    log IDs must be provided as a Python list of strings. Each string represents a UUID.
    The provided log IDs represent specific datasets containing log data under interest.
    The `search_filter` attribute can follow the syntax of an SQL `WHERE` clause. Unless the search filter is explicitly
     provided by the user, it is CRITICAL to use keys present in the dataset, e.g. "key_name"='key_value'. For this, the
      list of keys present in dataset can be retrieved via another tool exposed by this MCP server.

    In any case, following SQL syntax,
    - key names should be double-quoted
    - key value should be single-quoted if they are expected to be strings of characters
    - key value should not be quoted if they are expected to be numbers

    The metric function can be one of AVG, MIN, MAX, COUNT, MEAN, MEDIAN and SUM. The metric function takes a key name
    as attribute. Key names can be determined for given datasets, using one of the other tools provided by this MCP
    server.

    The `group_by_keys` attribute of this tool expects a list of keys belonging to the
    """)
def compute_metrics(timerange_start: int, timerange_end: int, log_ids: list[str], metric_functions: list[str],
                    search_filter='', group_by_keys=None) -> list[str]:
    if group_by_keys is None:
        group_by_keys = []
    logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s, metric_functions=%s, group_by_keys=[%s]',
                timerange_start, timerange_end, log_ids, ','.join(metric_functions), ','.join(group_by_keys))
    try:
        config = Config()
        bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
        result = bronto_client.search_post(timerange_start, timerange_end, log_ids, search_filter,
                                           _select=metric_functions)
        return result['result']
    except Exception as e:
        logger.error('Exception! exception=%s', e, exc_info=e)
        return [f"Error. exc={e}"]


@mcp.tool(description="""Provides a unix timestamp (in milliseconds) since epoch representation of the input time. This tool
    takes 1 string as parameters, representing a time in the following format '%Y-%m-%d %H:%M:%S'. For instance with
    input_time='2025-05-01 00:00:00' then this tool returns 1746054000000. And with input_time='2025-05-01 01:00:00',
    then this tool returns 1746057600000
    """)
def get_timestamp_as_unix_epoch(input_time: str) -> int:
    return int(datetime.strptime(input_time, '%Y-%m-%d %H:%M:%S').astimezone(timezone.utc).timestamp()) * 1000


@mcp.tool(description="""Fetches all dataset details. This tool returns a list strings. Each string provides
    - the name of the dataset
    - the collection it belongs to
    - its log ID, which is a UUID, i.e. a 36 character long string
    - a list of tags associated to the dataset. Each tag is a key-value pair. The key and the value are separated with an equal
    sign, i.e. the "=" character and the values are quoted with double quotes. Tags such as the `description` tag are
    particularly useful to understand the type of data that the dataset contains.
    """)
def get_datasets() -> list[str]:
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


@mcp.tool(description="""Fetches details about a Bronto dataset. A dataset is uniquely identify by its name and its collection name. In
    other words, several datasets with the same name can be associated with different collections. However only one
    dataset with a given name can be associated to a given collection.
    This tool provides details about the dataset whose name and collection name match `dataset_name` and
    `collection_name`. Details contains for instance the dataset log ID as well as all the tags associated to this
    dataset.
    """)
def get_dataset_by_name(dataset_name: str, collection_name: str) -> list[str]:
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    datasets = bronto_client.get_datasets()
    result = []
    collection_names = [dataset['logset'] for dataset in datasets]
    if len(collection_names) == 0:
        return [f'No dataset could be found for the provided dataset and collection names (i.e. {dataset_name} and '
                f'{collection_name}) as no collection named {collection_name} can be found.']
    for dataset in datasets:
        if dataset['log'] != dataset_name or dataset['logset'] != collection_name:
            continue
        tags_sub_sentence = 'has no tags'
        if len(dataset["tags"]) > 0:
            tags_sub_sentence = 'has the following tags: ' + ', '.join([f'{tag}="{dataset["tags"][tag]}' for tag in dataset["tags"]])
        result.append(f'Dataset with name {dataset["log"]}, in collection {dataset["logset"]} and log ID {dataset["log_id"]} {tags_sub_sentence}')
    if len(result) == 0:
        return [f"No dataset named {dataset_name} could be found in the {collection_name} collection"]
    return result


@mcp.tool(description="""Fetches all keys present in a dataset, which is represented by a log ID.
    This tool takes a log ID as parameter. A log ID is a string representing a UUID. A log ID maps to a dataset and
    collection name. So given a dataset and collection name, it is possible to retrieve its log ID by using another tool
    which provides details on datasets.
    This tool returns a list strings. Each string provides the name of a key present in the provided dataset
    """)
def get_keys(log_id: str) -> Dict[str, List[str]]:
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    keys = bronto_client.get_keys(log_id)
    return keys

@mcp.tool(description="This tool provides the current time in the YYYY-MM-DD HH:mm:ss format")
def get_current_time():
    return datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    logger.info('Starting Bronto MCP server')
    mcp.run(transport="streamable-http")
