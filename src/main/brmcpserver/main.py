import logging
import sys
import time

from pydantic import Field, BeforeValidator
from typing_extensions import Annotated
from datetime import datetime, timezone
from typing import List, Optional
from mcp.server.fastmcp import FastMCP
from config import Config
from clients import BrontoClient
from models import Dataset, DatasetKey, LogEvent, Datapoint, Timeseries

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Create an MCP server
mcp = FastMCP("Bronto", stateless_http=True, streamable_http_path="/", json_response=True)


@mcp.tool(description="""Searches log data. This tool returns a list of log events and their attributes
    The prompt should be a question or statement that you want for log data to be searched,
    such as "Can you please search some log data from datasets related to the Bronto ingestion system?".
    Only the @raw field should be presented to the user. No summary or other details should be presented to them.
    """)
def search_logs(
        timerange_start: Annotated[Optional[int], Field(
            description='Unix timestamp in millisecond representing the start of a time range, e.g. 1756063146000. '
                        'If not specify, the current time is selected',
            default_factory=lambda _: (int(time.time()) - (20 * 60)) * 1000
        )],
        timerange_end: Annotated[Optional[int], Field(
            description='Unix timestamp in millisecond representing the end of a time range, e.g. 1756063254000. '
                        'If not specify, the time from 20 minutes ago is selected',
            default_factory=lambda _: int(time.time()) * 1000
        )],
        log_ids: Annotated[list[str], Field(description='List of dataset IDs, identifying sets of log data. Each log ID '
                                                        'represents a UUID', min_length=1)],
        search_filter: Annotated[
            Optional[str],
            Field(default='', description="""
            If no value is specified for this field, then no filter is apply when searching log data. Otherwise, 
            this field must follow the syntax of an SQL `WHERE` clause. Unless the search filter is 
            explicitly provided by the user, it is CRITICAL to use keys present in the dataset, e.g. 
            "key_name"='key_value'. For this, the list of keys present in dataset can be retrieved via another tool 
            exposed by this MCP server. In any case, following SQL syntax,
                - key names should be double-quoted
                - key value should be single-quoted if they are expected to be strings of characters
                - key value should not be quoted if they are expected to be numbers."""
        )],
    ) -> Annotated[
        List[LogEvent],
        Field(description='A list of log events and their attributes. Attributes are key-value pairs associated with '
                          'the event, e.g. key=value')
    ]:
    logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s', timerange_start, timerange_end, log_ids)
    try:
        config = Config()
        bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
        log_events = bronto_client.search(timerange_start, timerange_end, log_ids, search_filter, _select=['*', '@raw'])
        return log_events
    except Exception as e:
        raise Exception('Exception! exception=%s', e)


@mcp.tool(description="""Computes metric data from log data. This tool returns a list of data points for each key in the group_by_keys
    list. Each list represents the value of the computed metrics for a subset of the provided time range.

    The prompt should be a question or statement that you want for a metric to be computed. For instance for web access
    or CDN logs, the question could look like the following: "Can you please provide the sum of the average response
    time per path for the last hour?". The answer would then return the AVG(response_time) metric, grouped by URL path,
    and split into a list of data points, one per every 5 minutes of the provided time range.
    """)
def compute_metrics(
        timerange_start: Annotated[int, Field(
            description='Unix timestamp in millisecond representing the start of a time range, e.g. 1756063146000',
            default_factory=lambda _: int(time.time()) * 1000
        )],
        timerange_end: Annotated[int, Field(
            description='Unix timestamp in millisecond representing the end of a time range, e.g. 1756063254000',
            default_factory=lambda _: (int(time.time()) - (20 * 60)) * 1000
        )],
        log_ids: Annotated[list[str], Field(description='List of dataset IDs, identifying sets of log data. Each log ID '
                                                        'represents a UUID', min_length=1)],
        metric_functions: Annotated[list[str], Field(description='''
            The metric function can be one of AVG, MIN, MAX, COUNT, MEAN, MEDIAN and SUM. The metric function takes a 
            key name as attribute, except for COUNT which only takes the character '*' as attribute. Key names can be 
            determined for given datasets, using one of the other tools provided by this MCP server.''')],
        search_filter: Annotated[str, Field(description="""
            The `search_filter` attribute can follow the syntax of an SQL `WHERE` clause. Unless the search filter is 
            explicitly provided by the user, it is CRITICAL to use keys present in the dataset, e.g. 
            "key_name"='key_value'. For this, the list of keys present in dataset can be retrieved via another tool 
            exposed by this MCP server. In any case, following SQL syntax,
                - key names should be double-quoted
                - key value should be single-quoted if they are expected to be strings of characters
                - key value should not be quoted if they are expected to be numbers."""
            )] = '',
        group_by_keys: Annotated[List[str], Field(description='List of keys expected to be present in log datasets and '
                                                              'by which the metric computed should be grouped')] = None
    ) -> Annotated[
        Timeseries,
        Field(description='Timeseries containing a list of data points for each key in the group_by_keys list. Each list '
                          'represents the value of the computed metrics for a subset of the provided time range')
    ]:
    if group_by_keys is None:
        group_by_keys = []
    logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s, metric_functions=%s, group_by_keys=[%s]',
                timerange_start, timerange_end, log_ids, ','.join(metric_functions), ','.join(group_by_keys))
    try:
        config = Config()
        bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
        resp = bronto_client.search_post(timerange_start, timerange_end, log_ids, search_filter,
                                           _select=metric_functions)
        totals = resp['totals']
        timeseries = []
        for datapoint in totals.get('timeseries', []):
            timeseries.append(Datapoint(timestamp=datapoint['@timestamp'], count=datapoint['count'],
                                        quantiles=datapoint['quantiles'], value=datapoint['value']))
        return Timeseries(count=totals['count'], timeseries=timeseries)
    except Exception as e:
        logger.error('Exception! exception=%s', e, exc_info=e)
        raise e


def _validate_input_time(input_time: str) -> str:
    try:
        datetime.strptime(input_time, '%Y-%m-%d %H:%M:%S')
        return input_time
    except ValueError as e:
        raise e


@mcp.tool(description="""Provides a unix timestamp (in milliseconds) since epoch representation of the input time. This tool
    takes 1 string as parameters, representing a time in the following format '%Y-%m-%d %H:%M:%S'. For instance with
    input_time='2025-05-01 00:00:00' then this tool returns 1746054000000. And with input_time='2025-05-01 01:00:00',
    then this tool returns 1746057600000
    """)
def get_timestamp_as_unix_epoch(
        input_time: Annotated[
            str,
            BeforeValidator(_validate_input_time),
            Field(description='Time represented in the "%Y-%m-%d %H:%M:%S" format')]
    ) -> Annotated[
        int,
        Field(description='A unix timestamp (in milliseconds) since epoch, representing the `input_time` parameter')
    ]:
    return int(datetime.strptime(input_time, '%Y-%m-%d %H:%M:%S').astimezone(timezone.utc).timestamp()) * 1000


@mcp.tool(description='Fetches all dataset details')
def get_datasets() -> Annotated[
        List[Dataset],
        Field(description='''List of datasets. Each dataset object contains
            - the name of the dataset
            - the collection it belongs to
            - its log ID, which is a UUID, i.e. a 36 character long string
            - a list of tags associated to the dataset. Each tag is a key-value pair. Both keys and values are 
            represented as strings. Tags such as the `description` tag are particularly useful to understand the type 
            of data that the dataset contains. Other common tags are `service`, `teams` and `environment`''')
    ]:
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    datasets_data = bronto_client.get_datasets()
    result = []
    for dataset in datasets_data:
        result.append(Dataset(name=dataset["log"], collection=dataset["logset"], log_id=dataset["log_id"],
                              tags=dataset["tags"]))
    return result


@mcp.tool(description="""Fetches details about a Bronto dataset. A dataset is uniquely identify by its name and its 
    collection name. In other words, several datasets with the same name can be associated with different collections. 
    However only one dataset with a given name can be associated to a given collection.
    """)
def get_datasets_by_name(
        dataset_name: Annotated[str, Field(description="The dataset name", min_length=1)],
        collection_name: Annotated[str, Field(description="The collection that the dataset is part of", min_length=1)]
    ) -> Annotated[
        List[Dataset],
        Field(description='List of datasets whose name and collection match the ones provided with the `dataset_name` '
                          'and `collection_name` parameters. Details contains for instance the dataset log ID as well '
                          'as all the tags associated to this dataset.')
    ]:
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    datasets = bronto_client.get_datasets()
    result = []
    collection_names = [dataset['logset'] for dataset in datasets]
    if len(collection_names) == 0:
        return []
    for dataset in datasets:
        if dataset['log'] != dataset_name or dataset['logset'] != collection_name:
            continue
        result.append(Dataset(name=dataset["log"], collection=dataset["logset"], log_id=dataset["log_id"],
                              tags=dataset["tags"]))
    if len(result) == 0:
        return []
    return result


@mcp.tool(description="""Fetches all keys present in a dataset, which is represented by a log ID.
    This tool takes a log ID as parameter. A log ID is a string representing a UUID. A log ID maps to a dataset and
    collection name. So given a dataset and collection name, it is possible to retrieve its log ID by using another tool
    which provides details on datasets.
    This tool returns a list strings. Each string provides the name of a key present in the provided dataset
    """)
def get_keys(
        log_id: Annotated[str, Field(description='The dataset ID, also named log ID', min_length=36, max_length=36)]
    ) -> Annotated[
        List[DatasetKey],
        Field(description='list key names for keys present in the provided dataset referenced with the `log_id` parameter')
    ]:
    config = Config()
    bronto_client = BrontoClient(config.bronto_api_key, config.bronto_api_endpoint)
    keys = bronto_client.get_keys(log_id)
    return keys

@mcp.tool(description="This tool provides the current time in the YYYY-MM-DD HH:mm:ss format")
def get_current_time() -> Annotated[str, Field(description='the current time in the YYYY-MM-DD HH:mm:ss format')]:
    return datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    logger.info('Starting Bronto MCP server')
    mcp.run(transport="streamable-http")
