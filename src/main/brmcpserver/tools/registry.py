"""MCP tool registration: names, titles and descriptions, bound to their handlers."""

from bronto.client import BrontoClient

from tools.datasets import DatasetTools
from tools.search import SearchTools


def register_tools(mcp, bronto_client: BrontoClient) -> None:
    """Register every Bronto tool on the given FastMCP server."""
    search = SearchTools(bronto_client)
    datasets = DatasetTools(bronto_client)

    mcp.tool(
        name='search_logs',
        title='Execute Event Query',
        description="""Searches log data. This tool returns a list of log events and their attributes
                The prompt should be a question or statement that you want for log data to be searched,
                such as "Can you please search some log data from datasets related to the Bronto ingestion system?".
                Only the @raw field should be presented to the user. No summary or other details should be presented to them.
                """
    )(search.search_logs)

    mcp.tool(
        name='timeseries',
        title='Execute Aggregate or Time-Series Query',
        description="""Computes metric data from log data. This tool returns a list of data points for each key in the group_by_keys
                list. Each list represents the value of the computed metrics for a subset of the provided time range.
            
                The prompt should be a question or statement that you want for a metric to be computed. For instance for web access
                or CDN logs, the question could look like the following: "Can you please provide the sum of the average response
                time per path for the last hour?". The answer would then return the AVG(response_time) metric, grouped by URL path,
                and split into a list of data points, one per every 5 minutes of the provided time range.
                """
    )(search.timeseries)

    mcp.tool(
        name='get_datasets',
        title='Retrieve a List of Logs',
        description='Fetches all dataset details'
    )(datasets.get_datasets)

    mcp.tool(
        name='get_datasets_by_name',
        title='Find Datasets by Name and Collection',
        description="""Fetches details about a Bronto dataset. A dataset is uniquely identify by its name and its 
                collection name. In other words, several datasets with the same name can be associated with different collections. 
                However only one dataset with a given name can be associated to a given collection.
                """
    )(datasets.get_datasets_by_name)

    mcp.tool(
        name='get_keys',
        title='Get Top Keys for a Specific Log ID',
        description="""Fetches all keys present in a dataset, which is represented by a log ID.
                This tool takes a log ID as parameter. A log ID is a string representing a UUID. A log ID maps to a dataset and
                collection name. So given a dataset and collection name, it is possible to retrieve its log ID by using another tool
                which provides details on datasets.
                This tool returns a list strings. Each string provides the name of a key present in the provided dataset
                """
    )(datasets.get_dataset_keys)

    mcp.tool(
        name='get_all_datasets_keys',
        title='Retrieve Top Keys for Logs',
        description="""Fetches all keys present in all datasets.
                This tool returns a list of strings. Each string provides the name of a key present in the provided 
                dataset. This tool is useful in cases such as:
                - to select datasets the contain certain keys
                - to identify the exact key name based on some description 
                """
    )(datasets.get_all_datasets_keys)

    mcp.tool(
        name='get_key_values',
        title='Retrieve Values for a Dataset Key',
        description="""Fetches the values of the provided key and dataset ID.
                This tool returns a list of strings. Each string provides the value of the key provided as input, for 
                the dataset provided as input."""
    )(datasets.get_key_values)
