"""MCP tool registration: names, titles and descriptions, bound to their handlers."""

from bronto.client import BrontoClient

from tools.coverage import CoverageTools
from tools.datasets import DatasetTools
from tools.errors import ErrorTools
from tools.monitors import MonitorTools
from tools.saved_searches import SavedSearchTools
from tools.search import SearchTools
from tools.usage import UsageTools


def register_tools(mcp, bronto_client: BrontoClient) -> None:
    """Register every Bronto tool on the given FastMCP server."""
    search = SearchTools(bronto_client)
    datasets = DatasetTools(bronto_client)
    monitors = MonitorTools(bronto_client)
    saved_searches = SavedSearchTools(bronto_client)
    errors = ErrorTools(bronto_client)
    usage = UsageTools(bronto_client)
    coverage = CoverageTools(bronto_client)

    mcp.tool(
        name='search_logs',
        title='Execute Event Query',
        description="""Searches log data. This tool returns a list of log events and their attributes.
                Bare words in search_filter perform full-text matching across the event. Field predicates
                such as "field"='value' only match that exact field; call get_keys before using a field name
                unless the field came from the user or a saved search. For trace ids, request ids, or other
                opaque-identifier investigations, start with a bare full-text word search unless the user
                explicitly asks for a field-specific match. If an exact-field query returns zero, recheck the
                field spelling with get_keys before reporting zero.
                """
    )(search.search_logs)

    mcp.tool(
        name='timeseries',
        title='Execute Aggregate or Time-Series Query',
        description="""Computes metric data from log data. This tool returns a list of data points for each key in the group_by_keys
                list. Each list represents the value of the computed metrics for a subset of the provided time range.
            
                The prompt should be a question or statement that you want for a metric to be computed. For instance for web access
                or CDN logs, the question could look like the following: "Can you please provide the average response
                time per path for the last hour?". The answer would then return the AVG(response_time) metric, grouped by URL path,
                and split into a list of data points, one per every 5 minutes of the provided time range.

                Each timeseries has a 'count' field (matching event count, not a byte value) and each data point a
                'value' field (the numeric aggregate result, e.g. SUM of bytes). Use 'value' as the answer for
                byte/numeric metrics; do not report 'count' as a byte total.
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

    mcp.tool(
        name='create_monitor',
        title='Create Monitor',
        description="""Creates an alerting monitor over one or more datasets.
                The monitor is ALWAYS created muted so it can be reviewed in the Bronto UI before it
                can alert. A notification email is required and MUST belong to an existing Bronto user
                (ask the user for it); the monitor notifies that address. Provide the threshold,
                comparison operator, evaluation window, the dataset log IDs, and an aggregate select
                such as count(*). Returns the created monitor's id."""
    )(monitors.create_monitor)

    mcp.tool(
        name='get_saved_searches',
        title='Get Saved Searches',
        description="""Lists saved searches, or fetches one by id. Optionally filter the list by a
                case-insensitive text query. Use this to discover reusable saved queries."""
    )(saved_searches.get_saved_searches)

    mcp.tool(
        name='create_saved_search',
        title='Create Saved Search',
        description="""Saves a reusable search. Provide a name, the dataset log IDs, and optionally a
                WHERE filter, a select, and a relative time range. Returns the created saved search."""
    )(saved_searches.create_saved_search)

    mcp.tool(
        name='run_saved_search',
        title='Run Saved Search',
        description="""Executes a saved search by id. Returns its events (each with @raw, @time, @status and
                message_kvs), or for an aggregate search the grouped results under totals/groups_series. Pass
                time_range to run over a different window than it was saved with. Use get_saved_searches to find
                the id."""
    )(saved_searches.run_saved_search)

    mcp.tool(
        name='search_monitors',
        title='Search Monitors',
        description="""Lists monitors, optionally filtered by a text query or status. With a monitor_id,
                returns that monitor plus its recent trigger/recovery events."""
    )(monitors.search_monitors)

    mcp.tool(
        name='get_error_summary',
        title='Get Error Summary',
        description="""Returns org-wide parsing/ingestion error and warning totals with a per-dataset
                breakdown. Precomputed, so it costs no search quota. Optionally scoped by time range."""
    )(errors.get_error_summary)

    mcp.tool(
        name='get_usage',
        title='Get Usage',
        description="""Reports per-dataset ingestion and search usage (bytes) for the org against its
                monthly limits. Use to understand data volume and spend. Only billable usage is reported:
                system collections such as .usage and .audit-trail are excluded."""
    )(usage.get_usage)

    mcp.tool(
        name='get_coverage',
        title='Get Coverage',
        description="""Reports, per dataset, whether it is monitored (which monitors cover it),
                searched, and still ingesting — surfacing blind spots such as a live dataset that no
                monitor is watching. Monitor coverage is resolved from the explicit dataset ids in monitors'
                backing queries; monitors that select datasets with a from_expr are not resolved, so their
                datasets may appear unmonitored even though a monitor covers them."""
    )(coverage.get_coverage)

    mcp.tool(
        name='validate_monitors',
        title='Validate Monitors',
        description="""Health-checks existing monitors by re-running each monitor's backing query over
                the last 24 hours: 'error' = the monitor cannot fire as built, 'warn' = valid but no data,
                'ok' = data present. Use to find alarms that have silently broken or gone quiet."""
    )(monitors.validate_monitors)
