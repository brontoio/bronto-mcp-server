"""Event and aggregate (time-series) search tools."""

import logging
import time
from typing import Dict, List, Optional

from bronto.client import BrontoClient
from bronto.models import Datapoint, LogEvent, Timeseries
from pydantic import Field
from typing_extensions import Annotated

logger = logging.getLogger()


class SearchTools:
    """Tools to search log data stored in Bronto"""

    def __init__(self, bronto_client: BrontoClient):
        self.bronto_client = bronto_client

    def search_logs(
            self,
            timerange_start: Annotated[Optional[int], Field(
                description='Unix timestamp in millisecond representing the start of a time range, e.g. 1756063146000. '
                            'If not specified, defaults to 20 minutes ago',
                default_factory=lambda _: (int(time.time()) - (20 * 60)) * 1000
            )],
            timerange_end: Annotated[Optional[int], Field(
                description='Unix timestamp in millisecond representing the end of a time range, e.g. 1756063254000. '
                            'If not specified, defaults to the current time',
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
                    - key value should not be quoted if they are expected to be numbers.""")
            ] = '',
    ) -> Annotated[
        List[LogEvent],
        Field(description='A list of log events and their attributes. Attributes are key-value pairs associated with '
                          'the event, e.g. key=value')
    ]:
        logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s', timerange_start, timerange_end, log_ids)
        log_events = self.bronto_client.search(timerange_start, timerange_end, log_ids, search_filter, _select=['*', '@raw'])
        return log_events

    def timeseries(
            self,
            timerange_start: Annotated[int, Field(
                description='Unix timestamp in millisecond representing the start of a time range, e.g. 1756063146000',
                default_factory=lambda _: (int(time.time()) - (20 * 60)) * 1000
            )],
            timerange_end: Annotated[int, Field(
                description='Unix timestamp in millisecond representing the end of a time range, e.g. 1756063254000',
                default_factory=lambda _: int(time.time()) * 1000
            )],
            log_ids: Annotated[list[str], Field(description='List of dataset IDs, identifying sets of log data. Each log ID '
                                                            'represents a UUID', min_length=1)],
            metric_functions: Annotated[list[str], Field(description='''
                The metric function can be one of AVG, MIN, MAX, COUNT, MEAN, MEDIAN and SUM. The metric function takes a 
                key name as attribute, except for COUNT which only takes the character '*' as attribute (i.e. 
                "COUNT(*)"). Key names can be determined for given datasets, using one of the other tools provided by 
                this MCP server.''')],
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
        Dict[str, Timeseries],
        Field(description='Map of Timeseries. The keys of the map represent group names based on the group_by_keys '
                          'parameter. The Timeseries represent a list of data points for the given group. Each list '
                          'represents the value of the computed metrics for a subset of the provided time range')
    ]:
        if group_by_keys is None:
            group_by_keys = []
        logger.info('timerange_start=%s, timerange_end=%s, log_ids=%s, metric_functions=%s, group_by_keys=[%s]',
                    timerange_start, timerange_end, log_ids, ','.join(metric_functions), ','.join(group_by_keys))
        resp = self.bronto_client.search_post(timerange_start, timerange_end, log_ids, search_filter,
                                              _select=metric_functions, group_by_keys=[','.join(group_by_keys)])
        if len(group_by_keys) == 0:
            totals = resp['totals']
            count = totals['count']
            timeseries = totals.get('timeseries', [])
            name = ''
            group_series = [{'name': name, 'timeseries':timeseries, 'count': count}]
        else:
            group_series = resp.get('groups_series', [])
        result = {}
        for group_serie in group_series:
            datapoints = []
            for datapoint in group_serie.get('timeseries', []):
                datapoints.append(Datapoint(timestamp=datapoint['@timestamp'], count=datapoint['count'],
                                            quantiles=datapoint['quantiles'], value=datapoint['value']))
            timeseries = Timeseries(count=group_serie['count'], timeseries=datapoints)
            result[group_serie['name']] = timeseries
        return result
