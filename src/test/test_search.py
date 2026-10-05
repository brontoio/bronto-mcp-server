import time
from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from bronto.models import Datapoint, LogEvent, Timeseries
from tools.search import SearchTools


@pytest.fixture
def mock_bronto_client(monkeypatch):
    client_mock = Mock(spec=BrontoClient)
    return client_mock


@pytest.fixture
def search_tools(mock_bronto_client):
    return SearchTools(mock_bronto_client)


def test_search_logs(search_tools, mock_bronto_client):
    log_event1 = LogEvent(message="test_event1", attributes={"key1": "value1"})
    log_event2 = LogEvent(message="test_event2", attributes={"key2": "value2"})
    mock_log_events = [log_event1, log_event2]
    mock_bronto_client.search.return_value = mock_log_events

    log_events = search_tools.search_logs(
        log_ids=["test_log_id"],
        timerange_start=int(time.time()) * 1000,
        timerange_end=int(time.time()) * 1000,
    )

    assert len(log_events) == 2
    assert log_event1 in log_events
    assert log_event2 in log_events


def test_timeseries_no_group(search_tools, mock_bronto_client):
    timestamp = 1672531200000
    mock_response = {
        "totals": {
            "count": 100,
            "timeseries": [
                {"@timestamp": timestamp, "count": 50, "quantiles": {}, "value": 10.5}
            ],
        }
    }
    mock_bronto_client.search_post.return_value = mock_response

    metrics = search_tools.timeseries(
        log_ids=["test_log_id"],
        metric_functions=["SUM"],
        timerange_start=int(time.time()) * 1000,
        timerange_end=int(time.time()) * 1000,
    )

    groups = list(metrics.keys())
    assert len(groups) == 1
    assert groups[0] == ""
    group = metrics[groups[0]]
    assert isinstance(group, Timeseries)
    assert group.count == 100
    assert len(group.timeseries) == 1
    assert group.timeseries[0] == Datapoint(
        timestamp=timestamp, count=50, quantiles={}, value=10.5
    )


def test_timeseries_single_group(search_tools, mock_bronto_client):
    timestamp = 1672531200000
    mock_response = {
        "groups_series": [
            {
                "name": "my_group",
                "count": 100,
                "timeseries": [
                    {
                        "@timestamp": timestamp,
                        "count": 50,
                        "quantiles": {},
                        "value": 10.5,
                    }
                ],
            }
        ]
    }
    mock_bronto_client.search_post.return_value = mock_response

    metrics = search_tools.timeseries(
        log_ids=["test_log_id"],
        metric_functions=["SUM"],
        timerange_start=int(time.time()) * 1000,
        timerange_end=int(time.time()) * 1000,
        group_by_keys=["some_key"],
    )

    groups = list(metrics.keys())
    assert len(groups) == 1
    assert groups[0] == "my_group"
    group = metrics[groups[0]]
    assert isinstance(group, Timeseries)
    assert group.count == 100
    assert len(group.timeseries) == 1
    assert group.timeseries[0] == Datapoint(
        timestamp=timestamp, count=50, quantiles={}, value=10.5
    )


def test_search_logs_time_range_descriptions_match_defaults():
    import inspect

    params = inspect.signature(SearchTools.search_logs).parameters
    start = params["timerange_start"].annotation.__metadata__[0].description
    end = params["timerange_end"].annotation.__metadata__[0].description
    assert "defaults to 20 minutes ago" in start
    assert "defaults to the current time" in end
