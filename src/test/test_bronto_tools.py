import pytest
import time
from datetime import datetime
from unittest.mock import Mock

from tools import BrontoTools
from models import Datapoint, Timeseries, DatasetKey, LogEvent
from clients import BrontoClient


@pytest.fixture
def mock_bronto_client(monkeypatch):
    client_mock = Mock(spec=BrontoClient)
    return client_mock


@pytest.fixture
def bronto_tools(mock_bronto_client):
    return BrontoTools(mock_bronto_client)


def test_get_current_time():
    current_time = BrontoTools.get_current_time()
    assert isinstance(current_time, str)
    datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")


def test_get_timestamp_as_unix_epoch():
    test_time = "2025-05-01 00:00:00"
    timestamp = BrontoTools.get_timestamp_as_unix_epoch(test_time)
    assert isinstance(timestamp, int)
    assert timestamp == 1746057600000


def test_get_timestamp_as_unix_epoch_wrong_format():
    test_time = "2025/05/01 00:00:00"
    with pytest.raises(ValueError):
        BrontoTools.get_timestamp_as_unix_epoch(test_time)


def test_get_datasets(bronto_tools, mock_bronto_client):
    mock_datasets = [
        {
            "log": "test_dataset",
            "logset": "test_collection",
            "log_id": "test_log_id",
            "tags": {"team": "test_team"},
        }
    ]
    mock_bronto_client.get_datasets.return_value = mock_datasets
    datasets = bronto_tools.get_datasets()
    assert len(datasets) == 1
    assert datasets[0].name == "test_dataset"
    assert datasets[0].collection == "test_collection"
    assert datasets[0].log_id == "test_log_id"
    assert datasets[0].tags == {"team": "test_team"}


def test_get_datasets_by_name(bronto_tools, mock_bronto_client):
    mock_datasets = [
        {
            "log": "test_dataset",
            "logset": "test_collection",
            "log_id": "test_log_id",
            "tags": {"team": "test_team"},
        }
    ]
    mock_bronto_client.get_datasets.return_value = mock_datasets
    datasets = bronto_tools.get_datasets_by_name("test_dataset", "test_collection")
    assert len(datasets) == 1
    assert datasets[0].name == "test_dataset"


def test_get_datasets_by_name_no_match(bronto_tools, mock_bronto_client):
    mock_datasets = [
        {
            "log": "other_dataset",
            "logset": "other_collection",
            "log_id": "other_log_id",
            "tags": {},
        }
    ]
    mock_bronto_client.get_datasets.return_value = mock_datasets
    datasets = bronto_tools.get_datasets_by_name("test_dataset", "test_collection")
    assert len(datasets) == 0


def test_get_dataset_keys(monkeypatch):
    bronto_client = BrontoClient("some_api_key", "some_endpoint")
    monkeypatch.setattr(
        BrontoClient,
        "get_top_keys",
        lambda _, __: {"key1": ["value1", "1"], "key2": ["value2", "2"]},
    )
    bronto_tools = BrontoTools(bronto_client)
    keys = bronto_tools.get_dataset_keys("test_log_id")
    assert len(keys) == 2
    assert "key1" in keys
    assert "key2" in keys


def test_search_logs(bronto_tools, mock_bronto_client):
    log_event1 = LogEvent(message="test_event1", attributes={"key1": "value1"})
    log_event2 = LogEvent(message="test_event2", attributes={"key2": "value2"})
    mock_log_events = [log_event1, log_event2]
    mock_bronto_client.search.return_value = mock_log_events

    log_events = bronto_tools.search_logs(
        log_ids=["test_log_id"],
        timerange_start=int(time.time()) * 1000,
        timerange_end=int(time.time()) * 1000,
    )

    assert len(log_events) == 2
    assert log_event1 in log_events
    assert log_event2 in log_events


def test_compute_metrics_no_group(bronto_tools, mock_bronto_client):
    timestamp = BrontoTools.get_timestamp_as_unix_epoch("2023-01-01 00:00:00")
    mock_response = {
        "totals": {
            "count": 100,
            "timeseries": [
                {"@timestamp": timestamp, "count": 50, "quantiles": {}, "value": 10.5}
            ],
        }
    }
    mock_bronto_client.search_post.return_value = mock_response

    metrics = bronto_tools.compute_metrics(
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


def test_compute_metrics_single_group(bronto_tools, mock_bronto_client):
    timestamp = BrontoTools.get_timestamp_as_unix_epoch("2023-01-01 00:00:00")
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

    metrics = bronto_tools.compute_metrics(
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
