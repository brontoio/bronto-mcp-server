import time
from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from tools.coverage import CoverageTools


@pytest.fixture
def mock_bronto_client():
    return Mock(spec=BrontoClient)


@pytest.fixture
def coverage_tools(mock_bronto_client):
    return CoverageTools(mock_bronto_client)


def test_get_coverage_flags_blind_spots(coverage_tools, mock_bronto_client):
    mock_bronto_client.get_datasets.return_value = [
        {"log": "cdn", "logset": "prod", "log_id": "log-1"},
        {"log": "fra", "logset": "prod", "log_id": "log-2"},  # no monitor -> blind spot
    ]
    mock_bronto_client.list_monitors.return_value = [
        {"id": "m1", "name": "5xx", "metric_id": "def-1"}
    ]
    mock_bronto_client.list_metric_definitions.return_value = [
        {"id": "def-1", "queries": [{"from": ["log-1"]}]}
    ]
    mock_bronto_client.get_usage_by_log.side_effect = lambda usage_type, tr: (
        [{"log_id": "log-1", "bytes_total": 100, "count": 2}]
        if usage_type == "ingestion"
        else []
    )
    out = coverage_tools.get_coverage()
    by = {d["log_id"]: d for d in out["datasets"]}
    assert by["log-1"]["monitors"] == ["m1"]
    assert by["log-2"]["monitors"] == []  # blind spot
    assert by["log-1"]["ingestion"] == {"active": True, "bytes": 100}
    assert out["summary"] == {"datasets_total": 2, "monitored": 1, "unmonitored": 1}


def test_get_coverage_uses_heartbeat_when_usage_omits_dataset(
    coverage_tools, mock_bronto_client
):
    now_ms = int(time.time() * 1000)
    mock_bronto_client.get_datasets.return_value = [
        {
            "log": "events",
            "logset": ".audit-trail",
            "log_id": "log-1",
            "metadata": {"last_heartbeat_at": now_ms - 60_000},
        },
        {
            "log": "old",
            "logset": "prod",
            "log_id": "log-2",
            "metadata": {"last_heartbeat_at": now_ms - 30 * 24 * 3600 * 1000},
        },
    ]
    mock_bronto_client.list_monitors.return_value = []
    mock_bronto_client.list_metric_definitions.return_value = []
    mock_bronto_client.get_usage_by_log.return_value = []
    by = {d["log_id"]: d for d in coverage_tools.get_coverage()["datasets"]}
    assert by["log-1"]["ingestion"]["active"] is True
    assert by["log-2"]["ingestion"]["active"] is False
