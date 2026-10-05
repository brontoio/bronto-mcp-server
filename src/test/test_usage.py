from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from tools.usage import UsageTools


@pytest.fixture
def mock_bronto_client():
    return Mock(spec=BrontoClient)


@pytest.fixture
def usage_tools(mock_bronto_client):
    return UsageTools(mock_bronto_client)


def test_get_usage_joins_axes_and_limits(usage_tools, mock_bronto_client):
    mock_bronto_client.get_usage_by_log.side_effect = lambda usage_type, tr: (
        [{"log_id": "log-1", "bytes_total": 100, "count": 1}]
        if usage_type == "ingestion"
        else [{"log_id": "log-1", "bytes_total": 50, "count": 1}]
    )
    mock_bronto_client.get_usage_limits.return_value = [
        {
            "target": "ORGANISATION",
            "type": "SYSTEM",
            "category": "INGESTION_LIMITS",
            "unit": "BYTES",
            "value": 1000,
        },
        {
            "target": "ORGANISATION",
            "type": "SYSTEM",
            "category": "SEARCH_LIMITS",
            "unit": "RATIO",
            "value": 20,
        },
    ]
    out = usage_tools.get_usage()
    assert (
        out["summary"]["ingest_bytes"] == 100 and out["summary"]["search_bytes"] == 50
    )
    assert (
        out["summary"]["ingest_limit"] == 1000
        and out["summary"]["search_limit"] == 20000
    )
    assert out["datasets"][0]["log_id"] == "log-1"
