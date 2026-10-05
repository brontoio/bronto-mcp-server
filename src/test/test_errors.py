from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from tools.errors import ErrorTools


@pytest.fixture
def mock_bronto_client():
    return Mock(spec=BrontoClient)


@pytest.fixture
def error_tools(mock_bronto_client):
    return ErrorTools(mock_bronto_client)


def test_get_error_summary_shapes_totals_and_by_dataset(
    error_tools, mock_bronto_client
):
    mock_bronto_client.get_error_analytics.return_value = {
        "groups_series": [
            {
                "name": "Error",
                "count": 5,
                "groups_series": [{"name": "log-1", "count": 5}],
            },
            {
                "name": "Warn",
                "count": 2,
                "groups_series": [{"name": "log-2", "count": 2}],
            },
        ]
    }
    mock_bronto_client.get_datasets.return_value = [
        {"log": "cdn", "logset": "prod", "log_id": "log-1"}
    ]
    out = error_tools.get_error_summary()
    assert out["errors"] == 5 and out["warnings"] == 2
    assert out["by_dataset"][0] == {
        "log_id": "log-1",
        "name": "cdn",
        "collection": "prod",
        "errors": 5,
        "warnings": 0,
    }
    assert (
        out["by_dataset"][1]["log_id"] == "log-2"
        and out["by_dataset"][1]["name"] is None
    )


def test_get_error_summary_empty_payload_is_zero(error_tools, mock_bronto_client):
    mock_bronto_client.get_error_analytics.return_value = {
        "groups_series": [],
        "total_count": 0,
    }
    mock_bronto_client.get_datasets.return_value = []
    assert error_tools.get_error_summary() == {"errors": 0, "warnings": 0}
