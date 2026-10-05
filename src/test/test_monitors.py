from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from tools.monitors import MonitorTools


@pytest.fixture
def mock_bronto_client():
    return Mock(spec=BrontoClient)


@pytest.fixture
def monitor_tools(mock_bronto_client):
    return MonitorTools(mock_bronto_client)


def test_create_monitor_builds_muted_body_with_email_action(
    monitor_tools, mock_bronto_client
):
    mock_bronto_client.get_users.return_value = [
        {"id": "u1", "email": "alice@example.com"}
    ]
    mock_bronto_client.create_monitor.return_value = {"id": "mon-1"}

    result = monitor_tools.create_monitor(
        name="5xx spike",
        notification_email="alice@example.com",
        threshold=10,
        comparison_operator="ABOVE",
        window="Last 15 minutes",
        log_ids=["log-1"],
        select="count(*)",
        search_filter="status >= 500",
    )

    assert result == {"id": "mon-1"}
    body = mock_bronto_client.create_monitor.call_args[0][0]
    assert body["mute_until"] == -1  # always muted for review
    assert body["actions"] == [{"type": "EMAIL", "email": "alice@example.com"}]
    assert body["threshold"] == 10 and body["comparison_operator"] == "ABOVE"
    assert body["queries"][0] == {
        "name": "5xx spike",
        "select": ["count(*)"],
        "from": ["log-1"],
        "where": "status >= 500",
    }


def test_create_monitor_rejects_unknown_email(monitor_tools, mock_bronto_client):
    mock_bronto_client.get_users.return_value = [
        {"id": "u1", "email": "alice@example.com"}
    ]
    with pytest.raises(ValueError, match="not an existing Bronto user"):
        monitor_tools.create_monitor(
            name="x",
            notification_email="stranger@example.com",
            threshold=1,
            comparison_operator="ABOVE",
            window="Last 15 minutes",
            log_ids=["log-1"],
        )
    mock_bronto_client.create_monitor.assert_not_called()


def test_create_monitor_dry_run_returns_body_without_creating(
    monitor_tools, mock_bronto_client
):
    mock_bronto_client.get_users.return_value = [
        {"id": "u1", "email": "alice@example.com"}
    ]
    out = monitor_tools.create_monitor(
        name="x",
        notification_email="alice@example.com",
        threshold=1,
        comparison_operator="ABOVE",
        window="Last 15 minutes",
        log_ids=["log-1"],
        dry_run=True,
    )
    assert out["dry_run"] is True
    assert out["monitor_body"]["mute_until"] == -1
    assert out["monitor_body"]["queries"][0]["from"] == ["log-1"]
    mock_bronto_client.create_monitor.assert_not_called()


def test_create_monitor_dry_run_still_validates_email(
    monitor_tools, mock_bronto_client
):
    mock_bronto_client.get_users.return_value = [
        {"id": "u1", "email": "alice@example.com"}
    ]
    with pytest.raises(ValueError, match="not an existing Bronto user"):
        monitor_tools.create_monitor(
            name="x",
            notification_email="stranger@example.com",
            threshold=1,
            comparison_operator="ABOVE",
            window="Last 15 minutes",
            log_ids=["log-1"],
            dry_run=True,
        )


def test_search_monitors_single_returns_events(monitor_tools, mock_bronto_client):
    mock_bronto_client.list_monitors.return_value = [
        {"id": "m1", "name": "e", "status": "OK"}
    ]
    mock_bronto_client.get_monitor_events.return_value = [{"status": "ALARM"}]
    out = monitor_tools.search_monitors(monitor_id="m1")
    assert out["monitor"]["id"] == "m1" and out["events"] == [{"status": "ALARM"}]


def test_validate_monitors_reports_ok_warn_error(monitor_tools, mock_bronto_client):
    mock_bronto_client.list_monitors.return_value = [
        {"id": "m1", "name": "has data", "metric_id": "def-1"},
        {"id": "m2", "name": "no data", "metric_id": "def-2"},
        {"id": "m3", "name": "orphaned", "metric_id": "gone"},
    ]
    mock_bronto_client.list_metric_definitions.return_value = [
        {"id": "def-1", "queries": [{"from": ["log-1"]}]},
        {"id": "def-2", "queries": [{"from": ["log-2"]}]},
    ]
    mock_bronto_client.run_search.side_effect = lambda body: (
        {"totals": {"count": 5}}
        if body.get("from") == ["log-1"]
        else {"totals": {"count": 0}}
    )
    out = monitor_tools.validate_monitors()
    by = {r["monitor_id"]: r["status"] for r in out["results"]}
    assert by == {"m1": "ok", "m2": "warn", "m3": "error"}
    assert out["summary"] == {"ok": 1, "warn": 1, "error": 1}


def test_validate_monitors_marks_failing_query_as_error(
    monitor_tools, mock_bronto_client
):
    from bronto.client import BrontoResponseException

    mock_bronto_client.list_monitors.return_value = [
        {"id": "m1", "name": "broken", "metric_id": "def-1"}
    ]
    mock_bronto_client.list_metric_definitions.return_value = [
        {"id": "def-1", "queries": [{"from": ["log-1"]}]}
    ]
    mock_bronto_client.run_search.side_effect = BrontoResponseException("bad filter")
    out = monitor_tools.validate_monitors()
    assert out["results"][0]["status"] == "error"
    assert "query failed" in out["results"][0]["reason"]
