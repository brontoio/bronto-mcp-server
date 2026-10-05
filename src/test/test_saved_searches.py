from unittest.mock import Mock

import pytest
from bronto.client import BrontoClient
from tools.saved_searches import SavedSearchTools


@pytest.fixture
def mock_bronto_client():
    return Mock(spec=BrontoClient)


@pytest.fixture
def saved_search_tools(mock_bronto_client):
    return SavedSearchTools(mock_bronto_client)


def test_get_saved_searches_filters_by_query(saved_search_tools, mock_bronto_client):
    mock_bronto_client.get_saved_searches.return_value = {
        "saved_searches": [
            {"id": "a", "name": "cdn errors"},
            {"id": "b", "name": "billing"},
        ]
    }
    out = saved_search_tools.get_saved_searches(query="cdn")
    assert out["total"] == 1 and out["saved_searches"][0]["id"] == "a"


def test_get_saved_searches_accepts_bare_list_response(
    saved_search_tools, mock_bronto_client
):
    mock_bronto_client.get_saved_searches.return_value = [
        {"id": "a", "name": "cdn errors"}
    ]
    out = saved_search_tools.get_saved_searches()
    assert out == {
        "saved_searches": [{"id": "a", "name": "cdn errors"}],
        "total": 1,
        "shown": 1,
        "truncated": False,
    }


def test_get_saved_searches_limit_reports_shown_and_truncated(
    saved_search_tools, mock_bronto_client
):
    mock_bronto_client.get_saved_searches.return_value = {
        "saved_searches": [{"id": "a"}, {"id": "b"}]
    }
    out = saved_search_tools.get_saved_searches(limit=1)
    assert out == {
        "saved_searches": [{"id": "a"}],
        "total": 2,
        "shown": 1,
        "truncated": True,
    }


def test_create_saved_search_builds_search_details(
    saved_search_tools, mock_bronto_client
):
    mock_bronto_client.create_saved_search.return_value = {"id": "ss-1"}
    out = saved_search_tools.create_saved_search(
        name="q",
        log_ids=["log-1"],
        search_filter="status=500",
        time_range="Last 1 hour",
    )
    assert out == {"saved_search": {"id": "ss-1"}}
    body = mock_bronto_client.create_saved_search.call_args[0][0]
    assert body["name"] == "q"
    assert body["log_ids"] == ["log-1"]
    assert body["search_details"] == {
        "from": "log-1",
        "select": "*,@raw",
        "where": "status=500",
        "time_range": "Last 1 hour",
    }


def test_create_saved_search_joins_multiple_log_ids_into_string(
    saved_search_tools, mock_bronto_client
):
    saved_search_tools.create_saved_search(name="q", log_ids=["log-1", "log-2"])
    body = mock_bronto_client.create_saved_search.call_args[0][0]
    assert body["log_ids"] == ["log-1", "log-2"]
    assert body["search_details"]["from"] == "log-1,log-2"


def test_create_saved_search_dry_run_returns_body_without_creating(
    saved_search_tools, mock_bronto_client
):
    out = saved_search_tools.create_saved_search(
        name="q", log_ids=["log-1"], dry_run=True
    )
    assert out == {
        "dry_run": True,
        "saved_search_body": {
            "name": "q",
            "log_ids": ["log-1"],
            "search_details": {"from": "log-1", "select": "*,@raw"},
        },
    }
    mock_bronto_client.create_saved_search.assert_not_called()


def test_run_saved_search_maps_details_to_search_body(
    saved_search_tools, mock_bronto_client
):
    mock_bronto_client.get_saved_searches.return_value = {
        "id": "ss-1",
        "name": "raw",
        "search_details": {
            "logIds": "log-1,log-2",
            "select": "count(*)",
            "groups": "path",
            "timeRange": "Last 2 hours",
        },
    }
    mock_bronto_client.run_search.return_value = {
        "groups_series": [{"name": "/x", "count": 3}]
    }
    out = saved_search_tools.run_saved_search("ss-1")
    assert out == {"groups_series": [{"name": "/x", "count": 3}]}
    body = mock_bronto_client.run_search.call_args[0][0]
    assert body["from"] == ["log-1", "log-2"]
    assert body["select"] == ["count(*)"] and body["groups"] == ["path"]
    assert body["time_range"] == "Last 2 hours"


def test_run_saved_search_trims_events_like_search_logs(
    saved_search_tools, mock_bronto_client
):
    mock_bronto_client.get_saved_searches.return_value = {
        "id": "ss-1",
        "search_details": {"select": "*,@raw", "timeRange": "Last 20 minutes"},
        "log_ids": ["log-1"],
    }  # UI-created: datasets only at the top level
    event = {
        "@raw": "x",
        "@time": "t",
        "@status": "info",
        "message_kvs": {"a": "1"},
        "attributes": {},
        "metadata": {"sequence": 1},
        "links": [{"rel": "context"}],
    }
    mock_bronto_client.run_search.return_value = {
        "result": [event],
        "events": [event],
        "answer": [],
        "groups_series": [],
        "totals": {},
        "explain": {"Matching events": "1"},
        "links": [{"rel": "next"}],
        "metadata": {},
    }
    out = saved_search_tools.run_saved_search("ss-1", limit=5)
    assert out == {
        "events": [
            {"@raw": "x", "@time": "t", "@status": "info", "message_kvs": {"a": "1"}}
        ],
        "explain": {"Matching events": "1"},
        "links": [{"rel": "next"}],
    }
    body = mock_bronto_client.run_search.call_args[0][0]
    assert body["from"] == ["log-1"] and body["limit"] == 5
