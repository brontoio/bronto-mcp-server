import asyncio
from unittest.mock import Mock

from bronto.client import BrontoClient
from mcp.server.fastmcp import FastMCP
from tools.registry import register_tools


def _tools():
    mcp = FastMCP("test")
    register_tools(mcp, Mock(spec=BrontoClient))
    return {t.name: t for t in asyncio.run(mcp.list_tools())}


def test_registers_all_tools():
    assert list(_tools()) == [
        "search_logs",
        "timeseries",
        "get_datasets",
        "get_datasets_by_name",
        "get_keys",
        "get_all_datasets_keys",
        "get_key_values",
        "create_monitor",
        "get_saved_searches",
        "create_saved_search",
        "run_saved_search",
        "search_monitors",
        "get_error_summary",
        "get_usage",
        "get_coverage",
        "validate_monitors",
    ]


def test_create_monitor_schema_restricts_comparison_operator():
    schema = _tools()["create_monitor"].inputSchema["properties"]["comparison_operator"]
    assert schema["enum"] == ["ABOVE", "BELOW", "ABOVE_OR_EQUAL", "BELOW_OR_EQUAL"]
