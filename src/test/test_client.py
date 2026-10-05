from bronto.client import BrontoClient


def test_client_escapes_ids_in_url_paths(monkeypatch):
    client = BrontoClient("key", "https://api.example.com")
    paths = []
    monkeypatch.setattr(
        client, "_request", lambda method, url_path, **kw: paths.append(url_path) or {}
    )
    client.get_saved_searches("abc/../../users")
    client.get_monitor_events("m?x#y")
    assert paths == [
        "saved-searches/abc%2F..%2F..%2Fusers",
        "monitors/m%3Fx%23y/events",
    ]


def test_get_top_keys_returns_empty_when_dataset_has_no_keys(monkeypatch):
    import io
    import json as _json

    class FakeResponse(io.BytesIO):
        status = 200
        reason = "OK"

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda _req: FakeResponse(_json.dumps({"other-log": {}}).encode()),
    )
    client = BrontoClient("key", "https://api.example.com")
    assert client.get_top_keys("log-without-keys") == {}


def test_list_metric_definitions_reads_definitions_key(monkeypatch):
    client = BrontoClient("key", "https://api.example.com")
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, url_path, **kw: {
            "definitions": [{"id": "def-1", "queries": []}]
        },
    )
    assert client.list_metric_definitions() == [{"id": "def-1", "queries": []}]


def test_request_error_includes_bronto_details(monkeypatch):
    import io
    from urllib.error import HTTPError

    import pytest

    from bronto.client import BrontoResponseException

    def not_found(_req):
        body = io.BytesIO(
            b'{"code": 404, "details": "One of the selected log ids does not exist"}'
        )
        raise HTTPError("https://api.example.com/search", 404, "Not Found", {}, body)

    monkeypatch.setattr("urllib.request.urlopen", not_found)
    client = BrontoClient("key", "https://api.example.com")
    with pytest.raises(
        BrontoResponseException, match="status 404. One of the selected log ids"
    ):
        client.run_search({"from": ["gone"]})


def test_bad_request_error_includes_bronto_details(monkeypatch):
    import io
    from urllib.error import HTTPError

    import pytest

    from bronto.client import BrontoResponseException

    def bad_request(_req):
        body = io.BytesIO(
            b'{"code": 400, "details": "Syntax error in expression: {{from}}"}'
        )
        raise HTTPError("https://api.example.com/search", 400, "Bad Request", {}, body)

    monkeypatch.setattr("urllib.request.urlopen", bad_request)
    client = BrontoClient("key", "https://api.example.com")
    with pytest.raises(
        BrontoResponseException, match="malformed. Check the parameters. Syntax error"
    ):
        client.run_search({"from_expr": "{{from}}"})
