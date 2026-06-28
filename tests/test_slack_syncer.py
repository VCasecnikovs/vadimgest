import json
import urllib.parse
import urllib.request
from io import BytesIO
from unittest.mock import patch

import pytest

from vadimgest.ingest.sources.slack.syncer import SlackSyncer
from vadimgest.models import SourceState
from vadimgest.store import DataStore


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


@pytest.fixture
def slack_syncer(tmp_path):
    store = DataStore(tmp_path / "data")
    return SlackSyncer(store, {
        "token": "xoxp-test",
        "workspace": "testspace",
        "channels": [],
        "types": "public_channel",
        "bootstrap_days": 7,
        "page_size": 50,
        "max_channels": 10,
        "include_threads": False,
    })


def test_check_ready_accepts_config_token(tmp_path):
    with patch("vadimgest.ingest.sources.slack.syncer.get_source_config",
               return_value={"token": "xoxp-test"}), \
         patch.dict("os.environ", {}, clear=True):
        assert SlackSyncer.check_ready() == {"ok": True}


def test_fetch_new_converts_slack_messages(slack_syncer):
    history_params = []

    def fake_urlopen(req, timeout=30):
        url = req.full_url
        if "conversations.list" in url:
            return _FakeResponse({
                "ok": True,
                "channels": [{"id": "C123", "name": "general", "is_channel": True, "team": "T1"}],
                "response_metadata": {"next_cursor": ""},
            })
        if "conversations.history" in url:
            history_params.append(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
            return _FakeResponse({
                "ok": True,
                "messages": [{
                    "type": "message",
                    "user": "U123",
                    "text": "hello from Slack",
                    "ts": "1710000000.000100",
                }],
                "response_metadata": {"next_cursor": ""},
            })
        if "users.info" in url:
            return _FakeResponse({
                "ok": True,
                "user": {"name": "vadim", "profile": {"real_name": "Vadim"}},
            })
        raise AssertionError(f"unexpected Slack URL: {url}")

    with patch.object(urllib.request, "urlopen", side_effect=fake_urlopen):
        records = list(slack_syncer.fetch_new(SourceState(), limit=10))

    assert len(records) == 1
    rec = records[0]
    assert rec["id"] == "slack_T1_C123_1710000000_000100"
    assert rec["type"] == "slack_message"
    assert rec["channel"] == "general"
    assert rec["sender"] == "Vadim"
    assert rec["text"] == "hello from Slack"
    assert rec["timestamp"] == "2024-03-09T16:00:00.000100+00:00"
    assert history_params[0]["oldest"][0].isdigit()
