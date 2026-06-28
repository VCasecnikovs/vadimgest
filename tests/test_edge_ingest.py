import json

import pytest

from vadimgest.edge import (
    EdgeAuthError,
    EdgeIngestError,
    create_edge_token,
    ingest_edge_batch,
    list_edge_tokens,
    normalize_edge_event,
    revoke_edge_token,
    sanitize_source,
    verify_edge_token,
)
from vadimgest.edge_agent import EdgeAgent
from vadimgest.store import DataStore
from vadimgest.web.app import create_app


@pytest.fixture
def store(tmp_path):
    return DataStore(tmp_path / "data")


def test_sanitize_source_rejects_empty():
    with pytest.raises(EdgeIngestError):
        sanitize_source("")


def test_normalize_edge_event_derives_stable_id_from_source_uri():
    source, record = normalize_edge_event({
        "source": "iMessage",
        "source_uri": "imessage://chat/a/message/1",
        "text": "send proposal",
    })

    assert source == "imessage"
    assert record["id"].startswith("edge_")
    assert record["source_uri"] == "imessage://chat/a/message/1"
    assert record["text"] == "send proposal"
    assert record["type"] == "edge_event"


def test_normalize_edge_event_preserves_full_record_fields():
    source, record = normalize_edge_event({
        "source": "browser",
        "id": "tab-1",
        "url": "https://example.com",
        "title": "Example",
        "nested": {"kept": True},
    })

    assert source == "browser"
    assert record["url"] == "https://example.com"
    assert record["title"] == "Example"
    assert record["nested"] == {"kept": True}


def test_ingest_edge_batch_is_idempotent(store):
    payload = {
        "device_id": "macbook-vadim",
        "source": "imessage",
        "events": [
            {
                "source_uri": "imessage://chat/a/message/1",
                "observed_at": "2026-05-16T10:00:00+00:00",
                "actor": "Alice",
                "text": "Can you send the proposal?",
            }
        ],
    }

    first = ingest_edge_batch(store, payload)
    second = ingest_edge_batch(store, payload)

    assert first.accepted == 1
    assert first.skipped == 0
    assert second.accepted == 0
    assert second.skipped == 1
    assert store.count("imessage") == 1

    row = json.loads((store.sources_dir / "imessage.jsonl").read_text().strip())
    assert row["actor"] == "Alice"
    assert row["edge"]["device_id"] == "macbook-vadim"


def test_web_edge_batch_endpoint(store):
    app = create_app(store)
    app.config["TESTING"] = True
    client = app.test_client()
    issued = create_edge_token("test mac", store.base_path)

    resp = client.post("/api/edge/events/batch", json={
        "device_id": "macbook-vadim",
        "events": [
            {
                "source": "dayflow",
                "id": "dayflow-1",
                "observed_at": "2026-05-16T11:00:00+00:00",
                "text": "Focused work in Codex",
                "privacy": {"raw_uploaded": False, "redaction": "summary"},
            }
        ],
    }, headers={"Authorization": f"Bearer {issued.token}"})

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["accepted"] == 1
    assert data["records"][0]["source"] == "dayflow"
    assert store.exists("dayflow", "dayflow-1")


def test_web_edge_batch_rejects_invalid_payload(store):
    app = create_app(store)
    app.config["TESTING"] = True
    client = app.test_client()

    issued = create_edge_token("test mac", store.base_path)
    resp = client.post(
        "/api/edge/events/batch",
        json={"source": "imessage"},
        headers={"Authorization": f"Bearer {issued.token}"},
    )

    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False


def test_web_edge_batch_requires_token(store):
    app = create_app(store)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/api/edge/events/batch", json={"events": []})

    assert resp.status_code == 401
    assert resp.get_json()["ok"] is False


def test_edge_token_generation_verify_and_revoke(store):
    issued = create_edge_token("macbook", store.base_path)

    assert issued.token.startswith("vg_edge_")
    assert "hash" not in issued.metadata
    assert list_edge_tokens(store.base_path)[0]["label"] == "macbook"
    assert verify_edge_token(issued.token, store.base_path)["label"] == "macbook"
    assert revoke_edge_token(issued.metadata["id"], store.base_path) is True

    with pytest.raises(EdgeAuthError):
        verify_edge_token(issued.token, store.base_path)


def test_edge_agent_upload_advances_checkpoint_only_on_success(store):
    store.append("local", {"id": "r1", "type": "document", "title": "One"})
    store.append("local", {"id": "r2", "type": "document", "title": "Two"})
    calls = []

    def transport(url, token, payload, timeout):
        calls.append(payload)
        return 200, {
            "ok": True,
            "accepted": len(payload["events"]),
            "skipped": 0,
            "errors": [],
            "records": [{"index": i, "status": "accepted"} for i in range(len(payload["events"]))],
        }

    agent = EdgeAgent(
        store,
        {"enabled": True, "server_url": "https://server.test", "device_id": "mac", "batch_size": 100, "sources": ["local"]},
        token="secret",
        transport=transport,
    )
    agent.selected_sources = lambda: ["local"]
    agent._sync_source = lambda source: (0, None)

    result = agent.run_once().to_dict()
    second = agent.run_once().to_dict()

    assert result["ok"] is True
    assert result["sources"][0]["uploaded"] == 2
    assert result["sources"][0]["checkpoint"] == 2
    assert second["sources"][0]["pending"] == 0
    assert len(calls) == 1


def test_edge_agent_persists_last_run_summary(store):
    store.append("local", {"id": "r1", "type": "document", "title": "One"})

    def transport(url, token, payload, timeout):
        return 200, {
            "ok": True,
            "accepted": len(payload["events"]),
            "skipped": 0,
            "errors": [],
            "records": [{"index": i, "status": "accepted"} for i in range(len(payload["events"]))],
        }

    agent = EdgeAgent(
        store,
        {"enabled": True, "server_url": "https://server.test", "device_id": "mac", "batch_size": 100, "sources": ["local"]},
        token="secret",
        transport=transport,
    )
    agent.selected_sources = lambda: ["local"]
    agent._sync_source = lambda source: (0, None)

    result = agent.run_once().to_dict()
    last_run = json.loads((store.base_path / "edge_state.json").read_text())["last_run"]

    assert result["ok"] is True
    assert last_run["ok"] is True
    assert last_run["started_at"]
    assert last_run["finished_at"]
    assert "duration_sec" in last_run
    assert last_run["totals"]["uploaded"] == 1
    assert last_run["uploaded_total"] == 1
    assert last_run["totals"]["skipped"] == 0
    assert last_run["totals"]["failed"] == 0
    assert last_run["totals"]["pending"] == 0
    assert last_run["sources"][0]["source"] == "local"
    assert last_run["sources"][0]["checkpoint"] == 1


def test_edge_agent_keeps_pending_records_after_network_failure(store):
    store.append("local", {"id": "r1", "type": "document"})

    def transport(url, token, payload, timeout):
        raise RuntimeError("network down")

    agent = EdgeAgent(
        store,
        {"enabled": True, "server_url": "https://server.test", "device_id": "mac", "batch_size": 100, "sources": ["local"]},
        token="secret",
        transport=transport,
    )
    agent.selected_sources = lambda: ["local"]
    agent._sync_source = lambda source: (0, None)

    result = agent.run_once().to_dict()

    assert result["ok"] is False
    assert result["sources"][0]["checkpoint"] == 0
    assert result["sources"][0]["pending"] == 1
    last_run = json.loads((store.base_path / "edge_state.json").read_text())["last_run"]
    assert last_run["ok"] is False
    assert last_run["totals"]["error_count"] == 1
    assert last_run["sources"][0]["error"] == "network down"


def test_edge_agent_persists_fatal_config_error(store):
    agent = EdgeAgent(
        store,
        {"enabled": True, "server_url": "", "device_id": "mac", "batch_size": 100, "sources": ["local"]},
        token="",
    )

    result = agent.run_once().to_dict()
    last_run = json.loads((store.base_path / "edge_state.json").read_text())["last_run"]

    assert result["ok"] is False
    assert result["error"] == "edge.server_url is required"
    assert last_run["error"] == "edge.server_url is required"
    assert last_run["totals"]["error_count"] == 1


def test_web_edge_agent_status_returns_observability(store, monkeypatch):
    import vadimgest.web.app as appmod

    (store.base_path / "edge_state.json").write_text(json.dumps({
        "sources": {},
        "last_run": {
            "ok": False,
            "finished_at": "2026-06-28T20:00:00+00:00",
            "totals": {"uploaded": 0, "skipped": 0, "failed": 0, "pending": 0, "error_count": 1},
            "sources": [],
            "error": "edge.server_url is required",
        },
    }))
    monkeypatch.setattr(appmod, "get_edge_config", lambda: {
        "enabled": False,
        "server_url": "",
        "device_id": "mac",
        "interval_seconds": 300,
        "batch_size": 100,
        "sources": None,
        "token_configured": False,
    })

    app = create_app(store)
    app.config["TESTING"] = True
    data = app.test_client().get("/api/edge/agent").get_json()

    assert data["last_run"]["error"] == "edge.server_url is required"
    assert "edge config disabled" in data["config_issues"]
    assert "edge.server_url is required" in data["config_issues"]
    assert "VADIMGEST_EDGE_TOKEN is required" in data["config_issues"]
    assert data["service_hint"]["manager"]


def test_edge_autostart_launchd_is_separate_from_dashboard_services(tmp_path, monkeypatch):
    from vadimgest.web import autostart

    monkeypatch.setattr(autostart.sys, "platform", "darwin")
    monkeypatch.setattr(autostart.Path, "home", lambda: tmp_path)
    calls = []
    monkeypatch.setattr(autostart.subprocess, "run", lambda cmd, capture_output=True: calls.append(cmd))

    autostart.install_edge(interval=123)

    plist = tmp_path / "Library" / "LaunchAgents" / "com.vadimgest.edge-agent.plist"
    assert plist.exists()
    text = plist.read_text()
    assert "edge-agent" in text
    assert "com.vadimgest.dashboard" not in text
    assert "com.vadimgest.daemon" not in text
    assert autostart.is_edge_installed() is True
    assert autostart.is_installed() is False
