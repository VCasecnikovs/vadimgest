import json

import pytest

from vadimgest.edge import EdgeIngestError, ingest_edge_batch, normalize_edge_event, sanitize_source
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
    })

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

    resp = client.post("/api/edge/events/batch", json={"source": "imessage"})

    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False
