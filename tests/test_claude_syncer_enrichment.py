"""Regression tests for enriched Claude session ingestion."""

import json
from datetime import datetime

from vadimgest.ingest.sources.claude.syncer import ClaudeSyncer
from vadimgest.store import DataStore


def test_claude_syncer_captures_assistant_tools_errors_and_source_uri(tmp_path):
    index_dir = tmp_path / "projects" / "repo"
    index_dir.mkdir(parents=True)
    session_file = index_dir / "sess1.jsonl"
    rows = [
        {"type": "user", "message": {"content": "Hello Claude"}},
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "I will check."},
            {"type": "tool_use", "id": "tool_1", "name": "Bash", "input": {"command": "ls"}},
        ]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "tool_1", "is_error": True, "content": "Error: nope"}
        ]}},
    ]
    session_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    syncer = ClaudeSyncer(DataStore(tmp_path / "store"), {"projects_dir": str(tmp_path / "projects")})
    record = syncer._session_to_record({
        "entry": {
            "sessionId": "sess1",
            "firstPrompt": "Hello Claude",
            "created": "2026-06-10T00:00:00Z",
            "modified": "2026-06-10T00:01:00Z",
        },
        "project_path": "/repo",
        "index_dir": index_dir,
        "modified": datetime(2026, 6, 10),
    })

    assert record["source_uri"].endswith("sess1.jsonl#L1")
    assert [m["role"] for m in record["messages"]] == ["user", "assistant"]
    assert record["tool_calls"][0]["name"] == "Bash"
    assert record["errors"] == ["Error: nope"]
    assert record["meta"]["assistant_message_count"] == 1
