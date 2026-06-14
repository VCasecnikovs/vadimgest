"""Tests for Codex session ingestion."""

import json
import sqlite3

from vadimgest.ingest.sources.codex.syncer import CodexSyncer
from vadimgest.models import SourceState
from vadimgest.store import DataStore


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def test_codex_syncer_extracts_turns_without_encrypted_reasoning(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    _write_jsonl(session, [
        {"type": "session_meta", "payload": {"id": "thr_1", "timestamp": "2026-06-10T00:00:00Z", "cwd": "/repo", "model_provider": "openai"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1", "started_at": 1781049600}},
        {"type": "turn_context", "payload": {"turn_id": "turn_1", "cwd": "/repo", "model": "gpt-5.5", "git": {"branch": "main"}}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Please inspect this"}},
        {"type": "response_item", "payload": {"type": "reasoning", "encrypted_content": "SECRET-REASONING"}},
        {"type": "response_item", "payload": {"type": "function_call", "name": "exec_command", "call_id": "call_1", "arguments": "{\"cmd\":\"ls\"}"}},
        {"type": "response_item", "payload": {"type": "function_call_output", "call_id": "call_1", "output": "file.py\n"}},
        {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn_1", "last_agent_message": "Done"}},
    ])

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
    })

    records = syncer._records_from_session_file(session)

    assert len(records) == 1
    record = records[0]
    assert record["id"] == "codex_thr_1_turn_1_2"
    assert record["type"] == "agent_turn"
    assert record["source_uri"].endswith("rollout.jsonl#L2")
    assert record["user_messages"][0]["text"] == "Please inspect this"
    assert record["assistant_messages"][-1]["text"] == "Done"
    assert record["tool_calls"][0]["name"] == "exec_command"
    assert "SECRET-REASONING" not in json.dumps(record)


def test_codex_syncer_enriches_sqlite_thread_metadata(tmp_path):
    codex_dir = tmp_path / ".codex"
    db = codex_dir / "state_5.sqlite"
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.execute(
        "create table threads (id text, created_at text, updated_at text, source text, "
        "model_provider text, cwd text, title text, git_branch text, model text, "
        "thread_source text, preview text)"
    )
    con.execute(
        "insert into threads values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("thr_1", "2026-06-10T00:00:00Z", "2026-06-10T00:02:00Z", "vscode",
         "openai", "/repo", "Thread title", "main", "gpt-5.5", "user", "preview"),
    )
    con.execute("create table thread_spawn_edges (parent_thread_id text, child_thread_id text)")
    con.execute("insert into thread_spawn_edges values (?, ?)", ("parent_1", "thr_1"))
    con.commit()
    con.close()

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {"codex_dir": str(codex_dir)})
    metadata = syncer._load_metadata()

    assert metadata["threads"]["thr_1"]["title"] == "Thread title"
    assert metadata["parents"]["thr_1"] == ["parent_1"]


def test_codex_syncer_normalizes_epoch_sqlite_timestamps(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    _write_jsonl(session, [
        {"type": "session_meta", "payload": {"id": "thr_1"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Hi"}},
    ])

    db = codex_dir / "state_5.sqlite"
    con = sqlite3.connect(db)
    con.execute(
        "create table threads (id text, created_at integer, updated_at integer, source text, "
        "model_provider text, cwd text, title text, git_branch text, model text, "
        "thread_source text, preview text)"
    )
    con.execute(
        "insert into threads values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("thr_1", 1781049600, 1781049720, "vscode",
         "openai", "/repo", "Thread title", "main", "gpt-5.5", "user", "preview"),
    )
    con.execute("create table thread_spawn_edges (parent_thread_id text, child_thread_id text)")
    con.commit()
    con.close()

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {"codex_dir": str(codex_dir)})
    record = syncer._records_from_session_file(session, syncer._load_metadata())[0]

    assert record["created_at"] == "2026-06-10T00:00:00+00:00"
    assert record["updated_at"] == "2026-06-10T00:02:00+00:00"


def test_codex_syncer_parses_numeric_state_timestamp(tmp_path):
    codex_dir = tmp_path / ".codex"
    syncer = CodexSyncer(DataStore(tmp_path / "store"), {"codex_dir": str(codex_dir)})

    assert syncer._parse_ts(1781049600).isoformat() == "2026-06-10T00:00:00+00:00"
    assert syncer._parse_ts("1781049600").isoformat() == "2026-06-10T00:00:00+00:00"


def test_codex_syncer_dedups_by_stable_turn_id(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    _write_jsonl(session, [
        {"type": "session_meta", "payload": {"id": "thr_1"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Hi"}},
    ])

    store = DataStore(tmp_path / "store")
    syncer = CodexSyncer(store, {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
    })

    first_count, _ = syncer.sync()
    second_count, _ = syncer.sync()

    assert first_count == 1
    assert second_count == 0
    records = list(store.read_all("codex"))
    assert records[0].data["id"] == "codex_thr_1_turn_1_2"


def test_fetch_new_ignores_session_index_jsonl(tmp_path):
    codex_dir = tmp_path / ".codex"
    _write_jsonl(codex_dir / "sessions" / "index" / "by-dir" / "repo.jsonl", [
        {"id": "not_a_transcript", "title": "Index only"},
    ])
    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
    })

    assert list(syncer.fetch_new(SourceState(), limit=10)) == []
