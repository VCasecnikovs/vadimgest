"""Tests for Codex session ingestion."""

import json
import os
import sqlite3
from datetime import datetime, timezone

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


def test_codex_syncer_extracts_legacy_top_level_rows(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "archived_sessions" / "rollout-2025-09-03T18-25-49-legacy.jsonl"
    _write_jsonl(session, [
        {"id": "legacy", "timestamp": "2025-09-03T18:25:49.259Z", "git": {"branch": "main"}},
        {"record_type": "state"},
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<environment_context>\nignored\n</environment_context>"}]},
        {"record_type": "state"},
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Real historical prompt"}]},
        {"type": "reasoning", "encrypted_content": "SECRET-REASONING"},
        {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Historical answer"}]},
        {"type": "function_call", "name": "shell", "call_id": "call_1", "arguments": "{\"command\":\"ls\"}"},
        {"type": "function_call_output", "call_id": "call_1", "output": "{\"output\":\"ok\"}"},
    ])

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": True,
        "include_sqlite_metadata": False,
    })

    records = syncer._records_from_session_file(session)

    assert len(records) == 1
    record = records[0]
    assert record["id"] == "codex_legacy_line_3_3"
    assert record["created_at"] == "2025-09-03T18:25:49.259Z"
    assert record["user_messages"] == [{"text": "Real historical prompt", "line": 5, "images": 0}]
    assert record["assistant_messages"][0]["text"] == "Historical answer"
    assert record["tool_calls"][0]["name"] == "shell"
    assert "SECRET-REASONING" not in json.dumps(record)


def test_codex_syncer_tolerates_invalid_utf8_bytes(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    session.parent.mkdir(parents=True, exist_ok=True)
    session.write_bytes(
        b'{"type":"session_meta","payload":{"id":"thr_1"}}\n'
        b'{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn_1"}}\n'
        b'{"type":"event_msg","payload":{"type":"user_message","message":"bad '
        b'\xe2 byte"}}\n'
    )

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
    })

    records = syncer._records_from_session_file(session)

    assert len(records) == 1
    assert records[0]["user_messages"][0]["text"] == "bad � byte"


def test_codex_syncer_compresses_before_truncating(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    long_prompt = "raw-long-message " * 50
    _write_jsonl(session, [
        {"type": "session_meta", "payload": {"id": "thr_1"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": long_prompt}},
    ])

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
        "compress_long_messages": True,
        "compression_min_chars": 1,
        "max_user_chars": 20,
    })

    class Result:
        messages = [{"role": "user", "content": "compressed prompt"}]
        tokens_before = 100
        tokens_after = 10
        tokens_saved = 90
        compression_ratio = 0.9
        transforms_applied = ["test"]

    seen = {}

    def fake_compress(messages):
        seen["content"] = messages[0]["content"]
        return Result()

    syncer._compress_messages_with_headroom = fake_compress

    record = syncer._records_from_session_file(session)[0]

    assert seen["content"] == long_prompt.strip()
    assert record["user_messages"][0]["text"] == "compressed prompt"
    assert record["meta"]["compression"]["compressed_messages"] == 1
    assert record["meta"]["compression"]["tokens_saved"] == 90


def test_codex_syncer_compression_failure_falls_back_to_truncation(tmp_path):
    codex_dir = tmp_path / ".codex"
    session = codex_dir / "sessions" / "2026" / "06" / "10" / "rollout.jsonl"
    long_prompt = "raw-long-message " * 50
    _write_jsonl(session, [
        {"type": "session_meta", "payload": {"id": "thr_1"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": long_prompt}},
    ])

    syncer = CodexSyncer(DataStore(tmp_path / "store"), {
        "codex_dir": str(codex_dir),
        "include_archived": False,
        "include_sqlite_metadata": False,
        "compress_long_messages": True,
        "compression_min_chars": 1,
        "max_user_chars": 20,
    })

    def broken_compress(messages):
        raise RuntimeError("headroom unavailable")

    syncer._compress_messages_with_headroom = broken_compress

    record = syncer._records_from_session_file(session)[0]

    assert record["user_messages"][0]["text"] == long_prompt[:20] + "\n[truncated]"
    assert "RuntimeError: headroom unavailable" in record["meta"]["compression"]["error"]


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


def test_fetch_new_backfills_old_unseen_archived_sessions(tmp_path):
    codex_dir = tmp_path / ".codex"
    archived = codex_dir / "archived_sessions" / "rollout-2025-09-03T18-25-49-old.jsonl"
    _write_jsonl(archived, [
        {"type": "session_meta", "payload": {"id": "old"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Historical prompt"}},
    ])
    old_mtime = datetime(2025, 9, 3, 18, 25, 49, tzinfo=timezone.utc).timestamp()
    os.utime(archived, (old_mtime, old_mtime))

    store = DataStore(tmp_path / "store")
    syncer = CodexSyncer(store, {
        "codex_dir": str(codex_dir),
        "include_archived": True,
        "include_sqlite_metadata": False,
    })
    state = SourceState(last_ts=datetime(2026, 6, 15, tzinfo=timezone.utc).isoformat())

    records = list(syncer.fetch_new(state, limit=10))

    assert len(records) == 1
    assert records[0]["id"] == "codex_old_turn_1_2"


def test_fetch_new_skips_old_archived_sessions_once_seen(tmp_path):
    codex_dir = tmp_path / ".codex"
    archived = codex_dir / "archived_sessions" / "rollout-2025-09-03T18-25-49-old.jsonl"
    _write_jsonl(archived, [
        {"type": "session_meta", "payload": {"id": "old"}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn_1"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Historical prompt"}},
    ])
    old_mtime = datetime(2025, 9, 3, 18, 25, 49, tzinfo=timezone.utc).timestamp()
    os.utime(archived, (old_mtime, old_mtime))

    store = DataStore(tmp_path / "store")
    store.append("codex", {
        "id": "codex_old_turn_1_2",
        "source_uri": f"file://{archived}#L2",
        "updated_at": "2025-09-03T18:25:49+00:00",
    })
    syncer = CodexSyncer(store, {
        "codex_dir": str(codex_dir),
        "include_archived": True,
        "include_sqlite_metadata": False,
    })
    state = SourceState(last_ts=datetime(2026, 6, 15, tzinfo=timezone.utc).isoformat())

    assert list(syncer.fetch_new(state, limit=10)) == []
