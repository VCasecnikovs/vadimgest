"""Tests for vadimgest/consumer/reader.py - ConsumerReader checkpoint-based reading."""

import json
import pytest
from pathlib import Path

from vadimgest.store import DataStore
from vadimgest.consumer.reader import ConsumerReader
from vadimgest.models import Record, ConsumerCheckpoint


@pytest.fixture
def store(tmp_path):
    """Create a DataStore with temp directory."""
    return DataStore(tmp_path / "data")


@pytest.fixture
def reader(store):
    """Create a ConsumerReader."""
    return ConsumerReader(store)


def _add_records(store, source, n):
    """Helper to add n records to a source."""
    for i in range(n):
        store.append(source, {"id": f"rec-{i}", "type": "test", "text": f"Record {i}"})


def _add_chat_messages(store, source, chat, n, start=0):
    """Helper to add n chat messages for a specific chat."""
    for i in range(start, start + n):
        store.append(source, {
            "id": f"{chat}-msg-{i}",
            "type": "message",
            "chat": chat,
            "sender": "Alice",
            "text": f"Message {i}",
            "timestamp": f"2026-01-01T10:{i:02d}:00Z",
        })


class TestGetCheckpoint:
    def test_fresh_checkpoint(self, reader):
        cp = reader.get_checkpoint("heartbeat")
        assert cp.consumer == "heartbeat"
        assert cp.positions == {}
        assert cp.updated_at == ""

    def test_loads_existing_checkpoint(self, reader, store):
        # Manually write a checkpoint file
        cp_data = {
            "consumer": "heartbeat",
            "positions": {"telegram": {"line": 5, "id": "rec-4"}},
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        cp_file = store.checkpoints_dir / "heartbeat.json"
        cp_file.write_text(json.dumps(cp_data))

        cp = reader.get_checkpoint("heartbeat")
        assert cp.consumer == "heartbeat"
        assert cp.positions["telegram"]["line"] == 5
        assert cp.updated_at == "2026-01-01T00:00:00+00:00"

    def test_different_consumers_independent(self, reader, store):
        # Write checkpoint for consumer A
        _add_records(store, "telegram", 3)
        reader.commit("telegram", "consumer_a")

        # Consumer B should have empty checkpoint
        cp_b = reader.get_checkpoint("consumer_b")
        assert cp_b.positions == {}


class TestReadNew:
    def test_reads_all_when_no_checkpoint(self, reader, store):
        _add_records(store, "telegram", 5)
        records = list(reader.read_new("telegram", "heartbeat"))
        assert len(records) == 5

    def test_reads_only_new_after_commit(self, reader, store):
        _add_records(store, "telegram", 3)
        reader.commit("telegram", "heartbeat")

        # Add 2 more
        _add_records(store, "telegram", 2)

        records = list(reader.read_new("telegram", "heartbeat"))
        assert len(records) == 2

    def test_no_new_records(self, reader, store):
        _add_records(store, "telegram", 3)
        reader.commit("telegram", "heartbeat")

        records = list(reader.read_new("telegram", "heartbeat"))
        assert len(records) == 0

    def test_empty_source(self, reader):
        records = list(reader.read_new("nonexistent", "heartbeat"))
        assert len(records) == 0

    def test_multiple_sources_independent(self, reader, store):
        _add_records(store, "telegram", 3)
        _add_records(store, "signal", 5)

        # Commit only telegram
        reader.commit("telegram", "heartbeat")

        tg_records = list(reader.read_new("telegram", "heartbeat"))
        sig_records = list(reader.read_new("signal", "heartbeat"))
        assert len(tg_records) == 0
        assert len(sig_records) == 5


class TestCommit:
    def test_commit_advances_checkpoint(self, reader, store):
        _add_records(store, "telegram", 5)
        reader.commit("telegram", "heartbeat")

        cp = reader.get_checkpoint("heartbeat")
        assert cp.positions["telegram"]["line"] == 5
        assert cp.updated_at  # should be set

    def test_commit_with_explicit_line(self, reader, store):
        _add_records(store, "telegram", 10)
        reader.commit("telegram", "heartbeat", line=7, record_id="rec-6")

        cp = reader.get_checkpoint("heartbeat")
        assert cp.positions["telegram"]["line"] == 7
        assert cp.positions["telegram"]["id"] == "rec-6"

    def test_commit_preserves_other_sources(self, reader, store):
        _add_records(store, "telegram", 3)
        _add_records(store, "signal", 5)

        reader.commit("telegram", "heartbeat")
        reader.commit("signal", "heartbeat")

        cp = reader.get_checkpoint("heartbeat")
        assert "telegram" in cp.positions
        assert "signal" in cp.positions

    def test_commit_updates_timestamp(self, reader, store):
        _add_records(store, "telegram", 1)
        reader.commit("telegram", "heartbeat")

        cp = reader.get_checkpoint("heartbeat")
        assert cp.updated_at != ""


class TestCommitAll:
    def test_commits_all_sources(self, reader, store):
        _add_records(store, "telegram", 3)
        _add_records(store, "signal", 5)

        reader.commit_all("heartbeat")

        cp = reader.get_checkpoint("heartbeat")
        assert cp.positions["telegram"]["line"] == 3
        assert cp.positions["signal"]["line"] == 5

    def test_commit_all_empty(self, reader, store):
        # No sources should be fine
        reader.commit_all("heartbeat")
        cp = reader.get_checkpoint("heartbeat")
        assert cp.positions == {}

    def test_commit_all_then_read_new(self, reader, store):
        _add_records(store, "telegram", 3)
        reader.commit_all("heartbeat")

        records = list(reader.read_new("telegram", "heartbeat"))
        assert len(records) == 0

        # Add more and read
        _add_records(store, "telegram", 2)
        records = list(reader.read_new("telegram", "heartbeat"))
        assert len(records) == 2


class TestReadWithContext:
    def test_returns_context_for_chat_source(self, reader, store):
        _add_chat_messages(store, "telegram", "ChatA", 5)
        reader.commit("telegram", "hb")
        _add_chat_messages(store, "telegram", "ChatA", 2, start=5)

        ctx, new = reader.read_with_context("telegram", "hb", context=3)
        assert len(new) == 2
        assert len(ctx) == 3
        assert all(r.data["chat"] == "ChatA" for r in ctx)

    def test_context_zero_returns_empty_context(self, reader, store):
        _add_chat_messages(store, "telegram", "ChatA", 3)
        reader.commit("telegram", "hb")
        _add_chat_messages(store, "telegram", "ChatA", 2, start=3)

        ctx, new = reader.read_with_context("telegram", "hb", context=0)
        assert ctx == []
        assert len(new) == 2

    def test_no_context_for_non_chat_source(self, reader, store):
        """Non-chat sources (gmail, etc.) skip context entirely."""
        for i in range(5):
            store.append("gmail", {"id": f"email-{i}", "type": "email", "chat": "inbox"})
        reader.commit("gmail", "hb")
        store.append("gmail", {"id": "email-5", "type": "email", "chat": "inbox"})

        ctx, new = reader.read_with_context("gmail", "hb", context=3)
        assert ctx == []
        assert len(new) == 1

    def test_context_only_for_chats_with_new_messages(self, reader, store):
        """Only fetch context for chats that have new messages, not all chats."""
        _add_chat_messages(store, "telegram", "ChatA", 5)
        _add_chat_messages(store, "telegram", "ChatB", 5, start=10)
        reader.commit("telegram", "hb")
        _add_chat_messages(store, "telegram", "ChatA", 1, start=5)

        ctx, new = reader.read_with_context("telegram", "hb", context=3)
        assert len(new) == 1
        assert new[0].data["chat"] == "ChatA"
        assert all(r.data["chat"] == "ChatA" for r in ctx)

    def test_context_capped_per_chat(self, reader, store):
        """Context returns at most N records per chat."""
        _add_chat_messages(store, "telegram", "ChatA", 20)
        reader.commit("telegram", "hb")
        _add_chat_messages(store, "telegram", "ChatA", 1, start=20)

        ctx, new = reader.read_with_context("telegram", "hb", context=3)
        assert len(ctx) == 3
        assert ctx[0].data["text"] == "Message 17"
        assert ctx[1].data["text"] == "Message 18"
        assert ctx[2].data["text"] == "Message 19"

    def test_context_with_multiple_chats(self, reader, store):
        _add_chat_messages(store, "signal", "GroupX", 4)
        _add_chat_messages(store, "signal", "GroupY", 4, start=10)
        reader.commit("signal", "hb")
        _add_chat_messages(store, "signal", "GroupX", 1, start=4)
        _add_chat_messages(store, "signal", "GroupY", 1, start=14)

        ctx, new = reader.read_with_context("signal", "hb", context=2)
        assert len(new) == 2
        ctx_chats = {r.data["chat"] for r in ctx}
        assert ctx_chats == {"GroupX", "GroupY"}
        per_chat = {}
        for r in ctx:
            per_chat.setdefault(r.data["chat"], []).append(r)
        assert len(per_chat["GroupX"]) == 2
        assert len(per_chat["GroupY"]) == 2

    def test_no_context_when_no_checkpoint(self, reader, store):
        """Fresh consumer with no checkpoint - no older data to look back."""
        _add_chat_messages(store, "telegram", "ChatA", 3)
        ctx, new = reader.read_with_context("telegram", "hb", context=5)
        assert ctx == []
        assert len(new) == 3

    def test_no_new_records_returns_empty(self, reader, store):
        _add_chat_messages(store, "telegram", "ChatA", 3)
        reader.commit("telegram", "hb")

        ctx, new = reader.read_with_context("telegram", "hb", context=3)
        assert ctx == []
        assert new == []

    def test_context_fewer_than_requested(self, reader, store):
        """If chat has fewer old messages than context size, return what exists."""
        _add_chat_messages(store, "telegram", "ChatA", 2)
        reader.commit("telegram", "hb")
        _add_chat_messages(store, "telegram", "ChatA", 1, start=2)

        ctx, new = reader.read_with_context("telegram", "hb", context=10)
        assert len(ctx) == 2
        assert len(new) == 1
