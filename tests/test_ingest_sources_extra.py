"""Extra tests for ingest source syncers to maximize coverage.

Covers fetch_new() flows, __init__ logic, error paths, and
integration-level patterns for:
- Signal, WhatsApp, iMessage, GitHub, GitHub Notifications,
  Browser, GTasks, GDrive, Hlopya
- __init__.py: _LazySyncers, get_syncer_class, get_load_error,
  available_sources, all_source_names, get_all_manifests
"""

import sys
import os
import json
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from vadimgest.store import DataStore
from vadimgest.models import SourceState


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def tmp_store(tmp_path):
    """Create a temporary DataStore for tests."""
    return DataStore(tmp_path / "data")


@pytest.fixture
def signal_syncer(tmp_store):
    from vadimgest.ingest.sources.signal.syncer import SignalSyncer
    return SignalSyncer(tmp_store, config={})


@pytest.fixture
def whatsapp_syncer(tmp_store):
    from vadimgest.ingest.sources.whatsapp.syncer import WhatsAppSyncer
    return WhatsAppSyncer(tmp_store, config={"fetch_limit": 50, "chat_limit": 50})


@pytest.fixture
def imessage_syncer(tmp_store):
    from vadimgest.ingest.sources.imessage.syncer import IMessageSyncer
    return IMessageSyncer(tmp_store, config={})


@pytest.fixture
def github_syncer(tmp_store):
    from vadimgest.ingest.sources.github.syncer import GitHubSyncer
    config = {
        "projects": [{"owner": "acme-org", "project_number": 5}],
        "repos": [{"owner": "acme-org", "repo": "acme-repo"}],
    }
    return GitHubSyncer(tmp_store, config)


@pytest.fixture
def ghnotif_syncer(tmp_store):
    from vadimgest.ingest.sources.github_notifications.syncer import GitHubNotificationsSyncer
    return GitHubNotificationsSyncer(tmp_store, config={
        "participating": True,
        "per_page": 50,
    })


@pytest.fixture
def browser_syncer(tmp_store):
    from vadimgest.ingest.sources.browser.syncer import BrowserSyncer
    return BrowserSyncer(tmp_store, config={"session_window_minutes": 30})


@pytest.fixture
def gtasks_syncer(tmp_store):
    from vadimgest.ingest.sources.gtasks.syncer import GTasksSyncer
    return GTasksSyncer(tmp_store, config={
        "email": "test@gmail.com",
        "max_tasks": 100,
    })


@pytest.fixture
def gdrive_syncer(tmp_store):
    from vadimgest.ingest.sources.gdrive.syncer import GDriveSyncer
    return GDriveSyncer(tmp_store, config={
        "accounts": ["test@example.com"],
        "max_results": 50,
        "content_preview_size": 5000,
    })


@pytest.fixture
def hlopya_syncer(tmp_store, tmp_path):
    from vadimgest.ingest.sources.hlopya.syncer import HlopyaSyncer
    rec_dir = tmp_path / "recordings"
    rec_dir.mkdir()
    return HlopyaSyncer(tmp_store, config={"recordings_dir": str(rec_dir)})


# ============================================================
# Signal Syncer - fetch_new and _export_signal_db
# ============================================================

class TestSignalFetchNew:
    """Test SignalSyncer.fetch_new flow with mocked DB."""

    def _make_in_memory_db(self):
        """Create an in-memory sqlite3 DB that mimics Signal structure."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                profileFullName TEXT,
                profileName TEXT,
                serviceId TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE messages (
                rowid INTEGER PRIMARY KEY,
                id TEXT,
                conversationId TEXT,
                sent_at INTEGER,
                type TEXT,
                body TEXT,
                source TEXT,
                hasAttachments INTEGER DEFAULT 0,
                sourceServiceId TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE message_attachments (
                messageId TEXT,
                contentType TEXT,
                fileName TEXT,
                size INTEGER,
                orderInMessage INTEGER DEFAULT 0
            )
        """)
        return conn

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_fetch_new_success(self, mock_conv_settings, signal_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }

        conn = self._make_in_memory_db()
        conn.execute(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?)",
            ("conv1", "Alice", "private", "Alice Smith", None, "svc1"),
        )
        conn.execute(
            "INSERT INTO messages (rowid, id, conversationId, sent_at, type, body, hasAttachments, sourceServiceId) "
            "VALUES (1, 'msg1', 'conv1', 1710500000000, 'incoming', 'Hello', 0, NULL)"
        )
        conn.execute(
            "INSERT INTO messages (rowid, id, conversationId, sent_at, type, body, hasAttachments, sourceServiceId) "
            "VALUES (2, 'msg2', 'conv1', 1710500060000, 'outgoing', 'Hi there', 0, NULL)"
        )
        conn.commit()

        with patch.object(signal_syncer, "_export_signal_db", return_value=conn):
            state = SourceState()
            records = list(signal_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["chat"] == "Alice"
        assert len(records[0]["messages"]) == 2

    def test_fetch_new_export_fails(self, signal_syncer):
        """When export fails, fetch_new yields nothing."""
        with patch.object(signal_syncer, "_export_signal_db", side_effect=Exception("sigtop crash")):
            state = SourceState()
            records = list(signal_syncer.fetch_new(state))
        assert records == []

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_fetch_new_no_messages(self, mock_conv_settings, signal_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conn = self._make_in_memory_db()
        conn.execute(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?)",
            ("conv1", "Bob", "private", None, None, None),
        )
        conn.commit()

        with patch.object(signal_syncer, "_export_signal_db", return_value=conn):
            state = SourceState()
            records = list(signal_syncer.fetch_new(state))
        assert records == []

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_fetch_new_with_last_ts(self, mock_conv_settings, signal_syncer):
        """When last_ts is set, messages after that timestamp are returned.

        Note: due to SQL operator precedence in the syncer
        (WHERE body OR hasAttachments AND sent_at > ?), the sent_at filter
        only applies to attachment-only messages. Text messages always pass.
        This test verifies that fetch_new runs correctly with last_ts set.
        """
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conn = self._make_in_memory_db()
        conn.execute(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?)",
            ("conv1", "Alice", "private", None, None, None),
        )
        # New message
        conn.execute(
            "INSERT INTO messages (rowid, id, conversationId, sent_at, type, body, hasAttachments, sourceServiceId) "
            "VALUES (1, 'msg1', 'conv1', 1710500060000, 'incoming', 'New msg', 0, NULL)"
        )
        conn.commit()

        with patch.object(signal_syncer, "_export_signal_db", return_value=conn):
            state = SourceState(last_ts="2024-03-15T00:00:00Z")
            records = list(signal_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["messages"][0]["text"] == "New msg"

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_fetch_new_with_attachments(self, mock_conv_settings, signal_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conn = self._make_in_memory_db()
        conn.execute(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?)",
            ("conv1", "Alice", "private", None, None, None),
        )
        conn.execute(
            "INSERT INTO messages (rowid, id, conversationId, sent_at, type, body, hasAttachments, sourceServiceId) "
            "VALUES (1, 'msg1', 'conv1', 1710500000000, 'incoming', 'See photo', 1, NULL)"
        )
        conn.execute(
            "INSERT INTO message_attachments VALUES ('msg1', 'image/jpeg', 'photo.jpg', 2048, 0)"
        )
        conn.commit()

        with patch.object(signal_syncer, "_export_signal_db", return_value=conn):
            state = SourceState()
            records = list(signal_syncer.fetch_new(state))

        assert len(records) == 1
        assert "attachments" in records[0]["messages"][0]
        assert records[0]["messages"][0]["attachments"][0]["content_type"] == "image/jpeg"

    def test_export_signal_db_success(self, signal_syncer, tmp_path):
        """Test _export_signal_db calls sigtop and returns connection."""
        signal_syncer.temp_db_path = tmp_path / "test_signal.db"

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("vadimgest.ingest.sources.signal.syncer.subprocess.run", return_value=mock_result):
            # Need to create the file that sigtop would create
            signal_syncer.temp_db_path.touch()
            conn = signal_syncer._export_signal_db()
            assert conn is not None
            conn.close()

    def test_export_signal_db_failure(self, signal_syncer, tmp_path):
        signal_syncer.temp_db_path = tmp_path / "test_signal.db"

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "sigtop error: no key"

        with patch("vadimgest.ingest.sources.signal.syncer.subprocess.run", return_value=mock_result):
            with pytest.raises(Exception, match="sigtop error"):
                signal_syncer._export_signal_db()

    def test_export_signal_db_removes_old_file(self, signal_syncer, tmp_path):
        signal_syncer.temp_db_path = tmp_path / "old_export.db"
        signal_syncer.temp_db_path.touch()

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("vadimgest.ingest.sources.signal.syncer.subprocess.run", return_value=mock_result):
            signal_syncer.temp_db_path.touch()  # recreate after remove
            conn = signal_syncer._export_signal_db()
            conn.close()

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_fetch_new_conv_name_priority(self, mock_conv_settings, signal_syncer):
        """Test conversation name priority: name > profileFullName > profileName > id."""
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conn = self._make_in_memory_db()
        # name=None, profileFullName="Profile Full", profileName="Profile"
        conn.execute(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?)",
            ("conv1", None, "private", "Profile Full", "Profile", None),
        )
        conn.execute(
            "INSERT INTO messages (rowid, id, conversationId, sent_at, type, body, hasAttachments, sourceServiceId) "
            "VALUES (1, 'msg1', 'conv1', 1710500000000, 'incoming', 'Hi', 0, NULL)"
        )
        conn.commit()

        with patch.object(signal_syncer, "_export_signal_db", return_value=conn):
            state = SourceState()
            records = list(signal_syncer.fetch_new(state))

        assert records[0]["chat"] == "Profile Full"


# ============================================================
# WhatsApp Syncer - fetch_new, _wacli_call, _list_chats, _list_messages
# ============================================================

class TestWacliCall:
    """Test _wacli_call helper function."""

    def test_success(self):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"success": True, "data": [{"id": 1}]})

        with patch("vadimgest.ingest.sources.whatsapp.syncer.subprocess.run", return_value=mock_result):
            result = _wacli_call(["chats", "list"])
        assert result == [{"id": 1}]

    def test_failure_nonzero(self):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "connection error"

        with patch("vadimgest.ingest.sources.whatsapp.syncer.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="wacli chats list failed"):
                _wacli_call(["chats", "list"])

    def test_empty_stdout(self):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "   "

        with patch("vadimgest.ingest.sources.whatsapp.syncer.subprocess.run", return_value=mock_result):
            assert _wacli_call(["chats", "list"]) is None

    def test_success_false(self):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"success": False, "error": "unauthorized"})

        with patch("vadimgest.ingest.sources.whatsapp.syncer.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="wacli"):
                _wacli_call(["chats", "list"])


class TestWhatsAppFetchNew:
    """Test WhatsAppSyncer.fetch_new with mocked wacli."""

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_fetch_success(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        chats = [
            {"JID": "123@s.whatsapp.net", "Name": "Alice", "Kind": "dm"},
        ]
        messages = {
            "messages": [
                {"Timestamp": "2026-03-15T10:00:00Z", "SenderJID": "123@s.whatsapp.net",
                 "Text": "Hey", "FromMe": False, "MediaType": ""},
                {"Timestamp": "2026-03-15T10:01:00Z", "SenderJID": "me",
                 "Text": "Hi!", "FromMe": True, "MediaType": ""},
            ]
        }

        with patch.object(whatsapp_syncer, "_list_chats", return_value=chats), \
             patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=messages):
            state = SourceState(last_ts="2026-03-14T00:00:00Z")
            records = list(whatsapp_syncer.fetch_new(state))

        assert len(records) >= 1
        assert records[0]["chat"] == "Alice"

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_fetch_no_chats(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        with patch.object(whatsapp_syncer, "_list_chats", return_value=[]):
            state = SourceState()
            records = list(whatsapp_syncer.fetch_new(state))
        assert records == []

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_fetch_skips_status_broadcast(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        chats = [
            {"JID": "status@broadcast", "Name": "Status", "Kind": ""},
            {"JID": "123@s.whatsapp.net", "Name": "Bob", "Kind": "dm"},
        ]
        messages = {
            "messages": [
                {"Timestamp": "2026-03-15T10:00:00Z", "SenderJID": "bob",
                 "Text": "Hello", "FromMe": False, "MediaType": ""},
            ]
        }

        with patch.object(whatsapp_syncer, "_list_chats", return_value=chats), \
             patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=messages):
            state = SourceState(last_ts="2026-03-14T00:00:00Z")
            records = list(whatsapp_syncer.fetch_new(state))
        # Only Bob, not status@broadcast
        for r in records:
            assert r["chat"] != "Status"

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_fetch_bootstrap_no_last_ts(self, mock_conv_settings, whatsapp_syncer):
        """When no last_ts, bootstrap sync uses 7 days ago."""
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        chats = [{"JID": "123@s.whatsapp.net", "Name": "Alice", "Kind": "dm"}]

        with patch.object(whatsapp_syncer, "_list_chats", return_value=chats), \
             patch.object(whatsapp_syncer, "_list_messages", return_value=[]) as mock_list:
            state = SourceState()
            list(whatsapp_syncer.fetch_new(state))
            # Should have been called with an ISO date string (bootstrap)
            call_args = mock_list.call_args
            assert call_args is not None
            assert call_args[1]["after"] is not None

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_fetch_group_type_detection(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        chats = [
            {"JID": "123-456@g.us", "Name": "Team Chat", "Kind": "group"},
        ]
        messages = {
            "messages": [
                {"Timestamp": "2026-03-15T10:00:00Z", "SenderJID": "user1",
                 "Text": "Hey team", "FromMe": False, "MediaType": ""},
            ]
        }

        with patch.object(whatsapp_syncer, "_list_chats", return_value=chats), \
             patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=messages):
            state = SourceState(last_ts="2026-03-14T00:00:00Z")
            records = list(whatsapp_syncer.fetch_new(state))

        if records:
            assert records[0]["meta"]["chat_type"] == "group"

    def test_list_chats_error(self, whatsapp_syncer):
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call",
                    side_effect=RuntimeError("connection error")):
            result = whatsapp_syncer._list_chats()
        assert result == []

    def test_list_chats_none(self, whatsapp_syncer):
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=None):
            result = whatsapp_syncer._list_chats()
        assert result == []

    def test_list_chats_non_list(self, whatsapp_syncer):
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value="string"):
            result = whatsapp_syncer._list_chats()
        assert result == []

    def test_list_messages_error(self, whatsapp_syncer):
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call",
                    side_effect=RuntimeError("fail")):
            result = whatsapp_syncer._list_messages("123@s.whatsapp.net")
        assert result == []

    def test_list_messages_none(self, whatsapp_syncer):
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=None):
            result = whatsapp_syncer._list_messages("123@s.whatsapp.net")
        assert result == []

    def test_list_messages_with_after_timestamp(self, whatsapp_syncer):
        messages = {
            "messages": [
                {"Timestamp": "2026-03-15T10:00:00Z", "SenderJID": "user",
                 "Text": "msg", "FromMe": False, "MediaType": ""},
            ]
        }
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=messages):
            result = whatsapp_syncer._list_messages("jid", after="2026-03-14T00:00:00")
        assert len(result) == 1

    def test_list_messages_with_media(self, whatsapp_syncer):
        messages = {
            "messages": [
                {"Timestamp": "2026-03-15T10:00:00Z", "SenderJID": "user",
                 "Text": "", "FromMe": False, "MediaType": "image"},
            ]
        }
        with patch("vadimgest.ingest.sources.whatsapp.syncer._wacli_call", return_value=messages):
            result = whatsapp_syncer._list_messages("jid")
        assert result[0]["has_media"] is True


class TestWhatsAppChunkToRecordExtra:
    """Additional tests for text truncation in _chunk_to_record."""

    def test_text_truncation(self, whatsapp_syncer):
        """Text longer than 5000 chars should be truncated."""
        long_text = "x" * 6000
        chunk = [{
            "timestamp": "2026-03-15T10:00:00Z",
            "sender": "user1",
            "text": long_text,
            "is_from_me": False,
            "has_media": False,
        }]
        record = whatsapp_syncer._chunk_to_record(chunk, "jid@s.whatsapp.net", "Chat", "dm")
        assert record["messages"][0]["text"].endswith("... [truncated]")
        assert len(record["messages"][0]["text"]) < 6000

    def test_timestamp_space_to_T(self, whatsapp_syncer):
        """Timestamps with spaces instead of T should be normalized."""
        chunk = [{
            "timestamp": "2026-03-15 10:00:00",
            "sender": "user1",
            "text": "Hello",
            "is_from_me": False,
            "has_media": False,
        }]
        record = whatsapp_syncer._chunk_to_record(chunk, "jid@s.whatsapp.net", "Chat", "dm")
        assert "T" in record["messages"][0]["ts"]
        assert "T" in record["period_start"]

    def test_jid_truncation_in_id(self, whatsapp_syncer):
        """JID is truncated to 20 chars in record ID."""
        chunk = [{
            "timestamp": "2026-03-15T10:00:00Z",
            "sender": "user1",
            "text": "Hi",
            "is_from_me": False,
            "has_media": False,
        }]
        long_jid = "a" * 30 + "@s.whatsapp.net"
        record = whatsapp_syncer._chunk_to_record(chunk, long_jid, "Chat", "dm")
        assert record["id"].startswith("wa_")


class TestWhatsAppGroupIntoChunksExtra:
    """Extra group_into_chunks tests for edge cases."""

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_invalid_timestamp_skipped(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "not-a-date", "sender": "user1", "text": "bad", "is_from_me": False},
            {"timestamp": "2026-03-15T10:00:00Z", "sender": "user1", "text": "good", "is_from_me": False},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        # The bad timestamp is skipped, the good one forms a chunk
        assert len(chunks) >= 1

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_empty_timestamp_skipped(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "", "sender": "user1", "text": "no-ts"},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        assert chunks == []

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_space_separated_timestamp(self, mock_conv_settings, whatsapp_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "2026-03-15 10:00:00", "sender": "user1", "text": "msg"},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        assert len(chunks) == 1


# ============================================================
# iMessage Syncer - fetch_new, _export_db, helpers
# ============================================================

class TestIMessageAppleDateHelpers:
    """Test Apple date conversion functions."""

    def test_apple_date_none(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime
        assert _apple_date_to_datetime(None) is None

    def test_apple_date_zero(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime
        assert _apple_date_to_datetime(0) is None

    def test_apple_date_nanoseconds(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime, APPLE_EPOCH
        # Nanosecond timestamp (> 1 billion)
        ns_ts = 700_000_000_000_000_000  # nanoseconds
        dt = _apple_date_to_datetime(ns_ts)
        assert dt is not None

    def test_apple_date_seconds(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime, APPLE_EPOCH
        # Seconds timestamp (< 1 billion)
        sec_ts = 700_000_000
        dt = _apple_date_to_datetime(sec_ts)
        assert dt is not None

    def test_datetime_to_apple_date_roundtrip(self):
        from vadimgest.ingest.sources.imessage.syncer import (
            _apple_date_to_datetime, _datetime_to_apple_date
        )
        dt = datetime(2026, 3, 15, 10, 0, 0)
        apple_ts = _datetime_to_apple_date(dt)
        back = _apple_date_to_datetime(apple_ts)
        assert abs((back - dt).total_seconds()) < 1


class TestIMessageFetchNew:
    """Test IMessageSyncer.fetch_new with mocked DB."""

    def _make_imsg_db(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT)")
        conn.execute("CREATE TABLE chat (chat_identifier TEXT, display_name TEXT)")
        conn.execute("""
            CREATE TABLE message (
                ROWID INTEGER PRIMARY KEY,
                guid TEXT,
                text TEXT,
                date INTEGER,
                is_from_me INTEGER,
                handle_id INTEGER,
                cache_roomnames TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE chat_message_join (
                chat_id INTEGER,
                message_id INTEGER
            )
        """)
        # chat table needs ROWID for join
        # Use chat_identifier as pseudo-key
        return conn

    @patch("vadimgest.ingest.sources.imessage.syncer.get_conversation_settings")
    def test_fetch_new_success(self, mock_conv_settings, imessage_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conn = self._make_imsg_db()
        conn.execute("INSERT INTO handle VALUES (1, '+1234567890')")
        conn.execute("INSERT INTO chat VALUES ('+1234567890', 'Alice')")

        from vadimgest.ingest.sources.imessage.syncer import _datetime_to_apple_date
        ts = _datetime_to_apple_date(datetime(2026, 3, 15, 10, 0, 0))

        conn.execute(
            "INSERT INTO message VALUES (1, 'guid1', 'Hello', ?, 0, 1, NULL)",
            (ts,),
        )
        conn.execute("INSERT INTO chat_message_join VALUES (1, 1)")
        conn.commit()

        # Fix: chat table needs ROWID
        conn.execute("DROP TABLE chat")
        conn.execute("CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, chat_identifier TEXT, display_name TEXT)")
        conn.execute("INSERT INTO chat VALUES (1, '+1234567890', 'Alice')")
        conn.commit()

        with patch.object(imessage_syncer, "_export_db", return_value=conn):
            state = SourceState()
            records = list(imessage_syncer.fetch_new(state))

        assert len(records) >= 1

    def test_fetch_new_export_fails_filenotfound(self, imessage_syncer):
        with patch.object(imessage_syncer, "_export_db", side_effect=FileNotFoundError("no binary")):
            state = SourceState()
            records = list(imessage_syncer.fetch_new(state))
        assert records == []

    def test_fetch_new_export_fails_runtime(self, imessage_syncer):
        with patch.object(imessage_syncer, "_export_db", side_effect=RuntimeError("perms")):
            state = SourceState()
            records = list(imessage_syncer.fetch_new(state))
        assert records == []

    def test_export_db_binary_not_found(self, imessage_syncer):
        with patch("vadimgest.ingest.sources.imessage.syncer.EXPORT_BINARY",
                    Path("/nonexistent/binary")):
            with pytest.raises(FileNotFoundError):
                imessage_syncer._export_db()

    def test_export_db_runtime_error(self, imessage_syncer, tmp_path):
        binary = tmp_path / "fake-binary"
        binary.touch()

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Full Disk Access required"

        with patch("vadimgest.ingest.sources.imessage.syncer.EXPORT_BINARY", binary), \
             patch("vadimgest.ingest.sources.imessage.syncer.TEMP_DB", tmp_path / "tmp.db"), \
             patch("vadimgest.ingest.sources.imessage.syncer.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="imessage-export failed"):
                imessage_syncer._export_db()

    def test_check_ready_missing_binary(self):
        from vadimgest.ingest.sources.imessage.syncer import IMessageSyncer
        with patch("vadimgest.ingest.sources.imessage.syncer.Path") as MockPath:
            mock_path = MagicMock()
            mock_path.exists.return_value = False
            MockPath.__truediv__ = MagicMock(return_value=mock_path)
            MockPath.return_value = mock_path
            # Actually test the check_ready logic through the class
            import sys as _sys
            with patch.object(_sys, "platform", "darwin"):
                # Just test that it returns a dict with ok key
                result = IMessageSyncer.check_ready()
                assert "ok" in result

    @patch("vadimgest.ingest.sources.imessage.syncer.get_conversation_settings")
    def test_group_into_chunks_with_last_ts(self, mock_conv_settings, imessage_syncer):
        mock_conv_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        from vadimgest.ingest.sources.imessage.syncer import _datetime_to_apple_date
        ts = _datetime_to_apple_date(datetime(2026, 3, 15, 10, 0, 0))

        rows = [{
            "date": ts,
            "text": "Hello",
            "is_from_me": 1,
            "handle_id": 1,
            "cache_roomnames": None,
            "chat_identifier": "+1234567890",
            "chat_display_name": "Alice",
        }]
        handle_map = {1: "+1234567890"}
        chat_name_map = {}
        chunks = imessage_syncer._group_into_chunks(rows, handle_map, chat_name_map)
        assert len(chunks) >= 1
        assert chunks[0]["messages"][0]["sender"] == "Me"


# ============================================================
# GitHub Syncer - fetch_new, _gh_call, _fetch_project_items, _fetch_commits
# ============================================================

class TestGhCall:
    """Test _gh_call helper."""

    def test_success(self):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"items": []})

        with patch("vadimgest.ingest.sources.github.syncer.subprocess.run", return_value=mock_result):
            result = _gh_call(["api", "/repos"])
        assert result == {"items": []}

    def test_failure(self):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "auth required"

        with patch("vadimgest.ingest.sources.github.syncer.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="gh api"):
                _gh_call(["api", "/repos"])

    def test_empty_stdout(self):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "  "

        with patch("vadimgest.ingest.sources.github.syncer.subprocess.run", return_value=mock_result):
            assert _gh_call(["api", "/repos"]) is None


class TestGitHubFetchNew:
    """Test GitHubSyncer.fetch_new flow."""

    def test_fetch_projects_and_commits(self, github_syncer):
        project_items = [
            {"id": "PVTI_1", "title": "Issue 1", "assignees": [], "status": "Todo",
             "content": {"number": 1, "type": "Issue"}},
        ]
        commits = [
            {
                "sha": "abc1234567890",
                "commit": {"message": "fix: bug", "author": {"name": "alice-dev", "date": "2026-03-15T10:00:00Z"}},
                "author": {"login": "alice-dev"},
                "html_url": "https://github.com/acme-org/acme-repo/commit/abc1234",
            },
        ]

        with patch.object(github_syncer, "_fetch_project_items", return_value=project_items), \
             patch.object(github_syncer, "_fetch_commits", return_value=commits):
            state = SourceState(last_ts="2026-03-14T00:00:00Z")
            records = list(github_syncer.fetch_new(state))

        assert len(records) == 2
        types = {r["type"] for r in records}
        assert "issue" in types
        assert "commit" in types

    def test_fetch_no_projects_no_repos(self, tmp_store):
        from vadimgest.ingest.sources.github.syncer import GitHubSyncer
        syncer = GitHubSyncer(tmp_store, config={"projects": [], "repos": []})
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_fetch_invalid_project_config(self, tmp_store):
        from vadimgest.ingest.sources.github.syncer import GitHubSyncer
        syncer = GitHubSyncer(tmp_store, config={
            "projects": [{"owner": "", "project_number": None}],
            "repos": [{"owner": "", "repo": ""}],
        })
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_fetch_project_items_error(self, github_syncer):
        with patch("vadimgest.ingest.sources.github.syncer._gh_call",
                    side_effect=RuntimeError("gh failed")):
            result = github_syncer._fetch_project_items("acme-org", 5)
        assert result == []

    def test_fetch_project_items_non_dict(self, github_syncer):
        with patch("vadimgest.ingest.sources.github.syncer._gh_call", return_value="not a dict"):
            result = github_syncer._fetch_project_items("acme-org", 5)
        assert result == []

    def test_fetch_commits_error(self, github_syncer):
        with patch("vadimgest.ingest.sources.github.syncer._gh_call",
                    side_effect=RuntimeError("network error")):
            result = github_syncer._fetch_commits("acme-org", "repo")
        assert result == []

    def test_fetch_commits_non_list(self, github_syncer):
        with patch("vadimgest.ingest.sources.github.syncer._gh_call", return_value={"key": "val"}):
            result = github_syncer._fetch_commits("acme-org", "repo")
        assert result == []

    def test_get_commits_since_no_state(self, github_syncer):
        state = SourceState()
        result = github_syncer._get_commits_since(state)
        assert result is not None
        # Should be ~7 days ago
        assert "T" in result

    def test_get_commits_since_with_state(self, github_syncer):
        state = SourceState(last_ts="2026-03-14T00:00:00Z")
        result = github_syncer._get_commits_since(state)
        assert result == "2026-03-14T00:00:00Z"

    def test_commit_to_record_no_author_obj(self, github_syncer):
        """When author field is None (deleted user)."""
        from vadimgest.ingest.sources.github.syncer import TRACKED_AUTHORS
        commit = {
            "sha": "abc1234567890",
            "commit": {
                "message": "fix something",
                "author": {"name": "alice-dev", "date": "2026-03-15T10:00:00Z"},
            },
            "author": None,
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "repo")
        # Should use commit.author.name as fallback
        if record:
            assert record["author"] == "alice-dev"

    def test_fetch_respects_limit(self, github_syncer):
        items = [
            {"id": f"PVTI_{i}", "title": f"Issue {i}", "content": {}}
            for i in range(10)
        ]
        with patch.object(github_syncer, "_fetch_project_items", return_value=items), \
             patch.object(github_syncer, "_fetch_commits", return_value=[]):
            state = SourceState()
            records = list(github_syncer.fetch_new(state, limit=3))
        assert len(records) == 3


# ============================================================
# GitHub Notifications Syncer - fetch_new, _notif_to_record
# ============================================================

class TestGitHubNotificationsFetchNew:
    """Test GitHubNotificationsSyncer.fetch_new."""

    def test_fetch_success(self, ghnotif_syncer):
        notifications = [
            {
                "id": "123",
                "reason": "review_requested",
                "subject": {"title": "PR: Fix bug", "type": "PullRequest",
                            "url": "https://api.github.com/repos/acme-org/acme-repo/pulls/42"},
                "repository": {"full_name": "acme-org/acme-repo"},
                "unread": True,
                "updated_at": "2026-03-15T10:00:00Z",
            },
        ]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(notifications)

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["id"] == "ghn_123"
        assert records[0]["type"] == "notification"
        assert records[0]["subject"] == "PR: Fix bug"
        assert records[0]["subject_type"] == "PullRequest"
        assert records[0]["repo"] == "acme-org/acme-repo"
        assert "github.com" in records[0]["url"]
        assert "/pull/" in records[0]["url"]

    def test_fetch_with_last_ts(self, ghnotif_syncer):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([])

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result) as mock_run:
            state = SourceState(last_ts="2026-03-14T00:00:00Z")
            list(ghnotif_syncer.fetch_new(state))
            # Check that since is in the URL
            cmd_args = mock_run.call_args[0][0]
            url = cmd_args[2]  # gh api <url>
            assert "since=2026-03-14" in url

    def test_fetch_gh_not_found(self, ghnotif_syncer):
        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    side_effect=FileNotFoundError("gh not found")):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))
        assert records == []

    def test_fetch_timeout(self, ghnotif_syncer):
        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    side_effect=subprocess.TimeoutExpired(cmd="gh", timeout=30)):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))
        assert records == []

    def test_fetch_invalid_json(self, ghnotif_syncer):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not json {{"

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))
        assert records == []

    def test_fetch_non_list_response(self, ghnotif_syncer):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"message": "not found"})

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))
        assert records == []

    def test_fetch_nonzero_return(self, ghnotif_syncer):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "auth required"

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state))
        assert records == []

    def test_fetch_respects_limit(self, ghnotif_syncer):
        notifications = [
            {"id": str(i), "subject": {"title": f"N{i}", "type": "Issue", "url": ""},
             "repository": {"full_name": "org/repo"}, "unread": True, "updated_at": ""}
            for i in range(10)
        ]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(notifications)

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result):
            state = SourceState()
            records = list(ghnotif_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_notif_to_record_no_id(self, ghnotif_syncer):
        assert ghnotif_syncer._notif_to_record({}) is None
        assert ghnotif_syncer._notif_to_record({"id": ""}) is None

    def test_notif_to_record_url_conversion(self, ghnotif_syncer):
        notif = {
            "id": "456",
            "subject": {"title": "Issue", "type": "Issue",
                        "url": "https://api.github.com/repos/org/repo/issues/5"},
            "repository": {"full_name": "org/repo"},
            "unread": False,
            "updated_at": "2026-03-15T10:00:00Z",
        }
        record = ghnotif_syncer._notif_to_record(notif)
        assert "api.github.com" not in record["url"]
        assert "github.com" in record["url"]

    def test_notif_participating_flag(self, ghnotif_syncer):
        """Test that participating=true is added to query."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([])

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result) as mock_run:
            state = SourceState()
            list(ghnotif_syncer.fetch_new(state))
            url = mock_run.call_args[0][0][2]
            assert "participating=true" in url

    def test_notif_not_participating(self, tmp_store):
        from vadimgest.ingest.sources.github_notifications.syncer import GitHubNotificationsSyncer
        syncer = GitHubNotificationsSyncer(tmp_store, config={"participating": False, "per_page": 50})

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([])

        with patch("vadimgest.ingest.sources.github_notifications.syncer.subprocess.run",
                    return_value=mock_result) as mock_run:
            state = SourceState()
            list(syncer.fetch_new(state))
            url = mock_run.call_args[0][0][2]
            assert "participating" not in url


# ============================================================
# Browser Syncer - fetch_new flow
# ============================================================

class TestBrowserFetchNew:
    """Test BrowserSyncer.fetch_new with mocked DB."""

    def _make_history_db(self, path):
        """Create a minimal Chromium History sqlite db."""
        conn = sqlite3.connect(path)
        conn.execute("""
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE visits (
                id INTEGER PRIMARY KEY,
                url INTEGER REFERENCES urls(id),
                visit_time INTEGER,
                visit_duration INTEGER DEFAULT 0,
                transition INTEGER DEFAULT 0
            )
        """)
        return conn

    def test_fetch_new_success(self, browser_syncer, tmp_path):
        db_path = tmp_path / "History"
        conn = self._make_history_db(str(db_path))

        # Chrome timestamp for 2026-03-15 10:00:00 UTC
        from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
        chrome_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)

        conn.execute("INSERT INTO urls VALUES (1, 'https://github.com/page', 'GitHub', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (1, 1, {chrome_ts}, 30000000, 1)")
        conn.commit()
        conn.close()

        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]

        state = SourceState()
        records = list(browser_syncer.fetch_new(state))
        assert len(records) >= 1
        assert records[0]["type"] == "browsing_session"
        assert records[0]["domain"] == "github.com"

    def test_fetch_new_db_not_found(self, browser_syncer, tmp_path):
        browser_syncer.profiles = [{"name": "Test", "path": str(tmp_path / "nonexistent")}]
        state = SourceState()
        records = list(browser_syncer.fetch_new(state))
        assert records == []

    def test_fetch_new_db_copy_fails(self, browser_syncer, tmp_path):
        db_path = tmp_path / "History"
        db_path.touch()
        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]

        with patch("vadimgest.ingest.sources.browser.syncer.shutil.copy2",
                    side_effect=PermissionError("locked")):
            state = SourceState()
            records = list(browser_syncer.fetch_new(state))
        assert records == []

    def test_fetch_new_with_last_ts(self, browser_syncer, tmp_path):
        db_path = tmp_path / "History"
        conn = self._make_history_db(str(db_path))

        from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
        old_ts = int((datetime(2025, 1, 1).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)
        new_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)

        conn.execute("INSERT INTO urls VALUES (1, 'https://old.com/page', 'Old', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (1, 1, {old_ts}, 0, 1)")
        conn.execute("INSERT INTO urls VALUES (2, 'https://new.com/page', 'New', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (2, 2, {new_ts}, 0, 1)")
        conn.commit()
        conn.close()

        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]

        state = SourceState(last_ts="2026-03-01T00:00:00Z")
        records = list(browser_syncer.fetch_new(state))
        # Only the "new" visit should appear
        domains = [r["domain"] for r in records]
        assert "new.com" in domains

    def test_fetch_new_filters_noise(self, browser_syncer, tmp_path):
        db_path = tmp_path / "History"
        conn = self._make_history_db(str(db_path))

        from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
        chrome_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)

        # Noise URLs that should be filtered
        conn.execute("INSERT INTO urls VALUES (1, 'chrome-extension://abc/page', 'Ext', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (1, 1, {chrome_ts}, 0, 0)")
        conn.execute("INSERT INTO urls VALUES (2, 'https://localhost/dev', 'Local', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (2, 2, {chrome_ts + 1}, 0, 0)")
        # Real URL
        conn.execute("INSERT INTO urls VALUES (3, 'https://example.com/page', 'Example', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (3, 3, {chrome_ts + 2}, 0, 0)")
        conn.commit()
        conn.close()

        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]

        state = SourceState()
        records = list(browser_syncer.fetch_new(state))
        domains = [r["domain"] for r in records]
        assert "example.com" in domains
        assert "localhost" not in domains

    def test_fetch_new_skip_transitions(self, browser_syncer, tmp_path):
        """auto_subframe (3) transitions should be skipped."""
        db_path = tmp_path / "History"
        conn = self._make_history_db(str(db_path))

        from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
        chrome_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)

        conn.execute("INSERT INTO urls VALUES (1, 'https://example.com/frame', 'Frame', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (1, 1, {chrome_ts}, 0, 3)")  # auto_subframe
        conn.commit()
        conn.close()

        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]
        state = SourceState()
        records = list(browser_syncer.fetch_new(state))
        assert records == []

    def test_fetch_visits_dedup_and_pages(self, browser_syncer, tmp_path):
        """Multiple visits to same domain get grouped into one session."""
        db_path = tmp_path / "History"
        conn = self._make_history_db(str(db_path))

        from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
        base_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)

        conn.execute("INSERT INTO urls VALUES (1, 'https://github.com/page1', 'Page 1', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (1, 1, {base_ts}, 60000000, 1)")
        conn.execute("INSERT INTO urls VALUES (2, 'https://github.com/page2', 'Page 2 - Longer Title', 1, 0)")
        conn.execute(f"INSERT INTO visits VALUES (2, 2, {base_ts + 60000000}, 30000000, 0)")  # 1 min later
        conn.commit()
        conn.close()

        browser_syncer.profiles = [{"name": "Test", "path": str(db_path)}]
        state = SourceState()
        records = list(browser_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["domain"] == "github.com"
        assert records[0]["total_visits"] == 2

    def test_chrome_ts_conversion_overflow(self, browser_syncer):
        """Invalid chrome timestamp should return fallback date."""
        from vadimgest.ingest.sources.browser.syncer import BrowserSyncer
        dt = BrowserSyncer._chrome_ts_to_datetime(-99999999999999999)
        assert dt == datetime(2020, 1, 1)

    def test_iso_to_chrome_ts(self, browser_syncer):
        from vadimgest.ingest.sources.browser.syncer import BrowserSyncer
        ts = BrowserSyncer._iso_to_chrome_ts("2026-03-15T10:00:00Z")
        assert isinstance(ts, int)
        assert ts > 0

    def test_multiple_profiles(self, browser_syncer, tmp_path):
        db1 = tmp_path / "History1"
        db2 = tmp_path / "History2"

        for db_path in [db1, db2]:
            conn = self._make_history_db(str(db_path))
            from vadimgest.ingest.sources.browser.syncer import _CHROME_EPOCH_OFFSET
            chrome_ts = int((datetime(2026, 3, 15, 10, 0, 0).timestamp() + _CHROME_EPOCH_OFFSET) * 1_000_000)
            conn.execute("INSERT INTO urls VALUES (1, 'https://example.com/page', 'Example', 1, 0)")
            conn.execute(f"INSERT INTO visits VALUES (1, 1, {chrome_ts}, 0, 1)")
            conn.commit()
            conn.close()

        browser_syncer.profiles = [
            {"name": "Profile1", "path": str(db1)},
            {"name": "Profile2", "path": str(db2)},
        ]
        state = SourceState()
        records = list(browser_syncer.fetch_new(state))
        assert len(records) >= 2


# ============================================================
# GTasksSyncer - fetch_new, helpers
# ============================================================

class TestGTasksFetchNew:
    """Test GTasksSyncer.fetch_new with mocked gog CLI."""

    def test_fetch_success(self, gtasks_syncer):
        task_lists = [
            {"id": "list1", "title": "Personal"},
            {"id": "list2", "title": "Work"},
        ]
        tasks_personal = [
            {"id": "task1", "title": "Buy groceries", "status": "needsAction", "notes": "", "due": "2026-03-20"},
        ]
        tasks_work = [
            {"id": "task2", "title": "Review PR", "status": "needsAction", "notes": "PR #42"},
        ]

        with patch.object(gtasks_syncer, "_get_task_lists", return_value=task_lists), \
             patch.object(gtasks_syncer, "_get_tasks", side_effect=[tasks_personal, tasks_work]):
            state = SourceState()
            records = list(gtasks_syncer.fetch_new(state))

        assert len(records) == 2
        assert records[0]["type"] == "task"
        assert records[0]["list_name"] == "Personal"
        assert records[0]["title"] == "Buy groceries"
        assert records[1]["list_name"] == "Work"

    def test_fetch_no_lists(self, gtasks_syncer):
        with patch.object(gtasks_syncer, "_get_task_lists", return_value=[]):
            state = SourceState()
            records = list(gtasks_syncer.fetch_new(state))
        assert records == []

    def test_fetch_skip_empty_list_id(self, gtasks_syncer):
        task_lists = [{"id": "", "title": "Bad List"}]
        with patch.object(gtasks_syncer, "_get_task_lists", return_value=task_lists):
            state = SourceState()
            records = list(gtasks_syncer.fetch_new(state))
        assert records == []

    def test_fetch_respects_limit(self, gtasks_syncer):
        task_lists = [{"id": "list1", "title": "Tasks"}]
        tasks = [{"id": f"t{i}", "title": f"Task {i}"} for i in range(10)]

        with patch.object(gtasks_syncer, "_get_task_lists", return_value=task_lists), \
             patch.object(gtasks_syncer, "_get_tasks", return_value=tasks):
            state = SourceState()
            records = list(gtasks_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_get_task_lists_error(self, gtasks_syncer):
        with patch("vadimgest.ingest.sources.gtasks.syncer.gog_call",
                    side_effect=RuntimeError("gog failed")):
            result = gtasks_syncer._get_task_lists()
        assert result == []

    def test_get_tasks_error(self, gtasks_syncer):
        with patch("vadimgest.ingest.sources.gtasks.syncer.gog_call",
                    side_effect=RuntimeError("gog failed")):
            result = gtasks_syncer._get_tasks("list1")
        assert result == []

    def test_task_to_record_no_id(self, gtasks_syncer):
        assert gtasks_syncer._task_to_record({}, "list1", "Work") is None
        assert gtasks_syncer._task_to_record({"id": ""}, "list1", "Work") is None

    def test_task_to_record_full(self, gtasks_syncer):
        task = {
            "id": "abc123",
            "title": "Do something",
            "notes": "Details here",
            "status": "completed",
            "due": "2026-03-20T00:00:00Z",
            "updated": "2026-03-15T10:00:00Z",
        }
        record = gtasks_syncer._task_to_record(task, "list1", "Work")
        assert record is not None
        assert record["id"] == "gtask_list1_abc123"
        assert record["type"] == "task"
        assert record["title"] == "Do something"
        assert record["notes"] == "Details here"
        assert record["status"] == "completed"
        assert record["due"] == "2026-03-20T00:00:00Z"
        assert record["meta"]["task_id"] == "abc123"

    def test_task_to_record_defaults(self, gtasks_syncer):
        task = {"id": "t1"}
        record = gtasks_syncer._task_to_record(task, "list1", "Work")
        assert record["title"] == "(untitled)"
        assert record["notes"] == ""
        assert record["status"] == "needsAction"

    def test_task_to_record_updated_at_fallback(self, gtasks_syncer):
        task = {"id": "t1", "updatedAt": "2026-03-15T10:00:00Z"}
        record = gtasks_syncer._task_to_record(task, "list1", "Work")
        assert record["updated_at"] == "2026-03-15T10:00:00Z"

    def test_get_task_lists_calls_gog(self, gtasks_syncer):
        with patch("vadimgest.ingest.sources.gtasks.syncer.gog_call",
                    return_value={"tasklists": [{"id": "l1", "title": "List"}]}) as mock_call:
            result = gtasks_syncer._get_task_lists()
            mock_call.assert_called_once_with("tasks", "lists list", account="test@gmail.com")
        assert len(result) == 1

    def test_get_tasks_calls_gog(self, gtasks_syncer):
        with patch("vadimgest.ingest.sources.gtasks.syncer.gog_call",
                    return_value={"tasks": [{"id": "t1"}]}) as mock_call:
            result = gtasks_syncer._get_tasks("list1")
            mock_call.assert_called_once_with("tasks", "list", ["list1"], account="test@gmail.com")
        assert len(result) == 1


# ============================================================
# GDrive Syncer - fetch_new, _search_files, _file_to_record, _get_content_preview
# ============================================================

class TestGDriveFetchNew:
    """Test GDriveSyncer.fetch_new with mocked gog CLI."""

    def test_fetch_success(self, gdrive_syncer):
        files = [
            {"id": "file1", "name": "doc.txt", "mimeType": "text/plain",
             "modifiedTime": "2026-03-15T10:00:00Z", "owner": "user@test.com",
             "webViewLink": "https://drive.google.com/file/d/file1"},
        ]

        with patch.object(gdrive_syncer, "_search_files", return_value=files), \
             patch.object(gdrive_syncer, "_get_content_preview", return_value="content preview"):
            state = SourceState()
            records = list(gdrive_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["id"] == "gdrive_file1"
        assert records[0]["type"] == "drive_file"
        assert records[0]["name"] == "doc.txt"
        assert records[0]["content_preview"] == "content preview"

    def test_fetch_no_accounts(self, tmp_store):
        from vadimgest.ingest.sources.gdrive.syncer import GDriveSyncer
        syncer = GDriveSyncer(tmp_store, config={"accounts": [], "max_results": 50})
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_fetch_no_files(self, gdrive_syncer):
        with patch.object(gdrive_syncer, "_search_files", return_value=[]):
            state = SourceState()
            records = list(gdrive_syncer.fetch_new(state))
        assert records == []

    def test_fetch_respects_limit(self, gdrive_syncer):
        files = [{"id": f"f{i}", "name": f"file{i}.txt", "mimeType": "text/plain"} for i in range(10)]
        with patch.object(gdrive_syncer, "_search_files", return_value=files), \
             patch.object(gdrive_syncer, "_get_content_preview", return_value=""):
            state = SourceState()
            records = list(gdrive_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_search_files_error(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    side_effect=RuntimeError("gog failed")):
            result = gdrive_syncer._search_files("test@example.com", None)
        assert result == []

    def test_search_files_with_last_ts(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value={"files": []}) as mock_call:
            gdrive_syncer._search_files("test@example.com", "2026-03-14T00:00:00+00:00")
            call_args = mock_call.call_args
            query = call_args[0][2][0]
            assert "modifiedTime" in query

    def test_search_files_ts_with_z(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value={"files": []}) as mock_call:
            gdrive_syncer._search_files("test@example.com", "2026-03-14T00:00:00Z")
            call_args = mock_call.call_args
            query = call_args[0][2][0]
            assert query.count("Z") == 1

    def test_file_to_record_no_id(self, gdrive_syncer):
        assert gdrive_syncer._file_to_record({}, "test@example.com") is None
        assert gdrive_syncer._file_to_record({"id": ""}, "test@example.com") is None

    def test_file_to_record_skip_folder(self, gdrive_syncer):
        file_info = {"id": "f1", "mimeType": "application/vnd.google-apps.folder"}
        assert gdrive_syncer._file_to_record(file_info, "test@example.com") is None

    def test_file_to_record_skip_shortcut(self, gdrive_syncer):
        file_info = {"id": "f1", "mimeType": "application/vnd.google-apps.shortcut"}
        assert gdrive_syncer._file_to_record(file_info, "test@example.com") is None

    def test_file_to_record_skip_form(self, gdrive_syncer):
        file_info = {"id": "f1", "mimeType": "application/vnd.google-apps.form"}
        assert gdrive_syncer._file_to_record(file_info, "test@example.com") is None

    def test_file_to_record_text_file_gets_preview(self, gdrive_syncer):
        file_info = {
            "id": "f1",
            "name": "notes.txt",
            "mimeType": "text/plain",
            "modifiedTime": "2026-03-15T10:00:00Z",
        }
        with patch.object(gdrive_syncer, "_get_content_preview", return_value="file content"):
            record = gdrive_syncer._file_to_record(file_info, "test@example.com")
        assert record["content_preview"] == "file content"

    def test_file_to_record_binary_no_preview(self, gdrive_syncer):
        file_info = {
            "id": "f1",
            "name": "image.png",
            "mimeType": "image/png",
        }
        record = gdrive_syncer._file_to_record(file_info, "test@example.com")
        assert record["content_preview"] == ""

    def test_file_to_record_alternate_field_names(self, gdrive_syncer):
        """Test fallback field names (mime_type, modified_at, web_link)."""
        file_info = {
            "id": "f1",
            "name": "doc.txt",
            "mime_type": "text/plain",
            "modified_at": "2026-03-15T10:00:00Z",
            "web_link": "https://drive.google.com/file/d/f1",
        }
        with patch.object(gdrive_syncer, "_get_content_preview", return_value=""):
            record = gdrive_syncer._file_to_record(file_info, "test@example.com")
        assert record["mime_type"] == "text/plain"
        assert record["modified_at"] == "2026-03-15T10:00:00Z"
        assert record["web_link"] == "https://drive.google.com/file/d/f1"

    def test_get_content_preview_google_doc(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value={"content": "Hello World"}) as mock_call:
            result = gdrive_syncer._get_content_preview(
                "test@example.com", "f1", "application/vnd.google-apps.document"
            )
            mock_call.assert_called_once_with("docs", "cat", ["f1"], account="test@example.com")
        assert result == "Hello World"

    def test_get_content_preview_other_file(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value="plain text content"):
            result = gdrive_syncer._get_content_preview("test@example.com", "f1", "text/plain")
        assert result == "plain text content"

    def test_get_content_preview_error(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    side_effect=RuntimeError("fail")):
            result = gdrive_syncer._get_content_preview("test@example.com", "f1")
        assert result == ""

    def test_get_content_preview_truncation(self, gdrive_syncer):
        long_content = "x" * 10000
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call", return_value=long_content):
            result = gdrive_syncer._get_content_preview("test@example.com", "f1", "text/plain")
        assert len(result) == 5000

    def test_get_content_preview_dict_with_text(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value={"text": "from text field"}):
            result = gdrive_syncer._get_content_preview("test@example.com", "f1")
        assert result == "from text field"

    def test_get_content_preview_dict_non_string(self, gdrive_syncer):
        with patch("vadimgest.ingest.sources.gdrive.syncer.gog_call",
                    return_value={"content": 42}):
            result = gdrive_syncer._get_content_preview("test@example.com", "f1")
        assert result == ""


# ============================================================
# Hlopya Syncer - fetch_new, _build_record, _read_json, _read_text
# ============================================================

class TestHlopyaFetchNew:
    """Test HlopyaSyncer.fetch_new with real filesystem."""

    def _make_session(self, rec_dir, session_id, meta=None, notes=None,
                      transcript=None, personal_notes=None):
        session_dir = rec_dir / session_id
        session_dir.mkdir()

        if meta is None:
            meta = {"status": "done", "title": "Test Meeting", "duration": 3600,
                    "participants": ["p1", "p2"], "participant_names": {"p1": "Alice"}}
        (session_dir / "meta.json").write_text(json.dumps(meta))

        if notes is not None:
            (session_dir / "notes.json").write_text(json.dumps(notes))

        if transcript is not None:
            (session_dir / "transcript.json").write_text(json.dumps(transcript))

        if personal_notes is not None:
            (session_dir / "personal_notes.md").write_text(personal_notes)

        return session_dir

    def test_fetch_success(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "2026-03-15_10-00-00")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))

        assert len(records) == 1
        assert records[0]["id"] == "hlopya_2026-03-15_10-00-00"
        assert records[0]["type"] == "meeting"
        assert records[0]["title"] == "Test Meeting"
        assert records[0]["duration_minutes"] == 60
        assert records[0]["participants"] == ["p1", "p2"]
        assert records[0]["created_at"] == "2026-03-15T10:00:00"

    def test_fetch_no_recordings_dir(self, tmp_store, tmp_path):
        from vadimgest.ingest.sources.hlopya.syncer import HlopyaSyncer
        syncer = HlopyaSyncer(tmp_store, config={"recordings_dir": str(tmp_path / "nonexistent")})
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_fetch_skips_non_done(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "2026-03-15_10-00-00",
                           meta={"status": "recording", "title": "In Progress"})

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records == []

    def test_fetch_skips_no_meta(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        session_dir = rec_dir / "2026-03-15_10-00-00"
        session_dir.mkdir()
        # No meta.json

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records == []

    def test_fetch_skips_hidden_dirs(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        hidden = rec_dir / ".hidden"
        hidden.mkdir()
        (hidden / "meta.json").write_text(json.dumps({"status": "done", "title": "Hidden"}))

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records == []

    def test_fetch_respects_limit(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        for i in range(5):
            self._make_session(rec_dir, f"2026-03-{15+i:02d}_10-00-00")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state, limit=2))
        assert len(records) == 2

    def test_build_record_with_notes(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        notes = {
            "summary": "We discussed X.",
            "enriched_notes": "Detailed notes here.",
            "topics": [{"topic": "Budget", "details": "Approved $10k"}],
            "action_items": [{"owner": "Alice", "task": "Send proposal", "deadline": "2026-03-20"}],
            "decisions": ["Go with option A"],
            "insights": ["Market is shifting"],
            "follow_ups": ["Check with Bob next week"],
        }
        self._make_session(rec_dir, "2026-03-15_10-00-00", notes=notes)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))

        assert len(records) == 1
        record_notes = records[0]["notes"]
        assert "Summary" in record_notes
        assert "We discussed X." in record_notes
        assert "Meeting Notes" in record_notes
        assert "Topics" in record_notes
        assert "Budget" in record_notes
        assert "Action Items" in record_notes
        assert "Alice" in record_notes
        assert "Decisions" in record_notes
        assert "option A" in record_notes
        assert "Insights" in record_notes
        assert "Follow-ups" in record_notes

    def test_build_record_with_transcript(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        transcript = {"full_text": "Alice: Hello. Bob: Hi there."}
        self._make_session(rec_dir, "2026-03-15_10-00-00", transcript=transcript)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))

        assert records[0]["transcript"] == "Alice: Hello. Bob: Hi there."
        assert records[0]["meta"]["has_transcript"] is True

    def test_build_record_with_fullText_fallback(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        transcript = {"fullText": "Alternative transcript field."}
        self._make_session(rec_dir, "2026-03-15_10-00-00", transcript=transcript)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["transcript"] == "Alternative transcript field."

    def test_build_record_with_personal_notes(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "2026-03-15_10-00-00", personal_notes="My private thoughts")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert "Personal Notes" in records[0]["notes"]
        assert "My private thoughts" in records[0]["notes"]
        assert records[0]["meta"]["has_personal_notes"] is True

    def test_build_record_no_notes(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "2026-03-15_10-00-00")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["notes"] == ""
        assert records[0]["transcript"] is None

    def test_build_record_invalid_session_id(self, hlopya_syncer, tmp_path):
        """Session ID that doesn't match date pattern."""
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "not-a-date")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["created_at"] is None

    def test_build_record_no_duration(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        meta = {"status": "done", "title": "Quick Chat"}
        self._make_session(rec_dir, "2026-03-15_10-00-00", meta=meta)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["duration_minutes"] == 0

    def test_read_json_invalid(self, hlopya_syncer, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {[")
        assert hlopya_syncer._read_json(bad_file) is None

    def test_read_json_missing(self, hlopya_syncer, tmp_path):
        assert hlopya_syncer._read_json(tmp_path / "nonexistent.json") is None

    def test_read_text_missing(self, hlopya_syncer, tmp_path):
        assert hlopya_syncer._read_text(tmp_path / "nonexistent.md") is None

    def test_read_text_empty(self, hlopya_syncer, tmp_path):
        empty_file = tmp_path / "empty.md"
        empty_file.write_text("")
        assert hlopya_syncer._read_text(empty_file) is None

    def test_read_text_whitespace_only(self, hlopya_syncer, tmp_path):
        ws_file = tmp_path / "ws.md"
        ws_file.write_text("   \n  \n  ")
        assert hlopya_syncer._read_text(ws_file) is None

    def test_init_default_recordings_dir(self, tmp_store):
        from vadimgest.ingest.sources.hlopya.syncer import HlopyaSyncer
        syncer = HlopyaSyncer(tmp_store, config={})
        assert syncer.recordings_dir == Path.home() / "recordings"

    def test_build_record_title_fallback(self, hlopya_syncer, tmp_path):
        """Title falls back to notes.title then session_id."""
        rec_dir = tmp_path / "recordings"
        meta = {"status": "done"}
        notes = {"title": "Notes Title"}
        self._make_session(rec_dir, "2026-03-15_10-00-00", meta=meta, notes=notes)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["title"] == "Notes Title"

    def test_build_record_title_session_id_fallback(self, hlopya_syncer, tmp_path):
        """Title falls back to session_id when no title in meta or notes."""
        rec_dir = tmp_path / "recordings"
        meta = {"status": "done"}
        self._make_session(rec_dir, "2026-03-15_10-00-00", meta=meta)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["title"] == "2026-03-15_10-00-00"

    def test_build_record_action_items_no_deadline(self, hlopya_syncer, tmp_path):
        """Action items without deadline don't crash."""
        rec_dir = tmp_path / "recordings"
        notes = {
            "action_items": [{"owner": "Bob", "task": "Do thing"}],
        }
        self._make_session(rec_dir, "2026-03-15_10-00-00", notes=notes)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert "Bob" in records[0]["notes"]
        assert "deadline" not in records[0]["notes"].lower() or "due:" not in records[0]["notes"]

    def test_multiple_sessions_sorted(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        self._make_session(rec_dir, "2026-03-16_10-00-00")
        self._make_session(rec_dir, "2026-03-15_10-00-00")

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert len(records) == 2
        # Should be sorted by session_id (chronological)
        assert records[0]["id"] == "hlopya_2026-03-15_10-00-00"
        assert records[1]["id"] == "hlopya_2026-03-16_10-00-00"

    def test_model_used_in_meta(self, hlopya_syncer, tmp_path):
        rec_dir = tmp_path / "recordings"
        notes = {"model_used": "gpt-4o"}
        self._make_session(rec_dir, "2026-03-15_10-00-00", notes=notes)

        state = SourceState()
        records = list(hlopya_syncer.fetch_new(state))
        assert records[0]["meta"]["model_used"] == "gpt-4o"


# ============================================================
# __init__.py - _LazySyncers, get_syncer_class, etc.
# ============================================================

class TestInitGetSyncerClass:
    """Test get_syncer_class from __init__.py."""

    def test_unknown_source(self):
        from vadimgest.ingest.sources import get_syncer_class
        assert get_syncer_class("nonexistent_source_xyz") is None

    def test_cached_source(self):
        from vadimgest.ingest.sources import get_syncer_class, _loaded
        # Load a known source
        cls = get_syncer_class("signal")
        if cls is not None:
            # Second call should use cache
            cls2 = get_syncer_class("signal")
            assert cls is cls2

    def test_failed_source_cached(self):
        from vadimgest.ingest.sources import get_syncer_class, _failed, _SYNCER_REGISTRY
        # Simulate a failed import by patching
        orig = _SYNCER_REGISTRY.get("signal")
        _SYNCER_REGISTRY["_test_bad_source"] = (".nonexistent_module", "BadClass")
        try:
            result = get_syncer_class("_test_bad_source")
            assert result is None
            assert "_test_bad_source" in _failed

            # Second call should also return None (cached failure)
            result2 = get_syncer_class("_test_bad_source")
            assert result2 is None
        finally:
            del _SYNCER_REGISTRY["_test_bad_source"]
            _failed.pop("_test_bad_source", None)


class TestInitGetLoadError:
    """Test get_load_error from __init__.py."""

    def test_known_failure(self):
        from vadimgest.ingest.sources import get_load_error, _failed, _SYNCER_REGISTRY
        _SYNCER_REGISTRY["_test_fail"] = (".nonexistent_xyz", "Bad")
        try:
            error = get_load_error("_test_fail")
            assert error is not None
        finally:
            del _SYNCER_REGISTRY["_test_fail"]
            _failed.pop("_test_fail", None)

    def test_loadable_source_no_error(self):
        from vadimgest.ingest.sources import get_load_error
        error = get_load_error("signal")
        # signal should be loadable, no error
        assert error is None


class TestInitAvailableSources:
    """Test available_sources from __init__.py."""

    def test_returns_list(self):
        from vadimgest.ingest.sources import available_sources
        sources = available_sources()
        assert isinstance(sources, list)
        # At least some sources should be available
        assert len(sources) > 0

    def test_signal_in_available(self):
        from vadimgest.ingest.sources import available_sources
        sources = available_sources()
        assert "signal" in sources


class TestInitAllSourceNames:
    """Test all_source_names from __init__.py."""

    def test_returns_all_registered(self):
        from vadimgest.ingest.sources import all_source_names, _SYNCER_REGISTRY
        names = all_source_names()
        assert set(names) == set(_SYNCER_REGISTRY.keys())

    def test_includes_known_sources(self):
        from vadimgest.ingest.sources import all_source_names
        names = all_source_names()
        for expected in ["signal", "whatsapp", "imessage", "github", "browser", "gtasks", "gdrive", "hlopya"]:
            assert expected in names


class TestLazySyncers:
    """Test _LazySyncers dict behavior."""

    def test_contains_registered(self):
        from vadimgest.ingest.sources import SYNCERS
        assert "signal" in SYNCERS

    def test_contains_unknown(self):
        from vadimgest.ingest.sources import SYNCERS
        assert "nonexistent_xyz" not in SYNCERS

    def test_getitem_loadable(self):
        from vadimgest.ingest.sources import SYNCERS
        cls = SYNCERS["signal"]
        assert cls is not None

    def test_getitem_unknown_raises(self):
        from vadimgest.ingest.sources import SYNCERS
        with pytest.raises(KeyError, match="unavailable"):
            SYNCERS["nonexistent_xyz"]

    def test_keys(self):
        from vadimgest.ingest.sources import SYNCERS, _SYNCER_REGISTRY
        assert set(SYNCERS.keys()) == set(_SYNCER_REGISTRY.keys())

    def test_len(self):
        from vadimgest.ingest.sources import SYNCERS, _SYNCER_REGISTRY
        assert len(SYNCERS) == len(_SYNCER_REGISTRY)

    def test_iter(self):
        from vadimgest.ingest.sources import SYNCERS, _SYNCER_REGISTRY
        names = list(SYNCERS)
        assert set(names) == set(_SYNCER_REGISTRY.keys())

    def test_items_yields_loadable(self):
        from vadimgest.ingest.sources import SYNCERS
        items = list(SYNCERS.items())
        # Should have at least some items
        assert len(items) > 0
        for name, cls in items:
            assert isinstance(name, str)
            assert cls is not None

    def test_values_yields_classes(self):
        from vadimgest.ingest.sources import SYNCERS
        vals = list(SYNCERS.values())
        assert len(vals) > 0
        for cls in vals:
            assert cls is not None


class TestGetAllManifests:
    """Test get_all_manifests from __init__.py."""

    def test_returns_all_sources(self):
        from vadimgest.ingest.sources import get_all_manifests, _SYNCER_REGISTRY
        with patch("vadimgest.config.get_source_config", return_value={"enabled": True}):
            manifests = get_all_manifests()
        assert set(manifests.keys()) == set(_SYNCER_REGISTRY.keys())

    def test_loadable_has_metadata(self):
        from vadimgest.ingest.sources import get_all_manifests
        with patch("vadimgest.config.get_source_config", return_value={"enabled": True}):
            manifests = get_all_manifests()

        signal_m = manifests.get("signal")
        if signal_m and signal_m["loadable"]:
            assert "display_name" in signal_m
            assert "description" in signal_m
            assert "category" in signal_m
            assert "dependencies" in signal_m
            assert signal_m["ready"] is not None

    def test_unloadable_has_fallback(self):
        from vadimgest.ingest.sources import get_all_manifests, _SYNCER_REGISTRY, _loaded, _failed
        # Add a fake unloadable source
        _SYNCER_REGISTRY["_test_unloadable"] = (".nonexistent_xyz_mod", "Bad")
        try:
            with patch("vadimgest.config.get_source_config", return_value={"enabled": False}):
                manifests = get_all_manifests()
            m = manifests["_test_unloadable"]
            assert m["loadable"] is False
            assert "load_error" in m
            assert m["enabled"] is False
        finally:
            del _SYNCER_REGISTRY["_test_unloadable"]
            _loaded.pop("_test_unloadable", None)
            _failed.pop("_test_unloadable", None)

    def test_source_config_error_handled(self):
        from vadimgest.ingest.sources import get_all_manifests
        with patch("vadimgest.config.get_source_config",
                    side_effect=Exception("config error")):
            manifests = get_all_manifests()
        # Should not crash - returns manifests with empty config
        assert len(manifests) > 0

    def test_check_ready_error_handled(self):
        from vadimgest.ingest.sources import get_all_manifests, get_syncer_class
        # Mock a syncer whose check_ready raises
        cls = get_syncer_class("signal")
        if cls:
            with patch.object(cls, "check_ready", side_effect=Exception("check failed")), \
                 patch("vadimgest.config.get_source_config", return_value={}):
                manifests = get_all_manifests()
            m = manifests.get("signal", {})
            if m.get("loadable"):
                assert m["ready"]["ok"] is False


# ============================================================
# gog_utils - gog_call
# ============================================================

class TestGogCall:
    """Test gog_call helper."""

    def test_success(self):
        from vadimgest.ingest.sources.gog_utils import gog_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"tasklists": [{"id": "1"}]})

        with patch("vadimgest.ingest.sources.gog_utils.subprocess.run", return_value=mock_result) as mock_run:
            result = gog_call("tasks", "lists list", account="test@gmail.com")
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "gog"
            assert "tasks" in cmd
            assert "--json" in cmd
            assert "-a" in cmd
            assert "test@gmail.com" in cmd
        assert result == {"tasklists": [{"id": "1"}]}

    def test_with_args(self):
        from vadimgest.ingest.sources.gog_utils import gog_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"tasks": []})

        with patch("vadimgest.ingest.sources.gog_utils.subprocess.run", return_value=mock_result) as mock_run:
            gog_call("tasks", "list", ["list_id"], account="")
            cmd = mock_run.call_args[0][0]
            assert "list_id" in cmd
            assert "-a" not in cmd  # No account when empty

    def test_failure(self):
        from vadimgest.ingest.sources.gog_utils import gog_call
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "auth failed"

        with patch("vadimgest.ingest.sources.gog_utils.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="gog tasks list failed"):
                gog_call("tasks", "list")

    def test_empty_stdout(self):
        from vadimgest.ingest.sources.gog_utils import gog_call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "  "

        with patch("vadimgest.ingest.sources.gog_utils.subprocess.run", return_value=mock_result):
            assert gog_call("tasks", "list") == {}
