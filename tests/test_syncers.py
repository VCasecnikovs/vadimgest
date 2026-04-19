"""Tests for syncer pure/helper functions.

Covers testable helpers from low-coverage syncers:
- Gmail: _msg_to_record, _is_account_address, _parse_email_date
- Signal: _chunk_to_record, _group_into_chunks
- GitHub: _item_to_record, _commit_to_record, _get_commits_since, _gh_call
- WhatsApp: _chunk_to_record, _group_into_chunks, _wacli_call
- iMessage: _apple_date_to_datetime, _datetime_to_apple_date, _get_chat_name, _is_group_chat, _chunk_to_record
- Browser: _chrome_ts_to_datetime, _iso_to_chrome_ts, _window_to_record, _group_into_sessions
- Dayflow: _row_to_record
- Indexer: _extract_title, _extract_jsonl_text, _extract_jsonl_meta, _content_hash, _count_lines
"""

import sys
import os
import json
import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

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
def gmail_syncer(tmp_store):
    """Gmail syncer with test config."""
    from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
    config = {
        "accounts": ["test@gmail.com", "work@company.com"],
        "query": "newer_than:1d",
        "page_size": 10,
    }
    return GmailSyncer(tmp_store, config)


@pytest.fixture
def signal_syncer(tmp_store):
    """Signal syncer with test config."""
    from vadimgest.ingest.sources.signal.syncer import SignalSyncer
    return SignalSyncer(tmp_store, config={})


@pytest.fixture
def github_syncer(tmp_store):
    """GitHub syncer with test config."""
    from vadimgest.ingest.sources.github.syncer import GitHubSyncer
    config = {
        "projects": [{"owner": "acme-org", "project_number": 5}],
        "repos": [{"owner": "acme-org", "repo": "acme-repo"}],
    }
    return GitHubSyncer(tmp_store, config)


@pytest.fixture
def whatsapp_syncer(tmp_store):
    """WhatsApp syncer with test config."""
    from vadimgest.ingest.sources.whatsapp.syncer import WhatsAppSyncer
    return WhatsAppSyncer(tmp_store, config={"fetch_limit": 50, "chat_limit": 50})


@pytest.fixture
def imessage_syncer(tmp_store):
    """iMessage syncer with test config."""
    from vadimgest.ingest.sources.imessage.syncer import IMessageSyncer
    return IMessageSyncer(tmp_store, config={})


@pytest.fixture
def browser_syncer(tmp_store):
    """Browser syncer with test config."""
    from vadimgest.ingest.sources.browser.syncer import BrowserSyncer
    return BrowserSyncer(tmp_store, config={"session_window_minutes": 30})


@pytest.fixture
def dayflow_syncer(tmp_store):
    """Dayflow syncer with test config."""
    from vadimgest.ingest.sources.dayflow.syncer import DayflowSyncer
    return DayflowSyncer(tmp_store, config={"db_path": "/tmp/nonexistent.sqlite"})


# ============================================================
# Gmail Syncer Tests
# ============================================================

class TestGmailMsgToRecord:
    """Test GmailSyncer._msg_to_record."""

    def test_basic_record(self, gmail_syncer):
        msg = {
            "message_id": "abc123",
            "from": "sender@example.com",
            "to": "test@gmail.com",
            "subject": "Hello World",
            "date": "2026-03-15T10:00:00Z",
            "body": "Test body",
            "labels": ["INBOX", "UNREAD"],
            "thread_id": "thread_1",
        }
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record is not None
        assert record["id"] == "gmail_test_abc123"
        assert record["type"] == "email"
        assert record["subject"] == "Hello World"
        assert record["from"] == "sender@example.com"
        assert record["direction"] == "received"
        assert record["is_unread"] is True
        assert "awaiting_reply" not in record

    def test_sent_direction_with_awaiting(self, gmail_syncer):
        msg = {
            "message_id": "xyz789",
            "from": "test@gmail.com",
            "subject": "Follow up",
            "body": "Checking in",
            "labels": [],
        }
        record = gmail_syncer._msg_to_record(
            msg, "test@gmail.com", direction="sent", awaiting_reply=True
        )
        assert record["direction"] == "sent"
        assert record["awaiting_reply"] is True

    def test_no_message_id_returns_none(self, gmail_syncer):
        msg = {"from": "a@b.com", "subject": "No ID"}
        assert gmail_syncer._msg_to_record(msg, "test@gmail.com") is None

    def test_body_truncation(self, gmail_syncer):
        msg = {
            "message_id": "trunc1",
            "body": "x" * 6000,
            "labels": [],
        }
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert len(record["body"]) < 6000
        assert record["body"].endswith("... [truncated]")

    def test_uses_id_fallback(self, gmail_syncer):
        msg = {"id": "fallback_id", "labels": []}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record is not None
        assert "fallback_id" in record["id"]

    def test_account_short_strips_domain(self, gmail_syncer):
        msg = {"message_id": "m1", "labels": []}
        record = gmail_syncer._msg_to_record(msg, "user@example.com")
        assert record["id"] == "gmail_user_m1"

    def test_unread_false_when_no_unread_label(self, gmail_syncer):
        msg = {"message_id": "m2", "labels": ["INBOX"]}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record["is_unread"] is False

    def test_awaiting_reply_none_omitted(self, gmail_syncer):
        msg = {"message_id": "m3", "labels": []}
        record = gmail_syncer._msg_to_record(
            msg, "test@gmail.com", direction="received", awaiting_reply=None
        )
        assert "awaiting_reply" not in record

    def test_default_subject(self, gmail_syncer):
        msg = {"message_id": "m4", "labels": []}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record["subject"] == "(no subject)"


class TestGmailIsAccountAddress:
    """Test GmailSyncer._is_account_address."""

    def test_exact_match(self, gmail_syncer):
        assert gmail_syncer._is_account_address("test@gmail.com", "test@gmail.com")

    def test_case_insensitive(self, gmail_syncer):
        assert gmail_syncer._is_account_address("TEST@gmail.com", "test@gmail.com")

    def test_formatted_name_email(self, gmail_syncer):
        assert gmail_syncer._is_account_address(
            "John Doe <test@gmail.com>", "test@gmail.com"
        )

    def test_configured_accounts(self, gmail_syncer):
        assert gmail_syncer._is_account_address(
            "work@company.com", "other@gmail.com"
        )

    def test_unknown_address(self, gmail_syncer):
        assert not gmail_syncer._is_account_address(
            "stranger@example.com", "test@gmail.com"
        )

    def test_empty_address(self, gmail_syncer):
        assert not gmail_syncer._is_account_address("", "test@gmail.com")

    def test_name_bracket_format(self, gmail_syncer):
        assert gmail_syncer._is_account_address(
            '"Work Email" <work@company.com>', "test@gmail.com"
        )


class TestGmailParseEmailDate:
    """Test GmailSyncer._parse_email_date static method."""

    def test_rfc2822(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("Wed, 19 Feb 2026 14:30:00 +0000")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 2
        assert dt.day == 19

    def test_iso8601_utc(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("2026-02-19T14:30:00Z")
        assert dt is not None
        assert dt.year == 2026

    def test_iso8601_with_tz(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("2026-02-19T14:30:00+0200")
        assert dt is not None

    def test_simple_datetime(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("2026-02-19 14:30:00")
        assert dt is not None
        assert dt.tzinfo == timezone.utc  # assumed UTC

    def test_with_pst_suffix(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("Wed, 19 Feb 2026 14:30:00 +0000 (PST)")
        assert dt is not None

    def test_with_utc_in_parens(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("Wed, 19 Feb 2026 14:30:00 +0000 (UTC)")
        assert dt is not None

    def test_empty_string(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        assert GmailSyncer._parse_email_date("") is None

    def test_invalid_string(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        assert GmailSyncer._parse_email_date("not a date") is None

    def test_rfc2822_without_day_name(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("19 Feb 2026 14:30:00 +0000")
        assert dt is not None
        assert dt.day == 19

    def test_human_readable_format(self):
        from vadimgest.ingest.sources.gmail.syncer import GmailSyncer
        dt = GmailSyncer._parse_email_date("Feb 19, 2026 2:30 PM")
        assert dt is not None
        assert dt.hour == 14
        assert dt.minute == 30


# ============================================================
# Signal Syncer Tests
# ============================================================

class TestSignalChunkToRecord:
    """Test SignalSyncer._chunk_to_record."""

    def test_basic_chunk(self, signal_syncer):
        conv_map = {"conv1": {"name": "Alice", "type": "private"}}
        chunk = [
            {"sent_at": 1710500000000, "type": "incoming", "body": "Hello",
             "sourceServiceId": None},
            {"sent_at": 1710500060000, "type": "outgoing", "body": "Hi there",
             "sourceServiceId": None},
        ]
        record = signal_syncer._chunk_to_record(chunk, "conv1", conv_map)
        assert record["type"] == "conversation"
        assert record["chat"] == "Alice"
        assert len(record["messages"]) == 2
        assert record["messages"][0]["sender"] == "Alice"
        assert record["messages"][1]["sender"] == "Me"

    def test_group_chat_sender_resolution(self, signal_syncer):
        conv_map = {"grp1": {"name": "Team Chat", "type": "group"}}
        service_id_names = {"svc_bob": "Bob", "svc_alice": "Alice"}
        chunk = [
            {"sent_at": 1710500000000, "type": "incoming", "body": "Hey team",
             "sourceServiceId": "svc_bob"},
            {"sent_at": 1710500060000, "type": "incoming", "body": "Hi!",
             "sourceServiceId": "svc_alice"},
        ]
        record = signal_syncer._chunk_to_record(
            chunk, "grp1", conv_map, service_id_names
        )
        assert record["messages"][0]["sender"] == "Bob"
        assert record["messages"][1]["sender"] == "Alice"

    def test_record_id_format(self, signal_syncer):
        conv_map = {"c1": {"name": "Test", "type": "private"}}
        chunk = [
            {"sent_at": 1000, "type": "incoming", "body": "msg", "sourceServiceId": None},
        ]
        record = signal_syncer._chunk_to_record(chunk, "c1", conv_map)
        assert record["id"] == "c1_1000_1000"

    def test_message_with_attachments(self, signal_syncer):
        conv_map = {"c2": {"name": "Bob", "type": "private"}}
        chunk = [
            {"sent_at": 1710500000000, "type": "incoming", "body": "Photo",
             "sourceServiceId": None,
             "_attachments": [{"content_type": "image/jpeg", "file_name": "pic.jpg", "size": 1024}]},
        ]
        record = signal_syncer._chunk_to_record(chunk, "c2", conv_map)
        assert "attachments" in record["messages"][0]
        assert record["messages"][0]["attachments"][0]["content_type"] == "image/jpeg"

    def test_meta_fields(self, signal_syncer):
        conv_map = {"c3": {"name": "Chat", "type": "private"}}
        chunk = [
            {"sent_at": 1000, "type": "incoming", "body": "a", "sourceServiceId": None},
            {"sent_at": 2000, "type": "incoming", "body": "b", "sourceServiceId": None},
        ]
        record = signal_syncer._chunk_to_record(chunk, "c3", conv_map)
        assert record["meta"]["conversation_id"] == "c3"
        assert record["meta"]["message_count"] == 2


class TestSignalGroupIntoChunks:
    """Test SignalSyncer._group_into_chunks."""

    def test_empty_rows(self, signal_syncer):
        assert signal_syncer._group_into_chunks([], {}) == []

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_single_conversation(self, mock_settings, signal_syncer):
        mock_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conv_map = {"c1": {"name": "Alice", "type": "private"}}
        rows = [
            {"conversationId": "c1", "sent_at": 1000, "type": "incoming",
             "body": "hi", "sourceServiceId": None},
            {"conversationId": "c1", "sent_at": 2000, "type": "outgoing",
             "body": "hello", "sourceServiceId": None},
        ]
        chunks = signal_syncer._group_into_chunks(rows, conv_map)
        assert len(chunks) == 1
        assert len(chunks[0]["messages"]) == 2

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_multiple_conversations(self, mock_settings, signal_syncer):
        mock_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conv_map = {
            "c1": {"name": "Alice", "type": "private"},
            "c2": {"name": "Bob", "type": "private"},
        }
        rows = [
            {"conversationId": "c1", "sent_at": 1000, "type": "incoming",
             "body": "from alice", "sourceServiceId": None},
            {"conversationId": "c2", "sent_at": 2000, "type": "incoming",
             "body": "from bob", "sourceServiceId": None},
        ]
        chunks = signal_syncer._group_into_chunks(rows, conv_map)
        assert len(chunks) == 2

    @patch("vadimgest.ingest.sources.signal.syncer.get_conversation_settings")
    def test_time_window_splits(self, mock_settings, signal_syncer):
        mock_settings.return_value = {
            "time_window_hours": 1,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        conv_map = {"c1": {"name": "Alice", "type": "private"}}
        rows = [
            {"conversationId": "c1", "sent_at": 1000, "type": "incoming",
             "body": "msg1", "sourceServiceId": None},
            # 2 hours later in ms
            {"conversationId": "c1", "sent_at": 1000 + 2 * 3600 * 1000,
             "type": "incoming", "body": "msg2", "sourceServiceId": None},
        ]
        chunks = signal_syncer._group_into_chunks(rows, conv_map)
        assert len(chunks) == 2


# ============================================================
# GitHub Syncer Tests
# ============================================================

class TestGitHubItemToRecord:
    """Test GitHubSyncer._item_to_record."""

    def test_basic_item(self, github_syncer):
        item = {
            "id": "PVTI_123",
            "title": "Fix login bug",
            "assignees": ["jsmith"],
            "status": "In Progress",
            "priority": "high",
            "deadline": "2026-03-20",
            "content": {
                "number": 42,
                "repository": "acme-org/acme-repo",
                "type": "Issue",
                "url": "https://github.com/acme-org/acme-repo/issues/42",
            },
        }
        record = github_syncer._item_to_record(item, "acme-org", 5)
        assert record is not None
        assert record["id"] == "ghp_acme-org_5_PVTI_123"
        assert record["type"] == "issue"
        assert record["title"] == "Fix login bug"
        assert record["number"] == 42
        assert record["assignees"] == ["jsmith"]
        assert record["status"] == "In Progress"
        assert record["due_date"] == "2026-03-20"

    def test_no_title_returns_none(self, github_syncer):
        item = {"id": "PVTI_456", "title": "", "content": {}}
        assert github_syncer._item_to_record(item, "acme-org", 5) is None

    def test_non_dict_returns_none(self, github_syncer):
        assert github_syncer._item_to_record("not a dict", "acme-org", 5) is None

    def test_no_deadline(self, github_syncer):
        item = {"id": "PVTI_789", "title": "Some task", "content": {}}
        record = github_syncer._item_to_record(item, "acme-org", 5)
        assert "due_date" not in record

    def test_null_content(self, github_syncer):
        item = {"id": "PVTI_000", "title": "Draft", "content": None}
        record = github_syncer._item_to_record(item, "org", 1)
        assert record is not None
        assert record["meta"]["item_type"] == "issue"  # default

    def test_null_assignees(self, github_syncer):
        item = {"id": "PVTI_111", "title": "Task", "assignees": None, "content": {}}
        record = github_syncer._item_to_record(item, "org", 1)
        assert record["assignees"] == []


class TestGitHubCommitToRecord:
    """Test GitHubSyncer._commit_to_record."""

    def test_tracked_author(self, github_syncer):
        commit = {
            "sha": "abc1234567890",
            "commit": {
                "message": "fix: resolve login issue\n\nDetails here",
                "author": {"name": "Alice Dev", "date": "2026-03-15T10:00:00Z"},
            },
            "author": {"login": "alice-dev"},
            "html_url": "https://github.com/acme-org/acme-repo/commit/abc1234",
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "acme-repo")
        assert record is not None
        assert record["id"] == "ghc_acme-org_acme-repo_abc1234"
        assert record["short_sha"] == "abc1234"
        assert record["author"] == "alice-dev"
        assert record["message"] == "fix: resolve login issue"
        assert record["repo"] == "acme-org/acme-repo"

    def test_all_authors_tracked_when_list_empty(self, github_syncer):
        commit = {
            "sha": "def5678901234",
            "commit": {
                "message": "chore: update deps",
                "author": {"name": "Random Dev", "date": "2026-03-15T10:00:00Z"},
            },
            "author": {"login": "random_dev"},
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "acme-repo")
        assert record is not None
        assert record["author"] == "random_dev"

    def test_no_sha_returns_none(self, github_syncer):
        commit = {"sha": "", "commit": {"message": "test"}}
        assert github_syncer._commit_to_record(commit, "o", "r") is None

    def test_non_dict_returns_none(self, github_syncer):
        assert github_syncer._commit_to_record("bad", "o", "r") is None

    def test_author_fallback_to_commit_name(self, github_syncer):
        commit = {
            "sha": "aaa1234567890",
            "commit": {
                "message": "test commit",
                "author": {"name": "jsmith", "date": "2026-03-15"},
            },
            "author": None,
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "acme-repo")
        assert record is not None
        assert record["author"] == "jsmith"

    def test_full_message_omitted_if_single_line(self, github_syncer):
        commit = {
            "sha": "bbb1234567890",
            "commit": {
                "message": "simple commit",
                "author": {"name": "alice-dev", "date": "2026-03-15"},
            },
            "author": {"login": "alice-dev"},
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "acme-repo")
        assert "full_message" not in record

    def test_message_first_line_truncation(self, github_syncer):
        long_line = "a" * 300
        commit = {
            "sha": "ccc1234567890",
            "commit": {
                "message": long_line + "\nmore details",
                "author": {"name": "bob-dev", "date": "2026-03-15"},
            },
            "author": {"login": "bob-dev"},
        }
        record = github_syncer._commit_to_record(commit, "acme-org", "acme-repo")
        assert len(record["message"]) == 200


class TestGitHubGetCommitsSince:
    """Test GitHubSyncer._get_commits_since."""

    def test_with_last_ts(self, github_syncer):
        state = SourceState(last_ts="2026-03-10T00:00:00Z")
        assert github_syncer._get_commits_since(state) == "2026-03-10T00:00:00Z"

    def test_without_last_ts_returns_7_days_ago(self, github_syncer):
        state = SourceState()
        result = github_syncer._get_commits_since(state)
        assert result is not None
        assert result.endswith("Z")
        dt = datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ")
        assert (datetime.now(timezone.utc) - dt.replace(tzinfo=timezone.utc)).days <= 8


class TestGhCall:
    """Test _gh_call helper."""

    @patch("subprocess.run")
    def test_success(self, mock_run):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_run.return_value = MagicMock(
            returncode=0, stdout='{"items": []}', stderr=""
        )
        result = _gh_call(["project", "item-list", "5"])
        assert result == {"items": []}

    @patch("subprocess.run")
    def test_failure_raises(self, mock_run):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="auth required"
        )
        with pytest.raises(RuntimeError, match="auth required"):
            _gh_call(["api", "repos"])

    @patch("subprocess.run")
    def test_empty_output_returns_none(self, mock_run):
        from vadimgest.ingest.sources.github.syncer import _gh_call
        mock_run.return_value = MagicMock(returncode=0, stdout="  ", stderr="")
        assert _gh_call(["api", "repos"]) is None


# ============================================================
# WhatsApp Syncer Tests
# ============================================================

class TestWhatsAppChunkToRecord:
    """Test WhatsAppSyncer._chunk_to_record."""

    def test_basic_chunk(self, whatsapp_syncer):
        chunk = [
            {"timestamp": "2026-03-15T10:00:00", "sender": "Alice", "text": "Hey"},
            {"timestamp": "2026-03-15T10:01:00", "sender": "Me", "text": "Hi!"},
        ]
        record = whatsapp_syncer._chunk_to_record(
            chunk, "1234@s.whatsapp.net", "Alice", "dm"
        )
        assert record["type"] == "conversation"
        assert record["chat"] == "Alice"
        assert len(record["messages"]) == 2
        assert record["meta"]["chat_type"] == "dm"
        assert record["meta"]["message_count"] == 2

    def test_truncates_long_text(self, whatsapp_syncer):
        chunk = [
            {"timestamp": "2026-03-15T10:00:00", "sender": "X", "text": "y" * 6000},
        ]
        record = whatsapp_syncer._chunk_to_record(
            chunk, "jid@g.us", "Group", "group"
        )
        assert record["messages"][0]["text"].endswith("... [truncated]")
        assert len(record["messages"][0]["text"]) < 6000

    def test_space_timestamp_converted(self, whatsapp_syncer):
        chunk = [
            {"timestamp": "2026-03-15 10:00:00", "sender": "A", "text": "msg"},
        ]
        record = whatsapp_syncer._chunk_to_record(chunk, "jid", "Chat", "dm")
        assert "T" in record["messages"][0]["ts"]

    def test_jid_short_truncated(self, whatsapp_syncer):
        long_jid = "a" * 50 + "@s.whatsapp.net"
        chunk = [
            {"timestamp": "2026-03-15T10:00:00", "sender": "A", "text": "hi"},
        ]
        record = whatsapp_syncer._chunk_to_record(chunk, long_jid, "Name", "dm")
        assert record["id"].startswith("wa_")
        # JID part should be truncated to 20 chars
        parts = record["id"].split("_")
        assert len(parts[1]) <= 20


class TestWhatsAppGroupIntoChunks:
    """Test WhatsAppSyncer._group_into_chunks."""

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_empty_messages(self, mock_settings, whatsapp_syncer):
        mock_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        result = whatsapp_syncer._group_into_chunks([], "jid", "Chat", "dm")
        assert result == []

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_single_chunk(self, mock_settings, whatsapp_syncer):
        mock_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "2026-03-15T10:00:00", "sender": "A", "text": "hi"},
            {"timestamp": "2026-03-15T10:05:00", "sender": "B", "text": "hey"},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        assert len(chunks) == 1
        assert len(chunks[0]["messages"]) == 2

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_time_window_split(self, mock_settings, whatsapp_syncer):
        mock_settings.return_value = {
            "time_window_hours": 1,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "2026-03-15T10:00:00", "sender": "A", "text": "morning"},
            {"timestamp": "2026-03-15T14:00:00", "sender": "A", "text": "afternoon"},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        assert len(chunks) == 2

    @patch("vadimgest.ingest.sources.whatsapp.syncer.get_conversation_settings")
    def test_skips_empty_timestamps(self, mock_settings, whatsapp_syncer):
        mock_settings.return_value = {
            "time_window_hours": 4,
            "min_messages_per_chunk": 1,
            "max_messages_per_chunk": 100,
        }
        messages = [
            {"timestamp": "", "sender": "A", "text": "no ts"},
            {"timestamp": "2026-03-15T10:00:00", "sender": "B", "text": "has ts"},
        ]
        chunks = whatsapp_syncer._group_into_chunks(messages, "jid", "Chat", "dm")
        assert len(chunks) == 1
        assert len(chunks[0]["messages"]) == 1


class TestWacliCall:
    """Test _wacli_call helper."""

    @patch("subprocess.run")
    def test_success(self, mock_run):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"success": true, "data": [{"JID": "123"}]}',
            stderr="",
        )
        result = _wacli_call(["chats", "list"])
        assert result == [{"JID": "123"}]

    @patch("subprocess.run")
    def test_failure_raises(self, mock_run):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="not connected"
        )
        with pytest.raises(RuntimeError, match="not connected"):
            _wacli_call(["chats", "list"])

    @patch("subprocess.run")
    def test_success_false_raises(self, mock_run):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"success": false, "error": "bad"}',
            stderr="",
        )
        with pytest.raises(RuntimeError):
            _wacli_call(["messages", "list"])

    @patch("subprocess.run")
    def test_empty_output_returns_none(self, mock_run):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        assert _wacli_call(["chats", "list"]) is None

    @patch("subprocess.run")
    def test_json_flag_appended(self, mock_run):
        from vadimgest.ingest.sources.whatsapp.syncer import _wacli_call
        mock_run.return_value = MagicMock(
            returncode=0, stdout='{"data": null}', stderr=""
        )
        _wacli_call(["chats", "list", "--limit", "10"])
        cmd = mock_run.call_args[0][0]
        assert cmd[-1] == "--json"


# ============================================================
# iMessage Syncer Tests
# ============================================================

class TestAppleDateConversion:
    """Test _apple_date_to_datetime and _datetime_to_apple_date."""

    def test_nanosecond_timestamp(self):
        from vadimgest.ingest.sources.imessage.syncer import (
            _apple_date_to_datetime, APPLE_EPOCH,
        )
        # 10 seconds after epoch in nanoseconds (must be > 1_000_000_000)
        ns_val = 10_000_000_000  # 10 seconds in nanoseconds
        dt = _apple_date_to_datetime(ns_val)
        assert dt is not None
        expected = datetime.fromtimestamp(10 + APPLE_EPOCH)
        assert abs((dt - expected).total_seconds()) < 1

    def test_second_timestamp(self):
        from vadimgest.ingest.sources.imessage.syncer import (
            _apple_date_to_datetime, APPLE_EPOCH,
        )
        # Value less than 1 billion = seconds
        sec_val = 100
        dt = _apple_date_to_datetime(sec_val)
        assert dt is not None
        expected = datetime.fromtimestamp(100 + APPLE_EPOCH)
        assert abs((dt - expected).total_seconds()) < 1

    def test_none_returns_none(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime
        assert _apple_date_to_datetime(None) is None

    def test_zero_returns_none(self):
        from vadimgest.ingest.sources.imessage.syncer import _apple_date_to_datetime
        assert _apple_date_to_datetime(0) is None

    def test_roundtrip(self):
        from vadimgest.ingest.sources.imessage.syncer import (
            _apple_date_to_datetime, _datetime_to_apple_date,
        )
        original_dt = datetime(2026, 3, 15, 10, 30, 0)
        apple_ts = _datetime_to_apple_date(original_dt)
        roundtrip_dt = _apple_date_to_datetime(apple_ts)
        assert abs((roundtrip_dt - original_dt).total_seconds()) < 1


class TestIMessageGetChatName:
    """Test IMessageSyncer._get_chat_name."""

    def test_display_name_from_row(self, imessage_syncer):
        row = {"chat_display_name": "Family Group", "cache_roomnames": None,
               "handle_id": None, "chat_identifier": None}
        assert imessage_syncer._get_chat_name(row, {}, {}) == "Family Group"

    def test_cache_roomnames_mapped(self, imessage_syncer):
        row = {"chat_display_name": None, "cache_roomnames": "room1",
               "handle_id": None, "chat_identifier": None}
        chat_name_map = {"room1": "Work Team"}
        assert imessage_syncer._get_chat_name(row, {}, chat_name_map) == "Work Team"

    def test_cache_roomnames_unmapped(self, imessage_syncer):
        row = {"chat_display_name": None, "cache_roomnames": "room2",
               "handle_id": None, "chat_identifier": None}
        assert imessage_syncer._get_chat_name(row, {}, {}) == "room2"

    def test_handle_map_fallback(self, imessage_syncer):
        row = {"chat_display_name": None, "cache_roomnames": None,
               "handle_id": 5, "chat_identifier": None}
        handle_map = {5: "+1234567890"}
        assert imessage_syncer._get_chat_name(row, handle_map, {}) == "+1234567890"

    def test_chat_identifier_fallback(self, imessage_syncer):
        row = {"chat_display_name": None, "cache_roomnames": None,
               "handle_id": None, "chat_identifier": "chat1234"}
        assert imessage_syncer._get_chat_name(row, {}, {}) == "chat1234"

    def test_unknown_fallback(self, imessage_syncer):
        row = {"chat_display_name": None, "cache_roomnames": None,
               "handle_id": None, "chat_identifier": None}
        assert imessage_syncer._get_chat_name(row, {}, {}) == "Unknown"


class TestIMessageIsGroupChat:
    """Test IMessageSyncer._is_group_chat."""

    def test_cache_roomnames(self, imessage_syncer):
        row = {"cache_roomnames": "chat123", "chat_identifier": None}
        assert imessage_syncer._is_group_chat(row) is True

    def test_chat_identifier_prefix(self, imessage_syncer):
        row = {"cache_roomnames": None, "chat_identifier": "chat123456"}
        assert imessage_syncer._is_group_chat(row) is True

    def test_dm_chat(self, imessage_syncer):
        row = {"cache_roomnames": None, "chat_identifier": "+1234567890"}
        assert imessage_syncer._is_group_chat(row) is False

    def test_no_fields(self, imessage_syncer):
        row = {"cache_roomnames": None, "chat_identifier": None}
        assert imessage_syncer._is_group_chat(row) is False


class TestIMessageChunkToRecord:
    """Test IMessageSyncer._chunk_to_record."""

    def test_basic_dm_chunk(self, imessage_syncer):
        from vadimgest.ingest.sources.imessage.syncer import (
            _datetime_to_apple_date,
        )
        dt1 = datetime(2026, 3, 15, 10, 0, 0)
        dt2 = datetime(2026, 3, 15, 10, 5, 0)
        chunk = [
            {"date": _datetime_to_apple_date(dt1), "is_from_me": False,
             "handle_id": 1, "text": "Hello",
             "chat_display_name": None, "cache_roomnames": None,
             "chat_identifier": "+1234567890"},
            {"date": _datetime_to_apple_date(dt2), "is_from_me": True,
             "handle_id": None, "text": "Hi!",
             "chat_display_name": None, "cache_roomnames": None,
             "chat_identifier": "+1234567890"},
        ]
        handle_map = {1: "+1234567890"}
        record = imessage_syncer._chunk_to_record(
            chunk, "+1234567890", handle_map, {}
        )
        assert record["type"] == "conversation"
        assert record["messages"][0]["sender"] == "+1234567890"
        assert record["messages"][1]["sender"] == "Me"
        assert record["meta"]["is_group"] is False

    def test_group_chunk(self, imessage_syncer):
        from vadimgest.ingest.sources.imessage.syncer import _datetime_to_apple_date
        dt = datetime(2026, 3, 15, 10, 0, 0)
        chunk = [
            {"date": _datetime_to_apple_date(dt), "is_from_me": False,
             "handle_id": 2, "text": "Group msg",
             "chat_display_name": "Team", "cache_roomnames": "room1",
             "chat_identifier": "chat123"},
        ]
        handle_map = {2: "alice@icloud.com"}
        record = imessage_syncer._chunk_to_record(
            chunk, "chat123", handle_map, {"room1": "Team"}
        )
        assert record["chat"] == "Team"
        assert record["meta"]["is_group"] is True


# ============================================================
# Browser Syncer Tests
# ============================================================

class TestChromeTsConversion:
    """Test BrowserSyncer._chrome_ts_to_datetime and _iso_to_chrome_ts."""

    def test_chrome_ts_to_datetime(self, browser_syncer):
        # Chrome epoch: 1601-01-01, offset = 11644473600 seconds
        # 2026-01-01 00:00:00 UTC = Unix 1767225600
        # Chrome ts = (1767225600 + 11644473600) * 1_000_000
        chrome_ts = (1767225600 + 11644473600) * 1_000_000
        dt = browser_syncer._chrome_ts_to_datetime(chrome_ts)
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 1

    def test_iso_to_chrome_ts(self, browser_syncer):
        iso_ts = "2026-01-01T00:00:00+00:00"
        chrome_ts = browser_syncer._iso_to_chrome_ts(iso_ts)
        assert chrome_ts > 0
        # Roundtrip
        dt = browser_syncer._chrome_ts_to_datetime(chrome_ts)
        assert dt.year == 2026

    def test_invalid_chrome_ts_returns_fallback(self, browser_syncer):
        dt = browser_syncer._chrome_ts_to_datetime(-999999999999999999)
        assert dt.year == 2020


class TestBrowserWindowToRecord:
    """Test BrowserSyncer._window_to_record."""

    def test_basic_window(self, browser_syncer):
        visits = [
            {
                "url": "https://example.com/page1",
                "title": "Example Page 1",
                "domain": "example.com",
                "visit_time": datetime(2026, 3, 15, 10, 0),
                "duration_sec": 120,
                "transition": "link",
            },
            {
                "url": "https://example.com/page2",
                "title": "Example Page 2 - Longer Title",
                "domain": "example.com",
                "visit_time": datetime(2026, 3, 15, 10, 5),
                "duration_sec": 60,
                "transition": "typed",
            },
        ]
        record = browser_syncer._window_to_record("example.com", visits, "Default")
        assert record["type"] == "browsing_session"
        assert record["domain"] == "example.com"
        assert record["title"] == "Example Page 2 - Longer Title"  # longest title
        assert record["total_visits"] == 2
        assert record["total_duration_sec"] == 180
        assert len(record["pages"]) == 2
        assert record["meta"]["profile"] == "Default"

    def test_dedup_urls(self, browser_syncer):
        visits = [
            {
                "url": "https://example.com/same",
                "title": "Same Page",
                "domain": "example.com",
                "visit_time": datetime(2026, 3, 15, 10, 0),
                "duration_sec": 30,
                "transition": "link",
            },
            {
                "url": "https://example.com/same",
                "title": "Same Page",
                "domain": "example.com",
                "visit_time": datetime(2026, 3, 15, 10, 1),
                "duration_sec": 30,
                "transition": "reload",
            },
        ]
        record = browser_syncer._window_to_record("example.com", visits, "Default")
        assert len(record["pages"]) == 1
        assert record["total_visits"] == 2  # count is still 2

    def test_caps_at_20_pages(self, browser_syncer):
        visits = [
            {
                "url": f"https://example.com/page{i}",
                "title": f"Page {i}",
                "domain": "example.com",
                "visit_time": datetime(2026, 3, 15, 10, i),
                "duration_sec": 10,
                "transition": "link",
            }
            for i in range(25)
        ]
        record = browser_syncer._window_to_record("example.com", visits, "Default")
        assert len(record["pages"]) == 20


class TestBrowserGroupIntoSessions:
    """Test BrowserSyncer._group_into_sessions."""

    def test_single_domain_single_session(self, browser_syncer):
        visits = [
            {
                "url": "https://github.com/a",
                "title": "A",
                "domain": "github.com",
                "visit_time": datetime(2026, 3, 15, 10, 0),
                "duration_sec": 60,
                "transition": "link",
            },
            {
                "url": "https://github.com/b",
                "title": "B",
                "domain": "github.com",
                "visit_time": datetime(2026, 3, 15, 10, 10),
                "duration_sec": 30,
                "transition": "link",
            },
        ]
        sessions = browser_syncer._group_into_sessions(visits, "Default")
        assert len(sessions) == 1

    def test_time_gap_splits_session(self, browser_syncer):
        visits = [
            {
                "url": "https://github.com/a",
                "title": "A",
                "domain": "github.com",
                "visit_time": datetime(2026, 3, 15, 10, 0),
                "duration_sec": 60,
                "transition": "link",
            },
            {
                "url": "https://github.com/b",
                "title": "B",
                "domain": "github.com",
                # 45 min later - exceeds 30 min window
                "visit_time": datetime(2026, 3, 15, 10, 45),
                "duration_sec": 30,
                "transition": "link",
            },
        ]
        sessions = browser_syncer._group_into_sessions(visits, "Default")
        assert len(sessions) == 2

    def test_multiple_domains(self, browser_syncer):
        visits = [
            {
                "url": "https://github.com/x",
                "title": "GH",
                "domain": "github.com",
                "visit_time": datetime(2026, 3, 15, 10, 0),
                "duration_sec": 60,
                "transition": "link",
            },
            {
                "url": "https://google.com/search",
                "title": "Search",
                "domain": "google.com",
                "visit_time": datetime(2026, 3, 15, 10, 5),
                "duration_sec": 10,
                "transition": "typed",
            },
        ]
        sessions = browser_syncer._group_into_sessions(visits, "Default")
        assert len(sessions) == 2
        domains = {s["domain"] for s in sessions}
        assert domains == {"github.com", "google.com"}


# ============================================================
# Dayflow Syncer Tests
# ============================================================

class TestDayflowRowToRecord:
    """Test DayflowSyncer._row_to_record."""

    def test_basic_row(self, dayflow_syncer):
        row = {
            "id": "card_123",
            "start": "10:00",
            "end": "11:30",
            "start_ts": 1710500000,
            "end_ts": 1710505400,
            "day": "2026-03-15",
            "title": "Coding Session",
            "summary": "Working on vadimgest tests",
            "detailed_summary": "Writing pytest for syncer functions",
            "category": "Development",
            "subcategory": "Testing",
            "metadata": None,
        }
        record = dayflow_syncer._row_to_record(row)
        assert record is not None
        assert record["id"] == "card_card_123"
        assert record["type"] == "activity"
        assert record["title"] == "Coding Session"
        assert record["category"] == "Development"
        assert record["duration_seconds"] == 5400
        assert record["day"] == "2026-03-15"
        assert record["time_start"] == "10:00"
        assert record["time_end"] == "11:30"

    def test_with_metadata_distractions(self, dayflow_syncer):
        row = {
            "id": "card_456",
            "start": "14:00",
            "end": "15:00",
            "start_ts": 1710500000,
            "end_ts": 1710503600,
            "day": "2026-03-15",
            "title": "Meeting",
            "summary": "Team standup",
            "detailed_summary": "",
            "category": "Communication",
            "subcategory": None,
            "metadata": json.dumps({"distractions": 3}),
        }
        record = dayflow_syncer._row_to_record(row)
        assert record["meta"]["distractions"] == 3

    def test_no_timestamps(self, dayflow_syncer):
        row = {
            "id": "card_789",
            "start": None,
            "end": None,
            "start_ts": None,
            "end_ts": None,
            "day": "2026-03-15",
            "title": "Unknown Activity",
            "summary": "",
            "detailed_summary": "",
            "category": "",
            "subcategory": None,
            "metadata": None,
        }
        record = dayflow_syncer._row_to_record(row)
        assert record["started_at"] is None
        assert record["ended_at"] is None
        assert record["duration_seconds"] == 0

    def test_invalid_metadata_json(self, dayflow_syncer):
        row = {
            "id": "card_bad",
            "start": "10:00",
            "end": "11:00",
            "start_ts": 1710500000,
            "end_ts": 1710503600,
            "day": "2026-03-15",
            "title": "Activity",
            "summary": "test",
            "detailed_summary": "",
            "category": "Work",
            "subcategory": None,
            "metadata": "not valid json{{{",
        }
        record = dayflow_syncer._row_to_record(row)
        assert record["meta"]["distractions"] is None


# ============================================================
# Indexer Tests
# ============================================================

class TestExtractTitle:
    """Test indexer._extract_title."""

    def test_heading(self):
        from vadimgest.search.indexer import _extract_title
        text = "---\ntitle: Test\n---\n# My Title\nContent here"
        assert _extract_title(text, Path("test.md")) == "My Title"

    def test_no_heading_uses_filename(self):
        from vadimgest.search.indexer import _extract_title
        text = "Just some text without headings"
        assert _extract_title(text, Path("my-note.md")) == "my-note"

    def test_frontmatter_skipped(self):
        from vadimgest.search.indexer import _extract_title
        text = "---\ntitle: ignored\n---\n# Actual Title"
        assert _extract_title(text, Path("f.md")) == "Actual Title"

    def test_heading_with_extra_spaces(self):
        from vadimgest.search.indexer import _extract_title
        text = "#   Spaced Title  \nContent"
        assert _extract_title(text, Path("f.md")) == "Spaced Title"


class TestExtractJsonlText:
    """Test indexer._extract_jsonl_text."""

    def test_conversation(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {
            "type": "conversation",
            "chat": "Alice",
            "folder": "telegram",
            "period_end": "2026-03-15T10:00:00",
            "messages": [
                {"sender": "Alice", "text": "Hello"},
                {"sender": "Me", "text": "Hi"},
            ],
        }
        title, text = _extract_jsonl_text(record)
        assert "Alice" in title
        assert "2026-03-15" in title
        assert "Alice: Hello" in text
        assert "Me: Hi" in text

    def test_email(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {
            "type": "email",
            "subject": "Important Update",
            "body": "Please review this document.",
        }
        title, text = _extract_jsonl_text(record)
        assert title == "Important Update"
        assert text == "Please review this document."

    def test_meeting(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {
            "type": "meeting",
            "title": "Sprint Planning",
            "notes": "Discussed priorities",
            "transcript": "Full transcript here",
        }
        title, text = _extract_jsonl_text(record)
        assert title == "Sprint Planning"
        assert "Discussed priorities" in text
        assert "Full transcript here" in text

    def test_issue(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {
            "type": "issue",
            "number": 42,
            "title": "Fix bug",
            "body": "Steps to reproduce",
        }
        title, text = _extract_jsonl_text(record)
        assert "#42" in title
        assert "Fix bug" in title
        assert text == "Steps to reproduce"

    def test_task(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {"type": "task", "title": "Do thing", "notes": "Some notes"}
        title, text = _extract_jsonl_text(record)
        assert title == "Do thing"
        assert text == "Some notes"

    def test_activity(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {"type": "activity", "title": "Coding", "summary": "Writing tests"}
        title, text = _extract_jsonl_text(record)
        assert title == "Coding"
        assert text == "Writing tests"

    def test_document(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {"type": "document", "title": "README", "content": "# Guide\nStuff"}
        title, text = _extract_jsonl_text(record)
        assert title == "README"
        assert text == "# Guide\nStuff"

    def test_message(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {"type": "message", "sender": "Bob", "chat": "DMs", "text": "Hey"}
        title, text = _extract_jsonl_text(record)
        assert "DMs" in title
        assert "Bob" in title
        assert text == "Hey"

    def test_fallback_type(self):
        from vadimgest.search.indexer import _extract_jsonl_text
        record = {"type": "unknown_type", "title": "Custom", "data": "foo"}
        title, text = _extract_jsonl_text(record)
        assert title == "Custom"
        assert "unknown_type" in text  # JSON stringified


class TestExtractJsonlMeta:
    """Test indexer._extract_jsonl_meta."""

    def test_with_chat_and_folder(self):
        from vadimgest.search.indexer import _extract_jsonl_meta
        record = {"chat": "Team Chat", "folder": "signal"}
        chat, folder = _extract_jsonl_meta(record)
        assert chat == "Team Chat"
        assert folder == "signal"

    def test_missing_fields(self):
        from vadimgest.search.indexer import _extract_jsonl_meta
        record = {"type": "email"}
        chat, folder = _extract_jsonl_meta(record)
        assert chat == ""
        assert folder == ""


class TestContentHash:
    """Test indexer._content_hash."""

    def test_deterministic(self):
        from vadimgest.search.indexer import _content_hash
        h1 = _content_hash("hello world")
        h2 = _content_hash("hello world")
        assert h1 == h2

    def test_different_inputs(self):
        from vadimgest.search.indexer import _content_hash
        h1 = _content_hash("hello")
        h2 = _content_hash("world")
        assert h1 != h2

    def test_length_16(self):
        from vadimgest.search.indexer import _content_hash
        h = _content_hash("test content")
        assert len(h) == 16


class TestCountLines:
    """Test indexer._count_lines."""

    def test_basic_file(self, tmp_path):
        from vadimgest.search.indexer import _count_lines
        f = tmp_path / "test.jsonl"
        f.write_text("line1\nline2\nline3\n")
        assert _count_lines(f) == 3

    def test_empty_file(self, tmp_path):
        from vadimgest.search.indexer import _count_lines
        f = tmp_path / "empty.jsonl"
        f.write_text("")
        assert _count_lines(f) == 0

    def test_no_trailing_newline(self, tmp_path):
        from vadimgest.search.indexer import _count_lines
        f = tmp_path / "test.jsonl"
        f.write_text("line1\nline2")
        assert _count_lines(f) == 1

    def test_large_content(self, tmp_path):
        from vadimgest.search.indexer import _count_lines
        f = tmp_path / "big.jsonl"
        lines = ["x" * 100 + "\n" for _ in range(5000)]
        f.write_text("".join(lines))
        assert _count_lines(f) == 5000


class TestIndexJsonl:
    """Test indexer.index_jsonl with a real SQLite FTS5 index."""

    def test_index_new_records(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_jsonl

        # Create a small JSONL file
        jsonl = tmp_path / "test.jsonl"
        records = [
            {"type": "email", "subject": "Test email", "body": "Hello world content here"},
            {"type": "task", "title": "Do testing", "notes": "Write tests for vadimgest"},
        ]
        with open(jsonl, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        db = tmp_path / "index.db"
        conn = get_db(db)
        result = index_jsonl(conn, "test_source", jsonl)
        conn.commit()
        conn.close()

        assert result["added"] == 2
        assert result["total"] == 2

    def test_incremental_index(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_jsonl

        jsonl = tmp_path / "test.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"type": "email", "subject": "First", "body": "content one"}) + "\n")

        db = tmp_path / "index.db"
        conn = get_db(db)
        result1 = index_jsonl(conn, "src", jsonl)
        conn.commit()
        assert result1["added"] == 1

        # Append more data
        with open(jsonl, "a") as f:
            f.write(json.dumps({"type": "email", "subject": "Second", "body": "content two"}) + "\n")

        result2 = index_jsonl(conn, "src", jsonl)
        conn.commit()
        conn.close()

        assert result2["added"] == 1  # only the new one


class TestGetDb:
    """Test indexer.get_db creates proper schema."""

    def test_creates_tables(self, tmp_path):
        from vadimgest.search.indexer import get_db
        db = tmp_path / "test.db"
        conn = get_db(db)

        # Verify FTS5 table exists
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "meta" in table_names
        assert "source_state" in table_names
        assert "schema_info" in table_names

        # Verify schema version
        ver = conn.execute(
            "SELECT value FROM schema_info WHERE key = 'version'"
        ).fetchone()
        assert ver is not None
        assert int(ver[0]) == 5

        conn.close()

    def test_idempotent(self, tmp_path):
        from vadimgest.search.indexer import get_db
        db = tmp_path / "test.db"
        conn1 = get_db(db)
        conn1.close()
        conn2 = get_db(db)
        conn2.close()
        # Should not raise


# ============================================================
# Gmail _get_thread_messages normalization test
# ============================================================

class TestGmailGetThreadMessagesNormalization:
    """Test the normalization logic inside _get_thread_messages (mocked)."""

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_normalizes_raw_api_format(self, mock_gog, gmail_syncer):
        mock_gog.return_value = {
            "thread": {
                "messages": [
                    {
                        "id": "msg1",
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "alice@example.com"},
                                {"name": "To", "value": "test@gmail.com"},
                                {"name": "Subject", "value": "Hi"},
                                {"name": "Date", "value": "2026-03-15"},
                            ],
                        },
                        "labelIds": ["INBOX", "UNREAD"],
                    },
                ],
            },
        }
        msgs = gmail_syncer._get_thread_messages("test@gmail.com", "thread_1")
        assert len(msgs) == 1
        assert msgs[0]["from"] == "alice@example.com"
        assert msgs[0]["to"] == "test@gmail.com"
        assert msgs[0]["subject"] == "Hi"
        assert msgs[0]["labels"] == ["INBOX", "UNREAD"]

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_empty_thread_id(self, mock_gog, gmail_syncer):
        result = gmail_syncer._get_thread_messages("test@gmail.com", "")
        assert result == []
        mock_gog.assert_not_called()

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_api_failure_returns_empty(self, mock_gog, gmail_syncer):
        mock_gog.side_effect = Exception("API error")
        result = gmail_syncer._get_thread_messages("test@gmail.com", "t1")
        assert result == []


# ============================================================
# Gmail _search_threads tests
# ============================================================

class TestGmailSearchThreads:
    """Test GmailSyncer._search_threads."""

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_returns_threads(self, mock_gog, gmail_syncer):
        mock_gog.return_value = {
            "threads": [
                {"id": "t1", "from": "a@b.com", "subject": "Hello"},
            ]
        }
        threads = gmail_syncer._search_threads("test@gmail.com", "newer_than:1d")
        assert len(threads) == 1
        assert threads[0]["id"] == "t1"

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_returns_list_directly(self, mock_gog, gmail_syncer):
        mock_gog.return_value = [{"id": "t1"}]
        threads = gmail_syncer._search_threads("test@gmail.com", "q")
        assert len(threads) == 1

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_error_returns_empty(self, mock_gog, gmail_syncer):
        mock_gog.side_effect = Exception("auth failed")
        threads = gmail_syncer._search_threads("test@gmail.com", "q")
        assert threads == []

    @patch("vadimgest.ingest.sources.gmail.syncer.gog_call")
    def test_unexpected_type_returns_empty(self, mock_gog, gmail_syncer):
        mock_gog.return_value = "unexpected string"
        threads = gmail_syncer._search_threads("test@gmail.com", "q")
        assert threads == []


# ============================================================
# Browser noise filtering constants
# ============================================================

class TestBrowserNoiseConstants:
    """Test browser syncer noise filtering constants."""

    def test_noise_prefixes_block_internal(self):
        from vadimgest.ingest.sources.browser.syncer import _NOISE_PREFIXES
        test_urls = [
            "chrome-extension://abc/popup.html",
            "arc://settings",
            "chrome://newtab",
            "about:blank",
            "data:text/html,<h1>Test</h1>",
        ]
        for url in test_urls:
            assert any(url.startswith(p) for p in _NOISE_PREFIXES), f"{url} should be noise"

    def test_noise_domains_block_local(self):
        from vadimgest.ingest.sources.browser.syncer import _NOISE_DOMAINS
        assert "localhost" in _NOISE_DOMAINS
        assert "127.0.0.1" in _NOISE_DOMAINS

    def test_transitions_map(self):
        from vadimgest.ingest.sources.browser.syncer import _TRANSITIONS
        assert _TRANSITIONS[1] == "typed"
        assert _TRANSITIONS[0] == "link"
        assert _TRANSITIONS[7] == "search"


# ============================================================
# Indexer index_obsidian / index_skills tests
# ============================================================

class TestIndexObsidian:
    """Test indexer.index_obsidian."""

    def test_indexes_md_files(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_obsidian
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "note1.md").write_text("# First Note\nSome content")
        (vault / "sub").mkdir()
        (vault / "sub" / "note2.md").write_text("# Second Note\nMore content")

        db = tmp_path / "index.db"
        conn = get_db(db)
        result = index_obsidian(conn, vault)
        conn.commit()

        assert result["total"] == 2
        assert result["added"] == 2
        assert result["unchanged"] == 0

        # Second run should skip unchanged
        result2 = index_obsidian(conn, vault)
        conn.commit()
        assert result2["unchanged"] == 2
        assert result2["added"] == 0

        conn.close()

    def test_removes_deleted_files(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_obsidian
        vault = tmp_path / "vault"
        vault.mkdir()
        note = vault / "temp.md"
        note.write_text("# Temporary")

        db = tmp_path / "index.db"
        conn = get_db(db)
        index_obsidian(conn, vault)
        conn.commit()

        # Delete the file
        note.unlink()
        result = index_obsidian(conn, vault)
        conn.commit()
        assert result["removed"] == 1
        conn.close()


class TestIndexSkills:
    """Test indexer.index_skills."""

    def test_indexes_skill_files(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_skills
        skills_dir = tmp_path / "skills"
        skill1 = skills_dir / "my-skill"
        skill1.mkdir(parents=True)
        (skill1 / "SKILL.md").write_text("---\nname: test\n---\n# My Skill\nDoes stuff")

        db = tmp_path / "index.db"
        conn = get_db(db)
        result = index_skills(conn, skills_dir)
        conn.commit()

        assert result["total"] == 1
        assert result["added"] == 1
        conn.close()

    def test_nonexistent_dir(self, tmp_path):
        from vadimgest.search.indexer import get_db, index_skills
        db = tmp_path / "index.db"
        conn = get_db(db)
        result = index_skills(conn, tmp_path / "nonexistent")
        assert result["total"] == 0
        conn.close()
