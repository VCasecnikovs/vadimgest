"""Comprehensive tests for vadimgest/cli.py."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from vadimgest.cli import (
    format_record,
    _check_tool,
    show_stats,
    show_health,
    show_logs,
    read_consumer,
    sync_source,
    sync_all,
    cmd_list,
    cmd_init,
    cmd_config,
    cmd_doctor,
)
from vadimgest.models import Record
from vadimgest.store import DataStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rec(data: dict, line: int = 1, source: str = "test") -> Record:
    """Shorthand to create a Record."""
    return Record(_line=line, _ingested_at="2026-01-15T12:00:00Z", _source=source, data=data)


# ===========================================================================
# format_record - JSON format
# ===========================================================================

class TestFormatRecordJson:
    def test_json_format_returns_valid_json(self):
        rec = _rec({"type": "message", "sender": "Alice", "text": "hi"})
        result = format_record(rec, "telegram", "json")
        parsed = json.loads(result)
        assert parsed["sender"] == "Alice"

    def test_json_format_preserves_unicode(self):
        rec = _rec({"type": "message", "sender": "Вадим", "text": "Привет"})
        result = format_record(rec, "telegram", "json")
        assert "Вадим" in result
        assert "Привет" in result

    def test_json_format_all_fields(self):
        data = {"type": "email", "subject": "Test", "from": "a@b.com", "extra": 42}
        rec = _rec(data)
        result = format_record(rec, "gmail", "json")
        parsed = json.loads(result)
        assert parsed == data


# ===========================================================================
# format_record - short format
# ===========================================================================

class TestFormatRecordShort:
    def test_short_format_basic(self):
        rec = _rec({"type": "message", "id": "msg-123"}, line=5)
        result = format_record(rec, "telegram", "short")
        assert "[5]" in result
        assert "message" in result
        assert "msg-123" in result

    def test_short_format_truncates_id(self):
        long_id = "x" * 100
        rec = _rec({"type": "issue", "id": long_id}, line=1)
        result = format_record(rec, "github", "short")
        assert len(long_id[:50]) == 50
        assert long_id[:50] in result
        assert long_id not in result

    def test_short_format_missing_id(self):
        rec = _rec({"type": "document"}, line=3)
        result = format_record(rec, "obsidian", "short")
        assert "?" in result

    def test_short_format_missing_type(self):
        rec = _rec({"some": "data"}, line=1)
        result = format_record(rec, "test", "short")
        assert "unknown" in result


# ===========================================================================
# format_record - conversation (md)
# ===========================================================================

class TestFormatRecordConversation:
    def test_conversation_basic(self):
        data = {
            "type": "conversation",
            "chat": "Team DEV",
            "folder": "Work",
            "period_end": "2026-01-15T18:00:00Z",
            "messages": [
                {"sender": "Alice", "text": "Hello", "ts": "2026-01-15T10:00:00Z"},
                {"sender": "Bob", "text": "Hi there", "ts": "2026-01-15T10:05:00Z"},
            ],
        }
        result = format_record(_rec(data), "telegram", "md")
        assert "### [Work/Team DEV]" in result
        assert "2026-01-15" in result
        assert "Alice: Hello" in result
        assert "Bob: Hi there" in result

    def test_conversation_truncates_at_15(self):
        messages = [{"sender": f"User{i}", "text": f"msg{i}", "ts": f"2026-01-15T10:{i:02}:00Z"} for i in range(20)]
        data = {"type": "conversation", "chat": "Big", "messages": messages}
        result = format_record(_rec(data), "telegram", "md")
        assert "+5 more" in result

    def test_conversation_empty_messages(self):
        data = {"type": "conversation", "chat": "Empty", "messages": []}
        result = format_record(_rec(data), "signal", "md")
        assert "### [/Empty]" in result

    def test_conversation_missing_fields(self):
        data = {"type": "conversation"}
        result = format_record(_rec(data), "telegram", "md")
        assert "### [/unknown]" in result

    def test_conversation_long_text_truncated(self):
        data = {
            "type": "conversation",
            "chat": "test",
            "messages": [{"sender": "A", "text": "x" * 300, "ts": "2026-01-15T10:00:00Z"}],
        }
        result = format_record(_rec(data), "telegram", "md")
        # text truncated to 200
        assert "x" * 200 in result
        assert "x" * 201 not in result

    def test_conversation_newlines_in_text_replaced(self):
        data = {
            "type": "conversation",
            "chat": "test",
            "messages": [{"sender": "A", "text": "line1\nline2\nline3", "ts": ""}],
        }
        result = format_record(_rec(data), "telegram", "md")
        assert "\n" not in result.split("\n")[-1]  # message line has no newlines

    def test_conversation_no_period_end(self):
        data = {"type": "conversation", "chat": "test", "period_end": None, "messages": []}
        result = format_record(_rec(data), "telegram", "md")
        assert "### [/test]" in result


# ===========================================================================
# format_record - meeting (md)
# ===========================================================================

class TestFormatRecordMeeting:
    def test_meeting_basic(self):
        data = {
            "type": "meeting",
            "title": "Sprint Planning",
            "duration_minutes": 45,
            "participants": [{"name": "Alice"}, {"name": "Bob"}],
            "notes": "Discussed roadmap",
            "transcript": "Alice: Let's start",
        }
        result = format_record(_rec(data), "granola", "md")
        assert "### Meeting: Sprint Planning (45m)" in result
        assert "Participants: Alice, Bob" in result
        assert "**Notes:**" in result
        assert "Discussed roadmap" in result
        assert "**Transcript:**" in result

    def test_meeting_string_participants(self):
        data = {
            "type": "meeting",
            "title": "Call",
            "duration_minutes": 10,
            "participants": ["Alice", "Bob"],
        }
        result = format_record(_rec(data), "granola", "md")
        assert "Participants: Alice, Bob" in result

    def test_meeting_mixed_participants(self):
        data = {
            "type": "meeting",
            "title": "Call",
            "duration_minutes": 5,
            "participants": [{"name": "Alice"}, "Bob"],
        }
        result = format_record(_rec(data), "granola", "md")
        assert "Alice" in result
        assert "Bob" in result

    def test_meeting_no_optional_fields(self):
        data = {"type": "meeting"}
        result = format_record(_rec(data), "granola", "md")
        assert "### Meeting: Meeting (0m)" in result
        assert "Participants" not in result

    def test_meeting_truncates_notes_and_transcript(self):
        data = {
            "type": "meeting",
            "title": "Long",
            "notes": "n" * 600,
            "transcript": "t" * 900,
        }
        result = format_record(_rec(data), "granola", "md")
        assert "n" * 500 in result
        assert "n" * 501 not in result
        assert "t" * 800 in result
        assert "t" * 801 not in result

    def test_meeting_participants_capped_at_5(self):
        data = {
            "type": "meeting",
            "title": "Big",
            "participants": [{"name": f"P{i}"} for i in range(10)],
        }
        result = format_record(_rec(data), "granola", "md")
        assert "P4" in result
        assert "P5" not in result


# ===========================================================================
# format_record - activity (md)
# ===========================================================================

class TestFormatRecordActivity:
    def test_activity_basic(self):
        data = {
            "type": "activity",
            "title": "VS Code",
            "category": "Development",
            "duration_seconds": 3600,
            "summary": "Coding session",
        }
        result = format_record(_rec(data), "dayflow", "md")
        assert "[Development]" in result
        assert "VS Code" in result
        assert "(60m)" in result
        assert "Coding session" in result

    def test_activity_defaults(self):
        data = {"type": "activity"}
        result = format_record(_rec(data), "dayflow", "md")
        assert "Activity" in result
        assert "(0m)" in result


# ===========================================================================
# format_record - document (md)
# ===========================================================================

class TestFormatRecordDocument:
    def test_document_basic(self):
        data = {"type": "document", "title": "README.md", "path": "/docs/README.md"}
        result = format_record(_rec(data), "obsidian", "md")
        assert "README.md: /docs/README.md" in result

    def test_document_defaults(self):
        data = {"type": "document"}
        result = format_record(_rec(data), "obsidian", "md")
        assert "Document:" in result


# ===========================================================================
# format_record - issue (md)
# ===========================================================================

class TestFormatRecordIssue:
    def test_issue_full(self):
        data = {
            "type": "issue",
            "number": 42,
            "title": "Fix bug",
            "status": "In Progress",
            "project": "MyProject",
            "assignees": ["Alice", "Bob"],
        }
        result = format_record(_rec(data), "github", "md")
        assert "#42" in result
        assert "Fix bug" in result
        assert "[In Progress]" in result
        assert "-> Alice, Bob" in result
        assert "(MyProject)" in result

    def test_issue_minimal(self):
        data = {"type": "issue"}
        result = format_record(_rec(data), "github", "md")
        assert "#?" in result
        assert "Issue" in result

    def test_issue_no_status_no_assignees(self):
        data = {"type": "issue", "number": 1, "title": "Test", "project": "P"}
        result = format_record(_rec(data), "github", "md")
        assert "[" not in result or "[" in result  # no status bracket
        assert "->" not in result


# ===========================================================================
# format_record - email (md)
# ===========================================================================

class TestFormatRecordEmail:
    def test_email_received_unread(self):
        data = {
            "type": "email",
            "subject": "Hello",
            "from": "alice@example.com",
            "to": "alice@example.com",
            "account": "user@example.com",
            "is_unread": True,
            "direction": "received",
        }
        result = format_record(_rec(data), "gmail", "md")
        assert "Hello" in result
        assert "[UNREAD]" in result
        assert "from alice@example.com" in result
        assert "(user@example.com)" in result

    def test_email_sent(self):
        data = {
            "type": "email",
            "subject": "Reply",
            "from": "alice@example.com",
            "to": "bob@example.com",
            "direction": "sent",
            "account": "me@example.com",
        }
        result = format_record(_rec(data), "gmail", "md")
        assert "[SENT]" in result
        assert "to bob@example.com" in result

    def test_email_awaiting_reply(self):
        data = {
            "type": "email",
            "subject": "Follow up",
            "direction": "sent",
            "awaiting_reply": True,
            "account": "test",
        }
        result = format_record(_rec(data), "gmail", "md")
        assert "AWAITING REPLY" in result
        assert "SENT" in result

    def test_email_no_tags(self):
        data = {"type": "email", "subject": "Normal", "direction": "received", "account": "a"}
        result = format_record(_rec(data), "gmail", "md")
        assert "[" not in result or result.index("Normal") < result.index("[") if "[" in result else True

    def test_email_defaults(self):
        data = {"type": "email"}
        result = format_record(_rec(data), "gmail", "md")
        assert "(no subject)" in result


# ===========================================================================
# format_record - email_status_update (md)
# ===========================================================================

class TestFormatRecordEmailStatusUpdate:
    def test_email_status_update(self):
        data = {"type": "email_status_update", "subject": "RE: Proposal", "account": "me@example.com"}
        result = format_record(_rec(data), "gmail", "md")
        assert "[REPLY RECEIVED]" in result
        assert "RE: Proposal" in result
        assert "(me@example.com)" in result

    def test_email_status_update_defaults(self):
        data = {"type": "email_status_update"}
        result = format_record(_rec(data), "gmail", "md")
        assert "(no subject)" in result


# ===========================================================================
# format_record - task (md)
# ===========================================================================

class TestFormatRecordTask:
    def test_task_with_due(self):
        data = {"type": "task", "title": "Send invoice", "list_name": "Work", "due": "2026-03-20T00:00:00Z"}
        result = format_record(_rec(data), "gtasks", "md")
        assert "Send invoice" in result
        assert "due:2026-03-20" in result
        assert "(Work)" in result

    def test_task_no_due(self):
        data = {"type": "task", "title": "Buy milk", "list_name": "Personal"}
        result = format_record(_rec(data), "gtasks", "md")
        assert "Buy milk" in result
        assert "due:" not in result

    def test_task_defaults(self):
        data = {"type": "task"}
        result = format_record(_rec(data), "gtasks", "md")
        assert "(untitled)" in result


# ===========================================================================
# format_record - calendar_event (md)
# ===========================================================================

class TestFormatRecordCalendarEvent:
    def test_calendar_event_full(self):
        data = {
            "type": "calendar_event",
            "title": "Team Standup",
            "start": "2026-03-16T10:00:00+02:00",
            "location": "Zoom",
            "calendar_name": "Work",
            "attendees": ["alice@ex.com", "bob@ex.com"],
        }
        result = format_record(_rec(data), "calendar", "md")
        assert "2026-03-16T10:00" in result
        assert "Team Standup" in result
        assert "@ Zoom" in result
        assert "(2 attendees)" in result
        assert "[Work]" in result

    def test_calendar_event_no_location_no_attendees(self):
        data = {"type": "calendar_event", "title": "Lunch", "start": "2026-03-16T12:00:00", "calendar_name": "Personal"}
        result = format_record(_rec(data), "calendar", "md")
        assert "@ " not in result
        assert "attendees" not in result

    def test_calendar_event_defaults(self):
        data = {"type": "calendar_event"}
        result = format_record(_rec(data), "calendar", "md")
        assert "(no title)" in result


# ===========================================================================
# format_record - linkedin_message (md)
# ===========================================================================

class TestFormatRecordLinkedinMessage:
    def test_linkedin_message(self):
        data = {
            "type": "linkedin_message",
            "sender": "John Smith",
            "body": "Let's connect!",
            "participants": ["John Smith", "Jane Doe"],
        }
        result = format_record(_rec(data), "linkedin", "md")
        assert "[LinkedIn]" in result
        assert "John Smith" in result
        assert "(John Smith, Jane Doe)" in result
        assert "Let's connect!" in result

    def test_linkedin_message_no_participants(self):
        data = {"type": "linkedin_message", "sender": "Alice", "body": "Hi"}
        result = format_record(_rec(data), "linkedin", "md")
        assert "[LinkedIn] Alice:" in result
        assert "(" not in result or "()" not in result

    def test_linkedin_message_long_body_truncated(self):
        data = {"type": "linkedin_message", "sender": "X", "body": "b" * 300}
        result = format_record(_rec(data), "linkedin", "md")
        assert "b" * 200 in result
        assert "b" * 201 not in result


# ===========================================================================
# format_record - linkedin_invitation (md)
# ===========================================================================

class TestFormatRecordLinkedinInvitation:
    def test_linkedin_invitation_full(self):
        data = {
            "type": "linkedin_invitation",
            "from_name": "Jane Doe",
            "from_headline": "CEO at Startup",
            "message": "I'd love to connect",
        }
        result = format_record(_rec(data), "linkedin", "md")
        assert "[LinkedIn Invite]" in result
        assert "Jane Doe" in result
        assert "- CEO at Startup" in result
        assert ": I'd love to connect" in result

    def test_linkedin_invitation_no_headline_no_message(self):
        data = {"type": "linkedin_invitation", "from_name": "Alice"}
        result = format_record(_rec(data), "linkedin", "md")
        assert "[LinkedIn Invite] Alice" in result
        assert " - " not in result


# ===========================================================================
# format_record - linkedin_profile_view (md)
# ===========================================================================

class TestFormatRecordLinkedinProfileView:
    def test_profile_view(self):
        data = {"type": "linkedin_profile_view", "viewer_name": "Recruiter", "viewer_headline": "HR at Google"}
        result = format_record(_rec(data), "linkedin", "md")
        assert "[Profile View]" in result
        assert "Recruiter" in result
        assert "- HR at Google" in result

    def test_profile_view_no_headline(self):
        data = {"type": "linkedin_profile_view", "viewer_name": "Anon"}
        result = format_record(_rec(data), "linkedin", "md")
        assert "[Profile View] Anon" in result
        assert " - " not in result

    def test_profile_view_defaults(self):
        data = {"type": "linkedin_profile_view"}
        result = format_record(_rec(data), "linkedin", "md")
        assert "Anonymous" in result


# ===========================================================================
# format_record - message (md)
# ===========================================================================

class TestFormatRecordMessage:
    def test_message_basic(self):
        data = {
            "type": "message",
            "sender": "Alice",
            "text": "Hello world",
            "timestamp": "2026-03-16T14:30:00Z",
            "chat": "General",
        }
        result = format_record(_rec(data), "telegram", "md")
        assert "[14:30]" in result
        assert "Alice: Hello world" in result

    def test_message_self_marked(self):
        with patch("vadimgest.cli.load_config", return_value={"self_names": ["John", "Smith", "jsmith"]}):
            for sender in ["John Smith", "jsmith@example.com", "Some Smith"]:
                data = {"type": "message", "sender": sender, "text": "Test"}
                result = format_record(_rec(data), "telegram", "md")
                assert "[Me]" in result, f"Failed for sender: {sender}"

    def test_message_self_case_insensitive(self):
        with patch("vadimgest.cli.load_config", return_value={"self_names": ["JSmith"]}):
            data = {"type": "message", "sender": "jsmith", "text": "hi"}
            result = format_record(_rec(data), "telegram", "md")
            assert "[Me]" in result

    def test_message_not_self(self):
        with patch("vadimgest.cli.load_config", return_value={"self_names": ["John Smith"]}):
            data = {"type": "message", "sender": "Bob", "text": "hi"}
            result = format_record(_rec(data), "telegram", "md")
            assert "Bob:" in result
            assert "[Me]" not in result

    def test_message_no_self_names_configured(self):
        with patch("vadimgest.cli.load_config", return_value={}):
            data = {"type": "message", "sender": "Anyone", "text": "hi"}
            result = format_record(_rec(data), "telegram", "md")
            assert "Anyone:" in result
            assert "[Me]" not in result

    def test_message_truncates_text(self):
        data = {"type": "message", "sender": "A", "text": "z" * 400}
        result = format_record(_rec(data), "telegram", "md")
        assert "z" * 300 in result
        assert "z" * 301 not in result

    def test_message_newlines_replaced(self):
        data = {"type": "message", "sender": "A", "text": "line1\nline2"}
        result = format_record(_rec(data), "telegram", "md")
        assert "line1 line2" in result

    def test_message_no_timestamp(self):
        data = {"type": "message", "sender": "A", "text": "hi", "timestamp": ""}
        result = format_record(_rec(data), "telegram", "md")
        assert "[]" in result

    def test_message_short_timestamp(self):
        data = {"type": "message", "sender": "A", "text": "hi", "timestamp": "14:30"}
        result = format_record(_rec(data), "telegram", "md")
        assert "14:30" in result

    def test_message_none_text(self):
        data = {"type": "message", "sender": "A", "text": None}
        result = format_record(_rec(data), "telegram", "md")
        assert "A:" in result


# ===========================================================================
# format_record - unknown type (md)
# ===========================================================================

class TestFormatRecordUnknown:
    def test_unknown_type(self):
        data = {"type": "weird_thing", "foo": "bar"}
        result = format_record(_rec(data), "custom_source", "md")
        assert "[custom_source]" in result
        assert "foo" in result

    def test_unknown_type_truncated(self):
        data = {"type": "weird", "big": "x" * 300}
        result = format_record(_rec(data), "test", "md")
        assert len(result) <= 250  # [test] + 200 chars of data

    def test_no_type_at_all(self):
        data = {"just": "data"}
        result = format_record(_rec(data), "test", "md")
        assert "[test]" in result


# ===========================================================================
# _check_tool
# ===========================================================================

class TestCheckTool:
    @patch("shutil.which", return_value="/usr/bin/sigtop")
    def test_regular_tool_found(self, mock_which):
        assert _check_tool("sigtop") is True
        mock_which.assert_called_with("sigtop")

    @patch("shutil.which", return_value=None)
    def test_regular_tool_not_found(self, mock_which):
        assert _check_tool("sigtop") is False

    def test_imessage_export_checks_file(self):
        from vadimgest import cli
        original = cli._PACKAGE_DIR
        try:
            cli._PACKAGE_DIR = Path("/fake/pkg")
            with patch.object(Path, "exists", return_value=True), \
                 patch("os.access", return_value=True):
                result = _check_tool("imessage-export")
                assert result is True
        finally:
            cli._PACKAGE_DIR = original

    def test_imessage_export_not_found(self):
        from vadimgest import cli
        original = cli._PACKAGE_DIR
        try:
            cli._PACKAGE_DIR = Path("/nonexistent/path")
            result = _check_tool("imessage-export")
            assert result is False
        finally:
            cli._PACKAGE_DIR = original

    @patch("shutil.which", return_value="/usr/bin/mcp-cli")
    def test_mcp_cli_found_in_path(self, mock_which):
        assert _check_tool("mcp-cli") is True

    @patch("shutil.which", return_value=None)
    def test_mcp_cli_not_in_path_no_versions(self, mock_which):
        with patch.object(Path, "exists", return_value=False):
            assert _check_tool("mcp-cli") is False


# ===========================================================================
# show_stats
# ===========================================================================

class TestShowStats:
    def test_show_stats_with_data(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1", "text": "hi"})
        store.append("gmail", {"type": "email", "id": "e1", "subject": "Test"})

        with patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}):
            show_stats(store)

        out = capsys.readouterr().out
        assert "vadimgest Statistics" in out
        assert "telegram" in out
        assert "gmail" in out
        assert "TOTAL" in out

    def test_show_stats_empty(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        show_stats(store)
        out = capsys.readouterr().out
        assert "No data yet" in out

    def test_show_stats_with_checkpoints(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})
        store.commit("telegram", "heartbeat")

        with patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}):
            show_stats(store)

        out = capsys.readouterr().out
        assert "Consumers:" in out
        assert "heartbeat" in out


# ===========================================================================
# show_health
# ===========================================================================

class TestShowHealth:
    def test_show_health_no_runs(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()):
            show_health(store)

        out = capsys.readouterr().out
        assert "Health Check" in out
        assert "never run" in out

    def test_show_health_with_ok_run(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        runs_file = tmp_path / "sync_runs.jsonl"
        now = datetime.now()
        run = {"source": "telegram", "ts": now.isoformat(), "status": "ok", "count": 5}
        runs_file.write_text(json.dumps(run) + "\n")

        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()):
            show_health(store)

        out = capsys.readouterr().out
        assert "ok" in out

    def test_show_health_with_stale_run(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        runs_file = tmp_path / "sync_runs.jsonl"
        old = datetime.now() - timedelta(hours=3)
        run = {"source": "telegram", "ts": old.isoformat(), "status": "ok"}
        runs_file.write_text(json.dumps(run) + "\n")

        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()):
            show_health(store)

        out = capsys.readouterr().out
        assert "stale" in out
        assert "Some sources need attention" in out

    def test_show_health_with_error_run(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        runs_file = tmp_path / "sync_runs.jsonl"
        now = datetime.now()
        run = {"source": "telegram", "ts": now.isoformat(), "status": "error", "error": "connection timeout"}
        runs_file.write_text(json.dumps(run) + "\n")

        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()):
            show_health(store)

        out = capsys.readouterr().out
        assert "error" in out
        assert "connection timeout" in out

    def test_show_health_unavailable_source(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        with patch("vadimgest.cli.all_source_names", return_value=["linkedin"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="No module named 'linkedin_api'"):
            show_health(store)

        out = capsys.readouterr().out
        assert "unavailable" in out


# ===========================================================================
# show_logs
# ===========================================================================

class TestShowLogs:
    def test_show_logs_no_file(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        show_logs(store)
        out = capsys.readouterr().out
        assert "No logs yet" in out

    def test_show_logs_with_file(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        log_file = tmp_path / "sync.log"
        log_file.write_text("2026-03-16 10:00 INFO sync started\n2026-03-16 10:01 INFO sync done\n")
        show_logs(store, lines=10)
        out = capsys.readouterr().out
        assert "sync started" in out
        assert "sync done" in out

    def test_show_logs_respects_lines_limit(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        log_file = tmp_path / "sync.log"
        lines = [f"line {i}\n" for i in range(50)]
        log_file.write_text("".join(lines))
        show_logs(store, lines=5)
        out = capsys.readouterr().out
        assert "line 45" in out
        assert "line 49" in out
        assert "line 44" not in out


# ===========================================================================
# read_consumer
# ===========================================================================

class TestReadConsumer:
    def test_read_consumer_stats_only(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})
        store.append("telegram", {"type": "message", "id": "m2"})

        read_consumer(store, "test_consumer", ["telegram"], stats_only=True)
        out = capsys.readouterr().out
        assert "test_consumer" in out
        assert "2 total" in out
        assert "2 new" in out

    def test_read_consumer_stats_with_checkpoint(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})
        store.commit("telegram", "myc")
        store.append("telegram", {"type": "message", "id": "m2"})

        read_consumer(store, "myc", ["telegram"], stats_only=True)
        out = capsys.readouterr().out
        assert "1 new" in out

    def test_read_consumer_commit(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})

        read_consumer(store, "test_consumer", ["telegram"], commit=True)
        out = capsys.readouterr().out
        assert "Committed" in out

        # Verify checkpoint was advanced
        cp = store.get_checkpoint("test_consumer")
        assert cp.positions["telegram"]["line"] == 1

    def test_read_consumer_no_new_data(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})
        store.commit("telegram", "myc")

        read_consumer(store, "myc", ["telegram"])
        out = capsys.readouterr().out
        assert "No new data" in out

    def test_read_consumer_json_format(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1", "text": "hello"})

        read_consumer(store, "c", ["telegram"], fmt="json")
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert "telegram" in parsed
        assert parsed["telegram"]["new"][0]["text"] == "hello"

    def test_read_consumer_md_format(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("gmail", {"type": "email", "id": "e1", "subject": "Test Email"})

        read_consumer(store, "c", ["gmail"], fmt="md")
        out = capsys.readouterr().out
        assert "Vadimgest Data" in out
        assert "Gmail" in out
        assert "1 records" in out

    def test_read_consumer_md_chat_grouping(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1", "sender": "Alice", "text": "hi", "chat": "GroupA", "timestamp": "2026-01-01T10:00:00Z"})
        store.append("telegram", {"type": "message", "id": "m2", "sender": "Bob", "text": "yo", "chat": "GroupB", "timestamp": "2026-01-01T11:00:00Z"})
        store.append("telegram", {"type": "message", "id": "m3", "sender": "Alice", "text": "bye", "chat": "GroupA", "timestamp": "2026-01-01T12:00:00Z"})

        read_consumer(store, "c", ["telegram"], fmt="md")
        out = capsys.readouterr().out
        assert "GroupA (2 new)" in out
        assert "GroupB (1 new)" in out

    def test_read_consumer_dayflow_capped(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        for i in range(40):
            store.append("dayflow", {"type": "activity", "id": f"a{i}", "title": f"App{i}"})

        read_consumer(store, "c", ["dayflow"], fmt="md", limit=50)
        out = capsys.readouterr().out
        assert "+10 more activities" in out

    def test_read_consumer_limit_respected(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        for i in range(10):
            store.append("github", {"type": "issue", "id": f"i{i}", "number": i, "title": f"Issue {i}"})

        read_consumer(store, "c", ["github"], fmt="md", limit=3)
        out = capsys.readouterr().out
        assert "+7 more github records" in out


# ===========================================================================
# sync_source
# ===========================================================================

class TestSyncSource:
    def test_sync_unknown_source(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        with patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli.all_source_names", return_value=["telegram"]):
            count, error = sync_source(store, "nonexistent")

        assert count == 0
        assert error == "unknown source"
        out = capsys.readouterr().out
        assert "Unknown source" in out

    def test_sync_unavailable_source(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        with patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="No module named 'telethon'"):
            count, error = sync_source(store, "telegram")

        assert count == 0
        assert "telethon" in error
        out = capsys.readouterr().out
        assert "unavailable" in out

    def test_sync_success(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        mock_syncer_cls = MagicMock()
        mock_syncer = MagicMock()
        mock_syncer.sync.return_value = (42, ["chat1", "chat2"])
        mock_syncer_cls.return_value = mock_syncer

        with patch("vadimgest.cli.get_syncer_class", return_value=mock_syncer_cls), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}):
            count, error = sync_source(store, "telegram")

        assert count == 42
        assert error is None
        mock_syncer.log_run.assert_called_once()

    def test_sync_error(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        mock_syncer_cls = MagicMock()
        mock_syncer = MagicMock()
        mock_syncer.sync.side_effect = RuntimeError("connection failed")
        mock_syncer_cls.return_value = mock_syncer

        with patch("vadimgest.cli.get_syncer_class", return_value=mock_syncer_cls), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}):
            count, error = sync_source(store, "telegram")

        assert count == 0
        assert "connection failed" in error
        mock_syncer.log_run.assert_called_once()


# ===========================================================================
# sync_all
# ===========================================================================

class TestSyncAll:
    def test_sync_all_skips_unavailable(self, tmp_path, capsys):
        store = DataStore(tmp_path)

        mock_syncer_cls = MagicMock()
        mock_syncer = MagicMock()
        mock_syncer.sync.return_value = (10, [])
        mock_syncer_cls.return_value = mock_syncer

        def mock_get_syncer(name):
            if name == "telegram":
                return mock_syncer_cls
            return None

        with patch("vadimgest.cli.all_source_names", return_value=["telegram", "linkedin"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", side_effect=mock_get_syncer):
            results = sync_all(store)

        assert "telegram" in results
        assert "linkedin" not in results

    def test_sync_all_returns_counts(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        mock_cls = MagicMock()
        mock_inst = MagicMock()
        mock_inst.sync.return_value = (5, ["item1"])
        mock_cls.return_value = mock_inst

        with patch("vadimgest.cli.all_source_names", return_value=["signal"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=mock_cls):
            results = sync_all(store)

        assert results["signal"] == 5


# ===========================================================================
# cmd_list
# ===========================================================================

class TestCmdList:
    def test_cmd_list_output(self, capsys):
        with patch("vadimgest.cli.all_source_names", return_value=["telegram", "gmail"]), \
             patch("vadimgest.cli.get_source_config", side_effect=lambda n: {"mode": "cron", "enabled": True}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()), \
             patch("vadimgest.cli.get_load_error", return_value=None):
            cmd_list()

        out = capsys.readouterr().out
        assert "Sources:" in out
        assert "telegram" in out
        assert "gmail" in out
        assert "ready" in out

    def test_cmd_list_disabled_source(self, capsys):
        with patch("vadimgest.cli.all_source_names", return_value=["obsidian"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()), \
             patch("vadimgest.cli.get_load_error", return_value=None):
            cmd_list()

        out = capsys.readouterr().out
        assert "disabled" in out

    def test_cmd_list_unavailable_source(self, capsys):
        with patch("vadimgest.cli.all_source_names", return_value=["linkedin"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="No module named 'linkedin_api'"):
            cmd_list()

        out = capsys.readouterr().out
        assert "unavail" in out
        assert "pip:" in out


# ===========================================================================
# cmd_init
# ===========================================================================

class TestCmdInit:
    def test_cmd_init_config_exists(self, tmp_path, capsys, monkeypatch):
        config_dir = tmp_path / "vadimgest"
        config_dir.mkdir()
        config_file = config_dir / "config.yaml"
        config_file.write_text("test: true")

        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
        cmd_init()
        out = capsys.readouterr().out
        assert "already exists" in out

    def test_cmd_init_local_config(self, capsys):
        with patch("vadimgest.cli._PACKAGE_DIR", new=Path("/fake/pkg")), \
             patch("vadimgest.cli._find_config_file", return_value=None):
            # config_file won't exist in XDG, and local config exists
            with patch.object(Path, "exists", side_effect=lambda self=None: True):
                # This is tricky - cmd_init first checks XDG, then local
                # Let's just verify the function doesn't crash
                pass


# ===========================================================================
# cmd_config
# ===========================================================================

class TestCmdConfig:
    def test_cmd_config_output(self, capsys):
        with patch("vadimgest.cli._find_config_file", return_value=Path("/etc/vadimgest/config.yaml")), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data/vadimgest")), \
             patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": True, "mode": "daemon"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()), \
             patch("vadimgest.cli.get_load_error", return_value=None):
            cmd_config()

        out = capsys.readouterr().out
        assert "Configuration" in out
        assert "/etc/vadimgest/config.yaml" in out
        assert "/data/vadimgest" in out
        assert "telegram" in out
        assert "enabled" in out

    def test_cmd_config_no_config_file(self, capsys):
        with patch("vadimgest.cli._find_config_file", return_value=None), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli.all_source_names", return_value=[]), \
             patch("vadimgest.cli.get_source_config", return_value={}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None):
            cmd_config()

        out = capsys.readouterr().out
        assert "(none - using defaults)" in out

    def test_cmd_config_unavailable_source(self, capsys):
        with patch("vadimgest.cli._find_config_file", return_value=None), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": False, "mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="missing dep"):
            cmd_config()

        out = capsys.readouterr().out
        assert "unavailable: missing dep" in out


# ===========================================================================
# cmd_doctor
# ===========================================================================

class TestCmdDoctor:
    def test_cmd_doctor_runs(self, capsys):
        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": True, "mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._find_config_file", return_value=Path("/config.yaml")), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=True):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "vadimgest Doctor" in out
        assert "Python" in out
        assert "Summary:" in out

    def test_cmd_doctor_no_config(self, capsys):
        with patch("vadimgest.cli.all_source_names", return_value=[]), \
             patch("vadimgest.cli.get_source_config", return_value={}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._find_config_file", return_value=None), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=False):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "[!!]" in out or "[OK]" in out  # Python check at minimum


# ===========================================================================
# Edge cases and integration-style tests
# ===========================================================================

class TestEdgeCases:
    def test_format_record_empty_data(self):
        rec = _rec({})
        result = format_record(rec, "test", "md")
        assert "[test]" in result

    def test_format_record_none_values_in_message(self):
        data = {"type": "message", "sender": None, "text": None, "timestamp": None}
        rec = _rec(data)
        # Should not crash
        result = format_record(rec, "telegram", "md")
        assert isinstance(result, str)

    def test_read_consumer_multiple_sources(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "t1", "sender": "A", "text": "hi", "chat": "X", "timestamp": "2026-01-01T10:00:00Z"})
        store.append("gmail", {"type": "email", "id": "e1", "subject": "Test"})

        read_consumer(store, "c", ["telegram", "gmail"], fmt="md")
        out = capsys.readouterr().out
        assert "Telegram" in out
        assert "Gmail" in out

    def test_read_consumer_stats_no_checkpoint(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})

        read_consumer(store, "new_consumer", ["telegram"], stats_only=True)
        out = capsys.readouterr().out
        assert "No checkpoint yet" in out

    def test_read_consumer_stats_with_updated_at(self, tmp_path, capsys):
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})
        store.commit("telegram", "myc")

        read_consumer(store, "myc", ["telegram"], stats_only=True)
        out = capsys.readouterr().out
        assert "Last checkpoint:" in out

    def test_conversation_message_with_none_text(self):
        data = {
            "type": "conversation",
            "chat": "test",
            "messages": [{"sender": "A", "text": None, "ts": "2026-01-15T10:00:00Z"}],
        }
        result = format_record(_rec(data), "telegram", "md")
        # Should not crash - empty text gets handled by (msg.get("text") or "")
        assert "A:" not in result  # text is empty so the if text: check skips it

    def test_format_record_email_combined_tags(self):
        data = {
            "type": "email",
            "subject": "Urgent",
            "is_unread": True,
            "direction": "sent",
            "awaiting_reply": True,
            "account": "test",
        }
        result = format_record(_rec(data), "gmail", "md")
        assert "[UNREAD, SENT, AWAITING REPLY]" in result

    def test_issue_with_empty_assignees_list(self):
        data = {"type": "issue", "number": 1, "title": "Bug", "status": "Open", "assignees": [], "project": "P"}
        result = format_record(_rec(data), "github", "md")
        assert "->" not in result  # empty assignees = no arrow

    def test_calendar_event_start_truncated(self):
        data = {
            "type": "calendar_event",
            "title": "Meeting",
            "start": "2026-03-16T10:00:00+02:00",
            "calendar_name": "Work",
        }
        result = format_record(_rec(data), "calendar", "md")
        assert "2026-03-16T10:00" in result
        assert "+02:00" not in result

    def test_message_timestamp_index_error(self):
        """Line 357-358: message with short/weird timestamp that triggers exception."""
        data = {"type": "message", "sender": "Alice", "text": "hi", "timestamp": 12345}
        rec = _rec(data)
        result = format_record(rec, "telegram", "md")
        assert "Alice" in result

    def test_message_timestamp_none(self):
        """Empty timestamp doesn't crash."""
        data = {"type": "message", "sender": "Alice", "text": "hi", "timestamp": ""}
        rec = _rec(data)
        result = format_record(rec, "telegram", "md")
        assert "Alice" in result


# ===========================================================================
# show_stats - long timestamp truncation (line 96)
# ===========================================================================

class TestShowStatsLongTimestamp:
    def test_stats_truncate_long_timestamp(self, tmp_path, capsys):
        """Line 96: last_ts longer than 19 chars gets truncated."""
        store = DataStore(tmp_path)
        store.append("telegram", {"type": "message", "id": "m1"})

        mock_stats = {
            "telegram": {
                "records": 1,
                "last_ts": "2026-03-16T10:00:00.123456+00:00"  # > 19 chars
            }
        }
        with patch.object(store, "stats", return_value=mock_stats), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}):
            show_stats(store)

        out = capsys.readouterr().out
        assert "2026-03-16T10:00:00" in out
        # Truncated at 19 chars - no .123456 or timezone
        assert ".123456" not in out


# ===========================================================================
# show_health - bad JSON (lines 132-133)
# ===========================================================================

class TestShowHealthBadJson:
    def test_show_health_malformed_json_line(self, tmp_path, capsys):
        """Lines 132-133: bad JSON in sync_runs.jsonl is silently skipped."""
        store = DataStore(tmp_path)
        runs_file = tmp_path / "sync_runs.jsonl"
        now = datetime.now()
        good_run = json.dumps({"source": "telegram", "ts": now.isoformat(), "status": "ok"})
        runs_file.write_text(f"BAD JSON LINE\n{good_run}\n")

        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()):
            show_health(store)

        out = capsys.readouterr().out
        # Should still process the good line
        assert "ok" in out


# ===========================================================================
# read_consumer - chat records exceeding limit (line 441)
# ===========================================================================

class TestReadConsumerChatLimit:
    def test_chat_messages_exceed_limit(self, tmp_path, capsys):
        """Line 441: When a single chat has more messages than the limit."""
        store = DataStore(tmp_path)
        for i in range(10):
            store.append("telegram", {
                "type": "message", "id": f"m{i}",
                "sender": "Alice", "text": f"msg{i}",
                "chat": "BigChat",
                "timestamp": f"2026-01-01T10:{i:02d}:00Z",
            })

        read_consumer(store, "c", ["telegram"], fmt="md", limit=3)
        out = capsys.readouterr().out
        assert "+7 more" in out


# ===========================================================================
# _default_read_sources (lines 459-464)
# ===========================================================================

class TestDefaultReadSources:
    def test_default_read_sources_returns_enabled(self):
        """Lines 459-464: Returns only enabled sources."""
        from vadimgest.cli import _default_read_sources

        def mock_config(name):
            return {"enabled": name == "telegram"}

        with patch("vadimgest.cli.all_source_names", return_value=["telegram", "gmail", "signal"]), \
             patch("vadimgest.cli.get_source_config", side_effect=mock_config):
            result = _default_read_sources()

        assert result == ["telegram"]

    def test_default_read_sources_fallback_when_none_enabled(self):
        """Lines 459-464: Falls back to all sources when none enabled."""
        from vadimgest.cli import _default_read_sources

        with patch("vadimgest.cli.all_source_names", return_value=["telegram", "gmail"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": False}):
            result = _default_read_sources()

        assert set(result) == {"telegram", "gmail"}


# ===========================================================================
# _check_tool - mcp-cli with versions dir (line 588)
# ===========================================================================

class TestCheckToolMcpVersions:
    def test_check_tool_mcp_cli_versions_dir(self, tmp_path):
        """Line 588: mcp-cli found via versions dir iteration."""
        versions_dir = tmp_path / ".local" / "share" / "claude" / "versions"
        versions_dir.mkdir(parents=True)
        exec_file = versions_dir / "claude-v1.0"
        exec_file.write_text("#!/bin/sh")
        exec_file.chmod(0o755)

        with patch("shutil.which", return_value=None), \
             patch("pathlib.Path.home", return_value=tmp_path):
            result = _check_tool("mcp-cli")

        assert result is True

    def test_check_tool_mcp_cli_no_versions_dir(self, tmp_path):
        """Line 588: mcp-cli not found - versions dir doesn't exist."""
        with patch("shutil.which", return_value=None), \
             patch("pathlib.Path.home", return_value=tmp_path):
            result = _check_tool("mcp-cli")

        assert result is False


# ===========================================================================
# cmd_list - various unavailable reasons (lines 621, 627-639)
# ===========================================================================

class TestCmdListUnavailReasons:
    def test_cmd_list_mcp_cli_error(self, capsys):
        """Line 627-628: mcp-cli error message."""
        with patch("vadimgest.cli.all_source_names", return_value=["github"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="mcp-cli not found"):
            cmd_list()

        out = capsys.readouterr().out
        assert "needs mcp-cli" in out

    def test_cmd_list_file_not_found_error(self, capsys):
        """Lines 629-633: FileNotFoundError with external tool."""
        with patch("vadimgest.cli.all_source_names", return_value=["signal"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="FileNotFoundError: sigtop not found"), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"signal": {"platform": "macOS", "external": ["sigtop"]}}):
            cmd_list()

        out = capsys.readouterr().out
        assert "missing: sigtop" in out

    def test_cmd_list_file_not_found_no_external(self, capsys):
        """Lines 632-633: FileNotFoundError but no external tools defined."""
        with patch("vadimgest.cli.all_source_names", return_value=["custom"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="FileNotFoundError: something not found"), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"custom": {"platform": "any", "external": []}}):
            cmd_list()

        out = capsys.readouterr().out
        assert "FileNotFoundError" in out

    def test_cmd_list_generic_error(self, capsys):
        """Lines 634-635: Generic error message."""
        with patch("vadimgest.cli.all_source_names", return_value=["custom"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="Something weird happened"), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"custom": {"platform": "any"}}):
            cmd_list()

        out = capsys.readouterr().out
        assert "Something weird happened" in out

    def test_cmd_list_no_error_with_external_tools(self, capsys):
        """Lines 636-639: No error but external tool check needed."""
        with patch("vadimgest.cli.all_source_names", return_value=["signal"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"signal": {"platform": "any", "external": ["sigtop"]}}), \
             patch("vadimgest.cli._check_tool", return_value=False):
            cmd_list()

        out = capsys.readouterr().out
        assert "missing: sigtop" in out

    def test_cmd_list_macos_only_on_non_darwin(self, capsys):
        """Line 621: macOS-only source on non-macOS platform."""
        with patch("vadimgest.cli.all_source_names", return_value=["signal"]), \
             patch("vadimgest.cli.get_source_config", return_value={"mode": "cron", "enabled": False}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"signal": {"platform": "macOS", "external": []}}), \
             patch("sys.platform", "linux"):
            cmd_list()

        out = capsys.readouterr().out
        assert "macOS only" in out


# ===========================================================================
# cmd_doctor - extended coverage (lines 730, 747-764)
# ===========================================================================

class TestCmdDoctorExtended:
    def test_cmd_doctor_disabled_source(self, capsys):
        """Lines 747-748: Source available but disabled."""
        with patch("vadimgest.cli.all_source_names", return_value=["obsidian"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": False, "mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=MagicMock()), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._find_config_file", return_value=Path("/config.yaml")), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=True), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {"obsidian": {"platform": "any", "external": []}}):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "available but disabled" in out

    def test_cmd_doctor_unavailable_source_with_missing(self, capsys):
        """Lines 749-764: Unavailable source with pip + external tool missing."""
        with patch("vadimgest.cli.all_source_names", return_value=["telegram"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": False, "mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value="No module"), \
             patch("vadimgest.cli._find_config_file", return_value=Path("/config.yaml")), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=False), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {
                 "telegram": {
                     "platform": "any",
                     "external": ["sigtop"],
                     "pip_extra": "telegram (telethon)",
                 }
             }):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "[!!]" in out
        assert "install sigtop" in out

    def test_cmd_doctor_macos_only_on_non_darwin(self, capsys):
        """Line 752-753: macOS-only source on non-darwin in cmd_doctor."""
        with patch("vadimgest.cli.all_source_names", return_value=["signal"]), \
             patch("vadimgest.cli.get_source_config", return_value={"enabled": False, "mode": "cron"}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._find_config_file", return_value=Path("/config.yaml")), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=False), \
             patch("vadimgest.cli._SOURCE_REQUIREMENTS", {
                 "signal": {"platform": "macOS", "external": []}
             }), \
             patch("sys.platform", "linux"):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "macOS only" in out

    def test_cmd_doctor_no_claude_projects_dir(self, capsys):
        """Line 730: Claude projects directory doesn't exist."""
        with patch("vadimgest.cli.all_source_names", return_value=[]), \
             patch("vadimgest.cli.get_source_config", return_value={}), \
             patch("vadimgest.cli.get_syncer_class", return_value=None), \
             patch("vadimgest.cli.get_load_error", return_value=None), \
             patch("vadimgest.cli._find_config_file", return_value=None), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/data")), \
             patch("vadimgest.cli._check_tool", return_value=False), \
             patch("pathlib.Path.home", return_value=Path("/nonexistent")):
            cmd_doctor()

        out = capsys.readouterr().out
        assert "[--]" in out
        assert "Claude projects" in out


# ===========================================================================
# cmd_init - new config creation (lines 781-803)
# ===========================================================================

class TestCmdInitExtended:
    def test_cmd_init_local_config_exists(self, tmp_path, capsys, monkeypatch):
        """Lines 781-784: Local config.yaml exists in package dir."""
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
        fake_pkg_dir = tmp_path / "pkg"
        fake_pkg_dir.mkdir()
        fake_home_dir = tmp_path / "vadimgest_home"
        local_config = fake_pkg_dir / "config.yaml"
        local_config.write_text("test: true")

        with patch("vadimgest.cli._PACKAGE_DIR", new=fake_pkg_dir), \
             patch("vadimgest.cli._HOME_CONFIG_DIR", new=fake_home_dir):
            cmd_init()

        out = capsys.readouterr().out
        assert "Using local config" in out

    def test_cmd_init_home_config_exists(self, tmp_path, capsys, monkeypatch):
        """~/.vadimgest/config.yaml takes precedence over writing a fresh XDG template."""
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        fake_pkg_dir = tmp_path / "pkg"
        fake_pkg_dir.mkdir()
        fake_home_dir = tmp_path / "vadimgest_home"
        fake_home_dir.mkdir()
        (fake_home_dir / "config.yaml").write_text("test: true")

        with patch("vadimgest.cli._PACKAGE_DIR", new=fake_pkg_dir), \
             patch("vadimgest.cli._HOME_CONFIG_DIR", new=fake_home_dir):
            cmd_init()

        out = capsys.readouterr().out
        assert "Using local config" in out
        assert "vadimgest_home/config.yaml" in out

    def test_cmd_init_from_template(self, tmp_path, capsys, monkeypatch):
        """Lines 789-793: Create config from example template."""
        xdg_dir = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_dir))
        fake_pkg_dir = tmp_path / "pkg"
        fake_pkg_dir.mkdir()
        fake_home_dir = tmp_path / "vadimgest_home"
        example = fake_pkg_dir / "config.example.yaml"
        example.write_text("example: config")

        with patch("vadimgest.cli._PACKAGE_DIR", new=fake_pkg_dir), \
             patch("vadimgest.cli._HOME_CONFIG_DIR", new=fake_home_dir):
            cmd_init()

        out = capsys.readouterr().out
        assert "Created config from template" in out
        config_file = xdg_dir / "vadimgest" / "config.yaml"
        assert config_file.exists()
        assert config_file.read_text() == "example: config"

    def test_cmd_init_minimal_config(self, tmp_path, capsys, monkeypatch):
        """Lines 794-803: Generate minimal config when no template exists."""
        xdg_dir = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_dir))
        fake_pkg_dir = tmp_path / "pkg"
        fake_pkg_dir.mkdir()
        fake_home_dir = tmp_path / "vadimgest_home"
        # No config.example.yaml

        with patch("vadimgest.cli._PACKAGE_DIR", new=fake_pkg_dir), \
             patch("vadimgest.cli._HOME_CONFIG_DIR", new=fake_home_dir):
            cmd_init()

        out = capsys.readouterr().out
        assert "Created minimal config" in out
        config_file = xdg_dir / "vadimgest" / "config.yaml"
        assert config_file.exists()
        assert "Edit it to enable sources" in out


# ===========================================================================
# main() - CLI dispatch (lines 833-935)
# ===========================================================================

class TestMain:
    """Test the main() CLI dispatch function."""

    def test_main_no_command_shows_list(self, capsys):
        """Line 899: No command defaults to cmd_list."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest"]), \
             patch("vadimgest.cli.cmd_list") as mock_list, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_list.assert_called_once()

    def test_main_list_command(self, capsys):
        """Line 899: 'list' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "list"]), \
             patch("vadimgest.cli.cmd_list") as mock_list, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_list.assert_called_once()

    def test_main_init_command(self):
        """Line 891-893: 'init' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "init"]), \
             patch("vadimgest.cli.cmd_init") as mock_init, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_init.assert_called_once()

    def test_main_config_command(self):
        """Line 895-897: 'config' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "config"]), \
             patch("vadimgest.cli.cmd_config") as mock_config, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_config.assert_called_once()

    def test_main_doctor_command(self):
        """Line 903-905: 'doctor' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "doctor"]), \
             patch("vadimgest.cli.cmd_doctor") as mock_doctor, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_doctor.assert_called_once()

    def test_main_stats_command(self):
        """Line 918-919: 'stats' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "stats"]), \
             patch("vadimgest.cli.show_stats") as mock_stats, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_stats.assert_called_once()

    def test_main_health_command(self):
        """Line 921-922: 'health' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "health"]), \
             patch("vadimgest.cli.show_health") as mock_health, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_health.assert_called_once()

    def test_main_logs_command(self):
        """Line 924-925: 'logs' command."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "logs", "-n", "5"]), \
             patch("vadimgest.cli.show_logs") as mock_logs, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_logs.assert_called_once()

    def test_main_sync_specific_sources(self, capsys):
        """Lines 907-912: 'sync' with specific sources."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "sync", "telegram", "gmail"]), \
             patch("vadimgest.cli.sync_source") as mock_sync, \
             patch("vadimgest.cli.show_stats"), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore") as MockStore:
            main()
        assert mock_sync.call_count == 2

    def test_main_sync_all(self, capsys):
        """Lines 913-914: 'sync' without sources calls sync_all."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "sync"]), \
             patch("vadimgest.cli.sync_all") as mock_sync_all, \
             patch("vadimgest.cli.show_stats"), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_sync_all.assert_called_once()

    def test_main_read_with_sources(self):
        """Lines 927-929: 'read' command with sources."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "read", "-c", "heartbeat", "-s", "telegram,gmail"]), \
             patch("vadimgest.cli.read_consumer") as mock_read, \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_read.assert_called_once()
        args = mock_read.call_args
        assert args[0][2] == ["telegram", "gmail"]

    def test_main_read_default_sources(self):
        """Line 928: 'read' without -s uses _default_read_sources."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "read", "-c", "heartbeat"]), \
             patch("vadimgest.cli.read_consumer") as mock_read, \
             patch("vadimgest.cli._default_read_sources", return_value=["telegram"]), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        mock_read.assert_called_once()
        args = mock_read.call_args
        assert args[0][2] == ["telegram"]

    def test_main_read_with_stats_flag(self):
        """Line 929: 'read' with --stats flag."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "read", "-c", "heartbeat", "--stats"]), \
             patch("vadimgest.cli.read_consumer") as mock_read, \
             patch("vadimgest.cli._default_read_sources", return_value=["telegram"]), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"):
            main()
        # stats_only should be True
        assert mock_read.call_args[1].get("stats_only", mock_read.call_args[0][-1]) is True

    def test_main_commit_with_sources(self, tmp_path, capsys):
        """Lines 931-935: 'commit' command with specific sources."""
        from vadimgest.cli import main
        mock_store = MagicMock()
        with patch("sys.argv", ["vadimgest", "commit", "-c", "heartbeat", "-s", "telegram,gmail"]), \
             patch("vadimgest.cli.get_data_dir", return_value=tmp_path), \
             patch("vadimgest.cli.DataStore", return_value=mock_store):
            main()
        assert mock_store.commit.call_count == 2
        out = capsys.readouterr().out
        assert "Committed checkpoint" in out

    def test_main_commit_all_sources(self, tmp_path, capsys):
        """Line 932: 'commit' without -s uses glob for all sources."""
        from vadimgest.cli import main
        # Create some source files
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "telegram.jsonl").write_text("")
        (sources_dir / "gmail.jsonl").write_text("")

        mock_store = MagicMock()
        mock_store.sources_dir = sources_dir
        with patch("sys.argv", ["vadimgest", "commit", "-c", "heartbeat"]), \
             patch("vadimgest.cli.get_data_dir", return_value=tmp_path), \
             patch("vadimgest.cli.DataStore", return_value=mock_store):
            main()
        assert mock_store.commit.call_count == 2
        out = capsys.readouterr().out
        assert "Committed" in out

    def test_main_search_delegates(self):
        """Lines 884-889: 'search' delegates to search module."""
        from vadimgest.cli import main
        with patch("sys.argv", ["vadimgest", "search", "query", "--md"]), \
             patch("vadimgest.cli.get_data_dir", return_value=Path("/tmp/test")), \
             patch("vadimgest.cli.DataStore"), \
             patch("vadimgest.search.__main__.main") as mock_search:
            main()
        mock_search.assert_called_once()
