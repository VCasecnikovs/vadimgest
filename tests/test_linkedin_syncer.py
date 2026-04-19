"""Tests for LinkedIn syncer - full message thread fetching.

Unit tests mock Playwright, integration test hits real LinkedIn API.
"""

import hashlib
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone

from vadimgest.ingest.sources.linkedin.syncer import LinkedInSyncer


# --- Fixtures ---

@pytest.fixture
def store(tmp_path):
    """Mock DataStore with real Path for base_path."""
    s = MagicMock()
    s.base_path = tmp_path
    return s


@pytest.fixture
def syncer(store):
    """LinkedInSyncer with mocked store."""
    config = {"enabled": True, "max_conversations": 50}
    with patch("vadimgest.ingest.sources.linkedin.syncer.get_source_config", return_value=config):
        return LinkedInSyncer(store, config)


def make_gql_participant(first="John", last="Doe", is_org=False):
    """Build a GraphQL participant dict."""
    if is_org:
        return {
            "participantType": {
                "organization": {"name": {"text": first}}
            }
        }
    return {
        "participantType": {
            "member": {
                "firstName": {"text": first},
                "lastName": {"text": last},
            }
        }
    }


def make_gql_message(sender_first="John", sender_last="Doe", body="Hello",
                     delivered_at=1700000000000):
    """Build a GraphQL message dict."""
    return {
        "actor": make_gql_participant(sender_first, sender_last),
        "body": {"text": body},
        "deliveredAt": delivered_at,
    }


def make_conversation(participants, messages, conv_id="conv123",
                      unread_count=0, last_activity=1700000000000):
    """Build a conversation dict as returned by conversations endpoint."""
    return {
        "entityUrn": f"urn:li:msg_conversation:(urn:li:fsd_profile:abc,{conv_id})",
        "conversationParticipants": participants,
        "messages": {"elements": messages},
        "unreadCount": unread_count,
        "lastActivityAt": last_activity,
    }


# --- Unit Tests ---

class TestMakeRecordId:
    """Record IDs must be stable and deterministic."""

    def test_stable_id(self, syncer):
        id1 = syncer._make_record_id("John Doe", "Hello", "conv1")
        id2 = syncer._make_record_id("John Doe", "Hello", "conv1")
        assert id1 == id2

    def test_different_content_different_id(self, syncer):
        id1 = syncer._make_record_id("John", "Hello", "conv1")
        id2 = syncer._make_record_id("John", "Hi", "conv1")
        assert id1 != id2

    def test_different_conv_different_id(self, syncer):
        id1 = syncer._make_record_id("John", "Hello", "conv1")
        id2 = syncer._make_record_id("John", "Hello", "conv2")
        assert id1 != id2

    def test_id_format(self, syncer):
        rid = syncer._make_record_id("John Doe", "Hello", "conv1")
        assert rid.startswith("li_msg_john_doe_")
        assert len(rid) > len("li_msg_john_doe_")

    def test_long_name_truncated(self, syncer):
        long_name = "A" * 100
        rid = syncer._make_record_id(long_name, "Hello", "conv1")
        # safe_name is truncated to 30 chars
        prefix = rid.split("_", 3)  # li, msg, name_part, hash
        assert len(rid) < 60


class TestParticipantName:
    """Extract names from various participant formats."""

    def test_member_name(self, syncer):
        p = make_gql_participant("Jane", "Smith")
        assert syncer._gql_participant_name(p) == "Jane Smith"

    def test_org_name(self, syncer):
        p = make_gql_participant("LinkedIn for Marketing", is_org=True)
        assert syncer._gql_participant_name(p) == "LinkedIn for Marketing"

    def test_unknown_when_empty(self, syncer):
        p = {"participantType": {}}
        assert syncer._gql_participant_name(p) == "Unknown"

    def test_string_first_name(self, syncer):
        """Handle case where firstName is a raw string, not dict."""
        p = {
            "participantType": {
                "member": {
                    "firstName": "Jane",
                    "lastName": "Smith",
                }
            }
        }
        assert syncer._gql_participant_name(p) == "Jane Smith"


class TestMsgBody:
    """Extract body text from messages."""

    def test_dict_body(self, syncer):
        msg = {"body": {"text": "Hello world"}}
        assert syncer._gql_msg_body(msg) == "Hello world"

    def test_string_body(self, syncer):
        msg = {"body": "Hello world"}
        assert syncer._gql_msg_body(msg) == "Hello world"

    def test_empty_body(self, syncer):
        msg = {"body": {"text": ""}}
        assert syncer._gql_msg_body(msg) == ""


class TestMsgSender:
    """Extract sender from actor or sender field."""

    def test_from_actor(self, syncer):
        msg = {
            "actor": make_gql_participant("Jane", "Doe"),
            "sender": make_gql_participant("Wrong", "Person"),
        }
        assert syncer._gql_msg_sender(msg) == "Jane Doe"

    def test_fallback_to_sender(self, syncer):
        msg = {
            "sender": make_gql_participant("Jane", "Doe"),
        }
        assert syncer._gql_msg_sender(msg) == "Jane Doe"

    def test_missing_both(self, syncer):
        msg = {}
        assert syncer._gql_msg_sender(msg) == "Unknown"


class TestParseConversationMeta:
    """Extract metadata from conversation objects."""

    def test_basic_conversation(self, syncer):
        conv = make_conversation(
            participants=[
                make_gql_participant("John", "Smith"),
                make_gql_participant("Jane", "Doe"),
            ],
            messages=[make_gql_message()],
            conv_id="conv123",
            unread_count=2,
        )
        conv_urn, conv_id, names, unread = syncer._parse_conversation_meta(conv)
        assert "conv123" in conv_id
        assert "John Smith" in names
        assert "Jane Doe" in names
        assert unread is True

    def test_no_unread(self, syncer):
        conv = make_conversation(
            participants=[make_gql_participant("John", "Doe")],
            messages=[make_gql_message()],
            unread_count=0,
        )
        _, _, _, unread = syncer._parse_conversation_meta(conv)
        assert unread is False

    def test_filters_unknown_participants(self, syncer):
        conv = make_conversation(
            participants=[
                make_gql_participant("Jane", "Doe"),
                {"participantType": {}},  # Unknown
            ],
            messages=[make_gql_message()],
        )
        _, _, names, _ = syncer._parse_conversation_meta(conv)
        assert "Jane Doe" in names
        assert "Unknown" not in names


class TestFetchConversationMessages:
    """Fetch full message threads for a conversation."""

    def test_multiple_messages(self, syncer):
        """Should return a record for each message, not just the first."""
        mock_page = MagicMock()

        gql_response = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "Hello!", 1700000001000),
                        make_gql_message("Bob", "B", "Hi there!", 1700000002000),
                        make_gql_message("Alice", "A", "How are you?", 1700000003000),
                    ]
                }
            }
        }
        mock_page.evaluate.return_value = {
            "status": 200,
            "body": json.dumps(gql_response),
        }

        records = syncer._fetch_conversation_messages(
            mock_page,
            conv_urn="urn:li:msg_conversation:(urn:li:fsd_profile:abc,conv1)",
            conv_id="conv1",
            participant_names=["Alice A", "Bob B"],
            unread=False,
        )

        assert len(records) == 3
        assert records[0]["sender"] == "Alice A"
        assert records[0]["body"] == "Hello!"
        assert records[1]["sender"] == "Bob B"
        assert records[1]["body"] == "Hi there!"
        assert records[2]["sender"] == "Alice A"
        assert records[2]["body"] == "How are you?"

    def test_all_records_have_required_fields(self, syncer):
        mock_page = MagicMock()
        gql_response = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "Test", 1700000000000),
                    ]
                }
            }
        }
        mock_page.evaluate.return_value = {
            "status": 200,
            "body": json.dumps(gql_response),
        }

        records = syncer._fetch_conversation_messages(
            mock_page, "urn:conv", "conv1", ["Alice A"], False
        )

        assert len(records) == 1
        r = records[0]
        assert "id" in r
        assert "type" in r and r["type"] == "linkedin_message"
        assert "sender" in r
        assert "body" in r
        assert "timestamp" in r
        assert "participants" in r
        assert "conversation_id" in r
        assert "meta" in r

    def test_skips_empty_body(self, syncer):
        mock_page = MagicMock()
        gql_response = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "", 1700000000000),
                        make_gql_message("Bob", "B", "Real message", 1700000001000),
                    ]
                }
            }
        }
        mock_page.evaluate.return_value = {
            "status": 200,
            "body": json.dumps(gql_response),
        }

        records = syncer._fetch_conversation_messages(
            mock_page, "urn:conv", "conv1", ["Alice A", "Bob B"], False
        )
        assert len(records) == 1
        assert records[0]["sender"] == "Bob B"

    def test_api_error_returns_empty(self, syncer):
        """GraphQL error should return empty list, not crash."""
        mock_page = MagicMock()
        mock_page.evaluate.return_value = {
            "status": 400,
            "body": '{"error": "bad request"}',
        }

        records = syncer._fetch_conversation_messages(
            mock_page, "urn:conv", "conv1", ["Alice A"], False
        )
        assert records == []

    def test_record_ids_are_unique_per_message(self, syncer):
        """Different messages in same conversation should have different IDs."""
        mock_page = MagicMock()
        gql_response = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "Message 1", 1700000001000),
                        make_gql_message("Alice", "A", "Message 2", 1700000002000),
                    ]
                }
            }
        }
        mock_page.evaluate.return_value = {
            "status": 200,
            "body": json.dumps(gql_response),
        }

        records = syncer._fetch_conversation_messages(
            mock_page, "urn:conv", "conv1", ["Alice A"], False
        )
        ids = [r["id"] for r in records]
        assert len(set(ids)) == 2  # all unique

    def test_timestamp_format(self, syncer):
        """Timestamps should be ISO format."""
        mock_page = MagicMock()
        gql_response = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "Hello", 1700000000000),
                    ]
                }
            }
        }
        mock_page.evaluate.return_value = {
            "status": 200,
            "body": json.dumps(gql_response),
        }

        records = syncer._fetch_conversation_messages(
            mock_page, "urn:conv", "conv1", ["Alice A"], False
        )
        ts = records[0]["timestamp"]
        # Should parse as ISO format
        parsed = datetime.fromisoformat(ts)
        assert parsed.year == 2023


class TestFetchNewIntegration:
    """Test the full fetch_new flow with mocked browser."""

    def test_two_phase_flow(self, syncer):
        """fetch_new should: get conversations -> fetch messages for each."""
        mock_page = MagicMock()

        # Sequence of evaluate calls:
        # 1. _get_my_urn -> /me API
        # 2. _gql_fetch conversations
        # 3. _gql_fetch messages for conv1
        # 4. _gql_fetch messages for conv2

        me_response = {
            "included": [
                {"dashEntityUrn": "urn:li:fsd_profile:abc123"}
            ]
        }

        conv_response = {
            "data": {
                "messengerConversationsBySyncToken": {
                    "elements": [
                        make_conversation(
                            [make_gql_participant("Alice", "A"),
                             make_gql_participant("John", "S")],
                            [make_gql_message("Alice", "A", "Latest")],
                            conv_id="conv1", last_activity=1700000002000,
                        ),
                        make_conversation(
                            [make_gql_participant("Bob", "B"),
                             make_gql_participant("John", "S")],
                            [make_gql_message("Bob", "B", "Latest")],
                            conv_id="conv2", last_activity=1700000001000,
                        ),
                    ]
                }
            }
        }

        msgs_conv1 = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", "Hi!", 1700000001000),
                        make_gql_message("John", "S", "Hey!", 1700000002000),
                    ]
                }
            }
        }

        msgs_conv2 = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Bob", "B", "Hello", 1700000001000),
                    ]
                }
            }
        }

        call_count = 0
        responses = [
            {"status": 200, "body": json.dumps(me_response)},       # /me
            {"status": 200, "body": json.dumps(conv_response)},      # conversations
            {"status": 200, "body": json.dumps(msgs_conv1)},         # messages conv1
            {"status": 200, "body": json.dumps(msgs_conv2)},         # messages conv2
        ]

        def side_effect(*args, **kwargs):
            nonlocal call_count
            resp = responses[min(call_count, len(responses) - 1)]
            call_count += 1
            return resp

        mock_page.evaluate.side_effect = side_effect
        mock_page.url = "https://www.linkedin.com/feed/"

        mock_browser = MagicMock()
        mock_browser.pages = [mock_page]

        with patch("vadimgest.ingest.sources.linkedin.syncer.sync_playwright") as mock_pw:
            mock_pw_instance = MagicMock()
            mock_pw.return_value.start.return_value = mock_pw_instance
            mock_pw_instance.chromium.launch_persistent_context.return_value = mock_browser

            from vadimgest.models import SourceState
            records = list(syncer.fetch_new(SourceState()))

        # Should have 3 records total (2 from conv1 + 1 from conv2)
        assert len(records) == 3
        bodies = [r["body"] for r in records]
        assert "Hi!" in bodies
        assert "Hey!" in bodies
        assert "Hello" in bodies

    def test_respects_limit(self, syncer):
        """fetch_new should stop after reaching limit."""
        mock_page = MagicMock()

        me_response = {
            "included": [{"dashEntityUrn": "urn:li:fsd_profile:abc"}]
        }
        conv_response = {
            "data": {
                "messengerConversationsBySyncToken": {
                    "elements": [
                        make_conversation(
                            [make_gql_participant("Alice", "A")],
                            [make_gql_message()],
                            conv_id="conv1",
                        ),
                    ]
                }
            }
        }
        msgs = {
            "data": {
                "messengerMessagesBySyncToken": {
                    "elements": [
                        make_gql_message("Alice", "A", f"Msg {i}", 1700000000000 + i * 1000)
                        for i in range(10)
                    ]
                }
            }
        }

        responses = [
            {"status": 200, "body": json.dumps(me_response)},
            {"status": 200, "body": json.dumps(conv_response)},
            {"status": 200, "body": json.dumps(msgs)},
        ]
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            resp = responses[min(call_count, len(responses) - 1)]
            call_count += 1
            return resp

        mock_page.evaluate.side_effect = side_effect
        mock_page.url = "https://www.linkedin.com/feed/"

        mock_browser = MagicMock()
        mock_browser.pages = [mock_page]

        with patch("vadimgest.ingest.sources.linkedin.syncer.sync_playwright") as mock_pw:
            mock_pw_instance = MagicMock()
            mock_pw.return_value.start.return_value = mock_pw_instance
            mock_pw_instance.chromium.launch_persistent_context.return_value = mock_browser

            from vadimgest.models import SourceState
            records = list(syncer.fetch_new(SourceState(), limit=3))

        assert len(records) == 3


# --- Live Integration Test (requires LinkedIn session) ---

@pytest.mark.skipif(
    not __import__("os").path.exists(__import__("os").path.expanduser("~/.linkedin_browser")),
    reason="No LinkedIn browser session"
)
class TestLiveLinkedIn:
    """Live tests against real LinkedIn API. Requires authenticated browser session."""

    @pytest.mark.timeout(120)
    def test_fetch_first_conversations(self):
        """Fetch a few conversations and verify we get full threads."""
        from vadimgest.store import DataStore
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            store = DataStore(tmpdir)
            config = {"enabled": True, "max_conversations": 5}

            with patch("vadimgest.ingest.sources.linkedin.syncer.get_source_config", return_value=config):
                syncer = LinkedInSyncer(store, config)

            from vadimgest.models import SourceState
            records = list(syncer.fetch_new(SourceState(), limit=100))

            print(f"\nFetched {len(records)} messages from up to 5 conversations")

            assert len(records) > 0, "Should fetch at least some messages"

            # Verify record structure
            for r in records:
                assert "id" in r
                assert "type" in r and r["type"] == "linkedin_message"
                assert "sender" in r
                assert "body" in r and len(r["body"]) > 0
                assert "timestamp" in r
                assert "participants" in r and len(r["participants"]) > 0
                assert "conversation_id" in r
                assert "meta" in r

            # Check we got multiple messages for at least one conversation
            from collections import Counter
            conv_counts = Counter(r["conversation_id"] for r in records)
            multi_msg_convos = [cid for cid, cnt in conv_counts.items() if cnt > 1]

            print(f"Conversations: {len(conv_counts)}")
            print(f"Conversations with 2+ messages: {len(multi_msg_convos)}")
            for cid, cnt in conv_counts.most_common(5):
                sample = next(r for r in records if r["conversation_id"] == cid)
                others = [p for p in sample["participants"] if p != "John Smith"]
                print(f"  {', '.join(others) or 'Unknown':30s} | {cnt} msgs")
