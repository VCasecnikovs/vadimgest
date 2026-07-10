"""Tests for ingest source modules: Telegram, Obsidian, XNews.

Covers helpers, filtering, record construction, and fetch_new logic
with all external I/O mocked.
"""

import sys
import os
import json
import base64
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock
from types import ModuleType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from vadimgest.store import DataStore
from vadimgest.models import SourceState


# ============================================================
# Mock telethon before importing telegram syncer
# ============================================================

def _ensure_telethon_mock():
    """Install mock telethon modules in sys.modules if not already present."""
    # Check if SQLiteSession is importable - if not, we need to mock
    try:
        from telethon.sessions import SQLiteSession  # noqa
        return  # Real telethon with SQLiteSession works fine
    except (ImportError, ModuleNotFoundError):
        pass
    # Remove partial real telethon if present
    for key in list(sys.modules.keys()):
        if key == "telethon" or key.startswith("telethon."):
            del sys.modules[key]

    # Create mock module hierarchy
    telethon = ModuleType("telethon")
    telethon.TelegramClient = MagicMock

    tl = ModuleType("telethon.tl")
    tl_types = ModuleType("telethon.tl.types")

    # Real-ish types for isinstance checks
    class MessageMediaDocument:
        def __init__(self, document=None, **kwargs):
            self.document = document

    class MessageMediaPhoto:
        def __init__(self, photo=None, **kwargs):
            self.photo = photo

    class DocumentAttributeAudio:
        def __init__(self, duration=0, voice=False, **kwargs):
            self.duration = duration
            self.voice = voice

    class DialogFilter:
        pass

    class DialogFilterDefault:
        pass

    tl_types.MessageMediaDocument = MessageMediaDocument
    tl_types.MessageMediaPhoto = MessageMediaPhoto
    tl_types.DocumentAttributeAudio = DocumentAttributeAudio
    tl_types.DialogFilter = DialogFilter
    tl_types.DialogFilterDefault = DialogFilterDefault

    sessions = ModuleType("telethon.sessions")
    sessions.StringSession = MagicMock
    sessions.SQLiteSession = MagicMock

    tl_functions = ModuleType("telethon.tl.functions")
    tl_functions_messages = ModuleType("telethon.tl.functions.messages")
    tl_functions_messages.TranscribeAudioRequest = MagicMock
    tl_functions_messages.GetDialogFiltersRequest = MagicMock

    messages_module = ModuleType("telethon.tl.functions.messages")
    messages_module.TranscribeAudioRequest = tl_functions_messages.TranscribeAudioRequest
    messages_module.GetDialogFiltersRequest = tl_functions_messages.GetDialogFiltersRequest

    functions_mod = ModuleType("telethon.tl.functions")
    functions_mod.messages = messages_module

    sys.modules["telethon"] = telethon
    sys.modules["telethon.tl"] = tl
    sys.modules["telethon.tl.types"] = tl_types
    sys.modules["telethon.sessions"] = sessions
    sys.modules["telethon.tl.functions"] = functions_mod
    sys.modules["telethon.tl.functions.messages"] = messages_module


_ensure_telethon_mock()

# Now safe to import
import vadimgest.ingest.sources.telegram.syncer as _tg_mod
import vadimgest.ingest.sources.obsidian.syncer as _obs_mod
import vadimgest.ingest.sources.xnews.syncer as _xn_mod

TelegramSyncer = _tg_mod.TelegramSyncer
ObsidianSyncer = _obs_mod.ObsidianSyncer
XNewsSyncer = _xn_mod.XNewsSyncer

# Get the mock types we created
_tl_types = sys.modules["telethon.tl.types"]
MessageMediaDocument = _tl_types.MessageMediaDocument
MessageMediaPhoto = _tl_types.MessageMediaPhoto
DocumentAttributeAudio = _tl_types.DocumentAttributeAudio
DialogFilter = _tl_types.DialogFilter
DialogFilterDefault = _tl_types.DialogFilterDefault


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def tmp_store(tmp_path):
    """Create a temporary DataStore for tests."""
    return DataStore(tmp_path / "data")


@pytest.fixture
def telegram_syncer(tmp_store, tmp_path):
    """Telegram syncer with test config, mocked credentials dir."""
    creds_dir = tmp_path / "creds"
    creds_dir.mkdir()
    config = {
        "api_id": "12345",
        "api_hash": "abc123",
        "monitored_folders": [],
        "max_messages_per_chat": 200,
        "transcribe_voice": False,
        "download_media": False,
        "describe_images": False,
        "ocr_images": False,
        "exclude_patterns": ["Spam"],
    }
    with patch.object(_tg_mod, "get_source_config", return_value=config), \
         patch.object(_tg_mod, "get_credentials_dir", return_value=creds_dir):
        return TelegramSyncer(tmp_store, config)


@pytest.fixture
def obsidian_syncer(tmp_store, tmp_path):
    """Obsidian syncer with a fake vault directory."""
    vault = tmp_path / "vault"
    vault.mkdir()
    config = {
        "vault_path": str(vault),
        "skip_dirs": [".obsidian", ".trash", ".git"],
        "include_extensions": [".md"],
    }
    with patch.object(_obs_mod, "get_source_config", return_value=config):
        return ObsidianSyncer(tmp_store, config)


@pytest.fixture
def xnews_syncer(tmp_store):
    """XNews syncer with test config."""
    config = {"count": 10}
    with patch.object(_xn_mod, "get_source_config", return_value=config):
        return XNewsSyncer(tmp_store, config)


# ============================================================
# Telegram Syncer Tests
# ============================================================

class TestTelegramPeerId:
    """Tests for TelegramSyncer._peer_id."""

    def test_user_id(self, telegram_syncer):
        peer = MagicMock(spec=[])
        peer.user_id = 42
        assert telegram_syncer._peer_id(peer) == 42

    def test_channel_id(self, telegram_syncer):
        peer = MagicMock(spec=[])
        peer.channel_id = 100
        assert telegram_syncer._peer_id(peer) == 100

    def test_chat_id(self, telegram_syncer):
        peer = MagicMock(spec=[])
        peer.chat_id = 200
        assert telegram_syncer._peer_id(peer) == 200

    def test_no_id(self, telegram_syncer):
        peer = MagicMock(spec=[])
        assert telegram_syncer._peer_id(peer) == 0


class TestTelegramEntityName:
    """Tests for TelegramSyncer._entity_name."""

    def test_none_entity(self, telegram_syncer):
        assert telegram_syncer._entity_name(None) == "Unknown"

    def test_entity_with_title(self, telegram_syncer):
        entity = MagicMock()
        entity.title = "My Group"
        assert telegram_syncer._entity_name(entity) == "My Group"

    def test_entity_with_empty_title_returns_unknown(self, telegram_syncer):
        """Empty title -> returns Unknown (title attr exists but falsy)."""
        entity = type("E", (), {"title": "", "first_name": "John", "last_name": "Doe"})()
        assert telegram_syncer._entity_name(entity) == "Unknown"

    def test_entity_with_first_name_only(self, telegram_syncer):
        """No title attr -> falls through to first_name/last_name."""
        entity = type("E", (), {"first_name": "Alice"})()
        assert telegram_syncer._entity_name(entity) == "Alice"

    def test_entity_with_first_and_last(self, telegram_syncer):
        entity = type("E", (), {"first_name": "John", "last_name": "Doe"})()
        assert telegram_syncer._entity_name(entity) == "John Doe"

    def test_entity_no_name(self, telegram_syncer):
        entity = type("E", (), {})()
        assert telegram_syncer._entity_name(entity) == "Unknown"

    def test_entity_with_none_title_returns_unknown(self, telegram_syncer):
        """None title -> hasattr True, `None or 'Unknown'` returns Unknown."""
        entity = type("E", (), {"title": None, "first_name": "Bob"})()
        assert telegram_syncer._entity_name(entity) == "Unknown"


class TestTelegramShouldExclude:
    """Tests for TelegramSyncer._should_exclude."""

    def test_exclude_matching_pattern(self, telegram_syncer):
        assert telegram_syncer._should_exclude("SpamBot Chat") is True

    def test_exclude_case_insensitive(self, telegram_syncer):
        assert telegram_syncer._should_exclude("spam group") is True

    def test_no_exclude_normal_name(self, telegram_syncer):
        assert telegram_syncer._should_exclude("Normal Chat") is False

    def test_exclude_empty_name(self, telegram_syncer):
        assert telegram_syncer._should_exclude("") is False

    def test_exclude_none_name(self, telegram_syncer):
        assert telegram_syncer._should_exclude(None) is False

    def test_no_patterns_configured(self, telegram_syncer):
        telegram_syncer.config["exclude_patterns"] = []
        assert telegram_syncer._should_exclude("SpamBot") is False


class TestTelegramIsVoice:
    """Tests for TelegramSyncer._is_voice."""

    def test_not_voice_no_media(self, telegram_syncer):
        msg = MagicMock()
        msg.media = None
        assert telegram_syncer._is_voice(msg) is False

    def test_not_voice_text_message(self, telegram_syncer):
        msg = MagicMock()
        msg.media = "not a document"
        assert telegram_syncer._is_voice(msg) is False

    def test_not_voice_no_document(self, telegram_syncer):
        media = MessageMediaDocument(document=None)
        msg = MagicMock()
        msg.media = media
        assert telegram_syncer._is_voice(msg) is False

    def test_voice_message_real_types(self, telegram_syncer):
        audio_attr = DocumentAttributeAudio(duration=10, voice=True)
        doc = MagicMock()
        doc.attributes = [audio_attr]
        media = MessageMediaDocument(document=doc)
        msg = MagicMock()
        msg.media = media
        assert telegram_syncer._is_voice(msg) is True

    def test_not_voice_audio_not_voice(self, telegram_syncer):
        audio_attr = DocumentAttributeAudio(duration=180, voice=False)
        doc = MagicMock()
        doc.attributes = [audio_attr]
        media = MessageMediaDocument(document=doc)
        msg = MagicMock()
        msg.media = media
        assert telegram_syncer._is_voice(msg) is False

    def test_voice_no_attributes(self, telegram_syncer):
        doc = MagicMock()
        doc.attributes = []
        media = MessageMediaDocument(document=doc)
        msg = MagicMock()
        msg.media = media
        assert telegram_syncer._is_voice(msg) is False


class TestTelegramImages:
    """Tests for Telegram image media handling."""

    def test_photo_media_is_image(self, telegram_syncer):
        msg = MagicMock()
        msg.media = MessageMediaPhoto(photo=MagicMock())
        assert telegram_syncer._is_image(msg) is True

    def test_image_document_is_image(self, telegram_syncer):
        doc = MagicMock()
        doc.mime_type = "image/png"
        msg = MagicMock()
        msg.media = MessageMediaDocument(document=doc)
        assert telegram_syncer._is_image(msg) is True
        assert telegram_syncer._image_mime(msg) == "image/png"

    def test_non_image_document_is_not_image(self, telegram_syncer):
        doc = MagicMock()
        doc.mime_type = "application/pdf"
        msg = MagicMock()
        msg.media = MessageMediaDocument(document=doc)
        assert telegram_syncer._is_image(msg) is False

    def test_process_image_downloads_and_adds_context(self, telegram_syncer):
        telegram_syncer.config["download_media"] = True
        telegram_syncer.config["describe_images"] = True
        telegram_syncer.config["ocr_images"] = True

        async def fake_download_media(_msg, file):
            Path(file).write_bytes(b"fake image")
            return file

        client = AsyncMock()
        client.download_media.side_effect = fake_download_media

        msg = MagicMock()
        msg.id = 456
        msg.media = MessageMediaPhoto(photo=MagicMock())

        with patch.object(telegram_syncer, "_describe_image", return_value="A whiteboard sketch"), \
             patch.object(telegram_syncer, "_ocr_image", return_value="Visible text"):
            text, attachment = asyncio.run(telegram_syncer._process_image(client, msg, "123"))

        assert "A whiteboard sketch" in text
        assert "Visible text" in text
        assert attachment["type"] == "image"
        assert attachment["mime_type"] == "image/jpeg"
        assert Path(attachment["path"]).exists()


class TestTelegramGetClient:
    """Tests for TelegramSyncer._get_client."""

    def test_get_client_creates_client(self, telegram_syncer):
        with patch.object(_tg_mod, "TelegramClient") as mock_tc:
            mock_tc.return_value = MagicMock()
            client = telegram_syncer._get_client()
            mock_tc.assert_called_once()
            args = mock_tc.call_args
            assert args[0][1] == 12345  # api_id
            assert args[0][2] == "abc123"  # api_hash


class TestTelegramEnsureSqliteSession:
    """Tests for TelegramSyncer._ensure_sqlite_session."""

    def test_existing_session_returns_false(self, telegram_syncer):
        session_file = Path(telegram_syncer.session_path + ".session")
        session_file.parent.mkdir(parents=True, exist_ok=True)
        session_file.touch()
        result = asyncio.run(telegram_syncer._ensure_sqlite_session())
        assert result is False

    def test_no_session_file_raises(self, telegram_syncer):
        with pytest.raises(RuntimeError, match="No telegram session file found"):
            asyncio.run(telegram_syncer._ensure_sqlite_session())


class TestTelegramBuildChatRules:
    """Tests for TelegramSyncer._build_chat_rules."""

    def test_empty_config(self, telegram_syncer):
        client = AsyncMock()
        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert rules == {}

    def test_include_chat_ids(self, telegram_syncer):
        telegram_syncer.config["include_chat_ids"] = [123, 456]
        client = AsyncMock()
        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert "123" in rules
        assert "456" in rules
        assert rules["123"]["folder"] == "Manual"

    def test_exclude_chat_ids(self, telegram_syncer):
        telegram_syncer.config["include_chat_ids"] = [123, 456]
        telegram_syncer.config["exclude_chat_ids"] = [123]
        client = AsyncMock()
        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert "123" not in rules
        assert "456" in rules

    def test_monitored_folders(self, telegram_syncer):
        telegram_syncer.config["monitored_folders"] = ["Work"]

        peer = MagicMock()
        peer.user_id = 999
        dialog_filter = MagicMock(spec=DialogFilter)
        dialog_filter.title = "Work"
        dialog_filter.include_peers = [peer]

        default_filter = MagicMock(spec=DialogFilterDefault)

        result_obj = MagicMock()
        result_obj.filters = [default_filter, dialog_filter]

        client = AsyncMock()
        client.return_value = result_obj

        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert "999" in rules
        assert rules["999"]["folder"] == "Work"

    def test_monitored_folders_error(self, telegram_syncer):
        """Folder fetch error is caught gracefully."""
        telegram_syncer.config["monitored_folders"] = ["Broken"]
        client = AsyncMock()
        client.side_effect = Exception("API error")
        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert rules == {}

    def test_folder_title_with_text_attr(self, telegram_syncer):
        """DialogFilter.title may have a .text attribute."""
        telegram_syncer.config["monitored_folders"] = ["Work"]

        peer = MagicMock(spec=[])
        peer.channel_id = 777

        title_obj = MagicMock(spec=[])
        title_obj.text = "Work"
        dialog_filter = MagicMock(spec=DialogFilter)
        dialog_filter.title = title_obj
        dialog_filter.include_peers = [peer]

        result_obj = MagicMock()
        result_obj.filters = [dialog_filter]

        client = AsyncMock()
        client.return_value = result_obj

        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert "777" in rules

    def test_include_does_not_overwrite_folder_entries(self, telegram_syncer):
        telegram_syncer.config["monitored_folders"] = ["Work"]
        telegram_syncer.config["include_chat_ids"] = [999]

        peer = MagicMock()
        peer.user_id = 999
        dialog_filter = MagicMock(spec=DialogFilter)
        dialog_filter.title = "Work"
        dialog_filter.include_peers = [peer]

        result_obj = MagicMock()
        result_obj.filters = [dialog_filter]

        client = AsyncMock()
        client.return_value = result_obj

        rules = asyncio.run(telegram_syncer._build_chat_rules(client))
        assert rules["999"]["folder"] == "Work"


class TestTelegramTranscribeVoice:
    """Tests for TelegramSyncer._transcribe_voice."""

    def test_transcribe_success(self, telegram_syncer):
        client = AsyncMock()
        result = MagicMock()
        result.pending = False
        result.text = "Hello world"
        client.return_value = result

        msg = MagicMock()
        msg.peer_id = 123
        msg.id = 456

        text = asyncio.run(telegram_syncer._transcribe_voice(client, msg))
        assert text == "Hello world"

    def test_transcribe_pending_then_ready(self, telegram_syncer):
        client = AsyncMock()
        pending_result = MagicMock()
        pending_result.pending = True
        pending_result.text = ""

        ready_result = MagicMock()
        ready_result.pending = False
        ready_result.text = "Transcribed text"

        client.side_effect = [pending_result, ready_result]

        msg = MagicMock()
        msg.peer_id = 123
        msg.id = 456

        with patch.object(_tg_mod.asyncio, "sleep", new_callable=AsyncMock):
            text = asyncio.run(telegram_syncer._transcribe_voice(client, msg))
        assert text == "Transcribed text"

    def test_transcribe_failure(self, telegram_syncer):
        client = AsyncMock()
        client.side_effect = Exception("API error")

        msg = MagicMock()
        msg.peer_id = 123
        msg.id = 456

        text = asyncio.run(telegram_syncer._transcribe_voice(client, msg))
        assert text is None

    def test_transcribe_empty_text(self, telegram_syncer):
        client = AsyncMock()
        result = MagicMock()
        result.pending = False
        result.text = ""
        client.return_value = result

        msg = MagicMock()
        msg.peer_id = 123
        msg.id = 456

        text = asyncio.run(telegram_syncer._transcribe_voice(client, msg))
        assert text is None


class TestTelegramFetchNew:
    """Tests for TelegramSyncer.fetch_new."""

    def test_fetch_new_yields_records(self, telegram_syncer):
        mock_records = [{"id": "1_1", "text": "hello"}]
        with patch.object(_tg_mod.asyncio, "run", return_value=mock_records):
            state = SourceState()
            results = list(telegram_syncer.fetch_new(state))
            assert len(results) == 1
            assert results[0]["id"] == "1_1"

    def test_fetch_new_empty(self, telegram_syncer):
        with patch.object(_tg_mod.asyncio, "run", return_value=[]):
            state = SourceState()
            results = list(telegram_syncer.fetch_new(state))
            assert results == []


class TestTelegramSourceMetadata:
    """Tests for TelegramSyncer class attributes."""

    def test_metadata(self, telegram_syncer):
        assert telegram_syncer.source_name == "telegram"
        assert telegram_syncer.display_name == "Telegram"
        assert telegram_syncer.category == "messaging"
        assert "telethon" in telegram_syncer.dependencies["python"]

    def test_config_schema(self, telegram_syncer):
        schema = telegram_syncer.config_schema
        assert "monitored_folders" in schema
        assert "max_messages_per_chat" in schema
        assert "transcribe_voice" in schema


# ============================================================
# Obsidian Syncer Tests
# ============================================================

class TestObsidianShouldSkip:
    """Tests for ObsidianSyncer._should_skip."""

    def test_skip_obsidian_dir(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / ".obsidian" / "config.json"
        assert obsidian_syncer._should_skip(path) is True

    def test_skip_trash_dir(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / ".trash" / "old_note.md"
        assert obsidian_syncer._should_skip(path) is True

    def test_skip_hidden_dir(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / ".hidden" / "file.md"
        assert obsidian_syncer._should_skip(path) is True

    def test_skip_git_dir(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / ".git" / "HEAD"
        assert obsidian_syncer._should_skip(path) is True

    def test_no_skip_normal_dir(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / "People" / "note.md"
        assert obsidian_syncer._should_skip(path) is False

    def test_skip_path_outside_vault(self, obsidian_syncer):
        path = Path("/tmp/other/file.md")
        assert obsidian_syncer._should_skip(path) is True

    def test_no_skip_root_file(self, obsidian_syncer):
        path = obsidian_syncer.vault_path / "README.md"
        assert obsidian_syncer._should_skip(path) is False


class TestObsidianExtractLinks:
    """Tests for ObsidianSyncer._extract_links."""

    def test_simple_wikilinks(self, obsidian_syncer):
        text = "See [[Note A]] and [[Note B]]."
        links = obsidian_syncer._extract_links(text)
        assert links == ["Note A", "Note B"]

    def test_wikilink_with_alias(self, obsidian_syncer):
        text = "See [[Note A|display text]]."
        links = obsidian_syncer._extract_links(text)
        assert links == ["Note A"]

    def test_wikilink_with_heading(self, obsidian_syncer):
        text = "See [[Note A#Section]]."
        links = obsidian_syncer._extract_links(text)
        assert links == ["Note A"]

    def test_no_wikilinks(self, obsidian_syncer):
        text = "No links here."
        links = obsidian_syncer._extract_links(text)
        assert links == []

    def test_deduplicate_wikilinks(self, obsidian_syncer):
        text = "[[Note A]] and again [[Note A]]."
        links = obsidian_syncer._extract_links(text)
        assert links == ["Note A"]

    def test_empty_text(self, obsidian_syncer):
        links = obsidian_syncer._extract_links("")
        assert links == []


class TestObsidianExtractFrontmatterLinks:
    """Tests for ObsidianSyncer._extract_frontmatter_links."""

    def test_string_values(self, obsidian_syncer):
        fm = {"company": "Works at [[Acme Corp]]"}
        links = obsidian_syncer._extract_frontmatter_links(fm)
        assert links == ["Acme Corp"]

    def test_list_values(self, obsidian_syncer):
        fm = {"tags": ["#tag", "[[Person A]]"]}
        links = obsidian_syncer._extract_frontmatter_links(fm)
        assert links == ["Person A"]

    def test_none_frontmatter(self, obsidian_syncer):
        assert obsidian_syncer._extract_frontmatter_links(None) == []

    def test_non_dict_frontmatter(self, obsidian_syncer):
        assert obsidian_syncer._extract_frontmatter_links("string") == []

    def test_mixed_types(self, obsidian_syncer):
        fm = {"name": "[[Bob]]", "count": 42, "refs": ["[[Alice]]", 99]}
        links = obsidian_syncer._extract_frontmatter_links(fm)
        assert "Bob" in links
        assert "Alice" in links

    def test_empty_dict(self, obsidian_syncer):
        assert obsidian_syncer._extract_frontmatter_links({}) == []


class TestObsidianFileToRecord:
    """Tests for ObsidianSyncer._file_to_record."""

    def test_simple_markdown(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "test.md"
        md_file.write_text("# Hello\nSome content about [[People]].", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert record["id"] == "test.md"
        assert record["type"] == "document"
        assert record["title"] == "test"
        assert "People" in record["links"]
        assert "Hello" in record["content"]

    def test_with_frontmatter(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "note.md"
        md_file.write_text("---\ntitle: My Note\ntags: [test]\n---\nBody text.", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert record["title"] == "My Note"
        assert record["frontmatter"] == {"title": "My Note", "tags": ["test"]}
        assert record["content"] == "Body text."

    def test_truncate_large_content(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "huge.md"
        big_content = "x" * 60000
        md_file.write_text(big_content, encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert len(record["content"]) < 60000
        assert "[truncated]" in record["content"]

    def test_subfolder_record_id(self, obsidian_syncer):
        sub = obsidian_syncer.vault_path / "People"
        sub.mkdir()
        md_file = sub / "Alice.md"
        md_file.write_text("# Alice\nA person.", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record["id"] == "People/Alice.md"
        assert record["meta"]["folder"] == "People"

    def test_root_file_folder_is_none(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "root.md"
        md_file.write_text("# Root", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record["meta"]["folder"] is None

    def test_bad_frontmatter(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "bad_fm.md"
        md_file.write_text("---\n: invalid yaml [[\n---\nContent.", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert record["frontmatter"] is None

    def test_frontmatter_links_merged(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "linked.md"
        md_file.write_text(
            "---\ncompany: \"[[Acme]]\"\n---\nSee [[Bob]] and [[Acme]].",
            encoding="utf-8",
        )
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert "Bob" in record["links"]
        assert "Acme" in record["links"]
        assert record["links"].count("Acme") == 1

    def test_modified_at_field(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "ts.md"
        md_file.write_text("# Test", encoding="utf-8")
        mtime = datetime(2025, 6, 15, 10, 30, 0)

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record["modified_at"] == mtime.isoformat()

    def test_size_bytes_in_meta(self, obsidian_syncer):
        content = "Hello world"
        md_file = obsidian_syncer.vault_path / "size.md"
        md_file.write_text(content, encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record["meta"]["size_bytes"] == len(content)

    def test_no_content_after_frontmatter(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "empty_body.md"
        md_file.write_text("---\ntitle: Empty\n---\n", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record is not None
        assert record["title"] == "Empty"

    def test_path_field(self, obsidian_syncer):
        md_file = obsidian_syncer.vault_path / "pathtest.md"
        md_file.write_text("content", encoding="utf-8")
        mtime = datetime.now()

        record = obsidian_syncer._file_to_record(md_file, mtime)
        assert record["path"] == str(md_file)


class TestObsidianFetchNew:
    """Tests for ObsidianSyncer.fetch_new."""

    def test_vault_not_found(self, tmp_store, tmp_path):
        config = {
            "vault_path": str(tmp_path / "nonexistent"),
            "skip_dirs": [],
            "include_extensions": [".md"],
        }
        with patch.object(_obs_mod, "get_source_config", return_value=config):
            syncer = ObsidianSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_fetch_new_files(self, obsidian_syncer):
        md = obsidian_syncer.vault_path / "note1.md"
        md.write_text("# Note 1\nContent.", encoding="utf-8")

        state = SourceState()
        records = list(obsidian_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "note1"

    def test_fetch_respects_limit(self, obsidian_syncer):
        for i in range(5):
            (obsidian_syncer.vault_path / f"note{i}.md").write_text(f"# Note {i}", encoding="utf-8")

        state = SourceState()
        records = list(obsidian_syncer.fetch_new(state, limit=2))
        assert len(records) == 2

    def test_fetch_skips_old_files(self, obsidian_syncer):
        md = obsidian_syncer.vault_path / "old.md"
        md.write_text("# Old", encoding="utf-8")

        future = (datetime.now() + timedelta(hours=1)).isoformat()
        state = SourceState(last_ts=future)
        records = list(obsidian_syncer.fetch_new(state))
        assert records == []

    def test_fetch_skips_non_md_files(self, obsidian_syncer):
        (obsidian_syncer.vault_path / "image.png").write_bytes(b'\x89PNG')
        (obsidian_syncer.vault_path / "note.md").write_text("# MD", encoding="utf-8")

        state = SourceState()
        records = list(obsidian_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "note"

    def test_fetch_skips_hidden_dirs(self, obsidian_syncer):
        hidden = obsidian_syncer.vault_path / ".obsidian"
        hidden.mkdir()
        (hidden / "config.md").write_text("config", encoding="utf-8")
        (obsidian_syncer.vault_path / "visible.md").write_text("# Visible", encoding="utf-8")

        state = SourceState()
        records = list(obsidian_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "visible"

    def test_fetch_with_last_ts_includes_newer(self, obsidian_syncer):
        md = obsidian_syncer.vault_path / "new.md"
        md.write_text("# New", encoding="utf-8")

        past = (datetime.now() - timedelta(hours=1)).isoformat()
        state = SourceState(last_ts=past)
        records = list(obsidian_syncer.fetch_new(state))
        assert len(records) == 1

    def test_fetch_multiple_files_sorted_by_mtime(self, obsidian_syncer):
        import time
        f1 = obsidian_syncer.vault_path / "first.md"
        f1.write_text("# First", encoding="utf-8")
        time.sleep(0.05)
        f2 = obsidian_syncer.vault_path / "second.md"
        f2.write_text("# Second", encoding="utf-8")

        state = SourceState()
        records = list(obsidian_syncer.fetch_new(state))
        assert len(records) == 2
        assert records[0]["title"] == "first"
        assert records[1]["title"] == "second"


class TestObsidianSourceMetadata:
    """Tests for ObsidianSyncer class attributes."""

    def test_metadata(self, obsidian_syncer):
        assert obsidian_syncer.source_name == "obsidian"
        assert obsidian_syncer.display_name == "Obsidian"
        assert obsidian_syncer.category == "knowledge"

    def test_config_schema(self, obsidian_syncer):
        schema = obsidian_syncer.config_schema
        assert "vault_path" in schema
        assert "skip_dirs" in schema
        assert "include_extensions" in schema


# ============================================================
# XNews Syncer Tests
# ============================================================

class TestBirdCall:
    """Tests for _bird_call helper."""

    def test_bird_call_success(self):
        tweets = [{"id": "1", "text": "hello"}]

        with patch.object(_xn_mod.subprocess, "run") as mock_run, \
             patch.object(_xn_mod.tempfile, "TemporaryFile") as mock_tf:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""

            mock_file = MagicMock()
            raw_json = json.dumps(tweets).encode()
            mock_file.read.return_value = raw_json
            mock_tf.return_value.__enter__ = MagicMock(return_value=mock_file)
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            mock_run.return_value = mock_result

            result = _xn_mod._bird_call(["home", "-n", "10"])
            assert result == tweets

    def test_bird_call_error(self):
        with patch.object(_xn_mod.subprocess, "run") as mock_run, \
             patch.object(_xn_mod.tempfile, "TemporaryFile") as mock_tf:
            mock_result = MagicMock()
            mock_result.returncode = 1
            mock_result.stderr = b"some error occurred"

            mock_file = MagicMock()
            mock_tf.return_value.__enter__ = MagicMock(return_value=mock_file)
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            mock_run.return_value = mock_result

            with pytest.raises(RuntimeError, match="bird home"):
                _xn_mod._bird_call(["home"])

    def test_bird_call_empty_output(self):
        with patch.object(_xn_mod.subprocess, "run") as mock_run, \
             patch.object(_xn_mod.tempfile, "TemporaryFile") as mock_tf:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""

            mock_file = MagicMock()
            mock_file.read.return_value = b"   "
            mock_tf.return_value.__enter__ = MagicMock(return_value=mock_file)
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            mock_run.return_value = mock_result

            result = _xn_mod._bird_call(["home"])
            assert result == []

    def test_bird_call_non_list_json(self):
        with patch.object(_xn_mod.subprocess, "run") as mock_run, \
             patch.object(_xn_mod.tempfile, "TemporaryFile") as mock_tf:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""

            mock_file = MagicMock()
            mock_file.read.return_value = json.dumps({"key": "value"}).encode()
            mock_tf.return_value.__enter__ = MagicMock(return_value=mock_file)
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            mock_run.return_value = mock_result

            result = _xn_mod._bird_call(["home"])
            assert result == []

    def test_bird_call_filters_safari_eperm_errors(self):
        """Errors about Safari/EPERM should be filtered from stderr."""
        with patch.object(_xn_mod.subprocess, "run") as mock_run, \
             patch.object(_xn_mod.tempfile, "TemporaryFile") as mock_tf:
            mock_result = MagicMock()
            mock_result.returncode = 1
            mock_result.stderr = b"Safari warning\nEPERM denied\n"

            mock_file = MagicMock()
            mock_file.read.return_value = b""
            mock_tf.return_value.__enter__ = MagicMock(return_value=mock_file)
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            mock_run.return_value = mock_result

            result = _xn_mod._bird_call(["home"])
            assert result == []


class TestXNewsFetchNew:
    """Tests for XNewsSyncer.fetch_new."""

    def test_fetch_tweets(self, xnews_syncer):
        tweets = [
            {
                "id": "123",
                "text": "Hello world",
                "createdAt": "2025-01-01T00:00:00Z",
                "author": {"username": "user1", "name": "User One"},
                "likeCount": 10,
                "retweetCount": 5,
                "replyCount": 2,
                "media": [],
                "conversationId": "123",
            },
            {
                "id": "456",
                "text": "Reply tweet",
                "createdAt": "2025-01-01T01:00:00Z",
                "author": {"username": "user2", "name": "User Two"},
                "likeCount": 0,
                "retweetCount": 0,
                "replyCount": 0,
                "media": [{"type": "photo"}],
                "conversationId": "123",
            },
        ]

        with patch.object(_xn_mod, "_bird_call", return_value=tweets):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))

        assert len(records) == 2

        r0 = records[0]
        assert r0["id"] == "xtweet_123"
        assert r0["type"] == "tweet"
        assert r0["text"] == "Hello world"
        assert r0["author"] == "user1"
        assert r0["likes"] == 10
        assert r0["is_reply"] is False
        assert r0["has_media"] is False

        r1 = records[1]
        assert r1["id"] == "xtweet_456"
        assert r1["is_reply"] is True
        assert r1["has_media"] is True
        assert r1["media_types"] == ["photo"]

    def test_fetch_empty_timeline(self, xnews_syncer):
        with patch.object(_xn_mod, "_bird_call", return_value=[]):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))
        assert records == []

    def test_fetch_bird_call_failure(self, xnews_syncer):
        with patch.object(_xn_mod, "_bird_call", side_effect=RuntimeError("bird failed")):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))
        assert records == []

    def test_fetch_skips_tweets_without_id(self, xnews_syncer):
        tweets = [
            {"text": "no id tweet", "author": {}, "media": [], "conversationId": ""},
            {"id": "789", "text": "has id", "author": {"username": "u"}, "media": [], "conversationId": "789"},
        ]

        with patch.object(_xn_mod, "_bird_call", return_value=tweets):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["tweet_id"] == "789"

    def test_fetch_default_values(self, xnews_syncer):
        """Tweet with missing optional fields gets defaults."""
        tweets = [{"id": "100"}]
        with patch.object(_xn_mod, "_bird_call", return_value=tweets):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))
        assert len(records) == 1
        r = records[0]
        assert r["text"] == ""
        assert r["author"] == ""
        assert r["author_name"] == ""
        assert r["likes"] == 0
        assert r["retweets"] == 0
        assert r["replies"] == 0
        assert r["has_media"] is False
        assert r["media_types"] == []

    def test_fetch_uses_config_count(self, xnews_syncer):
        """Verifies count param is read from config."""
        with patch.object(_xn_mod, "_bird_call", return_value=[]) as mock_call:
            state = SourceState()
            list(xnews_syncer.fetch_new(state))
            mock_call.assert_called_once_with(["home", "-n", "10"])

    def test_xnews_source_metadata(self, xnews_syncer):
        assert xnews_syncer.source_name == "xnews"
        assert xnews_syncer.display_name == "X/Twitter Feed"
        assert xnews_syncer.category == "social"

    def test_tweet_timestamp_fallback(self, xnews_syncer):
        """Tweet without createdAt gets current timestamp."""
        tweets = [{"id": "200", "author": {}, "media": []}]
        with patch.object(_xn_mod, "_bird_call", return_value=tweets):
            state = SourceState()
            records = list(xnews_syncer.fetch_new(state))
        assert len(records) == 1
        ts = records[0]["ts"]
        datetime.fromisoformat(ts.replace("Z", "+00:00"))


# ============================================================
# Import Gmail, Calendar, Claude, Granola, Nextcloud syncers
# ============================================================

import vadimgest.ingest.sources.gmail.syncer as _gmail_mod
import vadimgest.ingest.sources.calendar.syncer as _cal_mod
import vadimgest.ingest.sources.claude.syncer as _claude_mod
import vadimgest.ingest.sources.granola.syncer as _granola_mod
import vadimgest.ingest.sources.nextcloud.syncer as _nc_mod

GmailSyncer = _gmail_mod.GmailSyncer
CalendarSyncer = _cal_mod.CalendarSyncer
ClaudeSyncer = _claude_mod.ClaudeSyncer
GranolaSyncer = _granola_mod.GranolaSyncer
NextcloudSyncer = _nc_mod.NextcloudSyncer


# ============================================================
# Fixtures for new syncers
# ============================================================

@pytest.fixture
def gmail_syncer(tmp_store):
    """Gmail syncer with test config."""
    config = {
        "accounts": ["test@gmail.com", "work@gmail.com"],
        "query": "newer_than:1d",
        "bootstrap_query": "newer_than:7d",
        "sent_query": "in:sent newer_than:7d",
        "sent_bootstrap_query": "in:sent newer_than:14d",
        "follow_up_hours": 48,
        "page_size": 25,
        "batch_size": 25,
    }
    with patch.object(_gmail_mod, "get_source_config", return_value=config):
        return GmailSyncer(tmp_store, config)


@pytest.fixture
def calendar_syncer(tmp_store):
    """Calendar syncer with test config."""
    config = {
        "accounts": ["test@gmail.com"],
        "days_back": 7,
        "days_forward": 14,
        "calendar_ids": [],
    }
    with patch.object(_cal_mod, "get_source_config", return_value=config):
        return CalendarSyncer(tmp_store, config)


@pytest.fixture
def claude_syncer(tmp_store, tmp_path):
    """Claude syncer with a fake projects directory."""
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    config = {
        "projects_dir": str(projects_dir),
    }
    with patch.object(_claude_mod, "get_source_config", return_value=config):
        return ClaudeSyncer(tmp_store, config)


@pytest.fixture
def granola_syncer(tmp_store, tmp_path):
    """Granola syncer with a fake cache path."""
    cache_file = tmp_path / "cache-v3.json"
    config = {
        "cache_path": str(cache_file),
    }
    with patch.object(_granola_mod, "get_source_config", return_value=config):
        return GranolaSyncer(tmp_store, config)


@pytest.fixture
def nextcloud_syncer(tmp_store):
    """Nextcloud syncer with test config."""
    config = {
        "server": "https://drive.test.com",
        "username": "testuser",
        "token": "testtoken",
        "max_results": 200,
        "content_preview": True,
        "skip_dirs": [".Trash", ".versions"],
    }
    with patch.object(_nc_mod, "get_source_config", return_value=config):
        return NextcloudSyncer(tmp_store, config)


# ============================================================
# Gmail Syncer Tests
# ============================================================

class TestGmailSourceMetadata:
    """Tests for GmailSyncer class attributes."""

    def test_metadata(self, gmail_syncer):
        assert gmail_syncer.source_name == "gmail"
        assert gmail_syncer.display_name == "Gmail"
        assert gmail_syncer.category == "email"

    def test_config_values(self, gmail_syncer):
        assert gmail_syncer.accounts == ["test@gmail.com", "work@gmail.com"]
        assert gmail_syncer.query == "newer_than:1d"
        assert gmail_syncer.follow_up_hours == 48
        assert gmail_syncer.page_size == 25


class TestGmailSearchThreads:
    """Tests for GmailSyncer._search_threads."""

    def test_search_returns_threads_from_dict(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", return_value={"threads": [{"id": "t1"}]}):
            result = gmail_syncer._search_threads("test@gmail.com", "query")
        assert result == [{"id": "t1"}]

    def test_search_returns_list_directly(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", return_value=[{"id": "t1"}]):
            result = gmail_syncer._search_threads("test@gmail.com", "query")
        assert result == [{"id": "t1"}]

    def test_search_returns_empty_on_exception(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", side_effect=RuntimeError("fail")):
            result = gmail_syncer._search_threads("test@gmail.com", "query")
        assert result == []

    def test_search_returns_empty_on_non_dict_non_list(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", return_value="unexpected"):
            result = gmail_syncer._search_threads("test@gmail.com", "query")
        assert result == []


class TestGmailGetMessage:
    """Tests for GmailSyncer._get_message."""

    def test_get_message_returns_dict(self, gmail_syncer):
        msg_data = {"body": "Hello", "headers": {}}
        with patch.object(_gmail_mod, "gog_call", return_value=msg_data):
            result = gmail_syncer._get_message("test@gmail.com", "msg123")
        assert result == msg_data

    def test_get_message_returns_empty_on_error(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", side_effect=RuntimeError("fail")):
            result = gmail_syncer._get_message("test@gmail.com", "msg123")
        assert result == {}

    def test_get_message_returns_empty_on_non_dict(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", return_value="string"):
            result = gmail_syncer._get_message("test@gmail.com", "msg123")
        assert result == {}


class TestGmailGetThreadMessages:
    """Tests for GmailSyncer._get_thread_messages."""

    def test_empty_thread_id(self, gmail_syncer):
        assert gmail_syncer._get_thread_messages("test@gmail.com", "") == []

    def test_exception_returns_empty(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", side_effect=RuntimeError("fail")):
            result = gmail_syncer._get_thread_messages("test@gmail.com", "t1")
        assert result == []

    def test_non_dict_returns_empty(self, gmail_syncer):
        with patch.object(_gmail_mod, "gog_call", return_value="string"):
            result = gmail_syncer._get_thread_messages("test@gmail.com", "t1")
        assert result == []

    def test_normalizes_messages(self, gmail_syncer):
        raw_response = {
            "thread": {
                "messages": [
                    {
                        "id": "msg1",
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "alice@test.com"},
                                {"name": "To", "value": "bob@test.com"},
                                {"name": "Subject", "value": "Hello"},
                                {"name": "Date", "value": "2025-01-01"},
                            ]
                        },
                        "labelIds": ["INBOX", "UNREAD"],
                    }
                ]
            }
        }
        with patch.object(_gmail_mod, "gog_call", return_value=raw_response):
            result = gmail_syncer._get_thread_messages("test@gmail.com", "t1")
        assert len(result) == 1
        assert result[0]["id"] == "msg1"
        assert result[0]["from"] == "alice@test.com"
        assert result[0]["to"] == "bob@test.com"
        assert result[0]["subject"] == "Hello"
        assert result[0]["labels"] == ["INBOX", "UNREAD"]

    def test_preserves_raw_headers_body_and_attachment_metadata(self, gmail_syncer):
        encoded = base64.urlsafe_b64encode(b"Full untruncated email body").decode().rstrip("=")
        raw_message = {
            "id": "msg-full",
            "threadId": "thread-1",
            "internalDate": "1780000000000",
            "sizeEstimate": 12345,
            "snippet": "Full email",
            "labelIds": ["INBOX"],
            "payload": {
                "mimeType": "multipart/mixed",
                "headers": [
                    {"name": "From", "value": "alice@test.com"},
                    {"name": "To", "value": "test@gmail.com"},
                    {"name": "Subject", "value": "Lossless"},
                    {"name": "Date", "value": "2026-07-10"},
                    {"name": "Message-ID", "value": "<lossless@example.com>"},
                    {"name": "Received", "value": "first-hop"},
                    {"name": "Received", "value": "second-hop"},
                ],
                "parts": [
                    {
                        "mimeType": "text/plain",
                        "body": {"data": encoded, "size": 27},
                    },
                    {
                        "mimeType": "application/pdf",
                        "filename": "contract.pdf",
                        "body": {"attachmentId": "att-1", "size": 99},
                    },
                ],
            },
        }
        with patch.object(
            _gmail_mod,
            "gog_call",
            return_value={"thread": {"messages": [raw_message]}},
        ):
            result = gmail_syncer._get_thread_messages("test@gmail.com", "thread-1")

        assert result[0]["body"] == "Full untruncated email body"
        assert result[0]["rfc822_message_id"] == "<lossless@example.com>"
        assert result[0]["headers"]["received"] == ["first-hop", "second-hop"]
        assert result[0]["attachments"] == [
            {
                "filename": "contract.pdf",
                "mime_type": "application/pdf",
                "attachment_id": "att-1",
                "size": 99,
            }
        ]
        assert result[0]["raw_message"] == raw_message

    def test_thread_without_nested_thread_key(self, gmail_syncer):
        """Response has messages at top level (no 'thread' wrapper)."""
        raw_response = {
            "messages": [
                {
                    "id": "msg2",
                    "payload": {"headers": []},
                    "labelIds": [],
                }
            ]
        }
        with patch.object(_gmail_mod, "gog_call", return_value=raw_response):
            result = gmail_syncer._get_thread_messages("test@gmail.com", "t1")
        assert len(result) == 1
        assert result[0]["id"] == "msg2"


class TestGmailMsgToRecord:
    """Tests for GmailSyncer._msg_to_record."""

    def test_basic_record(self, gmail_syncer):
        msg = {
            "message_id": "abc123",
            "subject": "Test Subject",
            "from": "alice@test.com",
            "to": "test@gmail.com",
            "date": "2025-01-01",
            "body": "Hello world",
            "labels": ["INBOX", "UNREAD"],
            "thread_id": "t1",
        }
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record is not None
        assert record["id"] == "gmail_test_abc123"
        assert record["type"] == "email"
        assert record["subject"] == "Test Subject"
        assert record["is_unread"] is True
        assert record["direction"] == "received"
        assert "awaiting_reply" not in record

    def test_no_message_id_returns_none(self, gmail_syncer):
        msg = {"subject": "No ID"}
        result = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert result is None

    def test_uses_id_field_as_fallback(self, gmail_syncer):
        msg = {"id": "fallback_id", "subject": "Test"}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record is not None
        assert "fallback_id" in record["id"]

    def test_body_is_not_truncated(self, gmail_syncer):
        msg = {"message_id": "x", "body": "A" * 6000}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record["body"] == "A" * 6000

    def test_record_preserves_rfc822_id_headers_and_raw_message(self, gmail_syncer):
        msg = {
            "message_id": "gmail-api-id",
            "rfc822_message_id": "<rfc822@example.com>",
            "headers": {"message-id": ["<rfc822@example.com>"], "received": ["hop"]},
            "attachments": [{"filename": "a.pdf", "attachment_id": "a1"}],
            "raw_message": {"id": "gmail-api-id", "payload": {"headers": []}},
        }
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")

        assert record["rfc822_message_id"] == "<rfc822@example.com>"
        assert record["headers"]["received"] == ["hop"]
        assert record["attachments"][0]["attachment_id"] == "a1"
        assert record["raw_message"]["id"] == "gmail-api-id"
        assert record["meta"]["rfc822_message_id"] == "<rfc822@example.com>"

    def test_sent_direction_with_awaiting_reply(self, gmail_syncer):
        msg = {"message_id": "s1", "subject": "Sent"}
        record = gmail_syncer._msg_to_record(
            msg, "test@gmail.com", direction="sent", awaiting_reply=True
        )
        assert record["direction"] == "sent"
        assert record["awaiting_reply"] is True

    def test_sent_direction_no_awaiting_reply(self, gmail_syncer):
        msg = {"message_id": "s2", "subject": "Sent"}
        record = gmail_syncer._msg_to_record(
            msg, "test@gmail.com", direction="sent", awaiting_reply=False
        )
        assert record["awaiting_reply"] is False

    def test_defaults_for_missing_fields(self, gmail_syncer):
        msg = {"message_id": "d1"}
        record = gmail_syncer._msg_to_record(msg, "test@gmail.com")
        assert record["subject"] == "(no subject)"
        assert record["body"] == ""
        assert record["is_unread"] is False


class TestGmailIsAccountAddress:
    """Tests for GmailSyncer._is_account_address."""

    def test_exact_match(self, gmail_syncer):
        assert gmail_syncer._is_account_address("test@gmail.com", "test@gmail.com") is True

    def test_bracket_format(self, gmail_syncer):
        assert gmail_syncer._is_account_address("Alice <test@gmail.com>", "test@gmail.com") is True

    def test_case_insensitive(self, gmail_syncer):
        assert gmail_syncer._is_account_address("Test@Gmail.com", "test@gmail.com") is True

    def test_different_configured_account(self, gmail_syncer):
        assert gmail_syncer._is_account_address("work@gmail.com", "test@gmail.com") is True

    def test_unknown_address(self, gmail_syncer):
        assert gmail_syncer._is_account_address("stranger@other.com", "test@gmail.com") is False

    def test_empty_address(self, gmail_syncer):
        assert gmail_syncer._is_account_address("", "test@gmail.com") is False


class TestGmailCheckAwaitingReply:
    """Tests for GmailSyncer._check_awaiting_reply."""

    def test_no_thread_id_returns_true(self, gmail_syncer):
        result = gmail_syncer._check_awaiting_reply("test@gmail.com", {"thread_id": ""})
        assert result is True

    def test_thread_fetch_fails_returns_true(self, gmail_syncer):
        with patch.object(gmail_syncer, "_get_thread_messages", return_value=[]):
            result = gmail_syncer._check_awaiting_reply("test@gmail.com", {"thread_id": "t1"})
        assert result is True

    def test_single_message_returns_true(self, gmail_syncer):
        with patch.object(gmail_syncer, "_get_thread_messages", return_value=[{"from": "test@gmail.com"}]):
            result = gmail_syncer._check_awaiting_reply("test@gmail.com", {"thread_id": "t1"})
        assert result is True

    def test_last_message_from_us(self, gmail_syncer):
        msgs = [
            {"from": "other@test.com"},
            {"from": "test@gmail.com"},
        ]
        with patch.object(gmail_syncer, "_get_thread_messages", return_value=msgs):
            result = gmail_syncer._check_awaiting_reply("test@gmail.com", {"thread_id": "t1"})
        assert result is True

    def test_last_message_from_other(self, gmail_syncer):
        msgs = [
            {"from": "test@gmail.com"},
            {"from": "other@test.com"},
        ]
        with patch.object(gmail_syncer, "_get_thread_messages", return_value=msgs):
            result = gmail_syncer._check_awaiting_reply("test@gmail.com", {"thread_id": "t1"})
        assert result is False


class TestGmailParseEmailDate:
    """Tests for GmailSyncer._parse_email_date."""

    def test_rfc2822(self):
        result = GmailSyncer._parse_email_date("Wed, 19 Feb 2025 14:30:00 +0000")
        assert result is not None
        assert result.year == 2025
        assert result.month == 2

    def test_iso8601(self):
        result = GmailSyncer._parse_email_date("2025-02-19T14:30:00Z")
        assert result is not None
        assert result.year == 2025

    def test_simple_datetime(self):
        result = GmailSyncer._parse_email_date("2025-02-19 14:30:00")
        assert result is not None
        assert result.tzinfo is not None  # Should assume UTC

    def test_date_with_parenthetical_tz(self):
        result = GmailSyncer._parse_email_date("Wed, 19 Feb 2025 14:30:00 +0000 (UTC)")
        assert result is not None

    def test_empty_string(self):
        assert GmailSyncer._parse_email_date("") is None

    def test_none_input(self):
        assert GmailSyncer._parse_email_date(None) is None

    def test_unparseable(self):
        assert GmailSyncer._parse_email_date("not a date") is None

    def test_rfc2822_without_day_name(self):
        result = GmailSyncer._parse_email_date("19 Feb 2025 14:30:00 +0000")
        assert result is not None

    def test_american_format(self):
        result = GmailSyncer._parse_email_date("Feb 19, 2025 2:30 PM")
        assert result is not None


class TestGmailFetchNew:
    """Tests for GmailSyncer.fetch_new."""

    def test_no_accounts(self, tmp_store):
        config = {"accounts": []}
        with patch.object(_gmail_mod, "get_source_config", return_value=config):
            syncer = GmailSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_uses_bootstrap_query_on_first_sync(self, gmail_syncer):
        state = SourceState()  # no last_ts
        with patch.object(gmail_syncer, "_search_threads", return_value=[]) as mock_search:
            list(gmail_syncer.fetch_new(state))
        # Should be called with bootstrap_query for all accounts
        mock_search.assert_any_call("test@gmail.com", "newer_than:7d")

    def test_uses_regular_query_on_subsequent_sync(self, gmail_syncer):
        state = SourceState(last_ts="2025-01-01T00:00:00Z")
        with patch.object(gmail_syncer, "_search_threads", return_value=[]) as mock_search:
            list(gmail_syncer.fetch_new(state))
        mock_search.assert_any_call("test@gmail.com", "newer_than:1d")

    def test_yields_records_from_threads(self, gmail_syncer):
        threads = [
            {"id": "t1", "from": "alice@test.com", "subject": "Hello", "date": "2025-01-01", "labels": [], "messageCount": 1},
        ]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads):
            records = list(gmail_syncer.fetch_new(state))
        # 2 accounts, each returns the same threads => 2 records (different account prefix in ID)
        assert len(records) == 2
        assert all(r["type"] == "email" for r in records)
        assert all(r["direction"] == "received" for r in records)

    def test_fetch_new_prefers_full_thread_messages_over_sparse_search_metadata(self, gmail_syncer):
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [
            {"id": "thread-1", "from": "alice@test.com", "subject": "Sparse", "messageCount": 2},
        ]
        full_messages = [
            {
                "id": "message-1",
                "thread_id": "thread-1",
                "from": "alice@test.com",
                "to": "test@gmail.com",
                "subject": "Full",
                "date": "2026-07-10",
                "body": "Complete body",
                "headers": {"message-id": ["<full@example.com>"]},
                "rfc822_message_id": "<full@example.com>",
                "raw_message": {"id": "message-1"},
                "labels": ["INBOX"],
            }
        ]
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), patch.object(
            gmail_syncer, "_get_thread_messages", return_value=full_messages
        ):
            records = list(gmail_syncer.fetch_new(SourceState()))

        assert len(records) == 1
        assert records[0]["meta"]["message_id"] == "message-1"
        assert records[0]["rfc822_message_id"] == "<full@example.com>"
        assert records[0]["body"] == "Complete body"

    def test_respects_limit(self, gmail_syncer):
        threads = [
            {"id": f"t{i}", "from": "a@b.com", "subject": f"S{i}", "date": "2025-01-01", "labels": [], "messageCount": 1}
            for i in range(10)
        ]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads):
            records = list(gmail_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_no_threads_found(self, gmail_syncer):
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=[]):
            records = list(gmail_syncer.fetch_new(state))
        assert records == []


class TestGmailFetchSent:
    """Tests for GmailSyncer.fetch_sent."""

    def test_no_accounts(self, tmp_store):
        config = {"accounts": []}
        with patch.object(_gmail_mod, "get_source_config", return_value=config):
            syncer = GmailSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_sent(state))
        assert records == []

    def test_fetch_sent_with_thread_messages(self, gmail_syncer):
        # Use single account to simplify assertions
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": "t1", "from": "test@gmail.com", "subject": "Sent", "date": "2025-01-01", "labels": []}]
        thread_msgs = [
            {"id": "m1", "from": "test@gmail.com", "to": "other@test.com", "subject": "Sent", "date": "2025-01-01", "labels": []},
        ]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "_get_thread_messages", return_value=thread_msgs):
            records = list(gmail_syncer.fetch_sent(state))
        assert len(records) == 1
        assert records[0]["direction"] == "sent"
        assert records[0]["awaiting_reply"] is True

    def test_fetch_sent_fallback_when_thread_empty(self, gmail_syncer):
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": "t1", "from": "me", "subject": "X", "date": "2025-01-01", "labels": []}]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "_get_thread_messages", return_value=[]):
            records = list(gmail_syncer.fetch_sent(state))
        assert len(records) == 1
        assert records[0]["awaiting_reply"] is True

    def test_fetch_sent_skips_empty_thread_id(self, gmail_syncer):
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": "", "from": "me", "subject": "X"}]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads):
            records = list(gmail_syncer.fetch_sent(state))
        assert records == []

    def test_fetch_sent_respects_limit(self, gmail_syncer):
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": f"t{i}", "from": "me", "subject": f"S{i}", "date": "2025-01-01", "labels": []} for i in range(5)]
        thread_msgs = [
            {"id": "m1", "from": "test@gmail.com", "subject": "S", "date": "2025-01-01", "labels": []},
        ]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "_get_thread_messages", return_value=thread_msgs):
            records = list(gmail_syncer.fetch_sent(state, limit=2))
        assert len(records) == 2

    def test_fetch_sent_filters_non_account_messages(self, gmail_syncer):
        """Only sent messages from our account should be yielded."""
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": "t1", "from": "test@gmail.com", "subject": "S", "date": "2025-01-01", "labels": []}]
        thread_msgs = [
            {"id": "m1", "from": "other@test.com", "subject": "Reply", "date": "2025-01-01", "labels": []},
            {"id": "m2", "from": "test@gmail.com", "subject": "Sent", "date": "2025-01-02", "labels": []},
        ]
        state = SourceState()
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "_get_thread_messages", return_value=thread_msgs):
            records = list(gmail_syncer.fetch_sent(state))
        # Only our messages should be yielded
        assert len(records) == 1
        assert "m2" in records[0]["meta"]["message_id"]


class TestGmailSync:
    """Tests for GmailSyncer.sync."""

    def test_sync_deduplicates(self, gmail_syncer):
        gmail_syncer.accounts = ["test@gmail.com"]
        threads = [{"id": "t1", "from": "a@b.com", "subject": "S", "date": "2025-01-01", "labels": [], "messageCount": 1}]
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "fetch_sent", return_value=iter([])):
            count1, _ = gmail_syncer.sync()
        assert count1 == 1
        # Sync again - should deduplicate
        with patch.object(gmail_syncer, "_search_threads", return_value=threads), \
             patch.object(gmail_syncer, "fetch_sent", return_value=iter([])):
            count2, _ = gmail_syncer.sync()
        assert count2 == 0


class TestGmailGetFollowUps:
    """Tests for GmailSyncer.get_follow_ups."""

    def test_returns_old_awaiting_emails(self, gmail_syncer):
        # Manually append a sent email to the store
        old_date = "Wed, 10 Jan 2025 10:00:00 +0000"
        gmail_syncer.store.append("gmail", {
            "id": "test_sent_1",
            "direction": "sent",
            "awaiting_reply": True,
            "date": old_date,
            "subject": "Old email",
        })
        follow_ups = gmail_syncer.get_follow_ups(hours=1)
        assert len(follow_ups) == 1
        assert follow_ups[0]["subject"] == "Old email"
        assert "_age_hours" in follow_ups[0]

    def test_ignores_received_emails(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "r1",
            "direction": "received",
            "awaiting_reply": False,
            "date": "Wed, 10 Jan 2025 10:00:00 +0000",
        })
        follow_ups = gmail_syncer.get_follow_ups(hours=1)
        assert follow_ups == []

    def test_ignores_recent_emails(self, gmail_syncer):
        from datetime import timezone as tz
        recent = datetime.now(tz.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
        gmail_syncer.store.append("gmail", {
            "id": "recent1",
            "direction": "sent",
            "awaiting_reply": True,
            "date": recent,
        })
        follow_ups = gmail_syncer.get_follow_ups(hours=100)
        assert follow_ups == []

    def test_ignores_not_awaiting(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "na1",
            "direction": "sent",
            "awaiting_reply": False,
            "date": "Wed, 10 Jan 2025 10:00:00 +0000",
        })
        follow_ups = gmail_syncer.get_follow_ups()
        assert follow_ups == []

    def test_ignores_empty_date(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "nd1",
            "direction": "sent",
            "awaiting_reply": True,
            "date": "",
        })
        follow_ups = gmail_syncer.get_follow_ups()
        assert follow_ups == []


class TestGmailRefreshFollowUpStatus:
    """Tests for GmailSyncer.refresh_follow_up_status."""

    def test_reply_received_creates_update_record(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "sent_1",
            "direction": "sent",
            "awaiting_reply": True,
            "account": "test@gmail.com",
            "thread_id": "t1",
            "subject": "Test",
            "meta": {"message_id": "m1"},
        })
        with patch.object(gmail_syncer, "_check_awaiting_reply", return_value=False):
            checked, changed = gmail_syncer.refresh_follow_up_status()
        assert checked == 1
        assert changed == 1

    def test_still_awaiting_no_update(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "sent_2",
            "direction": "sent",
            "awaiting_reply": True,
            "account": "test@gmail.com",
            "thread_id": "t2",
            "subject": "Test",
            "meta": {"message_id": "m2"},
        })
        with patch.object(gmail_syncer, "_check_awaiting_reply", return_value=True):
            checked, changed = gmail_syncer.refresh_follow_up_status()
        assert checked == 1
        assert changed == 0

    def test_skips_missing_account_or_thread(self, gmail_syncer):
        gmail_syncer.store.append("gmail", {
            "id": "sent_3",
            "direction": "sent",
            "awaiting_reply": True,
            "account": "",
            "thread_id": "",
        })
        checked, changed = gmail_syncer.refresh_follow_up_status()
        assert checked == 0
        assert changed == 0


# ============================================================
# Calendar Syncer Tests
# ============================================================

class TestCalendarSourceMetadata:
    """Tests for CalendarSyncer class attributes."""

    def test_metadata(self, calendar_syncer):
        assert calendar_syncer.source_name == "calendar"
        assert calendar_syncer.display_name == "Google Calendar"
        assert calendar_syncer.category == "calendar"

    def test_config_values(self, calendar_syncer):
        assert calendar_syncer.accounts == ["test@gmail.com"]
        assert calendar_syncer.days_back == 7
        assert calendar_syncer.days_forward == 14


class TestCalendarListCalendars:
    """Tests for CalendarSyncer._list_calendars."""

    def test_returns_calendars(self, calendar_syncer):
        with patch.object(_cal_mod, "gog_call", return_value={"calendars": [{"id": "cal1"}]}):
            result = calendar_syncer._list_calendars("test@gmail.com")
        assert result == [{"id": "cal1"}]

    def test_returns_empty_on_error(self, calendar_syncer):
        with patch.object(_cal_mod, "gog_call", side_effect=RuntimeError("fail")):
            result = calendar_syncer._list_calendars("test@gmail.com")
        assert result == []


class TestCalendarGetEvents:
    """Tests for CalendarSyncer._get_events."""

    def test_returns_events(self, calendar_syncer):
        with patch.object(_cal_mod, "gog_call", return_value={"events": [{"id": "e1"}]}):
            result = calendar_syncer._get_events("cal1", "min", "max", "test@gmail.com")
        assert result == [{"id": "e1"}]

    def test_returns_empty_on_error(self, calendar_syncer):
        with patch.object(_cal_mod, "gog_call", side_effect=RuntimeError("fail")):
            result = calendar_syncer._get_events("cal1", "min", "max", "test@gmail.com")
        assert result == []


class TestCalendarParseEventDatetime:
    """Tests for CalendarSyncer._parse_event_datetime."""

    def test_structured_datetime(self, calendar_syncer):
        event = {"start": {"dateTime": "2025-01-01T10:00:00Z"}}
        result = calendar_syncer._parse_event_datetime(event, "start")
        assert result == "2025-01-01T10:00:00Z"

    def test_structured_date_only(self, calendar_syncer):
        event = {"start": {"date": "2025-01-01"}}
        result = calendar_syncer._parse_event_datetime(event, "start")
        assert result == "2025-01-01"

    def test_flat_string(self, calendar_syncer):
        event = {"start": "2025-01-01T10:00:00Z"}
        result = calendar_syncer._parse_event_datetime(event, "start")
        assert result == "2025-01-01T10:00:00Z"

    def test_missing_field(self, calendar_syncer):
        event = {}
        result = calendar_syncer._parse_event_datetime(event, "start")
        assert result == ""


class TestCalendarEventToRecord:
    """Tests for CalendarSyncer._event_to_record."""

    def test_basic_event(self, calendar_syncer):
        event = {
            "id": "evt1",
            "summary": "Team Meeting",
            "start": "2025-01-01T10:00:00Z",
            "end": "2025-01-01T11:00:00Z",
            "location": "Room A",
            "description": "Weekly sync",
            "status": "confirmed",
            "htmlLink": "https://cal.google.com/evt1",
            "organizer": "boss@test.com",
            "attendees": ["alice@test.com", "bob@test.com"],
        }
        record = calendar_syncer._event_to_record(event, "cal1@gmail.com", "Work")
        assert record is not None
        assert record["type"] == "calendar_event"
        assert record["title"] == "Team Meeting"
        assert record["start"] == "2025-01-01T10:00:00Z"
        assert record["location"] == "Room A"
        assert record["attendees"] == ["alice@test.com", "bob@test.com"]
        assert record["calendar_name"] == "Work"
        assert record["organizer"] == "boss@test.com"

    def test_no_event_id_generates_from_title(self, calendar_syncer):
        event = {"summary": "Meeting", "start": "2025-01-01"}
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert record is not None
        assert "Meeting" in record["meta"]["event_id"]

    def test_no_id_no_title_returns_none(self, calendar_syncer):
        event = {}
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert record is None

    def test_attendees_as_dicts(self, calendar_syncer):
        event = {
            "id": "e1",
            "summary": "Test",
            "start": "2025-01-01",
            "end": "2025-01-01",
            "attendees": [
                {"email": "a@test.com", "displayName": "Alice"},
                {"displayName": "Bob"},
            ],
        }
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert "a@test.com" in record["attendees"]
        assert "Bob" in record["attendees"]

    def test_attendees_as_string(self, calendar_syncer):
        event = {
            "id": "e2",
            "summary": "Test",
            "start": "2025-01-01",
            "end": "2025-01-01",
            "attendees": "alice@test.com, bob@test.com",
        }
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert len(record["attendees"]) == 2

    def test_organizer_as_dict(self, calendar_syncer):
        event = {
            "id": "e3",
            "summary": "Test",
            "start": "2025-01-01",
            "end": "2025-01-01",
            "organizer": {"email": "org@test.com"},
        }
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert record["organizer"] == "org@test.com"

    def test_description_truncation(self, calendar_syncer):
        event = {
            "id": "e4",
            "summary": "Test",
            "start": "2025-01-01",
            "description": "X" * 5000,
        }
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert len(record["description"]) < 5000
        assert "[truncated]" in record["description"]

    def test_no_title_defaults(self, calendar_syncer):
        event = {"id": "e5", "start": "2025-01-01"}
        record = calendar_syncer._event_to_record(event, "cal1", "Cal")
        assert record["title"] == "(no title)"


class TestCalendarFetchNew:
    """Tests for CalendarSyncer.fetch_new."""

    def test_no_accounts(self, tmp_store):
        config = {"accounts": []}
        with patch.object(_cal_mod, "get_source_config", return_value=config):
            syncer = CalendarSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_full_flow(self, calendar_syncer):
        calendars = [{"id": "cal1", "summary": "Work"}]
        events = [
            {"id": "e1", "summary": "Meeting", "start": "2025-01-01T10:00:00Z", "end": "2025-01-01T11:00:00Z"},
        ]
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars), \
             patch.object(calendar_syncer, "_get_events", return_value=events):
            records = list(calendar_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "Meeting"

    def test_deduplicates_across_accounts(self, calendar_syncer):
        calendar_syncer.accounts = ["a1@gmail.com", "a2@gmail.com"]
        calendars = [{"id": "cal1", "summary": "Shared"}]
        events = [
            {"id": "e_same", "summary": "Shared Meeting", "start": "2025-01-01", "end": "2025-01-01"},
        ]
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars), \
             patch.object(calendar_syncer, "_get_events", return_value=events):
            records = list(calendar_syncer.fetch_new(state))
        assert len(records) == 1  # deduped

    def test_no_calendars_skips_account(self, calendar_syncer):
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=[]):
            records = list(calendar_syncer.fetch_new(state))
        assert records == []

    def test_filters_calendars_by_configured_ids(self, calendar_syncer):
        calendar_syncer.calendar_ids = ["cal1"]
        calendars = [
            {"id": "cal1", "summary": "Work"},
            {"id": "cal2", "summary": "Personal"},
        ]
        events = [{"id": "e1", "summary": "Meeting", "start": "2025-01-01", "end": "2025-01-01"}]
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars), \
             patch.object(calendar_syncer, "_get_events", return_value=events):
            records = list(calendar_syncer.fetch_new(state))
        # Only cal1 events should be fetched
        assert len(records) == 1

    def test_respects_limit(self, calendar_syncer):
        calendars = [{"id": "cal1", "summary": "Work"}]
        events = [
            {"id": f"e{i}", "summary": f"M{i}", "start": "2025-01-01", "end": "2025-01-01"}
            for i in range(10)
        ]
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars), \
             patch.object(calendar_syncer, "_get_events", return_value=events):
            records = list(calendar_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_skips_events_without_id_if_no_fallback(self, calendar_syncer):
        calendars = [{"id": "cal1", "summary": "Work"}]
        events = [{}]  # No id, no summary, no start
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars), \
             patch.object(calendar_syncer, "_get_events", return_value=events):
            records = list(calendar_syncer.fetch_new(state))
        assert records == []

    def test_skips_empty_cal_id(self, calendar_syncer):
        calendars = [{"id": "", "summary": "Empty"}]
        state = SourceState()
        with patch.object(calendar_syncer, "_list_calendars", return_value=calendars):
            records = list(calendar_syncer.fetch_new(state))
        assert records == []


# ============================================================
# Claude Syncer Tests
# ============================================================

class TestClaudeSourceMetadata:
    """Tests for ClaudeSyncer class attributes."""

    def test_metadata(self, claude_syncer):
        assert claude_syncer.source_name == "claude"
        assert claude_syncer.display_name == "Claude Sessions"
        assert claude_syncer.category == "activity"


class TestClaudeParseTs:
    """Tests for ClaudeSyncer._parse_ts."""

    def test_none_input(self, claude_syncer):
        assert claude_syncer._parse_ts(None) is None

    def test_empty_string(self, claude_syncer):
        assert claude_syncer._parse_ts("") is None

    def test_iso_string(self, claude_syncer):
        result = claude_syncer._parse_ts("2025-01-01T10:00:00Z")
        assert result is not None
        assert result.year == 2025

    def test_datetime_passthrough(self, claude_syncer):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        result = claude_syncer._parse_ts(dt)
        assert result is dt

    def test_unparseable(self, claude_syncer):
        assert claude_syncer._parse_ts("not a date") is None


class TestClaudeSessionToRecord:
    """Tests for ClaudeSyncer._session_to_record."""

    def test_basic_session(self, claude_syncer, tmp_path):
        projects_dir = tmp_path / "projects"
        projects_dir.mkdir(exist_ok=True)
        index_dir = projects_dir / "proj1"
        index_dir.mkdir()

        # Create session JSONL file
        session_file = index_dir / "sess1.jsonl"
        msg_line = json.dumps({
            "type": "user",
            "message": {"content": "Hello Claude"},
        })
        session_file.write_text(msg_line + "\n")

        session_info = {
            "entry": {
                "sessionId": "sess1",
                "firstPrompt": "Hello Claude",
                "created": "2025-01-01T10:00:00Z",
                "modified": "2025-01-01T11:00:00Z",
                "messageCount": 5,
                "isSidechain": False,
                "gitBranch": "main",
            },
            "project_path": "/test/project",
            "index_dir": index_dir,
            "modified": datetime(2025, 1, 1, 11, 0),
        }

        record = claude_syncer._session_to_record(session_info)
        assert record is not None
        assert record["id"] == "session_sess1"
        assert record["type"] == "session"
        assert record["title"] == "Hello Claude"
        assert record["project_path"] == "/test/project"
        assert record["git_branch"] == "main"
        assert len(record["messages"]) == 1
        assert record["meta"]["session_id"] == "sess1"

    def test_no_session_id(self, claude_syncer, tmp_path):
        session_info = {
            "entry": {},
            "project_path": "",
            "index_dir": tmp_path,
            "modified": None,
        }
        assert claude_syncer._session_to_record(session_info) is None

    def test_session_file_not_found(self, claude_syncer, tmp_path):
        session_info = {
            "entry": {"sessionId": "missing"},
            "project_path": "",
            "index_dir": tmp_path,
            "modified": None,
        }
        assert claude_syncer._session_to_record(session_info) is None

    def test_session_with_list_content(self, claude_syncer, tmp_path):
        index_dir = tmp_path / "proj"
        index_dir.mkdir()
        session_file = index_dir / "sess2.jsonl"
        msg_line = json.dumps({
            "type": "user",
            "message": {
                "content": [
                    {"type": "text", "text": "Hello"},
                    {"type": "text", "text": "World"},
                    "plain string",
                ]
            },
        })
        session_file.write_text(msg_line + "\n")

        session_info = {
            "entry": {
                "sessionId": "sess2",
                "firstPrompt": "Hello World",
                "created": "2025-01-01T10:00:00Z",
                "modified": "2025-01-01T11:00:00Z",
            },
            "project_path": "/test",
            "index_dir": index_dir,
            "modified": datetime(2025, 1, 1),
        }
        record = claude_syncer._session_to_record(session_info)
        assert record is not None
        assert "Hello" in record["messages"][0]["content"]
        assert "World" in record["messages"][0]["content"]

    def test_empty_messages_returns_none(self, claude_syncer, tmp_path):
        """Session with only non-user messages returns None."""
        index_dir = tmp_path / "proj"
        index_dir.mkdir()
        session_file = index_dir / "sess3.jsonl"
        msg_line = json.dumps({
            "type": "assistant",
            "message": {"content": "Response"},
        })
        session_file.write_text(msg_line + "\n")

        session_info = {
            "entry": {"sessionId": "sess3", "firstPrompt": "X"},
            "project_path": "/test",
            "index_dir": index_dir,
            "modified": datetime(2025, 1, 1),
        }
        assert claude_syncer._session_to_record(session_info) is None

    def test_session_with_fullpath_fallback(self, claude_syncer, tmp_path):
        """Uses fullPath if session file not in index_dir."""
        alt_dir = tmp_path / "alt"
        alt_dir.mkdir()
        session_file = alt_dir / "sess4.jsonl"
        msg_line = json.dumps({
            "type": "user",
            "message": {"content": "Prompt"},
        })
        session_file.write_text(msg_line + "\n")

        index_dir = tmp_path / "nonexistent_idx"
        index_dir.mkdir()

        session_info = {
            "entry": {
                "sessionId": "sess4",
                "fullPath": str(session_file),
                "firstPrompt": "Prompt",
            },
            "project_path": "/test",
            "index_dir": index_dir,
            "modified": datetime(2025, 1, 1),
        }
        record = claude_syncer._session_to_record(session_info)
        assert record is not None

    def test_content_truncation(self, claude_syncer, tmp_path):
        """User message content is truncated to 5000 chars."""
        index_dir = tmp_path / "proj"
        index_dir.mkdir()
        session_file = index_dir / "sess5.jsonl"
        long_content = "A" * 10000
        msg_line = json.dumps({
            "type": "user",
            "message": {"content": long_content},
        })
        session_file.write_text(msg_line + "\n")

        session_info = {
            "entry": {"sessionId": "sess5", "firstPrompt": "Long"},
            "project_path": "/test",
            "index_dir": index_dir,
            "modified": datetime(2025, 1, 1),
        }
        record = claude_syncer._session_to_record(session_info)
        assert record is not None
        assert len(record["messages"][0]["content"]) == 5000


class TestClaudeFetchNew:
    """Tests for ClaudeSyncer.fetch_new."""

    def test_projects_dir_not_found(self, tmp_store, tmp_path):
        config = {"projects_dir": str(tmp_path / "nonexistent")}
        with patch.object(_claude_mod, "get_source_config", return_value=config):
            syncer = ClaudeSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_full_flow(self, claude_syncer, tmp_path):
        projects_dir = claude_syncer.projects_dir
        proj_dir = projects_dir / "project1"
        proj_dir.mkdir()

        # Create index
        index_data = {
            "originalPath": "/test/project",
            "entries": [
                {
                    "sessionId": "s1",
                    "firstPrompt": "Hello",
                    "created": "2025-01-01T10:00:00Z",
                    "modified": "2025-01-01T11:00:00Z",
                    "messageCount": 3,
                }
            ],
        }
        (proj_dir / "sessions-index.json").write_text(json.dumps(index_data))

        # Create session JSONL
        msg_line = json.dumps({
            "type": "user",
            "message": {"content": "Hello"},
        })
        (proj_dir / "s1.jsonl").write_text(msg_line + "\n")

        state = SourceState()
        records = list(claude_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "Hello"

    def test_skips_old_sessions(self, claude_syncer, tmp_path):
        projects_dir = claude_syncer.projects_dir
        proj_dir = projects_dir / "project1"
        proj_dir.mkdir()

        index_data = {
            "originalPath": "/test/project",
            "entries": [
                {
                    "sessionId": "s_old",
                    "firstPrompt": "Old",
                    "created": "2024-01-01T10:00:00Z",
                    "modified": "2024-01-01T11:00:00Z",
                }
            ],
        }
        (proj_dir / "sessions-index.json").write_text(json.dumps(index_data))
        msg_line = json.dumps({"type": "user", "message": {"content": "Old"}})
        (proj_dir / "s_old.jsonl").write_text(msg_line + "\n")

        state = SourceState(last_ts="2025-01-01T00:00:00Z")
        records = list(claude_syncer.fetch_new(state))
        assert records == []

    def test_respects_limit(self, claude_syncer, tmp_path):
        projects_dir = claude_syncer.projects_dir
        proj_dir = projects_dir / "project1"
        proj_dir.mkdir()

        entries = []
        for i in range(5):
            sid = f"s{i}"
            entries.append({
                "sessionId": sid,
                "firstPrompt": f"Prompt {i}",
                "created": f"2025-01-0{i+1}T10:00:00Z",
                "modified": f"2025-01-0{i+1}T11:00:00Z",
            })
            msg_line = json.dumps({"type": "user", "message": {"content": f"P{i}"}})
            (proj_dir / f"{sid}.jsonl").write_text(msg_line + "\n")

        index_data = {"originalPath": "/test", "entries": entries}
        (proj_dir / "sessions-index.json").write_text(json.dumps(index_data))

        state = SourceState()
        records = list(claude_syncer.fetch_new(state, limit=2))
        assert len(records) == 2

    def test_handles_corrupt_index(self, claude_syncer, tmp_path):
        projects_dir = claude_syncer.projects_dir
        proj_dir = projects_dir / "project1"
        proj_dir.mkdir()
        (proj_dir / "sessions-index.json").write_text("not valid json")

        state = SourceState()
        records = list(claude_syncer.fetch_new(state))
        assert records == []

    def test_skips_empty_content_messages(self, claude_syncer, tmp_path):
        """Messages with empty text content are skipped."""
        projects_dir = claude_syncer.projects_dir
        proj_dir = projects_dir / "project1"
        proj_dir.mkdir()

        index_data = {
            "originalPath": "/test",
            "entries": [{"sessionId": "sx", "modified": "2025-06-01T00:00:00Z"}],
        }
        (proj_dir / "sessions-index.json").write_text(json.dumps(index_data))

        # User message with empty content and then one with real content
        lines = [
            json.dumps({"type": "user", "message": {"content": "   "}}),
            json.dumps({"type": "user", "message": {"content": "Real prompt"}}),
        ]
        (proj_dir / "sx.jsonl").write_text("\n".join(lines) + "\n")

        state = SourceState()
        records = list(claude_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["messages"][0]["content"] == "Real prompt"


# ============================================================
# Granola Syncer Tests
# ============================================================

class TestGranolaSourceMetadata:
    """Tests for GranolaSyncer class attributes."""

    def test_metadata(self, granola_syncer):
        assert granola_syncer.source_name == "granola"
        assert granola_syncer.display_name == "Granola"
        assert granola_syncer.category == "meetings"


class TestGranolaParseTs:
    """Tests for GranolaSyncer._parse_ts."""

    def test_none_input(self, granola_syncer):
        assert granola_syncer._parse_ts(None) is None

    def test_empty_string(self, granola_syncer):
        assert granola_syncer._parse_ts("") is None

    def test_iso_string(self, granola_syncer):
        result = granola_syncer._parse_ts("2025-01-01T10:00:00Z")
        assert result is not None
        assert result.tzinfo is None  # Should be naive

    def test_datetime_strips_tz(self, granola_syncer):
        dt = datetime(2025, 6, 15, tzinfo=timezone.utc)
        result = granola_syncer._parse_ts(dt)
        assert result.tzinfo is None

    def test_unparseable(self, granola_syncer):
        assert granola_syncer._parse_ts("not a date") is None


class TestGranolaLoadCache:
    """Tests for GranolaSyncer._load_cache."""

    def test_cache_not_found(self, granola_syncer, tmp_path):
        granola_syncer.cache_path = tmp_path / "missing.json"
        with pytest.raises(FileNotFoundError):
            granola_syncer._load_cache()

    def test_nested_json_parsing(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        inner = json.dumps({"state": {"documents": {"d1": {"title": "Meeting"}}}})
        cache_file.write_text(json.dumps({"cache": inner}))
        granola_syncer.cache_path = cache_file

        result = granola_syncer._load_cache()
        assert "state" in result
        assert "d1" in result["state"]["documents"]


class TestGranolaDocToRecord:
    """Tests for GranolaSyncer._doc_to_record."""

    def test_basic_document(self, granola_syncer):
        doc = {
            "title": "Standup",
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T11:00:00Z",
            "notes_markdown": "# Notes\n- Item 1",
            "people": {
                "attendees": ["Alice", "Bob"],
                "creator": "Charlie",
            },
            "valid_meeting": True,
        }
        record = granola_syncer._doc_to_record("doc1", doc, [])
        assert record is not None
        assert record["id"] == "meeting_doc1"
        assert record["type"] == "meeting"
        assert record["title"] == "Standup"
        assert "Alice" in record["participants"]
        assert "Charlie" in record["participants"]
        assert record["notes"] == "# Notes\n- Item 1"
        assert record["meta"]["valid_meeting"] is True

    def test_empty_doc_no_transcript_returns_none(self, granola_syncer):
        doc = {"notes_markdown": "", "notes_plain": ""}
        assert granola_syncer._doc_to_record("doc2", doc, []) is None

    def test_transcript_segments(self, granola_syncer):
        doc = {"title": "Call", "notes_markdown": "Notes"}
        segments = [
            {"text": "Hello there"},
            {"text": "How are you"},
            {"text": "  "},  # empty after strip
        ]
        record = granola_syncer._doc_to_record("doc3", doc, segments)
        assert record is not None
        assert "Hello there" in record["transcript"]
        assert "How are you" in record["transcript"]
        assert record["meta"]["has_transcript"] is True
        assert record["meta"]["segment_count"] == 3

    def test_duration_from_calendar_event(self, granola_syncer):
        doc = {
            "title": "Long Meeting",
            "notes_markdown": "Notes",
            "google_calendar_event": {
                "start": "2025-01-01T10:00:00Z",
                "end": "2025-01-01T11:30:00Z",
            },
        }
        record = granola_syncer._doc_to_record("doc4", doc, [])
        assert record["duration_minutes"] == 90

    def test_attendees_as_dicts(self, granola_syncer):
        doc = {
            "title": "Meeting",
            "notes_markdown": "X",
            "people": {
                "attendees": [
                    {"name": "Alice", "email": "alice@test.com"},
                    {"email": "bob@test.com"},
                    {"notname": "x"},
                ],
            },
        }
        record = granola_syncer._doc_to_record("doc5", doc, [])
        assert "Alice" in record["participants"]
        assert "bob@test.com" in record["participants"]
        assert "Unknown" in record["participants"]

    def test_untitled_meeting(self, granola_syncer):
        doc = {"notes_markdown": "Some notes"}
        record = granola_syncer._doc_to_record("doc6", doc, [])
        assert record["title"] == "Untitled Meeting"

    def test_prefers_markdown_notes(self, granola_syncer):
        doc = {
            "notes_markdown": "# Markdown",
            "notes_plain": "Plain text",
        }
        record = granola_syncer._doc_to_record("doc7", doc, [])
        assert record["notes"] == "# Markdown"

    def test_falls_back_to_plain_notes(self, granola_syncer):
        doc = {
            "notes_markdown": "",
            "notes_plain": "Plain text",
        }
        record = granola_syncer._doc_to_record("doc8", doc, [])
        assert record["notes"] == "Plain text"

    def test_creator_not_duplicated(self, granola_syncer):
        doc = {
            "title": "M",
            "notes_markdown": "X",
            "people": {
                "attendees": ["Charlie"],
                "creator": "Charlie",
            },
        }
        record = granola_syncer._doc_to_record("doc9", doc, [])
        assert record["participants"].count("Charlie") == 1


class TestGranolaFetchNew:
    """Tests for GranolaSyncer.fetch_new."""

    def test_cache_error_handled_gracefully(self, granola_syncer, tmp_path):
        granola_syncer.cache_path = tmp_path / "missing.json"
        state = SourceState()
        records = list(granola_syncer.fetch_new(state))
        assert records == []

    def test_full_flow(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        inner = {
            "state": {
                "documents": {
                    "d1": {
                        "title": "Standup",
                        "created_at": "2025-01-01T10:00:00Z",
                        "updated_at": "2025-01-01T11:00:00Z",
                        "notes_markdown": "Notes",
                    }
                },
                "transcripts": {},
            }
        }
        cache_file.write_text(json.dumps({"cache": json.dumps(inner)}))
        granola_syncer.cache_path = cache_file

        state = SourceState()
        records = list(granola_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["title"] == "Standup"

    def test_skips_deleted_documents(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        inner = {
            "state": {
                "documents": {
                    "d1": {
                        "title": "Deleted",
                        "notes_markdown": "X",
                        "deleted_at": "2025-01-01T12:00:00Z",
                    }
                },
                "transcripts": {},
            }
        }
        cache_file.write_text(json.dumps({"cache": json.dumps(inner)}))
        granola_syncer.cache_path = cache_file

        state = SourceState()
        records = list(granola_syncer.fetch_new(state))
        assert records == []

    def test_skips_old_documents(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        inner = {
            "state": {
                "documents": {
                    "d1": {
                        "title": "Old",
                        "created_at": "2024-01-01T10:00:00Z",
                        "updated_at": "2024-01-01T11:00:00Z",
                        "notes_markdown": "Old notes",
                    }
                },
                "transcripts": {},
            }
        }
        cache_file.write_text(json.dumps({"cache": json.dumps(inner)}))
        granola_syncer.cache_path = cache_file

        state = SourceState(last_ts="2025-01-01T00:00:00Z")
        records = list(granola_syncer.fetch_new(state))
        assert records == []

    def test_respects_limit(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        docs = {}
        for i in range(10):
            docs[f"d{i}"] = {
                "title": f"Meeting {i}",
                "created_at": f"2025-01-0{(i % 9) + 1}T10:00:00Z",
                "updated_at": f"2025-01-0{(i % 9) + 1}T11:00:00Z",
                "notes_markdown": f"Notes {i}",
            }
        inner = {"state": {"documents": docs, "transcripts": {}}}
        cache_file.write_text(json.dumps({"cache": json.dumps(inner)}))
        granola_syncer.cache_path = cache_file

        state = SourceState()
        records = list(granola_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_includes_transcripts(self, granola_syncer, tmp_path):
        cache_file = tmp_path / "cache.json"
        inner = {
            "state": {
                "documents": {
                    "d1": {
                        "title": "Call",
                        "updated_at": "2025-01-01T10:00:00Z",
                        "notes_markdown": "Notes",
                    }
                },
                "transcripts": {
                    "d1": [{"text": "Hello"}, {"text": "World"}],
                },
            }
        }
        cache_file.write_text(json.dumps({"cache": json.dumps(inner)}))
        granola_syncer.cache_path = cache_file

        state = SourceState()
        records = list(granola_syncer.fetch_new(state))
        assert len(records) == 1
        assert "Hello" in records[0]["transcript"]


# ============================================================
# Nextcloud Syncer Tests
# ============================================================

class TestNextcloudSourceMetadata:
    """Tests for NextcloudSyncer class attributes."""

    def test_metadata(self, nextcloud_syncer):
        assert nextcloud_syncer.source_name == "nextcloud"
        assert nextcloud_syncer.display_name == "Nextcloud"
        assert nextcloud_syncer.category == "files"

    def test_config_values(self, nextcloud_syncer):
        assert nextcloud_syncer.server == "https://drive.test.com"
        assert nextcloud_syncer.username == "testuser"
        assert nextcloud_syncer.token == "testtoken"
        assert nextcloud_syncer.max_results == 200


class TestNextcloudIsTextType:
    """Tests for NextcloudSyncer._is_text_type."""

    def test_text_plain(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("text/plain") is True

    def test_text_markdown(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("text/markdown") is True

    def test_application_json(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("application/json") is True

    def test_text_prefix(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("text/x-python") is True

    def test_image_not_text(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("image/png") is False

    def test_application_pdf_not_text(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("application/pdf") is False

    def test_empty_string(self, nextcloud_syncer):
        assert nextcloud_syncer._is_text_type("") is False


class TestNextcloudParsePropfind:
    """Tests for NextcloudSyncer._parse_propfind."""

    def _make_xml(self, nextcloud_syncer, entries):
        """Build minimal PROPFIND XML for testing."""
        username = nextcloud_syncer.username
        lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        lines.append('<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">')
        for entry in entries:
            href = entry.get("href", f"/remote.php/dav/files/{username}/{entry.get('path', '')}")
            lines.append(f"<d:response><d:href>{href}</d:href>")
            lines.append("<d:propstat><d:prop>")
            if "resourcetype" in entry and entry["resourcetype"] == "collection":
                lines.append("<d:resourcetype><d:collection/></d:resourcetype>")
            else:
                lines.append("<d:resourcetype/>")
            if "modified" in entry:
                lines.append(f"<d:getlastmodified>{entry['modified']}</d:getlastmodified>")
            if "size" in entry:
                lines.append(f"<d:getcontentlength>{entry['size']}</d:getcontentlength>")
            if "mime" in entry:
                lines.append(f"<d:getcontenttype>{entry['mime']}</d:getcontenttype>")
            if "etag" in entry:
                lines.append(f'<d:getetag>"{entry["etag"]}"</d:getetag>')
            if "fileid" in entry:
                lines.append(f"<oc:fileid>{entry['fileid']}</oc:fileid>")
            lines.append("</d:prop></d:propstat></d:response>")
        lines.append("</d:multistatus>")
        return "\n".join(lines)

    def test_parses_files(self, nextcloud_syncer):
        xml = self._make_xml(nextcloud_syncer, [
            {"path": "docs/readme.md", "modified": "Mon, 01 Jan 2025 10:00:00 GMT", "size": "100", "mime": "text/markdown", "etag": "abc", "fileid": "42"},
        ])
        files = nextcloud_syncer._parse_propfind(xml)
        assert len(files) == 1
        assert files[0]["path"] == "docs/readme.md"
        assert files[0]["name"] == "readme.md"
        assert files[0]["mime_type"] == "text/markdown"
        assert files[0]["size_bytes"] == 100
        assert files[0]["etag"] == "abc"
        assert files[0]["file_id"] == "42"

    def test_skips_directories(self, nextcloud_syncer):
        xml = self._make_xml(nextcloud_syncer, [
            {"path": "docs/", "resourcetype": "collection"},
            {"path": "docs/file.txt", "mime": "text/plain"},
        ])
        files = nextcloud_syncer._parse_propfind(xml)
        assert len(files) == 1
        assert files[0]["path"] == "docs/file.txt"

    def test_skips_binary_extensions(self, nextcloud_syncer):
        xml = self._make_xml(nextcloud_syncer, [
            {"path": "photo.jpg"},
            {"path": "archive.zip"},
            {"path": "notes.md", "mime": "text/markdown"},
        ])
        files = nextcloud_syncer._parse_propfind(xml)
        assert len(files) == 1
        assert files[0]["path"] == "notes.md"

    def test_skips_trash_dir(self, nextcloud_syncer):
        xml = self._make_xml(nextcloud_syncer, [
            {"path": ".Trash/old.md", "mime": "text/markdown"},
            {"path": "good.md", "mime": "text/markdown"},
        ])
        files = nextcloud_syncer._parse_propfind(xml)
        assert len(files) == 1
        assert files[0]["path"] == "good.md"

    def test_skips_root_path(self, nextcloud_syncer):
        """Root directory entry (empty path) should be skipped."""
        xml = self._make_xml(nextcloud_syncer, [
            {"path": ""},
            {"path": "file.txt", "mime": "text/plain"},
        ])
        files = nextcloud_syncer._parse_propfind(xml)
        assert len(files) == 1


class TestNextcloudFileToRecord:
    """Tests for NextcloudSyncer._file_to_record."""

    def test_basic_file(self, nextcloud_syncer):
        file_info = {
            "path": "docs/readme.md",
            "name": "readme.md",
            "mime_type": "text/markdown",
            "modified": datetime(2025, 1, 1, 10, 0),
            "size_bytes": 1024,
            "etag": "abc123",
            "file_id": "42",
        }
        with patch.object(nextcloud_syncer, "_get_content_preview", return_value="# README"):
            record = nextcloud_syncer._file_to_record(file_info)
        assert record["type"] == "cloud_file"
        assert record["name"] == "readme.md"
        assert record["path"] == "/docs/readme.md"
        assert record["content_preview"] == "# README"
        assert record["size_bytes"] == 1024
        assert record["meta"]["etag"] == "abc123"

    def test_non_text_file_no_preview(self, nextcloud_syncer):
        file_info = {
            "path": "data.bin",
            "name": "data.bin",
            "mime_type": "application/octet-stream",
            "modified": None,
            "size_bytes": 5000,
            "etag": "",
            "file_id": "",
        }
        record = nextcloud_syncer._file_to_record(file_info)
        assert record["content_preview"] == ""

    def test_no_modified_date(self, nextcloud_syncer):
        file_info = {
            "path": "file.txt",
            "name": "file.txt",
            "mime_type": "text/plain",
            "modified": None,
            "size_bytes": 0,
            "etag": "",
            "file_id": "",
        }
        with patch.object(nextcloud_syncer, "_get_content_preview", return_value=""):
            record = nextcloud_syncer._file_to_record(file_info)
        assert record["modified_at"] is None


class TestNextcloudGetContentPreview:
    """Tests for NextcloudSyncer._get_content_preview."""

    def test_successful_download(self, nextcloud_syncer):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "Hello World content"
        with patch.object(_nc_mod.requests, "get", return_value=mock_resp):
            result = nextcloud_syncer._get_content_preview("docs/file.txt")
        assert result == "Hello World content"

    def test_partial_content_206(self, nextcloud_syncer):
        mock_resp = MagicMock()
        mock_resp.status_code = 206
        mock_resp.text = "Partial content"
        with patch.object(_nc_mod.requests, "get", return_value=mock_resp):
            result = nextcloud_syncer._get_content_preview("docs/file.txt")
        assert result == "Partial content"

    def test_error_returns_empty(self, nextcloud_syncer):
        with patch.object(_nc_mod.requests, "get", side_effect=Exception("timeout")):
            result = nextcloud_syncer._get_content_preview("docs/file.txt")
        assert result == ""

    def test_non_200_returns_empty(self, nextcloud_syncer):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch.object(_nc_mod.requests, "get", return_value=mock_resp):
            result = nextcloud_syncer._get_content_preview("docs/file.txt")
        assert result == ""


class TestNextcloudListFiles:
    """Tests for NextcloudSyncer._list_files."""

    def test_successful_list(self, nextcloud_syncer):
        mock_resp = MagicMock()
        mock_resp.status_code = 207
        mock_resp.text = """<?xml version="1.0" encoding="UTF-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
<d:response>
<d:href>/remote.php/dav/files/testuser/test.md</d:href>
<d:propstat><d:prop>
<d:resourcetype/>
<d:getcontenttype>text/markdown</d:getcontenttype>
<d:getcontentlength>100</d:getcontentlength>
</d:prop></d:propstat>
</d:response>
</d:multistatus>"""
        mock_resp.raise_for_status = MagicMock()
        with patch.object(_nc_mod.requests, "request", return_value=mock_resp):
            files = nextcloud_syncer._list_files()
        assert len(files) == 1
        assert files[0]["path"] == "test.md"

    def test_error_raises(self, nextcloud_syncer):
        with patch.object(_nc_mod.requests, "request", side_effect=Exception("network")):
            with pytest.raises(Exception):
                nextcloud_syncer._list_files()


class TestNextcloudFetchNew:
    """Tests for NextcloudSyncer.fetch_new."""

    def test_no_credentials(self, tmp_store):
        config = {"server": "https://x.com", "username": "", "token": ""}
        with patch.object(_nc_mod, "get_source_config", return_value=config), \
             patch.dict(os.environ, {"NEXTCLOUD_USER": "", "NEXTCLOUD_TOKEN": ""}, clear=False):
            syncer = NextcloudSyncer(tmp_store, config)
        state = SourceState()
        records = list(syncer.fetch_new(state))
        assert records == []

    def test_full_flow(self, nextcloud_syncer):
        files = [
            {"path": "docs/readme.md", "name": "readme.md", "mime_type": "text/markdown",
             "modified": datetime(2025, 1, 1), "size_bytes": 100, "etag": "e1", "file_id": "1"},
        ]
        state = SourceState()
        with patch.object(nextcloud_syncer, "_list_files", return_value=files), \
             patch.object(nextcloud_syncer, "_get_content_preview", return_value="content"):
            records = list(nextcloud_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["name"] == "readme.md"

    def test_filters_by_last_modified(self, nextcloud_syncer):
        files = [
            {"path": "old.md", "name": "old.md", "mime_type": "text/plain",
             "modified": datetime(2024, 1, 1, tzinfo=timezone.utc), "size_bytes": 50, "etag": "", "file_id": ""},
            {"path": "new.md", "name": "new.md", "mime_type": "text/plain",
             "modified": datetime(2025, 6, 1, tzinfo=timezone.utc), "size_bytes": 50, "etag": "", "file_id": ""},
        ]
        state = SourceState(last_ts="2025-01-01T00:00:00Z")
        with patch.object(nextcloud_syncer, "_list_files", return_value=files), \
             patch.object(nextcloud_syncer, "_get_content_preview", return_value=""):
            records = list(nextcloud_syncer.fetch_new(state))
        assert len(records) == 1
        assert records[0]["name"] == "new.md"

    def test_respects_limit(self, nextcloud_syncer):
        files = [
            {"path": f"f{i}.txt", "name": f"f{i}.txt", "mime_type": "text/plain",
             "modified": datetime(2025, 1, i + 1), "size_bytes": 10, "etag": "", "file_id": ""}
            for i in range(10)
        ]
        state = SourceState()
        with patch.object(nextcloud_syncer, "_list_files", return_value=files), \
             patch.object(nextcloud_syncer, "_get_content_preview", return_value=""):
            records = list(nextcloud_syncer.fetch_new(state, limit=3))
        assert len(records) == 3

    def test_list_files_error_handled(self, nextcloud_syncer):
        state = SourceState()
        with patch.object(nextcloud_syncer, "_list_files", side_effect=Exception("network error")):
            records = list(nextcloud_syncer.fetch_new(state))
        assert records == []

    def test_sorts_by_modification_time(self, nextcloud_syncer):
        files = [
            {"path": "b.txt", "name": "b.txt", "mime_type": "text/plain",
             "modified": datetime(2025, 3, 1), "size_bytes": 10, "etag": "", "file_id": ""},
            {"path": "a.txt", "name": "a.txt", "mime_type": "text/plain",
             "modified": datetime(2025, 1, 1), "size_bytes": 10, "etag": "", "file_id": ""},
        ]
        state = SourceState()
        with patch.object(nextcloud_syncer, "_list_files", return_value=files), \
             patch.object(nextcloud_syncer, "_get_content_preview", return_value=""):
            records = list(nextcloud_syncer.fetch_new(state))
        assert records[0]["name"] == "a.txt"
        assert records[1]["name"] == "b.txt"

    def test_content_preview_disabled(self, nextcloud_syncer):
        nextcloud_syncer.content_preview = False
        files = [
            {"path": "f.md", "name": "f.md", "mime_type": "text/markdown",
             "modified": datetime(2025, 1, 1), "size_bytes": 10, "etag": "", "file_id": ""},
        ]
        state = SourceState()
        with patch.object(nextcloud_syncer, "_list_files", return_value=files):
            records = list(nextcloud_syncer.fetch_new(state))
        assert records[0]["content_preview"] == ""


class TestNextcloudInitFromEnv:
    """Tests for NextcloudSyncer init with environment variables."""

    def test_reads_from_env(self, tmp_store):
        config = {"server": "https://x.com"}
        with patch.object(_nc_mod, "get_source_config", return_value=config), \
             patch.dict(os.environ, {"NEXTCLOUD_USER": "envuser", "NEXTCLOUD_TOKEN": "envtoken"}):
            syncer = NextcloudSyncer(tmp_store, config)
        assert syncer.username == "envuser"
        assert syncer.token == "envtoken"


# ============================================================
# Custom source auto-discovery
# ============================================================

class TestCustomSourceDiscovery:
    """Auto-discovery finds custom sources dropped into the sources dir."""

    def test_discovers_custom_source(self, tmp_path, tmp_store):
        """A folder with syncer.py containing a BaseSyncer subclass gets auto-discovered."""
        import vadimgest.ingest.sources as src_mod

        sources_dir = Path(src_mod.__file__).parent
        custom_dir = sources_dir / "zztest_autodiscovery"
        custom_dir.mkdir()

        try:
            (custom_dir / "__init__.py").touch()
            (custom_dir / "syncer.py").write_text(
                "from vadimgest.ingest.sources.base import BaseSyncer\n"
                "\n"
                "class AutoTestSyncer(BaseSyncer):\n"
                "    source_name = 'zztest_autodiscovery'\n"
                "    display_name = 'Auto Test'\n"
                "    description = 'test auto-discovery'\n"
                "    category = 'test'\n"
                "    dependencies = {'python': [], 'cli': [], 'credentials': [], 'os': []}\n"
                "    config_schema = {'x': {'type': 'int', 'default': 1}}\n"
                "    def fetch_new(self, state, limit=1000): return iter([])\n"
            )

            # Reset discovery state so it re-scans
            src_mod._discovery_done = False
            src_mod._loaded.pop("zztest_autodiscovery", None)
            src_mod._failed.pop("zztest_autodiscovery", None)
            src_mod._SYNCER_REGISTRY.pop("zztest_autodiscovery", None)

            names = src_mod.all_source_names()
            assert "zztest_autodiscovery" in names

            cls = src_mod.get_syncer_class("zztest_autodiscovery")
            assert cls is not None
            assert cls.display_name == "Auto Test"
            assert cls.category == "test"
        finally:
            import shutil
            shutil.rmtree(custom_dir, ignore_errors=True)
            src_mod._SYNCER_REGISTRY.pop("zztest_autodiscovery", None)
            src_mod._loaded.pop("zztest_autodiscovery", None)
            src_mod._discovery_done = False

    def test_ignores_dir_without_syncer(self, tmp_path):
        """Directories without syncer.py are ignored."""
        import vadimgest.ingest.sources as src_mod

        sources_dir = Path(src_mod.__file__).parent
        custom_dir = sources_dir / "zztest_no_syncer"
        custom_dir.mkdir()
        (custom_dir / "__init__.py").touch()

        try:
            src_mod._discovery_done = False
            names = src_mod.all_source_names()
            assert "zztest_no_syncer" not in names
        finally:
            import shutil
            shutil.rmtree(custom_dir, ignore_errors=True)
            src_mod._discovery_done = False

    def test_external_sources_dir(self, tmp_path, monkeypatch):
        """Sources in VADIMGEST_SOURCES_DIR are discovered and loadable."""
        import vadimgest.ingest.sources as src_mod

        ext_dir = tmp_path / "custom_sources"
        src_dir = ext_dir / "zzext_weather"
        src_dir.mkdir(parents=True)
        (src_dir / "syncer.py").write_text(
            "from vadimgest.ingest.sources.base import CronSyncer\n"
            "\n"
            "class WeatherSyncer(CronSyncer):\n"
            "    source_name = 'zzext_weather'\n"
            "    display_name = 'Weather'\n"
            "    description = 'Weather data'\n"
            "    category = 'activity'\n"
            "    dependencies = {'python': [], 'cli': [], 'credentials': [], 'os': []}\n"
            "    config_schema = {'city': {'type': 'str', 'default': 'SF'}}\n"
            "    def fetch_new(self, state, limit=1000): return iter([])\n"
        )

        monkeypatch.setenv("VADIMGEST_SOURCES_DIR", str(ext_dir))
        src_mod._discovery_done = False
        src_mod._loaded.pop("zzext_weather", None)
        src_mod._failed.pop("zzext_weather", None)
        src_mod._SYNCER_REGISTRY.pop("zzext_weather", None)

        try:
            names = src_mod.all_source_names()
            assert "zzext_weather" in names

            cls = src_mod.get_syncer_class("zzext_weather")
            assert cls is not None
            assert cls.display_name == "Weather"
            assert cls.config_schema == {"city": {"type": "str", "default": "SF"}}
        finally:
            src_mod._SYNCER_REGISTRY.pop("zzext_weather", None)
            src_mod._loaded.pop("zzext_weather", None)
            src_mod._discovery_done = False
