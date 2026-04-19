"""Telegram CronSyncer - fetches new messages via iter_dialogs + rule engine.

Uses SQLiteSession for entity cache. On each sync:
1. Connect, build rule set (folders + contacts + DMs)
2. iter_dialogs until cutoff, fetch new messages per whitelisted chat
3. Disconnect

Voice messages: if transcribe_voice=true in config, uses Telegram Premium's
built-in TranscribeAudio API (free with Premium).
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from telethon import TelegramClient
from telethon.sessions import StringSession, SQLiteSession
from telethon.tl.types import (
    DialogFilter, DialogFilterDefault,
    MessageMediaDocument, DocumentAttributeAudio,
)
from telethon.tl import functions
from telethon.tl.functions.messages import TranscribeAudioRequest

from ..base import CronSyncer
from ....config import get_source_config, get_credentials_dir


class TelegramSyncer(CronSyncer):
    """Cron-based Telegram syncer. iter_dialogs + per-chat fetch + rule engine."""

    source_name = "telegram"
    display_name = "Telegram"
    description = "Messages from Telegram chats, groups, and channels"
    category = "messaging"
    dependencies = {
        "python": ["telethon"],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    credential_help = {}
    config_schema = {
        "monitored_folders": {"type": "list", "default": [], "description": "Telegram folders to sync messages from (empty = all chats)", "placeholder": "Work\nFamily"},
        "max_messages_per_chat": {"type": "int", "default": 200, "description": "Maximum number of messages to fetch per chat in each sync cycle", "min": 1, "max": 10000, "placeholder": "200"},
        "transcribe_voice": {"type": "bool", "default": False, "description": "Transcribe voice messages using Telegram Premium transcription"},
        "exclude_patterns": {"type": "list", "default": [], "description": "Exclude chats whose name matches any pattern (substring match)", "placeholder": "Spam Group\nAds Channel"},
    }

    def __init__(self, store, config: dict | None = None):
        config = config or get_source_config("telegram")
        super().__init__(store, config)

        credentials_dir = get_credentials_dir()
        self.session_path = str(credentials_dir / "telegram")  # .session added by Telethon
        self.string_session_file = credentials_dir / "telegram_session.txt"

    # --- session ---

    async def _ensure_sqlite_session(self) -> bool:
        """Migrate StringSession → SQLiteSession if needed. Returns True if first run."""
        session_file = Path(self.session_path + ".session")
        if session_file.exists():
            return False

        if not self.string_session_file.exists():
            raise RuntimeError("No telegram session file found")

        ss_data = self.string_session_file.read_text().strip()
        api_id = int(self.config.get("api_id", 0))
        api_hash = self.config.get("api_hash", "")

        tmp_client = TelegramClient(StringSession(ss_data), api_id, api_hash)
        await tmp_client.connect()

        dc_id = tmp_client.session.dc_id
        server_address = tmp_client.session.server_address
        port = tmp_client.session.port
        auth_key = tmp_client.session.auth_key
        await tmp_client.disconnect()

        sq = SQLiteSession(self.session_path)
        sq.set_dc(dc_id, server_address, port)
        sq.auth_key = auth_key
        await sq.save()
        self.log("Migrated StringSession → SQLiteSession")
        return True

    def _get_client(self) -> TelegramClient:
        api_id = int(self.config.get("api_id", 0))
        api_hash = self.config.get("api_hash", "")
        return TelegramClient(self.session_path, api_id, api_hash)

    # --- helpers ---

    @staticmethod
    def _peer_id(peer) -> int:
        """Extract numeric ID from any Telegram peer type."""
        for attr in ("user_id", "channel_id", "chat_id"):
            if hasattr(peer, attr):
                return getattr(peer, attr)
        return 0

    def _entity_name(self, entity) -> str:
        if not entity:
            return "Unknown"
        if hasattr(entity, "title"):
            return entity.title or "Unknown"
        name = (getattr(entity, "first_name", "") or "").strip()
        last = getattr(entity, "last_name", None)
        if last:
            name = f"{name} {last}".strip()
        return name or "Unknown"

    def _should_exclude(self, name: str) -> bool:
        if not name:
            return False
        patterns = self.config.get("exclude_patterns") or []
        low = name.lower()
        return any(p.lower() in low for p in patterns)

    @staticmethod
    def _is_voice(msg) -> bool:
        """Check if message is a voice message."""
        media = getattr(msg, "media", None)
        if not isinstance(media, MessageMediaDocument):
            return False
        doc = getattr(media, "document", None)
        if not doc:
            return False
        for attr in getattr(doc, "attributes", []):
            if isinstance(attr, DocumentAttributeAudio) and getattr(attr, "voice", False):
                return True
        return False

    async def _transcribe_voice(self, client, msg) -> str | None:
        """Transcribe voice message using Telegram Premium's built-in API."""
        try:
            result = await client(TranscribeAudioRequest(
                peer=msg.peer_id,
                msg_id=msg.id,
            ))
            # If still processing, poll up to 5 times with 2s intervals
            if result.pending:
                for _ in range(5):
                    await asyncio.sleep(2)
                    result = await client(TranscribeAudioRequest(
                        peer=msg.peer_id,
                        msg_id=msg.id,
                    ))
                    if not result.pending:
                        break
            return result.text if result.text else None
        except Exception as e:
            self.log(f"Voice transcription failed: {str(e)[:80]}")
            return None

    # --- rule-based filtering ---

    async def _build_chat_rules(self, client) -> dict:
        """Build chat_id → {name, folder} from config rules.

        Sources:
        1. Chats from monitored folders (peer IDs extracted directly - no get_entity)
        2. Auto-include DMs (handled in _async_fetch during iter_dialogs)
        3. Explicit include_chat_ids
        Minus: exclude_chat_ids and exclude_patterns
        """
        allowed = {}

        # 1. Monitored folders (extract peer IDs directly, no get_entity calls)
        folders = self.config.get("monitored_folders", [])
        if folders:
            try:
                result = await client(functions.messages.GetDialogFiltersRequest())
                for f in result.filters:
                    if isinstance(f, DialogFilterDefault) or not isinstance(f, DialogFilter):
                        continue
                    title = f.title
                    if hasattr(title, "text"):
                        title = title.text
                    if title not in folders:
                        continue
                    count = 0
                    for peer in getattr(f, "include_peers", []):
                        pid = str(self._peer_id(peer))
                        if pid != "0":
                            allowed[pid] = {"name": pid, "folder": title}
                            count += 1
                    self.log(f"  Folder '{title}': {count} chats")
            except Exception as e:
                self.log(f"  Folder error: {e}")

        # 2. Explicit includes
        for cid in self.config.get("include_chat_ids") or []:
            s = str(cid)
            if s not in allowed:
                allowed[s] = {"name": s, "folder": "Manual"}

        # 3. Explicit excludes
        for cid in self.config.get("exclude_chat_ids") or []:
            allowed.pop(str(cid), None)

        return allowed

    # --- fetch via iter_dialogs ---

    async def _async_fetch(self, state, limit: int) -> list[dict]:
        first_run = await self._ensure_sqlite_session()
        client = self._get_client()
        await client.connect()

        if not await client.is_user_authorized():
            self.log("Session not authorized")
            await client.disconnect()
            return []

        me = await client.get_me()
        self.log(f"Connected as {me.first_name} (ID: {me.id})")

        if first_run:
            self.log("First run - warming entity cache...")
            await client.get_dialogs()

        # Build rules
        chat_rules = await self._build_chat_rules(client)
        self.log(f"Rules: {len(chat_rules)} allowed chats")

        auto_dm = self.config.get("auto_add_private", True)
        per_chat = state.extra.get("per_chat", {})
        records = []

        # Cutoff: don't scan dialogs older than last sync
        cutoff = None
        if state.last_ts:
            cutoff = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)

        matched = 0
        stale_streak = 0
        max_stale = 50  # stop after N consecutive dialogs older than cutoff
        async for dialog in client.iter_dialogs():
            if cutoff and dialog.date and dialog.date < cutoff:
                stale_streak += 1
                if stale_streak >= max_stale:
                    break
                continue
            stale_streak = 0

            chat_id = str(dialog.entity.id)

            # Auto-include DMs
            if auto_dm and dialog.is_user:
                if chat_id not in chat_rules:
                    name = self._entity_name(dialog.entity)
                    if not self._should_exclude(name):
                        chat_rules[chat_id] = {"name": name, "folder": "DM"}

            if chat_id not in chat_rules:
                continue

            # Exclude check by name
            info = chat_rules[chat_id]
            dialog_name = dialog.name or info.get("name", chat_id)
            if self._should_exclude(dialog_name):
                continue

            # Update placeholder name from dialog
            if info["name"] == chat_id:
                info["name"] = dialog_name

            matched += 1
            last_msg_id = per_chat.get(chat_id, {}).get("last_message_id")

            try:
                kwargs = {"limit": min(limit, 200), "reverse": True}
                if last_msg_id:
                    kwargs["min_id"] = last_msg_id
                elif cutoff:
                    kwargs["offset_date"] = cutoff

                transcribe = self.config.get("transcribe_voice", False)
                new_last_id = last_msg_id
                async for msg in client.iter_messages(dialog.entity, **kwargs):
                    text = msg.message
                    media_type = None

                    # Voice message transcription
                    if not text and transcribe and self._is_voice(msg):
                        text = await self._transcribe_voice(client, msg)
                        media_type = "voice"

                    if not text:
                        continue

                    sender_name = self._entity_name(msg.sender)

                    meta = {
                        "chat_id": int(chat_id),
                        "message_id": msg.id,
                        "sender_id": msg.sender_id,
                    }
                    if media_type:
                        meta["media_type"] = media_type

                    records.append({
                        "id": f"{chat_id}_{msg.id}",
                        "type": "message",
                        "chat": info["name"],
                        "folder": info.get("folder"),
                        "timestamp": msg.date.isoformat() if msg.date else datetime.now(timezone.utc).isoformat(),
                        "sender": sender_name,
                        "text": text,
                        "meta": meta,
                    })
                    if new_last_id is None or msg.id > new_last_id:
                        new_last_id = msg.id

                if new_last_id and new_last_id != last_msg_id:
                    per_chat[chat_id] = {"last_message_id": new_last_id}

            except Exception as e:
                self.log(f"  {info['name']}: error - {str(e)[:80]}")

        self.log(f"Scanned {matched} active whitelisted chats")
        await client.disconnect()

        # Persist per-chat cursors (re-read state to avoid overwriting last_ts)
        current_state = self.store.get_state(self.source_name)
        current_state.extra["per_chat"] = per_chat
        self.store.set_state(self.source_name, current_state)

        return records

    # --- CronSyncer interface ---

    def fetch_new(self, state, limit: int = 1000) -> Iterator[dict]:
        records = asyncio.run(self._async_fetch(state, limit))
        self.log(f"Fetched {len(records)} messages")
        yield from records
