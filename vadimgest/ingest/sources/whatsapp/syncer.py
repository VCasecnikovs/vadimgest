"""WhatsApp Syncer - sync messages from WhatsApp via wacli CLI.

Groups messages into conversation chunks (same pattern as Telegram/Signal/iMessage).
"""

import json
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config, get_conversation_settings


def _wacli_call(command: list[str], timeout: int = 30) -> Any:
    """
    Call wacli CLI with --json flag, parse and return JSON response.

    Args:
        command: wacli subcommand + args, e.g. ["chats", "list", "--limit", "50"]
        timeout: subprocess timeout in seconds

    Returns:
        Parsed value of response["data"] on success

    Raises:
        RuntimeError: if wacli call fails or returns success=false
    """
    cmd = ["wacli"] + command + ["--json"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    if result.returncode != 0:
        raise RuntimeError(f"wacli {' '.join(command)} failed: {result.stderr.strip()}")

    if not result.stdout.strip():
        return None

    parsed = json.loads(result.stdout)

    if not parsed.get("success", True):
        raise RuntimeError(f"wacli {' '.join(command)} error: {parsed}")

    return parsed.get("data")


class WhatsAppSyncer(CronSyncer):
    """WhatsApp syncer via wacli with conversation grouping."""

    source_name = "whatsapp"
    display_name = "WhatsApp"
    description = "Messages from WhatsApp chats and groups"
    category = "messaging"
    dependencies = {
        "python": [],
        "cli": ["wacli"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "chat_limit": {"type": "int", "default": 50, "description": "Maximum number of chats to scan per sync", "min": 1, "max": 1000, "placeholder": "50"},
        "fetch_limit": {"type": "int", "default": 50, "description": "Maximum messages to fetch per chat", "min": 1, "max": 10000, "placeholder": "50"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("whatsapp")
        super().__init__(store, config)

        self.fetch_limit = config.get("fetch_limit", 50)
        self.chat_limit = config.get("chat_limit", 50)

        # JID aliases: map LID JIDs -> canonical phone JIDs
        # Merges split conversations from WhatsApp's LID migration
        self.jid_aliases: dict[str, str] = config.get("jid_aliases", {})

    def _list_chats(self) -> list[dict]:
        """Fetch active chats sorted by last activity."""
        try:
            data = _wacli_call(["chats", "list", "--limit", str(self.chat_limit)])
        except Exception as e:
            self.log(f"Failed to list chats: {e}")
            return []

        if not data:
            return []

        # data is a list of {"JID": ..., "Kind": "dm|group", "Name": ..., "LastMessageTS": ...}
        return data if isinstance(data, list) else []

    def _list_messages(self, chat_jid: str, after: str | None = None) -> list[dict]:
        """Fetch messages from a chat, return normalized list of dicts."""
        cmd = ["messages", "list", "--chat", chat_jid, "--limit", str(self.fetch_limit)]
        if after:
            # wacli requires RFC3339 (with Z or +00:00) or YYYY-MM-DD
            ts = after
            if "T" in ts and not ts.endswith("Z") and "+" not in ts and "-" not in ts.split("T")[1]:
                ts += "Z"
            cmd.extend(["--after", ts])

        try:
            data = _wacli_call(cmd, timeout=30)
        except Exception as e:
            self.log(f"Failed to list messages for {chat_jid}: {e}")
            return []

        if not data:
            return []

        # data is {"messages": [...]} or {"fts": true, "messages": [...]}
        raw_messages = (data.get("messages") or []) if isinstance(data, dict) else []

        messages = []
        for m in raw_messages:
            has_media = bool(m.get("MediaType", ""))
            messages.append({
                "timestamp": m.get("Timestamp", ""),
                "sender": m.get("SenderJID", "Unknown"),
                "text": m.get("Text", ""),
                "is_from_me": m.get("FromMe", False),
                "has_media": has_media,
            })

        return messages

    def _group_into_chunks(self, messages: list[dict], chat_jid: str,
                           chat_name: str, chat_type: str) -> list[dict]:
        """Group messages into conversation chunks by time window."""
        if not messages:
            return []

        # Sort by timestamp
        messages.sort(key=lambda x: x.get("timestamp", ""))

        chunks = []
        window_sec = get_conversation_settings()["time_window_hours"] * 3600
        min_msgs = get_conversation_settings()["min_messages_per_chunk"]
        max_msgs = get_conversation_settings()["max_messages_per_chunk"]

        current_chunk = []
        chunk_start_ts = None

        for msg in messages:
            ts_str = msg.get("timestamp", "")
            if not ts_str:
                continue

            # Parse timestamp
            try:
                if "T" in ts_str:
                    msg_dt = datetime.fromisoformat(ts_str)
                else:
                    msg_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            if chunk_start_ts is None:
                chunk_start_ts = msg_dt
                current_chunk = [msg]
            elif (msg_dt - chunk_start_ts).total_seconds() <= window_sec and len(current_chunk) < max_msgs:
                current_chunk.append(msg)
            else:
                if len(current_chunk) >= min(min_msgs, 1):
                    chunks.append(self._chunk_to_record(
                        current_chunk, chat_jid, chat_name, chat_type
                    ))
                chunk_start_ts = msg_dt
                current_chunk = [msg]

        # Flush remaining
        if len(current_chunk) >= min(min_msgs, 1):
            chunks.append(self._chunk_to_record(
                current_chunk, chat_jid, chat_name, chat_type
            ))

        return chunks

    def _chunk_to_record(self, chunk: list[dict], chat_jid: str,
                         chat_name: str, chat_type: str) -> dict:
        """Convert message chunk to conversation record."""
        first_ts = chunk[0].get("timestamp", "")
        last_ts = chunk[-1].get("timestamp", "")

        start_str = first_ts.replace(" ", "T") if first_ts else ""
        end_str = last_ts.replace(" ", "T") if last_ts else ""

        messages = []
        for msg in chunk:
            ts = msg.get("timestamp", "")
            if ts and "T" not in ts:
                ts = ts.replace(" ", "T")

            text = msg.get("text", "")
            if text and len(text) > 5000:
                text = text[:5000] + "... [truncated]"

            messages.append({
                "ts": ts,
                "sender": msg.get("sender", "Unknown"),
                "text": text,
            })

        jid_short = chat_jid.split("@")[0][:20]
        record_id = f"wa_{jid_short}_{start_str}_{end_str}"

        return {
            "id": record_id,
            "type": "conversation",
            "chat": chat_name,
            "period_start": start_str,
            "period_end": end_str,
            "messages": messages,
            "meta": {
                "chat_jid": chat_jid,
                "chat_type": chat_type,
                "message_count": len(messages),
            },
        }

    def _resolve_jid(self, jid: str) -> str:
        """Resolve a JID through aliases. Returns canonical JID."""
        return self.jid_aliases.get(jid, jid)

    def _is_alias_target(self, jid: str) -> bool:
        """Check if this JID is a target of an alias (will be merged from LID chat)."""
        return jid in self.jid_aliases.values()

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new WhatsApp messages and group into conversations.

        Handles JID aliases: when a contact has both a LID and phone JID,
        messages from both are merged under the canonical (phone) JID.
        """
        self.log("Fetching chat list...")
        chats = self._list_chats()

        if not chats:
            self.log("No chats found")
            return

        self.log(f"Found {len(chats)} chats")

        after = state.last_ts
        if not after:
            bootstrap_dt = datetime.now(timezone.utc) - timedelta(days=7)
            after = bootstrap_dt.isoformat()
            self.log(f"Bootstrap sync: fetching since {after}")

        # Group chats by canonical JID to merge aliased conversations
        # Key: canonical JID, Value: list of (chat_jid, chat_name, chat_type)
        canonical_chats: dict[str, list[tuple[str, str, str]]] = {}
        for chat in chats:
            chat_jid = chat.get("JID", "")
            chat_name = chat.get("Name", "")

            if not chat_jid or chat_jid == "status@broadcast":
                continue

            chat_kind = chat.get("Kind", "")
            chat_type = "group" if chat_kind == "group" or "@g.us" in chat_jid else "dm"

            canonical = self._resolve_jid(chat_jid)
            if canonical not in canonical_chats:
                canonical_chats[canonical] = []
            canonical_chats[canonical].append((chat_jid, chat_name, chat_type))

        if self.jid_aliases:
            merged = sum(1 for v in canonical_chats.values() if len(v) > 1)
            if merged:
                self.log(f"Merged {merged} split conversations via JID aliases")

        yielded = 0
        for canonical_jid, chat_sources in canonical_chats.items():
            if yielded >= limit:
                break

            # Collect messages from all JIDs that map to this canonical JID
            all_messages = []
            # Use the name from the canonical (phone) JID if available, else first
            canonical_name = chat_sources[0][1]
            canonical_type = chat_sources[0][2]
            for src_jid, src_name, src_type in chat_sources:
                if src_jid == canonical_jid:
                    canonical_name = src_name
                    canonical_type = src_type
                messages = self._list_messages(src_jid, after=after)
                if messages:
                    self.log(f"Got {len(messages)} messages from '{src_name}' ({src_jid})")
                    all_messages.extend(messages)

            if not all_messages:
                continue

            chunks = self._group_into_chunks(
                all_messages, canonical_jid, canonical_name, canonical_type
            )
            for chunk in chunks:
                if yielded >= limit:
                    break
                yield chunk
                yielded += 1
