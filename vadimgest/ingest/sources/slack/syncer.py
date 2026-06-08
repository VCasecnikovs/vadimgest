"""Slack Syncer - sync messages via Slack Web API."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Iterator

from ..base import CronSyncer
from ....config import get_source_config
from ....models import SourceState
from ....store import DataStore

SLACK_API_BASE = "https://slack.com/api/"


class SlackSyncer(CronSyncer):
    """Slack syncer using the official Conversations API."""

    source_name = "slack"
    display_name = "Slack"
    description = "Messages from Slack channels, private channels, DMs, and group DMs"
    category = "messaging"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": ["SLACK_TOKEN"],
        "os": [],
    }
    config_schema = {
        "token": {
            "type": "str",
            "default": "",
            "description": "Slack OAuth token. Prefer SLACK_TOKEN in the environment",
            "placeholder": "${SLACK_TOKEN}",
        },
        "workspace": {
            "type": "str",
            "default": "",
            "description": "Optional workspace label for records",
            "placeholder": "acme",
        },
        "channels": {
            "type": "list",
            "default": [],
            "description": "Optional channel names or IDs to sync. Empty means all accessible conversations",
            "placeholder": "general",
        },
        "types": {
            "type": "str",
            "default": "public_channel,private_channel,im,mpim",
            "description": "Slack conversation types to list",
            "placeholder": "public_channel,private_channel,im,mpim",
        },
        "bootstrap_days": {
            "type": "int",
            "default": 7,
            "description": "Lookback window for first sync",
            "min": 1,
            "max": 3650,
        },
        "page_size": {
            "type": "int",
            "default": 100,
            "description": "Messages per conversations.history request",
            "min": 1,
            "max": 999,
        },
        "max_channels": {
            "type": "int",
            "default": 200,
            "description": "Maximum conversations to scan per sync",
            "min": 1,
        },
        "include_threads": {
            "type": "bool",
            "default": False,
            "description": "Also fetch thread replies with conversations.replies",
        },
    }
    credential_help = {
        "SLACK_TOKEN": "Slack OAuth token with conversations read/history scopes",
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("slack")
        super().__init__(store, config)

        self.token = os.environ.get("SLACK_TOKEN") or config.get("token", "")
        self.workspace = config.get("workspace", "")
        self.channel_filter = {str(c).lstrip("#") for c in config.get("channels", []) if str(c).strip()}
        self.types = config.get("types", "public_channel,private_channel,im,mpim")
        self.bootstrap_days = int(config.get("bootstrap_days", 7))
        self.page_size = int(config.get("page_size", 100))
        self.max_channels = int(config.get("max_channels", 200))
        self.include_threads = bool(config.get("include_threads", False))
        self._users: dict[str, str] = {}

    @classmethod
    def check_ready(cls) -> dict:
        cfg = get_source_config("slack")
        token = os.environ.get("SLACK_TOKEN") or cfg.get("token", "")
        if not token:
            return {"ok": False, "missing": ["SLACK_TOKEN or slack.token not configured"]}
        return {"ok": True}

    def _api(self, method: str, params: dict | None = None) -> dict:
        if not self.token:
            raise RuntimeError("Slack token not configured")

        params = params or {}
        url = SLACK_API_BASE + method
        if params:
            url += "?" + urllib.parse.urlencode(params)

        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        for attempt in range(2):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt == 0:
                    delay = int(e.headers.get("Retry-After", "60"))
                    self.log(f"Rate limited by Slack; sleeping {delay}s")
                    time.sleep(delay)
                    continue
                body = e.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Slack API HTTP {e.code}: {body}") from e
        else:
            raise RuntimeError("Slack API request failed")

        if not data.get("ok"):
            raise RuntimeError(f"Slack API {method} failed: {data.get('error', 'unknown_error')}")
        return data

    def _list_conversations(self) -> list[dict]:
        channels: list[dict] = []
        cursor = ""

        while len(channels) < self.max_channels:
            params = {
                "types": self.types,
                "exclude_archived": "true",
                "limit": min(200, max(1, self.max_channels - len(channels))),
            }
            if cursor:
                params["cursor"] = cursor

            data = self._api("conversations.list", params)
            for ch in data.get("channels", []):
                cid = ch.get("id", "")
                name = ch.get("name") or cid
                if self.channel_filter and cid not in self.channel_filter and name not in self.channel_filter:
                    continue
                channels.append(ch)
                if len(channels) >= self.max_channels:
                    break

            cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break

        return channels

    def _user_name(self, user_id: str) -> str:
        if not user_id:
            return ""
        if user_id in self._users:
            return self._users[user_id]
        try:
            data = self._api("users.info", {"user": user_id})
            profile = data.get("user", {}).get("profile", {})
            name = (
                profile.get("real_name")
                or profile.get("display_name")
                or data.get("user", {}).get("name")
                or user_id
            )
        except Exception:
            name = user_id
        self._users[user_id] = name
        return name

    @staticmethod
    def _iso_from_slack_ts(ts: str) -> str:
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        except Exception:
            return ""

    @staticmethod
    def _record_id(team: str, channel_id: str, ts: str) -> str:
        safe_ts = ts.replace(".", "_")
        team_part = team or "workspace"
        return f"slack_{team_part}_{channel_id}_{safe_ts}"

    @staticmethod
    def _channel_type(channel: dict) -> str:
        if channel.get("is_channel"):
            return "channel"
        if channel.get("is_group"):
            return "group"
        if channel.get("is_im"):
            return "im"
        if channel.get("is_mpim"):
            return "mpim"
        return "conversation"

    def _message_to_record(self, msg: dict, channel: dict) -> dict | None:
        ts = msg.get("ts", "")
        if not ts:
            return None

        channel_id = channel.get("id", "")
        channel_name = channel.get("name") or channel_id
        team = channel.get("context_team_id") or channel.get("team") or self.workspace
        user_id = msg.get("user") or msg.get("bot_id") or ""
        sender = self._user_name(user_id) if msg.get("user") else (msg.get("username") or user_id)
        subtype = msg.get("subtype", "")
        text = msg.get("text", "")

        files = [
            {
                "id": f.get("id"),
                "name": f.get("name"),
                "mimetype": f.get("mimetype"),
                "url_private": f.get("url_private"),
            }
            for f in msg.get("files", [])
            if isinstance(f, dict)
        ]

        return {
            "id": self._record_id(team, channel_id, ts),
            "type": "slack_message",
            "workspace": self.workspace or team,
            "team_id": team,
            "channel": channel_name,
            "channel_id": channel_id,
            "channel_type": self._channel_type(channel),
            "sender": sender,
            "user_id": user_id,
            "text": text,
            "timestamp": self._iso_from_slack_ts(ts),
            "ts": ts,
            "thread_ts": msg.get("thread_ts", ts),
            "is_thread_reply": msg.get("thread_ts") not in (None, ts),
            "subtype": subtype,
            "reactions": msg.get("reactions", []),
            "files": files,
            "meta": {
                "reply_count": msg.get("reply_count", 0),
                "permalink": msg.get("permalink"),
            },
        }

    def _history(self, channel_id: str, oldest: str, limit: int) -> list[dict]:
        records: list[dict] = []
        cursor = ""

        while len(records) < limit:
            params = {
                "channel": channel_id,
                "oldest": oldest,
                "limit": min(self.page_size, limit - len(records)),
            }
            if cursor:
                params["cursor"] = cursor

            data = self._api("conversations.history", params)
            messages = data.get("messages", [])
            records.extend(messages)

            cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not cursor or not messages:
                break

        return records

    def _thread_replies(self, channel_id: str, thread_ts: str) -> list[dict]:
        data = self._api("conversations.replies", {"channel": channel_id, "ts": thread_ts, "limit": self.page_size})
        return data.get("messages", [])[1:]

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        if not self.token:
            self.log("Slack token not configured")
            return

        if state.last_ts:
            try:
                oldest = str(datetime.fromisoformat(state.last_ts.replace("Z", "+00:00")).timestamp())
            except ValueError:
                oldest = str(float(state.last_ts))
        else:
            oldest = str(time.time() - self.bootstrap_days * 86400)

        channels = self._list_conversations()
        self.log(f"Fetching Slack history from {len(channels)} conversations")

        remaining = limit
        for channel in channels:
            if remaining <= 0:
                break

            channel_id = channel.get("id")
            if not channel_id:
                continue

            try:
                messages = self._history(channel_id, oldest, remaining)
            except RuntimeError as e:
                self.log(f"History failed for {channel.get('name') or channel_id}: {e}")
                continue

            for msg in reversed(messages):
                record = self._message_to_record(msg, channel)
                if record:
                    yield record
                    remaining -= 1

                if self.include_threads and msg.get("reply_count", 0) and remaining > 0:
                    try:
                        replies = self._thread_replies(channel_id, msg["ts"])
                    except RuntimeError as e:
                        self.log(f"Thread fetch failed for {channel.get('name') or channel_id}: {e}")
                        replies = []
                    for reply in replies:
                        record = self._message_to_record(reply, channel)
                        if record:
                            yield record
                            remaining -= 1
                            if remaining <= 0:
                                break

                if remaining <= 0:
                    break
