"""X/Twitter Home Feed Syncer - fetch personalized For You timeline via bird CLI.

Runs every 2 hours to capture tweets from your feed.
"""

import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


def _bird_call(command: list[str], timeout: int = 60) -> list[dict]:
    """Call bird CLI with --json, return parsed list.

    Uses tempfile for stdout to avoid 64KB pipe buffer truncation.
    """
    cmd = ["bird"] + command + ["--json"]
    env = {**os.environ, "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"}

    with tempfile.TemporaryFile() as tmp:
        result = subprocess.run(
            cmd, stdout=tmp, stderr=subprocess.PIPE, timeout=timeout,
            env=env,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            lines = [l for l in stderr.split("\n") if "Safari" not in l and "EPERM" not in l]
            err = "\n".join(lines).strip()
            if err:
                raise RuntimeError(f"bird {' '.join(command)} failed: {err}")

        tmp.seek(0)
        raw = tmp.read()

    if not raw.strip():
        return []

    text = raw.decode("utf-8", errors="replace")
    parsed = json.loads(text)
    return parsed if isinstance(parsed, list) else []


class XNewsSyncer(CronSyncer):
    """X/Twitter home feed syncer."""

    source_name = "xnews"
    display_name = "X/Twitter Feed"
    description = "Personal For You timeline from X/Twitter via bird CLI"
    category = "social"
    dependencies = {
        "python": [],
        "cli": ["bird"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "count": {"type": "int", "default": 30, "description": "Number of tweets to fetch per sync", "min": 1, "max": 200, "placeholder": "30"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("xnews")
        super().__init__(store, config)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch home timeline tweets."""
        count = self.config.get("count", 30)

        try:
            tweets = _bird_call(["home", "-n", str(count)])
        except Exception as e:
            self.log(f"Failed to fetch home timeline: {e}")
            return

        if not tweets:
            self.log("No tweets in home timeline")
            return

        for tweet in tweets:
            tweet_id = tweet.get("id", "")
            if not tweet_id:
                continue

            record_id = f"xtweet_{tweet_id}"

            author = tweet.get("author", {})
            media = tweet.get("media", [])

            yield {
                "id": record_id,
                "type": "tweet",
                "ts": tweet.get("createdAt", datetime.now(timezone.utc).isoformat()),
                "tweet_id": tweet_id,
                "text": tweet.get("text", ""),
                "author": author.get("username", ""),
                "author_name": author.get("name", ""),
                "likes": tweet.get("likeCount", 0),
                "retweets": tweet.get("retweetCount", 0),
                "replies": tweet.get("replyCount", 0),
                "has_media": len(media) > 0,
                "media_types": [m.get("type", "") for m in media],
                "conversation_id": tweet.get("conversationId", ""),
                "is_reply": tweet.get("conversationId", "") != tweet_id,
            }
