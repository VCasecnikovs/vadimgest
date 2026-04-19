"""GitHub Notifications Syncer - sync notifications via gh CLI."""

import json
import subprocess
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class GitHubNotificationsSyncer(CronSyncer):
    """GitHub notifications syncer via gh CLI."""

    source_name = "github_notifications"
    display_name = "GitHub Notifications"
    description = "GitHub notification feed for participating threads"
    category = "dev"
    dependencies = {
        "python": [],
        "cli": ["gh"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "participating": {"type": "bool", "default": True, "description": "Only participating notifications"},
        "per_page": {"type": "int", "default": 50, "description": "Notifications per page"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("github_notifications")
        super().__init__(store, config)

        self.participating = config.get("participating", True)
        self.per_page = config.get("per_page", 50)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new GitHub notifications via gh api."""
        # Build query params
        query = f"per_page={min(self.per_page, limit)}"
        if state.last_ts:
            query += f"&since={state.last_ts}"
        if self.participating:
            query += "&participating=true"

        try:
            result = subprocess.run(
                ["gh", "api", f"/notifications?{query}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                self.log(f"gh api failed: {result.stderr.strip()}")
                return

            notifications = json.loads(result.stdout)
        except FileNotFoundError:
            self.log("gh CLI not found - install GitHub CLI")
            return
        except subprocess.TimeoutExpired:
            self.log("gh api timed out")
            return
        except json.JSONDecodeError as e:
            self.log(f"Failed to parse gh response: {e}")
            return

        if not isinstance(notifications, list):
            self.log(f"Unexpected response type: {type(notifications)}")
            return

        self.log(f"Fetched {len(notifications)} notifications")

        yielded = 0
        for notif in notifications:
            if yielded >= limit:
                break
            record = self._notif_to_record(notif)
            if record:
                yield record
                yielded += 1

        self.log(f"Yielded {yielded} notification records")

    def _notif_to_record(self, notif: dict) -> dict | None:
        """Convert gh api notification to vadimgest record."""
        notif_id = notif.get("id", "")
        if not notif_id:
            return None

        # Subject info
        subject = notif.get("subject", {})
        subject_title = subject.get("title", "")
        subject_type = subject.get("type", "")
        subject_url = subject.get("url", "")

        # Repo info
        repo = notif.get("repository", {})
        repo_name = repo.get("full_name", "")

        # Convert API URL to web URL
        url = subject_url
        if url and "api.github.com" in url:
            url = url.replace("api.github.com/repos/", "github.com/")
            url = url.replace("/pulls/", "/pull/")

        return {
            "id": f"ghn_{notif_id}",
            "type": "notification",
            "reason": notif.get("reason", ""),
            "subject": subject_title,
            "subject_type": subject_type,
            "repo": repo_name,
            "url": url,
            "is_unread": notif.get("unread", True),
            "updated_at": notif.get("updated_at", ""),
            "meta": {
                "notification_id": str(notif_id),
                "thread_id": str(notif_id),
            },
        }


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn

    DATA_DIR = DATA_DIR_fn()
    store = DataStore(DATA_DIR)
    syncer = GitHubNotificationsSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
