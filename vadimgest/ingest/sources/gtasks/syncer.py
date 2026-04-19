"""Google Tasks Syncer - sync tasks from all lists via gog CLI."""

from datetime import datetime
from typing import Iterator

from ..base import CronSyncer
from ..gog_utils import gog_call
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class GTasksSyncer(CronSyncer):
    """Google Tasks syncer via gog CLI."""

    source_name = "gtasks"
    display_name = "Google Tasks"
    description = "Tasks from Google Tasks lists"
    category = "calendar"
    dependencies = {
        "python": [],
        "cli": ["gog"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "email": {"type": "str", "default": "", "description": "Google account email"},
        "max_tasks": {"type": "int", "default": 100, "description": "Max tasks to fetch"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("gtasks")
        super().__init__(store, config)

        self.email = config.get("email", "")
        self.max_tasks = config.get("max_tasks", 100)

    def _get_task_lists(self) -> list[dict]:
        """Fetch all task lists."""
        try:
            result = gog_call("tasks", "lists list", account=self.email)
        except Exception as e:
            self.log(f"Failed to get task lists: {e}")
            return []

        return result.get("tasklists", [])

    def _get_tasks(self, list_id: str) -> list[dict]:
        """Fetch open tasks from a list."""
        try:
            result = gog_call("tasks", "list", [list_id], account=self.email)
        except Exception as e:
            self.log(f"Failed to get tasks from {list_id}: {e}")
            return []

        return result.get("tasks", [])

    def _task_to_record(self, task: dict, list_id: str, list_name: str) -> dict | None:
        """Convert a task dict to a vadimgest record."""
        task_id = task.get("id", "")
        if not task_id:
            return None

        title = task.get("title", "(untitled)")
        notes = task.get("notes", "")
        status = task.get("status", "needsAction")
        due = task.get("due", "")
        updated = task.get("updated", task.get("updatedAt", ""))

        record_id = f"gtask_{list_id}_{task_id}"

        return {
            "id": record_id,
            "type": "task",
            "list_name": list_name,
            "title": title,
            "notes": notes,
            "status": status,
            "due": due,
            "updated_at": updated,
            "meta": {
                "task_id": task_id,
                "list_id": list_id,
            },
        }

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch open tasks from all Google Tasks lists."""
        self.log("Fetching task lists...")
        task_lists = self._get_task_lists()

        if not task_lists:
            self.log("No task lists found")
            return

        self.log(f"Found {len(task_lists)} task lists")

        yielded = 0
        for tl in task_lists:
            if yielded >= limit:
                break

            list_id = tl.get("id", "")
            list_name = tl.get("title", "Unknown")

            if not list_id:
                continue

            self.log(f"Fetching tasks from '{list_name}'...")
            tasks = self._get_tasks(list_id)
            self.log(f"Got {len(tasks)} open tasks from '{list_name}'")

            for task in tasks:
                if yielded >= limit:
                    break
                record = self._task_to_record(task, list_id, list_name)
                if record:
                    yield record
                    yielded += 1
