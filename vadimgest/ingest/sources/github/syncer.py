"""GitHub Projects Syncer - sync project items and commits via gh CLI."""

import json
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


# Team members to track commits for (empty = track all authors)
TRACKED_AUTHORS = []


def _gh_call(args: list[str], timeout: int = 30) -> Any:
    """Call gh CLI and return parsed JSON."""
    cmd = ["gh"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {result.stderr.strip()}")

    if not result.stdout.strip():
        return None

    return json.loads(result.stdout)


class GitHubSyncer(CronSyncer):
    """GitHub Projects v2 + commit syncer via gh CLI."""

    source_name = "github"
    display_name = "GitHub"
    description = "Project items and commits from GitHub repositories"
    category = "dev"
    dependencies = {
        "python": [],
        "cli": ["gh"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "projects": {"type": "list", "default": [], "description": "GitHub Projects to track in owner/number format", "placeholder": "myorg/1\nmyorg/2"},
        "repos": {"type": "list", "default": [], "description": "Repositories to track commits in owner/repo format", "placeholder": "owner/repo"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("github")
        super().__init__(store, config)

        self.projects = config.get("projects", [])
        self.repos = config.get("repos", [])

    def _fetch_project_items(self, owner: str, project_number: int) -> list[dict]:
        """Fetch all items from a project via gh project item-list."""
        try:
            result = _gh_call([
                "project", "item-list", str(project_number),
                "--owner", owner,
                "--format", "json",
                "--limit", "500",
            ], timeout=60)
        except Exception as e:
            self.log(f"Failed to fetch items from {owner}#{project_number}: {e}")
            return []

        if not isinstance(result, dict):
            return []

        return result.get("items", [])

    def _item_to_record(self, item: dict, owner: str, project_number: int) -> dict | None:
        """Convert a gh project item to a vadimgest record.

        gh project item-list --format json returns items like:
          {
            "id": "PVTI_...",
            "title": "Issue title",
            "assignees": ["user1"],
            "status": "In Progress",
            "priority": "high",
            "deadline": "2026-02-13",
            "story points": 2,
            "sprint": {"title": "Sprint 1", ...},
            "content": {
              "number": 292,
              "repository": "myorg/myrepo",
              "title": "...",
              "type": "Issue",
              "url": "https://github.com/..."
            }
          }
        """
        if not isinstance(item, dict):
            return None

        item_id = item.get("id", "")
        title = item.get("title", "")
        if not title:
            return None

        content = item.get("content", {}) or {}
        content_type = content.get("type", "Issue").lower()
        issue_number = content.get("number")

        assignees = item.get("assignees", []) or []
        status = item.get("status")
        priority = item.get("priority")
        due_date = item.get("deadline")

        record_id = f"ghp_{owner}_{project_number}_{item_id}"

        record = {
            "id": record_id,
            "type": "issue",
            "project": f"{owner}#{project_number}",
            "number": issue_number,
            "title": title,
            "assignees": assignees,
            "status": status,
            "priority": priority,
            "meta": {
                "project_owner": owner,
                "project_number": project_number,
                "item_type": content_type,
            },
        }

        if due_date:
            record["due_date"] = due_date

        return record

    # === Commit Tracking ===

    def _fetch_commits(self, owner: str, repo: str, since: str | None = None) -> list[dict]:
        """Fetch commits from a repo via gh api."""
        url = f"repos/{owner}/{repo}/commits?per_page=100"
        if since:
            url += f"&since={since}"

        try:
            result = _gh_call(["api", url, "--paginate"], timeout=60)
        except Exception as e:
            self.log(f"Failed to fetch commits from {owner}/{repo}: {e}")
            return []

        if not isinstance(result, list):
            return []

        # Safety cap
        return result[:500]

    def _commit_to_record(self, commit: dict, owner: str, repo: str) -> dict | None:
        """Convert a GitHub commit API response to a vadimgest record.

        gh api repos/.../commits returns:
          {
            "sha": "abc123...",
            "commit": {
              "message": "fix: something",
              "author": {"name": "...", "date": "2026-02-20T..."}
            },
            "author": {"login": "username", ...},
            "html_url": "https://github.com/..."
          }
        """
        if not isinstance(commit, dict):
            return None

        sha = commit.get("sha", "")
        if not sha:
            return None

        commit_data = commit.get("commit", {})
        message = commit_data.get("message", "")

        # Author: prefer top-level author.login, fall back to commit.author.name
        author_obj = commit.get("author") or {}
        author_login = author_obj.get("login", "") if isinstance(author_obj, dict) else ""
        commit_author = commit_data.get("author", {})
        author_name = commit_author.get("name", "") if isinstance(commit_author, dict) else ""
        author = author_login or author_name

        if TRACKED_AUTHORS and author not in TRACKED_AUTHORS:
            return None

        date = ""
        if isinstance(commit_author, dict):
            date = commit_author.get("date", "")

        html_url = commit.get("html_url", f"https://github.com/{owner}/{repo}/commit/{sha}")

        short_sha = sha[:7]
        record_id = f"ghc_{owner}_{repo}_{short_sha}"
        first_line = message.split("\n")[0][:200] if message else ""

        record = {
            "id": record_id,
            "type": "commit",
            "sha": sha,
            "short_sha": short_sha,
            "author": author,
            "message": first_line,
            "full_message": message if message != first_line else None,
            "date": date,
            "url": html_url,
            "repo": f"{owner}/{repo}",
            "meta": {
                "author_name": author_name,
                "author_login": author_login,
            },
        }

        return {k: v for k, v in record.items() if v is not None}

    def _get_commits_since(self, state: SourceState) -> str | None:
        """Determine the 'since' timestamp for commit fetching."""
        if state.last_ts:
            return state.last_ts

        since = datetime.now(timezone.utc) - timedelta(days=7)
        return since.strftime("%Y-%m-%dT%H:%M:%SZ")

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch project items and commits from all configured sources."""
        yielded = 0

        # === Part 1: Project items ===
        if self.projects:
            for proj in self.projects:
                if yielded >= limit:
                    break

                owner = proj.get("owner")
                project_number = proj.get("project_number")

                if not owner or not project_number:
                    self.log(f"Invalid project config: {proj}")
                    continue

                self.log(f"Fetching project {owner}/{project_number}...")
                items = self._fetch_project_items(owner, project_number)
                self.log(f"Got {len(items)} items from {owner}#{project_number}")

                for item in items:
                    if yielded >= limit:
                        break
                    record = self._item_to_record(item, owner, project_number)
                    if record:
                        yield record
                        yielded += 1
        else:
            self.log("No projects configured")

        # === Part 2: Commits ===
        if self.repos:
            since = self._get_commits_since(state)
            self.log(f"Fetching commits since {since}")

            for repo_cfg in self.repos:
                if yielded >= limit:
                    break

                owner = repo_cfg.get("owner")
                repo = repo_cfg.get("repo")

                if not owner or not repo:
                    self.log(f"Invalid repo config: {repo_cfg}")
                    continue

                self.log(f"Fetching commits from {owner}/{repo}...")
                commits = self._fetch_commits(owner, repo, since=since)
                self.log(f"Got {len(commits)} commits from {owner}/{repo}")

                for commit in commits:
                    if yielded >= limit:
                        break
                    record = self._commit_to_record(commit, owner, repo)
                    if record:
                        yield record
                        yielded += 1
        else:
            self.log("No repos configured for commit tracking")
