"""Gmail Syncer - sync emails from multiple accounts via gog CLI.

Supports both incoming email sync and outgoing email follow-up tracking.
Sent emails are stored with `direction: "sent"` and `awaiting_reply: true/false`.
"""

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Iterator

from ..base import CronSyncer
from ..gog_utils import gog_call
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config

# Default threshold for considering a sent email as needing follow-up
FOLLOW_UP_HOURS = 48


class GmailSyncer(CronSyncer):
    """Gmail syncer for multiple accounts via gog CLI.

    Syncs both incoming and sent emails. Sent emails are cross-referenced
    with thread activity to determine `awaiting_reply` status.
    """

    source_name = "gmail"
    display_name = "Gmail"
    description = "Emails from multiple Gmail accounts"
    category = "email"
    dependencies = {
        "python": [],
        "cli": ["gog"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "accounts": {"type": "list", "default": [], "description": "Gmail accounts to sync (one email per line)", "placeholder": "user@gmail.com"},
        "query": {"type": "str", "default": "newer_than:1d -in:spam -in:trash", "description": "Gmail search query for incoming mail. Uses Gmail search syntax", "placeholder": "newer_than:1d -in:spam -in:trash"},
        "bootstrap_query": {"type": "str", "default": "newer_than:7d -in:spam -in:trash", "description": "Query for first-time sync (usually wider time range)", "placeholder": "newer_than:7d -in:spam -in:trash"},
        "page_size": {"type": "int", "default": 25, "description": "Number of threads per API page request", "min": 1, "max": 500, "placeholder": "25"},
        "batch_size": {"type": "int", "default": 25, "description": "Number of messages to fetch details for in parallel", "min": 1, "max": 100, "placeholder": "25"},
        "follow_up_hours": {"type": "int", "default": 48, "description": "Hours after sending before an email is flagged as awaiting reply", "min": 1, "placeholder": "48"},
        "sent_query": {"type": "str", "default": "in:sent newer_than:7d", "description": "Gmail search query for sent mail sync", "placeholder": "in:sent newer_than:7d"},
        "sent_bootstrap_query": {"type": "str", "default": "in:sent newer_than:14d", "description": "Sent mail query for first-time sync", "placeholder": "in:sent newer_than:14d"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("gmail")
        super().__init__(store, config)

        self.accounts = config.get("accounts", [])
        self.query = config.get("query", "newer_than:1d -in:spam -in:trash")
        self.bootstrap_query = config.get("bootstrap_query", "newer_than:7d -in:spam -in:trash")
        self.sent_query = config.get("sent_query", "in:sent newer_than:7d")
        self.sent_bootstrap_query = config.get("sent_bootstrap_query", "in:sent newer_than:14d")
        self.follow_up_hours = config.get("follow_up_hours", FOLLOW_UP_HOURS)
        self.page_size = config.get("page_size", 25)
        self.batch_size = config.get("batch_size", 25)

    def _search_threads(self, account: str, query: str) -> list[dict]:
        """Search for threads in an account.

        Returns list of thread dicts with keys:
        id, date, from, subject, labels, messageCount
        """
        try:
            result = gog_call("gmail", "search", [query], account=account)
        except Exception as e:
            self.log(f"Search failed for {account}: {e}")
            return []

        if isinstance(result, dict):
            return result.get("threads", [])
        if isinstance(result, list):
            return result
        return []

    def _get_message(self, account: str, message_id: str) -> dict:
        """Fetch a single message with body.

        Returns dict with keys: body, headers, attachments, message
        """
        try:
            result = gog_call("gmail", "get", [message_id], account=account)
        except Exception as e:
            self.log(f"Message fetch failed for {account}/{message_id}: {e}")
            return {}

        return result if isinstance(result, dict) else {}

    def _get_thread_messages(self, account: str, thread_id: str) -> list[dict]:
        """Fetch all messages in a thread (raw Gmail API format).

        gog returns: {"thread": {"messages": [...]}, "downloaded": ...}
        Each message has payload.headers for From/Subject/Date.
        """
        if not thread_id:
            return []
        try:
            result = gog_call("gmail", "thread get", [thread_id], account=account)
        except Exception as e:
            self.log(f"Thread fetch failed for {account}/{thread_id}: {e}")
            return []

        if isinstance(result, dict):
            thread = result.get("thread", result)
            raw_msgs = thread.get("messages", [])
            # Normalize raw API messages into flat dicts
            normalized = []
            for raw in raw_msgs:
                msg = {"id": raw.get("id", "")}
                headers = {}
                payload = raw.get("payload", {})
                for h in payload.get("headers", []):
                    headers[h.get("name", "").lower()] = h.get("value", "")
                msg["from"] = headers.get("from", "")
                msg["to"] = headers.get("to", "")
                msg["subject"] = headers.get("subject", "")
                msg["date"] = headers.get("date", "")
                msg["labels"] = raw.get("labelIds", [])
                normalized.append(msg)
            return normalized

        return []

    def _msg_to_record(self, msg: dict, account: str,
                       direction: str = "received",
                       awaiting_reply: bool | None = None) -> dict | None:
        """Convert a message dict to a vadimgest record.

        Args:
            msg: Raw message dict from gog CLI.
            account: Email account this message belongs to.
            direction: "received" or "sent".
            awaiting_reply: Whether this sent email is awaiting a reply.
                            None means not applicable (for received emails).
        """
        message_id = msg.get("message_id") or msg.get("id", "")
        if not message_id:
            return None

        # Clean account name for ID (remove @domain)
        account_short = account.split("@")[0]
        record_id = f"gmail_{account_short}_{message_id}"

        subject = msg.get("subject", "(no subject)")
        from_addr = msg.get("from", "")
        to_addr = msg.get("to", account)
        date = msg.get("date", "")
        body = msg.get("body", "")
        labels = msg.get("labels", [])
        thread_id = msg.get("thread_id", "")
        is_unread = "UNREAD" in labels if isinstance(labels, list) else False

        # Truncate body
        if body and len(body) > 5000:
            body = body[:5000] + "... [truncated]"

        record = {
            "id": record_id,
            "type": "email",
            "account": account,
            "subject": subject,
            "from": from_addr,
            "to": to_addr,
            "date": date,
            "body": body,
            "labels": labels,
            "thread_id": thread_id,
            "is_unread": is_unread,
            "direction": direction,
            "meta": {
                "message_id": message_id,
                "account": account,
            },
        }

        if awaiting_reply is not None:
            record["awaiting_reply"] = awaiting_reply

        return record

    def _is_account_address(self, email_addr: str, account: str) -> bool:
        """Check if an email address belongs to one of the configured accounts."""
        if not email_addr:
            return False
        # Extract just the email from "Name <email>" format
        match = re.search(r'<([^>]+)>', email_addr)
        addr = match.group(1).lower() if match else email_addr.strip().lower()
        # Check against the specific account and all configured accounts
        all_accounts = {a.lower() for a in self.accounts}
        return addr in all_accounts or addr == account.lower()

    def _check_awaiting_reply(self, account: str, sent_msg: dict) -> bool:
        """Check if a sent message is awaiting a reply.

        A sent message is "awaiting reply" when:
        1. It's the last message in the thread (no reply after it), AND
        2. The last message was sent BY us (not a reply we received)

        Returns True if the sent message appears to be awaiting a reply.
        """
        thread_id = sent_msg.get("thread_id", "")
        if not thread_id:
            # No thread ID - can't determine, assume awaiting
            return True

        thread_msgs = self._get_thread_messages(account, thread_id)
        if not thread_msgs:
            # Thread fetch failed - assume awaiting
            return True

        if len(thread_msgs) <= 1:
            # Only our sent message in thread - awaiting reply
            return True

        # Find the last message in the thread by date
        # The last message determines if we're awaiting a reply
        last_msg = thread_msgs[-1]  # Usually the API returns in order

        # Check if the last message is from us
        last_from = last_msg.get("from", "")
        return self._is_account_address(last_from, account)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new emails from all configured accounts.

        Uses search results directly - gog search returns thread metadata
        (from, subject, date, labels) which is sufficient for records.
        Only fetches full body when needed.
        """
        if not self.accounts:
            self.log("No email accounts configured")
            return

        # Use bootstrap query for first sync
        query = self.bootstrap_query if not state.last_ts else self.query

        yielded = 0
        for account in self.accounts:
            if yielded >= limit:
                break

            self.log(f"Searching {account}...")

            threads = self._search_threads(account, query)
            if not threads:
                self.log(f"No new messages in {account}")
                continue

            self.log(f"Found {len(threads)} threads in {account}")

            # Create records directly from search results (they have metadata)
            for thread in threads:
                if yielded >= limit:
                    break

                # Search results have: id, from, subject, date, labels, messageCount
                # Include messageCount in msg ID so new messages create new records
                thread_id = thread.get("id", "")
                msg_count = thread.get("messageCount", 1)
                msg = {
                    "message_id": f"{thread_id}_mc{msg_count}",
                    "id": thread_id,
                    "from": thread.get("from", ""),
                    "subject": thread.get("subject", ""),
                    "date": thread.get("date", ""),
                    "labels": thread.get("labels", []),
                    "thread_id": thread_id,
                    "body": "",  # Skip body fetch for speed
                }

                record = self._msg_to_record(msg, account, direction="received")
                if record:
                    yield record
                    yielded += 1

    def fetch_sent(self, state: SourceState, limit: int = 500) -> Iterator[dict]:
        """Fetch sent emails and determine awaiting_reply status.

        For sent tracking, we need to check threads to determine if a reply
        was received. This requires _get_thread_messages API calls.
        """
        if not self.accounts:
            self.log("No email accounts configured for sent tracking")
            return

        # Use bootstrap query for first sent sync
        query = self.sent_bootstrap_query if not state.last_ts else self.sent_query

        yielded = 0
        for account in self.accounts:
            if yielded >= limit:
                break

            self.log(f"Searching sent emails in {account}...")

            threads = self._search_threads(account, query)
            if not threads:
                self.log(f"No sent messages in {account}")
                continue

            self.log(f"Found {len(threads)} sent threads in {account}")

            for thread in threads:
                if yielded >= limit:
                    break

                thread_id = thread.get("id", "")
                if not thread_id:
                    continue

                # For sent emails, fetch full thread to check reply status
                msgs = self._get_thread_messages(account, thread_id)
                if not msgs:
                    # Fallback: use search result as-is, assume awaiting
                    msg = {
                        "id": thread_id,
                        "from": thread.get("from", ""),
                        "subject": thread.get("subject", ""),
                        "date": thread.get("date", ""),
                        "labels": thread.get("labels", []),
                        "thread_id": thread_id,
                        "body": "",
                    }
                    record = self._msg_to_record(msg, account, direction="sent", awaiting_reply=True)
                    if record:
                        yield record
                        yielded += 1
                    continue

                # Check if last message is from us (awaiting reply)
                last_from = msgs[-1].get("from", "")
                awaiting = self._is_account_address(last_from, account)

                # Yield only sent messages from this account
                for msg in msgs:
                    if yielded >= limit:
                        break
                    from_addr = msg.get("from", "")
                    if not self._is_account_address(from_addr, account):
                        continue

                    msg["thread_id"] = thread_id
                    record = self._msg_to_record(msg, account, direction="sent", awaiting_reply=awaiting)
                    if record:
                        yield record
                        yielded += 1

            self.log(f"Processed {yielded} sent messages from {account}")

    def sync(self, limit: int = 10000) -> tuple[int, list[str]]:
        """Sync both incoming and sent emails.

        Overrides BaseSyncer.sync() to also sync sent emails with
        follow-up tracking.
        """
        state = self.store.get_state(self.source_name)
        count = 0
        summaries = []

        # 1. Sync incoming (original behavior)
        for record in self.fetch_new(state, limit):
            record_id = record.get("id")
            if record_id and self.store.exists(self.source_name, record_id):
                continue
            self.store.append(self.source_name, record)
            count += 1
            if len(summaries) < 5:
                label = self._extract_label(record)
                if label and label not in summaries:
                    summaries.append(label)

        # 2. Sync sent emails with follow-up tracking
        sent_limit = min(limit, 500)  # Cap sent email sync
        for record in self.fetch_sent(state, sent_limit):
            record_id = record.get("id")
            if record_id and self.store.exists(self.source_name, record_id):
                continue
            self.store.append(self.source_name, record)
            count += 1
            if len(summaries) < 5:
                label = self._extract_label(record)
                if label and label not in summaries:
                    summaries.append(label)

        return count, summaries

    def get_follow_ups(self, hours: int | None = None) -> list[dict]:
        """Get sent emails that are awaiting reply for more than N hours.

        Scans the gmail.jsonl store for sent emails marked as awaiting_reply
        that are older than the threshold.

        Args:
            hours: Hours threshold. Defaults to self.follow_up_hours (48).

        Returns:
            List of record dicts for emails needing follow-up, sorted by date.
        """
        threshold_hours = hours if hours is not None else self.follow_up_hours
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=threshold_hours)
        follow_ups = []

        for record in self.store.read_all(self.source_name):
            data = record.data

            # Only sent emails that are awaiting reply
            if data.get("direction") != "sent":
                continue
            if not data.get("awaiting_reply", False):
                continue

            # Parse date and check if older than cutoff
            date_str = data.get("date", "")
            if not date_str:
                continue

            msg_date = self._parse_email_date(date_str)
            if msg_date and msg_date < cutoff:
                follow_ups.append({
                    **data,
                    "_parsed_date": msg_date.isoformat(),
                    "_age_hours": int((now - msg_date).total_seconds() / 3600),
                })

        # Sort by date (oldest first - most overdue)
        follow_ups.sort(key=lambda x: x.get("_parsed_date", ""))

        return follow_ups

    @staticmethod
    def _parse_email_date(date_str: str) -> datetime | None:
        """Parse various email date formats into a datetime.

        Handles formats like:
        - "Wed, 19 Feb 2026 14:30:00 +0000"
        - "2026-02-19T14:30:00Z"
        - "2026-02-19 14:30:00"
        - "Feb 19, 2026 2:30 PM"
        """
        if not date_str:
            return None

        # Try common formats
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",     # RFC 2822
            "%d %b %Y %H:%M:%S %z",          # without day name
            "%Y-%m-%dT%H:%M:%S%z",           # ISO 8601
            "%Y-%m-%dT%H:%M:%SZ",            # ISO 8601 UTC
            "%Y-%m-%d %H:%M:%S",             # simple datetime
            "%b %d, %Y %I:%M %p",            # "Feb 19, 2026 2:30 PM"
            "%a, %d %b %Y %H:%M:%S",         # RFC 2822 without tz
        ]

        # Clean up the date string - remove extra whitespace, handle "(PST)" etc
        clean = re.sub(r'\s*\([A-Z]+\)\s*$', '', date_str.strip())
        # Handle "+0000 (UTC)" -> "+0000"
        clean = re.sub(r'\s*\([^)]+\)$', '', clean)

        for fmt in formats:
            try:
                dt = datetime.strptime(clean, fmt)
                # If no timezone info, assume UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        return None

    def refresh_follow_up_status(self) -> tuple[int, int]:
        """Re-check awaiting_reply status for existing sent emails.

        Reads all sent emails from the store, re-checks their threads,
        and updates the store records if the status changed.

        Returns:
            Tuple of (total_checked, status_changed).
        """
        checked = 0
        changed = 0

        # Read all records and find sent emails that are awaiting reply
        records_to_check = []
        for record in self.store.read_all(self.source_name):
            data = record.data
            if data.get("direction") == "sent" and data.get("awaiting_reply", False):
                records_to_check.append(data)

        self.log(f"Checking {len(records_to_check)} sent emails for reply status...")

        for data in records_to_check:
            account = data.get("account", "")
            thread_id = data.get("thread_id", "")
            if not account or not thread_id:
                continue

            checked += 1
            still_awaiting = self._check_awaiting_reply(account, data)

            if not still_awaiting:
                # Reply was received - we need to update the record
                # Since JSONL is append-only, we store an update record
                update_record = {
                    "id": data["id"] + "_reply_received",
                    "type": "email_status_update",
                    "original_id": data["id"],
                    "thread_id": thread_id,
                    "account": account,
                    "subject": data.get("subject", ""),
                    "awaiting_reply": False,
                    "status_changed_at": datetime.now(timezone.utc).isoformat(),
                    "direction": "sent",
                    "meta": {
                        "update_type": "reply_received",
                        "original_message_id": data.get("meta", {}).get("message_id", ""),
                    },
                }
                if not self.store.exists(self.source_name, update_record["id"]):
                    self.store.append(self.source_name, update_record)
                    changed += 1

        self.log(f"Checked {checked} emails, {changed} received replies")
        return checked, changed
