"""Google Calendar Syncer - sync calendar events via gog CLI."""

from datetime import datetime, timedelta
from typing import Iterator

from ..base import CronSyncer
from ..gog_utils import gog_call
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class CalendarSyncer(CronSyncer):
    """Google Calendar syncer via gog CLI."""

    source_name = "calendar"
    display_name = "Google Calendar"
    description = "Calendar events from Google Calendar"
    category = "calendar"
    dependencies = {
        "python": [],
        "cli": ["gog"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "email": {"type": "str", "default": "", "description": "Primary Google account email for calendar sync", "placeholder": "user@gmail.com"},
        "calendar_ids": {"type": "list", "default": [], "description": "Specific calendar IDs to sync (empty = all calendars)", "placeholder": "primary"},
        "days_back": {"type": "int", "default": 7, "description": "How many days in the past to fetch events", "min": 1, "max": 365, "placeholder": "7"},
        "days_forward": {"type": "int", "default": 14, "description": "How many days in the future to fetch events", "min": 1, "max": 365, "placeholder": "14"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("calendar")
        super().__init__(store, config)

        # Support both single `email` and multi-account `accounts` list
        email = config.get("email", "")
        self.accounts = config.get("accounts", [email] if email else [])
        self.days_back = config.get("days_back", 7)
        self.days_forward = config.get("days_forward", 14)
        self.calendar_ids = config.get("calendar_ids", [])  # empty = all calendars

    def _list_calendars(self, account: str) -> list[dict]:
        """Fetch available calendars for a given account."""
        try:
            result = gog_call("calendar", "calendars", account=account)
        except Exception as e:
            self.log(f"Failed to list calendars for {account}: {e}")
            return []

        return result.get("calendars", [])

    def _get_events(self, calendar_id: str, time_min: str, time_max: str, account: str) -> list[dict]:
        """Fetch events from a calendar within a time range."""
        try:
            result = gog_call(
                "calendar", "events",
                [calendar_id, "--from", time_min, "--to", time_max, "--all-pages"],
                account=account,
            )
        except Exception as e:
            self.log(f"Failed to get events from {calendar_id} ({account}): {e}")
            return []

        return result.get("events", [])

    def _parse_event_datetime(self, event: dict, field: str) -> str:
        """Extract datetime string from event, handling both structured and flat formats.

        Google Calendar API returns start/end as objects: {"dateTime": "...", "date": "..."}
        """
        val = event.get(field, "")

        # Structured format (dict with dateTime or date)
        if isinstance(val, dict):
            return val.get("dateTime", val.get("date", ""))

        # Flat string
        return str(val)

    def _event_to_record(self, event: dict, calendar_id: str, calendar_name: str) -> dict | None:
        """Convert an event dict to a vadimgest record."""
        event_id = event.get("id", "")
        if not event_id:
            # Generate ID from title + start time
            title = event.get("summary", "")
            start = self._parse_event_datetime(event, "start")
            if title and start:
                event_id = f"{title}_{start}"[:80]
            else:
                return None

        title = event.get("summary", "(no title)")
        start = self._parse_event_datetime(event, "start")
        end = self._parse_event_datetime(event, "end")
        location = event.get("location", "")
        description = event.get("description", "")
        status = event.get("status", "")
        html_link = event.get("htmlLink", event.get("html_link", ""))
        organizer = event.get("organizer", "")

        # Handle attendees - can be list of strings or list of dicts
        raw_attendees = event.get("attendees", [])
        attendees = []
        if isinstance(raw_attendees, list):
            for att in raw_attendees:
                if isinstance(att, str):
                    attendees.append(att)
                elif isinstance(att, dict):
                    attendees.append(att.get("email", att.get("displayName", str(att))))
                else:
                    attendees.append(str(att))
        elif isinstance(raw_attendees, str):
            attendees = [a.strip() for a in raw_attendees.split(",") if a.strip()]

        # Handle organizer
        if isinstance(organizer, dict):
            organizer = organizer.get("email", organizer.get("displayName", ""))

        # Truncate description
        if description and len(description) > 3000:
            description = description[:3000] + "... [truncated]"

        # Clean calendar ID for record ID
        cal_short = calendar_id.split("@")[0][:20]
        record_id = f"cal_{cal_short}_{event_id}"

        return {
            "id": record_id,
            "type": "calendar_event",
            "title": title,
            "start": start,
            "end": end,
            "location": location,
            "description": description,
            "attendees": attendees,
            "calendar_name": calendar_name,
            "calendar_id": calendar_id,
            "status": status,
            "url": html_link,
            "organizer": organizer,
            "date": start,  # for state.last_ts tracking
            "meta": {
                "event_id": event_id,
                "calendar_id": calendar_id,
            },
        }

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch calendar events from configured time range across all accounts."""
        if not self.accounts:
            self.log("No accounts configured for calendar source")
            return

        # Calculate time range
        now = datetime.now()
        time_min = (now - timedelta(days=self.days_back)).isoformat() + "Z"
        time_max = (now + timedelta(days=self.days_forward)).isoformat() + "Z"

        self.log(f"Fetching events from {self.days_back}d ago to {self.days_forward}d ahead across {len(self.accounts)} account(s)...")

        yielded = 0
        seen_event_ids: set[str] = set()  # dedup across accounts

        for account in self.accounts:
            if yielded >= limit:
                break

            # Get calendar list for this account
            calendars = self._list_calendars(account)
            if not calendars:
                self.log(f"No calendars for {account}, skipping")
                continue

            # Filter calendars if specific IDs configured
            if self.calendar_ids:
                calendars = [c for c in calendars
                             if c.get("id") in self.calendar_ids
                             or c.get("summary") in self.calendar_ids]

            self.log(f"Account {account}: {len(calendars)} calendar(s)")

            for cal in calendars:
                if yielded >= limit:
                    break

                cal_id = cal.get("id", "")
                if not cal_id:
                    continue
                cal_name = cal.get("summary", cal.get("name", cal_id))

                self.log(f"Fetching events from '{cal_name}' ({account})...")
                events = self._get_events(cal_id, time_min, time_max, account)
                self.log(f"Got {len(events)} events from '{cal_name}'")

                for event in events:
                    if yielded >= limit:
                        break
                    # Dedup: same event can appear in multiple accounts
                    event_id = event.get("id", "")
                    if event_id and event_id in seen_event_ids:
                        continue
                    if event_id:
                        seen_event_ids.add(event_id)

                    record = self._event_to_record(event, cal_id, cal_name)
                    if record:
                        yield record
                        yielded += 1
