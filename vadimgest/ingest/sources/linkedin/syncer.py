"""LinkedIn Syncer - sync messages via Playwright browser context.

Uses persistent Playwright browser profile (~/.linkedin_browser/) to access
LinkedIn's GraphQL messaging API through in-browser fetch() calls.

Two-phase approach:
1. Fetch conversation list (sorted by last activity)
2. For each conversation, fetch full message history via separate endpoint

Record types:
    - linkedin_message: conversation message from messaging
"""

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Iterator

from playwright.sync_api import sync_playwright

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config

BROWSER_DATA_DIR = os.path.expanduser("~/.linkedin_browser")
VOYAGER_BASE = "/voyager/api"
MESSAGES_QUERY_ID = "messengerMessages.5846eeb71c981f11e0134cb6626cc314"


class LinkedInSyncer(CronSyncer):
    """LinkedIn syncer using Playwright persistent browser context."""

    source_name = "linkedin"
    display_name = "LinkedIn"
    description = "Messages from LinkedIn via persistent browser session"
    category = "social"
    dependencies = {
        "python": ["playwright"],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "max_conversations": {"type": "int", "default": 20, "description": "Max conversations to sync"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("linkedin")
        super().__init__(store, config)
        self.max_conversations = config.get("max_conversations", 50)

    def _get_self_name(self) -> str:
        """Get own display name from config (self_names[0]) or fallback."""
        from ....config import load_config
        cfg = load_config()
        names = cfg.get("self_names", [])
        return names[0] if names else "Me"

    def _make_record_id(self, sender: str, body: str, conv_id: str) -> str:
        """Generate a stable record ID."""
        content = f"{conv_id}:{sender}:{body}"
        content_hash = hashlib.md5(content.encode()).hexdigest()[:10]
        safe_name = sender.lower().replace(" ", "_")[:30]
        return f"li_msg_{safe_name}_{content_hash}"

    def _api_call(self, page, endpoint: str, params: dict | None = None) -> dict:
        """Make a Voyager API call via browser fetch with CSRF token."""
        url = f"{VOYAGER_BASE}{endpoint}"
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{qs}"

        result = page.evaluate(f"""async () => {{
            const c = document.cookie.split('; ').find(c => c.startsWith('JSESSIONID='));
            const csrf = c ? c.split('=').slice(1).join('=').replace(/"/g, '') : '';
            const r = await fetch('{url}', {{
                headers: {{
                    'accept': 'application/vnd.linkedin.normalized+json+2.1',
                    'csrf-token': csrf,
                }}
            }});
            return {{status: r.status, body: await r.text()}};
        }}""")

        if result["status"] >= 400:
            raise RuntimeError(f"API error {result['status']} for {endpoint}")

        return json.loads(result["body"])

    def _get_my_urn(self, page) -> str:
        """Get current user's fsd_profile URN."""
        me = self._api_call(page, "/me")
        for item in me.get("included", []):
            dash = item.get("dashEntityUrn", "")
            if "fsd_profile" in dash:
                return dash
        raise RuntimeError("Cannot determine profile URN")

    def _gql_participant_name(self, participant: dict) -> str:
        """Extract name from GraphQL messaging participant."""
        pt = participant.get("participantType", {})
        member = pt.get("member")
        org = pt.get("organization")
        if member:
            fn = member.get("firstName", {})
            ln = member.get("lastName", {})
            first = fn.get("text", "") if isinstance(fn, dict) else str(fn)
            last = ln.get("text", "") if isinstance(ln, dict) else str(ln)
            return f"{first} {last}".strip()
        if org:
            name = org.get("name", {})
            return name.get("text", "") if isinstance(name, dict) else str(name)
        return "Unknown"

    def _gql_msg_body(self, msg: dict) -> str:
        """Extract message body text from GraphQL message."""
        body = msg.get("body", {})
        if isinstance(body, dict):
            return body.get("text", "")
        return str(body)

    def _gql_msg_sender(self, msg: dict, my_urn: str = "") -> str:
        """Extract sender name from GraphQL message."""
        # actor has full participantType data, sender only has URN
        actor = msg.get("actor") or msg.get("sender") or {}

        # Compare actor URN to our own URN to correctly attribute self-sent messages.
        # Without this, LinkedIn sometimes returns the other participant as actor,
        # causing sender inversion (own messages attributed to the other person).
        if my_urn and actor:
            actor_urn = (actor.get("entityUrn") or actor.get("urn") or "")
            # URN format: urn:li:fsd_profile:ACoAADbbRFk... - compare by ID suffix
            my_id = my_urn.split(":")[-1] if ":" in my_urn else my_urn
            if my_id and my_id in str(actor_urn):
                return self._get_self_name()

        return self._gql_participant_name(actor)

    def _gql_fetch(self, page, query_id: str, variables: str) -> dict:
        """Make a GraphQL messaging API call via browser fetch."""
        result = page.evaluate(f"""async () => {{
            const c = document.cookie.split("; ").find(c => c.startsWith("JSESSIONID="));
            const csrf = c ? c.split("=").slice(1).join("=").replace(/"/g, "") : "";
            const url = "/voyager/api/voyagerMessagingGraphQL/graphql?queryId={query_id}&variables={variables}";
            const r = await fetch(url, {{ headers: {{ "accept": "application/graphql", "csrf-token": csrf }} }});
            return {{status: r.status, body: await r.text()}};
        }}""")
        if result["status"] >= 400:
            raise RuntimeError(f"GraphQL error {result['status']}")
        return json.loads(result["body"])

    def _parse_conversation_meta(self, conv: dict) -> tuple[str, str, list[str], bool]:
        """Extract (conv_urn, conv_id, participant_names, unread) from conversation."""
        participants = conv.get("conversationParticipants", [])
        participant_names = [
            self._gql_participant_name(p)
            for p in participants
            if self._gql_participant_name(p) != "Unknown"
        ]
        conv_urn = conv.get("entityUrn", "")
        conv_id = conv_urn.split(":")[-1] if conv_urn else ""
        unread = conv.get("unreadCount", 0) > 0
        return conv_urn, conv_id, participant_names, unread

    def _fetch_conversation_messages(self, page, conv_urn: str, conv_id: str,
                                     participant_names: list[str],
                                     unread: bool, my_urn: str = "") -> list[dict]:
        """Fetch all messages for a conversation via dedicated messages endpoint."""
        encoded_conv = (conv_urn
                        .replace(":", "%3A").replace(",", "%2C")
                        .replace("(", "%28").replace(")", "%29")
                        .replace("=", "%3D"))
        variables = f"(conversationUrn:{encoded_conv})"

        try:
            data = self._gql_fetch(page, MESSAGES_QUERY_ID, variables)
        except RuntimeError:
            return []

        msgs = (data.get("data", {})
                .get("messengerMessagesBySyncToken", {})
                .get("elements", []))

        records = []
        for msg in msgs:
            sender = self._gql_msg_sender(msg, my_urn=my_urn)
            body = self._gql_msg_body(msg)
            if not body:
                continue

            delivered_at = msg.get("deliveredAt", 0)
            if delivered_at:
                timestamp = datetime.fromtimestamp(
                    delivered_at / 1000, tz=timezone.utc
                ).isoformat()
            else:
                timestamp = datetime.now(timezone.utc).isoformat()

            record_id = self._make_record_id(sender, body, conv_id)
            records.append({
                "id": record_id,
                "type": "linkedin_message",
                "sender": sender,
                "body": body,
                "timestamp": timestamp,
                "participants": participant_names,
                "conversation_id": conv_id,
                "meta": {
                    "unread": unread,
                },
            })

        return records

    def fetch_new(self, state: SourceState, limit: int = 100000) -> Iterator[dict]:
        """Fetch LinkedIn messages via Playwright browser + GraphQL.

        Two-phase approach:
        1. Get conversation list (paginated, sorted by last activity)
        2. For each conversation, fetch full message history

        Dedup handled by store via stable record IDs.
        """
        yielded = 0

        self.log("Launching browser...")
        pw = sync_playwright().start()
        try:
            browser = pw.chromium.launch_persistent_context(
                user_data_dir=BROWSER_DATA_DIR,
                headless=True,
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            page_obj = browser.pages[0] if browser.pages else browser.new_page()

            # Navigate to LinkedIn
            self.log("Navigating to LinkedIn...")
            page_obj.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            url = page_obj.url
            if "/login" in url or "/checkpoint" in url or "/authwall" in url:
                self.log("ERROR: Not logged in. Run auth.py first.")
                browser.close()
                pw.stop()
                return

            # Get profile URN
            self.log("Getting profile URN...")
            try:
                urn = self._get_my_urn(page_obj)
            except RuntimeError as e:
                self.log(f"Failed to get profile URN: {e}")
                browser.close()
                pw.stop()
                return

            encoded_urn = urn.replace(":", "%3A").replace(",", "%2C")
            total_convos = 0

            # Phase 1: Collect conversations
            all_convos = []

            # Page 1: SyncToken endpoint
            self.log("Fetching conversations page 1 (sync)...")
            try:
                data = self._gql_fetch(
                    page_obj,
                    "messengerConversations.0d5e6781bbee71c3e51c8843c6519f48",
                    f"(mailboxUrn:{encoded_urn})",
                )
            except RuntimeError as e:
                self.log(f"Error fetching page 1: {e}")
                browser.close()
                pw.stop()
                return

            elements = (
                data.get("data", {})
                .get("messengerConversationsBySyncToken", {})
                .get("elements", [])
            )
            all_convos.extend(elements)
            total_convos = len(elements)

            # Pages 2+: only if max_conversations > 20
            if elements and len(elements) >= 20 and self.max_conversations > 20:
                last_updated = str(elements[-1].get("lastActivityAt", 0))
                next_cursor = ""
                page_num = 1

                while total_convos < self.max_conversations:
                    if next_cursor:
                        pagination_param = f",nextCursor:{next_cursor}"
                    else:
                        pagination_param = f",lastUpdatedBefore:{last_updated}"

                    variables = (
                        f"(query:(predicateUnions:List((conversationCategoryPredicate:(category:PRIMARY_INBOX)))),"
                        f"count:20,mailboxUrn:{encoded_urn}{pagination_param})"
                    )

                    try:
                        data = self._gql_fetch(
                            page_obj,
                            "messengerConversations.9501074288a12f3ae9e3c7ea243bccbf",
                            variables,
                        )
                    except RuntimeError as e:
                        self.log(f"Error on page {page_num + 1}: {e}")
                        break

                    result_data = data.get("data", {}).get("messengerConversationsByCategoryQuery", {})
                    elements = result_data.get("elements", [])
                    if not elements:
                        break

                    page_num += 1
                    all_convos.extend(elements)
                    total_convos += len(elements)

                    metadata = result_data.get("metadata", {})
                    next_cursor = metadata.get("nextCursor", "")
                    if not next_cursor or len(elements) < 20:
                        break

            self.log(f"Found {total_convos} conversations, fetching messages...")

            # Phase 2: Fetch full messages for each conversation
            for i, conv in enumerate(all_convos):
                if yielded >= limit:
                    break

                conv_urn, conv_id, participant_names, unread = self._parse_conversation_meta(conv)
                if not conv_id:
                    continue

                records = self._fetch_conversation_messages(
                    page_obj, conv_urn, conv_id, participant_names, unread, my_urn=urn
                )

                for record in records:
                    if yielded >= limit:
                        break
                    yield record
                    yielded += 1

                if (i + 1) % 20 == 0:
                    self.log(f"  {i + 1}/{total_convos} conversations, {yielded} messages")

                time.sleep(0.3)  # rate limit

            self.log(f"Done: {total_convos} conversations, {yielded} messages")
            browser.close()

        except Exception as e:
            self.log(f"Browser error: {e}")
        finally:
            pw.stop()
