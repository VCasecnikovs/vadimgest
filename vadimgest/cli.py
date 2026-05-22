#!/usr/bin/env python3
"""
vadimgest CLI - Personal data ETL pipeline.

Usage:
    vadimgest sync                    # Sync all sources
    vadimgest read -c heartbeat       # Read new records since checkpoint
    vadimgest commit -c heartbeat     # Advance checkpoint
    vadimgest stats                   # Show statistics
    vadimgest health                  # Show health status
    vadimgest list                    # Show available sources
    vadimgest search "query" --md     # Full-text search
    vadimgest init                    # Create config file
    vadimgest config                  # Show effective config
    vadimgest serve                   # Launch web dashboard on :8484
    vadimgest daemon                  # Run background sync scheduler
    vadimgest edge-agent --once       # Push local records to a server
"""

import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .store import DataStore
from .config import _HOME_CONFIG_DIR
from .config import (
    get_data_dir, get_source_config, load_config,
    _find_config_file, _PACKAGE_DIR, _SOURCE_DEFAULTS,
)
from .ingest.sources import SYNCERS, get_syncer_class, get_load_error, all_source_names


def sync_source(store: DataStore, source: str, limit: int = 10000) -> tuple[int, str | None]:
    """Sync a single source (cron mode). Returns (count, error)."""
    syncer_class = get_syncer_class(source)
    if syncer_class is None:
        error = get_load_error(source)
        if error:
            print(f"Source '{source}' unavailable: {error}")
        else:
            print(f"Unknown source: {source}")
            print(f"Available: {', '.join(all_source_names())}")
        return 0, error or "unknown source"

    config = get_source_config(source)
    syncer = syncer_class(store, config)

    print(f"Syncing {source}...", end=" ", flush=True)
    start = datetime.now()

    try:
        count, summary = syncer.sync(limit=limit)
        duration = (datetime.now() - start).total_seconds()
        label = f" ({', '.join(summary[:3])})" if summary else ""
        print(f"\u2713 {count} records ({duration:.1f}s){label}")
        syncer.log_run("ok", count=count, duration=duration, summary=summary)
        return count, None
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        error_msg = str(e)
        print(f"\u2717 Error: {e}")
        import traceback
        traceback.print_exc()
        syncer.log_run("error", error=error_msg, duration=duration)
        return 0, error_msg


def sync_all(store: DataStore, limit: int = 10000) -> dict[str, int]:
    """Sync all cron sources."""
    results = {}
    for source in all_source_names():
        config = get_source_config(source)
        # Skip unavailable sources silently
        if get_syncer_class(source) is None:
            continue
        count, _ = sync_source(store, source, limit)
        results[source] = count
    return results



def show_stats(store: DataStore):
    """Show statistics for all sources."""
    print("\nvadimgest Statistics")
    print("=" * 60)

    stats = store.stats()
    if not stats:
        print("No data yet. Run sync first.")
        return

    total = 0
    for source, info in sorted(stats.items()):
        count = info["records"]
        total += count
        last_ts = info.get("last_ts", "N/A")
        if last_ts and len(str(last_ts)) > 19:
            last_ts = str(last_ts)[:19]
        mode = get_source_config(source).get("mode", "unknown")
        print(f"{source:12} {count:>8} records   last: {last_ts}  [{mode}]")

    print("-" * 60)
    print(f"{'TOTAL':12} {total:>8} records")

    # Show consumer checkpoints
    checkpoints_dir = store.checkpoints_dir
    if checkpoints_dir.exists():
        consumers = list(checkpoints_dir.glob("*.json"))
        if consumers:
            print(f"\nConsumers: {len(consumers)}")
            for cp_file in consumers:
                print(f"  - {cp_file.stem}")


def show_health(store: DataStore):
    """Show health status of all sources."""
    print("\nvadimgest Health Check")
    print("=" * 70)

    runs_file = store.base_path / "sync_runs.jsonl"

    # Load recent runs
    recent_runs = {}
    if runs_file.exists():
        with open(runs_file) as f:
            for line in f:
                try:
                    run = json.loads(line.strip())
                    source = run.get("source")
                    if source:
                        if source not in recent_runs:
                            recent_runs[source] = []
                        recent_runs[source].append(run)
                except Exception:
                    pass

    now = datetime.now(timezone.utc)
    now_naive = datetime.now()
    all_ok = True

    for source in all_source_names():
        config = get_source_config(source)
        mode = config.get("mode", "cron")

        # Check if source is loadable
        syncer_cls = get_syncer_class(source)
        available = syncer_cls is not None

        if not available:
            error = get_load_error(source)
            status = f"- unavailable: {error[:25]}" if error else "- unavailable"
            last_run = "N/A"
        else:
            # Check if dependencies are satisfied
            try:
                ready = syncer_cls.check_ready()
            except Exception:
                ready = {"ok": True}

            runs = recent_runs.get(source, [])
            if runs:
                last = runs[-1]
                last_ts = datetime.fromisoformat(last["ts"])
                if last_ts.tzinfo is None:
                    age = now_naive - last_ts
                else:
                    last_ts = last_ts.astimezone(timezone.utc)
                    age = now - last_ts

                if not ready.get("ok", True):
                    missing = ready.get("missing", [])
                    reason = missing[0][:25] if missing else "deps missing"
                    status = f"\u26a0\ufe0f  {reason}"
                    all_ok = False
                elif last["status"] == "ok":
                    if age < timedelta(hours=2):
                        status = "\u2713 ok"
                    else:
                        status = "\u26a0\ufe0f  stale"
                        all_ok = False
                else:
                    status = f"\u2717 error: {last.get('error', 'unknown')[:30]}"
                    all_ok = False

                last_run = f"{age.total_seconds() / 60:.0f}m ago"
            else:
                if not ready.get("ok", True):
                    missing = ready.get("missing", [])
                    reason = missing[0][:25] if missing else "deps missing"
                    status = f"\u26a0\ufe0f  {reason}"
                else:
                    status = "\u26a0\ufe0f  never run"
                last_run = "N/A"
                all_ok = False

        print(f"{source:12} {status:30} last: {last_run}")

    print("-" * 70)
    if all_ok:
        print("Overall: \u2713 All sources healthy")
    else:
        print("Overall: \u26a0\ufe0f  Some sources need attention")

    # Show recent errors
    all_runs = []
    for source, runs in recent_runs.items():
        all_runs.extend(runs)

    errors = [r for r in all_runs if r.get("status") == "error"]
    errors.sort(key=lambda x: x["ts"], reverse=True)

    if errors:
        print(f"\nRecent errors ({len(errors)} total):")
        for err in errors[:5]:
            ts = err["ts"][:19]
            print(f"  [{ts}] {err['source']}: {err.get('error', 'unknown')[:50]}")


def show_logs(store: DataStore, lines: int = 20):
    """Show recent sync logs."""
    log_file = store.base_path / "sync.log"

    if not log_file.exists():
        print("No logs yet.")
        return

    print(f"\nRecent logs ({log_file}):")
    print("-" * 60)

    with open(log_file) as f:
        all_lines = f.readlines()

    for line in all_lines[-lines:]:
        print(line.rstrip())


def _is_self_sender(sender: str) -> bool:
    """Check if sender matches configured self_names (case-insensitive substring)."""
    if not sender:
        return False
    cfg = load_config()
    self_names = cfg.get("self_names", [])
    if not self_names:
        return False
    sender_lower = sender.lower()
    return any(name.lower() in sender_lower for name in self_names)


def format_record(record, source: str, fmt: str = "short") -> str:
    """Format a single record for output."""
    data = record.data
    record_type = data.get("type", "unknown")

    if fmt == "json":
        return json.dumps(data, ensure_ascii=False)

    if fmt == "short":
        return f"[{record._line}] {record_type}: {data.get('id', '?')[:50]}"

    # Detailed format
    if record_type == "conversation":
        chat = data.get("chat", "unknown")
        folder = data.get("folder", "")
        messages = data.get("messages", [])
        period_end = data.get("period_end", "")[:10] if data.get("period_end") else ""

        lines = [f"### [{folder}/{chat}] {period_end}"]
        for msg in messages[:15]:
            sender = msg.get("sender", "?")
            text = (msg.get("text") or "")[:200].replace("\n", " ")
            ts = msg.get("ts", "")[-8:-3] if msg.get("ts") else ""
            if text:
                lines.append(f"  [{ts}] {sender}: {text}")
        if len(messages) > 15:
            lines.append(f"  ... +{len(messages) - 15} more")
        return "\n".join(lines)

    elif record_type == "meeting":
        title = data.get("title", "Meeting")
        duration = data.get("duration_minutes", 0)
        raw_participants = data.get("participants", [])[:5]
        participants = ", ".join(
            p.get("name", str(p)) if isinstance(p, dict) else str(p)
            for p in raw_participants
        )
        notes = (data.get("notes") or "")[:500]
        transcript = (data.get("transcript") or "")[:800]

        lines = [f"### Meeting: {title} ({duration}m)"]
        if participants:
            lines.append(f"Participants: {participants}")
        if notes:
            lines.append(f"\n**Notes:**\n{notes}")
        if transcript:
            lines.append(f"\n**Transcript:**\n{transcript}...")
        return "\n".join(lines)

    elif record_type == "activity":
        title = data.get("title", "Activity")
        category = data.get("category", "")
        duration = data.get("duration_seconds", 0) // 60
        summary = data.get("summary", "")[:100]
        return f"- [{category}] {title} ({duration}m) {summary}"

    elif record_type == "document":
        title = data.get("title", "Document")
        path = data.get("path", "")
        return f"- {title}: {path}"

    elif record_type == "issue":
        number = data.get("number", "?")
        title = data.get("title", "Issue")
        status = data.get("status", "")
        project = data.get("project", "")
        assignees = ", ".join(data.get("assignees", []))
        status_str = f" [{status}]" if status else ""
        assign_str = f" -> {assignees}" if assignees else ""
        return f"- #{number} {title}{status_str}{assign_str} ({project})"

    elif record_type == "email":
        subject = data.get("subject", "(no subject)")
        from_addr = data.get("from", "")
        to_addr = data.get("to", "")
        account = data.get("account", "")
        is_unread = data.get("is_unread", False)
        direction = data.get("direction", "received")
        awaiting_reply = data.get("awaiting_reply")

        tags = []
        if is_unread:
            tags.append("UNREAD")
        if direction == "sent":
            tags.append("SENT")
        if awaiting_reply:
            tags.append("AWAITING REPLY")

        tag_str = " [" + ", ".join(tags) + "]" if tags else ""
        addr_info = f"from {from_addr}" if direction == "received" else f"to {to_addr}"
        return f"- {subject}{tag_str} {addr_info} ({account})"

    elif record_type == "email_status_update":
        subject = data.get("subject", "(no subject)")
        account = data.get("account", "")
        return f"- [REPLY RECEIVED] {subject} ({account})"

    elif record_type == "task":
        title = data.get("title", "(untitled)")
        list_name = data.get("list_name", "")
        due = data.get("due", "")
        due_str = f" due:{due[:10]}" if due else ""
        return f"- {title}{due_str} ({list_name})"

    elif record_type == "calendar_event":
        title = data.get("title", "(no title)")
        start = data.get("start", "")[:16] if data.get("start") else ""
        location = data.get("location", "")
        calendar_name = data.get("calendar_name", "")
        attendees = data.get("attendees", [])
        att_count = f" ({len(attendees)} attendees)" if attendees else ""
        loc_str = f" @ {location}" if location else ""
        return f"- {start} {title}{loc_str}{att_count} [{calendar_name}]"

    elif record_type == "linkedin_message":
        sender = data.get("sender", "Unknown")
        body = (data.get("body") or "")[:200].replace("\n", " ")
        participants = data.get("participants", [])
        part_str = f" ({', '.join(participants)})" if participants else ""
        return f"- [LinkedIn] {sender}{part_str}: {body}"

    elif record_type == "linkedin_invitation":
        from_name = data.get("from_name", "Unknown")
        headline = data.get("from_headline", "")
        message = (data.get("message") or "")[:100]
        hl_str = f" - {headline}" if headline else ""
        msg_str = f": {message}" if message else ""
        return f"- [LinkedIn Invite] {from_name}{hl_str}{msg_str}"

    elif record_type == "linkedin_profile_view":
        viewer = data.get("viewer_name", "Anonymous")
        headline = data.get("viewer_headline", "")
        hl_str = f" - {headline}" if headline else ""
        return f"- [Profile View] {viewer}{hl_str}"

    elif record_type == "message":
        sender = data.get("sender", "Unknown")
        text = (data.get("text") or "")[:300].replace("\n", " ")
        ts = data.get("timestamp", "")
        # Extract HH:MM from timestamp
        time_str = ""
        if ts:
            try:
                time_str = ts[11:16] if len(ts) > 16 else ts[-5:]
            except (IndexError, TypeError):
                pass
        chat = data.get("chat", "")
        # Mark own messages (configure via config.yaml self_names)
        is_self = _is_self_sender(sender)
        prefix = "[Me]" if is_self else sender
        return f"[{time_str}] {prefix}: {text}"

    else:
        return f"[{source}] {str(data)[:200]}"


def read_consumer(store: DataStore, consumer: str, sources: list[str], fmt: str = "md",
                  limit: int = 50, commit: bool = False, stats_only: bool = False,
                  context: int = 0):
    """Universal consumer reader with checkpoint support."""

    if stats_only:
        print(f"Vadimgest Stats for '{consumer}':")
        checkpoint = store.get_checkpoint(consumer)

        for source in sources:
            total = store.count(source)
            pos = checkpoint.positions.get(source, {})
            last_line = pos.get("line", 0)
            new_count = total - last_line
            print(f"  {source}: {total} total, {new_count} new (checkpoint at {last_line})")

        if checkpoint.updated_at:
            print(f"\nLast checkpoint: {checkpoint.updated_at}")
        else:
            print("\nNo checkpoint yet - use 'commit' command to initialize")
        return

    if commit:
        for source in sources:
            store.commit(source, consumer)
        print(f"Committed checkpoint for: {', '.join(sources)}")
        return

    # Read new records (with optional context for chat sources)
    from .consumer.reader import ConsumerReader, CHAT_SOURCES
    reader = ConsumerReader(store)

    results = {}
    context_results = {}
    total = 0
    for source in sources:
        if context > 0 and source in CHAT_SOURCES:
            ctx, new = reader.read_with_context(source, consumer, context)
            if new:
                results[source] = new
                total += len(new)
            if ctx:
                context_results[source] = ctx
        else:
            records = list(store.read_new(source, consumer=consumer))
            if records:
                results[source] = records
                total += len(records)

    if not results:
        print("No new data since last checkpoint.")
        return

    if fmt == "json":
        output = {}
        for source, records in results.items():
            src_out = {"new": [r.data for r in records[:limit]]}
            if source in context_results:
                src_out["context"] = [r.data for r in context_results[source]]
            output[source] = src_out
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    # Markdown format
    ctx_total = sum(len(v) for v in context_results.values())
    header = f"# Vadimgest Data ({total} new records"
    if ctx_total:
        header += f", {ctx_total} context"
    header += ")\n"
    print(header)

    from collections import defaultdict

    for source, records in results.items():
        print(f"\n## {source.title()} ({len(records)} records)\n")

        if source in CHAT_SOURCES:
            ctx_by_chat: dict[str, list] = defaultdict(list)
            if source in context_results:
                for r in context_results[source]:
                    ctx_by_chat[r.data.get("chat", "Unknown")].append(r)

            chat_groups: dict[str, list] = defaultdict(list)
            for r in records:
                chat_groups[r.data.get("chat", "Unknown")].append(r)

            for chat_name, chat_records in chat_groups.items():
                chat_records.sort(key=lambda r: r.data.get("timestamp", ""))
                ctx_records = ctx_by_chat.get(chat_name, [])
                ctx_records.sort(key=lambda r: r.data.get("timestamp", ""))

                label = f"{len(chat_records)} new"
                if ctx_records:
                    label += f", {len(ctx_records)} context"
                print(f"### {chat_name} ({label})\n")

                if ctx_records:
                    print("[context]")
                    for r in ctx_records:
                        print(format_record(r, source, "md"))
                    print("\n--- new messages ---\n")

                shown = chat_records[:limit]
                for r in shown:
                    print(format_record(r, source, "md"))
                if len(chat_records) > limit:
                    print(f"... +{len(chat_records) - limit} more")
                print()

        elif source == "dayflow":
            for r in records[:min(30, limit)]:
                print(format_record(r, source, "md"))
            if len(records) > 30:
                print(f"\n... +{len(records) - 30} more activities")
        else:
            for r in records[:limit]:
                print(format_record(r, source, "md"))
                print()
            if len(records) > limit:
                print(f"... +{len(records) - limit} more {source} records\n")


def _default_read_sources() -> list[str]:
    """Get default sources for read command (all enabled sources)."""
    sources = []
    for name in all_source_names():
        config = get_source_config(name)
        if config.get("enabled"):
            sources.append(name)
    return sources or list(all_source_names())  # fallback to all if none enabled


# ---- Source requirement metadata ----

_SOURCE_REQUIREMENTS = {
    "telegram": {
        "platform": "any",
        "pip_extra": "telegram (telethon)",
        "external": [],
        "setup": "API credentials from my.telegram.org + .env",
    },
    "signal": {
        "platform": "macOS",
        "pip_extra": None,
        "external": ["sigtop"],
        "setup": "Signal Desktop running locally",
    },
    "granola": {
        "platform": "macOS",
        "pip_extra": None,
        "external": [],
        "setup": "Granola app installed",
    },
    "dayflow": {
        "platform": "macOS",
        "pip_extra": None,
        "external": [],
        "setup": "Dayflow app installed",
    },
    "obsidian": {
        "platform": "any",
        "pip_extra": None,
        "external": [],
        "setup": "vault_path in config",
    },
    "claude": {
        "platform": "any",
        "pip_extra": None,
        "external": [],
        "setup": "Claude Code installed",
    },
    "github": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "GitHub MCP server configured",
    },
    "gmail": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "Google MCP server + OAuth",
    },
    "gtasks": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "Google MCP server + OAuth",
    },
    "whatsapp": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "WhatsApp MCP server configured",
    },
    "imessage": {
        "platform": "macOS",
        "pip_extra": None,
        "external": ["imessage-export"],
        "setup": "Compile export.swift + Full Disk Access",
    },
    "browser": {
        "platform": "macOS",
        "pip_extra": None,
        "external": [],
        "setup": "Arc browser installed",
    },
    "github_notifications": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "GitHub MCP server configured",
    },
    "nextcloud": {
        "platform": "any",
        "pip_extra": None,
        "external": [],
        "setup": "NEXTCLOUD_USER + NEXTCLOUD_TOKEN in .env",
    },
    "gdrive": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "Google MCP server + OAuth",
    },
    "calendar": {
        "platform": "any",
        "pip_extra": None,
        "external": ["mcp-cli"],
        "setup": "Google MCP server + OAuth",
    },
    "linkedin": {
        "platform": "any",
        "pip_extra": "linkedin-api",
        "external": [],
        "setup": "linkedin.email + linkedin.password in config.yaml",
    },
}


def _check_tool(name: str) -> bool:
    """Check if an external tool is available in PATH."""
    import shutil
    if name == "imessage-export":
        # Check in sources/imessage/ directory
        binary = _PACKAGE_DIR / "sources" / "imessage" / "imessage-export"
        return binary.exists() and os.access(binary, os.X_OK)
    if name == "mcp-cli":
        # Check mcp-cli or Claude binary
        if shutil.which("mcp-cli"):
            return True
        versions_dir = Path.home() / ".local/share/claude/versions"
        if versions_dir.exists():
            return any(p.is_file() and os.access(p, os.X_OK) for p in versions_dir.iterdir())
        return False
    return shutil.which(name) is not None


def cmd_list():
    """List sources with availability status and requirements."""
    import sys

    print("Sources:")
    print(f"{'Name':12} {'Mode':7} {'Status':10} {'Reason'}")
    print("-" * 65)

    for source in all_source_names():
        config = get_source_config(source)
        mode = config.get("mode", "cron")
        enabled = config.get("enabled", False)
        available = get_syncer_class(source) is not None
        error = get_load_error(source)
        reqs = _SOURCE_REQUIREMENTS.get(source, {})

        # Determine status
        if available and enabled:
            status = "\u2713 ready"
            reason = ""
        elif available and not enabled:
            status = "- disabled"
            reason = "enable in config.yaml"
        else:
            status = "\u2717 unavail"
            # Build helpful reason
            reasons = []
            if reqs.get("platform") == "macOS" and sys.platform != "darwin":
                reasons.append("macOS only")
            if error:
                # Simplify common error messages
                if "No module named" in error:
                    mod = error.split("'")[1] if "'" in error else error
                    reasons.append(f"pip: {mod}")
                elif "mcp-cli" in error.lower() or "mcp_cli" in error.lower():
                    reasons.append("needs mcp-cli")
                elif "FileNotFoundError" in error or "not found" in error.lower():
                    for tool in reqs.get("external", []):
                        reasons.append(f"missing: {tool}")
                    if not reasons:
                        reasons.append(error[:40])
                else:
                    reasons.append(error[:40])
            elif reqs.get("external"):
                for tool in reqs["external"]:
                    if not _check_tool(tool):
                        reasons.append(f"missing: {tool}")
            reason = "; ".join(reasons) if reasons else ""

        print(f"  {source:12} {mode:7} {status:10} {reason}")

    print()
    print("Run 'vadimgest doctor' for detailed dependency check.")
    print("Run 'vadimgest config' to see config file locations.")


def cmd_doctor():
    """Check all external dependencies and report status."""
    import sys
    import shutil

    print("vadimgest Doctor")
    print("=" * 60)

    # Python version
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    ok = sys.version_info >= (3, 11)
    print(f"{'[OK]' if ok else '[!!]'} Python {py_ver} {'(>= 3.11 required)' if not ok else ''}")

    # Platform
    platform = "macOS" if sys.platform == "darwin" else sys.platform
    print(f"[--] Platform: {platform}")

    # Config
    config_file = _find_config_file()
    print(f"{'[OK]' if config_file else '[!!]'} Config: {config_file or 'not found (run vadimgest init)'}")

    # Data dir
    data_dir = get_data_dir()
    print(f"[OK] Data dir: {data_dir}")

    print()
    print("Python packages:")
    print("-" * 40)

    # Check pip packages
    pip_checks = [
        ("pyyaml", "yaml", "Core dependency"),
        ("filelock", "filelock", "Core dependency"),
        ("telethon", "telethon", "Telegram source"),
        ("python-dotenv", "dotenv", ".env file loading"),
    ]
    for pkg_name, import_name, purpose in pip_checks:
        try:
            __import__(import_name)
            print(f"[OK] {pkg_name:20} {purpose}")
        except ImportError:
            print(f"[--] {pkg_name:20} {purpose} (pip install {pkg_name})")

    print()
    print("External tools:")
    print("-" * 40)

    # Check external tools
    tool_checks = [
        ("sigtop", "Signal source", "brew install sigtop"),
        ("mcp-cli", "GitHub/Gmail/GTasks/WhatsApp sources", "install Claude Code"),
        ("imessage-export", "iMessage source", "swiftc -O sources/imessage/export.swift -o sources/imessage/imessage-export"),
    ]
    for tool, purpose, install_hint in tool_checks:
        found = _check_tool(tool)
        if found:
            print(f"[OK] {tool:20} {purpose}")
        else:
            print(f"[--] {tool:20} {purpose}")
            print(f"     Install: {install_hint}")

    # macOS-specific app data
    if sys.platform == "darwin":
        print()
        print("macOS app data:")
        print("-" * 40)
        app_checks = [
            ("Granola", Path.home() / "Library/Application Support/Granola/cache-v3.json"),
            ("Dayflow", Path.home() / "Library/Application Support/Dayflow/chunks.sqlite"),
            ("Signal Desktop", Path.home() / "Library/Application Support/Signal"),
            ("Messages DB", Path.home() / "Library/Messages/chat.db"),
        ]
        for name, path in app_checks:
            exists = path.exists()
            print(f"{'[OK]' if exists else '[--]'} {name:20} {path}")

    # Claude sessions
    claude_dir = Path.home() / ".claude" / "projects"
    if claude_dir.exists():
        print(f"[OK] {'Claude projects':20} {claude_dir}")
    else:
        print(f"[--] {'Claude projects':20} {claude_dir}")

    print()
    print("Per-source status:")
    print("-" * 40)
    ready = 0
    total = 0
    for source in all_source_names():
        total += 1
        config = get_source_config(source)
        enabled = config.get("enabled", False)
        available = get_syncer_class(source) is not None
        reqs = _SOURCE_REQUIREMENTS.get(source, {})

        if available and enabled:
            print(f"[OK] {source:12} ready")
            ready += 1
        elif available and not enabled:
            print(f"[--] {source:12} available but disabled in config")
        else:
            error = get_load_error(source)
            missing = []
            if reqs.get("platform") == "macOS" and sys.platform != "darwin":
                missing.append("macOS only")
            for tool in reqs.get("external", []):
                if not _check_tool(tool):
                    missing.append(f"install {tool}")
            if reqs.get("pip_extra"):
                try:
                    pkg = reqs["pip_extra"].split("(")[1].rstrip(")")
                    __import__(pkg)
                except (ImportError, IndexError):
                    missing.append(f'pip install -e ".[{reqs["pip_extra"].split()[0]}]"')
            reason = "; ".join(missing) if missing else (error[:50] if error else "unavailable")
            print(f"[!!] {source:12} {reason}")

    print()
    print(f"Summary: {ready}/{total} sources ready")


def cmd_autostart(enable: bool = True, port: int = 8484, interval: int = 300):
    """Install or remove autostart services (launchd on macOS, systemd on Linux)."""
    import sys
    from .web.autostart import install, uninstall, is_installed

    if not enable:
        if not is_installed():
            print("Autostart is not installed.")
            return
        uninstall()
        print("Autostart services removed.")
        return

    install(port=port, interval=interval)

    if sys.platform == "darwin":
        print(f"Installed and started: com.vadimgest.dashboard")
        print(f"Installed and started: com.vadimgest.daemon")
        print(f"\nDashboard: http://localhost:{port}")
        print(f"Daemon: syncing every {interval}s")
        print(f"Logs: /tmp/vadimgest-dashboard.log, /tmp/vadimgest-daemon.log")
    else:
        print(f"Installed and started: vadimgest-dashboard")
        print(f"Installed and started: vadimgest-daemon")
        print(f"\nDashboard: http://localhost:{port}")
        print(f"Daemon: syncing every {interval}s")
        print(f"Logs: journalctl --user -u vadimgest-dashboard -f")

    print(f"\nTo remove: vadimgest autostart --disable")


def cmd_init():
    """Create config file from template."""
    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    config_dir = Path(xdg) / "vadimgest"
    config_file = config_dir / "config.yaml"

    if config_file.exists():
        print(f"Config already exists: {config_file}")
        return

    # Check if a local config exists (home dotfolder / submodule / dev mode)
    for local_config in (_HOME_CONFIG_DIR / "config.yaml", _PACKAGE_DIR / "config.yaml"):
        if local_config.exists():
            print(f"Using local config: {local_config}")
            return

    config_dir.mkdir(parents=True, exist_ok=True)

    # Write template
    example = _PACKAGE_DIR / "config.example.yaml"
    if example.exists():
        import shutil
        shutil.copy(example, config_file)
        print(f"Created config from template: {config_file}")
    else:
        # Generate minimal config
        import yaml
        config_file.write_text(yaml.dump(
            {name: {"enabled": False} for name in _SOURCE_DEFAULTS},
            default_flow_style=False,
        ))
        print(f"Created minimal config: {config_file}")

    print(f"Edit it to enable sources: {config_file}")


def cmd_config():
    """Show effective config and file locations."""
    config_file = _find_config_file()
    data_dir = get_data_dir()

    print("vadimgest Configuration")
    print("=" * 60)
    print(f"Config file:  {config_file or '(none - using defaults)'}")
    print(f"Data dir:     {data_dir}")
    print()

    print("Sources:")
    for name in all_source_names():
        config = get_source_config(name)
        enabled = config.get("enabled", False)
        mode = config.get("mode", "cron")
        available = get_syncer_class(name) is not None
        error = get_load_error(name) if not available else None

        status = "enabled" if enabled else "disabled"
        if not available:
            status += f" (unavailable: {error})" if error else " (unavailable)"

        print(f"  {name:12} [{mode:6}] {status}")


def main():
    parser = argparse.ArgumentParser(description="vadimgest - Personal data ETL")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # sync command
    sync_parser = subparsers.add_parser("sync", help="Sync sources")
    sync_parser.add_argument("sources", nargs="*", help="Sources to sync")
    sync_parser.add_argument("--limit", type=int, default=10000, help="Max records")

    # stats command
    subparsers.add_parser("stats", help="Show statistics")

    # health command
    subparsers.add_parser("health", help="Show health status")

    # logs command
    logs_parser = subparsers.add_parser("logs", help="Show recent logs")
    logs_parser.add_argument("-n", "--lines", type=int, default=20, help="Number of lines")

    # read command
    read_parser = subparsers.add_parser("read", help="Read new records with checkpoint tracking")
    read_parser.add_argument("--consumer", "-c", required=True, help="Consumer name")
    read_parser.add_argument("--sources", "-s", help="Sources to read (comma-separated)")
    read_parser.add_argument("--exclude-source", "-x", action="append", default=[], help="Sources to exclude (repeatable)")
    read_parser.add_argument("--format", "-f", choices=["md", "json", "short"], default="md", help="Output format")
    read_parser.add_argument("--limit", type=int, default=50, help="Max records per source")
    read_parser.add_argument("--context", type=int, default=0, help="Older messages per chat for context (chat sources only)")
    read_parser.add_argument("--stats", action="store_true", help="Show stats only")

    # commit command
    commit_parser = subparsers.add_parser("commit", help="Advance consumer checkpoint to current position")
    commit_parser.add_argument("--consumer", "-c", required=True, help="Consumer name")
    commit_parser.add_argument("--sources", "-s", help="Sources to commit (comma-separated, default: all)")

    # list command
    subparsers.add_parser("list", help="List available sources")

    # doctor command
    subparsers.add_parser("doctor", help="Check external dependencies for all sources")

    # init command
    subparsers.add_parser("init", help="Create config file from template")

    # config command
    subparsers.add_parser("config", help="Show effective configuration")

    # search command (delegates to vadimgest.search)
    subparsers.add_parser("search", help="FTS5 full-text search (pass -h for search help)")

    # serve command
    serve_parser = subparsers.add_parser("serve", help="Launch web dashboard")
    serve_parser.add_argument("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
    serve_parser.add_argument("--port", "-p", type=int, default=8484, help="Port (default: 8484)")
    serve_parser.add_argument("--debug", action="store_true", help="Debug mode")
    serve_parser.add_argument("--no-open", action="store_true", help="Don't open browser automatically")

    # daemon command
    daemon_parser = subparsers.add_parser("daemon", help="Run background sync scheduler")
    daemon_parser.add_argument("--interval", "-i", type=int, default=300, help="Sync interval in seconds (default: 300)")
    daemon_parser.add_argument("--sources", "-s", help="Sources to sync (comma-separated, default: all enabled)")

    # edge-agent command
    edge_parser = subparsers.add_parser("edge-agent", help="Run local edge uploader")
    edge_parser.add_argument("--once", action="store_true", help="Run one sync/upload cycle and exit")

    # autostart command
    autostart_parser = subparsers.add_parser("autostart", help="Install/remove autostart services (launchd/systemd)")
    autostart_parser.add_argument("--disable", action="store_true", help="Remove autostart services")
    autostart_parser.add_argument("--port", "-p", type=int, default=8484, help="Dashboard port (default: 8484)")
    autostart_parser.add_argument("--interval", "-i", type=int, default=300, help="Sync interval in seconds (default: 300)")

    args, remaining = parser.parse_known_args()

    data_dir = get_data_dir()
    store = DataStore(data_dir)

    if args.command == "search":
        import sys
        sys.argv = ["vadimgest search"] + remaining
        from .search.__main__ import main as search_main
        search_main()
        return

    if args.command == "autostart":
        cmd_autostart(enable=not args.disable, port=args.port, interval=args.interval)
        return

    if args.command == "init":
        cmd_init()
        return

    if args.command == "config":
        cmd_config()
        return

    if args.command == "list" or not args.command:
        cmd_list()
        return

    if args.command == "doctor":
        cmd_doctor()
        return

    if args.command == "sync":
        print(f"Data directory: {data_dir}")
        print()
        if args.sources:
            for source in args.sources:
                sync_source(store, source, args.limit)
        else:
            sync_all(store, args.limit)
        print()
        show_stats(store)

    elif args.command == "stats":
        show_stats(store)

    elif args.command == "health":
        show_health(store)

    elif args.command == "logs":
        show_logs(store, args.lines)

    elif args.command == "read":
        sources = args.sources.split(",") if args.sources else _default_read_sources()
        if args.exclude_source:
            sources = [s for s in sources if s not in args.exclude_source]
        read_consumer(store, args.consumer, sources, args.format, args.limit, commit=False, stats_only=args.stats, context=args.context)

    elif args.command == "commit":
        sources = args.sources.split(",") if args.sources else [f.stem for f in store.sources_dir.glob("*.jsonl")]
        for source in sources:
            store.commit(source, args.consumer)
        print(f"Committed checkpoint for {args.consumer}: {', '.join(sources)}")

    elif args.command == "serve":
        try:
            from .web.app import create_app
        except ImportError:
            print("Flask not installed. Run: pip install 'vadimgest[web]'")
            return
        if args.host not in ("127.0.0.1", "localhost", "::1"):
            print("\n⚠️  WARNING: Binding to a non-localhost address.")
            print("   vadimgest has no authentication - all your data will be")
            print("   accessible to anyone who can reach this address.\n")
        app = create_app(store)
        url = f"http://{args.host}:{args.port}"
        print(f"vadimgest dashboard: {url}")
        if not args.no_open:
            import webbrowser
            import threading as _th
            _th.Timer(1.0, webbrowser.open, args=[url]).start()
        app.run(host=args.host, port=args.port, debug=args.debug)

    elif args.command == "daemon":
        from .daemon import run_daemon
        sources = args.sources.split(",") if args.sources else None
        run_daemon(interval=args.interval, sources=sources)

    elif args.command == "edge-agent":
        from .edge_agent import run_edge_agent
        run_edge_agent(once=args.once)


if __name__ == "__main__":
    main()
