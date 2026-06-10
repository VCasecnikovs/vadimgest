"""vadimgest web dashboard - monitoring, setup, and source management UI."""

import json
import os
import sys
import time
import threading
import urllib.request

import yaml
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, Response

from ..store import DataStore
from ..edge import (
    EdgeAuthError,
    EdgeIngestError,
    create_edge_token,
    ingest_edge_batch,
    list_edge_tokens,
    revoke_edge_token,
    verify_edge_token,
)
from ..config import (
    get_data_dir, get_source_config, load_config, _find_config_file,
    _SOURCE_DEFAULTS, save_source_config, ensure_config_file,
    save_env_vars, get_env_status, get_env_file_path,
    get_search_config, save_search_config, get_edge_config, save_edge_config,
)
from ..edge_agent import EdgeAgent, _default_transport
from ..ingest.sources import get_syncer_class, get_load_error, all_source_names
from ..daemon import SyncDaemon
from .setup import (
    check_app, check_full_disk_access, scan_obsidian_vaults, test_nextcloud,
    check_auth_state,
    AUTH_MANAGER, TELEGRAM_AUTH, AUTH_COMMANDS,
    DEFAULT_TELEGRAM_API_ID, DEFAULT_TELEGRAM_API_HASH,
)
from .setup_map import get_setup_info


_STATIC_DEPS = {
    "granola": {"python": [], "cli": [], "credentials": [], "os": ["macos"]},
    "nextcloud": {"python": ["requests"], "cli": [], "credentials": [], "os": []},
    "calendar": {"python": [], "cli": ["gog"], "credentials": [], "os": []},
    "gdrive": {"python": [], "cli": ["gog"], "credentials": [], "os": []},
    "browser": {"python": [], "cli": [], "credentials": [], "os": ["macos"]},
    "github": {"python": [], "cli": ["gh"], "credentials": [], "os": []},
    "github_notifications": {"python": [], "cli": ["gh"], "credentials": [], "os": []},
    "hlopya": {"python": [], "cli": [], "credentials": [], "os": ["macos"]},
    "telegram": {"python": ["telethon"], "cli": [], "credentials": [], "os": []},
    "gtasks": {"python": [], "cli": ["gog"], "credentials": [], "os": []},
    "dayflow": {"python": [], "cli": [], "credentials": [], "os": ["macos"]},
    "imessage": {"python": [], "cli": [], "credentials": [], "os": ["macos:full_disk_access"]},
    "signal": {"python": [], "cli": ["sigtop"], "credentials": [], "os": ["macos"]},
    "gmail": {"python": [], "cli": ["gog"], "credentials": [], "os": []},
    "whatsapp": {"python": [], "cli": ["wacli"], "credentials": [], "os": []},
    "linkedin": {"python": ["playwright"], "cli": [], "credentials": [], "os": []},
    "xnews": {"python": [], "cli": ["bird"], "credentials": [], "os": []},
}

_STATIC_CRED_HELP = {}


def _safe_error(e: Exception) -> str:
    """Return error message with home directory paths redacted."""
    msg = str(e)
    home = str(Path.home())
    if home in msg:
        msg = msg.replace(home, "~")
    return msg


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _hours_old(value, *, now: datetime | None = None) -> float | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (now - dt).total_seconds() / 3600)


def _worst_status(items: list[dict]) -> str:
    if not items:
        return "unknown"
    order = {"healthy": 0, "unknown": 1, "degraded": 2, "broken": 3}
    return max((str(i.get("status") or "unknown") for i in items), key=lambda s: order.get(s, 1))


def _fetch_json_url(url: str, *, timeout: float = 1.5) -> tuple[dict | None, str | None]:
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw or "{}"), None
    except Exception as e:
        return None, _safe_error(e)


def create_app(store: DataStore | None = None) -> Flask:
    if store is None:
        store = DataStore(get_data_dir())

    app = Flask(__name__)
    app.config["store"] = store

    _sync_log: list[dict] = []
    _sync_lock = threading.Lock()

    _daemon: SyncDaemon | None = None
    _daemon_lock = threading.Lock()
    _daemon_started_at: str | None = None

    _schema_cache: dict[str, dict] = {}

    def _validate_source_config(name: str, config: dict) -> list[str]:
        errors = []
        cls = get_syncer_class(name)
        schema = cls.config_schema if cls else _schema_cache.get(name, {})
        for key, value in config.items():
            if key not in schema:
                continue
            field_def = schema[key]
            field_type = field_def.get("type", "str")

            if field_type == "int":
                try:
                    int_val = int(value)
                except (ValueError, TypeError):
                    errors.append(f"{key}: must be an integer")
                    continue
                if "min" in field_def and int_val < field_def["min"]:
                    errors.append(f"{key}: minimum value is {field_def['min']}")
                if "max" in field_def and int_val > field_def["max"]:
                    errors.append(f"{key}: maximum value is {field_def['max']}")

            if key == "schedule" or "schedule" in key:
                str_val = str(value).strip()
                if str_val:
                    parts = str_val.split()
                    if len(parts) != 5:
                        errors.append(f"{key}: cron schedule must have 5 fields (minute hour day month weekday)")
                    else:
                        ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]
                        field_names = ["minute", "hour", "day", "month", "weekday"]
                        for i, (part, (lo, hi), fname) in enumerate(zip(parts, ranges, field_names)):
                            if part == "*":
                                continue
                            try:
                                val = int(part)
                                if val < lo or val > hi:
                                    errors.append(f"{key}: {fname} must be {lo}-{hi}, got {val}")
                            except ValueError:
                                if not all(c in "0123456789,*-/" for c in part):
                                    errors.append(f"{key}: invalid {fname} value '{part}'")

            if field_type == "path":
                str_val = str(value)
                if "\x00" in str_val:
                    errors.append(f"{key}: path contains null bytes")

        return errors

    def _get_sources_data() -> list[dict]:
        stats = store.stats()
        demo_cold = os.environ.get("VADIMGEST_DEMO_COLD") == "1"
        sources = []
        for name in all_source_names():
            config = get_source_config(name)
            raw_config = load_config().get(name, {})
            cls = get_syncer_class(name)
            error = get_load_error(name) if cls is None else None
            state = store.get_state(name)
            stat = stats.get(name, {})

            ready_info = None
            if cls:
                try:
                    ready_info = cls.check_ready()
                except Exception:
                    ready_info = {"ok": False, "missing": ["check error"]}
                deps = cls.dependencies
                schema = cls.config_schema
                cred_help = getattr(cls, "credential_help", {})
                if schema:
                    _schema_cache[name] = schema
            else:
                deps = _STATIC_DEPS.get(name, {"python": [], "cli": [], "credentials": [], "os": []})
                schema = _schema_cache.get(name, {})
                cred_help = _STATIC_CRED_HELP.get(name, {})
                missing_list = []
                for pkg in deps.get("python", []):
                    missing_list.append(f"Python: {pkg}")
                for tool in deps.get("cli", []):
                    missing_list.append(f"CLI: {tool}")
                for cred in deps.get("credentials", []):
                    if not get_env_status([cred]).get(cred):
                        missing_list.append(f"Credential: {cred}")
                if missing_list:
                    ready_info = {"ok": False, "missing": missing_list}

            cred_keys = deps.get("credentials", [])
            env_status = get_env_status(cred_keys) if cred_keys else {}

            defaults = _SOURCE_DEFAULTS.get(name, {})

            setup_info = dict(get_setup_info(name))
            if setup_info.get("app"):
                setup_info["app_state"] = check_app(setup_info["app"])
            if setup_info.get("os_help", {}).get("kind") == "full_disk_access":
                setup_info["fda_granted"] = check_full_disk_access()
            if name == "telegram":
                setup_info["telegram_provisioned"] = bool(
                    DEFAULT_TELEGRAM_API_ID and DEFAULT_TELEGRAM_API_HASH
                )
                session_file = Path(get_data_dir()) / "credentials" / "telegram.session"
                setup_info["telegram_signed_in"] = session_file.exists()

            auth_meta = setup_info.get("auth")
            if auth_meta and auth_meta.get("method") not in (None, "telegram_phone"):
                try:
                    setup_info["auth_state"] = check_auth_state(auth_meta["method"])
                except Exception:
                    setup_info["auth_state"] = {"signed_in": False, "detail": ""}

            effective_ready = ready_info
            os_reqs = deps.get("os", []) or []
            # Does the running platform satisfy this source's OS constraint?
            os_satisfied = True
            if os_reqs and any(r.startswith("macos") for r in os_reqs) and sys.platform != "darwin":
                os_satisfied = False
                missing = list((effective_ready or {}).get("missing", []))
                missing.append(f"OS: macOS required (running on {sys.platform})")
                effective_ready = {"ok": False, "missing": missing}
            if setup_info.get("app_state") and not setup_info["app_state"]["installed"]:
                missing = list((effective_ready or {}).get("missing", []))
                missing.append(f"App: {setup_info['app_state']['display']} not installed")
                effective_ready = {"ok": False, "missing": missing}
            if setup_info.get("os_help", {}).get("kind") == "full_disk_access" and not setup_info.get("fda_granted"):
                missing = list((effective_ready or {}).get("missing", []))
                missing.append("macOS: Full Disk Access not granted")
                effective_ready = {"ok": False, "missing": missing}

            helper = setup_info.get("config_helper")
            if helper == "nextcloud_form":
                cfg = raw_config or {}
                needed = [k for k in ("server", "username", "token") if not cfg.get(k)]
                if needed:
                    missing = list((effective_ready or {}).get("missing", []))
                    missing.append("Config: " + ", ".join(needed) + " required")
                    effective_ready = {"ok": False, "missing": missing}
            elif helper == "obsidian_vault_picker":
                if not (raw_config or {}).get("vault_path"):
                    missing = list((effective_ready or {}).get("missing", []))
                    missing.append("Config: vault_path required")
                    effective_ready = {"ok": False, "missing": missing}

            if demo_cold:
                missing = []
                for pkg in deps.get("python", []) or []:
                    missing.append(f"Python package: {pkg} not installed")
                for tool in deps.get("cli", []) or []:
                    missing.append(f"CLI: {tool} not on PATH")
                for cred in deps.get("credentials", []) or []:
                    missing.append(f"Credential: {cred} not set")
                if setup_info.get("app_state"):
                    setup_info["app_state"] = {
                        **setup_info["app_state"],
                        "installed": False,
                        "path": None,
                    }
                    missing.append(f"App: {setup_info['app_state']['display']} not installed")
                if setup_info.get("os_help", {}).get("kind") == "full_disk_access":
                    setup_info["fda_granted"] = False
                    missing.append("macOS: Full Disk Access not granted")
                if setup_info.get("auth"):
                    setup_info["auth_state"] = {"signed_in": False, "detail": "", "account": None}
                if name == "telegram":
                    setup_info["telegram_provisioned"] = False
                    setup_info["telegram_signed_in"] = False
                env_status = {k: False for k in env_status}
                effective_ready = {"ok": False, "missing": missing or ["Demo mode: reset"]}

            sources.append({
                "name": name,
                "display_name": cls.display_name if cls else name.replace("_", " ").title(),
                "description": cls.description if cls else "",
                "category": cls.category if cls else "",
                "enabled": config.get("enabled", False),
                "available": cls is not None,
                "ready": effective_ready,
                "error": error,
                "records": stat.get("records", 0),
                "last_ts": state.last_ts,
                "config_schema": schema,
                "dependencies": deps,
                "os_satisfied": os_satisfied,
                "current_platform": sys.platform,
                "env_status": env_status,
                "credential_help": cred_help,
                "setup_info": setup_info,
                "current_config": {k: str(v) if isinstance(v, Path) else v
                                   for k, v in raw_config.items()},
                "defaults": {k: str(v) if isinstance(v, Path) else v
                             for k, v in defaults.items()
                             if k not in ("enabled",)},
            })
        return sources

    def _get_sync_runs(limit: int = 50) -> list[dict]:
        runs_file = store.base_path / "sync_runs.jsonl"
        if not runs_file.exists():
            return []
        runs = []
        with open(runs_file) as f:
            for line in f:
                if line.strip():
                    try:
                        runs.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return runs[-limit:]

    def _get_search_health() -> dict:
        try:
            from ..search.indexer import DEFAULT_DB
            import sqlite3
            db_path = DEFAULT_DB
            if not db_path.exists():
                return {"available": False, "reason": "Index not built"}
            conn = sqlite3.connect(str(db_path))
            try:
                row = conn.execute("SELECT COUNT(*) FROM docs").fetchone()
                total = row[0] if row else 0
                sources_row = conn.execute(
                    "SELECT source, COUNT(*) FROM docs GROUP BY source ORDER BY COUNT(*) DESC"
                ).fetchall()
                stat = conn.execute("SELECT value FROM schema_info WHERE key='last_indexed'").fetchone()
            finally:
                conn.close()
            size_bytes = db_path.stat().st_size
            return {
                "available": True,
                "db_path": str(db_path),
                "total_documents": total,
                "size_mb": round(size_bytes / 1048576, 1),
                "last_indexed": stat[0] if stat else None,
                "by_source": {r[0]: r[1] for r in sources_row},
            }
        except Exception as e:
            return {"available": False, "reason": _safe_error(e)}

    def _get_queues_data() -> dict:
        stats = store.stats()
        consumers = {}
        for f in store.checkpoints_dir.glob("*.json"):
            try:
                consumers[f.stem] = json.loads(f.read_text())
            except Exception:
                continue

        consumer_names = sorted(consumers.keys())
        source_names = sorted(all_source_names())
        rows = []
        totals = {c: 0 for c in consumer_names}
        for src in source_names:
            total = stats.get(src, {}).get("records", 0)
            pending = {}
            for c in consumer_names:
                pos = consumers[c].get("positions", {}).get(src, {})
                line = pos.get("line", 0)
                p = max(0, total - line)
                pending[c] = p
                totals[c] += p
            rows.append({"source": src, "total": total, "pending": pending})

        return {
            "consumers": consumer_names,
            "rows": rows,
            "totals": totals,
            "updated": {c: consumers[c].get("updated_at") for c in consumer_names},
        }

    def _source_observatory(sources: list[dict], runs: list[dict]) -> dict:
        now = datetime.now(timezone.utc)
        latest_by_source = {}
        for run in runs:
            src = run.get("source")
            if src:
                latest_by_source[src] = run

        items = []
        for src in sources:
            latest = latest_by_source.get(src["name"])
            last_seen = (latest or {}).get("ts") or src.get("last_ts")
            age = _hours_old(last_seen, now=now)
            reason = ""
            if not src.get("enabled"):
                status = "healthy"
                reason = "disabled"
            elif src.get("ready") and not src["ready"].get("ok", True):
                status = "broken"
                reason = ", ".join(src["ready"].get("missing", []) or ["setup required"])
            elif latest and latest.get("status") == "error":
                status = "broken"
                reason = latest.get("error") or "last sync failed"
            elif last_seen is None:
                status = "unknown"
                reason = "no sync telemetry"
            elif age is not None and age > 24:
                status = "degraded"
                reason = f"last activity {int(age)}h ago"
            else:
                status = "healthy"
                reason = "fresh"
            items.append({
                "name": src["name"],
                "display_name": src.get("display_name") or src["name"],
                "status": status,
                "enabled": src.get("enabled", False),
                "records": src.get("records", 0),
                "last_sync": (latest or {}).get("ts"),
                "last_data": src.get("last_ts"),
                "last_error": latest.get("error") if latest else None,
                "reason": reason,
                "runs_via": "vadimgest",
                "where": "server" if not any(r.startswith("macos") for r in src.get("dependencies", {}).get("os", []) or []) else "edge",
            })
        return {
            "status": _worst_status([i for i in items if i.get("enabled")]),
            "total": len(items),
            "enabled": len([i for i in items if i.get("enabled")]),
            "broken": len([i for i in items if i["status"] == "broken"]),
            "degraded": len([i for i in items if i["status"] == "degraded"]),
            "unknown": len([i for i in items if i["status"] == "unknown"]),
            "items": items,
        }

    def _edge_observatory() -> dict:
        cfg = get_edge_config()
        tokens = list_edge_tokens(store.base_path)
        state_file = store.base_path / "edge_state.json"
        state = {}
        if state_file.exists():
            try:
                state = json.loads(state_file.read_text())
            except Exception:
                state = {}

        selected_sources = cfg.get("sources") or []
        source_names = selected_sources or [
            s for s in all_source_names()
            if get_source_config(s).get("enabled", False)
        ]
        pending = []
        source_state = state.get("sources", {}) if isinstance(state.get("sources"), dict) else {}
        for source in source_names:
            uploaded_line = int((source_state.get(source) or {}).get("uploaded_line") or 0)
            total = store.count(source)
            pending.append({
                "source": source,
                "total": total,
                "uploaded_line": uploaded_line,
                "pending": max(0, total - uploaded_line),
                "updated_at": (source_state.get(source) or {}).get("updated_at"),
            })

        last_run = state.get("last_run") if isinstance(state.get("last_run"), dict) else {}
        last_run_age = _hours_old(last_run.get("finished_at"))
        token_items = []
        for token in tokens:
            age = _hours_old(token.get("last_seen_at"))
            if not token.get("active"):
                status = "healthy"
                reason = "revoked"
            elif not token.get("last_seen_at"):
                status = "unknown"
                reason = "never seen"
            elif age is not None and age > 24:
                status = "degraded"
                reason = f"last seen {int(age)}h ago"
            else:
                status = "healthy"
                reason = "recent"
            token_items.append({**token, "status": status, "reason": reason})

        local_errors = last_run.get("errors") or []
        if local_errors:
            local_status = "broken"
        elif cfg.get("enabled") and not cfg.get("server_url"):
            local_status = "broken"
        elif cfg.get("enabled") and last_run_age is not None and last_run_age > 24:
            local_status = "degraded"
        elif cfg.get("enabled") and not last_run:
            local_status = "unknown"
        else:
            local_status = "healthy"

        try:
            from .autostart import is_edge_installed
            installed = is_edge_installed()
        except Exception:
            installed = False

        parts = token_items + [{"status": local_status}]
        return {
            "status": _worst_status(parts),
            "ingest_url": request.host_url.rstrip("/") + "/api/edge/events/batch",
            "server_can_see_edge": any(t.get("last_seen_at") and t.get("active") for t in token_items),
            "edge_can_reach_server": bool(last_run.get("ok")) if last_run else None,
            "tokens": token_items,
            "local_agent": {
                "status": local_status,
                "installed": installed,
                "enabled": bool(cfg.get("enabled")),
                "device_id": cfg.get("device_id") or last_run.get("device_id"),
                "hostname": last_run.get("hostname"),
                "server_url": cfg.get("server_url"),
                "config_path": str(_find_config_file()) if _find_config_file() else None,
                "state_path": str(state_file),
                "last_run": last_run,
                "pending_total": sum(p["pending"] for p in pending),
                "sources": pending,
                "version": "1",
            },
        }

    def _klava_observatory() -> dict:
        url = os.environ.get("VADIMGEST_KLAVA_STATUS_URL", "http://127.0.0.1:18788/api/dashboard")
        data, error = _fetch_json_url(url)
        if error or not isinstance(data, dict) or data.get("error"):
            return {
                "status": "unknown",
                "url": url,
                "reachable": False,
                "error": error or (data.get("error") if isinstance(data, dict) else "unreachable"),
            }

        stats = data.get("stats") or {}
        health_score = stats.get("health_score")
        failing_jobs = data.get("failing_jobs") or []
        services = data.get("services") or []
        cron_jobs = data.get("cron_jobs") or []
        down_services = [s for s in services if not s.get("running")]
        if isinstance(health_score, (int, float)) and health_score < 50:
            status = "broken"
        elif down_services or failing_jobs or (isinstance(health_score, (int, float)) and health_score < 80):
            status = "degraded"
        else:
            status = "healthy"

        return {
            "status": status,
            "url": url,
            "reachable": True,
            "generated_at": data.get("generated_at"),
            "health_score": health_score,
            "services": {
                "total": len(services),
                "down": len(down_services),
                "items": services,
            },
            "cron": {
                "total": len(cron_jobs),
                "failing": len(failing_jobs),
                "failing_jobs": failing_jobs,
            },
            "heartbeat_backlog": len(data.get("heartbeat_backlog") or []),
            "activity": (data.get("activity") or [])[:8],
        }

    def _collect_observatory() -> dict:
        sources = _get_sources_data()
        runs = _get_sync_runs(100)
        queues = _get_queues_data()
        search = _get_search_health()
        source_status = _source_observatory(sources, runs)
        edge = _edge_observatory()
        klava = _klava_observatory()
        daemon_running = _daemon is not None and not _daemon._stop.is_set()
        queue_pending = sum(queues.get("totals", {}).values())
        queue_status = "degraded" if queue_pending > 1000 else "healthy"
        search_status = "healthy" if search.get("available") else "broken"
        server = {
            "status": "healthy",
            "data_dir": str(store.base_path),
            "config_file": str(_find_config_file()) if _find_config_file() else None,
            "dashboard": "running",
            "daemon": {
                "status": "healthy" if daemon_running else "unknown",
                "running": daemon_running,
                "started_at": _daemon_started_at if daemon_running else None,
            },
        }
        subsystems = [
            {"key": "server", "label": "Server Hub", **server},
            {"key": "edge", "label": "Edge Devices", "status": edge["status"]},
            {"key": "sources", "label": "Sources", "status": source_status["status"]},
            {"key": "search", "label": "Search", "status": search_status},
            {"key": "queues", "label": "Queues", "status": queue_status},
            {"key": "klava", "label": "Klava Processing", "status": klava["status"]},
        ]
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": _worst_status(subsystems),
            "positioning": {
                "vadimgest": "personal source-of-truth event lake, search index, and source-backed record store",
                "vadimgest_edge": "local privacy-boundary collector that pushes laptop-only records to Bakeneko",
                "klava": "operator layer that consumes vadimgest and Obsidian context, creates tasks/results, and writes durable knowledge back",
            },
            "subsystems": subsystems,
            "server": server,
            "edge": edge,
            "sources": source_status,
            "search": {"status": search_status, **search},
            "queues": {
                "status": queue_status,
                "pending_total": queue_pending,
                **queues,
            },
            "klava": klava,
            "recent_errors": [
                {"source": r.get("source"), "ts": r.get("ts"), "error": r.get("error"), "where": "vadimgest"}
                for r in reversed(runs)
                if r.get("status") == "error" or r.get("error")
            ][:12],
        }

    # ---- Routes ----

    @app.route("/")
    def index():
        return _render_dashboard()

    @app.route("/api/sources")
    def api_sources():
        return jsonify(_get_sources_data())

    @app.route("/api/fs/browse")
    def api_fs_browse():
        """List directories under a given path for the config folder picker.

        Only directories are returned. Hidden entries (dotfiles) are skipped
        unless show_hidden=1. The starting path defaults to $HOME. Returns
        parent, absolute current path, and entries with display name + path.
        """
        raw = request.args.get("path", "").strip()
        show_hidden = request.args.get("show_hidden") == "1"
        files_too = request.args.get("files") == "1"
        try:
            target = Path(raw).expanduser() if raw else Path.home()
            target = target.resolve()
        except (OSError, RuntimeError) as e:
            return jsonify({"error": f"invalid path: {_safe_error(e)}"}), 400
        if not target.exists() or not target.is_dir():
            return jsonify({"error": "not a directory", "path": str(target)}), 404
        try:
            entries = []
            for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
                if not show_hidden and child.name.startswith("."):
                    continue
                is_dir = child.is_dir()
                if not is_dir and not files_too:
                    continue
                entries.append({
                    "name": child.name,
                    "path": str(child),
                    "is_dir": is_dir,
                })
        except PermissionError as e:
            return jsonify({"error": f"permission denied: {_safe_error(e)}"}), 403
        parent = str(target.parent) if target.parent != target else None
        return jsonify({
            "path": str(target),
            "parent": parent,
            "home": str(Path.home()),
            "entries": entries,
        })

    @app.route("/api/sources/<name>", methods=["PUT"])
    def api_update_source(name):
        nonlocal _daemon, _daemon_started_at
        if name not in _SOURCE_DEFAULTS:
            return jsonify({"error": f"Unknown source: {name}"}), 404
        data = request.json or {}
        updates = {}
        if "enabled" in data:
            updates["enabled"] = bool(data["enabled"])
        if "config" in data and isinstance(data["config"], dict):
            updates.update(data["config"])

        config_fields = data.get("config", {})
        if config_fields:
            validation_errors = _validate_source_config(name, config_fields)
            if validation_errors:
                return jsonify({"ok": False, "errors": validation_errors}), 422

        if updates:
            save_source_config(name, updates)

        daemon_started = False
        if updates.get("enabled") is True:
            with _daemon_lock:
                if _daemon is None or _daemon._stop.is_set():
                    _daemon = SyncDaemon(store, interval=300)
                    _daemon.start()
                    _daemon_started_at = datetime.now().isoformat()
                    daemon_started = True

        result = {"ok": True, "saved": updates}
        if daemon_started:
            result["daemon_started"] = True
        return jsonify(result)

    @app.route("/api/sources/<name>/sync", methods=["POST"])
    def api_source_sync(name):
        if name not in _SOURCE_DEFAULTS and name not in all_source_names():
            return jsonify({"error": f"Unknown source: {name}"}), 404
        cls = get_syncer_class(name)
        if cls is None:
            return jsonify({"error": f"Source '{name}' not available (missing dependencies)"}), 400
        config = get_source_config(name)
        syncer = cls(store, config)
        try:
            count, summary = syncer.sync(limit=10000)
            syncer.log_run("ok", count=count, summary=summary)
            return jsonify({"ok": True, "count": count, "summary": summary})
        except Exception as e:
            syncer.log_run("error", error=str(e))
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/credentials", methods=["PUT"])
    def api_update_credentials():
        data = request.json or {}
        variables = {k: v for k, v in data.items() if isinstance(v, str) and v}
        if variables:
            save_env_vars(variables)
        return jsonify({"ok": True, "saved": list(variables.keys())})

    @app.route("/api/env/status")
    def api_env_status():
        keys = request.args.get("keys", "").split(",")
        keys = [k.strip() for k in keys if k.strip()]
        return jsonify(get_env_status(keys))

    @app.route("/api/stats")
    def api_stats():
        return jsonify(store.stats())

    @app.route("/api/runs")
    def api_runs():
        limit = request.args.get("limit", 50, type=int)
        return jsonify(_get_sync_runs(limit))

    @app.route("/api/consumers")
    def api_consumers():
        consumers = {}
        for f in store.checkpoints_dir.glob("*.json"):
            data = json.loads(f.read_text())
            consumers[f.stem] = data
        return jsonify(consumers)

    @app.route("/api/queues")
    def api_queues():
        return jsonify(_get_queues_data())

    @app.route("/api/observatory")
    def api_observatory():
        return jsonify(_collect_observatory())

    @app.route("/api/sync", methods=["POST"])
    def api_sync():
        source = request.json.get("source") if request.json else None
        if not source:
            return jsonify({"error": "source required"}), 400

        from ..cli import sync_source
        def _run():
            entry = {"ts": datetime.now().isoformat(), "source": source, "status": "running"}
            with _sync_lock:
                _sync_log.append(entry)
            try:
                count, error = sync_source(store, source)
                entry["status"] = "error" if error else "ok"
                entry["count"] = count
                if error:
                    entry["error"] = error
            except Exception as e:
                entry["status"] = "error"
                entry["error"] = str(e)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return jsonify({"ok": True, "message": f"Sync started for {source}"})

    @app.route("/api/edge/events/batch", methods=["POST"])
    def api_edge_events_batch():
        """Receive a batch from a local edge-agent.

        Edge agents run on machines that can see local-only sources such as
        iMessage, browser state, Dayflow, or local files. The server keeps the
        canonical append-only vadimgest store; this endpoint is the stable
        boundary local collectors push through.
        """
        auth = request.headers.get("Authorization", "")
        prefix = "Bearer "
        if not auth.startswith(prefix):
            return jsonify({"ok": False, "error": "edge bearer token required"}), 401
        try:
            verify_edge_token(auth[len(prefix):], store.base_path)
        except EdgeAuthError as e:
            return jsonify({"ok": False, "error": str(e)}), 401

        payload = request.get_json(silent=True)
        try:
            result = ingest_edge_batch(store, payload)
        except EdgeIngestError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        status = 207 if result.errors else 200
        return jsonify(result.to_dict()), status

    @app.route("/api/edge/tokens", methods=["GET", "POST"])
    def api_edge_tokens():
        if request.method == "GET":
            return jsonify({"tokens": list_edge_tokens(store.base_path)})
        data = request.json or {}
        issued = create_edge_token(data.get("label", ""), store.base_path)
        return jsonify({"ok": True, "token": issued.token, "metadata": issued.metadata})

    @app.route("/api/edge/tokens/<token_id>", methods=["DELETE"])
    def api_edge_token_delete(token_id):
        if not revoke_edge_token(token_id, store.base_path):
            return jsonify({"ok": False, "error": "token not found"}), 404
        return jsonify({"ok": True})

    @app.route("/api/edge/status", methods=["GET"])
    def api_edge_status():
        return jsonify({
            "ok": True,
            "ingest_url": request.host_url.rstrip("/") + "/api/edge/events/batch",
            "tokens": list_edge_tokens(store.base_path),
            "config": get_edge_config(),
            "stats": store.stats(),
        })

    @app.route("/api/edge/config", methods=["GET", "PUT"])
    def api_edge_config():
        if request.method == "GET":
            return jsonify(get_edge_config())
        data = request.json or {}
        updates = {k: data[k] for k in ("enabled", "server_url", "device_id", "interval_seconds", "batch_size", "sources") if k in data}
        if updates:
            save_edge_config(updates)
        token = data.get("token")
        if token:
            save_env_vars({"VADIMGEST_EDGE_TOKEN": str(token).strip()})
        return jsonify({"ok": True, "config": get_edge_config()})

    @app.route("/api/edge/test", methods=["POST"])
    def api_edge_test():
        data = request.json or {}
        cfg = get_edge_config()
        server_url = str(data.get("server_url") or cfg.get("server_url") or "").rstrip("/")
        token = str(data.get("token") or os.environ.get("VADIMGEST_EDGE_TOKEN") or "").strip()
        if not server_url or not token:
            return jsonify({"ok": False, "error": "server_url and token are required"}), 400
        payload = {
            "device_id": str(data.get("device_id") or cfg.get("device_id") or ""),
            "source": "edge_test",
            "events": [],
        }
        try:
            status, body = _default_transport(server_url + "/api/edge/events/batch", token, payload, 15)
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 502
        if status not in (200, 207):
            return jsonify({"ok": False, "status": status, "error": body.get("error") or "connection failed"}), 502
        return jsonify({"ok": True, "status": status, "response": body})

    @app.route("/api/edge/agent/run-once", methods=["POST"])
    def api_edge_agent_run_once():
        try:
            result = EdgeAgent(store).run_once().to_dict()
            return jsonify(result), (200 if result.get("ok") else 500)
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/edge/agent", methods=["GET"])
    def api_edge_agent_status():
        from .autostart import is_edge_installed
        return jsonify({
            "installed": is_edge_installed(),
            "config": get_edge_config(),
        })

    @app.route("/api/edge/agent/install", methods=["POST", "DELETE"])
    def api_edge_agent_install():
        from .autostart import install_edge, uninstall_edge
        try:
            if request.method == "POST":
                interval = int((request.json or {}).get("interval", get_edge_config().get("interval_seconds", 300)))
                install_edge(interval=interval)
            else:
                uninstall_edge()
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/config", methods=["GET"])
    def api_config():
        config_file = _find_config_file()
        return jsonify({
            "config_file": str(config_file) if config_file else None,
            "data_dir": str(get_data_dir()),
            "env_file": str(get_env_file_path()),
            "has_config": config_file is not None,
        })

    @app.route("/api/config/init", methods=["POST"])
    def api_config_init():
        path = ensure_config_file()
        return jsonify({"ok": True, "path": str(path)})

    @app.route("/api/config/global", methods=["GET", "PUT"])
    def api_global_config():
        config = load_config()
        if request.method == "GET":
            conv = config.get("conversation", {})
            return jsonify({
                "self_names": config.get("self_names", []),
                "time_window_hours": conv.get("time_window_hours", 4),
                "min_messages_per_chunk": conv.get("min_messages_per_chunk", 3),
                "max_messages_per_chunk": conv.get("max_messages_per_chunk", 100),
            })
        data = request.json or {}
        config_file = ensure_config_file()
        with open(config_file) as f:
            raw = yaml.safe_load(f) or {}
        if "self_names" in data:
            raw["self_names"] = [n.strip() for n in data["self_names"] if n.strip()]
        conv_keys = ("time_window_hours", "min_messages_per_chunk", "max_messages_per_chunk")
        conv_updates = {k: int(data[k]) for k in conv_keys if k in data}
        if conv_updates:
            if "conversation" not in raw:
                raw["conversation"] = {}
            raw["conversation"].update(conv_updates)
        with open(config_file, "w") as f:
            yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        load_config.cache_clear()
        return jsonify({"ok": True})

    @app.route("/api/autostart", methods=["GET", "POST", "DELETE"])
    def api_autostart():
        from .autostart import is_installed, install, uninstall
        if request.method == "GET":
            return jsonify({"installed": is_installed()})
        elif request.method == "POST":
            data = request.json or {}
            port = data.get("port", 8484)
            interval = data.get("interval", 300)
            try:
                install(port=port, interval=interval)
                return jsonify({"ok": True})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500
        else:
            try:
                uninstall(keep_running=True)
                return jsonify({"ok": True})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/daemon")
    def api_daemon():
        nonlocal _daemon, _daemon_started_at
        with _daemon_lock:
            running = _daemon is not None and not _daemon._stop.is_set()
            return jsonify({
                "running": running,
                "interval": _daemon.interval if _daemon else 300,
                "sources": _daemon._get_sync_sources() if _daemon else [],
                "started_at": _daemon_started_at if running else None,
            })

    @app.route("/api/daemon/start", methods=["POST"])
    def api_daemon_start():
        nonlocal _daemon, _daemon_started_at
        data = request.get_json(silent=True) or {}
        interval = data.get("interval", 300)
        try:
            with _daemon_lock:
                if _daemon is not None and not _daemon._stop.is_set():
                    return jsonify({"ok": True, "message": "Already running"})
                _daemon = SyncDaemon(store, interval=interval)
                _daemon.start()
                _daemon_started_at = datetime.now().isoformat()
            return jsonify({"ok": True, "message": "Daemon started"})
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/daemon/stop", methods=["POST"])
    def api_daemon_stop():
        nonlocal _daemon, _daemon_started_at
        with _daemon_lock:
            if _daemon is None or _daemon._stop.is_set():
                return jsonify({"ok": True, "message": "Not running"})
            _daemon.stop()
            _daemon = None
            _daemon_started_at = None
        return jsonify({"ok": True, "message": "Daemon stopped"})

    @app.route("/api/install", methods=["POST"])
    def api_install():
        import subprocess
        import sys
        import shutil

        data = request.json or {}
        package = data.get("package")
        method = data.get("method", "pip")

        if not package:
            return jsonify({"error": "package required"}), 400

        def _run(cmd, timeout=180):
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result

        def _clear_caches():
            from ..ingest.sources import _failed, _loaded
            _failed.clear()
            _loaded.clear()

        try:
            if method == "pip":
                allowed_pip = {
                    "telethon", "python-dotenv", "pysqlite3", "sqlite-vec",
                    "httpx", "flask", "linkedin-api", "requests", "playwright",
                }
                if package not in allowed_pip:
                    return jsonify({"error": f"pip package '{package}' not allowed"}), 400

                cmd = [sys.executable, "-m", "pip", "install", package]
                result = _run(cmd)
                if result.returncode != 0 and "No module named pip" in (result.stderr or ""):
                    _run([sys.executable, "-m", "ensurepip", "--user"], timeout=120)
                    result = _run(cmd)
                if result.returncode != 0 and "externally-managed" in (result.stderr or ""):
                    cmd.append("--break-system-packages")
                    result = _run(cmd)

            elif method == "brew_setup":
                if shutil.which("brew"):
                    return jsonify({"ok": True, "output": "Homebrew already installed"})
                cmd = ["/bin/bash", "-c",
                       'NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"']
                result = _run(cmd, timeout=600)

            elif method == "npm_setup":
                if shutil.which("npm"):
                    return jsonify({"ok": True, "output": "npm already installed"})
                if not shutil.which("brew"):
                    return jsonify({"ok": False, "error": "needs_brew",
                                    "install_cmd": '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'})
                cmd = ["brew", "install", "node"]
                result = _run(cmd, timeout=300)

            elif method == "brew":
                allowed_brew = {
                    "sigtop": {"formula": "tbvdm/tap/sigtop", "head": True},
                    "wacli": {"formula": "steipete/tap/wacli"},
                    "gog": {"formula": "steipete/tap/gogcli"},
                    "gh": {"formula": "gh"},
                }
                if package not in allowed_brew:
                    return jsonify({"error": f"brew package '{package}' not allowed"}), 400
                if not shutil.which("brew"):
                    return jsonify({"ok": False, "error": "needs_brew",
                                    "install_cmd": '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'})
                info = allowed_brew[package]
                cmd = ["brew", "install"]
                if info.get("head"):
                    cmd.append("--HEAD")
                cmd.append(info["formula"])
                result = _run(cmd, timeout=300)

            elif method == "pipx_setup":
                if shutil.which("pipx") or shutil.which("uv"):
                    return jsonify({"ok": True, "output": "pipx already installed"})
                if shutil.which("brew"):
                    result = _run(["brew", "install", "pipx"], timeout=300)
                    if result.returncode == 0:
                        subprocess.run(["pipx", "ensurepath"], capture_output=True, text=True, timeout=30)
                else:
                    cmd = [sys.executable, "-m", "pip", "install", "--user", "pipx"]
                    result = _run(cmd)
                    if result.returncode != 0 and "No module named pip" in (result.stderr or ""):
                        _run([sys.executable, "-m", "ensurepip", "--user"], timeout=120)
                        result = _run(cmd)
                    if result.returncode != 0 and "externally-managed" in (result.stderr or ""):
                        cmd.append("--break-system-packages")
                        result = _run(cmd)

            elif method == "pipx":
                allowed_pipx = {"bird-cli"}
                if package not in allowed_pipx:
                    return jsonify({"error": f"pipx package '{package}' not allowed"}), 400
                pipx = shutil.which("pipx") or shutil.which("uv")
                if not pipx:
                    return jsonify({"ok": False, "error": "needs_pipx",
                                    "install_cmd": "brew install pipx"})
                installed_binary = {"bird-cli": "bird"}.get(package, package)
                if shutil.which(installed_binary):
                    _clear_caches()
                    return jsonify({"ok": True, "output": f"{package} already installed"})
                tool = "pipx" if "pipx" in pipx else "uv"
                cmd = [pipx, "tool", "install", package] if tool == "uv" else [pipx, "install", package]
                result = _run(cmd)

            elif method == "npm":
                allowed_npm = {"@steipete/bird"}
                if package not in allowed_npm:
                    return jsonify({"error": f"npm package '{package}' not allowed"}), 400
                npm = shutil.which("npm")
                if not npm:
                    return jsonify({"ok": False, "error": "needs_npm",
                                    "install_cmd": "brew install node"})
                cmd = [npm, "install", "-g", package]
                result = _run(cmd, timeout=120)

            else:
                return jsonify({"error": f"Unknown method: {method}"}), 400

            if result.returncode == 0:
                _clear_caches()
                output = (result.stdout + result.stderr)[-500:]
                return jsonify({"ok": True, "output": output})
            else:
                return jsonify({"ok": False, "error": result.stderr[-500:]}), 500

        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": "Install timed out"}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    # ---- Wizard setup endpoints ----

    @app.route("/api/apps/check", methods=["GET"])
    def api_apps_check():
        key = request.args.get("key", "")
        if not key:
            return jsonify({"error": "key required"}), 400
        return jsonify(check_app(key))

    @app.route("/api/fda/check", methods=["GET"])
    def api_fda_check():
        return jsonify({"granted": check_full_disk_access()})

    @app.route("/api/obsidian/vaults", methods=["GET"])
    def api_obsidian_vaults():
        try:
            vaults = scan_obsidian_vaults()
            return jsonify({"vaults": vaults})
        except Exception as e:
            return jsonify({"vaults": [], "error": _safe_error(e)}), 500

    @app.route("/api/nextcloud/test", methods=["POST"])
    def api_nextcloud_test():
        data = request.json or {}
        result = test_nextcloud(
            server=(data.get("server") or "").strip(),
            username=(data.get("username") or "").strip(),
            token=(data.get("token") or "").strip(),
        )
        return jsonify(result)

    # ---- CLI auth sessions (gh / gog / wacli / linkedin) ----

    @app.route("/api/auth/cli/start", methods=["POST"])
    def api_auth_cli_start():
        import shutil as _sh
        data = request.json or {}
        method = data.get("method") or ""
        source_name = data.get("source") or method
        account = data.get("account") or ""

        spec = AUTH_COMMANDS.get(method)
        if not spec:
            return jsonify({"ok": False, "error": f"Unknown auth method: {method}"}), 400

        if "command_fn" in spec:
            if not account:
                return jsonify({"ok": False, "error": "account required"}), 400
            command = spec["command_fn"](account)
        else:
            command = list(spec["command"])

        if not _sh.which(command[0]):
            return jsonify({
                "ok": False,
                "error": f"CLI tool '{command[0]}' not installed",
                "needs_install": command[0],
            }), 400

        try:
            sess = AUTH_MANAGER.create(
                source_name=source_name,
                command=command,
                parser=spec.get("parser", "generic"),
                env=spec.get("env"),
            )
            return jsonify({"ok": True, "session": sess.snapshot()})
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/auth/cli/stream/<sid>")
    def api_auth_cli_stream(sid):
        sess = AUTH_MANAGER.get(sid)
        if not sess:
            return jsonify({"error": "session not found"}), 404

        def stream():
            last_snapshot = None
            while True:
                snap = sess.snapshot()
                snap_key = (len(snap["lines"]), snap["done"], snap["device_code"],
                            snap["verification_url"], snap["qr_text"], snap["summary"])
                if snap_key != last_snapshot:
                    yield f"data: {json.dumps(snap)}\n\n"
                    last_snapshot = snap_key
                if snap["done"]:
                    return
                time.sleep(0.5)

        return Response(stream(), mimetype="text/event-stream")

    @app.route("/api/auth/cli/snapshot/<sid>", methods=["GET"])
    def api_auth_cli_snapshot(sid):
        sess = AUTH_MANAGER.get(sid)
        if not sess:
            return jsonify({"error": "session not found"}), 404
        return jsonify(sess.snapshot())

    @app.route("/api/auth/cli/input/<sid>", methods=["POST"])
    def api_auth_cli_input(sid):
        sess = AUTH_MANAGER.get(sid)
        if not sess:
            return jsonify({"error": "session not found"}), 404
        data = request.json or {}
        text = data.get("text", "")
        ok = sess.send_input(text)
        return jsonify({"ok": ok})

    @app.route("/api/auth/cli/stop/<sid>", methods=["POST"])
    def api_auth_cli_stop(sid):
        sess = AUTH_MANAGER.get(sid)
        if not sess:
            return jsonify({"error": "session not found"}), 404
        sess.stop()
        return jsonify({"ok": True})

    # ---- LinkedIn auth (launch headed Playwright browser) ----

    @app.route("/api/auth/linkedin/launch", methods=["POST"])
    def api_auth_linkedin_launch():
        import subprocess
        import shutil as _sh
        if not _sh.which("python3"):
            return jsonify({"ok": False, "error": "python3 not found"}), 400
        try:
            import playwright  # noqa: F401
        except ImportError:
            return jsonify({"ok": False, "error": "playwright not installed - install it first"}), 400
        script = "\n".join([
            "from playwright.sync_api import sync_playwright",
            "import os",
            "pw = sync_playwright().start()",
            "b = pw.chromium.launch_persistent_context(",
            "    user_data_dir=os.path.expanduser('~/.linkedin_browser'),",
            "    headless=False, viewport={'width':1280,'height':900},",
            "    args=['--disable-blink-features=AutomationControlled','--no-sandbox'])",
            "p = b.pages[0] if b.pages else b.new_page()",
            "p.goto('https://www.linkedin.com/login', wait_until='domcontentloaded', timeout=30000)",
            "try:",
            "    p.wait_for_url('**/feed/**', timeout=300000)",
            "except: pass",
            "b.close()",
            "pw.stop()",
        ])
        subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return jsonify({"ok": True})

    # ---- Telegram auth (in-dashboard SMS flow) ----

    @app.route("/api/telegram/auth/send-code", methods=["POST"])
    def api_telegram_send_code():
        data = request.json or {}
        phone = (data.get("phone") or "").strip()
        if not phone:
            return jsonify({"ok": False, "error": "phone required"}), 400

        api_id = (data.get("api_id") or DEFAULT_TELEGRAM_API_ID or "").strip()
        api_hash = (data.get("api_hash") or DEFAULT_TELEGRAM_API_HASH or "").strip()

        env_status = get_env_status(["TELEGRAM_API_ID", "TELEGRAM_API_HASH"])
        if not api_id or not api_hash:
            import os as _os
            api_id = api_id or _os.environ.get("TELEGRAM_API_ID", "")
            api_hash = api_hash or _os.environ.get("TELEGRAM_API_HASH", "")

        if not api_id or not api_hash:
            return jsonify({
                "ok": False,
                "error": "No Telegram API credentials available. Provide api_id/api_hash or set VADIMGEST_SHARED_TELEGRAM_API_ID/HASH.",
            }), 400

        creds_dir = Path(get_data_dir()) / "credentials"
        creds_dir.mkdir(parents=True, exist_ok=True)
        session_path = str(creds_dir / "telegram")

        try:
            sess = TELEGRAM_AUTH.start(phone, api_id, api_hash, session_path)
            snap = sess.snapshot()
            if snap.get("error") and not snap.get("done"):
                return jsonify({"ok": False, "error": snap["error"], "session_id": sess.id}), 500
            return jsonify({"ok": True, "session_id": sess.id, "session": snap})
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/telegram/auth/verify", methods=["POST"])
    def api_telegram_verify():
        data = request.json or {}
        sid = data.get("session_id", "")
        code = (data.get("code") or "").strip()
        password = data.get("password") or None
        if not sid:
            return jsonify({"ok": False, "error": "session_id required"}), 400

        result = TELEGRAM_AUTH.verify(sid, code, password)
        if result.get("ok"):
            api_id = (data.get("api_id") or DEFAULT_TELEGRAM_API_ID or "").strip()
            api_hash = (data.get("api_hash") or DEFAULT_TELEGRAM_API_HASH or "").strip()
            if api_id and api_hash:
                save_env_vars({"TELEGRAM_API_ID": api_id, "TELEGRAM_API_HASH": api_hash})
            save_source_config("telegram", {"enabled": True})
        return jsonify(result)

    @app.route("/api/telegram/auth/cancel", methods=["POST"])
    def api_telegram_cancel():
        data = request.json or {}
        sid = data.get("session_id", "")
        if sid:
            TELEGRAM_AUTH.cancel(sid)
        return jsonify({"ok": True})

    # ---- LinkedIn bootstrap (playwright install + login) ----

    @app.route("/api/linkedin/bootstrap", methods=["POST"])
    def api_linkedin_bootstrap():
        """Install playwright chromium if missing."""
        import subprocess as _sp, sys as _sys
        try:
            result = _sp.run(
                [_sys.executable, "-m", "playwright", "install", "chromium"],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0:
                return jsonify({"ok": True, "output": (result.stdout + result.stderr)[-500:]})
            return jsonify({"ok": False, "error": result.stderr[-500:]}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/events")
    def api_events():
        def stream():
            last_stats = None
            while True:
                stats = store.stats()
                if stats != last_stats:
                    yield f"data: {json.dumps({'type': 'stats', 'data': stats})}\n\n"
                    last_stats = stats
                time.sleep(5)
        return Response(stream(), mimetype="text/event-stream")

    # ---- Data Explorer API ----

    @app.route("/api/data/overview")
    def api_data_overview():
        stats = store.stats()
        result = []
        for source_file in sorted(store.sources_dir.glob("*.jsonl")):
            name = source_file.stem
            size = source_file.stat().st_size
            stat = stats.get(name, {})
            records = stat.get("records", 0)

            first_ts = last_ts = None
            types = {}

            try:
                with open(source_file) as f:
                    first_line = f.readline().strip()
                    if first_line:
                        rec = json.loads(first_line)
                        first_ts = rec.get("_ingested_at", "")[:19]
            except Exception:
                pass

            if size > 0:
                try:
                    with open(source_file, "rb") as f:
                        f.seek(max(0, size - 8192))
                        chunk = f.read().decode(errors="replace").strip().split("\n")
                        for ll in reversed(chunk):
                            ll = ll.strip()
                            if not ll:
                                continue
                            try:
                                rec = json.loads(ll)
                                last_ts = rec.get("_ingested_at", "")[:19]
                                break
                            except Exception:
                                pass
                except Exception:
                    pass

            try:
                with open(source_file) as f:
                    for i, line in enumerate(f):
                        if i >= 1000:
                            break
                        try:
                            rec = json.loads(line)
                            t = rec.get("type", "unknown")
                            types[t] = types.get(t, 0) + 1
                        except Exception:
                            pass
            except Exception:
                pass

            result.append({
                "name": name,
                "records": records,
                "size_bytes": size,
                "first_ts": first_ts,
                "last_ts": last_ts,
                "types": types,
            })

        total_records = sum(r["records"] for r in result)
        total_size = sum(r["size_bytes"] for r in result)
        return jsonify({"sources": result, "total_records": total_records, "total_size": total_size})

    @app.route("/api/data/browse")
    def api_data_browse():
        source = request.args.get("source")
        if not source:
            return jsonify({"error": "source required"}), 400

        offset = request.args.get("offset", 0, type=int)
        limit = min(request.args.get("limit", 20, type=int), 100)

        source_file = store.sources_dir / f"{source}.jsonl"
        if not source_file.resolve().is_relative_to(store.sources_dir.resolve()):
            return jsonify({"error": "Invalid source name"}), 400
        if not source_file.exists():
            return jsonify({"error": f"No data for {source}"}), 404

        records = []
        with open(source_file) as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
                if i >= offset + limit:
                    break
                try:
                    rec = json.loads(line)
                    for key in list(rec.keys()):
                        val = rec[key]
                        if isinstance(val, str) and len(val) > 500:
                            rec[key] = val[:500] + "..."
                        elif isinstance(val, list) and len(val) > 10:
                            rec[key] = val[:10] + [f"... +{len(val)-10} more"]
                    records.append(rec)
                except Exception:
                    pass

        total = store.count(source)
        return jsonify({"records": records, "total": total, "offset": offset, "limit": limit})

    @app.route("/api/data/search")
    def api_data_search():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"error": "query required"}), 400

        source = request.args.get("source")
        limit = min(request.args.get("limit", 20, type=int), 100)

        try:
            from ..search import search as fts_search
            kwargs = {"n": limit, "raw": True}
            if source:
                kwargs["source"] = source

            results = fts_search(q, **kwargs)
            items = []
            for r in results:
                items.append({
                    "source": r.source,
                    "title": r.title,
                    "snippet": r.snippet,
                    "path": r.path,
                    "chat": r.chat,
                    "folder": r.folder,
                })
            return jsonify({"results": items, "query": q})
        except ImportError:
            return jsonify({"error": "Search index not available"}), 500
        except Exception as e:
            return jsonify({"error": _safe_error(e)}), 500

    @app.route("/api/search/config", methods=["GET"])
    def api_search_config_get():
        return jsonify(get_search_config())

    @app.route("/api/search/config", methods=["PUT"])
    def api_search_config_put():
        data = request.json or {}
        allowed = {"vault_path", "skills_dir", "index_db", "embedding_provider", "exclude_sources", "ollama_url", "ollama_model"}
        updates = {k: v for k, v in data.items() if k in allowed}
        if not updates:
            return jsonify({"error": "No valid fields to update"}), 400
        save_search_config(updates)
        return jsonify({"ok": True, "saved": updates})

    @app.route("/api/search/reindex", methods=["POST"])
    def api_search_reindex():
        try:
            from ..search import index as run_index
            cfg = get_search_config()
            rebuild = request.json.get("rebuild", False) if request.json else False
            results = run_index(
                vault=Path(cfg["vault_path"]),
                jsonl_dir=store.sources_dir,
                db_path=Path(cfg["index_db"]),
                skills_dir=Path(cfg["skills_dir"]),
                rebuild=rebuild,
            )
            return jsonify({"ok": True, "results": results})
        except Exception as e:
            return jsonify({"ok": False, "error": _safe_error(e)}), 500

    @app.route("/api/search/health")
    def api_search_health():
        return jsonify(_get_search_health())

    return app


def _render_dashboard() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>vadimgest</title>
<script>const t=localStorage.getItem('vg-theme');if(t)document.documentElement.setAttribute('data-theme',t);</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root, [data-theme="dark"] {
  --bg: #09090b;
  --bg2: #18181b;
  --bg3: #27272a;
  --border: #27272a;
  --border-hover: #3f3f46;
  --text: #fafafa;
  --text2: #a1a1aa;
  --text3: #71717a;
  --accent: #34d399;
  --accent-hover: #6ee7b7;
  --accent-glow: rgba(52,211,153,0.12);
  --accent2: #a78bfa;
  --green: #34d399;
  --yellow: #fbbf24;
  --red: #f87171;
  --green-bg: rgba(52,211,153,0.12);
  --yellow-bg: rgba(251,191,36,0.12);
  --red-bg: rgba(248,113,113,0.12);
  --gray-bg: rgba(113,113,122,0.12);
  --card-shadow: none;
  --card-shadow-hover: none;
  --mono: 'SF Mono', 'Fira Code', 'JetBrains Mono', 'Cascadia Code', monospace;
  --radius: 12px;
  --radius-sm: 8px;
}
[data-theme="light"] {
  --bg: #fafafa;
  --bg2: #ffffff;
  --bg3: #f4f4f5;
  --border: #e4e4e7;
  --border-hover: #d4d4d8;
  --text: #09090b;
  --text2: #52525b;
  --text3: #a1a1aa;
  --accent: #10b981;
  --accent-hover: #059669;
  --accent-glow: transparent;
  --accent2: #8b5cf6;
  --green: #10b981;
  --yellow: #f59e0b;
  --red: #ef4444;
  --green-bg: rgba(16,185,129,0.08);
  --yellow-bg: rgba(245,158,11,0.08);
  --red-bg: rgba(239,68,68,0.06);
  --gray-bg: rgba(161,161,170,0.08);
  --card-shadow: none;
  --card-shadow-hover: none;
}

* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  background: var(--bg);
  color: var(--text);
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
}

/* ---- Header ---- */
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 32px;
  border-bottom: 1px solid var(--border);
  background: var(--bg2);
  position: sticky;
  top: 0;
  z-index: 50;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 28px;
}
.logo {
  font-size: 17px;
  font-weight: 600;
  letter-spacing: 2px;
  text-transform: lowercase;
  color: var(--text);
}
.logo span { color: var(--accent); font-weight: 700; }
.header-stats {
  display: flex;
  gap: 24px;
}
.header-stat {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
}
.header-stat-value {
  font-family: var(--mono);
  font-size: 16px;
  font-weight: 500;
  color: var(--text);
}
.header-stat-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text3);
}
.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}
.theme-btn {
  background: none;
  border: 1px solid var(--border);
  color: var(--text2);
  width: 36px;
  height: 36px;
  border-radius: 8px;
  cursor: pointer;
  font-size: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}
.theme-btn:hover {
  border-color: var(--border-hover);
  color: var(--text);
}

/* ---- Daemon Status ---- */
.daemon-status {
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  padding: 6px 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 12px;
  color: var(--text2);
  transition: all 0.2s;
}
.daemon-status:hover {
  border-color: var(--border-hover);
  color: var(--text);
}
.daemon-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #555;
  transition: background 0.2s;
}
.daemon-dot.running {
  background: #4ade80;
}

/* ---- Tabs ---- */
.tabs {
  display: flex;
  gap: 0;
  padding: 0 32px;
  border-bottom: 1px solid var(--border);
  background: var(--bg);
}
.tab {
  padding: 11px 22px;
  cursor: pointer;
  color: var(--text3);
  font-size: 13px;
  font-weight: 500;
  border-bottom: 2px solid transparent;
  transition: all 0.2s;
  user-select: none;
  letter-spacing: 0.3px;
}
.tab:hover { color: var(--text); }
.tab.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
}

/* ---- Content ---- */
.content { padding: 28px 36px; max-width: 1440px; }
.tab-content { display: none; }
.tab-content.active { display: block; }

/* ---- Banner ---- */
.banner {
  background: linear-gradient(135deg, var(--bg3) 0%, var(--bg2) 100%);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 36px;
  text-align: center;
  margin-bottom: 24px;
  box-shadow: var(--card-shadow);
}
.banner h2 {
  font-size: 22px;
  font-weight: 600;
  margin-bottom: 10px;
}
.banner p {
  color: var(--text2);
  margin-bottom: 22px;
  font-size: 15px;
}

/* ---- Panels ---- */
.panel {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 18px;
  margin-bottom: 16px;
  transition: border-color 0.15s;
}
.panel:hover { border-color: var(--border-hover); }
.panel-title {
  display: flex;
  align-items: center;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text3);
  margin-bottom: 14px;
  font-weight: 600;
  gap: 8px;
}

/* ---- Source Cards (Klava-style) ---- */
.src-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 10px;
}
.src-card {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  transition: border-color 0.15s;
  cursor: pointer;
}
.src-card:hover { border-color: var(--border-hover); }
.src-card.disabled { opacity: 0.45; }
.src-card.warn { border-color: rgba(251,191,36,0.35); }
.src-header {
  display: flex;
  align-items: center;
  gap: 8px;
}
.src-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.src-dot.active { background: var(--green); box-shadow: 0 0 6px rgba(52,211,153,0.4); }
.src-dot.warn { background: var(--yellow); }
.src-dot.off { background: var(--text3); }
.src-name {
  font-weight: 600;
  font-size: 13px;
  flex: 1;
  color: var(--text);
}
.src-badge {
  font-size: 9px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  padding: 2px 7px;
  border-radius: 4px;
}
.src-badge.active { background: var(--green-bg); color: var(--green); }
.src-badge.warn { background: var(--yellow-bg); color: var(--yellow); }
.src-badge.off { background: var(--bg3); color: var(--text3); }
.src-desc {
  font-size: 12px;
  color: var(--text2);
  line-height: 1.4;
}
.src-stats {
  display: flex;
  gap: 12px;
  font-size: 11px;
  font-family: var(--mono);
  color: var(--text3);
}
.src-stats .highlight {
  color: var(--text2);
  font-weight: 500;
}
.src-bar {
  height: 3px;
  background: var(--bg3);
  border-radius: 2px;
}
.src-bar-fill {
  height: 100%;
  border-radius: 2px;
  background: var(--accent2);
  transition: width 0.3s ease;
}
.src-card.disabled .src-bar-fill { background: var(--text3); }
.src-card.warn .src-bar-fill { background: var(--yellow); }
.src-missing {
  font-size: 11px;
  color: var(--red);
  line-height: 1.5;
}
.src-missing span {
  display: block;
  padding-left: 10px;
  position: relative;
}
.src-missing span::before {
  content: '-';
  position: absolute;
  left: 0;
}
.src-deps {
  font-size: 11px;
  color: var(--text3);
  border-top: 1px solid var(--border);
  padding-top: 8px;
  margin-top: 2px;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.src-deps strong {
  color: var(--text2);
  font-weight: 500;
}

/* ---- Legacy compat ---- */
.category-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text3);
  margin: 28px 0 14px;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 8px;
}
.category-label:first-child { margin-top: 0; }
.cards-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 10px;
}

/* ---- Badges ---- */
.badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 6px;
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.3px;
}
.badge-green { background: var(--green-bg); color: var(--green); }
.badge-yellow { background: var(--yellow-bg); color: var(--yellow); }
.badge-red { background: var(--red-bg); color: var(--red); }
.badge-gray { background: var(--gray-bg); color: var(--text3); }

/* ---- Buttons ---- */
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 8px 16px;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  font-family: inherit;
  cursor: pointer;
  transition: all 0.2s;
  border: 1px solid var(--border);
  background: var(--bg3);
  color: var(--text);
  gap: 6px;
}
.btn:hover { border-color: var(--accent); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-primary {
  background: var(--accent);
  color: #fff;
  border-color: var(--accent);
  font-weight: 600;
}
.btn-primary:hover { background: var(--accent-hover); border-color: var(--accent-hover); box-shadow: 0 2px 8px rgba(16,185,129,0.25); }
.btn-sm { padding: 4px 10px; font-size: 11px; border-radius: 6px; }
.btn-install {
  padding: 3px 8px;
  font-size: 10px;
  border-radius: 5px;
  background: var(--accent);
  color: #fff;
  border: none;
  cursor: pointer;
  font-weight: 500;
  font-family: inherit;
}
.btn-install:hover { background: var(--accent-hover); }
.btn-install:disabled { opacity: 0.5; cursor: not-allowed; }

/* ---- Tables ---- */
.runs-table, .queues-table {
  width: 100%;
  border-collapse: collapse;
}
.runs-table th, .queues-table th {
  text-align: left;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text3);
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  font-weight: 500;
}
.runs-table td, .queues-table td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
}
.runs-table tr:hover, .queues-table tr:hover {
  background: var(--bg3);
}
.runs-table .mono, .queues-table .mono {
  font-family: var(--mono);
  font-size: 12px;
}
.queues-table .q-zero { color: var(--text3); }
.queues-table .q-green { color: var(--green); font-weight: 500; }
.queues-table .q-yellow { color: var(--yellow); font-weight: 500; }
.queues-table .q-red { color: var(--red); font-weight: 600; }
.queues-table .total-row td {
  font-weight: 600;
  border-top: 2px solid var(--border);
}
.queues-table .updated-row td {
  font-size: 11px;
  color: var(--text3);
  border-bottom: none;
}

/* ---- Activity Feed ---- */
.health-bar {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}
.health-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  border-radius: 20px;
  font-size: 11px;
  font-weight: 500;
  background: var(--bg);
  border: 1px solid var(--border);
  cursor: default;
  transition: border-color 0.15s;
}
.health-chip .h-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  flex-shrink: 0;
}
.health-chip.h-ok .h-dot { background: var(--green); }
.health-chip.h-stale .h-dot { background: var(--yellow); }
.health-chip.h-error .h-dot { background: var(--red); }
.health-chip.h-error { border-color: var(--red); background: var(--red-bg); }
.health-chip.h-never .h-dot { background: var(--text3); }

.activity-entry {
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  margin-bottom: 8px;
  overflow: hidden;
  background: var(--bg);
  transition: border-color 0.15s;
}
.activity-entry:hover { border-color: var(--border-hover); }
.activity-entry.ae-error { border-left: 3px solid var(--red); }
.activity-row {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  cursor: pointer;
  user-select: none;
}
.activity-row .ae-arrow {
  font-size: 10px;
  color: var(--text3);
  transition: transform 0.15s;
  width: 12px;
  text-align: center;
  flex-shrink: 0;
}
.activity-entry.ae-open .ae-arrow { transform: rotate(90deg); }
.activity-row .ae-time {
  font-family: var(--mono);
  font-size: 12px;
  color: var(--text3);
  min-width: 70px;
}
.activity-row .ae-source { font-size: 13px; font-weight: 500; min-width: 90px; }
.activity-row .ae-count {
  font-family: var(--mono);
  font-size: 12px;
  color: var(--text2);
}
.activity-row .ae-dur {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text3);
  margin-left: auto;
}
.activity-detail {
  display: none;
  padding: 0 14px 10px 36px;
  font-size: 12px;
  color: var(--text2);
  line-height: 1.6;
}
.activity-entry.ae-open .activity-detail { display: block; }
.activity-detail .ad-item {
  display: flex;
  align-items: center;
  gap: 6px;
}
.activity-detail .ad-item::before {
  content: '';
  width: 4px;
  height: 4px;
  border-radius: 50%;
  background: var(--accent);
  flex-shrink: 0;
}
.activity-detail .ad-error {
  color: var(--red);
  font-weight: 500;
}

/* ---- Drawer ---- */
.overlay {
  position: fixed; inset: 0;
  background: rgba(0,0,0,0.5);
  z-index: 100;
  opacity: 0;
  visibility: hidden;
  transition: all 0.3s;
}
.overlay.open { opacity: 1; visibility: visible; }
.drawer {
  position: fixed;
  top: 0; right: 0; bottom: 0;
  width: 520px;
  max-width: 95vw;
  background: var(--bg);
  border-left: 1px solid var(--border);
  z-index: 101;
  transform: translateX(100%);
  transition: transform 0.3s ease;
  display: flex;
  flex-direction: column;
  box-shadow: -8px 0 32px rgba(0,0,0,0.3);
}
.drawer.open { transform: translateX(0); }
.drawer-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20px 24px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
.drawer-header h3 {
  font-size: 16px;
  font-weight: 600;
}
.drawer-close {
  background: none; border: none;
  color: var(--text3);
  font-size: 20px;
  cursor: pointer;
  padding: 4px;
  line-height: 1;
}
.drawer-close:hover { color: var(--text); }
.drawer-body {
  flex: 1;
  overflow-y: auto;
  padding: 24px;
}
.drawer-footer {
  padding: 16px 24px;
  border-top: 1px solid var(--border);
  display: flex;
  gap: 10px;
  flex-shrink: 0;
}

/* ---- Drawer Sections ---- */
.drawer-section {
  margin-bottom: 24px;
}
.drawer-section-title {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text3);
  margin-bottom: 12px;
  font-weight: 500;
}

/* Setup Steps */
.setup-step {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 0;
}
.setup-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.setup-dot.ok { background: var(--green); }
.setup-dot.missing { background: var(--red); }
.setup-dot.info { background: var(--text3); }
.setup-step-text {
  flex: 1;
  font-size: 13px;
  color: var(--text2);
}
.setup-step-text code {
  font-family: var(--mono);
  font-size: 12px;
  background: var(--bg3);
  padding: 1px 5px;
  border-radius: 4px;
}
.setup-step-action { flex-shrink: 0; }

.progress-bar {
  height: 4px;
  background: var(--bg3);
  border-radius: 2px;
  margin-top: 12px;
  overflow: hidden;
}
.progress-bar-fill {
  height: 100%;
  background: var(--accent);
  border-radius: 2px;
  transition: width 0.3s;
}
.progress-text {
  font-size: 11px;
  color: var(--text3);
  margin-top: 6px;
}

/* Toggle */
.toggle-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 0;
}
.toggle-label {
  font-size: 14px;
  font-weight: 500;
}
.toggle {
  position: relative;
  width: 44px;
  height: 24px;
  cursor: pointer;
}
.toggle input {
  opacity: 0;
  width: 100%;
  height: 100%;
  position: absolute;
  inset: 0;
  margin: 0;
  z-index: 2;
  cursor: pointer;
}
.toggle-track {
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: var(--bg3);
  border: 1px solid var(--border);
  border-radius: 12px;
  transition: all 0.2s;
}
.toggle input:checked + .toggle-track {
  background: var(--accent);
  border-color: var(--accent);
}
.toggle-track::after {
  content: '';
  position: absolute;
  top: 3px; left: 3px;
  width: 16px; height: 16px;
  border-radius: 50%;
  background: var(--text);
  transition: transform 0.2s;
}
.toggle input:checked + .toggle-track::after {
  transform: translateX(20px);
  background: #fff;
}

/* Fields */
.field {
  margin-bottom: 14px;
}
.field label {
  display: block;
  font-size: 12px;
  color: var(--text2);
  margin-bottom: 4px;
  text-transform: capitalize;
}
.field input[type="text"],
.field input[type="number"],
.field textarea {
  width: 100%;
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--bg);
  color: var(--text);
  font-family: var(--mono);
  font-size: 12px;
  outline: none;
  transition: border-color 0.2s;
}
.field input:focus, .field textarea:focus {
  border-color: var(--accent);
}
.field textarea { resize: vertical; min-height: 60px; }
.field-checkbox {
  display: flex;
  align-items: center;
  gap: 8px;
}
.field-checkbox input[type="checkbox"] {
  accent-color: var(--accent);
  width: 16px; height: 16px;
}

/* Segmented switch (2 choices) */
.seg-switch {
  display: inline-flex;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 2px;
  gap: 2px;
}
.seg-btn {
  background: transparent;
  border: 0;
  color: var(--text3);
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  text-transform: capitalize;
}
.seg-btn.active {
  background: var(--accent);
  color: var(--bg);
  font-weight: 500;
}

/* Path row (input + browse button) */
.path-row {
  display: flex;
  gap: 6px;
  align-items: stretch;
}
.path-row input {
  flex: 1;
}

/* Repeater (list of structured items) */
.repeater {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.repeater-item {
  display: flex;
  gap: 6px;
  align-items: center;
}
.repeater-item input {
  flex: 1;
}

/* Path picker modal */
.path-picker-modal {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.6);
  z-index: 10000;
  align-items: center;
  justify-content: center;
}
.path-picker-dialog {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 12px;
  width: min(600px, 92vw);
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.path-picker-head {
  padding: 12px 14px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-family: JetBrains Mono, monospace;
  font-size: 12px;
  color: var(--text2);
  gap: 8px;
}
.path-picker-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}
.path-picker-row {
  padding: 8px 14px;
  cursor: pointer;
  font-size: 13px;
  color: var(--text2);
}
.path-picker-row:hover,
.path-picker-row:focus {
  background: var(--bg3);
  outline: none;
}
.path-picker-row:focus-visible {
  box-shadow: inset 2px 0 0 var(--accent);
}
.path-picker-foot {
  padding: 10px 14px;
  border-top: 1px solid var(--border);
  display: flex;
  gap: 8px;
  align-items: center;
}
.path-picker-foot input {
  flex: 1;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 10px;
  color: var(--text);
  font-family: JetBrains Mono, monospace;
  font-size: 12px;
}

/* Stat boxes */
.stat-boxes {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.stat-box {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px;
  box-shadow: var(--card-shadow);
}
.stat-box-value {
  font-family: var(--mono);
  font-size: 18px;
  font-weight: 500;
}
.stat-box-label {
  font-size: 11px;
  color: var(--text3);
  margin-top: 2px;
}

/* Credential input row */
.cred-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 4px;
}
.cred-row input {
  flex: 1;
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--bg);
  color: var(--text);
  font-family: var(--mono);
  font-size: 12px;
  outline: none;
}
.cred-row input:focus { border-color: var(--accent); }

/* Setup Wizard */
.wizard-overlay {
  position: fixed; inset: 0;
  background: rgba(0,0,0,0.5);
  z-index: 1100;
  display: none;
}
.wizard-overlay.open { display: block; }
.wizard {
  position: fixed;
  top: 50%; left: 50%;
  transform: translate(-50%, -50%);
  width: min(560px, 90vw);
  max-height: 80vh;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 16px;
  z-index: 1200;
  display: none;
  flex-direction: column;
  box-shadow: 0 24px 80px rgba(0,0,0,0.5);
}
.wizard.open { display: flex; }
.wizard-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  border-bottom: 1px solid var(--border);
}
.wizard-step-indicator {
  font-size: 12px;
  color: var(--text3);
  font-weight: 500;
  letter-spacing: 0.5px;
}
.wizard-body {
  flex: 1;
  overflow-y: auto;
  padding: 24px;
}
.wizard-footer {
  display: flex;
  justify-content: space-between;
  padding: 16px 20px;
  border-top: 1px solid var(--border);
}
.wizard h2 {
  font-size: 20px;
  font-weight: 600;
  margin: 0 0 8px;
}
.wizard p {
  font-size: 14px;
  color: var(--text2);
  margin: 0 0 16px;
  line-height: 1.5;
}
.wizard .source-pick {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 14px;
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 6px;
  cursor: pointer;
  transition: border-color 0.15s, background 0.15s;
}
.wizard .source-pick:hover { border-color: var(--accent); }
.wizard .source-pick.selected { border-color: var(--accent); background: color-mix(in srgb, var(--accent) 8%, transparent); }
.wizard .source-pick input[type="checkbox"] {
  accent-color: var(--accent);
  width: 16px; height: 16px;
  flex-shrink: 0;
}
.wizard .source-pick-info { flex: 1; min-width: 0; }
.wizard .source-pick-name { font-size: 14px; font-weight: 500; }
.wizard .source-pick-desc { font-size: 12px; color: var(--text3); margin-top: 2px; }
.wizard .source-pick-badge {
  font-size: 10px;
  padding: 2px 7px;
  border-radius: 5px;
  flex-shrink: 0;
}
.wizard .source-pick .badge-rec {
  background: rgba(52,211,153,0.12);
  color: var(--accent);
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 0.5px;
}
.wizard .wiz-category {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text3);
  margin: 16px 0 8px;
  font-weight: 500;
}
.wizard .wiz-category:first-child { margin-top: 0; }
.wizard .wiz-sync-log {
  font-family: var(--mono);
  font-size: 12px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
  max-height: 200px;
  overflow-y: auto;
  line-height: 1.8;
}
.wizard .wiz-sync-line { color: var(--text2); }
.wizard .wiz-sync-ok { color: var(--green); }
.wizard .wiz-sync-err { color: var(--red); }
.wizard .wiz-sync-count { color: var(--accent); font-weight: 500; }
.wizard .wiz-install-card {
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
  margin-bottom: 10px;
  background: var(--bg2);
}
.wizard .wiz-install-card.ready { border-color: var(--green); background: color-mix(in srgb, var(--green) 6%, var(--bg2)); }
.wizard .wiz-install-card.skipped { opacity: 0.55; }
.wizard .wiz-install-head {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
}
.wizard .wiz-install-head-name { font-size: 14px; font-weight: 600; flex: 1; }
.wizard .wiz-install-status {
  font-size: 10px;
  padding: 2px 8px;
  border-radius: 5px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
}
.wizard .wiz-install-status.ready { background: color-mix(in srgb, var(--green) 20%, transparent); color: var(--green); }
.wizard .wiz-install-status.pending { background: color-mix(in srgb, var(--yellow) 20%, transparent); color: var(--yellow); }
.wizard .wiz-install-status.skipped { background: var(--bg3); color: var(--text3); }
.wizard .wiz-install-dep {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 0;
  border-top: 1px dashed var(--border);
  font-size: 13px;
}
.wizard .wiz-install-dep:first-of-type { border-top: none; }
.wizard .wiz-install-dep-info { flex: 1; min-width: 0; }
.wizard .wiz-install-dep-text code {
  font-family: var(--mono);
  font-size: 11px;
  background: var(--bg3);
  padding: 1px 6px;
  border-radius: 4px;
}
.wizard .wiz-install-dep-hint { font-size: 11px; color: var(--text3); margin-top: 3px; word-break: break-word; }
.wizard .wiz-install-dep-hint code { white-space: nowrap; }
.wizard .wiz-install-actions { display: flex; gap: 6px; flex-shrink: 0; }
.wizard .wiz-install-card-actions {
  display: flex;
  gap: 8px;
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--border);
}
.wizard .wiz-install-card-actions button { font-size: 12px; }
.wizard .wiz-cred-row { display: flex; gap: 6px; flex-shrink: 0; }
.wizard .wiz-cred-row input {
  font-size: 12px;
  padding: 4px 8px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 5px;
  color: var(--text);
  width: 140px;
}
.wizard .wiz-dot {
  width: 8px; height: 8px; border-radius: 50%;
  flex-shrink: 0;
}
.wizard .wiz-dot.ok { background: var(--green); }
.wizard .wiz-dot.missing { background: var(--yellow); }
.wizard .wiz-dot.info { background: var(--text3); }
.wizard .wiz-setup-note {
  background: color-mix(in srgb, var(--accent) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--accent) 30%, transparent);
  color: var(--text2);
  font-size: 12px;
  padding: 10px 12px;
  border-radius: 8px;
  margin-bottom: 14px;
}
.wizard .wiz-setup-hint {
  font-size: 12px;
  color: var(--text2);
  background: var(--bg3);
  border-left: 2px solid var(--accent);
  padding: 6px 10px;
  border-radius: 4px;
  margin-bottom: 8px;
}
.wizard .wiz-setup-alt {
  font-size: 12px;
  color: var(--text2);
  background: color-mix(in srgb, var(--accent) 8%, transparent);
  border: 1px dashed color-mix(in srgb, var(--accent) 40%, transparent);
  padding: 8px 10px;
  border-radius: 6px;
  margin-bottom: 10px;
  display: flex;
  align-items: center;
  gap: 10px;
  justify-content: space-between;
}
.wizard .wiz-setup-alt button { font-size: 11px; padding: 3px 8px; flex-shrink: 0; }

/* Auth modal */
.auth-modal-backdrop {
  position: fixed; inset: 0;
  background: rgba(0,0,0,0.6);
  z-index: 10000;
  display: flex; align-items: center; justify-content: center;
  padding: 20px;
}
.auth-modal {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 12px;
  width: 100%;
  max-width: 520px;
  max-height: 90vh;
  overflow-y: auto;
  padding: 24px;
}
.auth-modal h3 { margin: 0 0 8px 0; font-size: 16px; }
.auth-modal p { color: var(--text2); font-size: 13px; margin: 0 0 14px 0; }
.auth-modal .auth-field {
  display: flex; flex-direction: column; gap: 6px;
  margin-bottom: 12px;
}
.auth-modal .auth-field label {
  font-size: 11px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.auth-modal .auth-field input {
  padding: 8px 10px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  color: var(--text);
  font-size: 14px;
}
.auth-modal .auth-device-code {
  font-family: var(--mono);
  font-size: 22px;
  letter-spacing: 3px;
  padding: 10px 14px;
  background: var(--bg3);
  border-radius: 8px;
  text-align: center;
  user-select: all;
  cursor: pointer;
  margin: 10px 0;
}
.auth-modal .auth-url a {
  color: var(--accent);
  font-size: 13px;
  word-break: break-all;
}
.auth-modal .auth-qr {
  font-family: var(--mono);
  font-size: 9px;
  line-height: 1;
  background: #fff;
  color: #000;
  padding: 12px;
  border-radius: 6px;
  white-space: pre;
  text-align: center;
  overflow-x: auto;
}
.auth-modal .auth-log {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 10px;
  font-family: var(--mono);
  font-size: 11px;
  max-height: 140px;
  overflow-y: auto;
  color: var(--text2);
  white-space: pre-wrap;
}
.auth-modal .auth-actions {
  display: flex; gap: 8px; justify-content: flex-end;
  margin-top: 16px;
}
.auth-modal .auth-status {
  padding: 8px 12px;
  border-radius: 6px;
  font-size: 12px;
  margin-bottom: 10px;
}
.auth-modal .auth-status.success { background: color-mix(in srgb, var(--green) 15%, transparent); color: var(--green); }
.auth-modal .auth-status.error { background: color-mix(in srgb, var(--red) 15%, transparent); color: var(--red); }
.auth-modal .auth-status.info { background: color-mix(in srgb, var(--accent) 12%, transparent); color: var(--accent); }

/* Vault picker list */
.wiz-vault-list {
  display: flex; flex-direction: column;
  gap: 6px;
  margin: 8px 0;
}
.wiz-vault-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  padding: 8px 10px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 12px;
}
.wiz-vault-item .wiz-vault-path {
  font-family: var(--mono);
  color: var(--text3);
  font-size: 10px;
  word-break: break-all;
}
.wiz-vault-item button { font-size: 11px; flex-shrink: 0; }

/* Nextcloud form */
.wiz-nc-form {
  display: flex; flex-direction: column;
  gap: 8px;
  margin: 10px 0;
  padding: 12px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
}
.wiz-nc-form input {
  padding: 6px 10px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 5px;
  color: var(--text);
  font-size: 12px;
}
.wiz-nc-form label {
  font-size: 10px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.wiz-nc-form .wiz-nc-actions {
  display: flex; gap: 6px;
  margin-top: 4px;
}
.wiz-nc-form .wiz-nc-test-result { font-size: 11px; padding: 4px 0; }

/* Consumer cards */
.consumer-card {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 14px 16px;
}
.consumer-card-name {
  font-weight: 600;
  font-size: 15px;
  margin-bottom: 4px;
}
.consumer-card-updated {
  font-size: 11px;
  color: var(--text3);
  margin-bottom: 12px;
}
.consumer-positions {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.consumer-pos-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 12px;
}
.consumer-pos-source { color: var(--text2); }
.consumer-pos-value {
  font-family: var(--mono);
  font-size: 12px;
}

/* ---- Toast ---- */
.toast-container {
  position: fixed;
  bottom: 24px; right: 24px;
  z-index: 200;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.toast {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 12px 16px;
  font-size: 13px;
  min-width: 260px;
  max-width: 420px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.4);
  animation: toastIn 0.3s ease;
  border-left: 3px solid var(--accent);
}
.toast.error { border-left-color: var(--red); }
@keyframes toastIn {
  from { opacity:0; transform: translateY(10px); }
  to { opacity:1; transform: translateY(0); }
}

/* ---- Empty state ---- */
.empty {
  text-align: center;
  padding: 48px;
  color: var(--text3);
}
.empty p { font-size: 14px; }

/* ---- Scrollbar ---- */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text3); }

/* ---- Dashboard cards (alias to src-card) ---- */
.dash-card {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 14px 16px;
  cursor: pointer;
  transition: border-color 0.15s;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.dash-card:hover {
  border-color: var(--border-hover);
}

.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-dot.syncing { background: var(--green); box-shadow: 0 0 6px rgba(52,211,153,0.4); }
.status-dot.error { background: var(--red); }
.status-dot.setup { background: var(--yellow); }
.status-dot.disabled { background: var(--text3); }

/* ---- Setup banner ---- */
.setup-banner {
  background: var(--yellow-bg);
  border: 1px solid rgba(245,158,11,0.2);
  border-left: 3px solid var(--yellow);
  border-radius: 10px;
  padding: 16px 20px;
  margin-bottom: 20px;
}
.setup-banner-title {
  font-weight: 600;
  font-size: 14px;
  margin-bottom: 10px;
  color: var(--yellow);
}
.setup-banner-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 6px 0;
  font-size: 13px;
  color: var(--text2);
}

/* ---- Collapsible section ---- */
.collapsible-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 18px;
  cursor: pointer;
  color: var(--text2);
  font-size: 13px;
  font-weight: 500;
  user-select: none;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  margin-top: 24px;
  margin-bottom: 14px;
  background: var(--bg2);
  transition: all 0.2s;
}
.collapsible-header:hover { color: var(--text); border-color: var(--border-hover); }
.collapsible-arrow { transition: transform 0.2s; font-size: 12px; color: var(--text3); }
.collapsible-arrow.open { transform: rotate(90deg); }

/* ---- KPI Row ---- */
.kpi-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 10px;
  margin-bottom: 24px;
}
.kpi {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 14px 16px;
  text-align: center;
  transition: border-color 0.15s;
}
.kpi:hover { border-color: var(--border-hover); }
.kpi-val {
  font-size: 22px;
  font-weight: 700;
  line-height: 1;
  letter-spacing: -0.5px;
}
.kpi-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--text3);
  margin-top: 6px;
  font-weight: 500;
}

/* ---- Search bar ---- */
.search-bar {
  display: flex;
  gap: 10px;
  margin-bottom: 24px;
  align-items: stretch;
}
.search-bar input {
  flex: 1;
  padding: 10px 14px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 8px;
  color: var(--text);
  font-size: 14px;
  outline: none;
  transition: border-color 0.2s;
}
.search-bar input:focus { border-color: var(--accent); }
.search-bar select {
  padding: 10px 12px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 8px;
  color: var(--text);
  font-size: 13px;
  outline: none;
}
.search-bar button {
  padding: 10px 20px;
  background: var(--accent);
  color: white;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
  font-weight: 600;
  transition: background 0.15s;
}
.search-bar button:hover { background: var(--accent-hover); }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="logo">vadim<span>gest</span></div>
    <div class="header-stats">
      <div class="header-stat">
        <div class="header-stat-value" id="stat-records">-</div>
        <div class="header-stat-label">Records</div>
      </div>
      <div class="header-stat">
        <div class="header-stat-value" id="stat-sources">-</div>
        <div class="header-stat-label">Sources</div>
      </div>
      <div class="header-stat">
        <div class="header-stat-value" id="stat-active">-</div>
        <div class="header-stat-label">Active</div>
      </div>
    </div>
  </div>
  <div class="header-right">
    <button class="theme-btn" onclick="localStorage.removeItem('vadimgest_wizard_done');openWizard()" title="Open setup wizard" aria-label="Open setup wizard" style="font-size:16px">&#9881;</button>
    <div class="daemon-status" id="daemon-status" onclick="toggleDaemon()" title="Sync daemon">
      <span class="daemon-dot" id="daemon-dot"></span>
      <span id="daemon-label">Daemon</span>
    </div>
    <button class="theme-btn" onclick="toggleTheme()" title="Toggle theme" aria-label="Toggle theme">
      <span id="theme-icon">&#9790;</span>
    </button>
  </div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="dashboard">Dashboard</div>
  <div class="tab" data-tab="observatory">Observatory</div>
  <div class="tab" data-tab="sources">Sources</div>
  <div class="tab" data-tab="edge">Edge Sync</div>
  <div class="tab" data-tab="docs">Docs</div>
</div>

<div class="content">
  <div id="config-banner"></div>
  <div class="tab-content active" id="tab-dashboard"></div>
  <div class="tab-content" id="tab-observatory"></div>
  <div class="tab-content" id="tab-sources"></div>
  <div class="tab-content" id="tab-edge"></div>
  <div class="tab-content" id="tab-docs"></div>
</div>

<div class="overlay" id="overlay" onclick="closeDrawer()"></div>
<div class="drawer" id="drawer">
  <div class="drawer-header">
    <h3 id="drawer-title">Source</h3>
    <button class="drawer-close" onclick="closeDrawer()">&times;</button>
  </div>
  <div class="drawer-body" id="drawer-body"></div>
  <div class="drawer-footer" id="drawer-footer"></div>
</div>

<div class="toast-container" id="toasts"></div>

<!-- Setup Wizard -->
<div class="wizard-overlay" id="wizard-overlay"></div>
<div class="wizard" id="wizard">
  <div class="wizard-header">
    <span class="wizard-step-indicator" id="wizard-step-indicator">Step 1 of 3</span>
    <button class="drawer-close" onclick="closeWizard()">&times;</button>
  </div>
  <div class="wizard-body" id="wizard-body"></div>
  <div class="wizard-footer" id="wizard-footer"></div>
</div>

<script>
let sourcesData = [];
let runsData = [];
let queuesData = null;
let consumersData = null;
let appConfig = {};
let searchHealth = null;
let edgeStatus = null;
let observatoryData = null;
let openSourceName = null;

const SOURCE_ICONS = {
  telegram: '\\u2708\\uFE0F', signal: '\\uD83D\\uDD12', whatsapp: '\\uD83D\\uDCAC', imessage: '\\uD83D\\uDCF1',
  gmail: '\\u2709\\uFE0F', gtasks: '\\u2705', calendar: '\\uD83D\\uDCC5',
  github: '\\uD83D\\uDC19', github_notifications: '\\uD83D\\uDD14',
  obsidian: '\\uD83D\\uDCDD', gdrive: '\\u2601\\uFE0F', nextcloud: '\\u2601\\uFE0F',
  browser: '\\uD83C\\uDF10', dayflow: '\\u23F1\\uFE0F', claude: '\\uD83E\\uDD16',
  granola: '\\uD83C\\uDF99\\uFE0F', hlopya: '\\uD83C\\uDF99\\uFE0F',
  linkedin: '\\uD83D\\uDCBC', xnews: '\\uD83D\\uDCF0',
};
const CAT_ICONS = {
  messaging: '\\uD83D\\uDCE8', email: '\\u2709\\uFE0F', calendar: '\\uD83D\\uDCC5',
  dev: '\\uD83D\\uDCBB', files: '\\uD83D\\uDCC1', activity: '\\u23F1\\uFE0F',
  meetings: '\\uD83C\\uDF99\\uFE0F', social: '\\uD83C\\uDF10', knowledge: '\\uD83D\\uDCDA',
};

// ---- Theme ----
function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme') || 'dark';
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('vg-theme', next);
  document.getElementById('theme-icon').textContent = next === 'dark' ? '\\u263E' : '\\u263C';
}
(function(){
  const t = document.documentElement.getAttribute('data-theme') || localStorage.getItem('vg-theme') || 'dark';
  document.documentElement.setAttribute('data-theme', t);
  const icon = document.getElementById('theme-icon');
  if(icon) icon.textContent = t === 'dark' ? '\\u263E' : '\\u263C';
})();

// ---- Tabs ----
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    const target = tab.getAttribute('data-tab');
    document.getElementById('tab-' + target).classList.add('active');
    if (target === 'dashboard') renderDashboard();
    if (target === 'observatory') renderObservatory();
    if (target === 'sources') renderSourcesPage();
    if (target === 'edge') renderEdgePage();
    if (target === 'docs') renderDocsPage();
  });
});

// ---- Formatting ----
function fmtNum(n) {
  if (n === null || n === undefined) return '-';
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return n.toString();
}

function timeAgo(ts) {
  if (!ts) return 'never';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return 'never';
  const secs = Math.floor((Date.now() - d.getTime()) / 1000);
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  if (secs < 604800) return Math.floor(secs / 86400) + 'd ago';
  return d.toLocaleDateString();
}

function escHtml(s) {
  if (!s) return '';
  if (typeof s !== 'string') s = String(s);
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---- Activity expand/collapse ----
function toggleAe(i) {
  const el = document.getElementById('ae-' + i);
  if (el) el.classList.toggle('ae-open');
}

// ---- Config widget helpers ----
function segSelect(btn) {
  const parent = btn.parentElement;
  parent.querySelectorAll('.seg-btn').forEach(b => {
    b.classList.remove('active');
    b.setAttribute('aria-checked', 'false');
  });
  btn.classList.add('active');
  btn.setAttribute('aria-checked', 'true');
}

function toggleSecret(btn) {
  const input = btn.parentElement.querySelector('input[type=password], input[type=text]');
  if (!input) return;
  if (input.type === 'password') {
    input.type = 'text';
    btn.textContent = 'Hide';
    btn.setAttribute('aria-label', 'Hide value');
  } else {
    input.type = 'password';
    btn.textContent = 'Show';
    btn.setAttribute('aria-label', 'Show value');
  }
}

function repeaterAdd(btn, fields) {
  const parent = btn.parentElement;
  const item = document.createElement('div');
  item.className = 'repeater-item';
  (fields || []).forEach(f => {
    const inp = document.createElement('input');
    inp.type = 'text';
    inp.setAttribute('data-repeater-key', f.key);
    inp.placeholder = f.placeholder || f.key;
    item.appendChild(inp);
  });
  const rm = document.createElement('button');
  rm.type = 'button';
  rm.className = 'btn btn-sm';
  rm.textContent = '\u00D7';
  rm.onclick = function() { repeaterRemove(rm); };
  item.appendChild(rm);
  parent.insertBefore(item, btn);
}

function repeaterRemove(btn) {
  const item = btn.closest('.repeater-item');
  if (item) item.remove();
}

async function openPathPicker(btn, key) {
  const input = btn.parentElement.querySelector('input[data-config-key="' + key + '"]');
  const startPath = (input && input.value) || '';
  let modal = document.getElementById('path-picker-modal');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'path-picker-modal';
    modal.className = 'path-picker-modal';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-label', 'Folder picker');
    modal.innerHTML = '<div class="path-picker-dialog">'
      + '<div class="path-picker-head"><span id="path-picker-current" aria-live="polite"></span>'
      + '<button type="button" class="btn btn-sm" aria-label="Close picker" onclick="document.getElementById(\\'path-picker-modal\\').style.display=\\'none\\'">Cancel</button></div>'
      + '<div class="path-picker-list" id="path-picker-list" role="listbox" tabindex="0"></div>'
      + '<div class="path-picker-foot">'
      + '<input type="text" id="path-picker-input" placeholder="Or type a path\u2026" aria-label="Selected path" spellcheck="false" autocapitalize="off" autocorrect="off">'
      + '<button type="button" class="btn btn-sm btn-primary" id="path-picker-select">Select this folder</button>'
      + '</div></div>';
    // Click on backdrop (outside dialog) closes
    modal.addEventListener('click', function(e) {
      if (e.target === modal) modal.style.display = 'none';
    });
    document.body.appendChild(modal);
  }
  modal.style.display = 'flex';
  modal._targetInput = input;
  await pathPickerNav(startPath || '~');
  // Focus text field so keyboard users can type path directly
  const ti = document.getElementById('path-picker-input');
  if (ti) setTimeout(() => ti.focus(), 50);
}

async function pathPickerNav(path) {
  try {
    const r = await fetch('/api/fs/browse?path=' + encodeURIComponent(path));
    const d = await r.json();
    if (d.error) { showToast(d.error, 'error'); return; }
    const cur = document.getElementById('path-picker-current');
    const list = document.getElementById('path-picker-list');
    const ti = document.getElementById('path-picker-input');
    const sel = document.getElementById('path-picker-select');
    cur.textContent = d.path;
    ti.value = d.path;
    const doSelect = function() {
      const modal = document.getElementById('path-picker-modal');
      const chosen = ti.value || d.path;
      if (modal._targetInput) modal._targetInput.value = chosen;
      modal.style.display = 'none';
    };
    sel.onclick = doSelect;
    ti.onkeydown = function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        // If user typed a new path, navigate to it; otherwise select current
        if (ti.value && ti.value !== d.path) pathPickerNav(ti.value);
        else doSelect();
      }
    };
    let html = '';
    if (d.parent && d.parent !== d.path) {
      html += '<div class="path-picker-row" role="option" tabindex="0" aria-label="Go to parent folder" onclick="pathPickerNav(\\'' + escHtml(d.parent) + '\\')" onkeydown="if(event.key===&quot;Enter&quot;||event.key===&quot; &quot;){event.preventDefault();pathPickerNav(\\'' + escHtml(d.parent) + '\\')}">\u2B11 ..</div>';
    }
    (d.entries || []).forEach(e => {
      const safePath = escHtml(e.path.replace(/\\\\/g, '\\\\\\\\').replace(/\\'/g, "\\\\'"));
      html += '<div class="path-picker-row" role="option" tabindex="0" aria-label="Open ' + escHtml(e.name) + '" onclick="pathPickerNav(\\'' + safePath + '\\')" onkeydown="if(event.key===&quot;Enter&quot;||event.key===&quot; &quot;){event.preventDefault();pathPickerNav(\\'' + safePath + '\\')}">\\uD83D\\uDCC1 ' + escHtml(e.name) + '</div>';
    });
    if (!d.entries.length) html += '<div style="padding:12px;color:var(--text3);text-align:center">Empty folder \u2014 press <b>Select this folder</b> to use it</div>';
    list.innerHTML = html;
  } catch (e) {
    showToast('Browse failed: ' + e.message, 'error');
  }
}

// ---- Toast ----
function showToast(msg, type) {
  const container = document.getElementById('toasts');
  const toast = document.createElement('div');
  toast.className = 'toast' + (type === 'error' ? ' error' : '');
  toast.textContent = msg;
  container.appendChild(toast);
  setTimeout(() => { toast.remove(); }, 4000);
}

// ---- API Calls ----
async function apiFetch(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(url + ' returned ' + res.status);
  return res.json();
}

async function fetchSources() {
  try {
    sourcesData = await apiFetch('/api/sources');
  } catch(e) { console.error('fetchSources', e); showToast('Sources: ' + e.message, 'error'); }
}

async function fetchRuns() {
  try {
    runsData = await apiFetch('/api/runs');
  } catch(e) { console.error('fetchRuns', e); showToast('Runs: ' + e.message, 'error'); }
}

async function fetchQueues() {
  try {
    queuesData = await apiFetch('/api/queues');
    renderQueues();
  } catch(e) { console.error('fetchQueues', e); showToast('Queues: ' + e.message, 'error'); }
}

async function fetchConsumers() {
  try {
    consumersData = await apiFetch('/api/consumers');
    renderConsumers();
  } catch(e) { console.error('fetchConsumers', e); showToast('Consumers: ' + e.message, 'error'); }
}

async function fetchConfig() {
  try {
    appConfig = await apiFetch('/api/config');
  } catch(e) { console.error('fetchConfig', e); showToast('Config: ' + e.message, 'error'); }
}

async function fetchSearchHealth() {
  try {
    searchHealth = await apiFetch('/api/search/health');
  } catch(e) { searchHealth = null; }
}

async function fetchEdgeStatus() {
  try {
    edgeStatus = await apiFetch('/api/edge/status');
  } catch(e) { edgeStatus = null; }
}

async function fetchObservatory() {
  try {
    observatoryData = await apiFetch('/api/observatory');
  } catch(e) { observatoryData = null; console.error('fetchObservatory', e); }
}

async function fetchDaemon() {
  try {
    const data = await apiFetch('/api/daemon');
    const dot = document.getElementById('daemon-dot');
    const label = document.getElementById('daemon-label');
    if (data.running) {
      dot.classList.add('running');
      label.textContent = 'running';
    } else {
      dot.classList.remove('running');
      label.textContent = 'stopped';
    }
  } catch(e) { console.error('fetchDaemon', e); }
}

async function toggleDaemon() {
  const dot = document.getElementById('daemon-dot');
  const isRunning = dot.classList.contains('running');
  try {
    const url = isRunning ? '/api/daemon/stop' : '/api/daemon/start';
    const res = await fetch(url, {method: 'POST'});
    const data = await res.json();
    if (data.ok) {
      showToast('Daemon ' + (isRunning ? 'stopped' : 'started'), 'success');
    }
    await fetchDaemon();
  } catch(e) { showToast('Daemon toggle failed: ' + e.message, 'error'); }
}

async function refresh() {
  await Promise.all([fetchSources(), fetchRuns(), fetchConfig(), fetchDaemon(), fetchEdgeStatus(), fetchObservatory()]);
  updateHeaderStats();
  const activeTab = document.querySelector('.tab.active');
  if (activeTab) {
    const target = activeTab.getAttribute('data-tab');
    if (target === 'dashboard') renderDashboard();
    else if (target === 'observatory') renderObservatory();
    else if (target === 'sources') renderSourcesPage();
    else if (target === 'edge') renderEdgePage();
  }
  fetchSearchHealth().then(() => {
    const activeTab = document.querySelector('.tab.active');
    if (!activeTab) return;
    const target = activeTab.getAttribute('data-tab');
    if (target === 'dashboard') renderDashboard();
    else if (target === 'observatory') renderObservatory();
    else if (target === 'sources') renderSourcesPage();
  });
}

function updateHeaderStats() {
  let total = 0;
  let active = 0;
  sourcesData.forEach(s => {
    total += s.records || 0;
    if (s.enabled) active++;
  });
  document.getElementById('stat-records').textContent = fmtNum(total);
  document.getElementById('stat-sources').textContent = sourcesData.length;
  document.getElementById('stat-active').textContent = active;
}

// ---- Banner ----
function renderBanner() {
  // Banner is now rendered inside the Dashboard tab
  const el = document.getElementById('config-banner');
  if (el) el.innerHTML = '';
}

async function createConfig() {
  try {
    const res = await fetch('/api/config/init', {method:'POST'});
    const data = await res.json();
    if (data.ok) {
      showToast('Config created: ' + data.path, 'success');
      await refresh();
    } else {
      showToast('Failed to create config', 'error');
    }
  } catch(e) { showToast(e.message, 'error'); }
}

function statusBadge(status) {
  const s = status || 'unknown';
  const cls = s === 'healthy' ? 'badge-green' : s === 'degraded' ? 'badge-yellow' : s === 'broken' ? 'badge-red' : 'badge-gray';
  return '<span class="badge ' + cls + '">' + escHtml(s) + '</span>';
}

function statusColor(status) {
  if (status === 'healthy') return 'var(--green)';
  if (status === 'degraded') return 'var(--yellow)';
  if (status === 'broken') return 'var(--red)';
  return 'var(--text3)';
}

function renderObservatory() {
  const el = document.getElementById('tab-observatory');
  const d = observatoryData;
  if (!d) {
    el.innerHTML = '<div class="empty"><p>Loading Observatory...</p></div>';
    return;
  }

  const subsystems = d.subsystems || [];
  const sources = (d.sources && d.sources.items) || [];
  const edge = d.edge || {};
  const localAgent = edge.local_agent || {};
  const klava = d.klava || {};
  const queues = d.queues || {};
  const recentErrors = d.recent_errors || [];

  let html = '';
  html += '<div class="panel" style="border-color:' + statusColor(d.status) + '">';
  html += '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap">';
  html += '<div><div style="font-size:12px;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px">Vadimgest Observatory</div>';
  html += '<div style="font-size:30px;font-weight:700;color:' + statusColor(d.status) + '">' + escHtml((d.status || 'unknown').toUpperCase()) + '</div>';
  html += '<div style="font-size:13px;color:var(--text2);margin-top:6px">Last updated ' + timeAgo(d.generated_at) + '</div></div>';
  html += '<div style="display:grid;grid-template-columns:repeat(3,minmax(110px,1fr));gap:10px;min-width:min(100%,420px)">';
  html += '<div class="kpi"><div class="kpi-val">' + fmtNum((d.sources || {}).enabled || 0) + '</div><div class="kpi-label">Enabled Sources</div></div>';
  html += '<div class="kpi"><div class="kpi-val">' + fmtNum(localAgent.pending_total || 0) + '</div><div class="kpi-label">Edge Pending</div></div>';
  html += '<div class="kpi"><div class="kpi-val">' + fmtNum(queues.pending_total || 0) + '</div><div class="kpi-label">Queue Pending</div></div>';
  html += '</div></div></div>';

  html += '<div class="kpi-row">';
  subsystems.forEach(s => {
    html += '<div class="kpi" style="border-top:3px solid ' + statusColor(s.status) + '">';
    html += '<div class="kpi-val" style="font-size:14px;color:' + statusColor(s.status) + '">' + escHtml(s.status || 'unknown') + '</div>';
    html += '<div class="kpi-label">' + escHtml(s.label || s.key) + '</div></div>';
  });
  html += '</div>';

  html += '<div class="panel"><div class="panel-title">Server Hub</div><table class="runs-table"><tbody>';
  html += '<tr><td>Dashboard</td><td>' + statusBadge((d.server || {}).status) + '</td><td>' + escHtml((d.server || {}).data_dir || '') + '</td></tr>';
  html += '<tr><td>Config</td><td>' + escHtml((d.server || {}).config_file || 'not found') + '</td><td></td></tr>';
  html += '<tr><td>Sync daemon</td><td>' + statusBadge(((d.server || {}).daemon || {}).status) + '</td><td>' + ((((d.server || {}).daemon || {}).running) ? 'running since ' + timeAgo(((d.server || {}).daemon || {}).started_at) : 'not running / unknown') + '</td></tr>';
  html += '</tbody></table></div>';

  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px">';
  html += '<div class="panel"><div class="panel-title">Edge Devices</div>';
  html += '<div style="display:grid;gap:8px;margin-bottom:12px">';
  html += '<div>Server sees edge: ' + statusBadge(edge.server_can_see_edge ? 'healthy' : 'unknown') + '</div>';
  html += '<div>Edge reaches server: ' + statusBadge(edge.edge_can_reach_server === true ? 'healthy' : edge.edge_can_reach_server === false ? 'broken' : 'unknown') + '</div>';
  html += '<div>Local agent: ' + statusBadge(localAgent.status) + ' <span style="color:var(--text3);font-size:12px">' + escHtml(localAgent.device_id || '') + '</span></div></div>';
  if ((edge.tokens || []).length) {
    html += '<table class="runs-table"><thead><tr><th>Device</th><th>Last seen</th><th>Status</th></tr></thead><tbody>';
    (edge.tokens || []).forEach(t => {
      html += '<tr><td>' + escHtml(t.label || t.id) + '</td><td>' + timeAgo(t.last_seen_at) + '</td><td>' + statusBadge(t.status) + '</td></tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<div class="empty"><p>No edge tokens registered.</p></div>';
  }
  if ((localAgent.sources || []).length) {
    html += '<div class="category-label" style="margin-top:14px">Local pending</div><table class="runs-table"><thead><tr><th>Source</th><th>Total</th><th>Uploaded</th><th>Pending</th></tr></thead><tbody>';
    (localAgent.sources || []).forEach(s => {
      html += '<tr><td>' + escHtml(s.source) + '</td><td>' + fmtNum(s.total) + '</td><td>' + fmtNum(s.uploaded_line) + '</td><td>' + fmtNum(s.pending) + '</td></tr>';
    });
    html += '</tbody></table>';
  }
  html += '</div>';

  html += '<div class="panel"><div class="panel-title">Search</div><div style="display:grid;gap:8px">';
  html += '<div>Status: ' + statusBadge((d.search || {}).status) + '</div>';
  html += '<div>Documents: <b>' + fmtNum((d.search || {}).total_documents || 0) + '</b></div>';
  html += '<div>Last indexed: ' + escHtml((d.search || {}).last_indexed || 'never') + '</div>';
  html += '<div style="font-size:12px;color:var(--text3);word-break:break-all">' + escHtml((d.search || {}).db_path || (d.search || {}).reason || '') + '</div>';
  html += '</div></div></div>';

  html += '<div class="panel"><div class="panel-title">Sources</div>';
  if (sources.length) {
    html += '<table class="runs-table"><thead><tr><th>Source</th><th>Status</th><th>Records</th><th>Last Sync</th><th>Where</th><th>Detail</th></tr></thead><tbody>';
    sources.filter(s => s.enabled || s.status !== 'healthy').sort((a,b) => {
      const order = {broken:0,degraded:1,unknown:2,healthy:3};
      return (order[a.status] || 4) - (order[b.status] || 4);
    }).slice(0, 30).forEach(s => {
      html += '<tr><td>' + escHtml(s.display_name || s.name) + '</td><td>' + statusBadge(s.status) + '</td><td>' + fmtNum(s.records) + '</td><td>' + timeAgo(s.last_sync || s.last_data) + '</td><td>' + escHtml(s.where || '') + '</td><td>' + escHtml(s.reason || '') + '</td></tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<div class="empty"><p>No source telemetry.</p></div>';
  }
  html += '</div>';

  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px">';
  html += '<div class="panel"><div class="panel-title">Queues</div><div style="display:grid;gap:8px">';
  html += '<div>Status: ' + statusBadge(queues.status) + '</div><div>Consumers: <b>' + fmtNum((queues.consumers || []).length) + '</b></div><div>Pending records: <b>' + fmtNum(queues.pending_total || 0) + '</b></div></div>';
  if (queues.totals) {
    html += '<table class="runs-table" style="margin-top:12px"><thead><tr><th>Consumer</th><th>Pending</th><th>Updated</th></tr></thead><tbody>';
    Object.keys(queues.totals).forEach(c => {
      html += '<tr><td>' + escHtml(c) + '</td><td>' + fmtNum(queues.totals[c]) + '</td><td>' + timeAgo((queues.updated || {})[c]) + '</td></tr>';
    });
    html += '</tbody></table>';
  }
  html += '</div>';

  html += '<div class="panel"><div class="panel-title">Klava Processing</div><div style="display:grid;gap:8px">';
  html += '<div>Status: ' + statusBadge(klava.status) + '</div><div>Reachable: <b>' + (klava.reachable ? 'yes' : 'no') + '</b></div>';
  html += '<div>Health score: <b>' + escHtml(klava.health_score == null ? 'unknown' : klava.health_score) + '</b></div>';
  html += '<div>Services down: <b>' + fmtNum(((klava.services || {}).down) || 0) + '</b></div><div>Failing jobs: <b>' + fmtNum(((klava.cron || {}).failing) || 0) + '</b></div>';
  if (klava.error) html += '<div style="color:var(--red);font-size:12px">' + escHtml(klava.error) + '</div>';
  html += '<div style="font-size:12px;color:var(--text3);word-break:break-all">' + escHtml(klava.url || '') + '</div></div></div></div>';

  html += '<div class="panel"><div class="panel-title">Recent Failures</div>';
  if (recentErrors.length) {
    html += '<table class="runs-table"><thead><tr><th>When</th><th>Source</th><th>Error</th></tr></thead><tbody>';
    recentErrors.forEach(e => {
      html += '<tr><td>' + timeAgo(e.ts) + '</td><td>' + escHtml(e.source || '') + '</td><td>' + escHtml(e.error || '') + '</td></tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<div class="empty"><p>No recent failures.</p></div>';
  }
  html += '</div>';

  el.innerHTML = html;
}

// ---- Sources Tab ----
function renderSources() {
  const el = document.getElementById('tab-sources');
  if (!sourcesData.length) {
    el.innerHTML = '<div class="empty"><p>No sources registered</p><p style="font-size:12px;color:var(--text3);margin-top:6px">Check <code>vadimgest list</code> on the CLI \u2014 if sources appear there but not here, the web process is using a different config. Refresh this page.</p></div>';
    return;
  }

  const categories = {};
  sourcesData.forEach(s => {
    const cat = s.category || 'other';
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(s);
  });

  const catOrder = ['messaging','email','calendar','meetings','dev','activity','files','social','knowledge','other'];
  let html = '';

  catOrder.forEach(cat => {
    const items = categories[cat];
    if (!items || !items.length) return;
    const catIcon = CAT_ICONS[cat] || '';
    html += '<div class="category-label">' + catIcon + ' ' + escHtml(cat) + '</div>';
    html += '<div class="cards-grid">';
    items.forEach(s => {
      const badge = getBadge(s);
      const icon = SOURCE_ICONS[s.name] || '\\uD83D\\uDCE6';
      html += '<div class="source-card" onclick="openDrawer(\\'' + s.name + '\\')">';
      html += '<div class="source-card-header">';
      html += '<span style="display:flex;align-items:center;gap:8px"><span style="font-size:18px">' + icon + '</span><span class="source-card-name">' + escHtml(s.display_name) + '</span></span>';
      html += badge;
      html += '</div>';
      if (s.description) {
        html += '<div class="source-card-desc">' + escHtml(s.description) + '</div>';
      }
      html += '<div class="source-card-footer">';
      html += '<span class="source-card-records">' + fmtNum(s.records) + ' records</span>';
      html += '<span class="source-card-time">' + timeAgo(s.last_ts) + '</span>';
      html += '</div>';
      html += '</div>';
    });
    html += '</div>';
  });
  el.innerHTML = html;
}

function getBadge(s) {
  if (!s.available) return '<span class="badge badge-gray">unavailable</span>';
  if (!s.enabled) return '<span class="badge badge-gray">disabled</span>';
  if (s.ready && s.ready.ok) return '<span class="badge badge-green">ready</span>';
  if (s.ready && !s.ready.ok) return '<span class="badge badge-yellow">needs setup</span>';
  return '<span class="badge badge-gray">disabled</span>';
}

// ---- Dashboard Tab (home) ----
function getSourceStatus(s) {
  if (!s.available) return 'disabled';
  if (!s.enabled) return 'disabled';
  if (s.ready && !s.ready.ok) return 'setup';
  if (s.ready && s.ready.ok) return 'syncing';
  return 'disabled';
}

function renderDashboard() {
  const el = document.getElementById('tab-dashboard');
  const daemonDot = document.getElementById('daemon-dot');
  const daemonRunning = daemonDot && daemonDot.classList.contains('running');

  // Build data overview from sourcesData
  let totalRecords = 0;
  let totalSize = 0;
  let activeCount = 0;
  sourcesData.forEach(s => {
    totalRecords += s.records || 0;
    if (s.enabled) activeCount++;
  });

  let html = '';

  // Welcome banner (if no config)
  if (!appConfig.has_config) {
    html += '<div class="banner">';
    html += '<h2>Welcome to vadimgest</h2>';
    html += '<p>No configuration file found. Create one to get started with your data sources.</p>';
    html += '<button class="btn btn-primary" onclick="createConfig()">Create Config</button>';
    html += '</div>';
  }

  // Search bar
  html += '<div class="search-bar">';
  html += '<input id="dash-search" placeholder="Search all data..." onkeydown="if(event.key===&quot;Enter&quot;)dashSearch()">';
  html += '<select id="dash-source-filter">';
  html += '<option value="">All sources</option>';
  sourcesData.forEach(s => {
    if (s.records > 0) html += '<option value="' + escHtml(s.name) + '">' + escHtml(s.name) + '</option>';
  });
  html += '</select>';
  html += '<button onclick="dashSearch()">Search</button>';
  html += '</div>';

  // Search results area (hidden until search)
  html += '<div id="dash-search-results"></div>';

  // KPI row
  html += '<div class="kpi-row">';
  html += '<div class="kpi"><div class="kpi-val">' + fmtNum(totalRecords) + '</div><div class="kpi-label">Total Records</div></div>';
  html += '<div class="kpi"><div class="kpi-val">' + sourcesData.length + '</div><div class="kpi-label">Sources</div></div>';
  html += '<div class="kpi"><div class="kpi-val" style="color:var(--green)">' + activeCount + '</div><div class="kpi-label">Active</div></div>';
  if (daemonRunning) {
    html += '<div class="kpi"><div class="kpi-val" style="color:var(--green);font-size:14px">Syncing</div><div class="kpi-label">Daemon</div></div>';
  } else {
    html += '<div class="kpi"><div class="kpi-val" style="color:var(--text3);font-size:14px">Stopped</div><div class="kpi-label">Daemon</div></div>';
  }
  if (searchHealth && searchHealth.available) {
    html += '<div class="kpi"><div class="kpi-val" style="color:var(--accent2);font-size:14px">' + fmtNum(searchHealth.total_documents) + ' docs</div><div class="kpi-label">Search Index</div></div>';
  } else {
    const reason = searchHealth ? searchHealth.reason : 'Not loaded';
    html += '<div class="kpi"><div class="kpi-val" style="color:var(--red);font-size:14px">N/A</div><div class="kpi-label" title="' + escHtml(reason) + '">Search Index</div></div>';
  }
  html += '</div>';

  // Source cards in panel
  html += '<div class="panel">';
  html += '<div class="panel-title">Sources</div>';
  html += '<div class="src-grid">';
  const sorted = [...sourcesData].sort((a, b) => {
    const statusOrder = {syncing: 0, setup: 1, error: 2, disabled: 3};
    const sa = statusOrder[getSourceStatus(a)] || 3;
    const sb = statusOrder[getSourceStatus(b)] || 3;
    if (sa !== sb) return sa - sb;
    return (b.records || 0) - (a.records || 0);
  });
  const maxRec = Math.max(...sorted.map(s => s.records || 0));
  sorted.forEach(s => {
    const status = getSourceStatus(s);
    const isActive = s.enabled && status === 'syncing';
    const isWarn = s.enabled && status === 'setup';
    const dotCls = isActive ? 'active' : isWarn ? 'warn' : 'off';
    const badgeLbl = isActive ? 'Active' : isWarn ? 'Setup' : s.enabled ? 'Error' : 'Off';
    const cardCls = 'src-card' + (!s.enabled ? ' disabled' : '') + (isWarn ? ' warn' : '');
    const barPct = s.records > 0 ? Math.max(3, Math.min(100, (s.records / (maxRec || 1)) * 100)) : 0;
    html += '<div class="' + cardCls + '" onclick="openDrawer(\\'' + s.name + '\\')">';
    html += '<div class="src-header">';
    html += '<span class="src-dot ' + dotCls + '"></span>';
    html += '<span class="src-name">' + escHtml(s.display_name) + '</span>';
    html += '<span class="src-badge ' + dotCls + '">' + badgeLbl + '</span>';
    html += '</div>';
    if (s.description) html += '<div class="src-desc">' + escHtml(s.description) + '</div>';
    if (s.records > 0) {
      html += '<div class="src-bar"><div class="src-bar-fill" style="width:' + barPct + '%"></div></div>';
      html += '<div class="src-stats"><span class="highlight">' + fmtNum(s.records) + ' records</span><span>' + timeAgo(s.last_ts) + '</span></div>';
    } else {
      html += '<div class="src-stats"><span>No data</span></div>';
    }
    html += '</div>';
  });
  html += '</div></div>';

  // Source health bar in panel
  html += '<div class="panel" style="margin-top:0">';
  html += '<div class="panel-title">Source Health</div>';
  html += '<div class="health-bar">';
  const enabledSources = (sourcesData || []).filter(s => s.enabled);
  if (enabledSources.length > 0) {
    const latestBySource = {};
    [...runsData].reverse().forEach(r => {
      if (!latestBySource[r.source]) latestBySource[r.source] = r;
    });
    enabledSources.forEach(s => {
      const latest = latestBySource[s.name];
      let cls = 'h-never';
      let tip = 'Never synced';
      if (latest) {
        if (latest.status === 'error') {
          cls = 'h-error';
          tip = 'Error: ' + (latest.error || 'unknown');
        } else {
          const age = (Date.now() - new Date(latest.ts).getTime()) / 1000;
          if (age < 3600) { cls = 'h-ok'; tip = timeAgo(latest.ts); }
          else if (age < 86400) { cls = 'h-stale'; tip = timeAgo(latest.ts); }
          else { cls = 'h-stale'; tip = timeAgo(latest.ts) + ' (stale)'; }
        }
      }
      html += '<div class="health-chip ' + cls + '" title="' + escHtml(tip) + '">';
      html += '<span class="h-dot"></span>';
      html += escHtml(s.display_name || s.name);
      html += '</div>';
    });
  } else {
    html += '<div style="font-size:12px;color:var(--text3);display:flex;align-items:center;gap:10px">';
    html += '<span>No sources enabled yet.</span>';
    html += '<button class="btn btn-sm btn-primary" onclick="localStorage.removeItem(&quot;vadimgest_wizard_done&quot;);openWizard()">Run setup wizard</button>';
    html += '</div>';
  }
  html += '</div></div>';

  // Recent activity feed in panel
  html += '<div class="panel">';
  html += '<div class="panel-title">Recent Activity</div>';
  if (runsData.length > 0) {
    const recent = [...runsData].reverse().slice(0, 15);
    recent.forEach((r, i) => {
      const isError = r.status === 'error';
      const hasSummary = r.summary && r.summary.length > 0;
      const hasDetail = hasSummary || isError;
      const dur = r.duration_sec !== undefined ? r.duration_sec.toFixed(1) + 's' : '';
      const countLabel = isError ? '' : (r.count > 0 ? '+' + r.count.toLocaleString() : 'no new');

      html += '<div class="activity-entry' + (isError ? ' ae-error' : '') + '" id="ae-' + i + '">';
      html += '<div class="activity-row" onclick="toggleAe(' + i + ')">';
      html += '<span class="ae-arrow">' + (hasDetail ? '\u25B6' : '') + '</span>';
      html += '<span class="ae-time">' + timeAgo(r.ts) + '</span>';
      html += '<span class="ae-source">' + escHtml(r.source) + '</span>';
      if (isError) {
        html += '<span class="badge badge-red">error</span>';
      } else if (r.count > 0) {
        html += '<span class="ae-count badge badge-green">' + countLabel + '</span>';
      } else {
        html += '<span class="ae-count" style="color:var(--text3)">' + countLabel + '</span>';
      }
      html += '<span class="ae-dur">' + dur + '</span>';
      html += '</div>';

      if (hasDetail) {
        html += '<div class="activity-detail">';
        if (isError) {
          html += '<div class="ad-error">' + escHtml(r.error || 'Unknown error') + '</div>';
        }
        if (hasSummary) {
          r.summary.forEach(s => {
            html += '<div class="ad-item">' + escHtml(s) + '</div>';
          });
        } else if (!isError) {
          html += '<div style="color:var(--text3)">No new records</div>';
        }
        html += '</div>';
      }
      html += '</div>';
    });
    if (runsData.length > 15) {
      html += '<div style="text-align:center;padding:8px"><span style="font-size:12px;color:var(--text3)">' + runsData.length + ' total runs</span></div>';
    }
  } else {
    html += '<div class="empty"><p>No sync runs yet. Enable a source and run your first sync.</p></div>';
  }
  html += '</div>';

  el.innerHTML = html;
}

function dashSearch() {
  var q = document.getElementById('dash-search').value.trim();
  if (!q) {
    document.getElementById('dash-search-results').innerHTML = '';
    return;
  }
  var source = document.getElementById('dash-source-filter').value;
  var el = document.getElementById('dash-search-results');
  el.innerHTML = '<div style="padding:20px;color:var(--text3)">Searching...</div>';
  var url = '/api/data/search?q=' + encodeURIComponent(q) + '&limit=20';
  if (source) url += '&source=' + encodeURIComponent(source);
  fetch(url).then(r => r.json()).then(data => {
    if (data.error) { el.innerHTML = '<div style="color:var(--accent);padding:20px">' + escHtml(data.error) + '</div>'; return; }
    var html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
    html += '<h3 style="font-size:16px;font-weight:600;color:var(--text);margin:0">Results for "' + escHtml(data.query) + '" <span style="color:var(--text3);font-weight:400;font-size:13px">' + data.results.length + ' found</span></h3>';
    html += '<button class="btn btn-sm" onclick="document.getElementById(\\'dash-search-results\\').innerHTML=\\'\\';document.getElementById(\\'dash-search\\').value=\\'\\'">Clear</button>';
    html += '</div>';
    data.results.forEach(r => {
      html += '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:12px 14px;margin-bottom:6px">';
      html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">';
      html += '<span style="background:var(--accent);color:white;padding:2px 8px;border-radius:10px;font-size:11px">' + escHtml(r.source) + '</span>';
      html += '<span style="font-size:13px;font-weight:600;color:var(--text)">' + escHtml(r.title) + '</span>';
      if (r.chat) html += '<span style="font-size:11px;color:var(--text3)">' + escHtml(r.chat) + '</span>';
      html += '</div>';
      html += '<div style="font-size:12px;color:var(--text2);line-height:1.5">' + escHtml(r.snippet) + '</div>';
      html += '</div>';
    });
    if (data.results.length === 0) {
      html += '<div style="padding:20px;color:var(--text3);text-align:center">No results found</div>';
    }
    el.innerHTML = html;
  }).catch(e => {
    el.innerHTML = '<div style="color:var(--accent);padding:20px">Error: ' + escHtml(e.message) + '</div>';
  });
}

// ---- Sources Page (detailed management) ----
function renderSourcesPage() {
  const el = document.getElementById('tab-sources');
  if (!sourcesData.length) {
    el.innerHTML = '<div class="empty"><p>No sources registered</p><p style="font-size:12px;color:var(--text3);margin-top:6px">Check <code>vadimgest list</code> on the CLI \u2014 if sources appear there but not here, the web process is using a different config. Refresh this page.</p></div>';
    return;
  }

  let html = '';

  // Wizard re-run button
  html += '<div style="display:flex;justify-content:flex-end;margin-bottom:12px">';
  html += '<button class="btn btn-sm" onclick="localStorage.removeItem(\\'vadimgest_wizard_done\\');openWizard()">&#x2728; Run Setup Wizard</button>';
  html += '</div>';

  // Quick Setup section - sources that need deps
  const needsSetup = sourcesData.filter(s => {
    if (!s.enabled) return false;
    return s.ready && !s.ready.ok;
  });
  if (needsSetup.length > 0) {
    html += '<div class="setup-banner">';
    html += '<div class="setup-banner-title">' + needsSetup.length + ' source' + (needsSetup.length > 1 ? 's' : '') + ' need setup</div>';
    needsSetup.forEach(s => {
      const missing = (s.ready && s.ready.missing) || [];
      const summary = missing.map(m => m.replace('Python: ', '').replace('CLI: ', '').replace('Credential: ', '')).join(', ');
      html += '<div class="setup-banner-item">';
      html += '<span><span class="status-dot setup" style="margin-right:8px"></span>' + escHtml(s.display_name) + ' - <span style="color:var(--text3);font-size:12px">' + escHtml(summary) + '</span></span>';
      html += '<button class="btn btn-sm" onclick="openDrawer(\\'' + s.name + '\\')">Fix</button>';
      html += '</div>';
    });
    html += '</div>';
  }

  // Enabled sources
  const enabledSources = sourcesData.filter(s => s.enabled);
  const maxRecSrc = Math.max(...sourcesData.map(s => s.records || 0));
  if (enabledSources.length > 0) {
    html += '<div class="panel">';
    html += '<div class="panel-title">Enabled Sources</div>';
    html += '<div class="src-grid">';
    enabledSources.forEach(s => {
      const status = getSourceStatus(s);
      const isActive = s.enabled && status === 'syncing';
      const isWarn = s.enabled && status === 'setup';
      const dotCls = isActive ? 'active' : isWarn ? 'warn' : 'off';
      const badgeLbl = isActive ? 'Active' : isWarn ? 'Setup' : 'Error';
      const cardCls = 'src-card' + (isWarn ? ' warn' : '');
      const barPct = s.records > 0 ? Math.max(3, Math.min(100, (s.records / (maxRecSrc || 1)) * 100)) : 0;
      html += '<div class="' + cardCls + '" onclick="openDrawer(\\'' + s.name + '\\')">';
      html += '<div class="src-header">';
      html += '<span class="src-dot ' + dotCls + '"></span>';
      html += '<span class="src-name">' + escHtml(s.display_name) + '</span>';
      html += '<span class="src-badge ' + dotCls + '">' + badgeLbl + '</span>';
      html += '</div>';
      if (s.description) html += '<div class="src-desc">' + escHtml(s.description) + '</div>';
      if (s.records > 0) {
        html += '<div class="src-bar"><div class="src-bar-fill" style="width:' + barPct + '%"></div></div>';
        html += '<div class="src-stats"><span class="highlight">' + fmtNum(s.records) + ' records</span><span>' + timeAgo(s.last_ts) + '</span></div>';
      } else {
        html += '<div class="src-stats"><span>No data</span></div>';
      }
      html += '</div>';
    });
    html += '</div></div>';
  }

  // Available (disabled) sources - collapsed
  const disabledSources = sourcesData.filter(s => !s.enabled);
  if (disabledSources.length > 0) {
    html += '<div class="collapsible-header" onclick="var c=this.nextElementSibling;var a=this.querySelector(\\'.collapsible-arrow\\');if(c.style.display===\\'none\\'){c.style.display=\\'block\\';a.classList.add(\\'open\\');}else{c.style.display=\\'none\\';a.classList.remove(\\'open\\');}">';
    html += '<span>' + disabledSources.length + ' more sources available</span>';
    html += '<span class="collapsible-arrow">&#9654;</span>';
    html += '</div>';
    html += '<div style="display:none">';
    html += '<div class="panel"><div class="src-grid">';
    disabledSources.forEach(s => {
      html += '<div class="src-card disabled" onclick="openDrawer(\\'' + s.name + '\\')">';
      html += '<div class="src-header">';
      html += '<span class="src-dot off"></span>';
      html += '<span class="src-name">' + escHtml(s.display_name) + '</span>';
      html += '<span class="src-badge off">Off</span>';
      html += '</div>';
      if (s.description) html += '<div class="src-desc">' + escHtml(s.description) + '</div>';
      html += '<div class="src-stats"><span>No data</span></div>';
      html += '</div>';
    });
    html += '</div></div></div>';
  }

  // Global Settings section - collapsed
  html += '<div class="collapsible-header" onclick="var c=this.nextElementSibling;var a=this.querySelector(\\'.collapsible-arrow\\');if(c.style.display===\\'none\\'){c.style.display=\\'block\\';a.classList.add(\\'open\\');loadGlobalSettings();}else{c.style.display=\\'none\\';a.classList.remove(\\'open\\');}">';
  html += '<span>Global Settings</span>';
  html += '<span class="collapsible-arrow">&#9654;</span>';
  html += '</div>';
  html += '<div style="display:none" id="global-settings-panel">';
  html += '<div class="empty"><p>Loading...</p></div>';
  html += '</div>';

  // Search Index section - collapsed
  html += '<div class="collapsible-header" onclick="var c=this.nextElementSibling;var a=this.querySelector(\\'.collapsible-arrow\\');if(c.style.display===\\'none\\'){c.style.display=\\'block\\';a.classList.add(\\'open\\');loadSearchSettings();}else{c.style.display=\\'none\\';a.classList.remove(\\'open\\');}">';
  html += '<span>Search Index</span>';
  if (searchHealth && searchHealth.available) {
    html += '<span style="font-size:12px;color:var(--green);margin-left:10px">' + fmtNum(searchHealth.total_documents) + ' docs / ' + searchHealth.size_mb + 'MB</span>';
  }
  html += '<span class="collapsible-arrow">&#9654;</span>';
  html += '</div>';
  html += '<div style="display:none" id="search-settings-panel">';
  html += '<div class="empty"><p>Loading...</p></div>';
  html += '</div>';

  // Pipeline section (queues + consumers) - collapsed
  html += '<div class="collapsible-header" onclick="var c=this.nextElementSibling;var a=this.querySelector(\\'.collapsible-arrow\\');if(c.style.display===\\'none\\'){c.style.display=\\'block\\';a.classList.add(\\'open\\');if(!queuesData)fetchQueues();if(!consumersData)fetchConsumers();}else{c.style.display=\\'none\\';a.classList.remove(\\'open\\');}">';
  html += '<span>Pipeline</span>';
  html += '<span class="collapsible-arrow">&#9654;</span>';
  html += '</div>';
  html += '<div style="display:none">';
  html += '<div class="category-label">Queues</div>';
  html += '<div id="pipeline-queues"><div class="empty"><p>Loading...</p></div></div>';
  html += '<div class="category-label" style="margin-top:20px">Consumers</div>';
  html += '<div id="pipeline-consumers"><div class="empty"><p>Loading...</p></div></div>';
  html += '</div>';

  el.innerHTML = html;
}

// ---- Edge Sync Page ----
function renderEdgePage() {
  const el = document.getElementById('tab-edge');
  const st = edgeStatus || {};
  const cfg = st.config || {};
  const tokens = st.tokens || [];
  const sourceSet = new Set(cfg.sources || []);
  let html = '';

  html += '<div class="panel">';
  html += '<div class="panel-title">Server Tokens</div>';
  html += '<div style="display:grid;grid-template-columns:1fr auto;gap:10px;margin-bottom:12px">';
  html += '<input id="edge-token-label" placeholder="Device label, e.g. Vadim MacBook" style="background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px">';
  html += '<button class="btn btn-primary" onclick="edgeCreateToken()">Generate Token</button>';
  html += '</div>';
  html += '<div style="font-size:12px;color:var(--text3);margin-bottom:10px">Ingest URL: <code style="background:var(--bg3);padding:2px 6px;border-radius:4px">' + escHtml(st.ingest_url || '') + '</code></div>';
  html += '<div id="edge-new-token"></div>';
  if (tokens.length) {
    html += '<table class="runs-table"><thead><tr><th>Label</th><th>Created</th><th>Last seen</th><th>Status</th><th></th></tr></thead><tbody>';
    tokens.forEach(t => {
      html += '<tr>';
      html += '<td>' + escHtml(t.label || t.id) + '</td>';
      html += '<td class="mono">' + escHtml(t.created_at || '') + '</td>';
      html += '<td>' + timeAgo(t.last_seen_at) + '</td>';
      html += '<td>' + (t.active ? '<span class="badge badge-green">active</span>' : '<span class="badge badge-gray">revoked</span>') + '</td>';
      html += '<td>' + (t.active ? '<button class="btn btn-sm" onclick="edgeRevokeToken(\\'' + escHtml(t.id) + '\\')">Revoke</button>' : '') + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<div class="empty"><p>No edge tokens yet.</p></div>';
  }
  html += '</div>';

  html += '<div class="panel">';
  html += '<div class="panel-title">Local Edge Agent</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px">';
  html += '<div class="field"><label>Server URL</label><input id="edge-server-url" value="' + escHtml(cfg.server_url || '') + '" placeholder="https://server.example.com" style="width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px"></div>';
  html += '<div class="field"><label>Token</label><input id="edge-token" type="password" placeholder="' + (cfg.token_configured ? 'configured - leave blank to keep' : 'paste generated token') + '" style="width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px"></div>';
  html += '<div class="field"><label>Device ID</label><input id="edge-device-id" value="' + escHtml(cfg.device_id || '') + '" style="width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px"></div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">';
  html += '<div class="field"><label>Interval seconds</label><input id="edge-interval" type="number" min="1" value="' + escHtml(cfg.interval_seconds || 300) + '" style="width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px"></div>';
  html += '<div class="field"><label>Batch size</label><input id="edge-batch-size" type="number" min="1" max="1000" value="' + escHtml(cfg.batch_size || 100) + '" style="width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 10px"></div>';
  html += '</div></div>';
  html += '<label class="toggle" style="margin:12px 0"><input type="checkbox" id="edge-enabled" ' + (cfg.enabled ? 'checked' : '') + '><span class="toggle-track"></span><span style="margin-left:8px">Edge agent enabled in config</span></label>';
  html += '<div style="font-size:12px;color:var(--text3);margin-bottom:8px">If no source is selected, the agent uploads all enabled local sources.</div>';
  html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px">';
  (sourcesData || []).forEach(s => {
    const checked = sourceSet.has(s.name);
    html += '<label style="display:flex;align-items:center;gap:6px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:6px 8px;font-size:12px">';
    html += '<input type="checkbox" class="edge-source" value="' + escHtml(s.name) + '" ' + (checked ? 'checked' : '') + '>';
    html += escHtml(s.name);
    html += '</label>';
  });
  html += '</div>';
  html += '<div style="display:flex;flex-wrap:wrap;gap:8px">';
  html += '<button class="btn btn-primary" onclick="edgeSaveConfig()">Save</button>';
  html += '<button class="btn" onclick="edgeTestConnection()">Test Connection</button>';
  html += '<button class="btn" onclick="edgeRunOnce()">Run Once</button>';
  html += '<button class="btn" onclick="edgeInstallAgent()">Install/Start Agent</button>';
  html += '<button class="btn" onclick="edgeUninstallAgent()">Stop/Uninstall</button>';
  html += '</div>';
  html += '<div id="edge-agent-output" style="margin-top:12px"></div>';
  html += '</div>';

  el.innerHTML = html;
}

function edgeConfigPayload() {
  const selected = Array.from(document.querySelectorAll('.edge-source:checked')).map(i => i.value);
  const payload = {
    enabled: document.getElementById('edge-enabled').checked,
    server_url: document.getElementById('edge-server-url').value.trim(),
    device_id: document.getElementById('edge-device-id').value.trim(),
    interval_seconds: parseInt(document.getElementById('edge-interval').value || '300', 10),
    batch_size: parseInt(document.getElementById('edge-batch-size').value || '100', 10),
    sources: selected.length ? selected : null
  };
  const token = document.getElementById('edge-token').value.trim();
  if (token) payload.token = token;
  return payload;
}

async function edgeCreateToken() {
  const label = document.getElementById('edge-token-label').value.trim();
  const res = await fetch('/api/edge/tokens', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({label})});
  const data = await res.json();
  if (!data.ok) { showToast(data.error || 'Token generation failed', 'error'); return; }
  await fetchEdgeStatus();
  renderEdgePage();
  document.getElementById('edge-new-token').innerHTML = '<div style="background:var(--green-bg);border:1px solid var(--green);border-radius:8px;padding:10px;margin-bottom:12px"><div style="font-size:12px;color:var(--text2);margin-bottom:4px">Copy this token now. It will not be shown again.</div><code style="word-break:break-all">' + escHtml(data.token) + '</code></div>';
  showToast('Token generated', 'success');
}

async function edgeRevokeToken(id) {
  const res = await fetch('/api/edge/tokens/' + encodeURIComponent(id), {method:'DELETE'});
  const data = await res.json();
  if (!data.ok) { showToast(data.error || 'Revoke failed', 'error'); return; }
  showToast('Token revoked', 'success');
  await fetchEdgeStatus();
  renderEdgePage();
}

async function edgeSaveConfig() {
  const res = await fetch('/api/edge/config', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(edgeConfigPayload())});
  const data = await res.json();
  if (!data.ok) { showToast(data.error || 'Save failed', 'error'); return; }
  document.getElementById('edge-token').value = '';
  showToast('Edge config saved', 'success');
  await fetchEdgeStatus();
  renderEdgePage();
}

async function edgeTestConnection() {
  const out = document.getElementById('edge-agent-output');
  out.innerHTML = '<div class="empty"><p>Testing connection...</p></div>';
  const res = await fetch('/api/edge/test', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(edgeConfigPayload())});
  const data = await res.json();
  out.innerHTML = '<pre style="background:var(--bg3);padding:12px;border-radius:8px;white-space:pre-wrap">' + escHtml(JSON.stringify(data, null, 2)) + '</pre>';
  showToast(data.ok ? 'Edge connection works' : 'Edge connection failed', data.ok ? 'success' : 'error');
}

async function edgeRunOnce() {
  await edgeSaveConfig();
  const out = document.getElementById('edge-agent-output');
  out.innerHTML = '<div class="empty"><p>Running edge-agent once...</p></div>';
  const res = await fetch('/api/edge/agent/run-once', {method:'POST'});
  const data = await res.json();
  out.innerHTML = '<pre style="background:var(--bg3);padding:12px;border-radius:8px;white-space:pre-wrap">' + escHtml(JSON.stringify(data, null, 2)) + '</pre>';
  showToast(data.ok ? 'Edge run complete' : 'Edge run failed', data.ok ? 'success' : 'error');
  await refresh();
}

async function edgeInstallAgent() {
  await edgeSaveConfig();
  const res = await fetch('/api/edge/agent/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({interval:parseInt(document.getElementById('edge-interval').value || '300', 10)})});
  const data = await res.json();
  showToast(data.ok ? 'Edge agent installed' : (data.error || 'Install failed'), data.ok ? 'success' : 'error');
}

async function edgeUninstallAgent() {
  const res = await fetch('/api/edge/agent/install', {method:'DELETE'});
  const data = await res.json();
  showToast(data.ok ? 'Edge agent removed' : (data.error || 'Remove failed'), data.ok ? 'success' : 'error');
}

// ---- Docs Page (merged Docs + Agent) ----
function renderDocsPage() {
  const el = document.getElementById('tab-docs');
  const codeStyle = 'background:var(--bg3);padding:12px 16px;border-radius:8px;font-family:JetBrains Mono,monospace;font-size:12px;white-space:pre;overflow-x:auto;margin:0';
  const sectionStyle = 'background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:16px';
  const headerStyle = 'font-size:20px;font-weight:600;margin-bottom:16px';
  const copyBtnStyle = 'position:absolute;top:8px;right:8px;background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-family:inherit';
  let html = '<div style="max-width:800px">';

  // Getting Started
  html += '<h2 style="' + headerStyle + '">Getting Started</h2>';
  html += '<div style="color:var(--text2);margin-bottom:24px;line-height:1.7">';
  html += '<p style="margin-bottom:8px">1. Install vadimgest: <code style="background:var(--bg3);padding:2px 6px;border-radius:4px;font-family:JetBrains Mono,monospace;font-size:12px">pip install vadimgest</code></p>';
  html += '<p style="margin-bottom:8px">2. Start the dashboard: <code style="background:var(--bg3);padding:2px 6px;border-radius:4px;font-family:JetBrains Mono,monospace;font-size:12px">vadimgest serve</code></p>';
  html += '<p style="margin-bottom:8px">3. Enable sources in the Sources tab - click a source card, toggle it on, and configure credentials if needed</p>';
  html += '<p style="margin-bottom:8px">4. Start the sync daemon using the status indicator in the header, or it will auto-start when you enable a source</p>';
  html += '</div>';

  // How Syncing Works
  html += '<h2 style="' + headerStyle + '">How Syncing Works</h2>';
  html += '<div style="color:var(--text2);margin-bottom:24px;line-height:1.7">';
  html += '<p style="margin-bottom:8px">The <strong>sync daemon</strong> runs in the background and periodically fetches new data from all enabled sources. The default interval is 300 seconds (5 minutes).</p>';
  html += '<p style="margin-bottom:8px">Each source has its own syncer that connects to the data source, fetches new records since the last sync, and stores them in JSONL format.</p>';
  html += '<p style="margin-bottom:8px">You can also trigger a manual sync for any source using the <strong>Sync Now</strong> button in the source drawer.</p>';
  html += '</div>';

  // CLI Reference
  html += '<div style="' + sectionStyle + '">';
  html += '<h2 style="' + headerStyle + '">CLI Reference</h2>';
  html += '<div style="position:relative"><button style="' + copyBtnStyle + '" onclick="copyBlock(this)">Copy</button>';
  html += '<pre style="' + codeStyle + '">';
  html += '## Install\\npip install vadimgest\\n\\n';
  html += '## Start dashboard\\nvadimgest serve\\n\\n';
  html += '## Or use CLI directly\\n';
  html += 'vadimgest list                       # show available sources\\n';
  html += 'vadimgest sync                       # sync all enabled sources\\n';
  html += 'vadimgest search "query" --md --raw  # full-text search\\n';
  html += 'vadimgest stats                      # record counts\\n';
  html += 'vadimgest health                     # health check';
  html += '</pre></div></div>';

  // Python API Reference
  html += '<div style="' + sectionStyle + '">';
  html += '<h2 style="' + headerStyle + '">Python API Reference</h2>';
  html += '<div style="position:relative"><button style="' + copyBtnStyle + '" onclick="copyBlock(this)">Copy</button>';
  html += '<pre style="' + codeStyle + '">';
  html += '# DataStore - append-only JSONL storage\\n';
  html += 'from vadimgest.store import DataStore\\n';
  html += 'store = DataStore("~/.local/share/vadimgest")\\n\\n';
  html += '# Read new records (consumer pattern)\\n';
  html += 'for record in store.read_new("telegram", consumer="my-agent"):\\n';
  html += '    process(record)\\n';
  html += 'store.commit("telegram", consumer="my-agent")\\n\\n';
  html += '# Search\\n';
  html += 'from vadimgest.search import search, index\\n';
  html += 'results = search("query", md=True, raw=True, n=10)\\n';
  html += 'for r in results:\\n';
  html += '    print(r.source, r.title, r.snippet)\\n\\n';
  html += '# Source management\\n';
  html += 'from vadimgest.ingest.sources import all_source_names, get_syncer_class\\n';
  html += 'from vadimgest.config import get_source_config';
  html += '</pre></div></div>';

  // REST API Reference
  html += '<div style="' + sectionStyle + '">';
  html += '<h2 style="' + headerStyle + '">REST API Reference</h2>';
  html += '<table style="width:100%;border-collapse:collapse;font-size:13px">';
  html += '<thead><tr style="text-align:left;border-bottom:2px solid var(--border)">';
  html += '<th style="padding:8px 12px">Method</th><th style="padding:8px 12px">Endpoint</th><th style="padding:8px 12px">Description</th></tr></thead><tbody>';
  var endpoints = [
    ['GET', '/api/sources', 'List all sources with status, deps, config'],
    ['PUT', '/api/sources/:name', 'Update source config or enable/disable'],
    ['POST', '/api/sources/:name/sync', 'Sync a source (synchronous, returns count)'],
    ['GET', '/api/stats', 'Record counts per source'],
    ['GET', '/api/runs', 'Recent sync history'],
    ['GET', '/api/consumers', 'Consumer checkpoint positions'],
    ['GET', '/api/queues', 'Queue depths per source per consumer'],
    ['GET', '/api/observatory', 'Unified health for vadimgest, edge devices, search, queues, and Klava'],
    ['GET', '/api/config', 'Current config and data directory'],
    ['POST', '/api/config/init', 'Initialize config file'],
    ['PUT', '/api/credentials', 'Save environment variables'],
    ['GET', '/api/daemon', 'Daemon status (running, interval, sources)'],
    ['POST', '/api/daemon/start', 'Start background sync daemon'],
    ['POST', '/api/daemon/stop', 'Stop daemon'],
    ['POST', '/api/install', 'Install CLI tool (brew/npm/pipx)'],
    ['GET', '/api/events', 'SSE stream for real-time updates'],
    ['GET', '/api/data/overview', 'Data overview with record counts and sizes'],
    ['GET', '/api/data/search', 'Full-text search across all data'],
    ['GET', '/api/data/browse', 'Browse records from a specific source'],
    ['POST', '/api/edge/events/batch', 'Authenticated edge-agent batch ingest'],
    ['GET', '/api/edge/status', 'Edge status, ingest URL, token metadata'],
    ['GET/POST', '/api/edge/tokens', 'List or generate edge tokens'],
    ['DELETE', '/api/edge/tokens/:id', 'Revoke an edge token'],
    ['GET/PUT', '/api/edge/config', 'Read or save local edge-agent config'],
    ['POST', '/api/edge/test', 'Test edge server URL and token'],
    ['POST', '/api/edge/agent/run-once', 'Run one edge-agent upload cycle'],
    ['GET', '/api/edge/agent', 'Local edge-agent service status'],
    ['POST/DELETE', '/api/edge/agent/install', 'Install/start or stop/remove edge service'],
  ];
  endpoints.forEach(function(ep) {
    html += '<tr style="border-bottom:1px solid var(--border)">';
    html += '<td style="padding:8px 12px;font-weight:600;color:var(--accent)">' + ep[0] + '</td>';
    html += '<td style="padding:8px 12px;font-family:JetBrains Mono,monospace;font-size:12px">' + ep[1] + '</td>';
    html += '<td style="padding:8px 12px;color:var(--text2)">' + ep[2] + '</td>';
    html += '</tr>';
  });
  html += '</tbody></table></div>';

  // Agent Prompt Template
  html += '<div style="' + sectionStyle + '">';
  html += '<h2 style="' + headerStyle + '">Agent Prompt Template</h2>';
  html += '<p style="color:var(--text2);margin-bottom:12px;font-size:13px">Copy this into your agent\\\'s system prompt or CLAUDE.md:</p>';
  html += '<div style="position:relative"><button style="' + copyBtnStyle + '" onclick="copyBlock(this)">Copy</button>';
  html += '<pre style="' + codeStyle + '">';
  html += '## vadimgest - Personal Data Search\\n\\n';
  html += 'vadimgest is installed on this machine. Use it to search across personal data sources.\\n\\n';
  html += '### Search (most common)\\n';
  html += 'vadimgest search "query" --md --raw    # search everything\\n';
  html += 'vadimgest search "query" -s telegram   # specific source\\n';
  html += 'vadimgest search "query" --raw --chat "Name"  # filter by chat\\n';
  html += 'vadimgest search "query" --md --folder "Deals" # filter by folder\\n';
  html += 'vadimgest search "query" --raw --json  # JSON output for parsing\\n\\n';
  html += '### Data Management\\n';
  html += 'vadimgest sync                    # sync all enabled sources\\n';
  html += 'vadimgest read -c myagent         # read new records since checkpoint\\n';
  html += 'vadimgest commit -c myagent       # advance checkpoint after processing\\n';
  html += 'vadimgest stats                   # see what data is available\\n\\n';
  html += '### Web Dashboard\\n';
  html += 'vadimgest serve                   # start dashboard at http://localhost:8484';
  html += '</pre></div></div>';

  // Adding a Custom Source
  html += '<div style="' + sectionStyle + '">';
  html += '<h2 style="' + headerStyle + '">Adding a Custom Source</h2>';
  html += '<p style="color:var(--text2);margin-bottom:12px;font-size:13px;line-height:1.6">';
  html += 'Create a folder with a syncer.py anywhere on disk, then point vadimgest to it via environment variable or config.</p>';
  html += '<div style="position:relative"><button style="' + copyBtnStyle + '" onclick="copyBlock(this)">Copy</button>';
  html += '<pre style="' + codeStyle + '">';
  html += '# 1. Create your source directory\\n';
  html += 'mkdir -p ~/.vadimgest-sources/my_source\\n\\n';
  html += '# 2. Create syncer.py with a BaseSyncer subclass\\n';
  html += 'cat > ~/.vadimgest-sources/my_source/syncer.py << \\\'PYEOF\\\'\\n';
  html += 'from vadimgest.ingest.sources.base import CronSyncer\\n\\n';
  html += 'class MySourceSyncer(CronSyncer):\\n';
  html += '    source_name = "my_source"\\n';
  html += '    display_name = "My Source"\\n';
  html += '    description = "Description of what this syncs"\\n';
  html += '    category = "knowledge"  # messaging|email|files|dev|activity|meetings|social|knowledge\\n';
  html += '    dependencies = {"python": [], "cli": [], "credentials": [], "os": []}\\n';
  html += '    config_schema = {\\n';
  html += '        "data_path": {"type": "path", "default": "~/data", "description": "Path to data"},\\n';
  html += '    }\\n\\n';
  html += '    def fetch_new(self, state, limit=1000):\\n';
  html += '        # Yield dicts with at least "id" and "type" keys\\n';
  html += '        # state.last_id / state.last_ts track position\\n';
  html += '        for item in self.read_data():\\n';
  html += '            yield {"id": item.id, "type": "document", "title": item.title, "content": item.text}\\n';
  html += 'PYEOF\\n\\n';
  html += '# 3. Tell vadimgest where to find custom sources\\n';
  html += 'export VADIMGEST_SOURCES_DIR=~/.vadimgest-sources\\n\\n';
  html += '# Or add to ~/.config/vadimgest/config.yaml:\\n';
  html += '# custom_sources_dir: ~/.vadimgest-sources\\n\\n';
  html += '# 4. Verify it appears\\n';
  html += 'vadimgest list  # should show "my_source"';
  html += '</pre></div></div>';

  // Source Setup Reference (from original Docs tab)
  html += '<h2 style="' + headerStyle + '">Source Setup Reference</h2>';
  if (sourcesData.length) {
    sourcesData.forEach(s => {
      html += '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:12px">';
      html += '<div style="font-weight:600;font-size:14px;margin-bottom:4px">' + escHtml(s.display_name) + '</div>';
      if (s.description) html += '<div style="color:var(--text2);font-size:13px;margin-bottom:8px">' + escHtml(s.description) + '</div>';

      const deps = s.dependencies || {};
      const hasDeps = (deps.python || []).length || (deps.cli || []).length || (deps.credentials || []).length;
      if (hasDeps) {
        html += '<div style="font-size:12px;color:var(--text3);margin-bottom:4px">Dependencies:</div>';
        html += '<ul style="margin:0 0 8px 20px;font-size:12px;color:var(--text2)">';
        (deps.python || []).forEach(p => { html += '<li>Python: <code style="background:var(--bg3);padding:1px 4px;border-radius:3px;font-size:11px">' + escHtml(p) + '</code></li>'; });
        (deps.cli || []).forEach(t => { html += '<li>CLI: <code style="background:var(--bg3);padding:1px 4px;border-radius:3px;font-size:11px">' + escHtml(t) + '</code></li>'; });
        (deps.credentials || []).forEach(c => { html += '<li>Credential: <code style="background:var(--bg3);padding:1px 4px;border-radius:3px;font-size:11px">' + escHtml(c) + '</code></li>'; });
        html += '</ul>';
      }

      const schema = s.config_schema || {};
      const keys = Object.keys(schema);
      if (keys.length) {
        html += '<div style="font-size:12px;color:var(--text3);margin-bottom:4px">Configuration fields:</div>';
        html += '<ul style="margin:0 0 0 20px;font-size:12px;color:var(--text2)">';
        keys.forEach(k => {
          const def = schema[k];
          html += '<li><strong>' + escHtml(k) + '</strong>';
          if (def.type) html += ' <span style="color:var(--text3)">(' + escHtml(def.type) + ')</span>';
          if (def.description) html += ' - ' + escHtml(def.description);
          if (def.default !== undefined && def.default !== '' && !(Array.isArray(def.default) && !def.default.length)) html += ' <span style="color:var(--text3)">[default: ' + escHtml(String(def.default)) + ']</span>';
          html += '</li>';
        });
        html += '</ul>';
      }
      html += '</div>';
    });
  }

  // Data Storage
  html += '<h2 style="' + headerStyle + ';margin-top:24px">Data Storage</h2>';
  html += '<div style="color:var(--text2);margin-bottom:24px;line-height:1.7">';
  html += '<p style="margin-bottom:8px">All synced data is stored as <strong>JSONL</strong> (JSON Lines) files in the data directory, one file per source.</p>';
  html += '<p style="margin-bottom:8px">Each line in a JSONL file is a self-contained JSON record with a unique ID and timestamp.</p>';
  html += '<p style="margin-bottom:8px"><strong>Consumers</strong> read from these JSONL files using checkpoints to track their position. Each consumer maintains a line offset per source, so multiple consumers can read independently.</p>';
  if (appConfig.data_dir) {
    html += '<p style="margin-bottom:8px">Data directory: <code style="background:var(--bg3);padding:2px 6px;border-radius:4px;font-family:JetBrains Mono,monospace;font-size:12px">' + escHtml(appConfig.data_dir) + '</code></p>';
  }
  html += '</div>';

  html += '</div>';
  el.innerHTML = html;
}

// ---- History Tab (kept as helper) ----
function renderHistory() {
  const el = document.getElementById('tab-history');
  if (!runsData.length) {
    el.innerHTML = '<div class="empty"><p>No sync runs yet</p></div>';
    return;
  }

  let html = '<table class="runs-table">';
  html += '<thead><tr><th>Time</th><th>Source</th><th>Status</th><th>Records</th><th>Duration</th><th>Error</th></tr></thead>';
  html += '<tbody>';
  const sorted = [...runsData].reverse();
  sorted.forEach(r => {
    const statusClass = r.status === 'ok' ? 'badge-green' : r.status === 'error' ? 'badge-red' : 'badge-yellow';
    const dur = r.duration_sec !== undefined ? r.duration_sec.toFixed(1) + 's' : '-';
    html += '<tr>';
    html += '<td class="mono">' + timeAgo(r.ts) + '</td>';
    html += '<td>' + escHtml(r.source) + '</td>';
    html += '<td><span class="badge ' + statusClass + '">' + escHtml(r.status) + '</span></td>';
    html += '<td class="mono">' + (r.count !== undefined ? r.count.toLocaleString() : '-') + '</td>';
    html += '<td class="mono">' + dur + '</td>';
    html += '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--red)">' + escHtml(r.error || '') + '</td>';
    html += '</tr>';
  });
  html += '</tbody></table>';
  el.innerHTML = html;
}

// ---- Queues Tab ----
function renderQueues() {
  const el = document.getElementById('pipeline-queues') || document.getElementById('tab-sources');
  if (!queuesData || !queuesData.consumers.length) {
    el.innerHTML = '<div class="empty"><p>No consumers found</p></div>';
    return;
  }

  const consumers = queuesData.consumers;
  let html = '<table class="queues-table">';
  html += '<thead><tr><th>Source</th><th class="mono">Total</th>';
  consumers.forEach(c => {
    const abbr = c.substring(0, 4);
    html += '<th class="mono" title="' + escHtml(c) + '">' + escHtml(abbr) + '</th>';
  });
  html += '</tr></thead><tbody>';

  queuesData.rows.forEach(row => {
    const allZero = consumers.every(c => (row.pending[c] || 0) === 0);
    if (allZero && row.total === 0) return;
    html += '<tr><td>' + escHtml(row.source) + '</td>';
    html += '<td class="mono">' + row.total.toLocaleString() + '</td>';
    consumers.forEach(c => {
      const p = row.pending[c] || 0;
      let cls = 'q-zero';
      if (p > 0 && p < 50) cls = 'q-green';
      else if (p >= 50 && p < 500) cls = 'q-yellow';
      else if (p >= 500) cls = 'q-red';
      html += '<td class="mono ' + cls + '">' + p.toLocaleString() + '</td>';
    });
    html += '</tr>';
  });

  // TOTAL row
  html += '<tr class="total-row"><td><strong>TOTAL</strong></td><td></td>';
  consumers.forEach(c => {
    const t = queuesData.totals[c] || 0;
    let cls = 'q-zero';
    if (t > 0 && t < 50) cls = 'q-green';
    else if (t >= 50 && t < 500) cls = 'q-yellow';
    else if (t >= 500) cls = 'q-red';
    html += '<td class="mono ' + cls + '">' + t.toLocaleString() + '</td>';
  });
  html += '</tr>';

  // Updated row
  html += '<tr class="updated-row"><td>Updated</td><td></td>';
  consumers.forEach(c => {
    html += '<td>' + timeAgo(queuesData.updated[c]) + '</td>';
  });
  html += '</tr>';

  html += '</tbody></table>';
  el.innerHTML = html;
}

// ---- Consumers Tab ----
function renderConsumers() {
  const el = document.getElementById('pipeline-consumers') || document.getElementById('tab-sources');
  if (!consumersData || !Object.keys(consumersData).length) {
    el.innerHTML = '<div class="empty"><p>No consumers found</p></div>';
    return;
  }

  let html = '<div class="cards-grid">';
  Object.entries(consumersData).sort((a,b) => a[0].localeCompare(b[0])).forEach(([name, data]) => {
    html += '<div class="consumer-card">';
    html += '<div class="consumer-card-name">' + escHtml(name) + '</div>';
    html += '<div class="consumer-card-updated">Updated ' + timeAgo(data.updated_at) + '</div>';
    html += '<div class="consumer-positions">';
    const positions = data.positions || {};
    Object.entries(positions).sort((a,b) => a[0].localeCompare(b[0])).forEach(([src, pos]) => {
      html += '<div class="consumer-pos-row">';
      html += '<span class="consumer-pos-source">' + escHtml(src) + '</span>';
      html += '<span class="consumer-pos-value">' + (pos.line || 0).toLocaleString() + '</span>';
      html += '</div>';
    });
    html += '</div></div>';
  });
  html += '</div>';
  el.innerHTML = html;
}

// (renderDocs removed - merged into renderDocsPage)

function copyBlock(btn) {
  const code = btn.parentElement.querySelector('pre').textContent;
  navigator.clipboard.writeText(code);
  btn.textContent = 'Copied';
  setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
}

// (renderAgent removed - merged into renderDocsPage)

// ---- Data Explorer ----
let dataOverview = null;

function fmtBytes(b) {
  if (b >= 1073741824) return (b / 1073741824).toFixed(1) + ' GB';
  if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
  if (b >= 1024) return (b / 1024).toFixed(1) + ' KB';
  return b + ' B';
}

function fmtDate(ts) {
  if (!ts) return '';
  return ts.replace('T', ' ').substring(0, 16);
}

function extractTitle(rec) {
  var t = rec.type || '';
  var data = rec.data || rec;
  if (t === 'conversation' || t === 'message') return data.chat || data.title || data.period_start || '';
  if (t === 'email') return data.subject || '';
  if (t === 'meeting') return data.title || '';
  if (t === 'document') return data.title || data.name || '';
  if (t === 'issue' || t === 'pull_request') return (data.number ? '#' + data.number + ' ' : '') + (data.title || '');
  if (t === 'task') return data.title || '';
  if (t === 'browsing_session') return (data.domain || '') + ' - ' + (data.title || '');
  if (t === 'activity') return data.title || '';
  if (t === 'notification') return data.subject || data.title || '';
  return data.title || data.subject || data.chat || data.name || data.id || '';
}

function renderData() {
  var el = document.getElementById('tab-data');
  el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading data overview...</div>';
  fetch('/api/data/overview').then(r => r.json()).then(data => {
    dataOverview = data;
    renderDataContent(data);
  }).catch(e => {
    el.innerHTML = '<div style="color:var(--accent);padding:20px">Error loading data: ' + escHtml(e.message) + '</div>';
  });
}

function renderDataContent(data) {
  var el = document.getElementById('tab-data');
  var sources = data.sources || [];
  var maxRecords = Math.max.apply(null, sources.map(s => s.records).concat([1]));

  var firstDate = '', lastDate = '';
  sources.forEach(s => {
    if (s.first_ts && (!firstDate || s.first_ts < firstDate)) firstDate = s.first_ts;
    if (s.last_ts && (!lastDate || s.last_ts > lastDate)) lastDate = s.last_ts;
  });

  var html = '<div style="max-width:1200px">';

  // Search bar
  html += '<div style="display:flex;gap:8px;margin-bottom:20px;align-items:stretch">';
  html += '<input id="data-search" placeholder="Search all data..." style="flex:1;padding:10px 14px;background:var(--bg2);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px;outline:none" onkeydown="if(event.key===&quot;Enter&quot;)searchData()">';
  html += '<select id="data-source-filter" style="padding:10px 12px;background:var(--bg2);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:13px;outline:none">';
  html += '<option value="">All sources</option>';
  sources.forEach(s => {
    html += '<option value="' + escHtml(s.name) + '">' + escHtml(s.name) + '</option>';
  });
  html += '</select>';
  html += '<button onclick="searchData()" style="padding:10px 20px;background:var(--accent);color:white;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600">Search</button>';
  html += '</div>';

  // Overview stats
  html += '<div style="display:flex;gap:32px;margin-bottom:24px;flex-wrap:wrap">';
  html += '<div><span style="font-size:24px;font-weight:700;color:var(--text)">' + fmtNum(data.total_records) + '</span> <span style="color:var(--text3);font-size:14px">records</span></div>';
  html += '<div><span style="font-size:24px;font-weight:700;color:var(--text)">' + fmtBytes(data.total_size) + '</span> <span style="color:var(--text3);font-size:14px">total</span></div>';
  html += '<div><span style="font-size:24px;font-weight:700;color:var(--text)">' + sources.length + '</span> <span style="color:var(--text3);font-size:14px">sources</span></div>';
  if (firstDate && lastDate) {
    html += '<div><span style="font-size:24px;font-weight:700;color:var(--text)">' + fmtDate(firstDate).substring(0,7) + '</span> <span style="color:var(--text3);font-size:14px">-</span> <span style="font-size:24px;font-weight:700;color:var(--text)">' + fmtDate(lastDate).substring(0,7) + '</span></div>';
  }
  html += '</div>';

  // Source cards grid
  sources.sort((a, b) => b.records - a.records);
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px;margin-bottom:24px">';
  sources.forEach(s => {
    var pct = maxRecords > 0 ? Math.round(s.records / maxRecords * 100) : 0;
    var typeKeys = Object.keys(s.types || {});
    html += '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px;cursor:pointer" onclick="browseSource(\\x27' + escHtml(s.name) + '\\x27)">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
    html += '<span style="font-weight:600;font-size:14px;color:var(--text)">' + escHtml(s.name) + '</span>';
    if (typeKeys.length > 0) {
      html += '<span style="background:var(--accent);color:white;padding:2px 8px;border-radius:10px;font-size:11px">' + escHtml(typeKeys[0]) + (typeKeys.length > 1 ? ' +' + (typeKeys.length - 1) : '') + '</span>';
    }
    html += '</div>';
    html += '<div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text2);margin-bottom:4px">';
    html += '<span>' + fmtNum(s.records) + ' records</span>';
    html += '<span>' + fmtBytes(s.size_bytes) + '</span>';
    html += '</div>';
    html += '<div style="height:4px;background:var(--bg3);border-radius:2px;margin:8px 0">';
    html += '<div style="height:100%;width:' + pct + '%;background:var(--accent);border-radius:2px"></div>';
    html += '</div>';
    if (s.first_ts || s.last_ts) {
      html += '<div style="font-size:11px;color:var(--text3)">' + fmtDate(s.first_ts) + ' \\u2192 ' + fmtDate(s.last_ts) + '</div>';
    }
    html += '</div>';
  });
  html += '</div>';

  // Results area
  html += '<div id="data-results"></div>';
  html += '</div>';
  el.innerHTML = html;
}

function browseSource(name, offset) {
  offset = offset || 0;
  var limit = 20;
  var el = document.getElementById('data-results');
  el.innerHTML = '<div style="padding:20px;color:var(--text3)">Loading ' + escHtml(name) + '...</div>';
  fetch('/api/data/browse?source=' + encodeURIComponent(name) + '&offset=' + offset + '&limit=' + limit)
    .then(r => r.json()).then(data => {
      if (data.error) { el.innerHTML = '<div style="color:var(--accent);padding:20px">' + escHtml(data.error) + '</div>'; return; }
      renderBrowseResults(name, data.records, data.total, data.offset, data.limit);
    }).catch(e => {
      el.innerHTML = '<div style="color:var(--accent);padding:20px">Error: ' + escHtml(e.message) + '</div>';
    });
}

function renderBrowseResults(source, records, total, offset, limit) {
  var el = document.getElementById('data-results');
  var html = '';
  html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
  html += '<h3 style="font-size:16px;font-weight:600;color:var(--text);margin:0">' + escHtml(source) + ' <span style="color:var(--text3);font-weight:400;font-size:13px">Showing ' + (offset + 1) + '-' + Math.min(offset + limit, total) + ' of ' + fmtNum(total) + '</span></h3>';
  html += '<div style="display:flex;gap:8px">';
  if (offset > 0) html += '<button onclick="browseSource(\\x27' + escHtml(source) + '\\x27,' + Math.max(0, offset - limit) + ')" style="padding:6px 14px;background:var(--bg2);border:1px solid var(--border);border-radius:6px;color:var(--text);cursor:pointer;font-size:12px">\\u2190 Previous</button>';
  if (offset + limit < total) html += '<button onclick="browseSource(\\x27' + escHtml(source) + '\\x27,' + (offset + limit) + ')" style="padding:6px 14px;background:var(--bg2);border:1px solid var(--border);border-radius:6px;color:var(--text);cursor:pointer;font-size:12px">Next \\u2192</button>';
  html += '</div></div>';

  records.forEach((rec, idx) => {
    var rid = 'data-rec-' + offset + '-' + idx;
    var title = extractTitle(rec);
    var recType = (rec.data || rec).type || rec.type || 'unknown';
    var ts = rec._ingested_at || '';
    html += '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;margin-bottom:6px;overflow:hidden">';
    html += '<div onclick="var d=document.getElementById(\\x27' + rid + '\\x27);d.style.display=d.style.display===\\x27none\\x27?\\x27block\\x27:\\x27none\\x27" style="padding:10px 14px;cursor:pointer;display:flex;align-items:center;gap:10px">';
    html += '<span style="background:var(--accent);color:white;padding:2px 8px;border-radius:10px;font-size:11px;white-space:nowrap">' + escHtml(recType) + '</span>';
    html += '<span style="flex:1;font-size:13px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escHtml(title) + '</span>';
    html += '<span style="font-size:11px;color:var(--text3);white-space:nowrap">' + fmtDate(ts) + '</span>';
    html += '</div>';
    html += '<div id="' + rid + '" style="display:none;padding:0 14px 12px 14px">';
    html += '<pre style="background:var(--bg3);padding:12px;border-radius:6px;font-family:JetBrains Mono,monospace;font-size:11px;overflow-x:auto;white-space:pre-wrap;word-break:break-all;color:var(--text2);margin:0">' + escHtml(JSON.stringify(rec, null, 2)) + '</pre>';
    html += '</div></div>';
  });

  if (records.length === 0) {
    html += '<div style="padding:20px;color:var(--text3);text-align:center">No records found</div>';
  }
  el.innerHTML = html;
}

function searchData() {
  var q = document.getElementById('data-search').value.trim();
  if (!q) return;
  var source = document.getElementById('data-source-filter').value;
  var el = document.getElementById('data-results');
  el.innerHTML = '<div style="padding:20px;color:var(--text3)">Searching...</div>';
  var url = '/api/data/search?q=' + encodeURIComponent(q) + '&limit=20';
  if (source) url += '&source=' + encodeURIComponent(source);
  fetch(url).then(r => r.json()).then(data => {
    if (data.error) { el.innerHTML = '<div style="color:var(--accent);padding:20px">' + escHtml(data.error) + '</div>'; return; }
    renderSearchResults(data.results, data.query);
  }).catch(e => {
    el.innerHTML = '<div style="color:var(--accent);padding:20px">Error: ' + escHtml(e.message) + '</div>';
  });
}

function renderSearchResults(results, query) {
  var el = document.getElementById('data-results');
  var html = '<h3 style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:12px">Results for "' + escHtml(query) + '" <span style="color:var(--text3);font-weight:400;font-size:13px">' + results.length + ' found</span></h3>';
  results.forEach(r => {
    html += '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:12px 14px;margin-bottom:6px">';
    html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">';
    html += '<span style="background:var(--accent);color:white;padding:2px 8px;border-radius:10px;font-size:11px">' + escHtml(r.source) + '</span>';
    html += '<span style="font-size:13px;font-weight:600;color:var(--text)">' + escHtml(r.title) + '</span>';
    if (r.chat) html += '<span style="font-size:11px;color:var(--text3)">' + escHtml(r.chat) + '</span>';
    html += '</div>';
    html += '<div style="font-size:12px;color:var(--text2);line-height:1.5">' + escHtml(r.snippet) + '</div>';
    html += '</div>';
  });
  if (results.length === 0) {
    html += '<div style="padding:20px;color:var(--text3);text-align:center">No results found</div>';
  }
  el.innerHTML = html;
}

// ---- Drawer ----
function openDrawer(name) {
  openSourceName = name;
  const s = sourcesData.find(x => x.name === name);
  if (!s) return;
  document.getElementById('drawer-title').textContent = s.display_name;
  renderDrawerBody(s);
  renderDrawerFooter(s);
  document.getElementById('overlay').classList.add('open');
  document.getElementById('drawer').classList.add('open');
}

function closeDrawer() {
  openSourceName = null;
  document.getElementById('overlay').classList.remove('open');
  document.getElementById('drawer').classList.remove('open');
}

function refreshDrawer() {
  if (!openSourceName) return;
  const s = sourcesData.find(x => x.name === openSourceName);
  if (s) {
    renderDrawerBody(s);
    renderDrawerFooter(s);
  }
}

const INSTALL_MAP = {
  sigtop: 'brew',
  wacli: 'brew',
  gog: 'brew',
  gh: 'brew',
  bird: 'npm',
};

const INSTALL_HINTS = {
  'playwright': 'pip install playwright && playwright install',
};

const CRED_LABELS = {
  'TELEGRAM_API_ID': { label: 'API ID', placeholder: '12345678', inputType: 'text' },
  'TELEGRAM_API_HASH': { label: 'API Hash', placeholder: '0123456789abcdef...' },
  'GITHUB_TOKEN': { label: 'Personal Access Token', placeholder: 'ghp_xxxxxxxxxxxx' },
};

function renderDrawerBody(s) {
  const body = document.getElementById('drawer-body');
  let html = '';

  // 1. Setup Checklist
  const deps = s.dependencies || {};
  const pyDeps = deps.python || [];
  const cliDeps = deps.cli || [];
  const credDeps = deps.credentials || [];
  const osDeps = deps.os || [];
  const envStatus = s.env_status || {};
  const credHelp = s.credential_help || {};
  const ready = s.ready;
  const missing = (ready && ready.missing) || [];

  let totalSteps = pyDeps.length + cliDeps.length + credDeps.length;
  let doneSteps = 0;

  // Prominent "Setup required" banner when source is not ready
  if (ready && ready.ok === false && missing.length > 0) {
    html += '<div style="background:color-mix(in srgb,var(--yellow) 10%,transparent);border:1px solid color-mix(in srgb,var(--yellow) 40%,transparent);border-radius:10px;padding:12px 14px;margin-bottom:14px">';
    html += '<div style="font-size:13px;font-weight:600;color:var(--yellow);margin-bottom:4px">Setup required</div>';
    html += '<div style="font-size:12px;color:var(--text2);line-height:1.5">This source can\u2019t sync yet \u2014 ' + missing.length + ' dependenc' + (missing.length === 1 ? 'y' : 'ies') + ' missing. Use the checklist below to install, set credentials, or follow the manual instructions.</div>';
    html += '</div>';
  } else if (!s.available && s.error) {
    html += '<div style="background:var(--red-bg);border:1px solid var(--red);border-radius:10px;padding:12px 14px;margin-bottom:14px">';
    html += '<div style="font-size:13px;font-weight:600;color:var(--red);margin-bottom:4px">Source failed to load</div>';
    html += '<div style="font-size:12px;color:var(--text2);line-height:1.5">Install the Python dependency below and reload the dashboard.</div>';
    html += '</div>';
  }

  if (totalSteps > 0 || osDeps.length > 0) {
    html += '<div class="drawer-section">';
    html += '<div class="drawer-section-title">Setup Checklist</div>';

    // Python deps
    pyDeps.forEach(pkg => {
      const isMissing = missing.some(m => m.includes(pkg) && m.includes('Python'));
      if (!isMissing) doneSteps++;
      html += '<div class="setup-step">';
      html += '<div class="setup-dot ' + (isMissing ? 'missing' : 'ok') + '"></div>';
      html += '<span class="setup-step-text">Python: <code>' + escHtml(pkg) + '</code></span>';
      if (isMissing) {
        html += '<span class="setup-step-action"><button class="btn-install" onclick="installPkg(\\'' + escHtml(pkg) + '\\',\\'pip\\',this)">Install</button></span>';
      }
      html += '</div>';
    });

    // CLI deps
    cliDeps.forEach(tool => {
      const isMissing = missing.some(m => m.includes(tool) && m.includes('CLI'));
      if (!isMissing) doneSteps++;
      html += '<div class="setup-step">';
      html += '<div class="setup-dot ' + (isMissing ? 'missing' : 'ok') + '"></div>';
      html += '<span class="setup-step-text">CLI: <code>' + escHtml(tool) + '</code></span>';
      if (isMissing) {
        const method = INSTALL_MAP[tool];
        if (method) {
          const pkgName = tool === 'bird' ? '@steipete/bird' : tool;
          html += '<span class="setup-step-action"><button class="btn-install" onclick="installPkg(\\'' + escHtml(pkgName) + '\\',\\'' + method + '\\',this)">Install</button></span>';
        } else {
          const hint = INSTALL_HINTS[tool] || ('Install ' + tool + ' manually');
          html += '<span class="setup-step-action" style="font-size:11px;color:var(--text3)" title="' + escHtml(hint) + '">manual</span>';
        }
      }
      html += '</div>';
    });

    // Credentials (suppress raw inputs when in-dashboard auth handles it)
    const drawerInfo = s.setup_info || {};
    const drawerSuppressCreds = drawerInfo.auth && drawerInfo.auth.method === 'telegram_phone';
    credDeps.forEach(envVar => {
      const isSet = envStatus[envVar];
      if (isSet) doneSteps++;
      if (drawerSuppressCreds) return;
      const help = credHelp[envVar] || {};
      const cl = CRED_LABELS[envVar] || {};
      const friendlyLabel = cl.label || help.description || envVar;
      const placeholder = cl.placeholder || help.description || 'value';
      const inputType = cl.inputType || 'password';
      html += '<div class="setup-step">';
      html += '<div class="setup-dot ' + (isSet ? 'ok' : 'missing') + '"></div>';
      html += '<span class="setup-step-text">' + escHtml(friendlyLabel);
      if (help.help_url) {
        html += ' <a href="' + escHtml(help.help_url) + '" target="_blank" style="font-size:11px;color:var(--accent)">How to get this &rarr;</a>';
      }
      html += '</span>';
      if (!isSet) {
        html += '<div class="setup-step-action">';
        html += '<div class="cred-row">';
        html += '<input type="' + inputType + '" placeholder="' + escHtml(placeholder) + '" id="cred-' + envVar + '" style="width:120px">';
        html += '<button class="btn-install" onclick="saveCred(\\'' + escHtml(envVar) + '\\',this)">Set</button>';
        html += '</div></div>';
      }
      html += '</div>';
    });

    // OS requirements: satisfied -> hide (no clutter). Unsatisfied -> hard block.
    // Full Disk Access is tracked separately via fda_granted, not here.
    const osSatisfied = s.os_satisfied !== false;
    const currentPlatform = s.current_platform || '';
    osDeps.forEach(req => {
      if (req === 'macos:full_disk_access') return;  // FDA gets its own step
      const isMacReq = req === 'macos' || req.startsWith('macos');
      const reqMet = !isMacReq || currentPlatform === 'darwin';
      if (reqMet) return;  // hide satisfied OS requirements
      let label = 'Requires macOS \u2014 this source only works on a Mac';
      if (currentPlatform) label += ' (you\u2019re on ' + escHtml(currentPlatform) + ')';
      html += '<div class="setup-step">';
      html += '<div class="setup-dot missing"></div>';
      html += '<span class="setup-step-text" style="color:var(--red)">' + label + '</span>';
      html += '</div>';
    });

    // App install (download + recheck)
    const info = s.setup_info || {};
    if (info.app_state) {
      const a = info.app_state;
      totalSteps++;
      if (a.installed) { doneSteps++; }
      html += '<div class="setup-step">';
      html += '<div class="setup-dot ' + (a.installed ? 'ok' : 'missing') + '"></div>';
      html += '<span class="setup-step-text">App: ' + escHtml(a.display) + '</span>';
      if (!a.installed) {
        html += '<span class="setup-step-action">';
        html += '<a class="btn-install" target="_blank" href="' + escHtml(a.download_url) + '">Download</a> ';
        html += '<button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">I installed it</button>';
        html += '</span>';
      }
      html += '</div>';
    }

    // Full Disk Access
    if (info.os_help && info.os_help.kind === 'full_disk_access') {
      const fdaOk = !!info.fda_granted;
      totalSteps++;
      if (fdaOk) doneSteps++;
      html += '<div class="setup-step">';
      html += '<div class="setup-dot ' + (fdaOk ? 'ok' : 'missing') + '"></div>';
      html += '<span class="setup-step-text">' + escHtml(info.os_help.label || 'Full Disk Access') + '</span>';
      if (!fdaOk) {
        html += '<span class="setup-step-action">';
        html += '<a class="btn-install" href="' + escHtml(info.os_help.deeplink) + '">Open Settings</a> ';
        html += '<button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">Re-check</button>';
        html += '</span>';
      }
      html += '</div>';
    }

    // Auth (gh, gog, wacli, bird, linkedin, telegram)
    const authMeta = (info.auth || {});
    const authMethod = authMeta.method;
    if (authMethod) {
      const authState = info.auth_state || {};
      totalSteps++;

      if (authMethod === 'telegram_phone') {
        const signedIn = info.telegram_signed_in;
        if (signedIn) doneSteps++;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (signedIn ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">Telegram: ' + (signedIn ? 'signed in' : 'not signed in') + '</span>';
        if (!signedIn) {
          html += '<span class="setup-step-action"><button class="btn-install" onclick="wizTelegramStart(\\'' + s.name + '\\')">Sign in</button></span>';
        }
        html += '</div>';
      } else if (authMethod === 'bird') {
        const ok = authState.signed_in;
        if (ok) doneSteps++;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (ok ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">X / Twitter: ' + (ok ? escHtml(authState.detail || 'signed in') : 'not signed in') + '</span>';
        if (!ok) {
          html += '<span class="setup-step-action">Log into <a href="https://x.com" target="_blank" style="color:var(--accent)">x.com</a> in your browser, then <button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">Re-check</button></span>';
        }
        html += '</div>';
      } else if (authMethod === 'linkedin_browser') {
        const ok = authState.signed_in;
        if (ok) doneSteps++;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (ok ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">LinkedIn: ' + (ok ? 'session active' : 'not signed in') + '</span>';
        if (!ok) {
          html += '<span class="setup-step-action"><button class="btn-install" onclick="wizLinkedInLaunch(\\'' + s.name + '\\')">Open login</button> <button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">Re-check</button></span>';
        }
        html += '</div>';
      } else if (authMethod === 'gog') {
        const connectedAccounts = authState.accounts || (authState.signed_in && authState.account ? [authState.account] : []);
        if (connectedAccounts.length > 0) doneSteps++;
        connectedAccounts.forEach(acc => {
          html += '<div class="setup-step"><div class="setup-dot ok"></div>';
          html += '<span class="setup-step-text">Google: ' + escHtml(acc) + '</span></div>';
        });
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (connectedAccounts.length > 0 ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">' + (connectedAccounts.length > 0 ? 'Add another account' : 'Connect Google account') + '</span>';
        html += '<span class="setup-step-action">';
        html += '<input id="drawer-gog-acc-' + s.name + '" type="text" placeholder="name@gmail.com" style="width:120px;padding:3px 6px;font-size:11px;background:var(--bg);border:1px solid var(--border);border-radius:4px;color:var(--text)"> ';
        html += '<button class="btn-install" onclick="drawerGogAuth(\\'' + s.name + '\\')">Connect</button>';
        html += '</span></div>';
      } else {
        // gh, wacli_pair, etc.
        const ok = authState.signed_in;
        if (ok) doneSteps++;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (ok ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">' + escHtml(authMeta.label || 'Auth') + ': ' + (ok ? escHtml(authState.detail || 'signed in') : 'not signed in') + '</span>';
        if (!ok) {
          html += '<span class="setup-step-action"><button class="btn-install" onclick="drawerAuthStart(\\'' + s.name + '\\',\\'' + authMethod + '\\')">Sign in</button></span>';
        } else {
          html += '<span class="setup-step-action"><button class="btn-install" onclick="drawerAuthStart(\\'' + s.name + '\\',\\'' + authMethod + '\\')">Re-auth</button></span>';
        }
        html += '</div>';
      }
    }

    // Post-install hint
    if (info.post_install_hint) {
      html += '<div style="font-size:11px;color:var(--text3);margin:6px 0 2px 22px;line-height:1.4">' + escHtml(info.post_install_hint) + '</div>';
    }

    // Progress (only show when there are actionable steps)
    if (totalSteps > 0) {
      const pct = Math.round((doneSteps / totalSteps) * 100);
      html += '<div class="progress-bar"><div class="progress-bar-fill" style="width:' + pct + '%"></div></div>';
      html += '<div class="progress-text">' + doneSteps + ' of ' + totalSteps + ' steps complete</div>';
    }
    html += '</div>';
  }

  // Setup section for sources with auth/app but no deps
  if (!(totalSteps > 0 || osDeps.length > 0)) {
    const info = s.setup_info || {};
    const hasSetup = info.app_state || info.auth || (info.os_help && info.os_help.kind === 'full_disk_access');
    if (hasSetup) {
      html += '<div class="drawer-section">';
      html += '<div class="drawer-section-title">Setup</div>';

      if (info.app_state) {
        const a = info.app_state;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (a.installed ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">App: ' + escHtml(a.display) + '</span>';
        if (!a.installed) {
          html += '<span class="setup-step-action"><a class="btn-install" target="_blank" href="' + escHtml(a.download_url) + '">Download</a> <button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">I installed it</button></span>';
        }
        html += '</div>';
      }

      const authMeta2 = info.auth || {};
      const authMethod2 = authMeta2.method;
      if (authMethod2) {
        const authState2 = info.auth_state || {};
        const ok2 = authState2.signed_in || info.telegram_signed_in;
        html += '<div class="setup-step">';
        html += '<div class="setup-dot ' + (ok2 ? 'ok' : 'missing') + '"></div>';
        html += '<span class="setup-step-text">' + escHtml(authMeta2.label || 'Auth') + ': ' + (ok2 ? 'connected' : 'not connected') + '</span>';
        if (!ok2) {
          if (authMethod2 === 'bird') {
            html += '<span class="setup-step-action">Log into <a href="https://x.com" target="_blank" style="color:var(--accent)">x.com</a> in your browser, then <button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">Re-check</button></span>';
          } else if (authMethod2 === 'linkedin_browser') {
            html += '<span class="setup-step-action"><button class="btn-install" onclick="wizLinkedInLaunch(\\'' + s.name + '\\')">Open login</button> <button class="btn-install" onclick="recheckSource(\\'' + s.name + '\\')">Re-check</button></span>';
          } else if (authMethod2 === 'telegram_phone') {
            html += '<span class="setup-step-action"><button class="btn-install" onclick="wizTelegramStart(\\'' + s.name + '\\')">Sign in</button></span>';
          } else {
            html += '<span class="setup-step-action"><button class="btn-install" onclick="drawerAuthStart(\\'' + s.name + '\\',\\'' + authMethod2 + '\\')">Sign in</button></span>';
          }
        }
        html += '</div>';
      }

      if (info.post_install_hint) {
        html += '<div style="font-size:11px;color:var(--text3);margin:6px 0 2px 22px;line-height:1.4">' + escHtml(info.post_install_hint) + '</div>';
      }
      html += '</div>';
    }
  }

  // 2. Enable Toggle
  html += '<div class="drawer-section">';
  html += '<div class="toggle-row">';
  html += '<span class="toggle-label">Enabled</span>';
  html += '<label class="toggle">';
  html += '<input type="checkbox" id="toggle-enabled" ' + (s.enabled ? 'checked' : '') + ' onchange="toggleEnabled(\\'' + s.name + '\\', this.checked)">';
  html += '<span class="toggle-track"></span>';
  html += '</label></div></div>';

  // 3. Configuration
  const schema = s.config_schema || {};
  const currentConfig = s.current_config || {};
  const defaults = s.defaults || {};
  const allKeys = new Set([...Object.keys(schema), ...Object.keys(defaults)]);

  if (allKeys.size > 0) {
    html += '<div class="drawer-section">';
    html += '<div class="drawer-section-title">Configuration</div>';

    // Common fields not always in per-source schema: mode is always cron|daemon
    const COMMON_CHOICES = {
      mode: ['cron', 'daemon'],
    };

    const basicKeys = [];
    const advancedKeys = [];
    allKeys.forEach(key => {
      if (key === 'enabled') return;
      const def = schema[key] || {};
      if (def.advanced) advancedKeys.push(key);
      else basicKeys.push(key);
    });

    const renderField = (key) => {
      const schemaDef = schema[key] || {};
      let type = schemaDef.type || 'str';
      const val = currentConfig[key] !== undefined ? currentConfig[key] : defaults[key];
      const desc = schemaDef.description || '';
      const placeholder = schemaDef.placeholder || '';
      let choices = schemaDef.choices || COMMON_CHOICES[key] || null;
      if (choices) type = 'enum';

      let row = '<div class="field">';
      row += '<label title="' + escHtml(desc) + '">' + escHtml(key.replace(/_/g, ' '));
      if (schemaDef.auto_detected) row += ' <span style="font-size:10px;color:var(--accent);background:rgba(52,211,153,0.12);padding:1px 6px;border-radius:8px;margin-left:4px;text-transform:uppercase;letter-spacing:0.04em">auto-detected</span>';
      row += '</label>';
      if (desc && type !== 'bool') row += '<div style="font-size:11px;color:var(--text3);margin-bottom:4px">' + escHtml(desc) + '</div>';

      if (type === 'bool') {
        const checked = val === true || val === 'true' || val === 'True';
        row += '<div class="field-checkbox">';
        row += '<input type="checkbox" data-config-key="' + escHtml(key) + '" ' + (checked ? 'checked' : '') + '>';
        if (desc) row += '<span style="font-size:11px;color:var(--text3)">' + escHtml(desc) + '</span>';
        row += '</div>';
      } else if (type === 'enum' && choices && choices.length === 2) {
        // 2 choices -> segmented switch
        const curr = val != null ? String(val) : choices[0];
        row += '<div class="seg-switch" role="radiogroup" aria-label="' + escHtml(key.replace(/_/g, ' ')) + '" data-config-key="' + escHtml(key) + '">';
        choices.forEach(c => {
          const active = c === curr ? ' active' : '';
          const ariaChecked = c === curr ? 'true' : 'false';
          row += '<button type="button" role="radio" aria-checked="' + ariaChecked + '" class="seg-btn' + active + '" data-val="' + escHtml(c) + '" onclick="segSelect(this)">' + escHtml(c) + '</button>';
        });
        row += '</div>';
      } else if (type === 'enum' && choices) {
        const curr = val != null ? String(val) : '';
        row += '<select data-config-key="' + escHtml(key) + '" aria-label="' + escHtml(key.replace(/_/g, ' ')) + '">';
        choices.forEach(c => {
          const sel = c === curr ? ' selected' : '';
          row += '<option value="' + escHtml(c) + '"' + sel + '>' + escHtml(c) + '</option>';
        });
        row += '</select>';
      } else if (type === 'int') {
        row += '<input type="number" data-config-key="' + escHtml(key) + '" value="' + (val !== undefined && val !== null ? val : '') + '"' + (placeholder ? ' placeholder="' + escHtml(placeholder) + '"' : '') + '>';
      } else if (type === 'path') {
        const strVal = val !== undefined && val !== null ? String(val) : '';
        row += '<div class="path-row">';
        row += '<input type="text" data-config-key="' + escHtml(key) + '" value="' + escHtml(strVal) + '"' + (placeholder ? ' placeholder="' + escHtml(placeholder) + '"' : '') + ' spellcheck="false" autocapitalize="off" autocorrect="off">';
        row += '<button type="button" class="btn btn-sm" aria-label="Browse for folder" onclick="openPathPicker(this, \\'' + escHtml(key) + '\\')">Browse\u2026</button>';
        row += '</div>';
      } else if (type === 'list' && schemaDef.item_type === 'object') {
        // List of structured objects -> repeater UI. Stored as JSON in a hidden textarea.
        const items = Array.isArray(val) ? val : [];
        const fields = schemaDef.item_fields || [];
        row += '<div class="repeater" data-config-key="' + escHtml(key) + '" data-type="json">';
        items.forEach((item, idx) => {
          row += '<div class="repeater-item">';
          fields.forEach(f => {
            const fv = item[f.key] != null ? String(item[f.key]) : '';
            row += '<input type="text" data-repeater-key="' + escHtml(f.key) + '" value="' + escHtml(fv) + '" placeholder="' + escHtml(f.placeholder || f.key) + '" aria-label="' + escHtml(f.key) + '">';
          });
          row += '<button type="button" class="btn btn-sm" aria-label="Remove item" title="Remove" onclick="repeaterRemove(this)">\u00D7</button>';
          row += '</div>';
        });
        row += '<button type="button" class="btn btn-sm" onclick="repeaterAdd(this, ' + escHtml(JSON.stringify(fields)) + ')">+ Add</button>';
        row += '</div>';
      } else if (type === 'list' || Array.isArray(val)) {
        // Primitive list (strings/numbers) -> newline-separated textarea
        const safe = Array.isArray(val) ? val.filter(x => typeof x !== 'object').map(String) : [];
        const arrVal = safe.length ? safe.join('\\n') : (typeof val === 'string' ? val : '');
        row += '<textarea data-config-key="' + escHtml(key) + '" data-type="list"' + (placeholder ? ' placeholder="' + escHtml(placeholder) + '"' : '') + '>' + escHtml(arrVal) + '</textarea>';
      } else if (schemaDef.sensitive) {
        // Sensitive -> masked input with Show/Hide toggle
        const strVal = val !== undefined && val !== null ? String(val) : '';
        row += '<div class="path-row">';
        row += '<input type="password" data-config-key="' + escHtml(key) + '" value="' + escHtml(strVal) + '"' + (placeholder ? ' placeholder="' + escHtml(placeholder) + '"' : '') + ' autocomplete="off" spellcheck="false">';
        row += '<button type="button" class="btn btn-sm" aria-label="Show value" onclick="toggleSecret(this)">Show</button>';
        row += '</div>';
      } else {
        const strVal = val !== undefined && val !== null ? String(val) : '';
        row += '<input type="text" data-config-key="' + escHtml(key) + '" value="' + escHtml(strVal) + '"' + (placeholder ? ' placeholder="' + escHtml(placeholder) + '"' : '') + '>';
      }
      row += '</div>';
      return row;
    };

    basicKeys.forEach(key => { html += renderField(key); });

    if (advancedKeys.length > 0) {
      html += '<details class="advanced-settings"><summary style="cursor:pointer;font-size:12px;color:var(--text3);margin:8px 0">Advanced settings</summary>';
      advancedKeys.forEach(key => { html += renderField(key); });
      html += '</details>';
    }
    html += '</div>';
  }

  // 4. Data Stats
  html += '<div class="drawer-section">';
  html += '<div class="drawer-section-title">Data Stats</div>';
  html += '<div class="stat-boxes">';
  html += '<div class="stat-box"><div class="stat-box-value">' + fmtNum(s.records) + '</div><div class="stat-box-label">Records</div></div>';
  html += '<div class="stat-box"><div class="stat-box-value">' + timeAgo(s.last_ts) + '</div><div class="stat-box-label">Last sync</div></div>';
  html += '</div></div>';

  // Error
  if (s.error) {
    html += '<div class="drawer-section">';
    html += '<div class="drawer-section-title">Load Error</div>';
    html += '<div style="background:var(--red-bg);border:1px solid var(--red);border-radius:8px;padding:12px;font-size:12px;font-family:JetBrains Mono,monospace;color:var(--red);word-break:break-all">' + escHtml(s.error) + '</div>';
    html += '</div>';
  }

  body.innerHTML = html;
}

function renderDrawerFooter(s) {
  const footer = document.getElementById('drawer-footer');
  let html = '<button class="btn btn-primary" onclick="saveConfig(\\'' + s.name + '\\')">Save Changes</button>';
  if (s.enabled && s.available && s.ready && s.ready.ok) {
    html += '<button class="btn" onclick="syncNow(\\'' + s.name + '\\', this)">Sync Now</button>';
  }
  footer.innerHTML = html;
}

// ---- Drawer Actions ----
async function toggleEnabled(name, enabled) {
  try {
    const res = await fetch('/api/sources/' + name, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({enabled: enabled})
    });
    const data = await res.json();
    showToast(name + ' ' + (enabled ? 'enabled' : 'disabled'), 'success');
    if (data.daemon_started) {
      showToast('Sync daemon started automatically', 'success');
    }
    await refresh();
    refreshDrawer();
  } catch(e) { showToast(e.message, 'error'); }
}

async function saveConfig(name) {
  const fields = document.querySelectorAll('[data-config-key]');
  const config = {};
  fields.forEach(f => {
    const key = f.getAttribute('data-config-key');
    if (f.type === 'checkbox') {
      config[key] = f.checked;
    } else if (f.classList && f.classList.contains('seg-switch')) {
      const active = f.querySelector('.seg-btn.active');
      config[key] = active ? active.getAttribute('data-val') : null;
    } else if (f.classList && f.classList.contains('repeater')) {
      const items = [];
      f.querySelectorAll('.repeater-item').forEach(row => {
        const item = {};
        row.querySelectorAll('[data-repeater-key]').forEach(inp => {
          item[inp.getAttribute('data-repeater-key')] = inp.value;
        });
        if (Object.values(item).some(v => v && String(v).trim())) items.push(item);
      });
      config[key] = items;
    } else if (f.tagName === 'SELECT') {
      config[key] = f.value;
    } else if (f.getAttribute('data-type') === 'list') {
      config[key] = f.value.split('\\n').map(x => x.trim()).filter(Boolean);
    } else if (f.type === 'number' && f.value !== '') {
      config[key] = parseInt(f.value, 10);
    } else {
      config[key] = f.value;
    }
  });

  try {
    const res = await fetch('/api/sources/' + name, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({config: config})
    });
    const data = await res.json();
    if (data.ok) {
      showToast('Configuration saved', 'success');
      await refresh();
      refreshDrawer();
    } else if (data.errors && Array.isArray(data.errors)) {
      data.errors.forEach(err => showToast(err, 'error'));
    } else {
      showToast('Save failed: ' + (data.error || ''), 'error');
    }
  } catch(e) { showToast(e.message, 'error'); }
}

async function syncNow(name, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Syncing...'; }
  try {
    const res = await fetch('/api/sources/' + encodeURIComponent(name) + '/sync', {
      method: 'POST',
    });
    const data = await res.json();
    if (data.ok) {
      showToast('Synced ' + (data.count || 0) + ' records', 'success');
    } else {
      showToast('Sync failed: ' + (data.error || ''), 'error');
    }
  } catch(e) { showToast(e.message, 'error'); }
  if (btn) { btn.disabled = false; btn.textContent = 'Sync Now'; }
  await refresh();
  refreshDrawer();
}

async function saveCred(envVar, btn) {
  const input = document.getElementById('cred-' + envVar);
  if (!input || !input.value.trim()) { showToast('Value required', 'error'); return; }
  btn.disabled = true; btn.textContent = '...';
  try {
    const body = {};
    body[envVar] = input.value.trim();
    const res = await fetch('/api/credentials', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (data.ok) {
      showToast(envVar + ' saved', 'success');
      await refresh();
      refreshDrawer();
    } else {
      showToast('Failed to save', 'error');
      btn.disabled = false; btn.textContent = 'Set';
    }
  } catch(e) { showToast(e.message, 'error'); btn.disabled = false; btn.textContent = 'Set'; }
}

async function installPkg(pkg, method, btn) {
  btn.disabled = true; btn.textContent = 'Installing...';
  try {
    const res = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:pkg, method:method})});
    const data = await res.json();
    if (data.ok) { showToast(pkg+' installed', 'success'); btn.textContent='Done'; await refresh(); refreshDrawer(); }
    else if (data.error === 'needs_brew') {
      btn.textContent = 'Install Homebrew first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing Homebrew...';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'homebrew', method:'brew_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('Homebrew installed', 'success'); installPkg(pkg, method, btn); }
        else { showToast('Homebrew install failed: '+(d2.error||''), 'error'); btn.textContent='Retry Homebrew'; btn.disabled=false; }
      };
    }
    else if (data.error === 'needs_npm') {
      btn.textContent = 'Install Node.js first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing Node.js...';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'node', method:'npm_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('Node.js installed', 'success'); installPkg(pkg, method, btn); }
        else { showToast('Node.js install failed: '+(d2.error||''), 'error'); btn.textContent='Retry Node.js'; btn.disabled=false; }
      };
    }
    else if (data.error === 'needs_pipx') {
      btn.textContent = 'Install pipx first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing pipx...';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'pipx', method:'pipx_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('pipx installed', 'success'); installPkg(pkg, method, btn); }
        else { showToast('pipx install failed: '+(d2.error||''), 'error'); btn.textContent='Retry pipx'; btn.disabled=false; }
      };
    }
    else { showToast('Failed: '+(data.error||'').substring(0,120), 'error'); btn.textContent='Retry'; btn.disabled=false; }
  } catch(e) { showToast(e.message,'error'); btn.textContent='Retry'; btn.disabled=false; }
}

async function recheckSource(name) {
  await refresh();
  refreshDrawer();
  showToast('Rechecked ' + name, 'info');
}

async function drawerAuthStart(sourceName, method) {
  let account = '';
  const accInput = document.getElementById('drawer-gog-acc-' + sourceName);
  if (accInput) {
    account = (accInput.value || '').trim();
    if (!account && method === 'gog') {
      showToast('Enter your Google account email first', 'error');
      return;
    }
  }
  try {
    const res = await fetch('/api/auth/cli/start', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({method: method, source: sourceName, account: account})
    });
    const data = await res.json();
    if (!data.ok) {
      if (data.needs_install) showToast('Install the ' + data.needs_install + ' CLI first', 'error');
      else showToast('Auth failed: ' + (data.error || ''), 'error');
      return;
    }
    _openAuthModal(sourceName, method, data.session);
  } catch(e) {
    showToast('Auth error: ' + e.message, 'error');
  }
}

async function drawerGogAuth(sourceName) {
  const inp = document.getElementById('drawer-gog-acc-' + sourceName);
  const account = (inp && inp.value || '').trim();
  if (!account) {
    showToast('Enter a Google account email first', 'error');
    return;
  }
  drawerAuthStart(sourceName, 'gog');
}

// ---- SSE Live Updates ----
function connectSSE() {
  const es = new EventSource('/api/events');
  es.onmessage = function(e) {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'stats') {
        const stats = msg.data;
        let total = 0;
        Object.values(stats).forEach(s => { total += s.records || 0; });
        document.getElementById('stat-records').textContent = fmtNum(total);
      }
    } catch(err) {}
  };
  es.onerror = function() {
    es.close();
    setTimeout(connectSSE, 10000);
  };
}

// ---- Global Settings ----

async function loadGlobalSettings() {
  const panel = document.getElementById('global-settings-panel');
  if (!panel) return;
  try {
    const cfg = await apiFetch('/api/config/global');
    let html = '<div style="padding:8px 0">';

    html += '<div class="field"><label>Your Names</label>';
    html += '<textarea id="global-self-names" rows="2" placeholder="Jane Doe\\nJane" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:13px;resize:vertical">' + escHtml((cfg.self_names || []).join('\\n')) + '</textarea>';
    html += '<div style="font-size:11px;color:var(--text-muted);margin-top:2px">Used to identify your own messages across all sources (one name per line, substring match)</div>';
    html += '</div>';

    html += '<div style="font-weight:500;margin:12px 0 8px;font-size:13px">Conversation Grouping</div>';

    html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px">';
    html += '<div class="field"><label>Time Window (hours)</label>';
    html += '<input type="number" id="global-time-window" value="' + (cfg.time_window_hours || 4) + '" min="1" max="48" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:13px">';
    html += '<div style="font-size:10px;color:var(--text-muted);margin-top:2px">Messages within this gap are grouped together</div></div>';
    html += '<div class="field"><label>Min Messages per Chunk</label>';
    html += '<input type="number" id="global-min-chunk" value="' + (cfg.min_messages_per_chunk || 3) + '" min="1" max="50" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:13px"></div>';
    html += '<div class="field"><label>Max Messages per Chunk</label>';
    html += '<input type="number" id="global-max-chunk" value="' + (cfg.max_messages_per_chunk || 100) + '" min="10" max="1000" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:13px"></div>';
    html += '</div>';

    html += '<div style="margin-top:12px"><button class="btn" onclick="saveGlobalSettings()">Save Settings</button></div>';
    html += '</div>';
    panel.innerHTML = html;
  } catch(e) {
    panel.innerHTML = '<div class="empty"><p>Error: ' + escHtml(e.message) + '</p></div>';
  }
}

async function saveGlobalSettings() {
  const namesRaw = document.getElementById('global-self-names')?.value || '';
  const data = {
    self_names: namesRaw.split('\\n').map(s => s.trim()).filter(Boolean),
    time_window_hours: parseInt(document.getElementById('global-time-window')?.value || '4'),
    min_messages_per_chunk: parseInt(document.getElementById('global-min-chunk')?.value || '3'),
    max_messages_per_chunk: parseInt(document.getElementById('global-max-chunk')?.value || '100'),
  };
  try {
    const res = await fetch('/api/config/global', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const d = await res.json();
    if (d.ok) showToast('Global settings saved', 'success');
    else showToast('Failed: ' + (d.error || ''), 'error');
  } catch(e) { showToast(e.message, 'error'); }
}

// ---- Search Settings ----
async function loadSearchSettings() {
  const panel = document.getElementById('search-settings-panel');
  if (!panel) return;
  try {
    const cfg = await apiFetch('/api/search/config');
    // Check which API keys are set in env
    try {
      const envRes = await apiFetch('/api/env/status?keys=GEMINI_API_KEY,OPENAI_API_KEY');
      window._searchKeyStatus = envRes;
    } catch(e) { window._searchKeyStatus = {}; }
    const health = searchHealth || {};
    let html = '<div style="padding:8px 0">';

    // Health summary
    if (health.available) {
      html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:16px">';
      html += '<div class="stat-box"><div class="stat-box-value">' + fmtNum(health.total_documents) + '</div><div class="stat-box-label">Indexed Documents</div></div>';
      html += '<div class="stat-box"><div class="stat-box-value">' + health.size_mb + ' MB</div><div class="stat-box-label">Index Size</div></div>';
      html += '<div class="stat-box"><div class="stat-box-value">' + Object.keys(health.by_source || {}).length + '</div><div class="stat-box-label">Sources Indexed</div></div>';
      html += '</div>';

      // Per-source breakdown
      html += '<div class="drawer-section"><div class="drawer-section-title">Documents by Source</div>';
      const sorted = Object.entries(health.by_source || {}).sort((a,b) => b[1] - a[1]);
      sorted.forEach(([src, count]) => {
        const pct = Math.round(count / health.total_documents * 100);
        html += '<div style="display:flex;align-items:center;gap:8px;padding:4px 0;font-size:13px">';
        html += '<span style="min-width:120px;font-weight:500">' + escHtml(src) + '</span>';
        html += '<div style="flex:1;height:6px;background:var(--bg3);border-radius:3px;overflow:hidden">';
        html += '<div style="height:100%;width:' + Math.max(pct, 1) + '%;background:var(--accent);border-radius:3px"></div>';
        html += '</div>';
        html += '<span class="mono" style="min-width:60px;text-align:right;font-size:12px">' + fmtNum(count) + '</span>';
        html += '</div>';
      });
      html += '</div>';
    } else {
      html += '<div class="setup-banner" style="margin-bottom:16px"><div class="setup-banner-title">Search index not built</div>';
      html += '<div style="font-size:13px;color:var(--text2);margin-top:4px">' + escHtml(health.reason || 'Run "vadimgest index --rebuild" or click Rebuild below') + '</div></div>';
    }

    // Settings fields
    html += '<div class="drawer-section"><div class="drawer-section-title">Settings</div>';
    html += '<div class="field"><label>Obsidian Vault Path</label>';
    html += '<input type="text" id="search-vault-path" value="' + escHtml(cfg.vault_path || '') + '"></div>';
    html += '<div class="field"><label>Skills Directory</label>';
    html += '<input type="text" id="search-skills-dir" value="' + escHtml(cfg.skills_dir || '') + '"></div>';
    html += '<div class="field"><label>Index Database Path</label>';
    html += '<input type="text" id="search-index-db" value="' + escHtml(cfg.index_db || '') + '"></div>';
    html += '<div class="field"><label>Embedding Provider</label>';
    html += '<select id="search-embed-provider" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:13px">';
    const providers = [["", "None (FTS only)"], ["gemini", "Gemini (free tier)"], ["openai", "OpenAI"], ["ollama", "Ollama (local)"]];
    providers.forEach(([val, label]) => {
      html += '<option value="' + val + '"' + (cfg.embedding_provider === val ? ' selected' : '') + '>' + label + '</option>';
    });
    html += '</select></div>';

    // API key field (conditional on provider)
    const providerKeys = {gemini: 'GEMINI_API_KEY', openai: 'OPENAI_API_KEY'};
    const curProvider = cfg.embedding_provider || '';
    const keyName = providerKeys[curProvider] || '';
    html += '<div id="search-key-wrap" style="display:' + (keyName ? 'block' : 'none') + '">';
    html += '<div class="field"><label id="search-key-label">' + escHtml(keyName || 'API Key') + '</label>';
    html += '<div style="display:flex;gap:8px">';
    html += '<input type="password" id="search-api-key" placeholder="' + (keyName ? 'Set via .env or paste here' : '') + '" style="flex:1">';
    html += '<button class="btn" style="white-space:nowrap" onclick="toggleKeyVis(this)">Show</button>';
    html += '</div>';
    const keySet = keyName ? !!window._searchKeyStatus?.[keyName] : false;
    html += '<div id="search-key-status" style="font-size:11px;margin-top:4px;color:' + (keySet ? 'var(--ok)' : 'var(--text-muted)') + '">' + (keySet ? '✓ Set in environment' : keyName ? 'Not set - paste key to save' : '') + '</div>';
    html += '</div></div>';

    // Ollama fields (conditional on provider)
    const isOllama = curProvider === 'ollama';
    html += '<div id="search-ollama-wrap" style="display:' + (isOllama ? 'block' : 'none') + '">';
    html += '<div class="field"><label>Ollama Server URL</label>';
    html += '<input type="text" id="search-ollama-url" value="' + escHtml(cfg.ollama_url || 'http://localhost:11434') + '" placeholder="http://localhost:11434"></div>';
    html += '<div class="field"><label>Ollama Model</label>';
    html += '<input type="text" id="search-ollama-model" value="' + escHtml(cfg.ollama_model || 'nomic-embed-text') + '" placeholder="nomic-embed-text"></div>';
    html += '</div>';
    html += '</div>';

    // Wire provider dropdown to show/hide key field + ollama fields
    document.getElementById('search-embed-provider')?.addEventListener('change', function() {
      const wrap = document.getElementById('search-key-wrap');
      const ollamaWrap = document.getElementById('search-ollama-wrap');
      const label = document.getElementById('search-key-label');
      const input = document.getElementById('search-api-key');
      const status = document.getElementById('search-key-status');
      const pk = {gemini: 'GEMINI_API_KEY', openai: 'OPENAI_API_KEY'};
      const k = pk[this.value] || '';
      if (k) {
        wrap.style.display = 'block';
        label.textContent = k;
        input.placeholder = 'Set via .env or paste here';
        const isSet = !!window._searchKeyStatus?.[k];
        status.style.color = isSet ? 'var(--ok)' : 'var(--text-muted)';
        status.textContent = isSet ? '✓ Set in environment' : 'Not set - paste key to save';
      } else {
        wrap.style.display = 'none';
      }
      ollamaWrap.style.display = this.value === 'ollama' ? 'block' : 'none';
      input.value = '';
    });

    // Action buttons
    html += '<div style="display:flex;gap:8px;margin-top:12px">';
    html += '<button class="btn" onclick="saveSearchSettings()">Save Settings</button>';
    html += '<button class="btn" onclick="rebuildIndex(false, this)">Update Index</button>';
    html += '<button class="btn" style="color:var(--red)" onclick="if(confirm(\\'This will delete and rebuild the entire index. Continue?\\'))rebuildIndex(true,this)">Full Rebuild</button>';
    html += '</div>';

    html += '</div>';
    panel.innerHTML = html;
  } catch(e) {
    panel.innerHTML = '<div class="empty"><p>Error loading search settings: ' + escHtml(e.message) + '</p></div>';
  }
}

function toggleKeyVis(btn) {
  const input = document.getElementById('search-api-key');
  if (input.type === 'password') { input.type = 'text'; btn.textContent = 'Hide'; }
  else { input.type = 'password'; btn.textContent = 'Show'; }
}

async function saveSearchSettings() {
  const provider = document.getElementById('search-embed-provider').value;
  const data = {
    vault_path: document.getElementById('search-vault-path').value,
    skills_dir: document.getElementById('search-skills-dir').value,
    index_db: document.getElementById('search-index-db').value,
    embedding_provider: provider,
  };
  if (provider === 'ollama') {
    data.ollama_url = document.getElementById('search-ollama-url')?.value || 'http://localhost:11434';
    data.ollama_model = document.getElementById('search-ollama-model')?.value || 'nomic-embed-text';
  }
  try {
    // Save config
    const res = await fetch('/api/search/config', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const d = await res.json();

    // Save API key if provided
    const apiKey = document.getElementById('search-api-key')?.value;
    if (apiKey) {
      const keyMap = {gemini: 'GEMINI_API_KEY', openai: 'OPENAI_API_KEY'};
      const envKey = keyMap[provider];
      if (envKey) {
        await fetch('/api/credentials', {
          method: 'PUT',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({[envKey]: apiKey})
        });
        // Update status display
        window._searchKeyStatus = window._searchKeyStatus || {};
        window._searchKeyStatus[envKey] = true;
        const status = document.getElementById('search-key-status');
        if (status) { status.style.color = 'var(--ok)'; status.textContent = '✓ Key saved'; }
        document.getElementById('search-api-key').value = '';
      }
    }

    if (d.ok) showToast('Search settings saved', 'success');
    else showToast('Failed: ' + (d.error || ''), 'error');
  } catch(e) { showToast(e.message, 'error'); }
}

async function rebuildIndex(full, btn) {
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Indexing...';
  try {
    const res = await fetch('/api/search/reindex', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rebuild: full})
    });
    const d = await res.json();
    if (d.ok) {
      showToast('Index ' + (full ? 'rebuilt' : 'updated') + ' successfully', 'success');
      await fetchSearchHealth();
      loadSearchSettings();
    } else {
      showToast('Index failed: ' + (d.error || '').substring(0, 120), 'error');
    }
  } catch(e) { showToast(e.message, 'error'); }
  btn.disabled = false;
  btn.textContent = origText;
}

// ---- Setup Wizard ----
// Steps: 0=Welcome  1=Pick  2=Install  3=Sync
let wizardStep = 0;
let wizardSelected = new Set();
let wizardSkipped = new Set();
const WIZARD_STEPS = 4;

function shouldShowWizard() {
  if (localStorage.getItem('vadimgest_wizard_done')) return false;
  const enabled = sourcesData.filter(s => s.enabled);
  return enabled.length === 0;
}

function openWizard() {
  wizardStep = 0;
  wizardSelected = new Set();
  wizardSkipped = new Set();
  document.getElementById('wizard-overlay').classList.add('open');
  document.getElementById('wizard').classList.add('open');
  renderWizardStep();
}

function closeWizard() {
  document.getElementById('wizard-overlay').classList.remove('open');
  document.getElementById('wizard').classList.remove('open');
  localStorage.setItem('vadimgest_wizard_done', '1');
}

function wizSourceByName(name) {
  return sourcesData.find(s => s.name === name);
}

function wizSourceIsReady(name) {
  const s = wizSourceByName(name);
  return !!(s && s.ready && s.ready.ok);
}

function wizSourcesToSync() {
  return [...wizardSelected].filter(n => !wizardSkipped.has(n) && wizSourceIsReady(n));
}

function renderWizardStep() {
  const body = document.getElementById('wizard-body');
  const footer = document.getElementById('wizard-footer');
  const indicator = document.getElementById('wizard-step-indicator');

  if (wizardStep === 0) {
    indicator.textContent = 'Step 1 of ' + WIZARD_STEPS;
    body.innerHTML = '<h2>Welcome to vadimgest</h2>' +
      '<p>Syncs your messages, emails, meetings, and browsing history into a queue for your agents to process - and a searchable archive to find anything when you need it.</p>' +
      '<p>This wizard will help you enable your first data sources and run your first sync.</p>' +
      '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px">' +
        '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px">' +
          '<div style="font-size:24px;margin-bottom:4px">&#x1F4E8;</div>' +
          '<div style="font-size:13px;font-weight:500">Messages</div>' +
          '<div style="font-size:11px;color:var(--text3)">Telegram, Signal, WhatsApp, iMessage</div>' +
        '</div>' +
        '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px">' +
          '<div style="font-size:24px;margin-bottom:4px">&#x1F4E7;</div>' +
          '<div style="font-size:13px;font-weight:500">Email & Tasks</div>' +
          '<div style="font-size:11px;color:var(--text3)">Gmail, Google Tasks, Calendar</div>' +
        '</div>' +
        '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px">' +
          '<div style="font-size:24px;margin-bottom:4px">&#x1F310;</div>' +
          '<div style="font-size:13px;font-weight:500">Activity</div>' +
          '<div style="font-size:11px;color:var(--text3)">Browser, Dayflow, Claude sessions</div>' +
        '</div>' +
        '<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px">' +
          '<div style="font-size:24px;margin-bottom:4px">&#x1F4DD;</div>' +
          '<div style="font-size:13px;font-weight:500">Knowledge</div>' +
          '<div style="font-size:11px;color:var(--text3)">Obsidian, GitHub, Google Drive</div>' +
        '</div>' +
      '</div>';
    footer.innerHTML = '<div></div><button class="btn btn-primary" onclick="wizardStep=1;renderWizardStep()">Get Started &rarr;</button>';

  } else if (wizardStep === 1) {
    indicator.textContent = 'Step 2 of ' + WIZARD_STEPS;
    let html = '<h2>Choose your sources</h2>';
    html += '<p>Pick any sources you want to sync. We\u2019ll walk through installing the tools they need in the next step.</p>';

    const categories = {};
    sourcesData.forEach(s => {
      const cat = s.category || 'other';
      if (!categories[cat]) categories[cat] = [];
      categories[cat].push(s);
    });

    const catOrder = ['messaging', 'email', 'activity', 'knowledge', 'dev', 'meetings', 'social', 'calendar', 'files', 'other'];
    const catNames = {messaging:'Messaging',email:'Email & Tasks',activity:'Activity',knowledge:'Knowledge',dev:'Development',meetings:'Meetings',social:'Social',calendar:'Calendar',files:'Files',other:'Other'};

    // Build set of sources recommended as alternatives (e.g. hlopya via granola's recommended_alt)
    const recommendedSources = new Set();
    sourcesData.forEach(s => {
      const alt = (s.setup_info || {}).recommended_alt;
      if (alt && alt.source) recommendedSources.add(alt.source);
    });

    catOrder.forEach(cat => {
      const sources = categories[cat];
      if (!sources || sources.length === 0) return;
      html += '<div class="wiz-category">' + escHtml(catNames[cat] || cat) + '</div>';
      sources.forEach(s => {
        const sel = wizardSelected.has(s.name);
        const ready = s.ready && s.ready.ok;
        const isRec = recommendedSources.has(s.name);
        const depCount = s.dependencies ? (
          (s.dependencies.python||[]).length +
          (s.dependencies.cli||[]).length +
          (s.dependencies.credentials||[]).length
        ) : 0;
        html += '<div class="source-pick' + (sel ? ' selected' : '') + '" onclick="toggleWizSource(\\'' + s.name + '\\',this)">';
        html += '<input type="checkbox"' + (sel ? ' checked' : '') + ' onclick="event.stopPropagation();toggleWizSource(\\'' + s.name + '\\',this.closest(\\\'.source-pick\\'))">';
        html += '<div class="source-pick-info">';
        html += '<div class="source-pick-name">' + escHtml(s.display_name) + (isRec ? ' <span class="badge badge-rec">recommended</span>' : '') + '</div>';
        if (s.description) html += '<div class="source-pick-desc">' + escHtml(s.description) + '</div>';
        html += '</div>';
        if (ready) {
          html += '<span class="source-pick-badge badge badge-green">ready</span>';
        } else if (depCount > 0) {
          html += '<span class="source-pick-badge badge badge-yellow">' + depCount + ' dep' + (depCount !== 1 ? 's' : '') + '</span>';
        }
        html += '</div>';
      });
    });

    body.innerHTML = html;
    const nextLabel = wizardSelected.size === 0
      ? 'Select at least one source'
      : 'Continue with ' + wizardSelected.size + ' source' + (wizardSelected.size !== 1 ? 's' : '') + ' \u2192';
    footer.innerHTML = '<button class="btn" onclick="wizardStep=0;renderWizardStep()">&larr; Back</button>' +
      '<button class="btn btn-primary" onclick="wizardStep=2;renderWizardStep()"' +
      (wizardSelected.size === 0 ? ' disabled' : '') +
      '>' + nextLabel + '</button>';

  } else if (wizardStep === 2) {
    indicator.textContent = 'Step 3 of ' + WIZARD_STEPS;
    renderWizardInstall();

  } else if (wizardStep === 3) {
    indicator.textContent = 'Step 4 of ' + WIZARD_STEPS;
    let html = '<h2>First sync</h2>';
    const toSync = wizSourcesToSync();
    const notReady = [...wizardSelected].filter(n => !wizSourceIsReady(n) && !wizardSkipped.has(n));
    if (toSync.length === 0) {
      html += '<p>No sources are ready yet. Go back and finish setup, or skip this step.</p>';
    } else {
      html += '<p>Enabling ' + toSync.length + ' ready source' + (toSync.length !== 1 ? 's' : '') + ' and running your first sync\u2026</p>';
    }
    if (wizardSkipped.size > 0) {
      html += '<div class="wiz-setup-note">Skipped: ' + [...wizardSkipped].map(escHtml).join(', ') + '. You can finish their setup later from the source drawer.</div>';
    }
    if (notReady.length > 0) {
      html += '<div class="wiz-setup-note">Still missing deps: ' + notReady.map(escHtml).join(', ') + '. These were left disabled \u2014 open the source to install or skip.</div>';
    }
    html += '<div class="wiz-sync-log" id="wiz-sync-log"><div class="wiz-sync-line">Starting\u2026</div></div>';
    html += '<div id="wiz-autostart-section" style="display:none;margin-top:16px;padding:14px;background:var(--bg3);border-radius:10px">';
    html += '<div style="display:flex;align-items:center;gap:10px">';
    html += '<label class="toggle"><input type="checkbox" id="wiz-autostart-toggle" onchange="wizToggleAutostart(this.checked)"><span class="toggle-track"></span></label>';
    html += '<div><div style="font-size:13px;font-weight:500;color:var(--text)">Start on boot</div>';
    html += '<div style="font-size:11px;color:var(--text3)">Keep the dashboard and sync daemon running in the background, even after restart.</div></div>';
    html += '</div>';
    html += '<div id="wiz-autostart-status" style="font-size:11px;color:var(--accent);margin-top:6px;display:none"></div>';
    html += '</div>';
    body.innerHTML = html;
    footer.innerHTML = '<button class="btn" onclick="wizardStep=2;renderWizardStep()">&larr; Back</button>' +
      '<button class="btn btn-primary" id="wiz-done-btn" disabled onclick="closeWizard();refresh()">Done</button>';
    runWizardSync();
  }
}

function renderWizardInstall() {
  const body = document.getElementById('wizard-body');
  const footer = document.getElementById('wizard-footer');

  let html = '<h2>Install dependencies</h2>';
  html += '<p>For each source below, install what\u2019s missing. You can auto-install most tools, copy a command, or skip anything tricky for now.</p>';

  const selected = [...wizardSelected];
  if (selected.length === 0) {
    html += '<div class="wiz-setup-note">No sources selected \u2014 go back.</div>';
    body.innerHTML = html;
    footer.innerHTML = '<button class="btn" onclick="wizardStep=1;renderWizardStep()">&larr; Back</button>' +
      '<button class="btn btn-primary" disabled>Continue &rarr;</button>';
    return;
  }

  // Unified Google Accounts card when any gog source is selected
  const GOG_SOURCES = ['gmail', 'calendar', 'gdrive', 'gtasks'];
  const selectedGog = selected.filter(n => GOG_SOURCES.includes(n));
  if (selectedGog.length > 0) {
    html += renderGoogleAccountsCard(selectedGog);
  }

  selected.forEach(name => {
    html += renderWizardInstallCard(name);
  });

  body.innerHTML = html;

  const readyCount = selected.filter(n => wizSourceIsReady(n) || wizardSkipped.has(n)).length;
  const allResolved = readyCount === selected.length;
  const nextLabel = allResolved
    ? 'Continue to sync \u2192'
    : 'Install or skip remaining (' + (selected.length - readyCount) + ')';
  footer.innerHTML = '<button class="btn" onclick="wizardStep=1;renderWizardStep()">&larr; Back</button>' +
    '<button class="btn btn-primary" onclick="wizardStep=3;renderWizardStep()"' +
    (allResolved ? '' : ' disabled') + '>' + nextLabel + '</button>';
}

function renderGoogleAccountsCard(selectedGog) {
  // Gather connected accounts from any selected Google source
  const anyGogSource = wizSourceByName(selectedGog[0]);
  const authState = (anyGogSource && anyGogSource.setup_info && anyGogSource.setup_info.auth_state) || {signed_in:false};
  const connectedAccounts = authState.accounts || (authState.signed_in && authState.account ? [authState.account] : []);
  const sourceLabels = {gmail:'Gmail', calendar:'Calendar', gdrive:'Drive', gtasks:'Tasks'};
  const activeLabels = selectedGog.map(n => sourceLabels[n] || n).join(', ');

  let html = '<div class="wiz-install-card' + (connectedAccounts.length > 0 ? ' ready' : '') + '">';
  html += '<div class="wiz-install-head">';
  html += '<div class="wiz-install-head-name">Google Accounts</div>';
  html += '<span class="wiz-install-status ' + (connectedAccounts.length > 0 ? 'ready' : 'pending') + '">' + (connectedAccounts.length > 0 ? connectedAccounts.length + ' connected' : 'not connected') + '</span>';
  html += '</div>';

  html += '<div style="font-size:12px;color:var(--text3);margin-bottom:8px">One Google login covers all selected services: ' + escHtml(activeLabels) + '</div>';

  if (connectedAccounts.length > 0) {
    connectedAccounts.forEach(acc => {
      html += wizDepRow({label:'Account', code: acc, ok:true});
    });
  }

  html += wizDepRow({
    label: 'Auth',
    code: connectedAccounts.length > 0 ? 'Add another account' : 'Connect Google account',
    ok: false,
    action: '<input id="wiz-gog-account" type="text" placeholder="name@gmail.com" style="padding:4px 8px;font-size:12px;width:170px;background:var(--bg);border:1px solid var(--border);border-radius:5px;color:var(--text)"> '
      + '<button class="btn btn-sm btn-primary" onclick="wizGogAuth()">Connect</button>'
  });
  html += '</div>';
  return html;
}

async function wizGogAuth() {
  const inp = document.getElementById('wiz-gog-account');
  const account = (inp && inp.value || '').trim();
  if (!account) {
    showToast('Enter a Google account email first', 'error');
    return;
  }
  try {
    const res = await fetch('/api/auth/cli/start', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({method:'gog', source:'gmail', account:account})
    });
    const data = await res.json();
    if (!data.ok) {
      if (data.needs_install) {
        showToast('Install the gog CLI first', 'error');
      } else {
        showToast('Auth failed: ' + (data.error || ''), 'error');
      }
      return;
    }
    _openAuthModal('gmail', 'gog', data.session);
  } catch(e) {
    showToast('Auth error: ' + e.message, 'error');
  }
}

function renderWizardInstallCard(name) {
  const s = wizSourceByName(name);
  if (!s) return '';
  const ready = wizSourceIsReady(name);
  const skipped = wizardSkipped.has(name);
  const deps = s.dependencies || {};
  const pyDeps = deps.python || [];
  const cliDeps = deps.cli || [];
  const credDeps = deps.credentials || [];
  const osDeps = deps.os || [];
  const envStatus = s.env_status || {};
  const credHelp = s.credential_help || {};
  const missing = (s.ready && s.ready.missing) || [];
  const info = s.setup_info || {};
  const hasAuth = !!info.auth;
  // When a source has an in-dashboard auth method, suppress the raw credential prompts.
  const suppressCreds = hasAuth && (info.auth.method === 'telegram_phone');

  let statusCls = 'pending', statusLabel = 'needs install';
  if (ready) { statusCls = 'ready'; statusLabel = 'ready'; }
  else if (skipped) { statusCls = 'skipped'; statusLabel = 'skipped'; }

  let html = '<div class="wiz-install-card ' + (ready ? 'ready' : (skipped ? 'skipped' : '')) + '" id="wiz-card-' + escHtml(name) + '">';
  html += '<div class="wiz-install-head">';
  html += '<div class="wiz-install-head-name">' + escHtml(s.display_name) + '</div>';
  html += '<span class="wiz-install-status ' + statusCls + '">' + statusLabel + '</span>';
  html += '</div>';

  if (ready) {
    html += '<div style="font-size:12px;color:var(--text3)">All dependencies installed. Will sync in next step.</div>';
  } else if (skipped) {
    html += '<div style="font-size:12px;color:var(--text3)">Skipped \u2014 you can finish setup later from the source drawer.</div>';
    html += '<div class="wiz-install-card-actions">';
    html += '<button class="btn btn-sm" onclick="wizUnskip(\\'' + name + '\\')">Unskip</button>';
    html += '</div>';
  } else {
    if (info.recommended_alt) {
      const alt = info.recommended_alt;
      const altAlreadySelected = wizardSelected.has(alt.source);
      html += '<div class="wiz-setup-alt">'
        + '<div>\\uD83D\\uDCA1 ' + escHtml(alt.reason || ('Consider ' + alt.source)) + '</div>';
      if (altAlreadySelected) {
        html += '<span style="font-size:12px;color:var(--accent)">\\u2714 ' + escHtml(alt.source) + ' already selected</span>';
      } else {
        html += '<button class="btn btn-sm" onclick="wizAddAlt(\\'' + escHtml(alt.source) + '\\')">Also add ' + escHtml(alt.source) + '</button>';
      }
      html += '</div>';
    }
    if (info.post_install_hint) {
      html += '<div class="wiz-setup-hint">' + escHtml(info.post_install_hint) + '</div>';
    }

    // App install (Download link + "I installed it")
    if (info.app_state) {
      const a = info.app_state;
      if (a.installed) {
        html += wizDepRow({label:'App', code: a.display, ok:true});
      } else {
        html += wizDepRow({
          label:'App', code: a.display, ok:false,
          action: '<a class="btn btn-sm btn-primary" target="_blank" href="' + escHtml(a.download_url) + '">Download</a> '
            + '<button class="btn btn-sm" onclick="wizRecheck(\\'' + name + '\\')">I installed it</button>'
        });
      }
    }

    // macOS Full Disk Access deeplink
    if (info.os_help && info.os_help.kind === 'full_disk_access') {
      const ok = !!info.fda_granted;
      html += wizDepRow({
        label:'macOS', code: info.os_help.label || 'Full Disk Access', ok: ok,
        hintHtml: ok ? 'Granted.' : escHtml(info.os_help.instructions || ''),
        action: ok ? '' :
          '<a class="btn btn-sm btn-primary" href="' + escHtml(info.os_help.deeplink) + '">Open Settings</a> '
          + '<button class="btn btn-sm" onclick="wizRecheck(\\'' + name + '\\')">Re-check</button>'
      });
    }

    // Auth method (skip for Google sources - handled by unified card)
    const GOG_SOURCES_SET = ['gmail', 'calendar', 'gdrive', 'gtasks'];
    if (hasAuth && !(info.auth.method === 'gog' && GOG_SOURCES_SET.includes(name))) {
      html += wizRenderAuthRow(name, info.auth, s);
    }

    // Config helper (Obsidian vault / Nextcloud form)
    if (info.config_helper === 'obsidian_vault_picker') {
      html += wizRenderObsidianPicker(name, s);
    } else if (info.config_helper === 'nextcloud_form') {
      html += wizRenderNextcloudForm(name, s);
    }

    pyDeps.forEach(pkg => {
      const isMissing = missing.some(m => m.includes(pkg) && m.includes('Python'));
      html += wizDepRow({
        label: 'Python package',
        code: pkg,
        ok: !isMissing,
        action: isMissing ? ('<button class="btn btn-sm" onclick="wizInstall(\\'' + name + '\\',\\'' + pkg + '\\',\\'pip\\',this)">Install</button>') : ''
      });
    });

    cliDeps.forEach(tool => {
      const isMissing = missing.some(m => m.includes(tool) && m.includes('CLI'));
      let action = '';
      let hint = '';
      if (isMissing) {
        const method = INSTALL_MAP[tool];
        if (method) {
          const pkgName = tool === 'bird' ? '@steipete/bird' : tool;
          action = '<button class="btn btn-sm" onclick="wizInstall(\\'' + name + '\\',\\'' + pkgName + '\\',\\'' + method + '\\',this)">Install</button>';
        } else {
          const h = INSTALL_HINTS[tool] || ('Install ' + tool + ' manually');
          hint = h;
        }
      }
      html += wizDepRow({
        label: 'CLI tool',
        code: tool,
        ok: !isMissing,
        hint: hint,
        action: action
      });
    });

    credDeps.forEach(envVar => {
      const isSet = envStatus[envVar];
      if (suppressCreds) return;
      const help = credHelp[envVar] || {};
      const cl = CRED_LABELS[envVar] || {};
      const friendlyLabel = cl.label || help.description || envVar;
      const placeholder = cl.placeholder || 'value';
      const inputType = cl.inputType || 'password';
      let action = '';
      let hintHtml = '';
      if (!isSet) {
        action = '<div class="wiz-cred-row">'
          + '<input type="' + inputType + '" placeholder="' + escHtml(placeholder) + '" id="wiz-cred-' + envVar + '">'
          + '<button class="btn btn-sm" onclick="wizSaveCred(\\'' + name + '\\',\\'' + envVar + '\\',this)">Save</button>'
          + '</div>';
        if (help.help_url) {
          hintHtml = '<a href="' + escHtml(help.help_url) + '" target="_blank" style="color:var(--accent)">How to get this \u2192</a>';
        } else if (help.description) {
          hintHtml = escHtml(help.description);
        }
      }
      html += wizDepRow({
        label: 'Credential',
        code: friendlyLabel,
        ok: isSet,
        hintHtml: hintHtml,
        action: action
      });
    });

    // OS requirements: hide when satisfied, block when not.
    const currentPlatform = s.current_platform || '';
    osDeps.forEach(req => {
      if (req === 'macos:full_disk_access' && info.os_help) return;
      const isMacReq = req === 'macos' || req.startsWith('macos');
      const reqMet = !isMacReq || currentPlatform === 'darwin';
      if (reqMet) return;
      const label = 'Requires macOS \u2014 only works on a Mac' +
        (currentPlatform ? ' (you\u2019re on ' + currentPlatform + ')' : '');
      html += '<div class="wiz-install-dep">'
        + '<div class="wiz-dot missing"></div>'
        + '<div class="wiz-install-dep-info"><div class="wiz-install-dep-text" style="color:var(--red)">' + escHtml(label) + '</div></div>'
        + '</div>';
    });

    html += '<div class="wiz-install-card-actions">';
    html += '<button class="btn btn-sm" onclick="wizRecheck(\\'' + name + '\\')">Re-check</button>';
    html += '<button class="btn btn-sm" onclick="wizSkip(\\'' + name + '\\')">Skip for now</button>';
    html += '</div>';
  }

  html += '</div>';
  return html;
}

function wizDepRow(opts) {
  let html = '<div class="wiz-install-dep">';
  html += '<div class="wiz-dot ' + (opts.ok ? 'ok' : 'missing') + '"></div>';
  html += '<div class="wiz-install-dep-info">';
  html += '<div class="wiz-install-dep-text">' + escHtml(opts.label) + ': <code>' + escHtml(opts.code) + '</code></div>';
  if (opts.hint) html += '<div class="wiz-install-dep-hint"><code>' + escHtml(opts.hint) + '</code></div>';
  else if (opts.hintHtml) html += '<div class="wiz-install-dep-hint">' + opts.hintHtml + '</div>';
  html += '</div>';
  if (opts.action) html += '<div class="wiz-install-actions">' + opts.action + '</div>';
  html += '</div>';
  return html;
}

async function wizInstall(sourceName, pkg, method, btn) {
  btn.disabled = true; btn.textContent = 'Installing\u2026';
  try {
    const res = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package: pkg, method: method})});
    const data = await res.json();
    if (data.ok) {
      showToast(pkg + ' installed', 'success');
      await refresh();
      renderWizardInstall();
    } else if (data.error === 'needs_brew') {
      btn.textContent = 'Install Homebrew first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing Homebrew\u2026';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'homebrew', method:'brew_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('Homebrew installed', 'success'); wizInstall(sourceName, pkg, method, btn); }
        else { showToast('Homebrew install failed: ' + (d2.error || ''), 'error'); btn.textContent = 'Retry Homebrew'; btn.disabled = false; }
      };
    } else if (data.error === 'needs_npm') {
      btn.textContent = 'Install Node.js first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing Node.js\u2026';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'node', method:'npm_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('Node.js installed', 'success'); wizInstall(sourceName, pkg, method, btn); }
        else { showToast('Node.js install failed: ' + (d2.error || ''), 'error'); btn.textContent = 'Retry Node.js'; btn.disabled = false; }
      };
    } else if (data.error === 'needs_pipx') {
      btn.textContent = 'Install pipx first';
      btn.disabled = false;
      btn.onclick = async function() {
        btn.disabled = true; btn.textContent = 'Installing pipx\u2026';
        const r2 = await fetch('/api/install', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({package:'pipx', method:'pipx_setup'})});
        const d2 = await r2.json();
        if (d2.ok) { showToast('pipx installed', 'success'); wizInstall(sourceName, pkg, method, btn); }
        else { showToast('pipx install failed: ' + (d2.error || ''), 'error'); btn.textContent = 'Retry pipx'; btn.disabled = false; }
      };
    } else {
      showToast('Failed: ' + ((data.error || '') + '').substring(0, 120), 'error');
      btn.textContent = 'Retry';
      btn.disabled = false;
    }
  } catch(e) {
    showToast(e.message, 'error');
    btn.textContent = 'Retry';
    btn.disabled = false;
  }
}

async function wizSaveCred(sourceName, envVar, btn) {
  const input = document.getElementById('wiz-cred-' + envVar);
  if (!input || !input.value.trim()) { showToast('Value required', 'error'); return; }
  btn.disabled = true; btn.textContent = '\u2026';
  try {
    const body = {};
    body[envVar] = input.value.trim();
    const res = await fetch('/api/credentials', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    const data = await res.json();
    if (data.ok) {
      showToast(envVar + ' saved', 'success');
      await refresh();
      renderWizardInstall();
    } else {
      showToast('Failed to save', 'error');
      btn.disabled = false; btn.textContent = 'Save';
    }
  } catch(e) {
    showToast(e.message, 'error');
    btn.disabled = false; btn.textContent = 'Save';
  }
}

async function wizRecheck(sourceName) {
  await refresh();
  renderWizardInstall();
  if (wizSourceIsReady(sourceName)) {
    showToast(sourceName + ' is ready', 'success');
  } else {
    showToast(sourceName + ': still missing deps', 'error');
  }
}

function wizSkip(name) {
  wizardSkipped.add(name);
  renderWizardInstall();
}

function wizUnskip(name) {
  wizardSkipped.delete(name);
  renderWizardInstall();
}

// ---- Per-source setup handlers ------------------------------------------

function wizAddAlt(newName) {
  wizardSelected.add(newName);
  renderWizardInstall();
  showToast('Added ' + newName, 'success');
}

async function wizLinkedInLaunch(sourceName) {
  try {
    const res = await fetch('/api/auth/linkedin/launch', {method:'POST'});
    const data = await res.json();
    if (data.ok) {
      showToast('LinkedIn browser opened - log in and close it when done', 'success');
    } else {
      showToast('Failed: ' + (data.error || 'unknown'), 'error');
    }
  } catch(e) {
    showToast('Failed to launch: ' + e.message, 'error');
  }
}

function wizRenderAuthRow(name, auth, s) {
  const info = s.setup_info || {};
  const method = auth.method;

  if (method === 'telegram_phone') {
    const signedIn = info.telegram_signed_in;
    if (signedIn) {
      return wizDepRow({label:'Telegram', code:'signed in', ok:true});
    }
    return wizDepRow({
      label:'Auth', code: auth.label, ok:false,
      hintHtml: 'We\\u2019ll send an SMS code to verify your number.',
      action: '<button class="btn btn-sm btn-primary" onclick="wizTelegramStart(\\'' + name + '\\')">Sign in with phone number</button>'
    });
  }

  if (method === 'bird') {
    const authState = info.auth_state || {signed_in: false};
    if (authState.signed_in) {
      return wizDepRow({label:'Auth', code:'X / Twitter', ok:true, hintHtml: authState.detail ? escHtml(authState.detail) : 'Signed in via browser cookies.'});
    }
    return wizDepRow({
      label:'Auth', code:'X / Twitter', ok:false,
      hintHtml: 'Log into <a href="https://x.com" target="_blank" style="color:var(--accent)">x.com</a> in Safari, Chrome, or Firefox. Bird reads your browser cookies automatically.',
      action: '<button class="btn btn-sm btn-primary" onclick="wizRecheck(\\'' + name + '\\')">Check</button>'
    });
  }

  if (method === 'linkedin_browser') {
    const authState = info.auth_state || {signed_in: false};
    if (authState.signed_in) {
      return wizDepRow({label:'Auth', code:'LinkedIn', ok:true, hintHtml:'Browser session active.'});
    }
    return wizDepRow({
      label:'Auth', code:'Sign in to LinkedIn', ok:false,
      hintHtml: 'Opens a real Chromium window - log in like you would in a normal browser. We save the session.',
      action: '<button class="btn btn-sm btn-primary" onclick="wizLinkedInLaunch(\\'' + name + '\\')">Open LinkedIn login</button> '
        + '<button class="btn btn-sm" onclick="wizRecheck(\\'' + name + '\\')">Check</button>'
    });
  }

  // CLI-backed auth methods
  const authState = info.auth_state || {signed_in: false, detail: ''};
  const needsAccount = !!auth.needs_account;
  const isMulti = !!auth.multi;
  const connectedAccounts = authState.accounts || (authState.signed_in && authState.account ? [authState.account] : []);

  if (authState.signed_in && needsAccount && isMulti) {
    let html = '';
    connectedAccounts.forEach(acc => {
      html += wizDepRow({
        label: 'Account',
        code: acc,
        ok: true
      });
    });
    html += wizDepRow({
      label: 'Auth',
      code: 'Add another account',
      ok: false,
      action: '<input id="wiz-auth-acc-' + escHtml(name) + '" type="text" placeholder="'
        + escHtml(auth.account_placeholder || 'email@example.com')
        + '" style="padding:4px 8px;font-size:12px;width:170px;background:var(--bg);border:1px solid var(--border);border-radius:5px;color:var(--text)"> '
        + '<button class="btn btn-sm btn-primary" onclick="wizAuthStart(\\'' + name + '\\',\\'' + method + '\\')">Connect</button>'
    });
    return html;
  }

  if (authState.signed_in) {
    return wizDepRow({
      label: 'Auth',
      code: auth.label,
      ok: true,
      hintHtml: authState.detail ? escHtml(authState.detail) : 'Already signed in.',
      action: '<button class="btn btn-sm" onclick="wizAuthStart(\\'' + name + '\\',\\'' + method + '\\')">Re-authenticate</button>'
    });
  }

  let action = '';
  if (needsAccount) {
    action = '<input id="wiz-auth-acc-' + escHtml(name) + '" type="text" placeholder="'
      + escHtml(auth.account_placeholder || 'email@example.com')
      + '" style="padding:4px 8px;font-size:12px;width:170px;background:var(--bg);border:1px solid var(--border);border-radius:5px;color:var(--text)"> ';
  }
  action += '<button class="btn btn-sm btn-primary" onclick="wizAuthStart(\\'' + name + '\\',\\'' + method + '\\')">Sign in</button>';

  return wizDepRow({
    label: 'Auth',
    code: auth.label,
    ok: false,
    hintHtml: needsAccount ? ('Enter your ' + escHtml(auth.account_label || 'account') + ':') : '',
    action: action
  });
}

// ---- CLI auth modal (gh / gog / wacli) ---------------------------

let _authModalState = { sid: null, eventSource: null };

async function wizAuthStart(sourceName, method) {
  let account = '';
  const accInput = document.getElementById('wiz-auth-acc-' + sourceName);
  if (accInput) {
    account = (accInput.value || '').trim();
    if (!account && (method === 'gog' || method.startsWith('gog_'))) {
      showToast('Please enter your Google account email first', 'error');
      return;
    }
  }

  try {
    const res = await fetch('/api/auth/cli/start', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({method: method, source: sourceName, account: account})
    });
    const data = await res.json();
    if (!data.ok) {
      if (data.needs_install) {
        showToast('Install the ' + data.needs_install + ' CLI first', 'error');
      } else {
        showToast('Auth failed: ' + (data.error || ''), 'error');
      }
      return;
    }
    _authModalState.sid = data.session.id;
    _openAuthModal(sourceName, method, data.session);
  } catch(e) {
    showToast('Auth start failed: ' + e.message, 'error');
  }
}

function _openAuthModal(sourceName, method, initial) {
  const backdrop = document.createElement('div');
  backdrop.className = 'auth-modal-backdrop';
  backdrop.id = 'auth-modal-backdrop';
  backdrop.innerHTML =
    '<div class="auth-modal">'
    + '<h3 id="auth-modal-title">Signing in\u2026</h3>'
    + '<div id="auth-modal-status" class="auth-status info">Starting auth session\u2026</div>'
    + '<div id="auth-modal-content"></div>'
    + '<div class="auth-log" id="auth-modal-log"></div>'
    + '<div class="auth-actions">'
    + '<button class="btn btn-sm" onclick="wizAuthCancel()">Cancel</button>'
    + '<button class="btn btn-sm btn-primary" id="auth-modal-done" onclick="wizAuthDone()" style="display:none">Done</button>'
    + '</div>'
    + '</div>';
  document.body.appendChild(backdrop);

  const title = document.getElementById('auth-modal-title');
  if (method === 'gh') title.textContent = 'Sign in to GitHub';
  else if (method === 'gog' || method.startsWith('gog_')) title.textContent = 'Sign in to Google';
  else if (method === 'bird') title.textContent = 'Sign in to X / Twitter';
  else if (method === 'wacli_pair') title.textContent = 'Pair WhatsApp';
  else title.textContent = 'Auth';

  _authModalRender(initial, method);
  _authModalStream(initial.id, method, sourceName);
}

function _authModalStream(sid, method, sourceName) {
  if (_authModalState.eventSource) { try { _authModalState.eventSource.close(); } catch(e){} }
  const es = new EventSource('/api/auth/cli/stream/' + sid);
  _authModalState.eventSource = es;
  es.onmessage = function(ev) {
    try {
      const snap = JSON.parse(ev.data);
      _authModalRender(snap, method);
      if (snap.done) {
        try { es.close(); } catch(e){}
        _authModalState.eventSource = null;
        refresh().then(() => {
          renderWizardInstall();
          refreshDrawer();
        });
      }
    } catch(e) { console.error(e); }
  };
  es.onerror = function() { try { es.close(); } catch(e){} };
}

function _authModalRender(snap, method) {
  const status = document.getElementById('auth-modal-status');
  const content = document.getElementById('auth-modal-content');
  const log = document.getElementById('auth-modal-log');
  const doneBtn = document.getElementById('auth-modal-done');
  if (!status || !content) return;

  let html = '';
  if (snap.device_code) {
    html += '<div style="font-size:12px;color:var(--text2)">Enter this code on the verification page:</div>';
    html += '<div class="auth-device-code" onclick="navigator.clipboard.writeText(this.textContent);showToast(\\'Code copied\\',\\'success\\')">' + escHtml(snap.device_code) + '</div>';
  }
  if (snap.verification_url) {
    html += '<div class="auth-url" style="margin:10px 0">'
      + '<a href="' + escHtml(snap.verification_url) + '" target="_blank">\u2192 ' + escHtml(snap.verification_url) + '</a>'
      + '</div>';
    html += '<div style="font-size:11px;color:var(--text3);margin-bottom:10px">Opening a new tab so you can sign in. Come back here when done.</div>';
  }
  if (snap.qr_text) {
    html += '<div style="font-size:12px;color:var(--text2);margin-bottom:6px">Scan from WhatsApp \u2192 Settings \u2192 Linked Devices:</div>';
    html += '<div class="auth-qr">' + escHtml(snap.qr_text) + '</div>';
  }
  content.innerHTML = html;

  if (log) {
    log.textContent = (snap.lines || []).join('\\n');
    log.scrollTop = log.scrollHeight;
  }

  if (snap.done) {
    // Some CLIs (gog, notably) close their pty before waitpid settles,
    // leaving exit_code=null even on success. Trust the summary parser
    // first. Second-best: if any of the snap.lines look like a success
    // marker (email/services/token issued), treat as success too.
    var looksSuccess = snap.exit_code === 0 || !!snap.summary;
    if (!looksSuccess && (snap.lines || []).length > 0) {
      var last = (snap.lines || []).slice(-10).join('\\n');
      if (/Authorization received|signed in|authenticated|Logged in|^email\\s/im.test(last)) {
        looksSuccess = true;
      }
    }
    if (looksSuccess) {
      status.className = 'auth-status success';
      status.textContent = snap.summary || 'Signed in \u2713';
      if (doneBtn) doneBtn.style.display = 'inline-block';
    } else {
      status.className = 'auth-status error';
      var exitDesc = snap.exit_code === null ? 'no exit code captured' : 'exit ' + snap.exit_code;
      status.textContent = 'Auth failed (' + exitDesc + '). Check the log below.';
      if (doneBtn) doneBtn.style.display = 'inline-block';
    }
  } else if (snap.verification_url || snap.device_code || snap.qr_text) {
    status.className = 'auth-status info';
    status.textContent = 'Waiting for you to complete the flow in the other tab\u2026';
  } else {
    status.className = 'auth-status info';
    status.textContent = 'Running\u2026';
  }
}

function wizAuthCancel() {
  if (_authModalState.sid) {
    fetch('/api/auth/cli/stop/' + _authModalState.sid, {method:'POST'}).catch(()=>{});
  }
  _closeAuthModal();
}

function wizAuthDone() {
  _closeAuthModal();
  refresh().then(() => { renderWizardInstall(); refreshDrawer(); });
}

function _closeAuthModal() {
  if (_authModalState.eventSource) { try { _authModalState.eventSource.close(); } catch(e){} }
  _authModalState = { sid: null, eventSource: null };
  const backdrop = document.getElementById('auth-modal-backdrop');
  if (backdrop) backdrop.remove();
}

// ---- Telegram phone/SMS modal -------------------------------------------

let _tgAuthState = { sid: null, api_id: null, api_hash: null };

function wizTelegramStart(sourceName, useOwnApi) {
  const backdrop = document.createElement('div');
  backdrop.className = 'auth-modal-backdrop';
  backdrop.id = 'auth-modal-backdrop';
  const apiFields =
    '<div id="tg-api-section" style="display:none">'
    + '<div class="auth-field"><label>API ID</label><input id="tg-api-id" placeholder="12345"></div>'
    + '<div class="auth-field"><label>API Hash</label><input id="tg-api-hash" placeholder="abcd\u2026"></div>'
    + '</div>'
    + '<div style="margin-bottom:8px"><a href="#" id="tg-show-api" onclick="event.preventDefault();document.getElementById(\\'tg-api-section\\').style.display=\\'block\\';this.style.display=\\'none\\';" style="font-size:11px;color:var(--text3)">Use my own API app (advanced)</a></div>';
  backdrop.innerHTML =
    '<div class="auth-modal">'
    + '<h3>Sign in to Telegram</h3>'
    + '<p>Same flow as Telegram Desktop. We\u2019ll send you an SMS code.</p>'
    + '<div id="auth-modal-status" class="auth-status info" style="display:none"></div>'
    + '<div id="tg-step-1">'
    +   '<div class="auth-field"><label>Phone number (with country code)</label>'
    +     '<input id="tg-phone" placeholder="+15551234567" type="tel"></div>'
    +   apiFields
    +   '<div class="auth-actions">'
    +     '<button class="btn btn-sm" onclick="_closeAuthModal()">Cancel</button>'
    +     '<button class="btn btn-sm btn-primary" id="tg-send-btn" onclick="wizTelegramSendCode(\\'' + sourceName + '\\')">Send code</button>'
    +   '</div>'
    + '</div>'
    + '<div id="tg-step-2" style="display:none">'
    +   '<div class="auth-field"><label>SMS code</label><input id="tg-code" placeholder="12345" autocomplete="one-time-code"></div>'
    +   '<div id="tg-2fa-field" style="display:none" class="auth-field"><label>2FA password (cloud password)</label><input id="tg-2fa" type="password"></div>'
    +   '<div class="auth-actions">'
    +     '<button class="btn btn-sm" onclick="_closeAuthModal()">Cancel</button>'
    +     '<button class="btn btn-sm btn-primary" id="tg-verify-btn" onclick="wizTelegramVerify(\\'' + sourceName + '\\')">Verify</button>'
    +   '</div>'
    + '</div>'
    + '</div>';
  document.body.appendChild(backdrop);
}

async function wizTelegramSendCode(sourceName) {
  const phoneEl = document.getElementById('tg-phone');
  const phone = (phoneEl.value || '').trim();
  if (!phone) { showToast('Phone number required', 'error'); return; }

  const body = {phone: phone};
  const apiIdEl = document.getElementById('tg-api-id');
  const apiHashEl = document.getElementById('tg-api-hash');
  if (apiIdEl && apiHashEl) {
    const apiId = (apiIdEl.value || '').trim();
    const apiHash = (apiHashEl.value || '').trim();
    if (apiId && apiHash) {
      body.api_id = apiId;
      body.api_hash = apiHash;
      _tgAuthState.api_id = apiId;
      _tgAuthState.api_hash = apiHash;
    }
  }

  const btn = document.getElementById('tg-send-btn');
  btn.disabled = true; btn.textContent = 'Sending\u2026';
  const status = document.getElementById('auth-modal-status');

  try {
    const res = await fetch('/api/telegram/auth/send-code', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (!data.ok) {
      status.style.display = 'block';
      status.className = 'auth-status error';
      status.textContent = data.error || 'Failed to send code';
      btn.disabled = false; btn.textContent = 'Send code';
      return;
    }
    _tgAuthState.sid = data.session_id;
    if (data.session && data.session.done && data.session.user) {
      status.style.display = 'block';
      status.className = 'auth-status success';
      status.textContent = 'Already signed in as ' + (data.session.user.first_name || 'user');
      setTimeout(() => { _closeAuthModal(); refresh().then(() => { renderWizardInstall(); refreshDrawer(); }); }, 1200);
      return;
    }
    document.getElementById('tg-step-1').style.display = 'none';
    document.getElementById('tg-step-2').style.display = 'block';
    status.style.display = 'block';
    status.className = 'auth-status info';
    status.textContent = 'Code sent. Check your Telegram.';
    setTimeout(() => { const codeEl = document.getElementById('tg-code'); if (codeEl) codeEl.focus(); }, 100);
  } catch(e) {
    status.style.display = 'block';
    status.className = 'auth-status error';
    status.textContent = e.message;
    btn.disabled = false; btn.textContent = 'Send code';
  }
}

async function wizTelegramVerify(sourceName) {
  const code = (document.getElementById('tg-code').value || '').trim();
  const twofa = document.getElementById('tg-2fa');
  const password = twofa && twofa.offsetParent !== null ? (twofa.value || '') : null;
  if (!code && !password) { showToast('Code required', 'error'); return; }

  const btn = document.getElementById('tg-verify-btn');
  btn.disabled = true; btn.textContent = 'Verifying\u2026';
  const status = document.getElementById('auth-modal-status');

  const body = {session_id: _tgAuthState.sid, code: code};
  if (password) body.password = password;
  if (_tgAuthState.api_id) { body.api_id = _tgAuthState.api_id; body.api_hash = _tgAuthState.api_hash; }

  try {
    const res = await fetch('/api/telegram/auth/verify', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (data.ok) {
      status.className = 'auth-status success';
      status.textContent = 'Signed in as ' + (data.user && data.user.first_name ? data.user.first_name : 'user');
      setTimeout(() => { _closeAuthModal(); refresh().then(() => { renderWizardInstall(); refreshDrawer(); }); }, 1200);
      return;
    }
    if (data.needs_2fa) {
      document.getElementById('tg-2fa-field').style.display = 'flex';
      status.className = 'auth-status info';
      status.textContent = 'Enter your cloud password (2FA).';
      btn.disabled = false; btn.textContent = 'Verify';
      setTimeout(() => { const el = document.getElementById('tg-2fa'); if (el) el.focus(); }, 100);
      return;
    }
    status.className = 'auth-status error';
    status.textContent = data.error || 'Verification failed';
    btn.disabled = false; btn.textContent = 'Verify';
  } catch(e) {
    status.className = 'auth-status error';
    status.textContent = e.message;
    btn.disabled = false; btn.textContent = 'Verify';
  }
}

// ---- Obsidian vault picker ----------------------------------------------

function wizRenderObsidianPicker(name, s) {
  const current = s.current_config && s.current_config.vault_path ? s.current_config.vault_path : '';
  let html = '<div class="wiz-install-dep">';
  html += '<div class="wiz-dot ' + (current ? 'ok' : 'missing') + '"></div>';
  html += '<div class="wiz-install-dep-info">';
  html += '<div class="wiz-install-dep-text">Vault: <code>' + (current ? escHtml(current) : 'not set') + '</code></div>';
  html += '<div class="wiz-install-dep-hint">We\u2019ll scan common locations for <code>.obsidian</code> folders.</div>';
  html += '</div>';
  html += '<div class="wiz-install-actions">';
  html += '<button class="btn btn-sm" onclick="wizObsidianScan(\\'' + name + '\\')">Pick vault</button>';
  html += '</div>';
  html += '</div>';
  html += '<div id="wiz-vault-list-' + escHtml(name) + '"></div>';
  return html;
}

async function wizObsidianScan(name) {
  const listEl = document.getElementById('wiz-vault-list-' + name);
  if (!listEl) return;
  listEl.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:8px 0">Scanning\u2026</div>';
  try {
    const res = await fetch('/api/obsidian/vaults');
    const data = await res.json();
    const vaults = data.vaults || [];
    let html = '<div class="wiz-vault-list">';
    if (vaults.length === 0) {
      html += '<div style="font-size:12px;color:var(--text3)">No vaults found in common locations.</div>';
    } else {
      vaults.forEach(v => {
        html += '<div class="wiz-vault-item">';
        html += '<div><div style="font-weight:500">' + escHtml(v.name) + '</div>';
        html += '<div class="wiz-vault-path">' + escHtml(v.path) + ' \u00B7 ' + v.file_count + ' .md files</div></div>';
        html += '<button class="btn btn-sm btn-primary" onclick="wizObsidianPick(\\'' + name + '\\',this.dataset.path)" data-path="' + escHtml(v.path) + '">Use</button>';
        html += '</div>';
      });
    }
    html += '<div class="wiz-vault-item">';
    html += '<input type="text" id="wiz-vault-manual-' + name + '" placeholder="Or paste path manually..." style="flex:1;padding:5px 8px;background:var(--bg2);border:1px solid var(--border);border-radius:5px;color:var(--text);font-size:11px">';
    html += '<button class="btn btn-sm" onclick="wizObsidianManual(\\'' + name + '\\')">Use</button>';
    html += '</div>';
    html += '</div>';
    listEl.innerHTML = html;
  } catch(e) {
    listEl.innerHTML = '<div style="font-size:12px;color:var(--red)">Scan failed: ' + e.message + '</div>';
  }
}

function wizObsidianManual(name) {
  const input = document.getElementById('wiz-vault-manual-' + name);
  if (!input || !input.value.trim()) { showToast('Path required', 'error'); return; }
  wizObsidianPick(name, input.value.trim());
}

async function wizObsidianPick(name, vaultPath) {
  try {
    const res = await fetch('/api/sources/' + name, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({config: {vault_path: vaultPath}})
    });
    const data = await res.json();
    if (data.ok === false && data.errors) {
      showToast('Invalid: ' + data.errors.join(', '), 'error');
      return;
    }
    showToast('Vault set: ' + vaultPath, 'success');
    await refresh();
    renderWizardInstall();
  } catch(e) {
    showToast('Failed: ' + e.message, 'error');
  }
}

// ---- Nextcloud form ------------------------------------------------------

function wizRenderNextcloudForm(name, s) {
  const cur = s.current_config || {};
  let html = '<div class="wiz-nc-form">';
  html += '<label>Server URL</label>';
  html += '<input id="wiz-nc-server-' + name + '" type="text" placeholder="https://cloud.example.com" value="' + escHtml(cur.server || '') + '">';
  html += '<label>Username</label>';
  html += '<input id="wiz-nc-user-' + name + '" type="text" placeholder="you" value="' + escHtml(cur.username || '') + '">';
  html += '<label>App password <a href="https://docs.nextcloud.com/server/latest/user_manual/en/session_management.html#managing-devices" target="_blank" style="color:var(--accent);font-size:10px;text-transform:none;letter-spacing:0">(how?)</a></label>';
  html += '<input id="wiz-nc-token-' + name + '" type="password" placeholder="xxxxx-xxxxx-xxxxx-xxxxx" value="' + escHtml(cur.token || '') + '">';
  html += '<div class="wiz-nc-actions">';
  html += '<button class="btn btn-sm" onclick="wizNextcloudTest(\\'' + name + '\\')">Test</button>';
  html += '<button class="btn btn-sm btn-primary" onclick="wizNextcloudSave(\\'' + name + '\\')">Save</button>';
  html += '</div>';
  html += '<div class="wiz-nc-test-result" id="wiz-nc-result-' + name + '"></div>';
  html += '</div>';
  return html;
}

function _wizNextcloudFormData(name) {
  return {
    server: (document.getElementById('wiz-nc-server-' + name).value || '').trim(),
    username: (document.getElementById('wiz-nc-user-' + name).value || '').trim(),
    token: (document.getElementById('wiz-nc-token-' + name).value || '').trim(),
  };
}

async function wizNextcloudTest(name) {
  const data = _wizNextcloudFormData(name);
  const out = document.getElementById('wiz-nc-result-' + name);
  out.innerHTML = '<span style="color:var(--text3)">Testing\u2026</span>';
  try {
    const res = await fetch('/api/nextcloud/test', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data)});
    const r = await res.json();
    if (r.ok) out.innerHTML = '<span style="color:var(--green)">\u2713 ' + escHtml(r.message) + '</span>';
    else out.innerHTML = '<span style="color:var(--red)">\u2717 ' + escHtml(r.message) + '</span>';
  } catch(e) {
    out.innerHTML = '<span style="color:var(--red)">' + escHtml(e.message) + '</span>';
  }
}

async function wizNextcloudSave(name) {
  const data = _wizNextcloudFormData(name);
  const out = document.getElementById('wiz-nc-result-' + name);
  if (!data.server || !data.username || !data.token) {
    out.innerHTML = '<span style="color:var(--red)">All fields required</span>';
    return;
  }
  out.innerHTML = '<span style="color:var(--text3)">Testing then saving\u2026</span>';
  try {
    const testRes = await fetch('/api/nextcloud/test', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data)});
    const testData = await testRes.json();
    if (!testData.ok) {
      out.innerHTML = '<span style="color:var(--red)">\u2717 ' + escHtml(testData.message) + ' - not saving.</span>';
      return;
    }
    const res = await fetch('/api/sources/' + name, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({config: data})
    });
    const r = await res.json();
    if (r.ok) {
      out.innerHTML = '<span style="color:var(--green)">\u2713 Saved</span>';
      await refresh();
      renderWizardInstall();
    } else {
      out.innerHTML = '<span style="color:var(--red)">Save failed: ' + escHtml(JSON.stringify(r.errors || r.error || '')) + '</span>';
    }
  } catch(e) {
    out.innerHTML = '<span style="color:var(--red)">' + escHtml(e.message) + '</span>';
  }
}

function toggleWizSource(name, el) {
  if (wizardSelected.has(name)) {
    wizardSelected.delete(name);
    wizardSkipped.delete(name);
    el.classList.remove('selected');
    el.querySelector('input').checked = false;
  } else {
    wizardSelected.add(name);
    el.classList.add('selected');
    el.querySelector('input').checked = true;
  }
  const btn = document.querySelector('.wizard-footer .btn-primary');
  if (btn) {
    btn.disabled = wizardSelected.size === 0;
    const label = wizardSelected.size === 0
      ? 'Select at least one source'
      : 'Continue with ' + wizardSelected.size + ' source' + (wizardSelected.size !== 1 ? 's' : '') + ' \u2192';
    btn.textContent = label;
  }
}

async function runWizardSync() {
  const log = document.getElementById('wiz-sync-log');
  const toSync = wizSourcesToSync();

  try {
    await fetch('/api/config/init', {method:'POST'});
    log.innerHTML += '<div class="wiz-sync-line">Config file ready</div>';
  } catch(e) {}

  if (toSync.length === 0) {
    log.innerHTML += '<div class="wiz-sync-line">Nothing to sync \u2014 no ready sources.</div>';
    await _wizEnsureAutostart();
    const doneBtn = document.getElementById('wiz-done-btn');
    if (doneBtn) { doneBtn.disabled = false; doneBtn.textContent = 'Finish'; }
    return;
  }

  for (const name of toSync) {
    log.innerHTML += '<div class="wiz-sync-line">Enabling ' + escHtml(name) + '\u2026</div>';
    log.scrollTop = log.scrollHeight;
    try {
      const res = await fetch('/api/sources/' + name, {
        method: 'PUT',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({enabled: true})
      });
      const data = await res.json();
      if (data.ok) {
        log.innerHTML += '<div class="wiz-sync-ok">\u2713 ' + escHtml(name) + ' enabled</div>';
      } else {
        log.innerHTML += '<div class="wiz-sync-err">\u2717 ' + escHtml(name) + ': ' + escHtml(data.error || 'failed') + '</div>';
      }
    } catch(e) {
      log.innerHTML += '<div class="wiz-sync-err">\u2717 ' + escHtml(name) + ': ' + escHtml(e.message) + '</div>';
    }
  }

  log.innerHTML += '<div class="wiz-sync-line" style="margin-top:8px">Running first sync\u2026</div>';
  let successCount = 0;
  for (const name of toSync) {
    log.scrollTop = log.scrollHeight;
    try {
      const res = await fetch('/api/sources/' + name + '/sync', {method:'POST'});
      const data = await res.json();
      if (data.ok) {
        const summary = data.summary && data.summary.length > 0 ? ' (' + data.summary.slice(0,3).join(', ') + ')' : '';
        log.innerHTML += '<div class="wiz-sync-ok">\u2713 ' + escHtml(name) + ': <span class="wiz-sync-count">+' + (data.count||0) + '</span>' + escHtml(summary) + '</div>';
        successCount++;
      } else {
        log.innerHTML += '<div class="wiz-sync-err">\u2717 ' + escHtml(name) + ': ' + escHtml(data.error || 'sync failed').substring(0,100) + '</div>';
      }
    } catch(e) {
      log.innerHTML += '<div class="wiz-sync-err">\u2717 ' + escHtml(name) + ': ' + escHtml(e.message) + '</div>';
    }
  }

  log.innerHTML += '<div class="wiz-sync-line" style="margin-top:8px;font-weight:500">' + successCount + '/' + toSync.length + ' sources synced successfully</div>';
  log.scrollTop = log.scrollHeight;

  // Show autostart toggle. Enabled-by-default: auto-install on first
  // reveal so the daemons come up after reboot without the user having
  // to flip anything. User can still turn it off via the toggle.
  await _wizEnsureAutostart();

  const doneBtn = document.getElementById('wiz-done-btn');
  if (doneBtn) { doneBtn.disabled = false; doneBtn.textContent = 'Go to Dashboard \u2192'; }
}

async function _wizEnsureAutostart() {
  const autostartSection = document.getElementById('wiz-autostart-section');
  if (!autostartSection) return;
  autostartSection.style.display = 'block';
  const toggle = document.getElementById('wiz-autostart-toggle');
  const statusEl = document.getElementById('wiz-autostart-status');
  try {
    const asRes = await fetch('/api/autostart');
    const asData = await asRes.json();
    if (asData.installed) {
      if (toggle) toggle.checked = true;
      return;
    }
    // Not installed yet \u2014 install by default. User can toggle off after.
    if (statusEl) {
      statusEl.style.display = 'block';
      statusEl.textContent = 'Enabling start on boot\u2026';
    }
    const inst = await fetch('/api/autostart', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({port: 8484, interval: 300}),
    });
    const instData = await inst.json();
    if (instData.ok) {
      if (toggle) toggle.checked = true;
      if (statusEl) {
        statusEl.textContent = '\u2713 Start on boot is on. Toggle off if you don\\'t want it.';
      }
    } else {
      if (toggle) toggle.checked = false;
      if (statusEl) {
        statusEl.textContent = 'Could not enable automatically: ' + (instData.error || 'unknown');
      }
    }
  } catch(e) {
    if (toggle) toggle.checked = false;
    if (statusEl) {
      statusEl.style.display = 'block';
      statusEl.textContent = 'Autostart check failed: ' + e.message;
    }
  }
}

async function wizToggleAutostart(enable) {
  const status = document.getElementById('wiz-autostart-status');
  if (!status) return;
  status.style.display = 'block';
  status.style.color = 'var(--text3)';
  status.textContent = enable ? 'Installing autostart\u2026' : 'Removing autostart\u2026';
  try {
    const res = await fetch('/api/autostart', {
      method: enable ? 'POST' : 'DELETE',
      headers: {'Content-Type': 'application/json'},
      body: enable ? JSON.stringify({port: 8484, interval: 300}) : undefined
    });
    const data = await res.json();
    if (data.ok) {
      status.style.color = 'var(--accent)';
      status.textContent = enable ? 'Autostart enabled - vadimgest will run on boot.' : 'Autostart removed.';
    } else {
      status.style.color = 'var(--red)';
      status.textContent = 'Failed: ' + (data.error || 'unknown error');
    }
  } catch(e) {
    status.style.color = 'var(--red)';
    status.textContent = 'Error: ' + e.message;
  }
}

// ---- Init ----
window.onerror = function(msg, url, line) {
  showToast('JS Error: ' + msg + ' (line ' + line + ')', 'error');
};

// Global keyboard: Escape closes topmost modal, Cmd/Ctrl+S saves config drawer.
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    const picker = document.getElementById('path-picker-modal');
    if (picker && picker.style.display === 'flex') {
      picker.style.display = 'none';
      e.preventDefault();
      return;
    }
    const authModal = document.getElementById('auth-modal-backdrop');
    if (authModal) {
      if (typeof _closeAuthModal === 'function') _closeAuthModal();
      e.preventDefault();
      return;
    }
    const wiz = document.getElementById('wizard');
    if (wiz && wiz.classList.contains('open')) {
      // Don't let Escape nuke wizard progress silently - just focus close button if exists
      return;
    }
    const drawer = document.getElementById('drawer');
    if (drawer && drawer.classList.contains('open')) {
      closeDrawer();
      e.preventDefault();
      return;
    }
  }
  if ((e.metaKey || e.ctrlKey) && e.key === 's') {
    const drawer = document.getElementById('drawer');
    if (drawer && drawer.classList.contains('open') && openSourceName) {
      e.preventDefault();
      saveConfig(openSourceName);
    }
  }
});

refresh().then(() => {
  connectSSE();
  if (shouldShowWizard()) openWizard();
}).catch(e => {
  showToast('Init error: ' + e.message, 'error');
  console.error('Init error', e);
});
setInterval(() => {
  const activeTab = document.querySelector('.tab.active');
  if (activeTab && activeTab.getAttribute('data-tab') === 'observatory') {
    refresh();
  }
}, 30000);
</script>
</body>
</html>"""
