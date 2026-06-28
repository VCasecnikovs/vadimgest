"""Centralized configuration for vadimgest.

Config resolution order:
1. VADIMGEST_CONFIG env var (path to config.yaml)
2. ~/.config/vadimgest/config.yaml (XDG - for standalone installs)
3. ~/.vadimgest/config.yaml (home dotfolder - canonical user config)
4. ./config.yaml (next to package - legacy fallback)

Data directory resolution:
1. VADIMGEST_DATA_DIR env var
2. config.yaml "data_dir" key
3. ./data/ (next to package)
4. ~/.local/share/vadimgest/ (XDG fallback)
"""

import os
import socket
from functools import lru_cache
from pathlib import Path

import yaml

# Package directory (where this file lives)
_PACKAGE_DIR = Path(__file__).parent
# User-scoped config dotfolder at ~/.vadimgest/ (OpenClaw-style: user data lives
# outside the repo tree). Holds config.yaml and .env; can grow to data/, logs/, etc.
_HOME_CONFIG_DIR = Path.home() / ".vadimgest"


def _load_dotenv():
    """Try to load .env from config dir, repo roots, or cwd."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    # Load multiple locations without overriding already-set values. This lets a
    # sparse source-specific .env coexist with a repo-level .env.
    xdg_cfg = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config")))
    for env_path in [
        _find_config_dir() / ".env" if _find_config_dir() else None,
        xdg_cfg / "vadimgest" / ".env",
        _HOME_CONFIG_DIR / ".env",
        _PACKAGE_DIR / ".env",
        _PACKAGE_DIR.parent / ".env",
        _PACKAGE_DIR.parents[1] / ".env",
        Path.cwd() / ".env",
    ]:
        if env_path and env_path.exists():
            load_dotenv(env_path, override=False)


def _find_config_dir() -> Path | None:
    """Find the directory containing the config file."""
    config_file = _find_config_file()
    return config_file.parent if config_file else None


def _find_config_file() -> Path | None:
    """Find config file in resolution order."""
    # 1. VADIMGEST_CONFIG env var
    env_path = os.environ.get("VADIMGEST_CONFIG")
    if env_path:
        p = Path(env_path).expanduser()
        if p.exists():
            return p

    # 2. XDG config dir (primary for standalone installs)
    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    xdg_config = Path(xdg) / "vadimgest" / "config.yaml"
    if xdg_config.exists():
        return xdg_config

    # 3. ~/.vadimgest/config.yaml (user dotfolder - canonical)
    home_config = _HOME_CONFIG_DIR / "config.yaml"
    if home_config.exists():
        return home_config

    # 4. ./config.yaml next to package (legacy, pre-dotfolder layout)
    pkg_config = _PACKAGE_DIR / "config.yaml"
    if pkg_config.exists():
        return pkg_config

    return None


def _expand_path(path_str: str | None) -> Path | None:
    """Expand ~ and env vars in path."""
    if not path_str:
        return None
    expanded = os.path.expandvars(os.path.expanduser(str(path_str)))
    return Path(expanded)


# ---- Public API ----


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load and merge YAML config. Cached after first call."""
    config = {}

    config_file = _find_config_file()
    if config_file:
        with open(config_file) as f:
            config = yaml.safe_load(f) or {}

        # Load local overrides next to config file
        local_file = config_file.parent / "config.local.yaml"
        if local_file.exists():
            with open(local_file) as f:
                local = yaml.safe_load(f) or {}
                for key, value in local.items():
                    if isinstance(value, dict) and key in config:
                        config[key].update(value)
                    else:
                        config[key] = value

    return config


def get_data_dir() -> Path:
    """Get data directory, creating if needed."""
    # 1. VADIMGEST_DATA_DIR env var
    env = os.environ.get("VADIMGEST_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # 2. config.yaml "data_dir" key
    config = load_config()
    custom = config.get("data_dir")
    if custom:
        p = _expand_path(custom)
        if p:
            p.mkdir(parents=True, exist_ok=True)
            return p

    # 3. XDG data dir (primary for standalone installs)
    xdg = os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))
    p = Path(xdg) / "vadimgest"
    if p.exists():
        return p

    # 4. ./data/ next to package (submodule / dev)
    pkg_data = _PACKAGE_DIR / "data"
    if pkg_data.exists() or (_PACKAGE_DIR / "config.yaml").exists():
        pkg_data.mkdir(exist_ok=True)
        return pkg_data

    # 5. XDG fallback (create it)
    p = Path(xdg) / "vadimgest"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_credentials_dir() -> Path:
    """Get credentials directory."""
    p = get_data_dir() / "credentials"
    p.mkdir(exist_ok=True)
    return p


def get_sources_dir() -> Path:
    """Get sources data directory."""
    p = get_data_dir() / "sources"
    p.mkdir(exist_ok=True)
    return p


def get_source_config(source_name: str) -> dict:
    """Get config for a specific source, merged with defaults."""
    config = load_config()
    source_cfg = config.get(source_name, {})

    # Merge with structural defaults (no personal data)
    defaults = _SOURCE_DEFAULTS.get(source_name, {})
    merged = {**defaults, **source_cfg}

    # Expand path values
    for key in ("vault_path", "cache_path", "db_path", "projects_dir",
                "session_file", "legacy_db", "browser_data_dir", "media_dir",
                "native_db_path", "attachments_dir"):
        if key in merged and isinstance(merged[key], str):
            merged[key] = _expand_path(merged[key])

    # Telegram: ensure session_file and credentials paths
    if source_name == "telegram":
        if not merged.get("session_file"):
            merged["session_file"] = get_credentials_dir() / "telegram_session.txt"
        merged["api_id"] = os.getenv("TELEGRAM_API_ID") or merged.get("api_id") or ""
        merged["api_hash"] = os.getenv("TELEGRAM_API_HASH") or merged.get("api_hash") or ""

    return merged


def get_conversation_settings() -> dict:
    """Get conversation grouping settings."""
    config = load_config()
    conv = config.get("conversation", {})
    return {
        "time_window_hours": conv.get("time_window_hours", 4),
        "min_messages_per_chunk": conv.get("min_messages_per_chunk", 3),
        "max_messages_per_chunk": conv.get("max_messages_per_chunk", 100),
    }


def get_all_source_configs() -> dict:
    """Get config for all known sources."""
    return {name: get_source_config(name) for name in _SOURCE_DEFAULTS}


def get_edge_config() -> dict:
    """Get local edge-agent configuration merged with defaults."""
    config = load_config()
    edge = config.get("edge", {}) or {}
    sources = edge.get("sources")
    if sources is not None and not isinstance(sources, list):
        sources = None
    return {
        "enabled": bool(edge.get("enabled", False)),
        "server_url": str(edge.get("server_url") or "").rstrip("/"),
        "device_id": str(edge.get("device_id") or socket.gethostname() or "edge-device"),
        "interval_seconds": int(edge.get("interval_seconds") or 300),
        "batch_size": int(edge.get("batch_size") or 100),
        "sources": sources,
        "token_configured": bool(os.environ.get("VADIMGEST_EDGE_TOKEN")),
    }


# ---- Structural defaults (NO personal data) ----

_SOURCE_DEFAULTS = {
    "telegram": {
        "enabled": False,
        "mode": "daemon",
        "monitored_folders": [],
        "include_contacts": True,
        "auto_add_private": True,
        "download_media": False,
        "describe_images": False,
        "image_describer_provider": "gemini",
        "image_describer_model": "gemini-3.5-flash",
        "ocr_images": False,
        "ocr_lang": "eng",
        "max_image_bytes": 8000000,
        "index_non_image_media": True,
    },
    "signal": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "include_attachment_paths": True,
        "attachment_summary": True,
    },
    "bee": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "bee_bin": "/srv/codex-klava/data/bee-npm/node_modules/.bin/bee",
        "sync_dir": "~/.local/share/vadimgest/bee-sync",
        "recent_days": 7,
    },
    "granola": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
    },
    "dayflow": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
    },
    "obsidian": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
        "skip_dirs": [".obsidian", ".trash", ".git", "templates", "Templates"],
        "include_extensions": [".md", ".markdown"],
    },
    "claude": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
    },
    "github": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "projects": [],
        "repos": [],
    },
    "gmail": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "accounts": [],
        "query": "newer_than:1d -in:spam -in:trash",
        "bootstrap_query": "newer_than:7d -in:spam -in:trash",
        "sent_query": "in:sent newer_than:7d",
        "sent_bootstrap_query": "in:sent newer_than:14d",
        "follow_up_hours": 48,
        "page_size": 25,
        "batch_size": 25,
    },
    "gtasks": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "max_tasks": 100,
    },
    "whatsapp": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "fetch_limit": 50,
        "chat_limit": 50,
        "native_enabled": True,
        "native_batch_size": 10000,
        "native_include_contacts": True,
        "native_include_calls": True,
        "native_db_path": "~/Library/Group Containers/group.net.whatsapp.WhatsApp.shared",
    },
    "imessage": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "include_attachments": True,
    },
    "browser": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "session_window_minutes": 30,
    },
    "github_notifications": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/5 * * * *",
        "participating": True,
        "per_page": 50,
    },
    "nextcloud": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
        "server": "https://cloud.example.com",
        "max_results": 200,
        "content_preview": True,
    },
    "gdrive": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
        "accounts": [],
        "max_results": 50,
        "content_preview_size": 5000,
    },
    "calendar": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "email": "",
        "days_back": 7,
        "days_forward": 14,
        "calendar_ids": [],
    },
    "linkedin": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 */4 * * *",  # every 4 hours
        "max_conversations": 20,
    },
    "xnews": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 */2 * * *",  # every 2 hours
    },
    "hlopya": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/30 * * * *",
    },
    "slack": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "token": "${SLACK_TOKEN}",
        "workspace": "",
        "channels": [],
        "types": "public_channel,private_channel,im,mpim",
        "bootstrap_days": 7,
        "page_size": 100,
        "max_channels": 200,
        "include_threads": True,
        "include_file_metadata": True,
    },
    "discord": {
        "enabled": False,
        "mode": "cron",
        "schedule": "*/15 * * * *",
        "token": "${DISCORD_TOKEN}",
        "guild_ids": [],
        "channel_ids": [],
        "bootstrap_days": 7,
        "page_size": 100,
        "max_channels": 200,
        "include_attachments": True,
    },
    "codex": {
        "enabled": False,
        "mode": "cron",
        "schedule": "0 * * * *",
        "codex_dir": "~/.codex",
        "include_archived": True,
        "include_sqlite_metadata": True,
        "compress_long_messages": False,
        "compression_min_chars": 12000,
        "max_user_chars": 8000,
        "max_assistant_chars": 8000,
        "max_tool_output_chars": 1200,
    },
}


# ---- Config write-back ----


def ensure_config_file() -> Path:
    """Create config file if it doesn't exist. Returns path."""
    existing = _find_config_file()
    if existing:
        return existing

    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    config_dir = Path(xdg) / "vadimgest"
    config_file = config_dir / "config.yaml"
    config_dir.mkdir(parents=True, exist_ok=True)

    config_file.write_text(yaml.dump(
        {name: {"enabled": False} for name in _SOURCE_DEFAULTS},
        default_flow_style=False,
    ))
    load_config.cache_clear()
    return config_file


def save_source_config(name: str, updates: dict) -> Path:
    """Update source config in YAML file and clear cache."""
    config_file = ensure_config_file()

    with open(config_file) as f:
        raw = yaml.safe_load(f) or {}

    if name not in raw:
        raw[name] = {}
    raw[name].update(updates)

    with open(config_file, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    load_config.cache_clear()
    return config_file


def save_edge_config(updates: dict) -> Path:
    """Update edge-agent config in YAML file and clear cache."""
    config_file = ensure_config_file()

    allowed = {"enabled", "server_url", "device_id", "interval_seconds", "batch_size", "sources"}
    clean = {k: v for k, v in updates.items() if k in allowed}
    if "server_url" in clean:
        clean["server_url"] = str(clean["server_url"] or "").rstrip("/")
    if "device_id" in clean:
        clean["device_id"] = str(clean["device_id"] or "").strip()
    for key in ("interval_seconds", "batch_size"):
        if key in clean:
            clean[key] = int(clean[key])
    if "sources" in clean and clean["sources"] is not None:
        clean["sources"] = [str(s).strip() for s in clean["sources"] if str(s).strip()]

    with open(config_file) as f:
        raw = yaml.safe_load(f) or {}

    if "edge" not in raw or not isinstance(raw["edge"], dict):
        raw["edge"] = {}
    raw["edge"].update(clean)

    with open(config_file, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    load_config.cache_clear()
    return config_file


def get_search_config() -> dict:
    """Get search index configuration."""
    config = load_config()
    search = config.get("search", {})
    return {
        "vault_path": str(_expand_path(search.get("vault_path")) or (Path.home() / "Documents" / "Notes")),
        "skills_dir": str(_expand_path(search.get("skills_dir")) or (Path.home() / ".claude" / "skills")),
        "index_db": str(_expand_path(search.get("index_db")) or (Path.home() / ".vadimsearch" / "index.db")),
        "embedding_provider": search.get("embedding_provider", ""),
        "exclude_sources": search.get("exclude_sources", []),
        "ollama_url": search.get("ollama_url", "http://localhost:11434"),
        "ollama_model": search.get("ollama_model", "nomic-embed-text"),
    }


def save_search_config(updates: dict) -> Path:
    """Update search config in YAML file."""
    config_file = ensure_config_file()

    with open(config_file) as f:
        raw = yaml.safe_load(f) or {}

    if "search" not in raw:
        raw["search"] = {}
    raw["search"].update(updates)

    with open(config_file, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    load_config.cache_clear()
    return config_file


def get_env_file_path() -> Path:
    """Get path to .env file (in config dir)."""
    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    return Path(xdg) / "vadimgest" / ".env"


def save_env_vars(variables: dict[str, str]):
    """Save environment variables to .env file with restricted permissions."""
    env_file = get_env_file_path()
    env_file.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(env_file.parent, 0o700)

    lines = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()

    for key, value in variables.items():
        found = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = f"{key}={value}"
                found = True
                break
        if not found:
            lines.append(f"{key}={value}")
        os.environ[key] = value

    env_file.write_text("\n".join(lines) + "\n")
    os.chmod(env_file, 0o600)


def get_env_status(keys: list[str]) -> dict[str, bool]:
    """Check which env vars are set."""
    return {k: bool(os.environ.get(k)) for k in keys}


# ---- Backward compatibility ----
# Module-level attributes for existing code that imports DATA_DIR, SOURCES, etc.

_load_dotenv()

# Eagerly set these for backward compat (existing code does `from .config import DATA_DIR`)
BASE_DIR = _PACKAGE_DIR
DATA_DIR = get_data_dir()
CREDENTIALS_DIR = get_credentials_dir()
SOURCES_DIR = get_sources_dir()
SOURCES = get_all_source_configs()
CONVERSATION_SETTINGS = get_conversation_settings()
