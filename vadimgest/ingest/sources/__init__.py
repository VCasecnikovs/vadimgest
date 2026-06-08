"""Data source syncers for vadimgest.

Sources are loaded lazily to avoid import errors when
optional dependencies (e.g. telethon) are not installed.

Custom sources are auto-discovered from two locations:
1. Subdirectories of this package (for built-in/dev sources)
2. External directory via VADIMGEST_SOURCES_DIR env var or
   custom_sources_dir in config.yaml (for user plugins)

Drop a folder with a syncer.py containing a BaseSyncer subclass
and it will be picked up automatically.
"""

import importlib
import importlib.util
import inspect
import os
import warnings
from pathlib import Path
from typing import Type

from .base import BaseSyncer, CronSyncer

__all__ = [
    "BaseSyncer",
    "CronSyncer",
    "SYNCERS",
    "get_syncer_class",
    "available_sources",
    "get_all_manifests",
]

# Registry: source name -> (module_path, class_name)
_SYNCER_REGISTRY = {
    "telegram": (".telegram.syncer", "TelegramSyncer"),
    "signal": (".signal.syncer", "SignalSyncer"),
    "granola": (".granola.syncer", "GranolaSyncer"),
    "dayflow": (".dayflow.syncer", "DayflowSyncer"),
    "obsidian": (".obsidian.syncer", "ObsidianSyncer"),
    "claude": (".claude.syncer", "ClaudeSyncer"),
    "github": (".github.syncer", "GitHubSyncer"),
    "gmail": (".gmail.syncer", "GmailSyncer"),
    "gtasks": (".gtasks.syncer", "GTasksSyncer"),
    "whatsapp": (".whatsapp.syncer", "WhatsAppSyncer"),
    "imessage": (".imessage.syncer", "IMessageSyncer"),
    "browser": (".browser.syncer", "BrowserSyncer"),
    "github_notifications": (".github_notifications.syncer", "GitHubNotificationsSyncer"),
    "nextcloud": (".nextcloud.syncer", "NextcloudSyncer"),
    "gdrive": (".gdrive.syncer", "GDriveSyncer"),
    "calendar": (".calendar.syncer", "CalendarSyncer"),
    "linkedin": (".linkedin.syncer", "LinkedInSyncer"),
    "xnews": (".xnews.syncer", "XNewsSyncer"),
    "hlopya": (".hlopya.syncer", "HlopyaSyncer"),
    "slack": (".slack.syncer", "SlackSyncer"),
}

# Cache of loaded syncer classes
_loaded: dict[str, Type[BaseSyncer]] = {}
_failed: dict[str, str] = {}
_discovery_done = False


def _scan_directory(sources_dir: Path, *, external: bool = False):
    """Scan a directory for syncer modules and register them."""
    if not sources_dir.is_dir():
        return
    for child in sorted(sources_dir.iterdir()):
        if not child.is_dir() or child.name.startswith(("_", ".")):
            continue
        if child.name in _SYNCER_REGISTRY:
            continue
        syncer_file = child / "syncer.py"
        if not syncer_file.exists():
            continue

        if external:
            spec = importlib.util.spec_from_file_location(
                f"vadimgest_custom_{child.name}_syncer", str(syncer_file),
            )
            if spec is None or spec.loader is None:
                continue
            try:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            except Exception:
                continue
        else:
            module_path = f".{child.name}.syncer"
            try:
                module = importlib.import_module(module_path, package=__package__)
            except Exception:
                continue

        _register_from_module(module, child.name, syncer_file, external)


def _register_from_module(module, dir_name: str, syncer_file: Path, external: bool):
    """Find and register a BaseSyncer subclass from a loaded module."""
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if (inspect.isclass(obj)
                and issubclass(obj, BaseSyncer)
                and obj is not BaseSyncer
                and obj is not CronSyncer
                and hasattr(obj, "source_name")
                and obj.source_name):
            if external:
                _SYNCER_REGISTRY[obj.source_name] = ("_external", str(syncer_file))
            else:
                _SYNCER_REGISTRY[obj.source_name] = (f".{dir_name}.syncer", attr_name)
            _loaded[obj.source_name] = obj
            break


def _get_custom_sources_dir() -> Path | None:
    """Get external custom sources directory from env or config."""
    env = os.environ.get("VADIMGEST_SOURCES_DIR")
    if env:
        p = Path(env).expanduser()
        if p.is_dir():
            return p

    try:
        from ...config import load_config
        cfg = load_config()
        custom = cfg.get("custom_sources_dir")
        if custom:
            p = Path(os.path.expandvars(os.path.expanduser(str(custom))))
            if p.is_dir():
                return p
    except Exception:
        pass

    return None


def _discover_custom_sources():
    """Scan for custom syncer modules in package dir and external dir."""
    global _discovery_done
    if _discovery_done:
        return
    _discovery_done = True

    _scan_directory(Path(__file__).parent, external=False)

    custom_dir = _get_custom_sources_dir()
    if custom_dir:
        _scan_directory(custom_dir, external=True)


def get_syncer_class(name: str) -> Type[BaseSyncer] | None:
    """Load and return a syncer class by name.

    Returns None if the source is unknown or dependencies are missing.
    """
    if name in _loaded:
        return _loaded[name]

    if name in _failed:
        return None

    _discover_custom_sources()

    if name not in _SYNCER_REGISTRY:
        return None

    entry = _SYNCER_REGISTRY[name]
    module_path, class_name = entry

    if module_path == "_external":
        _failed[name] = "external source failed to load during discovery"
        return None

    try:
        module = importlib.import_module(module_path, package=__package__)
        cls = getattr(module, class_name)
        _loaded[name] = cls
        return cls
    except ImportError as e:
        _failed[name] = str(e)
        return None
    except Exception as e:
        _failed[name] = str(e)
        warnings.warn(f"Failed to load source '{name}': {e}", stacklevel=2)
        return None


def get_load_error(name: str) -> str | None:
    """Get the error message if a source failed to load."""
    if name in _failed:
        return _failed[name]
    # Try loading to populate _failed
    get_syncer_class(name)
    return _failed.get(name)


def available_sources() -> list[str]:
    """Return list of source names that can be loaded."""
    return [name for name in _SYNCER_REGISTRY if get_syncer_class(name) is not None]


def all_source_names() -> list[str]:
    """Return all registered source names (whether loadable or not)."""
    _discover_custom_sources()
    return list(_SYNCER_REGISTRY.keys())


class _LazySyncers(dict):
    """Dict that loads syncer classes on first access."""

    def __getitem__(self, key):
        if key not in dict.keys(self):
            cls = get_syncer_class(key)
            if cls is None:
                error = _failed.get(key, "unknown source")
                raise KeyError(f"Source '{key}' unavailable: {error}")
            self[key] = cls
        return super().__getitem__(key)

    def __contains__(self, key):
        if super().__contains__(key):
            return True
        return key in _SYNCER_REGISTRY and get_syncer_class(key) is not None

    def keys(self):
        return _SYNCER_REGISTRY.keys()

    def items(self):
        for k in self.keys():
            try:
                yield k, self[k]
            except KeyError:
                pass

    def values(self):
        for k in self.keys():
            try:
                yield self[k]
            except KeyError:
                pass

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(_SYNCER_REGISTRY)


SYNCERS = _LazySyncers()


# --- Manifest metadata registry (for sources that can't be imported) ---

_MANIFEST_FALLBACKS: dict[str, dict] = {
    # Fallback manifest data for sources whose deps aren't installed.
    # Only used when the syncer class can't be imported.
}


def get_all_manifests() -> dict[str, dict]:
    """Return manifest metadata for all registered sources.

    Works even when source dependencies are missing - returns
    loadable=False with fallback metadata in that case.
    """
    from ...config import get_source_config

    result = {}
    for name in _SYNCER_REGISTRY:
        cls = get_syncer_class(name)
        if cls is not None:
            # Source loadable - get manifest from class
            try:
                source_cfg = get_source_config(name)
            except Exception:
                source_cfg = {}

            try:
                ready = cls.check_ready()
            except Exception as e:
                ready = {"ok": False, "missing": [f"check_ready() error: {e}"]}

            result[name] = {
                "display_name": cls.display_name,
                "description": cls.description,
                "category": cls.category,
                "dependencies": cls.dependencies,
                "config_schema": cls.config_schema,
                "loadable": True,
                "enabled": source_cfg.get("enabled", False),
                "ready": ready,
            }
        else:
            # Source not loadable - use fallback or empty
            fallback = _MANIFEST_FALLBACKS.get(name, {})
            try:
                source_cfg = get_source_config(name)
            except Exception:
                source_cfg = {}

            result[name] = {
                "display_name": fallback.get("display_name", name.title()),
                "description": fallback.get("description", f"{name} source (dependencies not installed)"),
                "category": fallback.get("category", ""),
                "dependencies": fallback.get("dependencies", {"python": [], "cli": [], "credentials": [], "os": []}),
                "config_schema": fallback.get("config_schema", {}),
                "loadable": False,
                "enabled": source_cfg.get("enabled", False),
                "ready": None,
                "load_error": get_load_error(name),
            }

    return result
