---
user_invocable: true
name: add-source
description: Guide for adding a new data source syncer to vadimgest
---

# Adding a New Source to vadimgest

Step-by-step guide for creating a new data source syncer.

## 1. Create the Syncer

Create `vadimgest/ingest/sources/{name}/syncer.py`:

```python
from ..base import CronSyncer  # or DaemonSyncer for realtime

class MySourceSyncer(CronSyncer):
    source_name = "mysource"
    display_name = "My Source"
    description = "What this source ingests"
    category = "messaging"  # messaging|email|calendar|dev|files|activity|meetings|social|knowledge

    # Dependencies the source needs
    python_deps = ["some-package"]   # pip packages
    cli_deps = ["some-cli"]          # CLI tools
    credential_deps = ["API_KEY"]    # env vars
    os_deps = ["macOS"]              # OS requirements

    def fetch_new(self, state, limit=1000):
        """Yield new records as dicts. Called by sync daemon/cron.

        Args:
            state: dict with last sync state (you manage the cursor)
            limit: max records per batch

        Yields:
            dict with at minimum: id, type, and source-specific fields
        """
        last_id = state.get("last_id", 0)

        for item in self._read_from_source(since=last_id, limit=limit):
            yield {
                "id": str(item.id),
                "type": "message",  # conversation|message|meeting|email|task|document|activity
                "chat": item.chat_name,
                "sender": item.sender,
                "text": item.text,
                "date": item.date.isoformat(),
            }
            last_id = item.id

        state["last_id"] = last_id
```

### Syncer Types

- **CronSyncer**: Runs periodically (every N seconds). Best for: APIs, polling sources.
- **DaemonSyncer**: Runs continuously in background. Best for: realtime streams, file watchers.

### Record Types

| Type | Expected Fields |
|------|----------------|
| `conversation` | `chat`, `folder`, `messages[]` (each with `sender`, `text`, `date`) |
| `message` | `chat`, `sender`, `text`, `date` |
| `meeting` | `title`, `participants`, `notes`, `transcript` |
| `email` | `subject`, `from`, `account`, `body`, `date` |
| `task` | `title`, `list_name`, `due`, `notes`, `status` |
| `document` | `title`, `path`, `content` |
| `activity` | `title`, `category`, `duration_seconds` |

## 2. Create `__init__.py`

Create `vadimgest/ingest/sources/{name}/__init__.py`:

```python
from .syncer import MySourceSyncer

__all__ = ["MySourceSyncer"]
```

## 3. Register the Source

In `vadimgest/ingest/sources/__init__.py`, add to the registry:

```python
from .mysource import MySourceSyncer
# Add to SYNCERS dict and all_source_names()
```

## 4. Add Config Defaults

In `vadimgest/config.py`, add default config:

```python
# In DEFAULT_CONFIG or the config template
"mysource": {
    "enabled": False,
    # source-specific settings
}
```

## 5. Add Dashboard Icon

In `vadimgest/web/app.py`, find the `SOURCE_ICONS` JavaScript object and add:

```javascript
mysource: '\\uD83D\\uDCE6',  // pick an appropriate emoji
```

Note: use double-escaped unicode for emojis in the Python string.

## 6. Search Integration

The search indexer automatically picks up new JSONL sources. For custom text extraction, add a case to `_extract_jsonl_text()` in `vadimgest/search/indexer.py`.

## 7. Test

```bash
# Sync the new source
vadimgest sync mysource

# Check it appears
vadimgest list
vadimgest stats

# Search it
vadimgest search "test" -s mysource

# Run tests
python3 -m pytest tests/ -x -q
```

## Checklist

- [ ] Syncer class with `fetch_new()`
- [ ] `__init__.py` with export
- [ ] Registered in sources `__init__.py`
- [ ] Config defaults added
- [ ] Dashboard icon added
- [ ] Tests pass
- [ ] Search finds records from new source
