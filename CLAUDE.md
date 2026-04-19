# vadimgest - Claude Code Instructions

Personal data ETL with 19 sources, append-only JSONL storage, and FTS5 search.

## Quick Reference

```bash
# Run tests
python3 -m pytest tests/ -x -q

# Run specific test module
python3 -m pytest tests/test_web.py -x -q
python3 -m pytest tests/test_cli.py -x -q

# Start dashboard
python3 -m vadimgest serve --port 8484

# CLI
vadimgest sync                       # sync all enabled sources
vadimgest sync telegram signal       # sync specific
vadimgest list                       # show available sources
vadimgest stats                      # record counts
vadimgest health                     # source health
vadimgest search "query" --md --raw  # search everything
```

## Architecture

Single-package Python project. No microservices, no Docker.

```
vadimgest/
  cli.py          # CLI entry (click)
  config.py       # YAML config loader with lru_cache
  store.py        # DataStore - append-only JSONL + checkpoints
  daemon.py       # Background sync daemon
  models.py       # Dataclasses
  search/         # FTS5 search (SQLite)
  consumer/       # Checkpoint-based consumption API
  ingest/sources/ # 19 source syncers (telegram/, signal/, gmail/, etc.)
  web/app.py      # Flask dashboard - single file, ALL HTML/CSS/JS inline
```

### Key Design Decisions

- **Single-file dashboard**: `web/app.py` contains ALL HTML, CSS, and JavaScript inline in `_render_dashboard()`. No build step, no webpack, no npm. One Python file = full dashboard.
- **Append-only JSONL**: Never modify or delete records. Each source gets one `.jsonl` file in `data/sources/`.
- **FTS5 search**: SQLite full-text search with incremental indexing. First search auto-builds index.
- **Config with lru_cache**: `load_config()` is cached. Call `load_config.cache_clear()` if you modify config at runtime.
- **Source syncers**: Each source extends `CronSyncer` or `DaemonSyncer` from `ingest/sources/base.py`. Implement `fetch_new()` to yield records.

## Testing

1268 tests across multiple modules. Always run tests before committing:

```bash
python3 -m pytest tests/ -x -q           # all tests
python3 -m pytest tests/test_web.py -x   # 76 web/dashboard tests
python3 -m pytest tests/test_cli.py -x   # CLI tests
```

Tests use fixtures from `conftest.py` that create isolated temp directories.

## Dashboard Development

The dashboard is a single-page app rendered entirely from Python strings in `web/app.py`:

- **CSS**: Lines ~770-1824 - CSS custom properties, zinc-based dark/light theme
- **HTML**: Lines ~1825-1920 - header, tabs, content containers, drawer
- **JavaScript**: Lines ~1920+ - all rendering functions, API calls, state management

CSS uses variables matching the Klava dashboard zinc palette:
- `--bg: #09090b` / `--bg2: #18181b` / `--bg3: #27272a` (depth hierarchy)
- `--accent: #34d399` (green), `--accent2: #a78bfa` (purple for data bars)
- Cards use `src-card` class with glowing status dots and uppercase badges

Key JS functions:
- `renderDashboard()` - main tab with KPIs, source grid, health, activity
- `renderSourcesPage()` - detailed source management
- `renderDocsPage()` - documentation and CLI reference
- `openDrawer(name)` - source detail/config drawer

After modifying `app.py`, restart the Flask server to see changes (no hot reload).

## Adding a New Source

See the `/add-source` skill or:

1. Create `ingest/sources/newsource/syncer.py` extending `CronSyncer`
2. Implement `fetch_new(state, limit)` yielding dicts with `id`, `type`, and data fields
3. Register in `ingest/sources/__init__.py`
4. Add config defaults in `config.py`
5. Add to dashboard `SOURCE_ICONS` map in `web/app.py` JS section
6. Search indexer picks up new JSONL sources automatically

## Conventions

- No comments unless the WHY is non-obvious
- Tests required for new features
- CSS: use var() references, never hardcode colors
- JS in app.py: use double-escaped unicode for emoji (`\\uD83D\\uDD12` not `\uD83D\uDD12`) because Python string -> JS interpretation
