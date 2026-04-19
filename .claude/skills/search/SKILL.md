---
user_invocable: true
name: search
description: Search all personal data sources via vadimgest unified search
---

# vadimgest Search

Search across all ingested personal data - messages, meetings, notes, emails, activity, tasks.

## When to Use

- Finding past conversations, decisions, agreements
- Looking up what was discussed with a specific person
- Searching across messaging platforms (Telegram, Signal, WhatsApp, iMessage)
- Finding meeting notes or transcripts
- Checking email history
- Any "did we discuss X?" or "when did Y happen?" question

## Commands

```bash
# Basic search (specify at least one scope)
vadimgest search "query" --md          # Obsidian vault only
vadimgest search "query" --raw         # all JSONL sources
vadimgest search "query" --md --raw    # everything

# Source-specific
vadimgest search "query" -s telegram
vadimgest search "query" -s signal
vadimgest search "query" -s gmail
vadimgest search "query" -s obsidian

# Filter by chat/person
vadimgest search "contract" --raw --chat "Alice"

# Filter by folder
vadimgest search "deal" --md --folder "Deals"

# More results
vadimgest search "query" --raw -n 20

# Full content (not snippets)
vadimgest search "query" --raw --full

# JSON output for programmatic use
vadimgest search "query" --raw --json
```

## Search Syntax

Uses SQLite FTS5. Supports:

- Simple words: `meeting friday`
- Phrases: `"board meeting"`
- Boolean: `contract AND alice`
- Prefix: `crypto*`
- Exclusion: `meeting NOT cancelled`

## Consumer API (for pipelines)

```bash
# Read new records since last checkpoint
vadimgest read -c my-pipeline

# Read from specific source only
vadimgest read -c my-pipeline -s telegram

# Stats only (don't output records)
vadimgest read -c my-pipeline --stats

# Commit checkpoint after processing
vadimgest commit -c my-pipeline
```

## Tips

- Always cite the source in your response: `[Source: telegram]`
- Use `--chat` filter when you know the person/group name
- Use `-s` filter when you know which platform
- Use `--full` when snippets aren't enough context
- The index auto-builds on first search. Rebuild with `vadimgest search index --rebuild`
