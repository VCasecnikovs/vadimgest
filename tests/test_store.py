"""Comprehensive tests for vadimgest DataStore."""

import json
import os
from pathlib import Path

import pytest

from vadimgest.store import DataStore
from vadimgest.models import Record, SourceState, ConsumerCheckpoint


@pytest.fixture
def store(tmp_path):
    """Create a DataStore in a temp directory."""
    return DataStore(tmp_path / "data")


# ── __init__ ──


class TestInit:
    def test_creates_directories(self, tmp_path):
        base = tmp_path / "fresh"
        store = DataStore(base)
        assert store.sources_dir.exists()
        assert store.checkpoints_dir.exists()

    def test_sets_paths(self, tmp_path):
        base = tmp_path / "store"
        store = DataStore(base)
        assert store.base_path == base
        assert store.sources_dir == base / "sources"
        assert store.checkpoints_dir == base / "checkpoints"
        assert store.state_file == base / "state.json"

    def test_idempotent_creation(self, tmp_path):
        base = tmp_path / "store"
        DataStore(base)
        DataStore(base)  # should not raise

    def test_accepts_string_path(self, tmp_path):
        store = DataStore(str(tmp_path / "data"))
        assert store.base_path == tmp_path / "data"


# ── State management ──


class TestState:
    def test_get_state_returns_default_for_unknown_source(self, store):
        state = store.get_state("nonexistent")
        assert state.total_records == 0
        assert state.last_id is None
        assert state.last_ts is None

    def test_set_and_get_state(self, store):
        s = SourceState(last_id="abc", last_ts="2026-01-01", total_records=42)
        store.set_state("telegram", s)
        loaded = store.get_state("telegram")
        assert loaded.last_id == "abc"
        assert loaded.last_ts == "2026-01-01"
        assert loaded.total_records == 42

    def test_set_state_overwrites(self, store):
        store.set_state("src", SourceState(total_records=1))
        store.set_state("src", SourceState(total_records=99))
        assert store.get_state("src").total_records == 99

    def test_multiple_sources_independent(self, store):
        store.set_state("a", SourceState(total_records=10))
        store.set_state("b", SourceState(total_records=20))
        assert store.get_state("a").total_records == 10
        assert store.get_state("b").total_records == 20

    def test_state_persists_to_disk(self, store):
        store.set_state("x", SourceState(last_id="persist"))
        raw = json.loads(store.state_file.read_text())
        assert raw["x"]["last_id"] == "persist"

    def test_load_state_empty_when_no_file(self, store):
        assert store._load_state() == {}

    def test_save_state_atomic_write(self, store):
        """_save_state uses atomic rename, so no partial writes."""
        store._save_state({"test": {"last_id": None, "last_ts": None, "total_records": 5, "extra": {}}})
        assert store.state_file.exists()
        data = json.loads(store.state_file.read_text())
        assert data["test"]["total_records"] == 5

    def test_state_extra_field_preserved(self, store):
        s = SourceState(extra={"cursor": "abc123"})
        store.set_state("src", s)
        loaded = store.get_state("src")
        assert loaded.extra == {"cursor": "abc123"}


# ── Append ──


class TestAppend:
    def test_append_creates_file(self, store):
        store.append("telegram", {"id": "1", "text": "hello"})
        assert (store.sources_dir / "telegram.jsonl").exists()

    def test_append_returns_record(self, store):
        rec = store.append("telegram", {"id": "1", "text": "hello"})
        assert isinstance(rec, Record)
        assert rec._line == 1
        assert rec._source == "telegram"
        assert rec.data["id"] == "1"

    def test_append_increments_line_numbers(self, store):
        r1 = store.append("src", {"id": "a"})
        r2 = store.append("src", {"id": "b"})
        r3 = store.append("src", {"id": "c"})
        assert r1._line == 1
        assert r2._line == 2
        assert r3._line == 3

    def test_append_updates_state_total_records(self, store):
        store.append("src", {"id": "1"})
        store.append("src", {"id": "2"})
        assert store.get_state("src").total_records == 2

    def test_append_updates_last_id(self, store):
        store.append("src", {"id": "first"})
        store.append("src", {"id": "second"})
        assert store.get_state("src").last_id == "second"

    def test_append_updates_last_ts_from_timestamp(self, store):
        store.append("src", {"id": "1", "timestamp": "2026-03-01T00:00:00Z"})
        assert store.get_state("src").last_ts == "2026-03-01T00:00:00Z"

    def test_append_updates_last_ts_from_period_end(self, store):
        store.append("src", {"id": "1", "period_end": "2026-03-15"})
        assert store.get_state("src").last_ts == "2026-03-15"

    def test_append_last_ts_only_advances(self, store):
        store.append("src", {"id": "1", "timestamp": "2026-03-10"})
        store.append("src", {"id": "2", "timestamp": "2026-03-05"})
        # last_ts should stay at the later one
        assert store.get_state("src").last_ts == "2026-03-10"

    def test_append_last_ts_advances_across_rfc2822_and_iso(self, store):
        # Regression: Gmail emits mixed date formats. RFC 2822 "Wed, 8 Apr..."
        # used to beat any ISO string in lex compare, freezing last_ts.
        store.append("src", {"id": "1", "date": "Wed, 8 Apr 2026 20:10:08 -0700"})
        store.append("src", {"id": "2", "date": "2026-04-19 22:15"})
        assert store.get_state("src").last_ts == "2026-04-19 22:15"

    def test_append_last_ts_does_not_regress_with_rfc2822(self, store):
        store.append("src", {"id": "1", "date": "Sun, 19 Apr 2026 22:15:00 +0000"})
        store.append("src", {"id": "2", "date": "Wed, 8 Apr 2026 20:10:08 -0700"})
        # Earlier RFC date must not overwrite the later one
        assert store.get_state("src").last_ts == "Sun, 19 Apr 2026 22:15:00 +0000"

    def test_append_last_ts_advances_with_twitter_format(self, store):
        # Regression: xnews ingests Twitter's "Wed Apr 29 13:28:24 +0000 2026"
        # format. Lex compare put "Wed Mar 25..." above any "Wed Apr/Feb/Jan..."
        # because "M" > "A"/"F"/"J", so last_ts stuck on Mar 25 even after
        # newer Apr tweets arrived. (2026-04-29)
        store.append("src", {"id": "1", "ts": "Wed Mar 25 23:59:26 +0000 2026"})
        store.append("src", {"id": "2", "ts": "Wed Apr 29 13:28:24 +0000 2026"})
        assert store.get_state("src").last_ts == "Wed Apr 29 13:28:24 +0000 2026"

    def test_append_last_ts_does_not_regress_with_twitter_format(self, store):
        store.append("src", {"id": "1", "ts": "Wed Apr 29 13:28:24 +0000 2026"})
        store.append("src", {"id": "2", "ts": "Wed Mar 25 23:59:26 +0000 2026"})
        assert store.get_state("src").last_ts == "Wed Apr 29 13:28:24 +0000 2026"

    def test_append_no_id_field(self, store):
        rec = store.append("src", {"text": "no id"})
        assert rec._line == 1
        assert store.get_state("src").last_id is None

    def test_append_writes_valid_jsonl(self, store):
        store.append("src", {"id": "1", "text": "hello"})
        raw = (store.sources_dir / "src.jsonl").read_text().strip()
        data = json.loads(raw)
        assert data["_line"] == 1
        assert data["_source"] == "src"
        assert data["id"] == "1"
        assert data["text"] == "hello"
        assert "_ingested_at" in data

    def test_append_unicode_data(self, store):
        store.append("src", {"id": "1", "text": "privet mir"})
        records = list(store.read_all("src"))
        assert records[0].data["text"] == "privet mir"

    def test_append_updates_id_cache(self, store):
        # Prime the cache
        store.exists("src", "nonexistent")
        store.append("src", {"id": "new_record"})
        assert store.exists("src", "new_record")


# ── append_batch ──


class TestAppendBatch:
    def test_append_batch_multiple_records(self, store):
        count = store.append_batch("src", [
            {"id": "a", "text": "one"},
            {"id": "b", "text": "two"},
            {"id": "c", "text": "three"},
        ])
        assert count == 3
        assert store.count("src") == 3

    def test_append_batch_empty(self, store):
        count = store.append_batch("src", [])
        assert count == 0

    def test_append_batch_returns_count(self, store):
        count = store.append_batch("src", [{"id": "x"}])
        assert count == 1


# ── read_all ──


class TestReadAll:
    def test_read_all_empty_source(self, store):
        records = list(store.read_all("nonexistent"))
        assert records == []

    def test_read_all_returns_records(self, store):
        store.append("src", {"id": "1", "text": "hello"})
        store.append("src", {"id": "2", "text": "world"})
        records = list(store.read_all("src"))
        assert len(records) == 2
        assert records[0].data["id"] == "1"
        assert records[1].data["id"] == "2"

    def test_read_all_preserves_order(self, store):
        for i in range(5):
            store.append("src", {"id": str(i)})
        records = list(store.read_all("src"))
        ids = [r.data["id"] for r in records]
        assert ids == ["0", "1", "2", "3", "4"]

    def test_read_all_skips_blank_lines(self, store):
        store.append("src", {"id": "1"})
        # Inject a blank line manually
        with open(store.sources_dir / "src.jsonl", "a") as f:
            f.write("\n")
        store.append("src", {"id": "2"})
        records = list(store.read_all("src"))
        assert len(records) == 2


# ── read_from ──


class TestReadFrom:
    def test_read_from_line_1(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        store.append("src", {"id": "c"})
        records = list(store.read_from("src", 1))
        assert len(records) == 3

    def test_read_from_middle(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        store.append("src", {"id": "c"})
        records = list(store.read_from("src", 2))
        assert len(records) == 2
        assert records[0].data["id"] == "b"

    def test_read_from_last_line(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        records = list(store.read_from("src", 2))
        assert len(records) == 1
        assert records[0].data["id"] == "b"

    def test_read_from_beyond_end(self, store):
        store.append("src", {"id": "a"})
        records = list(store.read_from("src", 100))
        assert records == []

    def test_read_from_nonexistent_source(self, store):
        records = list(store.read_from("nope", 1))
        assert records == []


# ── read_range ──


class TestReadRange:
    def test_read_full_range(self, store):
        for i in range(5):
            store.append("src", {"id": str(i)})
        records = list(store.read_range("src", 1, 5))
        assert len(records) == 5
        assert records[0].data["id"] == "0"
        assert records[4].data["id"] == "4"

    def test_read_middle_range(self, store):
        for i in range(5):
            store.append("src", {"id": str(i)})
        records = list(store.read_range("src", 2, 4))
        assert len(records) == 3
        assert [r.data["id"] for r in records] == ["1", "2", "3"]

    def test_read_single_line(self, store):
        for i in range(3):
            store.append("src", {"id": str(i)})
        records = list(store.read_range("src", 2, 2))
        assert len(records) == 1
        assert records[0].data["id"] == "1"

    def test_range_beyond_end(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        records = list(store.read_range("src", 1, 100))
        assert len(records) == 2

    def test_range_start_beyond_end(self, store):
        store.append("src", {"id": "a"})
        records = list(store.read_range("src", 50, 100))
        assert records == []

    def test_range_nonexistent_source(self, store):
        records = list(store.read_range("nope", 1, 10))
        assert records == []

    def test_range_stops_early(self, store):
        """Verify read_range breaks out of the loop at end_line, not scanning whole file."""
        for i in range(100):
            store.append("src", {"id": str(i)})
        records = list(store.read_range("src", 1, 3))
        assert len(records) == 3
        assert records[2].data["id"] == "2"


# ── count ──


class TestCount:
    def test_count_zero_for_unknown_source(self, store):
        assert store.count("nonexistent") == 0

    def test_count_after_appends(self, store):
        store.append("src", {"id": "1"})
        store.append("src", {"id": "2"})
        store.append("src", {"id": "3"})
        assert store.count("src") == 3


# ── Consumer: read_new / get_checkpoint / commit ──


class TestConsumer:
    def test_get_checkpoint_returns_empty_for_new_consumer(self, store):
        cp = store.get_checkpoint("test_consumer")
        assert cp.consumer == "test_consumer"
        assert cp.positions == {}

    def test_commit_creates_checkpoint_file(self, store):
        store.append("src", {"id": "1"})
        store.commit("src", "consumer1")
        assert (store.checkpoints_dir / "consumer1.json").exists()

    def test_commit_advances_to_current_count(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        store.commit("src", "consumer1")
        cp = store.get_checkpoint("consumer1")
        assert cp.positions["src"]["line"] == 2

    def test_commit_with_explicit_line(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        store.commit("src", "consumer1", line=1)
        cp = store.get_checkpoint("consumer1")
        assert cp.positions["src"]["line"] == 1

    def test_read_new_returns_all_for_fresh_consumer(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        records = list(store.read_new("src", "consumer1"))
        assert len(records) == 2

    def test_read_new_returns_only_new_after_commit(self, store):
        store.append("src", {"id": "a"})
        store.commit("src", "consumer1")
        store.append("src", {"id": "b"})
        store.append("src", {"id": "c"})
        records = list(store.read_new("src", "consumer1"))
        assert len(records) == 2
        assert records[0].data["id"] == "b"
        assert records[1].data["id"] == "c"

    def test_read_new_returns_empty_when_caught_up(self, store):
        store.append("src", {"id": "a"})
        store.commit("src", "consumer1")
        records = list(store.read_new("src", "consumer1"))
        assert records == []

    def test_read_new_nonexistent_source(self, store):
        records = list(store.read_new("nope", "consumer1"))
        assert records == []

    def test_multiple_consumers_independent(self, store):
        store.append("src", {"id": "a"})
        store.append("src", {"id": "b"})
        store.commit("src", "c1", line=1)
        # c2 has no checkpoint, should see all
        records_c2 = list(store.read_new("src", "c2"))
        assert len(records_c2) == 2
        # c1 committed at line 1, should see only line 2
        records_c1 = list(store.read_new("src", "c1"))
        assert len(records_c1) == 1
        assert records_c1[0].data["id"] == "b"

    def test_commit_sets_updated_at(self, store):
        store.append("src", {"id": "a"})
        store.commit("src", "consumer1")
        cp = store.get_checkpoint("consumer1")
        assert cp.updated_at != ""

    def test_commit_preserves_other_source_positions(self, store):
        store.append("src_a", {"id": "1"})
        store.append("src_b", {"id": "2"})
        store.commit("src_a", "consumer1")
        store.commit("src_b", "consumer1")
        cp = store.get_checkpoint("consumer1")
        assert "src_a" in cp.positions
        assert "src_b" in cp.positions


# ── commit_all ──


class TestCommitAll:
    def test_commit_all_advances_all_sources(self, store):
        store.append("telegram", {"id": "t1"})
        store.append("telegram", {"id": "t2"})
        store.append("signal", {"id": "s1"})
        store.commit_all("consumer1")
        cp = store.get_checkpoint("consumer1")
        assert cp.positions["telegram"]["line"] == 2
        assert cp.positions["signal"]["line"] == 1

    def test_commit_all_then_read_new_empty(self, store):
        store.append("src", {"id": "a"})
        store.commit_all("consumer1")
        records = list(store.read_new("src", "consumer1"))
        assert records == []


# ── sources ──


class TestSources:
    def test_sources_empty(self, store):
        assert store.sources() == []

    def test_sources_lists_all(self, store):
        store.append("telegram", {"id": "1"})
        store.append("signal", {"id": "2"})
        result = store.sources()
        assert set(result) == {"telegram", "signal"}

    def test_sources_does_not_include_lock_files(self, store):
        store.append("src", {"id": "1"})
        # Lock file should exist but not appear in sources
        result = store.sources()
        assert "src" in result
        assert len(result) == 1


# ── exists / ID cache ──


class TestExists:
    def test_exists_false_for_empty_source(self, store):
        assert store.exists("src", "missing") is False

    def test_exists_true_after_append(self, store):
        store.append("src", {"id": "abc123"})
        assert store.exists("src", "abc123") is True

    def test_exists_false_for_different_id(self, store):
        store.append("src", {"id": "abc"})
        assert store.exists("src", "xyz") is False

    def test_exists_no_id_field(self, store):
        store.append("src", {"text": "no id"})
        assert store.exists("src", "anything") is False

    def test_invalidate_id_cache(self, store):
        store.append("src", {"id": "abc"})
        # Prime cache
        assert store.exists("src", "abc") is True
        # Invalidate
        store._invalidate_id_cache("src")
        # Should rebuild from file and still find it
        assert store.exists("src", "abc") is True

    def test_invalidate_cache_for_nonexistent_source(self, store):
        # Should not raise
        store._invalidate_id_cache("nonexistent")


# ── stats ──


class TestStats:
    def test_stats_empty(self, store):
        assert store.stats() == {}

    def test_stats_after_appends(self, store):
        store.append("telegram", {"id": "t1", "timestamp": "2026-01-01"})
        store.append("telegram", {"id": "t2", "timestamp": "2026-01-02"})
        store.append("signal", {"id": "s1", "timestamp": "2026-02-01"})
        result = store.stats()
        assert "telegram" in result
        assert "signal" in result
        assert result["telegram"]["records"] == 2
        assert result["telegram"]["last_id"] == "t2"
        assert result["telegram"]["last_ts"] == "2026-01-02"
        assert result["signal"]["records"] == 1


# ── Edge cases ──


class TestEdgeCases:
    def test_append_with_nested_data(self, store):
        data = {"id": "1", "meta": {"nested": {"deep": True}}, "tags": ["a", "b"]}
        store.append("src", data)
        records = list(store.read_all("src"))
        assert records[0].data["meta"]["nested"]["deep"] is True
        assert records[0].data["tags"] == ["a", "b"]

    def test_append_with_empty_dict(self, store):
        rec = store.append("src", {})
        assert rec._line == 1
        assert store.get_state("src").last_id is None

    def test_large_batch(self, store):
        records = [{"id": str(i), "text": f"record {i}"} for i in range(100)]
        count = store.append_batch("src", records)
        assert count == 100
        assert store.count("src") == 100
        all_records = list(store.read_all("src"))
        assert len(all_records) == 100

    def test_multiple_ts_fields_priority(self, store):
        """period_end takes priority over timestamp."""
        store.append("src", {
            "id": "1",
            "period_end": "2026-12-31",
            "timestamp": "2026-01-01",
        })
        assert store.get_state("src").last_ts == "2026-12-31"

    def test_ts_from_date_field(self, store):
        store.append("src", {"id": "1", "date": "2026-06-15"})
        assert store.get_state("src").last_ts == "2026-06-15"

    def test_ts_from_updated_at(self, store):
        store.append("src", {"id": "1", "updated_at": "2026-07-01"})
        assert store.get_state("src").last_ts == "2026-07-01"

    def test_ts_from_ended_at(self, store):
        store.append("src", {"id": "1", "ended_at": "2026-08-01"})
        assert store.get_state("src").last_ts == "2026-08-01"

    def test_ts_from_modified_at(self, store):
        store.append("src", {"id": "1", "modified_at": "2026-09-01"})
        assert store.get_state("src").last_ts == "2026-09-01"

    def test_record_roundtrip(self, store):
        """Record survives write -> read cycle."""
        original_data = {"id": "rt1", "text": "roundtrip", "count": 42}
        store.append("src", original_data)
        records = list(store.read_all("src"))
        assert len(records) == 1
        r = records[0]
        assert r.data["id"] == "rt1"
        assert r.data["text"] == "roundtrip"
        assert r.data["count"] == 42
        assert r._source == "src"
        assert r._line == 1

    def test_concurrent_sources_dont_interfere(self, store):
        """Appending to different sources doesn't mix data."""
        store.append("a", {"id": "a1"})
        store.append("b", {"id": "b1"})
        store.append("a", {"id": "a2"})
        a_records = list(store.read_all("a"))
        b_records = list(store.read_all("b"))
        assert len(a_records) == 2
        assert len(b_records) == 1
        assert a_records[0].data["id"] == "a1"
        assert b_records[0].data["id"] == "b1"

    def test_consumer_workflow_full_cycle(self, store):
        """Full workflow: append -> read_new -> commit -> read_new returns empty -> append more -> read_new."""
        # Initial data
        store.append("src", {"id": "1"})
        store.append("src", {"id": "2"})

        # Consumer reads all
        new = list(store.read_new("src", "worker"))
        assert len(new) == 2

        # Commit
        store.commit("src", "worker")

        # Nothing new
        new = list(store.read_new("src", "worker"))
        assert len(new) == 0

        # More data arrives
        store.append("src", {"id": "3"})

        # Consumer sees only the new one
        new = list(store.read_new("src", "worker"))
        assert len(new) == 1
        assert new[0].data["id"] == "3"
