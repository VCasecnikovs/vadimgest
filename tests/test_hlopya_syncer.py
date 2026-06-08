"""Tests for Hlopya recording ingestion."""

import json

from vadimgest.ingest.sources.hlopya.syncer import HlopyaSyncer
from vadimgest.store import DataStore


def _write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def test_hlopya_ingests_transcribed_session_without_notes(tmp_path):
    recordings = tmp_path / "recordings"
    session = recordings / "2026-05-18_11-00-53"
    session.mkdir(parents=True)
    _write_json(session / "meta.json", {"status": "transcribed", "duration": 0})
    _write_json(
        session / "transcript.json",
        {
            "duration_seconds": 0,
            "full_text": "**Them** [0.5s]: Да, ну и меня слышно.",
            "segments": [
                {"start": 0.5, "end": 4.0, "text": "Да, ну и меня слышно."},
                {"start": 2700.0, "end": 2754.6, "text": "Заключительный фрагмент."},
            ],
        },
    )

    store = DataStore(tmp_path / "data")
    syncer = HlopyaSyncer(store, {"recordings_dir": str(recordings)})

    count, summary = syncer.sync()

    assert count == 1
    assert summary == ["2026-05-18_11-00-53"]
    rows = list(store.read_all("hlopya"))
    assert rows[0].data["id"] == "hlopya_2026-05-18_11-00-53"
    assert rows[0].data["duration_minutes"] == 46
    assert rows[0].data["meta"]["has_transcript"] is True


def test_hlopya_skips_recorded_session_without_transcript(tmp_path):
    recordings = tmp_path / "recordings"
    session = recordings / "2026-05-18_11-00-53"
    session.mkdir(parents=True)
    _write_json(session / "meta.json", {"status": "recorded"})

    store = DataStore(tmp_path / "data")
    syncer = HlopyaSyncer(store, {"recordings_dir": str(recordings)})

    count, _ = syncer.sync()

    assert count == 0
