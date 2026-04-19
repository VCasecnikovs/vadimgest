"""
vadimgest - Personal data ETL pipeline.

Two layers:
  vadimgest.ingest   - Sources -> JSONL (atomic fetcher)
  vadimgest.consumer - Read JSONL via checkpoints
"""

from .store import DataStore
from .models import Record, SourceState, ConsumerCheckpoint

__version__ = "0.2.0"
__all__ = ["DataStore", "Record", "SourceState", "ConsumerCheckpoint"]
