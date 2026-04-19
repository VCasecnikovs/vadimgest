"""vadimgest daemon - background sync scheduler.

Replaces external CRON dependency. Runs sync loops on configurable intervals
per source, with error backoff and graceful shutdown.
"""

import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from .store import DataStore
from .config import get_data_dir, get_source_config
from .ingest.sources import get_syncer_class, get_load_error, all_source_names


class SyncDaemon:
    def __init__(self, store: DataStore, interval: int = 300, sources: list[str] | None = None):
        self.store = store
        self.interval = interval
        self.sources = sources
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _get_sync_sources(self) -> list[str]:
        if self.sources:
            return self.sources
        result = []
        for name in all_source_names():
            config = get_source_config(name)
            if config.get("enabled", False) and get_syncer_class(name) is not None:
                result.append(name)
        return result

    def _sync_one(self, source: str) -> tuple[int, str | None]:
        cls = get_syncer_class(source)
        if cls is None:
            return 0, get_load_error(source) or "unavailable"

        config = get_source_config(source)
        syncer = cls(self.store, config)

        try:
            count, summary = syncer.sync(limit=10000)
            syncer.log_run("ok", count=count, summary=summary)
            return count, None
        except Exception as e:
            syncer.log_run("error", error=str(e))
            return 0, str(e)

    def _run_cycle(self):
        sources = self._get_sync_sources()
        if not sources:
            return

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] Syncing {len(sources)} sources...", flush=True)

        total = 0
        errors = 0
        for source in sources:
            if self._stop.is_set():
                break
            count, err = self._sync_one(source)
            total += count
            if err:
                errors += 1
                print(f"  {source}: error - {err[:60]}", flush=True)
            elif count > 0:
                print(f"  {source}: +{count}", flush=True)

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] Done: {total} records, {errors} errors. Next in {self.interval}s", flush=True)

    def _loop(self):
        while not self._stop.is_set():
            try:
                self._run_cycle()
            except Exception as e:
                print(f"Cycle error: {e}", flush=True)
            self._stop.wait(self.interval)

    def start(self):
        print(f"vadimgest daemon starting (interval={self.interval}s)")
        print(f"Data: {self.store.base_path}")
        sources = self._get_sync_sources()
        print(f"Sources: {', '.join(sources) if sources else '(none enabled)'}")
        print(f"PID: {os.getpid()}")
        print()

        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30)

    def run_forever(self):
        self.start()
        try:
            while not self._stop.is_set():
                self._stop.wait(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.stop()


def run_daemon(interval: int = 300, sources: list[str] | None = None):
    store = DataStore(get_data_dir())
    daemon = SyncDaemon(store, interval=interval, sources=sources)

    def handle_signal(signum, frame):
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    daemon.run_forever()
