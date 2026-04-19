"""Docker smoke test - validates vadimgest works on a fresh machine.

Runs without any pre-existing config, credentials, or external tools.
Tests the full journey: install → boot → configure → sync → read.
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path


PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
errors = []


def check(name, condition, detail=""):
    if condition:
        print(f"  {PASS} {name}")
    else:
        msg = f"{name}: {detail}" if detail else name
        errors.append(msg)
        print(f"  {FAIL} {name} — {detail}")


def api(path, method="GET", data=None):
    url = f"http://127.0.0.1:9999{path}"
    body = json.dumps(data).encode() if data else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except Exception:
            body = {}
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def main():
    print("\n=== vadimgest smoke test (fresh environment) ===\n")

    # 1. CLI basics
    print("[1] CLI commands")
    r = subprocess.run(["vadimgest", "--help"], capture_output=True, text=True)
    check("vadimgest --help", r.returncode == 0, r.stderr[:100] if r.returncode else "")

    r = subprocess.run(["vadimgest", "stats"], capture_output=True, text=True)
    check("vadimgest stats", r.returncode == 0, r.stderr[:100] if r.returncode else "")

    r = subprocess.run(["vadimgest", "health"], capture_output=True, text=True)
    check("vadimgest health", r.returncode == 0, r.stderr[:100] if r.returncode else "")

    r = subprocess.run(["vadimgest", "list"], capture_output=True, text=True)
    check("vadimgest list", r.returncode == 0 and "telegram" in r.stdout, r.stderr[:100] if r.returncode else "")

    # 2. Data directory created
    print("\n[2] Data directory")
    data_dir = Path.home() / ".local/share/vadimgest"
    check("data dir exists", data_dir.exists(), str(data_dir))
    check("sources dir exists", (data_dir / "sources").exists())

    # 3. Python imports
    print("\n[3] Python imports")
    try:
        from vadimgest.store import DataStore
        check("import DataStore", True)
    except Exception as e:
        check("import DataStore", False, str(e))

    try:
        from vadimgest.ingest.sources import all_source_names, get_syncer_class
        names = all_source_names()
        check(f"all_source_names() = {len(names)} sources", len(names) >= 19, f"got {len(names)}")
    except Exception as e:
        check("import sources", False, str(e))

    try:
        from vadimgest.web.app import create_app
        check("import create_app", True)
    except Exception as e:
        check("import create_app", False, str(e))

    # 4. Start web server
    print("\n[4] Web server")
    server = subprocess.Popen(
        ["vadimgest", "serve", "--host", "0.0.0.0", "--port", "9999"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    server_up = False
    for _ in range(30):
        try:
            with urllib.request.urlopen("http://127.0.0.1:9999/", timeout=2) as resp:
                if resp.status == 200:
                    server_up = True
                    break
        except Exception:
            time.sleep(1)

    if not server_up:
        status, _ = api("/")
        check("GET / returns 200", status == 200, f"status={status}")
    else:
        check("GET / returns 200", True)

    # 5. API endpoints
    print("\n[5] API endpoints")
    status, data = api("/api/sources")
    check("GET /api/sources", status == 200 and isinstance(data, list), f"status={status}")
    if status == 200:
        check(f"  {len(data)} sources returned", len(data) >= 19, f"got {len(data)}")
        tg = next((s for s in data if s["name"] == "telegram"), None)
        check("  telegram has dependencies", tg and "dependencies" in tg)
        # Use browser for schema check - always importable (no external deps)
        br = next((s for s in data if s["name"] == "browser"), None)
        check("  browser has config_schema", br and "config_schema" in br and br["config_schema"])

    status, data = api("/api/config")
    check("GET /api/config", status == 200, f"status={status}")

    status, data = api("/api/daemon")
    check("GET /api/daemon", status == 200 and "running" in data, f"status={status}")
    check("  daemon not running initially", status == 200 and not data.get("running"))

    # 6. Enable a source (obsidian - no external deps)
    print("\n[6] Source configuration")
    status, data = api("/api/sources/obsidian", "PUT", {"config": {"vault_path": "/tmp/test_vault"}})
    check("PUT config for obsidian", status == 200, f"status={status} data={data}")

    status, data = api("/api/sources/obsidian", "PUT", {"enabled": True})
    check("Enable obsidian", status == 200, f"status={status} data={data}")

    status, sources = api("/api/sources")
    if status == 200:
        obs = next((s for s in sources if s["name"] == "obsidian"), None)
        check("  obsidian shows enabled", obs and obs.get("enabled"), f"enabled={obs.get('enabled') if obs else 'not found'}")

    # 7. Input validation (use browser - always importable, has int field)
    print("\n[7] Input validation")
    status, data = api("/api/sources/browser", "PUT", {"config": {"session_window_minutes": "not_a_number"}})
    check("Rejects non-integer", status == 422, f"status={status} data={data}")
    if status == 422:
        check("  error mentions field", "session_window_minutes" in str(data.get("errors", "")))

    # 8. Daemon control
    print("\n[8] Daemon control")
    status, data = api("/api/daemon/start", "POST")
    check("POST /api/daemon/start", status == 200, f"status={status}")

    status, data = api("/api/daemon")
    check("  daemon now running", status == 200 and data.get("running"), f"data={data}")

    status, data = api("/api/daemon/stop", "POST")
    check("POST /api/daemon/stop", status == 200, f"status={status}")

    status, data = api("/api/daemon")
    check("  daemon stopped", status == 200 and not data.get("running"), f"data={data}")

    # 9. Dashboard HTML
    print("\n[9] Dashboard content")
    try:
        with urllib.request.urlopen("http://127.0.0.1:9999/", timeout=5) as resp:
            html = resp.read().decode()
        check("HTML contains VADIMGEST", "VADIMGEST" in html or "vadimgest" in html.lower())
        check("HTML contains Sources tab", "Sources" in html)
        check("HTML contains Docs tab", "Docs" in html)
        check("HTML has JS INSTALL_MAP", "INSTALL_MAP" in html)
        check("HTML > 10KB", len(html) > 10000, f"size={len(html)}")
    except Exception as e:
        check("fetch dashboard HTML", False, str(e))

    # 10. Sync attempt (obsidian with empty vault)
    print("\n[10] Sync attempt")
    os.makedirs("/tmp/test_vault", exist_ok=True)
    Path("/tmp/test_vault/test.md").write_text("# Test\nHello world")
    status, data = api("/api/sources/obsidian/sync", "POST")
    check("POST /api/sources/obsidian/sync", status == 200, f"status={status} data={data}")

    # cleanup
    server.terminate()
    server.wait(timeout=5)

    # summary
    print(f"\n{'=' * 50}")
    if errors:
        print(f"{FAIL} {len(errors)} failures:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print(f"{PASS} All checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
