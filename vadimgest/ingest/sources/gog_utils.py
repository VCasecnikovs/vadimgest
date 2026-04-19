"""Shared gog CLI helper for vadimgest syncers that use Google services."""

import json
import subprocess
from typing import Any


def gog_call(service: str, command: str, args: list[str] | None = None,
             account: str = "", timeout: int = 30) -> Any:
    """
    Call gog CLI, parse JSON response.

    Args:
        service: e.g. "gmail", "calendar", "tasks", "drive"
        command: e.g. "search", "events", "lists list"
        args: additional CLI arguments
        account: Google account email
        timeout: subprocess timeout in seconds

    Returns:
        Parsed JSON response (dict/list/str)

    Raises:
        RuntimeError: if gog call fails
    """
    cmd = ["gog", service] + command.split() + ["--json"]

    if account:
        cmd.extend(["-a", account])

    if args:
        cmd.extend(args)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"gog {service} {command} failed: {stderr}")

    if not result.stdout.strip():
        return {}

    return json.loads(result.stdout)
