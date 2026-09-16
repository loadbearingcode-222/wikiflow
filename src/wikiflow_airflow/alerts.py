"""Failure notification.

Deliberately dependency-light. Alerting is the thing you need most when something else
is broken, so it reads its own configuration straight from the environment rather than
through `wikiflow.config` - a failure to load settings must still be able to tell you
that settings failed to load.

Delivery is best-effort by design: a webhook that is down, slow or unconfigured must
never turn a task failure into two failures, or mask the original error behind a
networking one. Everything is logged regardless, so the task log is always the source
of truth and the webhook is a convenience.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

log = logging.getLogger(__name__)

WEBHOOK_ENV = "WIKIFLOW_SLACK_WEBHOOK_URL"
TIMEOUT_SECONDS = 10.0


def format_failure(context: dict[str, Any]) -> str:
    """Build a message that says what broke, when, and where to look."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(task_instance, "dag_id", "unknown-dag")
    task_id = getattr(task_instance, "task_id", "unknown-task")
    try_number = getattr(task_instance, "try_number", "?")
    max_tries = getattr(task_instance, "max_tries", "?")
    logical_date = getattr(dag_run, "logical_date", None)

    lines = [
        f"FAILED  {dag_id}.{task_id}",
        f"  data interval : {logical_date}",
        f"  attempt       : {try_number} of {max_tries}",
    ]
    if exception is not None:
        # One line only. A full traceback belongs in the task log, not in a chat channel.
        lines.append(f"  error         : {type(exception).__name__}: {exception}")
    if (url := getattr(task_instance, "log_url", None)) is not None:
        lines.append(f"  log           : {url}")
    return "\n".join(lines)


def notify_failure(context: dict[str, Any]) -> None:
    """Airflow `on_failure_callback`.

    Never raises. A callback that throws would replace the real failure with its own.
    """
    try:
        message = format_failure(context)
    except Exception:
        log.exception("failed to format the failure alert")
        return

    log.error(message)

    webhook = os.environ.get(WEBHOOK_ENV, "").strip()
    if not webhook:
        log.info("%s not set; alert logged only", WEBHOOK_ENV)
        return

    try:
        response = httpx.post(webhook, json={"text": message}, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as exc:
        log.warning("could not deliver alert to the webhook: %s", exc)


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "wikiflow",
    "on_failure_callback": notify_failure,
}
