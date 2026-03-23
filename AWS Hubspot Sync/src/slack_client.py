"""Standalone Slack client for sync notifications.

Uses the Slack Web API directly via requests. Only needs the `requests` library.
Optional — sync works without Slack configured.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests

from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class SlackConfig:
    """Slack API configuration."""

    bot_token: str = ""

    def __post_init__(self) -> None:
        if not self.bot_token:
            self.bot_token = os.environ.get("SLACK_BOT_TOKEN", "")
        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable is required for Slack notifications")


class SlackClient:
    """Lightweight Slack Web API client for posting messages."""

    BASE_URL = "https://slack.com/api"

    def __init__(self, config: SlackConfig | None = None) -> None:
        self.config = config or SlackConfig()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.config.bot_token}",
                "Content-Type": "application/json; charset=utf-8",
            }
        )

    def send_message(
        self,
        channel: str,
        blocks: list[dict[str, Any]] | None = None,
        text: str = "",
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        """Post a message to a Slack channel."""
        payload: dict[str, Any] = {"channel": channel, "text": text}
        if blocks:
            payload["blocks"] = blocks
        if thread_ts:
            payload["thread_ts"] = thread_ts

        response = self._session.post(f"{self.BASE_URL}/chat.postMessage", json=payload)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            raise RuntimeError(f"Slack API error: {data.get('error', 'unknown')}")

        return data

    def send_message_with_ts(
        self,
        channel: str,
        blocks: list[dict[str, Any]] | None = None,
        text: str = "",
    ) -> str | None:
        """Post a message and return its thread timestamp (for threading replies)."""
        data = self.send_message(channel=channel, blocks=blocks, text=text)
        return data.get("ts")
