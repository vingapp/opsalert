"""Pluggable transport — ABC and built-in implementations.

The host application injects a transport via configure(). The package
never depends on any specific email library.
"""
import json
import logging
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from collections.abc import Callable

from opsalert.types import AlertMessage

logger = logging.getLogger(__name__)


class Transport(ABC):
    """Abstract base for alert notification delivery."""

    @abstractmethod
    def send(self, message: AlertMessage, *, to: str, from_addr: str, from_name: str) -> bool:
        """Send a notification. Never raises. Returns True on success."""


class CallableTransport(Transport):
    """Wraps a host-app send function.

    Usage::

        def _send_via_sendgrid(message, *, to, from_addr, from_name):
            sg = SendGridEmail(...)
            sg.send()
            return bool(sg.msg_id)

        opsalert.configure(transport=CallableTransport(_send_via_sendgrid))
    """

    def __init__(self, send_fn: Callable[..., bool]) -> None:
        self._send_fn = send_fn

    def send(self, message: AlertMessage, *, to: str, from_addr: str, from_name: str) -> bool:
        try:
            return self._send_fn(message, to=to, from_addr=from_addr, from_name=from_name)
        except Exception:
            logger.exception("CallableTransport send failed")
            return False


class LogTransport(Transport):
    """Log alerts instead of sending — for development and testing."""

    def send(self, message: AlertMessage, *, to: str, from_addr: str, from_name: str) -> bool:
        logger.warning(
            "ALERT [%s] %s: %s (to=%s)",
            message.severity.upper(),
            message.category,
            message.subject,
            to,
        )
        return True


class WebhookTransport(Transport):
    """POST JSON to a webhook URL (Slack, PagerDuty, etc).

    Uses only stdlib — no requests/httpx dependency.
    """

    def __init__(self, url: str, *, headers: dict[str, str] | None = None) -> None:
        self._url = url
        self._headers = headers or {}

    def send(self, message: AlertMessage, *, to: str, from_addr: str, from_name: str) -> bool:
        payload = {
            "severity": message.severity,
            "category": message.category,
            "subject": message.subject,
            "text": message.text_body,
            "alert_count": message.alert_count,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            headers={"Content-Type": "application/json", **self._headers},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return 200 <= resp.status < 300
        except Exception:
            logger.exception("WebhookTransport send failed to %s", self._url)
            return False
