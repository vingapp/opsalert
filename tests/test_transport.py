"""Tests for transport implementations."""
import json
from unittest.mock import patch

import pytest

from opsalert.transport import CallableTransport, LogTransport, WebhookTransport
from opsalert.types import AlertMessage


def _make_message(**overrides) -> AlertMessage:
    """Create a test AlertMessage."""
    defaults = {
        "subject": "[ERROR] test: something failed",
        "html_body": "<p>Test</p>",
        "text_body": "Test alert",
        "severity": "error",
        "category": "test",
        "alert_count": 1,
    }
    defaults.update(overrides)
    return AlertMessage(**defaults)


class TestCallableTransport:
    """Test CallableTransport wrapping a host-app function."""

    def test_calls_send_fn(self):
        """Delegates to the provided function."""
        calls = []

        def mock_send(message, *, to, from_addr, from_name):
            calls.append((message, to, from_addr, from_name))
            return True

        transport = CallableTransport(mock_send)
        msg = _make_message()
        result = transport.send(
            msg, to="ops@test.com", from_addr="alert@test.com", from_name="Alerts"
        )

        assert result is True
        assert len(calls) == 1
        assert calls[0][0] is msg
        assert calls[0][1] == "ops@test.com"

    def test_returns_false_on_fn_failure(self):
        """Returns False when the function returns False."""
        transport = CallableTransport(lambda *a, **kw: False)
        result = transport.send(_make_message(), to="a", from_addr="b", from_name="c")
        assert result is False

    def test_catches_exceptions(self):
        """Never raises — returns False on exception."""
        def boom(*a, **kw):
            raise ConnectionError("network down")

        transport = CallableTransport(boom)
        result = transport.send(_make_message(), to="a", from_addr="b", from_name="c")
        assert result is False


class TestLogTransport:
    """Test LogTransport for development."""

    def test_returns_true(self):
        """Always returns True."""
        transport = LogTransport()
        result = transport.send(_make_message(), to="dev@test.com", from_addr="a", from_name="b")
        assert result is True

    def test_logs_message(self, caplog):
        """Logs the alert at WARNING level."""
        import logging
        with caplog.at_level(logging.WARNING):
            transport = LogTransport()
            transport.send(
                _make_message(severity="critical", category="infra"),
                to="ops@test.com",
                from_addr="a",
                from_name="b",
            )

        assert "CRITICAL" in caplog.text
        assert "infra" in caplog.text


class _FakeResponse:
    """Stands in for the urlopen context manager — a real object, not a mock."""

    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeUrlopen:
    """Records the Request it was handed and returns a canned response."""

    def __init__(self, status: int = 200) -> None:
        self._status = status
        self.calls: list[tuple[object, object]] = []

    def __call__(self, req, timeout=None):
        self.calls.append((req, timeout))
        return _FakeResponse(self._status)

    @property
    def request(self):
        assert len(self.calls) == 1, f"expected exactly one urlopen call, got {len(self.calls)}"
        return self.calls[0][0]

    @property
    def timeout(self):
        assert len(self.calls) == 1, f"expected exactly one urlopen call, got {len(self.calls)}"
        return self.calls[0][1]


class TestWebhookTransport:
    """Test WebhookTransport against a fake urlopen."""

    def test_posts_json(self):
        """POSTs JSON payload to the configured URL."""
        fake_urlopen = _FakeUrlopen(status=200)
        with patch("opsalert.transport.urllib.request.urlopen", new=fake_urlopen):
            transport = WebhookTransport("https://hooks.example.com/alert")
            msg = _make_message(severity="error", category="sendgrid")
            result = transport.send(msg, to="ops@test.com", from_addr="a", from_name="b")

        assert result is True
        assert len(fake_urlopen.calls) == 1
        req = fake_urlopen.request
        assert req.full_url == "https://hooks.example.com/alert"
        payload = json.loads(req.data)
        assert payload["severity"] == "error"
        assert payload["category"] == "sendgrid"
        assert fake_urlopen.timeout == 10

    def test_returns_false_on_error(self):
        """Returns False on network error."""
        def boom(req, timeout=None):
            raise ConnectionError("down")

        with patch("opsalert.transport.urllib.request.urlopen", new=boom):
            transport = WebhookTransport("https://hooks.example.com/alert")
            result = transport.send(_make_message(), to="a", from_addr="b", from_name="c")

        assert result is False

    @pytest.mark.parametrize("status", [300, 404, 500])
    def test_returns_false_on_non_2xx(self, status):
        """A non-2xx response is a failed send."""
        fake_urlopen = _FakeUrlopen(status=status)
        with patch("opsalert.transport.urllib.request.urlopen", new=fake_urlopen):
            transport = WebhookTransport("https://hooks.example.com/alert")
            result = transport.send(_make_message(), to="a", from_addr="b", from_name="c")

        assert result is False
        assert len(fake_urlopen.calls) == 1

    def test_custom_headers(self):
        """Custom headers are included in the request."""
        fake_urlopen = _FakeUrlopen(status=200)
        with patch("opsalert.transport.urllib.request.urlopen", new=fake_urlopen):
            transport = WebhookTransport(
                "https://hooks.example.com/alert",
                headers={"Authorization": "Bearer tok123"},
            )
            transport.send(_make_message(), to="a", from_addr="b", from_name="c")

        req = fake_urlopen.request
        assert req.get_header("Authorization") == "Bearer tok123"
        assert req.get_header("Content-type") == "application/json"
