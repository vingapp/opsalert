"""Environment labelling — subjects, bodies, and stored context name the deployment.

An alert that does not say which deployment produced it is a bug report with
the machine name torn off. These tests pin the label onto every surface that
reaches a human, and pin the unconfigured case to byte-identical old output so
consumers that never set `environment` are unaffected.
"""
import json

import pytest
from sqlalchemy import select

import opsalert
from opsalert.delivery import (
    _render_digest_email,
    _render_immediate_email,
    _subject_prefix,
    deliver_alerts,
)
from opsalert.model import Alert
from opsalert.store import fire_alert
from opsalert.types import AlertMessage


class _TrackingTransport(opsalert.Transport):
    def __init__(self):
        self.sent: list[AlertMessage] = []

    def send(self, message, *, to, from_addr, from_name):
        self.sent.append(message)
        return True


@pytest.fixture(autouse=True)
def _reset():
    yield
    opsalert.reset_config()


# --------------------------------------------------------------------------
# Subject prefix
# --------------------------------------------------------------------------


def test_subject_prefix_upper_cases_the_environment():
    assert _subject_prefix("staging") == "[STAGING] "


def test_subject_prefix_is_empty_without_an_environment():
    assert _subject_prefix(None) == ""
    assert _subject_prefix("") == ""


async def test_immediate_subject_carries_the_environment(session, session_factory):
    transport = _TrackingTransport()
    opsalert.configure(
        session_factory=session_factory, transport=transport, environment="staging"
    )
    await fire_alert(
        session, severity="error", category="import_pipeline", message="Row 42 failed"
    )
    await session.commit()

    await deliver_alerts(session)

    assert transport.sent[0].subject == "[STAGING] [ERROR] import_pipeline: Row 42 failed"
    assert transport.sent[0].environment == "staging"


async def test_immediate_subject_unchanged_without_an_environment(session, session_factory):
    transport = _TrackingTransport()
    opsalert.configure(session_factory=session_factory, transport=transport)
    await fire_alert(
        session, severity="error", category="import_pipeline", message="Row 42 failed"
    )
    await session.commit()

    await deliver_alerts(session)

    assert transport.sent[0].subject == "[ERROR] import_pipeline: Row 42 failed"
    assert transport.sent[0].environment is None


async def test_digest_subject_carries_the_environment(session, session_factory):
    transport = _TrackingTransport()
    opsalert.configure(
        session_factory=session_factory, transport=transport, environment="production"
    )
    await fire_alert(session, severity="warn", category="unknown_param", message="?foo")
    await session.commit()

    await deliver_alerts(session)

    subject = transport.sent[0].subject
    assert subject.startswith("[PRODUCTION] [ALERT DIGEST] ")
    assert transport.sent[0].environment == "production"


async def test_digest_subject_unchanged_without_an_environment(session, session_factory):
    transport = _TrackingTransport()
    opsalert.configure(session_factory=session_factory, transport=transport)
    await fire_alert(session, severity="warn", category="unknown_param", message="?foo")
    await session.commit()

    await deliver_alerts(session)

    assert transport.sent[0].subject.startswith("[ALERT DIGEST] ")


# --------------------------------------------------------------------------
# Body line
# --------------------------------------------------------------------------


def test_immediate_body_opens_with_the_environment():
    html = _render_immediate_email(
        category="import_pipeline",
        severity="error",
        count=3,
        latest_message="Row 42 failed",
        environment="staging",
    )
    assert "Environment: <strong>staging</strong>" in html
    # Ahead of the heading, so it is the first thing read.
    assert html.index("Environment:") < html.index("Alert — import_pipeline")


def test_digest_body_opens_with_the_environment(session):
    class _Row:
        category = "unknown_param"
        latest_message = "?foo"
        count = 2

    html = _render_digest_email([_Row()], environment="staging")
    assert "Environment: <strong>staging</strong>" in html
    assert html.index("Environment:") < html.index("Alert Digest")


async def test_text_bodies_open_with_the_environment(session, session_factory):
    transport = _TrackingTransport()
    opsalert.configure(
        session_factory=session_factory, transport=transport, environment="staging"
    )
    await fire_alert(session, severity="error", category="cat_a", message="boom")
    await fire_alert(session, severity="warn", category="cat_b", message="meh")
    await session.commit()

    await deliver_alerts(session)

    for message in transport.sent:
        assert message.text_body.startswith("Environment: staging\n")


# --------------------------------------------------------------------------
# None == byte-identical old behaviour
# --------------------------------------------------------------------------


def test_unconfigured_environment_renders_byte_identical_bodies():
    """The label must be invisible — not merely blank — when unset."""
    kwargs = dict(
        category="import_pipeline", severity="error", count=3, latest_message="Row 42 failed"
    )
    assert _render_immediate_email(**kwargs) == _render_immediate_email(
        **kwargs, environment=None
    )
    # Golden: the exact pre-change body, character for character. A substring
    # check would pass on a body that merely gained a blank line where the
    # environment paragraph goes; this fails on it.
    assert _render_immediate_email(**kwargs) == """
    <div style="font-family: sans-serif; max-width: 600px;">
        <h2 style="color: #fd7e14;">
            ERROR Alert \u2014 import_pipeline
        </h2>
        <p style="font-size: 16px;">Row 42 failed</p>
        <table style="border-collapse: collapse; margin-top: 12px;">
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Category:</td>
                <td>import_pipeline</td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Occurrences:</td>
                <td>3</td></tr>
        </table>
    </div>
    """

    class _Row:
        category = "unknown_param"
        latest_message = "?foo"
        count = 2

    digest = _render_digest_email([_Row()])
    assert "Environment" not in digest
    assert '<div style="font-family: sans-serif; max-width: 600px;">\n        <h2' in digest


async def test_unconfigured_environment_stores_context_unchanged(session, session_factory):
    opsalert.configure(session_factory=session_factory)
    await fire_alert(
        session, severity="error", category="cat", message="m", context={"row": 42}
    )
    await session.commit()

    alert = (await session.execute(select(Alert))).scalar_one()
    assert json.loads(alert.context_json) == {"row": 42}


# --------------------------------------------------------------------------
# Context stamp
# --------------------------------------------------------------------------


async def test_stored_occurrence_context_is_stamped(session, session_factory):
    opsalert.configure(session_factory=session_factory, environment="staging")
    await fire_alert(
        session, severity="error", category="cat", message="m", context={"row": 42}
    )
    await session.commit()

    alert = (await session.execute(select(Alert))).scalar_one()
    assert json.loads(alert.context_json) == {"row": 42, "environment": "staging"}


async def test_stamp_creates_a_context_when_the_caller_passed_none(session, session_factory):
    opsalert.configure(session_factory=session_factory, environment="staging")
    await fire_alert(session, severity="error", category="cat", message="m")
    await session.commit()

    alert = (await session.execute(select(Alert))).scalar_one()
    assert json.loads(alert.context_json) == {"environment": "staging"}


async def test_stamp_never_clobbers_a_caller_provided_key(session, session_factory):
    opsalert.configure(session_factory=session_factory, environment="staging")
    await fire_alert(
        session,
        severity="error",
        category="cat",
        message="m",
        context={"environment": "production", "row": 1},
    )
    await session.commit()

    alert = (await session.execute(select(Alert))).scalar_one()
    assert json.loads(alert.context_json)["environment"] == "production"


async def test_stamp_does_not_mutate_the_callers_dict(session, session_factory):
    opsalert.configure(session_factory=session_factory, environment="staging")
    caller_context = {"row": 42}
    await fire_alert(
        session, severity="error", category="cat", message="m", context=caller_context
    )
    await session.commit()

    assert caller_context == {"row": 42}


async def test_stamp_survives_an_unconfigured_opsalert(session):
    """Storing must never depend on configure() having been called."""
    opsalert.reset_config()
    await fire_alert(
        session, severity="error", category="cat", message="m", context={"row": 42}
    )
    await session.commit()

    alert = (await session.execute(select(Alert))).scalar_one()
    assert json.loads(alert.context_json) == {"row": 42}
