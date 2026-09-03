"""Alert models — the two tables owned entirely by the package.

``Alert`` is one *occurrence*: a single ``opsalert.error(...)`` call.
``AlertCondition`` is the *thing that is wrong* — the recurring problem those
occurrences are instances of. Occurrences are volatile (pruned on a retention
clock); the condition carries the state a human cares about: acknowledged,
resolved, the issue URL, how often it fires, whether it came back.
"""
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class OpsAlertBase(DeclarativeBase):
    """Package's own declarative base.

    Tables are created via ``opsalert.ensure_tables(engine)`` at startup.
    """

    pass


class Alert(OpsAlertBase):
    """Operational alert occurrence.

    Every ``opsalert.warn/error/critical`` call creates one row. Occurrences
    are grouped for display by ``category`` (broad error type) and ``message``
    (specific sub-type), and — when condition resolution succeeded at fire
    time — by :class:`AlertCondition` via ``condition_id``.

    ``condition_id`` is nullable on purpose: an occurrence is never lost to a
    failure of conditionization (P2). A NULL row is an orphan; the maintenance
    sweeper adopts it later, and until then it still delivers and still shows
    up in the legacy category views.

    Lifecycle lives on the *condition*, not here. An occurrence is a fact that
    happened: it exists until retention prunes it.
    """

    __tablename__ = "opsalert"

    __table_args__ = (
        # Dashboard L1: group by category, ordered by recency
        Index("ix_admin_alert_cat_created", "category", "created"),
        # Dashboard L2: message-level drill-down
        Index("ix_admin_alert_cat_msg", "category", "message"),
        # Delivery sweeper: find un-notified alerts by severity
        Index("ix_admin_alert_notified_sev", "notified", "severity", "category"),
        # Batch throttle check: recent notified alerts per category
        Index("ix_admin_alert_cat_notified_created", "category", "notified", "created"),
        # Cleanup sweeper: age-based deletion
        Index("ix_admin_alert_created", "created"),
        # Condition drill-down, stats watermark scan, orphan adoption
        Index("ix_admin_alert_condition", "condition_id", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    # Classification
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    message: Mapped[str] = mapped_column(String(500), nullable=False)

    # Structured context (JSON string, per-occurrence variable data)
    context_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    # The condition this occurrence is an instance of. NULL = not yet
    # resolved (fire-time failure, or a row that predates conditions);
    # never an error, never a reason to drop the occurrence.
    condition_id: Mapped[int | None] = mapped_column(
        ForeignKey("alert_condition.id", ondelete="SET NULL"), nullable=True
    )

    # Email delivery tracking
    notified: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", nullable=False
    )

    # Timestamps — no host-app mixin dependency
    created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Alert(id={self.id}, severity={self.severity!r}, category={self.category!r})>"


class AlertCondition(OpsAlertBase):
    """A recurring operational problem — the thing occurrences are instances of.

    Identity is ``signature_key`` = hash(category, source, environment,
    message_template); see :mod:`opsalert.signature`. Editing a template in
    code therefore mints a NEW condition and strands the old one — accepted,
    and the auto-stale rule garbage-collects the stranded row.

    ``status`` (new | acknowledged | resolved | closed) and ``disposition``
    (NULL | immediate | digest | collect) are orthogonal: status says where the
    condition is in its life, disposition says how loudly it should be
    delivered. "wontfix" is acknowledged + collect + a note.

    Counters (``occurrence_count``, ``first_seen``, ``last_seen``) survive
    occurrence pruning: they are maintained by the stats sweeper up to the
    ``stats_synced_through`` watermark, and cleanup may only delete
    occurrences at or below that watermark (P3).

    ``acknowledged`` does not mean silenced forever: ``acknowledged_severity``,
    ``acknowledged_occurrence_count`` and ``acknowledged_until`` let the
    lifecycle sweep notice the episode got worse (severity escalation, a
    burst far above the ack-time rate) or that a time-boxed lease expired
    while the condition kept firing, and reopen it (opsalert#7).
    """

    __tablename__ = "alert_condition"

    __table_args__ = (
        # Attention/list queries: environment-scoped, status- and severity-faceted
        Index("ix_alert_condition_env_status", "environment", "status"),
        Index("ix_alert_condition_env_category", "environment", "category"),
        # Lifecycle sweeps scan by status + silence
        Index("ix_alert_condition_status_last_seen", "status", "last_seen"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    # Identity
    signature_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    environment: Mapped[str | None] = mapped_column(String(50), nullable=True)
    message_template: Mapped[str] = mapped_column(String(500), nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(
        String(12), default="new", server_default="new", nullable=False
    )
    disposition: Mapped[str | None] = mapped_column(String(12), nullable=True)

    # Worst severity ever seen, and the severity of the most recent occurrence.
    # Both are needed: the worst drives delivery urgency, the latest tells a
    # reader whether the condition is still as bad as it once was.
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    latest_severity: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Human resolution record
    issue_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Audit stamps
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    acknowledged_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # Baseline captured AT ack time, so the escalation rule (opsalert#7) has
    # something to compare the condition's current state against. NULL on
    # rows acknowledged before this column existed, and on any row where the
    # ack predates the baseline being known — lifecycle.py treats NULL as
    # "unknowable", not "zero", except where documented at the call site.
    acknowledged_severity: Mapped[str | None] = mapped_column(String(10), nullable=True)
    acknowledged_occurrence_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Optional lease: past this time, a still-firing acknowledged condition
    # reopens even without escalation or burst — the operator said "I've got
    # this for a while", not "forever".
    acknowledged_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status_changed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Derived statistics — outlive the occurrences they were computed from
    first_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    occurrence_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    reopened_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    median_interval_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Highest occurrence id folded into the counters above. Occurrences at or
    # below it are provably counted, which is what makes them safe to prune.
    stats_synced_through: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )

    created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False
    )
    updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<AlertCondition(id={self.id}, category={self.category!r}, "
            f"status={self.status!r}, count={self.occurrence_count})>"
        )
