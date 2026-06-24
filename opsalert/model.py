"""Alert model — single table owned entirely by the package."""
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Index, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class OpsAlertBase(DeclarativeBase):
    """Package's own declarative base.

    Tables are created via ``opsalert.ensure_tables(engine)`` at startup.
    """

    pass


class Alert(OpsAlertBase):
    """Operational alert occurrence.

    Every ``opsalert.warn/error/critical`` call creates one row. Alerts are
    grouped by ``category`` (broad error type) and ``message`` (specific
    sub-type) for dashboard display and batch deletion.

    Lifecycle: created → viewed in dashboard → batch-deleted after fix.
    No resolved/acknowledged states — entries exist or they don't.
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
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    # Classification
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    message: Mapped[str] = mapped_column(String(500), nullable=False)

    # Structured context (JSON string, per-occurrence variable data)
    context_json: Mapped[str | None] = mapped_column(Text, nullable=True)

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
