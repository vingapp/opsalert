"""Test fixtures — in-memory SQLite async session."""
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import opsalert
from opsalert.model import OpsAlertBase


@pytest.fixture
async def engine():
    """Create an in-memory SQLite async engine with tables."""
    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(OpsAlertBase.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine):
    """Provide a fresh async session for each test."""
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        yield session


@pytest.fixture
async def session_factory(engine):
    """Provide a session factory (async context manager) for configure()."""
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def factory():
        async with maker() as session:
            yield session

    return factory


@pytest.fixture(autouse=True)
def reset_opsalert_config():
    """Reset opsalert config and ingest state before each test."""
    opsalert.reset_config()
    _reset_ingest()
    yield
    opsalert.reset_config()
    _reset_ingest()


def _reset_ingest():
    """Reset the ingest module state for test isolation.

    Increments the generation counter so old threads exit their loops,
    then waits for them to die before clearing state.
    """
    from opsalert import ingest

    # Bump generation so old threads exit
    ingest._generation += 1
    # Wake any sleeping thread so it sees the generation change
    with ingest._condition:
        ingest._condition.notify_all()

    # Wait for any existing thread to actually finish — must complete before
    # we clear state, otherwise a dying thread could write to _sample_state
    # after we clear it.
    if ingest._thread is not None and ingest._thread.is_alive():
        ingest._thread.join(timeout=10.0)
        if ingest._thread.is_alive():
            # Thread refused to die — _sample_state may be contaminated.
            # The generation bump prevents it from producing new batches,
            # but it could still be mid-write.
            import warnings

            warnings.warn(
                "opsalert ingest thread did not exit after 10s; "
                "test isolation may be compromised",
                stacklevel=2,
            )

    # Dispose any existing engine
    if ingest._engine is not None:
        try:
            ingest._engine.dispose()
        except Exception:
            pass

    with ingest._condition:
        ingest._queue.clear()
        ingest._per_fp.clear()
        ingest._dropped.clear()
        ingest._sample_state.clear()
    ingest._thread = None
    ingest._engine = None
    ingest._error_logged = False
    ingest._flush_event.clear()
    ingest._flush_done_event.clear()
    ingest._written_total = 0
    ingest._sampled_out_total = 0
    ingest._dropped_total = 0
