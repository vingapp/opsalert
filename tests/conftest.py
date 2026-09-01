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
    """Reset opsalert config before each test."""
    opsalert.reset_config()
    yield
    opsalert.reset_config()
