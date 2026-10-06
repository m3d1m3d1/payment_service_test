"""Тесты используют реальный PostgreSQL (отдельная БД, её таблицы пересоздаются!).

    createdb payments_test
    TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test pytest
"""

import os

# Принудительно тестовая БД, чтобы случайно не затереть рабочую
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://payments:payments@localhost:5432/payments_test",
)
os.environ["API_KEY"] = "test-key"

import httpx  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.db import SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Base  # noqa: E402

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Test order",
    "metadata": {"order_id": 1},
    "webhook_url": "http://localhost:9000/hook",
}


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _schema():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def _clean():
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=app)  # lifespan не запускается: брокер не нужен
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": "test-key"}
    ) as c:
        yield c


@pytest_asyncio.fixture
def session_factory():
    return SessionLocal
