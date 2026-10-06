import asyncio

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import OutboxEvent, Payment
from tests.conftest import BODY


def idem(key: str) -> dict:
    return {"Idempotency-Key": key}


async def test_create_and_get(client):
    r = await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    assert r.status_code == 202
    data = r.json()
    assert data["status"] == "pending"

    r = await client.get(f"/api/v1/payments/{data['payment_id']}")
    assert r.status_code == 200
    got = r.json()
    assert got["amount"] == "100.50" and got["currency"] == "RUB"
    assert got["metadata"] == {"order_id": 1}
    assert got["processed_at"] is None


async def test_payment_and_outbox_written_together(client):
    r = await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    async with SessionLocal() as s:
        payments = (await s.execute(select(Payment))).scalars().all()
        events = (await s.execute(select(OutboxEvent))).scalars().all()
    assert len(payments) == 1 and len(events) == 1
    assert events[0].payload == {"payment_id": r.json()["payment_id"]}
    assert events[0].published_at is None


async def test_repeat_same_key_returns_same_payment_without_duplicates(client):
    r1 = await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    r2 = await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    assert r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]
    async with SessionLocal() as s:
        assert await s.scalar(select(func.count()).select_from(Payment)) == 1
        assert await s.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_same_key_with_equivalent_amount_format_is_a_replay(client):
    await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    r = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "100.5"}, headers=idem("k1")
    )
    assert r.status_code == 202


async def test_same_key_with_different_body_conflicts(client):
    await client.post("/api/v1/payments", json=BODY, headers=idem("k1"))
    r = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "999.00"}, headers=idem("k1")
    )
    assert r.status_code == 409


async def test_parallel_requests_with_same_key_create_one_payment(client):
    results = await asyncio.gather(
        *[client.post("/api/v1/payments", json=BODY, headers=idem("race")) for _ in range(8)]
    )
    assert {r.status_code for r in results} == {202}
    assert len({r.json()["payment_id"] for r in results}) == 1
    async with SessionLocal() as s:
        assert await s.scalar(select(func.count()).select_from(Payment)) == 1
        assert await s.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_auth_required(client):
    r = await client.post(
        "/api/v1/payments", json=BODY, headers={**idem("k"), "X-API-Key": "wrong"}
    )
    assert r.status_code == 401
    r = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": ""}
    )
    assert r.status_code == 401


async def test_validation(client):
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422  # нет ключа
    bad = [
        {**BODY, "currency": "GBP"},
        {**BODY, "amount": "0"},
        {**BODY, "amount": "-5"},
        {**BODY, "amount": "1.999"},
        {**BODY, "webhook_url": "ftp://x"},
        {**BODY, "metadata": {"x": "a" * 20000}},
    ]
    for i, body in enumerate(bad):
        r = await client.post("/api/v1/payments", json=body, headers=idem(f"bad-{i}"))
        assert r.status_code == 422, body


async def test_get_unknown_payment_404(client):
    r = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


async def test_race_lost_at_insert_returns_existing_payment(client, monkeypatch):
    """Детерминированная гонка: проверка ключа ничего не нашла, но INSERT проиграл."""
    from app import main

    key = {"Idempotency-Key": "race-det"}
    first = await client.post("/api/v1/payments", json=BODY, headers=key)

    real = main._get_by_idempotency_key
    calls = {"n": 0}

    async def stale_first_lookup(session, k):
        calls["n"] += 1
        return None if calls["n"] == 1 else await real(session, k)

    monkeypatch.setattr(main, "_get_by_idempotency_key", stale_first_lookup)

    r = await client.post("/api/v1/payments", json=BODY, headers=key)
    assert r.status_code == 202
    assert r.json()["payment_id"] == first.json()["payment_id"]

    calls["n"] = 0  # то же самое, но с другим телом -> 409, а не 500
    r = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=key)
    assert r.status_code == 409


async def test_metadata_limit_counts_utf8_bytes(client):
    # 6000 кириллических символов = 12000 байт < 16 КБ, а 9000 = 18000 байт > 16 КБ
    ok = {**BODY, "metadata": {"t": "я" * 6000}}
    too_big = {**BODY, "metadata": {"t": "я" * 9000}}
    assert (await client.post("/api/v1/payments", json=ok, headers=idem("m1"))).status_code == 202
    assert (await client.post("/api/v1/payments", json=too_big, headers=idem("m2"))).status_code == 422
