"""Мини-приемник webhook для ручной проверки.

POST /hook  -> 200, печатает тело в лог
POST /fail  -> 500 (для проверки retry и DLQ)
"""

import logging

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("webhook-receiver")
app = FastAPI()


@app.post("/hook")
async def hook(request: Request) -> dict:
    log.info("WEBHOOK RECEIVED: %s", await request.json())
    return {"ok": True}


@app.post("/fail")
async def fail(request: Request) -> None:
    log.info("WEBHOOK (forced failure): %s", await request.json())
    raise HTTPException(500, "forced failure")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9000)
