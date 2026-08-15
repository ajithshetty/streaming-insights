import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import claude_client
import db

DEFAULT_CONTEXT_MINUTES = int(os.environ.get("CONTEXT_WINDOW_MINUTES", "15"))

app = FastAPI(title="Streaming Insights Q&A")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str
    minutes: int | None = None


@app.on_event("startup")
async def on_startup():
    await db.init_pool()


@app.on_event("shutdown")
async def on_shutdown():
    await db.close_pool()
    await claude_client.close_client()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/stats")
async def stats(minutes: int = DEFAULT_CONTEXT_MINUTES):
    summary = await db.get_summary(minutes)
    event_types = await db.get_event_type_breakdown(minutes)
    top_pages = await db.get_top_pages(minutes)
    return {
        "minutes": minutes,
        "summary_by_window": summary,
        "event_type_totals": event_types,
        "top_pages": top_pages,
    }


@app.post("/api/ask")
async def ask(req: AskRequest):
    if not req.question or not req.question.strip():
        raise HTTPException(status_code=400, detail="question must not be empty")

    minutes = req.minutes or DEFAULT_CONTEXT_MINUTES

    summary = await db.get_summary(minutes)
    event_types = await db.get_event_type_breakdown(minutes)
    top_pages = await db.get_top_pages(minutes)
    raw_windows = await db.get_recent_windows_raw(minutes)

    if not summary:
        return {
            "answer": (
                f"There's no aggregated data yet for the last {minutes} "
                "minutes. Give the pipeline a little longer to produce a "
                "few windows, then ask again."
            ),
            "minutes": minutes,
        }

    try:
        answer = await claude_client.ask_question(
            req.question, minutes, summary, event_types, top_pages, raw_windows
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Claude request failed: {exc}") from exc

    return {"answer": answer, "minutes": minutes}