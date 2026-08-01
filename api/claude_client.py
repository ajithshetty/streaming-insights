import json
import os

from anthropic import AsyncAnthropic

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-5")

_client: AsyncAnthropic | None = None


def get_client() -> AsyncAnthropic:
    global _client
    if _client is None:
        _client = AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


SYSTEM_PROMPT = """You are a streaming-analytics assistant embedded in a live \
user-interaction event pipeline (Kafka -> Flink -> Postgres). You will be \
given windowed aggregate data covering a recent time range, along with a \
question from a data engineer.

Rules:
- Answer using only the data provided in the context. Do not invent numbers.
- If the data doesn't cover what's asked (e.g. asking about a time range \
with no windows), say so plainly.
- Be concise and quantitative: cite the actual counts/rates you see.
- If something in the data looks like an anomaly (a sudden spike or drop \
relative to the surrounding windows), you may point it out even if not asked.
- Do not use markdown headers; a couple of short paragraphs or a small \
bullet list is enough.
"""


def _build_context(minutes: int, summary, event_type_breakdown, top_pages, raw_windows) -> str:
    return json.dumps(
        {
            "context_window_minutes": minutes,
            "summary_by_window": summary,
            "event_type_totals": event_type_breakdown,
            "top_pages": top_pages,
            "event_type_by_window": raw_windows,
        },
        default=str,
        indent=2,
    )


async def ask_question(question: str, minutes: int, summary, event_type_breakdown, top_pages, raw_windows) -> str:
    context_json = _build_context(minutes, summary, event_type_breakdown, top_pages, raw_windows)

    client = get_client()
    response = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=800,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Aggregate data for the last {minutes} minutes "
                    f"(JSON):\n{context_json}\n\nQuestion: {question}"
                ),
            }
        ],
    )

    parts = [block.text for block in response.content if block.type == "text"]
    return "\n".join(parts).strip()