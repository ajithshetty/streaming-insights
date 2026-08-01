import os
from datetime import timedelta

import asyncpg

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streamdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            min_size=1,
            max_size=5,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_summary(minutes: int):
    pool = await init_pool()
    rows = await pool.fetch(
        """
        SELECT window_start, window_end, total_events, unique_users, events_per_sec
        FROM summary_agg
        WHERE window_start >= now() - $1::interval
        ORDER BY window_start ASC
        """,
        timedelta(minutes=minutes),
    )
    return [dict(r) for r in rows]


async def get_event_type_breakdown(minutes: int):
    pool = await init_pool()
    rows = await pool.fetch(
        """
        SELECT event_type, SUM(event_count) AS event_count, MAX(unique_users) AS peak_unique_users
        FROM event_type_agg
        WHERE window_start >= now() - $1::interval
        GROUP BY event_type
        ORDER BY event_count DESC
        """,
        timedelta(minutes=minutes),
    )
    return [dict(r) for r in rows]


async def get_top_pages(minutes: int, limit: int = 10):
    pool = await init_pool()
    rows = await pool.fetch(
        """
        SELECT page, SUM(event_count) AS event_count
        FROM page_agg
        WHERE window_start >= now() - $1::interval
        GROUP BY page
        ORDER BY event_count DESC
        LIMIT $2
        """,
        timedelta(minutes=minutes),
        limit,
    )
    return [dict(r) for r in rows]


async def get_recent_windows_raw(minutes: int):
    """Fine-grained per-window event_type rows, used as Claude's context."""
    pool = await init_pool()
    rows = await pool.fetch(
        """
        SELECT window_start, window_end, event_type, event_count, unique_users
        FROM event_type_agg
        WHERE window_start >= now() - $1::interval
        ORDER BY window_start ASC
        """,
        timedelta(minutes=minutes),
    )
    return [dict(r) for r in rows]
