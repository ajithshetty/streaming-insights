# Streaming Insights Q&A

Ask natural-language questions about a live stream of user-interaction events.

```
producer (dummy events) → Kafka/Redpanda → PyFlink (30s windows) → Postgres → FastAPI → Claude → React UI
```

- **producer** — generates dummy `view / click / add_to_cart / purchase / scroll`
  events onto the `user-events` topic (with an occasional purchase "spike" so
  there's something to notice).
- **flink-job** — a PyFlink Table API job that runs three tumbling 30s window
  aggregations (event-type counts, page counts, overall summary) and writes
  them straight to Postgres via the JDBC connector.
- **api** — FastAPI service. `/api/stats` serves the live aggregates for the
  dashboard; `/api/ask` pulls the last N minutes of aggregates, hands them to
  Claude as context, and returns a grounded answer.
- **frontend** — React chat UI with a live stats sidebar (event mix, top
  pages, events/sec), polling `/api/stats` every 5s.

## Run it

```bash
cp .env.example .env   # add your ANTHROPIC_API_KEY
docker compose up --build
```

Then open:
- UI: http://localhost:5173
- API docs: http://localhost:8000/docs

It takes ~30-60s after startup for the first aggregate windows to land in
Postgres, so give it a minute before asking questions.

## Try asking

- "What's the most common event type right now?"
- "How many purchases happened in the last 10 minutes?"
- "Is there anything unusual in the traffic pattern?"
- "Which page is getting the most engagement?"

## Notes / where to take this further

- The Flink job runs as an embedded local mini-cluster inside its container
  (simplest path for a demo). For a "real" cluster, split it into
  jobmanager/taskmanager services and submit via `flink run`.
- `WINDOW_SECONDS` (flink-job) and `CONTEXT_WINDOW_MINUTES` (api) are both
  configurable via env vars in `docker-compose.yml`.
- The `/api/ask` context is the full JSON of recent aggregates — for a much
  longer time range you'd want to switch to a text-to-SQL approach instead
  of stuffing everything into the prompt.
- No auth on any service — this is meant for local/demo use.

## Local frontend dev (hot reload)

```bash
cd frontend
npm install
VITE_API_BASE_URL=http://localhost:8000 npm run dev
```

## Built with love from Claude and Cursor