-- Aggregate tables written to by the Flink job

CREATE TABLE IF NOT EXISTS event_type_agg (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    event_type   TEXT NOT NULL,
    event_count  BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    PRIMARY KEY (window_start, event_type)
);

CREATE TABLE IF NOT EXISTS page_agg (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    page         TEXT NOT NULL,
    event_count  BIGINT NOT NULL,
    PRIMARY KEY (window_start, page)
);

CREATE TABLE IF NOT EXISTS summary_agg (
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    total_events  BIGINT NOT NULL,
    unique_users  BIGINT NOT NULL,
    events_per_sec DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS idx_event_type_agg_window ON event_type_agg (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_page_agg_window ON page_agg (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_summary_agg_window ON summary_agg (window_start DESC);
