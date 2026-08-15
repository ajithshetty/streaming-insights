import { useEffect, useRef, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const POLL_MS = 5000;
const CONTEXT_MINUTES = 15;

const EVENT_LABELS = {
  view: "Views",
  click: "Clicks",
  add_to_cart: "Add to cart",
  purchase: "Purchases",
  scroll: "Scrolls",
};

function formatTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return iso;
  }
}

function useStats() {
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const res = await fetch(`${API_BASE}/api/stats?minutes=${CONTEXT_MINUTES}`);
        if (!res.ok) throw new Error(`stats ${res.status}`);
        const data = await res.json();
        if (!cancelled) {
          setStats(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError(err.message);
      }
    }

    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  return { stats, error };
}

function LiveDot({ active }) {
  return <span className={`live-dot ${active ? "live-dot--on" : ""}`} aria-hidden="true" />;
}

function StatsPanel({ stats, error }) {
  const latestWindow = stats?.summary_by_window?.at(-1);
  const rate = latestWindow?.events_per_sec ?? 0;
  const pulseSpeed = Math.max(0.4, 1.6 - rate / 10); // faster pulse at higher throughput

  const maxTypeCount = Math.max(1, ...(stats?.event_type_totals || []).map((e) => Number(e.event_count)));
  const maxPageCount = Math.max(1, ...(stats?.top_pages || []).map((p) => Number(p.event_count)));

  return (
    <aside className="stats-panel">
      <div className="stats-header">
        <div>
          <h2>Live stream</h2>
          <p className="stats-sub">last {CONTEXT_MINUTES} min · window 30s</p>
        </div>
        <div className="live-indicator">
          <LiveDot active={!!latestWindow} />
          <span>LIVE</span>
        </div>
      </div>

      <div className="pulse-track" style={{ "--pulse-duration": `${pulseSpeed}s` }}>
        <div className="pulse-fill" />
      </div>

      {error && <p className="stats-error">Can't reach the API: {error}</p>}

      <div className="metric-row">
        <div className="metric-card">
          <span className="metric-label">events / sec</span>
          <span className="metric-value">{rate ? rate.toFixed(1) : "—"}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">unique users</span>
          <span className="metric-value">{latestWindow?.unique_users ?? "—"}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">last window</span>
          <span className="metric-value metric-value--mono">
            {latestWindow ? formatTime(latestWindow.window_end) : "—"}
          </span>
        </div>
      </div>

      <div className="stats-section">
        <h3>Event mix</h3>
        <ul className="bar-list">
          {(stats?.event_type_totals || []).map((row) => (
            <li key={row.event_type}>
              <div className="bar-list-labels">
                <span>{EVENT_LABELS[row.event_type] || row.event_type}</span>
                <span className="mono">{row.event_count}</span>
              </div>
              <div className="bar-track">
                <div
                  className={`bar-fill bar-fill--${row.event_type}`}
                  style={{ width: `${(Number(row.event_count) / maxTypeCount) * 100}%` }}
                />
              </div>
            </li>
          ))}
          {!stats?.event_type_totals?.length && <li className="empty">Waiting for events…</li>}
        </ul>
      </div>

      <div className="stats-section">
        <h3>Top pages</h3>
        <ul className="bar-list">
          {(stats?.top_pages || []).slice(0, 6).map((row) => (
            <li key={row.page}>
              <div className="bar-list-labels">
                <span className="mono">{row.page}</span>
                <span className="mono">{row.event_count}</span>
              </div>
              <div className="bar-track">
                <div
                  className="bar-fill bar-fill--page"
                  style={{ width: `${(Number(row.event_count) / maxPageCount) * 100}%` }}
                />
              </div>
            </li>
          ))}
          {!stats?.top_pages?.length && <li className="empty">Waiting for events…</li>}
        </ul>
      </div>
    </aside>
  );
}

function ChatPanel() {
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content:
        "Ask me anything about what's happening in the stream right now — e.g. \"what's trending in the last 10 minutes?\" or \"any unusual spikes?\"",
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, loading]);

  async function handleSubmit(e) {
    e.preventDefault();
    const question = input.trim();
    if (!question || loading) return;

    setMessages((prev) => [...prev, { role: "user", content: question }]);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch(`${API_BASE}/api/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, minutes: CONTEXT_MINUTES }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `request failed (${res.status})`);
      setMessages((prev) => [...prev, { role: "assistant", content: data.answer }]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Something went wrong: ${err.message}`, isError: true },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="chat-panel">
      <header className="chat-header">
        <h2>Ask the stream</h2>
        <p className="stats-sub">answers are grounded in the last {CONTEXT_MINUTES} minutes of aggregates</p>
      </header>

      <div className="chat-scroll" ref={scrollRef}>
        {messages.map((m, i) => (
          <div key={i} className={`bubble bubble--${m.role} ${m.isError ? "bubble--error" : ""}`}>
            {m.content}
          </div>
        ))}
        {loading && (
          <div className="bubble bubble--assistant bubble--loading">
            <span className="typing-dot" />
            <span className="typing-dot" />
            <span className="typing-dot" />
          </div>
        )}
      </div>

      <form className="chat-form" onSubmit={handleSubmit}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="What's happening in the stream?"
          disabled={loading}
        />
        <button type="submit" disabled={loading || !input.trim()}>
          Ask
        </button>
      </form>
    </section>
  );
}

export default function App() {
  const { stats, error } = useStats();

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="brand">
          <span className="brand-mark">◈</span>
          <div>
            <h1>Streaming Insights Q&amp;A</h1>
            <p>Kafka → Flink → Postgres → Claude</p>
          </div>
        </div>
      </header>

      <main className="app-main">
        <StatsPanel stats={stats} error={error} />
        <ChatPanel />
      </main>
    </div>
  );
}
