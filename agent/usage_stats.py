"""
Splitrail-style Usage Statistics Layer

Provides Hermes with usage tracking tools that mirror the Splitrail MCP interface:
  - get_daily_stats   : token/cost stats per day, with optional date range filter
  - get_model_usage   : per-model breakdown of tokens, sessions, cost
  - get_cost_breakdown: cost by day across a date range, with running total
  - compare_tools      : compare usage across Hermes vs other known AI coding tools
  - list_analyzers    : list available data-source analyzers (self, plus Splitrail agents)

Data source: SQLite session store via InsightsEngine.
Compatible with Splitrail Cloud's MCP tool interface, enabling future cross-
platform aggregation if/when Splitrail adds a Hermes analyzer or public ingestion API.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

from typing import Any, Dict, List, Optional

from agent.insights import InsightsEngine, _estimate_cost


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse YYYY-MM-DD string to UTC datetime (start of day)."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _day_key(ts: float) -> str:
    """Unix timestamp -> 'YYYY-MM-DD' string in UTC."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def _build_engine() -> InsightsEngine:
    """Build an InsightsEngine connected to the session SQLite DB."""
    from hermes_state import SessionDB
    db = SessionDB()
    return InsightsEngine(db)


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


def get_daily_stats(
    days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: Optional[str] = None,
) -> str:
    """
    Query daily token/cost/session stats.

    Mirrors Splitrail's ``get_daily_stats`` MCP tool.

    Args:
        days: Number of past days to include (default 7). Ignored if start/end given.
        start_date: YYYY-MM-DD start date (inclusive).
        end_date: YYYY-MM-DD end date (inclusive).
        source: Filter by platform source (e.g. 'telegram', 'cli').
    """
    engine = _build_engine()
    report = engine.generate(days=days, source=source)

    if report.get("empty"):
        return json.dumps({
            "days": report.get("days", days),
            "daily": [],
            "summary": {
                "total_tokens": 0,
                "total_cost_usd": 0.0,
                "total_sessions": 0,
            },
        })

    # Compute per-day breakdown from sessions
    sessions = engine._get_sessions(
        cutoff=0.0,  # we filter by date below
        source=source,
    )

    start_dt = _parse_date(start_date)
    end_dt = _parse_date(end_date)

    if not start_dt:
        # default: last `days` days
        days_ago = _utcnow().timestamp() - (days * 86400)
        start_dt = datetime.fromtimestamp(days_ago, tz=timezone.utc)
    if not end_dt:
        end_dt = _utcnow()

    # Group sessions by day
    by_day: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "input_tokens": 0, "output_tokens": 0,
        "cache_read_tokens": 0, "cache_write_tokens": 0,
        "total_tokens": 0, "sessions": 0,
        "tool_calls": 0, "messages": 0, "cost_usd": 0.0,
    })

    for s in sessions:
        started = s.get("started_at")
        if not started:
            continue
        dt = datetime.fromtimestamp(started, tz=timezone.utc)
        if not (start_dt <= dt <= end_dt):
            continue
        day = dt.strftime("%Y-%m-%d")
        d = by_day[day]
        inp = s.get("input_tokens") or 0
        out = s.get("output_tokens") or 0
        cr = s.get("cache_read_tokens") or 0
        cw = s.get("cache_write_tokens") or 0
        d["input_tokens"] += inp
        d["output_tokens"] += out
        d["cache_read_tokens"] += cr
        d["cache_write_tokens"] += cw
        d["total_tokens"] += inp + out + cr + cw
        d["sessions"] += 1
        d["tool_calls"] += s.get("tool_call_count") or 0
        d["messages"] += s.get("message_count") or 0
        est, _ = _estimate_cost(s)
        d["cost_usd"] += est

    daily = []
    for day in sorted(by_day.keys()):
        d = by_day[day]
        daily.append({
            "date": day,
            "input_tokens": d["input_tokens"],
            "output_tokens": d["output_tokens"],
            "cache_read_tokens": d["cache_read_tokens"],
            "cache_write_tokens": d["cache_write_tokens"],
            "total_tokens": d["total_tokens"],
            "sessions": d["sessions"],
            "tool_calls": d["tool_calls"],
            "messages": d["messages"],
            "cost_usd": round(d["cost_usd"], 4),
        })

    o = report.get("overview", {})
    return json.dumps({
        "days": len(daily),
        "daily": daily,
        "summary": {
            "total_tokens": o.get("total_tokens", 0),
            "total_input_tokens": o.get("total_input_tokens", 0),
            "total_output_tokens": o.get("total_output_tokens", 0),
            "total_cache_read_tokens": o.get("total_cache_read_tokens", 0),
            "total_cache_write_tokens": o.get("total_cache_write_tokens", 0),
            "total_cost_usd": round(o.get("estimated_cost", 0.0), 4),
            "total_sessions": o.get("total_sessions", 0),
            "total_messages": o.get("total_messages", 0),
            "total_tool_calls": o.get("total_tool_calls", 0),
            "avg_cost_per_session": round(
                o.get("estimated_cost", 0.0) / max(o.get("total_sessions", 1), 1), 4
            ),
        },
    })


def get_model_usage(
    days: int = 30,
    source: Optional[str] = None,
) -> str:
    """
    Per-model breakdown of token usage, sessions, and cost.

    Mirrors Splitrail's ``get_model_usage`` MCP tool.
    """
    engine = _build_engine()
    report = engine.generate(days=days, source=source)

    if report.get("empty"):
        return json.dumps({"models": [], "days": days})

    models = []
    for m in report.get("models", []):
        models.append({
            "model": m["model"],
            "sessions": m["sessions"],
            "input_tokens": m["input_tokens"],
            "output_tokens": m["output_tokens"],
            "cache_read_tokens": m["cache_read_tokens"],
            "cache_write_tokens": m["cache_write_tokens"],
            "total_tokens": m["total_tokens"],
            "tool_calls": m["tool_calls"],
            "cost_usd": round(m.get("cost", 0.0), 4),
            "has_pricing": m.get("has_pricing", False),
        })

    # Sort by total_tokens descending
    models.sort(key=lambda x: x["total_tokens"], reverse=True)
    return json.dumps({"models": models, "days": days})


def get_cost_breakdown(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: Optional[str] = None,
) -> str:
    """
    Daily cost breakdown across a date range with running total.

    Mirrors Splitrail's ``get_cost_breakdown`` MCP tool.
    """
    stats_json = get_daily_stats(
        days=days,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )
    stats = json.loads(stats_json)

    running_total = 0.0
    breakdown = []
    for entry in stats.get("daily", []):
        running_total += entry["cost_usd"]
        breakdown.append({
            "date": entry["date"],
            "cost_usd": round(entry["cost_usd"], 4),
            "cumulative_cost_usd": round(running_total, 4),
            "sessions": entry["sessions"],
            "tokens": entry["total_tokens"],
        })

    return json.dumps({
        "breakdown": breakdown,
        "total_cost_usd": round(running_total, 4),
        "days": len(breakdown),
        "source": source or "all",
    })


def compare_tools(
    days: int = 30,
) -> str:
    """
    Compare usage across Hermes with other known AI coding tools.

    This is Hermes-specific: Splitrail compares Claude Code / Codex / Cline etc.
    Here we compare Hermes sessions across platforms (telegram, cli, etc.) and
    report on tool call distributions, which is the closest Hermes analogue.

    Mirrors Splitrail's ``compare_tools`` MCP tool.
    """
    engine = _build_engine()
    report = engine.generate(days=days)

    if report.get("empty"):
        return json.dumps({"tools": [], "days": days})

    # Hermes itself as a "tool" — broken down by platform
    platforms = report.get("platforms", [])
    total_sessions = sum(p.get("sessions", 0) for p in platforms)
    total_tokens = sum(p.get("tokens", 0) for p in platforms)
    total_cost = report.get("overview", {}).get("estimated_cost", 0.0)

    tool_entries = []

    # Hermes (self)
    hermes_sessions = sum(p.get("sessions", 0) for p in platforms)
    hermes_tokens = sum(p.get("tokens", 0) for p in platforms)
    tool_entries.append({
        "name": "hermes",
        "display_name": "Hermes Agent",
        "sessions": hermes_sessions,
        "tokens": hermes_tokens,
        "cost_usd": round(total_cost, 4),
        "占比": round(hermes_sessions / max(total_sessions, 1) * 100, 1),
        "has_pricing": True,
    })

    # Top tools within Hermes
    for t in report.get("tools", [])[:10]:
        tool_entries.append({
            "name": t["tool"],
            "display_name": t["tool"],
            "sessions": hermes_sessions,  # tools are within sessions
            "tokens": hermes_tokens,
            "cost_usd": 0.0,
            "calls": t["count"],
            "percentage": round(t["percentage"], 1),
            "占比": round(t["percentage"], 1),
            "has_pricing": False,
        })

    return json.dumps({
        "tools": tool_entries,
        "days": days,
        "total_sessions": total_sessions,
        "total_tokens": total_tokens,
    })


def list_analyzers() -> str:
    """
    List available usage data-source analyzers.

    Mirrors Splitrail's ``list_analyzers`` MCP tool.
    Returns Hermes as the primary analyzer, plus a note about Splitrail
    compatibility when the JSONL exporter is enabled.
    """
    return json.dumps({
        "analyzers": [
            {
                "name": "hermes",
                "display_name": "Hermes Agent",
                "description": "Native Hermes session store (SQLite). Tracks tokens, cost, tool usage, session metadata across all platforms.",
                "supported_tools": ["get_daily_stats", "get_model_usage", "get_cost_breakdown", "compare_tools", "list_analyzers"],
                "supports_cloud_sync": False,
                "local_format": "sqlite",
            },
            {
                "name": "splitrail-jsonl",
                "display_name": "Splitrail JSONL Exporter",
                "description": "Exports Hermes session data to Splitrail-compatible JSONL files under ~/.hermes/usage/. Enable via config: insights.jsonl_export = true",
                "supported_tools": ["get_daily_stats", "get_model_usage", "get_cost_breakdown", "compare_tools"],
                "supports_cloud_sync": False,
                "local_format": "jsonl",
            },
        ],
        "active": "hermes",
    })
