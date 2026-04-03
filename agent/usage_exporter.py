"""
Splitrail-Compatible JSONL Usage Exporter

Exports Hermes session data to JSONL files that can be consumed by:
  1. Splitrail Cloud (if/when they add a Hermes-compatible local file analyzer)
  2. Any external usage tracking / cost analysis tool that reads JSONL

Output format (one JSON object per line):
  {
    "timestamp": "2026-04-03T18:00:00Z",      -- ISO8601 UTC
    "session_id": "abc123",                     -- Hermes session ID
    "model": "anthropic/claude-sonnet-4-7",    -- model name
    "input_tokens": 1234,                       -- prompt tokens
    "output_tokens": 567,                      -- completion tokens
    "cache_read_tokens": 890,                  -- cache read (Anthropic)
    "cache_write_tokens": 100,                 -- cache write (Anthropic)
    "total_tokens": 2791,                       -- sum of above
    "cost_usd": 0.0234,                         -- estimated USD
    "source": "telegram",                       -- platform (telegram|cli|discord...)
    "duration_seconds": 45,                     -- session duration
    "messages": 12,                            -- message count
    "tool_calls": 8,                           -- tool call count
    "tool_names": ["web_search", "terminal"], -- tools used
    "ended_at": "2026-04-03T18:00:45Z",       -- session end (ISO8601)
    "analyzer": "hermes",                       -- always "hermes" for our data
    "version": "1.0"                            -- format version
  }

Files are written to: ~/.hermes/usage/sessions_YYYY-MM-DD.jsonl
A daily cron job (hermes cron) can be set up to append each day's sessions.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from agent.insights import InsightsEngine, _estimate_cost

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def _usage_dir() -> Path:
    """Return the usage export directory, creating it if needed."""
    from hermes_constants import get_hermes_home
    d = get_hermes_home() / "usage"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# Core export logic
# ---------------------------------------------------------------------------

def _session_to_jsonl(session: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a session dict to a Splitrail-compatible JSONL record."""
    started = session.get("started_at")
    ended = session.get("ended_at")

    # Timestamps
    start_ts = datetime.fromtimestamp(started, tz=timezone.utc).isoformat() if started else None
    end_ts = datetime.fromtimestamp(ended, tz=timezone.utc).isoformat() if ended else None

    # Duration
    duration = (ended - started) if (started and ended and ended > started) else 0

    # Tokens
    inp = session.get("input_tokens") or 0
    out = session.get("output_tokens") or 0
    cr = session.get("cache_read_tokens") or 0
    cw = session.get("cache_write_tokens") or 0
    total = inp + out + cr + cw

    # Cost
    cost, _ = _estimate_cost(session)
    cost = round(cost, 6)

    # Tool names
    tool_names = session.get("tool_names") or []
    if isinstance(tool_names, str):
        try:
            tool_names = json.loads(tool_names)
        except Exception:
            tool_names = []

    return {
        "timestamp": start_ts,
        "session_id": session.get("id") or session.get("session_id", ""),
        "model": session.get("model") or "unknown",
        "input_tokens": inp,
        "output_tokens": out,
        "cache_read_tokens": cr,
        "cache_write_tokens": cw,
        "total_tokens": total,
        "cost_usd": cost,
        "source": session.get("source") or "unknown",
        "duration_seconds": round(duration, 1) if duration else 0,
        "messages": session.get("message_count") or 0,
        "tool_calls": session.get("tool_call_count") or 0,
        "tool_names": tool_names,
        "ended_at": end_ts,
        "analyzer": "hermes",
        "version": "1.0",
    }


def export_sessions(
    days: int = 1,
    output_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export Hermes sessions from the last N days to a JSONL file.

    Args:
        days: Number of past days to export (default 1 = today only).
        output_file: Optional explicit output path. If None, defaults to
            ~/.hermes/usage/sessions_YYYY-MM-DD.jsonl for each day.

    Returns:
        Dict with export statistics.
    """
    from hermes_state import SessionDB

    db = SessionDB()
    engine = InsightsEngine(db)

    cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
    sessions = engine._get_sessions(cutoff=cutoff, source=None)

    if not sessions:
        db.close()
        return {"exported": 0, "files": [], "days": days}

    records: List[Dict[str, Any]] = []
    for s in sessions:
        records.append(_session_to_jsonl(s))

    # Default output: one file per day
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return {
            "exported": len(records),
            "files": [str(output_file)],
            "days": days,
        }

    # Group by day → one file per day
    from collections import defaultdict
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        ts = rec.get("timestamp", "")
        day = ts[:10] if ts else "unknown"
        by_day[day].append(rec)

    written_files: List[str] = []
    for day, day_records in sorted(by_day.items()):
        path = _usage_dir() / f"sessions_{day}.jsonl"
        with path.open("a") as f:  # append (idempotent on re-runs)
            for rec in day_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        written_files.append(str(path))

    db.close()

    return {
        "exported": len(records),
        "files": written_files,
        "days": days,
        "by_day": {day: len(recs) for day, recs in sorted(by_day.items())},
    }


def export_all_sessions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export all sessions within a date range (or all time if no range given).

    Args:
        start_date: YYYY-MM-DD start date (inclusive).
        end_date: YYYY-MM-DD end date (inclusive).
        output_file: Single output file path. If None, one file per day.

    Returns:
        Dict with export statistics.
    """
    from hermes_state import SessionDB

    db = SessionDB()
    engine = InsightsEngine(db)

    # Get all sessions, filter in Python
    all_sessions = engine._get_sessions(cutoff=0.0, source=None)

    if not all_sessions:
        db.close()
        return {"exported": 0, "files": [], "start_date": start_date, "end_date": end_date}

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date else datetime.now(timezone.utc)

    records: List[Dict[str, Any]] = []
    for s in all_sessions:
        started = s.get("started_at")
        if not started:
            continue
        dt = datetime.fromtimestamp(started, tz=timezone.utc)
        if start_dt and dt < start_dt:
            continue
        if dt > end_dt:
            continue
        records.append(_session_to_jsonl(s))

    if not records:
        db.close()
        return {"exported": 0, "files": [], "start_date": start_date, "end_date": end_date}

    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        db.close()
        return {
            "exported": len(records),
            "files": [str(output_file)],
            "start_date": start_date,
            "end_date": end_date,
        }

    # Default: one file per day
    from collections import defaultdict
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        ts = rec.get("timestamp", "")
        day = ts[:10] if ts else "unknown"
        by_day[day].append(rec)

    written_files: List[str] = []
    for day, day_records in sorted(by_day.items()):
        path = _usage_dir() / f"sessions_{day}.jsonl"
        with path.open("a") as f:
            for rec in day_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        written_files.append(str(path))

    db.close()
    return {
        "exported": len(records),
        "files": written_files,
        "start_date": start_date,
        "end_date": end_date,
        "by_day": {day: len(recs) for day, recs in sorted(by_day.items())},
    }


def iter_jsonl_records(file_path: Path) -> Iterator[Dict[str, Any]]:
    """Read a JSONL file and yield records one at a time (memory-efficient)."""
    with file_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed line in %s", file_path)
                continue


def get_export_summary() -> Dict[str, Any]:
    """Return a summary of what's been exported to the usage directory."""
    usage_dir = _usage_dir()
    if not usage_dir.exists():
        return {"files": 0, "total_records": 0, "size_bytes": 0, "date_range": None}

    files = sorted(usage_dir.glob("sessions_*.jsonl"))
    total_records = 0
    earliest = None
    latest = None
    size_bytes = 0

    for f in files:
        size_bytes += f.stat().st_size
        for rec in iter_jsonl_records(f):
            total_records += 1
            ts = rec.get("timestamp", "")[:10] if rec.get("timestamp") else None
            if ts:
                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

    return {
        "files": len(files),
        "total_records": total_records,
        "size_bytes": size_bytes,
        "date_range": (earliest, latest) if earliest else None,
        "export_dir": str(usage_dir),
    }
