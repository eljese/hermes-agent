#!/usr/bin/env python3
"""
Usage Statistics Tool — Splitrail-style MCP tools for Hermes Agent.

Exposes:
  - get_daily_stats    : token/cost stats per day, date-range filterable
  - get_model_usage    : per-model breakdown of tokens, sessions, cost
  - get_cost_breakdown : daily cost with running total
  - compare_tools      : Hermes vs other tools (platform breakdown)
  - list_analyzers     : available analyzer sources

Mirrors the Splitrail MCP server interface so that:
  1. Jorma can answer "what did I spend on tokens today?" natively
  2. An external Splitrail installation could call Hermes as an MCP server
     (Option C path — future work)
  3. JSONL export preserves Option C compatibility without cloud dependency
"""

import json
import logging
from typing import Any, Dict, Optional

from tools.registry import registry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool schemas — mirror Splitrail MCP tool signatures
# ---------------------------------------------------------------------------

GET_DAILY_STATS_SCHEMA = {
    "name": "get_daily_stats",
    "description": (
        "Query daily token/cost/session statistics from Hermes.\n\n"
        "Returns per-day breakdowns of input tokens, output tokens, cache tokens, "
        "session counts, tool call counts, and estimated USD cost.\n\n"
        "Examples:\n"
        "  'Show me my last 7 days of usage' → days=7\n"
        "  'What did I spend in March?' → start_date='2026-03-01', end_date='2026-03-31'\n"
        "  'Filter by Telegram sessions only' → source='telegram'"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of past days to include (default: 7). Ignored if start/end date provided.",
                "default": 7,
            },
            "start_date": {
                "type": "string",
                "description": "Start date in YYYY-MM-DD format (inclusive). If not provided, defaults to `days` ago.",
            },
            "end_date": {
                "type": "string",
                "description": "End date in YYYY-MM-DD format (inclusive). If not provided, defaults to today.",
            },
            "source": {
                "type": "string",
                "description": "Filter by platform source (e.g. 'telegram', 'cli', 'discord'). Leave empty for all.",
            },
        },
        "required": [],
    },
}

GET_MODEL_USAGE_SCHEMA = {
    "name": "get_model_usage",
    "description": (
        "Per-model breakdown of token usage, sessions, and cost over a time window.\n\n"
        "Shows which models were used, how many tokens each consumed, session counts, "
        "tool call counts, and estimated USD cost — sorted by total tokens descending.\n\n"
        "Examples:\n"
        "  'Which models am I using most?' → days=30\n"
        "  'How much did Claude cost me last month?' → days=30, filter output for 'claude'"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of past days to analyze (default: 30).",
                "default": 30,
            },
            "source": {
                "type": "string",
                "description": "Filter by platform source. Leave empty for all sources.",
            },
        },
        "required": [],
    },
}

GET_COST_BREAKDOWN_SCHEMA = {
    "name": "get_cost_breakdown",
    "description": (
        "Daily cost breakdown across a date range with a running cumulative total.\n\n"
        "Use this to track spend velocity (is cost growing? flat? declining?) over time.\n\n"
        "Examples:\n"
        "  'Track my spend over the last 30 days' → days=30\n"
        "  'How much did I spend in Q1 2026?' → start_date='2026-01-01', end_date='2026-03-31'"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of past days (default: 30).",
                "default": 30,
            },
            "start_date": {
                "type": "string",
                "description": "Start date YYYY-MM-DD.",
            },
            "end_date": {
                "type": "string",
                "description": "End date YYYY-MM-DD.",
            },
            "source": {
                "type": "string",
                "description": "Filter by platform source.",
            },
        },
        "required": [],
    },
}

COMPARE_TOOLS_SCHEMA = {
    "name": "compare_tools",
    "description": (
        "Compare usage across Hermes Agent and other AI coding tools.\n\n"
        "On Hermes, this means: Hermes sessions across platforms (Telegram, CLI, Discord...) "
        "and the top tool calls within those sessions. "
        "Shows session share, token volume, cost, and call distribution.\n\n"
        "This is the Hermes equivalent of what Splitrail does when comparing "
        "Claude Code vs Cline vs Roo Code on the same machine."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of past days to analyze (default: 30).",
                "default": 30,
            },
        },
        "required": [],
    },
}

LIST_ANALYZERS_SCHEMA = {
    "name": "list_analyzers",
    "description": (
        "List available usage data-source analyzers.\n\n"
        "Returns the set of backends that can answer usage queries. "
        "Currently: Hermes native (SQLite session store) and the Splitrail JSONL exporter. "
        "The JSONL exporter enables future cross-tool aggregation if Splitrail adds "
        "a Hermes-compatible local file analyzer."
    ),
    "parameters": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

EXPORT_USAGE_SCHEMA = {
    "name": "export_usage",
    "description": (
        "Export Hermes session usage data to Splitrail-compatible JSONL files.\n\n"
        "Writes one JSON object per session to ~/.hermes/usage/sessions_YYYY-MM-DD.jsonl.\n"
        "Files are appended to (idempotent on re-runs). "
        "These JSONL files can later be consumed by Splitrail Cloud or any external "
        "usage analysis tool.\n\n"
        "Examples:\n"
        "  'Export today' → days=1 (default)\n"
        "  'Export last 7 days' → days=7\n"
        "  'Export to a specific file' → output_path='/tmp/my_usage.jsonl'"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "days": {
                "type": "integer",
                "description": "Number of past days to export (default: 1 = today).",
                "default": 1,
            },
            "output_path": {
                "type": "string",
                "description": "Optional explicit output file path. If not provided, defaults to ~/.hermes/usage/sessions_YYYY-MM-DD.jsonl.",
            },
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Availability check — always available when hermes_state.db exists
# ---------------------------------------------------------------------------

def check_usage_stats_requirements() -> bool:
    """Usage stats are always available; the DB may be empty but not unavailable."""
    try:
        from hermes_constants import get_hermes_home
        db_path = get_hermes_home() / "hermes_state.db"
        return db_path.exists()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

from agent.usage_stats import (
    get_daily_stats as _get_daily_stats,
    get_model_usage as _get_model_usage,
    get_cost_breakdown as _get_cost_breakdown,
    compare_tools as _compare_tools,
    list_analyzers as _list_analyzers,
)
from agent.usage_exporter import export_sessions as _export_sessions


def handle_get_daily_stats(args: Dict[str, Any], **kw) -> str:
    return _get_daily_stats(
        days=args.get("days", 7),
        start_date=args.get("start_date"),
        end_date=args.get("end_date"),
        source=args.get("source"),
    )


def handle_get_model_usage(args: Dict[str, Any], **kw) -> str:
    return _get_model_usage(
        days=args.get("days", 30),
        source=args.get("source"),
    )


def handle_get_cost_breakdown(args: Dict[str, Any], **kw) -> str:
    return _get_cost_breakdown(
        days=args.get("days", 30),
        start_date=args.get("start_date"),
        end_date=args.get("end_date"),
        source=args.get("source"),
    )


def handle_compare_tools(args: Dict[str, Any], **kw) -> str:
    return _compare_tools(days=args.get("days", 30))


def handle_list_analyzers(args: Dict[str, Any], **kw) -> str:
    return _list_analyzers()


def handle_export_usage(args: Dict[str, Any], **kw) -> str:
    import json as _json
    output_path = args.get("output_path")
    if output_path:
        from pathlib import Path
        output_path = Path(output_path)
    result = _export_sessions(
        days=args.get("days", 1),
        output_file=output_path,
    )
    return _json.dumps(result)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

registry.register(
    name="get_daily_stats",
    toolset="usage_stats",
    schema=GET_DAILY_STATS_SCHEMA,
    handler=handle_get_daily_stats,
    check_fn=check_usage_stats_requirements,
    emoji="",
)

registry.register(
    name="get_model_usage",
    toolset="usage_stats",
    schema=GET_MODEL_USAGE_SCHEMA,
    handler=handle_get_model_usage,
    check_fn=check_usage_stats_requirements,
    emoji="",
)

registry.register(
    name="get_cost_breakdown",
    toolset="usage_stats",
    schema=GET_COST_BREAKDOWN_SCHEMA,
    handler=handle_get_cost_breakdown,
    check_fn=check_usage_stats_requirements,
    emoji="",
)

registry.register(
    name="compare_tools",
    toolset="usage_stats",
    schema=COMPARE_TOOLS_SCHEMA,
    handler=handle_compare_tools,
    check_fn=check_usage_stats_requirements,
    emoji="",
)

registry.register(
    name="list_analyzers",
    toolset="usage_stats",
    schema=LIST_ANALYZERS_SCHEMA,
    handler=handle_list_analyzers,
    check_fn=check_usage_stats_requirements,
    emoji="",
)

registry.register(
    name="export_usage",
    toolset="usage_stats",
    schema=EXPORT_USAGE_SCHEMA,
    handler=handle_export_usage,
    check_fn=check_usage_stats_requirements,
    emoji="",
)
