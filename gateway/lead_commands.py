"""
Lead Kanban and HIL Feedback Commands for Proventia Market Intel.
Handlers for /lead and /feedback slash commands via Telegram.
"""

import json
import re
import subprocess
from pathlib import Path
from typing import Optional

from gateway.platforms.base import MessageEvent

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VAULT_BASE = Path("/home/eljese/.hermes/compliance-tracker/market-research")
KANBAN_FILE = Path("/home/eljese/.hermes/compliance-tracker/kanban/boards/main.md")
FEEDBACK_FILE = Path("/home/eljese/.hermes/compliance-tracker/patrol_feedback.json")
PY_OPS = Path("/home/eljese/.hermes/compliance-tracker/patrol_hil_feedback.py")

LANE_LABELS = {
    "inbox-leads": "Inbox-Leads",
    "review-jesse": "Review-Jesse",
    "contacted": "Contacted",
    "accepted": "Accepted",
    "rejected": "Rejected",
    "done-leads": "Done-Leads",
}

LANES = list(LANE_LABELS.keys())


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def find_lead_file(partial: str, all_leads: list[str]) -> Optional[str]:
    if partial.endswith(".md"):
        return next((l for l in all_leads if l == partial), None)
    p = partial.lower()
    matches = [l for l in all_leads if p in l.lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Try word-boundary match
        for m in matches:
            mp = m.replace(".md", "").replace("-", " ").replace("_", " ").lower()
            if p.replace("-", " ").replace("_", " ") in mp:
                return m
    return None


def get_all_leads() -> list[str]:
    leads = []
    for vert_dir in VAULT_BASE.iterdir():
        if vert_dir.is_dir():
            for md in vert_dir.glob("*.md"):
                leads.append(md.name)
    return leads


def run_ops(*args) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["python3", str(PY_OPS), *args],
            capture_output=True, text=True, timeout=15
        )
        return result.returncode, result.stdout + result.stderr
    except Exception as e:
        return 1, str(e)


# ---------------------------------------------------------------------------
# Card movement
# ---------------------------------------------------------------------------

def _move_card(filename: str, new_lane: str) -> str:
    """Move a lead card to a new lane in the kanban."""
    if new_lane not in LANES:
        valid = ", ".join(LANES)
        return f"Unknown lane: {new_lane}. Valid lanes: {valid}"

    code, out = run_ops("move", filename, new_lane)
    if code == 0 and "OK" in out:
        return f"Moved [{filename}] → {LANE_LABELS.get(new_lane, new_lane)}"
    return f"Could not move {filename}: {out[:200]}"


def _reject(filename: str) -> str:
    code, out = run_ops("reject", filename)
    if code == 0 and "OK" in out:
        return f"Rejected [{filename}]"
    return f"Could not reject {filename}: {out[:200]}"


def _accept(filename: str) -> str:
    code, out = run_ops("accept", filename)
    if code == 0 and "OK" in out:
        return f"Accepted [{filename}]"
    return f"Could not accept {filename}: {out[:200]}"


def _contact(filename: str) -> str:
    return _move_card(filename, "contacted")


# ---------------------------------------------------------------------------
# Feedback recording
# ---------------------------------------------------------------------------

def _record_feedback(args_str: str) -> str:
    """Parse and record HIL feedback from args like 'f1.md good | f2.md bad'."""
    if not args_str.strip():
        return "Usage: /feedback <filename.md good|bad|invalid [...]>"

    code, out = run_ops(args_str)
    if code == 0:
        try:
            parsed = json.loads(out)
            applied = parsed.get("feedback_applied", 0)
            entries = parsed.get("entries", {})
            lines = [f"Feedback recorded ({applied} lead(s)):"]
            for fn, q in entries.items():
                lines.append(f"  • {fn}: {q}")
            return "\n".join(lines)
        except Exception:
            return f"Feedback applied: {out[:300]}"
    return f"Feedback parse error: {out[:200]}"


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def _kanban_status() -> str:
    code, out = run_ops("status")
    return out if code == 0 else f"Status error: {out[:200]}"


# ---------------------------------------------------------------------------
# Main handlers
# ---------------------------------------------------------------------------

async def handle_lead_command(event: MessageEvent) -> str:
    """
    Handle /lead command.
    Usage:
      /lead status              — show all leads by lane
      /lead list                — JSON list of leads by lane
      /lead move <name> <lane>  — move lead card to lane
      /lead reject <name>        — move to rejected
      /lead accept <name>        — move to accepted
      /lead contact <name>       — move to contacted
      /lead inbox <name>         — move to inbox-leads
    """
    args = event.get_command_args().strip()
    parts = args.split()
    sub = parts[0] if parts else "status"
    rest = " ".join(parts[1:])

    all_leads = get_all_leads()

    if sub == "status":
        return _kanban_status()

    if sub == "list":
        code, out = run_ops("list")
        try:
            data = json.loads(out)
            lines = ["Leads by lane:", ""]
            for lane, leads in data.items():
                label = LANE_LABELS.get(lane, lane)
                lines.append(f"{label}: {len(leads)} lead(s)")
                for fn in leads:
                    lines.append(f"  • {fn}")
            return "\n".join(lines)
        except Exception:
            return out[:500]

    # Sub-commands that need a partial name
    if sub in ("move", "reject", "accept", "contact", "inbox"):
        if not rest:
            return f"Usage: /lead {sub} <partial-name>"

        # Lane argument for move
        if sub == "move":
            lane_part = rest.rsplit(" ", 1)[-1]
            name_part = rest.rsplit(" ", 1)[0]
            if lane_part in LANES:
                partial = name_part
                target_lane = lane_part
            else:
                partial = rest
                target_lane = "review-jesse"  # default
        else:
            partial = rest
            target_lane = None

        resolved = find_lead_file(partial, all_leads)
        if not resolved:
            return f"Could not find a lead matching '{partial}'. Try /lead status."

        if sub == "move":
            return _move_card(resolved, target_lane)
        elif sub == "reject":
            return _reject(resolved)
        elif sub == "accept":
            return _accept(resolved)
        elif sub == "contact":
            return _contact(resolved)
        elif sub == "inbox":
            return _move_card(resolved, "inbox-leads")

    return ("Unknown sub-command. Usage:\n"
            "/lead status\n"
            "/lead list\n"
            "/lead move <name> <lane>\n"
            "/lead reject <name>\n"
            "/lead accept <name>\n"
            "/lead contact <name>\n"
            "/lead inbox <name>")


async def handle_feedback_command(event: MessageEvent) -> str:
    """
    Handle /feedback command.
    Usage:
      /feedback f1.md good | f2.md bad
      /feedback Finferries good, Trafikverket bad
      /feedback f1.md good
    """
    args = event.get_command_args().strip()
    if not args:
        return ("Usage: /feedback <leadname.md good|bad|invalid [...]>\n"
                "Examples:\n"
                "  /feedback Finferries good\n"
                "  /feedback f1.md good | f2.md bad\n"
                "  /feedback f1.md, f2.md bad")
    return _record_feedback(args)
