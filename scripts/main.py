"""
scripts/main.py

Main entrypoint for Congress data ingestion.
Supports interactive database setup and CLI options for flexible data scoping (--limit, --full, --random).
"""

import argparse
from pathlib import Path
import sys

from pipeline import start_pipeline

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    DB_PATH,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)

# ── CLI & Main ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Congress data into DuckDB for downstream dbt transformations."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run ingestion for all target district members (no limit).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_MEMBER_LIMIT,
        help=f"Cap the number of members to ingest (default: {DEFAULT_MEMBER_LIMIT}). Ignored if --full is set.",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Randomly sample districts/members up to --limit instead of deterministic order.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        default=None,
        help="Force table recreation/reset without prompting.",
    )
    parser.add_argument(
        "--no-reset",
        action="store_false",
        dest="reset",
        help="Skip table reset without prompting (preserve existing data).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_pipeline(args)




if __name__ == "__main__":
    main()
