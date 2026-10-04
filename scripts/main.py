"""
scripts/main.py

Main entrypoint for Congress data ingestion.
Supports interactive database setup and CLI options for flexible data scoping (--limit, --full, --random, --members-only, --skip-members).
"""

import argparse
from pathlib import Path
import sys

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    DB_PATH,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from pipeline import prompt_reset_tables, start_pipeline

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
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",  # Avoids conflict with Python's 'async' keyword
        help="Run ingestion using async HTTP workers and streaming queue.",
    )

    parser.add_argument(
        "--members-only",
        action="store_true",
        help="Only fetch and update raw_members metadata; skip bills and amendments.",
    )
    parser.add_argument(
        "--skip-members",
        action="store_true",
        help="Skip fetching member metadata and read existing members directly from DuckDB to fetch bills.",
    )


    return parser.parse_args()


def main():
    args = parse_args()
    start_pipeline(args)




if __name__ == "__main__":
    main()
