"""
scripts/main.py

Main entrypoint for Congress data ingestion.
Supports interactive database setup and CLI options for flexible data scoping (--limit, --full, --random, --bills-per-member).
"""

import argparse
from pathlib import Path
import sys
import time

import duckdb

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import (
    fetch_amendments_for_bill,
    fetch_legislation_for_member,
    fetch_members_for_districts,
)
from config import (
    BILLS_PER_MEMBER as DEFAULT_BILLS_PER_MEMBER,
    DB_PATH,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from database import (
    check_tables_exist,
    create_tables,
    get_existing_bill_timestamps,
    get_target_districts,
    validate_seed_tables,
)
from ingestion import load_amendments, load_bills, load_members


# ── Prompt Helper ─────────────────────────────────────────────────────────────

def prompt_reset_tables() -> bool:
    """
    Prompt the user to recreate/reset raw tables.
    Only accepts 'y', 'yes', 'n', 'no' (case-insensitive).
    Re-prompts until valid input is given.
    """
    while True:
        choice = input("Recreate/reset raw tables? (y/n): ").strip().lower()
        if choice in ("y", "yes"):
            return True
        elif choice in ("n", "no"):
            return False
        print("Invalid input. Please enter 'y' (yes) or 'n' (no).")


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_database(db, reset: bool | None = None):
    """
    Initialize database tables.
    If reset is explicitly provided (True/False via CLI), use it.
    Otherwise, interactively prompt the user.
    """
    if reset is None:
        should_reset = prompt_reset_tables()
    else:
        should_reset = reset

    if should_reset:
        print("Recreating raw tables (reset mode)...")
        create_tables(db, replace=True)
    else:
        print("Ensuring raw tables exist (incremental/preserve mode)...")
        create_tables(db, replace=False)


# ── Ingestion ─────────────────────────────────────────────────────────────────

def run_ingestion(
    db,
    member_limit: int | None = DEFAULT_MEMBER_LIMIT,
    bills_per_member: int | None = DEFAULT_BILLS_PER_MEMBER,
    random_sample: bool = False,
):
    """Fetch data from Congress API and ingest into database tables incrementally."""
    start_time = time.time()

    # Validate prerequisite tables before running
    if not validate_seed_tables(db):
        raise RuntimeError(
            "Prerequisite seed tables ('target_counties', 'raw_census__cd11920_county20') are missing. "
            "Please run 'uv run dbt seed' from the dbt/ directory first."
        )

    if not check_tables_exist(db, ["raw_members", "raw_bills", "raw_amendments"]):
        print("Raw destination tables missing. Creating raw tables...")
        create_tables(db, replace=False)

    print(f"\nReading target districts from seed tables (random={random_sample})...")
    target_districts = get_target_districts(db, shuffle=random_sample)

    # Load existing bill timestamps for incremental change detection
    existing_bill_timestamps = get_existing_bill_timestamps(db)

    limit_desc = "ALL" if member_limit is None else str(member_limit)
    print(f"\nFetching members (limit={limit_desc}, random={random_sample})...")
    members = fetch_members_for_districts(target_districts, member_limit=member_limit)
    load_members(db, members)

    # Track processed bills in current session to avoid duplicate work
    processed_bills = set()
    skipped_amendments = 0
    fetched_amendments = 0
    total_bills_processed = 0

    for idx, member in enumerate(members, start=1):
        bid = member["bioguideId"]
        member_start = time.time()
        print(f"\n[{idx}/{len(members)}] Processing {member.get('name', bid)}...")

        sponsored, cosponsored = fetch_legislation_for_member(bid)
        print(f"  {len(sponsored)} sponsored, {len(cosponsored)} cosponsored bills")

        load_bills(db, sponsored, bid, "sponsor")
        load_bills(db, cosponsored, bid, "cosponsor")

        all_bills = sponsored + cosponsored
        if bills_per_member is not None:
            all_bills = all_bills[:bills_per_member]

        for bill in all_bills:
            total_bills_processed += 1
            congress = bill.get("congress")
            bill_type = bill.get("type", "").upper()
            bill_number = str(bill.get("number"))
            key = (congress, bill_type, bill_number)

            if key in processed_bills:
                continue
            processed_bills.add(key)

            # Check if bill was updated since previous ingestion
            current_update = bill.get("updateDate") or bill.get("updateDateIncludingText")
            previous_update = existing_bill_timestamps.get(key)

            if previous_update and current_update and current_update <= previous_update:
                skipped_amendments += 1
                continue

            fetched_amendments += 1
            amendments = fetch_amendments_for_bill(congress, bill_type, bill_number)
            print(f"  {congress} - {bill_type}-{bill_number}: {len(amendments)} amendments")
            load_amendments(db, amendments, congress, bill_type, bill_number)

        print(f"  Completed member in {time.time() - member_start:.2f}s")

    elapsed = time.time() - start_time
    print(f"\n── Ingestion Summary ──")
    print(f"  Total time elapsed: {elapsed:.2f}s")
    print(f"  Members processed: {len(members)}")
    print(f"  Unique bills tracked: {len(processed_bills)}")
    print(f"  Amendment calls made: {fetched_amendments}")
    print(f"  Amendment calls skipped (unchanged): {skipped_amendments}")

    print("\n── Tables in dev.duckdb ──")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")


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
        "--bills-per-member",
        type=int,
        default=DEFAULT_BILLS_PER_MEMBER,
        help="Cap bills processed per member (useful for fast testing).",
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

    member_limit = None if args.full else args.limit
    bills_per_member = args.bills_per_member
    random_sample = args.random

    db = duckdb.connect(str(DB_PATH))

    # Setup phase (table initialization/reset)
    setup_database(db, reset=args.reset)

    # Ingestion phase
    run_ingestion(
        db,
        member_limit=member_limit,
        bills_per_member=bills_per_member,
        random_sample=random_sample,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")


if __name__ == "__main__":
    main()
