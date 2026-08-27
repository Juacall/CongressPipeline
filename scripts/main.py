"""
scripts/main.py

Traversal strategy: district-first with incremental timestamp check and checksum tracking.
"""

from pathlib import Path
import sys

import duckdb

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import (
    fetch_amendments_for_bill,
    fetch_legislation_for_member,
    fetch_members_for_districts,
)
from config import (
    BILLS_PER_MEMBER,
    DB_PATH,
    MEMBER_LIMIT,
    STATE_FIPS_TO_ABBR,
)
from database import create_tables, get_existing_bill_timestamps, get_target_districts
from ingestion import load_amendments, load_bills, load_members


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_database(db):
    """Initialize database tables, optionally prompting for full reset."""
    choice = input("Recreate/reset raw tables? (y/N): ").strip().lower()
    if choice in ("y", "yes"):
        print("Recreating raw tables (reset mode)...")
        create_tables(db, replace=True)
    else:
        print("Ensuring raw tables exist (incremental/preserve mode)...")
        create_tables(db, replace=False)

# ── Ingestion ─────────────────────────────────────────────────────────────────

def run_ingestion(db):
    """Fetch data from Congress API and ingest into database tables incrementally."""
    print("\nReading target districts from seed tables...")
    target_districts = get_target_districts(db)

    # Load existing bill timestamps for incremental change detection
    existing_bill_timestamps = get_existing_bill_timestamps(db)

    print(f"\nFetching members (limit={MEMBER_LIMIT})...")
    members = fetch_members_for_districts(target_districts, member_limit=MEMBER_LIMIT)
    load_members(db, members)

    # Track processed bills in current session to avoid duplicate work
    processed_bills = set()
    skipped_amendments = 0
    fetched_amendments = 0

    for member in members:
        bid = member["bioguideId"]
        print(f"\nProcessing {member.get('name', bid)}...")

        sponsored, cosponsored = fetch_legislation_for_member(bid)
        print(f"  {len(sponsored)} sponsored, {len(cosponsored)} cosponsored bills")

        load_bills(db, sponsored, bid, "sponsor")
        load_bills(db, cosponsored, bid, "cosponsor")

        all_bills = sponsored + cosponsored
        if BILLS_PER_MEMBER is not None:
            all_bills = all_bills[:BILLS_PER_MEMBER]

        for bill in all_bills:
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
            print(f"{congress} - {bill_type}-{bill_number}: {len(amendments)} amendments")
            load_amendments(db, amendments, congress, bill_type, bill_number)

    print(f"\nAmendments summary: {fetched_amendments} fetched, {skipped_amendments} unchanged (skipped)")

    print("\n── Tables in dev.duckdb ──")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    db = duckdb.connect(str(DB_PATH))

    # Setup phase (table initialization/reset)
    setup_database(db)

    # Ingestion phase
    run_ingestion(db)

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")


if __name__ == "__main__":
    main()
