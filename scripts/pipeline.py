import argparse
from pathlib import Path
from helpers import parse_api_date
import sys
import time
# import asyncio
# import httpx
from datetime import datetime
import duckdb

# Bounded queue and concurrency limits to prevent memory bloat and rate-limiting
# QUEUE_MAX_SIZE = 500
# NUM_AMENDMENT_WORKERS = 5
# SEMAPHORE = asyncio.Semaphore(NUM_AMENDMENT_WORKERS)



# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import (
    fetch_amendments_for_bill,
    fetch_legislation_for_member,
    fetch_members_for_districts,
)
from config import (
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
    skipped_amendments = 0
    fetched_amendments = 0
    total_bills_processed = 0
    # 1. Stage 1: Collect & deduplicate unique bills in memory
    unique_bills = {}

    for idx, member in enumerate(members, start=1):
        bid = member["bioguideId"]
        member_start = time.time()
        print(f"\n[{idx}/{len(members)}] Processing {member.get('name', bid)}...")

        sponsored, cosponsored = fetch_legislation_for_member(bid)
        print(f"  {len(sponsored)} sponsored, {len(cosponsored)} cosponsored bills")

        load_bills(db, sponsored, bid, "sponsor")
        load_bills(db, cosponsored, bid, "cosponsor")

        for bill in sponsored + cosponsored:
            key = (
                bill.get("congress"),
                bill.get("type", "").upper(),
                str(bill.get("number")),
            )
            if key not in unique_bills:
                unique_bills[key] = bill
        print(f"  Completed member in {time.time() - member_start:.2f}s")

    for (congress, bill_type, bill_number), bill in unique_bills.items():
        total_bills_processed += 1

        # Check if bill was updated since previous ingestion
        current_update = parse_api_date(bill.get("updateDate") or bill.get("updateDateIncludingText"))
        previous_update = existing_bill_timestamps.get((congress,bill_type,bill_number))


        if previous_update and current_update and current_update <= previous_update:
            skipped_amendments += 1
            continue

        fetched_amendments += 1
        amendments = fetch_amendments_for_bill(congress, bill_type, bill_number)
        print(f"  {congress} - {bill_type}-{bill_number}: {len(amendments)} amendments")
        load_amendments(db, amendments, congress, bill_type, bill_number)

    elapsed = time.time() - start_time
    print(f"\n── Ingestion Summary ──")
    print(f"  Total time elapsed: {elapsed:.2f}s")
    print(f"  Members processed: {len(members)}")
    print(f"  Unique bills tracked: {len(unique_bills)}")
    print(f"  Total bills processed: {total_bills_processed}")
    print(f"  Amendment calls made: {fetched_amendments}")
    print(f"  Amendment calls skipped (unchanged): {skipped_amendments}")

    print("\n── Tables in dev.duckdb ──")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")

#---------------- Streaming Pipeline Flow ----------------------

# async def amendment_worker(
#         worker_id: int,
#         queue: asyncio.Queue,
#         client: httpx.AsyncClient,
#         db,
#         existing_timestamps: dict,
#         stats: dict,
# ):
#     """
#     Consumer Worker: Continuously pulls unique bills from the queue
#     and fetches their amendments.
#     """
#     while True:
#         item = await queue.get()
#         if item is None:  # Sentinel value signaling pipeline completion
#             queue.task_done()
#             break
#
#         congress, bill_type, bill_number, update_date_raw = item
#         key = (congress, bill_type, bill_number)
#
#         # 1. Fast In-Memory Timestamp Delta Check
#         current_update = parse_api_date(update_date_raw)
#         previous_update = existing_timestamps.get(key)
#
#         if previous_update and current_update and current_update <= previous_update:
#             stats["skipped_amendments"] += 1
#             queue.task_done()
#             continue
#
#         # 2. Fetch Amendments with Bounded Concurrency
#         async with SEMAPHORE:
#             url = f"https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}/amendments"
#             params = {"api_key": API_KEY, "format": "json", "limit": 250}
#
#             try:
#                 response = await client.get(url, params=params, timeout=10.0)
#                 if response.status_code == 429:
#                     retry_after = int(response.headers.get("Retry-After", 10))
#                     await asyncio.sleep(retry_after)
#                     # Re-queue the bill to try again
#                     await queue.put(item)
#                     queue.task_done()
#                     continue
#
#                 response.raise_for_status()
#                 amendments = response.json().get("amendments", [])
#
#                 # 3. Synchronous write to DuckDB
#                 if amendments:
#                     load_amendments(db, amendments, congress, bill_type, bill_number)
#                     stats["fetched_amendments"] += 1
#
#             except Exception as e:
#                 print(f"[Worker {worker_id}] Error fetching {congress}-{bill_type}-{bill_number}: {e}")
#
#         queue.task_done()
#
# async def run_streaming_pipeline(db, members, existing_timestamps):
#     """
#     Orchestrates streaming bill ingestion and concurrent amendment workers.
#     """
#     queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
#     unique_bills_seen = set()
#     stats = {"fetched_amendments": 0, "skipped_amendments": 0}
#
#     async with httpx.AsyncClient() as client:
#         # 1. Spawn Worker Pool (Consumers)
#         workers = [
#             asyncio.create_task(
#                 amendment_worker(i, queue, client, db, existing_timestamps, stats)
#             )
#             for i in range(NUM_AMENDMENT_WORKERS)
#         ]
#
#         # 2. Producer Phase: Fetch legislation for each member
#         for idx, member in enumerate(members, start=1):
#             bid = member["bioguideId"]
#             print(f"[{idx}/{len(members)}] Fetching legislation for {member.get('name', bid)}...")
#
#             # Fetch sponsored / cosponsored (can also be made async)
#             sponsored, cosponsored = await fetch_legislation_for_member_async(client, bid)
#
#             # Write member-bill relationships immediately
#             load_bills(db, sponsored, bid, "sponsor")
#             load_bills(db, cosponsored, bid, "cosponsor")
#
#             # Enqueue unique bills for the amendment workers in real time
#             for bill in sponsored + cosponsored:
#                 congress = bill.get("congress")
#                 bill_type = bill.get("type", "").upper()
#                 bill_number = str(bill.get("number"))
#                 key = (congress, bill_type, bill_number)
#
#                 if key not in unique_bills_seen:
#                     unique_bills_seen.add(key)
#                     update_date_raw = bill.get("updateDate") or bill.get("updateDateIncludingText")
#
#                     # Push bill into queue (blocks if queue reaches maxsize)
#                     await queue.put((congress, bill_type, bill_number, update_date_raw))
#
#         # 3. Shutdown Workers cleanly once all bills are produced
#         await queue.join()  # Wait for all queued bills to be processed
#         for _ in range(NUM_AMENDMENT_WORKERS):
#             await queue.put(None)  # Send sentinel values
#
#         await asyncio.gather(*workers)
#
#     print(f"\n── Queue Processing Complete ──")
#     print(f"  Unique Bills Processed: {len(unique_bills_seen)}")
#     print(f"  Amendment Packages Fetched: {stats['fetched_amendments']}")
#     print(f"  Unchanged Bills Skipped: {stats['skipped_amendments']}")


def start_pipeline(args):
    member_limit = None if args.full else args.limit
    random_sample = args.random

    db = duckdb.connect(str(DB_PATH))

    # Setup phase (table initialization/reset)
    setup_database(db, reset=args.reset)

    # Ingestion phase
    run_ingestion(
        db,
        member_limit=member_limit,
        random_sample=random_sample,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")