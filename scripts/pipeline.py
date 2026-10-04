import argparse
from pathlib import Path
from helpers import parse_api_date
import sys
import time
import asyncio
import httpx
from datetime import datetime
import duckdb

# Bounded queue and concurrency limits to prevent memory bloat and rate-limiting
QUEUE_MAX_SIZE = 500
NUM_AMENDMENT_WORKERS = 5
SEMAPHORE = asyncio.Semaphore(NUM_AMENDMENT_WORKERS)



# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import (
    fetch_amendments_for_bill,
    fetch_legislation_for_member,
    fetch_members_for_districts,
)
from config import (
    BASE_URL,
    API_KEY,
    DB_PATH,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from database import (
    check_tables_exist,
    create_tables,
    get_existing_bill_timestamps,
    get_existing_members,
    get_target_districts,
    validate_seed_tables,
)
from ingestion import load_amendments, load_bills, load_members


# -- Prompt Helper -------------------------------------------------------------

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


# -- Setup ---------------------------------------------------------------------

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


# -- Ingestion -----------------------------------------------------------------

def run_ingestion(
        db,
        member_limit: int | None = DEFAULT_MEMBER_LIMIT,
        random_sample: bool = False,
        use_async: bool = False,
        members_only: bool = False,
        skip_members: bool = False,
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

    limit_desc = "ALL" if member_limit is None else str(member_limit)

    if skip_members:
        print(f"\nSkipping member API fetch. Loading existing members from raw_members table (limit={limit_desc}, random={random_sample})...")
        members = get_existing_members(db, member_limit=member_limit, shuffle=random_sample)
        print(f"Loaded {len(members)} member(s) from database.")
    else:
        print(f"\nReading target districts from seed tables (random={random_sample})...")
        target_districts = get_target_districts(db, shuffle=random_sample)

        print(f"\nFetching members from Congress API (limit={limit_desc}, random={random_sample})...")
        members = fetch_members_for_districts(target_districts, member_limit=member_limit)
        load_members(db, members)

    if members_only:
        print("\n'--members-only' flag specified. Skipping bills and amendments ingestion.")
        unique_bills_cnt = 0
        fetched_amdts = 0
        skipped_amdts = 0
    else:
        # Load existing bill timestamps for incremental change detection
        existing_bill_timestamps = get_existing_bill_timestamps(db)

        # Execution Branch: Sync vs Async Engine
        mode = "Async Streaming" if use_async else "Synchronous"
        print(f"\n Starting Ingestion Pipeline [{mode} Mode]...")

        if use_async:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_async_pipeline_wrapper(
                db, members, existing_bill_timestamps
            )
        else:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_sync_pipeline(
                db, members, existing_bill_timestamps
            )

    elapsed = time.time() - start_time
    print(f"\n-- Ingestion Summary --")
    print(f"  Total time elapsed: {elapsed:.2f}s")
    print(f"  Members processed: {len(members)}")
    print(f"  Unique bills tracked: {unique_bills_cnt}")
    print(f"  Amendment calls made: {fetched_amdts}")
    print(f"  Amendment calls skipped (unchanged): {skipped_amdts}")

    print("\n-- Tables in dev.duckdb --")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")


def run_sync_pipeline(db, members, existing_timestamps):
    """Synchronous pipeline using in-memory unique_bills dict."""
    unique_bills = {}
    skipped_amendments = 0
    fetched_amendments = 0

    # Stage 1: Load Bills & Deduplicate in Memory
    for idx, member in enumerate(members, start=1):
        bid = member["bioguideId"]
        print(f"[{idx}/{len(members)}] Sync fetching bills for {member.get('name', bid)}...")
        sponsored, cosponsored = fetch_legislation_for_member(bid)

        load_bills(db, sponsored, bid, "sponsor")
        load_bills(db, cosponsored, bid, "cosponsor")

        for bill in sponsored + cosponsored:
            key = (bill.get("congress"), bill.get("type", "").upper(), str(bill.get("number")))
            if key not in unique_bills:
                unique_bills[key] = bill

    # Stage 2: Fetch Amendments
    total_bills = len(unique_bills)
    for idx, ((congress, bill_type, bill_number), bill) in enumerate(unique_bills.items(), start=1):
        key = (congress, bill_type, bill_number)
        bill_ref = f"{congress}-{bill_type}-{bill_number}"
        current_update = parse_api_date(bill.get("updateDate") or bill.get("updateDateIncludingText"))
        previous_update = existing_timestamps.get(key)

        if previous_update and current_update and current_update <= previous_update:
            skipped_amendments += 1
            print(f"[{idx}/{total_bills}] Skipping amendments for {bill_ref} (unchanged since {previous_update})")
            continue

        fetched_amendments += 1
        print(f"[{idx}/{total_bills}] Fetching amendments for {bill_ref}...")
        amendments = fetch_amendments_for_bill(congress, bill_type, bill_number)
        load_amendments(db, amendments, congress, bill_type, bill_number)
        print(f"[{idx}/{total_bills}] Loaded {len(amendments)} amendment(s) for {bill_ref}")

    return len(unique_bills), fetched_amendments, skipped_amendments




# ---------------- Streaming Pipeline Flow ----------------------

def run_async_pipeline_wrapper(db, members, existing_timestamps):
    """Wrapper to trigger asyncio event loop for async streaming engine."""
    return asyncio.run(run_streaming_pipeline(db, members, existing_timestamps))

async def amendment_worker(
        worker_id: int,
        queue: asyncio.Queue,
        write_queue: asyncio.Queue,
        existing_timestamps: dict,
        stats: dict,
):
    """
    Consumer Worker: Continuously pulls unique bills from the queue, fetches
    their amendments, and hands the DB write off to the single db_writer_worker
    (which owns the DuckDB connection) via write_queue.
    """
    while True:
        item = await queue.get()
        if item is None:  # Sentinel value signaling pipeline completion
            queue.task_done()
            break

        congress, bill_type, bill_number, update_date_raw = item
        key = (congress, bill_type, bill_number)
        bill_ref = f"{congress}-{bill_type}-{bill_number}"

        # 1. Fast In-Memory Timestamp Delta Check
        current_update = parse_api_date(update_date_raw)
        previous_update = existing_timestamps.get(key)

        if previous_update and current_update and current_update <= previous_update:
            stats["skipped_amendments"] += 1
            print(f"[Worker {worker_id}] Skipping amendments for {bill_ref} (unchanged since {previous_update})")
            queue.task_done()
            continue

        # 2. Fetch Amendments with Bounded Concurrency
        # Reuse the synchronous api.py client (pagination + 429/5xx retry + 404
        # handling) off the event loop via a thread so the loop stays responsive.
        async with SEMAPHORE:
            try:
                amendments = await asyncio.to_thread(
                    fetch_amendments_for_bill, congress, bill_type, bill_number
                )

                # 3. Hand off the write to the single-owner db_writer_worker
                if amendments:
                    stats["fetched_amendments"] += 1
                    await write_queue.put((amendments, congress, bill_type, bill_number))
                    print(f"[Worker {worker_id}] Fetched {len(amendments)} amendment(s) for {bill_ref} -> queued for write")
                else:
                    print(f"[Worker {worker_id}] No amendments for {bill_ref}")

            except Exception as e:
                print(f"[Worker {worker_id}] Error fetching {bill_ref}: {e}")
            finally:
                queue.task_done()

async def run_streaming_pipeline(db, members, existing_timestamps):
    """
    Orchestrates streaming bill ingestion and concurrent amendment workers.
    """
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    write_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    unique_bills_seen = set()
    stats = {"fetched_amendments": 0, "skipped_amendments": 0}

    # Single consumer that owns the DuckDB write connection.
    writer = asyncio.create_task(db_writer_worker(db, write_queue))

    # 1. Spawn Worker Pool (Consumers)
    workers = [
        asyncio.create_task(
            amendment_worker(i, queue, write_queue, existing_timestamps, stats)
        )
        for i in range(NUM_AMENDMENT_WORKERS)
    ]

    # 2. Producer Phase: Fetch legislation for each member
    for idx, member in enumerate(members, start=1):
        bid = member["bioguideId"]
        print(f"[{idx}/{len(members)}] Fetching legislation for {member.get('name', bid)}...")

        # Fetch sponsored / cosponsored (can also be made async)
        sponsored, cosponsored = fetch_legislation_for_member(bid)

        # Write member-bill relationships immediately
        load_bills(db, sponsored, bid, "sponsor")
        load_bills(db, cosponsored, bid, "cosponsor")

        # Enqueue unique bills for the amendment workers in real time
        for bill in sponsored + cosponsored:
            congress = bill.get("congress")
            bill_type = bill.get("type", "").upper()
            bill_number = str(bill.get("number"))
            key = (congress, bill_type, bill_number)

            if key not in unique_bills_seen:
                unique_bills_seen.add(key)
                update_date_raw = bill.get("updateDate") or bill.get("updateDateIncludingText")

                # Push bill into queue (blocks if queue reaches maxsize)
                await queue.put((congress, bill_type, bill_number, update_date_raw))

    # 3. Shutdown fetch workers cleanly once all bills are produced
    await queue.join()  # Wait for all queued bills to be fetched
    for _ in range(NUM_AMENDMENT_WORKERS):
        await queue.put(None)  # Send sentinel values
    await asyncio.gather(*workers)

    # 4. Drain outstanding writes, then stop the writer (order matters: fetch
    #    workers are done here, so no new items can be enqueued for writing).
    await write_queue.join()
    await write_queue.put(None)  # Sentinel to stop the writer
    await writer

    print(f"\n-- Queue Processing Complete --")
    print(f"  Unique Bills Processed: {len(unique_bills_seen)}")
    print(f"  Amendment Packages Fetched: {stats['fetched_amendments']}")
    print(f"  Unchanged Bills Skipped: {stats['skipped_amendments']}")

    # Match run_sync_pipeline's return shape: (unique_bill_count, fetched, skipped)
    return len(unique_bills_seen), stats["fetched_amendments"], stats["skipped_amendments"]

async def db_writer_worker(db, write_queue: asyncio.Queue):
    """
    Single-consumer loop that owns the DuckDB write connection.

    Amendment workers hand off (amendments, congress, bill_type, bill_number)
    tuples here so all writes are serialized through one owner. Items are
    batched and flushed in one transaction to minimize transaction overhead.
    """
    batch = []

    while True:
        item = await write_queue.get()
        if item is None:  # Sentinel to stop worker
            _flush_batch_to_duckdb(db, batch)  # flush any remainder before exit
            batch.clear()
            write_queue.task_done()
            break

        batch.append(item)

        # Flush batch when it reaches size threshold or queue is temporarily empty
        if len(batch) >= 50 or write_queue.empty():
            _flush_batch_to_duckdb(db, batch)
            batch.clear()

        write_queue.task_done()


def _flush_batch_to_duckdb(db, batch: list[tuple]):
    """
    Persist a batch of per-bill amendment payloads in a single transaction.

    Each item is (amendments, congress, bill_type, bill_number). Reuses
    load_amendments so the upsert/hash logic lives in exactly one place and
    stays in sync with the raw_amendments schema.
    """
    if not batch:
        return

    db.execute("BEGIN TRANSACTION;")
    try:
        for amendments, congress, bill_type, bill_number in batch:
            load_amendments(db, amendments, congress, bill_type, bill_number)
        db.execute("COMMIT;")
    except Exception as e:
        db.execute("ROLLBACK;")
        print(f"[DB Writer] Error flushing batch of {len(batch)}: {e}")

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
        use_async=args.use_async,
        members_only=args.members_only,
        skip_members=args.skip_members,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")
