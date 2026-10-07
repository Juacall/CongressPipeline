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
    fetch_senate_members_for_states,
)
from config import (
    ENV,
    BASE_URL,
    API_KEY,
    DB_PATH,
    DEFAULT_CHAMBER,
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


# -- Prompt Helpers ------------------------------------------------------------

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


def prompt_refresh_mode() -> bool:
    """
    Prompt the user to specify whether this is a full refresh or incremental refresh.
    Returns True for full refresh (recreates/resets tables), False for incremental refresh (preserves existing data).
    """
    print("\nSelect refresh mode:")
    print("  1) Incremental refresh (preserve existing data, skip unchanged records)")
    print("  2) Full refresh (reset/recreate raw tables)")

    while True:
        choice = input("Is this a full refresh or incremental refresh? (1=incremental, 2=full, or i/f): ").strip().lower()
        if choice in ("1", "i", "incremental", "n", "no"):
            print("Selected: Incremental refresh\n")
            return False
        elif choice in ("2", "f", "full", "y", "yes"):
            print("Selected: Full refresh (tables will be reset)\n")
            return True
        print("Invalid choice. Please enter '1'/'i' for Incremental or '2'/'f' for Full refresh.")


def prompt_use_target_counties() -> bool:
    """
    Prompt the user for the member scope: every congressional district (full
    House / all states' senators) or only districts overlapping target_counties.csv.
    Returns True to restrict to target counties, False for all districts.
    """
    print("\nSelect member scope:")
    print("  1) All districts (full House and every state's senators)")
    print("  2) Target counties only (restrict to target_counties.csv)")

    while True:
        choice = input("Scope members to target counties? (1=all, 2=target, or a/t): ").strip().lower()
        if choice in ("1", "a", "all", "n", "no"):
            print("Selected: All districts\n")
            return False
        elif choice in ("2", "t", "target", "y", "yes"):
            print("Selected: Target counties only\n")
            return True
        print("Invalid choice. Please enter '1'/'a' for All districts or '2'/'t' for Target counties.")


def prompt_ingestion_menu() -> dict | None:
    """
    Display interactive menu for selecting the ingestion pipeline task.
    Returns a dict with 'chamber', 'members_only', and 'skip_members' configuration,
    or None if the user chooses to exit.
    """
    print("\n" + "=" * 55)
    print("           CONGRESS INGESTION PIPELINE")
    print("=" * 55)
    print("Please select which task to run:")
    print("  1) House Members Only")
    print("  2) Senate Members Only")
    print("  3) House Bills and Amendments")
    print("  4) Senate Bills and Amendments")
    print("  5) All (Both Chambers - Members, Bills & Amendments)")
    print("  6) Exit")
    print("=" * 55)

    options = {
        "1": {"chamber": "house", "members_only": True, "skip_members": False, "label": "House Members Only"},
        "2": {"chamber": "senate", "members_only": True, "skip_members": False, "label": "Senate Members Only"},
        "3": {"chamber": "house", "members_only": False, "skip_members": True, "label": "House Bills and Amendments"},
        "4": {"chamber": "senate", "members_only": False, "skip_members": True, "label": "Senate Bills and Amendments"},
        "5": {"chamber": "both", "members_only": False, "skip_members": True, "label": "All (Both Chambers - Members, Bills & Amendments)"},
    }

    while True:
        choice = input("Enter choice [1-6]: ").strip().lower()
        if choice in options:
            selected = options[choice]
            print(f"Selected: {selected['label']}")
            return selected
        elif choice in ("6", "q", "quit", "exit"):
            print("Exiting pipeline.")
            return None
        print("Invalid choice. Please enter a number from 1 to 6 (or 'q' to exit).")


# -- Setup ---------------------------------------------------------------------

def setup_database(
        db,
        reset: bool | None = None,
        reset_tables: list[str] | None = None,
):
    """
    Initialize database tables.
    If reset is explicitly provided (True/False via CLI or interactive prompt), use it.
    Otherwise, interactively prompt the user.
    On a full refresh, reset only the requested raw tables.
    """
    if reset is None:
        should_reset = prompt_refresh_mode()
    else:
        should_reset = reset

    if should_reset:
        tables = reset_tables if reset_tables is not None else [
            "raw_members", "raw_bills", "raw_amendments"
        ]
        if tables:
            print(f"Recreating raw table(s) (full refresh mode): {', '.join(tables)}...")
        else:
            print("Full refresh selected, but this task does not ingest into a raw table.")
        create_tables(db, replace=True, replace_tables=tables)
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
        chamber: str = DEFAULT_CHAMBER,
        use_target_counties: bool = False,
):
    """Fetch data from Congress API and ingest into database tables incrementally."""
    start_time = time.time()
    chamber = (chamber or DEFAULT_CHAMBER).lower()

    # Validate prerequisite tables before running. The census crosswalk is always
    # required; the target_counties seed is only needed when scoping to it.
    if use_target_counties:
        if not validate_seed_tables(db):
            raise RuntimeError(
                "Prerequisite seed tables ('target_counties', 'raw_census__cd11920_county20') are missing. "
                "Please run 'uv run dbt seed' from the dbt/ directory first."
            )
    elif not check_tables_exist(db, ["raw_census__cd11920_county20"]):
        raise RuntimeError(
            "Prerequisite seed table 'raw_census__cd11920_county20' is missing. "
            "Please run 'uv run dbt seed' from the dbt/ directory first."
        )

    if not check_tables_exist(db, ["raw_members", "raw_bills", "raw_amendments"]):
        print("Raw destination tables missing. Creating raw tables...")
        create_tables(db, replace=False)

    limit_desc = "ALL" if member_limit is None else str(member_limit)

    member_stats = None
    if skip_members:
        print(f"\nSkipping member API fetch. Loading existing members from raw_members table (chamber={chamber}, limit={limit_desc}, random={random_sample})...")
        members = get_existing_members(db, member_limit=member_limit, shuffle=random_sample, chamber=chamber if chamber != "both" else None)
        print(f"Loaded {len(members)} member(s) from database.")
    else:
        scope_desc = "target counties (target_counties.csv)" if use_target_counties else "ALL districts"
        print(f"\nReading districts from seed tables — scope: {scope_desc} (random={random_sample})...")
        target_districts = get_target_districts(db, shuffle=random_sample, use_target_counties=use_target_counties)

        members = []
        if chamber in ("house", "both"):
            print(f"\nFetching House members from Congress API (limit={limit_desc}, random={random_sample})...")
            house_members = fetch_members_for_districts(target_districts, member_limit=member_limit)
            members.extend(house_members)

        if chamber in ("senate", "both"):
            target_states = list(dict.fromkeys(d[0] for d in target_districts))
            senate_limit = member_limit if chamber == "senate" else (member_limit - len(members) if member_limit else None)
            if senate_limit is None or senate_limit > 0:
                print(f"\nFetching Senate members from Congress API (limit={limit_desc}, random={random_sample})...")
                senate_members = fetch_senate_members_for_states(target_states, member_limit=senate_limit)
                members.extend(senate_members)

        member_stats = load_members(db, members)

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
        print(f"\n Starting Ingestion Pipeline [{mode} Mode] (chamber={chamber})...")

        if use_async:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_async_pipeline_wrapper(
                db, members, existing_bill_timestamps, chamber=chamber
            )
        else:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_sync_pipeline(
                db, members, existing_bill_timestamps, chamber=chamber
            )

    elapsed = time.time() - start_time
    print(f"\n-- Ingestion Summary --")
    print(f"  Total time elapsed: {elapsed:.2f}s")
    print(f"  Chamber: {chamber.upper()}")
    print(f"  Members fetched: {len(members)}")
    if member_stats is not None:
        print(f"    - New (inserted):          {member_stats['inserted']}")
        print(f"    - Changed (updated):       {member_stats['updated']}")
        print(f"    - Unchanged (write skipped): {member_stats['unchanged']}")
    else:
        print(f"    (members loaded from DB; no upsert performed)")
    print(f"  Unique bills tracked: {unique_bills_cnt}")
    print(f"  Amendment calls made: {fetched_amdts}")
    print(f"  Amendment calls skipped (unchanged): {skipped_amdts}")

    print("\n-- Tables in dev.duckdb --")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")


def run_sync_pipeline(db, members, existing_timestamps, chamber: str = "house"):
    """Synchronous pipeline using in-memory unique_bills dict."""
    unique_bills = {}
    skipped_amendments = 0
    fetched_amendments = 0

    # Stage 1: Load Bills & Deduplicate in Memory
    for idx, member in enumerate(members, start=1):
        bid = member["bioguideId"]
        member_chamber = member.get("chamber") or chamber
        print(f"[{idx}/{len(members)}] Sync fetching bills for {member.get('name', bid)} ({member_chamber})...")
        sponsored, cosponsored = fetch_legislation_for_member(bid, chamber=member_chamber)

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
        # The list endpoint has no bill updateDate; use latestAction.actionDate as
        # the change signal (matches raw_bills.latest_action_date in the baseline).
        # latest_action_date is stored as a VARCHAR, so parse both sides to datetime.
        current_update = parse_api_date((bill.get("latestAction") or {}).get("actionDate"))
        previous_update = parse_api_date(existing_timestamps.get(key))

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

def run_async_pipeline_wrapper(db, members, existing_timestamps, chamber: str = "house"):
    """Wrapper to trigger asyncio event loop for async streaming engine."""
    return asyncio.run(run_streaming_pipeline(db, members, existing_timestamps, chamber=chamber))

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
        # update_date_raw is the bill's latestAction.actionDate; the baseline holds
        # latest_action_date as a VARCHAR, so parse both sides to datetime.
        current_update = parse_api_date(update_date_raw)
        previous_update = parse_api_date(existing_timestamps.get(key))

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

async def run_streaming_pipeline(db, members, existing_timestamps, chamber: str = "house"):
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
        member_chamber = member.get("chamber") or chamber
        print(f"[{idx}/{len(members)}] Fetching legislation for {member.get('name', bid)} ({member_chamber})...")

        # Fetch sponsored / cosponsored (can also be made async)
        sponsored, cosponsored = fetch_legislation_for_member(bid, chamber=member_chamber)

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
                # The list endpoint has no bill updateDate; use latestAction.actionDate
                # as the change signal (matches raw_bills.latest_action_date baseline).
                update_date_raw = (bill.get("latestAction") or {}).get("actionDate")

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
    member_limit = None if ENV != 'Dev' else 5
    random_sample = getattr(args, "random", False)

    # Member scope: None = not specified on the CLI (decide below), else explicit.
    use_target_counties = getattr(args, "target_counties", None)

    # Check if explicit mode/chamber CLI flags were passed
    has_explicit_cli_flags = any([
        getattr(args, "house", False),
        getattr(args, "senate", False),
        getattr(args, "chamber", None) is not None,
        getattr(args, "members_only", False),
        getattr(args, "skip_members", False),
    ])

    reset = getattr(args, "reset", None)

    if not has_explicit_cli_flags:
        # Prompt user with the interactive menu
        selection = prompt_ingestion_menu()
        if selection is None:
            return  # User selected Exit
        chamber = selection["chamber"]
        members_only = selection["members_only"]
        skip_members = selection["skip_members"]

        # Prompt right after the menu for full refresh vs incremental refresh if not specified via CLI
        if reset is None:
            reset = prompt_refresh_mode()

        # Prompt for member scope (all districts vs target counties) if not set via CLI
        if use_target_counties is None:
            use_target_counties = prompt_use_target_counties()
    else:
        # Resolve chamber configuration from CLI arguments
        chamber = DEFAULT_CHAMBER
        if getattr(args, "senate", False):
            chamber = "senate"
        elif getattr(args, "house", False):
            chamber = "house"
        elif getattr(args, "chamber", None):
            chamber = args.chamber
        members_only = getattr(args, "members_only", False)
        skip_members = getattr(args, "skip_members", False)

    # Default to all districts when scope was not specified via CLI.
    if use_target_counties is None:
        use_target_counties = False

    db = duckdb.connect(str(DB_PATH))

    # Setup phase (table initialization/reset)
    reset_tables = []
    if not skip_members:
        reset_tables.append("raw_members")
    if not members_only:
        reset_tables.extend(["raw_bills", "raw_amendments"])
    setup_database(db, reset=reset, reset_tables=reset_tables)

    # Ingestion phase
    run_ingestion(
        db,
        member_limit=member_limit,
        random_sample=random_sample,
        use_async=getattr(args, "use_async", False),
        members_only=members_only,
        skip_members=skip_members,
        chamber=chamber,
        use_target_counties=use_target_counties,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")
