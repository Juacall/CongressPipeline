"""
scripts/main.py

Traversal strategy: district-first.

The census seed table already maps counties to districts, so we can identify
which districts overlap our 350 target counties with a local SQL query — no API
calls needed. From there, /member/congress/{congress}/{state}/{district} lets us
fetch members for a specific district and congress directly. Every API call is
anchored to a known target county.

The alternative (bill-first) means pulling all 119th Congress House bills then
working backwards to find target-district members — most of what you fetch is
irrelevant.

Traversal order:
  1. SQL join on seed tables to get target districts.
  2. /member/congress/{congress}/{state}/{district} per district.
     Deduplicated by bioguideId — a district may overlap multiple target counties
     but a member should only appear once in raw_members.
  3. /member/{bioguideId}/sponsored-legislation and cosponsored-legislation.
     No congress filter on these endpoints, so 119th Congress filtering happens
     locally in Python.
  4. /bill/{congress}/{billType}/{billNumber}/amendments per unique bill.
     404 = no amendments, treated as empty not an error.

Pagination: paginate() follows pagination.next exhaustively at 250 items/page.

Tables loaded:
  raw_members    — target-district members only. Amendment sponsors outside
                   target districts are stored in raw_amendments.sponsor_id
                   but not loaded as members.
  raw_bills      — one row per member-bill relationship (sponsor or cosponsor).
  raw_amendments — all amendments to target bills regardless of sponsor.
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
from database import create_tables
from ingestion import load_amendments, load_bills, load_members


# ── Traversal Queries ─────────────────────────────────────────────────────────

def get_target_districts(db):
    """
    Join the two seed tables to find all congressional districts that overlap
    at least one of the 350 target counties.

    target_counties stores county identifiers split across state_fips (2-digit)
    and county_fips (3-digit). The census table stores them as a single 5-digit
    GEOID_COUNTY_20. We reconstruct the 5-digit key with LPAD to join them.

    Districts are the bridge between counties and members — a county maps to a
    district, and a district maps to exactly one House member. One county can
    span multiple districts and one district can span multiple counties.

    Returns:
        districts: list of (state_abbr, district_num, geoid, geoid_county) tuples
    """
    rows = db.execute("""
        SELECT DISTINCT
            census.GEOID_CD119_20,
            census.GEOID_COUNTY_20,
            LEFT(census.GEOID_CD119_20, 2)                    AS state_fips,
            CAST(RIGHT(census.GEOID_CD119_20, 2) AS INTEGER)  AS district_num
        FROM raw_census__cd11920_county20 AS census
        INNER JOIN target_counties AS tc
            ON census.GEOID_COUNTY_20 =
               LPAD(CAST(tc.state_fips AS VARCHAR), 2, '0')
               || LPAD(CAST(tc.county_fips AS VARCHAR), 3, '0')
        WHERE census.GEOID_CD119_20 NOT LIKE '%ZZ'  -- exclude non-voting delegate districts
        ORDER BY
            random()
    """).fetchall()

    districts = []
    seen_states = set()

    for geoid_cd, geoid_county, state_fips, district_num in rows:
        state_abbr = STATE_FIPS_TO_ABBR.get(state_fips)
        if state_abbr:  # skip territories not in scope (e.g. Puerto Rico, Guam)
            districts.append((state_abbr, district_num, geoid_cd, geoid_county))
            seen_states.add(state_abbr)

    print(f"Found {len(districts)} target districts across {len(seen_states)} states")
    for state_abbr, district_num, geoid_cd, geoid_county in districts:
        print(f"  {state_abbr}-{district_num} | geoid_cd: {geoid_cd} | county: {geoid_county}")
    return districts


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    db = duckdb.connect(str(DB_PATH))

    print("Creating raw tables...")
    create_tables(db)

    print("\nReading target districts from seed tables...")
    target_districts = get_target_districts(db)

    print(f"\nFetching members (limit={MEMBER_LIMIT})...")
    members = fetch_members_for_districts(target_districts, member_limit=MEMBER_LIMIT)
    load_members(db, members)

    # Track processed bills to avoid fetching amendments more than once per bill
    processed_bills = set()

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
            key = (bill.get("congress"), bill.get("type"), str(bill.get("number")))
            if key in processed_bills:
                continue
            processed_bills.add(key)
            congress, bill_type, bill_number = key
            amendments = fetch_amendments_for_bill(congress, bill_type, bill_number)
            print(f"{congress} - {bill_type}-{bill_number}: {len(amendments)} amendments")
            load_amendments(db, amendments, congress, bill_type, bill_number)

    print("\n── Tables in dev.duckdb ──")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")


if __name__ == "__main__":
    main()
