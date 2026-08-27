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

MEMBER_LIMIT and BILLS_PER_MEMBER cap data volume during development. One member
can have hundreds of cosponsored bills, each triggering an amendments call.
Set both to None for a full run.
"""

from pathlib import Path
import os
import time

import duckdb
import requests

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).resolve().parent.parent / "dev.duckdb"
API_KEY = os.environ.get("CONGRESS_API_KEY", "your_api_key_here")
BASE_URL = "https://api.congress.gov/v3"
CONGRESS = 119

# Cap members processed during development. Set to None for full dataset.
MEMBER_LIMIT = 5

# Cap bills processed per member (sponsored + cosponsored combined).
# A member can have hundreds of cosponsored bills — without this limit each
# member triggers hundreds of API calls for amendments.
# Set to None to retrieve all bills for production use.
BILLS_PER_MEMBER = None

# Maps 2-digit state FIPS codes to abbreviations — the Congress API uses
# abbreviations in URLs (/member/TX/21) while the census data uses FIPS codes.
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "12": "FL", "13": "GA",
    "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO",
    "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ",
    "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC",
    "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT",
    "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
}

# House bill types only — filter out Senate legislation
HOUSE_BILL_TYPES = {"HR", "HRES", "HJRES", "HCONRES"}


# ── Database ──────────────────────────────────────────────────────────────────

def create_tables(db):
    """
    Create raw tables in DuckDB. CREATE OR REPLACE makes this script safely
    re-runnable — existing data is wiped and replaced on each run.

    Three tables feed the dbt models downstream:
    - raw_members:    one row per target-district House member only
    - raw_bills:      one row per member-bill relationship (sponsor or cosponsor)
    - raw_amendments: all amendments to target bills, regardless of sponsor

    Amendment sponsors are not loaded into raw_members unless they happen to
    be target-district members discovered during district traversal —
    raw_amendments scope is amendments to target bills regardless of sponsor,
    not all members who have ever sponsored an amendment.

    Note: the Congress API returns the current set of cosponsors for each bill.
    A member can withdraw a cosponsorship — when they do, the API simply omits
    them from subsequent responses. We capture cosponsorships as the API
    currently reports them.
    """
    db.execute("""
        CREATE OR REPLACE TABLE main.raw_members (
            bioguide_id  VARCHAR,
            name         VARCHAR,
            state        VARCHAR,
            district     INTEGER,
            party        VARCHAR,
            geoid_cd     VARCHAR   -- 4-digit GEOID, joins back to census seed
        )
    """)
    db.execute("""
        CREATE OR REPLACE TABLE main.raw_bills (
            congress        INTEGER,
            bill_type       VARCHAR,
            bill_number     VARCHAR,
            title           VARCHAR,
            member_id       VARCHAR,  -- bioguide_id of the connected member
            relationship    VARCHAR   -- 'sponsor' or 'cosponsor'
        )
    """)
    db.execute("""
        CREATE OR REPLACE TABLE main.raw_amendments (
            congress          INTEGER,
            bill_type         VARCHAR,
            bill_number       VARCHAR,
            amendment_number  VARCHAR,
            amendment_type    VARCHAR,
            description       VARCHAR,
            purpose           VARCHAR,
            sponsor_id        VARCHAR   -- null or outside target districts is valid
        )
    """)


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
        districts: list of (state_abbr, district_num, geoid) tuples for use
                   with the Congress API (/member/congress/{congress}/{state}/{district})
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
    # put mtg first because per raw_amendments she has amendments

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

def api_get(url, params=None, retries=5):
    query = {"api_key": API_KEY, "format": "json"}
    if params:
        query.update(params)
    
    for attempt in range(retries):
        time.sleep(0.1)  # throttle every request
        response = requests.get(url, params=query)
        if response.status_code >= 500:
            wait = 2 ** (attempt + 1)
            print(f"  {response.status_code} on attempt {attempt + 1}, retrying in {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response.json()
    
    raise RuntimeError(f"Failed after {retries} retries: {url}")

def paginate(url, result_key, params=None):
    """
    Collect all pages of results for a Congress API endpoint.

    The API returns a pagination.next URL in the response body when more pages
    exist. We follow it until exhausted, accumulating all results into one list.
    We request 250 items per page (the API maximum) to minimise round trips.

    Subsequent page URLs already have all query params baked in, so we only
    pass our own params on the first request.

    Args:
        url:        The initial endpoint URL.
        result_key: Top-level JSON key holding the results list (e.g. "members").
        params:     Optional dict of query parameters for the first request.

    Returns:
        Flat list of all result items across all pages.
    """
    first_page_params = {**(params or {}), "limit": 250}
    results = []
    next_url = url

    while next_url:
        data = api_get(next_url, first_page_params if next_url == url else None)
        results.extend(data.get(result_key, []))
        next_url = data.get("pagination", {}).get("next")

    return results


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_members_for_districts(districts):
    """
    For each target district, fetch House members from the Congress API scoped
    to the 119th Congress.

    Endpoint: /member/congress/{congress}/{stateCode}/{district}

    Deduplicates by bioguideId — a member's district may overlap multiple target
    counties, but the member should only appear once in raw_members.

    Args:
        districts: list of (state_abbr, district_num, geoid_cd, geoid_county) tuples.

    Returns:
        List of member dicts with '_geoid_cd' added for the downstream census join.
    """
    members = []
    seen_ids = set()

    for state_abbr, district_num, geoid_cd, geoid_county in districts:
        if MEMBER_LIMIT and len(members) >= MEMBER_LIMIT:
            break

        url = f"{BASE_URL}/member/congress/{CONGRESS}/{state_abbr}/{district_num}"
        try:
            page = paginate(url, "members")
        except requests.HTTPError as e:
            print(f"  Warning: could not fetch {state_abbr}-{district_num}: {e}")
            continue

        for m in page:
            bid = m.get("bioguideId")
            if not bid or bid in seen_ids:
                continue
            seen_ids.add(bid)
            m["_geoid_cd"] = geoid_cd  # carry the geoid forward for the census join
            members.append(m)
            print(f"  Member: {m.get('name')} ({state_abbr}-{district_num}) [county GEOID: {geoid_county}]")

    print(f"  Total: {len(members)} members fetched")
    return members


def fetch_legislation_for_member(bioguide_id):
    """
    Fetch all bills sponsored and cosponsored by a member in the 119th Congress.

    Returns two separate lists so the caller can store the correct relationship
    ('sponsor' or 'cosponsor') for each bill in raw_bills.

    Filters to House bill types only — members may also appear on Senate
    legislation which is out of scope for this pipeline.
    """
    def _fetch(endpoint, result_key):
        url = f"{BASE_URL}/member/{bioguide_id}/{endpoint}"
        return [
            b for b in paginate(url, result_key)
            if b.get("congress") == CONGRESS and b.get("type") in HOUSE_BILL_TYPES
        ]

    sponsored = _fetch("sponsored-legislation", "sponsoredLegislation")
    cosponsored = _fetch("cosponsored-legislation", "cosponsoredLegislation")
    return sponsored, cosponsored


def fetch_amendments_for_bill(congress, bill_type, bill_number):
    """
    Fetch all amendments to a bill, regardless of who sponsored them.

    Amendments are scoped to target bills — we never call this for bills outside
    the pipeline. The API returns 404 when a bill has no amendments rather than
    an empty list, so we treat 404 as a valid empty result.
    """
    # url = f"{BASE_URL}/bill/{congress}/{bill_type}/{bill_number}/amendments"
    url = f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/amendments"
    try:
        return paginate(url, "amendments")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise


# ── Load ──────────────────────────────────────────────────────────────────────

def load_members(db, members):
    """Insert member rows into raw_members. Only target-district members are loaded."""
    rows = [
        (m.get("bioguideId"), m.get("name"), m.get("state"),
         m.get("district"), m.get("partyName"), m.get("_geoid_cd"))
        for m in members
    ]
    if rows:
        db.executemany("INSERT INTO main.raw_members VALUES (?, ?, ?, ?, ?, ?)", rows)


def load_bills(db, bills, member_id, relationship):
    """Insert bill rows into raw_bills. One row per member-bill relationship."""
    rows = [
        (b.get("congress"), b.get("type"), str(b.get("number")),
         b.get("title"), member_id, relationship)
        for b in bills
    ]
    if rows:
        db.executemany("INSERT INTO main.raw_bills VALUES (?, ?, ?, ?, ?, ?)", rows)


def load_amendments(db, amendments, congress, bill_type, bill_number):
    """
    Insert amendment rows into raw_amendments for a given parent bill.

    sponsor_id is stored as-is — it may be null, or reference a member outside
    target districts. Both are valid: we store all amendments to target bills
    regardless of who sponsored them.
    """
    rows = []
    for a in amendments:
        sponsor = a.get("sponsor")
        sponsor_id = sponsor.get("bioguideId") if isinstance(sponsor, dict) else None
        rows.append((
            congress, bill_type, bill_number,
            str(a.get("number", "")), a.get("type"),
            a.get("description"), a.get("purpose"), sponsor_id,
        ))
    if rows:
        db.executemany(
            "INSERT INTO main.raw_amendments VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    db = duckdb.connect(str(DB_PATH))

    print("Creating raw tables...")
    create_tables(db)

    print("\nReading target districts from seed tables...")
    target_districts = get_target_districts(db)

    print(f"\nFetching members (limit={MEMBER_LIMIT})...")
    members = fetch_members_for_districts(target_districts)
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

        # Prioritise sponsored bills, then fill remaining slots with cosponsored.
        # In production (BILLS_PER_MEMBER=None) all bills are processed.
      
      
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
