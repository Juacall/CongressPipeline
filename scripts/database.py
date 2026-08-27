"""
scripts/database.py

Database schema initialization, connection helpers, and metadata tracking for DuckDB.
"""

from pathlib import Path
import sys
import duckdb

# Add script folder to path if executed standalone or as module
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from config import DB_PATH
except ImportError:
    from scripts.config import DB_PATH


def get_db_connection(db_path: Path = DB_PATH):
    """Open and return a connection to the DuckDB database."""
    return duckdb.connect(str(db_path))


def create_tables(db, replace: bool = False):
    """
    Initialize raw tables in DuckDB with primary keys and checksum/timestamp tracking.

    Three raw tables feed the dbt models downstream:
    - raw_members:    one row per target-district House member only
    - raw_bills:      one row per member-bill relationship (sponsor or cosponsor)
    - raw_amendments: all amendments to target bills, regardless of sponsor
    """
    create_stmt = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"

    db.execute(f"""
        {create_stmt} main.raw_members (
            bioguide_id   VARCHAR,
            name          VARCHAR,
            state         VARCHAR,
            district      INTEGER,
            party         VARCHAR,
            geoid_cd      VARCHAR,
            update_date   VARCHAR,
            row_hash      VARCHAR,
            ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (bioguide_id)
        )
    """)
    db.execute(f"""
        {create_stmt} main.raw_bills (
            congress        INTEGER,
            bill_type       VARCHAR,
            bill_number     VARCHAR,
            title           VARCHAR,
            member_id       VARCHAR,
            relationship    VARCHAR,
            update_date     VARCHAR,
            row_hash        VARCHAR,
            ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (congress, bill_type, bill_number, member_id, relationship)
        )
    """)
    db.execute(f"""
        {create_stmt} main.raw_amendments (
            congress          INTEGER,
            bill_type         VARCHAR,
            bill_number       VARCHAR,
            amendment_number  VARCHAR,
            amendment_type    VARCHAR,
            description       VARCHAR,
            purpose           VARCHAR,
            sponsor_id        VARCHAR,
            update_date       VARCHAR,
            row_hash          VARCHAR,
            ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (congress, bill_type, bill_number, amendment_number)
        )
    """)


def get_existing_bill_timestamps(db) -> dict[tuple[int, str, str], str]:
    """
    Query existing bills and return a map of (congress, bill_type, bill_number) -> update_date.
    Used for incremental change detection before fetching amendments.
    """
    # Ensure table exists first
    create_tables(db, replace=False)
    rows = db.execute("""
        SELECT congress, bill_type, bill_number, MAX(update_date) as max_update
        FROM main.raw_bills
        WHERE update_date IS NOT NULL
        GROUP BY congress, bill_type, bill_number
    """).fetchall()
    return {(row[0], row[1].upper(), str(row[2])): row[3] for row in rows}
def get_target_districts(db):
    """
    Join the two seed tables to find all congressional districts that overlap
    at least one of the 350 target counties.
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
    return districts
