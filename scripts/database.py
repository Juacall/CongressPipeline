"""
scripts/database.py

Database schema initialization and connection helpers for DuckDB.
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
