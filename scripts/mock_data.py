"""
scripts/mock_data.py

Mock datasets for testing database creation, ingestion, and validation workflows
without calling external Congress APIs or modifying the production DuckDB database.
"""

from pathlib import Path
import sys
import duckdb

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from database import create_tables
except ImportError:
    from scripts.database import create_tables

# Mock district records: (state_abbr, district_num, geoid_cd, geoid_county)
MOCK_DISTRICTS = [
    ("GA", 14, "1314", "13115"),  # Floyd County, GA
    ("TX", 21, "4821", "48453"),  # Travis County, TX
]

# Mock member records returned from API
MOCK_MEMBERS = [
    {
        "bioguideId": "G000596",
        "name": "Greene, Marjorie Taylor",
        "state": "GA",
        "district": 14,
        "partyName": "Republican",
        "_geoid_cd": "1314",
        "updateDate": "2025-01-15T12:00:00Z",
    },
    {
        "bioguideId": "R000614",
        "name": "Roy, Chip",
        "state": "TX",
        "district": 21,
        "partyName": "Republican",
        "_geoid_cd": "4821",
        "updateDate": "2025-01-16T15:30:00Z",
    },
]

# Mock legislation records for member G000596
MOCK_SPONSORED_BILLS = [
    {
        "congress": 119,
        "type": "HR",
        "number": "101",
        "title": "Protect American Energy Act",
        "updateDate": "2025-02-01T10:00:00Z",
        "latestAction": {
            "actionDate": "2025-02-01",
            "text": "Referred to the House Committee on Energy and Commerce.",
        },
    }
]

MOCK_COSPONSORED_BILLS = [
    {
        "congress": 119,
        "type": "HR",
        "number": "102",
        "title": "Border Security Acceleration Act",
        "updateDate": "2025-02-02T11:00:00Z",
        "latestAction": {
            "actionDate": "2025-02-02",
            "text": "Became Public Law No: 119-1.",
        },
    }
]

# Mock amendment records
MOCK_AMENDMENTS = [
    {
        "number": "1",
        "type": "HAMDT",
        "description": "An amendment to specify federal funding limits.",
        "purpose": "Clarifies funding allocations in Section 3.",
        "sponsor": {"bioguideId": "G000596"},
        "updateDate": "2025-02-03T09:00:00Z",
    }
]


def create_in_memory_db_with_seeds() -> duckdb.DuckDBPyConnection:
    """
    Creates an isolated in-memory DuckDB instance populated with mock seed tables
    and initializes the raw destination tables. Useful for unit and integration testing.
    """
    db = duckdb.connect(":memory:")

    # Create and populate target_counties mock seed
    db.execute("""
        CREATE TABLE target_counties (
            state_fips INTEGER,
            county_fips INTEGER
        );
        INSERT INTO target_counties VALUES
            (13, 115),  -- GA Floyd
            (48, 453);  -- TX Travis
    """)

    # Create and populate census mock seed
    db.execute("""
        CREATE TABLE raw_census__cd11920_county20 (
            GEOID_CD119_20 VARCHAR,
            GEOID_COUNTY_20 VARCHAR
        );
        INSERT INTO raw_census__cd11920_county20 VALUES
            ('1314', '13115'),
            ('4821', '48453');
    """)

    # Initialize raw target tables
    create_tables(db, replace=False)

    return db
