"""
tests/test_ingestion_validation.py

Unit and integration tests for data ingestion, schema validation, checksumming,
idempotency, deterministic vs. random district sampling, and chamber filtering.
"""

from pathlib import Path
import sys
from unittest.mock import patch

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import duckdb

from api import fetch_members_for_districts, fetch_senate_members_for_states
from database import (
    check_tables_exist,
    create_tables,
    get_existing_members,
    get_target_districts,
    validate_seed_tables,
)
from ingestion import load_amendments, load_bills, load_members
from main import parse_args
from mock_data import (
    MOCK_AMENDMENTS,
    MOCK_COSPONSORED_BILLS,
    MOCK_MEMBERS,
    MOCK_SENATE_AMENDMENTS,
    MOCK_SENATE_COSPONSORED_BILLS,
    MOCK_SENATE_MEMBERS,
    MOCK_SENATE_SPONSORED_BILLS,
    MOCK_SPONSORED_BILLS,
    create_in_memory_db_with_seeds,
)
from pipeline import run_ingestion, setup_database


def test_table_existence_validation():
    """Verify check_tables_exist accurately detects present and missing tables."""
    db = create_in_memory_db_with_seeds()

    # Raw tables and seed tables must exist in the mock database
    assert check_tables_exist(db, ["raw_members", "raw_bills", "raw_amendments"]) is True
    assert validate_seed_tables(db) is True

    # Non-existent table should return False
    assert check_tables_exist(db, ["non_existent_table"]) is False
    db.close()


def test_full_refresh_replaces_only_selected_raw_tables():
    """A targeted full refresh preserves other raw tables and run history."""
    db = create_in_memory_db_with_seeds()
    db.execute("INSERT INTO main.raw_members (bioguide_id) VALUES ('M000001')")
    db.execute("""
        INSERT INTO main.raw_bills
        (congress, bill_type, bill_number, member_id, relationship)
        VALUES (119, 'HR', '1', 'M000001', 'sponsor')
    """)
    db.execute("""
        INSERT INTO main.raw_amendments
        (congress, bill_type, bill_number, amendment_number, amendment_type)
        VALUES (119, 'HR', '1', '1', 'HAM')
    """)
    db.execute("""
        INSERT INTO main.ingestion_runs (run_id, status)
        VALUES ('existing-run', 'COMPLETED')
    """)

    setup_database(db, reset=True, reset_tables=["raw_bills"])

    assert db.execute("SELECT COUNT(*) FROM main.raw_members").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM main.raw_bills").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM main.raw_amendments").fetchone()[0] == 1
    assert db.execute(
        "SELECT COUNT(*) FROM main.ingestion_runs WHERE run_id = 'existing-run'"
    ).fetchone()[0] == 1
    db.close()


def test_member_ingestion_and_data_validation():
    """Verify member records are inserted and column values match expectations."""
    db = create_in_memory_db_with_seeds()

    load_members(db, MOCK_MEMBERS)
    rows = db.execute("SELECT bioguide_id, name, state, chamber, district, party, geoid_cd, row_hash FROM main.raw_members ORDER BY bioguide_id").fetchall()

    assert len(rows) == 2

    # Validate Marjorie Taylor Greene
    mtg = rows[0]
    assert mtg[0] == "G000596"
    assert mtg[1] == "Greene, Marjorie Taylor"
    assert mtg[2] == "GA"
    assert mtg[3] == "House"
    assert mtg[4] == 14
    assert mtg[5] == "Republican"
    assert mtg[6] == "1314"
    assert mtg[7] is not None  # row_hash computed

    # Validate Chip Roy
    roy = rows[1]
    assert roy[0] == "R000614"
    assert roy[1] == "Roy, Chip"
    assert roy[2] == "TX"
    assert roy[3] == "House"
    assert roy[4] == 21
    assert roy[5] == "Republican"
    assert roy[6] == "4821"
    assert roy[7] is not None
    db.close()


def test_senate_member_ingestion():
    """Verify Senate member records are correctly inserted with chamber='Senate'."""
    db = create_in_memory_db_with_seeds()

    load_members(db, MOCK_SENATE_MEMBERS)
    rows = db.execute("SELECT bioguide_id, name, state, chamber, district, party FROM main.raw_members ORDER BY bioguide_id").fetchall()

    assert len(rows) == 2
    cruz = rows[0]
    assert cruz[0] == "C001098"
    assert cruz[1] == "Cruz, Ted"
    assert cruz[2] == "TX"
    assert cruz[3] == "Senate"
    assert cruz[4] is None

    db.close()


def test_fetch_senate_members_for_states_pagination_and_filter():
    """Verify fetch_senate_members_for_states uses paginate and filters for senators."""
    mock_api_members = [
        {
            "bioguideId": "R000614",
            "name": "Roy, Chip",
            "state": "TX",
            "district": 21,
            "terms": {"item": [{"chamber": "House of Representatives", "startYear": 2019}]},
        },
        {
            "bioguideId": "C001098",
            "name": "Cruz, Ted",
            "state": "TX",
            "district": None,
            "terms": {"item": [{"chamber": "Senate", "startYear": 2013}]},
        },
        {
            "bioguideId": "C001056",
            "name": "Cornyn, John",
            "state": "TX",
            "district": None,
            "terms": {"item": {"chamber": "Senate", "startYear": 2002}},  # single dict term test
        },
    ]

    with patch("api.paginate", return_value=mock_api_members) as mock_pag:
        # Test passing state FIPS "48" which maps to "TX"
        senators = fetch_senate_members_for_states(["48"], member_limit=None)
        assert len(senators) == 2
        assert senators[0]["bioguideId"] == "C001098"
        assert senators[0]["chamber"] == "Senate"
        assert senators[0]["district"] is None
        assert senators[1]["bioguideId"] == "C001056"
        assert senators[1]["chamber"] == "Senate"
        assert mock_pag.called


def test_bills_ingestion_and_relationship():
    """Verify bills are loaded with proper relationship and composite keys."""
    db = create_in_memory_db_with_seeds()

    bid = "G000596"
    load_bills(db, MOCK_SPONSORED_BILLS, bid, "sponsor")
    load_bills(db, MOCK_COSPONSORED_BILLS, bid, "cosponsor")

    rows = db.execute("SELECT congress, bill_type, bill_number, title, member_id, relationship FROM main.raw_bills ORDER BY bill_number").fetchall()
    assert len(rows) == 2

    # Check sponsored bill
    b1 = rows[0]
    assert b1 == (119, "HR", "101", "Protect American Energy Act", bid, "sponsor")

    # Check cosponsored bill
    b2 = rows[1]
    assert b2 == (119, "HR", "102", "Border Security Acceleration Act", bid, "cosponsor")
    db.close()


def test_amendments_ingestion():
    """Verify amendments are correctly linked to parent bills."""
    db = create_in_memory_db_with_seeds()

    load_amendments(db, MOCK_AMENDMENTS, 119, "HR", "101")
    rows = db.execute("SELECT congress, bill_type, bill_number, amendment_number, amendment_type, description, purpose, sponsor_id FROM main.raw_amendments").fetchall()

    assert len(rows) == 1
    amdt = rows[0]
    assert amdt[0] == 119
    assert amdt[1] == "HR"
    assert amdt[2] == "101"
    assert amdt[3] == "1"
    assert amdt[4] == "HAMDT"
    assert "funding limits" in amdt[5]
    assert "Section 3" in amdt[6]
    assert amdt[7] == "G000596"
    db.close()


def test_idempotent_duplicate_run():
    """Verify running ingestion twice produces no duplicate rows or corrupted state."""
    db = create_in_memory_db_with_seeds()

    # First run
    load_members(db, MOCK_MEMBERS)
    load_bills(db, MOCK_SPONSORED_BILLS, "G000596", "sponsor")
    load_amendments(db, MOCK_AMENDMENTS, 119, "HR", "101")

    # Second run with exact same data
    load_members(db, MOCK_MEMBERS)
    load_bills(db, MOCK_SPONSORED_BILLS, "G000596", "sponsor")
    load_amendments(db, MOCK_AMENDMENTS, 119, "HR", "101")

    # Verify counts did NOT double
    member_count = db.execute("SELECT COUNT(*) FROM main.raw_members").fetchone()[0]
    bill_count = db.execute("SELECT COUNT(*) FROM main.raw_bills").fetchone()[0]
    amendment_count = db.execute("SELECT COUNT(*) FROM main.raw_amendments").fetchone()[0]

    assert member_count == 2
    assert bill_count == 1
    assert amendment_count == 1
    db.close()


def test_row_update_on_content_change():
    """Verify updating a record's attribute updates the database row and checksum."""
    db = create_in_memory_db_with_seeds()

    load_members(db, MOCK_MEMBERS)
    orig_hash = db.execute("SELECT row_hash FROM main.raw_members WHERE bioguide_id = 'G000596'").fetchone()[0]

    # Modify member name and re-load
    updated_members = [
        {
            "bioguideId": "G000596",
            "name": "Greene, Marjorie Taylor (Updated)",
            "state": "GA",
            "chamber": "House",
            "district": 14,
            "partyName": "Republican",
            "_geoid_cd": "1314",
            "updateDate": "2025-02-15T12:00:00Z",
        }
    ]
    load_members(db, updated_members)

    row = db.execute("SELECT name, row_hash FROM main.raw_members WHERE bioguide_id = 'G000596'").fetchone()
    assert row[0] == "Greene, Marjorie Taylor (Updated)"
    assert row[1] != orig_hash  # Hash must change
    db.close()


def test_deterministic_vs_random_district_sampling():
    """Verify deterministic vs random ordering options in get_target_districts."""
    db = create_in_memory_db_with_seeds()

    # Deterministic call
    districts_det = get_target_districts(db, shuffle=False)
    assert len(districts_det) == 2
    # In deterministic mode, GA (1314) comes before TX (4821)
    assert districts_det[0][0] == "GA"
    assert districts_det[1][0] == "TX"

    # Random shuffle call returns valid districts in a valid list
    districts_rand = get_target_districts(db, shuffle=True)
    assert len(districts_rand) == 2
    states = {d[0] for d in districts_rand}
    assert states == {"GA", "TX"}

    db.close()


def test_get_target_districts_scope_flag():
    """Verify use_target_counties toggles between all districts and the target subset."""
    db = duckdb.connect(":memory:")
    # Census crosswalk with THREE districts; target_counties covers only ONE of them.
    db.execute("""
        CREATE TABLE target_counties (state_fips INTEGER, county_fips INTEGER);
        INSERT INTO target_counties VALUES (13, 115);  -- GA Floyd -> GEOID 13115
        CREATE TABLE raw_census__cd11920_county20 (GEOID_CD119_20 VARCHAR, GEOID_COUNTY_20 VARCHAR);
        INSERT INTO raw_census__cd11920_county20 VALUES
            ('1314', '13115'),   -- GA-14 (in target counties)
            ('4821', '48453'),   -- TX-21 (NOT in target counties)
            ('0611', '06075');   -- CA-11 (NOT in target counties)
    """)

    all_districts = get_target_districts(db, use_target_counties=False)
    assert len(all_districts) == 3
    assert {d[0] for d in all_districts} == {"GA", "TX", "CA"}
    # all-districts mode carries no county (one row per district)
    assert all(d[3] is None for d in all_districts)

    target_only = get_target_districts(db, use_target_counties=True)
    assert len(target_only) == 1
    assert target_only[0][0] == "GA"
    assert target_only[0][1] == 14
    db.close()


def test_fetch_members_for_districts_exact_district_match():
    """Verify the House fetch keeps only members whose district matches the queried one.

    The Congress API district endpoint also returns neighboring-district members and
    (for at-large /0) former House members now in the Senate (district=None). Only the
    exact-district member must be kept, with chamber='House' and the queried geoid.
    """
    api_page = [
        {"bioguideId": "P000197", "name": "Pelosi, Nancy", "district": 11},   # exact match
        {"bioguideId": "D000623", "name": "DeSaulnier, Mark", "district": 10}, # neighbor -> drop
        {"bioguideId": "L000571", "name": "Lummis, Cynthia", "district": None},# senator -> drop
    ]
    with patch("api.paginate", return_value=api_page):
        members = fetch_members_for_districts([("CA", 11, "0611", None)], member_limit=None)

    assert len(members) == 1
    m = members[0]
    assert m["bioguideId"] == "P000197"
    assert m["chamber"] == "House"
    assert m["district"] == 11
    assert m["_geoid_cd"] == "0611"


def test_get_existing_members():
    """Verify get_existing_members retrieves member records from raw_members table."""
    db = create_in_memory_db_with_seeds()
    load_members(db, MOCK_MEMBERS + MOCK_SENATE_MEMBERS)

    members = get_existing_members(db)
    assert len(members) == 4

    # Test chamber filter
    house_members = get_existing_members(db, chamber="house")
    assert len(house_members) == 2
    assert all(m["chamber"] == "House" for m in house_members)

    senate_members = get_existing_members(db, chamber="senate")
    assert len(senate_members) == 2
    assert all(m["chamber"] == "Senate" for m in senate_members)

    # Test limit
    members_limited = get_existing_members(db, member_limit=1)
    assert len(members_limited) == 1
    db.close()


def test_cli_flags_parsing():
    """Verify CLI --members-only, --skip-members, --house, --senate, and --random flags are properly parsed."""
    # Test without flags
    with sys_argv(["scripts/main.py", "--limit", "10"]):
        args = parse_args()
        assert args.random is False
        assert args.limit == 10
        assert args.members_only is False
        assert args.skip_members is False
        assert args.house is False
        assert args.senate is False

    # Test with --members-only
    with sys_argv(["scripts/main.py", "--members-only"]):
        args = parse_args()
        assert args.members_only is True

    # Test with --skip-members
    with sys_argv(["scripts/main.py", "--skip-members"]):
        args = parse_args()
        assert args.skip_members is True

    # Test with --senate and -senate
    with sys_argv(["scripts/main.py", "--senate"]):
        args = parse_args()
        assert args.senate is True

    with sys_argv(["scripts/main.py", "-senate"]):
        args = parse_args()
        assert args.senate is True

    # Test with --house and -house
    with sys_argv(["scripts/main.py", "--house"]):
        args = parse_args()
        assert args.house is True

    with sys_argv(["scripts/main.py", "-house"]):
        args = parse_args()
        assert args.house is True


@patch("pipeline.fetch_members_for_districts", return_value=MOCK_MEMBERS)
@patch("pipeline.fetch_legislation_for_member", return_value=(MOCK_SPONSORED_BILLS, MOCK_COSPONSORED_BILLS))
@patch("pipeline.fetch_amendments_for_bill", return_value=MOCK_AMENDMENTS)
def test_run_ingestion_members_only(mock_amendments, mock_leg, mock_members):
    """Verify run_ingestion with members_only=True skips bills and amendments."""
    db = create_in_memory_db_with_seeds()

    run_ingestion(db, member_limit=2, members_only=True, chamber="house")

    member_count = db.execute("SELECT COUNT(*) FROM main.raw_members").fetchone()[0]
    bill_count = db.execute("SELECT COUNT(*) FROM main.raw_bills").fetchone()[0]
    amendment_count = db.execute("SELECT COUNT(*) FROM main.raw_amendments").fetchone()[0]

    assert member_count == 2
    assert bill_count == 0
    assert amendment_count == 0

    assert mock_members.called
    assert not mock_leg.called
    assert not mock_amendments.called
    db.close()


@patch("pipeline.fetch_members_for_districts")
@patch("pipeline.fetch_legislation_for_member", return_value=(MOCK_SPONSORED_BILLS, MOCK_COSPONSORED_BILLS))
@patch("pipeline.fetch_amendments_for_bill", return_value=MOCK_AMENDMENTS)
def test_run_ingestion_skip_members(mock_amendments, mock_leg, mock_members):
    """Verify run_ingestion with skip_members=True pulls members from table and ingests bills/amendments."""
    db = create_in_memory_db_with_seeds()

    # Pre-populate raw_members
    load_members(db, MOCK_MEMBERS)

    run_ingestion(db, skip_members=True, chamber="house")

    assert not mock_members.called
    assert mock_leg.called
    assert mock_amendments.called

    bill_count = db.execute("SELECT COUNT(*) FROM main.raw_bills").fetchone()[0]
    amendment_count = db.execute("SELECT COUNT(*) FROM main.raw_amendments").fetchone()[0]

    assert bill_count > 0
    assert amendment_count > 0
    db.close()


@patch("pipeline.fetch_senate_members_for_states", return_value=MOCK_SENATE_MEMBERS)
@patch("pipeline.fetch_legislation_for_member", return_value=(MOCK_SENATE_SPONSORED_BILLS, MOCK_SENATE_COSPONSORED_BILLS))
@patch("pipeline.fetch_amendments_for_bill", return_value=MOCK_SENATE_AMENDMENTS)
def test_run_ingestion_senate(mock_amendments, mock_leg, mock_senate_members):
    """Verify run_ingestion with chamber='senate' fetches senate members and legislation."""
    db = create_in_memory_db_with_seeds()

    run_ingestion(db, member_limit=2, chamber="senate")

    member_rows = db.execute("SELECT bioguide_id, chamber FROM main.raw_members ORDER BY bioguide_id").fetchall()
    assert len(member_rows) == 2
    assert all(r[1] == "Senate" for r in member_rows)

    bill_rows = db.execute("SELECT congress, bill_type, bill_number FROM main.raw_bills ORDER BY bill_number").fetchall()
    assert len(bill_rows) == 4
    assert all(r[1] == "S" for r in bill_rows)

    assert mock_senate_members.called
    assert mock_leg.called
    assert mock_amendments.called
    db.close()


class sys_argv:
    """Context manager to safely mock sys.argv for CLI testing."""
    def __init__(self, argv):
        self.argv = argv
        self._orig = None

    def __enter__(self):
        self._orig = sys.argv
        sys.argv = self.argv
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.argv = self._orig


if __name__ == "__main__":
    test_table_existence_validation()
    test_member_ingestion_and_data_validation()
    test_senate_member_ingestion()
    test_fetch_senate_members_for_states_pagination_and_filter()
    test_bills_ingestion_and_relationship()
    test_amendments_ingestion()
    test_idempotent_duplicate_run()
    test_row_update_on_content_change()
    test_deterministic_vs_random_district_sampling()
    test_get_existing_members()
    test_cli_flags_parsing()
    test_run_ingestion_members_only()
    test_run_ingestion_skip_members()
    test_run_ingestion_senate()
    print("All ingestion and data validation tests passed successfully.")
