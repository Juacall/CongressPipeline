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
from pipeline import run_ingestion


def test_table_existence_validation():
    """Verify check_tables_exist accurately detects present and missing tables."""
    db = create_in_memory_db_with_seeds()

    # Raw tables and seed tables must exist in the mock database
    assert check_tables_exist(db, ["raw_members", "raw_bills", "raw_amendments"]) is True
    assert validate_seed_tables(db) is True

    # Non-existent table should return False
    assert check_tables_exist(db, ["non_existent_table"]) is False
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
