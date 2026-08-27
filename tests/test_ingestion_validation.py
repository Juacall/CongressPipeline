"""
tests/test_ingestion_validation.py

Unit and integration tests for data ingestion, schema validation, checksumming,
idempotency, and deterministic vs. random district sampling.
"""

from pathlib import Path
import sys

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from database import (
    check_tables_exist,
    create_tables,
    get_target_districts,
    validate_seed_tables,
)
from ingestion import load_amendments, load_bills, load_members
from main import parse_args
from mock_data import (
    MOCK_AMENDMENTS,
    MOCK_COSPONSORED_BILLS,
    MOCK_MEMBERS,
    MOCK_SPONSORED_BILLS,
    create_in_memory_db_with_seeds,
)


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
    rows = db.execute("SELECT bioguide_id, name, state, district, party, geoid_cd, row_hash FROM main.raw_members ORDER BY bioguide_id").fetchall()

    assert len(rows) == 2

    # Validate Marjorie Taylor Greene
    mtg = rows[0]
    assert mtg[0] == "G000596"
    assert mtg[1] == "Greene, Marjorie Taylor"
    assert mtg[2] == "GA"
    assert mtg[3] == 14
    assert mtg[4] == "Republican"
    assert mtg[5] == "1314"
    assert mtg[6] is not None  # row_hash computed

    # Validate Chip Roy
    roy = rows[1]
    assert roy[0] == "R000614"
    assert roy[1] == "Roy, Chip"
    assert roy[2] == "TX"
    assert roy[3] == 21
    assert roy[4] == "Republican"
    assert roy[5] == "4821"
    assert roy[6] is not None
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


def test_cli_random_flag_parsing():
    """Verify CLI --random flag is properly parsed."""
    # Test without --random
    with sys_argv(["scripts/main.py", "--limit", "10"]):
        args = parse_args()
        assert args.random is False
        assert args.limit == 10

    # Test with --random
    with sys_argv(["scripts/main.py", "--limit", "15", "--random"]):
        args = parse_args()
        assert args.random is True
        assert args.limit == 15


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
    test_bills_ingestion_and_relationship()
    test_amendments_ingestion()
    test_idempotent_duplicate_run()
    test_row_update_on_content_change()
    test_deterministic_vs_random_district_sampling()
    test_cli_random_flag_parsing()
    print("All ingestion and data validation tests passed successfully.")
