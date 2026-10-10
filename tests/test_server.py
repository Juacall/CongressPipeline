from pathlib import Path
import sys

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.server import server


def _create_politicians_db(path):
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE mart_legislative_activity (
            target_member_id VARCHAR,
            target_member_name VARCHAR,
            target_member_state_code VARCHAR,
            target_member_state VARCHAR,
            target_member_chamber VARCHAR,
            target_member_party VARCHAR,
            relationship VARCHAR,
            bill_id VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO mart_legislative_activity VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("H1", "House Member", "WA", "Washington", "House", "Party A", "sponsor", "HR1"),
            ("S1", "Senate Member", "WA", "Washington", "Senate", "Party B", "sponsor", "S1"),
        ],
    )
    conn.close()


def test_politicians_chamber_filter_normalizes_request_and_stored_value(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "politicians.duckdb"
    _create_politicians_db(db_path)
    monkeypatch.setattr(server, "DB_PATH", db_path)

    for chamber in ("Senate", "senate", "S"):
        politicians = server.get_politicians(chamber=chamber)
        assert [politician["id"] for politician in politicians] == ["S1"]


def test_politicians_without_chamber_filter_returns_all_members(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "politicians.duckdb"
    _create_politicians_db(db_path)
    monkeypatch.setattr(server, "DB_PATH", db_path)

    politicians = server.get_politicians()

    assert {politician["id"] for politician in politicians} == {"H1", "S1"}
