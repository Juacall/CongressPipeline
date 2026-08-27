"""
scripts/ingestion.py

Data loading functions for DuckDB raw tables.
"""


def load_members(db, members):
    """Insert member rows into raw_members. Only target-district members are loaded."""
    rows = [
        (
            m.get("bioguideId"),
            m.get("name"),
            m.get("state"),
            m.get("district"),
            m.get("partyName"),
            m.get("_geoid_cd"),
        )
        for m in members
    ]
    if rows:
        db.executemany("INSERT INTO main.raw_members VALUES (?, ?, ?, ?, ?, ?)", rows)


def load_bills(db, bills, member_id, relationship):
    """Insert bill rows into raw_bills. One row per member-bill relationship."""
    rows = [
        (
            b.get("congress"),
            b.get("type"),
            str(b.get("number")),
            b.get("title"),
            member_id,
            relationship,
        )
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
        rows.append(
            (
                congress,
                bill_type,
                bill_number,
                str(a.get("number", "")),
                a.get("type"),
                a.get("description"),
                a.get("purpose"),
                sponsor_id,
            )
        )
    if rows:
        db.executemany(
            "INSERT INTO main.raw_amendments VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
