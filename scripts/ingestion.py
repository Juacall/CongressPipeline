"""
scripts/ingestion.py

Data loading and idempotent upsert functions for DuckDB raw tables.
Includes MD5 checksum generation and timestamp tracking.
"""

import hashlib
from datetime import datetime, timezone


def _compute_hash(*values) -> str:
    """Compute an MD5 hash over given string/numeric values."""
    payload = "|".join("" if v is None else str(v) for v in values)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def load_members(db, members):
    """
    Idempotently upsert member rows into raw_members.
    Primary Key: (bioguide_id).
    """
    if not members:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for m in members:
        bid = m.get("bioguideId")
        name = m.get("name")
        state = m.get("state")
        district = m.get("district")
        party = m.get("partyName")
        geoid_cd = m.get("_geoid_cd")
        update_date = m.get("updateDate")

        row_hash = _compute_hash(bid, name, state, district, party, geoid_cd)
        rows.append((bid, name, state, district, party, geoid_cd, update_date, row_hash, now_iso))

    db.executemany("""
        INSERT INTO main.raw_members (
            bioguide_id, name, state, district, party, geoid_cd, update_date, row_hash, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            name = EXCLUDED.name,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            party = EXCLUDED.party,
            geoid_cd = EXCLUDED.geoid_cd,
            update_date = EXCLUDED.update_date,
            row_hash = EXCLUDED.row_hash,
            ingested_at = EXCLUDED.ingested_at
        WHERE main.raw_members.row_hash != EXCLUDED.row_hash
    """, rows)


def load_bills(db, bills, member_id, relationship):
    """
    Idempotently upsert bill rows into raw_bills.
    Primary Key: (congress, bill_type, bill_number, member_id, relationship).
    """
    if not bills:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for b in bills:
        congress = b.get("congress")
        bill_type = b.get("type")
        bill_number = str(b.get("number"))
        title = b.get("title")
        update_date = b.get("updateDate") or b.get("updateDateIncludingText")

        # Current bill status: the Congress API exposes the most-recent action
        # inline on the sponsored/cosponsored list response (no extra call). We
        # track current status only, so this is overwritten on each run.
        latest_action = b.get("latestAction") or {}
        latest_action_date = latest_action.get("actionDate")
        latest_action_text = latest_action.get("text")

        row_hash = _compute_hash(
            congress, bill_type, bill_number, title, member_id, relationship,
            latest_action_date, latest_action_text
        )
        rows.append((
            congress, bill_type, bill_number, title,
            latest_action_date, latest_action_text, member_id, relationship,
            update_date, row_hash, now_iso
        ))

    db.executemany("""
        INSERT INTO main.raw_bills (
            congress, bill_type, bill_number, title,
            latest_action_date, latest_action_text, member_id, relationship,
            update_date, row_hash, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (congress, bill_type, bill_number, member_id, relationship) DO UPDATE SET
            title = EXCLUDED.title,
            latest_action_date = EXCLUDED.latest_action_date,
            latest_action_text = EXCLUDED.latest_action_text,
            update_date = EXCLUDED.update_date,
            row_hash = EXCLUDED.row_hash,
            ingested_at = EXCLUDED.ingested_at
        WHERE main.raw_bills.row_hash != EXCLUDED.row_hash
    """, rows)


def load_amendments(db, amendments, congress, bill_type, bill_number):
    """
    Idempotently upsert amendment rows into raw_amendments for a parent bill.
    Primary Key: (congress, bill_type, bill_number, amendment_number).
    """
    if not amendments:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for a in amendments:
        sponsor = a.get("sponsor")
        sponsor_id = sponsor.get("bioguideId") if isinstance(sponsor, dict) else None
        amendment_num = str(a.get("number", ""))
        amendment_type = a.get("type")
        description = a.get("description")
        purpose = a.get("purpose")
        update_date = a.get("updateDate")

        row_hash = _compute_hash(
            congress, bill_type, bill_number, amendment_num, amendment_type,
            description, purpose, sponsor_id
        )
        rows.append((
            congress, bill_type, bill_number, amendment_num, amendment_type,
            description, purpose, sponsor_id, update_date, row_hash, now_iso
        ))

    db.executemany("""
        INSERT INTO main.raw_amendments (
            congress, bill_type, bill_number, amendment_number, amendment_type,
            description, purpose, sponsor_id, update_date, row_hash, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (congress, bill_type, bill_number, amendment_number, amendment_type) DO UPDATE SET
            amendment_type = EXCLUDED.amendment_type,
            description = EXCLUDED.description,
            purpose = EXCLUDED.purpose,
            sponsor_id = EXCLUDED.sponsor_id,
            update_date = EXCLUDED.update_date,
            row_hash = EXCLUDED.row_hash,
            ingested_at = EXCLUDED.ingested_at
        WHERE main.raw_amendments.row_hash != EXCLUDED.row_hash
    """, rows)


