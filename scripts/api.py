"""
scripts/api.py

Congress API client handling HTTP requests, rate throttling, pagination,
and data fetching routines.
"""

from pathlib import Path
from urllib.parse import urlparse, parse_qs
import sys
import time

import requests

# Add script folder to path if executed standalone or as module
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from config import API_KEY, BASE_URL, CONGRESS, HOUSE_BILL_TYPES, MEMBER_LIMIT
except ImportError:
    from scripts.config import API_KEY, BASE_URL, CONGRESS, HOUSE_BILL_TYPES, MEMBER_LIMIT


def api_get(url: str, params: dict | None = None, retries: int = 5) -> dict:
    """
    Execute an HTTP GET request against the Congress API with throttling and retry on 5xx errors.
    """
    query = {"api_key": API_KEY, "format": "json"}
    if params:
        query.update(params)

    for attempt in range(retries):
        time.sleep(0.1)  # throttle every request
        response = requests.get(url, params=query)

        # 1. Handle Rate Limiting (429)
        if response.status_code == 429:
            # Respect the API's Retry-After header if provided, else default to a longer sleep (e.g., 60s)
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited (429). Waiting {retry_after}s before retrying...")
            time.sleep(retry_after)
            continue
        #  Handle Server Errors
        if response.status_code >= 500:
            wait = 2 ** (attempt + 1)
            print(f"  {response.status_code} on attempt {attempt + 1}, retrying in {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Failed after {retries} retries: {url}")


def paginate(url: str, result_key: str, params: dict | None = None) -> list:
    """
    Collect all pages of results for a Congress API endpoint.

    The API returns a pagination.next URL in the response body when more pages
    exist. We follow it until exhausted, accumulating all results into one list.
    We request 250 items per page (the API maximum) to minimise round trips.
    """
    first_page_params = {**(params or {}), "limit": 250}
    results = []
    next_url = url

    while next_url:
        # Parse any query params embedded in next_url by the API
        parsed_url = urlparse(next_url)
        url_without_params = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        url_params = {k: v[0] for k, v in parse_qs(parsed_url.query).items()}

        merged_params = {first_page_params,url_params}
        data = api_get(url_without_params, params=merged_params)

        results.extend(data.get(result_key, []))
        next_url = data.get("pagination", {}).get("next")

    return results


def fetch_members_for_districts(districts: list, member_limit: int | None = MEMBER_LIMIT) -> list:
    """
    For each target district, fetch House members from the Congress API scoped
    to the target Congress.

    Endpoint: /member/congress/{congress}/{stateCode}/{district}

    Deduplicates by bioguideId — a member's district may overlap multiple target
    counties, but the member should only appear once in raw_members.
    """
    members = []
    seen_ids = set()

    for state_abbr, district_num, geoid_cd, geoid_county in districts:
        if member_limit and len(members) >= member_limit:
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


def fetch_legislation_for_member(bioguide_id: str) -> tuple[list, list]:
    """
    Fetch all bills sponsored and cosponsored by a member in the target Congress.

    Returns two separate lists so the caller can store the correct relationship
    ('sponsor' or 'cosponsor') for each bill in raw_bills.
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


def fetch_amendments_for_bill(congress: int, bill_type: str, bill_number: str) -> list:
    """
    Fetch all amendments to a bill, regardless of who sponsored them.
    Treats 404 as an empty list (bill has no amendments).
    """
    url = f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/amendments"
    try:
        return paginate(url, "amendments")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
