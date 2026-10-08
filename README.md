

### Getting Started

1. Create a **private** GitHub repository from this archive and push your initial commit.
2. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
3. Run `uv sync` to install dependencies.
4. Get a free [Congress API key](https://api.congress.gov/sign-up/). The inherited script reads it from the `CONGRESS_API_KEY` environment variable — `export CONGRESS_API_KEY=<your key>` in your shell, or copy `.env.example` to `.env` (gitignored) and source it before running. How the key gets into the environment is up to you.
5. `cd dbt` — run all dbt commands from the `dbt/` directory.
6. Verify your setup: `uv run dbt debug`
7. Load seed data into a local DuckDB database: `uv run dbt seed`
8. Back in the project root, run the inherited ingestion: `cd .. && uv run scripts/main.py`
9. Build the inherited dbt models: `cd dbt && uv run dbt build`

Step 8 takes roughly 10 minutes with the inherited defaults (the script makes ~800 API calls). Step 9 is sub-second.

### Run ingestion stages separately

The interactive ingestion menu offers separate House/Senate bills-only and
amendments-only runs. Amendments-only uses the bills already stored in
`raw_bills`. The same modes are available from the CLI:

```powershell
uv run scripts/main.py --house --bills-only --skip-members
uv run scripts/main.py --house --amendments-only
```

Use `--reset` with either command to fully refresh only the selected destination
table. Amendments-only runs require bills to have already been ingested.
The pipeline stages are also callable independently from `scripts.pipeline`:
`ingest_members(...)`, `ingest_member_bills(db, members, ...)`, and
`ingest_bill_amendments(db, bills, ...)`.
Bill API requests use a bounded pool of five worker threads by default; their
database writes are serialized through the calling thread.
Member ingestion now fetches current Congress members directly from the API,
then uses the census crosswalk only to attach House district GEOIDs. The
target-district and target-county member-selection options have been removed.
Amendment refresh decisions track the latest action date separately from
`raw_bills`, so loading bills first does not cause their amendments to be
mistakenly skipped. Once a bill's amendments have been fetched, unchanged bills
are skipped on later runs; use a full amendment refresh to process them again.

The pipeline should run end-to-end. We'd recommend that you verify that before you begin making changes.

> **Note:** The DuckDB database file (`dev.duckdb`) is gitignored and should not be committed. 