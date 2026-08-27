# Decisions

For each significant choice you made, 2–4 sentences covering: what you chose, what alternative you rejected, and why.

- **Modularization of Ingestion Pipeline**: Separated the monolithic `scripts/main.py` into dedicated modules (`config.py`, `database.py`, `api.py`, `ingestion.py`). This isolates concerns (environment configuration, network/API client, schema management, and database persistence), making the codebase easy to maintain and test without whole-file context.
- **Incremental Timestamp Tracking & Checksumming**: Implemented `update_date` comparison to skip re-fetching amendments for unchanged bills on subsequent runs, combined with row-level MD5 checksums (`row_hash`) and `ON CONFLICT DO UPDATE` upserts. We rejected blindly dropping and recreating tables or full-refreshing every endpoint because fetching ~800 amendment endpoints is network-bound (~10 mins); timestamp tracking makes repeat runs sub-second while checksums guarantee deterministic idempotency.
- **Configurable Setup vs. Ingestion**: Made table recreation/reset an explicit option rather than running `CREATE OR REPLACE` unconditionally on every execution. This enables reviewers and teammates to execute ingestion multiple times against an existing database state safely without data loss.
- **Adding Validation, Tests and Mock Data**:
- **Deterministic District Traversal & CLI Scoping**: Replaced random district selection with deterministic ordering (`ORDER BY census.GEOID_CD119_20`) and added CLI arguments (`--full`, `--limit`, `--random`, `--reset`, `--no-reset`). We rejected hardcoded limits in source code because teammates need the flexibility to run reproducible dev subsets or full production runs directly from the command line. We deliberately scope the dev subset by *member* (`--limit`) rather than by bills-per-member: capping bills would write bills into `raw_bills` whose amendments were never fetched, and the incremental `update_date` skip would then treat those bills as "up to date" and never backfill their amendments on a later full run — silently violating the `raw_amendments = all amendments to target bills` invariant.


## What you chose not to fix

_Name 2–3 specific gaps you saw and chose not to fix. For each, what would break if it became real, and why did you defer?
- ** Put text queries into sepearate file.

## Other decisions

_Use this space for any choice you want a reviewer to understand — refactor scope, dependency picks, anything you'd flag in a PR description._
- Validate table creations
- PR Review Question? Do we need to recreate tables on every run?
- Add unit tests.
- PR Review Question? Was it a intentional decision to only fetch 5 random congress members.