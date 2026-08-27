# AI Usage

5–10 sentences describing how you used AI on this exercise. AI use is allowed and expected; we don't penalize it. The disclosure itself is a signal we read.

Please cover:

- **Where AI helped.** Which tasks, which decisions, which kinds of work did you reach for AI on? 
- **Where AI enabled you to improve your work product.**
- **Where you chose not to use it.** Anything you deliberately did by hand, and why.
- **Any case where you overrode an AI suggestion.** What did the model propose, what did you do instead, and what did the model miss?

You can write this as a few short paragraphs or a tight bulleted list — whichever fits how you actually used it.

### Few sentences
- I used AI to validate what i thought would be best. Keep a to do list of what needs to be done. Python is sensitive to formatting. Using AI for code generation and copying and pasting is the best choice to avoid small errors that can be overlooked because of a small space.
- I did not use AI to move small amounts such as a single function, or make very small details changes.
- When discussing AI only wanted to use updated_date or checksum. I countered with using both to have the desired outcome.
- AI wanted to add BILL PER MEMBER. I removed this flow. I thought it would be good to add for subsets but was getting out of scope.

### Activity Log
- Extracted configuration settings and environment variable management from `scripts/main.py` into a dedicated `scripts/config.py` module to begin modularizing the ingestion pipeline.
- Extracted database loading functions (`load_members`, `load_bills`, `load_amendments`) from `scripts/main.py` into `scripts/ingestion.py` to isolate persistence logic.
- Extracted database table initialization (`create_tables`) from `scripts/main.py` into `scripts/database.py` to isolate database schema definition.
- Extracted Congress API interactions, pagination, and fetching logic (`api_get`, `paginate`, `fetch_*`) from `scripts/main.py` into `scripts/api.py`.
- Separated database setup and ingestion flows in `scripts/main.py` into dedicated functions (`setup_database` and `run_ingestion`) and added interactive terminal prompts to make table recreation optional.
- Implemented incremental timestamp tracking (`update_date`) and MD5 content hashing (`row_hash`) with `ON CONFLICT` upserts in `scripts/ingestion.py` and `scripts/database.py` to bypass unchanged amendment API calls and ensure idempotent writes.
- Added terminal prompt input validation with retry looping in `prompt_reset_tables()`, seed table existence validation in `database.py` (`check_tables_exist`, `validate_seed_tables`), and created unit test `tests/test_main_prompt.py`.
- Prepared mock datasets and in-memory DuckDB helper fixtures in `scripts/mock_data.py` for isolated unit testing.
- Created `tests/test_ingestion_validation.py` to test schema existence checks, member/bill/amendment ingestion, checksum calculation, idempotency on repeat runs, and in-place updates on content changes without touching production data.
- Added bill status tracking (Task 3, current-status-only): ingested `latestAction.{actionDate,text}` into new `raw_bills.latest_action_date/latest_action_text` columns (folded into `row_hash`), derived a coarse `bill_status` bucket in `stg_bills`, and carried status through `int_legislative_activity` (amendments inherit parent status) into `mart_legislative_activity`; verified end-to-end against an isolated scratch DuckDB without touching `dev.duckdb`.
