# AI Usage

5–10 sentences describing how you used AI on this exercise. AI use is allowed and expected; we don't penalize it. The disclosure itself is a signal we read.

Please cover:

- **Where AI helped.** Which tasks, which decisions, which kinds of work did you reach for AI on? 
- **Where AI enabled you to improve your work product.**
- **Where you chose not to use it.** Anything you deliberately did by hand, and why.
- **Any case where you overrode an AI suggestion.** What did the model propose, what did you do instead, and what did the model miss?

You can write this as a few short paragraphs or a tight bulleted list — whichever fits how you actually used it.

### Activity Log
- Extracted configuration settings and environment variable management from `scripts/main.py` into a dedicated `scripts/config.py` module to begin modularizing the ingestion pipeline.
- Extracted database loading functions (`load_members`, `load_bills`, `load_amendments`) from `scripts/main.py` into `scripts/ingestion.py` to isolate persistence logic.
- Extracted database table initialization (`create_tables`) from `scripts/main.py` into `scripts/database.py` to isolate database schema definition.
- Extracted Congress API interactions, pagination, and fetching logic (`api_get`, `paginate`, `fetch_*`) from `scripts/main.py` into `scripts/api.py`.
- Separated database setup and ingestion flows in `scripts/main.py` into dedicated functions (`setup_database` and `run_ingestion`) and added interactive terminal prompts to make table recreation optional.
- Implemented incremental timestamp tracking (`update_date`) and MD5 content hashing (`row_hash`) with `ON CONFLICT` upserts in `scripts/ingestion.py` and `scripts/database.py` to bypass unchanged amendment API calls and ensure idempotent writes.
