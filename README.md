# Movement Labs. Senior Data Engineer Exercise.

## Skills Assessment — Brownfield Handoff

You have inherited a working data pipeline from a previous engineer responding to the scenario described below. It runs end-to-end, but it isn't fully production-ready. Your job is to make deliberate choices about how to improve it, extend it, and get it ready to ship.

This is a **brownfield** exercise. We're hiring for someone who can work across a full codebase, exercise strong judgment about when to refactor and when to leave things alone, and reduce future toil. The most important thing we're evaluating is the quality & clarity of your judgment — what to change, what to leave, and why.

### Time Guidance

**~4 hours of focused effort**, plus a 30-minute live follow-up where we'll discuss your choices.

The four tasks below have approximate time budgets that total roughly 4 hours. Note that strong candidates will differentiate in the written artifacts (DECISIONS.md, AI_USAGE.md, and the operations writeup) as much as in the code, since communication about the data platform is as essential as the code itself.

### Assistance

**You may use AI.** AI use is expected and we don't penalize it. We *do* ask you to disclose how you used it (see `AI_USAGE.md` below). During the live follow-up, you should be able to defend your choices without live help from either AI or another human.

### Review Lens

Your submission will be reviewed the way we'd review a teammate's PR. We're looking for:

- **Specificity of judgment** — does your work indicate thoughtful engagement with the code & its intended purpose?
- **Correctness** — does the refactored pipeline produce the right answer, idempotently?
- **Reliability** — are there tests, freshness checks, and named edge cases?
- **Production readiness** — have your changes made it easier for a teammate to jump in tomorrow?

---

### The Scenario

A civic engagement program is tracking a set of 350 US counties across the United States. The list of target counties is provided as `target_counties.csv` in the `seeds/` directory of the dbt project. The program team wants to understand the legislative activity of US House members whose districts include one or more of these target counties.

Specifically, the program team wants to see:

- **Bills** sponsored or cosponsored by these House members in the current Congress (119th)
- **Amendments** proposed to those bills, regardless of who sponsored the amendment

The previous engineer built this pipeline against that brief. Their solution covers the happy path but has gaps in reliability, idempotency, and observability. Your job is in the four tasks below.

---

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

The pipeline should run end-to-end. We'd recommend that you verify that before you begin making changes.

> **Note:** The DuckDB database file (`dev.duckdb`) is gitignored and should not be committed. Your submission must be fully reproducible — a reviewer should be able to clone your repo, set their API key, run your scripts, then `dbt build` to recreate everything from scratch.

---

### Task 1 — Refactor and harden (~90 min)

`scripts/main.py` is a single ~400-line module that mixes API client, traversal logic, transformation, and database loads. The script is destructive (`CREATE OR REPLACE`) and its data scope is hardcoded.

Make this code ready to hand off:

 - A teammate should be able to navigate it and find where to make a change without
  reading the whole file.
 - A reviewer will run your ingestion twice; the second run should not duplicate rows
  or corrupt the database.
 - A teammate should be able to run a smaller dev subset without editing source code.
 - The dbt models downstream depend on the current table shapes. If you change a
  schema, update the dbt models and call it out in `DECISIONS.md`.

If you run out of time, prioritize your changes and explain your thinking in `DECISIONS.md`.

### Task 2 — Add the missing reliability layer (~75 min)

There are no tests anywhere in this project. There is no source freshness configured.

- Add dbt tests where applicable. Be comprehensive, but deliberate to ensure signal is indicative of real failures and errors
- Configure source freshness on at least one Congress API source. The Congress API exposes update timestamps you can use.
- Fix one or two correctness gaps you discover (or that the prior engineer left). Document what you found and what you chose to leave alone in `DECISIONS.md`.

### Task 3 — Small extension (~45 min)

Add **bill status** tracking to the mart. The program team wants to be able to filter or aggregate by status alongside the existing fields.

Be deliberate about grain. If status changes over time, what does your mart represent — current status only, or status history? Document the decision in `DECISIONS.md`.

### Task 4 — Operations & Architecture writeup (~30 min)

Add a one-page document (`OPERATIONS.md`, committed to the repo) covering:

- **Scheduling.** How would this pipeline run on a recurring basis? What's incremental vs. full-refresh, and why?
- **Observability.** What signals would tell you the pipeline is healthy? Where would alerts go, and at what threshold?
- **Edge cases.** Walk through behavior for at least three of: at-large districts (`AL`), non-voting delegates (Puerto Rico, DC, etc.), mid-Congress vacancies, party switches, withdrawn cosponsorships, redistricting between the 2020 and 2030 cycles.
- **Scale.** What changes at 10x — 3,000 counties? Adding Senate activity? Adding state legislatures?

**A short, specific document beats a long, generic one.**

---

### Required short artifacts

In addition to the four tasks above, commit two short markdown files to the repo root. Templates are provided as starting points.

**`DECISIONS.md`** — For each significant choice you made, 2–4 sentences covering: what you chose, what alternative you rejected, and why. At minimum, name 2–3 things you chose *not* to fix and explain the call.

**`AI_USAGE.md`** — If you used AI, write 5–10 sentences describing where AI helped you, where you chose not to use it, and any example of a case where you overrode an AI suggestion. We don't penalize AI use; we do want to understand how you fit AI into your workflow.

If you created any skills, agents, AGENT.md-type-files, or other artifacts while completing the exercise, please include them in your submission.

---

### Submission

Invite the GitHub users specified in your instructions email as Outside Collaborators to your **private** repository. Please don't make the repository public and don't collaborate with other people — this should be your own work. Reply to your instructions email with a link to your repository.

Expect a 30-minute follow-up where we'll ask you to walk through specific choices in your own words without live AI assistance.
