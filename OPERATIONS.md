How would this pipeline run on a recurring basis? What's incremental vs. full-refresh, and why?
*    Right now theres the option to do a recurring or full refresh. Full refresh load the entire dataset again. Incremental Refresh only refresh data that has changes.
*    Theres also the option to run a subset of congress members vs all members. Expectations for repeated run would be Incremental.
*    Full refreshes resolves gaps and prevents possible ttl removals.

What signals would tell you the pipeline is healthy? Where would alerts go, and at what threshold?
*     If runtime is at or below the expected runtime at specific threshold such as full or incremental run times.
*     Runs succeding, Run starting
*  Alerts needed for run time, run execution, run success, How many amendments bills processed.  
* Thresholds should be determined based on the amount processed or a default. Having a static or dynamic threshold. I'm leaning towards dynamic

`Edge cases. Walk through behavior for at least three of: at-large districts (AL), non-voting delegates (Puerto Rico, DC, etc.), mid-Congress vacancies, party switches, withdrawn cosponsorships, redistricting between the 2020 and 2030 cycles.
* Party Switches - members are re-fetched every run. Currenlty fetching only 5 members as default. Will have stale data frequently if consistent default runs.
* Mid Congress Vacancy - Does not impact pipeline. however members of congress not returning for whatever reason is a gap.
* 


Scale. What changes at 10x — 3,000 counties? Adding Senate activity? Adding state legislatures?
* Drastically increasing the time to run.
* Creating separate pipelines would be needed.
* Limiting what can be done to the data while ingesting. Such as transformations grouping aggregations.
* Reads taking longer.