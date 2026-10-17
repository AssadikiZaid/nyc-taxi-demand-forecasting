# Design decisions

Each entry records a choice, the alternatives considered and why this one was kept.
New entries are added at the bottom as the project moves through its phases.

## D-001 · `src` layout with a single package

**Decision.** All code lives in `src/taxi_forecast/`, split into subpackages by stage
(ingestion, pipeline, features, models, evaluation, monitoring, serving).

**Why.** With a `src` layout, tests import the installed package rather than whatever
happens to be in the current folder, so a missing file or dependency fails in CI instead
of in production. Notebooks, DAGs and the API all import the same functions.

## D-002 · uv for Python versions, environments and the lockfile

**Alternatives.** pip + venv, Poetry, conda.

**Why.** One tool installs the right Python, creates the environment and writes a
cross-platform lockfile (`uv.lock`). The CI installs exactly the locked versions
(`uv sync --locked`), so a run on GitHub uses the same packages as a run on a laptop.

## D-003 · Python 3.12

**Why.** Every library of the stack supports it. Darts requires at least 3.11, and the
current NumPy (2.5) requires 3.12, so 3.11 would already pin older releases.

## D-004 · Optional dependency groups

**Decision.** Core dependencies (pandas, NumPy, PyArrow, PyYAML, holidays) are shared.
Heavier ones sit in optional groups: `spark`, `models`, `dl` and `serve`. Development tools
are in a separate `dev` group.

**Why.** The serving image installs only `serve`, without Spark, Darts or PyTorch, which
keeps it small and quick to start on Cloud Run. PyTorch alone weighs several gigabytes and
is only needed for the TFT.

## D-005 · Ruff for linting and formatting, enforced twice

**Why.** One fast tool replaces flake8, isort and black. It runs on every commit through
pre-commit and again in CI, so style issues never reach `main`. Docstrings follow the
Google convention.

## D-006 · Minimal CI from phase 0

**Decision.** A GitHub Actions workflow runs lint and tests on every pull request and on
pushes to `main`. Docker builds and deployment are added in phase 7.

**Why.** It costs nothing on a public repository and protects the project from the first
commit.

## D-007 · One YAML configuration file

**Decision.** Period, zones, weather point, horizon and quantiles live in
`configs/project.yaml`, loaded and validated by `taxi_forecast.config`.

**Why.** No magic numbers scattered in the code, and an invalid value (for example
quantiles without the median) fails immediately with a clear message.

## D-008 · pandas 2.3 rather than pandas 3

**Decision.** pandas is pinned below 3 (`pandas>=2.3,<3`).

**Why.** PySpark 4.2 warns that it does not fully support pandas 3 yet. Every other library
of the stack works with pandas 2.3, so the pin costs nothing and removes a source of subtle
conversion bugs between Spark and pandas. To be revisited when PySpark supports pandas 3.

## D-009 · Timestamps: New York wall clock in, UTC hours out

**Context.** The TLC data dictionary describes `tpep_pickup_datetime` as the moment the
meter was engaged but states no time zone. The values are New York wall-clock times; the
quality report checks it by showing the busiest and quietest hours of the day.

**Decision.** The panel is indexed by UTC hour. Each pickup is counted in its wall-clock
hour, then that hour is mapped to UTC (`taxi_forecast.pipeline.hours`):

- the hour skipped in March (02:00) does not exist; trips stamped in it are clock errors
  and are moved to 03:00, as Java and Spark do;
- the hour repeated in November (01:00) cannot be told apart in the data; its trips are
  split evenly between the two UTC hours.

A "month" is always a calendar month in New York time, like the TLC files.

**Why.** In UTC every hour exists exactly once, so a lag of 24 or 168 rows is always 24 or
168 hours. Indexing by wall clock would create a missing hour in March and a doubled hour
in November, which would corrupt the lags around those dates. Splitting the repeated hour
avoids both a spike and an artificial zero. Calendar features (hour of day, day of week)
are still computed in New York time.

**Alternative.** Keep all trips of the repeated hour in the first UTC hour: simpler, but it
creates a doubled hour followed by an empty one once a year.

## D-010 · One month at a time in Spark

**Decision.** The Spark job reads, cleans and aggregates one monthly file at a time, and
writes it as its own partition (`year=YYYY/month=M`), replacing that partition only.

**Why.** Memory stays bounded whatever the length of the period: a synthetic month of
3.5 million trips takes about 50 seconds and 2.5 GB of RAM on two cores. A month can be
re-run on its own (idempotent), which is what the monthly Airflow DAG will do. Reading
months separately also sidesteps the small schema differences between TLC years.

## D-011 · Quality rules, counted one by one

**Decision.** The panel counts every pickup that happened, exactly once. Exact duplicate
rows are removed first. Each remaining trip is then flagged with the first rule it breaks,
in this order: pickup outside the file's month, invalid zone (missing, unknown, or
264-265), reversal record, negative duration, duration over 6 hours, no ride (the taxi did
not move and the meter ran for less than 60 seconds), distance over 100 miles. The counts
per rule are written to a JSON report per month and summarized in `docs/data_quality.md`.

**Why.** Counting each trip under one rule only makes the counts add up to the number of
trips removed, so the report can be checked by simple addition. Thresholds live in
`configs/project.yaml`. The first version of the rules was replaced after looking at the
real data (D-013); every report records the `rules_version` that produced it, and reports
from different versions are never summarized together.

## D-012 · A complete panel with explicit zeros, checked twice

**Decision.** The panel is the full grid of 263 zones x every UTC hour of the month,
left-joined to the pickup counts, with 0 where no trip started. Before a month is accepted,
the job checks that the grid has exactly one row per zone and hour, and that the panel
holds exactly as many pickups as trips kept.

**Why.** Without explicit zeros, the row "24 rows earlier" is not "24 hours earlier" for
quiet zones, and every lag feature is silently wrong. The two checks turn that kind of bug
into an immediate error.

## D-013 · Rules revised after checking January-March 2025 (rules version 2)

**Context.** The first rules (version 1) dropped every trip with a zero distance or a
duration of zero or less. On the real data of January to March 2025 (11.2 million trips),
three checks showed that they removed real pickups and kept duplicates:

| Finding | Evidence | Effect of version 1 |
| --- | --- | --- |
| Vendor 7 never records the dropoff time | 100 % of its trips have dropoff = pickup, with normal distances and fares; its share grows from 0.03 % (January) to 0.52 % (March) | All its trips dropped: a demand drop that would grow over time and look like a trend |
| 2.6 % of trips have a zero distance | 60 % of them last over a minute, cost more than zero and are not voided: mostly "flex fare" trips (payment type 0) whose distance is not recorded | Real pickups dropped, unevenly: 2.4 % of trips in a typical top-50 zone, up to 6 % in Harlem |
| 1.4 % of trips are reversal records | A negative fare whose trip (same vendor, pickup, dropoff and zones) also appears with the opposite fare; payment types are dispute (63 %), cash (24 %) and no charge (13 %) | The same pickup counted twice; JFK and LaGuardia are the most affected zones |

Negative fares alone are not a reliable signal: in March, 3.6 % of trips have a negative
`fare_amount` and no twin, and 95 % of those are flex-fare trips with a positive total
(92 % with a real distance).

**Decision (version 2).** Keep a trip whose dropoff equals its pickup when the taxi moved;
keep a trip with no distance when the meter ran for at least 60 seconds; drop the
negative-fare twin of a reversed trip; drop only negative durations (dropoff before pickup,
298 trips in three months).

**Result.** 97.7 % of trips are kept instead of 96.9 %. Over the 50 busiest zones (92 % of
pickups), the counts change by -4.7 % (JFK, where version 1 counted reversals twice) to
+4.5 % (Central Harlem and Morningside Heights, where it dropped flex-fare trips).

## D-014 · Incremental runs, robust reads

**Decision.** `make panel` skips a month when its quality report was written by the current
rules version from a raw file of the same size, so a run over the full period only builds
new or changed months (`make panel FORCE=1` rebuilds everything). A raw file whose
timestamps are stored in nanoseconds, which Spark cannot read, is converted to
microseconds in a temporary copy first.

**Why.** The monthly Airflow DAG (phase 8) only needs to process the newly published month,
and an interrupted run over 44 months can resume where it stopped. A rules change bumps the
version, which forces a rebuild of every month, so the panel never mixes two versions.

## D-015 · A Spark job sized for a laptop

**Context.** On an 8 GB Windows laptop, WSL2 sees about half of the RAM. The first full
run took 20 minutes for one month, against about one minute on a machine with more memory:
the JVM's 3 GB heap pushed the VM into swap.

**Decision.** Exact duplicates are found on a 64-bit hash of all the raw columns plus the
seven columns the rules use, instead of shuffling all twenty columns, and the narrow rows
are cached once. The driver memory defaults to 40 % of the visible RAM, between 1 and 3 GB
(`SPARK_DRIVER_MEMORY` overrides it), and the job logs the memory it uses.

**Result.** On January 2023, peak memory falls from 3.2 GB to 1.6 GB with a 1 GB driver,
the run takes 44 seconds instead of 63 on the same machine, and the counts are identical.
