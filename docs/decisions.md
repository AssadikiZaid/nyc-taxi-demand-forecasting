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
