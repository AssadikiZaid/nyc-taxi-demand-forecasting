# NYC taxi demand forecasting

Hourly forecasts of yellow-taxi pickups for each New York City taxi zone, 24 hours ahead,
with P10 / P50 / P90 quantiles.

> **Status: work in progress.** Phase 0 (project setup) is done. No model has been trained
> yet, so this README reports no results. Every figure added later will come from a tracked,
> reproducible run.

## What the project does

Every day at midnight (New York time), the system forecasts the number of taxi pickups in
each zone for each of the next 24 hours. It is built on the public
[NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
enriched with hourly weather and a holiday calendar.

Four approaches will be compared on the same rolling-origin backtest: a seasonal naive
baseline, Prophet (one model per zone), a global LightGBM model on lag features, and a
Temporal Fusion Transformer (Darts). The best one is served by a containerized FastAPI
service on Google Cloud Run, retrained monthly by Airflow and monitored for drift with
Evidently.

## Roadmap

| Phase | Content | Status |
| --- | --- | --- |
| 0 | Project setup: tooling, tests, CI | Done |
| 1 | Ingestion and PySpark pipeline: complete zone x hour panel | Not started |
| 2 | Weather and calendar enrichment, exploratory analysis | Not started |
| 3 | Rolling-origin backtest, metrics, MLflow tracking, seasonal baseline | Not started |
| 4 | Global LightGBM with lag features and quantile objectives | Not started |
| 5 | Prophet and TFT on the same folds | Not started |
| 6 | FastAPI service and Docker image | Not started |
| 7 | CI/CD and deployment to Cloud Run | Not started |
| 8 | Airflow monthly retraining and Evidently drift reports | Not started |
| 9 | Documentation and architecture diagram | Not started |

## Getting started

Requirements: Linux, macOS or WSL2 on Windows, with `git`, `make`, `curl` and
[uv](https://docs.astral.sh/uv/). Java 17 or later is needed from phase 1 (PySpark).
[docs/development_setup.md](docs/development_setup.md) walks through a fresh WSL2 install.

```bash
git clone <repository-url>
cd nyc-taxi-demand-forecasting
make doctor   # check the required tools
make setup    # create the environment (Python 3.12, core + dev tools) and git hooks
make test     # run the test suite
```

Run `make` with no argument to list every command.

## Repository layout

```
├── configs/              # project configuration (period, zones, horizon, quantiles)
├── src/taxi_forecast/
│   ├── ingestion/        # TLC files, taxi zones, weather, holidays
│   ├── pipeline/         # PySpark jobs: cleaning, hourly aggregation, complete panel
│   ├── features/         # calendar, weather, lags and rolling statistics, leakage-free
│   ├── models/           # baseline, LightGBM, Prophet, TFT behind one interface
│   ├── evaluation/       # rolling-origin backtest and metrics
│   ├── monitoring/       # Evidently drift reports
│   └── serving/          # FastAPI application
├── tests/
├── dags/                 # Airflow DAGs
├── notebooks/            # exploratory analysis only
├── docker/               # service image and local compose file
├── scripts/              # helper scripts
└── docs/                 # setup guide and design decisions
```

Optional dependency groups keep each environment small: `spark`, `models`, `dl` (Darts and
PyTorch) and `serve`. For example, `uv sync --extra spark` adds PySpark.

## Design decisions

Choices and their reasons are logged in [docs/decisions.md](docs/decisions.md).

## License

[MIT](LICENSE)
