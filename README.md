# NYC taxi demand forecasting

Hourly forecasts of yellow-taxi pickups for each New York City taxi zone, 24 hours ahead,
with P10 / P50 / P90 quantiles.

> **Status: work in progress.** The data pipeline (phase 1) is being validated on real
> data. No model has been trained yet, so this README reports no results. Every figure added
> later will come from a tracked, reproducible run.

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
| 1 | Ingestion and PySpark pipeline: complete zone x hour panel | In progress |
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
git clone https://github.com/AssadikiZaid/nyc-taxi-demand-forecasting.git
cd nyc-taxi-demand-forecasting
make doctor   # check the required tools
make setup    # create the environment (Python 3.12, PySpark, dev tools) and git hooks
make test     # run the test suite
```

Run `make` with no argument to list every command.

## Data pipeline

```bash
make pipeline                              # January to March 2025, the development sample
make pipeline START=2023-01 END=2026-08    # the full period
```

`make pipeline` runs two steps, which can also be called one by one:

1. `make data` downloads the monthly TLC files and the taxi zone table. Files already on
   disk are skipped, so the command can be re-run safely.
2. `make panel` builds, with PySpark, the hourly number of pickups for every taxi zone and
   every UTC hour, with explicit zeros, in `data/processed/demand_hourly/`. Each month also
   gets a JSON quality report in `data/quality/`. Months already built with the current
   rules are skipped; `make panel FORCE=1` rebuilds them.

`make quality-report` then summarizes the monthly reports in
[docs/data_quality.md](docs/data_quality.md).

The cleaning rules, the time-zone handling and the checks run on every month are explained
in [docs/decisions.md](docs/decisions.md) (D-009 to D-016): the rules were revised after
checking the real data, which showed one vendor never recording dropoff times, some
trips appearing twice through reversal records and real trips crossing the fall-back
hour. Facts about the data worth knowing before modeling are in
[docs/data_notes.md](docs/data_notes.md). Data files stay out of git; set
`TAXI_FORECAST_DATA_DIR` to keep them on another drive.

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
