# Docker

The multi-stage Dockerfile of the forecasting API and the `docker compose` file for local
development (API + MLflow) will live here (phase 6). The serving image installs only the
`serve` dependency group: no Spark, no Darts, no PyTorch.
