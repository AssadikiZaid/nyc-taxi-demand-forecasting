.DEFAULT_GOAL := help
.PHONY: help setup setup-all doctor lint format test check data panel quality-report pipeline clean

# Optional dependency groups used by everyday commands (see pyproject.toml).
EXTRAS ?= --extra spark
RUN := uv run $(EXTRAS)

# Months processed by the data commands. Defaults to the development sample;
# use for example `make pipeline START=2023-01 END=2026-08` for the full period.
START ?= 2025-01
END ?= 2025-03

help: ## Show the available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Create the environment (core, Spark, dev tools) and install the git hooks
	uv sync $(EXTRAS)
	$(RUN) pre-commit install

setup-all: ## Same as setup, plus every optional group (models, deep learning, serving)
	uv sync --all-extras
	uv run --all-extras pre-commit install

doctor: ## Check that uv, Python 3.12, Java 17+, git and make are available
	@bash scripts/check_env.sh

lint: ## Check style and formatting without changing files
	$(RUN) ruff check .
	$(RUN) ruff format --check .

format: ## Fix style and formatting in place
	$(RUN) ruff check --fix .
	$(RUN) ruff format .

test: ## Run the test suite (Spark tests need Java)
	$(RUN) pytest

check: lint test ## Run lint and tests, as the CI does

data: ## Download the TLC files and the zone table for START..END (default 2025-01..2025-03)
	$(RUN) python -m taxi_forecast.ingestion.tlc --start $(START) --end $(END)

# Months already built from the same raw file with the current rules are skipped;
# `make panel FORCE=1` rebuilds every month of START..END.
PANEL_FLAGS := $(if $(FORCE),,--skip-up-to-date)

panel: ## Build the zone x hour demand panel for START..END with PySpark (new months only)
	$(RUN) python -m taxi_forecast.pipeline.demand_hourly --start $(START) --end $(END) $(PANEL_FLAGS)

quality-report: ## Summarize the monthly quality reports into docs/data_quality.md
	$(RUN) python -m taxi_forecast.pipeline.quality_report

pipeline: data panel ## Download and build the panel for START..END in one go

clean: ## Remove caches and build artifacts (keeps data and the environment)
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov build dist
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
