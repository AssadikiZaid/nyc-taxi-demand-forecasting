.DEFAULT_GOAL := help
.PHONY: help setup setup-all doctor lint format test check clean

help: ## Show the available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Create the environment (core + dev tools) and install the git hooks
	uv sync
	uv run pre-commit install

setup-all: ## Same as setup, plus every optional group (Spark, models, deep learning, serving)
	uv sync --all-extras
	uv run pre-commit install

doctor: ## Check that uv, Python 3.12, Java 17+, git and make are available
	@bash scripts/check_env.sh

lint: ## Check style and formatting without changing files
	uv run ruff check .
	uv run ruff format --check .

format: ## Fix style and formatting in place
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run the test suite
	uv run pytest

check: lint test ## Run lint and tests, as the CI does

clean: ## Remove caches and build artifacts (keeps data and the environment)
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov build dist
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
