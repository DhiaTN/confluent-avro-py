.DEFAULT_GOAL:=help
.PHONY: help tests install develop uninstall deps-check clean clean-build clean-test format lint
PACKAGE="avrokafka"

help:  ## Display this help menu
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

tests:  ## Run tests and save coverage report to htmlcov
	@pytest -n 4 --cov-report html:htmlcov --cov=$(PACKAGE)

install: clean  ## Install the package, w/dependencies
	@python setup.py install

develop: clean ## Install the package in "Development Mode", w/dependencies
	@python setup.py develop

uninstall: clean  ## Uninstall the package, w/o dependencies
	@pip uninstall $(PACKAGE)

deps-check:  ## Check outdated packages and security updates
	@pipenv update --dry-run  # Check outdated packages
	@pipenv check # Check security updates

clean: clean-build clean-test ## Remove all build, test and coverage artifacts

clean-build: ## Remove build artifacts
	@rm -fr build/
	@rm -fr dist/
	@rm -fr .eggs/
	@find . -name '*.egg-info' -exec rm -fr {} +
	@find . -name '*.egg' -exec rm -f {} +

clean-test: ## Remove test and coverage artifacts
	@rm -f .coverage
	@rm -fr htmlcov/
	@rm -fr .mypy_cache
	@rm -fr .pytest_cache

lint: ## Check code style and typing annotations
	@echo "Running code-style check..."
	@isort --check-only -rc avrokafka tests # Run code style checks
	@black --check avrokafka tests # Run code style checks
	@echo "Running static-type checker..."
	@mypy avrokafka tests # Run compile-time type checking (PEP 484)

format: ## Check the code style, sort imports and write the files back
	@isort -ac -rc avrokafka tests
	@black avrokafka tests

