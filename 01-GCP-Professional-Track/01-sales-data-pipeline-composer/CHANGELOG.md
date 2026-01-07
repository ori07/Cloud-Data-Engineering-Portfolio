# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.5.0] - 2026-01-07
### Added
- **Orchestration Layer**: Created src/main.py as the main entry point, coordinating the flow between validation, transformation, and persistence.

- **Cloud Run Jobs**: Successfully deployed and configured ecommerce-etl-job for serverless execution.

- **Environment Configuration**: Integrated dynamic GCS paths (INPUT_PATH, GOLD_OUTPUT_PATH) via environment variables to decouple infrastructure from code.

### Changed
- **Validation Logic (Breaking Change)**: Refactored source_validator.py to implement the "Transform and Pass" pattern. Functions now return the DataFrame instead of booleans, ensuring data integrity across the pipeline.

- **Deployment Flow**: Migrated build process to Cloud Build and Artifact Registry, eliminating local Docker dependencies.

### Fixed
- **Type Safety**: Resolved TypeError: 'bool' object is not subscriptable in the validation chain by fixing function return types.

- **Data Ingestion**: Fixed schema mismatch errors by standardizing CSV input format (delimiter and header handling).

## [0.4.0] - 2026-01-07

### Added
- **Cloud Build Integration**: Introduced `cloudbuild.yaml` to automate Docker image construction on GCP, establishing a native CI/CD path.

- **Technical Debt Tracking**: Created `tech-debt.md` to formally document design trade-offs, pending refactors, and scalability considerations.

### Changed
- **Dependency Standardization**: Refactored `pyproject.toml` to strictly adhere to modern **Poetry** standards.
- **Lockfile Refresh**: Regenerated `poetry.lock` to ensure cross-dependency consistency and reproducible builds across local and cloud environments.

## [0.3.0] - 2026-01-07
### Added
- **Containerization**: Developed a multi-stage Dockerfile optimized for production using python:3.11-slim as the base image.

- **Container Security**: Implemented a non-root user (pipeline) and isolated development dependencies by leveraging Poetry with the --only main flag.

### Changed
- **I/O Abstraction**: Refactored `io_manager.py` to use a unified file system interface (fsspec/pyarrow.fs), enabling agnostic execution between local environments and GCS.


## [0.2.0] - 2025-12-29

### Added
- **Ruff** configuration in `pyproject.toml` for standardized linting and formatting (rules: E, F, I).
- **GitHub Actions** workflow (`ci.yml`) to automate testing and quality gate validation.
- **Dependency caching** system in CI to accelerate pipeline execution.
- **Pre-commit hooks** configuration to run local validations before every commit.
- Development dependencies: `ruff`, `pre-commit`.

### Changed
- Refactored Poetry installation in CI using the official `snok/install-poetry` action.
- Optimized CI triggers using **path filters** for `src/` and `tests/` directories.

## [0.1.0] - 2025-12-18

### Added
- Base project structure using **Poetry**.
- Implementation of `source_validator.py` with technical validations (Hard Fail).
- Initial transformation layer in `transforms.py` with business logic flags (`Flag`: Anomaly, Refund, Promotion, Standard).
- Unit testing suite with **Pytest** and Factory Fixtures.
