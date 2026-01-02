# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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