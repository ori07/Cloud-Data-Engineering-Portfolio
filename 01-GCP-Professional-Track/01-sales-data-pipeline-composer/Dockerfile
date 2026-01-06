# === Stage 1: Builder ===
FROM python:3.11-slim AS builder

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

# Install system dependencies needed for Poetry and potential project dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    pip install poetry==1.7.1

# Copy project files (pyproject.toml and poetry.lock)
COPY pyproject.toml poetry.lock ./

# Install project dependencies into the system's Python environment
RUN poetry install --only main --no-root && rm -rf $POETRY_CACHE_DIR

# === Stage 2: Final Runtime ===
FROM python:3.11-slim AS runtime

# We create a system user to not use the root user to run the app
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

# Setting the work directory
WORKDIR /home/pipeline/app

# We copy only the buider's necessary libraries
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# We copy the source code
COPY src/ ./src/

# Setting the permitions
RUN chown -R pipeline:pipeline /home/pipeline/app
USER pipeline

# Set PYTHONPATH so folder src can be located
ENV PYTHONPATH=/home/pipeline/app/src

# Command to run the application
CMD ["python", "src/main.py"]
