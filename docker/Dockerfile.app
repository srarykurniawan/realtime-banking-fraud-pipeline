# Simulator (Kafka producer) + Investigation console
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 FRAUD_CONFIG_DIR=/app/config FRAUD_SQL_DIR=/app/sql
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir "pandas>=2.2,<3" ".[kafka,postgres,console]"
COPY config ./config
COPY sql ./sql
COPY apps ./apps
RUN useradd -m -u 1001 app && mkdir -p /data && chown -R app /data
USER app
