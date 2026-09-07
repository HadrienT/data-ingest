FROM python:3.11-slim

WORKDIR /app

# Dependency metadata first, so editing a source does not reinstall every wheel.
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

ENTRYPOINT ["ingest"]
CMD ["run", "--all"]
