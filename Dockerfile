FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir '.[web]' pytest

EXPOSE 9999

# Run unit tests first, then smoke test
CMD ["sh", "-c", "python -m pytest tests/ -x -q --tb=short 2>&1 && python tests/smoke_test.py"]
