FROM python:3.11-slim

# Prevent python buffering logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml .

COPY src ./src

RUN pip install --upgrade pip && pip install .

# Make src importable
ENV PYTHONPATH=/app/src

CMD ["python", "-m", "pipelines.flights_pipeline"]