FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Native XGBoost runtime dependency on the slim Python image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system medical \
    && useradd --system --gid medical --home-dir /app medical

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY configs ./configs
COPY models ./models
COPY src ./src
COPY scripts ./scripts
COPY data ./data

RUN chown -R medical:medical /app

USER medical

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "from src.realtime_medical.config import AppConfig; AppConfig.load_config()" || exit 1

CMD ["python", "-m", "src.realtime_medical.main"]
