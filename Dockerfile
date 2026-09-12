FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MINIFLOW_DB=/data/miniflow.db

WORKDIR /app

COPY pyproject.toml README.md ./
COPY miniflow ./miniflow
RUN pip install --no-cache-dir .

RUN mkdir -p /data
EXPOSE 8000

CMD ["miniflow", "serve", "--host", "0.0.0.0", "--port", "8000"]
