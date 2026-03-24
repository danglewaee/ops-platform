FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY pyproject.toml README.md /app/
COPY ops_platform /app/ops_platform
COPY scripts /app/scripts
COPY docs /app/docs

RUN pip install --no-cache-dir -e .[full]

EXPOSE 8000

CMD ["python", "scripts/run_api.py"]
