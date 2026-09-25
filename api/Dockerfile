FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ api/
RUN useradd --create-home app
USER app

EXPOSE 8000
# One worker per container: scale with replicas (compose --scale / K8s), not in-process workers.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
