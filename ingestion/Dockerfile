FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ingestion/
RUN useradd --create-home app
USER app

CMD ["python", "-m", "ingestion.main"]
