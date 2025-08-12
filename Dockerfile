FROM python:3.11-slim

WORKDIR /app

# System deps (optional)
RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV UVICORN_PORT=8000

EXPOSE 8000

CMD ["uvicorn", "quant.orchestrator.service:app", "--host", "0.0.0.0", "--port", "8000"]