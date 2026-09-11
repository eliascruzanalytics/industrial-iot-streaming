FROM python:3.12-slim

WORKDIR /app

COPY bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge/ /app/bridge/

ENV PYTHONPATH=/app

CMD ["python", "bridge/mqtt_to_kafka.py"]
