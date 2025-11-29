FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt producer.py ./
RUN pip install --no-cache-dir -r requirements.txt
ENV KAFKA_BOOTSTRAP=kafka:9092
CMD ["python", "producer.py"]
