FROM python:3.8-slim

RUN pip install kafka-python

COPY producer.py /app/producer.py

CMD ["python", "/app/producer.py"]
