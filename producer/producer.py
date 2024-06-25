import logging
from kafka import KafkaProducer
import json
import time
import random
from kafka.errors import NoBrokersAvailable

# Set up logging
logging.basicConfig(filename='/app/producer.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

def connect_kafka_producer():
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info("Kafka producer connected.")
            return producer
        except NoBrokersAvailable:
            retries -= 1
            logging.warning("No brokers available. Retrying...")
            time.sleep(5)
    logging.error("No brokers available after retries.")
    raise Exception("Failed to connect to Kafka after multiple retries")

producer = connect_kafka_producer()

def generate_weather_data():
    return {
        "main": {
            "temp": 450.0,  # Very high temperature
            "humidity": 1.0,  # Very low humidity
            "temp_min": 40.0,
            "temp_max": 50.0,
            "pressure": 1000,
            "sea_level": 1000,
            "grnd_level": 950
        },
        "visibility": 10000,
        "wind": {
            "speed": 300.0,  # Very high wind speed
            "deg": random.randint(0, 360),
            "gust": 30.0
        },
        "rain": {"1h": 0.0},  # No rain
        "clouds": {"all": 10},
        "weather": [{"id": 500, "main": "Weather", "description": "clear sky", "icon": "10d"}],
        "coord": {"lon": round(random.uniform(-180, 180), 2), "lat": round(random.uniform(-90, 90), 2)},
        "dt": int(time.time()),
        "sys": {
            "type": 1,
            "id": 1000,
            "country": "Country",
            "sunrise": int(time.time() - 3600),
            "sunset": int(time.time() + 3600)
        },
        "timezone": random.randint(-43200, 50400),
        "id": random.randint(1000, 100000),
        "name": "City",
        "cod": 200
    }

while True:
    weather_data = generate_weather_data()
    try:
        producer.send('weather-data', value=weather_data)
        logging.info(f"Sent data: {weather_data}")
    except Exception as e:
        logging.error(f"Failed to send data: {e}")
        producer = connect_kafka_producer()  # Reconnect if sending fails
    time.sleep(60)  # Wait for 60 seconds before generating new data
