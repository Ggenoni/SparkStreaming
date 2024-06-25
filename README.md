docker-compose -p streaming up --build -d

docker ps

docker exec -it streaming-spark-1 /bin/bash

cat /opt/bitnami/spark/scripts/spark_consumer.py

#/opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/scripts/spark_consumer.py

/opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/scripts/spark_consumer.py


#for producing

docker exec -it streaming-kafka-producer-1 /bin/bash

tail -f /app/producer.log

#for message topic

docker exec -it kafka-container /bin/bash

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather-data --from-beginning

version: '3.8'
services:
zookeeper:
image: bitnami/zookeeper
ports:
- "2181:2181"
environment:
ALLOW_ANONYMOUS_LOGIN: "yes"
volumes:
- zookeeper_data:/bitnami/zookeeper

kafka:
image: bitnami/kafka
ports:
- "9092:9092"
environment:
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
ALLOW_PLAINTEXT_LISTENER: "yes"
depends_on:
- zookeeper
volumes:
- kafka_data:/bitnami/kafka
healthcheck:
test: ["CMD", "kafka-topics.sh", "--list", "--zookeeper", "zookeeper:2181"]
interval: 30s
timeout: 10s
retries: 3
restart: unless-stopped

spark:
build: .
ports:
- "4040:4040"
depends_on:
- kafka
entrypoint: ["/bin/bash", "-c", "while true; do sleep 1000; done"]

kafka-producer:
build:
context: ./producer
depends_on:
- kafka
restart: always

volumes:
zookeeper_data:
kafka_data:





FROM bitnami/spark

Install necessary Python packages
USER root
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install numpy py4j pyspark pyarrow

Create the necessary directory for Ivy
RUN mkdir -p /tmp/.ivy2

Create Spark configuration directory and set Ivy config
RUN mkdir -p /opt/bitnami/spark/conf
RUN echo "spark.jars.ivy=/tmp/.ivy2" > /opt/bitnami/spark/conf/spark-defaults.conf

Download Kafka dependencies
RUN /opt/bitnami/spark/bin/spark-shell --packages org.apache.spark
.12:3.5.1 -i /dev/null || true

Copy the Spark consumer script and the fwi_calculator module
COPY spark_consumer.py /opt/bitnami/spark/scripts/spark_consumer.py
COPY fwi_calculator.py /opt/bitnami/spark/scripts/fwi_calculator.py

Ensure the scripts are executable
RUN chmod +x /opt/bitnami/spark/scripts/spark_consumer.py

Set the entrypoint to be able to run any command
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD ["/opt/bitnami/spark/bin/spark-submit", "--packages", "org.apache.spark
.12:3.5.1", "/opt/bitnami/spark/scripts/spark_consumer.py"]
