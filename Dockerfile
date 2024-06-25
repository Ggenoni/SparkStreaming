FROM bitnami/spark:latest

# Install necessary Python packages
USER root
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install numpy py4j pyspark pyarrow

# Create the necessary directory for Ivy
RUN mkdir -p /tmp/.ivy2

# Create Spark configuration directory and set Ivy config
RUN mkdir -p /opt/bitnami/spark/conf
RUN echo "spark.jars.ivy=/tmp/.ivy2" > /opt/bitnami/spark/conf/spark-defaults.conf

# Download Kafka dependencies
RUN /opt/bitnami/spark/bin/spark-shell --packages org.apache.spark.12:3.5.1 -i /dev/null || true


# Copy the Spark consumer script and the fwi_calculator module
COPY spark_consumer.py /opt/bitnami/spark/scripts/spark_consumer.py
COPY fwi_calculator.py /opt/bitnami/spark/scripts/fwi_calculator.py

# Ensure the scripts are executable
RUN chmod +x /opt/bitnami/spark/scripts/spark_consumer.py

# Set the entrypoint to be able to run any command
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD ["/opt/bitnami/spark/bin/spark-submit", "--packages", "org.apache.spark.12:3.5.1", "/opt/bitnami/spark/scripts/spark_consumer.py"]
