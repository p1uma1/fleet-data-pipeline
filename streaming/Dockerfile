FROM apache/spark:3.5.1-python3

USER root
RUN pip3 install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1 \
    kafka-python==2.0.2

# Create Ivy cache directory with correct ownership so spark-submit
# can resolve --packages without a FileNotFoundException at runtime.
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2

WORKDIR /opt/project

# Pre-fetch the Kafka + PostgreSQL JARs at image-build time so the
# container starts instantly without needing internet access.
USER spark
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 1 2>/dev/null || true
