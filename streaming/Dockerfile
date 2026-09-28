FROM apache/spark:3.5.1-python3

USER root
RUN pip3 install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1 \
    kafka-python==2.0.2

WORKDIR /opt/project
USER spark
