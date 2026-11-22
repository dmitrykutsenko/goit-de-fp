FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow

RUN pip install pyspark
