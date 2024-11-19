FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir apache-airflow==2.10.2 \
    apache-airflow-providers-openlineage==1.12.2 \
    apache-airflow-providers-google==10.24.0 \
    apache-airflow-providers-apache-spark==4.11.1 \
    pyspark==3.4.2 \
    faker

