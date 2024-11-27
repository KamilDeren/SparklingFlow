# Apache Airflow on Steroids with Java, Scala and Python Spark Jobs

This project orchestrates Spark jobs written in different programming languages using Apache Airflow, all within a Dockerized environment. The DAG `sparking_flow` is designed to submit Spark jobs written in Python, Scala, and Java, ensuring that data processing is handled efficiently and reliably on a daily schedule.

## Project Structure

The DAG `sparking_flow` includes the following tasks:

- `start`: A PythonOperator that prints "Jobs started".
- `python_job`: A SparkSubmitOperator that submits a Python Spark job.
- `end`: A PythonOperator that prints "Jobs completed successfully".

These tasks are executed in a sequence where the `start` task triggers the Spark jobs in parallel, and upon their completion, the `end` task is executed.

## Prerequisites

Before setting up the project, ensure you have the following:

- Docker and Docker Compose installed on your system.
- Apache Airflow Docker image or a custom image with Airflow installed.
- Apache Spark Docker image or a custom image with Spark installed and configured to work with Airflow.
- Docker volumes for Airflow DAGs, logs, and Spark jobs are properly set up.

## Directory Structure for Jobs
Ensure your Spark job files are placed in the following directories and are accessible to the Airflow container:

* Python job: jobs/python/wordcountjob.py

These paths should be relative to the mounted Docker volume for Airflow DAGs.

## Usage
After the Docker environment is set up, the `sparking_flow` DAG will be available in the Airflow web UI [localhost:8080](localhost:8080), where it can be triggered manually or run on its daily schedule.