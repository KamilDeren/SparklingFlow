import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="mobile_pipeline",
    default_args={
        "owner": "Kamil Deren Damian Kluczynski Dominik Dziuba",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

repartition = SparkSubmitOperator(
    task_id="repartition",
    conn_id="spark-conn",
    application="jobs/python/repartition.py",
    application_args=["--input", "/data/source/data1.csv",
                      "--output", "/data/bronze/partitions/"],
    dag=dag
)

faker_data = SparkSubmitOperator(
    task_id="faker_data",
    conn_id="spark-conn",
    application="jobs/python/generate_fake_data.py",
    application_args=["--input", "/data/bronze/partitions/",
                      "--output", "/data/bronze/fake_names_output"],
    dag=dag
)

internet_usage = SparkSubmitOperator(
    task_id="internet_usage",
    conn_id="spark-conn",
    application="jobs/python/internet_usage.py",
    application_args=["--input", "/data/bronze/partitions/",
                      "--output", "/data/silver/internet_usage_output"],
    dag=dag
)
mobile_usage = SparkSubmitOperator(
    task_id="mobile_usage",
    conn_id="spark-conn",
    application="jobs/python/mobile_usage.py",
    application_args=["--input", "/data/bronze/partitions/",
                      "--output", "/data/silver/mobile_usage_output"],
    dag=dag
)

tourist_country = SparkSubmitOperator(
    task_id="tourist_country",
    conn_id="spark-conn",
    application="jobs/python/tourist_country.py",
    application_args=["--input", "/data/bronze/fake_names_output/",
                      "--output", "/data/silver/tourist"],
    dag=dag
)

internet_per_tourist = SparkSubmitOperator(
    task_id="internet_per_tourist",
    conn_id="spark-conn",
    application="jobs/python/internet_per_tourist.py",
    application_args=["--input1", "/data/silver/tourist/",
                      "--input2", "/data/silver/internet_usage_output/",
                      "--output", "/data/gold/total_internet_usage"],
    dag=dag
)

mobile_per_tourist = SparkSubmitOperator(
    task_id="mobile_per_tourist",
    conn_id="spark-conn",
    application="jobs/python/mobile_per_tourist.py",
    application_args=["--input1", "/data/silver/tourist/",
                      "--input2", "/data/silver/mobile_usage_output/",
                      "--output", "/data/gold/total_mobile_usage"],
    dag=dag
)

country_count = SparkSubmitOperator(
    task_id="country_count",
    conn_id="spark-conn",
    application="jobs/python/country_count.py",
    application_args=["--input", "/data/gold/total_internet_usage/",
                      "--output", "/data/gold/country_count/"],
    dag=dag
)

percent_usage = SparkSubmitOperator(
    task_id="percent_usage",
    conn_id="spark-conn",
    application="jobs/python/percent_usage.py",
    application_args=["--input1", "/data/gold/total_mobile_usage/",
                      "--input2", "/data/gold/total_internet_usage/"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> repartition >> faker_data >> [tourist_country, internet_usage, mobile_usage] >> mobile_per_tourist >> internet_per_tourist >> [country_count, percent_usage] >> end
