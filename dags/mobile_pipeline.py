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
                      "--temp", "/data/temp_partitions/",
                      "--output", "/data/bronze/partitions/"],
    dag=dag
)

faker_data = SparkSubmitOperator(
    task_id="faker_data",
    conn_id="spark-conn",
    application="jobs/python/generate_fake_data.py",
    dag=dag
)

internet_usage = SparkSubmitOperator(
    task_id="internet_usage",
    conn_id="spark-conn",
    application="jobs/python/internet_usage.py",
    dag=dag
)
mobile_usage = SparkSubmitOperator(
    task_id="mobile_usage",
    conn_id="spark-conn",
    application="jobs/python/mobile_usage.py",
    dag=dag
)

tourist_country = SparkSubmitOperator(
    task_id="tourist_country",
    conn_id="spark-conn",
    application="jobs/python/tourist_country.py",
    dag=dag
)

internet_per_tourist = SparkSubmitOperator(
    task_id="internet_per_tourist",
    conn_id="spark-conn",
    application="jobs/python/internet_per_tourist.py",
    dag=dag
)

mobile_per_tourist = SparkSubmitOperator(
    task_id="mobile_per_tourist",
    conn_id="spark-conn",
    application="jobs/python/mobile_per_tourist.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> repartition >> faker_data >> [tourist_country, internet_usage, mobile_usage] >> [mobile_per_tourist,internet_per_tourist] >> end
