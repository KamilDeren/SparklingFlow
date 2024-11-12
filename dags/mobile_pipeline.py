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

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> repartition >> faker_data >> end
