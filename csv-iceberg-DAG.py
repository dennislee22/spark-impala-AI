from datetime import datetime, timedelta
from dateutil import parser
import pendulum
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.bash import BashOperator

default_args = {
        'owner': 'dennis',
        'retry_delay': timedelta(seconds=5),
        'depends_on_past': False,
        'start_date': pendulum.datetime(2025, 8, 12, tz="Asia/Singapore")
        }

dag1 = DAG(
        'csv-iceberg-pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        is_paused_upon_creation=False
        )

spark_step = CDEJobRunOperator(
        task_id='csv-iceberg-job',
        dag=dag1,
        job_name='celltower-csv-iceberg'
        )

shell = BashOperator(
        task_id='bash',
        dag=dag1,
        bash_command='echo "Hello Airflow" '
        )

spark_step >> shell