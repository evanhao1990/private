from airflow import DAG, AirflowException
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from custom_object.functions.utils import email_alert
import os
import json
import re


def read_sql(fname):
    path = os.path.join(os.getcwd(), 'dags', 'bq-scheduled-query', 'sql', fname)
    sql = open(path).read()
    return sql


def dag_generator(fname):

    sql = read_sql(fname)

    content = json.loads(re.findall('#!(.*)!#', sql)[0])
    dag_id = content['id']
    interval = content['interval']

    default_args = {
        'owner': 'airflow',
        'start_date': days_ago(1),
        'bigquery_conn_id': 'alg-hn-insights-gcp-conn'}

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=dag_id,
        schedule_interval=interval,
        catchup=False
        )

    with dag:
        task = BigQueryOperator(
            dag=dag,
            task_id=dag_id,
            sql=sql,
            use_legacy_sql=False,
            on_failure_callback=email_alert,
            on_success_callback=email_alert
            )

    return dag


file_names = ['agg_daily_lander_sessions_insert.sql',
              'agg_daily_site_sessions_insert.sql',
              'agg_hourly_site_sessions_create.sql']

for file_name in file_names:
    globals()[file_name] = dag_generator(file_name)

