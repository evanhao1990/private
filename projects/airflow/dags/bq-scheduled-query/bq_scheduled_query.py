from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email
import os
from airflow.configuration import AIRFLOW_HOME

def email_alert_success(context):
    body = f"""
<h1> task {context['task_instance'].task_id} Succeeded</h1>
<h1> Time: {context['ds']}</h1>
<h1> Dag: {context['dag'].dag_id}</h1>
"""
    send_email(to=['hzhang@harlemnext.com'],
               subject=context['task_instance'].task_id,
               html_content=body)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'bigquery_conn_id': 'alg-hn-insights-gcp-conn'
}

bq_dag = DAG(
    dag_id='bq_query',
    default_args=default_args,
    description='bq connect test',
    schedule_interval=None,
    catchup=False
)
# path = '/Users/haozhang/repo/private/projects/airflow/dags/bq-scheduled-query/sql/insert.sql'
path = os.path.join(os.getcwd(), 'dags', 'bq-scheduled-query', 'sql', 'insert.sql')
sql_insert = open(path).read()
bq_create = BigQueryOperator(
    dag=bq_dag,
    task_id='create_table',
    sql=sql_insert,
    use_legacy_sql=False,
    on_success_callback=email_alert_success
)
