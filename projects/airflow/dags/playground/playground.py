from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook, BigQueryPandasConnector
from airflow.operators.email_operator import EmailOperator
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

bq_dag = DAG(
    dag_id='bq_dag',
    default_args=default_args,
    description='bq connect test',
    schedule_interval=None,
    catchup=False
)


def bq_test(context):
    bq_hook = BigQueryHook(bigquery_conn_id='alg-hn-insights-gcp-conn', use_legacy_sql=False)
    test = bq_hook.table_exists(project_id='alg-hn-insights', dataset_id='GA', table_id='agg_daily_ga_sessions_partitioned')
    print(test)


bq = PythonOperator(
    dag=bq_dag,
    task_id='this_is_bq',
    python_callable=bq_test,
    provide_context=True
)

email = EmailOperator(
        task_id='send_email',
        to='hzhang@harlemnext.com',
        subject='Airflow Alert',
        html_content=""" <h3> {{ context['task_instance'].task_id }}</h3> """,
        dag=bq_dag,
    )

email >> bq
