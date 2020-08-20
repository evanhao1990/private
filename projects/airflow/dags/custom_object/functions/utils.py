from airflow.utils.email import send_email


def email_alert(context):
    body = f"""
<h1> Task {context['task_instance'].state.upper()}. 
<h3>Time: {context['ds']}. Dag: {context['dag'].dag_id}</h3>
"""
    send_email(to=['hzhang@harlemnext.com'],
               subject="AirflowAlert",
               html_content=body)

