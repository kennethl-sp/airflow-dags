from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

with DAG(
    dag_id="test_smtp",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=[
        send_smtp_notification(
            from_email="no-reply@suryapamenang.com",
            to="kenneth.lieyanto@suryapamenang.com",
            # subject="[Error] The dag {{ dag.dag_id }} failed",
            # html_content="debug logs",
        )
    ],
):
    BashOperator(
        task_id="sample_task",
        bash_command="fail",
    )
