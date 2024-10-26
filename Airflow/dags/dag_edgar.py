from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator 
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="edgar_dag",
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=10
)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

start_task >> end_task