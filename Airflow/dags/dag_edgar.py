from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from edgar_helper.extract import scraping_edgar_data

def extract_data(**kwargs):
  topicId = kwargs['dag_run'].conf.get("topicId", "dermatitis")
  file_location = scraping_edgar_data(topicId="dermatitis")
  if not file_location:
    raise Exception('No File Location Returned')
  

#----------------- DAG -----------------#

dag = DAG(
  dag_id="EDGAR_Metadata_DAG",
  schedule_interval=None,
  start_date=days_ago(0),
  catchup=False,
  dagrun_timeout=timedelta(minutes=60),
  max_active_runs=10
)

start_task = DummyOperator(task_id='start', dag=dag)

extract_data_task = PythonOperator(
  task_id='extract_metadata',
  python_callable=extract_data,
  provide_context=True,
  dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> extract_data_task >> end_task