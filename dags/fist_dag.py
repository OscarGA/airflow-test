from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG("my_first_dag",
  start_date=datetime(2024, 5 ,15), 
  schedule_interval='@daily', 
  catchup=False):

  hi = BashOperator(
    task_id="hi_task",
    bash_command="echo 'Hi!'"
  )

  bye = BashOperator(
    task_id="bye_task",
    bash_command="echo 'Bye!'"
  )

  hi >> bye