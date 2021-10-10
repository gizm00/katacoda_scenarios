from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
 
# Default settings applied to all tasks
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0,
   'catchup': False,
   'start_date': datetime(2021, 1, 1)
}
 
with DAG(
   dag_id='data_pipeline_demo',
   description='Data pipeline demo',
   schedule_interval=None,
   default_args=default_args
   ) as dag:
 
   t1 = BashOperator(
       task_id='bash_task_10',
       bash_command='echo "Sleeping..." && sleep 5s && date'
   )
 
   for t in range(3):
       t0 = BashOperator(
           task_id=f"bash_task_{t}",
           bash_command=f"echo value: {t}"
       )
 
       t0 >> t1
