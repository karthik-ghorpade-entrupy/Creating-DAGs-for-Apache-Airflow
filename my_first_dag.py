# importing the libraries 

from datetime import timedelta
# DAG object; this is needed to instantiate a DAG
from airflow import DAG
#Operator
from airflow.operators.bash_operator import BashOperator
# making scheduling easy
from airflow.utils.dates import days_ago

# Defining DAG arguments

default_args = {
    'owner': 'Karthik J Ghorpade',
    'start_date': days_ago(0),
    'email': ['karthik@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delta': timedelta(minutes=5)
}

# Defining the DAG

dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description='My First DAG! (Technically not)',
    schedule_interval=timedelta(days=1)
)

# Defining the tasks
#First Task
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag
)
#Second Task
transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag
)

#Task Pipeline
extract >> transform_and_load