# importing the libraries 

from datetime import timedelta
# DAG object; this is needed to instantiate a DAG
from airflow import DAG
#Operator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# making scheduling easy
from airflow.utils.dates import days_ago
# Importing other libraries for downloading and saving file
import urllib
import zipfile

# Defining DAG Arguments

default_args = {
    'owner': 'Karthik J Ghorpade',
    'start_date': days_ago(0),
    'email': ['karthik@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delta': timedelta(minutes=5)
}

dag = DAG(
    'Server_Access_Log_Processing_Dag',
    default_args=default_args,
    description='ETL of Timestamp and Visitor ID using this DAG',
    schedule_interval=timedelta(days=1),
    )

# Defining the tasks
#First Task - Download File from URL

def download_file(uri, target_path):
    with urllib.request.urlopen(uri) as file:
        with open(target_path, "wb") as new_file:
            new_file.write(file.read())

download = PythonOperator(
    task_id='download',
    python_callable = download_file,
    op_kwargs = {
        "uri" : "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt",
        "target_path" : "/home/project/airflow/dags/web-server-access-log.txt"
    },
    dag=dag
)

# Second Task - Extract the Timestamp & Visitor ID
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d"#" -f1,4 /home/project/airflow/dags/web-server-access-log.txt > /home/project/airflow/dags/extracted-data.txt',
    dag=dag
)

#Third Task - Transform data to change delimiter from "#" to ","
transform = BashOperator(
    task_id='transform',
    bash_command='tr "#" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag
)

#Fourth Task - Load by compressing data 
def zipping_file(file_to_zip, target_path):
   with zipfile.ZipFile(target_path, 'w') as zip:
    zip.write(file_to_zip)

load = PythonOperator(
    task_id='load',
    python_callable = zipping_file,
    op_kwargs = {
        "file_to_zip" : "/home/project/airflow/dags/transformed-data.csv",
        "target_path" : "/home/project/airflow/dags/log.zip"
    },
    dag=dag
)

#Task Pipeline
download >> extract >> transform >> load