from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator
from requests import get
import smtplib 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def email_Trigger():  
    from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator
import subprocess


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def connections():
    import subprocess, platform

    # Ping parameters as function of OS
    ping_str = "-n 1" if  platform.system().lower()=="windows" else "-c 1"
    args = "telnet " + " " + "dqlabs-qa.c2jb582uui1r.us-east-1.rds.amazonaws.com 5432"
    need_sh = False if  platform.system().lower()=="windows" else True

    # Ping
    print(subprocess.call(args, shell=need_sh) == 0)

with DAG(
        default_args=default_args,
        dag_id='java_versionget',
        start_date=datetime(2023, 10, 8),
        catchup=False,
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='first',
        python_callable=connections
    )
    
    task1
 

with DAG(
        default_args=default_args,
        dag_id='email_trigger',
        start_date=datetime(2023, 10, 8),
        catchup=False,
        schedule_interval=None
) as dag:
    task1 = PythonOperator(
        task_id='first',
        python_callable=email_Trigger
    )
    
    task1

