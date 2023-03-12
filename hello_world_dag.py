#datetime
from datetime import timedelta, datetime
import csv
# The DAG object
from airflow import DAG
from datetime import datetime

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
		'owner': 'Ranga',
		'start_date': datetime(2022, 3, 4),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
hello_world_dag = DAG('hello_world_dag',
		default_args=default_args,
		description='Hello World DAG',
		schedule_interval='*/20 * * * *', 
		catchup=False,
		tags=['example, helloworld']
)

# python callable function
def print_hello():

     row_list = [["SNo", "Name", "Subject"],
                  [1, "Ash Ketchum", "English"],
                  [2, "Gary Oak", "Mathematics"],
                  [3, "Brock Lesner", "Physics"]]

     with open('studentsq.csv', 'w', newline='') as file:
         writer = csv.writer(file)
         writer.writerows(row_list)

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='hello_world_task', python_callable=print_hello, dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)

# Set the order of execution of tasks. 
start_task >> hello_world_task >> end_task