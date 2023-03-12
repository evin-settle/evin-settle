#datetime
from datetime import timedelta, datetime

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
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['example, helloworld']
)

# python callable function
def print_hello():

    # define a timestamp format you like
    FORMAT = '%Y%m%d%H%M%S'
    path = 'foo.txt'
    data = 'data to be written to the file\n'
    new_path = '%s_%s' % (datetime.now().strftime(FORMAT), path)
    open(new_path, 'w').write(data)

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='hello_world_task', python_callable=print_hello, dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)

# Set the order of execution of tasks. 
start_task >> hello_world_task >> end_task