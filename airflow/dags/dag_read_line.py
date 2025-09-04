from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import inspect

# Default arguments from JSON parameters
default_args = {
    'owner': 'read_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Dynamic function extraction from script content
def read_param_schedule() -> str:
    with open("scripts/do_something.py", "r", encoding="UTF-8") as f:
        line = f.readline().replace("#", "").lstrip()
    return line


with DAG(
    dag_id='generated_py_read_line',
    default_args=default_args,
    description='Обработка сырых данных',
    schedule_interval='@daily',
    start_date=datetime(2025, 9, 4),
    tags=['data-processing', 'etl'],
    catchup=False,
    max_active_runs=1
) as dag:

    # Get all functions from current module (excluding built-ins)
    current_module = sys.modules[__name__]
    functions = inspect.getmembers(current_module, inspect.isfunction)

    # Filter out built-in functions and get only user-defined functions
    user_functions = [(name, obj) for name, obj in functions
                     if not name.startswith('_') and obj.__module__ == __name__]

    # Create tasks dynamically
    previous_task = None

    for func_name, func_obj in user_functions:
        task = PythonOperator(
            task_id=func_name,
            python_callable=func_obj,
            dag=dag,
        )

        # Set dependencies
        if previous_task:
            previous_task >> task
        previous_task = task