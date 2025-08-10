from airflow.decorators import dag, task, task_group
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1)
}

@dag(default_args=default_args, schedule=None, catchup=False)
def group():

    @task
    def a():
        return 42

    @task_group(default_args={"retries": 2})
    def inner_group(val: int):

        @task
        def b(my_val: int):
            print(f"Task B executed with value: {my_val + 42}")

        @task_group(default_args={"retries": 3})
        def inner_nested_group():

            @task
            def c():
                print("Task C executed")

            c()

        b(val) >> inner_nested_group()

    val = a()
    inner_group(val)

# Assign the DAG to a variable so Airflow can find it
group()
