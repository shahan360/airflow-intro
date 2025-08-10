from airflow.sdk import dag, task

@dag
def branch():

    @task
    def a():
        return 1

    @task.branch
    def b(val):
        if val == 1:
            return "equal_1"
        return "not_equal_1"

    @task
    def equal_1(val: int):
        print(f"Equal to 1: {val}")

    @task
    def not_equal_1(val: int):
        print(f"Not equal to 1: {val}")

    val = a()
    branch_task = b(val)  # call b with val properly
    branch_task >> [equal_1(val), not_equal_1(val)]

branch()
