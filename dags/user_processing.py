from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
'''
Q: What is a Sensor?
A: A sensor is a special type of operator that will keep running until a certain condition is met.
In this case, we will use a sensor to check if a table exists in the database.
'''
from airflow.sdk.bases.sensor import PokeReturnValue


'''
Q: What is PythonOperator?
A: A PythonOperator is an operator that allows you to execute a Python function as a task
'''
# from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

# def _extract_user(ti):
#     # fake_user = ti.xcom_pull(task_ids='is_api_available') # xcom_pull is used to share data between tasks. here we are pulling the data from the is_api_available task.
#     import requests
#     response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
#     fake_user = response.json()
#     return {
#         "id": fake_user['id'],
#         "firstname": fake_user['personalInfo']['firstName'],
#         "lastname": fake_user['personalInfo']['lastName'],
#         "email": fake_user['personalInfo']['email']
#     }
#     print(fake_user)
    

@dag
def user_processing():
    # We will define an operator. An operator is conceptually a template for a predefined task.
    # We will use SQLExecuteQueryOperator to execute a SQL query.
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Verify if the API is available or not.
    # We have implemented a sensor to check if the API is available.
    # If the API is available, we will fetch the data from the API and insert it
    # into the users table.
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        # If the response status code is 200, the API is available.
        # If the response status code is not 200, the API is not available.
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        # fake_user = ti.xcom_pull(task_ids='is_api_available') # xcom_pull is used to share data between tasks. here we are pulling the data from the is_api_available task.
        # import requests
        # response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        # fake_user = response.json()
        return {
            "id": fake_user['id'],
            "firstname": fake_user['personalInfo']['firstName'],
            "lastname": fake_user['personalInfo']['lastName'],
            "email": fake_user['personalInfo']['email'],
        }

    # extract_user = PythonOperator(
    #     task_id='extract_user',
    #     python_callable=_extract_user
    # )

    @task
    def process_user(user_info):
        import csv
        from datetime import datetime

        # user_info = {
        # "id": 123,
        # "firstname": 'John',
        # "lastname": 'Doe',
        # "email": 'john.doe@example.com',
        # "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        # "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # }

        user_info["created_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_info["updated_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open('/tmp/user_info.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )

    process_user(extract_user(create_table >> is_api_available())) >> store_user()  # Chain the tasks together
    #Call the sensor to check if the API is available.
    # fake_user = is_api_available()
    # user_info = extract_user(fake_user)
    # process_user(user_info)
    # store_user()

user_processing()