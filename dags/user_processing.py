from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
    )

user_processing()
