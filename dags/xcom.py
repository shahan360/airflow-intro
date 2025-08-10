from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    @task
    #def t1(context: Context):
    # def t1() -> int:
    def t1() -> Dict[str, Any]:
        # val = 42
        my_val = 42
        my_sentence = "Hello, World!"
        # context['ti'].xcom_push(key='my_key', value=val)
        # return val # this is equivalent of doing xcom_push with the key = return_value and value = val
        return {
            'my_val': my_val,
            'my_sentence': my_sentence
        }

    @task
    # def t2(context: Context):
    # def t2(val: int):
    def t2(data: Dict[str, Any]):
        # val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
        # print(f"Received XCom value: {val}")
        print(f"Received XCom value: {data['my_val']}")
        print(f"Received XCom sentence: {data['my_sentence']}")

    # t1() >> t2()
    val = t1()
    t2(val)

xcom_dag()