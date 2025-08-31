from airflow import DAG
import pendulum
import datetime
import random
from airflow.providers.standard.operators.python import PythonOperator # (만든) python 함수를 실행시켜주는 역할


with DAG( # DAG을 정의한는 부분
    dag_id="dags_python_operator", # DAG python 파일명과 일치 권장
    schedule="30 6 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:
    def select_fruit():
        fruit = ['apple', 'banana', 'cherry','avocado']
        rand_int = random.randint(0, 3)
        return fruit[rand_int]
    

    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=select_fruit
    )

    py_t1 
