from airflow import DAG
from airflow.decorators import task
import pendulum
import datetime
from airflow.providers.standard.operators.python import PythonOperator # (만든) python 함수를 실행시켜주는 역할
import random

with DAG( # DAG을 정의한는 부분
    dag_id="dags_python_task_decorator", # DAG python 파일명과 일치 권장
    schedule="30 6 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:

    @task(task_id="python_task_1")
    def print_context(your_name):
        print(your_name)

    python_task_1 = print_context('연정 시작합니다.')

    #python_task_1 이렇게 선언도 안해도 됨.
    
