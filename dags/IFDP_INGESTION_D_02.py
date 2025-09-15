from airflow.sdk import DAG, task, task_group #버전 3
import pendulum
import datetime
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG( # DAG을 정의한는 부분
    dag_id="IFDP_INGESTION_D_02", # DAG python 파일명과 일치 권장
    schedule=None, # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:

    @task_group()
    def collection_group(): # 수집 task group
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")
    


    @task_group()
    def parsing_group(): # 수집 task group
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")
    

    @task_group()
    def ml_group(): # 수집 task group
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")
    


    @task_group()
    def load_group(): # 수집 task group
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")







    