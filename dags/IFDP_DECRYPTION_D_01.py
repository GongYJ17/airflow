from airflow import DAG # Airflow 버전 2
from airflow.decorators import task # Airflow 버전 2
from common.config import schedule_param
import pendulum
import datetime

with DAG( # DAG을 정의한는 부분
    dag_id="IFDP_DECRYPTION_D_01", # DAG python 파일명과 일치 권장
    schedule="* * * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 9, 15, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:
    # 샘플
    @task(task_id="python_task_1")
    def print_context(your_name):
        print(your_name)

    python_task_1 = print_context('연정 시작합니다.')