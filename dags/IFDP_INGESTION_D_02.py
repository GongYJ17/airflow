from airflow.sdk import DAG, task, task_group #버전 3
import pendulum
import datetime
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# task_id 명명규칙 : t{순번}_{상세기능}
# 소문자만 사용,
# 순번은 01, 02,,,로 작성
# 상세기능은 task_id로 기능을 유추할 수 있어야하고, 두 단어 이상일 경우 언더스코어로 구분함
# dag 파일 내 task 내에는 유지보수 편의성 및 디버깅을 위해 가급적 업무 로직 대응 코드를 포함하지 않는 것을 권장한다. 업무 로직 대응 코드의 경우 별도 python module을 만들고 해당 module을 PythonOperator로 호출하여 사용하는 것을 권장한다.
# 마지막 줄은,, dag 파일 내에 작성하지 말라는 건지, 같은 파일은 되는데 task에 작성하지 말라는 건지 모르겠네.

with DAG( # DAG을 정의하는 부분
    dag_id="IFDP_INGESTION_D_02", # DAG python 파일명과 일치 권장
    schedule=None, # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:

    @task_group()
    def collection_group(): # 수집 task group
        t01_check_connections = EmptyOperator(task_id="t01_check_connections")
        task2 = EmptyOperator(task_id="task2")
        task3 = EmptyOperator(task_id="task3")

    


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







    