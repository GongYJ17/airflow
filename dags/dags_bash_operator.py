from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.bash import BashOperator


with DAG( # DAG을 정의한는 부분
    dag_id="dags_bash_operator", # DAG python 파일명과 일치 권장
    schedule="0 0 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
    #tags=["example", "example2", "example3"], # 해시태크들
) as dag:
    
    bash_t1 = BashOperator( # bash관련 기능이 담긴 객체 생성 -> 이걸 가지고 task 만듦
        task_id="bash_t1", # task명, 변수명과 동일해야 찾기 쉬움
        bash_command="echo whoami") #echo는 print와 비슷한 역할
    
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME"
    )

    bash_t1 >> bash_t2