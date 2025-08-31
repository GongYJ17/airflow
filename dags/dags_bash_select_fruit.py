from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.bash import BashOperator


with DAG( # DAG을 정의한는 부분
    dag_id="dags_bash_select_fruit", # DAG python 파일명과 일치 권장
    schedule="10 0 * * 6#1", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 1/1일부터 소급적용을 할건지
) as dag:
        
        t1_orange = BashOperator( # bash관련 기능이 담긴 객체 생성 -> 이걸 가지고 task 만듦
            task_id="t1_orange", # task명, 변수명과 동일해야 찾기 쉬움
            bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE")

        t2_apple = BashOperator(
            task_id="t2_apple",
            bash_command="/opt/airflow/plugins/shell/select_fruit.sh APPLE"
        )

        t1_orange >> t2_apple