from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.python import PythonOperator # (만든) python 함수를 실행시켜주는 역할
# python 인터프리터는 plugins 부터 인식함. airflow는 sys.path에 plugins가 있어서 from common ~으로 작성해야함
# 둘의 상충 문제를 해결하기 위해 .env를 사용
from common.common_func import regist2


with DAG( # DAG을 정의한는 부분
    dag_id="dags_python_with_op_kwargs", # DAG python 파일명과 일치 권장
    schedule="30 6 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 9, 4, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:

    regist2_t1 = PythonOperator(
        task_id="regist2_t1",
        python_callable = regist2,
        op_args=['hjkim', 'man', 'kr', 'seoul'],
        op_kwargs={'email' : 'yjgong@vaiv.kr', 'phone' : '010-1234-5678'}

    )

    regist2_t1
