from airflow.sdk import DAG, task
import pendulum
import datetime
from airflow.providers.http.operators.http import HttpOperator



with DAG( # DAG을 정의한는 부분
    dag_id="dags_http_operator3", # DAG python 파일명과 일치 권장
    schedule=None, # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:
    
    get_diplomacy_info = HttpOperator(
        task_id='get_diplomacy_info',
        http_conn_id='openapi.data.go.kr',
        endpoint='/1262000/DiplomacyJournalService/getDiplomacyJournalList',  # 실제 API 엔드포인트
        method='GET',
        data={
        "serviceKey": 'EY9KD4N1rfHjBemZB5rmj/GdCrIwKdmHC2EibAahXYrY/2EW6glZA5kjIoLQ9lpMPSVNkcvqKzSg9tt5y8vhpA==',
        "numOfRows": 10,
        "pageNo": 1
        },
        headers={"Content-Type": "application/json"},
        extra_options={"verify": False}  # SSL 검증 비활성화
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        print(ti)
        rslt = ti.xcom_pull(task_ids='get_deplomacy_info')
        print(rslt)
        # import json
        # from pprint import pprint

        # pprint(rslt.json())
        
    get_diplomacy_info >> python_2()