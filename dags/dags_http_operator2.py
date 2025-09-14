from airflow.sdk import DAG, task
import pendulum
import datetime
from airflow.providers.http.operators.http import HttpOperator



with DAG( # DAG을 정의한는 부분
    dag_id="dags_python_task_decorator", # DAG python 파일명과 일치 권장
    schedule="30 6 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:
    
    get_deplomacy_info = HttpOperator(
        task_id = 'get_deplomacy_info',
        http_conn_id='openapi.data.go.kr',
        endpoint='EY9KD4N1rfHjBemZB5rmj/GdCrIwKdmHC2EibAahXYrY/2EW6glZA5kjIoLQ9lpMPSVNkcvqKzSg9tt5y8vhpA==/10/1/',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()