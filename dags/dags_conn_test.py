from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.empty import EmptyOperator



with DAG( # DAG을 정의한는 부분
    dag_id="dags_conn_test", # DAG python 파일명과 일치 권장
    schedule="0 0 * * *", # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
    #tags=["example", "example2", "example3"], # 해시태크들
) as dag:
    
    t1 = EmptyOperator(
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )
    
    t3 = EmptyOperator(
        task_id="t3"
    )

    t4 = EmptyOperator(
        task_id="t4"
    )

    t5 = EmptyOperator(
        task_id="t5"
    )

    t6 = EmptyOperator(
        task_id="t6"
    )

    t7 = EmptyOperator(
        task_id="t7"
    )

    t8 = EmptyOperator(
        task_id="t8"
    )

    t1 >> [t2, t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8


    