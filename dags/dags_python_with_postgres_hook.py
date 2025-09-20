from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook




with DAG(
    dag_id = 'dags_python_with_postgres_hook',
    start_date=None,
    schedule=None,
    catchup=False
) as dag:
    
    def insrt_postgres_hook(conn_id, **kwargs) : 
        import psycopg2
        from contextlib import closing


        conn_info = PostgresHook(conn_id)

        with closing(conn_info.get_conn()) as conn:
            with closing(conn.cursor()) as cursor : #여기서 cursor는 db를 부르는 곳과 db를 연결해주는 것을 session이라고 하는데, session 안에서 쿼리를 전달하고 응답을 받아주는 애가 cursor임.
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook사용해서 insert작업 수행해보기'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()
            

    insrt_postgres_hook = PythonOperator(
        task_id = 'insrt_postgres_hook',
        python_callable=insrt_postgres_hook,
        op_kwargs={'conn_id' : 'conn-db-postgres-custom'}
    )

    insrt_postgres_hook