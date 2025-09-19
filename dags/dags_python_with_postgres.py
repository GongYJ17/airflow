from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_postgres',
    start_date=None,
    schedule=None,
    catchup=False
) as dag:
    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs) : 
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host = ip, dbname = dbname, user = user, password = passwd, port = int(port))) as conn:
            with closing(conn.cursor()) as cursor : #여기서 cursor는 db를 부르는 곳과 db를 연결해주는 것을 session이라고 하는데, session 안에서 쿼리를 전달하고 응답을 받아주는 애가 cursor임.
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insert 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()
            

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'yjgong','yjgong','yjgong']
    )

    insrt_postgres