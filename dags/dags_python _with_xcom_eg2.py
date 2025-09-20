from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    start_date=None,
    schedule=None,
    catchup=False
) as dag:
    @task(task_id = 'xcom_return_task')
    def xcom_return(**kwargs):
        return 'Success'

    @task(task_id = 'python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        return_value = ti.xcom_pull(key='return_value', task_ids = 'xcom_return_task')
        print(f'return_value: {return_value}')
        only_task_ids = ti.xcom_pull(task_ids = 'xcom_return_task')
        print(only_task_ids)
    
    @task(task_id = 'xcom_input_task')
    def xcom_return_input(status, **kwargs):
        print(status)

    xcom_return_input() >> [xcom_pull(), xcom_pull(xcom_return_input())]




    