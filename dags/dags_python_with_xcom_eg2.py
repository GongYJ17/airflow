from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    start_date=None,
    schedule=None,
    catchup=False
) as dag:
    @task(task_id = 'xcom_return_task')
    def xcom_return(**kwargs):
        return 'Success' # {key1 : 'Suceess', key2 : 'Fail'} 로 여러개 전달가능.

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
    
    @task(task_id = 'xcom_return_multiple_task')
    def xcom_multiple_return(**kwargs):
        return {'key1' : 'Suceess', 'key2' : 'Fail'}

    @task(task_id = 'xcom_input_multiple_task')
    def xcom_multiple_return_input(status_dic, **kwargs):
        print('status_dic['key1'] : ' + status_dic['key1'])
        print('status_dic['key2'] : ' + status_dic['key2'])



     
    xcom_return_task = xcom_return() #변수 선언 없이 하면 xcom_return()객체가 서로 다른 존재가 돼서 두개가 생겨버림. 
    xcom_return_task >> [xcom_pull(), xcom_return_input(xcom_return_task) ]
    xcom_multiple_return_input(xcom_multiple_return())



    