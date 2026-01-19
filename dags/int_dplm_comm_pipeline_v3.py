# from airflow import DAG
# from airflow.decorators import task_group
# from datetime import datetime
# from airflow.utils.task_group import TaskGroup

# from operators.collect_source_path_operator import CollectSourcePathOperator
# from operators.ml_operator import MLOperator
# from operators.transform_operator import TransformOperator
# from operators.translate_operator import TranslateOperator

# with DAG(
#     dag_id="int_dplm_comm_pipeline_v3",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["int_dplm_comm"],
# ) as dag:

#         collect_ts = CollectSourcePathOperator(
#             task_id="collect_ts"
#         )

#         categories = ["일반", "대외비", "비밀"]

#         for i, category in enumerate(categories):
#             with TaskGroup(group_id=f"process_{category}") as tg:

#                 transform = TransformOperator(
#                     task_id=f"transform_{i}",
#                     category=category,
#                     ts="{{ ti.xcom_pull(task_ids='collect_ts') }}",
#                 )

#                 translate = TranslateOperator(
#                     task_id=f"translate_{i}",
#                     category=category,
#                     ts="{{ ti.xcom_pull(task_ids='collect_ts') }}",
#                 )

#                 ml = MLOperator(
#                     task_id=f"ml_{i}",
#                     category=category,
#                     ts="{{ ti.xcom_pull(task_ids='collect_ts') }}",
#                 )

#                 transform >> translate >> ml

#         collect_ts >> tg


