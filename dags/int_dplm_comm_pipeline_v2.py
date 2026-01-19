# from airflow import DAG
# from airflow.decorators import task_group
# from datetime import datetime
# from airflow.utils.task_group import TaskGroup

# from operators.collect_source_path_operator import CollectSourcePathOperator
# from operators.ml_operator import MLOperator
# from operators.transform_operator import TransformOperator
# from operators.translate_operator import TranslateOperator

# with DAG(
#     dag_id="int_dplm_comm_pipeline_v2",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["int_dplm_comm"],
# ) as dag:

#     # 1️⃣ source_path 리스트 생성
#     # collect_source_paths = CollectSourcePathOperator(
#     #     task_id="collect_source_paths"
#     # )

#     results = [("path1", "일반"), ("path2", "대외비"), ("path3", "비밀")]

#     for i, (source_path, category) in enumerate(results):
#         with TaskGroup(group_id=f"process_{category}") as tg:

#             transform = TransformOperator(
#                 task_id= f"transform_{i}",
#                 source_path=source_path, # input : tmp/ | output : staging/
#             )

#             translate = TranslateOperator(
#                 task_id = f'translate_{i}',
#                 source_path = "{{ ti.xcom_pull(task_ids=task.task_group.group_id + '.transform_' + str(i) + '') }}"
#             )

#             ml = MLOperator(
#                 task_id = f'ml_{i}',
#                 source_path = "{{ ti.xcom_pull(task_ids=task.task_group.group_id + '.translate_' + str(i) + '') }}"
#             )

#             transform >> translate >> ml
    
#     tg

