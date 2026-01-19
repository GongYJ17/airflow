# from airflow import DAG
# from airflow.decorators import task_group
# from datetime import datetime

# from operators.collect_source_path_operator import CollectSourcePathOperator
# from operators.ml_operator import MLOperator
# from operators.transform_operator import TransformOperator
# from operators.translate_operator import TranslateOperator

# with DAG(
#     dag_id="int_dplm_comm_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["int_dplm_comm"],
# ) as dag:

#     # 1️⃣ source_path 리스트 생성
#     collect_source_paths = CollectSourcePathOperator(
#         task_id="collect_source_paths"
#     )

#     # 2️⃣ path 하나당 독립 파이프라인 TaskGroup
#     @task_group(group_id="pipeline")
#     def pipeline_group(source_path: str):

#         transform = TransformOperator(
#             task_id="transform",
#             source_path=source_path, # input : tmp/ | output : staging/
#         )

#         translate = TranslateOperator(
#             task_id="translate",
#             source_path=source_path, # input
#         )

#         ml = MLOperator(
#             task_id="ml",
#             source_path=source_path,
#         )

#         # path 내부 순서만 보장
#         transform >> translate >> ml

#     # 3️⃣ TaskGroup 자체를 Dynamic Mapping
#     pipelines = pipeline_group.expand(
#         source_path=collect_source_paths.output
#     )

#     collect_source_paths >> pipelines
