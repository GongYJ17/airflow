from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from plugins.operators.collect_source_path_operator import CollectSourcePathOperator
from plugins.operators.ml_operator import MLOperator
from plugins.operators.transform_operator import TransformOperator
from plugins.operators.translate_operator import TranslateOperator


with DAG(
    dag_id="int_dplm_comm_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 1. source_path 수집
    collect_source_paths = CollectSourcePathOperator(
        task_id="collect_source_paths"
    )

    # 2. path 하나당 처리 파이프라인
    with TaskGroup("process_by_security_level") as process_group:

        transform = TransformOperator.partial(
            task_id="transform"
        ).expand(
            source_path=collect_source_paths.output
        )

        translate = TranslateOperator.partial(
            task_id="translate"
        ).expand(
            source_path=collect_source_paths.output
        )

        ml = MLOperator.partial(
            task_id="ml"
        ).expand(
            source_path=collect_source_paths.output
        )

        # path 내부 순서 보장
        transform >> translate >> ml

    collect_source_paths >> process_group
