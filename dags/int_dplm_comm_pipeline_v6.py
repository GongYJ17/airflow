from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from operators.collect_source_ts_operator import CollectSourceTsOperator
from operators.transform_operator import TransformOperator
from operators.translate_operator import TranslateOperator
from operators.ml_operator import MLOperator


with DAG(
    dag_id="int_dplm_comm_pipeline_v6",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["int_dplm_comm"],
) as dag:

    ingest_type = "init"  # "init" | "incremental"

    # 🔹 ts는 외부 raw 기준

    # 🔹 병렬 처리 단위 (csv에 정의된 collection_nm)
    collections = [
        "int_dplm_comm_general",
        "int_dplm_comm_restricted",
        "int_dplm_comm_secret",
    ]

    for i, collection_nm in enumerate(collections):
        with TaskGroup(group_id=f"process_{collection_nm}") as tg:

            transform = TransformOperator(
                task_id=f"transform",
                collection_nm=collection_nm,
                ts="{{ dag_run.start_date.strftime('%Y%m%d%H%M%S') }}",
                ingest_type=ingest_type,
            )

            translate = TranslateOperator(
                task_id=f"translate",
                collection_nm=collection_nm,
                ts="{{ dag_run.start_date.strftime('%Y%m%d%H%M%S') }}",
                ingest_type=ingest_type,
            )

            ml = MLOperator(
                task_id=f"ml",
                collection_nm=collection_nm,
                ts="{{ dag_run.start_date.strftime('%Y%m%d%H%M%S') }}",
                ingest_type=ingest_type,
            )

            transform >> translate >> ml

        tg
