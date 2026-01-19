from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from operators.collect_source_ts_operator import CollectSourceTsOperator
from operators.transform_operator import TransformOperator
from operators.translate_operator import TranslateOperator
from operators.ml_operator import MLOperator


with DAG(
    dag_id="int_dplm_comm_pipeline_v5",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["int_dplm_comm"],
) as dag:

    ingest_type = "incremental"  # "init" | "incremental"
    collection_nm = "ext_domestic_news"
    raw_base_path = f"/share/airflow/data/raw/{collection_nm}"

    # 🔹 ts는 외부 raw 기준
    decoding = CollectSourceTsOperator(
        task_id="decoding",
        raw_base_path= raw_base_path,
    )

    transform = TransformOperator(
        task_id=f"transform",
        collection_nm=collection_nm,
        ts="{{ ti.xcom_pull(task_ids='decoding') }}",
        ingest_type=ingest_type,
    )

    translate = TranslateOperator(
        task_id=f"translate",
        collection_nm=collection_nm,
        ts="{{ ti.xcom_pull(task_ids='decoding') }}",
        ingest_type=ingest_type,
    )

    ml = MLOperator(
        task_id=f"ml",
        collection_nm=collection_nm,
        ts="{{ ti.xcom_pull(task_ids='decoding') }}",
        ingest_type=ingest_type,
    )

    decoding >> transform >> translate >> ml

