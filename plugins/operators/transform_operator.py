
from airflow.models import BaseOperator
from pathlib import Path
import time
from path.data_path_builder import DataPathBuilder

class TransformOperator(BaseOperator):
    def __init__(self, collection_nm: str, ts: str, ingest_type: str, **kwargs):
        super().__init__(**kwargs)
        self.collection_nm = collection_nm
        self.ts = ts
        self.ingest_type = ingest_type

    def execute(self, context):
        if self.collection_nm == 'int_dplm_comm_general':
            time.sleep(10)
        builder = DataPathBuilder(self.ingest_type)

        input_path = builder.tmp(self.collection_nm, self.ts)
        output_path = builder.staging(self.collection_nm, self.ts)

        self.log.info(f"[Transform] INPUT  : {input_path}")
        self.log.info(f"[Transform] OUTPUT : {output_path}")