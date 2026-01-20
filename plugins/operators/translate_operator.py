
from airflow.models import BaseOperator
from pathlib import Path
import time
from path.data_path_builder import DataPathBuilder
from airflow.exceptions import AirflowException

class TranslateOperator(BaseOperator):
    template_fields = ("ts")
    def __init__(self, collection_nm: str, ts: str, ingest_type: str, **kwargs):
        super().__init__(**kwargs)
        self.collection_nm = collection_nm
        self.ts = ts
        self.ingest_type = ingest_type

    def execute(self, context):

        builder = DataPathBuilder(self.ingest_type)

        path = builder.staging(self.collection_nm, self.ts)
        self.log.info(f"[Translate] TARGET : {path}")

        if self.collection_nm == 'int_dplm_comm_general':
            time.sleep(10)
            raise AirflowException("의도적으로 실패시킨 task")