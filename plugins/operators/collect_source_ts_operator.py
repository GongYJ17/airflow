from airflow.models import BaseOperator
from pathlib import Path

class CollectSourceTsOperator(BaseOperator):
    """
    ts 전달
    """
    def __init__(self, raw_base_path: str, **kwargs):
        super().__init__(**kwargs)
        self.raw_base_path = raw_base_path

    def execute(self, context):
        self.log.info(f"[Transform] INPUT  : {self.raw_base_path}")
        return "20260119130000"   # ✅ XCom
