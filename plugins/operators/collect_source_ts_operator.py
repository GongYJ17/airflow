from airflow.models import BaseOperator
from pathlib import Path

class CollectSourceTsOperator(BaseOperator):
    """
    ts 전달
    """

    def execute(self, context):

        return "20260119130000"   # ✅ XCom
