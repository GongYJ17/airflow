from airflow.models import BaseOperator
from pathlib import Path
import time

class TranslateOperator(BaseOperator):
    def __init__(self, source_path: str, **kwargs):
        super().__init__(**kwargs)
        self.source_path = source_path

    def execute(self, context):
        if self.source_path == 'path1':
            time.sleep(10)
        self.log.info("Translate start: %s", self.source_path)
