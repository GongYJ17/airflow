from airflow.models import BaseOperator
from pathlib import Path

class CollectSourcePathOperator(BaseOperator):
    """
    일반 / 비밀 / 대외비 source_path 목록 생성
    """

    def execute(self, context):
        # ts = context["ts_nodash"]

        # base_paths = [
        #     f"/tmp/int_dplm_comm_일반/{ts}",
        #     f"/tmp/int_dplm_comm_비밀/{ts}",
        #     f"/tmp/int_dplm_comm_대외비/{ts}",
        # ]

        base_paths = [
            "path1",
            "path2",
            "path3"
        ]

        self.log.info("Source paths: %s", base_paths)
        return base_paths   # ✅ XCom
